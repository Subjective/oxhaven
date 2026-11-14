//! Writes a [`BarrageMessage`] for one client: the `BarrageUpdateMetadata` that says which
//! rows the client gained, lost and must change, and the record batches with their data.
//!
//! One message serves every subscriber of a table; each gets the part its viewport and
//! columns cover. A subscriber of the whole table is told row keys, a viewport subscriber
//! is told positions within its viewport.

use arrow::array::{Array, ArrayRef, UInt64Array};

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageMessageWrapper, BarrageMessageWrapperArgs, BarrageModColumnMetadata,
    BarrageModColumnMetadataArgs, BarrageUpdateMetadata, BarrageUpdateMetadataArgs, MAGIC,
};
use oxhaven_proto::flight::FlightData;

use super::codec::{encode_row_set, encode_shift_data, ColumnSet};
use crate::flight::{has_sentinel, record_batch_messages_with, BatchOptions, EncodeError};

/// The modified rows of one column and their new values, in row order.
#[derive(Clone, Debug)]
pub struct ModColumn {
    pub rows_modified: RowSet,
    /// One value per modified row; empty when no row is.
    pub data: ArrayRef,
}

/// A snapshot of a table, or the change of a table over one or more update cycles, before
/// it is cut down to what one client receives.
///
/// Port of `io.deephaven.engine.table.impl.util.BarrageMessage`.
#[derive(Clone, Debug)]
pub struct BarrageMessage {
    /// The steps of the update graph this covers; -1 for a static table.
    pub first_seq: i64,
    pub last_seq: i64,
    pub is_snapshot: bool,
    /// The rows the table gained; for a snapshot, every row of the table.
    pub rows_added: RowSet,
    pub rows_removed: RowSet,
    pub shifted: RowSetShiftData,
    /// The rows `add_columns` has data for: the added rows some subscriber can see, and
    /// rows that moved into a viewport.
    pub rows_included: RowSet,
    /// Per column of the table, the values of `rows_included`, or an empty array for a
    /// column that was not recorded.
    pub add_columns: Vec<ArrayRef>,
    pub mod_columns: Vec<ModColumn>,
    pub table_size: i64,
}

/// What a client asked of the data it is sent.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageOptions`
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct WireOptions {
    /// Nulls of primitive columns are sent as Java's sentinel values, with no validity
    /// buffer.
    pub use_deephaven_nulls: bool,
    /// Each column of a record batch is sent as the one element of a list, for clients
    /// that cannot read columns of different lengths. Subscriptions only.
    pub columns_as_list: bool,
    /// The most elements sent of each cell of an array or vector column; see
    /// [`BatchOptions::list_length_limit`].
    pub preview_list_length_limit: i64,
    /// The sizes the client asked for. The writer fills in the other fields.
    pub batch: BatchOptions,
}

/// Which part of a message one subscriber receives.
///
/// Port of the arguments of `BarrageMessageWriter#getSubView`.
pub struct SubView<'a> {
    pub options: WireOptions,
    pub is_initial_snapshot: bool,
    pub is_full_subscription: bool,
    /// The subscriber's viewport in positions, if it has one.
    pub viewport: Option<&'a RowSet>,
    pub reverse_viewport: bool,
    /// The row keys the viewport covered before the message, and covers after it.
    pub keyspace_viewport_prev: Option<&'a RowSet>,
    pub keyspace_viewport: Option<&'a RowSet>,
    pub subscribed_columns: Option<&'a ColumnSet>,
}

/// Which part of a snapshot message a snapshot request receives.
///
/// Port of the arguments of `BarrageMessageWriter#getSnapshotView`.
pub struct SnapshotView<'a> {
    pub options: WireOptions,
    pub viewport: Option<&'a RowSet>,
    pub reverse_viewport: bool,
    pub keyspace_viewport: Option<&'a RowSet>,
    pub snapshot_columns: Option<&'a ColumnSet>,
}

fn error(problem: impl std::fmt::Display) -> EncodeError {
    EncodeError(problem.to_string())
}

/// What the record batch writer is told: the sizes the client asked for, and what its
/// options mean for the elements of array and vector columns.
fn batch(options: WireOptions) -> BatchOptions {
    BatchOptions {
        element_sentinels: options.use_deephaven_nulls,
        list_length_limit: options.preview_list_length_limit,
        ..options.batch
    }
}

/// Per column, whether a validity buffer may be written. An array or vector column has
/// one whenever a cell is null; [`batch`] passes the option on for its elements.
///
/// Behavior: `BaseChunkWriter.BaseChunkInputStream#nullCount`
fn validity(message: &BarrageMessage, nullable: &[bool], options: WireOptions) -> Vec<bool> {
    message
        .add_columns
        .iter()
        .zip(nullable)
        .map(|(column, &nullable)| {
            nullable && !(options.use_deephaven_nulls && has_sentinel(column.data_type()))
        })
        .collect()
}

/// The values of `data` at `offsets`; a column without data stays without data.
fn take(data: &ArrayRef, offsets: &RowSet) -> Result<ArrayRef, EncodeError> {
    if data.is_empty() || *offsets == RowSet::flat(data.len() as u64) {
        return Ok(data.clone());
    }
    let positions: UInt64Array = offsets.iter().map(|offset| offset as u64).collect();
    arrow::compute::take(data.as_ref(), &positions, None).map_err(error)
}

fn unapply(shifted: &RowSetShiftData, rows: &RowSet) -> Result<RowSet, EncodeError> {
    shifted.inverse().apply(rows).map_err(error)
}

fn bytes<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    bytes: &[u8],
) -> WIPOffset<flatbuffers::Vector<'a, i8>> {
    // the schema declares the byte vectors as signed
    let signed: Vec<i8> = bytes.iter().map(|byte| *byte as i8).collect();
    builder.create_vector(&signed)
}

fn rows<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    rows: &RowSet,
) -> WIPOffset<flatbuffers::Vector<'a, i8>> {
    bytes(builder, &encode_row_set(rows))
}

/// Wraps a finished `BarrageUpdateMetadata` as the `app_metadata` of a Flight message.
///
/// Behavior: the end of `BarrageMessageWriterImpl.SubView#getSubscriptionMetadata`
fn wrap(metadata: &[u8]) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let payload = bytes(&mut builder, metadata);
    let wrapper = BarrageMessageWrapper::create(
        &mut builder,
        &BarrageMessageWrapperArgs {
            magic: MAGIC,
            msg_type: BarrageMessageType::BarrageUpdateMetadata,
            msg_payload: Some(payload),
        },
    );
    builder.finish(wrapper, None);
    builder.finished_data().to_vec()
}

/// Negative and zero sizes mean the server's default.
pub(super) fn batch_options(batch_size: i32, max_message_size: i32) -> BatchOptions {
    BatchOptions {
        batch_size: usize::try_from(batch_size).unwrap_or(0),
        max_message_size: usize::try_from(max_message_size).unwrap_or(0),
        ..Default::default()
    }
}

/// The Flight messages one subscriber receives for `message`. `nullable` says, per column
/// of the table, whether its field is nullable in the schema the subscriber was sent.
///
/// A message that leaves the subscriber nothing to add or modify is still one record
/// batch of no rows: the metadata may remove rows, and a snapshot always tells the
/// viewport and columns now in effect.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl.SubView`
pub fn subscription_messages(
    message: &BarrageMessage,
    nullable: &[bool],
    view: &SubView<'_>,
) -> Result<Vec<FlightData>, EncodeError> {
    let included = &message.rows_included;
    let invert = |within: &RowSet, keys: &RowSet| within.invert(keys).map_err(error);
    // Behavior: BarrageMessageWriterImpl.SubView#isColumnIncluded
    let is_included = |column: usize| {
        view.subscribed_columns
            .is_none_or(|columns| columns.contains(column))
    };
    let no_rows = RowSet::empty();

    let client_included_rows;
    let client_included_offsets;
    let mut client_removed_rows = None;
    if view.is_full_subscription {
        // A full subscription is sent every removed row and tracks the table's row set.
        if let Some(keyspace_viewport) = view.keyspace_viewport {
            // growing towards the whole table: only the rows this snapshot adds
            client_included_rows = keyspace_viewport.intersect(included);
            client_included_offsets = invert(included, &client_included_rows)?;
        } else {
            // rows recorded for the viewports of others are left out
            client_included_rows = message.rows_added.clone();
            client_included_offsets = invert(included, &client_included_rows)?;
        }
    } else {
        let (Some(keyspace_viewport), Some(keyspace_viewport_prev)) =
            (view.keyspace_viewport, view.keyspace_viewport_prev)
        else {
            return Err(error("a viewport subscription has no viewport"));
        };
        let keyspace_included = keyspace_viewport.intersect(included);
        // a viewport client is told positions, and sees every included row as added
        client_included_rows = invert(keyspace_viewport, &keyspace_included)?;
        client_included_offsets = invert(included, &keyspace_included)?;

        // A row that left the viewport and came back within the message is not among the
        // added or removed rows, but is sent whole: the client removes it and adds it.
        let mut repainted = keyspace_included.clone();
        if !message.is_snapshot {
            repainted.remove(&message.rows_added);
            repainted = unapply(&message.shifted, &repainted)?;
        }
        repainted.retain(keyspace_viewport_prev);

        // rows that were in the viewport and no longer are
        let retained = if message.is_snapshot {
            keyspace_viewport.clone()
        } else {
            unapply(
                &message.shifted,
                &keyspace_viewport.minus(&message.rows_added),
            )?
        };
        let mut gone = keyspace_viewport_prev.minus(&retained);
        gone.insert(&repainted);
        client_removed_rows = Some(invert(keyspace_viewport_prev, &gone)?);
    }
    let included_count = client_included_offsets.size() as usize;

    // The modified rows the client sees, per column, and where their values are. A row
    // sent whole as included is not also sent as modified.
    let mut client_modded_rows = Vec::new();
    let mut mod_data = Vec::with_capacity(message.mod_columns.len());
    let mut mod_count = 0;
    for (index, column) in message.mod_columns.iter().enumerate() {
        // the message has data for every column any subscriber needs
        let data = match view.keyspace_viewport {
            _ if !is_included(index) => {
                if view.keyspace_viewport.is_some() {
                    client_modded_rows.push(RowSet::empty());
                }
                column.data.slice(0, 0)
            }
            None => column.data.clone(),
            Some(keyspace_viewport) => {
                let visible = keyspace_viewport
                    .intersect(&column.rows_modified)
                    .minus(included);
                let offsets = invert(&column.rows_modified, &visible)?;
                client_modded_rows.push(if view.is_full_subscription {
                    visible
                } else {
                    invert(keyspace_viewport, &visible)?
                });
                if offsets.is_empty() {
                    column.data.slice(0, 0)
                } else {
                    take(&column.data, &offsets)?
                }
            }
        };
        mod_count = mod_count.max(data.len());
        mod_data.push(data);
    }

    let has_viewport = view.keyspace_viewport.is_some();
    let mut builder = FlatBufferBuilder::new();
    let (effective_viewport, effective_column_set) = if message.is_snapshot {
        (
            view.viewport.map(|viewport| rows(&mut builder, viewport)),
            view.subscribed_columns
                .map(|columns| bytes(&mut builder, &columns.to_bytes())),
        )
    } else {
        (None, None)
    };
    let added_rows = if !view.is_full_subscription {
        rows(&mut builder, &client_included_rows)
    } else if message.is_snapshot && !view.is_initial_snapshot && !has_viewport {
        // a settled full subscription is not sent the whole row set again
        rows(&mut builder, &RowSet::empty())
    } else {
        rows(&mut builder, &message.rows_added)
    };
    let removed_rows = match &client_removed_rows {
        Some(removed) => rows(&mut builder, removed),
        None => rows(&mut builder, &message.rows_removed),
    };
    // shifts are sent to subscribers of the whole table only
    let shift_data = view
        .is_full_subscription
        .then(|| bytes(&mut builder, &encode_shift_data(&message.shifted)));
    // left out for a viewport client, and when it would repeat the added rows
    let added_rows_included = (view.is_full_subscription
        && (message.is_snapshot || client_included_rows != message.rows_added))
        .then(|| rows(&mut builder, &included.intersect(&client_included_rows)));
    let mut nodes = Vec::with_capacity(message.mod_columns.len());
    for (index, column) in message.mod_columns.iter().enumerate() {
        let modified = if has_viewport {
            &client_modded_rows[index]
        } else if !is_included(index) {
            &no_rows
        } else {
            &column.rows_modified
        };
        let modified_rows = Some(rows(&mut builder, modified));
        nodes.push(BarrageModColumnMetadata::create(
            &mut builder,
            &BarrageModColumnMetadataArgs { modified_rows },
        ));
    }
    let mod_column_nodes = Some(builder.create_vector(&nodes));
    let metadata = BarrageUpdateMetadata::create(
        &mut builder,
        &BarrageUpdateMetadataArgs {
            first_seq: message.first_seq,
            last_seq: message.last_seq,
            is_snapshot: message.is_snapshot,
            effective_viewport,
            effective_reverse_viewport: view.reverse_viewport,
            effective_column_set,
            added_rows: Some(added_rows),
            removed_rows: Some(removed_rows),
            shift_data,
            added_rows_included,
            mod_column_nodes,
            table_size: message.table_size,
        },
    );
    builder.finish(metadata, None);
    let metadata = wrap(builder.finished_data());

    let nullable = validity(message, nullable, view.options);
    let as_lists = view.options.columns_as_list;
    let add_data = message
        .add_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if is_included(index) {
                take(column, &client_included_offsets)
            } else {
                Ok(column.slice(0, 0))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if included_count == 0 && mod_count == 0 {
        return record_batch_messages_with(
            &add_data,
            &nullable,
            0,
            batch(view.options),
            metadata,
            true,
            as_lists,
        );
    }
    // the metadata goes with the first batch, whichever kind that is
    let (with_adds, with_mods) = if included_count > 0 {
        (metadata, Vec::new())
    } else {
        (Vec::new(), metadata)
    };
    let mut messages = record_batch_messages_with(
        &add_data,
        &nullable,
        included_count,
        batch(view.options),
        with_adds,
        false,
        as_lists,
    )?;
    messages.extend(record_batch_messages_with(
        &mod_data,
        &nullable,
        mod_count,
        batch(view.options),
        with_mods,
        false,
        as_lists,
    )?);
    Ok(messages)
}

/// The `app_metadata` of a snapshot that is not part of a subscription.
///
/// Behavior: `BarrageMessageWriterImpl.SnapshotView#getSnapshotMetadata`
fn snapshot_metadata(
    message: &BarrageMessage,
    view: &SnapshotView<'_>,
    client_added_rows: &RowSet,
) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let effective_viewport = view.viewport.map(|viewport| rows(&mut builder, viewport));
    let effective_column_set = view
        .snapshot_columns
        .map(|columns| bytes(&mut builder, &columns.to_bytes()));
    let added_rows = Some(rows(&mut builder, &message.rows_added));
    // a snapshot has no shifts, but the structure is sent
    let shift_data = Some(bytes(&mut builder, &encode_shift_data(&message.shifted)));
    let added_rows_included = (message.is_snapshot || *client_added_rows != message.rows_added)
        .then(|| {
            rows(
                &mut builder,
                &message.rows_included.intersect(client_added_rows),
            )
        });
    let metadata = BarrageUpdateMetadata::create(
        &mut builder,
        &BarrageUpdateMetadataArgs {
            first_seq: message.first_seq,
            last_seq: message.last_seq,
            is_snapshot: message.is_snapshot,
            effective_viewport,
            effective_reverse_viewport: view.reverse_viewport,
            effective_column_set,
            added_rows,
            removed_rows: None,
            shift_data,
            added_rows_included,
            mod_column_nodes: None,
            // the snapshot writer does not say the size
            table_size: 0,
        },
    );
    builder.finish(metadata, None);
    wrap(builder.finished_data())
}

/// The Flight messages of a snapshot for a `DoGet` or a `BarrageSnapshotRequest`. Without
/// rows it is still one record batch, which carries the metadata.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl.SnapshotView`
pub fn snapshot_messages(
    message: &BarrageMessage,
    nullable: &[bool],
    view: &SnapshotView<'_>,
) -> Result<Vec<FlightData>, EncodeError> {
    let (client_added_rows, offsets) = match view.keyspace_viewport {
        Some(keyspace_viewport) => {
            let rows = keyspace_viewport.intersect(&message.rows_included);
            let offsets = message.rows_included.invert(&rows).map_err(error)?;
            (rows, offsets)
        }
        None => (
            message.rows_added.clone(),
            RowSet::flat(message.rows_added.size()),
        ),
    };
    let metadata = snapshot_metadata(message, view, &client_added_rows);
    let nullable = validity(message, nullable, view.options);
    // Behavior: BarrageMessageWriterImpl.SnapshotView#isColumnIncluded
    let data = message
        .add_columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if view
                .snapshot_columns
                .is_none_or(|columns| columns.contains(index))
            {
                take(column, &offsets)
            } else {
                Ok(column.slice(0, 0))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    record_batch_messages_with(
        &data,
        &nullable,
        offsets.size() as usize,
        batch(view.options),
        metadata,
        true,
        false,
    )
}
