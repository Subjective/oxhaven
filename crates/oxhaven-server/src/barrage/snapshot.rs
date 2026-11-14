//! Snapshots that are not part of a subscription: `DoGet` and `BarrageSnapshotRequest`.

use std::sync::Arc;

use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::Table;
use oxhaven_proto::flight::FlightData;

use super::codec::ColumnSet;
use super::producer::{construct_snapshot, MAX_SNAPSHOT_CELL_COUNT, MIN_SNAPSHOT_CELL_COUNT};
use super::writer::{snapshot_messages, SnapshotView, WireOptions};
use crate::session::ExportError;

fn internal(problem: impl std::fmt::Display) -> ExportError {
    ExportError::Internal(problem.to_string())
}

/// The record batches of a snapshot of `table`: of `columns` (`None` for all) and of the
/// rows at the positions of `viewport` (`None` for all), counted from the last row if
/// `reverse_viewport`. `nullable` is per column of the table, as in the schema sent.
///
/// A refreshing table is read at once, under its update graph's lock, and gives one
/// message even without rows. A static table is read in parts: every part but the last
/// tells the positions sent so far as its viewport, and a table without rows gives
/// nothing.
///
/// What differs from Java:
///
/// - The parts of a static table do not depend on how long reading took: the first is
///   Java's minimum number of cells, later ones its maximum.
/// - Positions past the end of a static table are not read. Java reads them part by
///   part, each an empty message, which for a viewport such as "everything up to
///   2^62" never ends.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#createAndSendSnapshot`,
/// `BarrageUtil#createAndSendStaticSnapshot`
pub fn snapshot_request_messages(
    table: &Arc<dyn Table>,
    nullable: &[bool],
    columns: Option<&ColumnSet>,
    viewport: Option<&RowSet>,
    reverse_viewport: bool,
    options: WireOptions,
) -> Result<Vec<FlightData>, ExportError> {
    let empty = RowSet::empty();
    let graph = table.update_graph().filter(|_| table.is_refreshing());
    if let Some(graph) = graph {
        let positions = viewport.map(|viewport| {
            if reverse_viewport {
                (&empty, viewport)
            } else {
                (viewport, &empty)
            }
        });
        let message = locked(graph.as_ref(), || {
            construct_snapshot(table, columns, positions)
        })
        .map_err(internal)?;
        let keyspace_viewport = viewport.map(|viewport| {
            message
                .rows_added
                .sub_set_for_positions(viewport, reverse_viewport)
        });
        return snapshot_messages(
            &message,
            nullable,
            &SnapshotView {
                options,
                viewport,
                reverse_viewport,
                keyspace_viewport: keyspace_viewport.as_ref(),
                snapshot_columns: columns,
            },
        )
        .map_err(internal);
    }

    let row_set = table.row_set();
    let all = RowSet::flat(row_set.size());
    let target = match viewport {
        None => all,
        Some(viewport) if !reverse_viewport => viewport.intersect(&all),
        // a reverse viewport as the forward positions of the same rows
        Some(viewport) => row_set
            .invert(&row_set.sub_set_for_positions(viewport, true))
            .map_err(internal)?,
    };
    let column_count = match columns {
        Some(columns) => columns.cardinality(),
        None => table.definition().columns().len(),
    }
    .max(1) as u64;

    let mut parts = Vec::new();
    let mut start = 0;
    let mut cell_count = MIN_SNAPSHOT_CELL_COUNT;
    while start < target.size() {
        let end = start + (cell_count / column_count).max(1);
        parts.push(target.sub_set_by_position_range(start, end));
        start = end;
        cell_count = MAX_SNAPSHOT_CELL_COUNT;
    }
    if parts.is_empty() && viewport.is_some_and(|viewport| !viewport.is_empty()) {
        // a viewport past the end of the table: Java's message without rows
        parts.push(RowSet::empty());
    }

    let mut messages = Vec::new();
    let mut sent = RowSet::empty();
    let last = parts.len().saturating_sub(1);
    for (index, part) in parts.iter().enumerate() {
        sent.insert(part);
        let message = construct_snapshot(table, columns, Some((part, &empty))).map_err(internal)?;
        let view = if index < last {
            SnapshotView {
                options,
                viewport: Some(&sent),
                reverse_viewport: false,
                keyspace_viewport: Some(&message.rows_included),
                snapshot_columns: columns,
            }
        } else {
            SnapshotView {
                options,
                viewport,
                reverse_viewport,
                keyspace_viewport: Some(&message.rows_included),
                snapshot_columns: columns,
            }
        };
        messages.extend(snapshot_messages(&message, nullable, &view).map_err(internal)?);
    }
    Ok(messages)
}
