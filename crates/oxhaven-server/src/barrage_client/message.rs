//! The Flight messages of a Barrage stream, put together into the snapshots and updates
//! they carry.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::Schema;
use arrow::ipc;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageMessageWrapper, BarrageUpdateMetadata, MAGIC,
};
use oxhaven_proto::flight::FlightData;

use super::decode::{ColumnReader, Cursor};
use super::error::{not_ported, protocol, ClientError};
use crate::barrage::codec::{decode_row_set, decode_shift_data, ColumnSet};

/// The modified rows of one column and their new values, in row order.
#[derive(Clone, Debug)]
pub struct ModifiedColumn {
    pub rows_modified: RowSet,
    /// One value per modified row.
    pub data: ArrayRef,
}

/// A snapshot of a remote table, or its change over one or more of the server's update
/// cycles, as one client was sent it.
///
/// Port of `io.deephaven.engine.table.impl.util.BarrageMessage` as
/// `io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl` fills it.
#[derive(Clone, Debug)]
pub struct RemoteMessage {
    pub first_seq: i64,
    pub last_seq: i64,
    pub is_snapshot: bool,
    /// The viewport, in positions, the server says it is honouring; only a snapshot says.
    /// `None` in a snapshot means the whole table.
    pub effective_viewport: Option<RowSet>,
    pub effective_reverse_viewport: bool,
    /// The columns the server says it is sending; only a snapshot says, and `None` there
    /// means all of them.
    pub effective_columns: Option<ColumnSet>,
    pub rows_added: RowSet,
    pub rows_removed: RowSet,
    pub shifted: RowSetShiftData,
    /// The rows `add_columns` has data for.
    pub rows_included: RowSet,
    /// Per column of the table, the cells of `rows_included` as the engine stores them,
    /// or no cells for a column the server left out.
    pub add_columns: Vec<ArrayRef>,
    /// Per column the server described, its modified rows and their cells.
    pub mod_columns: Vec<ModifiedColumn>,
    pub table_size: i64,
}

impl RemoteMessage {
    /// Whether this is a snapshot that says the server sends the whole table and each of
    /// its `columns` columns: how a server acknowledges a full subscription.
    ///
    /// Behavior: `io.deephaven.client.impl.BarrageSubscriptionImpl.CheckForCompletion#viewportChanged`
    pub fn is_whole_table(&self, columns: usize) -> bool {
        self.is_snapshot
            && self.effective_viewport.is_none()
            && self
                .effective_columns
                .as_ref()
                .is_none_or(|sent| sent.cardinality() == columns)
    }
}

/// What the client asked of the encoding, as far as reading depends on it.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub(super) struct ReadOptions {
    pub(super) columns_as_list: bool,
}

/// A message whose record batches have not all arrived.
struct Partial {
    message: RemoteMessage,
    add_parts: Vec<Vec<ArrayRef>>,
    mod_parts: Vec<Vec<ArrayRef>>,
    add_read: u64,
    add_total: u64,
    mod_read: u64,
    mod_total: u64,
}

/// Reads the messages of one `DoExchange` stream.
///
/// The first record batch of a message carries its `BarrageUpdateMetadata`. The batches
/// that follow hold the added rows until all of them have arrived, then the modified
/// rows, where each column has as many rows as it has left.
///
/// Port of `io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl`.
pub(super) struct MessageReader {
    definition: TableDefinition,
    options: ReadOptions,
    /// Made from the schema of the fetched table.
    readers: Option<Vec<ColumnReader>>,
    partial: Option<Partial>,
}

fn rows(bytes: Option<flatbuffers::Vector<'_, i8>>) -> Result<RowSet, ClientError> {
    match bytes {
        None => Ok(RowSet::empty()),
        Some(bytes) => decode_row_set(bytes.bytes()).map_err(protocol),
    }
}

fn concat(parts: &[ArrayRef], empty: impl FnOnce() -> ArrayRef) -> Result<ArrayRef, ClientError> {
    match parts {
        [] => Ok(empty()),
        [one] => Ok(one.clone()),
        many => {
            let parts: Vec<&dyn Array> = many.iter().map(|part| part.as_ref()).collect();
            arrow::compute::concat(&parts).map_err(protocol)
        }
    }
}

impl MessageReader {
    /// A reader of the messages of a table whose fetched schema is `schema` and whose
    /// definition is `definition`.
    ///
    /// Behavior: the column types `io.deephaven.client.impl.BarrageSubscriptionImpl` hands
    /// its reader come from the schema of the table handle, not from the stream
    pub(super) fn new(
        definition: TableDefinition,
        schema: &Schema,
        options: ReadOptions,
    ) -> Result<Self, ClientError> {
        let mut reader = MessageReader {
            definition,
            options,
            readers: None,
            partial: None,
        };
        reader.configure(schema)?;
        Ok(reader)
    }

    /// The Arrow type of the cells of each column as the engine stores them.
    pub(super) fn stored_types(&self) -> Vec<arrow::datatypes::DataType> {
        self.readers
            .iter()
            .flatten()
            .map(|reader| reader.empty().data_type().clone())
            .collect()
    }

    /// Whether a message has begun and not ended.
    pub(super) fn is_mid_message(&self) -> bool {
        self.partial.is_some()
    }

    /// The readers of the columns, from the schema the stream declares.
    fn configure(&mut self, schema: &Schema) -> Result<(), ClientError> {
        let columns = self.definition.columns();
        if schema.fields().len() != columns.len() {
            return Err(protocol(format!(
                "the stream's schema has {} columns, the table has {}",
                schema.fields().len(),
                columns.len()
            )));
        }
        let readers = schema
            .fields()
            .iter()
            .zip(columns)
            .map(|(field, column)| {
                ColumnReader::new(field, &column.column_type, self.options.columns_as_list)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.readers = Some(readers);
        Ok(())
    }

    /// Begins the message `metadata` describes.
    ///
    /// Behavior: the `BarrageUpdateMetadata` branch of `BarrageMessageReaderImpl#safelyParseFrom`
    fn begin(&mut self, metadata: BarrageUpdateMetadata<'_>) -> Result<(), ClientError> {
        if let Some(partial) = &self.partial {
            return Err(protocol(format!(
                "Previous message was not complete; pending {} add rows and {} mod rows",
                partial.add_total - partial.add_read,
                partial.mod_total.saturating_sub(partial.mod_read)
            )));
        }
        let is_snapshot = metadata.is_snapshot();
        let (effective_viewport, effective_columns) = if is_snapshot {
            (
                metadata
                    .effective_viewport()
                    .map(|bytes| decode_row_set(bytes.bytes()).map_err(protocol))
                    .transpose()?,
                metadata
                    .effective_column_set()
                    .map(|bytes| ColumnSet::from_bytes(bytes.bytes())),
            )
        } else {
            (None, None)
        };
        let rows_added = rows(metadata.added_rows())?;
        let rows_included = match metadata.added_rows_included() {
            Some(bytes) => decode_row_set(bytes.bytes()).map_err(protocol)?,
            None => rows_added.clone(),
        };
        let shifted = match metadata.shift_data() {
            Some(bytes) => decode_shift_data(bytes.bytes()).map_err(protocol)?,
            None => RowSetShiftData::empty(),
        };
        let columns = self.definition.columns().len();
        let mut mod_columns = Vec::new();
        let mut mod_total = 0;
        for node in metadata.mod_column_nodes().into_iter().flatten() {
            if mod_columns.len() == columns {
                return Err(protocol(
                    "the message describes modified rows for more columns than the table has",
                ));
            }
            let rows_modified = rows(node.modified_rows())?;
            mod_total = mod_total.max(rows_modified.size());
            mod_columns.push(ModifiedColumn {
                rows_modified,
                data: arrow::array::new_empty_array(&arrow::datatypes::DataType::Null),
            });
        }
        // Behavior: with `columnsAsList` and no columns there is nothing to count rows by
        let add_total = if self.options.columns_as_list && columns == 0 {
            0
        } else {
            rows_included.size()
        };
        self.partial = Some(Partial {
            add_parts: vec![Vec::new(); columns],
            mod_parts: vec![Vec::new(); mod_columns.len()],
            add_read: 0,
            add_total,
            mod_read: 0,
            mod_total,
            message: RemoteMessage {
                first_seq: metadata.first_seq(),
                last_seq: metadata.last_seq(),
                is_snapshot,
                effective_viewport,
                effective_reverse_viewport: metadata.effective_reverse_viewport(),
                effective_columns,
                rows_added,
                rows_removed: rows(metadata.removed_rows())?,
                shifted,
                rows_included,
                add_columns: Vec::new(),
                mod_columns,
                table_size: metadata.table_size(),
            },
        });
        Ok(())
    }

    /// Reads one record batch into the message that has begun.
    ///
    /// Behavior: the record batch branch of `BarrageMessageReaderImpl#safelyParseFrom`
    fn batch(
        &mut self,
        batch: ipc::RecordBatch<'_>,
        body: &[u8],
        version: ipc::MetadataVersion,
    ) -> Result<(), ClientError> {
        let Some(partial) = &mut self.partial else {
            return Err(protocol(
                "Missing app metadata tag; cannot decode using BarrageStreamReader",
            ));
        };
        let Some(readers) = &self.readers else {
            return Err(protocol("a record batch arrived before the schema"));
        };
        let as_list = self.options.columns_as_list;
        let mut cursor = Cursor::new(batch, body, version)?;
        let length = cursor.length() as u64;
        if partial.add_read < partial.add_total {
            let remaining = partial.add_total - partial.add_read;
            if !as_list && length > remaining {
                return Err(protocol(
                    "Batch length exceeded the expected number of rows from app metadata",
                ));
            }
            let mut first = 0;
            for (index, reader) in readers.iter().enumerate() {
                let cells = reader.read(&mut cursor)?;
                if index == 0 {
                    first = cells.len() as u64;
                }
                partial.add_parts[index].push(cells);
            }
            // Behavior: with `columnsAsList` the batch has one row; the first column says
            // how many rows arrived
            partial.add_read += if as_list && !readers.is_empty() {
                first
            } else {
                length
            };
        } else {
            let mut most = 0;
            for (index, parts) in partial.mod_parts.iter_mut().enumerate() {
                let cells = readers[index].read(&mut cursor)?;
                let modified = partial.message.mod_columns[index].rows_modified.size();
                let expected = modified.saturating_sub(partial.mod_read).min(length);
                if !as_list && cells.len() as u64 != expected {
                    return Err(protocol(format!(
                        "column {index} has {} modified rows in a batch, expected {expected}",
                        cells.len()
                    )));
                }
                most = most.max(cells.len() as u64);
                parts.push(cells);
            }
            partial.mod_read += most;
        }
        Ok(())
    }

    /// The finished message, if every row it announced has arrived.
    fn finish(&mut self) -> Result<Option<RemoteMessage>, ClientError> {
        let done = self.partial.as_ref().is_some_and(|partial| {
            partial.add_read == partial.add_total && partial.mod_read == partial.mod_total
        });
        if !done {
            return Ok(None);
        }
        let Some(mut partial) = self.partial.take() else {
            return Ok(None);
        };
        let Some(readers) = &self.readers else {
            return Err(protocol("a message ended before the schema arrived"));
        };
        let included = partial.message.rows_included.size() as usize;
        for (index, parts) in partial.add_parts.iter().enumerate() {
            let cells = concat(parts, || readers[index].empty())?;
            // a column the server leaves out has no rows at all
            if cells.len() != included && !cells.is_empty() {
                return Err(protocol(format!(
                    "column {index} has {} added rows, the message announced {included}",
                    cells.len()
                )));
            }
            partial.message.add_columns.push(cells);
        }
        for (index, parts) in partial.mod_parts.iter().enumerate() {
            let cells = concat(parts, || readers[index].empty())?;
            let column = &mut partial.message.mod_columns[index];
            if cells.len() as u64 != column.rows_modified.size() {
                return Err(protocol(format!(
                    "column {index} has {} modified rows, the message announced {}",
                    cells.len(),
                    column.rows_modified.size()
                )));
            }
            column.data = cells;
        }
        Ok(Some(partial.message))
    }

    /// Takes the next Flight message of the stream. Returns the snapshot or update it
    /// completes, if it completes one.
    ///
    /// Behavior: `BarrageMessageReaderImpl#safelyParseFrom`
    pub(super) fn accept(
        &mut self,
        data: &FlightData,
    ) -> Result<Option<RemoteMessage>, ClientError> {
        if !data.app_metadata.is_empty() {
            let wrapper = flatbuffers::root::<BarrageMessageWrapper>(&data.app_metadata)
                .map_err(|problem| protocol(format!("unreadable app_metadata: {problem}")))?;
            // Java logs and skips metadata that is not Barrage's
            if wrapper.magic() == MAGIC
                && wrapper.msg_type() == BarrageMessageType::BarrageUpdateMetadata
            {
                let payload = wrapper
                    .msg_payload()
                    .ok_or_else(|| protocol("a BarrageUpdateMetadata without a payload"))?;
                let metadata = flatbuffers::root::<BarrageUpdateMetadata>(payload.bytes())
                    .map_err(|problem| {
                        protocol(format!("unreadable BarrageUpdateMetadata: {problem}"))
                    })?;
                self.begin(metadata)?;
            }
        }
        if data.data_header.is_empty() {
            return Err(protocol("Missing metadata header; cannot decode body"));
        }
        let header = ipc::root_as_message(&data.data_header)
            .map_err(|problem| protocol(format!("unreadable Arrow message: {problem}")))?;
        match header.header_type() {
            // Behavior: Java returns early on a schema message; the columns are read as
            // the schema of the fetched table says
            ipc::MessageHeader::Schema => Ok(None),
            ipc::MessageHeader::RecordBatch => {
                let batch = header
                    .header_as_record_batch()
                    .ok_or_else(|| protocol("the record batch has no header"))?;
                self.batch(batch, &data.data_body, header.version())?;
                self.finish()
            }
            ipc::MessageHeader::DictionaryBatch => Err(not_ported(
                "io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl#safelyParseFrom",
                "dictionary batches",
            )),
            _ => Err(protocol(
                "Only know how to decode Schema/BarrageRecordBatch messages",
            )),
        }
    }
}
