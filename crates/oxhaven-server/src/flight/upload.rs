//! Arrow data a client sends: a schema to a table definition, record batches to a table.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType};
use arrow::ipc;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::uploaded_table;
use tonic::Status;

use super::wire::remember_uploaded;
use crate::schema::{column_type_for_java_name, TYPE_KEY};

/// Why data a client sent was refused.
#[derive(Clone, Debug)]
pub enum UploadError {
    /// A failure Java reports to the client with this status.
    Status(Status),
    /// A failure Java logs and reports to the client by an error id only.
    Internal(String),
}

impl From<Status> for UploadError {
    fn from(status: Status) -> Self {
        UploadError::Status(status)
    }
}

/// The status of something Java does and this server does not do yet. A client can reach
/// it, so it is a status and not the panic of `unported!`.
pub fn not_ported(java: &str, what: impl std::fmt::Display) -> Status {
    Status::unimplemented(format!("not ported yet: {java}: {what}"))
}

const GET_DEFAULT_TYPE: &str = "io.deephaven.extensions.barrage.util.BarrageUtil#getDefaultType";
const CONVERT_ARROW_SCHEMA: &str =
    "io.deephaven.extensions.barrage.util.BarrageUtil#convertArrowSchema";

/// The column type Java gives a field that does not name one.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#getDefaultType(Field, Class)`
fn default_type(field: &Field) -> Result<ColumnType, Status> {
    Ok(match field.data_type() {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int8 => ColumnType::Byte,
        DataType::Int16 => ColumnType::Short,
        DataType::Int32 => ColumnType::Int,
        DataType::Int64 => ColumnType::Long,
        DataType::UInt16 => ColumnType::Char,
        DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Utf8 => ColumnType::String,
        DataType::Timestamp(_, zone) if zone.as_deref().is_none_or(|zone| zone == "UTC") => {
            ColumnType::Instant
        }
        other => {
            return Err(not_ported(
                GET_DEFAULT_TYPE,
                format_args!("field '{}' of Arrow type {other}", field.name()),
            ))
        }
    })
}

/// Whether a column name needs no change to be a legal one. Java renames the others.
fn is_plain_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && characters.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// The definition of the table a client describes with an Arrow schema. A field's
/// `deephaven:type` names its column type; without it the Arrow type decides.
///
/// Java reads many more combinations than there are column types here, converting as it
/// reads: wider and narrower integers, timestamps in other units, dates, decimals, lists,
/// dictionaries. Those answer `UNIMPLEMENTED` and name what is missing; so does a column
/// name that Java would rename to make it legal.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#convertArrowSchema`
pub fn convert_schema(schema: &Schema) -> Result<TableDefinition, Status> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        if !is_plain_identifier(field.name()) {
            return Err(not_ported(
                "io.deephaven.api.util.NameValidator#legalizeColumnName",
                format_args!("column name '{}'", field.name()),
            ));
        }
        let column_type = match field.metadata().get(TYPE_KEY).map(String::as_str) {
            None => default_type(field)?,
            // Java turns the primitive into the boxed type, which can hold a null
            Some("boolean") => ColumnType::Boolean,
            Some(name) => column_type_for_java_name(name).ok_or_else(|| {
                not_ported(
                    CONVERT_ARROW_SCHEMA,
                    format_args!("field '{}' of type {name}", field.name()),
                )
            })?,
        };
        let direct = match (&column_type, field.data_type()) {
            (ColumnType::Instant, DataType::Timestamp(TimeUnit::Nanosecond, zone)) => {
                zone.as_deref().is_none_or(|zone| zone == "UTC")
            }
            (column_type, arrow_type) => column_type.arrow_type() == *arrow_type,
        };
        if !direct {
            return Err(not_ported(
                "io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory#newReader",
                format_args!(
                    "field '{}': reading Arrow type {} as {column_type:?}",
                    field.name(),
                    field.data_type()
                ),
            ));
        }
        columns.push(ColumnDefinition::new(field.name().clone(), column_type));
    }
    TableDefinition::new(columns).map_err(|duplicate| {
        // Java: an IllegalArgumentException out of TableDefinition.of, shown by its error id
        Status::invalid_argument(duplicate.to_string())
    })
}

/// The schema in an encapsulated IPC message, as `CreateInputTableRequest.schema` and
/// `ExportedTableCreationResponse.schema_header` hold it.
///
/// Behavior: `io.deephaven.proto.flight.util.SchemaHelper#flatbufSchema(ByteBuffer)`
pub fn schema_from_ipc_bytes(bytes: &[u8]) -> Result<Schema, Status> {
    let message = bytes
        .get(8..)
        .ok_or_else(|| Status::invalid_argument("Not enough bytes for Message/Schema"))?;
    schema_of_message(message).map_err(|problem| Status::invalid_argument(problem.to_owned()))
}

/// The schema a flatbuffer `Message` carries.
fn schema_of_message(message: &[u8]) -> Result<Schema, &'static str> {
    let message = ipc::root_as_message(message).map_err(|_| "not an Arrow message")?;
    let schema = message
        .header_as_schema()
        .ok_or("Expected header type Schema")?;
    match catch_unwind(AssertUnwindSafe(|| ipc::convert::try_fb_to_schema(schema))) {
        Ok(Ok(schema)) => Ok(schema),
        _ => Err("not a readable Arrow schema"),
    }
}

/// A schema a client sent, with the table definition it describes.
struct Layout {
    definition: TableDefinition,
    /// The fields as the client sent them: what its record batches are read with.
    sent: SchemaRef,
}

/// Builds a table from the Arrow messages of one upload, in the order they arrive.
///
/// Behavior: `io.deephaven.extensions.barrage.util.ArrowToTableConverter` and the message
/// handling of `io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver#processMessage`
#[derive(Default)]
pub struct TableUpload {
    /// The schema of the latest schema message.
    schema: Option<Schema>,
    /// Fixed by the first message after a schema; a later schema message changes nothing.
    layout: Option<Layout>,
    batches: Vec<RecordBatch>,
}

impl TableUpload {
    /// Takes one Arrow message: the flatbuffer `Message` and its body. Returns whether it
    /// was a record batch, which the caller acknowledges.
    pub fn message(&mut self, header: &[u8], body: &[u8]) -> Result<bool, UploadError> {
        let message = ipc::root_as_message(header).map_err(|problem| {
            UploadError::Internal(format!("unreadable Arrow message: {problem}"))
        })?;
        if message.header_type() == ipc::MessageHeader::Schema {
            self.schema = Some(schema_of_message(header).map_err(|problem| {
                UploadError::Internal(format!("unreadable Arrow schema: {problem}"))
            })?);
            return Ok(false);
        }
        self.settle_layout()?;
        let Some(layout) = &self.layout else {
            return Err(Status::invalid_argument(
                "Schema must be processed before record-batch messages",
            )
            .into());
        };
        match message.header_type() {
            ipc::MessageHeader::RecordBatch => {}
            ipc::MessageHeader::DictionaryBatch => {
                return Err(not_ported(
                    "io.deephaven.extensions.barrage.util.ArrowToTableConverter#addDictionaryBatch",
                    "dictionary batches",
                )
                .into())
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "Only schema/record-batch/dictionary-batch messages supported, instead got {}",
                    other.variant_name().unwrap_or("an unknown message")
                ))
                .into())
            }
        }
        let batch = message
            .header_as_record_batch()
            .ok_or_else(|| UploadError::Internal("the record batch has no header".into()))?;
        self.batches
            .push(read_batch(batch, body, &layout.sent, message.version())?);
        Ok(true)
    }

    /// Fixes the layout from the schema seen so far, if that has not happened yet.
    ///
    /// Behavior: `ArrowToTableConverter#configureWithSchema`
    fn settle_layout(&mut self) -> Result<(), Status> {
        if self.layout.is_none() {
            if let Some(schema) = &self.schema {
                self.layout = Some(Layout {
                    definition: convert_schema(schema)?,
                    sent: Arc::new(schema.clone()),
                });
            }
        }
        Ok(())
    }

    /// The table of everything received: a static, flat table. Without a record batch it
    /// has no rows.
    ///
    /// A record batch of no rows adds nothing. In Java such a batch inserts the row range
    /// `0..-1` into the table's row set; nothing wrong was seen in what the reference
    /// server then returned (the `put_zero_*` fixtures), and this table is right by
    /// construction.
    pub fn finish(mut self) -> Result<Arc<dyn Table>, UploadError> {
        self.settle_layout()?;
        let Some(layout) = self.layout else {
            return Err(Status::invalid_argument("Result flight schema never provided").into());
        };
        let fields: Vec<Field> = layout
            .definition
            .columns()
            .iter()
            .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
            .collect();
        let stored = Arc::new(Schema::new(fields));
        let batches = self
            .batches
            .iter()
            .map(|batch| stored_batch(batch, &stored))
            .collect::<Result<Vec<_>, _>>()?;
        let table: Arc<dyn Table> = uploaded_table(layout.definition, &batches)
            .map_err(|failure| UploadError::Internal(failure.message))?;
        remember_uploaded(&table, layout.sent.as_ref().clone());
        Ok(table)
    }
}

/// Decodes one record batch. Every buffer must lie inside the body: the Arrow reader
/// trusts the header on that.
fn read_batch(
    batch: ipc::RecordBatch<'_>,
    body: &[u8],
    schema: &SchemaRef,
    version: ipc::MetadataVersion,
) -> Result<RecordBatch, UploadError> {
    let internal = |problem: String| UploadError::Internal(problem);
    let inside = batch.buffers().is_some_and(|buffers| {
        buffers.iter().all(|buffer| {
            buffer.offset() >= 0
                && buffer.length() >= 0
                && buffer
                    .offset()
                    .checked_add(buffer.length())
                    .is_some_and(|end| end as u64 <= body.len() as u64)
        })
    });
    if !inside || batch.length() < 0 {
        return Err(internal(
            "the record batch describes buffers outside its body".into(),
        ));
    }
    // a copy, because the reader wants the alignment of a fresh allocation
    let body = Buffer::from_vec(body.to_vec());
    let read = catch_unwind(AssertUnwindSafe(|| {
        ipc::reader::read_record_batch(
            &body,
            batch,
            schema.clone(),
            &HashMap::new(),
            None,
            &version,
        )
    }));
    match read {
        Ok(Ok(batch)) => Ok(batch),
        Ok(Err(problem)) => Err(internal(format!("unreadable record batch: {problem}"))),
        Err(_) => Err(internal("unreadable record batch".into())),
    }
}

/// A batch as the table stores it: each column in the Arrow type of its column type, and
/// null wherever Java would hold its null sentinel, which is where the client sent a null
/// and also where it sent the sentinel value itself.
///
/// Behavior: the readers of `io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory`,
/// which write `QueryConstants` nulls into the chunks the table's columns are filled from
fn stored_batch(batch: &RecordBatch, stored: &SchemaRef) -> Result<RecordBatch, UploadError> {
    let internal = |problem: String| UploadError::Internal(problem);
    let columns = batch
        .columns()
        .iter()
        .zip(stored.fields())
        .map(|(column, field)| {
            let column: ArrayRef = match column.as_primitive_opt::<TimestampNanosecondType>() {
                // Only the time zone of a timestamp can differ. The values are nanoseconds
                // since the epoch whatever the zone says, as Java reads them.
                Some(instants) if column.data_type() != field.data_type() => {
                    Arc::new(instants.clone().with_timezone("UTC"))
                }
                _ => column.clone(),
            };
            sentinels_to_nulls(&column).map_err(|failure| internal(failure.message))
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        stored.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|problem| internal(problem.to_string()))
}
