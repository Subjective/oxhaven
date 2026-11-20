//! Reads the columns of a Barrage record batch and turns them into the cells the engine
//! stores.
//!
//! A Barrage record batch is not an Arrow record batch: the columns of a batch of
//! modified rows each have their own number of rows, and a column the client did not ask
//! for has none. So each column is read by itself, with Arrow's reader, from the field
//! nodes and buffers that belong to it.

use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BinaryArray, Date32Array, Int32Array, Int8Array,
    ListArray, PrimitiveArray, StringArray, StructArray, TimestampNanosecondArray,
};
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, DurationNanosecondType, Field,
    IntervalYearMonthType, Schema, Time64NanosecondType, TimestampNanosecondType,
};
use arrow::ipc;
use flatbuffers::FlatBufferBuilder;
use oxhaven_column::java_values::minimal;
use oxhaven_column::sentinel::{sentinels_to_nulls, NULL_INT, NULL_LONG};
use oxhaven_contracts::schema::ColumnType;

use super::error::{not_ported, protocol, ClientError};
use super::schema::check_readable;

/// How one column of a table arrives and what it is stored as.
#[derive(Clone, Debug)]
pub(super) struct ColumnReader {
    column_type: ColumnType,
    /// The field the table's schema declares; with `as_list`, the list that wraps it.
    sent: Arc<Schema>,
    /// The column arrives as the one element of a list.
    ///
    /// Behavior: `io.deephaven.extensions.barrage.chunk.SingleElementListHeaderReader`
    as_list: bool,
    nodes: usize,
    buffers: usize,
}

/// How many field nodes and buffers a column of `wire` has in a record batch.
fn shape(wire: &DataType) -> Result<(usize, usize), ClientError> {
    Ok(match wire {
        DataType::Utf8 | DataType::Binary => (1, 3),
        DataType::List(element) => {
            let (nodes, buffers) = shape(element.data_type())?;
            (nodes + 1, buffers + 2)
        }
        DataType::Boolean
        | DataType::Int8
        | DataType::UInt16
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => (1, 2),
        other => {
            return Err(not_ported(
                "io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory#newReader",
                format_args!("a column of Arrow type {other}"),
            ))
        }
    })
}

impl ColumnReader {
    /// The reader of a column of `column_type` that the schema of the table declares as
    /// `field`. With `as_list` the column arrives wrapped in a list of one element.
    ///
    /// Behavior: `DefaultChunkReaderFactory#newReaderPojo`, and the `columnsAsList`
    /// branch of `io.deephaven.extensions.barrage.util.BarrageMessageReaderImpl#safelyParseFrom`
    pub(super) fn new(
        field: &Field,
        column_type: &ColumnType,
        as_list: bool,
    ) -> Result<Self, ClientError> {
        check_readable(field, column_type, field.data_type())?;
        // the metadata of the field is not needed to read it
        let own = Field::new(field.name(), field.data_type().clone(), true);
        let sent = if as_list {
            Field::new(
                field.name(),
                DataType::List(Arc::new(Field::new("", own.data_type().clone(), true))),
                true,
            )
        } else {
            own
        };
        let (nodes, buffers) = shape(sent.data_type())?;
        Ok(ColumnReader {
            column_type: column_type.clone(),
            sent: Arc::new(Schema::new(vec![sent])),
            as_list,
            nodes,
            buffers,
        })
    }

    /// Reads this column off the front of what `cursor` has left of a record batch, as
    /// the cells the engine stores.
    pub(super) fn read(&self, cursor: &mut Cursor<'_>) -> Result<ArrayRef, ClientError> {
        let sent = cursor.column(&self.sent, self.nodes, self.buffers)?;
        let own = if self.as_list {
            let wrapper = sent
                .as_list_opt::<i32>()
                .ok_or_else(|| protocol("a column sent as a list is not a list"))?;
            if wrapper.len() != 1 || wrapper.is_null(0) {
                return Err(protocol(format!(
                    "a column sent as a list has {} elements, expected 1",
                    wrapper.len()
                )));
            }
            wrapper.value(0)
        } else {
            sent
        };
        to_stored(&self.column_type, &own)
    }

    /// No cells of this column.
    pub(super) fn empty(&self) -> ArrayRef {
        new_null_array(&self.column_type.arrow_type(), 0)
    }
}

/// What is left to read of one record batch.
pub(super) struct Cursor<'a> {
    batch: ipc::RecordBatch<'a>,
    body: Buffer,
    version: ipc::MetadataVersion,
    node: usize,
    buffer: usize,
}

impl<'a> Cursor<'a> {
    /// A cursor at the first column of `batch`. Every buffer must lie inside the body:
    /// Arrow's reader trusts the header on that.
    pub(super) fn new(
        batch: ipc::RecordBatch<'a>,
        body: &[u8],
        version: ipc::MetadataVersion,
    ) -> Result<Self, ClientError> {
        let inside = batch.buffers().is_none_or(|buffers| {
            buffers.iter().all(|buffer| {
                buffer.offset() >= 0
                    && buffer.length() >= 0
                    && buffer
                        .offset()
                        .checked_add(buffer.length())
                        .is_some_and(|end| end as u64 <= body.len() as u64)
            })
        });
        let lengths = batch
            .nodes()
            .is_none_or(|nodes| nodes.iter().all(|node| node.length() >= 0));
        if !inside || !lengths || batch.length() < 0 {
            return Err(protocol(
                "the record batch describes buffers outside its body",
            ));
        }
        Ok(Cursor {
            batch,
            // a copy, because the reader wants the alignment of a fresh allocation
            body: Buffer::from_vec(body.to_vec()),
            version,
            node: 0,
            buffer: 0,
        })
    }

    /// The length the header gives the batch.
    pub(super) fn length(&self) -> i64 {
        self.batch.length()
    }

    /// Reads the next `nodes` field nodes and `buffers` buffers as the one column of
    /// `schema`, in the Arrow type the schema declares.
    fn column(
        &mut self,
        schema: &Arc<Schema>,
        nodes: usize,
        buffers: usize,
    ) -> Result<ArrayRef, ClientError> {
        let short = || protocol("the record batch ends before its last column");
        let all_nodes = self.batch.nodes().ok_or_else(short)?;
        let all_buffers = self.batch.buffers().ok_or_else(short)?;
        if self.node + nodes > all_nodes.len() || self.buffer + buffers > all_buffers.len() {
            return Err(short());
        }
        let own_nodes: Vec<ipc::FieldNode> = (self.node..self.node + nodes)
            .map(|at| *all_nodes.get(at))
            .collect();
        let own_buffers: Vec<ipc::Buffer> = (self.buffer..self.buffer + buffers)
            .map(|at| *all_buffers.get(at))
            .collect();
        self.node += nodes;
        self.buffer += buffers;

        let mut builder = FlatBufferBuilder::new();
        let length = own_nodes[0].length();
        let nodes = builder.create_vector(&own_nodes);
        let buffers = builder.create_vector(&own_buffers);
        let header = ipc::RecordBatch::create(
            &mut builder,
            &ipc::RecordBatchArgs {
                length,
                nodes: Some(nodes),
                buffers: Some(buffers),
                ..Default::default()
            },
        );
        builder.finish(header, None);
        let header = flatbuffers::root::<ipc::RecordBatch>(builder.finished_data())
            .map_err(|problem| protocol(format!("unreadable record batch: {problem}")))?;
        let read = catch_unwind(AssertUnwindSafe(|| {
            ipc::reader::read_record_batch(
                &self.body,
                header,
                schema.clone(),
                &HashMap::new(),
                None,
                &self.version,
            )
        }));
        match read {
            Ok(Ok(batch)) => Ok(batch.column(0).clone()),
            Ok(Err(problem)) => Err(protocol(format!("unreadable record batch: {problem}"))),
            Err(_) => Err(protocol("unreadable record batch")),
        }
    }
}

fn unexpected(column_type: &ColumnType, sent: &ArrayRef) -> ClientError {
    protocol(format!(
        "a column of {column_type:?} arrived as {}",
        sent.data_type()
    ))
}

/// The validity of `values` with the cells that hold `sentinel` made null too.
///
/// Behavior: the `isNull` test each reader of `DefaultChunkReaderFactory` makes on the
/// value it read (`value == QueryConstants.NULL_LONG ? null : ...`)
fn without_sentinel<T: ArrowPrimitiveType>(
    values: &PrimitiveArray<T>,
    sentinel: T::Native,
) -> Option<NullBuffer>
where
    T::Native: PartialEq,
{
    let valid: NullBuffer = values
        .iter()
        .map(|value| value.is_some_and(|value| value != sentinel))
        .collect();
    (valid.null_count() > 0).then_some(valid)
}

/// Sixty-four bit cells stored in the type of `column_type`, null where the server sent
/// a null or the value Java reserves for one.
fn longs_as<T, S>(sent: &ArrayRef, column_type: &ColumnType) -> Result<ArrayRef, ClientError>
where
    T: ArrowPrimitiveType<Native = i64>,
    S: ArrowPrimitiveType<Native = i64>,
{
    let values = sent
        .as_primitive_opt::<T>()
        .ok_or_else(|| unexpected(column_type, sent))?;
    let valid = without_sentinel(values, NULL_LONG);
    let stored = PrimitiveArray::<S>::new(values.values().clone(), valid)
        .with_data_type(column_type.arrow_type());
    Ok(Arc::new(stored))
}

/// The cells of a column of `column_type` as the engine stores them, from the cells a
/// server sent. The inverse of [`crate::wire_columns::WireColumn::cells`] for the types
/// Java's client reads back, with Java's losses: a `Period` arrives as months alone, a
/// `ZonedDateTime` as its instant in UTC.
///
/// A null is a cell the server marked null and also one that holds the value Java
/// reserves for a null of its type, whether or not the client asked for Deephaven nulls.
///
/// Behavior: the readers of `io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory`:
/// `#timestampToInstant`, `#timestampToZonedDateTime`, `#dateToLocalDate`,
/// `#timeToLocalTime`, `#durationToDuration`, `#intervalToPeriod`, `#binaryToBigDecimal`,
/// `#binaryToBigInt`, `#binaryToByteArray`, and
/// `io.deephaven.extensions.barrage.chunk.ListChunkReader` for arrays and vectors
pub(super) fn to_stored(
    column_type: &ColumnType,
    sent: &ArrayRef,
) -> Result<ArrayRef, ClientError> {
    let wrong = || unexpected(column_type, sent);
    let table = |failure: oxhaven_contracts::table::TableFailure| protocol(failure.message);
    Ok(match column_type {
        ColumnType::Boolean | ColumnType::PrimitiveBoolean | ColumnType::String => {
            if *sent.data_type() != column_type.arrow_type() {
                return Err(wrong());
            }
            sent.clone()
        }
        ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double => {
            if *sent.data_type() != column_type.arrow_type() {
                return Err(wrong());
            }
            sentinels_to_nulls(sent).map_err(table)?
        }
        ColumnType::Instant => {
            let nanos = sent
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(wrong)?;
            // the values are nanoseconds since the epoch whatever zone the field names
            let utc: ArrayRef = Arc::new(nanos.clone().with_timezone("UTC"));
            sentinels_to_nulls(&utc).map_err(table)?
        }
        ColumnType::ZonedDateTime => {
            let nanos = sent
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(wrong)?;
            let zone = match sent.data_type() {
                DataType::Timestamp(_, Some(zone)) => zone.clone(),
                _ => return Err(wrong()),
            };
            let valid = without_sentinel(nanos, NULL_LONG);
            let instants = TimestampNanosecondArray::new(nanos.values().clone(), valid.clone())
                .with_data_type(ColumnType::Instant.arrow_type());
            let zones: StringArray = (0..nanos.len())
                .map(|row| {
                    valid
                        .as_ref()
                        .is_none_or(|valid| valid.is_valid(row))
                        .then_some(zone.as_ref())
                })
                .collect();
            let DataType::Struct(fields) = column_type.arrow_type() else {
                return Err(wrong());
            };
            let cells =
                StructArray::try_new(fields, vec![Arc::new(instants), Arc::new(zones)], valid)
                    .map_err(protocol)?;
            Arc::new(cells)
        }
        ColumnType::LocalDate => match sent.data_type() {
            DataType::Date32 => {
                let days = sent.as_primitive::<Date32Type>();
                let valid = without_sentinel(days, NULL_INT);
                Arc::new(Date32Array::new(days.values().clone(), valid))
            }
            DataType::Date64 => {
                let millis = sent.as_primitive::<Date64Type>();
                let valid = without_sentinel(millis, NULL_LONG);
                // Java divides the milliseconds by those of a day, towards zero
                let days: Result<Vec<i32>, ClientError> = millis
                    .values()
                    .iter()
                    .enumerate()
                    .map(|(row, millis)| {
                        if valid.as_ref().is_some_and(|valid| valid.is_null(row)) {
                            return Ok(0);
                        }
                        i32::try_from(millis / 86_400_000).map_err(|_| {
                            protocol(format!("a LocalDate of {millis} ms is out of range"))
                        })
                    })
                    .collect();
                Arc::new(Date32Array::new(days?.into(), valid))
            }
            _ => return Err(wrong()),
        },
        ColumnType::LocalTime => {
            longs_as::<Time64NanosecondType, Time64NanosecondType>(sent, column_type)?
        }
        ColumnType::Duration => {
            longs_as::<DurationNanosecondType, DurationNanosecondType>(sent, column_type)?
        }
        ColumnType::Period => {
            let months = sent
                .as_primitive_opt::<IntervalYearMonthType>()
                .ok_or_else(wrong)?;
            let valid = without_sentinel(months, NULL_INT);
            let DataType::Struct(fields) = column_type.arrow_type() else {
                return Err(wrong());
            };
            // Behavior: `java.time.Period#ofMonths`: no years and no days
            let zeros = || Arc::new(Int32Array::from(vec![0; months.len()])) as ArrayRef;
            let months: ArrayRef = Arc::new(Int32Array::new(months.values().clone(), None));
            let cells = StructArray::try_new(fields, vec![zeros(), months, zeros()], valid)
                .map_err(protocol)?;
            Arc::new(cells)
        }
        ColumnType::BigInteger | ColumnType::BigDecimal => {
            let cells = sent.as_binary_opt::<i32>().ok_or_else(wrong)?;
            let prefix = if *column_type == ColumnType::BigDecimal {
                4
            } else {
                0
            };
            let mut stored = Vec::with_capacity(cells.len());
            for cell in cells {
                let Some(cell) = cell else {
                    stored.push(None);
                    continue;
                };
                // Java: an ArrayIndexOutOfBoundsException for a short scale, and
                // `NumberFormatException: Zero length BigInteger`
                if cell.len() <= prefix {
                    return Err(protocol(format!(
                        "a {column_type:?} cell of {} bytes holds no number",
                        cell.len()
                    )));
                }
                let (scale, unscaled) = cell.split_at(prefix);
                stored.push(Some([scale, minimal(unscaled)].concat()));
            }
            Arc::new(BinaryArray::from_iter(stored))
        }
        ColumnType::Array(element) | ColumnType::Vector(element) => {
            let DataType::List(field) = column_type.arrow_type() else {
                return Err(wrong());
            };
            let (offsets, elements, valid) = match sent.data_type() {
                DataType::Binary if **element == ColumnType::Byte => {
                    let cells = sent.as_binary::<i32>();
                    let bytes: Int8Array = cells
                        .value_data()
                        .iter()
                        .map(|byte| *byte as i8)
                        .collect::<Vec<i8>>()
                        .into();
                    let bytes: ArrayRef = Arc::new(bytes);
                    (cells.offsets().clone(), bytes, cells.nulls().cloned())
                }
                DataType::List(_) => {
                    let cells = sent.as_list::<i32>();
                    (
                        cells.offsets().clone(),
                        cells.values().clone(),
                        cells.nulls().cloned(),
                    )
                }
                _ => return Err(wrong()),
            };
            let elements = to_stored(element, &elements)?;
            Arc::new(ListArray::try_new(field, offsets, elements, valid).map_err(protocol)?)
        }
        other => {
            return Err(not_ported(
                "io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory#newReader",
                format_args!("reading a column of {other:?}"),
            ))
        }
    })
}
