//! Arrow IPC record batches laid out as the Java server lays them out.
//!
//! The Arrow format leaves a writer several choices, and clients may depend on the ones Java
//! makes, so this writer makes the same ones instead of using Arrow's own writer:
//!
//! - a column without nulls in the batch has a validity buffer of length 0;
//! - a column of a field that the schema declares not nullable never has a validity buffer
//!   and reports no nulls, whatever it holds;
//! - a validity bitmap and a `Boolean` value bitmap are whole 64-bit words, every other
//!   buffer is padded to a multiple of 8 bytes, and the lengths in the header include the
//!   padding;
//! - the value under a null is Java's sentinel for the type (`false` for `Boolean`, no bytes
//!   for `String`);
//! - a batch of no rows has no bytes at all, not even the first offset of a `String` column;
//! - the offsets of an array or vector column start at 0 and a null cell has no elements,
//!   so the elements sent are exactly those of the cells that are not null.
//!
//! The same layout is what a Barrage snapshot or update carries, so the subscription code
//! uses this module too.

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, ListArray, PrimitiveArray, UInt32Array,
};
use arrow::datatypes::{
    DataType, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time64NanosecondType, TimeUnit, TimestampNanosecondType, UInt16Type,
};
use arrow::ipc;
use flatbuffers::FlatBufferBuilder;
use oxhaven_column::sentinel::nulls_to_sentinels;
use oxhaven_proto::flight::FlightData;

/// The number of rows in the first record batch of a snapshot, unless the client asks for
/// fewer.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl#DEFAULT_INITIAL_BATCH_SIZE`
const INITIAL_BATCH_SIZE: usize = 4096;

/// The size a message stays under unless one row alone is larger.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl#DEFAULT_MESSAGE_SIZE_LIMIT`
const MESSAGE_SIZE_LIMIT: usize = 100 * 1024 * 1024;

/// What a client may ask of the record batches it is sent. Zero means the server's default.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageSnapshotOptions#batchSize`,
/// `#maxMessageSize`
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct BatchOptions {
    /// The largest number of rows in one record batch.
    pub batch_size: usize,
    /// The largest size of one message in bytes.
    pub max_message_size: usize,
    /// The elements of an array or vector column are sent with Java's sentinel values in
    /// place of a validity buffer, where their type has one. The client asked for
    /// Deephaven nulls, which reach the writer of the elements as they reach the writer
    /// of a column; the caller applies them to the columns themselves.
    ///
    /// Behavior: `io.deephaven.extensions.barrage.BarrageOptions#useDeephavenNulls`
    pub element_sentinels: bool,
    /// The most elements sent of each cell of an array or vector column, and of each list
    /// within one: the first so many if positive, the last so many if negative, all if
    /// zero.
    ///
    /// Behavior: `io.deephaven.extensions.barrage.BarrageOptions#previewListLengthLimit`
    pub list_length_limit: i64,
}

/// Whether Java's writer for the type sends sentinel values in place of a validity buffer
/// when the client asked for Deephaven nulls: the primitive writers do, the `Boolean`,
/// `String` and list writers do not.
///
/// Behavior: the `dhNullable` argument each writer passes to
/// `io.deephaven.extensions.barrage.chunk.BaseChunkWriter`
pub(crate) fn has_sentinel(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::UInt16
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Date64
            | DataType::Time64(TimeUnit::Nanosecond)
    )
}

/// What a list column sends: the end of each cell among the elements sent, counted from 0,
/// and those elements. A null cell has no elements, whatever its offsets say, and a cell
/// longer than `limit` allows is cut as [`BatchOptions::list_length_limit`] says.
///
/// Behavior: the `expand` of every expansion kernel, for example
/// `io.deephaven.extensions.barrage.chunk.vector.IntVectorExpansionKernel#expand` (dense
/// offsets, a null row is empty), and
/// `io.deephaven.extensions.barrage.chunk.ListChunkWriter.ListChunkInputStream` (the limit)
fn sent_elements(lists: &ListArray, limit: i64) -> Result<(Vec<i32>, ArrayRef), EncodeError> {
    let too_large = || EncodeError("a list column is too large for one record batch".into());
    let offsets = lists.value_offsets();
    let mut ends = Vec::with_capacity(lists.len());
    // the ranges of elements to send, joined where one continues the one before
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let mut sent = 0usize;
    for row in 0..lists.len() {
        if lists.is_valid(row) {
            let (mut start, mut end) = (offsets[row] as usize, offsets[row + 1] as usize);
            let most = usize::try_from(limit.unsigned_abs()).unwrap_or(usize::MAX);
            if limit < 0 {
                start = start.max(end.saturating_sub(most));
            } else if limit > 0 {
                end = end.min(start.saturating_add(most));
            }
            if end > start {
                sent += end - start;
                match ranges.last_mut() {
                    Some(last) if last.1 == start => last.1 = end,
                    _ => ranges.push((start, end)),
                }
            }
        }
        ends.push(i32::try_from(sent).map_err(|_| too_large())?);
    }
    let values = lists.values();
    let elements = match ranges.as_slice() {
        [] => values.slice(0, 0),
        [(start, end)] => values.slice(*start, end - start),
        ranges => {
            let positions: UInt32Array = ranges
                .iter()
                .flat_map(|(start, end)| *start as u32..*end as u32)
                .collect::<Vec<u32>>()
                .into();
            arrow::compute::take(values.as_ref(), &positions, None)
                .map_err(|problem| EncodeError(problem.to_string()))?
        }
    };
    Ok((ends, elements))
}

/// The data could not be written: a column has an Arrow type no column type maps to, or
/// more bytes than a message can describe.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EncodeError(pub String);

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EncodeError {}

/// One record batch: the flatbuffer `Message` and the body it describes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EncodedBatch {
    pub header: Vec<u8>,
    pub body: Vec<u8>,
}

/// Collects the buffers of a batch and what the header says about them.
#[derive(Default)]
struct BatchWriter {
    options: BatchOptions,
    body: Vec<u8>,
    nodes: Vec<ipc::FieldNode>,
    buffers: Vec<ipc::Buffer>,
}

/// The size of a bitmap of `rows` bits in whole 64-bit words.
///
/// Behavior: `io.deephaven.extensions.barrage.chunk.BaseChunkWriter#getValidityMapSerializationSizeFor`
fn bitmap_bytes(rows: usize) -> usize {
    rows.div_ceil(64) * 8
}

impl BatchWriter {
    /// Adds a buffer and pads it to a multiple of 8 bytes.
    ///
    /// Behavior: `BaseChunkWriter.BaseChunkInputStream#padBufferSize`, `#writePadBuffer`
    fn buffer(&mut self, bytes: &[u8]) {
        let offset = self.body.len();
        self.body.extend_from_slice(bytes);
        self.body.resize(offset + bytes.len().div_ceil(8) * 8, 0);
        self.buffers.push(ipc::Buffer::new(
            offset as i64,
            (self.body.len() - offset) as i64,
        ));
    }

    /// Adds a bitmap of `rows` bits, least significant bit first.
    ///
    /// Behavior: `BaseChunkWriter.SerContext`
    fn bitmap(&mut self, rows: usize, bit: impl Fn(usize) -> bool) {
        let mut bytes = vec![0u8; bitmap_bytes(rows)];
        for row in (0..rows).filter(|&row| bit(row)) {
            bytes[row / 8] |= 1 << (row % 8);
        }
        self.buffer(&bytes);
    }

    /// Starts a column: its field node and its validity buffer, which is empty unless the
    /// field is nullable and the column has a null.
    ///
    /// Behavior: `BaseChunkWriter.BaseChunkInputStream#nullCount`, `#sendValidityBuffer`,
    /// `#writeValidityBuffer`
    fn start_column(&mut self, array: &dyn Array, nullable: bool) {
        let null_count = if nullable { array.null_count() } else { 0 };
        self.nodes
            .push(ipc::FieldNode::new(array.len() as i64, null_count as i64));
        if null_count == 0 {
            self.buffer(&[]);
        } else {
            self.bitmap(array.len(), |row| array.is_valid(row));
        }
    }

    /// Behavior: the `drainTo` of each primitive writer, for example
    /// `io.deephaven.extensions.barrage.chunk.IntChunkWriter.IntChunkInputStream#drainTo`,
    /// which writes the chunk's values as they are, sentinels included
    fn primitive<T: ArrowPrimitiveType>(&mut self, with_sentinels: &ArrayRef) {
        let values: &PrimitiveArray<T> = with_sentinels.as_primitive();
        // Arrow buffers and the wire are both little-endian
        self.buffer(values.values().inner().as_slice());
    }

    fn column(&mut self, array: &ArrayRef, nullable: bool) -> Result<(), EncodeError> {
        // A `LocalDate` is sent as the milliseconds of its day and a `LocalTime` as the
        // nanoseconds of its time, both by the writer of a `long`.
        //
        // Behavior: io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory#dateFromLocalDate,
        // `#timeFromLocalTime`
        let longs: ArrayRef;
        let array = match array.data_type() {
            DataType::Date64 => {
                let millis = array.as_primitive::<Date64Type>();
                longs = std::sync::Arc::new(millis.reinterpret_cast::<Int64Type>());
                &longs
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let nanos = array.as_primitive::<Time64NanosecondType>();
                longs = std::sync::Arc::new(nanos.reinterpret_cast::<Int64Type>());
                &longs
            }
            _ => array,
        };
        self.start_column(array.as_ref(), nullable);
        let unsupported = || EncodeError(format!("cannot write a column of {}", array.data_type()));
        match array.data_type() {
            // Behavior: io.deephaven.extensions.barrage.chunk.BooleanChunkWriter
            DataType::Boolean => {
                let values = array.as_boolean();
                self.bitmap(values.len(), |row| {
                    values.is_valid(row) && values.value(row)
                });
            }
            // Behavior: io.deephaven.extensions.barrage.chunk.VarBinaryChunkWriter
            // A `byte[]` is written by the same writer: its bytes where a string has
            // its UTF-8.
            DataType::Utf8 | DataType::Binary => {
                let values: Vec<Option<&[u8]>> = match array.as_string_opt::<i32>() {
                    Some(strings) => strings
                        .iter()
                        .map(|value| value.map(str::as_bytes))
                        .collect(),
                    None => array.as_binary::<i32>().iter().collect(),
                };
                let mut offsets = Vec::with_capacity(4 * (values.len() + 1));
                let mut data = Vec::new();
                if !values.is_empty() {
                    offsets.extend_from_slice(&0i32.to_le_bytes());
                }
                for value in values {
                    data.extend_from_slice(value.unwrap_or_default());
                    let end = i32::try_from(data.len()).map_err(|_| {
                        EncodeError("a string column is too large for one record batch".into())
                    })?;
                    offsets.extend_from_slice(&end.to_le_bytes());
                }
                self.buffer(&offsets);
                self.buffer(&data);
            }
            // The offsets of the cells, then the elements as a column of their own: a
            // field node, a validity buffer and the buffers of their type. A null cell is
            // told by the validity buffer alone, whether or not the client asked for
            // Deephaven nulls.
            //
            // Behavior: io.deephaven.extensions.barrage.chunk.ListChunkWriter
            DataType::List(_) => {
                let (ends, elements) =
                    sent_elements(array.as_list::<i32>(), self.options.list_length_limit)?;
                let mut offsets = Vec::with_capacity(4 * (ends.len() + 1));
                if !ends.is_empty() {
                    offsets.extend_from_slice(&0i32.to_le_bytes());
                }
                for end in ends {
                    offsets.extend_from_slice(&end.to_le_bytes());
                }
                self.buffer(&offsets);
                // The element field is nullable in every schema this server sends.
                let validity =
                    !(self.options.element_sentinels && has_sentinel(elements.data_type()));
                self.column(&elements, validity)?;
            }
            other => {
                let values = nulls_to_sentinels(array).map_err(|_| unsupported())?;
                match other {
                    DataType::Int8 => self.primitive::<Int8Type>(&values),
                    DataType::UInt16 => self.primitive::<UInt16Type>(&values),
                    DataType::Int16 => self.primitive::<Int16Type>(&values),
                    DataType::Int32 => self.primitive::<Int32Type>(&values),
                    DataType::Int64 => self.primitive::<Int64Type>(&values),
                    DataType::Float32 => self.primitive::<Float32Type>(&values),
                    DataType::Float64 => self.primitive::<Float64Type>(&values),
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                        self.primitive::<TimestampNanosecondType>(&values)
                    }
                    _ => return Err(unsupported()),
                }
            }
        }
        Ok(())
    }

    /// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl#getInputStream`
    /// (the record batch header) and
    /// `io.deephaven.extensions.barrage.util.MessageHelper#wrapInMessage`
    fn finish(self, rows: usize) -> EncodedBatch {
        let mut builder = FlatBufferBuilder::new();
        let nodes = builder.create_vector(&self.nodes);
        let buffers = builder.create_vector(&self.buffers);
        let batch = ipc::RecordBatch::create(
            &mut builder,
            &ipc::RecordBatchArgs {
                length: rows as i64,
                nodes: Some(nodes),
                buffers: Some(buffers),
                ..Default::default()
            },
        );
        let message = ipc::Message::create(
            &mut builder,
            &ipc::MessageArgs {
                version: ipc::MetadataVersion::V5,
                header_type: ipc::MessageHeader::RecordBatch,
                header: Some(batch.as_union_value()),
                bodyLength: self.body.len() as i64,
                custom_metadata: None,
            },
        );
        builder.finish(message, None);
        EncodedBatch {
            header: builder.finished_data().to_vec(),
            body: self.body,
        }
    }
}

/// Writes rows `offset..offset + rows` of `columns` as one record batch. `nullable` says,
/// per column, whether the field is nullable in the schema the client was sent.
///
/// A column that ends before `offset + rows` gives the rows it has, and none if it has no
/// data at all: a Barrage update carries a column the subscriber did not ask for as an
/// empty column, and its modified columns each have their own number of rows.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl#appendAddColumns`,
/// `#appendModColumns`
pub fn encode_batch(
    columns: &[ArrayRef],
    nullable: &[bool],
    offset: usize,
    rows: usize,
) -> Result<EncodedBatch, EncodeError> {
    encode_batch_with(
        columns,
        nullable,
        offset,
        rows,
        false,
        BatchOptions::default(),
    )
}

/// [`encode_batch`], with each column as the one element of a list if `as_lists`: the
/// batch then has one row, and every column is preceded by the field node and the
/// buffers of a list of one element that holds the column's rows. A Barrage client asks
/// for this when it cannot read columns of different lengths.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl#setRecordBatchLength`,
/// `io.deephaven.extensions.barrage.chunk.SingleElementListHeaderWriter`
fn encode_batch_with(
    columns: &[ArrayRef],
    nullable: &[bool],
    offset: usize,
    rows: usize,
    as_lists: bool,
    options: BatchOptions,
) -> Result<EncodedBatch, EncodeError> {
    let mut writer = BatchWriter {
        options,
        ..Default::default()
    };
    for (column, &nullable) in columns.iter().zip(nullable) {
        let start = offset.min(column.len());
        let count = rows.min(column.len() - start);
        if as_lists {
            let end = i32::try_from(count)
                .map_err(|_| EncodeError("too many rows for one list".to_owned()))?;
            writer.nodes.push(ipc::FieldNode::new(1, 0));
            // no validity buffer, then the offsets of the one element
            writer.buffer(&[]);
            writer.buffer(&[0i32.to_le_bytes(), end.to_le_bytes()].concat());
        }
        writer.column(&column.slice(start, count), nullable)?;
    }
    Ok(writer.finish(if as_lists { 1 } else { rows }))
}

/// A Flight message that carries a schema.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl.SchemaMessageView`
pub fn schema_message(schema: &arrow::datatypes::Schema) -> FlightData {
    FlightData {
        data_header: crate::schema::schema_message(schema).into(),
        ..Default::default()
    }
}

fn flight_data(batch: EncodedBatch, app_metadata: Vec<u8>) -> FlightData {
    FlightData {
        data_header: batch.header.into(),
        data_body: batch.body.into(),
        app_metadata: app_metadata.into(),
        ..Default::default()
    }
}

/// The record batch messages for `rows` rows of `columns`, in order. `app_metadata` goes
/// with the first message only.
///
/// The first batch has at most 4096 rows. Each later one has as many as its predecessor's
/// bytes per row predict will fill nine tenths of the message size limit; a batch that
/// comes out over the limit is cut down and written again. With `send_empty`, no rows
/// still give one message, which is how a client of a refreshing table receives the
/// metadata; without it they give none.
///
/// Behavior: `io.deephaven.extensions.barrage.BarrageMessageWriterImpl#processBatches`,
/// `BarrageMessageWriterImpl.SnapshotView#forEachStream`
pub fn record_batch_messages(
    columns: &[ArrayRef],
    nullable: &[bool],
    rows: usize,
    options: BatchOptions,
    app_metadata: Vec<u8>,
    send_empty: bool,
) -> Result<Vec<FlightData>, EncodeError> {
    record_batch_messages_with(
        columns,
        nullable,
        rows,
        options,
        app_metadata,
        send_empty,
        false,
    )
}

/// [`record_batch_messages`], with each column of a batch as the one element of a list if
/// `as_lists`; see `encode_batch_with`.
pub fn record_batch_messages_with(
    columns: &[ArrayRef],
    nullable: &[bool],
    rows: usize,
    options: BatchOptions,
    app_metadata: Vec<u8>,
    send_empty: bool,
    as_lists: bool,
) -> Result<Vec<FlightData>, EncodeError> {
    if rows == 0 {
        return Ok(if send_empty {
            vec![flight_data(
                encode_batch_with(columns, nullable, 0, 0, as_lists, options)?,
                app_metadata,
            )]
        } else {
            Vec::new()
        });
    }
    let max_batch_size = match options.batch_size {
        0 => i32::MAX as usize,
        size => size,
    };
    let max_message_size = match options.max_message_size {
        0 => MESSAGE_SIZE_LIMIT,
        size => size,
    };
    let mut batch_size = INITIAL_BATCH_SIZE.min(max_batch_size);
    let mut app_metadata = Some(app_metadata);
    let mut messages = Vec::new();
    let mut offset = 0;
    while offset < rows {
        let count = batch_size.min(rows - offset);
        let batch = encode_batch_with(columns, nullable, offset, count, as_lists, options)?;
        let size =
            batch.header.len() + batch.body.len() + app_metadata.as_ref().map_or(0, Vec::len);
        if size < max_message_size || batch_size == 1 {
            messages.push(flight_data(batch, app_metadata.take().unwrap_or_default()));
            offset += count;
        }
        if let Some(row_limit) = max_message_size.checked_div(size / count) {
            batch_size = max_batch_size.min(((row_limit as f64 * 0.9) as usize).max(1));
        }
    }
    Ok(messages)
}
