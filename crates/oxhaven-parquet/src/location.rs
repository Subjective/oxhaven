//! A Parquet file as a location of a stored table.

use crate::schema::{self, FileColumn};
use crate::{not_ported, ReadError, ReadOptions, UnsignedLongTarget};
use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BinaryArray, Int64Array, Int8Array, ListArray,
    Time64NanosecondArray, UInt32Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{cast_with_options, concat, take, CastOptions};
use arrow::datatypes::{
    DataType, Field, Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampNanosecondType, UInt64Type,
};
use oxhaven_column::java_values::{
    big_decimal_cell, big_decimal_precision, big_decimal_text, big_integer_cell,
    big_integer_cell_unsigned, minimal,
};
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::TableFailure;
use oxhaven_engine::source::TableLocation;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::{Compression, LogicalType, Type as PhysicalType};
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::DataType as ParquetDataType;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// One Parquet file. Opening it reads the footer. The values of a column are decoded a
/// row group at a time, when a row of the group is first read, and kept.
///
/// Behavior: `io.deephaven.parquet.table.location.ParquetTableLocation` and
/// `ParquetColumnLocation`. Java keeps the pages it has decoded behind soft references;
/// this keeps the decoded column of a row group for as long as the table lives.
pub(crate) struct ParquetLocation {
    name: String,
    path: PathBuf,
    metadata: ArrowReaderMetadata,
    /// For each column of the table, where it is in the file; `None` for a column the
    /// file does not have, which reads as nulls.
    columns: Vec<Option<FileColumn>>,
    types: Vec<ColumnType>,
    /// For each partitioning column of the table, the value every row of the file has in
    /// it: an array of one element.
    constants: Vec<Option<ArrayRef>>,
    /// The first row of each row group, and after them the number of rows of the file.
    group_rows: Vec<u64>,
    /// The decoded values of each row group's columns: `groups * columns` cells.
    decoded: Vec<Mutex<Option<ArrayRef>>>,
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The codecs whose crates this build has (`docs/STORAGE.md`, "Dependencies").
pub(crate) fn check_codec(path: &Path, codec: Compression) -> Result<(), ReadError> {
    match codec {
        Compression::UNCOMPRESSED
        | Compression::SNAPPY
        | Compression::GZIP(_)
        | Compression::LZ4
        | Compression::ZSTD(_)
        | Compression::LZ4_RAW => Ok(()),
        other => Err(not_ported(
            "io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory#getByName",
            format_args!("{}: pages compressed with {other}", path.display()),
        )),
    }
}

impl ParquetLocation {
    /// Reads the footer of `path` and the columns of the file.
    fn load(
        path: &Path,
        options: &ReadOptions,
    ) -> Result<(ArrowReaderMetadata, Vec<FileColumn>), ReadError> {
        let problem =
            |error: &dyn std::fmt::Display| ReadError::Io(format!("{}: {error}", path.display()));
        let file = File::open(path).map_err(|error| problem(&error))?;
        // the types come from the Parquet schema, as in Java, not from an Arrow schema
        // the writer may have left in the footer
        let reader_options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
        let metadata =
            ArrowReaderMetadata::load(&file, reader_options).map_err(|error| problem(&error))?;
        let footer = metadata.metadata();
        let file_metadata = footer.file_metadata();
        let columns = schema::columns(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
            options,
        )?;
        for group in footer.row_groups() {
            for chunk in group.columns() {
                check_codec(path, chunk.compression())?;
            }
        }
        // what the reader will hand back must be a plain array or a list of plain values
        for column in &columns {
            let field = metadata.schema().field(column.root);
            if column.nested || column.column_type == ColumnType::array(ColumnType::Byte) {
                // the one leaf of a struct, or bytes that are no list: `decode` knows both
                continue;
            }
            let plain = match (column.column_type.list_element(), field.data_type()) {
                (Some(_), DataType::List(element)) => !element.data_type().is_nested(),
                (Some(_), _) => false,
                (None, stored) => !stored.is_nested(),
            };
            if !plain {
                return Err(not_ported(
                    "io.deephaven.parquet.table.location.ParquetColumnLocation#makeToPage",
                    format_args!(
                        "{}: column {} of type {:?}, stored as {}",
                        path.display(),
                        column.name,
                        column.column_type,
                        field.data_type()
                    ),
                ));
            }
        }
        Ok((metadata, columns))
    }

    fn new(
        path: &Path,
        metadata: ArrowReaderMetadata,
        columns: Vec<Option<FileColumn>>,
        types: Vec<ColumnType>,
        constants: Vec<Option<ArrayRef>>,
    ) -> Self {
        let mut group_rows = vec![0];
        let mut rows = 0;
        for group in metadata.metadata().row_groups() {
            rows += group.num_rows().max(0) as u64;
            group_rows.push(rows);
        }
        let cells = (group_rows.len() - 1) * columns.len();
        Self {
            name: path.display().to_string(),
            path: path.to_path_buf(),
            metadata,
            columns,
            types,
            constants,
            group_rows,
            decoded: (0..cells).map(|_| Mutex::new(None)).collect(),
        }
    }

    /// The columns of the file at `path`, which need not hold rows: a metadata file has
    /// columns too.
    ///
    /// Behavior: `io.deephaven.parquet.table.ParquetSchemaReader#convertSchema` of the
    /// footer of a file
    pub(crate) fn file_columns(
        path: &Path,
        options: &ReadOptions,
    ) -> Result<Vec<ColumnDefinition>, ReadError> {
        let (_, columns) = Self::load(path, options)?;
        let columns = columns.into_iter();
        Ok(columns
            .map(|column| ColumnDefinition::new(column.name, column.column_type))
            .collect())
    }

    /// The number of row groups of the file.
    pub(crate) fn row_groups(&self) -> usize {
        self.group_rows.len() - 1
    }

    /// The file at `path` as a location of a table with the columns of `definition`. A
    /// column the file does not have reads as nulls; a column the file has with another
    /// type is refused. A partitioning column is not read from the file, whether the file
    /// has it or not: every row has the value `partitions` gives for its name, an array of
    /// one element, or null.
    ///
    /// Behavior: `io.deephaven.parquet.table.location.ParquetTableLocation#makeColumnLocation`
    /// and `ParquetColumnLocation#exists`; Java converts between some types where a file
    /// disagrees with the definition, which is not ported.
    pub(crate) fn open(
        path: &Path,
        definition: &TableDefinition,
        options: &ReadOptions,
        partitions: &[(&str, ArrayRef)],
    ) -> Result<Self, ReadError> {
        let (metadata, mut found) = Self::load(path, options)?;
        let mut columns = Vec::with_capacity(definition.columns().len());
        let mut constants = Vec::with_capacity(definition.columns().len());
        for wanted in definition.columns() {
            if wanted.partitioning {
                let value = partitions.iter().find(|(name, _)| *name == wanted.name);
                let value = value.map(|(_, value)| value.clone());
                let null = || new_null_array(&wanted.column_type.arrow_type(), 1);
                constants.push(Some(value.unwrap_or_else(null)));
                columns.push(None);
                continue;
            }
            constants.push(None);
            let position = found.iter().position(|column| column.name == wanted.name);
            let column = position.map(|position| found.swap_remove(position));
            if let Some(column) = column
                .as_ref()
                .filter(|c| c.column_type != wanted.column_type)
            {
                return Err(not_ported(
                    "io.deephaven.parquet.table.location.ParquetColumnLocation#makeToPage",
                    format_args!(
                        "{}: column {} is {:?} in this file and {:?} in the table",
                        path.display(),
                        wanted.name,
                        column.column_type,
                        wanted.column_type
                    ),
                ));
            }
            columns.push(column);
        }
        let types = definition.columns().iter();
        let types = types.map(|column| column.column_type.clone()).collect();
        Ok(Self::new(path, metadata, columns, types, constants))
    }

    /// Decodes the values of row group `group` of the file column `column`.
    fn decode(&self, group: usize, column: &FileColumn) -> Result<ArrayRef, TableFailure> {
        let problem = |error: &dyn std::fmt::Display| failure(format!("{}: {error}", self.name));
        let named = |message: String| problem(&format_args!("column {}: {message}", column.name));
        let rows = (self.group_rows[group + 1] - self.group_rows[group]) as usize;
        let descriptor = self.metadata.parquet_schema().column(column.leaf);
        if column.nested && descriptor.max_def_level() > 1 {
            // Behavior: `io.deephaven.parquet.base.ColumnPageReaderImpl`, which refuses a
            // value that two levels of its path can leave out
            return Err(named(
                "Failed to read parquet page because nested optional levels are not supported"
                    .to_string(),
            ));
        }
        let values = if let Some(LogicalType::Decimal(decimal)) = schema::annotation(&descriptor) {
            let raw = self.raw(group, column.leaf, rows).map_err(named)?;
            let decimal = Decimal {
                precision: decimal.precision,
                scale: decimal.scale,
                fixed: descriptor.physical_type() == PhysicalType::FIXED_LEN_BYTE_ARRAY,
                integer: column.column_type == ColumnType::BigInteger,
            };
            decimal.cells(raw).map_err(named)?
        } else {
            let file = File::open(&self.path).map_err(|error| problem(&error))?;
            let projection = ProjectionMask::roots(self.metadata.parquet_schema(), [column.root]);
            let reader =
                ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.metadata.clone())
                    .with_projection(projection)
                    .with_row_groups(vec![group])
                    .with_batch_size(rows.max(1))
                    .build()
                    .map_err(|error| problem(&error))?;
            let mut batches = Vec::new();
            for batch in reader {
                let batch = batch.map_err(|error| problem(&error))?;
                batches.push(batch.column(0).clone());
            }
            let values = match batches.as_slice() {
                [one] => one.clone(),
                many => {
                    let arrays: Vec<&dyn Array> = many.iter().map(|batch| batch.as_ref()).collect();
                    concat(&arrays).map_err(|error| problem(&error))?
                }
            };
            let values = if column.nested {
                only_field(values).map_err(named)?
            } else {
                values
            };
            to_column_type(values, &column.column_type, column.unsigned_long).map_err(named)?
        };
        if values.len() != rows {
            return Err(problem(&format_args!(
                "column {} has {} values in a row group of {rows} rows",
                column.name,
                values.len()
            )));
        }
        Ok(values)
    }

    /// The values of the leaf column `leaf` in row group `group` as the file holds them,
    /// before any logical type: an integer as the bytes of its two's complement, bytes as
    /// they are, and `None` for a value that is left out.
    fn raw(&self, group: usize, leaf: usize, rows: usize) -> Result<Raw, String> {
        let text = |error: &dyn std::fmt::Display| error.to_string();
        let file = File::open(&self.path).map_err(|error| text(&error))?;
        let reader = SerializedFileReader::new(file).map_err(|error| text(&error))?;
        let row_group = reader.get_row_group(group).map_err(|error| text(&error))?;
        let column = row_group
            .get_column_reader(leaf)
            .map_err(|error| text(&error))?;
        let descriptor = self.metadata.parquet_schema().column(leaf);
        let max_def = descriptor.max_def_level();
        let integer = |value: i64| big_integer_cell(i128::from(value));
        let (values, bytes) = match column {
            ColumnReader::Int32ColumnReader(mut reader) => {
                let cell = |value: &i32| integer(i64::from(*value));
                (read_all(&mut reader, rows, max_def, cell)?, false)
            }
            ColumnReader::Int64ColumnReader(mut reader) => {
                let cell = |value: &i64| integer(*value);
                (read_all(&mut reader, rows, max_def, cell)?, false)
            }
            ColumnReader::ByteArrayColumnReader(mut reader) => {
                let cell = |value: &parquet::data_type::ByteArray| value.data().to_vec();
                (read_all(&mut reader, rows, max_def, cell)?, true)
            }
            ColumnReader::FixedLenByteArrayColumnReader(mut reader) => {
                let cell = |value: &parquet::data_type::FixedLenByteArray| value.data().to_vec();
                (read_all(&mut reader, rows, max_def, cell)?, true)
            }
            _ => {
                return Err(format!(
                    "a decimal stored as {}",
                    descriptor.physical_type()
                ))
            }
        };
        Ok(Raw { values, bytes })
    }

    /// The values of row group `group` of the table column `column`, decoded once.
    fn group(
        &self,
        group: usize,
        column: usize,
        source: &FileColumn,
    ) -> Result<ArrayRef, TableFailure> {
        let cell = &self.decoded[group * self.columns.len() + column];
        // held while decoding, so that two readers of one cell decode it once
        let mut cell = cell.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(values) = cell.as_ref() {
            return Ok(values.clone());
        }
        let values = self.decode(group, source)?;
        *cell = Some(values.clone());
        Ok(values)
    }
}

/// Every value of the `rows` rows of a column chunk that does not repeat: `None` where the
/// definition level says the value is left out.
fn read_all<T: ParquetDataType>(
    reader: &mut ColumnReaderImpl<T>,
    rows: usize,
    max_def: i16,
    bytes: impl Fn(&T::T) -> Vec<u8>,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut levels = Vec::new();
    let mut values = Vec::new();
    let mut read = 0;
    while read < rows {
        let levels = (max_def > 0).then_some(&mut levels);
        let (records, _, _) = reader
            .read_records(rows - read, levels, None, &mut values)
            .map_err(|error| error.to_string())?;
        if records == 0 {
            break;
        }
        read += records;
    }
    let mut values = values.iter().map(bytes);
    if max_def == 0 {
        return Ok(values.map(Some).collect());
    }
    let present = levels.iter().map(|level| *level == max_def);
    Ok(present
        .map(|present| present.then(|| values.next()).flatten())
        .collect())
}

/// The raw values of a column chunk, and whether the file holds them as bytes and not as
/// an integer.
struct Raw {
    values: Vec<Option<Vec<u8>>>,
    bytes: bool,
}

/// How the values of a decimal column of a file become cells.
struct Decimal {
    precision: i32,
    scale: i32,
    /// Whether the file holds the values in a fixed number of bytes.
    fixed: bool,
    /// Whether the column is a `BigInteger` column, which Java writes as a decimal of
    /// precision 1 and scale 0, and not a `BigDecimal` one.
    integer: bool,
}

impl Decimal {
    /// The cells of a `BigDecimal` or `BigInteger` column from the raw values of the file.
    ///
    /// Three accidents of Java are kept. Bytes of a fixed length that are all `0xff`,
    /// which is the unscaled integer -1, read as null. So do no bytes at all. And a
    /// `BigInteger` column stored as `INT32` or `INT64` cannot be read.
    ///
    /// Behavior: `io.deephaven.parquet.base.BigDecimalParquetBytesCodec#decode`,
    /// `io.deephaven.parquet.base.BigIntegerParquetBytesCodec#decode`,
    /// `io.deephaven.parquet.base.materializers.BigDecimalFromIntMaterializer`, and the
    /// decimal case of `ParquetColumnLocation.LogicalTypeVisitor#visit`
    fn cells(&self, raw: Raw) -> Result<ArrayRef, String> {
        let Decimal {
            precision, scale, ..
        } = *self;
        if !raw.bytes {
            if self.integer {
                let text = "The native type for a BigDecimal column is java.math.BigInteger";
                return Err(text.to_string());
            }
            let cells = raw.values.iter();
            let cells = cells.map(|value| value.as_deref().map(|v| big_decimal_cell(scale, v)));
            return Ok(Arc::new(BinaryArray::from_iter(cells)));
        }
        if !self.integer {
            // Behavior: `BigDecimalParquetBytesCodec#verifyPrecisionAndScale`
            let invalid = if precision <= 0 {
                Some(format!("precision (={precision}) should be > 0"))
            } else if scale < 0 {
                Some(format!("scale (={scale}) should be >= 0"))
            } else if scale > precision {
                Some(format!(
                    "scale (={scale}) is greater than precision (={precision})"
                ))
            } else {
                None
            };
            if let Some(invalid) = invalid {
                return Err(format!("Invalid scale and precision: {invalid}"));
            }
        }
        let mut cells = Vec::with_capacity(raw.values.len());
        for value in &raw.values {
            let value = value.as_deref().filter(|bytes| {
                !bytes.is_empty() && !(self.fixed && bytes.iter().all(|byte| *byte == 0xff))
            });
            let Some(bytes) = value else {
                cells.push(None);
                continue;
            };
            if self.integer {
                cells.push(Some(minimal(bytes).to_vec()));
                continue;
            }
            if big_decimal_precision(bytes) > precision as usize {
                return Err(format!(
                    "Unable to decode '{}' with decimal(precision={precision}, scale={scale})",
                    big_decimal_text(scale, bytes)
                ));
            }
            cells.push(Some(big_decimal_cell(scale, bytes)));
        }
        Ok(Arc::new(BinaryArray::from_iter(cells)))
    }
}

/// The one field of a struct, or of a struct in a struct: a value is null where it or a
/// struct around it is.
fn only_field(values: ArrayRef) -> Result<ArrayRef, String> {
    let mut values = values;
    while let Some(group) = values.as_struct_opt() {
        let [field] = group.columns() else {
            return Err(format!("a struct with {} fields", group.num_columns()));
        };
        let nulls = NullBuffer::union(group.nulls(), field.nulls());
        let data = field.to_data().into_builder().nulls(nulls).build();
        values = arrow::array::make_array(data.map_err(|error| error.to_string())?);
    }
    Ok(values)
}

/// The nanoseconds of the times of day `values`, whatever unit the file has them in.
///
/// Behavior: `io.deephaven.parquet.base.materializers.LocalTimeFromMillisMaterializer#convertValue`
/// and its two relatives, where `java.time.LocalTime#ofNanoOfDay` refuses a value outside
/// the day. Java multiplies before it checks, so that a count of microseconds too large
/// for a `long` of nanoseconds wraps around first; here it is refused like any other.
fn times_of_day(values: &ArrayRef) -> Result<ArrayRef, String> {
    let nanos: Vec<Option<i64>> = match values.data_type() {
        DataType::Time32(TimeUnit::Millisecond) => {
            let millis = values.as_primitive::<Time32MillisecondType>().iter();
            millis
                .map(|v| v.map(|v| i64::from(v) * 1_000_000))
                .collect()
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let micros = values.as_primitive::<Time64MicrosecondType>().iter();
            micros
                .map(|v| v.map(|v| v.checked_mul(1_000).unwrap_or(-1)))
                .collect()
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let nanos = values.as_primitive::<Time64NanosecondType>();
            nanos.iter().collect()
        }
        other => return Err(format!("a time of day stored as {other}")),
    };
    const LAST: i64 = 86_399_999_999_999;
    if let Some(outside) = nanos.iter().flatten().find(|v| !(0..=LAST).contains(*v)) {
        return Err(format!(
            "Invalid value for NanoOfDay (valid values 0 - {LAST}): {outside}"
        ));
    }
    Ok(Arc::new(Time64NanosecondArray::from(nanos)))
}

/// A column of byte strings as a column of `byte[]`: each string a list of its bytes, and
/// the byte Java reserves for a null `byte` a null element, as in every list of bytes.
///
/// Behavior: `io.deephaven.util.codec.SimpleByteArrayCodec#decode`
fn byte_arrays(values: &ArrayRef) -> Result<ArrayRef, String> {
    let binary = match values.data_type() {
        DataType::Binary => values.clone(),
        DataType::FixedSizeBinary(_) => {
            cast_with_options(values, &DataType::Binary, &CHECKED).map_err(|e| e.to_string())?
        }
        other => return Err(format!("a byte[] stored as {other}")),
    };
    let binary = binary.as_binary::<i32>();
    let bytes = ScalarBuffer::<i8>::from(binary.values().clone());
    let bytes: ArrayRef = Arc::new(Int8Array::new(bytes, None));
    let bytes = sentinels_to_nulls(&bytes).map_err(|failure| failure.message)?;
    let field = Arc::new(Field::new("", DataType::Int8, true));
    let offsets = OffsetBuffer::new(binary.offsets().inner().clone());
    let list = ListArray::try_new(field, offsets, bytes, binary.nulls().cloned());
    Ok(Arc::new(list.map_err(|error| error.to_string())?))
}

/// A cast that fails where a value does not fit, where the default makes it null.
const CHECKED: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: arrow::util::display::FormatOptions::new(),
};

/// `values`, as the Parquet reader decoded them, as an array of a column of type
/// `column_type`: unsigned and narrower integers widened, timestamps in nanoseconds, the
/// elements of a list under the element field every list column has here, and a value
/// equal to the Java null of its type as a null.
///
/// Behavior: the `ToPage` implementations of
/// `io.deephaven.parquet.table.pagestore.topage` that
/// `io.deephaven.parquet.table.location.ParquetColumnLocation#makeToPage` chooses, which
/// write each value into a Java array where the null of the type is a reserved value
fn to_column_type(
    values: ArrayRef,
    column_type: &ColumnType,
    unsigned_long: Option<UnsignedLongTarget>,
) -> Result<ArrayRef, String> {
    // The types Java holds as objects: nothing in a file stands for null but a value
    // that is left out, so no value becomes one here.
    match column_type {
        ColumnType::LocalDate => {
            return match values.data_type() {
                DataType::Date32 => Ok(values),
                other => Err(format!("a date stored as {other}")),
            };
        }
        ColumnType::LocalTime => return times_of_day(&values),
        ColumnType::LocalDateTime => {
            // Behavior: `LocalDateTimeFromMillisMaterializer#convertValue` and its two
            // relatives. A date and time too far from 1970 for a count of nanoseconds,
            // which Java holds, is an error here.
            let DataType::Timestamp(_, None) = values.data_type() else {
                return Err(format!(
                    "a local timestamp stored as {}",
                    values.data_type()
                ));
            };
            return cast_with_options(&values, &column_type.arrow_type(), &CHECKED)
                .map_err(|error| error.to_string());
        }
        ColumnType::BigInteger => {
            // Behavior: `BigIntegerFromUnsignedLongMaterializer#convertValue`
            let Some(unsigned) = values.as_primitive_opt::<UInt64Type>() else {
                return Err(format!("a BigInteger stored as {}", values.data_type()));
            };
            let cells = unsigned.iter().map(|v| v.map(big_integer_cell_unsigned));
            return Ok(Arc::new(BinaryArray::from_iter(cells)));
        }
        ColumnType::Array(element)
            if **element == ColumnType::Byte
                && !matches!(values.data_type(), DataType::List(_)) =>
        {
            return byte_arrays(&values);
        }
        _ => {}
    }
    if let Some(element) = column_type.list_element() {
        let Some(list) = values.as_list_opt::<i32>() else {
            return Err(format!("a list column stored as {}", values.data_type()));
        };
        let elements = to_column_type(list.values().clone(), element, unsigned_long)?;
        let field = Arc::new(Field::new("", element.arrow_type(), true));
        let list = ListArray::try_new(
            field,
            list.offsets().clone(),
            elements,
            list.nulls().cloned(),
        );
        return Ok(Arc::new(list.map_err(|error| error.to_string())?));
    }
    let target = column_type.arrow_type();
    let values = match (values.data_type(), unsigned_long) {
        (DataType::UInt64, Some(target)) => {
            let unsigned = values.as_primitive::<UInt64Type>();
            if target == UnsignedLongTarget::Long {
                // Behavior: `ToLongPage` for `UnsignedLongTarget#LONG`
                let too_large = |value: &Option<u64>| value.is_some_and(|v| v > i64::MAX as u64);
                if let Some(value) = unsigned.iter().find(too_large).flatten() {
                    return Err(format!("the unsigned value {value} does not fit a long"));
                }
            }
            let signed: Int64Array = unsigned.unary(|value| value as i64);
            Arc::new(signed) as ArrayRef
        }
        (stored, _) if *stored == target => values,
        (DataType::Timestamp(_, zone), _) => {
            // The unit is changed and the zone is not: every timestamp read as an
            // instant counts from the epoch in UTC, whatever its zone is called, and an
            // INT96 has none.
            let nanoseconds = DataType::Timestamp(TimeUnit::Nanosecond, zone.clone());
            let values = cast_with_options(&values, &nanoseconds, &CHECKED);
            let values = values.map_err(|error| error.to_string())?;
            let values = values.as_primitive::<TimestampNanosecondType>().clone();
            Arc::new(values.with_data_type(target))
        }
        _ => cast_with_options(&values, &target, &CHECKED).map_err(|error| error.to_string())?,
    };
    sentinels_to_nulls(&values).map_err(|failure| failure.message)
}

impl TableLocation for ParquetLocation {
    fn name(&self) -> &str {
        &self.name
    }

    fn size(&self) -> u64 {
        self.group_rows[self.group_rows.len() - 1]
    }

    fn read(&self, column: usize, rows: Range<u64>) -> Result<ArrayRef, TableFailure> {
        let size = (rows.end - rows.start) as usize;
        if let Some(value) = &self.constants[column] {
            let first = UInt32Array::from_value(0, size);
            return take(value, &first, None).map_err(|error| failure(error.to_string()));
        }
        let Some(source) = &self.columns[column] else {
            return Ok(new_null_array(&self.types[column].arrow_type(), size));
        };
        let mut pieces = Vec::new();
        // the first group that ends after the first row
        let first = self.group_rows[1..].partition_point(|end| *end <= rows.start);
        for group in first..self.group_rows.len() - 1 {
            let start = self.group_rows[group];
            if start >= rows.end {
                break;
            }
            let values = self.group(group, column, source)?;
            let from = rows.start.max(start);
            let to = rows.end.min(self.group_rows[group + 1]);
            pieces.push(values.slice((from - start) as usize, (to - from) as usize));
        }
        match pieces.as_slice() {
            [one] => Ok(one.clone()),
            many => {
                let arrays: Vec<&dyn Array> = many.iter().map(|piece| piece.as_ref()).collect();
                if arrays.is_empty() {
                    return Ok(new_null_array(&self.types[column].arrow_type(), 0));
                }
                concat(&arrays).map_err(|error| failure(format!("{}: {error}", self.name)))
            }
        }
    }
}
