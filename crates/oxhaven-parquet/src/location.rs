//! A Parquet file as a location of a stored table.

use crate::schema::{self, FileColumn};
use crate::{not_ported, ReadError, ReadOptions, UnsignedLongTarget};
use arrow::array::{new_null_array, Array, ArrayRef, AsArray, Int64Array, ListArray};
use arrow::compute::{cast_with_options, concat, CastOptions};
use arrow::datatypes::{DataType, Field, TimeUnit, TimestampNanosecondType, UInt64Type};
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::TableFailure;
use oxhaven_engine::source::TableLocation;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression;
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
            group_rows,
            decoded: (0..cells).map(|_| Mutex::new(None)).collect(),
        }
    }

    /// The file at `path` as the location that decides the columns of a table: the table
    /// has the columns of the file.
    ///
    /// Behavior: `io.deephaven.parquet.table.ParquetTools#infer`
    pub(crate) fn infer(
        path: &Path,
        options: &ReadOptions,
    ) -> Result<(TableDefinition, Self), ReadError> {
        let (metadata, columns) = Self::load(path, options)?;
        let definitions = columns.iter();
        let definitions = definitions
            .map(|column| ColumnDefinition::new(column.name.clone(), column.column_type.clone()))
            .collect();
        let definition = TableDefinition::new(definitions)
            .map_err(|duplicate| ReadError::Invalid(duplicate.to_string()))?;
        let types = columns.iter().map(|c| c.column_type.clone()).collect();
        let columns = columns.into_iter().map(Some).collect();
        Ok((definition, Self::new(path, metadata, columns, types)))
    }

    /// The file at `path` as a location of a table with the columns of `definition`. A
    /// column the file does not have reads as nulls; a column the file has with another
    /// type is refused.
    ///
    /// Behavior: `io.deephaven.parquet.table.location.ParquetTableLocation#makeColumnLocation`
    /// and `ParquetColumnLocation#exists`; Java converts between some types where a file
    /// disagrees with the definition, which is not ported.
    pub(crate) fn open(
        path: &Path,
        definition: &TableDefinition,
        options: &ReadOptions,
    ) -> Result<Self, ReadError> {
        let (metadata, mut found) = Self::load(path, options)?;
        let mut columns = Vec::with_capacity(definition.columns().len());
        for wanted in definition.columns() {
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
        Ok(Self::new(path, metadata, columns, types))
    }

    /// Decodes the values of row group `group` of the file column `column`.
    fn decode(&self, group: usize, column: &FileColumn) -> Result<ArrayRef, TableFailure> {
        let problem = |error: &dyn std::fmt::Display| failure(format!("{}: {error}", self.name));
        let file = File::open(&self.path).map_err(|error| problem(&error))?;
        let rows = (self.group_rows[group + 1] - self.group_rows[group]) as usize;
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
        let values = to_column_type(values, &column.column_type, column.unsigned_long)
            .map_err(|message| problem(&format_args!("column {}: {message}", column.name)))?;
        if values.len() != rows {
            return Err(problem(&format_args!(
                "column {} has {} values in a row group of {rows} rows",
                column.name,
                values.len()
            )));
        }
        Ok(values)
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
