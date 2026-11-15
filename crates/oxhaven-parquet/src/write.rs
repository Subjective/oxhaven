//! Writing tables as Parquet files.
//!
//! [`write`] is `io.deephaven.parquet.table.ParquetTools#writeTables`: each table becomes
//! one file with the Parquet schema, the `deephaven` footer entry and the row groups the
//! Java writer gives it, so that Java and this crate read the same table from a file of
//! either. `docs/STORAGE.md`, "Writing Parquet", has the design and what is refused.

use crate::{not_ported, ReadError as WriteError};
use arrow::array::{new_null_array, Array, ArrayRef, AsArray, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use arrow::row::{RowConverter, SortField};
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::{attribute_map, AttributeValue};
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
use parquet::basic::{
    Compression, LogicalType, Repetition, TimeUnit as ParquetTimeUnit, Type as PhysicalType,
};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::{ColumnPath, SchemaDescriptor, Type, TypePtr};
use std::collections::HashSet;
use std::fs;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The key of the footer entry Deephaven writes.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTableWriter#METADATA_KEY`
const METADATA_KEY: &str = "deephaven";

/// The version the `deephaven` footer entry names: the version of the reference this
/// writer follows (`oracle/REFERENCE.md`), which is also the version the server tells its
/// clients.
///
/// Behavior: `io.deephaven.parquet.table.metadata.TableInfo#version`
const DEEPHAVEN_VERSION: &str = "42.9";

const EXTENSION: &str = ".parquet";

/// Behavior: `io.deephaven.parquet.table.ParquetInstructions#DEFAULT_TARGET_PAGE_SIZE`
const DEFAULT_TARGET_PAGE_SIZE: i32 = 1 << 16;
/// Behavior: `ParquetInstructions#MIN_TARGET_PAGE_SIZE`
const MIN_TARGET_PAGE_SIZE: i32 = 1 << 11;
/// Behavior: `ParquetInstructions#DEFAULT_MAXIMUM_DICTIONARY_KEYS` and
/// `#DEFAULT_MAXIMUM_DICTIONARY_SIZE`
const DEFAULT_MAXIMUM_DICTIONARY: i32 = 1 << 20;

/// How the rows of a table are split into row groups.
///
/// Behavior: `io.deephaven.parquet.table.metadata.RowGroupInfo`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RowGroups {
    /// One row group.
    #[default]
    Single,
    /// This many row groups of the same size, the first ones a row larger where the rows
    /// do not divide evenly; one row each when there are fewer rows than groups.
    MaxGroups(i64),
    /// As few row groups of the same size as keep each at or under this many rows.
    MaxRows(i64),
    /// One row group for each run of rows with the same values in `columns`, split
    /// further as `MaxRows` splits when `max_rows` is given. The rows of a group must be
    /// next to each other.
    ByGroups {
        columns: Vec<String>,
        max_rows: Option<i64>,
    },
}

/// What to do with one column of the table when it is written.
///
/// Behavior: the per-column settings of `io.deephaven.parquet.table.ParquetInstructions`
/// that `deephaven.parquet.ColumnInstruction` sets for a write
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteColumn {
    /// The column's name in the table.
    pub name: String,
    /// The column's name in the file. Java needs one for every instruction.
    pub parquet_name: Option<String>,
    /// The class of an object codec. Not ported: naming one is refused.
    pub codec: Option<String>,
    /// Whether a string column gets a dictionary however large it grows.
    pub use_dictionary: bool,
}

/// The arguments of [`write`] besides the tables and the paths.
///
/// Behavior: `io.deephaven.parquet.table.ParquetInstructions`, the settings that apply to
/// writing
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOptions {
    /// The columns of the files, where they are not the tables': a column a table does
    /// not have is written as nulls, and a column of a table that is not listed is left
    /// out.
    pub definition: Option<TableDefinition>,
    pub columns: Vec<WriteColumn>,
    /// The name of the page compression; `None` is Snappy.
    pub compression: Option<String>,
    /// The most distinct strings a column chunk may have and still get a dictionary.
    pub max_dictionary_keys: Option<i32>,
    /// The most bytes of distinct strings a column chunk may have and still get one.
    pub max_dictionary_size: Option<i32>,
    /// The size in bytes after which a data page is closed.
    pub target_page_size: Option<i32>,
    /// Whether `_metadata` and `_common_metadata` files are written. Not ported.
    pub generate_metadata_files: bool,
    /// The column sets to write data indexes for. Not ported: naming one is refused.
    pub index_columns: Vec<Vec<String>>,
    pub row_groups: RowGroups,
}

fn invalid(message: impl Into<String>) -> WriteError {
    WriteError::Invalid(message.into())
}

fn io(path: &Path, error: impl std::fmt::Display) -> WriteError {
    WriteError::Io(format!("{}: {error}", path.display()))
}

impl WriteOptions {
    /// Refuses the instructions Java's builder refuses, in its order, and the ones that
    /// are not ported.
    ///
    /// Behavior: `deephaven.parquet._build_parquet_instructions` and the setters of
    /// `io.deephaven.parquet.table.ParquetInstructions.Builder` it calls
    fn check(&self) -> Result<(), WriteError> {
        const MAPPING: &str =
            "io.deephaven.parquet.table.ParquetInstructions.Builder#addColumnNameMapping";
        for (index, column) in self.columns.iter().enumerate() {
            let name = &column.name;
            let Some(parquet_name) = &column.parquet_name else {
                // Java: a `NullPointerException` out of the builder's map, for every
                // instruction without a name for the file
                return Err(invalid(format!(
                    "the instruction for column {name} names no parquet column"
                )));
            };
            if parquet_name.is_empty() {
                return Err(not_ported(MAPPING, "an empty parquet column name"));
            }
            let earlier = &self.columns[..index];
            let mapped = |other: &&WriteColumn| other.name == *name;
            let renamed = earlier.iter().filter(mapped).find_map(|other| {
                let first = other.parquet_name.as_ref()?;
                (first != parquet_name).then_some(first)
            });
            if let Some(first) = renamed {
                return Err(invalid(format!(
                    "Cannot add a mapping from parquetColumnName={parquet_name}: \
                     columnName={name} already mapped to parquetColumnName={first}"
                )));
            }
            let taken = |other: &&WriteColumn| {
                other.parquet_name.as_ref() == Some(parquet_name) && other.name != *name
            };
            if let Some(other) = earlier.iter().find(taken) {
                return Err(invalid(format!(
                    "Cannot add new mapping from parquetColumnName={parquet_name} to \
                     columnName={name}: already mapped to columnName={}",
                    other.name
                )));
            }
            if column.codec.as_deref().is_some_and(|codec| !codec.is_empty()) {
                return Err(not_ported(
                    "io.deephaven.parquet.table.ParquetInstructions.Builder#addColumnCodec",
                    format_args!("a codec for column {name}"),
                ));
            }
        }
        for (what, limit) in [
            ("maximumDictionaryKeys", self.max_dictionary_keys),
            ("maximumDictionarySize", self.max_dictionary_size),
        ] {
            if let Some(limit) = limit.filter(|limit| *limit < 0) {
                return Err(invalid(format!(
                    "Requirement failed: required {what} >= 0, instead {what} == {limit}."
                )));
            }
        }
        if self
            .target_page_size
            .is_some_and(|size| size < MIN_TARGET_PAGE_SIZE)
        {
            return Err(invalid(format!(
                "Target page size should be >= {MIN_TARGET_PAGE_SIZE}"
            )));
        }
        if self.generate_metadata_files {
            return Err(not_ported(
                "io.deephaven.parquet.table.ParquetMetadataFileWriterImpl#writeMetadataFiles",
                "the _metadata and _common_metadata files",
            ));
        }
        if !self.index_columns.is_empty() {
            return Err(not_ported(
                "io.deephaven.parquet.table.ParquetInstructions.Builder#addAllIndexColumns",
                "data indexes written beside the table",
            ));
        }
        Ok(())
    }

    /// The name in the file of the table's column `name`.
    ///
    /// Behavior: `ParquetInstructions#getParquetColumnNameFromColumnNameOrDefault`
    fn parquet_name_of<'a>(&'a self, name: &'a str) -> &'a str {
        let named = |column: &&WriteColumn| column.name == name;
        let instruction = self.columns.iter().find(named);
        instruction
            .and_then(|column| column.parquet_name.as_deref())
            .unwrap_or(name)
    }

    /// Behavior: `ParquetInstructions#useDictionary`; a later instruction for the same
    /// column replaces an earlier one
    fn use_dictionary(&self, name: &str) -> bool {
        let named = |column: &&WriteColumn| column.name == name;
        self.columns
            .iter()
            .rev()
            .find(named)
            .is_some_and(|column| column.use_dictionary)
    }
}

/// The page compression Java writes for the codec name `name`.
///
/// Java asks Hadoop's codec factory, which knows a codec by the simple name of its class
/// without regard to case, with or without the `Codec` at its end, and by its full class
/// name. The simple names here are the ones the reference server answered with a file;
/// every other name it refused.
///
/// Behavior: `io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory#getByName`,
/// `org.apache.hadoop.io.compress.CompressionCodecFactory#getCodecByName`
fn compression(name: &str) -> Result<Compression, WriteError> {
    const BY_NAME: &str =
        "io.deephaven.parquet.compress.DeephavenCompressorAdapterFactory#getByName";
    let lower = name.to_ascii_lowercase();
    if lower == "uncompressed" {
        return Ok(Compression::UNCOMPRESSED);
    }
    if lower == "lz4_raw" {
        return Ok(Compression::LZ4_RAW);
    }
    if name.contains('.') {
        return Err(not_ported(
            BY_NAME,
            format_args!("the compression codec named by its class, {name}"),
        ));
    }
    Ok(match lower.strip_suffix("codec").unwrap_or(&lower) {
        "snappy" => Compression::SNAPPY,
        "gzip" | "jdkgzip" => Compression::GZIP(Default::default()),
        "zstd" => Compression::ZSTD(Default::default()),
        "lz4" => Compression::LZ4,
        "lz4raw" => Compression::LZ4_RAW,
        // Java writes LZO pages; the Parquet crate has no LZO
        "lzo" => return Err(not_ported(BY_NAME, "pages compressed with LZO")),
        _ => {
            return Err(invalid(format!(
                "Failed to find CompressionCodec for codecName={name}"
            )))
        }
    })
}

/// One column of the files: where its values come from and how it is declared.
struct OutColumn {
    /// The name in the table and in the definition.
    name: String,
    parquet_name: String,
    column_type: ColumnType,
    /// Whether the values are strings, alone or in lists: the columns Java considers a
    /// dictionary for.
    strings: bool,
    use_dictionary: bool,
}

/// The Parquet type of a value of `column_type`, which is not a list.
///
/// Behavior: the `TypeInfo` implementations of `io.deephaven.parquet.table.TypeInfos`
fn primitive(name: &str, column_type: &ColumnType) -> Result<Type, WriteError> {
    let (physical, logical) = match column_type {
        ColumnType::Boolean => (PhysicalType::BOOLEAN, None),
        ColumnType::Byte => (PhysicalType::INT32, Some(LogicalType::integer(8, true))),
        ColumnType::Short => (PhysicalType::INT32, Some(LogicalType::integer(16, true))),
        ColumnType::Int => (PhysicalType::INT32, Some(LogicalType::integer(32, true))),
        ColumnType::Char => (PhysicalType::INT32, Some(LogicalType::integer(16, false))),
        ColumnType::Long => (PhysicalType::INT64, None),
        ColumnType::Float => (PhysicalType::FLOAT, None),
        ColumnType::Double => (PhysicalType::DOUBLE, None),
        ColumnType::String => (PhysicalType::BYTE_ARRAY, Some(LogicalType::String)),
        ColumnType::Instant => (
            PhysicalType::INT64,
            Some(LogicalType::timestamp(true, ParquetTimeUnit::NANOS)),
        ),
        other => {
            return Err(not_ported(
                "io.deephaven.parquet.table.TypeInfos#getTypeInfo",
                format_args!("column {name} of type {other:?}"),
            ))
        }
    };
    Type::primitive_type_builder(name, physical)
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(logical)
        .build()
        .map_err(|error| invalid(format!("column {name}: {error}")))
}

/// The field of the file's schema for `column`: an optional value, or for a vector or an
/// array an optional list of optional values named `element`.
///
/// Behavior: `io.deephaven.parquet.table.TypeInfos.TypeInfo#createSchemaType`
fn schema_field(column: &OutColumn) -> Result<TypePtr, WriteError> {
    let name = &column.parquet_name;
    let Some(element) = column.column_type.list_element() else {
        return Ok(Arc::new(primitive(name, &column.column_type)?));
    };
    if element.list_element().is_some() {
        // Java serializes each cell with a codec and writes the bytes
        return Err(not_ported(
            "io.deephaven.parquet.table.TypeInfos#getCodecAndArgs",
            format_args!(
                "column {}, whose cells hold vectors or arrays: Java writes them with \
                 io.deephaven.util.codec.SerializableCodec",
                column.name
            ),
        ));
    }
    let problem = |error| invalid(format!("column {name}: {error}"));
    let repeated = Type::group_type_builder("list")
        .with_repetition(Repetition::REPEATED)
        .with_fields(vec![Arc::new(primitive("element", element)?)])
        .build()
        .map_err(problem)?;
    let list = Type::group_type_builder(name)
        .with_repetition(Repetition::OPTIONAL)
        .with_logical_type(Some(LogicalType::List))
        .with_fields(vec![Arc::new(repeated)])
        .build()
        .map_err(problem)?;
    Ok(Arc::new(list))
}

/// The Arrow type the Parquet writer is handed for a column: the column type's own, with
/// the elements of a list under the name they have in the file.
fn arrow_type(column_type: &ColumnType) -> DataType {
    match column_type.list_element() {
        Some(element) => DataType::List(Arc::new(Field::new(
            "element",
            element.arrow_type(),
            true,
        ))),
        None => column_type.arrow_type(),
    }
}

/// `values`, a chunk of a column of `column_type`, as the writer takes them: every value
/// equal to the Java null of its type as a null, which is what it is in Java.
///
/// Behavior: the `TransferObject` implementations of
/// `io.deephaven.parquet.table.transfer`, which leave out the nulls of each type
fn to_file_type(values: ArrayRef, column_type: &ColumnType) -> Result<ArrayRef, WriteError> {
    let problem = |error: &dyn std::fmt::Display| invalid(error.to_string());
    let Some(element) = column_type.list_element() else {
        return sentinels_to_nulls(&values).map_err(|failure| problem(&failure.message));
    };
    let Some(list) = values.as_list_opt::<i32>() else {
        return Err(invalid(format!(
            "a column of type {column_type:?} is stored as {}",
            values.data_type()
        )));
    };
    let elements = sentinels_to_nulls(list.values()).map_err(|failure| problem(&failure.message))?;
    let field = Arc::new(Field::new("element", element.arrow_type(), true));
    let list = ListArray::try_new(field, list.offsets().clone(), elements, list.nulls().cloned());
    Ok(Arc::new(list.map_err(|error| problem(&error))?))
}

/// The columns of the files, from the definition to write and the instructions.
///
/// Behavior: `io.deephaven.parquet.table.MappedSchema#create`
fn out_columns(
    definition: &TableDefinition,
    options: &WriteOptions,
) -> Result<Vec<OutColumn>, WriteError> {
    let mut columns: Vec<OutColumn> = Vec::with_capacity(definition.columns().len());
    for column in definition.columns() {
        let parquet_name = options.parquet_name_of(&column.name).to_string();
        if let Some(other) = columns.iter().find(|c| c.parquet_name == parquet_name) {
            // Java builds the schema and fails when it writes the second column
            return Err(invalid(format!(
                "columns {} and {} have the same name in the file, {parquet_name}",
                other.name, column.name
            )));
        }
        let values = column.column_type.list_element();
        let values = values.unwrap_or(&column.column_type);
        columns.push(OutColumn {
            name: column.name.clone(),
            parquet_name,
            column_type: column.column_type.clone(),
            strings: *values == ColumnType::String,
            use_dictionary: options.use_dictionary(&column.name),
        });
    }
    Ok(columns)
}

/// The `deephaven` footer entry of a file with the columns `columns`, written from a
/// table Java holds sorted by `sorted` first.
///
/// The entry lists a column only where the Parquet schema does not say its type: here, a
/// vector, which the schema declares as the list an array is.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTableWriter#getParquetFileWriter`,
/// `io.deephaven.parquet.table.metadata.TableInfo#serializeToJSON`
fn table_info(columns: &[OutColumn], sorted: Option<&(String, bool)>) -> String {
    let text = |text: &str| serde_json::Value::String(text.to_string()).to_string();
    let mut info = format!("{{\"version\":{}", text(DEEPHAVEN_VERSION));
    let vectors: Vec<String> = columns
        .iter()
        .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
        .map(|column| {
            let name = text(&column.parquet_name);
            format!("{{\"columnName\":{name},\"specialType\":\"Vector\"}}")
        })
        .collect();
    if !vectors.is_empty() {
        info.push_str(&format!(",\"columnTypes\":[{}]", vectors.join(",")));
    }
    if let Some((name, descending)) = sorted {
        let direction = if *descending {
            "Descending"
        } else {
            "Ascending"
        };
        let name = text(name);
        info.push_str(&format!(
            ",\"sortingColumns\":[{{\"columnName\":{name},\"sortDirection\":\"{direction}\"}}]"
        ));
    }
    info.push('}');
    info
}

/// The first column Java holds `table` sorted by, and whether it is descending.
///
/// Behavior: `io.deephaven.engine.table.impl.SortedColumnsAttribute#getSortedColumns`, of
/// which `ParquetTableWriter#write` records the first
fn first_sorted(table: &dyn Table) -> Option<(String, bool)> {
    let attributes = attribute_map(table);
    let sorted = attributes.iter().find(|(name, _)| *name == "SortedColumns");
    let Some((_, AttributeValue::Text(text))) = sorted else {
        return None;
    };
    let (name, direction) = text.split(',').next()?.split_once('=')?;
    Some((name.to_string(), direction == "Descending"))
}

/// The sizes of the row groups of `groups` evenly sized groups over `rows` rows.
///
/// Behavior: `RowGroupTableIteratorVisitor#splitByMaxGroups` and its `SplitEvenlyIterator`
fn split_evenly(rows: u64, groups: i64) -> Result<Vec<u64>, WriteError> {
    if groups < 1 {
        return Err(invalid(format!(
            "Number of groups must be at least 1, got: {groups}"
        )));
    }
    let groups = groups as u64;
    if groups == 1 {
        return Ok(vec![rows]);
    }
    let (size, larger) = (rows / groups, rows % groups);
    let count = groups.min(rows);
    Ok((0..count).map(|group| size + u64::from(group < larger)).collect())
}

/// Behavior: `RowGroupTableIteratorVisitor#splitByMaxRows`
fn split_by_max_rows(rows: u64, max_rows: i64) -> Result<Vec<u64>, WriteError> {
    let max_rows = max_rows as u64;
    let groups = rows / max_rows + u64::from(rows % max_rows > 0);
    split_evenly(rows, groups as i64)
}

/// A float column with the two zeros as one value and every NaN as one, which is how
/// Java compares the keys of a group.
fn as_key(values: &ArrayRef) -> ArrayRef {
    match values.data_type() {
        DataType::Float32 => {
            let floats = values.as_primitive::<Float32Type>();
            Arc::new(floats.unary::<_, Float32Type>(|value| match value {
                value if value.is_nan() => f32::NAN,
                value if value == 0.0 => 0.0,
                value => value,
            }))
        }
        DataType::Float64 => {
            let floats = values.as_primitive::<Float64Type>();
            Arc::new(floats.unary::<_, Float64Type>(|value| match value {
                value if value.is_nan() => f64::NAN,
                value if value == 0.0 => 0.0,
                value => value,
            }))
        }
        _ => values.clone(),
    }
}

/// The sizes of the runs of rows of `batch` that have the same values in the columns
/// `names`. A value that comes back after another is an error, as it is in Java.
///
/// Behavior: `RowGroupTableIteratorVisitor.SplitByGroupsIterator`, which partitions the
/// table by the columns after `#ensureOrderedForGrouping` has checked the order
fn runs(batch: &RecordBatch, columns: &[OutColumn], names: &[String]) -> Result<Vec<u64>, WriteError> {
    let mut keys = Vec::with_capacity(names.len());
    for name in names {
        let Some(index) = columns.iter().position(|column| column.name == *name) else {
            let available: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
            return Err(invalid(format!(
                "Unknown column names [{name}], available column names are [{}]",
                available.join(", ")
            )));
        };
        if columns[index].column_type.list_element().is_some() {
            return Err(not_ported(
                "io.deephaven.parquet.table.RowGroupTableIteratorVisitor.SplitByGroupsIterator#next",
                format_args!("row groups by {name}, a column of vectors or arrays"),
            ));
        }
        keys.push(as_key(batch.column(index)));
    }
    let fields = keys.iter().map(|key| SortField::new(key.data_type().clone()));
    let problem = |error: arrow::error::ArrowError| invalid(error.to_string());
    let converter = RowConverter::new(fields.collect()).map_err(problem)?;
    let rows = converter.convert_columns(&keys).map_err(problem)?;
    let mut seen: HashSet<&[u8]> = HashSet::new();
    let mut sizes = Vec::new();
    for row in 0..rows.num_rows() {
        let key = rows.row(row);
        if row > 0 && rows.row(row - 1) == key {
            *sizes.last_mut().expect("a run was started at row 0") += 1;
            continue;
        }
        if !seen.insert(key.data()) {
            return Err(invalid(format!(
                "Misordered for Grouping column(s) [{}]: row {row} repeats a group that ended before it",
                names.join(", ")
            )));
        }
        sizes.push(1);
    }
    Ok(sizes)
}

/// The sizes of the row groups of a table with the rows of `batch`, in row order. A
/// group without rows is not written.
///
/// Behavior: `io.deephaven.parquet.table.RowGroupTableIteratorVisitor`
fn row_group_sizes(
    batch: &RecordBatch,
    columns: &[OutColumn],
    row_groups: &RowGroups,
) -> Result<Vec<u64>, WriteError> {
    let rows = batch.num_rows() as u64;
    let sizes = match row_groups {
        RowGroups::Single => vec![rows],
        RowGroups::MaxGroups(groups) => split_evenly(rows, *groups)?,
        RowGroups::MaxRows(max_rows) => split_by_max_rows(rows, *max_rows)?,
        RowGroups::ByGroups { columns: names, max_rows } => {
            let mut sizes = Vec::new();
            for run in runs(batch, columns, names)? {
                match max_rows {
                    Some(max_rows) => sizes.extend(split_by_max_rows(run, *max_rows)?),
                    None => sizes.push(run),
                }
            }
            sizes
        }
    };
    Ok(sizes.into_iter().filter(|size| *size > 0).collect())
}

/// Refuses the row group instructions Java refuses when they are made.
///
/// Behavior: the constructors of `RowGroupInfo.MaxGroups`, `MaxRows` and `ByGroups`
fn check_row_groups(row_groups: &RowGroups) -> Result<(), WriteError> {
    match row_groups {
        RowGroups::MaxGroups(groups) if *groups < 1 => {
            Err(invalid("Cannot define less than 1 RowGroup"))
        }
        RowGroups::MaxRows(max_rows) if *max_rows <= 0 => Err(invalid("MaxRows must be positive")),
        RowGroups::ByGroups { max_rows, .. } if max_rows.is_some_and(|max| max <= 0) => {
            Err(invalid("MaxRows must be positive"))
        }
        RowGroups::ByGroups { columns, .. } if columns.is_empty() => {
            Err(invalid("Must contain at least one group"))
        }
        _ => Ok(()),
    }
}

/// Whether Java gives the strings of `values`, the column chunk of one row group, a
/// dictionary: when it has a string, and neither more distinct strings than `max_keys`
/// nor more bytes of them than `max_size`.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTableWriter#tryEncodeDictionary` and
/// `io.deephaven.parquet.table.transfer.StringDictionary#add`
fn fits_dictionary(values: &ArrayRef, max_keys: i32, max_size: i32) -> bool {
    let strings = match values.as_list_opt::<i32>() {
        Some(list) => list.values().as_string::<i32>(),
        None => values.as_string::<i32>(),
    };
    let mut distinct: HashSet<&str> = HashSet::new();
    let mut size = 0u64;
    for text in strings.iter().flatten() {
        if distinct.insert(text) {
            if distinct.len() as u64 > max_keys as u64 {
                return false;
            }
            size += text.len() as u64;
            if size > max_size as u64 {
                return false;
            }
        }
    }
    // without a string, a chunk with a null is written plain; an empty one has no pages
    !distinct.is_empty() || strings.null_count() == 0
}

/// A file about to be written: the rows of a table as the columns of the file, and the
/// properties of its writer.
struct Plan {
    destination: PathBuf,
    arrow_schema: Arc<Schema>,
    parquet_schema: TypePtr,
    properties: WriterProperties,
    /// The rows of each row group.
    groups: Vec<RecordBatch>,
}

/// Reads the rows of `table` as the columns `columns` and plans its file.
///
/// The caller holds what keeps `table` still: the update graph's lock, for a table that
/// changes. Every row is read here, before a file is touched.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTableWriter#write`, with
/// `#pretransformTable` for a column the table does not have
fn plan(
    table: &dyn Table,
    destination: PathBuf,
    columns: &[OutColumn],
    codec: Compression,
    options: &WriteOptions,
) -> Result<Plan, WriteError> {
    let rows = table.row_set();
    let size = rows.size() as usize;
    let mut fields = Vec::with_capacity(columns.len());
    let mut schema_fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for column in columns {
        let stored = arrow_type(&column.column_type);
        let values = match table.definition().index_of(&column.name) {
            Some(index) => {
                let has = &table.definition().columns()[index].column_type;
                if *has != column.column_type {
                    // Java fails for most pairs of types, and for some writes the
                    // table's values under the definition's type (an int as a short)
                    return Err(not_ported(
                        "io.deephaven.parquet.table.transfer.TransferObject#create",
                        format_args!(
                            "column {} is {has:?} in the table and {:?} in the definition",
                            column.name, column.column_type
                        ),
                    ));
                }
                let values = table.column(index).chunk(&rows);
                let values = values.map_err(|failure| {
                    invalid(format!("Failed to write column {}: {}", column.name, failure.message))
                })?;
                to_file_type(values, &column.column_type)?
            }
            None => new_null_array(&stored, size),
        };
        fields.push(Field::new(&column.parquet_name, stored, true));
        schema_fields.push(schema_field(column)?);
        arrays.push(values);
    }
    let arrow_schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
        .map_err(|error| invalid(error.to_string()))?;
    let sizes = row_group_sizes(&batch, columns, &options.row_groups)?;
    let mut groups = Vec::with_capacity(sizes.len());
    let mut start = 0;
    for size in sizes {
        groups.push(batch.slice(start, size as usize));
        start += size as usize;
    }

    let parquet_schema = Type::group_type_builder("root")
        .with_fields(schema_fields)
        .build()
        .map_err(|error| invalid(error.to_string()))?;
    let page_size = options.target_page_size.unwrap_or(DEFAULT_TARGET_PAGE_SIZE);
    let info = table_info(columns, first_sorted(table).as_ref());
    let mut properties = WriterProperties::builder()
        .set_compression(codec)
        // Java's writer gives each column chunk its statistics, and no page its own
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_data_page_size_limit(page_size as usize)
        // a row group ends where the instructions say, and nowhere else
        .set_max_row_group_row_count(None)
        // Java has a dictionary for strings only
        .set_dictionary_enabled(false)
        .set_key_value_metadata(Some(vec![KeyValue::new(METADATA_KEY.to_string(), info)]));
    let max_keys = options.max_dictionary_keys.unwrap_or(DEFAULT_MAXIMUM_DICTIONARY);
    let max_size = options.max_dictionary_size.unwrap_or(DEFAULT_MAXIMUM_DICTIONARY);
    for (index, column) in columns.iter().enumerate().filter(|(_, c)| c.strings) {
        let (max_keys, max_size) = match column.use_dictionary {
            true => (i32::MAX, i32::MAX),
            false => (max_keys, max_size),
        };
        // Java decides for each row group; a column of this file has a dictionary when
        // every row group would have one
        let fits = |group: &RecordBatch| fits_dictionary(group.column(index), max_keys, max_size);
        if !groups.is_empty() && groups.iter().all(fits) {
            let mut path = vec![column.parquet_name.clone()];
            if column.column_type.list_element().is_some() {
                path.extend(["list".to_string(), "element".to_string()]);
            }
            let path = ColumnPath::new(path);
            properties = properties
                .set_column_dictionary_enabled(path.clone(), true)
                // the limits were applied above; the writer must not fall back by its own
                .set_column_dictionary_page_size_limit(path, usize::MAX);
        }
    }
    Ok(Plan {
        destination,
        arrow_schema,
        parquet_schema: Arc::new(parquet_schema),
        properties: properties.build(),
        groups,
    })
}

/// The file `.{prefix}_{name}` beside `destination`.
///
/// Behavior: `io.deephaven.extensions.trackedfile.LocalCompletableOutputStream#getShadowFile`,
/// `#getBackupFile`
fn beside(destination: &Path, prefix: &str) -> PathBuf {
    let name = destination.file_name().unwrap_or_default().to_string_lossy();
    destination.with_file_name(format!(".{prefix}_{name}"))
}

/// What a write has put on disk so far, to take back if it fails.
#[derive(Default)]
struct Written {
    /// For each destination made ready: the first directory created for it, if any.
    created: Vec<Option<PathBuf>>,
    shadows: Vec<PathBuf>,
    /// The destinations whose new file is in place, each with its old file if it had one.
    installed: Vec<(PathBuf, Option<PathBuf>)>,
}

impl Written {
    /// Makes the directory of `destination`, and refuses a destination Java refuses.
    ///
    /// Behavior: `LocalCompletableOutputStream#prepareDestinationFileLocation`
    fn prepare(&mut self, destination: &Path) -> Result<(), WriteError> {
        if destination.is_dir() {
            return Err(invalid(format!(
                "Destination {} exists and is a directory",
                destination.display()
            )));
        }
        let mut first_created = None;
        let parent = destination.parent().unwrap_or(Path::new("/"));
        if !destination.exists() && !parent.is_dir() {
            for ancestor in parent.ancestors() {
                if ancestor.is_dir() {
                    break;
                }
                if ancestor.exists() {
                    return Err(invalid(format!(
                        "Existing parent file {} of {} is not a directory",
                        ancestor.display(),
                        destination.display()
                    )));
                }
                first_created = Some(ancestor.to_path_buf());
            }
            // noted before it is made, so that a failure half way is taken back too
            self.created.push(first_created);
            fs::create_dir_all(parent).map_err(|error| io(parent, error))?;
        }
        let old = beside(destination, "OLD");
        if old.exists() {
            fs::remove_file(&old).map_err(|error| io(&old, error))?;
        }
        Ok(())
    }

    /// Writes the file of `plan` beside its destination, under the name of a file that
    /// is not finished.
    fn write_shadow(&mut self, plan: &Plan) -> Result<(), WriteError> {
        let shadow = beside(&plan.destination, "NEW");
        let file = fs::File::create(&shadow).map_err(|error| io(&shadow, error))?;
        self.shadows.push(shadow.clone());
        let problem = |error: parquet::errors::ParquetError| io(&plan.destination, error);
        let schema = SchemaDescriptor::new(plan.parquet_schema.clone());
        let writer_options = ArrowWriterOptions::new()
            .with_properties(plan.properties.clone())
            // the types are the Parquet schema's and the `deephaven` entry's, as in a
            // file of Java's; an Arrow schema in the footer would be a second opinion
            .with_skip_arrow_metadata(true)
            .with_parquet_schema(schema);
        let mut writer =
            ArrowWriter::try_new_with_options(BufWriter::new(file), plan.arrow_schema.clone(), writer_options)
                .map_err(problem)?;
        for group in &plan.groups {
            writer.write(group).map_err(problem)?;
            writer.flush().map_err(problem)?;
        }
        let file = writer.into_inner().map_err(problem)?;
        let file = file.into_inner().map_err(|error| io(&shadow, error.error()))?;
        file.sync_all().map_err(|error| io(&shadow, error))
    }

    /// Puts every finished file in the place of its destination.
    ///
    /// Behavior: `LocalCompletableOutputStream#complete` and `#installShadowFile`, then
    /// `#close`, which removes the file that was there before
    fn install(&mut self, plans: &[Plan]) -> Result<(), WriteError> {
        for plan in plans {
            let destination = &plan.destination;
            let old = beside(destination, "OLD");
            let had = destination.exists();
            if had {
                fs::rename(destination, &old).map_err(|error| io(destination, error))?;
            }
            self.installed
                .push((destination.clone(), had.then(|| old.clone())));
            let shadow = beside(destination, "NEW");
            fs::rename(&shadow, destination).map_err(|error| io(destination, error))?;
        }
        for (_, old) in self.installed.drain(..) {
            if let Some(old) = old {
                // the write is done; a file left behind is not its failure
                let _ = fs::remove_file(old);
            }
        }
        Ok(())
    }

    /// Takes back what was written: the unfinished files, the files put in place, and
    /// the directories made.
    ///
    /// Behavior: `LocalCompletableOutputStream#rollback`
    fn roll_back(self) {
        for (destination, old) in self.installed.into_iter().rev() {
            let _ = fs::remove_file(&destination);
            if let Some(old) = old {
                let _ = fs::rename(old, destination);
            }
        }
        for shadow in self.shadows {
            let _ = fs::remove_file(shadow);
        }
        for created in self.created.into_iter().rev().flatten() {
            let _ = fs::remove_dir_all(created);
        }
    }
}

/// The path of a destination, which must name a local file that ends in `.parquet`.
///
/// Behavior: the checks of `io.deephaven.parquet.table.ParquetTools#writeTables` on its
/// destinations; a destination with a URI scheme goes to the store of that scheme in
/// Java, which is not ported
fn destination(path: &str) -> Result<PathBuf, WriteError> {
    if !path.ends_with(EXTENSION) {
        return Err(invalid(format!(
            "Destination {path} does not end in {EXTENSION} extension"
        )));
    }
    if crate::layout::has_scheme(path) {
        return Err(not_ported(
            "io.deephaven.util.channel.SeekableChannelsProviderLoader#load",
            format_args!("a destination with a URI scheme, {path}"),
        ));
    }
    std::path::absolute(path).map_err(|error| io(Path::new(path), error))
}

/// Writes each of `tables` to the Parquet file at the path of `paths` with the same
/// position. Either every file is written or none is: a file that was there before a
/// failed write is there after it, and a directory made for it is not.
///
/// The files have the columns of `options.definition`, or without one the columns the
/// tables have, which must then be the same for all of them.
///
/// A table that changes must be held still by the caller, under its update graph's lock,
/// as for any read of several columns. Every row is read before a file is written.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#writeTables(Table[], String[],
/// ParquetInstructions)`
pub fn write(
    tables: &[Arc<dyn Table>],
    paths: &[&str],
    options: &WriteOptions,
) -> Result<(), WriteError> {
    options.check()?;
    check_row_groups(&options.row_groups)?;
    let Some(first) = tables.first() else {
        return Err(invalid("No source tables provided for writing"));
    };
    if tables.len() != paths.len() {
        return Err(invalid("Number of sources and destinations must match"));
    }
    let definition = match &options.definition {
        Some(definition) => definition,
        None => {
            let definition = first.definition();
            if tables.iter().any(|table| table.definition() != definition) {
                return Err(invalid(
                    "Table definition must be provided when writing multiple tables with \
                     different definitions",
                ));
            }
            definition
        }
    };
    let mut destinations: Vec<PathBuf> = Vec::with_capacity(paths.len());
    for path in paths {
        let destination = destination(path)?;
        if destinations.contains(&destination) {
            // Java writes both and fails to put the second in place
            return Err(invalid(format!(
                "Failed to install shadow file at {path}: the destination is given twice"
            )));
        }
        destinations.push(destination);
    }
    if definition.columns().is_empty() {
        return Err(invalid("Cannot write a parquet table with zero columns"));
    }
    let columns = out_columns(definition, options)?;
    let codec = compression(options.compression.as_deref().unwrap_or("SNAPPY"))?;

    let mut written = Written::default();
    let all = || {
        let mut plans = Vec::with_capacity(tables.len());
        for (table, destination) in tables.iter().zip(destinations) {
            written.prepare(&destination)?;
            let plan = plan(table.as_ref(), destination, &columns, codec, options)?;
            written.write_shadow(&plan)?;
            plans.push(plan);
        }
        written.install(&plans)
    };
    let result = all();
    if result.is_err() {
        written.roll_back();
    }
    result
}

/// Removes the file or the directory at `path`, with everything in it. A path that
/// names nothing is not an error.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#deleteTable`, which is
/// `io.deephaven.base.FileUtils#deleteRecursivelyOnNFS`
pub fn delete(path: &str) -> Result<(), WriteError> {
    let path = Path::new(path);
    let removed = match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => Err(error),
        Ok(found) if found.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
    };
    removed.map_err(|error| io(path, error))
}
