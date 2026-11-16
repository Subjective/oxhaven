//! Writing a table as a tree of directories named `key=value`, one level for each
//! partitioning column, with the rows of each key in a file of their own.
//!
//! Behavior: `io.deephaven.parquet.table.ParquetTools#writeKeyValuePartitionedTable`, both
//! of them, and `#writeKeyValuePartitionedTableImpl`

use super::metadata::MetadataFiles;
use super::{
    check_row_groups, invalid, local_path, out_columns, schema_field, write_files, WriteOptions,
};
use crate::layout::has_scheme;
use crate::{not_ported, WriteError};
use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{DataType, Date32Type, Time64NanosecondType, UInt16Type};
use arrow::row::{RowConverter, SortField};
use oxhaven_column::java_values::{
    big_decimal_parts, big_decimal_text, big_integer_text, local_date_text, local_time_text,
};
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::ops::partitioned::{partition_by, PartitionedTable};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The tokens of a base name.
///
/// Behavior: `io.deephaven.parquet.table.ParquetInstructions#UUID_TOKEN`,
/// `#PARTITIONS_TOKEN`, `#FILE_INDEX_TOKEN`
const UUID_TOKEN: &str = "{uuid}";
const PARTITIONS_TOKEN: &str = "{partitions}";
const FILE_INDEX_TOKEN: &str = "{i}";

const RESOLVE: &str = "io.deephaven.parquet.base.ParquetUtils#resolve";

/// What [`write_partitioned`] writes.
pub enum PartitionedSource<'a> {
    /// A table, split by the partitioning columns of the definition it is written with.
    Table(&'a Arc<dyn Table>),
    /// A partitioned table: one file for each constituent, under the directories of its
    /// keys.
    Partitioned(&'a PartitionedTable),
}

/// Writes `source` under the directory `destination_dir`: the rows of each key in the
/// file `<key>=<value>/.../<base name>.parquet`, without the key columns.
///
/// The base name of `options` names the files: `{uuid}` in it is a random UUID, which is
/// the default name; `{i}` counts the constituents of one key from 0; `{partitions}` is
/// the directory names of the file joined by `_`. A file that is there under a name the
/// write uses is replaced, and every other file and directory stays, as in Java: the
/// directory is not emptied first, so what an earlier write left under other names is
/// still read with the new files. All files are written or none, as in [`super::write`].
///
/// A table that changes must be held still by the caller, under its update graph's lock.
///
/// Behavior: `ParquetTools#writeKeyValuePartitionedTable(Table, String,
/// ParquetInstructions)` and `#writeKeyValuePartitionedTable(PartitionedTable, String,
/// ParquetInstructions)`
pub fn write_partitioned(
    source: PartitionedSource<'_>,
    destination_dir: &str,
    options: &WriteOptions,
) -> Result<(), WriteError> {
    options.check()?;
    check_row_groups(&options.row_groups)?;
    let without = |definition: &TableDefinition, keys: &[String]| {
        let columns = definition.columns().iter();
        let columns = columns.filter(|column| !keys.contains(&column.name));
        TableDefinition::new(columns.cloned().collect())
            .expect("the columns of a definition have different names")
    };
    let (partitioned, key_definition, leaf_definition) = match source {
        PartitionedSource::Table(table) => {
            let definition = options.definition.as_ref();
            let definition = definition.unwrap_or_else(|| table.definition());
            let keys = definition.columns().iter();
            let keys: Vec<ColumnDefinition> = keys.filter(|c| c.partitioning).cloned().collect();
            if keys.is_empty() {
                return Err(invalid(
                    "Table must have partitioning columns to write partitioned data",
                ));
            }
            let names: Vec<String> = keys.iter().map(|key| key.name.clone()).collect();
            let by: Vec<&str> = names.iter().map(String::as_str).collect();
            let partitioned =
                partition_by(table, &by, false).map_err(|error| invalid(error.to_string()))?;
            let leaf = without(definition, &names);
            (partitioned, keys, leaf)
        }
        PartitionedSource::Partitioned(partitioned) => {
            let names = partitioned.key_columns();
            let (keys_from, leaf_from) = match &options.definition {
                Some(definition) => (definition, definition),
                None => (
                    partitioned.table().definition(),
                    partitioned.constituent_definition(),
                ),
            };
            // a key column the definition leaves out still names a directory
            let keys = names.iter().filter_map(|name| {
                let index = keys_from.index_of(name)?;
                Some(keys_from.columns()[index].clone())
            });
            let leaf = without(leaf_from, names);
            (partitioned.clone(), keys.collect(), leaf)
        }
    };
    // Behavior: `ParquetTools#verifyNotAddingIndexOnPartitioningColumn`
    let keys = partitioned.key_columns();
    let on_key = |index: &&Vec<String>| index.len() == 1 && keys.contains(&index[0]);
    if let Some(index) = options.index_columns.iter().find(on_key) {
        return Err(invalid(format!(
            "Cannot add index on partitioning column {}",
            index[0]
        )));
    }
    options.refuse_indexes()?;
    write_constituents(
        &partitioned,
        &key_definition,
        &leaf_definition,
        destination_dir,
        options,
    )
}

/// The text of each value of `values`, a key column named `name` of the type
/// `column_type`, as a directory name has it; a null is the empty text.
///
/// Behavior: `io.deephaven.engine.table.impl.locations.util.PartitionFormatter`
fn key_texts(
    name: &str,
    column_type: &ColumnType,
    values: &ArrayRef,
) -> Result<Vec<String>, WriteError> {
    const FORMATTER: &str =
        "io.deephaven.engine.table.impl.locations.util.PartitionFormatter#formatObject";
    let problem = |error: &dyn std::fmt::Display| invalid(format!("key column {name}: {error}"));
    let unsupported = |simple: &str| invalid(format!("Unsupported type: {simple}"));
    let texts: Vec<Option<String>> = match column_type {
        ColumnType::String
        | ColumnType::Boolean
        | ColumnType::Byte
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long => {
            let values = sentinels_to_nulls(values).map_err(|failure| problem(&failure.message))?;
            let texts =
                arrow::compute::cast(&values, &DataType::Utf8).map_err(|error| problem(&error))?;
            let texts = texts.as_string::<i32>().iter();
            texts.map(|text| text.map(str::to_string)).collect()
        }
        ColumnType::Char => {
            let values = sentinels_to_nulls(values).map_err(|failure| problem(&failure.message))?;
            let units = values.as_primitive::<UInt16Type>().iter();
            units
                .map(|unit| unit.map(|unit| String::from_utf16_lossy(&[unit])))
                .collect()
        }
        ColumnType::BigInteger => {
            let cells = values.as_binary::<i32>().iter();
            cells.map(|cell| cell.map(big_integer_text)).collect()
        }
        ColumnType::BigDecimal => {
            let cells = values.as_binary::<i32>().iter();
            cells
                .map(|cell| {
                    let (scale, unscaled) = big_decimal_parts(cell?)?;
                    Some(big_decimal_text(scale, unscaled))
                })
                .collect()
        }
        ColumnType::LocalDate => {
            let days = values.as_primitive::<Date32Type>().iter();
            days.map(|day| day.map(|day| local_date_text(i64::from(day))))
                .collect()
        }
        ColumnType::LocalTime => {
            let nanos = values.as_primitive::<Time64NanosecondType>().iter();
            nanos.map(|nanos| nanos.map(local_time_text)).collect()
        }
        ColumnType::Float | ColumnType::Double | ColumnType::Instant => {
            return Err(not_ported(
                FORMATTER,
                format_args!("a directory named by {name}, a key column of type {column_type:?}"),
            ))
        }
        ColumnType::LocalDateTime => return Err(unsupported("LocalDateTime")),
        other => return Err(unsupported(&format!("{other:?}"))),
    };
    Ok(texts.into_iter().map(Option::unwrap_or_default).collect())
}

/// The rows of `keys`, the key columns of a table of `rows` rows, in groups of equal
/// keys: the groups in the order of their first rows, the rows of a group in theirs.
///
/// Behavior: the `groupBy` of `ParquetTools#writeKeyValuePartitionedTableImpl`
fn groups_of(keys: &[ArrayRef], rows: usize) -> Result<Vec<Vec<usize>>, WriteError> {
    if keys.is_empty() {
        return Ok(if rows == 0 {
            Vec::new()
        } else {
            vec![(0..rows).collect()]
        });
    }
    let problem = |error: arrow::error::ArrowError| invalid(error.to_string());
    let fields = keys
        .iter()
        .map(|key| SortField::new(key.data_type().clone()));
    let converter = RowConverter::new(fields.collect()).map_err(problem)?;
    let converted = converter.convert_columns(keys).map_err(problem)?;
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut group_of: HashMap<&[u8], usize> = HashMap::new();
    for row in 0..converted.num_rows() {
        let key = converted.row(row);
        let group = *group_of.entry(key.data()).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[group].push(row);
    }
    Ok(groups)
}

/// The path `relative` from the directory `base`, as Java resolves it: `\` is `/`, and
/// every other character is itself.
///
/// Java makes a URI of `relative`, which fails where a `:` comes before the first `/`
/// and what is before it is no URI scheme; that is every `:` in the value of the first
/// key, so a table whose first key is a time cannot be written. Where it is a scheme,
/// Java writes to the store of that scheme, and where a part is `.` or `..` or the path
/// starts at the root, to another directory: these are refused.
///
/// Behavior: `io.deephaven.parquet.base.ParquetUtils#resolve`
fn resolved(base: &Path, relative: &str) -> Result<PathBuf, WriteError> {
    let relative = relative.replace('\\', "/");
    let first = relative.split('/').next().unwrap_or_default();
    if first.contains(':') {
        if has_scheme(&relative) {
            return Err(not_ported(
                RESOLVE,
                format_args!("{relative}, a name that starts with a URI scheme"),
            ));
        }
        return Err(invalid(format!(
            "Failed to create URI from relative path: {relative}"
        )));
    }
    let leaves =
        relative.starts_with('/') || relative.split('/').any(|part| matches!(part, "." | ".."));
    if leaves {
        return Err(not_ported(
            RESOLVE,
            format_args!("{relative}, a path that leaves its directory"),
        ));
    }
    Ok(base.join(relative))
}

/// Behavior: `ParquetTools#writeKeyValuePartitionedTableImpl`
fn write_constituents(
    partitioned: &PartitionedTable,
    key_definition: &[ColumnDefinition],
    leaf_definition: &TableDefinition,
    destination_dir: &str,
    options: &WriteOptions,
) -> Result<(), WriteError> {
    if leaf_definition.columns().is_empty() {
        return Err(invalid(
            "Cannot write a partitioned parquet table without any non-partitioning columns",
        ));
    }
    let base_name = options.base_name.as_deref().unwrap_or(UUID_TOKEN);
    let with_index = base_name.contains(FILE_INDEX_TOKEN);
    let with_uuid = base_name.contains(UUID_TOKEN);
    if !partitioned.unique_keys() && !with_index && !with_uuid {
        return Err(invalid(
            "Cannot write a partitioned parquet table with non-unique keys without {i} or \
             {uuid} in the base name because there can be multiple partitions with the same \
             key values",
        ));
    }

    // the names of the directories of each row of the underlying table
    let key_table = partitioned.table();
    let rows = key_table.row_set();
    let size = rows.size() as usize;
    let mut directories: Vec<Vec<String>> = vec![Vec::new(); size];
    let mut keys = Vec::with_capacity(partitioned.key_columns().len());
    for name in partitioned.key_columns() {
        let index = key_table.definition().index_of(name);
        let index = index.expect("a partitioned table has its key columns");
        let column_type = &key_table.definition().columns()[index].column_type;
        let values = key_table.column(index).chunk(&rows);
        let values = values.map_err(|failure| invalid(failure.message))?;
        let texts = key_texts(name, column_type, &values)?;
        for (directory, text) in directories.iter_mut().zip(texts) {
            directory.push(format!("{name}={text}"));
        }
        keys.push(values);
    }
    let constituents = partitioned
        .constituents()
        .map_err(|error| invalid(error.to_string()))?;

    let root = local_path(destination_dir)?;
    let mut tables: Vec<Arc<dyn Table>> = Vec::with_capacity(size);
    let mut destinations = Vec::with_capacity(size);
    for group in groups_of(&keys, size)? {
        let partitions = &directories[group[0]];
        let relative: String = partitions.iter().map(|name| format!("{name}/")).collect();
        let directory = resolved(&root, &relative)?;
        for (count, row) in group.into_iter().enumerate() {
            let mut name = base_name.replace(PARTITIONS_TOKEN, &partitions.join("_"));
            name = name.replace(FILE_INDEX_TOKEN, &count.to_string());
            if with_uuid {
                name = name.replace(UUID_TOKEN, &uuid::Uuid::new_v4().to_string());
            }
            name.push_str(super::EXTENSION);
            destinations.push(resolved(&directory, &name)?);
            tables.push(constituents[row].clone());
        }
    }

    let metadata = match options.generate_metadata_files {
        true => {
            // Behavior: `ParquetTableWriter#getSchemaForTable` of the key columns
            if key_definition.is_empty() {
                return Err(invalid("Table definition must have at least one column"));
            }
            let definition = TableDefinition::new(key_definition.to_vec())
                .expect("the key columns have different names");
            let columns = out_columns(&definition, options, std::slice::from_ref(key_table))?;
            let fields = columns.iter().map(schema_field);
            Some(MetadataFiles {
                root: root.clone(),
                partitioning: fields.collect::<Result<_, _>>()?,
            })
        }
        false => None,
    };
    if tables.is_empty() {
        // Java asks the first destination for its scheme without looking whether there
        // is one, and fails with this
        return Err(invalid(
            "java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0",
        ));
    }
    write_files(&tables, leaf_definition, destinations, options, metadata)
}
