//! Parquet files as tables, and tables as Parquet files.
//!
//! [`write`] is `io.deephaven.parquet.table.ParquetTools#writeTables`: it is in `write.rs`.
//!
//! [`read`] and [`read_in`] are `io.deephaven.parquet.table.ParquetTools#readTable`: from a
//! path to a table whose columns are read from the files when an operation asks for them.
//! The table is `oxhaven_engine::source::stored_table` over one
//! [`location::ParquetLocation`] per file, or `following_table` when it is to follow the
//! files that appear in a directory. `docs/STORAGE.md` has the design, and the list of
//! what Java reads and this crate refuses for now.

mod layout;
mod location;
mod schema;
mod write;

#[cfg(test)]
mod tests;

use arrow::array::{make_comparator, Array, ArrayRef};
use arrow::compute::SortOptions;
use layout::{Files, Found};
use location::ParquetLocation;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::source::{following_table, stored_table, LocationProvider, TableLocation};
use oxhaven_engine::table::QueryTable;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

pub use layout::Layout;
pub use write::{delete, write, RowGroups, WriteColumn, WriteOptions};

/// Why a table could not be read, or written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadError {
    /// Java does this and this crate does not yet: `java` names the method that defines
    /// the behavior, as `unported!` would. An error and not a panic, because a user's file
    /// or argument can reach it.
    NotPorted { java: &'static str, what: String },
    /// An argument or a file that Java refuses as well.
    Invalid(String),
    /// A file or directory could not be read.
    Io(String),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::NotPorted { java, what } => write!(f, "not ported yet: {java}: {what}"),
            ReadError::Invalid(message) | ReadError::Io(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ReadError {}

/// Why tables could not be written: the same three cases as for a read.
pub type WriteError = ReadError;

impl From<ReadError> for TableFailure {
    fn from(error: ReadError) -> Self {
        TableFailure {
            message: error.to_string(),
        }
    }
}

pub(crate) fn not_ported(java: &'static str, what: impl fmt::Display) -> ReadError {
    ReadError::NotPorted {
        java,
        what: what.to_string(),
    }
}

/// The type an unsigned 64-bit Parquet column is read as, where it is not Java's default,
/// a `BigInteger`.
///
/// Behavior: `io.deephaven.parquet.table.ParquetInstructions.UnsignedLongTarget`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsignedLongTarget {
    /// A `long`; a value above the largest `long` is an error when it is read.
    Long,
    /// A `long` with the same bits, so that a value above the largest `long` is negative.
    SignedLong,
}

/// What to do with one column of the files.
///
/// Behavior: the per-column settings of `io.deephaven.parquet.table.ParquetInstructions`
/// that `deephaven.parquet.ColumnInstruction` sets for a read
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnInstruction {
    /// The column's name in the files.
    pub parquet_name: String,
    /// The column's name in the table, where it is another.
    pub name: Option<String>,
    /// The class of an object codec. Not ported: naming one is refused.
    pub codec: Option<String>,
    pub unsigned_long: Option<UnsignedLongTarget>,
}

/// The arguments of [`read`] besides the path.
///
/// Behavior: `io.deephaven.parquet.table.ParquetInstructions`, the settings that apply to
/// reading
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadOptions {
    /// How the files are laid out; `None` decides by the path.
    pub layout: Option<Layout>,
    /// Whether the table follows files that appear later.
    pub refreshing: bool,
    /// Whether a binary column without a logical type holds strings, as files of old
    /// Deephaven versions do.
    pub legacy: bool,
    pub columns: Vec<ColumnInstruction>,
}

impl ReadOptions {
    /// Refuses the instructions Java refuses, and the ones that are not ported.
    ///
    /// Behavior: `io.deephaven.parquet.table.ParquetInstructions.Builder#addColumnNameMapping`
    fn check(&self) -> Result<(), ReadError> {
        for (index, column) in self.columns.iter().enumerate() {
            let parquet_name = &column.parquet_name;
            let Some(name) = &column.name else {
                // Java: a `NullPointerException` out of the builder's map
                return Err(ReadError::Invalid(format!(
                    "the instruction for the parquet column {parquet_name} names no column"
                )));
            };
            let earlier = self.columns[..index].iter();
            let same_name = earlier.clone().find(|earlier| {
                earlier.name.as_ref() == Some(name) && earlier.parquet_name != *parquet_name
            });
            if let Some(earlier) = same_name {
                // Behavior: `ParquetInstructions.ColumnInstructions#setParquetColumnName`
                return Err(ReadError::Invalid(format!(
                    "Cannot add a mapping from parquetColumnName={parquet_name}: \
                     columnName={name} already mapped to parquetColumnName={}",
                    earlier.parquet_name
                )));
            }
            let earlier = earlier.filter(|earlier| earlier.parquet_name == *parquet_name);
            if let Some(mapped) = earlier.filter_map(|c| c.name.as_ref()).find(|n| *n != name) {
                return Err(ReadError::Invalid(format!(
                    "Cannot add new mapping from parquetColumnName={parquet_name} to \
                     columnName={name}: already mapped to columnName={mapped}"
                )));
            }
            if column.codec.is_some() {
                // Java takes the codec, and ignores it for a column that needs none
                return Err(not_ported(
                    "io.deephaven.parquet.table.ParquetInstructions.Builder#addColumnCodec",
                    format_args!("a codec for column {name}"),
                ));
            }
        }
        Ok(())
    }

    /// The table's name for the file column `parquet_name`, if an instruction gives one.
    ///
    /// Behavior: `ParquetInstructions#getColumnNameFromParquetColumnName`
    pub(crate) fn name_of(&self, parquet_name: &str) -> Option<&str> {
        let named = |column: &&ColumnInstruction| column.parquet_name == parquet_name;
        self.columns.iter().find(named)?.name.as_deref()
    }

    /// Behavior: `ParquetInstructions#getUnsignedLongTarget`, keyed by the table's name
    pub(crate) fn unsigned_long_of(&self, name: &str) -> Option<UnsignedLongTarget> {
        let named = |column: &&ColumnInstruction| column.name.as_deref() == Some(name);
        // a later instruction for the same column replaces an earlier one
        self.columns.iter().rev().find(named)?.unsigned_long
    }
}

/// How long a table that follows a directory waits before it looks at the directory
/// again.
///
/// Behavior: `io.deephaven.engine.table.impl.locations.util.TableDataRefreshService.Helper#DEFAULT_TABLE_LOCATION_REFRESH_MILLIS`
pub const LOCATIONS_REFRESH: Duration = Duration::from_secs(60);

/// A static table of the rows of the Parquet file or files at `path`.
///
/// Only the footers are read here: the last file's gives the columns, and every file's
/// gives its size. A column's values are read when they are first asked for.
///
/// Options that ask for a table that follows a directory are refused: that table needs an
/// update graph, which [`read_in`] takes.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#readTable(String, ParquetInstructions)`
pub fn read(path: &str, options: &ReadOptions) -> Result<Arc<QueryTable>, ReadError> {
    read_table(path, options, None)
}

/// As [`read`], and with `options.refreshing` a table that gains, on a cycle of `graph`,
/// the rows of the files that have appeared under the directory at `path`. It looks at the
/// directory when [`LOCATIONS_REFRESH`] has passed since it last did.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#readTable(String, ParquetInstructions)`
/// with `ParquetInstructions#isRefreshing`
pub fn read_in(
    graph: &Arc<dyn UpdateGraph>,
    path: &str,
    options: &ReadOptions,
) -> Result<Arc<QueryTable>, ReadError> {
    read_table(path, options, Some((graph.clone(), LOCATIONS_REFRESH)))
}

/// [`read_in`], with the graph of a table that follows its directory and how long it
/// waits between two looks at it.
pub(crate) fn read_table(
    path: &str,
    options: &ReadOptions,
    following: Option<(Arc<dyn UpdateGraph>, Duration)>,
) -> Result<Arc<QueryTable>, ReadError> {
    options.check()?;
    let mut found = layout::find(path, options.layout, options.refreshing)?;
    let layout = found.layout;
    let metadata = found.metadata.take();
    let arranged = arrange(path, found.into())?;
    // Behavior: `ParquetTools#infer`, which reads the columns from the last location, and
    // the constructor of `ParquetMetadataFileLayout`, which reads them from the metadata file
    let last = arranged.files.last().map(|(path, _)| path);
    let columns_of = metadata.as_ref().map(|metadata| &metadata.path).or(last);
    let columns_of = columns_of.ok_or_else(|| {
        let text = "Unable to infer schema for a partitioned parquet table when there are no \
                    initial parquet files";
        ReadError::Invalid(text.to_string())
    })?;
    let mut file_columns = ParquetLocation::file_columns(columns_of, options)?;
    if let Some(common) = metadata
        .as_ref()
        .and_then(|metadata| metadata.common.as_ref())
    {
        file_columns = common_columns(columns_of, file_columns, common, options)?;
    }
    if let Some((last, values)) = arranged.files.last() {
        for ((key, _), value) in arranged.keys.iter().zip(values) {
            if value.is_null(0) {
                return Err(ReadError::Invalid(format!(
                    "Last location key {} has null partition value at partition key {key}",
                    last.display()
                )));
            }
        }
    }
    // the partitioning columns first; a column of the files with the name of one is not
    // read
    let partitioning = arranged.keys.iter();
    let partitioning = partitioning.map(|(k, t)| ColumnDefinition::partitioning(k, t.clone()));
    let is_key = |name: &str| arranged.keys.iter().any(|(key, _)| key == name);
    let of_files = file_columns.into_iter().filter(|c| !is_key(&c.name));
    let definition = TableDefinition::new(partitioning.chain(of_files).collect())
        .map_err(|duplicate| ReadError::Invalid(duplicate.to_string()))?;
    if options.refreshing && layout == Layout::SingleFile {
        // Behavior: `ParquetTools#readTable(ParquetTableLocationKey, ParquetInstructions)`
        let text = "Unable to have a refreshing single parquet file";
        return Err(ReadError::Invalid(text.to_string()));
    }
    let mut locations: Vec<Arc<dyn TableLocation>> = Vec::with_capacity(arranged.files.len());
    let mut known = HashSet::new();
    for (index, (file, values)) in arranged.files.iter().enumerate() {
        let location = arranged.open(file, values, &definition, options)?;
        let listed = metadata.as_ref().map(|metadata| metadata.row_groups[index]);
        if let Some(listed) = listed.filter(|listed| *listed != location.row_groups()) {
            // Java reads the row groups the metadata file lists, by what it says of them
            return Err(not_ported(
                "io.deephaven.parquet.table.layout.ParquetMetadataFileLayout#create",
                format_args!(
                    "{}: the metadata file lists {listed} of its {} row groups",
                    file.display(),
                    location.row_groups()
                ),
            ));
        }
        locations.push(Arc::new(location));
        known.insert(file.clone());
    }
    let invalid = |failure: TableFailure| ReadError::Invalid(failure.message);
    if !options.refreshing {
        return stored_table(definition, locations).map_err(invalid);
    }
    let Some((graph, interval)) = following else {
        let text = "a table that follows the files of a directory needs an update graph";
        return Err(ReadError::Invalid(text.to_string()));
    };
    let provider = Arc::new(Directory {
        path: path.to_string(),
        options: ReadOptions {
            layout: Some(layout),
            ..options.clone()
        },
        definition: definition.clone(),
        interval,
        seen: Mutex::new((known, Instant::now())),
    });
    following_table(graph, definition, locations, provider).map_err(invalid)
}

/// The columns of a table read through the metadata file `metadata`, whose columns are
/// `leaf`, when the directory also has the file `common`: the columns of `common`, each of
/// which must be a column of `metadata`. A column only `common` has is a partitioning
/// column, which is not ported.
///
/// Behavior: the constructor of `io.deephaven.parquet.table.layout.ParquetMetadataFileLayout`
fn common_columns(
    metadata: &std::path::Path,
    leaf: Vec<ColumnDefinition>,
    common: &std::path::Path,
    options: &ReadOptions,
) -> Result<Vec<ColumnDefinition>, ReadError> {
    let full = ParquetLocation::file_columns(common, options)?;
    for column in &full {
        match leaf.iter().find(|leaf| leaf.name == column.name) {
            None => {
                return Err(not_ported(
                    "io.deephaven.parquet.table.layout.ParquetMetadataFileLayout#create",
                    format_args!(
                        "the partitioning column {} of {}",
                        column.name,
                        common.display()
                    ),
                ))
            }
            Some(leaf) if leaf == column => {}
            Some(_) => {
                return Err(ReadError::Invalid(format!(
                    "Schema mismatch between {} and {} for column {}",
                    metadata.display(),
                    common.display(),
                    column.name
                )))
            }
        }
    }
    Ok(full)
}

/// The files of a table in the order of their rows, each with the values of its
/// partitions.
struct Arranged {
    /// The partitioning columns.
    keys: Vec<(String, ColumnType)>,
    /// For each file, the value of each partitioning column: an array of one element.
    files: Vec<(PathBuf, Vec<ArrayRef>)>,
}

impl Arranged {
    fn open(
        &self,
        file: &std::path::Path,
        values: &[ArrayRef],
        definition: &TableDefinition,
        options: &ReadOptions,
    ) -> Result<ParquetLocation, ReadError> {
        let keys = self.keys.iter().map(|(key, _)| key.as_str());
        let partitions: Vec<_> = keys.zip(values.iter().cloned()).collect();
        ParquetLocation::open(file, definition, options, &partitions)
    }
}

/// A value as a cell of a line of CSV.
///
/// Behavior: `org.apache.commons.text.StringEscapeUtils#escapeCsv`
fn csv_cell(value: &str) -> String {
    if value.contains([',', '"', '\r', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Gives the partition values of `files` their types and puts the files in the order of
/// their rows: by the values of their partitions, then by path. A file a metadata file
/// lists has no partitions and stays where it is listed.
///
/// The types are the ones the CSV reader infers for a file whose header is the keys and
/// whose rows are the values, as in Java. Where that file has fewer rows than there are
/// files, because the reader skipped a line, the last files are left out, as in Java.
///
/// Behavior: `io.deephaven.engine.table.impl.locations.local.LocationTableBuilderCsv#build`,
/// `KeyValuePartitionLayout#buildLocationKeys`, and for the order
/// `io.deephaven.engine.table.impl.locations.local.URITableLocationKey#compareTo`
fn arrange(root: &str, found: FoundFiles) -> Result<Arranged, ReadError> {
    let FoundFiles { files, keys } = found;
    if keys.is_empty() {
        let files = files.into_iter();
        return Ok(Arranged {
            keys: Vec::new(),
            files: files.map(|file| (file.path, Vec::new())).collect(),
        });
    }
    let mut text = String::new();
    let line = |cells: &[String]| {
        let cells: Vec<String> = cells.iter().map(|cell| csv_cell(cell)).collect();
        cells.join(",") + "\n"
    };
    text.push_str(&line(&keys));
    for file in &files {
        text.push_str(&line(&file.values));
    }
    let failed = |error: &dyn fmt::Display| {
        ReadError::Invalid(format!(
            "Failed converting partition CSV to table for {root}: {error}"
        ))
    };
    let table = oxhaven_csv::read_bytes(text.as_bytes(), &oxhaven_csv::ReadOptions::default())
        .map_err(|error| failed(&error))?;
    let rows = table.row_set();
    let mut columns = Vec::new();
    let mut keys = Vec::new();
    for (index, column) in table.definition().columns().iter().enumerate() {
        let values = table.column(index).chunk(&rows);
        columns.push(values.map_err(|failure| failed(&failure.message))?);
        keys.push((column.name.clone(), column.column_type.clone()));
    }
    let count = files.len().min(rows.size() as usize);
    let order = SortOptions {
        descending: false,
        nulls_first: true,
    };
    let mut compare = Vec::with_capacity(columns.len());
    for column in &columns {
        let comparator = make_comparator(column, column, order);
        compare.push(comparator.map_err(|error| failed(&error))?);
    }
    let mut indices: Vec<usize> = (0..count).collect();
    indices.sort_by(|a, b| {
        let mut by_partitions = compare.iter().map(|compare| compare(*a, *b));
        let by_partitions = by_partitions.find(|order| *order != Ordering::Equal);
        by_partitions.unwrap_or_else(|| files[*a].path.cmp(&files[*b].path))
    });
    let files = indices.into_iter().map(|index| {
        let values = columns.iter().map(|column| column.slice(index, 1));
        (files[index].path.clone(), values.collect())
    });
    Ok(Arranged {
        keys,
        files: files.collect(),
    })
}

/// The files of a table and the names of their partitions, before the values have types.
struct FoundFiles {
    files: Vec<Found>,
    keys: Vec<String>,
}

/// The directory a table follows.
///
/// Behavior: `io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider`
/// over a `ParquetFlatPartitionedLayout` or a `ParquetKeyValuePartitionedLayout`
struct Directory {
    path: String,
    options: ReadOptions,
    definition: TableDefinition,
    interval: Duration,
    /// The files that are locations of the table, and when the directory was last looked
    /// at.
    seen: Mutex<(HashSet<PathBuf>, Instant)>,
}

impl LocationProvider for Directory {
    /// The files that were not there at the last look, in the order of
    /// [`arrange`]. The partitions of the directory must still have the names and types
    /// of the table's partitioning columns; Java gives a value of another type to the
    /// column as it is, and fails when the value is read.
    ///
    /// Behavior: `PollingTableLocationProvider#refresh`, and
    /// `io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager#refresh`,
    /// which sorts the locations it had not seen
    fn poll(&self) -> Result<Vec<Arc<dyn TableLocation>>, TableFailure> {
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.1.elapsed() < self.interval {
            return Ok(Vec::new());
        }
        seen.1 = Instant::now();
        let found = layout::find(&self.path, self.options.layout, true)?;
        let arranged = arrange(&self.path, found.into())?;
        let partitioning = self.definition.columns().iter().filter(|c| c.partitioning);
        let partitioning: Vec<_> = partitioning
            .map(|column| (column.name.clone(), column.column_type.clone()))
            .collect();
        if arranged.keys != partitioning {
            return Err(TableFailure {
                message: format!(
                    "the partitions of {} are now {:?}; the table was made with {partitioning:?}",
                    self.path, arranged.keys
                ),
            });
        }
        let mut locations: Vec<Arc<dyn TableLocation>> = Vec::new();
        for (file, values) in &arranged.files {
            if seen.0.contains(file) {
                continue;
            }
            let location = arranged.open(file, values, &self.definition, &self.options)?;
            locations.push(Arc::new(location));
            seen.0.insert(file.clone());
        }
        Ok(locations)
    }
}

impl From<Files> for FoundFiles {
    fn from(found: Files) -> Self {
        FoundFiles {
            files: found.files,
            keys: found.keys,
        }
    }
}
