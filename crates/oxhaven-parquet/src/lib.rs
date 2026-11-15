//! Reading Parquet files as tables.
//!
//! [`read`] is `io.deephaven.parquet.table.ParquetTools#readTable`: from a path to a static
//! table whose columns are read from the files when an operation asks for them. The table
//! is `oxhaven_engine::source::stored_table` over one [`location::ParquetLocation`] per
//! file. `docs/STORAGE.md` has the design, and the list of what Java reads and this crate
//! refuses for now.

mod layout;
mod location;
mod schema;

#[cfg(test)]
mod tests;

use location::ParquetLocation;
use oxhaven_contracts::table::TableFailure;
use oxhaven_engine::source::{stored_table, TableLocation};
use oxhaven_engine::table::QueryTable;
use std::fmt;
use std::sync::Arc;

pub use layout::Layout;

/// Why a table could not be read.
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

/// A static table of the rows of the Parquet file or files at `path`.
///
/// Only the footers are read here: the last file's gives the columns, and every file's
/// gives its size. A column's values are read when they are first asked for.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTools#readTable(String, ParquetInstructions)`
pub fn read(path: &str, options: &ReadOptions) -> Result<Arc<QueryTable>, ReadError> {
    options.check()?;
    let files = layout::find(path, options.layout)?;
    // Behavior: `ParquetTools#infer`, which reads the columns from the last location
    let last = files.paths.last().ok_or_else(|| {
        let text = "Unable to infer schema for a partitioned parquet table when there are no \
                    initial parquet files";
        ReadError::Invalid(text.to_string())
    })?;
    let (definition, last_location) = ParquetLocation::infer(last, options)?;
    if options.refreshing {
        return Err(if files.single {
            // Behavior: `ParquetTools#readTable(ParquetTableLocationKey, ParquetInstructions)`
            ReadError::Invalid("Unable to have a refreshing single parquet file".to_string())
        } else {
            not_ported(
                "io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider#refresh",
                "a table that follows the files of a directory",
            )
        });
    }
    let mut locations: Vec<Arc<dyn TableLocation>> = Vec::with_capacity(files.paths.len());
    for file in &files.paths[..files.paths.len() - 1] {
        locations.push(Arc::new(ParquetLocation::open(file, &definition, options)?));
    }
    locations.push(Arc::new(last_location));
    stored_table(definition, locations).map_err(|failure| ReadError::Invalid(failure.message))
}
