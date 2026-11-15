//! Reading and writing CSV files.
//!
//! [`read`] is `io.deephaven.csv.CsvTools#readCsv`: from a path to a static in-memory
//! table, with the column types Java's reader (the `deephaven-csv` library) infers from
//! the cells. [`write`] is `io.deephaven.csv.CsvTools#writeCsv`. `docs/CSV.md` has the
//! design, the rules of the inference, and the list of what Java does and this crate
//! refuses for now.

mod cells;
mod infer;
mod names;
mod tokens;
mod write;
mod zones;

#[cfg(test)]
mod tests;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::TableFailure;
use oxhaven_engine::table::{empty_table, in_memory_table, QueryTable};
use std::fmt;
use std::sync::Arc;

pub use infer::Parser;
pub use write::write;

/// Why a file could not be read or written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CsvError {
    /// Java does this and this crate does not yet: `java` names the method that defines
    /// the behavior, as `unported!` would. An error and not a panic, because a user's file
    /// or argument can reach it.
    NotPorted { java: &'static str, what: String },
    /// An argument or a file that Java refuses as well.
    Invalid(String),
    /// A file could not be read or written.
    Io(String),
}

impl fmt::Display for CsvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CsvError::NotPorted { java, what } => write!(f, "not ported yet: {java}: {what}"),
            CsvError::Invalid(message) | CsvError::Io(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for CsvError {}

impl From<CsvError> for TableFailure {
    fn from(error: CsvError) -> Self {
        TableFailure {
            message: error.to_string(),
        }
    }
}

pub(crate) fn not_ported(java: &'static str, what: impl fmt::Display) -> CsvError {
    CsvError::NotPorted {
        java,
        what: what.to_string(),
    }
}

/// A column the caller names, with the type to read it as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    pub name: String,
    pub parser: Parser,
}

/// The arguments of [`read`] besides the path.
///
/// Behavior: `io.deephaven.csv.CsvSpecs`, the settings `deephaven.csv.read` sets, with
/// their defaults
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOptions {
    /// The names and types of the columns, in place of the file's headers and of
    /// inference. Empty to take both from the file.
    pub columns: Vec<ColumnSpec>,
    /// Whether the file has a row of headers. Without one and without `columns`, the
    /// columns are named `Column1`, `Column2` and so on.
    pub has_header_row: bool,
    /// The number of rows before the header row.
    pub skip_header_rows: i64,
    /// The number of data rows to pass over.
    pub skip_rows: i64,
    /// The largest number of data rows to read.
    pub num_rows: i64,
    /// Whether a line without any text is no row. Otherwise it is a row with one empty
    /// cell.
    pub ignore_empty_lines: bool,
    /// Whether a row with fewer cells than there are columns is filled up with nulls.
    pub allow_missing_columns: bool,
    /// Whether the cells of a row beyond the last column are dropped.
    pub ignore_excess_columns: bool,
    /// What separates the cells of a row. Java takes none but a 7-bit ASCII character.
    pub delimiter: char,
    /// What a quoted cell begins and ends with; the same holds for it.
    pub quote: char,
    /// Whether spaces and tabs around a cell that is not quoted are no part of it. They
    /// are also what may precede the quote of a quoted cell.
    pub ignore_surrounding_spaces: bool,
    /// Whether spaces and tabs at the ends of a quoted cell are no part of it.
    pub trim: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            columns: Vec::new(),
            has_header_row: true,
            skip_header_rows: 0,
            skip_rows: 0,
            num_rows: i64::MAX,
            ignore_empty_lines: false,
            allow_missing_columns: false,
            ignore_excess_columns: false,
            delimiter: ',',
            quote: '"',
            ignore_surrounding_spaces: true,
            trim: false,
        }
    }
}

impl ReadOptions {
    /// Refuses the settings Java refuses, with every reason in one message.
    ///
    /// Behavior: `io.deephaven.csv.CsvSpecs#check`
    fn check(&self) -> Result<(), CsvError> {
        let mut problems = Vec::new();
        for (what, character) in [("quote", self.quote), ("delimiter", self.delimiter)] {
            if !character.is_ascii() {
                problems.push(format!(
                    "{what} is set to '{character}' but is required to be 7-bit ASCII"
                ));
            }
        }
        for (what, value) in [
            ("skipRows", self.skip_rows),
            ("skipHeaderRows", self.skip_header_rows),
            ("numRows", self.num_rows),
        ] {
            if value < 0 {
                problems.push(format!(
                    "{what} is set to {value}, but is required to be nonnegative"
                ));
            }
        }
        if !self.has_header_row && self.skip_header_rows > 0 {
            problems.push("skipHeaderRows != 0 but hasHeaderRow is not set".to_string());
        }
        if problems.is_empty() {
            return Ok(());
        }
        Err(CsvError::Invalid(format!(
            "CsvSpecs failed validation for the following reasons: {}",
            problems.join(", ")
        )))
    }
}

/// The file a path names, which may be a `file:` URL.
///
/// Behavior: `io.deephaven.csv.CsvTools#readCsv(String, CsvSpecs)`, which reads a path that
/// parses as a URL through the URL unless it is a plain `file:` one, and
/// `io.deephaven.engine.util.PathUtil#open`, which unpacks a file by the end of its name
fn file_of(path: &str) -> Result<&str, CsvError> {
    // the protocols a JVM has a handler for; any other `x:` is not a URL to it
    const PROTOCOLS: [&str; 8] = [
        "file", "ftp", "http", "https", "jar", "jmod", "jrt", "mailto",
    ];
    let mut file = path;
    if let Some((protocol, rest)) = path.split_once(':') {
        if PROTOCOLS.iter().any(|p| protocol.eq_ignore_ascii_case(p)) {
            let plain = protocol.eq_ignore_ascii_case("file")
                && !rest.starts_with("//")
                && !rest.contains(['?', '#']);
            if !plain {
                return Err(not_ported(
                    "io.deephaven.csv.CsvTools#readCsv(URL, CsvSpecs)",
                    format_args!("reading the URL {path}"),
                ));
            }
            file = rest;
        }
    }
    for packed in [".zip", ".bz2", ".gz", ".7z", ".zst", ".tar"] {
        if file.ends_with(packed) {
            return Err(not_ported(
                "io.deephaven.engine.util.PathUtil#open",
                format_args!("unpacking a {packed} file"),
            ));
        }
    }
    Ok(file)
}

/// A static table of the rows of the CSV file at `path`, read whole.
///
/// Behavior: `io.deephaven.csv.CsvTools#readCsv(String, CsvSpecs)` with the specs of
/// `CsvTools#builder`
pub fn read(path: &str, options: &ReadOptions) -> Result<Arc<QueryTable>, CsvError> {
    options.check()?;
    let file = file_of(path)?;
    let input = std::fs::read(file).map_err(|error| CsvError::Io(format!("{file}: {error}")))?;
    read_bytes(&input, options)
}

/// As [`read`], of the content of a file.
///
/// Behavior: `io.deephaven.csv.CsvTools#readCsv(InputStream, CsvSpecs)`
pub fn read_bytes(input: &[u8], options: &ReadOptions) -> Result<Arc<QueryTable>, CsvError> {
    options.check()?;
    if std::str::from_utf8(input).is_err() {
        // Java reads the bytes: a malformed sequence is U+FFFD in a string, and a byte
        // `0xFF` is an escape character, because that is its marker for having none
        return Err(not_ported(
            "io.deephaven.csv.reading.cells.DelimitedCellGrabber#grabNext",
            "a file that is not UTF-8",
        ));
    }
    let cells = cells::read_cells(input, options)?;
    // a caller's type goes with the name as the caller wrote it
    let parser_of = |header: &str| {
        let named = |column: &&ColumnSpec| column.name == header;
        options.columns.iter().find(named).map(|c| c.parser)
    };
    let names = names::legalize(&cells.headers)?;
    let mut definitions = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(names.len());
    for ((header, name), column) in cells.headers.iter().zip(names).zip(&cells.columns) {
        let values = infer::column_values(column, parser_of(header))?;
        let column_type = values.column_type();
        fields.push(Field::new(&name, column_type.arrow_type(), true));
        definitions.push(ColumnDefinition::new(name, column_type));
        arrays.push(values.into_array()?);
    }
    if arrays.is_empty() {
        // a file whose only header is empty: rows, and no column
        return Ok(empty_table(cells.rows as u64));
    }
    let invalid = |error: &dyn fmt::Display| CsvError::Invalid(error.to_string());
    let definition = TableDefinition::new(definitions).map_err(|e| invalid(&e))?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays);
    let batch = batch.map_err(|e| invalid(&e))?;
    in_memory_table(definition, &[batch]).map_err(|failure| invalid(&failure.message))
}
