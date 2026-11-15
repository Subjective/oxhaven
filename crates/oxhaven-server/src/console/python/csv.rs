//! `deephaven.csv` reaches the CSV reader and writer here.
//!
//! Java has no gRPC call that reads or writes a file, so these are not table service
//! operations: the Python module calls [`read_csv`] with the arguments of
//! `deephaven.csv.read` and [`write_csv`] with those of `deephaven.csv.write`, and the
//! work is `oxhaven_csv::read` and `oxhaven_csv::write` (`docs/CSV.md`).

use oxhaven_contracts::graph::locked;
use oxhaven_csv::{ColumnSpec, CsvError, Parser, ReadOptions};
use pyo3::exceptions::{PyNotImplementedError, PyOSError, PyValueError};
use pyo3::prelude::*;

use super::TableHandle;

/// The Python exception of a failed read or write; `deephaven.csv` wraps it in a
/// `DHError`.
fn exception(error: CsvError) -> PyErr {
    let text = error.to_string();
    match error {
        CsvError::NotPorted { .. } => PyNotImplementedError::new_err(text),
        CsvError::Invalid(_) => PyValueError::new_err(text),
        CsvError::Io(_) => PyOSError::new_err(text),
    }
}

/// The parser `deephaven.csv.read` has for a type of its `header`, by the name of the
/// constant in `io.deephaven.csv.parsers.Parsers`.
fn parser(name: &str) -> PyResult<Parser> {
    Ok(match name {
        "BOOLEAN" => Parser::Boolean,
        "BYTE" => Parser::Byte,
        "SHORT" => Parser::Short,
        "INT" => Parser::Int,
        "LONG" => Parser::Long,
        "FLOAT_FAST" => Parser::Float,
        "DOUBLE" => Parser::Double,
        "DATETIME" => Parser::DateTime,
        "CHAR" => Parser::Char,
        "STRING" => Parser::String,
        other => return Err(PyValueError::new_err(format!("no parser {other}"))),
    })
}

/// A delimiter or a quote, which Python passes as the number of its character.
fn character(what: &str, code: u32) -> PyResult<char> {
    let none = || PyValueError::new_err(format!("{what} is set to no character: {code}"));
    char::from_u32(code).ok_or_else(none)
}

/// The table of the CSV file at `path`. `columns` is `(name, parser)` for each column the
/// caller names.
///
/// Behavior: `deephaven.csv.read`, which builds a `CsvSpecs` from its arguments and calls
/// `io.deephaven.csv.CsvTools#readCsv(String, CsvSpecs)`
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(super) fn read_csv(
    py: Python<'_>,
    path: &str,
    columns: Vec<(String, String)>,
    has_header_row: bool,
    skip_header_rows: i64,
    skip_rows: i64,
    num_rows: i64,
    ignore_empty_lines: bool,
    allow_missing_columns: bool,
    ignore_excess_columns: bool,
    delimiter: u32,
    quote: u32,
    ignore_surrounding_spaces: bool,
    trim: bool,
) -> PyResult<TableHandle> {
    let mut options = ReadOptions {
        columns: Vec::with_capacity(columns.len()),
        has_header_row,
        skip_header_rows,
        skip_rows,
        num_rows,
        ignore_empty_lines,
        allow_missing_columns,
        ignore_excess_columns,
        delimiter: character("delimiter", delimiter)?,
        quote: character("quote", quote)?,
        ignore_surrounding_spaces,
        trim,
    };
    for (name, parser_name) in columns {
        let parser = parser(&parser_name)?;
        options.columns.push(ColumnSpec { name, parser });
    }
    let table = py
        .detach(|| oxhaven_csv::read(path, &options))
        .map_err(exception)?;
    Ok(TableHandle { table })
}

/// Writes `columns` of `source` (every column for none) to the CSV file at `path`, read
/// under the update graph's shared lock if the table is refreshing. `zone` is the id of
/// the server's time zone, in which Java writes an instant.
///
/// Behavior: `deephaven.csv.write`, which calls
/// `io.deephaven.csv.CsvTools#writeCsv(Table, boolean, String, String...)`
#[pyfunction]
pub(super) fn write_csv(
    py: Python<'_>,
    source: PyRef<'_, TableHandle>,
    path: &str,
    columns: Vec<String>,
    zone: &str,
) -> PyResult<()> {
    let table = source.table.clone();
    let graph = if table.is_refreshing() {
        table.update_graph()
    } else {
        None
    };
    py.detach(|| {
        let write = || oxhaven_csv::write(table.as_ref(), path, &columns, zone);
        match &graph {
            Some(graph) => locked(graph.as_ref(), write),
            None => write(),
        }
    })
    .map_err(exception)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int32Array};
    use oxhaven_contracts::schema::ColumnType;
    use oxhaven_contracts::table::Table;

    use super::super::tests::ONE_INTERPRETER;
    use crate::test_support::TestServer;

    fn ints(table: &dyn Table, name: &str) -> Vec<Option<i32>> {
        let index = table.definition().index_of(name).unwrap();
        let values = table.column(index).chunk(&table.row_set()).unwrap();
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        values.iter().collect()
    }

    #[tokio::test]
    async fn deephaven_csv_reads_and_writes_tables_and_raises_dherror() {
        let _turn = ONE_INTERPRETER.lock().await;
        let server = TestServer::start_with(|state| super::super::start(state).unwrap()).await;
        let console = server.state.console.clone().unwrap();
        let table = |name: &str| {
            let value = server.state.scope.get(name);
            value.unwrap().as_table().unwrap().clone()
        };
        let directory = std::env::temp_dir().join(format!("oxhaven-csv-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let directory_name = directory.to_str().unwrap();

        let changes = console.evaluate(&format!(
            r#"
import os
from deephaven import dtypes, read_csv, write_csv
from deephaven.csv import read, write

def path(name):
    return os.path.join("{directory_name}", name)

def text(name):
    with open(path(name), newline="") as f:
        return f.read()

with open(path("in.csv"), "w") as f:
    f.write("N,X,When\n1,a,2024-01-02T03:04:05Z\n2,\"b,c\",\n")
inferred = read(path("in.csv"))
doubled = inferred.update("M = N * 2")
named = read_csv(path("in.csv"), header={{"I": dtypes.long, "S": dtypes.string, "T": dtypes.string}})
headless = read(path("in.csv"), headless=True, skip_rows=1, num_rows=1)
with open(path("pipes.csv"), "w") as f:
    f.write("N|X\n1|'a|b'\n")
pipes = read(path("pipes.csv"), delimiter="|", quote="'")

write(inferred, path("all.csv"))
assert text("all.csv") == 'N,X,When\n1,a,2024-01-02T03:04:05Z\n2,"b,c",\n', text("all.csv")
write_csv(doubled, path("some.csv"), cols=["M", "X"])
assert text("some.csv") == 'M,X\n2,a\n4,"b,c"\n', text("some.csv")
back = read(path("all.csv"))
"#
        ));
        assert_eq!(changes.error, None);
        let types = |name: &str| -> Vec<ColumnType> {
            let columns = table(name).definition().columns().to_vec();
            columns.into_iter().map(|c| c.column_type).collect()
        };
        let inferred = [ColumnType::Int, ColumnType::String, ColumnType::Instant];
        assert_eq!(types("inferred"), inferred);
        assert_eq!(ints(&*table("doubled"), "M"), [2, 4].map(Some));
        let named = [ColumnType::Long, ColumnType::String, ColumnType::String];
        assert_eq!(types("named"), named);
        assert_eq!(table("named").definition().index_of("T"), Some(2));
        assert_eq!(table("headless").definition().index_of("Column3"), Some(2));
        assert_eq!(table("headless").row_set().size(), 1);
        assert_eq!(types("pipes"), [ColumnType::Int, ColumnType::String]);
        assert_eq!(types("back"), inferred);
        assert_eq!(ints(&*table("back"), "N"), [1, 2].map(Some));

        // every failure is a DHError with the reference's message, then the cause
        let failure = |code: &str, message: &str| {
            let code = format!(
                "import os\nfrom deephaven import dtypes\nfrom deephaven.csv import *\n\
                 def path(name):\n    return os.path.join({directory_name:?}, name)\n{code}"
            );
            let error = console.evaluate(&code).error.expect("the call fails");
            let last = error.trim_end().lines().last().unwrap().to_string();
            let prefix = format!("deephaven.dherror.DHError: {message} : ");
            let text = last.strip_prefix(&prefix);
            text.unwrap_or_else(|| panic!("not a DHError of {message}: {last}"))
                .to_string()
        };
        let missing = failure(r#"read(path("nope.csv"))"#, "read csv failed");
        assert!(missing.contains("nope.csv"), "{missing}");
        assert_eq!(
            failure(
                r#"read(path("in.csv"), header={"A": dtypes.int32})"#,
                "read csv failed"
            ),
            "Row 2 has too many columns (expected 1)"
        );
        assert_eq!(
            failure(r#"read(path("in.csv"), delimiter="€")"#, "read csv failed"),
            "CsvSpecs failed validation for the following reasons: delimiter is set to '\u{20ac}' \
             but is required to be 7-bit ASCII"
        );
        let zipped = failure(r#"read(path("in.csv.gz"))"#, "read csv failed");
        assert_eq!(
            zipped,
            "not ported yet: io.deephaven.engine.util.PathUtil#open: unpacking a .gz file"
        );
        let no_parser = failure(
            r#"read(path("in.csv"), header={"A": dtypes.int32_array})"#,
            "read csv failed",
        );
        assert!(
            no_parser.contains("int32_array") || no_parser.contains("[I"),
            "{no_parser}"
        );
        let unknown = failure(
            r#"write(read(path("in.csv")), path("out.csv"), cols=["Nope"])"#,
            "write csv failed.",
        );
        assert_eq!(
            unknown,
            "Unknown column names [Nope], available column names are [N, X, When]"
        );
        std::fs::remove_dir_all(&directory).unwrap();
    }
}
