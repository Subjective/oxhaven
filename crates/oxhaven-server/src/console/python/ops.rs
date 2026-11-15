//! What the `deephaven` package needs from the engine beyond the operations of the table
//! service: local operations, the properties of a table, and its values.
//!
//! A **local operation** is a table method that the table service has no request for. Each
//! is one function in [`OPERATIONS`] that takes its arguments as a JSON object and the
//! tables it reads, and calls the engine's Rust API. [`local_op`] runs it as
//! `table::run_local` runs a handler: under the update graph's shared lock when a source is
//! refreshing, and with an `unported!` panic turned into `UNIMPLEMENTED`. An operation whose
//! engine operator does not exist yet is in the list all the same and refuses with the name
//! of the Java method, so that the Python method raises `DHError` and is never left out or
//! approximated.
//!
//! The rest reads a table: [`is_flat`], [`is_blink`], [`read_columns`] for the iterators
//! and [`to_string`].

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_column::sentinel::{
    NULL_BYTE, NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_INT, NULL_LONG, NULL_SHORT,
};
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use serde_json::Value;
use tonic::Status;

use super::{error_text, runtime, TableHandle};
use crate::java_text::object_texts;
use crate::session::ExportError;
use crate::table::support::{not_ported, OpContext};

type Created = Result<Arc<dyn Table>, ExportError>;

/// One local operation: its name in Python, and the function that runs it on its
/// arguments and source tables.
type Operation = (
    &'static str,
    fn(&OpContext, &Value, &[Arc<dyn Table>]) -> Created,
);

/// The local operations. Every one of them waits for its engine operator, and answers only
/// the calls that Java answers without one.
const OPERATIONS: &[Operation] = &[
    ("head_pct", head_pct),
    ("merge_sorted", merge_sorted),
    ("move_columns", move_columns),
    ("move_columns_down", move_columns_down),
    ("move_columns_up", move_columns_up),
    ("rename_columns", rename_columns),
    ("restrict_sort_to", restrict_sort_to),
    ("slice_pct", slice_pct),
    ("tail_pct", tail_pct),
    ("where_one_of", where_one_of),
];

fn bad_argument(name: &str, expected: &str) -> ExportError {
    Status::invalid_argument(format!("argument {name} must be {expected}")).into()
}

/// The argument `name`, a list of strings.
fn strings(args: &Value, name: &str) -> Result<Vec<String>, ExportError> {
    let items = args.get(name).and_then(Value::as_array);
    items
        .and_then(|items| {
            let texts = items.iter().map(|item| item.as_str().map(str::to_string));
            texts.collect::<Option<Vec<_>>>()
        })
        .ok_or_else(|| bad_argument(name, "a list of strings"))
}

/// The argument `name`, a number.
fn number(args: &Value, name: &str) -> Result<f64, ExportError> {
    let value = args.get(name).and_then(Value::as_f64);
    value.ok_or_else(|| bad_argument(name, "a number"))
}

/// The argument `name`, an integer.
fn integer(args: &Value, name: &str) -> Result<i64, ExportError> {
    let value = args.get(name).and_then(Value::as_i64);
    value.ok_or_else(|| bad_argument(name, "an integer"))
}

/// The refusal of an operation whose engine operator is not written: `UNIMPLEMENTED`
/// naming the Java method and the call.
fn no_operator(java: &str, call: String) -> Created {
    Err(not_ported(java, call).into())
}

const QUERY_TABLE: &str = "io.deephaven.engine.table.impl.QueryTable";

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#headPct`
fn head_pct(_: &OpContext, args: &Value, _: &[Arc<dyn Table>]) -> Created {
    let percent = number(args, "pct")?;
    no_operator(
        &format!("{QUERY_TABLE}#headPct"),
        format!("headPct({percent})"),
    )
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#tailPct`
fn tail_pct(_: &OpContext, args: &Value, _: &[Arc<dyn Table>]) -> Created {
    let percent = number(args, "pct")?;
    no_operator(
        &format!("{QUERY_TABLE}#tailPct"),
        format!("tailPct({percent})"),
    )
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#slicePct`
fn slice_pct(_: &OpContext, args: &Value, _: &[Arc<dyn Table>]) -> Created {
    let (start, end) = (number(args, "start_pct")?, number(args, "end_pct")?);
    no_operator(
        &format!("{QUERY_TABLE}#slicePct"),
        format!("slicePct({start}, {end})"),
    )
}

/// The table itself when nothing is renamed or moved: `renameColumnsImpl`, which is also
/// `moveColumns`, returns its table for no pairs.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#renameColumnsImpl`
fn unchanged(columns: &[String], sources: &[Arc<dyn Table>]) -> Option<Arc<dyn Table>> {
    columns.is_empty().then(|| sources.first().cloned())?
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#renameColumns`
fn rename_columns(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let pairs = strings(args, "cols")?;
    if let Some(table) = unchanged(&pairs, sources) {
        return Ok(table);
    }
    no_operator(
        &format!("{QUERY_TABLE}#renameColumns"),
        format!("renameColumns({})", pairs.join(", ")),
    )
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#moveColumns`
fn move_columns(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let (index, columns) = (integer(args, "idx")?, strings(args, "cols")?);
    if let Some(table) = unchanged(&columns, sources) {
        return Ok(table);
    }
    no_operator(
        &format!("{QUERY_TABLE}#moveColumns"),
        format!("moveColumns({index}, {})", columns.join(", ")),
    )
}

/// Behavior: `io.deephaven.engine.table.Table#moveColumnsUp`
fn move_columns_up(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let columns = strings(args, "cols")?;
    if let Some(table) = unchanged(&columns, sources) {
        return Ok(table);
    }
    no_operator(
        "io.deephaven.engine.table.Table#moveColumnsUp",
        format!("moveColumnsUp({})", columns.join(", ")),
    )
}

/// Behavior: `io.deephaven.engine.table.Table#moveColumnsDown`
fn move_columns_down(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let columns = strings(args, "cols")?;
    if let Some(table) = unchanged(&columns, sources) {
        return Ok(table);
    }
    no_operator(
        "io.deephaven.engine.table.Table#moveColumnsDown",
        format!("moveColumnsDown({})", columns.join(", ")),
    )
}

/// A `where` whose filter is a disjunction of filter strings. The engine selects rows by a
/// disjunction, but offers it only for the conditions of the gRPC `filter` operation.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#where(Filter)` of an
/// `io.deephaven.api.filter.FilterOr`, or of `Filter#ofFalse` for no filters
fn where_one_of(_: &OpContext, args: &Value, _: &[Arc<dyn Table>]) -> Created {
    let filters = strings(args, "filters")?;
    no_operator(
        &format!("{QUERY_TABLE}#where"),
        format!("where(Filter.or({}))", filters.join(", ")),
    )
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#restrictSortTo`
fn restrict_sort_to(_: &OpContext, args: &Value, _: &[Arc<dyn Table>]) -> Created {
    let columns = strings(args, "cols")?;
    no_operator(
        &format!("{QUERY_TABLE}#restrictSortTo"),
        format!("restrictSortTo({})", columns.join(", ")),
    )
}

/// The merge of tables that are each sorted by a column, asked for tables of which one is
/// refreshing, which Java refuses. The merge of static tables is the table service's
/// `merge` with a key column.
///
/// Behavior: `io.deephaven.engine.table.impl.util.MergeSortedHelper#mergeSortedHelper`
fn merge_sorted(_: &OpContext, _: &Value, _: &[Arc<dyn Table>]) -> Created {
    Err(ExportError::Internal(
        "mergeSorted does not yet support refreshing tables!".to_string(),
    ))
}

/// The status of an `unported!` panic, which names what is missing.
///
/// Behavior: as `table::support::unported_status`
fn unported_status(panic: &(dyn std::any::Any + Send)) -> Option<Status> {
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())?;
    let missing = message.strip_prefix("unported: ")?;
    Some(Status::unimplemented(format!("not ported: {missing}")))
}

/// Runs `work` on `tables` as `table::run_local` runs a handler: under the update graph's
/// shared lock if one of the tables is refreshing, and with an `unported!` panic answered
/// as `UNIMPLEMENTED`.
fn guarded<T>(
    tables: &[Arc<dyn Table>],
    work: impl FnOnce() -> Result<T, ExportError>,
) -> Result<T, ExportError> {
    let graph = tables
        .iter()
        .filter(|table| table.is_refreshing())
        .find_map(|table| table.update_graph());
    let work = || {
        catch_unwind(AssertUnwindSafe(work)).unwrap_or_else(|panic| {
            match unported_status(panic.as_ref()) {
                Some(status) => Err(status.into()),
                None => resume_unwind(panic),
            }
        })
    };
    match graph {
        Some(graph) => locked(graph.as_ref(), work),
        None => work(),
    }
}

fn failure(error: ExportError) -> PyErr {
    PyRuntimeError::new_err(error_text(error))
}

/// Runs the local operation `name` with the arguments of the JSON object `json` on
/// `sources`.
#[pyfunction]
fn local_op(
    py: Python<'_>,
    name: &str,
    json: &str,
    sources: Vec<PyRef<'_, TableHandle>>,
) -> PyResult<TableHandle> {
    let Some((_, create)) = OPERATIONS.iter().find(|(known, _)| *known == name) else {
        return Err(PyValueError::new_err(format!(
            "not a local operation: {name}"
        )));
    };
    let args: Value = serde_json::from_str(json)
        .map_err(|e| PyValueError::new_err(format!("not the arguments of {name}: {e}")))?;
    let tables: Vec<Arc<dyn Table>> = sources.iter().map(|handle| handle.table.clone()).collect();
    let runtime = runtime()?;
    py.detach(|| guarded(&tables, || create(&runtime.context, &args, &tables)))
        .map(|table| TableHandle { table })
        .map_err(failure)
}

/// Behavior: `io.deephaven.engine.table.Table#isFlat`
#[pyfunction]
fn is_flat(source: PyRef<'_, TableHandle>) -> bool {
    source.table.is_flat()
}

/// Behavior: `io.deephaven.engine.table.impl.BlinkTableTools#isBlink`
#[pyfunction]
fn is_blink(source: PyRef<'_, TableHandle>) -> bool {
    source.table.is_refreshing() && source.table.attributes().blink
}

/// The attributes of a table by name. A table here keeps the facts its operators need and
/// not Java's attribute map, which also holds what the operations before left for the ones
/// after (`SortedColumns`, `SortRowRedirection`, ...), so the map cannot be given.
///
/// Behavior: `io.deephaven.engine.table.AttributeMap#getAttributes`
#[pyfunction]
fn attributes(_source: PyRef<'_, TableHandle>) -> PyResult<()> {
    Err(failure(
        not_ported(
            "io.deephaven.engine.table.AttributeMap#getAttributes",
            "the attribute map of a table",
        )
        .into(),
    ))
}

/// The indices of the columns `names` in `table`, or of every column for no names.
fn column_indices(table: &dyn Table, names: &[String]) -> Result<Vec<usize>, ExportError> {
    let definition = table.definition();
    if names.is_empty() {
        return Ok((0..definition.columns().len()).collect());
    }
    let missing: Vec<&str> = names
        .iter()
        .filter(|name| definition.index_of(name).is_none())
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        // Behavior: `io.deephaven.engine.table.impl.NoSuchColumnException`
        let available: Vec<&str> = definition.columns().iter().map(|c| &*c.name).collect();
        return Err(ExportError::Internal(format!(
            "Unknown column names [{}], available column names are [{}]",
            missing.join(", "),
            available.join(", ")
        )));
    }
    Ok(names
        .iter()
        .filter_map(|name| definition.index_of(name))
        .collect())
}

/// The cells of one column as Python values, as the reference package's readers hand
/// them out: a primitive is its Java value, so a null is the null constant of its type and
/// a `char` is its code unit; a `Boolean` or `String` is the value or `None`. An `Instant`
/// is its nanoseconds since the epoch, for a numpy `datetime64[ns]`, when `instants` asks
/// for that; as a single value Java gives a `java.time.Instant` object, which has no
/// counterpart yet.
///
/// Behavior: `io.deephaven.integrations.python.PythonTableDataReader#readChunkColumnMajor`
/// as `deephaven._table_reader` reads its arrays
fn cells<'py>(
    py: Python<'py>,
    column_type: &ColumnType,
    values: &ArrayRef,
    instants: bool,
) -> PyResult<Bound<'py, PyList>> {
    macro_rules! primitive {
        ($arrow:ty, $null:expr) => {
            PyList::new(
                py,
                values
                    .as_primitive::<$arrow>()
                    .iter()
                    .map(|value| value.unwrap_or($null)),
            )
        };
    }
    match column_type {
        ColumnType::Byte => primitive!(Int8Type, NULL_BYTE),
        ColumnType::Short => primitive!(Int16Type, NULL_SHORT),
        ColumnType::Int => primitive!(Int32Type, NULL_INT),
        ColumnType::Long => primitive!(Int64Type, NULL_LONG),
        ColumnType::Float => primitive!(Float32Type, NULL_FLOAT),
        ColumnType::Double => primitive!(Float64Type, NULL_DOUBLE),
        ColumnType::Char => primitive!(UInt16Type, NULL_CHAR),
        ColumnType::Boolean => PyList::new(py, values.as_boolean().iter()),
        ColumnType::String => PyList::new(py, values.as_string::<i32>().iter()),
        ColumnType::Instant if instants => primitive!(TimestampNanosecondType, NULL_LONG),
        other => Err(failure(
            not_ported(
                "io.deephaven.integrations.python.PythonTableDataReader#readChunkColumnMajor",
                format!("the Python values of a column of type {other:?}"),
            )
            .into(),
        )),
    }
}

/// The values of the rows at positions `first` to `first + count` of `columns` (every
/// column for none), one list per column in the table's column order, read under the update
/// graph's shared lock if the table is refreshing. See [`cells`] for the values.
#[pyfunction]
fn read_columns<'py>(
    py: Python<'py>,
    source: PyRef<'py, TableHandle>,
    columns: Vec<String>,
    first: u64,
    count: u64,
    instants: bool,
) -> PyResult<Vec<(String, Bound<'py, PyList>)>> {
    let table = source.table.clone();
    let read = py.detach(|| {
        guarded(std::slice::from_ref(&table), || {
            let mut indices = column_indices(table.as_ref(), &columns)?;
            indices.sort_unstable();
            let rows = table
                .row_set()
                .sub_set_by_position_range(first, first.saturating_add(count));
            let chunks = indices
                .into_iter()
                .map(|index| Ok((index, table.column(index).chunk(&rows)?)))
                .collect::<Result<Vec<_>, ExportError>>()?;
            Ok(chunks)
        })
    });
    let definition = source.table.definition().columns();
    read.map_err(failure)?
        .into_iter()
        .map(|(index, values)| {
            let column = &definition[index];
            let values = cells(py, &column.column_type, &values, instants)?;
            Ok((column.name.clone(), values))
        })
        .collect()
}

/// How many rows `TableShowTools` reads to size a string column.
const SIZED_BY_ROWS: u64 = 100;

/// The width of a column in the text of a table.
///
/// Behavior: `io.deephaven.engine.util.TableShowTools#getColumnLen`
fn column_width(name: &str, column_type: &ColumnType, sample: &[Option<String>]) -> usize {
    let least = match column_type {
        ColumnType::Long | ColumnType::Double => 20,
        _ => 10,
    };
    let mut width = name.encode_utf16().count().max(least);
    if *column_type == ColumnType::String {
        for text in sample.iter().flatten() {
            width = text.encode_utf16().count().max(width).min(100);
        }
    }
    width
}

/// One cell, padded or cut to `width`: a number to the right, anything else and a null to
/// the left, text that is too long cut with `...`, and a double in exponent notation cut in
/// its digits. Java counts UTF-16 code units and may cut a surrogate pair; this cuts at
/// characters.
///
/// Behavior: `io.deephaven.engine.util.TableShowTools.DefaultPrinter#print`
fn cell_text(column_type: &ColumnType, value: Option<&str>, width: usize) -> String {
    let is_number = value.is_some()
        && matches!(
            column_type,
            ColumnType::Byte
                | ColumnType::Short
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
        );
    let text = value.unwrap_or("(null)");
    let length = text.chars().count();
    if length <= width {
        return if is_number {
            format!("{text:>width$}")
        } else {
            format!("{text:<width$}")
        };
    }
    match text.split_once('E') {
        Some((digits, exponent)) if value.is_some() && *column_type == ColumnType::Double => {
            let kept = digits.len().saturating_sub(length - width);
            format!("{}E{exponent}", &digits[..kept])
        }
        _ if is_number => text.to_string(),
        _ => {
            let kept: String = text.chars().take(width.saturating_sub(3)).collect();
            format!("{kept}...")
        }
    }
}

/// The first `num_rows` rows of `columns` (every column for none) as text: a line of
/// column names, a line of dashes, and a line per row, with `|` between the columns.
///
/// Behavior: `io.deephaven.engine.util.TableTools#string(Table, int, String...)`,
/// `io.deephaven.engine.util.TableShowTools#showInternal`
#[pyfunction]
fn to_string(
    py: Python<'_>,
    source: PyRef<'_, TableHandle>,
    num_rows: i64,
    columns: Vec<String>,
) -> PyResult<String> {
    let table = source.table.clone();
    py.detach(|| {
        guarded(std::slice::from_ref(&table), || {
            let indices = column_indices(table.as_ref(), &columns)?;
            let all_rows = table.row_set();
            let shown = u64::try_from(num_rows).unwrap_or(0);
            let read = all_rows.sub_set_by_position_range(0, shown.max(SIZED_BY_ROWS));
            let (mut header, mut rule) = (String::new(), String::new());
            let mut printed = Vec::with_capacity(indices.len());
            for (position, index) in indices.into_iter().enumerate() {
                let column = &table.definition().columns()[index];
                let column_type = &column.column_type;
                match column_type {
                    ColumnType::Instant => {
                        return Err(not_ported(
                            "io.deephaven.time.DateTimeUtils#formatDateTime",
                            "the text of an Instant in the server's time zone",
                        )
                        .into())
                    }
                    ColumnType::Array(_) => {
                        return Err(not_ported(
                            "io.deephaven.engine.util.TableShowTools.ArrayPrinter",
                            format!("the text of a column of type {column_type:?}"),
                        )
                        .into())
                    }
                    _ => {}
                }
                let texts = object_texts(column_type, &table.column(index).chunk(&read)?);
                let sample = &texts[..texts.len().min(SIZED_BY_ROWS as usize)];
                let width = column_width(&column.name, column_type, sample);
                if position > 0 {
                    header.push('|');
                    rule.push('+');
                }
                header.push_str(&format!("{:>width$}", column.name));
                rule.push_str(&"-".repeat(width));
                printed.push((column_type.clone(), width, texts));
            }
            let mut text = format!("{header}\n{rule}");
            for row in 0..(read.size().min(shown) as usize) {
                text.push('\n');
                for (position, (column_type, width, texts)) in printed.iter().enumerate() {
                    if position > 0 {
                        text.push('|');
                    }
                    text.push_str(&cell_text(column_type, texts[row].as_deref(), *width));
                }
            }
            text.push('\n');
            Ok(text)
        })
    })
    .map_err(failure)
}

/// Adds the functions of this module to the native module `_oxhaven`.
pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_function(wrap_pyfunction!(local_op, native)?)?;
    native.add_function(wrap_pyfunction!(is_flat, native)?)?;
    native.add_function(wrap_pyfunction!(is_blink, native)?)?;
    native.add_function(wrap_pyfunction!(attributes, native)?)?;
    native.add_function(wrap_pyfunction!(read_columns, native)?)?;
    native.add_function(wrap_pyfunction!(to_string, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! The expectations are what the Java reference answered to the same code, through
    //! the script scenarios `table_*` and by hand; a comment says where Java differs.

    use super::super::tests::Python;
    use super::*;
    use arrow::array::TimestampNanosecondArray;
    use oxhaven_contracts::graph::UpdateGraph;
    use oxhaven_testkit::TableSnapshot;

    /// Runs Python whose `assert` statements are the test.
    async fn check(code: &str) {
        Python::start().await.run(code);
    }

    #[test]
    fn a_cell_is_padded_on_the_side_of_its_kind_and_cut_when_too_long() {
        assert_eq!(cell_text(&ColumnType::Int, Some("7"), 4), "   7");
        assert_eq!(cell_text(&ColumnType::String, Some("7"), 4), "7   ");
        assert_eq!(cell_text(&ColumnType::Int, None, 8), "(null)  ");
        assert_eq!(
            cell_text(&ColumnType::String, Some("abcdefgh"), 6),
            "abc..."
        );
        // a number is never cut, except the digits of a double in exponent notation
        assert_eq!(cell_text(&ColumnType::Long, Some("1234567"), 4), "1234567");
        assert_eq!(
            cell_text(&ColumnType::Double, Some("1.2345678E-10"), 10),
            "1.2345E-10"
        );
        assert_eq!(
            cell_text(&ColumnType::Float, Some("1.2345678E-10"), 10),
            "1.2345678E-10"
        );
    }

    #[test]
    fn a_column_is_as_wide_as_its_name_its_type_and_its_first_strings() {
        let none: &[Option<String>] = &[];
        assert_eq!(column_width("I", &ColumnType::Int, none), 10);
        assert_eq!(column_width("L", &ColumnType::Long, none), 20);
        assert_eq!(column_width("D", &ColumnType::Double, none), 20);
        assert_eq!(column_width("ALongColumnName", &ColumnType::Int, none), 15);
        let texts = [Some("x".repeat(12)), None, Some("y".repeat(120))];
        assert_eq!(column_width("S", &ColumnType::String, &texts[..2]), 12);
        assert_eq!(column_width("S", &ColumnType::String, &texts), 100);
        // only a string column looks at its values
        assert_eq!(column_width("C", &ColumnType::Char, &texts), 10);
    }

    #[tokio::test]
    async fn an_operation_without_an_engine_operator_names_the_java_method() {
        check(
            r#"
from deephaven import DHError, empty_table, merge_sorted
t = empty_table(4).update(["A = i", "B = i % 2"])
refused = {
    "QueryTable#headPct": lambda: t.head_pct(0.5),
    "QueryTable#tailPct": lambda: t.tail_pct(0.5),
    "QueryTable#slicePct": lambda: t.slice_pct(0.25, 0.75),
    "QueryTable#renameColumns": lambda: t.rename_columns("C = A"),
    "QueryTable#moveColumns": lambda: t.move_columns(1, "A"),
    "Table#moveColumnsUp": lambda: t.move_columns_up("B"),
    "Table#moveColumnsDown": lambda: t.move_columns_down("A"),
    "QueryTable#where": lambda: t.where_one_of(["A > 2", "B == 0"]),
    "QueryTable#restrictSortTo": lambda: t.restrict_sort_to("A"),
    "TableTools#mergeSorted": lambda: merge_sorted([t, t], "A"),
    "AttributeMap#getAttributes": lambda: t.attributes(),
}
for java, call in refused.items():
    try:
        call()
    except DHError as e:
        assert "Unimplemented: not ported: io.deephaven." in str(e), str(e)
        assert java in str(e), str(e)
    else:
        raise AssertionError(java + " was answered")
try:
    t.head_pct(0.5)
except DHError as e:
    assert str(e).startswith("table head_pct operation failed. : "), str(e)
"#,
        )
        .await;
    }

    #[tokio::test]
    async fn what_java_answers_without_an_operator_is_answered() {
        check(
            r#"
from deephaven import DHError, empty_table, merge_sorted, time_table
t = empty_table(4).update(["A = i", "B = i % 2"])
# renaming or moving no column is the table itself
for same in (t.rename_columns([]), t.move_columns(1, []), t.move_columns_up([]), t.move_columns_down([])):
    assert same.j_table.is_same(t.j_table)
assert t.coalesce().j_table.is_same(t.j_table)
# one alternative is that filter
assert [row["A"] for row in t.where_one_of("A > 1").iter_dict()] == [2, 3]
# Java refuses a sorted merge of refreshing tables
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC")
try:
    merge_sorted([clock, clock], "Timestamp")
except DHError as e:
    assert str(e) == "merge sorted operation failed. : mergeSorted does not yet support refreshing tables!", str(e)
else:
    raise AssertionError("merged")
"#,
        )
        .await;
    }

    #[tokio::test]
    async fn a_table_says_what_java_says_about_itself() {
        check(
            r#"
from deephaven import empty_table, time_table
t = empty_table(3).update([
    "B = (byte)(i == 1 ? null : i)", "I = i == 1 ? null : i", "L = i == 1 ? null : (long)i",
    "F = (float)(i == 1 ? null : i * 0.5)", "D = i == 1 ? null : i * 1e10 / 3",
    "C = i == 1 ? null : (char)(65 + i)", "S = i == 1 ? null : `a long string value ` + i",
    "Z = i == 1 ? null : i == 0",
])
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC")
assert (t.size, t.is_refreshing, t.is_blink, t.is_flat) == (3, False, False, True)
assert (clock.is_refreshing, clock.is_blink, clock.is_flat) == (True, False, True)
assert time_table("PT1h", blink_table=True).is_blink
kept = t.where("I > 0")
assert (kept.is_flat, kept.flatten().is_flat) == (False, True)
assert t.column_names == ["B", "I", "L", "F", "D", "C", "S", "Z"]
assert [str(col.data_type) for col in t.columns] == [
    "byte", "int", "long", "float", "double", "char", "java.lang.String", "java.lang.Boolean"]
assert list(t.definition) == t.column_names and "S" in t.definition and len(t.definition) == 8
assert t.definition == kept.definition and t.definition != clock.definition
assert (t.has_columns("I"), t.has_columns(["I", "S"]), t.has_columns(["I", "X"])) == (True, True, False)
assert t.meta_table.size == 8
assert t.definition.table.column_names == ["Name", "DataType", "ColumnType", "IsPartitioning"]
assert repr(t).startswith("deephaven.table.Table(io.deephaven.engine.table.Table(objectRef=0x")
assert repr(t).endswith("...}))") and len(repr(t)) == 120 and str(t) == repr(t)
assert repr(empty_table(2)).endswith(", num_rows = 2, columns = {}))")
assert t.to_string().split("\n") == [
    "         B|         I|                   L|         F|                   D|         C|                    S|         Z",
    "----------+----------+--------------------+----------+--------------------+----------+---------------------+----------",
    "         0|         0|                   0|       0.0|                 0.0|A         |a long string value 0|true      ",
    "(null)    |(null)    |(null)              |(null)    |(null)              |(null)    |(null)               |(null)    ",
    "         2|         2|                   2|       1.0| 6.666666666666667E9|C         |a long string value 2|false     ",
    "",
], t.to_string()
assert t.to_string(2, ["S", "I"]).split("\n") == [
    "                    S|         I",
    "---------------------+----------",
    "a long string value 0|         0",
    "(null)               |(null)    ",
    "",
], t.to_string(2, ["S", "I"])
assert t.to_string(0, "Z") == "         Z\n----------\n"
"#,
        )
        .await;
    }

    #[tokio::test]
    async fn rows_are_python_values_with_the_null_constants_of_java() {
        check(
            r#"
from deephaven import DHError, empty_table, time_table
t = empty_table(3).update([
    "B = (byte)(i == 1 ? null : i)", "H = (short)(i == 1 ? null : i)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long)i", "F = (float)(i == 1 ? null : i * 0.5)",
    "D = i == 1 ? null : i * 0.5", "C = i == 1 ? null : (char)(65 + i)",
    "S = i == 1 ? null : `s` + i", "Z = i == 1 ? null : i == 0",
])
assert list(t.iter_dict()) == [
    {'B': 0, 'H': 0, 'I': 0, 'L': 0, 'F': 0.0, 'D': 0.0, 'C': 65, 'S': 's0', 'Z': True},
    {'B': -128, 'H': -32768, 'I': -2147483648, 'L': -9223372036854775808,
     'F': -3.4028234663852886e+38, 'D': -1.7976931348623157e+308, 'C': 65535, 'S': None, 'Z': None},
    {'B': 2, 'H': 2, 'I': 2, 'L': 2, 'F': 1.0, 'D': 1.0, 'C': 67, 'S': 's2', 'Z': False},
]
# the columns come in the table's order whatever the order asked for, a chunk at a time
assert list(t.iter_dict(["S", "I"], chunk_size=2)) == [
    {'I': 0, 'S': 's0'}, {'I': -2147483648, 'S': None}, {'I': 2, 'S': 's2'}]
rows = list(t.iter_tuple(["I", "S"], tuple_name="Row", chunk_size=1))
assert repr(rows[0]) == "Row(I=0, S='s0')" and rows[2].S == 's2' and len(rows) == 3
# and so a tuple's fields, named as asked, hold the columns in the table's order
assert repr(next(t.iter_tuple(["S", "I"]))) == "Deephaven(S=0, I='s0')"
assert list(t.head(0).iter_dict()) == [] and list(empty_table(3).iter_dict()) == []
for bad, text in (
    (lambda: list(t.iter_dict("Nope")), "Invalid column names: {'Nope'}"),
    (lambda: list(t.iter_dict(["I", "I"])), "Invalid column names: set()"),
    (lambda: list(t.iter_tuple(chunk_size=-1)), "chunk_size must not be negative."),
):
    try:
        bad()
    except ValueError as e:
        assert str(e) == text, str(e)
    else:
        raise AssertionError(text)
# Java gives a java.time.Instant object, which has no counterpart yet
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC").head(1)
try:
    list(clock.iter_dict())
except DHError as e:
    assert "PythonTableDataReader#readChunkColumnMajor" in str(e), str(e)
else:
    raise AssertionError("read")
try:
    import numpy as np
except ImportError:
    np = None
if np is not None:
    chunks = list(t.iter_chunk_dict(chunk_size=2))
    assert [len(chunk["I"]) for chunk in chunks] == [2, 1]
    first = chunks[0]
    assert [str(first[name].dtype) for name in first] == [
        "int8", "int16", "int32", "int64", "float32", "float64", "uint16", "<U4", "bool"]
    # a null string is the text None and a null Boolean is True, as in the reference
    assert first["S"].tolist() == ["s0", "None"] and first["Z"].tolist() == [True, True]
    assert first["I"].tolist() == [0, -2147483648] and first["C"].tolist() == [65, 65535]
    whole = next(t.iter_chunk_tuple(["Z", "I"], tuple_name="Chunk"))
    assert type(whole).__name__ == "Chunk" and whole.Z.tolist() == [0, -2147483648, 2]
    stamps = next(clock.iter_chunk_dict())["Timestamp"]
    assert str(stamps.dtype) == "datetime64[ns]"
    assert str(stamps[0]) == "2024-01-01T00:00:00.000000000"
"#,
        )
        .await;
    }

    fn first_instant(table: &dyn Table) -> Option<i64> {
        let snapshot = TableSnapshot::current(table).unwrap();
        let stamps = snapshot.columns[0]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("an Instant column");
        stamps.iter().next().flatten()
    }

    #[tokio::test]
    async fn a_time_table_takes_its_period_and_start_as_python_gives_them() {
        let python = Python::start().await;
        python.run(
            r#"
from datetime import datetime, timedelta, timezone
from deephaven import DHError, time_table
start = datetime(2024, 1, 1, 0, 0, 0, 123457, tzinfo=timezone.utc)
from_strings = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC")
from_ints = time_table(3_600_000_000_000, start_time=1_704_067_200_000_000_000)
from_datetime = time_table(timedelta(hours=1), start_time=start)
from_now = time_table("PT1h", start_time=0)
for bad in (None, True, 1.5, "nonsense", 0, -5):
    try:
        time_table(bad)
    except DHError as e:
        assert str(e).startswith("failed to create a time table. : "), str(e)
    else:
        raise AssertionError(repr(bad))
try:
    time_table("PT1s", start_time=1.5)
except DHError as e:
    assert "Unsupported conversion: <class 'float'> -> Instant" in str(e), str(e)
else:
    raise AssertionError("started")
"#,
        );
        python.server.state.graph.run_cycle();
        const START: i64 = 1_704_067_200_000_000_000;
        let first = |name: &str| first_instant(python.table(name).as_ref());
        assert_eq!(first("from_strings"), Some(START));
        assert_eq!(first("from_ints"), Some(START));
        // the reference's floating point arithmetic on the seconds, not 123_457_000
        assert_eq!(first("from_datetime"), Some(START + 123_456_954));
        // a start of 0 is no start, as in the reference: the table starts now
        let now = first("from_now");
        assert!(now.is_none_or(|nanos| nanos > START), "{now:?}");
    }
}
