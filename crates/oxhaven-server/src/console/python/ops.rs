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
use oxhaven_engine::ops::select::ColumnPair;
use oxhaven_engine::ops::{distinct, filter, merge, select, sort};
use oxhaven_engine::table::{attribute_map, AttributeValue};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
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

/// The local operations.
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
    ("where_in", where_in),
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

/// The argument `name`, a string.
fn text<'a>(args: &'a Value, name: &str) -> Result<&'a str, ExportError> {
    let value = args.get(name).and_then(Value::as_str);
    value.ok_or_else(|| bad_argument(name, "a string"))
}

/// The argument `name`, a number. JSON has no NaN and no infinity, which Python sends as
/// the strings `"nan"`, `"inf"` and `"-inf"`.
fn number(args: &Value, name: &str) -> Result<f64, ExportError> {
    let value = match args.get(name) {
        Some(Value::String(text)) => match text.as_str() {
            "nan" => Some(f64::NAN),
            "inf" => Some(f64::INFINITY),
            "-inf" => Some(f64::NEG_INFINITY),
            _ => None,
        },
        Some(value) => value.as_f64(),
        None => None,
    };
    value.ok_or_else(|| bad_argument(name, "a number"))
}

/// The argument `name`, a Java `int`.
fn integer(args: &Value, name: &str) -> Result<i32, ExportError> {
    let value = args.get(name).and_then(Value::as_i64);
    value
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| bad_argument(name, "an integer"))
}

/// The argument `name`, a list of `Output = Input` pairs and column names.
///
/// Behavior: `io.deephaven.api.Pair#from`
fn pairs(args: &Value, name: &str) -> Result<Vec<ColumnPair>, ExportError> {
    let texts = strings(args, name)?;
    let pairs = texts.iter().map(|text| ColumnPair::parse(text));
    Ok(pairs.collect::<Result<Vec<_>, _>>()?)
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#headPct`
fn head_pct(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    Ok(sort::head_pct(&sources[0], number(args, "pct")?)?)
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#tailPct`
fn tail_pct(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    Ok(sort::tail_pct(&sources[0], number(args, "pct")?)?)
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#slicePct`
fn slice_pct(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let (start, end) = (number(args, "start_pct")?, number(args, "end_pct")?);
    Ok(sort::slice_pct(&sources[0], start, end)?)
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#renameColumns`
fn rename_columns(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    Ok(select::rename_columns(&sources[0], &pairs(args, "cols")?)?)
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#moveColumns`
fn move_columns(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let (index, columns) = (integer(args, "idx")?, pairs(args, "cols")?);
    Ok(select::move_columns(&sources[0], index, &columns)?)
}

/// Behavior: `io.deephaven.engine.table.Table#moveColumnsUp`
fn move_columns_up(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    Ok(select::move_columns_up(&sources[0], &pairs(args, "cols")?)?)
}

/// Behavior: `io.deephaven.engine.table.Table#moveColumnsDown`
fn move_columns_down(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    Ok(select::move_columns_down(
        &sources[0],
        &pairs(args, "cols")?,
    )?)
}

/// A `where` whose filter is a disjunction of filter strings.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#where(Filter)` of an
/// `io.deephaven.api.filter.FilterOr`, or of `Filter#ofFalse` for no filters
fn where_one_of(context: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let filters = strings(args, "filters")?;
    Ok(filter::where_one_of(
        &sources[0],
        &filters,
        context.compiler.as_ref(),
    )?)
}

/// `where_in` and `where_not_in` with no match columns, which the table service's
/// `where_in` refuses and Java's engine answers: every row of the first table, or none,
/// by whether the second table has a row.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#whereIn` and `#whereNotIn`
fn where_in(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    let inverted = args.get("inverted").and_then(Value::as_bool);
    let inverted = inverted.ok_or_else(|| bad_argument("inverted", "a boolean"))?;
    let [left, right] = sources else {
        return Err(bad_argument("sources", "two tables"));
    };
    Ok(if inverted {
        distinct::where_not_in(left, right, &[])?
    } else {
        distinct::where_in(left, right, &[])?
    })
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#restrictSortTo`
fn restrict_sort_to(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    match sort::restrict_sort_to(&sources[0], &strings(args, "cols")?) {
        // Behavior: `io.deephaven.engine.table.impl.NoSuchColumnException`
        Err(sort::SortError::NoSuchColumn(missing)) => {
            let columns = sources[0].definition().columns().iter();
            let available: Vec<&str> = columns.map(|column| &*column.name).collect();
            Err(ExportError::Internal(format!(
                "Unknown column names [{}], available column names are [{}]",
                missing.join(", "),
                available.join(", ")
            )))
        }
        result => Ok(result?),
    }
}

/// The merge of tables that are each sorted by a column. Java's method takes static tables
/// only, and unlike the table service's `merge` with a key column it takes no tables.
///
/// Behavior: `io.deephaven.engine.util.TableTools#mergeSorted`
fn merge_sorted(_: &OpContext, args: &Value, sources: &[Arc<dyn Table>]) -> Created {
    Ok(merge::merge_sorted(sources, text(args, "order_by")?)?)
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

/// The value of an attribute that is a Java object in the reference and has no value
/// here: what it is, for a reader to tell it is there.
#[pyclass(module = "_oxhaven", frozen)]
struct JavaObject {
    class: &'static str,
}

#[pymethods]
impl JavaObject {
    fn __repr__(&self) -> String {
        format!("<{}>", self.class)
    }
}

/// The attributes of a table by name: a Boolean or a string as Java has it, and a
/// [`JavaObject`] for an attribute whose value is an object of the Java engine
/// (`SortReverseLookup`, `InputTable`). See `oxhaven_engine::table::attribute_map` for
/// the attributes a table has.
///
/// Behavior: `io.deephaven.engine.table.AttributeMap#getAttributes`
#[pyfunction]
fn attributes<'py>(
    py: Python<'py>,
    source: PyRef<'py, TableHandle>,
) -> PyResult<Bound<'py, PyDict>> {
    let map = PyDict::new(py);
    for (name, value) in attribute_map(source.table.as_ref()) {
        match value {
            AttributeValue::Bool(value) => map.set_item(name, value)?,
            AttributeValue::Text(value) => map.set_item(name, value)?,
            AttributeValue::Object(class) => {
                map.set_item(name, Py::new(py, JavaObject { class })?)?
            }
        }
    }
    Ok(map)
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
    async fn the_local_operations_answer_as_java_answers() {
        check(
            r#"
from deephaven import DHError, empty_table, merge, merge_sorted, time_table
def rows(t):
    return [tuple(row.values()) for row in t.iter_dict()]
t = empty_table(8).update(["I = i"])
assert rows(t.head_pct(0.5)) == [(0,), (1,), (2,), (3,)]
assert rows(t.head_pct(0.126)) == [(0,), (1,)]
assert rows(t.tail_pct(0.3)) == [(5,), (6,), (7,)]
assert rows(t.slice_pct(0.25, 0.8)) == [(2,), (3,), (4,), (5,)]
assert rows(t.slice_pct(0.8, 0.2)) == []
# NaN is not outside 0 to 1
assert rows(t.head_pct(float("nan"))) == [] and rows(t.tail_pct(float("nan"))) == []
assert rows(t.slice_pct(float("nan"), 0.5)) == [(0,), (1,), (2,), (3,)]
assert (t.head_pct(0.5).is_flat, t.tail_pct(0.3).is_flat) == (True, False)

a = empty_table(3).update(["A = i", "B = i * 2", "C = i * 3", "D = i * 4"])
swapped = a.rename_columns(["A = B", "B = A"])
assert swapped.column_names == ["B", "A", "C", "D"] and rows(swapped)[1] == (1, 2, 3, 4)
assert rows(a.rename_columns("B = A")) == [(0, 0, 0), (1, 3, 4), (2, 6, 8)]
assert a.move_columns(2, ["D", "B"]).column_names == ["A", "C", "D", "B"]
assert a.move_columns(1, "A = B").column_names == ["C", "A", "D"]
assert a.move_columns(-3, "C").column_names == ["C", "A", "B", "D"]
assert a.move_columns_up(["C", "B"]).column_names == ["C", "B", "A", "D"]
assert a.move_columns_down(["D = A", "C"]).column_names == ["B", "D", "C"]
# renaming or moving no column is the table itself
for same in (a.rename_columns([]), a.move_columns(1, []), a.move_columns_up([]), a.move_columns_down([])):
    assert same.j_table.is_same(a.j_table)
assert a.coalesce().j_table.is_same(a.j_table)

assert rows(a.where_one_of(["A > 1", "B == 0"])) == [(0, 0, 0, 0), (2, 4, 6, 8)]
assert rows(a.where_one_of(["A > 1", "B == 0", "C == 3"])) == rows(a)
# one alternative is that filter
assert [row["A"] for row in a.where_one_of("A > 0").iter_dict()] == [1, 2]

# with no columns, every row or none, by whether the other table has a row
some, none = a.head(1), a.head(0)
assert (a.where_in(some, []).size, a.where_in(none, []).size) == (3, 0)
assert (a.where_not_in(some, []).size, a.where_not_in(none, []).size) == (0, 3)
assert a.where_in(some, None).size == 3
assert a.where_in(some, "A").size == 1

r = a.restrict_sort_to("B")
assert r.attributes() == {"SortableColumns": "B"}
assert a.restrict_sort_to(["D", "B"]).attributes() == {"SortableColumns": "D,B"}
assert r.restrict_sort_to("B").j_table.is_same(r.j_table)
assert rows(r.sort_descending("B"))[0] == (2, 4, 6, 8)
assert r.rename_columns("X = B").attributes() == {"SortableColumns": "X"}

x = empty_table(3).update(["K = i", "V = i * 10"])
y = empty_table(3).update(["K = i + 1", "V = i * 100"])
merged = merge_sorted([x, y], "K")
assert rows(merged) == [(0, 0), (1, 10), (1, 0), (2, 20), (2, 100), (3, 200)]
assert (merged.is_flat, merged.is_refreshing, merged.attributes()) == (False, False, {})
assert rows(merge_sorted([y, x], "K")) == [(0, 0), (1, 0), (1, 10), (2, 100), (2, 20), (3, 200)]
nothing = merge_sorted([], "K")
assert (nothing.column_names, nothing.size) == ([], 0)
"#,
        )
        .await;
    }

    #[tokio::test]
    async fn the_attributes_of_a_table_are_those_java_gives_it() {
        check(
            r#"
from deephaven import empty_table, merge, time_table
a = empty_table(3).update(["A = i", "B = 2 - i"])
assert a.attributes() == {}
assert a.sort("B").attributes() == {"SortedColumns": "B=Ascending", "SortRowRedirection": "B"}
assert a.sort_descending("B").attributes() == {
    "SortedColumns": "B=Descending", "SortRowRedirection": "+IDENTITY_REDIRECTION"}
assert a.where("A > 0").attributes() == {"AddOnly": True, "AppendOnly": True}
assert merge([a, a]).attributes() == {"MergedTable": True}
assert merge([a, a]).rename_columns("X = A").attributes() == {"MergedTable": True}
assert a.sort("B").rename_columns("X = A").attributes() == {}
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC")
assert clock.attributes() == {"AddOnly": True, "AppendOnly": True}
assert clock.head(1).attributes() == {"AppendOnly": True}
assert clock.head_pct(0.5).attributes() == {"AddOnly": True, "AppendOnly": True}
assert clock.tail_pct(0.5).attributes() == {}
assert time_table("PT1h", blink_table=True).attributes() == {"BlinkTable": True}
# an attribute that is a Java object in the reference is there, with a placeholder
ticking = clock.update("K = ii % 3").sort("K").attributes()
assert sorted(ticking) == ["SortReverseLookup", "SortRowRedirection", "SortedColumns"], ticking
assert (ticking["SortedColumns"], ticking["SortRowRedirection"]) == ("K=Ascending", "K")
assert repr(ticking["SortReverseLookup"]) == "<java.util.function.LongUnaryOperator>"
"#,
        )
        .await;
    }

    #[tokio::test]
    async fn the_local_operations_refuse_what_java_refuses() {
        check(
            r#"
from deephaven import DHError, empty_table, merge_sorted, time_table
t = empty_table(8).update(["I = i"])
a = empty_table(3).update(["A = i", "B = i * 2", "C = i * 3", "D = i * 4"])
r = a.restrict_sort_to("B")
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC")
blink = time_table("PT1h", blink_table=True)
refused = [
    (lambda: t.head_pct(1.5), "table head_pct operation failed. : ",
     "percentage of rows must be between [0,1]: percent=1.5"),
    (lambda: t.tail_pct(-0.1), "table tail_pct operation failed. : ",
     "percentage of rows must be between [0,1]: percent=-0.1"),
    (lambda: t.slice_pct(-1.0, 0.5), "table slice_pct operation failed. : ",
     "Cannot slice with start percentage (-1.0) and end percentage (0.5), percentages must be between [0, 1]"),
    (lambda: blink.tail_pct(1.5), "table tail_pct operation failed. : ",
     "Blink tables do not support tailPct; use BlinkTableTools.blinkToAppendOnly to accumulate full history"),
    (lambda: a.rename_columns(["X = Q", "Y = A", "Y = A"]), "table rename_columns operation failed. : ",
     "Column(s) not found: Q\nDuplicate source column(s): A\nDuplicate destination column(s): Y"),
    (lambda: a.rename_columns("1X = A"), "table rename_columns operation failed. : ",
     'Invalid column name "1X"'),
    (lambda: a.move_columns(1, "Q"), "table move_columns operation failed. : ",
     "Column(s) not found: Q"),
    (lambda: a.where_one_of([]), "table where_one_of operation failed. : ",
     "Should not be constructing literal boolean WhereFilter"),
    (lambda: a.where_one_of(["A > 1", "Q == 0"]), "table where_one_of operation failed. : ",
     'Column "Q" doesn\'t exist in this table, available columns: [A, B, C, D]'),
    (lambda: a.restrict_sort_to(["B", "Q", "Z"]), "table restrict_sort_to operation failed. : ",
     "Unknown column names [Q, Z], available column names are [A, B, C, D]"),
    (lambda: r.sort("A"), "table sort operation failed. : ",
     "Sorting is not allowed on this table, but was attempted on: A but was attempted on: A"),
    (lambda: merge_sorted([a, a], "Q"), "merge sorted operation failed. : ",
     "Unknown column names [Q], available column names are [A, B, C, D]"),
    (lambda: merge_sorted([a, a.view(["A", "B", "C", "W = D"])], "A"), "merge sorted operation failed. : ",
     "Incompatible column sources: [A, B, C, D] and [A, B, C, W]"),
    # Java refuses a sorted merge of refreshing tables
    (lambda: merge_sorted([clock, clock], "Timestamp"), "merge sorted operation failed. : ",
     "mergeSorted does not yet support refreshing tables!"),
]
for call, failure, text in refused:
    try:
        call()
    except DHError as e:
        assert str(e).startswith(failure), str(e)
        assert text in str(e), str(e)
    else:
        raise AssertionError(text + " was answered")
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
