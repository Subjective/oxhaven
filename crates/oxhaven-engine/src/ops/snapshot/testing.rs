//! Replays the fixtures of this module and of `ops/distinct` and compares each result with
//! what the Java server answered. `fixtures/record.py` documents the fixture format and
//! records the answers.
//!
//! The comparison is that of `oracle/harness/rules.py` as far as it applies: column names,
//! order and types, the row count, the cells by position (floats bit for bit, every NaN
//! equal to every NaN), and the attributes `IsFlat`, `AddOnly`, `AppendOnly` and
//! `BlinkTable`. Where Java refused an operation, it must be refused here with the same
//! status code.
//!
//! For the tables a fixture names under `updates`, every update is compared as well: the
//! added, removed and modified rows by position, update by update.

use crate::ops::sort::testing::graph;
use crate::ops::sort::{head, sort, tail, SortColumn};
use crate::table::{append_only_input_table, in_memory_table, keyed_input_table};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{definition, Recorder, TableSnapshot};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub(crate) fn list(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

pub(crate) fn text(value: &Value) -> &str {
    value.as_str().expect("a string")
}

pub(crate) fn texts(value: &Value) -> Vec<&str> {
    list(value).iter().map(text).collect()
}

fn float_cell(cell: &Value) -> Option<f64> {
    match cell {
        Value::Null => None,
        Value::String(word) => Some(match word.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            other => panic!("unknown float word {other}"),
        }),
        number => Some(number.as_f64().expect("a number")),
    }
}

fn int_cell(cell: &Value) -> Option<i64> {
    (!cell.is_null()).then(|| cell.as_i64().expect("an integer"))
}

fn narrow<T: TryFrom<i64>>(cell: &Value) -> Option<T> {
    int_cell(cell).map(|v| T::try_from(v).ok().expect("a value in the column's range"))
}

fn column_type(name: &str) -> ColumnType {
    match name {
        "bool" => ColumnType::Boolean,
        "int8" => ColumnType::Byte,
        "char" => ColumnType::Char,
        "int16" => ColumnType::Short,
        "int32" => ColumnType::Int,
        "int64" => ColumnType::Long,
        "float32" => ColumnType::Float,
        "float64" => ColumnType::Double,
        "string" => ColumnType::String,
        "timestamp" => ColumnType::Instant,
        other => panic!("no column of type {other}"),
    }
}

/// Column `index` of `rows` as an array of `column_type`.
fn cells(column_type: &ColumnType, rows: &[Value], index: usize) -> ArrayRef {
    let cells = rows.iter().map(|row| &row[index]);
    match column_type {
        ColumnType::Boolean => Arc::new(cells.map(Value::as_bool).collect::<BooleanArray>()),
        ColumnType::Byte => Arc::new(cells.map(narrow).collect::<Int8Array>()),
        ColumnType::Char => Arc::new(cells.map(narrow).collect::<UInt16Array>()),
        ColumnType::Short => Arc::new(cells.map(narrow).collect::<Int16Array>()),
        ColumnType::Int => Arc::new(cells.map(narrow).collect::<Int32Array>()),
        ColumnType::Long => Arc::new(cells.map(int_cell).collect::<Int64Array>()),
        ColumnType::Float => {
            let cells = cells.map(|cell| float_cell(cell).map(|v| v as f32));
            Arc::new(cells.collect::<Float32Array>())
        }
        ColumnType::Double => Arc::new(cells.map(float_cell).collect::<Float64Array>()),
        ColumnType::String => Arc::new(cells.map(Value::as_str).collect::<StringArray>()),
        ColumnType::Instant => {
            let nanos = cells.map(int_cell).collect::<TimestampNanosecondArray>();
            Arc::new(nanos.with_timezone("UTC"))
        }
        other => panic!("no cells of type {other:?}"),
    }
}

fn columns_of(value: &Value) -> Vec<(String, ColumnType)> {
    let columns = list(&value["columns"]).iter();
    columns
        .map(|column| {
            (
                text(&column["name"]).to_string(),
                column_type(text(&column["type"])),
            )
        })
        .collect()
}

fn batch(columns: &[(String, ColumnType)], rows: &[Value]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| Field::new(name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, (_, column_type))| cells(column_type, rows, index))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch")
}

/// One input of a fixture.
struct Input {
    table: Arc<dyn Table>,
    columns: Vec<(String, ColumnType)>,
    keys: Vec<String>,
}

impl Input {
    fn add(&self, rows: &[Value]) {
        let input = self.table.input_table().expect("an input table");
        input.add(batch(&self.columns, rows)).expect("an add");
    }

    fn delete(&self, rows: &[Value]) {
        let keys: Vec<(String, ColumnType)> = self
            .columns
            .iter()
            .filter(|(name, _)| self.keys.contains(name))
            .cloned()
            .collect();
        let input = self.table.input_table().expect("an input table");
        input.delete(batch(&keys, rows)).expect("a delete");
    }
}

/// Behavior: `record` in `fixtures/record.py`: the first rows of an input table go in
/// through `add`.
fn load(graph: &Arc<dyn UpdateGraph>, input: &Value) -> Input {
    let columns = columns_of(input);
    let named: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, t)| (name.as_str(), t.clone()))
        .collect();
    let definition = definition(&named);
    let rows = list(&input["rows"]);
    let ticking = &input["ticking"];
    if ticking.is_null() {
        let table = in_memory_table(definition, &[batch(&columns, rows)]).expect("a table");
        return Input {
            table,
            columns,
            keys: Vec::new(),
        };
    }
    let keys: Vec<String> = texts(&ticking["key_columns"])
        .into_iter()
        .map(str::to_string)
        .collect();
    let table = if keys.is_empty() {
        append_only_input_table(graph.clone(), definition, None)
    } else {
        let names: Vec<&str> = keys.iter().map(String::as_str).collect();
        keyed_input_table(graph.clone(), definition, &names, None)
    };
    let loaded = Input {
        table: table.expect("an input table"),
        columns,
        keys,
    };
    if !rows.is_empty() {
        loaded.add(rows);
    }
    loaded
}

/// What became of one table of a fixture: the table, or the gRPC status code that the
/// handler will answer with for the refusal.
pub(crate) type Built = Result<Arc<dyn Table>, &'static str>;

/// Whether two arrays of one type hold the same cells. Floats are compared bit for bit,
/// except that every NaN equals every NaN.
fn same_cells(expected: &ArrayRef, actual: &ArrayRef) -> bool {
    fn bits<A: Array + 'static, B: PartialEq>(
        expected: &ArrayRef,
        actual: &ArrayRef,
        cell: impl Fn(&A, usize) -> B,
    ) -> bool {
        let (Some(e), Some(a)) = (
            expected.as_any().downcast_ref::<A>(),
            actual.as_any().downcast_ref::<A>(),
        ) else {
            return false;
        };
        e.len() == a.len()
            && (0..e.len()).all(|row| {
                e.is_null(row) == a.is_null(row) && (e.is_null(row) || cell(e, row) == cell(a, row))
            })
    }
    match expected.data_type() {
        DataType::Float32 => bits::<Float32Array, _>(expected, actual, |array, row| {
            let value = array.value(row);
            (!value.is_nan()).then(|| value.to_bits())
        }),
        DataType::Float64 => bits::<Float64Array, _>(expected, actual, |array, row| {
            let value = array.value(row);
            (!value.is_nan()).then(|| value.to_bits())
        }),
        _ => expected == actual,
    }
}

fn compare(place: &str, expected: &Value, table: &dyn Table) {
    let columns = columns_of(expected);
    let found: Vec<(String, ColumnType)> = table
        .definition()
        .columns()
        .iter()
        .map(|c| (c.name.clone(), c.column_type.clone()))
        .collect();
    assert_eq!(found, columns, "{place}: columns");
    let rows = list(&expected["rows"]);
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    assert_eq!(
        snapshot.rows.size(),
        rows.len() as u64,
        "{place}: row count"
    );
    for (index, (name, column_type)) in columns.iter().enumerate() {
        let expected = cells(column_type, rows, index);
        assert!(
            same_cells(&expected, &snapshot.columns[index]),
            "{place}: column {name} is {:?}, Java has {expected:?}",
            snapshot.columns[index],
        );
    }
    let attributes = table.attributes();
    let found = [
        table.is_flat(),
        attributes.add_only,
        attributes.append_only,
        attributes.blink,
    ];
    let recorded = ["IsFlat", "AddOnly", "AppendOnly", "BlinkTable"].map(|name| {
        expected["attributes"][name]
            .as_bool()
            .expect("an attribute")
    });
    assert_eq!(
        found, recorded,
        "{place}: the attributes IsFlat, AddOnly, AppendOnly, BlinkTable"
    );
}

/// The rows of a recorded update, which are positions.
fn positions(rows: &Value) -> Vec<i64> {
    let rows = list(rows).iter();
    rows.map(|row| row.as_i64().expect("a position")).collect()
}

/// Compares the updates a table delivered during a step with the recorded ones. The
/// recording has positions, removed rows by their position in the table before the update
/// and the others by their position after it, so the row keys of each update are turned
/// into positions in `rows`, the table's row set before the update, and in what the
/// update makes of it, which `rows` then is.
///
/// An update that only shifts rows moves no position, so it is not an update to this
/// comparison, on either side: row keys are ours to choose (`PARITY.md` A4), and a table
/// here may shift where Java's does not, as the rows after a removed row of a sort do.
fn compare_updates(
    place: &str,
    expected: &Value,
    recorder: &Recorder,
    seen: &mut usize,
    rows: &mut RowSet,
) {
    let events = recorder.events().split_off(*seen);
    *seen += events.len();
    let found: Vec<[Vec<i64>; 3]> = events
        .into_iter()
        .map(|event| {
            let update = event.unwrap_or_else(|failure| panic!("{place}: {}", failure.message));
            let after = update.apply_to(rows).expect("a valid update");
            let positions = |keys: &RowSet, within: &RowSet| -> Vec<i64> {
                let found = within.invert(keys).expect("rows of the table");
                found.iter().collect()
            };
            let found = [
                positions(&update.added, &after),
                positions(&update.removed, rows),
                positions(&update.modified, &after),
            ];
            *rows = after;
            found
        })
        .filter(|found| found.iter().any(|rows| !rows.is_empty()))
        .collect();
    let recorded: Vec<[Vec<i64>; 3]> = list(expected)
        .iter()
        .map(|update| ["added", "removed", "modified"].map(|rows| positions(&update[rows])))
        .filter(|recorded| recorded.iter().any(|rows| !rows.is_empty()))
        .collect();
    assert_eq!(
        found, recorded,
        "{place}: the added, removed and modified positions of each update"
    );
}

/// Builds the table of an operation from the operation and its source tables.
pub(crate) type Define<'a> = &'a dyn Fn(&Value, &[Arc<dyn Table>]) -> Built;

/// Replays the fixture at `path`. `define` builds the table of an operation that this file
/// does not know (it knows `sort`, `head` and `tail`), given the operation and its source
/// tables in the order `source`, `trigger`, `set`. Returns the number of comparisons made:
/// one per result and step, including the refusals, and one per table of `updates` and
/// step after the first.
pub(crate) fn replay(path: &Path, define: Define) -> usize {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let fixture: Value =
        serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let name = text(&fixture["name"]);
    let graph = graph();

    let mut inputs: BTreeMap<&str, Input> = BTreeMap::new();
    let mut tables: BTreeMap<&str, Built> = BTreeMap::new();
    for input in list(&fixture["inputs"]) {
        let loaded = load(&graph, input);
        tables.insert(text(&input["name"]), Ok(loaded.table.clone()));
        inputs.insert(text(&input["name"]), loaded);
    }
    for operation in list(&fixture["operations"]) {
        let sources: Result<Vec<Arc<dyn Table>>, &'static str> = ["source", "trigger", "set"]
            .iter()
            .filter(|key| !operation[**key].is_null())
            .map(|key| tables[text(&operation[*key])].clone())
            .collect();
        let built = sources.and_then(|sources| match text(&operation["op"]) {
            "sort" => {
                let by = list(&operation["by"]).iter();
                let by: Vec<SortColumn> = by
                    .map(|by| match text(&by["direction"]) {
                        "ascending" => SortColumn::ascending(text(&by["column"])),
                        _ => SortColumn::descending(text(&by["column"])),
                    })
                    .collect();
                Ok(sort(&sources[0], &by).expect("a sort"))
            }
            "head" => Ok(head(&sources[0], operation["rows"].as_i64().unwrap()).expect("a head")),
            "tail" => Ok(tail(&sources[0], operation["rows"].as_i64().unwrap()).expect("a tail")),
            _ => define(operation, &sources),
        });
        tables.insert(text(&operation["name"]), built);
    }

    let mut recorders: Vec<(&str, Arc<Recorder>, usize, RowSet)> = texts(&fixture["updates"])
        .into_iter()
        .map(|name| {
            let table = tables[name].clone().expect("a table to listen to");
            let rows = (*table.row_set()).clone();
            (name, Recorder::attach(table), 0, rows)
        })
        .collect();
    let results = texts(&fixture["results"]);
    let recorded = list(&fixture["java"]["steps"]);
    let steps = list(&fixture["steps"]);
    assert_eq!(recorded.len(), steps.len() + 1, "{name}: recorded steps");
    let mut comparisons = 0;
    let mut update_comparisons = 0;
    let mut check = |step: usize| {
        for result in &results {
            let place = format!("{name}/{result} at step {step}");
            let expected = &recorded[step][*result];
            let failure = &expected["failure"];
            match &tables[*result] {
                Err(code) => {
                    assert!(!failure.is_null(), "{place}: refused, but Java has a table");
                    assert_eq!(*code, text(failure), "{place}: status code");
                }
                Ok(table) => {
                    assert!(failure.is_null(), "{place}: Java fails with {failure}");
                    // Java keeps serving a table that failed in a cycle; see `record.py`.
                    let fails = fixture["fails"][*result].as_u64();
                    let failed = fails.is_some_and(|at| step as u64 >= at);
                    assert_eq!(table.is_failed(), failed, "{place}: whether it has failed");
                    if !failed {
                        compare(&place, expected, &**table);
                    }
                }
            }
            comparisons += 1;
        }
    };

    check(0);
    for (index, step) in steps.iter().enumerate() {
        for action in list(&step["actions"]) {
            let input = &inputs[text(&action["table"])];
            let rows = list(&action["rows"]);
            match text(&action["action"]) {
                "add" => input.add(rows),
                "delete" => input.delete(rows),
                other => panic!("no action {other}"),
            }
        }
        check(index + 1);
        for (table, recorder, seen, rows) in &mut recorders {
            let place = format!("{name}/{table} during step {}", index + 1);
            let expected = &fixture["java"]["updates"][index][*table];
            compare_updates(&place, expected, recorder, seen, rows);
            update_comparisons += 1;
        }
    }
    comparisons + update_comparisons
}
