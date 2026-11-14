//! `ungroup` against the results recorded from the Java server (`oracle/corpus`, see
//! `oracle/README.md`).
//!
//! The replay of `ops/oracle.rs` builds what it can from the operations that exist, which
//! for `ungroup` is little: every array column of the scenarios comes from `group_by`, the
//! `group` aggregation or a join of their results, which are not ported.
//!
//! So a second test goes around that for the values: it takes the tables Java recorded
//! for those sources as static tables, with their vector columns, ungroups them and
//! compares the result with the table Java recorded for the `ungroup`. That covers the
//! static path at every step; the updates of a ticking `ungroup` are compared with Java
//! by `fixtures.rs`.

use super::{ungroup, UngroupError};
use crate::ops::oracle::{list, replay_all, text, Built};
use crate::ops::sort::testing::static_table;
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::ipc::reader::FileReader;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::TableSnapshot;
use serde_json::Value;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `oracle/corpus/group_by_ungroup`: `unknown`, `not_an_array` and `uneven_strict`.
fn code(error: &UngroupError) -> &'static str {
    match error {
        UngroupError::NoSuchColumn { .. }
        | UngroupError::NotAnArray(_)
        | UngroupError::SizesDiffer { .. } => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

fn columns_of(operation: &Value) -> Vec<String> {
    let columns = list(&operation["columns"]).iter();
    columns.map(|column| text(column).to_string()).collect()
}

/// The oracle's scenarios, as far as they can be built from `ungroup`.
#[test]
fn the_results_of_the_oracle_match_java() {
    let define = |operation: &Value, sources: &[Arc<dyn Table>]| -> Built {
        let null_fill = operation["null_fill"] == true;
        match ungroup(&sources[0], null_fill, &columns_of(operation)) {
            Ok(table) => Built::Table(table),
            Err(error) => Built::Refused(code(&error)),
        }
    };
    let tally = replay_all(&["ungroup"], &define);
    let differences: [&str; 0] = [];
    assert_eq!(tally.differences, differences, "differences from Java");
    // The two whose source is an input: a table without arrays, and a column that is
    // not an array.
    let compared = [
        "group_by_ungroup/no_arrays",
        "group_by_ungroup/not_an_array",
    ];
    assert_eq!(tally.compared, compared, "compared");
    assert_eq!(tally.comparisons, 2);
    // Results made from an operation of another test, all of which make the arrays.
    let not_run = [
        "group_by_ungroup/back: group_by",
        "group_by_ungroup/back_two: group_by",
        "group_by_ungroup/back_all: group_by",
        "group_by_ungroup/back_empty: group_by",
        "group_by_ungroup/back_some: group_by",
        "group_by_ungroup/back_one: group_by",
        "group_by_ungroup/uneven_filled: natural_join",
        "group_by_ungroup/uneven_strict: natural_join",
        "group_by_ungroup/uneven_one: natural_join",
        "group_by_ungroup/unknown: group_by",
        "group_by_ungroup/agg_back: agg_by",
        "ticking_group_ungroup_head_by/back: group_by",
        "ticking_group_ungroup_head_by/back_ids: group_by",
    ];
    assert_eq!(tally.not_run, not_run, "not run");
}

// ---------------------------------------------------------------------------------------
// the tables Java recorded as sources

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn json(path: PathBuf) -> Value {
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// What Java answered for `table` of `scenario` at `step`: the table, or the status code
/// of its refusal.
fn recorded(scenario: &str, step: usize, table: &str) -> Result<RecordBatch, String> {
    let directory = oracle().join("corpus").join(scenario);
    let manifest = json(directory.join("manifest.json"));
    let entries = list(&manifest["entries"]).iter();
    let entry = entries
        .clone()
        .find(|entry| entry["step"] == step && entry["table"] == table)
        .unwrap_or_else(|| panic!("no recording of {scenario}/{table} at step {step}"));
    if !entry["failure"].is_null() {
        return Err(text(&entry["failure"]["code"]).to_string());
    }
    let path = directory.join(text(&entry["file"]));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let reader = FileReader::try_new(file, None).expect("an Arrow IPC file");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("a batch")).collect();
    Ok(arrow::compute::concat_batches(&schema, &batches).expect("one schema"))
}

/// The column type of a field of a recording. A list is a vector or an array by the
/// `deephaven:type` of the field, `io.deephaven.vector.IntVector` or `int[]`.
fn column_type(field: &Field) -> ColumnType {
    fn of(data_type: &DataType, java_type: Option<&str>) -> ColumnType {
        match data_type {
            DataType::Boolean => ColumnType::Boolean,
            DataType::Int32 => ColumnType::Int,
            DataType::Int64 => ColumnType::Long,
            DataType::Float32 => ColumnType::Float,
            DataType::Float64 => ColumnType::Double,
            DataType::Utf8 => ColumnType::String,
            DataType::Timestamp(TimeUnit::Nanosecond, _) => ColumnType::Instant,
            DataType::List(element) => {
                let element = of(element.data_type(), None);
                match java_type {
                    Some(java) if java.starts_with("io.deephaven.vector.") => {
                        ColumnType::vector(element)
                    }
                    Some(java) if java.ends_with("[]") => ColumnType::array(element),
                    other => panic!("a list that is {other:?}"),
                }
            }
            other => panic!("no column type for {other}"),
        }
    }
    let java_type = field.metadata().get("deephaven:type");
    of(field.data_type(), java_type.map(String::as_str))
}

/// A static table of `source`, a table Java recorded, at the row keys `0..size`.
fn table_of(source: &RecordBatch) -> Arc<dyn Table> {
    let schema = source.schema();
    let fields = schema.fields().iter().zip(source.columns());
    let columns: Vec<(&str, ColumnType, ArrayRef)> = fields
        .map(|(field, values)| (field.name().as_str(), column_type(field), values.clone()))
        .collect();
    static_table(&columns)
}

/// Whether two arrays of one type hold the same cells. Floats are compared bit for bit,
/// except that every NaN equals every NaN. The cells of a vector or array column are the
/// same when both are null, or neither is and they have the same elements in order; a
/// null cell is not an empty one.
///
/// Behavior: the comparison rules of `oracle/README.md`
fn same_cells(expected: &ArrayRef, actual: &ArrayRef) -> bool {
    fn bits<A: Array + 'static>(
        expected: &ArrayRef,
        actual: &ArrayRef,
        bits: impl Fn(&A, usize) -> Option<u64>,
    ) -> bool {
        let expected = expected.as_any().downcast_ref::<A>().expect("the type");
        let Some(actual) = actual.as_any().downcast_ref::<A>() else {
            return false;
        };
        (0..expected.len()).all(|row| {
            let (e, a) = (expected.is_null(row), actual.is_null(row));
            e == a && (e || bits(expected, row) == bits(actual, row))
        })
    }
    if expected.len() != actual.len() {
        return false;
    }
    match expected.data_type() {
        DataType::Float32 => bits::<Float32Array>(expected, actual, |array, row| {
            let value = array.value(row);
            (!value.is_nan()).then(|| u64::from(value.to_bits()))
        }),
        DataType::Float64 => bits::<Float64Array>(expected, actual, |array, row| {
            let value = array.value(row);
            (!value.is_nan()).then(|| value.to_bits())
        }),
        DataType::List(_) => {
            let expected = expected
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("lists");
            let Some(actual) = actual.as_any().downcast_ref::<ListArray>() else {
                return false;
            };
            (0..expected.len()).all(|row| {
                let (e, a) = (expected.is_null(row), actual.is_null(row));
                e == a && (e || same_cells(&expected.value(row), &actual.value(row)))
            })
        }
        _ => expected == actual,
    }
}

/// How `table` differs from the table `expected` that Java returned, if it does: the
/// columns' names and types, a vector not being an array, the number of rows, and the
/// cells by position.
fn difference(expected: &RecordBatch, table: &dyn Table) -> Option<String> {
    let schema = expected.schema();
    let columns = table.definition().columns().iter();
    let ours: Vec<(&str, ColumnType)> = columns
        .map(|c| (c.name.as_str(), c.column_type.clone()))
        .collect();
    let fields = schema.fields().iter();
    let theirs: Vec<(&str, ColumnType)> = fields
        .map(|f| (f.name().as_str(), column_type(f)))
        .collect();
    if ours != theirs {
        return Some(format!("columns {ours:?}, Java has {theirs:?}"));
    }
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    if snapshot.rows.size() != expected.num_rows() as u64 {
        let (ours, theirs) = (snapshot.rows.size(), expected.num_rows());
        return Some(format!("{ours} rows, Java has {theirs}"));
    }
    (0..ours.len()).find_map(|index| {
        let (ours, theirs) = (&snapshot.columns[index], expected.column(index));
        (!same_cells(theirs, ours)).then(|| {
            let name = schema.field(index).name();
            format!("column {name} is {ours:?}, Java has {theirs:?}")
        })
    })
}

/// Every `ungroup` of the two scenarios that have one, of the table Java recorded for
/// its source, at every step. The attributes are not compared: the source here is a
/// static copy.
#[test]
fn the_ungrouped_tables_of_the_recorded_arrays_match_java() {
    let mut compared = Vec::new();
    let mut tables = 0;
    let mut elements = 0;
    let mut refused = 0;
    let scenarios = [
        ("static", "group_by_ungroup"),
        ("ticking", "ticking_group_ungroup_head_by"),
    ];
    for (kind, name) in scenarios {
        let path = oracle().join("scenarios").join(kind);
        let scenario = json(path.join(format!("{name}.json")));
        let inputs = list(&scenario["inputs"]).iter();
        let inputs: Vec<&str> = inputs.map(|input| text(&input["name"])).collect();
        let steps = list(&scenario["steps"]).len();
        let operations = list(&scenario["operations"]).iter();
        for operation in operations.filter(|operation| operation["op"] == "ungroup") {
            let result = text(&operation["name"]);
            let source = text(&operation["source"]);
            if inputs.contains(&source) {
                // Compared by the replay above.
                continue;
            }
            let null_fill = operation["null_fill"] == true;
            let asked = columns_of(operation);
            for step in 0..=steps {
                let place = format!("{name}/{result} at step {step}");
                let source = recorded(name, step, source).expect("a source");
                let ours = ungroup(&table_of(&source), null_fill, &asked);
                let difference = match (recorded(name, step, result), ours) {
                    (Ok(expected), Ok(ours)) => {
                        elements += expected.num_rows();
                        difference(&expected, &*ours)
                    }
                    (Err(expected), Err(error)) => {
                        refused += 1;
                        (expected != code(&error))
                            .then(|| format!("refused with {error}, Java with {expected}"))
                    }
                    (Ok(_), Err(error)) => Some(format!("refused: {error}")),
                    (Err(expected), Ok(_)) => Some(format!("Java fails with {expected}")),
                };
                assert_eq!(difference, None, "{place}");
                tables += 1;
            }
            compared.push(format!("{name}/{result}"));
        }
    }
    let expected = [
        "group_by_ungroup/back",
        "group_by_ungroup/back_two",
        "group_by_ungroup/back_all",
        "group_by_ungroup/back_empty",
        "group_by_ungroup/back_some",
        "group_by_ungroup/back_one",
        "group_by_ungroup/uneven_filled",
        "group_by_ungroup/uneven_strict",
        "group_by_ungroup/uneven_one",
        "group_by_ungroup/unknown",
        "group_by_ungroup/agg_back",
        "ticking_group_ungroup_head_by/back",
        "ticking_group_ungroup_head_by/back_ids",
    ];
    assert_eq!(compared, expected, "compared");
    // 11 static results at one step and 2 ticking ones at 9 steps; `uneven_strict` and
    // `unknown` are the refusals.
    assert_eq!(tables, 11 + 2 * 9);
    assert_eq!(refused, 2);
    assert!(elements > 100, "{elements} rows compared");
}
