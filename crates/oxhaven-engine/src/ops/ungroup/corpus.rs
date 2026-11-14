//! `ungroup` against the results recorded from the Java server (`oracle/corpus`, see
//! `oracle/README.md`).
//!
//! The replay of `ops/oracle.rs` builds what it can from the operations that exist, which
//! for `ungroup` is little: every array column of the scenarios comes from `group_by`, the
//! `group` aggregation or a join of their results, and the engine has no array columns.
//!
//! So a second test goes around that for the values: it takes the tables Java recorded
//! for those sources, with their list columns as stand-ins for array columns, ungroups
//! them and compares the result with the table Java recorded for the `ungroup`. That
//! covers the static path at every step; the updates of a ticking `ungroup` are not
//! compared with Java anywhere.

use super::keys::MINIMUM_BASE;
use super::operator::{ungroup_arrays, Ungrouped};
use super::sources::ArrayColumn;
use super::{ungroup, UngroupError};
use crate::ops::oracle::{list, replay_all, text, Built};
use crate::ops::sort::testing::static_table;
use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, ListArray, RecordBatch, UInt64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::reader::FileReader;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
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

fn column_type(data_type: &DataType) -> ColumnType {
    match data_type {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int32 => ColumnType::Int,
        DataType::Int64 => ColumnType::Long,
        DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Utf8 => ColumnType::String,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => ColumnType::Instant,
        other => panic!("no column type for {other}"),
    }
}

/// A list column Java recorded, as the array column of a table whose row keys are the
/// positions of the recording.
struct Recorded {
    lists: ListArray,
    element: ColumnType,
}

impl Recorded {
    fn size(&self, row: RowKey) -> u64 {
        let row = row as usize;
        if self.lists.is_null(row) {
            0
        } else {
            self.lists.value_length(row) as u64
        }
    }
}

impl ArrayColumn for Recorded {
    fn element_type(&self) -> &ColumnType {
        &self.element
    }

    fn sizes(&self, rows: &RowSet, _previous: bool) -> Result<Vec<u64>, TableFailure> {
        Ok(rows.iter().map(|row| self.size(row)).collect())
    }

    fn elements(&self, cells: &[(RowKey, u64)], _previous: bool) -> Result<ArrayRef, TableFailure> {
        let offsets = self.lists.value_offsets();
        let places = cells.iter().map(|(row, offset)| {
            (*offset < self.size(*row)).then(|| offsets[*row as usize] as u64 + offset)
        });
        let places: UInt64Array = places.collect();
        Ok(arrow::compute::take(self.lists.values(), &places, None).expect("the elements"))
    }
}

/// A static table of `source`, a table Java recorded, and its list columns as array
/// columns. In the table a list column is an `Int` column of nulls.
fn stand_in(source: &RecordBatch) -> (Arc<dyn Table>, Vec<(String, Ungrouped)>) {
    let schema = source.schema();
    let mut columns = Vec::new();
    let mut arrays = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        let values = source.column(index);
        match field.data_type() {
            DataType::List(element) => {
                let nulls: Int32Array = (0..values.len()).map(|_| None).collect();
                columns.push((field.name().as_str(), ColumnType::Int, Arc::new(nulls) as _));
                let lists = values.as_any().downcast_ref::<ListArray>().expect("lists");
                let recorded = Recorded {
                    lists: lists.clone(),
                    element: column_type(element.data_type()),
                };
                let ungrouped = Ungrouped {
                    index,
                    arrays: Arc::new(recorded),
                };
                arrays.push((field.name().clone(), ungrouped));
            }
            other => columns.push((field.name().as_str(), column_type(other), values.clone())),
        }
    }
    (static_table(&columns), arrays)
}

/// Whether two arrays of one type hold the same cells. Floats are compared bit for bit,
/// except that every NaN equals every NaN.
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
    match expected.data_type() {
        DataType::Float32 => bits::<Float32Array>(expected, actual, |array, row| {
            let value = array.value(row);
            (!value.is_nan()).then(|| u64::from(value.to_bits()))
        }),
        DataType::Float64 => bits::<Float64Array>(expected, actual, |array, row| {
            let value = array.value(row);
            (!value.is_nan()).then(|| value.to_bits())
        }),
        _ => expected == actual,
    }
}

/// How `table` differs from the table `expected` that Java returned, if it does: the
/// columns' names and types, the number of rows, and the cells by position. A list column
/// that was not ungrouped is compared by its name alone: the stand-in table has no lists.
fn difference(expected: &RecordBatch, table: &dyn Table) -> Option<String> {
    let schema = expected.schema();
    let is_list = |index: usize| {
        let field = schema.fields().get(index);
        field.is_some_and(|field| matches!(field.data_type(), DataType::List(_)))
    };
    let columns = table.definition().columns().iter().enumerate();
    let ours: Vec<(&str, Option<DataType>)> = columns
        .map(|(index, c)| {
            let data_type = (!is_list(index)).then(|| c.column_type.arrow_type());
            (c.name.as_str(), data_type)
        })
        .collect();
    let fields = schema.fields().iter().enumerate();
    let theirs: Vec<(&str, Option<DataType>)> = fields
        .map(|(index, f)| {
            let data_type = (!is_list(index)).then(|| f.data_type().clone());
            (f.name().as_str(), data_type)
        })
        .collect();
    if ours != theirs {
        return Some(format!("columns {ours:?}, Java has {theirs:?}"));
    }
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    if snapshot.rows.size() != expected.num_rows() as u64 {
        let (ours, theirs) = (snapshot.rows.size(), expected.num_rows());
        return Some(format!("{ours} rows, Java has {theirs}"));
    }
    let mut compared = (0..ours.len()).filter(|index| !is_list(*index));
    compared.find_map(|index| {
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
    let mut not_run = Vec::new();
    let mut tables = 0;
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
            let mut ran = false;
            for step in 0..=steps {
                let place = format!("{name}/{result} at step {step}");
                let source = recorded(name, step, source).expect("a source");
                let (table, arrays) = stand_in(&source);
                let is_array = |name: &String| arrays.iter().any(|(array, _)| array == name);
                if !asked.iter().all(is_array) {
                    // The public operation refuses these, and cannot see stand-ins.
                    not_run.push(format!("{place}: {asked:?} are not all arrays"));
                    continue;
                }
                // The columns asked for in their order, or every array column.
                let mut arrays: Vec<(String, Ungrouped)> = arrays;
                let mut columns = Vec::new();
                if asked.is_empty() {
                    columns.extend(arrays.into_iter().map(|(_, array)| array));
                } else {
                    for name in &asked {
                        let at = arrays.iter().position(|(array, _)| array == name);
                        columns.push(arrays.remove(at.expect("an array")).1);
                    }
                }
                let ours = ungroup_arrays(&table, null_fill, columns, MINIMUM_BASE);
                let difference = match (recorded(name, step, result), ours) {
                    (Ok(expected), Ok(ours)) => difference(&expected, &*ours),
                    (Err(expected), Err(error)) => (expected != code(&error))
                        .then(|| format!("refused with {error}, Java with {expected}")),
                    (Ok(_), Err(error)) => Some(format!("refused: {error}")),
                    (Err(expected), Ok(_)) => Some(format!("Java fails with {expected}")),
                };
                assert_eq!(difference, None, "{place}");
                tables += 1;
                ran = true;
            }
            if ran {
                compared.push(format!("{name}/{result}"));
            }
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
        "group_by_ungroup/agg_back",
        "ticking_group_ungroup_head_by/back",
        "ticking_group_ungroup_head_by/back_ids",
    ];
    assert_eq!(compared, expected, "compared");
    let unknown = ["group_by_ungroup/unknown at step 0: [\"Nope\"] are not all arrays"];
    assert_eq!(not_run, unknown, "not run");
    // 10 static results at one step and 2 ticking ones at 9 steps.
    assert_eq!(tables, 10 + 2 * 9);
}
