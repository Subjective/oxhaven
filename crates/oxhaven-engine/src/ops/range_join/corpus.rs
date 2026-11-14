//! Scenarios of `range_join`, checked against the results recorded from the Java server.
//! There are two sets:
//!
//! - the oracle's scenarios (`oracle/scenarios`, recorded in `oracle/corpus`, see
//!   `oracle/README.md`);
//! - the scenarios of `testdata/scenarios`, in the same format and recorded in
//!   `testdata/corpus` by `testdata/record.py`.
//!
//! A result of `range_join` has array-valued columns, which no table can hold yet, so the
//! shared replay of `ops/oracle.rs` can compare only what Java refuses: there the operation
//! must be refused here. The first test does that for every scenario of the oracle.
//!
//! The second test compares the results themselves, taken at the seam
//! (`grouped::Joined`): the column names and order, the Arrow type of every column, the
//! row count, and the cells by position (floats bit for bit, every NaN equal to every NaN,
//! arrays element by element, a null array equal only to a null array). The recorded
//! tables carry none of the attributes `IsFlat`, `AddOnly`, `AppendOnly` and `BlinkTable`,
//! which is checked of the recordings, since the seam has no attributes to compare.

use super::grouped::{join, Column, Joined};
use super::{RangeAggregation, RangeEndRule, RangeJoinError, RangeJoinMatch, RangeStartRule};
use crate::ops::agg::Pair;
use crate::ops::join::JoinMatch;
use crate::ops::oracle::{replay_all, Built};
use crate::table::in_memory_table;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, ListArray, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::definition;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn testdata() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops/range_join/testdata")
}

fn json(path: &Path) -> Value {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn list(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

fn text(value: &Value) -> &str {
    value.as_str().expect("a string")
}

/// Behavior: `decode_cell` in `oracle/harness/scenario.py`.
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

/// A static input of a scenario, as the harness uploads it.
fn load(input: &Value) -> Arc<dyn Table> {
    assert!(input["ticking"].is_null(), "a static input");
    let columns: Vec<(&str, ColumnType)> = list(&input["columns"])
        .iter()
        .map(|column| (text(&column["name"]), column_type(text(&column["type"]))))
        .collect();
    let rows = list(&input["rows"]);
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, (_, column_type))| cells(column_type, rows, index))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch");
    in_memory_table(definition(&columns), &[batch]).expect("a table")
}

/// Behavior: io.deephaven.api.JoinMatch#parse, for the forms the scenarios use: `A` and
/// `A = B`.
fn exact_match(text: &str) -> JoinMatch {
    match text.split_once('=') {
        None => JoinMatch::same(text.trim()),
        Some((left, right)) => JoinMatch::new(left.trim(), right.trim_start_matches('=').trim()),
    }
}

/// Behavior: `_aggregation` in `oracle/harness/runner.py`: `Out = In` and `Name` pairs.
fn aggregation(spec: &Value) -> RangeAggregation {
    let kind = text(&spec["agg"]);
    if kind != "group" {
        return RangeAggregation::Other(kind.to_string());
    }
    let pairs = list(&spec["columns"])
        .iter()
        .map(|column| match text(column).split_once('=') {
            None => Pair::of(text(column).trim()),
            Some((output, input)) => Pair::new(output.trim(), input.trim()),
        });
    RangeAggregation::group(pairs)
}

/// The scenario whose range matches `testdata/record.py` sends as the separate fields of
/// the request, because the text cannot name every pair of rules.
const BY_FIELDS: &str = "range_join_rule_pairs";

/// The range match that `range_join_by_fields` of `testdata/record.py` sends for `text`,
/// which is a match as io.deephaven.api.Strings#of(RangeJoinMatch) writes it. The request
/// becomes a match in io.deephaven.server.table.ops.RangeJoinGrpcImpl#adaptRangeMatch.
fn match_of_fields(text: &str) -> Result<RangeJoinMatch, RangeJoinError> {
    let mut words: Vec<&str> = text.split(' ').collect();
    let preceding = words.first() == Some(&"<-");
    let following = words.last() == Some(&"->");
    if preceding {
        words.remove(0);
    }
    if following {
        words.pop();
    }
    let [start, start_sign, value, end_sign, end] = words[..] else {
        panic!("not a range match: {text}");
    };
    let start_rule = match (preceding, start_sign) {
        (true, _) => RangeStartRule::LessThanOrEqualAllowPreceding,
        (false, "<") => RangeStartRule::LessThan,
        (false, _) => RangeStartRule::LessThanOrEqual,
    };
    let end_rule = match (following, end_sign) {
        (true, _) => RangeEndRule::GreaterThanOrEqualAllowFollowing,
        (false, "<") => RangeEndRule::GreaterThan,
        (false, _) => RangeEndRule::GreaterThanOrEqual,
    };
    RangeJoinMatch::new(start, start_rule, value, end_rule, end)
}

/// The `range_join` of `operation`, as `run_operation` in `oracle/harness/runner.py` sends
/// it: the last entry of `on` is the range match, the others are exact matches. With
/// `by_fields` the range match is the one `testdata/record.py` sends as separate fields.
fn joined(
    operation: &Value,
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    by_fields: bool,
) -> Result<Joined, RangeJoinError> {
    let on = list(&operation["on"]);
    let (range, exact) = on.split_last().expect("a range match");
    let exact: Vec<JoinMatch> = exact.iter().map(|m| exact_match(text(m))).collect();
    let range = if by_fields {
        match_of_fields(text(range))?
    } else {
        RangeJoinMatch::parse(text(range))?
    };
    let aggregations: Vec<RangeAggregation> =
        list(&operation["aggs"]).iter().map(aggregation).collect();
    join(left, right, &exact, &range, &aggregations)
}

/// Whether two arrays hold the same cells. Floats are compared bit for bit, except that
/// every NaN equals every NaN; arrays cell by cell under the rule of their elements.
fn same_cells(expected: &ArrayRef, actual: &ArrayRef) -> bool {
    fn bits<A: Array + 'static, B: PartialEq>(
        expected: &ArrayRef,
        actual: &ArrayRef,
        cell: impl Fn(&A, usize) -> B,
    ) -> bool {
        let e = expected.as_any().downcast_ref::<A>().expect("the type");
        let a = actual.as_any().downcast_ref::<A>().expect("the type");
        (0..e.len()).all(|row| {
            e.is_null(row) == a.is_null(row) && (e.is_null(row) || cell(e, row) == cell(a, row))
        })
    }
    if expected.data_type() != actual.data_type() || expected.len() != actual.len() {
        return false;
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
        DataType::List(_) => {
            let e = expected
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("lists");
            let a = actual.as_any().downcast_ref::<ListArray>().expect("lists");
            (0..e.len()).all(|row| {
                e.is_null(row) == a.is_null(row)
                    && (e.is_null(row) || same_cells(&e.value(row), &a.value(row)))
            })
        }
        _ => expected == actual,
    }
}

/// The Java type of a `group` column of elements of `element`, as the recordings have it
/// in `deephaven:type` and `deephaven:componentType`. Only the element types the
/// scenarios aggregate are known.
fn vector_type(element: &ColumnType) -> (&'static str, &'static str) {
    match element {
        ColumnType::Int => ("io.deephaven.vector.IntVector", "int"),
        ColumnType::Long => ("io.deephaven.vector.LongVector", "long"),
        ColumnType::Double => ("io.deephaven.vector.DoubleVector", "double"),
        ColumnType::String => ("io.deephaven.vector.ObjectVector", "java.lang.String"),
        other => panic!("no recording of a group of {other:?}"),
    }
}

/// How `joined` differs from the recorded table, if it does: the first difference found.
fn difference(joined: &Joined, expected: &RecordBatch) -> Option<String> {
    let schema = expected.schema();
    for name in ["IsFlat", "AddOnly", "AppendOnly", "BlinkTable"] {
        let key = format!("deephaven:attribute.{name}");
        if schema.metadata().get(&key).is_some_and(|v| v == "true") {
            return Some(format!(
                "Java's table is {name}; the seam has no attributes"
            ));
        }
    }
    let rows = joined.left.row_set();
    let ours: Vec<(&str, DataType)> = joined
        .columns
        .iter()
        .map(|(name, column)| {
            let data_type = match column {
                Column::Left(index) => joined.left.column(*index).column_type().arrow_type(),
                Column::Group(group) => {
                    let item = Field::new("", group.element_type().arrow_type(), true);
                    DataType::List(Arc::new(item))
                }
            };
            (name.as_str(), data_type)
        })
        .collect();
    let theirs: Vec<(&str, DataType)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type().clone()))
        .collect();
    if ours != theirs {
        return Some(format!("columns {ours:?}, Java has {theirs:?}"));
    }
    if rows.size() != expected.num_rows() as u64 {
        let (ours, theirs) = (rows.size(), expected.num_rows());
        return Some(format!("{ours} rows, Java has {theirs}"));
    }
    for (index, (name, column)) in joined.columns.iter().enumerate() {
        let found = match column {
            Column::Left(left) => joined.left.column(*left).chunk(&rows),
            Column::Group(group) => {
                let field = schema.field(index);
                let (class, component) = vector_type(group.element_type());
                let java = |key: &str| field.metadata().get(key).map(String::as_str);
                if java("deephaven:type") != Some(class)
                    || java("deephaven:componentType") != Some(component)
                {
                    let metadata = field.metadata();
                    return Some(format!("column {name} is a {class}, Java has {metadata:?}"));
                }
                group.chunk(&rows)
            }
        };
        let found = match found {
            Ok(found) => found,
            Err(error) => return Some(format!("column {name} cannot be read: {error}")),
        };
        if !same_cells(expected.column(index), &found) {
            let theirs = expected.column(index);
            return Some(format!("column {name} is {found:?}, Java has {theirs:?}"));
        }
    }
    None
}

fn recorded_table(directory: &Path, entry: &Value) -> RecordBatch {
    let path = directory.join(text(&entry["file"]));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let reader = FileReader::try_new(file, None).expect("an Arrow IPC file");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("a batch")).collect();
    arrow::compute::concat_batches(&schema, &batches).expect("one schema")
}

/// What was compared in a set of scenarios.
#[derive(Default)]
struct Tally {
    /// Results Java has a table for, that agree.
    tables: usize,
    /// Results Java refused, refused here.
    refused: usize,
    /// `scenario/table: how`, for every result that differs.
    differences: Vec<String>,
    /// `scenario/table`, for the results that were not built.
    not_run: Vec<String>,
}

/// Compares every result of the static scenario at `path` with its recording in
/// `corpus/<name>`.
fn run(path: &Path, corpus: &Path, tally: &mut Tally) {
    let scenario = json(path);
    let name = text(&scenario["name"]);
    assert!(
        list(&scenario["steps"]).is_empty(),
        "{name}: a static scenario"
    );
    let directory = corpus.join(name);
    let manifest = json(&directory.join("manifest.json"));

    let inputs: BTreeMap<&str, Arc<dyn Table>> = list(&scenario["inputs"])
        .iter()
        .map(|input| (text(&input["name"]), load(input)))
        .collect();
    let results: Vec<&str> = list(&scenario["results"])
        .iter()
        .map(|result| text(&result["table"]))
        .collect();
    for operation in list(&scenario["operations"]) {
        let table = text(&operation["name"]);
        assert_eq!(text(&operation["op"]), "range_join", "{name}/{table}");
        assert!(results.contains(&table), "{name}/{table} is a result");
        let place = format!("{name}/{table}");
        let sides = (
            inputs.get(text(&operation["left"])),
            inputs.get(text(&operation["right"])),
        );
        let (Some(left), Some(right)) = sides else {
            // The result of a range join is not a table yet.
            tally.not_run.push(place);
            continue;
        };
        let entries = list(&manifest["entries"]);
        let entry = entries
            .iter()
            .find(|entry| entry["step"] == 0 && entry["table"] == table)
            .unwrap_or_else(|| panic!("no recording of {place}"));
        let failure = &entry["failure"];
        let difference = match joined(operation, left, right, name == BY_FIELDS) {
            Err(error) if failure.is_null() => Some(format!("refused ({error}), Java has a table")),
            // Every refusal of a range join is `INVALID_ARGUMENT`.
            Err(_) => (failure["code"] != "INVALID_ARGUMENT")
                .then(|| format!("refused, Java fails with {}", failure["code"])),
            Ok(_) if !failure.is_null() => Some(format!("Java fails with {}", failure["code"])),
            Ok(joined) => difference(&joined, &recorded_table(&directory, entry)),
        };
        match difference {
            Some(difference) => tally.differences.push(format!("{place}: {difference}")),
            None if failure.is_null() => tally.tables += 1,
            None => tally.refused += 1,
        }
    }
}

#[test]
fn every_refusal_of_the_oracle_scenarios_is_a_refusal_here() {
    let tally = replay_all(&["range_join"], &|operation, parents| match joined(
        operation,
        &parents[0],
        &parents[1],
        false,
    ) {
        Err(_) => Built::Refused("INVALID_ARGUMENT"),
        Ok(_) => Built::NotRun("array-valued columns".to_string()),
    });
    assert_eq!(tally.differences, Vec::<String>::new());
    let refused = [
        "range_join/bad_match",
        "range_join/count_agg",
        "range_join/no_exact",
        "range_join/other_types",
        "range_join/strict_neighbours",
        "range_join/sum_agg",
        "range_join/unknown_column",
        "range_join/unsorted_right",
        "ticking_range_join/ticking_both",
        "ticking_range_join/ticking_left",
        "ticking_range_join/ticking_right",
    ];
    assert_eq!(tally.compared, refused);
    // The eight static refusals once, the three ticking ones at each of four steps.
    assert_eq!(tally.comparisons, 8 + 3 * 4);
    let not_run: Vec<&str> = tally
        .not_run
        .iter()
        .map(|result| result.split(':').next().expect("a name"))
        .collect();
    let tables = [
        "range_join/inclusive",
        "range_join/strict",
        "range_join/half_open",
        "range_join/other_half",
        "range_join/with_neighbours",
        "range_join/with_preceding",
        "range_join/no_exact_ordered",
        "range_join/no_exact_neighbours",
        "range_join/empty_right",
    ];
    assert_eq!(not_run, tables);
}

#[test]
fn the_ranges_of_the_oracle_scenario_are_those_of_java() {
    let mut tally = Tally::default();
    let scenario = oracle().join("scenarios/static/range_join.json");
    run(&scenario, &oracle().join("corpus"), &mut tally);
    assert_eq!(tally.differences, Vec::<String>::new());
    assert_eq!(tally.not_run, Vec::<String>::new());
    assert_eq!((tally.tables, tally.refused), (9, 8));
}

#[test]
fn the_ranges_of_the_recorded_scenarios_are_those_of_java() {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(testdata().join("scenarios")).expect("the scenarios") {
        paths.push(entry.expect("a scenario").path());
    }
    paths.sort();
    assert_eq!(paths.len(), 7, "the scenarios of this directory");
    let mut tally = Tally::default();
    for path in &paths {
        run(path, &testdata().join("corpus"), &mut tally);
    }
    assert_eq!(tally.differences, Vec::<String>::new());
    // Its left table is the result of a range join.
    assert_eq!(tally.not_run, ["range_join_refusals/of_a_range_join"]);
    assert_eq!((tally.tables, tally.refused), (79, 37));
}
