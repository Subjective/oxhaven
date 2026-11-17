//! `update_by` against the results recorded from the Java reference server in
//! `fixtures/java_update_by.json` (written by `fixtures/record_java_update_by.py`),
//! `fixtures/java_update_by_rest.json` (`fixtures/record_java_update_by_rest.py`),
//! `fixtures/java_update_by_group.json` (`fixtures/record_java_update_by_group.py`),
//! `fixtures/java_update_by_count_where.json`
//! (`fixtures/record_java_update_by_count_where.py`) and
//! `fixtures/java_update_by_formula.json` (`fixtures/record_java_update_by_formula.py`).
//!
//! Values are compared exactly, floating point cells bit for bit (any NaN equals any
//! NaN). The tolerance of PARITY.md's allowed difference A1 is not used: the operators
//! add and multiply in Java's order. The one exception is the exponential moving standard
//! deviation of a ticking table, see [`ticking_scenarios`].

use super::values::Values;
use super::{
    update_by_with, BadDataBehavior, DeltaControl, EmSpec, RollingWindow, UpdateByError,
    UpdateByOperation, UpdateBySpec, WindowScale,
};
use crate::graph::SteppedUpdateGraph;
use crate::table::keyed_input_table;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, ListArray, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{ConsistencyChecker, ReferenceTable, TableSnapshot};
use serde_json::Value;
use std::sync::Arc;

const RECORDED: &str = include_str!("fixtures/java_update_by.json");
/// The rolling standard deviation and weighted average and the exponential moving standard
/// deviation.
const RECORDED_REST: &str = include_str!("fixtures/java_update_by_rest.json");
/// The rolling group.
const RECORDED_GROUP: &str = include_str!("fixtures/java_update_by_group.json");
/// How many cases of each of its sections Java answers.
const GROUP_STATIC: usize = 181;
const GROUP_LONG: usize = 28;
const GROUP_TIMED: usize = 1;
const GROUP_STAMPED: usize = 201;
const GROUP_BURST: usize = 156;
/// The count-where operators.
const RECORDED_COUNT_WHERE: &str = include_str!("fixtures/java_update_by_count_where.json");
/// The rolling formula.
const RECORDED_FORMULA: &str = include_str!("fixtures/java_update_by_formula.json");

fn column_type(name: &str) -> ColumnType {
    match name {
        "byte" => ColumnType::Byte,
        "short" => ColumnType::Short,
        "char" => ColumnType::Char,
        "int" => ColumnType::Int,
        "long" => ColumnType::Long,
        "float" => ColumnType::Float,
        "double" => ColumnType::Double,
        "string" | "java.lang.String" => ColumnType::String,
        "boolean" | "java.lang.Boolean" => ColumnType::Boolean,
        "instant" | "java.time.Instant" => ColumnType::Instant,
        other => panic!("no column type for {other}"),
    }
}

/// The type of a recorded result column: `java` is the class Java reports for it, and
/// `component` the element type of a vector.
fn java_type(column: &Value) -> ColumnType {
    let java = column["java"].as_str().unwrap();
    if let Some(element) = java.strip_suffix("[]") {
        return ColumnType::array(column_type(element));
    }
    let Some(vector) = java.strip_prefix("io.deephaven.vector.") else {
        return column_type(java);
    };
    let component = column["component"].as_str().unwrap();
    let element = column_type(component);
    let primitive = vector
        .strip_suffix("Vector")
        .is_some_and(|name| name.eq_ignore_ascii_case(component));
    assert!(
        primitive || vector == "ObjectVector",
        "{java} of {component} is no vector type"
    );
    ColumnType::vector(element)
}

/// The cells of a recorded vector column or of ours, each null or its elements.
fn cells(array: &ArrayRef, element: &ColumnType) -> Vec<Option<Values>> {
    let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
    lists
        .iter()
        .map(|cell| cell.map(|cell| Values::read(&cell, element).unwrap()))
        .collect()
}

fn float(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(match text.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            "-0.0" => -0.0,
            other => panic!("not a float: {other}"),
        }),
        other => Some(other.as_f64().expect("a number")),
    }
}

fn array(column_type: &ColumnType, values: &[&Value]) -> ArrayRef {
    let ints = || values.iter().map(|v| v.as_i64());
    match column_type {
        ColumnType::Byte => Arc::new(ints().map(|v| v.map(|v| v as i8)).collect::<Int8Array>()),
        ColumnType::Short => Arc::new(ints().map(|v| v.map(|v| v as i16)).collect::<Int16Array>()),
        ColumnType::Char => Arc::new(ints().map(|v| v.map(|v| v as u16)).collect::<UInt16Array>()),
        ColumnType::Int => Arc::new(ints().map(|v| v.map(|v| v as i32)).collect::<Int32Array>()),
        ColumnType::Long => Arc::new(ints().collect::<Int64Array>()),
        ColumnType::Instant => Arc::new(
            ints()
                .collect::<TimestampNanosecondArray>()
                .with_timezone("UTC"),
        ),
        ColumnType::Float => Arc::new(
            values
                .iter()
                .map(|v| float(v).map(|v| v as f32))
                .collect::<Float32Array>(),
        ),
        ColumnType::Double => Arc::new(values.iter().map(|v| float(v)).collect::<Float64Array>()),
        ColumnType::String => Arc::new(values.iter().map(|v| v.as_str()).collect::<StringArray>()),
        ColumnType::Boolean => {
            Arc::new(values.iter().map(|v| v.as_bool()).collect::<BooleanArray>())
        }
        other => panic!("no values for {other:?}"),
    }
}

fn window(op: &Value) -> RollingWindow {
    let (rev, fwd) = (op["rev"].as_i64().unwrap(), op["fwd"].as_i64().unwrap());
    match op.get("ts").and_then(Value::as_str) {
        None => RollingWindow::ticks(rev, fwd),
        Some(column) => RollingWindow {
            rev: WindowScale::Time {
                column: column.into(),
                nanos: rev,
            },
            fwd: WindowScale::Time {
                column: column.into(),
                nanos: fwd,
            },
        },
    }
}

fn behavior(op: &Value, key: &str) -> Option<BadDataBehavior> {
    op.get(key).and_then(Value::as_str).map(|b| match b {
        "skip" => BadDataBehavior::Skip,
        "reset" => BadDataBehavior::Reset,
        "poison" => BadDataBehavior::Poison,
        "throw" => BadDataBehavior::Throw,
        other => panic!("no bad data behavior {other}"),
    })
}

fn em(op: &Value) -> EmSpec {
    let decay = op["decay"].as_f64().unwrap();
    EmSpec {
        window: match op.get("ts").and_then(Value::as_str) {
            None => WindowScale::Ticks(decay),
            Some(column) => WindowScale::Time {
                column: column.into(),
                nanos: op["decay"].as_i64().unwrap(),
            },
        },
        on_null: behavior(op, "on_null"),
        on_nan: behavior(op, "on_nan"),
    }
}

fn operation(op: &Value) -> UpdateByOperation {
    let spec = match op["op"].as_str().unwrap() {
        "cum_sum" => UpdateBySpec::CumSum,
        "cum_prod" => UpdateBySpec::CumProd,
        "cum_min" => UpdateBySpec::CumMin,
        "cum_max" => UpdateBySpec::CumMax,
        "forward_fill" => UpdateBySpec::ForwardFill,
        "delta" => UpdateBySpec::Delta(match op.get("control").and_then(Value::as_str) {
            None | Some("null") => DeltaControl::NullDominates,
            Some("value") => DeltaControl::ValueDominates,
            Some("zero") => DeltaControl::ZeroDominates,
            Some(other) => panic!("no delta control {other}"),
        }),
        "rolling_sum" => UpdateBySpec::RollingSum(window(op)),
        "rolling_avg" => UpdateBySpec::RollingAvg(window(op)),
        "rolling_min" => UpdateBySpec::RollingMin(window(op)),
        "rolling_max" => UpdateBySpec::RollingMax(window(op)),
        "rolling_count" => UpdateBySpec::RollingCount(window(op)),
        "rolling_prod" => UpdateBySpec::RollingProduct(window(op)),
        "rolling_std" => UpdateBySpec::RollingStd(window(op)),
        "rolling_group" => UpdateBySpec::RollingGroup(window(op)),
        "rolling_wavg" => UpdateBySpec::RollingWAvg {
            window: window(op),
            weight_column: op["weight"].as_str().unwrap().into(),
        },
        "emstd" => UpdateBySpec::EmStd(em(op)),
        "ema" => UpdateBySpec::Ema(em(op)),
        "ems" => UpdateBySpec::Ems(em(op)),
        "emmin" => UpdateBySpec::EmMin(em(op)),
        "emmax" => UpdateBySpec::EmMax(em(op)),
        "cum_count_where" => UpdateBySpec::CumCountWhere {
            result_column: op["result"].as_str().unwrap().into(),
            filters: strings(&op["filters"])
                .into_iter()
                .map(String::from)
                .collect(),
        },
        "rolling_count_where" => UpdateBySpec::RollingCountWhere {
            window: window(op),
            result_column: op["result"].as_str().unwrap().into(),
            filters: strings(&op["filters"])
                .into_iter()
                .map(String::from)
                .collect(),
        },
        "rolling_formula" => UpdateBySpec::RollingFormula {
            window: window(op),
            formula: op["formula"].as_str().unwrap().into(),
            param_token: op["param"].as_str().map(String::from),
        },
        other => panic!("no operation {other}"),
    };
    let columns: Vec<&str> = op["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c.as_str().unwrap())
        .collect();
    UpdateByOperation::new(spec, &columns)
}

fn strings(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect()
}

fn run(source: &Arc<dyn Table>, ops: &Value, by: &Value) -> Result<Arc<dyn Table>, UpdateByError> {
    let operations: Vec<UpdateByOperation> =
        ops.as_array().unwrap().iter().map(operation).collect();
    update_by_with(source, &operations, &strings(by), &oxhaven_expr::Compiler)
}

/// PARITY.md, Must match item 5: bit for bit, except that every NaN is the same NaN.
/// Allowed difference A1 is not needed: every recorded value is reproduced exactly.
fn floats_match(java: Option<f64>, ours: Option<f64>) -> bool {
    match (java, ours) {
        (None, None) => true,
        (Some(a), Some(b)) if a.is_nan() || b.is_nan() => a.is_nan() && b.is_nan(),
        (Some(a), Some(b)) => a.to_bits() == b.to_bits(),
        _ => false,
    }
}

/// PARITY.md, allowed difference A1 for a `double`; nulls, NaN and the infinities match
/// only themselves.
fn floats_close(java: Option<f64>, ours: Option<f64>) -> bool {
    match (java, ours) {
        (Some(a), Some(b)) if a.is_finite() && b.is_finite() => {
            (a - b).abs() <= (1e-9 * a.abs().max(b.abs())).max(1e-12)
        }
        _ => floats_match(java, ours),
    }
}

/// What differs between the recorded `columns` and `table`, if anything.
fn differences(table: &dyn Table, columns: &Value) -> Vec<String> {
    differences_within(table, columns, &[])
}

/// As [`differences`], with the columns named in `tolerant` compared under A1.
fn differences_within(table: &dyn Table, columns: &Value, tolerant: &[&str]) -> Vec<String> {
    let floats_match = |name: &str, java, ours| {
        if tolerant.contains(&name) {
            floats_close(java, ours)
        } else {
            floats_match(java, ours)
        }
    };
    let snapshot = TableSnapshot::current(table).unwrap();
    let mut out = Vec::new();
    for column in columns.as_array().unwrap() {
        let name = column["name"].as_str().unwrap();
        let Some(index) = table.definition().index_of(name) else {
            out.push(format!("{name}: missing"));
            continue;
        };
        let ours = &table.definition().columns()[index].column_type;
        let java = java_type(column);
        if *ours != java {
            out.push(format!("{name}: type {ours:?}, Java has {java:?}"));
            continue;
        }
        let same = |expected: &Values, actual: &Values| match (expected, actual) {
            (Values::Int(a), Values::Int(b)) => a == b,
            (Values::Text(a), Values::Text(b)) => a == b,
            (Values::Float(a), Values::Float(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| floats_match(name, *a, *b))
            }
            _ => false,
        };
        let expected: Vec<&Value> = column["values"].as_array().unwrap().iter().collect();
        if let Some(element) = java.list_element() {
            let expected: Vec<Option<Values>> = expected
                .iter()
                .map(|cell| {
                    let cell: Vec<&Value> = cell.as_array()?.iter().collect();
                    Some(Values::read(&array(element, &cell), element).unwrap())
                })
                .collect();
            let actual = cells(&snapshot.columns[index], element);
            if expected.len() != actual.len() {
                out.push(format!(
                    "{name}: {} rows, Java has {}",
                    actual.len(),
                    expected.len()
                ));
                continue;
            }
            let show = |cell: &Option<Values>| cell.as_ref().map_or("null".to_owned(), show);
            for (row, (expected, actual)) in expected.iter().zip(&actual).enumerate() {
                let equal = match (expected, actual) {
                    (None, None) => true,
                    (Some(expected), Some(actual)) => same(expected, actual),
                    _ => false,
                };
                if !equal {
                    out.push(format!(
                        "{name} row {row}:\n    java {}\n    ours {}",
                        show(expected),
                        show(actual)
                    ));
                }
            }
            continue;
        }
        let expected = Values::read(&array(&java, &expected), &java).unwrap();
        let actual = Values::read(&snapshot.columns[index], &java).unwrap();
        if !same(&expected, &actual) {
            let first = match (&expected, &actual) {
                (Values::Float(a), Values::Float(b)) => (0..a.len().min(b.len()))
                    .find(|&row| !floats_match(name, a[row], b[row]))
                    .map(|row| format!(" (row {row}: java {:?}, ours {:?})", a[row], b[row])),
                _ => None,
            };
            out.push(format!(
                "{name}{}:\n    java {}\n    ours {}",
                first.unwrap_or_default(),
                show(&expected),
                show(&actual)
            ));
        }
    }
    out
}

fn show(values: &Values) -> String {
    match values {
        Values::Int(v) => format!("{v:?}"),
        Values::Float(v) => format!("{v:?}"),
        Values::Text(v) => format!("{v:?}"),
    }
}

fn definition(columns: &Value) -> TableDefinition {
    TableDefinition::new(
        columns
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                ColumnDefinition::new(
                    c["name"].as_str().unwrap(),
                    column_type(c["type"].as_str().unwrap()),
                )
            })
            .collect(),
    )
    .unwrap()
}

/// Runs every case of one recorded table, `section`, which holds at least `at_least` cases
/// that Java answers.
fn static_section(recorded: &str, section: &str, at_least: usize) {
    static_section_within(recorded, section, at_least, |_| &[]);
}

/// Whether a case of the section `unported` reads a window by position, which is now
/// ported.
fn reads_by_position(case: &Value) -> bool {
    case["ops"].as_array().unwrap().iter().any(|op| {
        let formula = op["formula"].as_str().unwrap_or("");
        ["get(", "[", "diff("]
            .iter()
            .any(|read| formula.contains(read))
    })
}

/// As [`static_section`], with the result columns that `tolerant` names for a case compared
/// under PARITY.md's allowed difference A1.
fn static_section_within(
    recorded: &str,
    section: &str,
    at_least: usize,
    tolerant: fn(&Value) -> &'static [&'static str],
) {
    static_cases_within(recorded, section, at_least, tolerant, |_| true);
}

/// As [`static_section_within`], for the cases of the section that `keep` keeps.
fn static_cases_within(
    recorded: &str,
    section: &str,
    at_least: usize,
    tolerant: fn(&Value) -> &'static [&'static str],
    keep: fn(&Value) -> bool,
) {
    let recorded: Value = serde_json::from_str(recorded).unwrap();
    let columns = &recorded[section]["columns"];
    let definition = definition(columns);
    let values: Vec<ArrayRef> = columns
        .as_array()
        .unwrap()
        .iter()
        .zip(definition.columns())
        .map(|(c, d)| {
            let values: Vec<&Value> = c["values"].as_array().unwrap().iter().collect();
            array(&d.column_type, &values)
        })
        .collect();
    let size = values[0].len() as u64;
    let source: Arc<dyn Table> = ReferenceTable::new_static(definition, RowSet::flat(size), values);

    let cases: Vec<&Value> = recorded[section]["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| keep(case))
        .collect();
    let mut failures = Vec::new();
    let mut compared = 0;
    for case in &cases {
        let label = format!("{} by {}", case["ops"], case["by"]);
        let ours = run(&source, &case["ops"], &case["by"]);
        match (&case["result"], ours) {
            (Value::Null, Err(_)) => {}
            (Value::Null, Ok(_)) => {
                failures.push(format!("{label}: Java refuses, Oxhaven answers"))
            }
            (_, Err(e)) => failures.push(format!("{label}: Java answers, Oxhaven refuses: {e}")),
            (result, Ok(table)) => {
                compared += 1;
                let names: Vec<&str> = table
                    .definition()
                    .columns()
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect();
                if names != strings(&result["names"]) {
                    failures.push(format!(
                        "{label}: columns {names:?}, Java has {}",
                        result["names"]
                    ));
                }
                let columns = &result["columns"];
                for difference in differences_within(&*table, columns, tolerant(case)) {
                    failures.push(format!("{label}: {difference}"));
                }
            }
        }
    }
    assert!(
        compared >= at_least,
        "too few cases that Java answers: {compared}"
    );
    assert!(
        failures.is_empty(),
        "{} of {} cases differ from Java:\n{}",
        failures.len(),
        cases.len(),
        failures.join("\n")
    );
}

#[test]
fn static_results_match_java() {
    static_section(RECORDED, "static", 800);
}

/// Sums and products that round, over windows up to and beyond the 64 slots the operators
/// start with: the results are Java's only if they are taken in Java's order.
#[test]
fn rounded_results_match_java() {
    static_section(RECORDED, "long", 170);
}

/// Rolling operators over windows in time, and the exponential moving operators decaying by
/// ticks and by time, with every way of treating null and NaN values.
#[test]
fn timed_results_match_java() {
    static_section(RECORDED, "timed", 1135);
}

#[test]
fn static_results_of_the_rest_match_java() {
    static_section(RECORDED_REST, "static", 284);
}

/// Sums of values, squares and products that round, over windows up to and beyond the 128
/// slots the standard deviation and the weighted average start with.
#[test]
fn rounded_results_of_the_rest_match_java() {
    static_section(RECORDED_REST, "long", 310);
}

#[test]
fn static_results_of_the_group_match_java() {
    static_section(RECORDED_GROUP, "static", GROUP_STATIC);
}

#[test]
fn long_results_of_the_group_match_java() {
    static_section(RECORDED_GROUP, "long", GROUP_LONG);
}

/// The table has rows without a timestamp before rows with one, and Java refuses every
/// rolling group in time over it but one: with the buckets of `K` and the timestamp
/// column `TB`, which has no nulls.
#[test]
fn timed_results_of_the_group_match_java() {
    static_section(RECORDED_GROUP, "timed", GROUP_TIMED);
}

/// One case has an exponential moving average in time beside the groups, decaying over 5
/// nanoseconds. One of its cells is one unit in the last place from Java's, which is the
/// difference PARITY.md A1 allows for a result computed with `exp`; that column is
/// compared under A1 and everything else bit for bit.
#[test]
fn timed_results_of_the_group_with_every_timestamp_match_java() {
    static_section_within(RECORDED_GROUP, "stamped", GROUP_STAMPED, |case| {
        let ops = case["ops"].as_array().unwrap();
        if ops.iter().any(|op| op["op"] == "ema") {
            &["EA"]
        } else {
            &[]
        }
    });
}

/// Windows in time that more than 64 and more than 128 rows enter at once, after the
/// window has moved along single rows: the ring buffers of the rolling operators grow, and
/// where their values then stand depends on the capacity they started with.
///
/// Without buckets every result is Java's bit for bit. With buckets the floating point
/// results are compared under PARITY.md's allowed difference A1, because Java's are not
/// determined by the table: an operator's buffer belongs to the thread that computes a
/// bucket, is cleared between buckets and keeps the capacity it grew to
/// (io.deephaven.engine.table.impl.updateby.UpdateBy.PhasedUpdateProcessor#processWindowOperatorSet,
/// io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer#clear), so the order in which a
/// bucket's values are multiplied depends on the buckets its thread computed before. One
/// recorded product of a `float` column differs from the one here in its last bits.
#[test]
fn results_over_bursts_of_rows_match_java() {
    static_section_within(RECORDED_GROUP, "burst", GROUP_BURST, |case| {
        if case["by"].as_array().unwrap().is_empty() {
            &[]
        } else {
            &["R"]
        }
    });
}

#[test]
fn static_results_of_the_count_where_match_java() {
    static_section(RECORDED_COUNT_WHERE, "static", 231);
}

#[test]
fn long_results_of_the_count_where_match_java() {
    static_section(RECORDED_COUNT_WHERE, "long", 64);
}

/// Count-where over windows in time, in rows and cumulative, over a table with rows that
/// have no timestamp.
#[test]
fn timed_results_of_the_count_where_match_java() {
    static_section(RECORDED_COUNT_WHERE, "timed", 250);
}

#[test]
fn timed_results_of_the_count_where_with_every_timestamp_match_java() {
    static_section(RECORDED_COUNT_WHERE, "stamped", 250);
}

/// The scenario `timed_bucketed` ends when Java's table fails: its filters read the
/// timestamp column, an `Instant`, and an update computes rows whose windows no row
/// enters (see `CountWhere::rolling`).
#[test]
fn ticking_results_of_the_count_where_match_java() {
    ticking_scenarios(RECORDED_COUNT_WHERE, 5);
    let recorded: Value = serde_json::from_str(RECORDED_COUNT_WHERE).unwrap();
    let failed: Vec<&str> = recorded["ticking"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|scenario| scenario["steps"].as_array().unwrap().last().unwrap()["failed"] == true)
        .map(|scenario| scenario["name"].as_str().unwrap())
        .collect();
    assert_eq!(failed, ["timed_bucketed"]);
}

#[test]
fn static_results_of_the_formula_match_java() {
    static_section(RECORDED_FORMULA, "static", 400);
}

#[test]
fn long_results_of_the_formula_match_java() {
    static_section(RECORDED_FORMULA, "long", 50);
}

/// The rolling formula over windows in time and in rows, over a table with rows that have
/// no timestamp.
#[test]
fn timed_results_of_the_formula_match_java() {
    static_section(RECORDED_FORMULA, "timed", 150);
}

#[test]
fn timed_results_of_the_formula_with_every_timestamp_match_java() {
    static_section(RECORDED_FORMULA, "stamped", 150);
}

#[test]
fn ticking_results_of_the_formula_match_java() {
    ticking_scenarios(RECORDED_FORMULA, 5);
}

/// The formulas of the section `unported` that read a window by position (an index,
/// `get`, `diff`), which were refused before they were ported. A slice as the result
/// stays refused, and `nth` of a vector is refused by the expression crate.
#[test]
fn reads_of_a_window_by_position_match_java() {
    static_cases_within(RECORDED_FORMULA, "unported", 10, |_| &[], reads_by_position);
}

/// The section `unported` holds formulas that Java answers, or fails for some row, and
/// that are refused here: they call a function the expression crate refuses by name.
/// When one of them is no longer refused, it belongs in the section `static`; those that
/// read a window by position are compared by `reads_of_a_window_by_position_match_java`.
#[test]
fn formulas_that_are_not_ported_are_refused() {
    let recorded: Value = serde_json::from_str(RECORDED_FORMULA).unwrap();
    let columns = &recorded["unported"]["columns"];
    let definition = definition(columns);
    let values: Vec<ArrayRef> = columns
        .as_array()
        .unwrap()
        .iter()
        .zip(definition.columns())
        .map(|(c, d)| {
            let values: Vec<&Value> = c["values"].as_array().unwrap().iter().collect();
            array(&d.column_type, &values)
        })
        .collect();
    let size = values[0].len() as u64;
    let source: Arc<dyn Table> = ReferenceTable::new_static(definition, RowSet::flat(size), values);
    let cases = recorded["unported"]["cases"].as_array().unwrap();
    assert!(cases.len() >= 40);
    let answered: Vec<String> = cases
        .iter()
        .filter(|case| !reads_by_position(case))
        .filter(|case| {
            let run = std::panic::AssertUnwindSafe(|| run(&source, &case["ops"], &case["by"]));
            matches!(std::panic::catch_unwind(run), Ok(Ok(_)))
        })
        .map(|case| format!("{} by {}", case["ops"], case["by"]))
        .collect();
    assert!(answered.is_empty(), "answered:\n{}", answered.join("\n"));
}

#[test]
fn timed_results_of_the_rest_match_java() {
    static_section(RECORDED_REST, "timed", 412);
}

fn batch(columns: &Value, rows: &Value) -> RecordBatch {
    let definition = definition(columns);
    let rows = rows.as_array().unwrap();
    let arrays: Vec<ArrayRef> = definition
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let values: Vec<&Value> = rows.iter().map(|row| &row[index]).collect();
            array(&column.column_type, &values)
        })
        .collect();
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|c| Field::new(&c.name, c.column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// A static copy of what `table` holds now.
fn frozen(table: &dyn Table) -> Arc<dyn Table> {
    let snapshot = TableSnapshot::current(table).unwrap();
    ReferenceTable::new_static(
        table.definition().clone(),
        RowSet::flat(snapshot.rows.size()),
        snapshot.columns,
    )
}

#[test]
fn ticking_results_match_java() {
    ticking_scenarios(RECORDED, 5);
}

#[test]
fn ticking_results_of_the_rest_match_java() {
    ticking_scenarios(RECORDED_REST, 4);
}

/// The scenarios `timed` and `timed_bucketed` have rows without a timestamp, and end
/// when the rolling groups in time fail on one; in `stamped` and `stamped_bucketed` every
/// row has a timestamp.
#[test]
fn ticking_results_of_the_group_match_java() {
    ticking_scenarios(RECORDED_GROUP, 6);
    let recorded: Value = serde_json::from_str(RECORDED_GROUP).unwrap();
    let failed: Vec<&str> = recorded["ticking"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|scenario| scenario["steps"].as_array().unwrap().last().unwrap()["failed"] == true)
        .map(|scenario| scenario["name"].as_str().unwrap())
        .collect();
    assert_eq!(failed, ["timed", "timed_bucketed"]);
}

/// Replays the `count` ticking scenarios of `recorded` with the consistency checker and a
/// recompute from the static path, and compares the result columns after every step.
///
/// The results of an exponential moving standard deviation are compared under PARITY.md's
/// allowed difference A1, and nothing else is: after an update Java restarts from the
/// square of a result where it had a variance (see `Em::accumulate_std`), so what it holds
/// differs in the last bits from what it computes from scratch, and from the results here.
fn ticking_scenarios(recorded: &str, count: usize) {
    let recorded: Value = serde_json::from_str(recorded).unwrap();
    let scenarios = recorded["ticking"].as_array().unwrap();
    assert_eq!(scenarios.len(), count);
    let mut failures = Vec::new();
    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
        let columns = &scenario["columns"];
        let keys = strings(&scenario["key_columns"]);
        let source: Arc<dyn Table> =
            keyed_input_table(graph.clone(), definition(columns), &keys, None).unwrap();
        let input = source.input_table().unwrap();
        let (ops, by) = (scenario["ops"].clone(), scenario["by"].clone());
        let tolerant: Vec<String> = ops
            .as_array()
            .unwrap()
            .iter()
            .filter(|op| op["op"] == "emstd")
            .flat_map(|op| strings(&op["columns"]))
            .map(|pair| pair.split('=').next().unwrap().trim().to_owned())
            .collect();
        let tolerant: Vec<&str> = tolerant.iter().map(String::as_str).collect();
        let result = run(&source, &ops, &by).unwrap();
        let checker = {
            let source = source.clone();
            ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                run(&frozen(&*source), &ops, &by).unwrap()
            })
        };
        let key_columns: Value = columns
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| keys.contains(&c["name"].as_str().unwrap()))
            .cloned()
            .collect();
        for (number, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
            if !step["add"].as_array().unwrap().is_empty() {
                input.add(batch(columns, &step["add"])).unwrap();
            }
            if !step["delete"].as_array().unwrap().is_empty() {
                input.delete(batch(&key_columns, &step["delete"])).unwrap();
            }
            if step["failed"] == true {
                // Java's table failed in this step, and the recording ends.
                if !result.is_failed() {
                    failures.push(format!("{name} step {number}: Java fails, Oxhaven answers"));
                }
                break;
            }
            assert!(
                !result.is_failed(),
                "{name} step {number}: the result failed"
            );
            checker.assert_consistent();
            // The source rows first: if they differ, the input table is at fault.
            let rows = differences(&*result, &step["rows"]);
            assert!(
                rows.is_empty(),
                "{name} step {number}: source rows differ: {rows:?}"
            );
            for difference in differences_within(&*result, &step["result"], &tolerant) {
                failures.push(format!("{name} step {number}: {difference}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} columns differ from Java:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
