//! `update_by` against the results recorded from the Java reference server in
//! `fixtures/java_update_by.json` (written by `fixtures/record_java_update_by.py`).
//!
//! Values are compared exactly, floating point cells bit for bit (any NaN equals any
//! NaN). The tolerance of PARITY.md's allowed difference A1 is not used: the operators
//! add and multiply in Java's order.

use super::values::Values;
use super::{
    update_by, DeltaControl, RollingWindow, UpdateByError, UpdateByOperation, UpdateBySpec,
};
use crate::graph::SteppedUpdateGraph;
use crate::table::keyed_input_table;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
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
    RollingWindow::ticks(op["rev"].as_i64().unwrap(), op["fwd"].as_i64().unwrap())
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
    update_by(source, &operations, &strings(by))
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

/// What differs between the recorded `columns` and `table`, if anything.
fn differences(table: &dyn Table, columns: &Value) -> Vec<String> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let mut out = Vec::new();
    for column in columns.as_array().unwrap() {
        let name = column["name"].as_str().unwrap();
        let Some(index) = table.definition().index_of(name) else {
            out.push(format!("{name}: missing"));
            continue;
        };
        let ours = &table.definition().columns()[index].column_type;
        let java = column_type(column["java"].as_str().unwrap());
        if *ours != java {
            out.push(format!("{name}: type {ours:?}, Java has {java:?}"));
            continue;
        }
        let expected: Vec<&Value> = column["values"].as_array().unwrap().iter().collect();
        let expected = Values::read(&array(&java, &expected), &java).unwrap();
        let actual = Values::read(&snapshot.columns[index], &java).unwrap();
        let same = match (&expected, &actual) {
            (Values::Int(a), Values::Int(b)) => a == b,
            (Values::Text(a), Values::Text(b)) => a == b,
            (Values::Float(a), Values::Float(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| floats_match(*a, *b))
            }
            _ => false,
        };
        if !same {
            out.push(format!(
                "{name}:\n    java {}\n    ours {}",
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

#[test]
fn static_results_match_java() {
    let recorded: Value = serde_json::from_str(RECORDED).unwrap();
    let columns = &recorded["static"]["columns"];
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

    let cases = recorded["static"]["cases"].as_array().unwrap();
    assert!(cases.len() > 500, "the recording is incomplete");
    let mut failures = Vec::new();
    let mut compared = 0;
    for case in cases {
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
                for difference in differences(&*table, &result["columns"]) {
                    failures.push(format!("{label}: {difference}"));
                }
            }
        }
    }
    assert!(
        compared > 300,
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
    let recorded: Value = serde_json::from_str(RECORDED).unwrap();
    let scenarios = recorded["ticking"].as_array().unwrap();
    assert_eq!(scenarios.len(), 3);
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
            checker.assert_consistent();
            // The source rows first: if they differ, the input table is at fault.
            let rows = differences(&*result, &step["rows"]);
            assert!(
                rows.is_empty(),
                "{name} step {number}: source rows differ: {rows:?}"
            );
            for difference in differences(&*result, &step["result"]) {
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
