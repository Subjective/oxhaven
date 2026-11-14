//! Formulas of the oracle's scenarios, checked against the results recorded from the Java
//! server (`oracle/corpus`, see `oracle/README.md`).
//!
//! Every formula of a `view` or `update` operation is evaluated over the scenario's input
//! table and compared with the recorded column of the same name. Every `where` operation is
//! evaluated as the conjunction of its filters and compared with the recorded rows.

use super::*;
use arrow::array::RecordBatch;
use arrow::ipc::reader::FileReader;
use serde_json::Value;
use std::fs::File;
use std::path::PathBuf;

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn scenario(name: &str) -> Value {
    let path = oracle().join(format!("scenarios/static/{name}.json"));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The table Java returned for `table` of scenario `name`.
fn recorded(name: &str, table: &str) -> RecordBatch {
    let path = oracle().join(format!("corpus/{name}/step000.{table}.arrow"));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let reader = FileReader::try_new(file, None).expect("an Arrow IPC file");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("a batch")).collect();
    arrow::compute::concat_batches(&schema, &batches).expect("batches of one schema")
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

/// The input table `name` of a scenario.
fn input(scenario: &Value, name: &str) -> TestTable {
    let input = scenario["inputs"]
        .as_array()
        .expect("inputs")
        .iter()
        .find(|input| input["name"] == name)
        .unwrap_or_else(|| panic!("no input {name}"));
    let rows = input["rows"].as_array().expect("rows");
    let columns = input["columns"].as_array().expect("columns");
    let columns: Vec<(&str, Column)> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let cells = rows.iter().map(move |row| &row[index]);
            let values: Column = match column["type"].as_str().expect("a type") {
                "bool" => (
                    ColumnType::Boolean,
                    Arc::new(cells.map(Value::as_bool).collect::<BooleanArray>()),
                ),
                "int8" => (
                    ColumnType::Byte,
                    Arc::new(cells.map(narrow).collect::<Int8Array>()),
                ),
                "int16" => (
                    ColumnType::Short,
                    Arc::new(cells.map(narrow).collect::<Int16Array>()),
                ),
                "int32" => (
                    ColumnType::Int,
                    Arc::new(cells.map(narrow).collect::<Int32Array>()),
                ),
                "int64" => (
                    ColumnType::Long,
                    Arc::new(cells.map(int_cell).collect::<Int64Array>()),
                ),
                "float32" => (
                    ColumnType::Float,
                    Arc::new(
                        cells
                            .map(|cell| float_cell(cell).map(|v| v as f32))
                            .collect::<Float32Array>(),
                    ),
                ),
                "float64" => (
                    ColumnType::Double,
                    Arc::new(cells.map(float_cell).collect::<Float64Array>()),
                ),
                "string" => (
                    ColumnType::String,
                    Arc::new(cells.map(Value::as_str).collect::<StringArray>()),
                ),
                other => panic!("no column of type {other}"),
            };
            (column["name"].as_str().expect("a name"), values)
        })
        .collect();
    TestTable::with_previous(
        columns
            .into_iter()
            .map(|(name, column)| (name, column.clone(), column)),
    )
}

fn operation<'a>(scenario: &'a Value, name: &str) -> &'a Value {
    scenario["operations"]
        .as_array()
        .expect("operations")
        .iter()
        .find(|operation| operation["name"] == name)
        .unwrap_or_else(|| panic!("no operation {name}"))
}

fn strings_of(value: &Value) -> impl Iterator<Item = &str> {
    value
        .as_array()
        .expect("a list")
        .iter()
        .map(|item| item.as_str().expect("a string"))
}

/// Checks every formula of a `view` or `update` operation on an input table, except those
/// in `outside_subset`, which must be reported as unsupported. Returns how many it checked.
fn assert_formulas(scenario_name: &str, operation_name: &str, outside_subset: &[&str]) -> usize {
    let scenario = scenario(scenario_name);
    let operation = operation(&scenario, operation_name);
    let table = input(&scenario, operation["source"].as_str().expect("a source"));
    let expected = recorded(scenario_name, operation_name);
    let mut checked = 0;
    for text in strings_of(&operation["formulas"]) {
        let context = format!("{scenario_name}.{operation_name}: {text}");
        if outside_subset.contains(&text) {
            let parsed = Compiler
                .parse_assignment(text)
                .and_then(|a| Compiler.compile(&a.expr, &table.definition).map(|_| ()));
            assert!(
                matches!(parsed, Err(FormulaError::Unsupported(_))),
                "{context} gave {parsed:?}"
            );
            continue;
        }
        let assignment = Compiler
            .parse_assignment(text)
            .unwrap_or_else(|e| panic!("{context}: {e}"));
        let formula = Compiler
            .compile(&assignment.expr, &table.definition)
            .unwrap_or_else(|e| panic!("{context}: {e}"));
        let actual = formula
            .evaluate(&table, &table.rows, false)
            .unwrap_or_else(|e| panic!("{context}: {e}"));
        let expected = expected
            .column_by_name(&assignment.name)
            .unwrap_or_else(|| panic!("{context}: Java returned no column {}", assignment.name));
        assert_eq!(
            actual.data_type(),
            expected.data_type(),
            "type of {context}"
        );
        assert_eq!(
            actual.data_type(),
            &formula.result_type().arrow_type(),
            "declared type of {context}"
        );
        assert_eq!(render(&actual), render(expected), "values of {context}");
        checked += 1;
    }
    checked
}

/// Checks a `where` operation on an input table: the rows for which every filter is true
/// are the rows Java kept.
fn assert_filters(scenario_name: &str, operation_name: &str) {
    let scenario = scenario(scenario_name);
    let operation = operation(&scenario, operation_name);
    let table = input(&scenario, operation["source"].as_str().expect("a source"));
    let expected = recorded(scenario_name, operation_name);
    let context = format!("{scenario_name}.{operation_name}");

    let mut keep = BooleanArray::from(vec![true; table.rows.size() as usize]);
    for text in strings_of(&operation["filters"]) {
        let (column_type, values) = table
            .evaluate(text)
            .unwrap_or_else(|e| panic!("{context}: {text}: {e}"));
        assert_eq!(column_type, ColumnType::Boolean, "{context}: {text}");
        assert_eq!(values.null_count(), 0, "{context}: {text}");
        keep = arrow::compute::and(&keep, values.as_boolean()).expect("equal lengths");
    }
    for column in table.definition.columns() {
        // Read through a formula, so that a cell holding its type's null value is null, as
        // it is in the table Java was given.
        let (_, all) = table.evaluate(&column.name).expect("readable");
        let kept = arrow::compute::filter(&all, &keep).expect("equal lengths");
        let expected = expected
            .column_by_name(&column.name)
            .unwrap_or_else(|| panic!("{context}: Java returned no column {}", column.name));
        assert_eq!(
            render(&kept),
            render(expected),
            "{context}: column {}",
            column.name
        );
    }
}

#[test]
fn update_and_view_formulas_match_the_recorded_java_results() {
    let checked = [
        assert_formulas("update_float_special", "u", &[]),
        assert_formulas("update_int_overflow", "u", &[]),
        // String concatenation is outside the subset.
        assert_formulas("update_null_propagation", "u", &["M = S + `x`"]),
        assert_formulas("view_arithmetic", "ints", &[]),
        assert_formulas("view_arithmetic", "floats", &[]),
        assert_formulas("view_arithmetic", "small", &[]),
        assert_formulas("null_sentinel_inputs", "as_is", &[]),
        assert_formulas("null_sentinel_inputs", "plus", &[]),
    ];
    // Guards against a scenario file that silently lists no formulas.
    assert_eq!(checked, [8, 8, 7, 6, 6, 4, 4, 4]);
}

/// These filters are formulas in Java too: `ConditionFilter` compiles them through
/// `QueryLanguageParser`.
#[test]
fn condition_filters_match_the_recorded_java_results() {
    assert_filters("where_float_nan", "eq_self");
    assert_filters("where_float_nan", "is_null");
    assert_filters("where_int", "is_null");
    assert_filters("where_string_bool", "null_b");
    assert_filters("null_sentinel_inputs", "nulls");
}

/// Java evaluates these with `MatchFilter` and `RangeFilter`, not as formulas (PARITY.md,
/// "Filters"). On the recorded inputs the formula operators select the same rows.
#[test]
fn match_and_range_filters_agree_with_the_formula_operators_on_the_recorded_inputs() {
    for operation in ["gt", "lt", "ge"] {
        assert_filters("where_float_nan", operation);
    }
    for operation in ["gt", "le", "eq", "not_null_long", "none"] {
        assert_filters("where_int", operation);
    }
    for operation in ["eq", "ne", "null_s", "empty_s", "lt", "is_true", "is_false"] {
        assert_filters("where_string_bool", operation);
    }
}
