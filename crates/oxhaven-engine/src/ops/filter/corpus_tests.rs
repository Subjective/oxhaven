//! `where` against the results recorded from the Java server: the scenarios of
//! `oracle/scenarios` with `oracle/corpus/<scenario>`, and the `where` cases of the formula
//! corpus in `oracle/corpus/formulas` (see `oracle/README.md`).

use super::factory::parse_filter;
use super::filters::FilterSpec;
use super::tests::static_table;
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::keyed_input_table;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Schema};
use arrow::ipc::reader::FileReader;
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{definition, ConsistencyChecker, ReferenceTable, TableSnapshot};
use serde_json::Value as Json;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn json(path: &PathBuf) -> Json {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn column_type(name: &str) -> Option<ColumnType> {
    Some(match name {
        "bool" => ColumnType::Boolean,
        "int8" => ColumnType::Byte,
        "int16" => ColumnType::Short,
        "int32" => ColumnType::Int,
        "int64" => ColumnType::Long,
        "float32" => ColumnType::Float,
        "float64" => ColumnType::Double,
        "string" => ColumnType::String,
        "timestamp" => ColumnType::Instant,
        "char" => ColumnType::Char,
        _ => return None,
    })
}

/// Behavior: `decode_cell` in `oracle/harness/scenario.py`.
fn float_cell(cell: &Json) -> Option<f64> {
    match cell {
        Json::Null => None,
        Json::String(word) => Some(match word.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            other => panic!("unknown float word {other}"),
        }),
        number => Some(number.as_f64().expect("a number")),
    }
}

fn int_cell(cell: &Json) -> Option<i64> {
    (!cell.is_null()).then(|| cell.as_i64().expect("an integer"))
}

fn narrow<T: TryFrom<i64>>(cell: &Json) -> Option<T> {
    int_cell(cell).map(|v| T::try_from(v).ok().expect("a value in the column's range"))
}

/// The cells of one column, as the array a column of `column_type` holds.
fn column_of<'a>(column_type: &ColumnType, cells: impl Iterator<Item = &'a Json>) -> ArrayRef {
    match column_type {
        ColumnType::Boolean => Arc::new(cells.map(Json::as_bool).collect::<BooleanArray>()),
        ColumnType::Byte => Arc::new(cells.map(narrow).collect::<Int8Array>()),
        ColumnType::Short => Arc::new(cells.map(narrow).collect::<Int16Array>()),
        ColumnType::Int => Arc::new(cells.map(narrow).collect::<Int32Array>()),
        ColumnType::Char => Arc::new(cells.map(narrow).collect::<UInt16Array>()),
        ColumnType::Long => Arc::new(cells.map(int_cell).collect::<Int64Array>()),
        ColumnType::Float => Arc::new(
            cells
                .map(|cell| float_cell(cell).map(|v| v as f32))
                .collect::<Float32Array>(),
        ),
        ColumnType::Double => Arc::new(cells.map(float_cell).collect::<Float64Array>()),
        ColumnType::String => Arc::new(cells.map(Json::as_str).collect::<StringArray>()),
        ColumnType::Instant => {
            let nanos: ArrayRef = Arc::new(cells.map(int_cell).collect::<Int64Array>());
            arrow::compute::cast(&nanos, &ColumnType::Instant.arrow_type()).expect("a timestamp")
        }
        other => panic!("no cells for {other:?}"),
    }
}

/// The names and types of a `columns` list, or `None` if a type is not a column type here.
fn columns_of(columns: &Json) -> Option<Vec<(String, ColumnType)>> {
    columns
        .as_array()
        .expect("columns")
        .iter()
        .map(|column| {
            let name = column["name"].as_str().expect("a name").to_string();
            column_type(column["type"].as_str().expect("a type")).map(|t| (name, t))
        })
        .collect()
}

/// `rows` (a list of rows of cells) as one array per column.
fn arrays_of(columns: &[(String, ColumnType)], rows: &Json) -> Vec<ArrayRef> {
    let rows = rows.as_array().expect("rows");
    columns
        .iter()
        .enumerate()
        .map(|(index, (_, column_type))| column_of(column_type, rows.iter().map(|row| &row[index])))
        .collect()
}

fn batch_of(columns: &[(String, ColumnType)], rows: &Json) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| Field::new(name, column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays_of(columns, rows)).expect("a batch")
}

/// Each cell as text, with floats by their bits so that `-0.0` differs from `0.0`, and a
/// cell holding its type's null value as null.
fn rendered(values: &ArrayRef) -> Vec<String> {
    let values = sentinels_to_nulls(values).expect("a column type");
    // A timestamp is compared as its nanoseconds.
    let values = match values.data_type() {
        DataType::Timestamp(_, _) => {
            arrow::compute::cast(&values, &DataType::Int64).expect("nanoseconds")
        }
        _ => values,
    };
    let cell = |index: usize| -> String {
        if values.is_null(index) {
            return "null".to_string();
        }
        match values.data_type() {
            DataType::Float32 => format!("{:?}", values.as_primitive::<Float32Type>().value(index)),
            DataType::Float64 => format!("{:?}", values.as_primitive::<Float64Type>().value(index)),
            _ => arrow::util::display::array_value_to_string(&values, index).expect("a value"),
        }
    };
    (0..values.len()).map(cell).collect()
}

// ---------------------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------------------

/// The table Java returned, or `None` if it answered with a failure.
fn recorded(scenario: &str, step: usize, table: &str) -> Option<RecordBatch> {
    let manifest = json(&oracle().join(format!("corpus/{scenario}/manifest.json")));
    let entry = manifest["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|entry| entry["step"] == step && entry["table"] == table)
        .unwrap_or_else(|| panic!("{scenario}: no entry for {table} at step {step}"));
    if !entry["failure"].is_null() {
        assert_eq!(entry["failure"]["code"], "INVALID_ARGUMENT");
        return None;
    }
    let path = oracle().join(format!(
        "corpus/{scenario}/{}",
        entry["file"].as_str().expect("a file")
    ));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let reader = FileReader::try_new(file, None).expect("an Arrow IPC file");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("a batch")).collect();
    let batch = arrow::compute::concat_batches(&schema, &batches).expect("batches of one schema");
    assert_eq!(
        batch.num_rows() as u64,
        entry["rows"].as_u64().expect("rows")
    );
    Some(batch)
}

fn assert_matches_recording(context: &str, table: &dyn Table, expected: &RecordBatch) {
    let snapshot = TableSnapshot::current(table).unwrap();
    let columns = table.definition().columns();
    assert_eq!(expected.num_columns(), columns.len(), "{context}: columns");
    for (index, column) in columns.iter().enumerate() {
        let recorded = expected
            .column_by_name(&column.name)
            .unwrap_or_else(|| panic!("{context}: Java returned no column {}", column.name));
        assert_eq!(
            rendered(&snapshot.columns[index]),
            rendered(recorded),
            "{context}: column {}",
            column.name
        );
    }
}

fn filters_of(operation: &Json) -> Vec<&str> {
    let filters = operation["filters"].as_array().expect("filters");
    filters
        .iter()
        .map(|f| f.as_str().expect("a string"))
        .collect()
}

/// The `where` operations of a scenario that read an input table and whose result is
/// recorded, with that input.
fn recorded_wheres(scenario: &Json) -> Vec<(&Json, &Json)> {
    let inputs = scenario["inputs"].as_array().expect("inputs");
    let results = scenario["results"].as_array().expect("results");
    scenario["operations"]
        .as_array()
        .expect("operations")
        .iter()
        .filter(|operation| operation["op"] == "where")
        .filter(|operation| results.iter().any(|r| r["table"] == operation["name"]))
        .filter_map(|operation| {
            let input = inputs.iter().find(|i| i["name"] == operation["source"])?;
            Some((operation, input))
        })
        .collect()
}

/// Runs the recorded `where` operations of a static scenario, except those named in
/// `not_run`, which must be refused. Returns how many results were compared.
fn check_static_scenario(name: &str, not_run: &[&str]) -> usize {
    let scenario = json(&oracle().join(format!("scenarios/static/{name}.json")));
    let mut checked = 0;
    for (operation, input) in recorded_wheres(&scenario) {
        let operation_name = operation["name"].as_str().expect("a name");
        let context = format!("{name}.{operation_name}");
        let columns = columns_of(&input["columns"]).expect("column types");
        let named: Vec<(&str, ColumnType)> = columns
            .iter()
            .map(|(n, t)| (n.as_str(), t.clone()))
            .collect();
        let arrays = arrays_of(&columns, &input["rows"]);
        let size = arrays[0].len() as u64;
        let table: Arc<dyn Table> =
            ReferenceTable::new_static(definition(&named), RowSet::flat(size), arrays);
        let result = unstructured_filter(&table, &filters_of(operation), &Compiler);
        if not_run.contains(&operation_name) {
            assert!(
                matches!(result, Err(FilterError::Invalid(_))),
                "{context} is listed as not run but was not refused"
            );
            continue;
        }
        match (result, recorded(name, 0, operation_name)) {
            (Ok(table), Some(expected)) => assert_matches_recording(&context, &*table, &expected),
            (Err(FilterError::Invalid(_)), None) => {}
            (Ok(_), None) => panic!("{context}: Java refused this"),
            (Err(e), _) => panic!("{context}: {e}"),
        }
        checked += 1;
    }
    checked
}

/// Scenarios with a `where` that are not run here, and why:
/// - `agg_by_no_keys.no_nan` and `error_formula.nonzero` are not recorded results; they feed
///   an aggregation and an update.
/// - `ticking_error_after_step.after_joined` filters a natural join.
#[test]
fn static_scenarios_match_the_recorded_java_results() {
    let checked = [
        check_static_scenario("empty_table_ops", &[]),
        check_static_scenario("error_formula", &[]),
        check_static_scenario("error_unknown_column", &[]),
        check_static_scenario("null_sentinel_inputs", &[]),
        check_static_scenario("pipeline", &[]),
        check_static_scenario("timestamps", &[]),
        check_static_scenario("where_float_nan", &[]),
        check_static_scenario("where_int", &[]),
        check_static_scenario("where_string_bool", &[]),
    ];
    assert_eq!(checked, [1, 2, 2, 1, 1, 2, 7, 6, 8]);
}

/// Runs a ticking scenario's recorded `where` operations over a keyed input table, applying
/// each step's actions and comparing after each step. Returns how many results were compared.
fn check_ticking_scenario(name: &str) -> usize {
    let scenario = json(&oracle().join(format!("scenarios/ticking/{name}.json")));
    let mut checked = 0;
    for (operation, input) in recorded_wheres(&scenario) {
        let operation_name = operation["name"].as_str().expect("a name");
        let input_name = input["name"].as_str().expect("a name");
        let columns = columns_of(&input["columns"]).expect("column types");
        let named: Vec<(&str, ColumnType)> = columns
            .iter()
            .map(|(n, t)| (n.as_str(), t.clone()))
            .collect();
        let keys: Vec<&str> = input["ticking"]["key_columns"]
            .as_array()
            .expect("key columns")
            .iter()
            .map(|key| key.as_str().expect("a name"))
            .collect();
        let key_columns: Vec<(String, ColumnType)> = columns
            .iter()
            .filter(|(column, _)| keys.contains(&column.as_str()))
            .cloned()
            .collect();
        let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
        let initial = batch_of(&columns, &input["rows"]);
        let source: Arc<dyn Table> =
            keyed_input_table(graph, definition(&named), &keys, Some(&initial)).unwrap();
        let filters: Vec<String> = filters_of(operation)
            .iter()
            .map(|f| f.to_string())
            .collect();
        let result = unstructured_filter(&source, &filters, &Compiler).unwrap();
        let expected = source.clone();
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            let snapshot = TableSnapshot::current(&*expected).unwrap();
            let copy: Arc<dyn Table> = ReferenceTable::new_static(
                expected.definition().clone(),
                snapshot.rows,
                snapshot.columns,
            );
            unstructured_filter(&copy, &filters, &Compiler).unwrap()
        });
        let compare = |step: usize| {
            checker.assert_consistent();
            let context = format!("{name}.{operation_name} at step {step}");
            let expected = recorded(name, step, operation_name).expect("a recorded table");
            // The input must be what Java had, or the comparison of the result says nothing.
            let input_recorded = scenario["results"]
                .as_array()
                .is_some_and(|results| results.iter().any(|r| r["table"] == input_name));
            if input_recorded {
                let input = recorded(name, step, input_name).expect("a recorded table");
                assert_matches_recording(&format!("{context}: input"), &*source, &input);
            }
            assert_matches_recording(&context, &*result, &expected);
        };
        compare(0);
        checked += 1;
        let writer = source.input_table().expect("an input table");
        for (index, step) in scenario["steps"]
            .as_array()
            .expect("steps")
            .iter()
            .enumerate()
        {
            for action in step["actions"].as_array().expect("actions") {
                assert_eq!(action["table"], input_name);
                match action["action"].as_str().expect("an action") {
                    "add" => writer.add(batch_of(&columns, &action["rows"])).unwrap(),
                    "delete" => writer
                        .delete(batch_of(&key_columns, &action["rows"]))
                        .unwrap(),
                    other => panic!("unknown action {other}"),
                }
            }
            compare(index + 1);
            checked += 1;
        }
    }
    checked
}

#[test]
fn ticking_scenarios_match_the_recorded_java_results_at_every_step() {
    assert_eq!(check_ticking_scenario("ticking_empty_start"), 5);
    assert_eq!(check_ticking_scenario("ticking_where_sort"), 8);
}

// ---------------------------------------------------------------------------------------
// The formula corpus
// ---------------------------------------------------------------------------------------

/// What became of one `where` case of the formula corpus.
enum Verdict {
    /// The same rows as Java, or a failure where Java failed.
    Agrees,
    /// Not comparable, with the reason.
    NotRun(String),
    Differs(String),
}

fn short(text: &str) -> String {
    text.chars().take(70).collect()
}

/// Why the formula compiler does not take the formula a filter string comes down to, if
/// it does not: the reason a case that Java accepts cannot be run here.
fn compiler_refusal(text: &str, table: &dyn Table) -> Option<String> {
    let formula = match parse_filter(text) {
        Ok(FilterSpec::Condition(formula)) => formula,
        Ok(FilterSpec::Match {
            failover: Some(formula),
            ..
        })
        | Ok(FilterSpec::Range {
            failover: Some(formula),
            ..
        }) => formula,
        _ => return None,
    };
    let compiled = Compiler
        .parse(&formula)
        .and_then(|expr| Compiler.compile(&expr, table.definition()));
    compiled.err().map(|e| e.to_string())
}

fn verdict(case: &Json, table: &Arc<dyn Table>) -> Verdict {
    let formula = case["formula"].as_str().expect("a formula");
    let java = &case["java"];
    // The fixture tables here have the row keys 0, 10, 20, ..., to tell keys from positions;
    // Java recorded the case on a table whose keys are its positions.
    let reads_keys = formula
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|word| word == "k");
    if reads_keys {
        return Verdict::NotRun("the formula reads row keys, which differ here".to_string());
    }
    if java["nondeterministic"] == true {
        return Verdict::NotRun("the recorded outcome is not deterministic".to_string());
    }
    let java_rows: Option<Vec<i64>> = java["rows"].as_array().map(|rows| {
        rows.iter()
            .map(|row| row.as_i64().expect("a position"))
            .collect()
    });
    if java_rows.is_none() {
        assert_eq!(java["error"]["status"], "INVALID_ARGUMENT", "{case}");
    }
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        unstructured_filter(table, &[formula], &Compiler).map(|result| {
            result
                .row_set()
                .iter()
                .map(|key| key / 10)
                .collect::<Vec<i64>>()
        })
    }));
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("a panic");
            let construct = message.split(": ").take(2).collect::<Vec<_>>().join(": ");
            return if message.starts_with("unported: ") {
                Verdict::NotRun(construct)
            } else {
                Verdict::Differs(format!("panicked: {message}"))
            };
        }
    };
    match (outcome, java_rows) {
        (Ok(ours), Some(java)) if ours == java => Verdict::Agrees,
        (Ok(ours), Some(java)) => Verdict::Differs(format!("kept {ours:?}, Java kept {java:?}")),
        (Ok(ours), None) => Verdict::Differs(format!(
            "kept {ours:?}, Java failed: {}",
            java["error"]["message"]
        )),
        (Err(_), None) => Verdict::Agrees,
        (Err(FilterError::Invalid(ours)), Some(_)) => match compiler_refusal(formula, &**table) {
            Some(reason) => Verdict::NotRun(format!("the formula compiler: {}", short(&reason))),
            None => Verdict::Differs(format!("refused ({ours}), Java kept rows")),
        },
        (Err(FilterError::Failed(ours)), Some(_)) => {
            Verdict::Differs(format!("failed ({ours}), Java kept rows"))
        }
    }
}

/// Every `where` case of the formula corpus that runs directly against a fixture: the rows
/// kept, or the failure, must be Java's. Cases that cannot be run are counted by reason and
/// printed (`cargo test -- --nocapture`); they are cases with a `setup` (which needs
/// `update`, `view` or `select`), fixtures with array columns, formulas the formula compiler
/// on this branch refuses, and constructs reported as `unported!`.
#[test]
fn formula_corpus_where_cases_match_the_recorded_java_results() {
    let root = oracle().join("corpus/formulas");
    let fixtures = json(&root.join("fixtures.json"));
    let mut tables: BTreeMap<String, Option<Arc<dyn Table>>> = BTreeMap::new();
    for (name, fixture) in fixtures["fixtures"].as_object().expect("fixtures") {
        let table = (!fixture["columns"].is_null())
            .then(|| columns_of(&fixture["columns"]))
            .flatten()
            .map(|columns| {
                let arrays = arrays_of(&columns, &fixture["rows"]);
                let typed = columns
                    .iter()
                    .zip(arrays)
                    .map(|((name, column_type), values)| {
                        (name.as_str(), column_type.clone(), values)
                    })
                    .collect();
                static_table(typed)
            });
        tables.insert(name.clone(), table);
    }

    let mut agreed = 0;
    let mut not_run: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut differences = Vec::new();
    let mut groups: Vec<PathBuf> = std::fs::read_dir(root.join("cases"))
        .expect("the cases")
        .map(|entry| entry.expect("an entry").path())
        .collect();
    groups.sort();
    for group in groups {
        let file = File::open(&group).unwrap_or_else(|e| panic!("{}: {e}", group.display()));
        for line in BufReader::new(file).lines() {
            let case: Json = serde_json::from_str(&line.expect("a line")).expect("a case");
            if case["kind"] != "where" {
                continue;
            }
            let id = case["id"].as_str().expect("an id").to_string();
            let fixture = case["fixture"].as_str().expect("a fixture");
            let verdict = if !case["setup"].is_null() {
                Verdict::NotRun("the case has a setup of other operations".to_string())
            } else {
                match tables
                    .get(fixture)
                    .unwrap_or_else(|| panic!("no fixture {fixture}"))
                {
                    Some(table) => verdict(&case, table),
                    None => Verdict::NotRun("the fixture has array columns".to_string()),
                }
            };
            match verdict {
                Verdict::Agrees => agreed += 1,
                Verdict::NotRun(reason) => not_run.entry(reason).or_default().push(id),
                Verdict::Differs(how) => {
                    differences.push(format!("{id}: `{}` {how}", case["formula"]))
                }
            }
        }
    }
    let skipped: usize = not_run.values().map(Vec::len).sum();
    println!("formula corpus: {agreed} where cases agree, {skipped} not run");
    for (reason, ids) in &not_run {
        println!("  {:>4}  {reason}  (for example {})", ids.len(), ids[0]);
    }
    assert!(
        differences.is_empty(),
        "{} cases differ from Java:\n{}",
        differences.len(),
        differences.join("\n")
    );
    assert!(agreed >= 1330, "only {agreed} cases were compared");
}
