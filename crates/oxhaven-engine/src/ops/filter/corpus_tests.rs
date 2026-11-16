//! `where` against the results recorded from the Java server: the scenarios of
//! `oracle/scenarios` with `oracle/corpus/<scenario>`, and the `where` cases of the formula
//! corpus in `oracle/corpus/formulas` (see `oracle/README.md`).

use super::factory::parse_filter;
use super::filters::FilterSpec;
use super::tests::static_table;
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{append_only_input_table, keyed_input_table};
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

/// What Java answered for `table` at `step`: the table, or the status code of its failure.
fn recorded(
    manifest: &Json,
    scenario: &str,
    step: usize,
    table: &str,
) -> Result<RecordBatch, String> {
    let entry = manifest["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .find(|entry| entry["step"] == step && entry["table"] == table)
        .unwrap_or_else(|| panic!("{scenario}: no entry for {table} at step {step}"));
    if !entry["failure"].is_null() {
        return Err(entry["failure"]["code"]
            .as_str()
            .expect("a code")
            .to_string());
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
    Ok(batch)
}

/// The attributes Java sent with a recorded table: `IsFlat`, `AddOnly`, `AppendOnly`,
/// `BlinkTable`, and whether it has the `InputTable` attribute.
///
/// Behavior: `BarrageUtil#attributesToMetadata`, which sends a boolean attribute as
/// `deephaven:attribute.<name>` and names one it cannot send, as the input table's updater,
/// under `deephaven:unsent.attribute.<name>`.
fn recorded_attributes(expected: &RecordBatch) -> [bool; 5] {
    let schema = expected.schema();
    let metadata = schema.metadata();
    let flag = |name: &str| {
        metadata
            .get(&format!("deephaven:attribute.{name}"))
            .is_some_and(|value| value == "true")
    };
    [
        flag("IsFlat"),
        flag("AddOnly"),
        flag("AppendOnly"),
        flag("BlinkTable"),
        metadata.contains_key("deephaven:unsent.attribute.InputTable"),
    ]
}

fn attributes_of(table: &dyn Table) -> [bool; 5] {
    let attributes = table.attributes();
    [
        table.is_flat(),
        attributes.add_only,
        attributes.append_only,
        attributes.blink,
        table.input_table().is_some(),
    ]
}

/// How `table` differs from the table Java recorded, if it does: column names, order and
/// types, every cell by position (floats by their bits, with a cell holding its type's null
/// value as null), and the attributes.
fn difference(table: &dyn Table, expected: &RecordBatch) -> Option<String> {
    let columns = table.definition().columns();
    let ours: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| (c.name.as_str(), c.column_type.arrow_type()))
        .collect();
    let schema = expected.schema();
    let theirs: Vec<(&str, DataType)> = schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.data_type().clone()))
        .collect();
    if ours != theirs {
        return Some(format!("columns {ours:?}, Java has {theirs:?}"));
    }
    let snapshot = match TableSnapshot::current(table) {
        Ok(snapshot) => snapshot,
        Err(error) => return Some(format!("cannot be read: {error}")),
    };
    for (index, column) in columns.iter().enumerate() {
        let ours = rendered(&snapshot.columns[index]);
        let theirs = rendered(expected.column(index));
        if ours != theirs {
            return Some(format!(
                "column {} is {ours:?}, Java has {theirs:?}",
                column.name
            ));
        }
    }
    let (ours, theirs) = (attributes_of(table), recorded_attributes(expected));
    (ours != theirs).then(|| {
        format!(
            "IsFlat, AddOnly, AppendOnly, BlinkTable, InputTable are {ours:?}, Java has {theirs:?}"
        )
    })
}

/// A literal or column of a structured filter condition.
///
/// Behavior: `_value` in `oracle/harness/runner.py`, which makes the `Value` message.
fn condition_value(value: &Json) -> Value {
    let (kind, body) = value
        .as_object()
        .and_then(|object| object.iter().next())
        .expect("a value");
    let literal = match (kind.as_str(), body) {
        ("column", name) => return Value::Reference(name.as_str().expect("a name").to_string()),
        ("long", number) => Literal::Long(number.as_i64().expect("a long")),
        ("nanos", number) => Literal::NanoTime(number.as_i64().expect("nanoseconds")),
        (_, Json::Bool(flag)) => Literal::Bool(*flag),
        (_, Json::String(word)) if !["NaN", "Infinity", "-Infinity"].contains(&word.as_str()) => {
            Literal::String(word.clone())
        }
        (_, number) => Literal::Double(float_cell(number).expect("a number")),
    };
    Value::Literal(literal)
}

/// A structured filter condition, or why it has no engine form.
///
/// Behavior: `_condition` in `oracle/harness/runner.py`, which makes the `Condition`
/// message, and `condition` in `oxhaven-server`'s `table/ops/filter.rs`, which reads it.
fn condition(condition: &Json) -> Result<Condition, String> {
    let (kind, body) = condition
        .as_object()
        .and_then(|object| object.iter().next())
        .expect("a condition");
    let flag = |name: &str| body[name] == true;
    let case = || match flag("ignore_case") {
        true => CaseSensitivity::IgnoreCase,
        false => CaseSensitivity::MatchCase,
    };
    let match_type = || match flag("inverted") {
        true => MatchType::Inverted,
        false => MatchType::Regular,
    };
    let text = |name: &str| body[name].as_str().expect("a string").to_string();
    let all = || {
        body.as_array()
            .expect("conditions")
            .iter()
            .map(self::condition)
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(match kind.as_str() {
        "and" => Condition::And(all()?),
        "or" => Condition::Or(all()?),
        "not" => Condition::Not(Box::new(self::condition(body)?)),
        "compare" => Condition::Compare {
            op: match body["op"].as_str().expect("an operator") {
                "<" => CompareOp::LessThan,
                "<=" => CompareOp::LessThanOrEqual,
                ">" => CompareOp::GreaterThan,
                ">=" => CompareOp::GreaterThanOrEqual,
                "==" => CompareOp::Equals,
                "!=" => CompareOp::NotEquals,
                other => panic!("no comparison {other}"),
            },
            case_sensitivity: case(),
            lhs: condition_value(&body["lhs"]),
            rhs: condition_value(&body["rhs"]),
        },
        "in" => Condition::In {
            target: condition_value(&body["target"]),
            candidates: body["candidates"]
                .as_array()
                .expect("candidates")
                .iter()
                .map(condition_value)
                .collect(),
            case_sensitivity: case(),
            match_type: match_type(),
            nan_comparison: match flag("nan_equals") {
                true => NanComparison::NanEqualsNan,
                false => NanComparison::NanNotEqualsNan,
            },
        },
        "is_null" => Condition::IsNull {
            column: body.as_str().expect("a column").to_string(),
        },
        // The gRPC handler refuses it, as the Java server's visitor does; the engine has no
        // form of it.
        "is_nan" => return Err("is_nan, which the gRPC handler refuses".to_string()),
        "matches" => Condition::Matches {
            column: text("column"),
            regex: text("text"),
            case_sensitivity: case(),
            match_type: match_type(),
        },
        "contains" => Condition::Contains {
            column: text("column"),
            search_string: text("text"),
            case_sensitivity: case(),
            match_type: match_type(),
        },
        "invoke" => Condition::Invoke {
            method: text("method"),
            target: (!body["target"].is_null()).then(|| condition_value(&body["target"])),
            arguments: body["arguments"]
                .as_array()
                .expect("arguments")
                .iter()
                .map(condition_value)
                .collect(),
        },
        "search" => Condition::Search {
            search_string: text("text"),
            columns: body["columns"]
                .as_array()
                .map(|columns| {
                    columns
                        .iter()
                        .map(|c| c.as_str().expect("a column").to_string())
                        .collect()
                })
                .unwrap_or_default(),
        },
        other => panic!("no condition {other}"),
    })
}

/// One of this module's operations, applied to a parent table.
type Operation = Arc<dyn Fn(&Arc<dyn Table>) -> Result<Arc<dyn Table>, FilterError> + Send + Sync>;

/// The `where` or `filter` of a scenario operation, or why it is not run.
fn operation_of(operation: &Json) -> Result<Operation, String> {
    match operation["op"].as_str().expect("an op") {
        "where" => {
            let filters: Vec<String> = operation["filters"]
                .as_array()
                .expect("filters")
                .iter()
                .map(|f| f.as_str().expect("a string").to_string())
                .collect();
            Ok(Arc::new(move |parent| {
                unstructured_filter(parent, &filters, &Compiler)
            }))
        }
        "filter" => {
            let conditions = operation["conditions"]
                .as_array()
                .expect("conditions")
                .iter()
                .map(condition)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(move |parent| {
                filter(parent, &conditions, &Compiler)
            }))
        }
        other => Err(other.to_string()),
    }
}

/// A table of a scenario as far as it is made here.
enum Made {
    Table(Arc<dyn Table>),
    /// This module's operation refused the request, for the reason given.
    Refused(String),
    /// Made by an operation of another module, or from such a table, or an input this test
    /// cannot make, as the reason says.
    NotRun(String),
}

/// An input table that takes the actions of the steps.
struct Input {
    table: Arc<dyn Table>,
    columns: Vec<(String, ColumnType)>,
    keys: Vec<(String, ColumnType)>,
}

/// What was compared and what was not.
#[derive(Default)]
struct Tally {
    /// The steps compared, by `scenario.table`, for every result of this module's
    /// operations that agrees with Java at every step.
    compared: BTreeMap<String, usize>,
    /// `scenario.table at step N: how`, for every result that differs.
    differences: Vec<String>,
    /// `scenario.table: why`, for the results of this module's operations not made here.
    not_run: Vec<String>,
}

/// Replays a scenario: its inputs, its `where` and `filter` operations on them and on each
/// other, and its steps; and compares every recorded result of those operations at every
/// step, with the input it reads when that is recorded too. A refreshing result is checked
/// against a recomputation from a snapshot of its parent at every step.
fn replay(scenario: &Json, tally: &mut Tally) {
    let name = scenario["name"].as_str().expect("a name");
    let manifest = json(&oracle().join(format!("corpus/{name}/manifest.json")));
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let mut tables: BTreeMap<String, Made> = BTreeMap::new();
    let mut inputs: BTreeMap<String, Input> = BTreeMap::new();
    for input in scenario["inputs"].as_array().expect("inputs") {
        let input_name = input["name"].as_str().expect("a name").to_string();
        if input["ticking"]["blink"] == true {
            // This replay does not drive a blink input table.
            tables.insert(input_name, Made::NotRun("blink input".to_string()));
            continue;
        }
        let columns = columns_of(&input["columns"]).expect("column types");
        let named: Vec<(&str, ColumnType)> = columns
            .iter()
            .map(|(n, t)| (n.as_str(), t.clone()))
            .collect();
        let rows = input["rows"].as_array().expect("rows").len() as u64;
        let table: Arc<dyn Table> = if input["ticking"].is_null() {
            let arrays = arrays_of(&columns, &input["rows"]);
            ReferenceTable::new_static(definition(&named), RowSet::flat(rows), arrays)
        } else {
            let initial = batch_of(&columns, &input["rows"]);
            let keys: Vec<&str> = input["ticking"]["key_columns"]
                .as_array()
                .expect("key columns")
                .iter()
                .map(|key| key.as_str().expect("a name"))
                .collect();
            if keys.is_empty() {
                append_only_input_table(graph.clone(), definition(&named), Some(&initial)).unwrap()
            } else {
                keyed_input_table(graph.clone(), definition(&named), &keys, Some(&initial)).unwrap()
            }
        };
        let keys = input["ticking"]["key_columns"]
            .as_array()
            .map(|keys| {
                columns
                    .iter()
                    .filter(|(column, _)| keys.iter().any(|key| key == column.as_str()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        tables.insert(input_name.clone(), Made::Table(table.clone()));
        inputs.insert(
            input_name,
            Input {
                table,
                columns,
                keys,
            },
        );
    }

    // This module's operations, with the parent each reads.
    let mut ours: Vec<(&str, &str)> = Vec::new();
    let mut checkers = Vec::new();
    for operation in scenario["operations"].as_array().expect("operations") {
        let table_name = operation["name"].as_str().expect("a name");
        let op = operation["op"].as_str().expect("an op");
        let parent_name = operation["source"].as_str();
        let made = match operation_of(operation) {
            Err(why) => Made::NotRun(why),
            Ok(run) => match &tables[parent_name.expect("a source")] {
                Made::NotRun(why) => Made::NotRun(why.clone()),
                Made::Refused(_) => Made::NotRun("the source is refused".to_string()),
                Made::Table(parent) => {
                    // An operation may report a construct that is not ported yet by
                    // panicking.
                    match catch_unwind(AssertUnwindSafe(|| run(parent))) {
                        Ok(Ok(table)) => {
                            if table.is_refreshing() {
                                let parent = parent.clone();
                                let recompute = move || {
                                    let snapshot = TableSnapshot::current(&*parent).unwrap();
                                    let copy: Arc<dyn Table> = ReferenceTable::new_static(
                                        parent.definition().clone(),
                                        snapshot.rows,
                                        snapshot.columns,
                                    );
                                    run(&copy).unwrap()
                                };
                                checkers.push(ConsistencyChecker::attach_with_recompute(
                                    table.clone(),
                                    recompute,
                                ));
                            }
                            Made::Table(table)
                        }
                        Ok(Err(error)) => Made::Refused(error.to_string()),
                        Err(panic) => {
                            let message = panic
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| panic.downcast_ref::<&str>().copied())
                                .unwrap_or("a panic");
                            match message.strip_prefix("unported: ") {
                                Some(construct) => Made::NotRun(format!("unported {construct}")),
                                None => std::panic::resume_unwind(panic),
                            }
                        }
                    }
                }
            },
        };
        if OURS.contains(&op) {
            ours.push((table_name, parent_name.expect("a source")));
        }
        tables.insert(table_name.to_string(), made);
    }

    // The results of this module's operations.
    let results: Vec<(&str, &str)> = ours
        .into_iter()
        .filter(|(table, _)| {
            scenario["results"]
                .as_array()
                .expect("results")
                .iter()
                .any(|result| result["table"] == *table)
        })
        .collect();
    let recorded_inputs: Vec<&str> = scenario["results"]
        .as_array()
        .expect("results")
        .iter()
        .map(|result| result["table"].as_str().expect("a table"))
        .filter(|table| inputs.contains_key(*table))
        .collect();
    for (table, _) in &results {
        if let Made::NotRun(why) = &tables[*table] {
            tally.not_run.push(format!("{name}.{table}: {why}"));
        }
    }
    let mut differs: Vec<String> = Vec::new();
    let mut steps_compared = 0;
    let mut compare = |step: usize, differs: &mut Vec<String>| {
        for checker in &checkers {
            checker.assert_consistent();
        }
        for (table, parent) in &results {
            let place = format!("{name}.{table} at step {step}");
            let expected = recorded(&manifest, name, step, table);
            let difference = match (&tables[*table], expected) {
                (Made::NotRun(_), _) => continue,
                (Made::Refused(_), Err(code)) if code == "INVALID_ARGUMENT" => None,
                (Made::Refused(why), Err(code)) => {
                    Some(format!("refused ({why}), Java with {code}"))
                }
                (Made::Refused(why), Ok(_)) => Some(format!("refused ({why}), Java has a table")),
                (Made::Table(_), Err(code)) => Some(format!("Java fails with {code}")),
                (Made::Table(made), Ok(_)) if made.is_failed() => {
                    Some("the table has failed".to_string())
                }
                (Made::Table(made), Ok(expected)) => {
                    // The input must be what Java had, or the comparison of the result says
                    // nothing.
                    let input = recorded_inputs.contains(parent).then(|| {
                        let Made::Table(input) = &tables[*parent] else {
                            unreachable!("a recorded input is made")
                        };
                        let recorded = recorded(&manifest, name, step, parent).expect("a table");
                        difference(&**input, &recorded).map(|how| format!("input: {how}"))
                    });
                    input.flatten().or_else(|| difference(&**made, &expected))
                }
            };
            if let Some(how) = difference {
                differs.push(table.to_string());
                tally.differences.push(format!("{place}: {how}"));
            }
        }
    };
    compare(0, &mut differs);
    steps_compared += 1;
    for step in scenario["steps"].as_array().cloned().unwrap_or_default() {
        for action in step["actions"].as_array().expect("actions") {
            // An input that was not made takes no actions.
            let Some(input) = inputs.get(action["table"].as_str().expect("a table")) else {
                continue;
            };
            let writer = input.table.input_table().expect("an input table");
            match action["action"].as_str().expect("an action") {
                "add" => writer
                    .add(batch_of(&input.columns, &action["rows"]))
                    .unwrap(),
                "delete" => writer
                    .delete(batch_of(&input.keys, &action["rows"]))
                    .unwrap(),
                other => panic!("unknown action {other}"),
            }
        }
        compare(steps_compared, &mut differs);
        steps_compared += 1;
    }
    for (table, _) in &results {
        if !matches!(tables[*table], Made::NotRun(_)) && !differs.iter().any(|d| d == table) {
            tally
                .compared
                .insert(format!("{name}.{table}"), steps_compared);
        }
    }
}

/// The operations of this module.
const OURS: [&str; 2] = ["where", "filter"];

/// Results that differ from what Java recorded, each with the first difference found.
const KNOWN_DIFFERENCES: [&str; 0] = [];

/// Every scenario of the oracle, replayed as far as `where` and `filter` reach: each
/// recorded result of one of them is compared at every step, or listed as not run with the
/// reason. A `where` or `filter` that is not a recorded result (`agg_by_no_keys.no_nan`
/// and `error_formula.nonzero` feed an aggregation and an update) is made but not compared.
#[test]
fn scenarios_match_the_recorded_java_results() {
    let mut paths = Vec::new();
    for kind in ["static", "ticking"] {
        let directory = oracle().join("scenarios").join(kind);
        for entry in std::fs::read_dir(&directory).expect("the scenarios") {
            paths.push(entry.expect("a scenario").path());
        }
    }
    paths.sort();
    assert_eq!(paths.len(), 89);
    let mut tally = Tally::default();
    for path in &paths {
        replay(&json(path), &mut tally);
    }
    assert_eq!(
        tally.differences, KNOWN_DIFFERENCES,
        "differences from Java"
    );
    let compared: Vec<String> = tally
        .compared
        .iter()
        .map(|(table, steps)| format!("{table} x{steps}"))
        .collect();
    let expected = [
        "column_arrays.fails_null_boolean x1",
        "column_arrays.kept x1",
        "column_arrays.kept_boolean x1",
        "column_arrays.kept_two x1",
        "column_arrays.kept_whole x1",
        "column_statistics.big x1",
        "empty_table_ops.w x1",
        "error_formula.filter_not_boolean x1",
        "error_formula.syntax_filter x1",
        "error_unknown_column.bad_where x1",
        "error_unknown_column.good x1",
        "filter_structured.and_ x1",
        "filter_structured.bad_regex x1",
        "filter_structured.bool_equal x1",
        "filter_structured.bool_not_equal x1",
        "filter_structured.column_to_column x1",
        "filter_structured.column_to_other_type x1",
        "filter_structured.contains x1",
        "filter_structured.contains_empty x1",
        "filter_structured.contains_ignore_case x1",
        "filter_structured.contains_inverted x1",
        "filter_structured.double_equal_nan x1",
        "filter_structured.double_equal_zero x1",
        "filter_structured.double_greater_equal x1",
        "filter_structured.double_less x1",
        "filter_structured.double_less_infinity x1",
        "filter_structured.double_not_equal x1",
        "filter_structured.float_greater x1",
        "filter_structured.in_columns x1",
        "filter_structured.in_doubles x1",
        "filter_structured.in_doubles_finite x1",
        "filter_structured.in_doubles_nan_equal x1",
        "filter_structured.in_doubles_nan_not_equal x1",
        "filter_structured.in_ints x1",
        "filter_structured.in_strings x1",
        "filter_structured.in_strings_ignore_case x1",
        "filter_structured.int_equal x1",
        "filter_structured.int_greater x1",
        "filter_structured.int_greater_equal x1",
        "filter_structured.int_less x1",
        "filter_structured.int_less_equal x1",
        "filter_structured.int_not_equal x1",
        "filter_structured.invoke_function x1",
        "filter_structured.invoke_method x1",
        "filter_structured.invoke_method_no_nulls x1",
        "filter_structured.is_not_null_bool x1",
        "filter_structured.is_null_double x1",
        "filter_structured.is_null_string x1",
        "filter_structured.literal_on_the_left x1",
        "filter_structured.matches x1",
        "filter_structured.matches_empty x1",
        "filter_structured.matches_ignore_case x1",
        "filter_structured.matches_inverted x1",
        "filter_structured.matches_on_int x1",
        "filter_structured.nested x1",
        "filter_structured.not_ x1",
        "filter_structured.not_in_doubles_finite x1",
        "filter_structured.not_in_ints x1",
        "filter_structured.not_less x1",
        "filter_structured.of_empty x1",
        "filter_structured.or_ x1",
        "filter_structured.search x1",
        "filter_structured.search_columns x1",
        "filter_structured.string_against_number x1",
        "filter_structured.string_equal x1",
        "filter_structured.string_equal_ignore_case x1",
        "filter_structured.string_greater_equal_quoted x1",
        "filter_structured.string_less x1",
        "filter_structured.string_less_quoted x1",
        "filter_structured.string_not_equal x1",
        "filter_structured.time_equal x1",
        "filter_structured.time_greater x1",
        "filter_structured.two_filters x1",
        "filter_structured.unknown_column x1",
        "flatten_reverse.big x1",
        "null_sentinel_inputs.nulls x1",
        "pipeline.valid x1",
        "slice_positions.big x1",
        "snapshot_static.big x1",
        "ticking_column_arrays.appended_kept x6",
        "ticking_column_arrays.kept x6",
        "ticking_column_arrays.kept_two x6",
        "ticking_empty_start.w x5",
        "ticking_filter_structured.a_or_null x9",
        "ticking_filter_structured.both x9",
        "ticking_filter_structured.filter_of_filter x9",
        "ticking_filter_structured.id_below_i x9",
        "ticking_filter_structured.in_doubles x9",
        "ticking_filter_structured.in_list x9",
        "ticking_filter_structured.letters x9",
        "ticking_filter_structured.not_a x9",
        "ticking_filter_structured.positive x9",
        "ticking_filter_structured.small x9",
        "ticking_head_tail.big x9",
        "ticking_merge_refreshing.a_big x8",
        "ticking_slice_flatten_reverse.big x9",
        "ticking_snapshot_when.big x9",
        "ticking_where_sort.positive x8",
        "timestamps.after x1",
        "timestamps.not_null x1",
        "where_float_nan.eq_self x1",
        "where_float_nan.ge x1",
        "where_float_nan.gt x1",
        "where_float_nan.is_inf x1",
        "where_float_nan.is_nan x1",
        "where_float_nan.is_null x1",
        "where_float_nan.lt x1",
        "where_int.eq x1",
        "where_int.gt x1",
        "where_int.is_null x1",
        "where_int.le x1",
        "where_int.none x1",
        "where_int.not_null_long x1",
        "where_string_bool.empty_s x1",
        "where_string_bool.eq x1",
        "where_string_bool.is_false x1",
        "where_string_bool.is_true x1",
        "where_string_bool.lt x1",
        "where_string_bool.ne x1",
        "where_string_bool.null_b x1",
        "where_string_bool.null_s x1",
    ];
    assert_eq!(compared, expected, "compared");
    // Results of this module's operations made from an operation of another module or from
    // an input this test cannot make, and conditions the engine has no form of.
    let not_run = [
        "filter_structured.is_nan_double: is_nan, which the gRPC handler refuses",
        "filter_structured.is_nan_float: is_nan, which the gRPC handler refuses",
        "filter_structured.is_nan_int: is_nan, which the gRPC handler refuses",
        "lazy_update_update_view.uv_then_where: update_view",
        "time_types.where_d: update",
        "time_types.where_d_in: update",
        "time_types.where_d_range: update",
        "time_types.where_lt: update",
        "time_types.where_dur: update",
        "time_types.where_w: update",
        "time_types.where_w_ne: update",
        "time_types.where_z: update",
        "time_types.where_z_le: update",
        "time_types.where_z_gt: update",
        "ticking_blink_sort_head_tail.big: blink input",
        "ticking_column_arrays.kept_chain: update",
        "ticking_lazy_update_update_view.uv_big: update_view",
        "ticking_time_table.first_day: time_table",
    ];
    assert_eq!(tally.not_run, not_run, "not run");
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
