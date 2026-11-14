//! Aggregations of the oracle's scenarios, checked against the results recorded from the
//! Java server (`oracle/corpus`, see `oracle/README.md`).
//!
//! Every `agg_by` and `group_by` operation whose source is an input table of its scenario,
//! or a `merge` of input tables, is run, in every scenario, and compared at every recorded
//! step by the rules of `PARITY.md` as `oracle/harness/rules.py` applies them. One over the
//! result of another operation or over a blink input, and an `agg_by` with an aggregation
//! this crate does not have, cannot be run here and is listed in `NOT_RUN`.

use super::super::{group_by, AggregateError};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::merge::merge;
use crate::table::{append_only_input_table, keyed_input_table};
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampNanosecondArray,
};
use arrow::ipc::reader::FileReader;
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::graph::UpdateGraph;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;

/// The `agg_by` operations that are not run, as `scenario.operation: why`.
const NOT_RUN: [&str; 39] = [
    "agg_by_no_keys.all_d: the source is made by where",
    "agg_by_sets.by_key: the source is made by update_view",
    "agg_by_sets.no_key: the source is made by update_view",
    "agg_by_sets.by_bool: the source is made by update_view",
    "agg_by_sets.of_empty: the source is made by update_view",
    "agg_by_sets.of_empty_by_key: the source is made by update_view",
    "agg_by_sets.sorted_by_double: no aggregation sorted_first",
    "agg_by_sets.unique_sentinel: no aggregation unique",
    "agg_by_sets.count_where_unknown: no aggregation count_where",
    "agg_by_sets.formula_bad: no aggregation formula",
    "agg_by_sets.sorted_unknown: no aggregation sorted_first",
    "agg_by_statistics.by_key: no aggregation abs_sum",
    "agg_by_statistics.no_key: no aggregation abs_sum",
    "agg_by_statistics.by_two: no aggregation abs_sum",
    "agg_by_statistics.special_by_key: no aggregation abs_sum",
    "agg_by_statistics.special_no_key: no aggregation abs_sum",
    "agg_by_statistics.of_empty: no aggregation abs_sum",
    "agg_by_statistics.of_empty_by_key: no aggregation abs_sum",
    "agg_by_statistics.time_median: no aggregation median",
    "agg_by_statistics.string_median: no aggregation median",
    "agg_by_statistics.string_std: no aggregation std",
    "agg_by_statistics.string_abs_sum: no aggregation abs_sum",
    "agg_by_statistics.bool_var: no aggregation var",
    "agg_by_statistics.pct_out_of_range: no aggregation pct",
    "agg_by_statistics.string_weight: no aggregation weighted_avg",
    "pipeline.by_region: the source is made by natural_join",
    "ticking_agg_by_full.by_key: no aggregation abs_sum",
    "ticking_agg_by_full.no_key: no aggregation abs_sum",
    "ticking_blink_sort_head_tail.seen: the source is a blink input",
    "ticking_blink_sort_head_tail.seen_sorted: the source is made by sort",
    "ticking_blink_sort_head_tail.seen_first_two: the source is made by head",
    "ticking_blink_sort_head_tail.seen_last_two: the source is made by tail",
    "ticking_blink_sort_head_tail.seen_none: the source is made by head",
    "ticking_blink_sort_head_tail.seen_top_two: the source is made by head",
    "ticking_blink_sort_head_tail.seen_bottom_one: the source is made by tail",
    "ticking_blink_sort_head_tail.seen_rev: the source is made by reverse",
    "ticking_blink_sort_head_tail.seen_big_by_s: the source is made by where",
    "ticking_snapshot_when.count_snapshots: the source is made by snapshot_when",
    "ticking_time_table.first_day_count: the source is made by where",
];

/// The number of recorded results of `agg_all_by`, none of which is run: the engine has
/// no such operation, the gRPC handler makes the aggregations of an `agg_by` from it.
const AGG_ALL_BY: usize = 43;

/// The results the test must compare, as `scenario.table xN` for a table compared at N
/// recorded steps. A scenario or operation that silently stops being run is missing here.
const COMPARED: [&str; 34] = [
    "agg_by_avg.avg_g x1",
    "agg_by_avg.avg_k x1",
    "agg_by_int_overflow.avgs x1",
    "agg_by_int_overflow.sums x1",
    "agg_by_int_overflow.total x1",
    "agg_by_min_max_first_last.first_last x1",
    "agg_by_min_max_first_last.min_max x1",
    "agg_by_no_keys.all x1",
    "agg_by_sets.group_strings x1",
    "agg_by_sum_count.by_k x1",
    "agg_by_sum_count.by_k_g x1",
    "empty_table_and_meta.grouped x1",
    "empty_table_ops.a_all x1",
    "empty_table_ops.a_keyed x1",
    "error_types_and_matches.min_all x1",
    "error_types_and_matches.sum_bool x1",
    "group_by_ungroup.agg_group x1",
    "group_by_ungroup.ga x1",
    "group_by_ungroup.gb x1",
    "group_by_ungroup.grouped x1",
    "group_by_ungroup.grouped_all x1",
    "group_by_ungroup.grouped_empty x1",
    "group_by_ungroup.grouped_empty_all x1",
    "group_by_ungroup.grouped_two x1",
    "null_sentinel_inputs.counted x1",
    "ticking_agg_by.by_k x8",
    "ticking_agg_by.total x8",
    "ticking_empty_start.by_g x5",
    "ticking_empty_start.total x5",
    "ticking_error_after_step.healthy x6",
    "ticking_group_ungroup_head_by.grouped x9",
    "ticking_group_ungroup_head_by.grouped_all x9",
    "ticking_merge_refreshing.merged_count x8",
    "timestamps.range x1",
];

/// The number of operations that must be refused, as Java refused them.
const REFUSED: usize = 8;

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

pub(super) fn json(path: &PathBuf) -> Value {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The table Java returned, with the schema metadata it sent.
fn recorded(scenario: &str, file: &str) -> RecordBatch {
    let path = oracle().join(format!("corpus/{scenario}/{file}"));
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

/// A column of a scenario's type from its cells. A cell holding Java's null value of the
/// type becomes null, as it does when the table is sent to a server.
///
/// `vector<T>` and `array<T>` are types of the fixtures in `ops/agg/fixtures` only: a
/// cell is a list of cells of `T`, or null.
pub(super) fn column<'a>(
    type_name: &str,
    cells: impl Iterator<Item = &'a Value>,
) -> (ColumnType, ArrayRef) {
    column_of(type_name, &cells.collect::<Vec<_>>())
}

fn column_of(type_name: &str, cells: &[&Value]) -> (ColumnType, ArrayRef) {
    let list_type = [("vector<", true), ("array<", false)]
        .into_iter()
        .find_map(|(prefix, vector)| {
            let element = type_name.strip_prefix(prefix)?.strip_suffix('>')?;
            Some((vector, element))
        });
    if let Some((vector, element)) = list_type {
        let lengths: Vec<Option<usize>> = cells
            .iter()
            .map(|cell| cell.as_array().map(Vec::len))
            .collect();
        let items: Vec<&Value> = cells
            .iter()
            .filter_map(|cell| cell.as_array())
            .flatten()
            .collect();
        let (element_type, elements) = column_of(element, &items);
        let column_type = if vector {
            ColumnType::vector(element_type)
        } else {
            ColumnType::array(element_type)
        };
        let values = oxhaven_testkit::list_of(&column_type, lengths, elements);
        return (column_type, values);
    }
    let cells = cells.iter().copied();
    let (column_type, values): (ColumnType, ArrayRef) = match type_name {
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
        // Not a type of the oracle's scenarios: one more of the fixtures in `ops/agg/fixtures`.
        "char" => (
            ColumnType::Char,
            Arc::new(cells.map(narrow).collect::<arrow::array::UInt16Array>()),
        ),
        "string" => (
            ColumnType::String,
            Arc::new(cells.map(Value::as_str).collect::<StringArray>()),
        ),
        "timestamp" => (
            ColumnType::Instant,
            Arc::new(
                cells
                    .map(int_cell)
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone("UTC"),
            ),
        ),
        other => panic!("no column of type {other}"),
    };
    let values = sentinels_to_nulls(&values).expect("a column with nulls for sentinels");
    (column_type, values)
}

pub(super) fn list<'a>(value: &'a Value, what: &str) -> &'a Vec<Value> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{what} is not a list"))
}

pub(super) fn text<'a>(value: &'a Value, what: &str) -> &'a str {
    value
        .as_str()
        .unwrap_or_else(|| panic!("{what} is not a string"))
}

/// The rows of an input, or of an action on it, as a batch of the first `columns.len()`
/// columns of the input.
pub(super) fn rows_batch(columns: &[&Value], rows: &[Value]) -> (TableDefinition, RecordBatch) {
    let built: Vec<(&str, ColumnType, ArrayRef)> = columns
        .iter()
        .enumerate()
        .map(|(index, declared)| {
            let cells = rows.iter().map(move |row| &row[index]);
            let (column_type, values) = column(text(&declared["type"], "type"), cells);
            (text(&declared["name"], "name"), column_type, values)
        })
        .collect();
    batch(&built)
}

pub(super) struct Input {
    pub(super) table: Arc<dyn Table>,
    columns: Vec<Value>,
    key_columns: Vec<String>,
}

pub(super) fn inputs(scenario: &Value, graph: &Arc<SteppedUpdateGraph>) -> BTreeMap<String, Input> {
    list(&scenario["inputs"], "inputs")
        .iter()
        // The engine has no blink input table.
        .filter(|input| input["ticking"]["blink"] != true)
        .map(|input| {
            let columns = list(&input["columns"], "columns").clone();
            let declared: Vec<&Value> = columns.iter().collect();
            let (definition, batch) = rows_batch(&declared, list(&input["rows"], "rows"));
            let ticking = &input["ticking"];
            let key_columns: Vec<String> = match ticking.get("key_columns") {
                Some(keys) => list(keys, "key_columns")
                    .iter()
                    .map(|key| text(key, "key").to_string())
                    .collect(),
                None => Vec::new(),
            };
            let table: Arc<dyn Table> = if ticking.is_null() {
                in_memory_table(definition, &[batch]).expect("a static input")
            } else if key_columns.is_empty() {
                append_only_input_table(graph.clone(), definition, Some(&batch))
                    .expect("an append-only input")
            } else {
                let keys: Vec<&str> = key_columns.iter().map(String::as_str).collect();
                keyed_input_table(graph.clone(), definition, &keys, Some(&batch))
                    .expect("a keyed input")
            };
            let name = text(&input["name"], "name").to_string();
            let input = Input {
                table,
                columns,
                key_columns,
            };
            (name, input)
        })
        .collect()
}

/// The table an `agg_by` reads, if it is an input or a `merge` of such tables; otherwise
/// why it is not made here.
fn source(
    name: &str,
    scenario: &Value,
    inputs: &BTreeMap<String, Input>,
) -> Result<Arc<dyn Table>, String> {
    if let Some(input) = inputs.get(name) {
        return Ok(input.table.clone());
    }
    let operations = list(&scenario["operations"], "operations");
    let Some(operation) = operations.iter().find(|o| o["name"] == name) else {
        return Err("the source is a blink input".to_string());
    };
    match text(&operation["op"], "op") {
        "merge" => {
            let parents = list(&operation["sources"], "sources")
                .iter()
                .map(|parent| source(text(parent, "source"), scenario, inputs))
                .collect::<Result<Vec<_>, _>>()?;
            merge(&parents).map_err(|error| format!("the merge is refused: {error}"))
        }
        other => Err(format!("the source is made by {other}")),
    }
}

/// The aggregations of an `agg_by` operation, or the first one this crate does not have.
///
/// Behavior: `agg_by` in `oracle/harness/scenario.py`.
fn aggregations(operation: &Value) -> Result<Vec<Aggregation>, String> {
    list(&operation["aggs"], "aggs")
        .iter()
        .map(|entry| {
            let spec = match text(&entry["agg"], "agg") {
                "count" => return Ok(count(text(&entry["column"], "column"))),
                "sum" => AggSpec::Sum,
                "avg" => AggSpec::Avg,
                "min" => AggSpec::Min,
                "max" => AggSpec::Max,
                "first" => AggSpec::First,
                "last" => AggSpec::Last,
                "group" => AggSpec::Group,
                other => return Err(format!("no aggregation {other}")),
            };
            let pairs: Vec<&str> = list(&entry["columns"], "columns")
                .iter()
                .map(|pair| text(pair, "pair"))
                .collect();
            Ok(agg(spec, &pairs))
        })
        .collect()
}

/// The output columns that PARITY.md A1 lets differ within a tolerance: floating point
/// results of `sum` and `avg`.
///
/// Behavior: `computed_columns` in `oracle/harness/rules.py`.
fn computed_columns(aggregations: &[Aggregation]) -> Vec<String> {
    aggregations
        .iter()
        .filter_map(|aggregation| match aggregation {
            Aggregation::Columns {
                spec: AggSpec::Sum | AggSpec::Avg,
                pairs,
            } => Some(pairs.iter().map(|pair| pair.output.clone())),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Whether two floats are the same result. Null, NaN and infinity are never within a
/// tolerance; `0.0` and `-0.0` differ unless the column is computed.
///
/// Behavior: `floats_match` in `oracle/harness/rules.py`; PARITY.md A1.
fn floats_match(actual: f64, expected: f64, tolerance: Option<(f64, f64)>) -> bool {
    if actual.is_nan() || expected.is_nan() {
        return actual.is_nan() && expected.is_nan();
    }
    if actual.to_bits() == expected.to_bits() {
        return true;
    }
    let Some((relative, absolute)) = tolerance else {
        return false;
    };
    if actual.is_infinite() || expected.is_infinite() {
        return false;
    }
    let difference = (actual - expected).abs();
    difference <= absolute || difference <= relative * actual.abs().max(expected.abs())
}

/// Compares `table` with what Java returned, by the rules of `oracle/harness/rules.py`.
pub(super) fn assert_matches(
    context: &str,
    table: &dyn Table,
    expected: &RecordBatch,
    computed: &[String],
) {
    let actual = read(table);
    let schema = expected.schema();
    let names: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let actual_names: Vec<&str> = table
        .definition()
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(actual_names, names, "{context}: column names");
    let flat = schema
        .metadata()
        .get("deephaven:attribute.IsFlat")
        .is_some_and(|value| value == "true");
    assert_eq!(table.is_flat(), flat, "{context}: IsFlat");
    let size = actual.first().map_or(0, |column| column.len());
    assert_eq!(size, expected.num_rows(), "{context}: row count");
    for (index, name) in names.iter().enumerate() {
        let (actual, expected) = (&actual[index], expected.column(index));
        assert_eq!(
            actual.data_type(),
            expected.data_type(),
            "{context}: type of {name}"
        );
        let is_computed = computed.iter().any(|column| column == name);
        for row in 0..size {
            let context = format!("{context}: {name} at row {row}");
            assert_eq!(
                actual.is_null(row),
                expected.is_null(row),
                "{context}: null"
            );
            if actual.is_null(row) {
                continue;
            }
            match actual.data_type() {
                DataType::Float64 => {
                    let tolerance = is_computed.then_some((1e-9, 1e-12));
                    let actual = actual.as_primitive::<Float64Type>().value(row);
                    let expected = expected.as_primitive::<Float64Type>().value(row);
                    assert!(
                        floats_match(actual, expected, tolerance),
                        "{context}: {actual:?} is not Java's {expected:?}"
                    );
                }
                DataType::Float32 => {
                    let tolerance = is_computed.then_some((1e-5, 1e-6));
                    let actual = actual.as_primitive::<Float32Type>().value(row);
                    let expected = expected.as_primitive::<Float32Type>().value(row);
                    assert!(
                        floats_match(f64::from(actual), f64::from(expected), tolerance),
                        "{context}: {actual:?} is not Java's {expected:?}"
                    );
                }
                _ => assert_eq!(cell(actual, row), cell(expected, row), "{context}"),
            }
        }
    }
}

/// Applies the actions of one step to the inputs and runs a cycle.
pub(super) fn run_step(
    step: &Value,
    inputs: &BTreeMap<String, Input>,
    graph: &Arc<SteppedUpdateGraph>,
) {
    for action in list(&step["actions"], "actions") {
        apply_action(action, inputs);
    }
    graph.run_cycle();
}

/// Applies one action of a step to its input. The change shows in the next cycle.
pub(super) fn apply_action(action: &Value, inputs: &BTreeMap<String, Input>) {
    {
        // An input that was not made takes no actions.
        let Some(input) = inputs.get(text(&action["table"], "table")) else {
            return;
        };
        let writer = input.table.input_table().expect("an input table");
        let rows = list(&action["rows"], "rows");
        match text(&action["action"], "action") {
            "add" => {
                let declared: Vec<&Value> = input.columns.iter().collect();
                writer.add(rows_batch(&declared, rows).1).expect("an add");
            }
            "delete" => {
                let declared: Vec<&Value> = input
                    .columns
                    .iter()
                    .filter(|column| {
                        input
                            .key_columns
                            .iter()
                            .any(|key| column["name"] == key.as_str())
                    })
                    .collect();
                writer
                    .delete(rows_batch(&declared, rows).1)
                    .expect("a delete");
            }
            other => panic!("no action {other}"),
        }
    }
}

#[derive(Default)]
struct Tally {
    /// The number of steps compared, by `scenario.table`.
    compared: BTreeMap<String, usize>,
    refused: usize,
    not_run: Vec<String>,
    agg_all_by: usize,
}

fn run_scenario(path: &PathBuf, tally: &mut Tally) {
    let scenario = json(path);
    let name = text(&scenario["name"], "name");
    let operations = list(&scenario["operations"], "operations");
    let results = list(&scenario["results"], "results");
    tally.agg_all_by += operations
        .iter()
        .filter(|operation| operation["op"] == "agg_all_by")
        .filter(|operation| results.iter().any(|r| r["table"] == operation["name"]))
        .count();
    let grouping =
        |operation: &&Value| operation["op"] == "agg_by" || operation["op"] == "group_by";
    if !operations.iter().any(|operation| grouping(&operation)) {
        return;
    }
    let manifest = json(&oracle().join(format!("corpus/{name}/manifest.json")));
    let entries = list(&manifest["entries"], "entries");
    let graph = SteppedUpdateGraph::new();
    let inputs = inputs(&scenario, &graph);

    // The tables to compare at each step, with their computed columns.
    let mut tables: Vec<(&str, Arc<dyn Table>, Vec<String>)> = Vec::new();
    for operation in operations.iter().filter(grouping) {
        let table_name = text(&operation["name"], "name");
        let context = format!("{name}.{table_name}");
        let is_group_by = operation["op"] == "group_by";
        let parent = source(text(&operation["source"], "source"), &scenario, &inputs);
        let made = parent.and_then(|parent| {
            let aggregations = if is_group_by {
                Vec::new()
            } else {
                aggregations(operation)?
            };
            Ok((parent, aggregations))
        });
        let (parent, aggregations) = match made {
            Ok(made) => made,
            Err(why) => {
                tally.not_run.push(format!("{context}: {why}"));
                continue;
            }
        };
        let by: Vec<&str> = list(&operation["by"], "by")
            .iter()
            .map(|column| text(column, "by"))
            .collect();
        let result = if is_group_by {
            group_by(&parent, &by)
        } else {
            aggregate(&parent, &aggregations, false, &by)
        };
        let initial = entries
            .iter()
            .find(|entry| entry["step"] == 0 && entry["table"] == table_name);
        match (result, initial.map(|entry| &entry["failure"])) {
            (Ok(table), Some(Value::Null) | None) => {
                tables.push((table_name, table, computed_columns(&aggregations)));
            }
            (Err(error), Some(failure)) if !failure.is_null() => {
                assert_eq!(failure["code"], "INVALID_ARGUMENT", "{context}");
                assert!(
                    !matches!(error, AggregateError::Failed(_)),
                    "{context}: {error} is not a refusal of the request"
                );
                tally.refused += 1;
            }
            (Ok(_), Some(failure)) => panic!("{context}: Java refused with {failure}"),
            (Err(error), _) => panic!("{context}: {error}, where Java gave a table"),
        }
    }

    let steps = scenario["steps"].as_array().cloned().unwrap_or_default();
    for step in 0..=steps.len() {
        if step > 0 {
            run_step(&steps[step - 1], &inputs, &graph);
        }
        for (table_name, table, computed) in &tables {
            let context = format!("{name}.{table_name} at step {step}");
            let entry = entries
                .iter()
                .find(|entry| entry["step"] == step && entry["table"] == *table_name);
            let Some(entry) = entry else {
                // Not among the scenario's results: Java's table was not recorded.
                continue;
            };
            assert!(entry["failure"].is_null(), "{context}: Java failed");
            assert!(!table.is_failed(), "{context}: failed");
            let expected = recorded(name, text(&entry["file"], "file"));
            assert_matches(&context, &**table, &expected, computed);
            *tally
                .compared
                .entry(format!("{name}.{table_name}"))
                .or_default() += 1;
        }
    }
}

#[test]
fn aggregations_match_the_recorded_java_results() {
    let mut tally = Tally::default();
    let mut scenarios = 0;
    for kind in ["static", "ticking"] {
        let directory = oracle().join(format!("scenarios/{kind}"));
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&directory)
            .unwrap_or_else(|e| panic!("{}: {e}", directory.display()))
            .map(|entry| entry.expect("a directory entry").path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .collect();
        paths.sort();
        for path in paths {
            run_scenario(&path, &mut tally);
            scenarios += 1;
        }
    }
    assert!(scenarios > 0);
    assert_eq!(scenarios, 83);
    assert_eq!(tally.not_run, NOT_RUN);
    assert_eq!(tally.agg_all_by, AGG_ALL_BY);
    let compared: Vec<String> = tally
        .compared
        .iter()
        .map(|(table, steps)| format!("{table} x{steps}"))
        .collect();
    assert_eq!(compared, COMPARED, "compared");
    assert_eq!(tally.refused, REFUSED);
}
