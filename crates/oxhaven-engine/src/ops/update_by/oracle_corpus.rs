//! The oracle's scenarios, as far as they can be built from `update_by`, checked against
//! the results recorded from the Java server (`oracle/corpus`, see `oracle/README.md`).
//!
//! Every scenario is loaded. A table of a scenario is built when it is an `update_by` of an
//! input; the other tables need operations of other tasks and are counted as not run. Every
//! result that could be built is compared with its recording at every step by the rules of
//! `oracle/harness/rules.py` that apply here: column names, order and types, the row count,
//! the cells by position (floats bit for bit, every NaN equal to every NaN), and the
//! attributes `IsFlat`, `AddOnly`, `AppendOnly` and `BlinkTable`. Where Java refused the
//! operation, the operation must be refused here with the same status code. The tolerance
//! of PARITY.md A1 for `cum_sum` is not used: the sums are taken in Java's order.

use super::{
    update_by_with, DeltaControl, EmSpec, RollingWindow, UpdateByError, UpdateByOperation,
    UpdateBySpec, WindowScale,
};
use crate::graph::SteppedUpdateGraph;
use crate::table::{append_only_input_table, in_memory_table, keyed_input_table};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{definition, TableSnapshot};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn graph() -> Arc<dyn UpdateGraph> {
    SteppedUpdateGraph::new()
}

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
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

/// `rows` as a batch of the first `columns.len()` columns named.
fn batch(columns: &[(&str, ColumnType)], rows: &[Value]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, (_, column_type))| cells(column_type, rows, index))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch")
}

/// One input of a scenario.
struct Input {
    table: Arc<dyn Table>,
    columns: Vec<(String, ColumnType)>,
    /// The key columns of a keyed input table.
    keys: Vec<String>,
}

impl Input {
    fn named(&self) -> Vec<(&str, ColumnType)> {
        let columns = self.columns.iter();
        columns
            .map(|(name, t)| (name.as_str(), t.clone()))
            .collect()
    }

    fn add(&self, rows: &[Value]) {
        let input = self.table.input_table().expect("an input table");
        input.add(batch(&self.named(), rows)).expect("an add");
    }

    fn delete(&self, rows: &[Value]) {
        let columns = self.named();
        let keys: Vec<(&str, ColumnType)> = self
            .keys
            .iter()
            .map(|key| {
                let found = columns.iter().find(|(name, _)| name == key);
                found.expect("a key column").clone()
            })
            .collect();
        let input = self.table.input_table().expect("an input table");
        input.delete(batch(&keys, rows)).expect("a delete");
    }
}

/// Behavior: the loading of inputs in `run_scenario` of `oracle/harness/runner.py`: the
/// initial rows of an input table go in through `add`.
fn load(graph: &Arc<dyn UpdateGraph>, input: &Value) -> Input {
    let columns: Vec<(String, ColumnType)> = list(&input["columns"])
        .iter()
        .map(|column| {
            (
                text(&column["name"]).to_string(),
                column_type(text(&column["type"])),
            )
        })
        .collect();
    let named: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, t)| (name.as_str(), t.clone()))
        .collect();
    let definition: TableDefinition = definition(&named);
    let rows = list(&input["rows"]);
    let ticking = &input["ticking"];
    if ticking.is_null() {
        let table = in_memory_table(definition, &[batch(&named, rows)]).expect("a table");
        return Input {
            table,
            columns,
            keys: Vec::new(),
        };
    }
    let keys: Vec<String> = list(&ticking["key_columns"])
        .iter()
        .map(|key| text(key).to_string())
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

/// What became of one table of a scenario.
enum Built {
    /// The table, and the result columns of Java's table that it leaves out: those of the
    /// operations left `unported!`.
    Table(Arc<dyn Table>, Vec<String>),
    /// The operation was refused; the gRPC status code Java reports for this refusal.
    Refused(&'static str),
    /// The table needs an operation of another task.
    NotRun,
}

/// The gRPC status code the Java server answers a refused `update_by` with, as recorded in
/// `error_types_and_matches` (a cumulative sum of a string column) and
/// `update_by_rolling_tick` (`string_sum`, and `empty_window`, whose first computation fails
/// Java's assertion). A table whose first computation throws is answered like a refusal:
/// io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform maps any
/// exception that is not a status to INVALID_ARGUMENT.
fn code(error: &UpdateByError) -> &'static str {
    match error {
        UpdateByError::InvalidArgument(_) | UpdateByError::Failed(_) => "INVALID_ARGUMENT",
    }
}

/// A duration of whole seconds, `PT<n>S`, in nanoseconds. Other spellings are parsed by the
/// gRPC handler, which is not part of this module.
fn nanos(text: &str) -> Option<i64> {
    let seconds: i64 = text.strip_prefix("PT")?.strip_suffix('S')?.parse().ok()?;
    Some(seconds * 1_000_000_000)
}

/// The operation a scenario names by its `pydeephaven.updateby` function and arguments
/// (`oracle/harness/runner.py`, `_update_by`), or `None` when it is not ported or needs the
/// gRPC handler to parse an argument. A count-where is named with its result column as its
/// one column.
fn spec(op: &str, args: &Value, columns: &[&str]) -> Option<UpdateBySpec> {
    let filters = || -> Vec<String> {
        let filters = list(&args["filters"]).iter();
        filters.map(|filter| text(filter).to_string()).collect()
    };
    let ticks = |key: &str| Some(WindowScale::Ticks(args[key].as_f64()?));
    let time = |key: &str| {
        Some(WindowScale::Time {
            column: text(&args["ts_col"]).to_string(),
            nanos: nanos(args[key].as_str()?)?,
        })
    };
    let window = || {
        Some(if op.ends_with("_time") {
            RollingWindow {
                rev: time("rev_time")?,
                fwd: time("fwd_time")?,
            }
        } else {
            RollingWindow {
                rev: ticks("rev_ticks")?,
                fwd: ticks("fwd_ticks")?,
            }
        })
    };
    let em = || {
        Some(EmSpec {
            window: if op.ends_with("_time") {
                time("decay_time")?
            } else {
                ticks("decay_ticks")?
            },
            on_null: None,
            on_nan: None,
        })
    };
    let family = op.trim_end_matches("_tick").trim_end_matches("_time");
    Some(match family {
        "cum_sum" => UpdateBySpec::CumSum,
        "cum_prod" => UpdateBySpec::CumProd,
        "cum_min" => UpdateBySpec::CumMin,
        "cum_max" => UpdateBySpec::CumMax,
        "forward_fill" => UpdateBySpec::ForwardFill,
        "delta" => UpdateBySpec::Delta(match args["delta_control"].as_str() {
            None | Some("NULL_DOMINATES") => DeltaControl::NullDominates,
            Some("VALUE_DOMINATES") => DeltaControl::ValueDominates,
            Some("ZERO_DOMINATES") => DeltaControl::ZeroDominates,
            Some(other) => panic!("no delta control {other}"),
        }),
        "rolling_sum" => UpdateBySpec::RollingSum(window()?),
        "rolling_avg" => UpdateBySpec::RollingAvg(window()?),
        "rolling_min" => UpdateBySpec::RollingMin(window()?),
        "rolling_max" => UpdateBySpec::RollingMax(window()?),
        "rolling_count" => UpdateBySpec::RollingCount(window()?),
        "rolling_prod" => UpdateBySpec::RollingProduct(window()?),
        "rolling_std" => UpdateBySpec::RollingStd(window()?),
        "rolling_group" => UpdateBySpec::RollingGroup(window()?),
        "rolling_wavg" => UpdateBySpec::RollingWAvg {
            window: window()?,
            weight_column: text(&args["wcol"]).to_string(),
        },
        "emstd" => UpdateBySpec::EmStd(em()?),
        "ema" => UpdateBySpec::Ema(em()?),
        "ems" => UpdateBySpec::Ems(em()?),
        "emmin" => UpdateBySpec::EmMin(em()?),
        "emmax" => UpdateBySpec::EmMax(em()?),
        "cum_count_where" => UpdateBySpec::CumCountWhere {
            result_column: columns[0].to_string(),
            filters: filters(),
        },
        "rolling_count_where" => UpdateBySpec::RollingCountWhere {
            window: window()?,
            result_column: columns[0].to_string(),
            filters: filters(),
        },
        "rolling_formula" => UpdateBySpec::RollingFormula {
            window: window()?,
            formula: text(&args["formula"]).to_string(),
            param_token: args["formula_param"].as_str().map(String::from),
        },
        other => panic!("no update_by operation {other}"),
    })
}

/// Defines the table of `operation` from `tables`.
fn define(tables: &BTreeMap<String, Built>, operation: &Value) -> Built {
    if operation["op"].as_str() != Some("update_by") {
        return Built::NotRun;
    }
    let parent = match &tables[text(&operation["source"])] {
        // An operation of another `update_by` is not left out of a table built from it.
        Built::Table(table, left_out) if left_out.is_empty() => table.clone(),
        Built::Table(..) => return Built::NotRun,
        // A table defined from a refused table is refused the same way.
        Built::Refused(code) => return Built::Refused(code),
        Built::NotRun => return Built::NotRun,
    };
    let mut operations = Vec::new();
    let left_out: Vec<String> = Vec::new();
    for op in list(&operation["ops"]) {
        let columns: Vec<&str> = list(&op["columns"]).iter().map(text).collect();
        match spec(text(&op["op"]), &op["args"], &columns) {
            Some(
                spec
                @ (UpdateBySpec::CumCountWhere { .. } | UpdateBySpec::RollingCountWhere { .. }),
            ) => operations.push(UpdateByOperation::new::<&str>(spec, &[])),
            Some(spec) => operations.push(UpdateByOperation::new(spec, &columns)),
            None => return Built::NotRun,
        }
    }
    if operations.is_empty() {
        return Built::NotRun;
    }
    let by: Vec<&str> = list(&operation["by"]).iter().map(text).collect();
    match update_by_with(&parent, &operations, &by, &oxhaven_expr::Compiler) {
        Ok(table) => Built::Table(table, left_out),
        Err(error) => Built::Refused(code(&error)),
    }
}

/// The recordings of one scenario.
struct Recording {
    directory: PathBuf,
    manifest: Value,
}

impl Recording {
    fn of(scenario: &str) -> Self {
        let directory = oracle().join("corpus").join(scenario);
        let manifest = json(&directory.join("manifest.json"));
        Self {
            directory,
            manifest,
        }
    }

    fn entry(&self, step: usize, table: &str) -> &Value {
        list(&self.manifest["entries"])
            .iter()
            .find(|entry| entry["step"] == step && entry["table"] == table)
            .unwrap_or_else(|| panic!("no recording of {table} at step {step}"))
    }

    /// The recorded table, and the attributes in its schema.
    fn table(&self, entry: &Value) -> (RecordBatch, [bool; 4]) {
        let path = self.directory.join(text(&entry["file"]));
        let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let reader = FileReader::try_new(file, None).expect("an Arrow IPC file");
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("a batch")).collect();
        let table = arrow::compute::concat_batches(&schema, &batches).expect("one schema");
        let attributes = ["IsFlat", "AddOnly", "AppendOnly", "BlinkTable"].map(|name| {
            let key = format!("deephaven:attribute.{name}");
            schema.metadata().get(&key).is_some_and(|v| v == "true")
        });
        (table, attributes)
    }
}

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
        (0..e.len()).all(|row| {
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

/// Compares `table` with what Java returned.
fn compare(
    place: &str,
    recording: &Recording,
    entry: &Value,
    table: &dyn Table,
    left_out: &[String],
) {
    let (expected, attributes) = recording.table(entry);
    let kept: Vec<usize> = (0..expected.num_columns())
        .filter(|&index| !left_out.contains(expected.schema().field(index).name()))
        .collect();
    assert_eq!(
        kept.len() + left_out.len(),
        expected.num_columns(),
        "{place}: Java has every column left out"
    );
    let expected = expected.project(&kept).expect("the columns kept");
    let schema = expected.schema();
    let columns = table.definition().columns();
    let names = |fields: Vec<(&str, DataType)>| format!("{fields:?}");
    assert_eq!(
        names(
            columns
                .iter()
                .map(|c| (c.name.as_str(), c.column_type.arrow_type()))
                .collect()
        ),
        names(
            schema
                .fields()
                .iter()
                .map(|f| (f.name().as_str(), f.data_type().clone()))
                .collect()
        ),
        "{place}: columns"
    );
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    assert_eq!(
        snapshot.rows.size(),
        expected.num_rows() as u64,
        "{place}: row count"
    );
    for (index, column) in columns.iter().enumerate() {
        assert!(
            same_cells(expected.column(index), &snapshot.columns[index]),
            "{place}: column {} is {:?}, Java has {:?}",
            column.name,
            snapshot.columns[index],
            expected.column(index)
        );
    }
    let found = table.attributes();
    assert_eq!(
        [
            table.is_flat(),
            found.add_only,
            found.append_only,
            found.blink
        ],
        attributes,
        "{place}: the attributes IsFlat, AddOnly, AppendOnly, BlinkTable"
    );
}

/// What was compared and what was not, as `scenario/table`.
#[derive(Default)]
struct Tally {
    compared: Vec<String>,
    not_run: Vec<String>,
    /// The number of tables compared, over all steps.
    comparisons: usize,
}

fn run(path: &Path, tally: &mut Tally) {
    let scenario = json(path);
    let name = text(&scenario["name"]);
    let recording = Recording::of(name);
    let graph = graph();

    let mut inputs: BTreeMap<String, Input> = BTreeMap::new();
    let mut tables: BTreeMap<String, Built> = BTreeMap::new();
    for input in list(&scenario["inputs"]) {
        let loaded = load(&graph, input);
        let name = text(&input["name"]).to_string();
        tables.insert(name.clone(), Built::Table(loaded.table.clone(), Vec::new()));
        inputs.insert(name, loaded);
    }
    for operation in list(&scenario["operations"]) {
        let built = define(&tables, operation);
        tables.insert(text(&operation["name"]).to_string(), built);
    }

    let results: Vec<&str> = list(&scenario["results"])
        .iter()
        .map(|result| text(&result["table"]))
        // A result that is an input involves none of the operations under test.
        .filter(|result| !inputs.contains_key(*result))
        .collect();
    for result in &results {
        match &tables[*result] {
            Built::NotRun => tally.not_run.push(format!("{name}/{result}")),
            Built::Table(_, left_out) if !left_out.is_empty() => {
                let place = format!("{name}/{result} without {}", left_out.join(", "));
                tally.compared.push(place);
            }
            _ => tally.compared.push(format!("{name}/{result}")),
        }
    }
    let mut check = |step: usize| {
        for result in &results {
            let place = format!("{name}/{result} at step {step}");
            let entry = recording.entry(step, result);
            let failure = &entry["failure"];
            match &tables[*result] {
                Built::NotRun => continue,
                Built::Refused(code) => {
                    assert!(!failure.is_null(), "{place}: refused, but Java has a table");
                    assert_eq!(*code, text(&failure["code"]), "{place}: status code");
                }
                Built::Table(table, left_out) => {
                    assert!(failure.is_null(), "{place}: Java fails with {failure}");
                    assert!(!table.is_failed(), "{place}: the table has failed");
                    compare(&place, &recording, entry, &**table, left_out);
                }
            }
            tally.comparisons += 1;
        }
    };

    check(0);
    for (index, step) in list(&scenario["steps"]).iter().enumerate() {
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
    }
}

#[test]
fn results_match_the_recorded_java_results() {
    let mut paths = Vec::new();
    for kind in ["static", "ticking"] {
        let directory = oracle().join("scenarios").join(kind);
        for entry in std::fs::read_dir(&directory).expect("the scenarios") {
            paths.push(entry.expect("a scenario").path());
        }
    }
    paths.sort();
    let mut tally = Tally::default();
    for path in &paths {
        run(path, &mut tally);
    }
    tally.compared.sort();

    // Every result table of the corpus that is an `update_by` of an input. Everything else
    // needs another operation or, for
    // `update_by_time_and_ema/bad_duration`, the gRPC handler's parse of a duration.
    let expected = [
        "empty_table_ops/ub",
        "error_types_and_matches/cum_sum_bool",
        "error_types_and_matches/cum_sum_string",
        "ticking_update_by_append/keyed",
        "ticking_update_by_append/whole",
        "ticking_update_by_full/cumulative",
        "ticking_update_by_full/cumulative_all",
        "ticking_update_by_full/ema",
        "ticking_update_by_full/log_cumulative",
        "ticking_update_by_full/log_rolling",
        "ticking_update_by_full/rolling",
        "update_by_cum_sum/keyed",
        "update_by_cum_sum/whole",
        "update_by_cumulative/by_key",
        "update_by_cumulative/in_place",
        "update_by_cumulative/no_key",
        "update_by_cumulative/of_empty",
        "update_by_cumulative/of_special",
        "update_by_cumulative/string_fill",
        "update_by_cumulative/string_min",
        "update_by_cumulative/string_sum",
        "update_by_cumulative/time_delta",
        "update_by_cumulative/unknown_column",
        "update_by_cumulative/unknown_key",
        "update_by_rolling_tick/by_key",
        "update_by_rolling_tick/empty_window",
        "update_by_rolling_tick/negative_window",
        "update_by_rolling_tick/no_key",
        "update_by_rolling_tick/of_empty",
        "update_by_rolling_tick/of_special",
        "update_by_rolling_tick/one_row_window",
        "update_by_rolling_tick/string_sum",
        "update_by_time_and_ema/ema_by_key",
        "update_by_time_and_ema/ema_no_key",
        "update_by_time_and_ema/ema_null_timestamp",
        "update_by_time_and_ema/ema_special_ticks",
        "update_by_time_and_ema/int_timestamp",
        "update_by_time_and_ema/null_timestamp",
        "update_by_time_and_ema/timed_by_key",
        "update_by_time_and_ema/timed_empty",
        "update_by_time_and_ema/timed_no_key",
    ];
    assert_eq!(tally.compared, expected);
    // 33 tables of static scenarios at one step, 2 ticking ones at 6 steps and 6 at 8.
    assert_eq!(tally.comparisons, 33 + 2 * 6 + 6 * 8);
    assert_eq!(tally.not_run.len(), 895, "{:#?}", tally.not_run);
}
