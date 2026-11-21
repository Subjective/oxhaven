//! The results recorded from the Java server (`oracle/corpus`, see `oracle/README.md`).
//!
//! Every scenario of the oracle is replayed as far as this module's operations reach: a
//! result is compared when it is made by `view`, `update_view`, `lazy_update`, `select`,
//! `update` or `drop_columns` from an input table, from `empty_table` or from another such
//! result. The comparison follows `oracle/harness/rules.py`:
//! whether the result fails, column names, order and types, the `IsFlat`, `AddOnly`,
//! `AppendOnly` and `BlinkTable` attributes, the row count, and every cell by position, with
//! floats bit for bit except that every NaN equals every NaN. The status code of a failure
//! is not compared here, because the engine has none.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{append_only_input_table, empty_table, in_memory_table, keyed_input_table};
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int8Array, RecordBatch,
    StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::FileReader;
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_testkit::ConsistencyChecker;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

/// Results that differ from what Java recorded, each with the first difference found.
const KNOWN_DIFFERENCES: [&str; 0] = [];

/// The operations of this module.
const OURS: [&str; 6] = [
    "view",
    "update_view",
    "lazy_update",
    "select",
    "update",
    "drop_columns",
];

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn json(path: &PathBuf) -> Value {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// Every scenario of the oracle, by name.
fn scenarios() -> Vec<(String, Value)> {
    let mut found = Vec::new();
    for kind in ["static", "ticking"] {
        let directory = oracle().join("scenarios").join(kind);
        let entries = std::fs::read_dir(&directory)
            .unwrap_or_else(|e| panic!("{}: {e}", directory.display()));
        for entry in entries {
            let path = entry.expect("a directory entry").path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                let name = path.file_stem().unwrap().to_string_lossy().into_owned();
                found.push((name, json(&path)));
            }
        }
    }
    found.sort_by(|left, right| left.0.cmp(&right.0));
    found
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

/// One column of scenario cells, with the values Java reads as null made null.
fn array<'a>(column_type: &ColumnType, cells: impl Iterator<Item = &'a Value>) -> ArrayRef {
    let values: ArrayRef = match column_type {
        ColumnType::Boolean => Arc::new(cells.map(Value::as_bool).collect::<BooleanArray>()),
        ColumnType::Byte => Arc::new(cells.map(narrow).collect::<Int8Array>()),
        ColumnType::Short => Arc::new(cells.map(narrow).collect::<Int16Array>()),
        ColumnType::Int => Arc::new(cells.map(narrow).collect::<Int32Array>()),
        ColumnType::Long => Arc::new(cells.map(int_cell).collect::<Int64Array>()),
        ColumnType::Float => Arc::new(
            cells
                .map(|cell| float_cell(cell).map(|v| v as f32))
                .collect::<Float32Array>(),
        ),
        ColumnType::Double => Arc::new(cells.map(float_cell).collect::<Float64Array>()),
        ColumnType::String => Arc::new(cells.map(Value::as_str).collect::<StringArray>()),
        ColumnType::Instant => Arc::new(
            cells
                .map(int_cell)
                .collect::<TimestampNanosecondArray>()
                .with_timezone("UTC"),
        ),
        other => panic!("no scenario column of type {other:?}"),
    };
    sentinels_to_nulls(&values).expect("a column with sentinels")
}

/// The rows of a scenario as a batch of the given columns.
fn batch(columns: &[(String, ColumnType)], rows: &[Value]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| Field::new(name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, (_, column_type))| array(column_type, rows.iter().map(|row| &row[index])))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch")
}

/// A table of a scenario as far as this replay got.
#[derive(Clone)]
enum Made {
    /// An input table, with its columns and key columns.
    Input(Arc<dyn Table>, Vec<(String, ColumnType)>, Vec<String>),
    /// The result of one of this module's operations.
    Result(Arc<dyn Table>),
    /// One of this module's operations refused the request.
    Refused(String),
    /// Made by an operation of another module, or from such a table, or an input this
    /// replay cannot make.
    NotRun(String),
}

/// What Java recorded for one table at one step.
enum Recorded {
    Table(RecordBatch, BTreeMap<String, String>),
    Failure,
}

fn recorded(scenario: &str, entry: &Value) -> Recorded {
    if entry.get("failure").is_some() {
        return Recorded::Failure;
    }
    let path = oracle()
        .join("corpus")
        .join(scenario)
        .join(text(&entry["file"]));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let reader = FileReader::try_new(file, None).expect("an Arrow IPC file");
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|batch| batch.expect("a batch")).collect();
    let batch = arrow::compute::concat_batches(&schema, &batches).expect("batches of one schema");
    let metadata = schema.metadata().clone().into_iter().collect();
    Recorded::Table(batch, metadata)
}

/// How `table` differs from what Java recorded, if it does.
fn difference(table: &Result<Arc<dyn Table>, String>, expected: &Recorded) -> Option<String> {
    let snapshot = table
        .as_ref()
        .map_err(String::clone)
        .and_then(|table| TableSnapshot::current(&**table).map_err(|e| e.to_string()));
    let (batch, metadata) = match (expected, &snapshot) {
        (Recorded::Failure, Err(_)) => return None,
        (Recorded::Failure, Ok(_)) => return Some("Java failed and this did not".to_string()),
        (Recorded::Table(..), Err(e)) => return Some(format!("failed where Java did not: {e}")),
        (Recorded::Table(batch, metadata), Ok(_)) => (batch, metadata),
    };
    let (table, snapshot) = (table.as_ref().unwrap(), snapshot.unwrap());

    let describe = |names: Vec<String>, types: Vec<DataType>| {
        let columns = names.iter().zip(&types).map(|(n, t)| format!("{n}: {t}"));
        columns.collect::<Vec<_>>().join(", ")
    };
    let fields = batch.schema().fields().clone();
    let expected_columns = describe(
        fields.iter().map(|f| f.name().clone()).collect(),
        fields.iter().map(|f| f.data_type().clone()).collect(),
    );
    let definition = table.definition();
    let actual_columns = describe(
        definition
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect(),
        definition
            .columns()
            .iter()
            .map(|c| c.column_type.arrow_type())
            .collect(),
    );
    if expected_columns != actual_columns {
        return Some(format!(
            "columns [{actual_columns}], Java has [{expected_columns}]"
        ));
    }

    let attributes = table.attributes();
    let flags = [
        ("IsFlat", table.is_flat()),
        ("AddOnly", attributes.add_only),
        ("AppendOnly", attributes.append_only),
        ("BlinkTable", attributes.blink),
    ];
    for (name, actual) in flags {
        let key = format!("deephaven:attribute.{name}");
        let expected = metadata.get(&key).is_some_and(|value| value == "true");
        if actual != expected {
            return Some(format!("{name} is {actual}, Java has {expected}"));
        }
    }

    if snapshot.rows.size() as usize != batch.num_rows() {
        let (actual, expected) = (snapshot.rows.size(), batch.num_rows());
        return Some(format!("{actual} rows, Java has {expected}"));
    }
    for (index, column) in snapshot.columns.iter().enumerate() {
        if column.data_type() != batch.column(index).data_type() {
            return Some(format!("column {index} is read as {}", column.data_type()));
        }
        let (actual, expected) = (cells(column), cells(batch.column(index)));
        if actual != expected {
            let name = &definition.columns()[index].name;
            return Some(format!("{name} is {actual:?}, Java has {expected:?}"));
        }
    }
    None
}

fn names_of(value: &Value) -> Vec<&str> {
    list(value).iter().map(text).collect()
}

/// Runs one operation of a scenario on the tables made so far.
fn apply(operation: &Value, tables: &BTreeMap<String, Made>) -> Made {
    let kind = text(&operation["op"]);
    if kind == "empty_table" {
        let size = operation["size"].as_u64().expect("a size");
        return Made::Result(empty_table(size));
    }
    if !OURS.contains(&kind) {
        return Made::NotRun(kind.to_string());
    }
    let source = match &tables[text(&operation["source"])] {
        Made::Input(table, ..) | Made::Result(table) => table,
        // A table made from a refused one is refused too.
        Made::Refused(why) => return Made::Refused(why.clone()),
        Made::NotRun(why) => return Made::NotRun(why.clone()),
    };
    // An operation may report a construct that is not ported yet by panicking.
    let result = catch_unwind(AssertUnwindSafe(|| match kind {
        "view" => view(source, &names_of(&operation["formulas"]), &Compiler),
        "update_view" => update_view(source, &names_of(&operation["formulas"]), &Compiler),
        "lazy_update" => lazy_update(source, &names_of(&operation["formulas"]), &Compiler),
        "select" => select(source, &names_of(&operation["formulas"]), &Compiler),
        "update" => update(source, &names_of(&operation["formulas"]), &Compiler),
        _ => drop_columns(source, &names_of(&operation["columns"])),
    }));
    let result = match result {
        Ok(result) => result,
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("a panic");
            match message.strip_prefix("unported: ") {
                Some(construct) => return Made::NotRun(format!("unported {construct}")),
                None => std::panic::resume_unwind(panic),
            }
        }
    };
    match result {
        Ok(table) => {
            // Java sends these types in another form than the engine stores them in
            // (`oxhaven_server::wire_columns`), so the recorded tables are compared by
            // the replay gate instead.
            let sent_otherwise = table.definition().columns().iter().find(|column| {
                matches!(
                    column.column_type,
                    ColumnType::LocalDate
                        | ColumnType::LocalDateTime
                        | ColumnType::Period
                        | ColumnType::ZonedDateTime
                        | ColumnType::DayOfWeek
                )
            });
            match sent_otherwise {
                Some(column) => Made::NotRun(format!(
                    "column {} of type {:?}, sent in another form",
                    column.name, column.column_type
                )),
                None => Made::Result(table),
            }
        }
        // What the formula compiler does not support yet is not a refusal of Java's.
        Err(SelectError::Formula {
            column,
            error: FormulaError::Unsupported(_),
        }) => Made::NotRun(format!("unsupported formula {column}")),
        Err(error) => Made::Refused(error.to_string()),
    }
}

#[derive(Default)]
struct Outcome {
    /// `scenario.table`, with the number of steps compared.
    compared: BTreeMap<String, usize>,
    /// `scenario.table: why`, for results of this module's operations that were not run.
    not_run: Vec<String>,
    differences: Vec<String>,
}

fn replay(name: &str, scenario: &Value, outcome: &mut Outcome) {
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let mut tables: BTreeMap<String, Made> = BTreeMap::new();
    for input in list(&scenario["inputs"]) {
        if input["ticking"]["blink"] == true {
            // This replay does not drive a blink input table.
            let made = Made::NotRun("blink input".to_string());
            tables.insert(text(&input["name"]).to_string(), made);
            continue;
        }
        let columns: Vec<(String, ColumnType)> = list(&input["columns"])
            .iter()
            .map(|c| (text(&c["name"]).to_string(), column_type(text(&c["type"]))))
            .collect();
        let definition = TableDefinition::new(
            columns
                .iter()
                .map(|(name, column_type)| ColumnDefinition::new(name, column_type.clone()))
                .collect(),
        )
        .expect("distinct column names");
        let rows = batch(&columns, list(&input["rows"]));
        let (table, keys): (Arc<dyn Table>, Vec<String>) = match input.get("ticking") {
            None => (in_memory_table(definition, &[rows]).unwrap(), Vec::new()),
            Some(ticking) => {
                let keys = names_of(&ticking["key_columns"]);
                let table: Arc<dyn Table> = if keys.is_empty() {
                    append_only_input_table(graph.clone(), definition, Some(&rows)).unwrap()
                } else {
                    keyed_input_table(graph.clone(), definition, &keys, Some(&rows)).unwrap()
                };
                (table, keys.iter().map(|key| key.to_string()).collect())
            }
        };
        let made = Made::Input(table, columns, keys);
        tables.insert(text(&input["name"]).to_string(), made);
    }

    let mut ours = Vec::new();
    for operation in list(&scenario["operations"]) {
        let made = apply(operation, &tables);
        let table_name = text(&operation["name"]).to_string();
        if OURS.contains(&text(&operation["op"])) {
            ours.push(table_name.clone());
        }
        tables.insert(table_name, made);
    }

    // Every refreshing result is also watched for consistency. A view whose formula can
    // fail on a read cannot be watched: the checker reads every row.
    let mut checkers = Vec::new();
    for table_name in &ours {
        if let Made::Result(table) = &tables[table_name] {
            if table.is_refreshing() && name != "ticking_error_after_step" {
                checkers.push(ConsistencyChecker::attach(table.clone()));
            }
        }
    }

    let manifest = json(&oracle().join("corpus").join(name).join("manifest.json"));
    let entries = list(&manifest["entries"]);
    let steps = list(&scenario["steps"]);
    for step in 0..=steps.len() {
        if step > 0 {
            for action in list(&steps[step - 1]["actions"]) {
                let (table, columns, keys) = match &tables[text(&action["table"])] {
                    Made::Input(table, columns, keys) => (table, columns, keys),
                    // An input that was not made; what is made from it is not run either.
                    Made::NotRun(_) => continue,
                    _ => panic!("{name}: an action on a table that is not an input"),
                };
                let input = table.input_table().expect("an input table");
                let rows = list(&action["rows"]);
                match text(&action["action"]) {
                    "add" => input.add(batch(columns, rows)).unwrap(),
                    "delete" => {
                        let key_columns: Vec<(String, ColumnType)> = keys
                            .iter()
                            .map(|key| columns.iter().find(|c| &c.0 == key).unwrap().clone())
                            .collect();
                        input.delete(batch(&key_columns, rows)).unwrap()
                    }
                    other => panic!("{name}: unknown action {other}"),
                }
            }
            for checker in &checkers {
                if let Err(e) = checker.verify() {
                    outcome.differences.push(format!("{name} step {step}: {e}"));
                }
            }
        }
        for entry in entries.iter().filter(|e| e["step"] == step) {
            let table_name = text(&entry["table"]);
            if !ours.iter().any(|ours| ours == table_name) {
                continue;
            }
            let label = format!("{name}.{table_name}");
            let table = match &tables[table_name] {
                Made::Result(table) => Ok(table.clone()),
                Made::Refused(why) => Err(why.clone()),
                Made::NotRun(why) => {
                    if step == 0 {
                        outcome.not_run.push(format!("{label}: {why}"));
                    }
                    continue;
                }
                Made::Input(..) => unreachable!(),
            };
            match difference(&table, &recorded(name, entry)) {
                Some(difference) => outcome
                    .differences
                    .push(format!("{label} step {step}: {difference}")),
                None => *outcome.compared.entry(label).or_default() += 1,
            }
        }
    }

    if name == "ticking_error_after_step" {
        // Java does not record the stored column that failed at the third step, because it
        // goes on answering for a failed table. Here the table is failed.
        let Made::Result(remainder) = &tables["remainder"] else {
            panic!("remainder was not made");
        };
        assert!(remainder.is_failed());
    }
}

#[test]
fn results_match_the_ones_recorded_from_java() {
    let mut outcome = Outcome::default();
    let scenarios = scenarios();
    assert_eq!(scenarios.len(), 93);
    for (name, scenario) in &scenarios {
        replay(name, scenario, &mut outcome);
    }
    assert_eq!(
        outcome.differences, KNOWN_DIFFERENCES,
        "differences from Java"
    );

    let compared: Vec<String> = outcome
        .compared
        .iter()
        .map(|(table, steps)| format!("{table} x{steps}"))
        .collect();
    // Every table with the number of steps compared, the initial state included.
    assert_eq!(
        compared,
        [
            "column_arrays.before x1",
            "column_arrays.fails_no_column x1",
            "column_arrays.fails_shifted_name x1",
            "column_arrays.stored x1",
            "drop_columns.most x1",
            "drop_columns.one x1",
            "drop_columns.some x1",
            "empty_table_and_meta.five_columns x1",
            "empty_table_and_meta.five_view x1",
            "empty_table_and_meta.none_columns x1",
            "empty_table_ops.d x1",
            "empty_table_ops.u x1",
            "empty_table_ops.v x1",
            "error_formula.div_ok x1",
            "error_formula.rem_long x1",
            "error_formula.rem_select x1",
            "error_formula.rem_update x1",
            "error_formula.rem_view x1",
            "error_formula.syntax x1",
            "error_formula.type_error x1",
            "error_formula.unknown_function x1",
            "error_types_and_matches.duplicate_output x1",
            "error_unknown_column.bad_drop x1",
            "error_unknown_column.bad_view x1",
            "lazy_update_update_view.lazy x1",
            "lazy_update_update_view.lazy_chain x1",
            "lazy_update_update_view.lazy_empty x1",
            "lazy_update_update_view.lazy_of_uv x1",
            "lazy_update_update_view.lazy_remainder x1",
            "lazy_update_update_view.lazy_replace x1",
            "lazy_update_update_view.lazy_strings x1",
            "lazy_update_update_view.lazy_unknown x1",
            "lazy_update_update_view.uv x1",
            "lazy_update_update_view.uv_chain x1",
            "lazy_update_update_view.uv_duplicate x1",
            "lazy_update_update_view.uv_empty x1",
            "lazy_update_update_view.uv_of_lazy x1",
            "lazy_update_update_view.uv_remainder x1",
            "lazy_update_update_view.uv_replace x1",
            "lazy_update_update_view.uv_strings x1",
            "lazy_update_update_view.uv_unknown x1",
            "merge_column_order.b_as_a x1",
            "null_sentinel_inputs.as_is x1",
            "null_sentinel_inputs.plus x1",
            "select_distinct.without_key x1",
            "select_reorder_rename.chain x1",
            "select_reorder_rename.sel x1",
            "select_reorder_rename.upd_replace x1",
            "select_reorder_rename.vw x1",
            "ticking_column_arrays.after x6",
            "ticking_column_arrays.appended_after x6",
            "ticking_column_arrays.appended_before x6",
            "ticking_column_arrays.before x6",
            "ticking_column_arrays.fails_after_shift x6",
            "ticking_column_arrays.fails_call x6",
            "ticking_column_arrays.fails_fixed x6",
            "ticking_column_arrays.fails_position x6",
            "ticking_column_arrays.fails_whole x6",
            "ticking_column_arrays.lazy x6",
            "ticking_column_arrays.same_distance x6",
            "ticking_column_arrays.stored x6",
            "ticking_column_arrays.update_viewed x6",
            "ticking_column_arrays.viewed x6",
            "ticking_column_arrays.with_current x6",
            "ticking_empty_start.d x5",
            "ticking_empty_start.sel x5",
            "ticking_empty_start.u x5",
            "ticking_empty_start.v x5",
            "ticking_error_after_step.remainder_view x6",
            "ticking_lazy_update_update_view.lazy x7",
            "ticking_lazy_update_update_view.lazy_of_uv x7",
            "ticking_lazy_update_update_view.lazy_replace x7",
            "ticking_lazy_update_update_view.uv x7",
            "ticking_lazy_update_update_view.uv_of_lazy x7",
            "ticking_lazy_update_update_view.uv_replace x7",
            "ticking_update_by_append.computed x6",
            "update_float_special.u x1",
            "update_int_overflow.u x1",
            "update_null_propagation.u x1",
            "view_arithmetic.floats x1",
            "view_arithmetic.ints x1",
            "view_arithmetic.small x1",
        ],
        "compared"
    );
    // Results of this module's operations that need an operation of another module, the
    // one named (`ticking_time_table.numbered` is made from a `head` of a `time_table`).
    assert_eq!(
        outcome.not_run,
        [
            "column_arrays.sparse_before: where",
            "column_arrays.computed: where",
            "column_arrays.whole: where",
            "column_arrays.viewed: where",
            "column_arrays.update_viewed: where",
            "column_arrays.lazy: where",
            "column_arrays.fails_index_type: unsupported formula P = X_[D]",
            "error_formula.rem_ok: where",
            "error_unknown_column.after_bad_sort: sort",
            "pipeline.priced: where",
            "pipeline.slim: natural_join",
            "ticking_column_arrays.blink_before: blink input",
            "ticking_column_arrays.blink_whole: blink input",
            "ticking_time_table.numbered: head",
            "time_types.base: column D of type LocalDate, sent in another form",
            "time_types.sort_d: sort",
            "time_types.sort_lt: sort",
            "time_types.sort_dur: sort",
            "time_types.sort_w: sort",
            "time_types.sort_z: sort",
            "time_types.sort_z_desc: sort",
            "time_types.where_d: where",
            "time_types.where_d_in: where",
            "time_types.where_d_range: where",
            "time_types.where_lt: where",
            "time_types.where_dur: where",
            "time_types.where_w: where",
            "time_types.where_w_ne: where",
            "time_types.where_z: where",
            "time_types.where_z_le: where",
            "time_types.where_z_gt: where",
            "time_types.join_d: natural_join",
            "time_types.by_z: agg_by",
            "time_types.join_z: natural_join",
            "time_types.join_w: exact_join",
            "time_types.join_dur: join",
            "time_types.distinct_w_dur: select_distinct",
            "time_types.values_by_w: agg_by",
            "time_types.values_by_k: agg_by",
            "time_types.values_all: agg_by",
        ],
        "not run"
    );
}
