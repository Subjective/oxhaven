//! Scenarios of `cross_join`, checked against the results recorded from the Java server.
//! There are two sets:
//!
//! - the oracle's scenarios (`oracle/scenarios`, recorded in `oracle/corpus`, see
//!   `oracle/README.md`), as far as they can be built from cross joins;
//! - the scenarios of `testdata/scenarios`, in the same format and recorded in
//!   `testdata/corpus` by `testdata/record.py`.
//!
//! Every scenario is loaded. A table of a scenario is built when its operation is a cross
//! join (`join`) and both tables it is defined from could be built; the other tables need
//! operations of other tasks and are counted as not run. Every result that could be built
//! is compared with its recording at every step by the rules of `oracle/harness/rules.py`
//! that apply here: column names, order and types, the row count, the cells by position
//! (floats bit for bit, every NaN equal to every NaN), and the attributes `IsFlat`,
//! `AddOnly`, `AppendOnly` and `BlinkTable`. Row keys are not compared (`PARITY.md` A4);
//! `java_updates.rs` does that. Where Java refused the operation, the operation must be
//! refused here.
//!
//! The Python client sends no reserve of right bits, so the server uses its default,
//! [`DEFAULT_RESERVE_BITS`].
//!
//! A Java table that fails in an update cycle still answers a snapshot, with the rows it
//! had. `testdata/record.py` therefore also records, in `failed.json`, the steps after
//! which each result table had failed. A table must have failed here after exactly those
//! steps, and its rows are then not compared.
//!
//! The loading of scenarios and recordings is that of `ops/join/corpus.rs`.

use super::{cross_join, CrossJoinError, DEFAULT_RESERVE_BITS};
use crate::ops::join::{JoinAddition, JoinError, JoinMatch};
use crate::ops::sort::testing::graph;
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
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn testdata() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops/crossjoin/testdata")
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
    Table(Arc<dyn Table>),
    /// The operation was refused; the gRPC status code Java reports for this refusal.
    Refused(&'static str),
    /// The table needs an operation of another task.
    NotRun,
}

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `cross_join_arguments` and `ticking_cross_join_key_space` of `testdata`: the same code for
/// all of them.
fn code(error: &CrossJoinError) -> &'static str {
    match error {
        CrossJoinError::Join(
            JoinError::NoSuchColumn(_)
            | JoinError::ConflictingColumns(_)
            | JoinError::MismatchedTypes { .. },
        )
        | CrossJoinError::OutOfKeySpace(_) => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

/// `left=right` or one name for both, as the Python client writes a pair of columns.
fn pair(spec: &Value) -> (&str, &str) {
    let spec = text(spec);
    match spec.split_once('=') {
        Some((left, right)) => (left.trim(), right.trim()),
        None => (spec, spec),
    }
}

/// Defines the table of `operation` from `tables`.
fn define(tables: &BTreeMap<String, Built>, operation: &Value) -> Built {
    let Some("join") = operation["op"].as_str() else {
        return Built::NotRun;
    };
    let mut parents = Vec::with_capacity(2);
    for side in ["left", "right"] {
        match &tables[text(&operation[side])] {
            Built::Table(table) => parents.push(table.clone()),
            // A table defined from a refused table is refused the same way.
            Built::Refused(code) => return Built::Refused(code),
            Built::NotRun => return Built::NotRun,
        }
    }
    let on: Vec<JoinMatch> = list(&operation["on"])
        .iter()
        .map(|spec| {
            let (left, right) = pair(spec);
            JoinMatch::new(left, right)
        })
        .collect();
    let add: Vec<JoinAddition> = list(&operation["joins"])
        .iter()
        .map(|spec| {
            let (name, source) = pair(spec);
            JoinAddition::renamed(name, source)
        })
        .collect();
    let result = cross_join(&parents[0], &parents[1], &on, &add, DEFAULT_RESERVE_BITS);
    match result {
        Ok(table) => Built::Table(table),
        Err(error) => Built::Refused(code(&error)),
    }
}

/// The recordings of one scenario.
struct Recording {
    directory: PathBuf,
    manifest: Value,
    /// The `(step, table)` after which Java's table had failed, where that was recorded.
    failed: Option<BTreeSet<(usize, String)>>,
}

impl Recording {
    fn of(corpus: &Path, scenario: &str) -> Self {
        let directory = corpus.join(scenario);
        let manifest = json(&directory.join("manifest.json"));
        let probes = directory.join("failed.json");
        let failed = probes.is_file().then(|| {
            list(&json(&probes)["failed"])
                .iter()
                .map(|probe| {
                    let step = probe["step"].as_u64().expect("a step") as usize;
                    assert_eq!(text(&probe["code"]), "INVALID_ARGUMENT");
                    (step, text(&probe["table"]).to_string())
                })
                .collect()
        });
        Self {
            directory,
            manifest,
            failed,
        }
    }

    fn has_failed(&self, step: usize, table: &str) -> bool {
        let failed = self.failed.as_ref();
        failed.is_some_and(|failed| failed.contains(&(step, table.to_string())))
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
fn compare(place: &str, recording: &Recording, entry: &Value, table: &dyn Table) {
    let (expected, attributes) = recording.table(entry);
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
    /// The number of tables compared with a recorded table, over all steps.
    comparisons: usize,
    /// The number of refusals found where Java refused.
    refusals: usize,
    /// The number of failed tables found where Java's table had failed, over all steps.
    failures: usize,
}

fn run(path: &Path, corpus: &Path, tally: &mut Tally) {
    let scenario = json(path);
    let name = text(&scenario["name"]);
    let recording = Recording::of(corpus, name);
    let graph = graph();

    let mut inputs: BTreeMap<String, Input> = BTreeMap::new();
    let mut tables: BTreeMap<String, Built> = BTreeMap::new();
    for input in list(&scenario["inputs"]) {
        let loaded = load(&graph, input);
        let name = text(&input["name"]).to_string();
        tables.insert(name.clone(), Built::Table(loaded.table.clone()));
        inputs.insert(name, loaded);
    }
    for operation in list(&scenario["operations"]) {
        let built = define(&tables, operation);
        tables.insert(text(&operation["name"]).to_string(), built);
    }

    let results: Vec<&str> = list(&scenario["results"])
        .iter()
        .map(|result| text(&result["table"]))
        // A result that is an input involves no join.
        .filter(|result| !inputs.contains_key(*result))
        .collect();
    for result in &results {
        match &tables[*result] {
            Built::NotRun => tally.not_run.push(format!("{name}/{result}")),
            _ => tally.compared.push(format!("{name}/{result}")),
        }
    }
    let mut check = |step: usize| {
        for result in &results {
            let place = format!("{name}/{result} at step {step}");
            let entry = recording.entry(step, result);
            let failure = &entry["failure"];
            match &tables[*result] {
                Built::NotRun => {}
                Built::Refused(code) => {
                    assert!(!failure.is_null(), "{place}: refused, but Java has a table");
                    assert_eq!(*code, text(&failure["code"]), "{place}: status code");
                    tally.refusals += 1;
                }
                Built::Table(table) => {
                    assert!(failure.is_null(), "{place}: Java refuses with {failure}");
                    if recording.has_failed(step, result) {
                        assert!(table.is_failed(), "{place}: Java's table has failed");
                        tally.failures += 1;
                    } else {
                        assert!(!table.is_failed(), "{place}: the table has failed");
                        compare(&place, &recording, entry, &**table);
                        tally.comparisons += 1;
                    }
                }
            }
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

fn run_all(scenarios: &[PathBuf], corpus: &Path) -> Tally {
    let mut paths = Vec::new();
    for directory in scenarios {
        for entry in std::fs::read_dir(directory).expect("the scenarios") {
            paths.push(entry.expect("a scenario").path());
        }
    }
    paths.sort();
    let mut tally = Tally::default();
    for path in &paths {
        run(path, corpus, &mut tally);
    }
    tally.compared.sort();
    tally
}

#[test]
fn results_match_the_recorded_java_results_of_the_oracle() {
    let scenarios = oracle().join("scenarios");
    let tally = run_all(
        &[scenarios.join("static"), scenarios.join("ticking")],
        &oracle().join("corpus"),
    );
    // Every result table of the corpus that is a cross join of tables that are themselves
    // inputs or such results. Everything else needs another operation.
    let expected = [
        "error_types_and_matches/join_name_clash",
        "join_duplicate_keys/cross",
        "join_duplicate_keys/empty_left",
        "join_duplicate_keys/empty_right",
        "join_duplicate_keys/on_k",
        "join_duplicate_keys/renamed",
        "ticking_join_merge/j",
    ];
    assert_eq!(tally.compared, expected);
    // 5 tables and 1 refusal of static scenarios, and 1 ticking table at 8 steps.
    assert_eq!(
        (tally.comparisons, tally.refusals, tally.failures),
        (5 + 8, 1, 0)
    );
    assert_eq!(tally.not_run.len(), 957, "{:#?}", tally.not_run);
}

#[test]
fn results_match_the_recorded_java_results_of_this_module() {
    let tally = run_all(&[testdata().join("scenarios")], &testdata().join("corpus"));
    assert!(tally.not_run.is_empty(), "{:#?}", tally.not_run);
    // The 193 recorded results: 180 tables and 13 refusals; no table fails in a cycle.
    assert_eq!(
        (tally.comparisons, tally.refusals, tally.failures),
        (180, 13, 0)
    );
    assert_eq!(tally.compared.len(), 45);
}
