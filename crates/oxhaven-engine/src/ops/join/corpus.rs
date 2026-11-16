//! Scenarios of `natural_join` and `exact_join`, checked against the results recorded from
//! the Java server. There are two sets:
//!
//! - the oracle's scenarios (`oracle/scenarios`, recorded in `oracle/corpus`, see
//!   `oracle/README.md`), as far as they can be built from the two joins;
//! - the scenarios of `testdata/scenarios`, in the same format and recorded in
//!   `testdata/corpus` by `testdata/record.py`.
//!
//! Every scenario is loaded. A table of a scenario is built when its operation is one of
//! the two joins and both tables it is defined from could be built; the other tables need
//! operations of other tasks and are counted as not run. Every result that could be built
//! is compared with its recording at every step by the rules of `oracle/harness/rules.py`
//! that apply here: column names, order and types, the row count, the cells by position
//! (floats bit for bit, every NaN equal to every NaN), and the attributes `IsFlat`,
//! `AddOnly`, `AppendOnly` and `BlinkTable`. Where Java refused the operation, the
//! operation must be refused here.
//!
//! The `InputTable` attribute is compared too, by what Java sends of it: whether the result
//! has it, and which columns are marked as its key and value columns. Here that is the
//! input of the scenario whose write side the result has.
//!
//! A Java table that fails in an update cycle still answers a snapshot, with the rows it
//! had. `testdata/record.py` therefore also records, in `failed.json`, the steps after
//! which each result table had failed. A table must have failed here after exactly those
//! steps, and its rows are then not compared.
//!
//! The loading of scenarios and recordings is that of `ops/sort/corpus.rs`.

use super::{exact_join, natural_join, JoinAddition, JoinError, JoinMatch, NaturalJoinType};
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops/join/testdata")
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
    /// The table needs an operation of another task, or an input this test cannot make: the
    /// operation, or the input's kind.
    NotRun(String),
}

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `error_types_and_matches` of the oracle and in `join_arguments` and `join_zero_keys` of
/// `testdata`: the same code for all of them.
fn code(error: &JoinError) -> &'static str {
    match error {
        JoinError::NoSuchColumn(_)
        | JoinError::ConflictingColumns(_)
        | JoinError::MismatchedTypes { .. }
        | JoinError::DuplicateRightKey(_)
        | JoinError::NoRightRow(_)
        | JoinError::ZeroKeyStaticRight(_) => "INVALID_ARGUMENT",
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
    let op = text(&operation["op"]);
    if !matches!(op, "natural_join" | "exact_join") {
        return Built::NotRun(op.to_string());
    }
    let mut parents = Vec::with_capacity(2);
    for side in ["left", "right"] {
        match &tables[text(&operation[side])] {
            Built::Table(table) => parents.push(table.clone()),
            // A table defined from a refused table is refused the same way.
            Built::Refused(code) => return Built::Refused(code),
            Built::NotRun(why) => return Built::NotRun(why.clone()),
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
    let result = if text(&operation["op"]) == "exact_join" {
        exact_join(&parents[0], &parents[1], &on, &add)
    } else {
        let join_type = match operation["type"].as_str() {
            None | Some("error_on_duplicate") => NaturalJoinType::ErrorOnDuplicate,
            Some("first_match") => NaturalJoinType::FirstMatch,
            Some("last_match") => NaturalJoinType::LastMatch,
            Some("exactly_one_match") => NaturalJoinType::ExactlyOneMatch,
            Some(other) => panic!("no join type {other}"),
        };
        natural_join(&parents[0], &parents[1], &on, &add, join_type)
    };
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

    /// The recorded table, the attributes in its schema, and its input table.
    fn table(&self, entry: &Value) -> (RecordBatch, [bool; 4], InputColumns) {
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
        let marked = |role: &str| -> Vec<String> {
            let key = format!("deephaven:inputtable.{role}");
            let marked = schema
                .fields()
                .iter()
                .filter(|field| field.metadata().get(&key).is_some_and(|v| v == "true"));
            marked.map(|field| field.name().clone()).collect()
        };
        let input = InputColumns {
            attribute: schema
                .metadata()
                .contains_key("deephaven:unsent.attribute.InputTable"),
            keys: marked("isKey"),
            values: marked("isValue"),
        };
        (table, attributes, input)
    }
}

/// What a schema says of a table's `InputTable` attribute.
///
/// Behavior: `BarrageUtil#attributesToMetadata`, which names an attribute it cannot send, as
/// the input table's updater, under `deephaven:unsent.attribute.<name>`, and
/// `BarrageUtil#columnDefinitionsToFields`, which marks a column that
/// `InputTableUpdater#getKeyNames` has with `deephaven:inputtable.isKey` and one that
/// `InputTableUpdater#getValueNames` has with `deephaven:inputtable.isValue`. A column the
/// input table does not have is marked with neither.
#[derive(PartialEq, Eq, Debug)]
struct InputColumns {
    /// Whether the table has the attribute.
    attribute: bool,
    /// The columns marked as keys, in the table's order.
    keys: Vec<String>,
    /// The columns marked as values, in the table's order.
    values: Vec<String>,
}

impl InputColumns {
    /// Those of `table`, whose input table, if it has one, is that of one of `inputs`.
    fn of(table: &dyn Table, inputs: &BTreeMap<String, Input>) -> Self {
        let Some(found) = table.input_table() else {
            return Self {
                attribute: false,
                keys: Vec::new(),
                values: Vec::new(),
            };
        };
        let writes_to = |input: &&Input| {
            input.table.input_table().is_some_and(|own| {
                std::ptr::eq(
                    Arc::as_ptr(&own) as *const (),
                    Arc::as_ptr(&found) as *const (),
                )
            })
        };
        let input = inputs
            .values()
            .find(writes_to)
            .expect("the input table of an input of the scenario");
        let keys = found.key_columns();
        let has = |name: &str| input.columns.iter().any(|(column, _)| column == name);
        let (mut key_columns, mut value_columns) = (Vec::new(), Vec::new());
        for column in table.definition().columns() {
            if keys.contains(&column.name) {
                key_columns.push(column.name.clone());
            } else if has(&column.name) {
                value_columns.push(column.name.clone());
            }
        }
        Self {
            attribute: true,
            keys: key_columns,
            values: value_columns,
        }
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
    inputs: &BTreeMap<String, Input>,
) {
    let (expected, attributes, input) = recording.table(entry);
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
    assert_eq!(
        InputColumns::of(table, inputs),
        input,
        "{place}: the InputTable attribute and its key and value columns"
    );
}

/// What was compared and what was not, as `scenario/table`.
#[derive(Default)]
struct Tally {
    compared: Vec<String>,
    /// `scenario/table: why`, for the joins that are not built.
    not_run: Vec<String>,
    /// The number of results of each other operation; none of them is built.
    others: BTreeMap<String, usize>,
    /// The number of tables compared with a recorded table, over all steps.
    comparisons: usize,
    /// The number of refusals found where Java refused.
    refusals: usize,
    /// The number of failed tables found where Java's table had failed, over all steps.
    failures: usize,
    /// The number of scenarios run.
    scenarios: usize,
}

fn run(path: &Path, corpus: &Path, tally: &mut Tally) {
    let scenario = json(path);
    let name = text(&scenario["name"]);
    let recording = Recording::of(corpus, name);
    let graph = graph();

    let mut inputs: BTreeMap<String, Input> = BTreeMap::new();
    let mut tables: BTreeMap<String, Built> = BTreeMap::new();
    for input in list(&scenario["inputs"]) {
        let name = text(&input["name"]).to_string();
        if input["ticking"]["blink"] == true {
            // This replay does not drive a blink input table.
            tables.insert(name, Built::NotRun("blink input".to_string()));
            continue;
        }
        let loaded = load(&graph, input);
        tables.insert(name.clone(), Built::Table(loaded.table.clone()));
        inputs.insert(name, loaded);
    }
    // The operation that makes each table; an input has none.
    let mut made_by: BTreeMap<String, &str> = BTreeMap::new();
    for operation in list(&scenario["operations"]) {
        let built = define(&tables, operation);
        let table = text(&operation["name"]).to_string();
        made_by.insert(table.clone(), text(&operation["op"]));
        tables.insert(table, built);
    }

    let results: Vec<&str> = list(&scenario["results"])
        .iter()
        .map(|result| text(&result["table"]))
        // A result that is an input involves neither join.
        .filter(|result| made_by.contains_key(*result))
        .collect();
    for result in &results {
        match &tables[*result] {
            Built::NotRun(_) if !matches!(made_by[*result], "natural_join" | "exact_join") => {
                *tally
                    .others
                    .entry(made_by[*result].to_string())
                    .or_default() += 1;
            }
            Built::NotRun(why) => tally.not_run.push(format!("{name}/{result}: {why}")),
            _ => tally.compared.push(format!("{name}/{result}")),
        }
    }
    let mut check = |step: usize| {
        for result in &results {
            let place = format!("{name}/{result} at step {step}");
            let entry = recording.entry(step, result);
            let failure = &entry["failure"];
            match &tables[*result] {
                Built::NotRun(_) => {}
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
                        compare(&place, &recording, entry, &**table, &inputs);
                        tally.comparisons += 1;
                    }
                }
            }
        }
    };

    check(0);
    for (index, step) in list(&scenario["steps"]).iter().enumerate() {
        for action in list(&step["actions"]) {
            // An input that was not made takes no actions.
            let Some(input) = inputs.get(text(&action["table"])) else {
                continue;
            };
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
        tally.scenarios += 1;
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
    assert_eq!(tally.scenarios, 85);

    // Every result table of the corpus that is a natural or exact join of tables that are
    // themselves inputs or such results. Everything else needs another operation.
    let expected = [
        "error_types_and_matches/ej_missing_and_duplicate",
        "error_types_and_matches/join_key_types",
        "error_types_and_matches/nj_duplicate_right",
        "error_types_and_matches/nj_exactly_one",
        "error_types_and_matches/nj_first",
        "exact_join/on_k",
        "exact_join/on_k_s",
        "exact_join/renamed",
        "natural_join_duplicate_right/first",
        "natural_join_duplicate_right/last",
        "natural_join_unique/on_k",
        "natural_join_unique/on_k_s",
        "natural_join_unique/renamed",
        "ticking_natural_join/ej",
        "ticking_natural_join/nj",
        "ticking_natural_join/nj_static_left",
        "ticking_natural_join/nj_static_right",
    ];
    assert_eq!(tally.compared, expected);
    // 9 tables and 4 refusals of static scenarios, and 4 ticking tables at 8 steps each.
    assert_eq!(
        (tally.comparisons, tally.refusals, tally.failures),
        (9 + 32, 4, 0)
    );
    // The joins that need an operation of another task, or an input this test cannot make.
    let not_run = [
        "group_by_ungroup/uneven: group_by",
        "pipeline/located: update",
    ];
    assert_eq!(tally.not_run, not_run, "not run");
    // The results of every other operation, none of which is built here.
    let others: Vec<(&str, usize)> = tally
        .others
        .iter()
        .map(|(operation, results)| (operation.as_str(), *results))
        .collect();
    let expected = [
        ("agg_all_by", 43),
        ("agg_by", 70),
        ("aj", 12),
        ("column_statistics", 19),
        ("drop_columns", 8),
        ("empty_table", 2),
        ("filter", 76),
        ("flatten", 7),
        ("group_by", 11),
        ("head", 29),
        ("head_by", 14),
        ("join", 7),
        ("lazy_update", 13),
        ("left_join", 2),
        ("merge", 25),
        ("meta_table", 9),
        ("multi_join", 14),
        ("raj", 12),
        ("range_join", 20),
        ("reverse", 10),
        ("select", 6),
        ("select_distinct", 18),
        ("slice", 24),
        ("snapshot", 4),
        ("snapshot_when", 20),
        ("sort", 57),
        ("tail", 26),
        ("tail_by", 10),
        ("time_table", 2),
        ("ungroup", 15),
        ("update", 39),
        ("update_by", 42),
        ("update_view", 16),
        ("view", 14),
        ("where", 53),
        ("where_in", 20),
    ];
    assert_eq!(others, expected, "results of other operations");
}

#[test]
fn results_match_the_recorded_java_results_of_this_module() {
    let tally = run_all(&[testdata().join("scenarios")], &testdata().join("corpus"));
    assert!(tally.not_run.is_empty(), "{:#?}", tally.not_run);
    // The 402 recorded results: tables, refusals, and tables that had failed in a cycle.
    assert_eq!(
        (tally.comparisons, tally.refusals, tally.failures),
        (303, 34, 65)
    );
    assert_eq!(tally.compared.len(), 101);
}
