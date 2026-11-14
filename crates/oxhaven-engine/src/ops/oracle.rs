//! The replay of the oracle's scenarios (`oracle/scenarios`) against the results recorded
//! from the Java server (`oracle/corpus`, see `oracle/README.md`), shared by the corpus
//! tests of the operation families.
//!
//! Every scenario is loaded. A table of a scenario is built when its operation is one of
//! those the test names and every table it is defined from could be built, or when it is a
//! `time_table`, built on a fixed clock; the other tables need operations of other tests,
//! or an input this replay cannot make, and are counted as not run. Every result that could
//! be built is compared with its recording at every step by the rules of
//! `oracle/harness/rules.py` that apply here: column names, order and types, the row count,
//! the cells by position (floats bit for bit, every NaN equal to every NaN), and the
//! attributes `IsFlat`, `AddOnly`, `AppendOnly` and `BlinkTable`. Where Java refused the
//! operation, the operation must be refused here with the same status code.

use crate::graph::SteppedUpdateGraph;
use crate::table::{
    append_only_input_table, in_memory_table, keyed_input_table, time_table, TimeSource,
    TimeTableOptions,
};
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

fn oracle() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../oracle")
}

fn json(path: &Path) -> Value {
    let file = File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

pub(crate) fn list(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

pub(crate) fn text(value: &Value) -> &str {
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
pub(crate) enum Built {
    Table(Arc<dyn Table>),
    /// The operation was refused; the gRPC status code Java reports for this refusal.
    Refused(&'static str),
    /// The table was not built: the operation of another test that it needs, or the kind
    /// of input this replay cannot make.
    NotRun(String),
}

/// The clock of the scenarios' time tables. Every scenario reads rows of a time table that
/// are due long before this time, so any later time gives the same results; Java ran at the
/// time of the recording.
const NOW: &str = "2025-01-01T00:00:00Z";

struct FixedClock(i64);

impl TimeSource for FixedClock {
    fn now_nanos(&self) -> i64 {
        self.0
    }
}

/// The nanoseconds of a period written as hours, minutes or seconds (`PT1H`), or `None`
/// for any other text, which the gRPC handler parses.
fn period_nanos(text: &str) -> Option<i64> {
    let digits = text.strip_prefix("PT")?;
    let (number, unit) = digits.split_at(digits.len().checked_sub(1)?);
    let seconds = match unit {
        "H" => 3600,
        "M" => 60,
        "S" => 1,
        _ => return None,
    };
    Some(number.parse::<i64>().ok()? * seconds * 1_000_000_000)
}

/// The nanoseconds since the epoch of a UTC time written `YYYY-MM-DDTHH:MM:SSZ`, or `None`
/// for any other text, which the gRPC handler parses.
fn instant_nanos(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let separators = [
        (4, b'-'),
        (7, b'-'),
        (10, b'T'),
        (13, b':'),
        (16, b':'),
        (19, b'Z'),
    ];
    if bytes.len() != 20 || separators.iter().any(|&(at, byte)| bytes[at] != byte) {
        return None;
    }
    let field = |range: std::ops::Range<usize>| text[range].parse::<i64>().ok();
    let (year, month, day) = (field(0..4)?, field(5..7)?, field(8..10)?);
    // Days from 1970-01-01 to the date, in the proleptic Gregorian calendar.
    let (year, month) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = year.div_euclid(400);
    let of_era = year - era * 400;
    let of_year = (153 * month + 2) / 5 + day - 1;
    let of_cycle = of_era * 365 + of_era / 4 - of_era / 100 + of_year;
    let days = era * 146_097 + of_cycle - 719_468;
    let seconds = days * 86_400 + field(11..13)? * 3600 + field(14..16)? * 60 + field(17..19)?;
    Some(seconds * 1_000_000_000)
}

/// A `time_table` of a scenario on the clock [`NOW`], or why it is not made.
fn time_table_of(graph: &Arc<dyn UpdateGraph>, operation: &Value) -> Built {
    let period = period_nanos(text(&operation["period"]));
    let start = instant_nanos(text(&operation["start_time"]));
    let (Some(period_nanos), Some(start)) = (period, start) else {
        return Built::NotRun("a time the gRPC handler parses".to_string());
    };
    let options = TimeTableOptions {
        start_nanos: Some(start),
        period_nanos,
        blink: operation["blink"] == true,
    };
    let clock = Arc::new(FixedClock(instant_nanos(NOW).expect("a time")));
    let table = time_table(graph.clone(), clock, options).expect("a time table");
    Built::Table(table)
}

/// Builds the table of one of the test's operations from the operation and the tables it
/// is defined from, in the order of its keys `source` (or `sources`, or `left`), `trigger`
/// and `right`.
pub(crate) type Define<'a> = &'a dyn Fn(&Value, &[Arc<dyn Table>]) -> Built;

/// The names of the tables `operation` is defined from, in the order [`Define`] takes them.
fn sources(operation: &Value) -> Vec<&str> {
    if !operation["sources"].is_null() {
        return list(&operation["sources"]).iter().map(text).collect();
    }
    ["source", "left", "trigger", "right"]
        .iter()
        .filter(|key| !operation[**key].is_null())
        .map(|key| text(&operation[*key]))
        .collect()
}

/// Defines the table of `operation` from `tables`. An operation the test does not name is
/// not run, whatever its keys are.
fn build(
    graph: &Arc<dyn UpdateGraph>,
    tables: &BTreeMap<String, Built>,
    operation: &Value,
    ours: &[&str],
    define: Define,
) -> Built {
    let kind = text(&operation["op"]);
    if kind == "time_table" {
        return time_table_of(graph, operation);
    }
    if !ours.contains(&kind) {
        return Built::NotRun(kind.to_string());
    }
    let mut parents = Vec::new();
    for source in sources(operation) {
        match &tables[source] {
            Built::Table(table) => parents.push(table.clone()),
            // A table defined from a refused table is refused the same way.
            Built::Refused(code) => return Built::Refused(code),
            Built::NotRun(why) => return Built::NotRun(why.clone()),
        }
    }
    define(operation, &parents)
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

/// How `table` differs from what Java returned, if it does: the first difference found.
fn difference(recording: &Recording, entry: &Value, table: &dyn Table) -> Option<String> {
    let (expected, attributes) = recording.table(entry);
    let schema = expected.schema();
    let columns = table.definition().columns();
    let ours: Vec<(&str, DataType)> = columns
        .iter()
        .map(|c| (c.name.as_str(), c.column_type.arrow_type()))
        .collect();
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
    if snapshot.rows.size() != expected.num_rows() as u64 {
        let (ours, theirs) = (snapshot.rows.size(), expected.num_rows());
        return Some(format!("{ours} rows, Java has {theirs}"));
    }
    for (index, column) in columns.iter().enumerate() {
        if !same_cells(expected.column(index), &snapshot.columns[index]) {
            return Some(format!(
                "column {} is {:?}, Java has {:?}",
                column.name,
                snapshot.columns[index],
                expected.column(index)
            ));
        }
    }
    let found = table.attributes();
    let ours = [
        table.is_flat(),
        found.add_only,
        found.append_only,
        found.blink,
    ];
    (ours != attributes).then(|| {
        format!("IsFlat, AddOnly, AppendOnly, BlinkTable are {ours:?}, Java has {attributes:?}")
    })
}

/// What was compared and what was not.
#[derive(Default)]
pub(crate) struct Tally {
    /// `scenario/table`, for every result that was built and agrees with Java at every
    /// step.
    pub(crate) compared: Vec<String>,
    /// The number of results that agree, over all steps.
    pub(crate) comparisons: usize,
    /// `scenario/table at step N: how`, for every result that was built and differs.
    pub(crate) differences: Vec<String>,
    /// `scenario/table: why`, for the results of the test's operations that were not
    /// built.
    pub(crate) not_run: Vec<String>,
    /// The number of results of each other operation; none of them is built.
    pub(crate) others: BTreeMap<String, usize>,
}

fn run(path: &Path, ours: &[&str], define: Define, tally: &mut Tally) {
    let scenario = json(path);
    let name = text(&scenario["name"]);
    let recording = Recording::of(name);
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();

    let mut inputs: BTreeMap<String, Input> = BTreeMap::new();
    let mut tables: BTreeMap<String, Built> = BTreeMap::new();
    // The operation that makes each table; an input has none.
    let mut made_by: BTreeMap<String, &str> = BTreeMap::new();
    for input in list(&scenario["inputs"]) {
        let name = text(&input["name"]).to_string();
        if input["ticking"]["blink"] == true {
            // The engine has no blink input table.
            tables.insert(name, Built::NotRun("blink input".to_string()));
            continue;
        }
        let loaded = load(&graph, input);
        tables.insert(name.clone(), Built::Table(loaded.table.clone()));
        inputs.insert(name, loaded);
    }
    for operation in list(&scenario["operations"]) {
        let built = build(&graph, &tables, operation, ours, define);
        let table = text(&operation["name"]).to_string();
        made_by.insert(table.clone(), text(&operation["op"]));
        tables.insert(table, built);
    }

    // A result that is an input involves none of the operations under test.
    let results: Vec<&str> = list(&scenario["results"])
        .iter()
        .map(|result| text(&result["table"]))
        .filter(|result| made_by.contains_key(*result))
        .collect();
    for result in &results {
        let operation = made_by[*result];
        match &tables[*result] {
            Built::NotRun(_) if !ours.contains(&operation) => {
                *tally.others.entry(operation.to_string()).or_default() += 1;
            }
            Built::NotRun(why) => tally.not_run.push(format!("{name}/{result}: {why}")),
            _ => {}
        }
    }
    let mut differs: Vec<&str> = Vec::new();
    let mut check = |step: usize| {
        for result in &results {
            let entry = recording.entry(step, result);
            let failure = &entry["failure"];
            let difference = match &tables[*result] {
                Built::NotRun(_) => continue,
                Built::Refused(_) if failure.is_null() => {
                    Some("refused, but Java has a table".to_string())
                }
                Built::Refused(code) => (*code != text(&failure["code"]))
                    .then(|| format!("refused with {code}, Java with {}", failure["code"])),
                Built::Table(_) if !failure.is_null() => {
                    Some(format!("Java fails with {}", failure["code"]))
                }
                Built::Table(table) if table.is_failed() => Some("the table has failed".into()),
                Built::Table(table) => difference(&recording, entry, &**table),
            };
            match difference {
                None => tally.comparisons += 1,
                Some(difference) => {
                    differs.push(result);
                    let place = format!("{name}/{result} at step {step}");
                    tally.differences.push(format!("{place}: {difference}"));
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
    for result in &results {
        if !matches!(tables[*result], Built::NotRun(_)) && !differs.contains(result) {
            tally.compared.push(format!("{name}/{result}"));
        }
    }
}

/// Replays every scenario of the oracle, building the operations `ours` with `define`.
pub(crate) fn replay_all(ours: &[&str], define: Define) -> Tally {
    let mut paths = Vec::new();
    for kind in ["static", "ticking"] {
        let directory = oracle().join("scenarios").join(kind);
        for entry in std::fs::read_dir(&directory).expect("the scenarios") {
            paths.push(entry.expect("a scenario").path());
        }
    }
    paths.sort();
    assert_eq!(paths.len(), 83, "the scenarios of the oracle");
    let mut tally = Tally::default();
    for path in &paths {
        run(path, ours, define, &mut tally);
    }
    tally.compared.sort();
    tally
}
