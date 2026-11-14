//! The oracle's scenarios, as far as they can be built from `sort`, `head`, `tail` and
//! `merge`, checked against the results recorded from the Java server (`oracle/corpus`,
//! see `oracle/README.md`).
//!
//! Every scenario is loaded. A table of a scenario is built when its operation is one of
//! the four and every table it is defined from could be built; the other tables need
//! operations of other tasks and are counted as not run. Every result that could be built
//! is compared with its recording at every step by the rules of `oracle/harness/rules.py`
//! that apply here: column names, order and types, the row count, the cells by position
//! (floats bit for bit, every NaN equal to every NaN), and the attributes `IsFlat`,
//! `AddOnly`, `AppendOnly` and `BlinkTable`. Where Java refused the operation, the
//! operation must be refused here.

use super::testing::graph;
use super::{head, sort, tail, SortColumn, SortError};
use crate::ops::merge::{merge, MergeError};
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
    Table(Arc<dyn Table>),
    /// The operation was refused; the gRPC status code Java reports for this refusal.
    Refused(&'static str),
    /// The table needs an operation of another task.
    NotRun,
}

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `error_unknown_column` (a sort by a column the table does not have) and
/// `error_types_and_matches` (a merge of tables with different columns).
fn sort_code(error: &SortError) -> &'static str {
    match error {
        SortError::NoSuchColumn(_) => "FAILED_PRECONDITION",
        other => panic!("no recorded status code for {other:?}"),
    }
}

fn merge_code(error: &MergeError) -> &'static str {
    match error {
        MergeError::IncompatibleDefinitions(_) => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

fn sorted_or_refused(result: Result<Arc<dyn Table>, SortError>) -> Built {
    match result {
        Ok(table) => Built::Table(table),
        Err(error) => Built::Refused(sort_code(&error)),
    }
}

/// Defines the table of `operation` from `tables`.
fn define(tables: &BTreeMap<String, Built>, operation: &Value) -> Built {
    let sources: Vec<&str> = match operation["op"].as_str() {
        Some("sort" | "head" | "tail") => vec![text(&operation["source"])],
        Some("merge") => list(&operation["sources"]).iter().map(text).collect(),
        _ => return Built::NotRun,
    };
    let mut parents = Vec::with_capacity(sources.len());
    for source in sources {
        match &tables[source] {
            Built::Table(table) => parents.push(table.clone()),
            // A table defined from a refused table is refused the same way.
            Built::Refused(code) => return Built::Refused(code),
            Built::NotRun => return Built::NotRun,
        }
    }
    let rows = || operation["rows"].as_i64().expect("rows");
    match text(&operation["op"]) {
        "sort" => {
            let columns: Vec<SortColumn> = list(&operation["by"])
                .iter()
                .map(|by| match text(&by["direction"]) {
                    "ascending" => SortColumn::ascending(text(&by["column"])),
                    "descending" => SortColumn::descending(text(&by["column"])),
                    other => panic!("no direction {other}"),
                })
                .collect();
            sorted_or_refused(sort(&parents[0], &columns))
        }
        "head" => sorted_or_refused(head(&parents[0], rows())),
        "tail" => sorted_or_refused(tail(&parents[0], rows())),
        _ => match merge(&parents) {
            Ok(table) => Built::Table(table),
            Err(error) => Built::Refused(merge_code(&error)),
        },
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
        // A result that is an input involves none of the operations under test.
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
                Built::NotRun => continue,
                Built::Refused(code) => {
                    assert!(!failure.is_null(), "{place}: refused, but Java has a table");
                    assert_eq!(*code, text(&failure["code"]), "{place}: status code");
                }
                Built::Table(table) => {
                    assert!(failure.is_null(), "{place}: Java fails with {failure}");
                    assert!(!table.is_failed(), "{place}: the table has failed");
                    compare(&place, &recording, entry, &**table);
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

    // Every result table of the corpus that is a sort, head, tail or merge of tables that
    // are themselves inputs or such results. Everything else needs another operation.
    let expected = [
        "empty_table_ops/h",
        "empty_table_ops/m",
        "empty_table_ops/s",
        "empty_table_ops/tl",
        "error_types_and_matches/merge_different",
        "error_unknown_column/bad_sort",
        "head_tail/h0",
        "head_tail/h3",
        "head_tail/h5_t2",
        "head_tail/h7",
        "head_tail/h99",
        "head_tail/t0",
        "head_tail/t2",
        "head_tail/t7",
        "head_tail/t99",
        "merge_tables/aa",
        "merge_tables/ab",
        "merge_tables/abab",
        "merge_tables/aeb",
        "merge_tables/ba",
        "merge_tables/ee",
        "sort_float_nan/d_down",
        "sort_float_nan/d_up",
        "sort_float_nan/f_down",
        "sort_float_nan/f_up",
        "sort_int_nulls/down",
        "sort_int_nulls/up",
        "sort_multi_column/by_bool",
        "sort_multi_column/by_bool_down",
        "sort_multi_column/s_down_l_up",
        "sort_multi_column/s_then_i",
        "ticking_empty_start/s",
        "ticking_join_merge/m",
        "ticking_join_merge/m_sorted",
        "ticking_update_by_append/first3",
        "ticking_where_sort/by_s",
        "ticking_where_sort/by_v",
        "ticking_where_sort/first2",
        "ticking_where_sort/last2",
        "timestamps/down",
        "timestamps/twice",
        "timestamps/up",
    ];
    assert_eq!(tally.compared, expected);
    // 34 tables of static scenarios at one step, and the ticking ones at every step:
    // 1 at 5 steps, 2 at 8, 1 at 6 and 4 at 8.
    assert_eq!(tally.comparisons, 34 + 5 + 16 + 6 + 32);
    assert_eq!(tally.not_run.len(), 145, "{:#?}", tally.not_run);
}
