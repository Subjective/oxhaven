//! `ungroup` of vector and array columns against what the Java server answered for the
//! fixtures of `fixtures/`, which `fixtures/record.py` documents and records.
//!
//! The sources of a fixture are not computed here: they are built from the recording, with
//! their list columns, and a ticking source is driven by the updates Java's source
//! delivered, at row keys of its own. So a source can be anything Java can make, such as
//! the vectors of a `group_by`, which the engine does not have yet.
//!
//! Each result is compared at every step: the columns' names and types, a vector not being
//! an array, the row count, and the cells by position (floats bit for bit, every NaN equal
//! to every NaN, a null cell not being an empty one, elements in order). Each update of a
//! ticking result is compared as well: the added, removed and modified rows by position,
//! and the modified columns. Row keys and shifts are ours (`PARITY.md` A4). Where Java
//! refused an operation it must be refused here with the same status code.
//!
//! Every ticking result runs with the consistency checker and is recomputed from a static
//! copy of its source after every cycle.

use super::corpus::{code, same_cells};
use super::ungroup;
use crate::ops::sort::testing::{frozen, graph, scripted, static_table};
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampNanosecondArray, UInt64Array,
};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{
    list_of, ConsistencyChecker, Recorder, ScriptedTable, TableChange, TableSnapshot,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

fn list(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

fn text(value: &Value) -> &str {
    value.as_str().expect("a string")
}

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/ops/ungroup/fixtures")
        .join(format!("{name}.json"));
    let file = File::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_reader(file).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The column type a fixture names: a scalar, or `array<T>` or `vector<T>`.
fn column_type(name: &str) -> ColumnType {
    let element = |kind: &str| {
        let rest = name.strip_prefix(kind)?.strip_suffix('>')?;
        Some(column_type(rest))
    };
    if let Some(element) = element("array<") {
        return ColumnType::array(element);
    }
    if let Some(element) = element("vector<") {
        return ColumnType::vector(element);
    }
    match name {
        "bool" => ColumnType::Boolean,
        "int16" => ColumnType::Short,
        "int32" => ColumnType::Int,
        "int64" => ColumnType::Long,
        "float32" => ColumnType::Float,
        "float64" => ColumnType::Double,
        "string" => ColumnType::String,
        "timestamp" => ColumnType::Instant,
        other => panic!("no column type {other}"),
    }
}

fn float(cell: &Value) -> Option<f64> {
    match cell {
        Value::Null => None,
        Value::String(word) => Some(match word.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            other => panic!("no float {other}"),
        }),
        number => Some(number.as_f64().expect("a float")),
    }
}

/// The cells of a recording as an array of `column_type`. A list cell is a JSON list, a
/// null cell `null`.
fn cells(column_type: &ColumnType, values: &[&Value]) -> ArrayRef {
    let ints = || values.iter().map(|cell| cell.as_i64());
    match column_type {
        ColumnType::Boolean => {
            Arc::new(values.iter().map(|c| c.as_bool()).collect::<BooleanArray>())
        }
        ColumnType::Short => Arc::new(ints().map(|c| c.map(|c| c as i16)).collect::<Int16Array>()),
        ColumnType::Int => Arc::new(ints().map(|c| c.map(|c| c as i32)).collect::<Int32Array>()),
        ColumnType::Long => Arc::new(ints().collect::<Int64Array>()),
        ColumnType::Float => {
            let floats = values.iter().map(|c| float(c).map(|c| c as f32));
            Arc::new(floats.collect::<Float32Array>())
        }
        ColumnType::Double => Arc::new(values.iter().map(|c| float(c)).collect::<Float64Array>()),
        ColumnType::String => Arc::new(values.iter().map(|c| c.as_str()).collect::<StringArray>()),
        ColumnType::Instant => {
            let nanos = ints().collect::<TimestampNanosecondArray>();
            Arc::new(nanos.with_timezone("UTC"))
        }
        ColumnType::Vector(element) | ColumnType::Array(element) => {
            let lengths = values.iter().map(|cell| cell.as_array().map(Vec::len));
            let elements = values.iter().filter_map(|cell| cell.as_array()).flatten();
            let elements: Vec<&Value> = elements.collect();
            list_of(column_type, lengths, cells(element, &elements))
        }
        other => panic!("no cells of {other:?}"),
    }
}

/// A table of a recording.
struct Recorded {
    columns: Vec<(String, ColumnType)>,
    values: Vec<ArrayRef>,
    rows: usize,
}

impl Recorded {
    fn of(table: &Value) -> Self {
        let rows = list(&table["rows"]);
        let columns: Vec<(String, ColumnType)> = list(&table["columns"])
            .iter()
            .map(|c| (text(&c["name"]).to_string(), column_type(text(&c["type"]))))
            .collect();
        let values = columns.iter().enumerate().map(|(index, (_, column_type))| {
            let column: Vec<&Value> = rows.iter().map(|row| &row[index]).collect();
            cells(column_type, &column)
        });
        Self {
            values: values.collect(),
            columns,
            rows: rows.len(),
        }
    }

    fn columns(&self) -> Vec<(&str, ColumnType, ArrayRef)> {
        let columns = self.columns.iter().zip(&self.values);
        columns
            .map(|((name, column_type), values)| {
                (name.as_str(), column_type.clone(), values.clone())
            })
            .collect()
    }

    /// The cells of column `index` at `positions`.
    fn at(&self, index: usize, positions: &[usize]) -> ArrayRef {
        let positions: UInt64Array = positions.iter().map(|p| Some(*p as u64)).collect();
        arrow::compute::take(&*self.values[index], &positions, None).expect("rows")
    }
}

fn positions(rows: &Value) -> Vec<usize> {
    let rows = list(rows).iter();
    rows.map(|row| row.as_u64().expect("a position") as usize)
        .collect()
}

/// The distance of the row keys a ticking source starts with, which leaves room for the
/// rows Java adds between two rows.
const GAP: RowKey = 1 << 16;

/// A ticking source: a scripted table that follows what Java's source did.
struct Source {
    script: Arc<ScriptedTable>,
    /// The row keys in order.
    keys: Vec<RowKey>,
}

impl Source {
    fn new(graph: &Arc<dyn UpdateGraph>, recorded: &Recorded) -> Self {
        let keys: Vec<RowKey> = (1..=recorded.rows as RowKey).map(|row| row * GAP).collect();
        let script = scripted(graph, keys.iter().copied(), &recorded.columns());
        Self { script, keys }
    }

    /// Queues the change of an update Java's source delivered, after which the source is
    /// `after`: the removed rows by their position before, the added and modified rows by
    /// their position after, with the cells of `after`; of a modified row the columns Java
    /// reported as modified.
    fn apply(&mut self, update: &Value, after: &Recorded) {
        let removed = positions(&update["removed"]);
        let added = positions(&update["added"]);
        let modified = positions(&update["modified"]);
        let removed_keys: Vec<RowKey> = removed.iter().map(|p| self.keys[*p]).collect();
        let mut kept = self.keys.iter().enumerate();
        let mut kept = kept
            .by_ref()
            .filter(|(position, _)| !removed.contains(position))
            .map(|(_, key)| *key);
        let mut keys: Vec<Option<RowKey>> = (0..after.rows)
            .map(|position| (!added.contains(&position)).then(|| kept.next().expect("a row")))
            .collect();
        assert_eq!(
            kept.next(),
            None,
            "rows that Java's update does not account for"
        );
        // A run of added rows gets keys spread between its neighbours.
        let mut at = 0;
        while at < keys.len() {
            if keys[at].is_some() {
                at += 1;
                continue;
            }
            let run = keys[at..].iter().take_while(|key| key.is_none()).count() as RowKey;
            let before = if at == 0 { 0 } else { keys[at - 1].unwrap() };
            let after_run = keys.get(at + run as usize).copied().flatten();
            let step = after_run.map_or(GAP, |next| (next - before) / (run + 1));
            assert!(
                step >= 1,
                "no row keys left between {before} and {after_run:?}"
            );
            for offset in 0..run {
                keys[at + offset as usize] = Some(before + step * (offset + 1));
            }
            at += run as usize;
        }
        self.keys = keys.into_iter().map(|key| key.unwrap()).collect();
        let mut change = TableChange::new();
        if !removed.is_empty() {
            change = change.remove(removed_keys);
        }
        if !added.is_empty() {
            let values = (0..after.columns.len()).map(|index| after.at(index, &added));
            change = change.add(added.iter().map(|p| self.keys[*p]), values.collect());
        }
        if !modified.is_empty() {
            let names = list(&update["columns"]).iter().map(text);
            let values = names.map(|name| {
                let index = after.columns.iter().position(|(column, _)| column == name);
                let index = index.expect("a column");
                (index, after.at(index, &modified))
            });
            let values: Vec<(usize, ArrayRef)> = values.collect();
            change = change.modify(modified.iter().map(|p| self.keys[*p]), values);
        }
        self.script.push(change);
    }
}

/// How `table` differs from the recorded table `expected`, if it does.
fn difference(expected: &Recorded, table: &dyn Table) -> Option<String> {
    let columns = table.definition().columns().iter();
    let ours: Vec<(String, ColumnType)> = columns
        .map(|c| (c.name.clone(), c.column_type.clone()))
        .collect();
    if ours != expected.columns {
        return Some(format!("columns {ours:?}, Java has {:?}", expected.columns));
    }
    let snapshot = TableSnapshot::current(table).expect("a readable table");
    if snapshot.rows.size() != expected.rows as u64 {
        let (ours, theirs) = (snapshot.rows.size(), expected.rows);
        return Some(format!("{ours} rows, Java has {theirs}"));
    }
    (0..ours.len()).find_map(|index| {
        let (ours, theirs) = (&snapshot.columns[index], &expected.values[index]);
        (!same_cells(theirs, ours)).then(|| {
            let name = &expected.columns[index].0;
            format!("column {name} is {ours:?}, Java has {theirs:?}")
        })
    })
}

/// An update as it is compared: the added, removed and modified positions and the names
/// of the modified columns.
type Shown = ([Vec<usize>; 3], Vec<String>);

/// A result that is a table.
struct Ungrouped {
    name: String,
    table: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Option<Arc<ConsistencyChecker>>,
    /// The number of events of the recorder that were compared.
    seen: usize,
    /// The row set after those events.
    rows: RowSet,
}

impl Ungrouped {
    /// The updates since the last call, without those that only move row keys.
    fn updates(&mut self) -> Vec<Shown> {
        let events = self.recorder.events().split_off(self.seen);
        self.seen += events.len();
        let mut shown = Vec::new();
        for event in events {
            let update = event.unwrap_or_else(|failure| panic!("{}: {failure}", self.name));
            let after = update.apply_to(&self.rows).expect("a valid update");
            let positions = |keys: &RowSet, within: &RowSet| -> Vec<usize> {
                let found = within.invert(keys).expect("rows of the table");
                found.iter().map(|position| position as usize).collect()
            };
            let rows = [
                positions(&update.added, &after),
                positions(&update.removed, &self.rows),
                positions(&update.modified, &after),
            ];
            self.rows = after;
            if rows.iter().all(Vec::is_empty) {
                continue;
            }
            let columns = self.table.definition().columns().iter().enumerate();
            let dirty = columns.filter(|(index, _)| {
                !update.modified.is_empty() && update.modified_columns.contains(*index)
            });
            shown.push((rows, dirty.map(|(_, column)| column.name.clone()).collect()));
        }
        shown
    }
}

/// What a replay compared.
#[derive(Default, PartialEq, Debug)]
struct Compared {
    /// Results that are tables, over all steps.
    tables: usize,
    /// Their rows.
    rows: usize,
    /// Refusals.
    refused: usize,
    /// Updates of results.
    updates: usize,
}

fn replay(name: &str) -> Compared {
    let fixture = fixture(name);
    let java = &fixture["java"];
    let steps = list(&java["steps"]);
    let ticking = list(&fixture["inputs"])
        .iter()
        .any(|input| !input["ticking"].is_null());
    let graph = graph();
    let mut compared = Compared::default();

    let mut tables: BTreeMap<&str, Arc<dyn Table>> = BTreeMap::new();
    let mut sources: BTreeMap<&str, Source> = BTreeMap::new();
    for source in list(&fixture["sources"]).iter().map(text) {
        let recorded = Recorded::of(&steps[0][source]);
        if ticking {
            let scripted = Source::new(&graph, &recorded);
            tables.insert(source, scripted.script.table());
            sources.insert(source, scripted);
        } else {
            tables.insert(source, static_table(&recorded.columns()));
        }
    }

    let mut results = Vec::new();
    for operation in list(&fixture["operations"]) {
        let result = text(&operation["name"]);
        if operation["op"] != "ungroup" {
            assert!(
                tables.contains_key(result),
                "{name}/{result} is not a source"
            );
            continue;
        }
        let source = tables[text(&operation["source"])].clone();
        let null_fill = operation["null_fill"] == true;
        let columns = list(&operation["columns"]).iter();
        let columns: Vec<String> = columns.map(|column| text(column).to_string()).collect();
        let failure = &steps[0][result]["failure"];
        let table = match ungroup(&source, null_fill, &columns) {
            Ok(table) => table,
            Err(error) => {
                let java = failure.as_str();
                assert_eq!(
                    java,
                    Some(code(&error)),
                    "{name}/{result}: refused with {error}"
                );
                compared.refused += 1;
                continue;
            }
        };
        assert!(
            failure.is_null(),
            "{name}/{result}: Java fails with {failure}"
        );
        assert_eq!(table.is_refreshing(), ticking, "{name}/{result}");
        let checker = ticking.then(|| {
            let source = source.clone();
            ConsistencyChecker::attach_with_recompute(table.clone(), move || {
                ungroup(&frozen(&*source), null_fill, &columns).expect("a static result")
            })
        });
        results.push(Ungrouped {
            name: result.to_string(),
            recorder: Recorder::attach(table.clone()),
            rows: (*table.row_set()).clone(),
            table,
            checker,
            seen: 0,
        });
    }
    let named: Vec<&str> = list(&fixture["results"]).iter().map(text).collect();
    assert_eq!(
        results.len() + compared.refused,
        named.len(),
        "{name}: results"
    );

    for (step, recorded) in steps.iter().enumerate() {
        if step > 0 {
            let updates = &java["updates"][step - 1];
            for (source, scripted) in sources.iter_mut() {
                let delivered = list(&updates[*source]);
                assert!(delivered.len() <= 1, "{name}/{source} at step {step}");
                if let Some(update) = delivered.first() {
                    scripted.apply(update, &Recorded::of(&recorded[*source]));
                }
            }
            graph.run_cycle();
        }
        for result in &mut results {
            let place = format!("{name}/{} at step {step}", result.name);
            assert!(!result.table.is_failed(), "{place}: failed");
            if let Some(checker) = &result.checker {
                if let Err(error) = checker.verify() {
                    panic!("{place}: {error}");
                }
            }
            let expected = Recorded::of(&recorded[result.name.as_str()]);
            assert_eq!(difference(&expected, &*result.table), None, "{place}");
            compared.tables += 1;
            compared.rows += expected.rows;
            let found = result.updates();
            if step == 0 {
                assert!(found.is_empty(), "{place}: an update before the first step");
                continue;
            }
            let expected: Vec<Shown> = list(&java["updates"][step - 1][result.name.as_str()])
                .iter()
                .map(|update| {
                    let rows =
                        ["added", "removed", "modified"].map(|rows| positions(&update[rows]));
                    let columns = list(&update["columns"]).iter();
                    (
                        rows,
                        columns.map(|column| text(column).to_string()).collect(),
                    )
                })
                .collect();
            assert_eq!(found, expected, "{place}: the updates");
            compared.updates += found.len();
        }
    }
    compared
}

#[test]
fn static_tables_of_arrays_and_vectors_match_java() {
    let compared = replay("static_lists");
    // `strict`, `nope`, `not_array` and `mixed_bad` are refused.
    let expected = Compared {
        tables: 10,
        rows: 67 - 13,
        refused: 4,
        updates: 0,
    };
    assert_eq!(compared, expected);
}

#[test]
fn ticking_arrays_match_java_update_by_update() {
    let compared = replay("arrays_ticking");
    assert_eq!(compared.tables, 6 * 11);
    assert_eq!(compared.refused, 0);
    assert!(compared.rows > 2000, "{compared:?}");
    assert!(compared.updates > 30, "{compared:?}");
}

#[test]
fn ticking_vectors_of_a_group_by_match_java_update_by_update() {
    let compared = replay("vectors_ticking");
    assert_eq!(compared.tables, 4 * 10);
    assert_eq!(compared.refused, 0);
    assert!(compared.rows > 150, "{compared:?}");
    assert_eq!(compared.updates, 4 * 9);
}
