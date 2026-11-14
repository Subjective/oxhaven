//! Cross joins driven with hand-written updates at chosen row keys, checked against the
//! results and updates Java's engine produced for the same tables and updates.
//!
//! Each `testdata/updates/<name>.json` gives the tables, the joins and the steps, and
//! `testdata/record_updates.py` recorded Java's answer in `<name>.java.json`. The oracle's
//! harness sees only snapshots, so these are the only recordings with row keys and updates.
//!
//! Since the row key layout is Java's, the comparison is exact where it can be: the refusal,
//! the result's row set and cells after every step, and the added, removed and modified rows
//! and the modified columns of every update. Shifts are compared by their effect (where each
//! surviving row moves), as `PARITY.md` A4 does not fix how the ranges are cut. A step where
//! Java delivered no update must deliver none here, or an empty one. Once a result has
//! failed, in Java with whatever exception, it must have failed here too; its rows are not
//! compared after that.

use super::{cross_join, CrossJoinError};
use crate::ops::join::{JoinAddition, JoinMatch};
use crate::ops::sort::testing::{cells, graph};
use arrow::array::{ArrayRef, Int64Array};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{
    definition, ConsistencyChecker, Recorder, ReferenceTable, ScriptedTable, TableChange,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn updates_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops/crossjoin/testdata/updates")
}

fn list(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

fn longs(values: &Value) -> ArrayRef {
    Arc::new(Int64Array::from(
        list(values).iter().map(Value::as_i64).collect::<Vec<_>>(),
    ))
}

fn row_keys(values: &Value) -> Vec<RowKey> {
    list(values).iter().map(|v| v.as_i64().unwrap()).collect()
}

fn ranges(values: &Value) -> RowSet {
    RowSet::from_ranges(
        list(values)
            .iter()
            .map(|range| RowRange::new(range[0].as_i64().unwrap(), range[1].as_i64().unwrap())),
    )
}

fn names(values: &Value) -> Vec<String> {
    list(values)
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

/// A source table of the scenario: static, or refreshing and driven by a script.
struct Source {
    table: Arc<dyn Table>,
    script: Option<Arc<ScriptedTable>>,
    columns: Vec<String>,
}

fn source(graph: &Arc<dyn oxhaven_contracts::graph::UpdateGraph>, spec: &Value) -> Source {
    let columns = names(&spec["columns"]);
    let types: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|name| (name.as_str(), ColumnType::Long))
        .collect();
    let rows = RowSet::from_keys(row_keys(&spec["keys"]));
    let values = list(&spec["values"]).iter().map(longs).collect();
    if spec["refreshing"].as_bool().unwrap_or(false) {
        let script = ScriptedTable::new(graph.clone(), definition(&types), rows, values);
        Source {
            table: script.table(),
            script: Some(script),
            columns,
        }
    } else {
        Source {
            table: ReferenceTable::new_static(definition(&types), rows, values),
            script: None,
            columns,
        }
    }
}

/// The scenario's change of one table, as a [`TableChange`].
fn change(columns: &[String], spec: &Value) -> TableChange {
    let mut change = TableChange::new().remove(row_keys(&spec["removed"]));
    for shift in list(&spec["shifted"]) {
        let [first, last, delta] = [0, 1, 2].map(|i| shift[i].as_i64().unwrap());
        change = change.shift(first, last, delta);
    }
    if let Some(added) = spec.get("added") {
        let values = list(&added["values"]).iter().map(longs).collect();
        change = change.add(row_keys(&added["keys"]), values);
    }
    if let Some(modified) = spec.get("modified") {
        let values = names(&modified["columns"])
            .into_iter()
            .zip(list(&modified["values"]).iter().map(longs))
            .map(|(name, values)| (columns.iter().position(|c| *c == name).unwrap(), values))
            .collect::<Vec<_>>();
        change = change.modify(row_keys(&modified["keys"]), values);
    }
    change
}

/// Java's name of the exception for each refusal.
fn java_class(error: &CrossJoinError) -> &'static str {
    match error {
        CrossJoinError::ReserveBits(_) => "io.deephaven.base.verify.AssertionFailure",
        CrossJoinError::OutOfKeySpace(_) => "io.deephaven.engine.exceptions.OutOfKeySpaceException",
        CrossJoinError::Join(_) => "a join refusal",
    }
}

/// The cells of every column, as Java's recorder writes them: numbers, `null` for a null.
fn snapshot_columns(table: &dyn Table) -> BTreeMap<String, Vec<String>> {
    let definition = table.definition();
    let columns = definition.columns().iter().enumerate();
    columns
        .map(|(index, column)| (column.name.clone(), cells(table, index)))
        .collect()
}

fn java_columns(recorded: &Value) -> BTreeMap<String, Vec<String>> {
    let columns = recorded.as_object().unwrap().iter();
    columns
        .map(|(name, values)| {
            let values = list(values).iter().map(|value| match value.as_i64() {
                Some(value) => value.to_string(),
                None => "null".to_string(),
            });
            (name.clone(), values.collect())
        })
        .collect()
}

/// Where each surviving row of `prev` moves, by Java's shifts.
fn java_moves(prev: &RowSet, removed: &RowSet, shifts: &Value) -> Vec<(RowKey, RowKey)> {
    let shifts: Vec<[i64; 3]> = list(shifts)
        .iter()
        .map(|s| [0, 1, 2].map(|i| s[i].as_i64().unwrap()))
        .collect();
    let moved = |key: RowKey| {
        let shift = shifts
            .iter()
            .find(|[first, last, _]| *first <= key && key <= *last);
        key + shift.map_or(0, |[_, _, delta]| *delta)
    };
    prev.minus(removed)
        .iter()
        .map(|key| (key, moved(key)))
        .collect()
}

fn our_moves(prev: &RowSet, update: &TableUpdate) -> Vec<(RowKey, RowKey)> {
    let survivors = prev.minus(&update.removed);
    let shifted = &update.shifted;
    survivors
        .iter()
        .map(|key| (key, shifted.apply_to_key(key)))
        .collect()
}

fn modified_names(table: &dyn Table, update: &TableUpdate) -> Vec<String> {
    let columns = table.definition().columns().iter().enumerate();
    let mut names: Vec<String> = columns
        .filter(|(index, _)| update.modified_columns.contains(*index))
        .map(|(_, column)| column.name.clone())
        .collect();
    names.sort();
    names
}

/// Runs one scenario and returns the number of steps compared.
fn run(name: &str) -> usize {
    let dir = updates_dir();
    let read = |file: String| -> Value {
        let text = fs::read_to_string(dir.join(&file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("{file}: {e}"))
    };
    let scenario = read(format!("{name}.json"));
    let java = read(format!("{name}.java.json"));
    let graph = graph();
    let sources: BTreeMap<String, Source> = scenario["tables"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(table, spec)| (table.clone(), source(&graph, spec)))
        .collect();

    struct Joined {
        name: String,
        result: Arc<dyn Table>,
        recorder: Arc<Recorder>,
        checker: Arc<ConsistencyChecker>,
    }
    let mut joins = Vec::new();
    for join in list(&scenario["joins"]) {
        let join_name = join["name"].as_str().unwrap().to_string();
        let recorded = &java["joins"][&join_name];
        let on: Vec<JoinMatch> = names(&join["on"])
            .iter()
            .map(|c| JoinMatch::same(c.as_str()))
            .collect();
        let add: Vec<JoinAddition> = names(&join["add"])
            .iter()
            .map(|c| JoinAddition::same(c.as_str()))
            .collect();
        let reserve = join["reserve"].as_i64().unwrap_or(10) as i32;
        let left = sources[join["left"].as_str().unwrap()].table.clone();
        let right = sources[join["right"].as_str().unwrap()].table.clone();
        let result = cross_join(&left, &right, &on, &add, reserve);
        match (result, recorded["refused"].as_str()) {
            (Err(error), Some(refused)) => {
                assert_eq!(java_class(&error), refused, "{name}/{join_name}: {error}");
            }
            (Err(error), None) => panic!("{name}/{join_name}: refused here only: {error}"),
            (Ok(_), Some(refused)) => panic!("{name}/{join_name}: Java refused it: {refused}"),
            (Ok(result), None) => {
                let recorder = Recorder::attach(result.clone());
                let checker = ConsistencyChecker::attach(result.clone());
                joins.push(Joined {
                    name: join_name,
                    result,
                    recorder,
                    checker,
                });
            }
        }
    }

    let steps = list(&scenario["steps"]);
    let mut compared = 0;
    let mut failed: BTreeMap<String, bool> = BTreeMap::new();
    let mut prev_rows: BTreeMap<String, RowSet> = BTreeMap::new();
    for index in 0..=steps.len() {
        let mut seen = BTreeMap::new();
        if index > 0 {
            let step = steps[index - 1].as_object().unwrap();
            for joined in &joins {
                seen.insert(joined.name.clone(), joined.recorder.events().len());
            }
            for (table, spec) in step {
                let source = &sources[table];
                let script = source.script.as_ref().expect("a refreshing table");
                script.push(change(&source.columns, spec));
            }
            graph.run_cycle();
        }
        for joined in &joins {
            let at = format!("{name}/{}, step {index}", joined.name);
            let recorded = &java["joins"][&joined.name]["steps"][index];
            let java_failed = recorded["failed"].as_bool().unwrap();
            if *failed.get(&joined.name).unwrap_or(&false) {
                assert!(java_failed, "{at}: failed here only");
                continue;
            }
            let events = joined.recorder.events();
            let events = &events[seen.get(&joined.name).copied().unwrap_or(events.len())..];
            assert!(events.len() <= 1, "{at}: more than one update");
            if java_failed {
                assert!(
                    joined.result.is_failed(),
                    "{at}: Java failed: {}",
                    recorded["update"]
                );
                assert!(matches!(events, [Err(_)]), "{at}: no failure delivered");
                failed.insert(joined.name.clone(), true);
                continue;
            }
            assert!(
                !joined.result.is_failed(),
                "{at}: failed here only: {events:?}"
            );
            joined.checker.assert_consistent();
            let rows = (*joined.result.row_set()).clone();
            assert_eq!(rows, ranges(&recorded["rows"]), "{at}: rows");
            assert_eq!(
                snapshot_columns(&*joined.result),
                java_columns(&recorded["columns"]),
                "{at}: cells"
            );
            if index > 0 {
                let update = events.first().map(|event| event.clone().unwrap());
                let java_update = &recorded["update"];
                let prev = &prev_rows[&joined.name];
                if java_update.is_null() {
                    let empty = update.as_ref().is_none_or(TableUpdate::is_empty);
                    assert!(empty, "{at}: Java delivered no update; here {update:?}");
                } else {
                    let update = update.unwrap_or_else(|| panic!("{at}: no update delivered"));
                    assert_eq!(update.added, ranges(&java_update["added"]), "{at}: added");
                    assert_eq!(
                        update.removed,
                        ranges(&java_update["removed"]),
                        "{at}: removed"
                    );
                    let modified = ranges(&java_update["modified"]);
                    assert_eq!(update.modified, modified, "{at}: modified");
                    assert_eq!(
                        modified_names(&*joined.result, &update),
                        names(&java_update["modified_columns"]),
                        "{at}: modified columns"
                    );
                    assert_eq!(
                        our_moves(prev, &update),
                        java_moves(prev, &update.removed, &java_update["shifted"]),
                        "{at}: shifts {:?}, Java {}",
                        update.shifted,
                        java_update["shifted"]
                    );
                }
            }
            prev_rows.insert(joined.name.clone(), rows);
            compared += 1;
        }
    }
    compared
}

#[test]
fn keyed_joins_update_as_java_does() {
    assert_eq!(run("keyed"), 5 * 14);
}

#[test]
fn zero_key_joins_update_as_java_does() {
    assert_eq!(run("zero"), 6 * 9);
}

/// The reserve bits and the row key space: refusals, growth and failures in a cycle.
#[test]
fn limits_of_the_row_key_space_are_java_s() {
    // Six joins are refused, two fail on the way (after 2 and 1 compared steps) and four
    // run all four steps.
    assert_eq!(run("limits"), 4 * 4 + 2 + 1);
}
