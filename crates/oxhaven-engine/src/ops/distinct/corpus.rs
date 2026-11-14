//! `select_distinct` against the results recorded from the Java server in `fixtures/` by
//! `ops/snapshot/fixtures/record.py`. The oracle's own corpus (`oracle/corpus`) has no
//! scenario with this operation.

use super::{select_distinct, DistinctError};
use crate::ops::snapshot::testing::{replay, text, texts, Built};
use crate::ops::sort::testing::{cells, graph, scripted};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{ints, TableChange};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `fixtures/select_distinct_static.json`.
fn code(error: &DistinctError) -> &'static str {
    match error {
        DistinctError::NoSuchColumn(_) => "FAILED_PRECONDITION",
        other => panic!("no recorded status code for {other:?}"),
    }
}

fn define(operation: &Value, sources: &[Arc<dyn Table>]) -> Built {
    match text(&operation["op"]) {
        "select_distinct" => {
            let columns = texts(&operation["columns"]);
            let columns: Vec<String> = columns.into_iter().map(str::to_string).collect();
            select_distinct(&sources[0], &columns).map_err(|error| code(&error))
        }
        other => panic!("no operation {other}"),
    }
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/ops/distinct/fixtures")
        .join(format!("{name}.json"))
}

#[test]
fn static_tables_match_java() {
    // Every result of the fixture, at its one step.
    assert_eq!(replay(&fixture("select_distinct_static"), &define), 27);
}

#[test]
fn ticking_tables_match_java() {
    // 16 results at 14 steps.
    assert_eq!(
        replay(&fixture("select_distinct_ticking"), &define),
        16 * 14
    );
}

/// The keys of a sorted table come in the order in which the sort's updates bring them, so
/// this needs a sort that reports the same rows as added and as modified as Java's. At
/// step 4 a row changes its sort key to one that comes earlier and keeps its neighbours:
/// Java's sort removes and adds it, because it looks for the row's place among the
/// previous values, its own included
/// (io.deephaven.engine.table.impl.SortListener.TargetComparator#compareTargetTo), and
/// `ops::sort` reports it as modified, so the key it brings is met before the key of the
/// row added in front of it.
#[test]
#[ignore = "ops::sort reports a modified row that moves towards the front between the same neighbours as modified; Java removes and adds it"]
fn the_keys_of_a_sorted_table_match_java() {
    // 1 result at 14 steps.
    assert_eq!(replay(&fixture("select_distinct_of_sort"), &define), 14);
}

/// The rows of `table` in order, the cells of a row joined by `|`.
fn shown(table: &dyn Table) -> Vec<String> {
    let columns: Vec<Vec<String>> = (0..table.definition().columns().len())
        .map(|column| cells(table, column))
        .collect();
    (0..table.row_set().size() as usize)
        .map(|row| {
            let cells: Vec<&str> = columns.iter().map(|column| column[row].as_str()).collect();
            cells.join("|")
        })
        .collect()
}

/// The fixture's `m` puts the rows with `K >= 100` before the others with a `where` and a
/// `merge`, which this replay cannot run. The test scripts a table that changes as `m`
/// does, checks that against the recorded `m`, and compares its `select_distinct` with
/// the recorded one.
#[test]
fn keys_of_changed_rows_come_before_keys_of_added_rows_as_in_java() {
    let path = fixture("select_distinct_changed_before_added");
    let file = std::fs::File::open(&path).unwrap();
    let recorded: Value = serde_json::from_reader(file).unwrap();
    let rows_of = |step: usize, name: &str| -> Vec<String> {
        let rows = recorded["java"]["steps"][step][name]["rows"].as_array();
        let rows = rows.expect("recorded rows").iter();
        rows.map(|row| {
            let cells = row.as_array().unwrap().iter();
            cells.map(Value::to_string).collect::<Vec<_>>().join("|")
        })
        .collect()
    };

    let graph = graph();
    // K >= 100 at the row keys from 0, K < 100 at the row keys from 1000.
    let columns = [
        ("K", ColumnType::Int, ints([100, 1, 2])),
        ("G", ColumnType::Int, ints([30, 10, 20])),
    ];
    let script = scripted(&graph, [0, 1000, 1001], &columns);
    let m: Arc<dyn Table> = script.table();
    let by_g = select_distinct(&m, &["G".to_string()]).unwrap();
    let changes = [
        TableChange::new()
            .add([1], vec![ints([101]), ints([60])])
            .modify([1000], [(1, ints([50]))]),
        TableChange::new()
            .add([2, 3], vec![ints([102, 103]), ints([70, 90])])
            .modify([1001], [(1, ints([80]))]),
    ];
    assert_eq!(recorded["java"]["steps"].as_array().unwrap().len(), 3);
    for step in 0..3 {
        assert_eq!(shown(&*m), rows_of(step, "m"), "m at step {step}");
        assert_eq!(shown(&*by_g), rows_of(step, "by_g"), "by_g at step {step}");
        if let Some(change) = changes.get(step) {
            script.push(change.clone());
            graph.run_cycle();
        }
    }
}
