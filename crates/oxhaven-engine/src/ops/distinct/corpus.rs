//! `select_distinct` against the results recorded from the Java server: in `fixtures/` by
//! `ops/snapshot/fixtures/record.py`, and in the oracle's corpus (`oracle/corpus`) by the
//! replay of `ops/oracle.rs`.

use super::{select_distinct, where_in, where_not_in, DistinctError, WhereInError};
use crate::ops::join::JoinMatch;
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

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `fixtures/where_in_static.json` and `fixtures/where_in_ticking.json`.
fn where_in_code(error: &WhereInError) -> &'static str {
    match error {
        WhereInError::NoSuchColumn(_) | WhereInError::MismatchedTypes { .. } => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

/// The pairs of match columns of a request, each `Left = Right`, `Left == Right` or the
/// one name of both.
///
/// Behavior: io.deephaven.api.JoinMatch#parse
fn matches(columns: &[&str]) -> Vec<JoinMatch> {
    columns
        .iter()
        .map(|column| match column.split_once('=') {
            Some((left, right)) => {
                JoinMatch::new(left.trim(), right.trim_start_matches('=').trim())
            }
            None => JoinMatch::same(column.trim()),
        })
        .collect()
}

/// `where_in` (and with `inverted` `where_not_in`) as the server's handler will call it.
/// A request without match columns is refused there, before the engine sees it; the
/// engine itself has an answer for it (`where_in_tests.rs`).
///
/// Behavior: io.deephaven.server.table.ops.WhereInGrpcImpl#validateRequest
fn where_in_of(sources: &[Arc<dyn Table>], columns: &[&str], inverted: bool) -> Built {
    if columns.is_empty() {
        return Err("INVALID_ARGUMENT");
    }
    let on = matches(columns);
    let result = if inverted {
        where_not_in(&sources[0], &sources[1], &on)
    } else {
        where_in(&sources[0], &sources[1], &on)
    };
    result.map_err(|error| where_in_code(&error))
}

fn define(operation: &Value, sources: &[Arc<dyn Table>]) -> Built {
    match text(&operation["op"]) {
        "where_in" => where_in_of(sources, &texts(&operation["columns"]), false),
        "where_not_in" => where_in_of(sources, &texts(&operation["columns"]), true),
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

#[test]
fn where_in_of_static_tables_matches_java() {
    // Every result of the fixture, at its one step.
    assert_eq!(replay(&fixture("where_in_static"), &define), 55);
}

#[test]
fn where_in_of_ticking_tables_matches_java() {
    // 29 results at 20 steps, and the updates of 13 of them during 19 steps.
    assert_eq!(
        replay(&fixture("where_in_ticking"), &define),
        29 * 20 + 13 * 19
    );
}

/// The keys of a sorted table come in the order in which the sort's updates bring them, so
/// this needs a sort that reports the same rows as added and as modified as Java's. At
/// step 4 a row changes its sort key to one that comes earlier and keeps its neighbours:
/// Java's sort removes and adds it, because it looks for the row's place among the
/// previous values, its own included
/// (io.deephaven.engine.table.impl.SortListener.TargetComparator#compareTargetTo). Were
/// it reported as modified, the key it brings would be met before the key of the row
/// added in front of it.
#[test]
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

/// The oracle's scenarios, as far as they can be built from `select_distinct`.
#[test]
fn the_results_of_the_oracle_match_java() {
    use crate::ops::oracle::{self, replay_all};
    let define = |operation: &Value, sources: &[Arc<dyn Table>]| -> oracle::Built {
        match define(operation, sources) {
            Ok(table) => oracle::Built::Table(table),
            Err(code) => oracle::Built::Refused(code),
        }
    };
    let tally = replay_all(&["select_distinct"], &define);
    let differences: [&str; 0] = [];
    assert_eq!(tally.differences, differences, "differences from Java");
    let compared = [
        "select_distinct/all_columns",
        "select_distinct/by_bool",
        "select_distinct/by_double",
        "select_distinct/by_float",
        "select_distinct/by_formula",
        "select_distinct/by_int",
        "select_distinct/by_string",
        "select_distinct/by_time",
        "select_distinct/by_two",
        "select_distinct/by_two_swapped",
        "select_distinct/of_empty",
        "select_distinct/twice",
        "select_distinct/unknown",
        "ticking_column_statistics/distinct_groups",
        "ticking_select_distinct_where_in/doubles",
        "ticking_select_distinct_where_in/groups",
        "ticking_select_distinct_where_in/pairs",
    ];
    assert_eq!(tally.compared, compared, "compared");
    // 13 static results at one step, 1 ticking one at 4 steps and 3 at 9 steps.
    assert_eq!(tally.comparisons, 13 + 4 + 3 * 9);
    // Results made from an operation of another test.
    let not_run = ["select_distinct/all_but_key: drop_columns"];
    assert_eq!(tally.not_run, not_run, "not run");
}

/// The oracle's scenarios, as far as they can be built from `where_in` and
/// `select_distinct`. There an operation is `where_in` with `left`, `right`, `on` and
/// `inverted`.
#[test]
fn the_where_in_results_of_the_oracle_match_java() {
    use crate::ops::oracle::{self, replay_all};
    let define = |operation: &Value, sources: &[Arc<dyn Table>]| -> oracle::Built {
        let built = match text(&operation["op"]) {
            "where_in" => {
                let inverted = operation["inverted"] == true;
                where_in_of(sources, &texts(&operation["on"]), inverted)
            }
            _ => define(operation, sources),
        };
        match built {
            Ok(table) => oracle::Built::Table(table),
            Err(code) => oracle::Built::Refused(code),
        }
    };
    let tally = replay_all(&["select_distinct", "where_in"], &define);
    let differences: [&str; 0] = [];
    assert_eq!(tally.differences, differences, "differences from Java");
    let compared: Vec<&String> = tally
        .compared
        .iter()
        .filter(|name| name.contains("where_in/"))
        .collect();
    let expected = [
        "ticking_select_distinct_where_in/doubles",
        "ticking_select_distinct_where_in/groups",
        "ticking_select_distinct_where_in/in_distinct",
        "ticking_select_distinct_where_in/in_fixed",
        "ticking_select_distinct_where_in/in_ticking",
        "ticking_select_distinct_where_in/not_in_fixed",
        "ticking_select_distinct_where_in/not_in_ticking",
        "ticking_select_distinct_where_in/pairs",
        "where_in/empty_left",
        "where_in/in_double",
        "where_in/in_int",
        "where_in/in_itself",
        "where_in/in_none",
        "where_in/in_string",
        "where_in/in_two",
        "where_in/not_in_double",
        "where_in/not_in_int",
        "where_in/not_in_none",
        "where_in/not_in_string",
        "where_in/not_in_two",
        "where_in/other_type",
        "where_in/unknown_left",
        "where_in/unknown_right",
    ];
    assert_eq!(compared, expected, "compared");
    // The results of `select_distinct` alone (see above), 15 static results of `where_in`
    // at one step and 5 ticking ones at 9 steps.
    assert_eq!(tally.comparisons, (13 + 4 + 3 * 9) + 15 + 5 * 9);
    let not_run = ["select_distinct/all_but_key: drop_columns"];
    assert_eq!(tally.not_run, not_run, "not run");
}
