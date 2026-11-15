//! `multi_join` against the results recorded from the Java server in the oracle's corpus
//! (`oracle/corpus`), by the replay of `ops/oracle.rs`.

use super::{multi_join, JoinAddition, JoinMatch, MultiJoinError, MultiJoinInput};
use crate::ops::oracle::{list, replay_all, text, Built};
use crate::ops::select::view;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use serde_json::Value;
use std::sync::Arc;

/// The gRPC status code the Java server answers with for each refusal: every one is an
/// exception out of the engine, which `oracle/corpus/multi_join/manifest.json` has as
/// `INVALID_ARGUMENT` for a duplicate key, a column added twice, key types that differ and
/// an unknown key column.
fn code(error: &MultiJoinError) -> &'static str {
    match error {
        MultiJoinError::DuplicateKey { .. }
        | MultiJoinError::ColumnConflict { .. }
        | MultiJoinError::KeyTypeMismatch { .. }
        | MultiJoinError::NoSuchColumn { .. } => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

/// The two names of `Name` or `Left = Right`.
///
/// Behavior: io.deephaven.api.JoinMatch#parse and io.deephaven.api.JoinAddition#parse, for
/// the forms the scenarios use
fn pair(value: &Value) -> (String, String) {
    let (left, right) = match text(value).split_once('=') {
        Some((left, right)) => (left.trim(), right.trim()),
        None => (text(value).trim(), text(value).trim()),
    };
    (left.to_string(), right.to_string())
}

/// Behavior: `apply_operation` in `oracle/harness/runner.py` for `multi_join`, where the
/// Python client sends no added columns for an input without `joins` or with an empty list
fn define(operation: &Value, sources: &[Arc<dyn Table>]) -> Built {
    if text(&operation["op"]) == "view" {
        let formulas: Vec<&str> = list(&operation["formulas"]).iter().map(text).collect();
        // Only the views that choose columns: the others belong to the select family.
        let plain = |formula: &&str| sources[0].definition().index_of(formula).is_some();
        if !formulas.iter().all(plain) {
            return Built::NotRun("a view of formulas".to_string());
        }
        return Built::Table(view(&sources[0], &formulas, &Compiler).expect("a view"));
    }
    let inputs: Vec<MultiJoinInput> = list(&operation["inputs"])
        .iter()
        .zip(sources)
        .map(|(input, table)| MultiJoinInput {
            table: table.clone(),
            on: list(&input["on"])
                .iter()
                .map(pair)
                .map(|(left, right)| JoinMatch::new(left, right))
                .collect(),
            add: list(&input["joins"])
                .iter()
                .map(pair)
                .map(|(name, source)| JoinAddition::renamed(name, source))
                .collect(),
        })
        .collect();
    match multi_join(&inputs) {
        Ok(table) => Built::Table(table),
        Err(error) => Built::Refused(code(&error)),
    }
}

#[test]
fn the_results_of_the_oracle_match_java() {
    let tally = replay_all(&["multi_join", "view"], &define);
    // The views are built for the joins made from them. As results they are compared by
    // the select family's tests, which read an input's null sentinels as the server does.
    let ours = |name: &&String| name.contains("multi_join/");
    let differences: Vec<&String> = tally.differences.iter().filter(ours).collect();
    assert!(
        differences.is_empty(),
        "differences from Java: {differences:?}"
    );
    let compared: Vec<&String> = tally.compared.iter().filter(ours).collect();
    let expected = [
        "multi_join/chosen",
        "multi_join/duplicate_key",
        "multi_join/key_types",
        "multi_join/no_values",
        "multi_join/one",
        "multi_join/same_value_column",
        "multi_join/three",
        "multi_join/two",
        "multi_join/two_keys",
        "multi_join/two_swapped",
        "multi_join/unknown_key",
        "multi_join/with_empty",
        "ticking_multi_join/three",
        "ticking_multi_join/two",
    ];
    assert_eq!(compared, expected, "compared");
    // Every `multi_join` of the oracle is made from inputs and from views that choose
    // columns.
    let not_run: Vec<&String> = tally.not_run.iter().filter(ours).collect();
    assert!(not_run.is_empty(), "not run: {not_run:?}");
    assert_eq!(tally.others.get("multi_join"), None);
}
