//! `head_by` and `tail_by` against the results recorded from the Java server: in
//! `fixtures/` by `ops/snapshot/fixtures/record.py`, with the updates Java delivered, and
//! in the oracle's corpus (`oracle/corpus`) by the replay of `ops/oracle.rs`.

use super::{head_by, tail_by, HeadByError};
use crate::ops::snapshot::testing::{replay, text, texts, Built};
use oxhaven_contracts::table::Table;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `fixtures/head_tail_by_static.json`.
fn code(error: &HeadByError) -> &'static str {
    match error {
        HeadByError::RowsNotPositive(_)
        | HeadByError::GroupColumns { .. }
        | HeadByError::NoOtherColumns(_) => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

fn define(operation: &Value, sources: &[Arc<dyn Table>]) -> Built {
    let rows = operation["rows"].as_i64().expect("a number of rows");
    let by: Vec<String> = texts(&operation["by"])
        .into_iter()
        .map(str::to_string)
        .collect();
    let made = match text(&operation["op"]) {
        "head_by" => head_by(&sources[0], rows, &by),
        "tail_by" => tail_by(&sources[0], rows, &by),
        other => panic!("no operation {other}"),
    };
    made.map_err(|error| code(&error))
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/ops/head_by/fixtures")
        .join(format!("{name}.json"))
}

#[test]
fn static_tables_match_java() {
    // Every result of the fixture, at its one step.
    assert_eq!(replay(&fixture("head_tail_by_static"), &define), 63);
}

#[test]
fn ticking_tables_and_their_updates_match_java() {
    // 16 results at 22 steps, and the updates of 15 of them in 21 steps.
    assert_eq!(
        replay(&fixture("head_tail_by_ticking"), &define),
        16 * 22 + 15 * 21
    );
}

/// A group that outgrows the row keys Java's `ungroup` gave it: the result moves every
/// group to wider slots.
#[test]
fn groups_of_more_than_1024_kept_rows_match_java() {
    // 3 results at 5 steps, and their updates in 4 steps.
    assert_eq!(
        replay(&fixture("head_tail_by_wide_groups"), &define),
        3 * 5 + 3 * 4
    );
}

/// The oracle's scenarios, as far as they can be built from `head_by` and `tail_by`.
#[test]
fn the_results_of_the_oracle_match_java() {
    use crate::ops::oracle::{self, replay_all};
    let define = |operation: &Value, sources: &[Arc<dyn Table>]| -> oracle::Built {
        match define(operation, sources) {
            Ok(table) => oracle::Built::Table(table),
            Err(code) => oracle::Built::Refused(code),
        }
    };
    let tally = replay_all(&["head_by", "tail_by"], &define);
    let differences: [&str; 0] = [];
    assert_eq!(tally.differences, differences, "differences from Java");
    let compared = [
        "head_by_tail_by/head_all_keys",
        "head_by_tail_by/head_by_double",
        "head_by_tail_by/head_empty",
        "head_by_tail_by/head_many",
        "head_by_tail_by/head_no_key",
        "head_by_tail_by/head_one",
        "head_by_tail_by/head_pair",
        "head_by_tail_by/head_two",
        "head_by_tail_by/head_two_keys",
        "head_by_tail_by/head_unknown",
        "head_by_tail_by/head_zero",
        "head_by_tail_by/tail_all_keys",
        "head_by_tail_by/tail_by_int",
        "head_by_tail_by/tail_no_key",
        "head_by_tail_by/tail_one",
        "head_by_tail_by/tail_two",
        "head_by_tail_by/tail_two_keys",
        "head_by_tail_by/tail_unknown",
        "head_by_tail_by/tail_zero",
        "ticking_group_ungroup_head_by/head_one",
        "ticking_group_ungroup_head_by/head_two",
        "ticking_group_ungroup_head_by/head_zero",
        "ticking_group_ungroup_head_by/tail_one",
        "ticking_group_ungroup_head_by/tail_two",
    ];
    assert_eq!(tally.compared, compared, "compared");
    // 19 static results at one step and 5 ticking ones at 9 steps.
    assert_eq!(tally.comparisons, 19 + 5 * 9);
    // Every `head_by` and `tail_by` of the oracle is made from an input table.
    let not_run: [&str; 0] = [];
    assert_eq!(tally.not_run, not_run, "not run");
}
