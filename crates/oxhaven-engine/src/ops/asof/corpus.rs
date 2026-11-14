//! `aj` and `raj` against the results recorded from the Java server: in `fixtures/` by
//! `ops/snapshot/fixtures/record.py`, and in the oracle's corpus (`oracle/corpus`) by the
//! replay of `ops/oracle.rs`.
//!
//! To record the fixtures again:
//!
//! ```text
//! oracle/bin/start-java.sh 10865
//! oracle/.venv/bin/python crates/oxhaven-engine/src/ops/snapshot/fixtures/record.py \
//!     --url dh+psk://oxhaven@127.0.0.1:10865 crates/oxhaven-engine/src/ops/asof/fixtures/*.json
//! oracle/bin/stop-java.sh 10865
//! ```
//!
//! An operation of a fixture names its tables twice: as `left` and `right`, which the
//! recording script reads, and as `source` and `set`, which are the keys the replay of
//! `ops/snapshot/testing.rs` passes tables by.

use super::{aj, raj, AsOfJoinError, AsOfMatch, AsOfRule, JoinAddition, JoinMatch};
use crate::ops::snapshot::testing::{replay, text, texts, Built};
use oxhaven_contracts::table::Table;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `fixtures/asof_refusals.json`.
fn code(error: &AsOfJoinError) -> &'static str {
    match error {
        AsOfJoinError::UnsupportedRule { .. }
        | AsOfJoinError::NoSuchColumn(_)
        | AsOfJoinError::ConflictingColumns(_)
        | AsOfJoinError::StampTypes { .. }
        | AsOfJoinError::KeyTypes { .. } => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

/// The two names of `left = right`, `left == right` or a name for both.
///
/// Behavior: io.deephaven.api.JoinMatch#parse and io.deephaven.api.JoinAddition#parse, for
/// the well-formed texts of the fixtures
fn pair(text: &str) -> (&str, &str) {
    match text.split_once('=') {
        Some((left, right)) => (left.trim(), right.trim_start_matches('=').trim()),
        None => (text.trim(), text.trim()),
    }
}

/// The as-of match of the last entry of `on`. A name alone has the first rule given.
///
/// Behavior: io.deephaven.api.AsOfJoinMatch#parseForAj and #parseForRaj, for the
/// well-formed texts of the fixtures
fn as_of(text: &str, alone: AsOfRule) -> AsOfMatch {
    let rules = [
        (">=", AsOfRule::GreaterThanEqual),
        ("<=", AsOfRule::LessThanEqual),
        (">", AsOfRule::GreaterThan),
        ("<", AsOfRule::LessThan),
    ];
    for (operator, rule) in rules {
        if let Some((left, right)) = text.split_once(operator) {
            return AsOfMatch::new(left.trim(), rule, right.trim());
        }
    }
    AsOfMatch::new(text.trim(), alone, text.trim())
}

fn define(operation: &Value, sources: &[Arc<dyn Table>]) -> Built {
    let on = texts(&operation["on"]);
    let (stamp, exact) = on.split_last().expect("an as-of match");
    let exact: Vec<JoinMatch> = exact
        .iter()
        .map(|text| {
            let (left, right) = pair(text);
            JoinMatch::new(left, right)
        })
        .collect();
    let add: Vec<JoinAddition> = texts(&operation["joins"])
        .into_iter()
        .map(|text| {
            let (name, source) = pair(text);
            JoinAddition::renamed(name, source)
        })
        .collect();
    let (left, right) = (&sources[0], &sources[1]);
    let joined = match text(&operation["op"]) {
        "aj" => aj(
            left,
            right,
            &exact,
            &as_of(stamp, AsOfRule::GreaterThanEqual),
            &add,
        ),
        "raj" => raj(
            left,
            right,
            &exact,
            &as_of(stamp, AsOfRule::LessThanEqual),
            &add,
        ),
        other => panic!("no operation {other}"),
    };
    joined.map_err(|error| code(&error))
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/ops/asof/fixtures")
        .join(format!("{name}.json"))
}

#[test]
fn refusals_match_java() {
    // Every result of the fixture, at its one step.
    assert_eq!(replay(&fixture("asof_refusals"), &define), 45);
}

#[test]
fn the_four_rules_and_the_added_columns_match_java() {
    assert_eq!(replay(&fixture("asof_rules"), &define), 34);
}

#[test]
fn stamps_of_every_type_match_java() {
    assert_eq!(replay(&fixture("asof_stamp_types"), &define), 56);
}

#[test]
fn exact_match_columns_of_every_type_match_java() {
    assert_eq!(replay(&fixture("asof_key_types"), &define), 24);
}

/// The oracle's scenarios, as far as they can be built from `aj` and `raj`.
#[test]
fn the_results_of_the_oracle_match_java() {
    use crate::ops::oracle::{self, replay_all};
    let define = |operation: &Value, sources: &[Arc<dyn Table>]| -> oracle::Built {
        match define(operation, sources) {
            Ok(table) => oracle::Built::Table(table),
            Err(code) => oracle::Built::Refused(code),
        }
    };
    let tally = replay_all(&["aj", "raj"], &define);
    let differences: [&str; 0] = [];
    assert_eq!(tally.differences, differences, "differences from Java");
    let compared = [
        "aj_int_keys/keyed",
        "aj_int_keys/strict",
        "aj_int_keys/unkeyed",
        "aj_timestamps/ge",
        "aj_timestamps/gt",
        "aj_timestamps/no_key",
        "error_types_and_matches/aj_bool_stamp",
        "raj_and_left_join/aj_double",
        "raj_and_left_join/aj_le",
        "raj_and_left_join/aj_lt",
        "raj_and_left_join/raj_all_columns",
        "raj_and_left_join/raj_double",
        "raj_and_left_join/raj_empty_right",
        "raj_and_left_join/raj_ge",
        "raj_and_left_join/raj_gt",
        "raj_and_left_join/raj_name_clash",
        "raj_and_left_join/raj_no_key",
        "raj_and_left_join/raj_stamp_types",
        "raj_and_left_join/raj_wrong_rule",
        "ticking_aj/j",
        "ticking_raj/aj_le",
        "ticking_raj/raj_ge",
        "ticking_raj/raj_gt",
        "ticking_raj/raj_no_key",
    ];
    assert_eq!(tally.compared, compared, "compared");
    // 19 static results at one step, 1 ticking one at 7 steps and 4 at 8 steps.
    assert_eq!(tally.comparisons, 19 + 7 + 4 * 8);
    let not_run: [&str; 0] = [];
    assert_eq!(tally.not_run, not_run, "not run");
}
