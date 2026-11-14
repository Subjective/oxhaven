//! `snapshot` and `snapshot_when` against the results recorded from the Java server: in
//! `fixtures/` (see `fixtures/record.py`), and in the oracle's corpus (`oracle/corpus`) by
//! the replay of `ops/oracle.rs`.

use super::testing::{list, replay, text, Built};
use super::{snapshot, snapshot_when, SnapshotError, SnapshotWhenOptions, StampColumn};
use oxhaven_contracts::table::Table;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `fixtures/snapshot_static.json`.
fn code(error: &SnapshotError) -> &'static str {
    match error {
        SnapshotError::Unsupported(_) => "UNIMPLEMENTED",
        SnapshotError::InvalidName(_)
        | SnapshotError::NoSuchColumn(_)
        | SnapshotError::ColumnConflict(_) => "INVALID_ARGUMENT",
        other => panic!("no recorded status code for {other:?}"),
    }
}

/// As the Java handler does: the flags and the stamp column strings make the options,
/// which may refuse, and then the operation runs.
///
/// Behavior: io.deephaven.server.table.ops.SnapshotWhenTableGrpcImpl#options
fn define(operation: &Value, sources: &[Arc<dyn Table>]) -> Built {
    let result = match text(&operation["op"]) {
        "snapshot" => snapshot(&sources[0]),
        "snapshot_when" => {
            let flag = |name: &str| operation[name].as_bool().expect("a flag");
            let stamps = list(&operation["stamp_columns"]).iter();
            stamps
                .map(|stamp| StampColumn::parse(text(stamp)))
                .collect::<Result<Vec<_>, _>>()
                .and_then(|stamp_columns| {
                    let options = SnapshotWhenOptions {
                        initial: flag("initial"),
                        incremental: flag("incremental"),
                        history: flag("history"),
                        stamp_columns,
                    };
                    snapshot_when(&sources[0], &sources[1], &options)
                })
        }
        other => panic!("no operation {other}"),
    };
    result.map_err(|error| code(&error))
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/ops/snapshot/fixtures")
        .join(format!("{name}.json"))
}

#[test]
fn static_tables_match_java() {
    // Every result of the fixture, at its one step.
    assert_eq!(replay(&fixture("snapshot_static"), &define), 62);
}

#[test]
fn snapshot_when_of_ticking_tables_matches_java() {
    // 13 results at 14 steps.
    assert_eq!(replay(&fixture("snapshot_when_ticking"), &define), 13 * 14);
}

#[test]
fn incremental_snapshot_when_matches_java() {
    // 10 results at 25 steps.
    assert_eq!(
        replay(&fixture("snapshot_when_incremental"), &define),
        10 * 25
    );
}

#[test]
fn incremental_snapshot_when_keeps_javas_first_comparison() {
    // 2 results at 14 steps.
    assert_eq!(
        replay(&fixture("snapshot_when_incremental_readded"), &define),
        2 * 14
    );
}

#[test]
fn snapshot_history_matches_java() {
    // 8 results at 15 steps.
    assert_eq!(replay(&fixture("snapshot_when_history"), &define), 8 * 15);
}

/// The oracle's scenarios, as far as they can be built from `snapshot` and `snapshot_when`.
#[test]
fn the_results_of_the_oracle_match_java() {
    use crate::ops::oracle::{self, replay_all};
    // The flags of a scenario's `snapshot_when` are optional and default to false.
    let define = |operation: &Value, sources: &[Arc<dyn Table>]| -> oracle::Built {
        let mut operation = operation.clone();
        for flag in ["initial", "incremental", "history"] {
            let set = operation[flag] == true;
            operation[flag] = Value::Bool(set);
        }
        match define(&operation, sources) {
            Ok(table) => oracle::Built::Table(table),
            Err(code) => oracle::Built::Refused(code),
        }
    };
    let tally = replay_all(&["snapshot", "snapshot_when"], &define);
    let differences: [&str; 0] = [];
    assert_eq!(tally.differences, differences, "differences from Java");
    let compared = [
        "snapshot_static/snap",
        "snapshot_static/snap_empty",
        "snapshot_static/when_empty_base",
        "snapshot_static/when_history",
        "snapshot_static/when_history_incremental",
        "snapshot_static/when_history_initial",
        "snapshot_static/when_incremental",
        "snapshot_static/when_initial",
        "snapshot_static/when_no_rows",
        "snapshot_static/when_plain",
        "snapshot_static/when_stamp",
        "snapshot_static/when_stamp_clash",
        "snapshot_static/when_stamp_renamed",
        "snapshot_static/when_stamp_unknown",
        "snapshot_static/when_two_rows",
        "ticking_snapshot_when/snap",
        "ticking_snapshot_when/when_history",
        "ticking_snapshot_when/when_incremental",
        "ticking_snapshot_when/when_initial",
        "ticking_snapshot_when/when_keyed_trigger",
        "ticking_snapshot_when/when_plain",
        "ticking_snapshot_when/when_stamp",
    ];
    assert_eq!(tally.compared, compared, "compared");
    // 15 static results at one step, and 7 ticking ones at 9 steps.
    assert_eq!(tally.comparisons, 15 + 7 * 9);
    // Results made from an operation of another test.
    let not_run = [
        "snapshot_static/snap_of_where: where",
        "ticking_snapshot_when/when_derived: where",
    ];
    assert_eq!(tally.not_run, not_run, "not run");
}
