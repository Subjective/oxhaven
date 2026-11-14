//! `snapshot` and `snapshot_when` against the results recorded from the Java server in
//! `fixtures/` (see `fixtures/record.py`). The oracle's own corpus (`oracle/corpus`) has no
//! scenario with these operations.

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
