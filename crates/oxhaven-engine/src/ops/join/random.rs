//! Joins of randomly changing tables, compared after every cycle with the same join of
//! static copies of the tables.

use super::{natural_join, JoinAddition, JoinError, JoinMatch, NaturalJoinType};
use crate::ops::sort::testing::{frozen, graph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{definition, ConsistencyChecker, RandomUpdates, ScriptedTable};
use std::sync::Arc;
use NaturalJoinType::{ErrorOnDuplicate, ExactlyOneMatch, FirstMatch, LastMatch};

const COLUMNS: [(&str, ColumnType); 7] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
    ("C", ColumnType::Char),
    ("T", ColumnType::Instant),
    ("V", ColumnType::Long),
];

const KEYS: [&[&str]; 7] = [
    &["K"],
    &["D"],
    &["S"],
    &["T"],
    &["K", "S"],
    &["B", "C"],
    &["C", "D", "B"],
];

const TYPES: [NaturalJoinType; 4] = [ErrorOnDuplicate, FirstMatch, LastMatch, ExactlyOneMatch];

/// Which of the two tables change.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Live {
    Both,
    Left,
    Right,
}

fn join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    keys: &[&str],
    join_type: NaturalJoinType,
) -> Result<Arc<dyn Table>, JoinError> {
    let on: Vec<JoinMatch> = keys.iter().map(|key| JoinMatch::same(*key)).collect();
    let add = [
        JoinAddition::renamed("RV", "V"),
        JoinAddition::renamed("RS", "S"),
        JoinAddition::renamed("RD", "D"),
    ];
    natural_join(left, right, &on, &add, join_type)
}

/// What the cycles of a run showed.
#[derive(Default, Debug)]
struct Tally {
    /// Cycles after which a result was compared with the join of static copies.
    compared: usize,
    /// Results that failed in the cycle in which the join of static copies was first refused.
    failed: usize,
    /// Joins that were refused before the first cycle.
    refused: usize,
}

/// Runs every join type on every key of [`KEYS`] over two random tables for `steps` cycles.
///
/// After each cycle a result must be consistent and equal to the join of static copies of
/// the tables, and it must have failed exactly when that join is refused. A failed result
/// is not looked at again.
///
/// One join is not like its static counterpart: an exact join with a static right table
/// does not look for the right row of a left row that arrives later, so it is compared
/// with the natural join that fails on duplicates.
fn run(seed: u64, steps: usize, live: Live, tally: &mut Tally) {
    let graph = graph();
    let mut random = RandomUpdates::new(seed);
    let mut source = |index: u64| {
        let empty = (seed + index).is_multiple_of(4);
        let size = if empty { 0 } else { 6 };
        let values = COLUMNS
            .iter()
            .map(|(_, column_type)| random.values(column_type, size))
            .collect();
        ScriptedTable::new(
            graph.clone(),
            definition(&COLUMNS),
            RowSet::from_keys((0..size as RowKey).map(|k| 3 * k + 1)),
            values,
        )
    };
    let scripts = [source(0), source(1)];
    let table = |index: usize, changes: bool| -> Arc<dyn Table> {
        let table = scripts[index].table();
        if changes {
            table
        } else {
            frozen(&*table)
        }
    };
    let left = table(0, live != Live::Right);
    let right = table(1, live != Live::Left);

    struct Case {
        name: String,
        result: Arc<dyn Table>,
        checker: Arc<ConsistencyChecker>,
        expected: Box<dyn Fn() -> Result<Arc<dyn Table>, JoinError>>,
    }
    let mut cases = Vec::new();
    for keys in KEYS {
        for join_type in TYPES {
            let name = format!("{join_type:?} on {keys:?}, {live:?} live, seed {seed}");
            let Ok(result) = join(&left, &right, keys, join_type) else {
                // The tables are as their static copies now, so that join is refused too.
                assert!(
                    join(&frozen(&*left), &frozen(&*right), keys, join_type).is_err(),
                    "{name}: refused, but the static join is not"
                );
                tally.refused += 1;
                continue;
            };
            let later_type = if join_type == ExactlyOneMatch && live == Live::Left {
                ErrorOnDuplicate
            } else {
                join_type
            };
            let expected = {
                let (left, right) = (left.clone(), right.clone());
                move || join(&frozen(&*left), &frozen(&*right), keys, later_type)
            };
            let recompute = expected.clone();
            let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                recompute().expect("the caller has seen this join succeed")
            });
            cases.push(Case {
                name,
                result,
                checker,
                expected: Box::new(expected),
            });
        }
    }

    let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
    for step in 0..steps {
        for (index, script) in scripts.iter().enumerate() {
            let changes = match (live, index) {
                (Live::Left, 1) | (Live::Right, 0) => false,
                (Live::Both, _) => {
                    let pick = chooser.values(&ColumnType::Byte, 1);
                    let pick = pick
                        .as_any()
                        .downcast_ref::<arrow::array::Int8Array>()
                        .unwrap();
                    arrow::array::Array::is_null(pick, 0) || (pick.value(0) + index as i8) % 3 != 0
                }
                _ => true,
            };
            if changes {
                script.push(random.next_change(&*script.table(), 4));
            }
        }
        graph.run_cycle();
        cases.retain(|case| {
            let name = &case.name;
            match (case.expected)() {
                Ok(_) => {
                    assert!(
                        !case.result.is_failed(),
                        "{name}, step {step}: failed, but the static join is not refused"
                    );
                    if let Err(e) = case.checker.verify() {
                        panic!("{name}, step {step}: {e}");
                    }
                    tally.compared += 1;
                    true
                }
                Err(error) => {
                    assert!(
                        case.result.is_failed(),
                        "{name}, step {step}: not failed, but the static join is refused: {error}"
                    );
                    tally.failed += 1;
                    false
                }
            }
        });
    }
}

fn run_all(live: Live) -> Tally {
    let mut tally = Tally::default();
    for seed in 0..40 {
        run(seed, 30, live, &mut tally);
    }
    tally
}

#[test]
fn random_updates_of_both_tables() {
    let tally = run_all(Live::Both);
    // Enough of both outcomes that neither check is idle.
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}

#[test]
fn random_updates_of_the_left_table() {
    let tally = run_all(Live::Left);
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}

#[test]
fn random_updates_of_the_right_table() {
    let tally = run_all(Live::Right);
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}
