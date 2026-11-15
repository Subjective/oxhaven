//! Cross joins of randomly changing tables, compared after every cycle with the same join of
//! static copies of the tables, with the consistency checker attached.

use super::{cross_join, CrossJoinError, DEFAULT_RESERVE_BITS};
use crate::ops::join::{JoinAddition, JoinMatch};
use crate::ops::sort::testing::{frozen, graph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{
    at_least_for, definition, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable,
};
use std::sync::Arc;

const COLUMNS: [(&str, ColumnType); 7] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
    ("C", ColumnType::Char),
    ("T", ColumnType::Instant),
    ("V", ColumnType::Long),
];

/// Match columns; the first is none at all. `B` has three values, so its groups are large
/// and make the bits grow.
const KEYS: [&[&str]; 7] = [
    &[],
    &["B"],
    &["C"],
    &["S"],
    &["D"],
    &["T", "K"],
    &["B", "C"],
];

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
    reserve: i32,
) -> Result<Arc<dyn Table>, CrossJoinError> {
    let on: Vec<JoinMatch> = keys.iter().map(|key| JoinMatch::same(*key)).collect();
    let add = [
        JoinAddition::renamed("RV", "V"),
        JoinAddition::renamed("RS", "S"),
        JoinAddition::renamed("RD", "D"),
        JoinAddition::renamed("RB", "B"),
    ];
    cross_join(left, right, &on, &add, reserve)
}

/// Runs every key set of [`KEYS`] with two reserves over two random tables for `steps`
/// cycles. After each cycle each result must be consistent and equal to the join of static
/// copies of the tables. Returns the number of comparisons.
fn run(seed: u64, steps: usize, live: Live, size: u64) -> usize {
    let graph = graph();
    let mut random = RandomUpdates::new(seed);
    let mut source = |index: u64| {
        let empty = (seed + index).is_multiple_of(5);
        let rows = if empty { 0 } else { size };
        let values = COLUMNS
            .iter()
            .map(|(_, column_type)| random.values(column_type, rows))
            .collect();
        ScriptedTable::new(
            graph.clone(),
            definition(&COLUMNS),
            RowSet::from_keys((0..rows as RowKey).map(|k| 3 * k + 1)),
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

    let mut checkers = Vec::new();
    for keys in KEYS {
        for reserve in [1, DEFAULT_RESERVE_BITS] {
            let name = format!("{keys:?}, reserve {reserve}, {live:?} live, seed {seed}");
            let result = join(&left, &right, keys, reserve).expect("a join of small tables");
            let (l, r) = (left.clone(), right.clone());
            let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                join(&frozen(&*l), &frozen(&*r), keys, reserve).expect("the static join")
            });
            if let Err(e) = checker.verify() {
                panic!("{name}, before the first step: {e}");
            }
            checkers.push((name, result, checker));
        }
    }

    let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
    let mut compared = 0;
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
                script.push(random.next_change(&*script.table(), size.div_ceil(2)));
            }
        }
        graph.run_cycle();
        for (name, result, checker) in &checkers {
            assert!(!result.is_failed(), "{name}, step {step}: failed");
            if let Err(e) = checker.verify() {
                panic!("{name}, step {step}: {e}");
            }
            compared += 1;
        }
    }
    compared
}

/// The seeds of the runs of small tables, which do most of the comparisons.
const SMALL: std::ops::Range<u64> = 0..30;

fn run_all(live: Live) -> u64 {
    let mut compared = 0;
    for seed in test_seeds(SMALL) {
        compared += run(seed, 25, live, 6);
    }
    for seed in test_seeds(100..104) {
        compared += run(seed, 10, live, 40);
    }
    compared as u64
}

#[test]
fn random_updates_of_both_tables() {
    assert!(run_all(Live::Both) > at_least_for(10_000, SMALL));
}

#[test]
fn random_updates_of_the_left_table() {
    assert!(run_all(Live::Left) > at_least_for(10_000, SMALL));
}

#[test]
fn random_updates_of_the_right_table() {
    assert!(run_all(Live::Right) > at_least_for(10_000, SMALL));
}
