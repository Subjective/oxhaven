//! As-of joins of randomly changing tables, compared after every cycle with the same join
//! of static copies of the tables.

use super::{as_of_join, AsOfMatch, AsOfRule, JoinAddition, JoinMatch};
use crate::ops::sort::testing::{frozen, graph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{definition, ConsistencyChecker, RandomUpdates, ScriptedTable};
use std::sync::Arc;

const COLUMNS: [(&str, ColumnType); 8] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
    ("C", ColumnType::Char),
    ("T", ColumnType::Instant),
    ("V", ColumnType::Long),
    ("F", ColumnType::Float),
];

/// The exact-match columns and the stamp column of a join. Both tables have them.
const JOINS: [(&[&str], &str); 9] = [
    (&[], "K"),
    (&[], "D"),
    (&["S"], "K"),
    (&["K"], "D"),
    (&["B"], "T"),
    (&["C", "B"], "V"),
    (&["S"], "F"),
    (&["B"], "S"),
    (&["D"], "C"),
];

const RULES: [AsOfRule; 4] = [
    AsOfRule::GreaterThanEqual,
    AsOfRule::GreaterThan,
    AsOfRule::LessThanEqual,
    AsOfRule::LessThan,
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
    exact: &[&str],
    stamp: &str,
    rule: AsOfRule,
) -> Arc<dyn Table> {
    let exact: Vec<JoinMatch> = exact.iter().map(|key| JoinMatch::same(*key)).collect();
    let add = [
        JoinAddition::renamed("RV", "V"),
        JoinAddition::renamed("RS", "S"),
        JoinAddition::renamed("RD", "D"),
        JoinAddition::renamed("RK", "K"),
    ];
    match as_of_join(left, right, &exact, &AsOfMatch::new(stamp, rule, stamp), &add) {
        Ok(result) => result,
        Err(error) => panic!("{rule:?} on {exact:?} and {stamp}: {error}"),
    }
}

/// Runs every rule on every entry of [`JOINS`] over two random tables for `steps` cycles
/// and returns the number of results compared. After each cycle each result must be
/// consistent and equal to the join of static copies of the tables.
fn run(seed: u64, steps: usize, live: Live) -> usize {
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

    let mut cases: Vec<(String, Arc<ConsistencyChecker>)> = Vec::new();
    for (exact, stamp) in JOINS {
        for rule in RULES {
            let name = format!("{rule:?} on {exact:?} and {stamp}, {live:?} live, seed {seed}");
            let result = join(&left, &right, exact, stamp, rule);
            if !result.is_refreshing() {
                // A static left table without rows: the result is static and stays empty.
                assert!(live == Live::Right && left.row_set().is_empty(), "{name}");
                continue;
            }
            let (left, right) = (left.clone(), right.clone());
            let checker = ConsistencyChecker::attach_with_recompute(result, move || {
                join(&frozen(&*left), &frozen(&*right), exact, stamp, rule)
            });
            if let Err(e) = checker.verify() {
                panic!("{name}, before the first step: {e}");
            }
            cases.push((name, checker));
        }
    }

    let mut compared = 0;
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
        for (name, checker) in &cases {
            if let Err(e) = checker.verify() {
                panic!("{name}, step {step}: {e}");
            }
            compared += 1;
        }
    }
    compared
}

fn run_all(live: Live) -> usize {
    (0..40).map(|seed| run(seed, 30, live)).sum()
}

#[test]
fn random_updates_of_both_tables() {
    assert_eq!(run_all(Live::Both), 40 * 30 * JOINS.len() * RULES.len());
}

#[test]
fn random_updates_of_the_left_table() {
    assert_eq!(run_all(Live::Left), 40 * 30 * JOINS.len() * RULES.len());
}

#[test]
fn random_updates_of_the_right_table() {
    // One seed in four starts with a left table without rows, whose joins are static.
    assert_eq!(run_all(Live::Right), 30 * 30 * JOINS.len() * RULES.len());
}
