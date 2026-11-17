//! As-of joins of randomly changing tables, compared after every cycle with the same join
//! of static copies of the tables.

use super::{as_of_join, AsOfMatch, AsOfRule, JoinAddition, JoinMatch};
use crate::ops::select::update;
use crate::ops::sort::testing::{frozen, graph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{definition, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable};
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
    match as_of_join(
        left,
        right,
        &exact,
        &AsOfMatch::new(stamp, rule, stamp),
        &add,
    ) {
        Ok(result) => result,
        Err(error) => panic!("{rule:?} on {exact:?} and {stamp}: {error}"),
    }
}

/// Columns of the `java.time` types made by formulas from the random columns: few distinct
/// values, so that stamps tie, and nulls where `K` or `T` is null. `Zd` is one instant in
/// three zones, two of them with one offset.
const TIME_COLUMNS: [&str; 6] = [
    "Da = toLocalDate(T + (isNull(K) ? 0 : K % 4) * 86400000000000L, 'UTC')",
    "Ti = toLocalTime(T, 'Asia/Tokyo')",
    "Dt = toLocalDateTime(T, 'America/New_York')",
    "Du = isNull(K) ? null : (K % 3 == 0 ? 'PT1H' : (K % 3 == 1 ? 'PT-2M' : 'PT0.5S'))",
    "Dw = dayOfWeek(T + (isNull(V) ? 0 : V % 3) * 86400000000000L, 'UTC')",
    "Zd = toZonedDateTime(T, timeZone(isNull(V) || V % 3 == 0 ? `UTC` : V % 3 == 1 ? `Asia/Tokyo` : `Europe/London`))",
];

/// Joins on the columns of [`TIME_COLUMNS`], as stamps and as exact-match columns.
const TIME_JOINS: [(&[&str], &str); 9] = [
    (&[], "Da"),
    (&["S"], "Ti"),
    (&["Dw"], "Dt"),
    (&["Da"], "Du"),
    (&[], "Dw"),
    (&["B"], "Zd"),
    (&[], "Zd"),
    (&["Zd"], "Ti"),
    (&["Du", "Dt"], "Dw"),
];

/// Runs every rule on every entry of [`JOINS`] over two random tables for `steps` cycles
/// and returns the number of results compared. After each cycle each result must be
/// consistent and equal to the join of static copies of the tables.
fn run(seed: u64, steps: usize, live: Live) -> usize {
    run_joins(seed, steps, live, &JOINS, false)
}

/// [`run`] over `joins`; with `times`, the tables have the columns of [`TIME_COLUMNS`] too.
fn run_joins(
    seed: u64,
    steps: usize,
    live: Live,
    joins: &'static [(&'static [&'static str], &'static str)],
    times: bool,
) -> usize {
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
        let mut table: Arc<dyn Table> = scripts[index].table();
        if times {
            table = update(&table, &TIME_COLUMNS, &oxhaven_expr::Compiler).expect("an update");
        }
        if changes {
            table
        } else {
            frozen(&*table)
        }
    };
    let left = table(0, live != Live::Right);
    let right = table(1, live != Live::Left);

    let mut cases: Vec<(String, Arc<ConsistencyChecker>)> = Vec::new();
    for &(exact, stamp) in joins {
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

/// The seeds of a full run: 40, of which the 10 multiples of four start with a left table
/// without rows.
const SEEDS: std::ops::Range<u64> = 0..40;

fn run_all(live: Live) -> usize {
    test_seeds(SEEDS).map(|seed| run(seed, 30, live)).sum()
}

#[test]
fn random_updates_of_both_tables() {
    let seeds = test_seeds(SEEDS).count();
    assert_eq!(run_all(Live::Both), seeds * 30 * JOINS.len() * RULES.len());
}

#[test]
fn random_updates_of_the_left_table() {
    let seeds = test_seeds(SEEDS).count();
    assert_eq!(run_all(Live::Left), seeds * 30 * JOINS.len() * RULES.len());
}

#[test]
fn random_updates_of_the_right_table() {
    // One seed in four starts with a left table without rows, whose joins are static.
    let seeds = test_seeds(SEEDS)
        .filter(|seed| !seed.is_multiple_of(4))
        .count();
    assert_eq!(run_all(Live::Right), seeds * 30 * JOINS.len() * RULES.len());
}

/// Stamps and exact-match columns of the `java.time` types, made by formulas, with both
/// tables changing, then each alone.
#[test]
fn random_updates_of_date_and_time_stamps() {
    for live in [Live::Both, Live::Left, Live::Right] {
        let compared: usize = test_seeds(SEEDS)
            .map(|seed| run_joins(seed, 20, live, &TIME_JOINS, true))
            .sum();
        let seeds = test_seeds(SEEDS)
            .filter(|seed| live != Live::Right || !seed.is_multiple_of(4))
            .count();
        assert_eq!(
            compared,
            seeds * 20 * TIME_JOINS.len() * RULES.len(),
            "{live:?}"
        );
    }
}
