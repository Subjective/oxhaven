//! Joins of randomly changing tables, compared after every cycle with the same join of
//! static copies of the tables.

use super::{natural_join, JoinAddition, JoinError, JoinMatch, NaturalJoinType};
use crate::ops::sort::testing::{frozen, graph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{
    at_least_for, definition, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable,
};
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

const SEEDS: std::ops::Range<u64> = 0..40;

fn run_all(live: Live) -> Tally {
    let mut tally = Tally::default();
    for seed in test_seeds(SEEDS) {
        run(seed, 30, live, &mut tally);
    }
    tally
}

/// Enough of both outcomes that neither check is idle.
fn assert_enough(tally: &Tally) {
    let (compared, failed) = (at_least_for(5000, SEEDS), at_least_for(100, SEEDS));
    assert!(
        tally.compared as u64 > compared && tally.failed as u64 > failed,
        "{tally:?}"
    );
}

#[test]
fn random_updates_of_both_tables() {
    assert_enough(&run_all(Live::Both));
}

#[test]
fn random_updates_of_the_left_table() {
    assert_enough(&run_all(Live::Left));
}

#[test]
fn random_updates_of_the_right_table() {
    assert_enough(&run_all(Live::Right));
}

/// The slots of a [`KeyTable`](crate::ops::key_table::KeyTable) against the keys the joins
/// compared before they hashed: two rows have one slot exactly when their keys are equal.
#[test]
fn rows_share_a_slot_exactly_when_their_keys_are_equal() {
    use super::keys::KeyColumns;
    use crate::ops::key_table::{KeyTable, NO_SLOT};
    let mut pairs = [0u64; 2];
    for seed in test_seeds(SEEDS) {
        let mut random = RandomUpdates::new(seed);
        let size: usize = 40;
        let values = COLUMNS
            .iter()
            .map(|(_, column_type)| random.values(column_type, size as u64))
            .collect();
        let script = ScriptedTable::new(
            graph(),
            definition(&COLUMNS),
            RowSet::flat(size as u64),
            values,
        );
        let table = script.table();
        let (first, second) = (RowSet::from_range(0, 24), RowSet::from_range(25, 39));
        for names in KEYS {
            let columns = names
                .iter()
                .map(|name| table.definition().index_of(name).unwrap())
                .collect();
            let columns = KeyColumns { columns };
            let keys = columns.read(&*table, &table.row_set(), false).unwrap();
            // The first rows are inserted, and the rest are looked up and then inserted.
            let mut slots = KeyTable::new();
            let chunks = columns.chunks(&*table, &first, false).unwrap();
            let mut row_slots = slots.insert(&chunks, 25);
            let chunks = columns.chunks(&*table, &second, false).unwrap();
            let found = slots.find(&chunks, 15);
            let inserted = slots.insert(&chunks, 15);
            for (row, (found, inserted)) in found.iter().zip(&inserted).enumerate() {
                let known = keys[..25].contains(&keys[25 + row]);
                assert_eq!(*found != NO_SLOT, known, "{names:?}, seed {seed}");
                assert!(!known || found == inserted, "{names:?}, seed {seed}");
            }
            row_slots.extend(inserted);
            assert!(row_slots.iter().all(|slot| (*slot as usize) < slots.len()));
            for a in 0..size {
                for b in 0..a {
                    let equal = keys[a] == keys[b];
                    assert_eq!(
                        row_slots[a] == row_slots[b],
                        equal,
                        "{names:?}, seed {seed}, rows {a} and {b}"
                    );
                    pairs[usize::from(equal)] += 1;
                }
            }
        }
    }
    // Both answers were given often.
    let floor = at_least_for(1000, SEEDS);
    assert!(pairs[0] > floor && pairs[1] > floor, "{pairs:?}");
}
