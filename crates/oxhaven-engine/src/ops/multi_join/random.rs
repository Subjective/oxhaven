//! Multi joins of randomly changing tables, compared after every cycle with the same join
//! of static copies of the tables.
//!
//! A join that has followed its tables does not order its rows as a new join of the same
//! tables does: a key keeps the place it first had, also after every table has dropped it
//! and one has it again. So the test keeps the order in which keys first appeared, by the
//! rule of [`multi_join`]'s documentation, and puts the rows of the static join in it.

use super::{multi_join, JoinAddition, JoinMatch, MultiJoinError, MultiJoinInput};
use crate::ops::join::keys::{Key, KeyColumns};
use crate::ops::sort::testing::{frozen, graph};
use arrow::array::UInt32Array;
use arrow::compute::take;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{
    definition, ConsistencyChecker, RandomUpdates, ReferenceTable, ScriptedTable, TableChange,
    TableSnapshot,
};
use std::sync::{Arc, Mutex};

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

const TABLES: usize = 3;

/// Which of the three tables change.
const LIVE: [[bool; TABLES]; 4] = [
    [true, true, true],
    [true, false, false],
    [false, false, true],
    [false, true, true],
];

fn key_columns(keys: &[&str]) -> KeyColumns {
    let index = |key: &&str| COLUMNS.iter().position(|(name, _)| name == key).unwrap();
    KeyColumns {
        columns: keys.iter().map(index).collect(),
    }
}

/// Each table gives `V`, `S` and `D` under names of its own. The last table lists the key
/// columns in reverse.
fn join(tables: &[Arc<dyn Table>], keys: &[&str]) -> Result<Arc<dyn Table>, MultiJoinError> {
    let inputs: Vec<MultiJoinInput> = tables
        .iter()
        .enumerate()
        .map(|(index, table)| {
            let mut on: Vec<JoinMatch> = keys.iter().map(|key| JoinMatch::same(*key)).collect();
            if index == TABLES - 1 {
                on.reverse();
            }
            let add = ["V", "S", "D"]
                .iter()
                .map(|column| JoinAddition::renamed(format!("{column}{index}"), *column))
                .collect();
            MultiJoinInput {
                table: table.clone(),
                on,
                add,
            }
        })
        .collect();
    multi_join(&inputs)
}

/// The join of static copies of `tables`, its rows in the order `order` has their keys in.
fn expected(
    tables: &[Arc<dyn Table>],
    keys: &[&str],
    order: &[Key],
) -> Result<Arc<dyn Table>, MultiJoinError> {
    let copies: Vec<Arc<dyn Table>> = tables.iter().map(|table| frozen(&**table)).collect();
    let joined = join(&copies, keys)?;
    let of_result = KeyColumns {
        columns: (0..keys.len()).collect(),
    };
    let found = of_result.read(&*joined, &joined.row_set(), false).unwrap();
    let place = |key: &Key| -> usize {
        let place = order.iter().position(|seen| seen == key);
        place.expect("a key of the result that never appeared")
    };
    let mut positions: Vec<u32> = (0..found.len() as u32).collect();
    positions.sort_by_key(|position| place(&found[*position as usize]));
    let positions = UInt32Array::from(positions);
    let snapshot = TableSnapshot::current(&*joined).unwrap();
    let columns = snapshot
        .columns
        .iter()
        .map(|values| take(values, &positions, None).unwrap())
        .collect();
    Ok(ReferenceTable::new_static(
        joined.definition().clone(),
        RowSet::flat(found.len() as u64),
        columns,
    ))
}

/// Appends the keys of `rows` of `table` that `order` does not have, in row order.
fn appear(order: &mut Vec<Key>, columns: &KeyColumns, table: &dyn Table, rows: &RowSet) {
    for key in columns.read(table, rows, false).unwrap() {
        if !order.contains(&key) {
            order.push(key);
        }
    }
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

/// Runs the join on every key of [`KEYS`] over three random tables for `steps` cycles.
///
/// After each cycle a result must be consistent and equal to the join of static copies of
/// the tables, put in the order in which the keys first appeared, and it must have failed
/// exactly when that join is refused: when a table has two rows of one key. A failed
/// result is not looked at again.
fn run(seed: u64, steps: usize, live: [bool; TABLES], tally: &mut Tally) {
    let graph = graph();
    let mut random = RandomUpdates::new(seed);
    let scripts: Vec<Arc<ScriptedTable>> = (0..TABLES as u64)
        .map(|index| {
            let size = if (seed + index).is_multiple_of(4) {
                0
            } else {
                3
            };
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
        })
        .collect();
    let tables: Vec<Arc<dyn Table>> = scripts
        .iter()
        .zip(live)
        .map(|(script, changes)| -> Arc<dyn Table> {
            let table = script.table();
            if changes {
                table
            } else {
                frozen(&*table)
            }
        })
        .collect();

    struct Case {
        name: String,
        keys: &'static [&'static str],
        /// Every key a table has had, in the order of first appearance.
        order: Arc<Mutex<Vec<Key>>>,
        result: Arc<dyn Table>,
        checker: Arc<ConsistencyChecker>,
    }
    let mut cases = Vec::new();
    for keys in KEYS {
        let name = format!("on {keys:?}, {live:?} live, seed {seed}");
        let Ok(result) = join(&tables, keys) else {
            tally.refused += 1;
            continue;
        };
        let mut order = Vec::new();
        for table in &tables {
            appear(&mut order, &key_columns(keys), &**table, &table.row_set());
        }
        let order = Arc::new(Mutex::new(order));
        let recompute = {
            let (tables, order) = (tables.clone(), order.clone());
            move || {
                let order = order.lock().unwrap();
                expected(&tables, keys, &order).expect("the caller has seen this join succeed")
            }
        };
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
        if let Err(e) = checker.verify() {
            panic!("{name}, at the start: {e}");
        }
        cases.push(Case {
            name,
            keys,
            order,
            result,
            checker,
        });
    }

    let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
    for step in 0..steps {
        let mut changes: Vec<Option<TableChange>> = Vec::new();
        for (index, script) in scripts.iter().enumerate() {
            let pick = chooser.values(&ColumnType::Char, 1);
            let pick = pick
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .unwrap();
            // A null is one pick in ten: about a third of the tables sit out a cycle.
            let sits_out = !arrow::array::Array::is_null(pick, 0)
                && (pick.value(0) as usize + index).is_multiple_of(3);
            if !live[index] || sits_out {
                changes.push(None);
                continue;
            }
            let change = random.next_change(&*script.table(), 2);
            script.push(change.clone());
            changes.push(Some(change));
        }
        graph.run_cycle();
        cases.retain(|case| {
            let name = &case.name;
            let columns = key_columns(case.keys);
            let mut order = case.order.lock().unwrap();
            // The tables in turn; of one table the modified rows, if a key column of
            // theirs changed, and then the added rows.
            for (table, change) in tables.iter().zip(&changes) {
                let Some(TableChange { update, .. }) = change else {
                    continue;
                };
                if update.modified_columns.contains_any(&columns.columns) {
                    appear(&mut order, &columns, &**table, &update.modified);
                }
                appear(&mut order, &columns, &**table, &update.added);
            }
            let outcome = expected(&tables, case.keys, &order);
            drop(order);
            match outcome {
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
                        matches!(error, MultiJoinError::DuplicateKey { .. }),
                        "{name}, step {step}: the static join is refused with {error}"
                    );
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

fn run_all(live: [bool; TABLES]) -> Tally {
    let mut tally = Tally::default();
    for seed in 0..120 {
        run(seed, 20, live, &mut tally);
    }
    tally
}

#[test]
fn random_updates_of_all_tables() {
    let tally = run_all(LIVE[0]);
    // Enough of both outcomes that neither check is idle.
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}

#[test]
fn random_updates_of_the_first_table() {
    let tally = run_all(LIVE[1]);
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}

#[test]
fn random_updates_of_the_last_table() {
    let tally = run_all(LIVE[2]);
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}

#[test]
fn random_updates_of_the_last_two_tables() {
    let tally = run_all(LIVE[3]);
    assert!(tally.compared > 5000 && tally.failed > 100, "{tally:?}");
}
