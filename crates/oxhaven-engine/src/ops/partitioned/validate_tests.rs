//! The key checks of a proxy's operations on two partitioned tables, and the consistent
//! key columns. The scripted cases are what Java 42.9 was observed to do
//! (`docs/PARTITIONED.md`, "The proxy").

use super::tests::{keys, probe, probe_columns, Probe};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::merge::merge;
use crate::ops::sort::head;
use arrow::array::{Array, Int32Array};
use oxhaven_testkit::{
    at_least_for, definition, ints, test_seeds, ConsistencyChecker, RandomUpdates, ReferenceTable,
    ScriptedTable, TableChange,
};
use std::collections::{HashMap, HashSet};

/// `K`, `W` and `N` with the given values; a null where `None`.
fn static_table(k: &[i32], w: &[Option<i32>]) -> Arc<dyn Table> {
    let n: Vec<i32> = (0..k.len() as i32).collect();
    ReferenceTable::new_static(
        definition(&[
            ("K", ColumnType::Int),
            ("W", ColumnType::Int),
            ("N", ColumnType::Int),
        ]),
        RowSet::flat(k.len() as u64),
        vec![
            ints(k.iter().copied()),
            Arc::new(Int32Array::from(w.to_vec())),
            ints(n),
        ],
    )
}

fn parts(k: &[i32], w: &[Option<i32>]) -> PartitionedTable {
    partition_by(&static_table(k, w), &["K"], false).unwrap()
}

fn names(columns: &[&str]) -> Vec<String> {
    columns.iter().map(|name| name.to_string()).collect()
}

fn first_rows(tables: &[Arc<dyn Table>]) -> Result<Option<Arc<dyn Table>>, TableFailure> {
    Ok(Some(
        head(&tables[0], 1).map_err(|error| failure(error.to_string()))?,
    ))
}

fn both(tables: &[Arc<dyn Table>]) -> Result<Option<Arc<dyn Table>>, TableFailure> {
    Ok(Some(
        merge(tables).map_err(|error| failure(error.to_string()))?,
    ))
}

#[test]
fn a_partition_that_keeps_its_keys_has_them_as_consistent_key_columns() {
    let table = static_table(&[0, 1, 0], &[Some(1), Some(2), Some(3)]);
    let kept = partition_by(&table, &["K"], false).unwrap();
    assert_eq!(kept.consistent_key_columns(), ["K"]);
    let dropped = partition_by(&table, &["K"], true).unwrap();
    assert!(dropped.consistent_key_columns().is_empty());

    // a filter or a sort of the underlying table keeps them; a transform has none
    // until it is told, and is told only of key columns
    let same = kept.with_table(kept.table().clone()).unwrap();
    assert_eq!(same.consistent_key_columns(), ["K"]);
    let transformed = kept.transform(Arc::new(first_rows), &[]).unwrap();
    assert!(transformed.consistent_key_columns().is_empty());
    let told = transformed.with_consistent_key_columns(&names(&["W", "K"]));
    assert_eq!(told.consistent_key_columns(), ["K"]);
}

#[test]
fn partitioned_tables_with_other_keys_are_refused_when_keys_must_match() {
    let some = Some(0);
    let lhs = parts(&[0, 1, 2], &[some; 3]);
    let same = parts(&[2, 0, 1, 1], &[some; 4]);
    let (checked_lhs, checked_same) = require_matching_keys(&lhs, &same).unwrap();
    // nothing is refreshing: the tables are used as they are
    assert!(Arc::ptr_eq(checked_lhs.table(), lhs.table()));
    assert!(Arc::ptr_eq(checked_same.table(), same.table()));

    // Observed: "IllegalArgumentException: Partitioned table arguments have
    // non-matching keys", whichever side has the key the other has not
    for other in [parts(&[0, 1], &[some; 2]), parts(&[0, 1, 2, 3], &[some; 4])] {
        for (first, second) in [(&lhs, &other), (&other, &lhs)] {
            let error = require_matching_keys(first, second)
                .map(|_| ())
                .unwrap_err();
            assert!(
                matches!(&error, PartitionedError::IllegalArgument(text) if text.contains("non-matching keys")),
                "{error}"
            );
        }
    }
}

#[test]
fn a_join_key_in_two_constituents_is_refused() {
    let table = parts(&[0, 1, 0, 1], &[Some(5), Some(6), Some(7), Some(5)]);
    // N is 0, 1, 2, 3: every value in one constituent
    table.require_distinct_join_keys(&names(&["N"])).unwrap();
    // each K is in its own constituent, with whatever else
    table
        .require_distinct_join_keys(&names(&["K", "W"]))
        .unwrap();

    // Observed: "IllegalArgumentException: Partitioned table "QueryTable" has join keys
    // found in multiple constituents": W is 5 in both
    let overlap = |partitioned: &PartitionedTable, columns: &[&str]| {
        let refused = partitioned.require_distinct_join_keys(&names(columns));
        let error = refused.map(|_| ()).unwrap_err();
        assert!(
            matches!(&error, PartitionedError::IllegalArgument(text) if text.contains("multiple constituents")),
            "{error}"
        );
    };
    overlap(&table, &["W"]);
    // Observed: a join without match columns has the one key "EmptyTuple" in every
    // constituent that has a row
    overlap(&table, &[]);
    let one = parts(&[4, 4], &[Some(1), Some(1)]);
    one.require_distinct_join_keys(&[]).unwrap();
    one.require_distinct_join_keys(&names(&["W"])).unwrap();

    let missing = table.require_distinct_join_keys(&names(&["Nope"]));
    assert!(missing.is_err());
}

#[test]
fn a_null_join_key_of_one_column_is_refused_as_java_refuses_it() {
    // Observed: "java.lang.NullPointerException" for a null in the one match column,
    // in whatever constituent, and none for a null in one of two match columns
    let table = parts(&[0, 1, 0], &[Some(5), None, Some(7)]);
    let error = table.require_distinct_join_keys(&names(&["W"]));
    let error = error.map(|_| ()).unwrap_err();
    assert!(
        matches!(&error, PartitionedError::IllegalArgument(text) if text.contains("NullPointerException")),
        "{error}"
    );
    table
        .require_distinct_join_keys(&names(&["K", "W"]))
        .unwrap();
    table.require_distinct_join_keys(&names(&["N"])).unwrap();
}

/// A second table on the graph of the probe, with its keys 1 and 2.
fn second(graph: &Arc<SteppedUpdateGraph>) -> (Arc<ScriptedTable>, PartitionedTable) {
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&[
            ("K", ColumnType::Int),
            ("N", ColumnType::Int),
            ("V", ColumnType::Double),
        ]),
        RowSet::flat(2),
        probe_columns(&[2, 1], &[21, 31], &[20.0, 30.0]),
    );
    let table: Arc<dyn Table> = source.table();
    (source, partition_by(&table, &["K"], false).unwrap())
}

#[test]
fn a_key_that_only_one_side_gets_in_a_cycle_fails_what_was_made_of_both() {
    let Probe {
        graph,
        source,
        partitioned: lhs,
    } = probe(false);
    let (other_source, rhs) = second(&graph);
    let (checked_lhs, checked_rhs) = require_matching_keys(&lhs, &rhs).unwrap();
    let pairs = checked_lhs
        .partitioned_transform(&checked_rhs, Arc::new(both), &[])
        .unwrap();
    let unchecked = lhs
        .partitioned_transform(&rhs, Arc::new(both), &[])
        .unwrap();
    let merged = pairs.merge().unwrap();
    let checks = [
        ConsistencyChecker::attach(checked_lhs.table().clone()),
        ConsistencyChecker::attach(checked_rhs.table().clone()),
        ConsistencyChecker::attach(pairs.table().clone()),
    ];
    let verify = || checks.iter().for_each(|check| check.assert_consistent());
    assert_eq!(keys(&pairs), ["1", "2"]);

    // Both get the key 3 in one cycle, and rows for keys they have: nothing fails.
    source.push(TableChange::new().add([3, 4], probe_columns(&[3, 1], &[13, 14], &[4.0, 5.0])));
    other_source.push(TableChange::new().add([2], probe_columns(&[3], &[41], &[40.0])));
    graph.run_cycle();
    verify();
    assert_eq!(keys(&pairs), ["1", "2", "3"]);
    assert_eq!(keys(&checked_lhs), ["1", "2", "3"]);
    assert!(!pairs.table().is_failed() && !merged.is_failed());

    // Both lose the key 2 in one cycle.
    source.push(TableChange::new().remove([1]));
    other_source.push(TableChange::new().remove([0]));
    graph.run_cycle();
    verify();
    assert_eq!(keys(&pairs), ["1", "3"]);
    assert!(!pairs.table().is_failed());

    // Observed: only the first gets the key 7, and the result fails, with its merge. The
    // partitioned tables themselves, and what was made of them without the check, go on.
    source.push(TableChange::new().add([9], probe_columns(&[7], &[19], &[9.0])));
    graph.run_cycle();
    assert!(checked_lhs.table().is_failed() && checked_rhs.table().is_failed());
    assert!(pairs.table().is_failed() && merged.is_failed());
    assert!(!lhs.table().is_failed() && !rhs.table().is_failed());
    assert!(!unchecked.table().is_failed());
    assert_eq!(keys(&unchecked), ["1", "3"]);

    // Made now, it is refused; once the other has the key too, it is made.
    assert!(require_matching_keys(&lhs, &rhs).is_err());
    other_source.push(TableChange::new().add([7], probe_columns(&[7], &[71], &[70.0])));
    graph.run_cycle();
    assert!(require_matching_keys(&lhs, &rhs).is_ok());
    assert!(pairs.table().is_failed());
}

#[test]
fn a_static_side_is_not_copied_and_the_refreshing_side_fails_alone() {
    let Probe {
        graph,
        source,
        partitioned: refreshing,
    } = probe(false);
    let fixed = parts(&[1, 2], &[Some(0); 2]);
    let (checked_fixed, checked) = require_matching_keys(&fixed, &refreshing).unwrap();
    // Behavior: #validated answers the table itself for one that is not refreshing
    assert!(Arc::ptr_eq(checked_fixed.table(), fixed.table()));
    assert!(checked.table().is_refreshing());
    source.push(TableChange::new().add([3], probe_columns(&[3], &[13], &[4.0])));
    graph.run_cycle();
    assert!(checked.table().is_failed());
    assert!(!refreshing.table().is_failed());
}

#[test]
fn a_join_key_may_move_between_constituents_and_may_not_be_in_two() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    // N is 10 and 12 for the key 1, 11 for the key 2
    let checked = partitioned
        .require_distinct_join_keys(&names(&["N"]))
        .unwrap();
    let again = partitioned
        .require_distinct_join_keys(&names(&["K", "N"]))
        .unwrap();
    let check = ConsistencyChecker::attach(checked.table().clone());
    assert!(checked.table().is_refreshing());
    assert_eq!(checked.consistent_key_columns(), ["K"]);

    // The row N=12 moves from the key 1 to the key 2, and the row N=10 to a new key:
    // each join key is still in one constituent when the cycle ends.
    source.push(TableChange::new().modify([0, 2], [(0, ints([5, 2]))]));
    graph.run_cycle();
    check.assert_consistent();
    assert!(!checked.table().is_failed());
    assert_eq!(keys(&checked), ["2", "5"]);

    // New join keys, and one that is removed.
    source.push(
        TableChange::new()
            .add([5, 6], probe_columns(&[5, 9], &[15, 16], &[5.0, 6.0]))
            .remove([1]),
    );
    graph.run_cycle();
    check.assert_consistent();
    assert!(!checked.table().is_failed());

    // Observed: the join key 15 now also in the constituent of the key 2 fails the
    // check. A check of K and N together is not concerned.
    source.push(TableChange::new().add([7], probe_columns(&[2], &[15], &[7.0])));
    graph.run_cycle();
    assert!(checked.table().is_failed());
    assert!(!again.table().is_failed());
    assert!(!partitioned.table().is_failed());
    assert!(partitioned
        .require_distinct_join_keys(&names(&["N"]))
        .is_err());
}

#[test]
fn a_null_join_key_that_arrives_in_a_cycle_fails_the_check() {
    let graph = SteppedUpdateGraph::new();
    let columns = |k: &[i32], w: &[Option<i32>]| -> Vec<ArrayRef> {
        vec![
            Arc::new(Int32Array::from(k.to_vec())),
            Arc::new(Int32Array::from(w.to_vec())),
        ]
    };
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&[("K", ColumnType::Int), ("W", ColumnType::Int)]),
        RowSet::flat(2),
        columns(&[1, 2], &[Some(1), Some(2)]),
    );
    let table: Arc<dyn Table> = source.table();
    let partitioned = partition_by(&table, &["K"], false).unwrap();
    let checked = partitioned
        .require_distinct_join_keys(&names(&["W"]))
        .unwrap();
    let pairs = partitioned
        .require_distinct_join_keys(&names(&["K", "W"]))
        .unwrap();
    source.push(TableChange::new().add([2], columns(&[2], &[None])));
    graph.run_cycle();
    assert!(checked.table().is_failed());
    assert!(!pairs.table().is_failed());
}

/// Random changes of a table partitioned by `K`, with the check of the join key `G`.
/// The checked table fails in the first cycle after which a value of `G` is in two
/// constituents or is null, and until then is the underlying table, change for change.
fn random_join_keys(seed: u64) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    // Rows of their own keys to start with: the random values of later rows overlap soon.
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&[("K", ColumnType::Int), ("G", ColumnType::Int)]),
        RowSet::flat(4),
        vec![ints([1, 1, 2, 3]), ints([100, 101, 102, 103])],
    );
    let table: Arc<dyn Table> = source.table();
    let partitioned = partition_by(&table, &["K"], false).unwrap();
    let checked = partitioned
        .require_distinct_join_keys(&names(&["G"]))
        .unwrap();
    let check = ConsistencyChecker::attach(checked.table().clone());
    let overlaps = || {
        let rows = table.row_set();
        let read = |index: usize| table.column(index).chunk(&rows).unwrap();
        let (k, g) = (read(0), read(1));
        let (k, g) = (
            k.as_primitive::<arrow::datatypes::Int32Type>(),
            g.as_primitive::<arrow::datatypes::Int32Type>(),
        );
        let mut owners: HashMap<i32, HashSet<Option<i32>>> = HashMap::new();
        let mut null = false;
        for row in 0..g.len() {
            match g.is_valid(row) {
                true => {
                    let key = k.is_valid(row).then(|| k.value(row));
                    owners.entry(g.value(row)).or_default().insert(key);
                }
                false => null = true,
            }
        }
        null || owners.values().any(|keys| keys.len() > 1)
    };
    let mut cycles = 0;
    for _ in 0..12 {
        source.push(random.next_change(&*table, 2));
        graph.run_cycle();
        cycles += 1;
        let expected = overlaps();
        assert_eq!(
            checked.table().is_failed(),
            expected,
            "seed {seed}, cycle {cycles}"
        );
        if expected {
            break;
        }
        check.assert_consistent();
        assert_eq!(keys(&checked), keys(&partitioned), "seed {seed}");
    }
    cycles
}

#[test]
fn random_changes_fail_the_join_key_check_exactly_when_a_key_overlaps() {
    let mut cycles = 0;
    for seed in test_seeds(0..40) {
        cycles += random_join_keys(seed);
    }
    assert!(cycles >= at_least_for(60, 0..40), "only {cycles} cycles");
}
