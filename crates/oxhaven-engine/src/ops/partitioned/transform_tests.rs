//! `transform` and `partitioned_transform`. The scripted cases replay what Java 42.9 was
//! observed to do (`docs/PARTITIONED.md`, "transform"): which constituents the function
//! is called for, when, and what a result that Java refuses does to the partitioned table.

use super::tests::{groups, keys, names, probe, probe_columns, rows, seven_rows, Probe};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::merge::merge;
use crate::ops::select::drop_columns;
use crate::ops::sort::{head, reverse, tail};
use arrow::datatypes::Int32Type;
use oxhaven_contracts::schema::TableClass;
use oxhaven_testkit::{
    at_least_for, definition, ints, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable,
    TableChange,
};
use std::sync::atomic::{AtomicUsize, Ordering};

type Made = Result<Option<Arc<dyn Table>>, TableFailure>;

/// A transformer that counts its calls.
fn counted(
    calls: &Arc<AtomicUsize>,
    function: impl Fn(&[Arc<dyn Table>]) -> Made + Send + Sync + 'static,
) -> Transformer {
    let calls = calls.clone();
    Arc::new(move |tables: &[Arc<dyn Table>]| {
        calls.fetch_add(1, Ordering::SeqCst);
        function(tables)
    })
}

fn last_row(tables: &[Arc<dyn Table>]) -> Made {
    Ok(Some(tail(&tables[0], 1).map_err(|error| failure(error.to_string()))?))
}

fn reversed(tables: &[Arc<dyn Table>]) -> Made {
    Ok(Some(reverse(&tables[0]).map_err(|error| failure(error.to_string()))?))
}

/// The rows of the first table, then those of the second.
fn both(tables: &[Arc<dyn Table>]) -> Made {
    Ok(Some(merge(tables).map_err(|error| failure(error.to_string()))?))
}

fn taken(calls: &AtomicUsize) -> usize {
    calls.swap(0, Ordering::SeqCst)
}

#[test]
fn a_transformed_partition_keeps_its_rows_and_holds_what_the_function_made() {
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let transformed = partitioned
        .transform(counted(&calls, reversed), &[])
        .unwrap();
    // once for each constituent, and once for a constituent without rows
    assert_eq!(taken(&calls), 4);
    assert_eq!(
        transformed.table().definition().columns()[1].column_type,
        ColumnType::Table(TableClass::Table)
    );
    assert!(transformed.table().is_flat());
    assert!(!transformed.table().is_refreshing());
    assert_eq!(transformed.key_columns(), ["K"]);
    assert!(transformed.unique_keys());
    assert!(!transformed.constituent_changes_permitted());
    assert_eq!(
        rows(&*transformed.merge().unwrap()),
        ["0|9.0", "0|4.5", "0|0.0", "1|6.0", "1|1.5", "2|7.5", "2|3.0"]
    );
    // the key column still reads the keys of the partition: 0, 1, 2
    let keys = transformed
        .table()
        .column(0)
        .chunk(&transformed.table().row_set())
        .unwrap();
    assert_eq!(keys.as_primitive::<Int32Type>().values(), &[0, 1, 2]);

    assert_eq!(partitioned.empty_constituent().unwrap().row_set().size(), 0);
    assert_eq!(
        partitioned.empty_constituent().unwrap().definition(),
        partitioned.constituent_definition()
    );
}

#[test]
fn the_constituents_have_the_columns_the_function_gives_a_table_without_rows() {
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    let without_keys: Transformer = Arc::new(|tables: &[Arc<dyn Table>]| {
        Ok(Some(
            drop_columns(&tables[0], &["K"]).map_err(|error| failure(error.to_string()))?,
        ))
    });
    let transformed = partitioned.transform(without_keys, &[]).unwrap();
    assert_eq!(names(transformed.constituent_definition()), ["V"]);
    assert_eq!(names(transformed.table().definition()), ["K", CONSTITUENT]);
    assert_eq!(transformed.merge().unwrap().row_set().size(), 7);
}

#[test]
fn what_java_refuses_of_a_function_is_refused() {
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    let transform = |function: fn(&[Arc<dyn Table>]) -> Made| {
        partitioned
            .transform(Arc::new(function), &[])
            .map(|_| ())
            .unwrap_err()
    };

    // Observed: "NullPointerException: Cannot invoke ... because
    // "resultEmptyConstituent" is null" when it answers null for every table
    let nothing = transform(|_| Ok(None));
    assert!(matches!(nothing, PartitionedError::NullPointer(_)), "{nothing}");

    // Observed: "IllegalStateException: Encountered null constituent"
    let nothing_for_rows = transform(|tables| match tables[0].row_set().is_empty() {
        true => Ok(Some(tables[0].clone())),
        false => Ok(None),
    });
    assert_eq!(
        nothing_for_rows,
        PartitionedError::IllegalState("Encountered null constituent".to_string())
    );

    // Observed: "IncompatibleTableDefinitionException: Table definition
    // incompatibilities: \n\texpected column 'V' is missing in constituent"
    let other_columns = transform(|tables| match tables[0].row_set().is_empty() {
        true => Ok(Some(tables[0].clone())),
        false => Ok(Some(
            drop_columns(&tables[0], &["V"]).map_err(|error| failure(error.to_string()))?,
        )),
    });
    assert_eq!(
        other_columns.to_string(),
        "Table definition incompatibilities: \n\texpected column 'V' is missing in constituent"
    );

    let raised = transform(|_| Err(failure("no")));
    assert_eq!(raised, PartitionedError::Table(failure("no")));
}

#[test]
fn a_static_partition_holds_refreshing_tables_only_with_a_dependency() {
    let graph = SteppedUpdateGraph::new();
    let other = ScriptedTable::new(
        graph.clone(),
        definition(&[("K", ColumnType::Int), ("V", ColumnType::Double)]),
        RowSet::flat(1),
        vec![ints([7]), oxhaven_testkit::doubles([70.0])],
    );
    let ticking: Arc<dyn Table> = other.table();
    let partitioned = partition_by(&seven_rows(), &["K"], false).unwrap();
    let with_ticking = |ticking: Arc<dyn Table>| -> Transformer {
        Arc::new(move |tables: &[Arc<dyn Table>]| both(&[tables[0].clone(), ticking.clone()]))
    };

    // Observed: "IllegalStateException: Static partitioned tables cannot contain
    // refreshing constituents. ..."
    let refused = partitioned.transform(with_ticking(ticking.clone()), &[]);
    assert!(
        matches!(&refused, Err(PartitionedError::IllegalState(text)) if text.starts_with("Static")),
        "{:?}",
        refused.err()
    );
    // a dependency that is not refreshing is none
    let refused = partitioned.transform(with_ticking(ticking.clone()), &[seven_rows()]);
    assert!(matches!(refused, Err(PartitionedError::IllegalState(_))));

    // Observed: with a refreshing dependency the result is refreshing, its constituents
    // may be, and its constituents cannot change.
    let transformed = partitioned
        .transform(
            with_ticking(ticking.clone()),
            std::slice::from_ref(&ticking),
        )
        .unwrap();
    assert!(transformed.table().is_refreshing());
    assert!(!transformed.constituent_changes_permitted());
    let merged = transformed.merge().unwrap();
    let checks = [
        ConsistencyChecker::attach(transformed.table().clone()),
        ConsistencyChecker::attach(merged.clone()),
    ];
    assert_eq!(merged.row_set().size(), 7 + 3);
    other.push(TableChange::new().add([1], vec![ints([8]), oxhaven_testkit::doubles([80.0])]));
    graph.run_cycle();
    checks.iter().for_each(|check| check.assert_consistent());
    assert_eq!(merged.row_set().size(), 7 + 6);
    assert_eq!(transformed.table().row_set().size(), 3);

    // Observed: the merge of a refreshing partitioned table whose constituents cannot
    // change has the attributes they share, here the append-only of a `head` of a table
    // that is not refreshing.
    let first_rows: Transformer = Arc::new(|tables: &[Arc<dyn Table>]| {
        Ok(Some(
            head(&tables[0], 1).map_err(|error| failure(error.to_string()))?,
        ))
    });
    let heads = partitioned
        .transform(first_rows, std::slice::from_ref(&ticking))
        .unwrap();
    assert!(heads.table().is_refreshing());
    let merged = heads.merge().unwrap();
    assert!(merged.is_refreshing());
    assert!(merged.attributes().append_only);
    assert!(!merged.attributes().add_only);
    assert_eq!(rows(&*merged), ["0|0.0", "1|1.5", "2|3.0"]);
}

#[test]
fn the_function_is_called_in_the_cycle_for_each_constituent_that_appears() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let transformed = partitioned
        .transform(counted(&calls, last_row), &[])
        .unwrap();
    assert_eq!(taken(&calls), 3);
    assert!(transformed.table().is_refreshing());
    assert!(transformed.constituent_changes_permitted());
    assert!(transformed.unique_keys());
    assert_eq!(names(transformed.constituent_definition()), ["K", "N", "V"]);
    let merged = transformed.merge().unwrap();
    let first = transformed.constituents().unwrap().remove(0);
    let checks = [
        ConsistencyChecker::attach(transformed.table().clone()),
        ConsistencyChecker::attach(merged.clone()),
        ConsistencyChecker::attach(first.clone()),
    ];
    let verify = || checks.iter().for_each(|check| check.assert_consistent());
    assert_eq!(rows(&*merged), ["1|12|3.0", "2|11|2.0"]);

    // A new key: one call, for its constituent. A row added to a constituent that is
    // there is the business of what the function made of it.
    source.push(TableChange::new().add(
        [3, 4],
        probe_columns(&[3, 1], &[13, 14], &[4.0, 5.0]),
    ));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 1);
    assert_eq!(keys(&transformed), ["1", "2", "3"]);
    assert_eq!(rows(&*merged), ["1|14|5.0", "2|11|2.0", "3|13|4.0"]);
    assert_eq!(rows(&*first), ["1|14|5.0"]);

    // The key 2 loses its row: its row of the underlying table goes, and no call. What
    // the function made for it is kept for the cycle, for those who read what the row
    // held, and given up on the next change.
    let made_for_two = {
        let cells = transformed.table().column(1);
        let cells = cells.chunk(&transformed.table().row_set()).unwrap();
        let made = tables_of(&cells).unwrap().remove(1).unwrap();
        Arc::downgrade(&made)
    };
    source.push(TableChange::new().remove([1]));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 0);
    assert_eq!(keys(&transformed), ["1", "3"]);
    assert_eq!(rows(&*merged), ["1|14|5.0", "3|13|4.0"]);
    assert!(made_for_two.upgrade().is_some());

    // It has rows again: the function is called again, with the constituent it was
    // called with before, which has the two rows by then.
    source.push(TableChange::new().add(
        [7, 8],
        probe_columns(&[2, 2], &[17, 18], &[7.0, 8.0]),
    ));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 1);
    assert_eq!(keys(&transformed), ["1", "2", "3"]);
    assert_eq!(rows(&*merged), ["1|14|5.0", "2|18|8.0", "3|13|4.0"]);
    assert!(made_for_two.upgrade().is_none());
    // the table held for the key 1 is still the constituent of the key 1
    assert_eq!(rows(&*first), ["1|14|5.0"]);

    // A row that moves to another key and a value that changes: no call.
    source.push(TableChange::new().modify(
        [4, 8],
        [(0, ints([3, 2])), (2, oxhaven_testkit::doubles([5.5, 8.5]))],
    ));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 0);
    assert_eq!(rows(&*merged), ["1|12|3.0", "2|18|8.5", "3|14|5.5"]);
    assert_eq!(rows(&*first), ["1|12|3.0"]);
}

/// A transform of the probe whose function does `broken` for the constituent of the key
/// 3, which appears in a cycle.
fn fails_in_a_cycle(broken: fn(&Arc<dyn Table>) -> Made) -> String {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    let function: Transformer = Arc::new(move |tables: &[Arc<dyn Table>]| {
        match rows(&*tables[0]).first().map(String::as_str) {
            Some("3|13|4.0") => broken(&tables[0]),
            _ => last_row(tables),
        }
    });
    let transformed = partitioned.transform(function, &[]).unwrap();
    let merged = transformed.merge().unwrap();
    let recorder = oxhaven_testkit::Recorder::attach(transformed.table().clone());
    source.push(TableChange::new().add([3], probe_columns(&[3], &[13], &[4.0])));
    graph.run_cycle();
    assert!(transformed.table().is_failed());
    assert!(merged.is_failed());
    // the partitioned table it was made of goes on
    assert!(!partitioned.table().is_failed());
    assert_eq!(keys(&partitioned), ["1", "2", "3"]);
    let mut events = recorder.events();
    assert_eq!(events.len(), 1);
    events.remove(0).expect_err("a failure").message
}

#[test]
fn a_function_that_java_refuses_in_a_cycle_fails_the_result() {
    // Observed for each: the underlying table and the merge are failed after the cycle.
    assert_eq!(fails_in_a_cycle(|_| Err(failure("no"))), "no");
    assert_eq!(
        fails_in_a_cycle(|_| Ok(None)),
        "Encountered null constituent"
    );
    let other_columns = fails_in_a_cycle(|table| {
        Ok(Some(
            drop_columns(table, &["V"]).map_err(|error| failure(error.to_string()))?,
        ))
    });
    assert_eq!(
        other_columns,
        "Table definition incompatibilities: \n\texpected column 'V' is missing in constituent"
    );
}

#[test]
fn the_function_is_called_after_its_dependencies_have_finished() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    // A table that finishes late in the cycle: it is several operations from its source.
    let other = ScriptedTable::new(
        graph.clone(),
        definition(&[("X", ColumnType::Int)]),
        RowSet::flat(1),
        vec![ints([1])],
    );
    let other_table: Arc<dyn Table> = other.table();
    let late = (0..6).fold(other_table, |table, _| reverse(&table).unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let function: Transformer = {
        let (late, seen) = (late.clone(), seen.clone());
        Arc::new(move |tables: &[Arc<dyn Table>]| {
            seen.lock().unwrap().push(late.row_set().size());
            last_row(tables)
        })
    };
    let transformed = partitioned
        .transform(function, std::slice::from_ref(&late))
        .unwrap();
    seen.lock().unwrap().clear();
    other.push(TableChange::new().add([1, 2], vec![ints([2, 3])]));
    source.push(TableChange::new().add([3], probe_columns(&[3], &[13], &[4.0])));
    graph.run_cycle();
    assert_eq!(*seen.lock().unwrap(), [3]);
    assert_eq!(keys(&transformed), ["1", "2", "3"]);
}

/// The other side of a pair: `K`, `W`, with the keys 2 and 3.
fn other_side(graph: &Arc<SteppedUpdateGraph>) -> (Arc<ScriptedTable>, PartitionedTable) {
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&[
            ("K", ColumnType::Int),
            ("N", ColumnType::Int),
            ("V", ColumnType::Double),
        ]),
        RowSet::flat(2),
        probe_columns(&[2, 3], &[21, 31], &[20.0, 30.0]),
    );
    let table: Arc<dyn Table> = source.table();
    let partitioned = partition_by(&table, &["K"], false).unwrap();
    (source, partitioned)
}

#[test]
fn a_pair_has_the_rows_of_the_first_whose_key_the_second_has() {
    let Probe {
        graph,
        source,
        partitioned,
    } = probe(false);
    let (other_source, other) = other_side(&graph);
    let calls = Arc::new(AtomicUsize::new(0));
    let pairs = partitioned
        .partitioned_transform(&other, counted(&calls, both), &[])
        .unwrap();
    // the one pair, and the two tables without rows
    assert_eq!(taken(&calls), 2);
    assert!(pairs.table().is_refreshing());
    assert!(pairs.constituent_changes_permitted());
    assert_eq!(names(pairs.table().definition()), ["K", CONSTITUENT]);
    assert_eq!(
        pairs.table().definition().columns()[1].column_type,
        ColumnType::Table(TableClass::Table)
    );
    let merged = pairs.merge().unwrap();
    let checks = [
        ConsistencyChecker::attach(pairs.table().clone()),
        ConsistencyChecker::attach(merged.clone()),
    ];
    let verify = || checks.iter().for_each(|check| check.assert_consistent());
    assert_eq!(keys(&pairs), ["2"]);
    assert_eq!(rows(&*merged), ["2|11|2.0", "2|21|20.0"]);

    // The second gets the key 1: a pair appears, before the pair of the key 2.
    other_source.push(TableChange::new().add([2], probe_columns(&[1], &[41], &[10.0])));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 1);
    assert_eq!(keys(&pairs), ["1", "2"]);
    assert_eq!(
        rows(&*merged),
        ["1|10|1.0", "1|12|3.0", "1|41|10.0", "2|11|2.0", "2|21|20.0"]
    );

    // The first gets the key 3, which the second has.
    source.push(TableChange::new().add([3], probe_columns(&[3], &[13], &[4.0])));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 1);
    assert_eq!(keys(&pairs), ["1", "2", "3"]);

    // The second loses the key 2: the pair goes. A row more for the key 3 is no call.
    other_source.push(
        TableChange::new()
            .remove([0])
            .add([5], probe_columns(&[3], &[32], &[31.0])),
    );
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 0);
    assert_eq!(keys(&pairs), ["1", "3"]);
    assert_eq!(
        rows(&*merged),
        ["1|10|1.0", "1|12|3.0", "1|41|10.0", "3|13|4.0", "3|31|30.0", "3|32|31.0"]
    );

    // It has the key 2 again, and the first loses the key 1.
    other_source.push(TableChange::new().add([0], probe_columns(&[2], &[22], &[21.0])));
    source.push(TableChange::new().remove([0, 2]));
    graph.run_cycle();
    verify();
    assert_eq!(taken(&calls), 1);
    assert_eq!(keys(&pairs), ["2", "3"]);
    assert_eq!(
        rows(&*merged),
        ["2|11|2.0", "2|22|21.0", "3|13|4.0", "3|31|30.0", "3|32|31.0"]
    );
}

#[test]
fn a_pair_of_static_partitions_and_the_keys_java_refuses() {
    let left = partition_by(&seven_rows(), &["K"], false).unwrap();
    let right_rows = oxhaven_testkit::ReferenceTable::new_static(
        definition(&[("J", ColumnType::Int), ("V", ColumnType::Double)]),
        RowSet::flat(3),
        vec![ints([2, 5, 0]), oxhaven_testkit::doubles([0.5, 0.25, 0.125])],
    );
    let right_rows: Arc<dyn Table> = right_rows;
    // Key columns of other names are matched by position.
    let right = partition_by(&right_rows, &["J"], true).unwrap();
    let first: Transformer = Arc::new(last_row);
    let pairs = left.partitioned_transform(&right, first.clone(), &[]).unwrap();
    assert!(!pairs.table().is_refreshing());
    assert!(!pairs.constituent_changes_permitted());
    assert_eq!(pairs.key_columns(), ["K"]);
    assert_eq!(keys(&pairs), ["0", "2"]);
    assert_eq!(rows(&*pairs.merge().unwrap()), ["0|9.0", "2|7.5"]);
    // Observed: the underlying table is flat when all its rows are pairs, and not when
    // a row of the first found no constituent.
    assert!(!pairs.table().is_flat());
    let with_itself = left.partitioned_transform(&left, first.clone(), &[]).unwrap();
    assert!(with_itself.table().is_flat());

    // Observed: "Incompatible partitioned table input for partitioned transform; key
    // column sets don't contain the same names or the same number of columns: first has
    // [K], second has [K, V]"
    let two_keys = partition_by(&seven_rows(), &["K", "V"], false).unwrap();
    let refused = left.partitioned_transform(&two_keys, first.clone(), &[]);
    assert_eq!(
        refused.err().map(|error| error.to_string()),
        Some(
            "Incompatible partitioned table input for partitioned transform; key column sets \
             don't contain the same names or the same number of columns: first has [K], \
             second has [K, V]"
                .to_string()
        )
    );
    // Observed: "...; key column definitions don't match: [K, int] doesn't match [K, long]"
    let by_value = partition_by(&seven_rows(), &["V"], false).unwrap();
    let refused = left.partitioned_transform(&by_value, first, &[]);
    assert_eq!(
        refused.err().map(|error| error.to_string()),
        Some(
            "Incompatible partitioned table input for partitioned transform; key column \
             definitions don't match: [K, int] doesn't match [V, double]"
                .to_string()
        )
    );
}

/// A random source with the columns of [`random_transforms`].
fn random_source(
    graph: &Arc<SteppedUpdateGraph>,
    random: &mut RandomUpdates,
) -> Arc<ScriptedTable> {
    let columns = [
        ("K", ColumnType::String),
        ("G", ColumnType::Int),
        ("X", ColumnType::Double),
    ];
    ScriptedTable::new(
        graph.clone(),
        definition(&columns),
        RowSet::flat(20),
        columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, 20))
            .collect(),
    )
}

/// Random changes of two tables partitioned by `K`. After every cycle the transform of
/// the first holds each constituent reversed, and the pairs hold, for each key that both
/// have, the rows of the first and then those of the second; the merges are those one
/// after the other; and every update of the underlying tables and of the merges is a
/// valid change of what they held.
fn random_transforms(seed: u64, kinds: [bool; 4]) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    [random.adds, random.removes, random.modifies, random.shifts] = kinds;
    let (left_source, right_source) = (
        random_source(&graph, &mut random),
        random_source(&graph, &mut random),
    );
    let left: Arc<dyn Table> = left_source.table();
    let right: Arc<dyn Table> = right_source.table();
    let left_parts = partition_by(&left, &["K"], false).unwrap();
    let right_parts = partition_by(&right, &["K"], false).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let reversing = left_parts
        .transform(counted(&calls, reversed), &[])
        .unwrap();
    let pairs = left_parts
        .partitioned_transform(&right_parts, counted(&calls, both), &[])
        .unwrap();
    let (reversed_rows, paired_rows) = (reversing.merge().unwrap(), pairs.merge().unwrap());
    let checks = [
        ConsistencyChecker::attach(reversing.table().clone()),
        ConsistencyChecker::attach(pairs.table().clone()),
        ConsistencyChecker::attach(reversed_rows.clone()),
        ConsistencyChecker::attach(paired_rows.clone()),
    ];
    for _ in 0..30 {
        left_source.push(random.next_change(&*left, 6));
        right_source.push(random.next_change(&*right, 6));
        graph.run_cycle();
        for check in &checks {
            check.assert_consistent();
        }
        let (in_left, in_right) = (groups(&*left, &[0]), groups(&*right, &[0]));
        let left_keys = keys(&left_parts);
        assert_eq!(keys(&reversing), left_keys, "seed {seed}");
        let mut all = Vec::new();
        for (key, constituent) in left_keys.iter().zip(reversing.constituents().unwrap()) {
            let mut expected = in_left[key].clone();
            expected.reverse();
            assert_eq!(rows(&*constituent), expected, "seed {seed}, key {key}");
            all.extend(expected);
        }
        assert_eq!(rows(&*reversed_rows), all, "seed {seed}");

        let both_have: Vec<String> = left_keys
            .into_iter()
            .filter(|key| in_right.contains_key(key))
            .collect();
        assert_eq!(keys(&pairs), both_have, "seed {seed}");
        let mut all = Vec::new();
        for (key, constituent) in both_have.iter().zip(pairs.constituents().unwrap()) {
            let mut expected = in_left[key].clone();
            expected.extend(in_right[key].iter().cloned());
            assert_eq!(rows(&*constituent), expected, "seed {seed}, key {key}");
            all.extend(expected);
        }
        assert_eq!(rows(&*paired_rows), all, "seed {seed}");
    }
    let updates: u64 = checks.iter().map(|check| check.updates_seen()).sum();
    updates + calls.load(Ordering::SeqCst) as u64
}

#[test]
fn random_changes_keep_transforms_and_pairs_right() {
    let mut updates = 0;
    for seed in test_seeds(0..40) {
        updates += random_transforms(seed, [true; 4]);
        updates += random_transforms(seed, [true, true, false, true]);
        updates += random_transforms(seed, [true, false, true, false]);
    }
    assert!(
        updates >= at_least_for(4000, 0..40),
        "only {updates} updates and calls"
    );
}
