//! Tests of the ring table. What Java does was observed on Java 42.9
//! (`tasks/4.19-table-sources.md`).

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{append_only_input_table, empty_table, in_memory_table};
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_testkit::{
    at_least_for, definition, longs, test_seeds, ConsistencyChecker, RandomUpdates, Recorder,
    ScriptedTable, TableChange,
};
use std::collections::VecDeque;

fn graph() -> (Arc<SteppedUpdateGraph>, Arc<dyn UpdateGraph>) {
    let graph = SteppedUpdateGraph::new();
    (graph.clone(), graph)
}

fn qty() -> TableDefinition {
    definition(&[("Qty", ColumnType::Long)])
}

fn batch(values: impl IntoIterator<Item = i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("Qty", DataType::Int64, true)]));
    RecordBatch::try_new(schema, vec![longs(values)]).unwrap()
}

/// A static table of `Qty`.
fn fixed(values: impl IntoIterator<Item = i64>) -> Arc<dyn Table> {
    in_memory_table(qty(), &[batch(values)]).unwrap()
}

fn quantities(table: &dyn Table) -> Vec<i64> {
    let cells = table.column(0).chunk(&table.row_set()).unwrap();
    let cells = cells.as_any().downcast_ref::<Int64Array>().unwrap();
    cells.iter().map(|cell| cell.unwrap()).collect()
}

fn keys(table: &dyn Table) -> Vec<RowKey> {
    table.row_set().iter().collect()
}

fn message(result: Result<Arc<QueryTable>, TableFailure>) -> String {
    result.err().unwrap().message
}

#[test]
fn a_ring_of_a_static_table_is_static_and_holds_its_last_rows() {
    let ring = ring_table(&fixed(0..5), 2, true).unwrap();
    assert!(!ring.is_refreshing());
    assert_eq!(quantities(&*ring), [3, 4]);
    assert_eq!(keys(&*ring), [3, 4]);
    assert!(!ring.is_flat());

    // a ring larger than the table
    let ring = ring_table(&fixed(0..3), 5, true).unwrap();
    assert_eq!(quantities(&*ring), [0, 1, 2]);
    assert_eq!(keys(&*ring), [0, 1, 2]);

    let empty = ring_table(&fixed([]), 2, true).unwrap();
    assert_eq!(empty.row_set().size(), 0);
}

#[test]
fn a_ring_refuses_what_java_refuses() {
    let (_stepper, graph) = graph();
    let ticking: Arc<dyn Table> = append_only_input_table(graph.clone(), qty(), None).unwrap();
    for parent in [fixed(0..3), ticking.clone()] {
        assert_eq!(
            message(ring_table(&parent, 0, true)),
            "java.lang.IllegalArgumentException: Capacity must be positive"
        );
        assert_eq!(
            message(ring_table(&parent, -1, true)),
            "java.lang.NegativeArraySizeException: -1"
        );
        assert_eq!(
            message(ring_table(&parent, i32::MAX - 7, true)),
            "io.deephaven.base.verify.RequirementFailure: Requirement failed: required \
             capacity <= 2147483639, instead capacity == 2147483640."
        );
    }
    // the largest capacity is accepted, and nothing is set aside for it
    assert!(ring_table(&ticking, i32::MAX - 8, true).is_ok());

    let no_data = message(ring_table(&fixed(0..3), 2, false));
    assert!(no_data.starts_with("java.lang.IllegalArgumentException: Trying to initialize"));
    assert!(no_data.ends_with("but init=NONE; no data will be filled in this case."));
    // the check of the static parent comes before the check of the capacity
    assert_eq!(message(ring_table(&fixed(0..3), 0, false)), no_data);
}

/// Java makes one ring per column and asks the first for the rows.
#[test]
fn a_ring_of_a_table_without_columns_is_what_java_makes_of_it() {
    let (_stepper, graph) = graph();
    let none = TableDefinition::new(Vec::new()).unwrap();
    let rows: Arc<dyn Table> = empty_table(3);
    assert_eq!(
        message(ring_table(&rows, 2, true)),
        "java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0"
    );
    let no_rows: Arc<dyn Table> = empty_table(0);
    // the capacity is not looked at
    for capacity in [2, 0, -1] {
        let ring = ring_table(&no_rows, capacity, true).unwrap();
        assert_eq!(ring.row_set().size(), 0);
    }
    let ticking: Arc<dyn Table> = append_only_input_table(graph, none, None).unwrap();
    assert_eq!(
        message(ring_table(&ticking, 2, true)),
        "java.lang.IllegalArgumentException"
    );
}

#[test]
fn a_ring_of_an_append_only_table_is_the_ring_of_its_rows_so_far() {
    let (_stepper, graph) = graph();
    let source = append_only_input_table(graph, qty(), Some(&batch([1, 2, 3]))).unwrap();
    let parent: Arc<dyn Table> = source.clone();
    let started = ring_table(&parent, 2, true).unwrap();
    let empty = ring_table(&parent, 2, false).unwrap();
    let wide = ring_table(&parent, 5, true).unwrap();
    assert!(started.is_refreshing());
    assert_eq!(quantities(&*started), [2, 3]);
    assert_eq!(quantities(&*empty), [0i64; 0]);
    assert_eq!(quantities(&*wide), [1, 2, 3]);

    // The ring of a table that only grows is the ring of a static copy of it.
    let from_scratch = |capacity: i32| {
        let parent = parent.clone();
        move || -> Arc<dyn Table> {
            let rows = fixed(quantities(&*parent));
            ring_table(&rows, capacity, true).unwrap()
        }
    };
    let checkers = [
        ConsistencyChecker::attach_with_recompute(started.clone(), from_scratch(2)),
        ConsistencyChecker::attach(empty.clone()),
        ConsistencyChecker::attach_with_recompute(wide.clone(), from_scratch(5)),
    ];
    let input = source.input_table().unwrap();
    let mut next = 4;
    // one row, then more rows than either ring holds, then some
    for size in [1, 7, 2, 1, 6, 3] {
        input.add(batch(next..next + size)).unwrap();
        next += size;
        for checker in &checkers {
            checker.assert_consistent();
        }
        let last = |count: i64| (next - count..next).collect::<Vec<_>>();
        assert_eq!(quantities(&*started), last(2));
        assert_eq!(quantities(&*wide), last(5.min(next - 1)));
        // a ring that started empty has the rows added since
        assert_eq!(quantities(&*empty), last(2.min(next - 4)));
        // and counts its row keys from the first of them
        let since = next - 4;
        assert_eq!(
            keys(&*empty),
            ((since - 2).max(0)..since).collect::<Vec<_>>()
        );
    }
    assert!(checkers.iter().all(|checker| checker.updates_seen() == 6));
}

/// The rows of a change's added rows, in key order.
fn added_quantities(change: &TableChange) -> Vec<i64> {
    let Some(values) = change.added_values.first() else {
        return Vec::new();
    };
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    values.iter().map(|value| value.unwrap_or(-1)).collect()
}

/// A parent that adds rows anywhere and removes any of them, as a blink table removes all:
/// the ring holds the rows added last, in the order of the cycles and, within one, of the
/// row keys, whatever was removed since.
#[test]
fn a_ring_keeps_the_rows_added_last_whatever_the_parent_removes() {
    let mut updates = 0;
    for seed in test_seeds(0..40) {
        let (stepper, graph) = graph();
        let capacity = 1 + (seed % 7) as usize;
        let source = ScriptedTable::new(
            graph.clone(),
            qty(),
            RowSet::from_keys([2, 4, 9]),
            vec![longs([20, 40, 90])],
        );
        let parent: Arc<dyn Table> = source.table();
        let ring = ring_table(&parent, capacity as i32, seed % 2 == 0).unwrap();
        let checker = ConsistencyChecker::attach(ring.clone());
        let mut model: VecDeque<i64> = VecDeque::new();
        if seed % 2 == 0 {
            model.extend([20, 40, 90]);
        }
        let mut random = RandomUpdates::new(seed);
        random.modifies = false;
        random.shifts = false;
        for step in 0..60 {
            // every few steps more rows than the ring holds
            let size = if step % 5 == 0 { 12 } else { 3 };
            let change = random.next_change(&*parent, size);
            let mut added: Vec<(RowKey, i64)> = change
                .update
                .added
                .iter()
                .collect::<Vec<_>>()
                .into_iter()
                .zip(added_quantities(&change))
                .collect();
            added.sort();
            model.extend(added.into_iter().map(|(_, quantity)| quantity));
            source.push(change);
            stepper.run_cycle();
            checker.assert_consistent();
            while model.len() > capacity {
                model.pop_front();
            }
            let cells = ring.column(0).chunk(&ring.row_set()).unwrap();
            let cells = cells.as_any().downcast_ref::<Int64Array>().unwrap();
            let held: Vec<i64> = cells.iter().map(|cell| cell.unwrap_or(-1)).collect();
            assert_eq!(held, Vec::from(model.clone()), "seed {seed} step {step}");
        }
        updates += checker.updates_seen();
    }
    assert!(updates >= at_least_for(40 * 30, 0..40), "{updates} updates");
}

#[test]
fn a_ring_reports_the_rows_that_fell_out_and_the_new_ones() {
    let (stepper, graph) = graph();
    let source = ScriptedTable::new(graph.clone(), qty(), RowSet::empty(), vec![longs([])]);
    let parent: Arc<dyn Table> = source.table();
    let ring = ring_table(&parent, 3, true).unwrap();
    let recorder = Recorder::attach(ring.clone());
    let add = |keys: std::ops::Range<i64>| {
        let values: ArrayRef = longs(keys.clone());
        source.push(TableChange::new().add(keys, vec![values]));
        stepper.run_cycle();
    };
    add(0..2);
    add(2..4);
    // a removal alone changes nothing
    source.push(TableChange::new().remove([0, 1]));
    stepper.run_cycle();
    // more rows than the ring holds: the keys jump
    add(10..17);
    let changes: Vec<(RowSet, RowSet)> = recorder
        .events()
        .into_iter()
        .map(|event| event.map(|update| (update.added, update.removed)).unwrap())
        .collect();
    let range = RowSet::from_range;
    assert_eq!(
        changes,
        [
            (range(0, 1), RowSet::empty()),
            (range(2, 3), range(0, 0)),
            (range(8, 10), range(1, 3)),
        ]
    );
    assert_eq!(quantities(&*ring), [14, 15, 16]);
}

#[test]
fn a_ring_fails_when_its_parent_modifies_or_shifts_a_row() {
    for shifts in [false, true] {
        let (stepper, graph) = graph();
        let source = ScriptedTable::new(
            graph.clone(),
            qty(),
            RowSet::from_keys([1, 2]),
            vec![longs([10, 20])],
        );
        let parent: Arc<dyn Table> = source.table();
        let ring = ring_table(&parent, 3, true).unwrap();
        let recorder = Recorder::attach(ring.clone());
        source.push(if shifts {
            TableChange::new().shift(2, 2, 5)
        } else {
            TableChange::new().modify([1], [(0, longs([11]))])
        });
        stepper.run_cycle();
        assert!(ring.is_failed());
        let failure = recorder.events().pop().unwrap().err().unwrap();
        assert_eq!(
            failure.message,
            "java.lang.IllegalStateException: Not expecting modifies or shifts"
        );
        // it keeps the rows it had
        assert_eq!(quantities(&*ring), [10, 20]);
    }
}
