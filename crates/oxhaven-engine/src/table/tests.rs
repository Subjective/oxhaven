use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{aggregate, Aggregation};
use crate::ops::filter::unstructured_filter;
use crate::ops::merge::{flatten, merge};
use crate::ops::select::{drop_columns, lazy_update, select, update, update_view, view};
use crate::ops::sort::{head, reverse, sort, tail, SortColumn};
use crate::update::ModifiedColumnTransformer;
use arrow::array::{
    Array, Float32Array, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use oxhaven_contracts::graph::{locked, ClockPhase, Dependency, UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    assert_tables_equal, definition, longs, strings, ConsistencyChecker, RandomUpdates, Recorder,
    ReferenceTable, ScriptedTable, TableChange, TableSnapshot,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Mutex, Weak};
use std::thread;
use std::time::Duration;

// ---------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------

fn graph() -> (Arc<SteppedUpdateGraph>, Arc<dyn UpdateGraph>) {
    let graph = SteppedUpdateGraph::new();
    (graph.clone(), graph)
}

fn sym_qty() -> TableDefinition {
    definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)])
}

/// A scripted source with rows 0, 1, 5 and 6.
fn scripted(graph: &Arc<dyn UpdateGraph>) -> Arc<ScriptedTable> {
    ScriptedTable::new(
        graph.clone(),
        sym_qty(),
        RowSet::from_keys([0, 1, 5, 6]),
        vec![strings(["a", "b", "c", "d"]), longs([10, 20, 30, 40])],
    )
}

fn long_values(values: &ArrayRef) -> Vec<Option<i64>> {
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    values.iter().collect()
}

fn string_values(values: &ArrayRef) -> Vec<Option<String>> {
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    values.iter().map(|v| v.map(str::to_string)).collect()
}

/// The current values of a `Long` column, in row order.
fn longs_of(table: &dyn Table, column: usize) -> Vec<Option<i64>> {
    long_values(&TableSnapshot::current(table).unwrap().columns[column])
}

fn strings_of(table: &dyn Table, column: usize) -> Vec<Option<String>> {
    string_values(&TableSnapshot::current(table).unwrap().columns[column])
}

fn keys_of(table: &dyn Table) -> Vec<RowKey> {
    table.row_set().iter().collect()
}

fn batch(columns: &[(&str, ArrayRef)]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
        .collect();
    let arrays = columns.iter().map(|(_, values)| values.clone()).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

fn sym_qty_batch<'a>(
    syms: impl IntoIterator<Item = &'a str>,
    qtys: impl IntoIterator<Item = i64>,
) -> RecordBatch {
    batch(&[("Sym", strings(syms)), ("Qty", longs(qtys))])
}

// ---------------------------------------------------------------------------------------
// Operators written the way the module documentation says
// ---------------------------------------------------------------------------------------

/// The recipe of the module documentation.
fn pass_through(parent: &Arc<dyn Table>) -> Result<Arc<QueryTable>, TableFailure> {
    QueryTable::derive(parent, || {
        let definition = parent.definition().clone();
        let columns = (0..definition.columns().len())
            .map(|index| parent.column(index))
            .collect();
        let spec = TableSpec::new(definition, (*parent.row_set()).clone(), columns)
            .with_attributes(parent.attributes());
        let operator = |_result: &QueryTable, upstream: &TableUpdate| Ok(upstream.clone());
        Ok((spec, operator))
    })
}

fn twice_sym() -> TableDefinition {
    definition(&[("Twice", ColumnType::Long), ("Sym", ColumnType::String)])
}

fn doubled_values(parent: &dyn Table, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
    let qty = long_values(&parent.column(1).chunk(rows)?);
    Ok(Arc::new(Int64Array::from_iter(
        qty.into_iter().map(|v| v.map(|v| v.wrapping_mul(2))),
    )))
}

/// `Twice = Qty * 2` in a column the operator owns, and `Sym` read from the parent, in the
/// other column order, so that modified columns have to be translated.
fn doubled(parent: &Arc<dyn Table>) -> Result<Arc<QueryTable>, TableFailure> {
    QueryTable::derive(parent, || {
        let rows = (*parent.row_set()).clone();
        let twice = match parent.update_graph() {
            Some(graph) => tracked_source(&graph, ColumnType::Long),
            None => Arc::new(WritableSource::new(ColumnType::Long)),
        };
        twice.fill(&rows, &doubled_values(&**parent, &rows)?)?;
        let spec = TableSpec::new(twice_sym(), rows, vec![twice.clone(), parent.column(0)]);

        let parent = parent.clone();
        let columns = ModifiedColumnTransformer::new([(0, [1]), (1, [0])]);
        let operator = move |result: &QueryTable, upstream: &TableUpdate| {
            twice.remove(&upstream.removed);
            let survivors = result.row_set().minus(&upstream.removed);
            twice.apply_shifts(&upstream.shifted, &survivors)?;
            let written = upstream.added.union(&upstream.modified);
            twice.fill(&written, &doubled_values(&*parent, &written)?)?;
            Ok(TableUpdate {
                modified_columns: columns.transform(&upstream.modified_columns),
                ..upstream.clone()
            })
        };
        Ok((spec, operator))
    })
}

/// What `doubled` must equal, computed from scratch.
fn doubled_from_scratch(parent: &dyn Table) -> Arc<dyn Table> {
    let now = TableSnapshot::current(parent).unwrap();
    let twice: ArrayRef = Arc::new(Int64Array::from_iter(
        long_values(&now.columns[1])
            .into_iter()
            .map(|v| v.map(|v| v.wrapping_mul(2))),
    ));
    ReferenceTable::new_static(
        twice_sym(),
        RowSet::flat(now.rows.size()),
        vec![twice, now.columns[0].clone()],
    )
}

/// Where the second parent's rows go in a stacked table.
const SECOND: RowKey = 1 << 40;

/// The rows of `first` followed by the rows of `second`, copied into columns the operator
/// owns. The second table's rows are at their own keys plus [`SECOND`].
fn stacked(
    first: &Arc<dyn Table>,
    second: &Arc<dyn Table>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let parents = [first.clone(), second.clone()];
    QueryTable::derive_merged(&parents, Vec::new(), || {
        let graph = parents.iter().find_map(|parent| parent.update_graph());
        let definition = first.definition().clone();
        let columns: Vec<Arc<WritableSource>> = definition
            .columns()
            .iter()
            .map(|column| match &graph {
                Some(graph) => tracked_source(graph, column.column_type.clone()),
                None => Arc::new(WritableSource::new(column.column_type.clone())),
            })
            .collect();
        let offsets = [0, SECOND];
        let mut rows = RowSet::empty();
        for (parent, offset) in parents.iter().zip(offsets) {
            let theirs = parent.row_set();
            let ours = theirs.shift(offset).unwrap();
            for (index, column) in columns.iter().enumerate() {
                column.fill(&ours, &parent.column(index).chunk(&theirs)?)?;
            }
            rows.insert(&ours);
        }
        let spec = TableSpec::new(
            definition,
            rows,
            columns
                .iter()
                .map(|c| c.clone() as Arc<dyn ColumnSource>)
                .collect(),
        );

        let parents = parents.clone();
        let operator = move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
            let mut down = TableUpdate::empty();
            let mut shifts = Vec::new();
            for (update, offset) in upstream.iter().zip(offsets) {
                let Some(update) = update else { continue };
                down.added.insert(&update.added.shift(offset).unwrap());
                down.removed.insert(&update.removed.shift(offset).unwrap());
                down.modified
                    .insert(&update.modified.shift(offset).unwrap());
                down.modified_columns = down.modified_columns.union(&update.modified_columns);
                shifts.extend(update.shifted.ranges().iter().map(|s| ShiftRange {
                    first: s.first + offset,
                    last: s.last + offset,
                    delta: s.delta,
                }));
            }
            down.shifted = RowSetShiftData::new(shifts).unwrap();

            let survivors = result.row_set().minus(&down.removed);
            for column in &columns {
                column.remove(&down.removed);
                column.apply_shifts(&down.shifted, &survivors)?;
            }
            for ((update, offset), parent) in upstream.iter().zip(offsets).zip(&parents) {
                let Some(update) = update else { continue };
                let theirs = update.added.union(&update.modified);
                let ours = theirs.shift(offset).unwrap();
                for (index, column) in columns.iter().enumerate() {
                    column.fill(&ours, &parent.column(index).chunk(&theirs)?)?;
                }
            }
            Ok(down)
        };
        Ok((spec, operator))
    })
}

fn stacked_from_scratch(first: &dyn Table, second: &dyn Table) -> Arc<dyn Table> {
    let (a, b) = (
        TableSnapshot::current(first).unwrap(),
        TableSnapshot::current(second).unwrap(),
    );
    let columns = a
        .columns
        .iter()
        .zip(&b.columns)
        .map(|(a, b)| arrow::compute::concat(&[a.as_ref(), b.as_ref()]).unwrap())
        .collect();
    ReferenceTable::new_static(
        first.definition().clone(),
        RowSet::flat(a.rows.size() + b.rows.size()),
        columns,
    )
}

// ---------------------------------------------------------------------------------------
// The testkit on this graph
// ---------------------------------------------------------------------------------------

#[test]
fn testkit_scripted_table_random_updates_and_checker_run_on_the_stepped_graph() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let table: Arc<dyn Table> = source.table();
    let checker = ConsistencyChecker::attach(table.clone());
    let mut random = RandomUpdates::new(7);
    for _ in 0..100 {
        source.push(random.next_change(&*table, 6));
        stepper.run_cycle();
        checker.assert_consistent();
    }
    assert!(checker.updates_seen() > 50);
}

#[test]
fn checker_catches_a_misreported_update_on_the_stepped_graph() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let checker = ConsistencyChecker::attach(source.table());
    // The row is removed, but listeners are told nothing was.
    source.push_misreported(
        TableChange::new().remove([0]),
        TableUpdate {
            added: RowSet::from_keys([9]),
            ..TableUpdate::empty()
        },
    );
    stepper.run_cycle();
    assert!(checker.verify().is_err());
}

// ---------------------------------------------------------------------------------------
// QueryTable as the result of an operation
// ---------------------------------------------------------------------------------------

#[test]
fn pass_through_of_a_static_table_is_static_and_equal() {
    let parent: Arc<dyn Table> = ReferenceTable::new_static(
        sym_qty(),
        RowSet::from_keys([2, 4]),
        vec![strings(["a", "b"]), longs([1, 2])],
    );
    let result = pass_through(&parent).unwrap();
    let checker = ConsistencyChecker::attach(result.clone());

    assert!(!result.is_refreshing() && result.update_graph().is_none());
    assert!(result.satisfied(0) && result.satisfied(99));
    assert_eq!(*result.row_set(), *result.prev_row_set());
    assert_eq!(keys_of(&*result), [2, 4]);
    assert_tables_equal(&*parent, &*result);
    checker.assert_consistent();
}

#[test]
fn pass_through_follows_a_ticking_parent() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let result = pass_through(&parent).unwrap();
    let expected = parent.clone();
    let checker =
        ConsistencyChecker::attach_with_recompute(result.clone(), move || expected.clone());
    let recorder = Recorder::attach(result.clone());
    assert!(result.is_refreshing());

    source.push(
        TableChange::new()
            .remove([0])
            .add([9], vec![strings(["e"]), longs([50])])
            .modify([1], [(1, longs([21]))]),
    );
    stepper.run_cycle();
    checker.assert_consistent();
    assert_eq!(keys_of(&*result), [1, 5, 6, 9]);
    assert_eq!(
        longs_of(&*result, 1),
        [Some(21), Some(30), Some(40), Some(50)]
    );
    let events = recorder.events();
    assert_eq!(events.len(), 1);
    let update = events[0].as_ref().unwrap();
    assert_eq!(update.removed, RowSet::from_keys([0]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([1]));

    // A cycle without a change delivers nothing.
    stepper.run_cycle();
    checker.assert_consistent();
    assert_eq!(recorder.events().len(), 1);
    assert!(result.satisfied(graph.clock().step));

    let mut random = RandomUpdates::new(11);
    for _ in 0..60 {
        source.push(random.next_change(&*parent, 5));
        stepper.run_cycle();
        checker.assert_consistent();
    }
}

#[test]
fn previous_rows_are_readable_during_the_cycle_and_equal_current_rows_when_idle() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let result = pass_through(&parent).unwrap();

    /// Reads the table from inside the cycle, after its update.
    struct Looks {
        table: Arc<QueryTable>,
        seen: Mutex<Vec<(Vec<RowKey>, Vec<RowKey>)>>,
    }
    impl Dependency for Looks {
        fn satisfied(&self, _step: u64) -> bool {
            true
        }
    }
    impl oxhaven_contracts::table::UpdateListener for Looks {
        fn on_update(&self, _update: &TableUpdate) -> Result<(), TableFailure> {
            let prev = self.table.prev_row_set().iter().collect();
            self.seen
                .lock()
                .unwrap()
                .push((prev, keys_of(&*self.table)));
            Ok(())
        }
        fn on_failure(&self, _failure: &TableFailure) {}
    }
    let looks = Arc::new(Looks {
        table: result.clone(),
        seen: Mutex::default(),
    });
    let listener: Arc<dyn oxhaven_contracts::table::UpdateListener> = looks.clone();
    result.add_listener(&listener);

    source.push(TableChange::new().remove([0, 6]));
    stepper.run_cycle();
    assert_eq!(
        *looks.seen.lock().unwrap(),
        [(vec![0, 1, 5, 6], vec![1, 5])]
    );
    assert_eq!(graph.clock().phase, ClockPhase::Idle);
    assert_eq!(*result.prev_row_set(), *result.row_set());
}

#[test]
fn an_operator_with_its_own_column_stays_equal_to_its_recompute() {
    for seed in [1, 2, 3] {
        let (stepper, graph) = graph();
        let source = scripted(&graph);
        let parent: Arc<dyn Table> = source.table();
        let result = doubled(&parent).unwrap();
        assert_eq!(
            longs_of(&*result, 0),
            [Some(20), Some(40), Some(60), Some(80)]
        );
        let expected = parent.clone();
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            doubled_from_scratch(&*expected)
        });
        let mut random = RandomUpdates::new(seed);
        for _ in 0..80 {
            source.push(random.next_change(&*parent, 6));
            stepper.run_cycle();
            checker.assert_consistent();
        }
        assert!(checker.updates_seen() > 40);
    }
}

#[test]
fn an_operator_on_a_static_parent_builds_its_columns_once() {
    let parent: Arc<dyn Table> = ReferenceTable::new_static(
        sym_qty(),
        RowSet::from_keys([3, 8]),
        vec![strings(["a", "b"]), longs([4, 5])],
    );
    let result = doubled(&parent).unwrap();
    assert!(!result.is_refreshing());
    assert_tables_equal(&*doubled_from_scratch(&*parent), &*result);
    ConsistencyChecker::attach(result).assert_consistent();
}

#[test]
fn operators_stack_and_stay_consistent_at_every_level() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let middle: Arc<dyn Table> = pass_through(&parent).unwrap();
    let result = doubled(&middle).unwrap();
    let expected = parent.clone();
    let checkers = [
        ConsistencyChecker::attach(middle.clone()),
        ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            doubled_from_scratch(&*expected)
        }),
    ];
    let mut random = RandomUpdates::new(5);
    for _ in 0..60 {
        source.push(random.next_change(&*parent, 5));
        stepper.run_cycle();
        checkers.iter().for_each(|c| c.assert_consistent());
    }
}

#[test]
fn a_merged_result_runs_once_per_step_whichever_parents_tick() {
    let (stepper, graph) = graph();
    let (left, right) = (scripted(&graph), scripted(&graph));
    let (first, second): (Arc<dyn Table>, Arc<dyn Table>) = (left.table(), right.table());
    let result = stacked(&first, &second).unwrap();
    let (a, b) = (first.clone(), second.clone());
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
        stacked_from_scratch(&*a, &*b)
    });
    let recorder = Recorder::attach(result.clone());
    checker.assert_consistent();

    let mut random = RandomUpdates::new(3);
    let mut expected_updates = 0;
    for cycle in 0..90 {
        // Left only, right only, both, neither.
        if cycle % 4 == 0 || cycle % 4 == 2 {
            left.push(random.next_change(&*first, 4));
        }
        if cycle % 4 == 1 || cycle % 4 == 2 {
            right.push(random.next_change(&*second, 4));
        }
        stepper.run_cycle();
        checker.assert_consistent();
        if cycle % 4 != 3 {
            expected_updates += 1;
        }
        assert!(recorder.events().len() <= expected_updates);
        assert!(result.satisfied(graph.clock().step));
    }
    assert!(recorder.events().len() > 40);
}

#[test]
fn a_diamond_of_query_tables_delivers_one_consistent_update() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    // One side is two operators longer than the other.
    let short: Arc<dyn Table> = pass_through(&parent).unwrap();
    let long: Arc<dyn Table> = pass_through(
        &(pass_through(&(pass_through(&parent).unwrap() as Arc<dyn Table>)).unwrap()
            as Arc<dyn Table>),
    )
    .unwrap();
    let result = stacked(&short, &long).unwrap();
    let expected = parent.clone();
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
        stacked_from_scratch(&*expected, &*expected)
    });
    let recorder = Recorder::attach(result.clone());

    let mut random = RandomUpdates::new(9);
    for cycle in 1..=60 {
        source.push(random.next_change(&*parent, 5));
        stepper.run_cycle();
        checker.assert_consistent();
        assert!(recorder.events().len() <= cycle);
    }
}

#[test]
fn a_merged_result_waits_for_a_merged_parent_that_has_not_run_yet() {
    let (stepper, graph) = graph();
    // `other` refreshes first, so the outer result hears of a change before the inner
    // result has processed its own.
    let (other, left, right) = (scripted(&graph), scripted(&graph), scripted(&graph));
    let (c, a, b): (Arc<dyn Table>, Arc<dyn Table>, Arc<dyn Table>) =
        (other.table(), left.table(), right.table());
    let inner: Arc<dyn Table> = stacked(&a, &b).unwrap();
    let outer = stacked(&c, &inner).unwrap();
    let (c2, inner2) = (c.clone(), inner.clone());
    let checker = ConsistencyChecker::attach_with_recompute(outer.clone(), move || {
        stacked_from_scratch(&*c2, &*inner2)
    });
    let recorder = Recorder::attach(outer.clone());

    let mut random = RandomUpdates::new(21);
    for cycle in 1..=40 {
        other.push(random.next_change(&*c, 3));
        left.push(random.next_change(&*a, 3));
        stepper.run_cycle();
        checker.assert_consistent();
        assert!(recorder.events().len() <= cycle);
    }
    assert!(recorder.events().len() > 20);
}

#[test]
fn a_static_parent_among_refreshing_ones_never_ticks() {
    let (stepper, graph) = graph();
    let fixed: Arc<dyn Table> = ReferenceTable::new_static(
        sym_qty(),
        RowSet::from_keys([0]),
        vec![strings(["z"]), longs([-1])],
    );
    let source = scripted(&graph);
    let ticking: Arc<dyn Table> = source.table();
    let result = stacked(&fixed, &ticking).unwrap();
    let (a, b) = (fixed.clone(), ticking.clone());
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
        stacked_from_scratch(&*a, &*b)
    });
    assert!(result.is_refreshing());

    source.push(TableChange::new().remove([1]));
    stepper.run_cycle();
    checker.assert_consistent();
    assert_eq!(
        longs_of(&*result, 1),
        [Some(-1), Some(10), Some(30), Some(40)]
    );

    // All parents static: the result is static.
    let both_static = stacked(&fixed, &fixed).unwrap();
    assert!(!both_static.is_refreshing());
    assert_eq!(longs_of(&*both_static, 1), [Some(-1), Some(-1)]);
}

#[test]
fn an_operator_error_fails_the_result_and_what_is_built_on_it() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let broken = QueryTable::derive(&parent, || {
        let spec = TableSpec::new(sym_qty(), (*parent.row_set()).clone(), {
            vec![parent.column(0), parent.column(1)]
        });
        let operator = |_: &QueryTable, upstream: &TableUpdate| {
            if upstream.removed.is_empty() {
                Ok(upstream.clone())
            } else {
                Err(failure("cannot remove"))
            }
        };
        Ok((spec, operator))
    })
    .unwrap();
    let broken_dyn: Arc<dyn Table> = broken.clone();
    let below = pass_through(&broken_dyn).unwrap();
    let recorder = Recorder::attach(below.clone());

    source.push(TableChange::new().modify([1], [(1, longs([21]))]));
    stepper.run_cycle();
    assert!(!broken.is_failed() && !below.is_failed());

    source.push(TableChange::new().remove([0]));
    stepper.run_cycle();
    assert!(broken.is_failed() && below.is_failed());
    let events = recorder.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1], Err(failure("cannot remove")));
    // The failed tables keep the rows they had.
    assert_eq!(keys_of(&*below), [0, 1, 5, 6]);

    // Nothing more is delivered, and the tables stay satisfied.
    source.push(TableChange::new().remove([1]));
    stepper.run_cycle();
    assert_eq!(recorder.events().len(), 2);
    assert!(below.satisfied(graph.clock().step));

    // An operation on a failed table is an error.
    let error = pass_through(&broken_dyn).err().unwrap();
    assert_eq!(error, failure("the parent table has failed"));
    let error = stacked(&parent, &broken_dyn).err().unwrap();
    assert_eq!(error, failure("the parent table has failed"));
}

#[test]
fn an_operator_that_returns_an_impossible_update_fails_the_result() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let wrong = QueryTable::derive(&parent, || {
        let spec = TableSpec::new(sym_qty(), (*parent.row_set()).clone(), {
            vec![parent.column(0), parent.column(1)]
        });
        // Claims to remove a row the result never had.
        let operator = |_: &QueryTable, _: &TableUpdate| {
            Ok(TableUpdate {
                removed: RowSet::from_keys([77]),
                ..TableUpdate::empty()
            })
        };
        Ok((spec, operator))
    })
    .unwrap();
    let recorder = Recorder::attach(wrong.clone());

    source.push(TableChange::new().remove([0]));
    stepper.run_cycle();
    assert!(wrong.is_failed());
    let events = recorder.events();
    assert_eq!(events.len(), 1);
    let message = &events[0].as_ref().unwrap_err().message;
    assert!(
        message.starts_with("invalid update: removed rows"),
        "{message}"
    );
    assert_eq!(keys_of(&*wrong), [0, 1, 5, 6]);
}

#[test]
fn an_error_while_building_the_result_is_the_operations_error() {
    let (_stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let outcome = QueryTable::derive(&parent, || {
        Err::<(TableSpec, fn(&QueryTable, &TableUpdate) -> _), _>(failure("no such column"))
    });
    assert_eq!(outcome.err().unwrap(), failure("no such column"));
}

#[test]
fn a_result_keeps_its_parent_alive_and_dropping_it_unsubscribes() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let middle = pass_through(&parent).unwrap();
    let weak_middle: Weak<QueryTable> = Arc::downgrade(&middle);
    let middle_dyn: Arc<dyn Table> = middle;
    let result = doubled(&middle_dyn).unwrap();
    drop(middle_dyn);
    assert!(weak_middle.upgrade().is_some());

    source.push(TableChange::new().remove([0]));
    stepper.run_cycle();
    assert_eq!(keys_of(&*result), [1, 5, 6]);

    drop(result);
    assert!(weak_middle.upgrade().is_none());
    source.push(TableChange::new().remove([1]));
    stepper.run_cycle();
}

#[test]
#[should_panic(expected = "column Qty has a source of another type")]
fn a_spec_whose_columns_do_not_match_its_definition_panics() {
    let strings: Arc<dyn ColumnSource> = Arc::new(WritableSource::new(ColumnType::String));
    QueryTable::new_static(TableSpec::new(
        sym_qty(),
        RowSet::empty(),
        vec![strings.clone(), strings],
    ));
}

#[test]
#[should_panic(expected = "a flat table must start with a flat row set")]
fn a_flat_spec_whose_rows_are_not_flat_panics() {
    QueryTable::new_static(
        TableSpec::new(definition(&[]), RowSet::from_keys([1]), Vec::new()).flat(),
    );
}

// ---------------------------------------------------------------------------------------
// Root tables and attributes
// ---------------------------------------------------------------------------------------

/// A root table whose source delivers whatever update the test queued, without writing
/// any column.
struct Pushed {
    table: Weak<QueryTable>,
    next: Mutex<Option<TableUpdate>>,
}

impl UpdateSource for Pushed {
    fn refresh(&self) {
        if let (Some(table), Some(update)) =
            (self.table.upgrade(), self.next.lock().unwrap().take())
        {
            table.notify(update);
        }
    }
}

fn pushed(
    graph: &Arc<dyn UpdateGraph>,
    rows: RowSet,
    attributes: TableAttributes,
    flat: bool,
) -> (Arc<QueryTable>, Arc<Pushed>) {
    let mut spec = TableSpec::new(definition(&[]), rows, Vec::new()).with_attributes(attributes);
    spec.flat = flat;
    let mut handle = None;
    let table = QueryTable::root(graph.clone(), spec, |table| {
        let source = Arc::new(Pushed {
            table,
            next: Mutex::new(None),
        });
        handle = Some(source.clone());
        RootSource {
            source,
            input: None,
        }
    });
    (table, handle.unwrap())
}

fn adds(keys: impl IntoIterator<Item = RowKey>) -> TableUpdate {
    TableUpdate {
        added: RowSet::from_keys(keys),
        ..TableUpdate::empty()
    }
}

fn removes(keys: impl IntoIterator<Item = RowKey>) -> TableUpdate {
    TableUpdate {
        removed: RowSet::from_keys(keys),
        ..TableUpdate::empty()
    }
}

#[test]
fn a_root_table_is_satisfied_once_sources_are_done_or_its_update_is_delivered() {
    let (stepper, graph) = graph();
    let (table, source) = pushed(&graph, RowSet::flat(2), TableAttributes::default(), false);
    let checker = ConsistencyChecker::attach(table.clone());
    assert!(table.is_refreshing() && table.input_table().is_none());

    *source.next.lock().unwrap() = Some(adds([5]));
    stepper.run_cycle();
    checker.assert_consistent();
    assert_eq!(keys_of(&*table), [0, 1, 5]);
    let step = graph.clock().step;
    assert!(table.satisfied(step) && !table.satisfied(step + 1));

    stepper.run_cycle();
    assert!(table.satisfied(step + 1));
    checker.assert_consistent();
}

#[test]
fn a_root_table_lives_as_long_as_the_table_and_no_longer() {
    let (stepper, graph) = graph();
    let (table, source) = pushed(&graph, RowSet::empty(), TableAttributes::default(), false);
    let weak_source = Arc::downgrade(&source);
    drop(source);
    // The table owns its source; the graph does not.
    assert!(weak_source.upgrade().is_some());
    drop(table);
    assert!(weak_source.upgrade().is_none());
    stepper.run_cycle();
}

/// The failure message, if any, after the table is given `update` in one cycle.
fn outcome_of(
    rows: RowSet,
    attributes: TableAttributes,
    flat: bool,
    update: TableUpdate,
) -> Option<String> {
    let (stepper, graph) = graph();
    let (table, source) = pushed(&graph, rows, attributes, flat);
    let recorder = Recorder::attach(table.clone());
    *source.next.lock().unwrap() = Some(update);
    stepper.run_cycle();
    let events = recorder.events();
    assert_eq!(events.len(), 1);
    assert_eq!(table.is_failed(), events[0].is_err());
    events[0].as_ref().err().map(|e| e.message.clone())
}

#[test]
fn updates_that_break_an_attribute_fail_the_table() {
    let mut append_only = TableAttributes::default();
    append_only.add_only = true;
    append_only.append_only = true;
    let mut add_only = TableAttributes::default();
    add_only.add_only = true;
    let mut blink = TableAttributes::default();
    blink.blink = true;
    let plain = TableAttributes::default();
    let two = || RowSet::from_keys([3, 4]);

    assert_eq!(outcome_of(two(), plain, false, removes([3])), None);

    assert_eq!(outcome_of(two(), add_only, false, adds([0])), None);
    assert_eq!(
        outcome_of(two(), add_only, false, removes([3])).unwrap(),
        "an add-only table was given removes, modifies or shifts"
    );

    assert_eq!(outcome_of(two(), append_only, false, adds([5, 9])), None);
    assert_eq!(
        outcome_of(RowSet::empty(), append_only, false, adds([0])),
        None
    );
    assert_eq!(
        outcome_of(two(), append_only, false, adds([0])).unwrap(),
        "an append-only table was given rows before its last row"
    );

    assert_eq!(outcome_of(RowSet::flat(2), plain, true, adds([2])), None);
    assert_eq!(
        outcome_of(RowSet::flat(2), plain, true, adds([3])).unwrap(),
        "a flat table was given a row set that is not flat"
    );

    let replace = TableUpdate {
        added: RowSet::from_keys([7]),
        removed: two(),
        ..TableUpdate::empty()
    };
    assert_eq!(outcome_of(two(), blink, false, replace), None);
    assert_eq!(
        outcome_of(two(), blink, false, adds([7])).unwrap(),
        "a blink table must replace all of its rows on every update"
    );
    let keeps_one = TableUpdate {
        added: RowSet::from_keys([7]),
        removed: RowSet::from_keys([3]),
        ..TableUpdate::empty()
    };
    assert_eq!(
        outcome_of(two(), blink, false, keeps_one).unwrap(),
        "a blink table must replace all of its rows on every update"
    );

    assert!(outcome_of(two(), plain, false, adds([3]))
        .unwrap()
        .starts_with("invalid update: added rows already present"));
}

// ---------------------------------------------------------------------------------------
// Static tables
// ---------------------------------------------------------------------------------------

#[test]
fn in_memory_table_holds_its_batches_in_order_at_flat_keys() {
    let table = in_memory_table(
        sym_qty(),
        &[
            sym_qty_batch(["a", "b"], [1, 2]),
            // Columns in another order.
            batch(&[("Qty", longs([3])), ("Sym", strings(["c"]))]),
        ],
    )
    .unwrap();
    let checker = ConsistencyChecker::attach(table.clone());

    assert!(!table.is_refreshing() && table.is_flat() && !table.is_failed());
    assert!(table.update_graph().is_none() && table.input_table().is_none());
    assert_eq!(*table.row_set(), RowSet::flat(3));
    assert_eq!(*table.prev_row_set(), RowSet::flat(3));
    assert_eq!(
        strings_of(&*table, 0),
        ["a", "b", "c"].map(|s| Some(s.to_string()))
    );
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(2), Some(3)]);
    assert_eq!(
        long_values(&table.column(1).prev_chunk(&RowSet::from_keys([2])).unwrap()),
        [Some(3)]
    );
    checker.assert_consistent();
}

#[test]
fn in_memory_table_keeps_nulls_and_accepts_no_batches() {
    let with_null = in_memory_table(
        sym_qty(),
        &[batch(&[
            (
                "Sym",
                Arc::new(StringArray::from(vec![None, Some("x")])) as ArrayRef,
            ),
            (
                "Qty",
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            ),
        ])],
    )
    .unwrap();
    assert_eq!(strings_of(&*with_null, 0), [None, Some("x".to_string())]);
    assert_eq!(longs_of(&*with_null, 1), [Some(1), None]);

    let empty = in_memory_table(sym_qty(), &[]).unwrap();
    assert!(empty.row_set().is_empty() && empty.is_flat());
    assert!(longs_of(&*empty, 1).is_empty());
}

#[test]
fn in_memory_table_rejects_batches_that_do_not_match_the_definition() {
    let message = |batch: RecordBatch| in_memory_table(sym_qty(), &[batch]).err().unwrap().message;
    assert_eq!(
        message(batch(&[("Sym", strings(["a"]))])),
        "Table definition incompatibilities: column Qty is missing"
    );
    assert_eq!(
        message(batch(&[("Sym", strings(["a"])), ("Qty", strings(["1"]))])),
        "Table definition incompatibilities: column Qty is Utf8, expected Int64"
    );
    assert_eq!(
        message(batch(&[
            ("Sym", strings(["a"])),
            ("Qty", longs([1])),
            ("Px", longs([1]))
        ])),
        "Table definition incompatibilities: column Px is not expected"
    );
}

#[test]
fn empty_table_has_rows_and_no_columns() {
    let table = empty_table(5);
    assert_eq!(*table.row_set(), RowSet::flat(5));
    assert!(table.definition().columns().is_empty());
    assert!(table.is_flat() && !table.is_refreshing());
    ConsistencyChecker::attach(table).assert_consistent();
    assert!(empty_table(0).row_set().is_empty());
}

// ---------------------------------------------------------------------------------------
// Input tables
// ---------------------------------------------------------------------------------------

#[test]
fn append_only_input_table_appends_and_the_change_is_visible_when_add_returns() {
    let (_stepper, graph) = graph();
    let table = append_only_input_table(graph.clone(), sym_qty(), None).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    let input = table.input_table().unwrap();
    let attributes = table.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
    assert!(table.is_flat() && table.is_refreshing() && table.row_set().is_empty());
    assert!(input.key_columns().is_empty());
    assert_eq!(input.value_columns(), ["Sym", "Qty"]);

    let start = graph.clock().step;
    input.add(sym_qty_batch(["a", "b"], [1, 2])).unwrap();
    assert_eq!(graph.clock().step, start + 1);
    assert_eq!(graph.clock().phase, ClockPhase::Idle);
    assert_eq!(keys_of(&*table), [0, 1]);
    checker.assert_consistent();

    // The same key again is another row, and columns may come in any order.
    input
        .add(batch(&[("Qty", longs([3])), ("Sym", strings(["a"]))]))
        .unwrap();
    assert_eq!(keys_of(&*table), [0, 1, 2]);
    assert_eq!(
        strings_of(&*table, 0),
        ["a", "b", "a"].map(|s| Some(s.to_string()))
    );
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(2), Some(3)]);
    checker.assert_consistent();

    let events = recorder.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1], Ok(adds([2])));
}

#[test]
fn append_only_input_table_refuses_deletes_bad_batches_and_edits_from_a_cycle() {
    let (stepper, graph) = graph();
    let table = append_only_input_table(graph.clone(), sym_qty(), None).unwrap();
    let input = table.input_table().unwrap();
    let start = graph.clock().step;

    assert_eq!(
        input
            .delete(batch(&[("Sym", strings(["a"]))]))
            .err()
            .unwrap(),
        failure("Table doesn't support delete operation")
    );
    assert_eq!(
        input.add(batch(&[("Sym", strings(["a"]))])).err().unwrap(),
        failure("Table definition incompatibilities: column Qty is missing")
    );
    // An empty batch changes nothing and needs no cycle.
    input
        .add(sym_qty_batch(Vec::<&str>::new(), Vec::new()))
        .unwrap();
    assert_eq!(graph.clock().step, start);

    // A blocking edit from inside a cycle could never finish.
    let (input2, seen) = (input.clone(), Arc::new(Mutex::new(None)));
    let seen2 = seen.clone();
    struct Edits<F>(F);
    impl<F: Fn() + Send + Sync> UpdateSource for Edits<F> {
        fn refresh(&self) {
            (self.0)()
        }
    }
    let edits: Arc<dyn UpdateSource> = Arc::new(Edits(move || {
        *seen2.lock().unwrap() = Some(input2.add(sym_qty_batch(["a"], [1])));
    }));
    graph.add_source(&edits);
    stepper.run_cycle();
    let refused = seen.lock().unwrap().take().unwrap().err().unwrap();
    assert!(refused
        .message
        .starts_with("Attempted to make a blocking input table edit"));
    assert!(table.row_set().is_empty());
}

#[test]
fn append_only_input_table_starts_with_its_initial_rows() {
    let (_stepper, graph) = graph();
    let initial = sym_qty_batch(["a", "b"], [1, 2]);
    let table = append_only_input_table(graph.clone(), sym_qty(), Some(&initial)).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    assert_eq!(keys_of(&*table), [0, 1]);
    assert_eq!(*table.prev_row_set(), RowSet::flat(2));
    assert_eq!(
        long_values(&table.column(1).prev_chunk(&RowSet::flat(2)).unwrap()),
        [Some(1), Some(2)]
    );

    table
        .input_table()
        .unwrap()
        .add(sym_qty_batch(["c"], [3]))
        .unwrap();
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(2), Some(3)]);
    checker.assert_consistent();
}

#[test]
fn a_yielding_holder_of_the_shared_lock_writes_and_sees_its_rows() {
    let (_stepper, graph) = graph();
    let table = append_only_input_table(graph.clone(), sym_qty(), None).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    locked(&*graph, || {
        crate::graph::yielding_to_cycles(|| {
            let input = table.input_table().unwrap();
            input.add(sym_qty_batch(["a", "b"], [1, 2])).unwrap();
            // The change is in the table inside the same hold, as in a Java command.
            assert_eq!(longs_of(&*table, 1), [Some(1), Some(2)]);
            input.add(sym_qty_batch(["c"], [3])).unwrap();
            assert_eq!(longs_of(&*table, 1), [Some(1), Some(2), Some(3)]);
        });
    });
    checker.assert_consistent();
}

#[test]
#[should_panic(expected = "run_cycle called while holding the update graph's shared lock")]
fn a_holder_of_the_shared_lock_that_does_not_yield_cannot_write() {
    let (_stepper, graph) = graph();
    let table = append_only_input_table(graph.clone(), sym_qty(), None).unwrap();
    locked(&*graph, || {
        let _ = table.input_table().unwrap().add(sym_qty_batch(["a"], [1]));
    });
}

/// Long enough for a thread that is not blocked to make progress.
const SETTLE: Duration = Duration::from_millis(150);

#[test]
fn changes_queued_before_a_cycle_are_applied_together() {
    let (_stepper, graph) = graph();
    let table = append_only_input_table(graph.clone(), sym_qty(), None).unwrap();
    let recorder = Recorder::attach(table.clone());
    let checker = ConsistencyChecker::attach(table.clone());

    // While this thread holds the shared lock no cycle can run, so both adds queue up.
    let (done_tx, done_rx) = channel();
    let writers: Vec<_> = locked(&*graph, || {
        let writers = [("a", 1), ("b", 2)]
            .map(|(sym, qty)| {
                let (input, done) = (table.input_table().unwrap(), done_tx.clone());
                thread::spawn(move || {
                    input.add(sym_qty_batch([sym], [qty])).unwrap();
                    done.send(()).unwrap();
                })
            })
            .into_iter()
            .collect();
        thread::sleep(SETTLE);
        assert!(
            done_rx.try_recv().is_err(),
            "an add returned before any cycle"
        );
        assert!(table.row_set().is_empty());
        writers
    });
    for writer in writers {
        writer.join().unwrap();
    }

    assert_eq!(recorder.events(), [Ok(adds([0, 1]))]);
    let mut qtys = longs_of(&*table, 1);
    qtys.sort();
    assert_eq!(qtys, [Some(1), Some(2)]);
    checker.assert_consistent();
}

/// Queues `changes` (`true` for a delete) in order while no cycle can run, then lets one
/// cycle apply them all.
fn apply_in_one_cycle(
    graph: &Arc<dyn UpdateGraph>,
    table: &Arc<QueryTable>,
    changes: Vec<(bool, RecordBatch)>,
) {
    let writers: Vec<_> = locked(&**graph, || {
        changes
            .into_iter()
            .map(|(delete, rows)| {
                let input = table.input_table().unwrap();
                let writer = thread::spawn(move || {
                    if delete {
                        input.delete(rows).unwrap();
                    } else {
                        input.add(rows).unwrap();
                    }
                });
                // Let this change queue before the next one starts.
                thread::sleep(SETTLE);
                writer
            })
            .collect()
    });
    for writer in writers {
        writer.join().unwrap();
    }
}

#[test]
fn keyed_input_table_nets_out_the_changes_of_one_cycle() {
    let (_stepper, graph) = graph();
    let initial = sym_qty_batch(["a", "b"], [1, 2]);
    let table = keyed_input_table(graph.clone(), sym_qty(), &["Sym"], Some(&initial)).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    let sym = |name: &'static str| batch(&[("Sym", strings([name]))]);

    // Deleted and added back: the row is modified.
    let changes = vec![(true, sym("a")), (false, sym_qty_batch(["a"], [10]))];
    apply_in_one_cycle(&graph, &table, changes);
    assert_eq!(
        recorder.events(),
        [Ok(TableUpdate {
            modified: RowSet::from_keys([0]),
            modified_columns: ModifiedColumnSet::All,
            ..TableUpdate::empty()
        })]
    );
    assert_eq!(longs_of(&*table, 1), [Some(10), Some(2)]);
    checker.assert_consistent();

    // Added and deleted: nothing happened.
    let changes = vec![(false, sym_qty_batch(["c"], [3])), (true, sym("c"))];
    apply_in_one_cycle(&graph, &table, changes);
    assert_eq!(recorder.events().len(), 1);
    assert_eq!(keys_of(&*table), [0, 1]);
    assert!(!table.is_failed());
    checker.assert_consistent();

    // Modified and deleted: the row is removed.
    let changes = vec![(false, sym_qty_batch(["b"], [20])), (true, sym("b"))];
    apply_in_one_cycle(&graph, &table, changes);
    assert_eq!(recorder.events().len(), 2);
    assert_eq!(recorder.events()[1], Ok(removes([1])));
    checker.assert_consistent();

    // Added and modified: the row is added, with the last value.
    let changes = vec![
        (false, sym_qty_batch(["d"], [4])),
        (false, sym_qty_batch(["d"], [5])),
    ];
    apply_in_one_cycle(&graph, &table, changes);
    assert_eq!(recorder.events()[2], Ok(adds([3])));
    assert_eq!(longs_of(&*table, 1), [Some(10), Some(5)]);
    checker.assert_consistent();
}

/// Fails a table on the next cycle.
struct FailOnce(Mutex<Option<Arc<QueryTable>>>);

impl UpdateSource for FailOnce {
    fn refresh(&self) {
        if let Some(table) = self.0.lock().unwrap().take() {
            table.fail(failure("broken from outside"));
        }
    }
}

#[test]
fn an_input_table_that_has_failed_refuses_changes() {
    let (stepper, graph) = graph();
    let table = keyed(&graph);
    let input = table.input_table().unwrap();
    input.add(sym_qty_batch(["a"], [1])).unwrap();

    let breaker: Arc<dyn UpdateSource> = Arc::new(FailOnce(Mutex::new(Some(table.clone()))));
    graph.add_source(&breaker);
    stepper.run_cycle();
    assert!(table.is_failed());

    let error = input.add(sym_qty_batch(["b"], [2])).err().unwrap();
    assert_eq!(error, failure("the input table has failed"));
    // And keeps refusing, without running a cycle.
    let step = graph.clock().step;
    let error = input
        .delete(batch(&[("Sym", strings(["a"]))]))
        .err()
        .unwrap();
    assert_eq!(error, failure("the input table has failed"));
    assert_eq!(graph.clock().step, step);
    assert_eq!(keys_of(&*table), [0]);
}

fn keyed(graph: &Arc<dyn UpdateGraph>) -> Arc<QueryTable> {
    keyed_input_table(graph.clone(), sym_qty(), &["Sym"], None).unwrap()
}

#[test]
fn keyed_input_table_adds_replaces_deletes_and_reuses_row_keys() {
    let (_stepper, graph) = graph();
    let table = keyed(&graph);
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    let input = table.input_table().unwrap();
    assert_eq!(input.key_columns(), ["Sym"]);
    assert!(!table.is_flat() && table.attributes() == TableAttributes::default());
    let last = || recorder.events().last().cloned().unwrap().unwrap();

    input
        .add(sym_qty_batch(["a", "b", "c"], [1, 2, 3]))
        .unwrap();
    assert_eq!(last(), adds([0, 1, 2]));
    checker.assert_consistent();

    // An existing key is modified in place, a new one is added.
    input.add(sym_qty_batch(["b", "d"], [20, 4])).unwrap();
    assert_eq!(
        last(),
        TableUpdate {
            added: RowSet::from_keys([3]),
            modified: RowSet::from_keys([1]),
            modified_columns: ModifiedColumnSet::All,
            ..TableUpdate::empty()
        }
    );
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(20), Some(3), Some(4)]);
    checker.assert_consistent();

    // Unknown keys are ignored.
    input
        .delete(batch(&[("Sym", strings(["a", "zzz", "c"]))]))
        .unwrap();
    assert_eq!(last(), removes([0, 2]));
    assert_eq!(keys_of(&*table), [1, 3]);
    checker.assert_consistent();

    // A deleted key comes back at its old row key; a new key goes after all rows ever used.
    input.add(sym_qty_batch(["e", "a"], [5, 100])).unwrap();
    assert_eq!(last(), adds([0, 4]));
    assert_eq!(
        strings_of(&*table, 0),
        ["a", "b", "d", "e"].map(|s| Some(s.to_string()))
    );
    assert_eq!(
        longs_of(&*table, 1),
        [Some(100), Some(20), Some(4), Some(5)]
    );
    checker.assert_consistent();

    // Deleting only unknown keys runs a cycle and delivers nothing.
    let events = recorder.events().len();
    input.delete(batch(&[("Sym", strings(["nope"]))])).unwrap();
    assert_eq!(recorder.events().len(), events);
    checker.assert_consistent();
}

#[test]
fn keyed_input_table_keeps_the_last_row_of_a_repeated_key() {
    let (_stepper, graph) = graph();
    let table = keyed(&graph);
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    let input = table.input_table().unwrap();

    // A new key twice in one batch is one added row.
    input
        .add(sym_qty_batch(["a", "b", "a"], [1, 2, 3]))
        .unwrap();
    assert_eq!(recorder.events(), [Ok(adds([0, 1]))]);
    assert_eq!(longs_of(&*table, 1), [Some(3), Some(2)]);
    checker.assert_consistent();

    // An existing key twice is one modified row.
    input.add(sym_qty_batch(["b", "b"], [8, 9])).unwrap();
    assert_eq!(
        recorder.events()[1].as_ref().unwrap().modified,
        RowSet::from_keys([1])
    );
    assert_eq!(longs_of(&*table, 1), [Some(3), Some(9)]);
    checker.assert_consistent();
}

#[test]
fn keyed_input_table_treats_null_as_a_key_and_supports_several_key_columns() {
    let (_stepper, graph) = graph();
    let definition = definition(&[
        ("Sym", ColumnType::String),
        ("Venue", ColumnType::Long),
        ("Qty", ColumnType::Long),
    ]);
    let table = keyed_input_table(graph.clone(), definition, &["Venue", "Sym"], None).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    let input = table.input_table().unwrap();
    assert_eq!(input.key_columns(), ["Venue", "Sym"]);
    assert_eq!(input.value_columns(), ["Qty"]);
    let rows = |syms: Vec<Option<&str>>, venues: Vec<i64>, qtys: Vec<i64>| {
        batch(&[
            ("Sym", Arc::new(StringArray::from(syms)) as ArrayRef),
            ("Venue", longs(venues)),
            ("Qty", longs(qtys)),
        ])
    };

    input
        .add(rows(
            vec![Some("a"), None, Some("a")],
            vec![1, 1, 2],
            vec![10, 20, 30],
        ))
        .unwrap();
    assert_eq!(keys_of(&*table), [0, 1, 2]);
    input.add(rows(vec![None], vec![1], vec![21])).unwrap();
    assert_eq!(longs_of(&*table, 2), [Some(10), Some(21), Some(30)]);

    // A delete names the key columns only, in any order.
    input
        .delete(batch(&[
            (
                "Sym",
                Arc::new(StringArray::from(vec![None, Some("a")])) as ArrayRef,
            ),
            ("Venue", longs([1, 2])),
        ]))
        .unwrap();
    assert_eq!(keys_of(&*table), [0]);
    checker.assert_consistent();

    let extra = input.delete(rows(vec![Some("a")], vec![1], vec![0]));
    assert_eq!(
        extra.err().unwrap().message,
        "Invalid Key Table Definition: Table definition incompatibilities: column Qty is not expected"
    );
}

#[test]
fn keyed_input_table_starts_with_its_initial_rows_and_checks_its_key_columns() {
    let (_stepper, graph) = graph();
    let initial = sym_qty_batch(["a", "b", "a"], [1, 2, 3]);
    let table = keyed_input_table(graph.clone(), sym_qty(), &["Sym"], Some(&initial)).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    assert_eq!(keys_of(&*table), [0, 1]);
    assert_eq!(longs_of(&*table, 1), [Some(3), Some(2)]);
    assert_eq!(*table.prev_row_set(), *table.row_set());

    table
        .input_table()
        .unwrap()
        .add(sym_qty_batch(["a"], [4]))
        .unwrap();
    assert_eq!(longs_of(&*table, 1), [Some(4), Some(2)]);
    checker.assert_consistent();

    let missing = keyed_input_table(graph.clone(), sym_qty(), &["Sym", "Nope"], None);
    assert_eq!(
        missing.err().unwrap().message,
        "Missing key columns in definition: [\"Nope\"]"
    );
}

/// The floating point values every case of key equality needs: both zeros, two NaNs of
/// different bits and a null, and then a zero, a NaN and a null again.
fn float_keys() -> Vec<Option<f64>> {
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
    let first = [Some(0.0), Some(-0.0), Some(f64::NAN), Some(other_nan), None];
    let again = [Some(-0.0), Some(f64::NAN), None];
    first.into_iter().chain(again).collect()
}

/// A keyed input table with the key columns `keys`, the first of them of doubles or of
/// floats and the others of longs, and what it holds once a row for each of
/// [`float_keys`] was added, numbered from 1 in `Qty`: the first key column as bits and
/// `Qty`.
fn float_keyed(keys: &[&str], floats: bool) -> Vec<(Option<u64>, Option<i64>)> {
    let (_stepper, graph) = graph();
    let key_type = if floats {
        ColumnType::Float
    } else {
        ColumnType::Double
    };
    let mut columns = vec![(keys[0], key_type)];
    columns.extend(keys[1..].iter().map(|name| (*name, ColumnType::Long)));
    columns.push(("Qty", ColumnType::Long));
    let table = keyed_input_table(graph, definition(&columns), keys, None).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());

    let values = float_keys();
    let size = values.len() as i64;
    let first: ArrayRef = if floats {
        let narrow = values.iter().map(|value| value.map(|value| value as f32));
        Arc::new(Float32Array::from_iter(narrow))
    } else {
        Arc::new(Float64Array::from(values))
    };
    let mut batch_columns = vec![(keys[0], first)];
    batch_columns.extend(keys[1..].iter().map(|name| (*name, longs(vec![7; 8]))));
    batch_columns.push(("Qty", longs(1..=size)));
    let input = table.input_table().unwrap();
    input.add(batch(&batch_columns)).unwrap();
    checker.assert_consistent();

    let cells = table.column(0).chunk(&table.row_set()).unwrap();
    let bits: Vec<Option<u64>> = if floats {
        let cells = cells.as_any().downcast_ref::<Float32Array>().unwrap();
        let wide = cells.iter().map(|cell| cell.map(|cell| cell as f64));
        wide.map(|cell| cell.map(f64::to_bits)).collect()
    } else {
        let cells = cells.as_any().downcast_ref::<Float64Array>().unwrap();
        cells.iter().map(|cell| cell.map(f64::to_bits)).collect()
    };
    let quantities = longs_of(&*table, keys.len());
    bits.into_iter().zip(quantities).collect()
}

/// Observed on Java 42.9 with these values (`tasks/4.19-table-sources.md`).
#[test]
fn keyed_input_table_takes_floating_point_keys_as_java_compares_them() {
    let (zero, minus_zero) = (Some(0.0f64.to_bits()), Some((-0.0f64).to_bits()));
    let nan = Some(f64::NAN.to_bits());
    // One key column, or four: `Double#equals`. The zeros are two keys, every NaN is one,
    // and a row holds what the last add of its key wrote.
    let boxed = [
        (zero, Some(1)),
        (minus_zero, Some(6)),
        (nan, Some(7)),
        (None, Some(8)),
    ];
    assert_eq!(float_keyed(&["Px"], false), boxed);
    assert_eq!(float_keyed(&["Px"], true), boxed);
    assert_eq!(float_keyed(&["Px", "A", "B", "C"], false), boxed);
    // Two key columns, or three: a typed tuple. The zeros are one key.
    let tuple = [(minus_zero, Some(6)), (nan, Some(7)), (None, Some(8))];
    assert_eq!(float_keyed(&["Px", "A"], false), tuple);
    assert_eq!(float_keyed(&["Px", "A"], true), tuple);
    assert_eq!(float_keyed(&["Px", "A", "B"], false), tuple);
}

#[test]
fn keyed_input_table_deletes_floating_point_keys_as_java_compares_them() {
    let other_nan = f64::from_bits(0xfff8_0000_0000_0002);
    for (keys, left) in [(&["Px"][..], 2), (&["Px", "A"][..], 1)] {
        let (_stepper, graph) = graph();
        let mut columns = vec![("Px", ColumnType::Double)];
        columns.extend(keys[1..].iter().map(|name| (*name, ColumnType::Long)));
        columns.push(("Qty", ColumnType::Long));
        let table = keyed_input_table(graph, definition(&columns), keys, None).unwrap();
        let checker = ConsistencyChecker::attach(table.clone());
        let input = table.input_table().unwrap();
        let rows = |values: Vec<f64>, with_values: bool| {
            let size = values.len();
            let mut columns = vec![("Px", Arc::new(Float64Array::from(values)) as ArrayRef)];
            columns.extend(keys[1..].iter().map(|name| (*name, longs(vec![7; size]))));
            if with_values {
                columns.push(("Qty", longs(vec![1; size])));
            }
            batch(&columns)
        };
        input
            .add(rows(vec![0.0, -0.0, f64::NAN, 1.5], true))
            .unwrap();
        // The other zero, which is another key only for one key column, and another NaN.
        input.delete(rows(vec![0.0, other_nan], false)).unwrap();
        checker.assert_consistent();
        assert_eq!(table.row_set().size(), left, "key columns {keys:?}");
    }
}

#[test]
fn keyed_input_table_matches_a_model_under_random_adds_and_deletes() {
    let (_stepper, graph) = graph();
    let table = keyed(&graph);
    let checker = ConsistencyChecker::attach(table.clone());
    let input = table.input_table().unwrap();
    let mut model: BTreeMap<String, i64> = BTreeMap::new();
    let mut state = 99u64;
    let mut below = move |bound: u64| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) % bound
    };
    let names = ["a", "b", "c", "d", "e", "f", "g", "h"];

    for step in 0..200 {
        let syms: Vec<&str> = (0..below(4) + 1)
            .map(|_| names[below(8) as usize])
            .collect();
        if below(3) == 0 {
            input
                .delete(batch(&[("Sym", strings(syms.iter().copied()))]))
                .unwrap();
            for sym in &syms {
                model.remove(*sym);
            }
        } else {
            let qtys: Vec<i64> = syms.iter().map(|_| below(1000) as i64).collect();
            input
                .add(sym_qty_batch(syms.iter().copied(), qtys.iter().copied()))
                .unwrap();
            for (sym, qty) in syms.iter().zip(qtys) {
                model.insert(sym.to_string(), qty);
            }
        }
        checker.assert_consistent();
        let found: BTreeMap<String, i64> = strings_of(&*table, 0)
            .into_iter()
            .zip(longs_of(&*table, 1))
            .map(|(sym, qty)| (sym.unwrap(), qty.unwrap()))
            .collect();
        assert_eq!(found, model, "step {step}");
        assert_eq!(table.row_set().size() as usize, model.len(), "step {step}");
    }
}

#[test]
fn an_operator_follows_an_input_table() {
    let (_stepper, graph) = graph();
    let table = keyed(&graph);
    let parent: Arc<dyn Table> = table.clone();
    let result = doubled(&parent).unwrap();
    let expected = parent.clone();
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
        doubled_from_scratch(&*expected)
    });
    let input = table.input_table().unwrap();

    input.add(sym_qty_batch(["a", "b"], [1, 2])).unwrap();
    checker.assert_consistent();
    input.add(sym_qty_batch(["a"], [5])).unwrap();
    checker.assert_consistent();
    input.delete(batch(&[("Sym", strings(["b"]))])).unwrap();
    checker.assert_consistent();
    assert_eq!(longs_of(&*result, 0), [Some(10)]);
}

// ---------------------------------------------------------------------------------------
// Time tables
// ---------------------------------------------------------------------------------------

struct TestClock(AtomicI64);

impl TimeSource for TestClock {
    fn now_nanos(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn clock_at(now: i64) -> Arc<TestClock> {
    Arc::new(TestClock(AtomicI64::new(now)))
}

fn timestamps(table: &dyn Table) -> Vec<i64> {
    let column = TableSnapshot::current(table).unwrap().columns[0].clone();
    assert_eq!(
        *column.data_type(),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into()))
    );
    let column = column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert_eq!(column.null_count(), 0);
    column.values().to_vec()
}

fn options(start_nanos: Option<i64>, period_nanos: i64, blink: bool) -> TimeTableOptions {
    TimeTableOptions {
        start_nanos,
        period_nanos,
        blink,
    }
}

#[test]
fn time_table_with_a_start_has_the_rows_due_and_gains_one_per_period() {
    let (stepper, graph) = graph();
    let clock = clock_at(1_025);
    let table = time_table(
        graph.clone(),
        clock.clone(),
        options(Some(1_000), 10, false),
    )
    .unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());

    let attributes = table.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
    assert!(table.is_flat() && table.is_refreshing());
    assert_eq!(
        table.definition(),
        &definition(&[("Timestamp", ColumnType::Instant)])
    );
    assert_eq!(timestamps(&*table), [1_000, 1_010, 1_020]);

    // No period has passed: no update.
    clock.0.store(1_029, Ordering::SeqCst);
    stepper.run_cycle();
    assert!(recorder.events().is_empty());

    clock.0.store(1_050, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(recorder.events(), [Ok(adds([3, 4, 5]))]);
    assert_eq!(
        timestamps(&*table),
        [1_000, 1_010, 1_020, 1_030, 1_040, 1_050]
    );
    checker.assert_consistent();

    // A clock that goes back takes no rows away.
    clock.0.store(1_000, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(recorder.events().len(), 1);
    checker.assert_consistent();
    clock.0.store(1_060, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(recorder.events()[1..], [Ok(adds([6]))]);
    checker.assert_consistent();
}

#[test]
fn time_table_with_a_start_in_the_future_waits_for_it() {
    let (stepper, graph) = graph();
    // Less than one period before the start.
    let clock = clock_at(95);
    let table = time_table(graph.clone(), clock.clone(), options(Some(100), 10, false)).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    assert!(table.row_set().is_empty());

    clock.0.store(96, Ordering::SeqCst);
    stepper.run_cycle();
    assert!(table.row_set().is_empty());
    clock.0.store(100, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(timestamps(&*table), [100]);
    checker.assert_consistent();
}

#[test]
fn time_table_without_a_start_begins_at_the_first_refresh_rounded_down() {
    let (stepper, graph) = graph();
    let clock = clock_at(1_234);
    let table = time_table(graph.clone(), clock.clone(), options(None, 100, false)).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    assert!(table.row_set().is_empty());
    checker.assert_consistent();

    stepper.run_cycle();
    assert_eq!(timestamps(&*table), [1_200]);
    clock.0.store(1_450, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(timestamps(&*table), [1_200, 1_300, 1_400]);
    checker.assert_consistent();

    // Before the epoch, rounding is still downwards.
    let early = time_table(graph.clone(), clock_at(-250), options(None, 100, false)).unwrap();
    stepper.run_cycle();
    assert_eq!(timestamps(&*early), [-300]);
}

#[test]
fn blink_time_table_replaces_its_rows_every_cycle() {
    let (stepper, graph) = graph();
    let clock = clock_at(1_015);
    let table = time_table(graph.clone(), clock.clone(), options(Some(1_000), 10, true)).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    assert!(table.attributes().blink && !table.attributes().append_only && !table.is_flat());
    assert_eq!(keys_of(&*table), [0, 1]);

    clock.0.store(1_040, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(keys_of(&*table), [2, 3, 4]);
    assert_eq!(timestamps(&*table), [1_020, 1_030, 1_040]);
    checker.assert_consistent();

    // Nothing new: the old rows still go.
    stepper.run_cycle();
    assert!(table.row_set().is_empty());
    checker.assert_consistent();

    // Nothing new and nothing to remove: no update.
    stepper.run_cycle();
    assert_eq!(
        recorder.events(),
        [
            Ok(TableUpdate {
                added: RowSet::from_keys([2, 3, 4]),
                removed: RowSet::from_keys([0, 1]),
                ..TableUpdate::empty()
            }),
            Ok(removes([2, 3, 4]))
        ]
    );
    assert!(!table.is_failed());
}

/// Deephaven 42.9 answers a start of `-9223372036854775807` (nanoseconds, written as text)
/// with an error at creation: `DateTimeUtils#minus` of the clock and the start overflows.
#[test]
fn time_table_fails_where_the_time_since_its_start_overflows() {
    let (stepper, graph) = graph();
    let outcome = time_table(
        graph.clone(),
        clock_at(1_000),
        options(Some(-i64::MAX), 10, false),
    );
    assert_eq!(
        outcome.err().unwrap().message,
        format!("Subtracting {} nanos from 1000 would overflow", -i64::MAX)
    );

    // A start the clock passes later fails the table at that refresh.
    let start = -i64::MAX + 10;
    let clock = clock_at(start - 1);
    let table = time_table(
        graph.clone(),
        clock.clone(),
        options(Some(start), 10, false),
    )
    .unwrap();
    assert!(table.row_set().is_empty());
    clock.0.store(9, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!(table.row_set().size(), (9 - start) as u64 / 10 + 1);
    assert!(!table.is_failed());
    clock.0.store(11, Ordering::SeqCst);
    stepper.run_cycle();
    assert!(table.is_failed());
}

#[test]
fn time_table_rejects_a_period_that_is_not_positive() {
    let (_stepper, graph) = graph();
    for period in [0, -5] {
        let outcome = time_table(graph.clone(), clock_at(0), options(None, period, false));
        assert_eq!(
            outcome.err().unwrap().message,
            format!("Invalid time period: {period} nanoseconds")
        );
    }
}

#[test]
fn an_operator_follows_a_time_table() {
    let (stepper, graph) = graph();
    let clock = clock_at(5);
    let table = time_table(graph.clone(), clock.clone(), options(Some(0), 1, false)).unwrap();
    let parent: Arc<dyn Table> = table.clone();
    let result = pass_through(&parent).unwrap();
    let expected = parent.clone();
    let checker =
        ConsistencyChecker::attach_with_recompute(result.clone(), move || expected.clone());
    assert!(result.attributes().append_only);
    for now in [6, 6, 9, 20] {
        clock.0.store(now, Ordering::SeqCst);
        stepper.run_cycle();
        checker.assert_consistent();
        assert_eq!(result.row_set().size(), now as u64 + 1);
    }
}

// ---------------------------------------------------------------------------------------
// The input table attribute
// ---------------------------------------------------------------------------------------

#[test]
fn the_input_table_attribute_passes_through_the_operations_java_copies_it_for() {
    use oxhaven_expr::Compiler;
    let (_stepper, graph) = graph();
    let table: Arc<dyn Table> =
        keyed_input_table(graph.clone(), sym_qty(), &["Sym"], None).unwrap();
    let other: Arc<dyn Table> = keyed_input_table(graph, sym_qty(), &["Sym"], None).unwrap();
    let input = table.input_table().unwrap();
    let writes_to_table = |derived: &Arc<dyn Table>| {
        derived.input_table().is_some_and(|found| {
            std::ptr::eq(
                Arc::as_ptr(&found) as *const (),
                Arc::as_ptr(&input) as *const (),
            )
        })
    };
    assert!(!table.is_flat());

    let filtered = unstructured_filter(&table, &["Qty > 0"], &Compiler).unwrap();
    let sorted = sort(&table, &[SortColumn::descending("Qty")]).unwrap();
    let kept = [
        ("where", filtered.clone()),
        ("sort", sorted.clone()),
        ("flatten", flatten(&table).unwrap()),
        (
            "update_view",
            update_view(&table, &["X = Qty + 1"], &Compiler).unwrap(),
        ),
        (
            "lazy_update",
            lazy_update(&table, &["X = Qty + 1"], &Compiler).unwrap(),
        ),
        ("where of sort", {
            unstructured_filter(&sorted, &["Qty > 0"], &Compiler).unwrap()
        }),
        ("merge of one", merge(std::slice::from_ref(&table)).unwrap()),
        (
            "merge of its own results",
            merge(&[filtered.clone(), sorted.clone()]).unwrap(),
        ),
    ];
    for (operation, derived) in &kept {
        assert!(writes_to_table(derived), "{operation}");
        assert_eq!(derived.input_table().unwrap().key_columns(), ["Sym"]);
        // The input table's own columns, whatever columns the derived table has.
        assert_eq!(
            derived.input_table().unwrap().value_columns(),
            ["Qty"],
            "{operation}"
        );
    }

    let dropped = [
        ("view", view(&table, &["Sym", "Qty"], &Compiler).unwrap()),
        (
            "select",
            select(&table, &["Sym", "Qty"], &Compiler).unwrap(),
        ),
        (
            "update",
            update(&table, &["X = Qty + 1"], &Compiler).unwrap(),
        ),
        ("drop_columns", drop_columns(&table, &["Qty"]).unwrap()),
        ("head", head(&table, 2).unwrap()),
        ("tail", tail(&table, 2).unwrap()),
        ("reverse", reverse(&table).unwrap()),
        (
            "aggregate",
            aggregate(&table, &[Aggregation::count("N")], false, &["Sym"]).unwrap(),
        ),
        ("head of where", head(&filtered, 2).unwrap()),
        // Java sorts by a column it adds and then drops.
        (
            "sort by an absolute value",
            sort(&table, &[SortColumn::ascending("Qty").absolute()]).unwrap(),
        ),
        (
            "sort by a column and an absolute value",
            sort(
                &table,
                &[
                    SortColumn::ascending("Sym"),
                    SortColumn::descending("Qty").absolute(),
                ],
            )
            .unwrap(),
        ),
        // Two input tables have two updaters.
        (
            "merge of two input tables",
            merge(&[table.clone(), other.clone()]).unwrap(),
        ),
        (
            "merge with a result that lost it",
            merge(&[filtered.clone(), head(&table, 2).unwrap()]).unwrap(),
        ),
    ];
    for (operation, derived) in &dropped {
        assert!(derived.input_table().is_none(), "{operation}");
    }

    // A write through a derived table is a write to the input table.
    let rows = sym_qty_batch(["a", "b"], [1, -2]);
    filtered.input_table().unwrap().add(rows).unwrap();
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(-2)]);
    assert_eq!(longs_of(&*filtered, 1), [Some(1)]);
    assert!(other.row_set().is_empty());
}

// ---------------------------------------------------------------------------------------
// Blink input tables
// ---------------------------------------------------------------------------------------

fn replaces(added: u64, removed: u64) -> TableUpdate {
    TableUpdate {
        added: RowSet::flat(added),
        removed: RowSet::flat(removed),
        ..TableUpdate::empty()
    }
}

#[test]
fn blink_input_table_shows_the_rows_added_since_the_last_cycle_for_one_cycle() {
    let (stepper, graph) = graph();
    let table = blink_input_table(graph.clone(), sym_qty());
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    let input = table.input_table().unwrap();
    let attributes = table.attributes();
    assert!(attributes.blink && !attributes.add_only && !attributes.append_only);
    assert!(table.is_flat() && table.is_refreshing() && table.row_set().is_empty());
    assert!(input.key_columns().is_empty());
    assert_eq!(input.value_columns(), ["Sym", "Qty"]);

    // An add hands the rows over and returns: no cycle runs, and nothing shows yet.
    let start = graph.clock().step;
    input.add(sym_qty_batch(["a", "b"], [1, 2])).unwrap();
    input
        .add(batch(&[("Qty", longs([3])), ("Sym", strings(["c"]))]))
        .unwrap();
    assert_eq!(graph.clock().step, start);
    assert!(table.row_set().is_empty());

    // The next cycle shows the rows of both adds, in the order they were added.
    stepper.run_cycle();
    assert_eq!(keys_of(&*table), [0, 1, 2]);
    assert_eq!(
        strings_of(&*table, 0),
        ["a", "b", "c"].map(|s| Some(s.to_string()))
    );
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(2), Some(3)]);
    checker.assert_consistent();

    // The cycle after it removes them, and shows what was added in between.
    input.add(sym_qty_batch(["d"], [4])).unwrap();
    stepper.run_cycle();
    assert_eq!(keys_of(&*table), [0]);
    assert_eq!(strings_of(&*table, 0), [Some("d".to_string())]);
    assert_eq!(longs_of(&*table, 1), [Some(4)]);
    checker.assert_consistent();

    stepper.run_cycle();
    assert!(table.row_set().is_empty());
    checker.assert_consistent();
    // A cycle with nothing to remove and nothing to show delivers no update.
    stepper.run_cycle();
    assert_eq!(
        recorder.events(),
        [Ok(replaces(3, 0)), Ok(replaces(1, 3)), Ok(replaces(0, 1))]
    );
}

#[test]
fn blink_input_table_refuses_deletes_and_bad_batches_and_accepts_adds_from_a_cycle() {
    let (stepper, graph) = graph();
    let table = blink_input_table(graph.clone(), sym_qty());
    let recorder = Recorder::attach(table.clone());
    let input = table.input_table().unwrap();

    assert_eq!(
        input
            .delete(batch(&[("Sym", strings(["a"]))]))
            .err()
            .unwrap(),
        failure("Table does not support deletes")
    );
    assert_eq!(
        input.add(batch(&[("Sym", strings(["a"]))])).err().unwrap(),
        failure("Table definition incompatibilities: column Qty is missing")
    );
    // An empty batch shows nothing.
    input
        .add(sym_qty_batch(Vec::<&str>::new(), Vec::new()))
        .unwrap();
    stepper.run_cycle();
    assert!(recorder.events().is_empty());

    // An add does not wait for a cycle, so one from inside a cycle is accepted. The rows
    // show in the cycle after it.
    let (input2, seen) = (input.clone(), Arc::new(Mutex::new(None)));
    let seen2 = seen.clone();
    struct Edits<F>(F);
    impl<F: Fn() + Send + Sync> UpdateSource for Edits<F> {
        fn refresh(&self) {
            (self.0)()
        }
    }
    let edits: Arc<dyn UpdateSource> = Arc::new(Edits(move || {
        let mut seen = seen2.lock().unwrap();
        if seen.is_none() {
            *seen = Some(input2.add(sym_qty_batch(["a"], [1])));
        }
    }));
    graph.add_source(&edits);
    stepper.run_cycle();
    assert_eq!(*seen.lock().unwrap(), Some(Ok(())));
    stepper.run_cycle();
    assert_eq!(strings_of(&*table, 0), [Some("a".to_string())]);
    stepper.run_cycle();
    assert!(table.row_set().is_empty());
}

/// A stream table whose callbacks write down what happened.
struct Watched {
    table: Arc<QueryTable>,
    publisher: StreamPublisher,
    flushes: Arc<AtomicI64>,
    shutdowns: Arc<AtomicI64>,
}

/// `flush` is called with the publisher and the number of the flush, from 1.
fn watched_stream(
    graph: &Arc<dyn UpdateGraph>,
    flush: impl Fn(&StreamPublisher, i64) -> Result<(), TableFailure> + Send + Sync + 'static,
) -> Watched {
    let flushes = Arc::new(AtomicI64::new(0));
    let shutdowns = Arc::new(AtomicI64::new(0));
    let own: Arc<Mutex<Option<StreamPublisher>>> = Arc::new(Mutex::new(None));
    let (counted, flushed_by) = (flushes.clone(), own.clone());
    let ended = shutdowns.clone();
    let callbacks = StreamCallbacks {
        on_flush: Some(Box::new(move || {
            let number = counted.fetch_add(1, Ordering::SeqCst) + 1;
            let publisher = flushed_by.lock().unwrap().clone().unwrap();
            flush(&publisher, number)
        })),
        on_shutdown: Some(Box::new(move || {
            ended.fetch_add(1, Ordering::SeqCst);
        })),
    };
    let (table, publisher) = stream_table(graph.clone(), sym_qty(), callbacks);
    // The flush callback holds the publisher, as a Python one does: a reference cycle that
    // the end of the stream breaks.
    *own.lock().unwrap() = Some(publisher.clone());
    Watched {
        table,
        publisher,
        flushes,
        shutdowns,
    }
}

#[test]
fn a_stream_table_shows_what_was_published_and_flushed_since_the_last_cycle() {
    let (stepper, graph) = graph();
    let Watched {
        table,
        publisher,
        flushes,
        shutdowns,
    } = watched_stream(&graph, |publisher, number| {
        if number == 2 {
            publisher.add(vec![strings(["flushed"]), longs([9])])?;
        }
        Ok(())
    });
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    let attributes = table.attributes();
    assert!(attributes.blink && !attributes.add_only && !attributes.append_only);
    assert!(table.is_flat() && table.is_refreshing() && publisher.is_alive());

    // A table's rows, read by name: other columns are ignored.
    let static_rows = in_memory_table(
        definition(&[
            ("Other", ColumnType::Long),
            ("Qty", ColumnType::Long),
            ("Sym", ColumnType::String),
        ]),
        &[batch(&[
            ("Other", longs([0, 0])),
            ("Qty", longs([2, 3])),
            ("Sym", strings(["b", "c"])),
        ])],
    )
    .unwrap();
    publisher.add(vec![strings(["a"]), longs([1])]).unwrap();
    let static_rows: Arc<dyn Table> = static_rows;
    publisher.add_table(&static_rows).unwrap();
    assert!(table.row_set().is_empty());
    stepper.run_cycle();
    assert_eq!(flushes.load(Ordering::SeqCst), 1);
    assert_eq!(
        strings_of(&*table, 0),
        ["a", "b", "c"].map(|s| Some(s.to_string()))
    );
    assert_eq!(longs_of(&*table, 1), [Some(1), Some(2), Some(3)]);
    checker.assert_consistent();

    // What the flush callback adds shows in the cycle that called it.
    stepper.run_cycle();
    assert_eq!(strings_of(&*table, 0), [Some("flushed".to_string())]);
    checker.assert_consistent();
    stepper.run_cycle();
    assert!(table.row_set().is_empty());
    stepper.run_cycle();
    assert_eq!(flushes.load(Ordering::SeqCst), 4);
    assert_eq!(
        recorder.events(),
        [Ok(replaces(3, 0)), Ok(replaces(1, 3)), Ok(replaces(0, 1))]
    );

    // Failures go before the rows that arrived with them, all in one, and end the stream.
    publisher.add(vec![strings(["lost"]), longs([5])]).unwrap();
    publisher.fail(failure("first"));
    publisher.fail(failure("second"));
    assert!(publisher.is_alive() && !table.is_failed());
    stepper.run_cycle();
    assert!(table.is_failed() && !publisher.is_alive());
    assert!(table.row_set().is_empty());
    assert_eq!(
        recorder.events().last().unwrap(),
        &Err(failure(
            "Multiple errors encountered while ingesting stream:\nCause 0: first\nCause 1: \
             second\n"
        ))
    );
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    assert_eq!(flushes.load(Ordering::SeqCst), 4);

    // Then nothing happens, without an error.
    publisher.add(vec![strings(["late"]), longs([6])]).unwrap();
    publisher.fail(failure("third"));
    stepper.run_cycle();
    assert_eq!(recorder.events().len(), 4);
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    drop(table);
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
}

#[test]
fn a_stream_table_fails_alone_with_its_flush_error_or_a_failure_the_flush_published() {
    let (stepper, graph) = graph();
    let raising = watched_stream(&graph, |_, number| {
        if number == 2 {
            return Err(failure("flush raised"));
        }
        Ok(())
    });
    let publishing = watched_stream(&graph, |publisher, _| {
        publisher.add(vec![strings(["x"]), longs([1])])?;
        publisher.fail(failure("published in the flush"));
        Ok(())
    });
    raising
        .publisher
        .add(vec![strings(["a"]), longs([1])])
        .unwrap();
    stepper.run_cycle();
    assert!(!raising.table.is_failed() && raising.table.row_set().size() == 1);
    assert!(publishing.table.is_failed() && publishing.table.row_set().is_empty());
    assert_eq!(publishing.shutdowns.load(Ordering::SeqCst), 1);
    stepper.run_cycle();
    assert!(raising.table.is_failed() && !raising.publisher.is_alive());
    assert_eq!(raising.shutdowns.load(Ordering::SeqCst), 1);
    // The failed table keeps the rows it had.
    assert_eq!(raising.table.row_set().size(), 1);
}

#[test]
fn a_stream_ends_when_its_table_is_dropped_and_refuses_rows_that_do_not_fit() {
    let (stepper, graph) = graph();
    let Watched {
        table,
        publisher,
        shutdowns,
        ..
    } = watched_stream(&graph, |_, _| Ok(()));

    let message = |result: Result<(), TableFailure>| result.unwrap_err().message;
    assert_eq!(
        message(publisher.add(vec![strings(["a"])])),
        "java.lang.IllegalStateException: StreamConsumer data length = 1 chunks, expected 2"
    );
    assert_eq!(
        message(publisher.add(vec![strings(["a"]), strings(["b"])])),
        "column Qty is Utf8, expected Int64"
    );
    assert!(message(publisher.add(vec![strings(["a"]), longs([1, 2])])).contains("size()"));
    let wrong: Arc<dyn Table> = in_memory_table(
        definition(&[("Sym", ColumnType::Long)]),
        &[batch(&[("Sym", longs([1]))])],
    )
    .unwrap();
    assert_eq!(
        message(publisher.add_table(&wrong)),
        "java.lang.ClassCastException: Cannot convert ColumnSource[Sym] of type Long to type \
         String"
    );
    let missing: Arc<dyn Table> = in_memory_table(
        definition(&[("Sym", ColumnType::String)]),
        &[batch(&[("Sym", strings(["a"]))])],
    )
    .unwrap();
    assert_eq!(
        message(publisher.add_table(&missing)),
        "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [Qty], \
         available column names are [Sym]"
    );
    stepper.run_cycle();
    assert!(table.row_set().is_empty());

    // A refreshing table is read as it is between cycles.
    let source = scripted(&graph);
    let ticking: Arc<dyn Table> = source.table();
    publisher.add_table(&ticking).unwrap();
    stepper.run_cycle();
    assert_eq!(
        longs_of(&*table, 1),
        [Some(10), Some(20), Some(30), Some(40)]
    );

    // The publisher, held by its own flush callback, does not keep the table alive.
    assert!(publisher.is_alive());
    let weak = Arc::downgrade(&table);
    drop(table);
    assert!(weak.upgrade().is_none());
    assert!(!publisher.is_alive());
    assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    publisher.add(vec![strings(["a"]), longs([1])]).unwrap();
    stepper.run_cycle();
}

#[test]
fn blink_to_append_only_collects_every_row_and_is_the_same_table_when_asked_again() {
    let (stepper, graph) = graph();
    let (blink, publisher) = stream_table(graph.clone(), sym_qty(), StreamCallbacks::default());
    let blink: Arc<dyn Table> = blink;
    publisher
        .add(vec![strings(["a", "b"]), longs([1, 2])])
        .unwrap();
    stepper.run_cycle();

    let all = blink_to_append_only(&blink, u64::MAX).unwrap();
    let checker = ConsistencyChecker::attach(all.clone());
    let attributes = all.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
    assert!(all.is_flat() && all.is_refreshing());
    // It starts with the rows the blink table shows now.
    assert_eq!(longs_of(&*all, 1), [Some(1), Some(2)]);
    let first_two = blink_to_append_only(&blink, 2).unwrap();
    assert!(!Arc::ptr_eq(&first_two, &all));

    publisher.add(vec![strings(["c"]), longs([3])]).unwrap();
    stepper.run_cycle();
    stepper.run_cycle();
    assert!(blink.row_set().is_empty());
    assert_eq!(longs_of(&*all, 1), [Some(1), Some(2), Some(3)]);
    assert_eq!(longs_of(&*first_two, 1), [Some(1), Some(2)]);
    checker.assert_consistent();
    // Asked again, it is the same table, with what it collected.
    let again = blink_to_append_only(&blink, u64::MAX).unwrap();
    assert!(Arc::ptr_eq(&again, &all));
    // Once the first is gone, a new one starts from now.
    drop((again, checker, all));
    let fresh = blink_to_append_only(&blink, u64::MAX).unwrap();
    assert!(fresh.row_set().is_empty());

    // A table that is not a blink table is refused.
    for not_blink in [fresh.clone(), scripted(&graph).table() as Arc<dyn Table>] {
        assert_eq!(
            blink_to_append_only(&not_blink, u64::MAX).err().unwrap(),
            failure("java.lang.IllegalArgumentException: Input is not a blink table!")
        );
    }

    // A failure of the blink table fails it.
    publisher.fail(failure("broken"));
    stepper.run_cycle();
    assert!(fresh.is_failed());
}

fn done<E>(result: Result<Arc<dyn Table>, E>) -> Arc<dyn Table> {
    match result {
        Ok(table) => table,
        Err(_) => panic!("the operation failed"),
    }
}

#[test]
fn a_blink_input_table_is_a_copy_and_so_are_the_results_java_remembers() {
    use oxhaven_expr::Compiler;
    let (_stepper, graph) = graph();
    let table: Arc<dyn Table> = blink_input_table(graph.clone(), sym_qty());
    assert!(is_copy(&*table));
    let sorted = done(sort(&table, &[SortColumn::descending("Qty")]));
    let matched = done(unstructured_filter(&table, &["Qty in 1, 2"], &Compiler));
    let copies = [
        ("sort", sorted.clone()),
        ("reverse", done(reverse(&table))),
        ("flatten of where", done(flatten(&matched))),
        ("where of a match", matched.clone()),
        ("drop_columns", done(drop_columns(&table, &["Sym"]))),
        (
            "sort of sort",
            done(sort(&sorted, &[SortColumn::ascending("Sym")])),
        ),
    ];
    for (operation, copy) in &copies {
        assert!(is_copy(&**copy) && copy.attributes().blink, "{operation}");
        // Java copies no attribute to the `head` of a copy.
        let first = done(head(copy, 2));
        assert_eq!(
            first.attributes(),
            TableAttributes::default(),
            "{operation}"
        );
        assert!(first.is_flat(), "{operation}");
    }

    let ranged = done(unstructured_filter(&table, &["Qty > 0"], &Compiler));
    let originals = [
        ("where of a range", ranged.clone()),
        (
            "sort of where of a range",
            done(sort(&ranged, &[SortColumn::ascending("Qty")])),
        ),
        (
            "sort by an absolute value",
            done(sort(&table, &[SortColumn::ascending("Qty").absolute()])),
        ),
        ("update", done(update(&table, &["X = Qty + 1"], &Compiler))),
        (
            "view of a formula",
            done(view(&table, &["X = Qty"], &Compiler)),
        ),
        (
            "update_view",
            done(update_view(&table, &["X = Qty"], &Compiler)),
        ),
    ];
    for (operation, original) in &originals {
        assert!(
            !is_copy(&**original) && original.attributes().blink,
            "{operation}"
        );
        let first = done(head(original, 2)).attributes();
        assert!(
            first.add_only && first.append_only && !first.blink,
            "{operation}"
        );
    }
}

/// Recorded from Java 42.9: the schemas of `view(K)`, `select()` and their `head` of a blink
/// input table carry `IsFlat` and no other attribute, the `head` of `select()` has no row
/// two cycles after an add, and `count_by` of `view(K)` has no row then.
#[test]
fn a_view_of_the_columns_of_a_blink_input_table_is_not_a_blink_table() {
    use oxhaven_expr::Compiler;
    let (stepper, graph) = graph();
    let blink = blink_input_table(graph.clone(), sym_qty());
    let input = blink.input_table().unwrap();
    let table: Arc<dyn Table> = blink;
    let sorted = done(sort(&table, &[SortColumn::descending("Qty")]));
    let named = [
        ("view", done(view(&table, &["Qty", "Sym"], &Compiler))),
        ("select", done(select(&table, &[] as &[&str], &Compiler))),
        ("select of sort", done(select(&sorted, &["Sym"], &Compiler))),
    ];
    let firsts: Vec<Arc<dyn Table>> = named
        .iter()
        .map(|(_, result)| done(head(result, 1)))
        .collect();
    for ((operation, result), first) in named.iter().zip(&firsts) {
        for table in [result, first] {
            assert_eq!(
                table.attributes(),
                TableAttributes::default(),
                "{operation}"
            );
            assert!(
                table.is_flat() && table.input_table().is_none(),
                "{operation}"
            );
        }
    }

    // The rows still last one cycle, and a `head` that is not of a blink table does not
    // keep them.
    input.add(sym_qty_batch(["a", "b"], [1, 2])).unwrap();
    stepper.run_cycle();
    for ((operation, result), first) in named.iter().zip(&firsts) {
        assert_eq!(result.row_set().size(), 2, "{operation}");
        assert_eq!(first.row_set().size(), 1, "{operation}");
    }
    stepper.run_cycle();
    for ((operation, result), first) in named.iter().zip(&firsts) {
        assert!(result.row_set().is_empty(), "{operation}");
        assert!(first.row_set().is_empty(), "{operation}");
    }
}
