//! Tests of the function generated table. What Java does was observed on Java 42.9
//! (`tasks/4.19-table-sources.md`).

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{in_memory_table, TimeSource};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::Dependency;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_testkit::{
    assert_tables_equal, at_least_for, definition, longs, strings, test_seeds, ConsistencyChecker,
    RandomUpdates, Recorder, ScriptedTable, TableChange, TableSnapshot,
};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

fn graph() -> (Arc<SteppedUpdateGraph>, Arc<dyn UpdateGraph>) {
    let graph = SteppedUpdateGraph::new();
    (graph.clone(), graph)
}

/// A static table of the columns `columns`.
fn fixed(columns: &[(&str, ColumnType, ArrayRef)]) -> Arc<dyn Table> {
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, _, values)| Field::new(*name, values.data_type().clone(), true))
        .collect();
    let values = columns.iter().map(|(_, _, values)| values.clone());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), values.collect()).unwrap();
    in_memory_table(definition(&names), &[batch]).unwrap()
}

/// A static table of `Qty`.
fn quantities(values: impl IntoIterator<Item = i64>) -> Arc<dyn Table> {
    fixed(&[("Qty", ColumnType::Long, longs(values))])
}

/// A static copy of the rows of `table` now, in the order of its rows.
fn copy_of(table: &dyn Table) -> Arc<dyn Table> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let columns = table.definition().columns().iter().zip(snapshot.columns);
    let columns: Vec<(&str, ColumnType, ArrayRef)> = columns
        .map(|(column, values)| (&*column.name, column.column_type.clone(), values))
        .collect();
    fixed(&columns)
}

fn held(table: &dyn Table) -> Vec<i64> {
    let cells = table.column(0).chunk(&table.row_set()).unwrap();
    let cells = cells.as_any().downcast_ref::<Int64Array>().unwrap();
    cells.iter().map(|cell| cell.unwrap()).collect()
}

/// A scripted source of `Sym` and `Qty` with three rows.
fn scripted(graph: &Arc<dyn UpdateGraph>) -> Arc<ScriptedTable> {
    ScriptedTable::new(
        graph.clone(),
        definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)]),
        RowSet::from_keys([0, 1, 5]),
        vec![strings(["a", "b", "c"]), longs([10, 20, 30])],
    )
}

/// A clock that tests set.
struct ManualTime(AtomicI64);

impl TimeSource for ManualTime {
    fn now_nanos(&self) -> i64 {
        self.0.load(Ordering::SeqCst) * 1_000_000
    }
}

/// A function that returns the tables of `tables` one after another, and the last again.
fn in_turn(tables: Vec<Arc<dyn Table>>) -> (TableGenerator, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let generator: TableGenerator = Arc::new(move || {
        let call = counted.fetch_add(1, Ordering::SeqCst);
        Ok(tables[call.min(tables.len() - 1)].clone())
    });
    (generator, calls)
}

#[test]
fn a_table_generated_from_a_source_follows_a_function_of_it() {
    let mut updates = 0;
    for seed in test_seeds(0..40) {
        let (stepper, graph) = graph();
        let source = scripted(&graph);
        let parent: Arc<dyn Table> = source.table();
        let read = parent.clone();
        let generator: TableGenerator = Arc::new(move || Ok(copy_of(&*read)));
        let result = generated_from_sources(generator, std::slice::from_ref(&parent)).unwrap();
        assert!(result.is_refreshing() && !result.is_flat());
        let expected = parent.clone();
        let checker =
            ConsistencyChecker::attach_with_recompute(result.clone(), move || copy_of(&*expected));
        let mut random = RandomUpdates::new(seed);
        for _ in 0..60 {
            source.push(random.next_change(&*parent, 4));
            stepper.run_cycle();
            checker.assert_consistent();
        }
        updates += checker.updates_seen();
    }
    assert!(updates >= at_least_for(40 * 30, 0..40), "{updates} updates");
}

#[test]
fn a_generated_table_is_generated_once_per_cycle_in_which_a_source_changed() {
    let (stepper, graph) = graph();
    let (first, second) = (scripted(&graph), scripted(&graph));
    let parents: Vec<Arc<dyn Table>> = vec![first.table(), second.table()];
    let tables = [
        vec![1, 2],
        vec![3, 4, 5],
        vec![6],
        vec![],
        vec![],
        vec![7, 8],
    ];
    let (generator, calls) = in_turn(tables.into_iter().map(quantities).collect());
    let result = generated_from_sources(generator, &parents).unwrap();
    let recorder = Recorder::attach(result.clone());
    let checker = ConsistencyChecker::attach(result.clone());
    assert_eq!(
        (held(&*result), calls.load(Ordering::SeqCst)),
        (vec![1, 2], 1)
    );

    // a cycle without a change of a source
    stepper.run_cycle();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let tick = |both: bool| {
        first.push(
            TableChange::new()
                .remove([0])
                .add([0], vec![strings(["x"]), longs([1])]),
        );
        if both {
            second.push(TableChange::new().modify([1], [(1, longs([21]))]));
        }
        stepper.run_cycle();
        checker.assert_consistent();
    };
    tick(true);
    assert_eq!(
        (held(&*result), calls.load(Ordering::SeqCst)),
        (vec![3, 4, 5], 2)
    );
    tick(false);
    assert_eq!(held(&*result), [6]);
    tick(false);
    tick(true);
    tick(false);
    assert_eq!(
        (held(&*result), calls.load(Ordering::SeqCst)),
        (vec![7, 8], 6)
    );

    // Rows that stay are modified in every column, and rows come and go at the end. Of
    // no rows before and none after, nothing is said.
    let flat = RowSet::flat;
    let range = RowSet::from_range;
    let events: Vec<TableUpdate> = recorder.events().into_iter().map(Result::unwrap).collect();
    let expected = [
        (range(2, 2), RowSet::empty(), flat(2)),
        (RowSet::empty(), range(1, 2), flat(1)),
        (RowSet::empty(), flat(1), RowSet::empty()),
        (flat(2), RowSet::empty(), RowSet::empty()),
    ];
    assert_eq!(events.len(), expected.len());
    for (event, (added, removed, modified)) in events.iter().zip(expected) {
        let all = !modified.is_empty();
        assert_eq!((&event.added, &event.removed), (&added, &removed));
        assert_eq!(event.modified, modified);
        assert_eq!(event.modified_columns == ModifiedColumnSet::All, all);
    }

    // the same rows again: all of them modified
    tick(false);
    let last = recorder.events().pop().unwrap().unwrap();
    assert_eq!((last.modified, last.added.is_empty()), (flat(2), true));
}

#[test]
fn a_table_generated_without_sources_or_on_a_negative_interval_is_static() {
    let (_stepper, graph) = graph();
    let clock = Arc::new(ManualTime(AtomicI64::new(0)));
    let (generator, calls) = in_turn(vec![quantities([1, 2])]);
    let result = generated_from_sources(generator.clone(), &[]).unwrap();
    assert!(!result.is_refreshing());
    assert_eq!(held(&*result), [1, 2]);
    let result = generated_on_interval(generator, graph, clock, -5).unwrap();
    assert!(!result.is_refreshing());
    assert_eq!(
        (held(&*result), calls.load(Ordering::SeqCst)),
        (vec![1, 2], 2)
    );
}

#[test]
fn every_source_of_a_generated_table_must_be_refreshing() {
    let (_stepper, graph) = graph();
    let (generator, calls) = in_turn(vec![quantities([1])]);
    let sources = [scripted(&graph).table() as Arc<dyn Table>, quantities([])];
    let refused = generated_from_sources(generator, &sources).err().unwrap();
    assert_eq!(
        refused.message,
        "java.lang.IllegalArgumentException: All source tables must be refreshing"
    );
    // the function was not called
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn a_table_generated_on_an_interval_is_generated_when_the_interval_has_passed() {
    let (stepper, graph) = graph();
    let clock = Arc::new(ManualTime(AtomicI64::new(1_000)));
    let tables = [vec![1], vec![2, 3], vec![4]];
    let (generator, calls) = in_turn(tables.into_iter().map(quantities).collect());
    let result = generated_on_interval(generator, graph.clone(), clock.clone(), 400).unwrap();
    assert!(result.is_refreshing());
    let checker = ConsistencyChecker::attach(result.clone());
    let at = |millis: i64| {
        clock.0.store(millis, Ordering::SeqCst);
        stepper.run_cycle();
        checker.assert_consistent();
        (held(&*result), calls.load(Ordering::SeqCst))
    };
    assert_eq!(at(1_000), (vec![1], 1));
    assert_eq!(at(1_399), (vec![1], 1));
    assert_eq!(at(1_400), (vec![2, 3], 2));
    // the interval counts from the refresh, not from when it was due
    assert_eq!(at(1_500), (vec![2, 3], 2));
    assert_eq!(at(1_799), (vec![2, 3], 2));
    assert_eq!(at(1_950), (vec![4], 3));
    assert_eq!(at(2_349), (vec![4], 3));
    assert_eq!(at(2_350), (vec![4], 4));
    assert!(result.satisfied(graph.clock().step));
}

#[test]
fn a_table_generated_on_an_interval_of_zero_is_refreshing_and_never_generated_again() {
    let (stepper, graph) = graph();
    let clock = Arc::new(ManualTime(AtomicI64::new(0)));
    let (generator, calls) = in_turn(vec![quantities([1]), quantities([2])]);
    let result = generated_on_interval(generator, graph.clone(), clock.clone(), 0).unwrap();
    assert!(result.is_refreshing());
    clock.0.store(60_000, Ordering::SeqCst);
    stepper.run_cycle();
    assert_eq!((held(&*result), calls.load(Ordering::SeqCst)), (vec![1], 1));
    assert!(result.satisfied(graph.clock().step));
}

/// The result of a function that fails, or returns a table that cannot be copied, at its
/// second call: the failure and the rows the result is left with.
fn failed_at_the_second_call(second: Result<Arc<dyn Table>, TableFailure>) -> (String, Vec<i64>) {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let first = fixed(&[
        ("Qty", ColumnType::Long, longs([1, 2])),
        ("Sym", ColumnType::String, strings(["a", "b"])),
    ]);
    let calls = AtomicUsize::new(0);
    let generator: TableGenerator = Arc::new(move || {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(first.clone())
        } else {
            second.clone()
        }
    });
    let sources = [source.table() as Arc<dyn Table>];
    let result = generated_from_sources(generator, &sources).unwrap();
    let recorder = Recorder::attach(result.clone());
    for key in [0, 1] {
        source.push(TableChange::new().remove([key]));
        stepper.run_cycle();
    }
    assert!(result.is_failed());
    // one failure, and nothing after it
    let events = recorder.events();
    assert_eq!(events.len(), 1);
    (events[0].clone().err().unwrap().message, held(&*result))
}

#[test]
fn a_generated_table_fails_when_its_function_fails_in_a_cycle() {
    let (message, rows) = failed_at_the_second_call(Err(failure("boom")));
    assert_eq!((message.as_str(), rows), ("boom", vec![1, 2]));
}

#[test]
fn a_generated_table_fails_when_the_columns_of_the_function_change() {
    let qty = |values: [i64; 1]| ("Qty", ColumnType::Long, longs(values));
    let sym = ("Sym", ColumnType::String, strings(["z"]));
    // another column
    let more = fixed(&[qty([9]), sym.clone(), ("New", ColumnType::Long, longs([1]))]);
    let (message, rows) = failed_at_the_second_call(Ok(more));
    assert!(
        message.starts_with("java.lang.NullPointerException"),
        "{message}"
    );
    assert_eq!(rows, [1, 2]);
    // a column of another type
    let retyped = fixed(&[qty([9]), ("Sym", ColumnType::Long, longs([1]))]);
    let (message, _) = failed_at_the_second_call(Ok(retyped));
    assert!(
        message.contains("the type of column Sym changed"),
        "{message}"
    );
    // a column less, which Java copies: refused by the name of its method
    let (message, _) = failed_at_the_second_call(Ok(fixed(&[qty([9])])));
    assert_eq!(
        message,
        "not ported yet: io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory\
         #copyTable: the generated table no longer has the column Sym"
    );
}

#[test]
fn the_columns_of_a_generated_table_are_copied_by_name() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let first = fixed(&[
        ("Qty", ColumnType::Long, longs([1, 2])),
        ("Sym", ColumnType::String, strings(["a", "b"])),
    ]);
    let swapped = fixed(&[
        ("Sym", ColumnType::String, strings(["c"])),
        ("Qty", ColumnType::Long, longs([3])),
    ]);
    let (generator, _) = in_turn(vec![first.clone(), swapped]);
    let sources = [source.table() as Arc<dyn Table>];
    let result = generated_from_sources(generator, &sources).unwrap();
    assert_tables_equal(&*first, &*result);
    source.push(TableChange::new().remove([0]));
    stepper.run_cycle();
    let expected = fixed(&[
        ("Qty", ColumnType::Long, longs([3])),
        ("Sym", ColumnType::String, strings(["c"])),
    ]);
    assert_tables_equal(&*expected, &*result);
    let symbols = result.column(1).chunk(&result.row_set()).unwrap();
    let symbols = symbols.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(symbols.value(0), "c");
}

#[test]
fn a_function_may_return_a_refreshing_table_once_the_sources_have_refreshed() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    // From a source: the function runs after the graph's sources, and reads the table.
    let read = parent.clone();
    let generator: TableGenerator = Arc::new(move || Ok(read.clone()));
    let result = generated_from_sources(generator.clone(), std::slice::from_ref(&parent)).unwrap();
    let expected = parent.clone();
    let checker =
        ConsistencyChecker::attach_with_recompute(result.clone(), move || copy_of(&*expected));
    // On an interval: the function runs while the sources refresh, and Java refuses a
    // refreshing table then.
    let clock = Arc::new(ManualTime(AtomicI64::new(0)));
    let timed = generated_on_interval(generator, graph.clone(), clock.clone(), 10).unwrap();
    let recorder = Recorder::attach(timed.clone());
    assert_eq!(timed.row_set().size(), 3);

    clock.0.store(10, Ordering::SeqCst);
    source.push(TableChange::new().remove([0]));
    stepper.run_cycle();
    checker.assert_consistent();
    assert!(!result.is_failed() && timed.is_failed());
    let failure = recorder.events().pop().unwrap().err().unwrap();
    assert!(failure
        .message
        .contains("The function-generated table is not satisfied"));

    // a table of another graph
    let other: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let foreign: Arc<dyn Table> = scripted(&other).table();
    let generator: TableGenerator = Arc::new(move || Ok(foreign.clone()));
    let refused = generated_from_sources(generator, std::slice::from_ref(&parent));
    assert!(refused
        .err()
        .unwrap()
        .message
        .contains("must belong to the same UpdateGraph"));
}

#[test]
fn a_failure_of_the_first_call_is_the_error_of_making_the_table() {
    let (_stepper, graph) = graph();
    let source = scripted(&graph);
    let generator: TableGenerator = Arc::new(|| Err(failure("no table")));
    let sources = [source.table() as Arc<dyn Table>];
    let refused = generated_from_sources(generator.clone(), &sources)
        .err()
        .unwrap();
    assert_eq!(refused.message, "no table");
    let clock = Arc::new(ManualTime(AtomicI64::new(0)));
    let refused = generated_on_interval(generator, graph, clock, 100)
        .err()
        .unwrap();
    assert_eq!(refused.message, "no table");
}
