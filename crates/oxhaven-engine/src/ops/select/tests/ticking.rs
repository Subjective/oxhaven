//! Every incremental case, under the consistency checker.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{append_only_input_table, keyed_input_table, tracked_source, RootSource};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    at_least_for, definition, longs, strings, test_seeds, ConsistencyChecker, RandomUpdates,
    Recorder, ScriptedTable, TableChange,
};
use std::sync::{Mutex, Weak};

pub(super) type Operation = Arc<dyn Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync>;

pub(super) fn operation(f: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static) -> Operation {
    Arc::new(f)
}

pub(super) fn graph() -> (Arc<SteppedUpdateGraph>, Arc<dyn UpdateGraph>) {
    let graph = SteppedUpdateGraph::new();
    (graph.clone(), graph)
}

fn sym_a_b() -> TableDefinition {
    definition(&[
        ("Sym", ColumnType::String),
        ("A", ColumnType::Long),
        ("B", ColumnType::Long),
    ])
}

/// A scripted source with rows 0, 1, 5 and 6, `A` 10 to 40 and `B` 1 to 4.
pub(super) fn scripted(graph: &Arc<dyn UpdateGraph>) -> Arc<ScriptedTable> {
    ScriptedTable::new(
        graph.clone(),
        sym_a_b(),
        RowSet::from_keys([0, 1, 5, 6]),
        vec![
            strings(["a", "b", "c", "d"]),
            longs([10, 20, 30, 40]),
            longs([1, 2, 3, 4]),
        ],
    )
}

fn batch(columns: &[(&str, ArrayRef)]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, values)| Field::new(*name, values.data_type().clone(), true))
        .collect();
    let arrays = columns.iter().map(|(_, values)| values.clone()).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// A checker that compares `result` with `operation` run on a static copy of `parent`.
pub(super) fn checked(
    result: &Arc<dyn Table>,
    parent: &Arc<dyn Table>,
    operation: &Operation,
) -> Arc<ConsistencyChecker> {
    let (parent, operation) = (parent.clone(), operation.clone());
    ConsistencyChecker::attach_with_recompute(result.clone(), move || operation(&frozen(&*parent)))
}

/// Runs `operation` over a randomly changing source for many seeds, checking after every
/// cycle that the result is consistent and equal to the operation run from scratch.
pub(super) fn fuzz(what: &str, operation: Operation) {
    let mut updates = 0;
    for seed in test_seeds(0..24) {
        let (stepper, graph) = graph();
        let source = scripted(&graph);
        let parent: Arc<dyn Table> = source.table();
        let result = operation(&parent);
        assert!(result.is_refreshing(), "{what}");
        let checker = checked(&result, &parent, &operation);
        checker.assert_consistent();
        let mut random = RandomUpdates::new(seed * 31 + 7);
        for cycle in 0..30 {
            source.push(random.next_change(&*parent, 6));
            stepper.run_cycle();
            if let Err(e) = checker.verify() {
                panic!("{what}, seed {seed}, cycle {cycle}: {e}");
            }
        }
        assert!(!result.is_failed(), "{what}");
        updates += checker.updates_seen();
    }
    assert!(
        updates > at_least_for(24 * 15, 0..24),
        "{what}: only {updates} updates"
    );
}

// ---------------------------------------------------------------------------------------
// Random updates
// ---------------------------------------------------------------------------------------

#[test]
fn update_follows_random_changes() {
    fuzz(
        "update",
        operation(|t| {
            let columns = ["X = A + B", "Y = X * 2", "A = A + 1", "Z = A + Y", "C = 3"];
            update(t, &columns, &Compiler).unwrap()
        }),
    );
}

#[test]
fn select_follows_random_changes() {
    fuzz(
        "select",
        operation(|t| select(t, &["Sym", "S = A + B", "T = S + 1", "B2 = B"], &Compiler).unwrap()),
    );
    fuzz(
        "select of everything",
        operation(|t| select(t, &[] as &[&str], &Compiler).unwrap()),
    );
}

#[test]
fn view_and_update_view_follow_random_changes() {
    fuzz(
        "view",
        operation(|t| view(t, &["Sym", "S = A + B", "A2 = A", "T = S - A2"], &Compiler).unwrap()),
    );
    fuzz(
        "update_view",
        operation(|t| update_view(t, &["S = A + B", "B = S * 2", "C = 1 + 1"], &Compiler).unwrap()),
    );
}

#[test]
fn lazy_update_follows_random_changes() {
    fuzz(
        "lazy_update",
        operation(|t| lazy_update(t, &["S = A + B", "T = S + 1", "A = T"], &Compiler).unwrap()),
    );
}

#[test]
fn drop_columns_follows_random_changes() {
    fuzz(
        "drop_columns",
        operation(|t| drop_columns(t, &["A"]).unwrap()),
    );
}

#[test]
fn stacked_operations_follow_random_changes() {
    fuzz(
        "update of a view",
        operation(|t| {
            let viewed = view(t, &["V = A + 1", "B", "Sym"], &Compiler).unwrap();
            update(&viewed, &["W = V + B"], &Compiler).unwrap()
        }),
    );
    fuzz(
        "select of a view",
        operation(|t| {
            let viewed = view(t, &["V = A + 1", "B"], &Compiler).unwrap();
            select(&viewed, &["V", "V2 = V", "B", "W = V2 * B"], &Compiler).unwrap()
        }),
    );
    fuzz(
        "view of a select",
        operation(|t| {
            let stored = select(t, &["S = A + B", "Sym"], &Compiler).unwrap();
            view(&stored, &["T = S * 2", "Sym"], &Compiler).unwrap()
        }),
    );
    fuzz(
        "drop_columns of an update of an update",
        operation(|t| {
            let first = update(t, &["X = A + B"], &Compiler).unwrap();
            let second = update(&first, &["Y = X + 1", "X = Y + B"], &Compiler).unwrap();
            drop_columns(&second, &["A", "Y"]).unwrap()
        }),
    );
    fuzz(
        "lazy_update of an update_view",
        operation(|t| {
            let viewed = update_view(t, &["X = A + B"], &Compiler).unwrap();
            lazy_update(&viewed, &["Y = X + 1"], &Compiler).unwrap()
        }),
    );
}

// ---------------------------------------------------------------------------------------
// Each incremental case
// ---------------------------------------------------------------------------------------

pub(super) fn last_update(recorder: &Recorder) -> TableUpdate {
    match recorder.events().last() {
        Some(Ok(update)) => update.clone(),
        other => panic!("expected an update, got {other:?}"),
    }
}

#[test]
fn update_computes_added_rows_and_only_the_columns_a_modify_reaches() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let counting = Counting::new();
    // Sym 0, A 1, B 2, X 3, Y 4, Z 5.
    let columns = ["X = A + 1", "Y = B * 2", "Z = X + Y"];
    let result = update(&parent, &columns, &counting).unwrap();
    assert_eq!(counting.take(), 12);
    let checker = checked(
        &result,
        &parent,
        &operation(move |t| update(t, &columns, &Compiler).unwrap()),
    );
    let recorder = Recorder::attach(result.clone());
    let cycle = |change: TableChange| {
        source.push(change);
        stepper.run_cycle();
        checker.assert_consistent();
        counting.take()
    };

    // An added row is computed for every column.
    let evaluated =
        cycle(TableChange::new().add([9], vec![strings(["e"]), longs([50]), longs([5])]));
    assert_eq!(evaluated, 3);
    assert_eq!(last_update(&recorder).added, RowSet::from_keys([9]));
    assert_eq!(values(&*result, "Z"), ["13", "25", "37", "49", "61"]);

    // A change to A recomputes X, and Z through X, and leaves Y alone.
    let evaluated = cycle(TableChange::new().modify([1], [(1, longs([21]))]));
    assert_eq!(evaluated, 2);
    let update_seen = last_update(&recorder);
    assert_eq!(update_seen.modified, RowSet::from_keys([1]));
    assert_eq!(
        update_seen.modified_columns,
        ModifiedColumnSet::of([1, 3, 5])
    );
    assert_eq!(values(&*result, "Z"), ["13", "26", "37", "49", "61"]);

    // A change to a column no formula reads computes nothing.
    let evaluated = cycle(TableChange::new().modify([1], [(0, strings(["z"]))]));
    assert_eq!(evaluated, 0);
    assert_eq!(
        last_update(&recorder).modified_columns,
        ModifiedColumnSet::of([0])
    );

    // Removed and shifted rows compute nothing; shifted rows keep their values.
    assert_eq!(cycle(TableChange::new().remove([0])), 0);
    assert_eq!(last_update(&recorder).removed, RowSet::from_keys([0]));
    assert_eq!(cycle(TableChange::new().shift(5, 6, 2)), 0);
    assert_eq!(keys(&*result), [1, 7, 8, 9]);
    assert_eq!(values(&*result, "Z"), ["26", "37", "49", "61"]);
    assert!(!last_update(&recorder).shifted.ranges().is_empty());

    // Everything at once: the added row for all three columns, the modified B for Y and Z.
    let evaluated = cycle(
        TableChange::new()
            .remove([1])
            .shift(7, 8, -5)
            .add([0], vec![strings(["f"]), longs([60]), longs([6])])
            .modify([3], [(2, longs([40]))]),
    );
    assert_eq!(evaluated, 5);
    assert_eq!(keys(&*result), [0, 2, 3, 9]);
    assert_eq!(values(&*result, "Z"), ["73", "37", "121", "61"]);
    assert_eq!(
        last_update(&recorder).modified_columns,
        ModifiedColumnSet::of([2, 4, 5])
    );

    // A cycle that changes nothing delivers nothing.
    let events = recorder.events().len();
    assert_eq!(events, 6);
    assert_eq!(cycle(TableChange::new()), 0);
    assert_eq!(recorder.events().len(), events);
}

#[test]
fn select_computes_added_rows_and_only_the_columns_a_modify_reaches() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let counting = Counting::new();
    // B 0, X 1, Y 2.
    let columns = ["B", "X = A + 1", "Y = B * 2"];
    let result = select(&parent, &columns, &counting).unwrap();
    assert_eq!(counting.take(), 8);
    // A refreshing select keeps the parent's row keys.
    assert_eq!(keys(&*result), [0, 1, 5, 6]);
    let checker = checked(
        &result,
        &parent,
        &operation(move |t| select(t, &columns, &Compiler).unwrap()),
    );
    let recorder = Recorder::attach(result.clone());
    let cycle = |change: TableChange| {
        source.push(change);
        stepper.run_cycle();
        checker.assert_consistent();
        counting.take()
    };

    assert_eq!(cycle(TableChange::new().modify([5], [(1, longs([31]))])), 1);
    assert_eq!(
        last_update(&recorder).modified_columns,
        ModifiedColumnSet::of([1])
    );
    assert_eq!(cycle(TableChange::new().modify([5], [(2, longs([7]))])), 1);
    assert_eq!(
        last_update(&recorder).modified_columns,
        ModifiedColumnSet::of([0, 2])
    );
    assert_eq!(values(&*result, "X"), ["11", "21", "32", "41"]);
    assert_eq!(values(&*result, "Y"), ["2", "4", "14", "8"]);

    // A change to a column the result does not have is no change of the result.
    let events = recorder.events().len();
    assert_eq!(
        cycle(TableChange::new().modify([5], [(0, strings(["q"]))])),
        0
    );
    assert_eq!(recorder.events().len(), events);

    let added = TableChange::new().remove([0, 6]).add(
        [2, 3],
        vec![strings(["g", "h"]), longs([1, 2]), longs([3, 4])],
    );
    assert_eq!(cycle(added), 4);
    assert_eq!(keys(&*result), [1, 2, 3, 5]);
    assert_eq!(values(&*result, "X"), ["21", "2", "3", "32"]);
}

/// The modified columns `result` reports when the given parent column is modified in one
/// row, or `None` if it reports no update.
fn reported(
    make: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
    parent_column: usize,
) -> Option<ModifiedColumnSet> {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let make = operation(make);
    let result = make(&parent);
    let checker = checked(&result, &parent, &make);
    let recorder = Recorder::attach(result.clone());
    let value = if parent_column == 0 {
        strings(["changed"])
    } else {
        longs([77])
    };
    source.push(TableChange::new().modify([5], [(parent_column, value)]));
    stepper.run_cycle();
    checker.assert_consistent();
    let events = recorder.events();
    assert!(events.len() <= 1);
    events.last().map(|event| {
        let update = event.clone().unwrap();
        assert_eq!(update.modified, RowSet::from_keys([5]));
        update.modified_columns
    })
}

#[test]
fn modified_columns_follow_what_each_result_column_reads() {
    let (sym, a, b) = (0, 1, 2);
    let columns = |c: &[usize]| Some(ModifiedColumnSet::of(c.iter().copied()));

    // S 0, Sym2 1, B 2.
    let viewed =
        |t: &Arc<dyn Table>| view(t, &["S = A + B", "Sym2 = Sym", "B"], &Compiler).unwrap();
    assert_eq!(reported(viewed, a), columns(&[0]));
    assert_eq!(reported(viewed, b), columns(&[0, 2]));
    assert_eq!(reported(viewed, sym), columns(&[1]));

    // Sym 0, A 1, B 2, C 3: the new A reads B, and hides the parent's A from C.
    let shadowed =
        |t: &Arc<dyn Table>| update_view(t, &["A = B + 1", "C = A * 2"], &Compiler).unwrap();
    assert_eq!(reported(shadowed, a), None);
    assert_eq!(reported(shadowed, b), columns(&[1, 2, 3]));
    assert_eq!(reported(shadowed, sym), columns(&[0]));
    let stored = |t: &Arc<dyn Table>| update(t, &["A = B + 1", "C = A * 2"], &Compiler).unwrap();
    assert_eq!(reported(stored, a), None);
    assert_eq!(reported(stored, b), columns(&[1, 2, 3]));

    // A name assigned twice reports what its last value reads. Sym 0, A 1, B 2, X 3.
    let twice = |t: &Arc<dyn Table>| update(t, &["X = A", "X = B"], &Compiler).unwrap();
    assert_eq!(reported(twice, a), columns(&[1]));
    assert_eq!(reported(twice, b), columns(&[2, 3]));

    // A constant never changes.
    let constant = |t: &Arc<dyn Table>| view(t, &["C = 1", "A"], &Compiler).unwrap();
    assert_eq!(reported(constant, b), None);
    assert_eq!(reported(constant, a), columns(&[1]));

    // Sym 0, B 1.
    let dropped = |t: &Arc<dyn Table>| drop_columns(t, &["A"]).unwrap();
    assert_eq!(reported(dropped, a), None);
    assert_eq!(reported(dropped, b), columns(&[1]));
    assert_eq!(reported(dropped, sym), columns(&[0]));

    // A lazy update reports every column, whatever changed.
    let lazy = |t: &Arc<dyn Table>| lazy_update(t, &["X = A + 1"], &Compiler).unwrap();
    assert_eq!(reported(lazy, sym), Some(ModifiedColumnSet::All));
    assert_eq!(reported(lazy, a), Some(ModifiedColumnSet::All));
}

#[test]
fn a_view_reads_previous_values_during_the_cycle() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    let result = view(&parent, &["S = A + B"], &Compiler).unwrap();
    let lazy = lazy_update(&parent, &["S = A + B"], &Compiler).unwrap();

    struct Seen {
        tables: Vec<Arc<dyn Table>>,
        seen: Mutex<Vec<(Vec<String>, Vec<String>)>>,
    }
    impl oxhaven_contracts::table::UpdateListener for Seen {
        fn on_update(&self, _: &TableUpdate) -> Result<(), TableFailure> {
            for table in &self.tables {
                let column = table.column(table.definition().index_of("S").unwrap());
                let previous = column.prev_chunk(&table.prev_row_set())?;
                let current = column.chunk(&table.row_set())?;
                self.seen
                    .lock()
                    .unwrap()
                    .push((cells(&previous), cells(&current)));
            }
            Ok(())
        }
        fn on_failure(&self, _: &TableFailure) {}
    }
    impl oxhaven_contracts::graph::Dependency for Seen {
        fn satisfied(&self, step: u64) -> bool {
            self.tables.iter().all(|table| table.satisfied(step))
        }
    }
    let seen = Arc::new(Seen {
        tables: vec![result.clone(), lazy.clone()],
        seen: Mutex::default(),
    });
    let listener: Arc<dyn oxhaven_contracts::table::UpdateListener> = seen.clone();
    // The lazy table delivers after the view was satisfied too: both read the same parent.
    lazy.add_listener(&listener);

    source.push(
        TableChange::new()
            .remove([0])
            .modify([5], [(1, longs([100]))]),
    );
    stepper.run_cycle();
    let before = ["11", "22", "33", "44"].map(String::from).to_vec();
    let after = ["22", "103", "44"].map(String::from).to_vec();
    assert_eq!(
        *seen.seen.lock().unwrap(),
        [(before.clone(), after.clone()), (before, after)]
    );
}

// ---------------------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------------------

#[test]
fn a_failed_parent_fails_every_result() {
    let makes: [(&str, Operation); 6] = [
        (
            "view",
            operation(|t| view(t, &["X = A + 1"], &Compiler).unwrap()),
        ),
        (
            "update_view",
            operation(|t| update_view(t, &["X = A + 1"], &Compiler).unwrap()),
        ),
        (
            "lazy_update",
            operation(|t| lazy_update(t, &["X = A + 1"], &Compiler).unwrap()),
        ),
        (
            "select",
            operation(|t| select(t, &["X = A + 1"], &Compiler).unwrap()),
        ),
        (
            "update",
            operation(|t| update(t, &["X = A + 1"], &Compiler).unwrap()),
        ),
        (
            "drop_columns",
            operation(|t| drop_columns(t, &["A"]).unwrap()),
        ),
    ];
    for (what, make) in makes {
        let (stepper, graph) = graph();
        let source = scripted(&graph);
        let parent: Arc<dyn Table> = source.table();
        let result = make(&parent);
        let checker = ConsistencyChecker::attach(result.clone());
        let recorder = Recorder::attach(result.clone());
        source.push_failure("boom");
        stepper.run_cycle();
        assert!(result.is_failed(), "{what}");
        let events = recorder.events();
        assert_eq!(events.len(), 1, "{what}");
        let failure = events[0].clone().unwrap_err();
        assert!(failure.message.contains("boom"), "{what}: {failure}");
        // The checker reports the failure, and nothing else.
        let report = checker.verify().unwrap_err().to_string();
        assert!(report.ends_with("table failed: boom\n") || report.ends_with("table failed: boom"));
        assert_eq!(report.matches(" - ").count(), 1, "{what}: {report}");
        // Nothing can be made from a failed table.
        assert!(
            matches!(
                update(&result, &["Q = 1"], &Compiler),
                Err(SelectError::Failed(_))
            ),
            "{what}"
        );
    }
}

#[test]
fn a_formula_that_fails_in_a_cycle_fails_a_stored_result() {
    for bad in [
        // B becomes zero in an existing row.
        TableChange::new().modify([1], [(2, longs([0]))]),
        // A new row has a zero B.
        TableChange::new().add([9], vec![strings(["e"]), longs([50]), longs([0])]),
    ] {
        for stored in [update, select] {
            let (stepper, graph) = graph();
            let source = scripted(&graph);
            let parent: Arc<dyn Table> = source.table();
            let result = stored(&parent, &["X = A % B", "A"], &Compiler).unwrap();
            let recorder = Recorder::attach(result.clone());
            assert_eq!(values(&*result, "X"), ["0", "0", "0", "0"]);

            source.push(TableChange::new().modify([0], [(2, longs([3]))]));
            stepper.run_cycle();
            assert_eq!(values(&*result, "X"), ["1", "0", "0", "0"]);
            assert!(!result.is_failed());

            source.push(bad.clone());
            stepper.run_cycle();
            assert!(result.is_failed());
            let events = recorder.events();
            assert_eq!(events.len(), 2);
            assert!(events[0].is_ok() && events[1].is_err(), "{events:?}");

            // The failed result stays failed, and the parent goes on.
            source.push(TableChange::new().remove([0]));
            stepper.run_cycle();
            assert!(result.is_failed() && !parent.is_failed());
            assert_eq!(recorder.events().len(), 2);
        }
    }
}

#[test]
fn a_stored_result_cannot_be_made_while_its_formula_fails() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    source.push(TableChange::new().modify([1], [(2, longs([0]))]));
    stepper.run_cycle();
    assert!(matches!(
        update(&parent, &["X = A % B"], &Compiler),
        Err(SelectError::Failed(_))
    ));
    assert!(matches!(
        select(&parent, &["X = A % B"], &Compiler),
        Err(SelectError::Failed(_))
    ));
}

#[test]
fn a_view_fails_only_when_the_bad_row_is_read_and_recovers() {
    // The consistency checker reads every row and so cannot watch a table whose rows
    // cannot all be read; this test reads the columns itself.
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    for computed in [view, update_view, lazy_update] {
        // The parent may hold a zero left by an earlier round.
        let result = computed(&parent, &["X = A % B", "B"], &Compiler).unwrap();
        let recorder = Recorder::attach(result.clone());
        let x = result.column(result.definition().index_of("X").unwrap());
        assert_eq!(values(&*result, "X"), ["0", "0", "0", "0"]);

        source.push(TableChange::new().modify([1], [(2, longs([0]))]));
        stepper.run_cycle();
        // The update went through, and only a read of the bad row fails.
        assert!(!result.is_failed());
        assert_eq!(last_update(&recorder).modified, RowSet::from_keys([1]));
        assert!(x.chunk(&result.row_set()).is_err());
        assert_eq!(
            cells(&x.chunk(&RowSet::from_keys([0, 5])).unwrap()),
            ["0", "0"]
        );
        assert_eq!(values(&*result, "B"), ["1", "0", "3", "4"]);

        source.push(TableChange::new().modify([1], [(2, longs([7]))]));
        stepper.run_cycle();
        assert!(!result.is_failed());
        assert_eq!(values(&*result, "X"), ["0", "6", "0", "0"]);
        assert_eq!(recorder.events().len(), 2);
        ConsistencyChecker::attach(result.clone()).assert_consistent();

        source.push(TableChange::new().modify([1], [(2, longs([2]))]));
        stepper.run_cycle();
    }
}

// ---------------------------------------------------------------------------------------
// Attributes
// ---------------------------------------------------------------------------------------

/// A root table with one long column `V` that applies whatever the test queues.
pub(super) struct Fed {
    table: Weak<QueryTable>,
    column: Arc<WritableSource>,
    next: Mutex<Option<(TableUpdate, Vec<i64>)>>,
}

impl Fed {
    pub(super) fn push(&self, removed: &[i64], added: &[i64], values: &[i64]) {
        let update = TableUpdate {
            removed: RowSet::from_keys(removed.iter().copied()),
            added: RowSet::from_keys(added.iter().copied()),
            ..TableUpdate::empty()
        };
        *self.next.lock().unwrap() = Some((update, values.to_vec()));
    }
}

impl UpdateSource for Fed {
    fn refresh(&self) {
        let next = self.next.lock().unwrap().take();
        if let (Some(table), Some((update, values))) = (self.table.upgrade(), next) {
            self.column.remove(&update.removed);
            self.column.fill(&update.added, &longs(values)).unwrap();
            table.notify(update);
        }
    }
}

pub(super) fn fed(
    graph: &Arc<dyn UpdateGraph>,
    attributes: TableAttributes,
    flat: bool,
) -> (Arc<dyn Table>, Arc<Fed>) {
    let column = tracked_source(graph, ColumnType::Long);
    let mut spec = TableSpec::new(
        definition(&[("V", ColumnType::Long)]),
        RowSet::empty(),
        vec![column.clone()],
    )
    .with_attributes(attributes);
    spec.flat = flat;
    let mut handle = None;
    let table = QueryTable::root(graph.clone(), spec, |table| {
        let source = Arc::new(Fed {
            table,
            column,
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

pub(super) fn attributes(add_only: bool, append_only: bool, blink: bool) -> TableAttributes {
    let mut attributes = TableAttributes::default();
    attributes.add_only = add_only;
    attributes.append_only = append_only;
    attributes.blink = blink;
    attributes
}

fn computing() -> [(&'static str, Operation); 5] {
    [
        (
            "view",
            operation(|t| view(t, &["W = V + 1"], &Compiler).unwrap()),
        ),
        (
            "update_view",
            operation(|t| update_view(t, &["W = V + 1"], &Compiler).unwrap()),
        ),
        (
            "lazy_update",
            operation(|t| lazy_update(t, &["W = V + 1"], &Compiler).unwrap()),
        ),
        (
            "select",
            operation(|t| select(t, &["W = V + 1"], &Compiler).unwrap()),
        ),
        (
            "update",
            operation(|t| update(t, &["W = V + 1"], &Compiler).unwrap()),
        ),
    ]
}

#[test]
fn results_of_a_refreshing_parent_keep_what_it_says_about_its_changes() {
    let cases = [
        // (parent attributes, parent flat, result attributes)
        (
            attributes(false, false, false),
            false,
            attributes(false, false, false),
        ),
        (
            attributes(true, false, false),
            false,
            attributes(true, false, false),
        ),
        // Rows that are only added to a flat table are appended.
        (
            attributes(true, false, false),
            true,
            attributes(true, true, false),
        ),
        (
            attributes(true, true, false),
            false,
            attributes(true, true, false),
        ),
        // Append-only implies add-only.
        (
            attributes(false, true, false),
            false,
            attributes(true, true, false),
        ),
        (
            attributes(false, false, true),
            false,
            attributes(false, false, true),
        ),
    ];
    for (given, flat, expected) in cases {
        for (what, make) in computing() {
            let (_stepper, graph) = graph();
            let (parent, _source) = fed(&graph, given, flat);
            let result = make(&parent);
            assert_eq!(
                result.attributes(),
                expected,
                "{what} of {given:?}, flat {flat}"
            );
            assert_eq!(result.is_flat(), flat, "{what}");
        }
        // Dropping columns copies the attributes as they are.
        let (_stepper, graph) = graph();
        let (parent, _source) = fed(&graph, given, flat);
        let both = update(&parent, &["W = V"], &Compiler).unwrap();
        let expected_of_drop = both.attributes();
        let result = drop_columns(&both, &["W"]).unwrap();
        assert_eq!(result.attributes(), expected_of_drop);
        assert_eq!(result.is_flat(), flat);
    }
    let (_stepper, graph) = graph();
    let (parent, _source) = fed(&graph, attributes(true, false, false), true);
    assert_eq!(
        drop_columns(&parent, &["V"]).unwrap().attributes(),
        attributes(true, false, false)
    );
}

#[test]
fn results_of_a_blink_table_hold_only_the_rows_of_the_cycle() {
    for (what, make) in computing() {
        let (stepper, graph) = graph();
        let (parent, source) = fed(&graph, attributes(false, false, true), false);
        let result = make(&parent);
        let checker = checked(&result, &parent, &make);
        source.push(&[], &[0, 1], &[5, 6]);
        stepper.run_cycle();
        checker.assert_consistent();
        assert_eq!(values(&*result, "W"), ["6", "7"], "{what}");
        source.push(&[0, 1], &[0, 1, 2], &[1, 2, 3]);
        stepper.run_cycle();
        checker.assert_consistent();
        assert_eq!(values(&*result, "W"), ["2", "3", "4"], "{what}");
        source.push(&[0, 1, 2], &[], &[]);
        stepper.run_cycle();
        checker.assert_consistent();
        assert!(result.row_set().is_empty() && !result.is_failed(), "{what}");
    }
}

#[test]
fn results_of_input_tables_follow_their_writes() {
    let sym_qty = definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)]);
    let rows = |syms: &[&str], qtys: &[i64]| {
        batch(&[
            ("Sym", strings(syms.iter().copied())),
            ("Qty", longs(qtys.iter().copied())),
        ])
    };
    let columns = ["Sym", "Double = Qty * 2"];
    let makes: [Operation; 3] = [
        operation(move |t| select(t, &columns, &Compiler).unwrap()),
        operation(move |t| update(t, &columns, &Compiler).unwrap()),
        operation(move |t| view(t, &columns, &Compiler).unwrap()),
    ];

    for make in &makes {
        let (_stepper, graph) = graph();
        let appended: Arc<dyn Table> =
            append_only_input_table(graph.clone(), sym_qty.clone(), None).unwrap();
        let result = make(&appended);
        assert_eq!(result.attributes(), attributes(true, true, false));
        assert!(result.is_flat());
        let checker = checked(&result, &appended, make);
        let input = appended.input_table().unwrap();
        input.add(rows(&["a", "b"], &[1, 2])).unwrap();
        checker.assert_consistent();
        input.add(rows(&["a"], &[5])).unwrap();
        checker.assert_consistent();
        assert_eq!(values(&*result, "Double"), ["2", "4", "10"]);
        assert_eq!(keys(&*result), [0, 1, 2]);
        assert_eq!(checker.updates_seen(), 2);

        let keyed: Arc<dyn Table> =
            keyed_input_table(graph.clone(), sym_qty.clone(), &["Sym"], None).unwrap();
        let result = make(&keyed);
        assert_eq!(result.attributes(), TableAttributes::default());
        let checker = checked(&result, &keyed, make);
        let input = keyed.input_table().unwrap();
        input.add(rows(&["a", "b", "c"], &[1, 2, 3])).unwrap();
        checker.assert_consistent();
        input.add(rows(&["b", "d"], &[20, 4])).unwrap();
        checker.assert_consistent();
        assert_eq!(values(&*result, "Double"), ["2", "40", "6", "8"]);
        input
            .delete(batch(&[("Sym", strings(["a", "c"]))]))
            .unwrap();
        checker.assert_consistent();
        assert_eq!(values(&*result, "Sym"), ["b", "d"]);
        assert_eq!(checker.updates_seen(), 3);
    }
}

// ---------------------------------------------------------------------------------------
// Row variables
// ---------------------------------------------------------------------------------------

#[test]
fn row_variables_are_refused_where_a_row_can_move() {
    let refused = |parent: &Arc<dyn Table>, column: &str| {
        let all = [
            view(parent, &[column], &WithRowVariables),
            update_view(parent, &[column], &WithRowVariables),
            select(parent, &[column], &WithRowVariables),
            update(parent, &[column], &WithRowVariables),
        ];
        let refusals = all
            .iter()
            .filter(|result| matches!(result, Err(SelectError::UnsafeForRefresh(_))))
            .count();
        assert!(
            refusals == 0 || refusals == all.len(),
            "{column}: {refusals}"
        );
        assert!(
            refusals > 0 || all.iter().all(|result| result.is_ok()),
            "{column}"
        );
        refusals > 0
    };
    let (_stepper, graph) = graph();

    // Rows of a table without attributes can be removed and shifted.
    let (plain, _source) = fed(&graph, attributes(false, false, false), false);
    assert!(refused(&plain, "X = i"));
    assert!(refused(&plain, "X = ii"));
    assert!(refused(&plain, "X = k"));
    assert!(refused(&plain, "X = V_"));
    assert!(!refused(&plain, "X = V + 1"));
    match update(&plain, &["X = k"], &WithRowVariables) {
        Err(error) => assert_eq!(
            error.to_string(),
            "Formula 'X = k' uses i, ii, k, or column array variables, and is not safe to refresh"
        ),
        Ok(_) => panic!("expected a refusal"),
    }

    // A row of an add-only table keeps its key but not its position.
    let (add_only, _source) = fed(&graph, attributes(true, false, false), false);
    assert!(refused(&add_only, "X = i"));
    assert!(!refused(&add_only, "X = k"));
    assert!(refused(&add_only, "X = V_"));

    let (append_only, _source) = fed(&graph, attributes(true, true, false), false);
    assert!(!refused(&append_only, "X = i"));
    assert!(!refused(&append_only, "X = ii"));
    assert!(!refused(&append_only, "X = k"));
    assert!(refused(&append_only, "X = V_"));

    // A blink table's rows live for one cycle.
    let (blink, _source) = fed(&graph, attributes(false, false, true), false);
    assert!(!refused(&blink, "X = i"));
    assert!(!refused(&blink, "X = k"));
    assert!(!refused(&blink, "X = V_"));

    // A static table never changes.
    let fixed = frozen(&*plain);
    assert!(!refused(&fixed, "X = i"));
    assert!(!refused(&fixed, "X = V_"));
}

#[test]
fn row_variables_of_an_append_only_table_are_computed_for_new_rows() {
    let (stepper, graph) = graph();
    let (parent, source) = fed(&graph, attributes(true, true, false), false);
    let make = operation(|t| update(t, &["K = k", "I = i"], &WithRowVariables).unwrap());
    let result = make(&parent);
    let viewed = view(&parent, &["K = k", "I = i"], &WithRowVariables).unwrap();
    let checker = checked(&result, &parent, &make);
    source.push(&[], &[2, 5], &[1, 2]);
    stepper.run_cycle();
    checker.assert_consistent();
    source.push(&[], &[9], &[3]);
    stepper.run_cycle();
    checker.assert_consistent();
    for table in [&result, &viewed] {
        assert_eq!(values(&**table, "K"), ["2", "5", "9"]);
        assert_eq!(values(&**table, "I"), ["0", "1", "2"]);
    }
}
