use super::{snapshot, snapshot_when, SnapshotError, SnapshotWhenOptions, StampColumn};
use crate::ops::merge::merge;
use crate::ops::sort::testing::{cells, frozen, graph, keys, keys_of, scripted, static_table};
use crate::ops::sort::{sort, SortColumn, SortDirection};
use arrow::array::{new_empty_array, Array, ArrayRef, Int32Array, StringArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{
    ints, strings, ConsistencyChecker, RandomUpdates, Recorder, ReferenceTable, ScriptedTable,
    TableChange, TableSnapshot,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn options(flags: &str, stamps: &[&str]) -> SnapshotWhenOptions {
    SnapshotWhenOptions {
        initial: flags.contains("initial"),
        incremental: flags.contains("incremental"),
        history: flags.contains("history"),
        stamp_columns: stamps
            .iter()
            .map(|stamp| StampColumn::parse(stamp).unwrap())
            .collect(),
    }
}

/// The rows of `table` in order, the cells of a row joined by `|`.
fn rows(table: &dyn Table) -> Vec<String> {
    let columns: Vec<Vec<String>> = (0..table.definition().columns().len())
        .map(|column| cells(table, column))
        .collect();
    (0..table.row_set().size() as usize)
        .map(|row| {
            let cells: Vec<&str> = columns.iter().map(|column| column[row].as_str()).collect();
            cells.join("|")
        })
        .collect()
}

fn base_columns(k: &[i32], v: &[&str]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("K", ColumnType::Int, ints(k.iter().copied())),
        ("V", ColumnType::String, strings(v.iter().copied())),
    ]
}

fn trigger_columns(n: &[i32], w: &[&str]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("N", ColumnType::Int, ints(n.iter().copied())),
        ("W", ColumnType::String, strings(w.iter().copied())),
    ]
}

/// A row for the base: `K` and `V`.
fn row(k: i32, v: &str) -> Vec<ArrayRef> {
    vec![ints([k]), strings([v])]
}

/// A scripted base, a scripted trigger and a snapshot of them, with every update of the
/// three recorded and the result checked after every cycle. With `recompute`, the result
/// is also compared with `model`, which the test keeps up to date.
struct Rig {
    graph: Arc<dyn UpdateGraph>,
    base: Arc<ScriptedTable>,
    trigger: Arc<ScriptedTable>,
    result: Arc<dyn Table>,
    recorders: [Arc<Recorder>; 3],
    checker: Arc<ConsistencyChecker>,
    model: Arc<Mutex<Arc<dyn Table>>>,
}

/// What one cycle delivered.
struct Cycle {
    base: Option<TableUpdate>,
    trigger: Option<TableUpdate>,
    result: Option<Result<TableUpdate, TableFailure>>,
}

impl Cycle {
    /// The result's update.
    fn update(&self) -> &TableUpdate {
        self.result.as_ref().expect("an update").as_ref().unwrap()
    }
}

impl Rig {
    fn new(
        base: (&[RowKey], &[(&str, ColumnType, ArrayRef)]),
        trigger: (&[RowKey], &[(&str, ColumnType, ArrayRef)]),
        options: &SnapshotWhenOptions,
        recompute: bool,
    ) -> Self {
        let graph = graph();
        let base = scripted(&graph, base.0.iter().copied(), base.1);
        let trigger = scripted(&graph, trigger.0.iter().copied(), trigger.1);
        let (base_table, trigger_table): (Arc<dyn Table>, Arc<dyn Table>) =
            (base.table(), trigger.table());
        let result = snapshot_when(&base_table, &trigger_table, options).unwrap();
        let recorders = [
            Recorder::attach(base_table),
            Recorder::attach(trigger_table),
            Recorder::attach(result.clone()),
        ];
        let model = Arc::new(Mutex::new(frozen(&*result)));
        let checker = if recompute {
            let model = model.clone();
            ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                model.lock().unwrap().clone()
            })
        } else {
            ConsistencyChecker::attach(result.clone())
        };
        checker.assert_consistent();
        Self {
            graph,
            base,
            trigger,
            result,
            recorders,
            checker,
            model,
        }
    }

    /// Runs one cycle with the given changes. The caller checks the result, with
    /// [`Rig::expect`] or [`Rig::consistent`].
    fn cycle(&self, base: Option<TableChange>, trigger: Option<TableChange>) -> Cycle {
        let before = self.recorders.each_ref().map(|r| r.events().len());
        if let Some(change) = base {
            self.base.push(change);
        }
        if let Some(change) = trigger {
            self.trigger.push(change);
        }
        self.graph.run_cycle();
        let mut events = [0, 1, 2].map(|index| {
            let mut events = self.recorders[index].events().split_off(before[index]);
            assert!(events.len() <= 1, "more than one update in a cycle");
            events.pop()
        });
        Cycle {
            base: events[0].take().map(|event| event.unwrap()),
            trigger: events[1].take().map(|event| event.unwrap()),
            result: events[2].take(),
        }
    }

    fn base(&self, change: TableChange) -> Cycle {
        let cycle = self.cycle(Some(change), None);
        self.consistent();
        cycle
    }

    fn trigger(&self, change: TableChange) -> Cycle {
        let cycle = self.cycle(None, Some(change));
        self.consistent();
        cycle
    }

    fn consistent(&self) {
        self.checker.assert_consistent();
    }

    /// Sets what the result must be now and checks it.
    fn expect(&self, model: Arc<dyn Table>) {
        *self.model.lock().unwrap() = model;
        self.checker.assert_consistent();
    }

    fn base_table(&self) -> Arc<dyn Table> {
        self.base.table()
    }

    fn trigger_table(&self) -> Arc<dyn Table> {
        self.trigger.table()
    }
}

fn tick(key: RowKey, n: i32, w: &str) -> TableChange {
    TableChange::new().add([key], vec![ints([n]), strings([w])])
}

fn three_rows() -> Vec<(&'static str, ColumnType, ArrayRef)> {
    base_columns(&[1, 2, 3], &["a", "b", "c"])
}

// ---------------------------------------------------------------------------------------
// snapshot

#[test]
fn snapshot_is_a_flat_static_copy() {
    let graph = graph();
    let source = scripted(&graph, [5, 9, 20], &three_rows());
    let table: Arc<dyn Table> = source.table();
    let copy = snapshot(&table).unwrap();
    assert!(!copy.is_refreshing() && copy.is_flat());
    assert_eq!(keys_of(&*copy), [0, 1, 2]);
    assert_eq!(rows(&*copy), ["1|a", "2|b", "3|c"]);

    source.push(TableChange::new().remove([5]).add([30], row(4, "d")));
    graph.run_cycle();
    assert_eq!(rows(&*copy), ["1|a", "2|b", "3|c"]);
    assert_eq!(rows(&*snapshot(&table).unwrap()), ["2|b", "3|c", "4|d"]);
}

#[test]
fn snapshot_of_a_failed_table_is_refused() {
    let graph = graph();
    let source = scripted(&graph, [0], &base_columns(&[1], &["a"]));
    let table: Arc<dyn Table> = source.table();
    source.push_failure("broken");
    graph.run_cycle();
    assert!(matches!(snapshot(&table), Err(SnapshotError::Table(_))));
    let trigger = static_table(&trigger_columns(&[1], &["x"]));
    for flags in ["initial", "initial incremental", "history"] {
        let refused = snapshot_when(&table, &trigger, &options(flags, &[]));
        assert!(matches!(refused, Err(SnapshotError::Table(_))), "{flags}");
    }
}

// ---------------------------------------------------------------------------------------
// snapshot_when without incremental or history

#[test]
fn a_trigger_replaces_every_row_and_the_base_alone_changes_nothing() {
    let rig = Rig::new(
        (&[5, 9, 20], &three_rows()),
        (&[], &trigger_columns(&[], &[])),
        &options("initial", &[]),
        false,
    );
    assert!(rig.result.is_flat() && rig.result.is_refreshing());
    assert_eq!(
        rows(&*rig.result),
        ["1|a|null|null", "2|b|null|null", "3|c|null|null"]
    );

    let cycle = rig.base(
        TableChange::new()
            .remove([9])
            .add([30, 31], vec![ints([4, 5]), strings(["d", "e"])]),
    );
    assert!(cycle.result.is_none());
    assert_eq!(rows(&*rig.result).len(), 3);

    let cycle = rig.trigger(tick(0, 7, "x"));
    let update = cycle.update();
    assert_eq!(
        (keys(&update.added), keys(&update.removed)),
        (vec![0, 1, 2, 3], vec![0, 1, 2])
    );
    assert!(update.modified.is_empty() && update.shifted.is_empty());
    assert_eq!(
        rows(&*rig.result),
        ["1|a|7|x", "3|c|7|x", "4|d|7|x", "5|e|7|x"]
    );

    // A smaller base: the result shrinks.
    rig.base(TableChange::new().remove([5, 20, 30]));
    let cycle = rig.trigger(tick(1, 8, "y"));
    assert_eq!(
        (keys(&cycle.update().added), keys(&cycle.update().removed)),
        (vec![0], vec![0, 1, 2, 3])
    );
    assert_eq!(rows(&*rig.result), ["5|e|8|y"]);
}

#[test]
fn without_initial_the_result_is_empty_until_the_first_trigger() {
    let rig = Rig::new(
        (&[0, 1, 2], &three_rows()),
        (&[4], &trigger_columns(&[7], &["x"])),
        &options("", &[]),
        false,
    );
    assert!(rows(&*rig.result).is_empty());
    rig.trigger(tick(9, 8, "y"));
    assert_eq!(rows(&*rig.result), ["1|a|8|y", "2|b|8|y", "3|c|8|y"]);
}

#[test]
fn base_and_trigger_of_one_cycle_give_the_base_after_its_change() {
    let rig = Rig::new(
        (&[0, 1, 2], &three_rows()),
        (&[4], &trigger_columns(&[7], &["x"])),
        &options("initial", &[]),
        false,
    );
    let change = TableChange::new().modify([1], [(1, strings(["B"]))]);
    rig.cycle(Some(change), Some(tick(9, 8, "y")));
    rig.consistent();
    assert_eq!(rows(&*rig.result), ["1|a|8|y", "2|B|8|y", "3|c|8|y"]);
}

#[test]
fn a_trigger_waits_for_a_base_that_delivers_later_in_the_cycle() {
    let graph = graph();
    let trigger = scripted(&graph, [4], &trigger_columns(&[7], &["x"]));
    let source = scripted(&graph, [0, 1, 2], &three_rows());
    let by = [SortColumn {
        column: "K".to_string(),
        direction: SortDirection::Descending,
        absolute: false,
    }];
    // Two operations between the source and the base: the trigger's update is there first.
    let sorted = sort(&(source.table() as Arc<dyn Table>), &by).unwrap();
    let base = sort(&sorted, &by).unwrap();
    let trigger_table: Arc<dyn Table> = trigger.table();
    let result = snapshot_when(&base, &trigger_table, &options("initial", &[])).unwrap();
    source.push(TableChange::new().add([7], row(4, "d")));
    trigger.push(tick(9, 8, "y"));
    graph.run_cycle();
    assert_eq!(rows(&*result), ["4|d|8|y", "3|c|8|y", "2|b|8|y", "1|a|8|y"]);
    let history = snapshot_when(&base, &trigger_table, &options("history", &[])).unwrap();
    source.push(TableChange::new().add([8], row(5, "e")));
    trigger.push(tick(10, 9, "z"));
    graph.run_cycle();
    assert_eq!(rows(&*history).len(), 2 * 4 + 5);
}

#[test]
fn the_stamp_is_the_triggers_last_row_or_null() {
    let rig = Rig::new(
        (&[0], &base_columns(&[1], &["a"])),
        (&[4, 6], &trigger_columns(&[7, 8], &["x", "y"])),
        &options("initial", &["Stamp=W"]),
        false,
    );
    assert_eq!(rows(&*rig.result), ["1|a|y"]);
    // A shift alone is a change of the trigger, and so is a removal.
    rig.trigger(TableChange::new().shift(6, 6, -1));
    assert_eq!(rows(&*rig.result), ["1|a|y"]);
    rig.trigger(TableChange::new().remove([5]));
    assert_eq!(rows(&*rig.result), ["1|a|x"]);
    let cycle = rig.trigger(TableChange::new().remove([4]));
    assert_eq!(rows(&*rig.result), ["1|a|null"]);
    assert_eq!(
        (keys(&cycle.update().added), keys(&cycle.update().removed)),
        (vec![0], vec![0])
    );
}

#[test]
fn a_modify_outside_the_named_stamp_columns_is_no_trigger() {
    let build = |stamps: &[&str]| {
        Rig::new(
            (&[0], &base_columns(&[1], &["a"])),
            (&[4], &trigger_columns(&[7], &["x"])),
            &options("initial", stamps),
            false,
        )
    };
    let change_base = || TableChange::new().modify([0], [(1, strings(["A"]))]);
    let modify_n = || TableChange::new().modify([4], [(0, ints([70]))]);

    let named = build(&["W"]);
    named.base(change_base());
    assert!(named.trigger(modify_n()).result.is_none());
    assert_eq!(rows(&*named.result), ["1|a|x"]);
    named.trigger(TableChange::new().modify([4], [(1, strings(["X"]))]));
    assert_eq!(rows(&*named.result), ["1|A|X"]);

    // Without named stamp columns the trigger itself is listened to.
    let all = build(&[]);
    all.base(change_base());
    all.trigger(modify_n());
    assert_eq!(rows(&*all.result), ["1|A|70|x"]);
}

#[test]
fn a_failed_trigger_fails_the_result_and_a_failed_base_does_not() {
    for flags in ["initial", "initial incremental", "history"] {
        let build = || {
            Rig::new(
                (&[0], &base_columns(&[1], &["a"])),
                (&[4], &trigger_columns(&[7], &["x"])),
                &options(flags, &[]),
                false,
            )
        };
        let rig = build();
        rig.trigger.push_failure("trigger broke");
        rig.graph.run_cycle();
        assert!(rig.result.is_failed(), "{flags}");
        let event = rig.recorders[2].events().pop().unwrap();
        assert_eq!(event.unwrap_err().message, "trigger broke");

        let rig = build();
        rig.base.push_failure("base broke");
        rig.graph.run_cycle();
        // Only the incremental result listens to the base.
        assert_eq!(
            rig.result.is_failed(),
            flags.contains("incremental"),
            "{flags}"
        );
    }
}

#[test]
fn a_static_trigger_gives_a_static_result() {
    let graph = graph();
    let base = scripted(&graph, [3, 8], &base_columns(&[1, 2], &["a", "b"]));
    let base_table: Arc<dyn Table> = base.table();
    let trigger = static_table(&trigger_columns(&[7, 8], &["x", "y"]));
    let taken = snapshot_when(&base_table, &trigger, &options("initial", &[])).unwrap();
    let empty = snapshot_when(&base_table, &trigger, &options("", &[])).unwrap();
    let incremental =
        snapshot_when(&base_table, &trigger, &options("initial incremental", &[])).unwrap();
    let never = snapshot_when(&base_table, &trigger, &options("incremental", &[])).unwrap();
    let history = snapshot_when(&base_table, &trigger, &options("history", &[])).unwrap();
    base.push(TableChange::new().remove([3]));
    graph.run_cycle();
    for table in [&taken, &empty, &incremental, &never, &history] {
        assert!(!table.is_refreshing());
    }
    assert_eq!(rows(&*taken), ["1|a|8|y", "2|b|8|y"]);
    assert!(rows(&*empty).is_empty() && rows(&*never).is_empty());
    assert!(taken.is_flat() && history.is_flat() && !incremental.is_flat());
    // The incremental result keeps the base's row keys.
    assert_eq!(keys_of(&*incremental), [3, 8]);
    assert_eq!(rows(&*incremental), ["1|a|8|y", "2|b|8|y"]);
    assert_eq!(
        rows(&*history),
        ["7|x|1|a", "7|x|2|b", "8|y|1|a", "8|y|2|b"]
    );
}

// ---------------------------------------------------------------------------------------
// incremental

fn incremental(flags: &str, stamps: &[&str]) -> Rig {
    Rig::new(
        (&[10, 20, 30], &three_rows()),
        (&[4], &trigger_columns(&[7], &["x"])),
        &options(flags, stamps),
        false,
    )
}

#[test]
fn incremental_copies_and_stamps_only_the_changed_rows() {
    let rig = incremental("initial incremental", &["W"]);
    assert!(!rig.result.is_flat());
    assert_eq!(keys_of(&*rig.result), [10, 20, 30]);
    assert_eq!(rows(&*rig.result), ["1|a|x", "2|b|x", "3|c|x"]);

    // The base alone changes nothing, however often.
    assert!(rig
        .base(TableChange::new().add([40], row(4, "d")))
        .result
        .is_none());
    assert!(rig
        .base(TableChange::new().modify([20], [(1, strings(["B"]))]))
        .result
        .is_none());
    assert!(rig.base(TableChange::new().remove([10])).result.is_none());
    assert_eq!(rows(&*rig.result), ["1|a|x", "2|b|x", "3|c|x"]);

    let cycle = rig.trigger(tick(5, 8, "y"));
    let update = cycle.update();
    assert_eq!(
        (
            keys(&update.added),
            keys(&update.removed),
            keys(&update.modified)
        ),
        (vec![40], vec![10], vec![20])
    );
    assert_eq!(rows(&*rig.result), ["2|B|y", "3|c|x", "4|d|y"]);

    // A trigger without a change of the base since the last one does nothing.
    assert!(rig.trigger(tick(6, 9, "z")).result.is_none());
    // A row added and removed again between two triggers never shows.
    rig.base(TableChange::new().add([50], row(5, "e")));
    rig.base(TableChange::new().remove([50]));
    assert!(rig.trigger(tick(7, 10, "zz")).result.is_none());
    assert_eq!(rows(&*rig.result), ["2|B|y", "3|c|x", "4|d|y"]);
}

#[test]
fn incremental_takes_base_and_trigger_of_one_cycle_together() {
    let rig = incremental("initial incremental", &["W"]);
    let change = TableChange::new().modify([30], [(1, strings(["C"]))]);
    let cycle = rig.cycle(Some(change), Some(tick(5, 8, "y")));
    rig.consistent();
    assert_eq!(keys(&cycle.update().modified), [30]);
    assert_eq!(rows(&*rig.result), ["1|a|x", "2|b|x", "3|C|y"]);
}

#[test]
fn incremental_without_initial_copies_the_whole_base_at_the_first_trigger() {
    let rig = incremental("incremental", &["W"]);
    assert!(rows(&*rig.result).is_empty());
    // Changes before the first snapshot are part of it and are not remembered after it.
    rig.base(TableChange::new().remove([10]).shift(20, 20, -5));
    let cycle = rig.trigger(tick(5, 8, "y"));
    assert_eq!(keys(&cycle.update().added), [15, 30]);
    assert_eq!(rows(&*rig.result), ["2|b|y", "3|c|y"]);
    assert!(rig.trigger(tick(6, 9, "z")).result.is_none());
    assert_eq!(rows(&*rig.result), ["2|b|y", "3|c|y"]);
}

#[test]
fn a_modify_outside_the_named_stamp_columns_is_no_incremental_trigger() {
    let rig = incremental("initial incremental", &["W"]);
    rig.base(TableChange::new().modify([10], [(1, strings(["A"]))]));
    assert!(rig
        .trigger(TableChange::new().modify([4], [(0, ints([70]))]))
        .result
        .is_none());
    assert_eq!(rows(&*rig.result), ["1|a|x", "2|b|x", "3|c|x"]);
    rig.trigger(TableChange::new().modify([4], [(1, strings(["X"]))]));
    assert_eq!(rows(&*rig.result), ["1|A|X", "2|b|x", "3|c|x"]);
}

/// Java compares the base with a remembered row set whenever its changes since the last
/// snapshot shift rows or remove and add one row key, and that row set is empty until
/// this has happened once.
#[test]
fn the_first_comparison_of_row_sets_stamps_every_row_and_removes_none() {
    let rig = incremental("initial incremental", &["W"]);
    rig.base(TableChange::new().remove([10]).shift(20, 20, 5));
    let cycle = rig.trigger(tick(5, 8, "y"));
    let update = cycle.update();
    // Row 10 stays although the base removed it; 25 is new; 30 is stamped again.
    assert_eq!(
        (
            keys(&update.added),
            keys(&update.removed),
            keys(&update.modified)
        ),
        (vec![25], vec![], vec![30])
    );
    assert_eq!(keys_of(&*rig.result), [10, 20, 25, 30]);
    assert_eq!(rows(&*rig.result), ["1|a|x", "2|b|x", "2|b|y", "3|c|y"]);

    // From the second comparison on, the remembered row set is the base at the last one.
    rig.base(TableChange::new().remove([30]).shift(25, 25, 10));
    let cycle = rig.trigger(tick(6, 9, "z"));
    let update = cycle.update();
    assert_eq!(
        (
            keys(&update.added),
            keys(&update.removed),
            keys(&update.modified)
        ),
        (vec![35], vec![25, 30], vec![])
    );
    assert_eq!(rows(&*rig.result), ["1|a|x", "2|b|x", "2|b|z"]);

    // A row that comes back at a key the result still holds is a modify there.
    rig.base(TableChange::new().add([20], row(9, "i")));
    let cycle = rig.trigger(tick(7, 10, "zz"));
    assert_eq!(
        (keys(&cycle.update().added), keys(&cycle.update().modified)),
        (vec![], vec![20])
    );
    assert_eq!(rows(&*rig.result), ["1|a|x", "9|i|zz", "2|b|z"]);
}

#[test]
fn a_shift_after_a_plain_snapshot_is_compared_with_the_base_of_that_snapshot() {
    let rig = incremental("initial incremental", &["W"]);
    // The first snapshot after the initial one has no shift: it remembers the base.
    rig.base(TableChange::new().add([40], row(4, "d")));
    rig.trigger(tick(5, 8, "y"));
    rig.base(
        TableChange::new()
            .remove([10])
            .shift(20, 20, 5)
            .modify([30], [(1, strings(["C"]))]),
    );
    let cycle = rig.trigger(tick(6, 9, "z"));
    let update = cycle.update();
    assert_eq!(
        (
            keys(&update.added),
            keys(&update.removed),
            keys(&update.modified)
        ),
        (vec![25], vec![10, 20], vec![30])
    );
    assert_eq!(rows(&*rig.result), ["2|b|z", "3|C|z", "4|d|y"]);
}

#[test]
fn a_row_removed_and_added_back_at_its_key_is_copied_again() {
    let rig = incremental("initial incremental", &["W"]);
    rig.base(TableChange::new().add([40], row(4, "d")));
    rig.trigger(tick(5, 8, "y"));
    rig.base(TableChange::new().remove([20]));
    rig.base(TableChange::new().add([20], row(9, "i")));
    let cycle = rig.trigger(tick(6, 9, "z"));
    assert_eq!(keys(&cycle.update().modified), [20]);
    assert!(cycle.update().added.is_empty() && cycle.update().removed.is_empty());
    assert_eq!(rows(&*rig.result), ["1|a|x", "9|i|z", "3|c|x", "4|d|y"]);
}

#[test]
fn an_empty_trigger_leaves_the_stamp_last_written_at_the_row_key() {
    let rig = incremental("initial incremental", &["W"]);
    rig.base(TableChange::new().remove([20]));
    rig.trigger(tick(5, 8, "y"));
    assert_eq!(rows(&*rig.result), ["1|a|x", "3|c|x"]);
    rig.base(
        TableChange::new()
            .add([20, 40], vec![ints([9, 4]), strings(["i", "d"])])
            .modify([10], [(1, strings(["A"]))]),
    );
    rig.trigger(TableChange::new().remove([4, 5]));
    // 10 keeps its stamp, 20 has the stamp of the row that was there, 40 has none.
    assert_eq!(rows(&*rig.result), ["1|A|x", "9|i|x", "3|c|x", "4|d|null"]);
}

#[test]
fn incremental_of_a_static_base_copies_it_once() {
    let graph = graph();
    let base = ReferenceTable::new_static(
        oxhaven_testkit::definition(&[("K", ColumnType::Int), ("V", ColumnType::String)]),
        RowSet::from_keys([3, 8]),
        vec![ints([1, 2]), strings(["a", "b"])],
    ) as Arc<dyn Table>;
    let trigger = scripted(&graph, [4], &trigger_columns(&[7], &["x"]));
    let trigger_table: Arc<dyn Table> = trigger.table();
    let result = snapshot_when(&base, &trigger_table, &options("incremental", &["W"])).unwrap();
    let initial = snapshot_when(
        &base,
        &trigger_table,
        &options("initial incremental", &["W"]),
    )
    .unwrap();
    let checker = ConsistencyChecker::attach(result.clone());
    let recorder = Recorder::attach(result.clone());
    assert!(result.is_refreshing() && !initial.is_refreshing());
    assert!(rows(&*result).is_empty());
    assert_eq!(rows(&*initial), ["1|a|x", "2|b|x"]);

    // A modify of another column does not count.
    trigger.push(TableChange::new().modify([4], [(0, ints([70]))]));
    graph.run_cycle();
    checker.assert_consistent();
    assert!(rows(&*result).is_empty());
    for (key, w) in [(5, "y"), (6, "z")] {
        trigger.push(tick(key, 8, w));
        graph.run_cycle();
        checker.assert_consistent();
        assert_eq!(keys_of(&*result), [3, 8]);
        assert_eq!(rows(&*result), ["1|a|y", "2|b|y"]);
    }
    assert_eq!(recorder.events().len(), 1);
    assert_eq!(rows(&*initial), ["1|a|x", "2|b|x"]);
}

// ---------------------------------------------------------------------------------------
// history

fn history(stamps: &[&str]) -> Rig {
    Rig::new(
        (&[10, 20], &base_columns(&[1, 2], &["a", "b"])),
        (&[4], &trigger_columns(&[7], &["x"])),
        &options("history", stamps),
        false,
    )
}

#[test]
fn history_appends_a_copy_of_the_base_for_every_trigger_row() {
    let rig = history(&[]);
    assert!(rig.result.is_flat());
    assert_eq!(rows(&*rig.result), ["7|x|1|a", "7|x|2|b"]);

    assert!(rig
        .base(TableChange::new().modify([10], [(1, strings(["A"]))]))
        .result
        .is_none());
    let two = TableChange::new().add([5, 9], vec![ints([8, 9]), strings(["y", "z"])]);
    let cycle = rig.trigger(two);
    let update = cycle.update();
    assert_eq!(keys(&update.added), [2, 3, 4, 5]);
    assert!(update.removed.is_empty() && update.modified.is_empty());
    assert_eq!(
        rows(&*rig.result),
        ["7|x|1|a", "7|x|2|b", "8|y|1|A", "8|y|2|b", "9|z|1|A", "9|z|2|b"]
    );

    // Nothing is appended for a trigger row while the base is empty.
    rig.base(TableChange::new().remove([10, 20]));
    assert!(rig.trigger(tick(11, 10, "zz")).result.is_none());
    rig.cycle(
        Some(TableChange::new().add([30], row(3, "c"))),
        Some(tick(12, 11, "last")),
    );
    rig.consistent();
    assert_eq!(rows(&*rig.result)[6..], ["11|last|3|c"]);
}

#[test]
fn history_fails_when_the_trigger_is_not_append_only() {
    let broken: [(&str, TableChange); 4] = [
        ("removed", TableChange::new().remove([4])),
        (
            "modified",
            TableChange::new().modify([4], [(0, ints([70]))]),
        ),
        ("shifted", TableChange::new().shift(4, 4, 1)),
        ("before", tick(2, 8, "y")),
    ];
    for (what, change) in broken {
        // The stamp column is not the modified one: Java checks the trigger itself.
        let rig = history(&["W"]);
        rig.trigger.push(change);
        rig.graph.run_cycle();
        assert!(rig.result.is_failed(), "{what}");
        let event = rig.recorders[2].events().pop().unwrap();
        assert!(event.unwrap_err().message.contains(what), "{what}");
    }
    // The same key order is fine while the trigger was empty.
    let rig = Rig::new(
        (&[10], &base_columns(&[1], &["a"])),
        (&[], &trigger_columns(&[], &[])),
        &options("history", &[]),
        false,
    );
    rig.trigger(tick(2, 8, "y"));
    assert_eq!(rows(&*rig.result), ["8|y|1|a"]);
}

#[test]
fn a_base_column_takes_the_place_of_a_stamp_column_of_its_name() {
    let base = static_table(&base_columns(&[1, 2], &["a", "b"]));
    let trigger = static_table(&[
        ("N", ColumnType::Int, ints([7])),
        ("V", ColumnType::String, strings(["x"])),
        ("K", ColumnType::Int, ints([70])),
    ]);
    let result = snapshot_when(&base, &trigger, &options("history", &[])).unwrap();
    let names: Vec<&str> = result
        .definition()
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["N", "V", "K"]);
    assert_eq!(rows(&*result), ["7|a|1", "7|b|2"]);

    // Of another type, the stamp must be one Java can write into the base column's store.
    let attempt = |stamp: (&str, ColumnType, ArrayRef), base: &Arc<dyn Table>| {
        let trigger = static_table(&[stamp]);
        snapshot_when(base, &trigger, &options("history", &[]))
    };
    let no_strings: Arc<StringArray> = Arc::new(StringArray::from(vec![None::<&str>]));
    let refused = attempt(("K", ColumnType::String, strings(["x"])), &base);
    assert!(matches!(refused, Err(SnapshotError::ColumnConflict(_))));
    let null = attempt(("K", ColumnType::String, no_strings), &base).unwrap();
    assert_eq!(rows(&*null), ["1|a", "2|b"]);
    let into_string = attempt(
        (
            "V",
            ColumnType::Boolean,
            Arc::new(arrow::array::BooleanArray::from(vec![true])),
        ),
        &base,
    )
    .unwrap();
    assert_eq!(rows(&*into_string), ["a|1", "b|2"]);
    let refused = attempt(("V", ColumnType::Int, ints([1])), &base);
    assert!(matches!(refused, Err(SnapshotError::ColumnConflict(_))));
    // Nothing is written, and so nothing refused, while either table is empty.
    let empty = static_table(&base_columns(&[], &[]));
    assert!(attempt(("V", ColumnType::Int, ints([1])), &empty).is_ok());
}

#[test]
fn a_stamp_that_does_not_fit_fails_a_ticking_history_when_it_arrives() {
    let rig = Rig::new(
        (&[10], &base_columns(&[1], &["a"])),
        (
            &[],
            &[(
                "K",
                ColumnType::String,
                Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            )],
        ),
        &options("history", &[]),
        false,
    );
    let null: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
    rig.trigger(TableChange::new().add([0], vec![null]));
    assert_eq!(rows(&*rig.result), ["1|a"]);
    rig.trigger
        .push(TableChange::new().add([1], vec![strings(["x"])]));
    rig.graph.run_cycle();
    assert!(rig.result.is_failed());
}

// ---------------------------------------------------------------------------------------
// refusals

#[test]
fn bad_requests_are_refused() {
    let base = static_table(&base_columns(&[1], &["a"]));
    let trigger = static_table(&trigger_columns(&[7], &["x"]));
    let attempt = |options: &SnapshotWhenOptions| snapshot_when(&base, &trigger, options).err();
    for flags in [
        "history initial",
        "history incremental",
        "history initial incremental",
    ] {
        assert!(
            matches!(
                attempt(&options(flags, &[])),
                Some(SnapshotError::Unsupported(_))
            ),
            "{flags}"
        );
    }
    for flags in [
        "",
        "initial",
        "incremental",
        "initial incremental",
        "history",
    ] {
        let missing = attempt(&options(flags, &["Q=Nope"]));
        assert!(
            matches!(missing, Some(SnapshotError::NoSuchColumn(_))),
            "{flags}"
        );
    }
    for flags in ["", "initial", "incremental", "initial incremental"] {
        let conflict = attempt(&options(flags, &["K=N"]));
        assert_eq!(
            conflict,
            Some(SnapshotError::ColumnConflict(
                "Column name conflicts: K".into()
            )),
            "{flags}"
        );
    }
    // A later stamp column reads an earlier one, and a repeated name keeps its first place.
    let chained = options("initial", &["N=W", "M=N", "N"]);
    let result = snapshot_when(&base, &trigger, &chained).unwrap();
    assert_eq!(rows(&*result), ["1|a|x|x"]);

    let other = scripted(&graph(), [0], &trigger_columns(&[7], &["x"]));
    let ticking = scripted(&graph(), [0], &base_columns(&[1], &["a"]));
    let (other, ticking): (Arc<dyn Table>, Arc<dyn Table>) = (other.table(), ticking.table());
    let two_graphs = snapshot_when(&ticking, &other, &options("initial", &[]));
    assert!(matches!(two_graphs, Err(SnapshotError::IllegalArgument(_))));
}

#[test]
fn stamp_column_strings_are_parsed_as_java_does() {
    assert_eq!(StampColumn::parse("A").unwrap(), StampColumn::new("A", "A"));
    assert_eq!(
        StampColumn::parse(" New = Old_1 ").unwrap(),
        StampColumn::new("New", "Old_1")
    );
    assert_eq!(
        StampColumn::parse("$a=_b").unwrap(),
        StampColumn::new("$a", "_b")
    );
    for bad in [
        "", "=A", "A=", "1A", "A B", "A=B=C", "A-B", "int", "A=null", "i", "in=A", "_",
    ] {
        assert!(
            matches!(StampColumn::parse(bad), Err(SnapshotError::InvalidName(_))),
            "{bad}"
        );
    }
}

// ---------------------------------------------------------------------------------------
// random updates against models

const BASE: [(&str, ColumnType); 3] = [
    ("K", ColumnType::Int),
    ("V", ColumnType::String),
    ("D", ColumnType::Double),
];
const TRIGGER: [(&str, ColumnType); 2] = [("N", ColumnType::Int), ("W", ColumnType::String)];

fn random_rig(random: &mut RandomUpdates, seed: u64, options: &SnapshotWhenOptions) -> Rig {
    let table = |random: &mut RandomUpdates, columns: &[(&'static str, ColumnType)], size: u64| {
        let keys: Vec<RowKey> = (0..size as RowKey).map(|k| 3 * k + 1).collect();
        let columns: Vec<(&str, ColumnType, ArrayRef)> = columns
            .iter()
            .map(|(name, t)| (*name, t.clone(), random.values(t, size)))
            .collect();
        (keys, columns)
    };
    // Some bases and some triggers start empty.
    let base = table(random, &BASE, if seed.is_multiple_of(4) { 0 } else { 6 });
    let trigger = table(random, &TRIGGER, if seed.is_multiple_of(3) { 0 } else { 2 });
    Rig::new((&base.0, &base.1), (&trigger.0, &trigger.1), options, true)
}

/// Whether to change the base and the trigger in this cycle: the base in about two cycles
/// of three, the trigger in about one of two.
fn choose(chooser: &mut RandomUpdates) -> (bool, bool) {
    let pick = chooser.values(&ColumnType::Long, 2);
    let pick = pick
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let value = |index: usize| {
        if pick.is_null(index) {
            0
        } else {
            pick.value(index).rem_euclid(6)
        }
    };
    (value(0) < 4, value(1) < 3)
}

/// Whether the table of stamp columns `stamps` (trigger column indexes, empty for the
/// trigger itself) delivers `update`.
fn delivered(update: &Option<TableUpdate>, stamps: &[usize]) -> bool {
    update.as_ref().is_some_and(|update| {
        stamps.is_empty()
            || !(update.added.is_empty() && update.removed.is_empty() && update.shifted.is_empty())
            || update.modified_columns.contains_any(stamps)
    })
}

/// The model of the plain form: the snapshot the static path takes of copies of both
/// tables, renewed at every delivered trigger.
#[test]
fn random_updates_match_a_fresh_static_snapshot_at_every_trigger() {
    for (stamps, watched) in [(&[][..], &[][..]), (&["W"][..], &[1usize][..])] {
        for initial in [false, true] {
            let options = options(if initial { "initial" } else { "" }, stamps);
            for seed in 0..40 {
                let mut random = RandomUpdates::new(seed);
                let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
                let rig = random_rig(&mut random, seed, &options);
                let fresh = |rig: &Rig| {
                    let mut options = options.clone();
                    options.initial = true;
                    snapshot_when(
                        &frozen(&*rig.base_table()),
                        &frozen(&*rig.trigger_table()),
                        &options,
                    )
                    .unwrap()
                };
                if initial {
                    rig.expect(fresh(&rig));
                }
                let mut triggers = 0;
                for _ in 0..25 {
                    let (base, trigger) = choose(&mut chooser);
                    let base = base.then(|| random.next_change(&*rig.base_table(), 4));
                    let trigger = trigger.then(|| random.next_change(&*rig.trigger_table(), 2));
                    let cycle = rig.cycle(base, trigger);
                    if delivered(&cycle.trigger, watched) {
                        triggers += 1;
                        rig.expect(fresh(&rig));
                    } else {
                        assert!(cycle.result.is_none(), "seed {seed}");
                        rig.consistent();
                    }
                }
                assert!(triggers > 3, "seed {seed}: {triggers} triggers");
            }
        }
    }
}

/// One cell of every column of `table` at `key`.
fn cells_at(table: &dyn Table, key: RowKey, columns: std::ops::Range<usize>) -> Vec<ArrayRef> {
    let row = RowSet::from_keys([key]);
    columns
        .map(|column| table.column(column).chunk(&row).unwrap())
        .collect()
}

/// The model of the incremental form. It follows every row of the base by an identity of
/// its own and a version that counts its modifies, and at a trigger copies the rows whose
/// row key holds another identity or version than at the snapshot before.
#[derive(Default)]
struct Incremental {
    next_identity: u64,
    /// The base now: row key to identity and version.
    now: BTreeMap<RowKey, (u64, u64)>,
    /// The base at the last snapshot.
    last: BTreeMap<RowKey, (u64, u64)>,
    /// Whether the first snapshot has been taken.
    started: bool,
    /// Whether the base changed since the last snapshot.
    changed: bool,
    /// Whether a snapshot after the first has been taken.
    compared: bool,
    /// The result: the copied cells of the base's columns per row key.
    held: BTreeMap<RowKey, Vec<ArrayRef>>,
    /// The stamp last written at each row key, also of rows that are gone.
    stamps: BTreeMap<RowKey, Vec<ArrayRef>>,
}

impl Incremental {
    fn start(rig: &Rig) -> Self {
        let mut model = Self::default();
        for key in rig.base_table().row_set().iter() {
            model.now.insert(key, (model.next_identity, 0));
            model.next_identity += 1;
        }
        model
    }

    fn base_changed(&mut self, update: &TableUpdate) {
        for key in update.removed.iter() {
            self.now.remove(&key);
        }
        let moved = std::mem::take(&mut self.now);
        self.now = moved
            .into_iter()
            .map(|(key, row)| (update.shifted.apply_to_key(key), row))
            .collect();
        for key in update.added.iter() {
            self.now.insert(key, (self.next_identity, 0));
            self.next_identity += 1;
        }
        for key in update.modified.iter() {
            self.now.get_mut(&key).unwrap().1 += 1;
        }
        self.changed = self.started;
    }

    fn copy(&mut self, rig: &Rig, key: RowKey, stamp_columns: &[usize]) {
        let base = rig.base_table();
        self.held.insert(key, cells_at(&*base, key, 0..BASE.len()));
        let trigger = rig.trigger_table();
        if let Some(last) = trigger.row_set().last_row_key() {
            let stamp = stamp_columns
                .iter()
                .flat_map(|&column| cells_at(&*trigger, last, column..column + 1))
                .collect();
            self.stamps.insert(key, stamp);
        }
    }

    fn triggered(&mut self, rig: &Rig, stamp_columns: &[usize]) {
        let keys: Vec<RowKey> = self.now.keys().copied().collect();
        if !self.started {
            self.started = true;
            keys.iter()
                .for_each(|&key| self.copy(rig, key, stamp_columns));
        } else if self.changed {
            // Java compares row sets when a row moved or a row key changed its row.
            let key_of: BTreeMap<u64, RowKey> =
                self.last.iter().map(|(key, row)| (row.0, *key)).collect();
            let moved = self.now.iter().any(|(key, row)| {
                key_of.get(&row.0).is_some_and(|before| before != key)
                    || self.last.get(key).is_some_and(|before| before.0 != row.0)
            });
            if moved && !self.compared {
                keys.iter()
                    .for_each(|&key| self.copy(rig, key, stamp_columns));
            } else {
                let gone: Vec<RowKey> = self
                    .last
                    .keys()
                    .filter(|key| !self.now.contains_key(key))
                    .copied()
                    .collect();
                gone.iter().for_each(|key| {
                    self.held.remove(key);
                });
                for key in keys {
                    if self.last.get(&key) != self.now.get(&key) {
                        self.copy(rig, key, stamp_columns);
                    }
                }
            }
            self.compared = true;
        } else {
            return;
        }
        self.last = self.now.clone();
        self.changed = false;
    }

    fn table(&self, result: &dyn Table) -> Arc<dyn Table> {
        let definition = result.definition().clone();
        let columns = definition.columns().iter().enumerate();
        let columns = columns
            .map(|(index, column)| {
                let empty = new_empty_array(&column.column_type.arrow_type());
                let null = arrow::array::new_null_array(&column.column_type.arrow_type(), 1);
                let cells: Vec<ArrayRef> = self
                    .held
                    .iter()
                    .map(|(key, data)| match index.checked_sub(BASE.len()) {
                        None => data[index].clone(),
                        Some(stamp) => self
                            .stamps
                            .get(key)
                            .map_or(null.clone(), |s| s[stamp].clone()),
                    })
                    .collect();
                let mut parts: Vec<&dyn arrow::array::Array> = vec![empty.as_ref()];
                parts.extend(cells.iter().map(|cell| cell.as_ref()));
                arrow::compute::concat(&parts).unwrap()
            })
            .collect();
        ReferenceTable::new_static(definition, self.held.keys().copied().collect(), columns)
    }
}

#[test]
fn random_updates_match_the_incremental_model() {
    for (stamps, stamp_columns) in [(&[][..], &[0usize, 1][..]), (&["W"][..], &[1usize][..])] {
        for initial in [false, true] {
            let flags = if initial {
                "initial incremental"
            } else {
                "incremental"
            };
            let options = options(flags, stamps);
            let watched = if stamps.is_empty() {
                &[][..]
            } else {
                stamp_columns
            };
            let (mut slow_first, mut snapshots) = (0, 0);
            for seed in 0..60 {
                let mut random = RandomUpdates::new(seed);
                let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
                let rig = random_rig(&mut random, seed, &options);
                // Half of the runs never shift the base, so that the first snapshot after
                // the initial one remembers the base and Java's first comparison is
                // skipped; the other half meets it.
                let mut base_random = RandomUpdates::new(seed ^ 0xba5e);
                base_random.shifts = seed % 2 == 0;
                let mut model = Incremental::start(&rig);
                if initial {
                    model.triggered(&rig, stamp_columns);
                }
                rig.expect(model.table(&*rig.result));
                for step in 0..30 {
                    let (base, trigger) = choose(&mut chooser);
                    let base = base.then(|| base_random.next_change(&*rig.base_table(), 3));
                    let trigger = trigger.then(|| random.next_change(&*rig.trigger_table(), 2));
                    let cycle = rig.cycle(base, trigger);
                    if let Some(update) = &cycle.base {
                        model.base_changed(update);
                    }
                    if delivered(&cycle.trigger, watched) {
                        let stale = model.held.len();
                        let compared = model.compared;
                        model.triggered(&rig, stamp_columns);
                        snapshots += 1;
                        if !compared && model.compared && model.held.len() > model.now.len() {
                            slow_first += 1;
                        }
                        let _ = stale;
                    } else {
                        assert!(cycle.result.is_none(), "seed {seed}, step {step}");
                    }
                    rig.expect(model.table(&*rig.result));
                    let held: Vec<RowKey> = model.held.keys().copied().collect();
                    assert_eq!(keys_of(&*rig.result), held, "seed {seed}, step {step}");
                }
            }
            // The runs took snapshots, and some kept rows that the base had deleted.
            assert!(
                snapshots > 300 && slow_first > 3,
                "{snapshots} snapshots, {slow_first} kept rows"
            );
        }
    }
}

/// The model of a history: the model so far, then the static history of a copy of the base
/// and a table of the added trigger rows.
#[test]
fn random_base_updates_and_appended_triggers_match_merged_static_histories() {
    for stamps in [&[][..], &["W"][..]] {
        let options = options("history", stamps);
        for seed in 0..40 {
            let mut random = RandomUpdates::new(seed);
            let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
            let rig = random_rig(&mut random, seed, &options);
            let mut model = frozen(&*rig.result);
            let mut next_key: RowKey = 100;
            for step in 0..25 {
                let (base, trigger) = choose(&mut chooser);
                let base = base.then(|| random.next_change(&*rig.base_table(), 4));
                let size = (step % 3) as u64;
                let added = trigger.then(|| {
                    let values: Vec<ArrayRef> = TRIGGER
                        .iter()
                        .map(|(_, t)| random.values(t, size))
                        .collect();
                    let keys: Vec<RowKey> = (0..size as RowKey).map(|k| next_key + 2 * k).collect();
                    next_key += 10;
                    (keys, values)
                });
                let change = added
                    .clone()
                    .map(|(keys, values)| TableChange::new().add(keys, values));
                rig.cycle(base, change);
                if let Some((keys, values)) = added.filter(|(keys, _)| !keys.is_empty()) {
                    let rows = RowSet::from_keys(keys);
                    let definition = rig.trigger_table().definition().clone();
                    let added: Arc<dyn Table> =
                        ReferenceTable::new_static(definition, rows, values);
                    let appended =
                        snapshot_when(&frozen(&*rig.base_table()), &added, &options).unwrap();
                    model = frozen(&*merge(&[model, appended]).unwrap());
                }
                rig.expect(model.clone());
            }
            assert!(
                TableSnapshot::current(&*rig.result).unwrap().rows.size() > 0 || seed % 4 == 0,
                "seed {seed}"
            );
        }
    }
}

#[test]
fn int_and_string_helpers_build_what_the_tests_expect() {
    // A guard for the helpers above: a row is its cells joined by `|`, nulls as `null`.
    let table = static_table(&[
        (
            "K",
            ColumnType::Int,
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        ),
        ("V", ColumnType::String, strings(["a", "b"])),
    ]);
    assert_eq!(rows(&*table), ["1|a", "null|b"]);
}
