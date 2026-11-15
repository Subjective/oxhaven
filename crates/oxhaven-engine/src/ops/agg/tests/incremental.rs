//! Aggregation of changing tables: each kind of change, the update it gives, and the
//! parents Java treats differently.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{append_only_input_table, tracked_source, QueryTable, RootSource, TableSpec};
use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, TableAttributes};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{ConsistencyChecker, Recorder, ScriptedTable, TableChange};
use std::sync::{Mutex, Weak};

pub(super) fn strs(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

pub(super) fn ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<Int32Array>())
}

pub(super) fn int(value: i32) -> ArrayRef {
    ints(&[Some(value)])
}

pub(super) fn doubles(values: &[Option<f64>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<Float64Array>())
}

/// An aggregation of a scripted table, with a checker and a recorder on the result.
pub(super) struct Fixture {
    pub(super) graph: Arc<SteppedUpdateGraph>,
    pub(super) source: Arc<ScriptedTable>,
    pub(super) result: Arc<dyn Table>,
    pub(super) checker: Arc<ConsistencyChecker>,
    pub(super) recorder: Arc<Recorder>,
}

impl Fixture {
    /// `recompute` says whether the result must equal, after every cycle, the aggregation
    /// of a static copy of the parent. It must not where the result is defined by its
    /// history: preserved empty groups, and sums that passed a null sentinel or rounded.
    pub(super) fn with(
        columns: &[(&str, ColumnType, ArrayRef)],
        row_keys: &[RowKey],
        aggregations: Vec<Aggregation>,
        by: &'static [&'static str],
        preserve_empty: bool,
        recompute: bool,
    ) -> Fixture {
        let graph = SteppedUpdateGraph::new();
        let (definition, batch) = batch(columns);
        let source = ScriptedTable::new(
            graph.clone(),
            definition,
            RowSet::from_keys(row_keys.iter().copied()),
            batch.columns().to_vec(),
        );
        let parent: Arc<dyn Table> = source.table();
        let result = aggregate(&parent, &aggregations, preserve_empty, by).expect("an aggregation");
        assert!(result.is_refreshing());
        let checker = if recompute {
            let result = result.clone();
            ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                let from_scratch =
                    aggregate(&static_copy(&*parent), &aggregations, preserve_empty, by)
                        .expect("a static aggregation");
                in_order_of(&*from_scratch, &*result, by.len())
            })
        } else {
            ConsistencyChecker::attach(result.clone())
        };
        checker.assert_consistent();
        let recorder = Recorder::attach(result.clone());
        Fixture {
            graph,
            source,
            result,
            checker,
            recorder,
        }
    }

    pub(super) fn new(
        columns: &[(&str, ColumnType, ArrayRef)],
        row_keys: &[RowKey],
        aggregations: Vec<Aggregation>,
        by: &'static [&'static str],
    ) -> Fixture {
        Fixture::with(columns, row_keys, aggregations, by, false, true)
    }

    /// Gives the parent `change` in one cycle. Returns the update the result delivered, as
    /// [`describe`] prints it, or `"none"`.
    pub(super) fn step(&self, change: TableChange) -> String {
        let before = self.recorder.events().len();
        self.source.push(change);
        self.graph.run_cycle();
        self.checker.assert_consistent();
        let events = self.recorder.events();
        match &events[before..] {
            [] => "none".to_string(),
            [Ok(update)] => describe(update, self.result.definition().columns().len()),
            other => panic!("unexpected events {other:?}"),
        }
    }

    pub(super) fn rows(&self) -> Vec<String> {
        rows(&*self.result)
    }
}

/// An update of an aggregation as text. Aggregations never shift.
fn describe(update: &TableUpdate, columns: usize) -> String {
    assert!(update.shifted.is_empty());
    let keys = |rows: &RowSet| rows.iter().collect::<Vec<_>>();
    let modified_columns: Vec<usize> = (0..columns)
        .filter(|&column| update.modified_columns.contains(column))
        .collect();
    format!(
        "added {:?} removed {:?} modified {:?} columns {:?}",
        keys(&update.added),
        keys(&update.removed),
        keys(&update.modified),
        modified_columns
    )
}

/// Columns `K`, `I` and `D` with the given rows.
pub(super) fn kid(rows: &[(&str, i32, f64)]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        (
            "K",
            ColumnType::String,
            strs(&rows.iter().map(|row| row.0).collect::<Vec<_>>()),
        ),
        (
            "I",
            ColumnType::Int,
            ints(&rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>()),
        ),
        (
            "D",
            ColumnType::Double,
            doubles(&rows.iter().map(|row| Some(row.2)).collect::<Vec<_>>()),
        ),
    ]
}

/// The values of rows to add to a `kid` table.
pub(super) fn kid_values(rows: &[(&str, i32, f64)]) -> Vec<ArrayRef> {
    kid(rows).into_iter().map(|(_, _, values)| values).collect()
}

pub(super) fn three_rows() -> Vec<(&'static str, ColumnType, ArrayRef)> {
    kid(&[("a", 1, 0.5), ("b", 2, 1.5), ("a", 3, 2.5)])
}

#[test]
fn rows_join_groups_new_groups_appear_and_empty_groups_leave_and_return() {
    let fixture = Fixture::new(
        &three_rows(),
        &[0, 1, 2],
        vec![agg(AggSpec::Sum, &["S = I"]), count("N")],
        &["K"],
    );
    assert_eq!(fixture.rows(), ["a|4|2", "b|2|1"]);
    assert!(!fixture.result.is_flat());

    // A row for an existing group modifies its row; a new key adds a row after the others.
    let change = TableChange::new().add([5, 6], kid_values(&[("a", 10, 0.0), ("c", 5, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [2] removed [] modified [0] columns [1, 2]"
    );
    assert_eq!(fixture.rows(), ["a|14|3", "b|2|1", "c|5|1"]);

    // A group that loses its last row loses its result row.
    assert_eq!(
        fixture.step(TableChange::new().remove([1])),
        "added [] removed [1] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["a|14|3", "c|5|1"]);

    // When it gets a row again it returns to its place, with a fresh state.
    let change = TableChange::new().add([1], kid_values(&[("b", 7, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [1] removed [] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["a|14|3", "b|7|1", "c|5|1"]);

    // Losing a row and getting one in the same cycle is a modification.
    let change = TableChange::new()
        .remove([6])
        .add([7], kid_values(&[("c", 9, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [2] columns [1, 2]"
    );
    assert_eq!(fixture.rows(), ["a|14|3", "b|7|1", "c|9|1"]);
}

#[test]
fn a_modified_value_marks_only_the_results_it_changes() {
    let fixture = Fixture::new(
        &three_rows(),
        &[0, 1, 2],
        vec![
            agg(AggSpec::Sum, &["S = I"]),
            count("N"),
            agg(AggSpec::Max, &["M = D"]),
        ],
        &["K"],
    );
    assert_eq!(fixture.rows(), ["a|4|2|2.5", "b|2|1|1.5"]);

    assert_eq!(
        fixture.step(TableChange::new().modify([0], [(1, int(6))])),
        "added [] removed [] modified [0] columns [1]"
    );
    assert_eq!(fixture.rows(), ["a|9|2|2.5", "b|2|1|1.5"]);

    // A value that is not the maximum changes to another that is not: nothing to report.
    let change = TableChange::new().modify([0], [(2, doubles(&[Some(0.75)]))]);
    assert_eq!(fixture.step(change), "none");

    let change = TableChange::new().modify([2], [(2, doubles(&[Some(9.5)]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [3]"
    );
    assert_eq!(fixture.rows(), ["a|9|2|9.5", "b|2|1|1.5"]);

    // The maximum itself drops: the group's rows are read again.
    let change = TableChange::new().modify([2], [(2, doubles(&[Some(0.25)]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [3]"
    );
    assert_eq!(fixture.rows(), ["a|9|2|0.75", "b|2|1|1.5"]);

    // A value rewritten with itself changes no sum.
    assert_eq!(
        fixture.step(TableChange::new().modify([1], [(1, int(2))])),
        "none"
    );
}

#[test]
fn a_changed_key_moves_the_row_between_groups() {
    let fixture = Fixture::new(
        &three_rows(),
        &[0, 1, 2],
        vec![agg(AggSpec::Sum, &["S = I"]), count("N")],
        &["K"],
    );
    let change = TableChange::new().modify([2], [(0, strs(&["b"]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0, 1] columns [1, 2]"
    );
    assert_eq!(fixture.rows(), ["a|1|1", "b|5|2"]);

    // A key rewritten with itself moves nothing.
    assert_eq!(
        fixture.step(TableChange::new().modify([1], [(0, strs(&["b"]))])),
        "none"
    );

    // The keys of re-keyed rows get their groups before the keys of added rows, whatever
    // their row keys; a's last row leaves it.
    let change = TableChange::new()
        .modify([0], [(0, strs(&["z"]))])
        .add([3, 9], kid_values(&[("w", 1, 0.0), ("y", 4, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [2, 3, 4] removed [0] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["b|5|2", "z|1|1", "w|1|1", "y|4|1"]);

    // Key and value change together, into a group that had emptied.
    let change = TableChange::new().modify([9], [(0, strs(&["a"])), (1, int(40))]);
    assert_eq!(
        fixture.step(change),
        "added [0] removed [4] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["a|40|1", "b|5|2", "z|1|1", "w|1|1"]);
}

#[test]
fn shifts_change_nothing_but_which_row_is_first_and_last() {
    let fixture = Fixture::new(
        &three_rows(),
        &[2, 5, 8],
        vec![
            agg(AggSpec::First, &["F = I"]),
            agg(AggSpec::Last, &["L = I"]),
            agg(AggSpec::Sum, &["S = I"]),
        ],
        &["K"],
    );
    assert_eq!(fixture.rows(), ["a|1|3|4", "b|2|2|2"]);

    assert_eq!(fixture.step(TableChange::new().shift(8, 8, -2)), "none");
    assert_eq!(fixture.rows(), ["a|1|3|4", "b|2|2|2"]);

    // The first row of a goes; its other row, now at 6, is first and last.
    assert_eq!(
        fixture.step(TableChange::new().remove([2])),
        "added [] removed [] modified [0] columns [1, 3]"
    );
    assert_eq!(fixture.rows(), ["a|3|3|3", "b|2|2|2"]);

    // b's row moves up and a new row of b lands below it: first changes, last does not.
    let change = TableChange::new()
        .shift(5, 5, 15)
        .add([10], kid_values(&[("b", 7, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [1] columns [1, 3]"
    );
    assert_eq!(fixture.rows(), ["a|3|3|3", "b|7|2|9"]);

    // A row shifted and modified in one cycle.
    let change = TableChange::new()
        .shift(6, 6, -6)
        .modify([0], [(1, int(-3))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [1, 2, 3]"
    );
    assert_eq!(fixture.rows(), ["a|-3|-3|-3", "b|7|2|9"]);
}

#[test]
fn a_shift_alone_gives_no_update_when_no_aggregation_looks_at_row_keys() {
    let fixture = Fixture::new(
        &three_rows(),
        &[2, 5, 8],
        vec![
            agg(AggSpec::Sum, &["S = I"]),
            agg(AggSpec::Avg, &["A = D"]),
            count("N"),
        ],
        &["K"],
    );
    assert_eq!(fixture.step(TableChange::new().shift(2, 8, 100)), "none");
    assert_eq!(fixture.rows(), ["a|4|1.5|2", "b|2|1.5|1"]);
    assert_eq!(fixture.checker.updates_seen(), 0);
}

#[test]
fn minimum_and_maximum_survive_the_removal_of_the_extreme() {
    let fixture = Fixture::new(
        &[(
            "D",
            ColumnType::Double,
            doubles(&[Some(1.5), Some(-2.0), Some(4.0)]),
        )],
        &[0, 1, 2],
        vec![
            agg(AggSpec::Min, &["Lo = D"]),
            agg(AggSpec::Max, &["Hi = D"]),
        ],
        &[],
    );
    assert_eq!(fixture.rows(), ["-2.0|4.0"]);
    assert!(fixture.result.is_flat());

    assert_eq!(
        fixture.step(TableChange::new().remove([1])),
        "added [] removed [] modified [0] columns [0]"
    );
    assert_eq!(fixture.rows(), ["1.5|4.0"]);

    // A NaN makes both NaN for as long as it is there.
    let change = TableChange::new().add([3], vec![doubles(&[Some(f64::NAN)])]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [0, 1]"
    );
    assert_eq!(fixture.rows(), ["NaN|NaN"]);
    let change = TableChange::new().add([4], vec![doubles(&[Some(-7.0)])]);
    assert_eq!(fixture.step(change), "none");
    assert_eq!(
        fixture.step(TableChange::new().remove([3])),
        "added [] removed [] modified [0] columns [0, 1]"
    );
    assert_eq!(fixture.rows(), ["-7.0|4.0"]);

    // A value that becomes null no longer counts.
    let change = TableChange::new().modify([2], [(0, doubles(&[None]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [1]"
    );
    assert_eq!(fixture.rows(), ["-7.0|1.5"]);

    assert_eq!(
        fixture.step(TableChange::new().remove([0, 2, 4])),
        "added [] removed [0] modified [] columns []"
    );
    assert_eq!(fixture.rows(), [""; 0]);
}

#[test]
fn without_keys_the_row_comes_and_goes_with_the_rows_of_the_table() {
    let empty = [("I", ColumnType::Int, ints(&[]))];
    let aggregations = || vec![agg(AggSpec::Sum, &["S = I"]), count("N")];
    let fixture = Fixture::new(&empty, &[], aggregations(), &[]);
    assert_eq!(fixture.rows(), [""; 0]);
    assert!(fixture.result.is_flat());

    assert_eq!(
        fixture.step(TableChange::new().add([4], vec![int(3)])),
        "added [0] removed [] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["3|1"]);
    assert_eq!(
        fixture.step(TableChange::new().modify([4], [(0, int(5))])),
        "added [] removed [] modified [0] columns [0]"
    );
    assert_eq!(
        fixture.step(TableChange::new().remove([4])),
        "added [] removed [0] modified [] columns []"
    );
    assert_eq!(fixture.rows(), [""; 0]);
    assert_eq!(
        fixture.step(TableChange::new().add([0, 1], vec![ints(&[Some(1), None])])),
        "added [0] removed [] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["1|2"]);

    // With empty groups preserved the row is always there.
    let fixture = Fixture::with(&empty, &[], aggregations(), &[], true, true);
    assert_eq!(fixture.rows(), ["null|0"]);
    assert_eq!(
        fixture.step(TableChange::new().add([4], vec![int(3)])),
        "added [] removed [] modified [0] columns [0, 1]"
    );
    assert_eq!(fixture.rows(), ["3|1"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([4])),
        "added [] removed [] modified [0] columns [0, 1]"
    );
    assert_eq!(fixture.rows(), ["null|0"]);
}

#[test]
fn preserved_empty_groups_keep_their_rows() {
    let fixture = Fixture::with(
        &kid(&[("a", 1, 0.5)]),
        &[0],
        all_of("I"),
        &["K"],
        true,
        false,
    );
    assert_eq!(fixture.rows(), ["a|1|1.0|1|1|1|1|1"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([0])),
        "added [] removed [] modified [0] columns [1, 2, 3, 4, 5, 6, 7]"
    );
    assert_eq!(fixture.rows(), ["a|null|null|null|null|null|null|0"]);
    let change = TableChange::new().add([3, 4], kid_values(&[("b", 5, 0.0), ("a", 2, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [1] removed [] modified [0] columns [1, 2, 3, 4, 5, 6, 7]"
    );
    assert_eq!(fixture.rows(), ["a|2|2.0|2|2|2|2|1", "b|5|5.0|5|5|5|5|1"]);
}

#[test]
fn an_update_that_changes_nothing_and_a_failed_parent() {
    let fixture = Fixture::new(&three_rows(), &[0, 1, 2], all_of("I"), &["K"]);
    assert_eq!(fixture.step(TableChange::new()), "none");
    // A column no aggregation reads.
    let change = TableChange::new().modify([0, 1], [(2, doubles(&[Some(8.0), Some(9.0)]))]);
    assert_eq!(fixture.step(change), "none");
    assert_eq!(fixture.checker.updates_seen(), 0);
    assert_eq!(fixture.rows(), ["a|4|2.0|1|3|1|3|2", "b|2|2.0|2|2|2|2|1"]);

    fixture.source.push_failure("the parent broke");
    fixture.graph.run_cycle();
    assert!(fixture.result.is_failed());
    let events = fixture.recorder.events();
    let [Err(failure)] = &events[..] else {
        panic!("unexpected events {events:?}");
    };
    assert!(failure.message.contains("the parent broke"), "{failure}");
}

#[test]
fn every_kind_of_change_in_one_cycle() {
    let fixture = Fixture::new(
        &kid(&[
            ("a", 1, 0.5),
            ("b", 2, 1.5),
            ("a", 3, 2.5),
            ("c", 4, 3.5),
            ("b", 5, 4.5),
        ]),
        &[1, 3, 5, 7, 9],
        all_of("I"),
        &["K"],
    );
    // Row 1 (a) goes; rows 5 and 7 move down to 4 and 6; the row now at 4 turns from a to
    // c with a new value, the row at 9 changes value; a row of d and a row of a arrive.
    let change = TableChange::new()
        .remove([1])
        .shift(5, 7, -1)
        .modify(
            [4, 9],
            [(0, strs(&["c", "b"])), (1, ints(&[Some(30), None]))],
        )
        .add([0, 8], kid_values(&[("d", 6, 0.0), ("a", 7, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [3] removed [] modified [0, 1, 2] columns [1, 2, 3, 4, 5, 6, 7]"
    );
    assert_eq!(
        fixture.rows(),
        [
            "a|7|7.0|7|7|7|7|1",
            "b|2|2.0|2|2|2|null|2",
            "c|34|17.0|4|30|30|4|2",
            "d|6|6.0|6|6|6|6|1",
        ]
    );
}

#[test]
fn sums_are_retracted_as_java_retracts_them() {
    // Long.MAX_VALUE + 1 wraps onto the null long. Taking the 1 away again adds -1 to
    // what is then read as null, as IntChunkedSumOperator#updateInternal does, so the sum
    // of the one remaining row is -1 and not Long.MAX_VALUE.
    let longs: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 1]));
    let fixture = Fixture::with(
        &[("L", ColumnType::Long, longs)],
        &[0, 1],
        vec![agg(AggSpec::Sum, &["L"])],
        &[],
        false,
        false,
    );
    assert_eq!(fixture.rows(), ["null"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([1])),
        "added [] removed [] modified [0] columns [0]"
    );
    assert_eq!(fixture.rows(), ["-1"]);

    // A floating point sum is a running sum: what rounding lost when a row was added is
    // not found again when the row leaves. The static sum of the remaining row is 0.5.
    let fixture = Fixture::with(
        &[("D", ColumnType::Double, doubles(&[Some(0.5), Some(1e300)]))],
        &[0, 1],
        vec![agg(AggSpec::Sum, &["S = D"]), agg(AggSpec::Avg, &["A = D"])],
        &[],
        false,
        false,
    );
    assert_eq!(fixture.rows(), ["1e300|5e299"]);
    fixture.step(TableChange::new().remove([1]));
    assert_eq!(fixture.rows(), ["0.0|0.0"]);

    // When the last value leaves, the running sum of a sum starts again from zero
    // (DoubleChunkedSumOperator#removeChunk) and that of an average keeps what rounding
    // left in it (DoubleChunkedAvgOperator#removeChunk).
    let fixture = Fixture::with(
        &[("D", ColumnType::Double, doubles(&[Some(0.1), Some(0.2)]))],
        &[0, 1],
        vec![agg(AggSpec::Sum, &["S = D"]), agg(AggSpec::Avg, &["A = D"])],
        &[],
        false,
        false,
    );
    fixture.step(TableChange::new().remove([0]));
    fixture.step(TableChange::new().remove([1]));
    assert_eq!(fixture.rows(), [""; 0]);
    fixture.step(TableChange::new().add([2], vec![doubles(&[Some(1e-17)])]));
    let left_over = (0.1 + 0.2) - 0.1 - 0.2;
    assert!(left_over != 0.0);
    assert_eq!(fixture.rows(), [format!("1e-17|{:?}", left_over + 1e-17)]);
}

#[test]
fn rows_join_a_group_in_java_order_within_a_cycle() {
    // A row whose key changed joins its new group before the cycle's added rows do, which
    // shows in a floating point sum: (1e300 + -1e300) + 0.5, not (1e300 + 0.5) + -1e300.
    let fixture = Fixture::with(
        &kid(&[("a", 0, 1e300), ("b", 0, -1e300)]),
        &[0, 1],
        vec![agg(AggSpec::Sum, &["S = D"])],
        &["K"],
        false,
        false,
    );
    let change = TableChange::new()
        .modify([1], [(0, strs(&["a"]))])
        .add([2], kid_values(&[("a", 0, 0.5)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [1] modified [0] columns [1]"
    );
    assert_eq!(fixture.rows(), ["a|0.5"]);
}

#[test]
fn an_add_only_parent_gives_a_flat_result() {
    let graph = SteppedUpdateGraph::new();
    let (definition, initial) = batch(&kid(&[("a", 5, 0.0), ("b", 1, 0.0)]));
    let parent: Arc<dyn Table> =
        append_only_input_table(graph.clone(), definition, Some(&initial)).expect("an input");
    let result = agg_by(&parent, &all_of("I"), &["K"]);
    assert!(result.is_refreshing() && result.is_flat());
    let checker = {
        let (parent, result) = (parent.clone(), result.clone());
        ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            let from_scratch = agg_by(&static_copy(&*parent), &all_of("I"), &["K"]);
            in_order_of(&*from_scratch, &*result, 1)
        })
    };
    assert_eq!(rows(&*result), ["a|5|5.0|5|5|5|5|1", "b|1|1.0|1|1|1|1|1"]);

    let writer = parent.input_table().expect("an input table");
    let (_, more) = batch(&[
        ("K", ColumnType::String, strs(&["a", "c", "b", "a"])),
        (
            "I",
            ColumnType::Int,
            ints(&[Some(2), Some(9), None, Some(7)]),
        ),
        ("D", ColumnType::Double, doubles(&[None; 4])),
    ]);
    writer.add(more).expect("an add");
    graph.run_cycle();
    checker.assert_consistent();
    assert_eq!(
        rows(&*result),
        [
            "a|14|4.666666666666667|2|7|5|7|3",
            "b|1|1.0|1|1|1|null|2",
            "c|9|9.0|9|9|9|9|1",
        ]
    );
    assert_eq!(checker.updates_seen(), 1);
}

/// A blink table of columns `K` and `I`: each cycle its rows are replaced by the next
/// queued rows, at row keys from zero.
pub(super) struct BlinkSource {
    table: Weak<QueryTable>,
    columns: [Arc<WritableSource>; 2],
    queued: Mutex<Vec<Vec<(&'static str, i32)>>>,
    size: Mutex<u64>,
}

impl UpdateSource for BlinkSource {
    fn refresh(&self) {
        let rows = {
            let mut queued = self.queued.lock().expect("the queue");
            if queued.is_empty() {
                Vec::new()
            } else {
                queued.remove(0)
            }
        };
        let mut size = self.size.lock().expect("the size");
        let removed = RowSet::flat(*size);
        let added = RowSet::flat(rows.len() as u64);
        if removed.is_empty() && added.is_empty() {
            return;
        }
        for column in &self.columns {
            column.remove(&removed);
        }
        let keys = strs(&rows.iter().map(|row| row.0).collect::<Vec<_>>());
        let values = ints(&rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>());
        self.columns[0].fill(&added, &keys).expect("a write");
        self.columns[1].fill(&added, &values).expect("a write");
        *size = added.size();
        if let Some(table) = self.table.upgrade() {
            table.notify(TableUpdate {
                added,
                removed,
                ..TableUpdate::empty()
            });
        }
    }
}

pub(super) fn blink_table(graph: &Arc<SteppedUpdateGraph>) -> (Arc<QueryTable>, Arc<BlinkSource>) {
    let dyn_graph: Arc<dyn UpdateGraph> = graph.clone();
    let columns = [
        tracked_source(&dyn_graph, ColumnType::String),
        tracked_source(&dyn_graph, ColumnType::Int),
    ];
    let definition =
        oxhaven_testkit::definition(&[("K", ColumnType::String), ("I", ColumnType::Int)]);
    let mut attributes = TableAttributes::default();
    attributes.blink = true;
    let sources: Vec<Arc<dyn ColumnSource>> = columns
        .iter()
        .map(|column| column.clone() as Arc<dyn ColumnSource>)
        .collect();
    let spec = TableSpec::new(definition, RowSet::empty(), sources).with_attributes(attributes);
    let mut made = None;
    let table = QueryTable::root(dyn_graph, spec, |table| {
        let source = Arc::new(BlinkSource {
            table,
            columns,
            queued: Mutex::default(),
            size: Mutex::new(0),
        });
        made = Some(source.clone());
        RootSource {
            source,
            input: None,
        }
    });
    (table, made.expect("the source"))
}

#[test]
fn a_blink_parent_is_aggregated_over_every_row_it_ever_had() {
    let graph = SteppedUpdateGraph::new();
    let (table, source) = blink_table(&graph);
    let parent: Arc<dyn Table> = table;
    let by_key = agg_by(&parent, &all_of("I"), &["K"]);
    let total = agg_by(&parent, &[count("N"), agg(AggSpec::Last, &["I"])], &[]);
    assert!(by_key.is_flat() && total.is_flat());
    let checkers = [
        ConsistencyChecker::attach(by_key.clone()),
        ConsistencyChecker::attach(total.clone()),
    ];
    let cycle = |rows: &[(&'static str, i32)]| {
        if !rows.is_empty() {
            source.queued.lock().expect("the queue").push(rows.to_vec());
        }
        graph.run_cycle();
        for checker in &checkers {
            checker.assert_consistent();
        }
    };
    assert_eq!(rows(&*total), [""; 0]);

    cycle(&[("a", 5), ("b", 1), ("b", 3)]);
    assert_eq!(rows(&*by_key), ["a|5|5.0|5|5|5|5|1", "b|4|2.0|1|3|1|3|2"]);
    assert_eq!(rows(&*total), ["3|3"]);

    // The rows of the last cycle are gone from the parent and stay aggregated. The new
    // rows reuse their row keys, so first and last go by arrival, not by row key.
    cycle(&[("a", 2), ("a", 9)]);
    assert_eq!(
        rows(&*by_key),
        ["a|16|5.333333333333333|2|9|5|9|3", "b|4|2.0|1|3|1|3|2"]
    );
    cycle(&[("b", 7)]);
    assert_eq!(
        rows(&*by_key),
        [
            "a|16|5.333333333333333|2|9|5|9|3",
            "b|11|3.6666666666666665|1|7|1|7|3"
        ]
    );
    assert_eq!(rows(&*total), ["6|7"]);

    // A cycle in which the parent only loses its rows changes nothing.
    let seen: Vec<u64> = checkers
        .iter()
        .map(|checker| checker.updates_seen())
        .collect();
    cycle(&[]);
    assert_eq!(parent.row_set().size(), 0);
    assert_eq!(rows(&*total), ["6|7"]);
    assert_eq!(
        checkers
            .iter()
            .map(|checker| checker.updates_seen())
            .collect::<Vec<_>>(),
        seen
    );
}
