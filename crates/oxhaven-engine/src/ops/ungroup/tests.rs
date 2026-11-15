//! `ungroup` of tables with vector and array columns.
//!
//! Most tests write an array as a count: [`counted`] makes the cell of the count `n` the
//! array of the `|n|` longs `100 * n`, `100 * n + 1` and so on, and of no count a null
//! cell, so that a result shows which row and which place an element came from.

use super::keys::MINIMUM_BASE;
use super::{ungroup, ungroup_with_base, UngroupError};
use crate::ops::sort::testing::{
    blink_table, cells, frozen, graph, keys, keys_of, scripted, shifts, static_table, Columns,
};
use crate::table::holds_values;
use crate::update::modified_pre_shift;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Int64Type};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    at_least_for, definition, ints, list_of, strings, test_seeds, ConsistencyChecker,
    RandomUpdates, Recorder, ScriptedTable, TableChange,
};
use std::sync::Arc;

/// The type of the column `A` of these tests, a vector as `group_by` makes one.
fn vector() -> ColumnType {
    ColumnType::vector(ColumnType::Long)
}

/// The type of the columns `B` and `C` of these tests, a Java array.
fn array() -> ColumnType {
    ColumnType::array(ColumnType::Long)
}

/// Cells of `column_type`, a vector or an array of longs, one for each count: for the
/// count `n` the `|n|` elements `100 * n`, `100 * n + 1` and so on, and for no count a
/// null cell.
fn counted(column_type: &ColumnType, counts: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
    let counts: Vec<Option<i32>> = counts.into_iter().collect();
    let elements = counts.iter().flatten().flat_map(|&n| {
        let size = i64::from(n.unsigned_abs());
        (0..size).map(move |offset| 100 * i64::from(n) + offset)
    });
    let elements: Int64Array = elements.collect::<Vec<i64>>().into();
    let lengths = counts.iter().map(|n| n.map(|n| n.unsigned_abs() as usize));
    list_of(column_type, lengths, Arc::new(elements))
}

/// Cells of the vector column `A`.
fn a<const N: usize>(counts: [i32; N]) -> ArrayRef {
    counted(&vector(), counts.map(Some))
}

/// Cells of the array column `B`.
fn b<const N: usize>(counts: [i32; N]) -> ArrayRef {
    counted(&array(), counts.map(Some))
}

/// `source` with the columns `names` ungrouped, with slots of at least
/// `1 << minimum_base` keys.
fn ungrouped(
    source: &Arc<dyn Table>,
    null_fill: bool,
    names: &[&str],
    minimum_base: u32,
) -> Result<Arc<dyn Table>, UngroupError> {
    let names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
    ungroup_with_base(source, null_fill, &names, minimum_base)
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

/// `K`, a string that is not ungrouped, and `A`, a vector column of the counts `a`.
fn one(k: &[&str], a: &[i32]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let counts = a.iter().copied().map(Some);
    vec![
        ("K", ColumnType::String, strings(k.iter().copied())),
        ("A", vector(), counted(&vector(), counts)),
    ]
}

/// `K`, the vector column `A` and the array column `B`.
fn two(k: &[&str], a: &[i32], b: &[Option<i32>]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let mut columns = one(k, a);
    columns.push(("B", array(), counted(&array(), b.iter().copied())));
    columns
}

const K: usize = 0;
const A: usize = 1;
const B: usize = 2;

/// A scripted source and its ungrouped table, with every update of the result recorded
/// and checked, also against the ungrouped table of a static copy of the source.
struct Rig {
    graph: Arc<dyn UpdateGraph>,
    source: Arc<ScriptedTable>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Rig {
    fn new(
        keys: &[RowKey],
        columns: Columns,
        null_fill: bool,
        names: &'static [&'static str],
        minimum_base: u32,
    ) -> Self {
        Self::build(keys, columns, null_fill, names, minimum_base, true)
    }

    fn build(
        keys: &[RowKey],
        columns: Columns,
        null_fill: bool,
        names: &'static [&'static str],
        minimum_base: u32,
        recompute: bool,
    ) -> Self {
        let graph = graph();
        let source = scripted(&graph, keys.iter().copied(), columns);
        let table: Arc<dyn Table> = source.table();
        let result = ungrouped(&table, null_fill, names, minimum_base).unwrap();
        let recorder = Recorder::attach(result.clone());
        let checker = if recompute {
            ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                ungrouped(&frozen(&*table), null_fill, names, minimum_base).unwrap()
            })
        } else {
            ConsistencyChecker::attach(result.clone())
        };
        checker.assert_consistent();
        Self {
            graph,
            source,
            result,
            recorder,
            checker,
        }
    }

    /// Applies `change` in one cycle and returns the result's update, if it delivered one.
    fn step(&self, change: TableChange) -> Option<TableUpdate> {
        let before = self.recorder.events().len();
        self.source.push(change);
        self.graph.run_cycle();
        self.checker.assert_consistent();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one update in a cycle");
        events.pop().map(|event| event.unwrap())
    }

    /// As [`Rig::step`], for a change that must show.
    fn changed(&self, change: TableChange) -> Shown {
        let update = self.step(change).expect("an update");
        Shown {
            added: keys(&update.added),
            removed: keys(&update.removed),
            modified: keys(&update.modified),
            shifted: shifts(&update),
            columns: update.modified_columns,
        }
    }

    /// Applies `change` in one cycle, in which the result must fail: the failure.
    fn fails(&self, change: TableChange) -> String {
        self.source.push(change);
        self.graph.run_cycle();
        assert!(self.result.is_failed());
        let event = self.recorder.events().pop().expect("a failure");
        event.unwrap_err().message
    }

    fn rows(&self) -> Vec<String> {
        rows(&*self.result)
    }
}

/// An update of the result.
#[derive(PartialEq, Debug)]
struct Shown {
    added: Vec<RowKey>,
    removed: Vec<RowKey>,
    modified: Vec<RowKey>,
    shifted: Vec<(RowKey, RowKey, i64)>,
    columns: ModifiedColumnSet,
}

impl Shown {
    fn of(added: &[RowKey], removed: &[RowKey], modified: &[RowKey]) -> Self {
        Self {
            added: added.to_vec(),
            removed: removed.to_vec(),
            modified: modified.to_vec(),
            shifted: Vec::new(),
            columns: ModifiedColumnSet::empty(),
        }
    }

    fn shifted(mut self, shifted: &[(RowKey, RowKey, i64)]) -> Self {
        self.shifted = shifted.to_vec();
        self
    }

    fn columns(mut self, columns: &[usize]) -> Self {
        self.columns = ModifiedColumnSet::of(columns.iter().copied());
        self
    }
}

// ---------------------------------------------------------------------------------------
// the operation as a client calls it

fn plain() -> Arc<dyn Table> {
    static_table(&[
        ("K", ColumnType::String, strings(["x", "y"])),
        ("A", ColumnType::Int, ints([1, 2])),
    ])
}

fn names(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

#[test]
fn a_table_without_arrays_is_returned_as_it_is() {
    let source = plain();
    for null_fill in [false, true] {
        let result = ungroup(&source, null_fill, &[]).unwrap();
        assert!(Arc::ptr_eq(&result, &source));
    }
}

#[test]
fn a_column_that_is_not_an_array_is_refused() {
    let error = ungroup(&plain(), false, &names(&["A"]))
        .err()
        .expect("a refusal");
    assert_eq!(error, UngroupError::NotAnArray("A".to_string()));
    assert_eq!(error.to_string(), "Column A is not an array or Vector");
}

#[test]
fn a_column_that_does_not_exist_is_refused() {
    let error = ungroup(&plain(), true, &names(&["Nope"]))
        .err()
        .expect("a refusal");
    assert_eq!(
        error.to_string(),
        "Unknown column names [Nope], available column names are [K, A]"
    );
}

/// Java looks at the names in turn and stops at the first it cannot ungroup.
#[test]
fn the_first_bad_column_is_the_error() {
    let error = ungroup(&plain(), false, &names(&["K", "Nope"]))
        .err()
        .expect("a refusal");
    assert_eq!(error, UngroupError::NotAnArray("K".to_string()));
    let error = ungroup(&plain(), false, &names(&["Nope", "K"]))
        .err()
        .expect("a refusal");
    assert!(matches!(error, UngroupError::NoSuchColumn { name, .. } if name == "Nope"));
}

// ---------------------------------------------------------------------------------------
// static sources

#[test]
fn a_row_for_every_element_in_the_order_of_rows_and_arrays() {
    let source = static_table(&[
        ("K", ColumnType::String, strings(["x", "y", "z", "w"])),
        (
            "A",
            vector(),
            counted(&vector(), [Some(2), None, Some(0), Some(-3)]),
        ),
    ]);
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    // A null array and an empty one have no rows.
    assert_eq!(
        rows(&*result),
        ["x|200", "x|201", "w|-300", "w|-299", "w|-298"]
    );
    // Row 3 of the source has the slot of 1024 keys from 3 << 10.
    assert_eq!(keys_of(&*result), [0, 1, 3072, 3073, 3074]);
    let columns = result.definition().columns();
    let types: Vec<(&str, &ColumnType)> = columns
        .iter()
        .map(|column| (column.name.as_str(), &column.column_type))
        .collect();
    assert_eq!(
        types,
        [("K", &ColumnType::String), ("A", &ColumnType::Long)]
    );
    // An array column gives the same.
    let array_source = static_table(&[
        ("K", ColumnType::String, strings(["x", "y", "z", "w"])),
        (
            "A",
            array(),
            counted(&array(), [Some(2), None, Some(0), Some(-3)]),
        ),
    ]);
    // The operation as a client calls it, with Java's slots.
    let of_array = ungroup(&array_source, false, &names(&["A"])).unwrap();
    assert_eq!(rows(&*of_array), rows(&*result));
    assert_eq!(keys_of(&*of_array), keys_of(&*result));
    assert!(!result.is_refreshing() && !result.is_flat());
    assert_eq!(result.attributes(), Default::default());
    // Neither kind of column holds its values.
    assert!(!holds_values(&result.column(K)) && !holds_values(&result.column(A)));
}

/// A null cell between others: it has no rows, and with null fill its elements are null.
/// (`sources.rs` has the null cell that spans elements, which no table of the testkit
/// answers with.)
#[test]
fn a_null_cell_has_no_elements() {
    let DataType::List(field) = vector().arrow_type() else {
        panic!("a vector is a list");
    };
    let offsets = OffsetBuffer::from_lengths([2, 3, 1]);
    let valid = NullBuffer::from(vec![true, false, true]);
    let elements = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6]));
    let cells: ArrayRef = Arc::new(ListArray::new(field, offsets, elements, Some(valid)));
    let source = static_table(&[
        ("K", ColumnType::String, strings(["x", "y", "z"])),
        ("A", vector(), cells),
        ("B", array(), b([1, 2, 1])),
    ]);
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    assert_eq!(rows(&*result), ["x|1|[100]", "x|2|[100]", "z|6|[100]"]);
    // Beside a longer array its elements are null.
    let result = ungrouped(&source, true, &["A", "B"], MINIMUM_BASE).unwrap();
    assert_eq!(
        rows(&*result),
        ["x|1|100", "x|2|null", "y|null|200", "y|null|201", "z|6|100"]
    );
}

#[test]
fn an_empty_table_and_a_table_of_empty_arrays_have_no_rows() {
    let source = static_table(&one(&[], &[]));
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    assert!(result.row_set().is_empty());
    let source = static_table(&one(&["x", "y"], &[0, 0]));
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    assert!(result.row_set().is_empty());
}

#[test]
fn the_slots_hold_the_largest_array() {
    let source = static_table(&one(&["x", "y"], &[1, 1024]));
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    assert_eq!(result.row_set().get(1), Some(1 << 10));
    let source = static_table(&one(&["x", "y"], &[1, 1025]));
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    assert_eq!(result.row_set().get(1), Some(1 << 11));
    assert_eq!(result.row_set().size(), 1026);
    // A smaller minimum, as Java's tests set it.
    let result = ungrouped(&source, false, &["A"], 1).unwrap();
    assert_eq!(result.row_set().get(1), Some(1 << 11));
    let source = static_table(&one(&["x", "y"], &[1, 1]));
    let result = ungrouped(&source, false, &["A"], 1).unwrap();
    assert_eq!(keys_of(&*result), [0, 2]);
}

#[test]
fn arrays_of_one_row_have_one_size() {
    let columns = two(&["x", "y", "z"], &[2, 1, 0], &[Some(-2), Some(1), None]);
    let source = static_table(&columns);
    let result = ungrouped(&source, false, &["A", "B"], MINIMUM_BASE).unwrap();
    assert_eq!(rows(&*result), ["x|200|-200", "x|201|-199", "y|100|100"]);
    // Only the columns asked for are compared, and the other stays what it is.
    let columns = two(&["x", "y"], &[2, 1], &[Some(5), Some(0)]);
    let source = static_table(&columns);
    let result = ungrouped(&source, false, &["A"], MINIMUM_BASE).unwrap();
    assert_eq!(
        rows(&*result),
        [
            "x|200|[500, 501, 502, 503, 504]",
            "x|201|[500, 501, 502, 503, 504]",
            "y|100|[]"
        ]
    );
    assert_eq!(result.definition().columns()[B].column_type, array());
}

#[test]
fn arrays_that_differ_in_size_are_refused() {
    let columns = two(&["x", "y", "z"], &[2, 1, 4], &[Some(2), Some(3), Some(1)]);
    let source = scripted(&graph(), [4, 7, 9], &columns).table() as Arc<dyn Table>;
    let error = ungrouped(&source, false, &["A", "B"], MINIMUM_BASE)
        .err()
        .expect("a refusal");
    assert_eq!(
        error,
        UngroupError::SizesDiffer {
            row_key: 7,
            previous: false,
            position: 1,
            reference: "A".to_string(),
            reference_size: 1,
            column: "B".to_string(),
            size: 3,
        }
    );
    assert_eq!(
        error.to_string(),
        "Array sizes differ at row key 7 (position 1), A has size 1, B has size 3"
    );
    // The first column asked for is the one the others are compared with.
    let error = ungrouped(&source, false, &["B", "A"], MINIMUM_BASE)
        .err()
        .expect("a refusal");
    assert_eq!(
        error.to_string(),
        "Array sizes differ at row key 7 (position 1), B has size 3, A has size 1"
    );
}

/// Java reads 1024 rows at a time, and of those one column after the other.
#[test]
fn the_difference_reported_is_the_first_of_the_first_chunk() {
    let table = |b_differs: usize, c_differs: usize| {
        let size = 3000;
        let sizes = |differs: usize| {
            let sizes = (0..size).map(move |row| if row == differs { 2 } else { 1 });
            counted(&array(), sizes.map(Some))
        };
        static_table(&[
            (
                "A",
                vector(),
                counted(&vector(), (0..size).map(|_| Some(1))),
            ),
            ("B", array(), sizes(b_differs)),
            ("C", array(), sizes(c_differs)),
        ])
    };
    let differs =
        |source: Arc<dyn Table>| match ungrouped(&source, false, &["A", "B", "C"], MINIMUM_BASE)
            .err()
            .expect("a refusal")
        {
            UngroupError::SizesDiffer {
                row_key, column, ..
            } => (row_key, column),
            other => panic!("{other}"),
        };
    // In one chunk, the first column wins.
    assert_eq!(differs(table(900, 10)), (900, "B".to_string()));
    // The first chunk wins over the first column.
    assert_eq!(differs(table(1024, 1023)), (1023, "C".to_string()));
    assert_eq!(differs(table(2999, 2047)), (2047, "C".to_string()));
    assert_eq!(differs(table(2048, 2050)), (2048, "B".to_string()));
}

#[test]
fn null_fill_gives_a_row_the_size_of_its_largest_array() {
    let columns = two(
        &["x", "y", "z", "w"],
        &[2, 1, 0, 0],
        &[Some(1), Some(3), None, Some(1)],
    );
    let source = static_table(&columns);
    let result = ungrouped(&source, true, &["A", "B"], MINIMUM_BASE).unwrap();
    assert_eq!(
        rows(&*result),
        [
            "x|200|100",
            "x|201|null",
            "y|100|300",
            "y|null|301",
            "y|null|302",
            "w|null|100"
        ]
    );
    // One column has nothing to fill.
    let result = ungrouped(&source, true, &["B"], MINIMUM_BASE).unwrap();
    assert_eq!(result.row_set().size(), 5);
}

#[test]
fn a_slot_beyond_the_row_keys_is_refused() {
    let last = RowKey::MAX >> 10;
    let source = scripted(&graph(), [0, last], &one(&["x", "y"], &[1, 0])).table();
    let source: Arc<dyn Table> = source;
    let error = ungrouped(&source, false, &["A"], MINIMUM_BASE)
        .err()
        .expect("a refusal");
    assert_eq!(
        error,
        UngroupError::KeyOverflow {
            last_row_key: last,
            base: 10
        }
    );
}

// ---------------------------------------------------------------------------------------
// refreshing sources, one change at a time. The slots have two keys unless a test makes
// them grow.

fn rig(keys: &[RowKey], k: &[&str], a: &[i32]) -> Rig {
    Rig::new(keys, &one(k, a), false, &["A"], 1)
}

fn row(k: &str, count: i32) -> Vec<ArrayRef> {
    vec![strings([k]), a([count])]
}

#[test]
fn an_added_row_brings_its_elements() {
    let rig = rig(&[0, 2], &["x", "z"], &[1, 2]);
    assert!(rig.result.is_refreshing());
    assert_eq!(keys_of(&*rig.result), [0, 4, 5]);
    let shown = rig.changed(TableChange::new().add([1], row("y", 2)));
    assert_eq!(shown, Shown::of(&[2, 3], &[], &[]));
    assert_eq!(rig.rows(), ["x|100", "y|200", "y|201", "z|200", "z|201"]);
    // A row without elements shows nothing.
    assert_eq!(rig.step(TableChange::new().add([7], row("w", 0))), None);
}

#[test]
fn a_removed_row_takes_its_elements() {
    let rig = rig(&[0, 2, 3], &["x", "z", "w"], &[1, 2, 0]);
    let shown = rig.changed(TableChange::new().remove([2]));
    assert_eq!(shown, Shown::of(&[], &[4, 5], &[]));
    assert_eq!(rig.rows(), ["x|100"]);
    assert_eq!(rig.step(TableChange::new().remove([3])), None);
}

#[test]
fn a_modified_row_modifies_its_elements_whatever_column_changed() {
    let rig = rig(&[0, 2, 3], &["x", "z", "w"], &[1, 2, 0]);
    let change = TableChange::new().modify([2], [(K, strings(["q"]))]);
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[], &[], &[4, 5]).columns(&[K]));
    assert_eq!(rig.rows(), ["x|100", "q|200", "q|201"]);
    // A row without elements has nothing to modify.
    let change = TableChange::new().modify([3], [(K, strings(["r"]))]);
    assert_eq!(rig.step(change), None);
}

#[test]
fn a_modified_array_keeps_adds_and_removes_elements() {
    let rig = rig(&[0, 1, 2, 3], &["x", "y", "z", "w"], &[2, 2, 1, 0]);
    assert_eq!(keys_of(&*rig.result), [0, 1, 2, 3, 4]);
    // Row 0 keeps its size, row 1 shrinks, row 2 grows, row 3 gets its first element.
    let change = TableChange::new().modify([0, 1, 2, 3], [(A, a([-2, 1, 2, 1]))]);
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[5, 6], &[3], &[0, 1, 2, 4]).columns(&[A]));
    assert_eq!(
        rig.rows(),
        ["x|-200", "x|-199", "y|100", "z|200", "z|201", "w|100"]
    );
    // And loses its last.
    let change = TableChange::new().modify([1, 2], [(A, a([0, 2]))]);
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[], &[2], &[4, 5]).columns(&[A]));
}

#[test]
fn a_shifted_row_moves_its_elements() {
    let rig = rig(&[0, 2, 3], &["x", "z", "w"], &[2, 1, 0]);
    let shown = rig.changed(TableChange::new().shift(2, 3, 3));
    assert_eq!(shown, Shown::of(&[], &[], &[]).shifted(&[(4, 4, 6)]));
    assert_eq!(keys_of(&*rig.result), [0, 1, 10]);
    // With a change of the row that moved, which is named by its new key, and of
    // another row.
    let change = TableChange::new()
        .remove([0])
        .shift(5, 6, -4)
        .add([0], row("v", 1))
        .modify([1, 2], [(A, a([2, 2]))]);
    let shown = rig.changed(change);
    let expected = Shown::of(&[0, 3, 4, 5], &[0, 1], &[2])
        .shifted(&[(10, 10, -8)])
        .columns(&[A]);
    assert_eq!(shown, expected);
    assert_eq!(rig.rows(), ["v|100", "z|200", "z|201", "w|200", "w|201"]);
}

#[test]
fn a_shift_of_rows_without_elements_moves_nothing() {
    let rig = rig(&[0, 2], &["x", "z"], &[0, 0]);
    assert_eq!(rig.step(TableChange::new().shift(2, 2, 3)), None);
    let rig = rig_of_two_empty_and_one();
    assert_eq!(rig.step(TableChange::new().shift(2, 2, 1)), None);
}

fn rig_of_two_empty_and_one() -> Rig {
    rig(&[0, 2, 9], &["x", "z", "w"], &[0, 0, 1])
}

#[test]
fn an_added_array_that_does_not_fit_moves_every_slot() {
    let rig = rig(&[0, 1, 3], &["x", "y", "w"], &[2, 2, 1]);
    assert_eq!(keys_of(&*rig.result), [0, 1, 2, 3, 6]);
    // Five elements need slots of eight keys.
    let shown = rig.changed(TableChange::new().add([2], row("z", 5)));
    let expected = Shown::of(&[16, 17, 18, 19, 20], &[], &[]).shifted(&[(2, 3, 6), (6, 6, 18)]);
    assert_eq!(shown, expected);
    assert_eq!(keys_of(&*rig.result), [0, 1, 8, 9, 16, 17, 18, 19, 20, 24]);
    // The slots stay as large when the array has gone.
    let shown = rig.changed(TableChange::new().remove([2]).add([4], row("v", 1)));
    assert_eq!(shown, Shown::of(&[32], &[16, 17, 18, 19, 20], &[]));
}

#[test]
fn a_modified_array_that_does_not_fit_moves_every_slot() {
    let rig = rig(&[0, 1], &["x", "y"], &[1, 2]);
    let shown = rig.changed(TableChange::new().modify([0], [(A, a([3]))]));
    let expected = Shown::of(&[1, 2], &[], &[0])
        .shifted(&[(2, 3, 2)])
        .columns(&[A]);
    assert_eq!(shown, expected);
    assert_eq!(rig.rows(), ["x|300", "x|301", "x|302", "y|200", "y|201"]);
}

#[test]
fn every_change_at_once_with_slots_that_grow() {
    let rig = rig(&[0, 1, 2, 4], &["x", "y", "z", "w"], &[2, 1, 2, 2]);
    assert_eq!(keys_of(&*rig.result), [0, 1, 2, 4, 5, 8, 9]);
    let change = TableChange::new()
        .remove([0])
        .shift(4, 4, -1)
        .add([6], row("v", 1))
        .modify([1, 3], [(K, strings(["q", "r"])), (A, a([3, -2]))]);
    let shown = rig.changed(change);
    // The removed keys and the old places of the shifts are of slots of two keys, the
    // rest of slots of four.
    let expected = Shown::of(&[5, 6, 24], &[0, 1], &[4, 12, 13])
        .shifted(&[(2, 2, 2), (4, 5, 4), (8, 9, 4)])
        .columns(&[K, A]);
    assert_eq!(shown, expected);
    assert_eq!(
        rig.rows(),
        ["q|300", "q|301", "q|302", "z|200", "z|201", "r|-200", "r|-199", "v|100"]
    );
}

#[test]
fn a_source_that_removes_every_row_starts_again_with_small_slots() {
    let rig = rig(&[0, 1], &["x", "y"], &[3, 1]);
    assert_eq!(keys_of(&*rig.result), [0, 1, 2, 4]);
    let change = TableChange::new().remove([0, 1]).add([5], row("z", 1));
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[10], &[0, 1, 2, 4], &[]));
    let shown = rig.changed(TableChange::new().add([6], row("w", 2)));
    assert_eq!(shown, Shown::of(&[12, 13], &[], &[]));
    // To nothing, and from nothing.
    let shown = rig.changed(TableChange::new().remove([5, 6]));
    assert_eq!(shown, Shown::of(&[], &[10, 12, 13], &[]));
    let shown = rig.changed(TableChange::new().add([1], row("v", 4)));
    assert_eq!(shown, Shown::of(&[4, 5, 6, 7], &[], &[]));
}

/// Without that rule a source that keeps one row would keep slots of four keys.
#[test]
fn a_source_that_keeps_a_row_keeps_its_slots() {
    let rig = rig(&[0, 1], &["x", "y"], &[3, 1]);
    let change = TableChange::new().remove([0]).add([5], row("z", 1));
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[20], &[0, 1, 2], &[]));
}

#[test]
fn a_change_of_the_source_that_does_not_show() {
    let rig = rig(&[0, 2], &["x", "z"], &[0, 1]);
    let change = TableChange::new().modify([0], [(K, strings(["q"]))]);
    assert_eq!(rig.step(change), None);
    assert_eq!(rig.step(TableChange::new()), None);
    assert_eq!(rig.rows(), ["z|100"]);
}

#[test]
fn a_failure_of_the_source_fails_the_result() {
    let rig = rig(&[0], &["x"], &[1]);
    rig.source.push_failure("the source broke");
    rig.graph.run_cycle();
    assert!(rig.result.is_failed());
    let event = rig.recorder.events().pop().unwrap();
    assert_eq!(event.unwrap_err().message, "the source broke");
}

#[test]
fn an_added_row_whose_slot_is_beyond_the_row_keys_fails_the_result() {
    let rig = rig(&[0], &["x"], &[1]);
    let last = RowKey::MAX >> 1;
    // Whatever its size is.
    let failure = rig.fails(TableChange::new().add([last], row("y", 0)));
    assert_eq!(
        failure,
        "Key overflow detected, perhaps you should flatten your table before calling \
         ungroup: lastRowKey=4611686018427387903, base=1"
    );
}

/// Java checks an added row with the slots that hold the added rows, before it looks at
/// the modified rows, which here make the slots grow.
#[test]
fn an_added_row_is_checked_before_modified_rows_grow_the_slots() {
    let rig = rig(&[0], &["x"], &[1]);
    let last = RowKey::MAX >> 1;
    let change = TableChange::new()
        .add([last], row("y", 0))
        .modify([0], [(A, a([3]))]);
    assert_eq!(
        rig.fails(change),
        "Key overflow detected, perhaps you should flatten your table before calling \
         ungroup: lastRowKey=4611686018427387903, base=1"
    );
}

/// Java looks at the whole slot of a row when the row is added and when it is removed,
/// and not when the row moves: a row can move into the last slot, and get elements
/// there, but it cannot leave.
#[test]
fn a_row_can_move_into_the_last_slot_but_not_be_removed_from_it() {
    let rig = rig(&[0, 5], &["x", "y"], &[1, 0]);
    let last = RowKey::MAX >> 1;
    assert_eq!(rig.step(TableChange::new().shift(5, 5, last - 5)), None);
    let shown = rig.changed(TableChange::new().modify([last], [(A, a([1]))]));
    // No element was modified, so no column is reported as modified.
    assert_eq!(shown, Shown::of(&[RowKey::MAX - 1], &[], &[]));
    assert_eq!(rig.rows(), ["x|100", "y|100"]);
    let failure = rig.fails(TableChange::new().remove([last]));
    assert_eq!(
        failure,
        "Key overflow detected, perhaps you should flatten your table before calling \
         ungroup: lastRowKey=4611686018427387903, base=1"
    );
}

// ---------------------------------------------------------------------------------------
// refreshing sources with two array columns

fn pair(keys: &[RowKey], k: &[&str], a: &[i32], b: &[Option<i32>], null_fill: bool) -> Rig {
    Rig::new(keys, &two(k, a, b), null_fill, &["A", "B"], 1)
}

fn row_of_two(k: &str, count: i32, other: i32) -> Vec<ArrayRef> {
    vec![strings([k]), a([count]), b([other])]
}

#[test]
fn an_array_modified_alone_must_keep_its_size() {
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let shown = rig.changed(TableChange::new().modify([4], [(B, b([2]))]));
    assert_eq!(shown, Shown::of(&[], &[], &[8, 9]).columns(&[B]));
    assert_eq!(rig.rows(), ["x|200|200", "x|201|201", "y|100|-100"]);
    // The column that was not modified is the one it is compared with, whichever
    // comes first.
    let failure = rig.fails(TableChange::new().modify([7], [(B, b([2]))]));
    assert_eq!(
        failure,
        "Array sizes differ at row key 7 (position 1), A has size 1, B has size 2"
    );
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let failure = rig.fails(TableChange::new().modify([7], [(A, a([0]))]));
    assert_eq!(
        failure,
        "Array sizes differ at row key 7 (position 1), B has size 1, A has size 0"
    );
}

#[test]
fn arrays_modified_together_change_their_size_together() {
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let change = TableChange::new().modify([7], [(A, a([2])), (B, b([2]))]);
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[15], &[], &[14]).columns(&[A, B]));
    let change = TableChange::new().modify([7], [(A, a([2])), (B, b([1]))]);
    assert_eq!(
        rig.fails(change),
        "Array sizes differ at row key 7 (position 1), A has size 2, B has size 1"
    );
}

#[test]
fn an_added_row_whose_arrays_differ_fails_the_result() {
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let failure = rig.fails(TableChange::new().add([5], row_of_two("z", 1, 2)));
    assert_eq!(
        failure,
        "Array sizes differ at row key 5 (position 1), A has size 1, B has size 2"
    );
}

/// What Java does, not what it should: in a cycle whose added rows make the slots grow,
/// an array that was modified alone is not compared with the others, and the row keeps
/// the number of elements it had.
#[test]
fn an_array_modified_alone_goes_unchecked_when_added_rows_grow_the_slots() {
    let columns = two(&["x", "y"], &[2, 1], &[Some(-2), Some(-1)]);
    // Not compared with the static result, which refuses these arrays.
    let rig = Rig::build(&[0, 1], &columns, false, &["A", "B"], 1, false);
    let change = TableChange::new()
        .add([2], row_of_two("z", 3, 3))
        .modify([1], [(B, b([2]))]);
    let shown = rig.changed(change);
    let expected = Shown::of(&[8, 9, 10], &[], &[4])
        .shifted(&[(2, 2, 2)])
        .columns(&[B]);
    assert_eq!(shown, expected);
    assert_eq!(
        rig.rows(),
        [
            "x|200|-200",
            "x|201|-199",
            "y|100|200",
            "z|300|300",
            "z|301|301",
            "z|302|302"
        ]
    );
}

#[test]
fn null_fill_reads_every_array_when_one_is_modified() {
    let rig = pair(&[0, 1], &["x", "y"], &[2, 1], &[Some(1), None], true);
    assert_eq!(rig.rows(), ["x|200|100", "x|201|null", "y|100|null"]);
    // The array of `B` outgrows the one of `A`, and the slots.
    let shown = rig.changed(TableChange::new().modify([1], [(B, b([3]))]));
    let expected = Shown::of(&[5, 6], &[], &[4])
        .shifted(&[(2, 2, 2)])
        .columns(&[B]);
    assert_eq!(shown, expected);
    assert_eq!(
        rig.rows(),
        [
            "x|200|100",
            "x|201|null",
            "y|100|300",
            "y|null|301",
            "y|null|302"
        ]
    );
    // It shrinks below the one of `A`, which was not modified.
    let shown = rig.changed(TableChange::new().modify([1], [(B, b([0]))]));
    assert_eq!(shown, Shown::of(&[], &[5, 6], &[4]).columns(&[B]));
    assert_eq!(rig.rows(), ["x|200|100", "x|201|null", "y|100|null"]);
}

// ---------------------------------------------------------------------------------------
// blink sources

#[test]
fn a_blink_table_starts_again_every_cycle() {
    let graph = graph();
    let columns = [("K", ColumnType::String), ("A", vector())];
    let (source, blink) = blink_table(&graph, &columns);
    let result = ungrouped(&source, false, &["A"], 1).unwrap();
    assert!(result.attributes().blink);
    let recorder = Recorder::attach(result.clone());
    let checker = ConsistencyChecker::attach(result.clone());
    blink.push(vec![strings(["x", "y"]), a([5, 1])]);
    blink.push(vec![strings(["z"]), a([2])]);
    let mut seen = Vec::new();
    for _ in 0..3 {
        graph.run_cycle();
        checker.assert_consistent();
        seen.push((keys_of(&*result), rows(&*result)));
    }
    let text = |rows: &[&str]| rows.iter().map(|row| row.to_string()).collect::<Vec<_>>();
    // Slots of eight keys for the first rows, and of two for the next: the source's row
    // keys go on from 2.
    assert_eq!(
        seen,
        [
            (
                vec![0, 1, 2, 3, 4, 8],
                text(&["x|500", "x|501", "x|502", "x|503", "x|504", "y|100"])
            ),
            (vec![4, 5], text(&["z|200", "z|201"])),
            (vec![], vec![]),
        ]
    );
    assert_eq!(recorder.events().len(), 3);
}

// ---------------------------------------------------------------------------------------
// random changes

/// Cells of `column_type` with the sizes of the cells of `other`: the longs of `other`,
/// negated, a null cell where `other` has one.
fn sized_as(other: &ArrayRef, column_type: &ColumnType) -> ArrayRef {
    let lists = other.as_any().downcast_ref::<ListArray>().expect("lists");
    let lengths = (0..lists.len()).map(|row| lists.is_valid(row).then(|| lists.value(row).len()));
    let lengths: Vec<Option<usize>> = lengths.collect();
    let cells = (0..lists.len()).filter(|row| lists.is_valid(*row));
    let elements = cells.flat_map(|row| {
        let cell = lists.value(row);
        let cell = arrow::array::AsArray::as_primitive::<Int64Type>(&*cell);
        cell.iter().map(|e| e.map(|e| -e)).collect::<Vec<_>>()
    });
    let elements: Int64Array = elements.collect();
    list_of(column_type, lengths, Arc::new(elements))
}

/// Rewrites `change` so that the arrays of `A` and `B` keep one size for every row: `B`
/// gets the sizes of `A` where both change, and a column that changes alone gets the
/// sizes the other has.
fn keep_sizes_equal(source: &dyn Table, change: &mut TableChange) {
    if !change.added_values.is_empty() {
        change.added_values[B] = sized_as(&change.added_values[A], &array());
    }
    let a = change.modified_values.get(&A).cloned();
    let b = change.modified_values.contains_key(&B);
    let before = modified_pre_shift(&change.update).unwrap();
    match (a, b) {
        (Some(a), true) => {
            change.modified_values.insert(B, sized_as(&a, &array()));
        }
        (Some(_), false) => {
            let other = source.column(B).chunk(&before).unwrap();
            change
                .modified_values
                .insert(A, sized_as(&other, &vector()));
        }
        (None, true) => {
            let other = source.column(A).chunk(&before).unwrap();
            change.modified_values.insert(B, sized_as(&other, &array()));
        }
        (None, false) => {}
    }
}

/// Cells of `column_type` for random counts, which go up to ten where the lists of
/// `RandomUpdates` have three elements at most: these outgrow slots of two, four and
/// eight keys.
fn random_counted(random: &mut RandomUpdates, column_type: &ColumnType, size: usize) -> ArrayRef {
    let counts = random.values(&ColumnType::Int, size as u64);
    let counts = counts.as_any().downcast_ref::<Int32Array>().expect("ints");
    counted(column_type, counts.iter())
}

/// Replaces the cells of the array columns in `change` by [`random_counted`] ones.
fn with_large_arrays(random: &mut RandomUpdates, change: &mut TableChange) {
    for (index, column_type) in [(A, vector()), (B, array())] {
        if let Some(values) = change.added_values.get_mut(index) {
            *values = random_counted(random, &column_type, values.len());
        }
        if let Some(values) = change.modified_values.get_mut(&index) {
            *values = random_counted(random, &column_type, values.len());
        }
    }
}

/// Drives a source of `K`, `A` and `B` through random cycles for every seed of `seeds` that
/// the seed budget runs (`oxhaven_testkit::test_seeds`). After each
/// cycle the result must be consistent and equal to the ungrouped table of a static copy
/// of the source. Returns how many updates the results delivered.
fn check_random(
    seeds: std::ops::Range<u64>,
    null_fill: bool,
    names: &'static [&'static str],
    minimum_base: u32,
    equal_sizes: bool,
    large: bool,
) -> u64 {
    let types = [("K", ColumnType::String), ("A", vector()), ("B", array())];
    let mut updates = 0;
    for seed in test_seeds(seeds) {
        let graph = graph();
        let mut random = RandomUpdates::new(seed);
        let size = if seed % 5 == 0 { 0 } else { 8 };
        let mut values: Vec<ArrayRef> = types
            .iter()
            .map(|(_, column_type)| random.values(column_type, size))
            .collect();
        if large {
            values[A] = random_counted(&mut random, &vector(), size as usize);
            values[B] = random_counted(&mut random, &array(), size as usize);
        }
        if equal_sizes {
            values[B] = sized_as(&values[A], &array());
        }
        let script = ScriptedTable::new(
            graph.clone(),
            definition(&types),
            RowSet::from_keys((0..size as RowKey).map(|k| 3 * k + 1)),
            values,
        );
        let source: Arc<dyn Table> = script.table();
        let result = ungrouped(&source, null_fill, names, minimum_base).unwrap();
        let copy = source.clone();
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            ungrouped(&frozen(&*copy), null_fill, names, minimum_base).unwrap()
        });
        if let Err(e) = checker.verify() {
            panic!("seed {seed}, before the first step: {e}");
        }
        for step in 0..40 {
            let mut change = random.next_change(&*source, 4);
            if large {
                with_large_arrays(&mut random, &mut change);
            }
            if equal_sizes {
                keep_sizes_equal(&*source, &mut change);
            }
            script.push(change);
            graph.run_cycle();
            assert!(!result.is_failed(), "seed {seed}, step {step}: failed");
            if let Err(e) = checker.verify() {
                panic!("seed {seed}, step {step}: {e}");
            }
        }
        updates += checker.updates_seen();
    }
    updates
}

#[test]
fn random_changes_of_one_array_column() {
    // Up to ten elements: the slots start with two keys and grow to sixteen.
    assert!(check_random(0..100, false, &["A"], 1, false, true) > at_least_for(2000, 0..100));
    // The lists of `RandomUpdates`, with null elements, in slots that grow once.
    assert!(check_random(100..150, false, &["A"], 1, false, false) > at_least_for(1000, 100..150));
    // With Java's slots, which never grow here.
    assert!(
        check_random(150..200, false, &["A"], MINIMUM_BASE, false, false)
            > at_least_for(1000, 150..200)
    );
    // The array column instead of the vector.
    assert!(check_random(200..250, false, &["B"], 1, false, true) > at_least_for(1000, 200..250));
}

#[test]
fn random_changes_of_two_array_columns_with_null_fill() {
    assert!(check_random(0..100, true, &["A", "B"], 1, false, true) > at_least_for(2000, 0..100));
    assert!(
        check_random(100..150, true, &["A", "B"], 1, false, false) > at_least_for(1000, 100..150)
    );
    assert!(
        check_random(150..200, true, &["B", "A"], 3, false, true) > at_least_for(1000, 150..200)
    );
}

#[test]
fn random_changes_of_two_array_columns_of_one_size() {
    assert!(check_random(0..100, false, &["A", "B"], 1, true, true) > at_least_for(2000, 0..100));
    assert!(
        check_random(100..150, false, &["A", "B"], 1, true, false) > at_least_for(1000, 100..150)
    );
    assert!(
        check_random(150..200, false, &["B", "A"], 3, true, true) > at_least_for(1000, 150..200)
    );
}
