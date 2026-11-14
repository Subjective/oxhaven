//! `ungroup` with stand-ins for array columns: the contracts have no array-valued column
//! type, so the arrays of these tests are made from the values of an `Int` column.

use super::keys::MINIMUM_BASE;
use super::operator::{ungroup_arrays, Ungrouped};
use super::sources::ArrayColumn;
use super::{ungroup, UngroupError};
use crate::ops::sort::testing::{
    blink_table, cells, frozen, graph, keys, keys_of, scripted, shifts, static_table, Columns,
};
use crate::table::holds_values;
use crate::update::modified_pre_shift;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    definition, ints, strings, ConsistencyChecker, RandomUpdates, Recorder, ScriptedTable,
    TableChange,
};
use std::sync::Arc;

/// The arrays of an `Int` column, for tests. The array of a row whose value is `n` has
/// `|n|` elements, the longs `100 * n`, `100 * n + 1` and so on; a null value is a null
/// array. So the arrays change exactly when the column does, and their previous values
/// are the column's.
struct Counted {
    column: Arc<dyn ColumnSource>,
}

fn counts(values: &ArrayRef) -> &Int32Array {
    values.as_any().downcast_ref().expect("an Int column")
}

impl ArrayColumn for Counted {
    fn element_type(&self) -> &ColumnType {
        &ColumnType::Long
    }

    fn sizes(&self, rows: &RowSet, previous: bool) -> Result<Vec<u64>, TableFailure> {
        let values = if previous {
            self.column.prev_chunk(rows)?
        } else {
            self.column.chunk(rows)?
        };
        let sizes = counts(&values).iter();
        Ok(sizes
            .map(|n| n.map_or(0, |n| u64::from(n.unsigned_abs())))
            .collect())
    }

    fn elements(&self, cells: &[(RowKey, u64)], previous: bool) -> Result<ArrayRef, TableFailure> {
        let rows: Vec<RowKey> = cells.iter().map(|(row, _)| *row).collect();
        let values = self.column.chunk_by_keys(&rows, previous)?;
        let elements = counts(&values).iter().zip(cells).map(|(n, (_, offset))| {
            let n = n.filter(|n| *offset < u64::from(n.unsigned_abs()))?;
            Some(100 * i64::from(n) + *offset as i64)
        });
        Ok(Arc::new(elements.collect::<Int64Array>()))
    }
}

/// `source` with the `Int` columns `names` ungrouped as [`Counted`] arrays.
fn ungrouped(
    source: &Arc<dyn Table>,
    null_fill: bool,
    names: &[&str],
    minimum_base: u32,
) -> Result<Arc<dyn Table>, UngroupError> {
    let columns = names.iter().map(|name| {
        let index = source.definition().index_of(name).expect("a column");
        Ungrouped {
            index,
            arrays: Arc::new(Counted {
                column: source.column(index),
            }),
        }
    });
    ungroup_arrays(source, null_fill, columns.collect(), minimum_base)
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

fn some_ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<Int32Array>())
}

/// `K`, a string that is not ungrouped, and `A`, the sizes of the arrays.
fn one(k: &[&str], a: &[i32]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("K", ColumnType::String, strings(k.iter().copied())),
        ("A", ColumnType::Int, ints(a.iter().copied())),
    ]
}

/// `K` and the sizes of two array columns, `A` and `B`.
fn two(k: &[&str], a: &[i32], b: &[Option<i32>]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let mut columns = one(k, a);
    columns.push(("B", ColumnType::Int, some_ints(b)));
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
    static_table(&one(&["x", "y"], &[1, 2]))
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
    let error = ungroup(&plain(), false, &names(&["A"])).err().expect("a refusal");
    assert_eq!(error, UngroupError::NotAnArray("A".to_string()));
    assert_eq!(error.to_string(), "Column A is not an array or Vector");
}

#[test]
fn a_column_that_does_not_exist_is_refused() {
    let error = ungroup(&plain(), true, &names(&["Nope"])).err().expect("a refusal");
    assert_eq!(
        error.to_string(),
        "Unknown column names [Nope], available column names are [K, A]"
    );
}

/// Java looks at the names in turn and stops at the first it cannot ungroup.
#[test]
fn the_first_bad_column_is_the_error() {
    let error = ungroup(&plain(), false, &names(&["K", "Nope"])).err().expect("a refusal");
    assert_eq!(error, UngroupError::NotAnArray("K".to_string()));
    let error = ungroup(&plain(), false, &names(&["Nope", "K"])).err().expect("a refusal");
    assert!(matches!(error, UngroupError::NoSuchColumn { name, .. } if name == "Nope"));
}

// ---------------------------------------------------------------------------------------
// static sources

#[test]
fn a_row_for_every_element_in_the_order_of_rows_and_arrays() {
    let source = static_table(&[
        ("K", ColumnType::String, strings(["x", "y", "z", "w"])),
        ("A", ColumnType::Int, some_ints(&[Some(2), None, Some(0), Some(-3)])),
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
    assert!(!result.is_refreshing() && !result.is_flat());
    assert_eq!(result.attributes(), Default::default());
    // Neither kind of column holds its values.
    assert!(!holds_values(&result.column(K)) && !holds_values(&result.column(A)));
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
    assert_eq!(rows(&*result), ["x|200|5", "x|201|5", "y|100|0"]);
    assert_eq!(result.definition().columns()[B].column_type, ColumnType::Int);
}

#[test]
fn arrays_that_differ_in_size_are_refused() {
    let columns = two(&["x", "y", "z"], &[2, 1, 4], &[Some(2), Some(3), Some(1)]);
    let source = scripted(&graph(), [4, 7, 9], &columns).table() as Arc<dyn Table>;
    let error = ungrouped(&source, false, &["A", "B"], MINIMUM_BASE).err().expect("a refusal");
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
    let error = ungrouped(&source, false, &["B", "A"], MINIMUM_BASE).err().expect("a refusal");
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
            ints(sizes)
        };
        static_table(&[
            ("A", ColumnType::Int, ints((0..size).map(|_| 1))),
            ("B", ColumnType::Int, sizes(b_differs)),
            ("C", ColumnType::Int, sizes(c_differs)),
        ])
    };
    let differs = |source: Arc<dyn Table>| {
        match ungrouped(&source, false, &["A", "B", "C"], MINIMUM_BASE).err().expect("a refusal") {
            UngroupError::SizesDiffer {
                row_key, column, ..
            } => (row_key, column),
            other => panic!("{other}"),
        }
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
    let error = ungrouped(&source, false, &["A"], MINIMUM_BASE).err().expect("a refusal");
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

fn row(k: &str, a: i32) -> Vec<ArrayRef> {
    vec![strings([k]), ints([a])]
}

#[test]
fn an_added_row_brings_its_elements() {
    let rig = rig(&[0, 2], &["x", "z"], &[1, 2]);
    assert!(rig.result.is_refreshing());
    assert_eq!(keys_of(&*rig.result), [0, 4, 5]);
    let shown = rig.changed(TableChange::new().add([1], row("y", 2)));
    assert_eq!(shown, Shown::of(&[2, 3], &[], &[]));
    assert_eq!(
        rig.rows(),
        ["x|100", "y|200", "y|201", "z|200", "z|201"]
    );
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
    let change = TableChange::new().modify([0, 1, 2, 3], [(A, ints([-2, 1, 2, 1]))]);
    let shown = rig.changed(change);
    assert_eq!(
        shown,
        Shown::of(&[5, 6], &[3], &[0, 1, 2, 4]).columns(&[A])
    );
    assert_eq!(
        rig.rows(),
        ["x|-200", "x|-199", "y|100", "z|200", "z|201", "w|100"]
    );
    // And loses its last.
    let change = TableChange::new().modify([1, 2], [(A, ints([0, 2]))]);
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
        .modify([1, 2], [(A, ints([2, 2]))]);
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
    let expected =
        Shown::of(&[16, 17, 18, 19, 20], &[], &[]).shifted(&[(2, 3, 6), (6, 6, 18)]);
    assert_eq!(shown, expected);
    assert_eq!(keys_of(&*rig.result), [0, 1, 8, 9, 16, 17, 18, 19, 20, 24]);
    // The slots stay as large when the array has gone.
    let shown = rig.changed(TableChange::new().remove([2]).add([4], row("v", 1)));
    assert_eq!(shown, Shown::of(&[32], &[16, 17, 18, 19, 20], &[]));
}

#[test]
fn a_modified_array_that_does_not_fit_moves_every_slot() {
    let rig = rig(&[0, 1], &["x", "y"], &[1, 2]);
    let shown = rig.changed(TableChange::new().modify([0], [(A, ints([3]))]));
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
        .modify([1, 3], [(K, strings(["q", "r"])), (A, ints([3, -2]))]);
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

// ---------------------------------------------------------------------------------------
// refreshing sources with two array columns

fn pair(keys: &[RowKey], k: &[&str], a: &[i32], b: &[Option<i32>], null_fill: bool) -> Rig {
    Rig::new(keys, &two(k, a, b), null_fill, &["A", "B"], 1)
}

fn row_of_two(k: &str, a: i32, b: i32) -> Vec<ArrayRef> {
    vec![strings([k]), ints([a]), ints([b])]
}

#[test]
fn an_array_modified_alone_must_keep_its_size() {
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let shown = rig.changed(TableChange::new().modify([4], [(B, ints([2]))]));
    assert_eq!(shown, Shown::of(&[], &[], &[8, 9]).columns(&[B]));
    assert_eq!(rig.rows(), ["x|200|200", "x|201|201", "y|100|-100"]);
    // The column that was not modified is the one it is compared with, whichever
    // comes first.
    let failure = rig.fails(TableChange::new().modify([7], [(B, ints([2]))]));
    assert_eq!(
        failure,
        "Array sizes differ at row key 7 (position 1), A has size 1, B has size 2"
    );
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let failure = rig.fails(TableChange::new().modify([7], [(A, ints([0]))]));
    assert_eq!(
        failure,
        "Array sizes differ at row key 7 (position 1), B has size 1, A has size 0"
    );
}

#[test]
fn arrays_modified_together_change_their_size_together() {
    let rig = pair(&[4, 7], &["x", "y"], &[2, 1], &[Some(-2), Some(-1)], false);
    let change = TableChange::new().modify([7], [(A, ints([2])), (B, ints([2]))]);
    let shown = rig.changed(change);
    assert_eq!(shown, Shown::of(&[15], &[], &[14]).columns(&[A, B]));
    let change = TableChange::new().modify([7], [(A, ints([2])), (B, ints([1]))]);
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
        .modify([1], [(B, ints([2]))]);
    let shown = rig.changed(change);
    let expected = Shown::of(&[8, 9, 10], &[], &[4])
        .shifted(&[(2, 2, 2)])
        .columns(&[B]);
    assert_eq!(shown, expected);
    assert_eq!(
        rig.rows(),
        ["x|200|-200", "x|201|-199", "y|100|200", "z|300|300", "z|301|301", "z|302|302"]
    );
}

#[test]
fn null_fill_reads_every_array_when_one_is_modified() {
    let rig = pair(&[0, 1], &["x", "y"], &[2, 1], &[Some(1), None], true);
    assert_eq!(rig.rows(), ["x|200|100", "x|201|null", "y|100|null"]);
    // The array of `B` outgrows the one of `A`, and the slots.
    let shown = rig.changed(TableChange::new().modify([1], [(B, ints([3]))]));
    let expected = Shown::of(&[5, 6], &[], &[4])
        .shifted(&[(2, 2, 2)])
        .columns(&[B]);
    assert_eq!(shown, expected);
    assert_eq!(
        rig.rows(),
        ["x|200|100", "x|201|null", "y|100|300", "y|null|301", "y|null|302"]
    );
    // It shrinks below the one of `A`, which was not modified.
    let shown = rig.changed(TableChange::new().modify([1], [(B, ints([0]))]));
    assert_eq!(shown, Shown::of(&[], &[5, 6], &[4]).columns(&[B]));
    assert_eq!(rig.rows(), ["x|200|100", "x|201|null", "y|100|null"]);
}

// ---------------------------------------------------------------------------------------
// blink sources

#[test]
fn a_blink_table_starts_again_every_cycle() {
    let graph = graph();
    let columns = [("K", ColumnType::String), ("A", ColumnType::Int)];
    let (source, blink) = blink_table(&graph, &columns);
    let result = ungrouped(&source, false, &["A"], 1).unwrap();
    assert!(result.attributes().blink);
    let recorder = Recorder::attach(result.clone());
    let checker = ConsistencyChecker::attach(result.clone());
    blink.push(vec![strings(["x", "y"]), ints([5, 1])]);
    blink.push(vec![strings(["z"]), ints([2])]);
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

/// Rewrites `change` so that the arrays of `A` and `B` keep one size for every row: `B`
/// gets the sizes of `A` where both change, and a column that changes alone gets the
/// sizes the other has.
fn keep_sizes_equal(source: &dyn Table, change: &mut TableChange) {
    let negated = |values: &ArrayRef| -> ArrayRef {
        let negated = counts(values).iter().map(|n| n.map(|n| -n));
        Arc::new(negated.collect::<Int32Array>())
    };
    // The values of `signs` with the sizes of `sizes`.
    let sized = |signs: &ArrayRef, sizes: &ArrayRef| -> ArrayRef {
        let values = counts(signs).iter().zip(counts(sizes).iter());
        let values = values.map(|(sign, size)| {
            let size = size?.abs();
            Some(if sign.is_some_and(|sign| sign < 0) {
                -size
            } else {
                size
            })
        });
        Arc::new(values.collect::<Int32Array>())
    };
    if !change.added_values.is_empty() {
        change.added_values[B] = negated(&change.added_values[A]);
    }
    let a = change.modified_values.get(&A).cloned();
    let b = change.modified_values.get(&B).cloned();
    let before = modified_pre_shift(&change.update).unwrap();
    match (a, b) {
        (Some(a), Some(_)) => {
            change.modified_values.insert(B, negated(&a));
        }
        (Some(a), None) => {
            let other = source.column(B).chunk(&before).unwrap();
            change.modified_values.insert(A, sized(&a, &other));
        }
        (None, Some(b)) => {
            let other = source.column(A).chunk(&before).unwrap();
            change.modified_values.insert(B, sized(&b, &other));
        }
        (None, None) => {}
    }
}

/// Drives a source of `K`, `A` and `B` through random cycles for every seed. After each
/// cycle the result must be consistent and equal to the ungrouped table of a static copy
/// of the source. Returns how many updates the results delivered.
fn check_random(
    seeds: std::ops::Range<u64>,
    null_fill: bool,
    names: &'static [&'static str],
    minimum_base: u32,
    equal_sizes: bool,
) -> u64 {
    let types = [
        ("K", ColumnType::String),
        ("A", ColumnType::Int),
        ("B", ColumnType::Int),
    ];
    let mut updates = 0;
    for seed in seeds {
        let graph = graph();
        let mut random = RandomUpdates::new(seed);
        let size = if seed % 5 == 0 { 0 } else { 8 };
        let mut values: Vec<ArrayRef> = types
            .iter()
            .map(|(_, column_type)| random.values(column_type, size))
            .collect();
        if equal_sizes {
            values[B] = values[A].clone();
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
    // Sizes up to ten: the slots start with two keys and grow to sixteen.
    assert!(check_random(0..150, false, &["A"], 1, false) > 3000);
    // With Java's slots, which never grow here.
    assert!(check_random(150..200, false, &["A"], MINIMUM_BASE, false) > 1000);
}

#[test]
fn random_changes_of_two_array_columns_with_null_fill() {
    assert!(check_random(0..150, true, &["A", "B"], 1, false) > 3000);
    assert!(check_random(150..200, true, &["B", "A"], 3, false) > 1000);
}

#[test]
fn random_changes_of_two_array_columns_of_one_size() {
    assert!(check_random(0..150, false, &["A", "B"], 1, true) > 3000);
    assert!(check_random(150..200, false, &["B", "A"], 3, true) > 1000);
}
