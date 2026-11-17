//! Reference implementations of the contracts and the self-consistency checker.
//!
//! Everything here chooses clarity over speed. Use it to drive the code under test and to
//! check what comes out; never as part of the engine.
//!
//! | Item | What it is |
//! | --- | --- |
//! | [`ReferenceGraph`] | A single-threaded [`UpdateGraph`](oxhaven_contracts::graph::UpdateGraph): sources, then notifications until none are left |
//! | [`ReferenceTable`] | A [`Table`](oxhaven_contracts::table::Table) held as a row set and one Arrow array per column |
//! | [`ScriptedTable`] | A refreshing `ReferenceTable` that applies one scripted [`TableChange`] per cycle |
//! | [`RandomUpdates`] | A seeded generator of valid `TableChange`s: adds, removes, modifies and shifts |
//! | [`ConsistencyChecker`] | A listener that checks every update of any table, and optionally compares the table with a recompute |
//! | [`test_seeds`], [`at_least_for`] | The seeds a randomized test runs: a quick share by default, all with `OXHAVEN_TEST_SEEDS=full` |
//! | [`RangeRowSet`] | The row set as a sorted vector of ranges, to compare the contract row set with |
//! | [`Recorder`] | A listener that records the updates and failures it receives |
//! | [`diff_tables`], [`assert_tables_equal`] | Row-by-row comparison of two tables |
//!
//! # Script a table, attach the checker, step a cycle
//!
//! ```
//! use oxhaven_contracts::graph::UpdateGraph;
//! use oxhaven_contracts::rowset::RowSet;
//! use oxhaven_contracts::schema::ColumnType;
//! use oxhaven_contracts::table::Table;
//! use oxhaven_testkit::{
//!     definition, longs, strings, ConsistencyChecker, RandomUpdates, ReferenceGraph,
//!     ScriptedTable, TableChange,
//! };
//! use std::sync::Arc;
//!
//! let graph = ReferenceGraph::new();
//!
//! // A source table with rows 0 and 1. Keep `source`: dropping it stops the script.
//! let source = ScriptedTable::new(
//!     graph.clone(),
//!     definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)]),
//!     RowSet::flat(2),
//!     vec![strings(["a", "b"]), longs([10, 20])],
//! );
//!
//! // Build the table under test from `source.table()`. Here it is the source itself.
//! let result: Arc<dyn Table> = source.table();
//!
//! // Attach the checker before the first cycle, and keep it: dropping it detaches it.
//! // With `attach_with_recompute` it also compares the table with a from-scratch result.
//! let checker = ConsistencyChecker::attach(result.clone());
//!
//! // Script one cycle: remove row 0, add row 5, change Qty of row 1. Values are given in
//! // row-key order, one array per column for adds and one per modified column for modifies.
//! source.push(
//!     TableChange::new()
//!         .remove([0])
//!         .add([5], vec![strings(["c"]), longs([30])])
//!         .modify([1], [(1, longs([21]))]),
//! );
//! graph.run_cycle();
//! checker.assert_consistent(); // panics, listing what is wrong, if any check failed
//! assert_eq!(*result.row_set(), RowSet::from_keys([1, 5]));
//!
//! // Or let a seeded generator write the script. Generate between cycles, so that each
//! // change is made for the rows the table has by then.
//! let mut random = RandomUpdates::new(42);
//! for _ in 0..20 {
//!     source.push(random.next_change(&*source.table(), 10));
//!     graph.run_cycle();
//!     checker.assert_consistent();
//! }
//! ```
//!
//! # What the checker checks
//!
//! On every update: the update is a valid change from the rows the table had
//! ([`TableUpdate::apply_to`](oxhaven_contracts::update::TableUpdate::apply_to)) and yields
//! the rows it has now; the previous row set and previous values still read as they were at
//! the end of the last cycle; and every surviving row that is not modified, and every column
//! of a modified row that is not in the modified column set, kept its value across shifts.
//! Between cycles, [`ConsistencyChecker::verify`] also checks that the table did not change
//! without an update, that previous equals current, and that the table equals the recompute.
//! Call `verify` or `assert_consistent` after every `run_cycle`.

mod checker;
mod compare;
mod graph;
mod random;
mod range_rowset;
mod recorder;
mod script;
mod seeds;
mod table;

pub use checker::{ConsistencyChecker, ConsistencyError};
pub use compare::{assert_tables_equal, diff_tables, TableSnapshot};
pub use graph::ReferenceGraph;
pub use random::RandomUpdates;
pub use range_rowset::RangeRowSet;
pub use recorder::Recorder;
pub use script::ScriptedTable;
pub use seeds::{at_least_for, test_seeds, SEED_BUDGET_VARIABLE};
pub use table::{ChangeError, ReferenceTable, TableChange};

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use std::sync::Arc;

/// A table definition from `(name, type)` pairs.
///
/// # Panics
/// If a name repeats.
pub fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    let columns = columns
        .iter()
        .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
        .collect();
    match TableDefinition::new(columns) {
        Ok(definition) => definition,
        Err(e) => panic!("{e}"),
    }
}

/// Values for a [`ColumnType::Int`] column, without nulls. For nulls or other types, build
/// the Arrow array directly.
pub fn ints(values: impl IntoIterator<Item = i32>) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(values))
}

/// Values for a [`ColumnType::Long`] column, without nulls.
pub fn longs(values: impl IntoIterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

/// Values for a [`ColumnType::Double`] column, without nulls.
pub fn doubles(values: impl IntoIterator<Item = f64>) -> ArrayRef {
    Arc::new(Float64Array::from_iter_values(values))
}

/// Values for a column of the vector or array type `column_type`, from the length of each
/// cell, `None` for a null cell, and the elements of all cells one after another. A null
/// cell has no elements.
///
/// ```
/// use oxhaven_contracts::schema::ColumnType;
/// use oxhaven_testkit::{ints, list_of};
///
/// // [1, 2], null, [], [3]
/// let cells = list_of(
///     &ColumnType::vector(ColumnType::Int),
///     [Some(2), None, Some(0), Some(1)],
///     ints([1, 2, 3]),
/// );
/// assert_eq!(cells.len(), 4);
/// assert_eq!(cells.null_count(), 1);
/// ```
///
/// # Panics
/// If `column_type` is not list-valued, `elements` is not of its element's Arrow type, or
/// the lengths do not add up to the number of elements.
pub fn list_of(
    column_type: &ColumnType,
    lengths: impl IntoIterator<Item = Option<usize>>,
    elements: ArrayRef,
) -> ArrayRef {
    let DataType::List(field) = column_type.arrow_type() else {
        panic!("{column_type:?} is not a vector or an array");
    };
    let lengths: Vec<Option<usize>> = lengths.into_iter().collect();
    let total: usize = lengths.iter().flatten().sum();
    assert_eq!(total, elements.len(), "the lengths of the cells");
    let offsets = OffsetBuffer::from_lengths(lengths.iter().map(|length| length.unwrap_or(0)));
    let valid: NullBuffer = lengths.iter().map(Option::is_some).collect();
    let valid = (valid.null_count() > 0).then_some(valid);
    Arc::new(ListArray::new(field, offsets, elements, valid))
}

/// Values for a [`ColumnType::String`] column, without nulls.
pub fn strings<'a>(values: impl IntoIterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

#[cfg(test)]
pub(crate) mod testing {
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray};

    pub(crate) fn long_values(array: &ArrayRef) -> Vec<Option<i64>> {
        let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
        array.iter().collect()
    }

    pub(crate) fn string_values(array: &ArrayRef) -> Vec<Option<&str>> {
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();
        array.iter().collect()
    }
}
