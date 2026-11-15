//! Tables, column sources and listeners: the seam between operators.
//!
//! Port of the roles of `io.deephaven.engine.table.Table`,
//! `io.deephaven.engine.table.ColumnSource` and
//! `io.deephaven.engine.table.TableUpdateListener`.

use crate::graph::{Dependency, UpdateGraph};
use crate::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use crate::schema::{ColumnType, TableDefinition};
use crate::update::TableUpdate;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::record_batch::RecordBatch;
use std::fmt;
use std::sync::Arc;

/// Read access to one column's data by row key.
pub trait ColumnSource: Send + Sync {
    fn column_type(&self) -> &ColumnType;

    /// Current values at `rows`, in row-key order, as an Arrow array of
    /// `column_type().arrow_type()` with `rows.size()` elements. For a vector or array
    /// column that is a `ListArray`; [`ColumnType::list_element`] says how its cells are
    /// laid out.
    ///
    /// Port of `ColumnSource#getChunk` / `fillChunk`. `rows` must be a subset of the owning
    /// table's current row set.
    ///
    /// An error means the data could not be read, for example a failed read from storage.
    /// A listener that receives one fails its result table.
    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure>;

    /// Values as of the end of the previous cycle. `rows` is in the previous key space and
    /// must be a subset of the owning table's previous row set.
    ///
    /// Port of `ColumnSource#getPrevChunk` / `fillPrevChunk`. During the idle phase previous
    /// and current values are the same. During the updating phase a source must answer with
    /// previous values until the cycle ends, including for rows it has already changed.
    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure>;

    /// Values at `keys` in the order given, with repeats allowed. A `NULL_ROW_KEY` yields a
    /// null element. Reads previous values when `use_prev` is set. Every other key must be in
    /// the owning table's current (or previous) row set.
    ///
    /// This is the read joins and other redirected results need, where output order is not
    /// row-key order. Port of the unordered fill of `FillUnordered#fillChunkUnordered` and
    /// `fillPrevChunkUnordered`. The provided implementation reads the distinct keys once in
    /// key order and reorders; a source may override it with a direct lookup.
    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let distinct = RowSet::from_keys(keys.iter().copied().filter(|&k| k != NULL_ROW_KEY));
        let values = if use_prev {
            self.prev_chunk(&distinct)?
        } else {
            self.chunk(&distinct)?
        };
        let positions: UInt64Array = keys
            .iter()
            .map(|&k| {
                if k == NULL_ROW_KEY {
                    None
                } else {
                    distinct.find(k)
                }
            })
            .collect();
        arrow::compute::take(values.as_ref(), &positions, None).map_err(|e| TableFailure {
            message: e.to_string(),
        })
    }
}

/// Semantic hints carried by a table and propagated through operations.
///
/// Port of the attributes in `io.deephaven.engine.table.Table` that change engine behavior:
/// `BLINK_TABLE_ATTRIBUTE`, `ADD_ONLY_TABLE_ATTRIBUTE`, `APPEND_ONLY_TABLE_ATTRIBUTE`.
///
/// Build one with `TableAttributes::default()` and set fields; more hints will be added.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
#[non_exhaustive]
pub struct TableAttributes {
    /// Rows exist for exactly one cycle.
    pub blink: bool,
    /// Rows are only ever added; never removed, modified or shifted.
    pub add_only: bool,
    /// Add-only, and every added key is greater than all existing keys.
    pub append_only: bool,
}

/// A failure delivered to listeners in place of an update. After a failure a table delivers
/// nothing further.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TableFailure {
    pub message: String,
}

impl fmt::Display for TableFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TableFailure {}

/// Receives a table's changes.
///
/// Port of `TableUpdateListener`. The update graph calls a listener at most once per cycle
/// per parent, during the updating phase, after every table the listener depends on has
/// finished updating for that cycle. A table notifies its listeners at most once per step.
///
/// A listener is also a [`Dependency`]: it is satisfied for a step once it has run for that
/// step, or once it is known that it will not run because none of its parents will notify.
/// Port of `NotificationQueue.Dependency#satisfied` as implemented by `InstrumentedTableUpdateListener`
/// and `MergedListener`.
///
/// # Ownership
///
/// The result table of an operation owns its listener strongly (`Arc`). The listener owns
/// its parent tables strongly and refers to its result table weakly (`Weak`). A parent
/// holds its listeners weakly. So keeping a result table alive keeps its whole upstream
/// alive, dropping it unsubscribes, and there is no reference cycle. This replaces Java's
/// liveness reference counting.
pub trait UpdateListener: Dependency {
    /// Port of `TableUpdateListener#onUpdate`. The parent's previous row set and previous
    /// column values remain readable for the rest of the cycle.
    ///
    /// On `Err` the caller delivers the failure to this listener's own `on_failure`, which
    /// fails the result table and propagates downstream. Port of the catch in
    /// `InstrumentedTableListenerBase#doRunInternal`.
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure>;

    /// Port of `TableUpdateListener#onFailure`.
    fn on_failure(&self, failure: &TableFailure);
}

/// Writes to an input table. Port of the role of `io.deephaven.engine.util.input.InputTableUpdater`.
///
/// Changes are queued and applied by the table's update source on the next cycle. Both
/// calls return once the change is visible in the table.
pub trait InputTable: Send + Sync {
    /// Names of the key columns; empty for an append-only input table.
    /// Port of `InputTableUpdater#getKeyNames`.
    fn key_columns(&self) -> Vec<String>;

    /// Names of the value columns: the columns of the input table's own definition that
    /// are not key columns, in the definition's order. A table that carries this input
    /// table through an operation may have other columns, which are neither.
    /// Port of `InputTableUpdater#getValueNames`.
    fn value_columns(&self) -> Vec<String>;

    /// Adds rows, replacing rows with equal keys in a keyed table. `rows` must match the
    /// table's definition. Port of `InputTableUpdater#add`.
    fn add(&self, rows: RecordBatch) -> Result<(), TableFailure>;

    /// Deletes the rows whose keys appear in `keys`, which holds the key columns only.
    /// An error on an append-only input table. Port of `InputTableUpdater#delete`.
    fn delete(&self, keys: RecordBatch) -> Result<(), TableFailure>;
}

/// A columnar table, static or refreshing.
///
/// A refreshing table is a [`Dependency`]: it is satisfied for a step once it has delivered
/// its update for that step or it is known that it will not. A static table is always
/// satisfied. Port of `BaseTable#satisfied`.
///
/// # Reading under the lock
///
/// The row set and column data of a refreshing table may be read only while holding the
/// update graph's shared lock (see [`UpdateGraph::with_shared_lock`]) or from inside a cycle
/// on a thread for which `current_thread_processes_updates()` is true. An operation reads
/// its parents' state and calls `add_listener` under one hold of the lock, so that no cycle
/// can run between the snapshot and the subscription.
pub trait Table: Dependency {
    fn definition(&self) -> &TableDefinition;

    /// The current row set. Port of `Table#getRowSet`.
    fn row_set(&self) -> Arc<RowSet>;

    /// The row set as of the end of the previous cycle. Equal to `row_set()` while idle.
    /// Port of `TrackingRowSet#prev`.
    fn prev_row_set(&self) -> Arc<RowSet>;

    /// The column at `index` in `definition().columns()`.
    ///
    /// # Panics
    /// If `index` is out of range.
    fn column(&self, index: usize) -> Arc<dyn ColumnSource>;

    /// Port of `Table#isRefreshing`. A static table never notifies listeners.
    fn is_refreshing(&self) -> bool;

    fn attributes(&self) -> TableAttributes;

    /// Whether the row set is exactly `0..size` now and after every future update.
    /// Port of `Table#isFlat`.
    fn is_flat(&self) -> bool {
        false
    }

    /// Whether this table has failed. A failed table delivers nothing further, and an
    /// operation on it fails. Port of `Table#isFailed`.
    fn is_failed(&self) -> bool {
        false
    }

    /// The write side of this table if it is an input table.
    /// Port of the `INPUT_TABLE_ATTRIBUTE` lookup.
    fn input_table(&self) -> Option<Arc<dyn InputTable>> {
        None
    }

    /// The update graph that drives this table, or `None` for a static table.
    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>>;

    /// Registers a listener for this table's updates. The table holds the listener weakly:
    /// the caller keeps it alive, and dropping it unsubscribes.
    ///
    /// Port of `Table#addUpdateListener`. Has no effect on a static table.
    fn add_listener(&self, listener: &Arc<dyn UpdateListener>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnType;
    use arrow::array::{Array, Int64Array, ListArray};
    use arrow::datatypes::{DataType, Int64Type};

    /// A column whose value at row key `k` is `10 * k` now and `k` on the previous cycle.
    struct TenTimesKey(ColumnType);

    impl ColumnSource for TenTimesKey {
        fn column_type(&self) -> &ColumnType {
            &self.0
        }
        fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
            Ok(Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|k| 10 * k),
            )))
        }
        fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
            Ok(Arc::new(Int64Array::from_iter_values(rows.iter())))
        }
    }

    #[test]
    fn chunk_by_keys_keeps_order_repeats_and_nulls() {
        let source = TenTimesKey(ColumnType::Long);
        let keys = [7, 2, NULL_ROW_KEY, 7, 100];

        let current = source.chunk_by_keys(&keys, false).unwrap();
        let current = current.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            current.iter().collect::<Vec<_>>(),
            vec![Some(70), Some(20), None, Some(70), Some(1000)]
        );

        let prev = source.chunk_by_keys(&keys, true).unwrap();
        let prev = prev.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            prev.iter().collect::<Vec<_>>(),
            vec![Some(7), Some(2), None, Some(7), Some(100)]
        );
    }

    /// A vector column whose cell at row key `k` holds `k` elements, all `k`; the cell at
    /// row key 3 is null.
    struct KeyTimesKey(ColumnType);

    impl ColumnSource for KeyTimesKey {
        fn column_type(&self) -> &ColumnType {
            &self.0
        }
        fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
            let cells = rows
                .iter()
                .map(|k| (k != 3).then(|| (0..k).map(move |_| Some(k)).collect::<Vec<_>>()));
            let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(cells);
            let (_, offsets, values, nulls) = lists.into_parts();
            let DataType::List(field) = self.0.arrow_type() else {
                unreachable!("a vector is a list")
            };
            Ok(Arc::new(ListArray::new(field, offsets, values, nulls)))
        }
        fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
            self.chunk(rows)
        }
    }

    #[test]
    fn chunk_by_keys_of_a_list_column_gives_null_entries_and_whole_cells() {
        let source = KeyTimesKey(ColumnType::vector(ColumnType::Long));
        let cells = source
            .chunk_by_keys(&[2, NULL_ROW_KEY, 0, 3, 2, 1], false)
            .unwrap();
        assert_eq!(cells.data_type(), &source.column_type().arrow_type());
        let cells = cells.as_any().downcast_ref::<ListArray>().unwrap();
        let seen: Vec<Option<Vec<Option<i64>>>> = cells
            .iter()
            .map(|cell| {
                cell.map(|cell| {
                    let cell = cell.as_any().downcast_ref::<Int64Array>().unwrap();
                    cell.iter().collect()
                })
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                Some(vec![Some(2), Some(2)]),
                None,
                // an empty vector is not a null cell
                Some(vec![]),
                None,
                Some(vec![Some(2), Some(2)]),
                Some(vec![Some(1)]),
            ]
        );
    }
}
