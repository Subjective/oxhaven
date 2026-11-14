//! Tables, column sources and listeners: the seam between operators.
//!
//! Port of the roles of `io.deephaven.engine.table.Table`,
//! `io.deephaven.engine.table.ColumnSource` and
//! `io.deephaven.engine.table.TableUpdateListener`.

use crate::graph::UpdateGraph;
use crate::rowset::RowSet;
use crate::schema::{ColumnType, TableDefinition};
use crate::update::TableUpdate;
use arrow_array::ArrayRef;
use std::fmt;
use std::sync::Arc;

/// Read access to one column's data by row key.
pub trait ColumnSource: Send + Sync {
    fn column_type(&self) -> &ColumnType;

    /// Current values at `rows`, in row-key order, as an Arrow array of
    /// `column_type().arrow_type()` with `rows.size()` elements.
    ///
    /// Port of `ColumnSource#getChunk` / `fillChunk`. `rows` must be a subset of the owning
    /// table's current row set.
    fn chunk(&self, rows: &RowSet) -> ArrayRef;

    /// Values as of the end of the previous cycle. `rows` is in the previous key space and
    /// must be a subset of the owning table's previous row set.
    ///
    /// Port of `ColumnSource#getPrevChunk` / `fillPrevChunk`. During the idle phase previous
    /// and current values are the same. During the updating phase a source must answer with
    /// previous values until the cycle ends, including for rows it has already changed.
    fn prev_chunk(&self, rows: &RowSet) -> ArrayRef;
}

/// Semantic hints carried by a table and propagated through operations.
///
/// Port of the attributes in `io.deephaven.engine.table.Table` that change engine behavior:
/// `BLINK_TABLE_ATTRIBUTE`, `ADD_ONLY_TABLE_ATTRIBUTE`, `APPEND_ONLY_TABLE_ATTRIBUTE`.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
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
/// finished updating for that cycle.
pub trait UpdateListener: Send + Sync {
    /// Port of `TableUpdateListener#onUpdate`. The parent's previous row set and previous
    /// column values remain readable for the rest of the cycle.
    fn on_update(&self, update: &TableUpdate);

    /// Port of `TableUpdateListener#onFailure`.
    fn on_failure(&self, failure: &TableFailure);
}

/// A columnar table, static or refreshing.
pub trait Table: Send + Sync {
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

    /// The update graph that drives this table, or `None` for a static table.
    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>>;

    /// Registers a listener for this table's updates. The table holds the listener weakly:
    /// the caller keeps it alive, and dropping it unsubscribes.
    ///
    /// Port of `Table#addUpdateListener`. Has no effect on a static table.
    fn add_listener(&self, listener: &Arc<dyn UpdateListener>);
}
