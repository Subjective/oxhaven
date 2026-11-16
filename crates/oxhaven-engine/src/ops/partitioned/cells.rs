//! Handles: what a cell of a table column holds.

use arrow::array::ArrayRef;
use oxhaven_column::tables::{forget_if_gone, register};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

pub use oxhaven_column::tables::resolve;

/// The tables of the cells of one column, which it owns: a handle is good while this
/// lives. The column source of a table column holds it ([`TableColumn`]).
///
/// Behavior: the role of io.deephaven.engine.table.impl.sources.ObjectArraySource for a
/// column of `Table`, with the `manage` of
/// io.deephaven.engine.table.impl.by.PartitionByChunkedOperator#linkTableReferences
#[derive(Default)]
pub struct TableCells {
    owned: Mutex<HashMap<i64, Arc<dyn Table>>>,
}

impl TableCells {
    /// Takes `table` and returns the handle that names it from now on.
    pub fn register(&self, table: Arc<dyn Table>) -> i64 {
        let handle = register(&table);
        self.owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(handle, table);
        handle
    }

    /// Gives up the table that `handle` names: the handle names nothing from now on,
    /// unless a copy of the cell keeps the table.
    pub fn release(&self, handle: i64) {
        let table = self
            .owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&handle);
        if table.is_some() {
            drop(table);
            forget_if_gone(handle);
        }
    }
}

impl Drop for TableCells {
    fn drop(&mut self) {
        let owned = std::mem::take(self.owned.get_mut().unwrap_or_else(PoisonError::into_inner));
        let handles: Vec<i64> = owned.keys().copied().collect();
        drop(owned);
        for handle in handles {
            forget_if_gone(handle);
        }
    }
}

/// A column of tables: the handles, and the tables they name.
pub struct TableColumn {
    handles: Arc<dyn ColumnSource>,
    _cells: Arc<TableCells>,
}

impl TableColumn {
    /// `handles` is a source of a `ColumnType::Table` type whose cells `cells` registered.
    pub fn new(handles: Arc<dyn ColumnSource>, cells: Arc<TableCells>) -> TableColumn {
        TableColumn {
            handles,
            _cells: cells,
        }
    }
}

impl ColumnSource for TableColumn {
    fn column_type(&self) -> &ColumnType {
        self.handles.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.handles.chunk(rows)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.handles.prev_chunk(rows)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        self.handles.chunk_by_keys(keys, use_prev)
    }
}
