//! Handles: what a cell of a table column holds.

use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};

/// Every table that has a handle. Looked up only, never iterated.
static TABLES: LazyLock<Mutex<HashMap<i64, Weak<dyn Table>>>> = LazyLock::new(Mutex::default);
static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);

fn tables() -> MutexGuard<'static, HashMap<i64, Weak<dyn Table>>> {
    TABLES.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The table of a cell of a column of type `ColumnType::Table`, or `None` when the column
/// source that owned it is gone.
pub fn resolve(handle: i64) -> Option<Arc<dyn Table>> {
    tables().get(&handle).and_then(Weak::upgrade)
}

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
        let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
        tables().insert(handle, Arc::downgrade(&table));
        self.owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(handle, table);
        handle
    }

    /// Gives up the table that `handle` names: the handle names nothing from now on.
    pub fn release(&self, handle: i64) {
        let table = self
            .owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&handle);
        if table.is_some() {
            tables().remove(&handle);
        }
    }
}

impl Drop for TableCells {
    fn drop(&mut self) {
        let owned = self.owned.get_mut().unwrap_or_else(PoisonError::into_inner);
        let mut tables = tables();
        for handle in owned.keys() {
            tables.remove(handle);
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
