//! The tables that cells of a column of tables (`ColumnType::Table`) name.
//!
//! A cell holds a handle, a number that [`resolve`] turns back into the table. The registry
//! refers to a table weakly; what keeps it alive is the column source that made it (the
//! partition aggregation, a transform) and every column source that holds a copy of its
//! cell (`select`, `snapshot`, an aggregation that copies cells): [`FlatSource`] and
//! [`WritableSource`] keep the tables of the handles they store, as Java's object sources
//! keep the tables they refer to.
//!
//! [`FlatSource`]: crate::FlatSource
//! [`WritableSource`]: crate::WritableSource

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::Int64Type;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};

/// Every table that has a handle. Looked up, and swept of tables that are gone.
static TABLES: LazyLock<Mutex<Registry>> = LazyLock::new(Mutex::default);
static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);

#[derive(Default)]
struct Registry {
    tables: HashMap<i64, Weak<dyn Table>>,
    /// The size at which the next registration first drops the tables that are gone.
    sweep_at: usize,
}

fn registry() -> MutexGuard<'static, Registry> {
    TABLES.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The table of a cell of a column of tables, or `None` when nothing keeps it any more.
pub fn resolve(handle: i64) -> Option<Arc<dyn Table>> {
    registry().tables.get(&handle).and_then(Weak::upgrade)
}

/// A new handle for `table`. The caller keeps the table alive for as long as it hands the
/// handle out.
pub fn register(table: &Arc<dyn Table>) -> i64 {
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    let mut registry = registry();
    if registry.tables.len() >= registry.sweep_at {
        registry.tables.retain(|_, table| table.strong_count() > 0);
        registry.sweep_at = (2 * registry.tables.len()).max(1024);
    }
    registry.tables.insert(handle, Arc::downgrade(table));
    handle
}

/// Drops the entry of `handle` if its table is gone.
pub fn forget_if_gone(handle: i64) {
    let mut registry = registry();
    if registry
        .tables
        .get(&handle)
        .is_some_and(|table| table.strong_count() == 0)
    {
        registry.tables.remove(&handle);
    }
}

/// The tables that the cells of `values` name, when `column_type` is a column of tables:
/// what a source that stores `values` keeps alive. A handle whose table is gone names
/// nothing and keeps nothing.
pub(crate) fn kept_by(column_type: &ColumnType, values: &ArrayRef) -> Vec<Arc<dyn Table>> {
    match column_type {
        ColumnType::Table(_) => named_by(values),
        _ => Vec::new(),
    }
}

/// The tables that the handles in `values`, an array of handles, name.
pub(crate) fn named_by(values: &ArrayRef) -> Vec<Arc<dyn Table>> {
    let Some(handles) = values.as_primitive_opt::<Int64Type>() else {
        return Vec::new();
    };
    if handles.null_count() == handles.len() {
        return Vec::new();
    }
    let registry = registry();
    handles
        .iter()
        .flatten()
        .filter_map(|handle| registry.tables.get(&handle).and_then(Weak::upgrade))
        .collect()
}
