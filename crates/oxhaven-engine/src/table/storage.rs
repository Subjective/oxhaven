//! Which column sources hold their values in memory.
//!
//! `select` and `update` ask this of a parent column before they reuse it in their result
//! instead of copying it, and a static `select` that reuses none gives its result the row
//! keys `0..size`.

use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

/// The addresses of the live sources that do not hold their values: they compute them on
/// read, or read them from another source at other keys.
///
/// The `ColumnSource` contract cannot say whether a source holds its values in memory.
/// Until it can, the sources that do not are recognized by address: a source registers when
/// it is created and leaves when it is dropped, so an address in the set always belongs to
/// a live source.
static READS_THROUGH: Mutex<BTreeSet<usize>> = Mutex::new(BTreeSet::new());

/// Shares `source` as one that does not hold its values. Its `Drop` must call
/// [`forget_reads_through`].
pub(crate) fn register_reads_through<T: ColumnSource + 'static>(
    source: T,
) -> Arc<dyn ColumnSource> {
    let source = Arc::new(source);
    READS_THROUGH
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(Arc::as_ptr(&source) as usize);
    source
}

pub(crate) fn forget_reads_through<T>(source: &T) {
    READS_THROUGH
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&(source as *const T as usize));
}

/// Whether `source` holds its values in memory, so that `select` passes it through instead
/// of copying it. Every source is taken to, except those made with [`reads_through`] and
/// the formula columns of `view`, `update_view` and `lazy_update`.
///
/// Behavior: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#shouldPreserve,
/// which asks for an io.deephaven.engine.table.impl.sources.InMemoryColumnSource. The array
/// and sparse array sources are; `RedirectedColumnSource`, `UnionColumnSource` and
/// `ViewColumnSource` are not.
pub fn holds_values(source: &Arc<dyn ColumnSource>) -> bool {
    let address = Arc::as_ptr(source) as *const () as usize;
    !READS_THROUGH
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&address)
}

/// `source`, known not to hold its values in memory.
struct ReadsThrough<T>(T);

impl<T> Drop for ReadsThrough<T> {
    fn drop(&mut self) {
        forget_reads_through(self);
    }
}

impl<T: ColumnSource> ColumnSource for ReadsThrough<T> {
    fn column_type(&self) -> &ColumnType {
        self.0.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.0.chunk(rows)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.0.prev_chunk(rows)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        self.0.chunk_by_keys(keys, use_prev)
    }
}

/// Shares `source` as a column that reads its values from somewhere else: through a row
/// redirection, or from the columns of other tables. [`holds_values`] is false of it.
///
/// Behavior: the sources Java makes that are not an
/// io.deephaven.engine.table.impl.sources.InMemoryColumnSource, such as
/// io.deephaven.engine.table.impl.sources.RedirectedColumnSource and
/// io.deephaven.engine.table.impl.sources.UnionColumnSource
pub fn reads_through<T: ColumnSource + 'static>(source: T) -> Arc<dyn ColumnSource> {
    register_reads_through(ReadsThrough(source))
}

/// A source that is shared with the operation that writes it.
struct Shared<T>(Arc<T>);

impl<T: ColumnSource> ColumnSource for Shared<T> {
    fn column_type(&self) -> &ColumnType {
        self.0.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.0.chunk(rows)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.0.prev_chunk(rows)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        self.0.chunk_by_keys(keys, use_prev)
    }
}

/// [`reads_through`] for a source that its operation keeps writing: the column of a result
/// whose values Java reads from the operation's state, where this port stores them.
pub(crate) fn shared_reads_through<T: ColumnSource + 'static>(
    source: &Arc<T>,
) -> Arc<dyn ColumnSource> {
    reads_through(Shared(source.clone()))
}
