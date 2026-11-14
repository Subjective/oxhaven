//! Reading a column through a mapping from outer row keys to inner row keys.

use crate::tracked::{Clock, Tracked};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::{Arc, PoisonError, RwLock};

/// A mapping from the row keys of a result (outer) to the row keys of the data it shows
/// (inner). Sort, joins and other operators that reorder or select rows without copying
/// values publish their columns through one.
///
/// Behavior: io.deephaven.engine.table.impl.util.RowRedirection
pub trait RowRedirection: Send + Sync {
    /// The inner key for each of `keys`, in the order given, as of now or, with `use_prev`,
    /// as of the end of the previous cycle. An outer key with no mapping, and
    /// `NULL_ROW_KEY`, map to `NULL_ROW_KEY`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.RowRedirection#get and #getPrev
    fn redirect(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey>;
}

/// A row redirection that can be changed, with previous mappings for the current cycle.
/// Previous mappings follow the same rules as the previous values of
/// [`WritableSource`](crate::WritableSource).
///
/// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection
pub struct WritableRowRedirection {
    map: RwLock<Tracked<RowKey>>,
}

impl WritableRowRedirection {
    /// An empty redirection that does not track previous mappings.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(Tracked::new()),
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection#startTrackingPrevValues
    ///
    /// # Panics
    /// If called twice.
    pub fn start_tracking_prev(&self, clock: Arc<dyn Clock>) {
        self.write().start_tracking_prev(clock);
    }

    /// Maps `outer` to `inner` and returns the inner key it mapped to before, or
    /// `NULL_ROW_KEY`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection#put
    ///
    /// # Panics
    /// If either key is negative.
    pub fn put(&self, outer: RowKey, inner: RowKey) -> RowKey {
        assert!(
            outer >= 0 && inner >= 0,
            "invalid redirection {outer} -> {inner}"
        );
        let old = self.write().write().put(outer, Some(inner));
        old.unwrap_or(NULL_ROW_KEY)
    }

    /// Removes the mapping of `outer` and returns the inner key it mapped to, or
    /// `NULL_ROW_KEY`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection#remove
    pub fn remove(&self, outer: RowKey) -> RowKey {
        let old = self.write().write().put(outer, None);
        old.unwrap_or(NULL_ROW_KEY)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Tracked<RowKey>> {
        self.map.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for WritableRowRedirection {
    fn default() -> Self {
        Self::new()
    }
}

impl RowRedirection for WritableRowRedirection {
    fn redirect(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        let map = self.map.read().unwrap_or_else(PoisonError::into_inner);
        let reader = map.read(use_prev);
        keys.iter()
            .map(|&key| reader.get(key).unwrap_or(NULL_ROW_KEY))
            .collect()
    }
}

/// A column whose value at an outer row key is the inner column's value at the redirected
/// key. A previous-value read uses the previous mapping and the inner column's previous
/// values. An outer key with no mapping reads as null.
///
/// Behavior: io.deephaven.engine.table.impl.sources.RedirectedColumnSource#get, #getPrev,
/// #fillChunk and #fillPrevChunk
pub struct RedirectedSource {
    redirection: Arc<dyn RowRedirection>,
    inner: Arc<dyn ColumnSource>,
}

impl RedirectedSource {
    pub fn new(redirection: Arc<dyn RowRedirection>, inner: Arc<dyn ColumnSource>) -> Self {
        Self { redirection, inner }
    }
}

impl ColumnSource for RedirectedSource {
    fn column_type(&self) -> &ColumnType {
        self.inner.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let outer: Vec<RowKey> = rows.iter().collect();
        self.chunk_by_keys(&outer, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let outer: Vec<RowKey> = rows.iter().collect();
        self.chunk_by_keys(&outer, true)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let inner = self.redirection.redirect(keys, use_prev);
        self.inner.chunk_by_keys(&inner, use_prev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracked::testing::ManualClock;
    use crate::{FlatSource, WritableSource};
    use arrow::array::{Array, Int32Array};

    fn ints(chunk: Result<ArrayRef, TableFailure>) -> Vec<Option<i32>> {
        let chunk = chunk.unwrap();
        let chunk = chunk.as_any().downcast_ref::<Int32Array>().unwrap();
        chunk.iter().collect()
    }

    fn int_array(values: impl IntoIterator<Item = i32>) -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(values))
    }

    #[test]
    fn put_and_remove_return_the_old_mapping() {
        let redirection = WritableRowRedirection::new();
        assert_eq!(redirection.put(5, 50), NULL_ROW_KEY);
        assert_eq!(redirection.put(5, 51), 50);
        assert_eq!(
            redirection.redirect(&[5, 6, NULL_ROW_KEY], false),
            [51, -1, -1]
        );
        assert_eq!(redirection.remove(5), 51);
        assert_eq!(redirection.remove(5), NULL_ROW_KEY);
        assert_eq!(redirection.redirect(&[5], false), [NULL_ROW_KEY]);
    }

    #[test]
    fn reads_inner_values_in_outer_key_order() {
        let inner = FlatSource::new(ColumnType::Int, int_array([100, 101, 102, 103])).unwrap();
        let redirection = Arc::new(WritableRowRedirection::new());
        // A descending mapping, as a reverse sort produces.
        for (outer, inner) in [(10, 3), (11, 1), (12, 0), (20, 1)] {
            redirection.put(outer, inner);
        }
        let source = RedirectedSource::new(redirection, Arc::new(inner));
        assert_eq!(source.column_type(), &ColumnType::Int);
        assert_eq!(
            ints(source.chunk(&RowSet::from_keys([10, 11, 12, 13, 20]))),
            vec![Some(103), Some(101), Some(100), None, Some(101)]
        );
        assert_eq!(
            ints(source.chunk_by_keys(&[20, NULL_ROW_KEY, 10], false)),
            vec![Some(101), None, Some(103)]
        );
    }

    #[test]
    fn previous_read_uses_previous_mapping_and_previous_inner_values() {
        let clock = ManualClock::idle_at(3);
        let inner = Arc::new(WritableSource::new(ColumnType::Int));
        inner
            .fill(&RowSet::flat(3), &int_array([100, 101, 102]))
            .unwrap();
        inner.start_tracking_prev(clock.clone());
        let redirection = Arc::new(WritableRowRedirection::new());
        redirection.put(7, 0);
        redirection.put(8, 1);
        redirection.start_tracking_prev(clock.clone());
        let source = RedirectedSource::new(redirection.clone(), inner.clone());
        let rows = RowSet::from_range(7, 9);

        clock.start_cycle();
        redirection.put(7, 2);
        redirection.remove(8);
        redirection.put(9, 1);
        inner
            .fill(&RowSet::from_keys([0, 1]), &int_array([-100, -101]))
            .unwrap();

        assert_eq!(ints(source.chunk(&rows)), vec![Some(102), None, Some(-101)]);
        assert_eq!(
            ints(source.prev_chunk(&rows)),
            vec![Some(100), Some(101), None]
        );
        assert_eq!(
            ints(source.chunk_by_keys(&[8, 7], true)),
            vec![Some(101), Some(100)]
        );

        clock.finish_cycle();
        assert_eq!(ints(source.prev_chunk(&rows)), ints(source.chunk(&rows)));
    }
}
