//! Reading a column through a mapping from outer row keys to inner row keys.

use crate::tracked::{updating_step, Clock};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::collections::BTreeMap;
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

/// The least number of mappings a redirection is taken to have when it decides whether its
/// outer keys are dense: below it, a flat array costs a few kilobytes at most.
const DENSE_FLOOR: usize = 1024;

/// Whether `mappings` mappings whose largest outer key is `last` are kept in a flat array
/// indexed by outer key: when the array is at most four times as long as there are
/// mappings.
///
/// Java chooses once, when a join or sort makes its redirection: a flat array for a static
/// flat table, a sparse array source when that uses at least a quarter of each of its
/// blocks, and a hash map otherwise. The factor is the same; the forms are two here, and a
/// redirection changes form when its keys change.
///
/// Behavior: io.deephaven.engine.table.impl.JoinControl#getRedirectionType
fn is_dense(last: RowKey, mappings: usize) -> bool {
    let room = mappings.max(DENSE_FLOOR).saturating_mul(4);
    usize::try_from(last).is_ok_and(|last| last < room)
}

/// The mappings as they are now.
enum Current {
    /// The inner key of outer key `k` is `dense[k]`; `NULL_ROW_KEY` where `k` has none.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection
    Dense(Vec<RowKey>),
    /// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree, as an
    /// ordered map instead of a hash map
    Sparse(BTreeMap<RowKey, RowKey>),
}

impl Current {
    fn get(&self, outer: RowKey) -> RowKey {
        match self {
            Current::Dense(inner) => {
                let index = usize::try_from(outer).ok();
                index
                    .and_then(|index| inner.get(index))
                    .map_or(NULL_ROW_KEY, |inner| *inner)
            }
            Current::Sparse(inner) => inner.get(&outer).map_or(NULL_ROW_KEY, |inner| *inner),
        }
    }
}

struct Mappings {
    current: Current,
    /// The number of outer keys that have a mapping.
    size: usize,
    /// The size at which sparse mappings are next looked at for having become dense.
    look_at: usize,
    /// What each outer key changed in this cycle mapped to when the cycle began: the first
    /// write wins. `NULL_ROW_KEY` for a key that had no mapping.
    ///
    /// Behavior: the `checkpoint` of
    /// io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection
    prev: BTreeMap<RowKey, RowKey>,
    /// The step `prev` belongs to.
    prev_step: u64,
    clock: Option<Arc<dyn Clock>>,
}

impl Mappings {
    fn new(current: Current, size: usize) -> Self {
        Self {
            current,
            size,
            look_at: DENSE_FLOOR,
            prev: BTreeMap::new(),
            prev_step: 0,
            clock: None,
        }
    }

    /// Maps `outer`, which is not negative, to `inner`, or to nothing with `NULL_ROW_KEY`,
    /// and returns what it mapped to. Previous mappings are recorded by the rules of
    /// [`WritableSource`](crate::WritableSource): only while the clock is updating, and
    /// the record of an earlier cycle is dropped by the first write after it.
    fn set(&mut self, outer: RowKey, inner: RowKey) -> RowKey {
        let step = updating_step(self.clock.as_ref());
        if step != Some(self.prev_step) {
            self.prev.clear();
        }
        let old = self.replace(outer, inner);
        if let Some(step) = step {
            self.prev_step = step;
            self.prev.entry(outer).or_insert(old);
        }
        match (old == NULL_ROW_KEY, inner == NULL_ROW_KEY) {
            (true, false) => self.size += 1,
            (false, true) => self.size -= 1,
            _ => {}
        }
        if self.size >= self.look_at {
            self.look_at = self.size.saturating_mul(2);
            self.densify();
        }
        old
    }

    fn replace(&mut self, outer: RowKey, inner: RowKey) -> RowKey {
        match &mut self.current {
            Current::Dense(dense) => {
                let index = outer as usize;
                if index < dense.len() {
                    return std::mem::replace(&mut dense[index], inner);
                }
                if inner == NULL_ROW_KEY {
                    return NULL_ROW_KEY;
                }
                if is_dense(outer, self.size + 1) {
                    dense.resize(index + 1, NULL_ROW_KEY);
                    dense[index] = inner;
                    return NULL_ROW_KEY;
                }
                // The keys are no longer dense.
                let dense = std::mem::take(dense);
                let mapped = (0..).zip(dense).filter(|(_, inner)| *inner != NULL_ROW_KEY);
                let mut sparse: BTreeMap<RowKey, RowKey> = mapped.collect();
                sparse.insert(outer, inner);
                self.current = Current::Sparse(sparse);
                NULL_ROW_KEY
            }
            Current::Sparse(sparse) => {
                let old = if inner == NULL_ROW_KEY {
                    sparse.remove(&outer)
                } else {
                    sparse.insert(outer, inner)
                };
                old.unwrap_or(NULL_ROW_KEY)
            }
        }
    }

    /// Moves sparse mappings to a flat array if their keys have become dense.
    fn densify(&mut self) {
        let Current::Sparse(sparse) = &self.current else {
            return;
        };
        let Some((&last, _)) = sparse.last_key_value() else {
            return;
        };
        if !is_dense(last, self.size) {
            return;
        }
        let mut dense = vec![NULL_ROW_KEY; last as usize + 1];
        for (&outer, &inner) in sparse {
            dense[outer as usize] = inner;
        }
        self.current = Current::Dense(dense);
    }
}

/// A row redirection that can be changed, with previous mappings for the current cycle.
/// Previous mappings follow the same rules as the previous values of
/// [`WritableSource`](crate::WritableSource).
///
/// The mappings are kept in a flat array indexed by outer key while the outer keys are
/// dense (at most four array cells per mapping), and in an ordered map otherwise. The
/// redirection moves between the two by itself as keys come and go, so a caller never
/// chooses. Mappings changed during a cycle are remembered apart from both, as Java's
/// redirections remember them.
///
/// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection, and
/// io.deephaven.engine.table.impl.JoinRowRedirection#makeRowRedirection for the forms
pub struct WritableRowRedirection {
    map: RwLock<Mappings>,
}

impl WritableRowRedirection {
    /// An empty redirection that does not track previous mappings.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(Mappings::new(Current::Dense(Vec::new()), 0)),
        }
    }

    /// A redirection of the rows `outer`, in order, to `inner`; a row whose `inner` is
    /// `NULL_ROW_KEY` has no mapping. It does not track previous mappings. This is the way
    /// to make the redirection of a whole result at once: for the outer keys `0..n` the
    /// redirection is `inner` itself.
    ///
    /// # Panics
    /// If `inner` does not have one key for every row of `outer`.
    pub fn from_rows(outer: &RowSet, inner: Vec<RowKey>) -> Self {
        assert_eq!(outer.size(), inner.len() as u64, "one inner key per row");
        debug_assert!(inner.iter().all(|inner| *inner >= NULL_ROW_KEY));
        let size = inner.iter().filter(|inner| **inner != NULL_ROW_KEY).count();
        let last = outer.last_row_key().unwrap_or(NULL_ROW_KEY);
        let current = if outer.is_empty() {
            Current::Dense(Vec::new())
        } else if outer.first_row_key() == Some(0) && last + 1 == inner.len() as RowKey {
            Current::Dense(inner)
        } else if is_dense(last, size) {
            let mut dense = vec![NULL_ROW_KEY; last as usize + 1];
            for (outer, inner) in outer.iter().zip(inner) {
                dense[outer as usize] = inner;
            }
            Current::Dense(dense)
        } else {
            let mapped = outer.iter().zip(inner);
            Current::Sparse(mapped.filter(|(_, inner)| *inner != NULL_ROW_KEY).collect())
        };
        Self {
            map: RwLock::new(Mappings::new(current, size)),
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection#startTrackingPrevValues
    ///
    /// # Panics
    /// If called twice.
    pub fn start_tracking_prev(&self, clock: Arc<dyn Clock>) {
        let mut map = self.write();
        assert!(
            map.clock.is_none(),
            "start_tracking_prev called twice on one source"
        );
        map.clock = Some(clock);
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
        self.write().set(outer, inner)
    }

    /// Removes the mapping of `outer` and returns the inner key it mapped to, or
    /// `NULL_ROW_KEY`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.WritableRowRedirection#remove
    pub fn remove(&self, outer: RowKey) -> RowKey {
        if outer < 0 {
            // No key below zero has a mapping.
            return NULL_ROW_KEY;
        }
        self.write().set(outer, NULL_ROW_KEY)
    }

    /// Whether the mappings are in a flat array now.
    #[cfg(test)]
    fn is_flat_array(&self) -> bool {
        matches!(self.write().current, Current::Dense(_))
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Mappings> {
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
        let overlay = use_prev
            && !map.prev.is_empty()
            && updating_step(map.clock.as_ref()) == Some(map.prev_step);
        if overlay {
            let prev = |key: &RowKey| match map.prev.get(key) {
                Some(recorded) => *recorded,
                None => map.current.get(*key),
            };
            keys.iter().map(prev).collect()
        } else {
            keys.iter().map(|&key| map.current.get(key)).collect()
        }
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
