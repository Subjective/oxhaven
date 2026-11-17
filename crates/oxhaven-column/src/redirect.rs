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

    /// The inner key for each row of `rows`, in row-key order; otherwise as
    /// [`redirect`](Self::redirect). A redirection that can read a range of outer keys at
    /// once overrides it.
    fn redirect_rows(&self, rows: &RowSet, use_prev: bool) -> Vec<RowKey> {
        let keys: Vec<RowKey> = rows.iter().collect();
        self.redirect(&keys, use_prev)
    }
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
    /// The inner keys of `rows`, a range of the array at a time while the mappings are in
    /// one.
    fn get_rows(&self, rows: &RowSet) -> Vec<RowKey> {
        let Current::Dense(dense) = self else {
            return rows.iter().map(|outer| self.get(outer)).collect();
        };
        let mut inner = Vec::with_capacity(rows.size() as usize);
        for range in rows.ranges() {
            // Outer keys are never negative; those beyond the array have no mapping.
            let (start, end) = (range.first() as usize, range.last() as usize + 1);
            let stored = start.min(dense.len())..end.min(dense.len());
            let missing = (end - start) - stored.len();
            inner.extend_from_slice(&dense[stored]);
            inner.resize(inner.len() + missing, NULL_ROW_KEY);
        }
        inner
    }

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
    /// The number of writes until the mappings are next looked at for having become dense.
    /// It is set to the number of mappings each time, so that moving them all to an array
    /// costs a constant per write however keys come and go.
    writes_until_look: usize,
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
            writes_until_look: DENSE_FLOOR,
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
        self.writes_until_look -= 1;
        if self.writes_until_look == 0 {
            self.writes_until_look = self.size.max(DENSE_FLOOR);
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

    fn redirect_rows(&self, rows: &RowSet, use_prev: bool) -> Vec<RowKey> {
        let map = self.map.read().unwrap_or_else(PoisonError::into_inner);
        let mut inner = map.current.get_rows(rows);
        let overlay = use_prev
            && !map.prev.is_empty()
            && updating_step(map.clock.as_ref()) == Some(map.prev_step);
        if let (true, Some(first), Some(last)) =
            (overlay, rows.first_row_key(), rows.last_row_key())
        {
            // The mappings this cycle changed, where they are among `rows`.
            for (&outer, &recorded) in map.prev.range(first..=last) {
                if let Some(position) = rows.find(outer) {
                    inner[position as usize] = recorded;
                }
            }
        }
        inner
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
        let inner = self.redirection.redirect_rows(rows, false);
        self.inner.chunk_by_keys(&inner, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let inner = self.redirection.redirect_rows(rows, true);
        self.inner.chunk_by_keys(&inner, true)
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
    use oxhaven_contracts::rowset::RowRange;
    use std::collections::BTreeMap;

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

    /// Every key from 0 to `upto`, now and before.
    fn all(redirection: &WritableRowRedirection, upto: RowKey, use_prev: bool) -> Vec<RowKey> {
        let keys: Vec<RowKey> = (0..=upto).collect();
        redirection.redirect(&keys, use_prev)
    }

    #[test]
    fn dense_keys_are_kept_in_an_array_until_a_key_far_away_arrives() {
        let redirection = WritableRowRedirection::new();
        assert!(redirection.is_flat_array());
        for outer in [3, 0, 700, 2] {
            redirection.put(outer, outer + 100);
        }
        assert!(redirection.is_flat_array());
        // Removing a key that was never there, beyond the array, changes nothing.
        assert_eq!(redirection.remove(5000), NULL_ROW_KEY);
        assert_eq!(redirection.remove(-7), NULL_ROW_KEY);
        assert!(redirection.is_flat_array());

        let far = 1 << 40;
        assert_eq!(redirection.put(far, 9), NULL_ROW_KEY);
        assert!(!redirection.is_flat_array());
        assert_eq!(
            redirection.redirect(&[0, 1, 2, 3, 700, 701, far, far + 1], false),
            [100, -1, 102, 103, 800, -1, 9, -1]
        );
        assert_eq!(redirection.put(3, 4), 103);
        assert_eq!(redirection.remove(far), 9);
        assert_eq!(redirection.redirect(&[3, far], false), [4, -1]);
    }

    /// Keys put in descending order start out sparse, and are dense once enough of them
    /// have arrived.
    #[test]
    fn sparse_mappings_move_to_an_array_when_their_keys_have_become_dense() {
        let redirection = WritableRowRedirection::new();
        let last = 4 * DENSE_FLOOR as RowKey + 500;
        redirection.put(last, 1);
        assert!(!redirection.is_flat_array());
        for outer in (0..last).rev() {
            redirection.put(outer, outer + 7);
        }
        assert!(redirection.is_flat_array());
        let mut expected: Vec<RowKey> = (0..last).map(|outer| outer + 7).collect();
        expected.push(1);
        assert_eq!(all(&redirection, last, false), expected);
        assert_eq!(redirection.redirect(&[last + 1], false), [NULL_ROW_KEY]);
    }

    #[test]
    fn from_rows_maps_each_row_to_its_inner_key() {
        // The keys 0..n: the inner keys are the array.
        let flat = WritableRowRedirection::from_rows(&RowSet::flat(4), vec![9, -1, 7, 9]);
        assert!(flat.is_flat_array());
        assert_eq!(all(&flat, 4, false), [9, -1, 7, 9, -1]);
        assert_eq!(flat.put(1, 5), NULL_ROW_KEY);
        assert_eq!(flat.remove(0), 9);

        // Dense keys that do not start at zero.
        let rows = RowSet::from_keys([10, 20, 30]);
        let dense = WritableRowRedirection::from_rows(&rows, vec![1, -1, 3]);
        assert!(dense.is_flat_array());
        assert_eq!(
            dense.redirect(&[10, 11, 20, 30, 31], false),
            [1, -1, -1, 3, -1]
        );

        // Keys far apart.
        let far = 1 << 40;
        let rows = RowSet::from_keys([0, far, far + 5]);
        let sparse = WritableRowRedirection::from_rows(&rows, vec![4, -1, 6]);
        assert!(!sparse.is_flat_array());
        assert_eq!(
            sparse.redirect(&[0, far, far + 5, 1], false),
            [4, -1, 6, -1]
        );
        assert_eq!(sparse.remove(far), NULL_ROW_KEY);

        let empty = WritableRowRedirection::from_rows(&RowSet::empty(), Vec::new());
        assert_eq!(empty.redirect(&[0], false), [NULL_ROW_KEY]);
        assert_eq!(empty.put(0, 3), NULL_ROW_KEY);
    }

    #[test]
    #[should_panic(expected = "one inner key per row")]
    fn from_rows_needs_an_inner_key_for_every_row() {
        WritableRowRedirection::from_rows(&RowSet::flat(3), vec![1, 2]);
    }

    /// Previous mappings of an array: over several cycles, for keys the array grows to
    /// hold, and through the move to a map and back.
    #[test]
    fn an_array_keeps_previous_mappings_for_the_cycle() {
        let clock = ManualClock::idle_at(1);
        let redirection = WritableRowRedirection::from_rows(&RowSet::flat(3), vec![10, 11, 12]);
        redirection.start_tracking_prev(clock.clone());

        clock.start_cycle();
        redirection.put(0, 20);
        redirection.put(0, 30);
        redirection.remove(1);
        redirection.put(5, 15);
        assert!(redirection.is_flat_array());
        assert_eq!(all(&redirection, 5, false), [30, -1, 12, -1, -1, 15]);
        assert_eq!(all(&redirection, 5, true), [10, 11, 12, -1, -1, -1]);
        clock.finish_cycle();
        assert_eq!(all(&redirection, 5, true), [30, -1, 12, -1, -1, 15]);

        // The second cycle sees the first cycle's result as previous, and a key far away
        // moves the mappings to a map in the middle of it.
        clock.start_cycle();
        assert_eq!(all(&redirection, 5, true), [30, -1, 12, -1, -1, 15]);
        redirection.put(1, 21);
        redirection.remove(5);
        let far = 1 << 40;
        redirection.put(far, 1);
        assert!(!redirection.is_flat_array());
        redirection.put(2, 22);
        assert_eq!(all(&redirection, 5, false), [30, 21, 22, -1, -1, -1]);
        assert_eq!(all(&redirection, 5, true), [30, -1, 12, -1, -1, 15]);
        assert_eq!(redirection.redirect(&[far], true), [NULL_ROW_KEY]);
        assert_eq!(redirection.redirect(&[far], false), [1]);
        clock.finish_cycle();

        // Back to an array in the middle of a third cycle.
        clock.start_cycle();
        redirection.remove(far);
        redirection.put(0, 40);
        for outer in 10..2 * DENSE_FLOOR as RowKey {
            redirection.put(outer, outer);
        }
        assert!(redirection.is_flat_array());
        assert_eq!(all(&redirection, 5, false), [40, 21, 22, -1, -1, -1]);
        assert_eq!(all(&redirection, 5, true), [30, 21, 22, -1, -1, -1]);
        assert_eq!(redirection.redirect(&[far, 10, 11], true), [1, -1, -1]);
        assert_eq!(redirection.redirect(&[far, 10, 11], false), [-1, 10, 11]);
        clock.finish_cycle();
        assert_eq!(redirection.redirect(&[far, 0, 10], true), [-1, 40, 10]);
    }

    /// A write made while the clock is idle records nothing, as for a writable source.
    #[test]
    fn a_write_between_cycles_is_not_a_previous_mapping() {
        let clock = ManualClock::idle_at(1);
        let redirection = WritableRowRedirection::from_rows(&RowSet::flat(2), vec![10, 11]);
        redirection.start_tracking_prev(clock.clone());
        redirection.put(0, 20);
        assert_eq!(all(&redirection, 1, true), [20, 11]);
        clock.start_cycle();
        assert_eq!(all(&redirection, 1, true), [20, 11]);
        redirection.put(1, 21);
        assert_eq!(all(&redirection, 1, true), [20, 11]);
        clock.finish_cycle();
        // The record of that cycle is not used in the next one.
        clock.start_cycle();
        assert_eq!(all(&redirection, 1, true), [20, 21]);
    }

    /// Random puts and removes of keys near and far, over many cycles, against a plain
    /// map with a copy of itself from the start of the cycle.
    #[test]
    fn a_redirection_agrees_with_a_map_through_random_changes() {
        let clock = ManualClock::idle_at(1);
        let redirection = WritableRowRedirection::new();
        redirection.start_tracking_prev(clock.clone());
        let mut model: BTreeMap<RowKey, RowKey> = BTreeMap::new();
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut random = move |below: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % below
        };
        let mut forms = [0, 0];
        for cycle in 0..300 {
            let before = model.clone();
            clock.start_cycle();
            // Phases of dense keys, of keys spread wide, and of removals back to few keys.
            let (spread, removes) = match (cycle / 50) % 3 {
                0 => (3000, 1),
                1 => (1 << 30, 2),
                _ => (3000, 8),
            };
            for _ in 0..40 {
                let outer = random(spread) as RowKey;
                let expected = if random(10) < removes {
                    (redirection.remove(outer), model.remove(&outer))
                } else {
                    let inner = random(1000) as RowKey;
                    (redirection.put(outer, inner), model.insert(outer, inner))
                };
                assert_eq!(expected.0, expected.1.unwrap_or(NULL_ROW_KEY));
            }
            if cycle % 50 == 49 && spread > 3000 {
                // Drop the keys far away, so that the rest can be dense again.
                let far: Vec<RowKey> = model.range(3000..).map(|(outer, _)| *outer).collect();
                for outer in far {
                    assert_eq!(redirection.remove(outer), model.remove(&outer).unwrap());
                }
            }
            let mut keys: Vec<RowKey> = model.keys().chain(before.keys()).copied().collect();
            keys.extend([0, 1, 2999, 3000, (1 << 30) - 1, NULL_ROW_KEY]);
            let read = |map: &BTreeMap<RowKey, RowKey>| -> Vec<RowKey> {
                let inner = |key| map.get(key).map_or(NULL_ROW_KEY, |inner| *inner);
                keys.iter().map(inner).collect()
            };
            assert_eq!(redirection.redirect(&keys, false), read(&model), "{cycle}");
            assert_eq!(redirection.redirect(&keys, true), read(&before), "{cycle}");
            clock.finish_cycle();
            assert_eq!(redirection.redirect(&keys, true), read(&model), "{cycle}");
            forms[usize::from(redirection.is_flat_array())] += 1;
        }
        // Both forms were in use.
        assert!(forms[0] > 20 && forms[1] > 20, "{forms:?}");
    }

    /// Reading the rows of a row set at once gives what reading their keys one by one
    /// gives, now and before, in both forms, with ranges inside, across and beyond the
    /// array, and through a source that reads its inner column by the redirected keys.
    #[test]
    fn reading_a_row_set_agrees_with_reading_its_keys() {
        let clock = ManualClock::idle_at(1);
        let redirection = Arc::new(WritableRowRedirection::from_rows(
            &RowSet::flat(3000),
            (0..3000).map(|k| (k * 7) % 1000).collect(),
        ));
        redirection.start_tracking_prev(clock.clone());
        let inner = FlatSource::new(ColumnType::Int, int_array(0..1000)).unwrap();
        let source = RedirectedSource::new(redirection.clone(), Arc::new(inner));
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut random = move |below: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % below
        };
        let mut forms = [0, 0];
        for cycle in 0..60 {
            clock.start_cycle();
            let spread = if cycle % 20 < 10 { 4000 } else { 1 << 30 };
            for _ in 0..50 {
                let outer = random(spread) as RowKey;
                if random(4) == 0 {
                    redirection.remove(outer);
                } else {
                    redirection.put(outer, random(1000) as RowKey);
                }
            }
            if cycle % 20 == 19 {
                let far: Vec<RowKey> = (0..60).map(|_| random(1 << 30) as RowKey).collect();
                for outer in far.into_iter().filter(|outer| *outer >= 4000) {
                    redirection.remove(outer);
                }
            }
            for _ in 0..5 {
                let ranges = (0..1 + random(6)).map(|_| {
                    let first = random(5000) as RowKey;
                    RowRange::new(first, first + random(300) as RowKey)
                });
                let rows = RowSet::from_ranges(ranges);
                let keys: Vec<RowKey> = rows.iter().collect();
                for use_prev in [false, true] {
                    let one_by_one = redirection.redirect(&keys, use_prev);
                    assert_eq!(redirection.redirect_rows(&rows, use_prev), one_by_one);
                    let read = |use_prev| match use_prev {
                        false => ints(source.chunk(&rows)),
                        true => ints(source.prev_chunk(&rows)),
                    };
                    let expected: Vec<Option<i32>> = one_by_one
                        .iter()
                        .map(|&inner| (inner != NULL_ROW_KEY).then_some(inner as i32))
                        .collect();
                    assert_eq!(read(use_prev), expected, "{cycle}");
                }
            }
            clock.finish_cycle();
            forms[usize::from(redirection.is_flat_array())] += 1;
        }
        assert!(forms[0] > 5 && forms[1] > 5, "{forms:?}");
    }
}
