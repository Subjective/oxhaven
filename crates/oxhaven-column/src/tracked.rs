//! A row-keyed map that remembers, for the current cycle, what each changed key held when the
//! cycle began. Shared by the writable column source and the writable row redirection.

use oxhaven_contracts::graph::{ClockPhase, ClockValue, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Where a writable source or redirection learns the current logical time.
///
/// An update graph is a clock. Anything else that can report a step and phase works too, so
/// this crate does not depend on the engine.
pub trait Clock: Send + Sync {
    fn now(&self) -> ClockValue;
}

impl Clock for Arc<dyn UpdateGraph> {
    fn now(&self) -> ClockValue {
        self.clock()
    }
}

/// The step of `clock`, if it has one and is updating: the step whose previous values are
/// being recorded.
pub(crate) fn updating_step(clock: Option<&Arc<dyn Clock>>) -> Option<u64> {
    match clock?.now() {
        ClockValue {
            step,
            phase: ClockPhase::Updating,
        } => Some(step),
        _ => None,
    }
}

/// The least number of values a map is taken to have when it decides whether its keys are
/// dense: below it, a flat array costs a few kilobytes at most.
pub(crate) const DENSE_FLOOR: usize = 1024;

/// Whether `values` values whose largest key is `last` are kept in a flat array indexed by
/// key: when the array is at most four times as long as there are values.
///
/// Java chooses once, when a join or sort makes its redirection: a flat array for a static
/// flat table, a sparse array source when that uses at least a quarter of each of its
/// blocks, and a hash map otherwise. The factor is the same; the forms are two here, and a
/// map changes form when its keys change.
///
/// Behavior: io.deephaven.engine.table.impl.JoinControl#getRedirectionType
pub(crate) fn is_dense(last: RowKey, values: usize) -> bool {
    let room = values.max(DENSE_FLOOR).saturating_mul(4);
    usize::try_from(last).is_ok_and(|last| last < room)
}

/// The values as they are now.
enum Current<V> {
    /// The value of key `k` is `dense[k]`, the absent value where `k` has none. A read of
    /// a range of keys is a copy of a slice, and of one key an index.
    Dense(Vec<V>),
    Sparse(BTreeMap<RowKey, V>),
}

/// Current values by row key, plus the values that keys changed in the current cycle held at
/// the end of the previous cycle.
///
/// The current values are in a flat array indexed by key while the keys are dense (at most
/// four array cells per value), and in an ordered map otherwise; the map changes form by
/// itself as keys come and go. One value, given to [`Tracked::new`], stands for "no value"
/// in the array and is never stored for a key.
///
/// Previous values are recorded only while a clock is attached and its phase is updating.
/// They stop being used the moment the clock leaves that step, without any end-of-cycle
/// callback: a stale record is ignored by reads and discarded by the next write.
///
/// Behavior: io.deephaven.engine.table.impl.sources.IntegerSparseArraySource#shouldRecordPrevious,
/// #shouldUsePrevious and #commitUpdates. One difference follows the `ColumnSource` contract
/// rather than Java: a write made while the clock is idle records nothing, so previous and
/// current values agree whenever the graph is idle. Java records such a write and keeps the
/// old value readable until the end of the next cycle.
pub(crate) struct Tracked<V> {
    current: Current<V>,
    /// What a key without a value holds in the flat array.
    absent: V,
    /// The number of keys that have a value.
    size: usize,
    /// The number of writes until the form is next looked at. It is set to the number of
    /// values each time, so that changing form costs a constant per write however keys come
    /// and go.
    writes_until_look: usize,
    /// First-write-wins record of what each key held before this cycle changed it. `None`
    /// means the key was absent.
    prev: BTreeMap<RowKey, Option<V>>,
    /// The step `prev` belongs to.
    prev_step: u64,
    clock: Option<Arc<dyn Clock>>,
}

impl<V: Copy + PartialEq> Tracked<V> {
    /// An empty map; `absent` must never be put as a value.
    pub(crate) fn new(absent: V) -> Self {
        Self {
            current: Current::Dense(Vec::new()),
            absent,
            size: 0,
            writes_until_look: DENSE_FLOOR,
            prev: BTreeMap::new(),
            prev_step: 0,
            clock: None,
        }
    }

    /// Behavior: io.deephaven.engine.table.ColumnSource#startTrackingPrevValues, including
    /// the failure on a second call.
    pub(crate) fn start_tracking_prev(&mut self, clock: Arc<dyn Clock>) {
        assert!(
            self.clock.is_none(),
            "start_tracking_prev called twice on one source"
        );
        self.clock = Some(clock);
    }

    /// The number of keys that have a value now.
    pub(crate) fn len(&self) -> usize {
        self.size
    }

    /// The current values, in key order.
    pub(crate) fn values(&self) -> Vec<V> {
        match &self.current {
            Current::Dense(dense) => {
                let held = dense.iter().filter(|value| **value != self.absent);
                held.copied().collect()
            }
            Current::Sparse(sparse) => sparse.values().copied().collect(),
        }
    }

    /// Whether the current values are in a flat array now.
    #[cfg(test)]
    pub(crate) fn is_flat_array(&self) -> bool {
        matches!(self.current, Current::Dense(_))
    }

    /// Whether any previous value is recorded, stale or not.
    pub(crate) fn has_prev_record(&self) -> bool {
        !self.prev.is_empty()
    }

    fn updating_step(&self) -> Option<u64> {
        updating_step(self.clock.as_ref())
    }

    /// Starts one mutating operation: drops a record left by an earlier cycle and decides
    /// whether this operation records previous values.
    pub(crate) fn write(&mut self) -> Writer<'_, V> {
        let step = self.updating_step();
        if step != Some(self.prev_step) {
            self.prev.clear();
        }
        if let Some(step) = step {
            self.prev_step = step;
        }
        Writer {
            record: step.is_some(),
            map: self,
        }
    }

    /// A read view of current values, or of previous values when `use_prev` is set.
    pub(crate) fn read(&self, use_prev: bool) -> Reader<'_, V> {
        let overlay =
            use_prev && !self.prev.is_empty() && self.updating_step() == Some(self.prev_step);
        Reader {
            current: &self.current,
            absent: self.absent,
            prev: overlay.then_some(&self.prev),
        }
    }

    /// Sets (`Some`) or clears (`None`) the current value at `key` and returns what it held.
    fn replace(&mut self, key: RowKey, value: Option<V>) -> Option<V> {
        let absent = self.absent;
        debug_assert!(value != Some(absent), "the absent value was put");
        match &mut self.current {
            Current::Dense(dense) => {
                let index = usize::try_from(key).ok();
                if let Some(cell) = index.and_then(|index| dense.get_mut(index)) {
                    let old = std::mem::replace(cell, value.unwrap_or(absent));
                    return (old != absent).then_some(old);
                }
                let value = value?;
                if let (Some(index), true) = (index, is_dense(key, self.size + 1)) {
                    dense.resize(index + 1, absent);
                    dense[index] = value;
                    return None;
                }
                // The keys are no longer dense.
                let dense = std::mem::take(dense);
                let held = (0..).zip(dense).filter(|(_, value)| *value != absent);
                let mut sparse: BTreeMap<RowKey, V> = held.collect();
                sparse.insert(key, value);
                self.current = Current::Sparse(sparse);
                None
            }
            Current::Sparse(sparse) => match value {
                Some(value) => sparse.insert(key, value),
                None => sparse.remove(&key),
            },
        }
    }

    /// Moves the values to the form their keys call for: sparse values whose keys have
    /// become dense to a flat array, and a flat array most of whose cells have become
    /// absent to an ordered map.
    fn look(&mut self) {
        let absent = self.absent;
        match &mut self.current {
            Current::Sparse(sparse) => {
                let Some((&last, _)) = sparse.last_key_value() else {
                    self.current = Current::Dense(Vec::new());
                    return;
                };
                if is_dense(last, self.size) {
                    let mut dense = vec![absent; last as usize + 1];
                    for (&key, &value) in sparse.iter() {
                        dense[key as usize] = value;
                    }
                    self.current = Current::Dense(dense);
                }
            }
            Current::Dense(dense) => {
                let held = dense.iter().rposition(|value| *value != absent);
                dense.truncate(held.map_or(0, |last| last + 1));
                if dense.len() > self.size.max(DENSE_FLOOR).saturating_mul(4) {
                    let dense = std::mem::take(dense);
                    let held = (0..).zip(dense).filter(|(_, value)| *value != absent);
                    self.current = Current::Sparse(held.collect());
                }
            }
        }
    }
}

pub(crate) struct Writer<'a, V> {
    map: &'a mut Tracked<V>,
    record: bool,
}

impl<V: Copy + PartialEq> Writer<'_, V> {
    pub(crate) fn map(&self) -> &Tracked<V> {
        self.map
    }

    /// Sets (`Some`) or clears (`None`) the value at `key` and returns what it held.
    pub(crate) fn put(&mut self, key: RowKey, value: Option<V>) -> Option<V> {
        let old = self.map.replace(key, value);
        if self.record {
            self.map.prev.entry(key).or_insert(old);
        }
        match (old.is_some(), value.is_some()) {
            (false, true) => self.map.size += 1,
            (true, false) => self.map.size -= 1,
            _ => {}
        }
        self.map.writes_until_look -= 1;
        if self.map.writes_until_look == 0 {
            self.map.writes_until_look = self.map.size.max(DENSE_FLOOR);
            self.map.look();
        }
        old
    }

    /// Replaces every current value, in key order, without recording previous values. For
    /// re-addressing storage; only valid while no previous value is recorded.
    pub(crate) fn replace_all(&mut self, values: impl Iterator<Item = V>) {
        debug_assert!(self.map.prev.is_empty());
        let absent = self.map.absent;
        match &mut self.map.current {
            Current::Dense(dense) => {
                let held = dense.iter_mut().filter(|value| **value != absent);
                for (cell, value) in held.zip(values) {
                    *cell = value;
                }
            }
            Current::Sparse(sparse) => {
                for (cell, value) in sparse.values_mut().zip(values) {
                    *cell = value;
                }
            }
        }
    }
}

pub(crate) struct Reader<'a, V> {
    current: &'a Current<V>,
    absent: V,
    prev: Option<&'a BTreeMap<RowKey, Option<V>>>,
}

impl<V: Copy + PartialEq> Reader<'_, V> {
    /// The value at `key`, the absent value if it has none.
    pub(crate) fn get(&self, key: RowKey) -> V {
        if let Some(&recorded) = self.prev.and_then(|p| p.get(&key)) {
            return recorded.unwrap_or(self.absent);
        }
        match self.current {
            Current::Dense(dense) => {
                let index = usize::try_from(key).ok();
                index
                    .and_then(|index| dense.get(index))
                    .map_or(self.absent, |value| *value)
            }
            Current::Sparse(sparse) => sparse.get(&key).map_or(self.absent, |value| *value),
        }
    }

    /// The flat array of the values when they are in one and this view lays no previous
    /// value over it: the value of key `k` is at index `k`, and a key at or beyond its end
    /// has none.
    pub(crate) fn flat_array(&self) -> Option<&[V]> {
        match (self.current, self.prev) {
            (Current::Dense(dense), None) => Some(dense),
            _ => None,
        }
    }

    /// The value at each key of `rows`, in key order; the absent value where a key has none.
    pub(crate) fn values_of(&self, rows: &RowSet) -> Vec<V> {
        let mut values = Vec::with_capacity(rows.size() as usize);
        match self.current {
            Current::Dense(dense) => {
                for range in rows.ranges() {
                    // Row keys are never negative; those beyond the array have no value.
                    let (start, end) = (range.first() as usize, range.last() as usize + 1);
                    let stored = start.min(dense.len())..end.min(dense.len());
                    let missing = (end - start) - stored.len();
                    values.extend_from_slice(&dense[stored]);
                    values.resize(values.len() + missing, self.absent);
                }
                // The values this cycle changed, where they are among `rows`.
                let ends = rows.first_row_key().zip(rows.last_row_key());
                if let Some((prev, (first, last))) = self.prev.zip(ends) {
                    for (&key, &recorded) in prev.range(first..=last) {
                        if let Some(position) = rows.find(key) {
                            values[position as usize] = recorded.unwrap_or(self.absent);
                        }
                    }
                }
            }
            Current::Sparse(sparse) => {
                values.resize(rows.size() as usize, self.absent);
                let mut offset = 0;
                for range in rows.ranges() {
                    let (first, last) = (range.first(), range.last());
                    merged(sparse, self.prev, first, last, |key, value| {
                        values[offset + (key - first) as usize] = value;
                    });
                    offset += range.size() as usize;
                }
            }
        }
        values
    }
}

/// Calls `f(key, value)` in key order for each key in `first..=last` that has a value in
/// `current` with the records of `prev` laid over it.
fn merged<V: Copy>(
    current: &BTreeMap<RowKey, V>,
    prev: Option<&BTreeMap<RowKey, Option<V>>>,
    first: RowKey,
    last: RowKey,
    mut f: impl FnMut(RowKey, V),
) {
    let mut current = current.range(first..=last).peekable();
    let mut prev = prev
        .into_iter()
        .flat_map(|p| p.range(first..=last))
        .peekable();
    loop {
        let next_prev = prev.peek().map(|(&k, _)| k);
        let next_current = current.peek().map(|(&k, _)| k);
        match (next_prev, next_current) {
            (Some(p), c) if c.is_none_or(|c| p <= c) => {
                if c == Some(p) {
                    current.next();
                }
                if let Some((&k, &Some(v))) = prev.next() {
                    f(k, v);
                }
            }
            (_, Some(_)) => {
                if let Some((&k, &v)) = current.next() {
                    f(k, v);
                }
            }
            _ => break,
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex;

    /// A clock the test moves by hand.
    pub(crate) struct ManualClock(Mutex<ClockValue>);

    impl ManualClock {
        pub(crate) fn idle_at(step: u64) -> Arc<Self> {
            Arc::new(Self(Mutex::new(ClockValue {
                step,
                phase: ClockPhase::Idle,
            })))
        }

        /// Starts the next cycle.
        pub(crate) fn start_cycle(&self) {
            let mut value = self.0.lock().unwrap();
            value.step += 1;
            value.phase = ClockPhase::Updating;
        }

        /// Ends the current cycle.
        pub(crate) fn finish_cycle(&self) {
            self.0.lock().unwrap().phase = ClockPhase::Idle;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> ClockValue {
            *self.0.lock().unwrap()
        }
    }
}
