//! A row-keyed map that remembers, for the current cycle, what each changed key held when the
//! cycle began. Shared by the writable column source and the writable row redirection.

use oxhaven_contracts::graph::{ClockPhase, ClockValue, UpdateGraph};
use oxhaven_contracts::rowset::RowKey;
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

/// Current values by row key, plus the values that keys changed in the current cycle held at
/// the end of the previous cycle.
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
    current: BTreeMap<RowKey, V>,
    /// First-write-wins record of what each key held before this cycle changed it. `None`
    /// means the key was absent.
    prev: BTreeMap<RowKey, Option<V>>,
    /// The step `prev` belongs to.
    prev_step: u64,
    clock: Option<Arc<dyn Clock>>,
}

impl<V: Copy> Tracked<V> {
    pub(crate) fn new() -> Self {
        Self {
            current: BTreeMap::new(),
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

    pub(crate) fn current(&self) -> &BTreeMap<RowKey, V> {
        &self.current
    }

    /// Whether any previous value is recorded, stale or not.
    pub(crate) fn has_prev_record(&self) -> bool {
        !self.prev.is_empty()
    }

    fn updating_step(&self) -> Option<u64> {
        match self.clock.as_ref()?.now() {
            ClockValue {
                step,
                phase: ClockPhase::Updating,
            } => Some(step),
            _ => None,
        }
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
            prev: overlay.then_some(&self.prev),
        }
    }
}

pub(crate) struct Writer<'a, V> {
    map: &'a mut Tracked<V>,
    record: bool,
}

impl<V: Copy> Writer<'_, V> {
    pub(crate) fn map(&self) -> &Tracked<V> {
        self.map
    }

    /// Sets (`Some`) or clears (`None`) the value at `key` and returns what it held.
    pub(crate) fn put(&mut self, key: RowKey, value: Option<V>) -> Option<V> {
        let old = match value {
            Some(v) => self.map.current.insert(key, v),
            None => self.map.current.remove(&key),
        };
        if self.record {
            self.map.prev.entry(key).or_insert(old);
        }
        old
    }

    /// Replaces every current value, in key order, without recording previous values. For
    /// re-addressing storage; only valid while no previous value is recorded.
    pub(crate) fn replace_all(&mut self, values: impl Iterator<Item = V>) {
        debug_assert!(self.map.prev.is_empty());
        for (slot, value) in self.map.current.values_mut().zip(values) {
            *slot = value;
        }
    }
}

pub(crate) struct Reader<'a, V> {
    current: &'a BTreeMap<RowKey, V>,
    prev: Option<&'a BTreeMap<RowKey, Option<V>>>,
}

impl<V: Copy> Reader<'_, V> {
    pub(crate) fn get(&self, key: RowKey) -> Option<V> {
        if let Some(&recorded) = self.prev.and_then(|p| p.get(&key)) {
            return recorded;
        }
        self.current.get(&key).copied()
    }

    /// Calls `f(key, value)` in key order for each key in `first..=last` that has a value.
    pub(crate) fn for_each_in(&self, first: RowKey, last: RowKey, mut f: impl FnMut(RowKey, V)) {
        let mut current = self.current.range(first..=last).peekable();
        let mut prev = self
            .prev
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
