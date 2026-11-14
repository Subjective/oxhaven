//! A row set that remembers its value as of the end of the previous update cycle.

use oxhaven_contracts::rowset::RowSet;
use std::ops::Deref;

/// A row set plus its value as of the start of the current clock step.
///
/// The caller supplies the update graph's clock step (`ClockValue::step`) to every call that
/// depends on time, so this crate does not depend on the engine. Reads of the current value go
/// through `Deref`.
///
/// The previous value is captured lazily: the first [`write`](Self::write) in a step saves the
/// current value, and later writes in the same step leave that snapshot alone. A step in which
/// nothing was written has a previous value equal to the current one. Only the step number
/// matters, not the clock phase: a write made while the graph is idle belongs to the step of the
/// last cycle, exactly as in Java.
///
/// Java also hangs a lazily created `TrackingRowSet.Indexer` (the data index) off the tracking
/// row set. That belongs to the table layer and is not part of this type.
///
/// Behavior: io.deephaven.engine.rowset.impl.TrackingWritableRowSetImpl#checkAndGetPrev
#[derive(Clone, Debug)]
pub struct TrackingRowSet {
    current: RowSet,
    /// The value before the first write of `changed_step`. Unused while `changed_step` is `None`.
    prev: RowSet,
    /// The step of the most recent write, if any.
    changed_step: Option<u64>,
}

impl TrackingRowSet {
    /// Starts tracking `initial`. Until the first write, the previous value is `initial` too.
    ///
    /// Behavior: io.deephaven.engine.rowset.WritableRowSet#toTracking
    pub fn new(initial: RowSet) -> Self {
        Self {
            current: initial,
            prev: RowSet::empty(),
            changed_step: None,
        }
    }

    /// The current value.
    pub fn current(&self) -> &RowSet {
        &self.current
    }

    /// The value as of the start of `step`, which must be the clock's current step.
    ///
    /// Java's `sizePrev`, `firstRowKeyPrev`, `lastRowKeyPrev`, `getPrev`, `findPrev` and
    /// `copyPrev` are the corresponding `RowSet` methods on this value.
    ///
    /// Behavior: io.deephaven.engine.rowset.impl.TrackingWritableRowSetImpl#prev
    pub fn prev(&self, step: u64) -> &RowSet {
        if self.changed_step == Some(step) {
            &self.prev
        } else {
            &self.current
        }
    }

    /// Mutable access to the current value, for a change made during `step`, which must be the
    /// clock's current step. The first call in a step saves the previous value.
    ///
    /// Behavior: io.deephaven.engine.rowset.impl.TrackingWritableRowSetImpl#preMutationHook
    pub fn write(&mut self, step: u64) -> &mut RowSet {
        if self.changed_step != Some(step) {
            self.prev = self.current.clone();
            self.changed_step = Some(step);
        }
        &mut self.current
    }

    /// Forgets the saved previous value, so that the previous value is the current one until
    /// the next write.
    ///
    /// Behavior: io.deephaven.engine.rowset.impl.TrackingWritableRowSetImpl#initializePreviousValue
    pub fn initialize_previous_value(&mut self) {
        self.prev = RowSet::empty();
        self.changed_step = None;
    }

    /// Stops tracking and returns the current value.
    pub fn into_current(self) -> RowSet {
        self.current
    }
}

impl Deref for TrackingRowSet {
    type Target = RowSet;

    fn deref(&self) -> &RowSet {
        &self.current
    }
}

impl From<RowSet> for TrackingRowSet {
    fn from(initial: RowSet) -> Self {
        Self::new(initial)
    }
}
