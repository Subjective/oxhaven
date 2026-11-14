//! The row keys of a cross join result and the column sources that read through them.
//!
//! A result row key is a left row key shifted left by a number of bits, with the low bits
//! naming one right row. Without match columns the low bits are the right row key itself;
//! with match columns they are the position of the right row among the right rows of the
//! left row's key. The number of bits only grows. Columns of the left table read the left
//! row `key >> bits`; added columns read the right row the low bits name.
//!
//! Behavior: the class comment of io.deephaven.engine.table.impl.CrossJoinHelper,
//! io.deephaven.engine.table.impl.CrossJoinShiftState,
//! io.deephaven.engine.table.impl.sources.BitShiftingColumnSource,
//! io.deephaven.engine.table.impl.sources.BitMaskingColumnSource and
//! io.deephaven.engine.table.impl.sources.CrossJoinRightColumnSource

use oxhaven_column::{Clock, RowRedirection};
use oxhaven_contracts::graph::{ClockPhase, ClockValue};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// The number of bits needed to write `last_key`; 0 for 0 and for an empty table (-1).
///
/// Behavior: io.deephaven.engine.table.impl.CrossJoinShiftState#getMinBits
pub(super) fn min_bits(last_key: RowKey) -> u32 {
    64 - (last_key.max(0) as u64).leading_zeros()
}

/// The step of the cycle `clock` is updating, if it is updating.
fn updating_step(clock: &Option<Arc<dyn Clock>>) -> Option<u64> {
    match clock.as_ref()?.now() {
        ClockValue {
            step,
            phase: ClockPhase::Updating,
        } => Some(step),
        _ => None,
    }
}

/// A value with its value as of the end of the previous cycle.
pub(super) struct Versioned<T> {
    current: T,
    /// The step of the cycle that first changed the value, and the value before that.
    prev: Option<(u64, T)>,
}

impl<T: Clone> Versioned<T> {
    fn new(value: T) -> Self {
        Self {
            current: value,
            prev: None,
        }
    }

    fn get(&self, use_prev: bool, clock: &Option<Arc<dyn Clock>>) -> &T {
        match (&self.prev, use_prev) {
            (Some((step, value)), true) if updating_step(clock) == Some(*step) => value,
            _ => &self.current,
        }
    }

    fn set(&mut self, value: T, clock: &Option<Arc<dyn Clock>>) {
        if let Some(step) = updating_step(clock) {
            if self.prev.as_ref().map(|(s, _)| *s) != Some(step) {
                self.prev = Some((step, self.current.clone()));
            }
        }
        self.current = value;
    }
}

/// A map with, for the current cycle, what each key it changed held before.
pub(super) struct VersionedMap<V> {
    current: BTreeMap<RowKey, V>,
    /// First-write-wins: what a key held at the end of the previous cycle.
    prev: BTreeMap<RowKey, Option<V>>,
    prev_step: Option<u64>,
}

impl<V: Clone> VersionedMap<V> {
    fn new() -> Self {
        Self {
            current: BTreeMap::new(),
            prev: BTreeMap::new(),
            prev_step: None,
        }
    }

    pub(super) fn get(
        &self,
        key: RowKey,
        use_prev: bool,
        clock: &Option<Arc<dyn Clock>>,
    ) -> Option<&V> {
        if use_prev && self.prev_step.is_some() && updating_step(clock) == self.prev_step {
            if let Some(before) = self.prev.get(&key) {
                return before.as_ref();
            }
        }
        self.current.get(&key)
    }

    fn put(&mut self, key: RowKey, value: Option<V>, clock: &Option<Arc<dyn Clock>>) {
        let step = updating_step(clock);
        if step != self.prev_step {
            self.prev.clear();
            self.prev_step = step;
        }
        let old = match value {
            Some(value) => self.current.insert(key, value),
            None => self.current.remove(&key),
        };
        if step.is_some() {
            self.prev.entry(key).or_insert(old);
        }
    }
}

/// What the low bits of a result row key name.
pub(super) enum Inner {
    /// The right row key itself: a join without match columns.
    RightKey,
    /// The position among the right rows of the left row's key.
    Position {
        /// Left row key to the key's slot.
        left_slot: VersionedMap<RowKey>,
        /// Slot to the right rows that have its key.
        right_rows: VersionedMap<RowSet>,
    },
}

pub(super) struct LayoutState {
    bits: Versioned<u32>,
    pub(super) inner: Inner,
    clock: Option<Arc<dyn Clock>>,
}

impl LayoutState {
    pub(super) fn bits(&self, use_prev: bool) -> u32 {
        *self.bits.get(use_prev, &self.clock)
    }

    /// Behavior: io.deephaven.engine.table.impl.CrossJoinShiftState#setNumShiftBitsAndUpdatePrev
    pub(super) fn set_bits(&mut self, bits: u32) {
        if bits != self.bits(false) {
            self.bits.set(bits, &self.clock);
        }
    }

    pub(super) fn set_left_slot(&mut self, left: RowKey, slot: Option<RowKey>) {
        let clock = &self.clock;
        if let Inner::Position { left_slot, .. } = &mut self.inner {
            left_slot.put(left, slot, clock);
        }
    }

    pub(super) fn set_right_rows(&mut self, slot: RowKey, rows: RowSet) {
        let clock = &self.clock;
        if let Inner::Position { right_rows, .. } = &mut self.inner {
            right_rows.put(slot, Some(rows), clock);
        }
    }

    pub(super) fn left_slot(&self, left: RowKey, use_prev: bool) -> Option<RowKey> {
        match &self.inner {
            Inner::Position { left_slot, .. } => {
                left_slot.get(left, use_prev, &self.clock).copied()
            }
            Inner::RightKey => None,
        }
    }

    pub(super) fn right_rows(&self, slot: RowKey, use_prev: bool) -> Option<&RowSet> {
        match &self.inner {
            Inner::Position { right_rows, .. } => right_rows.get(slot, use_prev, &self.clock),
            Inner::RightKey => None,
        }
    }

    /// The right row key of each result row key, as of now or of the previous cycle.
    fn right_keys(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        let bits = self.bits(use_prev);
        let mask = (1i64 << bits) - 1;
        keys.iter()
            .map(|&key| {
                if key < 0 {
                    return NULL_ROW_KEY;
                }
                match &self.inner {
                    Inner::RightKey => key & mask,
                    Inner::Position { .. } => self
                        .left_slot(key >> bits, use_prev)
                        .and_then(|slot| self.right_rows(slot, use_prev))
                        .and_then(|rows| rows.get((key & mask) as u64))
                        .unwrap_or(NULL_ROW_KEY),
                }
            })
            .collect()
    }
}

/// The state the result's columns read, shared by the operator that changes it.
pub(super) struct Layout {
    state: RwLock<LayoutState>,
}

impl Layout {
    /// A layout of `bits` bits whose previous state follows `clock`, or that has none when
    /// the result is static.
    pub(super) fn new(bits: u32, keyed: bool, clock: Option<Arc<dyn Clock>>) -> Arc<Self> {
        let inner = if keyed {
            Inner::Position {
                left_slot: VersionedMap::new(),
                right_rows: VersionedMap::new(),
            }
        } else {
            Inner::RightKey
        };
        Arc::new(Self {
            state: RwLock::new(LayoutState {
                bits: Versioned::new(bits),
                inner,
                clock,
            }),
        })
    }

    pub(super) fn read(&self) -> RwLockReadGuard<'_, LayoutState> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn write(&self) -> RwLockWriteGuard<'_, LayoutState> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Result row key to left row key: the high bits.
///
/// Behavior: io.deephaven.engine.table.impl.sources.BitShiftingColumnSource
pub(super) struct LeftRows(pub(super) Arc<Layout>);

impl RowRedirection for LeftRows {
    fn redirect(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        let bits = self.0.read().bits(use_prev);
        keys.iter()
            .map(|&key| if key < 0 { NULL_ROW_KEY } else { key >> bits })
            .collect()
    }
}

/// Result row key to right row key: the low bits, read as a right row key or as a position
/// among the right rows of the left row's key.
///
/// Behavior: io.deephaven.engine.table.impl.sources.BitMaskingColumnSource and
/// io.deephaven.engine.table.impl.sources.CrossJoinRightColumnSource
pub(super) struct RightRows(pub(super) Arc<Layout>);

impl RowRedirection for RightRows {
    fn redirect(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        self.0.read().right_keys(keys, use_prev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_bits_is_the_bit_length_of_the_last_key() {
        assert_eq!(
            [-1, 0, 1, 2, 3, 4, 1023, 1024, i64::MAX].map(min_bits),
            [0, 0, 1, 2, 2, 3, 10, 11, 63]
        );
    }

    #[test]
    fn a_static_layout_splits_keys_at_its_bits() {
        let layout = Layout::new(3, false, None);
        assert_eq!(
            LeftRows(layout.clone()).redirect(&[0, 7, 8, 21, -1], false),
            [0, 0, 1, 2, -1]
        );
        assert_eq!(
            RightRows(layout).redirect(&[0, 7, 8, 21, -1], true),
            [0, 7, 0, 5, -1]
        );
    }

    #[test]
    fn a_keyed_layout_reads_positions_in_the_slot() {
        let layout = Layout::new(2, true, None);
        {
            let mut state = layout.write();
            state.set_left_slot(1, Some(9));
            state.set_right_rows(9, RowSet::from_keys([5, 40, 41]));
        }
        // Left row 1 starts at 4; positions 0, 1, 2 and one beyond the group.
        let keys = [4, 5, 6, 7, 0];
        assert_eq!(
            RightRows(layout.clone()).redirect(&keys, false),
            [5, 40, 41, -1, -1]
        );
        assert_eq!(LeftRows(layout).redirect(&keys, false), [1, 1, 1, 1, 0]);
    }
}
