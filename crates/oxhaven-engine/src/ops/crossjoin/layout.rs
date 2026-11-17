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

use oxhaven_column::{Clock, RowRedirection, WritableRowRedirection};
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

/// A value for each slot `0, 1, 2, ...`, with what each slot it changed in the current
/// cycle held before (first write wins). A slot past the end has no value.
pub(super) struct VersionedVec<V> {
    current: Vec<V>,
    /// What a slot held at the end of the previous cycle; `None` for a slot past the end.
    prev: BTreeMap<usize, Option<V>>,
    prev_step: Option<u64>,
}

impl<V: Clone + Default> VersionedVec<V> {
    fn new() -> Self {
        Self {
            current: Vec::new(),
            prev: BTreeMap::new(),
            prev_step: None,
        }
    }

    /// Slot `s` holds `values[s]`, with no change recorded.
    pub(super) fn from_vec(values: Vec<V>) -> Self {
        Self {
            current: values,
            ..Self::new()
        }
    }

    pub(super) fn get(
        &self,
        slot: usize,
        use_prev: bool,
        clock: &Option<Arc<dyn Clock>>,
    ) -> Option<&V> {
        if use_prev && self.prev_step.is_some() && updating_step(clock) == self.prev_step {
            if let Some(before) = self.prev.get(&slot) {
                return before.as_ref();
            }
        }
        self.current.get(slot)
    }

    fn put(&mut self, slot: usize, value: V, clock: &Option<Arc<dyn Clock>>) {
        let step = updating_step(clock);
        if step != self.prev_step {
            self.prev.clear();
            self.prev_step = step;
        }
        let old = if slot < self.current.len() {
            Some(std::mem::replace(&mut self.current[slot], value))
        } else {
            self.current.resize(slot, V::default());
            self.current.push(value);
            None
        };
        if step.is_some() {
            self.prev.entry(slot).or_insert(old);
        }
    }
}

/// The right rows of each slot.
pub(super) enum RightGroups {
    /// Of a static right table, which never changes: slot `s` has the rows
    /// `rows[starts[s]..starts[s + 1]]`, in order; a slot past the end has none.
    Fixed {
        starts: Vec<usize>,
        rows: Vec<RowKey>,
    },
    /// Of a refreshing right table.
    Changing(VersionedVec<RowSet>),
}

/// What the low bits of a result row key name.
pub(super) enum Inner {
    /// The right row key itself: a join without match columns.
    RightKey,
    /// The position among the right rows of the left row's key.
    Position {
        /// Left row key to the key's slot.
        left_slot: WritableRowRedirection,
        /// Slot to the right rows that have its key.
        right_rows: RightGroups,
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

    /// Sets the slot of each row of `rows`, in order, as the first state of a layout with
    /// no left rows yet. Not recorded as a change of the cycle.
    pub(super) fn start_left_slots(&mut self, rows: &RowSet, slots: Vec<RowKey>) {
        if let Inner::Position { left_slot, .. } = &mut self.inner {
            *left_slot = WritableRowRedirection::from_rows(rows, slots);
            if let Some(clock) = &self.clock {
                left_slot.start_tracking_prev(clock.clone());
            }
        }
    }

    /// Sets the right rows of every slot as the first state of a layout with no slots yet.
    /// Not recorded as a change of the cycle.
    pub(super) fn start_right_rows(&mut self, groups: RightGroups) {
        if let Inner::Position { right_rows, .. } = &mut self.inner {
            *right_rows = groups;
        }
    }

    pub(super) fn set_left_slot(&mut self, left: RowKey, slot: Option<RowKey>) {
        if let Inner::Position { left_slot, .. } = &mut self.inner {
            match slot {
                Some(slot) => left_slot.put(left, slot),
                None => left_slot.remove(left),
            };
        }
    }

    /// # Panics
    /// If the right rows are [`RightGroups::Fixed`].
    pub(super) fn set_right_rows(&mut self, slot: RowKey, rows: RowSet) {
        let clock = &self.clock;
        match &mut self.inner {
            Inner::Position {
                right_rows: RightGroups::Changing(groups),
                ..
            } => groups.put(slot as usize, rows, clock),
            Inner::Position { .. } => panic!("the right rows of a static right table changed"),
            Inner::RightKey => {}
        }
    }

    /// The slot of each of the left rows `rows`, `NULL_ROW_KEY` for a row without one.
    pub(super) fn left_slots(&self, rows: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        match &self.inner {
            Inner::Position { left_slot, .. } => left_slot.redirect(rows, use_prev),
            Inner::RightKey => vec![NULL_ROW_KEY; rows.len()],
        }
    }

    /// The right rows of `slot` of a refreshing right table.
    ///
    /// # Panics
    /// If the right rows are [`RightGroups::Fixed`].
    pub(super) fn right_rows(&self, slot: RowKey, use_prev: bool) -> Option<&RowSet> {
        match &self.inner {
            Inner::Position {
                right_rows: RightGroups::Changing(groups),
                ..
            } => groups.get(slot as usize, use_prev, &self.clock),
            Inner::Position { .. } => panic!("the right rows of a static right table are fixed"),
            Inner::RightKey => None,
        }
    }

    /// The number of right rows of `slot`.
    pub(super) fn right_size(&self, slot: RowKey, use_prev: bool) -> u64 {
        match &self.inner {
            Inner::Position {
                right_rows: RightGroups::Fixed { starts, .. },
                ..
            } => {
                let slot = slot as usize;
                match (starts.get(slot), starts.get(slot + 1)) {
                    (Some(start), Some(end)) => (end - start) as u64,
                    _ => 0,
                }
            }
            _ => self.right_rows(slot, use_prev).map_or(0, RowSet::size),
        }
    }

    /// The right row at `position` among the right rows of `slot`.
    fn right_row(&self, slot: RowKey, position: u64, use_prev: bool) -> Option<RowKey> {
        match &self.inner {
            Inner::Position {
                right_rows: RightGroups::Fixed { starts, rows },
                ..
            } => {
                let slot = slot as usize;
                let (&start, &end) = (starts.get(slot)?, starts.get(slot + 1)?);
                let at = start.checked_add(usize::try_from(position).ok()?)?;
                (at < end).then(|| rows[at])
            }
            _ => self.right_rows(slot, use_prev)?.get(position),
        }
    }

    /// The right row key of each result row key, as of now or of the previous cycle.
    fn right_keys(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        let bits = self.bits(use_prev);
        let mask = (1i64 << bits) - 1;
        let Inner::Position { left_slot, .. } = &self.inner else {
            let right = |&key: &RowKey| if key < 0 { NULL_ROW_KEY } else { key & mask };
            return keys.iter().map(right).collect();
        };
        let lefts: Vec<RowKey> = keys
            .iter()
            .map(|&key| if key < 0 { NULL_ROW_KEY } else { key >> bits })
            .collect();
        let slots = left_slot.redirect(&lefts, use_prev);
        keys.iter()
            .zip(slots)
            .map(|(&key, slot)| {
                if key < 0 || slot == NULL_ROW_KEY {
                    return NULL_ROW_KEY;
                }
                self.right_row(slot, (key & mask) as u64, use_prev)
                    .unwrap_or(NULL_ROW_KEY)
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
                left_slot: WritableRowRedirection::new(),
                right_rows: RightGroups::Changing(VersionedVec::new()),
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

    #[test]
    fn fixed_right_groups_read_positions_in_the_slot() {
        let layout = Layout::new(2, true, None);
        {
            let mut state = layout.write();
            // Slot 0 has no rows, slot 1 has 5, 40 and 41, slot 2 has 7.
            state.start_right_rows(RightGroups::Fixed {
                starts: vec![0, 0, 3, 4],
                rows: vec![5, 40, 41, 7],
            });
            state.start_left_slots(&RowSet::from_keys([1, 2, 3, 6]), vec![1, 0, 2, 9]);
            assert_eq!(
                [0, 1, 2, 3, 9].map(|slot| state.right_size(slot, false)),
                [0, 3, 1, 0, 0]
            );
        }
        // Left row 1 (slot 1) starts at 4, row 2 (slot 0) at 8, row 3 (slot 2) at 12,
        // row 6 (slot 9, past the end) at 24, and row 5 has no slot.
        let keys = [4, 5, 6, 7, 8, 12, 13, 24, 20, -1];
        assert_eq!(
            RightRows(layout).redirect(&keys, false),
            [5, 40, 41, -1, -1, 7, -1, -1, -1, -1]
        );
    }
}
