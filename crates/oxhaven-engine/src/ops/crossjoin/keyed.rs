//! The cross join with match columns.
//!
//! Rows of both tables are grouped by key into slots. A left row's region holds one result
//! row per right row of its slot, at the right row's position in the slot. Java keeps
//! three state managers with their own hash tables (both tables static, only the left
//! refreshing, the right refreshing); this is one implementation over an ordered map that
//! keeps the rules in which they differ:
//!
//! - which right rows count, and so how many bits the right part of a row key has: every
//!   right row, except when the left table is static and the right refreshing (only the
//!   keys the left table has) and when both are static and the left table has no more
//!   rows than the right (only the right rows some left row matches);
//! - the bits start at the reserved number when either table is refreshing and at 1 when
//!   both are static, grow to fit the largest group the join has seen, and never shrink.

use super::layout::{min_bits, Layout, LayoutState};
use super::region::{Downstream, InnerChange};
use super::CrossJoinError;
use crate::ops::join::keys::{Key, KeyColumns};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

fn failure(message: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: message.to_string(),
    }
}

/// The groups of rows that have one key.
#[derive(Default)]
struct Slots {
    ids: BTreeMap<Key, RowKey>,
    /// The left rows of each slot, by slot id. The right rows are in the layout, where the
    /// added columns read them.
    left: Vec<RowSet>,
    /// The size of the largest group of right rows so far.
    largest: u64,
}

impl Slots {
    fn find(&self, key: &Key) -> Option<RowKey> {
        self.ids.get(key).copied()
    }

    fn find_or_add(&mut self, key: Key) -> RowKey {
        if let Some(&slot) = self.ids.get(&key) {
            return slot;
        }
        let slot = self.left.len() as RowKey;
        self.ids.insert(key, slot);
        self.left.push(RowSet::empty());
        slot
    }

    fn left_rows(&mut self, slot: RowKey) -> &mut RowSet {
        &mut self.left[slot as usize]
    }
}

fn right_size(layout: &LayoutState, slot: RowKey) -> u64 {
    layout.right_rows(slot, false).map_or(0, RowSet::size)
}

/// Behavior: io.deephaven.engine.table.impl.RightIncrementalChunkedCrossJoinStateManager#validateKeySpaceSize
/// and its counterparts in LeftOnlyIncrementalChunkedCrossJoinStateManager and
/// StaticChunkedCrossJoinStateManager
pub(super) fn check_key_space(left_last: RowKey, bits: u32, largest: u64) -> Result<(), String> {
    let left_bits = min_bits(left_last);
    if left_bits + bits > 63 {
        return Err(format!(
            "join out of rowSet space (left reqBits + right reservedBits > 63): (left table: \
             {{maxRowKey: {left_last} reqBits: {left_bits}}}) X (right table: {{maxRowKeyUsed: \
             {} reservedBits: {bits}}}) exceeds Long.MAX_VALUE. Consider flattening left table \
             or reserving fewer right bits if possible.",
            largest as i64 - 1
        ));
    }
    Ok(())
}

/// The incremental half of a keyed cross join, and the state it keeps between cycles.
pub(super) struct Keyed {
    left: Arc<dyn Table>,
    right: Arc<dyn Table>,
    left_keys: KeyColumns,
    right_keys: KeyColumns,
    /// Whether right rows of a key no left row has are kept, and new keys of the right
    /// table make slots.
    all_right: bool,
    layout: Arc<Layout>,
    slots: Mutex<Slots>,
    left_columns: ModifiedColumnTransformer,
    right_columns: ModifiedColumnTransformer,
}

/// The result's row set and the operator.
pub(super) struct Built {
    pub(super) rows: RowSet,
    pub(super) operator: Keyed,
}

impl Keyed {
    /// Groups both tables and lays out the result.
    ///
    /// Behavior: io.deephaven.engine.table.impl.StaticChunkedCrossJoinStateManager#buildFromLeft
    /// and #buildFromRight, which of them io.deephaven.engine.table.impl.JoinControl#buildParameters
    /// picks for two static tables, LeftOnlyIncrementalChunkedCrossJoinStateManager#buildLeftTicking
    /// and RightIncrementalChunkedCrossJoinStateManager#build
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build(
        left: &Arc<dyn Table>,
        right: &Arc<dyn Table>,
        left_keys: KeyColumns,
        right_keys: KeyColumns,
        reserve: i32,
        layout: &Arc<Layout>,
        left_columns: ModifiedColumnTransformer,
        right_columns: ModifiedColumnTransformer,
    ) -> Result<Built, CrossJoinError> {
        let both_static = !left.is_refreshing() && !right.is_refreshing();
        let left_rows = left.row_set();
        let right_rows = right.row_set();
        let all_right =
            left.is_refreshing() || (both_static && left_rows.size() > right_rows.size());
        let start_bits = if both_static {
            1
        } else {
            super::check_reserve(reserve)?
        };

        let mut slots = Slots::default();
        let mut state = layout.write();
        let left_found = left_keys.read(&**left, &left_rows, false)?;
        let mut left_slots = Vec::with_capacity(left_found.len());
        let mut members: BTreeMap<RowKey, Vec<RowKey>> = BTreeMap::new();
        for (row, key) in left_rows.iter().zip(left_found) {
            let slot = slots.find_or_add(key);
            members.entry(slot).or_default().push(row);
            left_slots.push(slot);
        }
        for (slot, rows) in members {
            *slots.left_rows(slot) = RowSet::from_keys(rows);
        }
        let mut grouped: BTreeMap<RowKey, Vec<RowKey>> = BTreeMap::new();
        let right_found = right_keys.read(&**right, &right_rows, false)?;
        for (row, key) in right_rows.iter().zip(right_found) {
            let slot = if all_right {
                Some(slots.find_or_add(key))
            } else {
                slots.find(&key)
            };
            if let Some(slot) = slot {
                grouped.entry(slot).or_default().push(row);
            }
        }
        for (slot, rows) in grouped {
            slots.largest = slots.largest.max(rows.len() as u64);
            state.set_right_rows(slot, RowSet::from_keys(rows));
        }
        let bits = start_bits.max(min_bits(slots.largest as RowKey - 1));
        check_key_space(left_rows.last_row_key().unwrap_or(-1), bits, slots.largest)
            .map_err(CrossJoinError::OutOfKeySpace)?;
        state.set_bits(bits);

        let mut ranges = Vec::new();
        for (row, slot) in left_rows.iter().zip(left_slots) {
            state.set_left_slot(row, Some(slot));
            let size = right_size(&state, slot) as RowKey;
            if size > 0 {
                let start = row << bits;
                ranges.push(RowRange::new(start, start + size - 1));
            }
        }
        drop(state);
        Ok(Built {
            rows: RowSet::from_ranges(ranges),
            operator: Keyed {
                left: left.clone(),
                right: right.clone(),
                left_keys,
                right_keys,
                all_right,
                layout: layout.clone(),
                slots: Mutex::new(slots),
                left_columns,
                right_columns,
            },
        })
    }

    /// Applies the right table's change to the slots, and returns the change of each slot
    /// whose regions show it.
    ///
    /// Behavior: io.deephaven.engine.table.impl.RightIncrementalChunkedCrossJoinStateManager#rightRemove,
    /// #rightShift, #rightAdd, #rightModified and #onRightGroupInsertion, and
    /// io.deephaven.engine.table.impl.CrossJoinModifiedSlotTracker.SlotState#doFinalizeRightState.
    /// A modified row whose key changed leaves its old slot and joins its new one; one whose
    /// key did not change is modified in place. Rows of keys the join does not keep are
    /// ignored.
    fn apply_right(
        &self,
        slots: &mut Slots,
        state: &mut LayoutState,
        update: &TableUpdate,
    ) -> Result<BTreeMap<RowKey, InnerChange>, TableFailure> {
        #[derive(Default)]
        struct Touch {
            removed: Vec<RowKey>,
            added: Vec<RowKey>,
            modified: Vec<RowKey>,
        }
        let right = &*self.right;
        let keys = &self.right_keys;
        let mut touched: BTreeMap<RowKey, Touch> = BTreeMap::new();
        let slot_for = |slots: &mut Slots, key: Key| {
            if self.all_right {
                Some(slots.find_or_add(key))
            } else {
                slots.find(&key)
            }
        };

        for (row, key) in update
            .removed
            .iter()
            .zip(keys.read(right, &update.removed, true)?)
        {
            if let Some(slot) = slots.find(&key) {
                touched.entry(slot).or_default().removed.push(row);
            }
        }
        if !update.shifted.is_empty() {
            let staying = right.prev_row_set().minus(&update.removed);
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                for key in keys.read(right, &rows, true)? {
                    if let Some(slot) = slots.find(&key) {
                        touched.entry(slot).or_default();
                    }
                }
            }
        }
        for (row, key) in update
            .added
            .iter()
            .zip(keys.read(right, &update.added, false)?)
        {
            if let Some(slot) = slot_for(slots, key) {
                touched.entry(slot).or_default().added.push(row);
            }
        }
        if !update.modified.is_empty() {
            if update.modified_columns.contains_any(&keys.columns) {
                let pre = modified_pre_shift(update).map_err(failure)?;
                let before = keys.read(right, &pre, true)?;
                let after = keys.read(right, &update.modified, false)?;
                let rows = pre.iter().zip(update.modified.iter());
                for ((pre_row, post_row), (old, new)) in rows.zip(before.into_iter().zip(after)) {
                    let old_slot = slots.find(&old);
                    let new_slot = slot_for(slots, new);
                    if old_slot != new_slot {
                        if let Some(slot) = old_slot {
                            touched.entry(slot).or_default().removed.push(pre_row);
                        }
                        if let Some(slot) = new_slot {
                            touched.entry(slot).or_default().added.push(post_row);
                        }
                    } else if let Some(slot) = old_slot {
                        touched.entry(slot).or_default().modified.push(post_row);
                    }
                }
            } else {
                let found = keys.read(right, &update.modified, false)?;
                for (row, key) in update.modified.iter().zip(found) {
                    if let Some(slot) = slots.find(&key) {
                        touched.entry(slot).or_default().modified.push(row);
                    }
                }
            }
        }

        let positions = |rows: &RowSet, keys: Vec<RowKey>| {
            RowSet::from_keys(keys.into_iter().map(|key| match rows.find(key) {
                Some(position) => position as RowKey,
                None => panic!("right row {key} is not in its slot"),
            }))
        };
        let mut changes = BTreeMap::new();
        for (slot, touch) in touched {
            let before = state.right_rows(slot, false).cloned().unwrap_or_default();
            let removed = RowSet::from_keys(touch.removed.iter().copied());
            let mut after = update
                .shifted
                .apply(&before.minus(&removed))
                .map_err(failure)?;
            after.insert(&RowSet::from_keys(touch.added.iter().copied()));
            let change = InnerChange::positions(
                before.size(),
                positions(&before, touch.removed),
                after.size(),
                positions(&after, touch.added),
                positions(&after, touch.modified),
            );
            slots.largest = slots.largest.max(after.size());
            state.set_right_rows(slot, after);
            if !change.is_identity() {
                changes.insert(slot, change);
            }
        }
        let bits = state.bits(false).max(min_bits(slots.largest as RowKey - 1));
        state.set_bits(bits);
        Ok(changes)
    }

    /// Applies the left table's change to the slots and the layout.
    ///
    /// Behavior: io.deephaven.engine.table.impl.RightIncrementalChunkedCrossJoinStateManager#leftRemoved,
    /// #leftModified, #leftShift and #leftAdded, and
    /// io.deephaven.engine.table.impl.LeftOnlyIncrementalChunkedCrossJoinStateManager#removeLeft,
    /// #processLeftModifies, #applyLeftShift and #addLeft. A modified row whose key changed
    /// leaves its old slot and joins its new one, as a row removed and a row added; one
    /// whose key did not change is modified in place.
    fn apply_left(
        &self,
        slots: &mut Slots,
        state: &mut LayoutState,
        update: &TableUpdate,
    ) -> Result<LeftChange, TableFailure> {
        let left = &*self.left;
        let keys = &self.left_keys;
        let mut change = LeftChange::default();
        let mut leaving = update.removed.clone();
        for row in update.removed.iter() {
            if let Some(slot) = state.left_slot(row, false) {
                change.removed.push((row, slot));
            }
        }

        let mut arriving: Vec<(RowKey, Key)> = Vec::new();
        let mut in_place = Vec::new();
        if !update.modified.is_empty() && update.modified_columns.contains_any(&keys.columns) {
            let pre = modified_pre_shift(update).map_err(failure)?;
            let before = keys.read(left, &pre, true)?;
            let after = keys.read(left, &update.modified, false)?;
            let rows = pre.iter().zip(update.modified.iter());
            for ((pre_row, post_row), (old, new)) in rows.zip(before.into_iter().zip(after)) {
                if old == new {
                    in_place.push(post_row);
                } else {
                    if let Some(slot) = state.left_slot(pre_row, false) {
                        change.removed.push((pre_row, slot));
                    }
                    leaving.insert(&RowSet::from_keys([pre_row]));
                    arriving.push((post_row, new));
                }
            }
        } else {
            in_place.extend(update.modified.iter());
        }
        change.modified = RowSet::from_keys(in_place);
        change.removed.sort_unstable();

        let mut gone: BTreeMap<RowKey, Vec<RowKey>> = BTreeMap::new();
        for &(row, slot) in &change.removed {
            state.set_left_slot(row, None);
            gone.entry(slot).or_default().push(row);
        }
        for (slot, rows) in gone {
            slots.left_rows(slot).remove(&RowSet::from_keys(rows));
        }

        change.staying_prev = left.prev_row_set().minus(&leaving);
        if !update.shifted.is_empty() {
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = change
                    .staying_prev
                    .sub_set_by_key_range(range.first, range.last);
                for row in rows.iter() {
                    if let Some(slot) = state.left_slot(row, false) {
                        moves.push((row, row + range.delta, slot));
                    }
                }
            }
            let mut by_slot: BTreeMap<RowKey, (Vec<RowKey>, Vec<RowKey>)> = BTreeMap::new();
            for &(old, new, slot) in &moves {
                state.set_left_slot(old, None);
                let entry = by_slot.entry(slot).or_default();
                entry.0.push(old);
                entry.1.push(new);
            }
            for &(_, new, slot) in &moves {
                state.set_left_slot(new, Some(slot));
            }
            for (slot, (old, new)) in by_slot {
                let rows = slots.left_rows(slot);
                rows.remove(&RowSet::from_keys(old));
                rows.insert(&RowSet::from_keys(new));
            }
        }

        let added = keys.read(left, &update.added, false)?;
        arriving.extend(update.added.iter().zip(added));
        let mut joining: BTreeMap<RowKey, Vec<RowKey>> = BTreeMap::new();
        for (row, key) in arriving {
            let slot = slots.find_or_add(key);
            state.set_left_slot(row, Some(slot));
            joining.entry(slot).or_default().push(row);
            change.arrived.push((row, slot));
        }
        for (slot, rows) in joining {
            slots.left_rows(slot).insert(&RowSet::from_keys(rows));
        }
        change.arrived.sort_unstable();
        Ok(change)
    }
}

/// What the left table's change did to the left rows.
#[derive(Default)]
struct LeftChange {
    /// Rows that are gone or left their slot: the row key before the cycle and the slot.
    removed: Vec<(RowKey, RowKey)>,
    /// Rows that arrived or joined another slot: the row key now and the slot.
    arrived: Vec<(RowKey, RowKey)>,
    /// Rows modified that stayed in their slot, by row key now.
    modified: RowSet,
    /// The rows before the cycle that stay in their slot.
    staying_prev: RowSet,
}

impl Keyed {
    /// Behavior: the listeners of io.deephaven.engine.table.impl.CrossJoinHelper#internalJoin:
    /// the one of the left table when the right table is static, the one of the right table
    /// when the left table is static, and the merged one when both are refreshing.
    pub(super) fn on_update(
        &self,
        left_update: Option<&TableUpdate>,
        right_update: Option<&TableUpdate>,
    ) -> Result<TableUpdate, TableFailure> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = self.layout.write();
        let prev_bits = state.bits(false);
        let mut changes = BTreeMap::new();
        if let Some(update) = right_update {
            changes = self.apply_right(&mut slots, &mut state, update)?;
        }
        let bits = state.bits(false);
        let left_now = self.left.row_set();
        check_key_space(left_now.last_row_key().unwrap_or(-1), bits, slots.largest)
            .map_err(failure)?;

        let left = match left_update {
            Some(update) => self.apply_left(&mut slots, &mut state, update)?,
            None => LeftChange {
                staying_prev: (*left_now).clone(),
                ..LeftChange::default()
            },
        };

        let mut down = Downstream::default();
        let before = |state: &LayoutState, slot: RowKey| -> RowSet {
            match changes.get(&slot) {
                Some(change) => change.prev.clone(),
                None => RowSet::flat(right_size(state, slot)),
            }
        };
        for &(row, slot) in &left.removed {
            down.remove_region(row << prev_bits, &before(&state, slot));
        }

        let arrived = RowSet::from_keys(left.arrived.iter().map(|(row, _)| *row));
        let no_shift = Default::default();
        let shifted = left_update.map_or(&no_shift, |update| &update.shifted);
        let visit = if bits != prev_bits {
            left_now.minus(&arrived)
        } else {
            let mut visit = left.modified.clone();
            for slot in changes.keys() {
                visit.insert(&slots.left[*slot as usize]);
            }
            for range in shifted.ranges() {
                let rows = left
                    .staying_prev
                    .sub_set_by_key_range(range.first, range.last);
                visit.insert(&rows.shift(range.delta).map_err(failure)?);
            }
            visit.minus(&arrived)
        };
        let back = shifted.inverse();
        for row in visit.iter() {
            let Some(slot) = state.left_slot(row, false) else {
                panic!("left row {row} has no slot");
            };
            let unchanged;
            let change = match changes.get(&slot) {
                Some(change) => change,
                None => {
                    unchanged = InnerChange::unchanged(RowSet::flat(right_size(&state, slot)));
                    &unchanged
                }
            };
            down.survivor(
                back.apply_to_key(row) << prev_bits,
                prev_bits,
                row << bits,
                change,
                left.modified.contains(row),
            );
        }
        for &(row, slot) in &left.arrived {
            down.add_region(row << bits, &RowSet::flat(right_size(&state, slot)));
        }

        let mut columns = ModifiedColumnSet::empty();
        if let Some(update) = left_update.filter(|_| !left.modified.is_empty()) {
            columns = columns.union(&self.left_columns.transform(&update.modified_columns));
        }
        if let Some(update) =
            right_update.filter(|_| changes.values().any(|c| !c.modified.is_empty()))
        {
            columns = columns.union(&self.right_columns.transform(&update.modified_columns));
        }
        down.finish(columns)
    }
}
