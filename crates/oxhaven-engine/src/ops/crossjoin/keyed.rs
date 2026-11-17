//! The cross join with match columns.
//!
//! Rows of both tables are grouped by key into slots. A left row's region holds one result
//! row per right row of its slot, at the right row's position in the slot. Java keeps
//! three state managers with their own hash tables (both tables static, only the left
//! refreshing, the right refreshing); this is one implementation over the one
//! [`KeyTable`] that keeps the rules in which they differ:
//!
//! - which right rows count, and so how many bits the right part of a row key has: every
//!   right row, except when the left table is static and the right refreshing (only the
//!   keys the left table has) and when both are static and the left table has no more
//!   rows than the right (only the right rows some left row matches);
//! - the bits start at the reserved number when either table is refreshing and at 1 when
//!   both are static, grow to fit the largest group the join has seen, and never shrink.
//!
//! Keys are numbered a chunk of rows at a time; nothing is kept or looked up per row in an
//! ordered map.

use super::layout::{min_bits, Layout, LayoutState, RightGroups, VersionedVec};
use super::region::{Downstream, InnerChange};
use super::CrossJoinError;
use crate::ops::join::keys::KeyColumns;
use crate::ops::key_table::{KeyTable, NO_SLOT};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

fn failure(message: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: message.to_string(),
    }
}

/// The slot a key table reported, or `None` for [`NO_SLOT`].
fn slot_of(slot: u32) -> Option<RowKey> {
    (slot != NO_SLOT).then_some(RowKey::from(slot))
}

/// The rows of `rows` in each slot below `slots`, in row order, given the slot of each row;
/// rows without a slot are left out.
fn grouped(rows: &RowSet, row_slots: &[u32], slots: usize) -> Vec<RowSet> {
    let (starts, sorted) = grouped_flat(rows, row_slots, slots);
    (0..slots)
        .map(|slot| RowSet::from_keys(sorted[starts[slot]..starts[slot + 1]].iter().copied()))
        .collect()
}

/// As [`grouped`], in one array: slot `s` has `rows[starts[s]..starts[s + 1]]`.
fn grouped_flat(rows: &RowSet, row_slots: &[u32], slots: usize) -> (Vec<usize>, Vec<RowKey>) {
    let mut starts = vec![0usize; slots + 1];
    for &slot in row_slots {
        if slot != NO_SLOT {
            starts[slot as usize + 1] += 1;
        }
    }
    for slot in 0..slots {
        starts[slot + 1] += starts[slot];
    }
    let mut next = starts.clone();
    let mut sorted: Vec<RowKey> = vec![0; starts[slots]];
    for (row, &slot) in rows.iter().zip(row_slots) {
        if slot != NO_SLOT {
            sorted[next[slot as usize]] = row;
            next[slot as usize] += 1;
        }
    }
    (starts, sorted)
}

/// The groups of rows that have one key.
struct Slots {
    /// A key's slot is its group.
    keys: Box<KeyTable>,
    /// The left rows of each slot, by slot; a slot past the end has none. Kept only when
    /// the right table is refreshing, since only a change of a slot's right rows asks
    /// which left rows have it. The right rows are in the layout, where the added columns
    /// read them.
    left: Option<Vec<RowSet>>,
    /// The size of the largest group of right rows so far.
    largest: u64,
}

impl Slots {
    /// The slot of the key of each row of `rows` of `table`, in row order; with
    /// `use_prev`, of the previous values of rows of the previous key space. With `create`
    /// a key not seen before gets a new slot; without, its rows get [`NO_SLOT`].
    fn of(
        &mut self,
        columns: &KeyColumns,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
        create: bool,
    ) -> Result<Vec<u32>, TableFailure> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let chunks = columns.chunks(table, rows, use_prev)?;
        let size = rows.size() as usize;
        Ok(if create {
            self.keys.insert(&chunks, size)
        } else {
            self.keys.find(&chunks, size)
        })
    }

    /// The left rows of `slot`, if the left rows are kept.
    fn left_rows(&mut self, slot: RowKey) -> Option<&mut RowSet> {
        let left = self.left.as_mut()?;
        let slot = slot as usize;
        if left.len() <= slot {
            left.resize_with(slot + 1, RowSet::empty);
        }
        Some(&mut left[slot])
    }
}

fn right_size(layout: &LayoutState, slot: RowKey) -> u64 {
    layout.right_size(slot, false)
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

        let mut slots = Slots {
            keys: Box::default(),
            left: None,
            largest: 0,
        };
        let left_slots = slots.of(&left_keys, &**left, &left_rows, false, true)?;
        if right.is_refreshing() {
            slots.left = Some(grouped(&left_rows, &left_slots, slots.keys.len()));
        }
        let right_slots = slots.of(&right_keys, &**right, &right_rows, false, all_right)?;
        let (starts, sorted) = grouped_flat(&right_rows, &right_slots, slots.keys.len());
        drop(right_slots);
        let sizes: Vec<u64> = starts.windows(2).map(|w| (w[1] - w[0]) as u64).collect();
        slots.largest = sizes.iter().copied().max().unwrap_or(0);
        // The right rows of a static right table never change, and are kept in the one
        // array; those of a refreshing one as a row set per slot.
        let groups = if right.is_refreshing() {
            let sets = (0..sizes.len()).map(|slot| {
                RowSet::from_keys(sorted[starts[slot]..starts[slot + 1]].iter().copied())
            });
            RightGroups::Changing(VersionedVec::from_vec(sets.collect()))
        } else {
            RightGroups::Fixed {
                starts,
                rows: sorted,
            }
        };
        let bits = start_bits.max(min_bits(slots.largest as RowKey - 1));
        check_key_space(left_rows.last_row_key().unwrap_or(-1), bits, slots.largest)
            .map_err(CrossJoinError::OutOfKeySpace)?;

        let mut ranges = Vec::new();
        for (row, &slot) in left_rows.iter().zip(&left_slots) {
            let size = sizes[slot as usize] as RowKey;
            if size > 0 {
                let start = row << bits;
                ranges.push(RowRange::new(start, start + size - 1));
            }
        }
        let mut state = layout.write();
        state.set_bits(bits);
        state.start_right_rows(groups);
        state.start_left_slots(
            &left_rows,
            left_slots.into_iter().map(RowKey::from).collect(),
        );
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

        let removed = slots.of(keys, right, &update.removed, true, false)?;
        for (row, slot) in update.removed.iter().zip(removed) {
            if let Some(slot) = slot_of(slot) {
                touched.entry(slot).or_default().removed.push(row);
            }
        }
        if !update.shifted.is_empty() {
            let staying = right.prev_row_set().minus(&update.removed);
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                for slot in slots.of(keys, right, &rows, true, false)? {
                    if let Some(slot) = slot_of(slot) {
                        touched.entry(slot).or_default();
                    }
                }
            }
        }
        let added = slots.of(keys, right, &update.added, false, self.all_right)?;
        for (row, slot) in update.added.iter().zip(added) {
            if let Some(slot) = slot_of(slot) {
                touched.entry(slot).or_default().added.push(row);
            }
        }
        if !update.modified.is_empty() {
            if update.modified_columns.contains_any(&keys.columns) {
                let pre = modified_pre_shift(update).map_err(failure)?;
                let before = slots.of(keys, right, &pre, true, false)?;
                let after = slots.of(keys, right, &update.modified, false, self.all_right)?;
                let rows = pre.iter().zip(update.modified.iter());
                for ((pre_row, post_row), (old, new)) in rows.zip(before.into_iter().zip(after)) {
                    let (old_slot, new_slot) = (slot_of(old), slot_of(new));
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
                let found = slots.of(keys, right, &update.modified, false, false)?;
                for (row, slot) in update.modified.iter().zip(found) {
                    if let Some(slot) = slot_of(slot) {
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
        let removed: Vec<RowKey> = update.removed.iter().collect();
        for (&row, slot) in removed.iter().zip(state.left_slots(&removed, false)) {
            if slot != NULL_ROW_KEY {
                change.removed.push((row, slot));
            }
        }

        // The rows that changed key, before the cycle, and where they are now with their
        // new slot.
        let mut rekeyed: Vec<RowKey> = Vec::new();
        let mut arriving: Vec<(RowKey, RowKey)> = Vec::new();
        let mut in_place = Vec::new();
        if !update.modified.is_empty() && update.modified_columns.contains_any(&keys.columns) {
            let pre = modified_pre_shift(update).map_err(failure)?;
            // Every key a left row had has a slot, so the keys before are only found; the
            // keys after make the slots of keys not seen before, in row order.
            let before = slots.of(keys, left, &pre, true, false)?;
            let after = slots.of(keys, left, &update.modified, false, true)?;
            let pre_rows: Vec<RowKey> = pre.iter().collect();
            let pre_slots = state.left_slots(&pre_rows, false);
            let rows = pre_rows.iter().zip(pre_slots).zip(update.modified.iter());
            for (((&pre_row, pre_slot), post_row), (old, new)) in
                rows.zip(before.into_iter().zip(after))
            {
                if old == new {
                    in_place.push(post_row);
                } else {
                    if pre_slot != NULL_ROW_KEY {
                        change.removed.push((pre_row, pre_slot));
                    }
                    rekeyed.push(pre_row);
                    arriving.push((post_row, RowKey::from(new)));
                }
            }
        } else {
            in_place.extend(update.modified.iter());
        }
        let mut leaving = update.removed.clone();
        leaving.insert(&RowSet::from_keys(rekeyed));
        change.modified = RowSet::from_keys(in_place);
        change.removed.sort_unstable();

        let mut gone: BTreeMap<RowKey, Vec<RowKey>> = BTreeMap::new();
        for &(row, slot) in &change.removed {
            state.set_left_slot(row, None);
            gone.entry(slot).or_default().push(row);
        }
        for (slot, rows) in gone {
            if let Some(left_rows) = slots.left_rows(slot) {
                left_rows.remove(&RowSet::from_keys(rows));
            }
        }

        change.staying_prev = left.prev_row_set().minus(&leaving);
        if !update.shifted.is_empty() {
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows: Vec<RowKey> = change
                    .staying_prev
                    .sub_set_by_key_range(range.first, range.last)
                    .iter()
                    .collect();
                for (&row, slot) in rows.iter().zip(state.left_slots(&rows, false)) {
                    if slot != NULL_ROW_KEY {
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
                if let Some(rows) = slots.left_rows(slot) {
                    rows.remove(&RowSet::from_keys(old));
                    rows.insert(&RowSet::from_keys(new));
                }
            }
        }

        let added = slots.of(keys, left, &update.added, false, true)?;
        arriving.extend(update.added.iter().zip(added.into_iter().map(RowKey::from)));
        let mut joining: BTreeMap<RowKey, Vec<RowKey>> = BTreeMap::new();
        for (row, slot) in arriving {
            state.set_left_slot(row, Some(slot));
            joining.entry(slot).or_default().push(row);
            change.arrived.push((row, slot));
        }
        for (slot, rows) in joining {
            if let Some(left_rows) = slots.left_rows(slot) {
                left_rows.insert(&RowSet::from_keys(rows));
            }
        }
        change.arrived.sort_unstable();
        Ok(change)
    }
}

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
            if !changes.is_empty() {
                let Some(left_rows) = &slots.left else {
                    panic!("the left rows of each slot are kept for a refreshing right table");
                };
                for slot in changes.keys() {
                    if let Some(rows) = left_rows.get(*slot as usize) {
                        visit.insert(rows);
                    }
                }
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
        let visit: Vec<RowKey> = visit.iter().collect();
        let visit_slots = state.left_slots(&visit, false);
        for (&row, slot) in visit.iter().zip(visit_slots) {
            if slot == NULL_ROW_KEY {
                panic!("left row {row} has no slot");
            }
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
