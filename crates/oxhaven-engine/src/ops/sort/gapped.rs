//! The incremental half of `sort`: the sorted rows kept in a key space with gaps, so that
//! an update costs about what it changes and not the table.
//!
//! Java gives the result of an incremental sort the keys `2^30 - n/2 ..` without gaps and,
//! when an insertion finds no free key, moves the rows in its way, leaving a gap of 64 keys
//! after every 64 rows it has moved (`SortListener`, `REBALANCE_RANGE_SIZE` and
//! `REBALANCE_GAP_SIZE`). The idea is kept and the layout is not: here the key space
//! `0..len` is cut into leaves of [`LEAF`] keys, a new result fills the first
//! [`INITIAL_FILL`] keys of each leaf, and an insertion that finds no free key lays the rows
//! of the smallest window of leaves around it that has room out again, evenly among the
//! leaves and packed to the start of each (a packed-memory array). Rows that move are
//! reported as shifts; they keep their order, so the shift data is valid. The row keys of a
//! sort result therefore differ from Java's, which `PARITY.md` A4 allows: row keys are never
//! compared, positions are.
//!
//! The keys stay within `0..len`, at least three quarters full after a whole layout, so
//! the row redirection keeps its flat array and a read through it stays a slice copy per
//! range of keys.

use super::keys::{KeyEncoder, SortKeys};
use super::sorted::sorted_keys;
use crate::update::ModifiedColumnTransformer;
use oxhaven_column::WritableRowRedirection;
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_rowset::ShiftBuilder;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

/// The number of row keys in a leaf.
pub(super) const LEAF: usize = 128;
/// The number of rows a whole layout puts in each leaf.
pub(super) const INITIAL_FILL: usize = 96;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// What a cycle has done to the rows so far, by the key each row has now.
#[derive(Default)]
struct Changes {
    /// The keys of rows that are new in the result.
    fresh: BTreeSet<usize>,
    /// The key each moved row had when the cycle began.
    origin: BTreeMap<usize, usize>,
    /// The keys, as of the start of the cycle, of the rows that left.
    removed: Vec<RowKey>,
}

/// A row being laid out: its parent key, the index of its sort key, and the key it had when
/// the cycle began, `None` for a row that is new.
type Placed = (RowKey, u32, Option<usize>);

/// The rows of a sort result by row key.
struct Layout {
    /// The parent key at each row key; `NULL_ROW_KEY` where there is no row. Its length is
    /// a multiple of [`LEAF`].
    parents: Vec<RowKey>,
    /// The index in `keys` of the sort key of the row at each row key.
    key_at: Vec<u32>,
    /// The sort keys, in no order: those of rows that left stay until the next compaction.
    keys: SortKeys,
    /// The row keys that hold a row.
    rows: RowSet,
}

impl Layout {
    /// The rows `parents`, in sorted order, with the sort key of each at the same index of
    /// `keys`: [`INITIAL_FILL`] rows at the start of each leaf.
    fn new(parents: &[RowKey], keys: SortKeys) -> Self {
        let leaves = parents.len().div_ceil(INITIAL_FILL);
        let mut layout = Layout {
            parents: vec![NULL_ROW_KEY; leaves * LEAF],
            key_at: vec![0; leaves * LEAF],
            keys,
            rows: RowSet::empty(),
        };
        let mut ranges = Vec::with_capacity(leaves);
        for (leaf, rows) in parents.chunks(INITIAL_FILL).enumerate() {
            let (start, first) = (leaf * LEAF, leaf * INITIAL_FILL);
            layout.parents[start..start + rows.len()].copy_from_slice(rows);
            for (offset, key) in layout.key_at[start..start + rows.len()]
                .iter_mut()
                .enumerate()
            {
                *key = (first + offset) as u32;
            }
            ranges.push(RowRange::new(
                start as RowKey,
                (start + rows.len() - 1) as RowKey,
            ));
        }
        layout.rows = RowSet::from_ranges(ranges);
        layout
    }

    fn slot(&self, rank: u64) -> Option<usize> {
        self.rows.get(rank).map(|key| key as usize)
    }

    /// How the row at `slot` sorts against the row with the sort key `keys[index]` and the
    /// parent key `parent`: by sort key, and by parent key where those are equal.
    ///
    /// Behavior: io.deephaven.engine.table.impl.SortListener.TargetComparator#compareTargetTo
    fn compare(&self, slot: usize, keys: &SortKeys, index: usize, parent: RowKey) -> Ordering {
        self.keys
            .compare(self.key_at[slot] as usize, keys, index)
            .then(self.parents[slot].cmp(&parent))
    }

    /// The number of rows that do not sort after the given row.
    fn rank_after(&self, keys: &SortKeys, index: usize, parent: RowKey) -> u64 {
        let (mut low, mut high) = (0, self.rows.size());
        while low < high {
            let middle = low + (high - low) / 2;
            let slot = self.slot(middle).expect("a rank below the size");
            if self.compare(slot, keys, index, parent) == Ordering::Greater {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        low
    }

    /// The row key of the row with the given sort key and parent key.
    fn locate(&self, keys: &SortKeys, index: usize, parent: RowKey) -> Result<usize, TableFailure> {
        self.rank_after(keys, index, parent)
            .checked_sub(1)
            .and_then(|rank| self.slot(rank))
            .filter(|&slot| self.parents[slot] == parent)
            .ok_or_else(|| failure(format!("a sort does not hold the parent's row {parent}")))
    }

    /// The number of rows with a key in `start..end`.
    fn held(&self, start: usize, end: usize) -> usize {
        let rank = |key: usize| match self.rows.find_or_insertion(key as RowKey) {
            Ok(rank) | Err(rank) => rank,
        };
        (rank(end) - rank(start)) as usize
    }

    /// Puts `group`, rows that sort between the rows at `before` and `at` (the ends of the
    /// result where those are `None`), into the result.
    fn insert(
        &mut self,
        redirection: &WritableRowRedirection,
        changes: &mut Changes,
        before: Option<usize>,
        at: Option<usize>,
        group: Vec<Placed>,
    ) {
        let start = before.map_or(0, |slot| slot + 1);
        let count = group.len();
        // Free keys after the row before: the group goes there and nothing moves.
        if at.is_none_or(|at| at - start >= count) {
            let len = (start + count)
                .next_multiple_of(LEAF)
                .max(self.parents.len());
            self.parents.resize(len, NULL_ROW_KEY);
            self.key_at.resize(len, 0);
            for (slot, placed) in (start..).zip(group) {
                self.place(redirection, changes, slot, placed);
            }
            self.rows.insert(&RowSet::from_range(
                start as RowKey,
                (start + count - 1) as RowKey,
            ));
            return;
        }
        let at = at.expect("a group with no row after it is appended");
        // The smallest window of leaves around the place that has room: a leaf may be full,
        // a larger window keeps an eighth free, and the whole key space grows when it would
        // not.
        let len = self.parents.len();
        let mut width = LEAF;
        let (start, end) = loop {
            let start = at / width * width;
            let end = (start + width).min(len);
            let rows = self.held(start, end) + count;
            let room = if width == LEAF {
                end - start
            } else {
                (end - start) / 8 * 7
            };
            if rows <= room {
                break (start, end);
            }
            if start == 0 && end == len {
                let leaves = rows.div_ceil(INITIAL_FILL);
                self.parents.resize(leaves * LEAF, NULL_ROW_KEY);
                self.key_at.resize(leaves * LEAF, 0);
                break (0, leaves * LEAF);
            }
            width *= 2;
        };
        self.lay_out(redirection, changes, start, end, at, group);
    }

    /// Writes one row at a free key.
    fn place(
        &mut self,
        redirection: &WritableRowRedirection,
        changes: &mut Changes,
        slot: usize,
        (parent, key, origin): Placed,
    ) {
        self.parents[slot] = parent;
        self.key_at[slot] = key;
        redirection.put(slot as RowKey, parent);
        match origin {
            None => {
                changes.fresh.insert(slot);
            }
            Some(old) if old != slot => {
                changes.origin.insert(slot, old);
            }
            Some(_) => {}
        }
    }

    /// Lays the rows with keys in `start..end`, a whole number of leaves, out again with
    /// `group` before the row at `at`: the same number of rows in each leaf, give or take
    /// one, from the start of the leaf.
    fn lay_out(
        &mut self,
        redirection: &WritableRowRedirection,
        changes: &mut Changes,
        start: usize,
        end: usize,
        at: usize,
        group: Vec<Placed>,
    ) {
        let old = self
            .rows
            .sub_set_by_key_range(start as RowKey, end as RowKey - 1);
        let mut placed: Vec<Placed> = Vec::with_capacity(old.size() as usize + group.len());
        let mut group = Some(group);
        for slot in old.iter().map(|key| key as usize) {
            if slot == at {
                placed.extend(group.take().expect("one row at a key"));
            }
            let origin = if changes.fresh.remove(&slot) {
                None
            } else {
                Some(changes.origin.remove(&slot).unwrap_or(slot))
            };
            placed.push((self.parents[slot], self.key_at[slot], origin));
        }
        debug_assert!(
            group.is_none(),
            "the row the group goes before is in the window"
        );
        let before = self.parents[start..end].to_vec();
        self.parents[start..end].fill(NULL_ROW_KEY);
        let leaves = (end - start) / LEAF;
        let total = placed.len();
        let mut ranges = Vec::with_capacity(leaves);
        let mut placed = placed.into_iter();
        for leaf in 0..leaves {
            let rows = (leaf + 1) * total / leaves - leaf * total / leaves;
            let first = start + leaf * LEAF;
            for (slot, (parent, key, origin)) in (first..first + rows).zip(placed.by_ref()) {
                self.parents[slot] = parent;
                self.key_at[slot] = key;
                match origin {
                    None => {
                        changes.fresh.insert(slot);
                    }
                    Some(old) if old != slot => {
                        changes.origin.insert(slot, old);
                    }
                    Some(_) => {}
                }
            }
            if rows > 0 {
                ranges.push(RowRange::new(first as RowKey, (first + rows - 1) as RowKey));
            }
        }
        for (slot, was) in (start..end).zip(before) {
            match self.parents[slot] {
                now if now == was => {}
                NULL_ROW_KEY => {
                    redirection.remove(slot as RowKey);
                }
                now => {
                    redirection.put(slot as RowKey, now);
                }
            }
        }
        self.rows.remove(&old);
        self.rows.insert(&RowSet::from_ranges(ranges));
    }

    /// Drops the sort keys of rows that left, once they are more than the rows that stay.
    fn compact(&mut self, encoder: &KeyEncoder) {
        let rows = self.rows.size() as usize;
        if self.keys.len() <= 2 * rows + 4 * LEAF {
            return;
        }
        let mut keys = encoder.empty(rows, &self.keys);
        for slot in self.rows.iter().map(|key| key as usize) {
            keys.push_from(&self.keys, self.key_at[slot] as usize);
            self.key_at[slot] = (keys.len() - 1) as u32;
        }
        self.keys = keys;
    }
}

/// The incremental half of a sort.
pub(super) struct SortOperator {
    parent: Arc<dyn Table>,
    encoder: KeyEncoder,
    /// Indices of the sort columns in the parent's definition.
    sort_columns: Vec<usize>,
    transformer: ModifiedColumnTransformer,
    redirection: Arc<WritableRowRedirection>,
    layout: Mutex<Layout>,
}

impl SortOperator {
    /// The operator of a result that shows `parents`, the parent's rows in sorted order
    /// with their sort keys in `keys`, and the row keys and redirection of that result.
    pub(super) fn new(
        parent: &Arc<dyn Table>,
        encoder: KeyEncoder,
        parents: Vec<RowKey>,
        keys: SortKeys,
    ) -> (Self, RowSet, Arc<WritableRowRedirection>) {
        let layout = Layout::new(&parents, keys);
        let rows = layout.rows.clone();
        let redirection = Arc::new(WritableRowRedirection::from_rows(&rows, parents));
        let operator = SortOperator {
            parent: parent.clone(),
            sort_columns: encoder.column_indices(),
            encoder,
            transformer: ModifiedColumnTransformer::identity(parent.definition().columns().len()),
            redirection: redirection.clone(),
            layout: Mutex::new(layout),
        };
        (operator, rows, redirection)
    }

    /// Brings the sorted order up to date with one change of the parent.
    ///
    /// The cases are Java's. Removed rows leave their keys. Shifted rows keep their keys
    /// and only change the parent key they show. Added rows are placed by sort key, after
    /// the rows with an equal key that precede them in the parent. A modified row is
    /// placed again only when a sort column is among the modified columns, and is then
    /// reported as modified or as removed and added by Java's rule. Rows that leave are
    /// found by their previous sort keys, read from the parent's previous values, where
    /// Java keeps a reverse lookup.
    ///
    /// Behavior: io.deephaven.engine.table.impl.SortListener#onUpdate
    pub(super) fn on_update(&self, upstream: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let resort = !upstream.modified.is_empty()
            && upstream.modified_columns.contains_any(&self.sort_columns);
        let redirection = &*self.redirection;
        let mut layout = self.layout.lock().unwrap_or_else(PoisonError::into_inner);
        let layout = &mut *layout;
        let mut changes = Changes::default();

        // The rows the parent removed leave, found as they were when the cycle began.
        let leave = |layout: &mut Layout, changes: &mut Changes, slots: Vec<usize>| {
            for &slot in &slots {
                layout.parents[slot] = NULL_ROW_KEY;
                redirection.remove(slot as RowKey);
                changes.removed.push(slot as RowKey);
            }
            let slots = slots.into_iter().map(|slot| slot as RowKey);
            layout.rows.remove(&RowSet::from_keys(slots));
        };
        if !upstream.removed.is_empty() {
            let keys = self
                .encoder
                .encode(&*self.parent, &upstream.removed, true)?;
            let mut slots = Vec::new();
            for (index, parent) in upstream.removed.iter().enumerate() {
                slots.push(layout.locate(&keys, index, parent)?);
            }
            leave(layout, &mut changes, slots);
        }

        // Behavior: the shifts of the parent only change the redirection.
        if !upstream.shifted.is_empty() {
            for slot in layout.rows.iter() {
                let old = layout.parents[slot as usize];
                let new = upstream.shifted.apply_to_key(old);
                if new != old {
                    layout.parents[slot as usize] = new;
                    redirection.put(slot, new);
                }
            }
        }

        let to_place = if resort {
            upstream.added.union(&upstream.modified)
        } else {
            upstream.added.clone()
        };
        let (placed, incoming, order) = sorted_keys(&self.encoder, &*self.parent, &to_place)?;
        let key_of = |next: usize| order[next] as usize;
        // The rows to put into the result, as indices of `placed`: the added rows and the
        // modified rows that do not keep their place.
        let mut queued: Vec<usize> = (0..placed.len()).collect();
        // The modified rows that keep their place: parent key and index of the new sort key.
        let mut kept: Vec<(RowKey, u32)> = Vec::new();
        if resort {
            // Every row is judged against the rows as they were, the modified rows among
            // them with the sort keys they had: a row keeps its place when the row found
            // for it to follow is the row itself, unless the row queued last follows that
            // same row, which sorts before this one and so takes the place.
            //
            // Behavior: the test "check if new location differs from current location or
            // if the previous row needs to slot here" in
            // io.deephaven.engine.table.impl.SortListener#onUpdate
            queued.clear();
            let mut stays = Vec::new();
            let mut moved = Vec::new();
            let mut last_queued: Option<Option<usize>> = None;
            for next in 0..placed.len() {
                let parent = placed[next];
                let after = layout
                    .rank_after(&incoming, key_of(next), parent)
                    .checked_sub(1)
                    .and_then(|rank| layout.slot(rank));
                let itself = after.filter(|&slot| layout.parents[slot] == parent);
                match itself {
                    Some(slot) if last_queued != Some(after) => stays.push((slot, next)),
                    _ => {
                        if upstream.modified.contains(parent) {
                            moved.push(parent);
                        }
                        last_queued = Some(after);
                        queued.push(next);
                    }
                }
            }
            // The rows that move leave, found by the sort keys they had.
            if !moved.is_empty() {
                let moved = RowSet::from_keys(moved);
                let before = upstream.shifted.inverse().apply(&moved);
                let before = before.map_err(|error| failure(error.to_string()))?;
                let keys = self.encoder.encode(&*self.parent, &before, true)?;
                let mut slots = Vec::new();
                for (index, parent) in moved.iter().enumerate() {
                    slots.push(layout.locate(&keys, index, parent)?);
                }
                leave(layout, &mut changes, slots);
            }
            for (slot, next) in stays {
                layout.keys.push_from(&incoming, key_of(next));
                layout.key_at[slot] = (layout.keys.len() - 1) as u32;
                kept.push((placed[next], layout.key_at[slot]));
            }
        }

        let mut queued = queued.into_iter().peekable();
        while let Some(first) = queued.next() {
            let rank = layout.rank_after(&incoming, key_of(first), placed[first]);
            let at = layout.slot(rank);
            let before = rank.checked_sub(1).and_then(|rank| layout.slot(rank));
            // The rows that go to the same place: those that sort before the row at it.
            let mut group = Vec::new();
            let mut next = Some(first);
            while let Some(row) = next {
                layout.keys.push_from(&incoming, key_of(row));
                group.push((placed[row], (layout.keys.len() - 1) as u32, None));
                next = queued.next_if(|&row| {
                    at.is_none_or(|at| {
                        layout.compare(at, &incoming, key_of(row), placed[row]) == Ordering::Greater
                    })
                });
            }
            layout.insert(redirection, &mut changes, before, at, group);
        }

        // The modified rows that stay where they are, by the keys they have now.
        let mut modified = Vec::new();
        for (parent, key) in kept {
            modified.push(layout.locate(&layout.keys, key as usize, parent)? as RowKey);
        }
        if !resort && !upstream.modified.is_empty() {
            let keys = self
                .encoder
                .encode(&*self.parent, &upstream.modified, false)?;
            for (index, parent) in upstream.modified.iter().enumerate() {
                modified.push(layout.locate(&keys, index, parent)? as RowKey);
            }
        }
        layout.compact(&self.encoder);

        // Rows keep their order, so the moved rows are in the order of their old keys too;
        // neighbours that moved by the same distance are one shift.
        let mut shifted = ShiftBuilder::new();
        for (&new, &old) in &changes.origin {
            shifted.shift_range(old as RowKey, old as RowKey, new as RowKey - old as RowKey);
        }
        Ok(TableUpdate {
            added: RowSet::from_keys(changes.fresh.iter().map(|&slot| slot as RowKey)),
            removed: RowSet::from_keys(changes.removed),
            modified: RowSet::from_keys(modified),
            shifted: shifted
                .build()
                .map_err(|error| failure(error.to_string()))?,
            modified_columns: self.transformer.transform(&upstream.modified_columns),
        })
    }
}
