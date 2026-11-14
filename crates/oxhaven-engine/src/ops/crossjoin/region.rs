//! A cycle's change of a cross join result, put together one left row at a time.
//!
//! Every left row owns a region of result row keys: `left << bits` plus an inner key, the
//! right row key (no match columns) or the position of the right row among the right rows
//! of the left row's key. The right table's change is first described in the inner key
//! space of each group of right rows ([`InnerChange`]); the result's change is then that
//! description moved to the region of every left row that shows the group, plus the regions
//! of left rows that were removed or added.
//!
//! Behavior: the downstream computation of
//! io.deephaven.engine.table.impl.CrossJoinHelper#zeroKeyColumnsJoin and of the listeners in
//! io.deephaven.engine.table.impl.CrossJoinHelper#internalJoin, with
//! io.deephaven.engine.table.impl.CrossJoinModifiedSlotTracker.SlotState#doFinalizeRightState
//! for the inner change of a group

use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::TableFailure;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};

fn failure(message: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: message.to_string(),
    }
}

/// What a cycle did to the inner keys of one group of right rows.
#[derive(Clone, Debug)]
pub(super) struct InnerChange {
    /// The inner keys before the cycle.
    pub(super) prev: RowSet,
    /// The inner keys after the cycle.
    pub(super) curr: RowSet,
    /// Inner keys of `prev` whose right row left the group.
    pub(super) removed: RowSet,
    /// Inner keys of `curr` whose right row joined the group.
    pub(super) added: RowSet,
    /// Inner keys of `curr` whose right row stayed and was modified.
    pub(super) modified: RowSet,
    /// The rows that stayed: each run of inner keys of `prev` moves by its delta. The runs
    /// hold exactly those rows, in order.
    pub(super) runs: Vec<ShiftRange>,
}

impl InnerChange {
    /// A group the cycle left alone.
    pub(super) fn unchanged(rows: RowSet) -> Self {
        let runs = rows
            .ranges()
            .iter()
            .map(|range| ShiftRange {
                first: range.first(),
                last: range.last(),
                delta: 0,
            })
            .collect();
        Self {
            prev: rows.clone(),
            curr: rows,
            removed: RowSet::empty(),
            added: RowSet::empty(),
            modified: RowSet::empty(),
            runs,
        }
    }

    /// The change of a group whose inner keys are positions: `prev_size` rows before, of
    /// which those at `removed` left, and `curr_size` rows after, of which those at `added`
    /// arrived. The rows that stayed keep their order.
    ///
    /// Behavior: io.deephaven.engine.table.impl.CrossJoinModifiedSlotTracker.SlotState#doFinalizeRightState
    pub(super) fn positions(
        prev_size: u64,
        removed: RowSet,
        curr_size: u64,
        added: RowSet,
        modified: RowSet,
    ) -> Self {
        let prev = RowSet::flat(prev_size);
        let curr = RowSet::flat(curr_size);
        let before = prev.minus(&removed);
        let after = curr.minus(&added);
        let mut runs = Vec::new();
        let mut later = after.ranges().iter().map(|r| (r.first(), r.last()));
        let mut target: Option<(RowKey, RowKey)> = later.next();
        for range in before.ranges() {
            let mut first = range.first();
            while first <= range.last() {
                let Some(to) = target.as_mut() else {
                    panic!("fewer rows stayed after the cycle than before it");
                };
                let length = (range.last() - first + 1).min(to.1 - to.0 + 1);
                runs.push(ShiftRange {
                    first,
                    last: first + length - 1,
                    delta: to.0 - first,
                });
                first += length;
                to.0 += length;
                if to.0 > to.1 {
                    target = later.next();
                }
            }
        }
        Self {
            prev,
            curr,
            removed,
            added,
            modified,
            runs,
        }
    }

    /// The change of a group whose inner keys are the right row keys, as the right table
    /// reported it.
    pub(super) fn keys(
        prev: RowSet,
        curr: RowSet,
        removed: RowSet,
        added: RowSet,
        modified: RowSet,
        shifted: &RowSetShiftData,
    ) -> Self {
        let staying = prev.minus(&removed);
        let mut runs = Vec::new();
        for range in staying.ranges() {
            let (mut first, range_last) = (range.first(), range.last());
            // Split the range where a shift range starts or ends.
            for shift in shifted.ranges() {
                if shift.last < first || shift.first > range_last {
                    continue;
                }
                if shift.first > first {
                    runs.push(ShiftRange {
                        first,
                        last: shift.first - 1,
                        delta: 0,
                    });
                    first = shift.first;
                }
                let last = shift.last.min(range_last);
                runs.push(ShiftRange {
                    first,
                    last,
                    delta: shift.delta,
                });
                first = last + 1;
            }
            if first <= range_last {
                runs.push(ShiftRange {
                    first,
                    last: range_last,
                    delta: 0,
                });
            }
        }
        Self {
            prev,
            curr,
            removed,
            added,
            modified,
            runs,
        }
    }

    /// Whether a row that stayed has another inner key now.
    fn moves(&self) -> bool {
        self.runs.iter().any(|run| run.delta != 0)
    }

    /// Whether the cycle changed nothing a region shows.
    pub(super) fn is_identity(&self) -> bool {
        self.removed.is_empty()
            && self.added.is_empty()
            && self.modified.is_empty()
            && !self.moves()
    }

    /// The inner keys, after the cycle, of the rows that stayed.
    fn staying(&self) -> RowSet {
        self.curr.minus(&self.added)
    }
}

/// The result's change, gathered region by region.
#[derive(Default)]
pub(super) struct Downstream {
    added: Vec<RowRange>,
    removed: Vec<RowRange>,
    modified: Vec<RowRange>,
    shifts: Vec<ShiftRange>,
}

fn place(out: &mut Vec<RowRange>, inner: &RowSet, offset: RowKey) {
    out.extend(
        inner
            .ranges()
            .iter()
            .map(|range| RowRange::new(range.first() + offset, range.last() + offset)),
    );
}

impl Downstream {
    /// A left row that is gone, or that left its key: its region before the cycle.
    pub(super) fn remove_region(&mut self, prev_offset: RowKey, inner: &RowSet) {
        place(&mut self.removed, inner, prev_offset);
    }

    /// A left row that arrived, or that took a new key: its region now.
    pub(super) fn add_region(&mut self, curr_offset: RowKey, inner: &RowSet) {
        place(&mut self.added, inner, curr_offset);
    }

    /// A left row that stayed in its group, whose region moves from `prev_offset` to
    /// `curr_offset`. When the left row was modified every row of its region that stayed
    /// is modified.
    ///
    /// Call in increasing order of left row keys. A region whose rows keep their inner keys
    /// moves as a whole, the full `1 << prev_bits` keys of it, as Java shifts it.
    ///
    /// Behavior: the uses of io.deephaven.engine.rowset.RowSetShiftData.Builder#shiftRange
    /// and #appendShiftData in io.deephaven.engine.table.impl.CrossJoinHelper
    pub(super) fn survivor(
        &mut self,
        prev_offset: RowKey,
        prev_bits: u32,
        curr_offset: RowKey,
        change: &InnerChange,
        left_modified: bool,
    ) {
        place(&mut self.removed, &change.removed, prev_offset);
        place(&mut self.added, &change.added, curr_offset);
        if left_modified {
            place(&mut self.modified, &change.staying(), curr_offset);
        } else {
            place(&mut self.modified, &change.modified, curr_offset);
        }
        let delta = curr_offset - prev_offset;
        if !change.moves() {
            if delta != 0 {
                self.shifts.push(ShiftRange {
                    first: prev_offset,
                    last: prev_offset + (1i64 << prev_bits) - 1,
                    delta,
                });
            }
            return;
        }
        for run in &change.runs {
            self.shifts.push(ShiftRange {
                first: run.first + prev_offset,
                last: run.last + prev_offset,
                delta: run.delta + delta,
            });
        }
    }

    /// The change, with `modified_columns` if anything was modified.
    pub(super) fn finish(
        self,
        modified_columns: ModifiedColumnSet,
    ) -> Result<TableUpdate, TableFailure> {
        let shifted = RowSetShiftData::new(self.shifts).map_err(failure)?;
        let modified = RowSet::from_ranges(self.modified);
        let modified_columns = if modified.is_empty() {
            ModifiedColumnSet::empty()
        } else {
            modified_columns
        };
        Ok(TableUpdate {
            added: RowSet::from_ranges(self.added),
            removed: RowSet::from_ranges(self.removed),
            modified,
            shifted,
            modified_columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runs(change: &InnerChange) -> Vec<(RowKey, RowKey, i64)> {
        change
            .runs
            .iter()
            .map(|run| (run.first, run.last, run.delta))
            .collect()
    }

    #[test]
    fn positions_pair_the_rows_that_stayed_in_order() {
        // Six rows; 1 and 4 leave, two arrive at positions 0 and 3 of the seven after.
        let change = InnerChange::positions(
            6,
            RowSet::from_keys([1, 4]),
            6,
            RowSet::from_keys([0, 3]),
            RowSet::empty(),
        );
        // Before: 0 2 3 5 stay. After: 1 2 4 5 hold them.
        assert_eq!(runs(&change), [(0, 0, 1), (2, 2, 0), (3, 3, 1), (5, 5, 0)]);
        assert!(!change.is_identity());
        assert_eq!(change.staying(), RowSet::from_keys([1, 2, 4, 5]));
    }

    #[test]
    fn an_unchanged_group_is_the_identity() {
        let change =
            InnerChange::positions(3, RowSet::empty(), 3, RowSet::empty(), RowSet::empty());
        assert!(change.is_identity());
        assert_eq!(runs(&change), [(0, 2, 0)]);
        assert!(InnerChange::unchanged(RowSet::from_keys([1, 5])).is_identity());
    }

    #[test]
    fn keys_split_the_staying_rows_at_shift_ranges() {
        let shifted = RowSetShiftData::new(vec![ShiftRange {
            first: 3,
            last: 4,
            delta: 2,
        }])
        .unwrap();
        let change = InnerChange::keys(
            RowSet::from_keys([0, 1, 3, 4, 8]),
            RowSet::from_keys([0, 5, 6, 8]),
            RowSet::from_keys([1]),
            RowSet::empty(),
            RowSet::empty(),
            &shifted,
        );
        assert_eq!(runs(&change), [(0, 0, 0), (3, 4, 2), (8, 8, 0)]);
    }

    #[test]
    fn a_moving_region_shifts_whole_unless_its_rows_move_inside_it() {
        let mut down = Downstream::default();
        let still = InnerChange::unchanged(RowSet::flat(2));
        down.survivor(4, 2, 12, &still, true);
        let moved = InnerChange::positions(
            2,
            RowSet::from_keys([0]),
            2,
            RowSet::from_keys([1]),
            RowSet::empty(),
        );
        down.survivor(16, 2, 32, &moved, false);
        let update = down.finish(ModifiedColumnSet::of([0])).unwrap();
        let shifts: Vec<_> = update
            .shifted
            .ranges()
            .iter()
            .map(|s| (s.first, s.last, s.delta))
            .collect();
        assert_eq!(shifts, [(4, 7, 8), (17, 17, 15)]);
        assert_eq!(update.removed, RowSet::from_keys([16]));
        assert_eq!(update.added, RowSet::from_keys([33]));
        assert_eq!(update.modified, RowSet::from_keys([12, 13]));
    }
}
