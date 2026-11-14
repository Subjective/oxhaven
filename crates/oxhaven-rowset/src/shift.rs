//! Helpers around the contract type `RowSetShiftData`: builders, the order in which shifts can
//! be applied to stored data one at a time, and restriction to a row set.
//!
//! Moving a row set back to pre-shift keys (Java's `RowSetShiftData#unapply(WritableRowSet)`) is
//! `data.inverse().apply(rows)` on the contract type and needs no helper here.

use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftError, ShiftRange};

/// Calls `f` with every shift, in an order that loses no data when each shift is carried out
/// immediately as a move of stored values (memmove semantics).
///
/// Shifts are stored by increasing key. Within a run of shifts that move up, the highest moves
/// first; within a run that moves down, the lowest moves first.
///
/// Behavior: io.deephaven.engine.rowset.RowSetShiftData#apply(RowKeyRangeShiftCallback)
pub fn for_each_shift(data: &RowSetShiftData, mut f: impl FnMut(ShiftRange)) {
    for run in same_direction_runs(data) {
        if run[0].delta > 0 {
            run.iter().rev().for_each(|s| f(*s));
        } else {
            run.iter().for_each(|s| f(*s));
        }
    }
}

/// Calls `f` with the reverse of every shift (post-shift range, negated delta), in an order
/// that loses no data when each is carried out immediately. Moves stored values from post-shift
/// keys back to pre-shift keys.
///
/// Behavior: io.deephaven.engine.rowset.RowSetShiftData#unapply(RowKeyRangeShiftCallback)
pub fn for_each_unshift(data: &RowSetShiftData, mut f: impl FnMut(ShiftRange)) {
    let undo = |s: &ShiftRange| ShiftRange {
        first: s.first + s.delta,
        last: s.last + s.delta,
        delta: -s.delta,
    };
    for run in same_direction_runs(data) {
        if run[0].delta > 0 {
            run.iter().for_each(|s| f(undo(s)));
        } else {
            run.iter().rev().for_each(|s| f(undo(s)));
        }
    }
}

fn same_direction_runs(data: &RowSetShiftData) -> impl Iterator<Item = &[ShiftRange]> {
    data.ranges()
        .chunk_by(|a, b| (a.delta < 0) == (b.delta < 0))
}

/// The shifts whose pre-shift range contains at least one key of `rows`.
///
/// Behavior: io.deephaven.engine.rowset.RowSetShiftData#intersect
pub fn intersect(data: &RowSetShiftData, rows: &RowSet) -> RowSetShiftData {
    let kept = data
        .ranges()
        .iter()
        .filter(|s| has_key_in(rows, s.first, s.last))
        .copied()
        .collect();
    match RowSetShiftData::new(kept) {
        Ok(kept) => kept,
        Err(e) => panic!("a subset of valid shifts is valid: {e}"),
    }
}

/// Whether `rows` has a key in `first..=last`. An inverted range holds no key.
///
/// Behavior: io.deephaven.engine.rowset.RowSet#overlapsRange
fn has_key_in(rows: &RowSet, first: RowKey, last: RowKey) -> bool {
    let ranges = rows.ranges();
    let i = ranges.partition_point(|r| r.last() < first);
    first <= last && ranges.get(i).is_some_and(|r| r.first() <= last)
}

/// Collects shifts one range at a time, in increasing key order.
///
/// As in Java, a range with a zero delta or with `last < first` is ignored, and a range that
/// directly follows the previous one with the same delta extends it. Unlike Java, which throws
/// from `shiftRange`, ranges that overlap or reorder keys are reported by [`build`](Self::build).
///
/// Java's `Builder#appendShiftData`, `#limitPreviousShiftFor` and
/// `#getMinimumValidBeginForNextDelta` are not ported here; they serve `merge` and belong with it.
///
/// Behavior: io.deephaven.engine.rowset.RowSetShiftData.Builder
#[derive(Clone, Debug, Default)]
pub struct ShiftBuilder {
    shifts: Vec<ShiftRange>,
}

impl ShiftBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether no shift has been kept so far.
    pub fn is_empty(&self) -> bool {
        self.shifts.is_empty()
    }

    /// The greatest key assigned to a shift so far; `None` where Java returns -1.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetShiftData.Builder#lastShiftEnd
    pub fn last_shift_end(&self) -> Option<RowKey> {
        self.shifts.last().map(|s| s.last)
    }

    /// Moves the keys `first..=last` by `delta`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetShiftData.Builder#shiftRange
    pub fn shift_range(&mut self, first: RowKey, last: RowKey, delta: i64) {
        if delta == 0 || last < first {
            return;
        }
        match self.shifts.last_mut() {
            Some(prev) if prev.delta == delta && prev.last + 1 == first => prev.last = last,
            _ => self.shifts.push(ShiftRange { first, last, delta }),
        }
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetShiftData.Builder#build
    pub fn build(self) -> Result<RowSetShiftData, ShiftError> {
        RowSetShiftData::new(self.shifts)
    }
}

/// Collects shifts for a table whose pre-shift keys are known, and merges neighbouring ranges
/// that have the same delta when no pre-shift key lies between them. Ranges that hold no
/// pre-shift key are dropped. The result moves the pre-shift keys exactly as the uncoalesced
/// shifts would, with fewer ranges.
///
/// Java requires ranges to be presented in the order `for_each_shift` would visit them and
/// throws on some violations. This builder sorts, so it accepts any order; ranges that overlap
/// or reorder keys are reported by [`build`](Self::build).
///
/// Behavior: io.deephaven.engine.rowset.RowSetShiftData.SmartCoalescingBuilder
#[derive(Clone, Debug)]
pub struct CoalescingShiftBuilder {
    pre_shift_keys: RowSet,
    shifts: Vec<ShiftRange>,
}

impl CoalescingShiftBuilder {
    pub fn new(pre_shift_keys: RowSet) -> Self {
        Self {
            pre_shift_keys,
            shifts: Vec::new(),
        }
    }

    /// Whether no shift has been kept so far.
    pub fn is_empty(&self) -> bool {
        self.shifts.is_empty()
    }

    /// Moves the keys `first..=last` by `delta`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetShiftData.SmartCoalescingBuilder#shiftRange
    pub fn shift_range(&mut self, first: RowKey, last: RowKey, delta: i64) {
        if delta == 0 || last < first || !has_key_in(&self.pre_shift_keys, first, last) {
            return;
        }
        self.shifts.push(ShiftRange { first, last, delta });
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetShiftData.SmartCoalescingBuilder#build
    pub fn build(mut self) -> Result<RowSetShiftData, ShiftError> {
        self.shifts.sort_by_key(|s| s.first);
        let mut out: Vec<ShiftRange> = Vec::with_capacity(self.shifts.len());
        for s in self.shifts {
            match out.last_mut() {
                Some(prev)
                    if prev.delta == s.delta
                        && prev.last < s.first
                        && !has_key_in(&self.pre_shift_keys, prev.last + 1, s.first - 1) =>
                {
                    prev.last = s.last;
                }
                _ => out.push(s),
            }
        }
        RowSetShiftData::new(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_key_in_checks_the_inclusive_range() {
        let rows = RowSet::from_keys([5, 6, 20]);
        assert!(has_key_in(&rows, 0, 5));
        assert!(has_key_in(&rows, 6, 6));
        assert!(has_key_in(&rows, 7, 20));
        assert!(!has_key_in(&rows, 0, 4));
        assert!(!has_key_in(&rows, 7, 19));
        assert!(!has_key_in(&rows, 21, 30));
        // The gap between two ranges that touch is inverted and holds nothing, even when a
        // range of `rows` spans both of its bounds.
        assert!(!has_key_in(&rows, 6, 5));
    }
}
