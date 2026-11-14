//! Row-key shifts: ranges of keys that moved by a fixed delta within one update.
//!
//! Port of the observable behavior of `io.deephaven.engine.rowset.RowSetShiftData`.

use crate::rowset::{RowKey, RowRange, RowSet};
use std::fmt;

/// Keys in `first..=last` (pre-shift key space) move by `delta`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ShiftRange {
    pub first: RowKey,
    pub last: RowKey,
    pub delta: i64,
}

/// Why a set of shifts, or its application, is invalid.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ShiftError {
    /// A range is empty or negative, or applying it would move a key below zero.
    BadRange(ShiftRange),
    /// Ranges are not sorted by `first`, or overlap before or after shifting. Shifts never
    /// reorder keys.
    OutOfOrder(ShiftRange, ShiftRange),
    /// A shifted key landed on a key that did not move.
    Collision(RowKey),
}

impl fmt::Display for ShiftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShiftError::BadRange(r) => write!(f, "invalid shift range {r:?}"),
            ShiftError::OutOfOrder(a, b) => {
                write!(f, "shift ranges overlap or reorder keys: {a:?} then {b:?}")
            }
            ShiftError::Collision(k) => write!(f, "shifted key collides with unmoved key {k}"),
        }
    }
}

impl std::error::Error for ShiftError {}

/// An ordered list of non-overlapping shifts that preserves the relative order of all keys.
#[derive(Clone, PartialEq, Eq, Hash, Default, Debug)]
pub struct RowSetShiftData {
    shifts: Vec<ShiftRange>,
}

impl RowSetShiftData {
    /// Port of `RowSetShiftData#EMPTY`.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Validates and builds shift data. Ranges must be sorted by `first`, must not overlap
    /// before or after shifting, and must keep keys in the same relative order.
    ///
    /// As in `RowSetShiftData.Builder#shiftRange`, a range with a zero delta is dropped and
    /// adjacent ranges with the same delta are merged, so equal shifts compare equal.
    pub fn new(shifts: Vec<ShiftRange>) -> Result<Self, ShiftError> {
        let mut out: Vec<ShiftRange> = Vec::with_capacity(shifts.len());
        for s in shifts {
            if s.first < 0 || s.first > s.last {
                return Err(ShiftError::BadRange(s));
            }
            if s.delta == 0 {
                continue;
            }
            match out.last_mut() {
                Some(prev)
                    if prev.last >= s.first || prev.last + prev.delta >= s.first + s.delta =>
                {
                    return Err(ShiftError::OutOfOrder(*prev, s));
                }
                Some(prev) if prev.delta == s.delta && prev.last + 1 == s.first => {
                    prev.last = s.last;
                }
                _ => out.push(s),
            }
        }
        Ok(Self { shifts: out })
    }

    pub fn is_empty(&self) -> bool {
        self.shifts.is_empty()
    }

    pub fn ranges(&self) -> &[ShiftRange] {
        &self.shifts
    }

    /// Where a pre-shift key ends up.
    pub fn apply_to_key(&self, key: RowKey) -> RowKey {
        let i = self.shifts.partition_point(|s| s.last < key);
        match self.shifts.get(i) {
            Some(s) if s.first <= key => key + s.delta,
            _ => key,
        }
    }

    /// Moves the keys of `rows` (pre-shift key space) to their post-shift positions.
    ///
    /// Port of `RowSetShiftData#apply(WritableRowSet)`. All shifts apply at once, so one
    /// range's destination may overlap another range's source.
    pub fn apply(&self, rows: &RowSet) -> Result<RowSet, ShiftError> {
        if self.shifts.is_empty() {
            return Ok(rows.clone());
        }
        let sources =
            RowSet::from_ranges(self.shifts.iter().map(|s| RowRange::new(s.first, s.last)));
        let staying = rows.minus(&sources);
        let mut moved = RowSet::empty();
        for s in &self.shifts {
            let part = rows.sub_set_by_key_range(s.first, s.last);
            let shifted = part.shift(s.delta).map_err(|_| ShiftError::BadRange(*s))?;
            moved.insert(&shifted);
        }
        if let Some(k) = staying.intersect(&moved).first_row_key() {
            return Err(ShiftError::Collision(k));
        }
        Ok(staying.union(&moved))
    }

    /// The inverse shifts: applying them to post-shift keys gives the pre-shift keys.
    ///
    /// Port of the effect of `RowSetShiftData#unapply`.
    pub fn inverse(&self) -> RowSetShiftData {
        RowSetShiftData {
            shifts: self
                .shifts
                .iter()
                .map(|s| ShiftRange {
                    first: s.first + s.delta,
                    last: s.last + s.delta,
                    delta: -s.delta,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shift(first: RowKey, last: RowKey, delta: i64) -> ShiftRange {
        ShiftRange { first, last, delta }
    }

    #[test]
    fn apply_moves_only_keys_inside_shift_ranges() {
        let data = RowSetShiftData::new(vec![shift(10, 19, -5), shift(30, 39, 100)]).unwrap();
        let rows = RowSet::from_keys([1, 10, 12, 25, 30, 39]);
        assert_eq!(
            data.apply(&rows).unwrap(),
            RowSet::from_keys([1, 5, 7, 25, 130, 139])
        );
        assert_eq!(data.apply_to_key(12), 7);
        assert_eq!(data.apply_to_key(25), 25);
    }

    #[test]
    fn adjacent_ranges_can_shift_into_each_others_source() {
        // Both ranges move up by 5; the first lands where the second started.
        let data = RowSetShiftData::new(vec![shift(0, 4, 5), shift(5, 9, 5)]).unwrap();
        let rows = RowSet::from_range(0, 9);
        assert_eq!(data.apply(&rows).unwrap(), RowSet::from_range(5, 14));
    }

    #[test]
    fn inverse_undoes_apply() {
        let data = RowSetShiftData::new(vec![shift(10, 19, -5), shift(30, 39, 100)]).unwrap();
        let rows = RowSet::from_keys([1, 10, 12, 25, 30, 39]);
        let after = data.apply(&rows).unwrap();
        assert_eq!(data.inverse().apply(&after).unwrap(), rows);
    }

    #[test]
    fn rejects_shifts_that_reorder_overlap_or_go_negative() {
        assert!(matches!(
            RowSetShiftData::new(vec![shift(0, 4, 20), shift(10, 14, 1)]),
            Err(ShiftError::OutOfOrder(..))
        ));
        assert!(matches!(
            RowSetShiftData::new(vec![shift(0, 9, 1), shift(5, 14, 1)]),
            Err(ShiftError::OutOfOrder(..))
        ));
        assert!(matches!(
            RowSetShiftData::new(vec![shift(4, 3, 1)]),
            Err(ShiftError::BadRange(_))
        ));
        let below_zero = RowSetShiftData::new(vec![shift(3, 4, -4)]).unwrap();
        assert!(matches!(
            below_zero.apply(&RowSet::from_keys([3])),
            Err(ShiftError::BadRange(_))
        ));
    }

    #[test]
    fn zero_deltas_are_dropped_and_adjacent_equal_deltas_merge() {
        let merged = RowSetShiftData::new(vec![shift(0, 4, 5), shift(5, 9, 5)]).unwrap();
        assert_eq!(merged, RowSetShiftData::new(vec![shift(0, 9, 5)]).unwrap());
        assert!(RowSetShiftData::new(vec![shift(3, 4, 0)])
            .unwrap()
            .is_empty());
        let apart = RowSetShiftData::new(vec![shift(0, 4, 5), shift(6, 9, 5)]).unwrap();
        assert_eq!(apart.ranges().len(), 2);
    }

    #[test]
    fn landing_on_an_unmoved_key_is_a_collision() {
        let data = RowSetShiftData::new(vec![shift(0, 0, 5)]).unwrap();
        assert_eq!(
            data.apply(&RowSet::from_keys([0, 5])),
            Err(ShiftError::Collision(5))
        );
    }
}
