//! The row keys of an ungrouped table.
//!
//! Every row of the parent owns a slot of `1 << base` row keys of the result, which starts
//! at the parent's row key shifted left by `base`. The elements of the row's arrays are the
//! first keys of its slot, so the result is in the order of the parent's rows and, within a
//! row, of the array. The base is the same for every row; it grows when an array arrives
//! that does not fit in a slot.

use super::UngroupError;
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::TableFailure;

/// The smallest base of a new ungrouped table.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#minimumUngroupBase, the default of
/// the property `QueryTable.minimumUngroupBase`
pub(super) const MINIMUM_BASE: u32 = 10;

/// The base whose slots hold an array of `max_size` elements.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation#determineRequiredBase
pub(super) fn required_base(max_size: u64) -> u32 {
    if max_size == 0 {
        1
    } else {
        u64::BITS - (max_size - 1).leading_zeros()
    }
}

/// The first key of the slot of the parent's row `key`, or `None` if the slot does not fit
/// in the row keys.
///
/// Behavior: the check of io.deephaven.engine.table.impl.UngroupOperation#getUngroupRowset,
/// which also asserts that the base is at most 62
fn slot_start(key: RowKey, base: u32) -> Option<RowKey> {
    if base > 62 {
        return None;
    }
    let start = key.wrapping_shl(base);
    let fits = start >> base == key && start.wrapping_add(1 << base) >= 0;
    fits.then_some(start)
}

fn overflow(last_row_key: RowKey, base: u32) -> UngroupError {
    UngroupError::KeyOverflow { last_row_key, base }
}

/// The keys of the result for the parent's `rows`, whose arrays have `sizes` elements.
///
/// An error if the slot of the last of `rows` does not fit in the row keys, whatever the
/// sizes are.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation#getUngroupRowset
pub(super) fn slots(rows: &RowSet, sizes: &[u64], base: u32) -> Result<RowSet, UngroupError> {
    debug_assert_eq!(rows.size(), sizes.len() as u64);
    if let Some(last) = rows.last_row_key() {
        if slot_start(last, base).is_none() {
            return Err(overflow(last, base));
        }
    }
    let ranges = rows.iter().zip(sizes).filter(|(_, size)| **size != 0);
    Ok(RowSet::from_ranges(ranges.map(|(key, size)| {
        let first = key << base;
        RowRange::new(first, first + (*size as RowKey - 1))
    })))
}

/// How many elements each of the parent's `rows` has in `result`, a row set of the result
/// with slots of `base`. `rows` and `result` are of one cycle.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation#computeSizeFromResultRowset
pub(super) fn sizes_in(result: &RowSet, base: u32, rows: &RowSet) -> Vec<u64> {
    let ranges = result.ranges();
    let mut next = 0;
    let sizes = rows.iter().map(|key| {
        // A slot that does not fit in the row keys has no keys in the result.
        let Some(first) = slot_start(key, base) else {
            return 0;
        };
        let last = first + ((1 << base) - 1);
        while next < ranges.len() && ranges[next].last() < first {
            next += 1;
        }
        match ranges.get(next) {
            // The keys of a row are the first of its slot.
            Some(range) if range.first() <= first => (range.last().min(last) - first + 1) as u64,
            _ => 0,
        }
    });
    sizes.collect()
}

fn shift_data(shifts: Vec<ShiftRange>) -> Result<RowSetShiftData, UngroupError> {
    RowSetShiftData::new(shifts).map_err(|error| {
        UngroupError::Table(TableFailure {
            message: format!("ungroup made an invalid shift: {error}"),
        })
    })
}

/// The shifts of the result for the parent's shifts `upstream`, with slots of `base`
/// before and after. `rows` are the keys of the result that stay, in the key space before
/// the shifts.
///
/// Java does not look whether the slot a row moves to fits in the row keys; here that is
/// an error.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation.UngroupListener#processShifts
pub(super) fn shifts(
    rows: &RowSet,
    base: u32,
    upstream: &RowSetShiftData,
) -> Result<RowSetShiftData, UngroupError> {
    let mut shifts = Vec::new();
    for shift in upstream.ranges() {
        // Rows whose slot does not fit have no keys.
        let Some(first) = slot_start(shift.first, base) else {
            continue;
        };
        let last = slot_start(shift.last, base).map_or(RowKey::MAX, |start| {
            // The whole slot fits.
            start + ((1 << base) - 1)
        });
        for range in rows.sub_set_by_key_range(first, last).ranges() {
            let to = (range.last() >> base) + shift.delta;
            if slot_start(to, base).is_none() {
                return Err(overflow(to, base));
            }
            shifts.push(ShiftRange {
                first: range.first(),
                last: range.last(),
                delta: shift.delta << base,
            });
        }
    }
    shift_data(shifts)
}

/// The shifts that move every key of `rows` from its slot of `old_base` to the slot of
/// `new_base` of the row its parent row is after the parent's shifts `upstream`. `rows`
/// are the keys of the result that stay, in the key space before the shifts.
///
/// Java does not look whether the new slot fits in the row keys; here that is an error.
///
/// Behavior: io.deephaven.engine.table.impl.UngroupOperation.UngroupListener#rebase and
/// its `ShiftWrapper`
pub(super) fn rebase(
    rows: &RowSet,
    old_base: u32,
    new_base: u32,
    upstream: &RowSetShiftData,
) -> Result<RowSetShiftData, UngroupError> {
    let slot: RowKey = 1 << old_base;
    let mut shifts = Vec::new();
    for range in rows.ranges() {
        // A range is one slot, or several full slots and the keys of one more.
        let mut first = range.first();
        loop {
            let last = range.last().min(first + (slot - 1));
            let parent = upstream.apply_to_key(first >> old_base);
            let Some(to) = slot_start(parent, new_base) else {
                return Err(overflow(parent, new_base));
            };
            shifts.push(ShiftRange {
                first,
                last,
                delta: to - first,
            });
            if last == range.last() {
                break;
            }
            first = last + 1;
        }
    }
    shift_data(shifts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ranges: &[(RowKey, RowKey)]) -> RowSet {
        RowSet::from_ranges(ranges.iter().map(|(first, last)| RowRange::new(*first, *last)))
    }

    fn shift_ranges(data: &RowSetShiftData) -> Vec<(RowKey, RowKey, i64)> {
        let ranges = data.ranges().iter();
        ranges.map(|s| (s.first, s.last, s.delta)).collect()
    }

    fn upstream(ranges: &[(RowKey, RowKey, i64)]) -> RowSetShiftData {
        let ranges = ranges.iter().map(|&(first, last, delta)| ShiftRange {
            first,
            last,
            delta,
        });
        RowSetShiftData::new(ranges.collect()).unwrap()
    }

    /// The values of `64 - Long.numberOfLeadingZeros(maxSize - 1)`, and 1 for no elements.
    #[test]
    fn the_base_holds_the_largest_array() {
        let bases = [0, 1, 2, 3, 4, 5, 8, 9, 1024, 1025].map(required_base);
        assert_eq!(bases, [1, 0, 1, 2, 2, 3, 3, 4, 10, 11]);
        assert_eq!(required_base(1 << 31), 31);
        assert_eq!(required_base((1 << 31) + 1), 32);
    }

    #[test]
    fn a_row_gets_the_first_keys_of_its_slot() {
        let rows = RowSet::from_keys([0, 1, 3, 7]);
        // Row 1 has no elements, row 0 fills its slot and joins the keys of row 1's
        // neighbour only if they are adjacent.
        let result = slots(&rows, &[4, 0, 2, 1], 2).unwrap();
        assert_eq!(result, set(&[(0, 3), (12, 13), (28, 28)]));
        let result = slots(&rows, &[4, 4, 0, 3], 2).unwrap();
        assert_eq!(result, set(&[(0, 7), (28, 30)]));
        assert_eq!(slots(&RowSet::empty(), &[], 2).unwrap(), RowSet::empty());
    }

    /// The two conditions of Java's check, at the last row whatever its size: the key
    /// comes back from the shift, and the end of its slot is not negative.
    #[test]
    fn a_slot_beyond_the_row_keys_is_refused() {
        let last = RowKey::MAX >> 10;
        let rows = RowSet::from_keys([0, last]);
        // The slot ends at the last row key: its end plus one is negative.
        assert_eq!(
            slots(&rows, &[1, 0], 10).unwrap_err(),
            UngroupError::KeyOverflow {
                last_row_key: last,
                base: 10
            }
        );
        let rows = RowSet::from_keys([0, last - 1]);
        let result = slots(&rows, &[1, 2], 10).unwrap();
        assert_eq!(result.last_row_key(), Some(((last - 1) << 10) + 1));
        // The key does not come back from the shift.
        let rows = RowSet::from_keys([1 << 60]);
        assert_eq!(
            slots(&rows, &[1], 10).unwrap_err(),
            UngroupError::KeyOverflow {
                last_row_key: 1 << 60,
                base: 10
            }
        );
        assert_eq!(
            slots(&rows, &[1], 10).unwrap_err().to_string(),
            "Key overflow detected, perhaps you should flatten your table before calling \
             ungroup: lastRowKey=1152921504606846976, base=10"
        );
    }

    #[test]
    fn sizes_are_read_back_from_the_result() {
        let rows = RowSet::from_keys([0, 1, 2, 3, 5, 7, 9]);
        let sizes = [4, 4, 0, 2, 0, 1, 0];
        let result = slots(&rows, &sizes, 2).unwrap();
        assert_eq!(sizes_in(&result, 2, &rows), sizes);
        // Of some rows, and of rows that are not there.
        let some = RowSet::from_keys([1, 4, 7, 8, 1 << 61]);
        assert_eq!(sizes_in(&result, 2, &some), [4, 0, 1, 0, 0]);
        assert_eq!(sizes_in(&RowSet::empty(), 2, &some), [0; 5]);
    }

    #[test]
    fn a_shift_moves_the_keys_that_exist() {
        let rows = RowSet::from_keys([0, 1, 2, 5, 6]);
        let result = slots(&rows, &[1, 4, 4, 0, 3], 2).unwrap();
        // Rows 1 and 2 are full and move as one range; row 5 has no keys; the range of
        // the parent reaches past its last row.
        let moved = shifts(&result, 2, &upstream(&[(1, 2, 1), (5, 1 << 62, 10)])).unwrap();
        assert_eq!(shift_ranges(&moved), [(4, 11, 4), (24, 26, 40)]);
        // A shift of rows without keys moves nothing.
        let moved = shifts(&result, 2, &upstream(&[(3, 5, -1)])).unwrap();
        assert!(moved.is_empty());
        assert!(shifts(&RowSet::empty(), 2, &upstream(&[(3, 5, -1)]))
            .unwrap()
            .is_empty());
        // Down.
        let moved = shifts(&result, 2, &upstream(&[(6, 6, -3)])).unwrap();
        assert_eq!(shift_ranges(&moved), [(24, 26, -12)]);
    }

    #[test]
    fn a_shift_beyond_the_row_keys_is_refused() {
        let result = slots(&RowSet::from_keys([3]), &[2], 10).unwrap();
        let far = RowKey::MAX >> 10;
        assert_eq!(
            shifts(&result, 10, &upstream(&[(3, 3, far - 3)])).unwrap_err(),
            UngroupError::KeyOverflow {
                last_row_key: far,
                base: 10
            }
        );
    }

    #[test]
    fn a_new_base_moves_every_slot() {
        let rows = RowSet::from_keys([0, 1, 2, 5, 6]);
        let result = slots(&rows, &[2, 2, 1, 0, 2], 1).unwrap();
        assert_eq!(result, set(&[(0, 4), (12, 13)]));
        // Row 0 stays; rows 1 and 2 were one range with row 0.
        let moved = rebase(&result, 1, 3, &RowSetShiftData::empty()).unwrap();
        assert_eq!(
            shift_ranges(&moved),
            [(2, 3, 6), (4, 4, 12), (12, 13, 36)]
        );
        assert_eq!(
            moved.apply(&result).unwrap(),
            slots(&rows, &[2, 2, 1, 0, 2], 3).unwrap()
        );
        // With the parent's shifts: row 2 to 3, row 6 to 4.
        let parent = upstream(&[(2, 2, 1), (6, 6, -2)]);
        let moved = rebase(&result, 1, 3, &parent).unwrap();
        let after = RowSet::from_keys([0, 1, 3, 4]);
        assert_eq!(
            moved.apply(&result).unwrap(),
            slots(&after, &[2, 2, 1, 2], 3).unwrap()
        );
    }

    #[test]
    fn a_new_base_beyond_the_row_keys_is_refused() {
        let key = 1 << 50;
        let result = slots(&RowSet::from_keys([key]), &[2], 10).unwrap();
        assert_eq!(
            rebase(&result, 10, 13, &RowSetShiftData::empty()).unwrap_err(),
            UngroupError::KeyOverflow {
                last_row_key: key,
                base: 13
            }
        );
    }
}
