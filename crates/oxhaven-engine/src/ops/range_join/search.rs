//! The search for the range of right rows of each left row within one bucket.
//!
//! Java finds the ranges of a bucket's left rows together, 2048 rows at a time: it sorts the
//! rows by their bound and walks them along the right values, searching once for a row and
//! reusing what it found for the rows that follow. For two cases the answer of a row then
//! depends on the rows read with it, so the walk is kept as it is and not replaced by a
//! search per row:
//!
//! - With `->`, a row whose end lies between two right values takes the row after its
//!   range. It does not when a row of the same read ends exactly at the right value
//!   before: the walk gives the rows after that one the same end without looking again.
//! - Of `-0.0` and `0.0` the right side keeps the one that comes first. A bound that is
//!   the other zero is found equal only where the walk happens to stand at that value,
//!   and otherwise sorts next to it.
//!
//! Both are recorded from Java in `testdata` (`range_join_rules`, `range_join_chunks`,
//! `range_join_doubles`).

use super::{RangeEndRule, RangeStartRule};
use std::cmp::Ordering;

/// How many left rows of a bucket Java reads and searches at once.
///
/// Behavior: `MAX_LEFT_CHUNK_CAPACITY` of
/// io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation, which is
/// io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource#BLOCK_SIZE
pub(super) const LEFT_CHUNK: usize = 2048;

/// A range value that is not null, as the kernel of its type compares it.
pub(super) trait RangeValue {
    /// A value no range can be made of. Only floats have one.
    ///
    /// Behavior: `isNaN` of the `RangeSearchKernel` of each type
    fn is_nan(&self) -> bool {
        false
    }

    /// The order of two values that are not NaN. It is the `eq`, `lt` and `leq` of the
    /// `RangeSearchKernel` of each type, the order the left bounds are sorted in and the
    /// order the right values must be in: for these values they agree.
    ///
    /// Behavior: io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelDouble#lt and
    /// its siblings; the `doComparison` of the timsort kernels under
    /// io.deephaven.engine.table.impl.sort.timsort and of the kernels under
    /// io.deephaven.engine.table.impl.join.dupcompact
    fn order(&self, other: &Self) -> Ordering;

    /// The order Java's binary search uses, which tells `-0.0` from `0.0`.
    ///
    /// Behavior: java.util.Arrays#binarySearch(double[], int, int, double), called by
    /// io.deephaven.chunk.DoubleChunk#binarySearch
    fn search_order(&self, other: &Self) -> Ordering {
        self.order(other)
    }
}

/// Integers, and what Java reads as integers: `char`, `Boolean` as its byte and `Instant`
/// as its nanoseconds
/// (io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive).
impl RangeValue for i64 {
    fn order(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

/// `double`, and `float` widened, which keeps the order, the zeros and NaN.
impl RangeValue for f64 {
    fn is_nan(&self) -> bool {
        f64::is_nan(*self)
    }

    fn order(&self, other: &Self) -> Ordering {
        self.partial_cmp(other)
            .expect("NaN range values are set aside before any comparison")
    }

    fn search_order(&self, other: &Self) -> Ordering {
        // Equal values with different bits are the two zeros; the negative one is less.
        self.order(other)
            .then_with(|| other.is_sign_negative().cmp(&self.is_sign_negative()))
    }
}

/// A `String`, ordered as `String#compareTo` orders it: by UTF-16 code unit.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) struct Utf16(pub(super) Vec<u16>);

impl From<&str> for Utf16 {
    fn from(text: &str) -> Self {
        Utf16(text.encode_utf16().collect())
    }
}

impl RangeValue for Utf16 {
    fn order(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

/// The right range values of a bucket, without repeats.
pub(super) struct Runs<T> {
    /// The distinct values, ascending; of equal values the first.
    values: Vec<T>,
    /// The position in the bucket of the first row of each value.
    offsets: Vec<usize>,
    /// The number of rows in the bucket.
    rows: usize,
}

impl<T: RangeValue + Clone> Runs<T> {
    /// The runs of `values`, which are the range values of a bucket's right rows in row
    /// order, none of them null or NaN. If they are not ascending, the position of the
    /// first one that is less than the one before it.
    ///
    /// Behavior: io.deephaven.engine.table.impl.join.dupcompact.DoubleDupCompactKernel#compactDuplicatesPreferFirst
    /// and its siblings
    pub(super) fn of(values: &[T]) -> Result<Self, usize> {
        let mut runs: Runs<T> = Runs {
            values: Vec::new(),
            offsets: Vec::new(),
            rows: values.len(),
        };
        for (position, value) in values.iter().enumerate() {
            match runs.values.last().map(|last| last.order(value)) {
                Some(Ordering::Greater) => return Err(position),
                Some(Ordering::Equal) => {}
                Some(Ordering::Less) | None => {
                    runs.values.push(value.clone());
                    runs.offsets.push(position);
                }
            }
        }
        Ok(runs)
    }
}

/// Whether a range can be made of a start and an end. A null bound is open. When both
/// rules allow equality the start may equal the end; otherwise it must be less.
///
/// Behavior: `isValidPairAllowEqual` and `isValidPairDisallowEqual` of
/// io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelDouble and its siblings, and
/// which of them each pair of rules uses
fn is_valid<T: RangeValue>(
    start_rule: RangeStartRule,
    end_rule: RangeEndRule,
    start: Option<&T>,
    end: Option<&T>,
) -> bool {
    if start.is_some_and(T::is_nan) || end.is_some_and(T::is_nan) {
        return false;
    }
    let (Some(start), Some(end)) = (start, end) else {
        return true;
    };
    let allow_equal =
        start_rule != RangeStartRule::LessThan && end_rule != RangeEndRule::GreaterThan;
    match start.order(end) {
        Ordering::Less => true,
        Ordering::Equal => allow_equal,
        Ordering::Greater => false,
    }
}

/// Where `key` is among `values[low..]`: its index, or the index it would be inserted at.
/// The value at `low` is compared first, and as the kernels compare.
///
/// Behavior: the `eq(leftValue, rightValues.get(rightLowIndexInclusive)) ? ... :
/// rightValues.binarySearch(...)` at the top of each loop of
/// io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelDouble and its siblings. The
/// values are distinct and ascending in the search order as well, so any binary search
/// finds the same place.
fn find<T: RangeValue>(values: &[T], low: usize, key: &T) -> Result<usize, usize> {
    if key.order(&values[low]) == Ordering::Equal {
        return Ok(low);
    }
    values[low..]
        .binary_search_by(|probe| probe.search_order(key))
        .map(|index| index + low)
        .map_err(|index| index + low)
}

/// One walk of sorted left bounds along the right values.
struct Walk<'a, T> {
    /// The bounds of the rows, nulls first, then ascending.
    bounds: &'a [Option<&'a T>],
    /// The row of each bound.
    rows: &'a [usize],
    /// The next bound.
    next: usize,
    /// The position found for each row.
    found: &'a mut [usize],
}

impl<T: RangeValue> Walk<'_, T> {
    fn bound(&self) -> Option<&T> {
        self.bounds.get(self.next).copied().flatten()
    }

    fn set(&mut self, position: usize) {
        self.found[self.rows[self.next]] = position;
        self.next += 1;
    }

    /// Gives `position` to every following row whose bound is less than `value`, or with
    /// `or_equal` at most `value`.
    fn set_while(&mut self, value: &T, or_equal: bool, position: usize) {
        while let Some(bound) = self.bound() {
            match bound.order(value) {
                Ordering::Less => {}
                Ordering::Equal if or_equal => {}
                _ => break,
            }
            self.set(position);
        }
    }

    fn set_rest(&mut self, position: usize) {
        while self.next < self.bounds.len() {
            self.set(position);
        }
    }

    fn set_nulls(&mut self, position: usize) {
        while self.next < self.bounds.len() && self.bounds[self.next].is_none() {
            self.set(position);
        }
    }
}

/// For each row of `walk`, the position of the first right row of its range.
///
/// Behavior: `processRangeStartsLessThan`, `processRangeStartsLessThanEqual` and
/// `processRangeStartsLessThanEqualAllowPreceding` of
/// io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelDouble and its siblings
fn starts<T: RangeValue>(rule: RangeStartRule, right: &Runs<T>, mut walk: Walk<'_, T>) {
    // A null start is open: the range starts at the first right row.
    walk.set_nulls(0);
    let mut low = 0;
    while low < right.values.len() {
        let Some(bound) = walk.bound() else {
            return;
        };
        let found = find(&right.values, low, bound);
        // The first right value in the range.
        let index = match (rule, found) {
            (RangeStartRule::LessThan, Ok(index)) => index + 1,
            (_, Ok(index) | Err(index)) => index,
        };
        if index == right.values.len() {
            break;
        }
        let position = right.offsets[index];
        let value = &right.values[index];
        match rule {
            RangeStartRule::LessThan => {
                walk.set(position);
                walk.set_while(value, false, position);
            }
            RangeStartRule::LessThanOrEqual => {
                walk.set(position);
                walk.set_while(value, true, position);
            }
            RangeStartRule::LessThanOrEqualAllowPreceding => {
                if found.is_err() && index != 0 {
                    // No right value equals the bound: the row before is in the range.
                    walk.set(position - 1);
                    walk.set_while(value, false, position - 1);
                } else {
                    walk.set(position);
                }
                walk.set_while(value, true, position);
            }
        }
        low = index + 1;
    }
    // The bounds left are beyond the last right value.
    walk.set_rest(match rule {
        RangeStartRule::LessThanOrEqualAllowPreceding => right.rows - 1,
        _ => right.rows,
    });
}

/// For each row of `walk`, the position after the last right row of its range.
///
/// Behavior: `processRangeEndsGreaterThan`, `processRangeEndsGreaterThanEqual` and
/// `processRangeEndsGreaterThanEqualAllowFollowing` of
/// io.deephaven.engine.table.impl.rangejoin.RangeSearchKernelDouble and its siblings
fn ends<T: RangeValue>(rule: RangeEndRule, right: &Runs<T>, mut walk: Walk<'_, T>) {
    // A null end is open: the range ends after the last right row.
    walk.set_nulls(right.rows);
    let mut low = 0;
    while low < right.values.len() {
        let Some(bound) = walk.bound() else {
            return;
        };
        let found = find(&right.values, low, bound);
        // The first right value beyond the range.
        let index = match (rule, found) {
            (RangeEndRule::GreaterThan, Ok(index)) => index,
            (_, Ok(index)) => index + 1,
            (_, Err(index)) => index,
        };
        if index == right.values.len() {
            break;
        }
        let position = right.offsets[index];
        let value = &right.values[index];
        match rule {
            RangeEndRule::GreaterThan => {
                walk.set(position);
                walk.set_while(value, true, position);
                low = index + 1;
            }
            RangeEndRule::GreaterThanOrEqual => {
                walk.set(position);
                walk.set_while(value, false, position);
                low = index;
            }
            RangeEndRule::GreaterThanOrEqualAllowFollowing => {
                if found.is_err() {
                    // No right value equals the bound: the row after is in the range.
                    if position + 1 == right.rows {
                        break;
                    }
                    walk.set(position + 1);
                    walk.set_while(value, false, position + 1);
                } else {
                    // The rows that follow get this end without a search of their own,
                    // also those whose bound no right value equals.
                    walk.set(position);
                    walk.set_while(value, false, position);
                }
                low = index;
            }
        }
    }
    walk.set_rest(right.rows);
}

/// The ranges of the left rows of one bucket: for each row, in row order, the positions
/// in the bucket's right rows of the first row of its range and of the row after its
/// last, or `None` when no range can be made of its bounds. `right` is `None` for a
/// bucket without right rows, where every range is empty.
///
/// Behavior: `StaticRangeJoinPhase2#run` of
/// io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation, with
/// `processInvalidRanges` and `processAllRangesForEmptyRight` of the kernels
pub(super) fn bucket_ranges<T: RangeValue>(
    start_rule: RangeStartRule,
    end_rule: RangeEndRule,
    left_starts: &[Option<T>],
    left_ends: &[Option<T>],
    right: Option<&Runs<T>>,
) -> Vec<Option<(usize, usize)>> {
    let mut ranges = Vec::with_capacity(left_starts.len());
    for (chunk_starts, chunk_ends) in left_starts
        .chunks(LEFT_CHUNK)
        .zip(left_ends.chunks(LEFT_CHUNK))
    {
        let valid: Vec<usize> = (0..chunk_starts.len())
            .filter(|&row| {
                is_valid(
                    start_rule,
                    end_rule,
                    chunk_starts[row].as_ref(),
                    chunk_ends[row].as_ref(),
                )
            })
            .collect();
        let mut found_starts = vec![0; chunk_starts.len()];
        let mut found_ends = vec![0; chunk_starts.len()];
        if let Some(right) = right {
            let sorted = |bounds: &[Option<T>]| {
                let mut rows = valid.clone();
                // Stable, nulls first: the order of the engine's ascending sort.
                rows.sort_by(|&a, &b| match (&bounds[a], &bounds[b]) {
                    (None, None) => Ordering::Equal,
                    (None, Some(_)) => Ordering::Less,
                    (Some(_), None) => Ordering::Greater,
                    (Some(a), Some(b)) => a.order(b),
                });
                rows
            };
            let rows = sorted(chunk_starts);
            let bounds: Vec<Option<&T>> = rows.iter().map(|&r| chunk_starts[r].as_ref()).collect();
            let walk = Walk {
                bounds: &bounds,
                rows: &rows,
                next: 0,
                found: &mut found_starts,
            };
            starts(start_rule, right, walk);

            let rows = sorted(chunk_ends);
            let bounds: Vec<Option<&T>> = rows.iter().map(|&r| chunk_ends[r].as_ref()).collect();
            let walk = Walk {
                bounds: &bounds,
                rows: &rows,
                next: 0,
                found: &mut found_ends,
            };
            ends(end_rule, right, walk);
        }
        let mut chunk: Vec<Option<(usize, usize)>> = vec![None; chunk_starts.len()];
        for row in valid {
            chunk[row] = Some((found_starts[row], found_ends[row]));
        }
        ranges.extend(chunk);
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use RangeEndRule::{GreaterThan, GreaterThanOrEqual, GreaterThanOrEqualAllowFollowing};
    use RangeStartRule::{LessThan, LessThanOrEqual, LessThanOrEqualAllowPreceding};

    fn ranges<T: RangeValue + Clone>(
        start_rule: RangeStartRule,
        end_rule: RangeEndRule,
        left: &[(Option<T>, Option<T>)],
        right: &[T],
    ) -> Vec<Option<(usize, usize)>> {
        let starts: Vec<Option<T>> = left.iter().map(|(start, _)| start.clone()).collect();
        let ends: Vec<Option<T>> = left.iter().map(|(_, end)| end.clone()).collect();
        let runs = Runs::of(right).expect("ascending right values");
        let runs = (!right.is_empty()).then_some(&runs);
        bucket_ranges(start_rule, end_rule, &starts, &ends, runs)
    }

    fn one<T: RangeValue + Clone>(
        start_rule: RangeStartRule,
        end_rule: RangeEndRule,
        start: Option<T>,
        end: Option<T>,
        right: &[T],
    ) -> Option<(usize, usize)> {
        ranges(start_rule, end_rule, &[(start, end)], right)[0]
    }

    const RIGHT: [i64; 5] = [10, 20, 20, 30, 40];

    #[test]
    fn runs_keep_the_first_of_equal_values_and_report_the_first_descent() {
        let runs = Runs::of(&RIGHT).unwrap();
        assert_eq!(runs.values, vec![10, 20, 30, 40]);
        assert_eq!(runs.offsets, vec![0, 1, 3, 4]);
        assert_eq!(runs.rows, 5);
        assert_eq!(Runs::of(&[1i64, 3, 2, 1]).err(), Some(2));
        assert_eq!(Runs::of(&[2i64, 2, 1]).err(), Some(2));
        // The zeros are one value, in either order, and the first is kept.
        let runs = Runs::of(&[-1.0, 0.0, -0.0, 1.0]).unwrap();
        assert_eq!(runs.offsets, vec![0, 1, 3]);
        assert!(runs.values[1].is_sign_positive());
        assert_eq!(Runs::of(&[0.0, -0.0, -1.0]).err(), Some(2));
        let empty = Runs::<i64>::of(&[]).unwrap();
        assert_eq!((empty.values.len(), empty.rows), (0, 0));
    }

    #[test]
    fn each_start_rule_finds_its_first_row() {
        // (start, strict, inclusive, inclusive allowing the row before)
        let cases = [
            (Some(10), 1, 0, 0),
            (Some(20), 3, 1, 1),
            (Some(15), 1, 1, 0),
            (Some(25), 3, 3, 2),
            (Some(5), 0, 0, 0),
            (Some(40), 5, 4, 4),
            (Some(45), 5, 5, 4),
            (None, 0, 0, 0),
        ];
        for (start, strict, inclusive, preceding) in cases {
            let first = |rule| {
                one(rule, GreaterThanOrEqual, start, None, &RIGHT)
                    .unwrap()
                    .0
            };
            assert_eq!(first(LessThan), strict, "{start:?} <");
            assert_eq!(first(LessThanOrEqual), inclusive, "{start:?} <=");
            assert_eq!(
                first(LessThanOrEqualAllowPreceding),
                preceding,
                "<- {start:?} <="
            );
        }
    }

    #[test]
    fn each_end_rule_finds_the_row_after_its_last() {
        // (end, strict, inclusive, inclusive allowing the row after)
        let cases = [
            (Some(10), 0, 1, 1),
            (Some(20), 1, 3, 3),
            (Some(15), 1, 1, 2),
            (Some(25), 3, 3, 4),
            (Some(5), 0, 0, 1),
            (Some(35), 4, 4, 5),
            (Some(40), 4, 5, 5),
            (Some(45), 5, 5, 5),
            (None, 5, 5, 5),
        ];
        for (end, strict, inclusive, following) in cases {
            let after = |rule| one(LessThanOrEqual, rule, None, end, &RIGHT).unwrap().1;
            assert_eq!(after(GreaterThan), strict, "< {end:?}");
            assert_eq!(after(GreaterThanOrEqual), inclusive, "<= {end:?}");
            assert_eq!(
                after(GreaterThanOrEqualAllowFollowing),
                following,
                "<= {end:?} ->"
            );
        }
    }

    #[test]
    fn rows_walked_together_share_what_was_found() {
        let both = |left: &[(Option<i64>, Option<i64>)]| {
            ranges(
                LessThanOrEqualAllowPreceding,
                GreaterThanOrEqualAllowFollowing,
                left,
                &RIGHT,
            )
        };
        // Several rows per right value and per gap, out of order.
        let left = [
            (Some(31), Some(39)),
            (Some(12), Some(18)),
            (Some(10), Some(12)),
            (Some(13), Some(13)),
            (Some(41), Some(45)),
            (Some(1), Some(5)),
            (Some(20), Some(20)),
            (Some(21), Some(22)),
        ];
        assert_eq!(
            both(&left),
            vec![
                Some((3, 5)),
                Some((0, 2)),
                Some((0, 2)),
                Some((0, 2)),
                Some((4, 5)),
                Some((0, 1)),
                Some((1, 3)),
                // The row before ends at 20, so this one gets no row after its range.
                Some((2, 3)),
            ]
        );
        // Alone, the last row does.
        assert_eq!(both(&left[7..]), vec![Some((2, 4))]);
    }

    #[test]
    fn a_row_that_ends_at_a_right_value_hides_the_row_after_from_those_behind_it() {
        let after = |left: &[(Option<i64>, Option<i64>)]| -> Vec<usize> {
            ranges(
                LessThanOrEqual,
                GreaterThanOrEqualAllowFollowing,
                left,
                &RIGHT,
            )
            .iter()
            .map(|range| range.unwrap().1)
            .collect()
        };
        assert_eq!(after(&[(None, Some(25))]), vec![4]);
        assert_eq!(after(&[(None, Some(25)), (None, Some(20))]), vec![3, 3]);
        assert_eq!(after(&[(None, Some(25)), (None, Some(15))]), vec![4, 2]);
        // Not across reads of 2048 rows.
        let mut left = vec![(None, Some(20))];
        left.resize(LEFT_CHUNK, (None, Some(25)));
        left.push((None, Some(25)));
        let found = after(&left);
        assert_eq!(found[..LEFT_CHUNK], vec![3; LEFT_CHUNK]);
        assert_eq!(found[LEFT_CHUNK], 4);
    }

    #[test]
    fn bounds_that_make_no_range_are_none_and_an_empty_right_gives_empty_ranges() {
        let inclusive =
            |start, end, right: &[i64]| one(LessThanOrEqual, GreaterThanOrEqual, start, end, right);
        assert_eq!(inclusive(Some(30), Some(10), &RIGHT), None);
        assert_eq!(inclusive(Some(20), Some(20), &RIGHT), Some((1, 3)));
        assert_eq!(inclusive(Some(25), Some(25), &RIGHT), Some((3, 3)));
        assert_eq!(inclusive(Some(30), Some(10), &[]), None);
        assert_eq!(inclusive(Some(10), Some(30), &[]), Some((0, 0)));
        assert_eq!(inclusive(None, None, &[]), Some((0, 0)));
        // Equal bounds make a range only when both rules allow equality.
        for (start_rule, end_rule, allowed) in [
            (LessThan, GreaterThan, false),
            (LessThanOrEqual, GreaterThan, false),
            (LessThanOrEqualAllowPreceding, GreaterThan, false),
            (LessThan, GreaterThanOrEqual, false),
            (LessThan, GreaterThanOrEqualAllowFollowing, false),
            (LessThanOrEqual, GreaterThanOrEqual, true),
            (LessThanOrEqualAllowPreceding, GreaterThanOrEqual, true),
            (LessThanOrEqual, GreaterThanOrEqualAllowFollowing, true),
            (
                LessThanOrEqualAllowPreceding,
                GreaterThanOrEqualAllowFollowing,
                true,
            ),
        ] {
            for right in [&RIGHT[..], &[]] {
                let found = one(start_rule, end_rule, Some(20), Some(20), right);
                assert_eq!(found.is_some(), allowed, "{start_rule:?} {end_rule:?}");
            }
        }
    }

    #[test]
    fn the_pairs_of_rules_the_match_text_cannot_name_search_like_the_others() {
        // `<-` with a strict end, and a strict start with `->`.
        let found = one(
            LessThanOrEqualAllowPreceding,
            GreaterThan,
            Some(15),
            Some(30),
            &RIGHT,
        );
        assert_eq!(found, Some((0, 3)));
        let found = one(
            LessThan,
            GreaterThanOrEqualAllowFollowing,
            Some(10),
            Some(25),
            &RIGHT,
        );
        assert_eq!(found, Some((1, 4)));
    }

    #[test]
    fn nan_bounds_make_no_range_and_infinities_are_values() {
        let right = [f64::NEG_INFINITY, 1.5, f64::INFINITY];
        let inclusive = |start, end| one(LessThanOrEqual, GreaterThanOrEqual, start, end, &right);
        assert_eq!(inclusive(Some(f64::NAN), Some(1.0)), None);
        assert_eq!(inclusive(Some(1.0), Some(f64::NAN)), None);
        assert_eq!(inclusive(None, Some(f64::NAN)), None);
        assert_eq!(inclusive(Some(f64::NAN), None), None);
        assert_eq!(
            inclusive(Some(f64::NEG_INFINITY), Some(f64::INFINITY)),
            Some((0, 3))
        );
        assert_eq!(
            inclusive(Some(f64::INFINITY), Some(f64::INFINITY)),
            Some((2, 3))
        );
        assert_eq!(inclusive(Some(1.0), Some(2.0)), Some((1, 2)));
    }

    #[test]
    fn the_other_zero_is_found_only_where_the_walk_stands() {
        let inclusive =
            |start, end, right: &[f64]| one(LessThanOrEqual, GreaterThanOrEqual, start, end, right);
        // The first right value is compared as numbers.
        assert_eq!(inclusive(Some(0.0), None, &[-0.0, 1.0]), Some((0, 2)));
        assert_eq!(inclusive(Some(-0.0), None, &[0.0, 1.0]), Some((0, 2)));
        // A later one is searched for, and there the negative zero is the lesser.
        assert_eq!(inclusive(Some(0.0), None, &[-1.0, -0.0, 1.0]), Some((2, 3)));
        assert_eq!(inclusive(Some(-0.0), None, &[-1.0, 0.0, 1.0]), Some((1, 3)));
        assert_eq!(inclusive(None, Some(-0.0), &[-1.0, 0.0, 1.0]), Some((0, 1)));
        assert_eq!(inclusive(None, Some(0.0), &[-1.0, -0.0, 1.0]), Some((0, 2)));
        // Unless the walk stands there after an earlier row.
        let found = ranges(
            LessThanOrEqual,
            GreaterThanOrEqual,
            &[(None, Some(-0.0)), (None, Some(0.0)), (None, Some(-0.0))],
            &[-1.0, 0.0, 1.0],
        );
        assert_eq!(found, vec![Some((0, 1)), Some((0, 2)), Some((0, 2))]);
    }

    #[test]
    fn strings_order_by_utf16_code_unit() {
        // U+FF5E is one unit above the surrogates U+1F600 is written with.
        let tilde = Utf16::from("\u{ff5e}");
        let smile = Utf16::from("\u{1f600}");
        assert_eq!(smile.order(&tilde), Ordering::Less);
        assert_eq!(Utf16::from("a").order(&Utf16::from("ab")), Ordering::Less);
        assert_eq!(Utf16::from("").order(&Utf16::from("")), Ordering::Equal);
        assert!(Runs::of(&[smile.clone(), tilde.clone()]).is_ok());
        assert_eq!(Runs::of(&[tilde.clone(), smile.clone()]).err(), Some(1));
        let found = one(
            LessThanOrEqual,
            GreaterThanOrEqual,
            Some(smile.clone()),
            Some(tilde.clone()),
            &[smile.clone(), tilde.clone()],
        );
        assert_eq!(found, Some((0, 2)));
        let found = one(
            LessThanOrEqual,
            GreaterThanOrEqual,
            Some(tilde),
            Some(smile.clone()),
            &[smile],
        );
        assert_eq!(found, None);
    }
}
