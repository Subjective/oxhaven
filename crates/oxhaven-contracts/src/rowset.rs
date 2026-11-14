//! Row keys and row sets.
//!
//! A row set is an ordered set of 64-bit row keys. It is the address space of a table: columns are
//! read by row key, and updates describe changes as row sets.
//!
//! Port of the observable behavior of `io.deephaven.engine.rowset.RowSet` and
//! `io.deephaven.engine.rowset.WritableRowSet`. The representation here (sorted inclusive ranges)
//! is deliberately simple; a faster one may replace it without changing this API.

use std::fmt;

/// A row key. Valid keys are non-negative, as in Java.
pub type RowKey = i64;

/// Port of `io.deephaven.engine.rowset.RowSequence#NULL_ROW_KEY`.
pub const NULL_ROW_KEY: RowKey = -1;

/// An inclusive range of row keys, `first <= last`, both non-negative.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct RowRange {
    first: RowKey,
    last: RowKey,
}

impl RowRange {
    /// Creates the range `first..=last`.
    ///
    /// # Panics
    /// If `first` is negative or greater than `last`. Ranges are built by engine code, so a bad
    /// range is a broken invariant, not bad input.
    pub fn new(first: RowKey, last: RowKey) -> Self {
        assert!(
            0 <= first && first <= last,
            "invalid row range {first}..={last}"
        );
        Self { first, last }
    }

    pub fn first(&self) -> RowKey {
        self.first
    }

    pub fn last(&self) -> RowKey {
        self.last
    }

    /// Number of keys in the range.
    pub fn size(&self) -> u64 {
        (self.last - self.first) as u64 + 1
    }

    fn intersect(&self, other: &RowRange) -> Option<RowRange> {
        let first = self.first.max(other.first);
        let last = self.last.min(other.last);
        (first <= last).then_some(RowRange { first, last })
    }
}

/// Errors from row-set operations whose preconditions depend on the data.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RowSetError {
    /// `invert` was given keys that are not all in the row set.
    NotASubset,
    /// A shift would move a key below zero.
    NegativeKey,
}

impl fmt::Display for RowSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RowSetError::NotASubset => write!(f, "keys are not a subset of the row set"),
            RowSetError::NegativeKey => write!(f, "shift would produce a negative row key"),
        }
    }
}

impl std::error::Error for RowSetError {}

/// An ordered set of row keys.
///
/// Invariant: `ranges` is sorted, and consecutive ranges are neither overlapping nor adjacent.
/// Two row sets are equal exactly when they hold the same keys.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct RowSet {
    ranges: Vec<RowRange>,
}

impl fmt::Debug for RowSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowSet{{")?;
        for (i, r) in self.ranges.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            if r.first == r.last {
                write!(f, "{}", r.first)?;
            } else {
                write!(f, "{}-{}", r.first, r.last)?;
            }
        }
        write!(f, "}}")
    }
}

impl RowSet {
    /// Port of `RowSetFactory#empty`.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Port of `RowSetFactory#fromRange`.
    pub fn from_range(first: RowKey, last: RowKey) -> Self {
        Self {
            ranges: vec![RowRange::new(first, last)],
        }
    }

    /// Port of `RowSetFactory#flat`: the keys `0..size`.
    pub fn flat(size: u64) -> Self {
        if size == 0 {
            Self::empty()
        } else {
            Self::from_range(0, (size - 1) as RowKey)
        }
    }

    /// Builds a row set from ranges in any order; overlapping and adjacent ranges are merged.
    pub fn from_ranges(ranges: impl IntoIterator<Item = RowRange>) -> Self {
        let mut input: Vec<RowRange> = ranges.into_iter().collect();
        input.sort_unstable_by_key(|r| r.first);
        let mut out: Vec<RowRange> = Vec::with_capacity(input.len());
        for r in input {
            match out.last_mut() {
                Some(prev) if r.first <= prev.last.saturating_add(1) => {
                    prev.last = prev.last.max(r.last);
                }
                _ => out.push(r),
            }
        }
        Self { ranges: out }
    }

    /// Port of `RowSetFactory#fromKeys`. Keys may be in any order and may repeat.
    pub fn from_keys(keys: impl IntoIterator<Item = RowKey>) -> Self {
        Self::from_ranges(keys.into_iter().map(|k| RowRange::new(k, k)))
    }

    /// The ranges of this set, sorted, non-overlapping and non-adjacent.
    pub fn ranges(&self) -> &[RowRange] {
        &self.ranges
    }

    /// All keys in increasing order.
    pub fn iter(&self) -> impl Iterator<Item = RowKey> + '_ {
        self.ranges.iter().flat_map(|r| r.first..=r.last)
    }

    /// Port of `RowSequence#size`.
    pub fn size(&self) -> u64 {
        self.ranges.iter().map(RowRange::size).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Port of `RowSequence#firstRowKey`; `None` where Java returns `NULL_ROW_KEY`.
    pub fn first_row_key(&self) -> Option<RowKey> {
        self.ranges.first().map(|r| r.first)
    }

    /// Port of `RowSequence#lastRowKey`; `None` where Java returns `NULL_ROW_KEY`.
    pub fn last_row_key(&self) -> Option<RowKey> {
        self.ranges.last().map(|r| r.last)
    }

    /// Port of `RowSet#containsRange` for a single key.
    pub fn contains(&self, key: RowKey) -> bool {
        let i = self.ranges.partition_point(|r| r.last < key);
        self.ranges.get(i).is_some_and(|r| r.first <= key)
    }

    /// Position of `key` in this set, or `None` if absent.
    ///
    /// Port of `RowSet#find`, which returns a negative insertion point when absent.
    pub fn find(&self, key: RowKey) -> Option<u64> {
        let mut pos = 0u64;
        for r in &self.ranges {
            if key < r.first {
                return None;
            }
            if key <= r.last {
                return Some(pos + (key - r.first) as u64);
            }
            pos += r.size();
        }
        None
    }

    /// Position of `key` if present, or the position at which it would be inserted.
    ///
    /// Port of `RowSet#find`: `Ok(pos)` where Java returns `pos`, `Err(pos)` where Java returns
    /// `-pos - 1`.
    pub fn find_or_insertion(&self, key: RowKey) -> Result<u64, u64> {
        let mut pos = 0u64;
        for r in &self.ranges {
            if key < r.first {
                return Err(pos);
            }
            if key <= r.last {
                return Ok(pos + (key - r.first) as u64);
            }
            pos += r.size();
        }
        Err(pos)
    }

    /// The keys at the given positions. Positions at or beyond `size()` are ignored. With
    /// `reversed`, position 0 is the last key.
    ///
    /// Port of `RowSet#subSetForPositions` and `#subSetForReversePositions`.
    pub fn sub_set_for_positions(&self, positions: &RowSet, reversed: bool) -> RowSet {
        let size = self.size();
        let mut out = Vec::new();
        for p in positions.ranges() {
            let start = p.first() as u64;
            if start >= size {
                break;
            }
            let end = (p.last() as u64 + 1).min(size);
            let part = if reversed {
                self.sub_set_by_position_range(size - end, size - start)
            } else {
                self.sub_set_by_position_range(start, end)
            };
            out.extend_from_slice(part.ranges());
        }
        RowSet::from_ranges(out)
    }

    /// The key at position `pos`, or `None` if `pos >= size()`.
    ///
    /// Port of `RowSet#get`, which returns `NULL_ROW_KEY` when out of range.
    pub fn get(&self, pos: u64) -> Option<RowKey> {
        let mut remaining = pos;
        for r in &self.ranges {
            if remaining < r.size() {
                return Some(r.first + remaining as RowKey);
            }
            remaining -= r.size();
        }
        None
    }

    /// Port of `RowSet#union`.
    pub fn union(&self, other: &RowSet) -> RowSet {
        Self::from_ranges(self.ranges.iter().chain(&other.ranges).copied())
    }

    /// Port of `RowSet#intersect`.
    pub fn intersect(&self, other: &RowSet) -> RowSet {
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::new();
        while i < self.ranges.len() && j < other.ranges.len() {
            let (a, b) = (&self.ranges[i], &other.ranges[j]);
            if let Some(r) = a.intersect(b) {
                out.push(r);
            }
            if a.last < b.last {
                i += 1;
            } else {
                j += 1;
            }
        }
        RowSet { ranges: out }
    }

    /// Port of `RowSet#minus`: the keys in `self` that are not in `other`.
    pub fn minus(&self, other: &RowSet) -> RowSet {
        let mut out = Vec::new();
        let mut j = 0;
        for a in &self.ranges {
            let mut start = a.first;
            let mut exhausted = false;
            while j < other.ranges.len() && other.ranges[j].last < start {
                j += 1;
            }
            let mut k = j;
            while k < other.ranges.len() && other.ranges[k].first <= a.last {
                let b = &other.ranges[k];
                if b.first > start {
                    out.push(RowRange {
                        first: start,
                        last: b.first - 1,
                    });
                }
                if b.last >= a.last {
                    exhausted = true;
                    break;
                }
                start = b.last + 1;
                k += 1;
            }
            if !exhausted {
                out.push(RowRange {
                    first: start,
                    last: a.last,
                });
            }
        }
        RowSet { ranges: out }
    }

    /// Port of `RowSet#overlaps`.
    pub fn overlaps(&self, other: &RowSet) -> bool {
        !self.intersect(other).is_empty()
    }

    /// Port of `RowSet#subsetOf`.
    pub fn is_subset_of(&self, other: &RowSet) -> bool {
        self.minus(other).is_empty()
    }

    /// Positions, within `self`, of each key in `keys`.
    ///
    /// Port of `RowSet#invert`. `keys` must be a subset of `self`.
    pub fn invert(&self, keys: &RowSet) -> Result<RowSet, RowSetError> {
        let mut out = Vec::with_capacity(keys.ranges.len());
        let mut i = 0;
        let mut pos_before = 0u64;
        for k in &keys.ranges {
            let mut first = k.first;
            loop {
                while i < self.ranges.len() && self.ranges[i].last < first {
                    pos_before += self.ranges[i].size();
                    i += 1;
                }
                let Some(r) = self.ranges.get(i) else {
                    return Err(RowSetError::NotASubset);
                };
                if r.first > first {
                    return Err(RowSetError::NotASubset);
                }
                let last = k.last.min(r.last);
                let start = pos_before + (first - r.first) as u64;
                let end = pos_before + (last - r.first) as u64;
                out.push(RowRange::new(start as RowKey, end as RowKey));
                if last == k.last {
                    break;
                }
                first = last + 1;
            }
        }
        Ok(RowSet::from_ranges(out))
    }

    /// The keys at positions `start..end` (end exclusive, clamped to `size()`).
    ///
    /// Port of `RowSet#subSetByPositionRange`.
    pub fn sub_set_by_position_range(&self, start: u64, end: u64) -> RowSet {
        if start >= end {
            return RowSet::empty();
        }
        let mut out = Vec::new();
        let mut pos = 0u64;
        for r in &self.ranges {
            let r_end = pos + r.size();
            if r_end > start && pos < end {
                let lo = start.max(pos) - pos;
                let hi = end.min(r_end) - pos - 1;
                out.push(RowRange {
                    first: r.first + lo as RowKey,
                    last: r.first + hi as RowKey,
                });
            }
            pos = r_end;
            if pos >= end {
                break;
            }
        }
        RowSet { ranges: out }
    }

    /// The keys within `first..=last`.
    ///
    /// Port of `RowSet#subSetByKeyRange`.
    pub fn sub_set_by_key_range(&self, first: RowKey, last: RowKey) -> RowSet {
        if first > last {
            return RowSet::empty();
        }
        self.intersect(&RowSet::from_range(first.max(0), last))
    }

    /// Every key moved by `delta`.
    ///
    /// Port of `RowSet#shift`.
    pub fn shift(&self, delta: i64) -> Result<RowSet, RowSetError> {
        if let Some(first) = self.first_row_key() {
            if first + delta < 0 {
                return Err(RowSetError::NegativeKey);
            }
        }
        Ok(RowSet {
            ranges: self
                .ranges
                .iter()
                .map(|r| RowRange {
                    first: r.first + delta,
                    last: r.last + delta,
                })
                .collect(),
        })
    }

    /// Port of `WritableRowSet#insert(RowSet)`.
    pub fn insert(&mut self, other: &RowSet) {
        *self = self.union(other);
    }

    /// Port of `WritableRowSet#remove(RowSet)`.
    pub fn remove(&mut self, other: &RowSet) {
        *self = self.minus(other);
    }

    /// Port of `WritableRowSet#retain`.
    pub fn retain(&mut self, other: &RowSet) {
        *self = self.intersect(other);
    }
}

impl FromIterator<RowKey> for RowSet {
    fn from_iter<T: IntoIterator<Item = RowKey>>(iter: T) -> Self {
        RowSet::from_keys(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Small deterministic generator, so failures reproduce without a dependency.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }

        fn key_set(&mut self) -> BTreeSet<RowKey> {
            let mut s = BTreeSet::new();
            for _ in 0..self.next(6) {
                let first = self.next(60) as RowKey;
                let len = self.next(8) as RowKey;
                s.extend(first..=first + len);
            }
            s
        }
    }

    fn rs(model: &BTreeSet<RowKey>) -> RowSet {
        RowSet::from_keys(model.iter().copied())
    }

    fn keys(set: &RowSet) -> BTreeSet<RowKey> {
        set.iter().collect()
    }

    fn assert_canonical(set: &RowSet) {
        for w in set.ranges().windows(2) {
            assert!(w[0].last() + 1 < w[1].first(), "not canonical: {set:?}");
        }
    }

    #[test]
    fn matches_sorted_set_model() {
        let mut g = Lcg(42);
        for _ in 0..2000 {
            let (a, b) = (g.key_set(), g.key_set());
            let (ra, rb) = (rs(&a), rs(&b));
            assert_canonical(&ra);
            assert_eq!(keys(&ra), a);
            assert_eq!(ra.size(), a.len() as u64);
            assert_eq!(ra.first_row_key(), a.first().copied());
            assert_eq!(ra.last_row_key(), a.last().copied());

            let union = ra.union(&rb);
            assert_canonical(&union);
            assert_eq!(keys(&union), &a | &b, "{ra:?} union {rb:?}");
            let inter = ra.intersect(&rb);
            assert_canonical(&inter);
            assert_eq!(keys(&inter), &a & &b, "{ra:?} intersect {rb:?}");
            let minus = ra.minus(&rb);
            assert_canonical(&minus);
            assert_eq!(keys(&minus), &a - &b, "{ra:?} minus {rb:?}");
            assert_eq!(ra.overlaps(&rb), !a.is_disjoint(&b));
            assert_eq!(ra.is_subset_of(&rb), a.is_subset(&b));

            let sorted: Vec<RowKey> = a.iter().copied().collect();
            for probe in 0..80 {
                assert_eq!(ra.contains(probe), a.contains(&probe));
                assert_eq!(
                    ra.find(probe),
                    sorted.binary_search(&probe).ok().map(|p| p as u64)
                );
                assert_eq!(ra.get(probe as u64), sorted.get(probe as usize).copied());
                assert_eq!(
                    ra.find_or_insertion(probe),
                    sorted
                        .binary_search(&probe)
                        .map(|p| p as u64)
                        .map_err(|p| p as u64)
                );
            }

            let start = g.next(12);
            let end = start + g.next(12);
            let by_pos: BTreeSet<RowKey> = sorted
                .iter()
                .copied()
                .skip(start as usize)
                .take((end - start) as usize)
                .collect();
            let sub = ra.sub_set_by_position_range(start, end);
            assert_canonical(&sub);
            assert_eq!(sub, rs(&by_pos), "{ra:?} positions {start}..{end}");
            assert!(ra.sub_set_by_position_range(end, start).is_empty() || start == end);

            let positions = g.key_set();
            let picked: BTreeSet<RowKey> = positions
                .iter()
                .filter_map(|p| sorted.get(*p as usize).copied())
                .collect();
            assert_eq!(
                ra.sub_set_for_positions(&rs(&positions), false),
                rs(&picked)
            );
            let picked_rev: BTreeSet<RowKey> = positions
                .iter()
                .filter_map(|p| sorted.iter().rev().nth(*p as usize).copied())
                .collect();
            assert_eq!(
                ra.sub_set_for_positions(&rs(&positions), true),
                rs(&picked_rev)
            );

            let lo = g.next(70) as RowKey;
            let hi = lo + g.next(20) as RowKey - 5;
            let by_key: BTreeSet<RowKey> = if lo <= hi {
                a.range(lo..=hi).copied().collect()
            } else {
                BTreeSet::new()
            };
            assert_eq!(keys(&ra.sub_set_by_key_range(lo, hi)), by_key);

            let sub = &a & &b;
            let expected: BTreeSet<RowKey> = sub
                .iter()
                .map(|k| sorted.binary_search(k).unwrap() as RowKey)
                .collect();
            assert_eq!(keys(&ra.invert(&rs(&sub)).unwrap()), expected);
            if !b.is_subset(&a) {
                assert_eq!(ra.invert(&rb), Err(RowSetError::NotASubset));
            }
        }
    }

    #[test]
    fn empty_or_inverted_position_range_is_the_empty_set() {
        let set = RowSet::from_range(0, 9);
        assert_eq!(set.sub_set_by_position_range(2, 2), RowSet::empty());
        assert_eq!(set.sub_set_by_position_range(5, 3), RowSet::empty());
        assert_eq!(set.sub_set_by_position_range(20, 30), RowSet::empty());
    }

    #[test]
    fn shift_moves_every_key_and_rejects_negative_results() {
        let set = RowSet::from_keys([3, 4, 9]);
        assert_eq!(set.shift(2).unwrap(), RowSet::from_keys([5, 6, 11]));
        assert_eq!(set.shift(-3).unwrap(), RowSet::from_keys([0, 1, 6]));
        assert_eq!(set.shift(-4), Err(RowSetError::NegativeKey));
        assert_eq!(RowSet::empty().shift(-4).unwrap(), RowSet::empty());
    }

    #[test]
    fn adjacent_ranges_merge_and_flat_is_zero_based() {
        let set = RowSet::from_ranges([RowRange::new(5, 6), RowRange::new(0, 4)]);
        assert_eq!(set.ranges(), &[RowRange::new(0, 6)]);
        assert_eq!(RowSet::flat(7), set);
        assert!(RowSet::flat(0).is_empty());
    }

    #[test]
    fn in_place_operations_match_their_pure_forms() {
        let (a, b) = (RowSet::from_range(0, 9), RowSet::from_range(5, 14));
        let mut x = a.clone();
        x.insert(&b);
        assert_eq!(x, a.union(&b));
        let mut y = a.clone();
        y.remove(&b);
        assert_eq!(y, a.minus(&b));
        let mut z = a.clone();
        z.retain(&b);
        assert_eq!(z, a.intersect(&b));
    }
}
