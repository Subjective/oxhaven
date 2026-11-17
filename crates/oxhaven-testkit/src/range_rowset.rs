//! The row set as a sorted vector of ranges: the engine's first row set, kept as the reference
//! that the block row set of the contracts is compared with, operation by operation.
//!
//! Every operation costs the number of ranges, and a change rebuilds the vector. Use it in
//! tests and benchmarks only.

use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, RowSetError};
use std::fmt;

fn intersect(a: &RowRange, b: &RowRange) -> Option<RowRange> {
    let first = a.first().max(b.first());
    let last = a.last().min(b.last());
    (first <= last).then(|| RowRange::new(first, last))
}

/// An ordered set of row keys.
///
/// Invariant: `ranges` is sorted, and consecutive ranges are neither overlapping nor adjacent.
/// Two row sets are equal exactly when they hold the same keys.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct RangeRowSet {
    ranges: Vec<RowRange>,
}

impl fmt::Debug for RangeRowSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowSet{{")?;
        for (i, r) in self.ranges.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            if r.first() == r.last() {
                write!(f, "{}", r.first())?;
            } else {
                write!(f, "{}-{}", r.first(), r.last())?;
            }
        }
        write!(f, "}}")
    }
}

impl RangeRowSet {
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
        input.sort_unstable_by_key(|r| r.first());
        let mut out: Vec<RowRange> = Vec::with_capacity(input.len());
        for r in input {
            match out.last_mut() {
                Some(prev) if r.first() <= prev.last().saturating_add(1) => {
                    *prev = RowRange::new(prev.first(), prev.last().max(r.last()));
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
        self.ranges.iter().flat_map(|r| r.first()..=r.last())
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
        self.ranges.first().map(|r| r.first())
    }

    /// Port of `RowSequence#lastRowKey`; `None` where Java returns `NULL_ROW_KEY`.
    pub fn last_row_key(&self) -> Option<RowKey> {
        self.ranges.last().map(|r| r.last())
    }

    /// Port of `RangeRowSet#containsRange` for a single key.
    pub fn contains(&self, key: RowKey) -> bool {
        let i = self.ranges.partition_point(|r| r.last() < key);
        self.ranges.get(i).is_some_and(|r| r.first() <= key)
    }

    /// Position of `key` in this set, or `None` if absent.
    ///
    /// Port of `RangeRowSet#find`, which returns a negative insertion point when absent.
    pub fn find(&self, key: RowKey) -> Option<u64> {
        let mut pos = 0u64;
        for r in &self.ranges {
            if key < r.first() {
                return None;
            }
            if key <= r.last() {
                return Some(pos + (key - r.first()) as u64);
            }
            pos += r.size();
        }
        None
    }

    /// Position of `key` if present, or the position at which it would be inserted.
    ///
    /// Port of `RangeRowSet#find`: `Ok(pos)` where Java returns `pos`, `Err(pos)` where Java returns
    /// `-pos - 1`.
    pub fn find_or_insertion(&self, key: RowKey) -> Result<u64, u64> {
        let mut pos = 0u64;
        for r in &self.ranges {
            if key < r.first() {
                return Err(pos);
            }
            if key <= r.last() {
                return Ok(pos + (key - r.first()) as u64);
            }
            pos += r.size();
        }
        Err(pos)
    }

    /// The keys at the given positions. Positions at or beyond `size()` are ignored. With
    /// `reversed`, position 0 is the last key.
    ///
    /// Port of `RangeRowSet#subSetForPositions` and `#subSetForReversePositions`.
    pub fn sub_set_for_positions(&self, positions: &RangeRowSet, reversed: bool) -> RangeRowSet {
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
        RangeRowSet::from_ranges(out)
    }

    /// The key at position `pos`, or `None` if `pos >= size()`.
    ///
    /// Port of `RangeRowSet#get`, which returns `NULL_ROW_KEY` when out of range.
    pub fn get(&self, pos: u64) -> Option<RowKey> {
        let mut remaining = pos;
        for r in &self.ranges {
            if remaining < r.size() {
                return Some(r.first() + remaining as RowKey);
            }
            remaining -= r.size();
        }
        None
    }

    /// Port of `RangeRowSet#union`.
    pub fn union(&self, other: &RangeRowSet) -> RangeRowSet {
        Self::from_ranges(self.ranges.iter().chain(&other.ranges).copied())
    }

    /// Port of `RangeRowSet#intersect`.
    pub fn intersect(&self, other: &RangeRowSet) -> RangeRowSet {
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::new();
        while i < self.ranges.len() && j < other.ranges.len() {
            let (a, b) = (&self.ranges[i], &other.ranges[j]);
            if let Some(r) = intersect(a, b) {
                out.push(r);
            }
            if a.last() < b.last() {
                i += 1;
            } else {
                j += 1;
            }
        }
        RangeRowSet { ranges: out }
    }

    /// Port of `RangeRowSet#minus`: the keys in `self` that are not in `other`.
    pub fn minus(&self, other: &RangeRowSet) -> RangeRowSet {
        let mut out = Vec::new();
        let mut j = 0;
        for a in &self.ranges {
            let mut start = a.first();
            let mut exhausted = false;
            while j < other.ranges.len() && other.ranges[j].last() < start {
                j += 1;
            }
            let mut k = j;
            while k < other.ranges.len() && other.ranges[k].first() <= a.last() {
                let b = &other.ranges[k];
                if b.first() > start {
                    out.push(RowRange::new(start, b.first() - 1));
                }
                if b.last() >= a.last() {
                    exhausted = true;
                    break;
                }
                start = b.last() + 1;
                k += 1;
            }
            if !exhausted {
                out.push(RowRange::new(start, a.last()));
            }
        }
        RangeRowSet { ranges: out }
    }

    /// Port of `RangeRowSet#overlaps`.
    pub fn overlaps(&self, other: &RangeRowSet) -> bool {
        !self.intersect(other).is_empty()
    }

    /// Port of `RangeRowSet#subsetOf`.
    pub fn is_subset_of(&self, other: &RangeRowSet) -> bool {
        self.minus(other).is_empty()
    }

    /// Positions, within `self`, of each key in `keys`.
    ///
    /// Port of `RangeRowSet#invert`. `keys` must be a subset of `self`.
    pub fn invert(&self, keys: &RangeRowSet) -> Result<RangeRowSet, RowSetError> {
        let mut out = Vec::with_capacity(keys.ranges.len());
        let mut i = 0;
        let mut pos_before = 0u64;
        for k in &keys.ranges {
            let mut first = k.first();
            loop {
                while i < self.ranges.len() && self.ranges[i].last() < first {
                    pos_before += self.ranges[i].size();
                    i += 1;
                }
                let Some(r) = self.ranges.get(i) else {
                    return Err(RowSetError::NotASubset);
                };
                if r.first() > first {
                    return Err(RowSetError::NotASubset);
                }
                let last = k.last().min(r.last());
                let start = pos_before + (first - r.first()) as u64;
                let end = pos_before + (last - r.first()) as u64;
                out.push(RowRange::new(start as RowKey, end as RowKey));
                if last == k.last() {
                    break;
                }
                first = last + 1;
            }
        }
        Ok(RangeRowSet::from_ranges(out))
    }

    /// The keys at positions `start..end` (end exclusive, clamped to `size()`).
    ///
    /// Port of `RangeRowSet#subSetByPositionRange`.
    pub fn sub_set_by_position_range(&self, start: u64, end: u64) -> RangeRowSet {
        if start >= end {
            return RangeRowSet::empty();
        }
        let mut out = Vec::new();
        let mut pos = 0u64;
        for r in &self.ranges {
            let r_end = pos + r.size();
            if r_end > start && pos < end {
                let lo = start.max(pos) - pos;
                let hi = end.min(r_end) - pos - 1;
                out.push(RowRange::new(
                    r.first() + lo as RowKey,
                    r.first() + hi as RowKey,
                ));
            }
            pos = r_end;
            if pos >= end {
                break;
            }
        }
        RangeRowSet { ranges: out }
    }

    /// The keys within `first..=last`. Empty when the range is empty or wholly negative.
    ///
    /// Port of `RangeRowSet#subSetByKeyRange`.
    pub fn sub_set_by_key_range(&self, first: RowKey, last: RowKey) -> RangeRowSet {
        let first = first.max(0);
        if first > last {
            return RangeRowSet::empty();
        }
        self.intersect(&RangeRowSet::from_range(first, last))
    }

    /// Every key moved by `delta`.
    ///
    /// Port of `RangeRowSet#shift`.
    pub fn shift(&self, delta: i64) -> Result<RangeRowSet, RowSetError> {
        if let Some(first) = self.first_row_key() {
            if first + delta < 0 {
                return Err(RowSetError::NegativeKey);
            }
        }
        Ok(RangeRowSet {
            ranges: self
                .ranges
                .iter()
                .map(|r| RowRange::new(r.first() + delta, r.last() + delta))
                .collect(),
        })
    }

    /// Port of `WritableRowSet#insert(RangeRowSet)`.
    pub fn insert(&mut self, other: &RangeRowSet) {
        *self = self.union(other);
    }

    /// Port of `WritableRowSet#remove(RangeRowSet)`.
    pub fn remove(&mut self, other: &RangeRowSet) {
        *self = self.minus(other);
    }

    /// Port of `WritableRowSet#retain`.
    pub fn retain(&mut self, other: &RangeRowSet) {
        *self = self.intersect(other);
    }
}

impl FromIterator<RowKey> for RangeRowSet {
    fn from_iter<T: IntoIterator<Item = RowKey>>(iter: T) -> Self {
        RangeRowSet::from_keys(iter)
    }
}

impl From<&RowSet> for RangeRowSet {
    fn from(rows: &RowSet) -> Self {
        RangeRowSet {
            ranges: rows.ranges().collect(),
        }
    }
}

impl From<&RangeRowSet> for RowSet {
    fn from(rows: &RangeRowSet) -> Self {
        RowSet::from_ranges(rows.ranges().iter().copied())
    }
}
