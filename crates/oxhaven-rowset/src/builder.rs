//! Builders that assemble a row set from keys and ranges.

use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};

/// Builds a row set from keys and ranges appended in strictly increasing order.
///
/// Use this when the input is already ordered: an append that is not above everything appended
/// so far is a bug in the caller and panics, as the Java builder throws by default.
///
/// Behavior: io.deephaven.engine.rowset.RowSetBuilderSequential,
/// io.deephaven.engine.rowset.impl.RspBitmapBuilderSequential#appendRange
#[derive(Clone, Debug, Default)]
pub struct RowSetBuilderSequential {
    /// Sorted, non-overlapping and non-adjacent.
    ranges: Vec<RowRange>,
}

impl RowSetBuilderSequential {
    pub fn new() -> Self {
        Self::default()
    }

    /// # Panics
    /// If `key` is negative or not greater than every key appended so far.
    pub fn append_key(&mut self, key: RowKey) {
        self.append_range(key, key);
    }

    /// Appends the keys `first..=last`.
    ///
    /// # Panics
    /// If the range is invalid (see [`RowRange::new`]) or `first` is not greater than every key
    /// appended so far.
    pub fn append_range(&mut self, first: RowKey, last: RowKey) {
        let range = RowRange::new(first, last);
        match self.ranges.last_mut() {
            Some(prev) => {
                assert!(
                    first > prev.last(),
                    "out of order append: last={} while appending {first}..={last}",
                    prev.last()
                );
                if prev.last() + 1 == first {
                    *prev = RowRange::new(prev.first(), last);
                } else {
                    self.ranges.push(range);
                }
            }
            None => self.ranges.push(range),
        }
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderSequential#appendKeys
    pub fn append_keys(&mut self, keys: impl IntoIterator<Item = RowKey>) {
        for key in keys {
            self.append_key(key);
        }
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderSequential#appendRanges
    pub fn append_ranges(&mut self, ranges: impl IntoIterator<Item = RowRange>) {
        for r in ranges {
            self.append_range(r.first(), r.last());
        }
    }

    /// Appends every key of `rows`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderSequential#appendRowSequence
    pub fn append_row_set(&mut self, rows: &RowSet) {
        self.append_row_set_with_offset(rows, 0);
    }

    /// Appends every key of `rows`, moved by `offset`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderSequential#appendRowSequenceWithOffset
    pub fn append_row_set_with_offset(&mut self, rows: &RowSet, offset: i64) {
        for r in rows.ranges() {
            self.append_range(r.first() + offset, r.last() + offset);
        }
    }

    pub fn build(self) -> RowSet {
        RowSet::from_ranges(self.ranges)
    }
}

/// Builds a row set from keys and ranges added in any order, with repeats and overlaps allowed.
///
/// Behavior: io.deephaven.engine.rowset.RowSetBuilderRandom
#[derive(Clone, Debug, Default)]
pub struct RowSetBuilderRandom {
    ranges: Vec<RowRange>,
    /// The row sets added whole that have too many ranges to be worth copying out.
    rows: RowSet,
}

/// A row set with more ranges than this is added to a random builder by union, which shares
/// its blocks, and not range by range.
const MAX_COPIED_RANGES: usize = 64;

impl RowSetBuilderRandom {
    pub fn new() -> Self {
        Self::default()
    }

    /// # Panics
    /// If `key` is negative.
    pub fn add_key(&mut self, key: RowKey) {
        self.add_range(key, key);
    }

    /// Adds the keys `first..=last`.
    ///
    /// # Panics
    /// If the range is invalid (see [`RowRange::new`]).
    pub fn add_range(&mut self, first: RowKey, last: RowKey) {
        let range = RowRange::new(first, last);
        // Extending the previous range keeps ordered input, the common case, compact.
        match self.ranges.last_mut() {
            Some(prev) if prev.last() + 1 == first => *prev = RowRange::new(prev.first(), last),
            _ => self.ranges.push(range),
        }
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderRandom#addKeys
    pub fn add_keys(&mut self, keys: impl IntoIterator<Item = RowKey>) {
        for key in keys {
            self.add_key(key);
        }
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderRandom#addRanges
    pub fn add_ranges(&mut self, ranges: impl IntoIterator<Item = RowRange>) {
        for r in ranges {
            self.add_range(r.first(), r.last());
        }
    }

    /// Behavior: io.deephaven.engine.rowset.RowSetBuilderRandom#addRowSet
    pub fn add_row_set(&mut self, rows: &RowSet) {
        if rows.range_count() <= MAX_COPIED_RANGES {
            self.ranges.extend(rows.ranges());
        } else {
            self.rows.insert(rows);
        }
    }

    pub fn build(self) -> RowSet {
        let mut rows = self.rows;
        if !self.ranges.is_empty() {
            rows.insert(&RowSet::from_ranges(self.ranges));
        }
        rows
    }
}

/// The union of any number of row sets.
///
/// Behavior: io.deephaven.engine.rowset.RowSetFactory#unionInsert
pub fn union_all<'a>(row_sets: impl IntoIterator<Item = &'a RowSet>) -> RowSet {
    let mut builder = RowSetBuilderRandom::new();
    for rows in row_sets {
        builder.add_row_set(rows);
    }
    builder.build()
}
