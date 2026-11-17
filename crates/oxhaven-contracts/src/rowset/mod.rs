//! Row keys and row sets.
//!
//! A row set is an ordered set of 64-bit row keys. It is the address space of a table: columns are
//! read by row key, and updates describe changes as row sets.
//!
//! Port of the observable behavior of `io.deephaven.engine.rowset.RowSet` and
//! `io.deephaven.engine.rowset.WritableRowSet`. `docs/ROWSET.md` has the measurements behind the
//! representation.
//!
//! # Layout
//!
//! A row set is a sorted vector of spans and, beside it, the number of keys up to the end of
//! each span. A span is one of:
//!
//! - a count of whole blocks, however many: the keys 0 to 2^40 - 1 are one span;
//! - one run of keys within a block, stored inline;
//! - a list of up to 2047 runs within a block (4 bytes each);
//! - a bitmap of the block (8192 bytes), when it has more runs than that.
//!
//! Run lists and bitmaps are immutable and reference counted, so a copy of a row set, and the
//! result of an operation, share the blocks they did not change. Each set of keys has one
//! layout, so equality and hashing are structural.
//!
//! # Cost
//!
//! With `s` spans (at most one per 65536 keys of the key space in use, and usually far fewer
//! than there are ranges):
//!
//! - `size`, `is_empty`: O(1);
//! - `contains`, `find`, `get`: O(log s) plus a scan of one block;
//! - `insert`, `remove` of a set that touches `t` blocks: those blocks, plus a move of the
//!   spans after them;
//! - `union`, `intersect`, `minus`: the spans of both sets, and the blocks they share; 64 keys
//!   per step where both blocks are bitmaps. `intersect` searches past the spans of one set
//!   that lie between two spans of the other, so a small set against a large one costs the
//!   small one;
//! - `clone`: O(s), with no copy of any block.
//!
//! # Relation to the reference
//!
//! Java's `RspBitmap` has the same partition into 2^16-key blocks, spans of whole blocks, a
//! cardinality prefix array and shared containers. It differs in that its containers are the
//! RoaringBitmap ones (array, run, bitmap, and several small special forms), a row set starts
//! as a `SingleRange` or `SortedRanges` and becomes an `RspBitmap` when it grows, and sharing
//! is done with explicit reference counts. Here two block forms cover the same ground: a sorted
//! `u16` array is a run list whose runs have one key, at twice the bytes; and a small row set
//! is a few inline spans, so there is no second and third representation to convert between.
//!
//! Behavior: io.deephaven.engine.rowset.impl.rsp.RspArray,
//! io.deephaven.engine.rowset.impl.rsp.RspBitmap

mod container;
mod merge;
#[cfg(test)]
mod tests;

use container::{
    count_runs, seal_words, BitRuns, BlockRuns, Body, Op, Run, View, BLOCK_KEYS, MAX_RUNS, WORDS,
};
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

const BLOCK_BITS: u32 = BLOCK_KEYS.trailing_zeros();

fn block_of(key: RowKey) -> u64 {
    (key as u64) >> BLOCK_BITS
}

fn low(key: RowKey) -> u16 {
    key as u16
}

fn key_at(block: u64, low: u16) -> RowKey {
    ((block << BLOCK_BITS) | u64::from(low)) as RowKey
}

/// How many items at the start of `items` satisfy `holds`, which is true of a prefix and false
/// of the rest. A binary search that costs little when the answer is small, as it is when two
/// row sets are walked together.
fn leading<T>(items: &[T], holds: impl Fn(&T) -> bool) -> usize {
    let (mut known, mut step) = (0, 1);
    while known + step <= items.len() && holds(&items[known + step - 1]) {
        known += step;
        step *= 2;
    }
    // The item at `known + step - 1` does not hold, or is past the end.
    let end = (known + step - 1).min(items.len());
    known + items[known..end].partition_point(holds)
}

/// One or more consecutive blocks and their keys.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct Span {
    /// The first block: its keys are `block << 16 ..= (block << 16) + 65535`.
    block: u64,
    body: Body,
}

impl Span {
    fn blocks(&self) -> u64 {
        match self.body {
            Body::Full(blocks) => blocks,
            _ => 1,
        }
    }

    /// The block after the last one of this span.
    fn end(&self) -> u64 {
        self.block + self.blocks()
    }

    fn first_key(&self) -> RowKey {
        key_at(self.block, self.body.view().first())
    }

    fn last_key(&self) -> RowKey {
        key_at(self.end() - 1, self.body.view().last())
    }
}

/// How many spans of each form a row set has; see [`RowSet::check_layout`].
#[doc(hidden)]
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct SpanCounts {
    pub full: u64,
    pub one: u64,
    pub runs: u64,
    pub bits: u64,
}

/// An ordered set of row keys. See the [module](self) for the layout.
///
/// Two row sets are equal exactly when they hold the same keys.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct RowSet {
    /// Sorted by block, not overlapping, each in the one form its keys allow (see `Body`).
    spans: Vec<Span>,
    /// `ends[i]` is the number of keys in `spans[..=i]`.
    ///
    /// Behavior: io.deephaven.engine.rowset.impl.rsp.RspArray#ensureCardinalityCache
    ends: Vec<u64>,
}

impl fmt::Debug for RowSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RowSet{{")?;
        for (i, r) in self.ranges().enumerate() {
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

impl RowSet {
    /// Behavior: io.deephaven.engine.rowset.RowSetFactory#empty
    pub fn empty() -> Self {
        Self::default()
    }

    /// The keys `first..=last`.
    ///
    /// # Panics
    /// If the range is invalid (see [`RowRange::new`]).
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetFactory#fromRange
    pub fn from_range(first: RowKey, last: RowKey) -> Self {
        let range = RowRange::new(first, last);
        let mut appender = Appender::default();
        appender.append(range.first(), range.last());
        appender.finish()
    }

    /// The keys `0..size`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetFactory#flat
    pub fn flat(size: u64) -> Self {
        if size == 0 {
            Self::empty()
        } else {
            Self::from_range(0, (size - 1) as RowKey)
        }
    }

    /// Builds a row set from ranges in any order; they may overlap and touch.
    pub fn from_ranges(ranges: impl IntoIterator<Item = RowRange>) -> Self {
        let mut input: Vec<RowRange> = ranges.into_iter().collect();
        if !input.is_sorted_by_key(RowRange::first) {
            input.sort_unstable_by_key(RowRange::first);
        }
        let mut appender = Appender::default();
        let mut open: Option<(RowKey, RowKey)> = None;
        for r in input {
            open = match open {
                Some((first, last)) if r.first() <= last.saturating_add(1) => {
                    Some((first, last.max(r.last())))
                }
                Some((first, last)) => {
                    appender.append(first, last);
                    Some((r.first(), r.last()))
                }
                None => Some((r.first(), r.last())),
            };
        }
        if let Some((first, last)) = open {
            appender.append(first, last);
        }
        appender.finish()
    }

    /// Keys may be in any order and may repeat.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSetFactory#fromKeys
    pub fn from_keys(keys: impl IntoIterator<Item = RowKey>) -> Self {
        Self::from_ranges(keys.into_iter().map(|k| RowRange::new(k, k)))
    }

    /// The keys `first + i` for every `i` below `length` whose bit is set in `bits`: bit `i`
    /// is bit `i % 64`, counted from the lowest, of `bits[i / 64]`. Bits at and after
    /// `length` are not read as keys, whatever they hold.
    ///
    /// The result is the row set [`from_ranges`](Self::from_ranges) builds from the runs of
    /// set bits, in the same layout. It is built a block at a time from the words, with no
    /// walk of the runs of a block that is stored as a bitmap: for the kept rows of a filter
    /// over contiguous rows, whose mask is such a bitmap already.
    ///
    /// No Java counterpart: Java's filters hand the kept keys of a chunk to a
    /// `RowSetBuilderSequential`.
    ///
    /// # Panics
    /// If `bits` has fewer than `length` bits, or, with a `length` above zero, if `first` is
    /// negative or `first + length - 1` is not a row key.
    pub fn from_bitmap(first: RowKey, bits: &[u64], length: usize) -> Self {
        if length == 0 {
            return Self::empty();
        }
        assert!(
            length <= bits.len().saturating_mul(64),
            "{length} bits asked of {} words",
            bits.len()
        );
        let last = RowKey::try_from(length - 1)
            .ok()
            .and_then(|more| first.checked_add(more));
        let Some(last) = last.filter(|_| first >= 0) else {
            panic!("invalid bitmap of {length} keys from {first}");
        };
        // Bit `i` belongs at place `lead + i`, counted from the first key of the block of
        // `first`. A word of places is cut from two neighbouring words of `bits`.
        let lead = usize::from(low(first));
        let (skip, shift) = (lead / 64, (lead % 64) as u32);
        let full_words = length / 64;
        // Word `index` of `bits`, with nothing set from bit `length` on or past the end.
        let word = |index: usize| -> u64 {
            match index.cmp(&full_words) {
                std::cmp::Ordering::Less => bits[index],
                std::cmp::Ordering::Equal if !length.is_multiple_of(64) => {
                    bits[index] & (u64::MAX >> (64 - length % 64))
                }
                _ => 0,
            }
        };
        let mut out = Self::empty();
        let mut words = [0u64; WORDS];
        for (nth, block) in (block_of(first)..=block_of(last)).enumerate() {
            for (w, place) in words.iter_mut().enumerate() {
                // Bit `j` of the word of places from `64 * at` on is bit `j - shift` of
                // word `at - skip` of `bits` and, for `j` below `shift`, one of the top
                // `shift` bits of the word before that.
                let at = nth * WORDS + w;
                let high = if at >= skip { word(at - skip) } else { 0 };
                *place = if shift == 0 {
                    high
                } else {
                    let low = if at > skip { word(at - skip - 1) } else { 0 };
                    (high << shift) | (low >> (64 - shift))
                };
            }
            if let Some((body, keys)) = seal_words(&words) {
                out.push(block, body, keys);
            }
        }
        out
    }

    /// The ranges of this set: sorted, and neither overlapping nor adjacent.
    pub fn ranges(&self) -> Ranges<'_> {
        Ranges {
            spans: self.spans.iter(),
            block: 0,
            list: [].iter(),
            bits: None,
            open: None,
        }
    }

    /// The number of ranges that [`ranges`](Self::ranges) yields, without walking them: it
    /// costs the spans, and a count over the words of each bitmap.
    pub fn range_count(&self) -> usize {
        let mut count = 0;
        let mut open_end = None;
        for span in &self.spans {
            let view = span.body.view();
            count += match span.body {
                Body::Full(_) | Body::One(_) => 1,
                Body::Runs(ref runs) => runs.len(),
                Body::Bits(ref words) => count_runs(words),
            };
            // A run that ends its block goes on in a run that starts the next block.
            if open_end == Some(span.block) && view.first() == 0 {
                count -= 1;
            }
            open_end = (view.last() == u16::MAX).then(|| span.end());
        }
        count
    }

    /// The range of this set when its keys are one range, without a walk.
    pub fn as_single_range(&self) -> Option<RowRange> {
        let (first, last) = (self.first_row_key()?, self.last_row_key()?);
        ((last - first) as u64 == self.size() - 1).then(|| RowRange::new(first, last))
    }

    /// All keys in increasing order.
    pub fn iter(&self) -> impl Iterator<Item = RowKey> + '_ {
        self.ranges().flat_map(|r| r.first()..=r.last())
    }

    /// Behavior: io.deephaven.engine.rowset.RowSequence#size
    pub fn size(&self) -> u64 {
        self.ends.last().copied().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// `None` where Java returns `NULL_ROW_KEY`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSequence#firstRowKey
    pub fn first_row_key(&self) -> Option<RowKey> {
        self.spans.first().map(Span::first_key)
    }

    /// `None` where Java returns `NULL_ROW_KEY`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSequence#lastRowKey
    pub fn last_row_key(&self) -> Option<RowKey> {
        self.spans.last().map(Span::last_key)
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#containsRange, for a single key
    pub fn contains(&self, key: RowKey) -> bool {
        if key < 0 {
            return false;
        }
        let block = block_of(key);
        let i = self.spans.partition_point(|s| s.end() <= block);
        self.spans
            .get(i)
            .is_some_and(|s| s.block <= block && s.body.view().contains(low(key)))
    }

    /// Whether this set has a key in `first..=last`. An inverted range holds no key.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#overlapsRange
    pub fn overlaps_range(&self, first: RowKey, last: RowKey) -> bool {
        let first = first.max(0);
        if first > last {
            return false;
        }
        // The first key at or after `first`, if any, is the one at its insertion position.
        let (Ok(pos) | Err(pos)) = self.find_or_insertion(first);
        self.get(pos).is_some_and(|key| key <= last)
    }

    /// Position of `key` in this set, or `None` if absent.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#find
    pub fn find(&self, key: RowKey) -> Option<u64> {
        self.find_or_insertion(key).ok()
    }

    /// Position of `key` if present, or the position at which it would be inserted: `Ok(pos)`
    /// where Java returns `pos`, `Err(pos)` where Java returns `-pos - 1`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#find
    pub fn find_or_insertion(&self, key: RowKey) -> Result<u64, u64> {
        if key < 0 {
            return Err(0);
        }
        Seeker::new(self).seek(key)
    }

    /// The keys at the given positions. Positions at or beyond `size()` are ignored. With
    /// `reversed`, position 0 is the last key.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#subSetForPositions,
    /// io.deephaven.engine.rowset.RowSet#subSetForReversePositions
    pub fn sub_set_for_positions(&self, positions: &RowSet, reversed: bool) -> RowSet {
        let size = self.size();
        let mut parts = Vec::new();
        for p in positions.ranges() {
            let start = p.first() as u64;
            if start >= size {
                break;
            }
            let end = (p.last() as u64 + 1).min(size);
            parts.push(if reversed {
                self.sub_set_by_position_range(size - end, size - start)
            } else {
                self.sub_set_by_position_range(start, end)
            });
        }
        if reversed {
            parts.reverse();
        }
        // The parts are in key order now, so each insert works at the end of `out`.
        let mut out = RowSet::empty();
        for part in &parts {
            out.insert(part);
        }
        out
    }

    /// The key at position `pos`, or `None` if `pos >= size()`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#get
    pub fn get(&self, pos: u64) -> Option<RowKey> {
        let i = self.ends.partition_point(|&end| end <= pos);
        let span = self.spans.get(i)?;
        let within = pos - self.before(i);
        Some(match span.body {
            Body::Full(_) => ((span.block << BLOCK_BITS) + within) as RowKey,
            _ => key_at(span.block, span.body.view().select(within)),
        })
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#union
    pub fn union(&self, other: &RowSet) -> RowSet {
        merge::merge(Op::Union, self.all(), other.all())
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#intersect
    pub fn intersect(&self, other: &RowSet) -> RowSet {
        merge::merge(Op::Intersect, self.all(), other.all())
    }

    /// The keys in `self` that are not in `other`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#minus
    pub fn minus(&self, other: &RowSet) -> RowSet {
        merge::merge(Op::Minus, self.all(), other.all())
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#overlaps
    pub fn overlaps(&self, other: &RowSet) -> bool {
        !self.intersect(other).is_empty()
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#subsetOf
    pub fn is_subset_of(&self, other: &RowSet) -> bool {
        self.minus(other).is_empty()
    }

    /// Positions, within `self`, of each key in `keys`, which must be a subset of `self`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#invert
    pub fn invert(&self, keys: &RowSet) -> Result<RowSet, RowSetError> {
        let mut seeker = Seeker::new(self);
        let mut out = Appender::default();
        for r in keys.ranges() {
            // Every key of the range is present exactly when both ends are and their
            // positions are as far apart as the keys.
            let (Ok(first), Ok(last)) = (seeker.seek(r.first()), seeker.seek(r.last())) else {
                return Err(RowSetError::NotASubset);
            };
            if last - first != r.size() - 1 {
                return Err(RowSetError::NotASubset);
            }
            out.append(first as RowKey, last as RowKey);
        }
        Ok(out.finish())
    }

    /// The keys at positions `start..end` (end exclusive, clamped to `size()`).
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#subSetByPositionRange
    pub fn sub_set_by_position_range(&self, start: u64, end: u64) -> RowSet {
        let end = end.min(self.size());
        if start >= end {
            return RowSet::empty();
        }
        match (self.get(start), self.get(end - 1)) {
            // Consecutive positions are exactly the keys between the two ends.
            (Some(first), Some(last)) => self.sub_set_by_key_range(first, last),
            _ => panic!("positions {start}..{end} are below the size"),
        }
    }

    /// The keys within `first..=last`. Empty when the range is empty or wholly negative.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#subSetByKeyRange
    pub fn sub_set_by_key_range(&self, first: RowKey, last: RowKey) -> RowSet {
        let first = first.max(0);
        if first > last {
            return RowSet::empty();
        }
        self.intersect(&RowSet::from_range(first, last))
    }

    /// Every key moved by `delta`.
    ///
    /// # Panics
    /// If a key would move beyond the last key of the key space.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#shift,
    /// io.deephaven.engine.rowset.impl.rsp.RspBitmap#applyOffsetImpl
    pub fn shift(&self, delta: i64) -> Result<RowSet, RowSetError> {
        if let Some(first) = self.first_row_key() {
            if first + delta < 0 {
                return Err(RowSetError::NegativeKey);
            }
        }
        if let Some(last) = self.last_row_key() {
            assert!(
                last.checked_add(delta).is_some(),
                "shift of {last} by {delta} is beyond the key space"
            );
        }
        if delta % BLOCK_KEYS as i64 == 0 {
            // Whole blocks move: the result shares every block with `self`.
            let blocks = delta >> BLOCK_BITS;
            let spans = self
                .spans
                .iter()
                .map(|s| Span {
                    block: (s.block as i64 + blocks) as u64,
                    body: s.body.clone(),
                })
                .collect();
            return Ok(RowSet {
                spans,
                ends: self.ends.clone(),
            });
        }
        let mut out = Appender::default();
        for r in self.ranges() {
            out.append(r.first() + delta, r.last() + delta);
        }
        Ok(out.finish())
    }

    /// Behavior: io.deephaven.engine.rowset.WritableRowSet#insert(RowSet)
    pub fn insert(&mut self, other: &RowSet) {
        self.merge_in_place(Op::Union, other);
    }

    /// Behavior: io.deephaven.engine.rowset.WritableRowSet#remove(RowSet)
    pub fn remove(&mut self, other: &RowSet) {
        self.merge_in_place(Op::Minus, other);
    }

    /// Behavior: io.deephaven.engine.rowset.WritableRowSet#retain
    pub fn retain(&mut self, other: &RowSet) {
        *self = self.intersect(other);
    }

    /// Bytes this row set holds on the heap. A block shared with another row set is counted
    /// in full in both.
    pub fn heap_bytes(&self) -> usize {
        self.spans.capacity() * std::mem::size_of::<Span>()
            + self.ends.capacity() * std::mem::size_of::<u64>()
            + self
                .spans
                .iter()
                .map(|s| s.body.heap_bytes())
                .sum::<usize>()
    }

    /// Checks that this row set is in the one layout its keys allow, and returns how many
    /// spans of each form it has. For tests of the row set itself.
    ///
    /// # Panics
    /// If the layout is not the documented one.
    #[doc(hidden)]
    pub fn check_layout(&self) -> SpanCounts {
        let mut seen = SpanCounts::default();
        assert_eq!(self.spans.len(), self.ends.len());
        let mut total = 0;
        for (i, span) in self.spans.iter().enumerate() {
            let keys = match &span.body {
                Body::Full(blocks) => {
                    seen.full += 1;
                    assert!(*blocks >= 1);
                    blocks * BLOCK_KEYS
                }
                Body::One(run) => {
                    seen.one += 1;
                    assert!(run.first <= run.last);
                    assert_ne!(*run, Run::WHOLE, "a whole block stored as a run");
                    run.len()
                }
                Body::Runs(runs) => {
                    seen.runs += 1;
                    assert!((2..=MAX_RUNS).contains(&runs.len()), "{} runs", runs.len());
                    for w in runs.windows(2) {
                        assert!(u32::from(w[0].last) + 1 < u32::from(w[1].first));
                    }
                    runs.iter().map(|r| r.len()).sum()
                }
                Body::Bits(words) => {
                    seen.bits += 1;
                    assert!(count_runs(words) > MAX_RUNS, "a bitmap with few runs");
                    words.iter().map(|w| u64::from(w.count_ones())).sum()
                }
            };
            assert!(span.end() <= 1 << (63 - BLOCK_BITS), "beyond the key space");
            total += keys;
            assert_eq!(self.ends[i], total, "key count of span {i}");
            if let Some(next) = self.spans.get(i + 1) {
                assert!(span.end() <= next.block, "spans {i} and the next overlap");
                let both_full =
                    matches!(span.body, Body::Full(_)) && matches!(next.body, Body::Full(_));
                assert!(!(both_full && span.end() == next.block), "unjoined blocks");
            }
        }
        seen
    }

    /// The number of keys in `spans[..i]`.
    fn before(&self, i: usize) -> u64 {
        if i == 0 {
            0
        } else {
            self.ends[i - 1]
        }
    }

    /// The number of keys in `spans[i]`.
    fn keys_of(&self, i: usize) -> u64 {
        self.ends[i] - self.before(i)
    }

    /// Adds a span after all the others. Whole blocks that follow whole blocks extend them.
    fn push(&mut self, block: u64, body: Body, keys: u64) {
        let total = self.size() + keys;
        if let (Body::Full(more), Some(last)) = (&body, self.spans.last_mut()) {
            if let Body::Full(blocks) = &mut last.body {
                if last.block + *blocks == block {
                    *blocks += more;
                    if let Some(end) = self.ends.last_mut() {
                        *end = total;
                    }
                    return;
                }
            }
        }
        self.spans.push(Span { block, body });
        self.ends.push(total);
    }

    /// Adds `from.spans[at..end]`, which is not empty, after all the spans of `self`.
    fn extend(&mut self, from: &RowSet, at: usize, end: usize) {
        // Only the first can be whole blocks that follow whole blocks.
        let first = &from.spans[at];
        self.push(first.block, first.body.clone(), from.keys_of(at));
        let (size, passed) = (self.size(), from.ends[at]);
        self.spans.extend_from_slice(&from.spans[at + 1..end]);
        self.ends
            .extend(from.ends[at + 1..end].iter().map(|e| e - passed + size));
    }
}

impl FromIterator<RowKey> for RowSet {
    fn from_iter<T: IntoIterator<Item = RowKey>>(iter: T) -> Self {
        RowSet::from_keys(iter)
    }
}

/// Builds a row set from ranges that arrive in increasing order. A range may touch the one
/// before it but not overlap it.
#[derive(Default)]
struct Appender {
    out: RowSet,
    /// The block whose runs are being collected.
    block: u64,
    runs: BlockRuns,
}

impl Appender {
    fn append(&mut self, first: RowKey, last: RowKey) {
        let (first_block, last_block) = (block_of(first), block_of(last));
        if first_block == last_block {
            self.run(first_block, low(first), low(last));
            return;
        }
        self.run(first_block, low(first), u16::MAX);
        self.flush();
        let between = last_block - first_block - 1;
        if between > 0 {
            self.out
                .push(first_block + 1, Body::Full(between), between * BLOCK_KEYS);
        }
        self.run(last_block, 0, low(last));
    }

    fn run(&mut self, block: u64, first: u16, last: u16) {
        if block != self.block {
            self.flush();
            self.block = block;
        }
        self.runs.push(Run { first, last });
    }

    fn flush(&mut self) {
        if let Some((body, keys)) = self.runs.seal() {
            self.out.push(self.block, body, keys);
        }
    }

    fn finish(mut self) -> RowSet {
        self.flush();
        self.out
    }
}

/// Finds positions of keys that are asked for in increasing order, without starting over for
/// each: an inversion of `k` ranges costs the spans and blocks passed, not `k` searches.
struct Seeker<'a> {
    set: &'a RowSet,
    /// The first span that may hold the next key.
    span: usize,
    /// Progress within that span's block: runs (of a run list) or words (of a bitmap) passed,
    /// and the keys in them.
    passed: usize,
    keys_passed: u64,
}

impl<'a> Seeker<'a> {
    fn new(set: &'a RowSet) -> Self {
        Self {
            set,
            span: 0,
            passed: 0,
            keys_passed: 0,
        }
    }

    /// The position of `key`, or `Err` with the position it would be inserted at. `key` is
    /// not negative and not below the key of the previous call.
    fn seek(&mut self, key: RowKey) -> Result<u64, u64> {
        let spans = &self.set.spans;
        let block = block_of(key);
        if spans.get(self.span).is_some_and(|s| s.end() <= block) {
            let from = self.span + 1;
            self.span = from + leading(&spans[from..], |s| s.end() <= block);
            self.passed = 0;
            self.keys_passed = 0;
        }
        let before = self.set.before(self.span);
        let Some(span) = spans.get(self.span).filter(|s| s.block <= block) else {
            return Err(before);
        };
        let low = low(key);
        match &span.body {
            Body::Full(_) => Ok(before + ((block - span.block) << BLOCK_BITS) + u64::from(low)),
            Body::One(run) => {
                if low < run.first {
                    Err(before)
                } else if low <= run.last {
                    Ok(before + u64::from(low - run.first))
                } else {
                    Err(before + run.len())
                }
            }
            Body::Runs(runs) => {
                while let Some(run) = runs.get(self.passed).filter(|r| r.last < low) {
                    self.keys_passed += run.len();
                    self.passed += 1;
                }
                let at = before + self.keys_passed;
                match runs.get(self.passed) {
                    Some(run) if run.first <= low => Ok(at + u64::from(low - run.first)),
                    _ => Err(at),
                }
            }
            Body::Bits(words) => {
                let word = usize::from(low >> 6);
                while self.passed < word {
                    self.keys_passed += u64::from(words[self.passed].count_ones());
                    self.passed += 1;
                }
                let bit = 1u64 << (low & 63);
                let at =
                    before + self.keys_passed + u64::from((words[word] & (bit - 1)).count_ones());
                if words[word] & bit != 0 {
                    Ok(at)
                } else {
                    Err(at)
                }
            }
        }
    }
}

/// Iterator over the ranges of a [`RowSet`].
pub struct Ranges<'a> {
    spans: std::slice::Iter<'a, Span>,
    /// The block of the runs being walked.
    block: u64,
    /// What is left of the runs of the current span: of its run list, or else of its bitmap.
    list: std::slice::Iter<'a, Run>,
    bits: Option<BitRuns<'a>>,
    /// A piece already taken from the spans that starts the next range.
    open: Option<(RowKey, RowKey)>,
}

impl Ranges<'_> {
    /// The next run of keys within one span.
    fn piece(&mut self) -> Option<(RowKey, RowKey)> {
        loop {
            let run = match self.list.next() {
                Some(run) => Some(*run),
                None => self.bits.as_mut().and_then(Iterator::next),
            };
            if let Some(run) = run {
                return Some((key_at(self.block, run.first), key_at(self.block, run.last)));
            }
            let span = self.spans.next()?;
            self.block = span.block;
            self.bits = None;
            match &span.body {
                Body::Full(_) => {
                    return Some((key_at(span.block, 0), key_at(span.end() - 1, u16::MAX)));
                }
                body => match body.view() {
                    View::Runs(runs) => self.list = runs.iter(),
                    View::Bits(words) => self.bits = Some(BitRuns::new(words)),
                },
            }
        }
    }
}

impl Iterator for Ranges<'_> {
    type Item = RowRange;

    fn next(&mut self) -> Option<RowRange> {
        let (first, mut last) = match self.open.take() {
            Some(piece) => piece,
            None => self.piece()?,
        };
        // Only a piece that ends its block can go on in the next block.
        while low(last) == u16::MAX {
            match self.piece() {
                Some((next_first, next_last)) if next_first == last + 1 => last = next_last,
                next => {
                    self.open = next;
                    break;
                }
            }
        }
        Some(RowRange::new(first, last))
    }
}
