//! A row set for large tables: the key space is cut into blocks of 2^16 keys, and each block
//! that holds keys stores them in the smallest of three forms.
//!
//! [`BlockRowSet`] has the operations of the contract type
//! [`RowSet`](oxhaven_contracts::rowset::RowSet), with the same names and the same results; the
//! tests of this module compare every operation with the contract type. It differs in one
//! signature: [`ranges`](BlockRowSet::ranges) is an iterator, because the ranges are not stored
//! as a slice. `docs/ROWSET.md` has the measurements behind the design and what is needed to
//! make this the engine's row set.
//!
//! # Layout
//!
//! A row set is a sorted vector of spans and, beside it, the number of keys up to the end of
//! each span. A span is one of:
//!
//! - a count of whole blocks, however many: a table that holds every key from 0 to a billion
//!   is one span;
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
//!   per step where both blocks are bitmaps. `intersect` skips, by binary search, the spans of
//!   one set that lie between two spans of the other;
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

use container::{BitRuns, BlockRuns, Body, Op, Run, View, BLOCK_KEYS};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, RowSetError};
use std::fmt;

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

/// An ordered set of row keys, for tables with many rows. See the [module](self) for the layout.
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct BlockRowSet {
    /// Sorted by block, not overlapping, each in the one form its keys allow (see `Body`).
    spans: Vec<Span>,
    /// `ends[i]` is the number of keys in `spans[..=i]`.
    ///
    /// Behavior: io.deephaven.engine.rowset.impl.rsp.RspArray#ensureCardinalityCache
    ends: Vec<u64>,
}

impl fmt::Debug for BlockRowSet {
    /// The same text as the contract row set with these keys.
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

impl BlockRowSet {
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

    /// The ranges of this set: sorted, and neither overlapping nor adjacent.
    pub fn ranges(&self) -> Ranges<'_> {
        Ranges {
            spans: self.spans.iter(),
            block: 0,
            runs: Runs::Done,
            open: None,
        }
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
    pub fn sub_set_for_positions(&self, positions: &BlockRowSet, reversed: bool) -> BlockRowSet {
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
        let mut out = BlockRowSet::empty();
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
    pub fn union(&self, other: &BlockRowSet) -> BlockRowSet {
        merge::merge(Op::Union, self.all(), other.all())
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#intersect
    pub fn intersect(&self, other: &BlockRowSet) -> BlockRowSet {
        merge::merge(Op::Intersect, self.all(), other.all())
    }

    /// The keys in `self` that are not in `other`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#minus
    pub fn minus(&self, other: &BlockRowSet) -> BlockRowSet {
        merge::merge(Op::Minus, self.all(), other.all())
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#overlaps
    pub fn overlaps(&self, other: &BlockRowSet) -> bool {
        !self.intersect(other).is_empty()
    }

    /// Behavior: io.deephaven.engine.rowset.RowSet#subsetOf
    pub fn is_subset_of(&self, other: &BlockRowSet) -> bool {
        self.minus(other).is_empty()
    }

    /// Positions, within `self`, of each key in `keys`, which must be a subset of `self`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#invert
    pub fn invert(&self, keys: &BlockRowSet) -> Result<BlockRowSet, RowSetError> {
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
    pub fn sub_set_by_position_range(&self, start: u64, end: u64) -> BlockRowSet {
        let end = end.min(self.size());
        if start >= end {
            return BlockRowSet::empty();
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
    pub fn sub_set_by_key_range(&self, first: RowKey, last: RowKey) -> BlockRowSet {
        let first = first.max(0);
        if first > last {
            return BlockRowSet::empty();
        }
        self.intersect(&BlockRowSet::from_range(first, last))
    }

    /// Every key moved by `delta`.
    ///
    /// Behavior: io.deephaven.engine.rowset.RowSet#shift,
    /// io.deephaven.engine.rowset.impl.rsp.RspBitmap#applyOffsetImpl
    pub fn shift(&self, delta: i64) -> Result<BlockRowSet, RowSetError> {
        if let Some(first) = self.first_row_key() {
            if first + delta < 0 {
                return Err(RowSetError::NegativeKey);
            }
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
            return Ok(BlockRowSet {
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
    pub fn insert(&mut self, other: &BlockRowSet) {
        self.merge_in_place(Op::Union, other);
    }

    /// Behavior: io.deephaven.engine.rowset.WritableRowSet#remove(RowSet)
    pub fn remove(&mut self, other: &BlockRowSet) {
        self.merge_in_place(Op::Minus, other);
    }

    /// Behavior: io.deephaven.engine.rowset.WritableRowSet#retain
    pub fn retain(&mut self, other: &BlockRowSet) {
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
}

impl FromIterator<RowKey> for BlockRowSet {
    fn from_iter<T: IntoIterator<Item = RowKey>>(iter: T) -> Self {
        BlockRowSet::from_keys(iter)
    }
}

impl From<&RowSet> for BlockRowSet {
    fn from(rows: &RowSet) -> Self {
        let mut appender = Appender::default();
        for r in rows.ranges() {
            appender.append(r.first(), r.last());
        }
        appender.finish()
    }
}

impl From<&BlockRowSet> for RowSet {
    fn from(rows: &BlockRowSet) -> Self {
        RowSet::from_ranges(rows.ranges())
    }
}

/// Builds a row set from ranges that arrive in increasing order. A range may touch the one
/// before it but not overlap it.
#[derive(Default)]
struct Appender {
    out: BlockRowSet,
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
        if self.runs.is_empty() {
            return;
        }
        if let Some((body, keys)) = self.runs.take().seal() {
            self.out.push(self.block, body, keys);
        }
    }

    fn finish(mut self) -> BlockRowSet {
        self.flush();
        self.out
    }
}

/// Finds positions of keys that are asked for in increasing order, without starting over for
/// each: an inversion of `k` ranges costs the spans and blocks passed, not `k` searches.
struct Seeker<'a> {
    set: &'a BlockRowSet,
    /// The first span that may hold the next key.
    span: usize,
    /// Progress within that span's block: runs (of a run list) or words (of a bitmap) passed,
    /// and the keys in them.
    passed: usize,
    keys_passed: u64,
}

impl<'a> Seeker<'a> {
    fn new(set: &'a BlockRowSet) -> Self {
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
            self.span = from + spans[from..].partition_point(|s| s.end() <= block);
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

/// The runs of the span being walked.
enum Runs<'a> {
    Done,
    List(std::slice::Iter<'a, Run>),
    Bits(BitRuns<'a>),
}

/// Iterator over the ranges of a [`BlockRowSet`].
pub struct Ranges<'a> {
    spans: std::slice::Iter<'a, Span>,
    /// The block of `runs`.
    block: u64,
    runs: Runs<'a>,
    /// A range that the next piece may still extend, across a block boundary.
    open: Option<(RowKey, RowKey)>,
}

impl Ranges<'_> {
    /// The next run of keys within one span.
    fn piece(&mut self) -> Option<(RowKey, RowKey)> {
        loop {
            let run = match &mut self.runs {
                Runs::Done => None,
                Runs::List(runs) => runs.next().copied(),
                Runs::Bits(runs) => runs.next(),
            };
            if let Some(run) = run {
                return Some((key_at(self.block, run.first), key_at(self.block, run.last)));
            }
            let span = self.spans.next()?;
            self.block = span.block;
            self.runs = match &span.body {
                Body::Full(_) => {
                    self.runs = Runs::Done;
                    return Some((key_at(span.block, 0), key_at(span.end() - 1, u16::MAX)));
                }
                body => match body.view() {
                    View::Runs(runs) => Runs::List(runs.iter()),
                    View::Bits(words) => Runs::Bits(BitRuns::new(words)),
                },
            };
        }
    }
}

impl Iterator for Ranges<'_> {
    type Item = RowRange;

    fn next(&mut self) -> Option<RowRange> {
        loop {
            let piece = self.piece();
            match (self.open, piece) {
                (Some((first, last)), Some((next_first, next_last))) if last + 1 == next_first => {
                    self.open = Some((first, next_last));
                }
                (Some((first, last)), next) => {
                    self.open = next;
                    return Some(RowRange::new(first, last));
                }
                (None, Some(next)) => self.open = Some(next),
                (None, None) => return None,
            }
        }
    }
}
