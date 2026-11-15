//! Union, intersection and difference of two row sets, span by span.

use super::container::{combine, Body, Op, BLOCK_KEYS};
use super::{leading, BlockRowSet, Span};

/// A position in the spans `at..end` of a row set, for a walk in block order.
pub(super) struct Cursor<'a> {
    set: &'a BlockRowSet,
    at: usize,
    end: usize,
    /// Blocks of `spans[at]` already passed. Only a span of whole blocks is passed in parts.
    passed: u64,
}

impl<'a> Cursor<'a> {
    fn span(&self) -> &'a Span {
        &self.set.spans[self.at]
    }

    /// The block the cursor is at, or `None` at the end.
    fn block(&self) -> Option<u64> {
        (self.at < self.end).then(|| self.span().block + self.passed)
    }

    /// Blocks of the current span from the cursor on.
    fn left(&self) -> u64 {
        self.span().blocks() - self.passed
    }

    /// Moves on by `blocks`, which is at most `left()`.
    fn advance(&mut self, blocks: u64) {
        self.passed += blocks;
        if self.passed == self.span().blocks() {
            self.at += 1;
            self.passed = 0;
        }
    }

    /// Moves to the first block at or after `limit`. With `out`, the keys passed are added to
    /// it.
    fn pass(&mut self, limit: u64, mut out: Option<&mut BlockRowSet>) {
        while let Some(block) = self.block().filter(|&b| b < limit) {
            let span = self.span();
            if self.passed == 0 && span.end() <= limit {
                let rest = &self.set.spans[self.at..self.end];
                let whole = leading(rest, |s| s.end() <= limit);
                if let Some(out) = out.as_deref_mut() {
                    out.extend(self.set, self.at, self.at + whole);
                }
                self.at += whole;
            } else {
                // Part of a span of whole blocks: what is left of it, or up to the limit.
                let blocks = self.left().min(limit - block);
                if let Some(out) = out.as_deref_mut() {
                    out.push(block, Body::Full(blocks), blocks * BLOCK_KEYS);
                }
                self.advance(blocks);
            }
        }
    }
}

impl BlockRowSet {
    pub(super) fn all(&self) -> Cursor<'_> {
        self.window(0, self.spans.len())
    }

    fn window(&self, at: usize, end: usize) -> Cursor<'_> {
        Cursor {
            set: self,
            at,
            end,
            passed: 0,
        }
    }

    /// `self = self op other`, for an `op` that keeps the keys of `self` in blocks where
    /// `other` has none (union and minus). Only the spans of `self` among the blocks of
    /// `other` are rebuilt.
    pub(super) fn merge_in_place(&mut self, op: Op, other: &BlockRowSet) {
        debug_assert!(op != Op::Intersect);
        let (Some(first), Some(last)) = (other.spans.first(), other.spans.last()) else {
            return;
        };
        let lo = self.spans.partition_point(|s| s.end() <= first.block);
        let hi = self.spans.partition_point(|s| s.block < last.end());
        let merged = merge(op, self.window(lo, hi), other.all());

        let before = self.before(lo);
        let old_keys = self.before(hi) - before;
        let new_keys = merged.size();
        let count = merged.spans.len();
        self.spans.splice(lo..hi, merged.spans);
        self.ends
            .splice(lo..hi, merged.ends.into_iter().map(|end| before + end));
        for end in &mut self.ends[lo + count..] {
            *end = *end - old_keys + new_keys;
        }
        // Whole blocks on either side of a seam are one span. The later seam first, so that
        // the index of the earlier one stays valid.
        self.join(lo + count);
        self.join(lo);
    }

    /// Makes `spans[i - 1]` and `spans[i]` one span if both are whole blocks and adjacent.
    fn join(&mut self, i: usize) {
        if i == 0 || i >= self.spans.len() {
            return;
        }
        let (Body::Full(a), Body::Full(b)) = (&self.spans[i - 1].body, &self.spans[i].body) else {
            return;
        };
        if self.spans[i - 1].block + a != self.spans[i].block {
            return;
        }
        self.spans[i - 1].body = Body::Full(a + b);
        self.ends[i - 1] = self.ends[i];
        self.spans.remove(i);
        self.ends.remove(i);
    }
}

/// `a op b` over the spans the two cursors cover.
pub(super) fn merge(op: Op, mut a: Cursor<'_>, mut b: Cursor<'_>) -> BlockRowSet {
    let mut out = BlockRowSet::empty();
    // Whether the keys of one side stay where the other side has no block.
    let (keep_a, keep_b) = (op != Op::Intersect, op == Op::Union);
    loop {
        let block = match (a.block(), b.block()) {
            (None, None) => break,
            (Some(x), y) if y.is_none_or(|y| x < y) => {
                a.pass(y.unwrap_or(u64::MAX), keep_a.then_some(&mut out));
                continue;
            }
            (x, Some(y)) if x.is_none_or(|x| y < x) => {
                b.pass(x.unwrap_or(u64::MAX), keep_b.then_some(&mut out));
                continue;
            }
            (Some(x), _) => x,
            (None, Some(_)) => unreachable!("the arm above takes a `b` that is alone"),
        };
        // Both sides have keys in `block`.
        let (body_a, body_b) = (&a.span().body, &b.span().body);
        let blocks = match (op, body_a, body_b) {
            (_, Body::Full(_), Body::Full(_)) => {
                let blocks = a.left().min(b.left());
                if op != Op::Minus {
                    out.push(block, Body::Full(blocks), blocks * BLOCK_KEYS);
                }
                blocks
            }
            (Op::Union, Body::Full(_), _) | (Op::Union, _, Body::Full(_)) => {
                out.push(block, Body::Full(1), BLOCK_KEYS);
                1
            }
            (Op::Intersect, Body::Full(_), kept) => {
                out.push(block, kept.clone(), b.set.keys_of(b.at));
                1
            }
            (Op::Intersect, kept, Body::Full(_)) => {
                out.push(block, kept.clone(), a.set.keys_of(a.at));
                1
            }
            (Op::Minus, _, Body::Full(_)) => 1,
            _ => {
                if let Some((body, keys)) = combine(op, body_a.view(), body_b.view()).seal() {
                    out.push(block, body, keys);
                }
                1
            }
        };
        a.advance(blocks);
        b.advance(blocks);
    }
    out
}
