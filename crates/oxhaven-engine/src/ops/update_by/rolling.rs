//! The rolling operators: each row's result is computed from a window of rows around it.
//!
//! Java slides one window over the rows it recomputes, in order: before each row it pops
//! the values that left the window and pushes the ones that entered. Some operators keep a
//! running value across those pushes and pops, so their result for a row can depend on the
//! rows computed before it. The kernels here are driven by the same pops and pushes.

use super::failure;
use super::values::{
    double_array, double_result, long_array, long_result, narrowed_floats, pick, Values,
};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowRange, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Rolling {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Product,
}

/// A window in rows: the `rev` rows up to and including the current row, and the `fwd` rows
/// after it. Either may be negative; their sum is not.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct Ticks {
    pub(super) rev: i64,
    pub(super) fwd: i64,
}

impl Ticks {
    /// The positions, in a bucket of `size` rows, of the rows in the window of any row at
    /// `positions`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTicks#computeInfluencerRowsTicks
    pub(super) fn influencers(self, size: u64, positions: &RowSet) -> RowSet {
        spread(size, positions, self.rev, self.fwd)
    }

    /// The positions of the rows whose window holds any row at `positions`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTicks#computeAffectedRowsTicks
    pub(super) fn affected(self, size: u64, positions: &RowSet) -> RowSet {
        spread(size, positions, self.fwd + 1, self.rev - 1)
    }

    /// As `self`, with `rev` at least `min_rev` and `fwd` at least 0: the window Java uses
    /// to find the rows affected by added (`min_rev` 1) and removed (`min_rev` 0) rows.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTicks#computeAffectedRowsAndOperators
    pub(super) fn at_least(self, min_rev: i64) -> Ticks {
        Ticks {
            rev: self.rev.max(min_rev),
            fwd: self.fwd.max(0),
        }
    }

    /// For each affected position, in order, how many influencers leave the window and how
    /// many enter it. `influencers` holds the positions of the influencer rows, ascending.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTicks#computeWindows
    pub(super) fn slides(self, size: u64, affected: &RowSet, influencers: &RowSet) -> Vec<Slide> {
        let influencers: Vec<i64> = influencers.iter().collect();
        let (mut head, mut tail) = (0, 0);
        affected
            .iter()
            .map(|position| {
                let first = (position - self.rev + 1).max(0);
                let last = (position + self.fwd).min(size as i64 - 1);
                let old = (head, tail);
                while head < influencers.len() && influencers[head] < first {
                    head += 1;
                }
                while tail < influencers.len() && influencers[tail] <= last {
                    tail += 1;
                }
                Slide {
                    pop: head - old.0,
                    push: tail - old.1,
                }
            })
            .collect()
    }
}

/// How the window moves to reach one affected row.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Slide {
    pub(super) pop: usize,
    pub(super) push: usize,
}

fn spread(size: u64, positions: &RowSet, rev: i64, fwd: i64) -> RowSet {
    let max = size as i64 - 1;
    let mut next = 0;
    let mut ranges = Vec::new();
    for range in positions.ranges() {
        let head = (range.first() - rev + 1).max(next);
        let tail = (range.last() + fwd).min(max);
        // Java's two tests, on the bounds before they are clamped, and a third for a range
        // that clamping leaves empty because the range before it reached the last row.
        if range.last() + fwd < next || range.first() - rev + 1 > max || head > tail {
            continue;
        }
        ranges.push(RowRange::new(head, tail));
        next = tail + 1;
    }
    RowSet::from_ranges(ranges)
}

impl Rolling {
    /// The type of the result column for an input column of type `input`, or `None` if Java
    /// has no operator for that type.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor
    /// #makeRollingSumOperator, #makeRollingAvgOperator, #makeRollingMinMaxOperator,
    /// #makeRollingProductOperator and #makeRollingCountOperator
    pub(super) fn output_type(self, input: &ColumnType) -> Option<ColumnType> {
        use ColumnType::*;
        match self {
            Rolling::Sum => match input {
                Boolean | Byte | Char | Short | Int | Long => Some(Long),
                Float | Double => Some(Double),
                _ => None,
            },
            Rolling::Avg => match input {
                Boolean | Byte | Char | Short | Int | Long | Float | Double => Some(Double),
                _ => None,
            },
            Rolling::Min | Rolling::Max => match input {
                Byte | Char | Short | Int | Long | Instant | Float | Double | String => {
                    Some(input.clone())
                }
                _ => None,
            },
            // An Instant column reaches ObjectRollingCountOperator as a chunk of longs and
            // the operation fails with a ClassCastException.
            Rolling::Count => (*input != Instant).then_some(Long),
            Rolling::Product => match input {
                Byte | Char | Short | Int | Long | Float | Double => Some(Double),
                _ => None,
            },
        }
    }

    /// The result for each slide of the window over `influencers`, the input values of the
    /// influencer rows in order.
    pub(super) fn evaluate(
        self,
        input: &ColumnType,
        influencers: &ArrayRef,
        slides: &[Slide],
    ) -> Result<ArrayRef, TableFailure> {
        let output = self
            .output_type(input)
            .ok_or_else(|| failure("no rolling operator for this column type"))?;
        let values = if self == Rolling::Count && *input == ColumnType::String {
            // Only nullness matters to a count.
            Values::Int(
                (0..influencers.len())
                    .map(|i| influencers.is_valid(i).then_some(0))
                    .collect(),
            )
        } else {
            Values::read(influencers, input)?
        };
        // Behavior: the `Assert.geq(size, count)` that opens `pop` in every rolling operator's
        // context. A window that holds no row at all (`rev + fwd == 0`) slides its head ahead
        // of its tail, and Java fails the operation.
        let mut held = 0;
        for slide in slides {
            if slide.pop > held {
                return Err(failure(format!(
                    "Assertion failed: asserted windowValues.size() >= {}, instead windowValues.size() == {held}.",
                    slide.pop
                )));
            }
            held = held - slide.pop + slide.push;
        }
        let window = Window::over(&values, slides);
        Ok(match (self, &values) {
            (Rolling::Count, _) => {
                long_array(window.map(|w| Some((w.end - w.start - w.nulls) as i64)))
            }
            (Rolling::Sum, Values::Int(v)) => long_array(sum_longs(v, window)),
            (Rolling::Sum, Values::Float(v)) => double_array(sum_doubles(v, window)),
            (Rolling::Avg, Values::Int(v)) => double_array(avg_longs(v, window)),
            (Rolling::Avg, Values::Float(v)) => double_array(if *input == ColumnType::Float {
                avg_floats(v, window)
            } else {
                avg_doubles(v, window)
            }),
            (Rolling::Product, Values::Int(v)) => {
                let v: Vec<Option<f64>> = v.iter().map(|v| v.map(|v| v as f64)).collect();
                double_array(products(&v, window))
            }
            (Rolling::Product, Values::Float(v)) => double_array(products(v, window)),
            (Rolling::Min | Rolling::Max, Values::Float(v)) => {
                let max_value = if *input == ColumnType::Float {
                    f64::from(f32::MAX)
                } else {
                    f64::MAX
                };
                narrowed_floats(
                    &output,
                    float_extremes(v, window, self == Rolling::Max, max_value),
                )
            }
            (Rolling::Min | Rolling::Max, _) => {
                let wanted = if self == Rolling::Max {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
                return pick(None, influencers, extremes(&values, window, wanted));
            }
            (_, Values::Text(_)) => {
                return Err(failure("no rolling operator for this column type"))
            }
        })
    }
}

/// The window before one affected row is written: the influencers `start..end`, of which
/// `nulls` are null, after `popped` left and `pushed` entered.
struct Step {
    start: usize,
    end: usize,
    nulls: usize,
    popped: std::ops::Range<usize>,
    pushed: std::ops::Range<usize>,
}

/// Replays the slides of a window, counting the nulls in it as every Java rolling operator
/// does (`nullCount`).
struct Window<'a> {
    values: &'a Values,
    slides: std::slice::Iter<'a, Slide>,
    start: usize,
    end: usize,
    nulls: usize,
}

impl<'a> Window<'a> {
    fn over(values: &'a Values, slides: &'a [Slide]) -> Self {
        Window {
            values,
            slides: slides.iter(),
            start: 0,
            end: 0,
            nulls: 0,
        }
    }
}

impl Iterator for Window<'_> {
    type Item = Step;

    /// Behavior: io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator.Context#accumulateRolling,
    /// which pops before it pushes
    fn next(&mut self) -> Option<Step> {
        let slide = self.slides.next()?;
        let popped = self.start..self.start + slide.pop;
        let pushed = self.end..self.end + slide.push;
        let nulls = |range: &std::ops::Range<usize>| {
            range.clone().filter(|&i| self.values.is_null(i)).count()
        };
        self.nulls = self.nulls + nulls(&pushed) - nulls(&popped);
        self.start = popped.end;
        self.end = pushed.end;
        Some(Step {
            start: self.start,
            end: self.end,
            nulls: self.nulls,
            popped,
            pushed,
        })
    }
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingsum.LongRollingSumOperator.Context
/// #push, #pop and #writeToOutputChunk: a running total that values are added to and
/// subtracted from, which restarts from the next value when it reads as null
fn sum_longs(values: &[Option<i64>], window: Window) -> Vec<Option<i64>> {
    let mut total: Option<i64> = None;
    window
        .map(|w| {
            for value in values[w.popped].iter().flatten() {
                // Java subtracts from the reserved value too.
                total = long_result(total.unwrap_or(i64::MIN).wrapping_sub(*value));
            }
            for value in values[w.pushed].iter().flatten() {
                total = long_result(match total {
                    None => *value,
                    Some(total) => total.wrapping_add(*value),
                });
            }
            if w.end - w.start == w.nulls {
                None
            } else {
                total
            }
        })
        .collect()
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingavg.LongRollingAvgOperator.Context
/// #push, #pop and #writeToOutputChunk: a running `double` total divided by the count of
/// values that are not null
fn avg_longs(values: &[Option<i64>], window: Window) -> Vec<Option<f64>> {
    let mut total: Option<f64> = None;
    window
        .map(|w| {
            for value in values[w.popped].iter().flatten() {
                total = double_result(total.unwrap_or(-f64::MAX) - *value as f64);
            }
            for value in values[w.pushed].iter().flatten() {
                total = double_result(match total {
                    None => *value as f64,
                    Some(total) => total + *value as f64,
                });
            }
            let count = w.end - w.start - w.nulls;
            if count == 0 {
                None
            } else {
                double_result(total.unwrap_or(-f64::MAX) / count as f64)
            }
        })
        .collect()
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingproduct.DoubleRollingProductOperator.Context
/// #push, #pop and #writeToOutputChunk and the functions of its buffer, and
/// LongRollingProductOperator, whose values are never NaN or infinite
fn products(values: &[Option<f64>], window: Window) -> Vec<Option<f64>> {
    fn slots(a: f64, b: f64) -> f64 {
        if a == NULL_DOUBLE && b == NULL_DOUBLE {
            1.0
        } else if a == NULL_DOUBLE {
            b
        } else if b == NULL_DOUBLE {
            a
        } else {
            a * b
        }
    }
    PairwiseRing::new(1.0, slots, |a, b| a * b).replay(values, window, raw_double, |w, product| {
        if w.end - w.start == w.nulls {
            return None;
        }
        let in_window = || values[w.start..w.end].iter().flatten();
        let zero = in_window().any(|v| *v == 0.0);
        if in_window().any(|v| v.is_nan()) || (zero && in_window().any(|v| v.is_infinite())) {
            Some(f64::NAN)
        } else if zero {
            Some(0.0)
        } else {
            double_result(product())
        }
    })
}

/// The buffer the floating point rolling sum, average and product keep their window in.
/// Its slots are the leaves of a binary tree and the aggregate is the root, so which values
/// are combined first depends on the slots they are in: a value's slot is the count of
/// values pushed before it, modulo the capacity, until the buffer grows and moves the
/// window to the front. A result can depend on that order in its last bits.
///
/// Values are held as Java holds them, a null as the reserved value of the type, because
/// the functions that combine them test for it.
///
/// Behavior: io.deephaven.base.ringbuffer.AggregatingDoubleRingBuffer#evaluate,
/// #removeUnsafe and #grow, and io.deephaven.base.ringbuffer.DoubleRingBuffer#grow
struct PairwiseRing<T> {
    slots: Vec<T>,
    head: usize,
    tail: usize,
    identity: T,
    /// The tree: node `n` combines nodes `2n` and `2n + 1`, the lowest level combines
    /// pairs of slots, and node 1 is the root.
    tree_nodes: Vec<T>,
    /// The slots `calc_head..calc_tail`, as `head` and `tail` count them, are the ones
    /// the tree was last brought up to date for.
    calc_head: i64,
    calc_tail: i64,
    /// Combines two slots.
    leaf: fn(T, T) -> T,
    /// Combines two results.
    tree: fn(T, T) -> T,
}

impl<T: Copy> PairwiseRing<T> {
    /// Behavior: `BUFFER_INITIAL_SIZE` of DoubleRollingSumOperator, FloatRollingSumOperator
    /// and DoubleRollingProductOperator, and `PAIRWISE_BUFFER_INITIAL_SIZE` of
    /// DoubleRollingAvgOperator and FloatRollingAvgOperator
    const INITIAL_CAPACITY: usize = 64;

    fn new(identity: T, leaf: fn(T, T) -> T, tree: fn(T, T) -> T) -> Self {
        PairwiseRing {
            slots: vec![identity; Self::INITIAL_CAPACITY],
            head: 0,
            tail: 0,
            identity,
            tree_nodes: vec![identity; Self::INITIAL_CAPACITY],
            calc_head: 0,
            calc_tail: 0,
            leaf,
            tree,
        }
    }

    fn pop(&mut self, count: usize) {
        for _ in 0..count {
            let mask = self.slots.len() - 1;
            self.slots[self.head & mask] = self.identity;
            self.head += 1;
        }
    }

    fn push(&mut self, values: impl ExactSizeIterator<Item = T>) {
        let size = self.tail - self.head;
        if self.slots.len() - size < values.len() {
            let capacity = (self.slots.len() + values.len()).next_power_of_two();
            let mask = self.slots.len() - 1;
            let mut slots: Vec<T> = (self.head..self.tail)
                .map(|at| self.slots[at & mask])
                .collect();
            slots.resize(capacity, self.identity);
            self.slots = slots;
            self.tree_nodes = vec![self.identity; capacity];
            self.head = 0;
            self.tail = size;
            self.calc_head = 0;
            self.calc_tail = 0;
        }
        let mask = self.slots.len() - 1;
        for value in values {
            self.slots[self.tail & mask] = value;
            self.tail += 1;
        }
    }

    /// The root of the tree, after bringing up to date the part of it that Java does: the
    /// slots left since the last call and the slots filled since. Java skips the call for
    /// a product with a zero in its window, so the bookkeeping is kept as Java keeps it
    /// rather than recomputing the whole tree.
    ///
    /// Behavior: AggregatingDoubleRingBuffer#evaluate, line for line
    fn evaluate(&mut self) -> T {
        let length = self.slots.len() as i64;
        let mask = length - 1;
        let (head, tail) = (self.head as i64, self.tail as i64);
        let intersection = if self.calc_tail > head {
            self.calc_tail - head
        } else {
            0
        };
        let mut r1_head = self.calc_head;
        let mut r1_tail = self.calc_tail - intersection;
        let mut r2_head = head + intersection;
        let mut r2_tail = tail;
        let base = r1_head;
        r1_head -= base;
        r1_tail -= base;
        r2_head -= base;
        r2_tail -= base;
        let r2_size = r2_tail - r2_head;
        r2_head &= mask;
        r2_tail = r2_head + r2_size;
        if r2_tail <= length {
            if r2_tail <= r1_tail {
                r2_head = 0;
                r2_tail = 0;
            } else if r2_head <= r1_tail {
                r1_tail = r2_tail;
                r2_head = 0;
                r2_tail = 0;
            }
        } else {
            r1_head = r1_head.min(r2_head - length) + length;
            r1_tail = r2_tail.max(r2_tail - length) + length;
            r2_head = 0;
            r2_tail = 0;
        }
        r1_head += base;
        r1_tail += base;
        r2_head += base;
        r2_tail += base;
        if r1_tail - r1_head >= length || r2_tail - r2_head >= length {
            self.fixup_tree(0, length, 0, 0);
        } else {
            self.fixup_tree(r1_head, r1_tail, r2_head, r2_tail);
        }
        self.calc_head = head;
        self.calc_tail = tail;
        self.tree_nodes[1]
    }

    /// Behavior: AggregatingDoubleRingBuffer#fixupTree, line for line, with its fall
    /// through from a lone range that wraps into the code for two ranges
    fn fixup_tree(&mut self, r1_head: i64, r1_tail: i64, r2_head: i64, r2_tail: i64) {
        let length = self.slots.len() as i64;
        let mask = length - 1;
        let r1_size = r1_tail - r1_head;
        let r2_size = r2_tail - r2_head;
        let offset = length / 2;
        if r1_size == 0 && r2_size == 0 {
            return;
        }
        if r2_size == 0 {
            let r1_head_normal = r1_head & mask;
            let r1_tail_normal = r1_head_normal + r1_size;
            if r1_tail_normal <= length {
                let (r1h, r1t) = (r1_head_normal, r1_tail_normal - 1);
                self.store_slots(r1h, r1t);
                self.evaluate_range(offset + r1h / 2, offset + r1t / 2);
                return;
            }
            let (r1h, r1t) = (0, r1_tail_normal - length - 1);
            let (r2h, r2t) = (r1_head_normal, length - 1);
            self.store_slots(r1h, r1t);
            self.store_slots(r2h, r2t);
            self.evaluate_two_ranges(
                offset + r1h / 2,
                offset + r1t / 2,
                offset + r2h / 2,
                offset + r2t / 2,
            );
        }
        let (r1_head_normal, r1_tail_normal, r2_head_normal, r2_tail_normal) = {
            let r1_head_tmp = r1_head & mask;
            let r1_tail_tmp = r1_head_tmp + r1_size;
            let r2_head_tmp = r2_head & mask;
            let r2_tail_tmp = r2_head_tmp + r2_size;
            if r1_head_tmp <= r2_head_tmp {
                (r1_head_tmp, r1_tail_tmp, r2_head_tmp, r2_tail_tmp)
            } else {
                (r2_head_tmp, r2_tail_tmp, r1_head_tmp, r1_tail_tmp)
            }
        };
        if r1_tail_normal <= length && r2_tail_normal <= length {
            let (r1h, r1t) = (r1_head_normal, r1_tail_normal - 1);
            let (r2h, r2t) = (r2_head_normal, r2_tail_normal - 1);
            self.store_slots(r1h, r1t);
            self.store_slots(r2h, r2t);
            self.evaluate_two_ranges(
                offset + r1h / 2,
                offset + r1t / 2,
                offset + r2h / 2,
                offset + r2t / 2,
            );
            return;
        }
        let (r1h, r1t, r2h, r2t, r3h, r3t) = if r1_tail_normal <= length {
            (
                0,
                r2_tail_normal - length - 1,
                r1_head_normal,
                r1_tail_normal - 1,
                r2_head_normal,
                length - 1,
            )
        } else {
            (
                0,
                r1_tail_normal - length - 1,
                r2_head_normal,
                r2_tail_normal - 1,
                r1_head_normal,
                length - 1,
            )
        };
        self.store_slots(r1h, r1t);
        self.store_slots(r2h, r2t);
        self.store_slots(r3h, r3t);
        self.evaluate_three_ranges(
            [offset + r1h / 2, offset + r1t / 2],
            [offset + r2h / 2, offset + r2t / 2],
            [offset + r3h / 2, offset + r3t / 2],
        );
    }

    /// Behavior: AggregatingDoubleRingBuffer#evaluateThreeRanges
    fn evaluate_three_ranges(&mut self, mut r1: [i64; 2], mut r2: [i64; 2], mut r3: [i64; 2]) {
        loop {
            if r1[1] >= r2[0] {
                return self.evaluate_two_ranges(r1[0], r2[1], r3[0], r3[1]);
            }
            if r2[1] >= r3[0] {
                return self.evaluate_two_ranges(r1[0], r1[1], r2[0], r3[1]);
            }
            for range in [&mut r1, &mut r2, &mut r3] {
                self.store_nodes(range[0], range[1]);
                *range = [range[0] / 2, range[1] / 2];
            }
        }
    }

    /// Behavior: AggregatingDoubleRingBuffer#evaluateTwoRanges
    fn evaluate_two_ranges(&mut self, mut r1h: i64, mut r1t: i64, mut r2h: i64, mut r2t: i64) {
        loop {
            if r1t >= r2h {
                return self.evaluate_range(r1h, r2t);
            }
            self.store_nodes(r1h, r1t);
            self.store_nodes(r2h, r2t);
            r1h /= 2;
            r1t /= 2;
            r2h /= 2;
            r2t /= 2;
        }
    }

    /// Behavior: AggregatingDoubleRingBuffer#evaluateRange
    fn evaluate_range(&mut self, mut r1h: i64, mut r1t: i64) {
        while r1t > 1 {
            self.store_nodes(r1h, r1t);
            r1h /= 2;
            r1t /= 2;
        }
    }

    /// Behavior: AggregatingDoubleRingBuffer#evaluateAndStoreResults from the storage buffer
    fn store_slots(&mut self, start: i64, end: i64) {
        let offset = self.slots.len() / 2;
        let mut left = start & !1;
        while left <= end {
            let at = left as usize;
            self.tree_nodes[at / 2 + offset] = (self.leaf)(self.slots[at], self.slots[at + 1]);
            left += 2;
        }
    }

    /// Behavior: AggregatingDoubleRingBuffer#evaluateAndStoreResults from the tree
    fn store_nodes(&mut self, start: i64, end: i64) {
        let mut left = start & !1;
        while left <= end {
            let at = left as usize;
            self.tree_nodes[at / 2] = (self.tree)(self.tree_nodes[at], self.tree_nodes[at + 1]);
            left += 2;
        }
    }

    /// The aggregate of each window, `None` where `absent` says there is none to take.
    fn replay<V: Copy>(
        mut self,
        values: &[Option<V>],
        window: Window,
        raw: fn(Option<V>) -> T,
        mut result: impl FnMut(&Step, &mut dyn FnMut() -> T) -> Option<f64>,
    ) -> Vec<Option<f64>> {
        window
            .map(|w| {
                self.pop(w.popped.len());
                self.push(values[w.pushed.clone()].iter().map(|v| raw(*v)));
                result(&w, &mut || self.evaluate())
            })
            .collect()
    }
}

const NULL_DOUBLE: f64 = -f64::MAX;
const NULL_FLOAT: f32 = -f32::MAX;

fn raw_double(value: Option<f64>) -> f64 {
    value.unwrap_or(NULL_DOUBLE)
}

/// Behavior: the value function of io.deephaven.engine.table.impl.updateby.rollingsum
/// .DoubleRollingSumOperator.Context and FloatRollingSumOperator.Context, whose tree
/// function is `Double::sum`
fn sum_slots(a: f64, b: f64) -> f64 {
    if a == NULL_DOUBLE && b == NULL_DOUBLE {
        0.0
    } else if a == NULL_DOUBLE {
        b
    } else if b == NULL_DOUBLE {
        a
    } else {
        a + b
    }
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingsum.DoubleRollingSumOperator.Context
/// #writeToOutputChunk; FloatRollingSumOperator is the same over values widened to double
fn sum_doubles(values: &[Option<f64>], window: Window) -> Vec<Option<f64>> {
    PairwiseRing::new(0.0, sum_slots, |a, b| a + b).replay(values, window, raw_double, |w, sum| {
        (w.end - w.start != w.nulls)
            .then(|| sum())
            .and_then(double_result)
    })
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingavg.DoubleRollingAvgOperator.Context
/// #writeToOutputChunk and the one function of its buffer, which unlike the sum's passes a
/// null up the tree
fn avg_doubles(values: &[Option<f64>], window: Window) -> Vec<Option<f64>> {
    fn add(a: f64, b: f64) -> f64 {
        if a == NULL_DOUBLE {
            b
        } else if b == NULL_DOUBLE {
            a
        } else {
            a + b
        }
    }
    PairwiseRing::new(0.0, add, add).replay(values, window, raw_double, |w, sum| {
        let count = w.end - w.start - w.nulls;
        (count != 0)
            .then(|| sum() / count as f64)
            .and_then(double_result)
    })
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingavg.FloatRollingAvgOperator.Context
/// #writeToOutputChunk and the one function of its buffer: the sum is taken in `float`
/// arithmetic and only then widened
fn avg_floats(values: &[Option<f64>], window: Window) -> Vec<Option<f64>> {
    fn add(a: f32, b: f32) -> f32 {
        if a == NULL_FLOAT {
            b
        } else if b == NULL_FLOAT {
            a
        } else {
            a + b
        }
    }
    PairwiseRing::new(0.0, add, add).replay(
        values,
        window,
        |v| v.map_or(NULL_FLOAT, |v| v as f32),
        |w, sum| {
            let count = w.end - w.start - w.nulls;
            (count != 0)
                .then(|| f64::from(sum()) / count as f64)
                .and_then(double_result)
        },
    )
}

/// The index of the smallest or largest value in each window, for the types whose every
/// value compares with every other: the integer types, `Instant` and `String`.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingminmax.LongRollingMinMaxOperator.Context
/// and io.deephaven.engine.table.impl.updateby.rollingminmax.ComparableRollingMinMaxOperator.Context:
/// for a total order their running value is the extreme of the window, whatever was pushed
/// and popped before
fn extremes(values: &Values, window: Window, wanted: Ordering) -> Vec<Option<usize>> {
    window
        .map(|w| {
            (w.start..w.end)
                .filter(|&i| !values.is_null(i))
                .reduce(|best, i| {
                    if values.compare(i, best) == Some(wanted) {
                        i
                    } else {
                        best
                    }
                })
        })
        .collect()
}

/// Behavior: java.lang.Math#max(double, double) and #min: NaN if either is NaN, and `-0.0`
/// is less than `0.0`
fn java_extreme(a: f64, b: f64, max: bool) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == b {
        // Equal values differ only in the sign of zero.
        if a.is_sign_negative() == max {
            b
        } else {
            a
        }
    } else if (a > b) == max {
        a
    } else {
        b
    }
}

/// The rolling minimum or maximum of a `float` or `double` column, with the running value
/// Java keeps. It is not always the extreme of the window: a minimum starts from the
/// largest finite value of the type (`max_value`), so that a window that never held
/// anything smaller shows that value; a NaN is never replaced by a push; and the window is
/// evaluated again only after a value equal to the running value is popped.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingminmax.DoubleRollingMinMaxOperator.Context
/// #push, #pop, #writeToOutputChunk and #reset, and FloatRollingMinMaxOperator
fn float_extremes(
    values: &[Option<f64>],
    window: Window,
    max: bool,
    max_value: f64,
) -> Vec<Option<f64>> {
    let mut current = if max { None } else { Some(max_value) };
    let mut evaluate = false;
    window
        .map(|w| {
            for value in values[w.popped].iter().flatten() {
                if current == Some(*value) {
                    evaluate = true;
                }
            }
            for value in values[w.pushed].iter().flatten() {
                let replace = match current {
                    None => true,
                    Some(held) => {
                        if max {
                            held < *value
                        } else {
                            held > *value
                        }
                    }
                };
                if replace {
                    current = Some(*value);
                    evaluate = false;
                }
            }
            if w.end - w.start == w.nulls {
                current = None;
            } else if evaluate {
                // The ring buffer folds its values onto an identity: null for a maximum,
                // the largest finite value for a minimum.
                let identity = if max { None } else { Some(max_value) };
                current = values[w.start..w.end]
                    .iter()
                    .flatten()
                    .fold(identity, |held, value| {
                        Some(match held {
                            None => *value,
                            Some(held) => java_extreme(held, *value, max),
                        })
                    });
            }
            evaluate = false;
            current
        })
        .collect()
}
