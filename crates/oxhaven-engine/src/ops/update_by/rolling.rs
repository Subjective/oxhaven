//! The rolling operators: each row's result is computed from a window of rows around it.
//!
//! Java slides one window over the rows it recomputes, in order: before each row it pops
//! the values that left the window and pushes the ones that entered. Some operators keep a
//! running value across those pushes and pops, so their result for a row can depend on the
//! rows computed before it. The kernels here are driven by the same pops and pushes.

use super::failure;
use super::values::{
    double_array, double_result, java_extreme, long_array, long_result, narrowed_floats, pick,
    Values,
};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
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
    Std,
    /// The average of the input weighted by a second column.
    WAvg,
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

/// A window in time: the rows whose timestamp is at most `rev` nanoseconds before the
/// current row's and at most `fwd` after it, both ends included. Either may be negative;
/// their sum is not. A row without a timestamp is in no window and has no result.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct Time {
    pub(super) rev: i64,
    pub(super) fwd: i64,
}

impl Time {
    /// For each row at `affected`, timestamps in order, how many influencers leave the
    /// window and how many enter it; `influencers` are the influencers' timestamps, in order.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTime#computeWindows
    pub(super) fn slides(self, affected: &[i64], influencers: &[i64]) -> Vec<Slide> {
        let (mut head, mut tail) = (0, 0);
        affected
            .iter()
            .map(|&stamp| {
                let first = stamp.wrapping_sub(self.rev);
                let last = stamp.wrapping_add(self.fwd);
                let old = (head, tail);
                while head < tail && influencers[head] < first {
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

/// The rows of a bucket that have a timestamp, in order, and their timestamps: what Java
/// keeps in the bucket's timestamp SSA.
pub(super) struct Stamped {
    keys: Vec<RowKey>,
    stamps: Vec<i64>,
}

impl Stamped {
    /// Fails if the timestamps of the bucket decrease.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByBucketHelper#fillChunkWithNonNull,
    /// which skips a null timestamp and refuses one that is less than the one before it
    pub(super) fn new(rows: &RowSet, stamps: &[Option<i64>]) -> Result<Stamped, TableFailure> {
        let mut keys = Vec::with_capacity(stamps.len());
        let mut kept = Vec::with_capacity(stamps.len());
        for (key, stamp) in rows.iter().zip(stamps) {
            let Some(stamp) = *stamp else { continue };
            if kept.last().is_some_and(|last| stamp < *last) {
                return Err(failure(
                    "Timestamp values in UpdateBy operators must not decrease",
                ));
            }
            keys.push(key);
            kept.push(stamp);
        }
        Ok(Stamped { keys, stamps: kept })
    }

    /// The rows whose timestamp is at most `before` nanoseconds before and at most `after`
    /// after any of the timestamps `around`; a null one reaches nothing.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTime#computeInfluencerRowsTime,
    /// which walks the SSA once over timestamps that do not decrease, so that it collects the
    /// union of the windows
    pub(super) fn reach(
        &self,
        around: impl IntoIterator<Item = Option<i64>>,
        before: i64,
        after: i64,
    ) -> RowSet {
        let mut ranges: Vec<(usize, usize)> = Vec::new();
        for stamp in around.into_iter().flatten() {
            let first = stamp.wrapping_sub(before);
            let last = stamp.wrapping_add(after);
            let start = self.stamps.partition_point(|s| *s < first);
            let end = self.stamps.partition_point(|s| *s <= last);
            if start < end {
                ranges.push((start, end));
            }
        }
        ranges.sort_unstable();
        let mut keys = Vec::new();
        let mut next = 0;
        for (start, end) in ranges {
            for position in start.max(next)..end {
                keys.push(self.keys[position]);
            }
            next = next.max(end);
        }
        RowSet::from_keys(keys)
    }

    /// The timestamps of `rows`, all of which have one.
    pub(super) fn stamps_of(&self, rows: &RowSet) -> Vec<i64> {
        let mut out = Vec::with_capacity(rows.size() as usize);
        let mut position = 0;
        for key in rows.iter() {
            while self.keys[position] != key {
                position += 1;
            }
            out.push(self.stamps[position]);
        }
        out
    }

    /// Fails where Java fails to find the positions of the rows `affected`, whose
    /// timestamps are `stamps`, among the rows that have a timestamp. Java looks for them
    /// when an operator of the window wants positions, which the rolling group does, and
    /// its row sets refuse a key they do not hold, each kind in its own way: one that is a
    /// single range of keys refuses a key below the range and ignores one above it; one
    /// with gaps refuses every key it lacks; an empty one refuses nothing. So a rolling
    /// group in time fails when it computes a row without a timestamp that stands before a
    /// row with one, and does not when the rows without one come last, or are all there is.
    ///
    /// The row keys here are not Java's (PARITY.md A4), so for a ticking table this agrees
    /// with Java only as far as the keys do; and a Java row set too large for its sorted
    /// ranges (`RspBitmap`) tests less than this.
    ///
    /// Behavior: the `bucketRowSet.invert(ctx.affectedRows)` of
    /// io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingBase#processWindowBucketOperatorSet,
    /// with io.deephaven.engine.rowset.impl.singlerange.SingleRange#ixInvertOnNew and
    /// io.deephaven.engine.rowset.impl.sortedranges.SortedRanges#ixInvertOnNew
    pub(super) fn positions_exist(
        &self,
        affected: &RowSet,
        stamps: &[Option<i64>],
    ) -> Result<(), TableFailure> {
        let (Some(&first), Some(&last)) = (self.keys.first(), self.keys.last()) else {
            return Ok(());
        };
        let one_range = (last - first) as usize + 1 == self.keys.len();
        let mut unstamped = affected
            .iter()
            .zip(stamps)
            .filter_map(|(key, stamp)| stamp.is_none().then_some(key));
        if unstamped.any(|key| !one_range || key < first) {
            return Err(failure("keys argument has elements not in the rowSet"));
        }
        Ok(())
    }

    /// The rows that have a timestamp.
    pub(super) fn rows(&self) -> RowSet {
        RowSet::from_keys(self.keys.iter().copied())
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
    /// #makeRollingProductOperator, #makeRollingCountOperator, #makeRollingStdOperator and
    /// #makeRollingWAvgOperator
    pub(super) fn output_type(self, input: &ColumnType) -> Option<ColumnType> {
        use ColumnType::*;
        match self {
            Rolling::Sum => match input {
                Boolean | Byte | Char | Short | Int | Long => Some(Long),
                Float | Double => Some(Double),
                _ => None,
            },
            Rolling::Avg | Rolling::Std => match input {
                Boolean | Byte | Char | Short | Int | Long | Float | Double => Some(Double),
                _ => None,
            },
            // A Boolean is a `Comparable`, so Java makes its operator of objects for it,
            // which fails once it is given rows: see `evaluate`.
            Rolling::Min | Rolling::Max => match input {
                Byte | Char | Short | Int | Long | Instant | Float | Double | String | Boolean => {
                    Some(input.clone())
                }
                Vector(element) if super::orderable_vector(element) => Some(input.clone()),
                _ => None,
            },
            // An Instant column reaches ObjectRollingCountOperator as a chunk of longs and
            // the operation fails with a ClassCastException.
            Rolling::Count => (*input != Instant).then_some(Long),
            Rolling::Product | Rolling::WAvg => match input {
                Byte | Char | Short | Int | Long | Float | Double => Some(Double),
                _ => None,
            },
        }
    }

    /// The result for each slide of the window over `influencers`, the input values of the
    /// influencer rows in order. `weights` holds the type and the values of the weight
    /// column for the same rows, for the operator that has one.
    pub(super) fn evaluate(
        self,
        input: &ColumnType,
        influencers: &ArrayRef,
        weights: Option<(&ColumnType, &ArrayRef)>,
        slides: &[Slide],
    ) -> Result<ArrayRef, TableFailure> {
        let output = self
            .output_type(input)
            .ok_or_else(|| failure("no rolling operator for this column type"))?;
        if matches!(self, Rolling::Min | Rolling::Max) && *input == ColumnType::Boolean {
            // Behavior: io.deephaven.engine.table.impl.updateby.rollingminmax
            // .ComparableRollingMinMaxOperator.Context#setValueChunks, which casts the chunk
            // of bytes that a Boolean column is read as to a chunk of objects. It is called
            // for every chunk of rows, so a table without rows has the operation, and loses
            // it with its first row.
            return Err(failure(super::BOOLEAN_AS_OBJECTS));
        }
        let values = if self == Rolling::Count && *input == ColumnType::String {
            // Only nullness matters to a count.
            Values::Int(
                (0..influencers.len())
                    .map(|i| influencers.is_valid(i).then_some(0))
                    .collect(),
            )
        } else if matches!(self, Rolling::Min | Rolling::Max) {
            Values::read_ordered(influencers, input)?
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
        let doubles = |values: &Values| match values {
            Values::Int(v) => Ok(v.iter().map(|v| v.map(|v| v as f64)).collect()),
            Values::Float(v) => Ok(v.clone()),
            Values::Text(_) | Values::Lists(_) | Values::Ordered(_) => {
                Err(failure("no rolling operator for this column type"))
            }
        };
        Ok(match (self, &values) {
            (Rolling::Std, _) => double_array(stds(&doubles(&values)?, window)),
            (Rolling::WAvg, _) => {
                let (weight_type, weights) =
                    weights.ok_or_else(|| failure("a weighted average without weights"))?;
                let weights = doubles(&Values::read(weights, weight_type)?)?;
                double_array(weighted_averages(&doubles(&values)?, &weights, window))
            }
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
            (_, Values::Text(_) | Values::Lists(_) | Values::Ordered(_)) => {
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
        Self::with_capacity(Self::INITIAL_CAPACITY, identity, leaf, tree)
    }

    /// `capacity` is a power of two.
    fn with_capacity(
        capacity: usize,
        identity: T,
        leaf: fn(T, T) -> T,
        tree: fn(T, T) -> T,
    ) -> Self {
        PairwiseRing {
            slots: vec![identity; capacity],
            head: 0,
            tail: 0,
            identity,
            tree_nodes: vec![identity; capacity],
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
            .then(sum)
            .and_then(double_result)
    })
}

/// A sum that passes a null up the tree: the one function of the buffers of the rolling
/// average and weighted average.
fn add_slots(a: f64, b: f64) -> f64 {
    if a == NULL_DOUBLE {
        b
    } else if b == NULL_DOUBLE {
        a
    } else {
        a + b
    }
}

/// Behavior: java.lang.Math#ulp(double) for a finite value
fn ulp(value: f64) -> f64 {
    let magnitude = value.abs();
    if magnitude == f64::MAX {
        // 2^971, the distance to the value below.
        return f64::MAX - f64::from_bits(f64::MAX.to_bits() - 1);
    }
    f64::from_bits(magnitude.to_bits() + 1) - magnitude
}

/// Behavior: `BUFFER_INITIAL_CAPACITY` of the rolling standard deviation and weighted
/// average operators
const WIDE_INITIAL_CAPACITY: usize = 128;

/// The sample standard deviation of each window, from a pairwise sum of the values and one
/// of their squares. NaN for a window with one value, or with a sum that is not finite.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingstd.DoubleRollingStdOperator.Context
/// #push, #pop and #writeToOutputChunk, and the operators of the other primitive types, which
/// widen each value to `double` before they square it
fn stds(values: &[Option<f64>], window: Window) -> Vec<Option<f64>> {
    let ring = || PairwiseRing::with_capacity(WIDE_INITIAL_CAPACITY, 0.0, sum_slots, |a, b| a + b);
    let (mut sums, mut squares) = (ring(), ring());
    window
        .map(|w| {
            sums.pop(w.popped.len());
            squares.pop(w.popped.len());
            let pushed = &values[w.pushed.clone()];
            sums.push(pushed.iter().map(|v| raw_double(*v)));
            squares.push(pushed.iter().map(|v| v.map_or(NULL_DOUBLE, |v| v * v)));
            let count = w.end - w.start - w.nulls;
            if count == 0 {
                return None;
            }
            if count == 1 {
                return Some(f64::NAN);
            }
            // The buffers are evaluated only for a window with two values or more, which
            // matters to what their trees hold afterwards.
            let square_sum = squares.evaluate();
            let sum = sums.evaluate();
            if !square_sum.is_finite() || !sum.is_finite() {
                return Some(f64::NAN);
            }
            let count = count as f64;
            let eps = ulp(square_sum);
            let squared_mean = sum * (sum / count);
            let delta = square_sum - squared_mean;
            // Zero when the difference is within the rounding of the sum of squares.
            let variance = if (delta / eps).abs() > 1.0 {
                delta / (count - 1.0)
            } else {
                0.0
            };
            Some(variance.sqrt())
        })
        .collect()
}

/// The sum of weight times value over the sum of the weights, for the rows of each window
/// that have both. Neither sum is tested: weights that add up to zero give NaN or an
/// infinity.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingwavg.BasePrimitiveRollingWAvgOperator.Context
/// #pop and #writeToOutputChunk, with `push` of DoubleRollingWAvgOperator and the operators
/// of the other primitive types; the weights are widened by
/// io.deephaven.engine.table.impl.util.cast.ToDoubleCast
fn weighted_averages(
    values: &[Option<f64>],
    weights: &[Option<f64>],
    window: Window,
) -> Vec<Option<f64>> {
    let ring = || PairwiseRing::with_capacity(WIDE_INITIAL_CAPACITY, 0.0, add_slots, add_slots);
    let (mut products, mut weight_sums) = (ring(), ring());
    // Java counts a null when it pushes a row without a value or a weight, and takes one
    // off when it pops a product that reads as null, which a product of two values can.
    let mut reads_null = std::collections::VecDeque::new();
    let mut nulls = 0i64;
    window
        .map(|w| {
            for _ in w.popped.clone() {
                if reads_null.pop_front() == Some(true) {
                    nulls -= 1;
                }
            }
            products.pop(w.popped.len());
            weight_sums.pop(w.popped.len());
            let pushed: Vec<(f64, f64)> = w
                .pushed
                .clone()
                .map(|i| match (values[i], weights[i]) {
                    (Some(value), Some(weight)) => (weight * value, weight),
                    _ => {
                        nulls += 1;
                        (NULL_DOUBLE, NULL_DOUBLE)
                    }
                })
                .collect();
            reads_null.extend(pushed.iter().map(|(product, _)| *product == NULL_DOUBLE));
            products.push(pushed.iter().map(|(product, _)| *product));
            weight_sums.push(pushed.iter().map(|(_, weight)| *weight));
            if (w.end - w.start) as i64 == nulls {
                None
            } else {
                let product_sum = products.evaluate();
                double_result(product_sum / weight_sums.evaluate())
            }
        })
        .collect()
}

/// Behavior: io.deephaven.engine.table.impl.updateby.rollingavg.DoubleRollingAvgOperator.Context
/// #writeToOutputChunk and the one function of its buffer, which unlike the sum's passes a
/// null up the tree
fn avg_doubles(values: &[Option<f64>], window: Window) -> Vec<Option<f64>> {
    PairwiseRing::new(0.0, add_slots, add_slots).replay(values, window, raw_double, |w, sum| {
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

#[cfg(test)]
mod tests {
    use super::ulp;

    /// Behavior: java.lang.Math#ulp(double): the distance to the next value larger in
    /// magnitude, `Double.MIN_VALUE` for a zero, and 2^971 for `Double.MAX_VALUE`.
    #[test]
    fn ulp_is_the_distance_to_the_next_larger_magnitude() {
        assert_eq!(ulp(1.0), f64::EPSILON);
        assert_eq!(ulp(1.5), f64::EPSILON);
        assert_eq!(ulp(-4.0), 4.0 * f64::EPSILON);
        assert_eq!(ulp(0.0), f64::from_bits(1));
        assert_eq!(ulp(f64::MIN_POSITIVE), f64::from_bits(1));
        assert_eq!(ulp(f64::MAX), 2f64.powi(971));
        assert_eq!(ulp(-f64::MAX), 2f64.powi(971));
    }
}
