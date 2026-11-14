//! Count, sum and average: running state that retracts.
//!
//! The state is kept as Java keeps it, in the sentinel domain: a `long` equal to
//! `NULL_LONG` and a `double` equal to `NULL_DOUBLE` are null. That is what makes a sum
//! that wraps onto the sentinel read as null, and it is why these operators port the Java
//! arithmetic statement for statement instead of using `Option`.

use super::operator::{AggOperator, Effect};
use super::values::Run;
use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
use oxhaven_column::sentinel::{NULL_DOUBLE, NULL_FLOAT, NULL_LONG};
use oxhaven_contracts::schema::ColumnType;
use std::sync::Arc;

/// Behavior: io.deephaven.engine.util.NullSafeAddition#plusLong
fn plus_long(a: i64, b: i64) -> i64 {
    if a == NULL_LONG {
        b
    } else if b == NULL_LONG {
        a
    } else {
        a.wrapping_add(b)
    }
}

/// Behavior: io.deephaven.engine.util.NullSafeAddition#minusLong
fn minus_long(a: i64, b: i64) -> i64 {
    if a == NULL_LONG {
        b.wrapping_neg()
    } else if b == NULL_LONG {
        a
    } else {
        a.wrapping_sub(b)
    }
}

/// Behavior: io.deephaven.engine.util.NullSafeAddition#plusDouble
fn plus_double(a: f64, b: f64) -> f64 {
    if a == NULL_DOUBLE {
        b
    } else if b == NULL_DOUBLE {
        a
    } else {
        a + b
    }
}

fn grow<T: Clone>(values: &mut Vec<T>, slots: usize, fill: T) {
    if values.len() < slots {
        values.resize(slots, fill);
    }
}

fn long_results(values: &[i64], slots: &[usize]) -> ArrayRef {
    Arc::new(
        slots
            .iter()
            .map(|&slot| Some(values[slot]).filter(|&v| v != NULL_LONG))
            .collect::<Int64Array>(),
    )
}

fn double_results(values: &[f64], slots: &[usize]) -> ArrayRef {
    Arc::new(
        slots
            .iter()
            .map(|&slot| Some(values[slot]).filter(|&v| v != NULL_DOUBLE))
            .collect::<Float64Array>(),
    )
}

/// The wrapping sum of the non-null values of a run and how many there are.
///
/// Behavior: io.deephaven.engine.table.impl.by.SumIntChunk#sumIntChunk
fn sum_longs(run: &Run<'_>) -> (i64, i64) {
    run.longs().flatten().fold((0i64, 0i64), |(sum, count), v| {
        (sum.wrapping_add(v), count + 1)
    })
}

/// The number of rows in each group, null values included.
///
/// Behavior: io.deephaven.engine.table.impl.by.CountAggregationOperator
#[derive(Default)]
pub struct Count {
    counts: Vec<i64>,
}

impl AggOperator for Count {
    fn result_type(&self) -> ColumnType {
        ColumnType::Long
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.counts, slots, 0);
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.counts[slot] += run.len() as i64;
        Effect::Modified
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.counts[slot] -= run.len() as i64;
        Effect::Modified
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        Arc::new(
            slots
                .iter()
                .map(|&slot| self.counts[slot])
                .collect::<Int64Array>(),
        )
    }
}

/// The sum of a `byte`, `short`, `int`, `long` or `char` column, as a `long` that wraps.
/// Null when the group has no non-null value, and whenever the sum is `NULL_LONG`.
///
/// Behavior: io.deephaven.engine.table.impl.by.IntChunkedSumOperator, and the identical
/// Byte, Short, Long and Char operators.
#[derive(Default)]
pub struct LongSum {
    result: Vec<i64>,
    non_null: Vec<i64>,
}

impl LongSum {
    /// Behavior: IntChunkedSumOperator#updateInternal(long, long, int)
    fn update(&mut self, slot: usize, sum: i64, non_null: i64) -> Effect {
        if non_null > 0 {
            self.non_null[slot] += non_null;
            self.result[slot] = plus_long(self.result[slot], sum);
            Effect::Modified
        } else if non_null < 0 {
            self.non_null[slot] += non_null;
            self.result[slot] = if self.non_null[slot] > 0 {
                plus_long(self.result[slot], sum)
            } else {
                NULL_LONG
            };
            Effect::Modified
        } else {
            Effect::Unchanged
        }
    }
}

impl AggOperator for LongSum {
    fn result_type(&self) -> ColumnType {
        ColumnType::Long
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.result, slots, NULL_LONG);
        grow(&mut self.non_null, slots, 0);
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (sum, non_null) = sum_longs(run);
        self.update(slot, sum, non_null)
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (sum, non_null) = sum_longs(run);
        self.update(slot, sum.wrapping_neg(), -non_null)
    }

    /// Behavior: IntChunkedSumOperator#updateInternal(long, long, int, int)
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let (old_sum, old_non_null) = sum_longs(old);
        let (new_sum, new_non_null) = sum_longs(new);
        let difference = new_sum.wrapping_sub(old_sum);
        if old_non_null == 0 && new_non_null == 0 {
            return Effect::Unchanged;
        }
        let change = new_non_null - old_non_null;
        if change == 0 {
            if difference == 0 {
                return Effect::Unchanged;
            }
            self.result[slot] = plus_long(self.result[slot], difference);
        } else {
            self.non_null[slot] += change;
            self.result[slot] = if self.non_null[slot] > 0 {
                plus_long(self.result[slot], difference)
            } else {
                NULL_LONG
            };
        }
        Effect::Modified
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        long_results(&self.result, slots)
    }
}

/// The number of `true` values of a Boolean column. Null when the group has no non-null
/// value.
///
/// Behavior: io.deephaven.engine.table.impl.by.BooleanChunkedSumOperator
#[derive(Default)]
pub struct BooleanSum {
    /// The number of `true` values.
    result: Vec<i64>,
    false_count: Vec<i64>,
}

fn count_bools(run: &Run<'_>) -> (i64, i64) {
    run.bools()
        .flatten()
        .fold((0, 0), |(trues, falses), v| match v {
            true => (trues + 1, falses),
            false => (trues, falses + 1),
        })
}

/// Behavior: BooleanChunkedSumOperator#getAndAdd
fn get_and_add(values: &mut [i64], slot: usize, count: i64) -> i64 {
    let old = values[slot];
    if old == NULL_LONG || old == 0 {
        values[slot] = count;
        0
    } else {
        values[slot] = old + count;
        old
    }
}

/// Behavior: BooleanChunkedSumOperator#addAndGet
fn add_and_get(values: &mut [i64], slot: usize, count: i64) -> i64 {
    let old = values[slot];
    let new = if old == NULL_LONG || old == 0 {
        count
    } else {
        old + count
    };
    values[slot] = new;
    new
}

impl BooleanSum {
    /// The `false` count to judge emptiness by: the one just written, or the stored one.
    fn false_count_or(&self, slot: usize, written: Option<i64>) -> i64 {
        written.unwrap_or(self.false_count[slot])
    }
}

impl AggOperator for BooleanSum {
    fn result_type(&self) -> ColumnType {
        ColumnType::Long
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.result, slots, NULL_LONG);
        grow(&mut self.false_count, slots, NULL_LONG);
    }

    /// Behavior: BooleanChunkedSumOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (trues, falses) = count_bools(run);
        let mut modified = false;
        if trues > 0 {
            get_and_add(&mut self.result, slot, trues);
            modified = true;
        }
        if falses > 0 {
            let old_false = get_and_add(&mut self.false_count, slot, falses);
            let has_true = self.result[slot] != NULL_LONG && self.result[slot] > 0;
            if old_false == 0 && trues == 0 && !has_true {
                self.result[slot] = 0;
                modified = true;
            }
        }
        Effect::modified_if(modified)
    }

    /// Behavior: BooleanChunkedSumOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (trues, falses) = count_bools(run);
        let mut new_false = None;
        if falses > 0 {
            new_false = Some(add_and_get(&mut self.false_count, slot, -falses));
        }
        if trues > 0 {
            let mut new_true = self.result[slot] - trues;
            if new_true == 0 {
                let falses_left = self.false_count_or(slot, new_false);
                if falses_left == NULL_LONG || falses_left == 0 {
                    new_true = NULL_LONG;
                }
            }
            self.result[slot] = new_true;
            return Effect::Modified;
        }
        if new_false == Some(0) && self.result[slot] == 0 {
            self.result[slot] = NULL_LONG;
            return Effect::Modified;
        }
        Effect::Unchanged
    }

    /// Behavior: BooleanChunkedSumOperator#modifyChunk
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let (old_trues, old_falses) = count_bools(old);
        let (new_trues, new_falses) = count_bools(new);
        let true_modified = old_trues != new_trues;
        let false_modified = old_falses != new_falses;
        if !true_modified && !false_modified {
            return Effect::Unchanged;
        }
        let mut new_false = None;
        if false_modified {
            new_false = Some(add_and_get(
                &mut self.false_count,
                slot,
                new_falses - old_falses,
            ));
        }
        if true_modified {
            let mut new_true = plus_long(self.result[slot], new_trues - old_trues);
            if new_true == 0 {
                let falses_left = self.false_count_or(slot, new_false);
                if falses_left == NULL_LONG || falses_left == 0 {
                    new_true = NULL_LONG;
                }
            }
            self.result[slot] = new_true;
            return Effect::Modified;
        }
        let existing = self.result[slot];
        if new_false == Some(0) && existing == 0 {
            self.result[slot] = NULL_LONG;
            return Effect::Modified;
        }
        if new_false.is_some_and(|count| count > 0) && existing == NULL_LONG {
            self.result[slot] = 0;
            return Effect::Modified;
        }
        Effect::Unchanged
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        long_results(&self.result, slots)
    }
}

/// What a run of floating point values holds besides nulls.
#[derive(Clone, Copy, Default)]
struct FloatRun {
    /// The sum of the values that are neither NaN nor infinite, added in row order.
    sum: f64,
    normal: i64,
    nan: i64,
    positive_infinity: i64,
    negative_infinity: i64,
}

impl FloatRun {
    /// Behavior: io.deephaven.engine.table.impl.by.SumDoubleChunk#sumDoubleChunk
    fn of(run: &Run<'_>) -> FloatRun {
        let mut result = FloatRun::default();
        for value in run.doubles().flatten() {
            if value.is_nan() {
                result.nan += 1;
            } else if value == f64::INFINITY {
                result.positive_infinity += 1;
            } else if value == f64::NEG_INFINITY {
                result.negative_infinity += 1;
            } else {
                result.sum += value;
                result.normal += 1;
            }
        }
        result
    }

    fn has_non_normal(&self) -> bool {
        self.nan > 0 || self.positive_infinity > 0 || self.negative_infinity > 0
    }
}

/// The per-group counts every floating point operator keeps.
#[derive(Default)]
struct FloatCounts {
    normal: Vec<i64>,
    nan: Vec<i64>,
    positive_infinity: Vec<i64>,
    negative_infinity: Vec<i64>,
}

impl FloatCounts {
    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.normal, slots, 0);
        grow(&mut self.nan, slots, 0);
        grow(&mut self.positive_infinity, slots, 0);
        grow(&mut self.negative_infinity, slots, 0);
    }

    /// Adds `sign` times the counts of `run`, except the normal count.
    fn update_non_normal(&mut self, slot: usize, run: &FloatRun, sign: i64) {
        self.nan[slot] += sign * run.nan;
        self.positive_infinity[slot] += sign * run.positive_infinity;
        self.negative_infinity[slot] += sign * run.negative_infinity;
    }

    /// The result that NaN and infinite values force, if they do.
    fn forced(&self, slot: usize, positive_first: bool) -> Option<f64> {
        let positive = self.positive_infinity[slot] > 0;
        let negative = self.negative_infinity[slot] > 0;
        if self.nan[slot] > 0 || (positive && negative) {
            Some(f64::NAN)
        } else if positive_first && positive {
            Some(f64::INFINITY)
        } else if negative {
            Some(f64::NEG_INFINITY)
        } else if positive {
            Some(f64::INFINITY)
        } else {
            None
        }
    }
}

/// The sum of a `float` or `double` column, of the column's own type. A NaN makes it NaN,
/// as do infinities of both signs; one sign of infinity makes it that infinity. Null when
/// the group has no non-null value.
///
/// The running sum is a `double` that values are added to and subtracted from as rows
/// come and go, so the result of a ticking table depends on its update history exactly as
/// it does in Java. For a `float` column the result is the running sum narrowed.
///
/// Behavior: io.deephaven.engine.table.impl.by.DoubleChunkedSumOperator and
/// io.deephaven.engine.table.impl.by.FloatChunkedSumOperator
pub struct FloatSum {
    narrow: bool,
    counts: FloatCounts,
    running: Vec<f64>,
    /// The result, with `self.null()` for null.
    result: Vec<f64>,
}

impl FloatSum {
    /// A sum of a `float` column if `narrow`, else of a `double` column.
    pub fn new(narrow: bool) -> FloatSum {
        FloatSum {
            narrow,
            counts: FloatCounts::default(),
            running: Vec::new(),
            result: Vec::new(),
        }
    }

    /// The null of the result type: `NULL_FLOAT` or `NULL_DOUBLE`. A `float` sum that
    /// narrows onto `NULL_FLOAT` reads as null, as in Java.
    fn null(&self) -> f64 {
        if self.narrow {
            f64::from(NULL_FLOAT)
        } else {
            NULL_DOUBLE
        }
    }

    fn typed(&self, sum: f64) -> f64 {
        if self.narrow {
            f64::from(sum as f32)
        } else {
            sum
        }
    }

    /// Behavior: DoubleChunkedSumOperator#currentValueWithSum
    fn value_with_sum(&self, slot: usize, sum: f64) -> f64 {
        match self.counts.forced(slot, false) {
            Some(forced) => forced,
            None if self.counts.normal[slot] == 0 => self.null(),
            None => self.typed(sum),
        }
    }

    /// Behavior: DoubleChunkedSumOperator#currentValueNoSum
    fn value_no_sum(&self, slot: usize) -> Option<f64> {
        self.counts.forced(slot, false)
    }
}

impl AggOperator for FloatSum {
    fn result_type(&self) -> ColumnType {
        if self.narrow {
            ColumnType::Float
        } else {
            ColumnType::Double
        }
    }

    fn ensure_slots(&mut self, slots: usize) {
        self.counts.ensure_slots(slots);
        grow(&mut self.running, slots, NULL_DOUBLE);
        let null = self.null();
        grow(&mut self.result, slots, null);
    }

    /// Behavior: DoubleChunkedSumOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let chunk = FloatRun::of(run);
        self.counts.update_non_normal(slot, &chunk, 1);
        if chunk.normal > 0 {
            self.counts.normal[slot] += chunk.normal;
            let sum = plus_double(self.running[slot], chunk.sum);
            self.running[slot] = sum;
            self.result[slot] = self.value_with_sum(slot, sum);
        } else if chunk.has_non_normal() {
            if let Some(forced) = self.value_no_sum(slot) {
                self.result[slot] = forced;
            } else {
                self.result[slot] = self.null();
            }
        }
        Effect::Modified
    }

    /// Behavior: DoubleChunkedSumOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let chunk = FloatRun::of(run);
        self.counts.update_non_normal(slot, &chunk, -1);
        if chunk.normal > 0 {
            self.counts.normal[slot] -= chunk.normal;
            let sum = if self.counts.normal[slot] == 0 {
                0.0
            } else {
                plus_double(self.running[slot], -chunk.sum)
            };
            self.running[slot] = sum;
            self.result[slot] = self.value_with_sum(slot, sum);
            Effect::Modified
        } else if chunk.has_non_normal() {
            self.result[slot] = match self.value_no_sum(slot) {
                Some(forced) => forced,
                None if self.counts.normal[slot] == 0 => self.null(),
                None => self.typed(self.running[slot]),
            };
            Effect::Modified
        } else {
            Effect::Unchanged
        }
    }

    /// Behavior: DoubleChunkedSumOperator#modifyChunk
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let pre = FloatRun::of(old);
        let post = FloatRun::of(new);
        let non_normal_change = pre.nan != post.nan
            || pre.positive_infinity != post.positive_infinity
            || pre.negative_infinity != post.negative_infinity;
        let unchanged_result = !non_normal_change && post.sum == pre.sum;
        let normal_before = self.counts.normal[slot];
        let mut single_value_or_empty = false;
        if pre.normal != post.normal {
            self.counts.normal[slot] += post.normal - pre.normal;
            if self.counts.normal[slot] == 0 || normal_before == 0 {
                single_value_or_empty = true;
            }
        } else if unchanged_result {
            return Effect::Unchanged;
        }
        let total = self.counts.normal[slot];
        self.counts.update_non_normal(slot, &pre, -1);
        self.counts.update_non_normal(slot, &post, 1);
        let possible = self.value_no_sum(slot);
        let sum = if pre.sum != post.sum || single_value_or_empty {
            let sum = if total == 0 {
                0.0
            } else if normal_before == 0 {
                post.sum
            } else {
                plus_double(self.running[slot], post.sum - pre.sum)
            };
            self.running[slot] = sum;
            sum
        } else if possible.is_some() {
            f64::NAN
        } else {
            self.running[slot]
        };
        let value = match possible {
            Some(forced) => forced,
            None if total == 0 => self.null(),
            None => self.typed(sum),
        };
        let old_value = std::mem::replace(&mut self.result[slot], value);
        // io.deephaven.util.compare.DoubleComparisons#eq: equal, or both NaN.
        Effect::modified_if(!(old_value == value || (old_value.is_nan() && value.is_nan())))
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        let null = self.null();
        let values = slots
            .iter()
            .map(|&slot| Some(self.result[slot]).filter(|&v| v != null));
        if self.narrow {
            Arc::new(values.map(|v| v.map(|v| v as f32)).collect::<Float32Array>())
        } else {
            Arc::new(values.collect::<Float64Array>())
        }
    }
}

/// The mean of the non-null values of a `byte`, `short`, `int`, `long` or `char` column,
/// as a `double`: the wrapping `long` sum divided by the count. Null when the group has no
/// non-null value.
///
/// Behavior: io.deephaven.engine.table.impl.by.IntChunkedAvgOperator, and the identical
/// Byte, Short, Long and Char operators.
#[derive(Default)]
pub struct LongAvg {
    running: Vec<i64>,
    non_null: Vec<i64>,
    result: Vec<f64>,
}

impl AggOperator for LongAvg {
    fn result_type(&self) -> ColumnType {
        ColumnType::Double
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.running, slots, NULL_LONG);
        grow(&mut self.non_null, slots, 0);
        grow(&mut self.result, slots, NULL_DOUBLE);
    }

    /// Behavior: IntChunkedAvgOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (sum, non_null) = sum_longs(run);
        if non_null > 0 {
            self.non_null[slot] += non_null;
            let sum = plus_long(self.running[slot], sum);
            self.running[slot] = sum;
            self.result[slot] = sum as f64 / self.non_null[slot] as f64;
            Effect::Modified
        } else if self.non_null[slot] == 0 {
            self.result[slot] = NULL_DOUBLE;
            Effect::Modified
        } else {
            Effect::Unchanged
        }
    }

    /// Behavior: IntChunkedAvgOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (sum, non_null) = sum_longs(run);
        if non_null == 0 {
            return Effect::Unchanged;
        }
        self.non_null[slot] -= non_null;
        let sum = minus_long(self.running[slot], sum);
        self.running[slot] = sum;
        self.result[slot] = if self.non_null[slot] == 0 {
            NULL_DOUBLE
        } else {
            sum as f64 / self.non_null[slot] as f64
        };
        Effect::Modified
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        double_results(&self.result, slots)
    }
}

/// The mean of the non-null values of a `float` or `double` column, as a `double`. A NaN
/// makes it NaN, as do infinities of both signs; one sign of infinity makes it that
/// infinity. Null when the group has no non-null value.
///
/// As with [`FloatSum`], the running sum depends on the update history. Unlike it, the
/// running sum is not reset when the last normal value leaves, so what is left of it after
/// rounding stays in the group.
///
/// Behavior: io.deephaven.engine.table.impl.by.DoubleChunkedAvgOperator and
/// io.deephaven.engine.table.impl.by.FloatChunkedAvgOperator
#[derive(Default)]
pub struct FloatAvg {
    counts: FloatCounts,
    running: Vec<f64>,
    result: Vec<f64>,
}

impl FloatAvg {
    /// Behavior: DoubleChunkedAvgOperator#updateResultWithNewSum and
    /// #updateResultSumUnchanged
    fn update_result(&mut self, slot: usize) {
        self.result[slot] = match self.counts.forced(slot, true) {
            Some(forced) => forced,
            None if self.counts.normal[slot] == 0 => NULL_DOUBLE,
            None => self.running[slot] / self.counts.normal[slot] as f64,
        };
    }
}

impl AggOperator for FloatAvg {
    fn result_type(&self) -> ColumnType {
        ColumnType::Double
    }

    fn ensure_slots(&mut self, slots: usize) {
        self.counts.ensure_slots(slots);
        grow(&mut self.running, slots, NULL_DOUBLE);
        grow(&mut self.result, slots, NULL_DOUBLE);
    }

    /// Behavior: DoubleChunkedAvgOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let chunk = FloatRun::of(run);
        self.counts.normal[slot] += chunk.normal;
        self.counts.update_non_normal(slot, &chunk, 1);
        if chunk.normal > 0 {
            self.running[slot] = plus_double(self.running[slot], chunk.sum);
        }
        self.update_result(slot);
        Effect::Modified
    }

    /// Behavior: DoubleChunkedAvgOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let chunk = FloatRun::of(run);
        if chunk.normal == 0 && !chunk.has_non_normal() {
            return Effect::Unchanged;
        }
        self.counts.normal[slot] -= chunk.normal;
        self.counts.update_non_normal(slot, &chunk, -1);
        if chunk.normal > 0 {
            self.running[slot] = plus_double(self.running[slot], -chunk.sum);
        }
        self.update_result(slot);
        Effect::Modified
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        double_results(&self.result, slots)
    }
}
