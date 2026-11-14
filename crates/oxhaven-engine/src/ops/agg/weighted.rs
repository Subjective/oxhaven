//! Weighted average and weighted sum, from running sums that retract as rows leave.
//!
//! Each operator reads its input column and a weight column from the same rows (see
//! [`Run::with_weights`]). As in `sum`, the state is kept in Java's sentinel domain and the
//! arithmetic is ported statement for statement, quirks included.

use super::operator::{AggOperator, Effect};
use super::values::Run;
use arrow::array::{ArrayRef, Float64Array, Int64Array};
use oxhaven_column::sentinel::{NULL_DOUBLE, NULL_LONG};
use oxhaven_contracts::schema::ColumnType;
use std::sync::Arc;

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

fn grow<T: Clone>(values: &mut Vec<T>, slots: usize, fill: T) {
    if values.len() < slots {
        values.resize(slots, fill);
    }
}

/// What a run of (value, weight) pairs holds. A pair with a NaN or an infinity on either
/// side counts as NaN, even if the other side is null; otherwise a pair with a null is
/// skipped.
#[derive(Clone, Copy, Default)]
struct Sums {
    nan: i64,
    normal: i64,
    sum_of_weights: f64,
    weighted_sum: f64,
}

impl Sums {
    /// Behavior: io.deephaven.engine.table.impl.by.ChunkedWeightedAverageOperator#sumChunks
    /// and io.deephaven.engine.table.impl.by.DoubleChunkedWeightedSumOperator#sumChunks
    fn of(run: &Run<'_>) -> Sums {
        let mut sums = Sums::default();
        for (component, weight) in run.weighted_doubles() {
            let non_normal = |v: Option<f64>| v.is_some_and(|v| v.is_nan() || v.is_infinite());
            if non_normal(weight) || non_normal(component) {
                sums.nan += 1;
                continue;
            }
            let (Some(component), Some(weight)) = (component, weight) else {
                continue;
            };
            sums.normal += 1;
            sums.sum_of_weights += weight;
            sums.weighted_sum += weight * component;
        }
        sums
    }
}

/// The weighted average `sum(w * x) / sum(w)`, or with `!average` the weighted sum
/// `sum(w * x)`, of the rows where neither the value `x` nor the weight `w` is null, as a
/// `double`. A NaN or infinite value or weight makes it NaN. Null when no row has both.
///
/// Behavior: io.deephaven.engine.table.impl.by.ChunkedWeightedAverageOperator, and
/// io.deephaven.engine.table.impl.by.DoubleChunkedWeightedSumOperator for the sum (used
/// unless both columns are integers)
pub struct WeightedFloat {
    average: bool,
    nan: Vec<i64>,
    normal: Vec<i64>,
    sum_of_weights: Vec<f64>,
    weighted_sum: Vec<f64>,
    result: Vec<f64>,
}

impl WeightedFloat {
    pub fn new(average: bool) -> WeightedFloat {
        WeightedFloat {
            average,
            nan: Vec::new(),
            normal: Vec::new(),
            sum_of_weights: Vec::new(),
            weighted_sum: Vec::new(),
            result: Vec::new(),
        }
    }

    fn ratio(&self, weighted_sum: f64, sum_of_weights: f64) -> f64 {
        if self.average {
            weighted_sum / sum_of_weights
        } else {
            weighted_sum
        }
    }

    fn set_result(&mut self, slot: usize, value: f64) -> Effect {
        let old = std::mem::replace(&mut self.result[slot], value);
        // `newResult != existingResult`: a NaN is always a change.
        Effect::modified_if(old != value)
    }
}

impl AggOperator for WeightedFloat {
    fn result_type(&self) -> ColumnType {
        ColumnType::Double
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.nan, slots, 0);
        grow(&mut self.normal, slots, 0);
        grow(&mut self.sum_of_weights, slots, NULL_DOUBLE);
        grow(&mut self.weighted_sum, slots, NULL_DOUBLE);
        grow(&mut self.result, slots, NULL_DOUBLE);
    }

    /// Behavior: ChunkedWeightedAverageOperator#addChunk(DoubleChunk, DoubleChunk, int, int, long),
    /// DoubleChunkedWeightedSumOperator#addChunk(DoubleChunk, DoubleChunk, int, int, long)
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let new = Sums::of(run);
        self.nan[slot] += new.nan;
        let total_nans = self.nan[slot];
        self.normal[slot] += new.normal;
        let total_normal = self.normal[slot];
        if total_normal > 0 {
            let weighted_sum = plus_double(self.weighted_sum[slot], new.weighted_sum);
            let sum_of_weights = plus_double(self.sum_of_weights[slot], new.sum_of_weights);
            if new.normal > 0 {
                self.weighted_sum[slot] = weighted_sum;
                self.sum_of_weights[slot] = sum_of_weights;
            }
            if total_nans > 0 {
                if new.nan == total_nans {
                    self.result[slot] = f64::NAN;
                    return Effect::MODIFIED;
                }
                return Effect::UNCHANGED;
            }
            let value = self.ratio(weighted_sum, sum_of_weights);
            return self.set_result(slot, value);
        }
        if total_nans > 0 && total_nans == new.nan {
            self.result[slot] = f64::NAN;
            return Effect::MODIFIED;
        }
        Effect::UNCHANGED
    }

    /// Behavior: ChunkedWeightedAverageOperator#removeChunk(DoubleChunk, DoubleChunk, int, int, long),
    /// DoubleChunkedWeightedSumOperator#removeChunk(DoubleChunk, DoubleChunk, int, int, long)
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let old = Sums::of(run);
        self.nan[slot] -= old.nan;
        let total_nans = self.nan[slot];
        self.normal[slot] -= old.normal;
        let total_normal = self.normal[slot];
        let (weighted_sum, sum_of_weights) = if old.normal > 0 {
            let sums = if total_normal == 0 {
                (0.0, 0.0)
            } else {
                (
                    self.weighted_sum[slot] - old.weighted_sum,
                    self.sum_of_weights[slot] - old.sum_of_weights,
                )
            };
            self.weighted_sum[slot] = sums.0;
            self.sum_of_weights[slot] = sums.1;
            sums
        } else {
            (self.weighted_sum[slot], self.sum_of_weights[slot])
        };
        if total_nans > 0 {
            Effect::UNCHANGED
        } else if total_normal == 0 {
            if old.nan > 0 || old.normal > 0 {
                self.result[slot] = NULL_DOUBLE;
                Effect::MODIFIED
            } else {
                Effect::UNCHANGED
            }
        } else {
            let value = self.ratio(weighted_sum, sum_of_weights);
            self.set_result(slot, value)
        }
    }

    /// Behavior: ChunkedWeightedAverageOperator#modifyChunk(DoubleChunk, DoubleChunk, DoubleChunk, DoubleChunk, int, int, long),
    /// DoubleChunkedWeightedSumOperator#modifyChunk(DoubleChunk, DoubleChunk, DoubleChunk, DoubleChunk, int, int, long)
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let prev = Sums::of(old);
        let next = Sums::of(new);
        self.nan[slot] += next.nan - prev.nan;
        let total_nans = self.nan[slot];
        self.normal[slot] += next.normal - prev.normal;
        let total_normal = self.normal[slot];
        if total_normal > 0 {
            let existing_weighted_sum = self.weighted_sum[slot];
            let weighted_sum =
                plus_double(existing_weighted_sum, next.weighted_sum - prev.weighted_sum);
            if weighted_sum != existing_weighted_sum {
                self.weighted_sum[slot] = weighted_sum;
            }
            let sum_of_weights = plus_double(
                self.sum_of_weights[slot],
                next.sum_of_weights - prev.sum_of_weights,
            );
            // Java compares the new sum of weights with the old weighted sum here, and
            // keeps the stale sum of weights when they are equal.
            if sum_of_weights != existing_weighted_sum {
                self.sum_of_weights[slot] = sum_of_weights;
            }
            if total_nans > 0 {
                self.result[slot] = f64::NAN;
                return Effect::modified_if(prev.nan == 0);
            }
            // The ratio is of the local sums, not of what was stored.
            let value = self.ratio(weighted_sum, sum_of_weights);
            return self.set_result(slot, value);
        }
        if prev.normal > 0 {
            self.weighted_sum[slot] = 0.0;
            if self.average {
                self.sum_of_weights[slot] = 0.0;
            }
        }
        if total_nans == 0 {
            if prev.nan > 0 || prev.normal > 0 {
                self.result[slot] = NULL_DOUBLE;
                return Effect::MODIFIED;
            }
            return Effect::UNCHANGED;
        }
        if prev.nan == 0 {
            self.result[slot] = f64::NAN;
            return Effect::MODIFIED;
        }
        Effect::UNCHANGED
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        Arc::new(
            slots
                .iter()
                .map(|&slot| Some(self.result[slot]).filter(|&v| v != NULL_DOUBLE))
                .collect::<Float64Array>(),
        )
    }
}

/// The weighted sum `sum(w * x)` of an integer column weighted by an integer column, as a
/// `long` that wraps, over the rows where neither is null. Null when no row has both, and
/// whenever the sum is `NULL_LONG`.
///
/// Behavior: io.deephaven.engine.table.impl.by.LongChunkedWeightedSumOperator
#[derive(Default)]
pub struct WeightedLongSum {
    normal: Vec<i64>,
    weighted_sum: Vec<i64>,
    result: Vec<i64>,
}

/// Behavior: LongChunkedWeightedSumOperator#sumChunks
fn long_sums(run: &Run<'_>) -> (i64, i64) {
    run.weighted_longs()
        .fold((0, 0), |(normal, sum), pair| match pair {
            (Some(component), Some(weight)) => {
                (normal + 1, sum.wrapping_add(weight.wrapping_mul(component)))
            }
            _ => (normal, sum),
        })
}

impl WeightedLongSum {
    fn set_result(&mut self, slot: usize, value: i64) -> Effect {
        let old = std::mem::replace(&mut self.result[slot], value);
        Effect::modified_if(old != value)
    }
}

impl AggOperator for WeightedLongSum {
    fn result_type(&self) -> ColumnType {
        ColumnType::Long
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.normal, slots, 0);
        grow(&mut self.weighted_sum, slots, NULL_LONG);
        grow(&mut self.result, slots, NULL_LONG);
    }

    /// Behavior: LongChunkedWeightedSumOperator#addChunk(LongChunk, LongChunk, int, int, long)
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (normal, sum) = long_sums(run);
        self.normal[slot] += normal;
        if self.normal[slot] > 0 {
            let total = plus_long(self.weighted_sum[slot], sum);
            if normal > 0 {
                self.weighted_sum[slot] = total;
            }
            return self.set_result(slot, total);
        }
        Effect::UNCHANGED
    }

    /// Behavior: LongChunkedWeightedSumOperator#removeChunk(LongChunk, LongChunk, int, int, long)
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let (normal, sum) = long_sums(run);
        self.normal[slot] -= normal;
        let total_normal = self.normal[slot];
        let total = if normal > 0 {
            let total = if total_normal == 0 {
                0
            } else {
                self.weighted_sum[slot].wrapping_sub(sum)
            };
            self.weighted_sum[slot] = total;
            total
        } else {
            self.weighted_sum[slot]
        };
        if total_normal == 0 {
            if normal > 0 {
                self.result[slot] = NULL_LONG;
                return Effect::MODIFIED;
            }
            return Effect::UNCHANGED;
        }
        self.set_result(slot, total)
    }

    /// Behavior: LongChunkedWeightedSumOperator#modifyChunk(LongChunk, LongChunk, LongChunk, LongChunk, int, int, long)
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let (prev_normal, prev_sum) = long_sums(old);
        let (new_normal, new_sum) = long_sums(new);
        self.normal[slot] += new_normal - prev_normal;
        if self.normal[slot] > 0 {
            let total = plus_long(self.weighted_sum[slot], new_sum.wrapping_sub(prev_sum));
            self.weighted_sum[slot] = total;
            return self.set_result(slot, total);
        }
        if prev_normal > 0 {
            self.weighted_sum[slot] = 0;
            self.result[slot] = NULL_LONG;
            return Effect::MODIFIED;
        }
        Effect::UNCHANGED
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        Arc::new(
            slots
                .iter()
                .map(|&slot| Some(self.result[slot]).filter(|&v| v != NULL_LONG))
                .collect::<Int64Array>(),
        )
    }
}
