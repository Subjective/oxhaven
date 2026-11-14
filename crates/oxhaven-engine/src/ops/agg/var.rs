//! Sample variance and standard deviation, from running sums of the values and of their
//! squares that retract as rows leave.
//!
//! As in `sum`, the state is kept in Java's sentinel domain and the arithmetic is ported
//! statement for statement, so that a ticking result has the rounding history Java's has.

use super::operator::{AggOperator, Effect};
use super::values::Run;
use arrow::array::{ArrayRef, Float64Array};
use oxhaven_column::sentinel::NULL_DOUBLE;
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

fn grow<T: Clone>(values: &mut Vec<T>, slots: usize, fill: T) {
    if values.len() < slots {
        values.resize(slots, fill);
    }
}

/// What a run holds besides nulls: the sum of its normal values and of their squares, how
/// many there are, and the counts of NaN and infinite values. An integer column has only
/// normal values.
#[derive(Clone, Copy, Default)]
struct Moments {
    sum: f64,
    sum2: f64,
    normal: i64,
    nan: i64,
    positive_infinity: i64,
    negative_infinity: i64,
}

impl Moments {
    /// Behavior: io.deephaven.engine.table.impl.by.SumIntChunk#sum2IntChunk and
    /// io.deephaven.engine.table.impl.by.SumDoubleChunk#sum2DoubleChunk
    fn of(run: &Run<'_>, floating: bool) -> Moments {
        let mut moments = Moments::default();
        let values: Box<dyn Iterator<Item = f64>> = if floating {
            Box::new(run.doubles().flatten())
        } else {
            Box::new(run.longs().flatten().map(|v| v as f64))
        };
        for value in values {
            if value.is_nan() {
                moments.nan += 1;
            } else if value == f64::INFINITY {
                moments.positive_infinity += 1;
            } else if value == f64::NEG_INFINITY {
                moments.negative_infinity += 1;
            } else {
                moments.sum += value;
                moments.sum2 += value * value;
                moments.normal += 1;
            }
        }
        moments
    }

    fn non_normal(&self) -> i64 {
        self.nan + self.positive_infinity + self.negative_infinity
    }
}

/// The sample variance (or, with `std`, its square root) of the non-null values of a numeric
/// column, as a `double`: `(sum2 - sum * sum / n) / (n - 1)`. Null without a non-null value,
/// NaN with exactly one. For a `float` or `double` column a NaN or an infinity makes it NaN,
/// so does a running sum that has become infinite, and a variance within rounding of zero,
/// or below it, is zero.
///
/// Behavior: io.deephaven.engine.table.impl.by.IntChunkedVarOperator (and the Byte, Short,
/// Long and Char copies) for integer columns,
/// io.deephaven.engine.table.impl.by.DoubleChunkedVarOperator and
/// io.deephaven.engine.table.impl.by.FloatChunkedVarOperator for floating point ones
pub struct Var {
    std: bool,
    floating: bool,
    normal: Vec<i64>,
    nan: Vec<i64>,
    positive_infinity: Vec<i64>,
    negative_infinity: Vec<i64>,
    sum: Vec<f64>,
    sum2: Vec<f64>,
    result: Vec<f64>,
}

impl Var {
    /// `floating` for a `float` or `double` column.
    pub fn new(std: bool, floating: bool) -> Var {
        Var {
            std,
            floating,
            normal: Vec::new(),
            nan: Vec::new(),
            positive_infinity: Vec::new(),
            negative_infinity: Vec::new(),
            sum: Vec::new(),
            sum2: Vec::new(),
            result: Vec::new(),
        }
    }

    fn finish(&self, variance: f64) -> f64 {
        if self.std {
            variance.sqrt()
        } else {
            variance
        }
    }

    /// Behavior: IntChunkedVarOperator#addChunk / #removeChunk (the integer formula)
    fn integer_variance(&self, count: i64, sum: f64, sum2: f64) -> f64 {
        let n = count as f64;
        self.finish((sum2 - (sum * sum / n)) / (n - 1.0))
    }

    /// Behavior: DoubleChunkedVarOperator#computeVariance, and the check before it for a
    /// running sum that has become infinite
    fn floating_variance(&self, count: i64, sum: f64, sum2: f64) -> f64 {
        if sum.is_infinite() || sum2.is_infinite() {
            return f64::NAN;
        }
        let eps = ulp(sum2);
        let n = count as f64;
        let vs2bar = sum * (sum / n);
        let delta = sum2 - vs2bar;
        let rel_eps = delta / eps;
        let variance = if rel_eps.abs() > 1.0 {
            delta / (n - 1.0)
        } else {
            0.0
        };
        // `Math.max(variance, 0.0)`: NaN stays NaN, and -0.0 is below 0.0, so either zero
        // gives 0.0.
        let variance = if variance.is_nan() || variance > 0.0 {
            variance
        } else {
            0.0
        };
        self.finish(variance)
    }

    fn variance(&self, count: i64, sum: f64, sum2: f64) -> f64 {
        if self.floating {
            self.floating_variance(count, sum, sum2)
        } else {
            self.integer_variance(count, sum, sum2)
        }
    }

    fn forces_nan(&self, slot: usize) -> bool {
        self.nan[slot] > 0 || self.positive_infinity[slot] > 0 || self.negative_infinity[slot] > 0
    }

    fn count_non_normal(&mut self, slot: usize, moments: &Moments, sign: i64) {
        self.nan[slot] += sign * moments.nan;
        self.positive_infinity[slot] += sign * moments.positive_infinity;
        self.negative_infinity[slot] += sign * moments.negative_infinity;
    }
}

/// `Math.ulp(double)`: the distance to the next larger magnitude.
fn ulp(value: f64) -> f64 {
    if value.is_nan() {
        return f64::NAN;
    }
    if value.is_infinite() {
        return f64::INFINITY;
    }
    let magnitude = value.abs();
    if magnitude == f64::MAX {
        // Math.ulp(MAX_VALUE) is 2^971.
        return 2f64.powi(971);
    }
    f64::from_bits(magnitude.to_bits() + 1) - magnitude
}

impl AggOperator for Var {
    fn result_type(&self) -> ColumnType {
        ColumnType::Double
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.normal, slots, 0);
        grow(&mut self.nan, slots, 0);
        grow(&mut self.positive_infinity, slots, 0);
        grow(&mut self.negative_infinity, slots, 0);
        grow(&mut self.sum, slots, NULL_DOUBLE);
        grow(&mut self.sum2, slots, NULL_DOUBLE);
        grow(&mut self.result, slots, NULL_DOUBLE);
    }

    /// Behavior: IntChunkedVarOperator#addChunk, DoubleChunkedVarOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let moments = Moments::of(run, self.floating);
        self.count_non_normal(slot, &moments, 1);
        let force_nan = self.forces_nan(slot);
        if moments.normal > 0 {
            self.normal[slot] += moments.normal;
            let total = self.normal[slot];
            let sum = plus_double(self.sum[slot], moments.sum);
            let sum2 = plus_double(self.sum2[slot], moments.sum2);
            self.sum[slot] = sum;
            self.sum2[slot] = sum2;
            self.result[slot] = if force_nan || total == 1 {
                f64::NAN
            } else {
                self.variance(total, sum, sum2)
            };
            return Effect::MODIFIED;
        }
        if force_nan {
            self.result[slot] = f64::NAN;
            return Effect::MODIFIED;
        }
        match self.normal[slot] {
            0 => self.result[slot] = NULL_DOUBLE,
            1 => self.result[slot] = f64::NAN,
            _ => return Effect::modified_if(!self.floating),
        }
        Effect::MODIFIED
    }

    /// Behavior: IntChunkedVarOperator#removeChunk, DoubleChunkedVarOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let moments = Moments::of(run, self.floating);
        if moments.normal == 0 && moments.non_normal() == 0 {
            return Effect::UNCHANGED;
        }
        self.count_non_normal(slot, &moments, -1);
        let force_nan = self.forces_nan(slot);
        self.normal[slot] -= moments.normal;
        let total = self.normal[slot];
        let (sum, sum2) = if moments.normal > 0 || !self.floating {
            let sums = if total == 0 {
                (0.0, 0.0)
            } else {
                (
                    plus_double(self.sum[slot], -moments.sum),
                    plus_double(self.sum2[slot], -moments.sum2),
                )
            };
            self.sum[slot] = sums.0;
            self.sum2[slot] = sums.1;
            sums
        } else {
            (self.sum[slot], self.sum2[slot])
        };
        self.result[slot] = if self.floating {
            if total == 1 || force_nan {
                f64::NAN
            } else if total == 0 {
                NULL_DOUBLE
            } else {
                self.variance(total, sum, sum2)
            }
        } else if total == 0 {
            NULL_DOUBLE
        } else if total == 1 {
            f64::NAN
        } else {
            self.variance(total, sum, sum2)
        };
        Effect::MODIFIED
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
