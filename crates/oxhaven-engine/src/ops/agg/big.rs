//! Sums, absolute sums, averages, variances and standard deviations of `BigDecimal` and
//! `BigInteger` columns.
//!
//! Java computes them exactly, as `BigDecimal`s and `BigInteger`s, with the scale that
//! `BigDecimal#add` and `#subtract` give a result: the larger of the two. A running sum is
//! made of the sums of the chunks given to the operator, and keeps the largest scale of
//! any value added or removed since the group last had no value that was not null. A
//! `BigInteger` is a decimal of scale 0 here: its arithmetic is the same, and only the
//! type of a sum's result differs.

use super::operator::{AggOperator, Effect};
use super::values::{build, Run, Scalar};
use crate::ops::filter::java_double_to_string;
use arrow::array::ArrayRef;
use num_bigint::BigInt;
use num_traits::Zero;
use oxhaven_column::decimal::{Decimal, Rounding};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

/// The scale of a standard deviation: the default of the properties
/// `BigDecimalStdOperator.scale` and `BigIntegerStdOperator.scale`.
///
/// Behavior: io.deephaven.engine.table.impl.by.BigDecimalChunkedVarOperator#SCALE
const STD_SCALE: i32 = 10;

fn grow<T: Clone>(values: &mut Vec<T>, slots: usize, fill: T) {
    if values.len() < slots {
        values.resize(slots, fill);
    }
}

/// The decimal of a non-null value of a `BigDecimal` or `BigInteger` column.
fn decimal_of(value: &Scalar) -> Option<Decimal> {
    match value {
        Scalar::Decimal(value) => Some((**value).clone()),
        Scalar::Integer(value) => Some(Decimal::new((**value).clone(), 0)),
        _ => None,
    }
}

/// `BigDecimal.ZERO`, which a chunk's sum starts from.
fn zero() -> Decimal {
    Decimal::new(BigInt::zero(), 0)
}

/// Whether `value` `equals` `BigDecimal.ZERO`: zero, with scale 0. A zero of another scale
/// is not.
fn is_java_zero(value: &Decimal) -> bool {
    value.scale == 0 && value.unscaled.is_zero()
}

/// What the non-null values of a run add up to.
struct Partial {
    sum: Decimal,
    /// The sum of the squares, for a variance.
    sum2: Decimal,
    non_null: u64,
}

/// Behavior: io.deephaven.engine.table.impl.by.SumBigDecimalChunk#sumBigDecimalChunk,
/// #sumBigDecimalChunkAbs and #sum2BigDecimalChunk, and their `BigInteger` copies: from
/// `ZERO`, in the order of the run.
fn partial(run: &Run<'_>, absolute: bool, squares: bool) -> Result<Partial, String> {
    let mut partial = Partial {
        sum: zero(),
        sum2: zero(),
        non_null: 0,
    };
    for (_, value) in run.scalars() {
        let Some(value) = decimal_of(&value) else {
            continue;
        };
        partial.non_null += 1;
        let value = if absolute { value.abs() } else { value };
        partial.sum = partial.sum.add(&value)?;
        if squares {
            partial.sum2 = partial.sum2.add(&value.multiply(&value)?)?;
        }
    }
    Ok(partial)
}

/// `a + b` where either may be null: the other then, and null for two nulls.
///
/// Behavior: io.deephaven.engine.table.impl.by.BigDecimalChunkedSumOperator#plus
fn plus(a: &Option<Decimal>, b: &Decimal) -> Result<Decimal, String> {
    match a {
        None => Ok(b.clone()),
        Some(a) => a.add(b),
    }
}

/// The result cells of `slots` from `values`, as a column of `column_type`.
fn results(values: &[Option<Decimal>], slots: &[usize], integer: bool) -> ArrayRef {
    let scalars: Vec<Scalar> = slots
        .iter()
        .map(|&slot| match &values[slot] {
            None => Scalar::Null,
            Some(value) if integer => Scalar::Integer(Arc::new(value.unscaled.clone())),
            Some(value) => Scalar::Decimal(Arc::new(value.clone())),
        })
        .collect();
    let column_type = if integer {
        ColumnType::BigInteger
    } else {
        ColumnType::BigDecimal
    };
    build(&column_type, scalars.iter())
}

/// Where an operation on big numbers failed. Java throws from the operator, which fails
/// the result; the first failure is kept until the framework asks for it.
#[derive(Default)]
struct Failures(Option<TableFailure>);

impl Failures {
    fn note(&mut self, message: String) {
        if self.0.is_none() {
            self.0 = Some(TableFailure { message });
        }
    }

    /// The value of `result`, or `None` with the failure noted.
    fn check<T>(&mut self, result: Result<T, String>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(message) => {
                self.note(message);
                None
            }
        }
    }
}

/// The sum, or the sum of the absolute values, of the non-null values of a `BigDecimal` or
/// `BigInteger` column, of the column's type; null for a group without such a value.
///
/// Behavior: io.deephaven.engine.table.impl.by.BigDecimalChunkedSumOperator and
/// io.deephaven.engine.table.impl.by.BigIntegerChunkedSumOperator
pub struct BigSum {
    absolute: bool,
    integer: bool,
    sums: Vec<Option<Decimal>>,
    non_null: Vec<u64>,
    failures: Failures,
}

impl BigSum {
    pub fn new(absolute: bool, integer: bool) -> BigSum {
        BigSum {
            absolute,
            integer,
            sums: Vec::new(),
            non_null: Vec::new(),
            failures: Failures::default(),
        }
    }

    fn partial(&mut self, run: &Run<'_>) -> Option<Partial> {
        self.failures.check(partial(run, self.absolute, false))
    }
}

impl AggOperator for BigSum {
    fn result_type(&self) -> ColumnType {
        if self.integer {
            ColumnType::BigInteger
        } else {
            ColumnType::BigDecimal
        }
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.sums, slots, None);
        grow(&mut self.non_null, slots, 0);
    }

    /// Behavior: BigDecimalChunkedSumOperator#addChunk: a chunk's sum that is
    /// `BigDecimal.ZERO` changes nothing once the group has a sum.
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let Some(partial) = self.partial(run) else {
            return Effect::UNCHANGED;
        };
        if partial.non_null == 0 {
            return Effect::UNCHANGED;
        }
        let changed = self.sums[slot].is_none() || !is_java_zero(&partial.sum);
        if changed {
            let sum = plus(&self.sums[slot], &partial.sum);
            if let Some(sum) = self.failures.check(sum) {
                self.sums[slot] = Some(sum);
            }
        }
        self.non_null[slot] += partial.non_null;
        Effect::modified_if(changed)
    }

    /// Behavior: BigDecimalChunkedSumOperator#removeChunk: the sum is null once the group
    /// has no value that is not null, and otherwise loses the chunk's sum.
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let Some(partial) = self.partial(run) else {
            return Effect::UNCHANGED;
        };
        if partial.non_null == 0 {
            return Effect::UNCHANGED;
        }
        self.non_null[slot] -= partial.non_null;
        if self.non_null[slot] == 0 {
            self.sums[slot] = None;
        } else if is_java_zero(&partial.sum) {
            return Effect::UNCHANGED;
        } else {
            let sum = match &self.sums[slot] {
                Some(sum) => sum.subtract(&partial.sum),
                None => Ok(partial.sum.negate()),
            };
            if let Some(sum) = self.failures.check(sum) {
                self.sums[slot] = Some(sum);
            }
        }
        Effect::MODIFIED
    }

    /// Behavior: BigDecimalChunkedSumOperator#modifyChunk: the difference of the new values'
    /// sum and the old values' sum is added, unless the group is left without a value that
    /// is not null.
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let (Some(before), Some(after)) = (self.partial(old), self.partial(new)) else {
            return Effect::UNCHANGED;
        };
        if before.non_null != after.non_null {
            self.non_null[slot] = self.non_null[slot] + after.non_null - before.non_null;
            if self.non_null[slot] == 0 {
                self.sums[slot] = None;
                return Effect::MODIFIED;
            }
        }
        let Some(difference) = self.failures.check(after.sum.subtract(&before.sum)) else {
            return Effect::UNCHANGED;
        };
        if is_java_zero(&difference) {
            return Effect::UNCHANGED;
        }
        let sum = plus(&self.sums[slot], &difference);
        if let Some(sum) = self.failures.check(sum) {
            self.sums[slot] = Some(sum);
        }
        Effect::MODIFIED
    }

    fn take_failure(&mut self) -> Option<TableFailure> {
        self.failures.0.take()
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        results(&self.sums, slots, self.integer)
    }
}

/// `sum / count` at the scale of `sum`, rounded half up.
///
/// Behavior: BigDecimal#divide(BigDecimal, int) with BigDecimal#ROUND_HALF_UP, as
/// io.deephaven.engine.table.impl.by.BigDecimalChunkedAvgOperator calls it
fn divided(sum: &Decimal, count: u64) -> Result<Decimal, String> {
    let count = Decimal::new(BigInt::from(count), 0);
    sum.divide_to_scale(&count, sum.scale, Rounding::HalfUp)
}

/// The mean of the non-null values of a `BigDecimal` or `BigInteger` column, as a
/// `BigDecimal` with the scale of the running sum (0 for a `BigInteger` column).
///
/// Behavior: io.deephaven.engine.table.impl.by.BigDecimalChunkedAvgOperator and
/// io.deephaven.engine.table.impl.by.BigIntegerChunkedAvgOperator, whose modification is a
/// removal followed by an addition
pub struct BigAvg {
    sums: Vec<Option<Decimal>>,
    non_null: Vec<u64>,
    result: Vec<Option<Decimal>>,
    failures: Failures,
}

impl BigAvg {
    pub fn new() -> BigAvg {
        BigAvg {
            sums: Vec::new(),
            non_null: Vec::new(),
            result: Vec::new(),
            failures: Failures::default(),
        }
    }

    fn set(&mut self, slot: usize, sum: Result<Decimal, String>) {
        let result = sum.and_then(|sum| Ok((divided(&sum, self.non_null[slot])?, sum)));
        if let Some((result, sum)) = self.failures.check(result) {
            self.result[slot] = Some(result);
            self.sums[slot] = Some(sum);
        }
    }
}

impl Default for BigAvg {
    fn default() -> BigAvg {
        BigAvg::new()
    }
}

impl AggOperator for BigAvg {
    fn result_type(&self) -> ColumnType {
        ColumnType::BigDecimal
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.sums, slots, None);
        grow(&mut self.non_null, slots, 0);
        grow(&mut self.result, slots, None);
    }

    /// Behavior: BigDecimalChunkedAvgOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let Some(partial) = self.failures.check(partial(run, false, false)) else {
            return Effect::UNCHANGED;
        };
        if partial.non_null == 0 {
            return Effect::UNCHANGED;
        }
        self.non_null[slot] += partial.non_null;
        let sum = plus(&self.sums[slot], &partial.sum);
        self.set(slot, sum);
        Effect::MODIFIED
    }

    /// Behavior: BigDecimalChunkedAvgOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let Some(partial) = self.failures.check(partial(run, false, false)) else {
            return Effect::UNCHANGED;
        };
        if partial.non_null == 0 {
            return Effect::UNCHANGED;
        }
        self.non_null[slot] -= partial.non_null;
        if self.non_null[slot] == 0 {
            self.result[slot] = None;
            self.sums[slot] = None;
        } else {
            let sum = match &self.sums[slot] {
                Some(sum) => sum.subtract(&partial.sum),
                None => Ok(partial.sum.negate()),
            };
            self.set(slot, sum);
        }
        Effect::MODIFIED
    }

    fn take_failure(&mut self) -> Option<TableFailure> {
        self.failures.0.take()
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        results(&self.result, slots, false)
    }
}

/// The square root of `x` to `scale` places, by Newton's iteration from the `double`
/// nearest it: `ZERO` for zero, Java's `IllegalArgumentException` for a negative `x`.
///
/// Behavior: io.deephaven.util.BigDecimalUtils#sqrt(BigDecimal, int)
pub(crate) fn sqrt(x: &Decimal, scale: i32) -> Result<Decimal, String> {
    match x.signum() {
        0 => return Ok(zero()),
        sign if sign < 0 => {
            return Err(format!(
                "java.lang.IllegalArgumentException: x={} is negative.",
                x.text()
            ))
        }
        _ => {}
    }
    let root = x.to_double().sqrt();
    let initial = if root.is_finite() {
        root
    } else {
        f64::MAX.sqrt()
    };
    let epsilon = Decimal::new(BigInt::from(1), scale);
    let two = Decimal::new(BigInt::from(2), 0);
    // `BigDecimal#valueOf(double)`: the number of the text of `Double#toString`.
    let mut x0 = Decimal::parse(&java_double_to_string(initial))
        .ok_or_else(|| format!("the square root of {} has no first guess", x.text()))?;
    loop {
        let x1 = x0
            .add(&x.divide_to_scale(&x0, scale, Rounding::HalfUp)?)?
            .divide_to_scale(&two, scale, Rounding::HalfUp)?;
        let difference = x1.subtract(&x0)?.abs();
        x0 = x1;
        if difference.compare(&epsilon).is_le() {
            return Ok(x0);
        }
    }
}

/// The sample variance or standard deviation of the non-null values of a `BigDecimal` or
/// `BigInteger` column, as a `BigDecimal`; null for a group of fewer than two values.
///
/// Behavior: io.deephaven.engine.table.impl.by.BigDecimalChunkedVarOperator and
/// io.deephaven.engine.table.impl.by.BigIntegerChunkedVarOperator, whose modification is a
/// removal followed by an addition
pub struct BigVar {
    std: bool,
    sums: Vec<Option<(Decimal, Decimal)>>,
    non_null: Vec<u64>,
    result: Vec<Option<Decimal>>,
    failures: Failures,
}

impl BigVar {
    pub fn new(std: bool) -> BigVar {
        BigVar {
            std,
            sums: Vec::new(),
            non_null: Vec::new(),
            result: Vec::new(),
            failures: Failures::default(),
        }
    }

    /// The running sums of `slot` with `partial` added, or subtracted for `negate`.
    ///
    /// Behavior: BigDecimalChunkedVarOperator#plus, which makes two nulls `ZERO`
    fn moved(&self, slot: usize, partial: &Partial, negate: bool) -> Result<(Decimal, Decimal), String> {
        let (sum, sum2) = if negate {
            (partial.sum.negate(), partial.sum2.negate())
        } else {
            (partial.sum.clone(), partial.sum2.clone())
        };
        Ok(match &self.sums[slot] {
            None => (sum, sum2),
            Some((running, running2)) => (running.add(&sum)?, running2.add(&sum2)?),
        })
    }

    /// Behavior: BigDecimalChunkedVarOperator#doUpdate
    fn update(&mut self, slot: usize, sums: (Decimal, Decimal)) {
        let count = self.non_null[slot];
        self.sums[slot] = (count > 0).then(|| sums.clone());
        if count <= 1 {
            self.result[slot] = None;
            return;
        }
        let (sum, sum2) = sums;
        let std = self.std;
        let variance = (|| {
            let mean_square = divided(&sum.multiply(&sum)?, count)?;
            let variance = divided(&sum2.subtract(&mean_square)?, count - 1)?;
            if std {
                sqrt(&variance, STD_SCALE)
            } else {
                Ok(variance)
            }
        })();
        if let Some(variance) = self.failures.check(variance) {
            self.result[slot] = Some(variance);
        }
    }

    fn step(&mut self, slot: usize, run: &Run<'_>, remove: bool) -> Effect {
        let Some(partial) = self.failures.check(partial(run, false, true)) else {
            return Effect::UNCHANGED;
        };
        if partial.non_null == 0 {
            return Effect::UNCHANGED;
        }
        let Some(sums) = self.failures.check(self.moved(slot, &partial, remove)) else {
            return Effect::UNCHANGED;
        };
        if remove {
            self.non_null[slot] -= partial.non_null;
        } else {
            self.non_null[slot] += partial.non_null;
        }
        self.update(slot, sums);
        Effect::MODIFIED
    }
}

impl AggOperator for BigVar {
    fn result_type(&self) -> ColumnType {
        ColumnType::BigDecimal
    }

    fn ensure_slots(&mut self, slots: usize) {
        grow(&mut self.sums, slots, None);
        grow(&mut self.non_null, slots, 0);
        grow(&mut self.result, slots, None);
    }

    /// Behavior: BigDecimalChunkedVarOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.step(slot, run, false)
    }

    /// Behavior: BigDecimalChunkedVarOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.step(slot, run, true)
    }

    fn take_failure(&mut self) -> Option<TableFailure> {
        self.failures.0.take()
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        results(&self.result, slots, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(text: &str) -> Decimal {
        Decimal::parse(text).expect(text)
    }

    /// Behavior: BigDecimalUtils#sqrt; the roots seen on Deephaven 42.9 as the standard
    /// deviations of `tasks/11.5-agg-refusals.md`, Handover.
    #[test]
    fn square_roots_have_ten_places() {
        assert_eq!(sqrt(&d("0.7500"), 10).unwrap().text(), "0.8660254038");
        assert_eq!(sqrt(&d("1"), 10).unwrap().text(), "1.0000000000");
        assert_eq!(sqrt(&d("0.000000"), 10).unwrap().text(), "0");
        assert_eq!(
            sqrt(&d("-1.5"), 10).unwrap_err(),
            "java.lang.IllegalArgumentException: x=-1.5 is negative."
        );
    }
}
