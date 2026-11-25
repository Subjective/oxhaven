//! The operators of `BigDecimal` and `BigInteger` columns, and the arithmetic of
//! `java.math.MathContext` they compute with.
//!
//! A `BigInteger` is a decimal of scale 0 here. Java computes with it exactly where its
//! operator keeps `BigInteger`s (sums, products, deltas) and turns it into a `BigDecimal`
//! where the result is one (averages, standard deviations, weighted averages).
//!
//! Java's running values depend on the pass that computed them: a rolling sum keeps the
//! largest scale of the values it added or took away since its window last held no value,
//! and rounds each step to the math context, so a row recomputed after an update can show
//! another scale than the same row computed from the bucket's first row. The kernels here
//! are driven by the pops and pushes of the pass Java makes (`rolling::Window`), so they
//! give the results Java gives after the same updates.
//!
//! The arithmetic is written from the documented behaviour of `java.math.BigDecimal` and
//! `java.math.MathContext` and checked against values OpenJDK 21 computed (the tests below);
//! the JDK's source was not used.

use super::cumulative::Cumulative;
use super::failure;
use super::rolling::{check_held, PairwiseRing, Rolling, Slide, Window};
use super::values::{pick, Values};
use super::DeltaControl;
use crate::ops::filter::java_double_to_string;
use arrow::array::{ArrayRef, AsArray, BinaryArray};
use num_bigint::BigInt;
use num_traits::{One, Zero};
use oxhaven_column::decimal::{self, Decimal, Failure, Rounding};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
use std::sync::Arc;

/// A `java.math.MathContext`: the number of significant digits results are rounded to,
/// none for 0, and how.
///
/// Behavior: java.math.MathContext
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct MathContext {
    pub precision: u32,
    pub rounding: Rounding,
}

impl MathContext {
    /// Behavior: java.math.MathContext#UNLIMITED
    pub const UNLIMITED: MathContext = MathContext {
        precision: 0,
        rounding: Rounding::HalfUp,
    };
    /// Behavior: java.math.MathContext#DECIMAL32
    pub const DECIMAL32: MathContext = MathContext {
        precision: 7,
        rounding: Rounding::HalfEven,
    };
    /// Behavior: java.math.MathContext#DECIMAL64
    pub const DECIMAL64: MathContext = MathContext {
        precision: 16,
        rounding: Rounding::HalfEven,
    };
    /// Behavior: java.math.MathContext#DECIMAL128
    pub const DECIMAL128: MathContext = MathContext {
        precision: 34,
        rounding: Rounding::HalfEven,
    };
}

fn arithmetic(message: &str) -> Failure {
    format!("java.lang.ArithmeticException: {message}")
}

/// `10^exponent`; the exponents here are bounded by a precision and the digits of values
/// that exist.
fn ten_to(exponent: u64) -> BigInt {
    num_traits::pow(BigInt::from(10), exponent as usize)
}

fn scale_of(value: i64) -> Result<i32, Failure> {
    i32::try_from(value).map_err(|_| arithmetic(if value > 0 { "Underflow" } else { "Overflow" }))
}

/// `value` with the zeros at the end of its unscaled value taken off while its scale stays
/// above `floor`.
fn stripped_to(mut value: Decimal, floor: i64) -> Decimal {
    let ten = BigInt::from(10);
    while i64::from(value.scale) > floor && !value.unscaled.is_zero() {
        let (quotient, remainder) = (&value.unscaled / &ten, &value.unscaled % &ten);
        if !remainder.is_zero() {
            break;
        }
        value.unscaled = quotient;
        value.scale -= 1;
    }
    value
}

/// A zero of scale `scale`, which saturates at the range of an `int`.
fn zero_of(scale: i64) -> Decimal {
    Decimal::new(
        BigInt::zero(),
        scale.clamp(i32::MIN.into(), i32::MAX.into()) as i32,
    )
}

/// `BigDecimal.ZERO`, `BigDecimal.ONE`.
fn whole(value: i64) -> Decimal {
    Decimal::of_long(value)
}

/// Whether `value` `equals` `BigDecimal.ZERO`: a zero of scale 0.
fn is_java_zero(value: &Decimal) -> bool {
    value.scale == 0 && value.unscaled.is_zero()
}

impl MathContext {
    /// `value` rounded to the precision: the digits beyond it are dropped as the rounding
    /// mode says, and the scale falls by as many; nothing changes for a value that has no
    /// more digits, nor without a precision.
    ///
    /// Behavior: java.math.BigDecimal#round(MathContext)
    pub(super) fn round(&self, value: Decimal) -> Result<Decimal, Failure> {
        let precision = i64::from(self.precision);
        let mut value = value;
        while precision > 0 && i64::from(value.precision()) > precision {
            let drop = i64::from(value.precision()) - precision;
            let scale = scale_of(i64::from(value.scale) - drop)?;
            value = value.set_scale(scale, self.rounding)?;
        }
        Ok(value)
    }

    /// The result of an operation whose exact value is `exact` and whose preferred scale is
    /// `preferred`: `exact` rounded to the precision when it has more digits, otherwise
    /// the representation of it with the scale nearest the preferred one that has no more
    /// digits than the precision.
    ///
    /// Behavior: the rules for the scale of results in the class documentation of
    /// java.math.BigDecimal, and #add(BigDecimal, MathContext) for a zero operand
    fn fit(&self, exact: Decimal, preferred: i64) -> Result<Decimal, Failure> {
        if self.precision == 0 {
            return Ok(exact);
        }
        if exact.unscaled.is_zero() {
            return Ok(zero_of(preferred));
        }
        let precision = i64::from(self.precision);
        if i64::from(exact.precision()) > precision {
            return self.round(exact);
        }
        let scale = i64::from(exact.scale);
        Ok(match scale.cmp(&preferred) {
            Ordering::Equal => exact,
            Ordering::Greater => stripped_to(exact, preferred),
            Ordering::Less => {
                let room = precision - i64::from(exact.precision());
                let raise = (preferred - scale).min(room);
                Decimal::new(
                    exact.unscaled * ten_to(raise as u64),
                    (scale + raise) as i32,
                )
            }
        })
    }

    /// Behavior: java.math.BigDecimal#add(BigDecimal, MathContext)
    pub(super) fn add(&self, a: &Decimal, b: &Decimal) -> Result<Decimal, Failure> {
        let preferred = i64::from(a.scale.max(b.scale));
        self.fit(a.add(b)?, preferred)
    }

    /// Behavior: java.math.BigDecimal#subtract(BigDecimal, MathContext)
    pub(super) fn subtract(&self, a: &Decimal, b: &Decimal) -> Result<Decimal, Failure> {
        self.add(a, &b.negate())
    }

    /// Behavior: java.math.BigDecimal#multiply(BigDecimal, MathContext)
    pub(super) fn multiply(&self, a: &Decimal, b: &Decimal) -> Result<Decimal, Failure> {
        let preferred = i64::from(a.scale) + i64::from(b.scale);
        self.fit(a.multiply(b)?, preferred)
    }

    /// Behavior: java.math.BigDecimal#divide(BigDecimal, MathContext): the quotient rounded
    /// to the precision, or, when the quotient has no more digits than that, exactly, with
    /// the scale nearest `a.scale - b.scale`
    pub(super) fn divide(&self, a: &Decimal, b: &Decimal) -> Result<Decimal, Failure> {
        if self.precision == 0 {
            return a.divide_exactly(b);
        }
        if b.unscaled.is_zero() {
            return Err(arithmetic(if a.unscaled.is_zero() {
                "Division undefined"
            } else {
                "Division by zero"
            }));
        }
        let preferred = i64::from(a.scale) - i64::from(b.scale);
        if a.unscaled.is_zero() {
            return Ok(zero_of(preferred));
        }
        let precision = i64::from(self.precision);
        // |a / b| lies in [10^e, 10^(e + 2)) with e the difference of the exponents of the
        // leading digits; the scale that gives `precision` digits is one of two.
        let leading = |d: &Decimal| i64::from(d.precision()) - 1 - i64::from(d.scale);
        let exponent = leading(a) - leading(b);
        let mut scale = precision - 1 - exponent;
        let truncated = a.divide_to_scale(b, scale_of(scale)?, Rounding::Down)?;
        if i64::from(truncated.precision()) < precision {
            scale += 1;
        }
        let scale = scale_of(scale)?;
        let mut quotient = a.divide_to_scale(b, scale, self.rounding)?;
        if i64::from(quotient.precision()) > precision {
            // Rounded up to a power of ten: one more digit than wanted, and a zero.
            quotient =
                quotient.set_scale(scale_of(i64::from(scale) - 1)?, Rounding::Unnecessary)?;
            return Ok(quotient);
        }
        let exact = a.divide_to_scale(b, scale, Rounding::Unnecessary).is_ok();
        Ok(if exact {
            stripped_to(quotient, preferred)
        } else {
            quotient
        })
    }

    /// The square root, rounded to the precision; when the root has no more digits than
    /// that, exactly, with the scale nearest half the scale of `a`. Without a precision the
    /// root must be exact.
    ///
    /// Behavior: java.math.BigDecimal#sqrt(MathContext), observed on OpenJDK 21 for the
    /// rounding modes and the scale of exact roots (the tests below)
    pub(super) fn sqrt(&self, a: &Decimal) -> Result<Decimal, Failure> {
        match a.signum() {
            -1 => return Err(arithmetic("Attempted square root of negative BigDecimal")),
            0 => return Ok(zero_of(i64::from(a.scale / 2))),
            _ => {}
        }
        let preferred = i64::from(a.scale / 2);
        let not_exact = || arithmetic("Computed square root not exact.");
        // a = u × 10^-s lies in [10^e, 10^(e + 1)) with e = digits(u) - 1 - s; its root in
        // [10^floor(e / 2), 10^(floor(e / 2) + 1)).
        let leading = (i64::from(a.precision()) - 1 - i64::from(a.scale)).div_euclid(2);
        let precision = if self.precision == 0 {
            // An exact root has at most as many digits as the square, plus one.
            i64::from(a.precision()) + 1
        } else {
            i64::from(self.precision)
        };
        let scale = precision - 1 - leading;
        // The root of y = u × 10^(2 × scale - s), floored, and how y compares with the
        // squares around it.
        let shift = 2 * scale - i64::from(a.scale);
        let magnitude = &a.unscaled;
        let (y_numerator, y_denominator) = if shift >= 0 {
            (magnitude * ten_to(shift as u64), BigInt::one())
        } else {
            (magnitude.clone(), ten_to((-shift) as u64))
        };
        let root = (&y_numerator / &y_denominator).sqrt();
        let exact = &root * &root * &y_denominator == y_numerator;
        let rounding = if self.precision == 0 {
            Rounding::Unnecessary
        } else {
            self.rounding
        };
        let up = if exact {
            false
        } else {
            // y against (root + 1/2)^2: 4y against (2 root + 1)^2.
            let half: BigInt = &root * 2u32 + 1u32;
            let half = (&y_numerator * 4u32).cmp(&(&half * &half * &y_denominator));
            match rounding {
                Rounding::Up | Rounding::Ceiling => true,
                Rounding::Down | Rounding::Floor => false,
                Rounding::HalfUp => half != Ordering::Less,
                Rounding::HalfDown => half == Ordering::Greater,
                Rounding::HalfEven => match half {
                    Ordering::Less => false,
                    Ordering::Greater => true,
                    Ordering::Equal => (&root % 2u32).is_one(),
                },
                Rounding::Unnecessary => return Err(not_exact()),
            }
        };
        let mut result = Decimal::new(if up { root + 1u32 } else { root }, scale_of(scale)?);
        if self.precision > 0 && i64::from(result.precision()) > precision {
            result = result.set_scale(scale_of(scale - 1)?, Rounding::Unnecessary)?;
        }
        Ok(if exact {
            stripped_to(result, preferred)
        } else {
            result
        })
    }
}

/// `BigDecimal.valueOf(double)`: the decimal of the text `Double.toString` gives.
///
/// Behavior: java.math.BigDecimal#valueOf(double), which reads `Double#toString`; the
/// failure for a NaN or an infinity is `new BigDecimal(String)`'s, seen on OpenJDK 21
fn value_of_double(value: f64) -> Result<Decimal, Failure> {
    let text = java_double_to_string(value);
    Decimal::parse(&text).ok_or_else(|| {
        let first = text.chars().next().unwrap_or(' ');
        format!(
            "java.lang.NumberFormatException: Character {first} is neither a decimal digit \
             number, decimal point, nor \"e\" notation exponential mark."
        )
    })
}

pub(super) fn is_big(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::BigDecimal | ColumnType::BigInteger)
}

/// The values of a `BigDecimal` or `BigInteger` column, a `BigInteger` as a decimal of
/// scale 0.
fn decimals(
    array: &ArrayRef,
    column_type: &ColumnType,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let cells = array
        .as_binary_opt::<i32>()
        .ok_or_else(|| failure("a BigDecimal or BigInteger column must hold a binary array"))?;
    cells
        .iter()
        .map(|cell| match cell {
            None => Ok(None),
            Some(bytes) if *column_type == ColumnType::BigDecimal => Decimal::of_cell(bytes)
                .map(Some)
                .ok_or_else(|| failure("a BigDecimal cell without a scale")),
            Some(bytes) => Ok(Some(Decimal::new(decimal::integer_of_cell(bytes), 0))),
        })
        .collect()
}

/// The values of a column of a primitive or big number type, as the decimals Java casts
/// them to for a weighted average.
///
/// Behavior: io.deephaven.engine.table.impl.util.cast.ToBigDecimalCast#makeToBigDecimalCast
/// and its kernels: `BigDecimal#valueOf(long)` for the integer types and `char`,
/// `BigDecimal#valueOf(double)` for `float` (widened) and `double`, `new BigDecimal(BigInteger)`
fn cast_decimals(
    array: &ArrayRef,
    column_type: &ColumnType,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    if is_big(column_type) {
        return decimals(array, column_type);
    }
    match Values::read(array, column_type)? {
        Values::Int(v) => Ok(v.into_iter().map(|v| v.map(Decimal::of_long)).collect()),
        Values::Float(v) => v
            .into_iter()
            .map(|v| v.map(value_of_double).transpose().map_err(failure))
            .collect(),
        _ => Err(failure("no weighted average for this column type")),
    }
}

/// A column of `type` holding `values`: a `BigInteger` column holds the unscaled values,
/// which are whole numbers of scale 0 wherever Java computes `BigInteger`s.
fn decimal_array(column_type: &ColumnType, values: &[Option<Decimal>]) -> ArrayRef {
    let integer = *column_type == ColumnType::BigInteger;
    Arc::new(
        values
            .iter()
            .map(|value| {
                value.as_ref().map(|value| {
                    if integer {
                        decimal::integer_bytes(&value.unscaled)
                    } else {
                        value.cell()
                    }
                })
            })
            .collect::<BinaryArray>(),
    )
}

/// The context exact `BigInteger` arithmetic is done in.
fn exact() -> MathContext {
    MathContext::UNLIMITED
}

/// The type of the result of a cumulative operator on a big number column, or `None` when
/// the operator is not one of those Java has for big numbers.
///
/// Behavior: UpdateByOperatorFactory.OperationVisitor#makeCumSumOperator, #makeCumProdOperator
/// and #makeDeltaOperator: `BigDecimalCumSumOperator`, `BigIntegerCumSumOperator`,
/// `BigDecimalCumProdOperator`, `BigIntegerCumProdOperator`, `BigDecimalDeltaOperator` and
/// `BigIntegerDeltaOperator`, whose results have the input's type
pub(super) fn cumulative_output(kernel: Cumulative, input: &ColumnType) -> Option<ColumnType> {
    match kernel {
        Cumulative::Sum | Cumulative::Product | Cumulative::Delta(_) => Some(input.clone()),
        Cumulative::Min | Cumulative::Max | Cumulative::ForwardFill => None,
    }
}

/// The results of a cumulative sum, product or delta for a run of rows of a big number
/// column; `seed` is the result of the row before the run (its input, for a delta).
pub(super) fn cumulative(
    kernel: Cumulative,
    input: &ColumnType,
    values: &ArrayRef,
    seed: Option<&ArrayRef>,
    math: &MathContext,
) -> Result<ArrayRef, TableFailure> {
    let values = decimals(values, input)?;
    let seed = match seed {
        Some(seed) => decimals(seed, input)?.into_iter().next().flatten(),
        None => None,
    };
    let math = if *input == ColumnType::BigInteger {
        exact()
    } else {
        *math
    };
    let results = match kernel {
        Cumulative::Sum => running(&values, seed, |a, b| math.add(a, b))?,
        Cumulative::Product => running(&values, seed, |a, b| math.multiply(a, b))?,
        Cumulative::Delta(control) => deltas(&values, seed, control)?,
        Cumulative::Min | Cumulative::Max | Cumulative::ForwardFill => {
            return Err(failure(
                "no cumulative operator of big numbers of this kind",
            ))
        }
    };
    Ok(decimal_array(input, &results))
}

/// A running sum or product: a null is skipped, and the first value is taken as it is.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.sum.BigDecimalCumSumOperator.Context#push,
/// BigIntegerCumSumOperator.Context#push, and
/// io.deephaven.engine.table.impl.updateby.prod.BigDecimalCumProdOperator.Context#push and
/// BigIntegerCumProdOperator.Context#push, which resume from the result of the row before
/// (io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator#initializeCumulative)
fn running(
    values: &[Option<Decimal>],
    seed: Option<Decimal>,
    combine: impl Fn(&Decimal, &Decimal) -> Result<Decimal, Failure>,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let mut current = seed;
    values
        .iter()
        .map(|value| {
            current = match (current.take(), value) {
                (None, value) => value.clone(),
                (Some(held), Some(value)) => Some(combine(&held, value).map_err(failure)?),
                (held, None) => held,
            };
            Ok(current.clone())
        })
        .collect()
}

/// Each value less the one before it, exactly. A null row gives null and makes the next
/// row one without a predecessor, which gives what `control` says.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.delta.BigDecimalDeltaOperator.Context#push
/// and #initializeCumulative, and BigIntegerDeltaOperator; the zero is `BigDecimal.ZERO`
fn deltas(
    values: &[Option<Decimal>],
    seed: Option<Decimal>,
    control: DeltaControl,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let mut last = seed;
    values
        .iter()
        .map(|value| {
            let result = match (value, &last) {
                (None, _) => None,
                (Some(value), None) => match control {
                    DeltaControl::NullDominates => None,
                    DeltaControl::ValueDominates => Some(value.clone()),
                    DeltaControl::ZeroDominates => Some(whole(0)),
                },
                (Some(value), Some(last)) => Some(value.subtract(last).map_err(failure)?),
            };
            last = value.clone();
            Ok(result)
        })
        .collect()
}

/// The type of the result of a rolling operator on a big number column (`input`), with a
/// weight column of type `weight` for the weighted average; `None` when Java has no
/// operator of big numbers for them.
///
/// Behavior: UpdateByOperatorFactory.OperationVisitor#makeRollingSumOperator,
/// #makeRollingAvgOperator, #makeRollingProductOperator, #makeRollingStdOperator and
/// #makeRollingWAvgOperator: the sum and product keep the input's type, the others are
/// `BigDecimal`s; a weighted average is of big numbers when its input or its weights are
pub(super) fn rolling_output(
    kernel: Rolling,
    input: &ColumnType,
    weight: Option<&ColumnType>,
) -> Option<ColumnType> {
    match kernel {
        Rolling::Sum | Rolling::Product | Rolling::Min | Rolling::Max if is_big(input) => {
            Some(input.clone())
        }
        Rolling::Avg | Rolling::Std if is_big(input) => Some(ColumnType::BigDecimal),
        Rolling::WAvg if is_big(input) || weight.is_some_and(is_big) => {
            Some(ColumnType::BigDecimal)
        }
        _ => None,
    }
}

/// The results of a rolling operator of big numbers for each slide of the window over the
/// influencer values `influencers`, with the weights of the same rows for a weighted
/// average.
pub(super) fn rolling(
    kernel: Rolling,
    input: &ColumnType,
    influencers: &ArrayRef,
    weights: Option<(&ColumnType, &ArrayRef)>,
    slides: &[Slide],
    math: &MathContext,
) -> Result<ArrayRef, TableFailure> {
    if matches!(kernel, Rolling::Min | Rolling::Max) {
        check_held(slides, "aggMinMax.size()")?;
        let values = Values::read_ordered(influencers, input)?;
        let window = Window::over(&values, slides);
        let picked = comparable_extremes(&values, window, kernel == Rolling::Max);
        return pick(None, influencers, picked);
    }
    let output = rolling_output(kernel, input, weights.map(|(t, _)| t))
        .ok_or_else(|| failure("no rolling operator of big numbers for this column type"))?;
    // A `BigInteger` sum or product is exact; so is the running sum of a `BigInteger`
    // average, whose division alone is rounded.
    let running = if *input == ColumnType::BigInteger {
        exact()
    } else {
        *math
    };
    let results = match kernel {
        Rolling::WAvg => {
            let (weight_type, weights) =
                weights.ok_or_else(|| failure("a weighted average without weights"))?;
            // Java casts the values and the weights of a chunk before it slides over them.
            let values = cast_decimals(influencers, input)?;
            let weights = cast_decimals(weights, weight_type)?;
            check_held(slides, "windowValues.size()")?;
            weighted_averages(&values, &weights, slides, math)?
        }
        Rolling::Sum | Rolling::Avg => {
            // Behavior: io.deephaven.base.RingBuffer#remove, which the sum's `pop` calls
            // without a check of its own (seen on 42.9)
            if check_held(slides, "").is_err() {
                return Err(failure("java.util.NoSuchElementException"));
            }
            let average = (kernel == Rolling::Avg).then_some(math);
            sums(&decimals(influencers, input)?, slides, &running, average)?
        }
        Rolling::Product => {
            check_held(slides, "buffer.size()")?;
            products(&decimals(influencers, input)?, slides, &running)?
        }
        Rolling::Std => {
            check_held(slides, "valueBuffer.size()")?;
            stds(&decimals(influencers, input)?, slides, math)?
        }
        Rolling::Min | Rolling::Max | Rolling::Count => {
            return Err(failure("no rolling operator of big numbers of this kind"))
        }
    };
    Ok(decimal_array(&output, &results))
}

/// A running sum that values are added to and taken from, at `math`; with `average`, each
/// result is the sum divided by the count of values at that context.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingsum.BigDecimalRollingSumOperator.Context
/// #push, #pop and #writeToOutputChunk, BigIntegerRollingSumOperator, and
/// io.deephaven.engine.table.impl.updateby.rollingavg.BigDecimalRollingAvgOperator.Context and
/// BigIntegerRollingAvgOperator.Context, whose running sum of a `BigInteger` column is
/// exact: the first value is taken as it is, and the sum is forgotten when the window holds
/// no value
fn sums(
    values: &[Option<Decimal>],
    slides: &[Slide],
    math: &MathContext,
    average: Option<&MathContext>,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let nulls = Values::Int(values.iter().map(|v| v.as_ref().map(|_| 0)).collect());
    let mut total: Option<Decimal> = None;
    Window::over(&nulls, slides)
        .map(|w| {
            for value in values[w.popped.clone()].iter().flatten() {
                let held = total
                    .take()
                    .ok_or_else(|| failure("update_by: a value left a window without a sum"))?;
                total = Some(math.subtract(&held, value).map_err(failure)?);
            }
            for value in values[w.pushed.clone()].iter().flatten() {
                total = Some(match total.take() {
                    None => value.clone(),
                    Some(held) => math.add(&held, value).map_err(failure)?,
                });
            }
            let count = w.end - w.start - w.nulls;
            if count == 0 {
                total = None;
                return Ok(None);
            }
            match (average, &total) {
                (None, _) => Ok(total.clone()),
                (Some(context), Some(sum)) => Ok(Some(
                    context.divide(sum, &whole(count as i64)).map_err(failure)?,
                )),
                (Some(_), None) => Err(failure("update_by: a window of values without a sum")),
            }
        })
        .collect()
}

/// A value of a ring buffer of big numbers: the value, null, or the failure of the
/// arithmetic that made it.
type Cell = Result<Option<Decimal>, Failure>;

/// Behavior: the value functions of the buffers of the rolling product, standard deviation
/// and weighted average: a null gives way to the other value, two nulls give the identity
fn slots(
    identity: &Decimal,
    combine: impl Fn(&Decimal, &Decimal) -> Result<Decimal, Failure>,
) -> impl Fn(Cell, Cell) -> Cell {
    let identity = identity.clone();
    move |a, b| match (a?, b?) {
        (None, None) => Ok(Some(identity.clone())),
        (None, Some(b)) => Ok(Some(b)),
        (Some(a), None) => Ok(Some(a)),
        (Some(a), Some(b)) => combine(&a, &b).map(Some),
    }
}

/// Behavior: the tree functions of the same buffers, given two values that are not null
fn tree(
    combine: impl Fn(&Decimal, &Decimal) -> Result<Decimal, Failure>,
) -> impl Fn(Cell, Cell) -> Cell {
    move |a, b| match (a?, b?) {
        (Some(a), Some(b)) => combine(&a, &b).map(Some),
        _ => Err("update_by: a null in the tree of a ring buffer".to_owned()),
    }
}

fn evaluated(cell: Cell) -> Result<Decimal, TableFailure> {
    cell.map_err(failure)?
        .ok_or_else(|| failure("update_by: a ring buffer without a value"))
}

/// Behavior: `BUFFER_INITIAL_SIZE` of BigDecimalRollingProductOperator and
/// BigIntegerRollingProductOperator
const PRODUCT_CAPACITY: usize = 64;

/// Behavior: `BUFFER_INITIAL_CAPACITY` of BigDecimalRollingStdOperator,
/// BigIntegerRollingStdOperator, BigDecimalRollingWAvgOperator and
/// ComparableRollingMinMaxOperator
const WIDE_CAPACITY: usize = 128;

/// The product of the values of each window, multiplied pairwise in the tree of Java's
/// ring buffer at `math`. A window with a zero of scale 0 (`BigDecimal.ZERO`; any zero
/// `BigInteger`) has the product `0`; a zero of another scale is multiplied in.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingproduct.BigDecimalRollingProductOperator.Context
/// #push, #pop and #writeToOutputChunk, and BigIntegerRollingProductOperator.Context
fn products(
    values: &[Option<Decimal>],
    slides: &[Slide],
    math: &MathContext,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let one = whole(1);
    let multiply = |a: &Decimal, b: &Decimal| math.multiply(a, b);
    let mut ring = PairwiseRing::with_capacity(
        PRODUCT_CAPACITY,
        Ok(Some(one.clone())),
        slots(&one, multiply),
        tree(multiply),
    );
    let nulls = Values::Int(values.iter().map(|v| v.as_ref().map(|_| 0)).collect());
    let mut zeros = 0usize;
    Window::over(&nulls, slides)
        .map(|w| {
            zeros -= values[w.popped.clone()]
                .iter()
                .flatten()
                .filter(|v| is_java_zero(v))
                .count();
            zeros += values[w.pushed.clone()]
                .iter()
                .flatten()
                .filter(|v| is_java_zero(v))
                .count();
            ring.pop(w.popped.len());
            ring.push(values[w.pushed.clone()].iter().map(|v| Ok(v.clone())));
            if w.end - w.start == w.nulls {
                Ok(None)
            } else if zeros > 0 {
                Ok(Some(whole(0)))
            } else {
                evaluated(ring.evaluate()).map(Some)
            }
        })
        .collect()
}

/// The sample standard deviation of each window: `(Σx² / (n - 1) - (Σx)² / n / (n - 1))`
/// with each step but the subtraction rounded to `math`, and its square root at `math`.
/// The sums are exact, pairwise in the trees of two ring buffers. A window with fewer than
/// two values has none; a variance that rounds below zero fails as in Java.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingstd.BigDecimalRollingStdOperator.Context
/// #push, #pop and #writeToOutputChunk, and BigIntegerRollingStdOperator.Context
fn stds(
    values: &[Option<Decimal>],
    slides: &[Slide],
    math: &MathContext,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let zero = whole(0);
    let add = |a: &Decimal, b: &Decimal| a.add(b);
    let ring = || {
        PairwiseRing::with_capacity(
            WIDE_CAPACITY,
            Ok(Some(zero.clone())),
            slots(&zero, add),
            tree(add),
        )
    };
    let (mut sums, mut squares) = (ring(), ring());
    let nulls = Values::Int(values.iter().map(|v| v.as_ref().map(|_| 0)).collect());
    Window::over(&nulls, slides)
        .map(|w| {
            sums.pop(w.popped.len());
            squares.pop(w.popped.len());
            let pushed = &values[w.pushed.clone()];
            sums.push(pushed.iter().map(|v| Ok(v.clone())));
            squares.push(
                pushed
                    .iter()
                    .map(|v| v.as_ref().map(|v| v.multiply(v)).transpose()),
            );
            let count = w.end - w.start - w.nulls;
            if count <= 1 {
                return Ok(None);
            }
            let square_sum = evaluated(squares.evaluate())?;
            let sum = evaluated(sums.evaluate())?;
            let n = whole(count as i64);
            let n1 = whole(count as i64 - 1);
            let variance = (|| {
                let first = math.divide(&square_sum, &n1)?;
                let second = math.divide(&math.divide(&math.multiply(&sum, &sum)?, &n)?, &n1)?;
                math.sqrt(&first.subtract(&second)?)
            })()
            .map_err(failure)?;
            Ok(Some(variance))
        })
        .collect()
}

/// The sum of weight times value (each product at `math`) over the sum of the weights,
/// for the rows of each window that have both, divided at `math`. Weights that add up to
/// `BigDecimal.ZERO` give null; a zero of another scale fails the division, as in Java.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingwavg.BigDecimalRollingWAvgOperator.Context
/// #push, #pop and #writeToOutputChunk
fn weighted_averages(
    values: &[Option<Decimal>],
    weights: &[Option<Decimal>],
    slides: &[Slide],
    math: &MathContext,
) -> Result<Vec<Option<Decimal>>, TableFailure> {
    let zero = whole(0);
    let add = |a: &Decimal, b: &Decimal| a.add(b);
    let ring = || {
        PairwiseRing::with_capacity(
            WIDE_CAPACITY,
            Ok(Some(zero.clone())),
            slots(&zero, add),
            tree(add),
        )
    };
    let (mut products, mut weight_sums) = (ring(), ring());
    let pairs: Vec<Option<(Decimal, Decimal)>> = values
        .iter()
        .zip(weights)
        .map(|pair| match pair {
            (Some(value), Some(weight)) => Ok(Some((
                math.multiply(weight, value).map_err(failure)?,
                weight.clone(),
            ))),
            _ => Ok(None),
        })
        .collect::<Result<_, TableFailure>>()?;
    let nulls = Values::Int(pairs.iter().map(|v| v.as_ref().map(|_| 0)).collect());
    Window::over(&nulls, slides)
        .map(|w| {
            products.pop(w.popped.len());
            weight_sums.pop(w.popped.len());
            let pushed = &pairs[w.pushed.clone()];
            products.push(
                pushed
                    .iter()
                    .map(|p| Ok(p.as_ref().map(|(product, _)| product.clone()))),
            );
            weight_sums.push(
                pushed
                    .iter()
                    .map(|p| Ok(p.as_ref().map(|(_, weight)| weight.clone()))),
            );
            if w.end - w.start == w.nulls {
                return Ok(None);
            }
            let weight_sum = evaluated(weight_sums.evaluate())?;
            if is_java_zero(&weight_sum) {
                return Ok(None);
            }
            let product_sum = evaluated(products.evaluate())?;
            math.divide(&product_sum, &weight_sum)
                .map(Some)
                .map_err(failure)
        })
        .collect()
}

/// The index of the running minimum or maximum Java keeps for each window of comparable
/// values. A push replaces it only by a value strictly beyond it, so of equal values the
/// earlier stays; popping a value equal to it makes Java evaluate its ring buffer, whose
/// tree keeps the second of two equal values, so the one it finds depends on the slots of
/// the buffer the values are in. For a `BigDecimal` two equal values can differ in scale.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollingminmax.ComparableRollingMinMaxOperator.Context
/// #push, #pop and #writeToOutputChunk, and the function of its buffer
fn comparable_extremes(values: &Values, window: Window, max: bool) -> Vec<Option<usize>> {
    let wanted = if max {
        Ordering::Greater
    } else {
        Ordering::Less
    };
    let better = |a: usize, b: usize| values.compare(a, b) == Some(wanted);
    let combine = |a: Option<usize>, b: Option<usize>| match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => Some(if better(a, b) { a } else { b }),
    };
    let mut ring = PairwiseRing::with_capacity(WIDE_CAPACITY, None, combine, combine);
    let mut current: Option<usize> = None;
    let mut evaluate = false;
    window
        .map(|w| {
            for index in w.popped.clone() {
                if values.is_null(index) {
                    continue;
                }
                if current.is_some_and(|held| values.compare(held, index) == Some(Ordering::Equal))
                {
                    evaluate = true;
                }
            }
            ring.pop(w.popped.len());
            ring.push(w.pushed.clone().map(|i| (!values.is_null(i)).then_some(i)));
            for index in w.pushed.clone() {
                if values.is_null(index) {
                    continue;
                }
                match current {
                    None => current = Some(index),
                    Some(held) if better(index, held) => {
                        current = Some(index);
                        evaluate = false;
                    }
                    Some(_) => {}
                }
            }
            if w.end - w.start == w.nulls {
                current = None;
            } else if evaluate {
                current = ring.evaluate();
            }
            evaluate = false;
            current
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(text: &str) -> Decimal {
        Decimal::parse(text).unwrap()
    }

    /// The text `BigDecimal#toString` gives, and the scale.
    fn shown(value: Result<Decimal, Failure>) -> String {
        match value {
            Ok(value) => format!("{} s={}", value.text(), value.scale),
            Err(e) => e,
        }
    }

    fn context(precision: u32, rounding: Rounding) -> MathContext {
        MathContext {
            precision,
            rounding,
        }
    }

    /// Values printed by OpenJDK 21.0.12 for the same operations.
    #[test]
    fn operations_at_a_math_context_match_java() {
        let m = MathContext::DECIMAL64;
        let cases: Vec<(Result<Decimal, Failure>, &str)> = vec![
            (m.add(&d("0.00"), &d("5")), "5.00 s=2"),
            (m.add(&d("5"), &d("0.00")), "5.00 s=2"),
            (m.add(&d("9999999999999999"), &d("1")), "1.000000000000000E+16 s=-1"),
            (m.add(&d("99999999999999999"), &d("1")), "1.000000000000000E+17 s=-2"),
            (m.add(&d("0E+3"), &d("0E-2")), "0.00 s=2"),
            (m.add(&d("12345678901234567890"), &d("0.00")), "1.234567890123457E+19 s=-3"),
            (m.add(&d("1.23E+5"), &d("0.00")), "123000.00 s=2"),
            (m.subtract(&d("1.5"), &d("1.5")), "0.0 s=1"),
            (
                context(3, Rounding::Unnecessary).add(&d("1.25"), &d("1E+20")),
                "java.lang.ArithmeticException: Rounding necessary",
            ),
            (m.multiply(&d("1E+3"), &d("1E+2")), "1E+5 s=-5"),
            (m.multiply(&d("0.00"), &d("5")), "0.00 s=2"),
            (
                m.multiply(&d("1.23456789012345678"), &d("1.23456789012345678")),
                "1.524157875323884 s=15",
            ),
            (m.divide(&d("10"), &d("4")), "2.5 s=1"),
            (m.divide(&d("10.00"), &d("2")), "5.00 s=2"),
            (m.divide(&d("1"), &d("3")), "0.3333333333333333 s=16"),
            (m.divide(&d("1E+5"), &d("3")), "33333.33333333333 s=11"),
            (context(2, Rounding::HalfEven).divide(&d("1"), &d("8")), "0.12 s=2"),
            (m.divide(&d("1.00"), &d("8")), "0.125 s=3"),
            (m.divide(&d("100"), &d("4")), "25 s=0"),
            (m.divide(&d("1E+2"), &d("4")), "25 s=0"),
            (m.divide(&d("1E+2"), &d("0.4")), "2.5E+2 s=-1"),
            (context(2, Rounding::HalfEven).divide(&d("99.99"), &d("1")), "1.0E+2 s=-1"),
            (m.divide(&d("0E+3"), &d("7")), "0E+3 s=-3"),
            (
                m.divide(&d("1"), &d("0")),
                "java.lang.ArithmeticException: Division by zero",
            ),
            (
                m.divide(&d("0"), &d("0")),
                "java.lang.ArithmeticException: Division undefined",
            ),
            (
                MathContext::UNLIMITED.divide(&d("1"), &d("3")),
                "java.lang.ArithmeticException: Non-terminating decimal expansion; no exact representable decimal result.",
            ),
            (MathContext::UNLIMITED.divide(&d("7"), &d("2")), "3.5 s=1"),
            (MathContext::UNLIMITED.divide(&d("7.0"), &d("2E+1")), "0.35 s=2"),
            (m.sqrt(&d("4.00")), "2.0 s=1"),
            (m.sqrt(&d("0.0400")), "0.20 s=2"),
            (m.sqrt(&d("2")), "1.414213562373095 s=15"),
            (m.sqrt(&d("400")), "20 s=0"),
            (m.sqrt(&d("4E+2")), "2E+1 s=-1"),
            (m.sqrt(&d("4E+3")), "63.24555320336759 s=14"),
            (m.sqrt(&d("0E-5")), "0.00 s=2"),
            (m.sqrt(&d("0E+5")), "0E+2 s=-2"),
            (m.sqrt(&d("1.21")), "1.1 s=1"),
            (m.sqrt(&d("1.2100")), "1.10 s=2"),
            (m.sqrt(&d("12100")), "110 s=0"),
            (m.sqrt(&d("123456789012345678")), "351364182.8820144 s=7"),
            (m.sqrt(&d("1E-400")), "1E-200 s=200"),
            (m.sqrt(&d("1E-401")), "3.162277660168379E-201 s=216"),
            (
                m.sqrt(&d("-1")),
                "java.lang.ArithmeticException: Attempted square root of negative BigDecimal",
            ),
            (context(5, Rounding::Up).sqrt(&d("2")), "1.4143 s=4"),
            (context(5, Rounding::Down).sqrt(&d("2")), "1.4142 s=4"),
            (context(5, Rounding::Ceiling).sqrt(&d("2")), "1.4143 s=4"),
            (
                context(5, Rounding::Unnecessary).sqrt(&d("2")),
                "java.lang.ArithmeticException: Computed square root not exact.",
            ),
            (context(5, Rounding::Unnecessary).sqrt(&d("4")), "2 s=0"),
            (
                MathContext::UNLIMITED.sqrt(&d("2")),
                "java.lang.ArithmeticException: Computed square root not exact.",
            ),
            (MathContext::UNLIMITED.sqrt(&d("4.00")), "2.0 s=1"),
            (MathContext::UNLIMITED.sqrt(&d("1.44E+4")), "1.2E+2 s=-1"),
            (context(1, Rounding::HalfEven).sqrt(&d("2.25")), "2 s=0"),
            (context(1, Rounding::HalfEven).sqrt(&d("6.25")), "2 s=0"),
            (context(1, Rounding::HalfDown).sqrt(&d("6.25")), "2 s=0"),
            (
                MathContext::DECIMAL128.subtract(&whole(1), &d("0.7165313105737893")),
                "0.2834686894262107 s=16",
            ),
            (
                m.round(Decimal::new(BigInt::parse_bytes(b"123456789012345678901", 10).unwrap(), 0)),
                "1.234567890123457E+20 s=-5",
            ),
            (value_of_double(0.1f32 as f64), "0.10000000149011612 s=17"),
            (value_of_double(1e-7), "1.0E-7 s=8"),
            (value_of_double(1e21), "1.0E+21 s=-20"),
            (
                value_of_double(f64::NAN),
                "java.lang.NumberFormatException: Character N is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.",
            ),
            (
                value_of_double(f64::INFINITY),
                "java.lang.NumberFormatException: Character I is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.",
            ),
        ];
        for (index, (value, expected)) in cases.into_iter().enumerate() {
            assert_eq!(shown(value), expected, "case {index}");
        }
    }
}
