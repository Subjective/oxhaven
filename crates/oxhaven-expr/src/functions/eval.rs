//! Evaluation of the function library over Arrow arrays.
//!
//! Each algorithm is written once, over [`Number`], and runs in the Rust type of the Java
//! primitive type its arguments have, so a `float` function computes in `f32` as Java does.
//! The arguments arrive already converted to the parameter types [`super::resolve`] chose.

use super::{EpochUnit, InstantMethod, Kernel, Math, Parsed, TimeOrder, ZonedField};
use crate::eval::numeric::{self, JavaPrimitive};
use crate::parse::instant::{civil_from_days, days_from_civil, zoned_seconds, Zone};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray,
    PrimitiveArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, TimestampNanosecondType};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// `java.lang.Math#round(float)`: the nearest `int`, ties toward positive infinity, NaN as
/// zero, and the ends of the `int` range for what lies beyond them.
pub(super) fn round_float(value: f32) -> i32 {
    let floor = value.floor();
    (if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }) as i32
}

/// `java.lang.Math#round(double)`, likewise to `long`.
pub(super) fn round_double(value: f64) -> i64 {
    let floor = value.floor();
    (if value - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }) as i64
}

/// A Java primitive numeric type, with the operations the library applies to all of them.
pub(super) trait Number: JavaPrimitive + PartialOrd + std::fmt::Display {
    /// Java's `(int)` cast.
    fn int(self) -> i32;
    fn is_nan(self) -> bool;
    fn is_infinite(self) -> bool;
    /// `(T) Math.abs(value)`.
    fn abs(self) -> Self;
    /// `Math.round(value)`, with the overload Java reaches for the type: `round(float)` for
    /// every type but `double`.
    fn round(self) -> i64;
    /// `(T) (self - other)`.
    fn minus(self, other: Self) -> Self;
    /// `(T) (self + other)`.
    fn plus(self, other: Self) -> Self;
    fn is_positive(self) -> bool;
    /// The multiple of `interval` at or below `self`, or with `upper` at or above it.
    fn bin(self, interval: Self, upper: bool) -> Self;
}

macro_rules! integer_number {
    ($native:ty) => {
        impl Number for $native {
            fn int(self) -> i32 {
                self as i32
            }
            fn is_nan(self) -> bool {
                false
            }
            fn is_infinite(self) -> bool {
                false
            }
            // Behavior: `Numeric#abs`, `(T) Math.abs(value)`: computed as `int` or `long`,
            // where the lowest value is its own absolute value.
            fn abs(self) -> Self {
                Self::from_integer((self as i64).wrapping_abs())
            }
            fn round(self) -> i64 {
                round_float(self as f32).into()
            }
            fn minus(self, other: Self) -> Self {
                Self::from_integer((self as i64).wrapping_sub(other as i64))
            }
            fn plus(self, other: Self) -> Self {
                Self::from_integer((self as i64).wrapping_add(other as i64))
            }
            fn is_positive(self) -> bool {
                self as i64 > 0
            }
            // Behavior: `Numeric#lowerBin` and `#upperBin` for an integer type: `long`
            // division, which truncates, corrected toward the bin.
            fn bin(self, interval: Self, upper: bool) -> Self {
                let (value, interval) = (self as i64, interval as i64);
                let (quotient, remainder) = (value / interval, value % interval);
                let bins = if upper {
                    quotient + i64::from(remainder > 0)
                } else if remainder != 0 && value < 0 {
                    quotient - 1
                } else {
                    quotient
                };
                Self::from_integer(interval.wrapping_mul(bins))
            }
        }
    };
}

macro_rules! floating_number {
    ($native:ty, $round:expr) => {
        impl Number for $native {
            fn int(self) -> i32 {
                self as i32
            }
            fn is_nan(self) -> bool {
                <$native>::is_nan(self)
            }
            fn is_infinite(self) -> bool {
                <$native>::is_infinite(self)
            }
            fn abs(self) -> Self {
                <$native>::abs(self)
            }
            fn round(self) -> i64 {
                $round(self)
            }
            fn minus(self, other: Self) -> Self {
                self - other
            }
            fn plus(self, other: Self) -> Self {
                self + other
            }
            fn is_positive(self) -> bool {
                self > 0.0
            }
            // Behavior: `Numeric#lowerBin` and `#upperBin` for a floating type:
            // `(T) (interval * Math.floor(value / interval))`, the division in `T` and the
            // rest in `double`.
            fn bin(self, interval: Self, upper: bool) -> Self {
                let ratio = f64::from(self / interval);
                let bins = if upper { ratio.ceil() } else { ratio.floor() };
                Self::from_floating(f64::from(interval) * bins)
            }
        }
    };
}

integer_number!(i8);
integer_number!(i16);
integer_number!(u16);
integer_number!(i32);
integer_number!(i64);
floating_number!(f32, |value| i64::from(round_float(value)));
floating_number!(f64, round_double);

/// Evaluates `$body` with `$t` naming the Rust type of the numbers in an array.
macro_rules! in_number_type {
    ($array:expr, |$t:ident| $body:expr) => {
        match $array.data_type() {
            DataType::Int8 => {
                type $t = i8;
                $body
            }
            DataType::Int16 => {
                type $t = i16;
                $body
            }
            DataType::UInt16 => {
                type $t = u16;
                $body
            }
            DataType::Int32 => {
                type $t = i32;
                $body
            }
            DataType::Int64 => {
                type $t = i64;
                $body
            }
            DataType::Float32 => {
                type $t = f32;
                $body
            }
            DataType::Float64 => {
                type $t = f64;
                $body
            }
            other => unreachable!("the type checker passed a {other} array as a number"),
        }
    };
}

/// The arguments of a call as arrays of one numeric type.
struct Numbers<'a, T: Number>(Vec<&'a PrimitiveArray<T::Arrow>>);

impl<'a, T: Number> Numbers<'a, T> {
    fn new(arguments: &'a [ArrayRef]) -> Self {
        Numbers(
            arguments
                .iter()
                .map(|argument| argument.as_primitive::<T::Arrow>())
                .collect(),
        )
    }

    /// The argument at `index` in `row`, or `None` for null.
    fn get(&self, index: usize, row: usize) -> Option<T> {
        let array = self.0[index];
        array.is_valid(row).then(|| array.value(row))
    }

    /// The argument as the generated Java code holds it: null is the reserved value.
    fn raw(&self, index: usize, row: usize) -> T {
        self.get(index, row).unwrap_or(T::NULL)
    }

    fn rows(&self) -> usize {
        self.0[0].len()
    }

    /// A column of `T`, where the reserved value is null.
    fn map(&self, row: impl Fn(usize) -> T) -> ArrayRef {
        let values: PrimitiveArray<T::Arrow> = (0..self.rows())
            .map(|index| Some(row(index)).filter(|value| *value != T::NULL))
            .collect();
        Arc::new(values)
    }

    fn try_map(
        &self,
        row: impl Fn(usize) -> Result<T, TableFailure>,
    ) -> Result<ArrayRef, TableFailure> {
        let values = (0..self.rows())
            .map(|index| Ok(Some(row(index)?).filter(|value| *value != T::NULL)))
            .collect::<Result<PrimitiveArray<T::Arrow>, TableFailure>>()?;
        Ok(Arc::new(values))
    }

    /// A `Boolean` column without nulls.
    fn test(&self, row: impl Fn(usize) -> bool) -> ArrayRef {
        Arc::new(
            (0..self.rows())
                .map(|index| Some(row(index)))
                .collect::<BooleanArray>(),
        )
    }
}

/// Behavior: `Numeric#min(T...)` and `#max(T...)`, through `#min(Vector)`, `#max(Vector)`
/// and `#indexOfMin`: nulls are skipped, a NaN is the result, and no value at all is null.
fn extreme<T: Number>(numbers: &Numbers<T>, row: usize, max: bool) -> T {
    let mut best: Option<T> = None;
    for index in 0..numbers.0.len() {
        let Some(value) = numbers.get(index, row) else {
            continue;
        };
        if value.is_nan() {
            return value;
        }
        let better = best.is_none_or(|best| if max { value > best } else { value < best });
        if better {
            best = Some(value);
        }
    }
    best.unwrap_or(T::NULL)
}

/// Behavior: `Numeric#lowerBin(T, T)`, `#upperBin(T, T)` and their overloads with an offset.
fn bin<T: Number>(numbers: &Numbers<T>, row: usize, upper: bool) -> Result<T, TableFailure> {
    let (Some(value), Some(interval)) = (numbers.get(0, row), numbers.get(1, row)) else {
        return Ok(T::NULL);
    };
    // With an offset the value is `(T) (value - offset)`, and a null offset is not checked.
    let offset = (numbers.0.len() == 3).then(|| numbers.raw(2, row));
    let value = offset.map_or(value, |offset| value.minus(offset));
    // The shifted value can be the reserved value, which the inner call takes for null.
    if value == T::NULL {
        return Ok(offset.map_or(T::NULL, |offset| T::NULL.plus(offset)));
    }
    if !interval.is_positive() {
        return Err(failure(format!("Interval is not positive: {interval}")));
    }
    let bin = value.bin(interval, upper);
    Ok(offset.map_or(bin, |offset| bin.plus(offset)))
}

fn numeric<T: Number>(
    kernel: Kernel,
    arguments: &[ArrayRef],
    conditions: Option<&BooleanArray>,
) -> Result<ArrayRef, TableFailure> {
    let numbers = Numbers::<T>::new(arguments);
    Ok(match kernel {
        // Behavior: `Numeric#abs`: null for null.
        Kernel::Abs => numbers.map(|row| numbers.get(0, row).map_or(T::NULL, Number::abs)),
        // Behavior: `Numeric#signum`: `Integer.signum((int) value)`, null for null.
        Kernel::Signum => Arc::new(
            (0..numbers.rows())
                .map(|row| numbers.get(0, row).map(|value| value.int().signum()))
                .collect::<Int32Array>(),
        ),
        // Behavior: `Numeric#round`: `Math.round(value)`, null for null. A result equal to
        // the null `long` is null.
        Kernel::Round => Arc::new(
            (0..numbers.rows())
                .map(|row| {
                    numbers
                        .get(0, row)
                        .map(Number::round)
                        .filter(|value| *value != i64::MIN)
                })
                .collect::<Int64Array>(),
        ),
        // Behavior: `Numeric#isNaN`, `#isInf`: false for null and for every integer.
        Kernel::IsNaN => numbers.test(|row| numbers.get(0, row).is_some_and(Number::is_nan)),
        Kernel::IsInf => numbers.test(|row| numbers.get(0, row).is_some_and(Number::is_infinite)),
        // Behavior: `Numeric#isFinite`: false for null.
        Kernel::IsFinite => numbers.test(|row| {
            numbers
                .get(0, row)
                .is_some_and(|value| !value.is_nan() && !value.is_infinite())
        }),
        Kernel::Min => numbers.map(|row| extreme(&numbers, row, false)),
        Kernel::Max => numbers.map(|row| extreme(&numbers, row, true)),
        // Behavior: `Numeric#clamp`: `Require.leq(min, max)` first, on the values as Java
        // holds them, then null for a null value.
        Kernel::Clamp => numbers.try_map(|row| {
            let (min, max) = (numbers.raw(1, row), numbers.raw(2, row));
            // Not `min > max`: a NaN bound fails too.
            if !matches!(
                min.partial_cmp(&max),
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
            ) {
                return Err(failure(format!(
                    "clamp: min ({min}) is greater than max ({max})"
                )));
            }
            Ok(match numbers.get(0, row) {
                None => T::NULL,
                Some(value) if value < min => min,
                Some(value) if value > max => max,
                Some(value) => value,
            })
        })?,
        // Behavior: `Basic#inRange(T, T, T)`: false for a null value; the bounds are
        // compared as Java holds them.
        Kernel::InRange => numbers.test(|row| {
            numbers
                .get(0, row)
                .is_some_and(|value| value >= numbers.raw(1, row) && value <= numbers.raw(2, row))
        }),
        // Behavior: `Basic#in(T, T...)`: Java's `==` on the values as it holds them, so
        // null is in a list that has a null, and NaN is in no list.
        Kernel::In => numbers.test(|row| {
            let value = numbers.raw(0, row);
            (1..numbers.0.len()).any(|index| numbers.raw(index, row) == value)
        }),
        // Behavior: `Basic#replaceIfNull(T, T)`.
        Kernel::ReplaceIfNull => {
            numbers.map(|row| numbers.get(0, row).unwrap_or_else(|| numbers.raw(1, row)))
        }
        // Behavior: `Numeric#replaceIfNaN`, `#replaceIfNullNaN` and `#replaceIfNonFinite`,
        // where null is not finite.
        Kernel::ReplaceIfNaN | Kernel::ReplaceIfNullNaN | Kernel::ReplaceIfNonFinite => numbers
            .map(|row| {
                let replace = match numbers.get(0, row) {
                    None => kernel != Kernel::ReplaceIfNaN,
                    Some(value) => {
                        value.is_nan()
                            || (kernel == Kernel::ReplaceIfNonFinite && value.is_infinite())
                    }
                };
                numbers.raw(usize::from(replace), row)
            }),
        Kernel::LowerBin => numbers.try_map(|row| bin(&numbers, row, false))?,
        Kernel::UpperBin => numbers.try_map(|row| bin(&numbers, row, true))?,
        // Behavior: `Basic#ifelse(Boolean, T, T)`: null for a null condition.
        Kernel::IfElse => {
            let conditions = conditions.expect("ifelse has a condition");
            numbers.map(
                |row| match conditions.is_valid(row).then(|| conditions.value(row)) {
                    None => T::NULL,
                    Some(true) => numbers.raw(0, row),
                    Some(false) => numbers.raw(1, row),
                },
            )
        }
        other => unreachable!("{other:?} is not a function of numbers of one type"),
    })
}

impl Math {
    pub(super) fn apply(self, value: f64) -> f64 {
        match self {
            Math::Acos => value.acos(),
            Math::Asin => value.asin(),
            Math::Atan => value.atan(),
            Math::Ceil => value.ceil(),
            Math::Cos => value.cos(),
            Math::Cosh => value.cosh(),
            Math::Exp => value.exp(),
            Math::Expm1 => value.exp_m1(),
            Math::Floor => value.floor(),
            Math::Log => value.ln(),
            Math::Log10 => value.log10(),
            Math::Log1p => value.ln_1p(),
            Math::Rint => value.round_ties_even(),
            Math::Sin => value.sin(),
            Math::Sinh => value.sinh(),
            Math::Sqrt => value.sqrt(),
            Math::Tan => value.tan(),
            Math::Tanh => value.tanh(),
            Math::ToDegrees => value.to_degrees(),
            Math::ToRadians => value.to_radians(),
        }
    }
}

/// A `double` column, where the reserved value is null.
fn doubles(values: impl Iterator<Item = Option<f64>>) -> ArrayRef {
    Arc::new(
        values
            .map(|value| value.filter(|value| *value != -f64::MAX))
            .collect::<Float64Array>(),
    )
}

/// Behavior: `java.lang.Integer#parseInt(String)` and its relatives for the other types: an
/// optional sign and decimal digits, within `min..=max`.
pub(super) fn parse_integer(text: &str, min: i64, max: i64) -> Result<i64, TableFailure> {
    let invalid = || {
        failure(format!(
            "NumberFormatException: For input string: \"{text}\""
        ))
    };
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    // Java also takes the digits of other scripts, which this does not convert.
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let mut value: i128 = 0;
    for byte in digits.bytes() {
        value = value * 10 + i128::from(byte - b'0');
        if value > i128::from(u64::MAX) {
            return Err(invalid());
        }
    }
    if text.starts_with('-') {
        value = -value;
    }
    i64::try_from(value)
        .ok()
        .filter(|value| (min..=max).contains(value))
        .ok_or_else(invalid)
}

/// The first epoch second `DateTimeUtils#epochNanos` refuses to convert to nanoseconds.
const MAX_CONVERTIBLE_SECONDS: i128 = (i64::MAX / 1_000_000_000) as i128;

/// The nanoseconds of the instant `nanos` after the epoch, as a column holds an instant.
///
/// Behavior: `DateTimeUtils#epochNanos(Instant)`, which every instant passes through on its
/// way into a column: it refuses an instant from epoch second `Long.MAX_VALUE / 10^9` on,
/// and lets one before the range of a `long` wrap.
fn store_instant(nanos: i128) -> Result<i64, TableFailure> {
    let seconds = nanos.div_euclid(1_000_000_000);
    if seconds >= MAX_CONVERTIBLE_SECONDS {
        return Err(failure(format!(
            "Numeric overflow detected during conversion of {seconds} to nanoseconds"
        )));
    }
    Ok(nanos as i64)
}

/// Behavior: `DateTimeUtils#checkUnderflowMinus` and `#checkOverflowPlus`, which
/// `DateTimeUtils#minus(Instant, Instant)` subtracts with. The checks do not catch every
/// overflow, and what they miss wraps.
fn checked_minus(a: i64, b: i64) -> Result<i64, TableFailure> {
    if a < 0 && b > 0 && i64::MIN.wrapping_add(b) > a.wrapping_neg() {
        return Err(failure(format!(
            "Subtracting {b} nanos from {a} would underflow"
        )));
    }
    if a > 0 && b < 0 {
        let b = b.wrapping_neg();
        if b > 0 && i64::MAX - a < b {
            return Err(failure(format!(
                "Subtracting {} nanos from {a} would overflow",
                b.wrapping_neg()
            )));
        }
        return Ok(a.wrapping_add(b));
    }
    Ok(a.wrapping_sub(b))
}

fn instants(
    values: impl Iterator<Item = Result<Option<i64>, TableFailure>>,
) -> Result<ArrayRef, TableFailure> {
    let values = values
        .map(|value| Ok(value?.filter(|nanos| *nanos != i64::MIN)))
        .collect::<Result<TimestampNanosecondArray, TableFailure>>()?;
    Ok(Arc::new(
        values.with_data_type(ColumnType::Instant.arrow_type()),
    ))
}

fn time(kernel: Kernel, arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let rows = arguments[0].len();
    let longs = |index: usize| arguments[index].as_primitive::<Int64Type>();
    let stamps = |index: usize| arguments[index].as_primitive::<TimestampNanosecondType>();
    let stamp = |index: usize, row: usize| {
        let array = stamps(index);
        array.is_valid(row).then(|| array.value(row))
    };
    let long = |index: usize, row: usize| {
        let array = longs(index);
        array.is_valid(row).then(|| array.value(row))
    };
    match kernel {
        // Behavior: `DateTimeUtils#epochNanos(Instant)`: null for null.
        Kernel::EpochNanos => Ok(Arc::new(
            (0..rows).map(|row| stamp(0, row)).collect::<Int64Array>(),
        )),
        // Behavior: `DateTimeUtils#epochSeconds(Instant)` and `#epochMillis(Instant)`, which
        // are `Instant#getEpochSecond` and `Instant#toEpochMilli` and round toward the
        // past, and `#epochMicros(Instant)`, which divides the nanoseconds and so rounds
        // toward the epoch. Null for null.
        Kernel::Epoch(unit) => Ok(Arc::new(
            (0..rows)
                .map(|row| {
                    stamp(0, row).map(|nanos| match unit {
                        EpochUnit::Seconds => nanos.div_euclid(1_000_000_000),
                        EpochUnit::Millis => nanos.div_euclid(1_000_000),
                        EpochUnit::Micros => nanos / 1_000,
                    })
                })
                .collect::<Int64Array>(),
        )),
        // Behavior: `DateTimeUtils#epochSecondsToInstant(long)`, `#epochMillisToInstant`
        // and `#epochMicrosToInstant`: null for null, otherwise the instant, which a
        // column holds only as a `long` of nanoseconds.
        Kernel::EpochToInstant(unit) => instants((0..rows).map(|row| {
            let Some(count) = long(0, row) else {
                return Ok(None);
            };
            let nanos = i128::from(count)
                * match unit {
                    EpochUnit::Seconds => 1_000_000_000,
                    EpochUnit::Millis => 1_000_000,
                    EpochUnit::Micros => 1_000,
                };
            if nanos < i128::from(i64::MIN) {
                return Err(failure(format!(
                    "java.lang.ArithmeticException: long overflow storing the instant of {count}"
                )));
            }
            store_instant(nanos).map(Some)
        })),
        // Behavior: `DateTimeUtils#epochNanosToInstant(long)`: null for null.
        Kernel::EpochNanosToInstant => instants((0..rows).map(|row| {
            long(0, row)
                .map(|nanos| store_instant(nanos.into()))
                .transpose()
        })),
        // Behavior: `DateTimeUtils#plus(Instant, long)` and `#minus(Instant, long)`: null
        // when either is null, otherwise `Instant#plusNanos`, which is exact.
        Kernel::InstantPlus | Kernel::InstantMinus => instants((0..rows).map(|row| {
            let (Some(instant), Some(nanos)) = (stamp(0, row), long(1, row)) else {
                return Ok(None);
            };
            let nanos = if kernel == Kernel::InstantPlus {
                i128::from(nanos)
            } else {
                -i128::from(nanos)
            };
            store_instant(i128::from(instant) + nanos).map(Some)
        })),
        // Behavior: `DateTimeUtils#multiply(Duration, long)`, `#multiply(Period, int)` and
        // `#divide(Duration, long)`: null when either is null, otherwise
        // `Duration#multipliedBy`, `Period#multipliedBy` and `Duration#dividedBy`, which are
        // exact and fail on overflow and on a zero divisor. An amount is held here as a
        // `long` of nanoseconds; Java can hold a longer one, but cannot add it to an
        // `Instant` that a column holds, so an amount that overflows the `long` fails here.
        Kernel::DurationMultiply | Kernel::DurationDivide => {
            let values = (0..rows)
                .map(|row| {
                    let (Some(amount), Some(scalar)) = (long(0, row), long(1, row)) else {
                        return Ok(None);
                    };
                    let result = if kernel == Kernel::DurationMultiply {
                        amount.checked_mul(scalar)
                    } else if scalar == 0 {
                        return Err(failure(
                            "java.lang.ArithmeticException: Cannot divide by zero",
                        ));
                    } else {
                        amount.checked_div(scalar)
                    };
                    match result {
                        Some(nanos) if nanos != i64::MIN => Ok(Some(nanos)),
                        _ => Err(failure(
                            "java.lang.ArithmeticException: long overflow in a duration or period",
                        )),
                    }
                })
                .collect::<Result<Int64Array, TableFailure>>()?;
            Ok(Arc::new(values))
        }
        // Behavior: `DateTimeUtils#minus(Instant, Instant)`: null when either is null.
        Kernel::InstantDifference => {
            let values = (0..rows)
                .map(|row| match (stamp(0, row), stamp(1, row)) {
                    (Some(a), Some(b)) => {
                        Ok(Some(checked_minus(a, b)?).filter(|nanos| *nanos != i64::MIN))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array, TableFailure>>()?;
            Ok(Arc::new(values))
        }
        // Behavior: `DateTimeUtils#isBefore(Instant, Instant)` and its relatives: false when
        // either is null.
        Kernel::TimeOrder(order) => Ok(Arc::new(
            (0..rows)
                .map(|row| {
                    let (Some(a), Some(b)) = (stamp(0, row), stamp(1, row)) else {
                        return Some(false);
                    };
                    Some(match order {
                        TimeOrder::Before => a < b,
                        TimeOrder::BeforeOrEqual => a <= b,
                        TimeOrder::After => a > b,
                        TimeOrder::AfterOrEqual => a >= b,
                    })
                })
                .collect::<BooleanArray>(),
        )),
        // Behavior: `DateTimeUtils#lowerBin(Instant, long)`, `#lowerBin(Instant, long,
        // long)` and the same of `#upperBin`: null when any argument is null, otherwise
        // `Numeric#lowerBin(long, long)` of the nanoseconds, less the offset, plus the offset.
        Kernel::InstantLowerBin | Kernel::InstantUpperBin => instants((0..rows).map(|row| {
            let (Some(instant), Some(interval)) = (stamp(0, row), long(1, row)) else {
                return Ok(None);
            };
            let offset = if arguments.len() == 3 {
                match long(2, row) {
                    Some(offset) => offset,
                    None => return Ok(None),
                }
            } else {
                0
            };
            let value = instant.wrapping_sub(offset);
            if value == i64::MIN {
                return store_instant(i64::MIN.wrapping_add(offset).into()).map(Some);
            }
            if interval <= 0 {
                return Err(failure(format!("Interval is not positive: {interval}")));
            }
            let bin = value.bin(interval, kernel == Kernel::InstantUpperBin);
            store_instant(bin.wrapping_add(offset).into()).map(Some)
        })),
        other => unreachable!("{other:?} is not a function of time"),
    }
}

/// Evaluates a call whose arguments have the parameter types of [`super::Call`].
pub(crate) fn evaluate(kernel: Kernel, arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let rows = arguments[0].len();
    match kernel {
        // Behavior: `Numeric#sqrt` and the other functions of one `double`: null for null.
        Kernel::Math(math) => {
            let values = arguments[0].as_primitive::<Float64Type>();
            Ok(doubles(
                values.iter().map(|value| value.map(|v| math.apply(v))),
            ))
        }
        // Behavior: `Numeric#pow(T, U)`: `Math.pow` of both widened to `double`, null when
        // either is null. `Numeric#atan2(T, T)`: likewise `Math.atan2`.
        Kernel::Pow | Kernel::Atan2 => {
            let a = numeric::cast::<f64>(arguments[0].as_ref());
            let b = numeric::cast::<f64>(arguments[1].as_ref());
            Ok(doubles(a.iter().zip(b.iter()).map(|pair| match pair {
                (Some(a), Some(b)) if kernel == Kernel::Pow => Some(java_pow(a, b)),
                (Some(a), Some(b)) => Some(a.atan2(b)),
                _ => None,
            })))
        }
        // The generic `Basic#replaceIfNull(T, T)`.
        Kernel::ReplaceIfNull
            if !matches!(
                arguments[0].data_type(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::UInt16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
            ) =>
        {
            let is_null = arrow::compute::is_null(arguments[0].as_ref())
                .map_err(|error| failure(error.to_string()))?;
            arrow::compute::kernels::zip::zip(&is_null, &arguments[1], &arguments[0])
                .map_err(|error| failure(error.to_string()))
        }
        Kernel::IfElse => {
            let conditions = arguments[0].as_boolean();
            in_number_type!(arguments[1], |T| numeric::<T>(
                kernel,
                &arguments[1..],
                Some(conditions)
            ))
        }
        // Behavior: `Logic#and(Boolean...)` and `#or(Boolean...)`: the values are unboxed
        // in order until one decides, so a null before that throws.
        Kernel::And | Kernel::Or => {
            let decides = kernel == Kernel::Or;
            let values = (0..rows)
                .map(|row| {
                    for argument in arguments {
                        let argument = argument.as_boolean();
                        if argument.is_null(row) {
                            return Err(failure(
                                "NullPointerException: a null Boolean in and() or or()",
                            ));
                        }
                        if argument.value(row) == decides {
                            return Ok(Some(decides));
                        }
                    }
                    Ok(Some(!decides))
                })
                .collect::<Result<BooleanArray, TableFailure>>()?;
            Ok(Arc::new(values))
        }
        // Behavior: `Parse#parseInt(String)` and its relatives: null for null, otherwise
        // `Integer.parseInt`, which throws for anything but a number in range.
        // `Parse#parseBoolean(String)`: whether the text is `true`, in any case.
        Kernel::Parse(parsed) => {
            let texts = arguments[0].as_string::<i32>();
            // Behavior: `Parse#parseDouble(String)` and `#parseFloat(String)`: null for
            // null, otherwise `Double.parseDouble`. The value that stands for null parses,
            // and is then null.
            if parsed == Parsed::Double {
                let values = texts
                    .iter()
                    .map(|text| {
                        let value = text.map(super::statics::parse_double).transpose()?;
                        Ok(value.filter(|value| *value != -f64::MAX))
                    })
                    .collect::<Result<Float64Array, TableFailure>>()?;
                return Ok(Arc::new(values));
            }
            if parsed == Parsed::Float {
                let values = texts
                    .iter()
                    .map(|text| {
                        let value = text.map(super::statics::parse_float).transpose()?;
                        Ok(value.filter(|value| *value != -f32::MAX))
                    })
                    .collect::<Result<arrow::array::Float32Array, TableFailure>>()?;
                return Ok(Arc::new(values));
            }
            if parsed == Parsed::Boolean {
                return Ok(Arc::new(
                    texts
                        .iter()
                        .map(|text| text.map(|text| text.eq_ignore_ascii_case("true")))
                        .collect::<BooleanArray>(),
                ));
            }
            let (min, max, result) = match parsed {
                Parsed::Byte => (i8::MIN.into(), i8::MAX.into(), ColumnType::Byte),
                Parsed::Short => (i16::MIN.into(), i16::MAX.into(), ColumnType::Short),
                Parsed::Int => (i32::MIN.into(), i32::MAX.into(), ColumnType::Int),
                _ => (i64::MIN, i64::MAX, ColumnType::Long),
            };
            let values = texts
                .iter()
                .map(|text| text.map(|text| parse_integer(text, min, max)).transpose())
                .collect::<Result<Int64Array, TableFailure>>()?;
            // The lowest value of the type parses, and is then the type's null.
            Ok(match result {
                ColumnType::Byte => Arc::new(numeric::cast::<i8>(&values)) as ArrayRef,
                ColumnType::Short => Arc::new(numeric::cast::<i16>(&values)),
                ColumnType::Int => Arc::new(numeric::cast::<i32>(&values)),
                _ => Arc::new(numeric::cast::<i64>(&values)),
            })
        }
        Kernel::ParseInstant { quiet } => parse_instants(arguments[0].as_ref(), quiet),
        Kernel::InstantMethod(method) => instant_method(method, arguments[0].as_ref()),
        Kernel::Zoned(field, zone) => zoned(field, zone, arguments),
        Kernel::CompareTo => compare_to(arguments),
        Kernel::EpochNanos
        | Kernel::Epoch(_)
        | Kernel::EpochToInstant(_)
        | Kernel::EpochNanosToInstant
        | Kernel::InstantPlus
        | Kernel::InstantMinus
        | Kernel::InstantDifference
        | Kernel::DurationMultiply
        | Kernel::DurationDivide
        | Kernel::TimeOrder(_)
        | Kernel::InstantLowerBin
        | Kernel::InstantUpperBin => time(kernel, arguments),
        Kernel::Vector(function) => super::vectors::evaluate(function, arguments),
        Kernel::ArrayOf => array_of(arguments),
        _ => in_number_type!(arguments[0], |T| numeric::<T>(kernel, arguments, None)),
    }
}

/// Behavior: `DateTimeUtils#parseInstant(String)`, which throws for null and for text it
/// cannot read, and `#parseInstantQuiet(String)`, which is null for both and for text of
/// one character or none. Text Java reads in a spelling this server does not is a
/// failure for both.
fn parse_instants(texts: &dyn Array, quiet: bool) -> Result<ArrayRef, TableFailure> {
    let texts = texts.as_string::<i32>();
    instants(texts.iter().map(|text| {
        let Some(text) = text else {
            return if quiet {
                Ok(None)
            } else {
                Err(failure(
                    "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse \
                     instant (null): null",
                ))
            };
        };
        if quiet && text.chars().count() <= 1 {
            return Ok(None);
        }
        match crate::parse::instant_of_text(text) {
            Ok(nanos) => store_instant(nanos.into()).map(Some),
            Err(oxhaven_contracts::expr::FormulaError::Unsupported(_)) => Err(failure(format!(
                "parseInstant of \"{text}\", a spelling of an instant this server does not \
                 read (io.deephaven.time.DateTimeUtils#parseInstant)"
            ))),
            Err(_) if quiet => Ok(None),
            Err(_) => Err(failure(format!(
                "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse \
                 instant: {text}"
            ))),
        }
    }))
}

/// Behavior: `QueryLanguageFunctionUtils#compareTo(int, long)` and its relatives: null is
/// equal to null and below everything else, whole numbers compare as numbers, and
/// floating point numbers as `Double#compare` and `Float#compare` order them, with `-0.0`
/// below `0.0` and NaN above all. The arguments arrive as two `long`, two `float` or two
/// `double` values.
fn compare_to(arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    use std::cmp::Ordering;
    fn order<T: Copy>(a: Option<T>, b: Option<T>, compare: impl Fn(T, T) -> i32) -> i32 {
        match (a, b) {
            (None, None) => 0,
            (None, Some(_)) => -1,
            (Some(_), None) => 1,
            (Some(a), Some(b)) => compare(a, b),
        }
    }
    let whole = |a: i64, b: i64| match a.cmp(&b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };
    let floating = super::statics::java_compare;
    let values: Int32Array = match arguments[0].data_type() {
        DataType::Int64 => {
            let (a, b) = (
                arguments[0].as_primitive::<Int64Type>(),
                arguments[1].as_primitive::<Int64Type>(),
            );
            a.iter()
                .zip(b)
                .map(|(a, b)| Some(order(a, b, whole)))
                .collect()
        }
        DataType::Float32 => {
            let (a, b) = (
                arguments[0].as_primitive::<arrow::datatypes::Float32Type>(),
                arguments[1].as_primitive::<arrow::datatypes::Float32Type>(),
            );
            a.iter()
                .zip(b)
                .map(|(a, b)| Some(order(a, b, |a, b| floating(a.into(), b.into()))))
                .collect()
        }
        _ => {
            let (a, b) = (
                arguments[0].as_primitive::<Float64Type>(),
                arguments[1].as_primitive::<Float64Type>(),
            );
            a.iter()
                .zip(b)
                .map(|(a, b)| Some(order(a, b, floating)))
                .collect()
        }
    };
    Ok(Arc::new(values))
}

/// Behavior: `DateTimeUtils#hourOfDay(Instant, ZoneId, boolean)`, `#dayOfYear(Instant,
/// ZoneId)` and their relatives: null for a null instant, otherwise the field of the date
/// and time the instant is in the zone. A function of the day counts from midnight on
/// the clock when its flag is true, and otherwise the time that passed since the day
/// began (`DateTimeUtils#atMidnight`), which differs on a day the clocks change. A null
/// flag is unboxed, and that throws.
fn zoned(field: ZonedField, zone: Zone, arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let instants = arguments[0].as_primitive::<TimestampNanosecondType>();
    let on_the_clock = arguments.get(1).map(|flags| flags.as_boolean());
    if on_the_clock.is_some_and(|flags| flags.null_count() > 0) {
        return Err(failure(
            "java.lang.NullPointerException: Cannot invoke \
             \"java.lang.Boolean.booleanValue()\" because the value is null",
        ));
    }
    let outside = || failure("an instant outside the range of the time zone rules");
    let values = (0..instants.len())
        .map(|row| {
            if instants.is_null(row) {
                return Ok(None);
            }
            let nanos = instants.value(row);
            let seconds = nanos.div_euclid(1_000_000_000);
            let fraction = nanos.rem_euclid(1_000_000_000);
            let offset = match zone {
                Zone::Utc => 0,
                Zone::Region(region) => {
                    use chrono::{Offset, TimeZone};
                    let utc = chrono::DateTime::from_timestamp(seconds, 0)
                        .ok_or_else(outside)?
                        .naive_utc();
                    i64::from(
                        region
                            .offset_from_utc_datetime(&utc)
                            .fix()
                            .local_minus_utc(),
                    )
                }
            };
            let local = seconds + offset;
            let days = local.div_euclid(86_400);
            let clock = local.rem_euclid(86_400);
            let (year, month, day) = civil_from_days(days);
            let of_day = || -> Result<i64, TableFailure> {
                if on_the_clock.is_none_or(|flags| flags.value(row)) {
                    return Ok(clock * 1_000_000_000 + fraction);
                }
                let start = match zone {
                    Zone::Utc => days * 86_400,
                    Zone::Region(region) => {
                        zoned_seconds(&region, (year, month, day), (0, 0, 0)).ok_or_else(outside)?
                    }
                };
                Ok(nanos - start * 1_000_000_000)
            };
            Ok(Some(match field {
                ZonedField::SecondOfDay => of_day()? / 1_000_000_000,
                ZonedField::MinuteOfDay => of_day()? / 60_000_000_000,
                ZonedField::HourOfDay => of_day()? / 3_600_000_000_000,
                ZonedField::MillisOfDay => of_day()? / 1_000_000,
                ZonedField::NanosOfDay => of_day()?,
                ZonedField::Year => year,
                ZonedField::YearOfCentury => year.rem_euclid(100),
                ZonedField::MonthOfYear => month,
                ZonedField::DayOfYear => days - days_from_civil(year, 1, 1) + 1,
                ZonedField::DayOfMonth => day,
                // The first day of 1970 was a Thursday, and Monday is 1.
                ZonedField::DayOfWeekValue => (days + 3).rem_euclid(7) + 1,
                ZonedField::MinuteOfHour => clock / 60 % 60,
                ZonedField::SecondOfMinute => clock % 60,
                ZonedField::MillisOfSecond => fraction / 1_000_000,
                ZonedField::MicrosOfSecond => fraction / 1_000,
                ZonedField::NanosOfSecond => fraction,
            }))
        })
        .collect::<Result<Int64Array, TableFailure>>()?;
    Ok(match field.result_type() {
        ColumnType::Long => Arc::new(values),
        _ => Arc::new(numeric::cast::<i32>(&values)),
    })
}

/// Behavior: `java.time.Instant#getEpochSecond`, `#toEpochMilli` and `#toString`, called
/// on the value, so a null value is a `NullPointerException`. The text is ISO-8601 in UTC
/// with the fraction of a second in no, three, six or nine digits.
fn instant_method(method: InstantMethod, instants: &dyn Array) -> Result<ArrayRef, TableFailure> {
    let instants = instants.as_primitive::<TimestampNanosecondType>();
    if instants.null_count() > 0 {
        return Err(failure(
            "java.lang.NullPointerException: method call on a null Instant",
        ));
    }
    let values = instants.values().iter();
    Ok(match method {
        InstantMethod::GetEpochSecond => Arc::new(Int64Array::from_iter_values(
            values.map(|nanos| nanos.div_euclid(1_000_000_000)),
        )),
        InstantMethod::ToEpochMilli => Arc::new(Int64Array::from_iter_values(
            values.map(|nanos| nanos.div_euclid(1_000_000)),
        )),
        InstantMethod::ToString => Arc::new(arrow::array::StringArray::from_iter_values(
            values.map(|nanos| {
                let seconds = nanos.div_euclid(1_000_000_000);
                let fraction = nanos.rem_euclid(1_000_000_000);
                let (year, month, day) =
                    crate::parse::instant::civil_from_days(seconds.div_euclid(86_400));
                let clock = seconds.rem_euclid(86_400);
                let fraction = if fraction == 0 {
                    String::new()
                } else if fraction % 1_000_000 == 0 {
                    format!(".{:03}", fraction / 1_000_000)
                } else if fraction % 1_000 == 0 {
                    format!(".{:06}", fraction / 1_000)
                } else {
                    format!(".{fraction:09}")
                };
                format!(
                    "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}{fraction}Z",
                    clock / 3_600,
                    clock / 60 % 60,
                    clock % 60
                )
            }),
        )),
    })
}

/// One array per row, of that row's value of each argument in order.
///
/// Behavior: Java's array creation with an initializer, which
/// `QueryLanguageParser#visit(ArrayCreationExpr)` prints as it is. An element that holds
/// the value Java reserves for null in the element type is a null element, as it is in any
/// array a client is sent.
fn array_of(arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let rows = arguments[0].len();
    let width = arguments.len();
    if rows
        .checked_mul(width)
        .is_none_or(|elements| i32::try_from(elements).is_err())
    {
        return Err(failure("a column of arrays holds more than 2^31 elements"));
    }
    let arrays: Vec<&dyn Array> = arguments.iter().map(|array| array.as_ref()).collect();
    let indices: Vec<(usize, usize)> = (0..rows)
        .flat_map(|row| (0..width).map(move |argument| (argument, row)))
        .collect();
    let elements = arrow::compute::interleave(&arrays, &indices)
        .map_err(|error| failure(error.to_string()))?;
    let field = Arc::new(Field::new("", elements.data_type().clone(), true));
    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
        elements,
        None,
    )))
}

/// `java.lang.Math#pow`, which differs from C's `pow` in two cases: with a NaN exponent the
/// result is NaN even for a base of one, and one to an infinite power is NaN.
pub(super) fn java_pow(base: f64, exponent: f64) -> f64 {
    if exponent.is_nan() || (base.abs() == 1.0 && exponent.is_infinite()) {
        f64::NAN
    } else {
        base.powf(exponent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_is_half_up_and_saturates() {
        assert_eq!(round_double(2.5), 3);
        assert_eq!(round_double(-2.5), -2);
        assert_eq!(round_double(-2.51), -3);
        // Adding one half first would round this up to 1.
        assert_eq!(round_double(0.499_999_999_999_999_94), 0);
        assert_eq!(round_double(f64::NAN), 0);
        assert_eq!(round_double(f64::INFINITY), i64::MAX);
        assert_eq!(round_float(1.5), 2);
        assert_eq!(round_float(-0.5), 0);
        assert_eq!(round_float(f32::NEG_INFINITY), i32::MIN);
        // An int goes through `round(float)`, which cannot hold it exactly.
        assert_eq!(2_147_483_647_i32.round(), 2_147_483_647);
        assert_eq!(16_777_217_i32.round(), 16_777_216);
        assert_eq!(i64::MAX.round(), 2_147_483_647);
    }

    #[test]
    fn integer_bins_are_multiples_at_or_below_and_at_or_above() {
        assert_eq!(7_i32.bin(5, false), 5);
        assert_eq!(7_i32.bin(5, true), 10);
        assert_eq!((-7_i32).bin(5, false), -10);
        assert_eq!((-7_i32).bin(5, true), -5);
        assert_eq!(10_i64.bin(5, false), 10);
        assert_eq!(10_i64.bin(5, true), 10);
        assert_eq!((-10_i8).bin(5, false), -10);
        // `(byte) (100 * 2)` keeps the low bits.
        assert_eq!(127_i8.bin(100, true), -56);
    }

    #[test]
    fn floating_bins_divide_in_their_own_type() {
        assert_eq!(7.5_f64.bin(2.0, false), 6.0);
        assert_eq!(7.5_f64.bin(2.0, true), 8.0);
        assert_eq!((-7.5_f32).bin(2.0, false), -8.0);
        assert!(f64::NAN.bin(2.0, false).is_nan());
    }

    #[test]
    fn integers_parse_as_java_parses_them() {
        assert_eq!(parse_integer("12", -128, 127).ok(), Some(12));
        assert_eq!(parse_integer("+7", -128, 127).ok(), Some(7));
        assert_eq!(parse_integer("-0", -128, 127).ok(), Some(0));
        assert_eq!(parse_integer("-128", -128, 127).ok(), Some(-128));
        assert_eq!(
            parse_integer("-9223372036854775808", i64::MIN, i64::MAX).ok(),
            Some(i64::MIN)
        );
        for text in ["", "+", "-", " 7", "7 ", "1.0", "128", "0x10", "1_0", "--1"] {
            assert!(parse_integer(text, -128, 127).is_err(), "{text}");
        }
        assert!(parse_integer("9223372036854775808", i64::MIN, i64::MAX).is_err());
        assert!(parse_integer("99999999999999999999999999", i64::MIN, i64::MAX).is_err());
    }

    #[test]
    fn an_instant_is_stored_only_below_the_last_convertible_second() {
        assert_eq!(store_instant(5).ok(), Some(5));
        assert_eq!(store_instant(-1).ok(), Some(-1));
        assert_eq!(
            store_instant(9_223_372_035_999_999_999).ok(),
            Some(9_223_372_035_999_999_999)
        );
        assert!(store_instant(9_223_372_036_000_000_000).is_err());
        assert!(store_instant(i128::from(i64::MAX) + 1).is_err());
    }

    #[test]
    fn pow_follows_java_where_c_differs() {
        assert!(java_pow(1.0, f64::NAN).is_nan());
        assert!(java_pow(1.0, f64::INFINITY).is_nan());
        assert!(java_pow(-1.0, f64::NEG_INFINITY).is_nan());
        assert_eq!(java_pow(f64::NAN, 0.0), 1.0);
        assert_eq!(java_pow(2.0, 10.0), 1024.0);
    }
}
