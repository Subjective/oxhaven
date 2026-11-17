//! Java's primitive numeric types: conversions between all seven of them, and arithmetic in
//! the four Java computes in.

use crate::types::{ArithmeticOp, BitwiseOp};
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::ArrowNativeType;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit, TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

/// A Java primitive numeric type: `byte`, `short`, `char`, `int`, `long`, `float` or
/// `double`.
pub(crate) trait JavaPrimitive: ArrowNativeType + PartialEq {
    type Arrow: ArrowPrimitiveType<Native = Self>;

    /// The value Java stores for null. A result equal to it is null.
    ///
    /// Behavior: `io.deephaven.util.QueryConstants#NULL_BYTE`, `#NULL_SHORT`, `#NULL_CHAR`,
    /// `#NULL_INT`, `#NULL_LONG`, `#NULL_FLOAT`, `#NULL_DOUBLE`.
    const NULL: Self;

    /// Java's conversion from an integer type or `char`, whose values all fit an `i64`:
    /// widening keeps the value, narrowing keeps the low bits, and a floating point target
    /// takes the nearest value.
    ///
    /// Behavior: the Java Language Specification sections 5.1.2 and 5.1.3.
    fn from_integer(value: i64) -> Self;

    /// Java's conversion from `float` or `double`; a `float` is passed as the `double` of
    /// the same value. An integer target takes the value rounded toward zero, the nearest
    /// bound when it is out of range, and zero for NaN; `byte`, `short` and `char` take the
    /// low bits of that conversion to `int`.
    ///
    /// Behavior: the Java Language Specification section 5.1.3.
    fn from_floating(value: f64) -> Self;
}

/// A type Java computes numbers in: `int`, `long`, `float` or `double`.
pub(crate) trait JavaNumber: JavaPrimitive {
    /// `a op b` as Java computes it once both operands have this type. `None` is Java's
    /// `ArithmeticException`.
    fn apply(op: ArithmeticOp, a: Self, b: Self) -> Option<Self>;

    fn negate(self) -> Self;
}

macro_rules! java_narrow_integer {
    ($native:ty, $arrow:ty, $null:expr) => {
        impl JavaPrimitive for $native {
            type Arrow = $arrow;
            const NULL: Self = $null;

            fn from_integer(value: i64) -> Self {
                value as $native
            }
            fn from_floating(value: f64) -> Self {
                (value as i32) as $native
            }
        }
    };
}

macro_rules! java_integer {
    ($native:ty, $arrow:ty) => {
        impl JavaPrimitive for $native {
            type Arrow = $arrow;
            const NULL: Self = <$native>::MIN;

            fn from_integer(value: i64) -> Self {
                value as $native
            }
            fn from_floating(value: f64) -> Self {
                value as $native
            }
        }

        impl JavaNumber for $native {
            /// Behavior: Java's `+`, `-`, `*`, `%`, `&`, `|` and `^` on this type: overflow
            /// wraps, the remainder takes the sign of the dividend, and a zero divisor
            /// throws.
            fn apply(op: ArithmeticOp, a: Self, b: Self) -> Option<Self> {
                match op {
                    ArithmeticOp::Add => Some(a.wrapping_add(b)),
                    ArithmeticOp::Subtract => Some(a.wrapping_sub(b)),
                    ArithmeticOp::Multiply => Some(a.wrapping_mul(b)),
                    ArithmeticOp::Remainder => (b != 0).then(|| a.wrapping_rem(b)),
                    ArithmeticOp::Bitwise(BitwiseOp::And) => Some(a & b),
                    ArithmeticOp::Bitwise(BitwiseOp::Or) => Some(a | b),
                    ArithmeticOp::Bitwise(BitwiseOp::Xor) => Some(a ^ b),
                    ArithmeticOp::Divide => {
                        unreachable!("division is always computed in float or double")
                    }
                }
            }

            fn negate(self) -> Self {
                self.wrapping_neg()
            }
        }
    };
}

macro_rules! java_floating {
    ($native:ty, $arrow:ty) => {
        impl JavaPrimitive for $native {
            type Arrow = $arrow;
            const NULL: Self = -<$native>::MAX;

            fn from_integer(value: i64) -> Self {
                value as $native
            }
            fn from_floating(value: f64) -> Self {
                value as $native
            }
        }

        impl JavaNumber for $native {
            /// Behavior: Java's floating point operators, each one IEEE 754 operation;
            /// `%` is the truncating remainder (C `fmod`).
            fn apply(op: ArithmeticOp, a: Self, b: Self) -> Option<Self> {
                Some(match op {
                    ArithmeticOp::Add => a + b,
                    ArithmeticOp::Subtract => a - b,
                    ArithmeticOp::Multiply => a * b,
                    ArithmeticOp::Divide => a / b,
                    ArithmeticOp::Remainder => a % b,
                    ArithmeticOp::Bitwise(_) => {
                        unreachable!("the type checker allows bitwise operators on integers only")
                    }
                })
            }

            fn negate(self) -> Self {
                -self
            }
        }
    };
}

java_narrow_integer!(i8, Int8Type, i8::MIN);
java_narrow_integer!(i16, Int16Type, i16::MIN);
java_narrow_integer!(u16, UInt16Type, u16::MAX);
java_integer!(i32, Int32Type);
java_integer!(i64, Int64Type);
java_floating!(f32, Float32Type);
java_floating!(f64, Float64Type);

/// Converts a numeric, char or instant array to `T` with Java's primitive conversions.
/// Nulls stay null, and no value becomes null.
pub(crate) fn convert<T: JavaPrimitive>(array: &dyn Array) -> PrimitiveArray<T::Arrow> {
    // An array of the target type already is the answer; cloning it shares its buffers.
    if let Some(same) = array.as_primitive_opt::<T::Arrow>() {
        return same.clone();
    }
    match array.data_type() {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .unary(|v| T::from_integer(v.into())),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .unary(|v| T::from_integer(v.into())),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .unary(|v| T::from_integer(v.into())),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .unary(|v| T::from_integer(v.into())),
        DataType::Int64 => array.as_primitive::<Int64Type>().unary(T::from_integer),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .unary(T::from_integer),
        // A `LocalDate`, `LocalTime` and `Duration`, which are compared as their numbers.
        DataType::Date32 => array
            .as_primitive::<arrow::datatypes::Date32Type>()
            .unary(|v| T::from_integer(v.into())),
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_primitive::<arrow::datatypes::Time64NanosecondType>()
            .unary(T::from_integer),
        DataType::Duration(TimeUnit::Nanosecond) => array
            .as_primitive::<arrow::datatypes::DurationNanosecondType>()
            .unary(T::from_integer),
        DataType::Float32 => array
            .as_primitive::<Float32Type>()
            .unary(|v| T::from_floating(v.into())),
        DataType::Float64 => array.as_primitive::<Float64Type>().unary(T::from_floating),
        other => unreachable!("the type checker passed a {other} array as a number"),
    }
}

/// `(T) operand` for an operand of another primitive type.
///
/// Behavior: `QueryLanguageFunctionUtils#intCast`, `#longCast`, `#byteCast`, `#shortCast`,
/// `#charCast`, `#floatCast` and `#doubleCast`: null in, null out, otherwise Java's cast.
/// A result equal to the target type's null value is null, because that is how Java stores
/// null: `(byte) 128` and `(char) -1` are null.
pub(crate) fn cast<T: JavaPrimitive>(operand: &dyn Array) -> PrimitiveArray<T::Arrow> {
    null_values_to_nulls::<T>(convert::<T>(operand))
}

/// `array` with every element that holds `T`'s null value made null: a computed value equal
/// to it is null, because that is how Java stores null.
fn null_values_to_nulls<T: JavaPrimitive>(
    array: PrimitiveArray<T::Arrow>,
) -> PrimitiveArray<T::Arrow> {
    if !array.values().iter().any(|value| *value == T::NULL) {
        return array;
    }
    let (_, values, nulls) = array.into_parts();
    let computed = NullBuffer::new(BooleanBuffer::collect_bool(values.len(), |row| {
        values[row] != T::NULL
    }));
    let nulls = NullBuffer::union(nulls.as_ref(), Some(&computed));
    PrimitiveArray::new(values, nulls)
}

/// `left op right` with both operands converted to `T`.
///
/// Behavior: `QueryLanguageFunctionUtils#plus`, `#minus`, `#multiply`, `#divide`,
/// `#remainder`, `#binaryAnd`, `#binaryOr` and `#xor`: null when either operand is null,
/// otherwise Java's operator on the operands cast to the result type. A result equal to the
/// type's null value is null, because that is how Java stores null. An integer remainder by
/// zero fails the evaluation, but only for a row where neither operand is null.
pub(crate) fn arithmetic<T: JavaNumber>(
    op: ArithmeticOp,
    left: &dyn Array,
    right: &dyn Array,
) -> Result<PrimitiveArray<T::Arrow>, TableFailure> {
    let left = convert::<T>(left);
    let right = convert::<T>(right);
    let nulls = NullBuffer::union(left.nulls(), right.nulls());
    let (a, b) = (left.values(), right.values());
    // An operator that cannot fail is computed for every row, also where an operand is
    // null: the validity of the result hides what that gives. Naming the operator in each
    // arm lets the compiler make one loop per operator.
    macro_rules! every_row {
        ($op:expr) => {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| T::apply($op, *a, *b).unwrap_or(T::NULL))
                .collect::<Vec<T>>()
        };
    }
    let values = match op {
        ArithmeticOp::Add => every_row!(ArithmeticOp::Add),
        ArithmeticOp::Subtract => every_row!(ArithmeticOp::Subtract),
        ArithmeticOp::Multiply => every_row!(ArithmeticOp::Multiply),
        ArithmeticOp::Divide => every_row!(ArithmeticOp::Divide),
        ArithmeticOp::Bitwise(BitwiseOp::And) => every_row!(ArithmeticOp::Bitwise(BitwiseOp::And)),
        ArithmeticOp::Bitwise(BitwiseOp::Or) => every_row!(ArithmeticOp::Bitwise(BitwiseOp::Or)),
        ArithmeticOp::Bitwise(BitwiseOp::Xor) => every_row!(ArithmeticOp::Bitwise(BitwiseOp::Xor)),
        // The one operator that can fail, and only where neither operand is null.
        ArithmeticOp::Remainder => {
            let mut values = Vec::with_capacity(a.len());
            for (row, (a, b)) in a.iter().zip(b.iter()).enumerate() {
                if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                    values.push(T::NULL);
                    continue;
                }
                match T::apply(ArithmeticOp::Remainder, *a, *b) {
                    Some(result) => values.push(result),
                    None => {
                        return Err(TableFailure {
                            message: "java.lang.ArithmeticException: / by zero".to_string(),
                        })
                    }
                }
            }
            values
        }
    };
    Ok(null_values_to_nulls::<T>(PrimitiveArray::new(
        values.into(),
        nulls,
    )))
}

/// `-operand` with the operand converted to `T`.
///
/// Behavior: `QueryLanguageFunctionUtils#negate`: null in, null out, and a result equal to
/// the type's null value is null.
pub(crate) fn negate<T: JavaNumber>(operand: &dyn Array) -> PrimitiveArray<T::Arrow> {
    let operand = convert::<T>(operand);
    null_values_to_nulls::<T>(operand.unary(T::negate))
}

/// `array` with every valid element equal to `null_value` made null, or `None` when there
/// is no such element.
///
/// Java stores null as a reserved value of each primitive type, so a column cannot hold
/// that value as data: a cell holding it is null to every formula operator.
pub(crate) fn without_null_value<P: ArrowPrimitiveType>(
    array: &PrimitiveArray<P>,
    null_value: P::Native,
) -> Option<PrimitiveArray<P>> {
    // Most columns hold the value nowhere, which a pass over the values alone shows.
    if !array.values().iter().any(|v| *v == null_value) {
        return None;
    }
    let is_null_value = |v: Option<P::Native>| v.is_some_and(|v| v == null_value);
    if !array.iter().any(is_null_value) {
        return None;
    }
    let cleaned: PrimitiveArray<P> = array
        .iter()
        .map(|v| v.filter(|v| *v != null_value))
        .collect();
    Some(cleaned.with_data_type(array.data_type().clone()))
}

/// A primitive array as Java holds it: every null replaced by the value Java reserves for
/// null in the array's type.
///
/// Behavior: the null values in `io.deephaven.util.QueryConstants`.
pub(crate) fn with_null_values(array: &ArrayRef) -> ArrayRef {
    fn fill<T: JavaPrimitive>(array: &ArrayRef) -> ArrayRef {
        let values = array.as_primitive::<T::Arrow>();
        if values.null_count() == 0 {
            return array.clone();
        }
        let filled: PrimitiveArray<T::Arrow> =
            PrimitiveArray::from_iter_values(values.iter().map(|v| v.unwrap_or(T::NULL)));
        Arc::new(filled)
    }
    match array.data_type() {
        DataType::Int8 => fill::<i8>(array),
        DataType::UInt16 => fill::<u16>(array),
        DataType::Int16 => fill::<i16>(array),
        DataType::Int32 => fill::<i32>(array),
        DataType::Int64 => fill::<i64>(array),
        DataType::Float32 => fill::<f32>(array),
        DataType::Float64 => fill::<f64>(array),
        other => unreachable!("the type checker passed a {other} array as a primitive"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected values are Java's narrowing primitive conversions, the Java Language
    /// Specification section 5.1.3.
    #[test]
    fn integer_narrowing_keeps_the_low_bits() {
        assert_eq!(i8::from_integer(127), 127);
        assert_eq!(i8::from_integer(128), -128);
        assert_eq!(i8::from_integer(255), -1);
        assert_eq!(i8::from_integer(256), 0);
        assert_eq!(i8::from_integer(-129), 127);
        assert_eq!(i16::from_integer(32_768), -32_768);
        assert_eq!(i16::from_integer(65_535), -1);
        assert_eq!(i16::from_integer(-32_769), 32_767);
        assert_eq!(u16::from_integer(-1), 65_535);
        assert_eq!(u16::from_integer(65_536), 0);
        assert_eq!(u16::from_integer(65_601), 65);
        assert_eq!(i32::from_integer(2_147_483_648), i32::MIN);
        assert_eq!(i32::from_integer(4_294_967_295), -1);
        assert_eq!(i32::from_integer(i64::MAX), -1);
        assert_eq!(i32::from_integer(i64::MIN), 0);
        assert_eq!(i64::from_integer(i64::MIN), i64::MIN);
    }

    /// Expected values are Java's `(int) d`, `(long) d`, `(byte) d`, `(short) d` and
    /// `(char) d`, the Java Language Specification section 5.1.3: toward zero, saturating at
    /// the bounds of `int` or `long`, NaN to zero, and for the narrow types the low bits of
    /// the `int` result.
    #[test]
    fn floating_to_integer_truncates_saturates_and_maps_nan_to_zero() {
        assert_eq!(i32::from_floating(1.9), 1);
        assert_eq!(i32::from_floating(-1.9), -1);
        assert_eq!(i32::from_floating(f64::NAN), 0);
        assert_eq!(i32::from_floating(1e10), i32::MAX);
        assert_eq!(i32::from_floating(-1e10), i32::MIN);
        assert_eq!(i32::from_floating(f64::INFINITY), i32::MAX);
        assert_eq!(i64::from_floating(1e19), i64::MAX);
        assert_eq!(i64::from_floating(-1e19), i64::MIN);
        assert_eq!(i64::from_floating(f64::NAN), 0);
        assert_eq!(i64::from_floating(-0.5), 0);
        // (byte) 300.7 is (byte) 300, which is 44.
        assert_eq!(i8::from_floating(300.7), 44);
        // (byte) 1e10 is (byte) Integer.MAX_VALUE, which is -1.
        assert_eq!(i8::from_floating(1e10), -1);
        assert_eq!(i8::from_floating(-1e10), 0);
        assert_eq!(i8::from_floating(f64::NAN), 0);
        assert_eq!(i16::from_floating(1e10), -1);
        assert_eq!(i16::from_floating(40_000.5), -25_536);
        assert_eq!(u16::from_floating(-1.0), 65_535);
        assert_eq!(u16::from_floating(1e10), 65_535);
        assert_eq!(u16::from_floating(65.9), 65);
    }

    /// Expected values are Java's `(float) l`, `(double) l` and `(float) d`: the nearest
    /// value, ties to even, and infinity beyond the range of `float`.
    #[test]
    fn conversions_to_floating_point_round_to_nearest() {
        // 2^24 + 1 is not a float; the tie goes to the even neighbor.
        assert_eq!(f32::from_integer(16_777_217), 16_777_216.0);
        assert_eq!(f32::from_integer(16_777_219), 16_777_220.0);
        assert_eq!(
            f64::from_integer(9_007_199_254_740_993),
            9_007_199_254_740_992.0
        );
        assert_eq!(f32::from_integer(i64::MAX), 9.223_372e18);
        assert_eq!(f32::from_floating(1e39), f32::INFINITY);
        assert_eq!(f32::from_floating(-1e39), f32::NEG_INFINITY);
        assert_eq!(f32::from_floating(1e-46), 0.0);
        assert_eq!(f32::from_floating(0.1), 0.1_f32);
        assert!(f32::from_floating(f64::NAN).is_nan());
        assert_eq!(
            f64::from_floating(f64::from(0.1_f32)),
            0.100_000_001_490_116_12
        );
    }
}
