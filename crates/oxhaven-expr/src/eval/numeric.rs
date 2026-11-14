//! Arithmetic in the four types Java computes numbers in.

use crate::types::ArithmeticOp;
use arrow::array::{Array, AsArray, PrimitiveArray};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimeUnit, TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::table::TableFailure;

/// A type Java computes numbers in: `int`, `long`, `float` or `double`.
pub(crate) trait JavaNumber: Copy + PartialEq {
    type Arrow: ArrowPrimitiveType<Native = Self>;

    /// The value Java stores for null. A result equal to it is null.
    ///
    /// Behavior: `io.deephaven.util.QueryConstants#NULL_INT`, `#NULL_LONG`, `#NULL_FLOAT`,
    /// `#NULL_DOUBLE`.
    const NULL: Self;

    /// Java's widening conversion from an integer type or `char`.
    fn from_i64(value: i64) -> Self;
    fn from_f32(value: f32) -> Self;
    fn from_f64(value: f64) -> Self;

    /// `a op b` as Java computes it once both operands have this type. `None` is Java's
    /// `ArithmeticException`.
    fn apply(op: ArithmeticOp, a: Self, b: Self) -> Option<Self>;

    fn negate(self) -> Self;
}

macro_rules! java_integer {
    ($native:ty, $arrow:ty) => {
        impl JavaNumber for $native {
            type Arrow = $arrow;
            const NULL: Self = <$native>::MIN;

            fn from_i64(value: i64) -> Self {
                value as $native
            }
            fn from_f32(value: f32) -> Self {
                value as $native
            }
            fn from_f64(value: f64) -> Self {
                value as $native
            }

            /// Behavior: Java's `+`, `-`, `*` and `%` on this type: overflow wraps, the
            /// remainder takes the sign of the dividend, and a zero divisor throws.
            fn apply(op: ArithmeticOp, a: Self, b: Self) -> Option<Self> {
                match op {
                    ArithmeticOp::Add => Some(a.wrapping_add(b)),
                    ArithmeticOp::Subtract => Some(a.wrapping_sub(b)),
                    ArithmeticOp::Multiply => Some(a.wrapping_mul(b)),
                    ArithmeticOp::Remainder => (b != 0).then(|| a.wrapping_rem(b)),
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
        impl JavaNumber for $native {
            type Arrow = $arrow;
            const NULL: Self = -<$native>::MAX;

            fn from_i64(value: i64) -> Self {
                value as $native
            }
            fn from_f32(value: f32) -> Self {
                value as $native
            }
            fn from_f64(value: f64) -> Self {
                value as $native
            }

            /// Behavior: Java's floating point operators, each one IEEE 754 operation;
            /// `%` is the truncating remainder (C `fmod`).
            fn apply(op: ArithmeticOp, a: Self, b: Self) -> Option<Self> {
                Some(match op {
                    ArithmeticOp::Add => a + b,
                    ArithmeticOp::Subtract => a - b,
                    ArithmeticOp::Multiply => a * b,
                    ArithmeticOp::Divide => a / b,
                    ArithmeticOp::Remainder => a % b,
                })
            }

            fn negate(self) -> Self {
                -self
            }
        }
    };
}

java_integer!(i32, Int32Type);
java_integer!(i64, Int64Type);
java_floating!(f32, Float32Type);
java_floating!(f64, Float64Type);

/// Converts a numeric, char or instant array to `T` with Java's widening conversions.
/// Nulls stay null.
///
/// The type checker only asks for conversions Java performs implicitly, so a conversion
/// never narrows.
pub(crate) fn widen<T: JavaNumber>(array: &dyn Array) -> PrimitiveArray<T::Arrow> {
    match array.data_type() {
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .unary(|v| T::from_i64(v.into())),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .unary(|v| T::from_i64(v.into())),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .unary(|v| T::from_i64(v.into())),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .unary(|v| T::from_i64(v.into())),
        DataType::Int64 => array.as_primitive::<Int64Type>().unary(T::from_i64),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .unary(T::from_i64),
        DataType::Float32 => array.as_primitive::<Float32Type>().unary(T::from_f32),
        DataType::Float64 => array.as_primitive::<Float64Type>().unary(T::from_f64),
        other => unreachable!("the type checker passed a {other} array as a number"),
    }
}

/// A computed value, or null when it is the type's null value.
fn nullable<T: JavaNumber>(value: T) -> Option<T> {
    (value != T::NULL).then_some(value)
}

/// `left op right` with both operands converted to `T`.
///
/// Behavior: `QueryLanguageFunctionUtils#plus`, `#minus`, `#multiply`, `#divide` and
/// `#remainder`: null when either operand is null, otherwise Java's operator on the operands
/// cast to the result type. A result equal to the type's null value is null, because that is
/// how Java stores null. An integer remainder by zero fails the evaluation, but only for a
/// row where neither operand is null.
pub(crate) fn arithmetic<T: JavaNumber>(
    op: ArithmeticOp,
    left: &dyn Array,
    right: &dyn Array,
) -> Result<PrimitiveArray<T::Arrow>, TableFailure> {
    let left = widen::<T>(left);
    let right = widen::<T>(right);
    left.iter()
        .zip(right.iter())
        .map(|operands| match operands {
            (Some(a), Some(b)) => match T::apply(op, a, b) {
                Some(result) => Ok(nullable(result)),
                None => Err(TableFailure {
                    message: "java.lang.ArithmeticException: / by zero".to_string(),
                }),
            },
            _ => Ok(None),
        })
        .collect()
}

/// `-operand` with the operand converted to `T`.
///
/// Behavior: `QueryLanguageFunctionUtils#negate`: null in, null out, and a result equal to
/// the type's null value is null.
pub(crate) fn negate<T: JavaNumber>(operand: &dyn Array) -> PrimitiveArray<T::Arrow> {
    widen::<T>(operand)
        .iter()
        .map(|value| value.and_then(|v| nullable(v.negate())))
        .collect()
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
