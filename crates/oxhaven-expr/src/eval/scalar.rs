//! Operators with a literal on one side, and the choice between two computed branches:
//! loops over the values of the other side in their own type, with no array made for the
//! literal and no widened copy of the values.
//!
//! Each function gives what the general kernel gives for the literal repeated in every row
//! ([`super::compare::compare`], [`super::numeric::arithmetic`]) and returns `None` for
//! the cases it leaves to that kernel, which the caller then runs.

use super::compare::accepts;
use super::numeric::{convert, null_values_to_nulls, JavaNumber, JavaPrimitive};
use crate::types::{ArithmeticOp, BitwiseOp, CompareDomain, CompareOp};
use arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
};
use oxhaven_column::bits::pack;
use oxhaven_contracts::expr::Literal;
use std::cmp::Ordering;
use std::sync::Arc;

/// The value of a numeric or `char` literal converted to `T` as [`convert`] converts a
/// cell. `None` for a literal of another kind and for one that holds the value Java
/// reserves for null in its own type, which is a null and not a number.
pub(super) fn literal_as<T: JavaPrimitive>(literal: &Literal) -> Option<T> {
    match *literal {
        Literal::Char(value) if value != u16::NULL => Some(T::from_integer(value.into())),
        Literal::Int(value) if value != i32::NULL => Some(T::from_integer(value.into())),
        Literal::Long(value) if value != i64::NULL => Some(T::from_integer(value)),
        Literal::Float(value) if value != f32::NULL => Some(T::from_floating(value.into())),
        Literal::Double(value) if value != f64::NULL => Some(T::from_floating(value)),
        _ => None,
    }
}

fn mirrored(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Less => CompareOp::Greater,
        CompareOp::LessOrEqual => CompareOp::GreaterOrEqual,
        CompareOp::Greater => CompareOp::Less,
        CompareOp::GreaterOrEqual => CompareOp::LessOrEqual,
        equality => equality,
    }
}

/// `value op scalar` for every value, with the operator named in each loop so that each
/// is a loop of one comparison.
fn ordered<T: Copy + PartialOrd>(op: CompareOp, values: &[T], scalar: T) -> BooleanBuffer {
    match op {
        CompareOp::Equal => pack(values, |value| value == scalar),
        CompareOp::NotEqual => pack(values, |value| value != scalar),
        CompareOp::Less => pack(values, |value| value < scalar),
        CompareOp::LessOrEqual => pack(values, |value| value <= scalar),
        CompareOp::Greater => pack(values, |value| value > scalar),
        CompareOp::GreaterOrEqual => pack(values, |value| value >= scalar),
    }
}

/// `array op scalar` in the integers, compared in the array's own type when the scalar is
/// one of its values and as `long`s otherwise.
fn integral<P: ArrowPrimitiveType>(
    op: CompareOp,
    array: &PrimitiveArray<P>,
    scalar: i64,
) -> BooleanBuffer
where
    P::Native: Into<i64> + TryFrom<i64> + PartialOrd,
{
    match P::Native::try_from(scalar) {
        Ok(scalar) => ordered(op, array.values(), scalar),
        Err(_) => match op {
            CompareOp::Equal => pack(array.values(), |value| value.into() == scalar),
            CompareOp::NotEqual => pack(array.values(), |value| value.into() != scalar),
            CompareOp::Less => pack(array.values(), |value| value.into() < scalar),
            CompareOp::LessOrEqual => pack(array.values(), |value| value.into() <= scalar),
            CompareOp::Greater => pack(array.values(), |value| value.into() > scalar),
            CompareOp::GreaterOrEqual => pack(array.values(), |value| value.into() >= scalar),
        },
    }
}

/// `array op scalar` with each value widened to `double`, which is exact for every type
/// this is called with.
fn floating<P: ArrowPrimitiveType>(
    op: CompareOp,
    array: &PrimitiveArray<P>,
    scalar: f64,
) -> BooleanBuffer
where
    P::Native: Into<f64>,
{
    let values = array.values();
    match op {
        CompareOp::Equal => pack(values, |value| value.into() == scalar),
        CompareOp::NotEqual => pack(values, |value| value.into() != scalar),
        CompareOp::Less => pack(values, |value| value.into() < scalar),
        CompareOp::LessOrEqual => pack(values, |value| value.into() <= scalar),
        CompareOp::Greater => pack(values, |value| value.into() > scalar),
        CompareOp::GreaterOrEqual => pack(values, |value| value.into() >= scalar),
    }
}

/// `array op literal`, or `literal op array` with `literal_on_left`, in `domain`. The
/// result has no nulls. `None` for the domains, array types and literals left to
/// [`super::compare::compare`].
///
/// Behavior: as `decide` of [`super::compare`], for a right operand that is never null
/// and never NaN. A null cell is less than the literal. A floating point cell that is NaN
/// fails every comparison but `!=`, which is what the IEEE comparisons of the loops give;
/// `Double.compare` differs from them only in telling the two zeros apart, so a literal
/// zero is left to the general kernel.
pub(super) fn compare(
    op: CompareOp,
    domain: CompareDomain,
    array: &dyn Array,
    literal: &Literal,
    literal_on_left: bool,
) -> Option<BooleanArray> {
    // `literal op cell` is `cell mirrored(op) literal`, for a null cell too: null is below
    // the literal from either side.
    let op = if literal_on_left { mirrored(op) } else { op };
    let values = match domain {
        CompareDomain::Integral => {
            let scalar = match *literal {
                Literal::Instant(value) if value != i64::NULL => value,
                _ => literal_as::<i64>(literal)?,
            };
            match array.data_type() {
                DataType::Int8 => integral(op, array.as_primitive::<Int8Type>(), scalar),
                DataType::Int16 => integral(op, array.as_primitive::<Int16Type>(), scalar),
                DataType::UInt16 => integral(op, array.as_primitive::<UInt16Type>(), scalar),
                DataType::Int32 => integral(op, array.as_primitive::<Int32Type>(), scalar),
                DataType::Int64 => integral(op, array.as_primitive::<Int64Type>(), scalar),
                _ => return None,
            }
        }
        CompareDomain::Floating { through_float } => {
            let scalar = if through_float {
                f64::from(literal_as::<f32>(literal)?)
            } else {
                literal_as::<f64>(literal)?
            };
            if scalar.is_nan() || scalar == 0.0 {
                return None;
            }
            match array.data_type() {
                DataType::Float32 => floating(op, array.as_primitive::<Float32Type>(), scalar),
                // Through `float` the other types are rounded first.
                _ if through_float => return None,
                DataType::Float64 => floating(op, array.as_primitive::<Float64Type>(), scalar),
                DataType::Int8 => floating(op, array.as_primitive::<Int8Type>(), scalar),
                DataType::Int16 => floating(op, array.as_primitive::<Int16Type>(), scalar),
                DataType::UInt16 => floating(op, array.as_primitive::<UInt16Type>(), scalar),
                DataType::Int32 => floating(op, array.as_primitive::<Int32Type>(), scalar),
                _ => return None,
            }
        }
        _ => return None,
    };
    let values = match array.nulls().filter(|nulls| nulls.null_count() > 0) {
        None => values,
        // What the loop made of a null cell's slot is replaced by null's own answer.
        Some(nulls) if accepts(op, Ordering::Less) => &values | &!nulls.inner(),
        Some(nulls) => &values & nulls.inner(),
    };
    Some(BooleanArray::new(values, None))
}

/// `array op scalar`, or `scalar op array` with `scalar_on_left`, with the array converted
/// to `T`. `None` for an integer remainder with the scalar on the left or a zero divisor,
/// which are left to [`super::numeric::arithmetic`].
///
/// Behavior: as [`super::numeric::arithmetic`]. A remainder by a divisor that is not zero
/// fails for no row.
pub(super) fn arithmetic<T: JavaNumber>(
    op: ArithmeticOp,
    array: &dyn Array,
    scalar: T,
    scalar_on_left: bool,
) -> Option<PrimitiveArray<T::Arrow>> {
    let operand = convert::<T>(array);
    let cells = operand.values();
    macro_rules! every_row {
        ($op:expr) => {
            if scalar_on_left {
                cells
                    .iter()
                    .map(|cell| T::apply($op, scalar, *cell).unwrap_or(T::NULL))
                    .collect::<Vec<T>>()
            } else {
                cells
                    .iter()
                    .map(|cell| T::apply($op, *cell, scalar).unwrap_or(T::NULL))
                    .collect::<Vec<T>>()
            }
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
        ArithmeticOp::Remainder => {
            // Only an integer remainder by zero fails, and then for the first row that
            // has no null operand, which the general kernel finds.
            if scalar_on_left || T::apply(ArithmeticOp::Remainder, scalar, scalar).is_none() {
                return None;
            }
            T::remainders(cells, scalar)
        }
    };
    Some(null_values_to_nulls::<T>(PrimitiveArray::new(
        values.into(),
        operand.nulls().cloned(),
    )))
}

/// For each row the value of `if_true` where `condition`, which has no nulls, is true and
/// that of `if_false` elsewhere. `None` for arrays that are not of Java's primitives.
pub(super) fn choose(
    condition: &BooleanArray,
    if_true: &ArrayRef,
    if_false: &ArrayRef,
) -> Option<ArrayRef> {
    fn of<P: ArrowPrimitiveType>(
        condition: &BooleanArray,
        if_true: &ArrayRef,
        if_false: &ArrayRef,
    ) -> ArrayRef {
        let (a, b) = (if_true.as_primitive::<P>(), if_false.as_primitive::<P>());
        let chosen = condition.values();
        // The condition 64 rows at a time: a word of it, then a loop over the two
        // branches' values with no branch in it.
        let words = chosen.bit_chunks();
        let mut values: Vec<P::Native> = Vec::with_capacity(chosen.len());
        let pairs = a.values().chunks(64).zip(b.values().chunks(64));
        for (word, (a, b)) in words.iter_padded().zip(pairs) {
            values.extend(a.iter().zip(b).enumerate().map(|(bit, (a, b))| {
                if word >> bit & 1 == 1 {
                    *a
                } else {
                    *b
                }
            }));
        }
        let nulls = (a.null_count() > 0 || b.null_count() > 0).then(|| {
            let from_true = match a.nulls() {
                Some(nulls) => chosen & nulls.inner(),
                None => chosen.clone(),
            };
            let from_false = match b.nulls() {
                Some(nulls) => &!chosen & nulls.inner(),
                None => !chosen,
            };
            NullBuffer::new(&from_true | &from_false)
        });
        Arc::new(PrimitiveArray::<P>::new(values.into(), nulls))
    }
    Some(match if_true.data_type() {
        DataType::Int8 => of::<Int8Type>(condition, if_true, if_false),
        DataType::UInt16 => of::<UInt16Type>(condition, if_true, if_false),
        DataType::Int16 => of::<Int16Type>(condition, if_true, if_false),
        DataType::Int32 => of::<Int32Type>(condition, if_true, if_false),
        DataType::Int64 => of::<Int64Type>(condition, if_true, if_false),
        DataType::Float32 => of::<Float32Type>(condition, if_true, if_false),
        DataType::Float64 => of::<Float64Type>(condition, if_true, if_false),
        _ => return None,
    })
}
