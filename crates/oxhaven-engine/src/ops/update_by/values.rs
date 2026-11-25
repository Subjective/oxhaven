//! Column values as the kernels see them, and the arrays kernels give back.
//!
//! Java's operators work on primitive values in which a null is a reserved value of the
//! type, and several results depend on that: a sum that wraps onto the reserved value reads
//! as null and restarts. The kernels here keep nulls apart (`Option`) and call
//! [`long_result`] or [`double_result`] wherever Java's arithmetic could land on the reserved
//! value, so the outcome is the same without storing sentinels.

use super::failure;
use crate::ops::agg::{Scalar, Values as Decoded};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, ListArray, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
};
use arrow::compute::{concat, take};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
use std::sync::Arc;

/// The values of one column for a run of rows.
pub(super) enum Values {
    /// `byte`, `short`, `int`, `long`, `char`, `Instant` (nanoseconds) and `Boolean`
    /// (false 0, true 1), widened.
    Int(Vec<Option<i64>>),
    /// `float` and `double`, widened.
    Float(Vec<Option<f64>>),
    Text(Vec<Option<String>>),
    /// The cells of a vector, array, `BigDecimal` or `BigInteger` column, as they are: the
    /// operators that read such a column without computing with it (forward fill, rolling
    /// count, rolling group) read only which cells are null, and a bucket key reads the
    /// elements of a vector through [`push_list_keys`].
    Lists(ArrayRef),
    /// The cells of a vector or big number column read for an operator that orders them
    /// (minimum and maximum), each a [`Scalar::List`], [`Scalar::Decimal`],
    /// [`Scalar::Integer`] or a null.
    Ordered(Vec<Scalar>),
}

fn cast<A: Array + 'static>(array: &ArrayRef) -> Result<&A, TableFailure> {
    array.as_any().downcast_ref::<A>().ok_or_else(|| {
        failure(format!(
            "a column holds {} where its type promises another array",
            array.data_type()
        ))
    })
}

impl Values {
    /// Behavior: io.deephaven.engine.table.impl.util.ReinterpretUtils#maybeConvertToPrimitive,
    /// which is how io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy reads its inputs
    pub(super) fn read(array: &ArrayRef, column_type: &ColumnType) -> Result<Values, TableFailure> {
        macro_rules! ints {
            ($array:ty) => {
                Values::Int(
                    cast::<$array>(array)?
                        .iter()
                        .map(|v| v.map(i64::from))
                        .collect(),
                )
            };
        }
        Ok(match column_type {
            ColumnType::Boolean => ints!(BooleanArray),
            ColumnType::Byte => ints!(Int8Array),
            ColumnType::Char => ints!(UInt16Array),
            ColumnType::Short => ints!(Int16Array),
            ColumnType::Int => ints!(Int32Array),
            ColumnType::Long => ints!(Int64Array),
            ColumnType::Instant => ints!(TimestampNanosecondArray),
            ColumnType::Float => Values::Float(
                cast::<Float32Array>(array)?
                    .iter()
                    .map(|v| v.map(f64::from))
                    .collect(),
            ),
            ColumnType::Double => Values::Float(cast::<Float64Array>(array)?.iter().collect()),
            ColumnType::String => Values::Text(
                cast::<StringArray>(array)?
                    .iter()
                    .map(|v| v.map(str::to_owned))
                    .collect(),
            ),
            // Java reads a vector, an array or a big number column as a column of objects.
            ColumnType::Vector(_)
            | ColumnType::Array(_)
            | ColumnType::BigDecimal
            | ColumnType::BigInteger => Values::Lists(array.clone()),
            other => {
                return Err(failure(format!(
                    "update_by cannot read a column of type {other:?}"
                )))
            }
        })
    }

    /// [`Values::read`], with the cells of a vector column decoded to be ordered.
    pub(super) fn read_ordered(
        array: &ArrayRef,
        column_type: &ColumnType,
    ) -> Result<Values, TableFailure> {
        if !matches!(
            column_type,
            ColumnType::Vector(_) | ColumnType::BigDecimal | ColumnType::BigInteger
        ) {
            return Values::read(array, column_type);
        }
        match Decoded::decode(column_type, array)? {
            Decoded::Cells(cells) => Ok(Values::Ordered(cells)),
            _ => Err(failure("a vector column must decode to cells")),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Values::Int(v) => v.len(),
            Values::Float(v) => v.len(),
            Values::Text(v) => v.len(),
            Values::Lists(v) => v.len(),
            Values::Ordered(v) => v.len(),
        }
    }

    pub(super) fn is_null(&self, index: usize) -> bool {
        match self {
            Values::Int(v) => v[index].is_none(),
            Values::Float(v) => v[index].is_none(),
            Values::Text(v) => v[index].is_none(),
            Values::Lists(v) => v.is_null(index),
            Values::Ordered(v) => v[index].is_null(),
        }
    }

    /// How the non-null values at `a` and `b` compare, as Java's `<`, `>` and `==` on
    /// primitives or `compareTo` on a `String` or a vector do. `None` when a NaN is
    /// involved, for which all three primitive comparisons are false; a NaN inside a vector
    /// is ordered, as `DoubleComparisons#compare` orders it.
    pub(super) fn compare(&self, a: usize, b: usize) -> Option<Ordering> {
        match self {
            Values::Int(v) => v[a].partial_cmp(&v[b]),
            Values::Float(v) => v[a]?.partial_cmp(&v[b]?),
            // Behavior: java.lang.String#compareTo, which orders by UTF-16 code unit.
            Values::Text(v) => Some(
                v[a].as_deref()?
                    .encode_utf16()
                    .cmp(v[b].as_deref()?.encode_utf16()),
            ),
            // No operator that orders values is given an array column, nor a vector column
            // not read by `read_ordered`.
            Values::Lists(_) => None,
            // Behavior: io.deephaven.vector.IntVector#compareTo, ObjectVector#compareTo and
            // their siblings: element by element, a null element first, then the shorter.
            Values::Ordered(v) => Some(v[a].compare(&v[b])),
        }
    }
}

/// Whether `update_by` buckets by a vector column of type `column_type`.
///
/// It does when the elements, at every depth, are integers, `char`s, `Boolean`s,
/// `String`s, `Instant`s or vectors of these: every class of vector in Java compares those
/// elements by value, and hashes them consistently.
///
/// It also does for a vector of `float` or `double`, with `-0.0` equal to `0.0` and every
/// NaN equal to every other, as Java's buckets of a plain `double` column are (owner item
/// 7.6, allowed difference A19 of `PARITY.md`). Java's own buckets depend on the class of
/// the vectors: `DoubleVectorDirect#equals` compares two direct vectors (a vector made by
/// `select()`) with `Arrays#equals`, for which `-0.0` and `0.0` differ, but any other pair
/// (a vector of a group-by result is a `DoubleVectorColumnWrapper`) with
/// `DoubleComparisons#eq`, for which they are equal.
///
/// It does not for a vector of vectors of `float` or `double`, which the owner's answer
/// does not name, nor for an array inside a vector: `ObjectVector#equals` compares arrays
/// by value but `ObjectVector#hashCode` hashes them by identity, so equal vectors of arrays
/// fall in one bucket or apart as their hashes happen to agree (seen on Deephaven 42.9:
/// each in its own bucket; owner item 7.7, kept refused).
///
/// Behavior: io.deephaven.vector.IntVector#equals and #hashCode, and those of the other
/// vector interfaces and of their `Direct` classes, which
/// io.deephaven.util.compare.ObjectComparisons#eq and #hashCode call for a key of objects;
/// io.deephaven.util.compare.DoubleComparisons#eq and #hashCode for the elements of a
/// vector of `double` (and `FloatComparisons` for `float`)
pub(super) fn bucketable_vector(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Vector(element)
            if matches!(**element, ColumnType::Float | ColumnType::Double) =>
        {
            true
        }
        _ => exact_vector(column_type),
    }
}

/// Whether every class of vector in Java compares two vectors of `column_type` by their
/// elements' values: see [`bucketable_vector`].
fn exact_vector(column_type: &ColumnType) -> bool {
    use ColumnType::*;
    match column_type {
        Vector(element) => match &**element {
            Boolean | Byte | Char | Short | Int | Long | String | Instant => true,
            inner @ Vector(_) => exact_vector(inner),
            _ => false,
        },
        _ => false,
    }
}

/// The elements of a list array, its type resolved once for all its cells.
enum Elements<'a> {
    Boolean(&'a BooleanArray),
    Byte(&'a Int8Array),
    Char(&'a UInt16Array),
    Short(&'a Int16Array),
    Int(&'a Int32Array),
    Long(&'a Int64Array),
    Instant(&'a TimestampNanosecondArray),
    Float(&'a Float32Array),
    Double(&'a Float64Array),
    String(&'a StringArray),
    Lists(&'a ListArray, Box<Elements<'a>>),
}

impl<'a> Elements<'a> {
    /// The cells of `array`, of type `element`; only the types [`bucketable_vector`] takes.
    fn of(array: &'a ArrayRef, element: &ColumnType) -> Result<Elements<'a>, TableFailure> {
        Ok(match element {
            ColumnType::Boolean => Elements::Boolean(cast(array)?),
            ColumnType::Byte => Elements::Byte(cast(array)?),
            ColumnType::Char => Elements::Char(cast(array)?),
            ColumnType::Short => Elements::Short(cast(array)?),
            ColumnType::Int => Elements::Int(cast(array)?),
            ColumnType::Long => Elements::Long(cast(array)?),
            ColumnType::Instant => Elements::Instant(cast(array)?),
            ColumnType::Float => Elements::Float(cast(array)?),
            ColumnType::Double => Elements::Double(cast(array)?),
            ColumnType::String => Elements::String(cast(array)?),
            ColumnType::Vector(inner) => {
                let lists: &ListArray = cast(array)?;
                Elements::Lists(lists, Box::new(Elements::of(lists.values(), inner)?))
            }
            other => {
                return Err(failure(format!(
                    "update_by cannot bucket by vectors of {other:?}"
                )))
            }
        })
    }

    /// Appends the cell at `index` to `key`: whether it is null, then its value, so that
    /// two cells append the same bytes exactly when they are equal.
    fn push(&self, index: usize, key: &mut Vec<u8>) {
        macro_rules! fixed {
            ($array:expr) => {{
                let valid = $array.is_valid(index);
                key.push(u8::from(valid));
                if valid {
                    key.extend($array.value(index).to_le_bytes());
                }
            }};
        }
        match self {
            Elements::Boolean(a) => key.push(if a.is_null(index) {
                0
            } else {
                1 + u8::from(a.value(index))
            }),
            Elements::Byte(a) => fixed!(a),
            Elements::Char(a) => fixed!(a),
            Elements::Short(a) => fixed!(a),
            Elements::Int(a) => fixed!(a),
            Elements::Long(a) => fixed!(a),
            Elements::Instant(a) => fixed!(a),
            Elements::Float(a) => {
                let valid = a.is_valid(index);
                key.push(u8::from(valid));
                if valid {
                    key.extend(float_key(f64::from(a.value(index))).to_le_bytes());
                }
            }
            Elements::Double(a) => {
                let valid = a.is_valid(index);
                key.push(u8::from(valid));
                if valid {
                    key.extend(float_key(a.value(index)).to_le_bytes());
                }
            }
            Elements::String(a) => {
                let valid = a.is_valid(index);
                key.push(u8::from(valid));
                if valid {
                    let text = a.value(index);
                    key.extend((text.len() as u64).to_le_bytes());
                    key.extend(text.as_bytes());
                }
            }
            Elements::Lists(a, inner) => {
                let valid = a.is_valid(index);
                key.push(u8::from(valid));
                if valid {
                    let offsets = a.value_offsets();
                    let (start, end) = (offsets[index] as usize, offsets[index + 1] as usize);
                    key.extend(((end - start) as u64).to_le_bytes());
                    for element in start..end {
                        inner.push(element, key);
                    }
                }
            }
        }
    }
}

/// The bits that stand for a `float` or `double` element in a bucket key: equal for
/// `-0.0` and `0.0`, and for every NaN.
///
/// Behavior: io.deephaven.util.compare.DoubleComparisons#eq and #hashCode
fn float_key(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Appends to each of `keys` the bucket key of the cell of the same position in `array`, a
/// vector column of type `column_type`, which [`bucketable_vector`] takes. Two cells
/// append the same bytes exactly when Java's `equals` holds between their vectors: both
/// null, or of one size with equal elements, a null element equal only to a null element;
/// `float` and `double` elements are equal as [`float_key`] says.
///
/// Behavior: io.deephaven.vector.IntVector#equals, ObjectVector#equals (whose
/// `Objects#deepEquals` is `equals` for these elements), with
/// io.deephaven.util.compare.ObjectComparisons#eq for the key of objects that
/// io.deephaven.engine.table.impl.updateby.BucketedPartitionedUpdateByManager buckets by
pub(super) fn push_list_keys(
    keys: &mut [Vec<u8>],
    array: &ArrayRef,
    column_type: &ColumnType,
) -> Result<(), TableFailure> {
    let cells = Elements::of(array, column_type)?;
    for (index, key) in keys.iter_mut().enumerate() {
        cells.push(index, key);
    }
    Ok(())
}

/// A `long` result of Java arithmetic: the reserved value is null.
pub(super) fn long_result(value: i64) -> Option<i64> {
    (value != i64::MIN).then_some(value)
}

/// A `double` result of Java arithmetic: the reserved value is null.
pub(super) fn double_result(value: f64) -> Option<f64> {
    (value != -f64::MAX).then_some(value)
}

/// A `long` column.
pub(super) fn long_array(values: impl IntoIterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<Int64Array>())
}

/// A `double` column.
pub(super) fn double_array(values: impl IntoIterator<Item = Option<f64>>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<Float64Array>())
}

/// Integer results as a column of an integer type: each value is cut to the width of the
/// type as a Java cast does, and a result that is the type's reserved value is null.
///
/// # Panics
/// If `column_type` is not `byte`, `short`, `int`, `long` or `char`.
pub(super) fn narrowed_ints(
    column_type: &ColumnType,
    values: impl IntoIterator<Item = Option<i64>>,
) -> ArrayRef {
    let values = values.into_iter();
    match column_type {
        ColumnType::Byte => Arc::new(
            values
                .map(|v| v.map(|v| v as i8).filter(|v| *v != i8::MIN))
                .collect::<Int8Array>(),
        ),
        ColumnType::Short => Arc::new(
            values
                .map(|v| v.map(|v| v as i16).filter(|v| *v != i16::MIN))
                .collect::<Int16Array>(),
        ),
        ColumnType::Int => Arc::new(
            values
                .map(|v| v.map(|v| v as i32).filter(|v| *v != i32::MIN))
                .collect::<Int32Array>(),
        ),
        ColumnType::Long => long_array(values.map(|v| v.and_then(long_result))),
        ColumnType::Char => Arc::new(
            values
                .map(|v| v.map(|v| v as u16).filter(|v| *v != u16::MAX))
                .collect::<UInt16Array>(),
        ),
        other => panic!("{other:?} is not an integer column type"),
    }
}

/// Floating point results as a `float` or `double` column. A `float` is rounded from the
/// `double` it was computed in, which gives what Java's `float` arithmetic gives for a
/// single operation on `float` operands.
///
/// # Panics
/// If `column_type` is neither.
pub(super) fn narrowed_floats(
    column_type: &ColumnType,
    values: impl IntoIterator<Item = Option<f64>>,
) -> ArrayRef {
    let values = values.into_iter();
    match column_type {
        ColumnType::Double => double_array(values.map(|v| v.and_then(double_result))),
        ColumnType::Float => Arc::new(
            values
                .map(|v| v.map(|v| v as f32).filter(|v| *v != -f32::MAX))
                .collect::<Float32Array>(),
        ),
        other => panic!("{other:?} is not a floating point column type"),
    }
}

/// The elements of `seed` followed by `values`, picked by `indices`; `None` picks a null.
/// For results that are always one of the input values, whatever the column type.
pub(super) fn pick(
    seed: Option<&ArrayRef>,
    values: &ArrayRef,
    indices: impl IntoIterator<Item = Option<usize>>,
) -> Result<ArrayRef, TableFailure> {
    let indices: UInt32Array = indices
        .into_iter()
        .map(|index| index.map(|index| index as u32))
        .collect();
    let all = match seed {
        Some(seed) => concat(&[seed.as_ref(), values.as_ref()]),
        None => Ok(values.clone()),
    };
    all.and_then(|all| take(all.as_ref(), &indices, None))
        .map_err(|e| failure(e.to_string()))
}

/// Behavior: java.lang.Math#max(double, double) and #min: NaN if either is NaN, and `-0.0`
/// is less than `0.0`
pub(super) fn java_extreme(a: f64, b: f64, max: bool) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == b {
        // Equal values differ only in the sign of zero.
        if a.is_sign_negative() == max {
            b
        } else {
            a
        }
    } else if (a > b) == max {
        a
    } else {
        b
    }
}
