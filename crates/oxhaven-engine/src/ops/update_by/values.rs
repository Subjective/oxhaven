//! Column values as the kernels see them, and the arrays kernels give back.
//!
//! Java's operators work on primitive values in which a null is a reserved value of the
//! type, and several results depend on that: a sum that wraps onto the reserved value reads
//! as null and restarts. The kernels here keep nulls apart (`Option`) and call
//! [`long_result`] or [`double_result`] wherever Java's arithmetic could land on the reserved
//! value, so the outcome is the same without storing sentinels.

use super::failure;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
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
            other => {
                return Err(failure(format!(
                    "update_by cannot read a column of type {other:?}"
                )))
            }
        })
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Values::Int(v) => v.len(),
            Values::Float(v) => v.len(),
            Values::Text(v) => v.len(),
        }
    }

    pub(super) fn is_null(&self, index: usize) -> bool {
        match self {
            Values::Int(v) => v[index].is_none(),
            Values::Float(v) => v[index].is_none(),
            Values::Text(v) => v[index].is_none(),
        }
    }

    /// How the non-null values at `a` and `b` compare, as Java's `<`, `>` and `==` on
    /// primitives or `compareTo` on a `String` do. `None` when a NaN is involved, for which
    /// all three primitive comparisons are false.
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
        }
    }
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
