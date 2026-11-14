//! Java's sentinel null values, and conversion between them and Arrow validity bits.
//!
//! Oxhaven stores nulls as validity bits. The Java engine stores a null of a primitive type as
//! a reserved value of that type, and that stays observable in two places (PARITY.md, "Null
//! representation in the Java engine" and "Wire representation"):
//!
//! - a computed value that equals the sentinel is null: use [`sentinels_to_nulls`];
//! - with the Barrage option `useDeephavenNulls`, primitive columns travel without a validity
//!   buffer and with the sentinel in place of each null: use [`nulls_to_sentinels`] to write
//!   and [`sentinels_to_nulls`] to read.
//!
//! `Boolean` and `String` have no sentinel in a column of their own type and always keep
//! their validity bits. A `Boolean` reinterpreted as a byte does: see [`boolean_to_bytes`].

use crate::failure;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, Int8Array, PrimitiveArray,
};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

/// Behavior: io.deephaven.util.QueryConstants#NULL_BYTE
pub const NULL_BYTE: i8 = i8::MIN;
/// Behavior: io.deephaven.util.QueryConstants#NULL_CHAR
pub const NULL_CHAR: u16 = u16::MAX;
/// Behavior: io.deephaven.util.QueryConstants#NULL_SHORT
pub const NULL_SHORT: i16 = i16::MIN;
/// Behavior: io.deephaven.util.QueryConstants#NULL_INT
pub const NULL_INT: i32 = i32::MIN;
/// Behavior: io.deephaven.util.QueryConstants#NULL_LONG. Also the null of an `Instant` held as
/// nanoseconds.
pub const NULL_LONG: i64 = i64::MIN;
/// Behavior: io.deephaven.util.QueryConstants#NULL_FLOAT
pub const NULL_FLOAT: f32 = -f32::MAX;
/// Behavior: io.deephaven.util.QueryConstants#NULL_DOUBLE
pub const NULL_DOUBLE: f64 = -f64::MAX;
/// Behavior: io.deephaven.util.BooleanUtils#NULL_BOOLEAN_AS_BYTE
pub const NULL_BOOLEAN_AS_BYTE: i8 = NULL_BYTE;
/// Behavior: io.deephaven.util.BooleanUtils#TRUE_BOOLEAN_AS_BYTE
pub const TRUE_BOOLEAN_AS_BYTE: i8 = 1;
/// Behavior: io.deephaven.util.BooleanUtils#FALSE_BOOLEAN_AS_BYTE
pub const FALSE_BOOLEAN_AS_BYTE: i8 = 0;

/// The two directions of conversion, applied to one primitive array.
#[derive(Clone, Copy)]
enum Direction {
    ToSentinels,
    ToNulls,
}

fn convert_primitive<T: ArrowPrimitiveType>(
    array: &dyn Array,
    sentinel: T::Native,
    direction: Direction,
) -> ArrayRef
where
    T::Native: PartialEq,
{
    let array = array.as_primitive::<T>();
    let converted = match direction {
        Direction::ToSentinels => {
            let values: ScalarBuffer<T::Native> =
                array.iter().map(|v| v.unwrap_or(sentinel)).collect();
            PrimitiveArray::<T>::new(values, None)
        }
        Direction::ToNulls => {
            let valid: NullBuffer = array
                .iter()
                .map(|v| v.is_some_and(|v| v != sentinel))
                .collect();
            let valid = (valid.null_count() > 0).then_some(valid);
            PrimitiveArray::<T>::new(array.values().clone(), valid)
        }
    };
    Arc::new(converted.with_data_type(array.data_type().clone()))
}

fn convert(array: &ArrayRef, direction: Direction) -> Result<ArrayRef, TableFailure> {
    let a = array.as_ref();
    Ok(match array.data_type() {
        DataType::Boolean | DataType::Utf8 => array.clone(),
        DataType::Int8 => convert_primitive::<Int8Type>(a, NULL_BYTE, direction),
        DataType::UInt16 => convert_primitive::<UInt16Type>(a, NULL_CHAR, direction),
        DataType::Int16 => convert_primitive::<Int16Type>(a, NULL_SHORT, direction),
        DataType::Int32 => convert_primitive::<Int32Type>(a, NULL_INT, direction),
        DataType::Int64 => convert_primitive::<Int64Type>(a, NULL_LONG, direction),
        DataType::Float32 => convert_primitive::<Float32Type>(a, NULL_FLOAT, direction),
        DataType::Float64 => convert_primitive::<Float64Type>(a, NULL_DOUBLE, direction),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            convert_primitive::<TimestampNanosecondType>(a, NULL_LONG, direction)
        }
        other => {
            return Err(failure(format!(
                "no Deephaven null sentinel is defined for Arrow type {other}"
            )))
        }
    })
}

/// Replaces each null with its type's sentinel and drops the validity buffer, for an array of
/// any column type's Arrow type. `Boolean` and `String` arrays are returned unchanged, with
/// their validity: Java's writers for them do not use sentinels.
///
/// Behavior: io.deephaven.extensions.barrage.chunk.BaseChunkWriter.BaseChunkInputStream
/// (constructor, `dhNullable && options.useDeephavenNulls()`), the `dhNullable` argument of
/// each `*ChunkWriter` constructor, and
/// io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory#timestampFromInstant
pub fn nulls_to_sentinels(array: &ArrayRef) -> Result<ArrayRef, TableFailure> {
    convert(array, Direction::ToSentinels)
}

/// Turns each value equal to its type's sentinel into a null; existing nulls stay null and
/// the values are untouched. `Boolean` and `String` arrays are returned unchanged.
///
/// Behavior: io.deephaven.extensions.barrage.chunk.LongChunkReader#useDeephavenNulls, and for
/// computed values the `isNull` checks of io.deephaven.util.QueryConstants sentinels throughout
/// the engine.
pub fn sentinels_to_nulls(array: &ArrayRef) -> Result<ArrayRef, TableFailure> {
    convert(array, Direction::ToNulls)
}

/// A `Boolean` column as Java's byte reinterpretation: null is -128, false is 0, true is 1.
/// The result has no validity buffer.
///
/// Behavior: io.deephaven.util.BooleanUtils#booleanAsByte, as used by
/// io.deephaven.engine.table.impl.sources.ReinterpretUtils#booleanToByteSource
pub fn boolean_to_bytes(array: &BooleanArray) -> Int8Array {
    let bytes = array.iter().map(|v| match v {
        None => NULL_BOOLEAN_AS_BYTE,
        Some(true) => TRUE_BOOLEAN_AS_BYTE,
        Some(false) => FALSE_BOOLEAN_AS_BYTE,
    });
    Int8Array::new(bytes.collect(), None)
}

/// The inverse of [`boolean_to_bytes`]: -128 or a null is null, any other value is true when
/// greater than 0 and false otherwise.
///
/// Behavior: io.deephaven.util.BooleanUtils#byteAsBoolean, as used by
/// io.deephaven.engine.table.impl.sources.ReinterpretUtils#byteToBooleanSource
pub fn bytes_to_boolean(array: &Int8Array) -> BooleanArray {
    array
        .iter()
        .map(|v| match v {
            None | Some(NULL_BOOLEAN_AS_BYTE) => None,
            Some(byte) => Some(byte > FALSE_BOOLEAN_AS_BYTE),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt16Array,
    };
    use oxhaven_contracts::schema::ColumnType;

    #[test]
    fn constants_equal_the_java_values() {
        assert_eq!(NULL_BYTE, -128);
        assert_eq!(NULL_SHORT, -32768);
        assert_eq!(NULL_INT, -2_147_483_648);
        assert_eq!(NULL_LONG, -9_223_372_036_854_775_808);
        assert_eq!(NULL_CHAR, 0xFFFF);
        // -Float.MAX_VALUE and -Double.MAX_VALUE as raw bits.
        assert_eq!(NULL_FLOAT.to_bits(), 0xFF7F_FFFF);
        assert_eq!(NULL_DOUBLE.to_bits(), 0xFFEF_FFFF_FFFF_FFFF);
        assert_eq!(
            (
                NULL_BOOLEAN_AS_BYTE,
                FALSE_BOOLEAN_AS_BYTE,
                TRUE_BOOLEAN_AS_BYTE
            ),
            (-128, 0, 1)
        );
    }

    /// For each sentinel type: an array with a value, a null and a second value; the same
    /// with the sentinel in place of the null.
    fn cases() -> Vec<(ColumnType, ArrayRef, ArrayRef)> {
        let utc = |a: TimestampNanosecondArray| -> ArrayRef { Arc::new(a.with_timezone("UTC")) };
        vec![
            (
                ColumnType::Byte,
                Arc::new(Int8Array::from(vec![Some(1), None, Some(-127)])),
                Arc::new(Int8Array::from(vec![1, -128, -127])),
            ),
            (
                ColumnType::Char,
                Arc::new(UInt16Array::from(vec![Some(65), None, Some(0xFFFE)])),
                Arc::new(UInt16Array::from(vec![65, 0xFFFF, 0xFFFE])),
            ),
            (
                ColumnType::Short,
                Arc::new(Int16Array::from(vec![Some(1), None, Some(-32767)])),
                Arc::new(Int16Array::from(vec![1, i16::MIN, -32767])),
            ),
            (
                ColumnType::Int,
                Arc::new(Int32Array::from(vec![Some(1), None, Some(i32::MIN + 1)])),
                Arc::new(Int32Array::from(vec![1, i32::MIN, i32::MIN + 1])),
            ),
            (
                ColumnType::Long,
                Arc::new(Int64Array::from(vec![Some(1), None, Some(i64::MIN + 1)])),
                Arc::new(Int64Array::from(vec![1, i64::MIN, i64::MIN + 1])),
            ),
            (
                ColumnType::Float,
                Arc::new(Float32Array::from(vec![
                    Some(1.5),
                    None,
                    Some(f32::MIN_POSITIVE),
                ])),
                Arc::new(Float32Array::from(vec![1.5, -f32::MAX, f32::MIN_POSITIVE])),
            ),
            (
                ColumnType::Double,
                Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    None,
                    Some(f64::NEG_INFINITY),
                ])),
                Arc::new(Float64Array::from(vec![1.5, -f64::MAX, f64::NEG_INFINITY])),
            ),
            (
                ColumnType::Instant,
                utc(TimestampNanosecondArray::from(vec![
                    Some(1),
                    None,
                    Some(-5),
                ])),
                utc(TimestampNanosecondArray::from(vec![1, i64::MIN, -5])),
            ),
        ]
    }

    #[test]
    fn nulls_become_sentinels_without_a_validity_buffer() {
        for (column_type, with_nulls, with_sentinels) in cases() {
            let converted = nulls_to_sentinels(&with_nulls).unwrap();
            assert_eq!(converted.data_type(), &column_type.arrow_type());
            assert!(converted.nulls().is_none(), "{column_type:?}");
            assert_eq!(
                converted.to_data(),
                with_sentinels.to_data(),
                "{column_type:?}"
            );
        }
    }

    #[test]
    fn sentinels_become_nulls() {
        for (column_type, with_nulls, with_sentinels) in cases() {
            let converted = sentinels_to_nulls(&with_sentinels).unwrap();
            assert_eq!(converted.data_type(), &column_type.arrow_type());
            assert_eq!(converted.to_data(), with_nulls.to_data(), "{column_type:?}");
            // An array that already has nulls keeps them.
            let again = sentinels_to_nulls(&with_nulls).unwrap();
            assert_eq!(again.to_data(), with_nulls.to_data(), "{column_type:?}");
        }
    }

    #[test]
    fn arrays_without_nulls_or_sentinels_keep_values_and_have_no_validity() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        for converted in [
            nulls_to_sentinels(&values).unwrap(),
            sentinels_to_nulls(&values).unwrap(),
        ] {
            assert!(converted.nulls().is_none());
            assert_eq!(converted.to_data(), values.to_data());
        }
    }

    #[test]
    fn nan_and_negative_zero_are_not_sentinels() {
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![f64::NAN, -0.0, f64::MIN_POSITIVE]));
        assert_eq!(sentinels_to_nulls(&values).unwrap().null_count(), 0);
    }

    #[test]
    fn boolean_and_string_keep_their_validity() {
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a")]));
        for array in [booleans, strings] {
            assert_eq!(
                nulls_to_sentinels(&array).unwrap().to_data(),
                array.to_data()
            );
            assert_eq!(
                sentinels_to_nulls(&array).unwrap().to_data(),
                array.to_data()
            );
        }
    }

    #[test]
    fn types_without_a_defined_sentinel_are_errors() {
        let unsigned: ArrayRef = Arc::new(arrow::array::UInt32Array::from(vec![1]));
        assert!(nulls_to_sentinels(&unsigned).is_err());
        assert!(sentinels_to_nulls(&unsigned).is_err());
    }

    #[test]
    fn boolean_as_byte_uses_minus_128_zero_and_one() {
        let booleans = BooleanArray::from(vec![Some(true), Some(false), None]);
        let bytes = boolean_to_bytes(&booleans);
        assert!(bytes.nulls().is_none());
        assert_eq!(bytes.values().as_ref(), &[1, 0, -128]);
        assert_eq!(bytes_to_boolean(&bytes), booleans);
        // Java reads any byte above zero as true and any other non-null byte as false.
        let odd = Int8Array::from(vec![Some(2), Some(-1), None, Some(-128)]);
        assert_eq!(
            bytes_to_boolean(&odd),
            BooleanArray::from(vec![Some(true), Some(false), None, None])
        );
    }
}
