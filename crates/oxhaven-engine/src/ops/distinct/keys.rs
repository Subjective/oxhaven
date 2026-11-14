//! Keys of rows as bytes: two rows have the same key exactly when their bytes are equal.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::table::TableFailure;

/// The key of every row of `columns`, which hold one value per row each.
///
/// A null is a value like any other and equal only to a null. All NaNs are one value, and
/// `-0.0` and `0.0` are one value. Strings are equal when their characters are.
///
/// Behavior: the `eq` of io.deephaven.util.compare.ByteComparisons, CharComparisons,
/// ShortComparisons, IntComparisons, LongComparisons, FloatComparisons, DoubleComparisons
/// and ObjectComparisons, which the hashers generated for
/// io.deephaven.engine.table.impl.by.typed.TypedHasherFactory compare keys with
pub(super) fn encode(columns: &[ArrayRef], rows: usize) -> Result<Vec<Vec<u8>>, TableFailure> {
    let mut keys = vec![Vec::new(); rows];
    for values in columns {
        append(values, &mut keys)?;
    }
    Ok(keys)
}

fn float(value: f32) -> u32 {
    if value.is_nan() {
        f32::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

fn double(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0
    } else {
        value.to_bits()
    }
}

/// Appends the values of one key column to the keys. A null is the byte 0; a value is the
/// byte 1 and then its bytes, which have a fixed length or are preceded by their length,
/// so that the keys of several columns cannot run into each other.
fn append(values: &ArrayRef, keys: &mut [Vec<u8>]) -> Result<(), TableFailure> {
    if values.len() != keys.len() {
        return Err(TableFailure {
            message: format!(
                "a key column has {} values for {} rows",
                values.len(),
                keys.len()
            ),
        });
    }
    macro_rules! primitive {
        ($arrow:ty, $bytes:expr) => {{
            let values = values.as_primitive::<$arrow>();
            for (row, key) in keys.iter_mut().enumerate() {
                if values.is_null(row) {
                    key.push(0);
                } else {
                    key.push(1);
                    key.extend_from_slice(&$bytes(values.value(row)));
                }
            }
        }};
    }
    match values.data_type() {
        DataType::Boolean => {
            let values = values.as_boolean();
            for (row, key) in keys.iter_mut().enumerate() {
                if values.is_null(row) {
                    key.push(0);
                } else {
                    key.extend_from_slice(&[1, u8::from(values.value(row))]);
                }
            }
        }
        DataType::Int8 => primitive!(Int8Type, i8::to_le_bytes),
        DataType::UInt16 => primitive!(UInt16Type, u16::to_le_bytes),
        DataType::Int16 => primitive!(Int16Type, i16::to_le_bytes),
        DataType::Int32 => primitive!(Int32Type, i32::to_le_bytes),
        DataType::Int64 => primitive!(Int64Type, i64::to_le_bytes),
        DataType::Float32 => primitive!(Float32Type, |value| float(value).to_le_bytes()),
        DataType::Float64 => primitive!(Float64Type, |value| double(value).to_le_bytes()),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            primitive!(TimestampNanosecondType, i64::to_le_bytes)
        }
        DataType::Utf8 => {
            let values = values.as_string::<i32>();
            for (row, key) in keys.iter_mut().enumerate() {
                if values.is_null(row) {
                    key.push(0);
                } else {
                    let text = values.value(row).as_bytes();
                    key.push(1);
                    key.extend_from_slice(&(text.len() as u64).to_le_bytes());
                    key.extend_from_slice(text);
                }
            }
        }
        other => {
            return Err(TableFailure {
                message: format!("a key column of the Arrow type {other} is not supported"),
            })
        }
    }
    Ok(())
}
