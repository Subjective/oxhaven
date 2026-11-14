//! Stamps: the values of the as-of column of a row, in a form whose order is the order
//! Java's stamp kernels and sorted arrays use.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use std::cmp::Ordering;

/// A floating point value that is not NaN, with one zero.
#[derive(Clone, Copy, Debug)]
pub(super) struct Finite(f64);

impl PartialEq for Finite {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Finite {}

impl PartialOrd for Finite {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Finite {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// One stamp. The order of the variants and of their contents is Java's order of the
/// values of one column type: null is the least value and NaN the greatest, every NaN
/// equals every NaN, and `-0.0` equals `0.0`. A string orders by its UTF-16 code units. A
/// `Boolean` is the byte Java reinterprets it as (false 0, true 1) and an `Instant` its
/// nanoseconds, which is why Java joins a `Boolean` stamp with a `byte` one and an
/// `Instant` stamp with a `long` one. A `float` is widened to `double`, which keeps the
/// order.
///
/// Behavior: the `doComparison` and `eq` of the kernels under
/// io.deephaven.engine.table.impl.join.stamp and
/// io.deephaven.engine.table.impl.join.dupcompact, which call
/// io.deephaven.util.compare.DoubleComparisons#compare, FloatComparisons#compare,
/// CharComparisons#compare and ObjectComparisons#compare, and the natural order of the
/// integer types, whose null sentinel is their least value;
/// io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive for
/// `Boolean` and `Instant`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Stamp {
    Null,
    Int(i64),
    Float(Finite),
    Text(Box<[u16]>),
    NaN,
}

fn float_stamp(value: f64) -> Stamp {
    if value.is_nan() {
        Stamp::NaN
    } else if value == 0.0 {
        Stamp::Float(Finite(0.0))
    } else {
        Stamp::Float(Finite(value))
    }
}

/// What Java reads a column as when it is a stamp or an exact-match column of an as-of
/// join: the chunk type of the column after `Boolean` and `Instant` are reinterpreted. Two
/// columns can be compared when they have the same class.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive
/// as called by io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Primitive {
    Byte,
    Char,
    Short,
    Int,
    Long,
    Float,
    Double,
    Object,
}

pub(super) fn primitive(column_type: &ColumnType) -> Primitive {
    match column_type {
        ColumnType::Boolean | ColumnType::Byte => Primitive::Byte,
        ColumnType::Char => Primitive::Char,
        ColumnType::Short => Primitive::Short,
        ColumnType::Int => Primitive::Int,
        ColumnType::Long | ColumnType::Instant => Primitive::Long,
        ColumnType::Float => Primitive::Float,
        ColumnType::Double => Primitive::Double,
        ColumnType::String => Primitive::Object,
        other => unported!(
            "io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin",
            "an as-of join on a column of type {other:?}"
        ),
    }
}

fn stamps_of(values: &ArrayRef) -> Vec<Stamp> {
    fn each<A: Array + 'static>(
        values: &ArrayRef,
        stamp: impl Fn(&A, usize) -> Stamp,
    ) -> Vec<Stamp> {
        let Some(array) = values.as_any().downcast_ref::<A>() else {
            panic!("a {} array of another class", values.data_type());
        };
        (0..array.len())
            .map(|row| {
                if array.is_null(row) {
                    Stamp::Null
                } else {
                    stamp(array, row)
                }
            })
            .collect()
    }
    use arrow::datatypes::{DataType, TimeUnit};
    match values.data_type() {
        DataType::Boolean => each::<BooleanArray>(values, |a, row| Stamp::Int(a.value(row).into())),
        DataType::Int8 => each::<Int8Array>(values, |a, row| Stamp::Int(a.value(row).into())),
        DataType::Int16 => each::<Int16Array>(values, |a, row| Stamp::Int(a.value(row).into())),
        DataType::Int32 => each::<Int32Array>(values, |a, row| Stamp::Int(a.value(row).into())),
        DataType::Int64 => each::<Int64Array>(values, |a, row| Stamp::Int(a.value(row))),
        DataType::UInt16 => each::<UInt16Array>(values, |a, row| Stamp::Int(a.value(row).into())),
        DataType::Float32 => {
            each::<Float32Array>(values, |a, row| float_stamp(a.value(row).into()))
        }
        DataType::Float64 => each::<Float64Array>(values, |a, row| float_stamp(a.value(row))),
        DataType::Utf8 => each::<StringArray>(values, |a, row| {
            Stamp::Text(a.value(row).encode_utf16().collect())
        }),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            each::<TimestampNanosecondArray>(values, |a, row| Stamp::Int(a.value(row)))
        }
        other => unported!(
            "io.deephaven.engine.table.impl.join.stamp.StampKernel#makeStampKernel",
            "a stamp column stored as {other}"
        ),
    }
}

/// The stamp of each row of `rows`, in row order, from column `column` of `table`; with
/// `use_prev`, as of the end of the previous cycle for `rows` of the previous key space.
pub(super) fn read(
    table: &dyn Table,
    column: usize,
    rows: &RowSet,
    use_prev: bool,
) -> Result<Vec<Stamp>, TableFailure> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let source = table.column(column);
    let values = if use_prev {
        source.prev_chunk(rows)?
    } else {
        source.chunk(rows)?
    };
    Ok(stamps_of(&values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn doubles_order_as_java_orders_them() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        let found = stamps_of(
            &(Arc::new(Float64Array::from(vec![
                None,
                Some(f64::NEG_INFINITY),
                Some(-1.5),
                Some(-0.0),
                Some(0.0),
                Some(2.0),
                Some(f64::INFINITY),
                Some(f64::NAN),
                Some(other_nan),
            ])) as ArrayRef),
        );
        // null, -Infinity, the finite values, Infinity, NaN; the zeros and the NaNs tie.
        for pair in found.windows(2) {
            assert!(pair[0] <= pair[1], "{pair:?}");
        }
        assert_eq!(found[3], found[4]);
        assert_eq!(found[7], found[8]);
        let distinct = [0, 1, 2, 3, 5, 6, 7];
        for pair in distinct.windows(2) {
            assert!(found[pair[0]] < found[pair[1]], "{pair:?}");
        }
    }

    #[test]
    fn a_float_orders_as_the_double_of_its_value() {
        let floats = stamps_of(
            &(Arc::new(Float32Array::from(vec![
                None,
                Some(-0.0f32),
                Some(0.1),
                Some(f32::NAN),
            ])) as ArrayRef),
        );
        assert_eq!(
            floats,
            vec![
                Stamp::Null,
                float_stamp(0.0),
                float_stamp(f64::from(0.1f32)),
                Stamp::NaN
            ]
        );
    }

    #[test]
    fn strings_order_by_utf16_code_units() {
        // U+FF5E is one code unit, 0xFF5E; U+1F600 is the surrogates 0xD83D 0xDE00. By code
        // point the second is greater, by code unit the first.
        let found = stamps_of(
            &(Arc::new(StringArray::from(vec![
                Some("\u{ff5e}"),
                Some("\u{1f600}"),
                Some(""),
                None,
            ])) as ArrayRef),
        );
        assert!(found[1] < found[0]);
        assert!(found[2] < found[1]);
        assert!(found[3] < found[2]);
    }

    #[test]
    fn a_boolean_is_its_byte_and_an_instant_its_nanoseconds() {
        let booleans = stamps_of(
            &(Arc::new(BooleanArray::from(vec![None, Some(false), Some(true)])) as ArrayRef),
        );
        let bytes =
            stamps_of(&(Arc::new(Int8Array::from(vec![None, Some(0), Some(1)])) as ArrayRef));
        assert_eq!(booleans, bytes);
        let instants = stamps_of(
            &(Arc::new(TimestampNanosecondArray::from(vec![None, Some(-7)]).with_timezone("UTC"))
                as ArrayRef),
        );
        let longs = stamps_of(&(Arc::new(Int64Array::from(vec![None, Some(-7)])) as ArrayRef));
        assert_eq!(instants, longs);
        assert_eq!(
            primitive(&ColumnType::Boolean),
            primitive(&ColumnType::Byte)
        );
        assert_eq!(
            primitive(&ColumnType::Instant),
            primitive(&ColumnType::Long)
        );
        assert_ne!(primitive(&ColumnType::Int), primitive(&ColumnType::Long));
        assert_ne!(
            primitive(&ColumnType::Float),
            primitive(&ColumnType::Double)
        );
    }

    #[test]
    fn integers_and_chars_order_by_value_with_null_first() {
        let chars = stamps_of(
            &(Arc::new(UInt16Array::from(vec![None, Some(0), Some(0xFFFE)])) as ArrayRef),
        );
        assert!(chars[0] < chars[1] && chars[1] < chars[2]);
        let shorts =
            stamps_of(&(Arc::new(Int16Array::from(vec![None, Some(-32767), Some(5)])) as ArrayRef));
        assert!(shorts[0] < shorts[1] && shorts[1] < shorts[2]);
    }
}
