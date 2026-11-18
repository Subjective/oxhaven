//! Stamps: the values of the as-of column of a row, in a form whose order is the order
//! Java's stamp kernels and sorted arrays use.

use crate::ops::key_table::groupable_vector;
use crate::ops::sort::big_number_key;
use crate::ops::zoned;
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Date32Array, DurationNanosecondArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    Time64NanosecondArray, TimestampNanosecondArray, UInt16Array,
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
/// The `java.time` classes are objects that Java compares with `compareTo`, null first.
/// A `LocalDate` (days), `LocalTime` (nanoseconds of the day), `LocalDateTime`
/// (nanoseconds from 1970-01-01T00:00), `Duration` (nanoseconds) and `DayOfWeek` (Monday 1
/// to Sunday 7) order as the count a cell holds. A `ZonedDateTime` orders by its instant,
/// then by its local date and time, which for one instant is the order of the zones'
/// offsets, then by its zone's id; it equals another exactly when `equals` says so.
///
/// A `BigInteger` orders by its value. A vector orders element by element, each element
/// by the order of its type here, and a vector that is the start of another before it;
/// for the vectors [`incomparable`] takes, two vectors that compare equal are equal.
///
/// Behavior: the `doComparison` and `eq` of the kernels under
/// io.deephaven.engine.table.impl.join.stamp and
/// io.deephaven.engine.table.impl.join.dupcompact, which call
/// io.deephaven.util.compare.DoubleComparisons#compare, FloatComparisons#compare,
/// CharComparisons#compare and ObjectComparisons#compare, and the natural order of the
/// integer types, whose null sentinel is their least value;
/// io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive for
/// `Boolean` and `Instant`; java.time.LocalDate#compareTo, LocalTime#compareTo,
/// LocalDateTime#compareTo, Duration#compareTo, java.lang.Enum#compareTo and
/// java.time.chrono.ChronoZonedDateTime#compareTo; java.math.BigInteger#compareTo,
/// io.deephaven.vector.IntVector#compareTo and ObjectVector#compareTo (seen on Deephaven
/// 42.9: `aj` and `raj` on `BigInteger` and `IntVector` stamps).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Stamp {
    Null,
    Int(i64),
    Float(Finite),
    Text(Box<[u16]>),
    /// A `ZonedDateTime`: its instant in nanoseconds, its zone's offset at that instant in
    /// seconds, and its zone's id as UTF-16 code units.
    Zoned(i64, i32, Box<[u16]>),
    /// A `BigInteger`, as bytes that order as the numbers (the sort's key of one).
    Big(Box<[u8]>),
    /// The elements of a vector.
    List(Box<[Stamp]>),
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
/// join: the chunk type of the column after `Boolean` and `Instant` are reinterpreted.
/// Every class that is not reinterpreted is read as objects, the `java.time` classes among
/// them (a `ZonedDateTime` made by a formula is not reinterpreted: only a column source
/// that keeps the zone apart is).
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
        _ => Primitive::Object,
    }
}

/// Whether two stamp columns can be joined. Java compares the classes of the two columns
/// after `Boolean` and `Instant` are reinterpreted: a `Boolean` stamp joins a `byte` one
/// and an `Instant` a `long` one, and two object stamps join only when they hold one class.
///
/// Behavior: io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin, its check of
/// `getType()` of the two stamp sources
pub(super) fn same_stamp_class(left: &ColumnType, right: &ColumnType) -> bool {
    let class = primitive(left);
    class == primitive(right) && (class != Primitive::Object || left == right)
}

/// Why a stamp column of `column_type` cannot be joined on, as the text of the exception
/// Java throws once it compares two of its values; `None` when it can be.
///
/// A `Period` is not a `Comparable`, nor is a `ZoneId` that names a region (a
/// `ZoneRegion`): Java fails with a `ClassCastException` when it sorts two stamps of one
/// key or compares a left stamp with a right one, unless the two are one object
/// (`ObjectComparisons#compare` returns 0 for identical references before it casts). This
/// server has no object identity, and refuses such a column before it computes anything.
///
/// Behavior: io.deephaven.util.compare.ObjectComparisons#compare as called by the sort
/// kernels and the stamp kernels of the as-of join
pub(super) fn incomparable(column_type: &ColumnType) -> Option<String> {
    let class = match column_type {
        ColumnType::Period => "java.time.Period",
        ColumnType::ZoneId => "java.time.ZoneRegion",
        ColumnType::Boolean
        | ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Instant
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::String
        | ColumnType::LocalDate
        | ColumnType::LocalTime
        | ColumnType::LocalDateTime
        | ColumnType::Duration
        | ColumnType::DayOfWeek
        | ColumnType::ZonedDateTime
        | ColumnType::BigInteger => return None,
        // Seen on Deephaven 42.9: vector stamps join by `compareTo`, element by element.
        vector if groupable_vector(vector) => return None,
        // A `BigDecimal` orders by `compareTo` (`3` and `3.000` tie) but Java removes
        // duplicate right stamps by `equals`, so which of tied right stamps of two scales
        // a left row matches follows the binary search (owner item 7.18). Vectors of
        // `float` or `double` likewise (7.6); arrays, tables and objects are compared by
        // identity or fail (2.2, 2.3).
        other => unported!(
            "io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin",
            "an as-of join on a column of type {other:?}"
        ),
    };
    Some(format!(
        "class {class} cannot be cast to class java.lang.Comparable ({class} and \
         java.lang.Comparable are in module java.base of loader 'bootstrap')"
    ))
}

fn stamps_of(values: &ArrayRef) -> Result<Vec<Stamp>, TableFailure> {
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
    Ok(match values.data_type() {
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
        DataType::Date32 => each::<Date32Array>(values, |a, row| Stamp::Int(a.value(row).into())),
        DataType::Time64(TimeUnit::Nanosecond) => {
            each::<Time64NanosecondArray>(values, |a, row| Stamp::Int(a.value(row)))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            each::<DurationNanosecondArray>(values, |a, row| Stamp::Int(a.value(row)))
        }
        DataType::Struct(_) => {
            let (cells, instants, zones) = zoned::parts(values)?;
            let mut stamps = Vec::with_capacity(cells.len());
            for (row, &instant) in instants.iter().enumerate() {
                stamps.push(if cells.is_null(row) {
                    Stamp::Null
                } else {
                    let zone = zones.value(row);
                    Stamp::Zoned(
                        instant,
                        zoned::offset_seconds(zone, instant)?,
                        zone.encode_utf16().collect(),
                    )
                });
            }
            stamps
        }
        // A `BigInteger`: `incomparable` refuses `BigDecimal` stamps before they are read.
        DataType::Binary => each::<BinaryArray>(values, |a, row| {
            Stamp::Big(big_number_key(0, a.value(row)).into())
        }),
        DataType::List(_) => {
            let lists = values.as_list::<i32>();
            let elements = stamps_of(lists.values())?;
            let offsets = lists.value_offsets();
            (0..lists.len())
                .map(|row| {
                    if lists.is_null(row) {
                        Stamp::Null
                    } else {
                        let range = offsets[row] as usize..offsets[row + 1] as usize;
                        Stamp::List(elements[range].into())
                    }
                })
                .collect()
        }
        other => unported!(
            "io.deephaven.engine.table.impl.join.stamp.StampKernel#makeStampKernel",
            "a stamp column stored as {other}"
        ),
    })
}

/// A form of stamp the join can keep: values that order as Java orders the stamps of one
/// column, made from the column's values. Every stamp column has one of two forms:
/// [`Word`] for the columns of 64 bits or fewer and [`Stamp`] for the others.
pub(super) trait StampValue: Ord + Clone + Send + Sync + std::fmt::Debug + 'static {
    /// The stamp of each value of `values`, in order.
    fn of(values: &ArrayRef) -> Result<Vec<Self>, TableFailure>;
}

impl StampValue for Stamp {
    fn of(values: &ArrayRef) -> Result<Vec<Self>, TableFailure> {
        stamps_of(values)
    }
}

/// The stamp of a column of 64 bits or fewer: `None` for null, and otherwise a number
/// whose order is the order of [`Stamp`] for the same values. An integer, boolean or time
/// value is its count as in [`Stamp::Int`]; a floating point value is its bits as an
/// integer that orders as `f64::total_cmp`, after every NaN is made one NaN and `-0.0`
/// `0.0`, so that NaN is the greatest and the zeros tie (as [`Stamp::Float`] and
/// [`Stamp::NaN`]).
pub(super) type Word = Option<i64>;

/// The ordered number of a `double`, as [`Word`] describes.
fn float_word(value: f64) -> i64 {
    let canonical = if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    };
    let bits = canonical.to_bits() as i64;
    // Negative values order backwards by their bits: flip all but the sign bit.
    bits ^ (((bits >> 63) as u64) >> 1) as i64
}

/// Whether a stamp column stored as `data_type` is kept as a [`Word`].
pub(super) fn is_word(data_type: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::{DataType, TimeUnit};
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(TimeUnit::Nanosecond, _)
            | DataType::Date32
            | DataType::Time64(TimeUnit::Nanosecond)
            | DataType::Duration(TimeUnit::Nanosecond)
    )
}

impl StampValue for Word {
    fn of(values: &ArrayRef) -> Result<Vec<Self>, TableFailure> {
        use arrow::array::AsArray;
        use arrow::datatypes::{
            DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type, Int16Type,
            Int32Type, Int64Type, Int8Type, Time64NanosecondType, TimeUnit,
            TimestampNanosecondType, UInt16Type,
        };
        fn each<T: Copy>(values: &ArrayRef, slice: &[T], word: impl Fn(T) -> i64) -> Vec<Word> {
            match values.nulls().filter(|nulls| nulls.null_count() > 0) {
                None => slice.iter().map(|&value| Some(word(value))).collect(),
                Some(nulls) => slice
                    .iter()
                    .enumerate()
                    .map(|(row, &value)| nulls.is_valid(row).then(|| word(value)))
                    .collect(),
            }
        }
        Ok(match values.data_type() {
            DataType::Boolean => {
                let booleans = values.as_boolean();
                (0..booleans.len())
                    .map(|row| booleans.is_valid(row).then(|| booleans.value(row).into()))
                    .collect()
            }
            DataType::Int8 => each(
                values,
                values.as_primitive::<Int8Type>().values(),
                i64::from,
            ),
            DataType::Int16 => each(
                values,
                values.as_primitive::<Int16Type>().values(),
                i64::from,
            ),
            DataType::Int32 => each(
                values,
                values.as_primitive::<Int32Type>().values(),
                i64::from,
            ),
            DataType::Int64 => each(values, values.as_primitive::<Int64Type>().values(), |v| v),
            DataType::UInt16 => each(
                values,
                values.as_primitive::<UInt16Type>().values(),
                i64::from,
            ),
            DataType::Float32 => each(
                values,
                values.as_primitive::<Float32Type>().values(),
                |v: f32| float_word(v.into()),
            ),
            DataType::Float64 => each(
                values,
                values.as_primitive::<Float64Type>().values(),
                float_word,
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => each(
                values,
                values.as_primitive::<TimestampNanosecondType>().values(),
                |v| v,
            ),
            DataType::Date32 => each(
                values,
                values.as_primitive::<Date32Type>().values(),
                i64::from,
            ),
            DataType::Time64(TimeUnit::Nanosecond) => each(
                values,
                values.as_primitive::<Time64NanosecondType>().values(),
                |v| v,
            ),
            DataType::Duration(TimeUnit::Nanosecond) => each(
                values,
                values.as_primitive::<DurationNanosecondType>().values(),
                |v| v,
            ),
            other => panic!("a stamp column stored as {other} is not kept as a word"),
        })
    }
}

/// The stamp of each row of `rows`, in row order, from column `column` of `table`; with
/// `use_prev`, as of the end of the previous cycle for `rows` of the previous key space.
pub(super) fn read<S: StampValue>(
    table: &dyn Table,
    column: usize,
    rows: &RowSet,
    use_prev: bool,
) -> Result<Vec<S>, TableFailure> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let source = table.column(column);
    let values = if use_prev {
        source.prev_chunk(rows)?
    } else {
        source.chunk(rows)?
    };
    S::of(&values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::zoned::tests::zoned;
    use std::sync::Arc;

    fn of(values: &ArrayRef) -> Vec<Stamp> {
        stamps_of(values).unwrap()
    }

    #[test]
    fn a_zoned_date_time_orders_by_instant_then_local_time_then_zone() {
        // 2023-11-14T22:13:20Z: in New York 17:13 (offset -5h), in UTC 22:13, in Tokyo
        // 07:13 the next day (+9h). One instant orders by the local time.
        let t = 1_700_000_000_000_000_000;
        let found = of(&zoned(&[
            None,
            Some((t - 1, "Asia/Tokyo")),
            Some((t, "America/New_York")),
            Some((t, "UTC")),
            Some((t, "Asia/Tokyo")),
            Some((t + 1, "America/New_York")),
        ]));
        for pair in found.windows(2) {
            assert!(pair[0] < pair[1], "{pair:?}");
        }
        // One instant and one offset in two zones: the zones' ids decide.
        let same_offset = of(&zoned(&[Some((t, "UTC")), Some((t, "Europe/London"))]));
        assert!(same_offset[1] < same_offset[0]);
        assert_eq!(of(&zoned(&[Some((t, "UTC"))]))[0], same_offset[0]);
    }

    #[test]
    fn dates_times_and_durations_order_by_their_counts_with_null_first() {
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![None, Some(-3), Some(19_000)]));
        let times: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![None, Some(0), Some(5)]));
        let durations: ArrayRef =
            Arc::new(DurationNanosecondArray::from(vec![None, Some(-7), Some(0)]));
        let date_times: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            None,
            Some(-1),
            Some(1),
        ]));
        for values in [dates, times, durations, date_times] {
            let found = of(&values);
            assert!(found[0] < found[1] && found[1] < found[2], "{found:?}");
        }
    }

    #[test]
    fn object_stamps_join_only_a_column_of_their_own_class() {
        use ColumnType::*;
        let objects = [
            String,
            LocalDate,
            LocalTime,
            LocalDateTime,
            Duration,
            DayOfWeek,
            ZonedDateTime,
        ];
        for left in &objects {
            for right in &objects {
                assert_eq!(
                    same_stamp_class(left, right),
                    left == right,
                    "{left:?} {right:?}"
                );
            }
        }
        // A DayOfWeek is held as a byte here but is an object to Java.
        assert!(!same_stamp_class(&DayOfWeek, &Byte));
        assert!(!same_stamp_class(&ZonedDateTime, &Instant));
        assert!(same_stamp_class(&Instant, &Long));
        assert!(same_stamp_class(&Boolean, &Byte));
        // As exact-match columns every object class is read alike.
        assert_eq!(primitive(&LocalDate), primitive(&String));
        assert_ne!(primitive(&DayOfWeek), primitive(&Byte));
    }

    #[test]
    fn periods_and_zones_are_not_comparable() {
        let period = incomparable(&ColumnType::Period).unwrap();
        assert!(period
            .starts_with("class java.time.Period cannot be cast to class java.lang.Comparable"));
        let zone = incomparable(&ColumnType::ZoneId).unwrap();
        assert!(zone.starts_with("class java.time.ZoneRegion cannot be cast"));
        for comparable in [
            ColumnType::LocalDate,
            ColumnType::ZonedDateTime,
            ColumnType::DayOfWeek,
        ] {
            assert_eq!(incomparable(&comparable), None);
        }
    }

    #[test]
    fn doubles_order_as_java_orders_them() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        let found = of(&(Arc::new(Float64Array::from(vec![
            None,
            Some(f64::NEG_INFINITY),
            Some(-1.5),
            Some(-0.0),
            Some(0.0),
            Some(2.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
            Some(other_nan),
        ])) as ArrayRef));
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
        let floats = of(&(Arc::new(Float32Array::from(vec![
            None,
            Some(-0.0f32),
            Some(0.1),
            Some(f32::NAN),
        ])) as ArrayRef));
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
        let found = of(&(Arc::new(StringArray::from(vec![
            Some("\u{ff5e}"),
            Some("\u{1f600}"),
            Some(""),
            None,
        ])) as ArrayRef));
        assert!(found[1] < found[0]);
        assert!(found[2] < found[1]);
        assert!(found[3] < found[2]);
    }

    #[test]
    fn a_boolean_is_its_byte_and_an_instant_its_nanoseconds() {
        let booleans =
            of(&(Arc::new(BooleanArray::from(vec![None, Some(false), Some(true)])) as ArrayRef));
        let bytes = of(&(Arc::new(Int8Array::from(vec![None, Some(0), Some(1)])) as ArrayRef));
        assert_eq!(booleans, bytes);
        let instants = of(&(Arc::new(
            TimestampNanosecondArray::from(vec![None, Some(-7)]).with_timezone("UTC"),
        ) as ArrayRef));
        let longs = of(&(Arc::new(Int64Array::from(vec![None, Some(-7)])) as ArrayRef));
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

    /// A word orders as the stamp of the same value, for every column a word is made of.
    #[test]
    fn words_order_as_stamps() {
        let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                None,
                Some(f64::NEG_INFINITY),
                Some(-1e300),
                Some(-1.5),
                Some(-f64::MIN_POSITIVE),
                Some(-0.0),
                Some(0.0),
                Some(f64::MIN_POSITIVE),
                Some(2.0),
                Some(f64::INFINITY),
                Some(f64::NAN),
                Some(other_nan),
                Some(-f64::NAN),
            ])),
            Arc::new(Float32Array::from(vec![
                None,
                Some(-0.0f32),
                Some(0.0),
                Some(-3.5),
                Some(f32::NAN),
                Some(f32::MAX),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MAX),
                None,
                Some(i64::MIN),
                Some(0),
                Some(-1),
            ])),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(UInt16Array::from(vec![Some(0xFFFF), Some(0), None])),
            Arc::new(Int8Array::from(vec![Some(-128), Some(127), None])),
            Arc::new(Date32Array::from(vec![Some(-3), None, Some(7)])),
        ];
        for values in &columns {
            let stamps = of(values);
            let words = <Word as StampValue>::of(values).unwrap();
            for i in 0..stamps.len() {
                for j in 0..stamps.len() {
                    assert_eq!(
                        words[i].cmp(&words[j]),
                        stamps[i].cmp(&stamps[j]),
                        "{} rows {i} and {j}",
                        values.data_type()
                    );
                }
            }
        }
    }

    #[test]
    fn integers_and_chars_order_by_value_with_null_first() {
        let chars =
            of(&(Arc::new(UInt16Array::from(vec![None, Some(0), Some(0xFFFE)])) as ArrayRef));
        assert!(chars[0] < chars[1] && chars[1] < chars[2]);
        let shorts =
            of(&(Arc::new(Int16Array::from(vec![None, Some(-32767), Some(5)])) as ArrayRef));
        assert!(shorts[0] < shorts[1] && shorts[1] < shorts[2]);
    }
}
