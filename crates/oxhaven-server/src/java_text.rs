//! The text Java's `toString` gives for the values of a column, where a client is sent that
//! text in place of the values: the elements of a vector that is itself an element
//! ([`crate::wire_columns`]), and the preview of an array or vector cell
//! (`ApplyPreviewColumns`).

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    Date32Type, DurationNanosecondType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time64NanosecondType, TimestampNanosecondType, UInt16Type,
};
use oxhaven_column::java_values;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::unported;

/// The number of elements `Vector#toString()` prints.
///
/// Behavior: `io.deephaven.vector.IntVector.Indirect#toString` and its siblings, which
/// call `toString(this, 10)`
pub const VECTOR_TEXT_LENGTH: usize = 10;

/// What a vector prints for a null element.
///
/// Behavior: `io.deephaven.vector.Vector#NULL_ELEMENT_STRING`
const NULL_ELEMENT: &str = " ";

/// Lays out the shortest decimal digits of a number as Java does: `digits` are the
/// significant digits and `exponent` is the power of ten of the first one. Plain notation
/// with at least one digit after the point from 10^-3 up to 10^7, otherwise one digit, a
/// point, the remaining digits or `0`, `E` and the exponent.
///
/// Behavior: `java.lang.Double#toString(double)`, `java.lang.Float#toString(float)`
fn java_layout(digits: &str, exponent: i32) -> String {
    if !(-3..7).contains(&exponent) {
        let rest = if digits.len() > 1 { &digits[1..] } else { "0" };
        return format!("{}.{rest}E{exponent}", &digits[..1]);
    }
    if exponent < 0 {
        let zeros = "0".repeat((-exponent - 1) as usize);
        return format!("0.{zeros}{digits}");
    }
    let whole = exponent as usize + 1;
    if digits.len() <= whole {
        let zeros = "0".repeat(whole - digits.len());
        format!("{digits}{zeros}.0")
    } else {
        format!("{}.{}", &digits[..whole], &digits[whole..])
    }
}

/// Splits Rust's exponent notation (`1.5e-7`) into its digits, without trailing zeros, and
/// its exponent.
fn digits_and_exponent(scientific: &str) -> (String, i32) {
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((scientific, "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    (digits.to_string(), exponent.parse().unwrap_or(0))
}

/// The text of a floating point number that is neither NaN nor infinite nor zero, from
/// its magnitude in Rust's shortest exponent notation and in that notation with one
/// decimal: Java prints the shortest decimal that rounds to the value, but no fewer than
/// two digits.
fn java_float_text(negative: bool, shortest: &str, two_digits: &str) -> String {
    let (mut digits, mut exponent) = digits_and_exponent(shortest);
    if digits.len() == 1 {
        (digits, exponent) = digits_and_exponent(two_digits);
    }
    let sign = if negative { "-" } else { "" };
    format!("{sign}{}", java_layout(&digits, exponent))
}

macro_rules! java_to_string {
    ($name:ident, $native:ty) => {
        /// Behavior: `java.lang.Double#toString(double)` and
        /// `java.lang.Float#toString(float)` as specified since JDK 19
        pub fn $name(value: $native) -> String {
            if value.is_nan() {
                return "NaN".to_string();
            }
            let negative = value.is_sign_negative();
            let magnitude = value.abs();
            if magnitude.is_infinite() {
                return if negative { "-Infinity" } else { "Infinity" }.to_string();
            }
            if magnitude == 0.0 {
                return if negative { "-0.0" } else { "0.0" }.to_string();
            }
            java_float_text(
                negative,
                &format!("{magnitude:e}"),
                &format!("{magnitude:.1e}"),
            )
        }
    };
}

java_to_string!(double_to_string, f64);
java_to_string!(float_to_string, f32);

/// The year, month and day of a day counted from 1970-01-01, in the proleptic Gregorian
/// calendar.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Behavior: `java.time.Instant#toString`, which is `DateTimeFormatter.ISO_INSTANT`: UTC,
/// seconds always present, and the fraction in zero, three, six or nine digits. Every
/// instant a column can hold lies in the years 1677 to 2262, which print in four digits.
pub fn instant_to_string(nanos: i64) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let time = seconds.rem_euclid(86_400);
    let (hour, minute, second) = (time / 3_600, time / 60 % 60, time % 60);
    let fraction = if fraction == 0 {
        String::new()
    } else if fraction % 1_000_000 == 0 {
        format!(".{:03}", fraction / 1_000_000)
    } else if fraction % 1_000 == 0 {
        format!(".{:06}", fraction / 1_000)
    } else {
        format!(".{fraction:09}")
    };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}Z")
}

/// The text of each of `values`, which are cells of a column of `column_type` or elements
/// of an array or vector of it: what `toString` gives for the object, `None` for a null.
/// A primitive prints as its box does, a vector as [`vector_text`] of its first
/// [`VECTOR_TEXT_LENGTH`] elements.
///
/// # Panics
/// By `unported!`, for an array: Java prints its class and identity hash (`[I@1b6d3586`).
///
/// Behavior: `java.lang.Object#toString` of `Boolean`, `String`, `java.time.Instant`,
/// the boxes of the primitives, the other `java.time` and `java.math` classes, a table,
/// and `io.deephaven.vector.Vector`
pub fn object_texts(column_type: &ColumnType, values: &ArrayRef) -> Vec<Option<String>> {
    fn each<T>(
        values: impl Iterator<Item = Option<T>>,
        print: impl Fn(T) -> String,
    ) -> Vec<Option<String>> {
        values.map(|value| value.map(&print)).collect()
    }
    let v = values.as_ref();
    match column_type {
        ColumnType::Boolean => each(v.as_boolean().iter(), |value| value.to_string()),
        ColumnType::String => each(v.as_string::<i32>().iter(), str::to_string),
        ColumnType::Instant => each(
            v.as_primitive::<TimestampNanosecondType>().iter(),
            instant_to_string,
        ),
        ColumnType::Byte => each(v.as_primitive::<Int8Type>().iter(), |value| {
            value.to_string()
        }),
        ColumnType::Short => each(v.as_primitive::<Int16Type>().iter(), |value| {
            value.to_string()
        }),
        ColumnType::Int => each(v.as_primitive::<Int32Type>().iter(), |value| {
            value.to_string()
        }),
        ColumnType::Long => each(v.as_primitive::<Int64Type>().iter(), |value| {
            value.to_string()
        }),
        ColumnType::Float => each(v.as_primitive::<Float32Type>().iter(), float_to_string),
        ColumnType::Double => each(v.as_primitive::<Float64Type>().iter(), double_to_string),
        // a Java char is one UTF-16 code unit; half of a surrogate pair has no text of
        // its own in UTF-8
        ColumnType::Char => each(v.as_primitive::<UInt16Type>().iter(), |unit| {
            char::from_u32(unit.into())
                .unwrap_or(char::REPLACEMENT_CHARACTER)
                .to_string()
        }),
        ColumnType::Vector(element) => each(v.as_list::<i32>().iter(), |cell| {
            vector_text(element, &cell, VECTOR_TEXT_LENGTH)
        }),
        ColumnType::BigInteger => each(v.as_binary::<i32>().iter(), java_values::big_integer_text),
        ColumnType::BigDecimal => {
            each(
                v.as_binary::<i32>().iter(),
                |cell| match java_values::big_decimal_parts(cell) {
                    Some((scale, unscaled)) => java_values::big_decimal_text(scale, unscaled),
                    None => unreachable!("a BigDecimal cell holds a scale and a number"),
                },
            )
        }
        ColumnType::LocalDate => each(v.as_primitive::<Date32Type>().iter(), |days| {
            java_values::local_date_text(days.into())
        }),
        ColumnType::LocalTime => each(
            v.as_primitive::<Time64NanosecondType>().iter(),
            java_values::local_time_text,
        ),
        ColumnType::LocalDateTime => each(
            v.as_primitive::<TimestampNanosecondType>().iter(),
            java_values::local_date_time_text,
        ),
        // Behavior: `java.time.DayOfWeek#toString`, the name of the constant
        ColumnType::DayOfWeek => each(v.as_primitive::<Int8Type>().iter(), |day| {
            day_of_week_name(day).to_string()
        }),
        ColumnType::Duration => each(
            v.as_primitive::<DurationNanosecondType>().iter(),
            duration_text,
        ),
        ColumnType::Period => period_texts(v),
        // Behavior: `java.time.ZoneId#toString`, which is `getId`
        ColumnType::ZoneId => each(v.as_string::<i32>().iter(), str::to_string),
        ColumnType::ZonedDateTime => zoned_texts(v),
        ColumnType::Table(_) => (0..v.len())
            .map(|row| v.is_valid(row).then(|| TABLE_TEXT.to_string()))
            .collect(),
        // a column Java declares as `Object` holds values of one class, or nulls only
        ColumnType::Object(Some(class)) => object_texts(class, values),
        ColumnType::Object(None) => vec![None; v.len()],
        ColumnType::Array(_) => unported!(
            "java.lang.Object#toString",
            "the text of an array of type {column_type:?}, which holds its identity hash"
        ),
        other => unported!(
            "java.lang.Object#toString",
            "the text of a value of type {other:?}"
        ),
    }
}

/// The text of a table, which is its description: Java's `toString` of a table is
/// `getDescription`. Observed for the constituents of `partitionBy` and of a transform,
/// in a preview and on the wire; a table of another class (an uncoalesced source table,
/// say) has another text, and none of them can be a cell here yet.
///
/// Behavior: `io.deephaven.engine.table.impl.BaseTable#toString`
pub const TABLE_TEXT: &str = "QueryTable";

/// The text of a duration of `nanos` nanoseconds: `PT`, then the hours, minutes and
/// seconds that are not zero, each with its letter, the seconds with as many digits of
/// fraction as they need; `PT0S` for zero. A negative duration has the sign on each part.
/// Seen on Deephaven 42.9: `PT-0.000000001S`, `PT-1M-0.5S`, `PT25H1M1.000000123S`,
/// `PT1H0.000000001S`.
///
/// Behavior: `java.time.Duration#toString`
fn duration_text(nanos: i64) -> String {
    const NANOS_PER_SECOND: u64 = 1_000_000_000;
    if nanos == 0 {
        return "PT0S".to_string();
    }
    let sign = if nanos < 0 { "-" } else { "" };
    let magnitude = nanos.unsigned_abs();
    let seconds = magnitude / NANOS_PER_SECOND;
    let fraction = magnitude % NANOS_PER_SECOND;
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    let mut text = String::from("PT");
    if hours != 0 {
        text.push_str(&format!("{sign}{hours}H"));
    }
    if minutes != 0 {
        text.push_str(&format!("{sign}{minutes}M"));
    }
    if seconds != 0 || fraction != 0 {
        text.push_str(&format!("{sign}{seconds}"));
        if fraction != 0 {
            let digits = format!("{fraction:09}");
            text.push('.');
            text.push_str(digits.trim_end_matches('0'));
        }
        text.push('S');
    }
    text
}

/// The text of each `Period` of `values`, null where the cell is: `P0D` for the zero
/// period, else `P` and each part that is not zero, years `Y`, months `M`, days `D`, as
/// they are (`P14M`, `P-40D`, seen on Deephaven 42.9).
///
/// Behavior: `java.time.Period#toString`
fn period_texts(values: &dyn Array) -> Vec<Option<String>> {
    let cells = values.as_struct();
    let part = |at: usize| cells.column(at).as_primitive::<Int32Type>().clone();
    let (years, months, days) = (part(0), part(1), part(2));
    (0..cells.len())
        .map(|row| {
            cells.is_valid(row).then(|| {
                let parts = [years.value(row), months.value(row), days.value(row)];
                if parts == [0, 0, 0] {
                    return "P0D".to_string();
                }
                let mut text = String::from("P");
                for (amount, unit) in parts.into_iter().zip(['Y', 'M', 'D']) {
                    if amount != 0 {
                        text.push_str(&format!("{amount}{unit}"));
                    }
                }
                text
            })
        })
        .collect()
}

/// The text of each `ZonedDateTime` of `values`, null where the cell is: its local date
/// and time as `LocalDateTime#toString` prints it, its offset (`Z`, `+05:30`) and, when
/// the zone is not that offset, the zone's id in brackets
/// (`2024-03-10T03:30-04:00[America/New_York]`).
///
/// A cell is the instant and the zone's id; the offset is the zone's at that instant, from
/// the time zone database.
///
/// Behavior: `java.time.ZonedDateTime#toString`, `java.time.ZoneOffset#toString`
fn zoned_texts(values: &dyn Array) -> Vec<Option<String>> {
    let cells = values.as_struct();
    let instants = cells.column(0).as_primitive::<TimestampNanosecondType>();
    let zones = cells.column(1).as_string::<i32>();
    (0..cells.len())
        .map(|row| {
            cells.is_valid(row).then(|| {
                let (nanos, id) = (instants.value(row), zones.value(row));
                let offset = zone_offset_seconds(id, nanos);
                let local = nanos + i64::from(offset) * 1_000_000_000;
                let offset = offset_text(offset);
                let mut text = java_values::local_date_time_text(local);
                text.push_str(&offset);
                if offset != id {
                    text.push_str(&format!("[{id}]"));
                }
                text
            })
        })
        .collect()
}

/// The offset from UTC, in seconds, of the zone `id` at the instant `nanos`. The ids are
/// those a cell can hold: `UTC`, `Z` and the region ids of the IANA database.
///
/// Behavior: `java.time.zone.ZoneRules#getOffset(java.time.Instant)`
fn zone_offset_seconds(id: &str, nanos: i64) -> i32 {
    use chrono::{Offset, TimeZone};
    if matches!(id, "UTC" | "Z") {
        return 0;
    }
    let zone: chrono_tz::Tz = id
        .parse()
        .unwrap_or_else(|_| unreachable!("a ZonedDateTime cell holds a known zone, not {id}"));
    let utc = chrono::DateTime::from_timestamp(nanos.div_euclid(1_000_000_000), 0)
        .unwrap_or_else(|| unreachable!("an instant of a column is within chrono's range"))
        .naive_utc();
    zone.offset_from_utc_datetime(&utc).fix().local_minus_utc()
}

/// The text of a zone offset of `seconds` east of UTC: `Z`, `+hh:mm` or `+hh:mm:ss`.
///
/// Behavior: `java.time.ZoneOffset#toString`
fn offset_text(seconds: i32) -> String {
    if seconds == 0 {
        return "Z".into();
    }
    let sign = if seconds < 0 { '-' } else { '+' };
    let total = seconds.unsigned_abs();
    let (hours, minutes, rest) = (total / 3600, total / 60 % 60, total % 60);
    if rest == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{rest:02}")
    }
}

/// Whether [`object_texts`] knows the text of a value of `column_type`.
pub fn has_object_text(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Boolean
        | ColumnType::String
        | ColumnType::Instant
        | ColumnType::Byte
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::Char
        | ColumnType::BigInteger
        | ColumnType::BigDecimal
        | ColumnType::LocalDate
        | ColumnType::LocalTime
        | ColumnType::LocalDateTime
        | ColumnType::DayOfWeek
        | ColumnType::Duration
        | ColumnType::Period
        | ColumnType::ZoneId
        | ColumnType::ZonedDateTime
        | ColumnType::Table(_)
        | ColumnType::Object(None) => true,
        ColumnType::Vector(element) => has_object_text(element),
        ColumnType::Object(Some(class)) => has_object_text(class),
        _ => false,
    }
}

/// The name of the day whose `getValue` is `day` (Monday 1).
///
/// Behavior: `java.time.DayOfWeek#of`, `java.lang.Enum#name`
pub fn day_of_week_name(day: i8) -> &'static str {
    const NAMES: [&str; 7] = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ];
    usize::try_from(i32::from(day) - 1)
        .ok()
        .and_then(|at| NAMES.get(at))
        .copied()
        .unwrap_or_else(|| unreachable!("a DayOfWeek cell is 1 to 7, not {day}"))
}

/// The text of a vector, or of an array wrapped as one, with these `elements` of type
/// `element`: at most the first `limit` of them between brackets, separated by commas, a
/// null as one space, and `, ...` before the closing bracket if there are more.
///
/// Behavior: `io.deephaven.vector.IntVector#toString(IntVector, int)` and its siblings
/// for the other primitives, `io.deephaven.vector.ObjectVector#toString(ObjectVector, int)`,
/// `io.deephaven.vector.Vector#classToHelper`
pub fn vector_text(element: &ColumnType, elements: &ArrayRef, limit: usize) -> String {
    if elements.is_empty() {
        return "[]".to_string();
    }
    let shown = elements.len().min(limit);
    let texts = object_texts(element, &elements.slice(0, shown));
    let mut text = String::from("[");
    for (index, element) in texts.iter().enumerate() {
        if index > 0 {
            text.push(',');
        }
        text.push_str(element.as_deref().unwrap_or(NULL_ELEMENT));
    }
    text.push_str(if shown == elements.len() {
        "]"
    } else {
        ", ...]"
    });
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int8Array, StringArray,
        TimestampNanosecondArray, UInt16Array,
    };
    use oxhaven_testkit::list_of;
    use std::sync::Arc;

    /// Expected strings are the results of `Double.toString` and `Float.toString` on
    /// JDK 21.
    #[test]
    fn floating_point_numbers_print_as_java_prints_them() {
        for (value, text) in [
            (1.5, "1.5"),
            (100.0, "100.0"),
            (1.0e7, "1.0E7"),
            (9_999_999.0, "9999999.0"),
            (0.001, "0.001"),
            (0.0001, "1.0E-4"),
            (1.0e-5, "1.0E-5"),
            (4.9e-324, "4.9E-324"),
            (2.0e-3, "0.002"),
            (-0.0, "-0.0"),
            (f64::NAN, "NaN"),
            (f64::NEG_INFINITY, "-Infinity"),
            (1.0e23, "1.0E23"),
            (123_456_789.125, "1.23456789125E8"),
        ] {
            assert_eq!(double_to_string(value), text);
        }
        assert_eq!(float_to_string(0.1), "0.1");
        assert_eq!(float_to_string(1.0e10), "1.0E10");
        assert_eq!(float_to_string(f32::INFINITY), "Infinity");
        assert_eq!(float_to_string(3.4028235e38), "3.4028235E38");
    }

    #[test]
    fn an_instant_prints_its_fraction_in_groups_of_three_digits() {
        for (nanos, text) in [
            (0, "1970-01-01T00:00:00Z"),
            (1_700_000_000_000_000_001, "2023-11-14T22:13:20.000000001Z"),
            (1_700_000_000_120_000_000, "2023-11-14T22:13:20.120Z"),
            (1_700_000_000_000_450_000, "2023-11-14T22:13:20.000450Z"),
            (-1, "1969-12-31T23:59:59.999999999Z"),
            (951_782_400_000_000_000, "2000-02-29T00:00:00Z"),
        ] {
            assert_eq!(instant_to_string(nanos), text);
        }
    }

    fn text(element: ColumnType, elements: ArrayRef, limit: usize) -> String {
        vector_text(&element, &elements, limit)
    }

    #[test]
    fn a_vector_prints_its_first_elements_and_a_space_for_a_null() {
        let ints = |values: Vec<Option<i32>>| -> ArrayRef { Arc::new(Int32Array::from(values)) };
        assert_eq!(text(ColumnType::Int, ints(vec![]), 5), "[]");
        assert_eq!(text(ColumnType::Int, ints(vec![None]), 5), "[ ]");
        assert_eq!(
            text(ColumnType::Int, ints(vec![Some(1), None, Some(-3)]), 5),
            "[1, ,-3]"
        );
        let six = ints((1..=6).map(Some).collect());
        assert_eq!(text(ColumnType::Int, six.clone(), 5), "[1,2,3,4,5, ...]");
        assert_eq!(text(ColumnType::Int, six.clone(), 6), "[1,2,3,4,5,6]");
        assert_eq!(text(ColumnType::Int, six, 0), "[, ...]");
    }

    #[test]
    fn elements_of_every_type_print_as_their_java_objects_do() {
        assert_eq!(
            text(
                ColumnType::Boolean,
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                10
            ),
            "[true, ,false]"
        );
        assert_eq!(
            text(
                ColumnType::String,
                Arc::new(StringArray::from(vec![Some(""), Some("a"), None])),
                10
            ),
            "[,a, ]"
        );
        assert_eq!(
            text(
                ColumnType::Char,
                Arc::new(UInt16Array::from(vec![Some(98), None, Some(97)])),
                10
            ),
            "[b, ,a]"
        );
        assert_eq!(
            text(
                ColumnType::Byte,
                Arc::new(Int8Array::from(vec![Some(-7), None])),
                10
            ),
            "[-7, ]"
        );
        assert_eq!(
            text(
                ColumnType::Double,
                Arc::new(Float64Array::from(vec![Some(1.0e7), None, Some(f64::NAN)])),
                10
            ),
            "[1.0E7, ,NaN]"
        );
        assert_eq!(
            text(
                ColumnType::Float,
                Arc::new(Float32Array::from(vec![Some(1.5)])),
                10
            ),
            "[1.5]"
        );
        let instants = TimestampNanosecondArray::from(vec![Some(0), None]).with_timezone("UTC");
        assert_eq!(
            text(ColumnType::Instant, Arc::new(instants), 10),
            "[1970-01-01T00:00:00Z, ]"
        );
    }

    #[test]
    fn a_vector_of_vectors_prints_ten_elements_of_each_inner_vector() {
        let inner = ColumnType::vector(ColumnType::Int);
        let cells = list_of(
            &inner,
            [Some(12), None, Some(0), Some(1)],
            Arc::new(Int32Array::from((0..13).collect::<Vec<_>>())),
        );
        assert_eq!(
            text(inner, cells, 5),
            "[[0,1,2,3,4,5,6,7,8,9, ...], ,[],[12]]"
        );
    }

    #[test]
    #[should_panic(expected = "unported: java.lang.Object#toString")]
    fn the_text_of_an_array_element_is_not_ported() {
        let inner = ColumnType::array(ColumnType::Int);
        let cells = list_of(&inner, [Some(1)], Arc::new(Int32Array::from(vec![1])));
        let _ = text(inner, cells, 5);
    }
}
