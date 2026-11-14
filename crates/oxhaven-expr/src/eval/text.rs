//! Strings: Java's string conversion of every type, concatenation, and the `String`
//! methods formulas may call.
//!
//! A Java `String` is a sequence of UTF-16 code units and an Arrow string is UTF-8, so
//! lengths and indexes are counted in UTF-16 code units here, and an operation whose Java
//! result would hold half of a surrogate pair fails, because no Arrow string can hold it.

use super::failure;
use crate::parse::instant::civil_from_days;
use crate::types::StringMethod;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array, StringArray, UInt16Array};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

fn null_pointer() -> TableFailure {
    failure("java.lang.NullPointerException: method call on a null String")
}

fn unpaired_surrogate() -> TableFailure {
    failure("the result holds an unpaired UTF-16 surrogate, which a UTF-8 string cannot")
}

/// Lays out the shortest decimal digits of a number as `Double.toString` does: `digits` are
/// the significant digits and `exponent` is the power of ten of the first one.
///
/// Behavior: `java.lang.Double#toString(double)` and `java.lang.Float#toString(float)`:
/// plain notation with at least one digit after the point from 10^-3 up to 10^7, and
/// otherwise one digit, a point, the remaining digits or `0`, `E` and the exponent.
fn java_layout(digits: &str, exponent: i32) -> String {
    if (-3..7).contains(&exponent) {
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
    } else {
        let rest = if digits.len() > 1 { &digits[1..] } else { "0" };
        format!("{}.{rest}E{exponent}", &digits[..1])
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

macro_rules! java_to_string {
    ($name:ident, $native:ty) => {
        /// Behavior: `java.lang.Double#toString(double)` and `java.lang.Float#toString(float)`
        /// as specified since JDK 19: the shortest decimal that rounds to the value, and
        /// among those the closest to it, but with no fewer than two digits, so the smallest
        /// double is `4.9E-324` although `5E-324` also rounds to it.
        fn $name(value: $native) -> String {
            if value.is_nan() {
                return "NaN".to_string();
            }
            let sign = if value.is_sign_negative() { "-" } else { "" };
            let magnitude = value.abs();
            if magnitude.is_infinite() {
                return format!("{sign}Infinity");
            }
            if magnitude == 0.0 {
                return format!("{sign}0.0");
            }
            // Rust's `{:e}` prints the shortest digits that round to the value, closest to it.
            let (mut digits, mut exponent) = digits_and_exponent(&format!("{magnitude:e}"));
            if digits.len() == 1 {
                // The closest two-digit decimal, which the value correctly rounded to two
                // digits is.
                (digits, exponent) = digits_and_exponent(&format!("{magnitude:.1e}"));
            }
            format!("{sign}{}", java_layout(&digits, exponent))
        }
    };
}

java_to_string!(double_to_string, f64);
java_to_string!(float_to_string, f32);

/// Behavior: `java.time.Instant#toString`, which is `DateTimeFormatter.ISO_INSTANT`: UTC,
/// seconds always present, and the fraction in zero, three, six or nine digits.
pub(crate) fn instant_to_string(nanos: i64) -> String {
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

/// Each value of `array` as Java's string conversion gives it.
///
/// Behavior: the Java Language Specification section 5.1.11, applied to the value as the
/// generated code holds it. A primitive is never null there: a null `int` is the value
/// `NULL_INT` and prints as `-2147483648`, a null `double` prints as
/// `-1.7976931348623157E308`, and a null `char` is the character U+FFFF. A `Boolean`,
/// `String` or `Instant` is a reference, and a null reference prints as `null`.
fn java_strings(array: &dyn Array) -> Result<Vec<String>, TableFailure> {
    fn primitive<P: arrow::datatypes::ArrowPrimitiveType>(
        array: &dyn Array,
        null: P::Native,
        print: impl Fn(P::Native) -> String,
    ) -> Vec<String> {
        array
            .as_primitive::<P>()
            .iter()
            .map(|value| print(value.unwrap_or(null)))
            .collect()
    }
    fn reference<T>(
        values: impl Iterator<Item = Option<T>>,
        print: impl Fn(T) -> String,
    ) -> Vec<String> {
        values
            .map(|value| value.map_or_else(|| "null".to_string(), &print))
            .collect()
    }
    Ok(match array.data_type() {
        DataType::Boolean => reference(array.as_boolean().iter(), |v| v.to_string()),
        DataType::Utf8 => reference(array.as_string::<i32>().iter(), str::to_string),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => reference(
            array.as_primitive::<TimestampNanosecondType>().iter(),
            instant_to_string,
        ),
        DataType::Int8 => primitive::<Int8Type>(array, i8::MIN, |v| v.to_string()),
        DataType::Int16 => primitive::<Int16Type>(array, i16::MIN, |v| v.to_string()),
        DataType::Int32 => primitive::<Int32Type>(array, i32::MIN, |v| v.to_string()),
        DataType::Int64 => primitive::<Int64Type>(array, i64::MIN, |v| v.to_string()),
        DataType::Float32 => primitive::<Float32Type>(array, -f32::MAX, float_to_string),
        DataType::Float64 => primitive::<Float64Type>(array, -f64::MAX, double_to_string),
        DataType::UInt16 => array
            .as_primitive::<UInt16Type>()
            .iter()
            .map(|unit| {
                char::from_u32(unit.unwrap_or(u16::MAX).into())
                    .map(String::from)
                    .ok_or_else(unpaired_surrogate)
            })
            .collect::<Result<_, _>>()?,
        other => unreachable!("the type checker passed a {other} array to string conversion"),
    })
}

/// `left + right` where at least one side is a `String`. The result is never null.
///
/// Behavior: `QueryLanguageParser#visit(BinaryExpr)` prints such a `+` as Java's own
/// operator, which is string concatenation (the Java Language Specification section
/// 15.18.1).
pub(crate) fn concat(left: &dyn Array, right: &dyn Array) -> Result<StringArray, TableFailure> {
    let left = java_strings(left)?;
    let right = java_strings(right)?;
    Ok(StringArray::from_iter_values(
        left.iter().zip(&right).map(|(l, r)| format!("{l}{r}")),
    ))
}

fn out_of_bounds(what: std::fmt::Arguments) -> TableFailure {
    failure(format!("java.lang.StringIndexOutOfBoundsException: {what}"))
}

/// Behavior: `java.lang.String#substring(int, int)`: UTF-16 code units `begin..end`.
fn substring(text: &str, begin: i32, end: Option<i32>) -> Result<String, TableFailure> {
    let units: Vec<u16> = text.encode_utf16().collect();
    let length = units.len() as i64;
    let begin = i64::from(begin);
    let end = end.map_or(length, i64::from);
    if begin < 0 || begin > end || end > length {
        return Err(out_of_bounds(format_args!(
            "begin {begin}, end {end}, length {length}"
        )));
    }
    String::from_utf16(&units[begin as usize..end as usize]).map_err(|_| unpaired_surrogate())
}

/// Behavior: `java.lang.String#compareTo(String)`: the difference of the first pair of
/// UTF-16 code units that differ, or else the difference of the lengths.
fn compare_to(a: &str, b: &str) -> i32 {
    let mut left = a.encode_utf16();
    let mut right = b.encode_utf16();
    loop {
        match (left.next(), right.next()) {
            (Some(x), Some(y)) if x == y => {}
            (Some(x), Some(y)) => return i32::from(x) - i32::from(y),
            (Some(_), None) => return 1 + left.count() as i32,
            (None, Some(_)) => return -1 - right.count() as i32,
            (None, None) => return 0,
        }
    }
}

/// Behavior: `java.lang.String#trim`: removes leading and trailing characters up to and
/// including U+0020.
fn trim(text: &str) -> &str {
    text.trim_matches(|c: char| c <= ' ')
}

/// Calls `method` on each row of `receiver`. `int` arguments are `Int32` arrays, in which
/// null is Java's `NULL_INT`, and `String` arguments are string arrays.
///
/// Behavior: the methods of `java.lang.String`, called on the value as the generated code
/// holds it: a null receiver throws `NullPointerException`, and so does a null `String`
/// argument except in `equals`, which is false for it. Case conversion is that of a default
/// locale without language-specific rules (not Turkish, Azeri or Lithuanian).
pub(crate) fn string_method(
    method: StringMethod,
    receiver: &dyn Array,
    arguments: &[ArrayRef],
) -> Result<ArrayRef, TableFailure> {
    let receiver = receiver.as_string::<i32>();
    let rows = 0..receiver.len();
    let text = |row: usize| -> Result<&str, TableFailure> {
        if receiver.is_null(row) {
            Err(null_pointer())
        } else {
            Ok(receiver.value(row))
        }
    };
    let optional_string = |index: usize, row: usize| -> Option<&str> {
        let argument = arguments[index].as_string::<i32>();
        argument.is_valid(row).then(|| argument.value(row))
    };
    let string = |row: usize| -> Result<&str, TableFailure> {
        optional_string(0, row)
            .ok_or_else(|| failure("java.lang.NullPointerException: null String argument"))
    };
    let int = |index: usize, row: usize| -> i32 {
        let argument = arguments[index].as_primitive::<Int32Type>();
        if argument.is_valid(row) {
            argument.value(row)
        } else {
            i32::MIN
        }
    };
    let utf16_length = |text: &str| text.encode_utf16().count() as i32;

    macro_rules! each_row {
        ($array:ty, |$row:ident| $value:expr) => {{
            let values = rows
                .map(|$row| $value)
                .collect::<Result<Vec<_>, TableFailure>>()?;
            Arc::new(<$array>::from(values)) as ArrayRef
        }};
    }

    Ok(match method {
        StringMethod::Length => each_row!(Int32Array, |row| Ok(utf16_length(text(row)?))),
        StringMethod::IsEmpty => each_row!(BooleanArray, |row| Ok(text(row)?.is_empty())),
        StringMethod::CharAt => each_row!(UInt16Array, |row| {
            let text = text(row)?;
            let index = int(0, row);
            let unit = usize::try_from(index)
                .ok()
                .and_then(|index| text.encode_utf16().nth(index))
                .ok_or_else(|| {
                    out_of_bounds(format_args!("index {index}, length {}", utf16_length(text)))
                })?;
            // U+FFFF is the value Java stores for a null char.
            Ok((unit != u16::MAX).then_some(unit))
        }),
        StringMethod::Contains => {
            each_row!(BooleanArray, |row| Ok(text(row)?.contains(string(row)?)))
        }
        StringMethod::StartsWith => {
            each_row!(BooleanArray, |row| Ok(text(row)?.starts_with(string(row)?)))
        }
        StringMethod::EndsWith => {
            each_row!(BooleanArray, |row| Ok(text(row)?.ends_with(string(row)?)))
        }
        StringMethod::IndexOf => each_row!(Int32Array, |row| {
            let text = text(row)?;
            Ok(match text.find(string(row)?) {
                Some(at) => utf16_length(&text[..at]),
                None => -1,
            })
        }),
        StringMethod::Substring => each_row!(StringArray, |row| {
            let end = (arguments.len() == 2).then(|| int(1, row));
            substring(text(row)?, int(0, row), end)
        }),
        StringMethod::ToUpperCase => each_row!(StringArray, |row| Ok(text(row)?.to_uppercase())),
        StringMethod::ToLowerCase => each_row!(StringArray, |row| Ok(text(row)?.to_lowercase())),
        StringMethod::Trim => each_row!(StringArray, |row| Ok(trim(text(row)?).to_string())),
        StringMethod::Equals => each_row!(BooleanArray, |row| {
            let text = text(row)?;
            Ok(optional_string(0, row) == Some(text))
        }),
        StringMethod::CompareTo => {
            each_row!(Int32Array, |row| Ok(compare_to(text(row)?, string(row)?)))
        }
        StringMethod::Concat => each_row!(StringArray, |row| Ok(format!(
            "{}{}",
            text(row)?,
            string(row)?
        ))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected strings are the results of `Double.toString` on JDK 19 and later.
    #[test]
    fn doubles_print_as_double_to_string_prints_them() {
        let cases = [
            (1.0, "1.0"),
            (-1.0, "-1.0"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (100.0, "100.0"),
            (1.5, "1.5"),
            (0.1, "0.1"),
            (123456.789, "123456.789"),
            (0.001, "0.001"),
            (0.0012, "0.0012"),
            (1.0e-4, "1.0E-4"),
            (9.999e-4, "9.999E-4"),
            (9999999.0, "9999999.0"),
            (1.0e7, "1.0E7"),
            (1.2345e7, "1.2345E7"),
            (1.0e23, "1.0E23"),
            (2.0e-3, "0.002"),
            (1.0 / 3.0, "0.3333333333333333"),
            (2.0 / 3.0, "0.6666666666666666"),
            (100.0 / 3.0, "33.333333333333336"),
            (f64::MAX, "1.7976931348623157E308"),
            (-f64::MAX, "-1.7976931348623157E308"),
            (f64::MIN_POSITIVE, "2.2250738585072014E-308"),
            // Double.MIN_VALUE and its first multiples.
            (5e-324, "4.9E-324"),
            (1e-323, "9.9E-324"),
            (1.5e-323, "1.5E-323"),
            (2e-323, "2.0E-323"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (4.35, "4.35"),
            (1e21, "1.0E21"),
            (123456789012345680.0, "1.2345678901234568E17"),
        ];
        for (value, expected) in cases {
            assert_eq!(double_to_string(value), expected);
        }
    }

    /// Expected strings are the results of `Float.toString` on JDK 19 and later.
    #[test]
    fn floats_print_as_float_to_string_prints_them() {
        let cases = [
            (1.0_f32, "1.0"),
            (0.1, "0.1"),
            (1.5, "1.5"),
            (16777216.0, "1.6777216E7"),
            (1.0e10, "1.0E10"),
            (3.4028235e38, "3.4028235E38"),
            (-3.4028235e38, "-3.4028235E38"),
            (1.17549435e-38, "1.1754944E-38"),
            // Float.MIN_VALUE and its first multiples.
            (1e-45, "1.4E-45"),
            (3e-45, "2.8E-45"),
            (4e-45, "4.2E-45"),
            (0.001, "0.001"),
            (1.0e-4, "1.0E-4"),
            (f32::NAN, "NaN"),
            (f32::NEG_INFINITY, "-Infinity"),
            (-0.0, "-0.0"),
            (1.0 / 3.0, "0.33333334"),
        ];
        for (value, expected) in cases {
            assert_eq!(float_to_string(value), expected);
        }
    }

    /// Expected strings are the results of `Instant.toString`.
    #[test]
    fn instants_print_in_iso_form_with_whole_groups_of_fraction_digits() {
        let base = 1_700_000_000_000_000_000_i64;
        let cases = [
            (base, "2023-11-14T22:13:20Z"),
            (base + 500_000_000, "2023-11-14T22:13:20.500Z"),
            (base + 1_000, "2023-11-14T22:13:20.000001Z"),
            (base + 123_456_000, "2023-11-14T22:13:20.123456Z"),
            (base + 1, "2023-11-14T22:13:20.000000001Z"),
            (base - 20_000_000_000, "2023-11-14T22:13:00Z"),
            (0, "1970-01-01T00:00:00Z"),
            (-1, "1969-12-31T23:59:59.999999999Z"),
            (i64::MAX, "2262-04-11T23:47:16.854775807Z"),
            (i64::MIN + 1, "1677-09-21T00:12:43.145224193Z"),
            (951_782_400_000_000_000, "2000-02-29T00:00:00Z"),
        ];
        for (nanos, expected) in cases {
            assert_eq!(instant_to_string(nanos), expected);
        }
    }

    /// Expected values are those of `String#compareTo`.
    #[test]
    fn compare_to_returns_the_code_unit_or_length_difference() {
        assert_eq!(compare_to("a", "b"), -1);
        assert_eq!(compare_to("b", "a"), 1);
        assert_eq!(compare_to("a", "d"), -3);
        assert_eq!(compare_to("abc", "abc"), 0);
        assert_eq!(compare_to("abc", "a"), 2);
        assert_eq!(compare_to("a", "abcd"), -3);
        assert_eq!(compare_to("", ""), 0);
        assert_eq!(compare_to("A", "a"), -32);
        // U+1F600 is D83D DE00 in UTF-16, and D83D - FF5E is negative.
        assert_eq!(compare_to("\u{1F600}", "\u{FF5E}"), 0xD83D - 0xFF5E);
    }

    /// Expected values are those of `String#substring`.
    #[test]
    fn substring_counts_utf16_code_units() {
        assert_eq!(substring("hello", 1, Some(3)).unwrap(), "el");
        assert_eq!(substring("hello", 5, None).unwrap(), "");
        assert_eq!(substring("hello", 0, Some(0)).unwrap(), "");
        assert_eq!(substring("a\u{1F600}b", 3, None).unwrap(), "b");
        assert_eq!(substring("a\u{1F600}b", 1, Some(3)).unwrap(), "\u{1F600}");
        for (begin, end) in [
            (-1, None),
            (6, None),
            (2, Some(1)),
            (0, Some(6)),
            (i32::MIN, None),
        ] {
            let error = substring("hello", begin, end).unwrap_err();
            assert!(
                error.message.contains("StringIndexOutOfBoundsException"),
                "{begin} {end:?}: {}",
                error.message
            );
        }
        // The result would be half of a surrogate pair.
        assert!(substring("a\u{1F600}b", 2, None).is_err());
    }

    #[test]
    fn trim_removes_everything_up_to_a_space() {
        assert_eq!(trim("  a b \t\n"), "a b");
        assert_eq!(trim("\u{1}a\u{1f}"), "a");
        // U+00A0 and U+2003 are above U+0020, so Java keeps them.
        assert_eq!(trim("\u{a0}a\u{2003}"), "\u{a0}a\u{2003}");
        assert_eq!(trim("   "), "");
    }
}
