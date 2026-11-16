//! The values of partitioning columns that a metadata file gives the types of: the text
//! of a directory name read as a value of its column's type.
//!
//! A directory of a key-value layout without metadata files has its values typed by the
//! CSV reader instead (`arrange` in `lib.rs`).

use crate::{not_ported, ReadError};
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, Time64NanosecondArray, UInt16Array,
};
use oxhaven_column::java_values::{big_decimal_cell, minimal};
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::schema::ColumnType;
use std::sync::Arc;

/// The type of a partitioning column whose type in `_common_metadata` is `column_type`:
/// the same where Java has a parser for the values of the type, and `String` where it
/// has none (`LocalDateTime`, `byte[]`, arrays and vectors).
///
/// Behavior: `io.deephaven.parquet.table.layout.ParquetMetadataFileLayout#adjustPartitionDefinition`,
/// with `io.deephaven.engine.table.impl.locations.util.PartitionParser#lookup`
pub(crate) fn partition_type(column_type: &ColumnType) -> ColumnType {
    match column_type {
        ColumnType::Boolean
        | ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::String
        | ColumnType::Instant
        | ColumnType::BigInteger
        | ColumnType::BigDecimal
        | ColumnType::LocalDate
        | ColumnType::LocalTime => column_type.clone(),
        _ => ColumnType::String,
    }
}

/// The parser Java uses for a type and the class it parses to, for the text of a failure.
fn parser_of(column_type: &ColumnType) -> (&'static str, &'static str) {
    match column_type {
        ColumnType::Boolean => ("ForBoolean", "java.lang.Boolean"),
        ColumnType::Char => ("ForChar", "java.lang.Character"),
        ColumnType::Byte => ("ForByte", "java.lang.Byte"),
        ColumnType::Short => ("ForShort", "java.lang.Short"),
        ColumnType::Int => ("ForInt", "java.lang.Integer"),
        ColumnType::Long => ("ForLong", "java.lang.Long"),
        ColumnType::Float => ("ForFloat", "java.lang.Float"),
        ColumnType::Double => ("ForDouble", "java.lang.Double"),
        ColumnType::BigInteger => ("ForBigInteger", "java.math.BigInteger"),
        ColumnType::BigDecimal => ("ForBigDecimal", "java.math.BigDecimal"),
        ColumnType::Instant => ("ForInstant", "java.time.Instant"),
        ColumnType::LocalDate => ("ForLocalDate", "java.time.LocalDate"),
        ColumnType::LocalTime => ("ForLocalTime", "java.time.LocalTime"),
        _ => ("ForString", "java.lang.String"),
    }
}

/// `text` as a value of `column_type`, which [`partition_type`] gave: an array of one
/// element of the column type's Arrow type. Empty text is null, and so is a value equal
/// to the Java null of its type, as it is in Java, where it is the same bits. Text that
/// is not a value of the type fails as in Java; text Java reads and this does not yet is
/// refused as not ported.
///
/// Behavior: `io.deephaven.engine.table.impl.locations.util.PartitionParser#parse`
pub(crate) fn parse(column_type: &ColumnType, text: &str) -> Result<ArrayRef, ReadError> {
    if text.is_empty() {
        return Ok(arrow::array::new_null_array(&column_type.arrow_type(), 1));
    }
    let (parser, class) = parser_of(column_type);
    let failed = || {
        ReadError::Invalid(format!(
            "{parser} partition value parser for type {class} failed for \"{text}\""
        ))
    };
    let value: ArrayRef = match column_type {
        ColumnType::String => Arc::new(StringArray::from(vec![text])),
        // Behavior: `java.lang.Boolean#parseBoolean`
        ColumnType::Boolean => {
            Arc::new(BooleanArray::from(vec![text.eq_ignore_ascii_case("true")]))
        }
        ColumnType::Char => {
            let mut units = text.encode_utf16();
            match (units.next(), units.next()) {
                (Some(unit), None) => Arc::new(UInt16Array::from(vec![unit])),
                _ => return Err(failed()),
            }
        }
        ColumnType::Byte => Arc::new(Int8Array::from(vec![
            integer::<i8>(text)?.ok_or_else(failed)?
        ])),
        ColumnType::Short => Arc::new(Int16Array::from(vec![
            integer::<i16>(text)?.ok_or_else(failed)?
        ])),
        ColumnType::Int => Arc::new(Int32Array::from(vec![
            integer::<i32>(text)?.ok_or_else(failed)?
        ])),
        ColumnType::Long => Arc::new(Int64Array::from(vec![
            integer::<i64>(text)?.ok_or_else(failed)?
        ])),
        ColumnType::Float => {
            let value = floating::<f32>(text, "java.lang.Float#parseFloat")?.ok_or_else(failed)?;
            Arc::new(Float32Array::from(vec![value]))
        }
        ColumnType::Double => {
            let value =
                floating::<f64>(text, "java.lang.Double#parseDouble")?.ok_or_else(failed)?;
            Arc::new(Float64Array::from(vec![value]))
        }
        ColumnType::BigInteger => {
            let (negative, digits) = big_integer(text)?.ok_or_else(failed)?;
            let cell = twos_complement(negative, digits);
            Arc::new(BinaryArray::from(vec![cell.as_slice()]))
        }
        ColumnType::BigDecimal => {
            let (scale, negative, digits) = big_decimal(text)?.ok_or_else(failed)?;
            let cell = big_decimal_cell(scale, &twos_complement(negative, &digits));
            Arc::new(BinaryArray::from(vec![cell.as_slice()]))
        }
        ColumnType::LocalDate => {
            let days = local_date(text).ok_or_else(failed)?;
            let days = i32::try_from(days).map_err(|_| {
                ReadError::Invalid(format!(
                    "the date {text} is outside the dates a LocalDate column can hold here"
                ))
            })?;
            Arc::new(Date32Array::from(vec![days]))
        }
        ColumnType::LocalTime => Arc::new(Time64NanosecondArray::from(vec![
            local_time(text).ok_or_else(failed)?
        ])),
        ColumnType::Instant => {
            return Err(not_ported(
                "io.deephaven.time.DateTimeUtils#parseInstant",
                format_args!("the partition value \"{text}\" of an Instant column"),
            ))
        }
        other => unreachable!("partition_type never gives {other:?}"),
    };
    match column_type {
        ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double => {
            sentinels_to_nulls(&value).map_err(|failure| ReadError::Invalid(failure.message))
        }
        _ => Ok(value),
    }
}

/// Refuses text with characters outside ASCII, which Java reads where they are digits of
/// another script (`java.lang.Character#digit`) and this does not.
fn ascii(text: &str, java: &'static str) -> Result<(), ReadError> {
    if text.is_ascii() {
        return Ok(());
    }
    Err(not_ported(
        java,
        format_args!("the partition value \"{text}\", which has characters outside ASCII"),
    ))
}

/// An optional `+` or `-`, then decimal digits, in range of the type; `None` for text that
/// is not one.
///
/// Behavior: `java.lang.Byte#parseByte`, `Short#parseShort`, `Integer#parseInt`,
/// `Long#parseLong`, whose grammar is that of Rust's `FromStr` for ASCII text
fn integer<T: std::str::FromStr>(text: &str) -> Result<Option<T>, ReadError> {
    ascii(text, "java.lang.Integer#parseInt")?;
    Ok(text.parse().ok())
}

/// A `float` or `double` as Java reads it: after the characters up to a space are taken off
/// both ends, an optional sign, then `NaN`, `Infinity` or a decimal number with an
/// optional exponent and an optional `f`, `F`, `d` or `D` after it. `None` for text that is
/// not one. A hexadecimal number is refused as not ported.
///
/// Behavior: `jdk.internal.math.FloatingDecimal#readJavaFormatString`
fn floating<T: std::str::FromStr>(text: &str, java: &'static str) -> Result<Option<T>, ReadError> {
    ascii(text, java)?;
    // Behavior: `java.lang.String#trim`
    let trimmed = text.trim_matches(|c: char| c <= ' ');
    let (sign, rest) = match trimmed.as_bytes().first() {
        Some(b'+') | Some(b'-') => trimmed.split_at(1),
        _ => ("", trimmed),
    };
    if rest == "NaN" || rest == "Infinity" {
        // Rust's own spellings of these, which it reads in any case and Java does not
        let rust = if rest == "NaN" { "NaN" } else { "inf" };
        return Ok(format!("{sign}{rust}").parse().ok());
    }
    if rest.starts_with("0x") || rest.starts_with("0X") {
        return Err(not_ported(
            java,
            format_args!("the hexadecimal partition value \"{text}\""),
        ));
    }
    let number = rest.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(rest);
    let (significand, exponent) = match number.find(['e', 'E']) {
        Some(at) => (&number[..at], Some(&number[at + 1..])),
        None => (number, None),
    };
    let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    let digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    let exponent_ok = exponent.is_none_or(|exponent| {
        let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        !exponent.is_empty() && digits(exponent)
    });
    if whole.len() + fraction.len() == 0 || !digits(whole) || !digits(fraction) || !exponent_ok {
        return Ok(None);
    }
    // what is left is a decimal number in a form Rust reads, rounded as Java rounds it:
    // to the nearest value of the type
    Ok(format!("{sign}{number}").parse().ok())
}

/// The sign and the decimal digits of a `BigInteger`, without leading zeros; `None` for
/// text that is not one.
///
/// Behavior: `java.math.BigInteger#BigInteger(String)`
fn big_integer(text: &str) -> Result<Option<(bool, &str)>, ReadError> {
    ascii(text, "java.math.BigInteger#BigInteger(String)")?;
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Ok(None);
    }
    Ok(Some((negative, digits.trim_start_matches('0'))))
}

/// The scale, the sign and the decimal digits of the unscaled value of a `BigDecimal`;
/// `None` for text that is not one or whose scale is outside the range of an `int`.
///
/// Behavior: `java.math.BigDecimal#BigDecimal(String)`
fn big_decimal(text: &str) -> Result<Option<(i32, bool, String)>, ReadError> {
    ascii(text, "java.math.BigDecimal#BigDecimal(String)")?;
    let (negative, rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (significand, exponent) = match rest.find(['e', 'E']) {
        Some(at) => (&rest[..at], Some(&rest[at + 1..])),
        None => (rest, None),
    };
    let (whole, fraction) = significand.split_once('.').unwrap_or((significand, ""));
    let digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if whole.len() + fraction.len() == 0 || !digits(whole) || !digits(fraction) {
        return Ok(None);
    }
    let exponent: i64 = match exponent {
        None => 0,
        Some(exponent) => {
            let unsigned = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
            if unsigned.is_empty() || !digits(unsigned) {
                return Ok(None);
            }
            // more digits than this is far outside the range of a scale
            let significant = unsigned.trim_start_matches('0');
            if significant.len() > 12 {
                return Ok(None);
            }
            let Ok(exponent) = exponent.parse() else {
                return Ok(None);
            };
            exponent
        }
    };
    let scale = fraction.len() as i64 - exponent;
    let Ok(scale) = i32::try_from(scale) else {
        return Ok(None);
    };
    let unscaled = format!("{whole}{fraction}");
    let unscaled = unscaled.trim_start_matches('0').to_string();
    Ok(Some((scale, negative, unscaled)))
}

/// The two's complement bytes, in the fewest bytes, of the integer with the sign
/// `negative` and the decimal `digits`.
fn twos_complement(negative: bool, digits: &str) -> Vec<u8> {
    // the magnitude, least significant byte first
    let mut magnitude: Vec<u8> = Vec::new();
    for digit in digits.bytes() {
        let mut carry = u32::from(digit - b'0');
        for byte in magnitude.iter_mut() {
            let value = u32::from(*byte) * 10 + carry;
            *byte = value as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            magnitude.push(carry as u8);
            carry >>= 8;
        }
    }
    // a byte for the sign, then most significant first
    magnitude.push(0);
    magnitude.reverse();
    if negative {
        let mut carry = true;
        for byte in magnitude.iter_mut().rev() {
            let (inverted, overflow) = (!*byte).overflowing_add(u8::from(carry));
            *byte = inverted;
            carry = overflow;
        }
    }
    minimal(&magnitude).to_vec()
}

/// Up to `most` ASCII digits at the start of `text`, at least `least`: their value and the
/// rest of the text.
fn digits_at(text: &str, least: usize, most: usize) -> Option<(i64, &str)> {
    let count = text
        .bytes()
        .take(most)
        .take_while(u8::is_ascii_digit)
        .count();
    if count < least {
        return None;
    }
    Some((text[..count].parse().ok()?, &text[count..]))
}

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// The days since 1970-01-01 of a date of the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The days since 1970-01-01 of a date written `year-month-day`: a year of four digits, or
/// of five to ten after a `+`, or of four to ten after a `-`; a month and a day of one or
/// two digits. A day past the end of its month is the last day of the month, as the
/// smart resolver makes it. `None` for text that is not a date.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalDate`, with its formatter
/// `FORMATTER_ISO_LOCAL_DATE` (`SignStyle.EXCEEDS_PAD`, `ResolverStyle.SMART`)
fn local_date(text: &str) -> Option<i64> {
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'+') => (Some('+'), &text[1..]),
        Some(b'-') => (Some('-'), &text[1..]),
        _ => (None, text),
    };
    let count = rest.bytes().take(10).take_while(u8::is_ascii_digit).count();
    let width_ok = match sign {
        None => count == 4,
        Some('+') => count > 4,
        Some(_) => count >= 4,
    };
    if !width_ok {
        return None;
    }
    let (year, rest) = digits_at(rest, count, count)?;
    let year = if sign == Some('-') { -year } else { year };
    if !(-999_999_999..=999_999_999).contains(&year) {
        return None;
    }
    let rest = rest.strip_prefix('-')?;
    let (month, rest) = digits_at(rest, 1, 2)?;
    let rest = rest.strip_prefix('-')?;
    let (day, rest) = digits_at(rest, 1, 2)?;
    if !rest.is_empty() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let last = match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    Some(days_from_civil(year, month, day.min(last)))
}

/// The nanoseconds since midnight of a time written `hour:minute[:second[.fraction]]`, each
/// of one or two digits and the fraction of up to nine. `24:00`, with nothing after it but
/// zeros, is midnight, as the smart resolver makes it. `None` for text that is not a time.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalTime`, with its formatter
/// `FORMATTER_ISO_LOCAL_TIME` (`ResolverStyle.SMART`)
fn local_time(text: &str) -> Option<i64> {
    let (hour, rest) = digits_at(text, 1, 2)?;
    let rest = rest.strip_prefix(':')?;
    let (minute, mut rest) = digits_at(rest, 1, 2)?;
    let mut second = 0;
    let mut nanos = 0;
    if let Some(after) = rest.strip_prefix(':') {
        let (value, after) = digits_at(after, 1, 2)?;
        second = value;
        rest = after;
        if let Some(after) = rest.strip_prefix('.') {
            let count = after.bytes().take(9).take_while(u8::is_ascii_digit).count();
            let fraction = &after[..count];
            nanos = format!("{fraction:0<9}").parse().ok()?;
            rest = &after[count..];
        }
    }
    if !rest.is_empty() || minute > 59 || second > 59 {
        return None;
    }
    match hour {
        0..=23 => Some(((hour * 60 + minute) * 60 + second) * 1_000_000_000 + nanos),
        24 if minute == 0 && second == 0 && nanos == 0 => Some(0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Date32Type, Float32Type, Float64Type, Int32Type, Int64Type};
    use oxhaven_column::java_values::{big_decimal_parts, big_decimal_text, big_integer_text};

    /// The value, as text, `None` for a null, or the error.
    fn read(column_type: ColumnType, text: &str) -> Result<Option<String>, ReadError> {
        let array = parse(&column_type, text)?;
        assert_eq!(array.len(), 1);
        assert_eq!(array.data_type(), &column_type.arrow_type());
        if array.is_null(0) {
            return Ok(None);
        }
        Ok(Some(match column_type {
            ColumnType::String => array.as_string::<i32>().value(0).to_string(),
            ColumnType::Boolean => array.as_boolean().value(0).to_string(),
            ColumnType::Char => {
                let unit = array
                    .as_primitive::<arrow::datatypes::UInt16Type>()
                    .value(0);
                String::from_utf16_lossy(&[unit])
            }
            ColumnType::Byte => array
                .as_primitive::<arrow::datatypes::Int8Type>()
                .value(0)
                .to_string(),
            ColumnType::Short => array
                .as_primitive::<arrow::datatypes::Int16Type>()
                .value(0)
                .to_string(),
            ColumnType::Int => array.as_primitive::<Int32Type>().value(0).to_string(),
            ColumnType::Long => array.as_primitive::<Int64Type>().value(0).to_string(),
            ColumnType::Float => format!("{:?}", array.as_primitive::<Float32Type>().value(0)),
            ColumnType::Double => format!("{:?}", array.as_primitive::<Float64Type>().value(0)),
            ColumnType::BigInteger => big_integer_text(array.as_binary::<i32>().value(0)),
            ColumnType::BigDecimal => {
                let (scale, unscaled) =
                    big_decimal_parts(array.as_binary::<i32>().value(0)).unwrap();
                big_decimal_text(scale, unscaled)
            }
            ColumnType::LocalDate => oxhaven_column::java_values::local_date_text(i64::from(
                array.as_primitive::<Date32Type>().value(0),
            )),
            ColumnType::LocalTime => oxhaven_column::java_values::local_time_text(
                array
                    .as_primitive::<arrow::datatypes::Time64NanosecondType>()
                    .value(0),
            ),
            other => panic!("{other:?}"),
        }))
    }

    fn value(column_type: ColumnType, text: &str) -> String {
        read(column_type, text)
            .unwrap()
            .unwrap_or_else(|| "null".to_string())
    }

    fn fails(column_type: ColumnType, text: &str) -> String {
        match read(column_type, text) {
            Err(ReadError::Invalid(message)) => message,
            other => panic!("{text}: {other:?}"),
        }
    }

    /// What Java 42.9 read from the directories `P=<text>` of a table whose
    /// `_common_metadata` gives `P` each type (the probe of task 7.1c).
    #[test]
    fn values_are_read_as_java_reads_them() {
        use ColumnType::*;
        assert_eq!(value(Int, "+5"), "5");
        assert_eq!(value(Int, "-0"), "0");
        assert_eq!(value(Int, "007"), "7");
        assert_eq!(value(Long, "+9"), "9");
        assert_eq!(value(Long, "-9223372036854775808"), "null");
        assert_eq!(value(Byte, "127"), "127");
        assert_eq!(value(Byte, "-128"), "null");
        assert_eq!(value(Short, "-32768"), "null");
        assert_eq!(value(Char, "\u{e9}"), "\u{e9}");
        assert_eq!(value(Boolean, "TRUE"), "true");
        for text in ["yes", "False", "1"] {
            assert_eq!(value(Boolean, text), "false");
        }
        let floats = [
            ("1.5", "1.5"),
            ("NaN", "NaN"),
            ("Infinity", "inf"),
            ("-Infinity", "-inf"),
            ("1e3", "1000.0"),
            ("1.5f", "1.5"),
            ("1.5d", "1.5"),
            (" 2.5 ", "2.5"),
            (".5", "0.5"),
            ("5.", "5.0"),
            ("3.4028236e38", "inf"),
            ("1e-50", "0.0"),
        ];
        for (text, expected) in floats {
            assert_eq!(value(Float, text), expected, "{text}");
        }
        let doubles = [
            ("1.5", "1.5"),
            ("1E-3", "0.001"),
            ("-0.0", "-0.0"),
            ("4.9e-325", "0.0"),
            ("1e400", "inf"),
            ("+1", "1.0"),
        ];
        for (text, expected) in doubles {
            assert_eq!(value(Double, text), expected, "{text}");
        }
        let big = "123456789012345678901234567890123456789012";
        assert_eq!(value(BigInteger, big), big);
        assert_eq!(value(BigInteger, &format!("-{big}")), format!("-{big}"));
        assert_eq!(value(BigInteger, "-0"), "0");
        assert_eq!(value(BigInteger, "+7"), "7");
        assert_eq!(value(BigInteger, "007"), "7");
        assert_eq!(value(BigInteger, "-128"), "-128");
        assert_eq!(value(BigInteger, "255"), "255");
        let decimals = [
            ("1.50", "1.50"),
            ("-0.001", "-0.001"),
            ("1E+3", "1E+3"),
            ("1e-2", "0.01"),
            (".5", "0.5"),
            ("5.", "5"),
            ("+.5e1", "5"),
            ("1e2147483648", "1E+2147483648"),
            ("12.3e-4", "0.00123"),
            ("0.0", "0.0"),
            ("2.5", "2.5"),
        ];
        for (text, expected) in decimals {
            assert_eq!(value(BigDecimal, text), expected, "{text}");
        }
        let dates = [
            ("2024-1-2", "2024-01-02"),
            ("2023-02-30", "2023-02-28"),
            ("2024-02-30", "2024-02-29"),
            ("2024-04-31", "2024-04-30"),
            ("-0001-01-01", "-0001-01-01"),
            ("+12024-01-01", "+12024-01-01"),
        ];
        for (text, expected) in dates {
            assert_eq!(value(LocalDate, text), expected, "{text}");
        }
        let times = [
            ("1:2", "01:02"),
            ("01:02:03", "01:02:03"),
            ("1:2:3.5", "01:02:03.500"),
            ("23:59:59.123456789", "23:59:59.123456789"),
            ("24:00", "00:00"),
            ("24:00:00", "00:00"),
            ("12:00:00.", "12:00"),
        ];
        for (text, expected) in times {
            assert_eq!(value(LocalTime, text), expected, "{text}");
        }
        for column_type in [Int, String, LocalDate, BigDecimal, Boolean, Instant] {
            assert_eq!(read(column_type, "").unwrap(), None);
        }
        assert_eq!(value(String, "a b"), "a b");
    }

    #[test]
    fn text_java_does_not_read_fails_as_in_java() {
        use ColumnType::*;
        let failures = [
            (Int, "2147483648", "ForInt", "java.lang.Integer"),
            (Int, " 1", "ForInt", "java.lang.Integer"),
            (Int, "1_0", "ForInt", "java.lang.Integer"),
            (Byte, "128", "ForByte", "java.lang.Byte"),
            (Short, "40000", "ForShort", "java.lang.Short"),
            (Char, "ab", "ForChar", "java.lang.Character"),
            (Char, "\u{1F600}", "ForChar", "java.lang.Character"),
            (Float, "1e", "ForFloat", "java.lang.Float"),
            (Float, "inf", "ForFloat", "java.lang.Float"),
            (Double, "nan", "ForDouble", "java.lang.Double"),
            (BigInteger, "1e3", "ForBigInteger", "java.math.BigInteger"),
            (
                LocalDate,
                "2024-13-01",
                "ForLocalDate",
                "java.time.LocalDate",
            ),
            (
                LocalDate,
                "2024-01-32",
                "ForLocalDate",
                "java.time.LocalDate",
            ),
            (LocalDate, "24-01-01", "ForLocalDate", "java.time.LocalDate"),
            (
                LocalDate,
                "+2024-01-01",
                "ForLocalDate",
                "java.time.LocalDate",
            ),
            (
                LocalDate,
                "12024-01-01",
                "ForLocalDate",
                "java.time.LocalDate",
            ),
            (
                LocalDate,
                "2024-01-01T",
                "ForLocalDate",
                "java.time.LocalDate",
            ),
            (
                LocalTime,
                "23:59:59.1234567891",
                "ForLocalTime",
                "java.time.LocalTime",
            ),
            (LocalTime, "24:00:01", "ForLocalTime", "java.time.LocalTime"),
            (LocalTime, "12:60", "ForLocalTime", "java.time.LocalTime"),
            (
                LocalTime,
                "12:00:00Z",
                "ForLocalTime",
                "java.time.LocalTime",
            ),
        ];
        for (column_type, text, parser, class) in failures {
            assert_eq!(
                fails(column_type, text),
                format!("{parser} partition value parser for type {class} failed for \"{text}\"")
            );
        }
    }

    #[test]
    fn text_this_does_not_read_yet_is_refused_by_name() {
        let refused = |column_type: ColumnType, text: &str| match parse(&column_type, text) {
            Err(ReadError::NotPorted { java, .. }) => java,
            other => panic!("{text}: {other:?}"),
        };
        // Java reads digits of other scripts, and hexadecimal floating point numbers
        assert_eq!(
            refused(ColumnType::Int, "\u{663}"),
            "java.lang.Integer#parseInt"
        );
        assert_eq!(
            refused(ColumnType::Float, "0x1p3"),
            "java.lang.Float#parseFloat"
        );
        assert_eq!(
            refused(ColumnType::Instant, "2024-01-02T03:04:05Z"),
            "io.deephaven.time.DateTimeUtils#parseInstant"
        );
    }

    #[test]
    fn types_without_a_parser_are_strings() {
        use ColumnType::*;
        for column_type in [Int, Boolean, Char, BigDecimal, LocalTime, Instant, Float] {
            assert_eq!(partition_type(&column_type), column_type);
        }
        for column_type in [
            LocalDateTime,
            ColumnType::array(Byte),
            ColumnType::vector(Int),
        ] {
            assert_eq!(partition_type(&column_type), String);
        }
    }
}
