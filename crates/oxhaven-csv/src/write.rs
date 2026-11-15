//! Writing a table as a CSV file.

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use std::io::Write;

use crate::{not_ported, CsvError};

/// What Java ends a line with on this system.
///
/// Behavior: `java.lang.System#lineSeparator`
const LINE_SEPARATOR: &str = if cfg!(windows) { "\r\n" } else { "\n" };

const SEPARATOR: char = ',';

/// The text of a cell or header in the file: in double quotes, with each double quote
/// doubled, when it holds a double quote, a line feed or the separator.
///
/// Behavior: `io.deephaven.csv.CsvTools#separatorCsvEscape`
fn escaped(text: &str) -> String {
    if text.contains(['"', '\n', SEPARATOR]) {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text.to_string()
    }
}

/// Lays out the shortest digits of a number: `digits` are the significant digits and
/// `exponent` is the power of ten of the first.
///
/// Behavior: `java.lang.Double#toString(double)`: from a thousandth up to ten million
/// the number is written out with at least one digit after the point, and otherwise as
/// one digit, the point, the other digits or `0`, `E` and the exponent
fn java_layout(digits: &str, exponent: i32) -> String {
    if !(-3..7).contains(&exponent) {
        let rest = if digits.len() > 1 { &digits[1..] } else { "0" };
        return format!("{}.{rest}E{exponent}", &digits[..1]);
    }
    if exponent < 0 {
        return format!("0.{}{digits}", "0".repeat((-exponent - 1) as usize));
    }
    let whole = exponent as usize + 1;
    if digits.len() > whole {
        format!("{}.{}", &digits[..whole], &digits[whole..])
    } else {
        format!("{digits}{}.0", "0".repeat(whole - digits.len()))
    }
}

/// The significant digits and the decimal exponent of a number in Rust's `1.5e-7` form.
fn digits_of(scientific: &str) -> (String, i32) {
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((scientific, "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    (digits.to_string(), exponent.parse().unwrap_or(0))
}

macro_rules! java_text {
    ($name:ident, $native:ty) => {
        /// Behavior: `java.lang.Double#toString(double)` and
        /// `java.lang.Float#toString(float)` since JDK 19: the shortest decimal that
        /// reads back as the number, but never fewer than two digits
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
            let (mut digits, mut exponent) = digits_of(&format!("{magnitude:e}"));
            if digits.len() == 1 {
                // with two digits Java takes the closest: `4.9E-324`, not `5.0E-324`
                (digits, exponent) = digits_of(&format!("{magnitude:.1e}"));
            }
            format!("{sign}{}", java_layout(&digits, exponent))
        }
    };
}

java_text!(double_text, f64);
java_text!(float_text, f32);

/// The date of a number of days from 1970-01-01, in the proleptic Gregorian calendar.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_from_march = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// An instant as Java writes it for the time zone UTC: the date, `T`, hours and minutes,
/// the seconds unless they and the fraction are zero, the fraction in three, six or nine
/// digits unless it is zero, and `Z`.
///
/// Behavior: `io.deephaven.csv.CsvTools.CsvRowFormatter.InstantColumnFormatter#write`,
/// which is `java.time.LocalDateTime#toString` followed by the offset
fn instant_text(nanos: i64) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let time = seconds.rem_euclid(86_400);
    let (hour, minute, second) = (time / 3_600, time / 60 % 60, time % 60);
    let mut text = format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}");
    if second > 0 || fraction > 0 {
        text.push_str(&format!(":{second:02}"));
    }
    if fraction > 0 {
        text.push_str(&if fraction % 1_000_000 == 0 {
            format!(".{:03}", fraction / 1_000_000)
        } else if fraction % 1_000 == 0 {
            format!(".{:06}", fraction / 1_000)
        } else {
            format!(".{fraction:09}")
        });
    }
    text.push('Z');
    text
}

/// The cells of a column, top to bottom, as they stand in the file. A null is empty.
///
/// Behavior: `io.deephaven.csv.CsvTools.CsvRowFormatter#makeColumnFormatter` and the
/// formatter of each type, with nulls written as nothing
fn cells_of(column_type: &ColumnType, values: &ArrayRef) -> Result<Vec<String>, CsvError> {
    macro_rules! primitive {
        ($arrow:ty, $text:expr) => {{
            let wrong = || CsvError::Invalid(format!("a {column_type:?} column of another type"));
            let values = values.as_primitive_opt::<$arrow>().ok_or_else(wrong)?;
            let text = $text;
            values
                .iter()
                .map(|value| value.map(|value| escaped(&text(value))).unwrap_or_default())
                .collect()
        }};
    }
    let wrong = || CsvError::Invalid(format!("a {column_type:?} column of another type"));
    Ok(match column_type {
        ColumnType::Boolean => {
            let values = values.as_boolean_opt().ok_or_else(wrong)?;
            let text = |value: Option<bool>| value.map(|v| v.to_string()).unwrap_or_default();
            values.iter().map(text).collect()
        }
        ColumnType::Byte => primitive!(Int8Type, |v: i8| v.to_string()),
        ColumnType::Short => primitive!(Int16Type, |v: i16| v.to_string()),
        ColumnType::Int => primitive!(Int32Type, |v: i32| v.to_string()),
        ColumnType::Long => primitive!(Int64Type, |v: i64| v.to_string()),
        ColumnType::Float => primitive!(Float32Type, float_text),
        ColumnType::Double => primitive!(Float64Type, double_text),
        // Behavior: `java.io.FileWriter`, whose encoder writes `?` for half of a
        // surrogate pair
        ColumnType::Char => primitive!(UInt16Type, |v: u16| {
            char::from_u32(u32::from(v)).unwrap_or('?').to_string()
        }),
        ColumnType::Instant => primitive!(TimestampNanosecondType, instant_text),
        ColumnType::String => {
            let values = values.as_string_opt::<i32>().ok_or_else(wrong)?;
            let text = |value: Option<&str>| value.map(escaped).unwrap_or_default();
            values.iter().map(text).collect()
        }
        // a vector, an array, and any type added later
        _ => {
            return Err(not_ported(
                "io.deephaven.csv.CsvTools.CsvRowFormatter.ObjectColumnFormatter#write",
                format_args!("a {column_type:?} column"),
            ))
        }
    })
}

/// Writes the rows of `table` to the file at `path`, which is made or replaced: a line of
/// column names, then a line per row. `columns` names the columns to write, in order;
/// empty writes them all.
///
/// Java writes an instant as the local time and the offset of its time zone, which is
/// that of its machine unless it is configured. `zone` is the id of that zone; for one
/// that is not UTC throughout, a column of instants is refused.
///
/// Behavior: `io.deephaven.csv.CsvTools#writeCsv(Table, boolean, String, String...)`
/// without compression
pub fn write(
    table: &dyn Table,
    path: &str,
    columns: &[String],
    zone: &str,
) -> Result<(), CsvError> {
    let io = |error: std::io::Error| CsvError::Io(format!("{path}: {error}"));
    // Java opens the file before it looks at the columns, so a refused write leaves an
    // empty file behind
    let mut file = std::fs::File::create(path).map_err(io)?;
    let definition = table.definition();
    let all: Vec<String>;
    let names = if columns.is_empty() {
        all = definition
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();
        &all
    } else {
        columns
    };
    let header: Vec<String> = names.iter().map(|name| escaped(name)).collect();
    let mut text = header.join(&SEPARATOR.to_string());

    let rows = table.row_set();
    let mut cells = Vec::with_capacity(names.len());
    for name in names {
        let Some(index) = definition.index_of(name) else {
            // Behavior: `io.deephaven.engine.table.impl.NoSuchColumnException`
            let available: Vec<&str> = definition.columns().iter().map(|c| &*c.name).collect();
            return Err(CsvError::Invalid(format!(
                "Unknown column names [{name}], available column names are [{}]",
                available.join(", ")
            )));
        };
        let column = table.column(index);
        if *column.column_type() == ColumnType::Instant && !crate::zones::is_utc(zone) {
            return Err(not_ported(
                "io.deephaven.time.DateTimeUtils#timeZone",
                format_args!("an Instant written in the time zone {zone}"),
            ));
        }
        let values = column
            .chunk(&rows)
            .map_err(|failure| CsvError::Invalid(failure.message))?;
        cells.push(cells_of(column.column_type(), &values)?);
    }
    // Java starts each row with the line separator and ends the file with one
    if let Some(first) = cells.first() {
        for row in 0..first.len() {
            for (index, column) in cells.iter().enumerate() {
                text.push_str(if index == 0 { LINE_SEPARATOR } else { "," });
                text.push_str(&column[row]);
            }
        }
    }
    text.push_str(LINE_SEPARATOR);
    file.write_all(text.as_bytes()).map_err(io)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the reference writes for these values (42.9).
    #[test]
    fn numbers_and_instants_are_spelled_as_java_spells_them() {
        let doubles = [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (1e7, "1.0E7"),
            (1.0e-4, "1.0E-4"),
            (0.001, "0.001"),
            (123_456_789.125, "1.23456789125E8"),
            (5e-324, "4.9E-324"),
            (1.5, "1.5"),
            (1e-5, "1.0E-5"),
            (9_999_999.0, "9999999.0"),
        ];
        for (value, text) in doubles {
            assert_eq!(double_text(value), text);
        }
        assert_eq!(float_text(123_456_789.125), "1.2345679E8");
        assert_eq!(float_text(1e10), "1.0E10");
        assert_eq!(float_text(1.5), "1.5");

        let instants = [
            (0, "1970-01-01T00:00Z"),
            (1_000_000, "1970-01-01T00:00:00.001Z"),
            (1_704_164_640_000_000_000, "2024-01-02T03:04Z"),
            (-999_999_500, "1969-12-31T23:59:59.000000500Z"),
            (1_704_164_645_123_456_000, "2024-01-02T03:04:05.123456Z"),
            (1_704_164_645_000_000_000, "2024-01-02T03:04:05Z"),
        ];
        for (nanos, text) in instants {
            assert_eq!(instant_text(nanos), text);
        }
    }

    #[test]
    fn a_cell_with_a_quote_a_line_feed_or_a_comma_is_quoted() {
        assert_eq!(escaped("plain"), "plain");
        assert_eq!(escaped("q\"uo,te"), "\"q\"\"uo,te\"");
        assert_eq!(escaped("a\nb"), "\"a\nb\"");
        // not a carriage return, a tab or a space
        assert_eq!(escaped("a\rb"), "a\rb");
        assert_eq!(escaped(" sp\t"), " sp\t");
        assert_eq!(escaped(""), "");
    }
}
