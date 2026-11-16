//! The column names of a transposed table: the text of a column-by value, and what Java
//! makes of it so that it is a column name.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::unported;

use crate::ops::filter::java_double_to_string;

/// The words `javax.lang.model.SourceVersion#isKeyword` takes for keywords and literals.
const JAVA_KEYWORDS: &[&str] = &[
    "_",
    "abstract",
    "assert",
    "boolean",
    "break",
    "byte",
    "case",
    "catch",
    "char",
    "class",
    "const",
    "continue",
    "default",
    "do",
    "double",
    "else",
    "enum",
    "extends",
    "false",
    "final",
    "finally",
    "float",
    "for",
    "goto",
    "if",
    "implements",
    "import",
    "instanceof",
    "int",
    "interface",
    "long",
    "native",
    "new",
    "null",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "short",
    "static",
    "strictfp",
    "super",
    "switch",
    "synchronized",
    "this",
    "throw",
    "throws",
    "transient",
    "true",
    "try",
    "void",
    "volatile",
    "while",
];

/// The variables the query language keeps for itself.
const QUERY_RESERVED: &[&str] = &["in", "not", "i", "ii", "k"];

/// Behavior: io.deephaven.api.util.NameValidator#isReserved
fn is_reserved(name: &str) -> bool {
    QUERY_RESERVED.contains(&name) || JAVA_KEYWORDS.contains(&name)
}

/// `names` as column names, in order. A reserved word gets the prefix `column_`; every
/// character that is not an ASCII letter, a digit, `_` or `$` is dropped; a name that then
/// starts with a digit gets the prefix too; and a name an earlier one already has gets the
/// first number from 2 on that makes it new. A name with nothing left, or left with a
/// reserved word, is refused with Java's text.
///
/// Behavior: io.deephaven.api.util.NameValidator#legalizeColumnNames(String[], boolean)
/// resolving conflicts, over #legalizeName
pub(super) fn legalize(names: &[String]) -> Result<Vec<String>, String> {
    let mut taken: Vec<String> = Vec::with_capacity(names.len());
    for original in names {
        let error = || format!("Can not legalize column name {original}");
        let mut name = original.clone();
        if is_reserved(&name) {
            name = format!("column_{name}");
        }
        name.retain(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        if name.is_empty() {
            return Err(error());
        }
        if name.starts_with(|c: char| c.is_ascii_digit()) {
            name = format!("column_{name}");
        }
        let mut unique = name.clone();
        let mut next = 2;
        while taken.contains(&unique) {
            unique = format!("{name}{next}");
            next += 1;
        }
        // What is left is ASCII and does not start with a digit, so it is a column name
        // unless dropping characters made it a reserved word.
        if is_reserved(&name) {
            return Err(error());
        }
        taken.push(unique);
    }
    Ok(taken)
}

/// The text of each value of a column-by column, as Java's `String.valueOf` of the boxed
/// value gives it; `null` for a null.
///
/// Not ported: the text of a vector or an array.
///
/// Behavior: the `String.valueOf(s.get(rowKey))` of
/// io.deephaven.engine.table.impl.util.KeyedTranspose#keyedTranspose
pub(super) fn value_texts(column_type: &ColumnType, values: &ArrayRef) -> Vec<String> {
    fn each<T>(values: &ArrayRef, text: impl Fn(usize) -> T) -> Vec<String>
    where
        T: Into<String>,
    {
        (0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    "null".to_string()
                } else {
                    text(row).into()
                }
            })
            .collect()
    }
    match column_type {
        ColumnType::String => {
            let strings = values.as_string::<i32>();
            each(values, |row| strings.value(row))
        }
        ColumnType::Boolean => {
            let flags = values.as_boolean();
            each(values, |row| flags.value(row).to_string())
        }
        ColumnType::Byte => {
            let numbers = values.as_primitive::<Int8Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Short => {
            let numbers = values.as_primitive::<Int16Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Int => {
            let numbers = values.as_primitive::<Int32Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Long => {
            let numbers = values.as_primitive::<Int64Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Char => {
            let units = values.as_primitive::<UInt16Type>();
            // A lone surrogate has no text here; it is no character of a column name
            // either, so what takes its place is dropped like it.
            each(values, |row| String::from_utf16_lossy(&[units.value(row)]))
        }
        ColumnType::Double => {
            let numbers = values.as_primitive::<arrow::datatypes::Float64Type>();
            each(values, |row| java_double_to_string(numbers.value(row)))
        }
        ColumnType::Float => {
            let numbers = values.as_primitive::<arrow::datatypes::Float32Type>();
            each(values, |row| java_float_to_string(numbers.value(row)))
        }
        ColumnType::Instant => {
            let nanos = values.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
            each(values, |row| instant_to_string(nanos.value(row)))
        }
        other => unported!(
            "io.deephaven.engine.table.impl.util.KeyedTranspose#keyedTranspose",
            "a column-by column of type {other:?}, whose values Java names columns after \
             with String#valueOf"
        ),
    }
}

/// Behavior: java.lang.Float#toString(float), as specified since JDK 19: the shortest
/// decimal that reads back as the value, but no fewer than two digits. Where one digit
/// would do, Java takes the two-digit decimal closest to the value, which differs from
/// that digit and a zero only for the smallest subnormal values (`1.4E-45`). Without an
/// exponent from 0.001 up to ten million.
fn java_float_to_string(value: f32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let size = value.abs();
    if size.is_infinite() {
        return format!("{sign}Infinity");
    }
    if size == 0.0 {
        return format!("{sign}0.0");
    }
    // Rust's exponent notation prints the shortest digits that read back as the value.
    let digits_of = |text: String| -> (String, i32) {
        let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
        let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
        let kept = digits.trim_end_matches('0');
        let digits = if kept.is_empty() { "0" } else { kept };
        (digits.to_string(), exponent.parse().unwrap_or(0))
    };
    let (mut digits, mut exponent) = digits_of(format!("{size:e}"));
    if digits.len() == 1 {
        (digits, exponent) = digits_of(format!("{size:.1e}"));
    }
    if !(1e-3..1e7).contains(&size) {
        let rest = if digits.len() == 1 { "0" } else { &digits[1..] };
        return format!("{sign}{}.{rest}E{exponent}", &digits[..1]);
    }
    if exponent < 0 {
        let zeros = "0".repeat((-exponent - 1) as usize);
        return format!("{sign}0.{zeros}{digits}");
    }
    let whole = exponent as usize + 1;
    if digits.len() <= whole {
        let zeros = "0".repeat(whole - digits.len());
        format!("{sign}{digits}{zeros}.0")
    } else {
        format!("{sign}{}.{}", &digits[..whole], &digits[whole..])
    }
}

/// Behavior: `java.time.Instant#toString`, which is `DateTimeFormatter.ISO_INSTANT`: UTC,
/// seconds always present, and the fraction in zero, three, six or nine digits. Every
/// instant a column can hold lies in the years 1677 to 2262, which print in four digits.
fn instant_to_string(nanos: i64) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    // The day counted from 1970-01-01 as a date of the proleptic Gregorian calendar.
    let shifted = seconds.div_euclid(86_400) + 719_468;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Observed: the columns of `keyed_transpose` by a `float` column and by a timestamp
    /// column with these values, on Java 42.9 (the names keep the digits and letters).
    #[test]
    fn floats_and_instants_have_the_text_java_gives_them() {
        use arrow::array::{Float32Array, TimestampNanosecondArray};
        let floats: ArrayRef = std::sync::Arc::new(Float32Array::from(vec![
            Some(0.5),
            None,
            Some(f32::NAN),
            Some(1.0e10),
            Some(1.4e-45),
            Some(f32::MAX),
            Some(-0.0),
            Some(0.001),
            Some(1.0e7),
            Some(9_999_999.0),
            Some(f32::INFINITY),
            Some(f32::MIN_POSITIVE),
            Some(0.1),
            Some(-2.5e-4),
            Some(123.456),
            Some(1200.0),
            Some(0.012),
        ]));
        assert_eq!(
            value_texts(&ColumnType::Float, &floats),
            [
                "0.5",
                "null",
                "NaN",
                "1.0E10",
                "1.4E-45",
                "3.4028235E38",
                "-0.0",
                "0.001",
                "1.0E7",
                "9999999.0",
                "Infinity",
                "1.1754944E-38",
                "0.1",
                "-2.5E-4",
                "123.456",
                "1200.0",
                "0.012"
            ]
        );
        let instants: ArrayRef = std::sync::Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(1_700_000_000_000_000_000),
                Some(1_700_000_000_500_000_000),
                None,
                Some(1_700_000_000_000_001_000),
                Some(1_700_000_000_000_000_001),
                Some(-1_000_000),
                Some(9_223_372_035_000_000_000),
                Some(-9_223_372_035_000_000_000),
                Some(-6_213_559_680_000_000_000),
            ])
            .with_timezone("UTC"),
        );
        assert_eq!(
            value_texts(&ColumnType::Instant, &instants),
            [
                "2023-11-14T22:13:20Z",
                "2023-11-14T22:13:20.500Z",
                "null",
                "2023-11-14T22:13:20.000001Z",
                "2023-11-14T22:13:20.000000001Z",
                "1969-12-31T23:59:59.999Z",
                "2262-04-11T23:47:15Z",
                "1677-09-21T00:12:45Z",
                "1773-02-05T19:12:00Z"
            ]
        );
    }

    fn legal(names: &[&str]) -> Result<Vec<String>, String> {
        legalize(
            &names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
    }

    /// Observed: the columns of `keyed_transpose` by a column with these values.
    #[test]
    fn names_are_made_legal_and_unique_as_java_makes_them() {
        assert_eq!(
            legal(&["1-2.3/4", "12", "a b", "ab", "null", "int", "a-b", "null"]).unwrap(),
            [
                "column_1234",
                "column_12",
                "ab",
                "ab2",
                "column_null",
                "column_int",
                "ab3",
                "column_null2"
            ]
        );
        assert_eq!(
            legal(&["0.0", "1.5", "2024-01-01T00:00:01Z", "true", "A"]).unwrap(),
            [
                "column_00",
                "column_15",
                "column_20240101T000001Z",
                "column_true",
                "A"
            ]
        );
    }

    #[test]
    fn a_name_with_nothing_left_or_left_reserved_is_refused() {
        assert_eq!(legal(&[""]).unwrap_err(), "Can not legalize column name ");
        assert_eq!(legal(&["-"]).unwrap_err(), "Can not legalize column name -");
        assert_eq!(
            legal(&["i-n-t"]).unwrap_err(),
            "Can not legalize column name i-n-t"
        );
    }
}
