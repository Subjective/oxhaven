//! Literal text to typed values: what a match or range filter compares a column with.

use oxhaven_column::sentinel::{NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_LONG};
use oxhaven_contracts::expr::{Expr, FormulaCompiler, FormulaError, Literal};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::unported;

/// A value a filter compares cells with. Integer types, `char`, `Instant` (as nanoseconds),
/// `LocalDate` (as days), `LocalTime` (as nanoseconds) and `DayOfWeek` (as its number)
/// share `Int`; `float` values are widened to `Float`, which is exact.
#[derive(Clone, PartialEq, Debug)]
pub(super) enum Key {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// A `ZonedDateTime`: its instant in nanoseconds and its zone's id.
    Zoned {
        nanos: i64,
        zone: String,
    },
}

/// The value of `function` (`parseLocalDate`, `parseLocalTime`, `parseZonedDateTime`) of
/// the text `inner`, which the formula compiler computes: Java's convertor calls the same
/// `DateTimeUtils` method. An exception of that method is an error.
fn parse_time_value(
    function: &str,
    inner: &str,
    column_type: &ColumnType,
    compiler: &dyn FormulaCompiler,
) -> Result<Key, String> {
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Date32Type, Time64NanosecondType};
    let bad = || format!("Text '{inner}' could not be parsed by {function}");
    // The text is put in a string literal of a formula; text that would end it, or escape
    // in it, is nothing these methods read.
    if inner.contains(['"', '\\']) || inner.chars().any(char::is_control) {
        return Err(bad());
    }
    let expr = compiler
        .parse(&format!("{function}(\"{inner}\")"))
        .map_err(|e| e.to_string())?;
    let empty = TableDefinition::new(Vec::new()).map_err(|e| format!("{e:?}"))?;
    let formula = compiler.compile(&expr, &empty).map_err(|e| e.to_string())?;
    if formula.result_type() != column_type {
        return Err(bad());
    }
    let table = crate::table::empty_table(1);
    let rows = oxhaven_contracts::table::Table::row_set(&*table);
    let values = formula
        .evaluate(&*table, &rows, false)
        .map_err(|e| e.message)?;
    if values.len() != 1 || values.is_null(0) {
        return Ok(Key::Null);
    }
    Ok(match column_type {
        ColumnType::LocalDate => Key::Int(values.as_primitive::<Date32Type>().value(0).into()),
        ColumnType::LocalTime => Key::Int(values.as_primitive::<Time64NanosecondType>().value(0)),
        _ => {
            let (_, instants, zones) = crate::ops::zoned::parts(&values).map_err(|e| e.message)?;
            Key::Zoned {
                nanos: instants[0],
                zone: zones.value(0).to_string(),
            }
        }
    })
}

/// The Java class of a column type that has no convertor of its own, for the message of
/// the convertor every other class gets.
fn class_name(column_type: &ColumnType) -> Option<&'static str> {
    Some(match column_type {
        ColumnType::Duration => "java.time.Duration",
        ColumnType::Period => "java.time.Period",
        ColumnType::ZoneId => "java.time.ZoneId",
        // the class the column's definition names
        ColumnType::Table(TableClass::QueryTable) => "io.deephaven.engine.table.impl.QueryTable",
        ColumnType::Table(TableClass::Table) => "io.deephaven.engine.table.Table",
        _ => return None,
    })
}

/// The days of the week as `java.time.DayOfWeek#name` gives them, Monday first.
const DAYS: [&str; 7] = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
];

/// Behavior: java.lang.String#trim
pub(super) fn java_trim(text: &str) -> &str {
    text.trim_matches(|c: char| c <= ' ')
}

/// Behavior: java.lang.Character#toUpperCase(char) and #toLowerCase(char) for one UTF-16 unit:
/// the single-character mapping, or the unit itself where the mapping is not one character.
fn simple_case(unit: u16, upper: bool) -> u16 {
    let Some(c) = char::from_u32(u32::from(unit)) else {
        return unit;
    };
    if !upper && c == '\u{130}' {
        return u16::from(b'i');
    }
    let mut mapped = [c; 3];
    let mut count = 0;
    let mut push = |m: char| {
        if count < 3 {
            mapped[count] = m;
        }
        count += 1;
    };
    if upper {
        c.to_uppercase().for_each(&mut push);
    } else {
        c.to_lowercase().for_each(&mut push);
    }
    match (count, u16::try_from(u32::from(mapped[0]))) {
        (1, Ok(unit)) => unit,
        _ => unit,
    }
}

/// Behavior: java.lang.String#equalsIgnoreCase, which compares UTF-16 units: equal, or equal
/// after `Character.toUpperCase`, or equal after `toLowerCase` of that.
pub(super) fn java_equals_ignore_case(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    if a.is_ascii() && b.is_ascii() {
        return a.eq_ignore_ascii_case(b);
    }
    // A supplementary character is two units, which have no case of their own. Whether
    // Java maps such a character as a whole depends on the JDK, so only those without a
    // case mapping are compared here.
    let cased = |c: char| !c.to_uppercase().eq([c]) || !c.to_lowercase().eq([c]);
    if a.chars()
        .chain(b.chars())
        .any(|c| u32::from(c) > 0xFFFF && cased(c))
    {
        unported!(
            "java.lang.String#equalsIgnoreCase",
            "strings with supplementary characters that have a case mapping"
        );
    }
    let unit_equal = |x: u16, y: u16| {
        if x == y {
            return true;
        }
        let (x, y) = (simple_case(x, true), simple_case(y, true));
        x == y || simple_case(x, false) == simple_case(y, false)
    };
    let (a, b): (Vec<u16>, Vec<u16>) = (a.encode_utf16().collect(), b.encode_utf16().collect());
    a.len() == b.len() && a.iter().zip(&b).all(|(&x, &y)| unit_equal(x, y))
}

/// Behavior: java.lang.Long#parseLong(String), and with a narrower range
/// java.lang.Integer#parseInt, java.lang.Short#parseShort and java.lang.Byte#parseByte.
pub(super) fn parse_java_integer(text: &str, min: i64, max: i64) -> Result<i64, String> {
    let bad = || format!("For input string: \"{text}\"");
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() {
        return Err(bad());
    }
    if let Some(c) = digits.chars().find(|c| !c.is_ascii() && c.is_numeric()) {
        unported!("java.lang.Character#digit", "the non-ASCII digit {c:?}");
    }
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let negative = text.starts_with('-');
    let mut value: i128 = 0;
    for b in digits.bytes() {
        value = value * 10 + i128::from(b - b'0');
        if value > i128::from(u64::MAX) {
            return Err(bad());
        }
    }
    let value = if negative { -value } else { value };
    if value < i128::from(min) || value > i128::from(max) {
        return Err(bad());
    }
    Ok(value as i64)
}

/// Checks `text` against the grammar of `java.lang.Double#valueOf(String)` and returns the
/// part Rust's float parser reads to the same value, or `Ok(None)` for `NaN`.
///
/// Behavior: jdk.internal.math.FloatingDecimal#readJavaFormatString
fn java_float_text(text: &str) -> Result<Option<String>, String> {
    let bad = || format!("For input string: \"{text}\"");
    let trimmed = java_trim(text);
    let (sign, body) = match trimmed.strip_prefix(['+', '-']) {
        Some(body) => (&trimmed[..1], body),
        None => ("", trimmed),
    };
    if body == "NaN" {
        return Ok(None);
    }
    if body == "Infinity" {
        return Ok(Some(format!("{sign}inf")));
    }
    if body.starts_with("0x") || body.starts_with("0X") {
        unported!(
            "jdk.internal.math.FloatingDecimal#parseHexString",
            "the hexadecimal floating point literal {text}"
        );
    }
    let bytes = body.as_bytes();
    let digits = |from: usize| {
        bytes[from..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count()
    };
    let whole = digits(0);
    let mut at = whole;
    let mut fraction = 0;
    if bytes.get(at) == Some(&b'.') {
        fraction = digits(at + 1);
        at += 1 + fraction;
    }
    if whole + fraction == 0 {
        return Err(bad());
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        let mut exponent = at + 1;
        if matches!(bytes.get(exponent), Some(b'+' | b'-')) {
            exponent += 1;
        }
        let count = digits(exponent);
        if count == 0 {
            return Err(bad());
        }
        at = exponent + count;
    }
    let number = &body[..at];
    if matches!(bytes.get(at), Some(b'f' | b'F' | b'd' | b'D')) {
        at += 1;
    }
    if at != bytes.len() {
        return Err(bad());
    }
    Ok(Some(format!("{sign}{number}")))
}

/// Behavior: java.lang.Double#parseDouble
pub(super) fn parse_java_double(text: &str) -> Result<f64, String> {
    match java_float_text(text)? {
        None => Ok(f64::NAN),
        Some(number) => number
            .parse()
            .map_err(|_| format!("For input string: \"{text}\"")),
    }
}

/// Behavior: java.lang.Float#parseFloat
pub(super) fn parse_java_float(text: &str) -> Result<f32, String> {
    match java_float_text(text)? {
        None => Ok(f32::NAN),
        Some(number) => number
            .parse()
            .map_err(|_| format!("For input string: \"{text}\"")),
    }
}

/// Days from 1970-01-01 to the given date of the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn fixed_digits(text: &str, count: usize) -> Option<i64> {
    (text.len() == count && text.bytes().all(|b| b.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

/// `yyyy-MM-dd` to days since the epoch; `None` if the text has another shape or names a day
/// that does not exist.
fn parse_date(text: &str) -> Option<i64> {
    let mut parts = text.split('-');
    let (year, month, day) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let (year, month, day) = (
        fixed_digits(year, 4)?,
        fixed_digits(month, 2)?,
        fixed_digits(day, 2)?,
    );
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let length = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    (1..=length)
        .contains(&day)
        .then(|| days_from_civil(year, month, day))
}

/// `HH:mm[:ss[.fraction]]` to nanoseconds of the day.
fn parse_time(text: &str) -> Option<i64> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let mut parts = clock.split(':');
    let hour = fixed_digits(parts.next()?, 2)?;
    let minute = fixed_digits(parts.next()?, 2)?;
    let second = match parts.next() {
        Some(second) => Some(fixed_digits(second, 2)?),
        None => None,
    };
    if parts.next().is_some() || hour > 23 || minute > 59 || second.is_some_and(|s| s > 59) {
        return None;
    }
    let nanos = match fraction {
        None => 0,
        Some(digits) => {
            second?;
            if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            format!("{digits:0<9}").parse::<i64>().ok()?
        }
    };
    Some(((hour * 60 + minute) * 60 + second.unwrap_or(0)) * 1_000_000_000 + nanos)
}

/// Nanoseconds since the epoch of a date time string, or of an integer that counts seconds,
/// milliseconds, microseconds or nanoseconds, whichever its magnitude suggests.
///
/// Only the UTC forms `yyyy-MM-ddTHH:mm[:ss[.f]] UTC`, `yyyy-MM-dd UTC` and
/// `yyyy-MM-ddTHH:mm[:ss[.f]]Z` are read; other zones need the time zone database, which
/// [`parse_instant_with`] reaches through the formula compiler.
///
/// Behavior: io.deephaven.time.DateTimeUtils#parseInstant,
/// io.deephaven.time.DateTimeUtils#epochAutoToEpochNanos,
/// io.deephaven.time.DateTimeUtils#parseZonedDateTime
pub(super) fn parse_instant(text: &str) -> Result<i64, String> {
    match read_instant(text)? {
        Some(nanos) => Ok(nanos),
        None => unported!(
            "io.deephaven.time.DateTimeUtils#parseZonedDateTime",
            "the date time {text}"
        ),
    }
}

/// [`parse_instant`], with any zone the formula compiler reads in a time literal: the
/// text is the content of one, and Java reads both with the one method.
///
/// Behavior: io.deephaven.time.DateTimeUtils#parseInstant, which is
/// `parseZonedDateTime(s).toInstant()` for text that is not a whole number, as is the
/// instant of a time literal (io.deephaven.time.TimeLiteralReplacedExpression#convertExpression).
/// Text the compiler reads as another kind of time literal, a date or a duration, is no
/// instant.
pub(super) fn parse_instant_with(
    text: &str,
    compiler: &dyn FormulaCompiler,
) -> Result<i64, String> {
    if let Some(nanos) = read_instant(text)? {
        return Ok(nanos);
    }
    let bad = || format!("Cannot parse instant: {text}");
    // A quote would end the literal, and the rest would be read as a formula.
    if text.contains('\'') {
        return Err(bad());
    }
    match compiler.parse(&format!("'{text}'")) {
        Ok(Expr::Literal(Literal::Instant(nanos))) => Ok(nanos),
        Ok(_) | Err(FormulaError::Syntax(_) | FormulaError::Type(_)) => Err(bad()),
        Err(FormulaError::Unsupported(_)) => unported!(
            "io.deephaven.time.DateTimeUtils#parseZonedDateTime",
            "the date time {text}"
        ),
    }
}

/// [`parse_instant`] for the forms it reads itself; `None` for text with a date or a
/// time in it that is none of them.
fn read_instant(text: &str) -> Result<Option<i64>, String> {
    let bad = || format!("Cannot parse instant: {text}");
    let digits = text.strip_prefix('-').unwrap_or(text);
    if (1..=19).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit()) {
        let offset = parse_java_integer(text, i64::MIN, i64::MAX).map_err(|_| bad())?;
        // TimeConstants.MICROTIME_THRESHOLD
        const MICROS: u64 = 9_309_341_000_000;
        if offset == NULL_LONG {
            return Ok(Some(offset));
        }
        let size = offset.unsigned_abs();
        return Ok(Some(if size > 1000 * MICROS {
            offset
        } else if size > MICROS {
            offset.wrapping_mul(1000)
        } else if size > MICROS / 1000 {
            offset.wrapping_mul(1_000_000)
        } else {
            offset.wrapping_mul(1_000_000_000)
        }));
    }
    let nanos_of = |date: &str, time: Option<&str>| -> Option<i64> {
        let days = parse_date(date)?;
        let time = match time {
            Some(time) => parse_time(time)?,
            None => 0,
        };
        days.checked_mul(86_400_000_000_000)?.checked_add(time)
    };
    let utc = if let Some(local) = text.strip_suffix(" UTC") {
        match local.split_once('T') {
            Some((date, time)) => nanos_of(date, Some(time)),
            None => nanos_of(local, None),
        }
    } else if let Some(local) = text.strip_suffix('Z') {
        local
            .split_once('T')
            .and_then(|(date, time)| nanos_of(date, Some(time)))
    } else {
        None
    };
    match utc {
        Some(nanos) => Ok(Some(nanos)),
        // Every form Java reads has a date or a time in it.
        None if !text.bytes().any(|b| b.is_ascii_digit()) => Err(bad()),
        None => Ok(None),
    }
}

/// The value of the literal `text` for a column of `column_type`. A value equal to the
/// type's null sentinel is null, as is the word `null`.
///
/// Behavior: io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertorFactory#getConvertor
/// (each convertor's `convertStringLiteral`)
fn convert_literal(column_type: &ColumnType, text: &str) -> Result<Key, String> {
    convert_literal_with(column_type, text, None)
}

/// [`convert_literal`]; with a compiler, an instant may name any zone it reads.
fn convert_literal_with(
    column_type: &ColumnType,
    text: &str,
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<Key, String> {
    let integer = |null_name: &str, min: i64, max: i64| -> Result<Key, String> {
        if text == "null" || text == null_name {
            return Ok(Key::Null);
        }
        let value = parse_java_integer(text, min, max)?;
        Ok(if value == min {
            Key::Null
        } else {
            Key::Int(value)
        })
    };
    let units = || text.encode_utf16().collect::<Vec<u16>>();
    match column_type {
        ColumnType::Byte => integer("NULL_BYTE", i8::MIN.into(), i8::MAX.into()),
        ColumnType::Short => integer("NULL_SHORT", i16::MIN.into(), i16::MAX.into()),
        ColumnType::Int => integer("NULL_INT", i32::MIN.into(), i32::MAX.into()),
        ColumnType::Long => integer("NULL_LONG", i64::MIN, i64::MAX),
        ColumnType::Float => {
            if text == "null" || text == "NULL_FLOAT" {
                return Ok(Key::Null);
            }
            let value = parse_java_float(text)?;
            Ok(if value == NULL_FLOAT {
                Key::Null
            } else {
                Key::Float(value.into())
            })
        }
        ColumnType::Double => {
            if text == "null" || text == "NULL_DOUBLE" {
                return Ok(Key::Null);
            }
            let value = parse_java_double(text)?;
            Ok(if value == NULL_DOUBLE {
                Key::Null
            } else {
                Key::Float(value)
            })
        }
        ColumnType::Boolean => {
            if text == "null" || text == "NULL_BOOLEAN" {
                Ok(Key::Null)
            } else if java_equals_ignore_case(text, "true") {
                Ok(Key::Bool(true))
            } else if java_equals_ignore_case(text, "false") {
                Ok(Key::Bool(false))
            } else {
                Err(format!("String {text} isn't a valid boolean value"))
            }
        }
        ColumnType::Char => {
            if text == "null" || text == "NULL_CHAR" {
                return Ok(Key::Null);
            }
            let units = units();
            let quote = |u: u16| u == u16::from(b'\'') || u == u16::from(b'"');
            let unit = match units.as_slice() {
                [unit] => *unit,
                [open, unit, close] if open == close && quote(*open) => *unit,
                _ => return Err(format!("String {text} is not one character")),
            };
            Ok(if unit == NULL_CHAR {
                Key::Null
            } else {
                Key::Int(unit.into())
            })
        }
        ColumnType::String => {
            if text == "null" {
                return Ok(Key::Null);
            }
            let quote = |c: Option<char>| matches!(c, Some('"' | '\'' | '`'));
            if !quote(text.chars().next()) || !quote(text.chars().last()) || text.len() < 2 {
                return Err(format!("String literal not enclosed in quotes (\"{text}\")"));
            }
            Ok(Key::Str(text[1..text.len() - 1].to_string()))
        }
        ColumnType::Instant => {
            if text == "null" {
                return Ok(Key::Null);
            }
            if text.len() < 2 || !text.starts_with('\'') || !text.ends_with('\'') {
                return Err(format!(
                    "Instant literal not enclosed in single-quotes (\"{text}\")"
                ));
            }
            let inner = &text[1..text.len() - 1];
            let nanos = match compiler {
                Some(compiler) => parse_instant_with(inner, compiler)?,
                None => parse_instant(inner)?,
            };
            Ok(if nanos == NULL_LONG {
                Key::Null
            } else {
                Key::Int(nanos)
            })
        }
        // Behavior: the `LocalDate`, `LocalTime` and `ZonedDateTime` convertors, which
        // read the quoted text with DateTimeUtils#parseLocalDate, #parseLocalTime and
        // #parseZonedDateTime. (Java spells the last one's message "ZoneDateTime".)
        ColumnType::LocalDate | ColumnType::LocalTime | ColumnType::ZonedDateTime => {
            if text == "null" {
                return Ok(Key::Null);
            }
            let (class, function) = match column_type {
                ColumnType::LocalDate => ("LocalDate", "parseLocalDate"),
                ColumnType::LocalTime => ("LocalTime", "parseLocalTime"),
                _ => ("ZoneDateTime", "parseZonedDateTime"),
            };
            if text.len() < 2 || !text.starts_with('\'') || !text.ends_with('\'') {
                return Err(format!(
                    "{class} literal not enclosed in single-quotes (\"{text}\")"
                ));
            }
            let Some(compiler) = compiler else {
                unported!(
                    "io.deephaven.time.DateTimeUtils#parseLocalDate",
                    "a {class} literal without the formula compiler"
                );
            };
            parse_time_value(function, &text[1..text.len() - 1], column_type, compiler)
        }
        // Behavior: the convertor of an `Enum` class, `Enum.valueOf(cls, str)`, which takes
        // the constant's exact name and not `null`.
        ColumnType::DayOfWeek => match DAYS.iter().position(|day| *day == text) {
            Some(index) => Ok(Key::Int(index as i64 + 1)),
            None => Err(format!("No enum constant java.time.DayOfWeek.{text}")),
        },
        // Behavior: the convertor of every class without one of its own: `null` alone.
        other if class_name(other).is_some() => {
            if text == "null" {
                return Ok(Key::Null);
            }
            Err(format!(
                "Can't create {} from String Literal for value auto-conversion",
                class_name(other).unwrap_or_default()
            ))
        }
        // Java has no convertor for a vector or an array: only `null` is a value of one.
        other if other.list_element().is_some() => {
            if text == "null" {
                return Ok(Key::Null);
            }
            Err(format!(
                "Can't create {other:?} from String Literal for value auto-conversion"
            ))
        }
        other => unported!(
            "io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertorFactory#getConvertor",
            "column type {other:?}"
        ),
    }
}

/// The text of a quoted string literal.
///
/// Behavior: the `String` convertor of
/// io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertorFactory#getConvertor
pub(super) fn convert_string_literal(text: &str) -> Result<String, String> {
    match convert_literal(&ColumnType::String, text)? {
        Key::Str(value) => Ok(value),
        other => Err(format!("{text} is not a string literal: {other:?}")),
    }
}

/// The value `text` stands for in a filter on `column`. A name of a column of the table, or
/// of one of its column arrays, is not a value: the caller then falls back to a formula.
///
/// Oxhaven has no query scope, so the lookup of `text` as a query scope variable is absent.
///
/// Behavior: io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertor#convertValue
pub(super) fn convert_value(
    column: &ColumnDefinition,
    definition: &TableDefinition,
    text: &str,
    compiler: &dyn FormulaCompiler,
) -> Result<Key, String> {
    let failed = |what: &str, why: &str| match class_name(&column.column_type) {
        Some(class) => format!(
            "Failed to convert {what} <{text}> for column \"{}\" of type {class}{why}",
            column.name
        ),
        None => format!(
            "Failed to convert {what} <{text}> for column \"{}\" of type {:?}{why}",
            column.name, column.column_type
        ),
    };
    if definition.index_of(text).is_some() {
        return Err(failed("value", "; it is a column name"));
    }
    if text
        .strip_suffix('_')
        .is_some_and(|base| definition.index_of(base).is_some())
    {
        return Err(failed("value", "; it is a column array access name"));
    }
    convert_literal_with(&column.column_type, text, Some(compiler))
        .map_err(|e| failed("literal value", &format!(": {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integers_follow_java_parse_int() {
        let int = |text| parse_java_integer(text, i32::MIN.into(), i32::MAX.into());
        assert_eq!(int("42"), Ok(42));
        assert_eq!(int("+42"), Ok(42));
        assert_eq!(int("-2147483648"), Ok(-2_147_483_648));
        for bad in [
            "",
            "-",
            "+",
            "2147483648",
            "1L",
            "1.0",
            " 1",
            "0x10",
            "1_000",
            "--1",
        ] {
            assert!(int(bad).is_err(), "{bad}");
        }
        assert_eq!(
            parse_java_integer("-9223372036854775808", i64::MIN, i64::MAX),
            Ok(i64::MIN)
        );
        assert!(parse_java_integer("9223372036854775808", i64::MIN, i64::MAX).is_err());
        assert!(parse_java_integer("99999999999999999999999", i64::MIN, i64::MAX).is_err());
    }

    #[test]
    fn doubles_follow_java_parse_double() {
        assert_eq!(parse_java_double("1.5"), Ok(1.5));
        assert_eq!(parse_java_double(" 1.5f "), Ok(1.5));
        assert_eq!(parse_java_double("1.e2D"), Ok(100.0));
        assert_eq!(parse_java_double(".5"), Ok(0.5));
        assert_eq!(parse_java_double("-Infinity"), Ok(f64::NEG_INFINITY));
        assert_eq!(parse_java_double("+Infinity"), Ok(f64::INFINITY));
        assert!(parse_java_double("NaN").unwrap().is_nan());
        assert!(parse_java_double("-0").unwrap().is_sign_negative());
        // Rust's parser takes these; Java's does not.
        for bad in [
            "inf", "nan", "infinity", "NAN", ".", "1e", "", "1 .5", "e5", "1.5ff", "- 1",
        ] {
            assert!(parse_java_double(bad).is_err(), "{bad}");
        }
        assert_eq!(parse_java_float("0.1f"), Ok(0.1f32));
    }

    #[test]
    fn equals_ignore_case_compares_utf16_units_with_single_character_mappings() {
        assert!(java_equals_ignore_case("TRUE", "true"));
        assert!(java_equals_ignore_case("fal\u{17f}e", "false"));
        assert!(java_equals_ignore_case("\u{130}", "i"));
        assert!(java_equals_ignore_case("\u{1f80}", "\u{1f88}"));
        // ß has no single-character upper case, so it is not "SS".
        assert!(!java_equals_ignore_case("stra\u{df}e", "STRASSE"));
        assert!(!java_equals_ignore_case("a", "b"));
    }

    #[test]
    fn instants_are_utc_date_times_or_integers_scaled_by_magnitude() {
        assert_eq!(
            parse_instant("2023-11-14T22:13:20 UTC"),
            Ok(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_instant("2023-11-14T22:13:20.5Z"),
            Ok(1_700_000_000_500_000_000)
        );
        assert_eq!(
            parse_instant("2023-11-14T22:13 UTC"),
            Ok(1_699_999_980_000_000_000)
        );
        assert_eq!(parse_instant("1970-01-02 UTC"), Ok(86_400_000_000_000));
        assert_eq!(parse_instant("1969-12-31T23:59:59 UTC"), Ok(-1_000_000_000));
        assert_eq!(
            parse_instant("2024-02-29T00:00:00 UTC"),
            Ok(1_709_164_800_000_000_000)
        );
        // Seconds, milliseconds, microseconds, nanoseconds.
        assert_eq!(parse_instant("5"), Ok(5_000_000_000));
        assert_eq!(
            parse_instant("1700000000000"),
            Ok(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_instant("1700000000000000"),
            Ok(1_700_000_000_000_000_000)
        );
        assert_eq!(
            parse_instant("1700000000000000000"),
            Ok(1_700_000_000_000_000_000)
        );
        assert_eq!(parse_instant("-9223372036854775808"), Ok(i64::MIN));
        assert!(parse_instant("9223372036854775808").is_err());
    }

    #[test]
    #[should_panic(expected = "unported: io.deephaven.time.DateTimeUtils#parseZonedDateTime")]
    fn other_time_zones_are_unported_without_a_compiler() {
        let _ = parse_instant("2023-11-14T22:13:20 ET");
    }

    /// Expected values and refusals are those of Java 42.9 for `where T >= '...'`
    /// (tasks/4.20-fuzz-findings-2.md, D4, R2 and R3).
    #[test]
    fn a_compiler_reads_the_zones_java_reads_and_refuses_the_rest() {
        let with = |text| parse_instant_with(text, &oxhaven_expr::Compiler);
        let base = 1_700_000_000_000_000_000_i64;
        assert_eq!(with("2023-11-14T17:13:21 ET"), Ok(base + 1_000_000_000));
        assert_eq!(
            with("2023-11-14T23:13:21 Europe/Paris"),
            Ok(base + 1_000_000_000)
        );
        assert_eq!(with("2023-11-14T23:13:21 +01:00"), Ok(base + 1_000_000_000));
        assert_eq!(with("2023-11-14 ET"), Ok(base - 62_000_000_000_000));
        assert_eq!(with("2023-11-14T22:13:20 UTC"), Ok(base));
        assert_eq!(with("2023-11-14T23:13:20+01:00"), Ok(base));
        assert_eq!(with("2023-11-14T17:13:20 SystemV/EST5"), Ok(base));
        assert_eq!(with("2023-11-14t22:13:20z"), Ok(base));
        assert_eq!(with("1700000000"), Ok(base));
        for text in [
            "2023-11-14T22:13:21",
            "2023-11-14T22:13:21 NY",
            "2023-11-14T22:13:21 EST",
            "2023-11-14T22:13:21 et",
            "2023-11-14T22:13:21 Europe/Nowhere",
            "2023-11-14",
            "PT1s",
            "12:30",
            "ET",
            "2023-11-14T22:13:21 UTC' + '",
        ] {
            assert_eq!(
                with(text),
                Err(format!("Cannot parse instant: {text}")),
                "{text}"
            );
        }
    }

    #[test]
    #[should_panic(expected = "unported: io.deephaven.time.DateTimeUtils#parseZonedDateTime")]
    fn a_spelling_the_compiler_does_not_read_is_unported() {
        let _ = parse_instant_with("+12023-11-14T22:13:20 UTC", &oxhaven_expr::Compiler);
    }

    #[test]
    fn literals_convert_by_column_type_and_sentinels_are_null() {
        use ColumnType::*;
        assert_eq!(convert_literal(&Int, "7"), Ok(Key::Int(7)));
        assert_eq!(convert_literal(&Int, "null"), Ok(Key::Null));
        assert_eq!(convert_literal(&Int, "NULL_INT"), Ok(Key::Null));
        assert_eq!(convert_literal(&Int, "-2147483648"), Ok(Key::Null));
        assert!(convert_literal(&Int, "NULL_LONG").is_err());
        assert!(convert_literal(&Byte, "128").is_err());
        assert_eq!(convert_literal(&Byte, "-128"), Ok(Key::Null));
        assert_eq!(convert_literal(&Double, "5"), Ok(Key::Float(5.0)));
        assert_eq!(
            convert_literal(&Double, "-1.7976931348623157E308"),
            Ok(Key::Null)
        );
        assert_eq!(convert_literal(&Float, "-3.4028235E38"), Ok(Key::Null));
        assert_eq!(
            convert_literal(&Float, "0.1"),
            Ok(Key::Float(f64::from(0.1f32)))
        );
        assert_eq!(convert_literal(&Boolean, "True"), Ok(Key::Bool(true)));
        assert!(convert_literal(&Boolean, "yes").is_err());
        assert_eq!(convert_literal(&Char, "'x'"), Ok(Key::Int(120)));
        assert_eq!(convert_literal(&Char, "x"), Ok(Key::Int(120)));
        assert!(convert_literal(&Char, "xy").is_err());
        assert!(convert_literal(&Char, "").is_err());
        assert_eq!(
            convert_literal(&String, "`a b`"),
            Ok(Key::Str("a b".into()))
        );
        assert_eq!(convert_literal(&String, "\"a'"), Ok(Key::Str("a".into())));
        assert_eq!(convert_literal(&String, "null"), Ok(Key::Null));
        assert!(convert_literal(&String, "abc").is_err());
        assert!(convert_literal(&String, "\"").is_err());
        assert_eq!(
            convert_literal(&Instant, "'5'"),
            Ok(Key::Int(5_000_000_000))
        );
        assert!(convert_literal(&Instant, "5").is_err());
    }
}
