//! From the text of one cell to a value: the spellings Java's reader takes for a boolean,
//! an integer, a floating point number, a character and a date-time.

use crate::cells::trim_spaces_and_tabs;
use crate::zones::{self, Zone};
use crate::{not_ported, CsvError};

/// Reads digits from the start of `bytes`: at most `max`, and at least one. `bytes` must
/// be at least `min` long, which is not the same as `min` digits: `1-` passes for a
/// two-digit field. Gives the number and how many digits it read.
///
/// The arithmetic wraps as Java's does, and so does its overflow check, which misses some
/// numbers of twenty digits: `25000000000000000000` reads as `6553255926290448384`.
///
/// Behavior: `io.deephaven.csv.tokenization.Tokenizer.Mutating#tryParseWholeNumber`
fn whole_number(bytes: &[u8], min: usize, max: usize, negate: bool) -> Option<(i64, usize)> {
    if bytes.len() < min {
        return None;
    }
    let mut result: i64 = 0;
    let mut previous: i64 = 0;
    let mut read = 0;
    for &byte in bytes.iter().take(max) {
        if !byte.is_ascii_digit() {
            break;
        }
        // built from negative numbers, which reach one further than positive ones
        result = result.wrapping_mul(10).wrapping_sub(i64::from(byte - b'0'));
        if result > previous {
            return None;
        }
        previous = result;
        read += 1;
    }
    if read == 0 {
        return None;
    }
    if !negate {
        if result == i64::MIN {
            return None;
        }
        result = -result;
    }
    Some((result, read))
}

/// Behavior: `io.deephaven.csv.tokenization.Tokenizer#tryParseLong`
pub(crate) fn parse_long(cell: &[u8]) -> Option<i64> {
    let text = trim_spaces_and_tabs(cell);
    let (negative, digits) = match text {
        [b'+', rest @ ..] => (false, rest),
        [b'-', rest @ ..] => (true, rest),
        _ => (false, text),
    };
    let (value, read) = whole_number(digits, 1, 999, negative)?;
    (read == digits.len()).then_some(value)
}

/// Behavior: `io.deephaven.csv.tokenization.Tokenizer#tryParseBoolean`
pub(crate) fn parse_boolean(cell: &[u8]) -> Option<bool> {
    let text = trim_spaces_and_tabs(cell);
    if text.eq_ignore_ascii_case(b"true") {
        Some(true)
    } else if text.eq_ignore_ascii_case(b"false") {
        Some(false)
    } else {
        None
    }
}

/// A cell that is one character of the Basic Multilingual Plane, as its UTF-16 unit.
///
/// Behavior: `io.deephaven.csv.tokenization.Tokenizer#tryParseBMPChar`
pub(crate) fn parse_char(cell: &[u8]) -> Option<u16> {
    let text = std::str::from_utf8(cell).ok()?;
    let mut chars = text.chars();
    let first = chars.next()?;
    if chars.next().is_some() {
        return None;
    }
    u16::try_from(u32::from(first)).ok()
}

/// What Java's parser of floating point numbers skips at either end of the text.
fn is_java_whitespace(byte: u8) -> bool {
    byte <= b' '
}

/// Reads a cell as a `double`, with the grammar of a Java floating point literal: an
/// optional sign, then `NaN`, `Infinity`, a decimal number with an optional exponent, or a
/// hexadecimal number with a binary exponent; an optional suffix `d`, `D`, `f` or `F`
/// after a number; and whitespace at either end.
///
/// Behavior: `io.deephaven.csv.tokenization.Tokenizer#tryParseDouble` with the parser the
/// reference server installs, `io.deephaven.csv.tokenization.FastCustomDoubleParser`,
/// which is `ch.randelshofer.fastdoubleparser.JavaDoubleParser#parseDouble`
pub(crate) fn parse_double(cell: &[u8]) -> Result<Option<f64>, CsvError> {
    let mut text = cell;
    while let [first, rest @ ..] = text {
        if !is_java_whitespace(*first) {
            break;
        }
        text = rest;
    }
    while let [rest @ .., last] = text {
        if !is_java_whitespace(*last) {
            break;
        }
        text = rest;
    }
    let (negative, text) = match text {
        [b'-', rest @ ..] => (true, rest),
        [b'+', rest @ ..] => (false, rest),
        _ => (false, text),
    };
    let magnitude = match text {
        b"NaN" => return Ok(Some(f64::NAN)),
        b"Infinity" => Some(f64::INFINITY),
        [b'0', b'x' | b'X', rest @ ..] => hex_magnitude(rest),
        _ => decimal_magnitude(text)?,
    };
    Ok(magnitude.map(|magnitude| if negative { -magnitude } else { magnitude }))
}

fn without_suffix(text: &[u8]) -> &[u8] {
    match text {
        [rest @ .., b'd' | b'D' | b'f' | b'F'] => rest,
        _ => text,
    }
}

/// The digits of an exponent as Java's parser adds them up, and as they read: Java stops
/// adding digits once the number is 1024 or more, so `10240` is 1024 to it.
///
/// Behavior: `ch.randelshofer.fastdoubleparser.AbstractJavaFloatingPointBitsFromCharSequence#parseDecFloatLiteral`
fn exponent_numbers(text: &[u8]) -> Option<(i64, i64)> {
    let (negative, digits) = match text {
        [b'-', rest @ ..] => (true, rest),
        [b'+', rest @ ..] => (false, rest),
        _ => (false, text),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let (mut java, mut exact) = (0i64, 0i64);
    for digit in digits {
        let digit = i64::from(digit - b'0');
        if java < 1024 {
            java = 10 * java + digit;
        }
        // far beyond any exponent that matters
        exact = (exact * 10 + digit).min(1 << 40);
    }
    Some(if negative {
        (-java, -exact)
    } else {
        (java, exact)
    })
}

/// What a significand has reached when Java's parser takes no further digit into it.
const MINIMAL_NINETEEN_DIGIT_INTEGER: u64 = 1_000_000_000_000_000_000;

/// An unsigned decimal number: digits with at most one point and at least one digit, and
/// optionally `e` or `E`, a sign and at least one digit.
///
/// Behavior: `ch.randelshofer.fastdoubleparser.AbstractJavaFloatingPointBitsFromCharSequence#parseDecFloatLiteral`
fn decimal_magnitude(text: &[u8]) -> Result<Option<f64>, CsvError> {
    let text = without_suffix(text);
    let exponent_at = text.iter().position(|byte| matches!(byte, b'e' | b'E'));
    let (significand, exponent) = match exponent_at {
        Some(at) => (&text[..at], Some(&text[at + 1..])),
        None => (text, None),
    };
    let digits = significand.iter().filter(|b| b.is_ascii_digit()).count();
    let points = significand.iter().filter(|b| **b == b'.').count();
    if digits == 0 || points > 1 || digits + points != significand.len() {
        return Ok(None);
    }
    if let Some(exponent) = exponent {
        let Some((java, exact)) = exponent_numbers(exponent) else {
            return Ok(None);
        };
        if java != exact && digits > 19 {
            // Java reads the exponent short, and a number of more than nineteen digits
            // can bring the short exponent back into the range of a double: Java then
            // makes a value from it, by an algorithm this crate does not have
            let point = significand.iter().position(|b| *b == b'.');
            let integer_digits = point.unwrap_or(significand.len()) as i64;
            let mut value: u64 = 0;
            let mut taken = 0i64;
            for digit in significand.iter().filter(|b| b.is_ascii_digit()) {
                if value >= MINIMAL_NINETEEN_DIGIT_INTEGER {
                    break;
                }
                value = 10 * value + u64::from(digit - b'0');
                taken += 1;
            }
            // Behavior: `ch.randelshofer.fastdoubleparser.FastDoubleMath#tryDecFloatToDoubleTruncated`
            if value != 0 && (-325..=308).contains(&(integer_digits - taken + java)) {
                return Err(not_ported(
                    "ch.randelshofer.fastdoubleparser.JavaDoubleParser#parseDouble",
                    "a number of more than nineteen digits with an exponent beyond 1024",
                ));
            }
        }
    }
    // Rust reads this spelling as Java does, correctly rounded
    let text = std::str::from_utf8(text).ok();
    Ok(text.and_then(|text| text.parse().ok()))
}

/// An unsigned hexadecimal number after its `0x`: hexadecimal digits with at most one
/// point and at least one digit, then `p` or `P`, an optional sign and at least one
/// decimal digit.
///
/// Java makes the value from the leading digits, as many as keep the significand below
/// `10^18` and all of them when there are sixteen or fewer, and from the exponent as it
/// adds it up; the digits after those are dropped, not rounded. Only when the power of
/// two that goes with those digits is outside of the exponents of a normal `double` does
/// it read the whole text, rounded to the nearest `double`.
///
/// Behavior: `ch.randelshofer.fastdoubleparser.AbstractJavaFloatingPointBitsFromCharSequence#parseHexFloatLiteral`,
/// `ch.randelshofer.fastdoubleparser.FastDoubleMath#tryHexFloatToDoubleTruncated`
fn hex_magnitude(text: &[u8]) -> Option<f64> {
    let text = without_suffix(text);
    let exponent_at = text.iter().position(|byte| matches!(byte, b'p' | b'P'))?;
    let (significand, exponent) = (&text[..exponent_at], &text[exponent_at + 1..]);
    let (java_exponent, exponent) = exponent_numbers(exponent)?;
    let points = significand.iter().filter(|b| **b == b'.').count();
    let digits: Vec<u64> = significand
        .iter()
        .filter_map(|byte| (*byte as char).to_digit(16).map(u64::from))
        .collect();
    if digits.is_empty() || points > 1 || digits.len() + points != significand.len() {
        return None;
    }
    let integer_digits = significand.iter().position(|b| *b == b'.');
    let integer_digits = integer_digits.unwrap_or(significand.len()) as i64;
    let mut value: u64 = 0;
    let mut taken = 0i64;
    for digit in &digits {
        if digits.len() > 16 && value >= MINIMAL_NINETEEN_DIGIT_INTEGER {
            break;
        }
        value = value << 4 | digit;
        taken += 1;
    }
    let power = (integer_digits - taken) * 4 + java_exponent;
    if (-1022..=1023).contains(&power) {
        // Java converts the significand as a signed number and then adds `2^64` to a
        // negative one, which rounds twice
        let signed = value as i64;
        let unsigned = signed as f64 + if signed < 0 { 2f64.powi(64) } else { 0.0 };
        return Some(unsigned * 2f64.powi(power as i32));
    }
    Some(hex_rounded(significand, exponent))
}

/// The `double` nearest to hexadecimal digits with at most one point, times two to the
/// `exponent`; ties to even.
///
/// Behavior: `java.lang.Double#parseDouble`
fn hex_rounded(significand: &[u8], mut exponent: i64) -> f64 {
    // the value is `mantissa * 2^exponent`, plus less than one unit of `mantissa` when
    // `inexact`
    let mut mantissa: u128 = 0;
    let mut inexact = false;
    let mut seen_point = false;
    for &byte in significand {
        let Some(digit) = (byte as char).to_digit(16).map(u128::from) else {
            seen_point = true;
            continue;
        };
        if mantissa >> 120 == 0 {
            mantissa = mantissa << 4 | digit;
            if seen_point {
                exponent -= 4;
            }
        } else {
            inexact |= digit != 0;
            if !seen_point {
                exponent += 4;
            }
        }
    }
    if mantissa == 0 {
        return 0.0;
    }
    let bits = i64::from(128 - mantissa.leading_zeros());
    if exponent + bits > 1024 {
        return f64::INFINITY;
    }
    if exponent + bits < -1075 {
        // less than half of the smallest double
        return 0.0;
    }
    // the exponent of the lowest bit that a double of this size has
    let lowest = (exponent + bits - 53).max(-1074);
    let (kept, lowest) = if lowest <= exponent {
        // every bit fits; nothing was left out either, which takes more than 120 bits
        (mantissa, exponent)
    } else {
        let shift = (lowest - exponent) as u32;
        let (kept, dropped) = if shift >= 128 {
            (0, mantissa)
        } else {
            (mantissa >> shift, mantissa & ((1u128 << shift) - 1))
        };
        // `shift` is at most 129 here, and then `dropped` is below the half
        let half = 1u128.checked_shl(shift - 1).unwrap_or(u128::MAX);
        let round_up = dropped > half || (dropped == half && (inexact || kept & 1 == 1));
        (kept + u128::from(round_up), lowest)
    };
    // scaling by a power of two is exact here: every intermediate value is a double
    let mut value = kept as f64;
    let mut remaining = lowest;
    while remaining != 0 {
        let step = remaining.clamp(-1000, 1000);
        value *= 2f64.powi(step as i32);
        remaining -= step;
    }
    value
}

/// A date-time cell: an instant, or a time in a zone whose rules this crate does not
/// have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DateTime {
    /// Nanoseconds since the epoch.
    Nanos(i64),
    /// A local time in the named zone of the tz database.
    Region(String),
}

fn eat(text: &mut &[u8], byte: u8) -> bool {
    match text {
        [first, rest @ ..] if *first == byte => {
            *text = rest;
            true
        }
        _ => false,
    }
}

fn number(text: &mut &[u8], min: usize, max: usize) -> Option<i64> {
    let (value, read) = whole_number(text, min, max, false)?;
    *text = &text[read..];
    Some(value)
}

fn date_time_exception(message: String) -> CsvError {
    CsvError::Invalid(format!("java.time.DateTimeException: {message}"))
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// The days from 1970-01-01 to a date of the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

const MONTHS: [&str; 12] = [
    "JANUARY",
    "FEBRUARY",
    "MARCH",
    "APRIL",
    "MAY",
    "JUNE",
    "JULY",
    "AUGUST",
    "SEPTEMBER",
    "OCTOBER",
    "NOVEMBER",
    "DECEMBER",
];

/// Reads a cell as a date-time: `yyyy-MM-ddTHH:mm:ss.fffffffff` followed by a zone.
///
/// - The hyphens and colons are all there or all absent. `T` may be a space.
/// - Minutes, seconds and the fraction are optional; the fraction follows `.` or `,` and
///   has one to nine digits.
/// - The zone is `Z`, an offset (`+hh`, `+hhmm` or `+hh:mm`, or with `-`), or a space and
///   a time zone name ([`zones::lookup`]).
/// - A field may have fewer digits than its width when a character that is not a digit
///   follows it.
///
/// `Ok(None)` is a cell that is not a date-time. A cell that is one in form and names a
/// month, day, hour, minute or second that does not exist is an error, as in Java, where
/// the exception ends the read.
///
/// Behavior: `io.deephaven.csv.tokenization.Tokenizer#tryParseDateTime` with
/// `io.deephaven.csv.DeephavenTimeZoneParser`
pub(crate) fn parse_date_time(cell: &[u8]) -> Result<Option<DateTime>, CsvError> {
    let mut text = cell;
    let Some(year) = number(&mut text, 4, 4) else {
        return Ok(None);
    };
    let punctuated = eat(&mut text, b'-');
    let Some(month) = number(&mut text, 2, 2) else {
        return Ok(None);
    };
    if punctuated && !eat(&mut text, b'-') {
        return Ok(None);
    }
    let Some(day) = number(&mut text, 2, 2) else {
        return Ok(None);
    };
    if !eat(&mut text, b'T') && !eat(&mut text, b' ') {
        return Ok(None);
    }
    let Some(hour) = number(&mut text, 2, 2) else {
        return Ok(None);
    };
    let (mut minute, mut second, mut nanos) = (0, 0, 0);
    // Behavior: `Tokenizer.Mutating#tryParseHHmmssNanos`
    'time: {
        if punctuated && !eat(&mut text, b':') {
            break 'time;
        }
        match number(&mut text, 2, 2) {
            Some(value) => minute = value,
            None if punctuated => return Ok(None),
            None => break 'time,
        }
        if punctuated && !eat(&mut text, b':') {
            break 'time;
        }
        match number(&mut text, 2, 2) {
            Some(value) => second = value,
            None if punctuated => return Ok(None),
            None => break 'time,
        }
        if !eat(&mut text, b'.') && !eat(&mut text, b',') {
            break 'time;
        }
        let before = text.len();
        let Some(fraction) = number(&mut text, 1, 9) else {
            return Ok(None);
        };
        nanos = fraction * 10i64.pow(9 - (before - text.len()) as u32);
    }

    // Behavior: `Tokenizer.Mutating#tryParseIsoTimeZone`
    // the seconds to add to the local time read as UTC
    let adjustment: Result<i64, String> = if eat(&mut text, b'Z') {
        Ok(0)
    } else if let [sign @ (b'+' | b'-'), rest @ ..] = text {
        text = rest;
        let Some(hours) = number(&mut text, 2, 2) else {
            return Ok(None);
        };
        eat(&mut text, b':');
        let mut minutes = 0;
        if !text.is_empty() {
            let Some(value) = number(&mut text, 2, 2) else {
                return Ok(None);
            };
            minutes = value;
        }
        let offset = (hours * 60 + minutes) * 60;
        Ok(if *sign == b'-' { offset } else { -offset })
    } else if let [b' ', name @ ..] = text {
        text = &[];
        match zones::lookup(name) {
            Some(Zone::Fixed(offset)) => Ok(-i64::from(offset)),
            Some(Zone::Region(name)) => Err(name.to_string()),
            None => return Ok(None),
        }
    } else {
        return Ok(None);
    };
    // Behavior: `java.time.ZonedDateTime#of`, the checks of `java.time.LocalDateTime#of`.
    // Java makes the date-time before it looks at what follows the zone, so a field out
    // of range is an error even when the cell would not have been a date-time.
    let check = |field: &str, value: i64, low: i64, high: i64| {
        if (low..=high).contains(&value) {
            Ok(())
        } else {
            Err(date_time_exception(format!(
                "Invalid value for {field} (valid values {low} - {high}): {value}"
            )))
        }
    };
    check("MonthOfYear", month, 1, 12)?;
    check("DayOfMonth", day, 1, 31)?;
    let month_name = MONTHS[month as usize - 1];
    let month_length = match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day > month_length {
        return Err(date_time_exception(if day == 29 {
            format!("Invalid date 'February 29' as '{year}' is not a leap year")
        } else {
            format!("Invalid date '{month_name} {day}'")
        }));
    }
    check("HourOfDay", hour, 0, 23)?;
    check("MinuteOfHour", minute, 0, 59)?;
    check("SecondOfMinute", second, 0, 59)?;

    if !text.is_empty() {
        return Ok(None);
    }
    let adjustment = match adjustment {
        Ok(adjustment) => adjustment,
        Err(region) => return Ok(Some(DateTime::Region(region))),
    };
    let local = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    // Java multiplies and adds without a check, so a year past 2262 wraps
    let value = (local + adjustment)
        .wrapping_mul(1_000_000_000)
        .wrapping_add(nanos);
    Ok(Some(DateTime::Nanos(value)))
}
