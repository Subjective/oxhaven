//! `TimeTable`: a refreshing table that gains a row every period.

use super::prelude::*;
use oxhaven_contracts::unported;
use oxhaven_engine::table::{time_table, TimeTableOptions};
use oxhaven_proto::grpc::time_table_request::{Period, StartTime};
use oxhaven_proto::grpc::TimeTableRequest;

/// Behavior: `io.deephaven.server.table.ops.TimeTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = TimeTableRequest;

    fn result_id(request: &TimeTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    /// Behavior: `TimeTableGrpcImpl#validateRequest`. A period string that does not parse
    /// is an exception out of the request handler there, which the client sees as
    /// `INVALID_ARGUMENT` with an error id.
    fn validate(request: &TimeTableRequest) -> Result<(), Status> {
        let period = period_nanos(request).map_err(internal_status)?;
        if period <= 0 {
            // Java's message, which says `>=` where it means `>`.
            return Err(Status::failed_precondition(format!(
                "periodNanos must be >= 0 (found: {period})"
            )));
        }
        Ok(())
    }

    fn create(
        context: &OpContext,
        request: &TimeTableRequest,
        _sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let options = TimeTableOptions {
            start_nanos: start_nanos(request).map_err(ExportError::Internal)?,
            period_nanos: period_nanos(request).map_err(ExportError::Internal)?,
            blink: request.blink_table,
        };
        Ok(time_table(
            context.graph.clone(),
            context.clock.clone(),
            options,
        )?)
    }
}

/// Behavior: `TimeTableGrpcImpl#adaptPeriod`
fn period_nanos(request: &TimeTableRequest) -> Result<i64, String> {
    match &request.period {
        Some(Period::PeriodString(text)) => {
            parse_duration_nanos(text).map_err(|e| format!("Cannot parse time: {text}: {e}"))
        }
        Some(Period::PeriodNanos(nanos)) => Ok(*nanos),
        None => Ok(0),
    }
}

/// The start time, or `None` to start at the first refresh. A start in nanoseconds that
/// is zero or negative means none.
///
/// Behavior: `TimeTableGrpcImpl#adaptStartTime`
fn start_nanos(request: &TimeTableRequest) -> Result<Option<i64>, String> {
    match &request.start_time {
        Some(StartTime::StartTimeString(text)) => parse_instant_nanos(text)
            .map(Some)
            .map_err(|e| format!("Cannot parse instant: {text}: {e}")),
        Some(StartTime::StartTimeNanos(nanos)) if *nanos > 0 => Ok(Some(*nanos)),
        _ => Ok(None),
    }
}

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// A cursor over the bytes of a string being parsed.
struct Text<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Text<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            bytes: text.as_bytes(),
            at: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn done(&self) -> bool {
        self.at == self.bytes.len()
    }

    /// Consumes the next byte if `accept` takes it.
    fn take(&mut self, accept: impl Fn(u8) -> bool) -> Option<u8> {
        let byte = self.peek().filter(|&b| accept(b))?;
        self.at += 1;
        Some(byte)
    }

    fn take_byte(&mut self, wanted: u8) -> bool {
        self.take(|b| b == wanted).is_some()
    }

    fn take_letter(&mut self, upper: u8) -> bool {
        self.take(|b| b.to_ascii_uppercase() == upper).is_some()
    }

    /// Between `min` and `max` decimal digits, as text.
    fn digits(&mut self, min: usize, max: usize) -> Option<&'a str> {
        let start = self.at;
        while self.at - start < max && self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.at += 1;
        }
        if self.at - start < min {
            self.at = start;
            return None;
        }
        std::str::from_utf8(&self.bytes[start..self.at]).ok()
    }

    /// A number of `min` to `max` digits.
    fn number(&mut self, min: usize, max: usize) -> Option<i64> {
        self.digits(min, max)?.parse().ok()
    }
}

/// Up to nine fraction digits as nanoseconds.
fn fraction_nanos(digits: &str) -> i64 {
    let mut nanos = 0;
    for position in 0..9 {
        let digit = digits.as_bytes().get(position).map_or(0, |b| b - b'0');
        nanos = nanos * 10 + i64::from(digit);
    }
    nanos
}

/// A duration in nanoseconds from `[-]PT[-]hh:mm[:ss][.nnnnnnnnn]` or from the ISO-8601
/// form `[-]PnDTnHnMn.nS`.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseDurationNanos`,
/// `DateTimeUtils#parseDuration`, `java.time.Duration#parse`
pub(crate) fn parse_duration_nanos(text: &str) -> Result<i64, String> {
    let nanos = match parse_clock_duration(text) {
        Some(nanos) => nanos?,
        None => parse_iso_duration(text)?,
    };
    i64::try_from(nanos).map_err(|_| "the duration overflows a long of nanoseconds".to_string())
}

/// `[-]PT[-]hh:mm[:ss][.nnnnnnnnn]`, or `None` if the text is not of that form.
///
/// Behavior: `DateTimeUtils.TIME_DURATION_PATTERN` and its use in `DateTimeUtils#parseDuration`
fn parse_clock_duration(text: &str) -> Option<Result<i128, String>> {
    let mut text = Text::new(text);
    let mut sign = if text.take_byte(b'-') { -1 } else { 1 };
    if !(text.take_letter(b'P') && text.take_letter(b'T')) {
        return None;
    }
    if text.take_byte(b'-') {
        sign = -sign;
    }
    let hours = text.digits(1, usize::MAX)?;
    if !text.take_byte(b':') {
        return None;
    }
    let minutes = text.digits(1, usize::MAX)?;
    let seconds = if text.take_byte(b':') {
        Some(text.digits(1, usize::MAX)?)
    } else {
        None
    };
    let fraction = if text.take_byte(b'.') {
        Some(text.digits(1, 9)?)
    } else {
        None
    };
    if !text.done() {
        return None;
    }
    let field = |digits: &str| {
        digits
            .parse::<i64>()
            .map(i128::from)
            .map_err(|e| e.to_string())
    };
    Some((|| {
        let mut nanos = field(hours)? * 3600 * NANOS_PER_SECOND;
        nanos += field(minutes)? * 60 * NANOS_PER_SECOND;
        if let Some(seconds) = seconds {
            nanos += field(seconds)? * NANOS_PER_SECOND;
        }
        if let Some(fraction) = fraction {
            nanos += i128::from(fraction_nanos(fraction));
        }
        Ok(i128::from(sign) * nanos)
    })())
}

/// `[-+]PnDTnHnMn.nS`, letters in either case, each number optionally signed.
///
/// Behavior: `java.time.Duration#parse`
fn parse_iso_duration(text: &str) -> Result<i128, String> {
    let malformed = || "Text cannot be parsed to a Duration".to_string();
    let mut text = Text::new(text);
    let negate = text.take(|b| b == b'-' || b == b'+') == Some(b'-');
    if !text.take_letter(b'P') {
        return Err(malformed());
    }

    // A signed whole number, with whether it was written with a minus sign.
    let signed = |text: &mut Text| -> Option<Result<(i64, bool), String>> {
        let start = text.at;
        let minus = text.take(|b| b == b'-' || b == b'+') == Some(b'-');
        let Some(digits) = text.digits(1, usize::MAX) else {
            text.at = start;
            return None;
        };
        let value = digits
            .parse::<i64>()
            .map(|value| if minus { -value } else { value })
            .map_err(|e| e.to_string());
        Some(value.map(|value| (value, minus)))
    };

    let mut seconds: i128 = 0;
    let mut nanos: i128 = 0;
    let mut parts = 0;
    if let Some(days) = signed(&mut text) {
        if !text.take_letter(b'D') {
            return Err(malformed());
        }
        seconds += i128::from(days?.0) * 86_400;
        parts += 1;
    }
    if text.take_letter(b'T') {
        let before = parts;
        // Hours, minutes, seconds: each at most once, in this order.
        let mut next_unit = 0;
        while let Some(number) = signed(&mut text) {
            let (value, minus) = number?;
            let fraction = if text.take(|b| b == b'.' || b == b',').is_some() {
                Some(text.digits(0, 9).unwrap_or(""))
            } else {
                None
            };
            let unit = match text.take(|b| b.is_ascii_alphabetic()) {
                Some(letter) => letter.to_ascii_uppercase(),
                None => return Err(malformed()),
            };
            let (order, scale) = match unit {
                b'H' => (1, 3600),
                b'M' => (2, 60),
                b'S' => (3, 1),
                _ => return Err(malformed()),
            };
            if order < next_unit + 1 || (fraction.is_some() && unit != b'S') {
                return Err(malformed());
            }
            next_unit = order;
            seconds += i128::from(value) * scale;
            if let Some(fraction) = fraction {
                let fraction = i128::from(fraction_nanos(fraction));
                nanos += if minus { -fraction } else { fraction };
            }
            parts += 1;
        }
        if parts == before {
            return Err(malformed());
        }
    }
    if parts == 0 || !text.done() {
        return Err(malformed());
    }
    if i64::try_from(seconds).is_err() {
        return Err("the duration overflows a long of seconds".to_string());
    }
    let total = seconds * NANOS_PER_SECOND + nanos;
    Ok(if negate { -total } else { total })
}

/// Days from 1970-01-01 to the given date in the proleptic Gregorian calendar.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn month_length(year: i64, month: i64) -> i64 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 31,
    }
}

/// `yyyy-MM-dd` as days since the epoch. Strict takes two-digit months and days and no
/// day past the end of its month; lenient takes one or two digits and moves such a day to
/// the last day of the month.
///
/// Behavior: `DateTimeFormatter.ISO_LOCAL_DATE` (strict) and
/// `DateTimeUtils.FORMATTER_ISO_LOCAL_DATE` (lenient, resolved with `ResolverStyle.SMART`)
fn parse_date(text: &mut Text, strict: bool) -> Result<i64, String> {
    let bad = || "not a date".to_string();
    let min = if strict { 2 } else { 1 };
    if text.peek().is_some_and(|b| b == b'-' || b == b'+') {
        unported!(
            "io.deephaven.time.DateTimeUtils#parseInstant",
            "a year written with a sign"
        );
    }
    // Both formatters take more than four digits only after a sign.
    let year = text.number(4, 4).ok_or_else(bad)?;
    if !text.take_byte(b'-') {
        return Err(bad());
    }
    let month = text.number(min, 2).ok_or_else(bad)?;
    if !text.take_byte(b'-') {
        return Err(bad());
    }
    let mut day = text.number(min, 2).ok_or_else(bad)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err("month or day out of range".to_string());
    }
    let last = month_length(year, month);
    if day > last {
        if strict {
            return Err("day past the end of the month".to_string());
        }
        day = last;
    }
    Ok(days_from_civil(year, month, day))
}

/// A time of day in nanoseconds. Strict is `HH:mm[:ss[.fraction]]` with two-digit fields;
/// lenient takes one or two digits per field.
///
/// Behavior: `DateTimeFormatter.ISO_LOCAL_TIME` (strict) and
/// `DateTimeUtils.FORMATTER_ISO_LOCAL_TIME` (lenient)
fn parse_time(text: &mut Text, strict: bool) -> Result<i64, String> {
    let bad = || "not a time".to_string();
    let min = if strict { 2 } else { 1 };
    let hour = text.number(min, 2).ok_or_else(bad)?;
    if !text.take_byte(b':') {
        return Err(bad());
    }
    let minute = text.number(min, 2).ok_or_else(bad)?;
    let mut second = 0;
    let mut nanos = 0;
    if text.take_byte(b':') {
        second = text.number(min, 2).ok_or_else(bad)?;
        if text.take_byte(b'.') {
            let digits = text.digits(if strict { 1 } else { 0 }, 9).ok_or_else(bad)?;
            nanos = fraction_nanos(digits);
        }
    }
    if hour == 24 {
        unported!(
            "io.deephaven.time.DateTimeUtils#parseInstant",
            "hour 24, which java.time resolves as the end of the day"
        );
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err("time field out of range".to_string());
    }
    Ok(((hour * 60 + minute) * 60 + second) * 1_000_000_000 + nanos)
}

/// The offset of a named time zone from UTC, in seconds. Only the names of UTC are known;
/// every other zone needs the time zone database and Deephaven's aliases.
///
/// Behavior: `io.deephaven.time.TimeZoneAliases#zoneId`
fn zone_offset_seconds(zone: &str) -> i64 {
    match zone {
        "UTC" | "Z" | "GMT" | "UT" | "Etc/UTC" | "Etc/GMT" => 0,
        _ => unported!(
            "io.deephaven.time.TimeZoneAliases#zoneId",
            "time zone {zone}: only UTC is known"
        ),
    }
}

/// `Z` or `+hh:mm[:ss]` in seconds, at the cursor.
///
/// Behavior: the `+HH:MM:ss` offset pattern of `DateTimeFormatter.ISO_OFFSET_DATE_TIME`
fn parse_offset(text: &mut Text) -> Result<i64, String> {
    let bad = || "not a zone offset".to_string();
    if text.take_letter(b'Z') {
        return Ok(0);
    }
    let sign = match text.take(|b| b == b'-' || b == b'+') {
        Some(b'-') => -1,
        Some(_) => 1,
        None => return Err(bad()),
    };
    let hours = text.number(2, 2).ok_or_else(bad)?;
    if !text.take_byte(b':') {
        return Err(bad());
    }
    let minutes = text.number(2, 2).ok_or_else(bad)?;
    let seconds = if text.take_byte(b':') {
        text.number(2, 2).ok_or_else(bad)?
    } else {
        0
    };
    if hours > 18 || minutes > 59 || seconds > 59 {
        return Err("zone offset out of range".to_string());
    }
    Ok(sign * ((hours * 60 + minutes) * 60 + seconds))
}

fn epoch_nanos(days: i64, nanos_of_day: i64, offset_seconds: i64) -> Result<i64, String> {
    let nanos = (i128::from(days) * 86_400 - i128::from(offset_seconds)) * NANOS_PER_SECOND
        + i128::from(nanos_of_day);
    i64::try_from(nanos).map_err(|_| "the instant overflows a long of nanoseconds".to_string())
}

/// `yyyy-MM-ddTHH:mm[:ss[.f]]` followed by an offset and optionally `[zone]`.
///
/// Behavior: `java.time.ZonedDateTime#parse(CharSequence)`
fn parse_offset_date_time(text: &str) -> Result<i64, String> {
    let mut text = Text::new(text);
    let days = parse_date(&mut text, true)?;
    if !text.take_letter(b'T') {
        return Err("no time".to_string());
    }
    let nanos_of_day = parse_time(&mut text, true)?;
    let offset = parse_offset(&mut text)?;
    if text.take_byte(b'[') {
        let start = text.at;
        while text.take(|b| b != b']').is_some() {}
        let zone = std::str::from_utf8(&text.bytes[start..text.at]).unwrap_or("");
        if !text.take_byte(b']') || zone.is_empty() {
            return Err("unclosed zone".to_string());
        }
        // With a region the offset must be one the region uses at that time.
        if zone_offset_seconds(zone) != offset {
            unported!(
                "java.time.ZonedDateTime#parse",
                "an offset that differs from its region's"
            );
        }
    }
    if !text.done() {
        return Err("text after the date time".to_string());
    }
    epoch_nanos(days, nanos_of_day, offset)
}

/// An instant in nanoseconds since the epoch from a date time with an offset
/// (`2024-01-01T00:00:00Z`), a date time and a zone (`2024-01-01T00:00:00 UTC`) or a date
/// and a zone (`2024-01-01 UTC`). Zones other than UTC are not ported.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseInstant`,
/// `DateTimeUtils#parseZonedDateTime`
pub(crate) fn parse_instant_nanos(text: &str) -> Result<i64, String> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if (1..=19).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit()) {
        let offset: i64 = text.parse().map_err(|e| format!("{e}"))?;
        return epoch_auto_to_nanos(offset);
    }
    if let Ok(nanos) = parse_offset_date_time(text) {
        return Ok(nanos);
    }
    let Some((local, zone)) = text.split_once(' ') else {
        return Err("No time zone provided".to_string());
    };
    let mut local = Text::new(local);
    let days = parse_date(&mut local, false)?;
    // `yyyy-MM-dd[T] zone` is midnight; Java takes it only for a zone of letters, `_`, `/`.
    let date_only = local.done() || (local.bytes.len() - local.at == 1 && local.take_letter(b'T'));
    let zone_is_plain = !zone.is_empty()
        && zone
            .bytes()
            .all(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'/');
    // Behavior: `DateTimeUtils#parseTimeZoneQuiet`, which knows no zone of one character.
    if date_only && zone_is_plain {
        if zone.len() <= 1 {
            return Err(format!("No matching time zone: '{zone}'"));
        }
        return epoch_nanos(days, 0, zone_offset_seconds(zone));
    }
    if date_only || !local.take_letter(b'T') {
        return Err("not a date time".to_string());
    }
    let nanos_of_day = parse_time(&mut local, false)?;
    if !local.done() {
        return Err("text after the time".to_string());
    }
    if zone.len() <= 1 {
        return Err(format!("No matching time zone: {zone}"));
    }
    epoch_nanos(days, nanos_of_day, zone_offset_seconds(zone))
}

/// An offset from the epoch whose unit is told from its size: seconds, milliseconds,
/// microseconds or nanoseconds.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#epochAutoToEpochNanos`
fn epoch_auto_to_nanos(offset: i64) -> Result<i64, String> {
    // `io.deephaven.base.clock.TimeConstants.MICROTIME_THRESHOLD`
    const MICROTIME_THRESHOLD: u64 = 9_309_341_000_000;
    let size = offset.unsigned_abs();
    let scale = if size > 1000 * MICROTIME_THRESHOLD {
        1
    } else if size > MICROTIME_THRESHOLD {
        1000
    } else if size > MICROTIME_THRESHOLD / 1000 {
        1_000_000
    } else {
        1_000_000_000
    };
    // Java multiplies without a check; nothing in these ranges overflows.
    Ok(offset.wrapping_mul(scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_durations() {
        assert_eq!(parse_duration_nanos("PT00:00:01"), Ok(1_000_000_000));
        assert_eq!(parse_duration_nanos("pt1:02"), Ok(3_720_000_000_000));
        assert_eq!(parse_duration_nanos("PT0:00:00.5"), Ok(500_000_000));
        assert_eq!(parse_duration_nanos("PT0:00.000000001"), Ok(1));
        assert_eq!(parse_duration_nanos("-PT0:01"), Ok(-60_000_000_000));
        assert_eq!(parse_duration_nanos("-PT-0:01"), Ok(60_000_000_000));
        // without the `PT` it is neither form
        assert!(parse_duration_nanos("00:00:01").is_err());
        assert!(parse_duration_nanos("PT0:00:00.0000000001").is_err());
    }

    #[test]
    fn iso_durations() {
        assert_eq!(parse_duration_nanos("PT1S"), Ok(1_000_000_000));
        assert_eq!(parse_duration_nanos("PT0.5S"), Ok(500_000_000));
        assert_eq!(parse_duration_nanos("PT20.345S"), Ok(20_345_000_000));
        assert_eq!(parse_duration_nanos("PT15M"), Ok(900_000_000_000));
        assert_eq!(parse_duration_nanos("P2DT3H4M"), Ok(183_840_000_000_000));
        assert_eq!(parse_duration_nanos("pt1m"), Ok(60_000_000_000));
        assert_eq!(
            parse_duration_nanos("PT-6H3M"),
            Ok((-6 * 3600 + 180) * 1_000_000_000)
        );
        assert_eq!(
            parse_duration_nanos("-PT6H3M"),
            Ok(-(6 * 3600 + 180) * 1_000_000_000)
        );
        assert_eq!(
            parse_duration_nanos("-PT-6H+3M"),
            Ok((6 * 3600 - 180) * 1_000_000_000)
        );
        assert_eq!(parse_duration_nanos("PT-0.5S"), Ok(-500_000_000));
        assert_eq!(parse_duration_nanos("PT1,5S"), Ok(1_500_000_000));
        for bad in [
            "", "P", "PT", "1S", "PT1", "PT1X", "PT1M1H", "PT1.5M", "P1DT", "PT1S ", "P1S",
            "PT1H1H", "nope",
        ] {
            assert!(parse_duration_nanos(bad).is_err(), "{bad}");
        }
        assert!(parse_duration_nanos("P200000D").is_err());
    }

    const JAN_2024: i64 = 1_704_067_200_000_000_000;

    #[test]
    fn instants_with_an_offset() {
        assert_eq!(parse_instant_nanos("2024-01-01T00:00:00Z"), Ok(JAN_2024));
        assert_eq!(parse_instant_nanos("2024-01-01T00:00Z"), Ok(JAN_2024));
        assert_eq!(
            parse_instant_nanos("2024-01-01T00:00:00.000000001Z"),
            Ok(JAN_2024 + 1)
        );
        assert_eq!(
            parse_instant_nanos("2024-01-01T01:30:00+01:30"),
            Ok(JAN_2024)
        );
        assert_eq!(
            parse_instant_nanos("2023-12-31T19:00:00-05:00"),
            Ok(JAN_2024)
        );
        assert_eq!(
            parse_instant_nanos("2024-01-01T00:00:00Z[UTC]"),
            Ok(JAN_2024)
        );
        assert_eq!(parse_instant_nanos("1969-12-31T23:59:59Z"), Ok(-1_000_000_000));
        assert_eq!(
            parse_instant_nanos("2024-02-29T00:00:00Z"),
            Ok(JAN_2024 + 59 * 86_400_000_000_000)
        );
        // strict: no day past the end of the month, and no zone means no instant
        assert!(parse_instant_nanos("2023-02-29T00:00:00Z").is_err());
        assert_eq!(
            parse_instant_nanos("2024-01-01T00:00:00"),
            Err("No time zone provided".to_string())
        );
    }

    #[test]
    fn instants_with_a_zone_name() {
        assert_eq!(parse_instant_nanos("2024-01-01T00:00:00 UTC"), Ok(JAN_2024));
        assert_eq!(parse_instant_nanos("2024-1-1T0:0 UTC"), Ok(JAN_2024));
        assert_eq!(
            parse_instant_nanos("2024-01-01T00:00:00.5 UTC"),
            Ok(JAN_2024 + 500_000_000)
        );
        // a zone of one character is never found, even `Z`
        assert!(parse_instant_nanos("2024-01-01T00:00:00 Z").is_err());
        assert!(parse_instant_nanos("2024-01-01 Z").is_err());
        assert!(parse_instant_nanos("20240-01-01T00:00:00 UTC").is_err());
        assert_eq!(parse_instant_nanos("2024-01-01 UTC"), Ok(JAN_2024));
        assert_eq!(parse_instant_nanos("2024-01-01T UTC"), Ok(JAN_2024));
        // lenient: a day past the end of the month is the last day of the month
        assert_eq!(
            parse_instant_nanos("2024-02-31T00:00:00 UTC"),
            parse_instant_nanos("2024-02-29T00:00:00 UTC")
        );
        for bad in ["nope", "2024-01-01", "2024-13-01T00:00:00 UTC", "2024-01-01T25:00 UTC"] {
            assert!(parse_instant_nanos(bad).is_err(), "{bad}");
        }
    }

    #[test]
    #[should_panic(expected = "unported: io.deephaven.time.TimeZoneAliases#zoneId")]
    fn zones_other_than_utc_are_not_ported() {
        let _ = parse_instant_nanos("2024-01-01T00:00:00 America/New_York");
    }

    #[test]
    fn whole_numbers_are_epoch_offsets_by_size() {
        assert_eq!(
            parse_instant_nanos("1704067200"),
            Ok(JAN_2024),
            "seconds"
        );
        assert_eq!(parse_instant_nanos("1704067200000"), Ok(JAN_2024), "millis");
        assert_eq!(
            parse_instant_nanos("1704067200000000"),
            Ok(JAN_2024),
            "micros"
        );
        assert_eq!(
            parse_instant_nanos("1704067200000000000"),
            Ok(JAN_2024),
            "nanos"
        );
        assert_eq!(parse_instant_nanos("-5"), Ok(-5_000_000_000));
    }

    fn request(period: Option<Period>, start: Option<StartTime>) -> TimeTableRequest {
        TimeTableRequest {
            result_id: None,
            blink_table: false,
            start_time: start,
            period,
        }
    }

    #[test]
    fn a_period_must_be_positive() {
        for period in [None, Some(Period::PeriodNanos(0)), Some(Period::PeriodNanos(-4))] {
            let found = match &period {
                Some(Period::PeriodNanos(nanos)) => *nanos,
                _ => 0,
            };
            let status = Handler::validate(&request(period, None)).unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert_eq!(
                status.message(),
                format!("periodNanos must be >= 0 (found: {found})")
            );
        }
        assert!(Handler::validate(&request(Some(Period::PeriodNanos(1)), None)).is_ok());
        assert!(Handler::validate(&request(
            Some(Period::PeriodString("PT1S".to_string())),
            None
        ))
        .is_ok());
    }

    #[test]
    fn a_period_string_that_does_not_parse_is_an_internal_error() {
        let status = Handler::validate(&request(
            Some(Period::PeriodString("nope".to_string())),
            None,
        ))
        .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().starts_with("Details Logged w/ID '"),
            "{}",
            status.message()
        );
    }

    #[test]
    fn a_start_in_nanos_counts_only_if_positive() {
        assert_eq!(
            start_nanos(&request(None, Some(StartTime::StartTimeNanos(0)))),
            Ok(None)
        );
        assert_eq!(
            start_nanos(&request(None, Some(StartTime::StartTimeNanos(-3)))),
            Ok(None)
        );
        assert_eq!(
            start_nanos(&request(None, Some(StartTime::StartTimeNanos(7)))),
            Ok(Some(7))
        );
        assert_eq!(start_nanos(&request(None, None)), Ok(None));
        assert_eq!(
            start_nanos(&request(
                None,
                Some(StartTime::StartTimeString("2024-01-01T00:00:00 UTC".into()))
            )),
            Ok(Some(JAN_2024))
        );
    }
}
