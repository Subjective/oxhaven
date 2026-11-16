//! Time literals: single-quoted text that Java replaces before it parses the formula.
//!
//! Instants in UTC, in a time zone Java has an alias for or in a region of the IANA
//! database, durations and periods are read. Java accepts more spellings (offsets from UTC,
//! epoch numbers, local dates and times, time zone names); those are reported as
//! unsupported, by their text.

use chrono::{LocalResult, NaiveDate, Offset, TimeZone};
use oxhaven_contracts::expr::FormulaError;

/// What a time literal denotes.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum TimeValue {
    /// Nanoseconds since the epoch.
    Instant(i64),
    /// A `java.time.Duration`, which Java makes with `DateTimeUtils#parseDuration`.
    Duration,
    /// A `java.time.Period`, which Java makes with `DateTimeUtils#parsePeriod`.
    Period,
}

/// A single-quoted span of the formula that Java treats as a time literal.
pub(crate) struct TimeLiteral {
    /// Byte offset of the opening quote.
    pub(crate) start: usize,
    /// Byte offset just past the closing quote.
    pub(crate) end: usize,
    /// What the literal is, or why it is not accepted.
    pub(crate) value: Result<TimeValue, FormulaError>,
}

/// Finds the time literals of a formula.
///
/// Behavior: `io.deephaven.time.TimeLiteralReplacedExpression#convertExpression`, which
/// matches `'[^']*'` over the raw formula text, from left to right and without regard to
/// string literals, and leaves alone every match whose content is at most one `char` long.
pub(crate) fn time_literals(text: &str) -> Vec<TimeLiteral> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(open) = text[from..].find('\'').map(|at| from + at) {
        let Some(close) = text[open + 1..].find('\'').map(|at| open + 1 + at) else {
            break;
        };
        let content = &text[open + 1..close];
        if content.encode_utf16().count() > 1 {
            found.push(TimeLiteral {
                start: open,
                end: close + 1,
                value: parse(content),
            });
        }
        from = close + 1;
    }
    found
}

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// A time zone of a time literal.
enum Zone {
    Utc,
    Region(chrono_tz::Tz),
}

/// The zone a time literal names after its space, when this server knows that Java knows it.
///
/// Behavior: `io.deephaven.time.TimeZoneAliases#zoneId`, which is `ZoneId.of(name, aliases)`
/// with the aliases of `default_time_zone_aliases.csv`. Java 42.9 rejects `EST`, `NY` and
/// `et`. Only the aliases, `UTC`, `GMT` and names with a `/` are looked up here, case
/// sensitively as in Java; an offset such as `+01:00` or `UTC+1`, which Java reads, is left
/// unsupported.
fn zone(name: &str) -> Option<Zone> {
    let region = match name {
        "UTC" => return Some(Zone::Utc),
        "ET" => "America/New_York",
        "CT" => "America/Chicago",
        "MT" => "America/Denver",
        "PT" => "America/Los_Angeles",
        "HK" => "Asia/Hong_Kong",
        "JP" => "Asia/Tokyo",
        "LON" => "Europe/London",
        "GMT" => "GMT",
        other if other.contains('/') => other,
        _ => return None,
    };
    region.parse().ok().map(Zone::Region)
}

/// The seconds since the epoch of a local date and time in `zone`.
///
/// Behavior: `java.time.LocalDateTime#atZone`: a time that occurs twice, when clocks are
/// set back, is the earlier of the two, and a time that the clocks skip is moved later by
/// the length of the gap.
fn zoned_seconds(
    zone: &chrono_tz::Tz,
    date: (i64, i64, i64),
    time: (i64, i64, i64),
) -> Option<i64> {
    let (year, month, day) = date;
    let (hour, minute, second) = time;
    let local = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)?.and_hms_opt(
        hour as u32,
        minute as u32,
        second as u32,
    )?;
    Some(match zone.from_local_datetime(&local) {
        LocalResult::Single(only) => only.timestamp(),
        LocalResult::Ambiguous(earlier, _) => earlier.timestamp(),
        LocalResult::None => {
            // The offset in force before the gap, read a day ahead of it.
            let before = local.checked_sub_signed(chrono::Duration::days(1))?;
            let offset = zone.offset_from_utc_datetime(&before).fix();
            local.and_utc().timestamp() - i64::from(offset.local_minus_utc())
        }
    })
}

/// Reads the content of a time literal.
///
/// Behavior: `io.deephaven.time.TimeLiteralReplacedExpression#convertExpression`, which
/// tries an instant, then a period, then a duration, and then the kinds this server does
/// not read.
fn parse(content: &str) -> Result<TimeValue, FormulaError> {
    match parse_instant(content) {
        Ok(nanos) => Ok(TimeValue::Instant(nanos)),
        Err(error) => {
            if period_days(content).is_some() {
                Ok(TimeValue::Period)
            } else if duration_nanos(content).is_some() {
                Ok(TimeValue::Duration)
            } else {
                Err(error)
            }
        }
    }
}

/// The text after an optional sign and the `P` every period and duration starts with, and
/// whether the sign is `-`.
fn after_p(content: &str) -> Option<(bool, &str)> {
    let (negative, rest) = match content.as_bytes().first()? {
        b'-' => (true, &content[1..]),
        b'+' => (false, &content[1..]),
        _ => (false, content),
    };
    let rest = rest.strip_prefix(['P', 'p'])?;
    Some((negative, rest))
}

/// Takes `[-+]?digits` followed by the letter `unit`, in either case, off the front of
/// `text`; `None` when `text` does not start so.
fn component(text: &str, unit: u8) -> Option<(i64, &str)> {
    let bytes = text.as_bytes();
    let signed = usize::from(matches!(bytes.first(), Some(b'-' | b'+')));
    let digits = bytes[signed..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    let end = signed + digits;
    if digits == 0 || !bytes.get(end)?.eq_ignore_ascii_case(&unit) {
        return None;
    }
    Some((text[..end].parse().ok()?, &text[end + 1..]))
}

/// The days of a period that has no years and no months, or `Some(None)` for a period that
/// has them; `None` when the text is not a period.
///
/// Behavior: `java.time.Period#parse`: `[-+]?P[nY][nM][nW][nD]` in either case, each `n`
/// with its own sign, at least one of them present. A week is seven days.
pub(crate) fn period_days(content: &str) -> Option<Option<i32>> {
    let (negative, mut rest) = after_p(content)?;
    let mut found = [None; 4];
    for (slot, unit) in found.iter_mut().zip(*b"YMWD") {
        if let Some((value, after)) = component(rest, unit) {
            *slot = Some(value);
            rest = after;
        }
    }
    if !rest.is_empty() || found.iter().all(Option::is_none) {
        return None;
    }
    let [years, months, weeks, days] = found.map(|value| value.unwrap_or(0));
    let sign = if negative { -1 } else { 1 };
    let total = weeks
        .checked_mul(7)
        .and_then(|from_weeks| from_weeks.checked_add(days))
        .and_then(|days| days.checked_mul(sign))
        .and_then(|days| i32::try_from(days).ok());
    Some(match total {
        Some(days) if years == 0 && months == 0 => Some(days),
        _ => None,
    })
}

/// The nanoseconds of a duration, or `Some(None)` for one a `long` of nanoseconds cannot
/// hold or that is written as `PThh:mm[:ss]`; `None` when the text is not a duration.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseDuration`, which is
/// `java.time.Duration#parse` for `[-+]?P[nD][T[nH][nM][n[.f]S]]` in either case: each `n`
/// has its own sign, at least one is present, one follows a `T`, and the fraction of up to
/// nine digits has the sign of its seconds.
pub(crate) fn duration_nanos(content: &str) -> Option<Option<i64>> {
    let (negative, rest) = after_p(content)?;
    if let Some(clock) = rest.strip_prefix(['T', 't']) {
        // `PT1:30:00`, Deephaven's own spelling.
        let clock = clock.strip_prefix('-').unwrap_or(clock);
        let plausible = clock.contains(':')
            && clock.starts_with(|c: char| c.is_ascii_digit())
            && clock
                .chars()
                .all(|c| c.is_ascii_digit() || c == ':' || c == '.');
        if plausible {
            return Some(None);
        }
    }
    let mut total: Option<i128> = Some(0);
    let mut add = |nanos: Option<i128>| total = total.zip(nanos).map(|(a, b)| a + b);
    let mut any = false;
    let mut rest = rest;
    if let Some((days, after)) = component(rest, b'D') {
        add(Some(i128::from(days) * 86_400 * NANOS_PER_SECOND));
        any = true;
        rest = after;
    }
    if !rest.is_empty() {
        rest = rest.strip_prefix(['T', 't'])?;
        if rest.is_empty() {
            return None;
        }
        for (unit, seconds) in [(b'H', 3_600), (b'M', 60)] {
            if let Some((value, after)) = component(rest, unit) {
                add(Some(i128::from(value) * seconds * NANOS_PER_SECOND));
                any = true;
                rest = after;
            }
        }
        if !rest.is_empty() {
            // Seconds, with a fraction after `.` or `,`.
            let end = rest.len() - 1;
            if !rest.as_bytes()[end].eq_ignore_ascii_case(&b'S') {
                return None;
            }
            let (whole, fraction) = match rest[..end].split_once(['.', ',']) {
                Some((whole, fraction)) => (whole, fraction),
                None => (&rest[..end], ""),
            };
            let digits = whole.trim_start_matches(['-', '+']);
            let signs = whole.len() - digits.len();
            if signs > 1
                || digits.is_empty()
                || !digits.bytes().all(|b| b.is_ascii_digit())
                || fraction.len() > 9
                || !fraction.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            let seconds: Option<i128> = whole.parse::<i64>().ok().map(i128::from);
            let mut nanos = 0_i128;
            for place in 0..9 {
                let digit = fraction
                    .as_bytes()
                    .get(place)
                    .map_or(0, |d| i128::from(d - b'0'));
                nanos = nanos * 10 + digit;
            }
            if whole.starts_with('-') {
                nanos = -nanos;
            }
            add(seconds.map(|seconds| seconds * NANOS_PER_SECOND + nanos));
            any = true;
        }
    }
    if !any {
        return None;
    }
    let total = total.map(|total| if negative { -total } else { total });
    // The smallest long is the value Java reserves for null.
    Some(
        total
            .and_then(|total| i64::try_from(total).ok())
            .filter(|nanos| *nanos != i64::MIN),
    )
}

/// Reads the content of a time literal as an instant.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseInstant` and `#parseZonedDateTime`, for
/// these spellings only: `yyyy-MM-ddTHH:mm[:ss[.f]] ZONE` with a zone [`zone`] knows, the
/// same with `Z` directly after the time in place of the zone
/// (`java.time.ZonedDateTime#parse`), and `yyyy-MM-dd ZONE` or `yyyy-MM-ddT ZONE`, which is
/// midnight. Fields have exactly the widths shown and `f` is one to nine digits. `Z` after
/// a space is not a time zone to `io.deephaven.time.TimeZoneAliases#zoneId`, so Java
/// rejects it.
///
/// An instant that Java cannot store in a column is unsupported: Java can still compare
/// with it. `DateTimeUtils#epochNanos` refuses every instant from epoch second
/// `Long.MAX_VALUE / 1_000_000_000` on, which is the last 0.85 seconds a long of
/// nanoseconds could hold.
fn parse_instant(content: &str) -> Result<i64, FormulaError> {
    let unsupported = || FormulaError::Unsupported(format!("time literal '{content}'"));
    let invalid = || {
        FormulaError::Syntax(format!(
            "Cannot parse literal as a datetime, date, time, duration, period, or timezone: \
             {content}"
        ))
    };
    // Every spelling Java accepts, of any kind of time literal, is made of these characters.
    let plausible = |c: char| c.is_ascii_alphanumeric() || " :./_+-[],~".contains(c);
    if !content.chars().all(plausible) {
        return Err(invalid());
    }
    let bytes = content.as_bytes();
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let field = bytes.get(range)?;
        if field.is_empty() || !field.iter().all(u8::is_ascii_digit) {
            return None;
        }
        std::str::from_utf8(field).ok()?.parse().ok()
    };
    let is = |at: usize, wanted: u8| bytes.get(at) == Some(&wanted);

    let (Some(year), Some(month), Some(day)) = (digits(0..4), digits(5..7), digits(8..10)) else {
        return Err(unsupported());
    };
    if !(is(4, b'-') && is(7, b'-')) {
        return Err(unsupported());
    }

    let rest = &content[10..];
    if rest.ends_with(" Z") {
        return Err(invalid());
    }
    // Java takes the zone from the first space on.
    let (clock, zone) = match rest.split_once(' ') {
        Some((clock, name)) => match zone(name) {
            Some(zone) => (clock, zone),
            None => return Err(unsupported()),
        },
        None => match rest.strip_suffix('Z') {
            Some(clock) if clock.len() > 1 => (clock, Zone::Utc),
            _ => return Err(unsupported()),
        },
    };
    let (hour, minute, second, nanos) = if matches!(clock, "" | "T") {
        (0, 0, 0, 0)
    } else {
        let Some(time) = clock.strip_prefix('T') else {
            return Err(unsupported());
        };
        let time = time.as_bytes();
        let field = |at: usize| -> Option<i64> {
            let field = time.get(at..at + 2)?;
            field
                .iter()
                .all(u8::is_ascii_digit)
                .then(|| i64::from(field[0] - b'0') * 10 + i64::from(field[1] - b'0'))
        };
        let (Some(hour), Some(minute)) = (field(0), field(3)) else {
            return Err(unsupported());
        };
        if time.get(2) != Some(&b':') {
            return Err(unsupported());
        }
        let (second, nanos) = match time.len() {
            5 => (0, 0),
            8.. if time[5] == b':' => {
                let Some(second) = field(6) else {
                    return Err(unsupported());
                };
                let fraction = &time[8..];
                let nanos = match fraction {
                    [] => 0,
                    [b'.', digits @ ..]
                        if (1..=9).contains(&digits.len())
                            && digits.iter().all(u8::is_ascii_digit) =>
                    {
                        let mut nanos = 0_i64;
                        for place in 0..9 {
                            let digit = digits.get(place).map_or(0, |d| i64::from(d - b'0'));
                            nanos = nanos * 10 + digit;
                        }
                        nanos
                    }
                    _ => return Err(unsupported()),
                };
                (second, nanos)
            }
            _ => return Err(unsupported()),
        };
        (hour, minute, second, nanos)
    };

    // `java.time` rejects a field outside its range, and then no other kind of time literal
    // matches text of this shape.
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || minute > 59 || second > 59 {
        return Err(invalid());
    }
    if hour > 24 {
        return Err(invalid());
    }
    // Java's parser resolves a day past the end of the month and hour 24 by its own rules.
    if hour == 24 || day > days_in_month(year, month) {
        return Err(unsupported());
    }

    let seconds = match &zone {
        Zone::Utc => {
            days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second
        }
        Zone::Region(region) => {
            match zoned_seconds(region, (year, month, day), (hour, minute, second)) {
                Some(seconds) => seconds,
                None => return Err(unsupported()),
            }
        }
    };
    let total = i128::from(seconds) * NANOS_PER_SECOND + i128::from(nanos);
    // The smallest long is the value Java reserves for a null instant.
    match i64::try_from(total) {
        Ok(value) if value != i64::MIN && seconds < i64::MAX / 1_000_000_000 => Ok(value),
        _ => Err(FormulaError::Unsupported(format!(
            "time literal '{content}' outside the range of a 64-bit nanosecond timestamp"
        ))),
    }
}

/// Reads text as `DateTimeUtils#parseInstant` reads a `String` that is not null: an
/// instant spelled as a time literal spells it, or a whole number of seconds,
/// milliseconds, microseconds or nanoseconds since the epoch, told apart by its size. The
/// smallest `long` is the null instant.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseInstant` and `#epochAutoToEpochNanos`.
/// A spelling [`parse_instant`] leaves unsupported is unsupported here when it starts as
/// a date does, with four digits and a `-`, or with a sign; Java reads no other text.
pub(crate) fn instant_of_text(text: &str) -> Result<i64, FormulaError> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    if !(1..=19).contains(&digits.len()) || !digits.bytes().all(|b| b.is_ascii_digit()) {
        let bytes = text.as_bytes();
        let dated =
            bytes.len() > 4 && bytes[..4].iter().all(u8::is_ascii_digit) && bytes[4] == b'-';
        let signed = matches!(bytes.first(), Some(b'+' | b'-'));
        return match parse_instant(text) {
            Err(FormulaError::Unsupported(_)) if !dated && !signed => Err(FormulaError::Syntax(
                format!("Cannot parse instant: {text}"),
            )),
            other => other,
        };
    }
    // `TimeConstants#MICROTIME_THRESHOLD`.
    const THRESHOLD: u64 = 9_309_341_000_000;
    let offset: i64 = text
        .parse()
        .map_err(|_| FormulaError::Syntax(format!("Cannot parse instant: {text}")))?;
    let magnitude = offset.unsigned_abs();
    Ok(if magnitude > 1000 * THRESHOLD {
        offset
    } else if magnitude > THRESHOLD {
        offset * 1_000
    } else if magnitude > THRESHOLD / 1000 {
        offset * 1_000_000
    } else {
        offset * 1_000_000_000
    })
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 31,
    }
}

/// Days from 1970-01-01 to the given date of the proleptic Gregorian calendar, which is the
/// calendar of `java.time.LocalDate`.
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    // Years run from March, so that the leap day is the last day of the year.
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The inverse of [`days_from_civil`]: year, month and day.
pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn value(content: &str) -> Result<i64, FormulaError> {
        parse_instant(content)
    }

    /// Expected values are `java.time.Instant` epoch seconds times 10^9 plus nanoseconds;
    /// 1700000000 seconds is 2023-11-14T22:13:20Z.
    #[test]
    fn utc_instants_are_read_to_the_nanosecond() {
        let base = 1_700_000_000_000_000_000_i64;
        let cases = [
            ("2023-11-14T22:13:20 UTC", base),
            ("2023-11-14T22:13:20Z", base),
            ("2023-11-14T22:13 UTC", base - 20_000_000_000),
            ("2023-11-14T22:13Z", base - 20_000_000_000),
            ("2023-11-14T22:13:20.5 UTC", base + 500_000_000),
            ("2023-11-14T22:13:20.123456789 UTC", base + 123_456_789),
            ("2023-11-14T22:13:20.000000001Z", base + 1),
            (
                "2023-11-14 UTC",
                base - (22 * 3600 + 13 * 60 + 20) * 1_000_000_000,
            ),
            (
                "2023-11-14T UTC",
                base - (22 * 3600 + 13 * 60 + 20) * 1_000_000_000,
            ),
            ("1970-01-01T00:00:00 UTC", 0),
            ("1969-12-31T23:59:59.999999999 UTC", -1),
            ("2000-02-29T00:00:00 UTC", 951_782_400_000_000_000),
            ("2000-03-01T00:00:00 UTC", 951_868_800_000_000_000),
            ("1900-03-01T00:00:00 UTC", -2_203_891_200_000_000_000),
            // The last instant `DateTimeUtils#epochNanos` converts.
            (
                "2262-04-11T23:47:15.999999999 UTC",
                9_223_372_035_999_999_999,
            ),
            ("1677-09-21T00:12:43.145224193 UTC", i64::MIN + 1),
        ];
        for (content, expected) in cases {
            assert_eq!(value(content), Ok(expected), "{content}");
        }
    }

    /// Expected values are what Java 42.9 returned for the literal in a formula.
    #[test]
    fn instants_in_a_time_zone_are_read_as_java_reads_them() {
        let cases = [
            ("2024-01-01T00:00:00 ET", 1_704_085_200_000_000_000),
            ("2024-01-01 ET", 1_704_085_200_000_000_000),
            ("2024-01-01T ET", 1_704_085_200_000_000_000),
            (
                "2024-01-01T00:00 America/Chicago",
                1_704_088_800_000_000_000,
            ),
            ("2024-01-01T00:00:00 GMT", 1_704_067_200_000_000_000),
            ("2024-01-01T00:00:00 JP", 1_704_034_800_000_000_000),
            (
                "2024-01-01T00:00:00 Europe/London",
                1_704_067_200_000_000_000,
            ),
            ("2015-01-01T00:00:00 ET", 1_420_088_400_000_000_000),
            // The hour the clocks skip is moved later by the hour.
            ("2024-03-10T02:30:00 ET", 1_710_055_800_000_000_000),
            (
                "2024-03-10T01:59:59.999999999 ET",
                1_710_053_999_999_999_999,
            ),
            // The hour that occurs twice is its first occurrence.
            ("2024-11-03T01:30:00 ET", 1_730_611_800_000_000_000),
        ];
        for (content, expected) in cases {
            assert_eq!(value(content), Ok(expected), "{content}");
        }
    }

    #[test]
    fn periods_and_durations_are_told_apart_as_java_tells_them() {
        for (content, expected) in [
            ("P1d", TimeValue::Period),
            ("P1D", TimeValue::Period),
            ("P2W", TimeValue::Period),
            ("P1Y", TimeValue::Period),
            ("P1M", TimeValue::Period),
            ("-P1D", TimeValue::Period),
            ("PT1m", TimeValue::Duration),
            ("PT1H", TimeValue::Duration),
            ("P1DT1H", TimeValue::Duration),
            ("PT1.5S", TimeValue::Duration),
            ("pt1H30m", TimeValue::Duration),
            ("PT1:30:00", TimeValue::Duration),
        ] {
            assert_eq!(parse(content), Ok(expected), "{content}");
        }
        assert_eq!(period_days("P2W3D"), Some(Some(17)));
        assert_eq!(period_days("-P1D"), Some(Some(-1)));
        assert_eq!(period_days("P1M"), Some(None));
        assert_eq!(period_days("P1Y2D"), Some(None));
        assert_eq!(period_days("P"), None);
        assert_eq!(period_days("PT1H"), None);
        assert_eq!(duration_nanos("PT1m"), Some(Some(60_000_000_000)));
        assert_eq!(duration_nanos("P1DT1H"), Some(Some(90_000_000_000_000)));
        assert_eq!(duration_nanos("PT-1.5S"), Some(Some(-1_500_000_000)));
        assert_eq!(duration_nanos("-PT1H"), Some(Some(-3_600_000_000_000)));
        assert_eq!(duration_nanos("PT0.000000001S"), Some(Some(1)));
        assert_eq!(duration_nanos("PT1:30:00"), Some(None));
        assert_eq!(duration_nanos("P99999999999D"), Some(None));
        for content in [
            "PT",
            "P",
            "P1DT",
            "PT1",
            "PT1X",
            "PT1.0000000001S",
            "PT1H2D",
        ] {
            assert_eq!(duration_nanos(content), None, "{content}");
        }
    }

    #[test]
    fn other_time_literals_are_named() {
        for content in [
            "2023-11-14T22:13:20 NY",
            "2023-11-14T22:13:20 EST",
            "2023-11-14T22:13:20 et",
            "2023-11-14T22:13:20 America/Nowhere",
            "2023-11-14T22:13:20 +01:00",
            "2023-11-14T22:13:20 UTC+1",
            "2023-11-14T22:13:20+01:00",
            "2023-11-14",
            "2023-11-14T22:13:20",
            "2023-1-4T22:13:20 UTC",
            "2023-11-14T2:13:20 UTC",
            "2023-11-14T22:13:20. UTC",
            "2023-11-14T22:13:20.1234567890 UTC",
            "2023-11-14t22:13:20 UTC",
            "1700000000",
            "12:30",
            "UTC",
            // Java's resolver adjusts these instead of rejecting them.
            "2023-02-30T00:00:00 UTC",
            "2023-11-14T24:00:00 UTC",
        ] {
            assert_eq!(
                value(content),
                Err(FormulaError::Unsupported(format!(
                    "time literal '{content}'"
                ))),
                "{content}"
            );
        }
        for content in [
            "2262-04-11T23:47:16.854775808 UTC",
            "2262-04-11T23:47:16.854775807 UTC",
            "2262-04-11T23:47:16 UTC",
            "1677-09-21T00:12:43.145224192 UTC",
            "9999-01-01 UTC",
            "0001-01-01 UTC",
        ] {
            assert!(
                matches!(value(content), Err(FormulaError::Unsupported(m)) if m.contains("range")),
                "{content}"
            );
        }
    }

    #[test]
    fn fields_out_of_range_are_syntax_errors() {
        for content in [
            "2023-13-14T22:13:20 UTC",
            "2023-00-14T22:13:20 UTC",
            "2023-11-32T22:13:20 UTC",
            "2023-11-00 UTC",
            "2023-11-14T25:13:20 UTC",
            "2023-11-14T22:60:20 UTC",
            "2023-11-14T22:13:60Z",
            "2023-11-14T22:13:20 Z",
            "2023-11-14 Z",
            "2023-11-14T Z",
            "\\n",
            "ab\u{e9}",
            "s` + `it",
            "a = b",
        ] {
            assert!(
                matches!(value(content), Err(FormulaError::Syntax(_))),
                "{content} gave {:?}",
                value(content)
            );
        }
    }

    #[test]
    fn single_quoted_spans_pair_up_from_the_left() {
        let spans = |text: &str| -> Vec<(usize, usize)> {
            time_literals(text)
                .iter()
                .map(|literal| (literal.start, literal.end))
                .collect()
        };
        assert_eq!(spans("A == 'x'"), vec![]);
        assert_eq!(spans("T > '2023-11-14 UTC'"), vec![(4, 20)]);
        // The quotes pair up without regard to the string literal.
        assert_eq!(spans("`it's` + 'a' + `b`"), vec![(3, 10)]);
        assert_eq!(spans("'a' + 'bc' + 'd"), vec![(6, 10)]);
        assert_eq!(spans("`it's`"), vec![]);
        // An apostrophe char literal: the first two quotes pair up around a backslash.
        assert_eq!(spans("'\\''"), vec![]);
        // One supplementary character is two chars to Java.
        assert_eq!(spans("'\u{1F600}'"), vec![(0, 6)]);
    }

    #[test]
    fn calendar_conversions_invert_each_other() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2023, 11, 14), 19_675);
        assert_eq!(civil_from_days(19_675), (2023, 11, 14));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        for days in (-110_000..110_000).step_by(37) {
            let (year, month, day) = civil_from_days(days);
            assert_eq!(days_from_civil(year, month, day), days);
        }
    }
}
