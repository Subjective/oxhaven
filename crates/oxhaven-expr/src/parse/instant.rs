//! Time literals: single-quoted text that Java replaces before it parses the formula.
//!
//! Instants with a year of four digits, in any zone Java reads, durations, periods, dates,
//! times of day and zones are read. Java accepts more spellings (longer years after a
//! sign, some dates and times); those are reported as unsupported, by their text.

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
    /// A `java.time.ZoneId`, which Java makes with `DateTimeUtils#parseTimeZone`.
    Zone,
    /// A `java.time.LocalDate`, which Java makes with `DateTimeUtils#parseLocalDate`.
    LocalDate,
    /// A `java.time.LocalTime`, which Java makes with `DateTimeUtils#parseLocalTime`.
    LocalTime,
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
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Zone {
    Utc,
    Region(chrono_tz::Tz),
}

/// The zone a time literal names after its space, when this server knows that Java knows it.
///
/// Behavior: `io.deephaven.time.TimeZoneAliases#zoneId`, which is `ZoneId.of(name, aliases)`
/// with the aliases of `default_time_zone_aliases.csv`: an alias, or the name of a region
/// of the time zone database, case sensitively. Java 42.9 was asked for each of the 597
/// names of the database compiled in here and reads all but `EST`, `HST`, `MST` and `ROC`;
/// it rejects `NY` and `et`. A zone written as an offset (`+01:00`, `UTC+1`), which Java
/// reads too, is no region: see [`zone_offset`].
pub(crate) fn zone(name: &str) -> Option<Zone> {
    let region = match name {
        "UTC" => return Some(Zone::Utc),
        "ET" => "America/New_York",
        "CT" => "America/Chicago",
        "MT" => "America/Denver",
        "PT" => "America/Los_Angeles",
        "HK" => "Asia/Hong_Kong",
        "JP" => "Asia/Tokyo",
        "LON" => "Europe/London",
        "EST" | "HST" | "MST" | "ROC" => return None,
        other => other,
    };
    region.parse().ok().map(Zone::Region)
}

/// The seconds east of UTC of a zone written as an offset: `None` when `name` is not
/// written as one, and `Some(None)` when it is and Java rejects it.
///
/// Behavior: `java.time.ZoneId#of` for an id that starts with a sign, or with `UTC`, `GMT`
/// or `UT` and a sign, which is `java.time.ZoneOffset#of`: `h`, `hh`, `hh:mm`, `hhmm`,
/// `hh:mm:ss` or `hhmmss` after the sign, at most 18 hours. Seen on Java 42.9: `+1`,
/// `-05:30`, `+010030`, `UTC-0530`, `GMT+01:00:30`, `UT+1`, `-18:00` and `-0` are read, and
/// `+19:00`, `+01:60`, `+1:00`, `UTC+18:01`, `UTC+` and `UTC0` are not.
pub(crate) fn zone_offset(name: &str) -> Option<Option<i64>> {
    // `ZoneId.of("UT")` is UTC under the id `UT`, which is no region of the database.
    if name == "UT" {
        return Some(Some(0));
    }
    let rest = ["UTC", "GMT", "UT"]
        .iter()
        .find_map(|prefix| name.strip_prefix(prefix))
        .filter(|rest| rest.starts_with(['+', '-']))
        .unwrap_or(name);
    let negative = match rest.as_bytes().first()? {
        b'+' => false,
        b'-' => true,
        _ => return None,
    };
    let fields = &rest.as_bytes()[1..];
    let two = |at: usize| -> Option<i64> {
        let field = fields.get(at..at + 2)?;
        field
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| i64::from(field[0] - b'0') * 10 + i64::from(field[1] - b'0'))
    };
    let colon = |at: usize| fields.get(at) == Some(&b':');
    let parts = match fields.len() {
        1 if fields[0].is_ascii_digit() => Some((i64::from(fields[0] - b'0'), Some(0), Some(0))),
        2 => two(0).map(|hours| (hours, Some(0), Some(0))),
        4 => two(0).map(|hours| (hours, two(2), Some(0))),
        5 if colon(2) => two(0).map(|hours| (hours, two(3), Some(0))),
        6 => two(0).map(|hours| (hours, two(2), two(4))),
        8 if colon(2) && colon(5) => two(0).map(|hours| (hours, two(3), two(6))),
        _ => None,
    };
    Some(match parts {
        Some((hours, Some(minutes), Some(seconds)))
            if minutes <= 59
                && seconds <= 59
                && (hours < 18 || (hours == 18 && minutes == 0 && seconds == 0)) =>
        {
            let total = hours * 3_600 + minutes * 60 + seconds;
            Some(if negative { -total } else { total })
        }
        _ => None,
    })
}

/// The seconds since the epoch of a local date and time in `zone`.
///
/// Behavior: `java.time.LocalDateTime#atZone`: a time that occurs twice, when clocks are
/// set back, is the earlier of the two, and a time that the clocks skip is moved later by
/// the length of the gap.
pub(crate) fn zoned_seconds(
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
/// tries an instant, then a period, a duration, a date, a time of day and a zone. The
/// instant is `DateTimeUtils#parseInstantQuiet`, which also reads a whole number since
/// the epoch ([`instant_of_text`]) and is null, so no instant, for the smallest `long`.
/// Seen on Java 42.9: `'9309340999'` is the instant 1680-06-12T00:28:45.290448384Z.
fn parse(content: &str) -> Result<TimeValue, FormulaError> {
    let digits = content.strip_prefix('-').unwrap_or(content);
    let instant = if (1..=19).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit())
    {
        instant_of_text(content).and_then(|nanos| {
            if nanos == i64::MIN {
                Err(FormulaError::Syntax(format!(
                    "Cannot parse literal as a datetime, date, time, duration, period, or \
                     timezone: {content}"
                )))
            } else {
                Ok(nanos)
            }
        })
    } else {
        parse_instant(content)
    };
    match instant {
        Ok(nanos) => Ok(TimeValue::Instant(nanos)),
        Err(error) => {
            if period_days(content).is_some() {
                Ok(TimeValue::Period)
            } else if duration_nanos(content).is_some() {
                Ok(TimeValue::Duration)
            } else if let Some(date) = local_date_days(content) {
                // A date Java reads in a spelling this server does not is refused.
                date.map(|_| TimeValue::LocalDate)
            } else if let Some(time) = local_time_nanos(content) {
                time.map(|_| TimeValue::LocalTime)
            } else if zone(content).is_some() {
                Ok(TimeValue::Zone)
            } else {
                Err(error)
            }
        }
    }
}

/// The days since 1970-01-01 of a date; `None` when the text is no date, and an error for
/// a date Java reads that this server does not, or that is outside the range of a cell.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalDate`: the year in four digits,
/// or more after a `+`, or after a `-`, then the month and the day in one or two digits
/// each, separated by `-`. A day past the end of its month is the month's last day (the
/// formatter's `ResolverStyle.SMART`): seen on Java 42.9, `2021-02-29` is 2021-02-28 and
/// `2020-04-32` fails. Read here: four digits without a sign or after `-`, and five to
/// nine after `+`; other signed years are refused.
pub(crate) fn local_date_days(text: &str) -> Option<Result<i64, FormulaError>> {
    let (sign, unsigned) = match text.as_bytes().first()? {
        b'+' => (1, &text[1..]),
        b'-' => (-1, &text[1..]),
        _ => (0, text),
    };
    let mut fields = unsigned.split('-');
    let (year, month, day) = (fields.next()?, fields.next()?, fields.next()?);
    let digits = |field: &str, most: usize| {
        (1..=most).contains(&field.len()) && field.bytes().all(|b| b.is_ascii_digit())
    };
    if fields.next().is_some() || !digits(year, 10) || !digits(month, 2) || !digits(day, 2) {
        return None;
    }
    let read = match sign {
        1 => (5..=9).contains(&year.len()),
        _ => year.len() == 4,
    };
    if !read {
        if sign == 0 {
            // Java needs a `+` before a year of more than four digits.
            return None;
        }
        return Some(Err(FormulaError::Unsupported(format!(
            "the date \"{text}\", in a spelling this server does not read \
             (io.deephaven.time.DateTimeUtils#parseLocalDate)"
        ))));
    }
    let number = |field: &str| field.parse::<i64>().ok();
    let (year, month, day) = (number(year)?, number(month)?, number(day)?);
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let year = if sign < 0 { -year } else { year };
    let days = days_from_civil(year, month, day.min(days_in_month(year, month)));
    Some(if i32::try_from(days).is_ok() {
        Ok(days)
    } else {
        Err(FormulaError::Unsupported(format!(
            "the date \"{text}\", outside the range of a LocalDate column"
        )))
    })
}

/// The nanoseconds since midnight of a time of day; `None` when the text is no time of
/// day, and an error for one Java reads that this server does not.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalTime`: the hour and the minute,
/// then optionally the second and after it a `.` and zero to nine digits of fraction,
/// each field one or two digits, separated by `:`. Seen on Java 42.9: `24:00` and
/// `24:00:00` are midnight, `12:30:45.` is 12:30:45, and ten digits of fraction fail. An
/// hour of 24 with anything but zeros is refused here.
pub(crate) fn local_time_nanos(text: &str) -> Option<Result<i64, FormulaError>> {
    let (clock, fraction) = match text.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (text, None),
    };
    let fields: Vec<&str> = clock.split(':').collect();
    let short =
        |field: &&str| (1..=2).contains(&field.len()) && field.bytes().all(|b| b.is_ascii_digit());
    if !(2..=3).contains(&fields.len()) || !fields.iter().all(short) {
        return None;
    }
    if fraction.is_some() && fields.len() != 3 {
        return None;
    }
    let fraction = fraction.unwrap_or("");
    if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let number = |field: &str| field.parse::<i64>().ok();
    let hour = number(fields[0])?;
    let minute = number(fields[1])?;
    let second = match fields.get(2) {
        Some(field) => number(field)?,
        None => 0,
    };
    let mut nanos = 0;
    for place in 0..9 {
        let digit = fraction
            .as_bytes()
            .get(place)
            .map_or(0, |d| i64::from(d - b'0'));
        nanos = nanos * 10 + digit;
    }
    if hour > 24 || minute > 59 || second > 59 {
        return None;
    }
    if hour == 24 {
        return Some(if minute == 0 && second == 0 && nanos == 0 {
            Ok(0)
        } else {
            Err(FormulaError::Unsupported(format!(
                "the time \"{text}\", in a spelling this server does not read \
                 (io.deephaven.time.DateTimeUtils#parseLocalTime)"
            )))
        });
    }
    Some(Ok(((hour * 60 + minute) * 60 + second)
        * NANOS_PER_SECOND as i64
        + nanos))
}

/// The years, months and days of a period; `Some(None)` for one whose numbers no Java
/// `int` holds, which Java refuses; `None` when the text is not a period.
///
/// Behavior: `java.time.Period#parse`: each number is an `int`, multiplied by -1 when the
/// period starts with `-`, and the days are the days plus seven times the weeks, all in
/// exact arithmetic.
pub(crate) fn period_parts(content: &str) -> Option<Option<[i32; 3]>> {
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
    let sign = if negative { -1 } else { 1 };
    let exact = |value: Option<i64>| -> Option<i32> {
        i32::try_from(value.unwrap_or(0)).ok()?.checked_mul(sign)
    };
    Some((|| {
        let [years, months, weeks, days] = found;
        let days = exact(days)?.checked_add(exact(weeks)?.checked_mul(7)?)?;
        Some([exact(years)?, exact(months)?, days])
    })())
}

/// The id of a zone this server knows, as `java.time.ZoneId#getId` gives it.
pub(crate) fn zone_id(zone: &Zone) -> &'static str {
    match zone {
        Zone::Utc => "UTC",
        Zone::Region(region) => region.name(),
    }
}

/// The zone whose id is `id`, one [`zone_id`] or [`zoned_of_text`] gives; `None` for any
/// other.
pub(crate) fn zone_of_id(id: &str) -> Option<Zone> {
    match id {
        "UTC" | "Z" => Some(Zone::Utc),
        _ => id.parse().ok().map(Zone::Region),
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
/// a year of four digits without a sign: the date and time with the zone after a space
/// ([`spaced`]), or with an offset directly after the time (`java.time.ZonedDateTime#parse`,
/// [`iso`]). `Z` after a space is not a time zone to
/// `io.deephaven.time.TimeZoneAliases#zoneId`, so Java rejects it.
///
/// An instant that Java cannot store in a column is unsupported: Java can still compare
/// with it. `DateTimeUtils#epochNanos` refuses every instant from epoch second
/// `Long.MAX_VALUE / 1_000_000_000` on, which is the last 0.85 seconds a long of
/// nanoseconds could hold.
fn parse_instant(content: &str) -> Result<i64, FormulaError> {
    parse_zoned(content).map(|(nanos, _)| nanos)
}

/// [`parse_instant`], with the id of the zone the text names: `Z` where the offset after
/// the time is none, the zone in brackets, and otherwise the id of the zone after the
/// space. A zone written as another offset, or one of Java's `SystemV` zones, is
/// unsupported here: no column of this server holds its id.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseZonedDateTime`, which keeps the zone
/// that `java.time.ZonedDateTime#parse` or `DateTimeUtils#parseTimeZoneQuiet` gives.
pub(crate) fn zoned_of_text(content: &str) -> Result<(i64, &'static str), FormulaError> {
    match parse_zoned(content)? {
        (nanos, Some(id)) => Ok((nanos, id)),
        (_, None) => Err(FormulaError::Unsupported(format!(
            "time literal '{content}' as a ZonedDateTime: a zone written as an offset"
        ))),
    }
}

/// Where a time literal places its local date and time.
enum Placed {
    In(Zone),
    /// Seconds east of UTC.
    Offset(i64),
    /// One of Java's `SystemV` zones with daylight time: its standard seconds east of UTC.
    SystemV(i64),
}

/// The zone `SystemV/...` names: its standard time in seconds east of UTC, and whether it
/// has daylight time. These thirteen are the ids of `java.time.ZoneId#getAvailableZoneIds`
/// on Java 42.9 that the database compiled in here lacks; every other id of Java's is in
/// it.
fn system_v(name: &str) -> Option<(i64, bool)> {
    let (hours_west, daylight) = match name.strip_prefix("SystemV/")? {
        "AST4" => (4, false),
        "AST4ADT" => (4, true),
        "CST6" => (6, false),
        "CST6CDT" => (6, true),
        "EST5" => (5, false),
        "EST5EDT" => (5, true),
        "HST10" => (10, false),
        "MST7" => (7, false),
        "MST7MDT" => (7, true),
        "PST8" => (8, false),
        "PST8PDT" => (8, true),
        "YST9" => (9, false),
        "YST9YDT" => (9, true),
        _ => return None,
    };
    Some((-hours_west * 3_600, daylight))
}

/// The seconds since the epoch of the local second `local` (counted as if in UTC) of
/// `year` in a `SystemV` zone with daylight time.
///
/// Behavior: the `SystemV` rule of the time zone database Java ships: daylight time from
/// the last Sunday of April to the last Sunday of October, at two in the morning of the
/// clock, in every year but 1974 (from January 6 to the last Sunday of November) and 1975
/// (from February 23). Seen on Java 42.9 for `SystemV/EST5EDT` and `SystemV/PST8PDT`
/// around each of these days and in 1900: a time the clocks skip is moved later by the
/// hour, and one that occurs twice is the earlier.
fn system_v_seconds(standard: i64, year: i64, local: i64) -> i64 {
    let last_sunday = |month: i64| {
        let last = days_in_month(year, month);
        // 1970-01-01 was a Thursday.
        last - (days_from_civil(year, month, last) + 4).rem_euclid(7)
    };
    let (start_month, start_day) = match year {
        1974 => (1, 6),
        1975 => (2, 23),
        _ => (4, last_sunday(4)),
    };
    let end_month = if year == 1974 { 11 } else { 10 };
    // The first second of daylight time on the clock, and the first that is standard again.
    let start = days_from_civil(year, start_month, start_day) * 86_400 + 3 * 3_600;
    let end = days_from_civil(year, end_month, last_sunday(end_month)) * 86_400 + 2 * 3_600;
    if (start..end).contains(&local) {
        local - standard - 3_600
    } else {
        local - standard
    }
}

/// The seconds east of UTC of an offset as `java.time.ZonedDateTime#parse` reads one after
/// a time (`+HH`, `+HH:mm` or `+HH:mm:ss`) or, with `minutes`, in brackets, where the
/// minutes are needed; `None` for any other text.
///
/// Seen on Java 42.9: `+01`, `+01:00:30`, `-18:00` and `-00:00` after a time and `[+02:00]`
/// and `[-02:00:30]` are read; `+0100`, `+1`, `+18:01`, `+19:00`, `+01:60`, `[+02]` and
/// `[+0200]` are not.
fn iso_offset(text: &str, minutes: bool) -> Option<i64> {
    let bytes = text.as_bytes();
    let negative = match bytes.first()? {
        b'+' => false,
        b'-' => true,
        _ => return None,
    };
    let two = |at: usize| -> Option<i64> {
        let field = bytes.get(at..at + 2)?;
        field
            .iter()
            .all(u8::is_ascii_digit)
            .then(|| i64::from(field[0] - b'0') * 10 + i64::from(field[1] - b'0'))
    };
    let colon = |at: usize| bytes.get(at) == Some(&b':');
    let (hours, minute, second) = match bytes.len() {
        3 if !minutes => (two(1)?, 0, 0),
        6 if colon(3) => (two(1)?, two(4)?, 0),
        9 if colon(3) && colon(6) => (two(1)?, two(4)?, two(7)?),
        _ => return None,
    };
    let total = hours * 3_600 + minute * 60 + second;
    if minute > 59 || second > 59 || total > 18 * 3_600 {
        return None;
    }
    Some(if negative { -total } else { total })
}

/// Whether Java reads `name` as the zone in brackets after an offset (`None` when it does
/// not), and the id of the zone when that is one a column of this server holds.
///
/// Behavior: `java.time.format.DateTimeFormatterBuilder#appendZoneRegionId`: `Z`, a
/// region of the time zone database case sensitively, or an offset with its minutes,
/// alone or after `UTC`, `GMT` or `UT`. Deephaven's aliases are no regions. Seen on Java
/// 42.9: `[UTC]`, `[GMT]`, `[UT]`, `[Z]`, `[Europe/Paris]`, `[US/Eastern]`, `[EST5EDT]`,
/// `[SystemV/EST5]`, `[+02:00]`, `[UTC+02:00]` and `[UT-02:00]` are read; `[ET]`, `[EST]`,
/// `[z]`, `[europe/paris]`, `[Europe/Nowhere]`, `[GMT+2]`, `[GMT+02]`, `[UTC+02]` and `[]`
/// are not.
fn bracket_zone(name: &str) -> Option<Option<&'static str>> {
    match name {
        "Z" => return Some(Some("Z")),
        "UTC" => return Some(Some("UTC")),
        // Java's own, which the database compiled in here lacks.
        "UT" => return Some(None),
        // Regions of the database compiled in here that Java does not have.
        "EST" | "HST" | "MST" | "ROC" | "GMT+0" | "GMT-0" => return None,
        _ => {}
    }
    if let Ok(region) = name.parse::<chrono_tz::Tz>() {
        return Some(Some(region.name()));
    }
    if system_v(name).is_some() {
        return Some(None);
    }
    let offset = ["UTC", "GMT", "UT"]
        .iter()
        .find_map(|prefix| name.strip_prefix(prefix))
        .unwrap_or(name);
    iso_offset(offset, true).map(|_| None)
}

/// A field of digits: of exactly two when `exact`, and else of one or two.
fn clock_field(field: &str, exact: bool) -> Option<i64> {
    let wide = if exact {
        field.len() == 2
    } else {
        (1..=2).contains(&field.len())
    };
    if wide && field.bytes().all(|b| b.is_ascii_digit()) {
        field.parse().ok()
    } else {
        None
    }
}

/// The hour, minute, second and nanoseconds of `HH:mm[:ss[.f]]`, the fraction of zero to
/// nine digits; the fields of exactly two digits when `exact` and else of one or two.
/// `None` for other text. The ranges of the fields are the caller's to check.
fn clock(text: &str, exact: bool) -> Option<(i64, i64, i64, i64)> {
    let (whole, fraction) = match text.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (text, None),
    };
    let mut fields = whole.split(':');
    let hour = clock_field(fields.next()?, exact)?;
    let minute = clock_field(fields.next()?, exact)?;
    let second = match fields.next() {
        Some(field) => Some(clock_field(field, exact)?),
        None => None,
    };
    if fields.next().is_some() {
        return None;
    }
    let nanos = match fraction {
        // A fraction follows seconds only.
        Some(digits) => {
            second?;
            if digits.len() > 9 || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let mut nanos = 0_i64;
            for place in 0..9 {
                let digit = digits
                    .as_bytes()
                    .get(place)
                    .map_or(0, |d| i64::from(d - b'0'));
                nanos = nanos * 10 + digit;
            }
            nanos
        }
        None => 0,
    };
    Some((hour, minute, second.unwrap_or(0), nanos))
}

/// The instant of a time literal, and the id of its zone when a column of this server
/// holds that.
///
/// Read only when the text starts with a year of four digits and a `-`: Java reads longer
/// years after a sign, and wraps around for years far from 1970.
fn parse_zoned(content: &str) -> Result<(i64, Option<&'static str>), FormulaError> {
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
    let year = match content.as_bytes().get(..5) {
        Some([digits @ .., b'-']) if digits.iter().all(u8::is_ascii_digit) => {
            content[..4].parse::<i64>().map_err(|_| unsupported())?
        }
        _ => return Err(unsupported()),
    };
    let rest = &content[5..];
    let read = match rest.split_once(' ') {
        // Java takes the zone from the first space on.
        Some((local, name)) => spaced(year, local, name),
        None => iso(year, rest),
    };
    let ((month, day), (hour, minute, second, nanos), placed, id) = read.ok_or_else(invalid)?;

    // Hour 24 is midnight of the next day.
    let local = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    let (year, month, day) = civil_from_days(local.div_euclid(86_400));
    let seconds = match &placed {
        Placed::In(Zone::Utc) => local,
        Placed::Offset(offset) => local - offset,
        Placed::SystemV(standard) => system_v_seconds(*standard, year, local),
        Placed::In(Zone::Region(region)) => {
            let of_day = local.rem_euclid(86_400);
            let time = (of_day / 3_600, of_day % 3_600 / 60, of_day % 60);
            match zoned_seconds(region, (year, month, day), time) {
                Some(seconds) => seconds,
                None => return Err(unsupported()),
            }
        }
    };
    let total = i128::from(seconds) * NANOS_PER_SECOND + i128::from(nanos);
    // The smallest long is the value Java reserves for a null instant.
    match i64::try_from(total) {
        Ok(value) if value != i64::MIN && seconds < i64::MAX / 1_000_000_000 => Ok((value, id)),
        _ => Err(FormulaError::Unsupported(format!(
            "time literal '{content}' outside the range of a 64-bit nanosecond timestamp"
        ))),
    }
}

/// The month and day, the time, the placement and the zone id of a time literal.
type Local = (
    (i64, i64),
    (i64, i64, i64, i64),
    Placed,
    Option<&'static str>,
);

/// A time literal with a space, after its year and the `-`: `local` before the first
/// space and the zone `name` after it. `None` for text Java does not read.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseZonedDateTime` where
/// `java.time.ZonedDateTime#parse` has failed. The zone is one of
/// `io.deephaven.time.TimeZoneAliases#zoneId`: an alias, a region ([`zone`]), an offset
/// ([`zone_offset`]) or one of Java's `SystemV` zones ([`system_v`]). `yyyy-MM-dd` or
/// `yyyy-MM-ddT` and a zone of letters, `_` and `/` is midnight (`DATE_TZ_PATTERN`), and
/// any other text is read with `FORMATTER_ISO_LOCAL_DATE_TIME`: the month, day, hour,
/// minute and second in one or two digits each, `T` in either case, the seconds optional
/// and after them a `.` and zero to nine digits. Both resolve as `ResolverStyle.SMART`
/// does: a day up to 31 that is past the end of its month is the last day of the month,
/// and `24:00` with nothing but zeros after it is midnight of the next day. Seen on Java
/// 42.9: `2023-1-4T2:3:4 UTC`, `2023-11-14t22:13:20. UTC`, `2023-02-30 UTC` (February 28)
/// and `2023-12-31T24:00:00 America/New_York` (2024-01-01T05:00:00Z) are read;
/// `2023-11-14T24:00:01 UTC`, `2023-02-32T22:13:20 UTC`, `2023-11-14T22:13:20 Etc/GMT-15`
/// and `2023-11-14 SystemV/EST5` are not.
fn spaced(year: i64, local: &str, name: &str) -> Option<Local> {
    let (placed, id) = match (zone(name), zone_offset(name), system_v(name)) {
        (Some(zone), _, _) => (Placed::In(zone), Some(zone_id(&zone))),
        (None, Some(offset), _) => (Placed::Offset(offset?), None),
        (None, None, Some((standard, false))) => (Placed::Offset(standard), None),
        (None, None, Some((standard, true))) => (Placed::SystemV(standard), None),
        (None, None, None) => return None,
    };
    let (date, time) = match local.split_once(['T', 't']) {
        Some((date, time)) => (date, time),
        None => (local, ""),
    };
    let (month, day) = date.split_once('-')?;
    let midnight = time.is_empty();
    if midnight {
        let lettered = name
            .bytes()
            .all(|b| b.is_ascii_alphabetic() || b == b'_' || b == b'/');
        if !lettered {
            return None;
        }
    }
    let month = clock_field(month, midnight)?;
    let day = clock_field(day, midnight)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let day = day.min(days_in_month(year, month));
    let (hour, minute, second, nanos) = if midnight {
        (0, 0, 0, 0)
    } else {
        clock(time, false)?
    };
    if minute > 59 || second > 59 || hour > 24 {
        return None;
    }
    if hour == 24 && (minute, second, nanos) != (0, 0, 0) {
        return None;
    }
    Some(((month, day), (hour, minute, second, nanos), placed, id))
}

/// A time literal without a space, after its year and the `-`. `None` for text Java does
/// not read.
///
/// Behavior: `java.time.ZonedDateTime#parse`, which is
/// `DateTimeFormatter.ISO_ZONED_DATE_TIME`: `MM-ddTHH:mm[:ss[.f]]` with fields of two
/// digits and a fraction of zero to nine, `T` in either case, then `Z` in either case or
/// an offset ([`iso_offset`]), then optionally a zone in brackets ([`bracket_zone`]). The
/// date and the time are those of the calendar and the clock (`ResolverStyle.STRICT`), and
/// the instant is the one of the offset whatever the zone in brackets is. An offset of
/// nothing is the zone `Z`. Seen on Java 42.9: `2023-11-14T22:13:20+05:00[Europe/Paris]`
/// is 17:13:20Z, and `2023-11-14t22:13:20.z` and `2023-11-14T22:13+01` are read;
/// `2023-11-14T22+01:00`, `2023-02-29T22:13:20Z`, `2023-11-14T24:00:00Z`,
/// `2023-11-14T22:13:20[Europe/Paris]`, `2023-11-14T22:13:20` and `2023-11-14+01:00` are
/// not.
fn iso(year: i64, rest: &str) -> Option<Local> {
    let (date, after) = rest.split_once(['T', 't'])?;
    let (time, zone) = after.split_at(after.find(['Z', 'z', '+', '-', '['])?);
    let (month, day) = date.split_once('-')?;
    let (month, day) = (clock_field(month, true)?, clock_field(day, true)?);
    let (hour, minute, second, nanos) = clock(time, true)?;
    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let (offset, bracket) = match zone.split_once('[') {
        Some((offset, bracket)) => (offset, Some(bracket.strip_suffix(']')?)),
        None => (zone, None),
    };
    let seconds = match offset {
        "Z" | "z" => 0,
        other => iso_offset(other, false)?,
    };
    let id = match bracket {
        Some(name) => bracket_zone(name)?,
        None if seconds == 0 => Some("Z"),
        None => None,
    };
    Some((
        (month, day),
        (hour, minute, second, nanos),
        Placed::Offset(seconds),
        id,
    ))
}

/// Reads text as `DateTimeUtils#parseInstant` reads a `String` that is not null: an
/// instant spelled as a time literal spells it, or a whole number of seconds,
/// milliseconds, microseconds or nanoseconds since the epoch, told apart by its size. The
/// smallest `long` is the null instant.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseInstant` and `#epochAutoToEpochNanos`.
/// A spelling [`parse_instant`] leaves unsupported is unsupported here when it starts as
/// a date does, with four digits and a `-`, or with a sign; Java reads no other text.
/// Java multiplies the number by its unit without a check, so a number of seconds or
/// milliseconds too large for a `long` of nanoseconds wraps around: seen on Java 42.9,
/// `9309340999` is the instant -9137403074709551616 ns, in 1680.
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
        offset.wrapping_mul(1_000)
    } else if magnitude > THRESHOLD / 1000 {
        offset.wrapping_mul(1_000_000)
    } else {
        offset.wrapping_mul(1_000_000_000)
    })
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

pub(crate) fn days_in_month(year: i64, month: i64) -> i64 {
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

    /// Expected values are what Java 42.9 returned for the literal in a formula
    /// (tasks/4.20-fuzz-findings-2.md, R3).
    #[test]
    fn zones_written_as_offsets_and_regions_without_a_slash_are_read() {
        let base = 1_700_000_000_000_000_000_i64;
        let second = 1_000_000_000_i64;
        let cases = [
            ("+01:00", 1),
            ("+01", 1),
            ("+0100", 1),
            ("+1", 1),
            ("UTC+1", 1),
            ("UTC+01:00", 1),
            ("GMT+1", 1),
            ("UT+1", 1),
            ("CET", 1),
            ("-05:30", 23_401),
            ("UTC-0530", 23_401),
            ("+01:00:30", -29),
            ("+010030", -29),
            ("GMT+01:00:30", -29),
            ("UT+01:30", -1_799),
            ("+18:00", -61_199),
            ("-18:00", 68_401),
            ("-00:00", 3_601),
            ("+00", 3_601),
            ("-0", 3_601),
            ("UTC-00:00", 3_601),
            ("GMT+0", 3_601),
            ("UT", 3_601),
            ("GMT0", 3_601),
            ("Zulu", 3_601),
            ("Greenwich", 3_601),
            ("EST5EDT", 21_601),
            ("Cuba", 21_601),
            ("Japan", -28_799),
            ("Etc/GMT+5", 21_601),
        ];
        for (zone, seconds) in cases {
            let content = format!("2023-11-14T23:13:21 {zone}");
            assert_eq!(value(&content), Ok(base + seconds * second), "{content}");
        }
        // A date alone takes a zone of letters, `_` and `/` only.
        assert_eq!(value("2023-11-14 UT"), Ok(base - 80_000 * second));
        assert_eq!(value("2023-11-14 Japan"), Ok(base - 112_400 * second));
        // A zone id is kept for a region and not for an offset.
        assert_eq!(
            zoned_of_text("2023-11-14T23:13:21 Japan"),
            Ok((base - 28_799 * second, "Japan"))
        );
        assert!(matches!(
            zoned_of_text("2023-11-14T23:13:21 +01:00"),
            Err(FormulaError::Unsupported(_))
        ));
    }

    /// Each of these Java 42.9 refuses in a formula and in a filter, with
    /// `INVALID_ARGUMENT` (tasks/4.20-fuzz-findings-2.md, D4 and R3).
    #[test]
    fn zones_java_does_not_know_and_a_missing_zone_are_syntax_errors() {
        for content in [
            "2023-11-14T22:13:20 NY",
            "2023-11-14T22:13:20 EST",
            "2023-11-14T22:13:20 MST",
            "2023-11-14T22:13:20 HST",
            "2023-11-14T22:13:20 ROC",
            "2023-11-14T22:13:20 et",
            "2023-11-14T22:13:20 utc",
            "2023-11-14T22:13:20 z",
            "2023-11-14T22:13:20 UTCX",
            "2023-11-14T22:13:20 UTC0",
            "2023-11-14T22:13:20 UTC+",
            "2023-11-14T22:13:20 +19:00",
            "2023-11-14T22:13:20 UTC+18:01",
            "2023-11-14T22:13:20 +01:60",
            "2023-11-14T22:13:20 +1:00",
            "2023-11-14T22:13:20 UTC+1:00",
            "2023-11-14T22:13:20",
            "2023-11-14T22:13",
            "2023-11-14",
            "2023-11-14 +01:00",
            "2023-11-14 UTC+1",
            "2023-11-14 GMT0",
            "2023-11-14 EST5EDT",
            "2023-11-14 Etc/GMT+5",
            "2023-11-14 NY",
        ] {
            assert!(
                matches!(value(content), Err(FormulaError::Syntax(_))),
                "{content} gave {:?}",
                value(content)
            );
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

    /// Expected values are those Java 42.9 gave for `update("X = '...'")`.
    #[test]
    fn an_offset_or_a_zone_in_brackets_directly_after_the_time_is_read() {
        let base = 1_700_000_000_000_000_000_i64;
        let hour = 3_600_000_000_000_i64;
        let cases = [
            ("2023-11-14T22:13:20+01:00", base - hour),
            ("2023-11-14T22:13:20-05:00", base + 5 * hour),
            ("2023-11-14T22:13:20+01", base - hour),
            ("2023-11-14T22:13:20+01:00:30", base - hour - 30_000_000_000),
            ("2023-11-14T22:13:20+18:00", base - 18 * hour),
            ("2023-11-14T22:13:20-18:00", base + 18 * hour),
            ("2023-11-14T22:13:20-00:00", base),
            ("2023-11-14T22:13+01:00", base - hour - 20_000_000_000),
            ("2023-11-14T22:13:20.5+01:00", base - hour + 500_000_000),
            (
                "2023-11-14T22:13:20.123456789+01:00",
                base - hour + 123_456_789,
            ),
            ("2023-11-14T22:13:20.+01:00", base - hour),
            ("2023-11-14T22:13:20.Z", base),
            ("2023-11-14T22:13:20z", base),
            ("2023-11-14t22:13:20Z", base),
            ("2024-02-29T22:13:20Z", base + 107 * 24 * hour),
            // The offset places the time, whatever the zone in brackets is.
            ("2023-11-14T22:13:20Z[UTC]", base),
            ("2023-11-14T22:13:20Z[Europe/Paris]", base),
            ("2023-11-14T22:13:20+01:00[Europe/Paris]", base - hour),
            ("2023-11-14T22:13:20+05:00[Europe/Paris]", base - 5 * hour),
            ("2023-11-14T22:13:20+01:00[Z]", base - hour),
            ("2023-11-14T22:13:20+01:00[GMT]", base - hour),
            ("2023-11-14T22:13:20+01:00[UT]", base - hour),
            ("2023-11-14T22:13:20+01:00[EST5EDT]", base - hour),
            ("2023-11-14T22:13:20+01:00[US/Eastern]", base - hour),
            ("2023-11-14T22:13:20+01:00[SystemV/EST5]", base - hour),
            ("2023-11-14T22:13:20+01:00[+02:00]", base - hour),
            ("2023-11-14T22:13:20+01:00[-02:00:30]", base - hour),
            ("2023-11-14T22:13:20+01:00[UTC+02:00]", base - hour),
            ("2023-11-14T22:13:20+01:00[UT-02:00]", base - hour),
            // A time the clocks of the zone skip, and one they show twice.
            (
                "2024-03-10T02:30:00-05:00[America/New_York]",
                1_710_055_800_000_000_000,
            ),
            (
                "2024-11-03T01:30:00-05:00[America/New_York]",
                1_730_615_400_000_000_000,
            ),
            (
                "2024-11-03T01:30:00-04:00[America/New_York]",
                1_730_611_800_000_000_000,
            ),
        ];
        for (content, expected) in cases {
            assert_eq!(value(content), Ok(expected), "{content}");
        }
        for content in [
            "2023-11-14T22:13:20+0100",
            "2023-11-14T22:13:20+1",
            "2023-11-14T22:13:20+18:01",
            "2023-11-14T22:13:20+19:00",
            "2023-11-14T22:13:20-18:00:01",
            "2023-11-14T22:13:20+01:60",
            "2023-11-14T22:13:20+01:",
            "2023-11-14T22:13:20+",
            "2023-11-14T22+01:00",
            "2023-11-14T22:13.5Z",
            "2023-11-14T22:13:20.1234567891+01:00",
            "2023-11-14T22:13:20,5Z",
            "2023-11-14T22:13:20[Europe/Paris]",
            "2023-11-14T22:13:20+01:00[ET]",
            "2023-11-14T22:13:20+01:00[EST]",
            "2023-11-14T22:13:20+01:00[z]",
            "2023-11-14T22:13:20+01:00[europe/paris]",
            "2023-11-14T22:13:20+01:00[Europe/Nowhere]",
            "2023-11-14T22:13:20+01:00[GMT+2]",
            "2023-11-14T22:13:20+01:00[GMT+02]",
            "2023-11-14T22:13:20+01:00[UTC+02]",
            "2023-11-14T22:13:20+01:00[+02]",
            "2023-11-14T22:13:20+01:00[+0200]",
            "2023-11-14T22:13:20+01:00[]",
            "2023-11-14T22:13:20+01:00[Europe/Paris",
            "2023-11-14T22:13:20+01:00[Europe/Paris]x",
            "2023-11-14T22:13:20+01:00 UTC",
            "2023-11-14T22:13:20Z UTC",
            "2023-11-14+01:00",
            "2023-11-14Z",
            "2023-11-14T+01:00",
            // The calendar and the clock are strict here.
            "2023-02-30T22:13:20+01:00",
            "2023-02-29T22:13:20Z",
            "2023-11-14T24:00:00Z",
            "2023-11-14T23:59:60Z",
            "2023-11-14T2:13:20Z",
            "2023-11-14T22:13:2Z",
            "2023-1-14T22:13:20Z",
        ] {
            assert!(
                matches!(value(content), Err(FormulaError::Syntax(_))),
                "{content}"
            );
        }
        assert_eq!(zoned_of_text("2023-11-14T22:13:20+00:00"), Ok((base, "Z")));
        assert_eq!(
            zoned_of_text("2023-11-14T22:13:20+05:00[Europe/Paris]"),
            Ok((base - 5 * hour, "Europe/Paris"))
        );
        assert!(matches!(
            zoned_of_text("2023-11-14T22:13:20+05:00"),
            Err(FormulaError::Unsupported(_))
        ));
    }

    /// Expected values are those Java 42.9 gave for `update("X = '...'")`.
    #[test]
    fn the_system_v_zones_of_java_are_read() {
        let second = 1_000_000_000_i64;
        let at = |year: i64, month: i64, day: i64, hour: i64, minute: i64| {
            (days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60) * second
        };
        let cases = [
            ("2023-11-14T22:13:00 SystemV/EST5", at(2023, 11, 15, 3, 13)),
            ("2023-07-14T22:13:00 SystemV/EST5", at(2023, 7, 15, 3, 13)),
            ("2023-07-14T22:13:00 SystemV/AST4", at(2023, 7, 15, 2, 13)),
            (
                "2023-07-14T22:13:00 SystemV/AST4ADT",
                at(2023, 7, 15, 1, 13),
            ),
            ("2023-07-14T22:13:00 SystemV/CST6", at(2023, 7, 15, 4, 13)),
            (
                "2023-07-14T22:13:00 SystemV/CST6CDT",
                at(2023, 7, 15, 3, 13),
            ),
            ("2023-07-14T22:13:00 SystemV/HST10", at(2023, 7, 15, 8, 13)),
            ("2023-07-14T22:13:00 SystemV/MST7", at(2023, 7, 15, 5, 13)),
            (
                "2023-07-14T22:13:00 SystemV/MST7MDT",
                at(2023, 7, 15, 4, 13),
            ),
            ("2023-07-14T22:13:00 SystemV/PST8", at(2023, 7, 15, 6, 13)),
            (
                "2023-07-14T22:13:00 SystemV/PST8PDT",
                at(2023, 7, 15, 5, 13),
            ),
            ("2023-07-14T22:13:00 SystemV/YST9", at(2023, 7, 15, 7, 13)),
            (
                "2023-07-14T22:13:00 SystemV/YST9YDT",
                at(2023, 7, 15, 6, 13),
            ),
            // Daylight time from the last Sunday of April to the last of October.
            (
                "2023-03-20T12:00:00 SystemV/EST5EDT",
                at(2023, 3, 20, 17, 0),
            ),
            (
                "2023-04-29T12:00:00 SystemV/EST5EDT",
                at(2023, 4, 29, 17, 0),
            ),
            (
                "2023-04-30T01:59:00 SystemV/EST5EDT",
                at(2023, 4, 30, 6, 59),
            ),
            ("2023-04-30T02:00:00 SystemV/EST5EDT", at(2023, 4, 30, 7, 0)),
            (
                "2023-04-30T02:30:00 SystemV/EST5EDT",
                at(2023, 4, 30, 7, 30),
            ),
            ("2023-04-30T03:00:00 SystemV/EST5EDT", at(2023, 4, 30, 7, 0)),
            (
                "2023-10-29T00:59:00 SystemV/EST5EDT",
                at(2023, 10, 29, 4, 59),
            ),
            (
                "2023-10-29T01:00:00 SystemV/EST5EDT",
                at(2023, 10, 29, 5, 0),
            ),
            (
                "2023-10-29T01:30:00 SystemV/EST5EDT",
                at(2023, 10, 29, 5, 30),
            ),
            (
                "2023-10-29T02:00:00 SystemV/EST5EDT",
                at(2023, 10, 29, 7, 0),
            ),
            (
                "2023-11-01T12:00:00 SystemV/EST5EDT",
                at(2023, 11, 1, 17, 0),
            ),
            // The years with days of their own, and those before them.
            (
                "1976-04-25T12:00:00 SystemV/PST8PDT",
                at(1976, 4, 25, 19, 0),
            ),
            (
                "1976-04-24T12:00:00 SystemV/PST8PDT",
                at(1976, 4, 24, 20, 0),
            ),
            ("1975-03-01T12:00:00 SystemV/PST8PDT", at(1975, 3, 1, 19, 0)),
            (
                "1975-02-22T12:00:00 SystemV/PST8PDT",
                at(1975, 2, 22, 20, 0),
            ),
            (
                "1974-01-10T12:00:00 SystemV/PST8PDT",
                at(1974, 1, 10, 19, 0),
            ),
            ("1974-01-05T12:00:00 SystemV/PST8PDT", at(1974, 1, 5, 20, 0)),
            (
                "1974-11-20T12:00:00 SystemV/PST8PDT",
                at(1974, 11, 20, 19, 0),
            ),
            (
                "1974-11-25T12:00:00 SystemV/PST8PDT",
                at(1974, 11, 25, 20, 0),
            ),
            (
                "1973-04-28T12:00:00 SystemV/PST8PDT",
                at(1973, 4, 28, 20, 0),
            ),
            (
                "1973-04-30T12:00:00 SystemV/PST8PDT",
                at(1973, 4, 30, 19, 0),
            ),
            ("1900-07-01T12:00:00 SystemV/PST8PDT", at(1900, 7, 1, 19, 0)),
            ("1900-01-01T12:00:00 SystemV/PST8PDT", at(1900, 1, 1, 20, 0)),
        ];
        for (content, expected) in cases {
            assert_eq!(value(content), Ok(expected), "{content}");
        }
        for content in [
            "2023-07-14T22:13:20 SystemV/EST",
            "2023-11-14T22:13:20 SystemV/EST5 ",
            "2023-11-14 SystemV/EST5",
            "2023-11-14T SystemV/PST8",
            "2023-11-14T22:13:20 America/Nowhere",
            "2023-11-14T22:13:20 Europe/paris",
            "2023-11-14T22:13:20 Etc/GMT-15",
            "2023-11-14T22:13:20 Etc/Unknown",
            "2023-11-14T22:13:20 A/B",
            "2023-11-14T22:13:20 /",
            "2023-11-14T22:13:20 Europe/Paris/",
        ] {
            assert!(
                matches!(value(content), Err(FormulaError::Syntax(_))),
                "{content}"
            );
        }
        assert!(matches!(
            zoned_of_text("2023-11-14T22:13:20 SystemV/EST5"),
            Err(FormulaError::Unsupported(_))
        ));
    }

    /// Expected values are those Java 42.9 gave for `update("X = '...'")`.
    #[test]
    fn a_date_and_time_before_a_zone_are_read_as_leniently_as_java_reads_them() {
        let second = 1_000_000_000_i64;
        let at = |month: i64, day: i64, hour: i64, minute: i64, seconds: i64| {
            (days_from_civil(2023, month, day) * 86_400 + hour * 3_600 + minute * 60 + seconds)
                * second
        };
        let cases = [
            ("2023-1-4T2:3:4 UTC", at(1, 4, 2, 3, 4)),
            ("2023-11-14T2:13:20 UTC", at(11, 14, 2, 13, 20)),
            ("2023-11-14T22:3 UTC", at(11, 14, 22, 3, 0)),
            ("2023-11-14T22:13:2 UTC", at(11, 14, 22, 13, 2)),
            ("2023-11-14T22:13:20. UTC", at(11, 14, 22, 13, 20)),
            (
                "2023-11-14T22:13:2.5 UTC",
                at(11, 14, 22, 13, 2) + second / 2,
            ),
            ("2023-11-14t22:13:20 UTC", at(11, 14, 22, 13, 20)),
            ("2023-11-14t UTC", at(11, 14, 0, 0, 0)),
            // Hour 24 is midnight of the next day.
            ("2023-11-14T24:00:00 UTC", at(11, 15, 0, 0, 0)),
            ("2023-11-14T24:00:00.0 UTC", at(11, 15, 0, 0, 0)),
            ("2023-11-14T24:00 UTC", at(11, 15, 0, 0, 0)),
            ("2023-12-31T24:00:00 America/New_York", at(12, 32, 5, 0, 0)),
            // A day past the end of its month is the last day of the month.
            ("2023-02-30T22:13:20 UTC", at(2, 28, 22, 13, 20)),
            ("2023-02-31T22:13:20 UTC", at(2, 28, 22, 13, 20)),
            ("2023-04-31T22:13:20 UTC", at(4, 30, 22, 13, 20)),
            ("2023-02-30 UTC", at(2, 28, 0, 0, 0)),
            ("2023-02-30T UTC", at(2, 28, 0, 0, 0)),
        ];
        for (content, expected) in cases {
            assert_eq!(value(content), Ok(expected), "{content}");
        }
        assert_eq!(
            value("2024-02-30T22:13:20 UTC"),
            Ok((days_from_civil(2024, 2, 29) * 86_400 + 22 * 3_600 + 13 * 60 + 20) * second)
        );
        for content in [
            "2023-11-14T24:00:01 UTC",
            "2023-11-14T24:01 UTC",
            "2023-11-14T24:00:00.1 UTC",
            "2023-02-32T22:13:20 UTC",
            "2023-00-14T22:13:20 UTC",
            "2023-11-00T22:13:20 UTC",
            "2023-11-14T22:60:20 UTC",
            "2023-11-14T22:13:20.1234567890 UTC",
            "2023-11-14T22:13:20,5 UTC",
            "2023-1-4 UTC",
            "2023-11-14T22:13:20  UTC",
            "2023-11-14T22:13:20 UTC ",
        ] {
            assert!(
                matches!(value(content), Err(FormulaError::Syntax(_))),
                "{content}"
            );
        }
    }

    #[test]
    fn other_time_literals_are_named() {
        for content in [
            // Java reads a year of more digits after a sign only, and one before the
            // year 1 as an instant far in the future.
            "+12023-11-14T22:13:20 UTC",
            "12023-11-14T22:13:20Z",
            "-2023-11-14T22:13:20Z",
            "1700000000",
            "12:30",
            "UTC",
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

    /// Expected values seen on Java 42.9, as `epochNanos(parseInstant(...))`.
    #[test]
    fn whole_numbers_too_large_for_their_unit_wrap_as_java_multiplies() {
        for (text, nanos) in [
            ("9309340999", -9_137_403_074_709_551_616),
            ("-9309340999", 9_137_403_074_709_551_616),
            ("9309340999999", -9_137_403_073_710_551_616),
            ("9309340", 9_309_340_000_000_000),
            ("-9223372036854775808", i64::MIN),
        ] {
            assert_eq!(instant_of_text(text), Ok(nanos), "{text}");
            // A time literal is read as `parseInstantQuiet` reads it, which is null for
            // the smallest long.
            match parse(text) {
                Ok(TimeValue::Instant(read)) => assert_eq!(read, nanos, "'{text}'"),
                Err(FormulaError::Syntax(_)) if nanos == i64::MIN => {}
                other => panic!("'{text}' gave {other:?}"),
            }
        }
    }

    /// Every parser of time text answers, without an arithmetic overflow, for numbers at
    /// and past the limits of each type in each place a number can stand.
    #[test]
    fn no_parser_overflows_on_large_numbers() {
        let mut numbers: Vec<String> = Vec::new();
        for digits in 1..=40 {
            numbers.push("9".repeat(digits));
            numbers.push(format!("1{}", "0".repeat(digits - 1)));
        }
        // The largest numbers `epochAutoToEpochNanos` reads as seconds, milliseconds and
        // microseconds, and the limits of the types the parsers hold numbers in.
        let units = [9_309_341_000, 9_309_341_000_000, 9_309_341_000_000_000];
        for limit in units
            .into_iter()
            .chain([i64::MAX as i128, i32::MAX as i128, u64::MAX as i128])
        {
            for delta in [-1, 0, 1] {
                numbers.push((limit + delta).to_string());
            }
        }
        let shapes = [
            "{}",
            "-{}",
            "+{}",
            "P{}D",
            "P-{}D",
            "P{}Y{}M{}W{}D",
            "-P{}W-{}D",
            "PT{}H{}M{}S",
            "P{}DT{}H-{}M{}.{}S",
            "PT-{}.{}S",
            "PT{}:{}:{}",
            "{}-{}-{}",
            "+{}-{}-{}",
            "-{}-{}-{}",
            "{}:{}:{}.{}",
            "{}:{}",
            "2023-{}-{}T{}:{}:{} UTC",
            "2023-11-14T{}:{}:{}.{}Z",
            "2023-11-14T22:13:20+{}",
            "2023-11-14T22:13:20 UTC+{}",
            "2023-11-14T22:13:20 GMT-{}",
            "2023-11-14T22:13:20[UTC+{}]",
        ];
        for number in &numbers {
            for shape in shapes {
                let text = shape.replace("{}", number);
                let _ = instant_of_text(&text);
                let _ = parse(&text);
                let _ = zoned_of_text(&text);
                let _ = local_date_days(&text);
                let _ = local_time_nanos(&text);
                let _ = period_parts(&text);
                let _ = period_days(&text);
                let _ = duration_nanos(&text);
                let _ = zone_offset(&text);
                let _ = time_literals(&format!("'{text}'"));
            }
        }
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
