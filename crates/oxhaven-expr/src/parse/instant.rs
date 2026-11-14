//! Time literals: single-quoted text that Java replaces before it parses the formula.
//!
//! Only literals that denote an instant in UTC are read. Java accepts many more spellings
//! (other time zones, epoch numbers, durations, periods, local dates and times, time zone
//! names); those are reported as unsupported, by their text.

use oxhaven_contracts::expr::FormulaError;

/// A single-quoted span of the formula that Java treats as a time literal.
pub(crate) struct TimeLiteral {
    /// Byte offset of the opening quote.
    pub(crate) start: usize,
    /// Byte offset just past the closing quote.
    pub(crate) end: usize,
    /// Nanoseconds since the epoch, or why the literal is not accepted.
    pub(crate) value: Result<i64, FormulaError>,
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

/// Reads the content of a time literal as an instant.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseInstant` and `#parseZonedDateTime`, for
/// these spellings only: `yyyy-MM-ddTHH:mm[:ss[.f]] UTC`, the same with `Z` directly after
/// the time in place of ` UTC` (`java.time.ZonedDateTime#parse`), and `yyyy-MM-dd UTC` or
/// `yyyy-MM-ddT UTC`, which is midnight. Fields have exactly the widths shown and `f` is one
/// to nine digits. `Z` after a space is not a time zone to
/// `io.deephaven.time.TimeZoneAliases#zoneId`, so Java rejects it.
///
/// An instant that Java cannot store in a column is unsupported: Java can still compare
/// with it. `DateTimeUtils#epochNanos` refuses every instant from epoch second
/// `Long.MAX_VALUE / 1_000_000_000` on, which is the last 0.85 seconds a long of
/// nanoseconds could hold.
fn parse(content: &str) -> Result<i64, FormulaError> {
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
    let (hour, minute, second, nanos) = if matches!(rest, " UTC" | "T UTC") {
        (0, 0, 0, 0)
    } else {
        let Some(time) = rest
            .strip_prefix('T')
            .and_then(|time| time.strip_suffix(" UTC").or_else(|| time.strip_suffix('Z')))
        else {
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

    let seconds = days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second;
    let total = i128::from(seconds) * NANOS_PER_SECOND + i128::from(nanos);
    // The smallest long is the value Java reserves for a null instant.
    match i64::try_from(total) {
        Ok(value) if value != i64::MIN && seconds < i64::MAX / 1_000_000_000 => Ok(value),
        _ => Err(FormulaError::Unsupported(format!(
            "time literal '{content}' outside the range of a 64-bit nanosecond timestamp"
        ))),
    }
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
        parse(content)
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

    #[test]
    fn other_time_literals_are_named() {
        for content in [
            "2023-11-14T22:13:20 NY",
            "2023-11-14T22:13:20 America/New_York",
            "2023-11-14T22:13:20+01:00",
            "2023-11-14",
            "2023-11-14T22:13:20",
            "2023-1-4T22:13:20 UTC",
            "2023-11-14T2:13:20 UTC",
            "2023-11-14T22:13:20. UTC",
            "2023-11-14T22:13:20.1234567890 UTC",
            "2023-11-14t22:13:20 UTC",
            "1700000000",
            "PT1H",
            "P1D",
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
