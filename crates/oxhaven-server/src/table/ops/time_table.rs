//! `TimeTable`: a refreshing table that gains a row every period.

use super::prelude::*;
use oxhaven_contracts::unported;
use oxhaven_contracts::expr::FormulaError;
use oxhaven_engine::table::{time_table, TimeTableOptions};
use oxhaven_expr::{parse_instant_reach, InstantReach};
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
        Some(StartTime::StartTimeString(text)) => start_of_text(text),
        Some(StartTime::StartTimeNanos(nanos)) if *nanos > 0 => Ok(Some(*nanos)),
        _ => Ok(None),
    }
}

/// A start that no clock of this server reaches: the last nanosecond a clock reads.
///
/// Behavior: `io.deephaven.engine.table.impl.TimeTable#refresh`, which compares the clock
/// with the start as `java.time.Instant`s: a start later than every instant a `long` of
/// nanoseconds holds (`+10000-01-01T00:00:00Z`, `2300-01-01T00:00:00 ET`) is never
/// reached, and the table never gains a row. Seen on Java 42.9.
const NEVER: i64 = i64::MAX;

/// A start time written as text: a date and time with an offset or a zone, a date and a
/// zone, or a whole number since the epoch, read as the expression crate reads instants
/// (with Deephaven's time zone aliases and the time zone database).
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseInstant`; a whole number that is the
/// smallest `long` is the null instant (`DateTimeUtils#epochNanosToInstant`), which is no
/// start: seen on Java 42.9, `-9223372036854775808` makes a table that starts empty.
fn start_of_text(text: &str) -> Result<Option<i64>, String> {
    match parse_instant_reach(text) {
        Ok(InstantReach::Within(i64::MIN)) => Ok(None),
        Ok(InstantReach::Within(nanos)) => Ok(Some(nanos)),
        Ok(InstantReach::After) => Ok(Some(NEVER)),
        Ok(InstantReach::Before) => unported!(
            "io.deephaven.time.DateTimeUtils#epochNanos",
            "the start time {text}, before the instants a long of nanoseconds holds, \
             which Deephaven converts with a wrap-around"
        ),
        Err(FormulaError::Unsupported(what)) => unported!(
            "io.deephaven.time.DateTimeUtils#parseInstant",
            "the start time {text} ({what})"
        ),
        Err(error) => Err(format!("Cannot parse instant: {text}: {error}")),
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

    fn start(text: &str) -> Result<Option<i64>, String> {
        start_of_text(text)
    }

    /// Each start and the timestamp of row 0 of `session.time_table(10**18, start)`, seen
    /// on Deephaven 42.9.
    fn served_by_deephaven() -> Vec<(&'static str, i64)> {
        vec![
            ("2024-01-01T00:00:00Z", JAN_2024),
            ("2024-01-01T00:00Z", JAN_2024),
            ("2024-01-01T01:30:00+01:30", JAN_2024),
            ("2024-01-01T00:00:00 UTC", JAN_2024),
            ("2024-01-01 UTC", JAN_2024),
            ("2024-01-01T UTC", JAN_2024),
            ("2024-1-1T0:0 UTC", JAN_2024),
            ("1704067200", JAN_2024),
            // Z3: named zones, Deephaven's aliases and the time zone database
            ("2024-01-01T00:00:00 America/New_York", 1_704_085_200_000_000_000),
            ("2024-01-01T00:00:00 ET", 1_704_085_200_000_000_000),
            ("2024-01-01 America/New_York", 1_704_085_200_000_000_000),
            ("2024-01-01 ET", 1_704_085_200_000_000_000),
            ("2024-01-01T America/New_York", 1_704_085_200_000_000_000),
            ("2024-1-1T0:0 ET", 1_704_085_200_000_000_000),
            ("2024-01-01T00:00:00 US/Eastern", 1_704_085_200_000_000_000),
            ("2024-01-01T00:00:00 Europe/Paris", 1_704_063_600_000_000_000),
            ("2024-01-01T00:00:00 JP", 1_704_034_800_000_000_000),
            ("2024-01-01T00:00:00 Asia/Kolkata", 1_704_047_400_000_000_000),
            ("2024-01-01T00:00:00 +05:30", 1_704_047_400_000_000_000),
            ("2024-01-01T00:00:00 UTC+1", 1_704_063_600_000_000_000),
            ("2024-01-01T00:00:00 GMT", JAN_2024),
            ("2024-01-01T00:00:00 Etc/GMT-5", 1_704_049_200_000_000_000),
            ("2024-01-01T00:00:00 SystemV/EST5EDT", 1_704_085_200_000_000_000),
            ("2024-07-01T00:00:00 SystemV/EST5EDT", 1_719_806_400_000_000_000),
            ("2024-02-30T12:00 PT", 1_709_236_800_000_000_000),
            ("2024-01-01T00:00:00.123456789 CT", 1_704_088_800_123_456_789),
            ("2024-01-01T00:00:00 Australia/Lord_Howe", 1_704_027_600_000_000_000),
            // a local time the clocks skip is moved later by the gap ...
            ("2024-03-10T02:30:00 America/New_York", 1_710_055_800_000_000_000),
            ("2024-03-10T02:00:00 America/New_York", 1_710_054_000_000_000_000),
            ("2024-03-10T03:00:00 America/New_York", 1_710_054_000_000_000_000),
            ("2024-03-31T02:30:00 Europe/London", 1_711_848_600_000_000_000),
            ("2024-03-31T02:30:00 LON", 1_711_848_600_000_000_000),
            ("2024-10-06T02:15:00 Australia/Lord_Howe", 1_728_143_100_000_000_000),
            // ... and one that occurs twice is the earlier
            ("2024-11-03T01:30:00 America/New_York", 1_730_611_800_000_000_000),
            ("2024-11-03T01:00:00 ET", 1_730_610_000_000_000_000),
            ("2024-11-03T02:00:00 ET", 1_730_617_200_000_000_000),
            ("2024-10-27T01:30:00 LON", 1_729_989_000_000_000_000),
            ("2024-04-07T01:45:00 Australia/Lord_Howe", 1_712_414_700_000_000_000),
            // Z2: hour 24 is the next midnight, after a space only
            ("2024-01-01T24:00:00 UTC", 1_704_153_600_000_000_000),
            ("2024-01-01T24:00:00.000 UTC", 1_704_153_600_000_000_000),
            ("2023-12-31T24:00 America/New_York", 1_704_085_200_000_000_000),
            ("2024-03-09T24:00 ET", 1_710_046_800_000_000_000),
            // Z4: with an offset and a zone in brackets, the offset decides
            ("2024-01-01T00:00:00+05:00[Europe/Paris]", 1_704_049_200_000_000_000),
            ("2024-01-01T00:00:00Z[America/New_York]", JAN_2024),
            ("2024-01-01T00:00:00-05:00[America/New_York]", 1_704_085_200_000_000_000),
            ("2024-01-01T00:00:00+05:00[UTC]", 1_704_049_200_000_000_000),
            ("2024-01-01T00:00:00+05:00[+03:00]", 1_704_049_200_000_000_000),
            ("2024-03-10T02:30:00-05:00[America/New_York]", 1_710_055_800_000_000_000),
            ("2024-03-10T02:30:00-04:00[America/New_York]", 1_710_052_200_000_000_000),
            ("2024-11-03T01:30:00-05:00[America/New_York]", 1_730_615_400_000_000_000),
            ("2024-11-03T01:30:00-04:00[America/New_York]", 1_730_611_800_000_000_000),
        ]
    }

    #[test]
    fn start_times_are_read_as_deephaven_reads_them() {
        for (text, nanos) in served_by_deephaven() {
            assert_eq!(start(text), Ok(Some(nanos)), "{text}");
        }
    }

    /// Each of these made Deephaven 42.9 answer `INVALID_ARGUMENT`.
    #[test]
    fn start_times_deephaven_refuses_are_errors() {
        for text in [
            "nope",
            "2024-01-01T00:00:00",
            "2023-02-29T00:00:00Z",
            "2024-01-01T00:00:00 NY",
            "2024-01-01T00:00:00 EST",
            "2024-01-01T00:00:00 america/new_york",
            "2024-01-01T00:00:00 Z",
            "2024-01-01T24:00:00Z",
            "2024-01-01T24:00:00+00:00",
            "2024-01-01T24:00:01 UTC",
            "2024-01-01T00:00:00+01:00[ET]",
            "+2024-01-01T00:00:00Z",
            "+2024-01-01T00:00:00 UTC",
            "-0000-01-01T00:00:00Z",
            "-0000-01-01T00:00:00 UTC",
            "+10000-01-01 UTC",
            "10000-01-01T00:00:00Z",
            "+1000000000-01-01T00:00:00Z",
            "-1000000000-01-01T00:00:00Z",
            "+10001-02-29T00:00:00Z",
            "+10000-13-01T00:00:00Z",
            "+10000-01-01T00:00:00 Nowhere",
        ] {
            let error = start(text).unwrap_err();
            assert!(error.starts_with(&format!("Cannot parse instant: {text}")), "{error}");
        }
    }

    /// Z1: Deephaven 42.9 serves each of these as a table with no rows, which a clock of
    /// nanoseconds never reaches.
    #[test]
    fn a_start_after_every_instant_of_a_clock_is_never_reached() {
        for text in [
            "+10000-01-01T00:00:00Z",
            "+10000-01-01T00:00:00 UTC",
            "+10000-1-1T0:0 UTC",
            "+10000-01-01t00:00:00z",
            "+10000-01-01T24:00 UTC",
            "+10000-02-29T00:00:00Z",
            "+10001-02-29T00:00:00 UTC",
            "+10000-01-01T00:00:00 America/New_York",
            "+1000000-07-01T00:00:00 America/New_York",
            "+10000-01-01T00:00:00+05:00[Europe/Paris]",
            "+10000-01-01T00:00:00 +05:00",
            "+12024-01-01T00:00:00Z",
            "+99999-01-01T00:00:00Z",
            "+010000-01-01T00:00:00Z",
            "+0999999999-01-01T00:00:00Z",
            "+999999999-12-31T23:59:59Z",
            "2300-01-01T00:00:00Z",
            "2300-01-01T00:00:00 ET",
            "2262-04-11T23:47:16Z",
            "2262-04-11T23:47:17Z",
            "2262-04-11T23:47:16.854775807Z",
        ] {
            assert_eq!(start(text), Ok(Some(NEVER)), "{text}");
        }
    }

    /// The smallest long is Java's null instant: no start, as seen on Deephaven 42.9.
    #[test]
    fn the_null_instant_is_no_start() {
        assert_eq!(start("-9223372036854775808"), Ok(None));
        assert_eq!(start("-9223372036854775807"), Ok(Some(-9_223_372_036_854_775_807)));
    }

    /// Deephaven 42.9 wraps these around: `-0001-01-01T00:00:00Z` gave nine rows from
    /// -6858522978871345152, `1600-01-01T00:00:00Z` and `-2024-01-01T00:00:00Z` none.
    #[test]
    fn a_start_before_every_instant_of_a_clock_is_unported() {
        for text in [
            "-0001-01-01T00:00:00Z",
            "-2024-01-01T00:00:00Z",
            "-2024-01-01T00:00:00 UTC",
            "+00001-01-01T00:00:00Z",
            "-999999999-01-01T00:00:00Z",
            "1600-01-01T00:00:00Z",
            "1677-09-21T00:12:43Z",
        ] {
            let refused = std::panic::catch_unwind(|| start(text)).unwrap_err();
            let message = refused.downcast_ref::<String>().unwrap();
            assert!(
                message.starts_with("unported: io.deephaven.time.DateTimeUtils#epochNanos"),
                "{text}: {message}"
            );
        }
        // the first second Java converts without a wrap-around
        assert_eq!(
            start("1677-09-21T00:12:44Z"),
            Ok(Some(-9_223_372_036_000_000_000))
        );
    }

    #[test]
    fn whole_numbers_are_epoch_offsets_by_size() {
        assert_eq!(start("1704067200"), Ok(Some(JAN_2024)), "seconds");
        assert_eq!(start("1704067200000"), Ok(Some(JAN_2024)), "millis");
        assert_eq!(start("1704067200000000"), Ok(Some(JAN_2024)), "micros");
        assert_eq!(start("1704067200000000000"), Ok(Some(JAN_2024)), "nanos");
        assert_eq!(start("-5"), Ok(Some(-5_000_000_000)));
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
