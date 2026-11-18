//! Business calendars: which dates are business days, and when on each the business is
//! open.
//!
//! A [`BusinessCalendar`] is read from Deephaven's calendar file format ([`parse_calendar`])
//! and kept by name in the process's collection of calendars ([`Calendars`]). Dates are
//! days since 1970-01-01, times of day nanoseconds since midnight, and instants
//! nanoseconds since the epoch.
//!
//! Java caches a summary per month and year of each calendar and adds the summaries up.
//! The answers are those of adding up every day, which is what is done here: the cache
//! changes only which invalid date an error names first.

mod registry;
#[cfg(test)]
mod tests;
mod xml;
mod zone;

use std::collections::BTreeMap;
use std::fmt;

pub use registry::{
    calendars, Calendars, CalendarsGuard, DEFAULT_CALENDAR_NAME, SHIPPED_CALENDAR_NAMES,
};
pub use xml::{parse_calendar, read_calendar_file};
pub use zone::CalendarZone;

use crate::parse::instant::{civil_from_days, days_from_civil, local_date_days};

pub(crate) const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// What a calendar refuses: an exception Java throws, or what this server does not do.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CalendarError {
    /// The exception Java throws: its class and its message.
    Java { class: String, message: String },
    /// Not ported: the Java method, and what of it.
    Unported { method: String, what: String },
}

impl CalendarError {
    pub(crate) fn java(class: &str, message: impl Into<String>) -> Self {
        Self::Java {
            class: class.to_string(),
            message: message.into(),
        }
    }

    pub(crate) fn unported(method: &str, what: impl Into<String>) -> Self {
        Self::Unported {
            method: method.to_string(),
            what: what.into(),
        }
    }

    fn invalid_date(message: String) -> Self {
        Self::java(
            "io.deephaven.time.calendar.BusinessCalendar$InvalidDateException",
            message,
        )
    }

    fn illegal_argument(message: impl Into<String>) -> Self {
        Self::java("java.lang.IllegalArgumentException", message)
    }
}

impl fmt::Display for CalendarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Java { class, message } if class.is_empty() => write!(f, "{message}"),
            Self::Java { class, message } => write!(f, "{class}: {message}"),
            Self::Unported { method, what } if what.is_empty() => {
                write!(f, "not ported: {method}")
            }
            Self::Unported { method, what } => write!(f, "not ported: {method}: {what}"),
        }
    }
}

impl std::error::Error for CalendarError {}

/// A date as `java.time.LocalDate#toString` writes it.
pub fn date_text(day: i64) -> String {
    let (year, month, date) = civil_from_days(day);
    let year = if year.abs() < 1000 {
        if year < 0 {
            format!("-{:04}", -year)
        } else {
            format!("{year:04}")
        }
    } else if year > 9999 {
        format!("+{year}")
    } else {
        year.to_string()
    };
    format!("{year}-{month:02}-{date:02}")
}

/// A time of day as `java.time.LocalTime#toString` writes it.
fn time_text(nano_of_day: i64) -> String {
    let seconds = nano_of_day / 1_000_000_000;
    let nano = nano_of_day % 1_000_000_000;
    let mut text = format!("{:02}:{:02}", seconds / 3_600, seconds / 60 % 60);
    if seconds % 60 != 0 || nano != 0 {
        text.push_str(&format!(":{:02}", seconds % 60));
    }
    if nano != 0 {
        text.push_str(&if nano % 1_000_000 == 0 {
            format!(".{:03}", nano / 1_000_000)
        } else if nano % 1_000 == 0 {
            format!(".{:06}", nano / 1_000)
        } else {
            format!(".{nano:09}")
        });
    }
    text
}

/// An instant as `java.time.Instant#toString` writes it.
fn instant_text(nanos: i64) -> String {
    let day = nanos.div_euclid(NANOS_PER_DAY);
    let time = nanos.rem_euclid(NANOS_PER_DAY);
    let mut clock = time_text(time);
    if clock.len() == 5 {
        clock.push_str(":00");
    }
    format!("{}T{clock}Z", date_text(day))
}

/// Reads a date as `DateTimeUtils#parseLocalDate` does.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalDate`
pub fn parse_date(text: &str) -> Result<i64, CalendarError> {
    match local_date_days(text) {
        Some(Ok(day)) => Ok(day),
        Some(Err(error)) => Err(CalendarError::unported(
            "io.deephaven.time.DateTimeUtils#parseLocalDate",
            error.to_string(),
        )),
        None => Err(CalendarError::java(
            "io.deephaven.time.DateTimeUtils$DateTimeParseException",
            format!("Cannot parse local date: {text}"),
        )),
    }
}

/// Monday is 1 and Sunday 7, as `java.time.DayOfWeek#getValue` numbers them.
pub fn day_of_week(day: i64) -> u8 {
    // 1970-01-01 was a Thursday
    ((day + 3).rem_euclid(7) + 1) as u8
}

/// The names of the days of the week, as `java.time.DayOfWeek` spells them, Monday first.
pub const DAY_NAMES: [&str; 7] = [
    "MONDAY",
    "TUESDAY",
    "WEDNESDAY",
    "THURSDAY",
    "FRIDAY",
    "SATURDAY",
    "SUNDAY",
];

/// What the times of a [`CalendarDay`] are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeKind {
    /// Nanoseconds since midnight: a `java.time.LocalTime`.
    LocalTime,
    /// Nanoseconds since the epoch: a `java.time.Instant`.
    Instant,
}

impl TimeKind {
    fn text(self, value: i64) -> String {
        match self {
            Self::LocalTime => time_text(value),
            Self::Instant => instant_text(value),
        }
    }
}

/// A stretch of business time: from `start` to `end`, the end included or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
    pub inclusive_end: bool,
}

impl TimeRange {
    /// Behavior: `io.deephaven.time.calendar.TimeRange#TimeRange`, which refuses a range
    /// that does not start before it ends.
    pub fn new(
        start: i64,
        end: i64,
        inclusive_end: bool,
        kind: TimeKind,
    ) -> Result<Self, CalendarError> {
        let texts = || format!("startTime={} endTime={}", kind.text(start), kind.text(end));
        if start > end {
            return Err(CalendarError::illegal_argument(format!(
                "Start is after end: {}",
                texts()
            )));
        }
        if start == end {
            return Err(CalendarError::illegal_argument(format!(
                "Start is the same as end: {}",
                texts()
            )));
        }
        Ok(Self {
            start,
            end,
            inclusive_end,
        })
    }

    /// Behavior: `io.deephaven.time.calendar.TimeRange#nanos`
    pub fn nanos(&self) -> i64 {
        self.end - self.start + i64::from(self.inclusive_end)
    }

    /// Behavior: `io.deephaven.time.calendar.TimeRange#contains`
    pub fn contains(&self, time: i64) -> bool {
        self.start <= time
            && if self.inclusive_end {
                time <= self.end
            } else {
                time < self.end
            }
    }
}

/// The business time of one day, in ranges that do not overlap, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CalendarDay {
    ranges: Vec<TimeRange>,
    nanos: i64,
}

impl CalendarDay {
    /// A day without business time.
    pub fn holiday() -> Self {
        Self {
            ranges: Vec::new(),
            nanos: 0,
        }
    }

    /// Behavior: `io.deephaven.time.calendar.CalendarDay#CalendarDay`: the ranges sorted by
    /// their start, and refused when one starts before the previous ends, or where it ends
    /// when that end is included.
    pub fn new(mut ranges: Vec<TimeRange>) -> Result<Self, CalendarError> {
        ranges.sort_by_key(|range| range.start);
        if ranges.windows(2).any(|pair| {
            pair[1].start < pair[0].end || (pair[1].start == pair[0].end && pair[0].inclusive_end)
        }) {
            return Err(CalendarError::illegal_argument(
                "Business time ranges overlap.",
            ));
        }
        let nanos = ranges.iter().map(TimeRange::nanos).sum();
        Ok(Self { ranges, nanos })
    }

    pub fn ranges(&self) -> &[TimeRange] {
        &self.ranges
    }

    /// Behavior: `io.deephaven.time.calendar.CalendarDay#businessNanos`
    pub fn business_nanos(&self) -> i64 {
        self.nanos
    }

    /// Behavior: `io.deephaven.time.calendar.CalendarDay#isBusinessDay`
    pub fn is_business_day(&self) -> bool {
        self.nanos > 0
    }

    /// Behavior: `io.deephaven.time.calendar.CalendarDay#isBusinessTime`
    pub fn is_business_time(&self, time: i64) -> bool {
        self.ranges.iter().any(|range| range.contains(time))
    }

    /// The business time before `time`. A time at the end of a range whose end is included
    /// is not counted, as in Java.
    ///
    /// Behavior: `io.deephaven.time.calendar.CalendarDay#businessNanosElapsed`
    pub fn business_nanos_elapsed(&self, time: i64) -> i64 {
        let mut elapsed = 0;
        for range in &self.ranges {
            if time < range.start {
                break;
            }
            if time > range.end {
                elapsed += range.nanos();
            } else {
                elapsed += time - range.start;
                break;
            }
        }
        elapsed
    }

    /// Behavior: `io.deephaven.time.calendar.CalendarDay#businessNanosRemaining`
    pub fn business_nanos_remaining(&self, time: i64) -> i64 {
        self.nanos - self.business_nanos_elapsed(time)
    }

    /// The day of a schedule of times of day, on a date of a zone.
    ///
    /// Behavior: `io.deephaven.time.calendar.CalendarDay#toInstant`,
    /// `io.deephaven.time.calendar.TimeRange#toInstant`: each time is
    /// `DateTimeUtils#toInstant(LocalDate, LocalTime, ZoneId)`, and the ranges are checked
    /// again, so a range that the clocks change in can fail.
    pub fn on_date(&self, day: i64, zone: &CalendarZone) -> Result<Self, CalendarError> {
        let ranges = self
            .ranges
            .iter()
            .map(|range| {
                TimeRange::new(
                    zone.instant_of(day, range.start)?,
                    zone.instant_of(day, range.end)?,
                    range.inclusive_end,
                    TimeKind::Instant,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(ranges)
    }

    /// Behavior: `io.deephaven.time.calendar.CalendarDay#toString`
    pub fn text(&self, kind: TimeKind) -> String {
        let ranges: Vec<String> = self
            .ranges
            .iter()
            .map(|range| {
                format!(
                    "TimeRange{{start={}, end={}, inclusiveEnd={}}}",
                    kind.text(range.start),
                    kind.text(range.end),
                    range.inclusive_end
                )
            })
            .collect();
        format!("CalendarDay{{businessTimeRanges=[{}]}}", ranges.join(", "))
    }
}

/// Which dates a count or a walk over dates takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dates {
    All,
    Business,
    NonBusiness,
}

/// A business calendar: a time zone, the dates it is valid for, the business hours of a
/// standard day, the days of the weekend and the holidays.
///
/// Behavior: `io.deephaven.time.calendar.BusinessCalendar`, `io.deephaven.time.calendar.Calendar`
#[derive(Debug)]
pub struct BusinessCalendar {
    name: String,
    description: Option<String>,
    zone: CalendarZone,
    first_valid: i64,
    last_valid: i64,
    standard: CalendarDay,
    /// Indexed by `getValue() - 1`: Monday first.
    weekend: [bool; 7],
    holidays: BTreeMap<i64, CalendarDay>,
    first_complete_year: i64,
    last_complete_year: i64,
}

/// The parts of a calendar, as its file gives them.
pub struct CalendarParts {
    pub name: String,
    pub description: Option<String>,
    pub zone: CalendarZone,
    pub first_valid: i64,
    pub last_valid: i64,
    /// Times of day.
    pub standard: CalendarDay,
    /// `java.time.DayOfWeek#getValue` of each day of the weekend.
    pub weekend: Vec<u8>,
    /// Instants, by date.
    pub holidays: BTreeMap<i64, CalendarDay>,
}

impl BusinessCalendar {
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#BusinessCalendar`: only the
    /// years from the first to the last valid date that the calendar holds whole are
    /// years a fraction of business years can be computed for.
    pub fn new(parts: CalendarParts) -> Self {
        let mut weekend = [false; 7];
        for day in parts.weekend {
            weekend[usize::from(day - 1)] = true;
        }
        let (first_year, first_month, first_date) = civil_from_days(parts.first_valid);
        let first_complete_year = if (first_month, first_date) == (1, 1) {
            first_year
        } else {
            first_year + 1
        };
        // Java takes the 365th day of a leap year as its last: a calendar that ends on
        // 30 December of a leap year counts that year as complete.
        let (last_year, _, _) = civil_from_days(parts.last_valid);
        let day_of_year = parts.last_valid - days_from_civil(last_year, 1, 1) + 1;
        let last_complete_year = if day_of_year == 365 || day_of_year == 366 {
            last_year
        } else {
            last_year - 1
        };
        Self {
            name: parts.name,
            description: parts.description,
            zone: parts.zone,
            first_valid: parts.first_valid,
            last_valid: parts.last_valid,
            standard: parts.standard,
            weekend,
            holidays: parts.holidays,
            first_complete_year,
            last_complete_year,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn zone(&self) -> &CalendarZone {
        &self.zone
    }

    pub fn first_valid_date(&self) -> i64 {
        self.first_valid
    }

    pub fn last_valid_date(&self) -> i64 {
        self.last_valid
    }

    /// The business hours of a day that is no holiday and not on the weekend, in times of
    /// day.
    pub fn standard_business_day(&self) -> &CalendarDay {
        &self.standard
    }

    /// The days of the weekend, by `java.time.DayOfWeek#getValue`, Monday first.
    pub fn weekend_days(&self) -> Vec<u8> {
        (1..=7)
            .filter(|day| self.weekend[usize::from(day - 1)])
            .collect()
    }

    pub fn holidays(&self) -> &BTreeMap<i64, CalendarDay> {
        &self.holidays
    }

    /// Behavior: `io.deephaven.time.calendar.Calendar#toString`
    pub fn text(&self) -> String {
        format!(
            "Calendar{{name='{}', description='{}', timeZone={}}}",
            self.name,
            self.description.as_deref().unwrap_or("null"),
            self.zone.id()
        )
    }

    /// The date of an instant in the calendar's zone.
    pub fn date_of(&self, nanos: i64) -> Result<i64, CalendarError> {
        Ok(self.zone.local_of(nanos)?.0)
    }

    /// The business hours of a date.
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#calendarDay(LocalDate)`
    pub fn calendar_day(&self, day: i64) -> Result<CalendarDay, CalendarError> {
        if day < self.first_valid {
            return Err(CalendarError::invalid_date(format!(
                "Date is before the first valid business calendar date:  date={} firstValidDate={}",
                date_text(day),
                date_text(self.first_valid)
            )));
        }
        if day > self.last_valid {
            return Err(CalendarError::invalid_date(format!(
                "Date is after the last valid business calendar date:  date={} lastValidDate={}",
                date_text(day),
                date_text(self.last_valid)
            )));
        }
        if let Some(holiday) = self.holidays.get(&day) {
            return Ok(holiday.clone());
        }
        if self.weekend[usize::from(day_of_week(day) - 1)] {
            return Ok(CalendarDay::holiday());
        }
        self.standard.on_date(day, &self.zone)
    }

    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(LocalDate)`
    pub fn is_business_day(&self, day: i64) -> Result<bool, CalendarError> {
        Ok(self.calendar_day(day)?.is_business_day())
    }

    /// Whether a day of the week is no day of the weekend (holidays aside).
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(DayOfWeek)`
    pub fn is_business_weekday(&self, day_of_week: u8) -> bool {
        !self.weekend[usize::from(day_of_week - 1)]
    }

    /// Whether `day` is a business day and the next business day is in another month.
    /// Only the month is compared, not the year, as in Java.
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfMonth(LocalDate)`
    pub fn is_last_business_day_of_month(&self, day: i64) -> Result<bool, CalendarError> {
        let Some(next) = self.next_business_day(day)? else {
            return Ok(false);
        };
        Ok(civil_from_days(day).1 != civil_from_days(next).1)
    }

    /// Whether `day` is a business day and the next business day is earlier in the week,
    /// or six or more days later.
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfWeek(LocalDate)`
    pub fn is_last_business_day_of_week(&self, day: i64) -> Result<bool, CalendarError> {
        let Some(next) = self.next_business_day(day)? else {
            return Ok(false);
        };
        Ok(day_of_week(day) > day_of_week(next) || next - day + 1 > 6)
    }

    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfYear(LocalDate)`
    pub fn is_last_business_day_of_year(&self, day: i64) -> Result<bool, CalendarError> {
        let Some(next) = self.next_business_day(day)? else {
            return Ok(false);
        };
        Ok(civil_from_days(day).0 != civil_from_days(next).0)
    }

    /// The next business day after `day`, when `day` is a business day.
    fn next_business_day(&self, day: i64) -> Result<Option<i64>, CalendarError> {
        if !self.is_business_day(day)? {
            return Ok(None);
        }
        self.plus_days(day, 1, Dates::Business)
    }

    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#isBusinessTime(Instant)`
    pub fn is_business_time(&self, nanos: i64) -> Result<bool, CalendarError> {
        Ok(self
            .calendar_day(self.date_of(nanos)?)?
            .is_business_time(nanos))
    }

    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#fractionStandardBusinessDay(LocalDate)`
    pub fn fraction_standard_business_day(&self, day: i64) -> Result<f64, CalendarError> {
        Ok(self.calendar_day(day)?.business_nanos() as f64 / self.standard.business_nanos() as f64)
    }

    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayComplete(Instant)`
    pub fn fraction_business_day_complete(&self, nanos: i64) -> Result<f64, CalendarError> {
        let day = self.calendar_day(self.date_of(nanos)?)?;
        if !day.is_business_day() {
            return Ok(1.0);
        }
        Ok(day.business_nanos_elapsed(nanos) as f64 / day.business_nanos() as f64)
    }

    /// Whether a date is one of `which`; every date is one of [`Dates::All`], and only
    /// business and non-business dates must be valid dates of the calendar.
    fn is(&self, day: i64, which: Dates) -> Result<bool, CalendarError> {
        Ok(match which {
            Dates::All => true,
            Dates::Business => self.is_business_day(day)?,
            Dates::NonBusiness => !self.is_business_day(day)?,
        })
    }

    /// The dates of `which` from `start` to `end`, each end included or not; none when
    /// `start` is after `end`.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendar#calendarDates(LocalDate, LocalDate, boolean, boolean)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#businessDates(LocalDate, LocalDate, boolean, boolean)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(LocalDate, LocalDate, boolean, boolean)`
    pub fn dates(
        &self,
        start: i64,
        end: i64,
        start_inclusive: bool,
        end_inclusive: bool,
        which: Dates,
    ) -> Result<Vec<i64>, CalendarError> {
        let mut dates = Vec::new();
        for day in start..=end {
            let skip = (!start_inclusive && day == start) || (!end_inclusive && day == end);
            if !skip && self.is(day, which)? {
                dates.push(day);
            }
        }
        Ok(dates)
    }

    /// The number of [`BusinessCalendar::dates`]. Java computes the count of all dates
    /// from the length of the span and of the others by counting, with the same answers.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendar#numberCalendarDates(LocalDate, LocalDate, boolean, boolean)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(LocalDate, LocalDate, boolean, boolean)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(LocalDate, LocalDate, boolean, boolean)`
    pub fn number_of_dates(
        &self,
        start: i64,
        end: i64,
        start_inclusive: bool,
        end_inclusive: bool,
        which: Dates,
    ) -> Result<i32, CalendarError> {
        if which == Dates::All {
            let days =
                (end + 1 - start) as i32 - i32::from(!start_inclusive) - i32::from(!end_inclusive);
            return Ok(days.max(0));
        }
        let count = self
            .dates(start, end, start_inclusive, end_inclusive, which)?
            .len();
        Ok(count as i32)
    }

    /// The date `days` dates of `which` after `day` (before it for a negative count). Zero
    /// dates from a date that is not of `which` is no date; any date is one of
    /// [`Dates::All`].
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendar#plusDays(LocalDate, int)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#plusBusinessDays(LocalDate, int)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#plusNonBusinessDays(LocalDate, int)`
    pub fn plus_days(
        &self,
        day: i64,
        days: i32,
        which: Dates,
    ) -> Result<Option<i64>, CalendarError> {
        if which == Dates::All {
            return Ok(Some(day + i64::from(days)));
        }
        if days == 0 {
            return Ok(self.is(day, which)?.then_some(day));
        }
        let step = days.signum();
        let mut date = day;
        let mut count = 0;
        while count != days {
            date += i64::from(step);
            if self.is(date, which)? {
                count += step;
            }
        }
        Ok(Some(date))
    }

    /// An instant moved by `days` dates of `which`, at the same time of day on the date it
    /// lands on, in the calendar's zone.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendar#plusDays(ZonedDateTime, int)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#plusBusinessDays(ZonedDateTime, int)`,
    /// `io.deephaven.time.calendar.BusinessCalendar#plusNonBusinessDays(ZonedDateTime, int)`
    pub fn plus_days_at_time(
        &self,
        nanos: i64,
        days: i32,
        which: Dates,
    ) -> Result<Option<i64>, CalendarError> {
        let (day, time) = self.zone.local_of(nanos)?;
        match self.plus_days(day, days, which)? {
            Some(date) => Ok(Some(self.zone.instant_of(date, time)?)),
            None => Ok(None),
        }
    }

    /// The business time from `start` to `end`, negative when `end` is earlier.
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#diffBusinessNanos(Instant, Instant)`
    pub fn diff_business_nanos(&self, start: i64, end: i64) -> Result<i64, CalendarError> {
        if start > end {
            return Ok(-self.diff_business_nanos(end, start)?);
        }
        let start_date = self.date_of(start)?;
        let end_date = self.date_of(end)?;
        if start_date == end_date {
            let day = self.calendar_day(start_date)?;
            return Ok(day.business_nanos_elapsed(end) - day.business_nanos_elapsed(start));
        }
        let mut nanos = self
            .calendar_day(start_date)?
            .business_nanos_remaining(start)
            + self.calendar_day(end_date)?.business_nanos_elapsed(end);
        for day in start_date + 1..end_date {
            nanos += self.calendar_day(day)?.business_nanos();
        }
        Ok(nanos)
    }

    /// The time from `start` to `end` that is not business time.
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#diffNonBusinessNanos(Instant, Instant)`
    pub fn diff_non_business_nanos(&self, start: i64, end: i64) -> Result<i64, CalendarError> {
        let total = end.checked_sub(start).ok_or_else(|| {
            CalendarError::java(
                "io.deephaven.time.DateTimeUtils$DateTimeOverflowException",
                "Subtraction overflow",
            )
        })?;
        Ok(total - self.diff_business_nanos(start, end)?)
    }

    /// The business years from `start` to `end`: the fraction of the business time of
    /// each year at either end, and the whole years between. The calendar must hold each
    /// of those two years whole. A `start` after `end` is not turned around, as in Java.
    ///
    /// Behavior: `io.deephaven.time.calendar.BusinessCalendar#diffBusinessYears(Instant, Instant)`
    pub fn diff_business_years(&self, start: i64, end: i64) -> Result<f64, CalendarError> {
        let year_start = civil_from_days(self.date_of(start)?).0;
        let year_end = civil_from_days(self.date_of(end)?).0;
        if year_start == year_end {
            let year = self.year(year_start)?;
            return Ok(self.diff_business_nanos(start, end)? as f64 / year.nanos as f64);
        }
        let first = self.year(year_start)?;
        let last = self.year(year_end)?;
        let head = self.diff_business_nanos(start, first.end)? as f64 / first.nanos as f64;
        let tail = self.diff_business_nanos(last.start, end)? as f64 / last.nanos as f64;
        // in Java's order: each whole number is added as a double, one at a time
        Ok(head + tail + year_end as f64 - year_start as f64 - 1.0)
    }

    /// The start and end instants and the business time of a year the calendar holds
    /// whole.
    fn year(&self, year: i64) -> Result<YearSummary, CalendarError> {
        if year < self.first_complete_year || year > self.last_complete_year {
            return Err(CalendarError::invalid_date(format!(
                "Business calendar does not contain a complete year for: year={year}"
            )));
        }
        let first = days_from_civil(year, 1, 1);
        let next = days_from_civil(year + 1, 1, 1);
        let mut nanos = 0;
        for day in first..next {
            nanos += self.calendar_day(day)?.business_nanos();
        }
        Ok(YearSummary {
            start: self.zone.instant_of(first, 0)?,
            end: self.zone.instant_of(next, 0)?,
            nanos,
        })
    }
}

struct YearSummary {
    start: i64,
    end: i64,
    nanos: i64,
}
