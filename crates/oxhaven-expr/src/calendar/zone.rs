//! The time zone of a business calendar: a region of the time zone database or a fixed
//! offset from UTC, with the conversions between instants and local dates and times that
//! the calendar needs.

use chrono::{Offset, TimeZone};

use super::{CalendarError, NANOS_PER_DAY};
use crate::parse::instant::{zone, zone_offset, zoned_seconds, Zone};

const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// A time zone as `java.time.ZoneId` has one: its id and its rules.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CalendarZone {
    id: String,
    rules: Rules,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rules {
    /// Seconds east of UTC, always.
    Fixed(i64),
    Region(chrono_tz::Tz),
}

impl CalendarZone {
    /// The zone `TimeZoneAliases#zoneId` gives for `text`, with the aliases Deephaven
    /// ships; the console resolves a calendar's zone with its own, changeable, aliases and
    /// calls [`CalendarZone::of_id`] with the result.
    ///
    /// Behavior: `io.deephaven.time.TimeZoneAliases#zoneId`, `java.time.ZoneId#of`
    pub fn parse(text: &str) -> Result<Self, CalendarError> {
        if text == "Z" {
            return Ok(Self::fixed("Z", 0));
        }
        if let Some(offset) = zone_offset(text) {
            let Some(seconds) = offset else {
                return Err(CalendarError::java(
                    "java.time.DateTimeException",
                    format!("Invalid ID for offset-based ZoneId: {text}"),
                ));
            };
            let prefix = ["UTC", "GMT", "UT"]
                .into_iter()
                .find(|prefix| text.starts_with(prefix))
                .unwrap_or("");
            let id = match (prefix, seconds) {
                ("", _) => offset_id(seconds),
                (prefix, 0) => prefix.to_string(),
                (prefix, _) => format!("{prefix}{}", offset_id(seconds)),
            };
            return Ok(Self::fixed(&id, seconds));
        }
        match zone(text) {
            Some(Zone::Utc) => Ok(Self::fixed("UTC", 0)),
            Some(Zone::Region(region)) => Ok(Self {
                id: region.name().to_string(),
                rules: Rules::Region(region),
            }),
            None if text.starts_with("SystemV/") => Err(CalendarError::unported(
                "java.time.ZoneId#of",
                format!("the time zone {text}"),
            )),
            None => Err(CalendarError::java(
                "java.time.zone.ZoneRulesException",
                format!("Unknown time-zone ID: {text}"),
            )),
        }
    }

    /// The zone of an id that has been resolved already: a region of the time zone
    /// database, or a zone that is a fixed number of seconds east of UTC.
    pub fn of_id(id: &str, fixed_seconds: Option<i64>) -> Result<Self, CalendarError> {
        if let Some(seconds) = fixed_seconds {
            return Ok(Self::fixed(id, seconds));
        }
        match id.parse::<chrono_tz::Tz>() {
            Ok(region) => Ok(Self {
                id: id.to_string(),
                rules: Rules::Region(region),
            }),
            Err(_) => Err(CalendarError::unported(
                "java.time.ZoneId#of",
                format!("the time zone {id}, which this server's time zone database does not hold"),
            )),
        }
    }

    fn fixed(id: &str, seconds: i64) -> Self {
        Self {
            id: id.to_string(),
            rules: Rules::Fixed(seconds),
        }
    }

    /// The id, as `java.time.ZoneId#getId` gives it.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The seconds east of UTC at an instant.
    fn offset_at(&self, epoch_second: i64) -> Result<i64, CalendarError> {
        match self.rules {
            Rules::Fixed(seconds) => Ok(seconds),
            Rules::Region(region) => {
                let utc = chrono::DateTime::from_timestamp(epoch_second, 0)
                    .ok_or_else(|| out_of_range("an instant"))?
                    .naive_utc();
                Ok(i64::from(
                    region
                        .offset_from_utc_datetime(&utc)
                        .fix()
                        .local_minus_utc(),
                ))
            }
        }
    }

    /// The local date (days since 1970-01-01) and nanoseconds of the day of an instant.
    ///
    /// Behavior: `java.time.Instant#atZone`
    pub fn local_of(&self, nanos: i64) -> Result<(i64, i64), CalendarError> {
        let offset = self.offset_at(nanos.div_euclid(NANOS_PER_SECOND))?;
        let local = i128::from(nanos) + i128::from(offset) * i128::from(NANOS_PER_SECOND);
        let day = local.div_euclid(i128::from(NANOS_PER_DAY));
        let time = local.rem_euclid(i128::from(NANOS_PER_DAY));
        Ok((day as i64, time as i64))
    }

    /// The instant of a local date and time: a time the clocks skip is moved later by the
    /// length of the gap, and a time that occurs twice is the earlier.
    ///
    /// Behavior: `java.time.LocalDateTime#atZone`
    pub fn instant_of(&self, day: i64, nano_of_day: i64) -> Result<i64, CalendarError> {
        let second_of_day = nano_of_day.div_euclid(NANOS_PER_SECOND);
        let nano = nano_of_day.rem_euclid(NANOS_PER_SECOND);
        let seconds = match self.rules {
            Rules::Fixed(offset) => day
                .checked_mul(86_400)
                .and_then(|local| local.checked_add(second_of_day - offset)),
            Rules::Region(region) => {
                let (year, month, date) = crate::parse::instant::civil_from_days(day);
                zoned_seconds(
                    &region,
                    (year, month, date),
                    (
                        second_of_day / 3_600,
                        second_of_day / 60 % 60,
                        second_of_day % 60,
                    ),
                )
            }
        };
        seconds
            .and_then(|seconds| seconds.checked_mul(NANOS_PER_SECOND))
            .and_then(|nanos| nanos.checked_add(nano))
            .ok_or_else(|| out_of_range("a local date and time"))
    }
}

/// Behavior: `java.time.ZoneOffset#getId`
fn offset_id(seconds: i64) -> String {
    if seconds == 0 {
        return "Z".to_string();
    }
    let size = seconds.abs();
    let sign = if seconds < 0 { '-' } else { '+' };
    let mut id = format!("{sign}{:02}:{:02}", size / 3_600, size / 60 % 60);
    if size % 60 != 0 {
        id.push_str(&format!(":{:02}", size % 60));
    }
    id
}

/// What a calendar computes outside the instants a `long` of nanoseconds holds: Java
/// keeps an `Instant` in seconds and nanoseconds and goes further.
fn out_of_range(what: &str) -> CalendarError {
    CalendarError::unported(
        "io.deephaven.time.calendar.BusinessCalendar",
        format!("{what} outside the years 1677 to 2262"),
    )
}
