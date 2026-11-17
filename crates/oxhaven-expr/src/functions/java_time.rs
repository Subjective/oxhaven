//! Values of the `java.time` classes that are column types and no number: `LocalDate`,
//! `LocalTime`, `Duration`, `Period`, `ZoneId` and `ZonedDateTime`. The functions of
//! `io.deephaven.time.DateTimeUtils` that make them from text, numbers and instants, the
//! static methods `LocalDate.ofEpochDay` and `LocalTime.of`, and `Instant#atZone`.
//!
//! The cells are those `oxhaven_contracts::schema::ColumnType` describes for each type.

use crate::parse::instant::{
    days_from_civil, duration_nanos, local_date_days, local_time_nanos, period_parts, zone,
    zone_id, zone_of_id, zoned_of_text, zoned_seconds, Zone,
};
use arrow::array::{
    Array, ArrayRef, AsArray, Date32Array, DurationNanosecondArray, Int32Array, StringArray,
    StructArray, Time64NanosecondArray, TimestampNanosecondArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{
    DataType, Date32Type, DurationNanosecondType, Int32Type, Int64Type, Time64NanosecondType,
    TimestampNanosecondType,
};
use oxhaven_contracts::expr::FormulaError;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// A function, its parameter types and its result type.
pub(crate) type Resolved = (TimeFn, Vec<ColumnType>, ColumnType);

/// A function that makes or converts a value of one of these types.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TimeFn {
    /// `parseLocalDate(String)`, or `parseLocalDateQuiet`, which is null where it throws.
    ParseLocalDate {
        quiet: bool,
    },
    ParseLocalTime {
        quiet: bool,
    },
    ParseDuration {
        quiet: bool,
    },
    ParsePeriod {
        quiet: bool,
    },
    /// `parseTimeZone(String)` and `parseTimeZoneQuiet`; `timeZone(String)` is the quiet
    /// one for null and throws for other text.
    ParseTimeZone {
        quiet: bool,
        null_is_null: bool,
    },
    ParseZonedDateTime {
        quiet: bool,
    },
    /// `epochDaysToLocalDate(long)` and `epochDaysAsIntToLocalDate(int)`: null for null.
    EpochDaysToLocalDate,
    /// `LocalDate.ofEpochDay(long)`, which throws for every value outside its range, the
    /// null `long` among them.
    LocalDateOfEpochDay,
    /// `LocalDate.of(int, int, int)`, which throws for a field outside its range, the null
    /// `int` among them, and for a day the month does not have.
    LocalDateOf,
    /// `LocalTime.of(int, int)`, `(int, int, int)` and `(int, int, int, int)`, which
    /// throw for a field outside its range, the null `int` among them.
    LocalTimeOf,
    /// `toLocalDate(Instant, ZoneId)`, `toLocalTime(Instant, ZoneId)`,
    /// `toLocalDateTime(Instant, ZoneId)` and `toZonedDateTime(Instant, ZoneId)`: null
    /// when either is null.
    ToLocalDate,
    ToLocalTime,
    ToLocalDateTime,
    ToZonedDateTime,
    /// `Instant#atZone(ZoneId)`, which throws for a null instant or zone.
    AtZone,
    /// `toInstant(ZonedDateTime)`: null for null.
    ZonedToInstant,
    /// `toInstant(LocalDate, LocalTime, ZoneId)`: null when any is null.
    LocalToInstant,
    /// `plus(Instant, Duration)` and `minus(Instant, Duration)`: null when either is null.
    InstantAndDuration {
        subtract: bool,
    },
    /// `plus(Instant, Period)` and `minus(Instant, Period)`: null when either is null.
    InstantAndPeriod {
        subtract: bool,
    },
    /// `plus(LocalDate, Period)` and `minus(LocalDate, Period)`: null when either is null.
    LocalDateAndPeriod {
        subtract: bool,
    },
    /// `plus(Duration, Duration)` and `minus(Duration, Duration)`: null when either is null.
    DurationSum {
        subtract: bool,
    },
    /// `plus(Period, Period)` and `minus(Period, Period)`: null when either is null.
    PeriodSum {
        subtract: bool,
    },
    /// `multiply(Duration, long)` and `divide(Duration, long)` of a duration and a `long`,
    /// null when either is null; `multiply(Period, int)` of a period and an `int`, which
    /// arrives here as a `long`.
    DurationTimes,
    DurationDivided,
    PeriodTimes,
    /// `year(LocalDate)`, `monthOfYear`, `dayOfMonth`, `dayOfYear`, `dayOfWeekValue` and
    /// `yearOfCentury` of a date: null for null.
    DateField(super::ZonedField),
    /// `hourOfDay(LocalTime)`, `minuteOfDay`, `secondOfDay`, `millisOfDay` and
    /// `nanosOfDay` of a time of day: null for null.
    TimeField(super::ZonedField),
}

impl TimeFn {
    /// The function `name` of `arguments`, with the parameter types and the result type;
    /// `None` when the name is none of these functions. An argument list the function does
    /// not take is a type error.
    ///
    /// Behavior: the overloads of `io.deephaven.time.DateTimeUtils`.
    pub(crate) fn resolve(
        name: &str,
        types: &[ColumnType],
    ) -> Option<Result<Resolved, FormulaError>> {
        use ColumnType as T;
        let quiet = name.ends_with("Quiet");
        let (function, result) = match name {
            "parseLocalDate" | "parseLocalDateQuiet" => {
                (TimeFn::ParseLocalDate { quiet }, T::LocalDate)
            }
            "parseLocalTime" | "parseLocalTimeQuiet" => {
                (TimeFn::ParseLocalTime { quiet }, T::LocalTime)
            }
            "parseDuration" | "parseDurationQuiet" => {
                (TimeFn::ParseDuration { quiet }, T::Duration)
            }
            "parsePeriod" | "parsePeriodQuiet" => (TimeFn::ParsePeriod { quiet }, T::Period),
            "parseTimeZone" | "parseTimeZoneQuiet" => (
                TimeFn::ParseTimeZone {
                    quiet,
                    null_is_null: quiet,
                },
                T::ZoneId,
            ),
            "timeZone" if types.len() == 1 => (
                TimeFn::ParseTimeZone {
                    quiet: false,
                    null_is_null: true,
                },
                T::ZoneId,
            ),
            "parseZonedDateTime" | "parseZonedDateTimeQuiet" => {
                (TimeFn::ParseZonedDateTime { quiet }, T::ZonedDateTime)
            }
            "epochDaysToLocalDate" => (TimeFn::EpochDaysToLocalDate, T::LocalDate),
            "epochDaysAsIntToLocalDate" => (TimeFn::EpochDaysToLocalDate, T::LocalDate),
            "toLocalDate" => (TimeFn::ToLocalDate, T::LocalDate),
            "toLocalTime" => (TimeFn::ToLocalTime, T::LocalTime),
            "toLocalDateTime" => (TimeFn::ToLocalDateTime, T::LocalDateTime),
            "toZonedDateTime" => (TimeFn::ToZonedDateTime, T::ZonedDateTime),
            "toInstant" => match types {
                [T::ZonedDateTime] => (TimeFn::ZonedToInstant, T::Instant),
                _ => (TimeFn::LocalToInstant, T::Instant),
            },
            _ => return None,
        };
        let no_overload = || {
            let given: Vec<String> = types.iter().map(|t| format!("{t:?}")).collect();
            Err(FormulaError::Type(format!(
                "cannot find {name}({})",
                given.join(", ")
            )))
        };
        let integral =
            |t: &ColumnType| matches!(t, T::Byte | T::Short | T::Char | T::Int | T::Long);
        let parameters = match (function, types) {
            (
                TimeFn::ParseLocalDate { .. }
                | TimeFn::ParseLocalTime { .. }
                | TimeFn::ParseDuration { .. }
                | TimeFn::ParsePeriod { .. }
                | TimeFn::ParseTimeZone { .. }
                | TimeFn::ParseZonedDateTime { .. },
                [T::String],
            ) => vec![T::String],
            // `epochDaysToLocalDate(long)` takes every integral type, and
            // `epochDaysAsIntToLocalDate(int)` those but `long`.
            (TimeFn::EpochDaysToLocalDate, [days]) if integral(days) => {
                if name == "epochDaysAsIntToLocalDate" {
                    if *days == T::Long {
                        return Some(no_overload());
                    }
                    vec![T::Int]
                } else {
                    vec![T::Long]
                }
            }
            (
                TimeFn::ToLocalDate
                | TimeFn::ToLocalTime
                | TimeFn::ToLocalDateTime
                | TimeFn::ToZonedDateTime,
                [T::Instant, T::ZoneId],
            ) => {
                vec![T::Instant, T::ZoneId]
            }
            (TimeFn::ZonedToInstant, [T::ZonedDateTime]) => vec![T::ZonedDateTime],
            (TimeFn::LocalToInstant, [T::LocalDate, T::LocalTime, T::ZoneId]) => {
                vec![T::LocalDate, T::LocalTime, T::ZoneId]
            }
            (TimeFn::LocalToInstant, _) if name == "toInstant" => {
                return Some(Err(FormulaError::Unsupported(
                    "function toInstant() of these arguments".into(),
                )))
            }
            (
                TimeFn::ToLocalDate
                | TimeFn::ToLocalTime
                | TimeFn::ToLocalDateTime
                | TimeFn::ToZonedDateTime,
                _,
            ) => {
                return Some(Err(FormulaError::Unsupported(format!(
                    "function {name}() of these arguments"
                ))))
            }
            _ => return Some(no_overload()),
        };
        Some(Ok((function, parameters, result)))
    }

    /// A static method of `java.time.LocalDate` or `java.time.LocalTime` called through
    /// its class: `ofEpochDay(long)`, `LocalDate.of(int, int, int)` and
    /// `LocalTime.of(int, int[, int[, int]])`. An argument of
    /// another primitive type is converted by a cast, which keeps null.
    ///
    /// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` and `#convertParameters`, and
    /// `java.time.LocalDate#ofEpochDay`, `java.time.LocalTime#of`.
    pub(crate) fn static_method(
        class: &str,
        method: &str,
        types: &[ColumnType],
    ) -> Option<Result<Resolved, FormulaError>> {
        use ColumnType as T;
        let class = class.strip_prefix("java.time.").unwrap_or(class);
        let int = |t: &ColumnType| matches!(t, T::Byte | T::Short | T::Char | T::Int);
        let refused = || {
            Some(Err(FormulaError::Unsupported(format!(
                "static method call {class}.{method}() with these arguments"
            ))))
        };
        match (class, method) {
            ("LocalDate", "ofEpochDay") => match types {
                [days] if int(days) || *days == T::Long => Some(Ok((
                    TimeFn::LocalDateOfEpochDay,
                    vec![T::Long],
                    T::LocalDate,
                ))),
                _ => refused(),
            },
            ("LocalDate", "of") => match types {
                [_, _, _] if types.iter().all(int) => {
                    Some(Ok((TimeFn::LocalDateOf, vec![T::Int; 3], T::LocalDate)))
                }
                _ => refused(),
            },
            ("LocalTime", "of") => match types.len() {
                2..=4 if types.iter().all(int) => Some(Ok((
                    TimeFn::LocalTimeOf,
                    vec![T::Int; types.len()],
                    T::LocalTime,
                ))),
                _ => refused(),
            },
            _ => None,
        }
    }
}

/// The zone of each id of a `ZoneId` column; a failure for an id this server does not
/// know, which it cannot have made.
fn zones(ids: &dyn Array) -> Result<Vec<Option<Zone>>, TableFailure> {
    ids.as_string::<i32>()
        .iter()
        .map(|id| match id {
            None => Ok(None),
            Some(id) => zone_of_id(id)
                .map(Some)
                .ok_or_else(|| failure(format!("time zone {id} is not one this server knows"))),
        })
        .collect()
}

/// The offset from UTC in seconds of `zone` at the instant `seconds`.
fn offset_seconds(zone: Zone, seconds: i64) -> Result<i64, TableFailure> {
    match zone {
        Zone::Utc => Ok(0),
        Zone::Region(region) => {
            use chrono::{Offset, TimeZone};
            let utc = chrono::DateTime::from_timestamp(seconds, 0)
                .ok_or_else(|| failure("an instant outside the range of the time zone rules"))?
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

/// The local date (days since the epoch) and time of day (nanoseconds) of `nanos` in
/// `zone`.
fn local_of(nanos: i64, zone: Zone) -> Result<(i64, i64), TableFailure> {
    let seconds = nanos.div_euclid(1_000_000_000);
    let local = i128::from(nanos) + i128::from(offset_seconds(zone, seconds)?) * 1_000_000_000;
    let day = local.div_euclid(i128::from(NANOS_PER_DAY));
    let time = local.rem_euclid(i128::from(NANOS_PER_DAY));
    Ok((day as i64, time as i64))
}

/// A cell of a `LocalDateTime` column: the nanoseconds from the epoch to the time
/// `time` of the day `day`, read as if in UTC.
fn local_date_time(day: i64, time: i64) -> Result<i64, TableFailure> {
    day.checked_mul(NANOS_PER_DAY)
        .and_then(|nanos| nanos.checked_add(time))
        .ok_or_else(|| {
            failure(format!(
                "the time of epoch day {day} is outside the range of a LocalDateTime column"
            ))
        })
}

fn local_dates(values: Vec<Option<i64>>) -> Result<ArrayRef, TableFailure> {
    let days = values
        .into_iter()
        .map(|days| {
            days.map(|days| {
                i32::try_from(days).map_err(|_| {
                    failure(format!(
                        "the date of epoch day {days} is outside the range of a LocalDate column"
                    ))
                })
            })
            .transpose()
        })
        .collect::<Result<Date32Array, _>>()?;
    Ok(Arc::new(days))
}

fn local_times(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Time64NanosecondArray::from(values))
}

/// A `ZonedDateTime` column of instants and zone ids.
pub(crate) fn zoned_date_times(values: Vec<Option<(i64, &str)>>) -> ArrayRef {
    let DataType::Struct(fields) = ColumnType::ZonedDateTime.arrow_type() else {
        unreachable!("a ZonedDateTime is a struct")
    };
    let nulls = NullBuffer::from_iter(values.iter().map(Option::is_some));
    let instants = TimestampNanosecondArray::from_iter_values(
        values
            .iter()
            .map(|value| value.map_or(0, |(nanos, _)| nanos)),
    )
    .with_data_type(ColumnType::Instant.arrow_type());
    let ids =
        StringArray::from_iter_values(values.iter().map(|value| value.map_or("", |(_, id)| id)));
    Arc::new(StructArray::new(
        fields,
        vec![Arc::new(instants), Arc::new(ids)],
        Some(nulls),
    ))
}

/// A `Period` column.
fn periods(values: Vec<Option<[i32; 3]>>) -> ArrayRef {
    let DataType::Struct(fields) = ColumnType::Period.arrow_type() else {
        unreachable!("a Period is a struct")
    };
    let nulls = NullBuffer::from_iter(values.iter().map(Option::is_some));
    let part = |at: usize| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(
            values
                .iter()
                .map(|value| value.map_or(0, |parts| parts[at])),
        ))
    };
    Arc::new(StructArray::new(
        fields,
        vec![part(0), part(1), part(2)],
        Some(nulls),
    ))
}

/// What a `parse...` function does with one text: the value, or the failure Java throws.
fn parse_each<T>(
    texts: &dyn Array,
    quiet: bool,
    null_is_null: bool,
    what: &str,
    read: impl Fn(&str) -> Result<Option<T>, TableFailure>,
) -> Result<Vec<Option<T>>, TableFailure> {
    texts
        .as_string::<i32>()
        .iter()
        .map(|text| {
            let Some(text) = text else {
                return if quiet || null_is_null {
                    Ok(None)
                } else {
                    Err(failure(format!(
                        "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse \
                         {what} (null): null"
                    )))
                };
            };
            if quiet && text.encode_utf16().count() <= 1 {
                return Ok(None);
            }
            match read(text)? {
                Some(value) => Ok(Some(value)),
                None if quiet => Ok(None),
                None => Err(failure(format!(
                    "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse \
                     {what}: {text}"
                ))),
            }
        })
        .collect()
}

/// A text Java reads and this server does not, which neither throws nor is null here.
fn not_read(text: &str, method: &str) -> TableFailure {
    failure(format!(
        "\"{text}\", in a spelling this server does not read (io.deephaven.time.DateTimeUtils#{method})"
    ))
}

fn read_with(
    found: Option<Result<i64, FormulaError>>,
    text: &str,
    method: &str,
) -> Result<Option<i64>, TableFailure> {
    match found {
        None => Ok(None),
        Some(Ok(value)) => Ok(Some(value)),
        Some(Err(_)) => Err(not_read(text, method)),
    }
}

/// Evaluates `function` of `arguments`, which have its parameter types.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalDate`, `#parseLocalTime`,
/// `#parseDuration`, `#parsePeriod`, `#parseTimeZone`, `#timeZone(String)`,
/// `#parseZonedDateTime` and their `...Quiet` forms (null for null and for text of one
/// character or none, and where the others throw), `#epochDaysToLocalDate`,
/// `#epochDaysAsIntToLocalDate`, `#toLocalDate(Instant, ZoneId)`,
/// `#toLocalDateTime(Instant, ZoneId)`,
/// `#toLocalTime(Instant, ZoneId)`, `#toZonedDateTime(Instant, ZoneId)`,
/// `#toInstant(ZonedDateTime)`, `#toInstant(LocalDate, LocalTime, ZoneId)`;
/// `java.time.LocalDate#ofEpochDay`, `java.time.LocalTime#of`, `java.time.Instant#atZone`.
pub(crate) fn evaluate(function: TimeFn, arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let first = arguments[0].as_ref();
    match function {
        TimeFn::ParseLocalDate { quiet } => {
            let days = parse_each(first, quiet, false, "local date", |text| {
                read_with(local_date_days(text), text, "parseLocalDate")
            })?;
            local_dates(days)
        }
        TimeFn::ParseLocalTime { quiet } => {
            let nanos = parse_each(first, quiet, false, "local time", |text| {
                read_with(local_time_nanos(text), text, "parseLocalTime")
            })?;
            Ok(local_times(nanos))
        }
        TimeFn::ParseDuration { quiet } => {
            let nanos = parse_each(
                first,
                quiet,
                false,
                "duration",
                |text| match duration_nanos(text) {
                    None => Ok(None),
                    Some(Some(nanos)) => Ok(Some(nanos)),
                    Some(None) => Err(not_read(text, "parseDuration")),
                },
            )?;
            Ok(Arc::new(DurationNanosecondArray::from(nanos)))
        }
        TimeFn::ParsePeriod { quiet } => {
            let parts = parse_each(first, quiet, false, "period", |text| {
                Ok(period_parts(text).flatten())
            })?;
            Ok(periods(parts))
        }
        TimeFn::ParseTimeZone {
            quiet,
            null_is_null,
        } => {
            let ids = parse_each(first, quiet, null_is_null, "time zone", |text| {
                match zone(text) {
                    Some(zone) => Ok(Some(zone_id(&zone))),
                    None => Err(not_read(text, "parseTimeZone")),
                }
            })?;
            Ok(Arc::new(StringArray::from(ids)))
        }
        TimeFn::ParseZonedDateTime { quiet } => {
            let values =
                parse_each(
                    first,
                    quiet,
                    false,
                    "zoned date time",
                    |text| match zoned_of_text(text) {
                        Ok(value) => Ok(Some(value)),
                        Err(_) => Err(not_read(text, "parseZonedDateTime")),
                    },
                )?;
            Ok(zoned_date_times(values))
        }
        TimeFn::EpochDaysToLocalDate => {
            let days: Vec<Option<i64>> = match first.data_type() {
                DataType::Int32 => first
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|days| days.map(i64::from))
                    .collect(),
                _ => first.as_primitive::<Int64Type>().iter().collect(),
            };
            local_dates(days)
        }
        TimeFn::LocalDateOfEpochDay => {
            // `ChronoField.EPOCH_DAY`'s range, as Java 42.9 printed it.
            const RANGE: std::ops::RangeInclusive<i64> = -365_243_219_162..=365_241_780_471;
            let days = first
                .as_primitive::<Int64Type>()
                .iter()
                .map(|days| {
                    let days = days.unwrap_or(i64::MIN);
                    if RANGE.contains(&days) {
                        Ok(Some(days))
                    } else {
                        Err(failure(format!(
                            "java.time.DateTimeException: Invalid value for EpochDay (valid \
                             values -365243219162 - 365241780471): {days}"
                        )))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            local_dates(days)
        }
        TimeFn::LocalDateOf => {
            let fields: Vec<_> = arguments
                .iter()
                .map(|field| field.as_primitive::<Int32Type>())
                .collect();
            let days = (0..first.len())
                .map(|row| {
                    let [year, month, day] = [0, 1, 2].map(|at| {
                        if fields[at].is_null(row) {
                            i64::from(i32::MIN)
                        } else {
                            i64::from(fields[at].value(row))
                        }
                    });
                    local_date_of(year, month, day).map(Some)
                })
                .collect::<Result<Vec<_>, _>>()?;
            local_dates(days)
        }
        TimeFn::LocalTimeOf => {
            let fields: Vec<_> = arguments
                .iter()
                .map(|field| field.as_primitive::<Int32Type>())
                .collect();
            let names = [
                "HourOfDay",
                "MinuteOfHour",
                "SecondOfMinute",
                "NanoOfSecond",
            ];
            let ends = [23, 59, 59, 999_999_999];
            let nanos = (0..first.len())
                .map(|row| {
                    let mut values = [0_i64; 4];
                    for (at, field) in fields.iter().enumerate() {
                        let value = if field.is_null(row) {
                            i32::MIN
                        } else {
                            field.value(row)
                        };
                        if !(0..=ends[at]).contains(&value) {
                            return Err(failure(format!(
                                "java.time.DateTimeException: Invalid value for {} (valid \
                                 values 0 - {}): {value}",
                                names[at], ends[at]
                            )));
                        }
                        values[at] = i64::from(value);
                    }
                    let [hour, minute, second, nano] = values;
                    Ok(Some(
                        ((hour * 60 + minute) * 60 + second) * 1_000_000_000 + nano,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(local_times(nanos))
        }
        TimeFn::ToLocalDate
        | TimeFn::ToLocalTime
        | TimeFn::ToLocalDateTime
        | TimeFn::ToZonedDateTime
        | TimeFn::AtZone => {
            let instants = first.as_primitive::<TimestampNanosecondType>();
            let ids = arguments[1].as_string::<i32>();
            let zones = zones(ids)?;
            let throws = function == TimeFn::AtZone;
            let mut dates = Vec::with_capacity(instants.len());
            let mut zoned = Vec::with_capacity(instants.len());
            for (row, zone) in zones.into_iter().enumerate() {
                let (Some(zone), false) = (zone, instants.is_null(row)) else {
                    if throws {
                        return Err(failure(
                            "java.lang.NullPointerException: Instant#atZone of a null instant \
                             or zone",
                        ));
                    }
                    dates.push(None);
                    zoned.push(None);
                    continue;
                };
                let nanos = instants.value(row);
                let (day, time) = local_of(nanos, zone)?;
                dates.push(Some(match function {
                    TimeFn::ToLocalTime => time,
                    TimeFn::ToLocalDateTime => local_date_time(day, time)?,
                    _ => day,
                }));
                zoned.push(Some((nanos, ids.value(row))));
            }
            match function {
                TimeFn::ToLocalDate => local_dates(dates),
                TimeFn::ToLocalTime => Ok(local_times(dates)),
                TimeFn::ToLocalDateTime => Ok(Arc::new(
                    TimestampNanosecondArray::from(dates)
                        .with_data_type(ColumnType::LocalDateTime.arrow_type()),
                )),
                _ => Ok(zoned_date_times(zoned)),
            }
        }
        TimeFn::ZonedToInstant => {
            let parts = first.as_struct();
            let instants = parts.column(0).as_primitive::<TimestampNanosecondType>();
            let values =
                TimestampNanosecondArray::new(instants.values().clone(), parts.nulls().cloned())
                    .with_data_type(ColumnType::Instant.arrow_type());
            Ok(Arc::new(values))
        }
        TimeFn::LocalToInstant => {
            let dates = first.as_primitive::<Date32Type>();
            let times = arguments[1].as_primitive::<Time64NanosecondType>();
            let zones = zones(arguments[2].as_ref())?;
            let values = zones
                .into_iter()
                .enumerate()
                .map(|(row, zone)| {
                    let (Some(zone), false, false) = (zone, dates.is_null(row), times.is_null(row))
                    else {
                        return Ok(None);
                    };
                    let (year, month, day) =
                        crate::parse::instant::civil_from_days(dates.value(row).into());
                    let time = times.value(row);
                    let clock_seconds = time.div_euclid(1_000_000_000);
                    let fraction = time.rem_euclid(1_000_000_000);
                    let (hour, minute, second) = (
                        clock_seconds / 3600,
                        clock_seconds / 60 % 60,
                        clock_seconds % 60,
                    );
                    let seconds = match zone {
                        Zone::Utc => days_from_civil(year, month, day) * 86_400 + clock_seconds,
                        Zone::Region(region) => {
                            zoned_seconds(&region, (year, month, day), (hour, minute, second))
                                .ok_or_else(|| {
                                    failure("a date outside the range of the time zone rules")
                                })?
                        }
                    };
                    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(fraction);
                    i64::try_from(nanos)
                        .ok()
                        .filter(|nanos| *nanos != i64::MIN)
                        .map(Some)
                        .ok_or_else(|| {
                            failure("an instant outside the range of a 64-bit nanosecond timestamp")
                        })
                })
                .collect::<Result<TimestampNanosecondArray, TableFailure>>()?;
            Ok(Arc::new(
                values.with_data_type(ColumnType::Instant.arrow_type()),
            ))
        }
        TimeFn::InstantAndDuration { subtract } => {
            let stamps = first.as_primitive::<TimestampNanosecondType>();
            let amounts = arguments[1].as_primitive::<DurationNanosecondType>();
            super::eval::instants((0..stamps.len()).map(|row| {
                if stamps.is_null(row) || amounts.is_null(row) {
                    return Ok(None);
                }
                let amount = i128::from(amounts.value(row));
                let amount = if subtract { -amount } else { amount };
                super::eval::store_instant(i128::from(stamps.value(row)) + amount).map(Some)
            }))
        }
        TimeFn::InstantAndPeriod { subtract } => {
            let stamps = first.as_primitive::<TimestampNanosecondType>();
            let amounts = period_values(arguments[1].as_ref());
            super::eval::instants((0..stamps.len()).map(|row| {
                let (false, Some([years, months, days])) = (stamps.is_null(row), amounts[row])
                else {
                    return Ok(None);
                };
                // `Period#addTo` and `#subtractFrom`: whole years, or else the total in
                // months, then days; an `Instant` takes no unit longer than a day.
                if months == 0 && years != 0 {
                    return Err(unsupported_unit("Years"));
                }
                if i64::from(years) * 12 + i64::from(months) != 0 {
                    return Err(unsupported_unit("Months"));
                }
                let days = i128::from(days) * i128::from(NANOS_PER_DAY);
                let days = if subtract { -days } else { days };
                super::eval::store_instant(i128::from(stamps.value(row)) + days).map(Some)
            }))
        }
        TimeFn::LocalDateAndPeriod { subtract } => {
            let dates = first.as_primitive::<Date32Type>();
            let amounts = period_values(arguments[1].as_ref());
            let values = (0..dates.len())
                .map(|row| match (dates.is_null(row), amounts[row]) {
                    (false, Some(period)) => {
                        date_plus_period(i64::from(dates.value(row)), period, subtract).map(Some)
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>, _>>()?;
            local_dates(values)
        }
        TimeFn::DurationSum { subtract } => {
            let left = first.as_primitive::<DurationNanosecondType>();
            let right = arguments[1].as_primitive::<DurationNanosecondType>();
            let values = (0..left.len())
                .map(|row| {
                    if left.is_null(row) || right.is_null(row) {
                        return Ok(None);
                    }
                    let (a, b) = (left.value(row), right.value(row));
                    let sum = if subtract {
                        a.checked_sub(b)
                    } else {
                        a.checked_add(b)
                    };
                    sum.map(Some).ok_or_else(longer_than_a_long)
                })
                .collect::<Result<DurationNanosecondArray, _>>()?;
            Ok(Arc::new(values))
        }
        TimeFn::PeriodSum { subtract } => {
            let left = period_values(first);
            let right = period_values(arguments[1].as_ref());
            let values = left
                .into_iter()
                .zip(right)
                .map(|pair| {
                    let (Some(a), Some(b)) = pair else {
                        return Ok(None);
                    };
                    let mut parts = [0; 3];
                    for at in 0..3 {
                        let part = if subtract {
                            a[at].checked_sub(b[at])
                        } else {
                            a[at].checked_add(b[at])
                        };
                        parts[at] = part.ok_or_else(int_overflow)?;
                    }
                    Ok(Some(parts))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(periods(values))
        }
        TimeFn::DurationTimes | TimeFn::DurationDivided => {
            let amounts = first.as_primitive::<DurationNanosecondType>();
            let scalars = arguments[1].as_primitive::<Int64Type>();
            let values = (0..amounts.len())
                .map(|row| {
                    if amounts.is_null(row) || scalars.is_null(row) {
                        return Ok(None);
                    }
                    let (amount, scalar) = (amounts.value(row), scalars.value(row));
                    let result = if function == TimeFn::DurationTimes {
                        amount.checked_mul(scalar)
                    } else if scalar == 0 {
                        return Err(failure(
                            "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: \
                             Operation failed due to overflow (java.lang.ArithmeticException: \
                             Cannot divide by zero)",
                        ));
                    } else {
                        amount.checked_div(scalar)
                    };
                    result.map(Some).ok_or_else(longer_than_a_long)
                })
                .collect::<Result<DurationNanosecondArray, _>>()?;
            Ok(Arc::new(values))
        }
        TimeFn::DateField(_) | TimeFn::TimeField(_) => Ok(date_or_time_field(function, first)),
        TimeFn::PeriodTimes => {
            let amounts = period_values(first);
            let scalars = arguments[1].as_primitive::<Int64Type>();
            let values = amounts
                .into_iter()
                .enumerate()
                .map(|(row, amount)| {
                    let (Some(amount), false) = (amount, scalars.is_null(row)) else {
                        return Ok(None);
                    };
                    let scalar = i32::try_from(scalars.value(row)).map_err(|_| int_overflow())?;
                    let mut parts = [0; 3];
                    for at in 0..3 {
                        parts[at] = amount[at].checked_mul(scalar).ok_or_else(int_overflow)?;
                    }
                    Ok(Some(parts))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(periods(values))
        }
    }
}

/// Behavior: `DateTimeUtils#year(LocalDate)` and its relatives, `LocalDate#getYear`,
/// `#getMonthValue`, `#getDayOfMonth`, `#getDayOfYear` and `#getDayOfWeek`, with Monday 1;
/// `yearOfCentury` is the year `%` 100, negative for a negative year. Of a `LocalTime`,
/// `#toNanoOfDay` in each unit, rounded down.
fn date_or_time_field(function: TimeFn, values: &dyn Array) -> ArrayRef {
    use super::ZonedField as F;
    let field = match function {
        TimeFn::DateField(field) | TimeFn::TimeField(field) => field,
        _ => unreachable!("a field of a date or a time of day"),
    };
    let cells: Vec<Option<i64>> = match function {
        TimeFn::DateField(_) => values
            .as_primitive::<Date32Type>()
            .iter()
            .map(|days| {
                let days = i64::from(days?);
                let (year, month, day) = crate::parse::instant::civil_from_days(days);
                Some(match field {
                    F::Year => year,
                    F::YearOfCentury => year % 100,
                    F::MonthOfYear => month,
                    F::DayOfMonth => day,
                    F::DayOfYear => days - days_from_civil(year, 1, 1) + 1,
                    // The first day of 1970 was a Thursday.
                    _ => (days + 3).rem_euclid(7) + 1,
                })
            })
            .collect(),
        _ => values
            .as_primitive::<Time64NanosecondType>()
            .iter()
            .map(|nanos| {
                let nanos = nanos?;
                Some(match field {
                    F::NanosOfDay => nanos,
                    F::MillisOfDay => nanos / 1_000_000,
                    F::SecondOfDay => nanos / 1_000_000_000,
                    F::MinuteOfDay => nanos / 60_000_000_000,
                    _ => nanos / 3_600_000_000_000,
                })
            })
            .collect(),
    };
    if field.result_type() == ColumnType::Long {
        Arc::new(arrow::array::Int64Array::from(cells))
    } else {
        Arc::new(Int32Array::from_iter(
            cells.into_iter().map(|cell| cell.map(|value| value as i32)),
        ))
    }
}

/// The epoch day of a year, month and day.
///
/// Behavior: `java.time.LocalDate#of(int, int, int)`: `ChronoField#checkValidValue` of
/// each field, whose message names the field and its range, then `LocalDate#create`,
/// which refuses a day the month does not have.
fn local_date_of(year: i64, month: i64, day: i64) -> Result<i64, TableFailure> {
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
    for (name, range, value, (low, high)) in [
        (
            "Year",
            "-999999999 - 999999999",
            year,
            (-999_999_999, 999_999_999),
        ),
        ("MonthOfYear", "1 - 12", month, (1, 12)),
        ("DayOfMonth", "1 - 28/31", day, (1, 31)),
    ] {
        if !(low..=high).contains(&value) {
            return Err(failure(format!(
                "java.time.DateTimeException: Invalid value for {name} (valid values \
                 {range}): {value}"
            )));
        }
    }
    if day > crate::parse::instant::days_in_month(year, month) {
        let name = MONTHS[(month - 1) as usize];
        return Err(failure(if day == 29 {
            format!(
                "java.time.DateTimeException: Invalid date 'February 29' as '{year}' is not \
                 a leap year"
            )
        } else {
            format!("java.time.DateTimeException: Invalid date '{name} {day}'")
        }));
    }
    let days = days_from_civil(year, month, day);
    if i32::try_from(days).is_err() {
        return Err(failure(
            "a date outside the range of a 32-bit day count, which this server does not hold",
        ));
    }
    Ok(days)
}

/// The years, months and days of each cell of a `Period` column.
fn period_values(column: &dyn Array) -> Vec<Option<[i32; 3]>> {
    let parts = column.as_struct();
    let field = |at: usize| parts.column(at).as_primitive::<Int32Type>();
    let (years, months, days) = (field(0), field(1), field(2));
    (0..parts.len())
        .map(|row| {
            (!parts.is_null(row)).then(|| [years.value(row), months.value(row), days.value(row)])
        })
        .collect()
}

fn overflow(cause: &str) -> TableFailure {
    failure(format!(
        "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: Operation failed due to \
         overflow ({cause})"
    ))
}

fn unsupported_unit(unit: &str) -> TableFailure {
    overflow(&format!(
        "java.time.temporal.UnsupportedTemporalTypeException: Unsupported unit: {unit}"
    ))
}

fn int_overflow() -> TableFailure {
    overflow("java.lang.ArithmeticException: integer overflow")
}

/// Java holds a duration of more nanoseconds than a `long` does; this server does not.
fn longer_than_a_long() -> TableFailure {
    failure("a duration longer than a long of nanoseconds, which this server does not hold")
}

/// The epoch day of `days` plus, or minus, a period.
///
/// Behavior: `java.time.Period#addTo` and `#subtractFrom` of a `LocalDate`: whole years
/// by `LocalDate#plusYears` when the period has no months, otherwise its total in months by
/// `#plusMonths`, each of which moves the day back to the last of a shorter month; then
/// the days by `#plusDays`. A year outside -999,999,999 to 999,999,999 throws.
fn date_plus_period(days: i64, period: [i32; 3], subtract: bool) -> Result<i64, TableFailure> {
    const YEARS: std::ops::RangeInclusive<i64> = -999_999_999..=999_999_999;
    let sign = if subtract { -1 } else { 1 };
    let [years, months, extra] = period.map(|part| i64::from(part) * sign);
    let (mut year, mut month, mut day) = crate::parse::instant::civil_from_days(days);
    // Whole years move the date as many twelves of months do.
    let total = years * 12 + months;
    if total != 0 {
        let count = year * 12 + (month - 1) + total;
        year = count.div_euclid(12);
        month = count.rem_euclid(12) + 1;
        if !YEARS.contains(&year) {
            return Err(overflow(&format!(
                "java.time.DateTimeException: Invalid value for Year (valid values -999999999 \
                 - 999999999): {year}"
            )));
        }
        day = day.min(crate::parse::instant::days_in_month(year, month));
    }
    let result = days_from_civil(year, month, day) + extra;
    if i32::try_from(result).is_err() {
        return Err(failure(
            "a date outside the range of a 32-bit day count, which this server does not hold",
        ));
    }
    Ok(result)
}

/// Whether a value of this type is one of these objects.
pub(crate) fn is_time_object(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::LocalDate
            | ColumnType::LocalTime
            | ColumnType::Duration
            | ColumnType::Period
            | ColumnType::ZoneId
            | ColumnType::ZonedDateTime
    )
}
