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
    DataType, Date32Type, Int32Type, Int64Type, Time64NanosecondType, TimestampNanosecondType,
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

/// A function that makes or converts a value of one of these types.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TimeFn {
    /// `parseLocalDate(String)`, or `parseLocalDateQuiet`, which is null where it throws.
    ParseLocalDate { quiet: bool },
    ParseLocalTime { quiet: bool },
    ParseDuration { quiet: bool },
    ParsePeriod { quiet: bool },
    /// `parseTimeZone(String)` and `parseTimeZoneQuiet`; `timeZone(String)` is the quiet
    /// one for null and throws for other text.
    ParseTimeZone { quiet: bool, null_is_null: bool },
    ParseZonedDateTime { quiet: bool },
    /// `epochDaysToLocalDate(long)` and `epochDaysAsIntToLocalDate(int)`: null for null.
    EpochDaysToLocalDate,
    /// `LocalDate.ofEpochDay(long)`, which throws for every value outside its range, the
    /// null `long` among them.
    LocalDateOfEpochDay,
    /// `LocalTime.of(int, int)`, `(int, int, int)` and `(int, int, int, int)`, which
    /// throw for a field outside its range, the null `int` among them.
    LocalTimeOf,
    /// `toLocalDate(Instant, ZoneId)`, `toLocalTime(Instant, ZoneId)` and
    /// `toZonedDateTime(Instant, ZoneId)`: null when either is null.
    ToLocalDate,
    ToLocalTime,
    ToZonedDateTime,
    /// `Instant#atZone(ZoneId)`, which throws for a null instant or zone.
    AtZone,
    /// `toInstant(ZonedDateTime)`: null for null.
    ZonedToInstant,
    /// `toInstant(LocalDate, LocalTime, ZoneId)`: null when any is null.
    LocalToInstant,
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
    ) -> Option<Result<(TimeFn, Vec<ColumnType>, ColumnType), FormulaError>> {
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
        let integral = |t: &ColumnType| {
            matches!(
                t,
                T::Byte | T::Short | T::Char | T::Int | T::Long
            )
        };
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
            (TimeFn::ToLocalDate | TimeFn::ToLocalTime | TimeFn::ToZonedDateTime, [T::Instant, T::ZoneId]) => {
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
            (TimeFn::ToLocalDate | TimeFn::ToLocalTime | TimeFn::ToZonedDateTime, _) => {
                return Some(Err(FormulaError::Unsupported(format!(
                    "function {name}() of these arguments"
                ))))
            }
            _ => return Some(no_overload()),
        };
        Some(Ok((function, parameters, result)))
    }

    /// A static method of `java.time.LocalDate` or `java.time.LocalTime` called through
    /// its class: `ofEpochDay(long)` and `of(int, int[, int[, int]])`. An argument of
    /// another primitive type is converted by a cast, which keeps null.
    ///
    /// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` and `#convertParameters`, and
    /// `java.time.LocalDate#ofEpochDay`, `java.time.LocalTime#of`.
    pub(crate) fn static_method(
        class: &str,
        method: &str,
        types: &[ColumnType],
    ) -> Option<Result<(TimeFn, Vec<ColumnType>, ColumnType), FormulaError>> {
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
                region.offset_from_utc_datetime(&utc).fix().local_minus_utc(),
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
        values.iter().map(|value| value.map_or(0, |(nanos, _)| nanos)),
    )
    .with_data_type(ColumnType::Instant.arrow_type());
    let ids = StringArray::from_iter_values(values.iter().map(|value| value.map_or("", |(_, id)| id)));
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
            values.iter().map(|value| value.map_or(0, |parts| parts[at])),
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
            let nanos = parse_each(first, quiet, false, "duration", |text| {
                match duration_nanos(text) {
                    None => Ok(None),
                    Some(Some(nanos)) => Ok(Some(nanos)),
                    Some(None) => Err(not_read(text, "parseDuration")),
                }
            })?;
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
            let values = parse_each(first, quiet, false, "zoned date time", |text| {
                match zoned_of_text(text) {
                    Ok(value) => Ok(Some(value)),
                    Err(_) => Err(not_read(text, "parseZonedDateTime")),
                }
            })?;
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
        TimeFn::LocalTimeOf => {
            let fields: Vec<_> = arguments
                .iter()
                .map(|field| field.as_primitive::<Int32Type>())
                .collect();
            let names = ["HourOfDay", "MinuteOfHour", "SecondOfMinute", "NanoOfSecond"];
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
        TimeFn::ToLocalDate | TimeFn::ToLocalTime | TimeFn::ToZonedDateTime | TimeFn::AtZone => {
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
                dates.push(Some(if function == TimeFn::ToLocalTime {
                    time
                } else {
                    day
                }));
                zoned.push(Some((nanos, ids.value(row))));
            }
            match function {
                TimeFn::ToLocalDate => local_dates(dates),
                TimeFn::ToLocalTime => Ok(local_times(dates)),
                _ => Ok(zoned_date_times(zoned)),
            }
        }
        TimeFn::ZonedToInstant => {
            let parts = first.as_struct();
            let instants = parts.column(0).as_primitive::<TimestampNanosecondType>();
            let values = TimestampNanosecondArray::new(instants.values().clone(), parts.nulls().cloned())
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
                    let (hour, minute, second) =
                        (clock_seconds / 3600, clock_seconds / 60 % 60, clock_seconds % 60);
                    let seconds = match zone {
                        Zone::Utc => {
                            days_from_civil(year, month, day) * 86_400 + clock_seconds
                        }
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
    }
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
