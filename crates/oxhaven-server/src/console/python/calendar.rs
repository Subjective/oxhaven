//! Business calendars for the `deephaven` package: the collection of calendars
//! (`deephaven.calendar`) and the handle that a calendar of the package (`_calendar.py`)
//! wraps.
//!
//! The calendars are `oxhaven_expr::calendar`'s; this module only hands their answers to
//! Python as numbers: dates as days since 1970-01-01, instants as nanoseconds since the
//! epoch, times of day as nanoseconds since midnight. Python turns them into the values of
//! its `java.time` classes and chooses among the Java overloads.
//!
//! Behavior: `io.deephaven.time.calendar.Calendars`, `io.deephaven.time.calendar.BusinessCalendar`

use std::sync::Arc;

use oxhaven_expr::calendar::{
    calendars, parse_date, BusinessCalendar, CalendarDay, CalendarError, CalendarZone, Dates,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

/// The exception Python raises for what a calendar refuses: the `RuntimeError` jpy raises
/// for a Java exception, with Java's class and message, or the package's `NotPorted`.
fn raised(py: Python<'_>, error: CalendarError) -> PyErr {
    match error {
        CalendarError::Java { .. } => PyRuntimeError::new_err(error.to_string()),
        CalendarError::Unported { method, what } => py
            .import("deephaven._java_time")
            .and_then(|module| module.getattr("NotPorted"))
            .and_then(|class| class.call1((method, what)))
            .map_or_else(|failed| failed, PyErr::from_value),
    }
}

/// The zone of the text of a calendar file's `timeZone`, by the aliases of the package,
/// which a script can change (`deephaven.time.time_zone_alias_add`), as Java's.
///
/// Behavior: `io.deephaven.time.TimeZoneAliases#zoneId`
fn zone_of(py: Python<'_>, text: &str) -> Result<CalendarZone, CalendarError> {
    let resolved = py
        .import("deephaven._java_time")
        .and_then(|module| module.getattr("zone_of"))
        .and_then(|zone_of| zone_of.call1((text,)))
        .and_then(|zone| {
            let id: String = zone.getattr("id")?.extract()?;
            let fixed: Option<i64> = zone.getattr("_fixed")?.extract()?;
            Ok((id, fixed))
        });
    match resolved {
        Ok((id, fixed)) => CalendarZone::of_id(&id, fixed),
        Err(error) => {
            let text = error.value(py).to_string();
            let not_ported = py
                .import("deephaven._java_time")
                .and_then(|module| module.getattr("NotPorted"))
                .is_ok_and(|class| error.is_instance(py, &class));
            Err(match text.strip_prefix("not ported: ") {
                Some(rest) if not_ported => CalendarError::Unported {
                    method: rest.to_string(),
                    what: String::new(),
                },
                _ => CalendarError::Java {
                    class: String::new(),
                    message: text,
                },
            })
        }
    }
}

/// Behavior: `io.deephaven.api.util.NameValidator#isValidQueryParameterName`, the rules of
/// a column name
fn valid_name(name: &str) -> bool {
    crate::table::support::validate_column_name(name).is_ok()
}

fn dates_of(which: &str) -> PyResult<Dates> {
    match which {
        "all" => Ok(Dates::All),
        "business" => Ok(Dates::Business),
        "non_business" => Ok(Dates::NonBusiness),
        other => Err(PyValueError::new_err(format!("no dates named {other}"))),
    }
}

type Ranges = Vec<(i64, i64, bool)>;

fn ranges(day: &CalendarDay) -> Ranges {
    day.ranges()
        .iter()
        .map(|range| (range.start, range.end, range.inclusive_end))
        .collect()
}

/// A calendar, as the package's `BusinessCalendar` holds it.
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct CalendarHandle {
    calendar: Arc<BusinessCalendar>,
}

#[pymethods]
impl CalendarHandle {
    /// Whether two handles hold the same calendar: a Java calendar is equal only to itself.
    fn same(&self, other: &CalendarHandle) -> bool {
        Arc::ptr_eq(&self.calendar, &other.calendar)
    }

    fn name(&self) -> String {
        self.calendar.name().to_string()
    }

    fn description(&self) -> Option<String> {
        self.calendar.description().map(str::to_string)
    }

    fn zone_id(&self) -> String {
        self.calendar.zone().id().to_string()
    }

    fn text(&self) -> String {
        self.calendar.text()
    }

    fn first_valid(&self) -> i64 {
        self.calendar.first_valid_date()
    }

    fn last_valid(&self) -> i64 {
        self.calendar.last_valid_date()
    }

    fn weekend(&self) -> Vec<u8> {
        self.calendar.weekend_days()
    }

    /// The ranges of the standard business day, in nanoseconds since midnight.
    fn standard(&self) -> Ranges {
        ranges(self.calendar.standard_business_day())
    }

    /// The holidays, in order of their dates, with their ranges in instants.
    fn holidays(&self) -> Vec<(i64, Ranges)> {
        self.calendar
            .holidays()
            .iter()
            .map(|(day, hours)| (*day, ranges(hours)))
            .collect()
    }

    /// The ranges of a date, in instants.
    fn day(&self, py: Python<'_>, day: i64) -> PyResult<Ranges> {
        self.calendar
            .calendar_day(day)
            .map(|hours| ranges(&hours))
            .map_err(|error| raised(py, error))
    }

    fn date_of(&self, py: Python<'_>, nanos: i64) -> PyResult<i64> {
        self.calendar
            .date_of(nanos)
            .map_err(|error| raised(py, error))
    }

    fn is_business_day(&self, py: Python<'_>, day: i64) -> PyResult<bool> {
        self.calendar
            .is_business_day(day)
            .map_err(|error| raised(py, error))
    }

    fn is_business_weekday(&self, day_of_week: u8) -> PyResult<bool> {
        if !(1..=7).contains(&day_of_week) {
            return Err(PyValueError::new_err("a day of the week is 1 to 7"));
        }
        Ok(self.calendar.is_business_weekday(day_of_week))
    }

    /// Whether a date is the last business day of its `month`, `week` or `year`.
    fn is_last_business_day(&self, py: Python<'_>, day: i64, of: &str) -> PyResult<bool> {
        let answer = match of {
            "month" => self.calendar.is_last_business_day_of_month(day),
            "week" => self.calendar.is_last_business_day_of_week(day),
            "year" => self.calendar.is_last_business_day_of_year(day),
            other => return Err(PyValueError::new_err(format!("no period named {other}"))),
        };
        answer.map_err(|error| raised(py, error))
    }

    fn is_business_time(&self, py: Python<'_>, nanos: i64) -> PyResult<bool> {
        self.calendar
            .is_business_time(nanos)
            .map_err(|error| raised(py, error))
    }

    fn fraction_standard(&self, py: Python<'_>, day: i64) -> PyResult<f64> {
        self.calendar
            .fraction_standard_business_day(day)
            .map_err(|error| raised(py, error))
    }

    fn fraction_complete(&self, py: Python<'_>, nanos: i64) -> PyResult<f64> {
        self.calendar
            .fraction_business_day_complete(nanos)
            .map_err(|error| raised(py, error))
    }

    fn dates(
        &self,
        py: Python<'_>,
        start: i64,
        end: i64,
        start_inclusive: bool,
        end_inclusive: bool,
        which: &str,
    ) -> PyResult<Vec<i64>> {
        self.calendar
            .dates(start, end, start_inclusive, end_inclusive, dates_of(which)?)
            .map_err(|error| raised(py, error))
    }

    fn count(
        &self,
        py: Python<'_>,
        start: i64,
        end: i64,
        start_inclusive: bool,
        end_inclusive: bool,
        which: &str,
    ) -> PyResult<i32> {
        self.calendar
            .number_of_dates(start, end, start_inclusive, end_inclusive, dates_of(which)?)
            .map_err(|error| raised(py, error))
    }

    fn plus(&self, py: Python<'_>, day: i64, days: i32, which: &str) -> PyResult<Option<i64>> {
        self.calendar
            .plus_days(day, days, dates_of(which)?)
            .map_err(|error| raised(py, error))
    }

    fn plus_at_time(
        &self,
        py: Python<'_>,
        nanos: i64,
        days: i32,
        which: &str,
    ) -> PyResult<Option<i64>> {
        self.calendar
            .plus_days_at_time(nanos, days, dates_of(which)?)
            .map_err(|error| raised(py, error))
    }

    fn diff_business(&self, py: Python<'_>, start: i64, end: i64) -> PyResult<i64> {
        self.calendar
            .diff_business_nanos(start, end)
            .map_err(|error| raised(py, error))
    }

    fn diff_non_business(&self, py: Python<'_>, start: i64, end: i64) -> PyResult<i64> {
        self.calendar
            .diff_non_business_nanos(start, end)
            .map_err(|error| raised(py, error))
    }

    fn diff_years(&self, py: Python<'_>, start: i64, end: i64) -> PyResult<f64> {
        self.calendar
            .diff_business_years(start, end)
            .map_err(|error| raised(py, error))
    }
}

/// Behavior: `io.deephaven.time.calendar.Calendars#addCalendarFromFile(String)`
#[pyfunction]
fn calendar_add_file(py: Python<'_>, path: &str) -> PyResult<()> {
    let zone = |text: &str| zone_of(py, text);
    calendars()
        .add_from_file(path, &zone, &valid_name)
        .map(drop)
        .map_err(|error| raised(py, error))
}

/// Behavior: `io.deephaven.time.calendar.Calendars#addCalendar`
#[pyfunction]
fn calendar_add(py: Python<'_>, handle: &CalendarHandle) -> PyResult<()> {
    calendars()
        .add(handle.calendar.clone(), &valid_name)
        .map_err(|error| raised(py, error))
}

/// Behavior: `io.deephaven.time.calendar.Calendars#removeCalendar`
#[pyfunction]
fn calendar_remove(name: &str) {
    calendars().remove(name);
}

/// Behavior: `io.deephaven.time.calendar.Calendars#setCalendar`
#[pyfunction]
fn calendar_set_default(name: &str) {
    calendars().set_default(name);
}

/// Behavior: `io.deephaven.time.calendar.Calendars#calendarName`
#[pyfunction]
fn calendar_default_name() -> Option<String> {
    calendars().default_name().map(str::to_string)
}

/// Behavior: `io.deephaven.time.calendar.Calendars#calendarNames`
#[pyfunction]
fn calendar_names() -> Vec<String> {
    calendars().names()
}

/// The calendar of a name, or the default calendar; none when there is no default name.
///
/// Behavior: `io.deephaven.time.calendar.Calendars#calendar(String)`,
/// `io.deephaven.time.calendar.Calendars#calendar()`
#[pyfunction]
#[pyo3(signature = (name = None))]
fn calendar_get(py: Python<'_>, name: Option<&str>) -> PyResult<Option<CalendarHandle>> {
    let registry = calendars();
    let found = match name {
        Some(name) => registry.get(name).map(Some),
        None => registry.default_calendar(),
    };
    found
        .map(|calendar| calendar.map(|calendar| CalendarHandle { calendar }))
        .map_err(|error| raised(py, error))
}

/// Behavior: `io.deephaven.time.DateTimeUtils#parseLocalDate`
#[pyfunction]
fn calendar_parse_date(py: Python<'_>, text: &str) -> PyResult<i64> {
    parse_date(text).map_err(|error| raised(py, error))
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<CalendarHandle>()?;
    native.add_function(wrap_pyfunction!(calendar_add_file, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_add, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_remove, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_set_default, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_default_name, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_names, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_get, native)?)?;
    native.add_function(wrap_pyfunction!(calendar_parse_date, native)?)?;
    Ok(())
}
