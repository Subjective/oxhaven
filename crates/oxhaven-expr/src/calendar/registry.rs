//! The calendars of the process, by name, and the name of the default one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{parse_calendar, read_calendar_file, BusinessCalendar, CalendarError, CalendarZone};

/// The name of the default calendar before one is set: Deephaven's `Calendar.default`
/// property, `UTC` in the configuration it ships (`dh-defaults.prop`).
pub const DEFAULT_CALENDAR_NAME: &str = "UTC";

/// The calendars the collection starts with, by name and file text: those of the names
/// Deephaven 42.9 loads at start (`Calendar.importPath`), written for Oxhaven from public
/// rules by `shipped/generate.py`. Deephaven's own files are under the Deephaven Community
/// License and are not used; `tasks/12.5-shipped-calendars.md` lists where these differ
/// from them.
pub const SHIPPED_CALENDARS: [(&str, &str); 3] = [
    ("UTC", include_str!("shipped/UTC.calendar")),
    (
        "USNYSE_EXAMPLE",
        include_str!("shipped/USNYSE_EXAMPLE.calendar"),
    ),
    (
        "USBANK_EXAMPLE",
        include_str!("shipped/USBANK_EXAMPLE.calendar"),
    ),
];

/// A calendar the collection starts with, read anew, with the zone aliases Deephaven
/// ships; none for another name.
pub fn shipped_calendar(name: &str) -> Option<BusinessCalendar> {
    SHIPPED_CALENDARS
        .iter()
        .find(|(shipped, _)| *shipped == name)
        .map(|(_, text)| {
            parse_calendar(text, &CalendarZone::parse).expect("a shipped calendar reads")
        })
}

/// The collection of calendars.
///
/// Behavior: `io.deephaven.time.calendar.Calendars`
#[derive(Debug)]
pub struct Calendars {
    default_name: Option<String>,
    /// By the upper case of their names.
    calendars: BTreeMap<String, Arc<BusinessCalendar>>,
}

static CALENDARS: Mutex<Option<Calendars>> = Mutex::new(None);

/// The calendars of the process, as Java has one collection per Java VM.
pub fn calendars() -> CalendarsGuard {
    let mut guard = CALENDARS.lock().unwrap_or_else(PoisonError::into_inner);
    guard.get_or_insert_with(Calendars::new);
    CalendarsGuard(guard)
}

/// The locked collection of calendars.
pub struct CalendarsGuard(MutexGuard<'static, Option<Calendars>>);

impl std::ops::Deref for CalendarsGuard {
    type Target = Calendars;

    fn deref(&self) -> &Calendars {
        self.0.as_ref().expect("created when locked")
    }
}

impl std::ops::DerefMut for CalendarsGuard {
    fn deref_mut(&mut self) -> &mut Calendars {
        self.0.as_mut().expect("created when locked")
    }
}

impl Default for Calendars {
    fn default() -> Self {
        Self::new()
    }
}

impl Calendars {
    /// The collection Deephaven starts with: the calendars of [`SHIPPED_CALENDARS`], read
    /// once, with `UTC` the default.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#calendarsFromConfiguration`, which
    /// the server reads when it starts, so the zone aliases are the ones Deephaven ships
    pub fn new() -> Self {
        let calendars = SHIPPED_CALENDARS
            .iter()
            .filter_map(|(name, _)| shipped_calendar(name))
            .map(|calendar| (calendar.name().to_uppercase(), Arc::new(calendar)))
            .collect();
        Self {
            default_name: Some(DEFAULT_CALENDAR_NAME.to_string()),
            calendars,
        }
    }

    /// Removes the calendar of exactly that name: calendars are kept under the upper case
    /// of their names, so a name in lower case removes nothing, as in Java.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#removeCalendar`
    pub fn remove(&mut self, name: &str) {
        self.calendars.remove(name);
    }

    /// Adds a calendar under the upper case of its name. The same calendar again is no
    /// change; another of the same name is refused, whatever it holds (a calendar is equal
    /// only to itself).
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#addCalendar`; the name must be a
    /// valid query variable name (`NameValidator#isValidQueryParameterName`), which
    /// `valid_name` tells.
    pub fn add(
        &mut self,
        calendar: Arc<BusinessCalendar>,
        valid_name: &dyn Fn(&str) -> bool,
    ) -> Result<(), CalendarError> {
        let name = calendar.name().to_uppercase();
        if !valid_name(&name) {
            return Err(CalendarError::java(
                "java.lang.IllegalArgumentException",
                format!("Invalid name for calendar: name='{name}'"),
            ));
        }
        if let Some(present) = self.calendars.get(&name) {
            if Arc::ptr_eq(present, &calendar) {
                return Ok(());
            }
            return Err(CalendarError::java(
                "java.lang.IllegalArgumentException",
                format!("Multiple calendars have the same name: name='{name}'"),
            ));
        }
        self.calendars.insert(name, calendar);
        Ok(())
    }

    /// Reads a calendar file and adds its calendar; the file's name must end in
    /// `.calendar`.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#addCalendarFromFile(File)`
    pub fn add_from_file(
        &mut self,
        path: &str,
        zone_of: &dyn Fn(&str) -> Result<CalendarZone, CalendarError>,
        valid_name: &dyn Fn(&str) -> bool,
    ) -> Result<Arc<BusinessCalendar>, CalendarError> {
        let absolute = std::path::absolute(path)
            .map(|absolute| absolute.to_string_lossy().into_owned())
            .unwrap_or_default();
        if !absolute.ends_with(".calendar") {
            return Err(CalendarError::java(
                "java.lang.UnsupportedOperationException",
                "Calendar file must be in .calendar format",
            ));
        }
        let calendar = Arc::new(read_calendar_file(path, zone_of)?);
        self.add(calendar.clone(), valid_name)?;
        Ok(calendar)
    }

    /// Sets the name of the default calendar, without looking for a calendar of that name.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#setCalendar`
    pub fn set_default(&mut self, name: &str) {
        self.default_name = Some(name.to_string());
    }

    /// Behavior: `io.deephaven.time.calendar.Calendars#calendarName`
    pub fn default_name(&self) -> Option<&str> {
        self.default_name.as_deref()
    }

    /// The calendar of a name, in any case.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#calendar(String)`
    pub fn get(&self, name: &str) -> Result<Arc<BusinessCalendar>, CalendarError> {
        self.calendars
            .get(&name.to_uppercase())
            .cloned()
            .ok_or_else(|| {
                CalendarError::java(
                    "java.lang.IllegalArgumentException",
                    format!("No such calendar: {name}"),
                )
            })
    }

    /// The default calendar; none when no default name is set.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#calendar()`
    pub fn default_calendar(&self) -> Result<Option<Arc<BusinessCalendar>>, CalendarError> {
        self.default_name
            .as_deref()
            .map(|name| self.get(name))
            .transpose()
    }

    /// The names of the calendars, in order.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#calendarNames`
    pub fn names(&self) -> Vec<String> {
        self.calendars.keys().cloned().collect()
    }
}
