//! The calendars of the process, by name, and the name of the default one.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{read_calendar_file, BusinessCalendar, CalendarError, CalendarZone};

/// The name of the default calendar before one is set: Deephaven's `Calendar.default`
/// property, `UTC` in the configuration it ships (`dh-defaults.prop`).
pub const DEFAULT_CALENDAR_NAME: &str = "UTC";

/// The calendars Deephaven 42.9 loads at start (`Calendar.importPath`). Their files are
/// under the Deephaven Community License, not Apache-2.0 or MIT, so this server does not
/// ship them (`tasks/11.1-console-calendars.md`, owner question): their names are known,
/// and asking for one of them is refused as not ported.
pub const SHIPPED_CALENDAR_NAMES: [&str; 3] = ["USBANK_EXAMPLE", "USNYSE_EXAMPLE", "UTC"];

/// The collection of calendars.
///
/// Behavior: `io.deephaven.time.calendar.Calendars`
#[derive(Debug)]
pub struct Calendars {
    default_name: Option<String>,
    /// By the upper case of their names.
    calendars: BTreeMap<String, Arc<BusinessCalendar>>,
    /// The calendars Deephaven ships that are in Deephaven's collection and not in this one.
    not_shipped: BTreeSet<String>,
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
    /// The collection Deephaven starts with, without the calendars it ships.
    pub fn new() -> Self {
        Self {
            default_name: Some(DEFAULT_CALENDAR_NAME.to_string()),
            calendars: BTreeMap::new(),
            not_shipped: SHIPPED_CALENDAR_NAMES
                .iter()
                .map(|name| name.to_string())
                .collect(),
        }
    }

    /// Removes the calendar of exactly that name: calendars are kept under the upper case
    /// of their names, so a name in lower case removes nothing, as in Java.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#removeCalendar`
    pub fn remove(&mut self, name: &str) {
        self.calendars.remove(name);
        self.not_shipped.remove(name);
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
        let taken = match self.calendars.get(&name) {
            Some(present) if Arc::ptr_eq(present, &calendar) => return Ok(()),
            Some(_) => true,
            // a calendar this server cannot hand out is never this one
            None => self.not_shipped.contains(&name),
        };
        if taken {
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
        let key = name.to_uppercase();
        if let Some(calendar) = self.calendars.get(&key) {
            return Ok(calendar.clone());
        }
        if self.not_shipped.contains(&key) {
            return Err(CalendarError::unported(
                "io.deephaven.time.calendar.Calendars#calendar",
                format!(
                    "the calendar {key}, which Deephaven ships and this server does not \
                     (its file is not under Apache-2.0 or MIT)"
                ),
            ));
        }
        Err(CalendarError::java(
            "java.lang.IllegalArgumentException",
            format!("No such calendar: {name}"),
        ))
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

    /// The names of the calendars, in order; those Deephaven ships among them.
    ///
    /// Behavior: `io.deephaven.time.calendar.Calendars#calendarNames`
    pub fn names(&self) -> Vec<String> {
        let names: BTreeSet<&String> = self.calendars.keys().chain(&self.not_shipped).collect();
        names.into_iter().cloned().collect()
    }
}
