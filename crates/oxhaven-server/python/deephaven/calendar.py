"""Business calendars. Not ported: the engine has none.

The reference keeps named calendars (business days, holidays, the hours of a business day)
in Java and hands a script the Java calendar itself, whose Java methods the script then
calls. Every function here is refused by the name of the Java method it stands for, in the
`DHError` the reference raises when that method fails. The module is here so that code
which imports it and uses no calendar runs, as `deephaven.plot.express` does for a plot
without `calendar=`.

Behavior: `deephaven.calendar`, `io.deephaven.time.calendar.Calendars`
"""

from typing import Optional

from deephaven import DHError
from deephaven.dtypes import _java_time


def _refuse(java_method: str, message: str):
    try:
        raise _java_time.NotPorted(f"io.deephaven.time.calendar.Calendars#{java_method}")
    except Exception as e:
        raise DHError(e, message) from e


def remove_calendar(name: str) -> None:
    """Not ported: removes the calendar of that name."""
    _refuse("removeCalendar", f"failed to remove calendar '{name}'")


def add_calendar(cal) -> None:
    """Not ported: adds a calendar, or the calendar a file describes."""
    if cal is None:
        raise DHError(message="cal must be specified")
    if isinstance(cal, str):
        _refuse("addCalendarFromFile", f"failed to add calendar from file '{cal}'")
    _refuse("addCalendar", "failed to add calendar")


def set_calendar(name: str) -> None:
    """Not ported: makes the calendar of that name the default."""
    _refuse("setCalendar", f"failed to set the default calendar name to '{name}'")


def calendar_names() -> list[str]:
    """Not ported: the names of the calendars."""
    _refuse("calendarNames", "failed to obtain the available calendar names.")


def calendar_name() -> str:
    """Not ported: the name of the default calendar."""
    _refuse("calendarName", "failed to get the default calendar name.")


def calendar(name: Optional[str] = None):
    """Not ported: the calendar of that name, or the default calendar."""
    _refuse("calendar", "failed to get the default calendar.")
