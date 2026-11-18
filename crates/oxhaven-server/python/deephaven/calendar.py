"""Business calendars: the collection of named calendars of the server.

The reference keeps the calendars in Java (`io.deephaven.time.calendar.Calendars`) and
hands a script the Java `BusinessCalendar`, whose methods the script then calls. Here the
calendars are the server's, and a script gets a `deephaven._calendar.BusinessCalendar`
with the same methods (`docs/CONSOLE.md`, "Business calendars").

Deephaven starts with three calendars, read from files that are under the Deephaven
Community License; this server does not ship them, so their names are listed and asking
for one of them is refused as not ported. Calendars added from files work.

Behavior: `deephaven.calendar`, `io.deephaven.time.calendar.Calendars`
"""

from typing import Optional, Union

from deephaven import DHError
from deephaven._calendar import BusinessCalendar
from deephaven._java_time import JavaError


def _native():
    import _oxhaven

    return _oxhaven


def _require_name(name) -> str:
    """Behavior: `io.deephaven.base.verify.Require#neqNull`, and jpy, which finds no
    method for a name that is no `str`"""
    if name is None:
        raise JavaError(
            "io.deephaven.base.verify.RequirementFailure: Requirement failed: required name != null, "
            "instead name == null."
        )
    if not isinstance(name, str):
        raise JavaError("no matching Java method overloads found")
    return name


def remove_calendar(name: str) -> None:
    """Removes the calendar with the given name from the set of available options.

    Args:
        name (str): the name of the calendar to remove

    Raises:
        DHError
    """
    try:
        _native().calendar_remove(_require_name(name))
    except Exception as e:
        raise DHError(e, f"failed to remove calendar '{name}'") from e


def add_calendar(cal: Union[BusinessCalendar, str]) -> None:
    """Adds a new business calendar to the set of available options.

    Args:
        cal (Union[BusinessCalendar, str]): business calendar or a path to a business calendar file

    Raises:
        DHError
    """
    if cal is None:
        raise DHError(message="cal must be specified")
    elif isinstance(cal, str):
        try:
            _native().calendar_add_file(cal)
        except Exception as e:
            raise DHError(e, f"failed to add calendar from file '{cal}'") from e
    else:
        try:
            if not isinstance(cal, BusinessCalendar):
                raise JavaError("no matching Java method overloads found")
            _native().calendar_add(cal._handle)
        except Exception as e:
            raise DHError(e, "failed to add calendar") from e


def set_calendar(name: str) -> None:
    """Sets the default calendar.

    Args:
        name (str): the name of the calendar

    Raises:
        DHError
    """
    try:
        _native().calendar_set_default(_require_name(name))
    except Exception as e:
        raise DHError(e, f"failed to set the default calendar name to '{name}'") from e


def calendar_names() -> list[str]:
    """Returns the names of all available calendar names.

    Returns:
        a list of all available calendar names

    Raises:
        DHError
    """
    try:
        return list(_native().calendar_names())
    except Exception as e:
        raise DHError(e, "failed to obtain the available calendar names.") from e


def calendar_name() -> str:
    """Returns the default business calendar name.

    The default business calendar is set by the 'Calendar.default' property or by calling 'set_calendar'.

    Returns:
        the default business calendar name

    Raises:
        DHError
    """
    try:
        return _native().calendar_default_name()
    except Exception as e:
        raise DHError(e, "failed to get the default calendar name.") from e


def calendar(name: Optional[str] = None) -> BusinessCalendar:
    """Returns the calendar with the given name.

    The returned calendar has the methods of an 'io.deephaven.time.calendar.BusinessCalendar' Java object.
    For details on the available methods, see https://deephaven.io/core/javadoc/io/deephaven/time/calendar/BusinessCalendar.html.
    These methods must be called with string arguments or with the date-time values of the 'deephaven.time' module.

    Args:
        name (Optional[str]): the name of the calendar.  If not specified, the default calendar is returned.

    Returns:
        the calendar with the given name or the defalt calendar if name is not specified.

    Raises:
        DHError
    """
    try:
        if name is not None and not isinstance(name, str):
            raise JavaError("no matching Java method overloads found")
        handle = _native().calendar_get(name)
        return None if handle is None else BusinessCalendar(handle)
    except Exception as e:
        raise DHError(e, "failed to get the default calendar.") from e
