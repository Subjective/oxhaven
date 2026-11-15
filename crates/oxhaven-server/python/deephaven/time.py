"""The clock, time zones, and the conversions between Python's, numpy's and pandas' time
values and the engine's.

Behavior: `deephaven.time` of the reference package, function by function. The reference
returns Java objects of `java.time`; the functions here return the values of
`deephaven._java_time`, which have the same text form. `new_table` takes an `Instant` in a
`datetime_col`; the other types are values for Python code, since the engine has no column
type for them.

numpy and pandas are imported only when a function is given one of their values or asked
for one.
"""

import datetime
import sys
from typing import Literal, Optional, Union

from . import _java_time
from ._java_time import JavaError, NotPorted
from .dherror import DHError
from .dtypes import Duration, Instant, LocalDate, LocalTime, Period, TimeZone, ZonedDateTime

_NANOS_PER_SECOND = 1000000000
_NANOS_PER_MICRO = 1000

TimeZoneLike = Union[TimeZone.j_type, str, datetime.tzinfo, datetime.datetime]
LocalDateLike = Union[LocalDate.j_type, str, datetime.date, datetime.datetime]
LocalTimeLike = Union[LocalTime.j_type, int, str, datetime.time, datetime.datetime]
InstantLike = Union[Instant.j_type, int, str, datetime.datetime]
ZonedDateTimeLike = Union[ZonedDateTime.j_type, str, datetime.datetime]
DurationLike = Union[Duration.j_type, int, str, datetime.timedelta]
PeriodLike = Union[Period.j_type, str, datetime.timedelta]


def _numpy():
    return sys.modules.get("numpy")


def _pandas():
    return sys.modules.get("pandas")


def _is_null(value) -> bool:
    """Whether `value` is what `pandas.isnull` calls null: `None`, a NaN, a not-a-time."""
    if value is None:
        return True
    pandas = _pandas()
    if pandas is not None:
        null = pandas.isnull(value)
        # a boolean for one value, an array for a collection, whose truth is an error
        return bool(null)
    numpy = _numpy()
    if numpy is not None and isinstance(value, (numpy.datetime64, numpy.timedelta64)):
        return bool(numpy.isnat(value))
    return isinstance(value, float) and value != value


def _is_a(value, module, name: str) -> bool:
    """Whether `value` is an instance of the class `name` of a module that may not be
    loaded; a value of a module that is not loaded cannot exist."""
    return module is not None and isinstance(value, getattr(module, name))


# Clock


def dh_now(system: bool = False, resolution: Literal["ns", "ms"] = "ns") -> Instant:
    """The current time of the server as an instant, to the nanosecond or the millisecond.
    The engine has one clock, the system's, whatever `system` says."""
    try:
        if resolution == "ns":
            return _java_time.Instant(_java_time.now_nanos())
        elif resolution == "ms":
            return _java_time.Instant(_java_time.now_nanos() // 1_000_000 * 1_000_000)
        else:
            raise TypeError("Unsupported time resolution: " + resolution)
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def dh_today(tz: Optional[TimeZone] = None) -> str:
    """The current date in a time zone, the server's without one, as `yyyy-MM-dd`."""
    try:
        if tz is None:
            tz = _java_time.system_zone()
        if not isinstance(tz, _java_time.ZoneId):
            # jpy finds no `DateTimeUtils#today` for anything else
            raise RuntimeError("no matching Java method overloads found")
        now = _java_time.ZonedDateTime(_java_time.now_nanos(), tz)
        return str(now.local()[0])
    except Exception as e:
        raise DHError(e) from e


def dh_time_zone() -> TimeZone:
    """The time zone of the server."""
    try:
        return _java_time.system_zone()
    except Exception as e:
        raise DHError(e) from e


# Time zone aliases


def time_zone_alias_add(alias: str, tz: str) -> None:
    """Adds a name for a time zone, for every parser of time zones in this package. The
    formulas of the engine do not know the name: the engine has no time zone aliases."""
    try:
        return _java_time.alias_add(alias, tz)
    except Exception as e:
        raise DHError(e) from e


def time_zone_alias_rm(alias: str) -> bool:
    """Removes a time zone alias; whether there was one."""
    try:
        return _java_time.alias_rm(alias)
    except Exception as e:
        raise DHError(e) from e


# Conversions: Python to engine


def _tzinfo_to_j_time_zone(tzi: Optional[datetime.tzinfo] = None) -> TimeZone:
    if not tzi:
        return None
    pytz = sys.modules.get("pytz")
    if pytz is not None and isinstance(tzi, pytz.tzinfo.BaseTzInfo):
        return _java_time.parse_time_zone(tzi.zone)
    import zoneinfo

    if isinstance(tzi, zoneinfo.ZoneInfo):
        return _java_time.parse_time_zone(tzi.key)
    if isinstance(tzi, datetime.timezone):
        offset = tzi.utcoffset(None)
        if offset is None:
            raise ValueError("Unable to determine the time zone UTC offset")
        if not offset:
            return _java_time.parse_time_zone("UTC")
        if offset.microseconds != 0 or offset.seconds % 60 != 0:
            raise ValueError(f"Unsupported time zone offset contains fractions of a minute: {offset}")
        ts = offset.total_seconds()
        sign = "+" if ts >= 0 else "-"
        ts = abs(ts)
        hours = int(ts / 3600)
        minutes = int((ts % 3600) / 60)
        return _java_time.parse_time_zone(f"UTC{sign}{hours:02d}:{minutes:02d}")
    details = "\n\t".join(
        [f"type={type(tzi).mro()}"]
        + [f"obj.{attr}={getattr(tzi, attr)}" for attr in dir(tzi) if not attr.startswith("_")]
    )
    raise TypeError(f"Unsupported conversion: {str(type(tzi))} -> TimeZone\n\tDetails:\n\t{details}")


def to_j_time_zone(tz: Optional[TimeZoneLike]) -> Optional[TimeZone]:
    """A time zone from its name (`"America/New_York"`, `"ET"`, `"UTC+01:00"`), a
    `tzinfo`, or a `datetime` that has one."""
    try:
        if _is_null(tz):
            return None
        elif isinstance(tz, _java_time.ZoneId):
            return tz
        elif isinstance(tz, str):
            return _java_time.parse_time_zone(tz)
        elif isinstance(tz, datetime.tzinfo):
            return _tzinfo_to_j_time_zone(tz)
        elif isinstance(tz, datetime.datetime):
            rst = _tzinfo_to_j_time_zone(tz.tzinfo)
            if not rst:
                raise ValueError("datetime is not time zone aware")
            return rst
        else:
            raise TypeError("Unsupported conversion: " + str(type(tz)) + " -> TimeZone")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_local_date(dt: Optional[LocalDateLike]) -> Optional[LocalDate]:
    """A date without a time zone from a string (`"2024-01-31"`), a `date`, a `datetime`,
    a `numpy.datetime64` or a `pandas.Timestamp`."""
    try:
        if _is_null(dt):
            return None
        elif isinstance(dt, _java_time.LocalDate):
            return dt
        elif isinstance(dt, str):
            return _java_time.parse_local_date(dt)
        elif isinstance(dt, datetime.date):
            return _java_time.LocalDate.of(dt.year, dt.month, dt.day)
        elif _is_a(dt, _numpy(), "datetime64"):
            return to_j_local_date(dt.astype(datetime.date))
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> LocalDate")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_local_time(dt: Optional[LocalTimeLike]) -> Optional[LocalTime]:
    """A time of day from an integer of nanoseconds since midnight, a string
    (`"12:34:56.789"`), a `time`, a `datetime`, a `numpy.datetime64` or a
    `pandas.Timestamp`."""
    try:
        if _is_null(dt):
            return None
        elif isinstance(dt, _java_time.LocalTime):
            return dt
        elif isinstance(dt, int) and not isinstance(dt, bool):
            return _java_time.LocalTime.ofNanoOfDay(dt)
        elif isinstance(dt, str):
            return _java_time.parse_local_time(dt)
        elif _is_a(dt, _pandas(), "Timestamp"):
            return _java_time.LocalTime.of(
                dt.hour, dt.minute, dt.second, dt.microsecond * _NANOS_PER_MICRO + dt.nanosecond
            )
        elif isinstance(dt, datetime.time) or isinstance(dt, datetime.datetime):
            return _java_time.LocalTime.of(dt.hour, dt.minute, dt.second, dt.microsecond * _NANOS_PER_MICRO)
        elif _is_a(dt, _numpy(), "datetime64"):
            # to the microsecond only
            return to_j_local_time(dt.astype(datetime.time))
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> LocalTime")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def _instant_of_long(nanos: int) -> Optional[Instant]:
    """Behavior: `DateTimeUtils#epochNanosToInstant`, called through jpy with a `long`"""
    if not -(2**63) <= nanos < 2**63:
        raise OverflowError("int too big to convert")
    return _java_time.instant_of_nanos(nanos)


def to_j_instant(dt: Optional[InstantLike]) -> Optional[Instant]:
    """An instant from an integer of nanoseconds since the epoch, a string
    (`"2024-01-31T12:34:56 America/New_York"`), a `datetime`, a `numpy.datetime64` or a
    `pandas.Timestamp`. A `datetime` without a time zone is in the server's local time,
    and its conversion is in floating point, so it is exact to about a microsecond."""
    try:
        if _is_null(dt):
            return None
        elif isinstance(dt, _java_time.Instant):
            return dt
        elif isinstance(dt, int) and not isinstance(dt, bool):
            return _instant_of_long(dt)
        elif isinstance(dt, str):
            return _java_time.parse_instant(dt)
        elif isinstance(dt, datetime.datetime):
            epoch_time = dt.timestamp()
            epoch_sec = int(epoch_time)
            nanos = int((epoch_time - epoch_sec) * _NANOS_PER_SECOND)
            return _java_time.Instant.ofEpochSecond(epoch_sec, nanos)
        elif _is_a(dt, _numpy(), "datetime64"):
            epoch_nanos = dt.astype("datetime64[ns]").astype(_numpy().int64)
            return _java_time.Instant(int(epoch_nanos))
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> Instant")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_zdt(dt: Optional[ZonedDateTimeLike]) -> Optional[ZonedDateTime]:
    """A date and time with a time zone from a string, a `datetime`, a `numpy.datetime64`
    or a `pandas.Timestamp`; the server's time zone where the value has none."""
    try:
        if _is_null(dt):
            return None
        elif isinstance(dt, _java_time.ZonedDateTime):
            return dt
        elif isinstance(dt, str):
            return _java_time.parse_zoned_date_time(dt)
        elif isinstance(dt, datetime.datetime):
            instant = to_j_instant(dt)
            tz = to_j_time_zone(dt.tzinfo)
            if tz is None:
                tz = dh_time_zone()
            return _java_time.ZonedDateTime.ofInstant(instant, tz)
        elif _is_a(dt, _numpy(), "datetime64"):
            return _java_time.ZonedDateTime.ofInstant(to_j_instant(dt), _java_time.system_zone())
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> ZonedDateTime")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def _duration_of_long(nanos: int) -> Duration:
    """Behavior: `java.time.Duration#ofNanos`, called through jpy with a `long`"""
    if not -(2**63) <= nanos < 2**63:
        raise OverflowError("int too big to convert")
    return _java_time.Duration.ofNanos(nanos)


def to_j_duration(dt: Optional[DurationLike]) -> Optional[Duration]:
    """A duration from an integer of nanoseconds, a string (`"PT1H30M"`, `"PT1:30:00"`), a
    `timedelta`, a `numpy.timedelta64` or a `pandas.Timedelta`."""
    try:
        if _is_null(dt):
            return None
        elif isinstance(dt, _java_time.Duration):
            return dt
        elif isinstance(dt, int) and not isinstance(dt, bool):
            return _duration_of_long(dt)
        elif isinstance(dt, str):
            return _java_time.parse_duration(dt)
        elif _is_a(dt, _pandas(), "Timedelta"):
            return _duration_of_long(dt.value)
        elif isinstance(dt, datetime.timedelta):
            nanos = int((dt / datetime.timedelta(microseconds=1)) * _NANOS_PER_MICRO)
            return _duration_of_long(nanos)
        elif _is_a(dt, _numpy(), "timedelta64"):
            nanos = int(dt.astype("timedelta64[ns]").astype(_numpy().int64))
            return _duration_of_long(nanos)
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> Duration")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_j_period(dt: Optional[PeriodLike]) -> Optional[Period]:
    """A period of whole days, months or years from a string (`"P1Y2M3D"`, `"P2W"`), a
    `timedelta` or `pandas.Timedelta` of whole days, or a `numpy.timedelta64` in days,
    weeks, months or years."""
    try:
        if _is_null(dt):
            return None
        elif isinstance(dt, _java_time.Period):
            return dt
        elif isinstance(dt, str):
            return _java_time.parse_period(dt)
        elif _is_a(dt, _pandas(), "Timedelta"):
            if dt.seconds or dt.microseconds or dt.nanoseconds:
                raise ValueError(
                    "Unsupported conversion: " + str(type(dt)) + " -> Period: Periods must only be days or weeks"
                )
            elif dt.days:
                return _java_time.Period.ofDays(dt.days)
            else:
                raise ValueError("Unsupported conversion: " + str(type(dt)) + " -> Period")
        elif isinstance(dt, datetime.timedelta):
            if dt.seconds or dt.microseconds:
                raise ValueError(
                    "Unsupported conversion: " + str(type(dt)) + " -> Period: Periods must only be days or weeks"
                )
            elif dt.days:
                return _java_time.Period.ofDays(dt.days)
            else:
                raise ValueError("Unsupported conversion: " + str(type(dt)) + " -> Period")
        elif _is_a(dt, _numpy(), "timedelta64"):
            numpy = _numpy()
            units = numpy.datetime_data(dt)[0]
            value = int(dt.astype(numpy.int64))
            if units == "D":
                return _java_time.Period.ofDays(value)
            elif units == "W":
                return _java_time.Period.ofDays(value * 7)
            elif units == "M":
                return _java_time.Period.ofMonths(value)
            elif units == "Y":
                return _java_time.Period.ofYears(value)
            else:
                raise ValueError(
                    "Unsupported conversion: "
                    + str(type(dt))
                    + " -> Period: numpy.datetime64 must have units of 'D', 'W', 'M', or 'Y'"
                )
        else:
            raise TypeError("Unsupported conversion: " + str(type(dt)) + " -> Period")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


# Conversions: engine to Python


def _java_type(value) -> str:
    """How the reference names the type of a value in an error: a Java object by its
    class."""
    if isinstance(value, _java_time._Value):
        return f"<class '{value._java_name}'>"
    return str(type(value))


def to_date(dt: Union[None, LocalDate, ZonedDateTime]) -> Optional[datetime.date]:
    """A `datetime.date` from a local date, or from the date of a zoned date time."""
    try:
        if dt is None:
            return None
        if isinstance(dt, _java_time.ZonedDateTime):
            dt = dt.local()[0]
        if isinstance(dt, _java_time.LocalDate):
            return datetime.date(dt.year, dt.month, dt.day)
        raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> datetime.date")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_time(dt: Union[None, LocalTime, ZonedDateTime]) -> Optional[datetime.time]:
    """A `datetime.time` from a local time, or from the time of a zoned date time, to the
    microsecond."""
    try:
        if dt is None:
            return None
        if isinstance(dt, _java_time.ZonedDateTime):
            dt = dt.local()[1]
        if isinstance(dt, _java_time.LocalTime):
            hour, minute, second, nano = dt.components()
            return datetime.time(hour, minute, second, nano // _NANOS_PER_MICRO)
        raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> datetime.time")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def _instant_components(dt):
    """`(epoch second, nano)` of an instant or a zoned date time, else `None`."""
    if isinstance(dt, (_java_time.Instant, _java_time.ZonedDateTime)):
        return divmod(dt.nanos, _NANOS_PER_SECOND)
    return None


def to_datetime(dt: Union[None, Instant, ZonedDateTime]) -> Optional[datetime.datetime]:
    """A `datetime.datetime` without a time zone, in the server's local time, from an
    instant or a zoned date time. The conversion is in floating point."""
    try:
        if dt is None:
            return None
        components = _instant_components(dt)
        if components is None:
            raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> datetime.datetime")
        epoch_second, nano = components
        return datetime.datetime.fromtimestamp(epoch_second + (nano / _NANOS_PER_SECOND))
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_pd_timestamp(dt: Union[None, Instant, ZonedDateTime]):
    """A `pandas.Timestamp` without a time zone, in UTC, from an instant or a zoned date
    time."""
    try:
        if dt is None:
            return None
        if _instant_components(dt) is None:
            raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> pandas.Timestamp")
        import pandas

        return pandas.Timestamp(ts_input=_java_time.Instant(dt.nanos).epoch_nanos(), unit="ns")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_np_datetime64(dt: Union[None, Instant, ZonedDateTime]):
    """A `numpy.datetime64` in nanoseconds from an instant or a zoned date time."""
    try:
        if dt is None:
            return None
        if _instant_components(dt) is None:
            # the text is the reference's
            raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> datetime.datetime")
        import numpy

        return numpy.datetime64(dt.nanos, "ns")
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def _days_of_period(dt: _java_time.Period) -> int:
    if dt.years or dt.months:
        raise ValueError(
            "Unsupported conversion: "
            + _java_type(dt)
            + " -> datetime.timedelta: Periods must only be days or weeks"
        )
    return dt.days


def to_timedelta(dt: Union[None, Duration]) -> Optional[datetime.timedelta]:
    """A `datetime.timedelta` from a duration, to the microsecond, or from a period of
    days."""
    try:
        if dt is None:
            return None
        elif isinstance(dt, _java_time.Duration):
            seconds, nano = dt.components()
            return datetime.timedelta(seconds=seconds, microseconds=nano // _NANOS_PER_MICRO)
        elif isinstance(dt, _java_time.Period):
            return datetime.timedelta(days=_days_of_period(dt))
        else:
            raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> datetime.timedelta")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_pd_timedelta(dt: Union[None, Duration]):
    """A `pandas.Timedelta` from a duration or from a period of days."""
    try:
        if dt is None:
            return None
        elif isinstance(dt, _java_time.Duration):
            import pandas

            seconds, nano = dt.components()
            micros, nanos = divmod(nano, _NANOS_PER_MICRO)
            return pandas.Timedelta(seconds=seconds, microseconds=micros, nanoseconds=nanos)
        elif isinstance(dt, _java_time.Period):
            import pandas

            return pandas.Timedelta(days=_days_of_period(dt))
        else:
            raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> pandas.Timedelta")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


def to_np_timedelta64(dt: Union[None, Duration, Period]):
    """A `numpy.timedelta64` from a duration, in nanoseconds, or from a period that has
    only days, only months or only years, in that unit."""
    try:
        if dt is None:
            return None
        elif isinstance(dt, _java_time.Duration):
            import numpy

            return numpy.timedelta64(dt.toNanos(), "ns")
        elif isinstance(dt, _java_time.Period):
            import numpy

            y, m, d = dt.years, dt.months, dt.days
            count = (1 if d else 0) + (1 if m else 0) + (1 if y else 0)
            if count == 0:
                return numpy.timedelta64(0, "D")
            elif count > 1:
                raise ValueError(
                    "Unsupported conversion: "
                    + _java_type(dt)
                    + " -> datetime.timedelta64: Periods must be days, months, or years"
                )
            elif y:
                return numpy.timedelta64(y, "Y")
            elif m:
                return numpy.timedelta64(m, "M")
            else:
                return numpy.timedelta64(d, "D")
        else:
            raise TypeError("Unsupported conversion: " + _java_type(dt) + " -> datetime.timedelta64")
    except ValueError as e:
        raise e
    except TypeError as e:
        raise e
    except Exception as e:
        raise DHError(e) from e


# Utility


def simple_date_format(pattern: str):
    """Not available: the reference returns a `java.text.SimpleDateFormat` for use in
    query strings, and there is no Java object to return."""
    try:
        raise NotPorted("java.text.SimpleDateFormat#<init>")
    except Exception as e:
        raise DHError(e) from e
