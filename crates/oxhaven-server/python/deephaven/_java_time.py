"""The time values of the package: what `deephaven.time` hands out and takes.

The reference package hands Python the Java objects of `java.time`. This server has no JVM,
so each of those classes has a small value class here with the text form (`str()`) and the
parser of its Java counterpart. `deephaven.dtypes` names them as the `j_type` of its time
types, so `isinstance(value, dtypes.Instant.j_type)` works as in the reference.

Of these only an `Instant` can be a column (`oxhaven_contracts::schema::ColumnType`); the
others are values for Python code.

Time zone rules come from Python's `zoneinfo`, which reads the same tz database as Java.
What Java computes and this module does not is refused with `NotPorted`, never guessed.
"""

import datetime
import os
import re
import time as _time

NANOS_PER_SECOND = 1_000_000_000
_NANOS_PER_DAY = 86_400 * NANOS_PER_SECOND
_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1
_INT_MIN, _INT_MAX = -(2**31), 2**31 - 1


class NotPorted(NotImplementedError):
    """A Java behavior this package does not have. The text names the Java method."""

    def __init__(self, java_method: str, what: str = ""):
        super().__init__(f"not ported: {java_method}" + (f": {what}" if what else ""))


# An exception Java would have thrown, with Java's text for it: jpy hands Python a Java
# exception as a `RuntimeError`, and a script may catch it as one.
JavaError = RuntimeError


# Calendar arithmetic over the proleptic Gregorian calendar, for any year: Python's
# `datetime` stops at the years 1 and 9999, `java.time` does not.


def _is_leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _month_length(year: int, month: int) -> int:
    if month == 2:
        return 29 if _is_leap(year) else 28
    return 30 if month in (4, 6, 9, 11) else 31


def _epoch_day(year: int, month: int, day: int) -> int:
    """Days since 1970-01-01. Behavior: `java.time.LocalDate#toEpochDay`"""
    y = year - (month <= 2)
    era = y // 400
    year_of_era = y - era * 400
    day_of_year = (153 * (month + (9 if month <= 2 else -3)) + 2) // 5 + day - 1
    day_of_era = year_of_era * 365 + year_of_era // 4 - year_of_era // 100 + day_of_year
    return era * 146097 + day_of_era - 719468


def _civil(epoch_day: int):
    """`(year, month, day)` of a day count. Behavior: `java.time.LocalDate#ofEpochDay`"""
    z = epoch_day + 719468
    era = z // 146097
    day_of_era = z - era * 146097
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    mp = (5 * day_of_year + 2) // 153
    day = day_of_year - (153 * mp + 2) // 5 + 1
    month = mp + (3 if mp < 10 else -9)
    return year_of_era + era * 400 + (month <= 2), month, day


def _year_text(year: int) -> str:
    """Behavior: `java.time.LocalDate#toString`: four digits, a sign beyond them."""
    if abs(year) < 1000:
        return f"-{-year:04d}" if year < 0 else f"{year:04d}"
    return f"+{year}" if year > 9999 else str(year)


def _fraction_text(nano: int) -> str:
    """Behavior: `java.time.LocalTime#toString`: no digits, or 3, 6 or 9 of them."""
    if nano == 0:
        return ""
    if nano % 1_000_000 == 0:
        return f".{nano // 1_000_000:03d}"
    if nano % 1000 == 0:
        return f".{nano // 1000:06d}"
    return f".{nano:09d}"


def _time_text(nano_of_day: int, always_seconds: bool) -> str:
    seconds, nano = divmod(nano_of_day, NANOS_PER_SECOND)
    text = f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}"
    if always_seconds or seconds % 60 or nano:
        text += f":{seconds % 60:02d}{_fraction_text(nano)}"
    return text


class _Value:
    """A value compared by its fields, shown as the reference shows a Java object."""

    __slots__ = ()
    _java_name = ""

    def _key(self):
        return tuple(getattr(self, name) for name in self.__slots__)

    def __eq__(self, other):
        return type(other) is type(self) and other._key() == self._key()

    def __hash__(self):
        return hash((type(self).__name__, self._key()))

    def __repr__(self):
        return f"{self._java_name}({self})"

    def toString(self) -> str:
        return str(self)


# ------------------------------------------------------------------------------ time zones

_OFFSET_PREFIXES = ("UTC", "GMT", "UT")
# Names in the tz database that Python reads which `java.time.ZoneId#getAvailableZoneIds`
# of the reference's JDK does not list: its three-letter names are not zone ids
# (`ZoneId#SHORT_IDS`), and the rest are not zones.
_NOT_JAVA_REGIONS = frozenset({"EST", "HST", "MST", "ROC", "Factory", "localtime", "posixrules"})
_regions = None


def _region_ids() -> frozenset:
    """The names of the regions of the tz database, as Java lists them."""
    global _regions
    if _regions is None:
        import zoneinfo

        _regions = frozenset(zoneinfo.available_timezones()) - _NOT_JAVA_REGIONS
    return _regions


def _offset_id(seconds: int) -> str:
    """Behavior: `java.time.ZoneOffset#getId`"""
    if seconds == 0:
        return "Z"
    size = abs(seconds)
    text = f"{'-' if seconds < 0 else '+'}{size // 3600:02d}:{size // 60 % 60:02d}"
    return text + (f":{size % 60:02d}" if size % 60 else "")


def _parse_offset(text: str) -> int:
    """The seconds of `+h`, `+hh`, `+hhmm`, `+hh:mm`, `+hhmmss` or `+hh:mm:ss`, or of `Z`.

    Behavior: `java.time.ZoneOffset#of`
    """
    if text == "Z":
        return 0
    invalid = JavaError(f"java.time.DateTimeException: Invalid ID for ZoneOffset, invalid format: {text}")
    match = re.fullmatch(r"[+-](?:(\d)|(\d\d)(?:(:?)(\d\d)(?:\3(\d\d))?)?)", text)
    if match is None:
        raise invalid
    single, hours, _, minutes, seconds = match.groups()
    hours, minutes, seconds = int(single or hours), int(minutes or 0), int(seconds or 0)
    if hours > 18 or minutes > 59 or seconds > 59 or (hours == 18 and (minutes or seconds)):
        raise JavaError(f"java.time.DateTimeException: Zone offset not in valid range: {text}")
    total = hours * 3600 + minutes * 60 + seconds
    return -total if text[0] == "-" else total


class ZoneId(_Value):
    """A time zone: a region of the tz database or a fixed offset from UTC.

    Behavior: `java.time.ZoneId`
    """

    __slots__ = ("id", "_fixed", "_rules")
    _java_name = "java.time.ZoneId"

    def __init__(self, zone_id: str):
        self.id = zone_id
        # the offset in seconds of a zone without rules, or the rules of a region
        self._fixed = None
        self._rules = None

    def _key(self):
        return (self.id,)

    @classmethod
    def of(cls, zone_id: str) -> "ZoneId":
        """Behavior: `java.time.ZoneId#of(String)`"""
        if zone_id == "Z" or zone_id[:1] in ("+", "-"):
            zone = cls(_offset_id(_parse_offset(zone_id)))
            zone._fixed = _parse_offset(zone_id)
            return zone
        if zone_id in _OFFSET_PREFIXES:
            zone = cls(zone_id)
            zone._fixed = 0
            return zone
        for prefix in _OFFSET_PREFIXES:
            if zone_id.startswith(prefix) and zone_id[len(prefix) : len(prefix) + 1] in ("+", "-"):
                seconds = _parse_offset(zone_id[len(prefix) :])
                zone = cls(prefix + (_offset_id(seconds) if seconds else ""))
                zone._fixed = seconds
                return zone
        import zoneinfo

        if zone_id.startswith("SystemV/"):
            # in the JDK's tz database and not in the one Python reads
            raise NotPorted("java.time.ZoneId#of", f"the time zone {zone_id}")
        unknown = JavaError(f"java.time.zone.ZoneRulesException: Unknown time-zone ID: {zone_id}")
        if zone_id not in _region_ids():
            raise unknown
        try:
            rules = zoneinfo.ZoneInfo(zone_id)
        except Exception:
            raise unknown from None
        zone = cls(zone_id)
        zone._rules = rules
        return zone

    @property
    def is_offset(self) -> bool:
        """Whether this is a `java.time.ZoneOffset`: an offset without a name."""
        return self._fixed is not None and (self.id == "Z" or self.id[0] in "+-")

    def _datetime(self, seconds: int) -> datetime.datetime:
        try:
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
        except OverflowError:
            raise NotPorted(
                "java.time.zone.ZoneRules#getOffset", "a region's offset outside the years 1 to 9999"
            ) from None

    def offset_at(self, epoch_second: int) -> int:
        """The offset from UTC, in seconds, at an instant.

        Behavior: `java.time.zone.ZoneRules#getOffset(Instant)`
        """
        if self._fixed is not None:
            return self._fixed
        utc = self._datetime(epoch_second).replace(tzinfo=datetime.timezone.utc)
        return int(utc.astimezone(self._rules).utcoffset().total_seconds())

    def offset_of_local(self, local_second: int) -> int:
        """The offset a local date and time has in this zone: in a gap the offset before
        it, which moves the time later by the length of the gap, and in an overlap the
        earlier offset.

        Behavior: `java.time.ZonedDateTime#ofLocal` without a preferred offset
        """
        if self._fixed is not None:
            return self._fixed
        local = self._datetime(local_second).replace(tzinfo=self._rules, fold=0)
        return int(local.utcoffset().total_seconds())

    def getId(self) -> str:
        return self.id

    def __str__(self):
        return self.id


# Behavior: `props/configs/src/main/resources/default_time_zone_aliases.csv` of the
# reference, which `io.deephaven.time.TimeZoneAliases` loads
_aliases = {
    "ET": "America/New_York",
    "CT": "America/Chicago",
    "MT": "America/Denver",
    "PT": "America/Los_Angeles",
    "HK": "Asia/Hong_Kong",
    "JP": "Asia/Tokyo",
    "LON": "Europe/London",
}


def _zone_exists(zone_id: str) -> bool:
    try:
        ZoneId.of(zone_id)
    except Exception:
        return False
    return True


def alias_add(alias: str, zone_id: str) -> None:
    """Behavior: `io.deephaven.time.TimeZoneAliases.Cache#addAlias`"""
    if alias in _aliases or alias in _region_ids():
        raise JavaError(
            f"java.lang.IllegalArgumentException: Time zone already exists with the alias name: alias={alias}"
        )
    if zone_id in _aliases.values():
        raise JavaError(
            f"java.lang.IllegalArgumentException: Alias for time zone is already present: time_zone={zone_id}"
        )
    if not _zone_exists(zone_id):
        raise JavaError(f"java.lang.IllegalArgumentException: Invalid time zone id: {zone_id}")
    _aliases[alias] = zone_id


def alias_rm(alias: str) -> bool:
    """Behavior: `io.deephaven.time.TimeZoneAliases.Cache#rmAlias`. Java also removes a
    zone of the tz database from its list of known names when asked to, and still parses
    it afterwards; that has no effect to reproduce."""
    return _aliases.pop(alias, None) is not None


def zone_of(text: str) -> ZoneId:
    """A zone by its name, Deephaven's aliases included.

    Behavior: `io.deephaven.time.TimeZoneAliases#zoneId`, `java.time.ZoneId#of(String, Map)`
    """
    return ZoneId.of(_aliases.get(text, text))


def parse_time_zone(text: str) -> ZoneId:
    """Behavior: `io.deephaven.time.DateTimeUtils#parseTimeZone`"""
    try:
        return zone_of(text)
    except NotPorted:
        raise
    except Exception:
        raise JavaError(
            f"io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse time zone: {text}"
        ) from None


def _parse_time_zone_quiet(text: str):
    """Behavior: `io.deephaven.time.DateTimeUtils#parseTimeZoneQuiet`"""
    if len(text) <= 1:
        return None
    try:
        return parse_time_zone(text)
    except NotPorted:
        raise
    except JavaError:
        return None


def system_zone() -> ZoneId:
    """The time zone of the server's machine: `TZ`, else what `/etc/localtime` links to.

    Behavior: `java.time.ZoneId#systemDefault`. A JVM also honours `-Duser.timezone` and
    compares the content of `/etc/localtime` with the database when it is not a link; here
    a machine without either name is taken to be on UTC, as a JVM falls back to GMT.
    """
    name = os.environ.get("TZ", "").lstrip(":")
    if not name:
        try:
            name = os.path.realpath("/etc/localtime").partition("zoneinfo/")[2]
        except OSError:
            name = ""
    if name and _zone_exists(name):
        return ZoneId.of(name)
    return ZoneId.of("UTC")


# --------------------------------------------------------------------------- local values


class LocalDate(_Value):
    """A date without a time zone. Behavior: `java.time.LocalDate`"""

    __slots__ = ("year", "month", "day")
    _java_name = "java.time.LocalDate"

    def __init__(self, year: int, month: int, day: int):
        _check("Year", year, -999_999_999, 999_999_999)
        _check("MonthOfYear", month, 1, 12)
        _check("DayOfMonth", day, 1, 31)
        if day > _month_length(year, month):
            raise JavaError(f"java.time.DateTimeException: Invalid date '{year}-{month}-{day}'")
        self.year, self.month, self.day = year, month, day

    @classmethod
    def of(cls, year: int, month: int, day: int) -> "LocalDate":
        return cls(year, month, day)

    def to_epoch_day(self) -> int:
        return _epoch_day(self.year, self.month, self.day)

    def getYear(self) -> int:
        return self.year

    def getMonthValue(self) -> int:
        return self.month

    def getDayOfMonth(self) -> int:
        return self.day

    def getDayOfWeek(self) -> "DayOfWeek":
        # 1970-01-01 was a Thursday, day 4
        return DayOfWeek.of((self.to_epoch_day() + 3) % 7 + 1)

    def toEpochDay(self) -> int:
        return self.to_epoch_day()

    def __str__(self):
        return f"{_year_text(self.year)}-{self.month:02d}-{self.day:02d}"


class LocalTime(_Value):
    """A time of day without a time zone. Behavior: `java.time.LocalTime`"""

    __slots__ = ("nano_of_day",)
    _java_name = "java.time.LocalTime"

    def __init__(self, nano_of_day: int):
        _check("NanoOfDay", nano_of_day, 0, _NANOS_PER_DAY - 1)
        self.nano_of_day = nano_of_day

    @classmethod
    def of(cls, hour: int, minute: int, second: int = 0, nano: int = 0) -> "LocalTime":
        _check("HourOfDay", hour, 0, 23)
        _check("MinuteOfHour", minute, 0, 59)
        _check("SecondOfMinute", second, 0, 59)
        _check("NanoOfSecond", nano, 0, NANOS_PER_SECOND - 1)
        return cls(((hour * 60 + minute) * 60 + second) * NANOS_PER_SECOND + nano)

    @classmethod
    def ofNanoOfDay(cls, nano_of_day: int) -> "LocalTime":
        return cls(nano_of_day)

    def components(self):
        """`(hour, minute, second, nano)`"""
        seconds, nano = divmod(self.nano_of_day, NANOS_PER_SECOND)
        return seconds // 3600, seconds // 60 % 60, seconds % 60, nano

    def getHour(self) -> int:
        return self.components()[0]

    def getMinute(self) -> int:
        return self.components()[1]

    def getSecond(self) -> int:
        return self.components()[2]

    def getNano(self) -> int:
        return self.components()[3]

    def toNanoOfDay(self) -> int:
        return self.nano_of_day

    def __str__(self):
        return _time_text(self.nano_of_day, False)


def _check(field: str, value: int, least: int, most: int) -> None:
    """Behavior: `java.time.temporal.ValueRange#checkValidValue`"""
    if not least <= value <= most:
        raise JavaError(
            f"java.time.DateTimeException: Invalid value for {field} "
            f"(valid values {least} - {most}): {value}"
        )


_DAY_NAMES = ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY")


class DayOfWeek(_Value):
    """A day of the week, Monday 1 to Sunday 7. Behavior: `java.time.DayOfWeek`"""

    __slots__ = ("value",)
    _java_name = "java.time.DayOfWeek"

    def __init__(self, value: int):
        self.value = value

    @classmethod
    def of(cls, value: int) -> "DayOfWeek":
        if not 1 <= value <= 7:
            raise JavaError(f"java.time.DateTimeException: Invalid value for DayOfWeek: {value}")
        return _DAYS[value - 1]

    @classmethod
    def valueOf(cls, name: str) -> "DayOfWeek":
        if name not in _DAY_NAMES:
            raise JavaError(f"java.lang.IllegalArgumentException: No enum constant java.time.DayOfWeek.{name}")
        return _DAYS[_DAY_NAMES.index(name)]

    @classmethod
    def values(cls) -> list:
        return list(_DAYS)

    def getValue(self) -> int:
        return self.value

    def ordinal(self) -> int:
        return self.value - 1

    def name(self) -> str:
        return _DAY_NAMES[self.value - 1]

    def compareTo(self, other: "DayOfWeek") -> int:
        return self.value - other.value

    def equals(self, other) -> bool:
        return self == other

    def __str__(self):
        return self.name()


_DAYS = tuple(DayOfWeek(value) for value in range(1, 8))
for _day in _DAYS:
    setattr(DayOfWeek, _DAY_NAMES[_day.value - 1], _day)
del _day


# ------------------------------------------------------------------------------- instants


class Instant(_Value):
    """A point in time: nanoseconds since 1970-01-01T00:00:00Z.

    Behavior: `java.time.Instant`. A column holds an instant as a 64-bit count of
    nanoseconds, so only the years 1677 to 2262 fit one; this value holds any.
    """

    __slots__ = ("nanos",)
    _java_name = "java.time.Instant"

    def __init__(self, nanos: int):
        self.nanos = nanos

    @classmethod
    def ofEpochSecond(cls, epoch_second: int, nano: int = 0) -> "Instant":
        return cls(epoch_second * NANOS_PER_SECOND + nano)

    def getEpochSecond(self) -> int:
        return self.nanos // NANOS_PER_SECOND

    def getNano(self) -> int:
        return self.nanos % NANOS_PER_SECOND

    def toEpochMilli(self) -> int:
        return self.nanos // 1_000_000

    def epoch_nanos(self) -> int:
        """The nanoseconds as a column holds them.

        Behavior: `io.deephaven.time.DateTimeUtils#epochNanos(Instant)`
        """
        if not _LONG_MIN < self.nanos <= _LONG_MAX:
            raise JavaError("java.lang.ArithmeticException: long overflow")
        return self.nanos

    def __str__(self):
        seconds, nano = divmod(self.nanos, NANOS_PER_SECOND)
        days, second_of_day = divmod(seconds, 86_400)
        year, month, day = _civil(days)
        if year < 0:
            raise NotPorted("java.time.Instant#toString", "a year before 0")
        clock = _time_text(second_of_day * NANOS_PER_SECOND + nano, True)
        return f"{_year_text(year)}-{month:02d}-{day:02d}T{clock}Z"


class ZonedDateTime(_Value):
    """An instant with the time zone to show it in. Behavior: `java.time.ZonedDateTime`"""

    __slots__ = ("nanos", "zone")
    _java_name = "java.time.ZonedDateTime"

    def __init__(self, nanos: int, zone: ZoneId):
        self.nanos = nanos
        self.zone = zone

    @classmethod
    def ofInstant(cls, instant: Instant, zone: ZoneId) -> "ZonedDateTime":
        return cls(instant.nanos, zone)

    @classmethod
    def of_local(cls, epoch_day: int, nano_of_day: int, zone: ZoneId) -> "ZonedDateTime":
        """Behavior: `java.time.LocalDateTime#atZone`"""
        local_second = epoch_day * 86_400 + nano_of_day // NANOS_PER_SECOND
        offset = zone.offset_of_local(local_second)
        return cls((local_second - offset) * NANOS_PER_SECOND + nano_of_day % NANOS_PER_SECOND, zone)

    def offset(self) -> int:
        """The offset from UTC in seconds."""
        return self.zone.offset_at(self.nanos // NANOS_PER_SECOND)

    def toInstant(self) -> Instant:
        return Instant(self.nanos)

    def local(self):
        """`(LocalDate, LocalTime)` of this date time in its zone."""
        seconds, nano = divmod(self.nanos, NANOS_PER_SECOND)
        days, second_of_day = divmod(seconds + self.offset(), 86_400)
        return LocalDate(*_civil(days)), LocalTime(second_of_day * NANOS_PER_SECOND + nano)

    def __str__(self):
        date, clock = self.local()
        text = f"{date}T{clock}{_offset_id(self.offset())}"
        return text if self.zone.is_offset else f"{text}[{self.zone}]"


# ---------------------------------------------------------------- durations and periods


class Duration(_Value):
    """A length of time in nanoseconds. Behavior: `java.time.Duration`"""

    __slots__ = ("nanos",)
    _java_name = "java.time.Duration"

    def __init__(self, nanos: int):
        self.nanos = nanos

    @classmethod
    def ofNanos(cls, nanos: int) -> "Duration":
        return cls(nanos)

    def toNanos(self) -> int:
        if not _LONG_MIN <= self.nanos <= _LONG_MAX:
            raise JavaError("java.lang.ArithmeticException: long overflow")
        return self.nanos

    def components(self):
        """`(seconds, nano)` with the nanoseconds from 0 up, as Java holds a duration."""
        return divmod(self.nanos, NANOS_PER_SECOND)

    def __str__(self):
        """Behavior: `java.time.Duration#toString`"""
        if self.nanos == 0:
            return "PT0S"
        seconds, nano = divmod(self.nanos, NANOS_PER_SECOND)
        effective = seconds + 1 if seconds < 0 and nano > 0 else seconds
        size = abs(effective)
        sign = -1 if effective < 0 else 1
        hours, minutes, secs = sign * (size // 3600), sign * (size // 60 % 60), sign * (size % 60)
        text = "PT" + (f"{hours}H" if hours else "") + (f"{minutes}M" if minutes else "")
        if secs == 0 and nano == 0 and len(text) > 2:
            return text
        text += "-0" if seconds < 0 and nano > 0 and secs == 0 else str(secs)
        if nano > 0:
            digits = NANOS_PER_SECOND - nano if seconds < 0 else nano
            text += "." + f"{digits:09d}".rstrip("0")
        return text + "S"


class Period(_Value):
    """A length of time in years, months and days. Behavior: `java.time.Period`"""

    __slots__ = ("years", "months", "days")
    _java_name = "java.time.Period"

    def __init__(self, years: int, months: int, days: int):
        self.years, self.months, self.days = years, months, days

    @classmethod
    def ofDays(cls, days: int) -> "Period":
        return cls(0, 0, _to_int(days))

    @classmethod
    def ofMonths(cls, months: int) -> "Period":
        return cls(0, _to_int(months), 0)

    @classmethod
    def ofYears(cls, years: int) -> "Period":
        return cls(_to_int(years), 0, 0)

    def __str__(self):
        if not (self.years or self.months or self.days):
            return "P0D"
        return (
            "P"
            + (f"{self.years}Y" if self.years else "")
            + (f"{self.months}M" if self.months else "")
            + (f"{self.days}D" if self.days else "")
        )


def _to_int(value: int) -> int:
    """A Python `int` as jpy passes it to a Java `int` parameter: its low 32 bits."""
    if not _LONG_MIN <= value <= _LONG_MAX:
        raise OverflowError("int too big to convert")
    return (value + 2**31) % 2**32 - 2**31


# -------------------------------------------------------------------------------- parsing

_LONG_PATTERN = re.compile(r"-?\d{1,19}")
_DATE_TZ_PATTERN = re.compile(r"(?P<date>\d{4}-\d\d-\d\d)[tT]? (?P<zone>[a-zA-Z_/]+)")
_TIME_DURATION_PATTERN = re.compile(
    r"(-?)[pP][tT](-?)([0-9]+):([0-9]+)(:[0-9]+)?(\.[0-9]{1,9})?"
)
_DURATION_PATTERN = re.compile(
    r"([-+]?)P(?:([-+]?[0-9]+)D)?"
    r"(T(?:([-+]?[0-9]+)H)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?",
    re.IGNORECASE,
)
_PERIOD_PATTERN = re.compile(
    r"([-+]?)P(?:([-+]?[0-9]+)Y)?(?:([-+]?[0-9]+)M)?(?:([-+]?[0-9]+)W)?(?:([-+]?[0-9]+)D)?",
    re.IGNORECASE,
)
# `DateTimeUtils.FORMATTER_ISO_LOCAL_DATE`: a year of 4 to 10 digits, signed with `-`, or
# with `+` when it has more than 4; a month and a day of 1 or 2 digits.
_PERMISSIVE_DATE = r"(?P<year>-\d{4,10}|\+\d{5,10}|\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
# `DateTimeUtils.FORMATTER_ISO_LOCAL_TIME`
_PERMISSIVE_TIME = r"(?P<hour>\d{1,2}):(?P<minute>\d{1,2})(?::(?P<second>\d{1,2})(?:\.(?P<fraction>\d{0,9}))?)?"
_LOCAL_DATE = re.compile(_PERMISSIVE_DATE)
_LOCAL_TIME = re.compile(_PERMISSIVE_TIME)
_LOCAL_DATE_TIME = re.compile(_PERMISSIVE_DATE + "[tT]" + _PERMISSIVE_TIME)
# `DateTimeFormatter.ISO_ZONED_DATE_TIME`
_ISO_ZONED = re.compile(
    r"(?P<year>-\d{4,10}|\+\d{5,10}|\d{4})-(?P<month>\d\d)-(?P<day>\d\d)[tT]"
    r"(?P<hour>\d\d):(?P<minute>\d\d)(?::(?P<second>\d\d)(?:\.(?P<fraction>\d{0,9}))?)?"
    r"(?P<offset>[zZ]|[+-]\d\d:\d\d(?::\d\d)?)(?:\[(?P<region>[^\]]+)\])?"
)
# A date and time followed by a sign and digits that are not an offset of the form above.
_ISO_OTHER_OFFSET = re.compile(r"[-+]?\d{4,10}-\d\d-\d\d[tT]\d\d:\d\d[\d:.]*[+-][\d:]+(?:\[[^\]]+\])?")


def _date_of(match, smart: bool) -> LocalDate:
    """The date of a match. `smart` moves a day past the end of its month to the end, as
    `java.time.format.ResolverStyle#SMART` does; the strict style refuses it."""
    year, month, day = int(match["year"]), int(match["month"]), int(match["day"])
    _check("MonthOfYear", month, 1, 12)
    _check("DayOfMonth", day, 1, 31)
    if smart:
        day = min(day, _month_length(year, month))
    return LocalDate(year, month, day)


def _time_of(match, smart: bool):
    """The time of a match, and the days to add to its date. The smart resolver takes
    `24:00` as the end of the day, which is the start of the next (observed on the
    reference); what it does with an hour of 24 and anything after it, and what the strict
    resolver does with 24, is not ported.

    Behavior: `java.time.format.Parsed#resolveTimeFields`
    """
    hour, minute, second = int(match["hour"]), int(match["minute"]), int(match["second"] or 0)
    fraction = int((match["fraction"] or "").ljust(9, "0"))
    if hour == 24:
        if smart and minute == 0 and second == 0 and fraction == 0:
            return LocalTime(0), 1
        raise NotPorted("java.time.format.Parsed#resolveTimeFields", "the hour 24")
    return LocalTime.of(hour, minute, second, fraction), 0


def parse_local_date(text: str) -> LocalDate:
    """Behavior: `io.deephaven.time.DateTimeUtils#parseLocalDate`"""
    try:
        match = _LOCAL_DATE.fullmatch(text)
        if match is None:
            raise JavaError(
                f"java.time.format.DateTimeParseException: Text '{text}' could not be parsed"
            )
        return _date_of(match, True)
    except NotPorted:
        raise
    except JavaError as error:
        raise _parse_error("local date", text, error) from None


def parse_local_time(text: str) -> LocalTime:
    """Behavior: `io.deephaven.time.DateTimeUtils#parseLocalTime`"""
    try:
        match = _LOCAL_TIME.fullmatch(text)
        if match is None:
            raise JavaError(
                f"java.time.format.DateTimeParseException: Text '{text}' could not be parsed"
            )
        return _time_of(match, True)[0]
    except NotPorted:
        raise
    except JavaError as error:
        raise _parse_error("local time", text, error) from None


def _parse_error(what: str, text: str, cause: Exception) -> JavaError:
    return JavaError(
        f"io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse {what}: {text}"
        f"\ncaused by {cause}"
    )


def _parse_iso_zoned(text: str):
    """Behavior: `java.time.ZonedDateTime#parse(CharSequence)`; `None` for text that is not
    of that form."""
    match = _ISO_ZONED.fullmatch(text)
    if match is None:
        if _ISO_OTHER_OFFSET.fullmatch(text):
            # the offset is parsed leniently, and takes more forms than `+hh:mm[:ss]`
            raise NotPorted("java.time.ZonedDateTime#parse", f"the offset of {text}")
        return None
    try:
        date, (clock, _) = _date_of(match, False), _time_of(match, False)
        offset = _parse_offset(match["offset"].upper())
        region = ZoneId.of(match["region"]) if match["region"] else None
    except NotPorted:
        raise
    except JavaError:
        return None
    local_second = date.to_epoch_day() * 86_400 + clock.nano_of_day // NANOS_PER_SECOND
    nanos = (local_second - offset) * NANOS_PER_SECOND + clock.nano_of_day % NANOS_PER_SECOND
    if region is None:
        return ZonedDateTime(nanos, ZoneId.of(_offset_id(offset)))
    if region.offset_at(local_second - offset) != offset:
        raise NotPorted("java.time.ZonedDateTime#parse", "an offset that differs from its region's")
    return ZonedDateTime(nanos, region)


def parse_zoned_date_time(text: str) -> ZonedDateTime:
    """Behavior: `io.deephaven.time.DateTimeUtils#parseZonedDateTime`"""
    zoned = _parse_iso_zoned(text)
    if zoned is not None:
        return zoned
    try:
        match = _DATE_TZ_PATTERN.fullmatch(text)
        if match is not None:
            zone = _parse_time_zone_quiet(match["zone"])
            if zone is None:
                raise JavaError(f"java.lang.RuntimeException: No matching time zone: '{match['zone']}'")
            date = _date_of(_LOCAL_DATE.fullmatch(match["date"]), True)
            return ZonedDateTime.of_local(date.to_epoch_day(), 0, zone)
        local, space, zone_text = text.partition(" ")
        if not space:
            raise JavaError("java.lang.RuntimeException: No time zone provided")
        zone = _parse_time_zone_quiet(zone_text)
        if zone is None:
            raise JavaError(f"java.lang.RuntimeException: No matching time zone: {zone_text}")
        match = _LOCAL_DATE_TIME.fullmatch(local)
        if match is None:
            raise JavaError(
                f"java.time.format.DateTimeParseException: Text '{local}' could not be parsed"
            )
        date, (clock, days) = _date_of(match, True), _time_of(match, True)
        return ZonedDateTime.of_local(date.to_epoch_day() + days, clock.nano_of_day, zone)
    except NotPorted:
        raise
    except JavaError as error:
        raise _parse_error("zoned date time", text, error) from None


# `io.deephaven.base.clock.TimeConstants.MICROTIME_THRESHOLD`
_MICROTIME_THRESHOLD = 9_309_341_000_000


def epoch_auto_to_nanos(offset: int) -> int:
    """An offset from the epoch whose unit is told from its size: seconds, milliseconds,
    microseconds or nanoseconds.

    Behavior: `io.deephaven.time.DateTimeUtils#epochAutoToEpochNanos`
    """
    if offset == _LONG_MIN:
        return offset
    size = abs(offset)
    if size > 1000 * _MICROTIME_THRESHOLD:
        scale = 1
    elif size > _MICROTIME_THRESHOLD:
        scale = 1000
    elif size > _MICROTIME_THRESHOLD // 1000:
        scale = 1_000_000
    else:
        scale = NANOS_PER_SECOND
    # Java's multiplication, which wraps: a number on a threshold is beyond a long in the
    # smaller unit
    return (offset * scale + 2**63) % 2**64 - 2**63


def instant_of_nanos(nanos: int):
    """An instant from the nanoseconds of a column; the null of `long` is no instant.

    Behavior: `io.deephaven.time.DateTimeUtils#epochNanosToInstant`
    """
    return None if nanos == _LONG_MIN else Instant(nanos)


def parse_instant(text: str):
    """Behavior: `io.deephaven.time.DateTimeUtils#parseInstant`"""
    try:
        if _LONG_PATTERN.fullmatch(text):
            number = int(text)
            if not _LONG_MIN <= number <= _LONG_MAX:
                raise JavaError(f'java.lang.NumberFormatException: For input string: "{text}"')
            return instant_of_nanos(epoch_auto_to_nanos(number))
        return parse_zoned_date_time(text).toInstant()
    except NotPorted:
        raise
    except JavaError as error:
        raise _parse_error("instant", text, error) from None


def parse_duration(text: str) -> Duration:
    """Behavior: `io.deephaven.time.DateTimeUtils#parseDuration`,
    `java.time.Duration#parse`"""
    try:
        match = _TIME_DURATION_PATTERN.fullmatch(text)
        if match is not None:
            sign1, sign2, hours, minutes, seconds, fraction = match.groups()
            nanos = int(hours) * 3600 + int(minutes) * 60 + int((seconds or ":0")[1:])
            nanos = nanos * NANOS_PER_SECOND + int((fraction or ".")[1:].ljust(9, "0"))
            if nanos > _LONG_MAX:
                # Java's arithmetic wraps here
                raise NotPorted(
                    "io.deephaven.time.DateTimeUtils#parseDuration",
                    "a duration beyond a long of nanoseconds",
                )
            return Duration(nanos * (-1 if sign1 else 1) * (-1 if sign2 else 1))
        failed = JavaError("java.time.format.DateTimeParseException: Text cannot be parsed to a Duration")
        match = _DURATION_PATTERN.fullmatch(text)
        if match is None or (match[3] or "").upper() == "T":
            raise failed
        negate, days, _, hours, minutes, seconds, fraction = match.groups()
        if days is None and hours is None and minutes is None and seconds is None:
            raise failed
        overflow = JavaError(
            "java.time.format.DateTimeParseException: Text cannot be parsed to a Duration: overflow"
        )

        def exact(value: int) -> int:
            # Behavior: `Math#multiplyExact`, `Math#addExact`
            if not _LONG_MIN <= value <= _LONG_MAX:
                raise overflow
            return value

        total = exact(int(minutes or 0) * 60) + exact(int(seconds or 0))
        total = exact(exact(int(hours or 0) * 3600) + exact(total))
        total = exact(exact(int(days or 0) * 86_400) + total)
        nanos = int((fraction or "").ljust(9, "0"))
        if seconds is not None and seconds.startswith("-"):
            nanos = -nanos
        total = exact(total + nanos // NANOS_PER_SECOND)
        nanos = total * NANOS_PER_SECOND + nanos % NANOS_PER_SECOND
        if negate == "-":
            exact(-total - (1 if nanos % NANOS_PER_SECOND else 0))
            nanos = -nanos
        return Duration(nanos)
    except NotPorted:
        raise
    except JavaError as error:
        raise _parse_error("duration", text, error) from None


def parse_period(text: str) -> Period:
    """Behavior: `io.deephaven.time.DateTimeUtils#parsePeriod`, `java.time.Period#parse`"""
    try:
        failed = JavaError("java.time.format.DateTimeParseException: Text cannot be parsed to a Period")
        match = _PERIOD_PATTERN.fullmatch(text)
        if match is None or all(part is None for part in match.groups()[1:]):
            raise failed
        sign = -1 if match[1] == "-" else 1
        years, months, weeks, days = (sign * int(part or 0) for part in match.groups()[1:])
        days += weeks * 7
        if not all(_INT_MIN <= part <= _INT_MAX for part in (years, months, weeks, days)):
            raise failed
        return Period(years, months, days)
    except NotPorted:
        raise
    except JavaError as error:
        raise _parse_error("period", text, error) from None


def now_nanos() -> int:
    """The nanoseconds since the epoch of the server's clock.

    Behavior: `io.deephaven.base.clock.Clock#currentTimeNanos` of the system clock; the
    engine has no other clock.
    """
    return _time.time_ns()
