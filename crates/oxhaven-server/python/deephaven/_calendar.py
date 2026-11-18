"""The business calendar a script gets from `deephaven.calendar`, and what its methods
hand back.

The reference hands a script the Java `BusinessCalendar` itself, and the script calls its
Java methods through jpy. Here a `BusinessCalendar` holds a calendar of the server
(`oxhaven_expr::calendar`, through `_oxhaven.CalendarHandle`) and has the same methods,
by their Java names. What they take and give are the package's values of `java.time`
(`_java_time`), and the collections Java gives are small classes with the methods of
`java.util` that a script calls on them. A Java array is given as a Python list.

Java methods are overloaded by the types of their arguments, and jpy chooses the overload
by the Python values a script passes; `_choose` makes the same choice for the cases seen
on the reference: a `str` is a `String`, `None` fits any class, a `bool` and an `int` fit
`boolean` and `int` both, an `int` is cut to 32 bits. No fit is jpy's "no matching Java
method overloads found", two equally good ones its "ambiguous Java method call".

Behavior: `io.deephaven.time.calendar.BusinessCalendar`, `io.deephaven.time.calendar.Calendar`,
`io.deephaven.time.calendar.CalendarDay`, `io.deephaven.time.calendar.TimeRange`
"""

from deephaven import _java_time
from deephaven._java_time import JavaError, NotPorted

_NULL_INT = -(2**31)
_NULL_LONG = -(2**63)
_NULL_DOUBLE = -1.7976931348623157e308
_LONG_MIN, _LONG_MAX = -(2**63), 2**63 - 1


def _native():
    import _oxhaven

    return _oxhaven


# --------------------------------------------------------------------- choosing an overload

_CLASSES = {
    "LocalDate": _java_time.LocalDate,
    "LocalTime": _java_time.LocalTime,
    "Instant": _java_time.Instant,
    "ZonedDateTime": _java_time.ZonedDateTime,
    "DayOfWeek": _java_time.DayOfWeek,
    "ZoneId": _java_time.ZoneId,
    # no value of the package is one: only `None` fits
    "LocalDateTime": type("LocalDateTime", (), {}),
}


def _fit(kind: str, value) -> int:
    """How well a value fits a parameter: 0 not at all."""
    if kind in ("int", "boolean"):
        if isinstance(value, bool):
            return 10 if kind == "boolean" else 5
        if isinstance(value, int):
            return 10 if kind == "int" else 5
        return 0
    if value is None:
        return 1
    if kind == "String":
        return 10 if isinstance(value, str) else 0
    if kind == "BusinessCalendar":
        return 10 if isinstance(value, BusinessCalendar) else 0
    return 10 if isinstance(value, _CLASSES[kind]) else 0


def _converted(kind: str, value):
    if kind == "int":
        # jpy keeps the low 32 bits
        return (int(value) + 2**31) % 2**32 - 2**31
    if kind == "boolean":
        return bool(value)
    return value


def _choose(overloads, args):
    """The overload of `overloads` (`(parameter kinds, function)`) that jpy calls for
    `args`, and the arguments as Java receives them."""
    best, best_fit = [], 0
    for kinds, function in overloads:
        if len(kinds) != len(args):
            continue
        fits = [_fit(kind, arg) for kind, arg in zip(kinds, args)]
        if 0 in fits:
            continue
        fit = sum(fits) if fits else 1
        if fit > best_fit:
            best, best_fit = [(kinds, function)], fit
        elif fit == best_fit:
            best.append((kinds, function))
    if not best:
        raise JavaError("no matching Java method overloads found")
    if len(best) > 1:
        raise JavaError("ambiguous Java method call, too many matching method overloads found")
    kinds, function = best[0]
    return function, [_converted(kind, arg) for kind, arg in zip(kinds, args)]


def _java(*overloads):
    """A method of overloads, each `(parameter kinds..., function)`, or a function alone
    for the overload without parameters."""
    table = [((), overload) if callable(overload) else (tuple(overload[:-1]), overload[-1]) for overload in overloads]

    def method(self, *args):
        function, converted = _choose(table, args)
        return function(self, *converted)

    return method


def _static(*overloads):
    """A static method of overloads, as for `_java`."""
    method = _java(*overloads)

    def function(*args):
        return method(None, *args)

    return function


class _JavaObject:
    """A Java object a calendar hands out: its Java methods by name, and any other method
    refused by the name of its class."""

    __slots__ = ()
    _java_name = ""
    # The class has every public method of its Java class but those named here, which are
    # refused; a name Java does not have either is an AttributeError, as jpy's. A class
    # whose `_unported` is None (the collections) refuses any name it does not have.
    _unported = None
    _OBJECT_METHODS = frozenset({"wait", "notify", "notifyAll"})

    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        unported = type(self)._unported
        if unported is None or name in unported or name in _JavaObject._OBJECT_METHODS:
            raise NotPorted(f"{self._java_name}#{name}")
        raise AttributeError(f"'{self._java_name}' object has no attribute '{name}'")

    def hashCode(self):
        raise NotPorted(f"{self._java_name}#hashCode")

    def getClass(self):
        raise NotPorted(f"{self._java_name}#getClass")

    def equals(self, other) -> bool:
        return self == other

    def toString(self) -> str:
        return str(self)

    def __repr__(self):
        return f"{self._java_name}({self})"


# ------------------------------------------------------------------------------ collections


class JavaList(_JavaObject):
    """A list Java hands out. Behavior: `java.util.List`; `get` out of range throws as
    `java.util.Arrays$ArrayList#get` does"""

    __slots__ = ("_items",)
    _java_name = "java.util.List"

    def __init__(self, items):
        self._items = tuple(items)

    def size(self) -> int:
        return len(self._items)

    def isEmpty(self) -> bool:
        return not self._items

    def get(self, index: int):
        if not isinstance(index, int) or isinstance(index, bool):
            raise JavaError("no matching Java method overloads found")
        if not 0 <= index < len(self._items):
            raise JavaError(
                f"java.lang.ArrayIndexOutOfBoundsException: Index {index} out of bounds for length {len(self._items)}"
            )
        return self._items[index]

    def contains(self, item) -> bool:
        return item in self._items

    def indexOf(self, item) -> int:
        return self._items.index(item) if item in self._items else -1

    def toArray(self) -> list:
        return list(self._items)

    def __eq__(self, other):
        return isinstance(other, JavaList) and other._items == self._items

    def __hash__(self):
        return hash(self._items)

    def __str__(self):
        return "[" + ", ".join(str(item) for item in self._items) + "]"


class JavaSet(JavaList):
    """A set Java hands out, in a fixed order. Behavior: `java.util.Set`; Java's sets of
    `Set.copyOf` have an order that changes from run to run."""

    __slots__ = ()
    _java_name = "java.util.Set"

    def get(self, index):
        raise NotPorted("java.util.Set#get")

    def indexOf(self, item):
        raise NotPorted("java.util.Set#indexOf")

    def __eq__(self, other):
        return isinstance(other, JavaSet) and set(other._items) == set(self._items)

    def __hash__(self):
        return hash(frozenset(self._items))


class JavaMapEntry(_JavaObject):
    """Behavior: `java.util.Map.Entry`"""

    __slots__ = ("_key", "_value")
    _java_name = "java.util.Map$Entry"

    def __init__(self, key, value):
        self._key, self._value = key, value

    def getKey(self):
        return self._key

    def getValue(self):
        return self._value

    def __eq__(self, other):
        return isinstance(other, JavaMapEntry) and (other._key, other._value) == (self._key, self._value)

    def __hash__(self):
        return hash((self._key, self._value))

    def __str__(self):
        return f"{self._key}={self._value}"


class JavaMap(_JavaObject):
    """A map Java hands out, in the order of its keys. Behavior: `java.util.Map`; Java's
    maps of `Map.copyOf` have an order that changes from run to run."""

    __slots__ = ("_items",)
    _java_name = "java.util.Map"

    def __init__(self, items: dict):
        self._items = dict(items)

    def size(self) -> int:
        return len(self._items)

    def isEmpty(self) -> bool:
        return not self._items

    def get(self, key):
        return self._items.get(key) if _hashable(key) else None

    def getOrDefault(self, key, default):
        return self._items.get(key, default) if _hashable(key) else default

    def containsKey(self, key) -> bool:
        return _hashable(key) and key in self._items

    def containsValue(self, value) -> bool:
        return value in self._items.values()

    def keySet(self) -> JavaSet:
        return JavaSet(self._items)

    def values(self) -> JavaList:
        return JavaList(self._items.values())

    def entrySet(self) -> JavaSet:
        return JavaSet(JavaMapEntry(key, value) for key, value in self._items.items())

    def __eq__(self, other):
        return isinstance(other, JavaMap) and other._items == self._items

    def __hash__(self):
        return hash(frozenset(self._items.items()))

    def __str__(self):
        return "{" + ", ".join(f"{key}={value}" for key, value in self._items.items()) + "}"


def _hashable(value) -> bool:
    try:
        hash(value)
    except TypeError:
        return False
    return True


# ------------------------------------------------------------------------- business hours


def _time_kind(value, kind: type):
    """A time a day of `kind` compares with: Java casts it, and fails for another class."""
    if not isinstance(value, kind):
        raise JavaError(
            f"java.lang.ClassCastException: class {_java_class_name(value)} cannot be cast to class "
            f"{kind._java_name}"
        )
    return value.nano_of_day if kind is _java_time.LocalTime else value.nanos


def _java_class_name(value) -> str:
    if isinstance(value, str):
        return "java.lang.String"
    if isinstance(value, bool):
        return "java.lang.Boolean"
    if isinstance(value, int):
        return "java.lang.Integer"
    if isinstance(value, float):
        return "java.lang.Double"
    return getattr(value, "_java_name", "org.jpy.PyObject") or "org.jpy.PyObject"


class TimeRange(_JavaObject):
    """A stretch of business time: a start, an end, and whether the end is in it.

    Behavior: `io.deephaven.time.calendar.TimeRange`
    """

    __slots__ = ("_start", "_end", "_inclusive", "_kind")
    _java_name = "io.deephaven.time.calendar.TimeRange"
    _unported = frozenset({"toInstant"})

    def __init__(self, kind: type, start: int, end: int, inclusive: bool):
        self._kind = kind
        self._start, self._end, self._inclusive = start, end, inclusive

    def _value(self, raw: int):
        return self._kind(raw)

    def start(self):
        return self._value(self._start)

    def end(self):
        return self._value(self._end)

    def isInclusiveEnd(self) -> bool:
        return self._inclusive

    def nanos(self) -> int:
        return self._end - self._start + (1 if self._inclusive else 0)

    def duration(self):
        return _java_time.Duration(self.nanos())

    def contains(self, time) -> bool:
        if time is None:
            return False
        raw = _time_kind(time, self._kind)
        return self._start <= raw and (raw <= self._end if self._inclusive else raw < self._end)

    def __eq__(self, other):
        return isinstance(other, TimeRange) and other._key() == self._key()

    def _key(self):
        return (self._kind, self._start, self._end, self._inclusive)

    def __hash__(self):
        return hash(self._key())

    def __str__(self):
        inclusive = "true" if self._inclusive else "false"
        return f"TimeRange{{start={self.start()}, end={self.end()}, inclusiveEnd={inclusive}}}"


class CalendarDay(_JavaObject):
    """The business hours of a day, in ranges in order: times of day for the standard day
    of a calendar, instants for a date.

    Behavior: `io.deephaven.time.calendar.CalendarDay`
    """

    __slots__ = ("_ranges", "_kind")
    _java_name = "io.deephaven.time.calendar.CalendarDay"
    _unported = frozenset({"toInstant"})

    def __init__(self, kind: type, ranges):
        self._kind = kind
        self._ranges = tuple(TimeRange(kind, *bounds) for bounds in ranges)

    def businessTimeRanges(self) -> JavaList:
        return JavaList(self._ranges)

    def businessStart(self):
        return self._ranges[0].start() if self._ranges else None

    def businessEnd(self):
        return self._ranges[-1].end() if self._ranges else None

    def isInclusiveEnd(self) -> bool:
        return not self._ranges or self._ranges[-1].isInclusiveEnd()

    def businessNanos(self) -> int:
        return sum(time_range.nanos() for time_range in self._ranges)

    def businessDuration(self):
        return _java_time.Duration(self.businessNanos())

    def _elapsed(self, raw: int) -> int:
        elapsed = 0
        for time_range in self._ranges:
            if raw < time_range._start:
                break
            if raw > time_range._end:
                elapsed += time_range.nanos()
            else:
                elapsed += raw - time_range._start
                break
        return elapsed

    def businessNanosElapsed(self, time) -> int:
        if time is None:
            return _NULL_LONG
        return self._elapsed(_time_kind(time, self._kind))

    def businessDurationElapsed(self, time):
        if time is None:
            return None
        return _java_time.Duration(self.businessNanosElapsed(time))

    def businessNanosRemaining(self, time) -> int:
        if time is None:
            return _NULL_LONG
        return self.businessNanos() - self.businessNanosElapsed(time)

    def businessDurationRemaining(self, time):
        if time is None:
            return None
        return _java_time.Duration(self.businessNanosRemaining(time))

    def isBusinessDay(self) -> bool:
        return self.businessNanos() > 0

    def isBusinessTime(self, time) -> bool:
        if time is None:
            return False
        return any(time_range.contains(time) for time_range in self._ranges)

    def __eq__(self, other):
        return isinstance(other, CalendarDay) and other._ranges == self._ranges

    def __hash__(self):
        return hash(self._ranges)

    def __str__(self):
        return "CalendarDay{businessTimeRanges=[" + ", ".join(str(r) for r in self._ranges) + "]}"


# ------------------------------------------------------------------------------ the calendar


def _local_date(day: int):
    return _java_time.LocalDate(*_java_time._civil(day))


def _nanos(time) -> int:
    """The nanoseconds of an `Instant` or a `ZonedDateTime`, as the server's calendars
    take them."""
    if not _LONG_MIN <= time.nanos <= _LONG_MAX:
        raise NotPorted(
            "io.deephaven.time.calendar.BusinessCalendar", "an instant outside the years 1677 to 2262"
        )
    return time.nanos


def _null_int(days: int) -> bool:
    return days == _NULL_INT


def _day_of_week(day: int) -> int:
    """`java.time.DayOfWeek#getValue` of a date: 1970-01-01 was a Thursday."""
    return (day + 3) % 7 + 1


# The functions of overloads that differ only in the class of their date arguments.


def _method(name: str):
    def call(self, *args):
        return getattr(self, name)(*args)

    return call


def _day_of_week_method(self, date):
    return self._day_of_week(date)


def _day_of_week_value_method(self, date):
    return self._day_of_week_value(date)


def _plus_method(which: str, sign: int):
    def call(self, date, days):
        return self._plus(date, days, which, sign)

    return call


def _today_plus(which: str, sign: int):
    def call(self, days):
        return self._plus(_local_date(self._today()), days, which, sign)

    return call


def _last_method(period: str):
    def call(self, date):
        return self._is_last(date, period)

    return call


def _ranged(name: str, which: str) -> list:
    """The overloads of a method of a range of dates: two dates of one class, with or
    without whether each end is included (both are, without)."""
    overloads = []
    for kind in ("LocalDate", "String", "ZonedDateTime", "Instant"):
        overloads.append(
            (kind, kind, "boolean", "boolean", lambda self, s, e, si, ei: getattr(self, name)(s, e, si, ei, which))
        )
        overloads.append((kind, kind, lambda self, s, e: getattr(self, name)(s, e, True, True, which)))
    return overloads


def _differences(what: str) -> list:
    return [(kind, kind, lambda self, s, e: self._diff(s, e, what)) for kind in ("Instant", "ZonedDateTime")]


class BusinessCalendar(_JavaObject):
    """A business calendar of the server.

    Behavior: `io.deephaven.time.calendar.BusinessCalendar`; a calendar is equal only to
    itself, as Java's, which does not override `equals`.
    """

    __slots__ = ("_handle", "_zone")
    _java_name = "io.deephaven.time.calendar.BusinessCalendar"
    _unported = frozenset()

    def __init__(self, handle):
        self._handle = handle
        self._zone = None

    def __eq__(self, other):
        return isinstance(other, BusinessCalendar) and other._handle.same(self._handle)

    def __hash__(self):
        return hash(self._handle.name().upper())

    def __str__(self):
        return self._handle.text()

    # -- what a date argument is

    def _day(self, date) -> int:
        """Days since 1970-01-01 of a `LocalDate`, the text of one, or the date of an
        `Instant` or a `ZonedDateTime` in the calendar's zone."""
        if isinstance(date, str):
            return _native().calendar_parse_date(date)
        if isinstance(date, _java_time.LocalDate):
            return date.to_epoch_day()
        return self._handle.date_of(_nanos(date))

    def _today(self) -> int:
        """Behavior: `io.deephaven.time.DateTimeUtils#todayLocalDate(ZoneId)`"""
        return self._handle.date_of(_java_time.now_nanos())

    def _zoned(self, nanos: int):
        return _java_time.ZonedDateTime(nanos, self.timeZone())

    # -- Calendar

    def name(self) -> str:
        return self._handle.name()

    def description(self):
        return self._handle.description()

    def timeZone(self):
        if self._zone is None:
            self._zone = _java_time.ZoneId.of(self._handle.zone_id())
        return self._zone

    def calendarDate(self):
        return _local_date(self._today())

    def _day_of_week(self, date):
        return None if date is None else _java_time.DayOfWeek.of(_day_of_week(self._day(date)))

    def _day_of_week_value(self, date):
        return _NULL_INT if date is None else _day_of_week(self._day(date))

    dayOfWeek = _java(
        lambda self: self._day_of_week(_local_date(self._today())),
        *(((kind, _day_of_week_method)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")),
    )
    dayOfWeekValue = _java(
        lambda self: _day_of_week(self._today()),
        *(((kind, _day_of_week_value_method)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")),
    )

    def _plus(self, date, days, which: str, sign: int):
        """Behavior: `Calendar#plusDays`, `BusinessCalendar#plusBusinessDays`,
        `BusinessCalendar#plusNonBusinessDays` and their `minus` forms, which give the
        class of their argument back"""
        if date is None or _null_int(days):
            return None
        days = (sign * days + 2**31) % 2**32 - 2**31
        if isinstance(date, (_java_time.Instant, _java_time.ZonedDateTime)):
            nanos = self._handle.plus_at_time(_nanos(date), days, which)
            if nanos is None:
                return None
            if isinstance(date, _java_time.Instant):
                return _java_time.Instant(nanos)
            return self._zoned(nanos)
        day = self._handle.plus(self._day(date), days, which)
        if day is None:
            return None
        return str(_local_date(day)) if isinstance(date, str) else _local_date(day)

    plusDays = _java(*((kind, "int", _plus_method("all", 1)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")))
    minusDays = _java(*((kind, "int", _plus_method("all", -1)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")))

    futureDate = _java(("int", _today_plus("all", 1)))
    pastDate = _java(("int", _today_plus("all", -1)))

    def _dates(self, start, end, start_inclusive, end_inclusive, which: str):
        """Behavior: `Calendar#calendarDates`, `BusinessCalendar#businessDates`,
        `BusinessCalendar#nonBusinessDates`: the dates as text for dates given as text"""
        if start is None or end is None:
            return None
        days = self._handle.dates(self._day(start), self._day(end), start_inclusive, end_inclusive, which)
        if isinstance(start, str):
            return [str(_local_date(day)) for day in days]
        return [_local_date(day) for day in days]

    def _count(self, start, end, start_inclusive, end_inclusive, which: str):
        if start is None or end is None:
            return _NULL_INT
        return self._handle.count(self._day(start), self._day(end), start_inclusive, end_inclusive, which)

    calendarDates = _java(*_ranged("_dates", "all"))
    numberCalendarDates = _java(*_ranged("_count", "all"))

    # -- BusinessCalendar

    def firstValidDate(self):
        return _local_date(self._handle.first_valid())

    def lastValidDate(self):
        return _local_date(self._handle.last_valid())

    def weekendDays(self) -> JavaSet:
        return JavaSet(_java_time.DayOfWeek.of(day) for day in self._handle.weekend())

    def standardBusinessDay(self) -> CalendarDay:
        return CalendarDay(_java_time.LocalTime, self._handle.standard())

    def standardBusinessNanos(self) -> int:
        return self.standardBusinessDay().businessNanos()

    def standardBusinessDuration(self):
        return _java_time.Duration(self.standardBusinessNanos())

    def holidays(self) -> JavaMap:
        return JavaMap(
            {_local_date(day): CalendarDay(_java_time.Instant, ranges) for day, ranges in self._handle.holidays()}
        )

    def _calendar_day(self, date):
        if date is None:
            return None
        return CalendarDay(_java_time.Instant, self._handle.day(self._day(date)))

    calendarDay = _java(
        lambda self: self._calendar_day(_local_date(self._today())),
        *((kind, _method("_calendar_day")) for kind in ("LocalDate", "ZonedDateTime", "Instant", "String")),
    )

    def _is_business_day(self, date):
        if date is None:
            return False
        if isinstance(date, _java_time.DayOfWeek):
            return self._handle.is_business_weekday(date.getValue())
        return self._handle.is_business_day(self._day(date))

    isBusinessDay = _java(
        lambda self: self._handle.is_business_day(self._today()),
        *(
            (kind, _method("_is_business_day"))
            for kind in ("LocalDate", "String", "ZonedDateTime", "Instant", "DayOfWeek")
        ),
    )

    def _is_last(self, date, period: str):
        if date is None:
            return False
        return self._handle.is_last_business_day(self._day(date), period)

    # `isLastBusinessDayOfMonth(LocalDate)`, `isLastBusinessDayOfYear(LocalDate)` and
    # `isLastBusinessDayOfYear(String)` are not public, so jpy cannot call them.
    isLastBusinessDayOfMonth = _java(
        lambda self: self._handle.is_last_business_day(self._today(), "month"),
        *((kind, _last_method("month")) for kind in ("ZonedDateTime", "Instant", "String")),
    )
    isLastBusinessDayOfWeek = _java(
        lambda self: self._handle.is_last_business_day(self._today(), "week"),
        *((kind, _last_method("week")) for kind in ("LocalDate", "ZonedDateTime", "Instant", "String")),
    )
    isLastBusinessDayOfYear = _java(
        lambda self: self._handle.is_last_business_day(self._today(), "year"),
        *((kind, _last_method("year")) for kind in ("ZonedDateTime", "Instant")),
    )

    def _is_business_time(self, time):
        if time is None:
            return False
        return self._handle.is_business_time(_nanos(time))

    isBusinessTime = _java(
        lambda self: self._handle.is_business_time(_java_time.now_nanos()),
        *((kind, _method("_is_business_time")) for kind in ("ZonedDateTime", "Instant")),
    )

    def _fraction_standard(self, date):
        if date is None:
            return _NULL_DOUBLE
        return self._handle.fraction_standard(self._day(date))

    fractionStandardBusinessDay = _java(
        lambda self: self._handle.fraction_standard(self._today()),
        *((kind, _method("_fraction_standard")) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")),
    )

    def _fraction_complete(self, time):
        if time is None:
            return _NULL_DOUBLE
        return self._handle.fraction_complete(_nanos(time))

    def _fraction_remaining(self, time):
        if time is None:
            return _NULL_DOUBLE
        return 1.0 - self._handle.fraction_complete(_nanos(time))

    fractionBusinessDayComplete = _java(
        lambda self: self._handle.fraction_complete(_java_time.now_nanos()),
        *((kind, _method("_fraction_complete")) for kind in ("Instant", "ZonedDateTime")),
    )
    fractionBusinessDayRemaining = _java(
        lambda self: 1.0 - self._handle.fraction_complete(_java_time.now_nanos()),
        *((kind, _method("_fraction_remaining")) for kind in ("Instant", "ZonedDateTime")),
    )

    numberBusinessDates = _java(*_ranged("_count", "business"))
    numberNonBusinessDates = _java(*_ranged("_count", "non_business"))
    businessDates = _java(*_ranged("_dates", "business"))
    nonBusinessDates = _java(*_ranged("_dates", "non_business"))

    def _diff(self, start, end, what: str):
        """Behavior: `BusinessCalendar#diffBusinessNanos` and the methods built on it"""
        null = {"nanos": _NULL_LONG, "non_nanos": _NULL_LONG, "days": _NULL_DOUBLE, "years": _NULL_DOUBLE}
        if start is None or end is None:
            return null.get(what)
        start, end = _nanos(start), _nanos(end)
        if what == "nanos":
            return self._handle.diff_business(start, end)
        if what == "non_nanos":
            return self._handle.diff_non_business(start, end)
        if what == "duration":
            return _java_time.Duration(self._handle.diff_business(start, end))
        if what == "non_duration":
            return _java_time.Duration(self._handle.diff_non_business(start, end))
        if what == "days":
            return self._handle.diff_business(start, end) / float(self.standardBusinessNanos())
        return self._handle.diff_years(start, end)

    diffBusinessNanos = _java(*_differences("nanos"))
    diffNonBusinessNanos = _java(*_differences("non_nanos"))
    diffBusinessDuration = _java(*_differences("duration"))
    diffNonBusinessDuration = _java(*_differences("non_duration"))
    diffBusinessDays = _java(*_differences("days"))
    diffBusinessYears = _java(*_differences("years"))

    plusBusinessDays = _java(*((kind, "int", _plus_method("business", 1)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")))
    minusBusinessDays = _java(*((kind, "int", _plus_method("business", -1)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime")))
    plusNonBusinessDays = _java(
        *((kind, "int", _plus_method("non_business", 1)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime"))
    )
    minusNonBusinessDays = _java(
        *((kind, "int", _plus_method("non_business", -1)) for kind in ("LocalDate", "String", "Instant", "ZonedDateTime"))
    )

    futureBusinessDate = _java(("int", _today_plus("business", 1)))
    pastBusinessDate = _java(("int", _today_plus("business", -1)))
    futureNonBusinessDate = _java(("int", _today_plus("non_business", 1)))
    pastNonBusinessDate = _java(("int", _today_plus("non_business", -1)))


# ---------------------------------------------------------- io.deephaven.time.DateTimeUtils
# The two methods of `DateTimeUtils` that `deephaven.plot.express` calls for a figure with
# a calendar, which the module `jpy` carries out (`jpy._BACKED`).


def _system_time_zone(_):
    return _java_time.system_zone()


def _time_zone(_, text):
    """Behavior: `io.deephaven.time.DateTimeUtils#timeZone(String)`, which is
    `TimeZoneAliases#zoneId`"""
    return None if text is None else _java_time.zone_of(text)


def _instant_local_time(_, instant, zone):
    """Behavior: `io.deephaven.time.DateTimeUtils#toLocalTime(Instant, ZoneId)`"""
    if instant is None or zone is None:
        return None
    return _java_time.ZonedDateTime.ofInstant(instant, zone).local()[1]


def _zoned_local_time(_, date_time):
    """Behavior: `io.deephaven.time.DateTimeUtils#toLocalTime(ZonedDateTime)`"""
    return None if date_time is None else date_time.local()[1]


def _unported_local_time(_, date_time):
    raise NotPorted("io.deephaven.time.DateTimeUtils#toLocalTime", "a LocalDateTime")


date_time_utils = {
    "timeZone": _static(_system_time_zone, ("String", _time_zone)),
    "toLocalTime": _static(
        ("Instant", "ZoneId", _instant_local_time),
        ("ZonedDateTime", _zoned_local_time),
        ("LocalDateTime", _unported_local_time),
    ),
}
"""The members of `io.deephaven.time.DateTimeUtils` that are carried out, by name."""
