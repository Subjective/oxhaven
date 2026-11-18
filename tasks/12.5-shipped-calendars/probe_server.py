# Runs inside a server's Python console (Deephaven 42.9 or Oxhaven). Builds the table
# `cal125_out` with one text line per observation.
import datetime as _dt
import random as _random

from deephaven import calendar as _dcal
from deephaven import new_table as _new_table
from deephaven.column import string_col as _string_col
from deephaven.time import to_j_instant as _I

_lines = []


def _try(call):
    try:
        return str(call())
    except BaseException as error:  # noqa: BLE001
        text = str(error).replace("\n", " ")
        return "ERR:" + text[:160]


def _ranges(day):
    found = []
    for r in list(day.businessTimeRanges().toArray()):
        found.append(f"{r.start()}~{r.end()}{'i' if r.isInclusiveEnd() else ''}")
    return ";".join(found)


_lines.append("names|" + ",".join(str(n) for n in _dcal.calendar_names()))
_lines.append("default|" + str(_dcal.calendar_name()))
_lines.append("defaultcal|" + _try(lambda: _dcal.calendar().name()))

for _name in ("UTC", "USNYSE_EXAMPLE", "USBANK_EXAMPLE"):
    _cal = _dcal.calendar(_name)
    _p = _name + "|"
    _lines.append(_p + "str|" + str(_cal))
    _lines.append(_p + "zone|" + str(_cal.timeZone()))
    _lines.append(_p + "first|" + str(_cal.firstValidDate()))
    _lines.append(_p + "last|" + str(_cal.lastValidDate()))
    _lines.append(_p + "weekend|" + ",".join(sorted(str(d) for d in list(_cal.weekendDays().toArray()))))
    _lines.append(_p + "standard|" + _ranges(_cal.standardBusinessDay()))
    _lines.append(_p + "standardNanos|" + str(_cal.standardBusinessNanos()))
    _first = _dt.date.fromisoformat(str(_cal.firstValidDate()))
    _last = _dt.date.fromisoformat(str(_cal.lastValidDate()))
    _d = _first
    while _d <= _last:
        _s = _d.isoformat()
        _day = _cal.calendarDay(_s)
        _lines.append(f"{_p}day|{_s}|{'B' if _cal.isBusinessDay(_s) else 'N'}|{_ranges(_day)}")
        _d += _dt.timedelta(days=1)
    # outside the range on both sides
    for _s in ((_first - _dt.timedelta(days=1)).isoformat(), (_last + _dt.timedelta(days=1)).isoformat()):
        _lines.append(_p + "outside|" + _s + "|" + _try(lambda: _cal.isBusinessDay(_s)))

    _rng = _random.Random(125 + len(_name))
    _span = (_last - _first).days
    for _k in range(400):
        _a = (_first + _dt.timedelta(days=_rng.randrange(_span + 1))).isoformat()
        _b = (_first + _dt.timedelta(days=_rng.randrange(_span + 1))).isoformat()
        if _k % 3 == 0:
            # a short distance, where holidays matter most
            _b = (_dt.date.fromisoformat(_a) + _dt.timedelta(days=_rng.randrange(-12, 13))).isoformat()
        _n = _rng.randrange(-30, 31)
        _q = _p + f"s{_k}|{_a}|{_b}|{_n}|"
        _lines.append(_q + "nbd|" + _try(lambda: _cal.numberBusinessDates(_a, _b)))
        _lines.append(_q + "nbdx|" + _try(lambda: _cal.numberBusinessDates(_a, _b, False, True)))
        _lines.append(_q + "nnbd|" + _try(lambda: _cal.numberNonBusinessDates(_a, _b)))
        _lines.append(_q + "next|" + _try(lambda: _cal.plusBusinessDays(_a, 1)))
        _lines.append(_q + "prev|" + _try(lambda: _cal.minusBusinessDays(_a, 1)))
        _lines.append(_q + "plus|" + _try(lambda: _cal.plusBusinessDays(_a, _n)))
        _lines.append(_q + "minus|" + _try(lambda: _cal.minusBusinessDays(_a, _n)))
        _lines.append(_q + "plusnb|" + _try(lambda: _cal.plusNonBusinessDays(_a, _n)))
        _lines.append(_q + "frac|" + _try(lambda: _cal.fractionStandardBusinessDay(_a)))
        _lines.append(_q + "lastm|" + _try(lambda: _cal.isLastBusinessDayOfMonth(_a)))
        _lines.append(_q + "lastw|" + _try(lambda: _cal.isLastBusinessDayOfWeek(_a)))
        _ta = _I(f"{_a}T{_rng.randrange(24):02d}:{_rng.randrange(60):02d}:00Z")
        _tb = _I(f"{_b}T{_rng.randrange(24):02d}:{_rng.randrange(60):02d}:00Z")
        _q = _p + f"s{_k}|{_ta}|{_tb}|{_n}|"
        _lines.append(_q + "bt|" + _try(lambda: _cal.isBusinessTime(_ta)))
        _lines.append(_q + "fbdc|" + _try(lambda: _cal.fractionBusinessDayComplete(_ta)))
        _lines.append(_q + "fbdr|" + _try(lambda: _cal.fractionBusinessDayRemaining(_ta)))
        _lines.append(_q + "dbn|" + _try(lambda: _cal.diffBusinessNanos(_ta, _tb)))
        _lines.append(_q + "dbd|" + _try(lambda: _cal.diffBusinessDays(_ta, _tb)))
        _lines.append(_q + "dby|" + _try(lambda: _cal.diffBusinessYears(_ta, _tb)))
        _lines.append(_q + "plusI|" + _try(lambda: _cal.plusBusinessDays(_ta, _n)))

cal125_out = _new_table([_string_col("L", _lines)])
