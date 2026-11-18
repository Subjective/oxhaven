# Runs inside a server's Python console: what deephaven.plot.express's FigureCalendar reads
# of each shipped calendar, and of the default one, as `cal125_figure` (one line each).
import jpy as _jpy

from deephaven import calendar as _dcal
from deephaven import new_table as _new_table
from deephaven.column import string_col as _string_col

_utils = _jpy.get_type('io.deephaven.time.DateTimeUtils')
_DAYS = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']


def _periods(day, zone=None):
    found = []
    for time_range in list(day.businessTimeRanges().toArray()):
        start, end = time_range.start(), time_range.end()
        if zone:
            zone_id = _utils.timeZone(zone)
            start, end = _utils.toLocalTime(start, zone_id), _utils.toLocalTime(end, zone_id)
        found.append({'open': str(start), 'close': str(end)})
    return found


def _figure_dict(cal):
    zone = str(cal.timeZone())
    weekend = [str(day) for day in list(cal.weekendDays().toArray())]
    holidays = sorted(
        ({'date': str(key), 'businessPeriods': _periods(cal.holidays().get(key), zone)}
         for key in list(cal.holidays().keySet().toArray())),
        key=lambda holiday: holiday['date'])
    return {'timeZone': zone, 'businessDays': [day for day in _DAYS if day not in weekend], 'holidays': holidays,
            'businessPeriods': _periods(cal.standardBusinessDay()), 'name': str(cal.name())}


_lines = ['default|' + str(_figure_dict(_dcal.calendar()))]
for _name in ('UTC', 'USNYSE_EXAMPLE', 'USBANK_EXAMPLE'):
    _d = _figure_dict(_dcal.calendar(_name))
    _lines.append(_name + '|head|' + str({k: v for k, v in _d.items() if k != 'holidays'}))
    for _h in _d['holidays']:
        _lines.append(_name + '|holiday|' + str(_h))
cal125_figure = _new_table([_string_col('L', _lines)])
