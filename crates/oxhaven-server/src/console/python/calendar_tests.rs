//! Tests of the business calendars of the Python console (`deephaven.calendar`,
//! `deephaven._calendar`) and of the two methods of `io.deephaven.time.DateTimeUtils` the
//! `jpy` module carries out.
//!
//! The expected values are what Deephaven 42.9 answered to the same calls on the same
//! calendar files (`oxhaven-expr`'s `src/calendar/fixtures`). The tests share one
//! interpreter, and so one collection of calendars: a file is added only when its
//! calendar is not there yet.

use super::tests::Python;

fn setup() -> String {
    let fixtures = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../oxhaven-expr/src/calendar/fixtures/"
    );
    format!(
        r#"
import jpy
from deephaven import DHError
from deephaven import calendar as dcal
from deephaven.time import to_j_instant as I, to_j_local_date as LD, to_j_zdt as Z, to_j_time_zone, to_j_local_time

FIXTURES = {fixtures:?}
for _file, _name in (("OXTEST_NY", "OXTEST_NY"), ("oxtest_odd", "OXTEST_ODD"), ("OXTEST_ALLDAY", "OXTEST_ALLDAY")):
    if _name not in dcal.calendar_names():
        dcal.add_calendar(FIXTURES + _file + ".calendar")
ny = dcal.calendar("OXTEST_NY")
odd = dcal.calendar("oxtest_ODD")
allday = dcal.calendar("OXTEST_ALLDAY")

def raised(call):
    try:
        call()
    except BaseException as e:
        return type(e).__name__, str(e)
    return None

def message(call):
    return raised(call)[1]
"#
    )
}

/// The collection of calendars: names, the default, adding, removing, and the calendars
/// Deephaven ships, which this server does not.
#[tokio::test]
async fn calendars_are_named_added_and_removed_as_in_deephaven() {
    let python = Python::start().await;
    python.run(&setup());
    python.run(
        r#"
names = dcal.calendar_names()
assert {'OXTEST_ALLDAY', 'OXTEST_NY', 'OXTEST_ODD', 'USBANK_EXAMPLE', 'USNYSE_EXAMPLE', 'UTC'} <= set(names), names
assert names == sorted(names)
assert dcal.calendar_name() == 'UTC'
assert dcal.calendar("oxtest_ny").name() == 'OXTEST_NY' and dcal.calendar("oxtest_ny") == ny
assert odd.name() == 'oxTest_odd' and odd.description() is None
assert str(ny) == "Calendar{name='OXTEST_NY', description='Test calendar with a lunch break', timeZone=America/New_York}"

# the calendars Deephaven ships are named and refused
text = message(lambda: dcal.calendar())
assert text.startswith('failed to get the default calendar. : not ported: io.deephaven.time.calendar.Calendars#calendar'), text
assert 'not ported' in message(lambda: dcal.calendar('USNYSE_EXAMPLE'))
assert message(lambda: dcal.calendar('nope')) == 'failed to get the default calendar. : java.lang.IllegalArgumentException: No such calendar: nope'
assert message(lambda: dcal.calendar(5)) == 'failed to get the default calendar. : no matching Java method overloads found'

# adding again: the same calendar is no change, another of the name is refused
dcal.add_calendar(ny)
text = message(lambda: dcal.add_calendar(FIXTURES + 'OXTEST_NY.calendar'))
assert text.endswith("java.lang.IllegalArgumentException: Multiple calendars have the same name: name='OXTEST_NY'"), text
assert message(lambda: dcal.add_calendar(5)) == 'failed to add calendar : no matching Java method overloads found'
assert message(lambda: dcal.add_calendar(None)).startswith('cal must be specified')
text = message(lambda: dcal.add_calendar(FIXTURES + 'OXTEST_NY.xml'))
assert text.endswith('java.lang.UnsupportedOperationException: Calendar file must be in .calendar format'), text
text = message(lambda: dcal.add_calendar(FIXTURES + 'missing.calendar'))
assert 'Unable to load calendar file' in text and 'java.io.FileNotFoundException' in text, text

# removing is by the name as kept, in upper case
dcal.add_calendar(odd)
dcal.remove_calendar('oxtest_odd')
assert 'OXTEST_ODD' in dcal.calendar_names()
dcal.remove_calendar('OXTEST_ODD')
assert 'OXTEST_ODD' not in dcal.calendar_names()
dcal.remove_calendar('OXTEST_ODD')
dcal.add_calendar(odd)
assert dcal.calendar('OXTEST_ODD') == odd
assert message(lambda: dcal.remove_calendar(None)).endswith(
    'io.deephaven.base.verify.RequirementFailure: Requirement failed: required name != null, instead name == null.')

# the default is any name, looked for when it is asked for
dcal.set_calendar('nope')
assert dcal.calendar_name() == 'nope'
assert message(lambda: dcal.calendar()).endswith('No such calendar: nope')
dcal.set_calendar('oxtest_ny')
assert dcal.calendar() == ny and dcal.calendar(None) == ny
dcal.set_calendar('UTC')
"#,
    );
}

/// The methods of a calendar, chosen among Java's overloads by the types of the
/// arguments, as jpy chooses.
#[tokio::test]
async fn a_calendar_answers_as_deephavens() {
    let python = Python::start().await;
    python.run(&setup());
    python.run(
        r#"
assert str(odd.timeZone()) == 'Asia/Tokyo'
assert str(ny.firstValidDate()) == '2023-01-01' and ny.firstValidDate().getYear() == 2023
assert sorted(str(day) for day in ny.weekendDays().toArray()) == ['SATURDAY', 'SUNDAY']
assert [day.getValue() for day in odd.weekendDays().toArray()] == [5]
assert str(ny.standardBusinessDay()) == ('CalendarDay{businessTimeRanges=[TimeRange{start=09:30, end=12:00, inclusiveEnd=false}, '
                                         'TimeRange{start=13:00, end=16:00, inclusiveEnd=false}]}')
assert ny.standardBusinessNanos() == 19800000000000 and str(ny.standardBusinessDuration()) == 'PT5H30M'
assert sorted(str(k) for k in ny.holidays().keySet().toArray()) == ['2023-12-29', '2024-01-01', '2024-07-03', '2024-11-29', '2024-12-25']
assert str(ny.holidays().get(LD('2024-07-03'))) == 'CalendarDay{businessTimeRanges=[TimeRange{start=2024-07-03T13:30:00Z, end=2024-07-03T17:00:00Z, inclusiveEnd=false}]}'
assert ny.holidays().get('2024-01-01') is None and ny.holidays().size() == 5

# a date as text, a date, an instant, a zoned date-time
assert ny.isBusinessDay('2024-01-01') is False and ny.isBusinessDay('2024-01-02') is True
assert ny.isBusinessDay(LD('2024-07-03')) is True
assert ny.isBusinessDay(I('2024-01-02T03:00:00Z')) is False
assert ny.isBusinessDay(Z('2024-01-02T03:00:00 ET')) is True
assert ny.isBusinessDay('2024-1-2') is True
assert message(lambda: ny.isBusinessDay('2022-12-31')) == (
    'io.deephaven.time.calendar.BusinessCalendar$InvalidDateException: Date is before the first valid business '
    'calendar date:  date=2022-12-31 firstValidDate=2023-01-01')
assert message(lambda: ny.isBusinessDay('20240102')) == 'io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse local date: 20240102'
assert message(lambda: ny.isBusinessDay(None)) == 'ambiguous Java method call, too many matching method overloads found'
assert message(lambda: ny.isBusinessDay(5)) == 'no matching Java method overloads found'
assert message(lambda: ny.isBusinessDay('2024-01-02', 1)) == 'no matching Java method overloads found'

july4 = ny.calendarDay('2024-07-04')
assert str(july4.businessStart()) == '2024-07-04T13:30:00Z' and str(july4.businessEnd()) == '2024-07-04T20:00:00Z'
assert july4.businessNanos() == 19800000000000 and july4.isInclusiveEnd() is False
assert july4.businessNanosElapsed(I('2024-07-04T17:30:00Z')) == 10800000000000
assert july4.businessNanosElapsed(None) == -9223372036854775808 and july4.businessDurationElapsed(None) is None
assert str(july4.businessDurationRemaining(I('2024-07-04T17:30:00Z'))) == 'PT2H30M'
assert july4.businessTimeRanges().size() == 2
assert message(lambda: july4.businessTimeRanges().get(5)) == 'java.lang.ArrayIndexOutOfBoundsException: Index 5 out of bounds for length 2'
assert ny.calendarDay('2024-07-06').businessStart() is None and ny.calendarDay('2024-07-06').isInclusiveEnd() is True
assert july4 == ny.calendarDay('2024-07-04') and not july4.equals(ny.calendarDay('2024-07-05'))
assert allday.calendarDay('2024-03-10').businessNanos() == 5400000000000

assert list(ny.businessDates('2024-01-01', '2024-01-10', False, False)) == [
    '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05', '2024-01-08', '2024-01-09']
assert [str(d) for d in ny.businessDates(LD('2024-01-01'), LD('2024-01-10'))][-1] == '2024-01-10'
assert ny.businessDates(None, '2024-01-01') is None
assert list(ny.nonBusinessDates('2023-12-25', '2024-01-08')) == ['2023-12-29', '2023-12-30', '2023-12-31', '2024-01-01', '2024-01-06', '2024-01-07']
assert ny.numberBusinessDates('2024-01-01', '2024-12-31') == 260
assert ny.numberNonBusinessDates('2024-01-01', '2024-12-31') == 106
assert ny.numberBusinessDates(None, '2024-01-01') == -2147483648
assert odd.numberBusinessDates('2024-01-01', '2024-03-04') == 55

assert ny.plusBusinessDays('2024-01-01', 0) is None
assert ny.plusBusinessDays('2024-01-02', -1) == '2023-12-28'
assert ny.plusBusinessDays('2024-01-02', -2147483648) is None
assert str(ny.plusBusinessDays(LD('2024-01-02'), 3)) == '2024-01-05'
assert str(ny.plusBusinessDays(I('2024-03-08T07:30:00Z'), 1)) == '2024-03-11T06:30:00Z'
assert str(ny.plusBusinessDays(Z('2024-01-05T09:00:00 Asia/Tokyo'), 1)) == '2024-01-05T19:00-05:00[America/New_York]'
assert ny.minusNonBusinessDays('2024-01-02', 2) == '2023-12-31'
assert str(ny.plusDays(I('2024-03-09T12:00:00Z'), 1)) == '2024-03-10T11:00:00Z'

assert ny.isLastBusinessDayOfMonth('2024-03-29') is True
assert ny.isLastBusinessDayOfMonth(I('2024-02-01T03:00:00Z')) is True
# the overloads of a LocalDate, and of text for a year, are not public
assert message(lambda: ny.isLastBusinessDayOfMonth(LD('2024-01-31'))) == 'no matching Java method overloads found'
assert message(lambda: ny.isLastBusinessDayOfYear('2024-12-31')) == 'no matching Java method overloads found'
assert ny.isLastBusinessDayOfYear(I('2024-12-31T20:00:00Z')) is True
assert ny.isLastBusinessDayOfWeek(LD('2024-01-05')) is True

assert ny.isBusinessTime(Z('2024-01-02T10:00:00 ET')) is True
assert ny.fractionStandardBusinessDay('2024-11-29') == 0.45454545454545453
assert ny.fractionBusinessDayRemaining(I('2024-07-04T17:30:00Z')) == 0.4545454545454546
assert message(lambda: ny.fractionBusinessDayComplete('2024-07-04')) == 'no matching Java method overloads found'

start, end = I('2024-07-03T15:00:00Z'), I('2024-07-08T15:00:00Z')
assert ny.diffBusinessNanos(start, end) == 52200000000000
assert str(ny.diffNonBusinessDuration(start, end)) == 'PT105H30M'
assert ny.diffBusinessDays(start, end) == 2.6363636363636362
assert ny.diffBusinessYears(start, end) == 0.010175438596491228
assert allday.diffBusinessNanos(I('2024-11-02T15:00:00Z'), I('2024-11-04T15:00:00Z')) == 86400000000001
assert message(lambda: odd.diffBusinessYears(I('2024-01-03T15:00:00Z'), I('2024-01-08T15:00:00Z'))) == (
    'io.deephaven.time.calendar.BusinessCalendar$InvalidDateException: Business calendar does not contain a complete year for: year=2024')

# what a calendar does not do is refused by its name
assert message(lambda: ny.hashCode()).startswith('not ported: io.deephaven.time.calendar.BusinessCalendar#hashCode')
assert message(lambda: july4.toInstant).startswith('not ported: io.deephaven.time.calendar.CalendarDay#toInstant')
# and a name Java does not have either is no attribute, as in jpy
assert raised(lambda: ny.to_dict) == ('AttributeError', "'io.deephaven.time.calendar.BusinessCalendar' object has no attribute 'to_dict'")
assert getattr(ny, 'to_dict', None) is None
"#,
    );
}

/// What `deephaven.plot.express` asks of a calendar for a figure (its `FigureCalendar`):
/// the zone, the weekend, the holidays and the business hours, with the times of the
/// holidays as times of day in the calendar's zone through `DateTimeUtils`.
#[tokio::test]
async fn a_figure_reads_a_calendar_as_from_deephaven() {
    let python = Python::start().await;
    python.run(&setup());
    python.run(
        r#"
utils = jpy.get_type('io.deephaven.time.DateTimeUtils')
DAYS = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']

def periods(day, zone=None):
    found = []
    for time_range in list(day.businessTimeRanges().toArray()):
        start, end = time_range.start(), time_range.end()
        if zone:
            zone_id = utils.timeZone(zone)
            start, end = utils.toLocalTime(start, zone_id), utils.toLocalTime(end, zone_id)
        found.append({'open': str(start), 'close': str(end)})
    return found

def figure_dict(cal):
    zone = str(cal.timeZone())
    weekend = [str(day) for day in list(cal.weekendDays().toArray())]
    holidays = sorted(
        ({'date': str(key), 'businessPeriods': periods(cal.holidays().get(key), zone)}
         for key in list(cal.holidays().keySet().toArray())),
        key=lambda holiday: holiday['date'])
    return {'timeZone': zone, 'businessDays': [day for day in DAYS if day not in weekend], 'holidays': holidays,
            'businessPeriods': periods(cal.standardBusinessDay()), 'name': str(cal.name())}

assert str(figure_dict(ny)) == "{'timeZone': 'America/New_York', 'businessDays': ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY'], 'holidays': [{'date': '2023-12-29', 'businessPeriods': []}, {'date': '2024-01-01', 'businessPeriods': []}, {'date': '2024-07-03', 'businessPeriods': [{'open': '09:30', 'close': '13:00'}]}, {'date': '2024-11-29', 'businessPeriods': [{'open': '09:30', 'close': '12:00'}]}, {'date': '2024-12-25', 'businessPeriods': []}], 'businessPeriods': [{'open': '09:30', 'close': '12:00'}, {'open': '13:00', 'close': '16:00'}], 'name': 'OXTEST_NY'}", figure_dict(ny)
assert str(figure_dict(odd)) == "{'timeZone': 'Asia/Tokyo', 'businessDays': ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'SATURDAY', 'SUNDAY'], 'holidays': [{'date': '2023-12-28', 'businessPeriods': []}, {'date': '2024-01-05', 'businessPeriods': [{'open': '08:00', 'close': '10:00'}, {'open': '11:00', 'close': '12:00'}]}, {'date': '2024-03-04', 'businessPeriods': []}], 'businessPeriods': [{'open': '08:00', 'close': '17:00'}], 'name': 'oxTest_odd'}", figure_dict(odd)
assert str(figure_dict(allday)) == "{'timeZone': 'America/New_York', 'businessDays': ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'], 'holidays': [{'date': '2024-03-10', 'businessPeriods': [{'open': '01:00', 'close': '03:30'}]}, {'date': '2024-11-03', 'businessPeriods': [{'open': '00:30', 'close': '01:30'}]}], 'businessPeriods': [{'open': '00:00', 'close': '23:59:59.999999999'}], 'name': 'OXTEST_ALLDAY'}", figure_dict(allday)

# DateTimeUtils.timeZone and toLocalTime, by their Java overloads
assert str(utils.timeZone('ET')) == 'America/New_York'
assert str(utils.timeZone('UTC+1')) == 'UTC+01:00'
assert str(utils.timeZone()) == 'Etc/UTC'
assert utils.timeZone(None) is None
assert message(lambda: utils.timeZone('nope')) == 'java.time.zone.ZoneRulesException: Unknown time-zone ID: nope'
assert message(lambda: utils.timeZone(5)) == 'no matching Java method overloads found'
# a zone is no time zone text: Java has no overload of a ZoneId
assert message(lambda: utils.timeZone(to_j_time_zone('ET'))) == 'no matching Java method overloads found'
assert str(utils.toLocalTime(I('2024-07-03T13:30:00Z'), to_j_time_zone('ET'))) == '09:30'
assert utils.toLocalTime(I('2024-07-03T13:30:00Z'), None) is None
assert str(utils.toLocalTime(Z('2024-07-03T13:30:00 ET'))) == '13:30'
assert utils.toLocalTime(None, None) is None
assert message(lambda: utils.toLocalTime(None)) == 'ambiguous Java method call, too many matching method overloads found'
assert message(lambda: utils.toLocalTime('2024-07-03T13:30:00Z', to_j_time_zone('ET'))) == 'no matching Java method overloads found'
# the other members of the class are still refused
assert message(lambda: utils.parseInstant('2024-07-03T13:30:00Z')) == 'not ported: io.deephaven.time.DateTimeUtils#parseInstant'
"#,
    );
}
