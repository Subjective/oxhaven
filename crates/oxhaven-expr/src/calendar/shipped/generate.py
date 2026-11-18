#!/usr/bin/env python3
"""Writes the three business calendars that Oxhaven loads at start, from rules.

    python3 crates/oxhaven-expr/src/calendar/shipped/generate.py

rewrites `UTC.calendar`, `USNYSE_EXAMPLE.calendar` and `USBANK_EXAMPLE.calendar` next to
this script; with `--check` it writes nothing and fails when a file differs from what it
would write. `cargo test -p oxhaven-expr -- shipped` checks the calendars against what
Deephaven 42.9 answers.

Deephaven ships calendars of these names; their files are under the Deephaven Community
License and are not used here. Only what a user of Deephaven can observe was taken from
Deephaven: the names, which one is the default, each calendar's time zone, standard
business hours, weekend and first and last valid date. Every holiday and early close below
comes from the public rule or record cited next to it. Dates are facts; where a public
record and Deephaven disagree, `tasks/12.5-shipped-calendars.md` lists the date and the
decision.

Standard library only.
"""

import datetime
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent

MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY = range(7)


def nth_weekday(year, month, weekday, n):
    """The n-th given weekday of a month; n = -1 is the last."""
    if n > 0:
        first = datetime.date(year, month, 1)
        return first + datetime.timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    following = datetime.date(year + month // 12, month % 12 + 1, 1)
    last = following - datetime.timedelta(days=1)
    return last - datetime.timedelta(days=(last.weekday() - weekday) % 7)


def easter(year):
    """Western (Gregorian) Easter Sunday, by the anonymous Gregorian computus published in
    Nature in 1876 (also given by Meeus, Astronomical Algorithms, chapter 8)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return datetime.date(year, month, day + 1)


def nearest_weekday(day):
    """A date that falls on a Saturday moves to the Friday before, one on a Sunday to the
    Monday after."""
    if day.weekday() == SATURDAY:
        return day - datetime.timedelta(days=1)
    if day.weekday() == SUNDAY:
        return day + datetime.timedelta(days=1)
    return day


def sunday_to_monday(day):
    """A date on a Sunday moves to the Monday after; one on a Saturday is not observed."""
    if day.weekday() == SATURDAY:
        return None
    if day.weekday() == SUNDAY:
        return day + datetime.timedelta(days=1)
    return day


# --- New York Stock Exchange --------------------------------------------------------------
#
# Sources:
# - NYSE Rule 7.2 (Holidays), NYSE Rules, https://www.nyse.com/regulation/rules: the
#   exchange closes on New Year's Day, Martin Luther King, Jr. Day, Washington's Birthday,
#   Good Friday, Memorial Day, Juneteenth National Independence Day, Independence Day,
#   Labor Day, Thanksgiving Day and Christmas Day. A holiday on a Sunday is observed on the
#   Monday after. A holiday on a Saturday is observed on the Friday before, unless that
#   Friday ends a monthly or yearly accounting period: so New Year's Day on a Saturday
#   closes no day.
# - NYSE, "Holidays & Trading Hours", https://www.nyse.com/markets/hours-calendars: the
#   current schedule, the 1:00 p.m. early closes, and the start of Juneteenth in 2022.
# - Martin Luther King, Jr. Day: observed by the NYSE from 1998 (NYSE press release,
#   "NYSE to close on Martin Luther King Jr. Day", 1997).
# - The unscheduled closings are each in the NYSE's published history of market closings
#   and in its notices of the day:
#   11 to 14 September 2001 (the attacks of 11 September);
#   11 June 2004 (national day of mourning for President Reagan, Executive Order of
#   7 June 2004); 2 January 2007 (national day of mourning for President Ford,
#   Executive Order of 29 December 2006); 29 and 30 October 2012 (Hurricane Sandy);
#   5 December 2018 (national day of mourning for President George H. W. Bush,
#   Presidential proclamation of 1 December 2018); 9 January 2025 (national day of
#   mourning for President Carter, Executive Order of 30 December 2024).
# - The early closes at 1:00 p.m., from the NYSE's announced holiday schedules of each
#   year: the day after Thanksgiving; Christmas Eve when it is a Monday to Thursday; the
#   day before Independence Day when it is a Monday, Tuesday or Thursday, and also when
#   it is a Wednesday from 2013; before 2013, when Independence Day was a Thursday the
#   early close was on the Friday after it instead (5 July 2002). One early close by
#   announcement only: Friday 26 December 2003, the day after Christmas.

NYSE_OPEN, NYSE_CLOSE, NYSE_EARLY_CLOSE = "09:30", "16:00", "13:00"

NYSE_ANNOUNCED_EARLY = {
    datetime.date(2003, 12, 26): "day after Christmas, early close announced for 2003",
}

NYSE_UNSCHEDULED = {
    datetime.date(2001, 9, 11): "closed after the attacks of 11 September",
    datetime.date(2001, 9, 12): "closed after the attacks of 11 September",
    datetime.date(2001, 9, 13): "closed after the attacks of 11 September",
    datetime.date(2001, 9, 14): "closed after the attacks of 11 September",
    datetime.date(2004, 6, 11): "national day of mourning, President Reagan",
    datetime.date(2007, 1, 2): "national day of mourning, President Ford",
    datetime.date(2012, 10, 29): "closed for Hurricane Sandy",
    datetime.date(2012, 10, 30): "closed for Hurricane Sandy",
    datetime.date(2018, 12, 5): "national day of mourning, President George H. W. Bush",
    datetime.date(2025, 1, 9): "national day of mourning, President Carter",
}


def nyse_year(year):
    """The closed days and early closes of one year: {date: (what, close or None)}."""
    days = {}

    def closed(day, what):
        if day is not None:
            days[day] = (what, None)

    def early(day, what):
        days.setdefault(day, (what, NYSE_EARLY_CLOSE))

    new_year = datetime.date(year, 1, 1)
    closed(sunday_to_monday(new_year), "New Year's Day")
    closed(nth_weekday(year, 1, MONDAY, 3), "Martin Luther King, Jr. Day")
    closed(nth_weekday(year, 2, MONDAY, 3), "Washington's Birthday")
    closed(easter(year) - datetime.timedelta(days=2), "Good Friday")
    closed(nth_weekday(year, 5, MONDAY, -1), "Memorial Day")
    if year >= 2022:
        closed(nearest_weekday(datetime.date(year, 6, 19)), "Juneteenth")
    july_4 = datetime.date(year, 7, 4)
    closed(nearest_weekday(july_4), "Independence Day")
    closed(nth_weekday(year, 9, MONDAY, 1), "Labor Day")
    thanksgiving = nth_weekday(year, 11, THURSDAY, 4)
    closed(thanksgiving, "Thanksgiving Day")
    closed(nearest_weekday(datetime.date(year, 12, 25)), "Christmas Day")
    for day, what in NYSE_UNSCHEDULED.items():
        if day.year == year:
            closed(day, what)

    for day, what in NYSE_ANNOUNCED_EARLY.items():
        if day.year == year:
            early(day, what)
    early(thanksgiving + datetime.timedelta(days=1), "day after Thanksgiving")
    christmas_eve = datetime.date(year, 12, 24)
    if christmas_eve.weekday() <= THURSDAY:
        early(christmas_eve, "Christmas Eve")
    july_3 = datetime.date(year, 7, 3)
    if july_3.weekday() in (MONDAY, TUESDAY, THURSDAY) or (
        july_3.weekday() == WEDNESDAY and year >= 2013
    ):
        early(july_3, "day before Independence Day")
    if july_4.weekday() == THURSDAY and year < 2013:
        early(datetime.date(year, 7, 5), "day after Independence Day")
    return days


# --- United States banks -------------------------------------------------------------------
#
# Sources:
# - 5 U.S.C. 6103(a), the legal public holidays of the United States: New Year's Day,
#   Birthday of Martin Luther King, Jr. (third Monday in January), Washington's Birthday
#   (third Monday in February), Memorial Day (last Monday in May), Juneteenth National
#   Independence Day (19 June, added by Public Law 117-17 of 17 June 2021), Independence
#   Day, Labor Day (first Monday in September), Columbus Day (second Monday in October),
#   Veterans Day (11 November), Thanksgiving Day (fourth Thursday in November) and
#   Christmas Day.
# - The Federal Reserve Banks' holiday schedules (Federal Reserve Financial Services,
#   "Holiday Schedules", https://www.frbservices.org/about/holiday-schedules): a holiday on
#   a Sunday is observed the Monday after; for a holiday on a Saturday the Reserve Banks
#   are open the Friday before. The Reserve Banks first observed Juneteenth in 2022, and
#   stayed open on 18 June 2021, the day federal offices observed it that year.
# - Days closed by executive order for federal employees only (Christmas Eve of 2019,
#   2020, 2024 and 2025, 26 December 2025) and national days of mourning are not
#   Reserve Bank holidays and are business days here.

BANK_OPEN, BANK_CLOSE = "09:00", "17:00"
# The first year whose holidays the file lists, before its first valid date (2013).
BANK_HOLIDAYS_FROM = datetime.date(1999, 1, 1)


def bank_year(year):
    days = {}

    def closed(day, what):
        if day is not None:
            days[day] = (what, None)

    closed(sunday_to_monday(datetime.date(year, 1, 1)), "New Year's Day")
    closed(nth_weekday(year, 1, MONDAY, 3), "Birthday of Martin Luther King, Jr.")
    closed(nth_weekday(year, 2, MONDAY, 3), "Washington's Birthday")
    closed(nth_weekday(year, 5, MONDAY, -1), "Memorial Day")
    if year >= 2022:
        closed(sunday_to_monday(datetime.date(year, 6, 19)), "Juneteenth")
    closed(sunday_to_monday(datetime.date(year, 7, 4)), "Independence Day")
    closed(nth_weekday(year, 9, MONDAY, 1), "Labor Day")
    closed(nth_weekday(year, 10, MONDAY, 2), "Columbus Day")
    closed(sunday_to_monday(datetime.date(year, 11, 11)), "Veterans Day")
    closed(nth_weekday(year, 11, THURSDAY, 4), "Thanksgiving Day")
    closed(sunday_to_monday(datetime.date(year, 12, 25)), "Christmas Day")
    return days


# --- Writing -------------------------------------------------------------------------------

LICENSE = (
    "  Written for Oxhaven by generate.py in this directory, from the public rules cited\n"
    "  there. Not derived from Deephaven's calendar files. Licensed as the rest of\n"
    "  Oxhaven.\n"
)


def calendar_text(name, zone, description, first, last, hours, weekend, days, note):
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<!--",
        LICENSE.rstrip("\n"),
    ]
    lines += ["  " + line for line in note]
    lines += [
        "-->",
        "<calendar>",
        f"  <name>{name}</name>",
        f"  <description>{description}</description>",
        f"  <timeZone>{zone}</timeZone>",
        f"  <firstValidDate>{first.isoformat()}</firstValidDate>",
        f"  <lastValidDate>{last.isoformat()}</lastValidDate>",
        "  <default>",
    ]
    lines += [f"    <weekend>{day}</weekend>" for day in weekend]
    lines += [f"    {hours}", "  </default>"]
    year = None
    for day in sorted(days):
        if day.year != year:
            year = day.year
            lines.append(f"  <!-- {year} -->")
        what, close = days[day]
        hours_of_day = (
            "" if close is None else f"<businessTime><open>{NYSE_OPEN}</open><close>{close}</close></businessTime>"
        )
        lines.append(f"  <holiday><date>{day.isoformat()}</date>{hours_of_day}</holiday> <!-- {what} -->")
    lines.append("</calendar>")
    return "\n".join(lines) + "\n"


def collect(year_rule, first, last):
    days = {}
    for year in range(first.year, last.year + 1):
        days.update(year_rule(year))
    return days


def hours(open_, close, include_close=False):
    tail = "<includeClose>true</includeClose>" if include_close else ""
    return f"<businessTime><open>{open_}</open><close>{close}</close>{tail}</businessTime>"


def calendars():
    nyse_first, nyse_last = datetime.date(1999, 1, 1), datetime.date(2027, 12, 31)
    bank_first, bank_last = datetime.date(2013, 1, 1), datetime.date(2027, 12, 31)
    return {
        "UTC.calendar": calendar_text(
            "UTC",
            "UTC",
            "UTC",
            datetime.date(1900, 1, 1),
            datetime.date(2100, 12, 31),
            hours("00:00", "23:59:59.999999999", include_close=True),
            [],
            {},
            ["Every day is a whole business day: no weekend and no holidays."],
        ),
        "USNYSE_EXAMPLE.calendar": calendar_text(
            "USNYSE_EXAMPLE",
            "America/New_York",
            "An example calendar of the New York Stock Exchange's regular trading session, "
            "with its holidays and early closes; not for trading or other production use.",
            nyse_first,
            nyse_last,
            hours(NYSE_OPEN, NYSE_CLOSE),
            ["Saturday", "Sunday"],
            collect(nyse_year, nyse_first, nyse_last),
            ["The NYSE's regular session: holidays, unscheduled closings and early closes."],
        ),
        "USBANK_EXAMPLE.calendar": calendar_text(
            "USBANK_EXAMPLE",
            "America/New_York",
            "An example calendar of United States bank business days, by the Federal "
            "Reserve Banks' holiday schedule; not for trading or other production use.",
            bank_first,
            bank_last,
            hours(BANK_OPEN, BANK_CLOSE),
            ["Saturday", "Sunday"],
            collect(bank_year, BANK_HOLIDAYS_FROM, bank_last),
            [
                "The Federal Reserve Banks' holidays. Those from 1999 to 2012 are before the",
                "first valid date: no date of those years can be asked for, and holidays()",
                "lists them, as it does for Deephaven's calendar of this name.",
            ],
        ),
    }


def main(argv):
    check = argv[1:] == ["--check"]
    stale = []
    for file_name, text in calendars().items():
        path = HERE / file_name
        if check:
            if not path.exists() or path.read_text(encoding="utf-8") != text:
                stale.append(file_name)
        else:
            path.write_text(text, encoding="utf-8")
    if stale:
        print("not what generate.py writes: " + ", ".join(stale), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
