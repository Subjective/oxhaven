//! Tests of the business calendars against values recorded from Deephaven 42.9, which read
//! the same calendar files (`fixtures/`, written for these tests) through
//! `deephaven.calendar.add_calendar` and answered the same questions.

use std::sync::Arc;

use super::*;

const NANOS_PER_HOUR: i64 = 3_600_000_000_000;

fn read(text: &str) -> Result<BusinessCalendar, CalendarError> {
    parse_calendar(text, &CalendarZone::parse)
}

fn fixture(name: &str) -> BusinessCalendar {
    let text = match name {
        "ny" => include_str!("fixtures/OXTEST_NY.calendar"),
        "odd" => include_str!("fixtures/oxtest_odd.calendar"),
        "allday" => include_str!("fixtures/OXTEST_ALLDAY.calendar"),
        other => panic!("no fixture {other}"),
    };
    read(text).expect("the fixture reads")
}

fn day(text: &str) -> i64 {
    parse_date(text).expect("a date")
}

/// The instant of a date and an hour and minute in UTC, in nanoseconds since the epoch.
fn utc(date: &str, hour: i64, minute: i64) -> i64 {
    day(date) * NANOS_PER_DAY + hour * NANOS_PER_HOUR + minute * 60_000_000_000
}

fn java_message(error: CalendarError) -> String {
    match error {
        CalendarError::Java { class, message } => format!("{class}: {message}"),
        unported => panic!("expected a Java exception, got {unported}"),
    }
}

/// A calendar file of the given children of `<calendar>`.
fn calendar_of(children: &str) -> String {
    format!("<calendar>{children}</calendar>")
}

#[test]
fn a_calendar_file_is_read_as_deephaven_reads_it() {
    let ny = fixture("ny");
    assert_eq!(ny.name(), "OXTEST_NY");
    // the text of an element is trimmed
    assert_eq!(ny.description(), Some("Test calendar with a lunch break"));
    assert_eq!(ny.zone().id(), "America/New_York");
    assert_eq!(
        ny.text(),
        "Calendar{name='OXTEST_NY', description='Test calendar with a lunch break', timeZone=America/New_York}"
    );
    assert_eq!(date_text(ny.first_valid_date()), "2023-01-01");
    assert_eq!(date_text(ny.last_valid_date()), "2025-12-31");
    // weekend days are named in any case and with spaces around them
    assert_eq!(ny.weekend_days(), vec![6, 7]);
    assert_eq!(
        ny.standard_business_day().text(TimeKind::LocalTime),
        "CalendarDay{businessTimeRanges=[TimeRange{start=09:30, end=12:00, inclusiveEnd=false}, \
         TimeRange{start=13:00, end=16:00, inclusiveEnd=false}]}"
    );
    // a date of eight characters is read as yyyyMMdd
    let holidays: Vec<String> = ny.holidays().keys().map(|day| date_text(*day)).collect();
    assert_eq!(
        holidays,
        [
            "2023-12-29",
            "2024-01-01",
            "2024-07-03",
            "2024-11-29",
            "2024-12-25"
        ]
    );
    assert_eq!(
        ny.holidays()[&day("2024-07-03")].text(TimeKind::Instant),
        "CalendarDay{businessTimeRanges=[TimeRange{start=2024-07-03T13:30:00Z, end=2024-07-03T17:00:00Z, inclusiveEnd=false}]}"
    );
    assert_eq!(
        ny.holidays()[&day("2024-01-01")].text(TimeKind::Instant),
        "CalendarDay{businessTimeRanges=[]}"
    );

    // no description, an alias for the zone, legacy business periods, valid dates taken
    // from the holidays
    let odd = fixture("odd");
    assert_eq!(odd.name(), "oxTest_odd");
    assert_eq!(odd.description(), None);
    assert_eq!(odd.zone().id(), "Asia/Tokyo");
    assert_eq!(date_text(odd.first_valid_date()), "2023-12-28");
    assert_eq!(date_text(odd.last_valid_date()), "2024-03-04");
    assert_eq!(odd.weekend_days(), vec![5]);
    assert_eq!(
        odd.holidays()[&day("2024-01-05")].text(TimeKind::Instant),
        "CalendarDay{businessTimeRanges=[TimeRange{start=2024-01-04T23:00:00Z, end=2024-01-05T01:00:00Z, inclusiveEnd=false}, \
         TimeRange{start=2024-01-05T02:00:00Z, end=2024-01-05T03:00:00Z, inclusiveEnd=false}]}"
    );

    // includeClose in any case
    let allday = fixture("allday");
    assert_eq!(
        allday.standard_business_day().text(TimeKind::LocalTime),
        "CalendarDay{businessTimeRanges=[TimeRange{start=00:00, end=23:59:59.999999999, inclusiveEnd=true}]}"
    );
    assert_eq!(
        allday.holidays()[&day("2024-11-03")].text(TimeKind::Instant),
        "CalendarDay{businessTimeRanges=[TimeRange{start=2024-11-03T04:30:00Z, end=2024-11-03T05:30:00Z, inclusiveEnd=true}]}"
    );
}

#[test]
fn a_calendar_file_is_refused_as_deephaven_refuses_it() {
    let refused = |children: &str| java_message(read(&calendar_of(children)).unwrap_err());
    let default =
        "<default><businessTime><open>09:00</open><close>17:00</close></businessTime></default>";
    let valid =
        "<firstValidDate>2024-01-01</firstValidDate><lastValidDate>2024-12-31</lastValidDate>";

    assert!(refused("<timeZone>UTC</timeZone>")
        .starts_with("java.lang.Exception: Missing the 'name' tag in calendar file: text="));
    assert!(refused("<name>A</name>")
        .starts_with("java.lang.Exception: Missing the 'timeZone' tag in calendar file: text="));
    assert!(
        refused(&format!("<name>A</name><timeZone>UTC</timeZone>{valid}"))
            .starts_with("java.lang.Exception: Missing the 'default' tag in calendar file: text=")
    );
    assert_eq!(
        refused("<name>A</name><timeZone>Mars/Olympus</timeZone>"),
        "java.time.zone.ZoneRulesException: Unknown time-zone ID: Mars/Olympus"
    );
    // no valid dates and no holidays to take them from
    assert_eq!(
        refused(&format!("<name>A</name><timeZone>UTC</timeZone>{default}")),
        "java.util.NoSuchElementException: "
    );
    let with = |rest: &str| {
        refused(&format!(
            "<name>A</name><timeZone>UTC</timeZone>{valid}{rest}"
        ))
    };
    assert_eq!(
        with("<default><businessTime><open>09:00</open><close>24:00</close></businessTime></default>"),
        "java.lang.RuntimeException: Close time (24:00) is on the next day.  '23:59:59.999999999' is the maximum close time."
    );
    assert!(with("<default><businessTime><open>09:00</open><close>17:00</close></businessTime><businessPeriod>09:00,17:00</businessPeriod></default>")
        .starts_with("java.lang.Exception: Cannot have both 'businessTime' and 'businessPeriod' tags in the same element: text="));
    assert_eq!(
        with("<default><businessTime><open>09:00</open><close>12:00</close></businessTime><businessTime><open>11:00</open><close>13:00</close></businessTime></default>"),
        "java.lang.IllegalArgumentException: Business time ranges overlap."
    );
    assert_eq!(
        with("<default><businessTime><open>11:00</open><close>11:00</close></businessTime></default>"),
        "java.lang.IllegalArgumentException: Start is the same as end: startTime=11:00 endTime=11:00"
    );
    assert!(
        with("<default><businessTime><open>11:00</open></businessTime></default>")
            .starts_with("java.lang.Exception: Missing the 'close' tag in calendar file: text=")
    );
    assert_eq!(
        with("<default><businessPeriod>09:00</businessPeriod></default>"),
        "java.lang.IllegalArgumentException: Can not parse business periods; open/close = 09:00"
    );
    // Java's exception is caused by `Invalid value for MonthOfYear (valid values 1 - 12): 13`
    assert_eq!(
        with(&format!("{default}<holiday><date>2024-13-01</date></holiday>")),
        "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse local date: 2024-13-01"
    );
    // a date of eight characters that is no yyyyMMdd; Java's exception is caused by
    // `Invalid value for MonthOfYear (valid values 1 - 12): -2`
    assert_eq!(
        with(&format!("{default}<holiday><date>2024-2-3</date></holiday>")),
        "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse local date: 2024--2--3"
    );
    // Java's exception is caused by `No enum constant java.time.DayOfWeek.FUNDAY`
    assert_eq!(
        with("<default><weekend>funday</weekend></default>"),
        "java.lang.Exception: Invalid day of week: day=FUNDAY"
    );
    assert!(java_message(read("<calendar>").unwrap_err())
        .starts_with("org.xml.sax.SAXParseException: "));
}

#[test]
fn what_deephaven_accepts_in_a_calendar_file_is_accepted() {
    // the root may have any name, the valid range may be reversed, a time needs no
    // seconds and may have a fraction, and includeClose is true only when it says so
    let calendar = read(
        "<!DOCTYPE other [<!ENTITY who \"entities\">]><other><name>B</name><timeZone>UTC+1</timeZone>\
         <description>  with <![CDATA[<cdata>]]> and &who;<!-- not this -->  </description>\
         <firstValidDate>2024-12-31</firstValidDate><lastValidDate>2024-01-01</lastValidDate>\
         <default><businessTime><open>9:30</open><close>16:00:00.5</close><includeClose>yes</includeClose></businessTime></default>\
         </other>",
    )
    .expect("the calendar reads");
    assert_eq!(calendar.description(), Some("with <cdata> and entities"));
    assert_eq!(calendar.zone().id(), "UTC+01:00");
    assert_eq!(
        calendar.standard_business_day().text(TimeKind::LocalTime),
        "CalendarDay{businessTimeRanges=[TimeRange{start=09:30, end=16:00:00.500, inclusiveEnd=false}]}"
    );
    assert_eq!(
        java_message(calendar.is_business_day(day("2024-06-01")).unwrap_err()),
        "io.deephaven.time.calendar.BusinessCalendar$InvalidDateException: Date is before the first valid business \
         calendar date:  date=2024-06-01 firstValidDate=2024-12-31"
    );

    // a namespaced document has no name without a namespace
    assert!(java_message(
        read("<calendar xmlns=\"urn:x\"><name>A</name><timeZone>UTC</timeZone></calendar>")
            .unwrap_err()
    )
    .starts_with("java.lang.Exception: Missing the 'name' tag"));

    // a date given twice keeps the last
    let twice = read(&calendar_of(
        "<name>T</name><timeZone>UTC</timeZone><default/>\
         <holiday><date>2024-01-01</date></holiday>\
         <holiday><date>2024-01-01</date><businessTime><open>10:00</open><close>11:00</close></businessTime></holiday>",
    ))
    .expect("the calendar reads");
    assert_eq!(
        twice.holidays()[&day("2024-01-01")].business_nanos(),
        NANOS_PER_HOUR
    );
}

#[test]
fn a_gap_in_the_time_zone_fails_the_day_it_falls_on() {
    let gap = read(&calendar_of(
        "<name>B12</name><timeZone>America/New_York</timeZone>\
         <firstValidDate>2024-03-01</firstValidDate><lastValidDate>2024-03-31</lastValidDate>\
         <default><businessTime><open>02:30</open><close>03:00</close></businessTime></default>",
    ))
    .expect("the calendar reads");
    assert_eq!(gap.is_business_day(day("2024-03-09")), Ok(true));
    assert_eq!(
        java_message(gap.is_business_day(day("2024-03-10")).unwrap_err()),
        "java.lang.IllegalArgumentException: Start is after end: startTime=2024-03-10T07:30:00Z endTime=2024-03-10T07:00:00Z"
    );
}

#[test]
fn days_and_times_are_business_as_in_deephaven() {
    let ny = fixture("ny");
    assert_eq!(ny.is_business_day(day("2024-01-01")), Ok(false));
    assert_eq!(ny.is_business_day(day("2024-01-02")), Ok(true));
    assert_eq!(ny.is_business_day(day("2024-01-06")), Ok(false));
    assert_eq!(ny.is_business_day(day("2024-07-03")), Ok(true));
    assert_eq!(
        java_message(ny.is_business_day(day("2022-12-31")).unwrap_err()),
        "io.deephaven.time.calendar.BusinessCalendar$InvalidDateException: Date is before the first valid business \
         calendar date:  date=2022-12-31 firstValidDate=2023-01-01"
    );
    assert!(java_message(ny.is_business_day(day("2026-01-01")).unwrap_err()).ends_with(
        "Date is after the last valid business calendar date:  date=2026-01-01 lastValidDate=2025-12-31"
    ));
    // the date of an instant is the date in the calendar's zone
    assert_eq!(
        ny.date_of(utc("2024-01-02", 3, 0)).map(date_text),
        Ok("2024-01-01".into())
    );

    assert_eq!(
        ny.calendar_day(day("2024-07-04")).unwrap().text(TimeKind::Instant),
        "CalendarDay{businessTimeRanges=[TimeRange{start=2024-07-04T13:30:00Z, end=2024-07-04T16:00:00Z, inclusiveEnd=false}, \
         TimeRange{start=2024-07-04T17:00:00Z, end=2024-07-04T20:00:00Z, inclusiveEnd=false}]}"
    );
    let july4 = ny.calendar_day(day("2024-07-04")).unwrap();
    assert_eq!(july4.business_nanos(), 19_800_000_000_000);
    assert_eq!(
        july4.business_nanos_elapsed(utc("2024-07-04", 17, 30)),
        10_800_000_000_000
    );
    assert_eq!(
        july4.business_nanos_remaining(utc("2024-07-04", 17, 30)),
        9_000_000_000_000
    );
    assert_eq!(
        july4.business_nanos_elapsed(utc("2024-07-05", 17, 30)),
        19_800_000_000_000
    );
    assert_eq!(july4.business_nanos_elapsed(utc("2024-07-03", 17, 30)), 0);
    assert!(july4.is_business_time(utc("2024-07-04", 13, 30)));
    assert!(!july4.is_business_time(utc("2024-07-04", 16, 0)));

    let allday = fixture("allday");
    assert_eq!(
        allday.calendar_day(day("2024-03-11")).unwrap().text(TimeKind::Instant),
        "CalendarDay{businessTimeRanges=[TimeRange{start=2024-03-11T04:00:00Z, end=2024-03-12T03:59:59.999999999Z, inclusiveEnd=true}]}"
    );
    assert_eq!(
        allday
            .calendar_day(day("2024-03-10"))
            .unwrap()
            .business_nanos(),
        5_400_000_000_000
    );
    assert_eq!(
        allday
            .calendar_day(day("2024-03-09"))
            .unwrap()
            .business_nanos(),
        86_400_000_000_000
    );

    assert_eq!(ny.is_business_time(utc("2024-01-02", 15, 0)), Ok(true));
    assert_eq!(ny.is_business_time(utc("2024-01-02", 17, 30)), Ok(false));
    assert_eq!(
        ny.fraction_standard_business_day(day("2024-07-03")),
        Ok(0.6363636363636364)
    );
    assert_eq!(
        ny.fraction_standard_business_day(day("2024-07-06")),
        Ok(0.0)
    );
    assert_eq!(
        ny.fraction_standard_business_day(day("2024-11-29")),
        Ok(0.45454545454545453)
    );
    assert_eq!(
        ny.fraction_business_day_complete(utc("2024-07-03", 15, 0)),
        Ok(0.42857142857142855)
    );
    assert_eq!(
        ny.fraction_business_day_complete(utc("2024-07-04", 17, 30)),
        Ok(0.5454545454545454)
    );
    assert_eq!(
        ny.fraction_business_day_complete(utc("2024-07-06", 17, 30)),
        Ok(1.0)
    );

    // the month only is compared, and the last business day of a week is the last before
    // its weekend
    assert_eq!(
        ny.is_last_business_day_of_month(day("2024-01-31")),
        Ok(true)
    );
    assert_eq!(
        ny.is_last_business_day_of_month(day("2024-01-30")),
        Ok(false)
    );
    assert_eq!(
        ny.is_last_business_day_of_month(day("2024-03-29")),
        Ok(true)
    );
    assert_eq!(
        ny.is_last_business_day_of_month(day("2024-06-30")),
        Ok(false)
    );
    assert!(ny.is_last_business_day_of_month(day("2025-12-31")).is_err());
    assert_eq!(ny.is_last_business_day_of_week(day("2024-01-05")), Ok(true));
    assert_eq!(
        ny.is_last_business_day_of_week(day("2024-01-04")),
        Ok(false)
    );
    assert_eq!(ny.is_last_business_day_of_week(day("2023-12-28")), Ok(true));
    assert_eq!(
        ny.is_last_business_day_of_week(day("2024-01-06")),
        Ok(false)
    );
    assert_eq!(ny.is_last_business_day_of_year(day("2024-12-31")), Ok(true));
    let odd = fixture("odd");
    assert_eq!(
        odd.is_last_business_day_of_week(day("2024-01-04")),
        Ok(false)
    );
    assert_eq!(
        odd.is_last_business_day_of_week(day("2024-01-03")),
        Ok(false)
    );
}

#[test]
fn dates_are_listed_and_counted_as_in_deephaven() {
    let ny = fixture("ny");
    let texts = |days: Vec<i64>| days.into_iter().map(date_text).collect::<Vec<_>>();
    let span = |start: &str, end: &str, start_in: bool, end_in: bool, which: Dates| {
        texts(
            ny.dates(day(start), day(end), start_in, end_in, which)
                .unwrap(),
        )
    };
    assert_eq!(
        span("2024-01-01", "2024-01-10", true, true, Dates::Business),
        [
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09",
            "2024-01-10"
        ]
    );
    assert_eq!(
        span("2024-01-02", "2024-01-10", false, false, Dates::Business),
        [
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-08",
            "2024-01-09"
        ]
    );
    assert!(span("2024-01-10", "2024-01-01", true, true, Dates::Business).is_empty());
    assert_eq!(
        span("2023-12-25", "2024-01-08", true, true, Dates::NonBusiness),
        [
            "2023-12-29",
            "2023-12-30",
            "2023-12-31",
            "2024-01-01",
            "2024-01-06",
            "2024-01-07"
        ]
    );
    // calendar dates need no valid range
    assert_eq!(
        span("2022-12-30", "2023-01-02", true, true, Dates::All),
        ["2022-12-30", "2022-12-31", "2023-01-01", "2023-01-02"]
    );

    let count = |start: &str, end: &str, start_in: bool, end_in: bool, which: Dates| {
        ny.number_of_dates(day(start), day(end), start_in, end_in, which)
    };
    assert_eq!(
        count("2024-01-01", "2024-12-31", true, true, Dates::Business),
        Ok(260)
    );
    assert_eq!(
        count("2023-01-01", "2025-12-31", true, true, Dates::Business),
        Ok(780)
    );
    assert_eq!(
        count("2024-01-01", "2024-12-31", false, false, Dates::Business),
        Ok(259)
    );
    assert_eq!(
        count("2024-01-01", "2024-12-31", true, true, Dates::NonBusiness),
        Ok(106)
    );
    assert_eq!(
        count("2024-01-01", "2024-12-31", true, true, Dates::All),
        Ok(366)
    );
    assert_eq!(
        count("2024-01-05", "2024-01-01", true, true, Dates::Business),
        Ok(0)
    );
    assert_eq!(
        count("2022-12-01", "2024-01-01", true, true, Dates::All),
        Ok(397)
    );
    assert!(java_message(
        count("2024-01-01", "2026-02-01", true, true, Dates::Business).unwrap_err()
    )
    .ends_with("date=2026-01-01 lastValidDate=2025-12-31"));
    assert_eq!(
        fixture("odd").number_of_dates(
            day("2024-01-01"),
            day("2024-03-04"),
            true,
            true,
            Dates::Business
        ),
        Ok(55)
    );
}

#[test]
fn days_are_added_as_in_deephaven() {
    let ny = fixture("ny");
    let plus = |start: &str, days: i32, which: Dates| {
        ny.plus_days(day(start), days, which)
            .map(|found| found.map(date_text))
    };
    // no business day is zero business days from a holiday
    assert_eq!(plus("2024-01-01", 0, Dates::Business), Ok(None));
    assert_eq!(
        plus("2024-01-02", 0, Dates::Business),
        Ok(Some("2024-01-02".into()))
    );
    assert_eq!(
        plus("2024-01-02", 3, Dates::Business),
        Ok(Some("2024-01-05".into()))
    );
    assert_eq!(
        plus("2024-01-05", 1, Dates::Business),
        Ok(Some("2024-01-08".into()))
    );
    assert_eq!(
        plus("2024-01-02", -1, Dates::Business),
        Ok(Some("2023-12-28".into()))
    );
    assert!(plus("2025-12-30", 3, Dates::Business).is_err());
    assert!(
        java_message(plus("2023-01-03", -5, Dates::Business).unwrap_err())
            .ends_with("date=2022-12-31 firstValidDate=2023-01-01")
    );
    assert_eq!(plus("2024-01-02", 0, Dates::NonBusiness), Ok(None));
    assert_eq!(
        plus("2024-01-02", 2, Dates::NonBusiness),
        Ok(Some("2024-01-07".into()))
    );
    assert_eq!(
        plus("2024-01-02", -2, Dates::NonBusiness),
        Ok(Some("2023-12-31".into()))
    );
    assert_eq!(
        plus("2099-01-02", 5, Dates::All),
        Ok(Some("2099-01-07".into()))
    );

    // at the same time of day, in the calendar's zone, across a change of its offset
    assert_eq!(
        ny.plus_days_at_time(utc("2024-01-05", 20, 0), 1, Dates::Business),
        Ok(Some(utc("2024-01-08", 20, 0)))
    );
    assert_eq!(
        ny.plus_days_at_time(utc("2024-03-08", 7, 30), 1, Dates::Business),
        Ok(Some(utc("2024-03-11", 6, 30)))
    );
    assert_eq!(
        ny.plus_days_at_time(utc("2024-03-09", 12, 0), 1, Dates::All),
        Ok(Some(utc("2024-03-10", 11, 0)))
    );
    assert_eq!(
        ny.plus_days_at_time(utc("2024-01-01", 20, 0), 0, Dates::Business),
        Ok(None)
    );
}

#[test]
fn business_time_between_instants_is_as_in_deephaven() {
    let ny = fixture("ny");
    let (start, end) = (utc("2024-07-03", 15, 0), utc("2024-07-08", 15, 0));
    assert_eq!(ny.diff_business_nanos(start, end), Ok(52_200_000_000_000));
    assert_eq!(ny.diff_business_nanos(end, start), Ok(-52_200_000_000_000));
    assert_eq!(
        ny.diff_non_business_nanos(start, end),
        Ok(379_800_000_000_000)
    );
    assert_eq!(
        ny.diff_non_business_nanos(end, start),
        Ok(-379_800_000_000_000)
    );
    assert_eq!(
        ny.diff_business_nanos(utc("2024-07-04", 15, 0), utc("2024-07-04", 18, 0)),
        Ok(7_200_000_000_000)
    );
    assert_eq!(
        ny.diff_business_nanos(utc("2023-12-15", 15, 0), utc("2025-02-07", 18, 0)),
        Ok(5_869_800_000_000_000)
    );
    assert_eq!(ny.diff_business_years(start, end), Ok(0.010175438596491228));
    assert_eq!(
        ny.diff_business_years(start, utc("2025-07-08", 15, 0)),
        Ok(1.007788227098672)
    );
    assert_eq!(
        ny.diff_business_years(utc("2023-07-03", 15, 0), utc("2025-07-08", 15, 0)),
        Ok(2.0114713907817077)
    );
    assert_eq!(
        ny.diff_business_years(utc("2024-12-31", 15, 0), utc("2025-01-01", 15, 0)),
        Ok(0.003857082622971575)
    );
    // the start is not put before the end
    assert!(java_message(
        ny.diff_business_years(utc("2025-07-08", 15, 0), start)
            .unwrap_err()
    )
    .ends_with("date=2026-01-01 lastValidDate=2025-12-31"));
    assert_eq!(
        java_message(
            fixture("odd")
                .diff_business_years(utc("2024-01-03", 15, 0), utc("2024-01-08", 15, 0))
                .unwrap_err()
        ),
        "io.deephaven.time.calendar.BusinessCalendar$InvalidDateException: Business calendar does not contain a \
         complete year for: year=2024"
    );
    assert_eq!(
        fixture("odd").diff_business_nanos(utc("2024-01-04", 22, 0), utc("2024-01-05", 2, 30)),
        Ok(9_000_000_000_000)
    );

    // an inclusive close counts one nanosecond
    let allday = fixture("allday");
    assert_eq!(
        allday.diff_business_nanos(utc("2024-03-09", 15, 0), utc("2024-03-11", 15, 0)),
        Ok(95_400_000_000_000)
    );
    assert_eq!(
        allday.diff_business_nanos(utc("2024-11-02", 15, 0), utc("2024-11-04", 15, 0)),
        Ok(86_400_000_000_001)
    );
    assert_eq!(
        allday.diff_business_nanos(utc("2023-07-01", 15, 0), utc("2026-01-04", 15, 0)),
        Ok(79_151_400_000_000_001)
    );
    assert_eq!(
        allday.diff_business_years(utc("2024-01-03", 15, 0), utc("2024-01-08", 15, 0)),
        Ok(0.0137323339245866)
    );
    assert_eq!(
        allday.diff_business_years(utc("2024-01-03", 15, 0), utc("2025-01-08", 15, 0)),
        Ok(1.013682339973002)
    );
}

#[test]
fn the_collection_of_calendars_behaves_as_deephavens() {
    let mut registry = Calendars::new();
    let valid = |name: &str| !name.contains(' ');
    assert_eq!(registry.default_name(), Some("UTC"));
    assert_eq!(
        registry.names(),
        ["USBANK_EXAMPLE", "USNYSE_EXAMPLE", "UTC"]
    );
    // the calendars of the names Deephaven ships are there from the start, UTC the default
    let utc = registry.get("utc").unwrap();
    assert!(Arc::ptr_eq(
        &utc,
        &registry.default_calendar().unwrap().unwrap()
    ));
    assert_eq!(utc.name(), "UTC");
    assert_eq!(
        registry.get("usNYSE_example").unwrap().zone().id(),
        "America/New_York"
    );

    let ny = Arc::new(fixture("ny"));
    registry.add(ny.clone(), &valid).unwrap();
    // the same calendar again changes nothing, another of the name is refused
    registry.add(ny.clone(), &valid).unwrap();
    assert_eq!(
        java_message(registry.add(Arc::new(fixture("ny")), &valid).unwrap_err()),
        "java.lang.IllegalArgumentException: Multiple calendars have the same name: name='OXTEST_NY'"
    );
    // calendars are found by the upper case of their names
    let odd = Arc::new(fixture("odd"));
    registry.add(odd, &valid).unwrap();
    assert_eq!(registry.get("OXTEST_odd").unwrap().name(), "oxTest_odd");
    assert_eq!(
        java_message(registry.get(" OXTEST_ODD ").unwrap_err()),
        "java.lang.IllegalArgumentException: No such calendar:  OXTEST_ODD "
    );
    assert_eq!(
        registry.names(),
        [
            "OXTEST_NY",
            "OXTEST_ODD",
            "USBANK_EXAMPLE",
            "USNYSE_EXAMPLE",
            "UTC"
        ]
    );
    // the name of a calendar Deephaven ships is taken
    let utc_text = calendar_of(
        "<name>utc</name><timeZone>UTC</timeZone><firstValidDate>2024-01-01</firstValidDate>\
         <lastValidDate>2024-12-31</lastValidDate><default/>",
    );
    assert_eq!(
        java_message(
            registry
                .add(Arc::new(read(&utc_text).unwrap()), &valid)
                .unwrap_err()
        ),
        "java.lang.IllegalArgumentException: Multiple calendars have the same name: name='UTC'"
    );
    let spaced = calendar_of(
        "<name>bad name</name><timeZone>UTC</timeZone><firstValidDate>2024-01-01</firstValidDate>\
         <lastValidDate>2024-12-31</lastValidDate><default/>",
    );
    assert_eq!(
        java_message(
            registry
                .add(Arc::new(read(&spaced).unwrap()), &valid)
                .unwrap_err()
        ),
        "java.lang.IllegalArgumentException: Invalid name for calendar: name='BAD NAME'"
    );

    // a name in lower case removes nothing
    registry.remove("oxtest_ny");
    assert!(registry.get("OXTEST_NY").is_ok());
    registry.remove("OXTEST_NY");
    assert!(registry.get("OXTEST_NY").is_err());
    registry.remove("OXTEST_NY");
    // removing a shipped name forgets it
    registry.remove("UTC");
    assert_eq!(
        registry.names(),
        ["OXTEST_ODD", "USBANK_EXAMPLE", "USNYSE_EXAMPLE"]
    );

    // the default is any name, found when it is asked for
    registry.set_default("nope");
    assert_eq!(registry.default_name(), Some("nope"));
    assert_eq!(
        java_message(registry.default_calendar().unwrap_err()),
        "java.lang.IllegalArgumentException: No such calendar: nope"
    );
    registry.set_default("oxtest_odd");
    assert_eq!(
        registry.default_calendar().unwrap().unwrap().name(),
        "oxTest_odd"
    );

    // a file must be named .calendar
    let zone = |text: &str| CalendarZone::parse(text);
    assert_eq!(
        java_message(registry.add_from_file("", &zone, &valid).unwrap_err()),
        "java.lang.UnsupportedOperationException: Calendar file must be in .calendar format"
    );
    let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/src/calendar/fixtures/");
    assert_eq!(
        java_message(
            registry
                .add_from_file(&format!("{fixtures}OXTEST_ALLDAY.xml"), &zone, &valid)
                .unwrap_err()
        ),
        "java.lang.UnsupportedOperationException: Calendar file must be in .calendar format"
    );
    let missing = format!("{fixtures}missing.calendar");
    assert!(java_message(registry.add_from_file(&missing, &zone, &valid).unwrap_err()).starts_with(&format!(
        "java.lang.RuntimeException: Unable to load calendar file: file={missing}, caused by java.io.FileNotFoundException: {missing}"
    )));
    let added = registry
        .add_from_file(&format!("{fixtures}OXTEST_ALLDAY.calendar"), &zone, &valid)
        .unwrap();
    assert!(Arc::ptr_eq(&added, &registry.get("oxtest_allday").unwrap()));
}

#[test]
fn time_zones_are_named_as_in_java() {
    let id = |text: &str| CalendarZone::parse(text).map(|zone| zone.id().to_string());
    assert_eq!(id("UTC+1"), Ok("UTC+01:00".into()));
    assert_eq!(id("Z"), Ok("Z".into()));
    assert_eq!(id("+05:30"), Ok("+05:30".into()));
    assert_eq!(id("GMT"), Ok("GMT".into()));
    assert_eq!(id("JP"), Ok("Asia/Tokyo".into()));
    assert_eq!(id("America/New_York"), Ok("America/New_York".into()));
    assert_eq!(
        id("nope").map_err(java_message),
        Err("java.time.zone.ZoneRulesException: Unknown time-zone ID: nope".into())
    );
}
