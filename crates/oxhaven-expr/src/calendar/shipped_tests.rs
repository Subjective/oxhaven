//! The calendars the collection starts with (`shipped/`, written from public holiday rules
//! by `shipped/generate.py`) against what Deephaven 42.9 answers for its calendars of the
//! same names.
//!
//! `fixtures/shipped-deephaven-42.9.txt` was recorded from the reference server by the
//! scripts of `tasks/12.5-shipped-calendars/`: for every date each calendar covers, a
//! summary per year (business days, business nanoseconds, and a hash of each day's
//! business hours); the answers of a sample of calendar functions; and a hash of the
//! holidays each calendar lists. Every date where the calendars differ is in
//! [`DIFFERENCES`] or [`LISTED_ONLY_HERE`], with the public source that decides it in
//! `tasks/12.5-shipped-calendars.md`.

use std::collections::BTreeMap;

use super::*;

const RECORDED: &str = include_str!("fixtures/shipped-deephaven-42.9.txt");

const MINUTE: i64 = 60_000_000_000;

/// The business hours of a day: none, or from an opening to a close, in minutes after
/// midnight in the calendar's zone.
type Hours = Option<(i64, i64)>;

const NYSE: Hours = Some((9 * 60 + 30, 16 * 60));
const NYSE_EARLY: Hours = Some((9 * 60 + 30, 13 * 60));
const BANK: Hours = Some((9 * 60, 17 * 60));
/// Deephaven's hours of Christmas Eve of 2024 and 2025: an hour late.
const NYSE_EVE_DEEPHAVEN: Hours = Some((10 * 60 + 30, 14 * 60));

/// The dates where Oxhaven's calendar and Deephaven's differ: calendar, date, Oxhaven's
/// hours, Deephaven's hours.
const DIFFERENCES: [(&str, &str, Hours, Hours); 12] = [
    ("USNYSE_EXAMPLE", "1999-11-26", NYSE_EARLY, NYSE),
    ("USNYSE_EXAMPLE", "2000-07-03", NYSE_EARLY, NYSE),
    ("USNYSE_EXAMPLE", "2000-11-24", NYSE_EARLY, NYSE),
    ("USNYSE_EXAMPLE", "2001-07-03", NYSE_EARLY, NYSE),
    ("USNYSE_EXAMPLE", "2001-11-23", NYSE_EARLY, NYSE),
    ("USNYSE_EXAMPLE", "2001-12-24", NYSE_EARLY, NYSE),
    ("USNYSE_EXAMPLE", "2022-06-20", None, NYSE),
    (
        "USNYSE_EXAMPLE",
        "2024-12-24",
        NYSE_EARLY,
        NYSE_EVE_DEEPHAVEN,
    ),
    ("USNYSE_EXAMPLE", "2025-01-09", None, NYSE),
    (
        "USNYSE_EXAMPLE",
        "2025-12-24",
        NYSE_EARLY,
        NYSE_EVE_DEEPHAVEN,
    ),
    ("USBANK_EXAMPLE", "2019-11-26", BANK, None),
    ("USBANK_EXAMPLE", "2019-11-28", None, BANK),
];

/// Holidays before the first valid date that only Oxhaven's calendar lists.
const LISTED_ONLY_HERE: [(&str, &str); 1] = [("USBANK_EXAMPLE", "2009-01-19")];

fn shipped(name: &str) -> BusinessCalendar {
    shipped_calendar(name).expect("a shipped calendar")
}

fn date(text: &str) -> i64 {
    parse_date(text).expect("a date")
}

/// An instant of the text Java's `Instant#toString` gives for whole seconds.
fn instant(text: &str) -> i64 {
    let (day, time) = text
        .strip_suffix('Z')
        .and_then(|text| text.split_once('T'))
        .expect("an instant");
    let seconds: Vec<i64> = time.split(':').map(|part| part.parse().unwrap()).collect();
    date(day) * NANOS_PER_DAY + ((seconds[0] * 60 + seconds[1]) * 60 + seconds[2]) * 1_000_000_000
}

fn minutes(calendar: &BusinessCalendar, nanos: i64) -> i64 {
    let time = calendar.zone().local_of(nanos).unwrap().1;
    assert_eq!(time % MINUTE, 0);
    time / MINUTE
}

/// The hours of a day that has one range of business time or none.
fn hours_of(calendar: &BusinessCalendar, day: &CalendarDay) -> Hours {
    match day.ranges() {
        [] => None,
        [range] => Some((minutes(calendar, range.start), minutes(calendar, range.end))),
        ranges => panic!("a day of {} ranges", ranges.len()),
    }
}

fn day_of(calendar: &BusinessCalendar, day: i64, hours: Hours) -> CalendarDay {
    let Some((open, close)) = hours else {
        return CalendarDay::holiday();
    };
    let range = TimeRange::new(open * MINUTE, close * MINUTE, false, TimeKind::LocalTime);
    CalendarDay::new(vec![range.unwrap()])
        .unwrap()
        .on_date(day, calendar.zone())
        .unwrap()
}

fn fnv(state: u64, text: &str) -> u64 {
    text.bytes().fold(state, |state, byte| {
        (state ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
    })
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// Business days, business nanoseconds and the FNV-1a hash of one line per day
/// (`<days since 1970>:<B or N>:<start>,<end>,<0 or 1>;...`), per year.
fn years(calendar: &BusinessCalendar, replace: &BTreeMap<i64, Hours>) -> BTreeMap<i64, String> {
    let mut found: BTreeMap<i64, (i64, i64, u64)> = BTreeMap::new();
    for day in calendar.first_valid_date()..=calendar.last_valid_date() {
        let hours = match replace.get(&day) {
            Some(hours) => day_of(calendar, day, *hours),
            None => calendar.calendar_day(day).unwrap(),
        };
        let mut line = format!("{day}:{}:", if hours.is_business_day() { 'B' } else { 'N' });
        for range in hours.ranges() {
            let inclusive = u8::from(range.inclusive_end);
            line += &format!("{},{},{inclusive};", range.start, range.end);
        }
        line.push('\n');
        let entry = found
            .entry(civil_from_days(day).0)
            .or_insert((0, 0, FNV_OFFSET));
        entry.0 += i64::from(hours.is_business_day());
        entry.1 += hours.business_nanos();
        entry.2 = fnv(entry.2, &line);
    }
    found
        .into_iter()
        .map(|(year, (days, nanos, hash))| (year, format!("{days}|{nanos}|{hash:016x}")))
        .collect()
}

fn recorded(kind: &str, name: &str) -> Vec<String> {
    RECORDED
        .lines()
        .filter_map(|line| line.strip_prefix(kind)?.strip_prefix('|'))
        .filter_map(|line| line.strip_prefix(name)?.strip_prefix('|'))
        .map(str::to_string)
        .collect()
}

fn recorded_years(name: &str) -> BTreeMap<i64, String> {
    recorded("year", name)
        .iter()
        .map(|line| {
            let (year, rest) = line.split_once('|').unwrap();
            (year.parse().unwrap(), rest.to_string())
        })
        .collect()
}

fn time_text(minutes: i64) -> String {
    format!("{:02}:{:02}", minutes / 60, minutes % 60)
}

/// The count and hash of `<date>|<open>-<close>;...` per holiday, in order of date.
fn holiday_summary(holidays: &BTreeMap<i64, Hours>) -> String {
    let mut state = FNV_OFFSET;
    for (day, hours) in holidays {
        let periods = hours
            .map(|(open, close)| format!("{}-{};", time_text(open), time_text(close)))
            .unwrap_or_default();
        state = fnv(state, &format!("{}|{periods}\n", date_text(*day)));
    }
    format!("{}|{state:016x}", holidays.len())
}

#[test]
fn the_shipped_calendars_have_deephavens_names_zones_hours_and_ranges() {
    let registry = Calendars::new();
    assert_eq!(
        registry.names(),
        ["USBANK_EXAMPLE", "USNYSE_EXAMPLE", "UTC"]
    );
    assert_eq!(registry.default_name(), Some("UTC"));
    for (name, zone, first, last, weekend, open, close) in [
        (
            "UTC",
            "UTC",
            "1900-01-01",
            "2100-12-31",
            vec![],
            0,
            NANOS_PER_DAY - 1,
        ),
        (
            "USNYSE_EXAMPLE",
            "America/New_York",
            "1999-01-01",
            "2027-12-31",
            vec![6, 7],
            (9 * 60 + 30) * MINUTE,
            16 * 60 * MINUTE,
        ),
        (
            "USBANK_EXAMPLE",
            "America/New_York",
            "2013-01-01",
            "2027-12-31",
            vec![6, 7],
            9 * 60 * MINUTE,
            17 * 60 * MINUTE,
        ),
    ] {
        let calendar = registry.get(name).unwrap();
        assert_eq!(calendar.name(), name);
        assert_eq!(calendar.zone().id(), zone, "{name}");
        assert_eq!(calendar.first_valid_date(), date(first), "{name}");
        assert_eq!(calendar.last_valid_date(), date(last), "{name}");
        assert_eq!(calendar.weekend_days(), weekend, "{name}");
        let standard = calendar.standard_business_day().ranges();
        assert_eq!(standard.len(), 1, "{name}");
        assert_eq!(
            (standard[0].start, standard[0].end),
            (open, close),
            "{name}"
        );
        assert_eq!(standard[0].inclusive_end, name == "UTC", "{name}");
    }
    assert_eq!(
        registry.get("UTC").unwrap().text(),
        "Calendar{name='UTC', description='UTC', timeZone=UTC}"
    );
}

/// Every date of every calendar, by year: the same as Deephaven's but on the dates of
/// [`DIFFERENCES`], each of which is as listed there.
#[test]
fn the_shipped_calendars_hold_deephavens_days_but_the_listed_differences() {
    for name in ["UTC", "USNYSE_EXAMPLE", "USBANK_EXAMPLE"] {
        let calendar = shipped(name);
        let mut replace = BTreeMap::new();
        for (_, day, ours, deephavens) in DIFFERENCES.iter().filter(|entry| entry.0 == name) {
            let hours = calendar.calendar_day(date(day)).unwrap();
            assert_eq!(hours_of(&calendar, &hours), *ours, "{name} {day}");
            assert_ne!(ours, deephavens);
            replace.insert(date(day), *deephavens);
        }
        let recorded = recorded_years(name);
        assert!(!recorded.is_empty(), "{name}");
        assert_eq!(years(&calendar, &replace), recorded, "{name}");
        if !replace.is_empty() {
            assert_ne!(years(&calendar, &BTreeMap::new()), recorded, "{name}");
        }
    }
}

/// The holidays a calendar lists (`holidays()`, which `deephaven.plot.express` sends a
/// client for a figure with a calendar): Deephaven's but for the listed differences.
#[test]
fn the_shipped_calendars_list_deephavens_holidays_but_the_listed_differences() {
    for (name, standard) in [("USNYSE_EXAMPLE", NYSE), ("USBANK_EXAMPLE", BANK)] {
        let calendar = shipped(name);
        let ours: BTreeMap<i64, Hours> = calendar
            .holidays()
            .iter()
            .map(|(day, hours)| (*day, hours_of(&calendar, hours)))
            .collect();
        let mut deephavens = ours.clone();
        for (_, day, _, hours) in DIFFERENCES.iter().filter(|entry| entry.0 == name) {
            if *hours == standard {
                deephavens.remove(&date(day));
            } else {
                deephavens.insert(date(day), *hours);
            }
        }
        for (_, day) in LISTED_ONLY_HERE.iter().filter(|entry| entry.0 == name) {
            assert!(date(day) < calendar.first_valid_date());
            assert_eq!(deephavens.remove(&date(day)), Some(None), "{name} {day}");
        }
        assert_eq!(
            [holiday_summary(&deephavens)],
            recorded("holidays", name)[..],
            "{name}"
        );
        assert_ne!(
            [holiday_summary(&ours)],
            recorded("holidays", name)[..],
            "{name}"
        );
    }
    assert!(shipped("UTC").holidays().is_empty());
}

/// A sample of the calendar functions, at dates and instants drawn at random over each
/// calendar's range, against Deephaven's answers. A sample whose answer can depend on a
/// date of [`DIFFERENCES`] is left out, and counted.
#[test]
fn the_shipped_calendars_answer_as_deephavens() {
    let calendars: BTreeMap<&str, BusinessCalendar> = ["UTC", "USNYSE_EXAMPLE", "USBANK_EXAMPLE"]
        .into_iter()
        .map(|name| (name, shipped(name)))
        .collect();
    let mut compared = 0;
    let mut left_out = 0;
    for line in RECORDED.lines() {
        let Some(sample) = line.strip_prefix("sample|") else {
            continue;
        };
        let parts: Vec<&str> = sample.split('|').collect();
        let [name, _, a, b, n, kind, expected] = parts[..] else {
            panic!("a sample line: {line}");
        };
        let calendar = &calendars[name];
        let n: i32 = n.parse().unwrap();
        let (start, end) = if a.contains('T') {
            (instant(a), instant(b))
        } else {
            (date(a), date(b))
        };
        // the dates the answer can depend on: those between the two, those a count of
        // business days can reach, and for business years the whole years of both ends
        let (mut low, mut high) = if a.contains('T') {
            (
                start.min(end).div_euclid(NANOS_PER_DAY) - 1,
                start.max(end).div_euclid(NANOS_PER_DAY) + 1,
            )
        } else {
            (start.min(end), start.max(end))
        };
        if kind == "dby" {
            low = days_from_civil(civil_from_days(low).0, 1, 1);
            high = days_from_civil(civil_from_days(high).0 + 1, 1, 1);
        }
        let reach = i64::from(n.abs()) * 3 + 10;
        let touches = DIFFERENCES.iter().any(|(differs_in, day, _, _)| {
            *differs_in == name && (low - reach..=high + reach).contains(&date(day))
        });
        if touches {
            left_out += 1;
            continue;
        }
        let day_answer = |day: Result<Option<i64>, CalendarError>| match day {
            Ok(Some(day)) => date_text(day),
            Ok(None) => "None".to_string(),
            Err(error) => format!("ERR:{error}"),
        };
        let answer = match kind {
            "nbd" => text(calendar.number_of_dates(start, end, true, true, Dates::Business)),
            "nbdx" => text(calendar.number_of_dates(start, end, false, true, Dates::Business)),
            "nnbd" => text(calendar.number_of_dates(start, end, true, true, Dates::NonBusiness)),
            "next" => day_answer(calendar.plus_days(start, 1, Dates::Business)),
            "prev" => day_answer(calendar.plus_days(start, -1, Dates::Business)),
            "plus" => day_answer(calendar.plus_days(start, n, Dates::Business)),
            "minus" => day_answer(calendar.plus_days(start, -n, Dates::Business)),
            "plusnb" => day_answer(calendar.plus_days(start, n, Dates::NonBusiness)),
            "frac" => number(calendar.fraction_standard_business_day(start), expected),
            "lastm" => boolean(calendar.is_last_business_day_of_month(start)),
            "lastw" => boolean(calendar.is_last_business_day_of_week(start)),
            "bt" => boolean(calendar.is_business_time(start)),
            "fbdc" => number(calendar.fraction_business_day_complete(start), expected),
            "dbn" => text(calendar.diff_business_nanos(start, end)),
            "dby" => number(calendar.diff_business_years(start, end), expected),
            "plusI" => match calendar.plus_days_at_time(start, n, Dates::Business) {
                Ok(Some(nanos)) if expected.ends_with('Z') && instant(expected) == nanos => {
                    expected.to_string()
                }
                Err(error) => format!("ERR:{error}"),
                other => format!("{other:?}"),
            },
            other => panic!("no kind {other}"),
        };
        match expected.strip_prefix("ERR:") {
            // Java's text goes on with a stack trace, and the recording cut it short
            Some(error) => assert!(
                answer
                    .strip_prefix("ERR:")
                    .is_some_and(|answer| error.starts_with(&answer[..answer.len().min(100)])),
                "{line}: {answer}"
            ),
            None => assert_eq!(answer, expected, "{line}"),
        }
        compared += 1;
    }
    // of 1920 samples, those over long spans of the exchange's calendar reach a difference
    assert!(compared >= 1400, "{compared} compared");
    assert!(left_out < compared / 2, "{left_out} left out");
}

fn text<T: fmt::Display>(value: Result<T, CalendarError>) -> String {
    match value {
        Ok(value) => value.to_string(),
        Err(error) => format!("ERR:{error}"),
    }
}

fn boolean(value: Result<bool, CalendarError>) -> String {
    match value {
        Ok(true) => "True".to_string(),
        Ok(false) => "False".to_string(),
        Err(error) => format!("ERR:{error}"),
    }
}

/// The expected text when the answer is the same double, else the answer.
fn number(value: Result<f64, CalendarError>, expected: &str) -> String {
    match value {
        Ok(value)
            if expected
                .parse::<f64>()
                .is_ok_and(|expected| expected.to_bits() == value.to_bits()) =>
        {
            expected.to_string()
        }
        Ok(value) => format!("{value:?}"),
        Err(error) => format!("ERR:{error}"),
    }
}
