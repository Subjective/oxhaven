//! The calendar file format: XML with a `name`, a `timeZone`, the valid dates, a
//! `default` day and `holiday` days.

use std::collections::BTreeMap;
use std::path::Path;

use roxmltree::{Document, Node, ParsingOptions};

use super::{
    parse_date, BusinessCalendar, CalendarDay, CalendarError, CalendarParts, CalendarZone,
    TimeKind, TimeRange, DAY_NAMES,
};
use crate::parse::instant::local_time_nanos;

/// Reads the calendar file at `path`. `zone_of` resolves the text of its `timeZone`.
///
/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#loadBusinessCalendar(File)`,
/// which reports every failure as `Unable to load calendar file`, with the failure as its
/// cause.
pub fn read_calendar_file(
    path: &str,
    zone_of: &dyn Fn(&str) -> Result<CalendarZone, CalendarError>,
) -> Result<BusinessCalendar, CalendarError> {
    let loaded = std::fs::read(Path::new(path))
        .map_err(|error| {
            CalendarError::java("java.io.FileNotFoundException", format!("{path} ({error})"))
        })
        .and_then(|bytes| {
            String::from_utf8(bytes).map_err(|_| {
                CalendarError::unported(
                    "org.jdom2.input.SAXBuilder#build",
                    "a calendar file that is not UTF-8",
                )
            })
        })
        .and_then(|text| parse_calendar(&text, zone_of));
    loaded.map_err(|cause| match cause {
        CalendarError::Java { class, message } => CalendarError::java(
            "java.lang.RuntimeException",
            format!("Unable to load calendar file: file={path}, caused by {class}: {message}"),
        ),
        unported => unported,
    })
}

/// Reads a calendar from the text of a calendar file. `zone_of` resolves the text of its
/// `timeZone`.
///
/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#fill`. Elements are
/// found by their local name without a namespace, the first of a name counts, and the
/// root element may have any name. The text of an element is its own text and CDATA,
/// trimmed as `String#trim` does.
pub fn parse_calendar(
    text: &str,
    zone_of: &dyn Fn(&str) -> Result<CalendarZone, CalendarError>,
) -> Result<BusinessCalendar, CalendarError> {
    let options = ParsingOptions {
        allow_dtd: true,
        ..ParsingOptions::default()
    };
    let document = Document::parse_with_options(text, options).map_err(|error| {
        CalendarError::java(
            "org.xml.sax.SAXParseException",
            format!("Error parsing business calendar: {error}"),
        )
    })?;
    let root = document.root_element();
    let name = text_of(required(root, "name")?);
    let zone = zone_of(&text_of(required(root, "timeZone")?))?;
    let description = child(root, "description").map(text_of);
    let holidays = holidays(root, &zone)?;
    let first_valid = match child(root, "firstValidDate") {
        Some(element) => parse_date(&text_of(element))?,
        None => *holidays.keys().next().ok_or_else(no_holidays)?,
    };
    let last_valid = match child(root, "lastValidDate") {
        Some(element) => parse_date(&text_of(element))?,
        None => *holidays.keys().next_back().ok_or_else(no_holidays)?,
    };
    let default = required(root, "default")?;
    let weekend = weekend_days(default)?;
    let standard = schedule(default)?;
    Ok(BusinessCalendar::new(CalendarParts {
        name,
        description,
        zone,
        first_valid,
        last_valid,
        standard,
        weekend,
        holidays,
    }))
}

/// Behavior: `java.util.Collections#min` of no holidays, when a calendar names no valid
/// date
fn no_holidays() -> CalendarError {
    CalendarError::java("java.util.NoSuchElementException", "")
}

fn child<'a, 'input>(element: Node<'a, 'input>, name: &'static str) -> Option<Node<'a, 'input>> {
    children(element, name).next()
}

fn children<'a, 'input: 'a>(
    element: Node<'a, 'input>,
    name: &'static str,
) -> impl Iterator<Item = Node<'a, 'input>> + 'a {
    element.children().filter(move |node| {
        node.is_element() && node.tag_name().name() == name && node.tag_name().namespace().is_none()
    })
}

/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#getRequiredChild`
fn required<'a, 'input>(
    element: Node<'a, 'input>,
    name: &'static str,
) -> Result<Node<'a, 'input>, CalendarError> {
    child(element, name).ok_or_else(|| {
        CalendarError::java(
            "java.lang.Exception",
            format!(
                "Missing the '{name}' tag in calendar file: text={}",
                text_of(element)
            ),
        )
    })
}

/// Behavior: `org.jdom2.Element#getTextTrim`
fn text_of(element: Node<'_, '_>) -> String {
    let text: String = element
        .children()
        .filter(Node::is_text)
        .filter_map(|node| node.text())
        .collect();
    text.trim_matches(|c: char| c <= ' ').to_string()
}

fn parse_time(text: &str) -> Result<i64, CalendarError> {
    match local_time_nanos(text) {
        Some(Ok(nanos)) => Ok(nanos),
        Some(Err(error)) => Err(CalendarError::unported(
            "io.deephaven.time.DateTimeUtils#parseLocalTime",
            error.to_string(),
        )),
        None => Err(CalendarError::java(
            "io.deephaven.time.DateTimeUtils$DateTimeParseException",
            format!("Cannot parse local time: {text}"),
        )),
    }
}

/// The business hours of `default` or of a holiday: `businessTime` elements, or the older
/// `businessPeriod` elements, not both; neither is a day without business hours.
///
/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#parseCalendarDaySchedule`
fn schedule(element: Node<'_, '_>) -> Result<CalendarDay, CalendarError> {
    let times: Vec<Node> = children(element, "businessTime").collect();
    let periods: Vec<Node> = children(element, "businessPeriod").collect();
    let ranges = match (times.is_empty(), periods.is_empty()) {
        (true, true) => return Ok(CalendarDay::holiday()),
        (false, true) => times
            .into_iter()
            .map(business_time)
            .collect::<Result<Vec<_>, _>>()?,
        (true, false) => periods
            .into_iter()
            .map(business_period)
            .collect::<Result<Vec<_>, _>>()?,
        (false, false) => {
            return Err(CalendarError::java(
                "java.lang.Exception",
                format!(
                    "Cannot have both 'businessTime' and 'businessPeriod' tags in the same element: text={}",
                    text_of(element)
                ),
            ))
        }
    };
    CalendarDay::new(ranges)
}

/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#parseBusinessRanges`:
/// a close from `24:00` on is refused, and the close is included only when
/// `includeClose` is `true` in any case (`Boolean#parseBoolean`).
fn business_time(element: Node<'_, '_>) -> Result<TimeRange, CalendarError> {
    let open = text_of(required(element, "open")?);
    let close = text_of(required(element, "close")?);
    let include_close = child(element, "includeClose").map(text_of);
    if close.starts_with("24:00") {
        return Err(CalendarError::java(
            "java.lang.RuntimeException",
            format!(
                "Close time ({close}) is on the next day.  '23:59:59.999999999' is the maximum close time."
            ),
        ));
    }
    let open = parse_time(&open)?;
    let close = parse_time(&close)?;
    let inclusive = include_close.is_some_and(|text| text.eq_ignore_ascii_case("true"));
    TimeRange::new(open, close, inclusive, TimeKind::LocalTime)
}

/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#parseBusinessRangesLegacy`:
/// `open,close`, split as `String#split` splits, which drops empty fields at the end.
fn business_period(element: Node<'_, '_>) -> Result<TimeRange, CalendarError> {
    let text = text_of(element);
    let mut fields: Vec<&str> = text.split(',').collect();
    while fields.len() > 1 && fields.last() == Some(&"") {
        fields.pop();
    }
    if fields.len() != 2 {
        return Err(CalendarError::illegal_argument(format!(
            "Can not parse business periods; open/close = {}",
            element.text().unwrap_or("")
        )));
    }
    let open = parse_time(fields[0])?;
    let close = parse_time(fields[1])?;
    TimeRange::new(open, close, false, TimeKind::LocalTime)
}

/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#parseHolidays`: a date
/// of eight characters is read as `yyyyMMdd`, whatever the characters; a date given twice
/// has the hours of its last element.
fn holidays(
    root: Node<'_, '_>,
    zone: &CalendarZone,
) -> Result<BTreeMap<i64, CalendarDay>, CalendarError> {
    let mut holidays = BTreeMap::new();
    for holiday in children(root, "holiday") {
        let mut text = text_of(required(holiday, "date")?);
        let chars: Vec<char> = text.chars().collect();
        if chars.len() == 8 {
            let part = |range: std::ops::Range<usize>| chars[range].iter().collect::<String>();
            text = format!("{}-{}-{}", part(0..4), part(4..6), part(6..8));
        }
        let day = parse_date(&text)?;
        let hours = schedule(holiday)?.on_date(day, zone)?;
        holidays.insert(day, hours);
    }
    Ok(holidays)
}

/// Behavior: `io.deephaven.time.calendar.BusinessCalendarXMLParser#parseWeekendDays`: the
/// names of `java.time.DayOfWeek`, in any case.
fn weekend_days(default: Node<'_, '_>) -> Result<Vec<u8>, CalendarError> {
    children(default, "weekend")
        .map(|element| {
            let name = text_of(element).trim().to_uppercase();
            DAY_NAMES
                .iter()
                .position(|day| *day == name)
                .map(|index| index as u8 + 1)
                .ok_or_else(|| {
                    CalendarError::java(
                        "java.lang.Exception",
                        format!("Invalid day of week: day={name}"),
                    )
                })
        })
        .collect()
}
