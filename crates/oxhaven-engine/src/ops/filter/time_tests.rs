//! Match and range filters on the time column types (task 4.22). The expected rows are
//! those the Java 42.9 reference kept for the same table and filters.

use super::tests::static_table;
use super::{unstructured_filter, FilterError};
use arrow::array::{Date32Array, DurationNanosecondArray, Int8Array, Time64NanosecondArray};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use std::sync::Arc;

const HOUR: i64 = 3_600_000_000_000;
const DAY: i64 = 24 * HOUR;

fn kept(table: &Arc<dyn Table>, filter: &str) -> Vec<i64> {
    let result = unstructured_filter(table, &[filter], &Compiler)
        .unwrap_or_else(|e| panic!("{filter}: {e}"));
    result.row_set().iter().map(|key| key / 10).collect()
}

fn refused(table: &Arc<dyn Table>, filter: &str) {
    match unstructured_filter(table, &[filter], &Compiler) {
        Err(FilterError::Invalid(_)) => {}
        Err(other) => panic!("{filter}: refused otherwise: {other}"),
        Ok(_) => panic!("{filter} was accepted"),
    }
}

/// The table of the 4.22 probes: an instant of 2024-03-10T00:00:00Z plus `(5 - i) * 7`
/// hours, null in row 2, and from it `D` (its UTC date), `LT` (its UTC time of day), `W`
/// (its UTC day of the week) and `Z` (it in New York in the even rows, in UTC in the odd
/// ones); `Dur` is an hour in rows 0 and 3, two minutes back elsewhere and null in row 4.
fn time_table() -> Arc<dyn Table> {
    let base = 1_710_028_800_000_000_000i64;
    let instants: Vec<Option<i64>> = (0..6)
        .map(|i| (i != 2).then(|| base + (5 - i) * 7 * HOUR))
        .collect();
    let days: Date32Array = instants
        .iter()
        .map(|n| n.map(|n| n.div_euclid(DAY) as i32))
        .collect();
    let times: Time64NanosecondArray = instants
        .iter()
        .map(|n| n.map(|n| n.rem_euclid(DAY)))
        .collect();
    // 1970-01-01 was a Thursday, day 4.
    let weekdays: Int8Array = instants
        .iter()
        .map(|n| n.map(|n| ((n.div_euclid(DAY) + 3).rem_euclid(7) + 1) as i8))
        .collect();
    let durations: DurationNanosecondArray = (0..6)
        .map(|i| (i != 4).then_some(if i % 3 == 0 { HOUR } else { -120_000_000_000 }))
        .collect();
    let zoned: Vec<Option<(i64, &str)>> = instants
        .iter()
        .enumerate()
        .map(|(i, n)| {
            n.map(|n| {
                (
                    n,
                    if i % 2 == 0 {
                        "America/New_York"
                    } else {
                        "UTC"
                    },
                )
            })
        })
        .collect();
    static_table(vec![
        ("D", ColumnType::LocalDate, Arc::new(days)),
        ("LT", ColumnType::LocalTime, Arc::new(times)),
        ("Dur", ColumnType::Duration, Arc::new(durations)),
        ("W", ColumnType::DayOfWeek, Arc::new(weekdays)),
        (
            "Z",
            ColumnType::ZonedDateTime,
            crate::ops::zoned::tests::zoned(&zoned),
        ),
    ])
}

/// Behavior: the `LocalDate` and `LocalTime` convertors of
/// io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertorFactory#getConvertor
/// and SingleSidedComparableRangeFilter, null lowest.
#[test]
fn dates_and_times_of_day_match_and_range_like_java() {
    let table = time_table();
    assert_eq!(kept(&table, "D == '2024-03-10'"), [3, 4, 5]);
    assert_eq!(kept(&table, "D = '2024-03-10'"), [3, 4, 5]);
    assert_eq!(kept(&table, "D == '2024-3-10'"), [3, 4, 5]);
    assert_eq!(kept(&table, "D != '2024-03-10'"), [0, 1, 2]);
    assert_eq!(kept(&table, "D in '2024-03-10', null"), [2, 3, 4, 5]);
    assert_eq!(kept(&table, "D not in '2024-03-11'"), [2, 3, 4, 5]);
    assert_eq!(kept(&table, "D == null"), [2]);
    assert_eq!(kept(&table, "D < '2024-03-11'"), [2, 3, 4, 5]);
    assert_eq!(kept(&table, "D >= '2024-03-11'"), [0, 1]);
    assert_eq!(kept(&table, "D > null"), [0, 1, 3, 4, 5]);
    assert_eq!(kept(&table, "D <= null"), [2]);
    assert_eq!(kept(&table, "LT == '04:00'"), [1]);
    assert_eq!(kept(&table, "LT == '4:00'"), [1]);
    assert_eq!(kept(&table, "LT < '07:00:00'"), [1, 2, 5]);
    assert_eq!(kept(&table, "LT > '07:00:00.000000001'"), [0, 3]);
    assert_eq!(kept(&table, "LT in '04:00', '14:00:00'"), [1, 3]);
    refused(&table, "D == '2024-03-10T00:00'");
}

/// Behavior: the `Enum` convertor (`Enum.valueOf`, the exact name, no `null`) and the
/// convertor of a class without one (`null` alone) of
/// MatchFilter.ColumnTypeConvertorFactory#getConvertor; RangeFilter#init has no filter for
/// either type.
#[test]
fn days_and_durations_convert_as_java_does() {
    let table = time_table();
    assert_eq!(kept(&table, "W == MONDAY"), [0, 1]);
    assert_eq!(kept(&table, "W = SUNDAY"), [3, 4, 5]);
    assert_eq!(kept(&table, "W != MONDAY"), [2, 3, 4, 5]);
    assert_eq!(kept(&table, "W in MONDAY, SUNDAY"), [0, 1, 3, 4, 5]);
    assert_eq!(kept(&table, "W not in MONDAY"), [2, 3, 4, 5]);
    refused(&table, "W in null");
    refused(&table, "W == monday");
    refused(&table, "W < SUNDAY");
    assert_eq!(kept(&table, "Dur == null"), [4]);
    assert_eq!(kept(&table, "Dur != null"), [0, 1, 2, 3, 5]);
    refused(&table, "Dur in 'PT1H', 'PT-2M'");
    refused(&table, "Dur not in 'PT1H'");
}

/// Behavior: java.time.ZonedDateTime#equals (the instant in the same zone) for a match,
/// java.time.chrono.ChronoZonedDateTime#compareTo for a range.
#[test]
fn zoned_date_times_match_with_their_zone_and_range_by_compare_to() {
    let table = time_table();
    assert_eq!(kept(&table, "Z == '2024-03-10T00:00:00 UTC'"), [5]);
    assert!(kept(&table, "Z == '2024-03-10T07:00:00 UTC'").is_empty());
    assert_eq!(
        kept(&table, "Z == '2024-03-10T03:00:00 America/New_York'"),
        [4]
    );
    assert_eq!(
        kept(&table, "Z != '2024-03-10T00:00:00 UTC'"),
        [0, 1, 2, 3, 4]
    );
    assert_eq!(kept(&table, "Z in '2024-03-10T00:00:00 UTC', null"), [2, 5]);
    assert_eq!(kept(&table, "Z == null"), [2]);
    // Row 4 is this instant in New York, whose local time is earlier than in UTC.
    assert_eq!(kept(&table, "Z < '2024-03-10T07:00:00 UTC'"), [2, 4, 5]);
    assert_eq!(kept(&table, "Z <= '2024-03-10T07:00:00 UTC'"), [2, 4, 5]);
    assert_eq!(kept(&table, "Z >= null"), [0, 1, 2, 3, 4, 5]);
}
