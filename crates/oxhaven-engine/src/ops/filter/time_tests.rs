//! Match and range filters on the time column types (task 4.22). The expected rows are
//! those the Java 42.9 reference kept for the same table and filters.

use super::tests::static_table;
use super::{unstructured_filter, FilterError};
use arrow::array::{
    Date32Array, DurationNanosecondArray, Int8Array, Time64NanosecondArray,
    TimestampNanosecondArray,
};
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
/// hours, null in row 2, and from it `D` (its UTC date), `LT` (its UTC time of day), `M`
/// (its UTC date and time), `W`
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
    let local = TimestampNanosecondArray::from(instants.clone())
        .with_data_type(ColumnType::LocalDateTime.arrow_type());
    static_table(vec![
        ("D", ColumnType::LocalDate, Arc::new(days)),
        ("LT", ColumnType::LocalTime, Arc::new(times)),
        ("M", ColumnType::LocalDateTime, Arc::new(local)),
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

/// Behavior: the `LocalDateTime` convertor of
/// MatchFilter.ColumnTypeConvertorFactory#getConvertor (DateTimeUtils#parseLocalDateTime)
/// and SingleSidedComparableRangeFilter, null lowest.
#[test]
fn local_date_times_match_and_range_like_java() {
    let table = time_table();
    assert_eq!(kept(&table, "M == '2024-03-10T07:00'"), [4]);
    assert_eq!(kept(&table, "M == '2024-03-10'"), [5]);
    assert_eq!(kept(&table, "M == '2024-03-10T'"), [5]);
    assert_eq!(kept(&table, "M == '2024-3-10T7:00'"), [4]);
    assert_eq!(kept(&table, "M == null"), [2]);
    assert_eq!(kept(&table, "M in null"), [2]);
    assert_eq!(kept(&table, "M != null"), [0, 1, 3, 4, 5]);
    assert_eq!(
        kept(&table, "M in '2024-03-10T07:00', '2024-03-11T04:00:00.000'"),
        [1, 4]
    );
    assert_eq!(
        kept(&table, "M not in '2024-03-10T07:00', null"),
        [0, 1, 3, 5]
    );
    assert_eq!(kept(&table, "M > '2024-03-10T07:00'"), [0, 1, 3]);
    assert_eq!(kept(&table, "M < '2024-03-10T24:00'"), [2, 3, 4, 5]);
    assert_eq!(kept(&table, "M <= null"), [2]);
    assert_eq!(kept(&table, "M >= null"), [0, 1, 2, 3, 4, 5]);
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

/// Behavior: RangeFilter#init refuses a `DayOfWeek` and a `Duration`, and the filter falls
/// back to its formula, `QueryLanguageFunctionUtils#greaterEquals(Comparable, Comparable)`
/// and its relatives, which order null first. Seen on Java 42.9.
#[test]
fn days_and_durations_range_with_null_as_formulas() {
    let table = time_table();
    assert_eq!(kept(&table, "W >= null"), [0, 1, 2, 3, 4, 5]);
    assert_eq!(kept(&table, "W > null"), [0, 1, 3, 4, 5]);
    assert!(kept(&table, "W < null").is_empty());
    assert_eq!(kept(&table, "W <= null"), [2]);
    assert_eq!(kept(&table, "null < W"), [0, 1, 3, 4, 5]);
    assert_eq!(kept(&table, "Dur >= null"), [0, 1, 2, 3, 4, 5]);
    assert_eq!(kept(&table, "Dur > null"), [0, 1, 2, 3, 5]);
    assert!(kept(&table, "Dur < null").is_empty());
    assert_eq!(kept(&table, "Dur <= null"), [4]);
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
    // Row 5 is this instant in UTC, whose offset is less than Tokyo's although its zone id
    // sorts after `Asia/Tokyo` (as `where_z_le` of the recorded `time_types` scenario).
    assert_eq!(kept(&table, "Z < '2024-03-10T09:00:00 Asia/Tokyo'"), [2, 5]);
    assert_eq!(kept(&table, "Z >= null"), [0, 1, 2, 3, 4, 5]);
}

/// The table of the 11.8 probes: `T` is 2024-01-01T00:00:00Z plus `i` hours, null in row 2.
fn hours_table() -> Arc<dyn Table> {
    let base = 1_704_067_200_000_000_000i64;
    let t = (0..4)
        .map(|i| (i != 2).then_some(base + i * HOUR))
        .collect::<TimestampNanosecondArray>()
        .with_data_type(ColumnType::Instant.arrow_type());
    static_table(vec![("T", ColumnType::Instant, Arc::new(t))])
}

/// Behavior: the `Instant` convertor of MatchFilter.ColumnTypeConvertorFactory#getConvertor
/// (DateTimeUtils#parseInstant reads any instant, a year of more than four digits after a
/// sign among them) and ObjectChunkMatchFilterFactory, which compares instants with
/// `Objects.equals`; RangeFilter#init, whose DateTimeUtils#epochNanos fails past the
/// instants a column holds. Each answer is what Java 42.9 gave for the same table.
#[test]
fn an_instant_no_column_holds_matches_no_cell() {
    let t = hours_table();
    let none = [0i64; 0];
    for (filter, rows) in [
        ("T == '+10000-01-01T00:00:00 UTC'", &none[..]),
        ("T != '+10000-01-01T00:00:00 UTC'", &[0, 1, 2, 3]),
        ("T in '+10000-01-01T00:00:00 UTC'", &none),
        ("T not in '+10000-01-01T00:00:00 UTC', null", &[0, 1, 3]),
        (
            "T in '+10000-01-01T00:00:00Z', '2024-01-01T01:00:00Z'",
            &[1],
        ),
        ("T == '2263-01-01T00:00:00 UTC'", &none),
        ("T != '2263-01-01T00:00:00 UTC'", &[0, 1, 2, 3]),
        ("T == '1600-01-01T00:00:00 UTC'", &none),
        ("T == '-0001-01-01T00:00:00 UTC'", &none),
        ("T == '2024-01-01T01:00:00 UTC'", &[1]),
    ] {
        assert_eq!(kept(&t, filter), rows, "{filter}");
    }
    for filter in [
        "T < '+10000-01-01T00:00:00 UTC'",
        "T >= '+10000-01-01T00:00:00 UTC'",
        "T > '2263-01-01T00:00:00 UTC'",
        "T > '+10000-01-01T00:00:00Z'",
        "T > '+0999999999-12-31T23:59:59.999999999 UTC'",
        "'+10000-01-01T00:00:00 UTC' > T",
        // years Java does not read
        "T > '10000-01-01T00:00:00 UTC'",
        "T > '+2024-01-01T00:00:00 UTC'",
        "T > '+1000000000-01-01T00:00:00 UTC'",
        "T > '+10000-01-01 UTC'",
        "T == '+2024-01-01T00:00:00 UTC'",
    ] {
        refused(&t, filter);
    }
}

/// Java turns an instant before 1677 into nanoseconds by a multiplication that wraps
/// around: seen on Java 42.9, `T > '1600-01-01T00:00:00 UTC'` keeps none of these rows
/// and `T < ...` all of them, the null included.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.select.RangeFilter#makeInstantRangeFilter"
)]
fn a_range_filter_with_an_instant_before_every_column_is_not_ported() {
    kept(&hours_table(), "T > '1600-01-01T00:00:00 UTC'");
}
