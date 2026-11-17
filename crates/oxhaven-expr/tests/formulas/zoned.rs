//! The local values, texts and zones of `ZonedDateTime` values, the text of a `Period`, and
//! a `LocalDate` at a `LocalTime` (task 4.26). The expected values are those Java 42.9 gave
//! for the same formulas.

use super::comparison::{dates, struct_column};
use super::*;
use arrow::array::Time64NanosecondArray;

/// 2024-03-10T12:00Z, the day New York moved its clocks on: 08:00 there.
const MARCH_10_NOON: i64 = 1_710_072_000_000_000_000;
const HOUR: i64 = 3_600_000_000_000;
const DAY: i64 = 24 * HOUR;
const MARCH_10: i32 = 19_792;

fn local_times(values: Vec<Option<i64>>) -> Column {
    (
        ColumnType::LocalTime,
        Arc::new(Time64NanosecondArray::from(values)),
    )
}

fn local_date_times(values: Vec<Option<i64>>) -> Column {
    let array = TimestampNanosecondArray::from(values)
        .with_data_type(ColumnType::LocalDateTime.arrow_type());
    (ColumnType::LocalDateTime, Arc::new(array))
}

/// An instant `T` and a zone name `Name`; a `ZonedDateTime` `Z` and a `Period` `P`, null in
/// the last row; a date `D` null in the last row and a time `L` at 02:30, which New York
/// skipped on 2024-03-10.
fn table() -> TestTable {
    let zoned_instants: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![MARCH_10_NOON, 0, 0])
            .with_data_type(ColumnType::Instant.arrow_type()),
    );
    let ids: ArrayRef = Arc::new(StringArray::from(vec!["America/New_York", "UTC", ""]));
    let parts = |values: [i32; 3]| -> ArrayRef { Arc::new(Int32Array::from(values.to_vec())) };
    let valid = [true, true, false];
    TestTable::new([
        ("T", instants([Some(MARCH_10_NOON), None, Some(0)])),
        (
            "Name",
            strings([Some("America/New_York"), Some("UTC"), Some("UTC")]),
        ),
        (
            "Z",
            struct_column(ColumnType::ZonedDateTime, vec![zoned_instants, ids], &valid),
        ),
        (
            "P",
            struct_column(
                ColumnType::Period,
                vec![parts([1, 0, 0]), parts([2, 0, 0]), parts([3, -14, 0])],
                &valid,
            ),
        ),
        ("D", dates([Some(MARCH_10), Some(0), None])),
        (
            "L",
            local_times(vec![Some(2 * HOUR + HOUR / 2), Some(1), Some(0)]),
        ),
    ])
}

/// Behavior: `DateTimeUtils#toLocalDate(ZonedDateTime)`, `#toLocalTime(ZonedDateTime)`,
/// `#toLocalDateTime(ZonedDateTime)`, `#formatDate` and `#formatDateTime`: the local values
/// in the value's zone, null for null.
#[test]
fn local_values_and_texts_of_a_zoned_date_time() {
    let table = table();
    let zoned = "toZonedDateTime(T, timeZone(Name))";
    table.assert(
        &format!("toLocalDate({zoned})"),
        dates([Some(MARCH_10), None, Some(0)]),
    );
    table.assert(
        &format!("toLocalTime({zoned})"),
        local_times(vec![Some(8 * HOUR), None, Some(0)]),
    );
    table.assert(
        &format!("toLocalDateTime({zoned})"),
        local_date_times(vec![
            Some(i64::from(MARCH_10) * DAY + 8 * HOUR),
            None,
            Some(0),
        ]),
    );
    table.assert(
        &format!("formatDateTime({zoned})"),
        strings([
            Some("2024-03-10T08:00:00.000000000 ET"),
            None,
            Some("1970-01-01T00:00:00.000000000 UTC"),
        ]),
    );
    table.assert(
        "formatDateTime(T, timeZone(`Europe/Paris`))",
        strings([
            Some("2024-03-10T13:00:00.000000000 Europe/Paris"),
            None,
            Some("1970-01-01T01:00:00.000000000 Europe/Paris"),
        ]),
    );
    table.assert(
        &format!("formatDate({zoned})"),
        strings([Some("2024-03-10"), None, Some("1970-01-01")]),
    );
    table.assert(
        "formatDate(D)",
        strings([Some("2024-03-10"), Some("1970-01-01"), None]),
    );
}

/// Behavior: `java.time.ZonedDateTime#toString`, `#getZone` and `java.time.Period#toString`,
/// which throw for a null receiver.
#[test]
fn texts_and_zones_of_zoned_date_times_and_periods() {
    let table = table();
    let failure = table.evaluate("Z.toString()").unwrap_err();
    assert!(
        failure.message.contains("NullPointerException"),
        "{failure}"
    );
    assert!(table.evaluate("Z.getZone()").is_err());
    assert!(table.evaluate("P.toString()").is_err());
    let filled = |formula: &str| table.evaluate(formula).unwrap();
    let present = TestTable::new([
        (
            "Z",
            filled("isNull(Z) ? toZonedDateTime(T, timeZone(`UTC`)) : Z"),
        ),
        ("P", filled("isNull(P) ? 'P0D' : P")),
    ]);
    present.assert(
        "Z.toString()",
        strings([
            Some("2024-03-10T08:00-04:00[America/New_York]"),
            Some("1970-01-01T00:00Z[UTC]"),
            Some("1970-01-01T00:00Z[UTC]"),
        ]),
    );
    present.assert(
        "Z.getZone()",
        (
            ColumnType::ZoneId,
            Arc::new(StringArray::from(vec!["America/New_York", "UTC", "UTC"])),
        ),
    );
    present.assert(
        "P.toString()",
        strings([Some("P1Y2M3D"), Some("P-14D"), Some("P0D")]),
    );
}

/// Behavior: `java.time.LocalDate#atTime(LocalTime)`, which throws for a null date or time,
/// and `DateTimeUtils#toZonedDateTime(LocalDate, LocalTime, ZoneId)`, null for null, which
/// moves a time the clocks skip later by the gap.
#[test]
fn a_date_at_a_time_and_in_a_zone() {
    let table = table();
    assert!(table.evaluate("D.atTime(L)").is_err());
    table.assert(
        "formatDateTime(toZonedDateTime(D, L, timeZone(`ET`)))",
        strings([
            Some("2024-03-10T03:30:00.000000000 ET"),
            Some("1970-01-01T00:00:00.000000001 ET"),
            None,
        ]),
    );
    let present = TestTable::new([
        ("D", dates([Some(MARCH_10), Some(0)])),
        ("L", local_times(vec![Some(HOUR), Some(1)])),
    ]);
    present.assert(
        "D.atTime(L)",
        local_date_times(vec![Some(i64::from(MARCH_10) * DAY + HOUR), Some(1)]),
    );
}
