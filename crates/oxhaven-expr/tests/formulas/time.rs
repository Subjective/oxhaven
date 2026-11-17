//! Functions of `DateTimeUtils` on instants and text, the methods of an `Instant`, and the
//! `String` methods and parsers that came with them.
//!
//! The expected values are what Java 42.9 returned for the same formulas on the same
//! columns, over gRPC.

use super::*;

const FIRST: i64 = 1_710_053_999_123_456_789;
const SECOND: i64 = 1_710_057_599_123_456_789;

/// `TS` with a null in the third row, `N` text of whole numbers and `T` text with a null.
fn table() -> TestTable {
    TestTable::new([
        ("I", ints([Some(-1), Some(0), None])),
        ("L", longs([Some(-3), Some(0), None])),
        ("TS", instants([Some(FIRST), Some(SECOND), None])),
        ("N", strings([Some("-7"), Some("0"), Some("14")])),
        ("T", strings([Some("x-0"), Some(""), None])),
    ])
}

fn same<const N: usize>(text: &str) -> Column {
    strings([Some(text); N])
}

/// Behavior: `DateTimeUtils#epochSeconds(Instant)`, `#epochMillis`, `#epochMicros` and
/// the three functions back.
#[test]
fn an_instant_is_a_count_since_the_epoch_in_each_unit() {
    let table = table();
    table.assert(
        "epochSeconds(TS)",
        longs([Some(1_710_053_999), Some(1_710_057_599), None]),
    );
    table.assert(
        "epochMillis(TS)",
        longs([Some(1_710_053_999_123), Some(1_710_057_599_123), None]),
    );
    table.assert(
        "epochMicros(TS)",
        longs([
            Some(1_710_053_999_123_456),
            Some(1_710_057_599_123_456),
            None,
        ]),
    );
    table.assert(
        "epochSecondsToInstant(I)",
        instants([Some(-1_000_000_000), Some(0), None]),
    );
    table.assert(
        "epochMillisToInstant(L)",
        instants([Some(-3_000_000), Some(0), None]),
    );
    table.assert(
        "epochMicrosToInstant(L)",
        instants([Some(-3_000), Some(0), None]),
    );
    // Seconds and milliseconds round toward the past, microseconds toward the epoch.
    let before = "'1969-12-31T23:59:59.9999995 UTC'";
    table.assert(&format!("epochSeconds({before})"), longs([Some(-1); 3]));
    table.assert(&format!("epochMillis({before})"), longs([Some(-1); 3]));
    table.assert(&format!("epochMicros({before})"), longs([Some(0); 3]));
    let error = table
        .evaluate("epochSecondsToInstant(9223372036854775807L)")
        .unwrap_err();
    assert!(error.message.contains("overflow"), "{error:?}");
}

/// Behavior: `DateTimeUtils#parseInstant(String)` and `#parseInstantQuiet(String)`.
#[test]
fn text_is_parsed_as_an_instant() {
    let table = table();
    let all = |nanos: i64| instants([Some(nanos); 3]);
    for (formula, nanos) in [
        ("parseInstant(`2024-01-01T00:00:00 ET`)", 1_704_085_200),
        ("parseInstant(`2023-11-21T21:30:45Z`)", 1_700_602_245),
        ("parseInstant(`2022-04-03 ET`)", 1_648_958_400),
        (
            "parseInstant(`2022-04-03T19:34:22 America/Chicago`)",
            1_649_032_462,
        ),
        // A whole number is seconds, milliseconds, microseconds or nanoseconds by its size.
        ("parseInstant(`1710053999`)", 1_710_053_999),
        ("parseInstant(`-5`)", -5),
        ("parseInstantQuiet(`20`)", 20),
    ] {
        table.assert(formula, all(nanos * 1_000_000_000));
    }
    table.assert(
        "parseInstant(`1710053999123`)",
        all(1_710_053_999_123_000_000),
    );
    table.assert(
        "parseInstant(`2016-01-01T01:00 ET`) + i * HOUR",
        instants([
            Some(1_451_628_000_000_000_000),
            Some(1_451_631_600_000_000_000),
            Some(1_451_635_200_000_000_000),
        ]),
    );
    for formula in [
        "parseInstantQuiet(`nonsense`)",
        "parseInstantQuiet(T)",
        "parseInstantQuiet(`5`)",
        "parseInstantQuiet(`-9223372036854775808`)",
        "parseInstant(`-9223372036854775808`)",
    ] {
        table.assert(formula, instants([None; 3]));
    }
    for formula in [
        "parseInstant(`nonsense`)",
        "parseInstant(T)",
        "parseInstant((String)null)",
        "parseInstant(` 2023-11-21T21:30:45Z`)",
        "parseInstant(`99999999999999999999`)",
        "parseInstant(`9223372036854775807`)",
    ] {
        assert!(table.evaluate(formula).is_err(), "{formula}");
    }
    // An offset directly after the time, as `java.time.ZonedDateTime#parse` reads it.
    table.assert(
        "parseInstantQuiet(`2022-04-03T19:34:22+02:00`)",
        instants([Some(1_649_007_262_000_000_000); 3]),
    );
    // Java reads a longer year after a sign; this server names the function and fails.
    let error = table
        .evaluate("parseInstantQuiet(`+12022-04-03T19:34:22 UTC`)")
        .unwrap_err();
    assert!(
        error.message.contains("DateTimeUtils#parseInstant"),
        "{error:?}"
    );
}

/// Behavior: `DateTimeUtils#plus(Instant, Duration)`, `#minus(Instant, Period)`,
/// `#upperBin(Instant, Duration)`, `#lowerBin(Instant, Duration, Duration)`.
#[test]
fn a_duration_is_an_argument_of_the_functions_of_an_instant() {
    let table = table();
    let shifted = |by: i64| instants([Some(FIRST + by), Some(SECOND + by), None]);
    table.assert("minus(TS, 'PT5s')", shifted(-5_000_000_000));
    table.assert("plus(TS, 'PT10s')", shifted(10_000_000_000));
    table.assert("plus(TS, 'P1D')", shifted(86_400_000_000_000));
    table.assert("minus(TS, 'P1D')", shifted(-86_400_000_000_000));
    table.assert(
        "plus(TS, parseDuration(`PT1h`))",
        shifted(3_600_000_000_000),
    );
    let at = |seconds: i64| Some(seconds * 1_000_000_000);
    table.assert(
        "upperBin(TS, 'PT15m')",
        instants([at(1_710_054_000), at(1_710_057_600), None]),
    );
    table.assert(
        "lowerBin(TS, 'PT8m')",
        instants([at(1_710_053_760), at(1_710_057_120), None]),
    );
    table.assert(
        "upperBin(TS, 'PT15m', 'PT1m')",
        instants([at(1_710_054_060), at(1_710_057_660), None]),
    );
    table.assert(
        "lowerBin(TS, 'PT15m', 'PT1m')",
        instants([at(1_710_053_160), at(1_710_056_760), None]),
    );
    assert!(table.evaluate("upperBin(TS, 'PT0s')").is_err());
    for formula in [
        "lowerBin(TS, 'P1D')",
        "plus('PT1h', TS)",
        "upperBin(TS, 'PT15m', 5)",
    ] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula} gave {:?}",
            table.compile_error(formula)
        );
    }
}

/// Behavior: `java.time.Instant#getEpochSecond`, `#toEpochMilli` and `#toString`, which a
/// null value cannot be asked.
#[test]
fn the_methods_of_an_instant_fail_on_null() {
    let table = table();
    table.assert(
        "isNull(TS) ? NULL_LONG : TS.getEpochSecond()",
        longs([Some(1_710_053_999), Some(1_710_057_599), None]),
    );
    table.assert(
        "isNull(TS) ? null : TS.toString()",
        strings([
            Some("2024-03-10T06:59:59.123456789Z"),
            Some("2024-03-10T07:59:59.123456789Z"),
            None,
        ]),
    );
    for (literal, text) in [
        ("2024-03-03T15:00:00.000 UTC", "2024-03-03T15:00:00Z"),
        ("2024-03-03T15:00:00.1 UTC", "2024-03-03T15:00:00.100Z"),
        (
            "2024-03-03T15:00:00.0001 UTC",
            "2024-03-03T15:00:00.000100Z",
        ),
        (
            "2024-03-03T15:00:00.000000001 UTC",
            "2024-03-03T15:00:00.000000001Z",
        ),
        ("1969-12-31T23:59:59.5 UTC", "1969-12-31T23:59:59.500Z"),
    ] {
        table.assert(&format!("'{literal}'.toString()"), same::<3>(text));
    }
    table.assert(
        "'1969-12-31T23:59:59.5 UTC'.getEpochSecond()",
        longs([Some(-1); 3]),
    );
    table.assert(
        "'1969-12-31T23:59:59.5 UTC'.toEpochMilli()",
        longs([Some(-500); 3]),
    );
    for formula in ["TS.getEpochSecond()", "TS.toEpochMilli()", "TS.toString()"] {
        let error = table.evaluate(formula).unwrap_err();
        assert!(
            error.message.contains("NullPointerException"),
            "{formula} gave {error:?}"
        );
    }
    assert_eq!(
        table.compile_error("TS.getNano()"),
        FormulaError::Unsupported("method call .getNano() on a value of type Instant".into())
    );
}

/// Behavior: `io.deephaven.function.Parse#parseDouble(String)` and `#parseFloat(String)`:
/// null for null, otherwise Java's parser.
#[test]
fn text_is_parsed_as_a_floating_point_number() {
    let table = table();
    table.assert(
        "parseDouble(N)",
        doubles([Some(-7.0), Some(0.0), Some(14.0)]),
    );
    table.assert("parseFloat(N)", floats([Some(-7.0), Some(0.0), Some(14.0)]));
    table.assert(
        "parseDouble(N + `.5`)",
        doubles([Some(-7.5), Some(0.5), Some(14.5)]),
    );
    table.assert("parseDouble(` 1.5 `)", doubles([Some(1.5); 3]));
    table.assert("parseDouble(`1.5f`)", doubles([Some(1.5); 3]));
    table.assert("parseFloat(`1e40`)", floats([Some(f32::INFINITY); 3]));
    table.assert("parseDouble(`NaN`)", doubles([Some(f64::NAN); 3]));
    table.assert("parseDouble((String)null)", doubles([None; 3]));
    table.assert("parseFloat((String)null)", floats([None; 3]));
    for formula in ["parseDouble(T)", "parseFloat(T)", "parseDouble(`abc`)"] {
        assert!(table.evaluate(formula).is_err(), "{formula}");
    }
}

/// Behavior: `String#hashCode`, `String#replace(char, char)` and
/// `String#replace(CharSequence, CharSequence)`.
#[test]
fn a_string_is_hashed_and_its_parts_replaced() {
    let table = table();
    table.assert("`str`.hashCode()", ints([Some(114_225); 3]));
    table.assert("``.hashCode()", ints([Some(0); 3]));
    // The hash that is the null value of an `int`.
    table.assert("`polygenelubricants`.hashCode()", ints([None; 3]));
    table.assert(
        "isNull(T) ? null : T.hashCode()",
        ints([Some(116_763), Some(0), None]),
    );
    table.assert(
        "isNull(T) ? null : T.replace(\"x\", \"a\")",
        strings([Some("a-0"), Some(""), None]),
    );
    table.assert(
        "N.replace('-', '_')",
        strings([Some("_7"), Some("0"), Some("14")]),
    );
    table.assert(
        "N.replace(`1`, `xy`)",
        strings([Some("-7"), Some("0"), Some("xy4")]),
    );
    table.assert(
        "N.replace(``, `.`)",
        strings([Some(".-.7."), Some(".0."), Some(".1.4.")]),
    );
    for formula in ["T.hashCode()", "T.replace('x', 'y')", "N.replace(`1`, T)"] {
        let error = table.evaluate(formula).unwrap_err();
        assert!(
            error.message.contains("NullPointerException"),
            "{formula} gave {error:?}"
        );
    }
    for formula in ["N.replace('1', \"x\")", "N.replace(\"1\", '2')"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula} gave {:?}",
            table.compile_error(formula)
        );
    }
}

/// The instants of the probe: 01:59:59 and 03:59:59 in New York on the day in 2024 its
/// clocks skipped an hour, and two and three hours later.
fn clocks_change() -> TestTable {
    TestTable::new([
        (
            "TS",
            instants([
                Some(FIRST),
                Some(SECOND),
                None,
                Some(1_710_064_799_123_456_789),
                Some(1_710_068_399_123_456_789),
            ]),
        ),
        (
            "Z",
            booleans([Some(true), Some(false), None, Some(false), Some(true)]),
        ),
    ])
}

/// Behavior: `DateTimeUtils#secondOfDay(Instant, ZoneId, boolean)` and its relatives: the
/// flag chooses the time on the clock or the time since the day began.
#[test]
fn a_time_of_day_is_on_the_clock_or_since_the_day_began() {
    let table = clocks_change();
    let all = |values: [i32; 4]| {
        ints([
            Some(values[0]),
            Some(values[1]),
            None,
            Some(values[2]),
            Some(values[3]),
        ])
    };
    for (formula, expected) in [
        ("secondOfDay(TS, 'ET', true)", [7199, 14399, 21599, 25199]),
        ("secondOfDay(TS, 'ET', false)", [7199, 10799, 17999, 21599]),
        ("secondOfDay(TS, 'PT', true)", [82799, 86399, 7199, 14399]),
        ("secondOfDay(TS, 'PT', false)", [82799, 86399, 7199, 10799]),
        ("secondOfDay(TS, 'UTC', true)", [25199, 28799, 35999, 39599]),
        ("hourOfDay(TS, 'ET', true)", [1, 3, 5, 6]),
        ("hourOfDay(TS, 'ET', false)", [1, 2, 4, 5]),
        ("hourOfDay(TS, 'Asia/Shanghai', true)", [14, 15, 17, 18]),
        ("minuteOfDay(TS, 'ET', true)", [119, 239, 359, 419]),
        ("minuteOfDay(TS, 'ET', false)", [119, 179, 299, 359]),
        (
            "millisOfDay(TS, 'ET', false)",
            [7_199_123, 10_799_123, 17_999_123, 21_599_123],
        ),
        (
            "secondOfDay(TS, timeZone(`America/New_York`), true)",
            [7199, 14399, 21599, 25199],
        ),
        (
            "secondOfDay(TS, parseTimeZone(`ET`), true)",
            [7199, 14399, 21599, 25199],
        ),
    ] {
        table.assert(formula, all(expected));
    }
    table.assert(
        "nanosOfDay(TS, 'ET', false)",
        longs([
            Some(7_199_123_456_789),
            Some(10_799_123_456_789),
            None,
            Some(17_999_123_456_789),
            Some(21_599_123_456_789),
        ]),
    );
    // The day the clocks are set back is an hour longer.
    for (formula, expected) in [
        ("hourOfDay('2024-11-03T06:30:00 UTC', 'ET', true)", 1),
        ("hourOfDay('2024-11-03T06:30:00 UTC', 'ET', false)", 2),
        ("hourOfDay('2024-11-03T12:30:00 UTC', 'ET', true)", 7),
        ("hourOfDay('2024-11-03T12:30:00 UTC', 'ET', false)", 8),
        ("secondOfDay('2024-11-03T12:30:00 UTC', 'ET', false)", 30600),
        ("secondOfDay('2024-03-10T12:30:00 UTC', 'ET', false)", 27000),
    ] {
        table.assert(formula, ints([Some(expected); 5]));
    }
    let error = table.evaluate("hourOfDay(TS, 'ET', Z)").unwrap_err();
    assert!(error.message.contains("NullPointerException"), "{error:?}");
}

/// Behavior: `DateTimeUtils#dayOfYear(Instant, ZoneId)` and its relatives.
#[test]
fn a_date_and_time_have_their_fields_in_a_zone() {
    let table = clocks_change();
    let all = |values: [i32; 4]| {
        ints([
            Some(values[0]),
            Some(values[1]),
            None,
            Some(values[2]),
            Some(values[3]),
        ])
    };
    for (formula, expected) in [
        ("dayOfYear(TS, 'PT')", [69, 69, 70, 70]),
        ("dayOfYear(TS, 'Asia/Shanghai')", [70; 4]),
        ("dayOfMonth(TS, 'ET')", [10; 4]),
        ("monthOfYear(TS, 'ET')", [3; 4]),
        ("year(TS, 'ET')", [2024; 4]),
        ("yearOfCentury(TS, 'ET')", [24; 4]),
        ("dayOfWeekValue(TS, 'ET')", [7; 4]),
        ("minuteOfHour(TS, 'ET')", [59; 4]),
        ("secondOfMinute(TS, 'ET')", [59; 4]),
        ("millisOfSecond(TS, 'ET')", [123; 4]),
    ] {
        table.assert(formula, all(expected));
    }
    let long = |value: i64| longs([Some(value), Some(value), None, Some(value), Some(value)]);
    table.assert("nanosOfSecond(TS, 'ET')", long(123_456_789));
    table.assert("microsOfSecond(TS, 'ET')", long(123_456));
    for (formula, construct) in [
        (
            "secondOfDay(TS, null, true)",
            "null literal as an argument of secondOfDay()",
        ),
        ("dayOfYear(TS, 'NY')", "time literal 'NY'"),
        ("dayOfYear(TS, timeZone(`NY`))", "time zone 'NY'"),
        ("dayOfYear(TS)", "function dayOfYear()"),
    ] {
        assert_eq!(
            table.compile_error(formula),
            FormulaError::Unsupported(construct.into()),
            "{formula}"
        );
    }
}

/// Behavior: `DateTimeUtils#hourOfDay(Instant, ZoneId, boolean)` and its relatives, which
/// are those of `toZonedDateTime(instant, timeZone)` and null when either is null;
/// `#year(LocalDate)`, `#dayOfWeekValue(LocalDate)`, `#yearOfCentury(LocalDate)`
/// (`year % 100`), `#minuteOfDay(LocalTime)` and `#nanosOfDay(LocalTime)`.
#[test]
fn a_field_is_read_in_the_zone_of_each_row_and_of_a_date_or_time() {
    let table = TestTable::new([
        (
            "T",
            // 2024-03-10T12:00Z, the day New York moved its clocks on.
            instants([Some(1_710_072_000_000_000_000), None, Some(0), Some(0)]),
        ),
        (
            "Name",
            strings([Some("America/New_York"), Some("UTC"), None, Some("UTC")]),
        ),
    ]);
    // Behavior: `DateTimeUtils#toLocalDateTime(Instant, ZoneId)`: 08:00 in New York.
    let local =
        TimestampNanosecondArray::from(vec![Some(1_710_057_600_000_000_000), None, None, Some(0)])
            .with_data_type(ColumnType::LocalDateTime.arrow_type());
    table.assert(
        "toLocalDateTime(T, timeZone(Name))",
        (ColumnType::LocalDateTime, Arc::new(local)),
    );
    let int = |values: [Option<i32>; 4]| ints(values);
    table.assert(
        "hourOfDay(T, timeZone(Name), true)",
        int([Some(8), None, None, Some(0)]),
    );
    table.assert(
        "hourOfDay(T, timeZone(Name), false)",
        int([Some(7), None, None, Some(0)]),
    );
    let zoned = "toZonedDateTime(T, timeZone(Name))";
    table.assert(
        &format!("hourOfDay({zoned}, true)"),
        int([Some(8), None, None, Some(0)]),
    );
    table.assert(
        &format!("year({zoned})"),
        int([Some(2024), None, None, Some(1970)]),
    );
    let date = "toLocalDate(T, timeZone(Name))";
    table.assert(
        &format!("dayOfWeekValue({date})"),
        int([Some(7), None, None, Some(4)]),
    );
    table.assert(
        &format!("dayOfYear({date})"),
        int([Some(70), None, None, Some(1)]),
    );
    table.assert(
        "yearOfCentury(parseLocalDate(`-0005-01-01`))",
        int([Some(-5); 4]),
    );
    let time = "toLocalTime(T, timeZone(Name))";
    table.assert(
        &format!("minuteOfDay({time})"),
        int([Some(480), None, None, Some(0)]),
    );
    table.assert(
        &format!("nanosOfDay({time})"),
        longs([Some(8 * 3_600_000_000_000), None, None, Some(0)]),
    );
}

/// Behavior: `java.time.LocalDate#of(int, int, int)`. The messages are those Java 42.9
/// logged for the same formulas.
#[test]
fn a_date_is_made_of_its_year_month_and_day() {
    let first = TestTable::new([("Y", ints([Some(2024)]))]);
    first.assert(
        "LocalDate.of(Y, 2, 29)",
        (
            ColumnType::LocalDate,
            Arc::new(arrow::array::Date32Array::from(vec![19_782])),
        ),
    );
    let table = TestTable::new([("Y", ints([Some(2023)]))]);
    for (formula, message) in [
        (
            "LocalDate.of(Y, 2, 29)",
            "Invalid date 'February 29' as '2023' is not a leap year",
        ),
        ("LocalDate.of(2023, 4, 31)", "Invalid date 'APRIL 31'"),
        (
            "LocalDate.of(Y, 1, 32)",
            "Invalid value for DayOfMonth (valid values 1 - 28/31): 32",
        ),
        (
            "LocalDate.of(2020, Y, 1)",
            "Invalid value for MonthOfYear (valid values 1 - 12): 2023",
        ),
    ] {
        let Err(failure) = table.evaluate(formula) else {
            panic!("{formula} did not fail");
        };
        assert!(failure.message.contains(message), "{formula}: {failure}");
    }
    let nulls = TestTable::new([("Y", ints([None]))]);
    let Err(failure) = nulls.evaluate("LocalDate.of(Y, 1, 1)") else {
        panic!("a null year did not fail");
    };
    assert!(
        failure
            .message
            .contains("(valid values -999999999 - 999999999): -2147483648"),
        "{failure}"
    );
}

/// Behavior: `DateTimeUtils#parseLocalDateTime` and `#parseLocalDateTimeQuiet`. The values
/// are those Java 42.9 returned for the same texts.
#[test]
fn text_is_parsed_as_a_local_date_time() {
    const DAY: i64 = 86_400_000_000_000;
    const HOUR: i64 = 3_600_000_000_000;
    // 2023-11-14 and 2021-02-28 as days since the epoch.
    let (nov_14, feb_28) = (19_675, 18_686);
    let cases: [(&str, Option<i64>); 15] = [
        (
            "2023-11-14T22:13:20",
            Some(nov_14 * DAY + 80_000_000_000_000),
        ),
        ("2023-11-14", Some(nov_14 * DAY)),
        ("2023-11-14T", Some(nov_14 * DAY)),
        (
            "2023-11-14t22:13",
            Some(nov_14 * DAY + 22 * HOUR + 780_000_000_000),
        ),
        (
            "2023-11-14T22:13:20.5",
            Some(nov_14 * DAY + 80_000_500_000_000),
        ),
        ("2021-02-29T10:00", Some(feb_28 * DAY + 10 * HOUR)),
        ("2023-11-14T24:00", Some((nov_14 + 1) * DAY)),
        ("2021-02-29T24:00", Some((feb_28 + 1) * DAY)),
        (
            "2023-11-14T12:30:45.",
            Some(nov_14 * DAY + 45_045_000_000_000),
        ),
        ("2023-3-5", None),
        ("2020-04-32T00:00", None),
        ("2023-11-14 22:13", None),
        ("2023-11-14T24:30", None),
        ("2023-11-14T22:13:20.1234567891", None),
        ("2023-11-14T22", None),
    ];
    let table = TestTable::new([("S", strings(cases.map(|(text, _)| Some(text))))]);
    let expected = TimestampNanosecondArray::from(cases.map(|(_, nanos)| nanos).to_vec())
        .with_data_type(ColumnType::LocalDateTime.arrow_type());
    table.assert(
        "parseLocalDateTimeQuiet(S)",
        (ColumnType::LocalDateTime, Arc::new(expected)),
    );
    let one = TestTable::new([("S", strings([Some("2023-3-5T1:2:3.5")]))]);
    let expected = TimestampNanosecondArray::from(vec![19_421 * DAY + HOUR + 123_500_000_000])
        .with_data_type(ColumnType::LocalDateTime.arrow_type());
    one.assert(
        "parseLocalDateTime(S)",
        (ColumnType::LocalDateTime, Arc::new(expected)),
    );
    let Err(failure) = table.evaluate("parseLocalDateTime(S)") else {
        panic!("parseLocalDateTime of text it does not read did not fail");
    };
    assert!(
        failure
            .message
            .contains("Cannot parse local date time: 2023-3-5"),
        "{failure}"
    );
}
