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
    // Java reads an offset; this server names the function and fails.
    let error = table
        .evaluate("parseInstantQuiet(`2022-04-03T19:34:22+02:00`)")
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
