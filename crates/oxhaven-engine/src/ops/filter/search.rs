//! Quick filters: one search text tried against every column, each column in the way its
//! type reads the text.
//!
//! A text that a column's type cannot read leaves that column out. What is left is joined
//! by OR, and nothing left selects no rows.

use super::filters::{MatchOptions, Resolved};
use super::literal::{
    java_equals_ignore_case, parse_big_decimal, parse_big_integer, parse_instant_with,
    parse_java_double, parse_java_float, parse_java_integer, Decimal, Key,
};
use super::pattern::Matcher;
use oxhaven_column::sentinel::{NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_LONG};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::unported;

/// The suffixes of the columns that hold the formats of other columns, which a search
/// passes over.
///
/// Behavior: io.deephaven.engine.util.ColumnFormatting#isFormattingColumn
const FORMAT_SUFFIXES: [&str; 3] = [
    "__TABLE_STYLE_FORMAT",
    "__TABLE_NUMBER_FORMAT",
    "__TABLE_DATE_FORMAT",
];

/// The filter of a search for `text` in the columns of `definition`, or in those of them
/// that `columns` names if it names any. A name that is no column is passed over.
///
/// Behavior: io.deephaven.server.table.ops.filter.FilterFactory#onSearch,
/// io.deephaven.engine.table.impl.select.WhereFilterFactory#expandQuickFilter in its
/// `NORMAL` mode
pub(super) fn expand(
    definition: &TableDefinition,
    text: &str,
    columns: &[String],
    compiler: &dyn FormulaCompiler,
) -> Result<Resolved, String> {
    if text.is_empty() {
        return Ok(Resolved::Nothing);
    }
    let mut filters = Vec::new();
    for (index, column) in definition.columns().iter().enumerate() {
        let formatting = FORMAT_SUFFIXES
            .iter()
            .any(|suffix| column.name.ends_with(suffix));
        if formatting || !(columns.is_empty() || columns.contains(&column.name)) {
            continue;
        }
        let filter = match column.column_type.list_element() {
            None => quick_filter(index, &column.column_type, text, compiler)?,
            // Behavior: WhereFilterFactory#createVectorQuickFilter and
            // VectorComponentFilterWrapper#maybeCreateFilterWrapper: the element type's
            // filter, which every quick filter exposes as a chunk filter, applied to the
            // elements; a row is kept when one passes.
            Some(element) => {
                // A vector of a primitive type, which Java reads element by element,
                // boxed.
                let primitive = matches!(column.column_type, ColumnType::Vector(_))
                    && matches!(
                        element,
                        ColumnType::Byte
                            | ColumnType::Char
                            | ColumnType::Short
                            | ColumnType::Int
                            | ColumnType::Long
                            | ColumnType::Float
                            | ColumnType::Double
                    );
                quick_filter(index, element, text, compiler)?.map(|filter| Resolved::AnyElement {
                    column: index,
                    element_type: element.clone(),
                    element: Box::new(filter),
                    null_element_fails: primitive,
                })
            }
        };
        filters.extend(filter);
    }
    Ok(match filters.len() {
        0 => Resolved::Nothing,
        1 => filters.remove(0),
        _ => Resolved::Or(filters),
    })
}

fn matching(column: usize, key: Key) -> Resolved {
    Resolved::Match {
        column,
        keys: vec![key],
        options: MatchOptions::default(),
    }
}

/// The filter of one column for the search text, or `None` if a column of this type has
/// nothing that the text could be.
///
/// Behavior: io.deephaven.engine.table.impl.select.WhereFilterFactory#createQuickFilterInternal,
/// io.deephaven.engine.table.impl.select.WhereFilterFactory.InferenceResult
fn quick_filter(
    column: usize,
    column_type: &ColumnType,
    text: &str,
    compiler: &dyn FormulaCompiler,
) -> Result<Option<Resolved>, String> {
    let integer = |min: i64, max: i64| {
        parse_java_integer(text, min, max).ok().map(|value| {
            // the lowest value of each type is its null
            let key = if value == min {
                Key::Null
            } else {
                Key::Int(value)
            };
            matching(column, key)
        })
    };
    Ok(match column_type {
        ColumnType::Double => match parse_java_double(text) {
            Ok(value) if !value.is_nan() => Some(match decimal_places(text) {
                Some(places) => {
                    let (other, lower_inclusive) = neighbour(value, places);
                    between_floats(column, value, other, NULL_DOUBLE, lower_inclusive)
                }
                None => matching(column, float_key(value, NULL_DOUBLE)),
            }),
            _ => None,
        },
        ColumnType::Float => match parse_java_float(text) {
            Ok(value) if !value.is_nan() => Some(match decimal_places(text) {
                // Java reads the text as a double again and narrows the two ends.
                Some(places) => {
                    let wide = parse_java_double(text)?;
                    let (other, lower_inclusive) = neighbour(wide, places);
                    let narrow = |value: f64| f64::from(value as f32);
                    between_floats(
                        column,
                        narrow(wide),
                        narrow(other),
                        NULL_FLOAT.into(),
                        lower_inclusive,
                    )
                }
                None => matching(column, float_key(value.into(), NULL_FLOAT.into())),
            }),
            _ => None,
        },
        ColumnType::Int => integer(i32::MIN.into(), i32::MAX.into()),
        ColumnType::Long => integer(i64::MIN, i64::MAX),
        ColumnType::Short => integer(i16::MIN.into(), i16::MAX.into()),
        ColumnType::Byte => integer(i8::MIN.into(), i8::MAX.into()),
        ColumnType::String => Some(Resolved::Pattern {
            column,
            matcher: Matcher::any(vec![text.to_string()], true),
            invert_pattern: false,
        }),
        ColumnType::BigInteger => parse_big_integer(text)
            .ok()
            .map(|value| matching(column, Key::BigInteger(value))),
        // Behavior: AbstractRangeFilter#makeBigDecimalRange, which takes the number of
        // decimal places by `findPrecision` and throws for a text that `new BigDecimal`
        // reads and that is no plain decimal (`1e3`, `+5`, `.5`): that fails the search.
        ColumnType::BigDecimal => match parse_big_decimal(text) {
            Err(_) => None,
            Ok((scale, unscaled)) => {
                if decimal_places(text).is_none() {
                    return Err(format!("The value {text} is not a double"));
                }
                let value = Decimal::of(scale, &unscaled);
                let other = value.one_further();
                let at_least_zero = !value.is_negative();
                let key = |decimal: &Decimal| {
                    let (scale, unscaled) = decimal.parts();
                    Key::BigDecimal { scale, unscaled }
                };
                let (lower, upper) = if at_least_zero {
                    (key(&value), key(&other))
                } else {
                    (key(&other), key(&value))
                };
                Some(Resolved::Between {
                    column,
                    lower,
                    upper,
                    lower_inclusive: at_least_zero,
                    upper_inclusive: !at_least_zero,
                })
            }
        },
        // `boolean` is only ever the element of an array (`boolean[]`).
        ColumnType::Boolean | ColumnType::PrimitiveBoolean => {
            let is_true = java_equals_ignore_case(text, "true");
            (is_true || java_equals_ignore_case(text, "false"))
                .then(|| matching(column, Key::Bool(is_true)))
        }
        ColumnType::Instant => match instant(text, compiler) {
            None => None,
            Some(lower) => {
                let upper = match precision(text) {
                    Precision::None => i128::from(lower),
                    Precision::Nanos(unit) => i128::from(lower) + i128::from(unit),
                    // Java adds the day in its own time zone, which need not be UTC.
                    Precision::Day => unported!(
                        "io.deephaven.time.DateTimeUtils#timeZone",
                        "a search for the date {text:?} in an Instant column"
                    ),
                };
                Some(Resolved::Between {
                    column,
                    lower: Key::Int(epoch_nanos(i128::from(lower))?),
                    upper: Key::Int(epoch_nanos(upper)?),
                    lower_inclusive: true,
                    upper_inclusive: false,
                })
            }
        },
        ColumnType::Char => {
            let mut units = text.encode_utf16();
            match (units.next(), units.next()) {
                (Some(unit), None) => Some(matching(
                    column,
                    if unit == NULL_CHAR {
                        Key::Null
                    } else {
                        Key::Int(unit.into())
                    },
                )),
                _ => None,
            }
        }
        // A vector or an array inside one: Java has no quick filter for its elements.
        _ => None,
    })
}

fn float_key(value: f64, null: f64) -> Key {
    if value == null {
        Key::Null
    } else {
        Key::Float(value)
    }
}

/// The number of digits after the decimal point of a plain decimal number, trailing zeros
/// included; `None` for any other text, also one that Java reads as a number (`1e3`,
/// `.5`, `+5`, ` 5`).
///
/// Behavior: io.deephaven.engine.table.impl.select.AbstractRangeFilter#findPrecision
fn decimal_places(text: &str) -> Option<usize> {
    let digits = |text: &str| text.bytes().all(|b| b.is_ascii_digit());
    let number = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = match number.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (number, ""),
    };
    (!whole.is_empty() && digits(whole) && digits(fraction)).then_some(fraction.len())
}

/// The other end of the range a number with `places` decimal places stands for, and
/// whether the lower end belongs to the range: `1.5` is `[1.5, 1.6)` and `-1.5` is
/// `(-1.6, -1.5]`.
///
/// Behavior: io.deephaven.engine.table.impl.select.DoubleRangeFilter#makeRange
fn neighbour(value: f64, places: usize) -> (f64, bool) {
    // 10^-places as Java's `Math.pow` gives it, which is the nearest double
    let offset: f64 = format!("1e-{places}").parse().unwrap_or(0.0);
    if value >= 0.0 {
        (value + offset, true)
    } else {
        (value - offset, false)
    }
}

/// The filter of the range between two numbers, of which one end is inclusive. The ends
/// are put in order as Java orders floating point values: null, which is the lowest
/// finite value, first and NaN last.
///
/// Behavior: the constructor of io.deephaven.engine.table.impl.select.DoubleRangeFilter
fn between_floats(column: usize, a: f64, b: f64, null: f64, lower_inclusive: bool) -> Resolved {
    let key = |value: f64| float_key(value, null);
    let greater = match (key(a), key(b)) {
        (Key::Null, _) => false,
        (_, Key::Null) => true,
        _ => (a.is_nan() && !b.is_nan()) || a > b,
    };
    let (lower, upper) = if greater { (b, a) } else { (a, b) };
    Resolved::Between {
        column,
        lower: key(lower),
        upper: key(upper),
        lower_inclusive,
        upper_inclusive: !lower_inclusive,
    }
}

/// Whether `text` begins with a date as `java.time.format.DateTimeFormatter#ISO_LOCAL_DATE`
/// writes one, which every date time Java reads begins with; `rest` is what follows it.
fn after_iso_date(text: &str) -> Option<&str> {
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let year = unsigned.bytes().take_while(u8::is_ascii_digit).count();
    let rest = unsigned.get(year..)?.as_bytes();
    let two_digits = |at: usize| {
        rest.get(at..at + 2)
            .is_some_and(|d| d.iter().all(u8::is_ascii_digit))
    };
    ((4..=10).contains(&year)
        && rest.first() == Some(&b'-')
        && two_digits(1)
        && rest.get(3) == Some(&b'-')
        && two_digits(4))
    .then(|| &unsigned[year + 6..])
}

/// Whether `text` could be a duration: `PT` and a number, or `P`, a number and `D`.
/// These are the first characters of both forms Java reads.
///
/// Behavior: io.deephaven.time.DateTimeUtils#parseDuration, java.time.Duration#parse
fn may_be_duration(text: &str) -> bool {
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let Some(rest) = unsigned.strip_prefix(['P', 'p']) else {
        return false;
    };
    /// What follows the number `text` starts with, if it starts with one.
    fn after_number(text: &str) -> Option<&str> {
        let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
        let count = digits.bytes().take_while(u8::is_ascii_digit).count();
        (count > 0).then(|| &digits[count..])
    }
    match rest.strip_prefix(['T', 't']) {
        Some(time) => after_number(time).is_some(),
        None => after_number(rest).is_some_and(|after| after.starts_with(['D', 'd'])),
    }
}

/// The instant the search text names, in nanoseconds since the epoch, or `None` if it
/// names none.
///
/// Java reads the text as a date time and, failing that, as a time of today in the
/// server's time zone.
///
/// Behavior: io.deephaven.engine.table.impl.select.WhereFilterFactory.InferenceResult
fn instant(text: &str, compiler: &dyn FormulaCompiler) -> Option<i64> {
    let digits = text.strip_prefix('-').unwrap_or(text);
    let integer = (1..=19).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit());
    // A date time has a `T` and an offset after its date, or a space and a time zone.
    let date_time = after_iso_date(text).is_some_and(|rest| rest.contains(['T', 't', ' ']));
    if integer || date_time {
        // an integer that is the null of a long is no instant
        return parse_instant_with(text, compiler)
            .ok()
            .filter(|nanos| *nanos != NULL_LONG);
    }
    if may_be_duration(text) {
        unported!(
            "io.deephaven.engine.table.impl.select.WhereFilterFactory.InferenceResult",
            "a search for the time of day {text:?} in an Instant column, which depends on \
             the day and the time zone of the server"
        );
    }
    None
}

/// The finest unit a date time names.
enum Precision {
    /// The text names no unit Java recognizes: the range ends where it starts.
    None,
    /// A unit of this many nanoseconds.
    Nanos(i64),
    Day,
}

/// The finest unit of a date time. Java finds it with a pattern of its own: a date with an
/// optional upper case `T`, a time of one or two digits of hour and optional minutes,
/// seconds and fraction, and a zone of ASCII letters after a space. So it knows an integer
/// of two digits as an hour, takes any fraction for milliseconds, and finds no unit in a
/// date time that ends in `Z`, an offset, a lower case `t` or a zone with a `/` or `_`.
///
/// Behavior: io.deephaven.time.DateTimeUtils#parseTimePrecisionQuiet and its
/// `CAPTURING_DATETIME_PATTERN`
fn precision(text: &str) -> Precision {
    const SECOND: i64 = 1_000_000_000;
    // Java's text is longer than one UTF-16 unit; any shorter text is one byte here.
    if text.len() <= 1 {
        return Precision::None;
    }
    let pattern = regex::Regex::new(
        r"^(?:[0-9]{4}-[0-9]{2}-([0-9]{2})T?)?(?:([0-9][0-9]?)(?::([0-9][0-9])(?::([0-9][0-9]))?(?:\.([0-9]{1,9}))?)?)?(?: [a-zA-Z]+)?$",
    )
    .expect("a valid pattern");
    let Some(parts) = pattern.captures(text) else {
        return Precision::None;
    };
    let has = |group: usize| parts.get(group).is_some_and(|part| !part.is_empty());
    if has(5) {
        Precision::Nanos(SECOND / 1000)
    } else if has(4) {
        Precision::Nanos(SECOND)
    } else if has(3) {
        Precision::Nanos(60 * SECOND)
    } else if has(2) {
        Precision::Nanos(3600 * SECOND)
    } else if has(1) {
        Precision::Day
    } else {
        Precision::None
    }
}

/// `nanos` as the nanoseconds of an `Instant`, which Java refuses to count from the last
/// second a `long` of nanoseconds reaches.
///
/// Behavior: io.deephaven.time.DateTimeUtils#epochNanos(Instant), `#safeComputeNanos`
fn epoch_nanos(nanos: i128) -> Result<i64, String> {
    const SECOND: i128 = 1_000_000_000;
    let seconds = nanos.div_euclid(SECOND);
    if seconds >= i128::from(i64::MAX) / SECOND {
        return Err(format!(
            "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: Numeric overflow \
             detected during conversion of {seconds} to nanoseconds"
        ));
    }
    Ok(nanos as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_places_are_counted_only_in_plain_decimals() {
        assert_eq!(decimal_places("5"), Some(0));
        assert_eq!(decimal_places("5."), Some(0));
        assert_eq!(decimal_places("-1.50"), Some(2));
        assert_eq!(decimal_places("007.000"), Some(3));
        for text in [
            ".5", "+5", "1e3", " 5", "5 ", "5d", "-", "", "1.5.1", "Infinity", "--5",
        ] {
            assert_eq!(decimal_places(text), None, "{text:?}");
        }
    }

    #[test]
    fn a_date_time_begins_with_a_date() {
        assert_eq!(after_iso_date("2023-11-14T22:13 UTC"), Some("T22:13 UTC"));
        assert_eq!(after_iso_date("-2023-11-14"), Some(""));
        assert_eq!(after_iso_date("+12023-11-14 NY"), Some(" NY"));
        for text in [
            "202-11-14",
            "2023-1-14",
            "2023-11-1",
            "20231114",
            "x1",
            "1.5",
            "12:00",
            "",
        ] {
            assert_eq!(after_iso_date(text), None, "{text:?}");
        }
    }

    #[test]
    fn a_duration_begins_with_p_and_a_number() {
        for text in ["PT1H", "pt12:00", "-PT-1:00", "P1D", "P-2dT3H", "+PT5S"] {
            assert!(may_be_duration(text), "{text:?}");
        }
        for text in ["p", "pt", "PT", "pear", "P1", "Pd", "1D", "T1H", "ptx"] {
            assert!(!may_be_duration(text), "{text:?}");
        }
    }

    #[test]
    fn the_last_seconds_of_a_long_of_nanoseconds_are_refused() {
        assert_eq!(epoch_nanos(-1), Ok(-1));
        assert_eq!(
            epoch_nanos(9_223_372_035_999_999_999),
            Ok(9_223_372_035_999_999_999)
        );
        let refused = epoch_nanos(9_223_372_036_000_000_000).unwrap_err();
        assert!(refused.contains("conversion of 9223372036 to nanoseconds"));
        assert!(epoch_nanos(i128::from(i64::MAX) + 5).is_err());
    }

    use super::super::tests::static_table;
    use super::super::{filter, Condition, FilterError};
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
    };
    use oxhaven_contracts::table::Table;
    use oxhaven_expr::Compiler;
    use oxhaven_testkit::list_of;
    use std::sync::Arc;

    /// 2023-11-14T22:13:20Z.
    const T0: i64 = 1_700_000_000_000_000_000;
    const SECOND: i64 = 1_000_000_000;

    /// Sixteen rows, `K` = 1 to 16, with a column of each type. The Java server (42.9) was
    /// given this table, and the rows the tests below expect are the rows it kept.
    fn mixed() -> Arc<dyn Table> {
        let k: Int32Array = (1..=16).collect::<Vec<i32>>().into();
        let g = StringArray::from(vec![
            Some("a"),
            Some("B"),
            Some("Abc"),
            None,
            Some("true"),
            Some("5"),
            Some(""),
            Some("1.5"),
            Some("x1 É é"),
            Some("FALSE"),
            Some("55"),
            Some("é"),
            Some("Infinity"),
            Some("NaN"),
            Some("2023-11-14"),
            Some("p"),
        ]);
        let ints = [
            Some(5),
            None,
            Some(-5),
            Some(55),
            Some(0),
            Some(5),
            Some(i32::MAX),
            Some(15),
            Some(-7),
            Some(1),
            Some(100),
            Some(-1),
            Some(-15),
            Some(150),
            Some(1500),
            Some(6),
        ];
        let i: Int32Array = ints.into_iter().collect();
        let floats = [
            Some(1.5),
            None,
            Some(f64::NAN),
            Some(-0.0),
            Some(0.0),
            Some(5.0),
            Some(f64::INFINITY),
            Some(5.5),
            Some(5.999),
            Some(1.59),
            Some(1.6),
            Some(-1.5),
            Some(-1.59),
            Some(-1.6),
            Some(1e300),
            Some(6.0),
        ];
        let d: Float64Array = floats.into_iter().collect();
        // the float column holds the same numbers narrowed, but 0.5 in the first row and
        // 1e30 in the fifteenth
        let mut narrow = floats.map(|value| value.map(|value| value as f32));
        narrow[0] = Some(0.5);
        narrow[14] = Some(1e30);
        let f: Float32Array = narrow.into_iter().collect();
        let (t, o) = (Some(true), Some(false));
        let b = BooleanArray::from(vec![
            t, None, o, t, None, t, o, o, t, o, o, t, t, t, t, None,
        ]);
        let instants = vec![
            Some(T0),
            None,
            Some(T0 + SECOND / 2),
            Some(55 * SECOND),
            Some(3655 * SECOND - 1),
            Some(3655 * SECOND),
            Some(0),
            Some(T0 + SECOND - 1),
            Some(T0 + SECOND),
            Some(T0 + 60 * SECOND),
            Some(T0 - 1),
            Some(5 * SECOND),
            Some(T0 + SECOND / 1000),
            Some(T0 + SECOND / 1000 - 1),
            Some(1_699_920_000 * SECOND),
            Some(1700 * SECOND),
        ];
        let t = TimestampNanosecondArray::from(instants).with_timezone("UTC");
        let small = [
            Some(5),
            None,
            Some(-5),
            Some(55),
            Some(0),
            Some(127),
            Some(-127),
            Some(15),
            Some(1),
            Some(2),
            Some(100),
            Some(-1),
            Some(3),
            Some(4),
            Some(6),
            Some(7),
        ];
        let mut long = small.map(|value| value.map(i64::from));
        long[5] = Some(1_700_000_000);
        long[6] = Some(i64::MAX);
        let l: Int64Array = long.into_iter().collect();
        let y: Int8Array = small
            .into_iter()
            .map(|value| value.map(|v| v as i8))
            .collect();
        let mut short = small.map(|value| value.map(|v| v as i16));
        short[5] = Some(i16::MAX);
        short[6] = Some(-i16::MAX);
        let h: Int16Array = short.into_iter().collect();
        // a, null, c, d, 5, f, ...
        let c: UInt16Array = (1..=16u16)
            .map(|k| match k {
                2 => None,
                5 => Some(53),
                k => Some(96 + k),
            })
            .collect();
        let columns: Vec<(&str, ColumnType, ArrayRef)> = vec![
            ("K", ColumnType::Int, Arc::new(k)),
            ("G", ColumnType::String, Arc::new(g)),
            ("I", ColumnType::Int, Arc::new(i)),
            ("D", ColumnType::Double, Arc::new(d)),
            ("F", ColumnType::Float, Arc::new(f)),
            ("B", ColumnType::Boolean, Arc::new(b)),
            ("T", ColumnType::Instant, Arc::new(t)),
            ("L", ColumnType::Long, Arc::new(l)),
            ("Y", ColumnType::Byte, Arc::new(y)),
            ("H", ColumnType::Short, Arc::new(h)),
            ("C", ColumnType::Char, Arc::new(c)),
        ];
        static_table(columns)
    }

    fn search(text: &str, columns: &[&str]) -> Condition {
        Condition::Search {
            search_string: text.to_string(),
            columns: columns.iter().map(|name| (*name).to_string()).collect(),
        }
    }

    /// The `K` of the rows a search in these columns keeps.
    fn found_in(table: &Arc<dyn Table>, text: &str, columns: &[&str]) -> Vec<i64> {
        let result = filter(table, &[search(text, columns)], &Compiler)
            .unwrap_or_else(|e| panic!("{text:?} in {columns:?}: {e}"));
        result.row_set().iter().map(|key| key / 10 + 1).collect()
    }

    /// The `K` of the rows a search in every column but the `char` column keeps, which is
    /// the table the Java server searched.
    fn found(text: &str) -> Vec<i64> {
        let columns = ["K", "G", "I", "D", "F", "B", "T", "L", "Y", "H"];
        found_in(&mixed(), text, &columns)
    }

    #[test]
    fn a_string_column_is_searched_for_the_text_without_regard_to_ascii_case() {
        assert_eq!(found("a"), [1, 3, 10, 14]);
        assert_eq!(found("A"), [1, 3, 10, 14]);
        assert_eq!(found("abc"), [3]);
        assert_eq!(found(" "), [9]);
        // É and é are one letter only to a Unicode comparison
        assert_eq!(found("é"), [9, 12]);
        assert_eq!(found("É"), [9]);
        assert_eq!(found_in(&mixed(), "5", &["G"]), [6, 8, 11]);
    }

    #[test]
    fn an_integer_column_is_searched_for_the_number_the_text_is() {
        assert_eq!(found_in(&mixed(), "5", &["I"]), [1, 6]);
        assert_eq!(found_in(&mixed(), "+5", &["I"]), [1, 6]);
        assert_eq!(found_in(&mixed(), "-0", &["I"]), [5]);
        assert_eq!(found_in(&mixed(), "5.0", &["I", "L", "Y", "H"]), [0i64; 0]);
        assert_eq!(found_in(&mixed(), "1700000000", &["I", "L"]), [6]);
        // each type reads the text by itself: 127 is a byte, 32767 a short and no byte
        assert_eq!(found("127"), [6]);
        assert_eq!(found("32767"), [6]);
        assert_eq!(found_in(&mixed(), "32767", &["Y"]), [0i64; 0]);
        // the lowest value of a type is its null
        assert_eq!(found("-2147483648"), [2]);
        assert_eq!(found("-128"), [2]);
        assert_eq!(found("-9223372036854775808"), [2]);
    }

    #[test]
    fn a_floating_point_column_is_searched_for_the_range_the_decimal_stands_for() {
        // [5, 6)
        assert_eq!(found_in(&mixed(), "5", &["D"]), [6, 8, 9]);
        assert_eq!(found_in(&mixed(), "5.", &["D", "F"]), [6, 8, 9]);
        // [1.5, 1.6) and [1.50, 1.51): the float column narrows the same ends
        assert_eq!(found_in(&mixed(), "1.5", &["D"]), [1, 10]);
        assert_eq!(found_in(&mixed(), "1.5", &["F"]), [10]);
        assert_eq!(found_in(&mixed(), "1.50", &["D", "F"]), [1]);
        assert_eq!(found_in(&mixed(), "1.6", &["D", "F"]), [11]);
        // (-1.6, -1.5]
        assert_eq!(found_in(&mixed(), "-1.5", &["D", "F"]), [12, 13]);
        assert_eq!(found_in(&mixed(), "-1.6", &["D", "F"]), [14]);
        // both zeros are from zero up
        assert_eq!(found_in(&mixed(), "0.0", &["D", "F"]), [4, 5]);
        assert_eq!(found_in(&mixed(), "-0.0", &["D", "F"]), [4, 5]);
        assert_eq!(found_in(&mixed(), "-0", &["D"]), [4, 5]);
        assert_eq!(found_in(&mixed(), "-0", &["F"]), [1, 4, 5]);
    }

    #[test]
    fn a_number_that_is_no_plain_decimal_is_searched_for_as_one_value() {
        assert_eq!(found_in(&mixed(), ".5", &["D", "F"]), [1]);
        assert_eq!(found_in(&mixed(), "+5", &["D", "F"]), [6]);
        assert_eq!(found_in(&mixed(), " 5", &["D", "F"]), [6]);
        assert_eq!(found_in(&mixed(), "5d", &["D", "F"]), [6]);
        assert_eq!(found_in(&mixed(), "1.5e0", &["D", "F"]), [1]);
        assert_eq!(found_in(&mixed(), "1e0", &["D", "F"]), [0i64; 0]);
        assert_eq!(found_in(&mixed(), "Infinity", &["D", "F"]), [7]);
        assert_eq!(found_in(&mixed(), "NaN", &["D", "F"]), [0i64; 0]);
        // 1e300 is infinite as a float
        assert_eq!(found_in(&mixed(), "1e300", &["D"]), [15]);
        assert_eq!(found_in(&mixed(), "1e300", &["F"]), [7]);
        assert_eq!(found_in(&mixed(), "1E30", &["D", "F"]), [15]);
        // the value Java reserves for null
        assert_eq!(found_in(&mixed(), "-1.7976931348623157E308", &["D"]), [2]);
    }

    #[test]
    fn a_boolean_and_a_char_column_are_searched_for_one_value() {
        assert_eq!(
            found_in(&mixed(), "true", &["B"]),
            [1, 4, 6, 9, 12, 13, 14, 15]
        );
        assert_eq!(found_in(&mixed(), "False", &["B"]), [3, 7, 8, 10, 11]);
        assert_eq!(found_in(&mixed(), "t", &["B"]), [0i64; 0]);
        assert_eq!(found("TRUE"), [1, 4, 5, 6, 9, 12, 13, 14, 15]);

        assert_eq!(found_in(&mixed(), "a", &["C"]), [1]);
        assert_eq!(found_in(&mixed(), "5", &["C"]), [5]);
        assert_eq!(found_in(&mixed(), "e", &["C"]), [0i64; 0]);
        assert_eq!(found_in(&mixed(), "ac", &["C"]), [0i64; 0]);
        assert_eq!(found_in(&mixed(), "\u{ffff}", &["C"]), [2]);
    }

    #[test]
    fn an_instant_column_is_searched_for_the_unit_of_time_the_text_ends_in() {
        let table = mixed();
        let instants = |text: &str| found_in(&table, text, &["T"]);
        assert_eq!(instants("2023-11-14T22:13:20 UTC"), [1, 3, 8, 13, 14]);
        assert_eq!(instants("2023-11-14T22:13 UTC"), [1, 3, 8, 9, 11, 13, 14]);
        // any fraction is a millisecond
        assert_eq!(instants("2023-11-14T22:13:20.5 UTC"), [3]);
        assert_eq!(instants("2023-11-14T22:13:20.000 UTC"), [1, 14]);
        // Java does not find the unit of a date time that ends in Z, or of an integer:
        // the range ends where it starts
        assert_eq!(instants("2023-11-14T22:13:20Z"), [0i64; 0]);
        assert_eq!(instants("1700000000"), [0i64; 0]);
        assert_eq!(instants("5"), [0i64; 0]);
        // but an integer of two digits looks like an hour to it: that many seconds after
        // the epoch and the hour from there
        assert_eq!(instants("55"), [4, 5, 16]);
        assert_eq!(instants("07"), [4, 16]);
        // no instant at all
        for text in [
            "2023-11-14",
            "22:13",
            "x1",
            "1.5",
            "pt",
            "-9223372036854775808",
        ] {
            assert_eq!(instants(text), [0i64; 0], "{text:?}");
        }
    }

    fn refused(text: &str, columns: &[&str]) -> FilterError {
        match filter(&mixed(), &[search(text, columns)], &Compiler) {
            Ok(_) => panic!("{text:?} was accepted"),
            Err(error) => error,
        }
    }

    #[test]
    fn an_instant_in_the_last_seconds_of_a_long_fails_the_search() {
        for text in ["9223372036854775807", "9223372036"] {
            let FilterError::Invalid(message) = refused(text, &[]) else {
                panic!("{text:?} failed otherwise");
            };
            assert!(message.contains("DateTimeOverflowException"), "{message}");
        }
        assert_eq!(found("9223372035"), [0i64; 0]);
        // without an Instant column the text is only a number
        assert_eq!(found_in(&mixed(), "9223372036854775807", &["L"]), [7]);
    }

    #[test]
    fn a_search_reads_the_named_columns_or_all_and_nothing_found_is_no_row() {
        let table = mixed();
        assert_eq!(found("5"), [1, 5, 6, 8, 9, 11]);
        assert_eq!(found_in(&table, "5", &["I", "D"]), [1, 6, 8, 9]);
        // a name that is no column is passed over
        assert_eq!(found_in(&table, "5", &["Nope", "I"]), [1, 6]);
        assert_eq!(found_in(&table, "5", &["Nope"]), [0i64; 0]);
        // no column can hold the text, and the empty text is in no column
        assert_eq!(found_in(&table, "a", &["I"]), [0i64; 0]);
        assert_eq!(found(""), [0i64; 0]);
        // with no names every column is read, the char column too
        assert_eq!(found_in(&table, "a", &[]), [1, 3, 10, 14]);
        assert_eq!(found_in(&table, "f", &[]), [6, 10, 13]);
    }

    #[test]
    fn a_search_passes_over_the_columns_that_hold_formats() {
        let ones: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let fives = || -> ArrayRef { Arc::new(Int32Array::from(vec![5, 5])) };
        let table = static_table(vec![
            ("K", ColumnType::Int, ones),
            ("K__TABLE_STYLE_FORMAT", ColumnType::Int, fives()),
            ("K__TABLE_NUMBER_FORMAT", ColumnType::Int, fives()),
            ("K__TABLE_DATE_FORMAT", ColumnType::Int, fives()),
        ]);
        assert_eq!(found_in(&table, "5", &[]), [0i64; 0]);
        assert_eq!(found_in(&table, "5", &["K__TABLE_STYLE_FORMAT"]), [0i64; 0]);
        assert_eq!(found_in(&table, "2", &[]), [2]);
        // only the suffix makes a format column
        let table = static_table(vec![("__TABLE_STYLE_FORMAT_K", ColumnType::Int, fives())]);
        assert_eq!(found_in(&table, "5", &[]), [1, 2]);
    }

    #[test]
    fn a_search_joins_with_other_conditions_and_cannot_be_negated() {
        let table = mixed();
        let kept = |conditions: &[Condition]| -> Vec<i64> {
            let result = filter(&table, conditions, &Compiler).unwrap();
            result.row_set().iter().map(|key| key / 10 + 1).collect()
        };
        let columns = ["K", "G", "I", "D", "F", "B", "T", "L", "Y", "H"];
        assert_eq!(kept(&[search("a", &columns), search("b", &columns)]), [3]);
        let either = Condition::Or(vec![
            search("1.5", &columns),
            Condition::IsNull {
                column: "G".to_string(),
            },
        ]);
        assert_eq!(kept(&[either]), [1, 4, 8, 10]);
        let negated = Condition::Not(Box::new(search("a", &[])));
        assert_eq!(
            filter(&table, &[negated], &Compiler).err(),
            Some(FilterError::Invalid("Cannot not() a search".to_string()))
        );
    }

    fn lists(element: ColumnType, elements: ArrayRef) -> Arc<dyn Table> {
        let vector = ColumnType::vector(element);
        let cells = list_of(&vector, [Some(1), Some(1)], elements);
        let ones: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        static_table(vec![("K", ColumnType::Int, ones), ("V", vector, cells)])
    }

    #[test]
    fn a_vector_column_whose_elements_cannot_hold_the_text_is_passed_over() {
        let table = lists(ColumnType::Int, Arc::new(Int32Array::from(vec![1, 2])));
        assert_eq!(found_in(&table, "x", &[]), [0i64; 0]);
        assert_eq!(found_in(&table, "2", &["K"]), [2]);
    }

    /// `K` = 1 to `n`, and the column `V` of `column_type` with cells of these lengths
    /// (`None` for a null cell) holding `elements` in turn.
    fn listed(
        column_type: ColumnType,
        lengths: &[Option<usize>],
        elements: ArrayRef,
    ) -> Arc<dyn Table> {
        let cells = list_of(&column_type, lengths.iter().copied(), elements);
        let k: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=lengths.len() as i32));
        static_table(vec![("K", ColumnType::Int, k), ("V", column_type, cells)])
    }

    #[test]
    fn a_vector_or_array_keeps_the_rows_with_an_element_the_text_is() {
        // [1, 2], [], [4, 3], [2], [5, 2, 2]
        let lengths = [Some(2), Some(0), Some(2), Some(1), Some(3)];
        let ints = || -> ArrayRef { Arc::new(Int32Array::from(vec![1, 2, 4, 3, 2, 5, 2, 2])) };
        for column_type in [
            ColumnType::vector(ColumnType::Int),
            ColumnType::array(ColumnType::Int),
        ] {
            let table = listed(column_type.clone(), &lengths, ints());
            let found = |text: &str| found_in(&table, text, &["V"]);
            assert_eq!(found("2"), [1, 4, 5], "{column_type:?}");
            assert_eq!(found("5"), [5]);
            assert_eq!(found("6"), [0i64; 0]);
            assert_eq!(found("x"), [0i64; 0]);
            assert_eq!(found("-2147483648"), [0i64; 0]);
            // the key column too, with every column searched
            assert_eq!(found_in(&table, "3", &[]), [3]);
        }
    }

    #[test]
    fn a_null_element_of_an_array_or_an_object_vector_is_an_element_like_any() {
        // [1, 2], [null, 3], [2]
        let lengths = [Some(2), Some(2), Some(1)];
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(3),
            Some(2),
        ]));
        let table = listed(ColumnType::array(ColumnType::Int), &lengths, ints);
        assert_eq!(found_in(&table, "2", &["V"]), [1, 3]);
        assert_eq!(found_in(&table, "3", &["V"]), [2]);
        // the lowest int is its null, and finds the null element
        assert_eq!(found_in(&table, "-2147483648", &["V"]), [2]);
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            None,
            Some("c"),
            Some("b"),
        ]));
        let table = listed(ColumnType::vector(ColumnType::String), &lengths, strings);
        assert_eq!(found_in(&table, "c", &["V"]), [2]);
    }

    /// The `K` of the rows the search keeps, or `None` if it fails with a
    /// `NullPointerException`.
    fn found_or_failed(table: &Arc<dyn Table>, text: &str, columns: &[&str]) -> Option<Vec<i64>> {
        match filter(table, &[search(text, columns)], &Compiler) {
            Ok(result) => Some(result.row_set().iter().map(|key| key / 10 + 1).collect()),
            Err(FilterError::Failed(failure)) => {
                assert!(
                    failure.message.contains("NullPointerException"),
                    "{failure}"
                );
                None
            }
            Err(other) => panic!("{text:?}: {other}"),
        }
    }

    /// Behavior: recorded from Java 42.9 (`search_vectors`): `Cannot invoke
    /// "java.lang.Integer.intValue()" because the return value of
    /// "ValueIteratorOfInt.next()" is null`.
    #[test]
    fn a_null_element_of_a_primitive_vector_fails_the_search_unless_one_before_it_passes() {
        // [1, 2, null], [null, 3], [], [3]
        let lengths = [Some(3), Some(2), Some(0), Some(1)];
        let ints = || -> ArrayRef {
            Arc::new(Int32Array::from(vec![
                Some(1),
                Some(2),
                None,
                None,
                Some(3),
                Some(3),
            ]))
        };
        let table = listed(ColumnType::vector(ColumnType::Int), &lengths, ints());
        let found = |text: &str| found_or_failed(&table, text, &["V"]);
        // 2 passes before the null of the first row; the second row starts with a null
        assert_eq!(found("2"), None);
        assert_eq!(found("1"), None);
        assert_eq!(found("3"), None);
        assert_eq!(found("-2147483648"), None);
        // a text the elements cannot be makes no filter, and reads nothing
        assert_eq!(found("x"), Some(vec![]));
        // a row the key column keeps is not read, and 2 passes before the first row's null
        assert_eq!(found_or_failed(&table, "2", &["K", "V"]), Some(vec![1, 2]));
        let only_first = filter(&table, &[search("1", &["K"])], &Compiler).unwrap();
        assert_eq!(found_or_failed(&only_first, "2", &["V"]), Some(vec![1]));
        assert_eq!(found_or_failed(&only_first, "1", &["V"]), Some(vec![1]));
        assert_eq!(found_or_failed(&only_first, "4", &["V"]), None);
        // an array of the same cells reads the null element as one
        let table = listed(ColumnType::array(ColumnType::Int), &lengths, ints());
        assert_eq!(found_or_failed(&table, "3", &["V"]), Some(vec![2, 4]));

        // every primitive type, the value Java reserves for null included
        let lengths = [Some(2)];
        let cases: Vec<(ColumnType, &str, ArrayRef)> = vec![
            (
                ColumnType::Double,
                "0",
                Arc::new(Float64Array::from(vec![Some(0.5), Some(-f64::MAX)])),
            ),
            (
                ColumnType::Float,
                "0",
                Arc::new(Float32Array::from(vec![Some(0.5), None])),
            ),
            (
                ColumnType::Long,
                "7",
                Arc::new(Int64Array::from(vec![Some(7), Some(i64::MIN)])),
            ),
            (
                ColumnType::Short,
                "7",
                Arc::new(Int16Array::from(vec![Some(7), None])),
            ),
            (
                ColumnType::Byte,
                "7",
                Arc::new(Int8Array::from(vec![Some(7), Some(i8::MIN)])),
            ),
            (
                ColumnType::Char,
                "7",
                Arc::new(UInt16Array::from(vec![Some(55), Some(0xFFFF)])),
            ),
        ];
        for (element, first, cells) in cases {
            let table = listed(ColumnType::vector(element.clone()), &lengths, cells.clone());
            assert_eq!(found_or_failed(&table, "8", &["V"]), None, "{element:?}");
            assert_eq!(
                found_or_failed(&table, first, &["V"]),
                Some(vec![1]),
                "{element:?}"
            );
            let table = listed(ColumnType::array(element.clone()), &lengths, cells);
            assert_eq!(
                found_or_failed(&table, "8", &["V"]),
                Some(vec![]),
                "{element:?}"
            );
        }
    }

    #[test]
    fn each_element_type_reads_the_text_as_its_column_would() {
        // [1.5, NaN], [1.59, null], [-1.5], [], [1.6]
        let lengths = [Some(2), Some(2), Some(1), Some(0), Some(1)];
        let doubles: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(f64::NAN),
            Some(1.59),
            None,
            Some(-1.5),
            Some(1.6),
        ]));
        let table = listed(ColumnType::array(ColumnType::Double), &lengths, doubles);
        // [1.5, 1.6)
        assert_eq!(found_in(&table, "1.5", &["V"]), [1, 2]);
        assert_eq!(found_in(&table, "-1.5", &["V"]), [3]);
        assert_eq!(found_in(&table, "NaN", &["V"]), [0i64; 0]);
        assert_eq!(found_in(&table, "1.6", &["V"]), [5]);

        // ["Ab", null], [], ["x"], [null]
        let lengths = [Some(2), Some(0), Some(1), Some(1)];
        let strings: ArrayRef =
            Arc::new(StringArray::from(vec![Some("Ab"), None, Some("x"), None]));
        let table = listed(ColumnType::array(ColumnType::String), &lengths, strings);
        assert_eq!(found_in(&table, "a", &["V"]), [1]);
        assert_eq!(found_in(&table, "X", &["V"]), [3]);
        assert_eq!(found_in(&table, "null", &["V"]), [0i64; 0]);

        // [true], [false, null], [null]
        let lengths = [Some(1), Some(2), Some(1)];
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            None,
        ]));
        let table = listed(ColumnType::vector(ColumnType::Boolean), &lengths, booleans);
        assert_eq!(found_in(&table, "TRUE", &["V"]), [1]);
        assert_eq!(found_in(&table, "false", &["V"]), [2]);

        // ['a', 'b'], ['c'], [null]
        let lengths = [Some(2), Some(1), Some(1)];
        let chars: ArrayRef = Arc::new(UInt16Array::from(vec![Some(97), Some(98), Some(99), None]));
        let table = listed(ColumnType::array(ColumnType::Char), &lengths, chars);
        assert_eq!(found_in(&table, "b", &["V"]), [1]);
        assert_eq!(found_in(&table, "\u{ffff}", &["V"]), [3]);
        assert_eq!(found_in(&table, "bc", &["V"]), [0i64; 0]);

        // [T0, null], [T0 + 1s], [T0 + 1min]
        let lengths = [Some(2), Some(1), Some(1)];
        let instants: ArrayRef = Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(T0),
                None,
                Some(T0 + SECOND),
                Some(T0 + 60 * SECOND),
            ])
            .with_timezone("UTC"),
        );
        let table = listed(ColumnType::vector(ColumnType::Instant), &lengths, instants);
        assert_eq!(found_in(&table, "2023-11-14T22:13:20 UTC", &["V"]), [1]);
        assert_eq!(found_in(&table, "2023-11-14T22:13 UTC", &["V"]), [1, 2]);

        // longs, shorts, bytes and floats
        let lengths = [Some(1), Some(2)];
        let longs: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9]));
        let table = listed(ColumnType::vector(ColumnType::Long), &lengths, longs);
        assert_eq!(found_in(&table, "8", &["V"]), [2]);
        let shorts: ArrayRef = Arc::new(Int16Array::from(vec![7, 8, 9]));
        let table = listed(ColumnType::array(ColumnType::Short), &lengths, shorts);
        assert_eq!(found_in(&table, "7", &["V"]), [1]);
        let bytes: ArrayRef = Arc::new(Int8Array::from(vec![7, 8, 9]));
        let table = listed(ColumnType::vector(ColumnType::Byte), &lengths, bytes);
        assert_eq!(found_in(&table, "8", &["V"]), [2]);
        assert_eq!(found_in(&table, "300", &["V"]), [0i64; 0]);
        let floats: ArrayRef = Arc::new(Float32Array::from(vec![0.5, 1.5, 2.0]));
        let table = listed(ColumnType::array(ColumnType::Float), &lengths, floats);
        assert_eq!(found_in(&table, "1.5", &["V"]), [2]);

        // a vector of vectors has no quick filter for its elements
        let inner = ColumnType::vector(ColumnType::Int);
        let cells = list_of(
            &inner,
            [Some(1), Some(1)],
            Arc::new(Int32Array::from(vec![2, 3])),
        );
        let table = listed(ColumnType::vector(inner), &[Some(1), Some(1)], cells);
        assert_eq!(found_in(&table, "2", &["V"]), [0i64; 0]);
    }

    #[test]
    fn a_null_vector_or_array_fails_the_search_when_it_is_read() {
        // [1], null, [3]
        let lengths = [Some(1), None, Some(1)];
        let ints = || -> ArrayRef { Arc::new(Int32Array::from(vec![1, 3])) };
        for column_type in [
            ColumnType::vector(ColumnType::Int),
            ColumnType::array(ColumnType::Int),
        ] {
            let table = listed(column_type, &lengths, ints());
            let failed = |text: &str, columns: &[&str]| match filter(
                &table,
                &[search(text, columns)],
                &Compiler,
            ) {
                Err(FilterError::Failed(failure)) => failure.message,
                other => panic!("{text:?}: {:?}", other.map(|t| t.row_set())),
            };
            assert!(failed("1", &["V"]).contains("NullPointerException"));
            assert!(failed("3", &[]).contains("NullPointerException"));
            // a text the elements cannot be makes no filter of the column
            assert_eq!(found_in(&table, "x", &[]), [0i64; 0]);
            // a row the key column keeps is not read again
            assert_eq!(found_in(&table, "2", &[]), [2]);
            // and with no rows nothing is read
            let empty = filter(&table, &[search("4", &["K"])], &Compiler).unwrap();
            assert_eq!(found_in(&empty, "1", &["V"]), [0i64; 0]);
        }
    }

    #[test]
    fn a_boolean_array_fails_a_search_for_a_boolean() {
        let lengths = [Some(1), Some(0)];
        let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let table = listed(
            ColumnType::array(ColumnType::PrimitiveBoolean),
            &lengths,
            booleans,
        );
        match filter(&table, &[search("true", &["V"])], &Compiler) {
            Err(FilterError::Failed(failure)) => {
                assert!(failure.message.contains("ClassCastException"))
            }
            other => panic!("{:?}", other.map(|t| t.row_set())),
        }
        assert_eq!(found_in(&table, "x", &["V"]), [0i64; 0]);
    }

    #[test]
    #[should_panic(expected = "unported: io.deephaven.time.DateTimeUtils#timeZone")]
    fn a_search_for_a_day_in_an_instant_column_is_not_ported() {
        let _ = filter(&mixed(), &[search("2023-11-14 UTC", &["T"])], &Compiler);
    }

    #[test]
    #[should_panic(
        expected = "unported: io.deephaven.engine.table.impl.select.WhereFilterFactory.InferenceResult"
    )]
    fn a_search_for_a_time_of_day_in_an_instant_column_is_not_ported() {
        let _ = filter(&mixed(), &[search("PT1H", &["T"])], &Compiler);
    }

    /// Every expectation was observed on Deephaven 42.9 with the same rows.
    #[test]
    fn a_search_for_a_date_time_in_a_named_zone_spans_its_finest_unit() {
        // 2023-11-14 at these times, UTC
        let at = |hour: i64, minute: i64, millis: i64| {
            Some(1_699_920_000 * SECOND + (hour * 3600 + minute * 60) * SECOND + millis * 1_000_000)
        };
        let instants = vec![
            at(15, 0, 0),
            at(15, 0, 30_000),
            at(15, 0, 59_999),
            at(15, 1, 0),
            at(1, 0, 30_500),
            at(1, 0, 30_600),
            at(10, 0, 0),
            at(9, 0, 0),
            at(1, 0, 31_000),
        ];
        let t = TimestampNanosecondArray::from(instants).with_timezone("UTC");
        let table = static_table(vec![("T", ColumnType::Instant, Arc::new(t) as ArrayRef)]);
        let found = |text: &str| {
            let result = filter(&table, &[search(text, &["T"])], &Compiler)
                .unwrap_or_else(|e| panic!("{text:?}: {e}"));
            result
                .row_set()
                .iter()
                .map(|key| key / 10)
                .collect::<Vec<i64>>()
        };
        assert_eq!(found("2023-11-14T10:00 ET"), [0, 1, 2]);
        assert_eq!(found("2023-11-14T10:00:30 ET"), [1]);
        assert_eq!(found("2023-11-14T10:00:30.5 JP"), [4]);
        assert_eq!(found("2023-11-14T10:00:30 PT"), [0i64; 0]);
        assert_eq!(found("2023-11-14T10:00 UTC"), [6]);
        assert_eq!(found("2023-11-14T01:00:30.5 UTC"), [4]);
        // Java's pattern of units knows no zone with a `/`, no offset, no `Z` and no lower
        // case `t`: the range ends where it starts.
        for text in [
            "2023-11-14T10:00 America/New_York",
            "2023-11-14T10:00 Asia/Tokyo",
            "2023-11-14T10:00 Etc/GMT+5",
            "2023-11-14t10:00 UTC",
            "2023-11-14T10:00+01:00",
            "2023-11-14T10:00:00Z",
            "2023-11-14T10:00:00-05:00[America/New_York]",
        ] {
            assert_eq!(found(text), [0i64; 0], "{text}");
        }
        // no instant: Java reads no zone `NY`, `Z` after a space or `jp`, nor an hour alone
        for text in [
            "2023-11-14T10:00 NY",
            "2023-11-14T10 NY",
            "2023-11-14T10:00:30 Z",
            "2023-11-14T10:00:30.5 jp",
        ] {
            assert_eq!(found(text), [0i64; 0], "{text}");
        }
    }
}
