//! Columns of the `java.math` and `java.time` types, and `byte[]`: what a formula can do
//! with them (`isNull`, `toString()`, `new String(bytes)`, `.length`) and that it can do
//! nothing else.
//!
//! The expected texts are what Java 42.9 answered for the same values, read from
//! `crates/oxhaven-parquet/tests/data/more_types.parquet` and printed with the same
//! formulas.

use super::*;
use arrow::array::{BinaryArray, Date32Array, ListArray, Time64NanosecondArray};
use arrow::datatypes::Int8Type as ByteType;
use oxhaven_column::java_values::{big_decimal_cell, big_integer_cell};
use oxhaven_contracts::expr::Formula;

/// A `byte[]` column: a list of bytes under the element field every list column has.
fn byte_arrays(cells: Vec<Option<Vec<Option<i8>>>>) -> Column {
    let column_type = ColumnType::array(ColumnType::Byte);
    let lists = ListArray::from_iter_primitive::<ByteType, _, _>(cells);
    let (_, offsets, elements, nulls) = lists.into_parts();
    let DataType::List(field) = column_type.arrow_type() else {
        unreachable!("an array is a list")
    };
    let lists = ListArray::new(field, offsets, elements, nulls);
    (column_type, Arc::new(lists))
}

fn objects() -> TestTable {
    let decimal = |unscaled: i128, scale: i32| big_decimal_cell(scale, &big_integer_cell(unscaled));
    let decimals = [Some(decimal(150, 2)), None, Some(decimal(1, 10))];
    let integers = [
        Some(big_integer_cell(i128::from(u64::MAX))),
        None,
        Some(big_integer_cell(-1)),
    ];
    let binary =
        |cells: [Option<Vec<u8>>; 3]| -> ArrayRef { Arc::new(BinaryArray::from_iter(cells)) };
    let local = TimestampNanosecondArray::from(vec![Some(0), None, Some(-1)]);
    let bytes = byte_arrays(vec![Some(vec![Some(97), Some(98)]), None, Some(vec![])]);
    TestTable::new([
        ("Dec", (ColumnType::BigDecimal, binary(decimals))),
        ("Big", (ColumnType::BigInteger, binary(integers))),
        (
            "Date",
            (
                ColumnType::LocalDate,
                Arc::new(Date32Array::from(vec![Some(19_724), None, Some(-1)])),
            ),
        ),
        (
            "Time",
            (
                ColumnType::LocalTime,
                Arc::new(Time64NanosecondArray::from(vec![
                    Some(3_723_000_000_000),
                    None,
                    Some(1),
                ])),
            ),
        ),
        ("Local", (ColumnType::LocalDateTime, Arc::new(local))),
        ("Bytes", bytes),
        ("S", strings([Some("a"), Some("b"), Some("c")])),
    ])
}

/// Behavior: `QueryLanguageFunctionUtils#eq(Object, Object)`, `#less(Comparable,
/// Comparable)` (null first) and string concatenation, which prints `LocalDate#toString`.
#[test]
fn a_date_is_compared_and_printed() {
    let table = objects();
    table.assert("Date == Date", flags([true, true, true]));
    table.assert("Date < '2024-01-02'", flags([false, true, true]));
    table.assert("Date >= '2024-01-02'", flags([true, false, false]));
    table.assert(
        "S + Date",
        strings([Some("a2024-01-02"), Some("bnull"), Some("c1969-12-31")]),
    );
}

fn compile(table: &TestTable, formula: &str) -> Result<Arc<dyn Formula>, FormulaError> {
    let expr = Compiler.parse(formula).expect("parses");
    Compiler.compile(&expr, &table.definition)
}

fn texts(table: &TestTable, formula: &str) -> Vec<Option<String>> {
    let formula = compile(table, formula).unwrap_or_else(|e| panic!("{formula}: {e}"));
    assert_eq!(formula.result_type(), &ColumnType::String);
    let values = formula.evaluate(table, &table.rows, false).unwrap();
    let values = values.as_string::<i32>().iter();
    values.map(|value| value.map(str::to_string)).collect()
}

fn some<const N: usize>(values: [Option<&str>; N]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(str::to_string)).collect()
}

#[test]
fn to_string_prints_each_type_as_java_does() {
    let table = objects();
    let guarded = |column: &str| format!("isNull({column}) ? null : {column}.toString()");
    assert_eq!(
        texts(&table, &guarded("Dec")),
        some([Some("1.50"), None, Some("1E-10")])
    );
    assert_eq!(
        texts(&table, &guarded("Big")),
        some([Some("18446744073709551615"), None, Some("-1")])
    );
    assert_eq!(
        texts(&table, &guarded("Date")),
        some([Some("2024-01-02"), None, Some("1969-12-31")])
    );
    assert_eq!(
        texts(&table, &guarded("Time")),
        some([Some("01:02:03"), None, Some("00:00:00.000000001")])
    );
    assert_eq!(
        texts(&table, &guarded("Local")),
        some([
            Some("1970-01-01T00:00"),
            None,
            Some("1969-12-31T23:59:59.999999999")
        ])
    );
    // the formula reads the column once, whichever of the two uses names it first
    let formula = compile(&table, &guarded("Date")).unwrap();
    assert_eq!(formula.columns(), ["Date"]);
}

#[test]
fn a_method_called_on_a_null_object_is_a_null_pointer_exception() {
    let table = objects();
    for column in ["Dec", "Big", "Date", "Time", "Local"] {
        let formula = compile(&table, &format!("{column}.toString()")).unwrap();
        let failure = formula.evaluate(&table, &table.rows, false).unwrap_err();
        assert!(
            failure
                .message
                .starts_with("java.lang.NullPointerException"),
            "{column}: {}",
            failure.message
        );
        // the rows that hold a value are printed
        let rows = RowSet::from_keys([0, 2]);
        assert_eq!(formula.evaluate(&table, &rows, false).unwrap().len(), 2);
    }
    let formula = compile(&table, "new String(Bytes)").unwrap();
    let failure = formula.evaluate(&table, &table.rows, false).unwrap_err();
    assert!(
        failure
            .message
            .starts_with("java.lang.NullPointerException"),
        "{}",
        failure.message
    );
}

#[test]
fn is_null_tests_an_object_column() {
    let table = objects();
    for column in ["Dec", "Big", "Date", "Time", "Local", "Bytes"] {
        let formula = compile(&table, &format!("isNull({column})")).unwrap();
        assert_eq!(formula.result_type(), &ColumnType::Boolean);
        let nulls = formula.evaluate(&table, &table.rows, false).unwrap();
        assert_eq!(
            render(&nulls),
            render(&flags([false, true, false]).1),
            "{column}"
        );
    }
}

#[test]
fn bytes_become_text_and_have_a_length() {
    let table = objects();
    assert_eq!(
        texts(&table, "isNull(Bytes) ? null : new String(Bytes)"),
        some([Some("ab"), None, Some("")])
    );
    let length = compile(&table, "isNull(Bytes) ? NULL_INT : Bytes.length").unwrap();
    assert_eq!(length.result_type(), &ColumnType::Int);
    let lengths = length.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(render(&lengths), render(&ints([Some(2), None, Some(0)]).1));

    // bytes that are not UTF-8 are refused by the name of the Java constructor
    let table = TestTable::new([("Bytes", byte_arrays(vec![Some(vec![Some(-1), None])]))]);
    let formula = compile(&table, "new String(Bytes)").unwrap();
    let failure = formula.evaluate(&table, &table.rows, false).unwrap_err();
    assert!(
        failure.message.contains("java.lang.String#String(byte[])"),
        "{}",
        failure.message
    );
}

#[test]
fn nothing_else_is_done_with_an_object_column() {
    let table = objects();
    // `Dec` and `Big` are values now: `big_numbers.rs`.
    for formula in [
        "Date + 1",
        "Time.getHour()",
        "Local.toString().length() > 0 ? Local : Local",
    ] {
        let Err(FormulaError::Unsupported(message)) = compile(&table, formula).map(|_| ()) else {
            panic!("{formula} compiled, or failed otherwise than as unsupported");
        };
        assert!(message.contains("of type"), "{formula}: {message}");
    }
    // `new String(S)` and `new String()` are computed, as in Java: `statics.rs`.
    assert!(Compiler.parse("new String").is_err());
    for formula in ["new String(Bytes, Bytes)", "new Object()"] {
        let expr = Compiler.parse(formula);
        let refused = expr.and_then(|expr| Compiler.compile(&expr, &table.definition).map(|_| ()));
        assert!(
            matches!(refused, Err(FormulaError::Unsupported(_))),
            "{formula}: {refused:?}"
        );
    }
}

/// The table of the comparisons seen on Java 42.9: a `BigDecimal` `BD`, a `BigInteger`
/// `BI` and numbers, with a null in row 2 of each (and in row 7 of `L`, which Java made
/// as `Long.MIN_VALUE + 1`: null plus one is null).
fn big_numbers() -> TestTable {
    let decimal = |unscaled: i128, scale: i32| big_decimal_cell(scale, &big_integer_cell(unscaled));
    let binary =
        |cells: Vec<Option<Vec<u8>>>| -> ArrayRef { Arc::new(BinaryArray::from_iter(cells)) };
    let decimals = vec![
        Some(decimal(-150, 2)),
        Some(decimal(10, 2)),
        None,
        Some(decimal(1, -20)),
        Some(decimal(100_000_000_000_000_000_000_000, 3)),
        Some(decimal(1_000_000_014_901_161_193_847_656_250, 28)),
        Some(decimal(97, 0)),
        Some(decimal(0, 3)),
    ];
    let integers = vec![
        Some(big_integer_cell(-2)),
        Some(big_integer_cell(0)),
        None,
        Some(big_integer_cell(100_000_000_000_000_000_000)),
        Some(big_integer_cell(1 << 63)),
        Some(big_integer_cell(1)),
        Some(big_integer_cell(97)),
        Some(big_integer_cell(i128::from(i64::MIN))),
    ];
    TestTable::new([
        ("BD", (ColumnType::BigDecimal, binary(decimals))),
        ("BI", (ColumnType::BigInteger, binary(integers))),
        (
            "D",
            doubles([
                Some(0.1),
                Some(0.1),
                None,
                Some(1e20),
                Some(f64::NAN),
                Some(0.1),
                Some(97.0),
                Some(-0.0),
            ]),
        ),
        (
            "F",
            floats([
                Some(0.1),
                Some(0.1),
                None,
                Some(1e20),
                Some(f32::NAN),
                Some(0.1),
                Some(97.0),
                Some(-0.0),
            ]),
        ),
        (
            "L",
            longs([
                Some(-2),
                Some(0),
                None,
                Some(1),
                Some(i64::MAX),
                Some(1),
                Some(97),
                None,
            ]),
        ),
        (
            "C",
            chars([
                Some('a'),
                Some('a'),
                None,
                Some('a'),
                Some('a'),
                Some('a'),
                Some('a'),
                Some('a'),
            ]),
        ),
        (
            "I",
            ints([
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
            ]),
        ),
        ("S", strings([Some("s"); 8])),
        ("Z", booleans([Some(true); 8])),
    ])
}

/// Behavior: `QueryLanguageFunctionUtils#eq`, `#less` and their relatives of `BigDecimal`
/// and `BigInteger` with each other, the primitive numbers and `char`. Each row is what
/// Java 42.9 answered for the formula over the same values.
#[test]
fn big_numbers_compare_by_value_as_java_does() {
    let table = big_numbers();
    for (formula, expected) in [
        (
            "BD == D",
            [false, true, true, true, false, false, true, true],
        ),
        (
            "BD < D",
            [true, false, false, false, false, false, false, false],
        ),
        (
            "BD > D",
            [false, false, false, false, false, true, false, false],
        ),
        (
            "BD != D",
            [true, false, false, false, true, true, false, false],
        ),
        (
            "BD <= D",
            [true, true, true, true, false, false, true, true],
        ),
        (
            "D >= BD",
            [true, true, true, true, false, false, true, true],
        ),
        (
            "BD == F",
            [false, false, true, false, false, false, true, true],
        ),
        (
            "BD > F",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BI == D",
            [false, false, true, true, false, false, true, false],
        ),
        (
            "BI > D",
            [false, false, false, false, false, true, false, false],
        ),
        (
            "BI < F",
            [true, true, false, true, false, false, false, true],
        ),
        (
            "BI == L",
            [true, true, true, false, false, true, true, false],
        ),
        (
            "BI > L",
            [false, false, false, true, true, false, false, true],
        ),
        (
            "L < BI",
            [false, false, false, true, true, false, false, true],
        ),
        (
            "BD == L",
            [false, false, true, false, false, false, true, false],
        ),
        ("BD >= L", [true, true, true, true, true, false, true, true]),
        (
            "BD == C",
            [false, false, true, false, false, false, true, false],
        ),
        (
            "BI == C",
            [false, false, true, false, false, false, true, false],
        ),
        (
            "C <= BD",
            [false, false, true, true, true, false, true, false],
        ),
        (
            "BD == BI",
            [false, false, true, true, false, false, true, false],
        ),
        (
            "BD < BI",
            [false, false, false, false, false, true, false, false],
        ),
        (
            "BI >= BD",
            [false, false, true, true, false, true, true, false],
        ),
        ("BI == BI", [true, true, true, true, true, true, true, true]),
        (
            "BD == 0.1",
            [false, true, false, false, false, false, false, false],
        ),
        (
            "BD == 0.1f",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BD > 1e20",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BD == 1e20",
            [false, false, false, true, true, false, false, false],
        ),
        (
            "BD == 97",
            [false, false, false, false, false, false, true, false],
        ),
        (
            "BD == (byte) 97",
            [false, false, false, false, false, false, true, false],
        ),
        (
            "BD == (short) 97",
            [false, false, false, false, false, false, true, false],
        ),
        (
            "BD == 'a'",
            [false, false, false, false, false, false, true, false],
        ),
        (
            "BI == null",
            [false, false, true, false, false, false, false, false],
        ),
        (
            "BD != null",
            [true, true, false, true, true, true, true, true],
        ),
        (
            "BD == S",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BD == Z",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BD == I + 1",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BD == NULL_DOUBLE",
            [false, false, true, false, false, false, false, false],
        ),
        (
            "BD < NULL_INT",
            [false, false, false, false, false, false, false, false],
        ),
        (
            "BD > NULL_INT",
            [true, true, false, true, true, true, true, true],
        ),
        (
            "BD < Double.NaN",
            [false, false, false, false, false, false, false, false],
        ),
    ] {
        table.assert(formula, flags(expected));
    }
}

/// Seen on Java 42.9: `BD < S` fails with `ClassCastException` where neither is null, and
/// an ordering against `null` does not compile ("reference to less is ambiguous").
#[test]
fn big_numbers_refuse_what_java_refuses() {
    let table = big_numbers();
    let failure = table.evaluate("BD < S").expect_err("compareTo of a String");
    assert!(
        failure.message.contains("ClassCastException"),
        "{}",
        failure.message
    );
    for formula in ["BI < null", "null >= BD"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula}"
        );
    }
    table.assert(
        "(BD > 0) ? 1 : 2",
        ints([
            Some(2),
            Some(1),
            Some(2),
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
        ]),
    );
}
