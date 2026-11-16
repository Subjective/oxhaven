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
    for formula in [
        "Dec",
        "Dec + 1",
        "Date == Date",
        "S + Date",
        "Big > 1",
        "Time.getHour()",
        "Dec.toString(1)",
        "Local.toString().length() > 0 ? Local : Local",
        "min(Big, Big)",
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
