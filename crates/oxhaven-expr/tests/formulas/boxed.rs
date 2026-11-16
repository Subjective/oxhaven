//! `Object` columns and `java.time.DayOfWeek`: the expected values are what Java 42.9
//! answered for the same formulas (task 5.10, probed on the reference server).

use super::*;
use arrow::array::Int8Array;
use oxhaven_contracts::expr::Formula;

fn table() -> TestTable {
    TestTable::new([
        ("I", ints([Some(0), None, Some(2)])),
        ("D", doubles([Some(1.5), None, Some(f64::NAN)])),
        ("S", strings([Some("0"), None, Some("2")])),
        ("Z", booleans([Some(true), None, Some(false)])),
        // 2024-01-01 is a Monday; 1970-01-01 a Thursday.
        (
            "T",
            instants([Some(1_704_067_200_000_000_000), None, Some(0)]),
        ),
        (
            "Date",
            (
                ColumnType::LocalDate,
                Arc::new(arrow::array::Date32Array::from(vec![
                    Some(19_723),
                    Some(3),
                    Some(0),
                ])),
            ),
        ),
    ])
}

fn compiled(table: &TestTable, formula: &str) -> Arc<dyn Formula> {
    let expr = Compiler.parse(formula).expect("parses");
    Compiler
        .compile(&expr, &table.definition)
        .unwrap_or_else(|e| panic!("{formula}: {e}"))
}

fn result(table: &TestTable, formula: &str) -> (ColumnType, ArrayRef) {
    let formula = compiled(table, formula);
    let values = formula.evaluate(table, &table.rows, false).unwrap();
    (formula.result_type().clone(), values)
}

/// Behavior: `QueryLanguageParser#visit(NullLiteralExpr)`; `null` and `true ? null : null`
/// are columns of `java.lang.Object`, all null.
#[test]
fn null_alone_is_an_object_column_of_nulls() {
    let table = table();
    for formula in ["null", "true ? null : null", "(Object) null"] {
        let (column_type, values) = result(&table, formula);
        assert_eq!(column_type, ColumnType::object(None), "{formula}");
        assert_eq!(values.null_count(), 3, "{formula}");
    }
}

/// Seen on Java: `(Object) I` of a null `int` is the boxed -2147483648, which is printed
/// and is no null; `(int)` of it is null again; a null `String` stays null.
#[test]
fn a_cast_to_object_boxes_a_primitive_as_java_holds_it() {
    let table = table();
    let (column_type, values) = result(&table, "(Object) I");
    assert_eq!(column_type, ColumnType::object(Some(ColumnType::Int)));
    assert_eq!(
        values.as_primitive::<arrow::datatypes::Int32Type>(),
        &Int32Array::from(vec![0, i32::MIN, 2])
    );
    let (column_type, values) = result(&table, "(java.lang.Object) S");
    assert_eq!(column_type, ColumnType::object(Some(ColumnType::String)));
    assert_eq!(values.null_count(), 1);
    table.assert("(int) (Object) I", ints([Some(0), None, Some(2)]));
    table.assert(
        "(long) (Object) I",
        longs([Some(0), Some(i32::MIN.into()), Some(2)]),
    );
    table.assert("(String) (Object) S", strings([Some("0"), None, Some("2")]));
    table.assert("(int) null", ints([None, None, None]));
}

/// Behavior: `QueryLanguageFunctionUtils#eq(Object, Object)`: values of two classes are
/// unequal, `Double#equals` makes NaN equal NaN, and two nulls are equal.
#[test]
fn objects_are_equal_as_java_objects_are() {
    let table = table();
    table.assert("(Object) I == 2", flags([false, false, true]));
    table.assert("(Object) I == NULL_INT", flags([false, true, false]));
    table.assert("(Object) S == `2`", flags([false, false, true]));
    table.assert("(Object) S != `2`", flags([true, true, false]));
    table.assert("(Object) I == (Object) S", flags([false, false, false]));
    table.assert("(Object) I == 2L", flags([false, false, false]));
    table.assert("(Object) D == (Object) D", flags([true, true, true]));
    table.assert("D == D", flags([true, true, false]));
    table.assert("(Object) S == null", flags([false, true, false]));
    table.assert("null == (Object) S", flags([false, true, false]));
    table.assert("(Object) Z == true", flags([true, false, false]));
}

/// Behavior: `java.lang.String#valueOf(Object)` in string concatenation.
#[test]
fn an_object_is_concatenated_as_its_text() {
    let table = table();
    table.assert(
        "(Object) I + `s`",
        strings([Some("0s"), Some("-2147483648s"), Some("2s")]),
    );
    table.assert(
        "`s` + (Object) S",
        strings([Some("s0"), Some("snull"), Some("s2")]),
    );
}

/// A branch beside an `Object` is boxed; branches of two classes make a column of two
/// classes, which is refused.
#[test]
fn a_ternary_with_an_object_branch_has_one_class() {
    let table = table();
    let (column_type, values) = result(&table, "I == 0 ? (Object) I : 7");
    assert_eq!(column_type, ColumnType::object(Some(ColumnType::Int)));
    assert_eq!(
        values.as_primitive::<arrow::datatypes::Int32Type>(),
        &Int32Array::from(vec![0, 7, 7])
    );
    let (column_type, _) = result(&table, "I == 0 ? (Object) S : null");
    assert_eq!(column_type, ColumnType::object(Some(ColumnType::String)));
    assert!(matches!(
        table.compile_error("I == 0 ? (Object) I : (Object) S"),
        FormulaError::Unsupported(_)
    ));
    assert!(matches!(
        table.compile_error("(Object) I < 1"),
        FormulaError::Type(_)
    ));
}

/// A value of an `Object` column of another class fails a cast, as Java's
/// `ClassCastException`; nulls pass.
#[test]
fn a_cast_from_an_object_of_another_class_fails() {
    let table = table();
    assert!(table.evaluate("(int) (Object) S").is_err());
    assert!(table.evaluate("(String) (Object) I").is_err());
    table.assert("(String) (Object) null", strings([None, None, None]));
}

/// Behavior: `DateTimeUtils#dayOfWeek(Instant, ZoneId)`, `java.time.LocalDate#getDayOfWeek`;
/// a day is never equal to a number (`eq(Object, Object)`), and is printed by its name.
#[test]
fn a_day_of_week_is_its_number_and_prints_as_its_name() {
    let table = table();
    let (column_type, values) = result(&table, "dayOfWeek(T, 'UTC')");
    assert_eq!(column_type, ColumnType::DayOfWeek);
    assert_eq!(
        values.as_primitive::<arrow::datatypes::Int8Type>(),
        &Int8Array::from(vec![Some(1), None, Some(4)])
    );
    let (column_type, values) = result(&table, "Date.getDayOfWeek()");
    assert_eq!(column_type, ColumnType::DayOfWeek);
    assert_eq!(
        values.as_primitive::<arrow::datatypes::Int8Type>(),
        &Int8Array::from(vec![1, 7, 4])
    );
    table.assert("dayOfWeek(T, 'UTC') == 1", flags([false, false, false]));
    table.assert(
        "dayOfWeek(T, 'UTC') == Date.getDayOfWeek()",
        flags([true, false, true]),
    );
    table.assert(
        "dayOfWeek(T, 'UTC') + `!`",
        strings([Some("MONDAY!"), Some("null!"), Some("THURSDAY!")]),
    );
    assert!(matches!(
        table.compile_error("dayOfWeek(T, 'UTC') < Date.getDayOfWeek()"),
        FormulaError::Unsupported(_)
    ));
}
