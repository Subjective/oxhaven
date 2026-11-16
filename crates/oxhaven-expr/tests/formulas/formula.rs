//! What a compiled formula reports about itself and how it reads a table.

use super::*;
use oxhaven_contracts::expr::{Expr, Formula, Literal};

fn quotes() -> TestTable {
    TestTable::with_previous([
        (
            "Sym",
            strings([Some("A"), Some("B"), Some("C"), None]),
            strings([Some("A"), Some("B"), Some("C"), None]),
        ),
        (
            "Bid",
            doubles([Some(10.0), Some(20.0), None, Some(40.0)]),
            doubles([Some(9.0), Some(19.0), Some(29.0), Some(39.0)]),
        ),
        (
            "Ask",
            doubles([Some(11.0), Some(22.0), Some(33.0), Some(44.0)]),
            doubles([Some(10.0), Some(20.0), Some(30.0), Some(40.0)]),
        ),
        (
            "Size",
            ints([Some(100), Some(200), Some(300), Some(400)]),
            ints([Some(1), Some(2), Some(3), Some(4)]),
        ),
    ])
}

fn compile(table: &TestTable, formula: &str) -> Arc<dyn Formula> {
    let expr = Compiler.parse(formula).expect("parses");
    Compiler
        .compile(&expr, &table.definition)
        .unwrap_or_else(|e| panic!("{formula} did not compile: {e}"))
}

#[test]
fn a_formula_reports_its_type_and_the_columns_it_reads() {
    let table = quotes();
    let mid = compile(&table, "(Bid + Ask) / 2 + Bid * Size");
    assert_eq!(mid.result_type(), &ColumnType::Double);
    // In order of first use, each once.
    assert_eq!(mid.columns(), ["Bid", "Ask", "Size"]);
    assert!(mid.column_arrays().is_empty());
    assert!(!mid.uses_row_variables());

    let constant = compile(&table, "1 + 2");
    assert_eq!(constant.result_type(), &ColumnType::Int);
    assert!(constant.columns().is_empty());

    for (formula, expected) in [
        ("Sym", ColumnType::String),
        ("Size", ColumnType::Int),
        ("Size + 1L", ColumnType::Long),
        ("Size * 1.5f", ColumnType::Float),
        ("Size / 2", ColumnType::Double),
        ("Bid > Ask", ColumnType::Boolean),
        ("isNull(Sym)", ColumnType::Boolean),
        ("Bid > 15 && Sym != `B`", ColumnType::Boolean),
    ] {
        assert_eq!(
            compile(&table, formula).result_type(),
            &expected,
            "{formula}"
        );
    }
}

#[test]
fn a_formula_evaluates_the_rows_it_is_given_in_row_key_order() {
    let table = quotes();
    let mid = compile(&table, "(Bid + Ask) / 2");
    let all = mid.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(
        render(&all),
        render(&doubles([Some(10.5), Some(21.0), None, Some(42.0)]).1)
    );

    let some = mid
        .evaluate(&table, &RowSet::from_keys([3, 1]), false)
        .unwrap();
    assert_eq!(render(&some), render(&doubles([Some(21.0), Some(42.0)]).1));

    let none = mid.evaluate(&table, &RowSet::empty(), false).unwrap();
    assert_eq!(none.len(), 0);
    assert_eq!(none.data_type(), &DataType::Float64);

    let filter = compile(&table, "Bid > 15 && Sym != `B`");
    let kept = filter.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(render(&kept), render(&flags([false, false, false, true]).1));
    assert_eq!(kept.null_count(), 0);
}

#[test]
fn use_prev_reads_previous_values() {
    let table = quotes();
    let notional = compile(&table, "Bid * Size");
    let current = notional.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(
        render(&current),
        render(&doubles([Some(1000.0), Some(4000.0), None, Some(16000.0)]).1)
    );
    let previous = notional
        .evaluate(&table, &RowSet::from_range(1, 2), true)
        .unwrap();
    assert_eq!(
        render(&previous),
        render(&doubles([Some(38.0), Some(87.0)]).1)
    );
}

#[test]
fn an_assignment_compiles_like_its_expression() {
    let table = quotes();
    let assignment = Compiler.parse_assignment("Mid = (Bid + Ask) / 2").unwrap();
    assert_eq!(assignment.name, "Mid");
    let formula = Compiler
        .compile(&assignment.expr, &table.definition)
        .unwrap();
    let values = formula.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(
        render(&values),
        render(&doubles([Some(10.5), Some(21.0), None, Some(42.0)]).1)
    );

    // A bare name selects the column, with its own type and values.
    let passthrough = Compiler.parse_assignment("Sym").unwrap();
    let formula = Compiler
        .compile(&passthrough.expr, &table.definition)
        .unwrap();
    assert_eq!(formula.result_type(), &ColumnType::String);
    let values = formula.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(
        render(&values),
        render(&strings([Some("A"), Some("B"), Some("C"), None]).1)
    );
}

#[test]
fn a_column_that_cannot_be_read_fails_the_evaluation() {
    let mut table = quotes();
    let formula = compile(&table, "Size + 1");
    let unrelated = compile(&table, "Bid + 1");
    table.columns[3] = Arc::new(FailingColumn(ColumnType::Int));
    let failure = formula.evaluate(&table, &table.rows, false).unwrap_err();
    assert_eq!(failure.message, "disk on fire");
    assert!(formula.evaluate(&table, &table.rows, true).is_err());
    // A formula that does not read the column is unaffected.
    assert!(unrelated.evaluate(&table, &table.rows, false).is_ok());
}

#[test]
fn a_table_without_the_formula_columns_fails_the_evaluation() {
    let formula = compile(&quotes(), "Size + 1");
    let other = TestTable::new([("Volume", ints([Some(1)]))]);
    assert!(formula.evaluate(&other, &other.rows, false).is_err());
}

/// Literals the parser does not produce yet still evaluate when a tree contains them.
#[test]
fn string_and_instant_literals_are_constants() {
    let table = quotes();
    table.assert("`x`", strings([Some("x"); 4]));
    table.assert("1.5", doubles([Some(1.5); 4]));
    table.assert("true", flags([true; 4]));

    let instant = Compiler
        .compile(&Expr::Literal(Literal::Instant(5)), &table.definition)
        .unwrap();
    assert_eq!(instant.result_type(), &ColumnType::Instant);
    let values = instant
        .evaluate(&table, &RowSet::from_range(0, 1), false)
        .unwrap();
    assert_eq!(values.data_type(), &ColumnType::Instant.arrow_type());
    assert_eq!(render(&values), render(&instants([Some(5), Some(5)]).1));

    let null_instant = Compiler
        .compile(
            &Expr::Literal(Literal::Instant(i64::MIN)),
            &table.definition,
        )
        .unwrap();
    let values = null_instant
        .evaluate(&table, &RowSet::from_range(0, 1), false)
        .unwrap();
    assert_eq!(values.data_type(), &ColumnType::Instant.arrow_type());
    assert_eq!(render(&values), render(&instants([None, None]).1));
}

/// Behavior: `DhFormulaColumn#generateFillChunkHelper`: `i` and `ii` are the positions of
/// the rows in the table's row set and `k` their keys.
#[test]
fn row_variables_are_positions_in_the_table_and_row_keys() {
    let mut table = quotes();
    table.rows = Arc::new(RowSet::from_keys([0, 2, 3]));
    let rows = RowSet::from_keys([2, 3]);
    let value = |formula: &str| {
        let formula = compile(&table, formula);
        assert!(formula.uses_row_variables());
        let values = formula.evaluate(&table, &rows, false).unwrap();
        (formula.result_type().clone(), render(&values))
    };
    let int = |values| (ColumnType::Int, render(&ints(values).1));
    let long = |values| (ColumnType::Long, render(&longs(values).1));
    assert_eq!(value("i"), int([Some(1), Some(2)]));
    assert_eq!(value("ii"), long([Some(1), Some(2)]));
    assert_eq!(value("k"), long([Some(2), Some(3)]));
    assert_eq!(value("(int) i + Size"), int([Some(301), Some(402)]));
    assert_eq!(value("k * 10 + ii"), long([Some(21), Some(32)]));
    // The right operand sees the rows the left one leaves.
    assert_eq!(
        value("i > 1 && k == 3"),
        (ColumnType::Boolean, render(&flags([false, true]).1))
    );
    assert_eq!(
        value("k < 3"),
        (ColumnType::Boolean, render(&flags([true, false]).1))
    );
    assert!(!compile(&table, "Size + 1").uses_row_variables());

    let outside = compile(&table, "i").evaluate(&table, &RowSet::from_keys([1]), false);
    assert!(outside.is_err(), "row 1 is not in the table");
}

/// Behavior: `QueryLanguageParser#visit(FieldAccessExpr)` on the boxed numeric classes of
/// `java.lang`: each field has its declared type and value, and a value Deephaven reserves
/// for null is null (`FC/grid-function.jsonl: java.27`).
#[test]
fn fields_of_the_boxed_numeric_classes_are_constants() {
    let table = TestTable::new([("X", ints([Some(1)]))]);
    table.assert("Byte.MIN_VALUE", bytes([None]));
    table.assert("Byte.MAX_VALUE", bytes([Some(i8::MAX)]));
    table.assert("Short.MIN_VALUE", shorts([None]));
    table.assert("Short.MAX_VALUE", shorts([Some(i16::MAX)]));
    table.assert("Integer.MIN_VALUE", ints([None]));
    table.assert("Integer.MAX_VALUE", ints([Some(i32::MAX)]));
    table.assert("Long.MIN_VALUE", longs([None]));
    table.assert("Long.MAX_VALUE", longs([Some(i64::MAX)]));
    for (class, size) in [
        ("Byte", 1),
        ("Short", 2),
        ("Integer", 4),
        ("Long", 8),
        ("Float", 4),
        ("Double", 8),
    ] {
        table.assert(&format!("{class}.BYTES"), ints([Some(size)]));
        table.assert(&format!("{class}.SIZE"), ints([Some(8 * size)]));
    }

    table.assert("Float.NaN", floats([Some(f32::NAN)]));
    table.assert("Float.POSITIVE_INFINITY", floats([Some(f32::INFINITY)]));
    table.assert("Float.NEGATIVE_INFINITY", floats([Some(f32::NEG_INFINITY)]));
    table.assert("Float.MAX_VALUE", floats([Some(3.4028235e38)]));
    table.assert("Float.MIN_NORMAL", floats([Some(1.1754944e-38)]));
    table.assert("Float.MIN_VALUE", floats([Some(1.4e-45)]));
    table.assert("Float.MAX_EXPONENT", ints([Some(127)]));
    table.assert("Float.MIN_EXPONENT", ints([Some(-126)]));
    table.assert("Double.NaN", doubles([Some(f64::NAN)]));
    table.assert("Double.POSITIVE_INFINITY", doubles([Some(f64::INFINITY)]));
    table.assert(
        "Double.NEGATIVE_INFINITY",
        doubles([Some(f64::NEG_INFINITY)]),
    );
    table.assert("Double.MAX_VALUE", doubles([Some(1.7976931348623157e308)]));
    table.assert(
        "Double.MIN_NORMAL",
        doubles([Some(2.2250738585072014e-308)]),
    );
    table.assert("Double.MIN_VALUE", doubles([Some(4.9e-324)]));
    table.assert("Double.MAX_EXPONENT", ints([Some(1023)]));
    table.assert("Double.MIN_EXPONENT", ints([Some(-1022)]));

    // They are operands like any other value: `QueryLanguageFunctionUtils#negate(double)`
    // of `MAX_VALUE` is the null value, and `#plus(int, int)` promotes a `byte`.
    table.assert("-Double.MAX_VALUE", doubles([None]));
    table.assert("-Float.MAX_VALUE", floats([None]));
    table.assert("Byte.MAX_VALUE + X", ints([Some(128)]));
    table.assert("Long.MAX_VALUE - X", longs([Some(i64::MAX - 1)]));
}

/// Behavior: `QueryLanguageFunctionUtils#eq(double, double)`, which compares with Java's
/// `==` once neither side is null, so no row equals `Double.NaN`; the recorded
/// `filter_structured.double_equal_nan` keeps no row of a table with a NaN.
#[test]
fn a_column_compared_with_a_non_finite_field() {
    let table = TestTable::new([
        (
            "D",
            doubles([
                Some(1.5),
                Some(f64::NAN),
                None,
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
            ]),
        ),
        (
            "F",
            floats([
                Some(1.5),
                Some(f32::NAN),
                None,
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
            ]),
        ),
    ]);
    table.assert("D == Double.NaN", flags([false; 5]));
    table.assert("D != Double.NaN", flags([true; 5]));
    table.assert("F == Float.NaN", flags([false; 5]));
    table.assert(
        "D == Double.POSITIVE_INFINITY",
        flags([false, false, false, true, false]),
    );
    table.assert(
        "D == Double.NEGATIVE_INFINITY",
        flags([false, false, false, false, true]),
    );
    table.assert(
        "F == Float.POSITIVE_INFINITY",
        flags([false, false, false, true, false]),
    );
    table.assert("isNaN(Double.NaN)", flags([true; 5]));
}

/// Behavior: `QueryLanguageParser#resolveName` finds a column before a class, and the
/// column's type has no such field; `#visit(FieldAccessExpr)` fails for a field the class
/// does not have. Neither is read here, and no other field access is.
#[test]
fn other_field_accesses_stay_unsupported() {
    let table = TestTable::new([("Double", doubles([Some(1.0)])), ("X", ints([Some(1)]))]);
    for (formula, construct) in [
        ("Double.NaN", "field access .NaN"),
        ("Float.FOO", "field access .FOO"),
        ("Integer.TYPE", "field access .TYPE"),
        ("Double.PRECISION", "field access .PRECISION"),
        ("Math.TAU", "field access .TAU"),
        ("X.y.z", "field access .z"),
        ("java.util.Calendar.FEBRUARY", "field access .FEBRUARY"),
        ("X.MAX_VALUE", "field access .MAX_VALUE"),
        ("(Long).MAX_VALUE", "field access .MAX_VALUE"),
    ] {
        assert_eq!(
            table.compile_error(formula),
            FormulaError::Unsupported(construct.into()),
            "{formula}"
        );
    }
}
