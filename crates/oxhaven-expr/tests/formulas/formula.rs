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
