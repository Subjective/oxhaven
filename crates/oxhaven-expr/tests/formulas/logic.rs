//! `&& || !`, and what the type checker refuses.

use super::*;
use oxhaven_contracts::expr::{Expr, Literal, RowVariable, UnaryOp};

#[test]
fn and_or_and_not_follow_their_truth_tables() {
    let table = TestTable::new([
        (
            "L",
            booleans([Some(true), Some(true), Some(false), Some(false)]),
        ),
        (
            "R",
            booleans([Some(true), Some(false), Some(true), Some(false)]),
        ),
    ]);
    table.assert("L && R", flags([true, false, false, false]));
    table.assert("L || R", flags([true, true, true, false]));
    table.assert("!L", flags([false, false, true, true]));
    table.assert("L && !R", flags([false, true, false, false]));
    table.assert("!(L || R)", flags([false, false, false, true]));
    // `&&` binds tighter than `||`.
    table.assert("L || R && false", flags([true, true, false, false]));
    table.assert("(L || R) && false", flags([false; 4]));
    table.assert("true && L", flags([true, true, false, false]));
    table.assert("L == R || L", flags([true, true, false, true]));
}

/// `QueryLanguageParser#visit(BinaryExpr)` emits `&&` and `||` as Java's own operators, which
/// do not evaluate the right operand when the left one decides. QLFU `#remainder(int,int)`
/// throws for a zero divisor only when it is called.
#[test]
fn the_right_operand_is_not_evaluated_where_the_left_decides() {
    let table = TestTable::new([
        ("A", ints([Some(6), Some(7), Some(5)])),
        ("B", ints([Some(3), Some(0), Some(2)])),
    ]);
    table.assert("B != 0 && A % B == 0", flags([true, false, false]));
    table.assert("B == 0 || A % B == 0", flags([true, true, false]));
    table.assert(
        "B != 0 && (A % B == 0 || A % B == 1)",
        flags([true, false, true]),
    );
    // With the guard on the right, the remainder is computed for every row.
    assert!(table.evaluate("A % B == 0 && B != 0").is_err());
    assert!(table.evaluate("A % B == 0 || B == 0").is_err());
    // No row reaches the remainder.
    table.assert("false && A % 0 == 0", flags([false; 3]));
    table.assert("true || A % 0 == 0", flags([true; 3]));
}

/// A `Boolean` column is a `java.lang.Boolean` in the code Java generates
/// (`ConditionFilter#insertChunkValues`, `DhFormulaColumn#columnSourceGetMethodReturnType`),
/// and `&&` and `||` unbox their operands, so a null operand that is evaluated throws
/// `NullPointerException`. An operand that is skipped does not.
#[test]
fn a_null_operand_of_and_or_or_fails_only_if_it_is_evaluated() {
    let table = TestTable::new([
        ("L", booleans([Some(false), Some(true)])),
        ("R", booleans([None, Some(true)])),
    ]);
    table.assert("L && R", flags([false, true]));
    assert!(table.evaluate("L || R").is_err());
    assert!(table.evaluate("R && L").is_err());
    assert!(table.evaluate("R || L").is_err());

    let table = TestTable::new([
        ("L", booleans([Some(true), Some(false)])),
        ("R", booleans([None, Some(false)])),
    ]);
    table.assert("L || R", flags([true, false]));
    assert!(table.evaluate("L && R").is_err());

    // The usual guard works, because `||` skips the null.
    table.assert("isNull(R) || R", flags([true, false]));
    table.assert("R != null && R", flags([false, false]));
}

/// QLFU `#not(Boolean)`: null in, null out.
#[test]
fn not_of_a_null_boolean_is_null() {
    let table = TestTable::new([("B", booleans([Some(true), Some(false), None]))]);
    table.assert("!B", booleans([Some(false), Some(true), None]));
    table.assert("!!B", booleans([Some(true), Some(false), None]));
    table.assert("isNull(!B)", flags([false, false, true]));
}

fn checker_table() -> TestTable {
    TestTable::new([
        ("I", ints([Some(1)])),
        ("D", doubles([Some(1.0)])),
        ("B", booleans([Some(true)])),
        ("S", strings([Some("s")])),
        ("T", instants([Some(1)])),
    ])
}

/// Formulas Java rejects: `&&` and `||` are Java's operators and need boolean operands,
/// `QueryLanguageParser#visit(UnaryExpr)` finds no `not` or `negate` overload for these
/// operand types, and `#resolveName` finds no such variable.
#[test]
fn formulas_with_ill_typed_operands_are_type_errors() {
    let table = checker_table();
    for formula in [
        "I && B",
        "B || I",
        "S && S",
        "!I",
        "!S",
        "-B",
        "-S",
        "-T",
        "B + 1",
        "B * B",
        "Missing + 1",
        "Missing_",
        "isNull(Missing)",
        "isNull(I, I)",
        "isNull()",
    ] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula} gave {:?}",
            table.compile_error(formula)
        );
    }
}

#[test]
fn formulas_outside_the_subset_are_reported_as_unsupported() {
    let table = checker_table();
    let mut wrong = Vec::new();
    for (formula, construct) in [
        ("T >= 1L", "ordering comparison of Instant with long"),
        (
            "null",
            "null literal outside ==, !=, ?:, a cast and string concatenation",
        ),
        ("null == null", "comparison of null with null"),
        ("S < null", "null literal as an operand of <"),
        ("B & null", "null literal as an operand of &"),
        ("cbrt(I)", "function cbrt()"),
        (
            "replaceIfNull(I, null)",
            "null literal as an argument of replaceIfNull()",
        ),
        ("BS_ANY", "constant BS_ANY"),
        ("S.split(S)", "method call String.split()"),
        (
            "S.indexOf(S, I)",
            "method call String.indexOf() with 2 arguments",
        ),
        ("(Integer) I", "cast to Integer"),
        ("1.5F", "float literal with an uppercase F suffix 1.5F"),
        ("i", "row variable I"),
        ("ii + 1", "row variable Ii"),
        ("I + k", "row variable K"),
        ("I_", "column array I_"),
    ] {
        let result = Compiler
            .parse(formula)
            .and_then(|expr| Compiler.compile(&expr, &table.definition).map(|_| ()));
        if result != Err(FormulaError::Unsupported(construct.into())) {
            wrong.push(format!("{formula}: {result:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A tree built without the parser gets the same answers.
#[test]
fn expression_trees_outside_the_subset_do_not_compile() {
    let table = checker_table();
    let column = || Box::new(Expr::Column("I".into()));
    let compile = |expr: Expr| match Compiler.compile(&expr, &table.definition) {
        Ok(_) => panic!("{expr:?} compiled"),
        Err(error) => error,
    };
    assert_eq!(
        compile(Expr::MethodCall {
            receiver: Box::new(Expr::Column("S".into())),
            method: "hashCode".into(),
            arguments: vec![],
        }),
        FormulaError::Unsupported("method call String.hashCode()".into())
    );
    assert_eq!(
        compile(Expr::RowVariable(RowVariable::I)),
        FormulaError::Unsupported("row variable I".into())
    );
    assert_eq!(
        compile(Expr::Call {
            function: "cbrt".into(),
            arguments: vec![*column()],
        }),
        FormulaError::Unsupported("function cbrt()".into())
    );
    assert_eq!(
        compile(Expr::ColumnVector("I".into())),
        FormulaError::Unsupported("column array I_".into())
    );
    assert_eq!(
        compile(Expr::ColumnArrayAccess {
            column: "I".into(),
            position: Box::new(Expr::Literal(Literal::Int(0))),
        }),
        FormulaError::Unsupported("column array access I_[...]".into())
    );
    // `QueryLanguageParser#visit(UnaryExpr)` throws for every operator but `!` and `-`.
    assert!(matches!(
        compile(Expr::Unary {
            op: UnaryOp::BitNot,
            operand: column(),
        }),
        FormulaError::Type(_)
    ));
}
