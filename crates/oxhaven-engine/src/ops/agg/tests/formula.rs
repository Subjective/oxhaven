//! Formula aggregation in both forms: the formula with a parameter token
//! (`AggSpec::Formula`) and the formula over several columns (`Aggregation::Formula`).
//!
//! What Java answers is in `fixtures/formula_static.json` and
//! `fixtures/formula_ticking.json`, which `fixtures.rs` replays; the expectations here
//! follow those recordings, `FormulaChunkedOperator` and
//! `FormulaMultiColumnChunkedOperator`, and cover what the fixtures cannot hold:
//! `preserve_empty`, shifts, a formula that fails while the table ticks, and what is
//! refused by name.

use super::super::{aggregate as aggregate_without_compiler, replace_formula_tokens};
use super::incremental::{blink_table, doubles, int, kid, kid_values, three_rows, Fixture};
use super::*;
use crate::graph::SteppedUpdateGraph;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_testkit::TableChange;

fn each(formula: &str, pairs: &[&str]) -> Aggregation {
    agg(
        AggSpec::Formula {
            formula: formula.to_string(),
            param_token: "each".to_string(),
        },
        pairs,
    )
}

fn formula(column: &str, expression: &str) -> Aggregation {
    Aggregation::formula(column, expression)
}

fn invalid(result: Result<Arc<dyn Table>, AggregateError>) -> bool {
    matches!(result, Err(AggregateError::InvalidFormula(_)))
}

#[test]
fn a_formula_sees_each_group_as_a_vector_and_its_key_as_a_value() {
    let table = static_table(&three_rows());
    let result = agg_by(
        &table,
        &[
            each("sum(each)", &["SI = I", "SD = D"]),
            formula("M", "sum(I) + count(D)"),
            formula("KL", "K + len(I)"),
            formula("V", "D"),
        ],
        &["K"],
    );
    assert_eq!(
        schema(&*result),
        [
            ("K".to_string(), ColumnType::String),
            ("SI".to_string(), ColumnType::Long),
            ("SD".to_string(), ColumnType::Double),
            ("M".to_string(), ColumnType::Long),
            ("KL".to_string(), ColumnType::String),
            ("V".to_string(), ColumnType::vector(ColumnType::Double)),
        ]
    );
    assert_eq!(
        rows(&*result),
        ["a|4|3.0|6|a2|[0.5, 2.5]", "b|2|1.5|3|b1|[1.5]"]
    );
    let whole = agg_by(&table, &[formula("M", "sum(I) / count(D)")], &[]);
    assert_eq!(rows(&*whole), ["2.0"]);
}

#[test]
fn only_a_token_that_stands_alone_is_replaced() {
    let replaced = |formula: &str, token: &str| {
        replace_formula_tokens(formula, token, "Col").expect("a token")
    };
    assert_eq!(replaced("sum(each) + each", "each"), "sum(Col) + Col");
    // A letter or `_` before, or a letter, a digit or `_` after, joins the token to a name.
    assert_eq!(
        replaced("eachy + yeach + _each + each_ + each1", "each"),
        "eachy + yeach + _each + each_ + each1"
    );
    // A digit before does not.
    assert_eq!(replaced("1each", "each"), "1Col");
    assert_eq!(replaced("len(e) + e", "e"), "len(Col) + Col");
    // What was put in is not looked at again.
    assert_eq!(
        replace_formula_tokens("C + C", "C", "C C").unwrap(),
        "C C + C C"
    );
    assert_eq!(replaced("", "each"), "");
    assert!(matches!(
        replace_formula_tokens("sum(each)", "", "Col"),
        Err(AggregateError::InvalidFormula(_))
    ));
}

#[test]
fn an_empty_table_has_no_groups_and_a_preserved_one_is_evaluated_over_empty_vectors() {
    let empty = static_table(&kid(&[]));
    let aggregations = [
        each("sum(each)", &["SI = I"]),
        formula("N", "len(D)"),
        formula("C", "7"),
    ];
    assert_eq!(rows(&*agg_by(&empty, &aggregations, &["K"])), [""; 0]);
    assert_eq!(rows(&*agg_by(&empty, &aggregations, &[])), [""; 0]);
    // Behavior: FormulaChunkedOperator#propagateInitialState, which evaluates the rows of
    // the result, also the one `preserve_empty` keeps without rows.
    let preserved = aggregate(&empty, &aggregations, true, &[]).unwrap();
    assert_eq!(rows(&*preserved), ["null|0|7"]);
}

#[test]
fn what_java_refuses_is_refused() {
    let table = static_table(&three_rows());
    let by_k = |aggregations: &[Aggregation]| aggregate(&table, aggregations, false, &["K"]);
    // The formula with a token reads its one input, and no other column or key.
    assert!(invalid(by_k(&[each("sum(each) + sum(D)", &["I"])])));
    assert!(invalid(by_k(&[each("sum(each) + len(K)", &["I"])])));
    assert!(invalid(by_k(&[each("nope(each)", &["I"])])));
    assert!(invalid(by_k(&[each("sum(each", &["I"])])));
    assert!(invalid(by_k(&[each("sum(I_)", &["I"])])));
    assert_eq!(
        by_k(&[each("sum(each)", &["Nope"])]).err(),
        Some(AggregateError::UnknownColumn("Nope".to_string()))
    );
    assert_eq!(
        by_k(&[each("sum(each)", &["A = I", "A = D"])]).err(),
        Some(AggregateError::DuplicateOutput("A".to_string()))
    );
    // Behavior: AggregationProcessor#validateSelectColumnForFormula
    assert!(invalid(by_k(&[formula("M", "sum(I) + i")])));
    assert!(invalid(by_k(&[formula("M", "sum(I) + k")])));
    assert!(invalid(by_k(&[formula("M", "sum(I_)")])));
    // A formula reads the columns of the table, not the outputs before it.
    assert!(invalid(by_k(&[
        formula("A", "sum(I)"),
        formula("B", "A + 1")
    ])));
    assert_eq!(
        by_k(&[formula("A", "sum(I)"), formula("A", "sum(D)")]).err(),
        Some(AggregateError::DuplicateOutput("A".to_string()))
    );
    // A column that is grouped by is a value, not a vector.
    assert!(invalid(by_k(&[formula("M", "K.size()")])));
    assert!(invalid(aggregate_without_compiler(
        &table,
        &[formula("M", "sum(I)")],
        false,
        &["K"]
    )));
    assert!(invalid(aggregate_without_compiler(
        &table,
        &[each("sum(each)", &["I"])],
        false,
        &["K"]
    )));

    // Behavior: AggregationProcessor.Converter#unsupportedForBlinkTables("Formula")
    let graph = SteppedUpdateGraph::new();
    let (blink, _source) = blink_table(&graph);
    let blink: Arc<dyn Table> = blink;
    assert!(matches!(
        aggregate(&blink, &[each("sum(each)", &["I"])], false, &["K"]),
        Err(AggregateError::BlinkUnsupported(AggSpec::Formula { .. }))
    ));
    assert!(matches!(
        aggregate(&blink, &[formula("M", "sum(I)")], false, &["K"]),
        Err(AggregateError::BlinkUnsupported(AggSpec::Formula { .. }))
    ));
}

#[test]
fn a_formula_with_a_token_before_a_group_that_is_extended_fails_as_in_java() {
    let table = static_table(&three_rows());
    let group = |pairs: &[&str]| agg(AggSpec::Group, pairs);
    // Java: a NullPointerException, recorded as `multi_each_group_multi`.
    for later in [formula("M", "sum(D)"), group(&["GD = D"])] {
        let aggregations = [each("sum(each)", &["E = D"]), group(&["GI = I"]), later];
        assert!(invalid(aggregate(&table, &aggregations, false, &["K"])));
    }
    // Nothing is extended: the later aggregation reads what the group has.
    for later in [
        formula("M", "sum(I)"),
        formula("M", "7"),
        group(&["GI2 = I"]),
    ] {
        let aggregations = [each("sum(each)", &["E = D"]), group(&["GI = I"]), later];
        assert_eq!(rows(&*agg_by(&table, &aggregations, &["K"])).len(), 2);
    }
    // The formula with a token comes after: it keeps its own group operator.
    let aggregations = [
        group(&["GI = I"]),
        formula("M", "sum(D)"),
        each("sum(each)", &["E = D"]),
    ];
    assert_eq!(
        rows(&*agg_by(&table, &aggregations, &["K"])),
        ["a|[1, 3]|3.0|3.0", "b|[2]|1.5|1.5"]
    );
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.by.AggregationProcessor")]
fn a_formula_with_a_token_that_java_moves_to_the_shared_group_is_not_ported() {
    let table = static_table(&three_rows());
    let aggregations = [
        each("sum(each)", &["E = I"]),
        agg(AggSpec::Group, &["I"]),
        formula("M", "sum(D)"),
    ];
    let _ = aggregate(&table, &aggregations, false, &["K"]);
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.by.FormulaChunkedOperator")]
fn a_row_variable_in_a_formula_with_a_token_is_not_ported() {
    let table = static_table(&three_rows());
    let _ = aggregate(&table, &[each("sum(each) + i", &["I"])], false, &["K"]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.FormulaMultiColumnChunkedOperator"
)]
fn a_formula_that_reads_a_key_column_an_output_replaces_is_not_ported() {
    let table = static_table(&three_rows());
    let _ = aggregate(&table, &[formula("K", "K + 1")], false, &["K"]);
}

/// The sum of `I` by its token, a formula of `I` and `D`, one of the key, a constant, and
/// the count: columns 1 to 5 of the result.
fn formulas(by: &'static [&'static str], preserve_empty: bool) -> Fixture {
    Fixture::with(
        &three_rows(),
        &[10, 20, 30],
        vec![
            each("sum(each)", &["E = I"]),
            formula("M", "sum(I) + sum(D)"),
            formula("KK", "isNull(K)"),
            formula("C", "7"),
            count("N"),
        ],
        by,
        preserve_empty,
        !preserve_empty,
    )
}

#[test]
fn rows_that_join_and_leave_a_group_evaluate_its_formulas_again() {
    let fixture = formulas(&["K"], false);
    assert_eq!(fixture.rows(), ["a|4|7.0|false|7|2", "b|2|3.5|false|7|1"]);
    // The constant is reported with its row and never named among the columns.
    let change = TableChange::new().add([15, 40], kid_values(&[("a", 7, 7.5), ("c", 9, 9.5)]));
    assert_eq!(
        fixture.step(change),
        "added [2] removed [] modified [0] columns [1, 2, 3, 5]"
    );
    assert_eq!(
        fixture.rows(),
        [
            "a|11|21.5|false|7|3",
            "b|2|3.5|false|7|1",
            "c|9|18.5|false|7|1"
        ]
    );
    assert_eq!(
        fixture.step(TableChange::new().remove([10, 20])),
        "added [] removed [1] modified [0] columns [1, 2, 3, 5]"
    );
    assert_eq!(
        fixture.rows(),
        ["a|10|20.0|false|7|2", "c|9|18.5|false|7|1"]
    );
}

#[test]
fn a_modified_value_evaluates_the_formulas_that_read_its_column() {
    let fixture = formulas(&["K"], false);
    assert_eq!(
        fixture.step(TableChange::new().modify([30], [(1, int(6))])),
        "added [] removed [] modified [0] columns [1, 2]"
    );
    assert_eq!(fixture.rows()[0], "a|7|10.0|false|7|2");
    let change = TableChange::new().modify([20], [(2, doubles(&[None]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [1] columns [2]"
    );
    assert_eq!(fixture.rows()[1], "b|2|null|false|7|1");
    // A value written again: reported without comparing, as the group aggregation is.
    assert_eq!(
        fixture.step(TableChange::new().modify([20], [(1, int(2))])),
        "added [] removed [] modified [1] columns [1, 2]"
    );
}

#[test]
fn a_shift_evaluates_nothing() {
    let fixture = formulas(&["K"], false);
    let change = TableChange::new().shift(20, 30, 100);
    assert_eq!(fixture.step(change), "none");
    assert_eq!(fixture.rows(), ["a|4|7.0|false|7|2", "b|2|3.5|false|7|1"]);
    // The shifted rows are read where they are now.
    let change = TableChange::new().modify([130], [(1, int(5))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [1, 2]"
    );
    assert_eq!(fixture.rows()[0], "a|6|9.0|false|7|2");
}

#[test]
fn without_keys_the_row_comes_and_goes_and_a_preserved_one_is_evaluated_empty() {
    let fixture = formulas(&[], false);
    assert_eq!(fixture.rows(), ["6|10.5|false|7|3"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([10, 20, 30])),
        "added [] removed [0] modified [] columns []"
    );
    let change = TableChange::new().add([5], kid_values(&[("z", 5, 0.5)]));
    assert_eq!(
        fixture.step(change),
        "added [0] removed [] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["5|5.5|false|7|1"]);

    let preserved = formulas(&[], true);
    assert_eq!(
        preserved.step(TableChange::new().remove([10, 20, 30])),
        "added [] removed [] modified [0] columns [0, 1, 2, 4]"
    );
    assert_eq!(preserved.rows(), ["null|null|false|7|0"]);

    let by_key = formulas(&["K"], true);
    assert_eq!(
        by_key.step(TableChange::new().remove([20])),
        "added [] removed [] modified [1] columns [1, 2, 3, 5]"
    );
    assert_eq!(
        by_key.rows(),
        ["a|4|7.0|false|7|2", "b|null|null|false|7|0"]
    );
}

#[test]
fn a_formula_that_fails_on_a_group_fails_the_result() {
    // A remainder by zero is an `ArithmeticException` in Java.
    let failing = || vec![formula("M", "first(I) % (first(I) - 3)")];
    let fixture = Fixture::with(&three_rows(), &[0, 1, 2], failing(), &["K"], false, false);
    assert_eq!(fixture.rows(), ["a|1", "b|0"]);
    fixture
        .source
        .push(TableChange::new().modify([0], [(2, doubles(&[Some(9.0)]))]));
    fixture.graph.run_cycle();
    assert!(!fixture.result.is_failed());
    fixture
        .source
        .push(TableChange::new().modify([1], [(1, int(3))]));
    fixture.graph.run_cycle();
    assert!(fixture.result.is_failed());

    // The first evaluation fails the request.
    let table = static_table(&kid(&[("a", 3, 0.5)]));
    assert!(matches!(
        aggregate(&table, &failing(), false, &["K"]),
        Err(AggregateError::Failed(_))
    ));
    // A group that leaves the result is not evaluated: its vectors are empty.
    let leaving = vec![formula("M", "7 % len(I)")];
    let fixture = Fixture::new(&three_rows(), &[0, 1, 2], leaving, &["K"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([1])),
        "added [] removed [1] modified [] columns []"
    );
    assert!(!fixture.result.is_failed());
}
