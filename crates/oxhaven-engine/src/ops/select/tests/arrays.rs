//! Formulas that read a column at another row, through the five formula operations and
//! `where`, on static tables and under the consistency checker.
//!
//! Expected values and refusals were seen on Java 42.9 unless a test says otherwise.

use super::statics::{flat, keyed};
use super::ticking::{
    attributes, checked, fed, fuzz, graph, last_update, operation, scripted, Operation,
};
use super::*;
use crate::ops::agg::group_by;
use crate::ops::filter::unstructured_filter;
use crate::ops::select::arrays::{constant_array_accesses, Shift};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::update::ModifiedColumnSet;
use oxhaven_testkit::{
    at_least_for, longs, strings, test_seeds, RandomUpdates, Recorder, TableChange,
};

fn shift(column: &str, amount: i64) -> Shift {
    Shift {
        column: column.to_string(),
        amount,
    }
}

/// The shifted columns of `formula`, and the formula that reads them, parsed again.
fn rewritten(formula: &str) -> Option<(Vec<Shift>, Expr)> {
    let expr = Compiler.parse(formula).unwrap();
    constant_array_accesses(formula, &expr).map(|found| (found.shifts, found.expr))
}

/// Behavior: `ShiftedColumnsFactory#getShiftedColumnDefinitions`: an index that is `i` or
/// `ii`, alone or with a whole number added or subtracted, reached through binary
/// operators, parentheses and `?:`, is a shifted column.
#[test]
fn an_index_at_a_fixed_distance_becomes_a_shifted_column() {
    let parsed = |text: &str| Compiler.parse(text).unwrap();
    for (formula, shifts, reads) in [
        ("X_[i - 1]", vec![shift("X", -1)], "__X_Shifted_Minus_1__"),
        ("X_[ii+12]", vec![shift("X", 12)], "__X_Shifted_Plus_12__"),
        ("X_[i]", vec![shift("X", 0)], "__X_Shifted_Plus_0__"),
        (
            "X_[i-1] + Y_[ii + 2] * X_[ i - 1 ]",
            vec![shift("X", -1), shift("Y", 2)],
            "__X_Shifted_Minus_1__ + __Y_Shifted_Plus_2__ * __X_Shifted_Minus_1__",
        ),
        (
            "(X_[i-1] > 5) ? X_[i+1] : -X_[i-1]",
            vec![shift("X", -1), shift("X", 1)],
            "(__X_Shifted_Minus_1__ > 5) ? __X_Shifted_Plus_1__ : -X_[i-1]",
        ),
        (
            "X_[i - 1] > 3 && isNull(X_[i + 1])",
            vec![shift("X", -1)],
            "__X_Shifted_Minus_1__ > 3 && isNull(X_[i + 1])",
        ),
        (
            "X_[Y_[i] + 1] + Y_[i + 1]",
            vec![shift("Y", 1)],
            "X_[Y_[i] + 1] + __Y_Shifted_Plus_1__",
        ),
    ] {
        let (found, expr) = rewritten(formula).unwrap_or_else(|| panic!("{formula}"));
        assert_eq!(found, shifts, "{formula}");
        assert_eq!(expr, parsed(reads), "{formula}");
    }
    // Inside a call, a cast or a unary operator, and with any other index, the column
    // array stays one.
    for formula in [
        "isNull(X_[i - 1])",
        "(int) X_[i - 1]",
        "-X_[i - 1]",
        "!X_[i]",
        "X_[(i - 1)]",
        "X_[i - 1L]",
        "X_[1 + i]",
        "X_[i + -1]",
        "X_[i - 1 + 0]",
        "X_[3]",
        "X_[k - 1]",
        "X_[i * 2]",
        "X_.get(i - 1)",
        "X_.size() + sum(X_)",
        "X + 1",
    ] {
        assert!(rewritten(formula).is_none(), "{formula}");
    }
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.ShiftedColumnsFactory#parseForConstantArrayAccessAttributes"
)]
fn an_offset_in_another_base_is_unported() {
    let _ = rewritten("X_[i - 0x1]");
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnDefinitions"
)]
fn a_column_array_indexed_through_parentheses_is_unported() {
    let _ = rewritten("(X_)[i - 1]");
}

#[test]
fn every_operation_reads_the_row_before_and_the_row_after() {
    // A: 10, 20, 30, 40. B: 1, 2, 3, 4.
    let parent = keyed(&[3, 7, 9, 20]);
    let columns = [
        "P = A_[i - 1]",
        "N = A_[ii + 2]",
        "D = A - A_[i - 1] + B_[i]",
        "T = (A_[i-1] > 15) ? A_[i+1] : -A_[i-1]",
    ];
    for (what, result) in [
        ("update", update(&parent, &columns, &Compiler).unwrap()),
        (
            "update_view",
            update_view(&parent, &columns, &Compiler).unwrap(),
        ),
    ] {
        assert_eq!(
            names(&*result),
            ["Sym", "A", "B", "P", "N", "D", "T"],
            "{what}"
        );
        assert_eq!(values(&*result, "P"), ["null", "10", "20", "30"], "{what}");
        assert_eq!(values(&*result, "N"), ["30", "40", "null", "null"]);
        assert_eq!(values(&*result, "D"), ["null", "12", "13", "14"], "{what}");
        assert_eq!(keys(&*result), [3, 7, 9, 20], "{what}");
        assert!(!result.is_refreshing() && !result.is_flat(), "{what}");
    }
    // What is left of `T` reads a column array, which a lazy formula cannot.
    let result = lazy_update(&parent, &columns[..3], &Compiler).unwrap();
    assert_eq!(names(&*result), ["Sym", "A", "B", "P", "N", "D"]);
    assert_eq!(values(&*result, "D"), ["null", "12", "13", "14"]);

    let result = view(&parent, &columns, &Compiler).unwrap();
    assert_eq!(names(&*result), ["P", "N", "D", "T"]);
    assert_eq!(values(&*result, "D"), ["null", "12", "13", "14"]);
    assert_eq!(values(&*result, "T"), ["null", "-10", "40", "null"]);
    assert_eq!(keys(&*result), [3, 7, 9, 20]);

    let result = select(&flat(4), &columns, &Compiler).unwrap();
    assert_eq!(names(&*result), ["P", "N", "D", "T"]);
    assert_eq!(values(&*result, "P"), ["null", "10", "20", "30"]);
    assert_eq!(values(&*result, "D"), ["null", "12", "13", "14"]);
    assert!(result.is_flat());
}

/// The columns before the first shifted one are computed first and those after it read
/// its result; a later column may be shifted again, and may read an earlier one at a
/// distance.
///
/// Behavior: `AnalyzerContext#applyShiftsAndRemainingColumns`
#[test]
fn columns_around_a_shifted_one_keep_their_order_and_their_values() {
    let parent = keyed(&[3, 7, 9]);
    let columns = [
        "X = A + 1",
        "B",
        "P = X_[i - 1]",
        "Q = P + B",
        "R = Q_[i + 1]",
        "A = R",
    ];
    let result = update(&parent, &columns, &Compiler).unwrap();
    assert_eq!(names(&*result), ["Sym", "A", "B", "X", "P", "Q", "R"]);
    assert_eq!(values(&*result, "X"), ["11", "21", "31"]);
    assert_eq!(values(&*result, "P"), ["null", "11", "21"]);
    assert_eq!(values(&*result, "Q"), ["null", "13", "24"]);
    assert_eq!(values(&*result, "R"), ["13", "24", "null"]);
    assert_eq!(values(&*result, "A"), ["13", "24", "null"]);

    let result = view(&parent, &columns, &Compiler).unwrap();
    assert_eq!(names(&*result), ["X", "B", "P", "Q", "R", "A"]);
    assert_eq!(values(&*result, "R"), ["13", "24", "null"]);

    // A stored result that shares a column with its parent keeps the parent's row keys.
    let result = select(&parent, &["Sym", "P = A_[i - 1]", "Q = P + 1"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["Sym", "P", "Q"]);
    assert_eq!(values(&*result, "Q"), ["null", "11", "21"]);
    assert_eq!(keys(&*result), [3, 7, 9]);
}

/// A shifted column cannot be read by a later column, and the name of a column that does
/// not exist is the formula's error.
#[test]
fn shifted_columns_are_not_part_of_the_result() {
    let parent = keyed(&[3, 7, 9]);
    let error = |columns: &[&str]| match update(&parent, columns, &Compiler) {
        Err(error) => error,
        Ok(table) => panic!("{columns:?} gave columns {:?}", names(&*table)),
    };
    for columns in [
        &["P = A_[i - 1]", "Q = __A_Shifted_Minus_1__"][..],
        &["P = Nope_[i - 1]"],
        &["P = A_[i - 1] + Nope"],
        &["P = Sym_[i - 1] * 2"],
    ] {
        assert!(
            matches!(error(columns), SelectError::Formula { .. }),
            "{columns:?}: {:?}",
            error(columns)
        );
    }
}

/// Java reads the parent's columns by the result's new row keys here: seen on Java 42.9,
/// `empty_table(6).update("X = i * 10").where("X != 20").select("Y = X_[i-1]")` has the 20
/// of the row that `where` dropped.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#getPublishedColumnSources"
)]
fn a_shifted_column_of_a_static_select_that_gets_new_row_keys_is_unported() {
    let _ = select(&keyed(&[3, 7, 9]), &["P = A_[i - 1]"], &Compiler);
}

/// Any other use of a column array is evaluated over the table's rows by the formula.
#[test]
fn other_uses_of_a_column_array_are_evaluated_over_the_tables_rows() {
    let parent = keyed(&[3, 7, 9, 20]);
    let columns = [
        "F = A_[1]",
        "S = sum(A_)",
        "N = A_.size()",
        "G = A_[3 - i]",
        "W = isNull(A_[i - 1])",
        "V = A_.subVector(i, i + 2)",
        "L = A_[4]",
    ];
    let result = update(&parent, &columns, &Compiler).unwrap();
    assert_eq!(values(&*result, "F"), ["20"; 4]);
    assert_eq!(values(&*result, "S"), ["100"; 4]);
    assert_eq!(values(&*result, "N"), ["4"; 4]);
    assert_eq!(values(&*result, "G"), ["40", "30", "20", "10"]);
    assert_eq!(values(&*result, "W"), ["true", "false", "false", "false"]);
    assert_eq!(
        values(&*result, "V"),
        ["[10, 20]", "[20, 30]", "[30, 40]", "[40, ]"]
    );
    assert_eq!(values(&*result, "L"), ["null"; 4]);
    // A stored result with new row keys reads the same rows.
    let result = select(&parent, &["F = A_[1]", "G = A_[3 - i]"], &Compiler).unwrap();
    assert_eq!(values(&*result, "G"), ["40", "30", "20", "10"]);
    assert_eq!(keys(&*result), [0, 1, 2, 3]);
}

/// A shifted column of a column of vectors holds the vector of the other row.
#[test]
fn a_column_of_vectors_can_be_read_at_a_distance() {
    let grouped = group_by(&flat(5), &["Sym"]).unwrap();
    // Sym a: rows 0 and 3, b: rows 1 and 4, c: row 2.
    let result = update(&grouped, &["P = B_[i - 1]", "Q = B_[i]"], &Compiler).unwrap();
    assert_eq!(values(&*result, "P"), ["null", "[1, 4]", "[2, 5]"]);
    assert_eq!(values(&*result, "Q"), ["[1, 4]", "[2, 5]", "[3]"]);
    let column = result.definition().index_of("P").unwrap();
    assert_eq!(
        result.definition().columns()[column].column_type,
        ColumnType::vector(ColumnType::Long)
    );
}

#[test]
fn where_reads_the_rows_around_the_current_one() {
    let parent = keyed(&[3, 7, 9, 20]);
    let kept = |filters: &[&str]| {
        let result = unstructured_filter(&parent, filters, &Compiler).unwrap();
        assert_eq!(names(&*result), ["Sym", "A", "B"], "{filters:?}");
        keys(&*result)
    };
    assert_eq!(kept(&["A_[i - 1] > 10"]), [9, 20]);
    assert_eq!(kept(&["A_[i - 1] = 10"]), [7]);
    assert_eq!(kept(&["A_[i - 1] < A_[i + 1] && B > 1"]), [7, 9]);
    // Every filter sees the neighbours the table has, not those the filters before it
    // kept.
    assert_eq!(kept(&["A > 10", "A_[i - 1] < 30"]), [7, 9]);
    assert_eq!(kept(&["A_[i - 1] > 10 || A_[i + 2] == 30"]), [3, 9, 20]);
    // Any other use is evaluated by the formula.
    assert_eq!(kept(&["A_[1] == A || isNull(A_[i + 1])"]), [7, 20]);
    assert_eq!(kept(&["A_.size() > i + 2"]), [3, 7]);

    for filters in [
        &["Nope_[i - 1] > 10"][..],
        &["A_[i - 1] in 10, 30"],
        &["A_[i - 1] + 1"],
    ] {
        assert!(
            matches!(
                unstructured_filter(&parent, filters, &Compiler),
                Err(crate::ops::filter::FilterError::Invalid(_))
            ),
            "{filters:?}"
        );
    }
    // No row before the first: a null `Boolean` fails the filter.
    let nulls = update(&parent, &["Z = A > 10"], &Compiler).unwrap();
    assert!(matches!(
        unstructured_filter(&nulls, &["Z_[i - 1]"], &Compiler),
        Err(crate::ops::filter::FilterError::Failed(_))
    ));
    assert_eq!(
        keys(&*unstructured_filter(&nulls, &["Z_[1]"], &Compiler).unwrap()),
        [3, 7, 9, 20]
    );
}

// ---------------------------------------------------------------------------------------
// Refreshing tables
// ---------------------------------------------------------------------------------------

#[test]
fn shifted_columns_follow_random_changes() {
    let cases: [(&str, &[&str]); 6] = [
        ("the row before", &["P = A_[i - 1]"]),
        ("the row two after", &["P = A_[i + 2] + B"]),
        ("three rows before", &["X = A + B", "P = X_[i - 3]", "Q = P + 1"]),
        ("two columns at one distance", &["P = A_[i - 2] + B_[i - 2]"]),
        ("a distance and the row itself", &["P = A_[i + 1] - A_[i]"]),
        ("one after another", &["P = A_[i - 1]", "Q = B_[i + 1] + P"]),
    ];
    for (what, columns) in cases {
        fuzz(
            &format!("update, {what}"),
            operation(move |t| update(t, columns, &Compiler).unwrap()),
        );
        fuzz(
            &format!("update_view, {what}"),
            operation(move |t| update_view(t, columns, &Compiler).unwrap()),
        );
    }
    fuzz(
        "view",
        operation(|t| view(t, &["Sym", "P = A_[i - 1]", "Q = P + B"], &Compiler).unwrap()),
    );
    fuzz(
        "select",
        operation(|t| select(t, &["Sym", "P = A_[i + 1]", "Q = P + B"], &Compiler).unwrap()),
    );
    fuzz(
        "lazy_update",
        operation(|t| lazy_update(t, &["P = A_[i - 1] + B"], &Compiler).unwrap()),
    );
}

#[test]
fn where_with_shifted_columns_follows_random_changes() {
    let cases: [(&str, &[&str]); 4] = [
        ("the row before", &["A_[i - 1] > 20"]),
        ("the row after", &["isNull(A_[i + 1]) || A_[i + 1] > A"]),
        ("two filters", &["B > 1", "A_[i - 2] < A"]),
        ("a disjunction", &["A_[i - 1] > 30 || B_[i - 1] < 3"]),
    ];
    for (what, filters) in cases {
        let make = operation(move |t| unstructured_filter(t, filters, &Compiler).unwrap());
        // As `fuzz`, for a result that many changes of the parent do not change.
        let mut updates = 0;
        for seed in test_seeds(0..24) {
            let (stepper, graph) = graph();
            let source = scripted(&graph);
            let parent: Arc<dyn Table> = source.table();
            let result = make(&parent);
            let checker = checked(&result, &parent, &make);
            checker.assert_consistent();
            let mut random = RandomUpdates::new(seed * 37 + 11);
            for cycle in 0..30 {
                source.push(random.next_change(&*parent, 6));
                stepper.run_cycle();
                if let Err(e) = checker.verify() {
                    panic!("where, {what}, seed {seed}, cycle {cycle}: {e}");
                }
            }
            assert!(!result.is_failed(), "{what}");
            updates += checker.updates_seen();
        }
        assert!(
            updates > at_least_for(24 * 2, 0..24),
            "{what}: only {updates} updates"
        );
    }
}

/// Behavior: the listener of `ShiftedColumnOperation#getShiftedColumns`: a row whose
/// neighbour was added, removed or modified is reported as modified in the shifted
/// column, so the formula's column is.
#[test]
fn a_change_to_a_row_reaches_the_rows_that_read_it() {
    let (stepper, graph) = graph();
    let source = scripted(&graph);
    let parent: Arc<dyn Table> = source.table();
    // Rows 0, 1, 5, 6; A 10, 20, 30, 40. Sym 0, A 1, B 2, P 3.
    let make: Operation = operation(|t| update(t, &["P = A_[i - 1]"], &Compiler).unwrap());
    let result = make(&parent);
    let checker = checked(&result, &parent, &make);
    let recorder = Recorder::attach(result.clone());
    let cycle = |change: TableChange| {
        source.push(change);
        stepper.run_cycle();
        checker.assert_consistent();
        last_update(&recorder)
    };
    assert_eq!(values(&*result, "P"), ["null", "10", "20", "30"]);

    // A modified value changes the row after it.
    let seen = cycle(TableChange::new().modify([1], [(1, longs([21]))]));
    assert_eq!(seen.modified, RowSet::from_keys([1, 5]));
    assert_eq!(seen.modified_columns, ModifiedColumnSet::of([1, 3]));
    assert_eq!(values(&*result, "P"), ["null", "10", "21", "30"]);

    // A change to a column the formula does not read changes no other row.
    let seen = cycle(TableChange::new().modify([1], [(2, longs([9]))]));
    assert_eq!(seen.modified, RowSet::from_keys([1]));
    assert_eq!(seen.modified_columns, ModifiedColumnSet::of([2]));

    // An added row is the row before the one after it.
    let seen =
        cycle(TableChange::new().add([3], vec![strings(["e"]), longs([25]), longs([5])]));
    assert_eq!(seen.added, RowSet::from_keys([3]));
    assert_eq!(seen.modified, RowSet::from_keys([5]));
    assert_eq!(seen.modified_columns, ModifiedColumnSet::of([3]));
    assert_eq!(values(&*result, "P"), ["null", "10", "21", "25", "30"]);

    // A removed row leaves the row after it with another neighbour.
    let seen = cycle(TableChange::new().remove([0, 3]));
    assert_eq!(seen.removed, RowSet::from_keys([0, 3]));
    assert_eq!(seen.modified, RowSet::from_keys([1, 5]));
    assert_eq!(values(&*result, "P"), ["null", "21", "30"]);

    // The last row has no row after it; rows that only move keep their neighbours.
    let seen = cycle(TableChange::new().remove([6]));
    assert!(seen.modified.is_empty());
    let seen = cycle(TableChange::new().shift(5, 5, 4));
    assert!(seen.modified.is_empty());
    assert_eq!(keys(&*result), [1, 9]);
    assert_eq!(values(&*result, "P"), ["null", "21"]);
}

/// Java's `update` keeps old values here: seen on Java 42.9 for
/// `update("M = X_[i-1] + X_[i+1]")` and `update("P = X_[i-1] + X_[i-2]")` of a keyed
/// input table, where `update_view` of the same shows the new ones.
#[test]
fn two_different_distances_on_a_refreshing_table_are_unported() {
    let refused = |columns: &'static [&'static str]| {
        let (_stepper, graph) = graph();
        let parent: Arc<dyn Table> = scripted(&graph).table();
        let run = std::panic::AssertUnwindSafe(|| update(&parent, columns, &Compiler).map(drop));
        let message = match std::panic::catch_unwind(run) {
            Err(panic) => *panic.downcast::<String>().expect("a message"),
            Ok(result) => panic!("{columns:?} gave {result:?}"),
        };
        assert!(
            message.starts_with(
                "unported: io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace"
            ),
            "{message}"
        );
    };
    refused(&["P = A_[i - 1] + A_[i + 1]"]);
    refused(&["P = A_[i - 1] + B_[i - 2]"]);
    // A static table has nothing to follow.
    let result = update(&keyed(&[3, 7, 9]), &["P = A_[i - 1] + A_[i + 1]"], &Compiler).unwrap();
    assert_eq!(values(&*result, "P"), ["null", "40", "null"]);
}

/// Behavior: `AbstractFormulaColumn#validateSafeForRefresh`, which sees the formula after
/// the shifted columns have replaced what they can, on a table without the parent's
/// attributes.
#[test]
fn what_is_left_of_a_formula_decides_whether_it_is_safe_to_refresh() {
    let (_stepper, graph) = graph();
    let refused = |parent: &Arc<dyn Table>, column: &str| match update(parent, &[column], &Compiler)
    {
        Ok(_) => false,
        Err(SelectError::UnsafeForRefresh(_)) => true,
        Err(other) => panic!("{column}: {other:?}"),
    };
    let (plain, _source) = fed(&graph, attributes(false, false, false), false);
    let (append_only, _source) = fed(&graph, attributes(true, true, false), false);
    let (blink, _source) = fed(&graph, attributes(false, false, true), false);
    for parent in [&plain, &append_only] {
        assert!(!refused(parent, "X = V_[i - 1]"));
        assert!(!refused(parent, "X = V_[i - 1] + V_[i]"));
        assert!(refused(parent, "X = V_[i - 1] + i"));
        assert!(refused(parent, "X = V_[i - 1] + sum(V_)"));
        assert!(refused(parent, "X = isNull(V_[i - 1])"));
        assert!(refused(parent, "X = V_[1]"));
        assert!(refused(parent, "X = V_.size()"));
    }
    assert!(!refused(&blink, "X = V_[i + 1]"));
    // A filter is not refused.
    for filter in ["V_[1] > 3", "V_[i - 1] > 3 && i > 0"] {
        assert!(unstructured_filter(&plain, &[filter], &Compiler).is_ok());
    }
}

/// Behavior: `AnalyzerContext#applyShiftsAndRemainingColumns`: the result is append-only
/// when the parent is and no column is read ahead, a blink table when the parent is, and
/// never add-only.
#[test]
fn results_with_shifted_columns_keep_what_holds_for_them() {
    let (stepper, graph) = graph();
    let (append_only, source) = fed(&graph, attributes(true, true, false), false);
    let behind = update(&append_only, &["P = V_[i - 1]"], &Compiler).unwrap();
    assert_eq!(behind.attributes(), attributes(false, true, false));
    let ahead = update(&append_only, &["P = V_[i + 1]"], &Compiler).unwrap();
    assert_eq!(ahead.attributes(), attributes(false, false, false));
    let viewed = update_view(&append_only, &["P = V_[i - 1]"], &Compiler).unwrap();
    assert_eq!(viewed.attributes(), attributes(false, true, false));

    let (add_only, _source) = fed(&graph, attributes(true, false, false), false);
    let result = update(&add_only, &["P = V_[i - 1]"], &Compiler).unwrap();
    assert_eq!(result.attributes(), attributes(false, false, false));

    let (blink, _source) = fed(&graph, attributes(false, false, true), false);
    let result = update(&blink, &["P = V_[i + 1]"], &Compiler).unwrap();
    assert_eq!(result.attributes(), attributes(false, false, true));

    // Filters go through a table without attributes.
    let filtered = unstructured_filter(&append_only, &["V_[i - 1] > 0"], &Compiler).unwrap();
    assert_eq!(filtered.attributes(), attributes(false, false, false));

    // Appended rows see the rows before them, and the last row before sees them.
    source.push(&[], &[2, 5], &[1, 2]);
    stepper.run_cycle();
    source.push(&[], &[9], &[3]);
    stepper.run_cycle();
    assert_eq!(values(&*behind, "P"), ["null", "1", "2"]);
    assert_eq!(values(&*ahead, "P"), ["2", "3", "null"]);
    assert_eq!(keys(&*filtered), [5, 9]);
}
