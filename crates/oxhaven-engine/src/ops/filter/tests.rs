//! The `where` operator: what each kind of filter selects, and how the result follows a
//! ticking parent.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::append_only_input_table;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use oxhaven_column::sentinel::{NULL_DOUBLE, NULL_INT};
use oxhaven_contracts::expr::{ColumnAssignment, Expr, Formula, FormulaError};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    assert_tables_equal, at_least_for, definition, longs, strings, test_seeds, ConsistencyChecker,
    RandomUpdates, Recorder, ReferenceTable, ScriptedTable, TableChange, TableSnapshot,
};
use std::sync::atomic::{AtomicU64, Ordering};

// ---------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------

/// A static table whose row at position `p` has key `10 * p`.
pub(super) fn static_table(columns: Vec<(&str, ColumnType, ArrayRef)>) -> Arc<dyn Table> {
    let size = columns.first().map_or(0, |(_, _, values)| values.len());
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    ReferenceTable::new_static(
        definition(&names),
        RowSet::from_keys((0..size as i64).map(|position| position * 10)),
        columns.into_iter().map(|(_, _, values)| values).collect(),
    )
}

/// The positions, in the source, of the rows `filters` keep.
fn kept(table: &Arc<dyn Table>, filters: &[&str]) -> Vec<i64> {
    let result = unstructured_filter(table, filters, &Compiler)
        .unwrap_or_else(|e| panic!("{filters:?}: {e}"));
    assert!(result.row_set().is_subset_of(&table.row_set()));
    result.row_set().iter().map(|key| key / 10).collect()
}

fn kept_by(table: &Arc<dyn Table>, conditions: &[Condition]) -> Vec<i64> {
    let result =
        filter(table, conditions, &Compiler).unwrap_or_else(|e| panic!("{conditions:?}: {e}"));
    result.row_set().iter().map(|key| key / 10).collect()
}

fn refused(table: &Arc<dyn Table>, filters: &[&str]) -> FilterError {
    match unstructured_filter(table, filters, &Compiler) {
        Ok(_) => panic!("{filters:?} was accepted"),
        Err(e) => e,
    }
}

fn is_invalid(error: &FilterError) -> bool {
    matches!(error, FilterError::Invalid(_))
}

fn keys_of(table: &dyn Table) -> Vec<RowKey> {
    table.row_set().iter().collect()
}

/// Rows: 1.0, -0.0, 0.0, NaN, null, 5.0, the null sentinel as a value, +Infinity, -Infinity.
/// `Z` is 0.0 in every row.
fn doubles_table() -> Arc<dyn Table> {
    let d = Float64Array::from(vec![
        Some(1.0),
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        None,
        Some(5.0),
        Some(NULL_DOUBLE),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ]);
    let z = Float64Array::from(vec![0.0; 9]);
    static_table(vec![
        ("D", ColumnType::Double, Arc::new(d)),
        ("Z", ColumnType::Double, Arc::new(z)),
    ])
}

/// Rows: 5, null, -3, 0, the null sentinel as a value, 7.
fn ints_table() -> Arc<dyn Table> {
    let i = Int32Array::from(vec![
        Some(5),
        None,
        Some(-3),
        Some(0),
        Some(NULL_INT),
        Some(7),
    ]);
    static_table(vec![("I", ColumnType::Int, Arc::new(i))])
}

/// Rows of `S`: "a", "B", null, "", "ab", "b". Rows of `B`: true, false, null, true, false, null.
fn strings_table() -> Arc<dyn Table> {
    let s = StringArray::from(vec![
        Some("a"),
        Some("B"),
        None,
        Some(""),
        Some("ab"),
        Some("b"),
    ]);
    let b = BooleanArray::from(vec![
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ]);
    let c = UInt16Array::from(vec![Some(97), Some(98), None, Some(99), Some(65), Some(97)]);
    static_table(vec![
        ("S", ColumnType::String, Arc::new(s)),
        ("B", ColumnType::Boolean, Arc::new(b)),
        ("C", ColumnType::Char, Arc::new(c)),
    ])
}

// ---------------------------------------------------------------------------------------
// The three ways of comparing
// ---------------------------------------------------------------------------------------

#[test]
fn a_match_filter_follows_ieee_754_and_null_equals_null() {
    let t = doubles_table();
    assert_eq!(kept(&t, &["D == 0"]), [1, 2]);
    assert_eq!(kept(&t, &["D = -0.0"]), [1, 2]);
    assert_eq!(kept(&t, &["D != 0"]), [0, 3, 4, 5, 6, 7, 8]);
    // NaN equals nothing, itself included, so `!=` keeps every row.
    assert_eq!(kept(&t, &["D == NaN"]), [] as [i64; 0]);
    assert_eq!(kept(&t, &["D != NaN"]), [0, 1, 2, 3, 4, 5, 6, 7, 8]);
    // A null literal matches null cells, and a cell holding the null sentinel is null.
    assert_eq!(kept(&t, &["D == null"]), [4, 6]);
    assert_eq!(kept(&t, &["D != null"]), [0, 1, 2, 3, 5, 7, 8]);
    assert_eq!(kept(&t, &["D == NULL_DOUBLE"]), [4, 6]);
    assert_eq!(kept(&t, &["D == Infinity"]), [7]);
    assert_eq!(kept(&t, &["5 == D"]), [5]);
}

#[test]
fn an_in_list_matches_nan_with_nan() {
    let t = doubles_table();
    assert_eq!(kept(&t, &["D in NaN"]), [3]);
    assert_eq!(kept(&t, &["D in 0"]), [1, 2]);
    assert_eq!(kept(&t, &["D in -0.0, 5"]), [1, 2, 5]);
    assert_eq!(kept(&t, &["D not in NaN, null, 1"]), [1, 2, 5, 7, 8]);
    assert_eq!(kept(&t, &["D in null"]), [4, 6]);
    // No values: nothing matches, and everything matches when inverted.
    assert_eq!(kept(&t, &["D in  "]), [] as [i64; 0]);
    assert_eq!(kept(&t, &["D not in  "]), [0, 1, 2, 3, 4, 5, 6, 7, 8]);
}

#[test]
fn a_range_filter_orders_null_lowest_and_keeps_nan_out_of_greater_than() {
    let t = doubles_table();
    assert_eq!(kept(&t, &["D > 0"]), [0, 5, 7]);
    assert_eq!(kept(&t, &["D >= 0"]), [0, 1, 2, 5, 7]);
    // Null is below everything, so `<` and `<=` keep it; the two zeros are equal.
    assert_eq!(kept(&t, &["D < 0"]), [4, 6, 8]);
    assert_eq!(kept(&t, &["D <= 0"]), [1, 2, 4, 6, 8]);
    assert_eq!(kept(&t, &["D <= -0.0"]), [1, 2, 4, 6, 8]);
    // A literal on the left mirrors the operator.
    assert_eq!(kept(&t, &["0 < D"]), [0, 5, 7]);
    assert_eq!(kept(&t, &["0 >= D"]), [1, 2, 4, 6, 8]);
    // NaN is above everything, but `>` and `>=` never keep it.
    assert_eq!(kept(&t, &["D < NaN"]), [0, 1, 2, 4, 5, 6, 7, 8]);
    assert_eq!(kept(&t, &["D <= NaN"]), [0, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(kept(&t, &["D > NaN"]), [] as [i64; 0]);
    assert_eq!(kept(&t, &["D >= NaN"]), [] as [i64; 0]);
    assert_eq!(kept(&t, &["D > null"]), [0, 1, 2, 5, 7, 8]);
    assert_eq!(kept(&t, &["D >= null"]), [0, 1, 2, 4, 5, 6, 7, 8]);
    assert_eq!(kept(&t, &["D < null"]), [] as [i64; 0]);
    assert_eq!(kept(&t, &["D <= null"]), [4, 6]);
}

#[test]
fn a_comparison_of_two_columns_is_a_formula_and_compares_as_formulas_do() {
    let t = doubles_table();
    // The formula `<` orders -0.0 below 0.0 and null below everything; the range filter
    // `D < 0` above keeps neither zero.
    assert_eq!(kept(&t, &["D < Z"]), [1, 4, 6, 8]);
    assert_eq!(kept(&t, &["D < 0"]), [4, 6, 8]);
    // In a formula null equals null and NaN does not equal NaN.
    assert_eq!(kept(&t, &["D == D"]), [0, 1, 2, 4, 5, 6, 7, 8]);
    assert_eq!(kept(&t, &["D == Z"]), [1, 2]);
}

#[test]
fn float_columns_compare_like_double_columns() {
    let f = Float32Array::from(vec![Some(1.5), Some(-0.0), None, Some(f32::NAN), Some(0.1)]);
    let t = static_table(vec![("F", ColumnType::Float, Arc::new(f))]);
    assert_eq!(kept(&t, &["F == 0"]), [1]);
    // The literal is read as a float, so it equals the float cell.
    assert_eq!(kept(&t, &["F == 0.1"]), [4]);
    assert_eq!(kept(&t, &["F in NaN, 1.5f"]), [0, 3]);
    assert_eq!(kept(&t, &["F > 0"]), [0, 4]);
    assert_eq!(kept(&t, &["F <= 0"]), [1, 2]);
    assert_eq!(kept(&t, &["F != NaN"]), [0, 1, 2, 3, 4]);
}

/// Every expectation was observed on Deephaven 42.9 with the same rows.
#[test]
fn hexadecimal_float_literals_are_read_as_java_reads_them() {
    let p = Int32Array::from((0..6).collect::<Vec<i32>>());
    let d = Float64Array::from(vec![8.0, -3.0, 0.5, 1.0, 5e-324, 4.9e-324 * 3.0]);
    let f = Float32Array::from(vec![8.0, -3.0, 0.5, 1.0, 1.000_000_1, 1.4e-45]);
    let t = static_table(vec![
        ("P", ColumnType::Int, Arc::new(p)),
        ("D", ColumnType::Double, Arc::new(d)),
        ("F", ColumnType::Float, Arc::new(f)),
    ]);
    assert_eq!(kept(&t, &["D in 0x1p3"]), [0]);
    assert_eq!(kept(&t, &["D in -0x1.8p1, 0x.8p0"]), [1, 2]);
    assert_eq!(kept(&t, &["D == 0X1P3"]), [0]);
    assert_eq!(kept(&t, &["D in 0x1p3f"]), [0]);
    assert_eq!(kept(&t, &["D in 0x1p0d"]), [3]);
    assert_eq!(kept(&t, &["D in 0x1p-1074"]), [4]);
    assert_eq!(kept(&t, &["D in 0x1.8p-1073"]), [5]);
    assert_eq!(kept(&t, &["D > 0x1p1"]), [0]);
    assert_eq!(kept(&t, &["D <= 0x1p-1"]), [1, 2, 4, 5]);
    assert_eq!(kept(&t, &["F in 0x1p3"]), [0]);
    // a float literal is rounded once, to a float's bits, ties to even
    assert_eq!(kept(&t, &["F in 0x1.000001p0"]), [3]);
    assert_eq!(kept(&t, &["F in 0x1.0000010000001p0"]), [4]);
    assert_eq!(kept(&t, &["F in 0x1p-149"]), [5]);
    assert_eq!(kept(&t, &["F > 0x1p1"]), [0]);
    assert_eq!(kept(&t, &["F < 0x1p-1"]), [1, 5]);
    for bad in ["D in 0x1p", "D in 0x1p0L", "F in 0xgp1"] {
        assert!(is_invalid(&refused(&t, &[bad])), "{bad}");
    }
    let found = |text: &str| {
        let search = Condition::Search {
            search_string: text.to_string(),
            columns: vec!["P".into(), "D".into(), "F".into()],
        };
        kept_by(&t, &[search])
    };
    assert_eq!(found("0x1p3"), [0]);
    assert_eq!(found("-0x1.8p1"), [1]);
    assert_eq!(found("0x1p-1074"), [4]);
    assert_eq!(found("0x1.000001p0"), [3]);
    // No column takes the text, and `FilterFactory#onSearch` then keeps no row.
    assert_eq!(found("0x1p"), [0i64; 0]);
}

/// Every expectation was observed on Deephaven 42.9 with the same rows.
#[test]
fn integer_and_big_number_literals_read_every_decimal_digit_java_reads() {
    let p = Int32Array::from(vec![0, 1, 2, 3]);
    let i = Int32Array::from(vec![12, -12, 5, 0]);
    let l = Int64Array::from(vec![12, -12, 5, 0]);
    let t = static_table(vec![
        ("P", ColumnType::Int, Arc::new(p)),
        ("I", ColumnType::Int, Arc::new(i)),
        ("L", ColumnType::Long, Arc::new(l)),
    ]);
    assert_eq!(kept(&t, &["I in \u{661}\u{662}"]), [0]);
    assert_eq!(kept(&t, &["I in -\u{661}\u{662}"]), [1]);
    assert_eq!(kept(&t, &["I in +\u{ff11}\u{ff12}"]), [0]);
    assert_eq!(kept(&t, &["L in \u{661}\u{662}, \u{665}"]), [0, 2]);
    // a digit outside the Basic Multilingual Plane, a superscript and a Roman numeral are
    // not decimal digits
    for bad in [
        "I in \u{1d7cf}\u{1d7d0}",
        "I in \u{b2}",
        "I in \u{2167}",
        // Java compiles these as formulas, which fail
        "I == \u{966}",
        "I > \u{665}",
    ] {
        refused(&t, &[bad]);
    }
    let found = |text: &str| {
        let search = Condition::Search {
            search_string: text.to_string(),
            columns: vec!["P".into(), "I".into(), "L".into()],
        };
        kept_by(&t, &[search])
    };
    assert_eq!(found("\u{661}\u{662}"), [0]);
    assert_eq!(found("-\u{661}\u{662}"), [1]);
    assert_eq!(found("\u{665}"), [2]);
}

#[test]
fn integer_filters_treat_the_sentinel_as_null() {
    let t = ints_table();
    assert_eq!(kept(&t, &["I == 5"]), [0]);
    assert_eq!(kept(&t, &["I != 5"]), [1, 2, 3, 4, 5]);
    assert_eq!(kept(&t, &["I == null"]), [1, 4]);
    // A literal equal to the sentinel is the null literal.
    assert_eq!(kept(&t, &["I == -2147483648"]), [1, 4]);
    assert_eq!(kept(&t, &["I in 0, 7, NULL_INT"]), [1, 3, 4, 5]);
    assert_eq!(kept(&t, &["I not in 0, 7"]), [0, 1, 2, 4]);
    assert_eq!(kept(&t, &["I > 0"]), [0, 5]);
    assert_eq!(kept(&t, &["I >= 0"]), [0, 3, 5]);
    assert_eq!(kept(&t, &["I < 0"]), [1, 2, 4]);
    assert_eq!(kept(&t, &["I <= -3"]), [1, 2, 4]);
    assert_eq!(kept(&t, &["I > null"]), [0, 2, 3, 5]);
    // Several filters are a conjunction.
    assert_eq!(kept(&t, &["I > -5", "I < 6"]), [0, 2, 3]);
    assert_eq!(kept(&t, &["I > 5", "I < 5"]), [] as [i64; 0]);
    // A value that is not an `int` makes the comparison a formula.
    assert_eq!(kept(&t, &["I == 5.0"]), [0]);
    assert_eq!(kept(&t, &["I < 0.5"]), [1, 2, 3, 4]);
    // So does a string, and an `int` never equals one.
    assert_eq!(kept(&t, &["I == `x`"]), [] as [i64; 0]);
    assert_eq!(kept(&t, &["I != `x`"]), [0, 1, 2, 3, 4, 5]);
}

#[test]
fn string_boolean_and_char_filters() {
    let t = strings_table();
    assert_eq!(kept(&t, &["S == `a`"]), [0]);
    assert_eq!(kept(&t, &["S = \"a\""]), [0]);
    assert_eq!(kept(&t, &["S != `a`"]), [1, 2, 3, 4, 5]);
    assert_eq!(kept(&t, &["S == null"]), [2]);
    assert_eq!(kept(&t, &["S == ``"]), [3]);
    assert_eq!(kept(&t, &["S in `a`, \"b\", 'B'"]), [0, 1, 5]);
    assert_eq!(kept(&t, &["S not in `a`, null"]), [1, 3, 4, 5]);
    // Strings order by UTF-16 code unit, upper case first, with null lowest.
    assert_eq!(kept(&t, &["S < `a`"]), [1, 2, 3]);
    assert_eq!(kept(&t, &["S >= `ab`"]), [4, 5]);
    assert_eq!(kept(&t, &["S icase in `A`, `b`"]), [0, 1, 5]);
    assert_eq!(kept(&t, &["S icase not in `A`, `b`"]), [2, 3, 4]);
    // With four values or more a null value is accepted and matches null.
    assert_eq!(kept(&t, &["S icase in `A`, `x`, `y`, null"]), [0, 2]);

    assert_eq!(kept(&t, &["B == true"]), [0, 3]);
    assert_eq!(kept(&t, &["B != TRUE"]), [1, 2, 4, 5]);
    assert_eq!(kept(&t, &["B == null"]), [2, 5]);
    assert_eq!(kept(&t, &["B in false, null"]), [1, 2, 4, 5]);
    assert_eq!(kept(&t, &["B < true"]), [1, 2, 4, 5]);

    assert_eq!(kept(&t, &["C == 'a'"]), [0, 5]);
    assert_eq!(kept(&t, &["C in 'A', \"c\", null"]), [2, 3, 4]);
    assert_eq!(kept(&t, &["C > 'a'"]), [1, 3]);
    assert_eq!(kept(&t, &["C <= 'A'"]), [2, 4]);
}

#[test]
fn a_case_insensitive_match_of_up_to_three_values_fails_on_a_null_value() {
    let t = strings_table();
    let error = refused(&t, &["S icase in `zz`, null"]);
    assert!(
        matches!(&error, FilterError::Failed(f) if f.message.contains("NullPointerException")),
        "{error}"
    );
    // The null value is reached only by rows no earlier value matched; here every row
    // matches the first value.
    let all_a = static_table(vec![("S", ColumnType::String, strings(["a", "A"]))]);
    assert_eq!(kept(&all_a, &["S icase in `A`, null"]), [0, 1]);
}

#[test]
fn includes_searches_strings_and_never_keeps_null() {
    let t = strings_table();
    assert_eq!(kept(&t, &["S includes `a`"]), [0, 4]);
    assert_eq!(kept(&t, &["S icase includes \"b\""]), [1, 4, 5]);
    assert_eq!(kept(&t, &["S includes any `a`, `b`"]), [0, 4, 5]);
    assert_eq!(kept(&t, &["S includes all `a`, `b`"]), [4]);
    assert_eq!(kept(&t, &["S not includes `a`"]), [1, 3, 5]);
    // Java's inversions: "not any" keeps what lacks one of them, "not all" what has none.
    assert_eq!(kept(&t, &["S not includes any `a`, `b`"]), [0, 1, 3, 5]);
    assert_eq!(kept(&t, &["S not includes all `a`, `b`"]), [1, 3]);
    let error = refused(&t, &["B includes `a`"]);
    assert!(is_invalid(&error), "{error}");
}

#[test]
fn instant_filters_read_date_times_and_scaled_integers() {
    let nanos = TimestampNanosecondArray::from(vec![
        Some(1_700_000_000_000_000_000),
        Some(1_700_000_000_000_000_001),
        None,
        Some(-1),
        Some(0),
        Some(1_699_999_999_000_000_000),
    ])
    .with_timezone("UTC");
    let t = static_table(vec![("T", ColumnType::Instant, Arc::new(nanos))]);
    assert_eq!(kept(&t, &["T > '2023-11-14T22:13:20 UTC'"]), [1]);
    assert_eq!(kept(&t, &["T >= '2023-11-14T22:13:20Z'"]), [0, 1]);
    assert_eq!(kept(&t, &["T < '2023-11-14T22:13:20 UTC'"]), [2, 3, 4, 5]);
    assert_eq!(kept(&t, &["T == '2023-11-14T22:13:20 UTC'"]), [0]);
    assert_eq!(kept(&t, &["T == null"]), [2]);
    // A bare integer is seconds, milliseconds, microseconds or nanoseconds by its size.
    assert_eq!(kept(&t, &["T in '1700000000', '0'"]), [0, 4]);
    assert_eq!(kept(&t, &["T in '1700000000000'"]), [0]);
    assert_eq!(kept(&t, &["T == '1700000000000000001'"]), [1]);
}

// ---------------------------------------------------------------------------------------
// Formulas that evaluate to null
// ---------------------------------------------------------------------------------------

#[test]
fn a_condition_that_evaluates_to_null_fails_the_operation() {
    let t = strings_table();
    // `B` alone is a formula of type Boolean; rows 2 and 5 are null.
    let error = refused(&t, &["B"]);
    assert!(
        matches!(&error, FilterError::Failed(f) if f.message.contains("NullPointerException")),
        "{error}"
    );
    // A null operand of `&&` or `||` that is evaluated throws too.
    for text in ["B && S == S", "B || S != S"] {
        let error = refused(&t, &[text]);
        assert!(matches!(error, FilterError::Failed(_)), "{text}: {error}");
    }
    // Each filter sees only the rows the filters before it kept, so nulls that an earlier
    // filter removed do no harm.
    assert_eq!(kept(&t, &["!isNull(B)", "B"]), [0, 3]);
    assert_eq!(kept(&t, &["S == `a`", "B"]), [0]);
    // And a filter after one that kept nothing does not run at all.
    assert_eq!(kept(&t, &["S == `zz`", "B"]), [] as [i64; 0]);
}

#[test]
fn a_condition_that_evaluates_to_null_later_fails_the_result_table() {
    let stepper = SteppedUpdateGraph::new();
    let graph: Arc<dyn UpdateGraph> = stepper.clone();
    let source = ScriptedTable::new(
        graph,
        definition(&[("K", ColumnType::Long), ("B", ColumnType::Boolean)]),
        RowSet::from_keys([0, 1]),
        vec![
            longs([1, 2]),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    );
    let parent: Arc<dyn Table> = source.table();
    let result = unstructured_filter(&parent, &["B"], &Compiler).unwrap();
    let recorder = Recorder::attach(result.clone());
    assert_eq!(keys_of(&*result), [0]);

    source.push(TableChange::new().add(
        [2],
        vec![longs([3]), Arc::new(BooleanArray::from(vec![Some(true)]))],
    ));
    stepper.run_cycle();
    assert_eq!(keys_of(&*result), [0, 2]);
    assert!(!result.is_failed());

    source.push(TableChange::new().add(
        [3],
        vec![longs([4]), Arc::new(BooleanArray::from(vec![None]))],
    ));
    stepper.run_cycle();
    assert!(result.is_failed());
    let events = recorder.events();
    assert_eq!(events.len(), 2);
    let failure = events[1].clone().unwrap_err();
    assert!(
        failure.message.contains("NullPointerException"),
        "{failure}"
    );
    // A new `where` on the parent fails at once, and one on the failed result is refused.
    assert!(matches!(
        unstructured_filter(&parent, &["B"], &Compiler),
        Err(FilterError::Failed(_))
    ));
    assert!(matches!(
        unstructured_filter(&result, &["K > 0"], &Compiler),
        Err(FilterError::Failed(_))
    ));
}

// ---------------------------------------------------------------------------------------
// Filters that are refused
// ---------------------------------------------------------------------------------------

#[test]
fn filters_that_do_not_fit_the_table_are_errors() {
    let t = ints_table();
    for text in [
        "Nope > 1",
        "Nope == 1",
        "Nope in 1, 2",
        "I >",
        "I + 1",
        "",
        "I in `x`",
        "I in 1.5",
        "I includes `x`",
        "I > 1 &&\n I < 3",
        "I in 2147483648",
    ] {
        let error = refused(&t, &[text]);
        assert!(is_invalid(&error), "{text}: {error}");
    }
    // An int ordered against a string compiles as Java's `compareTo(Comparable, Comparable)`
    // and fails where the two meet, as Java's initial filter does.
    let error = refused(&t, &["I > `x`"]);
    assert!(
        matches!(&error, FilterError::Failed(f) if f.message.contains("ClassCastException")),
        "{error}"
    );
    // One bad filter among good ones refuses the operation.
    assert!(is_invalid(&refused(&t, &["I > 1", "Nope > 1"])));
    // The error of a simple filter is reported, not that of the formula tried after it.
    let error = refused(&t, &["Nope > 1"]);
    assert!(error.to_string().contains("Nope"), "{error}");
}

#[test]
fn a_match_on_a_missing_column_swaps_with_its_one_value_when_that_is_a_column() {
    // `x == I` is first read as column `x` with value `I`.
    let t = ints_table();
    assert_eq!(kept(&t, &["I == 5"]), [0]);
    let error = refused(&t, &["x == I"]);
    assert!(is_invalid(&error), "{error}");
    // `5 == I` has the literal on the left and is turned around by the routing.
    assert_eq!(kept(&t, &["5 == I"]), [0]);
    assert_eq!(kept(&t, &["5 <= I"]), [0, 5]);
}

// ---------------------------------------------------------------------------------------
// The result table
// ---------------------------------------------------------------------------------------

#[test]
fn no_filters_give_the_parent_itself() {
    let t = ints_table();
    let none: [&str; 0] = [];
    assert!(Arc::ptr_eq(
        &unstructured_filter(&t, &none, &Compiler).unwrap(),
        &t
    ));
    assert!(Arc::ptr_eq(&filter(&t, &[], &Compiler).unwrap(), &t));
}

#[test]
fn the_result_has_the_parents_columns_and_is_static_for_a_static_parent() {
    let t = strings_table();
    let result = unstructured_filter(&t, &["S != `a`"], &Compiler).unwrap();
    assert!(!result.is_refreshing());
    assert!(!result.is_flat());
    assert_eq!(result.definition(), t.definition());
    let expected = static_table(vec![
        (
            "S",
            ColumnType::String,
            Arc::new(StringArray::from(vec![
                Some("B"),
                None,
                Some(""),
                Some("ab"),
                Some("b"),
            ])),
        ),
        (
            "B",
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![
                Some(false),
                None,
                Some(true),
                Some(false),
                None,
            ])),
        ),
        (
            "C",
            ColumnType::Char,
            Arc::new(UInt16Array::from(vec![
                Some(98),
                None,
                Some(99),
                Some(65),
                Some(97),
            ])),
        ),
    ]);
    assert_tables_equal(&*expected, &*result);
    ConsistencyChecker::attach(result).assert_consistent();

    // An append-only parent gives an append-only result.
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let definition = definition(&[("Qty", ColumnType::Long)]);
    let parent: Arc<dyn Table> = append_only_input_table(graph, definition.clone(), None).unwrap();
    let result = unstructured_filter(&parent, &["Qty > 1"], &Compiler).unwrap();
    assert!(result.is_refreshing());
    let attributes = result.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
    let schema = Arc::new(Schema::new(vec![Field::new("Qty", DataType::Int64, true)]));
    let rows = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]);
    parent.input_table().unwrap().add(rows.unwrap()).unwrap();
    assert_eq!(keys_of(&*result), [1, 2]);
}

/// A refreshing table of two rows with the given attributes.
fn refreshing_with(add_only: bool, append_only: bool, blink: bool, flat: bool) -> Arc<dyn Table> {
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let source = ScriptedTable::new(
        graph,
        definition(&[("Qty", ColumnType::Long)]),
        RowSet::flat(2),
        vec![longs([1, 2])],
    );
    let parent: Arc<dyn Table> = source.table();
    let mut attributes = TableAttributes::default();
    attributes.add_only = add_only;
    attributes.append_only = append_only;
    attributes.blink = blink;
    let table = QueryTable::derive(&parent, || {
        let mut spec = TableSpec::new(
            parent.definition().clone(),
            (*parent.row_set()).clone(),
            vec![parent.column(0)],
        )
        .with_attributes(attributes);
        spec.flat = flat;
        Ok((spec, |_: &QueryTable, upstream: &TableUpdate| {
            Ok(upstream.clone())
        }))
    });
    table.unwrap()
}

#[test]
fn the_result_is_add_only_and_append_only_when_the_parent_is_or_is_static() {
    let attributes_of = |parent: &Arc<dyn Table>| {
        let attributes = unstructured_filter(parent, &["Qty > 1"], &Compiler)
            .unwrap()
            .attributes();
        (
            attributes.add_only,
            attributes.append_only,
            attributes.blink,
        )
    };
    // A static table never removes a row and never adds one out of order.
    let fixed = static_table(vec![("Qty", ColumnType::Long, longs([1, 2, 3]))]);
    assert_eq!(fixed.attributes(), TableAttributes::default());
    assert_eq!(attributes_of(&fixed), (true, true, false));
    // (add-only, append-only, blink, flat) of a refreshing parent, and the result's three.
    for (parent, expected) in [
        ((false, false, false, false), (false, false, false)),
        ((false, false, false, true), (false, false, false)),
        ((true, false, false, false), (true, false, false)),
        // Rows added to a flat add-only table are added at its end.
        ((true, false, false, true), (true, true, false)),
        ((false, true, false, false), (true, true, false)),
        ((true, true, false, false), (true, true, false)),
        ((false, false, true, false), (false, false, true)),
    ] {
        let (add_only, append_only, blink, flat) = parent;
        let table = refreshing_with(add_only, append_only, blink, flat);
        assert_eq!(attributes_of(&table), expected, "parent {parent:?}");
    }
    // The structured form builds its result the same way.
    let qty_above_one = compare(CompareOp::GreaterThan, column("Qty"), double(1.0));
    let attributes = filter(&fixed, &[qty_above_one], &Compiler)
        .unwrap()
        .attributes();
    assert!(attributes.add_only && attributes.append_only);
}

// ---------------------------------------------------------------------------------------
// Structured conditions
// ---------------------------------------------------------------------------------------

fn column(name: &str) -> Value {
    Value::Reference(name.to_string())
}

fn double(value: f64) -> Value {
    Value::Literal(Literal::Double(value))
}

fn compare(op: CompareOp, lhs: Value, rhs: Value) -> Condition {
    Condition::Compare {
        op,
        case_sensitivity: CaseSensitivity::MatchCase,
        lhs,
        rhs,
    }
}

fn is_in(target: Value, candidates: Vec<Value>, nan_comparison: NanComparison) -> Condition {
    Condition::In {
        target,
        candidates,
        case_sensitivity: CaseSensitivity::MatchCase,
        match_type: MatchType::Regular,
        nan_comparison,
    }
}

fn not(condition: Condition) -> Condition {
    Condition::Not(Box::new(condition))
}

#[test]
fn structured_comparisons_use_range_match_and_formula_semantics() {
    let t = doubles_table();
    let d = || column("D");
    // Range filters: null lowest, NaN never greater.
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::LessThan, d(), double(0.0))]),
        [4, 6, 8]
    );
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::GreaterThanOrEqual, d(), double(0.0))]
        ),
        [0, 1, 2, 5, 7]
    );
    // NOT is rewritten into the opposite range filter, which is not the complement.
    assert_eq!(
        kept_by(&t, &[not(compare(CompareOp::LessThan, d(), double(0.0)))]),
        [0, 1, 2, 5, 7]
    );
    // A literal on the left: Java filters `0 < D` as `D >= 0`, and `0 >= D` as `D < 0`.
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::LessThan, double(0.0), d())]),
        [0, 1, 2, 5, 7]
    );
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::GreaterThanOrEqual, double(0.0), d())]
        ),
        [4, 6, 8]
    );
    // Equality goes through `D == 0` as text: a match filter, IEEE 754.
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::Equals, d(), double(0.0))]),
        [1, 2]
    );
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::NotEquals, d(), double(1.0))]),
        [1, 2, 3, 4, 5, 6, 7, 8]
    );
    // A non-finite literal prints as a field, `D == Double.NaN`, which is a formula:
    // `QueryLanguageFunctionUtils#eq(double, double)` is `==` on non-null values, so NaN
    // equals nothing, as recorded for `filter_structured.double_equal_nan`.
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::Equals, d(), double(f64::NAN))]),
        [] as [i64; 0]
    );
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::Equals, double(f64::NAN), d())]),
        [] as [i64; 0]
    );
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::NotEquals, d(), double(f64::NAN))]),
        [0, 1, 2, 3, 4, 5, 6, 7, 8]
    );
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::Equals, d(), double(f64::INFINITY))]
        ),
        [7]
    );
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::Equals, d(), double(f64::NEG_INFINITY))]
        ),
        [8]
    );
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::NotEquals, d(), double(f64::INFINITY))]
        ),
        [0, 1, 2, 3, 4, 5, 6, 8]
    );
    // A range against NaN: everything is below it and nothing above.
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::LessThan, d(), double(f64::NAN))]),
        [0, 1, 2, 4, 5, 6, 7, 8]
    );
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::GreaterThan, d(), double(f64::NAN))]
        ),
        [] as [i64; 0]
    );
    // Two columns: a formula.
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::LessThan, d(), column("Z"))]),
        [1, 4, 6, 8]
    );
    // Several conditions are a conjunction.
    assert_eq!(
        kept_by(
            &t,
            &[
                compare(CompareOp::GreaterThan, d(), double(0.0)),
                compare(CompareOp::LessThan, d(), double(5.0)),
            ]
        ),
        [0]
    );
}

#[test]
fn structured_in_isnull_and_combinations() {
    let t = doubles_table();
    let d = || column("D");
    let candidates = || vec![double(1.0), double(-0.0)];
    assert_eq!(
        kept_by(
            &t,
            &[is_in(d(), candidates(), NanComparison::NanNotEqualsNan)]
        ),
        [0, 1, 2]
    );
    assert_eq!(
        kept_by(&t, &[is_in(d(), candidates(), NanComparison::NanEqualsNan)]),
        [0, 1, 2]
    );
    assert_eq!(
        kept_by(
            &t,
            &[not(is_in(d(), candidates(), NanComparison::NanEqualsNan))]
        ),
        [3, 4, 5, 6, 7, 8]
    );
    // A NaN or infinite candidate reaches the match filter as `Double.NaN` or
    // `Double.POSITIVE_INFINITY`, which Java's `Double.parseDouble` does not read.
    for value in [f64::NAN, f64::INFINITY] {
        let condition = is_in(d(), vec![double(value)], NanComparison::NanEqualsNan);
        assert!(matches!(
            filter(&t, &[condition], &Compiler),
            Err(FilterError::Invalid(_))
        ));
    }
    // No candidates: nothing, or everything when inverted.
    assert_eq!(
        kept_by(&t, &[is_in(d(), vec![], NanComparison::NanEqualsNan)]),
        [] as [i64; 0]
    );
    assert_eq!(
        kept_by(&t, &[not(is_in(d(), vec![], NanComparison::NanEqualsNan))]),
        [0, 1, 2, 3, 4, 5, 6, 7, 8]
    );
    let is_null = Condition::IsNull {
        column: "D".to_string(),
    };
    assert_eq!(kept_by(&t, std::slice::from_ref(&is_null)), [4, 6]);
    assert_eq!(kept_by(&t, &[not(is_null.clone())]), [0, 1, 2, 3, 5, 7, 8]);
    assert_eq!(
        kept_by(
            &t,
            &[Condition::Or(vec![
                is_null.clone(),
                Condition::And(vec![
                    compare(CompareOp::GreaterThan, d(), double(0.5)),
                    compare(CompareOp::LessThanOrEqual, d(), double(5.0)),
                ]),
            ])]
        ),
        [0, 4, 5, 6]
    );
    // NOT over an OR becomes an AND of the inverses.
    assert_eq!(
        kept_by(
            &t,
            &[not(Condition::Or(vec![
                is_null,
                compare(CompareOp::GreaterThan, d(), double(0.5)),
            ]))]
        ),
        [1, 2, 8]
    );
    // An empty OR, which a literal target without candidates turns into, keeps every row.
    assert_eq!(
        kept_by(
            &t,
            &[is_in(double(1.0), vec![], NanComparison::NanEqualsNan)]
        ),
        [0, 1, 2, 3, 4, 5, 6, 7, 8]
    );
}

#[test]
fn structured_string_conditions() {
    let t = strings_table();
    let pattern = |contains: bool, text: &str, ignore: bool, inverted: bool| {
        let case_sensitivity = if ignore {
            CaseSensitivity::IgnoreCase
        } else {
            CaseSensitivity::MatchCase
        };
        let match_type = if inverted {
            MatchType::Inverted
        } else {
            MatchType::Regular
        };
        if contains {
            Condition::Contains {
                column: "S".to_string(),
                search_string: text.to_string(),
                case_sensitivity,
                match_type,
            }
        } else {
            Condition::Matches {
                column: "S".to_string(),
                regex: text.to_string(),
                case_sensitivity,
                match_type,
            }
        }
    };
    assert_eq!(kept_by(&t, &[pattern(true, "b", false, false)]), [4, 5]);
    assert_eq!(kept_by(&t, &[pattern(true, "b", true, false)]), [1, 4, 5]);
    // Inverted keeps the values without the string, but never null.
    assert_eq!(kept_by(&t, &[pattern(true, "b", true, true)]), [0, 3]);
    assert_eq!(kept_by(&t, &[not(pattern(true, "b", true, false))]), [0, 3]);
    // The search string is not a regular expression.
    assert_eq!(
        kept_by(&t, &[pattern(true, ".", false, false)]),
        [] as [i64; 0]
    );
    // The regular expression must match the whole value.
    assert_eq!(kept_by(&t, &[pattern(false, "a.*", false, false)]), [0, 4]);
    assert_eq!(kept_by(&t, &[pattern(false, "b", true, false)]), [1, 5]);
    assert_eq!(kept_by(&t, &[pattern(false, "[ab]*", false, true)]), [1]);
    // A string comparison with a literal.
    let string = |text: &str| Value::Literal(Literal::String(text.to_string()));
    assert_eq!(
        kept_by(&t, &[compare(CompareOp::Equals, column("S"), string("a"))]),
        [0]
    );
    // An ordering hands the string to the range filter without quotes, and the range
    // filter reads only quoted strings; a string that carries its own quotes works.
    assert!(matches!(
        filter(
            &t,
            &[compare(CompareOp::LessThan, column("S"), string("a"))],
            &Compiler
        ),
        Err(FilterError::Invalid(_))
    ));
    assert_eq!(
        kept_by(
            &t,
            &[compare(CompareOp::LessThan, column("S"), string("`a`"))]
        ),
        [1, 2, 3]
    );
    assert_eq!(
        kept_by(
            &t,
            &[Condition::In {
                target: column("S"),
                candidates: vec![string("A"), string("ab")],
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Inverted,
                nan_comparison: NanComparison::NanNotEqualsNan,
            }]
        ),
        [1, 2, 3, 5]
    );
}

#[test]
fn structured_conditions_that_cannot_be_built_are_errors() {
    let t = strings_table();
    let refused =
        |condition: Condition| match filter(&t, std::slice::from_ref(&condition), &Compiler) {
            Ok(_) => panic!("{condition:?} was accepted"),
            Err(e) => e,
        };
    let string = |text: &str| Value::Literal(Literal::String(text.to_string()));
    for condition in [
        compare(CompareOp::LessThan, column("Nope"), double(1.0)),
        compare(CompareOp::Equals, column("Nope"), double(1.0)),
        // A string value where the column needs a boolean.
        compare(CompareOp::LessThan, column("B"), string("x")),
        is_in(
            column("Nope"),
            vec![double(1.0)],
            NanComparison::NanEqualsNan,
        ),
        is_in(column("S"), vec![double(1.5)], NanComparison::NanEqualsNan),
        Condition::IsNull {
            column: "Nope".to_string(),
        },
        Condition::Contains {
            column: "B".to_string(),
            search_string: "x".to_string(),
            case_sensitivity: CaseSensitivity::MatchCase,
            match_type: MatchType::Regular,
        },
        Condition::Matches {
            column: "S".to_string(),
            regex: "(".to_string(),
            case_sensitivity: CaseSensitivity::MatchCase,
            match_type: MatchType::Regular,
        },
        not(Condition::Search {
            search_string: "x".to_string(),
            columns: vec![],
        }),
    ] {
        let error = refused(condition.clone());
        assert!(is_invalid(&error), "{condition:?}: {error}");
    }
}

// ---------------------------------------------------------------------------------------
// Following a ticking parent
// ---------------------------------------------------------------------------------------

fn sym_qty() -> TableDefinition {
    definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)])
}

struct Ticking {
    stepper: Arc<SteppedUpdateGraph>,
    source: Arc<ScriptedTable>,
    parent: Arc<dyn Table>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

/// A parent with rows 0, 1, 5 and 6 holding `Qty` 10, 20, 30 and 40, filtered by `filters`,
/// with a checker that recomputes the result from scratch.
fn ticking(filters: &'static [&'static str], compiler: Arc<dyn FormulaCompiler>) -> Ticking {
    let stepper = SteppedUpdateGraph::new();
    let graph: Arc<dyn UpdateGraph> = stepper.clone();
    let source = ScriptedTable::new(
        graph,
        sym_qty(),
        RowSet::from_keys([0, 1, 5, 6]),
        vec![strings(["a", "b", "c", "d"]), longs([10, 20, 30, 40])],
    );
    let parent: Arc<dyn Table> = source.table();
    let result = unstructured_filter(&parent, filters, &*compiler).unwrap();
    let recorder = Recorder::attach(result.clone());
    let expected = parent.clone();
    let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
        from_scratch(&expected, |table| {
            unstructured_filter(table, filters, &Compiler)
        })
    });
    Ticking {
        stepper,
        source,
        parent,
        result,
        recorder,
        checker,
    }
}

/// The static path of an operation on a copy of `table` as it is now.
fn from_scratch(
    table: &Arc<dyn Table>,
    operation: impl Fn(&Arc<dyn Table>) -> Result<Arc<dyn Table>, FilterError>,
) -> Arc<dyn Table> {
    let snapshot = TableSnapshot::current(&**table).unwrap();
    let copy: Arc<dyn Table> =
        ReferenceTable::new_static(table.definition().clone(), snapshot.rows, snapshot.columns);
    assert!(!copy.is_refreshing());
    operation(&copy).unwrap()
}

impl Ticking {
    /// Applies `change` in one cycle and returns the update the result delivered, if any.
    fn step(&self, change: TableChange) -> Option<TableUpdate> {
        let before = self.recorder.events().len();
        self.source.push(change);
        self.stepper.run_cycle();
        self.checker.assert_consistent();
        let events = self.recorder.events();
        assert!(events.len() <= before + 1);
        events.get(before).map(|event| event.clone().unwrap())
    }
}

fn rows(keys: impl IntoIterator<Item = RowKey>) -> RowSet {
    RowSet::from_keys(keys)
}

#[test]
fn added_rows_are_filtered() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    assert_eq!(keys_of(&*t.result), [1, 5, 6]);
    assert!(t.result.is_refreshing());
    let update = t.step(TableChange::new().add([3, 9], vec![strings(["x", "y"]), longs([5, 50])]));
    assert_eq!(
        update,
        Some(TableUpdate {
            added: rows([9]),
            ..TableUpdate::empty()
        })
    );
    assert_eq!(keys_of(&*t.result), [1, 5, 6, 9]);
}

#[test]
fn removed_rows_leave_if_they_were_in_the_result() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    let update = t.step(TableChange::new().remove([0, 1]));
    assert_eq!(
        update,
        Some(TableUpdate {
            removed: rows([1]),
            ..TableUpdate::empty()
        })
    );
    assert_eq!(keys_of(&*t.result), [5, 6]);
}

#[test]
fn modified_rows_enter_leave_or_stay() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    // Row 0 starts to pass, row 5 stops, row 6 still passes, row 1 is not modified.
    let update = t.step(TableChange::new().modify([0, 5, 6], [(1, longs([100, 1, 41]))]));
    assert_eq!(
        update,
        Some(TableUpdate {
            added: rows([0]),
            removed: rows([5]),
            modified: rows([6]),
            modified_columns: ModifiedColumnSet::of([1]),
            ..TableUpdate::empty()
        })
    );
    assert_eq!(keys_of(&*t.result), [0, 1, 6]);
    // A modified row that failed before and fails now does not show.
    assert_eq!(
        t.step(TableChange::new().modify([5], [(1, longs([2]))])),
        None
    );
}

#[test]
fn shifts_pass_through() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    let update = t.step(TableChange::new().shift(5, 6, 10));
    let shifted = RowSetShiftData::new(vec![ShiftRange {
        first: 5,
        last: 6,
        delta: 10,
    }])
    .unwrap();
    assert_eq!(
        update,
        Some(TableUpdate {
            shifted: shifted.clone(),
            ..TableUpdate::empty()
        })
    );
    assert_eq!(keys_of(&*t.result), [1, 15, 16]);
    // A shift of rows the result does not have is still reported; it moves nothing.
    let update = t.step(TableChange::new().shift(0, 0, 100));
    assert_eq!(
        update.map(|u| u.added.is_empty() && u.removed.is_empty()),
        Some(true)
    );
    assert_eq!(keys_of(&*t.result), [1, 15, 16]);
}

#[test]
fn removes_shifts_modifies_and_adds_in_one_cycle() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    // Row 1 is removed. Rows 5 and 6 move to 7 and 8. The row now at 7 stops passing, so
    // it is removed by its old key 5. Row 0 starts passing and row 2 is new.
    let change = TableChange::new()
        .remove([1])
        .shift(5, 6, 2)
        .add([2, 3], vec![strings(["x", "y"]), longs([99, 1])])
        .modify([0, 7], [(1, longs([16, 3]))]);
    let update = t.step(change).unwrap();
    assert_eq!(update.added, rows([0, 2]));
    assert_eq!(update.removed, rows([1, 5]));
    assert_eq!(update.modified, RowSet::empty());
    assert_eq!(update.modified_columns, ModifiedColumnSet::empty());
    assert_eq!(update.shifted.ranges().len(), 1);
    assert_eq!(keys_of(&*t.result), [0, 2, 8]);

    // A row that keeps passing while it moves and changes is modified at its new key.
    let update = t
        .step(
            TableChange::new()
                .shift(8, 8, 12)
                .modify([20], [(1, longs([77]))]),
        )
        .unwrap();
    assert_eq!(update.modified, rows([20]));
    assert!(update.added.is_empty() && update.removed.is_empty());
    assert_eq!(keys_of(&*t.result), [0, 2, 20]);
}

#[test]
fn a_change_that_does_not_show_delivers_nothing() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    assert_eq!(
        t.step(TableChange::new().add([2], vec![strings(["x"]), longs([1])])),
        None
    );
    assert_eq!(t.step(TableChange::new().remove([0])), None);
    assert_eq!(t.step(TableChange::new()), None);
    assert_eq!(t.checker.updates_seen(), 0);
    assert_eq!(keys_of(&*t.result), [1, 5, 6]);
}

#[test]
fn a_parent_failure_fails_the_result() {
    let t = ticking(&["Qty > 15"], Arc::new(Compiler));
    t.source.push_failure("the source broke");
    t.stepper.run_cycle();
    assert!(t.result.is_failed());
    let events = t.recorder.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].clone().unwrap_err().message, "the source broke");
    assert!(matches!(
        unstructured_filter(&t.parent, &["Qty > 15"], &Compiler),
        Err(FilterError::Failed(_))
    ));
}

/// A compiler whose formulas count the rows they are evaluated for.
struct Counting {
    rows: Arc<AtomicU64>,
}

struct CountingFormula {
    inner: Arc<dyn Formula>,
    rows: Arc<AtomicU64>,
}

impl Formula for CountingFormula {
    fn result_type(&self) -> &ColumnType {
        self.inner.result_type()
    }
    fn columns(&self) -> &[String] {
        self.inner.columns()
    }
    fn column_arrays(&self) -> &[String] {
        self.inner.column_arrays()
    }
    fn uses_row_variables(&self) -> bool {
        self.inner.uses_row_variables()
    }
    fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure> {
        self.rows.fetch_add(rows.size(), Ordering::SeqCst);
        self.inner.evaluate(table, rows, use_prev)
    }
}

impl FormulaCompiler for Counting {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        Compiler.parse(text)
    }
    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        Compiler.parse_assignment(text)
    }
    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        Ok(Arc::new(CountingFormula {
            inner: Compiler.compile(expr, definition)?,
            rows: self.rows.clone(),
        }))
    }
}

#[test]
fn modified_rows_are_filtered_again_only_when_a_column_the_filters_read_changed() {
    let evaluated = Arc::new(AtomicU64::new(0));
    let count = || evaluated.swap(0, Ordering::SeqCst);
    let t = ticking(
        &["Qty * 2 > 30"],
        Arc::new(Counting {
            rows: evaluated.clone(),
        }),
    );
    assert_eq!(keys_of(&*t.result), [1, 5, 6]);
    assert_eq!(count(), 4);

    // Only `Sym` changed: the modified rows that are in the result are passed on, and the
    // formula is not evaluated.
    let update = t.step(TableChange::new().modify([0, 1], [(0, strings(["A", "B"]))]));
    assert_eq!(
        update,
        Some(TableUpdate {
            modified: rows([1]),
            modified_columns: ModifiedColumnSet::of([0]),
            ..TableUpdate::empty()
        })
    );
    assert_eq!(count(), 0);

    // `Qty` changed: the two modified rows are evaluated, and the added row.
    let change = TableChange::new()
        .add([9], vec![strings(["z"]), longs([1])])
        .modify([0, 1], [(1, longs([50, 60]))]);
    let update = t.step(change).unwrap();
    assert_eq!(update.added, rows([0]));
    assert_eq!(update.modified, rows([1]));
    assert_eq!(count(), 3);

    // Removes and shifts evaluate nothing.
    t.step(TableChange::new().remove([5]).shift(6, 6, 1));
    assert_eq!(count(), 0);
    assert_eq!(keys_of(&*t.result), [0, 1, 7]);
}

#[test]
fn later_filters_see_only_the_rows_earlier_filters_kept_when_ticking() {
    let evaluated = Arc::new(AtomicU64::new(0));
    let t = ticking(
        &["Sym in `a`, `b`", "Qty * 2 > 30"],
        Arc::new(Counting {
            rows: evaluated.clone(),
        }),
    );
    assert_eq!(keys_of(&*t.result), [1]);
    // The formula saw the two rows the match filter kept.
    assert_eq!(evaluated.swap(0, Ordering::SeqCst), 2);
    let change = TableChange::new().add(
        [7, 8, 9],
        vec![strings(["a", "q", "b"]), longs([100, 100, 1])],
    );
    assert_eq!(t.step(change).map(|u| u.added), Some(rows([7])));
    assert_eq!(evaluated.swap(0, Ordering::SeqCst), 2);
    // Nothing passes the first filter: the formula does not run.
    t.step(TableChange::new().add([10], vec![strings(["q"]), longs([100])]));
    assert_eq!(evaluated.swap(0, Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------------------
// Random updates
// ---------------------------------------------------------------------------------------

fn wide() -> TableDefinition {
    definition(&[
        ("Sym", ColumnType::String),
        ("Qty", ColumnType::Long),
        ("Px", ColumnType::Double),
        ("Flag", ColumnType::Boolean),
        ("N", ColumnType::Int),
    ])
}

/// Drives `operation` over a random parent for `cycles` cycles per seed of `seeds` that the
/// seed budget runs (`oxhaven_testkit::test_seeds`), checking after
/// every cycle that each update is consistent and that the result equals the static path
/// run on the parent's rows. Returns the number of updates the result delivered.
fn check_random(
    seeds: std::ops::Range<u64>,
    cycles: usize,
    kinds: impl Fn(&mut RandomUpdates),
    operation: impl Fn(&Arc<dyn Table>) -> Result<Arc<dyn Table>, FilterError>
        + Clone
        + Send
        + Sync
        + 'static,
) -> u64 {
    check_random_of(wide(), seeds, cycles, kinds, operation)
}

/// [`check_random`] over a parent of `definition`.
fn check_random_of(
    definition: TableDefinition,
    seeds: std::ops::Range<u64>,
    cycles: usize,
    kinds: impl Fn(&mut RandomUpdates),
    operation: impl Fn(&Arc<dyn Table>) -> Result<Arc<dyn Table>, FilterError>
        + Clone
        + Send
        + Sync
        + 'static,
) -> u64 {
    let mut updates = 0;
    for seed in test_seeds(seeds) {
        let stepper = SteppedUpdateGraph::new();
        let graph: Arc<dyn UpdateGraph> = stepper.clone();
        let mut random = RandomUpdates::new(seed);
        kinds(&mut random);
        let initial = RowSet::from_keys([2, 3, 7, 11, 12, 20]);
        let columns = definition
            .columns()
            .iter()
            .map(|column| random.values(&column.column_type, initial.size()))
            .collect();
        let source = ScriptedTable::new(graph, definition.clone(), initial, columns);
        let parent: Arc<dyn Table> = source.table();
        let result = operation(&parent).unwrap();
        let (expected, recompute) = (parent.clone(), operation.clone());
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            from_scratch(&expected, &recompute)
        });
        checker.assert_consistent();
        for _ in 0..cycles {
            source.push(random.next_change(&*parent, 6));
            stepper.run_cycle();
            checker.assert_consistent();
        }
        assert!(!result.is_failed());
        updates += checker.updates_seen();
    }
    updates
}

fn strings_op(
    filters: &'static [&'static str],
) -> impl Fn(&Arc<dyn Table>) -> Result<Arc<dyn Table>, FilterError> + Clone + Send + Sync + 'static
{
    move |table| unstructured_filter(table, filters, &Compiler)
}

#[test]
fn random_updates_through_range_filters() {
    let all = |_: &mut RandomUpdates| {};
    assert!(check_random(0..40, 60, all, strings_op(&["Qty > 0"])) > at_least_for(1000, 0..40));
    assert!(check_random(40..80, 60, all, strings_op(&["Px > 0"])) > at_least_for(1000, 40..80));
    assert!(
        check_random(80..120, 60, all, strings_op(&["Px <= 0.5", "N >= -3"]))
            > at_least_for(1000, 80..120)
    );
    assert!(
        check_random(120..140, 60, all, strings_op(&["Sym < `d`"])) > at_least_for(500, 120..140)
    );
}

#[test]
fn random_updates_through_match_filters() {
    let all = |_: &mut RandomUpdates| {};
    assert!(
        check_random(
            0..40,
            60,
            all,
            strings_op(&["Sym in `a`, `b`, `c`", "Qty != 3"])
        ) > at_least_for(1000, 0..40)
    );
    assert!(
        check_random(40..80, 60, all, strings_op(&["Px in NaN, 0.5, null, -1"]))
            > at_least_for(500, 40..80)
    );
    assert!(
        check_random(80..120, 60, all, strings_op(&["Px != 0", "Flag == true"]))
            > at_least_for(500, 80..120)
    );
    assert!(
        check_random(120..140, 60, all, strings_op(&["N == null"])) > at_least_for(200, 120..140)
    );
    assert!(
        check_random(
            140..160,
            60,
            all,
            strings_op(&["Sym icase not in `A`, `B`"])
        ) > at_least_for(500, 140..160)
    );
}

#[test]
fn random_updates_through_formulas_and_patterns() {
    let all = |_: &mut RandomUpdates| {};
    assert!(check_random(0..40, 60, all, strings_op(&["Px == Px"])) > at_least_for(1000, 0..40));
    assert!(
        check_random(40..80, 60, all, strings_op(&["Qty * 2 > N + 1"]))
            > at_least_for(1000, 40..80)
    );
    assert!(
        check_random(
            80..120,
            60,
            all,
            strings_op(&["!isNull(Flag)", "Flag", "Qty < N"])
        ) > at_least_for(500, 80..120)
    );
    assert!(
        check_random(
            120..150,
            60,
            all,
            strings_op(&["Sym includes any `a`, `c`", "Qty >= 0"])
        ) > at_least_for(500, 120..150)
    );
}

#[test]
fn random_updates_of_one_kind_at_a_time() {
    for kind in 0..4 {
        let only = move |random: &mut RandomUpdates| {
            random.adds = kind == 0 || kind == 3;
            random.removes = kind == 1 || kind == 3;
            random.modifies = kind == 2;
            random.shifts = kind == 3;
        };
        let updates = check_random(0..15, 50, only, strings_op(&["Qty > 0", "Px == Px"]));
        // With removes only the table is soon empty.
        assert!(updates > at_least_for(20, 0..15), "kind {kind}: {updates}");
    }
}

#[test]
fn random_updates_through_structured_conditions() {
    let all = |_: &mut RandomUpdates| {};
    let conditions = vec![
        Condition::Or(vec![
            compare(CompareOp::GreaterThan, column("Px"), double(0.25)),
            Condition::And(vec![
                Condition::IsNull {
                    column: "Px".to_string(),
                },
                not(compare(
                    CompareOp::LessThan,
                    column("Qty"),
                    Value::Literal(Literal::Long(0)),
                )),
            ]),
            Condition::Contains {
                column: "Sym".to_string(),
                search_string: "A".to_string(),
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Regular,
            },
        ]),
        not(is_in(
            column("N"),
            vec![
                Value::Literal(Literal::Long(1)),
                Value::Literal(Literal::Long(2)),
            ],
            NanComparison::NanNotEqualsNan,
        )),
    ];
    let operation = move |table: &Arc<dyn Table>| filter(table, &conditions, &Compiler);
    assert!(check_random(0..40, 60, all, operation) > at_least_for(1000, 0..40));
}

#[test]
fn a_where_of_a_where_stays_consistent() {
    let all = |_: &mut RandomUpdates| {};
    let operation = |table: &Arc<dyn Table>| {
        let first = unstructured_filter(table, &["Qty > -5"], &Compiler)?;
        unstructured_filter(&first, &["Px < 1", "Sym != `a`"], &Compiler)
    };
    assert!(check_random(0..40, 60, all, operation) > at_least_for(1000, 0..40));
}

/// Behavior: recorded from Java 42.9 with `where` over a vector column made by `group_by`
/// and a join, and the array `array(V)` of it.
#[test]
fn a_vector_or_array_column_matches_null_and_nothing_else() {
    let cells = |column_type: &ColumnType| {
        oxhaven_testkit::list_of(
            column_type,
            [None, Some(2), Some(0), None],
            Arc::new(Int32Array::from(vec![Some(5), None])),
        )
    };
    let (vector, array) = (
        ColumnType::vector(ColumnType::Int),
        ColumnType::array(ColumnType::Int),
    );
    let t = static_table(vec![
        ("V", vector.clone(), cells(&vector)),
        ("A", array.clone(), cells(&array)),
    ]);
    for column in ["V", "A"] {
        let kept = |filter: &str| kept(&t, &[&format!("{column} {filter}")]);
        assert_eq!(kept("== null"), [0, 3]);
        assert_eq!(kept("= null"), [0, 3]);
        assert_eq!(kept("!= null"), [1, 2]);
        assert_eq!(kept("in null, null"), [0, 3]);
        assert_eq!(kept("not in null"), [1, 2]);
        assert_eq!(kept("icase in null"), [0, 3]);
        // Java has no value but null for such a column, and no formula `V == 3` either.
        refused(&t, &[&format!("{column} == 3")]);
        refused(&t, &[&format!("{column} == `a`")]);
    }
    assert_eq!(kept(&t, &["V == null", "A != null"]), [] as [i64; 0]);
}

// ---------------------------------------------------------------------------------------
// where_one_of: the expectations are what Java 42.9 answered for a table of `A = i`,
// `B = i * 2`, `C = i * 3` and `D = i * 4`
// ---------------------------------------------------------------------------------------

fn four_columns() -> Arc<dyn Table> {
    let ints = |values: [i32; 3]| Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
    static_table(vec![
        ("A", ColumnType::Int, ints([0, 1, 2])),
        ("B", ColumnType::Int, ints([0, 2, 4])),
        ("C", ColumnType::Int, ints([0, 3, 6])),
        ("D", ColumnType::Int, ints([0, 4, 8])),
    ])
}

/// The positions, in the source, of the rows that one of `filters` keeps.
fn kept_by_one_of(table: &Arc<dyn Table>, filters: &[&str]) -> Vec<i64> {
    let result =
        where_one_of(table, filters, &Compiler).unwrap_or_else(|e| panic!("{filters:?}: {e}"));
    assert!(result.row_set().is_subset_of(&table.row_set()));
    result.row_set().iter().map(|key| key / 10).collect()
}

#[test]
fn a_row_is_kept_by_at_least_one_of_the_filters() {
    let table = four_columns();
    assert_eq!(kept_by_one_of(&table, &["A > 1", "B == 0"]), [0, 2]);
    assert_eq!(
        kept_by_one_of(&table, &["A > 1", "B == 0", "C == 3"]),
        [0, 1, 2]
    );
    assert_eq!(kept_by_one_of(&table, &["A > 5", "B < 0"]), [] as [i64; 0]);
    // one filter is that filter
    assert_eq!(kept_by_one_of(&table, &["A > 0"]), kept(&table, &["A > 0"]));
    // the result is a `where`: not flat, and a static one may only grow
    let result = where_one_of(&table, &["A > 1", "B == 0", "C == 3"], &Compiler).unwrap();
    assert!(!result.is_flat());
    let attributes = result.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
}

#[test]
fn no_filters_and_a_filter_of_no_column_are_refused() {
    let table = four_columns();
    let refused = |filters: &[&str]| match where_one_of(&table, filters, &Compiler) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("{filters:?} were accepted"),
    };
    assert_eq!(
        refused(&[]),
        "Should not be constructing literal boolean WhereFilter"
    );
    assert_eq!(
        refused(&["A > 1", "Q == 0"]),
        "Column \"Q\" doesn't exist in this table, available columns: [A, B, C, D]"
    );
}

fn one_of(
    filters: &'static [&'static str],
) -> impl Fn(&Arc<dyn Table>) -> Result<Arc<dyn Table>, FilterError> + Clone + Send + Sync + 'static
{
    move |table| where_one_of(table, filters, &Compiler)
}

#[test]
fn random_updates_through_a_disjunction() {
    let all = |_: &mut RandomUpdates| {};
    assert!(
        check_random(0..40, 60, all, one_of(&["Qty > 2", "Px < 0"])) > at_least_for(1000, 0..40)
    );
    assert!(
        check_random(
            40..80,
            60,
            all,
            one_of(&["Sym in `a`, `b`", "Flag == true", "N == null"])
        ) > at_least_for(1000, 40..80)
    );
    // a formula among the alternatives, and an alternative that no row passes
    assert!(
        check_random(
            80..120,
            60,
            all,
            one_of(&["Qty + N > 3", "Px == 0.5", "Qty > 1000000"])
        ) > at_least_for(500, 80..120)
    );
    // the same rows as the conjunction of the negations leaves out
    for seed in test_seeds(0..10) {
        let mut random = RandomUpdates::new(seed);
        let definition = wide();
        let rows = RowSet::from_keys((0..40).map(|position| position * 10));
        let columns = definition
            .columns()
            .iter()
            .map(|column| random.values(&column.column_type, rows.size()))
            .collect();
        let table: Arc<dyn Table> = ReferenceTable::new_static(definition, rows, columns);
        let either = kept_by_one_of(&table, &["Qty > 2", "N < 0"]);
        let first = kept(&table, &["Qty > 2"]);
        let second = kept(&table, &["N < 0"]);
        let mut union: Vec<i64> = first.into_iter().chain(second).collect();
        union.sort_unstable();
        union.dedup();
        assert_eq!(either, union, "seed {seed}");
    }
}

// ---------------------------------------------------------------------------------------
// Searches of vector and array columns
// ---------------------------------------------------------------------------------------

fn search(text: &str) -> Condition {
    Condition::Search {
        search_string: text.to_string(),
        columns: vec![],
    }
}

/// An array of ints, a vector and an array of strings, an array of doubles and a vector of
/// instants. A vector of a primitive type is left out: the search fails on its null
/// elements, which the random values have.
fn lists_definition() -> TableDefinition {
    definition(&[
        ("V", ColumnType::array(ColumnType::Int)),
        ("S", ColumnType::vector(ColumnType::String)),
        ("A", ColumnType::array(ColumnType::String)),
        ("D", ColumnType::array(ColumnType::Double)),
        ("T", ColumnType::vector(ColumnType::Instant)),
    ])
}

#[test]
fn random_updates_through_searches_of_vectors_and_arrays() {
    let all = |_: &mut RandomUpdates| {};
    for text in ["2", "a", "1.25", "-2147483648", "2023-11-14T22:13:25 UTC"] {
        // A search that reads a null cell fails, so the null cells are dropped first.
        let operation = move |table: &Arc<dyn Table>| {
            let present = unstructured_filter(
                table,
                &[
                    "V != null",
                    "S != null",
                    "A != null",
                    "D != null",
                    "T != null",
                ],
                &Compiler,
            )?;
            filter(&present, &[search(text)], &Compiler)
        };
        assert!(
            check_random_of(lists_definition(), 0..30, 60, all, operation)
                > at_least_for(400, 0..30),
            "{text:?}"
        );
    }
}

#[test]
fn a_null_vector_that_a_search_reads_later_fails_the_result_table() {
    let stepper = SteppedUpdateGraph::new();
    let graph: Arc<dyn UpdateGraph> = stepper.clone();
    let vector = ColumnType::vector(ColumnType::Int);
    let cells = |lengths: &[Option<usize>], elements: Vec<i32>| {
        oxhaven_testkit::list_of(
            &ColumnType::vector(ColumnType::Int),
            lengths.iter().copied(),
            Arc::new(Int32Array::from(elements)),
        )
    };
    let source = ScriptedTable::new(
        graph,
        definition(&[("K", ColumnType::Long), ("V", vector)]),
        RowSet::from_keys([0, 1]),
        vec![longs([1, 2]), cells(&[Some(2), Some(0)], vec![5, 7])],
    );
    let parent: Arc<dyn Table> = source.table();
    let result = filter(&parent, &[search("7")], &Compiler).unwrap();
    let recorder = Recorder::attach(result.clone());
    assert_eq!(keys_of(&*result), [0]);

    // A null vector in a row that the key column keeps is not read.
    source.push(TableChange::new().add([2], vec![longs([7]), cells(&[None], vec![])]));
    stepper.run_cycle();
    assert_eq!(keys_of(&*result), [0, 2]);
    assert!(!result.is_failed());

    source.push(TableChange::new().add([3], vec![longs([4]), cells(&[None], vec![])]));
    stepper.run_cycle();
    assert!(result.is_failed());
    let events = recorder.events();
    assert_eq!(events.len(), 2);
    let failure = events[1].clone().unwrap_err();
    assert!(
        failure.message.contains("NullPointerException"),
        "{failure}"
    );
}

/// The reference is each row's cell searched alone.
#[test]
fn a_search_of_many_vectors_keeps_what_each_row_alone_keeps_on_any_number_of_threads() {
    const PIECE: usize = 1 << 16;
    for rows in [PIECE - 1, PIECE + 1, 3 * PIECE + 17] {
        // lengths 0 to 4, with the elements 0 to 96 in turn
        let lengths: Vec<Option<usize>> = (0..rows).map(|row| Some(row * 7 % 5)).collect();
        let total: usize = lengths.iter().flatten().sum();
        let elements: Vec<i32> = (0..total).map(|at| (at * 13 % 97) as i32).collect();
        let mut starts = Vec::with_capacity(rows);
        let mut start = 0;
        for length in lengths.iter().flatten() {
            starts.push(start);
            start += length;
        }
        let found = |row: usize| {
            elements[starts[row]..starts[row] + lengths[row].unwrap_or(0)].contains(&42)
        };
        let vector = ColumnType::vector(ColumnType::Int);
        let cells = oxhaven_testkit::list_of(
            &vector,
            lengths.iter().copied(),
            Arc::new(Int32Array::from(elements.clone())),
        );
        let table = static_table(vec![("V", vector, cells)]);
        let expected: Vec<i64> = (0..rows)
            .filter(|row| found(*row))
            .map(|row| row as i64)
            .collect();
        assert!(!expected.is_empty() && expected.len() < rows);
        for threads in [1, 4] {
            let kept = oxhaven_column::parallel::with_threads(threads, || {
                kept_by(&table, &[search("42")])
            });
            assert!(kept == expected, "{rows} rows on {threads} threads");
        }
    }
}

// ---------------------------------------------------------------------------------------
// BigInteger and BigDecimal columns
// ---------------------------------------------------------------------------------------

/// `BigDecimal` cells of these unscaled integers and scales, `None` for null.
fn big_decimals(cells: &[Option<(i128, i32)>]) -> ArrayRef {
    use oxhaven_column::java_values::{big_decimal_cell, big_integer_cell};
    let cells: Vec<Option<Vec<u8>>> = cells
        .iter()
        .map(|cell| {
            cell.map(|(unscaled, scale)| big_decimal_cell(scale, &big_integer_cell(unscaled)))
        })
        .collect();
    Arc::new(arrow::array::BinaryArray::from_iter(cells))
}

fn big_integers(cells: &[Option<i128>]) -> ArrayRef {
    use oxhaven_column::java_values::big_integer_cell;
    let cells: Vec<Option<Vec<u8>>> = cells
        .iter()
        .map(|cell| cell.map(big_integer_cell))
        .collect();
    Arc::new(arrow::array::BinaryArray::from_iter(cells))
}

/// 1.5, 1.50, null, 2, -1.5, 10 with scale -1 (`1E+1`), 0.00, 1.6
fn decimals_table() -> Arc<dyn Table> {
    let x = big_decimals(&[
        Some((15, 1)),
        Some((150, 2)),
        None,
        Some((2, 0)),
        Some((-15, 1)),
        Some((1, -1)),
        Some((0, 2)),
        Some((16, 1)),
    ]);
    static_table(vec![("X", ColumnType::BigDecimal, x)])
}

/// Behavior: MatchFilter's `BigDecimal` convertor (`new BigDecimal(str)`) with
/// ObjectChunkMatchFilterFactory (`Objects.equals`, which for a `BigDecimal` compares the
/// scale too).
#[test]
fn a_big_decimal_match_compares_value_and_scale() {
    let t = decimals_table();
    assert_eq!(kept(&t, &["X == 1.5"]), [0]);
    assert_eq!(kept(&t, &["X == 1.50"]), [1]);
    assert_eq!(kept(&t, &["X in 1.50, 2, -1.5"]), [1, 3, 4]);
    assert_eq!(kept(&t, &["X == +2.0"]), [0i64; 0]);
    assert_eq!(kept(&t, &["X in +2"]), [3]);
    assert_eq!(kept(&t, &["X == 2.0"]), [0i64; 0]);
    assert_eq!(kept(&t, &["X in 1e1"]), [5]);
    assert_eq!(kept(&t, &["X == 10"]), [0i64; 0]);
    assert_eq!(kept(&t, &["X == 0.00"]), [6]);
    assert_eq!(kept(&t, &["X == -0.00"]), [6]);
    assert_eq!(kept(&t, &["X in 15E-1, .15e1"]), [0]);
    assert_eq!(kept(&t, &["X == null"]), [2]);
    assert_eq!(kept(&t, &["X != 1.5"]), [1, 2, 3, 4, 5, 6, 7]);
    assert_eq!(kept(&t, &["X not in 1.5, null"]), [1, 3, 4, 5, 6, 7]);
}

/// Behavior: RangeFilter#makeComparableRangeFilter, with ObjectComparisons and
/// `BigDecimal#compareTo`: by value, null lowest.
#[test]
fn a_big_decimal_range_compares_values() {
    let t = decimals_table();
    assert_eq!(kept(&t, &["X > 1.5"]), [3, 5, 7]);
    assert_eq!(kept(&t, &["X >= 1.500"]), [0, 1, 3, 5, 7]);
    assert_eq!(kept(&t, &["X < 0"]), [2, 4]);
    assert_eq!(kept(&t, &["X <= 0.0000"]), [2, 4, 6]);
    assert_eq!(kept(&t, &["X > 9.99999"]), [5]);
    assert_eq!(kept(&t, &["X < 10.0"]), [0, 1, 2, 3, 4, 6, 7]);
    assert_eq!(kept(&t, &["X > -1.6"]), [0, 1, 3, 4, 5, 6, 7]);
}

#[test]
fn a_big_integer_match_and_range_compare_values() {
    let y = big_integers(&[
        Some(5),
        None,
        Some(-5),
        Some(i128::from(i64::MAX) * 4),
        Some(0),
        Some(-129),
    ]);
    let t = static_table(vec![("Y", ColumnType::BigInteger, y)]);
    assert_eq!(kept(&t, &["Y == 5"]), [0]);
    assert_eq!(kept(&t, &["Y in +5"]), [0]);
    assert_eq!(kept(&t, &["Y == 0005"]), [0]);
    assert_eq!(kept(&t, &["Y == -0"]), [4]);
    assert_eq!(kept(&t, &["Y == -129"]), [5]);
    assert_eq!(kept(&t, &["Y == 36893488147419103228"]), [3]);
    assert_eq!(kept(&t, &["Y in null, -5"]), [1, 2]);
    assert_eq!(kept(&t, &["Y > 5"]), [3]);
    assert_eq!(kept(&t, &["Y < -5"]), [1, 5]);
    assert_eq!(kept(&t, &["Y >= -129"]), [0, 2, 3, 4, 5]);
}

#[test]
fn a_big_number_literal_that_java_cannot_read_is_refused() {
    let t = decimals_table();
    for filter in ["X == 1.5.1", "X == 1e", "X == 1e5.5", "X in 1, 2x"] {
        assert!(is_invalid(&refused(&t, &[filter])), "{filter}");
    }
    let y = big_integers(&[Some(5)]);
    let t = static_table(vec![("Y", ColumnType::BigInteger, y)]);
    for filter in ["Y in 1e3", "Y in +-1", "Y in 1.5"] {
        assert!(is_invalid(&refused(&t, &[filter])), "{filter}");
    }
}

/// Java compiles the formula of a match that does not fit the column (`Y == 1.5` becomes
/// the formula `Y == 1.5`, which compares as numbers), which the formula compiler does not
/// do for a big number yet.
#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.select.ConditionFilter#init")]
fn the_formula_of_a_big_number_filter_is_not_ported() {
    let y = big_integers(&[Some(5)]);
    let t = static_table(vec![("Y", ColumnType::BigInteger, y)]);
    let _ = unstructured_filter(&t, &["Y == 1.5"], &Compiler);
}

fn search_of(table: &Arc<dyn Table>, text: &str) -> Result<Vec<i64>, FilterError> {
    let condition = Condition::Search {
        search_string: text.to_string(),
        columns: vec![],
    };
    let result = filter(table, &[condition], &Compiler)?;
    Ok(result.row_set().iter().map(|key| key / 10).collect())
}

/// Behavior: WhereFilterFactory#createQuickFilterInternal: a `BigInteger` column is
/// matched with the text read as one, and a `BigDecimal` column takes the range the
/// plain decimal stands for, AbstractRangeFilter#makeBigDecimalRange; another text that
/// `new BigDecimal` reads fails the search.
#[test]
fn a_search_reads_big_numbers() {
    let t = decimals_table();
    // [1.5, 1.6)
    assert_eq!(search_of(&t, "1.5").unwrap(), [0, 1]);
    // [1.50, 1.51)
    assert_eq!(search_of(&t, "1.50").unwrap(), [0, 1]);
    // [1, 2)
    assert_eq!(search_of(&t, "1").unwrap(), [0, 1, 7]);
    assert_eq!(search_of(&t, "1.").unwrap(), [0, 1, 7]);
    // (-1.6, -1.5]
    assert_eq!(search_of(&t, "-1.5").unwrap(), [4]);
    // [0, 1), for -0 too
    assert_eq!(search_of(&t, "-0").unwrap(), [6]);
    // [9.9, 10.0)
    assert_eq!(search_of(&t, "9.9").unwrap(), [0i64; 0]);
    assert_eq!(search_of(&t, "10").unwrap(), [5]);
    assert_eq!(search_of(&t, "x").unwrap(), [0i64; 0]);
    for text in ["1e1", "+1", ".5", "1.5E0"] {
        assert!(
            matches!(search_of(&t, text), Err(FilterError::Invalid(_))),
            "{text}"
        );
    }

    let y = big_integers(&[Some(5), None, Some(-5)]);
    let t = static_table(vec![("Y", ColumnType::BigInteger, y)]);
    assert_eq!(search_of(&t, "5").unwrap(), [0]);
    assert_eq!(search_of(&t, "+5").unwrap(), [0]);
    assert_eq!(search_of(&t, "-5").unwrap(), [2]);
    assert_eq!(search_of(&t, "5.0").unwrap(), [0i64; 0]);
}

/// Each cycle checked against the filter made again from the parent's rows.
#[test]
fn big_number_filters_follow_a_ticking_parent() {
    let all: [&'static [&'static str]; 3] =
        [&["X > 1.5"], &["X in 1.50, null"], &["Y == 5", "X <= 2"]];
    for filters in all {
        let stepper = SteppedUpdateGraph::new();
        let graph: Arc<dyn UpdateGraph> = stepper.clone();
        let source = ScriptedTable::new(
            graph,
            definition(&[("X", ColumnType::BigDecimal), ("Y", ColumnType::BigInteger)]),
            RowSet::from_keys([0, 1, 2]),
            vec![
                big_decimals(&[Some((15, 1)), Some((150, 2)), None]),
                big_integers(&[Some(5), Some(5), None]),
            ],
        );
        let parent: Arc<dyn Table> = source.table();
        let operation =
            move |table: &Arc<dyn Table>| unstructured_filter(table, filters, &Compiler);
        let result = operation(&parent).unwrap();
        let (expected, recompute) = (parent.clone(), operation);
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            from_scratch(&expected, recompute)
        });
        checker.assert_consistent();
        let changes = [
            TableChange::new().add(
                [3, 4],
                vec![
                    big_decimals(&[Some((3, 0)), None]),
                    big_integers(&[Some(5), Some(6)]),
                ],
            ),
            TableChange::new().modify([0, 3], [(0, big_decimals(&[None, Some((150, 2))]))]),
            TableChange::new().remove([1]).modify(
                [2],
                [
                    (0, big_decimals(&[Some((2, 0))])),
                    (1, big_integers(&[Some(5)])),
                ],
            ),
            TableChange::new().shift(2, 4, 3),
        ];
        for change in changes {
            source.push(change);
            stepper.run_cycle();
            checker.assert_consistent();
        }
        assert!(!result.is_failed());
        assert!(checker.updates_seen() > 0, "{filters:?}");
    }
}
