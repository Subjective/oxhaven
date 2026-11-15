//! Which columns a static `select` passes through, and so whether its result keeps the row
//! keys of its parent or is flat.
//!
//! Each assertion is what the Java server reports as the `IsFlat` attribute for the same
//! operations on static tables: `oracle/bin/harness fuzz` seed 2997 over the default
//! operations, and the request cases of task 4.15.

use crate::ops::agg::{aggregate, AggSpec, Aggregation, Pair};
use crate::ops::asof::{as_of_join, AsOfMatch, AsOfRule};
use crate::ops::crossjoin::cross_join;
use crate::ops::distinct::select_distinct;
use crate::ops::filter::unstructured_filter;
use crate::ops::join::{exact_join, natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use crate::ops::range_join::{range_join, RangeAggregation, RangeJoinMatch};
use crate::ops::select::{select, update};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{definition, longs, strings, ReferenceTable};
use std::sync::Arc;

/// A static table that is not flat: the rows of `K` 1, 3, 4 and 7 at the row keys 0, 2, 3
/// and 4, with `S` and a range from `K` to `H`.
fn left() -> Arc<dyn Table> {
    let table: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&[("K", ColumnType::Long), ("S", ColumnType::String)]),
        RowSet::flat(5),
        vec![longs([1, 2, 3, 4, 7]), strings(["a", "a", "b", "c", "c"])],
    );
    let wide = update(&table, &["H = K + 1"], &Compiler).unwrap();
    filtered(&wide, "K != 2")
}

/// A static table with one row for each `K` of [`left`].
fn right() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[("K", ColumnType::Long), ("P", ColumnType::Long)]),
        RowSet::flat(5),
        vec![longs([1, 2, 3, 4, 7]), longs([1, 2, 3, 4, 7])],
    )
}

fn filtered(parent: &Arc<dyn Table>, filter: &str) -> Arc<dyn Table> {
    unstructured_filter(parent, &[filter], &Compiler).unwrap()
}

/// Whether the `select` of `column` alone from `parent`, which is not flat, is flat: it
/// is when `select` copies the column, and is not when it passes the column through.
fn select_is_flat(parent: &Arc<dyn Table>, column: &str) -> bool {
    assert!(!parent.is_flat() && !parent.is_refreshing());
    assert_ne!(
        *parent.row_set(),
        RowSet::flat(parent.row_set().size()),
        "the test needs a parent whose row keys are not 0..size"
    );
    let formula = format!("A = {column}");
    select(parent, &[formula], &Compiler).unwrap().is_flat()
}

#[test]
fn a_select_copies_the_columns_of_a_cross_join() {
    let add = [JoinAddition::same("P")];
    let keyed = cross_join(&left(), &right(), &[JoinMatch::same("K")], &add, 10).unwrap();
    assert!(select_is_flat(&keyed, "K"));
    assert!(select_is_flat(&keyed, "P"));
    let every = cross_join(&left(), &right(), &[], &add, 10).unwrap();
    assert!(select_is_flat(&every, "S"));
    assert!(select_is_flat(&every, "P"));
}

#[test]
fn a_select_copies_the_columns_a_join_adds_and_passes_the_left_columns_through() {
    let on = [JoinMatch::same("K")];
    let add = [JoinAddition::same("P")];
    let natural = natural_join(
        &left(),
        &right(),
        &on,
        &add,
        NaturalJoinType::ErrorOnDuplicate,
    );
    let exact = exact_join(&left(), &right(), &on, &add);
    for joined in [natural.unwrap(), exact.unwrap()] {
        assert!(select_is_flat(&joined, "P"));
        assert!(!select_is_flat(&joined, "K"));
        // the same columns behind a filter
        let rest = filtered(&joined, "K > 1");
        assert!(select_is_flat(&rest, "P"));
        assert!(!select_is_flat(&rest, "K"));
    }

    for rule in [AsOfRule::GreaterThanEqual, AsOfRule::LessThanEqual] {
        let stamps = AsOfMatch::new("K", rule, "K");
        let joined = as_of_join(&left(), &right(), &[], &stamps, &add).unwrap();
        assert!(select_is_flat(&joined, "P"));
        assert!(!select_is_flat(&joined, "K"));
    }

    let range = RangeJoinMatch::parse("K <= P <= H").unwrap();
    let aggregations = [RangeAggregation::group([Pair::of("P")])];
    let ranged = range_join(&left(), &right(), &[], &range, &aggregations).unwrap();
    assert!(select_is_flat(&ranged, "P"));
    assert!(!select_is_flat(&ranged, "K"));
}

#[test]
fn a_select_copies_the_keys_of_a_static_aggregation_and_the_columns_that_name_rows() {
    let by = ["S"];
    let rest_of = |aggregation: Aggregation| {
        let aggregated = aggregate(&left(), &[aggregation], false, &by).unwrap();
        filtered(&aggregated, "S != `a`")
    };
    let of = |spec: AggSpec| rest_of(Aggregation::columns(spec, [Pair::new("P", "K")]));

    // The key columns read the keys where the aggregation keeps them.
    assert!(select_is_flat(&of(AggSpec::Sum), "S"));
    assert!(select_is_flat(&rest_of(Aggregation::count("P")), "S"));
    let distinct = select_distinct(&left(), &["S".to_string()]).unwrap();
    assert!(select_is_flat(&filtered(&distinct, "S != `a`"), "S"));

    // These read a row of the parent, or its rows.
    let order = || vec!["H".to_string()];
    for spec in [
        AggSpec::First,
        AggSpec::Last,
        AggSpec::SortedFirst { columns: order() },
        AggSpec::SortedLast { columns: order() },
        AggSpec::Group,
        AggSpec::Distinct {
            include_nulls: false,
        },
    ] {
        assert!(select_is_flat(&of(spec.clone()), "P"), "{spec:?}");
    }

    // These hold a value for each group.
    for spec in [
        AggSpec::Sum,
        AggSpec::Avg,
        AggSpec::Min,
        AggSpec::Max,
        AggSpec::Std,
        AggSpec::Median {
            average_evenly_divided: true,
        },
        AggSpec::CountDistinct { count_nulls: false },
    ] {
        assert!(!select_is_flat(&of(spec.clone()), "P"), "{spec:?}");
    }
    assert!(!select_is_flat(&rest_of(Aggregation::count("P")), "P"));
}
