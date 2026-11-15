//! What operations answer for a copy of a table: the sort of a static table that is
//! already in order, and the flattened form of a static table whose row set is flat.
//!
//! The attributes asserted here are what the Java server reports for the same operations:
//! `oracle/bin/harness fuzz` over flatten, select_distinct, sort and where, seeds 251, 625
//! and 1094, and the request cases of task 4.13.

use crate::ops::agg::{aggregate, Aggregation};
use crate::ops::distinct::select_distinct;
use crate::ops::filter::unstructured_filter;
use crate::ops::merge::flatten;
use crate::ops::select::{drop_columns, select, update, update_view, view};
use crate::ops::sort::{reverse, sort, SortColumn};
use crate::table::is_copy;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{definition, longs, strings, ReferenceTable};
use std::sync::Arc;

/// A static table with the keys `K` in the order given, at row keys with gaps.
fn table(keys: &[i64]) -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[("K", ColumnType::Long), ("S", ColumnType::String)]),
        RowSet::from_keys((0..keys.len() as i64).map(|position| position * 10)),
        vec![
            longs(keys.iter().copied()),
            strings(keys.iter().map(|_| "a")),
        ],
    )
}

fn by(column: &str) -> [SortColumn; 1] {
    [SortColumn::ascending(column)]
}

fn filtered(parent: &Arc<dyn Table>, filter: &str) -> Arc<dyn Table> {
    unstructured_filter(parent, &[filter], &Compiler).unwrap()
}

/// Whether `table` has the add-only and the append-only attribute.
fn add_and_append_only(table: &Arc<dyn Table>) -> (bool, bool) {
    let attributes = table.attributes();
    (attributes.add_only, attributes.append_only)
}

/// A `where` result: add-only and append-only, with a row set that is not flat.
fn in_order() -> Arc<dyn Table> {
    let parent = filtered(&table(&[1, 2, 3, 4]), "K > 1");
    assert_eq!(add_and_append_only(&parent), (true, true));
    assert!(!is_copy(&*parent));
    parent
}

#[test]
fn a_sort_that_changes_nothing_is_a_copy_with_the_attributes_of_its_parent() {
    let parent = in_order();
    let sorted = sort(&parent, &by("K")).unwrap();
    assert!(is_copy(&*sorted));
    assert!(!Arc::ptr_eq(&sorted, &parent));
    assert_eq!(sorted.row_set(), parent.row_set());
    assert_eq!(add_and_append_only(&sorted), (true, true));
    assert!(!sorted.is_flat());

    // a sort that moves rows is a table of its own
    let sorted = sort(&parent, &[SortColumn::descending("K")]).unwrap();
    assert!(!is_copy(&*sorted));
    assert_eq!(add_and_append_only(&sorted), (false, false));
}

#[test]
fn where_of_a_copy_is_not_add_only_when_java_memoizes_its_filters() {
    let parent = in_order();
    let copy = sort(&parent, &by("K")).unwrap();

    // a match filter is memoized: the answer is a copy without the attributes
    for filters in [&["K in 2, 3"][..], &["K == 2"], &["K != 2", "S == `a`"]] {
        let result = unstructured_filter(&copy, filters, &Compiler).unwrap();
        assert!(is_copy(&*result), "{filters:?}");
        assert_eq!(add_and_append_only(&result), (false, false), "{filters:?}");
        // of the table itself the result has them
        let result = unstructured_filter(&parent, filters, &Compiler).unwrap();
        assert_eq!(add_and_append_only(&result), (true, true), "{filters:?}");
    }
    // a range filter and a formula are not
    for filters in [&["K > 2"][..], &["!isNull(K)"], &["K == 2", "K + 0 > 1"]] {
        let result = unstructured_filter(&copy, filters, &Compiler).unwrap();
        assert!(!is_copy(&*result), "{filters:?}");
        assert_eq!(add_and_append_only(&result), (true, true), "{filters:?}");
    }
    // the rows are those of the filter either way
    let result = filtered(&copy, "K in 2, 3");
    assert_eq!(*result.row_set(), RowSet::from_keys([10, 20]));
}

#[test]
fn select_distinct_and_an_aggregation_of_a_copy_have_no_attributes() {
    let parent = in_order();
    let copy = sort(&parent, &by("K")).unwrap();
    let columns = ["K".to_string()];

    let distinct = select_distinct(&parent, &columns).unwrap();
    assert_eq!(add_and_append_only(&distinct), (true, true));
    let distinct = select_distinct(&copy, &columns).unwrap();
    assert!(is_copy(&*distinct));
    assert_eq!(add_and_append_only(&distinct), (false, false));
    assert!(distinct.is_flat());
    assert_eq!(*distinct.row_set(), RowSet::flat(3));

    let counted = aggregate(&copy, &[Aggregation::count("N")], false, &["S"]).unwrap();
    assert!(is_copy(&*counted));
}

#[test]
fn a_sort_of_a_copy_is_a_copy_without_attributes() {
    let parent = in_order();
    let copy = sort(
        &parent,
        &[SortColumn::ascending("S"), SortColumn::ascending("K")],
    )
    .unwrap();
    assert!(is_copy(&*copy));
    // by another column: the copy is in order already, and the answer is a copy of that
    let again = sort(
        &copy,
        &[SortColumn::ascending("K"), SortColumn::ascending("S")],
    )
    .unwrap();
    assert!(is_copy(&*again));
    assert_eq!(add_and_append_only(&again), (false, false));
    // a sort that moves rows is answered with a copy too, flat as the sort is
    let reordered = sort(&copy, &[SortColumn::descending("K")]).unwrap();
    assert!(is_copy(&*reordered));
    assert!(reordered.is_flat());
    // and what is asked of that copy is a copy again
    let result = filtered(&reordered, "K == 2");
    assert!(is_copy(&*result));
    assert_eq!(add_and_append_only(&result), (false, false));
}

#[test]
fn a_sort_by_the_one_column_a_table_is_sorted_by_is_the_table() {
    // a sort result is marked as sorted by its first column
    let parent = table(&[3, 1, 2]);
    let sorted = sort(&parent, &by("K")).unwrap();
    assert!(!is_copy(&*sorted));
    let again = sort(&sorted, &by("K")).unwrap();
    assert!(Arc::ptr_eq(&again, &sorted));
    // in the other direction, or by more columns, it is sorted
    let down = sort(&sorted, &[SortColumn::descending("K")]).unwrap();
    assert!(!Arc::ptr_eq(&down, &sorted));
    let more = sort(
        &sorted,
        &[SortColumn::ascending("K"), SortColumn::ascending("S")],
    )
    .unwrap();
    assert!(!Arc::ptr_eq(&more, &sorted));
    // which finds it in order: a copy, since the sort result has a row redirection
    assert!(is_copy(&*more));

    // `where` keeps the mark, so its result is add-only after the second sort too
    let kept = filtered(&sorted, "K > 1");
    let again = sort(&kept, &by("K")).unwrap();
    assert!(Arc::ptr_eq(&again, &kept));
    assert_eq!(
        add_and_append_only(&filtered(&again, "K == 2")),
        (true, true)
    );

    // the copy that a sort in order gives is marked, and with no row redirection, so a
    // sort that starts with the same column is the copy itself
    let copy = sort(&in_order(), &by("K")).unwrap();
    assert!(Arc::ptr_eq(&sort(&copy, &by("K")).unwrap(), &copy));
    let more = sort(
        &copy,
        &[SortColumn::ascending("K"), SortColumn::ascending("S")],
    )
    .unwrap();
    assert!(is_copy(&*more));
    assert_eq!(add_and_append_only(&more), (false, false));
}

#[test]
fn flatten_of_a_static_table_with_a_flat_row_set_is_a_flat_copy() {
    let source = ReferenceTable::new_static(
        definition(&[("K", ColumnType::Long)]),
        RowSet::flat(3),
        vec![longs([1, 2, 3])],
    );
    let source: Arc<dyn Table> = source;
    assert!(!source.is_flat());
    let flat = flatten(&source).unwrap();
    assert!(is_copy(&*flat));
    assert!(flat.is_flat());
    assert_eq!(
        add_and_append_only(&filtered(&flat, "K == 2")),
        (false, false)
    );
    assert_eq!(
        add_and_append_only(&filtered(&source, "K == 2")),
        (true, true)
    );
    // flat already: the table itself
    assert!(Arc::ptr_eq(&flatten(&flat).unwrap(), &flat));
}

#[test]
fn flatten_of_a_copy_keeps_append_only_and_drop_columns_keeps_both() {
    let copy = sort(&in_order(), &by("K")).unwrap();
    let flat = flatten(&copy).unwrap();
    assert!(is_copy(&*flat));
    assert!(flat.is_flat());
    assert_eq!(add_and_append_only(&flat), (false, true));

    let dropped = drop_columns(&copy, &["S"]).unwrap();
    assert!(is_copy(&*dropped));
    assert_eq!(add_and_append_only(&dropped), (true, true));

    let reversed = reverse(&copy).unwrap();
    assert!(is_copy(&*reversed));
    assert_eq!(add_and_append_only(&reversed), (false, false));
}

/// Recorded from Java 42.9 through `where(K in 1, 2)` of each result, which is add-only
/// unless the result is a copy: the results of `view(K, S)`, `update_view(S)`, `update(S)`,
/// `select(K)` and `select()` of a sort that changed nothing are copies, and the results of
/// `view(K, T = S)`, `view(K = K)`, `update_view(T = S)` and `update(T = S)` are not.
#[test]
fn the_formula_operations_answer_a_copy_with_a_copy_when_every_column_is_a_name() {
    let copy = sort(&in_order(), &by("K")).unwrap();
    assert!(is_copy(&*view(&copy, &["K", "S"], &Compiler).unwrap()));
    assert!(is_copy(&*update_view(&copy, &["S"], &Compiler).unwrap()));
    assert!(is_copy(&*select(&copy, &["K"], &Compiler).unwrap()));
    assert!(is_copy(&*select::<&str>(&copy, &[], &Compiler).unwrap()));
    assert!(is_copy(&*update(&copy, &["S"], &Compiler).unwrap()));
    // a column under another name, or under its own after an `=`, is not memoized
    assert!(!is_copy(&*view(&copy, &["K", "T = S"], &Compiler).unwrap()));
    assert!(!is_copy(&*view(&copy, &["K = K"], &Compiler).unwrap()));
    assert!(!is_copy(
        &*update_view(&copy, &["T = S"], &Compiler).unwrap()
    ));
    assert!(!is_copy(&*update(&copy, &["T = S"], &Compiler).unwrap()));
    // a formula is not memoized
    assert!(!is_copy(
        &*view(&copy, &["K", "T = K + 1"], &Compiler).unwrap()
    ));
    assert!(!is_copy(
        &*update(&copy, &["T = K + 1"], &Compiler).unwrap()
    ));
    assert!(!is_copy(
        &*update_view(&copy, &["T = 1"], &Compiler).unwrap()
    ));
}
