//! The percentage slices, the restriction of sorting, and the attributes a sort leaves.
//! What is expected of a static table is what Java 42.9 answered to the same calls.

use super::testing::{blink_table, check_random, flat_table, operation, tags, Ticking};
use super::{
    head, head_pct, restrict_sort_to, reverse, slice_pct, sort, tail_pct, SortColumn, SortError,
};
use crate::ops::merge::flatten;
use crate::table::{attribute_map, sortable_columns, AttributeValue};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{ints, strings, TableChange};
use std::sync::Arc;

const LETTERS: [&str; 8] = ["a", "b", "c", "d", "e", "f", "g", "h"];

/// A flat static table of eight rows: `K` from 0 to 7 and `Tag` from `a` to `h`.
fn eight() -> Arc<dyn Table> {
    flat_table(&[
        ("K", ColumnType::Int, ints(0..8)),
        ("Tag", ColumnType::String, strings(LETTERS)),
    ])
}

/// The attributes of `table` as text: `Name` for a true Boolean, `Name=text` for a string
/// and `Name=<class>` for an object.
fn attributes(table: &Arc<dyn Table>) -> Vec<String> {
    attribute_map(&**table)
        .into_iter()
        .map(|(name, value)| match value {
            AttributeValue::Bool(true) => name.to_string(),
            AttributeValue::Bool(false) => format!("{name}=false"),
            AttributeValue::Text(text) => format!("{name}={text}"),
            AttributeValue::Object(class) => format!("{name}=<{class}>"),
        })
        .collect()
}

fn message(result: Result<Arc<dyn Table>, SortError>) -> SortError {
    match result {
        Err(error) => error,
        Ok(_) => panic!("expected a refusal"),
    }
}

// ---- percentage slices ---------------------------------------------------------------------

#[test]
fn a_share_of_a_static_table_is_the_rows_java_takes() {
    let table = eight();
    let first = |share: f64| tags(&*head_pct(&table, share).unwrap()).concat();
    let last = |share: f64| tags(&*tail_pct(&table, share).unwrap()).concat();
    let between = |start: f64, end: f64| tags(&*slice_pct(&table, start, end).unwrap()).concat();

    // the share of a head and of a tail is rounded up
    assert_eq!(first(0.5), "abcd");
    assert_eq!(first(0.125), "a");
    assert_eq!(first(0.126), "ab");
    assert_eq!(first(0.0), "");
    assert_eq!(first(1.0), "abcdefgh");
    assert_eq!(last(0.3), "fgh");
    assert_eq!(last(0.126), "gh");
    assert_eq!(last(1.0), "abcdefgh");
    // the bounds of a slice are rounded down
    assert_eq!(between(0.25, 0.8), "cdef");
    assert_eq!(between(0.0, 0.5), "abcd");
    assert_eq!(between(0.8, 0.2), "");
    assert_eq!(between(0.0, 1.0), "abcdefgh");
    // NaN is not outside 0 to 1, and is the position 0
    assert_eq!(first(f64::NAN), "");
    assert_eq!(last(f64::NAN), "");
    assert_eq!(between(f64::NAN, 0.5), "abcd");
    assert_eq!(between(0.5, f64::NAN), "");
}

#[test]
fn a_share_of_a_static_table_has_the_attributes_java_gives_it() {
    let table = eight();
    let first = head_pct(&table, 0.5).unwrap();
    assert!(first.is_flat());
    assert_eq!(attributes(&first), ["AddOnly", "AppendOnly"]);
    let last = tail_pct(&table, 0.3).unwrap();
    assert!(!last.is_flat());
    assert!(attributes(&last).is_empty());
    let inner = slice_pct(&table, 0.25, 0.8).unwrap();
    assert!(!inner.is_flat());
    assert_eq!(attributes(&inner), ["AppendOnly"]);
    let from_the_start = slice_pct(&table, 0.0, 0.5).unwrap();
    assert!(from_the_start.is_flat());
    assert_eq!(attributes(&from_the_start), ["AppendOnly"]);
}

#[test]
fn a_share_outside_zero_to_one_is_refused() {
    let table = eight();
    let illegal = |result| match message(result) {
        SortError::IllegalArgument(text) => text,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        illegal(head_pct(&table, 1.5)),
        "percentage of rows must be between [0,1]: percent=1.5"
    );
    assert_eq!(
        illegal(tail_pct(&table, -0.1)),
        "percentage of rows must be between [0,1]: percent=-0.1"
    );
    assert_eq!(
        illegal(head_pct(&table, 1e10)),
        "percentage of rows must be between [0,1]: percent=1.0E10"
    );
    assert_eq!(
        illegal(slice_pct(&table, -1.0, 0.5)),
        "Cannot slice with start percentage (-1.0) and end percentage (0.5), percentages \
         must be between [0, 1]"
    );
    assert_eq!(
        illegal(slice_pct(&table, 0.5, 2.0)),
        "Cannot slice with start percentage (0.5) and end percentage (2.0), percentages \
         must be between [0, 1]"
    );
}

#[test]
fn a_share_of_a_blink_table_is_refused_before_the_share_is_looked_at() {
    let graph = super::testing::graph();
    let (table, _blink) = blink_table(
        &graph,
        &[("K", ColumnType::Int), ("Tag", ColumnType::String)],
    );
    let unsupported = |result| match message(result) {
        SortError::Unsupported(text) => text,
        other => panic!("{other:?}"),
    };
    let text = |method: &str| {
        format!(
            "Blink tables do not support {method}; use BlinkTableTools.blinkToAppendOnly to \
             accumulate full history"
        )
    };
    assert_eq!(unsupported(head_pct(&table, 0.5)), text("headPct"));
    assert_eq!(unsupported(tail_pct(&table, 1.5)), text("tailPct"));
    assert_eq!(unsupported(slice_pct(&table, 0.1, 0.5)), text("slicePct"));
}

#[test]
fn a_share_follows_the_size_of_a_refreshing_table() {
    let columns = [
        ("K", ColumnType::Int, ints([1, 2, 3, 4])),
        ("Tag", ColumnType::String, strings(["a", "b", "c", "d"])),
    ];
    let half = Ticking::new([10, 20, 30, 40], &columns, |table| {
        head_pct(table, 0.5).unwrap()
    });
    assert_eq!(tags(&*half.result).concat(), "ab");
    // five rows: half of them, rounded up, is three
    let rows = vec![ints([5]), strings(["e"])];
    let update = half.step(TableChange::new().add([50], rows)).unwrap();
    assert_eq!(tags(&*half.result).concat(), "abc");
    assert_eq!(update.added.size(), 1);
    // three rows: two of them
    half.step(TableChange::new().remove([10, 20])).unwrap();
    assert_eq!(tags(&*half.result).concat(), "cd");
    // a row after the share changes nothing in it
    let rows = vec![ints([6]), strings(["f"])];
    assert!(half.step(TableChange::new().add([60], rows)).is_none());
    assert_eq!(tags(&*half.result).concat(), "cd");
}

const RANDOM_COLUMNS: [(&str, ColumnType); 3] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
];

#[test]
fn random_updates_through_shares() {
    let mut operations = Vec::new();
    for share in [0.0, 0.1, 0.34, 0.5, 0.99, 1.0] {
        operations.push(operation("head_pct", move |tables| {
            head_pct(&tables[0], share).unwrap()
        }));
        operations.push(operation("tail_pct", move |tables| {
            tail_pct(&tables[0], share).unwrap()
        }));
    }
    for (start, end) in [(0.0, 0.5), (0.2, 0.7), (0.5, 1.0), (0.9, 0.1), (0.0, 1.0)] {
        operations.push(operation("slice_pct", move |tables| {
            slice_pct(&tables[0], start, end).unwrap()
        }));
    }
    operations.push(operation("head_pct of a sort", |tables| {
        let sorted = sort(&tables[0], &[SortColumn::ascending("K")]).unwrap();
        head_pct(&sorted, 0.4).unwrap()
    }));
    operations.push(operation("a sort of tail_pct", |tables| {
        let last = tail_pct(&tables[0], 0.6).unwrap();
        sort(&last, &[SortColumn::descending("S")]).unwrap()
    }));
    operations.push(operation("slice_pct of a reverse", |tables| {
        slice_pct(&reverse(&tables[0]).unwrap(), 0.25, 0.75).unwrap()
    }));
    check_random(0..40, 30, 1, &RANDOM_COLUMNS, &operations);
}

// ---- the restriction of sorting ------------------------------------------------------------

/// A static table of the int columns `A`, `B`, `C` and `D`.
fn four_columns() -> Arc<dyn Table> {
    flat_table(&[
        ("A", ColumnType::Int, ints([2, 0, 1])),
        ("B", ColumnType::Int, ints([0, 2, 4])),
        ("C", ColumnType::Int, ints([0, 3, 6])),
        ("D", ColumnType::Int, ints([0, 4, 8])),
    ])
}

#[test]
fn a_restriction_is_a_copy_that_names_the_sortable_columns() {
    let table = four_columns();
    assert_eq!(sortable_columns(&*table), None);
    let restricted = restrict_sort_to(&table, &["B"]).unwrap();
    assert!(!Arc::ptr_eq(&restricted, &table));
    assert_eq!(attributes(&restricted), ["SortableColumns=B"]);
    assert_eq!(sortable_columns(&*restricted), Some(vec!["B".to_string()]));
    assert!(restricted.is_flat());
    assert_eq!(restricted.definition(), table.definition());

    let none: [&str; 0] = [];
    let unsortable = restrict_sort_to(&table, &none).unwrap();
    assert_eq!(attributes(&unsortable), ["SortableColumns="]);
    let two = restrict_sort_to(&table, &["D", "B"]).unwrap();
    assert_eq!(attributes(&two), ["SortableColumns=D,B"]);
    let twice = restrict_sort_to(&table, &["B", "B"]).unwrap();
    assert_eq!(attributes(&twice), ["SortableColumns=B,B"]);

    // the same restriction again is the table itself, another is another copy
    assert!(Arc::ptr_eq(
        &restrict_sort_to(&restricted, &["B"]).unwrap(),
        &restricted
    ));
    let other = restrict_sort_to(&restricted, &["A"]).unwrap();
    assert_eq!(attributes(&other), ["SortableColumns=A"]);

    assert_eq!(
        message(restrict_sort_to(&table, &["B", "Q", "Z"])),
        SortError::NoSuchColumn(vec!["Q".to_string(), "Z".to_string()])
    );
}

#[test]
fn a_sort_by_a_column_the_restriction_does_not_name_is_refused() {
    let restricted = restrict_sort_to(&four_columns(), &["B"]).unwrap();
    let refused = |columns: &[SortColumn]| match message(sort(&restricted, columns)) {
        SortError::NotSortable(text) => text,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        refused(&[SortColumn::ascending("A")]),
        "Sorting is not allowed on this table, but was attempted on: A but was attempted on: A"
    );
    assert_eq!(
        refused(&[SortColumn::descending("D")]),
        "Sorting is not allowed on this table, but was attempted on: D but was attempted on: D"
    );
    // every refused column, in the order of Java's set of them
    let columns = ["C", "A", "B", "D"].map(SortColumn::ascending);
    assert_eq!(
        refused(&columns),
        "Sorting is not allowed on this table, but was attempted on: A, C, D but was \
         attempted on: A, C, D"
    );
    // a sort by nothing is the table, and a sort by the named column is made
    assert!(Arc::ptr_eq(&sort(&restricted, &[]).unwrap(), &restricted));
    let sorted = sort(&restricted, &[SortColumn::descending("B")]).unwrap();
    assert_eq!(
        crate::ops::sort::testing::cells(&*sorted, 1),
        ["4", "2", "0"]
    );
}

#[test]
fn the_restriction_stays_with_the_operations_that_keep_it_in_java() {
    let restricted = restrict_sort_to(&four_columns(), &["B"]).unwrap();
    let sorted = sort(&restricted, &[SortColumn::descending("B")]).unwrap();
    assert_eq!(sortable_columns(&*sorted), Some(vec!["B".to_string()]));
    assert!(matches!(
        sort(&sorted, &[SortColumn::ascending("A")]),
        Err(SortError::NotSortable(_))
    ));
    let reversed = reverse(&restricted).unwrap();
    assert_eq!(attributes(&reversed), ["SortableColumns=B"]);
    let flat = flatten(&sorted).unwrap();
    assert_eq!(sortable_columns(&*flat), Some(vec!["B".to_string()]));
    // a slice is a table of its own
    assert_eq!(sortable_columns(&*head(&restricted, 2).unwrap()), None);
    assert_eq!(
        sortable_columns(&*head_pct(&restricted, 0.5).unwrap()),
        None
    );
}

// ---- the attributes of a sort --------------------------------------------------------------

#[test]
fn a_sort_leaves_the_attributes_java_leaves() {
    let table = four_columns();
    // rows out of order: Java names the result's redirection by its last column
    let sorted = sort(&table, &[SortColumn::ascending("A")]).unwrap();
    assert_eq!(
        attributes(&sorted),
        ["SortedColumns=A=Ascending", "SortRowRedirection=D"]
    );
    // rows in order already: a copy of the table
    let in_order = sort(&table, &[SortColumn::ascending("B")]).unwrap();
    assert_eq!(
        attributes(&in_order),
        [
            "SortedColumns=B=Ascending",
            "SortRowRedirection=+IDENTITY_REDIRECTION"
        ]
    );
    let descending = sort(&table, &[SortColumn::descending("C")]).unwrap();
    assert_eq!(
        attributes(&descending),
        ["SortedColumns=C=Descending", "SortRowRedirection=D"]
    );
    // a reverse keeps none of them, a slice only what it adds
    assert!(attributes(&reverse(&sorted).unwrap()).is_empty());
    assert_eq!(attributes(&head(&sorted, 2).unwrap()), ["AppendOnly"]);
}

#[test]
fn a_refreshing_sort_has_a_reverse_lookup() {
    let columns = [
        ("K", ColumnType::Int, ints([3, 1, 2])),
        ("Tag", ColumnType::String, strings(["a", "b", "c"])),
    ];
    let ticking = Ticking::new([10, 20, 30], &columns, |table| {
        sort(table, &[SortColumn::ascending("K")]).unwrap()
    });
    assert_eq!(
        attributes(&ticking.result),
        [
            "SortedColumns=K=Ascending",
            "SortRowRedirection=Tag",
            "SortReverseLookup=<java.util.function.LongUnaryOperator>"
        ]
    );
}
