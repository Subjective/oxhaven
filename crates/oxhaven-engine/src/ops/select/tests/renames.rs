//! `rename_columns` and `move_columns`. What is expected of a static table is what Java
//! 42.9 answered to the same calls on a table of `A = i`, `B = i * 2`, `C = i * 3` and
//! `D = i * 4`.

use super::statics::{column, same_source};
use super::*;
use crate::ops::merge::merge;
use crate::ops::sort::testing::{blink_table, check_random, flat_table, operation, Ticking};
use crate::ops::sort::{restrict_sort_to, sort, SortColumn};
use crate::table::{attribute_map, sortable_columns, AttributeValue};
use oxhaven_contracts::update::ModifiedColumnSet;
use oxhaven_testkit::{ints, TableChange};

fn four_columns() -> Arc<dyn Table> {
    flat_table(&[
        ("A", ColumnType::Int, ints([0, 1, 2])),
        ("B", ColumnType::Int, ints([0, 2, 4])),
        ("C", ColumnType::Int, ints([0, 3, 6])),
        ("D", ColumnType::Int, ints([0, 4, 8])),
    ])
}

fn pairs(texts: &[&str]) -> Vec<ColumnPair> {
    texts
        .iter()
        .map(|text| ColumnPair::parse(text).unwrap())
        .collect()
}

fn renamed(table: &Arc<dyn Table>, texts: &[&str]) -> Arc<dyn Table> {
    rename_columns(table, &pairs(texts)).unwrap()
}

fn moved(table: &Arc<dyn Table>, index: i32, texts: &[&str]) -> Vec<String> {
    names(&*move_columns(table, index, &pairs(texts)).unwrap())
}

/// The row at position 1, which tells the columns apart.
fn second_row(table: &Arc<dyn Table>) -> Vec<String> {
    rendered(&**table)
        .into_iter()
        .map(|column| column[1].clone())
        .collect()
}

fn attribute_names(table: &Arc<dyn Table>) -> Vec<&'static str> {
    attribute_map(&**table)
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

fn sortable(table: &Arc<dyn Table>) -> Option<String> {
    attribute_map(&**table)
        .into_iter()
        .find(|(name, _)| *name == "SortableColumns")
        .map(|(_, value)| match value {
            AttributeValue::Text(text) => text,
            other => panic!("{other:?}"),
        })
}

#[test]
fn a_pair_is_an_output_and_an_input_or_one_name_for_both() {
    let pair = |input: &str, output: &str| ColumnPair {
        input: input.to_string(),
        output: output.to_string(),
    };
    assert_eq!(ColumnPair::parse("X = A").unwrap(), pair("A", "X"));
    assert_eq!(ColumnPair::parse("X=A").unwrap(), pair("A", "X"));
    assert_eq!(ColumnPair::parse(" A ").unwrap(), pair("A", "A"));
    let invalid = |text: &str| match ColumnPair::parse(text) {
        Err(SelectError::InvalidName(message)) => message,
        other => panic!("{text}: {other:?}"),
    };
    assert_eq!(invalid("1X = A"), "Invalid column name \"1X\"");
    assert_eq!(
        invalid("= A"),
        "Invalid column name: name can not be null or empty"
    );
    assert_eq!(
        invalid(""),
        "Invalid column name: name can not be null or empty"
    );
}

#[test]
fn a_renamed_column_stays_in_its_place_and_replaces_a_column_of_its_new_name() {
    let table = four_columns();
    let result = renamed(&table, &["X = A"]);
    assert_eq!(names(&*result), ["X", "B", "C", "D"]);
    assert_eq!(second_row(&result), ["1", "2", "3", "4"]);
    assert!(same_source(&column(&result, "X"), &column(&table, "A")));
    assert_eq!(result.row_set(), table.row_set());

    // the column that had the name is gone
    let result = renamed(&table, &["B = A"]);
    assert_eq!(names(&*result), ["B", "C", "D"]);
    assert_eq!(second_row(&result), ["1", "3", "4"]);
    let result = renamed(&table, &["A = C"]);
    assert_eq!(names(&*result), ["B", "A", "D"]);

    // two columns exchange their names
    let result = renamed(&table, &["A = B", "B = A"]);
    assert_eq!(names(&*result), ["B", "A", "C", "D"]);
    assert_eq!(second_row(&result), ["1", "2", "3", "4"]);

    // a column takes the name of a column that is renamed itself
    let result = renamed(&table, &["B = A", "C = B"]);
    assert_eq!(names(&*result), ["B", "C", "D"]);
    assert_eq!(second_row(&result), ["1", "2", "4"]);
}

#[test]
fn moved_columns_start_at_the_position_among_the_columns_that_stay() {
    let table = four_columns();
    assert_eq!(moved(&table, 2, &["D", "B"]), ["A", "C", "D", "B"]);
    assert_eq!(moved(&table, 1, &["A = B"]), ["C", "A", "D"]);
    // a position past the end is the end, and one before the start is the start
    assert_eq!(moved(&table, 9, &["A"]), ["B", "C", "D", "A"]);
    assert_eq!(moved(&table, -3, &["C"]), ["C", "A", "B", "D"]);

    let result = move_columns(&table, 1, &pairs(&["X = D"])).unwrap();
    assert_eq!(names(&*result), ["A", "X", "B", "C"]);
    assert_eq!(second_row(&result), ["1", "4", "2", "3"]);

    let up = |texts: &[&str]| names(&*move_columns_up(&table, &pairs(texts)).unwrap());
    let down = |texts: &[&str]| names(&*move_columns_down(&table, &pairs(texts)).unwrap());
    assert_eq!(up(&["C", "B"]), ["C", "B", "A", "D"]);
    assert_eq!(down(&["A", "B"]), ["C", "D", "A", "B"]);
    assert_eq!(down(&["D = A", "C"]), ["B", "D", "C"]);
    assert_eq!(down(&["D = A"]), ["B", "C", "D"]);
    assert_eq!(down(&["A = D"]), ["B", "C", "A"]);
}

#[test]
fn no_pairs_are_the_table_itself_and_a_column_renamed_to_itself_is_not() {
    let table = four_columns();
    assert!(Arc::ptr_eq(&rename_columns(&table, &[]).unwrap(), &table));
    assert!(Arc::ptr_eq(&move_columns(&table, 2, &[]).unwrap(), &table));
    assert!(Arc::ptr_eq(&move_columns_up(&table, &[]).unwrap(), &table));
    assert!(Arc::ptr_eq(
        &move_columns_down(&table, &[]).unwrap(),
        &table
    ));
    let same_names = renamed(&table, &["A = A"]);
    assert!(!Arc::ptr_eq(&same_names, &table));
    assert_eq!(names(&*same_names), ["A", "B", "C", "D"]);
}

#[test]
fn pairs_that_name_no_column_or_a_column_twice_are_refused_together() {
    let table = four_columns();
    let refused = |texts: &[&str]| match rename_columns(&table, &pairs(texts)) {
        Err(SelectError::IllegalArgument(message)) => message,
        other => panic!("{texts:?}: {:?}", other.map(|table| names(&*table))),
    };
    assert_eq!(refused(&["X = Q", "Y = Z"]), "Column(s) not found: Q, Z");
    assert_eq!(
        refused(&["X = A", "Y = A"]),
        "Duplicate source column(s): A"
    );
    assert_eq!(
        refused(&["X = A", "X = B"]),
        "Duplicate destination column(s): X"
    );
    assert_eq!(
        refused(&["X = Q", "Y = A", "Y = A"]),
        "Column(s) not found: Q\nDuplicate source column(s): A\nDuplicate destination \
         column(s): Y"
    );
    assert!(matches!(
        move_columns(&table, 0, &pairs(&["Q"])),
        Err(SelectError::IllegalArgument(_))
    ));
}

#[test]
fn a_rename_keeps_what_java_keeps_of_its_table() {
    let table = four_columns();
    // flat stays flat
    assert!(renamed(&table, &["X = A"]).is_flat());
    assert!(attribute_names(&renamed(&table, &["X = A"])).is_empty());
    // the marks of a sort are not kept
    let sorted = sort(&table, &[SortColumn::descending("A")]).unwrap();
    assert_eq!(
        attribute_names(&sorted),
        ["SortedColumns", "SortRowRedirection"]
    );
    assert!(attribute_names(&renamed(&sorted, &["X = A"])).is_empty());
    assert!(attribute_names(&move_columns_up(&sorted, &pairs(&["D"])).unwrap()).is_empty());
    // that of a merge is
    let merged = merge(&[table.clone(), table.clone()]).unwrap();
    assert_eq!(attribute_names(&merged), ["MergedTable"]);
    let result = renamed(&merged, &["X = A"]);
    assert_eq!(attribute_names(&result), ["MergedTable"]);
    assert!(!result.is_flat());
}

#[test]
fn a_rename_of_a_blink_table_is_a_blink_table() {
    let graph = crate::ops::sort::testing::graph();
    let (table, _blink) = blink_table(&graph, &[("K", ColumnType::Int)]);
    let result = renamed(&table, &["X = K"]);
    assert_eq!(attribute_names(&result), ["BlinkTable"]);
    assert!(result.is_refreshing());
}

#[test]
fn the_sortable_columns_follow_the_names() {
    let table = four_columns();
    for restricted in [
        // a copy, and a table that is not one
        restrict_sort_to(&table, &["B"]).unwrap(),
        sort(
            &restrict_sort_to(&table, &["B"]).unwrap(),
            &[SortColumn::descending("B")],
        )
        .unwrap(),
    ] {
        let after = |texts: &[&str]| sortable(&renamed(&restricted, texts));
        assert_eq!(after(&["X = B"]).as_deref(), Some("X"));
        // the sortable column is replaced by one that is not
        assert_eq!(after(&["B = A"]).as_deref(), Some(""));
        assert_eq!(after(&["A = B", "B = A"]).as_deref(), Some("A"));
        assert_eq!(after(&["X = A"]).as_deref(), Some("B"));
        let up = |texts: &[&str]| sortable(&move_columns_up(&restricted, &pairs(texts)).unwrap());
        assert_eq!(up(&["B"]).as_deref(), Some("B"));
        assert_eq!(up(&["X = B"]).as_deref(), Some("X"));
    }

    // several names come in the order of Java's set of them
    let two = restrict_sort_to(&table, &["D", "B"]).unwrap();
    let two = sort(&two, &[SortColumn::ascending("D")]).unwrap();
    assert_eq!(sortable(&renamed(&two, &["X = D"])).as_deref(), Some("B,X"));
    assert_eq!(
        sortable(&renamed(&two, &["Y = B", "X = D"])).as_deref(),
        Some("X,Y")
    );
    assert_eq!(
        sortable_columns(&*renamed(&two, &["X = D"])),
        Some(vec!["B".to_string(), "X".to_string()])
    );
}

#[test]
fn dropped_columns_leave_the_sortable_columns_of_a_table_that_is_not_a_copy() {
    let table = four_columns();
    let restricted = restrict_sort_to(&table, &["B"]).unwrap();
    let sorted = sort(&restricted, &[SortColumn::descending("B")]).unwrap();
    let after = |columns: &[&str]| sortable(&drop_columns(&sorted, columns).unwrap());
    assert_eq!(after(&["A"]).as_deref(), Some("B"));
    assert_eq!(after(&["B"]).as_deref(), Some(""));
    // dropping columns of a copy is answered from the table it is a copy of
    let of_copy = |columns: &[&str]| sortable(&drop_columns(&restricted, columns).unwrap());
    assert_eq!(of_copy(&["A"]), None);
    assert_eq!(of_copy(&["B"]), None);
}

#[test]
fn a_modified_column_is_the_column_it_was_renamed_or_moved_to() {
    let columns = [
        ("A", ColumnType::Int, ints([1, 2])),
        ("B", ColumnType::Int, ints([10, 20])),
        ("C", ColumnType::Int, ints([100, 200])),
    ];
    // A is renamed onto C, and B moves to the front: B, C (the old A)
    let ticking = Ticking::new([5, 9], &columns, |table| {
        let renamed = rename_columns(table, &pairs(&["C = A"])).unwrap();
        move_columns_up(&renamed, &pairs(&["B"])).unwrap()
    });
    assert_eq!(names(&*ticking.result), ["B", "C"]);
    assert!(ticking.result.is_refreshing());
    let modify = |column: usize| {
        ticking
            .step(TableChange::new().modify([5], [(column, ints([7]))]))
            .map(|update| update.modified_columns)
    };
    assert_eq!(modify(0), Some(ModifiedColumnSet::of([1])));
    assert_eq!(modify(1), Some(ModifiedColumnSet::of([0])));
    // the column that was replaced is in no column of the result
    assert_eq!(modify(2), None);
    assert_eq!(rendered(&*ticking.result), [["7", "20"], ["7", "2"]]);
}

const RANDOM_COLUMNS: [(&str, ColumnType); 4] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
];

#[test]
fn random_updates_through_renames_and_moves() {
    let rename = |name: &'static str, texts: &'static [&'static str]| {
        operation(name, move |tables| renamed(&tables[0], texts))
    };
    let operations = [
        rename("one", &["X = K"]),
        rename("onto another", &["D = K"]),
        rename("exchange", &["K = S", "S = K"]),
        rename("chain", &["D = K", "S = D", "X = B"]),
        operation("move", |tables| {
            move_columns(&tables[0], 2, &pairs(&["B", "X = K"])).unwrap()
        }),
        operation("up", |tables| {
            move_columns_up(&tables[0], &pairs(&["S", "B"])).unwrap()
        }),
        operation("down", |tables| {
            move_columns_down(&tables[0], &pairs(&["K = D", "S"])).unwrap()
        }),
        operation("rename of a sort", |tables| {
            let sorted = sort(&tables[0], &[SortColumn::ascending("K")]).unwrap();
            renamed(&sorted, &["Key = K"])
        }),
        operation("sort of a rename", |tables| {
            let renamed = renamed(&tables[0], &["Key = K", "K = S"]);
            sort(&renamed, &[SortColumn::descending("Key")]).unwrap()
        }),
        operation("update of a move", |tables| {
            let moved = move_columns_down(&tables[0], &pairs(&["N = K"])).unwrap();
            update(&moved, &["M = N + 1"], &Compiler).unwrap()
        }),
    ];
    check_random(0..40, 30, 1, &RANDOM_COLUMNS, &operations);
}
