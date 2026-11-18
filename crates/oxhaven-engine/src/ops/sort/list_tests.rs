//! Sorts by vector and array columns. Every expected order was seen on Deephaven 42.9,
//! the vectors made by `group_by` (an empty one by `subVector(0, 0)`), the arrays by
//! `toArray()` of them.

use super::testing::{check_random, operation, static_table, tags};
use super::{sort, SortColumn, SortError};
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, StringArray, UInt16Array,
};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{list_of, strings};
use std::sync::Arc;

const LETTERS: [&str; 7] = ["a", "b", "c", "d", "e", "f", "g"];

/// A static table of the list column `K` and the letters `a`, `b`, ... in `Tag`.
fn tagged(
    column_type: ColumnType,
    lengths: &[Option<usize>],
    elements: ArrayRef,
) -> Arc<dyn Table> {
    let values = list_of(&column_type, lengths.iter().copied(), elements);
    let letters = strings(LETTERS[..lengths.len()].iter().copied());
    static_table(&[
        ("K", column_type, values),
        ("Tag", ColumnType::String, letters),
    ])
}

fn sorted(table: &Arc<dyn Table>, column: SortColumn) -> String {
    tags(&*sort(table, &[column]).unwrap()).concat()
}

fn both(table: &Arc<dyn Table>) -> (String, String) {
    (
        sorted(table, SortColumn::ascending("K")),
        sorted(table, SortColumn::descending("K")),
    )
}

/// `[3, 1] [3] [] [3, 1, 0] [-2] [3, null] [3, 1]`, as a vector and as an array.
#[test]
fn int_lists_sort_element_by_element_then_shorter_first() {
    let lengths = [
        Some(2),
        Some(1),
        Some(0),
        Some(3),
        Some(1),
        Some(2),
        Some(2),
    ];
    let elements = || -> ArrayRef {
        Arc::new(Int32Array::from(vec![
            Some(3),
            Some(1),
            Some(3),
            Some(3),
            Some(1),
            Some(0),
            Some(-2),
            Some(3),
            None,
            Some(3),
            Some(1),
        ]))
    };
    for column_type in [
        ColumnType::vector(ColumnType::Int),
        ColumnType::array(ColumnType::Int),
    ] {
        let table = tagged(column_type, &lengths, elements());
        // Descending negates the comparison: the two `[3, 1]` stay in parent order.
        assert_eq!(both(&table), ("cebfagd".to_string(), "dagfbec".to_string()));
    }
}

/// `[0.0, NaN] [-0.0] [1.0] [2.0, 3.0, 4.0] [5.0] [6.0, 7.0] [null, -1.0]`: null first,
/// `-0.0` equal to `0.0`, NaN last.
#[test]
fn double_lists_sort_with_null_first_one_zero_and_nan_last() {
    let lengths = [
        Some(2),
        Some(1),
        Some(1),
        Some(3),
        Some(1),
        Some(2),
        Some(2),
    ];
    let elements = || -> ArrayRef {
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(f64::NAN),
            Some(-0.0),
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(4.0),
            Some(5.0),
            Some(6.0),
            Some(7.0),
            None,
            Some(-1.0),
        ]))
    };
    for column_type in [
        ColumnType::vector(ColumnType::Double),
        ColumnType::array(ColumnType::Double),
    ] {
        let table = tagged(column_type, &lengths, elements());
        assert_eq!(both(&table), ("gbacdef".to_string(), "fedcabg".to_string()));
    }
}

/// `[b, a] [b] [z] [b, null, c] [A] [b, U+E000] [U+1F600, b]`: strings by UTF-16 code
/// unit, so the character above U+FFFF before U+E000 and both after `z`.
#[test]
fn string_lists_sort_by_utf16_code_units() {
    let lengths = [
        Some(2),
        Some(1),
        Some(1),
        Some(3),
        Some(1),
        Some(2),
        Some(2),
    ];
    let elements = || -> ArrayRef {
        Arc::new(StringArray::from(vec![
            Some("b"),
            Some("a"),
            Some("b"),
            Some("z"),
            Some("b"),
            None,
            Some("c"),
            Some("A"),
            Some("b"),
            Some("\u{E000}"),
            Some("\u{1F600}"),
            Some("b"),
        ]))
    };
    for column_type in [
        ColumnType::vector(ColumnType::String),
        ColumnType::array(ColumnType::String),
    ] {
        let table = tagged(column_type, &lengths, elements());
        assert_eq!(both(&table), ("ebdafcg".to_string(), "gcfadbe".to_string()));
    }
}

/// `[b, a] [b] [z] [b, null, c] [A] [b, x] [y, b]`: a null `char` first, although its
/// sentinel is the largest code unit (`CharArrayComparator`, `CharComparisons`).
#[test]
fn char_lists_sort_with_null_first() {
    let lengths = [
        Some(2),
        Some(1),
        Some(1),
        Some(3),
        Some(1),
        Some(2),
        Some(2),
    ];
    let elements = || -> ArrayRef {
        Arc::new(UInt16Array::from(
            [
                Some('b'),
                Some('a'),
                Some('b'),
                Some('z'),
                Some('b'),
                None,
                Some('c'),
                Some('A'),
                Some('b'),
                Some('x'),
                Some('y'),
                Some('b'),
            ]
            .map(|c| c.map(|c| c as u16))
            .to_vec(),
        ))
    };
    for column_type in [
        ColumnType::vector(ColumnType::Char),
        ColumnType::array(ColumnType::Char),
    ] {
        let table = tagged(column_type, &lengths, elements());
        assert_eq!(both(&table), ("ebdafgc".to_string(), "cgfadbe".to_string()));
    }
}

/// `[5] [1] null [2]` as vectors, and `[5] null null [2]` as arrays: a null list first,
/// last descending.
#[test]
fn a_null_list_is_first() {
    let vectors = tagged(
        ColumnType::vector(ColumnType::Int),
        &[Some(1), Some(1), None, Some(1)],
        Arc::new(Int32Array::from(vec![5, 1, 2])),
    );
    assert_eq!(both(&vectors), ("cbda".to_string(), "adbc".to_string()));
    let arrays = tagged(
        ColumnType::array(ColumnType::Int),
        &[Some(1), None, None, Some(1)],
        Arc::new(Int32Array::from(vec![5, 2])),
    );
    assert_eq!(both(&arrays), ("bcda".to_string(), "adbc".to_string()));
}

/// Vectors of vectors, of Booleans, and arrays of dates.
#[test]
fn nested_vectors_booleans_and_dates_sort_by_their_elements() {
    // [[3, 1], [3, 1, 0], [3, null]] and [[3], [9], [-2], [3, 1]]
    let inner = list_of(
        &ColumnType::vector(ColumnType::Int),
        [
            Some(2),
            Some(3),
            Some(2),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
        ],
        Arc::new(Int32Array::from(vec![
            Some(3),
            Some(1),
            Some(3),
            Some(1),
            Some(0),
            Some(3),
            None,
            Some(3),
            Some(9),
            Some(-2),
            Some(3),
            Some(1),
        ])),
    );
    let nested = tagged(
        ColumnType::vector(ColumnType::vector(ColumnType::Int)),
        &[Some(3), Some(4)],
        inner,
    );
    assert_eq!(sorted(&nested, SortColumn::ascending("K")), "ba");

    // [true, null] and [false, false]
    let booleans = tagged(
        ColumnType::vector(ColumnType::Boolean),
        &[Some(2), Some(2)],
        Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(false),
        ])),
    );
    assert_eq!(sorted(&booleans, SortColumn::ascending("K")), "ba");

    // [2020-01-01] [2020-01-02] [2020-01-03], descending
    let dates = tagged(
        ColumnType::array(ColumnType::LocalDate),
        &[Some(1), Some(1), Some(1)],
        Arc::new(Date32Array::from(vec![18262, 18263, 18264])),
    );
    assert_eq!(sorted(&dates, SortColumn::descending("K")), "cba");
}

/// Java's `NotSortableColumnException` for arrays of a class `ComparatorRegistry` has no
/// comparator for.
#[test]
fn arrays_without_a_comparator_are_not_sortable() {
    for (column_type, elements, class) in [
        (
            ColumnType::array(ColumnType::Boolean),
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            "class [Ljava.lang.Boolean;",
        ),
        (
            ColumnType::array(ColumnType::PrimitiveBoolean),
            Arc::new(BooleanArray::from(vec![true])),
            "class [Z",
        ),
        (
            ColumnType::array(ColumnType::DayOfWeek),
            Arc::new(arrow::array::Int8Array::from(vec![1])),
            "class [Ljava.time.DayOfWeek;",
        ),
        (
            ColumnType::array(ColumnType::array(ColumnType::Int)),
            list_of(
                &ColumnType::array(ColumnType::Int),
                [Some(1)],
                Arc::new(Int32Array::from(vec![1])),
            ),
            "class [[I",
        ),
    ] {
        let table = tagged(column_type, &[Some(1)], elements);
        match sort(&table, &[SortColumn::ascending("K")]) {
            Err(SortError::NotSortableColumn(message)) => {
                assert_eq!(message, format!("K is not a sortable type: {class}"));
            }
            other => panic!("expected an error, found {:?}", other.map(|_| ())),
        }
    }
}

/// Java fails with a `ClassCastException` once two periods of a vector meet.
#[test]
#[should_panic(expected = "unported: io.deephaven.vector.ObjectVector#compareTo")]
fn a_sort_by_vectors_of_periods_is_unported() {
    let periods = arrow::array::new_null_array(&ColumnType::Period.arrow_type(), 1);
    let table = tagged(ColumnType::vector(ColumnType::Period), &[Some(1)], periods);
    let _ = sort(&table, &[SortColumn::ascending("K")]);
}

#[test]
fn random_updates_through_sorts_by_vectors() {
    let operations = [
        operation("X", |tables| {
            sort(&tables[0], &[SortColumn::ascending("X")]).unwrap()
        }),
        operation("X desc, K", |tables| {
            let columns = [SortColumn::descending("X"), SortColumn::ascending("K")];
            sort(&tables[0], &columns).unwrap()
        }),
        operation("A desc", |tables| {
            sort(&tables[0], &[SortColumn::descending("A")]).unwrap()
        }),
    ];
    let columns = [
        ("K", ColumnType::Int),
        ("X", ColumnType::vector(ColumnType::Int)),
        ("A", ColumnType::array(ColumnType::String)),
    ];
    check_random(0..40, 30, 1, &columns, &operations);
}
