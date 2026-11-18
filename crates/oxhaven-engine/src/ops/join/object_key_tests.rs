//! `BigDecimal`, `BigInteger` and vector columns as the keys of joins, `where_in` and
//! `select_distinct`, and as the stamps of as-of joins. Every expected value was seen on
//! Deephaven 42.9: decimals made by `new java.math.BigDecimal(String)`, vectors by
//! `group_by` (an empty one by `subVector(0, 0)`).

use super::{exact_join, natural_join, JoinAddition, JoinError, JoinMatch, NaturalJoinType};
use crate::ops::asof::{aj, raj, AsOfMatch, AsOfRule};
use crate::ops::crossjoin::{cross_join, DEFAULT_RESERVE_BITS};
use crate::ops::distinct::{select_distinct, where_in, where_not_in};
use crate::ops::sort::testing::static_table;
use arrow::array::{ArrayRef, AsArray, BinaryArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Int64Type};
use oxhaven_column::java_values::{big_decimal_cell, big_integer_cell};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{list_of, TableSnapshot};
use std::sync::Arc;

/// The cell of `text` as `new BigDecimal(text)` reads it: digits with an optional point.
fn decimal(text: &str) -> Vec<u8> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let unscaled: i128 = format!("{whole}{fraction}").parse().expect("digits");
    big_decimal_cell(fraction.len() as i32, &unscaled.to_be_bytes())
}

fn decimals(texts: &[Option<&str>]) -> ArrayRef {
    let cells: Vec<Option<Vec<u8>>> = texts.iter().map(|t| t.map(decimal)).collect();
    Arc::new(cells.iter().map(|c| c.as_deref()).collect::<BinaryArray>())
}

fn integers(values: &[Option<i128>]) -> ArrayRef {
    let cells: Vec<Option<Vec<u8>>> = values.iter().map(|v| v.map(big_integer_cell)).collect();
    Arc::new(cells.iter().map(|c| c.as_deref()).collect::<BinaryArray>())
}

fn ints(values: &[i32]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

/// A vector column of `int` whose cells have the `lengths` (`None` a null vector).
fn int_vectors(lengths: &[Option<usize>], elements: &[i32]) -> ArrayRef {
    list_of(
        &ColumnType::vector(ColumnType::Int),
        lengths.iter().copied(),
        ints(elements),
    )
}

/// The `int` column `name` of `table`, `None` for null.
fn column(table: &Arc<dyn Table>, name: &str) -> Vec<Option<i64>> {
    let index = table.definition().index_of(name).expect("a column");
    let snapshot = TableSnapshot::current(&**table).expect("a table");
    let values = arrow::compute::cast(&snapshot.columns[index], &DataType::Int64).unwrap();
    values.as_primitive::<Int64Type>().iter().collect()
}

fn on(name: &str) -> Vec<JoinMatch> {
    vec![JoinMatch::same(name)]
}

fn add(name: &str) -> Vec<JoinAddition> {
    vec![JoinAddition::same(name)]
}

/// The probe's tables: `L` keyed `3, 3.000, 3.0, null, 1.5, 1.50` (`I` = 0 to 5) and `R`
/// keyed `3, 3.00, null, 1.50, 1.5` (`V` = 0, 10 to 40).
fn decimal_tables() -> (Arc<dyn Table>, Arc<dyn Table>) {
    let left = static_table(&[
        ("I", ColumnType::Int, ints(&[0, 1, 2, 3, 4, 5])),
        (
            "K",
            ColumnType::BigDecimal,
            decimals(&[
                Some("3"),
                Some("3.000"),
                Some("3.0"),
                None,
                Some("1.5"),
                Some("1.50"),
            ]),
        ),
    ]);
    let right = static_table(&[
        ("V", ColumnType::Int, ints(&[0, 10, 20, 30, 40])),
        (
            "K",
            ColumnType::BigDecimal,
            decimals(&[Some("3"), Some("3.00"), None, Some("1.50"), Some("1.5")]),
        ),
    ]);
    (left, right)
}

/// Behavior: java.math.BigDecimal#equals: `3`, `3.0` and `3.000` are three keys.
#[test]
fn big_decimal_keys_join_by_value_and_scale() {
    let (left, right) = decimal_tables();
    let natural = natural_join(
        &left,
        &right,
        &on("K"),
        &add("V"),
        NaturalJoinType::ErrorOnDuplicate,
    )
    .unwrap();
    assert_eq!(
        column(&natural, "V"),
        [Some(0), None, None, Some(20), Some(40), Some(30)]
    );

    let cross = cross_join(&left, &right, &on("K"), &add("V"), DEFAULT_RESERVE_BITS).unwrap();
    assert_eq!(column(&cross, "I"), [Some(0), Some(3), Some(4), Some(5)]);
    assert_eq!(column(&cross, "V"), [Some(0), Some(20), Some(40), Some(30)]);

    // Java: "Tables don't have one-to-one mapping - no mappings for key 3.000."
    let without_null = static_table(&[
        ("V", ColumnType::Int, ints(&[0, 10, 30, 40])),
        (
            "K",
            ColumnType::BigDecimal,
            decimals(&[Some("3"), Some("3.00"), Some("1.50"), Some("1.5")]),
        ),
    ]);
    match exact_join(&left, &without_null, &on("K"), &add("V")) {
        Err(JoinError::NoRightRow(message)) => assert!(message.contains("3.000"), "{message}"),
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("the exact join succeeded"),
    }
}

#[test]
fn big_decimal_keys_are_distinct_and_matched_by_value_and_scale() {
    let (left, right) = decimal_tables();
    let distinct = select_distinct(&left, &["K".to_string()]).unwrap();
    let snapshot = TableSnapshot::current(&*distinct).unwrap();
    assert_eq!(
        snapshot.columns[0].as_ref(),
        decimals(&[
            Some("3"),
            Some("3.000"),
            Some("3.0"),
            None,
            Some("1.5"),
            Some("1.50"),
        ])
        .as_ref()
    );

    let kept = where_in(&left, &right, &on("K")).unwrap();
    assert_eq!(column(&kept, "I"), [Some(0), Some(3), Some(4), Some(5)]);
    let others = where_not_in(&left, &right, &on("K")).unwrap();
    assert_eq!(column(&others, "I"), [Some(1), Some(2)]);
}

#[test]
fn big_integer_keys_join_by_value() {
    let table = static_table(&[
        ("I", ColumnType::Int, ints(&[0, 1, 2, 3])),
        (
            "K",
            ColumnType::BigInteger,
            integers(&[Some(0), Some(1), None, Some(1)]),
        ),
    ]);
    let firsts = static_table(&[
        ("J", ColumnType::Int, ints(&[0, 1, 2])),
        (
            "K",
            ColumnType::BigInteger,
            integers(&[Some(0), Some(1), None]),
        ),
    ]);
    let natural = natural_join(
        &table,
        &firsts,
        &on("K"),
        &add("J"),
        NaturalJoinType::ErrorOnDuplicate,
    )
    .unwrap();
    assert_eq!(column(&natural, "J"), [Some(0), Some(1), Some(2), Some(1)]);
}

/// `G` 0 to 5 with the vectors `[1, 2] [1] [1, 2] null [1, 2] []`, and a right table of
/// the rows 1 to 5 with `V` = 10 to 50.
fn vector_tables() -> (Arc<dyn Table>, Arc<dyn Table>) {
    let left = static_table(&[
        ("G", ColumnType::Int, ints(&[0, 1, 2, 3, 4, 5])),
        (
            "X",
            ColumnType::vector(ColumnType::Int),
            int_vectors(
                &[Some(2), Some(1), Some(2), None, Some(2), Some(0)],
                &[1, 2, 1, 1, 2, 1, 2],
            ),
        ),
        (
            "S",
            ColumnType::vector(ColumnType::String),
            list_of(
                &ColumnType::vector(ColumnType::String),
                [Some(2), Some(1), Some(2), Some(1), Some(2), Some(1)],
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    None,
                    Some("a"),
                    Some("b"),
                    Some("c"),
                ])),
            ),
        ),
    ]);
    let right = static_table(&[
        (
            "X",
            ColumnType::vector(ColumnType::Int),
            int_vectors(
                &[Some(1), Some(2), None, Some(2), Some(0)],
                &[1, 1, 2, 1, 2],
            ),
        ),
        ("V", ColumnType::Int, ints(&[10, 20, 30, 40, 50])),
    ]);
    (left, right)
}

/// Behavior: io.deephaven.vector.IntVector#equals: equal elements are one key, a null
/// vector matches a null one and an empty one an empty one.
#[test]
fn vector_keys_join_by_their_elements() {
    let (left, right) = vector_tables();
    let natural = natural_join(
        &left,
        &right,
        &on("X"),
        &add("V"),
        NaturalJoinType::FirstMatch,
    )
    .unwrap();
    assert_eq!(
        column(&natural, "V"),
        [Some(20), Some(10), Some(20), Some(30), Some(20), Some(50)]
    );
    let cross = cross_join(&left, &right, &on("X"), &add("V"), DEFAULT_RESERVE_BITS).unwrap();
    assert_eq!(column(&cross, "G"), [0, 0, 1, 2, 2, 3, 4, 4, 5].map(Some));
    assert_eq!(
        column(&cross, "V"),
        [20, 40, 10, 20, 40, 30, 20, 40, 50].map(Some)
    );
    let kept = where_in(&left, &right, &on("X")).unwrap();
    assert_eq!(column(&kept, "G"), (0..6).map(Some).collect::<Vec<_>>());
    let others = where_not_in(&left, &right, &on("X")).unwrap();
    assert_eq!(column(&others, "G"), []);

    // A vector of strings keys by its elements too, a null element equal to a null one.
    let by_strings = natural_join(
        &left,
        &left,
        &[JoinMatch::same("S")],
        &[JoinAddition::renamed("W", "G")],
        NaturalJoinType::LastMatch,
    )
    .unwrap();
    assert_eq!(column(&by_strings, "W"), [4, 1, 4, 3, 4, 5].map(Some));
}

#[test]
fn distinct_vectors_are_those_of_different_elements() {
    let (left, _) = vector_tables();
    let distinct = select_distinct(&left, &["X".to_string()]).unwrap();
    let snapshot = TableSnapshot::current(&*distinct).unwrap();
    assert_eq!(
        snapshot.columns[0].as_ref(),
        int_vectors(&[Some(2), Some(1), None, Some(0)], &[1, 2, 1]).as_ref()
    );
    let distinct = select_distinct(&left, &["S".to_string()]).unwrap();
    let snapshot = TableSnapshot::current(&*distinct).unwrap();
    let expected = list_of(
        &ColumnType::vector(ColumnType::String),
        [Some(2), Some(1), Some(1), Some(1)],
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("a"),
            None,
            Some("c"),
        ])),
    );
    assert_eq!(snapshot.columns[0].as_ref(), expected.as_ref());
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory")]
fn a_join_on_vectors_of_doubles_is_unported() {
    let doubles = list_of(
        &ColumnType::vector(ColumnType::Double),
        [Some(1)],
        Arc::new(arrow::array::Float64Array::from(vec![0.0])),
    );
    let table = static_table(&[("X", ColumnType::vector(ColumnType::Double), doubles)]);
    let _ = natural_join(&table, &table, &on("X"), &[], NaturalJoinType::FirstMatch);
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory")]
fn distinct_arrays_are_unported() {
    let arrays = list_of(&ColumnType::array(ColumnType::Int), [Some(1)], ints(&[1]));
    let table = static_table(&[("A", ColumnType::array(ColumnType::Int), arrays)]);
    let _ = select_distinct(&table, &["A".to_string()]);
}

fn stamp(left: &str, rule: AsOfRule, right: &str) -> AsOfMatch {
    AsOfMatch::new(left, rule, right)
}

/// Behavior: java.math.BigInteger#compareTo.
#[test]
fn big_integer_stamps_order_by_value() {
    let right = static_table(&[
        (
            "S",
            ColumnType::BigInteger,
            integers(&[Some(1), Some(5), Some(9), Some(-1)]),
        ),
        ("V", ColumnType::Int, ints(&[0, 1, 2, 3])),
    ]);
    let left = static_table(&[
        (
            "S",
            ColumnType::BigInteger,
            integers(&[Some(4), Some(9), Some(-3), None]),
        ),
        ("I", ColumnType::Int, ints(&[0, 1, 2, 3])),
    ]);
    let before = aj(
        &left,
        &right,
        &[],
        &stamp("S", AsOfRule::GreaterThanEqual, "S"),
        &add("V"),
    )
    .unwrap();
    assert_eq!(column(&before, "V"), [Some(0), Some(2), None, None]);
    let after = raj(
        &left,
        &right,
        &[],
        &stamp("S", AsOfRule::LessThanEqual, "S"),
        &add("V"),
    )
    .unwrap();
    assert_eq!(column(&after, "V"), [Some(1), Some(2), Some(3), Some(3)]);
}

/// Behavior: io.deephaven.vector.IntVector#compareTo: element by element, then the shorter
/// first.
#[test]
fn vector_stamps_order_element_by_element() {
    let left = static_table(&[
        ("K", ColumnType::Int, ints(&[0, 1])),
        (
            "X",
            ColumnType::vector(ColumnType::Int),
            int_vectors(&[Some(3), Some(3)], &[1, 1, 1, 2, 3, 2]),
        ),
    ]);
    let right = static_table(&[
        (
            "X",
            ColumnType::vector(ColumnType::Int),
            int_vectors(&[Some(1); 5], &[1, 2, 1, 3, 2]),
        ),
        ("V", ColumnType::Int, ints(&[0, 1, 2, 3, 4])),
    ]);
    let before = aj(
        &left,
        &right,
        &[],
        &stamp("X", AsOfRule::GreaterThanEqual, "X"),
        &add("V"),
    )
    .unwrap();
    assert_eq!(column(&before, "V"), [Some(2), Some(4)]);
    let after = raj(
        &left,
        &right,
        &[],
        &stamp("X", AsOfRule::LessThanEqual, "X"),
        &add("V"),
    )
    .unwrap();
    assert_eq!(column(&after, "V"), [Some(1), Some(3)]);
}

/// Seen on Deephaven 42.9: right stamps `3.0 3 3.000 3.00 3 3.0 3.000` and left stamps
/// `3`, `3.00`, `3.0000` give `raj` the rows 1, 0, 0: which of tied stamps of two scales
/// a left row matches follows Java's binary search (owner item 7.18).
#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin")]
fn big_decimal_stamps_are_unported() {
    let (left, right) = decimal_tables();
    let _ = aj(
        &left,
        &right,
        &[],
        &stamp("K", AsOfRule::GreaterThanEqual, "K"),
        &add("V"),
    );
}
