//! The `group` aggregation and `group_by`: the vectors of static tables, and each kind of
//! change of a changing one with the update it gives.
//!
//! What Java answers is in `fixtures/group_static.json` and `fixtures/group_ticking.json`,
//! which `fixtures.rs` replays; the expectations here follow those recordings and
//! `GroupByChunkedOperator`.

use super::super::{group_by, AggregateError};
use super::incremental::{
    blink_table, doubles, int, ints, kid, kid_values, strs, three_rows, Fixture,
};
use super::*;
use crate::graph::SteppedUpdateGraph;
use arrow::array::{BooleanArray, StringArray, TimestampNanosecondArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_testkit::{list_of, TableChange};

fn group(pairs: &[&str]) -> Aggregation {
    agg(AggSpec::Group, pairs)
}

#[test]
fn a_group_is_the_values_of_its_rows_in_row_order_nulls_included() {
    let table = static_table(&[
        (
            "K",
            ColumnType::String,
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("a"),
                None,
                Some("a"),
            ])),
        ),
        (
            "I",
            ColumnType::Int,
            ints(&[Some(3), None, None, Some(1), Some(2)]),
        ),
        (
            "S",
            ColumnType::String,
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some(""),
                None,
                Some("y"),
                Some("x"),
            ])),
        ),
    ]);
    let grouped = agg_by(&table, &[group(&["I", "T = S"])], &["K"]);
    assert_eq!(
        schema(&*grouped),
        [
            ("K".to_string(), ColumnType::String),
            ("I".to_string(), ColumnType::vector(ColumnType::Int)),
            ("T".to_string(), ColumnType::vector(ColumnType::String)),
        ]
    );
    assert_eq!(
        rows(&*grouped),
        ["a|[3, null, 2]|[x, null, x]", "b|[null]|[]", "null|[1]|[y]"]
    );
    assert!(grouped.is_flat() && !grouped.is_refreshing());
    // The second row's `[]` above is how one empty string prints; it is one element.
    let strings = read(&*grouped)[2].clone();
    assert_eq!(strings.as_list::<i32>().value(1).len(), 1);

    // Without keys the whole table is one group, in the table's order.
    let whole = group_by(&table, &[]).expect("a group_by");
    assert_eq!(
        rows(&*whole),
        ["[a, b, a, null, a]|[3, null, null, 1, 2]|[x, , null, y, x]"]
    );
    // By every other column: the group-by columns first, in the order given.
    let by_two = group_by(&table, &["S", "K"]).expect("a group_by");
    assert_eq!(
        rows(&*by_two),
        ["x|a|[3, 2]", "|b|[null]", "null|a|[null]", "y|null|[1]"]
    );
}

#[test]
fn the_result_is_a_vector_of_the_input_type_whatever_it_is() {
    let vector = ColumnType::vector(ColumnType::Int);
    let array = ColumnType::array(ColumnType::String);
    let instants: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(vec![Some(5), None]).with_timezone("UTC"));
    let scalars: Vec<(ColumnType, ArrayRef)> = vec![
        (
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![Some(true), None])),
        ),
        (
            ColumnType::Byte,
            Arc::new(arrow::array::Int8Array::from(vec![Some(1), None])),
        ),
        (
            ColumnType::Char,
            Arc::new(arrow::array::UInt16Array::from(vec![Some(65), None])),
        ),
        (
            ColumnType::Short,
            Arc::new(arrow::array::Int16Array::from(vec![Some(1), None])),
        ),
        (ColumnType::Int, ints(&[Some(1), None])),
        (
            ColumnType::Long,
            Arc::new(arrow::array::Int64Array::from(vec![Some(1), None])),
        ),
        (
            ColumnType::Float,
            Arc::new(arrow::array::Float32Array::from(vec![Some(-0.0), None])),
        ),
        (ColumnType::Double, doubles(&[Some(f64::NAN), None])),
        (ColumnType::String, strs(&["s", "t"])),
        (ColumnType::Instant, instants),
        // A null cell, and an empty one: they stay apart as elements.
        (vector.clone(), list_of(&vector, [None, Some(0)], ints(&[]))),
        (
            array.clone(),
            list_of(&array, [Some(2), None], strs(&["p", "q"])),
        ),
    ];
    let expected = [
        "[true, null]",
        "[1, null]",
        "[65, null]",
        "[1, null]",
        "[1, null]",
        "[1, null]",
        "[-0.0, null]",
        "[NaN, null]",
        "[s, t]",
        "[5, null]",
        "[null, []]",
        "[[p, q], null]",
    ];
    for ((column_type, values), expected) in scalars.into_iter().zip(expected) {
        let table = static_table(&[("X", column_type.clone(), values)]);
        let grouped = group_by(&table, &[]).expect("a group_by");
        assert_eq!(
            schema(&*grouped),
            [("X".to_string(), ColumnType::vector(column_type.clone()))],
        );
        assert_eq!(rows(&*grouped), [expected], "{column_type:?}");
    }
}

#[test]
fn group_stands_among_other_aggregations_in_the_order_given() {
    let table = static_table(&three_rows());
    let mixed = agg_by(
        &table,
        &[
            group(&["GI = I"]),
            agg(AggSpec::Sum, &["S = I"]),
            group(&["GD = D", "Again = I"]),
            count("N"),
        ],
        &["K"],
    );
    assert_eq!(
        rows(&*mixed),
        ["a|[1, 3]|4|[0.5, 2.5]|[1, 3]|2", "b|[2]|2|[1.5]|[2]|1"]
    );
    // A group of the key column itself.
    let keys = agg_by(&table, &[group(&["X = K"])], &["K"]);
    assert_eq!(rows(&*keys), ["a|[a, a]", "b|[b]"]);
}

#[test]
fn an_empty_table_has_no_groups_and_a_preserved_one_has_an_empty_vector() {
    let empty = static_table(&kid(&[]));
    assert_eq!(rows(&*group_by(&empty, &["K"]).unwrap()), [""; 0]);
    let whole = group_by(&empty, &[]).unwrap();
    assert_eq!(rows(&*whole), [""; 0]);
    assert_eq!(
        schema(&*whole)[0],
        ("K".to_string(), ColumnType::vector(ColumnType::String))
    );
    // Behavior: GroupByChunkedOperator#propagateInitialState, `nullToEmpty`: the group
    // that `preserve_empty` keeps has an empty row set, so an empty vector and not a null.
    let preserved = aggregate(&empty, &[group(&["I"]), count("N")], true, &[]).unwrap();
    assert_eq!(rows(&*preserved), ["[]|0"]);
    assert_eq!(read(&*preserved)[0].null_count(), 0);
}

#[test]
fn what_java_refuses_is_refused() {
    let table = static_table(&three_rows());
    assert_eq!(
        group_by(&table, &["K", "I", "D"]).err(),
        Some(AggregateError::NothingToAggregate)
    );
    assert_eq!(
        group_by(&table, &["Nope"]).err(),
        Some(AggregateError::UnknownColumn("Nope".to_string()))
    );
    assert_eq!(
        aggregate(&table, &[group(&["X = Nope"])], false, &["K"]).err(),
        Some(AggregateError::UnknownColumn("Nope".to_string()))
    );
    assert_eq!(
        aggregate(&table, &[group(&["X = I"]), group(&["X = D"])], false, &[]).err(),
        Some(AggregateError::DuplicateOutput("X".to_string()))
    );
    // Behavior: AggregationProcessor.Converter#unsupportedForBlinkTables("Group")
    let graph = SteppedUpdateGraph::new();
    let (blink, _source) = blink_table(&graph);
    let blink: Arc<dyn Table> = blink;
    assert_eq!(
        aggregate(&blink, &[count("N"), group(&["I"])], false, &["K"]).err(),
        Some(AggregateError::BlinkUnsupported(AggSpec::Group))
    );
    assert_eq!(
        group_by(&blink, &[]).err(),
        Some(AggregateError::BlinkUnsupported(AggSpec::Group))
    );
}

/// `group` of `I` and of `D`, and the sum of `I`, by `K`: columns 1, 2 and 3 of the result.
fn grouped(rows: &[(&str, i32, f64)], row_keys: &[i64]) -> Fixture {
    Fixture::new(
        &kid(rows),
        row_keys,
        vec![
            group(&["GI = I"]),
            group(&["GD = D"]),
            agg(AggSpec::Sum, &["S = I"]),
        ],
        &["K"],
    )
}

#[test]
fn rows_join_and_leave_groups_and_every_group_column_is_modified_with_them() {
    let fixture = grouped(
        &[("a", 1, 0.5), ("b", 2, 1.5), ("a", 3, 2.5)],
        &[10, 20, 30],
    );
    assert_eq!(fixture.rows(), ["a|[1, 3]|[0.5, 2.5]|4", "b|[2]|[1.5]|2"]);
    assert!(!fixture.result.is_flat());

    // A row joins a group between its rows, and a new group appears.
    let change = TableChange::new().add([15, 40], kid_values(&[("a", 7, 7.5), ("c", 9, 9.5)]));
    assert_eq!(
        fixture.step(change),
        "added [2] removed [] modified [0] columns [1, 2, 3]"
    );
    assert_eq!(
        fixture.rows(),
        [
            "a|[1, 7, 3]|[0.5, 7.5, 2.5]|11",
            "b|[2]|[1.5]|2",
            "c|[9]|[9.5]|9"
        ]
    );

    // A row leaves a group that keeps others.
    assert_eq!(
        fixture.step(TableChange::new().remove([10])),
        "added [] removed [] modified [0] columns [1, 2, 3]"
    );
    assert_eq!(fixture.rows()[0], "a|[7, 3]|[7.5, 2.5]|10");

    // A group that loses its last row loses its result row, and returns to its place.
    assert_eq!(
        fixture.step(TableChange::new().remove([20])),
        "added [] removed [1] modified [] columns []"
    );
    let change = TableChange::new().add([5], kid_values(&[("b", 4, 4.5)]));
    assert_eq!(
        fixture.step(change),
        "added [1] removed [] modified [] columns []"
    );
    assert_eq!(
        fixture.rows(),
        ["a|[7, 3]|[7.5, 2.5]|10", "b|[4]|[4.5]|4", "c|[9]|[9.5]|9"]
    );

    // A row that leaves and one that joins with the same values: the vectors are equal to
    // what they were and are reported all the same, as is the sum that was retracted.
    let change = TableChange::new()
        .remove([40])
        .add([41], kid_values(&[("c", 9, 9.5)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [2] columns [1, 2, 3]"
    );
    assert_eq!(fixture.rows()[2], "c|[9]|[9.5]|9");
}

#[test]
fn a_modified_value_marks_the_group_columns_whose_input_was_modified() {
    let fixture = grouped(&[("a", 1, 0.5), ("b", 2, 1.5), ("a", 3, 2.5)], &[0, 1, 2]);

    assert_eq!(
        fixture.step(TableChange::new().modify([2], [(1, int(6))])),
        "added [] removed [] modified [0] columns [1, 3]"
    );
    assert_eq!(fixture.rows()[0], "a|[1, 6]|[0.5, 2.5]|7");

    let change = TableChange::new().modify([1], [(2, doubles(&[None]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [1] columns [2]"
    );
    assert_eq!(fixture.rows()[1], "b|[2]|[null]|2");

    // A value rewritten with itself: Java reports the vector modified without comparing.
    assert_eq!(
        fixture.step(TableChange::new().modify([1], [(1, int(2))])),
        "added [] removed [] modified [1] columns [1]"
    );

    // Rows of two groups, both columns.
    let change = TableChange::new().modify(
        [0, 1],
        [
            (1, ints(&[None, Some(20)])),
            (2, doubles(&[Some(-0.0), Some(1.0)])),
        ],
    );
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0, 1] columns [1, 2, 3]"
    );
    assert_eq!(
        fixture.rows(),
        ["a|[null, 6]|[-0.0, 2.5]|6", "b|[20]|[1.0]|20"]
    );
}

#[test]
fn a_modification_of_a_column_that_is_not_grouped_is_not_reported() {
    let fixture = Fixture::new(&three_rows(), &[0, 1, 2], vec![group(&["GI = I"])], &["K"]);
    let change = TableChange::new().modify([0, 1], [(2, doubles(&[Some(8.0), Some(9.0)]))]);
    assert_eq!(fixture.step(change), "none");
    // Nor is a key rewritten with itself.
    assert_eq!(
        fixture.step(TableChange::new().modify([1], [(0, strs(&["b"]))])),
        "none"
    );
    assert_eq!(fixture.step(TableChange::new()), "none");
    assert_eq!(fixture.checker.updates_seen(), 0);
    assert_eq!(fixture.rows(), ["a|[1, 3]", "b|[2]"]);

    fixture.source.push_failure("the parent broke");
    fixture.graph.run_cycle();
    assert!(fixture.result.is_failed());
    let events = fixture.recorder.events();
    let [Err(failure)] = &events[..] else {
        panic!("unexpected events {events:?}");
    };
    assert!(failure.message.contains("the parent broke"), "{failure}");
}

#[test]
fn a_changed_key_moves_the_row_between_vectors() {
    let fixture = grouped(&[("a", 1, 0.5), ("b", 2, 1.5), ("a", 3, 2.5)], &[0, 1, 2]);
    // Only the key is modified: both groups, and every group column, as for any row that
    // leaves or joins.
    let change = TableChange::new().modify([0], [(0, strs(&["b"]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0, 1] columns [1, 2, 3]"
    );
    assert_eq!(fixture.rows(), ["a|[3]|[2.5]|3", "b|[1, 2]|[0.5, 1.5]|3"]);

    // The last row of a leaves for a new group, with a new value.
    let change = TableChange::new().modify([2], [(0, strs(&["z"])), (1, int(30))]);
    assert_eq!(
        fixture.step(change),
        "added [2] removed [0] modified [] columns []"
    );
    assert_eq!(fixture.rows(), ["b|[1, 2]|[0.5, 1.5]|3", "z|[30]|[2.5]|30"]);
}

#[test]
fn a_shift_moves_no_value_and_gives_no_update() {
    let fixture = grouped(&[("a", 1, 0.5), ("b", 2, 1.5), ("a", 3, 2.5)], &[2, 5, 8]);
    assert_eq!(fixture.step(TableChange::new().shift(5, 8, 10)), "none");
    assert_eq!(fixture.rows(), ["a|[1, 3]|[0.5, 2.5]|4", "b|[2]|[1.5]|2"]);

    // The groups know where their rows went: a row added between them lands between.
    let change = TableChange::new().add([10], kid_values(&[("a", 7, 7.5)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [1, 2, 3]"
    );
    assert_eq!(fixture.rows()[0], "a|[1, 7, 3]|[0.5, 7.5, 2.5]|11");

    // A shift with a modification of a shifted row: the rows are at 2, 10, 15 (b) and 18.
    let change = TableChange::new()
        .shift(10, 18, -6)
        .modify([12], [(2, doubles(&[Some(4.0)]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [2]"
    );
    assert_eq!(fixture.rows()[0], "a|[1, 7, 3]|[0.5, 7.5, 4.0]|11");
    // And the groups followed that shift too.
    let change = TableChange::new().add([5, 10], kid_values(&[("a", 8, 8.5), ("b", 9, 9.5)]));
    fixture.step(change);
    assert_eq!(
        fixture.rows(),
        [
            "a|[1, 7, 8, 3]|[0.5, 7.5, 8.5, 4.0]|19",
            "b|[2, 9]|[1.5, 9.5]|11"
        ]
    );
}

#[test]
fn every_kind_of_change_in_one_cycle() {
    let fixture = grouped(
        &[
            ("a", 1, 0.5),
            ("b", 2, 1.5),
            ("a", 3, 2.5),
            ("c", 4, 3.5),
            ("b", 5, 4.5),
        ],
        &[1, 3, 5, 7, 9],
    );
    // Row 1 (a) goes; rows 5 and 7 move down to 4 and 6; the row now at 4 turns from a to
    // c with a new value, the row at 9 changes value; a row of d and a row of a arrive.
    let change = TableChange::new()
        .remove([1])
        .shift(5, 7, -1)
        .modify(
            [4, 9],
            [(0, strs(&["c", "b"])), (1, ints(&[Some(30), None]))],
        )
        .add([0, 8], kid_values(&[("d", 6, 6.5), ("a", 7, 7.5)]));
    assert_eq!(
        fixture.step(change),
        "added [3] removed [] modified [0, 1, 2] columns [1, 2, 3]"
    );
    assert_eq!(
        fixture.rows(),
        [
            "a|[7]|[7.5]|7",
            "b|[2, null]|[1.5, 4.5]|2",
            "c|[30, 4]|[2.5, 3.5]|34",
            "d|[6]|[6.5]|6",
        ]
    );
}

#[test]
fn a_row_that_joins_any_group_marks_every_group_column_of_the_modified_rows() {
    // Behavior: GroupByChunkedOperator.InputToResultModifiedColumnSetFactory#apply:
    // `someKeyHasAddsOrRemoves` is about the cycle, not about the group.
    let fixture = Fixture::new(
        &three_rows(),
        &[0, 1, 2],
        vec![group(&["GI = I"]), group(&["GD = D"])],
        &["K"],
    );
    // b's row is modified in D only, and a new group arrives: b is reported in GI too.
    let change = TableChange::new()
        .modify([1], [(2, doubles(&[Some(9.0)]))])
        .add([7], kid_values(&[("c", 9, 9.5)]));
    assert_eq!(
        fixture.step(change),
        "added [2] removed [] modified [1] columns [1, 2]"
    );
    // The same modification alone.
    let change = TableChange::new().modify([1], [(2, doubles(&[Some(8.0)]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [1] columns [2]"
    );
    // With a group that leaves.
    let change = TableChange::new()
        .remove([7])
        .modify([1], [(2, doubles(&[Some(7.0)]))]);
    assert_eq!(
        fixture.step(change),
        "added [] removed [2] modified [1] columns [1, 2]"
    );
    assert_eq!(fixture.rows(), ["a|[1, 3]|[0.5, 2.5]", "b|[2]|[7.0]"]);
}

#[test]
fn without_keys_the_row_comes_and_goes_with_the_rows_of_the_table() {
    let fixture = Fixture::new(&kid(&[]), &[], vec![group(&["I"]), count("N")], &[]);
    assert_eq!(fixture.rows(), [""; 0]);
    assert!(fixture.result.is_flat());
    let change = TableChange::new().add([4], kid_values(&[("a", 40, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [0] removed [] modified [] columns []"
    );
    // A row that arrives later with a smaller row key comes first: the vector is in the
    // order of the row set, not of arrival.
    let change = TableChange::new().add([2], kid_values(&[("b", 20, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [0] columns [0, 1]"
    );
    assert_eq!(fixture.rows(), ["[20, 40]|2"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([2])),
        "added [] removed [] modified [0] columns [0, 1]"
    );
    assert_eq!(fixture.rows(), ["[40]|1"]);
    assert_eq!(
        fixture.step(TableChange::new().remove([4])),
        "added [] removed [0] modified [] columns []"
    );
    assert_eq!(fixture.rows(), [""; 0]);
}

#[test]
fn preserved_empty_groups_keep_an_empty_vector() {
    let fixture = Fixture::with(
        &three_rows(),
        &[0, 1, 2],
        vec![group(&["GI = I"])],
        &["K"],
        true,
        false,
    );
    assert_eq!(
        fixture.step(TableChange::new().remove([1])),
        "added [] removed [] modified [1] columns [1]"
    );
    assert_eq!(fixture.rows(), ["a|[1, 3]", "b|[]"]);
    assert_eq!(read(&*fixture.result)[1].null_count(), 0);
    let change = TableChange::new().add([9], kid_values(&[("b", 5, 0.0)]));
    assert_eq!(
        fixture.step(change),
        "added [] removed [] modified [1] columns [1]"
    );
    assert_eq!(fixture.rows(), ["a|[1, 3]", "b|[5]"]);

    let total = Fixture::with(
        &three_rows(),
        &[0, 1, 2],
        vec![group(&["I"])],
        &[],
        true,
        false,
    );
    assert_eq!(
        total.step(TableChange::new().remove([0, 1, 2])),
        "added [] removed [] modified [0] columns [0]"
    );
    assert_eq!(total.rows(), ["[]"]);
}

#[test]
fn a_group_of_a_ticking_group_reads_vectors_as_elements() {
    let fixture = Fixture::new(&three_rows(), &[0, 1, 2], vec![group(&["GI = I"])], &["K"]);
    let outer = group_by(&fixture.result, &[]).expect("a group_by of a group_by");
    assert_eq!(
        schema(&*outer)[1],
        (
            "GI".to_string(),
            ColumnType::vector(ColumnType::vector(ColumnType::Int))
        )
    );
    assert_eq!(rows(&*outer), ["[a, b]|[[1, 3], [2]]"]);
    let change = TableChange::new().add([5, 6], kid_values(&[("b", 9, 0.0), ("c", 8, 0.0)]));
    fixture.step(change);
    assert_eq!(rows(&*outer), ["[a, b, c]|[[1, 3], [2, 9], [8]]"]);
}
