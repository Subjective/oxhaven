//! Aggregations grouped by vector columns, the distinct vectors of a group, and count where
//! filters of vector and array columns. Every expected value was seen on Deephaven 42.9,
//! the vectors made by `group_by`, the arrays by `new int[]{...}`.

use super::*;
use crate::ops::sort::testing::Ticking;
use arrow::array::{Float32Array, Float64Array, Int32Array, StringArray, UInt16Array};
use oxhaven_testkit::{list_of, TableChange};

fn ints(values: &[i32]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

/// A vector or array column of `element` holding `cells` (`None` a null cell), whose
/// elements are `elements` in order.
fn lists(column_type: ColumnType, cells: &[Option<usize>], elements: ArrayRef) -> ArrayRef {
    list_of(&column_type, cells.iter().copied(), elements)
}

/// `K`, `N` and the vector `X` of `group_by(["K", "N"])` of the probe's table, followed by
/// a row whose vector is null (`K == 3`) and one whose vector is empty (`K == 5`):
/// `[9, 4] [5, 1, 5] [3, 7] null [9, 4] []`.
fn grouped() -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        ("K", ColumnType::Int, ints(&[2, 0, 1, 3, 4, 5])),
        ("N", ColumnType::Int, ints(&[0, 0, 1, 0, 0, 0])),
        (
            "X",
            ColumnType::vector(ColumnType::Int),
            lists(
                ColumnType::vector(ColumnType::Int),
                &[Some(2), Some(3), Some(2), None, Some(2), Some(0)],
                ints(&[9, 4, 5, 1, 5, 3, 7, 9, 4]),
            ),
        ),
    ]
}

/// Behavior: io.deephaven.vector.IntVector#equals with
/// io.deephaven.util.compare.ObjectComparisons#eq: equal vectors are one group, the null
/// vector one, the empty vector its own; groups in the order first seen.
#[test]
fn rows_are_grouped_by_the_contents_of_a_vector_column() {
    let table = static_table(&grouped());
    let by_vector = agg_by(
        &table,
        &[count("Cnt"), agg(AggSpec::Sum, &["KS = K"])],
        &["X"],
    );
    assert_eq!(
        rows(&*by_vector),
        [
            "[9, 4]|2|6",
            "[5, 1, 5]|1|0",
            "[3, 7]|1|1",
            "null|1|3",
            "[]|1|5"
        ]
    );
    assert_eq!(
        schema(&*by_vector)[0],
        ("X".to_string(), ColumnType::vector(ColumnType::Int))
    );
    let by_two = agg_by(&table, &[count("Cnt")], &["N", "X"]);
    assert_eq!(
        rows(&*by_two),
        [
            "0|[9, 4]|2",
            "0|[5, 1, 5]|1",
            "1|[3, 7]|1",
            "0|null|1",
            "0|[]|1"
        ]
    );

    // Strings, a null element equal only to a null element.
    let strings = static_table(&[(
        "S",
        ColumnType::vector(ColumnType::String),
        lists(
            ColumnType::vector(ColumnType::String),
            &[Some(3), Some(3), Some(2), Some(3)],
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                Some("g"),
                None,
                Some("c"),
                Some("d"),
                Some("e"),
                Some("f"),
                None,
                Some("c"),
                Some("d"),
            ])),
        ),
    )]);
    let by_strings = agg_by(&strings, &[count("Cnt")], &["S"]);
    assert_eq!(
        rows(&*by_strings),
        ["[a, b, g]|1", "[, c, d]|2", "[e, f]|1"].map(|row| row.replace("[, ", "[null, "))
    );
}

/// Java refuses an array group-by column, after checking the initial groups and before
/// planning any aggregation.
#[test]
fn an_array_group_by_column_is_java_s_error() {
    let table = static_table(&[
        ("K", ColumnType::Int, ints(&[1, 2])),
        (
            "A",
            ColumnType::array(ColumnType::Int),
            lists(
                ColumnType::array(ColumnType::Int),
                &[Some(1), Some(1)],
                ints(&[1, 2]),
            ),
        ),
        (
            "SA",
            ColumnType::array(ColumnType::String),
            lists(
                ColumnType::array(ColumnType::String),
                &[Some(1), None],
                Arc::new(StringArray::from(vec!["s"])),
            ),
        ),
    ]);
    for (by, message) in [
        (
            &["A"][..],
            "Cannot aggregate using an array column: A, column type is an array of int",
        ),
        (
            &["K", "SA"],
            "Cannot aggregate using an array column: SA, column type is an array of class \
             java.lang.String",
        ),
    ] {
        let Err(error) = aggregate(&table, &[count("Cnt")], false, by) else {
            panic!("an array group-by column was accepted");
        };
        assert!(
            matches!(error, AggregateError::ArrayKey { .. }),
            "{error:?}"
        );
        assert_eq!(error.to_string(), message);
    }
}

/// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ObjectChunkedDistinctOperator:
/// the distinct vectors in the order of `IntVector#compareTo` (element by element, then
/// the shorter first), the null vector first when nulls are included.
#[test]
fn the_distinct_vectors_of_a_group_are_ordered_as_vectors() {
    let table = static_table(&grouped());
    let distinct = |include_nulls: bool, by: &[&str]| {
        let spec = AggSpec::Distinct { include_nulls };
        rows(&*agg_by(&table, &[agg(spec, &["DX = X"])], by))
    };
    assert_eq!(
        distinct(false, &["N"]),
        ["0|[[], [5, 1, 5], [9, 4]]", "1|[[3, 7]]"]
    );
    assert_eq!(
        distinct(true, &["N"]),
        ["0|[null, [], [5, 1, 5], [9, 4]]", "1|[[3, 7]]"]
    );
    assert_eq!(
        distinct(true, &[]),
        ["[null, [], [3, 7], [5, 1, 5], [9, 4]]"]
    );
    let spec = AggSpec::Distinct {
        include_nulls: false,
    };
    let result = agg_by(&table, &[agg(spec, &["DX = X"])], &["N"]);
    assert_eq!(
        schema(&*result)[1].1,
        ColumnType::vector(ColumnType::vector(ColumnType::Int))
    );

    // Of the strings and chars of the probe's groups by `K` and `N`: `[s9, s4] [s5, s1,
    // s5] [null, s7] [s9] [s9, s4]` and `[J, E] [F, B, F] [D, H] [J] [J, E]`.
    let n = ints(&[0, 0, 1, 0, 0]);
    let cells = [Some(2), Some(3), Some(2), Some(1), Some(2)];
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("s9"),
        Some("s4"),
        Some("s5"),
        Some("s1"),
        Some("s5"),
        None,
        Some("s7"),
        Some("s9"),
        Some("s9"),
        Some("s4"),
    ]));
    let chars: ArrayRef = Arc::new(UInt16Array::from(
        "JEFBFDHJJE".encode_utf16().collect::<Vec<_>>(),
    ));
    let table = static_table(&[
        ("N", ColumnType::Int, n),
        (
            "S",
            ColumnType::vector(ColumnType::String),
            lists(ColumnType::vector(ColumnType::String), &cells, strings),
        ),
        (
            "C",
            ColumnType::vector(ColumnType::Char),
            lists(ColumnType::vector(ColumnType::Char), &cells, chars),
        ),
    ]);
    let spec = || AggSpec::Distinct {
        include_nulls: false,
    };
    let result = agg_by(
        &table,
        &[agg(spec(), &["DS = S"]), agg(spec(), &["DC = C"])],
        &["N"],
    );
    let [f, b, j, e, d, h] = [70, 66, 74, 69, 68, 72];
    assert_eq!(
        rows(&*result),
        [
            format!("0|[[s5, s1, s5], [s9], [s9, s4]]|[[{f}, {b}, {f}], [{j}], [{j}, {e}]]"),
            format!("1|[[null, s7]]|[[{d}, {h}]]"),
        ]
    );
}

/// `N` and the vectors of doubles `D` (and the same as floats in `F`) of the distinct
/// tests: `[0.0] [-0.0] [1.5, NaN] [-0.0] [0.0] null [NaN] [NaN] [] [2.5]` with `N`
/// `0 0 0 1 1 0 1 1 0 1`.
fn floating_vectors() -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let cells = [
        Some(1),
        Some(1),
        Some(2),
        Some(1),
        Some(1),
        None,
        Some(1),
        Some(1),
        Some(0),
        Some(1),
    ];
    let elements = [0.0, -0.0, 1.5, f64::NAN, -0.0, 0.0, f64::NAN, f64::NAN, 2.5];
    let floats: Vec<f32> = elements.iter().map(|&e| e as f32).collect();
    vec![
        ("N", ColumnType::Int, ints(&[0, 0, 0, 1, 1, 0, 1, 1, 0, 1])),
        (
            "D",
            ColumnType::vector(ColumnType::Double),
            lists(
                ColumnType::vector(ColumnType::Double),
                &cells,
                Arc::new(Float64Array::from(elements.to_vec())),
            ),
        ),
        (
            "F",
            ColumnType::vector(ColumnType::Float),
            lists(
                ColumnType::vector(ColumnType::Float),
                &cells,
                Arc::new(Float32Array::from(floats)),
            ),
        ),
    ]
}

/// The distinct vectors of `double`s and `float`s are ordered as vectors, with `-0.0`
/// equal to `0.0` and NaN after every number; of two vectors that differ only in the sign
/// of a zero the one shown is the first the group was given (owner item 7.13, allowed
/// difference A20 of `PARITY.md`). Seen on Deephaven 42.9 for the same vectors made by
/// `group_by`: the same rows, except that Java may show the other zero.
///
/// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.ObjectChunkedDistinctOperator,
/// io.deephaven.vector.DoubleVector#compareTo
#[test]
fn the_distinct_vectors_of_doubles_show_the_first_of_equal_ones() {
    let table = static_table(&floating_vectors());
    let distinct = |include_nulls: bool, by: &[&str]| {
        let spec = || AggSpec::Distinct { include_nulls };
        rows(&*agg_by(
            &table,
            &[agg(spec(), &["DD = D"]), agg(spec(), &["DF = F"])],
            by,
        ))
    };
    assert_eq!(
        distinct(false, &["N"]),
        [
            "0|[[], [0.0], [1.5, NaN]]|[[], [0.0], [1.5, NaN]]",
            "1|[[-0.0], [2.5], [NaN]]|[[-0.0], [2.5], [NaN]]"
        ]
    );
    assert_eq!(
        distinct(true, &["N"]),
        [
            "0|[null, [], [0.0], [1.5, NaN]]|[null, [], [0.0], [1.5, NaN]]",
            "1|[[-0.0], [2.5], [NaN]]|[[-0.0], [2.5], [NaN]]"
        ]
    );
    assert_eq!(
        distinct(true, &[]),
        ["[null, [], [0.0], [1.5, NaN], [2.5], [NaN]]|[null, [], [0.0], [1.5, NaN], [2.5], [NaN]]"]
    );
    let spec = AggSpec::Distinct {
        include_nulls: false,
    };
    let result = agg_by(&table, &[agg(spec, &["DD = D"])], &["N"]);
    assert_eq!(
        schema(&*result)[1].1,
        ColumnType::vector(ColumnType::vector(ColumnType::Double))
    );
}

/// On a ticking table the vector shown stays the one that first entered the group's set
/// for as long as an equal vector is in the group, as for distinct plain doubles; Java's
/// depends on the order its sorted set saw rows come and go (A20 of `PARITY.md`).
#[test]
fn a_ticking_distinct_of_double_vectors_keeps_the_vector_that_came_first() {
    let doubles = ColumnType::vector(ColumnType::Double);
    let cell = |value: f64| {
        lists(
            doubles.clone(),
            &[Some(1)],
            Arc::new(Float64Array::from(vec![value])),
        )
    };
    let t = Ticking::new(
        [0, 1],
        &[
            ("N", ColumnType::Int, ints(&[0, 0])),
            (
                "D",
                doubles.clone(),
                lists(
                    doubles.clone(),
                    &[Some(1), Some(1)],
                    Arc::new(Float64Array::from(vec![-0.0, 2.0])),
                ),
            ),
        ],
        |source| {
            let spec = AggSpec::Distinct {
                include_nulls: false,
            };
            agg_by(source, &[agg(spec, &["DD = D"])], &["N"])
        },
    );
    assert_eq!(rows(&*t.result), ["0|[[-0.0], [2.0]]"]);
    // `[0.0]` joins `[-0.0]`, which stays shown.
    t.step(TableChange::new().add([2], vec![ints(&[0]), cell(0.0)]));
    assert_eq!(rows(&*t.result), ["0|[[-0.0], [2.0]]"]);
}

/// Java's groups of vectors of doubles depend on the class of the vectors.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make"
)]
fn a_group_by_column_of_vectors_of_doubles_is_unported() {
    let table = static_table(&[(
        "D",
        ColumnType::vector(ColumnType::Double),
        lists(
            ColumnType::vector(ColumnType::Double),
            &[Some(1), Some(1)],
            Arc::new(Float64Array::from(vec![-0.0, 0.0])),
        ),
    )]);
    let _ = aggregate(&table, &[count("N")], false, &["D"]);
}

/// Behavior: io.deephaven.engine.table.impl.by.CountWhereOperator, whose filters are those
/// of `where` over the column of objects: a method of a null vector or array fails.
#[test]
fn count_where_filters_vector_and_array_columns_as_where_does() {
    // The probe's `group_by("K")` of nine rows: `[5, 1, 5] [3, 7, 2] [9, 4]`, with `S`
    // `[a, b, g] [null, c, d] [e, f]` and `N = K % 2`.
    let vectors = static_table(&[
        ("N", ColumnType::Int, ints(&[0, 1, 0])),
        (
            "X",
            ColumnType::vector(ColumnType::Int),
            lists(
                ColumnType::vector(ColumnType::Int),
                &[Some(3), Some(3), Some(2)],
                ints(&[5, 1, 5, 3, 7, 2, 9, 4]),
            ),
        ),
        (
            "S",
            ColumnType::vector(ColumnType::String),
            lists(
                ColumnType::vector(ColumnType::String),
                &[Some(3), Some(3), Some(2)],
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("g"),
                    None,
                    Some("c"),
                    Some("d"),
                    Some("e"),
                    Some("f"),
                ])),
            ),
        ),
    ]);
    let counted = agg_by(
        &vectors,
        &[
            Aggregation::count_where("C1", ["X.size() > 2"]),
            Aggregation::count_where("C2", ["S == null"]),
            Aggregation::count_where("C3", ["len(X) > 2"]),
            Aggregation::count_where("C4", ["X[0] > 3"]),
        ],
        &["N"],
    );
    assert_eq!(rows(&*counted), ["0|1|0|1|2", "1|1|0|1|0"]);

    // `A = new int[]{X, X + 1}`, `Q = X == 9 ? null : new int[]{X}` of the rows `K`, `X`:
    // (0, 5) (0, 1) (1, 3) (1, 7) (1, 2) (2, 9) (2, 4) (0, 5).
    let x = [5, 1, 3, 7, 2, 9, 4, 5];
    let pairs: Vec<i32> = x.iter().flat_map(|&v| [v, v + 1]).collect();
    let singles: Vec<i32> = x.iter().copied().filter(|&v| v != 9).collect();
    let arrays = static_table(&[
        ("K", ColumnType::Int, ints(&[0, 0, 1, 1, 1, 2, 2, 0])),
        (
            "A",
            ColumnType::array(ColumnType::Int),
            lists(
                ColumnType::array(ColumnType::Int),
                &[Some(2); 8],
                ints(&pairs),
            ),
        ),
        (
            "Q",
            ColumnType::array(ColumnType::Int),
            lists(
                ColumnType::array(ColumnType::Int),
                &x.map(|v| (v != 9).then_some(1)),
                ints(&singles),
            ),
        ),
    ]);
    let counted = agg_by(
        &arrays,
        &[
            Aggregation::count_where("C1", ["A[0] > 3"]),
            Aggregation::count_where("C2", ["Q == null"]),
            Aggregation::count_where("C3", ["len(A) > 1"]),
        ],
        &["K"],
    );
    assert_eq!(rows(&*counted), ["0|2|0|3", "1|1|0|3", "2|2|1|2"]);

    // Seen: a NullPointerException ("Cannot read the array length because "<local8>" is
    // null"), failing the request.
    let failed = aggregate(
        &arrays,
        &[Aggregation::count_where("C4", ["Q.length > 0"])],
        false,
        &["K"],
    );
    assert!(failed.is_err(), "{:?}", failed.map(|t| rows(&*t)));
}
