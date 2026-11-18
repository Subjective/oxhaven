//! Aggregations grouped by vector columns, the distinct vectors of a group, and count where
//! filters of vector and array columns. Every expected value was seen on Deephaven 42.9,
//! the vectors made by `group_by`, the arrays by `new int[]{...}`.

use super::*;
use arrow::array::{Float64Array, Int32Array, StringArray, UInt16Array};
use oxhaven_testkit::list_of;

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

/// Java keeps the distinct vectors of floating point elements, of Booleans and of Instants
/// in ways that are not ported: two vectors that differ only in the sign of a zero are one
/// value; the result of the others loses its inner element type.
#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.by.AggregationProcessor")]
fn the_distinct_vectors_of_doubles_are_unported() {
    let table = static_table(&[(
        "D",
        ColumnType::vector(ColumnType::Double),
        lists(
            ColumnType::vector(ColumnType::Double),
            &[Some(1), Some(1)],
            Arc::new(Float64Array::from(vec![-0.0, 0.0])),
        ),
    )]);
    let spec = AggSpec::Distinct {
        include_nulls: false,
    };
    let _ = aggregate(&table, &[agg(spec, &["DD = D"])], false, &[]);
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
