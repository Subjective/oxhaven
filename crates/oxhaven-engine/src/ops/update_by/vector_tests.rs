//! `update_by` over vector and array columns: bucketing by vectors, and the operators Java
//! applies to a column of objects (forward fill, rolling count, rolling group).
//!
//! The expected values were observed on Deephaven 42.9 (its Python console, vectors made
//! by `group_by` and arrays by `new int[]{...}` in `update`), as noted at each test.

use super::{
    update_by, update_by_with, RollingWindow, UpdateByError, UpdateByOperation, UpdateBySpec,
};
use crate::ops::sort::testing::{cells, static_table, Ticking};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{list_of, test_seeds, RandomUpdates};
use std::sync::Arc;

fn op(spec: UpdateBySpec, columns: &[&str]) -> UpdateByOperation {
    UpdateByOperation::new(spec, columns)
}

fn longs(values: impl IntoIterator<Item = i64>) -> ArrayRef {
    Arc::new(values.into_iter().collect::<Int64Array>())
}

/// Cells of one element each, `None` a null element.
fn singles<A: arrow::array::Array + 'static>(column_type: &ColumnType, elements: A) -> ArrayRef {
    let n = elements.len();
    list_of(column_type, (0..n).map(|_| Some(1)), Arc::new(elements))
}

/// The cumulative sum of `Y` in the buckets of `by`, as the text of each row.
fn bucketed_sums(source: &Arc<dyn Table>, by: &[&str]) -> Vec<String> {
    let result = update_by(source, &[op(UpdateBySpec::CumSum, &["S = Y"])], by).unwrap();
    let column = result.definition().index_of("S").unwrap();
    cells(&*result, column)
}

fn expected(values: &[i64]) -> Vec<String> {
    values.iter().map(i64::to_string).collect()
}

/// Seen on Deephaven 42.9: `empty_table(8).update(["G=i", "C = ...", "B = ...", "T = ...",
/// "L = ..."]).group_by("G").update(["Y=ii"])`, then `cum_sum("S=Y")` by each vector
/// column alone. Equal vectors are one bucket, the vectors of a null element one bucket.
#[test]
fn equal_vectors_are_one_bucket() {
    let char_vector = ColumnType::vector(ColumnType::Char);
    let boolean_vector = ColumnType::vector(ColumnType::Boolean);
    let instant_vector = ColumnType::vector(ColumnType::Instant);
    let long_vector = ColumnType::vector(ColumnType::Long);
    let a = Some(u16::from(b'a'));
    let b = Some(u16::from(b'b'));
    let day = 1_704_067_200_000_000_000_i64;
    let instants: TimestampNanosecondArray = [0, 1, 2, 0, 0, 1, 2, 0]
        .into_iter()
        .map(|i| match i {
            0 => Some(day),
            1 => None,
            _ => Some(day + 86_400_000_000_000),
        })
        .collect();
    let source = static_table(&[
        (
            "C",
            char_vector.clone(),
            singles(
                &char_vector,
                UInt16Array::from(vec![a, None, b, a, a, None, b, a]),
            ),
        ),
        (
            "B",
            boolean_vector.clone(),
            singles(
                &boolean_vector,
                BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ]),
            ),
        ),
        (
            "T",
            instant_vector.clone(),
            singles(&instant_vector, instants.with_timezone("UTC")),
        ),
        (
            "L",
            long_vector.clone(),
            singles(
                &long_vector,
                Int64Array::from(vec![
                    Some(0),
                    None,
                    Some(0),
                    Some(1),
                    Some(0),
                    None,
                    Some(0),
                    Some(1),
                ]),
            ),
        ),
        ("Y", ColumnType::Long, longs(0..8)),
    ]);
    let pattern = expected(&[0, 1, 2, 3, 7, 6, 8, 14]);
    assert_eq!(bucketed_sums(&source, &["C"]), pattern);
    assert_eq!(bucketed_sums(&source, &["B"]), pattern);
    assert_eq!(bucketed_sums(&source, &["T"]), pattern);
    assert_eq!(
        bucketed_sums(&source, &["L"]),
        expected(&[0, 1, 2, 3, 6, 6, 12, 10])
    );
}

/// Seen on Deephaven 42.9: vectors of `String` `[a] [a] [null] [null] [b] [b]` sum 0 1 2 5
/// 4 9; null vectors are one bucket (a natural join that finds no group gives four, which
/// sum 0 1 3 6); `[0, 1] [0, 1] [] [0]` sum 0 1 2 3; and a vector column after an `int`
/// column, `K = i % 3` and `E = [i % 2]` for eight rows, sums 0 1 2 3 4 5 6 8.
#[test]
fn vectors_are_equal_element_by_element() {
    let strings = ColumnType::vector(ColumnType::String);
    let ints = ColumnType::vector(ColumnType::Int);
    let source = static_table(&[
        (
            "T",
            strings.clone(),
            singles(
                &strings,
                StringArray::from(vec![Some("a"), Some("a"), None, None, Some("b"), Some("b")]),
            ),
        ),
        (
            "N",
            ints.clone(),
            list_of(
                &ints,
                [None; 6],
                Arc::new(Int32Array::from(Vec::<i32>::new())),
            ),
        ),
        (
            "X",
            ints.clone(),
            list_of(
                &ints,
                [Some(2), Some(2), Some(0), Some(1), Some(0), Some(0)],
                Arc::new(Int32Array::from(vec![0, 1, 0, 1, 0])),
            ),
        ),
        ("Y", ColumnType::Long, longs(0..6)),
    ]);
    assert_eq!(
        bucketed_sums(&source, &["T"]),
        expected(&[0, 1, 2, 5, 4, 9])
    );
    assert_eq!(
        bucketed_sums(&source, &["N"]),
        expected(&[0, 1, 3, 6, 10, 15])
    );
    // Rows 4 and 5 add two more empty vectors to the bucket of row 2.
    assert_eq!(
        bucketed_sums(&source, &["X"]),
        expected(&[0, 1, 2, 3, 6, 11])
    );

    let source = static_table(&[
        (
            "K",
            ColumnType::Int,
            Arc::new(Int32Array::from_iter_values((0..8).map(|i| i % 3))),
        ),
        (
            "E",
            ints.clone(),
            singles(&ints, Int32Array::from_iter_values((0..8).map(|i| i % 2))),
        ),
        ("Y", ColumnType::Long, longs(0..8)),
    ]);
    assert_eq!(
        bucketed_sums(&source, &["K", "E"]),
        expected(&[0, 1, 2, 3, 4, 5, 6, 8])
    );
}

/// A vector of vectors is a bucket key too: Java's `ObjectVector#equals` and `#hashCode`
/// call those of the inner vectors, which compare by value. Seen on Deephaven 42.9, before
/// and after a `select()`: `rolling_group_tick("R=X", rev_ticks=1)` of the `int` vectors
/// `[0] [1] [0] [0] [1] [0]`, then `cum_sum("S=Y")` by `R`, sums 0 1 2 5 5 10.
#[test]
fn vectors_of_vectors_are_equal_element_by_element() {
    let inner = ColumnType::vector(ColumnType::Int);
    let outer = ColumnType::vector(inner.clone());
    // [[0]], [[1]], [[0]], [[0], []], null, [[0], []]
    let inner_cells = list_of(
        &inner,
        [
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            Some(0),
            Some(1),
            Some(0),
        ],
        Arc::new(Int32Array::from(vec![0, 1, 0, 0, 0])),
    );
    let source = static_table(&[
        (
            "V",
            outer.clone(),
            list_of(
                &outer,
                [Some(1), Some(1), Some(1), Some(2), None, Some(2)],
                inner_cells,
            ),
        ),
        ("Y", ColumnType::Long, longs(0..6)),
    ]);
    assert_eq!(
        bucketed_sums(&source, &["V"]),
        expected(&[0, 1, 2, 3, 4, 8])
    );

    let ones = [0, 1, 0, 0, 1, 0];
    let inner_cells = singles(&inner, Int32Array::from(ones.to_vec()));
    let source = static_table(&[
        (
            "R",
            outer.clone(),
            list_of(&outer, [Some(1); 6], inner_cells),
        ),
        ("Y", ColumnType::Long, longs(0..6)),
    ]);
    assert_eq!(
        bucketed_sums(&source, &["R"]),
        expected(&[0, 1, 2, 5, 5, 10])
    );
}

fn refusal(column_type: ColumnType, cells: ArrayRef) -> String {
    let source = static_table(&[
        ("A", column_type, cells),
        ("Y", ColumnType::Long, longs(0..2)),
    ]);
    match update_by(&source, &[op(UpdateBySpec::CumSum, &["S = Y"])], &["A"]) {
        Err(UpdateByError::InvalidArgument(message)) => message,
        Err(other) => panic!("refused for another reason: {other}"),
        Ok(_) => panic!("not refused"),
    }
}

/// Seen on Deephaven 42.9 (`update(["A = new int[]{...}"])` and `new String[]{...}`).
#[test]
fn an_array_bucket_column_is_refused_as_java_refuses_it() {
    let ints = ColumnType::array(ColumnType::Int);
    let cells = singles(&ints, Int32Array::from(vec![0, 1]));
    assert_eq!(
        refusal(ints, cells),
        "Cannot aggregate using an array column: A, column type is an array of int"
    );
    let strings = ColumnType::array(ColumnType::String);
    let cells = singles(&strings, StringArray::from(vec!["a", "a"]));
    assert_eq!(
        refusal(strings, cells),
        "Cannot aggregate using an array column: A, column type is an array of class \
         java.lang.String"
    );
}

fn bucket_by(column_type: ColumnType, cells: ArrayRef) {
    let source = static_table(&[
        ("V", column_type, cells),
        ("Y", ColumnType::Long, longs(0..2)),
    ]);
    let _ = update_by(&source, &[op(UpdateBySpec::CumSum, &["S = Y"])], &["V"]);
}

/// Deephaven 42.9 puts `[0.0]` and `[-0.0]` in one bucket where the vectors come from a
/// `group_by`, and in two after a `select()`.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy: \
                           bucket column V of type Vector(Double)"
)]
fn a_bucket_column_of_double_vectors_is_unported() {
    let doubles = ColumnType::vector(ColumnType::Double);
    let cells = singles(&doubles, Float64Array::from(vec![0.0, -0.0]));
    bucket_by(doubles, cells);
}

/// Deephaven 42.9 puts each of the equal vectors `[[0]]` of a rolling group of an `int[]`
/// column in its own bucket.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy: \
                           bucket column V of type Vector(Array(Int))"
)]
fn a_bucket_column_of_vectors_of_arrays_is_unported() {
    let arrays = ColumnType::array(ColumnType::Int);
    let vectors = ColumnType::vector(arrays.clone());
    let cells = list_of(
        &vectors,
        [Some(1), Some(1)],
        singles(&arrays, Int32Array::from(vec![0, 0])),
    );
    bucket_by(vectors, cells);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.minmax.ComparableCumMinMaxOperator: \
                minimum or maximum of vector column V"
)]
fn a_minimum_of_vectors_is_unported() {
    let ints = ColumnType::vector(ColumnType::Int);
    let source = static_table(&[(
        "V",
        ints.clone(),
        singles(&ints, Int32Array::from(vec![0, 1])),
    )]);
    let _ = update_by(&source, &[op(UpdateBySpec::CumMin, &[])], &[] as &[&str]);
}

/// Seen on Deephaven 42.9: `N` is `[0] null [2] [3] null [5]`, a vector column of a
/// `group_by` or an `int[]` column; `forward_fill("F=N")`, `rolling_count_tick("C=N",
/// rev_ticks=2)` and `rolling_group_tick("R=N", rev_ticks=2)`.
#[test]
fn a_column_of_vectors_or_arrays_is_filled_counted_and_grouped() {
    for column_type in [
        ColumnType::vector(ColumnType::Int),
        ColumnType::array(ColumnType::Int),
    ] {
        let input = list_of(
            &column_type,
            [Some(1), None, Some(1), Some(1), None, Some(1)],
            Arc::new(Int32Array::from(vec![0, 2, 3, 5])),
        );
        let source = static_table(&[("N", column_type.clone(), input)]);
        let window = RollingWindow::ticks(2, 0);
        let result = update_by(
            &source,
            &[
                op(UpdateBySpec::ForwardFill, &["F = N"]),
                op(UpdateBySpec::RollingCount(window.clone()), &["C = N"]),
                op(UpdateBySpec::RollingGroup(window), &["R = N"]),
            ],
            &[] as &[&str],
        )
        .unwrap();
        let columns = result.definition().columns();
        assert_eq!(columns[1].column_type, column_type);
        assert_eq!(columns[2].column_type, ColumnType::Long);
        assert_eq!(columns[3].column_type, ColumnType::vector(column_type));
        assert_eq!(
            cells(&*result, 1),
            ["[0]", "[0]", "[2]", "[3]", "[3]", "[5]"]
        );
        assert_eq!(cells(&*result, 2), ["1", "1", "1", "2", "1", "1"]);
        assert_eq!(
            cells(&*result, 3),
            [
                "[[0]]",
                "[[0], null]",
                "[null, [2]]",
                "[[2], [3]]",
                "[[3], null]",
                "[null, [5]]"
            ]
        );
    }
}

/// Bucketing by vector columns and filling, counting and grouping one, under random
/// updates, against a recompute from scratch.
#[test]
fn random_updates_of_vector_columns_match_a_recompute() {
    let types = [
        ("K", ColumnType::vector(ColumnType::Boolean)),
        ("S", ColumnType::vector(ColumnType::String)),
        ("V", ColumnType::vector(ColumnType::Int)),
        ("A", ColumnType::array(ColumnType::Char)),
        ("I", ColumnType::Int),
    ];
    let window = RollingWindow::ticks(2, 1);
    let operations = vec![
        op(UpdateBySpec::CumSum, &["Sum = I"]),
        op(UpdateBySpec::ForwardFill, &["FV = V", "FA = A"]),
        op(
            UpdateBySpec::RollingCount(window.clone()),
            &["CV = V", "CA = A"],
        ),
        op(UpdateBySpec::RollingGroup(window), &["GV = V", "GA = A"]),
    ];
    for by in [&["K"][..], &["K", "S"], &["V"]] {
        for seed in test_seeds(0..16) {
            let mut random = RandomUpdates::new(seed);
            let columns: Vec<(&str, ColumnType, ArrayRef)> = types
                .iter()
                .map(|(name, column_type)| {
                    (*name, column_type.clone(), random.values(column_type, 8))
                })
                .collect();
            let operations = operations.clone();
            let by: Vec<&'static str> = by.to_vec();
            let t = Ticking::new([0, 2, 4, 6, 8, 10, 12, 14], &columns, move |source| {
                update_by(source, &operations, &by).unwrap()
            });
            for _ in 0..30 {
                let change = random.next_change(&*t.script.table(), 5);
                t.step(change);
            }
        }
    }
}

/// The rolling formula `text` with a window of the row and the two before it, over `S`
/// (`s0 s1 null s3 s4`), `T` (`t0` to `t4`), `I` (0 to 4) and `J` (0 to 40 by 10): its
/// values as text, or the failure.
fn objects_formula(text: &str, token: Option<&str>, columns: &[&str]) -> Result<String, String> {
    let source = static_table(&[
        (
            "S",
            ColumnType::String,
            Arc::new(StringArray::from(vec![
                Some("s0"),
                Some("s1"),
                None,
                Some("s3"),
                Some("s4"),
            ])),
        ),
        (
            "T",
            ColumnType::String,
            Arc::new(StringArray::from(vec!["t0", "t1", "t2", "t3", "t4"])),
        ),
        (
            "I",
            ColumnType::Int,
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4])),
        ),
        (
            "J",
            ColumnType::Int,
            Arc::new(Int32Array::from(vec![0, 10, 20, 30, 40])),
        ),
    ]);
    let spec = UpdateBySpec::RollingFormula {
        window: RollingWindow::ticks(3, 0),
        formula: text.into(),
        param_token: token.map(Into::into),
    };
    match update_by_with(
        &source,
        &[op(spec, columns)],
        &[] as &[&str],
        &oxhaven_expr::Compiler,
    ) {
        Ok(table) => {
            let last = table.definition().columns().len() - 1;
            Ok(cells(&*table, last).join(" "))
        }
        Err(UpdateByError::Failed(failure)) => Err(failure.message),
        Err(other) => panic!("{text}: {other}"),
    }
}

/// Seen on Deephaven 42.9 over the table of [`objects_formula`]: `len` of a condition
/// between two windows of strings throws `ClassCastException` for the `Object[]` the
/// window gives, for every row.
#[test]
#[should_panic(expected = "ObjectRingBufferVectorWrapper#toArray")]
fn len_of_a_condition_between_windows_of_objects_is_unported() {
    let _ = objects_formula("R = len(I[0] > 1 ? S : T)", None, &[]);
}

/// Seen on Deephaven 42.9 over the table of [`objects_formula`]: `(S.size() > 1 ? I :
/// J)[0]` is `0 0 0 1 2`, `(S.size() < 2 ? I : J).get(0)` `0 0 0 10 20`, the nested
/// condition `0 0 0 1 2`, and the guarded `[1]` `-1 1 1 2 3`; `[1]` and `.get(2)` without
/// a guard fail the first row, whose windows hold one element.
#[test]
fn a_read_of_a_window_chosen_by_a_condition_reads_the_chosen_window() {
    assert_eq!(
        objects_formula("R = (S.size() > 1 ? I : J)[0]", None, &[]),
        Ok("0 0 0 1 2".to_string())
    );
    assert_eq!(
        objects_formula("R = (S.size() < 2 ? I : J).get(0)", None, &[]),
        Ok("0 0 0 10 20".to_string())
    );
    assert_eq!(
        objects_formula(
            "R = (S.size() > 2 ? I : (S.size() > 1 ? J : I))[0]",
            None,
            &[]
        ),
        Ok("0 0 0 1 2".to_string())
    );
    let fails = objects_formula("R = J[1]", None, &[]);
    assert!(fails.is_err());
    for text in [
        "R = (S.size() > 1 ? I : J)[1]",
        "R = (S.size() > 1 ? I : J).get(2)",
    ] {
        assert_eq!(objects_formula(text, None, &[]), fails, "{text}");
    }
    assert_eq!(
        objects_formula(
            "R = S.size() > 1 ? (S.size() > 1 ? I : J)[1] : -1",
            None,
            &[]
        ),
        Ok("-1 1 1 2 3".to_string())
    );
}
