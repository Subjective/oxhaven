//! `update_by` over vector and array columns: bucketing by vectors, and the operators Java
//! applies to a column of objects (forward fill, rolling count, rolling group).
//!
//! The expected values were observed on Deephaven 42.9 (its Python console, vectors made
//! by `group_by` and arrays by `new int[]{...}` in `update`), as noted at each test.

use super::{
    update_by, update_by_with, RollingWindow, UpdateByError, UpdateByOperation, UpdateBySpec,
    WindowScale,
};
use crate::ops::sort::testing::{cells, static_table, Ticking};
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
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

/// Buckets of vectors of `double` and `float` treat `-0.0` as `0.0` and every NaN as one
/// value, as Java's buckets of a plain `double` column do (owner item 7.6, allowed
/// difference A19 of `PARITY.md`). Seen on Deephaven 42.9 for vectors made by `group_by`
/// (and cast to `float` for `F`): `D` is `[0.0] [-0.0] [NaN] [1.5] [0.0] [NaN] null
/// [-0.0, 1.5] [0.0, 1.5] null null [1.5]` and `cum_sum("S = Y")` by `D` sums
/// `0 1 2 3 5 7 6 7 15 15 25 14`. After a `select()` Java puts `[0.0]` and `[-0.0]` in two
/// buckets (`0 1 2 3 4 7 6 7 8 15 25 14`); that is the difference.
#[test]
fn a_bucket_column_of_floating_vectors_treats_zeros_as_equal() {
    let lengths = [
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        None,
        Some(2),
        Some(2),
        None,
        None,
        Some(1),
    ];
    let elements = [
        0.0,
        -0.0,
        f64::NAN,
        1.5,
        0.0,
        // Another NaN than the first, equal to it by `DoubleComparisons#eq`.
        f64::from_bits(f64::NAN.to_bits() | 1),
        -0.0,
        1.5,
        0.0,
        1.5,
        1.5,
    ];
    let sums = expected(&[0, 1, 2, 3, 5, 7, 6, 7, 15, 15, 25, 14]);
    let (doubles, cells) = vector_column(
        ColumnType::Double,
        &lengths,
        Float64Array::from(elements.to_vec()),
    );
    let (floats, float_cells) = vector_column(
        ColumnType::Float,
        &lengths,
        Float32Array::from(elements.iter().map(|&e| e as f32).collect::<Vec<_>>()),
    );
    let source = static_table(&[
        ("D", doubles, cells),
        ("F", floats, float_cells),
        ("Y", ColumnType::Long, longs(0..12)),
    ]);
    assert_eq!(bucketed_sums(&source, &["D"]), sums);
    assert_eq!(bucketed_sums(&source, &["F"]), sums);
    assert_eq!(bucketed_sums(&source, &["D", "F"]), sums);
}

/// A vector of vectors of doubles is not named by the owner's answer to item 7.6, and
/// stays refused: Java's equality of the inner vectors depends on their class.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy: \
                           bucket column V of type Vector(Vector(Double))"
)]
fn a_bucket_column_of_vectors_of_double_vectors_is_unported() {
    let doubles = ColumnType::vector(ColumnType::Double);
    let vectors = ColumnType::vector(doubles.clone());
    let cells = list_of(
        &vectors,
        [Some(1), Some(1)],
        singles(&doubles, Float64Array::from(vec![0.0, -0.0])),
    );
    bucket_by(vectors, cells);
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

/// A vector column of cells of `lengths` (`None` a null cell), of the elements `elements`.
fn vector_column<A: arrow::array::Array + 'static>(
    element: ColumnType,
    lengths: &[Option<usize>],
    elements: A,
) -> (ColumnType, ArrayRef) {
    let column_type = ColumnType::vector(element);
    let cells = list_of(&column_type, lengths.iter().copied(), Arc::new(elements));
    (column_type, cells)
}

/// The cells, as text, of the columns `names` of `update_by(source, operations, by)`; each
/// has the type of the column its name ends with.
fn columns_of(
    source: &Arc<dyn Table>,
    operations: &[UpdateByOperation],
    by: &[&str],
    names: &[&str],
) -> Vec<Vec<String>> {
    let result = update_by(source, operations, by).unwrap();
    let type_of = |table: &dyn Table, name: &str| {
        let index = table.definition().index_of(name).unwrap();
        table.definition().columns()[index].column_type.clone()
    };
    names
        .iter()
        .map(|name| {
            assert_eq!(type_of(&*result, name), type_of(&**source, &name[1..]));
            let index = result.definition().index_of(name).unwrap();
            cells(&*result, index)
        })
        .collect()
}

fn texts(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|cell| cell.to_string()).collect()
}

/// Seen on Deephaven 42.9: `E` is `[5, 1] [3, 7, 2] [1] [5, 1] [0]` (a `group_by` of
/// `int`s, before and after `select()`), with `cum_max`, `cum_min`, and
/// `rolling_max_tick`/`rolling_min_tick` with `rev_ticks=2`. Vectors are ordered element by
/// element (`[5, 1]` is greater than `[3, 7, 2]`), and the results keep the vector type.
///
/// Behavior: io.deephaven.vector.IntVector#compareTo
#[test]
fn the_minimum_and_maximum_of_int_vectors_order_them_element_by_element() {
    let (ints, cells) = vector_column(
        ColumnType::Int,
        &[Some(2), Some(3), Some(1), Some(2), Some(1)],
        Int32Array::from(vec![5, 1, 3, 7, 2, 1, 5, 1, 0]),
    );
    let source = static_table(&[("E", ints, cells)]);
    let cumulative = columns_of(
        &source,
        &[
            op(UpdateBySpec::CumMax, &["XE = E"]),
            op(UpdateBySpec::CumMin, &["NE = E"]),
        ],
        &[],
        &["XE", "NE"],
    );
    assert_eq!(cumulative[0], ["[5, 1]"; 5]);
    assert_eq!(cumulative[1], ["[5, 1]", "[3, 7, 2]", "[1]", "[1]", "[0]"]);
    let window = RollingWindow::ticks(2, 0);
    let rolling = columns_of(
        &source,
        &[
            op(UpdateBySpec::RollingMax(window.clone()), &["XE = E"]),
            op(UpdateBySpec::RollingMin(window), &["NE = E"]),
        ],
        &[],
        &["XE", "NE"],
    );
    assert_eq!(
        rolling[0],
        ["[5, 1]", "[5, 1]", "[3, 7, 2]", "[5, 1]", "[5, 1]"]
    );
    assert_eq!(rolling[1], ["[5, 1]", "[3, 7, 2]", "[1]", "[1]", "[0]"]);
}

/// Seen on Deephaven 42.9: `S` is `[b, b] [a, c] [null, a] [ab, null]` and `D` (and `F`,
/// the same as `float`s) `[1.5, 0.0] [NaN, 1.5] [-0.0, 3.0] [2.0, NaN]`, with `cum_max`,
/// `cum_min`, and for `S` `rolling_max_tick`/`rolling_min_tick` with `rev_ticks=2`. A null
/// element is less than every other, a NaN greater, and strings compare by `compareTo`.
///
/// Behavior: io.deephaven.vector.ObjectVector#compareTo with
/// io.deephaven.util.compare.ObjectComparisons#compare, and DoubleVector#compareTo and
/// FloatVector#compareTo with DoubleComparisons#compare and FloatComparisons#compare
#[test]
fn the_minimum_and_maximum_of_string_and_double_vectors_follow_java_comparisons() {
    let two = [Some(2); 4];
    let (strings, s) = vector_column(
        ColumnType::String,
        &two,
        StringArray::from(vec![
            Some("b"),
            Some("b"),
            Some("a"),
            Some("c"),
            None,
            Some("a"),
            Some("ab"),
            None,
        ]),
    );
    let d = [1.5, 0.0, f64::NAN, 1.5, -0.0, 3.0, 2.0, f64::NAN];
    let (doubles, dc) = vector_column(ColumnType::Double, &two, Float64Array::from(d.to_vec()));
    let f: Vec<f32> = d.iter().map(|&v| v as f32).collect();
    let (floats, fc) = vector_column(ColumnType::Float, &two, Float32Array::from(f));
    let source = static_table(&[("S", strings, s), ("D", doubles, dc), ("F", floats, fc)]);
    let cumulative = columns_of(
        &source,
        &[
            op(UpdateBySpec::CumMax, &["XS = S", "XD = D", "XF = F"]),
            op(UpdateBySpec::CumMin, &["NS = S", "ND = D", "NF = F"]),
        ],
        &[],
        &["XS", "XD", "XF", "NS", "ND", "NF"],
    );
    assert_eq!(cumulative[0], ["[b, b]"; 4]);
    let max = ["[1.5, 0.0]", "[NaN, 1.5]", "[NaN, 1.5]", "[NaN, 1.5]"];
    assert_eq!(cumulative[1], max);
    assert_eq!(cumulative[2], max);
    assert_eq!(
        cumulative[3],
        ["[b, b]", "[a, c]", "[null, a]", "[null, a]"]
    );
    let min = ["[1.5, 0.0]", "[1.5, 0.0]", "[-0.0, 3.0]", "[-0.0, 3.0]"];
    assert_eq!(cumulative[4], min);
    assert_eq!(cumulative[5], min);
    let window = RollingWindow::ticks(2, 0);
    let rolling = columns_of(
        &source,
        &[
            op(UpdateBySpec::RollingMax(window.clone()), &["XS = S"]),
            op(UpdateBySpec::RollingMin(window), &["NS = S"]),
        ],
        &[],
        &["XS", "NS"],
    );
    assert_eq!(rolling[0], ["[b, b]", "[b, b]", "[a, c]", "[ab, null]"]);
    assert_eq!(rolling[1], ["[b, b]", "[a, c]", "[null, a]", "[null, a]"]);
}

/// Seen on Deephaven 42.9, in buckets `K` of `0 1 0 1 0 1 0`: `C` is `[b, a] [b, c]
/// [a, a] null [b, a] [c, c] [a, b]` (`char`s) and `L` is `[3, 1] [null, 2] null [1, 1]
/// [5, null] [] [2, 2]` (`long`s); `cum_max`, `cum_min`, `rolling_max_tick` with
/// `rev_ticks=3`, and `rolling_min_tick` with `rev_ticks=3, fwd_ticks=1`. Null cells are
/// skipped, and an empty vector is less than every other. A `char` shows as its code unit.
#[test]
fn the_minimum_and_maximum_of_vectors_skip_null_cells_by_bucket() {
    let chars: Vec<u16> = "babcaabaccab".bytes().map(u16::from).collect();
    let (char_type, c) = vector_column(
        ColumnType::Char,
        &[Some(2), Some(2), Some(2), None, Some(2), Some(2), Some(2)],
        UInt16Array::from(chars),
    );
    let (long_type, l) = vector_column(
        ColumnType::Long,
        &[Some(2), Some(2), None, Some(2), Some(2), Some(0), Some(2)],
        Int64Array::from(vec![
            Some(3),
            Some(1),
            None,
            Some(2),
            Some(1),
            Some(1),
            Some(5),
            None,
            Some(2),
            Some(2),
        ]),
    );
    let k: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 0, 1, 0, 1, 0]));
    let source = static_table(&[
        ("K", ColumnType::Int, k),
        ("C", char_type, c),
        ("L", long_type, l),
    ]);
    let cumulative = columns_of(
        &source,
        &[
            op(UpdateBySpec::CumMax, &["XC = C", "XL = L"]),
            op(UpdateBySpec::CumMin, &["NC = C", "NL = L"]),
        ],
        &["K"],
        &["XC", "XL", "NC", "NL"],
    );
    let max_c = texts(&[
        "[98, 97]", "[98, 99]", "[98, 97]", "[98, 99]", "[98, 97]", "[99, 99]", "[98, 97]",
    ]);
    let max_l = texts(&[
        "[3, 1]",
        "[null, 2]",
        "[3, 1]",
        "[1, 1]",
        "[5, null]",
        "[1, 1]",
        "[5, null]",
    ]);
    let min_c = texts(&[
        "[98, 97]", "[98, 99]", "[97, 97]", "[98, 99]", "[97, 97]", "[98, 99]", "[97, 97]",
    ]);
    assert_eq!(cumulative[0], max_c);
    assert_eq!(cumulative[1], max_l);
    assert_eq!(cumulative[2], min_c);
    assert_eq!(
        cumulative[3],
        texts(&[
            "[3, 1]",
            "[null, 2]",
            "[3, 1]",
            "[null, 2]",
            "[3, 1]",
            "[]",
            "[2, 2]"
        ])
    );
    let rolling = columns_of(
        &source,
        &[
            op(
                UpdateBySpec::RollingMax(RollingWindow::ticks(3, 0)),
                &["XC = C", "XL = L"],
            ),
            op(
                UpdateBySpec::RollingMin(RollingWindow::ticks(3, 1)),
                &["NC = C", "NL = L"],
            ),
        ],
        &["K"],
        &["XC", "XL", "NC", "NL"],
    );
    assert_eq!(rolling[0], max_c);
    assert_eq!(rolling[1], max_l);
    assert_eq!(
        rolling[2],
        texts(&[
            "[97, 97]", "[98, 99]", "[97, 97]", "[98, 99]", "[97, 97]", "[98, 99]", "[97, 97]"
        ])
    );
    assert_eq!(
        rolling[3],
        texts(&[
            "[3, 1]",
            "[null, 2]",
            "[3, 1]",
            "[]",
            "[2, 2]",
            "[]",
            "[2, 2]"
        ])
    );
}

/// Of two vectors that compare equal, the cumulative minimum and maximum keep the first: a
/// later one replaces it only when it is strictly less or greater. Seen on Deephaven 42.9:
/// `[-0.0] [0.0] [-0.0]` gives `[-0.0]` for every row, for both.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.minmax.ComparableCumMinMaxOperator#doOperation
#[test]
fn a_cumulative_minimum_keeps_the_first_of_equal_vectors() {
    let (doubles, cells) = vector_column(
        ColumnType::Double,
        &[Some(1), Some(1), Some(1)],
        Float64Array::from(vec![-0.0, 0.0, -0.0]),
    );
    let source = static_table(&[("D", doubles, cells)]);
    let both = columns_of(
        &source,
        &[
            op(UpdateBySpec::CumMin, &["ND = D"]),
            op(UpdateBySpec::CumMax, &["XD = D"]),
        ],
        &[],
        &["ND", "XD"],
    );
    assert_eq!(both[0], ["[-0.0]"; 3]);
    assert_eq!(both[1], ["[-0.0]"; 3]);
}

/// Seen on Deephaven 42.9: the cumulative minimum and maximum of a vector of `Instant`s or
/// of `Boolean`s is a column of `ObjectVector` with the component type `String`, which a
/// client receives as text. Oxhaven keeps the element type: allowed difference A27 of
/// `PARITY.md` (owner item 3.3; Java's answers are in `oracle/corpus/agg_typed_vectors`).
#[test]
fn a_minimum_of_boolean_vectors_keeps_the_element_type() {
    let (booleans, column) = vector_column(
        ColumnType::Boolean,
        &[Some(1), Some(1), Some(2)],
        BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]),
    );
    let source = static_table(&[("V", booleans.clone(), column)]);
    let minimum = update_by(&source, &[op(UpdateBySpec::CumMin, &[])], &[] as &[&str]).unwrap();
    assert_eq!(minimum.definition().columns()[0].column_type, booleans);
    assert_eq!(cells(&*minimum, 0), ["[true]", "[false]", "[null, true]"]);
    let maximum = update_by(&source, &[op(UpdateBySpec::CumMax, &[])], &[] as &[&str]).unwrap();
    assert_eq!(cells(&*maximum, 0), ["[true]", "[true]", "[true]"]);
}

/// A vector of other objects than strings, Booleans and Instants: Java sends the minimum
/// as text too, which A27 does not cover.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.minmax.ComparableCumMinMaxOperator: \
                minimum or maximum of vector column V of type Vector(LocalDate)"
)]
fn a_minimum_of_vectors_of_dates_is_unported() {
    let elements = arrow::array::new_empty_array(&ColumnType::LocalDate.arrow_type());
    let column_type = ColumnType::vector(ColumnType::LocalDate);
    let column = list_of(&column_type, [None::<usize>; 0], elements);
    let source = static_table(&[("V", column_type, column)]);
    let _ = update_by(&source, &[op(UpdateBySpec::CumMin, &[])], &[] as &[&str]);
}

/// A rolling minimum or maximum of vectors of `double` or `float` keeps the earliest of
/// equal vectors in the window (owner item 7.10, allowed difference A20 of `PARITY.md`).
/// Seen on Deephaven 42.9: over 400 rows that alternate `[-0.0]` and `[0.0]`, from
/// `[-0.0]`, `rolling_min_tick` and `rolling_max_tick` with `rev_ticks=3` and
/// `rolling_min_time` and `rolling_max_time` with `rev_time="PT2s"` over one row a second
/// keep the earliest vector of the window except at rows 128, 256 and 384, where Java
/// shows `[0.0]`. `F` holds the same values as floats (not run on Deephaven).
#[test]
fn a_rolling_minimum_of_floating_vectors_keeps_the_earliest_of_equal_ones() {
    let rows = 400;
    let zeros: Vec<f64> = (0..rows)
        .map(|r| if r % 2 == 0 { -0.0 } else { 0.0 })
        .collect();
    let lengths = vec![Some(1); rows];
    let (doubles, cells) = vector_column(
        ColumnType::Double,
        &lengths,
        Float64Array::from(zeros.clone()),
    );
    let (floats, float_cells) = vector_column(
        ColumnType::Float,
        &lengths,
        Float32Array::from(zeros.iter().map(|&z| z as f32).collect::<Vec<_>>()),
    );
    let second = 1_000_000_000_i64;
    let times: TimestampNanosecondArray = (0..rows as i64).map(|r| Some(r * second)).collect();
    let source = static_table(&[
        ("D", doubles, cells),
        ("F", floats, float_cells),
        (
            "Ts",
            ColumnType::Instant,
            Arc::new(times.with_timezone("UTC")),
        ),
    ]);
    let ticks = RollingWindow::ticks(3, 0);
    let time = RollingWindow {
        rev: WindowScale::Time {
            column: "Ts".into(),
            nanos: 2 * second,
        },
        fwd: WindowScale::Time {
            column: "Ts".into(),
            nanos: 0,
        },
    };
    let all = columns_of(
        &source,
        &[
            op(
                UpdateBySpec::RollingMin(ticks.clone()),
                &["ND = D", "NF = F"],
            ),
            op(UpdateBySpec::RollingMax(ticks), &["XD = D", "XF = F"]),
            op(
                UpdateBySpec::RollingMin(time.clone()),
                &["MD = D", "MF = F"],
            ),
            op(UpdateBySpec::RollingMax(time), &["YD = D", "YF = F"]),
        ],
        &[],
        &["ND", "NF", "XD", "XF", "MD", "MF", "YD", "YF"],
    );
    // The earliest row of the window of row r is r - 2, or 0.
    let earliest: Vec<String> = (0..rows)
        .map(|r| {
            let first = r.saturating_sub(2);
            if first % 2 == 0 { "[-0.0]" } else { "[0.0]" }.to_string()
        })
        .collect();
    for column in &all {
        assert_eq!(column, &earliest);
    }
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
        ("D", ColumnType::vector(ColumnType::Double)),
    ];
    let window = RollingWindow::ticks(2, 1);
    let operations = vec![
        op(UpdateBySpec::CumSum, &["Sum = I"]),
        op(UpdateBySpec::ForwardFill, &["FV = V", "FA = A"]),
        op(
            UpdateBySpec::RollingCount(window.clone()),
            &["CV = V", "CA = A"],
        ),
        op(
            UpdateBySpec::RollingGroup(window.clone()),
            &["GV = V", "GA = A"],
        ),
        op(UpdateBySpec::CumMin, &["NV = V", "NS = S", "ND = D"]),
        op(UpdateBySpec::CumMax, &["XV = V", "XS = S", "XD = D"]),
        op(
            UpdateBySpec::RollingMin(window.clone()),
            &["RNV = V", "RNS = S", "RND = D"],
        ),
        op(
            UpdateBySpec::RollingMax(window.clone()),
            &["RXV = V", "RXS = S", "RXD = D"],
        ),
        op(
            UpdateBySpec::CumCountWhere {
                result_column: "CW".into(),
                filters: vec!["V != null".into(), "A == null".into()],
            },
            &[],
        ),
        op(
            UpdateBySpec::RollingCountWhere {
                window,
                result_column: "RW".into(),
                filters: vec!["len(V) > 1".into()],
            },
            &[],
        ),
    ];
    for by in [&["K"][..], &["K", "S"], &["V"], &["D"]] {
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
                update_by_with(source, &operations, &by, &oxhaven_expr::Compiler).unwrap()
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

/// Seen on Deephaven 42.9 over the table of [`objects_formula`]: a read of a condition
/// between a window and a slice of one, a slice of a condition between windows, and a
/// library function of a condition read the vector the condition chose, and fail where a
/// read of it fails (`NoSuchElementException`); `len` of a condition between windows of
/// strings fails as `len` of one does (`ClassCastException`).
///
/// Behavior: IntRingBufferVectorWrapper#get, IntVectorSlice#get, Basic#first and Basic#last
/// over the vector a condition chose
#[test]
fn a_read_of_a_condition_between_windows_and_slices_reads_the_chosen_one() {
    let read = |text: &str| objects_formula(text, None, &[]);
    for (text, expected) in [
        ("R = (I[0] > 1 ? I : I.subVector(0, 1))[0]", "0 0 0 1 2"),
        ("R = (I[0] > 1 ? I : J).subVector(0, 1)[0]", "0 0 0 10 2"),
        (
            "R = (I[0] > 1 ? I : (I[0] > 0 ? J : I.subVector(0, 1)))[0]",
            "0 0 0 10 2",
        ),
        ("R = (I[0] > 1 ? I : I.subVector(0, 1)).size()", "1 1 1 1 3"),
        ("R = max(I[0] > 1 ? I : I.subVector(0, 2))", "0 1 1 2 4"),
        (
            "R = sum((I[0] > 1 ? I : J).subVector(1, 3))",
            "null 10 30 50 7",
        ),
        ("R = first(I[0] > 1 ? I : J)", "0 0 0 10 2"),
        ("R = last(I[0] > 1 ? I : J)", "0 10 20 30 4"),
    ] {
        assert_eq!(read(text), Ok(expected.to_string()), "{text}");
    }
    let outside = read("R = J[1]");
    assert!(outside.is_err());
    for text in [
        "R = (I[0] > 1 ? I : J).subVector(1, 2)[0]",
        "R = (I[0] > 0 ? I : I.subVector(1, 2))[0]",
        "R = first(I[0] > 1 ? I : I.subVector(1, 3))",
        "R = (I[0] > 1 ? S : T)[1]",
    ] {
        assert_eq!(read(text), outside, "{text}");
    }
    let array_cast = read("R = len(S)");
    assert!(array_cast.is_err());
    for text in [
        "R = len(I[0] > 1 ? S : T)",
        "R = len(I[0] > 1 ? S : (I[0] > 0 ? T : S))",
    ] {
        assert_eq!(read(text), array_cast, "{text}");
    }
}

/// The table of the count-where tests, `group_by("B")` of nine rows in Java: `E` is
/// `[5, 1] [3, 7, 2] [1] [5, 1] [0]`, `S` `[s0, s1] [s2, s0, s1] [s2] [s0, s1] [s2]`, `V`
/// is `E` with a null third cell, `A` (an `int[]`) is `[B, 1]` with a null fourth cell, and
/// `K` is 1.
fn count_where_source() -> Arc<dyn Table> {
    let (ints, e) = vector_column(
        ColumnType::Int,
        &[Some(2), Some(3), Some(1), Some(2), Some(1)],
        Int32Array::from(vec![5, 1, 3, 7, 2, 1, 5, 1, 0]),
    );
    let (_, v) = vector_column(
        ColumnType::Int,
        &[Some(2), Some(3), None, Some(2), Some(1)],
        Int32Array::from(vec![5, 1, 3, 7, 2, 5, 1, 0]),
    );
    let (strings, s) = vector_column(
        ColumnType::String,
        &[Some(2), Some(3), Some(1), Some(2), Some(1)],
        StringArray::from(vec!["s0", "s1", "s2", "s0", "s1", "s2", "s0", "s1", "s2"]),
    );
    let arrays = ColumnType::array(ColumnType::Int);
    let a = list_of(
        &arrays,
        [Some(2), Some(2), Some(2), None, Some(2)],
        Arc::new(Int32Array::from(vec![0, 1, 1, 1, 2, 1, 4, 1])),
    );
    static_table(&[
        ("E", ints.clone(), e),
        ("S", strings, s),
        ("V", ints, v),
        ("A", arrays, a),
        ("K", ColumnType::Int, Arc::new(Int32Array::from(vec![1; 5]))),
    ])
}

/// The cumulative count by `K`, and the rolling count over the row and the one before it,
/// of the rows that pass `filter`, as text; or the refusal.
fn vector_counts(filter: &str) -> Result<[String; 2], UpdateByError> {
    let filters = vec![filter.to_string()];
    let cumulative = UpdateBySpec::CumCountWhere {
        result_column: "C".into(),
        filters: filters.clone(),
    };
    let rolling = UpdateBySpec::RollingCountWhere {
        window: RollingWindow::ticks(2, 0),
        result_column: "C".into(),
        filters,
    };
    let source = count_where_source();
    let count = |spec: UpdateBySpec, by: &[&str]| {
        let table = update_by_with(&source, &[op(spec, &[])], by, &oxhaven_expr::Compiler)?;
        let column = table.definition().index_of("C").unwrap();
        Ok::<_, UpdateByError>(cells(&*table, column).join(" "))
    };
    Ok([count(cumulative, &["K"])?, count(rolling, &[])?])
}

/// Seen on Deephaven 42.9 over the table of [`count_where_source`], with
/// `cum_count_where("C", filter)` by `K` and `rolling_count_where_tick("C", filter,
/// rev_ticks=2)`: the same rows pass as in `where(filter)`, and a filter that `where` fails
/// on a null vector or array (`NullPointerException`) fails the `update_by`.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.Context#applyFilters
#[test]
fn a_count_where_filters_vector_and_array_columns_as_where_does() {
    let counts = |cumulative: &str, rolling: &str| Ok([cumulative.into(), rolling.into()]);
    for (filter, expected) in [
        ("V == null", counts("0 0 1 1 1", "0 0 1 1 0")),
        ("V = null", counts("0 0 1 1 1", "0 0 1 1 0")),
        ("V != null", counts("1 2 2 3 4", "1 2 1 1 2")),
        ("!isNull(V)", counts("1 2 2 3 4", "1 2 1 1 2")),
        ("len(V) > 1", counts("1 2 2 3 3", "1 2 1 1 1")),
        ("sum(V) > 5", counts("1 2 2 3 3", "1 2 1 1 1")),
        ("A == null", counts("0 0 0 1 1", "0 0 0 1 1")),
        ("S.size() > 2", counts("0 1 1 1 1", "0 1 1 0 0")),
    ] {
        assert_eq!(vector_counts(filter), expected, "{filter}");
    }
    for filter in ["V.size() > 1", "V[0] > 2", "V.get(1) == 1", "A.length > 1"] {
        match vector_counts(filter) {
            Err(UpdateByError::Failed(_)) => {}
            other => panic!("{filter}: {other:?}"),
        }
    }
    // Java: `Failed to convert value <E> for column "V" of type
    // io.deephaven.vector.IntVector; it is a column name`.
    match vector_counts("V == E") {
        Err(UpdateByError::InvalidArgument(_)) => {}
        other => panic!("V == E: {other:?}"),
    }
}
