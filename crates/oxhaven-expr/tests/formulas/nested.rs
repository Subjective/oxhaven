//! Vectors of vectors: a column made by a `group_by` of a vector column.
//!
//! The tables are those of the scenario `oracle/scenarios/static/nested_vectors.json`
//! (`n` and `nn`), and the expected values what Java 42.9 answered for the same formulas
//! there (`oracle/corpus/nested_vectors`) or, for the refusals, in a console script.

use super::*;
use arrow::array::ListArray;
use arrow::buffer::OffsetBuffer;
use arrow::util::display::array_value_to_string;

fn vector(element: ColumnType) -> ColumnType {
    ColumnType::Vector(Box::new(element))
}

/// A column of lists of `column_type`, one list per entry; `None` is a null list.
fn lists(column_type: &ColumnType, elements: ArrayRef, lengths: &[Option<usize>]) -> ArrayRef {
    let DataType::List(field) = column_type.arrow_type() else {
        panic!("{column_type:?} is not a list type")
    };
    let nulls = lengths.iter().map(Option::is_some).collect::<Vec<bool>>();
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths(lengths.iter().map(|length| length.unwrap_or(0))),
        elements,
        nulls.contains(&false).then(|| nulls.into()),
    ))
}

/// Two rows of five inner vectors of two elements each: `X` of `int`s, `S` of strings, `D`
/// of doubles, and `B` of Booleans. With `nulls`, the fifth inner vector of `X` in the
/// first row and the second in the second row are null.
fn table(nulls: bool) -> TestTable {
    let inner = [Some(2); 10];
    let outer = [Some(5); 2];
    // Row 0 holds the even numbers, row 1 the odd ones: [[0, 10], [2, 12], ...].
    let order: Vec<i32> = (0..2)
        .flat_map(|row| (0..5).flat_map(move |at| [row + 2 * at, row + 2 * at + 10]))
        .collect();
    let mut x_inner: Vec<Option<usize>> = inner.to_vec();
    if nulls {
        x_inner[4] = None;
        x_inner[6] = None;
    }
    let x_elements: Vec<i32> = x_inner
        .iter()
        .zip(order.chunks(2))
        .flat_map(|(length, pair)| {
            if length.is_some() {
                pair.to_vec()
            } else {
                vec![]
            }
        })
        .collect();
    let ints = vector(ColumnType::Int);
    let x = lists(&ints, Arc::new(Int32Array::from(x_elements)), &x_inner);
    let texts = vector(ColumnType::String);
    let s_elements: Vec<String> = order.iter().map(|n| format!("s{n}")).collect();
    let s = lists(&texts, Arc::new(StringArray::from(s_elements)), &inner);
    let doubles_type = vector(ColumnType::Double);
    let d_elements: Vec<f64> = order.iter().map(|n| f64::from(*n) / 4.0).collect();
    let d = lists(
        &doubles_type,
        Arc::new(Float64Array::from(d_elements)),
        &inner,
    );
    let flags_type = vector(ColumnType::Boolean);
    let b_elements: Vec<bool> = order.iter().map(|n| n % 3 == 0).collect();
    let b = lists(
        &flags_type,
        Arc::new(BooleanArray::from(b_elements)),
        &inner,
    );
    let nested = |inner: ColumnType, cells: ArrayRef| {
        let column_type = vector(inner);
        let column = lists(&column_type, cells, &outer);
        (column_type, column)
    };
    TestTable::new([
        ("A", super::ints([Some(0), Some(1)])),
        ("X", nested(ints, x)),
        ("S", nested(texts, s)),
        ("D", nested(doubles_type, d)),
        ("B", nested(flags_type, b)),
    ])
}

/// The type of `formula` and its value in each row, printed.
fn shown(table: &TestTable, formula: &str) -> (ColumnType, Vec<String>) {
    let (column_type, values) = table
        .evaluate(formula)
        .unwrap_or_else(|e| panic!("{formula} failed: {e}"));
    assert_eq!(
        *values.data_type(),
        column_type.arrow_type(),
        "Arrow type of {formula}"
    );
    let printed = (0..values.len())
        .map(|row| array_value_to_string(&values, row).unwrap())
        .collect();
    (column_type, printed)
}

#[test]
fn the_test_tables_are_those_of_the_scenario() {
    let (column_type, printed) = shown(&table(false), "X[0]");
    assert_eq!(column_type, vector(ColumnType::Int));
    assert_eq!(printed, ["[0, 10]", "[1, 11]"]);
    let (_, printed) = shown(&table(true), "X[1]");
    assert_eq!(printed, ["[2, 12]", ""]);
}

/// Behavior: `QueryLanguageParser#visit(ArrayAccessExpr)` and `#visit(MethodCallExpr)` on a
/// column of `ObjectVector<IntVector>`: an element is an `IntVector`, read as a vector
/// column's cell is.
#[test]
fn an_element_of_a_vector_of_vectors_is_a_vector() {
    use ColumnType::{Boolean, Double, Int, Long, String};
    let table = table(false);
    for (formula, column_type, expected) in [
        ("X[0]", vector(Int), ["[0, 10]", "[1, 11]"]),
        ("X[0][1]", Int, ["10", "11"]),
        ("X.size()", Long, ["5", "5"]),
        ("len(X)", Long, ["5", "5"]),
        ("X[0].size()", Long, ["2", "2"]),
        ("X.get(1)", vector(Int), ["[2, 12]", "[3, 13]"]),
        ("X[7]", vector(Int), ["", ""]),
        ("countObj(X)", Long, ["5", "5"]),
        ("firstObj(X)", vector(Int), ["[0, 10]", "[1, 11]"]),
        ("lastObj(X)", vector(Int), ["[8, 18]", "[9, 19]"]),
        ("sum(X[0])", Long, ["10", "12"]),
        ("X == null", Boolean, ["false", "false"]),
        ("X.isEmpty()", Boolean, ["false", "false"]),
        ("X[i]", vector(Int), ["[0, 10]", "[3, 13]"]),
        ("X[0][-1]", Int, ["", ""]),
        ("X[0] + 1", ColumnType::array(Int), ["[1, 11]", "[2, 12]"]),
        ("D[1][0]", Double, ["0.5", "0.75"]),
        ("max(X[0])", Int, ["10", "11"]),
        ("S[1]", vector(String), ["[s2, s12]", "[s3, s13]"]),
        ("len(S[0])", Long, ["2", "2"]),
        ("firstObj(S)", vector(String), ["[s0, s10]", "[s1, s11]"]),
        ("len(X[2])", Long, ["2", "2"]),
        ("X[0].isEmpty()", Boolean, ["false", "false"]),
        ("lastObj(X)[0]", Int, ["8", "9"]),
        (
            "array(X[1])",
            ColumnType::array(Int),
            ["[2, 12]", "[3, 13]"],
        ),
        ("avg(lastObj(D))", Double, ["3.25", "3.5"]),
        ("X[1].subVector(0, 2)", vector(Int), ["[2, 12]", "[3, 13]"]),
        ("X[1][0] * 10", Int, ["20", "30"]),
        ("len(X) + X[2].size()", Long, ["7", "7"]),
        // Sizes and nulls of vectors whose elements Java does not type.
        ("len(B)", Long, ["5", "5"]),
        ("B.size()", Long, ["5", "5"]),
        ("countObj(B)", Long, ["5", "5"]),
    ] {
        let (actual_type, printed) = shown(&table, formula);
        assert_eq!(actual_type, column_type, "type of {formula}");
        assert_eq!(printed, expected, "{formula}");
    }
}

/// A null inner vector: functions of it are null, an index of it fails.
#[test]
fn a_null_inner_vector_is_null_and_fails_where_it_is_read() {
    use ColumnType::{Boolean, Int, Long};
    let table = table(true);
    for (formula, column_type, expected) in [
        ("len(X[1])", Long, ["2", ""]),
        ("isNull(X[1])", Boolean, ["false", "true"]),
        ("countObj(X)", Long, ["4", "4"]),
        ("X[1]", vector(Int), ["[2, 12]", ""]),
        ("sum(X[1])", Long, ["14", ""]),
        ("X[i % 3]", vector(Int), ["[0, 10]", ""]),
        ("X[0][9]", Int, ["", ""]),
    ] {
        let (actual_type, printed) = shown(&table, formula);
        assert_eq!(actual_type, column_type, "type of {formula}");
        assert_eq!(printed, expected, "{formula}");
    }
    // Seen on Java 42.9: java.lang.NullPointerException, "Cannot invoke
    // io.deephaven.vector.IntVector.get(long) because the return value of
    // io.deephaven.vector.ObjectVector.get(long) is null".
    for formula in ["X[1][0]", "X[1].size()", "X[7][0]", "X[1].isEmpty()"] {
        let failure = table.evaluate(formula).expect_err(formula);
        assert!(
            failure.message.contains("NullPointerException"),
            "{formula}: {}",
            failure.message
        );
    }
}

/// What Java gives without the element type, or as a raw `ObjectVector`, is refused: seen
/// on Java 42.9, `S[0][1]` is a `java.lang.Object` and `B[0]` a vector of the strings
/// `true` and `false`, `(X)` and the elements of `X_` are vectors of strings.
#[test]
fn what_java_gives_without_a_type_is_refused() {
    let table = table(false);
    for formula in [
        "S[0][1]",
        "S[0].get(1)",
        "firstObj(S[0])",
        "firstObj(S)[0]",
        "lastObj(S[1])",
        "B[0]",
        "firstObj(B)",
        "(X)",
        "A == 0 ? X : null",
        "X_[0]",
        "X.subVector(0, 1)",
    ] {
        let error = table.compile_error(formula);
        assert!(
            matches!(error, FormulaError::Unsupported(_)),
            "{formula}: {error}"
        );
    }
}
