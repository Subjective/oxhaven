//! An operator with a literal on one side is computed by a loop of its own, with no array
//! made for the literal. Its values and its failure must be those of the same operator
//! given a column that holds the literal's value in every row, which the general kernel
//! computes; likewise a conditional that chooses between two computed branches and one
//! that evaluates each branch for its own rows.

use super::{render, TestTable};
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array,
};
use oxhaven_column::parallel::with_threads;
use oxhaven_contracts::expr::{Formula, FormulaCompiler};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_expr::Compiler;
use std::sync::Arc;

/// More than three words of 64 rows and a rest.
const ROWS: usize = 211;

/// `edges` over and over, with a null in every eleventh row.
fn cycled<T: Copy>(edges: &[T]) -> impl Iterator<Item = Option<T>> + '_ {
    (0..ROWS).map(|row| (row % 11 != 5).then(|| edges[row % edges.len()]))
}

/// The literals, each with a column that holds its value in every row.
fn literals() -> Vec<(&'static str, &'static str, ColumnType, ArrayRef)> {
    let ints = |value: i32| -> (ColumnType, ArrayRef) {
        (
            ColumnType::Int,
            Arc::new(Int32Array::from_value(value, ROWS)),
        )
    };
    let longs = |value: i64| -> (ColumnType, ArrayRef) {
        (
            ColumnType::Long,
            Arc::new(Int64Array::from_value(value, ROWS)),
        )
    };
    let doubles = |value: f64| -> (ColumnType, ArrayRef) {
        (
            ColumnType::Double,
            Arc::new(Float64Array::from_value(value, ROWS)),
        )
    };
    let floats = |value: f32| -> (ColumnType, ArrayRef) {
        (
            ColumnType::Float,
            Arc::new(Float32Array::from_value(value, ROWS)),
        )
    };
    [
        ("0", "Ki0", ints(0)),
        ("1", "Ki1", ints(1)),
        ("7", "Ki7", ints(7)),
        ("100", "Ki100", ints(100)),
        ("70000", "Ki70000", ints(70_000)),
        ("2147483647", "KiMax", ints(i32::MAX)),
        ("7L", "Kl7", longs(7)),
        ("3000000000L", "Kl3e9", longs(3_000_000_000)),
        ("4294967295L", "Kl32", longs(u32::MAX as i64)),
        ("4294967296L", "Kl33", longs(u32::MAX as i64 + 1)),
        ("9223372036854775807L", "KlMax", longs(i64::MAX)),
        ("2.5", "Kd25", doubles(2.5)),
        ("0.0", "Kd0", doubles(0.0)),
        ("7.0", "Kd7", doubles(7.0)),
        ("1e300", "Kd300", doubles(1e300)),
        ("2.5f", "Kf25", floats(2.5)),
        ("0.1f", "Kf01", floats(0.1)),
        ("16777217f", "KfBig", floats(16_777_217.0)),
    ]
    .into_iter()
    .map(|(text, name, (column_type, values))| (text, name, column_type, values))
    .collect()
}

/// A column of every primitive type with the values at which the loops could differ from
/// the general kernel: the ends of each type, the value Java reserves for null, zero and
/// its negative, NaN, the infinities, values on both sides of each literal and of 2^32,
/// and nulls; and the columns of [`literals`].
fn table() -> TestTable {
    let b: ArrayRef = Arc::new(Int8Array::from_iter(cycled(&[
        0,
        1,
        -1,
        7,
        6,
        8,
        100,
        99,
        i8::MAX,
        i8::MIN,
        i8::MIN + 1,
        2,
        3,
    ])));
    let c: ArrayRef = Arc::new(UInt16Array::from_iter(cycled(&[
        0,
        1,
        7,
        8,
        6,
        97,
        100,
        101,
        u16::MAX,
        u16::MAX - 1,
        40_000,
    ])));
    let sh: ArrayRef = Arc::new(Int16Array::from_iter(cycled(&[
        0,
        1,
        -1,
        7,
        8,
        100,
        -100,
        i16::MAX,
        i16::MIN,
        i16::MIN + 1,
        3,
        2,
        -7,
    ])));
    let i: ArrayRef = Arc::new(Int32Array::from_iter(cycled(&[
        0,
        1,
        -1,
        7,
        6,
        8,
        -7,
        100,
        69_999,
        70_000,
        70_001,
        i32::MAX,
        i32::MAX - 1,
        i32::MIN,
        i32::MIN + 1,
        16_777_217,
        -13,
    ])));
    let l: ArrayRef = Arc::new(Int64Array::from_iter(cycled(&[
        0,
        1,
        -1,
        7,
        -7,
        100,
        2_999_999_999,
        3_000_000_000,
        3_000_000_001,
        -3_000_000_000,
        u32::MAX as i64,
        u32::MAX as i64 + 1,
        -(u32::MAX as i64),
        -(u32::MAX as i64) - 1,
        i64::MAX,
        i64::MAX - 1,
        i64::MIN,
        i64::MIN + 1,
        12_345_678_901_234,
        70_000,
    ])));
    let f: ArrayRef = Arc::new(Float32Array::from_iter(cycled(&[
        0.0,
        -0.0,
        2.5,
        2.4,
        2.6,
        0.1,
        7.0,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        -f32::MAX,
        f32::MAX,
        f32::MIN_POSITIVE,
        16_777_216.0,
        16_777_218.0,
        -3.5,
    ])));
    let d: ArrayRef = Arc::new(Float64Array::from_iter(cycled(&[
        0.0,
        -0.0,
        2.5,
        2.499_999,
        2.500_001,
        0.1,
        f64::from(0.1_f32),
        7.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -f64::MAX,
        f64::MAX,
        1e300,
        f64::MIN_POSITIVE,
        16_777_217.0,
        3e9,
        -3.5,
        100.0,
    ])));
    let x: ArrayRef = Arc::new(BooleanArray::from_iter(
        (0..ROWS).map(|row| Some(row % 3 == 0 || row % 7 == 2)),
    ));
    let zero: ArrayRef = Arc::new(Int32Array::from_value(0, ROWS));
    let mut columns = vec![
        ("B", (ColumnType::Byte, b)),
        ("C", (ColumnType::Char, c)),
        ("Sh", (ColumnType::Short, sh)),
        ("I", (ColumnType::Int, i)),
        ("L", (ColumnType::Long, l)),
        ("F", (ColumnType::Float, f)),
        ("D", (ColumnType::Double, d)),
        ("X", (ColumnType::Boolean, x)),
        ("Z", (ColumnType::Int, zero)),
    ];
    for (_, name, column_type, values) in literals() {
        columns.push((name, (column_type, values)));
    }
    TestTable::with_previous(
        columns
            .into_iter()
            .map(|(name, column)| (name, column.clone(), column)),
    )
}

/// The formula's values as text, or why it does not compile or fails.
fn outcome(table: &TestTable, text: &str) -> Result<Vec<String>, String> {
    let expr = Compiler.parse(text).map_err(|e| format!("parse: {e}"))?;
    let formula: Arc<dyn Formula> = Compiler
        .compile(&expr, &table.definition)
        .map_err(|e| format!("compile: {e}"))?;
    let values = formula
        .evaluate(table, &table.rows, false)
        .map_err(|e| format!("evaluate: {}", e.message))?;
    let mut rendered = render(&values);
    rendered.push(format!("{:?}", values.data_type()));
    Ok(rendered)
}

const OPERATORS: [&str; 14] = [
    "+", "-", "*", "/", "%", "&", "|", "^", "==", "!=", "<", "<=", ">", ">=",
];

#[test]
fn an_operator_with_a_literal_gives_what_it_gives_with_a_column_of_that_value() {
    let table = table();
    let mut compared = 0;
    for (literal, constant, _, _) in literals() {
        for column in ["B", "C", "Sh", "I", "L", "F", "D"] {
            for operator in OPERATORS {
                for literal_on_left in [false, true] {
                    let with = |operand: &str| match literal_on_left {
                        false => format!("{column} {operator} {operand}"),
                        true => format!("{operand} {operator} {column}"),
                    };
                    let (quick, general) = (with(literal), with(constant));
                    let expected = outcome(&table, &general);
                    let actual = with_threads(1, || outcome(&table, &quick));
                    match (&actual, &expected) {
                        // The texts of the two formulas differ, and so may the message
                        // that quotes them.
                        (Err(a), Err(b)) if a.starts_with("compile") => {
                            assert!(b.starts_with("compile"), "{quick}: {a}; {general}: {b}")
                        }
                        _ => assert!(
                            actual == expected,
                            "{quick} differs from {general}: {:?} against {:?}",
                            actual.as_ref().map(|values| values.len()),
                            expected.as_ref().map(|values| values.len())
                        ),
                    }
                    compared += usize::from(actual.is_ok());
                }
            }
        }
    }
    // Most combinations compile and evaluate: the comparison is not of failures alone.
    assert!(compared > 2500, "only {compared} formulas gave values");
}

/// A formula that calls a function evaluates each branch of a conditional for the rows
/// that take it; one made of operators alone computes both and chooses.
#[test]
fn a_choice_between_computed_branches_is_the_conditional_of_each_row() {
    let table = table();
    for (chosen, selective) in [
        ("X ? I : L", "(X ? I : L) + 0L * abs(Z)"),
        ("X ? L : I", "(X ? L : I) + 0L * abs(Z)"),
        ("I > 7 ? I : -I", "(I > 7 ? I : -I) + 0 * abs(Z)"),
        ("X ? D : F", "(X ? D : F) * (1.0 + 0 * abs(Z))"),
        (
            "D > 2.5 ? F : -F",
            "(D > 2.5 ? F : -F) * (1.0f + 0 * abs(Z))",
        ),
        ("L < 7 ? D : -D", "(L < 7 ? D : -D) * (1.0 + 0 * abs(Z))"),
        ("X ? C : C + 1", "(X ? C : C + 1) + 0 * abs(Z)"),
    ] {
        let expected = outcome(&table, selective);
        assert!(expected.is_ok(), "{selective}: {expected:?}");
        assert!(
            outcome(&table, chosen) == expected,
            "{chosen} differs from {selective}"
        );
    }
}
