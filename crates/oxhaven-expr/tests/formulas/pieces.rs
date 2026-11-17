//! Formulas over more rows than one piece of the evaluator (2^16 rows): the values and the
//! failure must be those of one pass over all rows, on any number of threads.
//!
//! The reference is the same formula evaluated for at most 2^16 rows at a time, which the
//! evaluator does in one pass on the calling thread.

use super::{render, Column, TestTable};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use oxhaven_column::parallel::with_threads;
use oxhaven_contracts::expr::{Formula, FormulaCompiler};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use oxhaven_expr::Compiler;
use std::sync::Arc;

const PIECE: usize = 1 << 16;

/// A fixed stream of numbers.
struct Stream(u64);

impl Stream {
    fn next(&mut self) -> u64 {
        // SplitMix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// `None` one time in eight.
    fn maybe<T>(&mut self, value: impl FnOnce(u64) -> T) -> Option<T> {
        let draw = self.next();
        (!draw.is_multiple_of(8)).then(|| value(draw >> 8))
    }
}

/// A table of `rows` rows with a column of every primitive type, a `Boolean` column without
/// nulls and a string column. The numbers are small, so that results collide and sums stay
/// in range, with now and then a null, the value Java reserves for null, a NaN, an
/// infinity and a negative zero.
fn table(rows: usize, seed: u64) -> TestTable {
    let mut stream = Stream(seed);
    let mut column = |make: &mut dyn FnMut(&mut Stream) -> ArrayRef| make(&mut stream);
    let small = |draw: u64| (draw % 201) as i64 - 100;
    let b: ArrayRef = column(&mut |s| {
        Arc::new(Int8Array::from_iter(
            (0..rows).map(|_| s.maybe(|d| small(d) as i8)),
        ))
    });
    let c: ArrayRef = column(&mut |s| {
        Arc::new(UInt16Array::from_iter(
            (0..rows).map(|_| s.maybe(|d| (d % 500) as u16)),
        ))
    });
    let sh: ArrayRef = column(&mut |s| {
        Arc::new(Int16Array::from_iter(
            (0..rows).map(|_| s.maybe(|d| small(d) as i16)),
        ))
    });
    let i: ArrayRef = column(&mut |s| {
        Arc::new(Int32Array::from_iter((0..rows).map(|_| {
            s.maybe(|d| match d % 97 {
                0 => i32::MIN,
                1 => i32::MAX,
                _ => small(d) as i32,
            })
        })))
    });
    let l: ArrayRef = column(&mut |s| {
        Arc::new(Int64Array::from_iter((0..rows).map(|_| {
            s.maybe(|d| match d % 89 {
                0 => i64::MIN,
                1 => i64::MAX,
                _ => small(d),
            })
        })))
    });
    let f: ArrayRef = column(&mut |s| {
        Arc::new(Float32Array::from_iter((0..rows).map(|_| {
            s.maybe(|d| match d % 53 {
                0 => f32::NAN,
                1 => -0.0,
                2 => -f32::MAX,
                _ => small(d) as f32 / 4.0,
            })
        })))
    });
    let d: ArrayRef = column(&mut |s| {
        Arc::new(Float64Array::from_iter((0..rows).map(|_| {
            s.maybe(|d| match d % 59 {
                0 => f64::NAN,
                1 => -0.0,
                2 => -f64::MAX,
                3 => f64::INFINITY,
                _ => small(d) as f64 / 8.0,
            })
        })))
    });
    let x: ArrayRef = column(&mut |s| {
        Arc::new(BooleanArray::from_iter(
            (0..rows).map(|_| Some(s.next().is_multiple_of(3))),
        ))
    });
    let t: ArrayRef = column(&mut |s| {
        Arc::new(StringArray::from_iter(
            (0..rows).map(|_| s.maybe(|d| format!("t{}", d % 40))),
        ))
    });
    TestTable::new([
        ("B", (ColumnType::Byte, b)),
        ("C", (ColumnType::Char, c)),
        ("Sh", (ColumnType::Short, sh)),
        ("I", (ColumnType::Int, i)),
        ("L", (ColumnType::Long, l)),
        ("F", (ColumnType::Float, f)),
        ("D", (ColumnType::Double, d)),
        ("X", (ColumnType::Boolean, x)),
        ("T", (ColumnType::String, t)),
    ])
}

fn compile(table: &TestTable, formula: &str) -> Arc<dyn Formula> {
    let expr = Compiler
        .parse(formula)
        .unwrap_or_else(|e| panic!("{formula} did not parse: {e}"));
    Compiler
        .compile(&expr, &table.definition)
        .unwrap_or_else(|e| panic!("{formula} did not compile: {e}"))
}

/// The formula's values for all rows, evaluated for at most `PIECE` rows at a time.
fn one_pass_each(table: &TestTable, formula: &dyn Formula, rows: usize) -> Vec<String> {
    let mut values = Vec::with_capacity(rows);
    let mut first = 0;
    while first < rows {
        let last = (first + PIECE).min(rows) - 1;
        let part = formula
            .evaluate(table, &RowSet::from_range(first as i64, last as i64), false)
            .expect("values");
        values.extend(render(&part));
        first = last + 1;
    }
    values
}

const FORMULAS: [&str; 30] = [
    "I * 2 + 1",
    "D * 1.5 - I",
    "L % 7",
    "B + Sh",
    "C + 1",
    "F / D",
    "F % 3",
    "L * L - I",
    "I & 12 | 3",
    "L ^ I",
    "-F",
    "-I",
    "(short)(I + B)",
    "(byte)L",
    "(int)(D * 100)",
    "(char)(C + 60000)",
    "(float)D + F",
    "I > 50 ? D : -D",
    "X ? I : L",
    "X ? T : `none`",
    "D > 3 ? (I > 0 ? L : I) : Sh",
    "I >= -20 && I < 60",
    "D > F || L == I",
    "X && I != 7",
    "D == D",
    "L < D",
    "!(F <= Sh)",
    "T + `_` + I",
    "ii % 3 + i",
    "k * 2 + B",
];

/// Row counts on both sides of the boundaries between pieces.
const SIZES: [usize; 5] = [PIECE, PIECE + 1, 2 * PIECE - 1, 2 * PIECE, 3 * PIECE + 17];

#[test]
fn many_rows_give_the_values_of_one_pass_on_any_number_of_threads() {
    for (seed, rows) in SIZES.into_iter().enumerate() {
        let table = table(rows, seed as u64);
        for text in FORMULAS {
            let formula = compile(&table, text);
            let expected = one_pass_each(&table, &*formula, rows);
            assert_eq!(expected.len(), rows);
            for threads in [1, 2, 4] {
                let actual = with_threads(threads, || formula.evaluate(&table, &table.rows, false))
                    .unwrap_or_else(|e| panic!("{text} failed: {e}"));
                assert_eq!(
                    actual.data_type(),
                    &formula.result_type().arrow_type(),
                    "Arrow type of {text}"
                );
                assert!(
                    render(&actual) == expected,
                    "{text} over {rows} rows on {threads} threads differs from one pass"
                );
            }
        }
    }
}

/// Results whose pieces are written into one buffer: a piece without a null beside pieces
/// with nulls, and none with a null at all, for a number and for an `Instant`, whose Arrow
/// type carries a time zone.
#[test]
fn pieces_with_and_without_nulls_share_one_buffer() {
    let rows = 4 * PIECE + 9;
    // Nulls in the second piece and at the very end only.
    let null_at =
        |row: usize| (PIECE..2 * PIECE).contains(&row) && row.is_multiple_of(3) || row == rows - 1;
    let numbers: Int32Array = (0..rows)
        .map(|row| (!null_at(row)).then_some(row as i32 % 1000))
        .collect();
    let instants: TimestampNanosecondArray = (0..rows)
        .map(|row| (!null_at(row)).then_some(row as i64 * 1_000_000_007))
        .collect();
    let instants = instants.with_data_type(ColumnType::Instant.arrow_type());
    let flags: BooleanArray = (0..rows).map(|row| Some(row % 5 != 0)).collect();
    let table = TestTable::new([
        ("I", (ColumnType::Int, Arc::new(numbers) as ArrayRef)),
        ("Ts", (ColumnType::Instant, Arc::new(instants) as ArrayRef)),
        ("X", (ColumnType::Boolean, Arc::new(flags) as ArrayRef)),
    ]);
    for text in [
        "I * 3 - 1",
        "X ? Ts : null",
        "(long)I + ii",
        "ii * 2",
        "X ? I : 7",
    ] {
        let formula = compile(&table, text);
        let expected = one_pass_each(&table, &*formula, rows);
        for threads in [1, 3] {
            let actual = with_threads(threads, || formula.evaluate(&table, &table.rows, false))
                .unwrap_or_else(|e| panic!("{text} failed: {e}"));
            assert_eq!(actual.data_type(), &formula.result_type().arrow_type());
            assert_eq!(actual.len(), rows);
            assert!(
                render(&actual) == expected,
                "{text} on {threads} threads differs from one pass"
            );
            // Only `ii * 2` has no null; the others have them where the columns do.
            let nulls = if text == "ii * 2" {
                0
            } else {
                expected.iter().filter(|v| *v == "null").count()
            };
            assert_eq!(actual.null_count(), nulls, "{text}");
        }
    }
}

/// A selection of rows that is not a range, longer than a piece.
#[test]
fn scattered_rows_give_the_values_of_one_pass() {
    let rows = 4 * PIECE;
    let table = table(rows, 11);
    let selection = RowSet::from_keys((0..rows as i64).filter(|key| key % 3 != 1));
    let positions: Vec<usize> = selection.iter().map(|key| key as usize).collect();
    for text in FORMULAS {
        let formula = compile(&table, text);
        let all = one_pass_each(&table, &*formula, rows);
        let expected: Vec<&String> = positions.iter().map(|row| &all[*row]).collect();
        let actual = with_threads(3, || formula.evaluate(&table, &selection, false))
            .unwrap_or_else(|e| panic!("{text} failed: {e}"));
        assert!(
            render(&actual).iter().collect::<Vec<_>>() == expected,
            "{text} over scattered rows differs from one pass"
        );
    }
}

fn failing_table(rows: usize) -> TestTable {
    let a: ArrayRef = Arc::new(Int32Array::from_iter_values(0..rows as i32));
    // zero in the last row only
    let z: ArrayRef = Arc::new(Int32Array::from_iter_values(
        (0..rows).map(|row| i32::from(row != rows - 1)),
    ));
    // null in the first row only
    let n: ArrayRef = Arc::new(BooleanArray::from_iter(
        (0..rows).map(|row| (row != 0).then_some(true)),
    ));
    TestTable::new([
        ("A", (ColumnType::Int, a)),
        ("Z", (ColumnType::Int, z)),
        ("N", (ColumnType::Boolean, n)),
    ])
}

fn failure_of(result: Result<ArrayRef, TableFailure>) -> String {
    result.expect_err("the formula fails").message
}

/// One pass evaluates the whole left operand first, so it reports the division by zero of
/// the last row, although the first row's null condition fails in the first piece.
#[test]
fn a_failure_in_a_late_piece_is_reported_as_one_pass_reports_it() {
    let rows = 3 * PIECE + 5;
    let table = failing_table(rows);
    let formula = compile(&table, "(A % Z) + (N ? 1 : 2)");
    for threads in [1, 2, 4] {
        let message = failure_of(with_threads(threads, || {
            formula.evaluate(&table, &table.rows, false)
        }));
        assert_eq!(
            message, "java.lang.ArithmeticException: / by zero",
            "{threads} threads"
        );
    }
    // The first piece alone fails otherwise: the test tells the two failures apart.
    let first = RowSet::from_range(0, PIECE as i64 - 1);
    assert!(failure_of(formula.evaluate(&table, &first, false)).contains("NullPointerException"));
    // A failure in the last piece only, and none: values again.
    let formula = compile(&table, "A % Z");
    assert_eq!(
        failure_of(with_threads(4, || formula.evaluate(
            &table,
            &table.rows,
            false
        ))),
        "java.lang.ArithmeticException: / by zero"
    );
    let but_last = RowSet::from_range(0, rows as i64 - 2);
    let values = with_threads(4, || formula.evaluate(&table, &but_last, false)).expect("values");
    assert_eq!(values.len(), rows - 1);
    assert_eq!(values.null_count(), 0);
}

/// A branch or a right operand that would fail in rows that do not take it does not fail,
/// and a null there does not count: only the rows that need it are judged.
#[test]
fn branches_not_taken_and_operands_not_needed_do_not_fail() {
    let rows = PIECE + 9;
    let table = failing_table(rows);
    // `Z` is zero where `A` is `rows - 1`, the one row that takes the other branch.
    let (_, kept): Column = table
        .evaluate(&format!("A < {} ? A % Z : -1", rows - 1))
        .expect("the failing branch is not taken");
    assert_eq!(render(&kept)[rows - 1], "-1");
    assert_eq!(render(&kept)[5], "0");
    let (_, both) = table
        .evaluate(&format!("A == {} || A % Z == 0", rows - 1))
        .expect("the failing operand is not needed");
    assert_eq!(render(&both)[rows - 1], "true");
    // `N` is null in row 0 only, where the left operand decides.
    let (_, decided) = table.evaluate("A > 0 && N").expect("the null is not read");
    assert_eq!(render(&decided)[0], "false");
    assert_eq!(render(&decided)[1], "true");
    // Where it is needed it fails as before.
    assert!(failure_of(table.evaluate("A >= 0 && N").map(|c| c.1)).contains("NullPointerException"));
    assert_eq!(
        failure_of(table.evaluate("A >= 0 ? A % Z : -1").map(|c| c.1)),
        "java.lang.ArithmeticException: / by zero"
    );
}
