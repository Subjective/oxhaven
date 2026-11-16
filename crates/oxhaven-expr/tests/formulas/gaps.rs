//! Operators on vectors and arrays, time literals with a zone, durations and periods, the
//! clock and the random numbers.
//!
//! The expected values are what Java 42.9 returned for the same formulas on the same
//! tables, read from a `view` of each.

use super::*;
use arrow::array::ListArray;
use arrow::buffer::OffsetBuffer;
use arrow::util::display::array_value_to_string;

/// A column of lists with the given element type and one list per entry; `None` is a null
/// list.
fn lists(column_type: ColumnType, elements: ArrayRef, lengths: &[Option<usize>]) -> Column {
    let DataType::List(field) = column_type.arrow_type() else {
        panic!("{column_type:?} is not a list type")
    };
    let nulls = lengths.iter().map(Option::is_some).collect::<Vec<bool>>();
    let list = ListArray::new(
        field,
        OffsetBuffer::from_lengths(lengths.iter().map(|length| length.unwrap_or(0))),
        elements,
        nulls.contains(&false).then(|| nulls.into()),
    );
    (column_type, Arc::new(list))
}

fn array_of(element: ColumnType) -> ColumnType {
    ColumnType::Array(Box::new(element))
}

/// `X = i`, `A = new int[]{i, NULL_INT, 7}`, `B = new double[]{1.5, i, NULL_DOUBLE}`,
/// `C = new int[]{i, 2}`, `S = new String[]{`a`, null}`, `Z = new boolean[]{true, i > 1}`,
/// `N` an `int[]` that is null in the second row, and `V` the same values as `A` in a
/// vector, in three rows.
fn table() -> TestTable {
    let a = Int32Array::from(vec![
        Some(0),
        None,
        Some(7),
        Some(1),
        None,
        Some(7),
        Some(2),
        None,
        Some(7),
    ]);
    let b = Float64Array::from(vec![
        Some(1.5),
        Some(0.0),
        None,
        Some(1.5),
        Some(1.0),
        None,
        Some(1.5),
        Some(2.0),
        None,
    ]);
    let c = Int32Array::from(vec![0, 2, 1, 2, 2, 2]);
    let s = StringArray::from(vec![Some("a"), None, Some("a"), None, Some("a"), None]);
    let z = BooleanArray::from(vec![true, false, true, false, true, true]);
    let n = Int32Array::from(vec![0, 2]);
    let three = [Some(3); 3];
    let two = [Some(2); 3];
    TestTable::new([
        ("X", ints([Some(0), Some(1), Some(2)])),
        ("L", longs([Some(0), None, Some(2)])),
        (
            "A",
            lists(array_of(ColumnType::Int), Arc::new(a.clone()), &three),
        ),
        (
            "B",
            lists(array_of(ColumnType::Double), Arc::new(b), &three),
        ),
        ("C", lists(array_of(ColumnType::Int), Arc::new(c), &two)),
        ("S", lists(array_of(ColumnType::String), Arc::new(s), &two)),
        ("Z", lists(array_of(ColumnType::Boolean), Arc::new(z), &two)),
        (
            "N",
            lists(
                array_of(ColumnType::Int),
                Arc::new(n),
                &[Some(1), None, Some(1)],
            ),
        ),
        (
            "V",
            lists(
                ColumnType::Vector(Box::new(ColumnType::Int)),
                Arc::new(a),
                &three,
            ),
        ),
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

fn assert_shown(formula: &str, element: ColumnType, expected: [&str; 3]) {
    let (column_type, printed) = shown(&table(), formula);
    assert_eq!(column_type, array_of(element), "{formula}");
    assert_eq!(printed, expected, "{formula}");
}

/// Behavior: `QueryLanguageFunctionUtils#plusArray`, `#divideArray`, `#remainderArray` and
/// their relatives: the operator of one value, on each element.
#[test]
fn arithmetic_with_an_array_is_arithmetic_with_each_element() {
    use ColumnType::{Double, Int, Long};
    for (formula, element, expected) in [
        ("A + 1", Int, ["[1, , 8]", "[2, , 8]", "[3, , 8]"]),
        ("1 + A", Int, ["[1, , 8]", "[2, , 8]", "[3, , 8]"]),
        ("V + 1", Int, ["[1, , 8]", "[2, , 8]", "[3, , 8]"]),
        ("A + B", Double, ["[1.5, , ]", "[2.5, , ]", "[3.5, , ]"]),
        ("A + X", Int, ["[0, , 7]", "[2, , 8]", "[4, , 9]"]),
        (
            "A / 2",
            Double,
            ["[0.0, , 3.5]", "[0.5, , 3.5]", "[1.0, , 3.5]"],
        ),
        ("A % 2", Int, ["[0, , 1]", "[1, , 1]", "[0, , 1]"]),
        ("A * 2L", Long, ["[0, , 14]", "[2, , 14]", "[4, , 14]"]),
        (
            "A + 'c'",
            Int,
            ["[99, , 106]", "[100, , 106]", "[101, , 106]"],
        ),
        // A null value beside the array makes every element null.
        ("A - L", Long, ["[0, , 7]", "[, , ]", "[0, , 5]"]),
        ("V + V", Int, ["[0, , 14]", "[2, , 14]", "[4, , 14]"]),
    ] {
        assert_shown(formula, element, expected);
    }
}

/// Behavior: `QueryLanguageFunctionUtils#eqArray`, `#lessArray`, `#greaterEqualsArray`:
/// null is equal to null and less than every value.
#[test]
fn a_comparison_with_an_array_compares_each_element() {
    let truth = ColumnType::Boolean;
    for (formula, expected) in [
        ("A == 7", ["[false, false, true]"; 3]),
        (
            "A < B",
            [
                "[true, true, false]",
                "[true, true, false]",
                "[false, true, false]",
            ],
        ),
        (
            "A >= B",
            [
                "[false, false, true]",
                "[false, false, true]",
                "[true, false, true]",
            ],
        ),
        ("S == `a`", ["[true, false]"; 3]),
        ("S < `b`", ["[true, true]"; 3]),
        (
            "Z == true",
            ["[true, false]", "[true, false]", "[true, true]"],
        ),
        ("(A == 7) == (A == 7)", ["[true, true, true]"; 3]),
    ] {
        assert_shown(formula, truth.clone(), expected);
    }
    // The result is an array like any other.
    let table = table();
    table.assert("(A + 1)[0]", ints([Some(1), Some(2), Some(3)]));
    table.assert("sum(A + 1)", longs([Some(9), Some(10), Some(11)]));
    table.assert("(V + 1).length", ints([Some(3); 3]));
    table.assert("A == null", flags([false; 3]));
}

#[test]
fn what_java_refuses_for_an_array_operand_is_refused() {
    let table = table();
    // Java asserts that `!` has a `boolean` operand, finds no `negate(int[])` and no
    // `binaryAndArray(boolean[], boolean)`, and takes no array for a condition.
    for formula in ["A != 7", "-A", "A > 1 ? 1 : 2", "(double[]) A"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula}: {:?}",
            table.compile_error(formula)
        );
    }
    // Computed by Java and not here: a bitwise operator on arrays, and the text of an
    // array in a concatenation, which is its identity hash.
    for formula in ["A & 1", "Z & true", "S + `x`"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Unsupported(_)),
            "{formula}: {:?}",
            table.compile_error(formula)
        );
    }
    // Java fails when it reads the rows: arrays of different lengths, and a null array.
    let lengths = table.evaluate("A + C").unwrap_err();
    assert!(
        lengths.message.contains("different length"),
        "{}",
        lengths.message
    );
    let null = table.evaluate("N + 1").unwrap_err();
    assert!(
        null.message.contains("NullPointerException"),
        "{}",
        null.message
    );
}

#[test]
fn a_null_cast_to_an_array_type_is_a_null_array() {
    let table = table();
    for (formula, element) in [
        ("(int[])(null)", ColumnType::Int),
        ("(double[]) null", ColumnType::Double),
        ("(String[]) null", ColumnType::String),
    ] {
        let (column_type, values) = table.evaluate(formula).unwrap();
        assert_eq!(column_type, array_of(element), "{formula}");
        assert_eq!(values.null_count(), 3, "{formula}");
        assert_eq!(*values.data_type(), column_type.arrow_type(), "{formula}");
    }
    let (column_type, _) = table.evaluate("(long[][]) null").unwrap();
    assert_eq!(column_type, array_of(array_of(ColumnType::Long)));
    // A cast to the array's own type does nothing.
    assert_shown("(int[]) C", ColumnType::Int, ["[0, 2]", "[1, 2]", "[2, 2]"]);
}

/// `T` is `'2024-03-10T01:30:00 ET' + ii * HOUR`, across the hour the clocks skip.
fn times() -> TestTable {
    TestTable::new([
        ("X", ints([Some(0), Some(1), Some(2)])),
        ("N", ints([Some(0), None, Some(2)])),
        ("L", longs([Some(0), None, Some(2)])),
        (
            "T",
            instants([
                Some(1_710_052_200_000_000_000),
                Some(1_710_055_800_000_000_000),
                Some(1_710_059_400_000_000_000),
            ]),
        ),
    ])
}

/// Behavior: `DateTimeUtils#plus(Instant, Duration)`, `#plus(Instant, Period)`, the two
/// `minus`, `#multiply(Duration, long)`, `#multiply(Period, int)` and
/// `#divide(Duration, long)`.
#[test]
fn durations_and_periods_are_added_to_instants() {
    let table = times();
    let start = 1_704_085_200_000_000_000_i64; // 2024-01-01T00:00:00 ET
    let minute = 60_000_000_000_i64;
    let hour = 60 * minute;
    let day = 24 * hour;
    let each = |values: [i64; 3]| instants(values.map(Some));
    for (formula, expected) in [
        (
            "'2024-01-01T00:00:00 ET' + 'PT1m' * ii",
            each([start, start + minute, start + 2 * minute]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 'PT1m' * L",
            instants([Some(start), None, Some(start + 2 * minute)]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + X * 'P1d'",
            each([start, start + day, start + 2 * day]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 'P1d' * N",
            instants([Some(start), None, Some(start + 2 * day)]),
        ),
        ("'2024-01-01T00:00:00 ET' - 'PT1h'", each([start - hour; 3])),
        (
            "'2024-01-01T00:00:00 ET' + 'PT1h' + 'PT1h'",
            each([start + 2 * hour; 3]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 'pt1H30m'",
            each([start + hour + 30 * minute; 3]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + '-PT1H'",
            each([start - hour; 3]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 'P1DT1H'",
            each([start + day + hour; 3]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 'P2W'",
            each([start + 14 * day; 3]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 2 * 'PT1h' * X",
            each([start, start + 2 * hour, start + 4 * hour]),
        ),
        (
            "'2024-01-01T00:00:00 ET' + 'PT1h' * -X",
            each([start, start - hour, start - 2 * hour]),
        ),
        (
            "T + 'PT1.5S'",
            each([
                1_710_052_201_500_000_000,
                1_710_055_801_500_000_000,
                1_710_059_401_500_000_000,
            ]),
        ),
        (
            "T - 'P1W'",
            each([
                1_709_447_400_000_000_000,
                1_709_451_000_000_000_000,
                1_709_454_600_000_000_000,
            ]),
        ),
        (
            "T + 'PT0.000000001S' * L",
            instants([
                Some(1_710_052_200_000_000_000),
                None,
                Some(1_710_059_400_000_000_002),
            ]),
        ),
    ] {
        table.assert(formula, expected);
    }
    table.assert("T == '2024-03-10T01:30:00 ET'", flags([true, false, false]));
}

#[test]
fn what_java_refuses_for_a_duration_or_period_is_refused() {
    let table = times();
    // No `plus(Duration, Instant)`, `multiply(Period, long)` or `multiply(Duration, double)`.
    for formula in [
        "'PT1h' + T",
        "T + 'P1d' * ii",
        "T + 'PT1h' * 2.5",
        "X + 'PT1h'",
    ] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula}: {:?}",
            table.compile_error(formula)
        );
    }
    // Java makes a column of durations, and fails reading a period of a month or a year
    // added to an `Instant`.
    for formula in ["'PT1h' * 2", "'PT1h'", "'P1d'", "T + 'P1m'", "T + 'P1Y'"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Unsupported(_)),
            "{formula}: {:?}",
            table.compile_error(formula)
        );
    }
    // Java fails when it reads the rows.
    for formula in [
        "T + 'PT1h' / X",
        "T + 'P1d' * 2147483647",
        "T + 'PT2562047H' * 2",
    ] {
        assert!(table.evaluate(formula).is_err(), "{formula}");
    }
}

/// Behavior: `DateTimeUtils#now` and `io.deephaven.function.Random`. The values have
/// nothing to be compared with; the types and the failures are Java's.
#[test]
fn the_clock_and_the_random_numbers_have_javas_types() {
    let table = times();
    for (formula, expected) in [
        ("now()", ColumnType::Instant),
        ("now() + i * SECOND", ColumnType::Instant),
        ("random()", ColumnType::Double),
        ("randomBool()", ColumnType::Boolean),
        ("randomInt(0, 10)", ColumnType::Int),
        ("randomInt(X, 10)", ColumnType::Int),
        ("randomLong(0, 10)", ColumnType::Long),
        ("randomFloat(0, 1)", ColumnType::Float),
        ("randomDouble(0, 1)", ColumnType::Double),
        ("randomDouble(X, X + 1)", ColumnType::Double),
        ("randomDouble(NULL_DOUBLE, 1)", ColumnType::Double),
        ("randomGaussian(0.0, 1.0)", ColumnType::Double),
        (
            "(ii % 3 == 0) ? NULL_DOUBLE : randomDouble(0, 1)",
            ColumnType::Double,
        ),
    ] {
        let (column_type, values) = table
            .evaluate(formula)
            .unwrap_or_else(|e| panic!("{formula} failed: {e}"));
        assert_eq!(column_type, expected, "{formula}");
        assert_eq!(*values.data_type(), expected.arrow_type(), "{formula}");
        assert_eq!(values.len(), 3, "{formula}");
    }
    let (_, within) = table.evaluate("randomDouble(X, X + 1)").unwrap();
    let within = within.as_primitive::<Float64Type>();
    for row in 0..3 {
        let low = row as f64;
        assert!((low..low + 1.0).contains(&within.value(row)), "row {row}");
    }
    for formula in ["randomDouble(1, 0)", "randomInt(5, 5)"] {
        assert!(table.evaluate(formula).is_err(), "{formula}");
    }
    // No overload takes these.
    for formula in ["randomInt(0L, 10L)", "randomFloat(0.0, 1.0)"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula}"
        );
    }
}
