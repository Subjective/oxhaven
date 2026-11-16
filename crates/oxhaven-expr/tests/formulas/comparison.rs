//! `== != < <= > >=`, comparison with `null`, and `isNull`.

use super::*;

/// Asserts the six comparisons of columns `L` and `R`, in the order `==`, `!=`, `<`, `<=`,
/// `>`, `>=`.
fn assert_comparisons<const N: usize>(left: Column, right: Column, expected: [[bool; N]; 6]) {
    let table = TestTable::new([("L", left), ("R", right)]);
    for (op, expected) in ["==", "!=", "<", "<=", ">", ">="].iter().zip(expected) {
        table.assert(&format!("L {op} R"), flags(expected));
    }
}

/// QLFU `#eq(int,int)`: null equals null and nothing else. `#compareTo(int,int)`, behind
/// `#less(int,int)`, `#lessEquals`, `#greater`, `#greaterEquals`: null is less than every
/// other value. `!=` is `!eq(a, b)` (`QueryLanguageParser#visit(BinaryExpr)`).
#[test]
fn null_equals_null_and_is_less_than_everything_else() {
    assert_comparisons(
        ints([Some(1), Some(2), Some(3), None, Some(1), None]),
        ints([Some(2), Some(2), Some(2), Some(1), None, None]),
        [
            [false, true, false, false, false, true],
            [true, false, true, true, true, false],
            [true, false, false, true, false, false],
            [true, true, false, true, false, true],
            [false, false, true, false, true, false],
            [false, true, true, false, true, true],
        ],
    );
}

/// QLFU `#eq(double,double)`: Java's `==`, false for NaN. `#less(double,double)` and its
/// siblings return false when either operand is NaN, before looking at nulls, so
/// `null < NaN` is false although null is less than every other value, down to `-Infinity`.
#[test]
fn nan_is_unequal_and_unordered_even_against_null() {
    let nan = f64::NAN;
    assert_comparisons(
        doubles([Some(nan), Some(1.0), Some(nan), Some(nan), None, None, None]),
        doubles([
            Some(1.0),
            Some(nan),
            Some(nan),
            None,
            Some(nan),
            Some(1.0),
            Some(f64::NEG_INFINITY),
        ]),
        [
            [false, false, false, false, false, false, false],
            [true, true, true, true, true, true, true],
            [false, false, false, false, false, true, true],
            [false, false, false, false, false, true, true],
            [false, false, false, false, false, false, false],
            [false, false, false, false, false, false, false],
        ],
    );
    assert_comparisons(
        floats([Some(f32::NAN), None, Some(1.0)]),
        floats([Some(f32::NAN), Some(f32::NAN), Some(f32::INFINITY)]),
        [
            [false, false, false],
            [true, true, true],
            [false, false, true],
            [false, false, true],
            [false, false, false],
            [false, false, false],
        ],
    );
}

/// QLFU `#eq(double,double)` is Java's `==`, for which `-0.0 == 0.0`. `#compareTo(double,double)`
/// is `Double.compare`, which orders `-0.0` below `0.0`, so `-0.0 < 0.0` is true and
/// `0.0 <= -0.0` is false.
#[test]
fn negative_zero_equals_zero_but_orders_below_it() {
    assert_comparisons(
        doubles([Some(-0.0), Some(0.0), Some(0.0)]),
        doubles([Some(0.0), Some(-0.0), Some(0.0)]),
        [
            [true, true, true],
            [false, false, false],
            [true, false, false],
            [true, false, true],
            [false, true, false],
            [false, true, true],
        ],
    );
}

/// QLFU `#compareTo(int,double)` is `Double.compare(a, b)`, so the int `0` is above `-0.0`.
/// `#compareTo(long,double)` compares the truncated double with the long, so the long `0`
/// equals `-0.0`.
#[test]
fn an_int_zero_is_above_negative_zero_and_a_long_zero_equals_it() {
    assert_comparisons(
        ints([Some(0)]),
        doubles([Some(-0.0)]),
        [[true], [false], [false], [false], [true], [true]],
    );
    assert_comparisons(
        longs([Some(0)]),
        doubles([Some(-0.0)]),
        [[true], [false], [false], [true], [false], [true]],
    );
}

/// QLFU `#eq(long,double)` is `a == b`, which converts the long to double and so loses its
/// low bits. `#compareTo(long,double)` is exact, and `#compareTo(double,long)` negates it.
/// So a long can equal a double and also be greater than it.
#[test]
fn a_long_can_equal_a_double_and_be_greater_than_it() {
    let above_2_to_53 = 9_007_199_254_740_993;
    let two_to_53 = 9_007_199_254_740_992.0;
    assert_comparisons(
        longs([Some(above_2_to_53), Some(i64::MAX), Some(i64::MAX), Some(5)]),
        doubles([
            Some(two_to_53),
            Some(9_223_372_036_854_775_808.0),
            Some(f64::INFINITY),
            Some(5.5),
        ]),
        [
            [true, true, false, false],
            [false, false, true, true],
            [false, false, true, true],
            [false, true, true, true],
            [true, false, false, false],
            [true, true, false, false],
        ],
    );
    assert_comparisons(
        doubles([Some(two_to_53), Some(5.5), Some(-5.5)]),
        longs([Some(above_2_to_53), Some(5), Some(-5)]),
        [
            [true, false, false],
            [false, true, true],
            [true, false, true],
            [true, false, true],
            [false, true, false],
            [false, true, false],
        ],
    );
}

/// QLFU `#eq(int,float)` and `#eq(long,float)` are `a == b`, which converts the integer to
/// float. `#compareTo(int,float)` is `Float.compare(a, b)`, which converts too, while
/// `#compareTo(long,float)` is exact. `#eq(int,double)` and `#compareTo(int,double)` convert
/// to double, which holds every int.
#[test]
fn an_integer_against_a_float_compares_as_float_except_a_long_ordering() {
    assert_comparisons(
        ints([Some(16_777_217)]),
        floats([Some(16_777_216.0)]),
        [[true], [false], [false], [true], [false], [true]],
    );
    assert_comparisons(
        longs([Some(16_777_217)]),
        floats([Some(16_777_216.0)]),
        [[true], [false], [false], [false], [true], [true]],
    );
    assert_comparisons(
        floats([Some(16_777_216.0)]),
        longs([Some(16_777_217)]),
        [[true], [false], [true], [true], [false], [false]],
    );
    assert_comparisons(
        ints([Some(16_777_217)]),
        doubles([Some(16_777_216.0)]),
        [[false], [true], [false], [false], [true], [true]],
    );
}

/// QLFU `#eq(float,double)` and `#compareTo(float,double)` widen the float, and
/// `(double) 0.1f` is above `0.1`.
#[test]
fn a_float_against_a_double_compares_as_double() {
    assert_comparisons(
        floats([Some(0.1), Some(0.5), None]),
        doubles([Some(0.1), Some(0.5), None]),
        [
            [false, true, true],
            [true, false, false],
            [false, false, false],
            [false, true, true],
            [true, false, false],
            [true, true, true],
        ],
    );
}

/// QLFU `#compareTo(char,char)`, `#eq(char,int)`, `#compareTo(byte,char)`,
/// `#compareTo(short,byte)`: Java's integer comparison after widening, with null first
/// although `NULL_CHAR` is the largest char.
#[test]
fn chars_and_small_integers_compare_as_integers() {
    assert_comparisons(
        chars([Some('a'), Some('b'), None, Some('a'), None]),
        chars([Some('b'), Some('a'), Some('a'), None, None]),
        [
            [false, false, false, false, true],
            [true, true, true, true, false],
            [true, false, true, false, false],
            [true, false, true, false, true],
            [false, true, false, true, false],
            [false, true, false, true, true],
        ],
    );
    let table = TestTable::new([
        ("C", chars([Some('a'), Some('b'), None])),
        ("I", ints([Some(97), Some(97), Some(97)])),
        ("B", bytes([Some(-1), Some(98), None])),
        ("S", shorts([Some(300), Some(-300), Some(0)])),
    ]);
    table.assert("C == I", flags([true, false, false]));
    table.assert("C > I", flags([false, true, false]));
    table.assert("C == 'a'", flags([true, false, false]));
    table.assert("C < 'b'", flags([true, false, true]));
    table.assert("C == 97", flags([true, false, false]));
    table.assert("B < C", flags([true, false, false]));
    table.assert("B == C", flags([false, true, true]));
    table.assert("S > B", flags([true, false, true]));
    table.assert("S >= 300L", flags([true, false, false]));
}

/// QLFU `#eq(Object,Object)` is `equals` with null equal to null. `#compareTo(Comparable,Comparable)`
/// puts null first and otherwise calls `String#compareTo`, which orders by UTF-16 code unit.
#[test]
fn strings_compare_by_utf16_code_unit_with_null_first() {
    assert_comparisons(
        strings([
            Some("a"),
            Some("b"),
            None,
            None,
            Some("b"),
            Some("\u{1F600}"),
            Some("ab"),
        ]),
        strings([
            Some("a"),
            Some("a"),
            Some("a"),
            None,
            None,
            Some("\u{FF5E}"),
            Some("b"),
        ]),
        [
            [true, false, false, true, false, false, false],
            [false, true, true, false, true, true, true],
            [false, false, true, false, false, true, true],
            [true, false, true, true, false, true, true],
            [false, true, false, false, true, false, false],
            [true, true, false, true, true, false, false],
        ],
    );
    let table = TestTable::new([("Sym", strings([Some("AAPL"), Some("MSFT"), None]))]);
    table.assert("Sym == `AAPL`", flags([true, false, false]));
    table.assert("Sym = `AAPL`", flags([true, false, false]));
    table.assert("Sym != \"AAPL\"", flags([false, true, true]));
    table.assert("Sym < `B`", flags([true, false, true]));
    table.assert("`B` < Sym", flags([false, true, false]));
}

/// QLFU `#eq(Object,Object)` for `Boolean` operands.
#[test]
fn booleans_compare_for_equality_with_null_equal_to_null() {
    let table = TestTable::new([
        (
            "L",
            booleans([Some(true), Some(false), None, None, Some(true)]),
        ),
        (
            "R",
            booleans([Some(true), Some(true), None, Some(false), None]),
        ),
    ]);
    table.assert("L == R", flags([true, false, true, false, false]));
    table.assert("L != R", flags([false, true, false, true, true]));
    table.assert("L == true", flags([true, false, false, false, true]));
    table.assert("false != L", flags([true, false, true, true, true]));
    table.assert("(L == R) == (R == L)", flags([true; 5]));
}

/// QLFU `#eq(Object,Object)` and `#compareTo(Comparable,Comparable)` for `Instant` operands:
/// `Instant#equals` and `Instant#compareTo`, with null first.
#[test]
fn instants_compare_in_time_order_with_null_first() {
    assert_comparisons(
        instants([Some(1), Some(5), None, Some(-3)]),
        instants([Some(2), Some(5), Some(7), None]),
        [
            [false, true, false, false],
            [true, false, true, true],
            [true, false, true, false],
            [true, true, true, false],
            [false, false, false, true],
            [false, true, false, true],
        ],
    );
}

/// `QueryLanguageParser#visit(BinaryExpr)` rewrites `x == null` to `isNull(x)` and `x != null`
/// to `!isNull(x)`; `io.deephaven.function.Basic#isNull` is true exactly for null.
#[test]
fn comparison_with_the_null_literal_is_is_null() {
    let table = TestTable::new([
        ("I", ints([Some(1), None])),
        ("D", doubles([Some(f64::NAN), None])),
        ("S", strings([Some(""), None])),
        ("B", booleans([Some(false), None])),
        ("T", instants([Some(0), None])),
        ("C", chars([Some('\0'), None])),
    ]);
    for column in ["I", "D", "S", "B", "T", "C"] {
        table.assert(&format!("{column} == null"), flags([false, true]));
        table.assert(&format!("null == {column}"), flags([false, true]));
        table.assert(&format!("{column} = null"), flags([false, true]));
        table.assert(&format!("{column} != null"), flags([true, false]));
        table.assert(&format!("null != {column}"), flags([true, false]));
        table.assert(&format!("isNull({column})"), flags([false, true]));
        table.assert(&format!("!isNull({column})"), flags([true, false]));
    }
    table.assert("isNull(I + 1)", flags([false, true]));
    table.assert("(I + 1) == null", flags([false, true]));
    // A comparison is a primitive boolean and is never null.
    table.assert("isNull(I > 0)", flags([false, false]));
    table.assert("isNull(1)", flags([false, false]));
}

/// The null literal beside an operator other than `==` and `!=`, as Java 42.9 answered for
/// these formulas: `QueryLanguageFunctionUtils#less(Comparable, Comparable)` and its
/// relatives order null first, `#binaryAnd(Boolean, Boolean)` and its relatives are null
/// when either side is, and `DateTimeUtils#minus(Instant, Instant)` of null is null.
#[test]
fn the_null_literal_is_an_operand_where_java_finds_one_method() {
    let table = TestTable::new([
        ("S", strings([Some("a"), None])),
        ("B", booleans([Some(true), None])),
        ("T", instants([Some(0), None])),
    ]);
    for column in ["S", "B", "T"] {
        table.assert(&format!("{column} < null"), flags([false, false]));
        table.assert(&format!("null < {column}"), flags([true, false]));
        table.assert(&format!("{column} <= null"), flags([false, true]));
        table.assert(&format!("null <= {column}"), flags([true, true]));
        table.assert(&format!("{column} > null"), flags([true, false]));
        table.assert(&format!("null > {column}"), flags([false, false]));
        table.assert(&format!("{column} >= null"), flags([true, true]));
        table.assert(&format!("null >= {column}"), flags([false, true]));
    }
    table.assert("\"a\" > null", flags([true, true]));
    table.assert("null == null", flags([true, true]));
    table.assert("null != null", flags([false, false]));
    for formula in [
        "B & null",
        "null | B",
        "B ^ null",
        "false & null",
        "true | null",
        "null & null",
        "null | null",
    ] {
        table.assert(formula, booleans([None, None]));
    }
    table.assert("null - T", longs([None, None]));
    for formula in [
        "null < null",
        "null >= null",
        "T - null",
        "T + null",
        "B + null",
        "S - null",
        "S * null",
    ] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula} gave {:?}",
            table.compile_error(formula)
        );
    }
}

/// A struct column of `column_type` (`Period` or `ZonedDateTime`) from its field arrays,
/// null where `valid` is false.
fn struct_column(column_type: ColumnType, fields: Vec<ArrayRef>, valid: &[bool]) -> Column {
    let DataType::Struct(names) = column_type.arrow_type() else {
        panic!("{column_type:?} is no struct")
    };
    let nulls = arrow::buffer::NullBuffer::from(valid.to_vec());
    let array = arrow::array::StructArray::new(names, fields, Some(nulls));
    (column_type, Arc::new(array))
}

fn dates<const N: usize>(values: [Option<i32>; N]) -> Column {
    let array = arrow::array::Date32Array::from(values.to_vec());
    (ColumnType::LocalDate, Arc::new(array))
}

/// The table of the probes: a `LocalDate` `D`, a `Duration` `Dur`, a `Period` `P`, a
/// `ZonedDateTime` `ZD`, a `ZoneId` `Z`, a `String` `S` and an `int` `I`, each null in the
/// first row but `I`.
fn time_objects() -> TestTable {
    let valid = [false, true, true, true];
    let ints32 = |values: [i32; 4]| -> ArrayRef { Arc::new(Int32Array::from(values.to_vec())) };
    let march_10 = 19_792;
    let durations = arrow::array::DurationNanosecondArray::from(vec![
        None,
        Some(1_000_000_000),
        Some(2_000_000_000),
        Some(3_000_000_000),
    ]);
    let zoned_instants: ArrayRef = Arc::new(
        TimestampNanosecondArray::from(vec![0, 1_710_028_800_000_000_000, 0, 0])
            .with_data_type(ColumnType::Instant.arrow_type()),
    );
    let zone_ids: ArrayRef = Arc::new(StringArray::from(vec!["", "UTC", "UTC", "UTC"]));
    let zones = StringArray::from(vec![None, Some("UTC"), Some("UTC"), Some("UTC")]);
    let text = Some("2024-03-10");
    TestTable::new([
        (
            "D",
            dates([None, Some(march_10), Some(march_10 + 1), Some(march_10 + 2)]),
        ),
        ("Dur", (ColumnType::Duration, Arc::new(durations))),
        (
            "P",
            struct_column(
                ColumnType::Period,
                vec![ints32([0, 1, 0, 0]), ints32([0; 4]), ints32([0, 0, 2, 3])],
                &valid,
            ),
        ),
        (
            "ZD",
            struct_column(
                ColumnType::ZonedDateTime,
                vec![zoned_instants, zone_ids],
                &valid,
            ),
        ),
        ("Z", (ColumnType::ZoneId, Arc::new(zones))),
        ("S", strings([None, text, text, text])),
        ("I", ints([Some(0), Some(1), Some(2), Some(3)])),
    ])
}

/// QLFU `#eq(Object, Object)` between a `java.time` value and a value of another class
/// (equal only when both are null; a primitive is boxed, so never null) or of its own
/// class (`equals`), and `#compareTo(Comparable, Comparable)` against `null` (null
/// first). Expected values seen on Java 42.9 for the same formulas on the same columns.
#[test]
fn time_objects_compare_with_other_classes_and_null_as_java_does() {
    let table = time_objects();
    for (formula, expected) in [
        // `2024-03-10` is the `int` 2011.
        ("D == 2024-03-10", [false; 4]),
        ("D != 2024-03-10", [true; 4]),
        ("D == `2024-03-10`", [false; 4]),
        ("D == S", [true, false, false, false]),
        ("D == I", [false; 4]),
        ("D == 3.5", [false; 4]),
        ("D == true", [false; 4]),
        ("D == 'PT1s'", [false; 4]),
        ("D == '2024-03-10'", [false, true, false, false]),
        ("Dur == P", [true, false, false, false]),
        ("Z == `UTC`", [false; 4]),
        ("ZD == ZD", [true; 4]),
        ("Dur < null", [false; 4]),
        ("Dur > null", [false, true, true, true]),
        ("Dur <= null", [true, false, false, false]),
        ("Dur >= null", [true; 4]),
        ("null < Dur", [false, true, true, true]),
        ("D > null", [false, true, true, true]),
        ("ZD < null", [false; 4]),
        ("null >= ZD", [true, false, false, false]),
        ("P == 'P1Y'", [false, true, false, false]),
        ("'P1Y' == P", [false, true, false, false]),
        ("P != 'P1Y'", [true, false, true, true]),
        ("P == 'P2D'", [false, false, true, false]),
        ("P == 'P12M'", [false; 4]),
        ("P == `P1Y`", [false; 4]),
        ("P == P", [true; 4]),
        ("P == 3", [false; 4]),
    ] {
        table.assert(formula, flags(expected));
    }
}

/// QLFU `#compareTo(Comparable, Comparable)` of two classes: `compareTo` throws
/// `ClassCastException` once two values that are not null meet, and the formula fails;
/// `Period` and `ZoneId` are no `Comparable`, so Java finds no method. Seen on Java 42.9.
#[test]
fn ordering_two_classes_fails_where_two_values_meet() {
    let table = time_objects();
    for formula in ["D < S", "D < 2024-03-10", "Dur < 5", "S < I", "D < ZD"] {
        let error = table.evaluate(formula).expect_err(formula);
        assert!(
            error.message.contains("ClassCastException"),
            "{formula}: {error:?}"
        );
    }
    for formula in ["P < null", "P < P", "Dur < P", "Z < null", "null >= Z"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula} gave {:?}",
            table.compile_error(formula)
        );
    }
    // Where one side is null in every row, nothing is compared and null is first.
    let nulls = TestTable::new([
        ("D", dates([None, None])),
        ("S", strings([None, Some("a")])),
        ("I", ints([Some(0), None])),
    ]);
    nulls.assert("D < I", flags([true, true]));
    nulls.assert("D < S", flags([false, true]));
    nulls.assert("3 < D", flags([false, false]));
    nulls.assert("D >= 3.5", flags([false, false]));
}
