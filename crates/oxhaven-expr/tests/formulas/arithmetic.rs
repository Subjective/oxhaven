//! `+ - * / %` and unary minus.

use super::*;

/// QLFU `#plus(int,int)`, `#minus(int,int)`, `#multiply(int,int)`, `#remainder(int,int)`,
/// `#divide(int,int)`: null when either operand is null.
#[test]
fn a_null_operand_makes_the_result_null() {
    let left = || ints([Some(6), None, Some(6), None]);
    let right = || ints([Some(4), Some(4), None, None]);
    assert_binary("L + R", left(), right(), ints([Some(10), None, None, None]));
    assert_binary("L - R", left(), right(), ints([Some(2), None, None, None]));
    assert_binary("L * R", left(), right(), ints([Some(24), None, None, None]));
    assert_binary("L % R", left(), right(), ints([Some(2), None, None, None]));
    assert_binary(
        "L / R",
        left(),
        right(),
        doubles([Some(1.5), None, None, None]),
    );
}

/// QLFU `#plus(int,int)`, `#minus(int,int)`, `#multiply(int,int)`: Java's `int` operators,
/// which wrap. A result of `Integer.MIN_VALUE` is `NULL_INT`.
#[test]
fn int_overflow_wraps_and_a_wrap_onto_the_null_value_is_null() {
    assert_binary(
        "L + R",
        ints([Some(i32::MAX), Some(i32::MAX), Some(-2_147_483_647)]),
        ints([Some(1), Some(2), Some(-2)]),
        ints([None, Some(-2_147_483_647), Some(2_147_483_647)]),
    );
    assert_binary(
        "L - R",
        ints([Some(-2_147_483_647), Some(-2_147_483_647)]),
        ints([Some(1), Some(2)]),
        ints([None, Some(2_147_483_647)]),
    );
    assert_binary(
        "L * R",
        ints([Some(65_536), Some(46_341), Some(65_536), Some(-65_536)]),
        ints([Some(65_536), Some(46_341), Some(32_768), Some(32_768)]),
        // The last row does not overflow: -2^31 is simply the null value.
        ints([Some(0), Some(-2_147_479_015), None, None]),
    );
}

/// QLFU `#plus(long,long)`, `#multiply(long,long)`: Java's `long` operators, which wrap.
#[test]
fn long_overflow_wraps_and_a_wrap_onto_the_null_value_is_null() {
    assert_binary(
        "L + R",
        longs([Some(i64::MAX), Some(i64::MAX)]),
        longs([Some(1), Some(2)]),
        longs([None, Some(-9_223_372_036_854_775_807)]),
    );
    assert_binary(
        "L * R",
        longs([Some(4_294_967_296), Some(3_037_000_500)]),
        longs([Some(4_294_967_296), Some(3_037_000_500)]),
        longs([Some(0), Some(-9_223_372_036_709_301_616)]),
    );
}

/// QLFU `#plus(byte,byte)`, `#plus(short,short)`, `#plus(char,int)`, `#multiply(char,char)`,
/// `#minus(char,char)`, `#remainder(short,byte)`: both operands are cast to `int`.
#[test]
fn byte_short_and_char_compute_as_int() {
    assert_binary(
        "L + R",
        bytes([Some(127), Some(-127), None]),
        bytes([Some(1), Some(-1), Some(1)]),
        // -128 is the null value of byte, not of int.
        ints([Some(128), Some(-128), None]),
    );
    assert_binary(
        "L + R",
        shorts([Some(32_767)]),
        shorts([Some(1)]),
        ints([Some(32_768)]),
    );
    assert_binary(
        "L + R",
        chars([Some('a'), None]),
        ints([Some(1), Some(1)]),
        ints([Some(98), None]),
    );
    assert_binary(
        "L * R",
        chars([Some('\u{ea60}')]),
        chars([Some('\u{ea60}')]),
        // 60000 * 60000 wraps as an int.
        ints([Some(-694_967_296)]),
    );
    assert_binary(
        "L - R",
        chars([Some('a')]),
        chars([Some('b')]),
        ints([Some(-1)]),
    );
    assert_binary(
        "L % R",
        shorts([Some(7)]),
        bytes([Some(3)]),
        ints([Some(1)]),
    );
}

/// QLFU `#plus(byte,long)`, `#plus(int,long)`, `#plus(long,float)`, `#multiply(int,double)`,
/// `#plus(float,double)`, `#multiply(float,int)`: the narrower operand is cast to the wider
/// type, and `long` with `float` is `float`.
#[test]
fn mixed_operands_compute_in_the_wider_type() {
    assert_binary(
        "L + R",
        bytes([Some(1)]),
        longs([Some(i64::MAX - 1)]),
        longs([Some(i64::MAX)]),
    );
    assert_binary(
        "L + R",
        ints([Some(i32::MAX)]),
        longs([Some(1)]),
        longs([Some(2_147_483_648)]),
    );
    assert_binary(
        "L + R",
        longs([Some(16_777_217)]),
        floats([Some(0.0)]),
        // (float) 16777217L is 16777216f.
        floats([Some(16_777_216.0)]),
    );
    assert_binary(
        "L * R",
        ints([Some(3)]),
        doubles([Some(0.5)]),
        doubles([Some(1.5)]),
    );
    assert_binary(
        "L + R",
        floats([Some(0.1)]),
        doubles([Some(0.0)]),
        // (double) 0.1f.
        doubles([Some(0.100_000_001_490_116_12)]),
    );
    assert_binary(
        "L * R",
        floats([Some(0.5)]),
        ints([Some(3)]),
        floats([Some(1.5)]),
    );
}

/// QLFU `#divide(int,int)`: `((double) a) / ((double) b)`, so never an integer division and
/// never an error.
#[test]
fn integer_division_is_floating_point_division() {
    assert_binary(
        "L / R",
        ints([Some(7), Some(1), Some(-1), Some(0), None, Some(0)]),
        ints([Some(2), Some(0), Some(0), Some(0), Some(0), Some(-1)]),
        doubles([
            Some(3.5),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            None,
            Some(-0.0),
        ]),
    );
    assert_binary(
        "L / R",
        longs([Some(i64::MAX)]),
        longs([Some(1)]),
        doubles([Some(9.223_372_036_854_776e18)]),
    );
    assert_binary(
        "L / R",
        chars([Some('a')]),
        chars([Some('b')]),
        doubles([Some(97.0 / 98.0)]),
    );
}

/// QLFU `#divide(int,float)` returns `float` (`((float) a) / b`), `#divide(float,int)`
/// returns `double` (`((double) a) / ((double) b)`), `#divide(float,float)` returns `float`
/// and `#divide(double,float)` returns `double`.
#[test]
fn division_by_a_float_is_float_and_division_of_a_float_is_double() {
    assert_binary(
        "L / R",
        ints([Some(1)]),
        floats([Some(3.0)]),
        floats([Some(0.333_333_34)]),
    );
    assert_binary(
        "L / R",
        floats([Some(0.1)]),
        ints([Some(3)]),
        doubles([Some(f64::from(0.1_f32) / 3.0)]),
    );
    assert_binary(
        "L / R",
        floats([Some(1.0), Some(1.0)]),
        floats([Some(3.0), Some(0.0)]),
        floats([Some(0.333_333_34), Some(f32::INFINITY)]),
    );
    assert_binary(
        "L / R",
        doubles([Some(1.0)]),
        floats([Some(3.0)]),
        doubles([Some(1.0 / 3.0)]),
    );
}

/// QLFU `#remainder(int,int)`: Java's `%`, which truncates toward zero and takes the sign
/// of the dividend.
#[test]
fn integer_remainder_takes_the_sign_of_the_dividend() {
    assert_binary(
        "L % R",
        ints([Some(7), Some(-7), Some(7), Some(-7), None]),
        ints([Some(3), Some(3), Some(-3), Some(-3), Some(0)]),
        // The last row is null, not a failure: the null check comes before the `%`.
        ints([Some(1), Some(-1), Some(1), Some(-1), None]),
    );
}

/// QLFU `#remainder(int,int)`, `#remainder(long,long)`, `#remainder(int,long)`: Java's `%`
/// throws `ArithmeticException` for a zero divisor.
#[test]
fn integer_remainder_by_zero_fails() {
    let ints_by_zero = TestTable::new([
        ("L", ints([Some(4), Some(5)])),
        ("R", ints([Some(2), Some(0)])),
    ]);
    assert!(ints_by_zero.evaluate("L % R").is_err());
    assert!(ints_by_zero.evaluate("L % 0").is_err());
    // The same rows divide without failing.
    ints_by_zero.assert("L / R", doubles([Some(2.0), Some(f64::INFINITY)]));

    let longs_by_zero = TestTable::new([
        ("L", longs([Some(5)])),
        ("R", longs([Some(0)])),
        ("I", ints([Some(5)])),
    ]);
    assert!(longs_by_zero.evaluate("L % R").is_err());
    assert!(longs_by_zero.evaluate("I % R").is_err());
}

/// QLFU `#remainder(double,double)`, `#remainder(int,double)`: Java's floating point `%`,
/// for which a zero divisor gives NaN.
#[test]
fn floating_remainder_truncates_and_never_fails() {
    assert_binary(
        "L % R",
        doubles([
            Some(5.5),
            Some(-5.5),
            Some(5.5),
            Some(1.0),
            Some(f64::INFINITY),
            Some(2.0),
        ]),
        doubles([
            Some(2.0),
            Some(2.0),
            Some(-2.0),
            Some(0.0),
            Some(2.0),
            Some(f64::INFINITY),
        ]),
        doubles([
            Some(1.5),
            Some(-1.5),
            Some(1.5),
            Some(f64::NAN),
            Some(f64::NAN),
            Some(2.0),
        ]),
    );
    assert_binary(
        "L % R",
        ints([Some(5)]),
        doubles([Some(0.0)]),
        doubles([Some(f64::NAN)]),
    );
}

/// QLFU `#multiply(float,int)`, `#multiply(double,double)`: the result is returned as is,
/// and `-Float.MAX_VALUE` and `-Double.MAX_VALUE` are `NULL_FLOAT` and `NULL_DOUBLE`.
#[test]
fn a_floating_result_equal_to_the_null_value_is_null() {
    assert_binary(
        "L * R",
        floats([Some(f32::MAX), Some(f32::MAX)]),
        ints([Some(-1), Some(-2)]),
        floats([None, Some(f32::NEG_INFINITY)]),
    );
    assert_binary(
        "L * R",
        doubles([Some(f64::MAX), Some(f64::MAX)]),
        doubles([Some(-1.0), Some(1.0)]),
        doubles([None, Some(f64::MAX)]),
    );
}

/// QLFU `#negate(byte)`, `#negate(short)`, `#negate(char)` return `int`; `#negate(int)`,
/// `#negate(long)`, `#negate(float)`, `#negate(double)` return their own type. All return
/// null for null.
#[test]
fn unary_minus_promotes_small_integers_and_keeps_null() {
    let table = TestTable::new([
        ("B", bytes([Some(5), None, Some(-127)])),
        ("S", shorts([Some(5), None, Some(-32_767)])),
        ("C", chars([Some('a'), None, Some('\u{fffe}')])),
        ("I", ints([Some(5), None, Some(-2_147_483_647)])),
        ("L", longs([Some(5), None, Some(i64::MAX)])),
        ("F", floats([Some(0.0), None, Some(f32::NAN)])),
        ("D", doubles([Some(-0.0), None, Some(f64::INFINITY)])),
    ]);
    table.assert("-B", ints([Some(-5), None, Some(127)]));
    table.assert("-S", ints([Some(-5), None, Some(32_767)]));
    table.assert("-C", ints([Some(-97), None, Some(-65_534)]));
    table.assert("-I", ints([Some(-5), None, Some(2_147_483_647)]));
    table.assert("-L", longs([Some(-5), None, Some(-i64::MAX)]));
    table.assert("-F", floats([Some(-0.0), None, Some(f32::NAN)]));
    table.assert("-D", doubles([Some(0.0), None, Some(f64::NEG_INFINITY)]));
    table.assert("- -I", ints([Some(5), None, Some(-2_147_483_647)]));
}

/// `QueryLanguageParser#visit(IntegerLiteralExpr)` types a literal beyond the `int` range as
/// `long`, `#visit(DoubleLiteralExpr)` types a trailing `f` as `float`, and
/// `#visit(CharLiteralExpr)` types a char literal as `char`. The operators then apply as
/// they do to columns: QLFU `#negate(long)`, `#plus(int,int)`, `#minus(long,long)`,
/// `#negate(double)`, `#plus(char,int)`.
#[test]
fn literals_have_java_types_and_go_through_the_same_operators() {
    let table = TestTable::new([("X", ints([Some(0)]))]);
    // 2147483648 does not fit an int, so this is the negation of a long.
    table.assert("-2147483648", longs([Some(-2_147_483_648)]));
    table.assert("-2147483647", ints([Some(-2_147_483_647)]));
    table.assert("-2147483647 - 1", ints([None]));
    table.assert("2147483647 + 1", ints([None]));
    table.assert("0x7fffffff + 0x1", ints([None]));
    table.assert("2147483647 + 1L", longs([Some(2_147_483_648)]));
    table.assert("-9223372036854775807L - 1", longs([None]));
    table.assert("0x8000000000000000L", longs([None]));
    table.assert("1 + 2L", longs([Some(3)]));
    table.assert("1.5f * 2", floats([Some(3.0)]));
    table.assert("0.1f + 0.2f", floats([Some(0.1_f32 + 0.2_f32)]));
    table.assert("0.1 + 0.2", doubles([Some(0.300_000_000_000_000_04)]));
    table.assert("7 / 2", doubles([Some(3.5)]));
    table.assert("'a' + 1", ints([Some(98)]));
    table.assert("'a'", chars([Some('a')]));
    table.assert("1e308 * 10", doubles([Some(f64::INFINITY)]));
    table.assert("-1.7976931348623157E308", doubles([None]));
    table.assert("-3.4028235E38f", floats([None]));
    table.assert("-0.0", doubles([Some(-0.0)]));
    table.assert("X + 1", ints([Some(1)]));
    assert!(table.evaluate("5 % 0").is_err());
    assert!(table.evaluate("5L % X").is_err());
}

#[test]
fn precedence_and_grouping_decide_the_value() {
    let table = TestTable::new([("X", ints([Some(4)]))]);
    table.assert("2 + 3 * X", ints([Some(14)]));
    table.assert("(2 + 3) * X", ints([Some(20)]));
    table.assert("10 - X - 3", ints([Some(3)]));
    table.assert("100 / 10 / X", doubles([Some(2.5)]));
    table.assert("100 % 7 * X", ints([Some(8)]));
    table.assert("-2 * -X", ints([Some(8)]));
    table.assert("2 - -X", ints([Some(6)]));
}

/// `io.deephaven.util.QueryConstants`: Java stores null as a reserved value of each
/// primitive type, so a cell holding that value is null to every operator, whatever its
/// validity bit says.
#[test]
fn a_cell_holding_the_null_value_of_its_type_is_null() {
    let table = TestTable::new([
        ("B", bytes([Some(i8::MIN), Some(1)])),
        ("S", shorts([Some(i16::MIN), Some(1)])),
        ("C", chars([Some('\u{ffff}'), Some('a')])),
        ("I", ints([Some(i32::MIN), Some(1)])),
        ("L", longs([Some(i64::MIN), Some(1)])),
        ("F", floats([Some(-f32::MAX), Some(1.0)])),
        ("D", doubles([Some(-f64::MAX), Some(1.0)])),
        ("T", instants([Some(i64::MIN), Some(1)])),
    ]);
    for column in ["B", "S", "C", "I", "L", "F", "D", "T"] {
        table.assert(&format!("isNull({column})"), flags([true, false]));
    }
    table.assert("B + 1", ints([None, Some(2)]));
    table.assert("C + 1", ints([None, Some(98)]));
    table.assert("I + 1", ints([None, Some(2)]));
    table.assert("L - 1", longs([None, Some(0)]));
    table.assert("F / 2", doubles([None, Some(0.5)]));
    table.assert("D * 0", doubles([None, Some(0.0)]));
    table.assert("I", ints([None, Some(1)]));
    table.assert("T", instants([None, Some(1)]));
}
