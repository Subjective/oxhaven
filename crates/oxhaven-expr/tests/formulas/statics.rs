//! Static methods and fields called through their class: `java.lang.Math`, the boxed
//! classes, `String`, and the classes of the default static imports.
//!
//! The expected values are what Java 42.9 returned for the same formulas on the same
//! columns: over gRPC for the methods a client may call, and in a console script for
//! those of `Math`, which Java refuses a client.

use super::*;
use oxhaven_expr::RequestCompiler;

/// A null in the third row and the type's extreme in the last, as the probe table had.
fn table() -> TestTable {
    TestTable::new([
        ("B", bytes([Some(-1), Some(0), None, Some(2), Some(3)])),
        (
            "S",
            shorts([Some(-100), Some(0), None, Some(200), Some(300)]),
        ),
        (
            "C",
            chars([Some('A'), Some('B'), None, Some('D'), Some('E')]),
        ),
        (
            "I",
            ints([Some(-1), Some(0), None, Some(2), Some(i32::MAX)]),
        ),
        (
            "L",
            longs([Some(-3), Some(0), None, Some(6), Some(i64::MAX)]),
        ),
        (
            "F",
            floats([Some(-0.5), Some(0.0), None, Some(1.0), Some(f32::NAN)]),
        ),
        (
            "D",
            doubles([Some(-0.5), Some(0.0), None, Some(1.0), Some(f64::NAN)]),
        ),
        (
            "Z",
            booleans([Some(true), Some(false), None, Some(false), Some(true)]),
        ),
        (
            "T",
            strings([Some("x-0"), Some("x-1"), None, Some("x-3"), Some("")]),
        ),
        (
            "N",
            strings([Some("-7"), Some("0"), Some("7"), Some("14"), Some("21")]),
        ),
        (
            "TS",
            instants([
                Some(1_710_053_999_123_456_789),
                Some(1_710_057_599_123_456_789),
                None,
                Some(1_710_064_799_123_000_000),
                Some(0),
            ]),
        ),
    ])
}

fn texts<const N: usize>(values: [&str; N]) -> Column {
    strings(values.map(Some))
}

#[test]
fn a_number_prints_as_the_value_the_generated_code_holds() {
    let table = table();
    let ints_printed = ["-1", "0", "-2147483648", "2", "2147483647"];
    table.assert("Integer.toString(I)", texts(ints_printed));
    table.assert("String.valueOf(I)", texts(ints_printed));
    // A narrower argument is converted by `intCast`, which keeps null.
    table.assert(
        "Integer.toString(B)",
        texts(["-1", "0", "-2147483648", "2", "3"]),
    );
    table.assert(
        "String.valueOf(S)",
        texts(["-100", "0", "-2147483648", "200", "300"]),
    );
    table.assert(
        "Long.toString(I)",
        texts(["-1", "0", "-9223372036854775808", "2", "2147483647"]),
    );
    table.assert("String.valueOf(C)", texts(["A", "B", "\u{ffff}", "D", "E"]));
    table.assert(
        "String.valueOf(D)",
        texts(["-0.5", "0.0", "-1.7976931348623157E308", "1.0", "NaN"]),
    );
    table.assert(
        "Float.toString(F)",
        texts(["-0.5", "0.0", "-3.4028235E38", "1.0", "NaN"]),
    );
    table.assert(
        "Double.toString(I)",
        texts([
            "-1.0",
            "0.0",
            "-1.7976931348623157E308",
            "2.0",
            "2.147483647E9",
        ]),
    );
    table.assert(
        "java.lang.String.valueOf(T)",
        texts(["x-0", "x-1", "null", "x-3", ""]),
    );
    table.assert(
        "String.valueOf(TS)",
        texts([
            "2024-03-10T06:59:59.123456789Z",
            "2024-03-10T07:59:59.123456789Z",
            "null",
            "2024-03-10T09:59:59.123Z",
            "1970-01-01T00:00:00Z",
        ]),
    );
}

#[test]
fn radix_and_bit_methods_read_the_null_value_as_a_number() {
    let table = table();
    table.assert(
        "Integer.toString(I, 16)",
        texts(["-1", "0", "-80000000", "2", "7fffffff"]),
    );
    table.assert(
        "Integer.toString(I, 37)",
        texts(["-1", "0", "-2147483648", "2", "2147483647"]),
    );
    table.assert(
        "Long.toString(L, 36)",
        texts(["-3", "0", "-1y2p0ij32e8e8", "6", "1y2p0ij32e8e7"]),
    );
    // The radix of the third row is `NULL_INT`, which is no radix, so 10.
    table.assert(
        "Long.toString(L, I)",
        texts([
            "-3",
            "0",
            "-9223372036854775808",
            "110",
            "9223372036854775807",
        ]),
    );
    table.assert(
        "Integer.toHexString(I)",
        texts(["ffffffff", "0", "80000000", "2", "7fffffff"]),
    );
    table.assert(
        "Long.toHexString(I)",
        texts(["ffffffffffffffff", "0", "8000000000000000", "2", "7fffffff"]),
    );
    table.assert(
        "Integer.toOctalString(I)",
        texts(["37777777777", "0", "20000000000", "2", "17777777777"]),
    );
    table.assert(
        "Long.numberOfLeadingZeros(L)",
        ints([Some(0), Some(64), Some(0), Some(61), Some(1)]),
    );
    table.assert(
        "Long.numberOfTrailingZeros(L)",
        ints([Some(0), Some(64), Some(63), Some(1), Some(0)]),
    );
    table.assert(
        "Integer.bitCount(I)",
        ints([Some(32), Some(0), Some(1), Some(1), Some(31)]),
    );
}

#[test]
fn a_result_is_null_only_where_it_is_the_null_value_of_its_type() {
    let table = table();
    table.assert(
        "Integer.valueOf(I)",
        ints([Some(-1), Some(0), None, Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "Integer.max(I, 0)",
        ints([Some(0), Some(0), Some(0), Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "Integer.min(I, 0)",
        ints([Some(-1), Some(0), None, Some(0), Some(0)]),
    );
    // The sum wraps to `Integer.MIN_VALUE` in the last row, and that is null.
    table.assert(
        "Integer.sum(I, 1)",
        ints([Some(0), Some(1), Some(-2147483647), Some(3), None]),
    );
    table.assert(
        "Long.compare(L, I)",
        ints([Some(-1), Some(0), Some(0), Some(1), Some(1)]),
    );
    table.assert(
        "Double.compare(D, 0.0)",
        ints([Some(-1), Some(0), Some(-1), Some(1), Some(1)]),
    );
    table.assert("Double.isNaN(D)", flags([false, false, false, false, true]));
    table.assert("Double.isNaN(I)", flags([false; 5]));
    table.assert("Float.isNaN(F)", flags([false, false, false, false, true]));
    table.assert("Double.isFinite(D)", flags([true, true, true, true, false]));
    table.assert(
        "Integer.signum(I)",
        ints([Some(-1), Some(0), Some(-1), Some(1), Some(1)]),
    );
}

#[test]
fn text_is_parsed_as_java_parses_it() {
    let table = table();
    table.assert(
        "Integer.parseInt(N)",
        ints([Some(-7), Some(0), Some(7), Some(14), Some(21)]),
    );
    table.assert(
        "Long.valueOf(N)",
        longs([Some(-7), Some(0), Some(7), Some(14), Some(21)]),
    );
    table.assert(
        "Double.parseDouble(N)",
        doubles([Some(-7.0), Some(0.0), Some(7.0), Some(14.0), Some(21.0)]),
    );
    table.assert("Boolean.parseBoolean(T)", flags([false; 5]));
    table.assert("Boolean.parseBoolean(`TRUE`)", flags([true; 5]));
    // A null fails: `NumberFormatException` or `NullPointerException` in Java.
    for formula in [
        "Integer.parseInt(T)",
        "Long.parseLong(T)",
        "Double.parseDouble(T)",
        "Float.parseFloat(T)",
        "Integer.valueOf(T)",
        "String.valueOf(Z)",
        "Boolean.toString(Z)",
    ] {
        assert!(table.evaluate(formula).is_err(), "{formula}");
    }
    table.assert(
        "Boolean.toString(Z == null ? false : Z)",
        texts(["true", "false", "false", "false", "true"]),
    );
}

#[test]
fn math_computes_on_the_null_values() {
    let table = table();
    // `doubleCast` keeps the null of the `int`, and `sin` takes it as a number.
    let of_null = (-f64::MAX).sin();
    let (ColumnType::Double, sines) = table.evaluate("Math.sin(I)").unwrap() else {
        panic!("Math.sin is a double")
    };
    let sines = sines.as_primitive::<arrow::datatypes::Float64Type>();
    assert!(sines.is_valid(2));
    assert_eq!(sines.value(2), of_null);
    table.assert(
        "Math.abs(I)",
        ints([Some(1), Some(0), None, Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "Math.abs(S)",
        ints([Some(100), Some(0), None, Some(200), Some(300)]),
    );
    table.assert(
        "Math.abs(D)",
        doubles([
            Some(0.5),
            Some(0.0),
            Some(f64::MAX),
            Some(1.0),
            Some(f64::NAN),
        ]),
    );
    table.assert(
        "Math.max(I, 2)",
        ints([Some(2), Some(2), Some(2), Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "Math.max(I, L)",
        longs([Some(-1), Some(0), None, Some(6), Some(i64::MAX)]),
    );
    table.assert(
        "Math.max(D, I)",
        doubles([Some(-0.5), Some(0.0), None, Some(2.0), Some(f64::NAN)]),
    );
    table.assert(
        "Math.floor(D)",
        doubles([Some(-1.0), Some(0.0), None, Some(1.0), Some(f64::NAN)]),
    );
    table.assert(
        "Math.round(D)",
        longs([Some(0), Some(0), None, Some(1), Some(0)]),
    );
    table.assert(
        "StrictMath.round(F)",
        ints([Some(0), Some(0), None, Some(1), Some(0)]),
    );
    table.assert(
        "Math.floorMod(I, 3)",
        ints([Some(2), Some(0), Some(1), Some(2), Some(1)]),
    );
    table.assert(
        "java.lang.Math.floorDiv(I, 2)",
        ints([
            Some(-1),
            Some(0),
            Some(-1_073_741_824),
            Some(1),
            Some(1_073_741_823),
        ]),
    );
    table.assert(
        "Math.exp(I) == 0",
        flags([false, false, true, false, false]),
    );
    table.assert("Math.PI", doubles([Some(std::f64::consts::PI); 5]));
    table.assert(
        "java.lang.StrictMath.E",
        doubles([Some(std::f64::consts::E); 5]),
    );
    assert!(table.evaluate("Math.floorMod(I, 0)").is_err());
    let (random_type, _) = table.evaluate("Math.random()").unwrap();
    assert_eq!(random_type, ColumnType::Double);
}

/// Behavior: `ParsingColumnExpressionValidator#validateInvocations`, as the corpus records
/// it for `Math.sin(I)` and its relatives sent as table operations.
#[test]
fn a_client_may_not_call_math_and_may_call_everything_else() {
    let table = table();
    let request = |formula: &str| {
        RequestCompiler
            .parse(formula)
            .and_then(|expr| RequestCompiler.compile(&expr, &table.definition))
            .map(|_| ())
    };
    for (formula, method) in [
        ("Math.sin(I)", "sin(double) on class java.lang.Math"),
        (
            "2 * Math.max(I, L) + 1",
            "max(long, long) on class java.lang.Math",
        ),
        (
            "java.lang.Math.floorMod(L, I)",
            "floorMod(long, int) on class java.lang.Math",
        ),
        ("Math.random()", "random() on class java.lang.Math"),
        (
            "StrictMath.acos(D)",
            "acos(double) on class java.lang.StrictMath",
        ),
        // The first call Java does not allow is the one it names.
        (
            "Math.abs(F) + Math.sqrt(D)",
            "abs(float) on class java.lang.Math",
        ),
    ] {
        assert_eq!(
            request(formula),
            Err(FormulaError::Type(format!(
                "java.lang.IllegalStateException: User expressions are not permitted to use \
                 static method {method}"
            ))),
            "{formula}"
        );
    }
    for formula in [
        "Math.PI * I",
        "sin(I)",
        "Integer.toString(I)",
        "String.valueOf(D)",
        "Long.toHexString(L)",
        "DateTimeUtils.epochNanos(TS)",
    ] {
        assert_eq!(request(formula), Ok(()), "{formula}");
    }
    // A formula that does not compile fails as it does without the check.
    assert!(matches!(
        request("Math.sin(T)"),
        Err(FormulaError::Unsupported(_))
    ));
}

/// The messages are those Java 42.9 logged for the same table operations of a client.
///
/// Behavior: `ParsingColumnExpressionValidator#validateInvocations`
#[test]
fn a_client_may_not_make_an_object_of_a_class() {
    let table = table();
    let range = "io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual";
    for (formula, refusal) in [
        (
            "new Random(L).nextInt(3)".to_string(),
            "instantiate class java.util.Random(long)".to_string(),
        ),
        (
            "1 + new java.util.Random(I).nextInt(I)".to_string(),
            "instantiate class java.util.Random(long)".to_string(),
        ),
        (
            format!("new {range}(0, 10, 5)"),
            format!("instantiate class {range}(double, double, int)"),
        ),
        (
            format!("new {range}(0, 10, 5).index(D)"),
            format!("instantiate class {range}(double, double, int)"),
        ),
    ] {
        let request = RequestCompiler
            .parse(&formula)
            .and_then(|expr| RequestCompiler.compile(&expr, &table.definition))
            .map(|_| ());
        assert_eq!(
            request,
            Err(FormulaError::Type(format!(
                "java.lang.IllegalStateException: User expressions are not permitted to {refusal}"
            ))),
            "{formula}"
        );
        // the console's own compiler takes each
        let expr = Compiler.parse(&formula).unwrap();
        assert!(
            Compiler.compile(&expr, &table.definition).is_ok(),
            "{formula}"
        );
    }
}

#[test]
fn a_default_import_may_be_called_through_its_class() {
    let table = table();
    let nanos = longs([
        Some(1_710_053_999_123_456_789),
        Some(1_710_057_599_123_456_789),
        None,
        Some(1_710_064_799_123_000_000),
        Some(0),
    ]);
    table.assert("DateTimeUtils.epochNanos(TS)", nanos.clone());
    table.assert("io.deephaven.time.DateTimeUtils.epochNanos(TS)", nanos);
    table.assert(
        "io.deephaven.function.Numeric.min(D, 0.25)",
        doubles([
            Some(-0.5),
            Some(0.0),
            Some(0.25),
            Some(0.25),
            Some(f64::NAN),
        ]),
    );
    table.assert(
        "io.deephaven.function.Numeric.abs(I)",
        ints([Some(1), Some(0), None, Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "DateTimeUtils.plus(TS, 1) == TS + 1",
        booleans([Some(true), Some(true), Some(true), Some(true), Some(true)]),
    );
    table.assert("io.deephaven.util.QueryConstants.NULL_INT", ints([None; 5]));
    table.assert("DateTimeUtils.SECOND", longs([Some(1_000_000_000); 5]));
    let (now, _) = table.evaluate("DateTimeUtils.now()").unwrap();
    assert_eq!(now, ColumnType::Instant);
    // A class has only its own methods, and only `DateTimeUtils` goes without a package.
    for formula in [
        "DateTimeUtils.abs(I)",
        "io.deephaven.function.Numeric.now()",
        "DateTimeUtils.plus(I, 1)",
        "io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils.plus(TS, 1)",
        "Numeric.abs(I)",
        "io.deephaven.function.Numeric.NULL_INT",
    ] {
        let error = table.compile_error(formula);
        assert!(
            matches!(error, FormulaError::Type(_) | FormulaError::Unsupported(_)),
            "{formula}: {error:?}"
        );
    }
}

#[test]
fn a_column_comes_before_a_class_of_its_name() {
    let table = TestTable::new([("Math", doubles([Some(1.0)])), ("X", ints([Some(4)]))]);
    assert_eq!(
        table.compile_error("Math.PI"),
        FormulaError::Unsupported("field access .PI".into())
    );
    assert!(matches!(
        table.compile_error("Math.sqrt(X)"),
        FormulaError::Type(_)
    ));
    table.assert("StrictMath.sqrt(X)", doubles([Some(2.0)]));
}

/// The arrays of `formula`, one text per row, after a check of the element type.
fn arrays(table: &TestTable, formula: &str, element: ColumnType) -> Vec<String> {
    let (column_type, values) = table
        .evaluate(formula)
        .unwrap_or_else(|e| panic!("{formula} failed: {e}"));
    assert_eq!(
        column_type,
        ColumnType::Array(Box::new(element)),
        "type of {formula}"
    );
    assert_eq!(
        *values.data_type(),
        column_type.arrow_type(),
        "Arrow type of {formula}"
    );
    (0..values.len())
        .map(|row| arrow::util::display::array_value_to_string(&values, row).unwrap())
        .collect()
}

/// Java's array creation: javac widens an element to the element type as it stands, so a
/// null `byte` in an `int[]` is -128, and an element that holds the null value of the
/// element type is a null element.
#[test]
fn an_array_is_created_from_its_elements() {
    use ColumnType as C;
    let table = table();
    for (formula, element, expected) in [
        (
            "new int[]{I, I+1}",
            C::Int,
            ["[-1, 0]", "[0, 1]", "[, ]", "[2, 3]", "[2147483647, ]"],
        ),
        (
            "new int[]{B, S}",
            C::Int,
            [
                "[-1, -100]",
                "[0, 0]",
                "[-128, -32768]",
                "[2, 200]",
                "[3, 300]",
            ],
        ),
        (
            "new int[]{C}",
            C::Int,
            ["[65]", "[66]", "[65535]", "[68]", "[69]"],
        ),
        (
            "new long[]{I, L}",
            C::Long,
            [
                "[-1, -3]",
                "[0, 0]",
                "[-2147483648, ]",
                "[2, 6]",
                "[2147483647, 9223372036854775807]",
            ],
        ),
        (
            "new double[]{I, D}",
            C::Double,
            [
                "[-1.0, -0.5]",
                "[0.0, 0.0]",
                "[-2147483648.0, ]",
                "[2.0, 1.0]",
                "[2147483647.0, NaN]",
            ],
        ),
        ("new double[]{NULL_INT}", C::Double, ["[-2147483648.0]"; 5]),
        ("new int[]{NULL_INT}", C::Int, ["[]"; 5]),
        (
            "new short[]{1, S}",
            C::Short,
            ["[1, -100]", "[1, 0]", "[1, ]", "[1, 200]", "[1, 300]"],
        ),
        (
            "new String[]{null, T}",
            C::String,
            ["[, x-0]", "[, x-1]", "[, ]", "[, x-3]", "[, ]"],
        ),
        ("new int[3]", C::Int, ["[0, 0, 0]"; 5]),
        ("new double[2]", C::Double, ["[0.0, 0.0]"; 5]),
        ("new String[2]", C::String, ["[, ]"; 5]),
    ] {
        assert_eq!(arrays(&table, formula, element), expected, "{formula}");
    }
    table.assert("new int[]{I}.length", ints([Some(1); 5]));
    table.assert("new int[]{I, 5}[1]", ints([Some(5); 5]));
    table.assert(
        "sum(new int[]{I, 5})",
        longs([Some(4), Some(5), Some(5), Some(7), Some(2147483652)]),
    );
    table.assert(
        "new double[]{D}[0]",
        doubles([Some(-0.5), Some(0.0), None, Some(1.0), Some(f64::NAN)]),
    );
}

/// What javac refuses in an array creation, and the arrays whose type no column has.
#[test]
fn an_array_java_cannot_create_is_refused() {
    let table = table();
    for formula in [
        "new int[]{L}",
        "new float[]{D}",
        "new String[]{I}",
        "new long[]{TS}",
        "new int[]{null}",
        "new int[]{}",
        "new byte[]{I}",
        "new byte[]{128}",
    ] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula} gave {:?}",
            table.compile_error(formula)
        );
    }
    for (formula, construct) in [
        ("new boolean[]{true}", "array creation new boolean[]"),
        ("new Boolean[]{Z}", "array creation new Boolean[]"),
        ("new Integer[]{I}", "array creation new Integer[]"),
        ("new Object[]{T}", "array creation new Object[]"),
        ("new int[2][2]", "array creation new int[][], of arrays"),
        (
            "new int[I]",
            "array creation new int[n] with a length that is not a literal from 1 to 1024",
        ),
    ] {
        assert_eq!(
            table.compile_error(formula),
            FormulaError::Unsupported(construct.into()),
            "{formula}"
        );
    }
}

/// `new Integer(x)` and its relatives: the constructor is chosen and its argument
/// converted as for a static method, and the formula's value is the primitive.
#[test]
fn a_boxed_value_is_constructed_from_its_primitive_or_from_text() {
    let table = table();
    table.assert(
        "new Integer(I)",
        ints([Some(-1), Some(0), None, Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "new java.lang.Integer(B)",
        ints([Some(-1), Some(0), None, Some(2), Some(3)]),
    );
    table.assert(
        "new Integer(C)",
        ints([Some(65), Some(66), None, Some(68), Some(69)]),
    );
    table.assert(
        "new Long(I)",
        longs([Some(-1), Some(0), None, Some(2), Some(2147483647)]),
    );
    table.assert(
        "new Double(F)",
        doubles([Some(-0.5), Some(0.0), None, Some(1.0), Some(f64::NAN)]),
    );
    table.assert(
        "new Byte(B)",
        bytes([Some(-1), Some(0), None, Some(2), Some(3)]),
    );
    table.assert(
        "new Short(S)",
        shorts([Some(-100), Some(0), None, Some(200), Some(300)]),
    );
    table.assert(
        "new Character(C)",
        chars([Some('A'), Some('B'), None, Some('D'), Some('E')]),
    );
    table.assert(
        "new Integer(N)",
        ints([Some(-7), Some(0), Some(7), Some(14), Some(21)]),
    );
    table.assert("new Boolean(T)", booleans([Some(false); 5]));
    table.assert("new Boolean(true)", booleans([Some(true); 5]));
    table.assert("new String(\"a\")", texts(["a"; 5]));
    table.assert("new String()", texts([""; 5]));
    table.assert(
        "new Integer(I) + 1",
        ints([Some(0), Some(1), None, Some(3), None]),
    );
    // The constructors read a null object.
    for formula in ["new String(T)", "new Boolean(Z)"] {
        let error = table.evaluate(formula).unwrap_err();
        assert!(
            error.message.contains("NullPointerException"),
            "{formula} gave {error:?}"
        );
    }
    for (formula, construct) in [
        (
            "new Integer(L)",
            "constructor new Integer() with these arguments",
        ),
        (
            "new Short(B)",
            "constructor new Short() with these arguments",
        ),
        (
            "new Float(D)",
            "constructor new Float() with these arguments",
        ),
        (
            "new String(new char[]{'a'})",
            "constructor new String() with these arguments",
        ),
        ("new Object()", "constructor new Object()"),
        (
            "new java.util.ArrayList()",
            "constructor new java.util.ArrayList()",
        ),
    ] {
        assert_eq!(
            table.compile_error(formula),
            FormulaError::Unsupported(construct.into()),
            "{formula}"
        );
    }
}

/// Behavior: `QueryLanguageFunctionUtils#compareTo` of two numbers: null is below every
/// number and equal to null, and NaN is above every number.
#[test]
fn two_numbers_are_compared_with_null_first_and_nan_last() {
    let table = table();
    for (formula, expected) in [
        ("compareTo(I, L)", [1, 0, 0, -1, -1]),
        ("compareTo(D, D)", [0; 5]),
        ("compareTo(D, I)", [1, 0, 0, -1, 1]),
        ("compareTo(I, 0)", [-1, 0, -1, 1, 1]),
        ("compareTo(F, 0.5)", [-1, -1, -1, 1, 1]),
        ("compareTo(C, 'B')", [-1, 0, -1, 1, 1]),
    ] {
        table.assert(formula, ints(expected.map(Some)));
    }
    for (formula, construct) in [
        (
            "compareTo(L, D)",
            "function compareTo() of a long and a floating point number",
        ),
        ("compareTo(T, T)", "function compareTo()"),
    ] {
        assert_eq!(
            table.compile_error(formula),
            FormulaError::Unsupported(construct.into()),
            "{formula}"
        );
    }
}

/// A function declared with a variable number of arguments gets them as an array.
#[test]
fn a_function_of_an_array_takes_its_elements_as_arguments() {
    let table = table();
    table.assert(
        "median(D, 1.0, 3.0, D)",
        doubles([Some(0.25), Some(0.5), Some(2.0), Some(1.0), Some(f64::NAN)]),
    );
    table.assert(
        "median(I, I, 5)",
        doubles([
            Some(-1.0),
            Some(0.0),
            Some(5.0),
            Some(2.0),
            Some(2147483647.0),
        ]),
    );
    table.assert(
        "absAvg(D, -3.0)",
        doubles([Some(1.75), Some(1.5), Some(3.0), Some(2.0), Some(f64::NAN)]),
    );
    table.assert(
        "avg(I, 4)",
        doubles([
            Some(1.5),
            Some(2.0),
            Some(4.0),
            Some(3.0),
            Some(1073741825.5),
        ]),
    );
    table.assert(
        "sum(I, I)",
        longs([Some(-2), Some(0), None, Some(4), Some(4294967294)]),
    );
    table.assert(
        "count(I, I)",
        longs([Some(2), Some(2), Some(0), Some(2), Some(2)]),
    );
    table.assert(
        "first(I, 2)",
        ints([Some(-1), Some(0), None, Some(2), Some(i32::MAX)]),
    );
    table.assert(
        "median(I)",
        doubles([Some(-1.0), Some(0.0), None, Some(2.0), Some(2147483647.0)]),
    );
    // Java converts arguments of different types, with rules of its own.
    for formula in ["median(I, L)", "absAvg(I, D)"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Unsupported(_)),
            "{formula}"
        );
    }
    table.assert("Calendar.JANUARY", ints([Some(0); 5]));
    table.assert("java.util.Calendar.JANUARY", ints([Some(0); 5]));
}
