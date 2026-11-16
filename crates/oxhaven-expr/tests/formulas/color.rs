//! Colours and the styles of table formats: the functions of `ColorUtilImpl` and `Color`.
//!
//! The expected values are what Deephaven 42.9 returned for the same formulas on the same
//! columns, in a console script.

use super::*;

const RED: i64 = 143833713130660064;
const BLUE: i64 = 72058689286103264;

/// `X = i`, `Y = i * 1.5`, `S = "a" + i` and `N`, null where `i` is even, as the probe
/// table had.
fn table() -> TestTable {
    TestTable::new([
        ("X", ints([0, 1, 2, 3, 4, 5].map(Some))),
        ("Y", doubles([0.0, 1.5, 3.0, 4.5, 6.0, 7.5].map(Some))),
        ("S", strings(["a0", "a1", "a2", "a3", "a4", "a5"].map(Some))),
        ("N", ints([None, Some(1), None, Some(3), None, Some(5)])),
    ])
}

fn all(style: i64) -> Column {
    longs([Some(style); 6])
}

fn each(styles: [i64; 6]) -> Column {
    longs(styles.map(Some))
}

#[test]
fn a_colour_is_its_style_as_an_argument() {
    let table = table();
    table.assert("toLong(RED)", all(RED));
    table.assert("io.deephaven.engine.util.ColorUtil.toLong(RED)", all(RED));
    table.assert("toLong(io.deephaven.gui.color.Color.RED)", all(RED));
    table.assert("toLong(NO_FORMATTING)", all(0));
    table.assert("toLong(`NO_FORMATTING`)", all(0));
    table.assert("toLong(color(`NO_FORMATTING`))", all(72057594069442784));
    table.assert("toLong(DB_PINK)", all(144026879270060032));
    table.assert("toLong(`#FF00AA`)", all(143834443275100384));
    table.assert("toLong(colorRGB(0x102030))", all(76596584227332320));
    table.assert("toLong(colorHSL(120f, 50f, 50f))", all(90282274162999296));
    table.assert("toLong(colorRGB(0.5f, 0.25f, 1f))", all(108157855049244896));
    table.assert(
        "toLong(colorRGB(X*40, 0, 0))",
        each([
            72057594069442784,
            83316593137869024,
            94575592206295264,
            105834591274721504,
            117093590343147744,
            128352589411573984,
        ]),
    );
    table.assert(
        "toLong(colorHSL(X*60, 100, 50))",
        each([
            143833713130660064,
            144114088581005312,
            72337969519788032,
            72339064736448512,
            72058689286103264,
            143834808347320544,
        ]),
    );
    table.assert("toLong(X > 2 ? RED : null)", each([0, 0, 0, RED, RED, RED]));
    table.assert(
        "bgfga(X > 3 ? RED : BLUE)",
        each([BLUE, BLUE, BLUE, BLUE, RED, RED]),
    );
    table.assert(
        "toLong(X > 2 ? `RED` : `BLUE`)",
        each([BLUE, BLUE, BLUE, RED, RED, RED]),
    );
}

#[test]
fn styles_are_packed_as_java_packs_them() {
    let table = table();
    table.assert("bgfg(1,2,3,4,5,6)", all(72341281006945542));
    table.assert("bgfga(200,200,200)", all(128573350715850752));
    table.assert("bgfg(`RED`, `BLUE`)", all(143833713183031551));
    table.assert("bgfg(bg(RED), fg(BLUE))", all(143833713183031551));
    table.assert("bgo(`RED`)", all(287948901175001088));
    table.assert("backgroundOverride(bg(RED))", all(287948901175001088));
    table.assert("fgo(BLUE)", all(117440767));
    table.assert("foreground(`RED`)", all(100597760));
    table.assert("backgroundForegroundAuto(`RED`)", all(RED));
    table.assert("fgo(RED) | bgo(BLUE)", all(216173877464596480));
    table.assert("bg(`#102030`)", all(76596584195817472));
    table.assert("fg(colorRGB(1, 2, 3))", all(83952131));
    table.assert(
        "fg(X, X, X)",
        each([83886080, 83951873, 84017666, 84083459, 84149252, 84215045]),
    );
    table.assert(
        "isBackgroundSelectionOverride(bgo(RED))",
        booleans([Some(true); 6]),
    );
    table.assert(
        "isForegroundSelectionOverride(bgo(RED))",
        booleans([Some(false); 6]),
    );
}

/// The methods take primitives and do not look for null: a null `int` is converted to the
/// null `long`, and a result that is the null `long` is null.
#[test]
fn a_null_is_the_value_java_reserves_for_it() {
    let table = table();
    let fg_null = 2214592512;
    table.assert(
        "fg(N)",
        each([fg_null, 67108864, fg_null, 67108864, fg_null, 67108864]),
    );
    table.assert(
        "bg(N)",
        longs([None, Some(0), None, Some(0), None, Some(0)]),
    );
    table.assert("toLong(N)", each([0, 1, 0, 3, 0, 5]));
    table.assert("toLong((long)N)", each([0, 1, 0, 3, 0, 5]));
    // The null `double` is below every minimum.
    table.assert(
        "heatmap(N, -3e9, 5, RED, BLUE)",
        each([RED, 72058684991135968, RED, 72058684991135968, RED, BLUE]),
    );
}

#[test]
fn heat_maps_mix_two_styles() {
    let table = table();
    table.assert(
        "heatmap(X, 0, 5, RED, GREEN)",
        each([
            143833713130660064,
            129505977109111008,
            115179340599189728,
            100851604577640672,
            86524968067719392,
            72198331557798112,
        ]),
    );
    table.assert(
        "heatmap(X, 0, 5, bg(RED), bg(GREEN))",
        each([
            143833713099145216,
            129505977109111008,
            115179340599189728,
            100851604577640672,
            86524968067719392,
            72198331526283264,
        ]),
    );
    let foregrounds = [100597760, 97779754, 89456810, 83886335, 83886335, 83886335];
    table.assert("heatmapFg(Y, 1, 4, fg(RED), fg(BLUE))", each(foregrounds));
    table.assert("heatmapForeground(Y, 1, 4, RED, BLUE)", each(foregrounds));
    table.assert(
        "heatmapFg(Y, 0, 5, RED, GREEN)",
        each([100597760, 95561216, 90590208, 85553920, 83918848, 83918848]),
    );
    table.assert(
        "heatmap(Y, 1, 4, `RED`, `BLUE`)",
        each([
            143833713130660064,
            131730469520728288,
            95983697234288864,
            72058689286103264,
            72058689286103264,
            72058689286103264,
        ]),
    );
}

#[test]
fn java_s_refusals_are_refusals() {
    let table = table();
    // No overload takes a style and a colour, a colour and a name, or a `double`;
    // `background` has no public overload of `long`; `Color` is no imported class.
    for formula in [
        "bgfg(bg(RED), BLUE)",
        "heatmap(X, 0, 5, RED, `GREEN`)",
        "fg(Y)",
        "background(X)",
        "toLong(Color.RED)",
    ] {
        assert!(
            !matches!(table.compile_error(formula), FormulaError::Syntax(_)),
            "{formula}"
        );
    }
    // A name that is no colour, and a part outside its range, fail when they are computed.
    for formula in [
        "toLong(`nosuch`)",
        "toLong(color(`db_pink`))",
        "toLong(` red`)",
        "toLong(colorRGB(300, 0, 0))",
        "toLong(color(S))",
    ] {
        assert!(table.evaluate(formula).is_err(), "{formula}");
    }
}

/// Java makes a column of `Color` objects of these; no column here has that type.
#[test]
fn a_colour_is_not_the_value_of_a_formula() {
    let table = table();
    for formula in [
        "RED",
        "X > 2 ? RED : BLUE",
        "colorRGB(1, 2, 3)",
        "RED.toHex()",
    ] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Unsupported(_)),
            "{formula}"
        );
    }
    // A column comes before a colour of its name.
    let shadowed = TestTable::new([("RED", ints([Some(7)]))]);
    shadowed.assert("RED + 1", ints([Some(8)]));
}
