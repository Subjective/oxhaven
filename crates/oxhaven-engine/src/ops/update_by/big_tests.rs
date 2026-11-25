//! `update_by` of `BigDecimal` and `BigInteger` columns: running totals, windows of rows and
//! of time, their failures, the math context, and what stays unported.
//!
//! The expected values were seen on Deephaven 42.9 as `BigDecimal#toString` of the result
//! cells (a client is sent a decimal without the zeros at the end of its digits). The
//! fixture is the first step of the oracle scenario `ticking_update_by_big_numbers`, made
//! there from text with `new BigDecimal(Txt)`; the ticking test is its second step.

use super::{
    update_by, update_by_controlled, update_by_with, DeltaControl, EmSpec, MathContext,
    RollingWindow, Rounding, UpdateByControl, UpdateByError, UpdateByOperation, UpdateBySpec,
    WindowScale,
};
use crate::ops::select::update_view;
use crate::ops::sort::testing::{cells, graph, scripted, static_table, Ticking};
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Float64Array, Int32Array, StringArray,
    TimestampNanosecondArray,
};
use oxhaven_column::java_values::{
    big_decimal_cell, big_decimal_parts, big_decimal_text, big_integer_cell, big_integer_text,
};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{test_seeds, ConsistencyChecker, RandomUpdates, TableChange, TableSnapshot};
use std::sync::Arc;

fn op(spec: UpdateBySpec, columns: &[&str]) -> UpdateByOperation {
    UpdateByOperation::new(spec, columns)
}

/// The cell of `text` as `new BigDecimal(String)` reads it: digits with an optional point
/// and an optional exponent.
fn decimal(text: &str) -> Vec<u8> {
    let (mantissa, exponent) = match text.split_once('E') {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().expect("an exponent")),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let unscaled: i128 = format!("{whole}{fraction}").parse().expect("digits");
    big_decimal_cell(
        fraction.len() as i32 - exponent,
        &unscaled.to_be_bytes()[..],
    )
}

fn decimals(values: &[Option<&str>]) -> ArrayRef {
    let cells: Vec<Option<Vec<u8>>> = values.iter().map(|v| v.map(decimal)).collect();
    Arc::new(cells.iter().map(|c| c.as_deref()).collect::<BinaryArray>())
}

fn integers(values: &[Option<i128>]) -> ArrayRef {
    let cells: Vec<Option<Vec<u8>>> = values.iter().map(|v| v.map(big_integer_cell)).collect();
    Arc::new(cells.iter().map(|c| c.as_deref()).collect::<BinaryArray>())
}

/// The values of column `name` as Java's `toString` writes them; `null` for a null.
fn texts(table: &dyn Table, name: &str) -> Vec<String> {
    let index = table.definition().index_of(name).expect("the column");
    let column_type = &table.definition().columns()[index].column_type;
    if !matches!(column_type, ColumnType::BigDecimal | ColumnType::BigInteger) {
        return cells(table, index);
    }
    let snapshot = TableSnapshot::current(table).unwrap();
    let values = snapshot.columns[index].as_binary::<i32>();
    (0..values.len())
        .map(|row| {
            if values.is_null(row) {
                "null".to_string()
            } else if *column_type == ColumnType::BigDecimal {
                let (scale, unscaled) = big_decimal_parts(values.value(row)).expect("a cell");
                big_decimal_text(scale, unscaled)
            } else {
                big_integer_text(values.value(row))
            }
        })
        .collect()
}

fn type_of(table: &dyn Table, name: &str) -> ColumnType {
    let index = table.definition().index_of(name).expect("the column");
    table.definition().columns()[index].column_type.clone()
}

const START: i64 = 1_700_000_000_000_000_000;
const SECOND: i64 = 1_000_000_000;

/// The text of `B` at each row of the fixture.
const TEXT: [Option<&str>; 10] = [
    Some("1.000"),
    Some("2"),
    Some("2.0"),
    None,
    Some("2.00"),
    Some("0"),
    Some("-3.5"),
    Some("7.25"),
    Some("2.0"),
    Some("0.0"),
];

/// `G`, the bucket; `B`, a decimal; `BI`, its integer part; `N` and its decimal `WB`
/// (`N.5`); `Wd`, a double weight; `T`, a timestamp. Row 4 (`Id` 4) is null but for `G`
/// and `T`. Its columns are in this order.
fn fixture(text: &[Option<&str>]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let n: [Option<i32>; 10] = [
        Some(1),
        Some(2),
        Some(3),
        None,
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
    ];
    let whole: Vec<Option<i128>> = text
        .iter()
        .map(|t| t.map(|t| t.split('.').next().unwrap().parse::<i128>().unwrap()))
        .collect();
    let halves: Vec<Option<String>> = n.iter().map(|n| n.map(|n| format!("{n}.5"))).collect();
    let halves: Vec<Option<&str>> = halves.iter().map(Option::as_deref).collect();
    let seconds = [0, 1, 1, 2, 3, 4, 5, 5, 7, 9];
    let times: TimestampNanosecondArray =
        seconds.iter().map(|s| Some(START + s * SECOND)).collect();
    vec![
        (
            "G",
            ColumnType::String,
            Arc::new(StringArray::from(vec![
                "a", "a", "b", "a", "a", "b", "a", "b", "a", "b",
            ])),
        ),
        ("B", ColumnType::BigDecimal, decimals(text)),
        ("BI", ColumnType::BigInteger, integers(&whole)),
        ("N", ColumnType::Int, Arc::new(Int32Array::from(n.to_vec()))),
        (
            "Wd",
            ColumnType::Double,
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                Some(0.5),
                None,
                Some(1.5),
                Some(1.0),
                Some(2.5),
                Some(1.0),
                Some(1.0),
                Some(3.0),
            ])),
        ),
        ("WB", ColumnType::BigDecimal, decimals(&halves)),
        (
            "T",
            ColumnType::Instant,
            Arc::new(times.with_timezone("UTC")),
        ),
    ]
}

/// The operations of the scenario's table `cumulative`, by `G`.
fn running_totals() -> Vec<UpdateByOperation> {
    vec![
        op(UpdateBySpec::CumSum, &["CS = B", "CSI = BI"]),
        op(UpdateBySpec::CumProd, &["CP = B", "CPI = BI"]),
        op(UpdateBySpec::CumMin, &["Mn = B", "MnI = BI"]),
        op(UpdateBySpec::CumMax, &["Mx = B"]),
        op(UpdateBySpec::ForwardFill, &["FF = B"]),
        op(
            UpdateBySpec::Delta(DeltaControl::NullDominates),
            &["D = B", "DI = BI"],
        ),
        op(
            UpdateBySpec::Delta(DeltaControl::ValueDominates),
            &["DV = B"],
        ),
        op(
            UpdateBySpec::Delta(DeltaControl::ZeroDominates),
            &["DZ = B"],
        ),
    ]
}

/// The operations of the scenario's table `rolling`, by `G`.
fn windows() -> Vec<UpdateByOperation> {
    let ticks = RollingWindow::ticks;
    vec![
        op(
            UpdateBySpec::RollingSum(ticks(3, 1)),
            &["RS = B", "RSI = BI"],
        ),
        op(
            UpdateBySpec::RollingAvg(ticks(3, 0)),
            &["RA = B", "RAI = BI"],
        ),
        op(
            UpdateBySpec::RollingProduct(ticks(2, 0)),
            &["RP = B", "RPI = BI"],
        ),
        op(
            UpdateBySpec::RollingStd(ticks(3, 0)),
            &["RSD = B", "RSDI = BI"],
        ),
        op(UpdateBySpec::RollingMin(ticks(3, 0)), &["RMn = B"]),
        op(
            UpdateBySpec::RollingMax(ticks(2, 1)),
            &["RMx = B", "RMxI = BI"],
        ),
        op(
            UpdateBySpec::RollingWAvg {
                window: ticks(3, 0),
                weight_column: "Wd".into(),
            },
            &["RW = B"],
        ),
        op(
            UpdateBySpec::RollingWAvg {
                window: ticks(2, 0),
                weight_column: "WB".into(),
            },
            &["RWB = N"],
        ),
        op(UpdateBySpec::RollingCount(ticks(3, 0)), &["RC = B"]),
    ]
}

fn seconds(column: &str, rev: i64, fwd: i64) -> RollingWindow {
    let span = |n: i64| WindowScale::Time {
        column: column.into(),
        nanos: n * SECOND,
    };
    RollingWindow {
        rev: span(rev),
        fwd: span(fwd),
    }
}

/// The operations of the scenario's table `timed`, over the whole table.
fn time_windows() -> Vec<UpdateByOperation> {
    vec![
        op(UpdateBySpec::RollingSum(seconds("T", 2, 0)), &["TS = B"]),
        op(UpdateBySpec::RollingAvg(seconds("T", 3, 1)), &["TA = BI"]),
        op(UpdateBySpec::RollingMin(seconds("T", 4, 0)), &["TMn = B"]),
        op(
            UpdateBySpec::RollingProduct(seconds("T", 2, 0)),
            &["TP = B"],
        ),
    ]
}

fn check(table: &dyn Table, expected: &[(&str, [&str; 10])], context: &str) {
    for (name, values) in expected {
        assert_eq!(texts(table, name), values, "{context}: column {name}");
    }
}

/// The running totals of step 0 of the scenario. A running sum or product keeps the
/// largest scale it was given; a minimum or maximum the first of equal values; a delta is
/// exact, of the previous row even when that is null.
const RUNNING_TOTALS: [(&str, [&str; 10]); 12] = [
    (
        "CS",
        [
            "1.000", "3.000", "2.0", "3.000", "5.000", "2.0", "1.500", "9.25", "3.500", "9.25",
        ],
    ),
    ("CSI", ["1", "3", "2", "3", "5", "2", "2", "9", "4", "9"]),
    (
        "CP",
        [
            "1.000",
            "2.000",
            "2.0",
            "2.000",
            "4.00000",
            "0.0",
            "-14.000000",
            "0.000",
            "-28.0000000",
            "0.0000",
        ],
    ),
    (
        "CPI",
        ["1", "2", "2", "2", "4", "0", "-12", "0", "-24", "0"],
    ),
    (
        "Mn",
        [
            "1.000", "1.000", "2.0", "1.000", "1.000", "0", "-3.5", "0", "-3.5", "0",
        ],
    ),
    (
        "Mx",
        [
            "1.000", "2", "2.0", "2", "2", "2.0", "2", "7.25", "2", "7.25",
        ],
    ),
    ("MnI", ["1", "1", "2", "1", "1", "0", "-3", "0", "-3", "0"]),
    (
        "FF",
        [
            "1.000", "2", "2.0", "2", "2.00", "0", "-3.5", "7.25", "2.0", "0.0",
        ],
    ),
    (
        "D",
        [
            "null", "1.000", "null", "null", "null", "-2.0", "-5.50", "7.25", "5.5", "-7.25",
        ],
    ),
    (
        "DI",
        [
            "null", "1", "null", "null", "null", "-2", "-5", "7", "5", "-7",
        ],
    ),
    (
        "DV",
        [
            "1.000", "1.000", "2.0", "null", "2.00", "-2.0", "-5.50", "7.25", "5.5", "-7.25",
        ],
    ),
    (
        "DZ",
        [
            "0", "1.000", "0", "null", "0", "-2.0", "-5.50", "7.25", "5.5", "-7.25",
        ],
    ),
];

/// The windows of step 0 of the scenario.
const WINDOWS: [(&str, [&str; 10]); 14] = [
    (
        "RS",
        [
            "3.000", "3.000", "2.0", "5.000", "0.500", "9.25", "0.500", "9.25", "0.500", "7.25",
        ],
    ),
    ("RSI", ["3", "3", "2", "5", "1", "9", "1", "9", "1", "7"]),
    (
        "RA",
        [
            "1.000",
            "1.500",
            "2.0",
            "1.500",
            "2.000",
            "1.0",
            "-0.750",
            "3.083333333333333",
            "0.1666666666666667",
            "2.416666666666667",
        ],
    ),
    (
        "RAI",
        [
            "1",
            "1.5",
            "2",
            "1.5",
            "2",
            "1",
            "-0.5",
            "3",
            "0.3333333333333333",
            "2.333333333333333",
        ],
    ),
    (
        "RP",
        [
            "1.000", "2.000", "2.0", "2", "2.00", "0", "-7.000", "0", "-7.00", "0.000",
        ],
    ),
    ("RPI", ["1", "2", "2", "2", "2", "0", "-6", "0", "-6", "0"]),
    (
        "RSD",
        [
            "null",
            "0.7071067811865475",
            "null",
            "0.7071067811865475",
            "0.00",
            "1.414213562373095",
            "3.889087296526011",
            "3.744440323110163",
            "3.175426480542942",
            "4.185789451624787",
        ],
    ),
    (
        "RSDI",
        [
            "null",
            "0.7071067811865475",
            "null",
            "0.7071067811865475",
            "0",
            "1.414213562373095",
            "3.535533905932738",
            "3.605551275463989",
            "2.886751345948129",
            "4.041451884327381",
        ],
    ),
    (
        "RMn",
        [
            "1.000", "1.000", "2.0", "1.000", "2.00", "0", "-3.5", "0", "-3.5", "0",
        ],
    ),
    (
        "RMx",
        [
            "2", "2", "2.0", "2", "2.00", "7.25", "2.00", "7.25", "2.0", "7.25",
        ],
    ),
    ("RMxI", ["2", "2", "2", "2", "2", "7", "2", "7", "2", "7"]),
    (
        "RW",
        [
            "1.000",
            "1.666666666666667",
            "2.0",
            "1.666666666666667",
            "2.00",
            "0.6666666666666667",
            "-1.4375",
            "3.30",
            "-0.75",
            "1.45",
        ],
    ),
    (
        "RWB",
        [
            "1",
            "1.625",
            "3",
            "2",
            "4",
            "4.222222222222222",
            "5.181818181818182",
            "6.153846153846154",
            "7.133333333333333",
            "8.117647058823529",
        ],
    ),
    ("RC", ["1", "2", "1", "2", "2", "2", "2", "3", "3", "3"]),
];

/// The windows of time of step 0 of the scenario.
const TIME_WINDOWS: [(&str, [&str; 10]); 4] = [
    (
        "TS",
        [
            "1.000", "5.000", "5.000", "5.000", "6.000", "2.000", "5.750", "5.750", "5.750",
            "2.000",
        ],
    ),
    (
        "TA",
        [
            "1.666666666666667",
            "1.666666666666667",
            "1.666666666666667",
            "1.75",
            "1.4",
            "1.666666666666667",
            "1.5",
            "1.5",
            "1.5",
            "1",
        ],
    ),
    (
        "TMn",
        [
            "1.000", "1.000", "1.000", "1.000", "1.000", "0", "-3.5", "-3.5", "-3.5", "-3.5",
        ],
    ),
    (
        "TP",
        [
            "1.000", "4.0000", "4.0000", "4.0000", "8.000", "0", "0", "0", "-50.7500", "0.00",
        ],
    ),
];

#[test]
fn running_totals_of_big_numbers_match_java() {
    let source = static_table(&fixture(&TEXT));
    let result = update_by(&source, &running_totals(), &["G"]).unwrap();
    check(&*result, &RUNNING_TOTALS, "static");
    // An integer's totals are integers.
    for name in ["CS", "CP", "Mn", "Mx", "FF", "D", "DV", "DZ"] {
        assert_eq!(type_of(&*result, name), ColumnType::BigDecimal, "{name}");
    }
    for name in ["CSI", "CPI", "MnI", "DI"] {
        assert_eq!(type_of(&*result, name), ColumnType::BigInteger, "{name}");
    }
}

#[test]
fn windows_of_big_numbers_match_java() {
    let source = static_table(&fixture(&TEXT));
    let result = update_by(&source, &windows(), &["G"]).unwrap();
    check(&*result, &WINDOWS, "static");
    // The sum, product, minimum and maximum of integers are integers; their average,
    // deviation and weighted average decimals.
    for name in ["RSI", "RPI", "RMxI"] {
        assert_eq!(type_of(&*result, name), ColumnType::BigInteger, "{name}");
    }
    for name in ["RAI", "RSDI", "RW", "RWB"] {
        assert_eq!(type_of(&*result, name), ColumnType::BigDecimal, "{name}");
    }
    assert_eq!(type_of(&*result, "RC"), ColumnType::Long);
}

#[test]
fn windows_of_time_of_big_numbers_match_java() {
    let source = static_table(&fixture(&TEXT));
    let result = update_by(&source, &time_windows(), &[] as &[&str]).unwrap();
    check(&*result, &TIME_WINDOWS, "static");
    assert_eq!(type_of(&*result, "TA"), ColumnType::BigDecimal);
}

/// Step 1 of the scenario: row 5 (`Id` 5, row key 4) becomes `2.5000`, and Java recomputes
/// the rows of its bucket from the first one the change reaches, in a new pass whose
/// running sums and products take the scales of the values that pass sees.
#[test]
fn a_modified_value_is_recomputed_in_a_new_pass() {
    let graph = graph();
    let source = scripted(&graph, 0..10, &fixture(&TEXT));
    let table: Arc<dyn Table> = source.table();
    let results = [
        update_by(&table, &running_totals(), &["G"]).unwrap(),
        update_by(&table, &windows(), &["G"]).unwrap(),
        update_by(&table, &time_windows(), &[] as &[&str]).unwrap(),
    ];
    let checkers: Vec<_> = results
        .iter()
        .map(|result| ConsistencyChecker::attach(result.clone()))
        .collect();
    // As the input table of the scenario does, the change replaces the whole row.
    let mut text = TEXT;
    text[4] = Some("2.5000");
    let columns = fixture(&text);
    let row = |values: &ArrayRef| values.slice(4, 1);
    let change = TableChange::new().modify(
        [4],
        columns
            .iter()
            .enumerate()
            .map(|(index, (_, _, values))| (index, row(values))),
    );
    source.push(change);
    graph.run_cycle();
    for checker in &checkers {
        checker.assert_consistent();
        assert!(checker.updates_seen() > 0);
    }
    check(
        &*results[0],
        &[
            (
                "CS",
                [
                    "1.000", "3.000", "2.0", "3.000", "5.5000", "2.0", "2.0000", "9.25", "4.0000",
                    "9.25",
                ],
            ),
            (
                "CP",
                [
                    "1.000",
                    "2.000",
                    "2.0",
                    "2.000",
                    "5.0000000",
                    "0.0",
                    "-17.50000000",
                    "0.000",
                    "-35.000000000",
                    "0.0000",
                ],
            ),
            (
                "Mx",
                [
                    "1.000", "2", "2.0", "2", "2.5000", "2.0", "2.5000", "7.25", "2.5000", "7.25",
                ],
            ),
            (
                "D",
                [
                    "null", "1.000", "null", "null", "null", "-2.0", "-6.0000", "7.25", "5.5",
                    "-7.25",
                ],
            ),
        ],
        "step 1",
    );
    check(
        &*results[1],
        &[
            (
                "RS",
                [
                    "3.000", "3.000", "2.0", "5.5000", "1.0000", "9.25", "1.0000", "9.25",
                    "1.0000", "7.25",
                ],
            ),
            (
                "RA",
                [
                    "1.000",
                    "1.500",
                    "2.0",
                    "1.500",
                    "2.2500",
                    "1.0",
                    "-0.5000",
                    "3.083333333333333",
                    "0.3333333333333333",
                    "2.416666666666667",
                ],
            ),
            (
                "RP",
                [
                    "1.000", "2.000", "2.0", "2", "2.5000", "0", "-8.75000", "0", "-7.00", "0.000",
                ],
            ),
            (
                "RSD",
                [
                    "null",
                    "0.7071067811865475",
                    "null",
                    "0.7071067811865475",
                    "0.3535533905932738",
                    "1.414213562373095",
                    "4.242640687119285",
                    "3.744440323110163",
                    "3.329164059239696",
                    "4.185789451624787",
                ],
            ),
            (
                "RMn",
                [
                    "1.000", "1.000", "2.0", "1.000", "2", "0", "-3.5", "0", "-3.5", "0",
                ],
            ),
            (
                "RMx",
                [
                    "2", "2", "2.0", "2.5000", "2.5000", "7.25", "2.5000", "7.25", "2.0", "7.25",
                ],
            ),
            (
                "RW",
                [
                    "1.000",
                    "1.666666666666667",
                    "2.0",
                    "1.666666666666667",
                    "2.214285714285714",
                    "0.6666666666666667",
                    "-1.2500",
                    "3.30",
                    "-0.6000",
                    "1.45",
                ],
            ),
        ],
        "step 1",
    );
    check(
        &*results[2],
        &[
            (
                "TS",
                [
                    "1.000", "5.000", "5.000", "5.000", "6.5000", "2.5000", "6.2500", "6.2500",
                    "5.750", "2.000",
                ],
            ),
            (
                "TP",
                [
                    "1.000", "4.0000", "4.0000", "4.0000", "10.00000", "0", "0", "0", "-50.7500",
                    "0.00",
                ],
            ),
        ],
        "step 1",
    );
}

/// The result columns of `operations` over `B`, decimals of `text`, `BI`, ones, and `W`,
/// as text, each row's values joined with `|`; or the failure.
fn outcome(
    operations: &[UpdateByOperation],
    text: &[&str],
    weights: (ColumnType, ArrayRef),
    control: &UpdateByControl,
) -> Result<String, String> {
    let values: Vec<Option<&str>> = text.iter().copied().map(Some).collect();
    let source = static_table(&[
        ("B", ColumnType::BigDecimal, decimals(&values)),
        (
            "BI",
            ColumnType::BigInteger,
            integers(&vec![Some(1); text.len()]),
        ),
        ("W", weights.0, weights.1),
    ]);
    match update_by_controlled(&source, operations, &[] as &[&str], None, control) {
        Ok(result) => {
            let names: Vec<String> = result
                .definition()
                .columns()
                .iter()
                .skip(3)
                .map(|column| column.name.clone())
                .collect();
            let columns: Vec<Vec<String>> =
                names.iter().map(|name| texts(&*result, name)).collect();
            Ok((0..text.len())
                .map(|row| {
                    let row: Vec<&str> = columns.iter().map(|c| c[row].as_str()).collect();
                    row.join("|")
                })
                .collect::<Vec<_>>()
                .join(" "))
        }
        Err(UpdateByError::Failed(failure)) => Err(failure.message),
        Err(other) => panic!("{other:?}"),
    }
}

fn doubles(values: &[f64]) -> (ColumnType, ArrayRef) {
    (
        ColumnType::Double,
        Arc::new(Float64Array::from(values.to_vec())),
    )
}

fn ints(values: &[i32]) -> (ColumnType, ArrayRef) {
    (ColumnType::Int, Arc::new(Int32Array::from(values.to_vec())))
}

fn wavg(window: RollingWindow, weight: &str, columns: &[&str]) -> UpdateByOperation {
    op(
        UpdateBySpec::RollingWAvg {
            window,
            weight_column: weight.into(),
        },
        columns,
    )
}

/// Seen on Deephaven 42.9 over `1.5 2 3`, with the weights `1.0 2.0 NaN`: a window of no
/// row fails each operator's assertion or buffer, and a weighted average fails to cast the
/// NaN to a decimal first, whichever of its columns holds it.
#[test]
fn windows_of_no_row_fail_as_java_does() {
    let empty = || RollingWindow::ticks(0, 0);
    let control = UpdateByControl::default();
    let not_a_number = "java.lang.NumberFormatException: Character N is neither a decimal \
                        digit number, decimal point, nor \"e\" notation exponential mark.";
    for (operation, message) in [
        (
            op(UpdateBySpec::RollingSum(empty()), &["R = B"]),
            "java.util.NoSuchElementException",
        ),
        (
            op(UpdateBySpec::RollingAvg(empty()), &["R = B"]),
            "java.util.NoSuchElementException",
        ),
        (
            op(UpdateBySpec::RollingProduct(empty()), &["R = B"]),
            "Assertion failed: asserted buffer.size() >= 1, instead buffer.size() == 0.",
        ),
        (
            op(UpdateBySpec::RollingStd(empty()), &["R = B"]),
            "Assertion failed: asserted valueBuffer.size() >= 1, instead valueBuffer.size() == 0.",
        ),
        (
            op(UpdateBySpec::RollingMin(empty()), &["R = B"]),
            "Assertion failed: asserted aggMinMax.size() >= 1, instead aggMinMax.size() == 0.",
        ),
        (wavg(empty(), "W", &["R = B"]), not_a_number),
        (
            wavg(RollingWindow::ticks(2, 0), "B", &["R = W"]),
            not_a_number,
        ),
    ] {
        let result = outcome(
            std::slice::from_ref(&operation),
            &["1.5", "2", "3"],
            doubles(&[1.0, 2.0, f64::NAN]),
            &control,
        );
        assert_eq!(result, Err(message.to_string()), "{operation:?}");
    }
}

/// Seen on Deephaven 42.9 over `1.5 2 3` in windows of two rows: weights that sum to an
/// integer zero give null; weights of `0.0` give a zero by zero, and `1.0 -1.0` a value by
/// zero.
#[test]
fn a_weighted_average_of_no_weight_is_null_or_fails() {
    let control = UpdateByControl::default();
    let run = |weights| {
        outcome(
            &[wavg(RollingWindow::ticks(2, 0), "W", &["R = B"])],
            &["1.5", "2", "3"],
            weights,
            &control,
        )
    };
    assert_eq!(run(ints(&[0, 0, 1])), Ok("null null 3".to_string()));
    assert_eq!(
        run(doubles(&[0.0, 0.0, 1.0])),
        Err("java.lang.ArithmeticException: Division undefined".to_string())
    );
    assert_eq!(
        run(doubles(&[1.0, -1.0, 1.0])),
        Err("java.lang.ArithmeticException: Division by zero".to_string())
    );
}

/// Seen on Deephaven 42.9, windows of two rows: a product is `0` while its window holds a
/// zero of scale 0, and otherwise multiplies, so `0.00` gives `0.00`. A `BigInteger` zero
/// is always `0`.
#[test]
fn a_window_product_of_a_zero_of_scale_zero_is_zero() {
    let text = ["0", "2", "3", "0.00", "4", "5", "-0"];
    let values: Vec<Option<&str>> = text.iter().copied().map(Some).collect();
    let whole: Vec<Option<i128>> = [0, 2, 3, 0, 4, 5, 0].into_iter().map(Some).collect();
    let source = static_table(&[
        ("B", ColumnType::BigDecimal, decimals(&values)),
        ("BI", ColumnType::BigInteger, integers(&whole)),
    ]);
    let result = update_by(
        &source,
        &[op(
            UpdateBySpec::RollingProduct(RollingWindow::ticks(2, 0)),
            &["R = B", "RI = BI"],
        )],
        &[] as &[&str],
    )
    .unwrap();
    assert_eq!(
        texts(&*result, "R"),
        ["0", "0", "6", "0.00", "0.00", "20", "0"]
    );
    assert_eq!(texts(&*result, "RI"), ["0", "0", "6", "0", "0", "20", "0"]);
}

/// Seen on Deephaven 42.9 over `1.00 null 2 3.0 null null 4`: a window sum forgets its
/// scale when its window holds no value, and starts again from the next value as it is.
#[test]
fn a_window_sum_starts_again_after_a_window_of_nulls() {
    let values = [
        Some("1.00"),
        None,
        Some("2"),
        Some("3.0"),
        None,
        None,
        Some("4"),
    ];
    let source = static_table(&[("B", ColumnType::BigDecimal, decimals(&values))]);
    let result = update_by(
        &source,
        &[
            op(
                UpdateBySpec::RollingSum(RollingWindow::ticks(1, 0)),
                &["S = B"],
            ),
            op(
                UpdateBySpec::RollingAvg(RollingWindow::ticks(2, 0)),
                &["A = B"],
            ),
            op(
                UpdateBySpec::RollingSum(RollingWindow::ticks(2, 0)),
                &["S2 = B"],
            ),
        ],
        &[] as &[&str],
    )
    .unwrap();
    assert_eq!(
        texts(&*result, "S"),
        ["1.00", "null", "2", "3.0", "null", "null", "4"]
    );
    assert_eq!(
        texts(&*result, "A"),
        ["1.00", "1.00", "2.00", "2.50", "3.00", "null", "4"]
    );
    assert_eq!(
        texts(&*result, "S2"),
        ["1.00", "1.00", "2.00", "5.00", "3.00", "null", "4"]
    );
}

/// Seen on Deephaven 42.9 over `2.0 2 2.00 3 2.000`: a rolling minimum of two rows and a
/// rolling maximum of three show which of equal values of other scales each keeps.
#[test]
fn a_rolling_extreme_of_equal_values_keeps_the_one_java_keeps() {
    let text = ["2.0", "2", "2.00", "3", "2.000"];
    let values: Vec<Option<&str>> = text.iter().copied().map(Some).collect();
    let source = static_table(&[("B", ColumnType::BigDecimal, decimals(&values))]);
    let result = update_by(
        &source,
        &[
            op(
                UpdateBySpec::RollingMin(RollingWindow::ticks(2, 0)),
                &["R = B"],
            ),
            op(
                UpdateBySpec::RollingMax(RollingWindow::ticks(3, 0)),
                &["X = B"],
            ),
        ],
        &[] as &[&str],
    )
    .unwrap();
    assert_eq!(
        texts(&*result, "R"),
        ["2.0", "2.0", "2.00", "2.00", "2.000"]
    );
    assert_eq!(texts(&*result, "X"), ["2.0", "2.0", "2.0", "3", "3"]);
}

/// Seen on Deephaven 42.9 with `updateBy(UpdateByControl.builder().mathContext(..))` over
/// `1 0 0 2.5 1`: a rolling average and deviation of three rows and a running product, at
/// `DECIMAL32`, at three digits rounded down, and unlimited, where the average of `1 0 0`
/// does not end.
#[test]
fn the_math_context_rounds_as_java_does() {
    let operations = [
        op(
            UpdateBySpec::RollingAvg(RollingWindow::ticks(3, 0)),
            &["R = B"],
        ),
        op(UpdateBySpec::CumProd, &["P = B"]),
        op(
            UpdateBySpec::RollingStd(RollingWindow::ticks(3, 0)),
            &["D = B"],
        ),
    ];
    let run = |math_context| {
        outcome(
            &operations,
            &["1", "0", "0", "2.5", "1"],
            ints(&[0; 5]),
            &UpdateByControl { math_context },
        )
    };
    assert_eq!(
        run(MathContext::DECIMAL32),
        Ok(
            "1|1|null 0.5|0|0.7071068 0.3333333|0|0.5773503 0.8333333|0.0|1.443376 \
            1.166667|0.0|1.258306"
                .to_string()
        )
    );
    assert_eq!(
        run(MathContext {
            precision: 3,
            rounding: Rounding::Down
        }),
        Ok("1|1|null 0.5|0|0.707 0.333|0|0.577 0.833|0.0|1.44 1.16|0.0|1.26".to_string())
    );
    assert_eq!(
        run(MathContext::UNLIMITED),
        Err(
            "java.lang.ArithmeticException: Non-terminating decimal expansion; no exact \
             representable decimal result."
                .to_string()
        )
    );
}

/// The decay of an exponential operator over big numbers is the decimal of HotSpot's
/// `Math.exp`, which is not ported (owner item 8.3).
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.em.BaseBigNumberEMOperator#computeAlpha"
)]
fn an_exponential_operator_of_big_numbers_is_unported() {
    let source = static_table(&fixture(&TEXT));
    let spec = EmSpec {
        window: WindowScale::Ticks(2.0),
        on_null: None,
        on_nan: None,
    };
    let _ = update_by(&source, &[op(UpdateBySpec::Ema(spec), &["E = BI"])], &["G"]);
}

/// Java casts the values of a weighted average with big number weights to `BigDecimal`,
/// which for a `String` column is not ported.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.util.cast.ToBigDecimalCast#makeToBigDecimalCast"
)]
fn a_weighted_average_of_text_with_big_weights_is_unported() {
    let source = static_table(&fixture(&TEXT));
    let _ = update_by(
        &source,
        &[wavg(RollingWindow::ticks(2, 0), "B", &["R = G"])],
        &[] as &[&str],
    );
}

/// Random changes to a ticking table of big numbers whose decimals have one scale and few
/// digits, so that no result depends on the pass that computed it, checked after every
/// cycle against the same operations on a static copy.
#[test]
fn random_updates_of_big_numbers_of_one_scale_match_a_recompute() {
    let types = [
        ("K", ColumnType::String),
        ("L", ColumnType::Long),
        ("I", ColumnType::Int),
    ];
    // Decimals of scale 2, integers beyond a long, and positive decimals of scale -3.
    let formulas = [
        "B = isNull(L) ? null : java.math.BigDecimal.valueOf(L, 2)",
        "G = isNull(L) ? null : java.math.BigInteger.valueOf(L).multiply(java.math.BigInteger.TEN.pow(20))",
        "W = isNull(I) ? null : java.math.BigDecimal.valueOf(abs(I) + 1, -3)",
    ];
    let ticks = RollingWindow::ticks;
    let mut operations = Vec::new();
    for input in ["B", "G", "W"] {
        let of = |spec: UpdateBySpec, name: &str| op(spec, &[&format!("{name}{input} = {input}")]);
        operations.extend([
            of(UpdateBySpec::CumSum, "CS"),
            of(UpdateBySpec::CumProd, "CP"),
            of(UpdateBySpec::CumMin, "CN"),
            of(UpdateBySpec::CumMax, "CX"),
            of(UpdateBySpec::ForwardFill, "FF"),
            of(UpdateBySpec::Delta(DeltaControl::NullDominates), "D"),
            of(UpdateBySpec::Delta(DeltaControl::ZeroDominates), "DZ"),
            of(UpdateBySpec::RollingSum(ticks(3, 1)), "RS"),
            of(UpdateBySpec::RollingAvg(ticks(2, 2)), "RA"),
            of(UpdateBySpec::RollingProduct(ticks(2, 1)), "RP"),
            of(UpdateBySpec::RollingStd(ticks(3, 0)), "RD"),
            of(UpdateBySpec::RollingMin(ticks(3, 1)), "RN"),
            of(UpdateBySpec::RollingMax(ticks(2, 0)), "RX"),
            of(UpdateBySpec::RollingCount(ticks(2, 0)), "RC"),
        ]);
        operations.push(wavg(ticks(3, 0), "I", &[&format!("WI{input} = {input}")]));
    }
    operations.push(wavg(ticks(2, 1), "W", &["WWB = B", "WWG = G", "WWI = I"]));
    for by in [&[][..], &["K"]] {
        for seed in test_seeds(0..12) {
            let mut random = RandomUpdates::new(seed);
            let columns: Vec<(&str, ColumnType, ArrayRef)> = types
                .iter()
                .map(|(name, column_type)| {
                    (*name, column_type.clone(), random.values(column_type, 8))
                })
                .collect();
            let operations = operations.clone();
            let by: Vec<&'static str> = by.to_vec();
            let t = Ticking::new(0..8, &columns, move |source| {
                let big = update_view(source, &formulas, &oxhaven_expr::Compiler).unwrap();
                update_by_with(&big, &operations, &by, &oxhaven_expr::Compiler).unwrap()
            });
            for _ in 0..30 {
                let change = random.next_change(&*t.script.table(), 8);
                t.step(change);
                assert!(!t.result.is_failed(), "seed {seed}");
            }
        }
    }
}
