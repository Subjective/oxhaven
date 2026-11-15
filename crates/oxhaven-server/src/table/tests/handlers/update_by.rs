//! The `UpdateBy` handler, called as a client calls it.

use super::*;
use oxhaven_engine::ops::update_by::{
    update_by, BadDataBehavior, EmSpec, RollingWindow, UpdateByOperation, UpdateBySpec, WindowScale,
};
use proto::update_by_request::update_by_operation::update_by_column::update_by_spec as spec;
use proto::update_by_request::update_by_operation::update_by_column::UpdateBySpec as ProtoSpec;
use proto::update_by_request::update_by_operation::{Type as OperationType, UpdateByColumn};
use proto::update_by_request::{UpdateByOperation as ProtoOperation, UpdateByOptions};
use proto::update_by_window_scale::update_by_window_time::Window;
use proto::update_by_window_scale::{Type as ScaleType, UpdateByWindowTicks, UpdateByWindowTime};
use proto::{UpdateByEmOptions, UpdateByRequest, UpdateByWindowScale};
use spec::Type as Spec;

type Scale = Option<UpdateByWindowScale>;

fn ticks(ticks: f64) -> Scale {
    Some(UpdateByWindowScale {
        r#type: Some(ScaleType::Ticks(UpdateByWindowTicks { ticks })),
    })
}

fn time(column: &str, window: Option<Window>) -> Scale {
    Some(UpdateByWindowScale {
        r#type: Some(ScaleType::Time(UpdateByWindowTime {
            column: column.to_string(),
            window,
        })),
    })
}

fn nanos(column: &str, nanos: i64) -> Scale {
    time(column, Some(Window::Nanos(nanos)))
}

fn duration(column: &str, text: &str) -> Scale {
    time(column, Some(Window::DurationString(text.to_string())))
}

fn rolling_sum(reverse: Scale, forward: Scale) -> Spec {
    Spec::RollingSum(spec::UpdateByRollingSum {
        reverse_window_scale: reverse,
        forward_window_scale: forward,
    })
}

fn sum() -> Spec {
    Spec::Sum(spec::UpdateByCumulativeSum {})
}

fn delta(null_behavior: Option<i32>) -> Spec {
    Spec::Delta(spec::UpdateByDelta {
        options: null_behavior.map(|null_behavior| proto::UpdateByDeltaOptions { null_behavior }),
    })
}

fn em_options(on_null_value: i32, on_nan_value: i32) -> Option<UpdateByEmOptions> {
    Some(UpdateByEmOptions {
        on_null_value,
        on_nan_value,
        ..Default::default()
    })
}

fn operation(spec: Spec, pairs: &[&str]) -> ProtoOperation {
    ProtoOperation {
        r#type: Some(OperationType::Column(UpdateByColumn {
            spec: Some(ProtoSpec { r#type: Some(spec) }),
            match_pairs: texts(pairs),
        })),
    }
}

fn request(result: i32, source: i32, operations: Vec<ProtoOperation>, by: &[&str]) -> Op {
    Op::UpdateBy(UpdateByRequest {
        result_id: ticket(result),
        source_id: by_ticket(source),
        options: None,
        operations,
        group_by_columns: texts(by),
    })
}

fn with_options(op: Op, options: UpdateByOptions) -> Op {
    match op {
        Op::UpdateBy(request) => Op::UpdateBy(UpdateByRequest {
            options: Some(options),
            ..request
        }),
        other => other,
    }
}

#[tokio::test]
async fn update_by_adds_a_column_per_operation_and_pair_computed_within_each_bucket() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    // N is 3, 1, -1 in the rows of `a` and 2, 0, -2 in the rows of `b`
    let operations = vec![
        operation(sum(), &["CumN = N"]),
        operation(rolling_sum(ticks(2.0), ticks(0.0)), &["RollN=N"]),
        operation(delta(None), &["DeltaN = N"]),
        operation(delta(Some(1)), &["DeltaNull = N"]),
        operation(delta(Some(2)), &["DeltaValue = N"]),
        operation(delta(Some(3)), &["DeltaZero = N"]),
        // a behavior the protocol does not define is the default
        operation(delta(Some(77)), &["DeltaOther = N"]),
        operation(Spec::Min(spec::UpdateByCumulativeMin {}), &["MinN = N"]),
        operation(Spec::Max(spec::UpdateByCumulativeMax {}), &["MaxN = N"]),
        operation(
            Spec::Product(spec::UpdateByCumulativeProduct {}),
            &["ProdN = N", "ProdI = I"],
        ),
        operation(Spec::Fill(spec::UpdateByFill {}), &["FillN = N"]),
    ];
    let expected: [(&str, Vec<String>); 12] = [
        ("CumN", ints(&[3, 2, 4, 2, 3, 0])),
        ("RollN", ints(&[3, 2, 4, 2, 0, -2])),
        ("DeltaN", texts(&["", "", "-2", "-2", "-2", "-2"])),
        ("DeltaNull", texts(&["", "", "-2", "-2", "-2", "-2"])),
        ("DeltaValue", ints(&[3, 2, -2, -2, -2, -2])),
        ("DeltaZero", ints(&[0, 0, -2, -2, -2, -2])),
        ("DeltaOther", texts(&["", "", "-2", "-2", "-2", "-2"])),
        ("MinN", ints(&[3, 2, 1, 0, -1, -2])),
        ("MaxN", ints(&[3, 2, 3, 2, 3, 2])),
        ("ProdN", ints(&[3, 2, 3, 0, -3, 0])),
        ("ProdI", ints(&[0, 1, 0, 3, 0, 15])),
        ("FillN", ints(&[3, 2, 1, 0, -1, -2])),
    ];
    let mut names = texts(&["I", "N", "S"]);
    names.extend(expected.iter().map(|(name, _)| (*name).to_string()));

    let response = call(&mut c, request(2, 1, operations.clone(), &["S"]))
        .await
        .unwrap();
    assert!(response.success);
    assert!(response.is_static);
    assert_eq!(response.size, 6);
    // and in a batch, from a source named by its offset
    let in_batch = Op::UpdateBy(UpdateByRequest {
        result_id: ticket(3),
        source_id: by_offset(0),
        options: None,
        operations,
        group_by_columns: texts(&["S"]),
    });
    let (responses, status) = c
        .batch(vec![fetch(by_ticket(1), None), in_batch])
        .await
        .unwrap();
    assert!(status.is_none(), "{status:?}");
    assert!(responses.iter().all(|response| response.success));
    for id in [2, 3] {
        let table = c.table(id);
        assert_eq!(column_names(table.as_ref()), names);
        assert_eq!(values(table.as_ref(), "N"), ints(&[3, 2, 1, 0, -1, -2]));
        for (name, column) in &expected {
            assert_eq!(&values(table.as_ref(), name), column, "{name}");
        }
    }

    // without group-by columns the table is one bucket, and a result named like its
    // input takes its place
    call(&mut c, request(4, 1, vec![operation(sum(), &["N"])], &[]))
        .await
        .unwrap();
    let table = c.table(4);
    assert_eq!(column_names(table.as_ref()), texts(&["I", "N", "S"]));
    assert_eq!(values(table.as_ref(), "N"), ints(&[3, 5, 6, 6, 5, 3]));

    // without pairs the operation takes every column it applies to but the group-by
    // columns
    call(&mut c, request(5, 1, vec![operation(sum(), &[])], &["S"]))
        .await
        .unwrap();
    let table = c.table(5);
    assert_eq!(column_names(table.as_ref()), texts(&["I", "N", "S"]));
    assert_eq!(values(table.as_ref(), "I"), ints(&[0, 1, 2, 4, 6, 9]));
    assert_eq!(values(table.as_ref(), "N"), ints(&[3, 2, 4, 2, 3, 0]));
    assert_eq!(
        values(table.as_ref(), "S"),
        texts(&["a", "b", "a", "b", "a", "b"])
    );
}

const SECOND: i64 = 1_000_000_000;

/// Exports, as `id`, `numbers` with `L`, a second per row with two rows in the third
/// second, and `D`, halves of `N` with a null in the third row and a NaN in the fifth.
async fn timed(c: &mut Connected, id: i32) {
    numbers(c, id + 1000).await;
    let columns = [
        "L = (I < 3 ? I : I - 1) * 1000000000L",
        "D = I == 2 ? NULL_DOUBLE : (I == 4 ? NAN_DOUBLE : N * 0.5)",
    ];
    call(c, Op::Update(formulas(id + 1000, id, &columns)))
        .await
        .unwrap();
    let table = c.table(id);
    assert_eq!(
        values(table.as_ref(), "L"),
        ints(&[0, SECOND, 2 * SECOND, 2 * SECOND, 3 * SECOND, 4 * SECOND])
    );
    assert_eq!(
        values(table.as_ref(), "D"),
        texts(&["1.5", "1.0", "", "0.0", "NaN", "-1.0"])
    );
}

fn in_time(nanos: i64) -> WindowScale {
    WindowScale::Time {
        column: "L".to_string(),
        nanos,
    }
}

fn rolling(
    make: fn(RollingWindow) -> UpdateBySpec,
    reverse: WindowScale,
    forward: WindowScale,
) -> UpdateBySpec {
    make(RollingWindow {
        rev: reverse,
        fwd: forward,
    })
}

fn em(
    window: WindowScale,
    on_null: Option<BadDataBehavior>,
    on_nan: Option<BadDataBehavior>,
) -> EmSpec {
    EmSpec {
        window,
        on_null,
        on_nan,
    }
}

macro_rules! proto_rolling {
    ($variant:ident, $message:ident, $reverse:expr, $forward:expr) => {
        Spec::$variant(spec::$message {
            reverse_window_scale: $reverse,
            forward_window_scale: $forward,
        })
    };
}

macro_rules! proto_em {
    ($variant:ident, $message:ident, $options:expr, $scale:expr) => {
        Spec::$variant(spec::$message {
            options: $options,
            window_scale: $scale,
        })
    };
}

/// Every window and every option of a request reaches the engine as the operation Java
/// would build from it: the result of each request is the result of the engine called
/// with that operation.
#[tokio::test]
async fn windows_decays_and_options_reach_the_engine_as_java_adapts_them() {
    use BadDataBehavior::{Poison, Reset, Skip};
    use WindowScale::Ticks;
    let mut c = Connected::start().await;
    timed(&mut c, 1).await;
    let source = c.table(1);
    let cases: Vec<(&str, Spec, UpdateBySpec)> = vec![
        (
            // more nanoseconds than a window may have rows
            "rolling sum over a duration string",
            rolling_sum(duration("L", "PT3S"), duration("L", "PT0S")),
            rolling(UpdateBySpec::RollingSum, in_time(3 * SECOND), in_time(0)),
        ),
        (
            "rolling sum over a clock duration string",
            rolling_sum(duration("L", "PT0:00:01"), duration("L", "PT0:00:01")),
            rolling(UpdateBySpec::RollingSum, in_time(SECOND), in_time(SECOND)),
        ),
        (
            "rolling sum over nanoseconds",
            rolling_sum(nanos("L", SECOND), nanos("L", 0)),
            rolling(UpdateBySpec::RollingSum, in_time(SECOND), in_time(0)),
        ),
        (
            "rolling sum over a time without a length",
            rolling_sum(time("L", None), time("L", None)),
            rolling(UpdateBySpec::RollingSum, in_time(0), in_time(0)),
        ),
        (
            "rolling sum over rows",
            rolling_sum(ticks(3.0), ticks(0.0)),
            rolling(UpdateBySpec::RollingSum, Ticks(3.0), Ticks(0.0)),
        ),
        (
            "rolling sum over rows ahead",
            rolling_sum(ticks(0.0), ticks(2.0)),
            rolling(UpdateBySpec::RollingSum, Ticks(0.0), Ticks(2.0)),
        ),
        (
            "rolling sum over fractions of rows",
            rolling_sum(ticks(2.9), ticks(1.9)),
            rolling(UpdateBySpec::RollingSum, Ticks(2.9), Ticks(1.9)),
        ),
        (
            "rolling avg",
            proto_rolling!(RollingAvg, UpdateByRollingAvg, ticks(2.0), ticks(1.0)),
            rolling(UpdateBySpec::RollingAvg, Ticks(2.0), Ticks(1.0)),
        ),
        (
            "rolling min",
            proto_rolling!(RollingMin, UpdateByRollingMin, ticks(2.0), ticks(1.0)),
            rolling(UpdateBySpec::RollingMin, Ticks(2.0), Ticks(1.0)),
        ),
        (
            "rolling max",
            proto_rolling!(
                RollingMax,
                UpdateByRollingMax,
                nanos("L", 1),
                nanos("L", SECOND)
            ),
            rolling(UpdateBySpec::RollingMax, in_time(1), in_time(SECOND)),
        ),
        (
            "rolling product",
            proto_rolling!(
                RollingProduct,
                UpdateByRollingProduct,
                ticks(2.0),
                ticks(0.0)
            ),
            rolling(UpdateBySpec::RollingProduct, Ticks(2.0), Ticks(0.0)),
        ),
        (
            "rolling count",
            proto_rolling!(RollingCount, UpdateByRollingCount, ticks(4.0), ticks(0.0)),
            rolling(UpdateBySpec::RollingCount, Ticks(4.0), Ticks(0.0)),
        ),
        (
            "ema by rows",
            proto_em!(Ema, UpdateByEma, None, ticks(2.0)),
            UpdateBySpec::Ema(em(Ticks(2.0), None, None)),
        ),
        (
            "ema by half rows",
            proto_em!(Ema, UpdateByEma, None, ticks(0.5)),
            UpdateBySpec::Ema(em(Ticks(0.5), None, None)),
        ),
        (
            "ema by time",
            proto_em!(Ema, UpdateByEma, None, duration("L", "PT2S")),
            UpdateBySpec::Ema(em(in_time(2 * SECOND), None, None)),
        ),
        (
            "ema with unset options",
            proto_em!(Ema, UpdateByEma, em_options(0, 0), ticks(2.0)),
            UpdateBySpec::Ema(em(Ticks(2.0), None, None)),
        ),
        (
            "ema that resets on a null",
            proto_em!(Ema, UpdateByEma, em_options(2, 0), ticks(2.0)),
            UpdateBySpec::Ema(em(Ticks(2.0), Some(Reset), None)),
        ),
        (
            "ema that a NaN poisons",
            proto_em!(Ema, UpdateByEma, em_options(0, 4), ticks(2.0)),
            UpdateBySpec::Ema(em(Ticks(2.0), None, Some(Poison))),
        ),
        (
            "ema that resets on a NaN and skips a null",
            proto_em!(Ema, UpdateByEma, em_options(3, 2), ticks(2.0)),
            UpdateBySpec::Ema(em(Ticks(2.0), Some(Skip), Some(Reset))),
        ),
        (
            "ems",
            proto_em!(Ems, UpdateByEms, em_options(2, 0), nanos("L", 2 * SECOND)),
            UpdateBySpec::Ems(em(in_time(2 * SECOND), Some(Reset), None)),
        ),
        (
            "em min",
            proto_em!(EmMin, UpdateByEmMin, None, ticks(2.0)),
            UpdateBySpec::EmMin(em(Ticks(2.0), None, None)),
        ),
        (
            "em max",
            proto_em!(EmMax, UpdateByEmMax, em_options(0, 2), ticks(2.0)),
            UpdateBySpec::EmMax(em(Ticks(2.0), None, Some(Reset))),
        ),
    ];
    let mut results: Vec<(&str, Vec<String>)> = Vec::new();
    for (index, (name, proto_spec, engine_spec)) in cases.into_iter().enumerate() {
        let id = 10 + index as i32;
        let op = request(id, 1, vec![operation(proto_spec, &["X = D"])], &[]);
        call(&mut c, op)
            .await
            .unwrap_or_else(|status| panic!("{name}: {status:?}"));
        let operations = [UpdateByOperation::new(engine_spec, &["X = D"])];
        let expected = update_by(&source, &operations, &[""; 0])
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let served = values(c.table(id).as_ref(), "X");
        assert_eq!(served, values(expected.as_ref(), "X"), "{name}");
        results.push((name, served));
    }
    // the data tells the cases apart: no two of them have the same result but the unset
    // options, which are the defaults
    for (index, (name, result)) in results.iter().enumerate() {
        for (other, other_result) in &results[..index] {
            let same = *name == "ema with unset options" && *other == "ema by rows";
            assert_eq!(
                result == other_result,
                same,
                "{name} and {other}: {result:?}"
            );
        }
    }

    // the behaviors for times are not read: a value the protocol does not define is not
    // looked at
    let ignored = Some(UpdateByEmOptions {
        on_null_time: 99,
        on_negative_delta_time: 99,
        on_zero_delta_time: 99,
        ..Default::default()
    });
    let spec = proto_em!(Ema, UpdateByEma, ignored, ticks(2.0));
    call(
        &mut c,
        request(90, 1, vec![operation(spec, &["X = D"])], &[]),
    )
    .await
    .unwrap();
    let by_rows = results.iter().find(|(name, _)| *name == "ema by rows");
    assert_eq!(values(c.table(90).as_ref(), "X"), by_rows.unwrap().1);

    // a null with THROW fails the operation in the engine
    let spec = proto_em!(Ema, UpdateByEma, em_options(1, 0), ticks(2.0));
    let op = request(91, 1, vec![operation(spec, &["X = D"])], &[]);
    assert_internal(&call(&mut c, op).await.unwrap_err());
}

#[tokio::test]
async fn update_by_requests_are_checked_as_java_checks_them() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let one = |spec: Spec, pairs: &[&str]| request(2, 1, vec![operation(spec, pairs)], &[]);
    // options with a request whose operation is refused too
    let options =
        |options: UpdateByOptions| with_options(one(rolling_sum(None, None), &["in"]), options);
    let context = |precision: i32, rounding_mode: i32| {
        Some(proto::MathContext {
            precision,
            rounding_mode,
        })
    };
    let ema = |options: Option<UpdateByEmOptions>, scale: Scale| {
        proto_em!(Ema, UpdateByEma, options, scale)
    };
    let big_value = |precision: i32, rounding_mode: i32| {
        Some(UpdateByEmOptions {
            big_value_context: context(precision, rounding_mode),
            ..Default::default()
        })
    };
    let formula = |formula: &str, token: &str, reverse: Scale| {
        Spec::RollingFormula(spec::UpdateByRollingFormula {
            reverse_window_scale: reverse,
            forward_window_scale: ticks(0.0),
            formula: formula.to_string(),
            param_token: token.to_string(),
        })
    };
    let count_where = |result_column: &str, reverse: Scale| {
        Spec::RollingCountWhere(spec::UpdateByRollingCountWhere {
            reverse_window_scale: reverse,
            forward_window_scale: ticks(0.0),
            result_column: result_column.to_string(),
            filters: texts(&["N > 0"]),
        })
    };
    let no_type = ProtoOperation { r#type: None };
    let no_spec = ProtoOperation {
        r#type: Some(OperationType::Column(UpdateByColumn {
            spec: None,
            match_pairs: texts(&["in"]),
        })),
    };
    let unset = "Unexpected timescale type: TYPE_NOT_SET";
    let empty_name = "Invalid column name: name can not be null or empty";
    let negative = "UpdateBy rolling window size must be non-negative";
    let too_large = "UpdateBy rolling window size may not exceed MAX_ARRAY_SIZE (2147483639)";
    let bad_behavior = "Unexpected BadDataBehavior: UNRECOGNIZED";
    let no_rounding = "Unexpected rounding mode: ROUNDING_MODE_NOT_SPECIFIED";
    let cases: Vec<(Op, &str)> = vec![
        (
            request(2, 1, vec![], &["in"]),
            "Operations must not be empty",
        ),
        // the options come before the operations; of their checks the last one declared
        // in Java comes first
        (
            options(UpdateByOptions {
                chunk_capacity: Some(0),
                ..Default::default()
            }),
            "UpdateByControl.chunkCapacity() must be greater than 0, is 0",
        ),
        (
            options(UpdateByOptions {
                initial_hash_table_size: Some(0),
                ..Default::default()
            }),
            "UpdateByControl.initialHashTableSize() must be greater than 0, is 0",
        ),
        (
            options(UpdateByOptions {
                chunk_capacity: Some(0),
                initial_hash_table_size: Some(-3),
                ..Default::default()
            }),
            "UpdateByControl.initialHashTableSize() must be greater than 0, is -3",
        ),
        (
            options(UpdateByOptions {
                initial_hash_table_size: Some(0),
                maximum_load_factor: Some(2.0),
                ..Default::default()
            }),
            "UpdateByControl.maximumLoadFactor() must be in the range (0.0, 1.0), is 2.000000",
        ),
        (
            options(UpdateByOptions {
                maximum_load_factor: Some(f64::NAN),
                ..Default::default()
            }),
            "UpdateByControl.maximumLoadFactor() must be in the range (0.0, 1.0), is NaN",
        ),
        (
            options(UpdateByOptions {
                maximum_load_factor: Some(2.0),
                target_load_factor: Some(0.0),
                ..Default::default()
            }),
            "UpdateByControl.targetLoadFactor() must be in the range (0.0, 1.0), is 0.000000",
        ),
        (
            options(UpdateByOptions {
                target_load_factor: Some(f64::NEG_INFINITY),
                ..Default::default()
            }),
            "UpdateByControl.targetLoadFactor() must be in the range (0.0, 1.0), is -Infinity",
        ),
        // the factor that is not set has the server's default
        (
            options(UpdateByOptions {
                maximum_load_factor: Some(0.5),
                ..Default::default()
            }),
            "UpdateByControl.targetLoadFactor() must be less than or equal to \
             UpdateByControl.maximumLoadFactor(). targetLoadFactor=0.700000, \
             maximumLoadFactor=0.500000",
        ),
        (
            options(UpdateByOptions {
                target_load_factor: Some(1.0000005),
                ..Default::default()
            }),
            "UpdateByControl.targetLoadFactor() must be less than or equal to \
             UpdateByControl.maximumLoadFactor(). targetLoadFactor=1.000001, \
             maximumLoadFactor=0.750000",
        ),
        // the math context comes before the other options, its rounding mode first
        (
            options(UpdateByOptions {
                chunk_capacity: Some(0),
                math_context: context(-1, 0),
                ..Default::default()
            }),
            no_rounding,
        ),
        (
            options(UpdateByOptions {
                math_context: context(-1, 99),
                ..Default::default()
            }),
            "Unexpected rounding mode: UNRECOGNIZED",
        ),
        (
            options(UpdateByOptions {
                chunk_capacity: Some(0),
                math_context: context(-1, 1),
                ..Default::default()
            }),
            "Digits < 0",
        ),
        // the operations in order, the spec of each before its pairs
        (
            request(2, 1, vec![no_type, no_spec.clone()], &[]),
            "Unexpected operation type case: TYPE_NOT_SET",
        ),
        (
            request(2, 1, vec![no_spec, operation(sum(), &["in"])], &[]),
            "Unexpected spec type: TYPE_NOT_SET",
        ),
        (one(rolling_sum(None, ticks(1.0)), &["in"]), unset),
        (one(rolling_sum(ticks(1.0), None), &["in"]), unset),
        (
            one(
                rolling_sum(ticks(1.0), Some(UpdateByWindowScale { r#type: None })),
                &[],
            ),
            unset,
        ),
        (one(ema(None, None), &[]), unset),
        (
            one(rolling_sum(nanos("", 1), None), &[]),
            "WindowScale.timestampCol() must not be an empty string",
        ),
        (
            one(rolling_sum(ticks(-1.0), ticks(-1.0)), &["X = N"]),
            negative,
        ),
        (
            one(rolling_sum(nanos("I", 5), nanos("I", -6)), &["X = N"]),
            negative,
        ),
        (
            one(rolling_sum(ticks(2_147_483_640.0), ticks(0.0)), &["X = N"]),
            too_large,
        ),
        (
            one(rolling_sum(ticks(3e9), nanos("I", 1)), &["X = N"]),
            too_large,
        ),
        // the timestamp columns are compared before the size is checked
        (
            one(rolling_sum(nanos("I", -5), nanos("N", 1)), &["X = N"]),
            "UpdateBy rolling window forward and reverse timestamp columns must match",
        ),
        // the options of an exponential moving operator before its decay
        (one(ema(em_options(99, 0), None), &[]), bad_behavior),
        (one(ema(em_options(0, 98), None), &[]), bad_behavior),
        (one(ema(big_value(-1, 0), ticks(1.0)), &[]), no_rounding),
        (one(ema(big_value(-1, 8), ticks(1.0)), &[]), "Digits < 0"),
        // a formula before its window, and with a token nothing but that it is there
        (
            one(formula("", "", ticks(-1.0)), &[]),
            "formula must not be empty",
        ),
        (
            one(formula("", "x", ticks(-1.0)), &[]),
            "formula must not be empty",
        ),
        (
            one(formula("a==b", "", ticks(-1.0)), &[]),
            "Unable to parse formula 'a==b', expected form '<newColumn>=<expression>'",
        ),
        (
            one(formula("sum(x)", "", ticks(1.0)), &[]),
            "Invalid column name \"sum(x)\"",
        ),
        (one(formula("a==b", "x", ticks(-1.0)), &[]), negative),
        // the result column of a count before its window
        (
            one(count_where("1x", ticks(-1.0)), &[]),
            "Invalid column name \"1x\"",
        ),
        (one(count_where("", None), &[]), unset),
        (one(count_where("Count", ticks(-1.0)), &[]), negative),
        (
            one(
                Spec::CountWhere(spec::UpdateByCumulativeCountWhere {
                    result_column: String::new(),
                    filters: Vec::new(),
                }),
                &[],
            ),
            empty_name,
        ),
        // the pairs
        (
            one(sum(), &["X = N", "in"]),
            "Invalid column name \"in\": \"in\" is a reserved keyword",
        ),
        (one(sum(), &["X = N = I"]), "Invalid column name \"N = I\""),
        (one(sum(), &["X ="]), empty_name),
        (one(sum(), &["A B"]), "Invalid column name \"A B\""),
        // the group-by columns come last and are not trimmed
        (
            request(2, 1, vec![operation(sum(), &[" N "])], &[" S"]),
            "Invalid column name \" S\"",
        ),
        (
            request(2, 1, vec![operation(sum(), &["1"])], &[" S"]),
            "Invalid column name \"1\"",
        ),
    ];
    for (unused, (op, message)) in (900..).zip(cases) {
        let refused = call(&mut c, op.clone()).await.unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
        // in a batch the whole call is refused and nothing before it is created. The
        // export named before the refused operation stays claimed, so each batch names
        // another
        let refused = c
            .batch(vec![Op::EmptyTable(empty(ticket(unused), 1)), op])
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
        assert_eq!(refused.message(), message);
        let session = c.server.session(&c.bearer);
        assert_ne!(
            session.get_export(unused).unwrap().state(),
            ExportState::Exported
        );
    }

    // a duration that does not parse is not one of Java's argument errors, and it is read
    // before the column is checked
    for text in ["soon", "", "PT", "2S"] {
        let op = one(rolling_sum(duration("", text), None), &["in"]);
        assert_internal(&call(&mut c, op).await.unwrap_err());
    }

    // the result ticket of a refused request is still free, and options that pass change
    // nothing
    let accepted = with_options(
        one(sum(), &[" N "]),
        UpdateByOptions {
            use_redirection: Some(true),
            chunk_capacity: Some(1),
            max_static_sparse_memory_overhead: Some(-1.0),
            initial_hash_table_size: Some(1),
            maximum_load_factor: Some(0.5),
            target_load_factor: Some(0.5),
            math_context: context(0, 8),
        },
    );
    call(&mut c, accepted).await.unwrap();
    assert_eq!(values(c.table(2).as_ref(), "N"), ints(&[3, 5, 6, 6, 5, 3]));
}

#[tokio::test]
async fn what_the_engine_refuses_is_an_internal_failure() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let window = |column: &str| rolling_sum(nanos(column, 5), nanos(column, 0));
    let none: &[&str] = &[];
    // a request that fails in the export uses up its ticket
    let mut next = 10;
    for (operations, by) in [
        // an input column, a group-by column and a timestamp column the table lacks
        (vec![operation(sum(), &["X = Missing"])], none),
        (vec![operation(sum(), &["X = N"])], &["Missing"][..]),
        (vec![operation(window("Missing"), &["X = N"])], none),
        // a timestamp column that is not one, and two of them
        (vec![operation(window("S"), &["X = N"])], none),
        (
            vec![
                operation(window("I"), &["X = N"]),
                operation(window("N"), &["Y = I"]),
            ],
            none,
        ),
        // a type the operation has no operator for, and one result name twice
        (vec![operation(sum(), &["X = S"])], none),
        (
            vec![operation(sum(), &["X = N"]), operation(sum(), &["X = I"])],
            none,
        ),
    ] {
        next += 2;
        let refused = call(&mut c, request(next, 1, operations.clone(), by))
            .await
            .unwrap_err();
        assert_internal(&refused);
        // in a batch the failure is that of the operation, not of the call
        let (responses, status) = c
            .batch(vec![request(next + 1, 1, operations, by)])
            .await
            .unwrap();
        assert_eq!(responses.len(), 1);
        assert!(!responses[0].success);
        assert_internal(&status.unwrap());
    }
}

/// The values are compared with Java's in the engine's `ops/update_by/corpus.rs`.
#[tokio::test]
async fn the_standard_deviations_and_the_weighted_average_reach_the_engine() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let operations = vec![
        operation(
            proto_rolling!(RollingStd, UpdateByRollingStd, ticks(2.0), ticks(0.0)),
            &["StdN = N"],
        ),
        operation(
            Spec::RollingWavg(spec::UpdateByRollingWAvg {
                reverse_window_scale: ticks(2.0),
                forward_window_scale: ticks(0.0),
                weight_column: "I".to_string(),
            }),
            &["WavgN = N"],
        ),
        operation(
            proto_em!(EmStd, UpdateByEmStd, None, ticks(2.0)),
            &["EmStdN = N"],
        ),
    ];
    let response = call(&mut c, request(2, 1, operations, &["S"]))
        .await
        .unwrap();
    assert!(response.success);
    assert_eq!(response.size, 6);
    // a weight column the table lacks is refused as Java refuses it
    let no_weight = operation(
        Spec::RollingWavg(spec::UpdateByRollingWAvg {
            reverse_window_scale: ticks(2.0),
            forward_window_scale: ticks(0.0),
            weight_column: "Nope".to_string(),
        }),
        &["WavgN = N"],
    );
    let refused = call(&mut c, request(3, 1, vec![no_weight], &[]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
}

#[tokio::test]
async fn a_rolling_group_is_answered() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let group = |rev, fwd| {
        operation(
            proto_rolling!(RollingGroup, UpdateByRollingGroup, ticks(rev), ticks(fwd)),
            &["Group = N"],
        )
    };
    let answered = call(&mut c, request(2, 1, vec![group(2.0, 0.0)], &[]))
        .await
        .unwrap();
    assert!(answered.success, "{answered:?}");
    // A window of negative size is refused as for the other rolling operators.
    let refused = call(&mut c, request(3, 1, vec![group(-3.0, 2.0)], &[]))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
}

fn cum_count_where(result_column: &str, filters: &[&str]) -> Spec {
    Spec::CountWhere(spec::UpdateByCumulativeCountWhere {
        result_column: result_column.to_string(),
        filters: texts(filters),
    })
}

fn rolling_count_where(result_column: &str, filters: &[&str], rev: f64, fwd: f64) -> Spec {
    Spec::RollingCountWhere(spec::UpdateByRollingCountWhere {
        reverse_window_scale: ticks(rev),
        forward_window_scale: ticks(fwd),
        result_column: result_column.to_string(),
        filters: texts(filters),
    })
}

/// The values are compared with Java's in the engine's `ops/update_by/corpus.rs`, and
/// requests against both servers by the engine's
/// `ops/update_by/fixtures/compare_servers_count_where.py`.
#[tokio::test]
async fn the_count_where_operators_count_the_rows_that_pass_their_filters() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    // N is 3, 2, 1, 0, -1, -2 and S is a, b, a, b, a, b. The column pairs are not used.
    let operations = vec![
        operation(cum_count_where("Cum", &["N > 0", "I != 1"]), &["Q = Nope"]),
        operation(rolling_count_where("Roll", &["N + 0 >= 0"], 2.0, 1.0), &[]),
        operation(sum(), &["Sum = N"]),
    ];
    let response = call(&mut c, request(2, 1, operations.clone(), &[]))
        .await
        .unwrap();
    assert!(response.success, "{response:?}");
    let table = c.table(2);
    assert_eq!(
        column_names(table.as_ref()),
        ["I", "N", "S", "Cum", "Roll", "Sum"]
    );
    assert_eq!(values(table.as_ref(), "Cum"), ints(&[1, 1, 2, 2, 2, 2]));
    assert_eq!(values(table.as_ref(), "Roll"), ints(&[2, 3, 3, 2, 1, 0]));
    call(&mut c, request(3, 1, operations, &["S"]))
        .await
        .unwrap();
    assert_eq!(
        values(c.table(3).as_ref(), "Cum"),
        ints(&[1, 0, 2, 0, 2, 0])
    );

    // What Java refuses when it makes the filters is an internal failure: a filter that
    // does not compile, in whichever operation; a row variable; a column array; a
    // formula that is null for a row; a window that holds no row.
    let mut next = 10;
    for operations in [
        vec![operation(cum_count_where("X", &["Nope > 0"]), &[])],
        vec![
            operation(sum(), &["Y = Missing"]),
            operation(cum_count_where("X", &["N +"]), &[]),
        ],
        vec![operation(cum_count_where("X", &["N + 1"]), &[])],
        vec![operation(cum_count_where("X", &["i > 2"]), &[])],
        vec![operation(cum_count_where("X", &["N_.size() > 2"]), &[])],
        vec![operation(cum_count_where("X", &["(Boolean)null"]), &[])],
        vec![operation(
            rolling_count_where("X", &["N > 0"], 0.0, 0.0),
            &[],
        )],
        vec![
            operation(cum_count_where("X", &["N > 0"]), &[]),
            operation(sum(), &["X = N"]),
        ],
    ] {
        next += 1;
        let refused = call(&mut c, request(next, 1, operations, &[]))
            .await
            .unwrap_err();
        assert_internal(&refused);
    }

    // What Java answers with something that is not a function of the table is not
    // ported: no filters, and a comparison with a row variable on one side.
    let class = "io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.Context";
    for (spec, message) in [
        (
            cum_count_where("X", &[]),
            format!("not ported: {class}#applyFilters: a count-where without filters"),
        ),
        (
            rolling_count_where("X", &[], 2.0, 0.0),
            format!("not ported: {class}#applyFilters: a count-where without filters"),
        ),
        (
            cum_count_where("X", &["N > ii"]),
            format!(
                "not ported: {class}#applyFilters: a count-where filter with i, ii, k or a \
                 column array that Java runs over a table of the chunk at hand"
            ),
        ),
    ] {
        next += 1;
        let refused = call(&mut c, request(next, 1, vec![operation(spec, &[])], &[]))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::Unimplemented, "{refused:?}");
        assert_eq!(refused.message(), message);
    }
}

fn rolling_formula(formula: &str, token: &str, rev: f64, fwd: f64) -> Spec {
    Spec::RollingFormula(spec::UpdateByRollingFormula {
        reverse_window_scale: ticks(rev),
        forward_window_scale: ticks(fwd),
        formula: formula.to_string(),
        param_token: token.to_string(),
    })
}

/// The values are compared with Java's in the engine's `ops/update_by/corpus.rs`, and
/// requests against both servers by the engine's
/// `ops/update_by/fixtures/compare_servers_formula.py`.
#[tokio::test]
async fn a_rolling_formula_is_evaluated_over_the_windows_of_its_columns() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    // With a token, one result per pair; without one, the result the formula names,
    // whatever the pairs.
    let operations = vec![
        operation(rolling_formula("sum(x)", "x", 2.0, 0.0), &["F = N", "N"]),
        operation(rolling_sum(ticks(2.0), ticks(0.0)), &["R = N"]),
        operation(
            rolling_formula("M = sum(N) + len(I)", "", 2.0, 0.0),
            &["Unused = Nope"],
        ),
    ];
    let response = call(&mut c, request(2, 1, operations.clone(), &[]))
        .await
        .unwrap();
    assert!(response.success, "{response:?}");
    let table = c.table(2);
    assert_eq!(column_names(table.as_ref()), ["I", "N", "S", "F", "R", "M"]);
    let sums = values(table.as_ref(), "R");
    assert_eq!(values(table.as_ref(), "F"), sums);
    assert_eq!(values(table.as_ref(), "N"), sums);
    assert_eq!(values(table.as_ref(), "M"), ints(&[4, 7, 5, 3, 1, -1]));
    // A bucket column is a scalar in the formula without a token.
    let keyed = vec![operation(
        rolling_formula("M = S + count(N)", "", 2.0, 0.0),
        &[],
    )];
    let response = call(&mut c, request(3, 1, keyed, &["S"])).await.unwrap();
    assert!(response.success, "{response:?}");

    // What Java refuses is an internal failure. Its validation replaces the token
    // wherever it occurs, so `max(x)` is validated as `maN(N)`; it groups the table by the
    // bucket columns, so a bucket column is no vector; and the engine knows no column
    // but the token.
    let mut next = 10;
    for (spec, pairs, by) in [
        (
            rolling_formula("max(x)", "x", 2.0, 0.0),
            &["X = N"][..],
            &[][..],
        ),
        (rolling_formula("sum(x)", "x", 2.0, 0.0), &["X = S"], &["S"]),
        (
            rolling_formula("sum(x) + I", "x", 2.0, 0.0),
            &["X = N"],
            &[],
        ),
        (rolling_formula("sum(y)", "x", 2.0, 0.0), &["X = N"], &[]),
        (rolling_formula("sum(x)", "x", 2.0, 0.0), &["X = Nope"], &[]),
        (rolling_formula("X = sum(Nope)", "", 2.0, 0.0), &[], &[]),
        (rolling_formula("X = sum(N) + i", "", 2.0, 0.0), &[], &[]),
        (rolling_formula("X = sum(S)", "", 2.0, 0.0), &[], &["S"]),
        (rolling_formula("X = sum(N)", "", 0.0, 0.0), &[], &[]),
    ] {
        next += 1;
        let refused = call(&mut c, request(next, 1, vec![operation(spec, pairs)], by))
            .await
            .unwrap_err();
        assert_internal(&refused);
    }
}

#[tokio::test]
async fn an_operator_that_is_not_ported_is_unimplemented_by_name() {
    let mut c = Connected::start().await;
    numbers(&mut c, 1).await;
    let wrapper = "io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper";
    let cases = [
        (
            rolling_formula("x.get(0)", "x", 2.0, 0.0),
            format!(
                "not ported: {wrapper}.IntRingBufferVectorWrapper#get: get in a rolling \
                 formula: outside a window Java throws NoSuchElementException"
            ),
        ),
        (
            rolling_formula("X = N[1]", "", 2.0, 0.0),
            format!(
                "not ported: {wrapper}.IntRingBufferVectorWrapper#get: [] in a rolling \
                 formula: outside a window Java throws NoSuchElementException"
            ),
        ),
        (
            rolling_formula("sum(x)", "x|y", 2.0, 0.0),
            "not ported: java.lang.String#replaceAll: the parameter token x|y, which Java \
             reads as a regular expression"
                .to_string(),
        ),
    ];
    // a request that fails in the export uses up its ticket
    let mut next = 10;
    for (spec, message) in cases {
        // alone, and after an operator that is ported
        for operations in [
            vec![operation(spec.clone(), &["X = N"])],
            vec![
                operation(sum(), &["Y = N"]),
                operation(spec.clone(), &["X = N"]),
            ],
        ] {
            next += 2;
            let refused = call(&mut c, request(next, 1, operations.clone(), &["S"]))
                .await
                .unwrap_err();
            assert_eq!(refused.code(), Code::Unimplemented, "{refused:?}");
            assert_eq!(refused.message(), message);
            let (responses, status) = c
                .batch(vec![request(next + 1, 1, operations, &["S"])])
                .await
                .unwrap();
            assert_eq!(responses.len(), 1);
            assert!(!responses[0].success);
            let status = status.unwrap();
            assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
            assert_eq!(status.message(), message);
        }
    }

    // Java takes a column name outside ASCII; the engine's update_by does not
    let op = request(2, 1, vec![operation(sum(), &["\u{dc}bung = N"])], &[]);
    let refused = call(&mut c, op).await.unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented, "{refused:?}");
    assert_eq!(
        refused.message(),
        "not ported: io.deephaven.api.Pair#parse: a column name outside ASCII in \u{dc}bung=N"
    );

    // the server still answers
    call(
        &mut c,
        request(3, 1, vec![operation(sum(), &["X = N"])], &[]),
    )
    .await
    .unwrap();
    assert_eq!(values(c.table(3).as_ref(), "X"), ints(&[3, 5, 6, 6, 5, 3]));
}

#[tokio::test]
async fn update_by_of_a_refreshing_table_follows_it() {
    let mut c = Connected::start().await;
    call(&mut c, Op::TimeTable(ticking_time_table(ticket(1))))
        .await
        .unwrap();
    call(&mut c, Op::UpdateView(formulas(1, 2, &["One = 1"])))
        .await
        .unwrap();
    let op = request(3, 2, vec![operation(sum(), &["Rows = One"])], &[]);
    let response = call(&mut c, op).await.unwrap();
    assert!(!response.is_static);
    let mut sizes = Vec::new();
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(5)).await;
        c.cycle().await;
        let rows = values(c.table(3).as_ref(), "Rows");
        let expected: Vec<i64> = (1..=rows.len() as i64).collect();
        assert_eq!(rows, ints(&expected));
        sizes.push(rows.len());
    }
    assert!(sizes[0] > 0 && sizes[2] > sizes[0], "{sizes:?}");
}
