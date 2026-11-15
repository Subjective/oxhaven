//! `UpdateBy`: the source's columns and, per operation and input column, a column computed
//! from each row and the rows around it in its bucket.
//!
//! The request is read the way Java reads it, in Java's order, so that a request with
//! several faults is refused for the same one: the options, then each operation (its spec,
//! then its column pairs), then the group-by columns. Everything the request alone decides
//! is checked before the export exists; what needs the table is left to the engine.
//!
//! The engine has no counterpart of the control options (see `ops::update_by`): they are
//! checked as Java checks them and otherwise have no effect on the result.
//!
//! An operator the engine has not ported (`ops::update_by` lists them) passes the request
//! checks and is then refused by the engine's `unported!`, which the service answers
//! `UNIMPLEMENTED`.
//!
//! Java checks the filters of the count-where operators of every operation against the
//! table with its `ColumnExpressionValidator` before it runs any of them
//! (`UpdateByGrpcImpl#validateFormulas`). Here the engine makes the filters with the
//! formula compiler, which knows only the function library, so a filter Java's validator
//! refuses does not compile (see `select.rs`); both servers answer `INVALID_ARGUMENT` with
//! an error id, whichever operation the fault is in. The count-where requests whose result
//! in Java is not a function of the table are answered `UNIMPLEMENTED`: without filters
//! here, the others by the engine (`ops::update_by::count_where`).
//!
//! Java validates a rolling formula the same way, against the table grouped by the bucket
//! columns. For the form without a parameter token that is the formula the engine
//! compiles, so nothing is done here. For the form with one, Java 42.9 validates another
//! text than its engine compiles: the formula with the token replaced by the input
//! column wherever it occurs, also inside a name ([`validate_token_formulas`]).

use super::prelude::*;
use super::time_table::parse_duration_nanos;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_engine::ops::select::ArrayFormula;
use oxhaven_engine::ops::update_by::{
    update_by_with, BadDataBehavior, DeltaControl, EmSpec, RollingWindow, UpdateByError,
    UpdateByOperation, UpdateBySpec, WindowScale,
};
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::math_context::RoundingMode;
use oxhaven_proto::grpc::update_by_request::update_by_operation::update_by_column::update_by_spec::Type as SpecType;
use oxhaven_proto::grpc::update_by_request::update_by_operation::update_by_column::UpdateBySpec as ProtoSpec;
use oxhaven_proto::grpc::update_by_request::update_by_operation::Type as OperationType;
use oxhaven_proto::grpc::update_by_request::{UpdateByOperation as ProtoOperation, UpdateByOptions};
use oxhaven_proto::grpc::update_by_window_scale::update_by_window_time::Window as TimeWindow;
use oxhaven_proto::grpc::update_by_window_scale::Type as ScaleType;
use oxhaven_proto::grpc::{
    UpdateByDeltaOptions, UpdateByEmOptions, UpdateByNullBehavior, UpdateByRequest,
    UpdateByWindowScale,
};

/// What Java's `updateBy` refuses is an exception out of the engine: the recorded results
/// have `INVALID_ARGUMENT` for a timestamp column that is not one.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<UpdateByError> for ExportError {
    fn from(error: UpdateByError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// Why a request cannot be read, by what Java throws for it.
#[derive(Debug, PartialEq)]
enum Refusal {
    /// An `IllegalArgumentException`: `UpdateByGrpcImpl#validateRequest` answers it
    /// `INVALID_ARGUMENT` with this message.
    Argument(String),
    /// Any other exception, which leaves the request handler and is answered
    /// `INVALID_ARGUMENT` with an error id, the text in the log.
    Exception(String),
}

impl From<Refusal> for Status {
    fn from(refusal: Refusal) -> Self {
        match refusal {
            Refusal::Argument(message) => Status::invalid_argument(message),
            Refusal::Exception(message) => internal_status(message),
        }
    }
}

fn argument(message: impl Into<String>) -> Refusal {
    Refusal::Argument(message.into())
}

/// A column name that is used as it is given. `InvalidNameException` is an
/// `IllegalArgumentException`.
///
/// Behavior: `io.deephaven.api.ColumnName#of`, `ColumnName#checkName`
fn column_name(name: &str) -> Result<&str, Refusal> {
    validate_column_name(name).map_err(Refusal::Argument)?;
    Ok(name)
}

/// `Output=Input` split at the first `=`, or one name for both, each side without the
/// space around it. The result is the pair as the engine reads it.
///
/// Behavior: `io.deephaven.api.Pair#parse`, `io.deephaven.api.ColumnName#parse`
fn pair(text: &str) -> Result<String, Refusal> {
    match text.split_once('=') {
        Some((output, input)) => {
            let output = column_name(java_trim(output))?;
            let input = column_name(java_trim(input))?;
            Ok(format!("{output}={input}"))
        }
        None => Ok(column_name(java_trim(text))?.to_string()),
    }
}

/// A double as `String.format("%f", value)` prints it: six decimals, rounded half up from
/// the shortest decimal that names the double, not from its exact value, so `5.0E-7` is
/// `0.000001`. (Java's formatter takes its digits from `FloatingDecimal`, which for a few
/// doubles has one digit more than the shortest; that can only matter in the seventeenth
/// digit.)
///
/// Behavior: `java.util.Formatter` with the conversion `%f`, run on OpenJDK 21
fn java_fixed(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    if value.is_infinite() {
        return format!("{sign}Infinity");
    }
    // the digits d1 d2 ... of the shortest decimal, which is 0.d1d2... * 10^point
    let text = format!("{:e}", value.abs());
    let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
    let point = exponent.parse::<i64>().unwrap_or(0) + 1;
    let shortest: Vec<u8> = mantissa
        .bytes()
        .filter(u8::is_ascii_digit)
        .map(|b| b - b'0')
        .collect();
    let digit = |index: i64| -> u8 {
        usize::try_from(index)
            .ok()
            .and_then(|index| shortest.get(index).copied())
            .unwrap_or(0)
    };
    // the value in millionths: the digits before the point and six after it
    let mut digits: Vec<u8> = (0..point + 6).map(digit).collect();
    if digit(point + 6) >= 5 {
        let mut carried = true;
        for d in digits.iter_mut().rev() {
            if *d == 9 {
                *d = 0;
            } else {
                *d += 1;
                carried = false;
                break;
            }
        }
        if carried {
            digits.insert(0, 1);
        }
    }
    while digits.len() < 7 {
        digits.insert(0, 0);
    }
    let digits: String = digits.iter().map(|d| char::from(b'0' + d)).collect();
    let (whole, fraction) = digits.split_at(digits.len() - 6);
    format!("{sign}{whole}.{fraction}")
}

/// Checks a math context. The rounding mode is read before the precision.
///
/// Behavior: `UpdateByGrpcImpl#adaptMathContext`, `UpdateByGrpcImpl#adaptRoundingMode`,
/// `java.math.MathContext#MathContext(int, RoundingMode)`
fn math_context(context: &proto::MathContext) -> Result<(), Refusal> {
    match RoundingMode::try_from(context.rounding_mode) {
        Ok(RoundingMode::NotSpecified) => {
            return Err(argument(
                "Unexpected rounding mode: ROUNDING_MODE_NOT_SPECIFIED",
            ))
        }
        Ok(_) => {}
        Err(_) => return Err(argument("Unexpected rounding mode: UNRECOGNIZED")),
    }
    if context.precision < 0 {
        return Err(argument("Digits < 0"));
    }
    Ok(())
}

/// Checks the control options. An option that is not set has the default of the Java
/// server, which the checks then see: 4096 for the chunk capacity and the initial hash
/// table size, 0.75 for the maximum load factor and 0.7 for the target load factor. The
/// checks run in the order of the generated `ImmutableUpdateByControl`, the last one
/// declared first (observed on the 42.9 jar).
///
/// Behavior: `UpdateByGrpcImpl#adaptOptions`, the `@Check` methods and defaults of
/// `io.deephaven.api.updateby.UpdateByControl`
fn options(options: &UpdateByOptions) -> Result<(), Refusal> {
    if let Some(context) = &options.math_context {
        math_context(context)?;
    }
    let chunk_capacity = options.chunk_capacity.unwrap_or(4096);
    let initial_hash_table_size = options.initial_hash_table_size.unwrap_or(4096);
    let maximum = options.maximum_load_factor.unwrap_or(0.75);
    let target = options.target_load_factor.unwrap_or(0.7);
    // `!(0 < x < 1)` is also true of NaN, as Java's explicit test is
    let outside = |factor: f64| !(factor > 0.0 && factor < 1.0);
    if target > maximum {
        return Err(argument(format!(
            "UpdateByControl.targetLoadFactor() must be less than or equal to \
             UpdateByControl.maximumLoadFactor(). targetLoadFactor={}, maximumLoadFactor={}",
            java_fixed(target),
            java_fixed(maximum)
        )));
    }
    if outside(target) {
        return Err(argument(format!(
            "UpdateByControl.targetLoadFactor() must be in the range (0.0, 1.0), is {}",
            java_fixed(target)
        )));
    }
    if outside(maximum) {
        return Err(argument(format!(
            "UpdateByControl.maximumLoadFactor() must be in the range (0.0, 1.0), is {}",
            java_fixed(maximum)
        )));
    }
    if initial_hash_table_size <= 0 {
        return Err(argument(format!(
            "UpdateByControl.initialHashTableSize() must be greater than 0, is \
             {initial_hash_table_size}"
        )));
    }
    if chunk_capacity <= 0 {
        return Err(argument(format!(
            "UpdateByControl.chunkCapacity() must be greater than 0, is {chunk_capacity}"
        )));
    }
    Ok(())
}

/// One side of a window, or the decay of an exponential moving operator. A scale that is
/// not set is the message without a type. A duration string that does not parse is not an
/// `IllegalArgumentException` in Java, and it is read before the column is checked.
///
/// Behavior: `UpdateByGrpcImpl#adaptWindowScale`,
/// `io.deephaven.time.DateTimeUtils#parseDurationNanos`,
/// `io.deephaven.api.updateby.spec.WindowScale#checkTimestampColEmpty`
fn window_scale(scale: &Option<UpdateByWindowScale>) -> Result<WindowScale, Refusal> {
    match scale.as_ref().and_then(|scale| scale.r#type.as_ref()) {
        Some(ScaleType::Ticks(ticks)) => Ok(WindowScale::Ticks(ticks.ticks)),
        Some(ScaleType::Time(time)) => {
            let nanos = match &time.window {
                Some(TimeWindow::DurationString(text)) => parse_duration_nanos(text)
                    .map_err(|e| Refusal::Exception(format!("Cannot parse time: {text}: {e}")))?,
                Some(TimeWindow::Nanos(nanos)) => *nanos,
                None => 0,
            };
            if time.column.is_empty() {
                return Err(argument(
                    "WindowScale.timestampCol() must not be an empty string",
                ));
            }
            Ok(WindowScale::Time {
                column: time.column.clone(),
                nanos,
            })
        }
        None => Err(argument("Unexpected timescale type: TYPE_NOT_SET")),
    }
}

/// The two sides of a rolling window, the reverse side read first. The window itself is
/// checked by [`check_window`] once the rest of the spec has been read.
fn rolling_window(
    reverse: &Option<UpdateByWindowScale>,
    forward: &Option<UpdateByWindowScale>,
) -> Result<RollingWindow, Refusal> {
    Ok(RollingWindow {
        rev: window_scale(reverse)?,
        fwd: window_scale(forward)?,
    })
}

/// The checks of a rolling window, in the order of the generated spec classes: the
/// timestamp columns before the size (observed on the 42.9 jar). The size is the sum of
/// both sides, ticks and nanoseconds alike, and has an upper limit only when the reverse
/// side is in ticks.
///
/// Behavior: `io.deephaven.api.updateby.spec.RollingOpSpec#checkCompatibleTimeColumns`,
/// `RollingOpSpec#checkWindowSizes`, `WindowScale#getFractionalTimeScaleUnits`
fn check_window(window: &RollingWindow) -> Result<(), Refusal> {
    // Behavior: `RollingOpSpec.MAX_ARRAY_SIZE`
    const MAX_ARRAY_SIZE: i32 = i32::MAX - 8;
    let units = |scale: &WindowScale| match scale {
        WindowScale::Ticks(ticks) => *ticks,
        WindowScale::Time { nanos, .. } => *nanos as f64,
    };
    if let (WindowScale::Time { column: reverse, .. }, WindowScale::Time { column: forward, .. }) =
        (&window.rev, &window.fwd)
    {
        if reverse != forward {
            return Err(argument(
                "UpdateBy rolling window forward and reverse timestamp columns must match",
            ));
        }
    }
    let size = units(&window.rev) + units(&window.fwd);
    if size < 0.0 {
        return Err(argument(
            "UpdateBy rolling window size must be non-negative",
        ));
    }
    if matches!(window.rev, WindowScale::Ticks(_)) && size > f64::from(MAX_ARRAY_SIZE) {
        return Err(argument(format!(
            "UpdateBy rolling window size may not exceed MAX_ARRAY_SIZE ({MAX_ARRAY_SIZE})"
        )));
    }
    Ok(())
}

/// A rolling window that has nothing else to check.
fn window(
    reverse: &Option<UpdateByWindowScale>,
    forward: &Option<UpdateByWindowScale>,
) -> Result<RollingWindow, Refusal> {
    let window = rolling_window(reverse, forward)?;
    check_window(&window)?;
    Ok(window)
}

/// What to do with a null or a NaN, or `None` to leave it to the default, which is to
/// skip the value.
///
/// Behavior: `UpdateByGrpcImpl#adaptEmOptions`, `UpdateByGrpcImpl#adaptBadDataBehavior`
fn bad_data(behavior: i32) -> Result<Option<BadDataBehavior>, Refusal> {
    match proto::BadDataBehavior::try_from(behavior) {
        Ok(proto::BadDataBehavior::NotSpecified) => Ok(None),
        Ok(proto::BadDataBehavior::Throw) => Ok(Some(BadDataBehavior::Throw)),
        Ok(proto::BadDataBehavior::Reset) => Ok(Some(BadDataBehavior::Reset)),
        Ok(proto::BadDataBehavior::Skip) => Ok(Some(BadDataBehavior::Skip)),
        Ok(proto::BadDataBehavior::Poison) => Ok(Some(BadDataBehavior::Poison)),
        Err(_) => Err(argument("Unexpected BadDataBehavior: UNRECOGNIZED")),
    }
}

/// The parameters of an exponential moving operator: the options, if any, then the decay.
/// Of the options Java reads the behavior for a null value, the one for a NaN and the math
/// context, in that order, and none of the three for times.
///
/// Behavior: `UpdateByGrpcImpl#adaptEma` (and `adaptEms`, `adaptEmMin`, `adaptEmMax`,
/// `adaptEmStd`), `UpdateByGrpcImpl#adaptEmOptions`
fn em(
    options: &Option<UpdateByEmOptions>,
    scale: &Option<UpdateByWindowScale>,
) -> Result<EmSpec, Refusal> {
    let (on_null, on_nan) = match options {
        Some(options) => {
            let on_null = bad_data(options.on_null_value)?;
            let on_nan = bad_data(options.on_nan_value)?;
            if let Some(context) = &options.big_value_context {
                math_context(context)?;
            }
            (on_null, on_nan)
        }
        None => (None, None),
    };
    Ok(EmSpec {
        window: window_scale(scale)?,
        on_null,
        on_nan,
    })
}

/// What `delta` gives for a value after a null: the null unless the options say otherwise,
/// also for a behavior the protocol does not define.
///
/// Behavior: `UpdateByGrpcImpl#adaptDelta`, `UpdateByGrpcImpl#adaptDeltaOptions`,
/// `io.deephaven.api.updateby.DeltaControl#DEFAULT`
fn delta(options: &Option<UpdateByDeltaOptions>) -> DeltaControl {
    let behavior = options
        .as_ref()
        .map(|options| UpdateByNullBehavior::try_from(options.null_behavior));
    match behavior {
        Some(Ok(UpdateByNullBehavior::ValueDominates)) => DeltaControl::ValueDominates,
        Some(Ok(UpdateByNullBehavior::ZeroDominates)) => DeltaControl::ZeroDominates,
        _ => DeltaControl::NullDominates,
    }
}

/// Checks a rolling formula that has no parameter token as the column assignment it must
/// be: `Name`, or `Name=expression` with something other than `=` after the `=`.
///
/// Behavior: `io.deephaven.api.Selectable#parse`
fn check_selectable(formula: &str) -> Result<(), Refusal> {
    let Some((name, expression)) = formula.split_once('=') else {
        return column_name(java_trim(formula)).map(|_| ());
    };
    if expression.is_empty() || expression.starts_with('=') {
        return Err(argument(format!(
            "Unable to parse formula '{formula}', expected form '<newColumn>=<expression>'"
        )));
    }
    column_name(java_trim(name)).map(|_| ())
}

/// Refuses the rolling formulas with a parameter token that Java's validation refuses
/// before its engine sees them. For each column pair Java compiles
/// `formula.replaceAll(token, input)` against a table of the bucket columns and the input
/// column, grouped by the bucket columns: the input is a vector unless it is a bucket
/// column, and the token is replaced wherever it occurs (`max(x)` with the token `x` and
/// the input `I` is validated as `maI(I)`, and refused).
///
/// A token that is not made of letters, digits and `_` is a regular expression to
/// `replaceAll`, and an input name with `$` a group reference: both are refused as not
/// ported.
///
/// Behavior: `UpdateByGrpcImpl#validateFormulaWithParamToken`
fn validate_token_formulas(
    context: &OpContext,
    operations: &[UpdateByOperation],
    parent: &dyn Table,
    by: &[String],
) -> Result<(), ExportError> {
    let columns = parent.definition().columns();
    let is_key = |name: &str| by.iter().any(|key| key == name);
    for operation in operations {
        let UpdateBySpec::RollingFormula {
            formula,
            param_token: Some(token),
            ..
        } = &operation.spec
        else {
            continue;
        };
        if !token.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(not_ported(
                "java.lang.String#replaceAll",
                format!("the parameter token {token}, which Java reads as a regular expression"),
            )
            .into());
        }
        let inputs: Vec<&str> = if operation.columns.is_empty() {
            columns
                .iter()
                .map(|column| column.name.as_str())
                .filter(|name| !is_key(name))
                .collect()
        } else {
            operation
                .columns
                .iter()
                .map(|pair| java_trim(pair.split_once('=').map_or(pair.as_str(), |(_, input)| input)))
                .collect()
        };
        for input in inputs {
            if input.contains(['$', '\\']) {
                return Err(not_ported(
                    "java.lang.String#replaceAll",
                    format!("the input column {input}, which Java reads as a replacement pattern"),
                )
                .into());
            }
            // Behavior: `TableTools.newTable(definition).groupBy(groupByColumns)`, which
            // refuses to group a table that has only its key columns.
            let seen: Vec<(&str, ColumnType)> = columns
                .iter()
                .filter(|column| is_key(&column.name) || column.name == input)
                .map(|column| {
                    let column_type = if is_key(&column.name) {
                        column.column_type.clone()
                    } else {
                        ColumnType::vector(column.column_type.clone())
                    };
                    (column.name.as_str(), column_type)
                })
                .collect();
            if seen.iter().all(|(name, _)| is_key(name)) {
                return Err(ExportError::Internal(format!(
                    "aggAllBy has no columns to aggregate: spec=AggSpecGroup{{}}, groupByColumns=[{}]",
                    by.join(", ")
                )));
            }
            let replaced = formula.replace(token.as_str(), input);
            ArrayFormula::compile(context.compiler.as_ref(), &replaced, &seen)
                .map_err(|error| ExportError::Internal(error.to_string()))?;
        }
    }
    Ok(())
}

/// The engine's operation for a spec. A spec that is not set is the message without a
/// type.
///
/// Behavior: `UpdateByGrpcImpl#adaptSpec` and the `adapt*` method of each spec, with the
/// checks of the spec classes in `io.deephaven.api.updateby.spec`:
/// `RollingFormulaSpec#checkFormula` (before the window's checks),
/// `CumCountWhereSpec#of` and `RollingCountWhereSpec#of` (the result column, before the
/// window's checks)
fn spec(spec: &Option<ProtoSpec>) -> Result<UpdateBySpec, Refusal> {
    let Some(spec) = spec.as_ref().and_then(|spec| spec.r#type.as_ref()) else {
        return Err(argument("Unexpected spec type: TYPE_NOT_SET"));
    };
    Ok(match spec {
        SpecType::Sum(_) => UpdateBySpec::CumSum,
        SpecType::Min(_) => UpdateBySpec::CumMin,
        SpecType::Max(_) => UpdateBySpec::CumMax,
        SpecType::Product(_) => UpdateBySpec::CumProd,
        SpecType::Fill(_) => UpdateBySpec::ForwardFill,
        SpecType::CountWhere(s) => UpdateBySpec::CumCountWhere {
            result_column: column_name(&s.result_column)?.to_string(),
            filters: s.filters.clone(),
        },
        SpecType::Ema(s) => UpdateBySpec::Ema(em(&s.options, &s.window_scale)?),
        SpecType::Ems(s) => UpdateBySpec::Ems(em(&s.options, &s.window_scale)?),
        SpecType::EmMax(s) => UpdateBySpec::EmMax(em(&s.options, &s.window_scale)?),
        SpecType::EmMin(s) => UpdateBySpec::EmMin(em(&s.options, &s.window_scale)?),
        SpecType::EmStd(s) => UpdateBySpec::EmStd(em(&s.options, &s.window_scale)?),
        SpecType::Delta(s) => UpdateBySpec::Delta(delta(&s.options)),
        SpecType::RollingSum(s) => {
            UpdateBySpec::RollingSum(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingGroup(s) => {
            UpdateBySpec::RollingGroup(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingAvg(s) => {
            UpdateBySpec::RollingAvg(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingMin(s) => {
            UpdateBySpec::RollingMin(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingMax(s) => {
            UpdateBySpec::RollingMax(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingProduct(s) => UpdateBySpec::RollingProduct(window(
            &s.reverse_window_scale,
            &s.forward_window_scale,
        )?),
        SpecType::RollingCount(s) => {
            UpdateBySpec::RollingCount(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingStd(s) => {
            UpdateBySpec::RollingStd(window(&s.reverse_window_scale, &s.forward_window_scale)?)
        }
        SpecType::RollingWavg(s) => UpdateBySpec::RollingWAvg {
            window: window(&s.reverse_window_scale, &s.forward_window_scale)?,
            weight_column: s.weight_column.clone(),
        },
        SpecType::RollingFormula(s) => {
            let window = rolling_window(&s.reverse_window_scale, &s.forward_window_scale)?;
            if s.formula.is_empty() {
                return Err(argument("formula must not be empty"));
            }
            let param_token = (!s.param_token.is_empty()).then(|| s.param_token.clone());
            if param_token.is_none() {
                check_selectable(&s.formula)?;
            }
            check_window(&window)?;
            UpdateBySpec::RollingFormula {
                window,
                formula: s.formula.clone(),
                param_token,
            }
        }
        SpecType::RollingCountWhere(s) => {
            let window = rolling_window(&s.reverse_window_scale, &s.forward_window_scale)?;
            let result_column = column_name(&s.result_column)?.to_string();
            check_window(&window)?;
            UpdateBySpec::RollingCountWhere {
                window,
                result_column,
                filters: s.filters.clone(),
            }
        }
    })
}

/// The engine's operation for one of the request: its spec, then its column pairs.
///
/// Behavior: `UpdateByGrpcImpl#adaptOperation`, `UpdateByGrpcImpl#adaptColumn`
fn operation(operation: &ProtoOperation) -> Result<UpdateByOperation, Refusal> {
    let Some(OperationType::Column(column)) = &operation.r#type else {
        return Err(argument("Unexpected operation type case: TYPE_NOT_SET"));
    };
    let spec = spec(&column.spec)?;
    let columns = column
        .match_pairs
        .iter()
        .map(|text| pair(text.as_str()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(UpdateByOperation { spec, columns })
}

/// The operations of a request, read and checked in Java's order. The group-by columns
/// are used as they are given, without trimming.
///
/// Behavior: `UpdateByGrpcImpl#validateRequest`, and the same reading in
/// `UpdateByGrpcImpl#create`
fn operations(request: &UpdateByRequest) -> Result<Vec<UpdateByOperation>, Refusal> {
    if request.operations.is_empty() {
        return Err(argument("Operations must not be empty"));
    }
    if let Some(options) = &request.options {
        self::options(options)?;
    }
    let operations = request
        .operations
        .iter()
        .map(operation)
        .collect::<Result<Vec<_>, _>>()?;
    for name in &request.group_by_columns {
        column_name(name)?;
    }
    Ok(operations)
}

/// Behavior: `io.deephaven.server.table.ops.UpdateByGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = UpdateByRequest;

    fn result_id(request: &UpdateByRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &UpdateByRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Behavior: `UpdateByGrpcImpl#validateRequest`
    fn validate(request: &UpdateByRequest) -> Result<(), Status> {
        operations(request)?;
        Ok(())
    }

    /// Java reads the request again here, outside the `try` of `validateRequest`, so a
    /// refusal would be an internal failure; a request that passed `validate` has none.
    ///
    /// Behavior: `UpdateByGrpcImpl#create`
    fn create(
        context: &OpContext,
        request: &UpdateByRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let operations = operations(request).map_err(|refusal| match refusal {
            Refusal::Argument(message) | Refusal::Exception(message) => {
                ExportError::Internal(message)
            }
        })?;
        // Java takes any Java identifier. The engine reads its pairs with ASCII names only
        // and would refuse such a pair as if the request were wrong.
        if let Some(pair) = operations
            .iter()
            .flat_map(|operation| &operation.columns)
            .find(|pair| !pair.is_ascii())
        {
            return Err(not_ported(
                "io.deephaven.api.Pair#parse",
                format!("a column name outside ASCII in {pair}"),
            )
            .into());
        }
        // The engine panics with `unported!` for a count-where without filters, whose
        // count Java leaves undefined; a request for one is refused here instead.
        let unfiltered = operations.iter().any(|operation| match &operation.spec {
            UpdateBySpec::CumCountWhere { filters, .. }
            | UpdateBySpec::RollingCountWhere { filters, .. } => filters.is_empty(),
            _ => false,
        });
        if unfiltered {
            return Err(not_ported(
                "io.deephaven.engine.table.impl.updateby.countwhere.CountWhereOperator.Context#applyFilters",
                "a count-where without filters",
            )
            .into());
        }
        validate_token_formulas(
            context,
            &operations,
            &*sources[0],
            &request.group_by_columns,
        )?;
        Ok(update_by_with(
            &sources[0],
            &operations,
            &request.group_by_columns,
            context.compiler.as_ref(),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_print_as_javas_percent_f() {
        // `String.format("%f", x)` on OpenJDK 21
        for (value, text) in [
            (0.5, "0.500000"),
            (2.0, "2.000000"),
            (0.75, "0.750000"),
            (1.1, "1.100000"),
            (-2.5, "-2.500000"),
            (0.0, "0.000000"),
            (-0.0, "-0.000000"),
            (1.0000005, "1.000001"),
            (5.0e-7, "0.000001"),
            (2.5e-7, "0.000000"),
            (1.5e-6, "0.000002"),
            (4.35e-6, "0.000004"),
            (1.25e-5, "0.000013"),
            (0.002, "0.002000"),
            (0.1234565, "0.123457"),
            (0.9999995, "1.000000"),
            (0.30000000000000004, "0.300000"),
            (123456.7890125, "123456.789013"),
            (1.0e7, "10000000.000000"),
            (1.0e-10, "0.000000"),
            (4.9e-324, "0.000000"),
            (8.41e21, "8410000000000000000000.000000"),
            (1.0e22, "10000000000000000000000.000000"),
            (1.0e23, "100000000000000000000000.000000"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ] {
            assert_eq!(java_fixed(value), text, "{value:e}");
        }
        let largest = java_fixed(f64::MAX);
        assert!(largest.starts_with("17976931348623157000"), "{largest}");
        assert_eq!(largest.len(), 309 + 7);
    }

    #[test]
    fn a_pair_is_a_name_or_two_names_around_the_first_equals_sign() {
        // `Pair.parse` of the 42.9 jar
        assert_eq!(pair("A"), Ok("A".to_string()));
        assert_eq!(pair(" A = B "), Ok("A=B".to_string()));
        assert_eq!(pair("\u{1f}A\0=B"), Ok("A=B".to_string()));
        for (text, message) in [
            ("in", "Invalid column name \"in\": \"in\" is a reserved keyword"),
            ("", "Invalid column name: name can not be null or empty"),
            ("A=B=C", "Invalid column name \"B=C\""),
            ("A=", "Invalid column name: name can not be null or empty"),
            ("A=B\u{2003}", "Invalid column name \"B\u{2003}\""),
        ] {
            assert_eq!(pair(text), Err(argument(message)), "{text:?}");
        }
    }

    #[test]
    fn a_formula_without_a_token_is_a_column_assignment() {
        // `RollingFormulaSpec.of` of the 42.9 jar
        assert_eq!(check_selectable(" X = sum(x)"), Ok(()));
        assert_eq!(check_selectable("X"), Ok(()));
        for formula in ["a=", "a==b"] {
            assert_eq!(
                check_selectable(formula),
                Err(argument(format!(
                    "Unable to parse formula '{formula}', expected form \
                     '<newColumn>=<expression>'"
                )))
            );
        }
        assert_eq!(
            check_selectable("1a=b+1"),
            Err(argument("Invalid column name \"1a\""))
        );
        assert_eq!(
            check_selectable("sum(x)"),
            Err(argument("Invalid column name \"sum(x)\""))
        );
    }
}
