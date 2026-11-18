//! Structured filter conditions: the typed form of the gRPC `filter` operation's
//! `Condition` message, the passes that normalize it, and its translation to filters.

use super::factory::parse_filter;
use super::filters::{FilterSpec, MatchOptions, PatternSpec, RangeOp};
use oxhaven_contracts::unported;

/// Behavior: the `Literal` message of `table.proto`
#[derive(Clone, PartialEq, Debug)]
pub enum Literal {
    String(String),
    Double(f64),
    Bool(bool),
    Long(i64),
    /// Nanoseconds since the epoch.
    NanoTime(i64),
}

/// Behavior: the `Value` message of `table.proto`
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    /// A column, by name.
    Reference(String),
    Literal(Literal),
}

impl Value {
    fn is_literal(&self) -> bool {
        matches!(self, Value::Literal(_))
    }
}

/// Behavior: `CompareCondition.CompareOperation` of `table.proto`
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompareOp {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equals,
    NotEquals,
}

/// Behavior: `CaseSensitivity` of `table.proto`
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum CaseSensitivity {
    #[default]
    MatchCase,
    IgnoreCase,
}

/// Behavior: `MatchType` of `table.proto`
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum MatchType {
    #[default]
    Regular,
    Inverted,
}

impl MatchType {
    fn inverted(self) -> Self {
        match self {
            MatchType::Regular => MatchType::Inverted,
            MatchType::Inverted => MatchType::Regular,
        }
    }
}

/// Behavior: `NanComparison` of `table.proto`. An unspecified comparison is
/// `NanNotEqualsNan`.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum NanComparison {
    #[default]
    NanNotEqualsNan,
    NanEqualsNan,
}

/// A structured filter condition.
///
/// Behavior: the `Condition` message of `table.proto`
#[derive(Clone, PartialEq, Debug)]
pub enum Condition {
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
    Compare {
        op: CompareOp,
        case_sensitivity: CaseSensitivity,
        lhs: Value,
        rhs: Value,
    },
    In {
        target: Value,
        candidates: Vec<Value>,
        case_sensitivity: CaseSensitivity,
        match_type: MatchType,
        nan_comparison: NanComparison,
    },
    Invoke {
        method: String,
        target: Option<Value>,
        arguments: Vec<Value>,
    },
    IsNull {
        column: String,
    },
    Matches {
        column: String,
        regex: String,
        case_sensitivity: CaseSensitivity,
        match_type: MatchType,
    },
    Contains {
        column: String,
        search_string: String,
        case_sensitivity: CaseSensitivity,
        match_type: MatchType,
    },
    Search {
        search_string: String,
        columns: Vec<String>,
    },
}

/// Applies `pass` to the conditions directly inside an `And`, `Or` or `Not`; any other
/// condition is returned as it is.
///
/// Behavior: io.deephaven.server.table.ops.filter.AbstractNormalizeFilters
fn descend(
    condition: &Condition,
    pass: &impl Fn(&Condition) -> Result<Condition, String>,
) -> Result<Condition, String> {
    let all = |filters: &[Condition]| filters.iter().map(pass).collect::<Result<Vec<_>, _>>();
    Ok(match condition {
        Condition::And(filters) => Condition::And(all(filters)?),
        Condition::Or(filters) => Condition::Or(all(filters)?),
        Condition::Not(filter) => Condition::Not(Box::new(pass(filter)?)),
        other => other.clone(),
    })
}

/// Rewrites `NOT` away wherever the condition under it has an inverse.
///
/// Behavior: io.deephaven.server.table.ops.filter.NormalizeNots
fn normalize_nots(condition: &Condition) -> Result<Condition, String> {
    match condition {
        Condition::Not(filter) => invert(filter),
        other => descend(other, &normalize_nots),
    }
}

/// The normalized inverse of `condition`.
///
/// Behavior: io.deephaven.server.table.ops.filter.NormalizeNots.NormalizeInvertedFilters
fn invert(condition: &Condition) -> Result<Condition, String> {
    let all = |filters: &[Condition]| filters.iter().map(invert).collect::<Result<Vec<_>, _>>();
    Ok(match condition {
        Condition::And(filters) => Condition::Or(all(filters)?),
        Condition::Or(filters) => Condition::And(all(filters)?),
        Condition::Not(filter) => normalize_nots(filter)?,
        Condition::Compare {
            op,
            case_sensitivity,
            lhs,
            rhs,
        } => Condition::Compare {
            op: match op {
                CompareOp::LessThan => CompareOp::GreaterThanOrEqual,
                CompareOp::LessThanOrEqual => CompareOp::GreaterThan,
                CompareOp::GreaterThan => CompareOp::LessThanOrEqual,
                CompareOp::GreaterThanOrEqual => CompareOp::LessThan,
                CompareOp::Equals => CompareOp::NotEquals,
                CompareOp::NotEquals => CompareOp::Equals,
            },
            case_sensitivity: *case_sensitivity,
            lhs: lhs.clone(),
            rhs: rhs.clone(),
        },
        Condition::In {
            target,
            candidates,
            case_sensitivity,
            match_type,
            nan_comparison,
        } => Condition::In {
            target: target.clone(),
            candidates: candidates.clone(),
            case_sensitivity: *case_sensitivity,
            match_type: match_type.inverted(),
            nan_comparison: *nan_comparison,
        },
        Condition::Contains {
            column,
            search_string,
            case_sensitivity,
            match_type,
        } => Condition::Contains {
            column: column.clone(),
            search_string: search_string.clone(),
            case_sensitivity: *case_sensitivity,
            match_type: match_type.inverted(),
        },
        Condition::Matches {
            column,
            regex,
            case_sensitivity,
            match_type,
        } => Condition::Matches {
            column: column.clone(),
            regex: regex.clone(),
            case_sensitivity: *case_sensitivity,
            match_type: match_type.inverted(),
        },
        Condition::IsNull { .. } | Condition::Invoke { .. } => {
            Condition::Not(Box::new(condition.clone()))
        }
        Condition::Search { .. } => return Err("Cannot not() a search".to_string()),
    })
}

/// Turns an `in` whose target is a literal, or with a column among its candidates, around:
/// each candidate becomes the target of its own `in` with the old target as its one
/// candidate.
///
/// Behavior: io.deephaven.server.table.ops.filter.FlipNonReferenceMatchExpression
fn flip_non_reference_matches(condition: &Condition) -> Result<Condition, String> {
    let Condition::In {
        target,
        candidates,
        case_sensitivity,
        match_type,
        nan_comparison,
    } = condition
    else {
        return descend(condition, &flip_non_reference_matches);
    };
    if !target.is_literal() && candidates.iter().all(Value::is_literal) {
        return Ok(condition.clone());
    }
    let mut flipped: Vec<Condition> = candidates
        .iter()
        .map(|candidate| Condition::In {
            target: candidate.clone(),
            candidates: vec![target.clone()],
            case_sensitivity: *case_sensitivity,
            match_type: *match_type,
            nan_comparison: *nan_comparison,
        })
        .collect();
    Ok(if flipped.len() == 1 {
        flipped.remove(0)
    } else {
        match match_type {
            MatchType::Regular => Condition::Or(flipped),
            MatchType::Inverted => Condition::And(flipped),
        }
    })
}

/// Merges an `And` directly inside an `And` into it, and likewise for `Or`.
///
/// Behavior: io.deephaven.server.table.ops.filter.MergeNestedBinaryOperations
fn merge_nested(condition: &Condition) -> Result<Condition, String> {
    fn collect<'a>(
        filters: &'a [Condition],
        children: fn(&Condition) -> Option<&[Condition]>,
        out: &mut Vec<&'a Condition>,
    ) {
        for filter in filters {
            match children(filter) {
                Some(inner) => collect(inner, children, out),
                None => out.push(filter),
            }
        }
    }
    let merged = |filters: &[Condition], children| {
        let mut flat = Vec::new();
        collect(filters, children, &mut flat);
        flat.into_iter()
            .map(merge_nested)
            .collect::<Result<Vec<_>, _>>()
    };
    match condition {
        Condition::And(filters) => Ok(Condition::And(merged(filters, |c| match c {
            Condition::And(inner) => Some(inner),
            _ => None,
        })?)),
        Condition::Or(filters) => Ok(Condition::Or(merged(filters, |c| match c {
            Condition::Or(inner) => Some(inner),
            _ => None,
        })?)),
        other => descend(other, &merge_nested),
    }
}

/// An `in` with one candidate of the same kind as its target (both columns or both
/// literals) becomes an equality.
///
/// Behavior: io.deephaven.server.table.ops.filter.ConvertInvalidInExpressions
fn convert_invalid_ins(condition: &Condition) -> Result<Condition, String> {
    match condition {
        Condition::In {
            target,
            candidates,
            case_sensitivity,
            match_type,
            ..
        } => match candidates.as_slice() {
            [only] if only.is_literal() == target.is_literal() => Ok(Condition::Compare {
                op: match match_type {
                    MatchType::Regular => CompareOp::Equals,
                    MatchType::Inverted => CompareOp::NotEquals,
                },
                case_sensitivity: *case_sensitivity,
                lhs: target.clone(),
                rhs: only.clone(),
            }),
            _ => Ok(condition.clone()),
        },
        other => descend(other, &convert_invalid_ins),
    }
}

/// A comparison that ignores case becomes a call of `equalsIgnoreCase`, guarded against a
/// null column on the left. The comparison's operator is not consulted.
///
/// Behavior: io.deephaven.server.table.ops.filter.MakeExpressionsNullSafe
fn make_null_safe(condition: &Condition) -> Result<Condition, String> {
    let Condition::Compare {
        case_sensitivity: CaseSensitivity::IgnoreCase,
        lhs,
        rhs,
        ..
    } = condition
    else {
        return descend(condition, &make_null_safe);
    };
    let equals_ignore_case = Condition::Invoke {
        method: "equalsIgnoreCase".to_string(),
        target: Some(lhs.clone()),
        arguments: vec![rhs.clone()],
    };
    let Value::Reference(left) = lhs else {
        return Ok(equals_ignore_case);
    };
    let is_null = |column: &String| Condition::IsNull {
        column: column.clone(),
    };
    let left_not_null = Condition::And(vec![
        Condition::Not(Box::new(is_null(left))),
        equals_ignore_case,
    ]);
    Ok(match rhs {
        Value::Literal(_) => left_not_null,
        Value::Reference(right) => Condition::Or(vec![
            Condition::And(vec![is_null(left), is_null(right)]),
            left_not_null,
        ]),
    })
}

/// The one condition a `filter` request's conditions stand for: their conjunction, after
/// the normalizing passes. `None` if there are no conditions.
///
/// Behavior: io.deephaven.server.table.ops.FilterTableGrpcImpl#finishConditions
pub fn normalize(filters: &[Condition]) -> Result<Option<Condition>, String> {
    let filter = match filters {
        [] => return Ok(None),
        [only] => only.clone(),
        several => Condition::And(several.to_vec()),
    };
    let filter = normalize_nots(&filter)?;
    let filter = flip_non_reference_matches(&filter)?;
    let filter = merge_nested(&filter)?;
    let filter = convert_invalid_ins(&filter)?;
    make_null_safe(&filter).map(Some)
}

/// Behavior: org.apache.commons.text.StringEscapeUtils#escapeJava
fn escape_java(text: &str) -> String {
    let mut out = String::new();
    for unit in text.encode_utf16() {
        match unit {
            0x22 => out.push_str("\\\""),
            0x5C => out.push_str("\\\\"),
            0x08 => out.push_str("\\b"),
            0x0A => out.push_str("\\n"),
            0x09 => out.push_str("\\t"),
            0x0C => out.push_str("\\f"),
            0x0D => out.push_str("\\r"),
            32..=0x7F => out.push(char::from(unit as u8)),
            _ => out.push_str(&format!("\\u{unit:04X}")),
        }
    }
    out
}

/// The significant decimal digits of a finite positive `value`, shortest that read back as
/// `value`, and the power of ten of the first digit.
fn shortest_digits(value: f64) -> (Vec<u8>, i32) {
    let text = format!("{value:e}");
    let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
    let digits = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    (digits, exponent.parse().unwrap_or(0))
}

/// For a positive `value` below `1e-300` whose shortest decimal has one digit, at
/// `10^exponent`: the decimal of at most two significant digits closest to it, and the
/// power of ten of its first digit. Java's `Double.toString` chooses among those, since
/// JDK 19, when one digit is enough; above `1e-300` a double's precision leaves only the
/// one-digit decimal itself among them. No two of them are equally close: half way
/// between two is a decimal no double below `1e-300` equals.
///
/// Behavior: java.lang.Double#toString(double), as specified since JDK 19
fn closest_of_two_digits(value: f64, exponent: i32) -> (Vec<u8>, i32) {
    let bits = value.to_bits();
    let (biased, fraction) = ((bits >> 52) as i64, bits & ((1 << 52) - 1));
    // value = significand * 2^-shift
    let (significand, shift) = match biased {
        0 => (fraction, 1074),
        _ => (fraction | 1 << 52, 1075 - biased),
    };
    // floor(value * 10^power) and whether the fraction is above a half, from
    // significand * 10^power held in 64-bit limbs, least significant first
    let scaled = |power: i32| -> (u64, bool) {
        let mut limbs = vec![significand];
        for _ in 0..power {
            let mut carry = 0u128;
            for limb in limbs.iter_mut() {
                let product = u128::from(*limb) * 10 + carry;
                *limb = product as u64;
                carry = product >> 64;
            }
            if carry > 0 {
                limbs.push(carry as u64);
            }
        }
        let bit = |at: i64| (limbs[(at / 64) as usize] >> (at % 64)) & 1;
        let whole = (shift..shift + 8)
            .filter(|at| (at / 64) < limbs.len() as i64)
            .map(|at| bit(at) << (at - shift))
            .sum::<u64>();
        (whole, bit(shift - 1) == 1)
    };
    // the power of ten of the value's first digit, one below the decimal's when that was
    // rounded up to a power of ten
    let mut first = exponent;
    let (mut whole, mut above_half) = scaled(1 - first);
    if whole < 10 {
        first -= 1;
        (whole, above_half) = scaled(1 - first);
    }
    let two = whole + u64::from(above_half);
    if two == 100 {
        return (vec![b'1'], first + 1);
    }
    let mut digits = vec![b'0' + (two / 10) as u8, b'0' + (two % 10) as u8];
    if digits[1] == b'0' {
        digits.pop();
    }
    (digits, first)
}

/// The digits written without an exponent: `12.5`, `0.001`, `1200`.
fn plain(digits: &[u8], exponent: i32) -> String {
    let mut out = String::new();
    if exponent < 0 {
        out.push_str("0.");
        out.extend(std::iter::repeat_n('0', (-exponent - 1) as usize));
        out.extend(digits.iter().map(|d| char::from(*d)));
        return out;
    }
    let whole = exponent as usize + 1;
    for at in 0..whole.max(digits.len()) {
        if at == whole {
            out.push('.');
        }
        out.push(digits.get(at).map_or('0', |d| char::from(*d)));
    }
    out
}

/// Behavior: java.lang.Double#toString(double), as specified since JDK 19.
pub(crate) fn java_double_to_string(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let size = value.abs();
    if size.is_infinite() {
        return format!("{sign}Infinity");
    }
    if size == 0.0 {
        return format!("{sign}0.0");
    }
    let (mut digits, mut exponent) = shortest_digits(size);
    if digits.len() == 1 && size < 1e-300 {
        (digits, exponent) = closest_of_two_digits(size, exponent);
    }
    if (1e-3..1e7).contains(&size) {
        let mut text = plain(&digits, exponent);
        if !text.contains('.') {
            text.push_str(".0");
        }
        return format!("{sign}{text}");
    }
    let first = char::from(digits[0]);
    let rest: String = match &digits[1..] {
        [] => "0".to_string(),
        rest => rest.iter().map(|d| char::from(*d)).collect(),
    };
    format!("{sign}{first}.{rest}E{exponent}")
}

/// A double as a `Literal` prints: a whole number without a fraction, anything else as
/// Java's `Double.toString`.
///
/// Behavior: io.deephaven.server.table.ops.filter.FilterPrinter#onLiteral
fn print_double(value: f64) -> String {
    if value == f64::NEG_INFINITY {
        "Double.NEGATIVE_INFINITY".to_string()
    } else if value == f64::INFINITY {
        "Double.POSITIVE_INFINITY".to_string()
    } else if value.is_nan() {
        "Double.NaN".to_string()
    } else {
        // Java's `(long) value` saturates, as `as` does.
        let whole = value as i64;
        if whole as f64 - value != 0.0 {
            java_double_to_string(value)
        } else {
            whole.to_string()
        }
    }
}

/// A double as `new DecimalFormat("##0.################")` formats it, or `None` where
/// that needs digits this port does not produce: from 2^53 up Java's formatter writes more
/// digits than the shortest, and a value exactly half way between two results of sixteen
/// fraction digits is rounded by rules that depend on the binary value.
///
/// Behavior: java.text.DecimalFormat#format(double) as called by
/// io.deephaven.server.table.ops.filter.FilterFactory#generateNumericConditionFilter
fn decimal_format(value: f64) -> Option<String> {
    const FRACTION_DIGITS: i32 = 16;
    if value.is_nan() {
        return Some("NaN".to_string());
    }
    let sign = if value.is_sign_negative() { "-" } else { "" };
    let size = value.abs();
    if size.is_infinite() {
        return Some(format!("{sign}\u{221E}"));
    }
    if size == 0.0 {
        return Some(format!("{sign}0"));
    }
    if size >= 9_007_199_254_740_992.0 {
        return None;
    }
    let (mut digits, mut exponent) = shortest_digits(size);
    // How many of the digits are at or above the sixteenth fraction place.
    let kept = exponent + 1 + FRACTION_DIGITS;
    if kept < 0 {
        return Some(format!("{sign}0"));
    }
    let kept = kept as usize;
    if kept < digits.len() {
        let next = digits[kept];
        let more = kept + 1 < digits.len();
        if next == b'5' && !more {
            return None;
        }
        digits.truncate(kept);
        if next > b'5' || (next == b'5' && more) {
            let mut at = digits.len();
            loop {
                if at == 0 {
                    digits.insert(0, b'1');
                    exponent += 1;
                    break;
                }
                at -= 1;
                if digits[at] == b'9' {
                    digits[at] = b'0';
                } else {
                    digits[at] += 1;
                    break;
                }
            }
        }
        while digits.last() == Some(&b'0') {
            digits.pop();
        }
        if digits.is_empty() {
            return Some(format!("{sign}0"));
        }
    }
    Some(format!("{sign}{}", plain(&digits, exponent)))
}

fn print_literal(literal: &Literal, escape: bool, out: &mut String) {
    match literal {
        Literal::String(text) if escape => {
            out.push('"');
            out.push_str(&escape_java(text));
            out.push('"');
        }
        Literal::String(text) => {
            out.push('"');
            out.push_str(text);
            out.push('"');
        }
        Literal::Double(value) => out.push_str(&print_double(*value)),
        Literal::Bool(value) => out.push_str(&value.to_string()),
        Literal::Long(value) => out.push_str(&value.to_string()),
        Literal::NanoTime(value) => out.push_str(&format!("'{value}'")),
    }
}

fn print_value(value: &Value, out: &mut String) {
    match value {
        Value::Reference(column) => out.push_str(column),
        Value::Literal(literal) => print_literal(literal, true, out),
    }
}

fn print_into(condition: &Condition, out: &mut String) {
    let joined = |filters: &[Condition], operator: &str, out: &mut String| match filters {
        [] => {}
        [only] => print_into(only, out),
        [first, rest @ ..] => {
            out.push('(');
            print_into(first, out);
            for filter in rest {
                out.push_str(operator);
                print_into(filter, out);
            }
            out.push(')');
        }
    };
    let pattern = |name: &str,
                   column: &str,
                   text: &str,
                   case_sensitivity: CaseSensitivity,
                   match_type: MatchType,
                   out: &mut String| {
        if match_type == MatchType::Inverted {
            out.push('!');
        }
        out.push_str(name);
        if case_sensitivity == CaseSensitivity::IgnoreCase {
            out.push_str("IgnoreCase");
        }
        out.push('(');
        out.push_str(column);
        out.push_str(",\"");
        out.push_str(&escape_java(text));
        out.push_str("\")");
    };
    match condition {
        Condition::And(filters) => joined(filters, " && ", out),
        Condition::Or(filters) => joined(filters, " || ", out),
        Condition::Not(filter) => {
            out.push_str("!(");
            print_into(filter, out);
            out.push(')');
        }
        Condition::Compare {
            op,
            case_sensitivity,
            lhs,
            rhs,
        } => {
            print_value(lhs, out);
            out.push_str(match op {
                CompareOp::LessThan => " < ",
                CompareOp::LessThanOrEqual => " <= ",
                CompareOp::GreaterThan => " > ",
                CompareOp::GreaterThanOrEqual => " >= ",
                CompareOp::Equals => " == ",
                CompareOp::NotEquals => " != ",
            });
            if matches!(op, CompareOp::Equals | CompareOp::NotEquals)
                && *case_sensitivity == CaseSensitivity::IgnoreCase
            {
                out.push_str("(ignore case)");
            }
            print_value(rhs, out);
        }
        Condition::In {
            target,
            candidates,
            case_sensitivity,
            match_type,
            nan_comparison,
        } => {
            let Some((first, rest)) = candidates.split_first() else {
                return;
            };
            print_value(target, out);
            if *case_sensitivity == CaseSensitivity::IgnoreCase {
                out.push_str(" icase");
            }
            if *match_type == MatchType::Inverted {
                out.push_str(" not");
            }
            out.push_str(" in ");
            print_value(first, out);
            for candidate in rest {
                out.push_str(", ");
                print_value(candidate, out);
            }
            if *nan_comparison == NanComparison::NanEqualsNan {
                out.push_str(" (nans equal)");
            }
        }
        Condition::IsNull { column } => {
            out.push_str("isNull(");
            out.push_str(column);
            out.push(')');
        }
        Condition::Invoke {
            method,
            target,
            arguments,
        } => {
            if let Some(target) = target {
                print_value(target, out);
                out.push('.');
            }
            out.push_str(method);
            out.push('(');
            for (at, argument) in arguments.iter().enumerate() {
                if at != 0 {
                    out.push(',');
                }
                print_value(argument, out);
            }
            out.push(')');
        }
        Condition::Contains {
            column,
            search_string,
            case_sensitivity,
            match_type,
        } => pattern(
            "contains",
            column,
            search_string,
            *case_sensitivity,
            *match_type,
            out,
        ),
        Condition::Matches {
            column,
            regex,
            case_sensitivity,
            match_type,
        } => pattern(
            "matches",
            column,
            regex,
            *case_sensitivity,
            *match_type,
            out,
        ),
        Condition::Search {
            search_string,
            columns,
        } => {
            out.push_str("searchTableColumns(\"");
            out.push_str(&escape_java(search_string));
            out.push('"');
            for column in columns {
                out.push(',');
                out.push_str(column);
            }
            out.push(')');
        }
    }
}

/// The condition as formula text. This is the text Java compiles for the conditions it
/// does not turn into a match, range or pattern filter.
///
/// Behavior: io.deephaven.server.table.ops.filter.FilterPrinter#print
pub fn print(condition: &Condition) -> String {
    let mut out = String::new();
    print_into(condition, &mut out);
    out
}

/// Behavior: io.deephaven.server.table.ops.filter.FilterFactory#generateNumericConditionFilter
/// and its `rangeCondition`
fn range(op: CompareOp, lhs: &Value, rhs: &Value) -> Result<FilterSpec, String> {
    let (column, literal, literal_first) = match (lhs, rhs) {
        (Value::Literal(literal), Value::Reference(column)) => (column, literal, true),
        (Value::Reference(column), Value::Literal(literal)) => (column, literal, false),
        // Two columns or two literals: a formula.
        _ => {
            return parse_filter(&print(&Condition::Compare {
                op,
                case_sensitivity: CaseSensitivity::MatchCase,
                lhs: lhs.clone(),
                rhs: rhs.clone(),
            }))
        }
    };
    let value = match literal {
        Literal::String(text) => text.clone(),
        Literal::Double(value) => match decimal_format(*value) {
            Some(text) => text,
            None => unported!("java.text.DecimalFormat#format", "the double {value:?}"),
        },
        Literal::Bool(value) => value.to_string(),
        Literal::Long(value) => value.to_string(),
        Literal::NanoTime(value) => format!("'{value}'"),
    };
    // With the literal on the left Java uses the operator's negation, not its mirror image:
    // `5 < A` selects `A >= 5`.
    let op = match (op, literal_first) {
        (CompareOp::LessThan, false) | (CompareOp::GreaterThanOrEqual, true) => RangeOp::Less,
        (CompareOp::LessThanOrEqual, false) | (CompareOp::GreaterThan, true) => {
            RangeOp::LessOrEqual
        }
        (CompareOp::GreaterThan, false) | (CompareOp::LessThanOrEqual, true) => RangeOp::Greater,
        (CompareOp::GreaterThanOrEqual, false) | (CompareOp::LessThan, true) => {
            RangeOp::GreaterOrEqual
        }
        (CompareOp::Equals | CompareOp::NotEquals, _) => {
            return Err(format!(
                "Can't handle compare operation {op:?} in range operation"
            ))
        }
    };
    Ok(FilterSpec::Range {
        column: column.clone(),
        op,
        value,
        failover: None,
    })
}

/// The filter a normalized condition stands for.
///
/// Behavior: io.deephaven.server.table.ops.filter.FilterFactory#makeFilter
pub(super) fn to_spec(condition: &Condition) -> Result<FilterSpec, String> {
    let all = |filters: &[Condition]| filters.iter().map(to_spec).collect::<Result<Vec<_>, _>>();
    let ignore_case =
        |case_sensitivity: &CaseSensitivity| *case_sensitivity == CaseSensitivity::IgnoreCase;
    match condition {
        Condition::And(filters) => Ok(FilterSpec::And(all(filters)?)),
        Condition::Or(filters) => Ok(FilterSpec::Or(all(filters)?)),
        Condition::Not(_) | Condition::IsNull { .. } | Condition::Invoke { .. } => {
            parse_filter(&print(condition))
        }
        Condition::Compare {
            op: CompareOp::Equals | CompareOp::NotEquals,
            case_sensitivity,
            ..
        } => {
            if ignore_case(case_sensitivity) {
                return Err("Should have been compiled out in a previous pass".to_string());
            }
            parse_filter(&print(condition))
        }
        Condition::Compare { op, lhs, rhs, .. } => range(*op, lhs, rhs),
        Condition::In {
            target,
            candidates,
            case_sensitivity,
            match_type,
            nan_comparison,
        } => {
            let Value::Reference(column) = target else {
                return Err(format!(
                    "The target of an in condition must be a column: {}",
                    print(condition)
                ));
            };
            let values = candidates
                .iter()
                .map(|candidate| match candidate {
                    Value::Literal(Literal::NanoTime(nanos)) => Ok(format!("'{nanos}'")),
                    Value::Literal(literal) => {
                        let mut text = String::new();
                        print_literal(literal, false, &mut text);
                        Ok(text)
                    }
                    Value::Reference(_) => Err(format!(
                        "The candidates of an in condition must be literals: {}",
                        print(condition)
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FilterSpec::Match {
                column: column.clone(),
                values,
                options: MatchOptions {
                    inverted: *match_type == MatchType::Inverted,
                    ignore_case: ignore_case(case_sensitivity),
                    nan_match: *nan_comparison == NanComparison::NanEqualsNan,
                },
                failover: None,
            })
        }
        Condition::Contains {
            column,
            search_string,
            case_sensitivity,
            match_type,
        } => Ok(FilterSpec::Pattern {
            column: column.clone(),
            pattern: PatternSpec::Any(vec![search_string.clone()]),
            ignore_case: ignore_case(case_sensitivity),
            invert_pattern: *match_type == MatchType::Inverted,
        }),
        Condition::Matches {
            column,
            regex,
            case_sensitivity,
            match_type,
        } => Ok(FilterSpec::Pattern {
            column: column.clone(),
            pattern: PatternSpec::Regex(regex.clone()),
            ignore_case: ignore_case(case_sensitivity),
            invert_pattern: *match_type == MatchType::Inverted,
        }),
        Condition::Search {
            search_string,
            columns,
        } => Ok(FilterSpec::Search {
            text: search_string.clone(),
            columns: columns.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str) -> Value {
        Value::Reference(name.to_string())
    }

    fn long(value: i64) -> Value {
        Value::Literal(Literal::Long(value))
    }

    fn string(value: &str) -> Value {
        Value::Literal(Literal::String(value.to_string()))
    }

    fn compare(op: CompareOp, lhs: Value, rhs: Value) -> Condition {
        Condition::Compare {
            op,
            case_sensitivity: CaseSensitivity::MatchCase,
            lhs,
            rhs,
        }
    }

    fn is_in(target: Value, candidates: Vec<Value>, match_type: MatchType) -> Condition {
        Condition::In {
            target,
            candidates,
            case_sensitivity: CaseSensitivity::MatchCase,
            match_type,
            nan_comparison: NanComparison::NanNotEqualsNan,
        }
    }

    fn is_null(name: &str) -> Condition {
        Condition::IsNull {
            column: name.to_string(),
        }
    }

    fn not(condition: Condition) -> Condition {
        Condition::Not(Box::new(condition))
    }

    fn normalized(condition: Condition) -> Condition {
        normalize(&[condition]).unwrap().unwrap()
    }

    #[test]
    fn no_conditions_normalize_to_none_and_several_to_their_conjunction() {
        assert_eq!(normalize(&[]), Ok(None));
        let (a, b) = (is_null("A"), is_null("B"));
        assert_eq!(
            normalize(&[a.clone(), b.clone()]),
            Ok(Some(Condition::And(vec![a, b])))
        );
    }

    #[test]
    fn not_is_pushed_down_to_the_conditions_that_have_an_inverse() {
        use CompareOp::*;
        for (op, inverse) in [
            (LessThan, GreaterThanOrEqual),
            (LessThanOrEqual, GreaterThan),
            (GreaterThan, LessThanOrEqual),
            (GreaterThanOrEqual, LessThan),
            (Equals, NotEquals),
            (NotEquals, Equals),
        ] {
            assert_eq!(
                normalized(not(compare(op, column("A"), long(1)))),
                compare(inverse, column("A"), long(1))
            );
        }
        // De Morgan, and a double negation disappears.
        assert_eq!(
            normalized(not(Condition::And(vec![
                compare(LessThan, column("A"), long(1)),
                not(compare(Equals, column("B"), long(2))),
                Condition::Or(vec![is_null("C"), not(is_null("D"))]),
            ]))),
            Condition::Or(vec![
                compare(GreaterThanOrEqual, column("A"), long(1)),
                compare(Equals, column("B"), long(2)),
                Condition::And(vec![not(is_null("C")), is_null("D")]),
            ])
        );
        assert_eq!(
            normalized(not(is_in(
                column("A"),
                vec![long(1), long(2)],
                MatchType::Regular
            ))),
            is_in(column("A"), vec![long(1), long(2)], MatchType::Inverted)
        );
        let contains = |match_type| Condition::Contains {
            column: "S".to_string(),
            search_string: "x".to_string(),
            case_sensitivity: CaseSensitivity::IgnoreCase,
            match_type,
        };
        assert_eq!(
            normalized(not(contains(MatchType::Regular))),
            contains(MatchType::Inverted)
        );
        let matches = |match_type| Condition::Matches {
            column: "S".to_string(),
            regex: "x.*".to_string(),
            case_sensitivity: CaseSensitivity::MatchCase,
            match_type,
        };
        assert_eq!(
            normalized(not(matches(MatchType::Inverted))),
            matches(MatchType::Regular)
        );
        // No inverse: the NOT stays.
        let invoke = Condition::Invoke {
            method: "isEmpty".to_string(),
            target: Some(column("S")),
            arguments: vec![],
        };
        assert_eq!(normalized(not(invoke.clone())), not(invoke));
        assert_eq!(normalized(not(is_null("A"))), not(is_null("A")));
        let search = Condition::Search {
            search_string: "x".to_string(),
            columns: vec![],
        };
        assert_eq!(
            normalize(&[not(search)]),
            Err("Cannot not() a search".to_string())
        );
    }

    #[test]
    fn an_in_with_a_literal_target_or_a_column_candidate_is_turned_around() {
        // One candidate: turned around. A column and a literal are not of one kind, so it
        // stays an `in`.
        assert_eq!(
            normalized(is_in(long(1), vec![column("A")], MatchType::Regular)),
            is_in(column("A"), vec![long(1)], MatchType::Regular)
        );
        // Several candidates: one `in` each, joined by OR, or by AND when inverted. Two
        // columns, or two literals, then compare as an equality.
        assert_eq!(
            normalized(is_in(
                column("A"),
                vec![column("B"), long(3)],
                MatchType::Regular
            )),
            Condition::Or(vec![
                compare(CompareOp::Equals, column("B"), column("A")),
                is_in(long(3), vec![column("A")], MatchType::Regular),
            ])
        );
        assert_eq!(
            normalized(is_in(
                long(1),
                vec![column("A"), long(2)],
                MatchType::Inverted
            )),
            Condition::And(vec![
                is_in(column("A"), vec![long(1)], MatchType::Inverted),
                compare(CompareOp::NotEquals, long(2), long(1)),
            ])
        );
        // A column target with literal candidates is left alone.
        let plain = is_in(column("A"), vec![long(1), long(2)], MatchType::Regular);
        assert_eq!(normalized(plain.clone()), plain);
        // A literal target without candidates is an empty OR, which accepts every row.
        assert_eq!(
            normalized(is_in(long(1), vec![], MatchType::Regular)),
            Condition::Or(vec![])
        );
    }

    #[test]
    fn nested_ands_and_ors_are_merged() {
        let leaf = |name: &str| is_null(name);
        assert_eq!(
            normalized(Condition::And(vec![
                leaf("A"),
                Condition::And(vec![leaf("B"), Condition::And(vec![leaf("C")])]),
                Condition::Or(vec![
                    leaf("D"),
                    Condition::Or(vec![leaf("E")]),
                    Condition::And(vec![leaf("F"), Condition::And(vec![leaf("G")])]),
                ]),
            ])),
            Condition::And(vec![
                leaf("A"),
                leaf("B"),
                leaf("C"),
                Condition::Or(vec![
                    leaf("D"),
                    leaf("E"),
                    Condition::And(vec![leaf("F"), leaf("G")]),
                ]),
            ])
        );
    }

    #[test]
    fn a_comparison_that_ignores_case_becomes_a_guarded_equals_ignore_case() {
        let ignoring = |op, lhs, rhs| Condition::Compare {
            op,
            case_sensitivity: CaseSensitivity::IgnoreCase,
            lhs,
            rhs,
        };
        let invoke = |lhs: Value, rhs: Value| Condition::Invoke {
            method: "equalsIgnoreCase".to_string(),
            target: Some(lhs),
            arguments: vec![rhs],
        };
        assert_eq!(
            normalized(ignoring(CompareOp::Equals, string("x"), column("S"))),
            invoke(string("x"), column("S"))
        );
        let guarded = Condition::And(vec![not(is_null("S")), invoke(column("S"), string("x"))]);
        assert_eq!(
            normalized(ignoring(CompareOp::Equals, column("S"), string("x"))),
            guarded
        );
        // The operator is not consulted: `!=` gives the same condition as `==`.
        assert_eq!(
            normalized(ignoring(CompareOp::NotEquals, column("S"), string("x"))),
            guarded
        );
        assert_eq!(
            normalized(ignoring(CompareOp::Equals, column("S"), column("T"))),
            Condition::Or(vec![
                Condition::And(vec![is_null("S"), is_null("T")]),
                Condition::And(vec![not(is_null("S")), invoke(column("S"), column("T"))]),
            ])
        );
        // An `in` of one column that ignores case takes the same route.
        assert_eq!(
            normalized(Condition::In {
                target: column("S"),
                candidates: vec![column("T")],
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Regular,
                nan_comparison: NanComparison::NanNotEqualsNan,
            }),
            Condition::Or(vec![
                Condition::And(vec![is_null("T"), is_null("S")]),
                Condition::And(vec![not(is_null("T")), invoke(column("T"), column("S"))]),
            ])
        );
    }

    #[test]
    fn conditions_print_as_java_prints_them() {
        let double = |value| Value::Literal(Literal::Double(value));
        assert_eq!(print(&Condition::And(vec![])), "");
        assert_eq!(print(&Condition::Or(vec![is_null("A")])), "isNull(A)");
        assert_eq!(
            print(&Condition::And(vec![
                is_null("A"),
                Condition::Or(vec![is_null("B"), not(is_null("C"))]),
            ])),
            "(isNull(A) && (isNull(B) || !(isNull(C))))"
        );
        assert_eq!(
            print(&compare(
                CompareOp::LessThanOrEqual,
                column("A"),
                double(1.5)
            )),
            "A <= 1.5"
        );
        assert_eq!(
            print(&Condition::Compare {
                op: CompareOp::NotEquals,
                case_sensitivity: CaseSensitivity::IgnoreCase,
                lhs: column("S"),
                rhs: string("a\"b\\c\n\t\u{1}\u{7f}\u{e9}\u{1F600}"),
            }),
            "S != (ignore case)\"a\\\"b\\\\c\\n\\t\\u0001\u{7f}\\u00E9\\uD83D\\uDE00\""
        );
        assert_eq!(
            print(&Condition::In {
                target: column("A"),
                candidates: vec![
                    long(-3),
                    double(f64::NAN),
                    Value::Literal(Literal::Bool(true)),
                    Value::Literal(Literal::NanoTime(17)),
                ],
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Inverted,
                nan_comparison: NanComparison::NanEqualsNan,
            }),
            "A icase not in -3, Double.NaN, true, '17' (nans equal)"
        );
        assert_eq!(print(&is_in(column("A"), vec![], MatchType::Regular)), "");
        assert_eq!(
            print(&Condition::Invoke {
                method: "f".to_string(),
                target: None,
                arguments: vec![column("A"), long(1)],
            }),
            "f(A,1)"
        );
        assert_eq!(
            print(&Condition::Invoke {
                method: "startsWith".to_string(),
                target: Some(column("S")),
                arguments: vec![string("x")],
            }),
            "S.startsWith(\"x\")"
        );
        assert_eq!(
            print(&Condition::Contains {
                column: "S".to_string(),
                search_string: "a\"".to_string(),
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Inverted,
            }),
            "!containsIgnoreCase(S,\"a\\\"\")"
        );
        assert_eq!(
            print(&Condition::Matches {
                column: "S".to_string(),
                regex: "a.*".to_string(),
                case_sensitivity: CaseSensitivity::MatchCase,
                match_type: MatchType::Regular,
            }),
            "matches(S,\"a.*\")"
        );
        assert_eq!(
            print(&Condition::Search {
                search_string: "q".to_string(),
                columns: vec!["A".to_string(), "B".to_string()],
            }),
            "searchTableColumns(\"q\",A,B)"
        );
    }

    /// `java_doubles.txt` holds, for doubles chosen by a JDK 21 program, the bits, the text
    /// of `FilterPrinter#onLiteral` and the text of the `DecimalFormat` of
    /// `FilterFactory#generateNumericConditionFilter`.
    #[test]
    fn doubles_print_as_the_jdk_prints_them() {
        let unescape = |text: &str| text.replace("\\u221E", "\u{221E}");
        let (mut formatted, mut skipped) = (0, 0);
        for line in include_str!("java_doubles.txt").lines() {
            let fields: Vec<&str> = line.split(' ').collect();
            let value = f64::from_bits(u64::from_str_radix(fields[0], 16).unwrap());
            assert_eq!(print_double(value), fields[1], "{line}");
            match decimal_format(value) {
                Some(text) => {
                    assert_eq!(text, unescape(fields[2]), "{line}");
                    formatted += 1;
                }
                None => skipped += 1,
            }
        }
        assert!(formatted > 300, "{formatted} formatted, {skipped} skipped");
    }

    /// `java_tiny_doubles.txt` holds the bits and JDK 21's `Double.toString` of positive
    /// doubles below `1e-300`: every subnormal up to 300 times the smallest, others at
    /// random, and those next to each decimal of one digit.
    #[test]
    fn doubles_below_1e_300_print_as_the_jdk_prints_them() {
        let mut lines = 0;
        for line in include_str!("java_tiny_doubles.txt").lines() {
            let (bits, text) = line.split_once(' ').unwrap();
            let value = f64::from_bits(u64::from_str_radix(bits, 16).unwrap());
            assert_eq!(java_double_to_string(value), text, "{line}");
            assert_eq!(java_double_to_string(-value), format!("-{text}"), "{line}");
            lines += 1;
        }
        assert!(lines > 800);
    }

    #[test]
    fn a_double_the_formatter_does_not_cover_is_reported() {
        // 2^53 and above, and exact ties at the sixteenth fraction digit.
        assert_eq!(decimal_format(9_007_199_254_740_992.0), None);
        assert_eq!(decimal_format(5e-17), None);
        assert_eq!(decimal_format(0.12345678901234565), None);
        assert_eq!(decimal_format(0.3), Some("0.3".to_string()));
    }

    fn spec(condition: Condition) -> Result<FilterSpec, String> {
        to_spec(&normalized(condition))
    }

    fn range_spec(column: &str, op: RangeOp, value: &str) -> FilterSpec {
        FilterSpec::Range {
            column: column.to_string(),
            op,
            value: value.to_string(),
            failover: None,
        }
    }

    #[test]
    fn an_ordering_of_a_column_and_a_literal_is_a_range_filter() {
        use CompareOp::*;
        for (op, range_op) in [
            (LessThan, RangeOp::Less),
            (LessThanOrEqual, RangeOp::LessOrEqual),
            (GreaterThan, RangeOp::Greater),
            (GreaterThanOrEqual, RangeOp::GreaterOrEqual),
        ] {
            assert_eq!(
                spec(compare(op, column("A"), long(5))),
                Ok(range_spec("A", range_op, "5"))
            );
        }
        // Java's choice for a literal on the left is the negated operator, not the
        // mirrored one: `5 < A` filters as `A >= 5`.
        for (op, range_op) in [
            (LessThan, RangeOp::GreaterOrEqual),
            (LessThanOrEqual, RangeOp::Greater),
            (GreaterThan, RangeOp::LessOrEqual),
            (GreaterThanOrEqual, RangeOp::Less),
        ] {
            assert_eq!(
                spec(compare(op, long(5), column("A"))),
                Ok(range_spec("A", range_op, "5"))
            );
        }
        let literal = |literal| compare(GreaterThan, column("A"), Value::Literal(literal));
        assert_eq!(
            spec(literal(Literal::String("x y".to_string()))),
            Ok(range_spec("A", RangeOp::Greater, "x y"))
        );
        assert_eq!(
            spec(literal(Literal::Double(-2.50))),
            Ok(range_spec("A", RangeOp::Greater, "-2.5"))
        );
        assert_eq!(
            spec(literal(Literal::Double(3.0))),
            Ok(range_spec("A", RangeOp::Greater, "3"))
        );
        assert_eq!(
            spec(literal(Literal::Bool(false))),
            Ok(range_spec("A", RangeOp::Greater, "false"))
        );
        assert_eq!(
            spec(literal(Literal::NanoTime(1_700_000_000_000_000_000))),
            Ok(range_spec("A", RangeOp::Greater, "'1700000000000000000'"))
        );
        // Two columns: the printed text goes through the string routing, which makes a
        // range filter with a formula to fall back on.
        assert_eq!(
            spec(compare(LessThan, column("A"), column("B"))),
            Ok(FilterSpec::Range {
                column: "A".to_string(),
                op: RangeOp::Less,
                value: "B".to_string(),
                failover: Some("A < B".to_string()),
            })
        );
    }

    #[test]
    fn an_equality_goes_through_its_printed_text() {
        assert_eq!(
            spec(compare(CompareOp::Equals, column("A"), long(5))),
            Ok(FilterSpec::Match {
                column: "A".to_string(),
                values: vec!["5".to_string()],
                options: MatchOptions::default(),
                failover: Some("A == 5".to_string()),
            })
        );
        assert_eq!(
            spec(compare(CompareOp::NotEquals, column("S"), string("x"))),
            Ok(FilterSpec::Match {
                column: "S".to_string(),
                values: vec!["\"x\"".to_string()],
                options: MatchOptions {
                    inverted: true,
                    ..MatchOptions::default()
                },
                failover: Some("S != \"x\"".to_string()),
            })
        );
        // A string with a quote in it is no longer a simple literal: a formula.
        assert_eq!(
            spec(compare(CompareOp::Equals, column("S"), string("a\"b"))),
            Ok(FilterSpec::Condition("S == \"a\\\"b\"".to_string()))
        );
        // Not normalized: the case-insensitive comparison must be gone by now.
        assert!(to_spec(&Condition::Compare {
            op: CompareOp::Equals,
            case_sensitivity: CaseSensitivity::IgnoreCase,
            lhs: column("S"),
            rhs: string("x"),
        })
        .is_err());
    }

    #[test]
    fn an_in_is_a_match_filter_with_unescaped_values() {
        assert_eq!(
            spec(Condition::In {
                target: column("S"),
                candidates: vec![
                    string("a\"b"),
                    Value::Literal(Literal::Double(1.0)),
                    Value::Literal(Literal::Double(0.25)),
                    Value::Literal(Literal::NanoTime(5)),
                ],
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Inverted,
                nan_comparison: NanComparison::NanEqualsNan,
            }),
            Ok(FilterSpec::Match {
                column: "S".to_string(),
                values: ["\"a\"b\"", "1", "0.25", "'5'"].map(String::from).to_vec(),
                options: MatchOptions {
                    inverted: true,
                    ignore_case: true,
                    nan_match: true,
                },
                failover: None,
            })
        );
        // What normalization leaves of an `in` over a literal and a column cannot be built.
        assert!(spec(is_in(
            column("A"),
            vec![column("B"), long(3)],
            MatchType::Regular
        ))
        .is_err());
    }

    #[test]
    fn the_other_conditions_become_patterns_formulas_and_combinations() {
        assert_eq!(
            spec(not(is_null("A"))),
            Ok(FilterSpec::Condition("!(isNull(A))".to_string()))
        );
        assert_eq!(
            spec(is_null("A")),
            Ok(FilterSpec::Condition("isNull(A)".to_string()))
        );
        assert_eq!(
            spec(Condition::Contains {
                column: "S".to_string(),
                search_string: "a.b".to_string(),
                case_sensitivity: CaseSensitivity::IgnoreCase,
                match_type: MatchType::Inverted,
            }),
            Ok(FilterSpec::Pattern {
                column: "S".to_string(),
                pattern: PatternSpec::Any(vec!["a.b".to_string()]),
                ignore_case: true,
                invert_pattern: true,
            })
        );
        assert_eq!(
            spec(Condition::Matches {
                column: "S".to_string(),
                regex: "a.b".to_string(),
                case_sensitivity: CaseSensitivity::MatchCase,
                match_type: MatchType::Regular,
            }),
            Ok(FilterSpec::Pattern {
                column: "S".to_string(),
                pattern: PatternSpec::Regex("a.b".to_string()),
                ignore_case: false,
                invert_pattern: false,
            })
        );
        assert_eq!(
            spec(Condition::Or(vec![
                is_null("A"),
                Condition::And(vec![
                    compare(CompareOp::LessThan, column("A"), long(1)),
                    is_null("B"),
                ]),
            ])),
            Ok(FilterSpec::Or(vec![
                FilterSpec::Condition("isNull(A)".to_string()),
                FilterSpec::And(vec![
                    range_spec("A", RangeOp::Less, "1"),
                    FilterSpec::Condition("isNull(B)".to_string()),
                ]),
            ]))
        );
    }

    #[test]
    fn a_search_waits_for_the_table_to_know_its_columns() {
        let spec = to_spec(&Condition::Search {
            search_string: "x".to_string(),
            columns: vec!["A".to_string()],
        });
        assert_eq!(
            spec,
            Ok(FilterSpec::Search {
                text: "x".to_string(),
                columns: vec!["A".to_string()],
            })
        );
    }
}
