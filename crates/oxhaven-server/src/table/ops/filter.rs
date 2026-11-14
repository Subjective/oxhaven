//! `Filter`: the rows that satisfy every structured condition.
//!
//! The conditions arrive as protocol messages and the engine has its own type for them;
//! this file is the translation. A message with an unset part is refused as Java refuses
//! it, with an exception while the conditions are read, which the client sees as an
//! internal error.

use super::prelude::*;
use oxhaven_engine::ops::filter as engine;
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::condition::Data;
use oxhaven_proto::grpc::FilterTableRequest;

/// Behavior: `io.deephaven.server.table.ops.FilterTableGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = FilterTableRequest;

    fn result_id(request: &FilterTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &FilterTableRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    fn create(
        context: &OpContext,
        request: &FilterTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let conditions = request
            .filters
            .iter()
            .map(condition)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExportError::Internal)?;
        Ok(engine::filter(
            &sources[0],
            &conditions,
            context.compiler.as_ref(),
        )?)
    }
}

/// The name of a column reference. A reference that is not set names the empty string, as
/// a message that is not set reads in Java.
fn column(reference: &Option<proto::Reference>) -> String {
    reference
        .as_ref()
        .map(|reference| reference.column_name.clone())
        .unwrap_or_default()
}

/// Behavior: `io.deephaven.server.table.ops.filter.FilterPrinter#onValue` and `#onLiteral`,
/// which refuse a value or literal with nothing set
fn value(value: &proto::Value) -> Result<engine::Value, String> {
    use proto::literal::Value as Lit;
    use proto::value::Data as ValueData;
    match &value.data {
        Some(ValueData::Reference(reference)) => {
            Ok(engine::Value::Reference(reference.column_name.clone()))
        }
        Some(ValueData::Literal(literal)) => Ok(engine::Value::Literal(match &literal.value {
            Some(Lit::StringValue(text)) => engine::Literal::String(text.clone()),
            Some(Lit::DoubleValue(number)) => engine::Literal::Double(*number),
            Some(Lit::BoolValue(flag)) => engine::Literal::Bool(*flag),
            Some(Lit::LongValue(number)) => engine::Literal::Long(*number),
            Some(Lit::NanoTimeValue(nanos)) => engine::Literal::NanoTime(*nanos),
            None => return Err("a literal with no value set".to_string()),
        })),
        None => Err("a value that is neither a reference nor a literal".to_string()),
    }
}

/// A value that is not set reads in Java as the default message, which has nothing set.
fn required_value(field: &Option<proto::Value>) -> Result<engine::Value, String> {
    match field {
        Some(set) => value(set),
        None => Err("a value that is neither a reference nor a literal".to_string()),
    }
}

fn case_sensitivity(code: i32) -> Result<engine::CaseSensitivity, String> {
    match proto::CaseSensitivity::try_from(code) {
        Ok(proto::CaseSensitivity::MatchCase) => Ok(engine::CaseSensitivity::MatchCase),
        Ok(proto::CaseSensitivity::IgnoreCase) => Ok(engine::CaseSensitivity::IgnoreCase),
        Err(_) => Err(format!("unrecognized case sensitivity {code}")),
    }
}

fn match_type(code: i32) -> Result<engine::MatchType, String> {
    match proto::MatchType::try_from(code) {
        Ok(proto::MatchType::Regular) => Ok(engine::MatchType::Regular),
        Ok(proto::MatchType::Inverted) => Ok(engine::MatchType::Inverted),
        Err(_) => Err(format!("unrecognized match type {code}")),
    }
}

fn nan_comparison(code: i32) -> Result<engine::NanComparison, String> {
    match proto::NanComparison::try_from(code) {
        Ok(proto::NanComparison::TypeNotSpecified)
        | Ok(proto::NanComparison::NanNotEqualsNan) => Ok(engine::NanComparison::NanNotEqualsNan),
        Ok(proto::NanComparison::NanEqualsNan) => Ok(engine::NanComparison::NanEqualsNan),
        Err(_) => Err(format!("unrecognized NaN comparison {code}")),
    }
}

fn compare_op(code: i32) -> Result<engine::CompareOp, String> {
    use proto::compare_condition::CompareOperation as Operation;
    match Operation::try_from(code) {
        Ok(Operation::LessThan) => Ok(engine::CompareOp::LessThan),
        Ok(Operation::LessThanOrEqual) => Ok(engine::CompareOp::LessThanOrEqual),
        Ok(Operation::GreaterThan) => Ok(engine::CompareOp::GreaterThan),
        Ok(Operation::GreaterThanOrEqual) => Ok(engine::CompareOp::GreaterThanOrEqual),
        Ok(Operation::Equals) => Ok(engine::CompareOp::Equals),
        Ok(Operation::NotEquals) => Ok(engine::CompareOp::NotEquals),
        Err(_) => Err(format!("unrecognized comparison {code}")),
    }
}

/// Behavior: `io.deephaven.server.table.ops.filter.FilterVisitor#accept`
fn condition(condition: &proto::Condition) -> Result<engine::Condition, String> {
    let all = |filters: &[proto::Condition]| {
        filters
            .iter()
            .map(self::condition)
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(match &condition.data {
        Some(Data::And(and)) => engine::Condition::And(all(&and.filters)?),
        Some(Data::Or(or)) => engine::Condition::Or(all(&or.filters)?),
        Some(Data::Not(not)) => match &not.filter {
            Some(inner) => engine::Condition::Not(Box::new(self::condition(inner)?)),
            None => return Err("Unsupported condition: a NOT of nothing".to_string()),
        },
        Some(Data::Compare(compare)) => engine::Condition::Compare {
            op: compare_op(compare.operation)?,
            case_sensitivity: case_sensitivity(compare.case_sensitivity)?,
            lhs: required_value(&compare.lhs)?,
            rhs: required_value(&compare.rhs)?,
        },
        Some(Data::In(within)) => engine::Condition::In {
            target: required_value(&within.target)?,
            candidates: within
                .candidates
                .iter()
                .map(value)
                .collect::<Result<_, _>>()?,
            case_sensitivity: case_sensitivity(within.case_sensitivity)?,
            match_type: match_type(within.match_type)?,
            nan_comparison: nan_comparison(within.nan_comparison)?,
        },
        Some(Data::Invoke(invoke)) => engine::Condition::Invoke {
            method: invoke.method.clone(),
            target: invoke.target.as_ref().map(value).transpose()?,
            arguments: invoke
                .arguments
                .iter()
                .map(value)
                .collect::<Result<_, _>>()?,
        },
        Some(Data::IsNull(is_null)) => engine::Condition::IsNull {
            column: column(&is_null.reference),
        },
        Some(Data::Matches(matches)) => engine::Condition::Matches {
            column: column(&matches.reference),
            regex: matches.regex.clone(),
            case_sensitivity: case_sensitivity(matches.case_sensitivity)?,
            match_type: match_type(matches.match_type)?,
        },
        Some(Data::Contains(contains)) => engine::Condition::Contains {
            column: column(&contains.reference),
            search_string: contains.search_string.clone(),
            case_sensitivity: case_sensitivity(contains.case_sensitivity)?,
            match_type: match_type(contains.match_type)?,
        },
        Some(Data::Search(search)) => engine::Condition::Search {
            search_string: search.search_string.clone(),
            columns: search
                .optional_references
                .iter()
                .map(|reference| reference.column_name.clone())
                .collect(),
        },
        // The reference server's visitor has no case for `is_nan` and refuses it with the
        // conditions that have nothing set.
        Some(Data::IsNan(_)) | None => return Err("Unsupported condition".to_string()),
    })
}
