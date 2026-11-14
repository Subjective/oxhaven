//! `RangeJoinTables`: the left table's rows, each with the right rows of its bucket whose
//! range value lies between two of its columns, as one vector per grouped right column.
//!
//! The request names the range match either as text (`range_match`) or by its parts
//! (`left_start_column`, `range_start_rule`, `right_range_column`, `range_end_rule`,
//! `left_end_column`), never both. Java checks the shape of the request, then reads the
//! matches, and answers each fault with a status and a message of its own; what the engine
//! refuses afterwards is an exception like any other.

use super::aggregate::{empty_field, validate_aggregation};
use super::prelude::*;
use oxhaven_engine::ops::agg::Pair;
use oxhaven_engine::ops::join::JoinMatch;
use oxhaven_engine::ops::range_join::{
    range_join, RangeAggregation, RangeEndRule, RangeJoinError, RangeJoinMatch, RangeStartRule,
};
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::agg_spec::Type as SpecType;
use oxhaven_proto::grpc::aggregation::Type as AggregationType;
use oxhaven_proto::grpc::range_join_tables_request::{
    RangeEndRule as ProtoEndRule, RangeStartRule as ProtoStartRule,
};
use oxhaven_proto::grpc::RangeJoinTablesRequest;

const REQUEST: &str = "RangeJoinTablesRequest";

/// What Java's range join refuses is an exception out of the engine: refreshing tables,
/// match columns that are missing or of differing types, aggregations other than `group`,
/// right rows out of order.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<RangeJoinError> for ExportError {
    fn from(error: RangeJoinError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

/// A column name as a request gives it: the characters `String#trim` removes are dropped
/// from both ends, and what is left must be able to name a column. The error is the
/// message of Java's `InvalidNameException`.
///
/// Behavior: `io.deephaven.api.ColumnName#parse`
fn column_name(text: &str) -> Result<String, String> {
    let name = java_trim(text);
    validate_column_name(name)?;
    Ok(name.to_string())
}

/// `Left`, `Left=Right` or `Left==Right`. The left name ends at the first `=`; everything
/// after the one or two `=` is the right name, so `A=B=C` is refused for the name `B=C`.
/// The error is the message of Java's `IllegalArgumentException`.
///
/// Behavior: `io.deephaven.api.JoinMatch#parse`
fn exact_match(text: &str) -> Result<JoinMatch, String> {
    let unparsable = || {
        format!(
            "Unable to parse match '{text}', expected form '<left>==<right>' or \
             `<left>=<right>`"
        )
    };
    let Some((left, right)) = text.split_once('=') else {
        return Ok(JoinMatch::same(column_name(text)?));
    };
    if right.is_empty() {
        return Err(unparsable());
    }
    let right = right.strip_prefix('=').unwrap_or(right);
    if right.is_empty() {
        return Err(unparsable());
    }
    Ok(JoinMatch::new(column_name(left)?, column_name(right)?))
}

/// Whether the request names its range match as text. A string field of the protocol has
/// no presence of its own: it is set when it is not empty.
///
/// Behavior: `RangeJoinGrpcImpl#hasRangeMatchString`
fn has_range_match_text(request: &RangeJoinTablesRequest) -> bool {
    !request.range_match.is_empty()
}

/// The five fields that name the range match by its parts, in the order Java looks at
/// them: the name, the number, and whether the request has the field.
fn detail_fields(request: &RangeJoinTablesRequest) -> [(&'static str, u32, bool); 5] {
    [
        (
            "left_start_column",
            5,
            !request.left_start_column.is_empty(),
        ),
        ("range_start_rule", 6, request.range_start_rule != 0),
        (
            "right_range_column",
            7,
            !request.right_range_column.is_empty(),
        ),
        ("range_end_rule", 8, request.range_end_rule != 0),
        ("left_end_column", 9, !request.left_end_column.is_empty()),
    ]
}

/// The range match of a request, from its text or from its parts. A rule the protocol does
/// not define is refused after the start column is read and before the range column is.
///
/// Behavior: `RangeJoinGrpcImpl#adaptRangeMatch` and its two `adapt`,
/// `io.deephaven.api.RangeJoinMatch#parse`, `io.deephaven.api.RangeJoinMatch#of`
fn range_match(request: &RangeJoinTablesRequest) -> Result<RangeJoinMatch, String> {
    if has_range_match_text(request) {
        let checked = RangeJoinMatch::parse_checked(&request.range_match, validate_column_name);
        return checked.map_err(|error| error.to_string());
    }
    let left_start = column_name(&request.left_start_column)?;
    let start_rule = match ProtoStartRule::try_from(request.range_start_rule) {
        Ok(ProtoStartRule::LessThan) => RangeStartRule::LessThan,
        Ok(ProtoStartRule::LessThanOrEqual) => RangeStartRule::LessThanOrEqual,
        Ok(ProtoStartRule::LessThanOrEqualAllowPreceding) => {
            RangeStartRule::LessThanOrEqualAllowPreceding
        }
        Ok(rule @ ProtoStartRule::StartUnspecified) => {
            let rule = rule.as_str_name();
            return Err(format!(
                "Unrecognized range start rule {rule} for range join"
            ));
        }
        Err(_) => {
            return Err("Unrecognized range start rule UNRECOGNIZED for range join".to_string())
        }
    };
    let right_range = column_name(&request.right_range_column)?;
    let end_rule = match ProtoEndRule::try_from(request.range_end_rule) {
        Ok(ProtoEndRule::GreaterThan) => RangeEndRule::GreaterThan,
        Ok(ProtoEndRule::GreaterThanOrEqual) => RangeEndRule::GreaterThanOrEqual,
        Ok(ProtoEndRule::GreaterThanOrEqualAllowFollowing) => {
            RangeEndRule::GreaterThanOrEqualAllowFollowing
        }
        Ok(rule @ ProtoEndRule::EndUnspecified) => {
            let rule = rule.as_str_name();
            return Err(format!("Unrecognized range end rule {rule} for range join"));
        }
        Err(_) => {
            return Err("Unrecognized range end rule UNRECOGNIZED for range join".to_string())
        }
    };
    let left_end = column_name(&request.left_end_column)?;
    RangeJoinMatch::new(left_start, start_rule, right_range, end_rule, left_end)
        .map_err(|error| error.to_string())
}

/// `Output=Input`, or one name for both. The output ends at the first `=`.
///
/// Behavior: `io.deephaven.api.Pair#parse`
fn pair(text: &str) -> Result<Pair, String> {
    match text.split_once('=') {
        Some((output, input)) => Ok(Pair::new(column_name(output)?, column_name(input)?)),
        None => Ok(Pair::of(column_name(text)?)),
    }
}

/// The engine's aggregation for one of the request. The range join takes `group` alone,
/// so only its columns are read; every other aggregation is passed on by its name in the
/// protocol, for the engine to refuse. Java reads the others too and then refuses them,
/// with the same status for a fault in one as for the refusal.
///
/// Behavior: `io.deephaven.server.table.ops.AggregationAdapter#adapt`,
/// `io.deephaven.api.agg.Aggregation#of(AggSpec, List)`
fn aggregation(aggregation: &proto::Aggregation) -> Result<RangeAggregation, String> {
    let other = |name: &str| Ok(RangeAggregation::Other(name.to_string()));
    match &aggregation.r#type {
        Some(AggregationType::Columns(columns)) => {
            let spec = columns.spec.as_ref().and_then(|spec| spec.r#type.as_ref());
            match spec {
                Some(SpecType::Group(_)) => {
                    let pairs = columns.match_pairs.iter().map(|text| pair(text));
                    Ok(RangeAggregation::group(
                        pairs.collect::<Result<Vec<_>, _>>()?,
                    ))
                }
                _ => other("columns"),
            }
        }
        Some(AggregationType::Count(_)) => other("count"),
        Some(AggregationType::FirstRowKey(_)) => other("first_row_key"),
        Some(AggregationType::LastRowKey(_)) => other("last_row_key"),
        Some(AggregationType::Partition(_)) => other("partition"),
        Some(AggregationType::Formula(_)) => other("formula"),
        Some(AggregationType::CountWhere(_)) => other("count_where"),
        None => other("not set"),
    }
}

/// Behavior: `io.deephaven.server.table.ops.RangeJoinGrpcImpl`
pub struct Handler;

impl TableOp for Handler {
    type Request = RangeJoinTablesRequest;

    fn result_id(request: &RangeJoinTablesRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &RangeJoinTablesRequest) -> Vec<TableReference> {
        vec![source(&request.left_id), source(&request.right_id)]
    }

    /// In Java's order: both tables are named; the range match is given one way and
    /// whole; there is an aggregation; the table references are well formed; the exact
    /// matches and then the range match can be read; the aggregations are well formed.
    /// Java also refuses a request with unknown fields, which prost drops.
    ///
    /// Behavior: `RangeJoinGrpcImpl#validateRequest`
    fn validate(request: &RangeJoinTablesRequest) -> Result<(), Status> {
        let (Some(left), Some(right)) = (&request.left_id, &request.right_id) else {
            return Err(match request.left_id {
                None => missing_field(REQUEST, "left_id", 2),
                Some(_) => missing_field(REQUEST, "right_id", 3),
            });
        };
        let by_text = has_range_match_text(request);
        for (field, number, present) in detail_fields(request) {
            if !by_text && !present {
                return Err(missing_field(REQUEST, field, number));
            }
            if by_text && present {
                // The message of the status Java catches is its code and its description.
                return Err(Status::invalid_argument(format!(
                    "If `range_match` is provided, range details should remain empty. \n\
                     Internal error: INVALID_ARGUMENT: \
                     io.deephaven.proto.backplane.grpc.{REQUEST} must not have field {field} \
                     ({number})"
                )));
            }
        }
        if request.aggregations.is_empty() {
            return Err(empty_field(REQUEST, "aggregations", 10));
        }
        validate_reference(left)?;
        validate_reference(right)?;
        for text in &request.exact_match_columns {
            exact_match(text).map_err(Status::invalid_argument)?;
        }
        range_match(request).map_err(Status::invalid_argument)?;
        for aggregation in &request.aggregations {
            validate_aggregation(aggregation)?;
        }
        Ok(())
    }

    /// The matches are read again, then the aggregations; a fault in an aggregation is an
    /// exception, as is every refusal of the engine.
    ///
    /// Behavior: `RangeJoinGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &RangeJoinTablesRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let exact = request
            .exact_match_columns
            .iter()
            .map(|text| exact_match(text))
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExportError::Internal)?;
        let range = range_match(request).map_err(ExportError::Internal)?;
        let aggregations = request
            .aggregations
            .iter()
            .map(aggregation)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ExportError::Internal)?;
        Ok(range_join(
            &sources[0],
            &sources[1],
            &exact,
            &range,
            &aggregations,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(text: &str) -> Result<(String, String), String> {
        exact_match(text).map(|pair| (pair.left, pair.right))
    }

    fn both(left: &str, right: &str) -> Result<(String, String), String> {
        Ok((left.to_string(), right.to_string()))
    }

    /// Every answer here is what Java 42.9 gave for the text as an exact match of a
    /// `RangeJoinTablesRequest`.
    #[test]
    fn an_exact_match_is_a_name_or_two_names_around_one_or_two_equals_signs() {
        assert_eq!(names("K"), both("K", "K"));
        assert_eq!(names(" K "), both("K", "K"));
        assert_eq!(names("K = P"), both("K", "P"));
        assert_eq!(names("K==P"), both("K", "P"));
        assert_eq!(names("K\t=\nP"), both("K", "P"));
        assert_eq!(names("\u{dc}bung"), both("\u{dc}bung", "\u{dc}bung"));
        let unparsable = |text: &str| {
            Err(format!(
                "Unable to parse match '{text}', expected form '<left>==<right>' or \
                 `<left>=<right>`"
            ))
        };
        assert_eq!(names("K="), unparsable("K="));
        assert_eq!(names("K=="), unparsable("K=="));
        let invalid = |name: &str| Err(format!("Invalid column name \"{name}\""));
        let empty = Err("Invalid column name: name can not be null or empty".to_string());
        let reserved = |name: &str| {
            Err(format!(
                "Invalid column name \"{name}\": \"{name}\" is a reserved keyword"
            ))
        };
        assert_eq!(names("=K"), empty);
        assert_eq!(names("==K"), empty);
        assert_eq!(names(""), empty);
        assert_eq!(names(" "), empty);
        assert_eq!(names("K===K"), invalid("=K"));
        assert_eq!(names("A=B=C"), invalid("B=C"));
        assert_eq!(names("1K"), invalid("1K"));
        assert_eq!(names("K K"), invalid("K K"));
        assert_eq!(names("K-K"), invalid("K-K"));
        assert_eq!(names("a.b"), invalid("a.b"));
        // `String#trim` leaves a no-break space where it is
        assert_eq!(names("K\u{a0}"), invalid("K\u{a0}"));
        assert_eq!(names("K=\u{dc}bung="), invalid("\u{dc}bung="));
        assert_eq!(names("in"), reserved("in"));
        assert_eq!(names("K=in"), reserved("in"));
        assert_eq!(names("i=K"), reserved("i"));
        // the left name is refused first
        assert_eq!(names("i=in"), reserved("i"));
    }

    #[test]
    fn a_pair_is_an_output_and_an_input_or_one_name_for_both() {
        let of = |text: &str| pair(text).map(|pair| (pair.output, pair.input));
        assert_eq!(of("V"), both("V", "V"));
        assert_eq!(of("  Vs  =  V  "), both("Vs", "V"));
        assert_eq!(of("Vs == V"), Err("Invalid column name \"= V\"".to_string()));
        assert_eq!(of("X=Y=Z"), Err("Invalid column name \"Y=Z\"".to_string()));
        assert_eq!(of("1x"), Err("Invalid column name \"1x\"".to_string()));
        assert_eq!(
            of(""),
            Err("Invalid column name: name can not be null or empty".to_string())
        );
    }
}
