//! `Aggregate` and `AggregateAll`: one row per group of rows, with aggregated columns.
//!
//! The engine has sum, avg, min, max, first, last and count. A request for any other
//! aggregation, or for initial groups, is refused with `UNIMPLEMENTED` and the name of
//! what is missing.

use super::prelude::*;
use oxhaven_engine::ops::agg::{aggregate, AggSpec, AggregateError, Aggregation, Pair};
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::agg_spec::Type as SpecType;
use oxhaven_proto::grpc::aggregation::Type as AggregationType;
use oxhaven_proto::grpc::{AggregateAllRequest, AggregateRequest};

/// What Java's aggregation refuses is an exception out of the engine.
impl From<AggregateError> for ExportError {
    fn from(error: AggregateError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

const ONE_OF_NOTE: &str = "Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.";

/// Behavior: `io.deephaven.server.grpc.GrpcErrorHelper#checkRepeatedFieldNonEmpty`
fn empty_field(message: &str, field: &str, number: u32) -> Status {
    Status::invalid_argument(format!(
        "io.deephaven.proto.backplane.grpc.{message} must have at least one {field} ({number})"
    ))
}

/// Behavior: `io.deephaven.server.grpc.GrpcErrorHelper#checkHasOneOf`
fn missing_one_of(message: &str) -> Status {
    Status::invalid_argument(format!(
        "io.deephaven.proto.backplane.grpc.{message} must have oneof type. {ONE_OF_NOTE}"
    ))
}

/// Behavior: `io.deephaven.server.table.ops.AggSpecAdapter#validate(AggSpec)`, without the
/// checks of the options of specs that are not ported
fn validate_spec(spec: &proto::AggSpec) -> Result<(), Status> {
    if spec.r#type.is_none() {
        return Err(missing_one_of("AggSpec"));
    }
    Ok(())
}

/// The engine's aggregation for a spec, or the status that names what is not ported.
///
/// Behavior: `io.deephaven.server.table.ops.AggSpecAdapter#adapt`
fn spec(spec: &proto::AggSpec) -> Result<AggSpec, Status> {
    let missing = |name: &str| {
        Err(not_ported(
            "io.deephaven.server.table.ops.AggSpecAdapter#adapt",
            format!("the aggregation {name}"),
        ))
    };
    match &spec.r#type {
        Some(SpecType::Sum(_)) => Ok(AggSpec::Sum),
        Some(SpecType::Avg(_)) => Ok(AggSpec::Avg),
        Some(SpecType::Min(_)) => Ok(AggSpec::Min),
        Some(SpecType::Max(_)) => Ok(AggSpec::Max),
        Some(SpecType::First(_)) => Ok(AggSpec::First),
        Some(SpecType::Last(_)) => Ok(AggSpec::Last),
        Some(SpecType::AbsSum(_)) => missing("abs_sum"),
        Some(SpecType::ApproximatePercentile(_)) => missing("approximate_percentile"),
        Some(SpecType::CountDistinct(_)) => missing("count_distinct"),
        Some(SpecType::Distinct(_)) => missing("distinct"),
        Some(SpecType::Formula(_)) => missing("formula"),
        Some(SpecType::Freeze(_)) => missing("freeze"),
        Some(SpecType::Group(_)) => missing("group"),
        Some(SpecType::Median(_)) => missing("median"),
        Some(SpecType::Percentile(_)) => missing("percentile"),
        Some(SpecType::SortedFirst(_)) => missing("sorted_first"),
        Some(SpecType::SortedLast(_)) => missing("sorted_last"),
        Some(SpecType::Std(_)) => missing("std"),
        Some(SpecType::TDigest(_)) => missing("t_digest"),
        Some(SpecType::Unique(_)) => missing("unique"),
        Some(SpecType::WeightedAvg(_)) => missing("weighted_avg"),
        Some(SpecType::WeightedSum(_)) => missing("weighted_sum"),
        Some(SpecType::Var(_)) => missing("var"),
        None => Err(missing_one_of("AggSpec")),
    }
}

/// A column name as a request gives it: surrounding space is dropped, and a name that
/// cannot name a column is an internal error, as Java's `InvalidNameException` is.
///
/// Behavior: `io.deephaven.api.ColumnName#parse`
fn column_name(text: &str) -> Result<String, ExportError> {
    let name = text.trim();
    validate_column_name(name).map_err(ExportError::Internal)?;
    Ok(name.to_string())
}

/// `Output=Input`, or one name for both.
///
/// Behavior: `io.deephaven.api.Pair#parse`
fn pair(text: &str) -> Result<Pair, ExportError> {
    match text.split_once('=') {
        Some((output, input)) => Ok(Pair::new(column_name(output)?, column_name(input)?)),
        None => Ok(Pair::of(column_name(text)?)),
    }
}

/// The engine panics with `unported!` for an output that has the name of a group-by
/// column; a request for one is refused here instead.
fn check_outputs(aggregations: &[Aggregation], group_by: &[String]) -> Result<(), Status> {
    let outputs = aggregations.iter().flat_map(|aggregation| match aggregation {
        Aggregation::Columns { pairs, .. } => pairs.iter().map(|p| &p.output).collect(),
        Aggregation::Count { column } => vec![column],
        _ => Vec::new(),
    });
    for output in outputs {
        if group_by.contains(output) {
            return Err(not_ported(
                "io.deephaven.engine.table.impl.by.AggregationContext#getResultColumns",
                format!("aggregation output {output} has the name of a group-by column"),
            ));
        }
    }
    Ok(())
}

fn run(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    group_by: &[String],
) -> Result<Arc<dyn Table>, ExportError> {
    check_outputs(aggregations, group_by)?;
    let group_by: Vec<&str> = group_by.iter().map(String::as_str).collect();
    Ok(aggregate(parent, aggregations, preserve_empty, &group_by)?)
}

/// Behavior: `io.deephaven.server.table.ops.AggregateGrpcImpl`
pub struct Aggregate;

impl TableOp for Aggregate {
    type Request = AggregateRequest;

    fn result_id(request: &AggregateRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &AggregateRequest) -> Vec<TableReference> {
        let mut sources = vec![source(&request.source_id)];
        sources.extend(request.initial_groups_id.clone());
        sources
    }

    /// Behavior: `AggregateGrpcImpl#validateRequest`,
    /// `io.deephaven.server.table.ops.AggregationAdapter#validate`
    fn validate(request: &AggregateRequest) -> Result<(), Status> {
        if request.source_id.is_none() {
            return Err(missing_field("AggregateRequest", "source_id", 2));
        }
        if request.aggregations.is_empty() {
            return Err(empty_field("AggregateRequest", "aggregations", 5));
        }
        for aggregation in &request.aggregations {
            match &aggregation.r#type {
                None => return Err(missing_one_of("Aggregation")),
                Some(AggregationType::Columns(columns)) => {
                    let Some(spec) = &columns.spec else {
                        return Err(missing_field("Aggregation.AggregationColumns", "spec", 1));
                    };
                    if columns.match_pairs.is_empty() {
                        return Err(empty_field(
                            "Aggregation.AggregationColumns",
                            "match_pairs",
                            2,
                        ));
                    }
                    validate_spec(spec)?;
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    fn create(
        _context: &OpContext,
        request: &AggregateRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        if request.initial_groups_id.is_some() {
            return Err(not_ported(
                "io.deephaven.engine.table.impl.QueryTable#aggBy",
                "initial groups",
            )
            .into());
        }
        let missing = |name: &str| {
            ExportError::Status(not_ported(
                "io.deephaven.server.table.ops.AggregationAdapter#adapt",
                format!("the aggregation {name}"),
            ))
        };
        let mut aggregations = Vec::new();
        for aggregation in &request.aggregations {
            aggregations.push(match &aggregation.r#type {
                Some(AggregationType::Columns(columns)) => {
                    let kind = spec(columns.spec.as_ref().ok_or_else(|| {
                        missing_field("Aggregation.AggregationColumns", "spec", 1)
                    })?)?;
                    let pairs = columns
                        .match_pairs
                        .iter()
                        .map(|text| pair(text))
                        .collect::<Result<Vec<_>, _>>()?;
                    Aggregation::columns(kind, pairs)
                }
                Some(AggregationType::Count(count)) => {
                    Aggregation::count(column_name(&count.column_name)?)
                }
                Some(AggregationType::FirstRowKey(_)) => return Err(missing("first_row_key")),
                Some(AggregationType::LastRowKey(_)) => return Err(missing("last_row_key")),
                Some(AggregationType::Partition(_)) => return Err(missing("partition")),
                Some(AggregationType::Formula(_)) => return Err(missing("formula")),
                Some(AggregationType::CountWhere(_)) => return Err(missing("count_where")),
                None => return Err(missing_one_of("Aggregation").into()),
            });
        }
        run(
            &sources[0],
            &aggregations,
            request.preserve_empty,
            &request.group_by_columns,
        )
    }
}

/// Behavior: `io.deephaven.server.table.ops.AggregateAllGrpcImpl`
pub struct AggregateAll;

impl TableOp for AggregateAll {
    type Request = AggregateAllRequest;

    fn result_id(request: &AggregateAllRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &AggregateAllRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Behavior: `AggregateAllGrpcImpl#validateRequest`
    fn validate(request: &AggregateAllRequest) -> Result<(), Status> {
        if request.source_id.is_none() {
            return Err(missing_field("AggregateAllRequest", "source_id", 2));
        }
        match &request.spec {
            None => Err(missing_field("AggregateAllRequest", "spec", 3)),
            Some(spec) => validate_spec(spec),
        }
    }

    /// The spec applied to every column that is not grouped by.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.QueryTable#aggAllBy` and its
    /// `singleAggregation`
    fn create(
        _context: &OpContext,
        request: &AggregateAllRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let parent = &sources[0];
        let kind = spec(
            request
                .spec
                .as_ref()
                .ok_or_else(|| missing_field("AggregateAllRequest", "spec", 3))?,
        )?;
        let group_by = &request.group_by_columns;
        let definition = parent.definition();
        if let Some(unknown) = group_by.iter().find(|name| definition.index_of(name).is_none()) {
            return Err(ExportError::Internal(format!(
                "aggAllBy spec references column that does not exist: {unknown}"
            )));
        }
        let pairs: Vec<Pair> = definition
            .columns()
            .iter()
            .filter(|column| !group_by.contains(&column.name))
            .map(|column| Pair::of(column.name.as_str()))
            .collect();
        if pairs.is_empty() {
            return Err(ExportError::Internal(
                "aggAllBy has no columns to aggregate".to_string(),
            ));
        }
        run(parent, &[Aggregation::columns(kind, pairs)], false, group_by)
    }
}
