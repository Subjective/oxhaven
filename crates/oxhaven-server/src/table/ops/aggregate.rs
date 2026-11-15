//! `Aggregate` and `AggregateAll`: one row per group of rows, with aggregated columns.
//!
//! The engine has sum, abs_sum, avg, min, max, first, last, var, std, median, percentile,
//! count_distinct, distinct, unique, weighted_avg, weighted_sum, sorted_first, sorted_last,
//! group, formula (in both forms), count and count_where. A request for any other
//! aggregation (partition, the row keys, approximate_percentile, t_digest, freeze), or for
//! initial groups, is refused with `UNIMPLEMENTED` and the name of what is missing.
//!
//! Java checks the filters of a count where and the formulas of a formula aggregation with
//! its `ColumnExpressionValidator` (`AggregateGrpcImpl#validateFormulas`,
//! `AggSpecFormulaValidator`) before the engine sees them; see `select.rs` for why the
//! formula compiler makes the same refusals here. Java validates a formula with a
//! parameter token against every column of the grouped table, and the engine then compiles
//! it against its one input column; a formula either refuses is refused here, by the
//! engine, with the same status.

use super::prelude::*;
use oxhaven_engine::ops::agg::{
    aggregate_with, AggSpec, AggregateError, Aggregation, Pair, Sentinel as NonUnique,
};
use oxhaven_proto::grpc as proto;
use oxhaven_proto::grpc::agg_spec::Type as SpecType;
use oxhaven_proto::grpc::aggregation::Type as AggregationType;
use oxhaven_proto::grpc::{AggregateAllRequest, AggregateRequest, ComboAggregateRequest};

/// What Java's aggregation refuses is an exception out of the engine.
impl From<AggregateError> for ExportError {
    fn from(error: AggregateError) -> Self {
        ExportError::Internal(error.to_string())
    }
}

const ONE_OF_NOTE: &str = "Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.";

/// Behavior: `io.deephaven.server.grpc.GrpcErrorHelper#checkRepeatedFieldNonEmpty`
pub(super) fn empty_field(message: &str, field: &str, number: u32) -> Status {
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

/// Behavior: `io.deephaven.server.table.ops.AggSpecAdapter#validate(AggSpec)`, whose only
/// check beyond the oneof is that of `unique`
fn validate_spec(spec: &proto::AggSpec) -> Result<(), Status> {
    match &spec.r#type {
        None => Err(missing_one_of("AggSpec")),
        Some(SpecType::Unique(unique)) => validate_unique(unique),
        Some(_) => Ok(()),
    }
}

/// Behavior: `AggSpecAdapter#validate(AggSpecUnique)` and
/// `AggSpecAdapter#validate(AggSpecNonUniqueSentinel)`
fn validate_unique(unique: &proto::agg_spec::AggSpecUnique) -> Result<(), Status> {
    use proto::agg_spec::agg_spec_non_unique_sentinel::Type as Sentinel;
    let Some(sentinel) = &unique.non_unique_sentinel else {
        return Err(missing_field(
            "AggSpec.AggSpecUnique",
            "non_unique_sentinel",
            2,
        ));
    };
    let out_of_range = |field: &str| {
        Err(Status::invalid_argument(format!(
            "AggSpecNonUniqueSentinel {field} out of range"
        )))
    };
    match sentinel.r#type {
        None => Err(missing_one_of("AggSpec.AggSpecNonUniqueSentinel")),
        Some(Sentinel::NullValue(value)) if value != proto::NullValue::NullValue as i32 => {
            out_of_range("null_value")
        }
        Some(Sentinel::ByteValue(value)) if i8::try_from(value).is_err() => {
            out_of_range("byte_value")
        }
        Some(Sentinel::ShortValue(value)) if i16::try_from(value).is_err() => {
            out_of_range("short_value")
        }
        Some(Sentinel::CharValue(value)) if u16::try_from(value).is_err() => {
            out_of_range("char_value")
        }
        Some(_) => Ok(()),
    }
}

/// A column name that a spec gives, which Java takes as it is, without trimming.
///
/// Behavior: `io.deephaven.api.ColumnName#of`
fn spec_column(text: &str) -> Result<String, ExportError> {
    validate_column_name(text).map_err(ExportError::Internal)?;
    Ok(text.to_string())
}

/// The engine's aggregation for a spec, or the status that names what is not ported.
///
/// Behavior: `io.deephaven.server.table.ops.AggSpecAdapter#adapt`
fn spec(spec: &proto::AggSpec) -> Result<AggSpec, ExportError> {
    use proto::agg_spec::agg_spec_non_unique_sentinel::Type as Sentinel;
    let missing = |name: &str| {
        Err(not_ported(
            "io.deephaven.server.table.ops.AggSpecAdapter#adapt",
            format!("the aggregation {name}"),
        )
        .into())
    };
    // Behavior: `AggSpecAdapter#adaptFirst`, `#adaptLast` and `#adapt(AggSpecSortedColumn)`,
    // which sort every column ascending.
    let sorted = |sorted: &proto::agg_spec::AggSpecSorted| {
        sorted
            .columns
            .iter()
            .map(|column| spec_column(&column.column_name))
            .collect::<Result<Vec<_>, _>>()
    };
    match &spec.r#type {
        Some(SpecType::Sum(_)) => Ok(AggSpec::Sum),
        Some(SpecType::Avg(_)) => Ok(AggSpec::Avg),
        Some(SpecType::Min(_)) => Ok(AggSpec::Min),
        Some(SpecType::Max(_)) => Ok(AggSpec::Max),
        Some(SpecType::First(_)) => Ok(AggSpec::First),
        Some(SpecType::Last(_)) => Ok(AggSpec::Last),
        Some(SpecType::AbsSum(_)) => Ok(AggSpec::AbsSum),
        Some(SpecType::Std(_)) => Ok(AggSpec::Std),
        Some(SpecType::Var(_)) => Ok(AggSpec::Var),
        Some(SpecType::Median(median)) => Ok(AggSpec::Median {
            average_evenly_divided: median.average_evenly_divided,
        }),
        Some(SpecType::Percentile(percentile)) => Ok(AggSpec::Percentile {
            percentile: percentile.percentile,
            average_evenly_divided: percentile.average_evenly_divided,
        }),
        Some(SpecType::CountDistinct(count)) => Ok(AggSpec::CountDistinct {
            count_nulls: count.count_nulls,
        }),
        Some(SpecType::Unique(unique)) => {
            let sentinel = unique.non_unique_sentinel.as_ref().and_then(|s| s.r#type.as_ref());
            // Behavior: `AggSpecAdapter#adapt(AggSpecNonUniqueSentinel)`. The casts are
            // Java's, of values `validate_unique` has found in range.
            let non_unique = match sentinel {
                Some(Sentinel::NullValue(_)) => NonUnique::Null,
                Some(Sentinel::StringValue(value)) => NonUnique::String(value.clone()),
                Some(Sentinel::IntValue(value)) => NonUnique::Int(*value),
                Some(Sentinel::LongValue(value)) => NonUnique::Long(*value),
                Some(Sentinel::FloatValue(value)) => NonUnique::Float(*value),
                Some(Sentinel::DoubleValue(value)) => NonUnique::Double(*value),
                Some(Sentinel::BoolValue(value)) => NonUnique::Boolean(*value),
                Some(Sentinel::ByteValue(value)) => NonUnique::Byte(*value as i8),
                Some(Sentinel::ShortValue(value)) => NonUnique::Short(*value as i16),
                Some(Sentinel::CharValue(value)) => NonUnique::Char(*value as u16),
                None => return Err(missing_one_of("AggSpec.AggSpecNonUniqueSentinel").into()),
            };
            Ok(AggSpec::Unique {
                include_nulls: unique.include_nulls,
                non_unique,
            })
        }
        Some(SpecType::WeightedAvg(weighted)) => Ok(AggSpec::WeightedAvg {
            weight: spec_column(&weighted.weight_column)?,
        }),
        Some(SpecType::WeightedSum(weighted)) => Ok(AggSpec::WeightedSum {
            weight: spec_column(&weighted.weight_column)?,
        }),
        Some(SpecType::SortedFirst(first)) => Ok(AggSpec::SortedFirst {
            columns: sorted(first)?,
        }),
        Some(SpecType::SortedLast(last)) => Ok(AggSpec::SortedLast {
            columns: sorted(last)?,
        }),
        Some(SpecType::ApproximatePercentile(_)) => missing("approximate_percentile"),
        Some(SpecType::Distinct(distinct)) => Ok(AggSpec::Distinct {
            include_nulls: distinct.include_nulls,
        }),
        // Behavior: `AggSpecAdapter#adapt(AggSpecFormula)`
        Some(SpecType::Formula(formula)) => Ok(AggSpec::Formula {
            formula: formula.formula.clone(),
            param_token: formula.param_token.clone(),
        }),
        Some(SpecType::Freeze(_)) => missing("freeze"),
        Some(SpecType::Group(_)) => Ok(AggSpec::Group),
        Some(SpecType::TDigest(_)) => missing("t_digest"),
        None => Err(missing_one_of("AggSpec").into()),
    }
}

/// The checks of one aggregation of a request, which need no table.
///
/// Behavior: `io.deephaven.server.table.ops.AggregationAdapter#validate`
pub(super) fn validate_aggregation(aggregation: &proto::Aggregation) -> Result<(), Status> {
    match &aggregation.r#type {
        None => Err(missing_one_of("Aggregation")),
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
            validate_spec(spec)
        }
        Some(_) => Ok(()),
    }
}

/// The columns besides the group-by columns that `aggAllBy` leaves out for `spec`: the
/// weight of a weighted spec, and for the specs that compute a new value the formatting
/// columns.
///
/// Behavior: `io.deephaven.api.agg.spec.AggregateAllExclusions#of`,
/// `io.deephaven.api.agg.spec.AggAllByExcludeFormattingColumns`,
/// `io.deephaven.engine.util.ColumnFormatting#isFormattingColumn`
fn excluded_by_agg_all(spec: &AggSpec, column: &str) -> bool {
    let formatting = ["__TABLE_STYLE_FORMAT", "__TABLE_NUMBER_FORMAT", "__TABLE_DATE_FORMAT"]
        .iter()
        .any(|suffix| column.ends_with(suffix));
    match spec {
        AggSpec::WeightedAvg { weight } | AggSpec::WeightedSum { weight } => {
            weight == column || formatting
        }
        AggSpec::Min
        | AggSpec::Max
        | AggSpec::First
        | AggSpec::Last
        | AggSpec::Unique { .. }
        | AggSpec::Distinct { .. }
        | AggSpec::SortedFirst { .. }
        | AggSpec::SortedLast { .. } => false,
        // Sum, abs_sum, avg, var, std, median, percentile, count_distinct and group.
        _ => formatting,
    }
}

/// The columns a spec names, which `aggAllBy` requires to exist.
///
/// Behavior: `io.deephaven.api.agg.spec.AggSpecColumnReferences`
fn spec_references(spec: &AggSpec) -> Vec<&str> {
    match spec {
        AggSpec::WeightedAvg { weight } | AggSpec::WeightedSum { weight } => vec![weight],
        AggSpec::SortedFirst { columns } | AggSpec::SortedLast { columns } => {
            columns.iter().map(String::as_str).collect()
        }
        _ => Vec::new(),
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

/// The output column and the expression of a formula aggregation: `Column = expression`,
/// or the name of a column, which is then both.
///
/// Behavior: `io.deephaven.server.table.ops.AggregationAdapter#adapt(Selectable)`, which
/// takes only the raw form, and `io.deephaven.api.Selectable#parse`
fn selectable(selectable: Option<&proto::Selectable>) -> Result<(String, String), ExportError> {
    let Some(proto::selectable::Type::Raw(text)) = selectable.and_then(|s| s.r#type.as_ref())
    else {
        return Err(ExportError::Internal(
            "Unsupported Selectable type (TYPE_NOT_SET) in Aggregation.".to_string(),
        ));
    };
    let Some((name, expression)) = text.split_once('=') else {
        let name = column_name(text)?;
        return Ok((name.clone(), name));
    };
    if expression.is_empty() || expression.starts_with('=') {
        return Err(ExportError::Internal(format!(
            "Unable to parse formula '{text}', expected form '<newColumn>=<expression>'"
        )));
    }
    Ok((column_name(name)?, expression.to_string()))
}

/// The engine panics with `unported!` for a count where without filters, whose count Java
/// leaves undefined; a request for one is refused here instead.
fn check_filters(aggregations: &[Aggregation]) -> Result<(), Status> {
    for aggregation in aggregations {
        if let Aggregation::CountWhere { filters, .. } = aggregation {
            if filters.is_empty() {
                return Err(not_ported(
                    "io.deephaven.engine.table.impl.by.CountWhereOperator#applyFilters",
                    "a count where without filters",
                ));
            }
        }
    }
    Ok(())
}

fn run(
    context: &OpContext,
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    group_by: &[String],
) -> Result<Arc<dyn Table>, ExportError> {
    check_filters(aggregations)?;
    let group_by: Vec<&str> = group_by.iter().map(String::as_str).collect();
    Ok(aggregate_with(
        parent,
        aggregations,
        preserve_empty,
        &group_by,
        context.compiler.as_ref(),
    )?)
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
        let Some(source) = &request.source_id else {
            return Err(missing_field("AggregateRequest", "source_id", 2));
        };
        if request.aggregations.is_empty() {
            return Err(empty_field("AggregateRequest", "aggregations", 5));
        }
        validate_reference(source)?;
        if let Some(initial_groups) = &request.initial_groups_id {
            validate_reference(initial_groups)?;
        }
        for aggregation in &request.aggregations {
            validate_aggregation(aggregation)?;
        }
        Ok(())
    }

    fn create(
        context: &OpContext,
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
                // Behavior: `AggregationAdapter#adapt(AggregationPartition)`
                Some(AggregationType::Partition(partition)) => Aggregation::Partition {
                    column: column_name(&partition.column_name)?,
                    include_group_by_columns: partition.include_group_by_columns,
                },
                Some(AggregationType::Formula(formula)) => {
                    let (column, expression) = selectable(formula.selectable.as_ref())?;
                    Aggregation::formula(column, expression)
                }
                // Behavior: `AggregationAdapter#adapt(AggregationCountWhere)`, whose
                // `ColumnName#of` takes the name as it is.
                Some(AggregationType::CountWhere(count)) => Aggregation::count_where(
                    spec_column(&count.column_name)?,
                    count.filters.iter().cloned(),
                ),
                None => return Err(missing_one_of("Aggregation").into()),
            });
        }
        run(
            context,
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
        let Some(source) = &request.source_id else {
            return Err(missing_field("AggregateAllRequest", "source_id", 2));
        };
        let Some(spec) = &request.spec else {
            return Err(missing_field("AggregateAllRequest", "spec", 3));
        };
        validate_reference(source)?;
        validate_spec(spec)
    }

    /// The spec applied to every column that is not grouped by.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.QueryTable#aggAllBy` and its
    /// `singleAggregation`
    fn create(
        context: &OpContext,
        request: &AggregateAllRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let kind = spec(
            request
                .spec
                .as_ref()
                .ok_or_else(|| missing_field("AggregateAllRequest", "spec", 3))?,
        )?;
        agg_all_by(context, &sources[0], kind, &request.group_by_columns)
    }
}

/// `spec` applied to every column that is not grouped by and that the spec does not leave
/// out.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#aggAllBy` and its
/// `singleAggregation`
fn agg_all_by(
    context: &OpContext,
    parent: &Arc<dyn Table>,
    kind: AggSpec,
    group_by: &[String],
) -> Result<Arc<dyn Table>, ExportError> {
    let definition = parent.definition();
    let references = spec_references(&kind);
    if let Some(unknown) = group_by
        .iter()
        .map(String::as_str)
        .chain(references)
        .find(|name| definition.index_of(name).is_none())
    {
        return Err(ExportError::Internal(format!(
            "aggAllBy spec references column that does not exist: {unknown}"
        )));
    }
    let pairs: Vec<Pair> = definition
        .columns()
        .iter()
        .filter(|column| {
            !group_by.contains(&column.name) && !excluded_by_agg_all(&kind, &column.name)
        })
        .map(|column| Pair::of(column.name.as_str()))
        .collect();
    if pairs.is_empty() {
        return Err(ExportError::Internal(
            "aggAllBy has no columns to aggregate".to_string(),
        ));
    }
    run(
        context,
        parent,
        &[Aggregation::columns(kind, pairs)],
        false,
        group_by,
    )
}

/// The engine's spec for a combo aggregate type other than `COUNT`, which has none. A type
/// this server does not know is refused.
///
/// Behavior: `ComboAggregateGrpcImpl#singleAggregateHelper` and `#makeAggregation`
fn combo_spec(aggregate: &proto::combo_aggregate_request::Aggregate) -> Result<AggSpec, ExportError> {
    use proto::combo_aggregate_request::AggType;
    Ok(match AggType::try_from(aggregate.r#type) {
        Ok(AggType::Sum) => AggSpec::Sum,
        Ok(AggType::AbsSum) => AggSpec::AbsSum,
        Ok(AggType::Avg) => AggSpec::Avg,
        Ok(AggType::First) => AggSpec::First,
        Ok(AggType::Last) => AggSpec::Last,
        Ok(AggType::Min) => AggSpec::Min,
        Ok(AggType::Max) => AggSpec::Max,
        Ok(AggType::Std) => AggSpec::Std,
        Ok(AggType::Var) => AggSpec::Var,
        // `AggSpec#median()`: AggSpecMedian.AVERAGE_EVENLY_DIVIDED_DEFAULT is true.
        Ok(AggType::Median) => AggSpec::Median {
            average_evenly_divided: true,
        },
        Ok(AggType::Percentile) => AggSpec::Percentile {
            percentile: aggregate.percentile,
            average_evenly_divided: aggregate.avg_median,
        },
        Ok(AggType::WeightedAvg) => AggSpec::WeightedAvg {
            weight: spec_column(&aggregate.column_name)?,
        },
        Ok(AggType::Group) => AggSpec::Group,
        Ok(AggType::Count) => unreachable!("COUNT has no spec"),
        // `default: throw new UnsupportedOperationException("Unsupported aggregate: ...")`
        Err(_) => {
            return Err(ExportError::Internal(format!(
                "Unsupported aggregate: {}",
                aggregate.r#type
            )))
        }
    })
}

/// The Java name of a combo aggregate type, as the validation messages print it.
fn agg_type_name(value: i32) -> &'static str {
    proto::combo_aggregate_request::AggType::try_from(value)
        .map_or("UNRECOGNIZED", |kind| kind.as_str_name())
}

/// Behavior: `io.deephaven.server.table.ops.ComboAggregateGrpcImpl`
pub struct ComboAggregate;

impl ComboAggregate {
    /// Behavior: `ComboAggregateGrpcImpl#isSimpleAggregation`
    fn is_simple(request: &ComboAggregateRequest) -> bool {
        use proto::combo_aggregate_request::AggType;
        let [only] = request.aggregates.as_slice() else {
            return false;
        };
        !request.force_combo
            && only.column_name.is_empty()
            && only.r#type != AggType::Percentile as i32
            && only.match_pairs.is_empty()
    }
}

impl TableOp for ComboAggregate {
    type Request = ComboAggregateRequest;

    fn result_id(request: &ComboAggregateRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &ComboAggregateRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Behavior: `ComboAggregateGrpcImpl#validateRequest`
    fn validate(request: &ComboAggregateRequest) -> Result<(), Status> {
        use proto::combo_aggregate_request::AggType;
        let invalid = |message: String| Err(Status::invalid_argument(message));
        if request.aggregates.is_empty() {
            return invalid("ComboAggregateRequest incorrectly has zero aggregates provided".into());
        }
        if request
            .group_by_columns
            .iter()
            .any(|column| validate_column_name(column).is_err())
        {
            return invalid("ComboAggregateRequest group by".into());
        }
        let counts_or_weighs =
            |kind: i32| kind == AggType::Count as i32 || kind == AggType::WeightedAvg as i32;
        if Self::is_simple(request) {
            let aggregate = &request.aggregates[0];
            let prefix = "force_combo is false and only one aggregate provided, but";
            if !aggregate.match_pairs.is_empty() {
                return invalid(format!("{prefix} match_pairs is specified"));
            }
            if aggregate.percentile != 0.0 {
                return invalid(format!("{prefix} percentile is specified"));
            }
            if aggregate.avg_median {
                return invalid(format!("{prefix} avg_median is specified"));
            }
            if !counts_or_weighs(aggregate.r#type) && !aggregate.column_name.is_empty() {
                return invalid(format!(
                    "{prefix} column_name is specified for type other than COUNT or WEIGHTED_AVG"
                ));
            }
            return Ok(());
        }
        for aggregate in &request.aggregates {
            let name = agg_type_name(aggregate.r#type);
            if aggregate.r#type != AggType::Percentile as i32 {
                if aggregate.percentile != 0.0 {
                    return invalid(format!("percentile is specified for type {name}"));
                }
                if aggregate.avg_median {
                    return invalid(format!("avg_median is specified for type {name}"));
                }
            }
            if aggregate.r#type == AggType::Count as i32 && !aggregate.match_pairs.is_empty() {
                return invalid("match_pairs is specified for type COUNT".into());
            }
            if !counts_or_weighs(aggregate.r#type) && !aggregate.column_name.is_empty() {
                return invalid(format!("column_name is specified for type {name}"));
            }
        }
        Ok(())
    }

    /// Behavior: `ComboAggregateGrpcImpl#create`, `#singleAggregateHelper` (whose
    /// `sumBy`, `medianBy` and the like are `aggAllBy`, and `countBy` is `aggBy` of one
    /// count) and `#comboAggregateHelper`
    fn create(
        context: &OpContext,
        request: &ComboAggregateRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        use proto::combo_aggregate_request::AggType;
        let parent = &sources[0];
        // `ColumnName#of`
        let group_by = request
            .group_by_columns
            .iter()
            .map(|column| spec_column(column))
            .collect::<Result<Vec<_>, _>>()?;
        let count = |aggregate: &proto::combo_aggregate_request::Aggregate| {
            Ok::<_, ExportError>(Aggregation::count(spec_column(&aggregate.column_name)?))
        };
        if Self::is_simple(request) {
            let aggregate = &request.aggregates[0];
            if aggregate.r#type == AggType::Count as i32 {
                return run(context, parent, &[count(aggregate)?], false, &group_by);
            }
            return agg_all_by(context, parent, combo_spec(aggregate)?, &group_by);
        }
        let definition = parent.definition();
        let mut aggregations = Vec::with_capacity(request.aggregates.len());
        for aggregate in &request.aggregates {
            if aggregate.r#type == AggType::Count as i32 {
                aggregations.push(count(aggregate)?);
                continue;
            }
            let kind = combo_spec(aggregate)?;
            // Behavior: `ComboAggregateGrpcImpl#getColumnPairs`: without match pairs, every
            // column that is not grouped by and is not the weight of a weighted average.
            let pairs: Vec<Pair> = if aggregate.match_pairs.is_empty() {
                definition
                    .columns()
                    .iter()
                    .filter(|column| {
                        !group_by.contains(&column.name)
                            && !(aggregate.r#type == AggType::WeightedAvg as i32
                                && aggregate.column_name == column.name)
                    })
                    .map(|column| Pair::of(column.name.as_str()))
                    .collect()
            } else {
                aggregate
                    .match_pairs
                    .iter()
                    .map(|text| pair(text))
                    .collect::<Result<_, _>>()?
            };
            // Behavior: `io.deephaven.api.agg.Aggregation#of(AggSpec, List)`
            if pairs.is_empty() {
                return Err(ExportError::Internal(
                    "Must have at least one pair to create an Aggregation from an AggSpec."
                        .to_string(),
                ));
            }
            aggregations.push(Aggregation::columns(kind, pairs));
        }
        run(context, parent, &aggregations, false, &group_by)
    }
}
