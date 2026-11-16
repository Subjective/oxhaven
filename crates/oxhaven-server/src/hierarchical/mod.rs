//! Hierarchical tables: the `HierarchicalTableService`, what a client is told about a
//! hierarchical table it opens, and the subscription to a view of one.
//!
//! A hierarchical table is exported as an object ([`ExportValue::object`]) that holds a
//! [`Hierarchical`], and a view of one as an object that holds a [`ViewOf`];
//! [`hierarchical_of`] and [`view_of`] read them back. `docs/HIERARCHICAL.md` is the design.

mod subscription;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tree_tests;

pub use subscription::ViewSubscription;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::table::Table;
use oxhaven_engine::ops::agg::{AggSpec, Aggregation};
use oxhaven_engine::ops::filter::{CheckedFilters, Condition};
use oxhaven_engine::ops::hierarchical::{
    promote_orphans, rollup, tree, HierarchicalError, Hierarchy, RollupTable, TreeTable, View,
    DEPTH_COLUMN, EXPANDED_COLUMN,
};
use oxhaven_engine::ops::sort::SortColumn;
use oxhaven_proto::grpc::hierarchical_table_service_server::HierarchicalTableService;
use oxhaven_proto::grpc::hierarchical_table_view_request::Target;
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::{
    ExportedTableCreationResponse, HierarchicalTableApplyRequest, HierarchicalTableApplyResponse,
    HierarchicalTableDescriptor, HierarchicalTableSourceExportRequest,
    HierarchicalTableViewRequest, HierarchicalTableViewResponse, RollupRequest, RollupResponse,
    SortDescriptor, TableReference, Ticket, TreeRequest, TreeResponse,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::partitioned::guarded;
use crate::schema::{arrow_schema, schema_ipc_bytes, SchemaContext};
use crate::session::{current_session, ExportError, ExportValue, Reply, TicketRouter};
use crate::table::ops::{adapt_aggregations, conditions, validate_aggregation};
use crate::table::support::{missing_field, not_ported, OpContext};
use crate::table::table_creation_response;

/// A hierarchical table of one of the kinds there are.
///
/// Behavior: `io.deephaven.engine.table.hierarchical.HierarchicalTable`
pub enum HierarchicalTable {
    Rollup(Arc<RollupTable>),
    Tree(Arc<TreeTable>),
}

impl HierarchicalTable {
    pub fn hierarchy(&self) -> Arc<dyn Hierarchy> {
        match self {
            HierarchicalTable::Rollup(rollup) => rollup.clone(),
            HierarchicalTable::Tree(tree) => tree.clone(),
        }
    }

    /// Behavior: `HierarchicalTable#getSource`
    pub fn source(&self) -> Arc<dyn Table> {
        match self {
            HierarchicalTable::Rollup(rollup) => rollup.source().clone(),
            HierarchicalTable::Tree(tree) => tree.source().clone(),
        }
    }

    /// The columns a client may sort by, or `None` when it may sort by any.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.BaseGridAttributes#getSortableColumns`
    pub fn sortable_columns(&self) -> Option<&[String]> {
        match self {
            HierarchicalTable::Rollup(rollup) => rollup.sortable_columns(),
            HierarchicalTable::Tree(tree) => tree.sortable_columns(),
        }
    }
}

/// A hierarchical table as exports and variables hold it.
pub type Hierarchical = Arc<HierarchicalTable>;

/// A view of a hierarchical table as exports hold it.
///
/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableView`
pub struct ViewOf {
    pub table: Hierarchical,
    pub view: View,
}

/// The name clients know the type of a hierarchical table by.
///
/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableTypePlugin#name`
pub const TYPE_NAME: &str = "HierarchicalTable";

/// The hierarchical table of a value, if it is one.
pub fn hierarchical_of(value: &ExportValue) -> Option<&Hierarchical> {
    value.downcast_ref::<Hierarchical>()
}

/// The view of a value, if it is one.
pub fn view_of(value: &ExportValue) -> Option<&Arc<ViewOf>> {
    value.downcast_ref::<Arc<ViewOf>>()
}

/// A hierarchical table as a value to export.
pub fn export_value(table: Hierarchical) -> ExportValue {
    ExportValue::object(table)
}

/// An engine refusal as a client is told it. Java throws an exception that nothing
/// catches, reported by its error id; what this engine has not ported is `UNIMPLEMENTED`
/// and names the Java method.
impl From<HierarchicalError> for ExportError {
    fn from(error: HierarchicalError) -> Self {
        let text = error.to_string();
        match text.strip_prefix("unported: ") {
            Some(missing) => Status::unimplemented(format!("not ported: {missing}")).into(),
            None => ExportError::Internal(text),
        }
    }
}

/// Behavior: the work of `HierarchicalTableServiceGrpcImpl#rollup`, which is
/// `io.deephaven.engine.table.impl.QueryTable#rollup`
pub fn make_rollup(
    context: &OpContext,
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    include_constituents: bool,
    group_by: &[String],
) -> Result<Hierarchical, ExportError> {
    let group_by: Vec<&str> = group_by.iter().map(String::as_str).collect();
    guarded(&[source], || {
        let rollup = rollup(
            source,
            aggregations,
            include_constituents,
            &group_by,
            Some(context.compiler.as_ref()),
        )?;
        Ok(Arc::new(HierarchicalTable::Rollup(Arc::new(rollup))))
    })
}

/// `source` as a tree by its identifier and parent columns; with `promote` the rows whose
/// parent is no row's identifier are children of the root.
///
/// Behavior: the work of `HierarchicalTableServiceGrpcImpl#tree`, which is
/// `io.deephaven.engine.table.hierarchical.TreeTable#promoteOrphans` and
/// `io.deephaven.engine.table.impl.QueryTable#tree`
pub fn make_tree(
    context: &OpContext,
    source: &Arc<dyn Table>,
    identifier_column: &str,
    parent_column: &str,
    promote: bool,
) -> Result<Hierarchical, ExportError> {
    guarded(&[source], || {
        let source = if promote {
            // Java asks the definition for the parent column's formula before anything
            // else, and so names a missing column as its `view` does
            promote_orphans(
                source,
                identifier_column,
                parent_column,
                context.compiler.as_ref(),
            )?
        } else {
            source.clone()
        };
        let tree = tree(&source, identifier_column, parent_column)?;
        Ok(Arc::new(HierarchicalTable::Tree(Arc::new(tree))))
    })
}

/// The sorts of an `Apply` request for the engine. A sort by an absolute value is, in
/// Java, a sort by a column named `__ABS__<name>`.
///
/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableGrpcHelper#translateAndValidateSorts`
fn translate_sorts(
    sorts: &[SortDescriptor],
    sortable: Option<&[String]>,
) -> Result<Vec<SortColumn>, Status> {
    let mut translated = Vec::with_capacity(sorts.len());
    for sort in sorts {
        let column = match SortDirection::try_from(sort.direction) {
            Ok(SortDirection::Ascending) => SortColumn::ascending(&sort.column_name),
            Ok(SortDirection::Descending) => SortColumn::descending(&sort.column_name),
            other => {
                let name = match other {
                    Ok(direction) => direction.as_str_name(),
                    Err(_) => "UNRECOGNIZED",
                };
                return Err(Status::invalid_argument(format!(
                    "Unsupported or unknown sort direction: {name}"
                )));
            }
        };
        translated.push(if sort.is_absolute {
            column.absolute()
        } else {
            column
        });
    }
    if let Some(sortable) = sortable {
        // Java reads the names from one string in which they are joined by commas, so
        // no name is one name, the empty one, and its list of names is never empty:
        // `Sorting is not supported on this hierarchical table` is not reached.
        let refused: Vec<&str> = translated
            .iter()
            .map(|sort| sort.column.as_str())
            .filter(|name| !sortable.iter().any(|allowed| allowed == name))
            .collect();
        if !refused.is_empty() {
            return Err(Status::invalid_argument(format!(
                "Sorting attempted on restricted column(s): [{}], available column(s) for \
                 sorting are: [{}]",
                refused.join(", "),
                sortable.join(", ")
            )));
        }
    }
    Ok(translated)
}

/// `table` with the filters and sorts of `request`: the filters first.
///
/// Behavior: the work of `HierarchicalTableServiceGrpcImpl#apply`, without its format
/// views and update views
pub fn apply(
    context: &OpContext,
    table: &Hierarchical,
    request: &HierarchicalTableApplyRequest,
) -> Result<Hierarchical, ExportError> {
    if request.filters.is_empty()
        && request.sorts.is_empty()
        && request.format_views.is_empty()
        && request.update_views.is_empty()
    {
        return Err(Status::invalid_argument("No operations specified").into());
    }
    // Java reads the conditions, then the sorts, before it applies anything.
    let conditions = conditions(&request.filters).map_err(ExportError::Internal)?;
    let sorts = translate_sorts(&request.sorts, table.sortable_columns())?;
    match table.as_ref() {
        HierarchicalTable::Rollup(rollup) => {
            apply_to_rollup(context, rollup, request, &conditions, &sorts)
        }
        HierarchicalTable::Tree(tree) => apply_to_tree(context, tree, request, &conditions, &sorts),
    }
}

/// Java reads neither the update views nor the format views of a request for a tree,
/// and answers as if it had none: a request with nothing else gives the tree back.
/// They are refused here, as a rollup's are, until it is ruled whether that is wanted.
///
/// Behavior: the work of `HierarchicalTableServiceGrpcImpl#apply` for a tree
fn apply_to_tree(
    context: &OpContext,
    tree: &Arc<TreeTable>,
    request: &HierarchicalTableApplyRequest,
    conditions: &[Condition],
    sorts: &[SortColumn],
) -> Result<Hierarchical, ExportError> {
    if !request.update_views.is_empty() || !request.format_views.is_empty() {
        return Err(not_ported(
            "io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl#apply",
            "update views and format views of a tree, which Java does not read",
        )
        .into());
    }
    let compiler = context.compiler.as_ref();
    let source = tree.source().clone();
    guarded(&[&source], || {
        let mut result = tree.clone();
        if !conditions.is_empty() {
            let filters = CheckedFilters::of_conditions(conditions, source.definition(), compiler)?;
            if !filters.is_empty() {
                result = Arc::new(result.with_filters(&filters, compiler)?);
            }
        }
        if !sorts.is_empty() {
            result.check_sorts(sorts)?;
            result = Arc::new(result.with_sorts(sorts));
        }
        Ok(Arc::new(HierarchicalTable::Tree(result)))
    })
}

/// Behavior: the work of `HierarchicalTableServiceGrpcImpl#apply` for a rollup
fn apply_to_rollup(
    context: &OpContext,
    rollup: &Arc<RollupTable>,
    request: &HierarchicalTableApplyRequest,
    conditions: &[Condition],
    sorts: &[SortColumn],
) -> Result<Hierarchical, ExportError> {
    if !request.update_views.is_empty() {
        return Err(not_ported(
            "io.deephaven.engine.table.impl.hierarchical.RollupNodeOperationsRecorder#updateView",
            "update views of a rollup",
        )
        .into());
    }
    if !request.format_views.is_empty() {
        return Err(not_ported(
            "io.deephaven.engine.table.impl.hierarchical.RollupNodeOperationsRecorder#formatColumns",
            "format views of a rollup",
        )
        .into());
    }
    let compiler = context.compiler.as_ref();
    let source = rollup.source().clone();
    guarded(&[&source], || {
        let mut result = rollup.clone();
        if !conditions.is_empty() {
            let filters = CheckedFilters::of_conditions(conditions, source.definition(), compiler)?;
            result
                .check_filters(&filters)
                .map_err(Status::invalid_argument)?;
            if !filters.is_empty() {
                result = Arc::new(result.with_filters(&filters, compiler)?);
            }
        }
        if !sorts.is_empty() {
            result.check_sorts(sorts)?;
            result = Arc::new(result.with_sorts(sorts));
        }
        Ok(Arc::new(HierarchicalTable::Rollup(result)))
    })
}

/// What Java says of an aggregation's result columns.
///
/// Behavior: `io.deephaven.api.agg.util.AggregationDescriptions` and the `description` of
/// each `io.deephaven.api.agg.spec.AggSpec`
fn spec_description(spec: &AggSpec) -> Option<String> {
    Some(match spec {
        AggSpec::Sum => "sum".to_string(),
        AggSpec::AbsSum => "absolute sum".to_string(),
        AggSpec::Avg => "average".to_string(),
        AggSpec::Min => "min".to_string(),
        AggSpec::Max => "max".to_string(),
        AggSpec::First => "first".to_string(),
        AggSpec::Last => "last".to_string(),
        AggSpec::Var => "variance".to_string(),
        AggSpec::Std => "standard deviation".to_string(),
        AggSpec::CountDistinct { count_nulls } => format!(
            "count distinct{}",
            if *count_nulls {
                " (counting nulls)"
            } else {
                ""
            }
        ),
        AggSpec::Unique { include_nulls, .. } => format!(
            "unique ({} nulls)",
            if *include_nulls {
                "including"
            } else {
                "excluding"
            }
        ),
        AggSpec::Distinct { include_nulls } => format!(
            "distinct{}",
            if *include_nulls {
                " (including nulls)"
            } else {
                ""
            }
        ),
        // Java appends the weight's `ColumnName`, whose text is `ColumnName(W)`.
        AggSpec::WeightedAvg { weight } => format!("average weighted by ColumnName({weight})"),
        AggSpec::WeightedSum { weight } => format!("sum weighted by ColumnName({weight})"),
        AggSpec::SortedFirst { columns } => format!("first sorted by {}", columns.join(", ")),
        AggSpec::SortedLast { columns } => format!("last sorted by {}", columns.join(", ")),
        // a rollup has none of these
        _ => return None,
    })
}

const PREFIX: &str = "deephaven:";

/// The schema of the snapshots of `table`, with what Java tells a client of each column.
///
/// Behavior: `io.deephaven.extensions.barrage.util.HierarchicalTableSchemaUtil#makeSchemaPayload`
pub fn snapshot_schema(table: &HierarchicalTable) -> Schema {
    match table {
        HierarchicalTable::Rollup(rollup) => rollup_schema(rollup),
        HierarchicalTable::Tree(tree) => tree_schema(tree),
    }
}

/// Behavior: `HierarchicalTableSchemaUtil#makeSchemaPayload` for a tree
fn tree_schema(tree: &TreeTable) -> Schema {
    let sortable = tree.sortable_columns();
    let context = SchemaContext {
        sortable: sortable.map(<[String]>::to_vec),
        ..SchemaContext::default()
    };
    let plain = arrow_schema(tree.columns(), &context);
    let fields: Vec<Field> = plain
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = field.name().as_str();
            let mut metadata = field.metadata().clone();
            let mut put = |key: &str| {
                metadata.insert(format!("{PREFIX}{key}"), "true".to_string());
            };
            if index == DEPTH_COLUMN || index == EXPANDED_COLUMN {
                put("hierarchicalTable.isStructuralColumn");
                if index == DEPTH_COLUMN {
                    put("hierarchicalTable.isRowDepthColumn");
                } else {
                    put("hierarchicalTable.isRowExpandedColumn");
                }
            } else {
                put("treeTable.isNodeColumn");
                // a tree by one column for both has both marks on it
                if name == tree.identifier_column() {
                    put("treeTable.isIdentifierColumn");
                    put("hierarchicalTable.isExpandByColumn");
                }
                if name == tree.parent_identifier_column() {
                    put("treeTable.isParentIdentifierColumn");
                }
            }
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect();
    let mut metadata = HashMap::new();
    if let Some(sortable) = sortable {
        metadata.insert(
            format!("{PREFIX}attribute.SortableColumns"),
            sortable.join(","),
        );
        metadata.insert(
            format!("{PREFIX}attribute_type.SortableColumns"),
            "java.lang.String".to_string(),
        );
    }
    Schema::new_with_metadata(fields, metadata)
}

/// Behavior: `HierarchicalTableSchemaUtil#makeSchemaPayload` for a rollup
fn rollup_schema(rollup: &RollupTable) -> Schema {
    let definition = rollup.columns();
    // the name of each result column's input, and its description
    let mut inputs = HashMap::new();
    let mut descriptions = HashMap::new();
    for aggregation in rollup.aggregations() {
        match aggregation {
            Aggregation::Columns { spec, pairs } => {
                for pair in pairs {
                    inputs.insert(pair.output.as_str(), pair.input.as_str());
                    if let Some(spec) = spec_description(spec) {
                        descriptions.insert(
                            pair.output.as_str(),
                            format!("{} aggregated with {spec}", pair.input),
                        );
                    }
                }
            }
            Aggregation::Count { column } => {
                descriptions.insert(column.as_str(), "count".to_string());
            }
            // a rollup has none of these
            _ => {}
        }
    }
    // Java asks the restriction of the rollup's sorting for every field, so that the
    // structural columns are sortable only without one.
    let sortable = rollup.sortable_columns();
    let context = SchemaContext {
        sortable: sortable.map(<[String]>::to_vec),
        ..SchemaContext::default()
    };
    let plain = arrow_schema(definition, &context);
    let fields: Vec<Field> = plain
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = field.name().as_str();
            let mut metadata = field.metadata().clone();
            let mut put = |key: &str, value: &str| {
                metadata.insert(format!("{PREFIX}{key}"), value.to_string());
            };
            let group_by = rollup.group_by().iter().any(|column| column == name);
            if index == DEPTH_COLUMN || index == EXPANDED_COLUMN {
                put("hierarchicalTable.isStructuralColumn", "true");
                if index == DEPTH_COLUMN {
                    put("hierarchicalTable.isRowDepthColumn", "true");
                    put("hierarchicalTable.isExpandByColumn", "true");
                } else {
                    put("hierarchicalTable.isRowExpandedColumn", "true");
                }
            } else {
                put("rollupTable.isAggregatedNodeColumn", "true");
                if group_by {
                    put("rollupTable.isGroupByColumn", "true");
                    put("hierarchicalTable.isExpandByColumn", "true");
                } else {
                    put(
                        "rollupTable.aggregationInputColumnName",
                        inputs.get(name).copied().unwrap_or(""),
                    );
                }
                if let Some(description) = descriptions.get(name) {
                    put("description", description);
                }
            }
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect();
    // Java cannot send the map of the descriptions and marks it as not sent.
    let mut metadata = HashMap::from([(
        format!("{PREFIX}unsent.attribute.ColumnDescriptions"),
        String::new(),
    )]);
    if let Some(sortable) = sortable {
        metadata.insert(
            format!("{PREFIX}attribute.SortableColumns"),
            sortable.join(","),
        );
        metadata.insert(
            format!("{PREFIX}attribute_type.SortableColumns"),
            "java.lang.String".to_string(),
        );
    }
    Schema::new_with_metadata(fields, metadata)
}

/// What a client that opens `table` is sent: the bytes of the
/// `HierarchicalTableDescriptor`.
///
/// Behavior: `HierarchicalTableTypePlugin#writeCompatibleObjectTo`
pub fn describe(table: &HierarchicalTable) -> Vec<u8> {
    HierarchicalTableDescriptor {
        snapshot_schema: schema_ipc_bytes(&snapshot_schema(table)),
        is_static: !table.source().is_refreshing(),
    }
    .encode_to_vec()
}

fn bytes(ticket: &Option<Ticket>) -> Vec<u8> {
    ticket
        .as_ref()
        .map(|ticket| ticket.ticket.clone())
        .unwrap_or_default()
}

/// Behavior: `GrpcErrorHelper#checkHasField` and `Common#validate(Ticket)`
fn ticket(
    ticket: &Option<Ticket>,
    message: &str,
    field: &str,
    number: u32,
) -> Result<Vec<u8>, Status> {
    let Some(ticket) = ticket else {
        return Err(missing_field(message, field, number));
    };
    if ticket.ticket.is_empty() {
        return Err(Status::invalid_argument("Ticket is empty"));
    }
    Ok(ticket.ticket.clone())
}

fn hierarchical_input(value: &ExportValue) -> Result<&Hierarchical, ExportError> {
    // Java: a ClassCastException, reported by its error id
    hierarchical_of(value)
        .ok_or_else(|| ExportError::Internal("the input is not a hierarchical table".into()))
}

fn table_input<'a>(value: &'a ExportValue, what: &str) -> Result<&'a Arc<dyn Table>, ExportError> {
    // Java: a ClassCastException, reported by its error id
    value
        .as_table()
        .ok_or_else(|| ExportError::Internal(format!("{what} is not a table")))
}

/// Behavior: `io.deephaven.server.hierarchicaltable.HierarchicalTableServiceGrpcImpl`
pub struct HierarchicalTableGrpc {
    router: Arc<TicketRouter>,
    context: OpContext,
}

impl HierarchicalTableGrpc {
    pub fn new(router: Arc<TicketRouter>, context: OpContext) -> Self {
        Self { router, context }
    }
}

#[tonic::async_trait]
impl HierarchicalTableService for HierarchicalTableGrpc {
    /// Behavior: `HierarchicalTableServiceGrpcImpl#rollup`
    async fn rollup(
        &self,
        request: Request<RollupRequest>,
    ) -> Result<Response<RollupResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let message = "RollupRequest";
        let result_id = ticket(
            &request.result_rollup_table_id,
            message,
            "result_rollup_table_id",
            1,
        )?;
        let source_id = ticket(&request.source_table_id, message, "source_table_id", 2)?;
        for aggregation in &request.aggregations {
            validate_aggregation(aggregation)?;
        }
        self.router.log_name(&source_id, "sourceTableId")?;
        let source = self
            .router
            .resolve(Some(&session), &source_id, "sourceTableId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let context = self.context.clone();
        session
            .new_export_for_ticket(&result_id, "resultRollupTableId")?
            .require([source])
            .on_error(move |status| fail(Err(status)))
            .on_success(move |_| succeed(Ok(RollupResponse {})))
            .submit(move |inputs| {
                let source = table_input(&inputs[0], "sourceTableId")?;
                let aggregations = adapt_aggregations(&request.aggregations)?;
                Ok(export_value(make_rollup(
                    &context,
                    source,
                    &aggregations,
                    request.include_constituents,
                    &request.group_by_columns,
                )?))
            })?;
        reply.wait().await
    }

    /// Behavior: `HierarchicalTableServiceGrpcImpl#tree`
    async fn tree(&self, request: Request<TreeRequest>) -> Result<Response<TreeResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let message = "TreeRequest";
        let result_id = ticket(
            &request.result_tree_table_id,
            message,
            "result_tree_table_id",
            1,
        )?;
        let source_id = ticket(&request.source_table_id, message, "source_table_id", 2)?;
        // Behavior: `GrpcErrorHelper#checkHasField`, for which a string that is empty is
        // a field that is not there
        if request.identifier_column.is_empty() {
            return Err(missing_field(message, "identifier_column", 3));
        }
        if request.parent_identifier_column.is_empty() {
            return Err(missing_field(message, "parent_identifier_column", 4));
        }
        self.router.log_name(&source_id, "sourceTableId")?;
        let source = self
            .router
            .resolve(Some(&session), &source_id, "sourceTableId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let context = self.context.clone();
        session
            .new_export_for_ticket(&result_id, "resultTreeTableId")?
            .require([source])
            .on_error(move |status| fail(Err(status)))
            .on_success(move |_| succeed(Ok(TreeResponse {})))
            .submit(move |inputs| {
                let source = table_input(&inputs[0], "sourceTableId")?;
                Ok(export_value(make_tree(
                    &context,
                    source,
                    &request.identifier_column,
                    &request.parent_identifier_column,
                    request.promote_orphans,
                )?))
            })?;
        reply.wait().await
    }

    /// Behavior: `HierarchicalTableServiceGrpcImpl#apply`
    async fn apply(
        &self,
        request: Request<HierarchicalTableApplyRequest>,
    ) -> Result<Response<HierarchicalTableApplyResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let message = "HierarchicalTableApplyRequest";
        let result_id = ticket(
            &request.result_hierarchical_table_id,
            message,
            "result_hierarchical_table_id",
            1,
        )?;
        let input_id = ticket(
            &request.input_hierarchical_table_id,
            message,
            "input_hierarchical_table_id",
            2,
        )?;
        self.router
            .log_name(&input_id, "inputHierarchicalTableId")?;
        let input = self
            .router
            .resolve(Some(&session), &input_id, "inputHierarchicalTableId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let context = self.context.clone();
        session
            .new_export_for_ticket(&result_id, "resultHierarchicalTableId")?
            .require([input])
            .on_error(move |status| fail(Err(status)))
            .on_success(move |_| succeed(Ok(HierarchicalTableApplyResponse {})))
            .submit(move |inputs| {
                let input = hierarchical_input(&inputs[0])?;
                Ok(export_value(apply(&context, input, &request)?))
            })?;
        reply.wait().await
    }

    /// Behavior: `HierarchicalTableServiceGrpcImpl#view`
    async fn view(
        &self,
        request: Request<HierarchicalTableViewRequest>,
    ) -> Result<Response<HierarchicalTableViewResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let message = "HierarchicalTableViewRequest";
        let result_id = ticket(&request.result_view_id, message, "result_view_id", 1)?;
        let (target_id, existing) = match &request.target {
            None => {
                return Err(Status::invalid_argument(
                    "io.deephaven.proto.backplane.grpc.HierarchicalTableViewRequest must have \
                     oneof target. Note: this may also indicate that the server is older than \
                     the client and doesn't know about this new oneof option.",
                ))
            }
            Some(Target::HierarchicalTableId(id)) => (id, false),
            Some(Target::ExistingViewId(id)) => (id, true),
        };
        if target_id.ticket.is_empty() {
            return Err(Status::invalid_argument("Ticket is empty"));
        }
        self.router.log_name(&target_id.ticket, "targetTableId")?;
        let export = session.new_export_for_ticket(&result_id, "resultViewId")?;
        let mut sources =
            vec![self
                .router
                .resolve(Some(&session), &target_id.ticket, "targetTableId")?];
        let expansions = request.expansions;
        if let Some(expansions) = &expansions {
            sources.push(self.router.resolve(
                Some(&session),
                &bytes(&expansions.key_table_id),
                "expansions.keyTableId",
            )?);
        }
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        export
            .require(sources)
            .on_error(move |status| fail(Err(status)))
            .on_success(move |_| succeed(Ok(HierarchicalTableViewResponse {})))
            .submit(move |inputs| {
                // Java: a ClassCastException for a target of the other kind, reported by
                // its error id
                let table = if existing {
                    view_of(&inputs[0])
                        .ok_or_else(|| {
                            ExportError::Internal("the target is not a view".to_string())
                        })?
                        .table
                        .clone()
                } else {
                    hierarchical_input(&inputs[0])?.clone()
                };
                let keys = match &expansions {
                    Some(expansions) => Some((
                        table_input(&inputs[1], "expansions.keyTableId")?.clone(),
                        expansions.key_table_action_column.clone(),
                    )),
                    None => None,
                };
                let (keys, action_column) = match keys {
                    Some((keys, action_column)) => (Some(keys), action_column),
                    None => (None, None),
                };
                let view = View::new(table.hierarchy(), keys, action_column);
                Ok(ExportValue::object(Arc::new(ViewOf { table, view })))
            })?;
        reply.wait().await
    }

    /// Behavior: `HierarchicalTableServiceGrpcImpl#exportSource`
    async fn export_source(
        &self,
        request: Request<HierarchicalTableSourceExportRequest>,
    ) -> Result<Response<ExportedTableCreationResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let message = "HierarchicalTableSourceExportRequest";
        let result_id = ticket(&request.result_table_id, message, "result_table_id", 1)?;
        let table_id = ticket(
            &request.hierarchical_table_id,
            message,
            "hierarchical_table_id",
            2,
        )?;
        self.router.log_name(&table_id, "hierarchicalTableId")?;
        let table = self
            .router
            .resolve(Some(&session), &table_id, "hierarchicalTableId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let described = Arc::new(Mutex::new(None));
        session
            .new_export_for_ticket(&result_id, "resultTableId")?
            .require([table])
            .on_error(move |status| fail(Err(status)))
            .on_success({
                let described = described.clone();
                move |_| {
                    let response = described
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take();
                    succeed(response.ok_or_else(|| {
                        Status::internal("the table was exported without a description")
                    }));
                }
            })
            .submit(move |inputs| {
                let source = hierarchical_input(&inputs[0])?.source();
                let reference = TableReference {
                    r#ref: Some(Ref::Ticket(Ticket { ticket: result_id })),
                };
                let response = table_creation_response(reference, source.as_ref());
                *described.lock().unwrap_or_else(PoisonError::into_inner) = Some(response);
                Ok(ExportValue::Table(source))
            })?;
        reply.wait().await
    }
}
