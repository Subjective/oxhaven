//! The table service: one-shot table operations, `Batch`, and the two calls that describe
//! exported tables.
//!
//! Every operation, one-shot or in a batch, goes through the registry in [`ops`]; an
//! operator task adds a handler there and nothing here. [`support`] holds what handlers
//! share.
//!
//! Behavior: `io.deephaven.server.table.ops.TableServiceGrpcImpl`

// AsOfJoin is deprecated in the protocol and still routed
#![allow(deprecated)]

pub mod ops;
pub mod support;
mod updates;

#[cfg(test)]
mod tests;

use crate::schema::table_schema_ipc_bytes;
use crate::session::{
    current_session, Export, ExportError, ExportValue, Reply, Session, TicketRouter, NON_EXPORT_ID,
};
use ops::{bind, BoundOp};
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::table::Table;
use oxhaven_proto::grpc::batch_table_request::operation::Op;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_server::TableService;
use oxhaven_proto::grpc::{
    AggregateAllRequest, AggregateRequest, AjRajTablesRequest, ApplyPreviewColumnsRequest,
    AsOfJoinTablesRequest, BatchTableRequest, ColumnStatisticsRequest, ComboAggregateRequest,
    CreateInputTableRequest, CrossJoinTablesRequest, DropColumnsRequest, EmptyTableRequest,
    ExactJoinTablesRequest, ExportedTableCreationResponse, ExportedTableUpdateMessage,
    ExportedTableUpdatesRequest, FetchTableRequest, FilterTableRequest, FlattenRequest,
    HeadOrTailByRequest, HeadOrTailRequest, LeftJoinTablesRequest, MergeTablesRequest,
    MetaTableRequest, MultiJoinTablesRequest, NaturalJoinTablesRequest, RangeJoinTablesRequest,
    RunChartDownsampleRequest, SeekRowRequest, SeekRowResponse, SelectDistinctRequest,
    SelectOrUpdateRequest, SliceRequest, SnapshotTableRequest, SnapshotWhenTableRequest,
    SortTableRequest, TableReference, Ticket, TimeTableRequest, UngroupRequest,
    UnstructuredFilterTableRequest, UpdateByRequest, WhereInRequest,
};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use support::{code_name, OpContext};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

type CreationResult = Result<Response<ExportedTableCreationResponse>, Status>;
type CreationStream = UnboundedReceiverStream<Result<ExportedTableCreationResponse, Status>>;

/// Describes `table` to the client that asked for it as `result_id`: its size, whether it
/// can change, and its schema.
///
/// Behavior: `io.deephaven.extensions.barrage.util.ExportUtil#buildTableCreationResponse`
pub fn table_creation_response(
    result_id: TableReference,
    table: &dyn Table,
) -> ExportedTableCreationResponse {
    let size = match table.update_graph() {
        Some(graph) => locked(graph.as_ref(), || table.row_set().size()),
        None => table.row_set().size(),
    };
    ExportedTableCreationResponse {
        result_id: Some(result_id),
        success: true,
        error_info: String::new(),
        schema_header: table_schema_ipc_bytes(table),
        is_static: !table.is_refreshing(),
        size: size as i64,
    }
}

fn ticket_reference(ticket: Vec<u8>) -> TableReference {
    TableReference {
        r#ref: Some(Ref::Ticket(Ticket { ticket })),
    }
}

/// Runs a bound operation on the values of its dependencies.
///
/// If a source is refreshing the handler runs under the update graph's shared lock, as the
/// Java handlers do for themselves.
fn run_operation(
    operation: &dyn BoundOp,
    context: &OpContext,
    values: &[ExportValue],
) -> Result<Arc<dyn Table>, ExportError> {
    let sources = values
        .iter()
        .map(|value| {
            // Java casts and fails with a ClassCastException
            value.as_table().cloned().ok_or_else(|| {
                ExportError::Internal("a source of a table operation is not a table".to_string())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let graph = sources
        .iter()
        .filter(|source| source.is_refreshing())
        .find_map(|source| source.update_graph());
    // An operator that meets something it has not ported panics with `unported!`. The
    // client is told what is missing; any other panic is left to the export, which reports
    // it as an internal failure.
    let create = || {
        catch_unwind(AssertUnwindSafe(|| operation.create(context, &sources))).unwrap_or_else(
            |panic| match support::unported_status(panic.as_ref()) {
                Some(status) => Err(ExportError::Status(status)),
                None => resume_unwind(panic),
            },
        )
    };
    match graph {
        Some(graph) => locked(graph.as_ref(), create),
        None => create(),
    }
}

/// Runs one operation for a caller inside the server, such as a script session, on tables
/// it holds: the operation's handler runs with the checks and under the lock it has for a
/// client's request, without a session, an export or a result ticket. A table reference of
/// the operation names a table by its position in `tables`, as a batch offset.
///
/// No Java counterpart: Java's script sessions call the engine directly.
#[cfg_attr(not(feature = "python"), allow(dead_code))]
pub(crate) fn run_local(
    context: &OpContext,
    op: Op,
    tables: &[Arc<dyn Table>],
) -> Result<Arc<dyn Table>, ExportError> {
    run_local_as(context, op, tables, true)
}

/// Runs `op` as [`run_local`] does, without the checks of the request's shape that the
/// table service makes before it calls the engine: for a call that Java's engine answers
/// and its table service refuses, which a script makes of the engine.
///
/// No Java counterpart: Java's script sessions call the engine directly.
#[cfg_attr(not(feature = "python"), allow(dead_code))]
pub(crate) fn run_local_unchecked(
    context: &OpContext,
    op: Op,
    tables: &[Arc<dyn Table>],
) -> Result<Arc<dyn Table>, ExportError> {
    run_local_as(context, op, tables, false)
}

#[cfg_attr(not(feature = "python"), allow(dead_code))]
fn run_local_as(
    context: &OpContext,
    op: Op,
    tables: &[Arc<dyn Table>],
    validate: bool,
) -> Result<Arc<dyn Table>, ExportError> {
    let operation = bind(op)?;
    if validate {
        operation.validate()?;
    }
    let sources = operation
        .sources()
        .iter()
        .map(|reference| {
            let table = match &reference.r#ref {
                Some(Ref::BatchOffset(offset)) => {
                    usize::try_from(*offset).ok().and_then(|i| tables.get(i))
                }
                _ => None,
            };
            table.cloned().map(ExportValue::Table).ok_or_else(|| {
                Status::failed_precondition(
                    "a table reference of a local operation must be the offset of one of its \
                     tables",
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    run_operation(operation.as_ref(), context, &sources)
}

/// The table service.
pub struct TableGrpc {
    router: Arc<TicketRouter>,
    context: OpContext,
}

impl TableGrpc {
    pub fn new(router: Arc<TicketRouter>, context: OpContext) -> Self {
        Self { router, context }
    }

    /// Runs one operation as its own RPC: the result is exported under the request's
    /// result ticket and described in the reply.
    ///
    /// Behavior: `TableServiceGrpcImpl#oneShotOperationWrapper`
    async fn one_shot<T>(&self, request: Request<T>, wrap: fn(T) -> Op) -> CreationResult {
        let session = current_session(&request)?;
        let operation = bind(wrap(request.into_inner()))?;
        let result_id = operation.result_id().to_vec();
        if result_id.is_empty() {
            return Err(Status::failed_precondition("No result ticket supplied"));
        }
        operation.validate()?;
        let dependencies = operation
            .sources()
            .iter()
            .map(|reference| match &reference.r#ref {
                Some(Ref::Ticket(ticket)) => {
                    self.router
                        .resolve(Some(&session), &ticket.ticket, "sourceId")
                }
                _ => Err(Status::failed_precondition(
                    "One-shot operations must use ticket references",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let reply = Reply::new();
        let fail = reply.completer();
        let succeed = reply.completer();
        // Built by the work, where a failure can still reach the client, and sent by the
        // success handler.
        let described = Arc::new(Mutex::new(None));
        let context = self.context.clone();
        session
            .new_export_for_ticket(&result_id, "resultId")?
            .require(dependencies)
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
            .submit(move |values| {
                let table = run_operation(operation.as_ref(), &context, &values)?;
                let response = table_creation_response(ticket_reference(result_id), table.as_ref());
                *described.lock().unwrap_or_else(PoisonError::into_inner) = Some(response);
                Ok(ExportValue::Table(table))
            })?;
        reply.wait().await
    }

    /// Behavior: `TableServiceGrpcImpl#batch`
    fn run_batch(
        &self,
        session: &Arc<Session>,
        request: BatchTableRequest,
    ) -> Result<CreationStream, Status> {
        // step 1: an export for every operation, so that operations can refer to each
        // other whatever their order. Each is claimed here, so a batch that is refused in
        // this step or the next leaves the exports it has claimed defined and unresolved,
        // and has run none of its operations.
        let mut operations = Vec::with_capacity(request.ops.len());
        let mut builders = Vec::with_capacity(request.ops.len());
        for operation in request.ops {
            let Some(op) = operation.op else {
                return Err(Status::invalid_argument(MISSING_OP));
            };
            let operation = bind(op)?;
            operation.validate()?;
            let result_id = operation.result_id();
            let mut builder = if result_id.is_empty() {
                session.non_export()?
            } else {
                session.new_export_for_ticket(result_id, "resultId")?
            };
            builder.claim()?;
            builders.push(builder);
            operations.push(operation);
        }

        // step 2: dependencies, by ticket or by position in this batch
        let mut dependencies = Vec::with_capacity(operations.len());
        for operation in &operations {
            let resolved = operation
                .sources()
                .iter()
                .map(|reference| self.resolve_batch_reference(session, &builders, reference))
                .collect::<Result<Vec<Export>, Status>>()?;
            dependencies.push(resolved);
        }

        // step 3: Java does not look for cycles either; operations in a cycle never finish

        // step 4: submit; the stream ends when every operation has resolved
        let (sender, receiver) = mpsc::unbounded_channel();
        let batch = Arc::new(Batch {
            sender: Mutex::new(Some(sender)),
            remaining: AtomicUsize::new(1 + operations.len()),
            first_failure: Mutex::new(None),
        });
        let submissions = operations.into_iter().zip(builders).zip(dependencies);
        for (offset, ((operation, builder), dependencies)) in submissions.enumerate() {
            let export = builder.export().clone();
            let result_id = if export.id() == NON_EXPORT_ID {
                TableReference {
                    r#ref: Some(Ref::BatchOffset(offset as i32)),
                }
            } else {
                ticket_reference(export.ticket().ticket)
            };
            let described = Arc::new(Mutex::new(None));
            let context = self.context.clone();
            builder
                .require(dependencies)
                .on_error({
                    let batch = batch.clone();
                    let result_id = result_id.clone();
                    move |_| {
                        batch.send(failure_response(result_id, &export, &batch));
                        batch.one_resolved();
                    }
                })
                .on_success({
                    let batch = batch.clone();
                    let described = described.clone();
                    move |_| {
                        let response = described
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .take();
                        if let Some(response) = response {
                            batch.send(response);
                        }
                        batch.one_resolved();
                    }
                })
                .submit(move |values| {
                    let table = run_operation(operation.as_ref(), &context, &values)?;
                    let response = table_creation_response(result_id, table.as_ref());
                    *described.lock().unwrap_or_else(PoisonError::into_inner) = Some(response);
                    Ok(ExportValue::Table(table))
                })?;
        }
        batch.one_resolved();
        Ok(UnboundedReceiverStream::new(receiver))
    }

    /// Behavior: `TableServiceGrpcImpl#resolveBatchReference`
    fn resolve_batch_reference(
        &self,
        session: &Arc<Session>,
        builders: &[crate::session::ExportBuilder],
        reference: &TableReference,
    ) -> Result<Export, Status> {
        match &reference.r#ref {
            Some(Ref::Ticket(ticket)) => {
                self.router
                    .resolve(Some(session), &ticket.ticket, "sourceId")
            }
            Some(Ref::BatchOffset(offset)) => usize::try_from(*offset)
                .ok()
                .and_then(|offset| builders.get(offset))
                .map(|builder| builder.export().clone())
                .ok_or_else(|| {
                    // the reference as protobuf's text format prints it
                    Status::invalid_argument(format!(
                        "invalid table reference: batch_offset: {offset}\n"
                    ))
                }),
            // The reference as protobuf's text format prints it is empty, so the message
            // ends in a space: the transport resets the stream instead of sending it
            // (`crate::transport`), and the client sees `CANCELLED`.
            None => Err(Status::invalid_argument("invalid table reference: ")),
        }
    }
}

const NO_OPS: &str =
    "io.deephaven.proto.backplane.grpc.BatchTableRequest must have at least one ops (1)";
const MISSING_OP: &str = "io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation must \
    have oneof op. Note: this may also indicate that the server is older than the client and \
    doesn't know about this new oneof option.";

/// The stream of one `Batch` call and what is left to send on it.
struct Batch {
    sender: Mutex<Option<mpsc::UnboundedSender<Result<ExportedTableCreationResponse, Status>>>>,
    /// Operations not yet resolved, plus one for the submission loop itself.
    remaining: AtomicUsize,
    /// The first failure that carries a status; it ends the stream.
    first_failure: Mutex<Option<Status>>,
}

impl Batch {
    fn send(&self, response: ExportedTableCreationResponse) {
        if let Some(sender) = &*self.sender.lock().unwrap_or_else(PoisonError::into_inner) {
            let _ = sender.send(Ok(response));
        }
    }

    /// Ends the stream once nothing is left: with the first failure if there was one.
    fn one_resolved(&self) {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let failure = self
            .first_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let (Some(sender), Some(failure)) = (sender, failure) {
            let _ = sender.send(Err(failure));
        }
    }
}

/// The response for a batch operation that failed: the error id, the dependency that
/// failed it if that is the reason, and the cause if it has a status.
///
/// Behavior: the `onError` handler in `TableServiceGrpcImpl#batch`
fn failure_response(
    result_id: TableReference,
    export: &Export,
    batch: &Batch,
) -> ExportedTableCreationResponse {
    let mut error_info = String::new();
    if let Some(failure) = export.failure() {
        error_info.push_str(&failure.error_id);
        if let Some(dependency) = &failure.dependency {
            error_info.push_str(" dependency: ");
            error_info.push_str(dependency);
        }
        if let Some(cause) = failure.cause {
            // the message of Java's StatusRuntimeException
            error_info.push_str(&format!(
                " cause: {}: {}",
                code_name(cause.code()),
                cause.message()
            ));
            batch
                .first_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert(cause);
        }
    }
    ExportedTableCreationResponse {
        result_id: Some(result_id),
        success: false,
        error_info,
        schema_header: Vec::new(),
        is_static: false,
        size: 0,
    }
}

/// Generates the service implementation: each one-shot RPC wraps its request in the batch
/// operation of the same meaning and runs it through [`TableGrpc::one_shot`].
macro_rules! table_service {
    ($( $method:ident($request:ty) => $variant:ident; )*) => {
        #[tonic::async_trait]
        impl TableService for TableGrpc {
            $(
                async fn $method(&self, request: Request<$request>) -> CreationResult {
                    self.one_shot(request, Op::$variant).await
                }
            )*

            type BatchStream = CreationStream;

            async fn batch(
                &self,
                request: Request<BatchTableRequest>,
            ) -> Result<Response<Self::BatchStream>, Status> {
                if request.get_ref().ops.is_empty() {
                    return Err(Status::invalid_argument(NO_OPS));
                }
                if request.get_ref().ops.iter().any(|operation| operation.op.is_none()) {
                    return Err(Status::invalid_argument(MISSING_OP));
                }
                let session = current_session(&request)?;
                self.run_batch(&session, request.into_inner()).map(Response::new)
            }

            type ExportedTableUpdatesStream =
                UnboundedReceiverStream<Result<ExportedTableUpdateMessage, Status>>;

            /// Behavior: `TableServiceGrpcImpl#exportedTableUpdates`
            async fn exported_table_updates(
                &self,
                request: Request<ExportedTableUpdatesRequest>,
            ) -> Result<Response<Self::ExportedTableUpdatesStream>, Status> {
                let session = current_session(&request)?;
                updates::subscribe(&session).map(Response::new)
            }

            /// Behavior: `TableServiceGrpcImpl#getExportedTableCreationResponse`
            async fn get_exported_table_creation_response(
                &self,
                request: Request<Ticket>,
            ) -> CreationResult {
                let session = current_session(&request)?;
                let ticket = request.into_inner();
                if ticket.ticket.is_empty() {
                    return Err(Status::failed_precondition("No request ticket supplied"));
                }
                let export = self.router.resolve(Some(&session), &ticket.ticket, "request")?;

                let reply = Reply::new();
                let fail = reply.completer();
                let succeed = reply.completer();
                session
                    .non_export()?
                    .require([export])
                    .on_error(move |status| fail(Err(status)))
                    .on_success(move |value| {
                        succeed(
                            value
                                .downcast_ref::<ExportedTableCreationResponse>()
                                .cloned()
                                .ok_or_else(|| Status::internal("no table description")),
                        )
                    })
                    .submit(move |values| {
                        let Some(table) = values.first().and_then(ExportValue::as_table) else {
                            return Err(Status::failed_precondition("Ticket is not a table").into());
                        };
                        let reference = ticket_reference(ticket.ticket);
                        Ok(ExportValue::object(table_creation_response(
                            reference,
                            table.as_ref(),
                        )))
                    })?;
                reply.wait().await
            }

            /// Behavior: `TableServiceGrpcImpl#seekRow`
            async fn seek_row(
                &self,
                request: Request<SeekRowRequest>,
            ) -> Result<Response<SeekRowResponse>, Status> {
                let session = current_session(&request)?;
                let request = request.into_inner();
                let ticket = request.source_id.clone().unwrap_or_default();
                if ticket.ticket.is_empty() {
                    return Err(Status::failed_precondition("No consoleId supplied"));
                }
                let export = self.router.resolve(Some(&session), &ticket.ticket, "sourceId")?;

                let reply = Reply::new();
                let fail = reply.completer();
                let succeed = reply.completer();
                session
                    .non_export()?
                    .require([export])
                    .on_error(move |status| fail(Err(status)))
                    .on_success(move |value| {
                        succeed(
                            value
                                .downcast_ref::<SeekRowResponse>()
                                .cloned()
                                .ok_or_else(|| Status::internal("no row position")),
                        )
                    })
                    .submit(move |values| {
                        // Java casts and fails with a ClassCastException
                        let Some(table) = values.first().and_then(ExportValue::as_table) else {
                            return Err(ExportError::Internal(
                                "the source of SeekRow is not a table".to_string(),
                            ));
                        };
                        let result_row = ops::seek_row(table, &request)?;
                        Ok(ExportValue::object(SeekRowResponse { result_row }))
                    })?;
                reply.wait().await
            }
        }
    };
}

table_service! {
    fetch_table(FetchTableRequest) => FetchTable;
    apply_preview_columns(ApplyPreviewColumnsRequest) => ApplyPreviewColumns;
    empty_table(EmptyTableRequest) => EmptyTable;
    time_table(TimeTableRequest) => TimeTable;
    drop_columns(DropColumnsRequest) => DropColumns;
    update(SelectOrUpdateRequest) => Update;
    lazy_update(SelectOrUpdateRequest) => LazyUpdate;
    view(SelectOrUpdateRequest) => View;
    update_view(SelectOrUpdateRequest) => UpdateView;
    select(SelectOrUpdateRequest) => Select;
    update_by(UpdateByRequest) => UpdateBy;
    select_distinct(SelectDistinctRequest) => SelectDistinct;
    filter(FilterTableRequest) => Filter;
    unstructured_filter(UnstructuredFilterTableRequest) => UnstructuredFilter;
    sort(SortTableRequest) => Sort;
    head(HeadOrTailRequest) => Head;
    tail(HeadOrTailRequest) => Tail;
    head_by(HeadOrTailByRequest) => HeadBy;
    tail_by(HeadOrTailByRequest) => TailBy;
    ungroup(UngroupRequest) => Ungroup;
    merge_tables(MergeTablesRequest) => Merge;
    cross_join_tables(CrossJoinTablesRequest) => CrossJoin;
    natural_join_tables(NaturalJoinTablesRequest) => NaturalJoin;
    exact_join_tables(ExactJoinTablesRequest) => ExactJoin;
    left_join_tables(LeftJoinTablesRequest) => LeftJoin;
    as_of_join_tables(AsOfJoinTablesRequest) => AsOfJoin;
    aj_tables(AjRajTablesRequest) => Aj;
    raj_tables(AjRajTablesRequest) => Raj;
    multi_join_tables(MultiJoinTablesRequest) => MultiJoin;
    range_join_tables(RangeJoinTablesRequest) => RangeJoin;
    combo_aggregate(ComboAggregateRequest) => ComboAggregate;
    aggregate_all(AggregateAllRequest) => AggregateAll;
    aggregate(AggregateRequest) => Aggregate;
    snapshot(SnapshotTableRequest) => Snapshot;
    snapshot_when(SnapshotWhenTableRequest) => SnapshotWhen;
    flatten(FlattenRequest) => Flatten;
    run_chart_downsample(RunChartDownsampleRequest) => RunChartDownsample;
    create_input_table(CreateInputTableRequest) => CreateInputTable;
    where_in(WhereInRequest) => WhereIn;
    meta_table(MetaTableRequest) => MetaTable;
    compute_column_statistics(ColumnStatisticsRequest) => ColumnStatistics;
    slice(SliceRequest) => Slice;
}
