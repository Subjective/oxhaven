//! Partitioned tables: the `PartitionedTableService`, and what a client is told about a
//! partitioned table it opens.
//!
//! A partitioned table is exported as an object ([`ExportValue::object`]) that holds a
//! [`Partitioned`]; [`partitioned_of`] reads it back. `docs/PARTITIONED.md` is the design.

#[cfg(test)]
mod tests;

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, PoisonError};

use oxhaven_contracts::graph::locked;
use oxhaven_contracts::table::Table;
use oxhaven_engine::ops::partitioned::{partition_by, PartitionedError, PartitionedTable};
use oxhaven_proto::grpc::get_table_request::UniqueBehavior;
use oxhaven_proto::grpc::partitioned_table_service_server::PartitionedTableService;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::{
    ExportedTableCreationResponse, GetTableRequest, MergeRequest, PartitionByRequest,
    PartitionByResponse, PartitionedTableDescriptor, TableReference, Ticket,
};
use prost::Message;
use tonic::{Request, Response, Status};

use crate::schema::table_schema_ipc_bytes;
use crate::session::{current_session, ExportError, ExportValue, Reply, TicketRouter};
use crate::table::support::unported_status;
use crate::table::table_creation_response;

/// A partitioned table as exports and variables hold it.
pub type Partitioned = Arc<PartitionedTable>;

/// The name clients know the type of a partitioned table by.
///
/// Behavior: `io.deephaven.partitionedtable.PartitionedTableTypePlugin#name`
pub const TYPE_NAME: &str = "PartitionedTable";

/// The partitioned table of a value, if it is one.
pub fn partitioned_of(value: &ExportValue) -> Option<&Partitioned> {
    value.downcast_ref::<Partitioned>()
}

/// A partitioned table as a value to export.
pub fn export_value(partitioned: Partitioned) -> ExportValue {
    ExportValue::object(partitioned)
}

/// An engine refusal as a client is told it. Java throws an exception that nothing
/// catches, reported by its error id; what this engine has not ported is `UNIMPLEMENTED`
/// and names the Java method.
impl From<PartitionedError> for ExportError {
    fn from(error: PartitionedError) -> Self {
        let text = error.to_string();
        match text.strip_prefix("unported: ") {
            Some(missing) => Status::unimplemented(format!("not ported: {missing}")).into(),
            None => ExportError::Internal(text),
        }
    }
}

/// Runs `work` under the update graph's shared lock if one of `tables` is refreshing, and
/// answers an `unported!` panic as `UNIMPLEMENTED`, as the table service does for its
/// handlers.
pub fn guarded<T>(
    tables: &[&Arc<dyn Table>],
    work: impl FnOnce() -> Result<T, ExportError>,
) -> Result<T, ExportError> {
    let graph = tables
        .iter()
        .filter(|table| table.is_refreshing())
        .find_map(|table| table.update_graph());
    let work = || {
        catch_unwind(AssertUnwindSafe(work)).unwrap_or_else(|panic| {
            match unported_status(panic.as_ref()) {
                Some(status) => Err(status.into()),
                None => resume_unwind(panic),
            }
        })
    };
    match graph {
        Some(graph) => locked(graph.as_ref(), work),
        None => work(),
    }
}

/// Behavior: the work of `PartitionedTableServiceGrpcImpl#partitionBy`, which is
/// `io.deephaven.engine.table.impl.QueryTable#partitionBy`
pub fn partition(
    table: &Arc<dyn Table>,
    key_columns: &[String],
    drop_keys: bool,
) -> Result<Partitioned, ExportError> {
    let keys: Vec<&str> = key_columns.iter().map(String::as_str).collect();
    guarded(&[table], || {
        Ok(Arc::new(partition_by(table, &keys, drop_keys)?))
    })
}

/// Behavior: the work of `PartitionedTableServiceGrpcImpl#merge`
pub fn merge(partitioned: &Partitioned) -> Result<Arc<dyn Table>, ExportError> {
    guarded(&[partitioned.table()], || Ok(partitioned.merge()?))
}

/// The constituent of the one key of `keys`, or with `unique` false the merge of the
/// constituents of all its keys.
///
/// Behavior: `PartitionedTableServiceGrpcImpl#lockAndGetConstituents` and
/// `#getConstituents`
pub fn constituents_for(
    partitioned: &Partitioned,
    keys: &Arc<dyn Table>,
    unique: bool,
) -> Result<Arc<dyn Table>, ExportError> {
    guarded(&[partitioned.table(), keys], || {
        if unique {
            let size = keys.row_set().size();
            if size != 1 {
                return Err(Status::invalid_argument(format!(
                    "Provided key table does not have one row, instead has {size}"
                ))
                .into());
            }
        }
        let rows = partitioned.rows_for_keys(keys)?;
        let requested = partitioned.with_table(rows.clone())?;
        if !unique {
            return Ok(requested.merge()?);
        }
        let size = rows.row_set().size();
        if size != 1 {
            return Err(Status::invalid_argument(format!(
                "Filtered PartitionedTable has more than one constituent, {size} constituents \
                 found"
            ))
            .into());
        }
        let found = requested.constituents()?.pop();
        // one row was counted under the same lock
        found.ok_or_else(|| ExportError::Internal("the constituent is gone".to_string()))
    })
}

/// What a client that opens `partitioned` is sent: the bytes of the
/// `PartitionedTableDescriptor`, and the table it is to be given a reference to.
///
/// Behavior: `PartitionedTableTypePlugin#writeCompatibleObjectTo`
pub fn describe(partitioned: &Partitioned) -> Result<(Vec<u8>, Arc<dyn Table>), ExportError> {
    let empty = partitioned.empty_constituent()?;
    let descriptor = PartitionedTableDescriptor {
        key_column_names: partitioned.key_columns().to_vec(),
        constituent_column_name: partitioned.constituent_column().to_string(),
        unique_keys: partitioned.unique_keys(),
        constituent_definition_schema: table_schema_ipc_bytes(empty.as_ref()),
        constituent_changes_permitted: partitioned.constituent_changes_permitted(),
    };
    Ok((descriptor.encode_to_vec(), partitioned.table().clone()))
}

fn partitioned_input(value: &ExportValue) -> Result<&Partitioned, ExportError> {
    // Java: a ClassCastException, reported by its error id
    partitioned_of(value)
        .ok_or_else(|| ExportError::Internal("partitionedTable is not a partitioned table".into()))
}

fn table_input<'a>(value: &'a ExportValue, what: &str) -> Result<&'a Arc<dyn Table>, ExportError> {
    // Java: a ClassCastException, reported by its error id
    value
        .as_table()
        .ok_or_else(|| ExportError::Internal(format!("{what} is not a table")))
}

fn bytes(ticket: Option<Ticket>) -> Vec<u8> {
    ticket.map(|ticket| ticket.ticket).unwrap_or_default()
}

/// Behavior: `io.deephaven.server.partitionedtable.PartitionedTableServiceGrpcImpl`
pub struct PartitionedTableGrpc {
    router: Arc<TicketRouter>,
}

impl PartitionedTableGrpc {
    pub fn new(router: Arc<TicketRouter>) -> Self {
        Self { router }
    }

    /// Exports the table `work` makes from the values of `sources` as `result_id`, and
    /// answers with its description.
    async fn table_call<T>(
        &self,
        request: &Request<T>,
        sources: &[(Vec<u8>, &'static str)],
        result_id: Vec<u8>,
        work: impl FnOnce(&[ExportValue]) -> Result<Arc<dyn Table>, ExportError> + Send + 'static,
    ) -> Result<Response<ExportedTableCreationResponse>, Status> {
        let session = current_session(request)?;
        for (ticket, log_id) in sources {
            self.router.log_name(ticket, log_id)?;
        }
        let mut exports = Vec::with_capacity(sources.len());
        for (ticket, log_id) in sources {
            exports.push(self.router.resolve(Some(&session), ticket, log_id)?);
        }
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let described = Arc::new(Mutex::new(None));
        session
            .new_export_for_ticket(&result_id, "resultId")?
            .require(exports)
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
                let table = work(&inputs)?;
                let reference = TableReference {
                    r#ref: Some(Ref::Ticket(Ticket { ticket: result_id })),
                };
                let response = table_creation_response(reference, table.as_ref());
                *described.lock().unwrap_or_else(PoisonError::into_inner) = Some(response);
                Ok(ExportValue::Table(table))
            })?;
        reply.wait().await
    }
}

#[tonic::async_trait]
impl PartitionedTableService for PartitionedTableGrpc {
    /// Behavior: `PartitionedTableServiceGrpcImpl#partitionBy`
    async fn partition_by(
        &self,
        request: Request<PartitionByRequest>,
    ) -> Result<Response<PartitionByResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let table_id = bytes(request.table_id);
        self.router.log_name(&table_id, "tableId")?;
        let table = self.router.resolve(Some(&session), &table_id, "tableId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let (keys, drop_keys) = (request.key_column_names, request.drop_keys);
        session
            .new_export_for_ticket(&bytes(request.result_id), "resultId")?
            .require([table])
            .on_error(move |status| fail(Err(status)))
            .on_success(move |_| succeed(Ok(PartitionByResponse {})))
            .submit(move |inputs| {
                let table = table_input(&inputs[0], "tableId")?;
                Ok(export_value(partition(table, &keys, drop_keys)?))
            })?;
        reply.wait().await
    }

    /// Behavior: `PartitionedTableServiceGrpcImpl#merge`
    async fn merge(
        &self,
        request: Request<MergeRequest>,
    ) -> Result<Response<ExportedTableCreationResponse>, Status> {
        let message = request.get_ref();
        let sources = [(bytes(message.partitioned_table.clone()), "partitionedTable")];
        let result_id = bytes(message.result_id.clone());
        self.table_call(&request, &sources, result_id, |inputs| {
            merge(partitioned_input(&inputs[0])?)
        })
        .await
    }

    /// Behavior: `PartitionedTableServiceGrpcImpl#getTable`
    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<ExportedTableCreationResponse>, Status> {
        let message = request.get_ref();
        let sources = [
            (bytes(message.partitioned_table.clone()), "partitionedTable"),
            (bytes(message.key_table_ticket.clone()), "keyTable"),
        ];
        let result_id = bytes(message.result_id.clone());
        let behavior = message.unique_behavior;
        self.table_call(&request, &sources, result_id, move |inputs| {
            let partitioned = partitioned_input(&inputs[0])?;
            let keys = table_input(&inputs[1], "keyTable")?;
            let unique = match UniqueBehavior::try_from(behavior) {
                Ok(UniqueBehavior::NotSetUniqueBehavior)
                | Ok(UniqueBehavior::RequireUniqueResultsStaticSingleKey) => true,
                Ok(UniqueBehavior::PermitMultipleKeys) => false,
                Err(_) => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid unique behavior {behavior}"
                    ))
                    .into())
                }
            };
            constituents_for(partitioned, keys, unique)
        })
        .await
    }
}
