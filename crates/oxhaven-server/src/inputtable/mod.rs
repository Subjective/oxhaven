//! Input tables: creating one, and the service that adds rows to one and deletes rows
//! from one.
//!
//! `CreateInputTable` is an operation of the table service; [`create_input_table`] is what
//! it calls. [`InputTableGrpc`] is the `InputTableService`. Both calls of the service answer
//! once the change is visible in the input table. Tables derived from it may still be
//! updating in the same cycle at that moment, as in Java.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{InputTable, Table};
use oxhaven_engine::table::{append_only_input_table, blink_input_table, keyed_input_table};
use oxhaven_proto::grpc::create_input_table_request::input_table_kind::Kind;
use oxhaven_proto::grpc::create_input_table_request::Definition;
use oxhaven_proto::grpc::input_table_service_server::InputTableService;
use oxhaven_proto::grpc::{
    AddTableRequest, AddTableResponse, CreateInputTableRequest, DeleteTableRequest,
    DeleteTableResponse, Ticket,
};
use tonic::{Request, Response, Status};

use crate::flight::{convert_schema, not_ported, schema_from_ipc_bytes, snapshot, UploadError};
use crate::session::{current_session, ExportError, ExportValue, Reply, TicketRouter};

/// Checks a `CreateInputTableRequest` before its source table is looked up.
///
/// Behavior: `io.deephaven.server.table.ops.CreateInputTableGrpcImpl#validateRequest`
pub fn validate_create_input_table(request: &CreateInputTableRequest) -> Result<(), Status> {
    if request.definition.is_none() {
        return Err(Status::invalid_argument(
            "Must specify one of schema and source_table_id",
        ));
    }
    if request
        .kind
        .as_ref()
        .and_then(|kind| kind.kind.as_ref())
        .is_none()
    {
        return Err(Status::invalid_argument("Unrecognized InputTableKind"));
    }
    Ok(())
}

/// Makes the input table a `CreateInputTableRequest` asks for, driven by `graph`.
///
/// `source` is the table `source_table_id` names, resolved by the caller; it is required
/// when the request has no schema. Only its columns are used, not its rows. The result has
/// no rows and reports `Table::input_table`.
///
/// Answers `UNIMPLEMENTED` for what the engine's input tables lack: a keyed table without
/// key columns.
///
/// Behavior: `io.deephaven.server.table.ops.CreateInputTableGrpcImpl#create`
pub fn create_input_table(
    graph: Arc<dyn UpdateGraph>,
    request: &CreateInputTableRequest,
    source: Option<&Arc<dyn Table>>,
) -> Result<Arc<dyn Table>, Status> {
    validate_create_input_table(request)?;
    let definition: TableDefinition = match (&request.definition, source) {
        (Some(Definition::Schema(bytes)), _) => {
            convert_schema(&schema_from_ipc_bytes(bytes)?).map_err(UploadError::into_status)?
        }
        (_, Some(source)) => source.definition().clone(),
        _ => {
            return Err(Status::invalid_argument(
                "missing schema and source_table_id",
            ))
        }
    };
    const MAKE_KEYED: &str = "io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable#make";
    let made = match request.kind.as_ref().and_then(|kind| kind.kind.as_ref()) {
        Some(Kind::InMemoryAppendOnly(_)) => append_only_input_table(graph, definition, None),
        Some(Kind::InMemoryKeyBacked(keyed)) => {
            if keyed.key_columns.is_empty() {
                return Err(not_ported(
                    MAKE_KEYED,
                    "a keyed input table without key columns",
                ));
            }
            let keys: Vec<&str> = keyed.key_columns.iter().map(String::as_str).collect();
            keyed_input_table(graph, definition, &keys, None)
        }
        // Behavior: io.deephaven.stream.TablePublisher#of and #inputTable
        Some(Kind::Blink(_)) => Ok(blink_input_table(graph, definition)),
        None => return Err(Status::invalid_argument("Unrecognized InputTableKind")),
    };
    // Java: an exception out of the factory, reported by its error id
    let table: Arc<dyn Table> =
        made.map_err(|failure| Status::invalid_argument(failure.message))?;
    Ok(table)
}

fn table_of<'a>(value: &'a ExportValue, what: &str) -> Result<&'a Arc<dyn Table>, ExportError> {
    // Java: a ClassCastException, reported by its error id
    value
        .as_table()
        .ok_or_else(|| ExportError::Internal(format!("{what} is not a table")))
}

fn input_table_of(target: &Arc<dyn Table>) -> Result<Arc<dyn InputTable>, Status> {
    target
        .input_table()
        .ok_or_else(|| Status::invalid_argument("Table can't be used as an input table"))
}

/// The rows of `table` now, as one record batch with the table's column names.
///
/// Behavior: `io.deephaven.engine.table.impl.util.BaseArrayBackedInputTable.ArrayBackedInputTableUpdater#snapshotData`
fn rows_of(table: &Arc<dyn Table>) -> Result<RecordBatch, ExportError> {
    let internal = |problem: String| ExportError::Internal(problem);
    let snapshot = snapshot(table).map_err(|failure| internal(failure.message))?;
    let fields: Vec<Field> = table
        .definition()
        .columns()
        .iter()
        .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(fields)),
        snapshot.columns,
        &RecordBatchOptions::new().with_row_count(Some(snapshot.rows)),
    )
    .map_err(|problem| internal(problem.to_string()))
}

/// A change to an input table that has passed the checks made when it is asked for, with
/// the rows as they were then: what Java queues for the table's next cycle.
///
/// Behavior: `io.deephaven.engine.table.impl.util.BaseArrayBackedInputTable.PendingChange`
pub struct Change {
    pub input: Arc<dyn InputTable>,
    pub rows: RecordBatch,
    pub delete: bool,
}

impl Change {
    /// Makes the change and returns once it is visible in the table. `DATA_LOSS` if it
    /// fails while it is applied.
    pub fn apply(self) -> Result<(), ExportError> {
        if self.delete {
            let done = self.input.delete(self.rows);
            done.map_err(|_| Status::data_loss("Error deleting table from inputtable").into())
        } else {
            let done = self.input.add(self.rows);
            done.map_err(|_| Status::data_loss("Error adding table to input table").into())
        }
    }
}

/// Adds the rows of `rows` to the input table `target` and returns once they are visible.
/// In a keyed input table a row replaces the row with the same key. A blink input table
/// returns once it has the rows, and shows them during the next update cycle only.
///
/// `INVALID_ARGUMENT` if `target` is not an input table or the two tables do not have the
/// same columns, by name and type, in any order; `DATA_LOSS` if the change fails while it
/// is applied.
///
/// Behavior: `io.deephaven.server.table.inputtables.InputTableServiceGrpcImpl#addTableToInputTable`,
/// `io.deephaven.engine.util.input.InputTableUpdater#validateAddOrModify`
pub fn add_table(target: &ExportValue, rows: &ExportValue) -> Result<(), ExportError> {
    match checked_add(target, rows)? {
        Some(change) => change.apply(),
        None => Ok(()),
    }
}

/// The checks of [`add_table`] and the rows to add as they are now; `None` when there is
/// no row to add.
///
/// Behavior: `InputTableUpdater#validateAddOrModify` and
/// `BaseArrayBackedInputTable.ArrayBackedInputTableUpdater#snapshotData`, which `#add` and
/// `#addAsync` both run before the change is queued
pub fn checked_add(
    target: &ExportValue,
    rows: &ExportValue,
) -> Result<Option<Change>, ExportError> {
    let target = table_of(target, "inputTable")?;
    let input = input_table_of(target)?;
    let rows = table_of(rows, "tableToAdd")?;
    let (ours, theirs) = (target.definition().columns(), rows.definition().columns());
    let compatible =
        ours.len() == theirs.len() && theirs.iter().all(|column| ours.contains(column));
    if !compatible {
        return Err(Status::invalid_argument(format!(
            "Provided tables's columns are not compatible: Table definition incompatibilities: \
             this has {ours:?}, other has {theirs:?}"
        ))
        .into());
    }
    let rows = rows_of(rows)?;
    Ok((rows.num_rows() > 0).then_some(Change {
        input,
        rows,
        delete: false,
    }))
}

/// Deletes from the input table `target` the rows whose keys are the rows of `keys`, and
/// returns once they are gone. A key that is not in the table is ignored.
///
/// `INVALID_ARGUMENT` if `target` is not an input table, is append-only or blink, or `keys`
/// does not have exactly the key columns; `DATA_LOSS` if the change fails while it is
/// applied.
///
/// Behavior: `InputTableServiceGrpcImpl#deleteTableFromInputTable`,
/// `io.deephaven.engine.util.input.InputTableUpdater#validateDelete`,
/// `io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable.AppendOnlyArrayBackedInputTableUpdater#validateDelete`
pub fn delete_table(target: &ExportValue, keys: &ExportValue) -> Result<(), ExportError> {
    match checked_delete(target, keys)? {
        Some(change) => change.apply(),
        None => Ok(()),
    }
}

/// The checks of [`delete_table`] and the keys to delete as they are now; `None` when
/// there is no key.
///
/// Behavior: `InputTableUpdater#validateDelete` and `#snapshotData`, which `#delete` and
/// `#deleteAsync` both run before the change is queued
pub fn checked_delete(
    target: &ExportValue,
    keys: &ExportValue,
) -> Result<Option<Change>, ExportError> {
    let target = table_of(target, "inputTable")?;
    let input = input_table_of(target)?;
    let keys = table_of(keys, "tableToRemove")?;
    let key_names = input.key_columns();
    // A blink input table keeps the default `validateDelete`, which has no key column to
    // refuse and so goes on to the checks below.
    if key_names.is_empty() && !target.attributes().blink {
        return Err(
            Status::invalid_argument("Provided input table does not support delete.").into(),
        );
    }
    let (ours, theirs) = (target.definition(), keys.definition());
    let mut problems = Vec::new();
    for name in &key_names {
        let sent = theirs.index_of(name).map(|index| &theirs.columns()[index]);
        let wanted = ours.index_of(name).map(|index| &ours.columns()[index]);
        match sent {
            None => problems.push(format!("Key Column \"{name}\" does not exist.")),
            Some(sent) if Some(sent) != wanted => {
                problems.push(format!("Key Column \"{name}\" is not compatible."))
            }
            Some(_) => {}
        }
    }
    let extra: Vec<&str> = theirs
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| !key_names.iter().any(|key| key == name))
        .collect();
    if !extra.is_empty() {
        problems.push(format!("Unknown key columns: {extra:?}"));
    }
    if !problems.is_empty() {
        // Java: an ArgumentException nothing catches, reported by its error id
        return Err(ExportError::Internal(format!(
            "Invalid Key Table Definition: {}",
            problems.join("\n")
        )));
    }
    if key_names.is_empty() {
        // Java: the UnsupportedOperationException of InputTableUpdater#deleteAsync, which
        // nothing catches, reported by its error id
        return Err(ExportError::Internal(
            "Table does not support deletes".to_string(),
        ));
    }
    let rows = rows_of(keys)?;
    Ok((rows.num_rows() > 0).then_some(Change {
        input,
        rows,
        delete: true,
    }))
}

/// Behavior: `io.deephaven.server.table.inputtables.InputTableServiceGrpcImpl`
pub struct InputTableGrpc {
    router: Arc<TicketRouter>,
}

impl InputTableGrpc {
    pub fn new(router: Arc<TicketRouter>) -> Self {
        Self { router }
    }

    /// Runs `change` on the values of two tickets once both are exported, and answers with
    /// `response` when it returns.
    async fn change<T, R: Send + 'static>(
        &self,
        request: Request<T>,
        tickets: impl FnOnce(T) -> [(Option<Ticket>, &'static str); 2],
        change: fn(&ExportValue, &ExportValue) -> Result<(), ExportError>,
        response: R,
    ) -> Result<Response<R>, Status> {
        let session = current_session(&request)?;
        let tickets = tickets(request.into_inner()).map(|(ticket, log_id)| {
            (
                ticket.map(|ticket| ticket.ticket).unwrap_or_default(),
                log_id,
            )
        });
        for (ticket, log_id) in &tickets {
            self.router.log_name(ticket, log_id)?;
        }
        let mut exports = Vec::with_capacity(2);
        for (ticket, log_id) in &tickets {
            exports.push(self.router.resolve(Some(&session), ticket, log_id)?);
        }
        let reply = Reply::new();
        let (on_error, on_success) = (reply.completer(), reply.completer());
        session
            .non_export()?
            .on_error(move |status| on_error(Err(status)))
            .require(exports)
            .submit(move |inputs| {
                change(&inputs[0], &inputs[1])?;
                on_success(Ok(response));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }
}

#[tonic::async_trait]
impl InputTableService for InputTableGrpc {
    /// Behavior: `InputTableServiceGrpcImpl#addTableToInputTable`
    async fn add_table_to_input_table(
        &self,
        request: Request<AddTableRequest>,
    ) -> Result<Response<AddTableResponse>, Status> {
        self.change(
            request,
            |request| {
                [
                    (request.input_table, "inputTable"),
                    (request.table_to_add, "tableToAdd"),
                ]
            },
            add_table,
            AddTableResponse {},
        )
        .await
    }

    /// Behavior: `InputTableServiceGrpcImpl#deleteTableFromInputTable`
    async fn delete_table_from_input_table(
        &self,
        request: Request<DeleteTableRequest>,
    ) -> Result<Response<DeleteTableResponse>, Status> {
        self.change(
            request,
            |request| {
                [
                    (request.input_table, "inputTable"),
                    (request.table_to_remove, "tableToRemove"),
                ]
            },
            delete_table,
            DeleteTableResponse {},
        )
        .await
    }
}
