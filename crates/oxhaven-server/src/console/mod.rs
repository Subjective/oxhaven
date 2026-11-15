//! The `ConsoleService` gRPC service.
//!
//! A server has at most one script session ([`ScriptSession`]), as in Java: every console a
//! client starts runs its code in that session and sees the same variables. `docs/CONSOLE.md`
//! describes the design and what is not built yet.
//!
//! With a script session ([`ConsoleGrpc::new`] with a [`Console`]):
//!
//! - `GetConsoleTypes` names the session's language and `StartConsole` exports the console.
//! - `ExecuteCommand` runs code. Its answer lists the variables that now hold, or no longer
//!   hold, something a client can open; the same changes go to the clients listening on
//!   `ApplicationService/ListFields`, and the ticket `s/<name>` resolves to the value. An
//!   error in the code is not an error of the call: the answer carries its text.
//! - What the code prints arrives through `SubscribeToLogs` ([`logs`]), with the server's
//!   own log.
//! - `GetHeapInfo` reports the memory of the process ([`memory`]).
//! - `BindTableToVariable` makes an exported table a variable.
//! - `AutoCompleteStream` answers every request "nothing found", which is what Java does
//!   when the Python package `jedi` is missing.
//!
//! Without one, the server is the Java server started with
//! `-Ddeephaven.console.disable=true`, as far as the web IDE can tell: `GetConsoleTypes`
//! answers that there is no console type and every other call is refused with
//! `UNIMPLEMENTED` and its name. Java answers the other calls each in their own way
//! (`src/ide/fixtures/java.txt`, the `console_*` cases).

pub mod logs;
pub mod memory;
#[cfg(feature = "python")]
pub mod python;
mod session;

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use oxhaven_proto::grpc::{FieldInfo, FieldsChangeUpdate, Ticket, TypedTicket};
use oxhaven_proto::script::auto_complete_request::Request as CompletionRequest;
use oxhaven_proto::script::auto_complete_response::Response as CompletionResponse;
use oxhaven_proto::script::console_service_server::ConsoleService;
use oxhaven_proto::script::{
    AutoCompleteRequest, AutoCompleteResponse, BindTableToVariableRequest,
    BindTableToVariableResponse, BrowserNextResponse, CancelAutoCompleteRequest,
    CancelAutoCompleteResponse, CancelCommandRequest, CancelCommandResponse, ExecuteCommandRequest,
    ExecuteCommandResponse, GetCompletionItemsResponse, GetConsoleTypesRequest,
    GetConsoleTypesResponse, GetHeapInfoRequest, GetHeapInfoResponse, LogSubscriptionRequest,
    StartConsoleRequest, StartConsoleResponse,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::app::scope_ticket;
use crate::session::{current_session, ExportError, ExportValue, Reply, TicketRouter};
use logs::{LogBuffer, LogUpdates};
pub use session::{
    type_name_if_displayable, Changes, Console, ScriptSession, Variable,
    CHECK_SCOPE_CHANGES_INTERVAL, TABLE_TYPE_NAME,
};

/// The status of a console call a server without a script session does not have.
fn no_session(method: &str) -> Status {
    Status::unimplemented(format!(
        "ConsoleService/{method} is not implemented: this server has no script session"
    ))
}

/// The status of a console call that is not built yet on a server with a script session.
fn not_built(method: &str) -> Status {
    Status::unimplemented(format!("ConsoleService/{method} is not implemented"))
}

/// The status of a call the Java service does not override.
///
/// Behavior: `io.grpc.stub.ServerCalls#asyncUnimplementedUnaryCall`
fn java_unimplemented(method: &str) -> Status {
    Status::unimplemented(format!(
        "Method io.deephaven.proto.backplane.script.grpc.ConsoleService/{method} is unimplemented"
    ))
}

/// Nanoseconds since the epoch, for the timestamps of `ExecuteCommandResponse`.
///
/// Behavior: `io.deephaven.base.clock.Clock#currentTimeNanos`
fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as i64)
}

/// A scope variable as clients are told about it.
///
/// Behavior: `ConsoleServiceGrpcImpl#makeVariableDefinition`
fn variable_definition(variable: &Variable) -> FieldInfo {
    FieldInfo {
        typed_ticket: Some(TypedTicket {
            ticket: Some(Ticket {
                ticket: scope_ticket(&variable.name),
            }),
            r#type: variable.type_name.clone(),
        }),
        field_name: variable.name.clone(),
        field_description: "query scope variable".to_string(),
        application_name: String::new(),
        application_id: "scope".to_string(),
    }
}

/// The console of an export, or the failure Java's cast of the export's value to
/// `ScriptSession` ends in.
fn console_of(value: &ExportValue) -> Result<Arc<Console>, ExportError> {
    value
        .downcast_ref::<Arc<Console>>()
        .cloned()
        .ok_or_else(|| ExportError::Internal("the console id does not name a console".to_string()))
}

/// The bytes of a ticket field; empty if the field is not set.
fn ticket_bytes(ticket: Option<Ticket>) -> Vec<u8> {
    ticket.map(|ticket| ticket.ticket).unwrap_or_default()
}

/// Behavior: `io.deephaven.server.console.ConsoleServiceGrpcImpl`
pub struct ConsoleGrpc {
    router: Arc<TicketRouter>,
    console: Option<Arc<Console>>,
    logs: Arc<LogBuffer>,
}

impl ConsoleGrpc {
    /// The service of a server whose script session is `console`; `None` is a server
    /// without a console.
    pub fn new(
        router: Arc<TicketRouter>,
        console: Option<Arc<Console>>,
        logs: Arc<LogBuffer>,
    ) -> Self {
        Self {
            router,
            console,
            logs,
        }
    }

    fn console(&self, method: &str) -> Result<&Arc<Console>, Status> {
        self.console.as_ref().ok_or_else(|| no_session(method))
    }
}

#[tonic::async_trait]
impl ConsoleService for ConsoleGrpc {
    /// The language of the script session, if there is one. As in Java, the call needs no
    /// session.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#getConsoleTypes`
    async fn get_console_types(
        &self,
        _request: Request<GetConsoleTypesRequest>,
    ) -> Result<Response<GetConsoleTypesResponse>, Status> {
        Ok(Response::new(GetConsoleTypesResponse {
            console_types: self
                .console
                .iter()
                .map(|console| console.console_type())
                .collect(),
        }))
    }

    /// Exports the console under the request's ticket. Every console is the one script
    /// session of the server.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#startConsole`
    async fn start_console(
        &self,
        request: Request<StartConsoleRequest>,
    ) -> Result<Response<StartConsoleResponse>, Status> {
        let console = self.console("StartConsole")?.clone();
        let session = current_session(&request)?;
        let request = request.into_inner();
        if !console.is_type(&request.session_type) {
            return Err(Status::failed_precondition(format!(
                "session type '{}' is not supported",
                request.session_type
            )));
        }
        let result_id = request.result_id.clone();
        let reply = Reply::new();
        let (on_error, on_success) = (reply.completer(), reply.completer());
        session
            .new_export_for_ticket(&ticket_bytes(request.result_id), "resultId")?
            .on_error(move |status| on_error(Err(status)))
            .submit(move |_| {
                on_success(Ok(StartConsoleResponse { result_id }));
                Ok(ExportValue::object(console))
            })?;
        reply.wait().await
    }

    /// The memory of the process, where Java reports the heap of its JVM; [`memory`] says
    /// which figure stands for which.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#getHeapInfo`
    async fn get_heap_info(
        &self,
        request: Request<GetHeapInfoRequest>,
    ) -> Result<Response<GetHeapInfoResponse>, Status> {
        self.console("GetHeapInfo")?;
        current_session(&request)?;
        let memory = memory::process_memory().ok_or_else(|| {
            Status::unimplemented(
                "ConsoleService/GetHeapInfo is not implemented on a system without /proc",
            )
        })?;
        Ok(Response::new(GetHeapInfoResponse {
            max_memory: memory.max,
            total_memory: memory.total,
            free_memory: memory.free,
        }))
    }

    type SubscribeToLogsStream = LogUpdates;

    /// The log records that are kept, then each new one, until the client ends the call,
    /// falls too far behind (`RESOURCE_EXHAUSTED`) or its session ends (`CANCELLED`). See
    /// [`logs`] for what is logged.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#subscribeToLogs`
    async fn subscribe_to_logs(
        &self,
        request: Request<LogSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeToLogsStream>, Status> {
        self.console("SubscribeToLogs")?;
        let session = current_session(&request)?;
        let updates = self.logs.subscribe_for(request.into_inner(), &session)?;
        Ok(Response::new(updates))
    }

    /// Runs the code on the session's serial queue, so that the commands of a session run
    /// one after the other, and answers with what it changed.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#executeCommand`
    async fn execute_command(
        &self,
        request: Request<ExecuteCommandRequest>,
    ) -> Result<Response<ExecuteCommandResponse>, Status> {
        self.console("ExecuteCommand")?;
        let session = current_session(&request)?;
        let request = request.into_inner();
        let console_id = ticket_bytes(request.console_id);
        if console_id.is_empty() {
            return Err(Status::failed_precondition("No consoleId supplied"));
        }
        self.router.log_name(&console_id, "consoleId")?;
        let console = self
            .router
            .resolve(Some(&session), &console_id, "consoleId")?;
        let code = request.code;
        let reply = Reply::new();
        let (on_error, on_success) = (reply.completer(), reply.completer());
        session
            .non_export()?
            .requires_serial_queue()
            .require([console])
            .on_error(move |status| on_error(Err(status)))
            .submit(move |inputs| {
                let console = console_of(&inputs[0])?;
                let start_timestamp = now_nanos();
                let changes = console.evaluate(&code);
                let end_timestamp = now_nanos();
                if let Some(error) = &changes.error {
                    tracing::error!("Error running script: {error}");
                }
                let definitions =
                    |variables: &[Variable]| variables.iter().map(variable_definition).collect();
                on_success(Ok(ExecuteCommandResponse {
                    error_message: changes.error.clone().unwrap_or_default(),
                    changes: Some(FieldsChangeUpdate {
                        created: definitions(&changes.created),
                        updated: definitions(&changes.updated),
                        removed: definitions(&changes.removed),
                    }),
                    start_timestamp,
                    end_timestamp,
                }));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }

    /// Behavior: `ConsoleServiceGrpcImpl#cancelCommand`, which Java does not have either
    async fn cancel_command(
        &self,
        _request: Request<CancelCommandRequest>,
    ) -> Result<Response<CancelCommandResponse>, Status> {
        self.console("CancelCommand")?;
        Err(java_unimplemented("CancelCommand"))
    }

    /// Makes the table of `table_id` the variable `variable_name` of the script session.
    /// With a `console_id` the ticket has to name a console; the variable is the same
    /// either way, because every console is the one script session.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#bindTableToVariable`
    async fn bind_table_to_variable(
        &self,
        request: Request<BindTableToVariableRequest>,
    ) -> Result<Response<BindTableToVariableResponse>, Status> {
        let console = self.console("BindTableToVariable")?.clone();
        let session = current_session(&request)?;
        let request = request.into_inner();
        let table_id = ticket_bytes(request.table_id);
        if table_id.is_empty() {
            return Err(Status::failed_precondition("No source tableId supplied"));
        }
        self.router.log_name(&table_id, "tableId")?;
        let mut required = vec![self.router.resolve(Some(&session), &table_id, "tableId")?];
        if let Some(console_id) = request.console_id {
            required.push(
                self.router
                    .resolve(Some(&session), &console_id.ticket, "consoleId")?,
            );
        }
        let name = request.variable_name;
        let reply = Reply::new();
        let (on_error, on_success) = (reply.completer(), reply.completer());
        session
            .non_export()?
            .requires_serial_queue()
            .require(required)
            .on_error(move |status| on_error(Err(status)))
            .submit(move |inputs| {
                let console = match inputs.get(1) {
                    Some(value) => console_of(value)?,
                    None => console,
                };
                // Java casts and fails with a ClassCastException
                let table = inputs[0].as_table().cloned().ok_or_else(|| {
                    ExportError::Internal("the table id does not name a table".to_string())
                })?;
                console
                    .bind_table(&name, table)
                    .map_err(ExportError::Internal)?;
                on_success(Ok(BindTableToVariableResponse {}));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }

    type AutoCompleteStreamStream = UnboundedReceiverStream<Result<AutoCompleteResponse, Status>>;

    /// Answers every request with "success, nothing found", and ends when the client does.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl.NoopAutoCompleteObserver`, which the Java server
    /// uses for Python when `jedi` is not installed
    async fn auto_complete_stream(
        &self,
        request: Request<Streaming<AutoCompleteRequest>>,
    ) -> Result<Response<Self::AutoCompleteStreamStream>, Status> {
        self.console("AutoCompleteStream")?;
        current_session(&request)?;
        let mut requests = request.into_inner();
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(Ok(request)) = requests.next().await {
                let response = match request.request {
                    // for clients from before the other requests existed
                    Some(CompletionRequest::GetCompletionItems(_)) => {
                        // the two fields are deprecated in the protocol and Java sets them
                        #[allow(deprecated)]
                        Some(CompletionResponse::CompletionItems(
                            GetCompletionItemsResponse {
                                success: true,
                                request_id: request.request_id,
                                ..Default::default()
                            },
                        ))
                    }
                    _ => None,
                };
                let answer = AutoCompleteResponse {
                    request_id: request.request_id,
                    success: true,
                    response,
                };
                if sender.send(Ok(answer)).is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(UnboundedReceiverStream::new(receiver)))
    }

    /// Behavior: `ConsoleServiceGrpcImpl#cancelAutoComplete`, which Java does not have
    /// either
    async fn cancel_auto_complete(
        &self,
        _request: Request<CancelAutoCompleteRequest>,
    ) -> Result<Response<CancelAutoCompleteResponse>, Status> {
        self.console("CancelAutoComplete")?;
        Err(java_unimplemented("CancelAutoComplete"))
    }

    type OpenAutoCompleteStreamStream =
        UnboundedReceiverStream<Result<AutoCompleteResponse, Status>>;

    async fn open_auto_complete_stream(
        &self,
        _request: Request<AutoCompleteRequest>,
    ) -> Result<Response<Self::OpenAutoCompleteStreamStream>, Status> {
        self.console("OpenAutoCompleteStream")?;
        Err(not_built("OpenAutoCompleteStream"))
    }

    async fn next_auto_complete_stream(
        &self,
        _request: Request<AutoCompleteRequest>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        self.console("NextAutoCompleteStream")?;
        Err(not_built("NextAutoCompleteStream"))
    }
}
