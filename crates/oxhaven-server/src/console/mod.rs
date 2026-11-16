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
//!   error in the code is not an error of the call: the answer carries its text. A command
//!   whose call the client cancels, or whose session ends, is stopped ([`Stop`]); Java
//!   lets it run on.
//! - What the code prints arrives through `SubscribeToLogs` ([`logs`]), with the server's
//!   own log.
//! - `GetHeapInfo` reports the memory of the process ([`memory`]).
//! - `BindTableToVariable` makes an exported table a variable.
//! - `AutoCompleteStream` answers requests about the code a user types: completions,
//!   signatures, hover texts ([`completer`]). A browser on an `https://` page sends the
//!   same stream as `OpenAutoCompleteStream` and one `NextAutoCompleteStream` call per
//!   message ([`browser`]).
//!
//! Without one, the server is the Java server started with
//! `-Ddeephaven.console.disable=true`, as far as the web IDE can tell: `GetConsoleTypes`
//! answers that there is no console type and every other call is refused with
//! `UNIMPLEMENTED` and its name. Java answers the other calls each in their own way
//! (`src/ide/fixtures/java.txt`, the `console_*` cases).

pub mod browser;
pub mod completer;
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
use oxhaven_proto::script::console_service_server::ConsoleService;
use oxhaven_proto::script::{
    AutoCompleteRequest, BindTableToVariableRequest, BindTableToVariableResponse,
    BrowserNextResponse, CancelAutoCompleteRequest, CancelAutoCompleteResponse,
    CancelCommandRequest, CancelCommandResponse, ExecuteCommandRequest, ExecuteCommandResponse,
    GetConsoleTypesRequest, GetConsoleTypesResponse, GetHeapInfoRequest, GetHeapInfoResponse,
    LogSubscriptionRequest, StartConsoleRequest, StartConsoleResponse,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};

use crate::app::scope_ticket;
use crate::session::{
    current_session, ExportError, ExportValue, OnCloseId, Reply, Session, TicketRouter,
};
use browser::{stream_data, BrowserStream};
use completer::{Answers, Completer};
use logs::{LogBuffer, LogUpdates};
pub use session::{
    type_name_if_displayable, Changes, Console, ScriptSession, Stop, Variable,
    CHECK_SCOPE_CHANGES_INTERVAL, TABLE_TYPE_NAME,
};

/// The status of a console call a server without a script session does not have.
fn no_session(method: &str) -> Status {
    Status::unimplemented(format!(
        "ConsoleService/{method} is not implemented: this server has no script session"
    ))
}

/// The status of a call the Java service does not override.
///
/// Behavior: `io.grpc.stub.ServerCalls#asyncUnimplementedUnaryCall`
fn java_unimplemented(method: &str) -> Status {
    Status::unimplemented(format!(
        "Method io.deephaven.proto.backplane.script.grpc.ConsoleService/{method} is unimplemented"
    ))
}

/// The completer of the console's script session for a stream that is opened now, if it
/// has one. A session that cannot say is refused, naming what Java would have done.
///
/// Behavior: `ConsoleServiceGrpcImpl#autoCompleteStream`
async fn ready_completer(console: &Arc<Console>) -> Result<Option<Arc<dyn Completer>>, Status> {
    let console = console.clone();
    // the session may run code of its language to get the completer ready
    match tokio::task::spawn_blocking(move || console.completer()).await {
        Ok(completer) => completer.map_err(Status::unimplemented),
        Err(error) => Err(completer::logged(&error.to_string())),
    }
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

/// Asks a command to stop when the call that sent it ends without its answer: when this
/// is dropped before [`StopWithCall::answered`], which is what happens to the call's
/// future when the client cancels the call or its connection is lost; and when the
/// session of the call ends.
struct StopWithCall {
    stop: Arc<Stop>,
    session: Arc<Session>,
    on_close: Option<OnCloseId>,
    answered: bool,
}

impl StopWithCall {
    fn new(session: &Arc<Session>) -> Result<Self, Status> {
        let stop = Stop::new();
        let on_close = session.add_on_close({
            let stop = stop.clone();
            move || stop.request()
        })?;
        Ok(Self {
            stop,
            session: session.clone(),
            on_close: Some(on_close),
            answered: false,
        })
    }

    fn answered(mut self) {
        self.answered = true;
    }
}

impl Drop for StopWithCall {
    fn drop(&mut self) {
        if let Some(on_close) = self.on_close.take() {
            self.session.remove_on_close(on_close);
        }
        if !self.answered {
            self.stop.request();
        }
    }
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
        request: Request<GetConsoleTypesRequest>,
    ) -> Result<Response<GetConsoleTypesResponse>, Status> {
        stream_data(request.metadata())?;
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
        stream_data(request.metadata())?;
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
        stream_data(request.metadata())?;
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
        stream_data(request.metadata())?;
        let session = current_session(&request)?;
        let updates = self.logs.subscribe_for(request.into_inner(), &session)?;
        Ok(Response::new(updates))
    }

    /// Runs the code on the session's serial queue, so that the commands of a session run
    /// one after the other, and answers with what it changed.
    ///
    /// When the client cancels the call, or its session ends, the command is stopped: one
    /// that waits on the queue does not run, and one that runs is interrupted, as far as
    /// the script session can (`docs/CONSOLE.md`, "Stopping a running command"). Java
    /// does neither: on Java 42.9 a command whose call was cancelled, or whose session was
    /// closed, ran to its end, and the commands after it waited (observed with a
    /// `time.sleep` and with a loop). A command that never ends holds the update graph's
    /// lock in either server, so there it is the end of every ticking table until the
    /// server is restarted.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#executeCommand`
    async fn execute_command(
        &self,
        request: Request<ExecuteCommandRequest>,
    ) -> Result<Response<ExecuteCommandResponse>, Status> {
        self.console("ExecuteCommand")?;
        stream_data(request.metadata())?;
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
        let call = StopWithCall::new(&session)?;
        let stop = call.stop.clone();
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
                let changes = console.evaluate_stoppable(&code, &stop);
                let end_timestamp = now_nanos();
                if let Some(failure) = changes.failure {
                    // Java: the exception of an object type's `isType` leaves
                    // `evaluateScript`, and the call fails though the code ran
                    return Err(ExportError::Internal(failure));
                }
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
        let answer = reply.wait().await;
        call.answered();
        answer
    }

    /// Not implemented, as in Java, whose web client does not call it either: its
    /// `IdeSession#runCode` refuses to cancel. A command is stopped by cancelling the
    /// `ExecuteCommand` call that sent it.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#cancelCommand`, which Java does not have either
    async fn cancel_command(
        &self,
        request: Request<CancelCommandRequest>,
    ) -> Result<Response<CancelCommandResponse>, Status> {
        self.console("CancelCommand")?;
        stream_data(request.metadata())?;
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
        stream_data(request.metadata())?;
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

    type AutoCompleteStreamStream = Answers;

    /// Answers the requests of the stream with the completer of the script session, or
    /// with "success, nothing found" when the session has none ([`completer`]).
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#autoCompleteStream`
    async fn auto_complete_stream(
        &self,
        request: Request<Streaming<AutoCompleteRequest>>,
    ) -> Result<Response<Self::AutoCompleteStreamStream>, Status> {
        let console = self.console("AutoCompleteStream")?;
        stream_data(request.metadata())?;
        let session = current_session(&request)?;
        let completer = ready_completer(console).await?;
        let answers = completer::serve(request.into_inner(), completer, &session, None)?;
        Ok(Response::new(answers))
    }

    /// Behavior: `ConsoleServiceGrpcImpl#cancelAutoComplete`, which Java does not have
    /// either
    async fn cancel_auto_complete(
        &self,
        request: Request<CancelAutoCompleteRequest>,
    ) -> Result<Response<CancelAutoCompleteResponse>, Status> {
        self.console("CancelAutoComplete")?;
        stream_data(request.metadata())?;
        Err(java_unimplemented("CancelAutoComplete"))
    }

    type OpenAutoCompleteStreamStream = Answers;

    /// `AutoCompleteStream` for a browser, which cannot stream to the server over
    /// gRPC-web: this call carries the first message and its response is the response
    /// stream; the later messages come one per `NextAutoCompleteStream` call
    /// ([`browser`]).
    ///
    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeOpen`, as
    /// `ConsoleServiceGrpcBinding#bindService` sets it up
    async fn open_auto_complete_stream(
        &self,
        request: Request<AutoCompleteRequest>,
    ) -> Result<Response<Self::OpenAutoCompleteStreamStream>, Status> {
        let console = self.console("OpenAutoCompleteStream")?;
        let data = stream_data(request.metadata())?;
        let session = current_session(&request)?;
        let data = browser::open_headers(data)?;
        let completer = ready_completer(console).await?;
        let (stream, messages) = BrowserStream::open(&session, &data)?;
        let messages = UnboundedReceiverStream::new(messages).map(Ok);
        let answers = completer::serve(messages, completer, &session, Some(stream.clone()))?;
        // the first message is in order by itself, so nothing refuses it
        stream.on_message(request.into_inner(), &data)?;
        Ok(Response::new(answers))
    }

    /// A later message of a stream that `OpenAutoCompleteStream` opened, named by the
    /// stream headers of the call. Answered when the message has been taken, which for one
    /// that arrives before the messages it follows is before it is handled.
    ///
    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeNext`
    async fn next_auto_complete_stream(
        &self,
        request: Request<AutoCompleteRequest>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        self.console("NextAutoCompleteStream")?;
        browser::next(request).await
    }
}
