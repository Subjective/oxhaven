//! The `ConsoleService` gRPC service of a server without a script session.
//!
//! `GetConsoleTypes` answers that there is no console type, which is what the web IDE needs
//! to start without a console. Every other call is refused with `UNIMPLEMENTED` and its name.
//!
//! The Java server nearest to this is one started with `-Ddeephaven.console.disable=true`.
//! It answers `GetConsoleTypes` the same way and the other calls each in their own way
//! (`src/ide/fixtures/java.txt`, the `console_*` cases): `FAILED_PRECONDITION` "Remote
//! console disabled" for `StartConsole` and `SubscribeToLogs`, the heap of the JVM for
//! `GetHeapInfo`, and no answer at all for `ExecuteCommand` and `BindTableToVariable`.

#[cfg(test)]
mod tests;

use oxhaven_proto::script::console_service_server::ConsoleService;
use oxhaven_proto::script::{
    AutoCompleteRequest, AutoCompleteResponse, BindTableToVariableRequest,
    BindTableToVariableResponse, BrowserNextResponse, CancelAutoCompleteRequest,
    CancelAutoCompleteResponse, CancelCommandRequest, CancelCommandResponse, ExecuteCommandRequest,
    ExecuteCommandResponse, GetConsoleTypesRequest, GetConsoleTypesResponse, GetHeapInfoRequest,
    GetHeapInfoResponse, LogSubscriptionData, LogSubscriptionRequest, StartConsoleRequest,
    StartConsoleResponse,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

/// The status of a console call this server does not have.
fn unimplemented(method: &str) -> Status {
    Status::unimplemented(format!(
        "ConsoleService/{method} is not implemented: this server has no script session"
    ))
}

/// Behavior: `io.deephaven.server.console.ConsoleServiceGrpcImpl`
pub struct ConsoleGrpc;

#[tonic::async_trait]
impl ConsoleService for ConsoleGrpc {
    /// No console types. As in Java, the call needs no session.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl#getConsoleTypes` with `REMOTE_CONSOLE_DISABLED`
    async fn get_console_types(
        &self,
        _request: Request<GetConsoleTypesRequest>,
    ) -> Result<Response<GetConsoleTypesResponse>, Status> {
        Ok(Response::new(GetConsoleTypesResponse::default()))
    }

    async fn start_console(
        &self,
        _request: Request<StartConsoleRequest>,
    ) -> Result<Response<StartConsoleResponse>, Status> {
        Err(unimplemented("StartConsole"))
    }

    async fn get_heap_info(
        &self,
        _request: Request<GetHeapInfoRequest>,
    ) -> Result<Response<GetHeapInfoResponse>, Status> {
        Err(unimplemented("GetHeapInfo"))
    }

    type SubscribeToLogsStream = UnboundedReceiverStream<Result<LogSubscriptionData, Status>>;

    async fn subscribe_to_logs(
        &self,
        _request: Request<LogSubscriptionRequest>,
    ) -> Result<Response<Self::SubscribeToLogsStream>, Status> {
        Err(unimplemented("SubscribeToLogs"))
    }

    async fn execute_command(
        &self,
        _request: Request<ExecuteCommandRequest>,
    ) -> Result<Response<ExecuteCommandResponse>, Status> {
        Err(unimplemented("ExecuteCommand"))
    }

    async fn cancel_command(
        &self,
        _request: Request<CancelCommandRequest>,
    ) -> Result<Response<CancelCommandResponse>, Status> {
        Err(unimplemented("CancelCommand"))
    }

    async fn bind_table_to_variable(
        &self,
        _request: Request<BindTableToVariableRequest>,
    ) -> Result<Response<BindTableToVariableResponse>, Status> {
        Err(unimplemented("BindTableToVariable"))
    }

    type AutoCompleteStreamStream = UnboundedReceiverStream<Result<AutoCompleteResponse, Status>>;

    async fn auto_complete_stream(
        &self,
        _request: Request<Streaming<AutoCompleteRequest>>,
    ) -> Result<Response<Self::AutoCompleteStreamStream>, Status> {
        Err(unimplemented("AutoCompleteStream"))
    }

    async fn cancel_auto_complete(
        &self,
        _request: Request<CancelAutoCompleteRequest>,
    ) -> Result<Response<CancelAutoCompleteResponse>, Status> {
        Err(unimplemented("CancelAutoComplete"))
    }

    type OpenAutoCompleteStreamStream =
        UnboundedReceiverStream<Result<AutoCompleteResponse, Status>>;

    async fn open_auto_complete_stream(
        &self,
        _request: Request<AutoCompleteRequest>,
    ) -> Result<Response<Self::OpenAutoCompleteStreamStream>, Status> {
        Err(unimplemented("OpenAutoCompleteStream"))
    }

    async fn next_auto_complete_stream(
        &self,
        _request: Request<AutoCompleteRequest>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        Err(unimplemented("NextAutoCompleteStream"))
    }
}
