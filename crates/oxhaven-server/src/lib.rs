//! gRPC services, sessions, tickets, and Barrage.

pub mod flight;
pub mod inputtable;
pub mod schema;
pub mod session;

use std::future::Future;
use std::sync::Arc;

use oxhaven_proto::flight::flight_service_server::FlightServiceServer;
use oxhaven_proto::grpc::input_table_service_server::InputTableServiceServer;
use oxhaven_proto::grpc::session_service_server::SessionServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Routes;

use flight::FlightGrpc;
use inputtable::InputTableGrpc;
use session::{
    system_clock, AuthHandlers, ExportTicketResolver, SessionGrpc, SessionLayer, SessionStore,
    TicketRouter, TokioExecutor,
};

/// The largest message a client may send, as in the reference server.
///
/// Behavior: `io.deephaven.server.config.ServerConfig#DEFAULT_MAX_INBOUND_MESSAGE_SIZE_MiB`
const MAX_INBOUND_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// What the server is started with.
pub struct ServerConfig {
    /// The ways a client may authenticate.
    pub auth_handlers: AuthHandlers,
    /// How long a session token lasts, in milliseconds.
    pub token_expire_ms: i64,
}

/// The state every service is built from.
#[derive(Clone)]
pub struct ServerState {
    pub sessions: Arc<SessionStore>,
    pub tickets: Arc<TicketRouter>,
}

impl ServerState {
    /// Must be called inside a tokio runtime: export work runs on its blocking pool, and the
    /// token cleanup task is spawned on it.
    pub fn new(config: ServerConfig) -> Result<Self, String> {
        let executor = Arc::new(TokioExecutor::new(tokio::runtime::Handle::current()));
        let sessions = SessionStore::new(
            system_clock(),
            executor,
            config.auth_handlers,
            config.token_expire_ms,
        )?;
        sessions.spawn_cleanup();
        // a service that owns another kind of ticket adds its resolver here
        let tickets = Arc::new(TicketRouter::new(vec![Arc::new(ExportTicketResolver)])?);
        Ok(Self { sessions, tickets })
    }

    /// The gRPC services of the server. A service task adds its service here; a call to a
    /// service that is not listed answers `UNIMPLEMENTED`.
    pub fn routes(&self) -> Routes {
        Routes::new(SessionServiceServer::new(SessionGrpc::new(
            self.sessions.clone(),
            self.tickets.clone(),
        )))
        .add_service(
            FlightServiceServer::new(FlightGrpc::new(self.sessions.clone(), self.tickets.clone()))
                .max_decoding_message_size(MAX_INBOUND_MESSAGE_SIZE),
        )
        .add_service(InputTableServiceServer::new(InputTableGrpc::new(
            self.tickets.clone(),
        )))
    }
}

/// Serves gRPC on `listener`, without TLS, until `shutdown` completes. Clients with a
/// termination subscription are then told, every session is ended, and open calls are given
/// the chance to finish.
pub async fn serve(
    state: ServerState,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    let sessions = state.sessions.clone();
    tonic::transport::Server::builder()
        .layer(SessionLayer::new(state.sessions.clone()))
        .add_routes(state.routes())
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            shutdown.await;
            sessions.on_shutdown();
        })
        .await
}
