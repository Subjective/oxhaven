//! gRPC services, sessions, tickets, and Barrage.

pub mod config;
pub mod flight;
pub mod inputtable;
pub mod schema;
pub mod session;
pub mod table;
pub mod update_graph;

#[cfg(test)]
pub(crate) mod test_support;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use oxhaven_engine::table::WallClock;
use oxhaven_proto::flight::flight_service_server::FlightServiceServer;
use oxhaven_proto::grpc::config_service_server::ConfigServiceServer;
use oxhaven_proto::grpc::input_table_service_server::InputTableServiceServer;
use oxhaven_proto::grpc::session_service_server::SessionServiceServer;
use oxhaven_proto::grpc::table_service_server::TableServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::Routes;

use flight::FlightGrpc;
use inputtable::InputTableGrpc;
use session::{
    system_clock, AuthHandlers, ExportTicketResolver, SessionGrpc, SessionLayer, SessionStore,
    TicketRouter, TokioExecutor,
};
use table::support::OpContext;
use update_graph::PeriodicUpdateGraph;

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
    /// The time from the start of one update graph cycle to the start of the next;
    /// [`update_graph::DEFAULT_TARGET_CYCLE_DURATION`] is Java's default. `None` runs a
    /// cycle only when one is requested.
    pub target_cycle_duration: Option<Duration>,
}

/// The state every service is built from.
#[derive(Clone)]
pub struct ServerState {
    pub sessions: Arc<SessionStore>,
    pub tickets: Arc<TicketRouter>,
    /// The update graph of every refreshing table the server creates. It runs its own
    /// cycles; see [`PeriodicUpdateGraph`].
    pub graph: Arc<PeriodicUpdateGraph>,
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
        let graph = PeriodicUpdateGraph::start(config.target_cycle_duration);
        Ok(Self {
            sessions,
            tickets,
            graph,
        })
    }

    /// What table operations run with: the server's graph, the wall clock and the formula
    /// language.
    pub fn op_context(&self) -> OpContext {
        OpContext {
            graph: self.graph.clone(),
            clock: Arc::new(WallClock),
            compiler: Arc::new(oxhaven_expr::Compiler),
        }
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
        .add_service(ConfigServiceServer::new(config::ConfigGrpc::new(
            self.sessions.auth_handlers(),
            self.sessions.expiration_delay_ms(),
        )))
        .add_service(TableServiceServer::new(table::TableGrpc::new(
            self.tickets.clone(),
            self.op_context(),
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
