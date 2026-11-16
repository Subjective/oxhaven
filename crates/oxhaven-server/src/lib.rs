//! gRPC services, sessions, tickets, and Barrage.

pub mod app;
pub mod barrage;
pub mod config;
pub mod console;
pub mod figure;
pub mod flight;
pub mod inputtable;
pub mod java_text;
pub mod object;
pub mod partitioned;
pub mod schema;
pub mod session;
pub mod storage;
pub mod table;
pub mod transport;
pub mod update_graph;
pub mod web;
pub mod wire_columns;

#[cfg(test)]
mod ide;
#[cfg(test)]
mod list_columns;
#[cfg(test)]
pub(crate) mod test_support;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use oxhaven_engine::table::WallClock;
use oxhaven_proto::browser_flight::browser_flight_service_server::BrowserFlightServiceServer;
use oxhaven_proto::flight::flight_service_server::FlightServiceServer;
use oxhaven_proto::grpc::application_service_server::ApplicationServiceServer;
use oxhaven_proto::grpc::config_service_server::ConfigServiceServer;
use oxhaven_proto::grpc::input_table_service_server::InputTableServiceServer;
use oxhaven_proto::grpc::object_service_server::ObjectServiceServer;
use oxhaven_proto::grpc::partitioned_table_service_server::PartitionedTableServiceServer;
use oxhaven_proto::grpc::session_service_server::SessionServiceServer;
use oxhaven_proto::grpc::storage_service_server::StorageServiceServer;
use oxhaven_proto::grpc::table_service_server::TableServiceServer;
use oxhaven_proto::script::console_service_server::ConsoleServiceServer;
use tokio::net::TcpListener;
use tonic::service::Routes;

use app::{
    ApplicationGrpc, ApplicationTicketResolver, Applications, Scope, ScopeTicketResolver,
    ScopedCompiler,
};
use flight::FlightGrpc;
use inputtable::InputTableGrpc;
use session::{
    system_clock, AuthHandlers, ExportTicketResolver, SessionGrpc, SessionStore,
    SharedTicketResolver, TicketRouter, TokioExecutor,
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
    /// The applications of the server: what the process itself offers to clients, listed by
    /// `ApplicationService/ListFields` and named by tickets `a/<application>/f/<field>`.
    pub applications: Arc<Applications>,
    /// The variables that tickets `s/<name>` name.
    pub scope: Arc<Scope>,
    /// The script session of the server and the places its variables are published to, if
    /// the server has a console. `None` after [`ServerState::new`]; set it before [`serve`],
    /// for example with [`console::python::start`].
    pub console: Option<Arc<console::Console>>,
    /// What scripts print, for `ConsoleService/SubscribeToLogs`.
    pub logs: Arc<console::logs::LogBuffer>,
    /// The static files of the web IDE, if the server has them. `None` after
    /// [`ServerState::new`]; set it before [`serve`].
    pub web_root: Option<Arc<web::WebRoot>>,
    /// The directory `StorageService` keeps the web IDE's layouts and notebooks in, if the
    /// server has one. `None` after [`ServerState::new`]; set it before [`serve`].
    pub storage: Option<Arc<storage::StorageRoot>>,
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
        let applications = Applications::new();
        let scope = Scope::new();
        let tickets = Arc::new(TicketRouter::new(vec![
            Arc::new(ExportTicketResolver),
            Arc::new(SharedTicketResolver::new()),
            Arc::new(ApplicationTicketResolver::new(applications.clone())),
            Arc::new(ScopeTicketResolver::new(scope.clone())),
        ])?);
        let graph = PeriodicUpdateGraph::start(config.target_cycle_duration);
        Ok(Self {
            sessions,
            tickets,
            graph,
            applications,
            scope,
            console: None,
            logs: Arc::new(console::logs::LogBuffer::new()),
            web_root: None,
            storage: None,
        })
    }

    /// What table operations run with: the server's graph, the wall clock and the formula
    /// language, whose formulas may name the variables of the query scope.
    pub fn op_context(&self) -> OpContext {
        OpContext {
            graph: self.graph.clone(),
            clock: Arc::new(WallClock),
            compiler: Arc::new(ScopedCompiler::new(
                Arc::new(oxhaven_expr::Compiler),
                self.scope.clone(),
            )),
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
        .add_service(
            BrowserFlightServiceServer::new(flight::BrowserFlightGrpc::new(self.tickets.clone()))
                .max_decoding_message_size(MAX_INBOUND_MESSAGE_SIZE),
        )
        .add_service(InputTableServiceServer::new(InputTableGrpc::new(
            self.tickets.clone(),
        )))
        .add_service(ConfigServiceServer::new(
            config::ConfigGrpc::new(
                self.sessions.auth_handlers(),
                self.sessions.expiration_delay_ms(),
            )
            .with_python_version(config::python_version(self.console.as_deref())),
        ))
        .add_service(TableServiceServer::new(table::TableGrpc::new(
            self.tickets.clone(),
            self.op_context(),
        )))
        .add_service(ApplicationServiceServer::new(ApplicationGrpc::new(
            self.applications.clone(),
        )))
        .add_service(ConsoleServiceServer::new(console::ConsoleGrpc::new(
            self.tickets.clone(),
            self.console.clone(),
            self.logs.clone(),
        )))
        .add_service(
            StorageServiceServer::new(storage::StorageGrpc::new(self.storage.clone()))
                .max_decoding_message_size(MAX_INBOUND_MESSAGE_SIZE),
        )
        .add_service(PartitionedTableServiceServer::new(
            partitioned::PartitionedTableGrpc::new(self.tickets.clone()),
        ))
        .add_service(ObjectServiceServer::new(object::ObjectGrpc::new(
            self.tickets.clone(),
        )))
    }
}

/// Serves `state` on `listener`, without TLS, until `shutdown` completes: gRPC for native
/// clients, and for browsers the web IDE's files, gRPC over a websocket and gRPC-web; see
/// [`transport`]. Clients with a termination subscription are then told, every session is
/// ended, and open calls are given the chance to finish.
pub async fn serve(
    state: ServerState,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    transport::serve(state, listener, shutdown).await
}
