//! Resolving URIs into objects: what `deephaven.uri.resolve` and Java's
//! `ResolveTools#resolve` do.
//!
//! The five resolvers of Deephaven 42.9 are the variants of [`Resolver`]: a query scope
//! variable (`dh:///scope/<name>`), an application field (`dh:///app/<id>/field/<name>`), a
//! table of another server (`dh://` and `dh+plain://` with a host), a CSV file and a
//! Parquet file. [`resolver_for`] picks one as Java's `UriResolvers#resolver` does: by the
//! exact scheme, then by the shape of the URI. [`UriResolvers`] resolves with the state of
//! a server.
//!
//! A table of another server is a subscription through the Barrage client
//! (`crate::barrage_client`) with one session per target, kept for the life of the
//! resolvers, as Java's `BarrageTableResolver` keeps them. The client runs on a runtime of
//! its own ([`block_on`]), so that a caller on any thread can wait for it, and the wait for
//! a subscription gives the update graph's lock up ([`waiting_for_subscription`]), as
//! Java's wait on its future does.
//!
//! Resolution is reachable from scripts only, as in Java; no gRPC call resolves a URI.
//! Port of `io.deephaven.uri.ResolveTools`, `io.deephaven.uri.resolver.UriResolvers` and
//! the resolvers of `io.deephaven.server.uri`.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::yielding_to_cycles;
use oxhaven_uri::{
    is_local_path, ApplicationUri, DeephavenTarget, QueryScopeUri, RemoteUri, StructuredUri, Uri,
    UriError,
};

use crate::app::{application_field_ticket, scope_ticket, Applications, Scope};
use crate::barrage_client::{
    BarrageSession, ClientConfig, ClientError, SnapshotOptions, SubscriptionOptions, Wake,
};
use crate::session::ExportValue;
use crate::update_graph::PeriodicUpdateGraph;
use crate::ServerState;

/// Why a URI did not resolve. `Display` is the text a script sees: Java's exception class
/// and message where Java throws, the Barrage client's or the reader's text otherwise.
#[derive(Clone, Debug)]
pub enum ResolveError {
    /// Java's `IllegalArgumentException`: a string that is no URI, a URI that is not the
    /// Deephaven form its resolver takes.
    IllegalArgument(String),
    /// Java's `UnsupportedOperationException`: no resolver, or a form Java does not
    /// resolve.
    UnsupportedOperation(String),
    /// The remote server could not be reached, or refused, or sent what the client does
    /// not read.
    Remote(ClientError),
    /// A file could not be read.
    Read(String),
    /// A subscription asked for from a place that cannot wait for an update cycle.
    CannotWait(&'static str),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::IllegalArgument(message) => {
                write!(f, "java.lang.IllegalArgumentException: {message}")
            }
            ResolveError::UnsupportedOperation(message) => {
                write!(f, "java.lang.UnsupportedOperationException: {message}")
            }
            ResolveError::Remote(error) => error.fmt(f),
            ResolveError::Read(text) => f.write_str(text),
            ResolveError::CannotWait(text) => f.write_str(text),
        }
    }
}

impl std::error::Error for ResolveError {}

impl From<UriError> for ResolveError {
    fn from(error: UriError) -> Self {
        // Java throws `IllegalArgumentException` for both kinds, with the syntax error's
        // message as its own
        ResolveError::IllegalArgument(error.to_string())
    }
}

impl From<ClientError> for ResolveError {
    fn from(error: ClientError) -> Self {
        ResolveError::Remote(error)
    }
}

/// The resolvers Deephaven 42.9 installs (`io.deephaven.server.uri.UriModule`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolver {
    /// Behavior: io.deephaven.server.uri.QueryScopeResolver
    QueryScope,
    /// Behavior: io.deephaven.server.uri.ApplicationResolver
    Application,
    /// Behavior: io.deephaven.server.uri.BarrageTableResolver
    BarrageTable,
    /// Behavior: io.deephaven.server.uri.CsvTableResolver
    Csv,
    /// Behavior: io.deephaven.server.uri.ParquetTableResolver
    Parquet,
}

const RESOLVERS: [Resolver; 5] = [
    Resolver::QueryScope,
    Resolver::Application,
    Resolver::BarrageTable,
    Resolver::Csv,
    Resolver::Parquet,
];

const CSV_SCHEMES: [&str; 7] = [
    "csv+http",
    "http+csv",
    "csv+https",
    "https+csv",
    "csv+file",
    "file+csv",
    "csv",
];

impl Resolver {
    /// The schemes the resolver is registered for.
    ///
    /// Behavior: `UriResolver#schemes` of each resolver
    fn schemes(self) -> &'static [&'static str] {
        match self {
            Resolver::QueryScope | Resolver::Application => &["dh"],
            Resolver::BarrageTable => &["dh", "dh+plain"],
            Resolver::Csv => &CSV_SCHEMES,
            Resolver::Parquet => &["parquet"],
        }
    }

    /// Whether the resolver takes `uri`, whose scheme is one of its own.
    ///
    /// Behavior: `UriResolver#isResolvable` of each resolver. The Barrage resolver takes
    /// proxied and custom inner URIs too, which it then refuses: Java does so on purpose,
    /// for the better error.
    fn is_resolvable(self, uri: &Uri) -> bool {
        match self {
            Resolver::QueryScope => QueryScopeUri::is_well_formed(uri),
            Resolver::Application => ApplicationUri::is_well_formed(uri),
            Resolver::BarrageTable => RemoteUri::is_well_formed(uri),
            Resolver::Csv => uri.scheme().is_some_and(|s| CSV_SCHEMES.contains(&s)),
            Resolver::Parquet => uri.scheme() == Some("parquet") && is_local_path(uri),
        }
    }

    /// How Java's `Class#toString` names the resolver, for the error that names several.
    pub fn java_class(self) -> &'static str {
        match self {
            Resolver::QueryScope => "class io.deephaven.server.uri.QueryScopeResolver",
            Resolver::Application => "class io.deephaven.server.uri.ApplicationResolver",
            Resolver::BarrageTable => "class io.deephaven.server.uri.BarrageTableResolver",
            Resolver::Csv => "class io.deephaven.server.uri.CsvTableResolver",
            Resolver::Parquet => "class io.deephaven.server.uri.ParquetTableResolver",
        }
    }
}

/// `s` as a URI, with the error `URI.create` throws.
///
/// Behavior: java.net.URI#create
pub fn parse(s: &str) -> Result<Uri, ResolveError> {
    Uri::parse(s).map_err(|error| ResolveError::IllegalArgument(error.to_string()))
}

/// The one resolver that takes `uri`: those registered for its scheme exactly (a relative
/// URI has none), that take its shape.
///
/// Behavior: io.deephaven.uri.resolver.UriResolvers#resolver
pub fn resolver_for(uri: &Uri) -> Result<Resolver, ResolveError> {
    let scheme = uri.scheme();
    let found: Vec<Resolver> = RESOLVERS
        .into_iter()
        .filter(|r| scheme.is_some_and(|s| r.schemes().contains(&s)) && r.is_resolvable(uri))
        .collect();
    match found.as_slice() {
        [] => Err(ResolveError::UnsupportedOperation(format!(
            "Unable to find resolver for uri '{uri}'"
        ))),
        [one] => Ok(*one),
        several => {
            let classes: Vec<&str> = several.iter().map(|r| r.java_class()).collect();
            Err(ResolveError::UnsupportedOperation(format!(
                "Found multiple resolvers for uri '{uri}': [{}]",
                classes.join(",")
            )))
        }
    }
}

/// Owner question 7.23: whether a URI whose authority Java cannot read as a host
/// (`dh://host_name/scope/t`, `dh://é.com/scope/t`, `dh://host:99999999999/scope/t`) is
/// resolved here, with its authority dropped, as Java resolves it. Such an authority is
/// registry-based for `java.net.URI` and has no host, so `UriHelper#isLocalPath` takes the
/// URI as local: the query scope variable `t` of this server, not a table of the server the
/// user named. The same holds for `parquet://` and for the CSV schemes, whose resolvers
/// drop that authority too.
///
/// `true` matches Java. This is the one place to change it: with `false`, such a URI is
/// refused by [`check_unread_host`].
const UNREAD_HOST_IS_LOCAL: bool = true;

/// The authority of `uri` that a resolver drops because it has no host Java can read.
///
/// Behavior: io.deephaven.uri.UriHelper#isLocalPath, which asks for no host and not for no
/// authority
fn unread_authority(uri: &Uri) -> Option<&str> {
    uri.raw_authority().filter(|_| uri.host().is_none())
}

/// Refuses a URI whose authority would be dropped, unless [`UNREAD_HOST_IS_LOCAL`].
fn check_unread_host(uri: &Uri) -> Result<(), ResolveError> {
    match unread_authority(uri) {
        Some(authority) if !UNREAD_HOST_IS_LOCAL => Err(ResolveError::IllegalArgument(format!(
            "'{authority}' in '{uri}' is no host name, address or port that can be read; \
             the URI would resolve on this server"
        ))),
        _ => Ok(()),
    }
}

/// The ticket of the table a remote URI names on its server. Proxied and custom inner URIs
/// are refused here, before any connection.
///
/// Behavior: io.deephaven.server.uri.BarrageTableResolver.RemoteResolver
pub fn remote_ticket(remote: &RemoteUri) -> Result<Vec<u8>, ResolveError> {
    const ISSUE: &str = "see https://github.com/deephaven/deephaven-core/issues/1483";
    match remote.uri() {
        StructuredUri::QueryScope(scope) => Ok(scope_ticket(scope.variable_name())),
        StructuredUri::Application(field) => Ok(application_field_ticket(
            field.application_id(),
            field.field_name(),
        )),
        // the target's host is the application id
        StructuredUri::Field(field) => Ok(application_field_ticket(
            remote.target().host(),
            field.field_name(),
        )),
        StructuredUri::Remote(_) => Err(ResolveError::UnsupportedOperation(format!(
            "Proxying not supported yet, {ISSUE}"
        ))),
        StructuredUri::Custom(_) => Err(ResolveError::UnsupportedOperation(format!(
            "Remote custom URIs not supported yet, {ISSUE}"
        ))),
    }
}

/// The client configuration of a target: TLS for `dh`, which the client refuses
/// (`ClientError::TlsNotAvailable`, owner question 7.24), and the default port of the
/// scheme when the target names none.
///
/// Behavior: `io.deephaven.client.impl.ClientConfig` with `target`, and
/// `io.deephaven.client.impl.ChannelHelper` (the default ports)
pub fn client_config(target: &DeephavenTarget) -> Result<ClientConfig, ClientError> {
    let port = target.port_or_default();
    let port = u16::try_from(port)
        .map_err(|_| ClientError::Config(format!("port {port} of {target} is out of range")))?;
    let mut config = ClientConfig::new(target.host(), port);
    config.use_tls = target.is_secure();
    Ok(config)
}

/// The runtime the Barrage client of the resolvers and of `deephaven.barrage` runs on: two
/// threads, for the life of the process. Sessions spawn their ping and the readers of their
/// subscriptions on it, so it must outlive them; and a caller on any thread, a runtime's
/// own among them, can wait for it.
fn client_runtime() -> Result<&'static tokio::runtime::Runtime, ClientError> {
    static RUNTIME: OnceLock<Result<tokio::runtime::Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("barrage-client")
                .enable_all()
                .build()
                .map_err(|e| e.to_string())
        })
        .as_ref()
        .map_err(|problem| {
            ClientError::Connect(format!("the Barrage client cannot start: {problem}"))
        })
}

/// Runs `future` on the client's runtime and waits for it on this thread.
pub fn block_on<T: Send + 'static>(
    future: impl Future<Output = Result<T, ClientError>> + Send + 'static,
) -> Result<T, ClientError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    client_runtime()?.spawn(async move {
        let _ = sender.send(future.await);
    });
    receiver.recv().unwrap_or_else(|_| {
        Err(ClientError::Protocol(
            "the Barrage client's task ended without an answer".to_string(),
        ))
    })
}

/// Runs `wait`, a wait for a subscription, without this thread's holds of the update
/// graph's lock: a subscribed table is made under the shared lock, which a waiting cycle
/// would keep from the client, and the table is complete only after a cycle has applied
/// what arrived. The holds are taken again afterwards, so what the thread read from
/// refreshing tables before may have changed. Refused inside a cycle, which cannot wait
/// for another.
///
/// Behavior: io.deephaven.engine.updategraph.UpdateGraphAwareCompletableFuture#get, which
/// waits on a condition of the exclusive lock when the thread holds that lock
pub fn waiting_for_subscription<T>(
    graph: &PeriodicUpdateGraph,
    wait: impl FnOnce() -> T,
) -> Result<T, ResolveError> {
    if graph.current_thread_processes_updates() {
        return Err(ResolveError::CannotWait(
            "a remote table cannot be subscribed to inside an update cycle: the \
             subscription waits for a cycle",
        ));
    }
    Ok(yielding_to_cycles(|| {
        graph.stepped().waiting_for_cycle(wait)
    }))
}

/// Subscribes to `ticket` of `session` and waits, as [`waiting_for_subscription`] says, for
/// the whole table.
///
/// Behavior: `BarrageSession#subscribe` then `BarrageSubscription#entireTable().get()`
pub fn subscribe(
    graph: &PeriodicUpdateGraph,
    session: &BarrageSession,
    ticket: Vec<u8>,
    options: SubscriptionOptions,
) -> Result<Arc<dyn Table>, ResolveError> {
    let session = session.clone();
    let subscribed = waiting_for_subscription(graph, || {
        block_on(async move { session.subscribe(&ticket, options).await })
    })?;
    Ok(subscribed?)
}

/// A snapshot of `ticket` of `session`. It needs no update cycle, so the thread keeps its
/// holds of the graph's lock while it waits, as Java's does.
///
/// Behavior: `BarrageSession#snapshot` then `BarrageSnapshot#entireTable().get()`
pub fn snapshot(
    session: &BarrageSession,
    ticket: Vec<u8>,
    options: SnapshotOptions,
) -> Result<Arc<dyn Table>, ResolveError> {
    let session = session.clone();
    Ok(block_on(async move {
        session.snapshot(&ticket, options).await
    })?)
}

/// What wakes `graph` when a subscription has something for its next cycle.
pub fn wake_of(graph: &Arc<PeriodicUpdateGraph>) -> Wake {
    let graph = Arc::downgrade(graph);
    Arc::new(move || {
        if let Some(graph) = graph.upgrade() {
            graph.request_cycle();
        }
    })
}

/// Connects to the server of `config`; its subscriptions are tables of `graph`.
pub fn connect(
    graph: &Arc<PeriodicUpdateGraph>,
    config: ClientConfig,
) -> Result<BarrageSession, ClientError> {
    let wake = wake_of(graph);
    let tables: Arc<dyn UpdateGraph> = graph.clone();
    block_on(async move { BarrageSession::connect(config, tables, Some(wake)).await })
}

/// The resolvers of one server.
pub struct UriResolvers {
    scope: Arc<Scope>,
    applications: Arc<Applications>,
    graph: Arc<PeriodicUpdateGraph>,
    /// One Barrage session per target, never closed (deephaven-core#1482).
    sessions: Mutex<HashMap<DeephavenTarget, BarrageSession>>,
}

impl UriResolvers {
    pub fn new(state: &ServerState) -> Self {
        Self {
            scope: state.scope.clone(),
            applications: state.applications.clone(),
            graph: state.graph.clone(),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    fn sessions(&self) -> MutexGuard<'_, HashMap<DeephavenTarget, BarrageSession>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Resolves `s` into an object: `None` for a variable or field that does not exist or
    /// holds the language's null, as Java's `null`.
    ///
    /// Behavior: io.deephaven.uri.ResolveTools#resolve(String)
    pub fn resolve(&self, s: &str) -> Result<Option<ExportValue>, ResolveError> {
        let uri = parse(s)?;
        let resolver = resolver_for(&uri)?;
        self.resolve_with(resolver, &uri)
    }

    /// Resolves `uri` with `resolver`, which [`resolver_for`] chose.
    ///
    /// Behavior: `UriResolver#resolve` of each resolver
    pub fn resolve_with(
        &self,
        resolver: Resolver,
        uri: &Uri,
    ) -> Result<Option<ExportValue>, ResolveError> {
        match resolver {
            Resolver::QueryScope => {
                check_unread_host(uri)?;
                let variable = QueryScopeUri::from_uri(uri)?;
                // Behavior: `QueryScope#readParamValue(name, null)`
                Ok(self.scope.get(variable.variable_name()))
            }
            Resolver::Application => {
                check_unread_host(uri)?;
                let field = ApplicationUri::from_uri(uri)?;
                let application = self.applications.get(field.application_id());
                Ok(application.and_then(|a| a.field(field.field_name())))
            }
            Resolver::BarrageTable => {
                let table = self.remote_table(&RemoteUri::from_uri(uri)?)?;
                Ok(Some(ExportValue::Table(table)))
            }
            Resolver::Csv => {
                check_unread_host(uri)?;
                Ok(Some(ExportValue::Table(read_csv(uri)?)))
            }
            Resolver::Parquet => {
                check_unread_host(uri)?;
                Ok(Some(ExportValue::Table(read_parquet(uri)?)))
            }
        }
    }

    /// A subscription to the table `remote` names, through the session of its target.
    ///
    /// Behavior: io.deephaven.server.uri.BarrageTableResolver#subscribe(RemoteUri)
    pub fn remote_table(&self, remote: &RemoteUri) -> Result<Arc<dyn Table>, ResolveError> {
        let ticket = remote_ticket(remote)?;
        let session = self.session(remote.target())?;
        subscribe(&self.graph, &session, ticket, SubscriptionOptions::RESOLVER)
    }

    /// The session of `target`, connected the first time it is asked for. A failed
    /// connection is not kept, and the next call tries again.
    ///
    /// Behavior: io.deephaven.server.uri.BarrageTableResolver#session, which makes it with
    /// no authentication (deephaven-core#3421), so as `Anonymous`
    pub fn session(&self, target: &DeephavenTarget) -> Result<BarrageSession, ResolveError> {
        let mut sessions = self.sessions();
        if let Some(session) = sessions.get(target).filter(|s| !s.is_closed()) {
            return Ok(session.clone());
        }
        let session = connect(&self.graph, client_config(target)?)?;
        sessions.insert(target.clone(), session.clone());
        Ok(session)
    }
}

/// The file URL a CSV URI stands for: the URI with the scheme `http`, `https` or `file`
/// and the other components, re-quoted.
///
/// Behavior: io.deephaven.server.uri.CsvTableResolver#csvString and #replaceScheme
pub fn csv_string(uri: &Uri) -> Result<String, ResolveError> {
    let scheme = match uri.scheme().unwrap_or_default() {
        "csv+http" | "http+csv" => "http",
        "csv+https" | "https+csv" => "https",
        "csv+file" | "file+csv" | "csv" => "file",
        other => {
            return Err(ResolveError::IllegalArgument(format!(
                "Unexpected scheme '{other}'"
            )))
        }
    };
    let rebuilt = Uri::from_components(
        Some(scheme),
        uri.user_info().as_deref(),
        uri.host(),
        uri.port(),
        uri.path().as_deref(),
        uri.query().as_deref(),
        uri.fragment().as_deref(),
    )
    // `IllegalArgumentException(URISyntaxException)`, whose message is the cause's
    // `toString`
    .map_err(|e| ResolveError::IllegalArgument(format!("java.net.URISyntaxException: {e}")))?;
    Ok(rebuilt.to_string())
}

/// Behavior: io.deephaven.server.uri.CsvTableResolver#read, which reads with
/// `io.deephaven.csv.CsvTools#readCsv(String)`
#[cfg(feature = "python")]
fn read_csv(uri: &Uri) -> Result<Arc<dyn Table>, ResolveError> {
    let path = csv_string(uri)?;
    let table = oxhaven_csv::read(&path, &oxhaven_csv::ReadOptions::default())
        .map_err(|e| ResolveError::Read(e.to_string()))?;
    Ok(table)
}

/// Behavior: io.deephaven.server.uri.ParquetTableResolver#resolve, which reads the decoded
/// path with `io.deephaven.parquet.table.ParquetTools#readTable(String)`
#[cfg(feature = "python")]
fn read_parquet(uri: &Uri) -> Result<Arc<dyn Table>, ResolveError> {
    let path = uri.path().unwrap_or_default();
    let table = oxhaven_parquet::read(&path, &oxhaven_parquet::ReadOptions::default())
        .map_err(|e| ResolveError::Read(e.to_string()))?;
    Ok(table)
}

/// The readers are in the build with the console, the only place a URI is resolved from.
#[cfg(not(feature = "python"))]
fn read_csv(uri: &Uri) -> Result<Arc<dyn Table>, ResolveError> {
    csv_string(uri)?;
    Err(ResolveError::Read(
        "this build has no CSV reader: it is part of the build with the Python console".to_string(),
    ))
}

#[cfg(not(feature = "python"))]
fn read_parquet(_: &Uri) -> Result<Arc<dyn Table>, ResolveError> {
    Err(ResolveError::Read(
        "this build has no Parquet reader: it is part of the build with the Python console"
            .to_string(),
    ))
}
