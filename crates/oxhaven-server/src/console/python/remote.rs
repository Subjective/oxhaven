//! `deephaven.uri` and `deephaven.barrage` reach the URI resolvers (`crate::uri`) and the
//! Barrage client (`crate::barrage_client`) here.
//!
//! A subscription waits for an update cycle of this server, so its wait gives the graph's
//! lock up (`crate::uri::waiting_for_subscription`), as a command's blocking write to an
//! input table does; it is refused inside a cycle and under a shared lock that Python code
//! took itself, where no cycle can run. The interpreter lock is released for every wait
//! on the network.

use std::sync::{Arc, PoisonError, RwLock};

use oxhaven_uri::{DeephavenTarget, Uri};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::{PythonSession, TableHandle};
use crate::barrage_client::{BarrageSession, ClientError, SnapshotOptions, SubscriptionOptions};
use crate::update_graph::PeriodicUpdateGraph;
use crate::uri::{self, ResolveError, Resolver, UriResolvers};
use crate::ServerState;

/// What the modules reach: the resolvers and the graph of the server whose Python session
/// was started last.
struct Remote {
    resolvers: UriResolvers,
    graph: Arc<PeriodicUpdateGraph>,
}

static REMOTE: RwLock<Option<Arc<Remote>>> = RwLock::new(None);

pub(super) fn install(state: &ServerState) {
    let remote = Remote {
        resolvers: UriResolvers::new(state),
        graph: state.graph.clone(),
    };
    *REMOTE.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(remote));
}

fn remote() -> PyResult<Arc<Remote>> {
    REMOTE
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .ok_or_else(|| PyRuntimeError::new_err("the server has no Python session"))
}

fn resolve_error(error: ResolveError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn client_error(error: ClientError) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

/// Refuses a subscription on a thread where Python code holds the shared lock: the
/// subscription waits for a cycle, which that lock keeps out. A command's own hold is
/// given up for the wait instead.
fn refuse_under_shared_lock() -> PyResult<()> {
    if super::listener::holds_the_shared_lock_only() {
        return Err(PyRuntimeError::new_err(
            "a remote table cannot be subscribed to while holding the update graph's shared \
             lock: the subscription waits for an update cycle, which the lock keeps out",
        ));
    }
    Ok(())
}

/// The object `uri` names, as the package's wrapper where it has one; `None` for a
/// variable or field that does not exist or holds `None`.
///
/// Behavior: `deephaven.uri.resolve`, which calls `io.deephaven.uri.ResolveTools#resolve`
#[pyfunction]
fn resolve_uri<'py>(py: Python<'py>, uri: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    let remote = remote()?;
    let parsed = uri::parse(uri).map_err(resolve_error)?;
    let resolver = uri::resolver_for(&parsed).map_err(resolve_error)?;
    if resolver == Resolver::BarrageTable {
        refuse_under_shared_lock()?;
    }
    let value = py
        .detach(|| remote.resolvers.resolve_with(resolver, &parsed))
        .map_err(resolve_error)?;
    value.map(|value| PythonSession::wrap(py, value)).transpose()
}

/// A session with another server, as `deephaven.barrage.BarrageSession` holds it.
#[pyclass(frozen, module = "_oxhaven")]
struct BarrageSessionHandle {
    session: BarrageSession,
    graph: Arc<PeriodicUpdateGraph>,
}

#[pymethods]
impl BarrageSessionHandle {
    /// A refreshing table that follows the remote table of `ticket`.
    ///
    /// Behavior: `io.deephaven.client.impl.BarrageSession#subscribe` with
    /// `BarrageTableResolver.SUB_OPTIONS`, then `entireTable().get()`
    fn subscribe(&self, py: Python<'_>, ticket: &[u8]) -> PyResult<TableHandle> {
        refuse_under_shared_lock()?;
        let ticket = ticket.to_vec();
        let options = SubscriptionOptions::RESOLVER;
        let table = py
            .detach(|| uri::subscribe(&self.graph, &self.session, ticket, options))
            .map_err(resolve_error)?;
        Ok(TableHandle { table })
    }

    /// A static table with the rows the remote table of `ticket` has now.
    ///
    /// Behavior: `io.deephaven.client.impl.BarrageSession#snapshot` with
    /// `BarrageTableResolver.SNAP_OPTIONS`, then `entireTable().get()`
    fn snapshot(&self, py: Python<'_>, ticket: &[u8]) -> PyResult<TableHandle> {
        let ticket = ticket.to_vec();
        let table = py
            .detach(|| uri::snapshot(&self.session, ticket, SnapshotOptions::resolver()))
            .map_err(resolve_error)?;
        Ok(TableHandle { table })
    }

    /// Closes the session; its subscribed tables fail on the next cycle. Closing a closed
    /// session does nothing.
    ///
    /// Behavior: `io.deephaven.client.impl.BarrageSession#close`
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let session = self.session.clone();
        py.detach(|| uri::block_on(async move { session.close().await }))
            .map_err(client_error)
    }

    /// `host:port` of the other server.
    fn target(&self) -> String {
        self.session.target().to_string()
    }

    fn is_closed(&self) -> bool {
        self.session.is_closed()
    }
}

/// A new session with the server `target_uri` names (`dh://host:port` or
/// `dh+plain://host:port`), authenticated with `authorization` (`None` for `Anonymous`).
///
/// Behavior: `deephaven.barrage.barrage_session`, which makes the target with
/// `DeephavenTarget.of(new URI(...))` and the session with
/// `SessionFactoryCreator#barrageFactory`
#[pyfunction]
#[pyo3(signature = (target_uri, authorization, extra_headers, tls_root_certs))]
fn barrage_session(
    py: Python<'_>,
    target_uri: &str,
    authorization: Option<&str>,
    extra_headers: Vec<(String, String)>,
    tls_root_certs: Option<Vec<u8>>,
) -> PyResult<BarrageSessionHandle> {
    let remote = remote()?;
    // `new URI(String)` throws the checked exception itself
    let parsed = Uri::parse(target_uri).map_err(|e| {
        PyRuntimeError::new_err(format!("java.net.URISyntaxException: {e}"))
    })?;
    let target = DeephavenTarget::from_uri(&parsed)
        .map_err(|e| resolve_error(ResolveError::from(e)))?;
    let mut config = uri::client_config(&target).map_err(client_error)?;
    if let Some(authorization) = authorization {
        config.authorization = authorization.to_string();
    }
    config.extra_headers = extra_headers;
    config.tls_root_certs = tls_root_certs;
    let graph = remote.graph.clone();
    let session = py
        .detach(|| uri::connect(&graph, config))
        .map_err(client_error)?;
    Ok(BarrageSessionHandle { session, graph })
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<BarrageSessionHandle>()?;
    native.add_function(wrap_pyfunction!(resolve_uri, native)?)?;
    native.add_function(wrap_pyfunction!(barrage_session, native)?)?;
    Ok(())
}
