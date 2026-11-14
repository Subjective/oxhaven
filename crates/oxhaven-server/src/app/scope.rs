//! The query scope: named values, and the tickets `s/<name>` that name them.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tonic::Status;

use crate::session::{Export, ExportBuilder, ExportValue, Session, TicketResolver};

/// The first byte of every scope ticket.
///
/// Behavior: `io.deephaven.proto.util.ScopeTicketHelper#TICKET_PREFIX`
pub const SCOPE_TICKET_ROUTE: u8 = b's';

/// The ticket of the scope variable `name`.
///
/// Behavior: `io.deephaven.proto.util.ScopeTicketHelper#nameToBytes`
pub fn scope_ticket(name: &str) -> Vec<u8> {
    format!("s/{name}").into_bytes()
}

/// The variables of the server, by name. In Java these are the variables of the script
/// session; here the process, a client that publishes to a scope ticket, and later a script
/// session put them.
///
/// Behavior: `io.deephaven.engine.context.QueryScope`
#[derive(Default)]
pub struct Scope {
    variables: Mutex<BTreeMap<String, ExportValue>>,
}

impl Scope {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, ExportValue>> {
        self.variables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Sets the variable `name`, replacing the value it had.
    ///
    /// Behavior: `QueryScope#putParam`
    pub fn put(&self, name: impl Into<String>, value: ExportValue) {
        self.lock().insert(name.into(), value);
    }

    /// The value of the variable `name`.
    ///
    /// Behavior: `QueryScope#readParamValue(String)`
    pub fn get(&self, name: &str) -> Option<ExportValue> {
        self.lock().get(name).cloned()
    }

    /// Removes the variable `name` and returns the value it had.
    pub fn remove(&self, name: &str) -> Option<ExportValue> {
        self.lock().remove(name)
    }

    /// The names of the variables, sorted.
    ///
    /// Behavior: `QueryScope#getParamNames`
    pub fn names(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }
}

/// Resolves scope tickets against the [`Scope`] of the server, and puts published values
/// into it.
///
/// Behavior: `io.deephaven.server.console.ScopeTicketResolver`
pub struct ScopeTicketResolver {
    scope: Arc<Scope>,
}

impl ScopeTicketResolver {
    pub fn new(scope: Arc<Scope>) -> Self {
        Self { scope }
    }
}

/// The variable a ticket names: what follows `s/`. Bytes that are not UTF-8 read as U+FFFD.
///
/// Behavior: `ScopeTicketResolver#nameForTicket`
fn name_for_ticket(ticket: &[u8], log_id: &str) -> Result<String, Status> {
    if ticket.is_empty() {
        return Err(Status::failed_precondition(format!(
            "Could not resolve '{log_id}': no ticket supplied"
        )));
    }
    match ticket {
        [SCOPE_TICKET_ROUTE, b'/', name @ ..] if !name.is_empty() => {
            Ok(String::from_utf8_lossy(name).into_owned())
        }
        _ => {
            let hex: String = ticket.iter().map(|byte| format!("{byte:02x}")).collect();
            // the quote that is never opened is Java's
            Err(Status::failed_precondition(format!(
                "Could not resolve '{log_id}': found 0x{hex}' (hex)"
            )))
        }
    }
}

impl TicketResolver for ScopeTicketResolver {
    fn route(&self) -> u8 {
        SCOPE_TICKET_ROUTE
    }

    /// Behavior: `ScopeTicketResolver#getLogNameFor`
    fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status> {
        Ok(format!("scope/{}", name_for_ticket(ticket, log_id)?))
    }

    /// The value of the variable now; there is no waiting for a variable to appear.
    ///
    /// Behavior: `ScopeTicketResolver#resolve(SessionState, ByteBuffer, String)`
    fn resolve(
        &self,
        _session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status> {
        let name = name_for_ticket(ticket, log_id)?;
        // the quote that is never closed is Java's
        self.scope.get(&name).map(Export::ready).ok_or_else(|| {
            Status::not_found(format!(
                "Could not resolve '{log_id}: variable '{name}' not found"
            ))
        })
    }

    /// The value the caller defines becomes the variable, and `on_publish` runs after that.
    ///
    /// Behavior: `ScopeTicketResolver#publish(SessionState, String, String, Runnable)`
    fn publish(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        let name = name_for_ticket(ticket, log_id)?;
        let result = session.non_export()?;
        let scope = self.scope.clone();
        session
            .non_export()?
            .requires_serial_queue()
            .require([result.export().clone()])
            .submit(move |mut values| {
                scope.put(name, values.remove(0));
                if let Some(on_publish) = on_publish {
                    on_publish();
                }
                Ok(ExportValue::unit())
            })?;
        Ok(result)
    }
}
