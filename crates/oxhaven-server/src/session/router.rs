//! Routes a ticket to the resolver for its first byte. Export tickets are resolved here; the
//! services that own the other ticket kinds register their resolvers with the router.

use std::collections::BTreeMap;
use std::sync::Arc;

use tonic::Status;

use super::export::{Export, ExportBuilder, ExportError, ExportValue};
use super::state::Session;
use super::ticket::{export_log_name, ticket_to_export_id, EXPORT_TICKET_ROUTE};

/// Resolves and publishes the tickets that start with one route byte. Each kind of ticket
/// (export, scope variable, application field, shared) has its own implementation.
///
/// Behavior: `io.deephaven.server.session.TicketResolver`
pub trait TicketResolver: Send + Sync {
    /// The first byte of the tickets this resolver handles.
    fn route(&self) -> u8;

    /// A readable name for the ticket, for logs and descriptions.
    fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status>;

    /// The export the ticket names. `session` is `None` for a call without a session.
    fn resolve(
        &self,
        session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status>;

    /// Starts the definition of the value the ticket will name. `on_publish` runs once the
    /// value is available.
    fn publish(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status>;
}

/// Behavior: `io.deephaven.server.session.TicketRouter`
pub struct TicketRouter {
    resolvers: BTreeMap<u8, Arc<dyn TicketResolver>>,
}

impl TicketRouter {
    /// Fails if two resolvers claim the same route byte.
    pub fn new(resolvers: Vec<Arc<dyn TicketResolver>>) -> Result<Self, String> {
        let mut by_route = BTreeMap::new();
        for resolver in resolvers {
            let route = resolver.route();
            if by_route.insert(route, resolver).is_some() {
                return Err(format!(
                    "Duplicate ticket resolver for ticket route {route}"
                ));
            }
        }
        Ok(Self {
            resolvers: by_route,
        })
    }

    /// Behavior: `io.deephaven.server.session.TicketRouter#getResolver(byte, String)`
    fn resolver(&self, route: u8, log_id: &str) -> Result<&Arc<dyn TicketResolver>, Status> {
        self.resolvers.get(&route).ok_or_else(|| {
            Status::failed_precondition(format!(
                "Could not resolve '{log_id}': no resolver for route '{route}' (byte)"
            ))
        })
    }

    /// A readable name for the ticket. An empty ticket is `INVALID_ARGUMENT`: Java reads the
    /// route byte without a length check, and the resulting exception reaches the client as
    /// that status.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#getLogNameFor(ByteBuffer, String)`
    pub fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status> {
        let route = *ticket.first().ok_or_else(|| {
            Status::invalid_argument(format!("could not name '{log_id}': it's an empty ticket"))
        })?;
        self.resolver(route, log_id)?.log_name(ticket, log_id)
    }

    /// The export a ticket names. An empty ticket, an unknown route and a ticket its
    /// resolver cannot name are errors of the call. A failure of the resolver itself is
    /// returned as an already failed export, so that it reaches the client through the
    /// error handler of whatever requires it.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#resolve(SessionState, ByteBuffer,
    /// String)`
    pub fn resolve(
        &self,
        session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status> {
        if ticket.is_empty() {
            return Err(Status::failed_precondition(format!(
                "could not resolve '{log_id}' it's an empty ticket"
            )));
        }
        self.log_name(ticket, log_id)?;
        let resolver = self.resolver(ticket[0], log_id)?;
        Ok(resolver
            .resolve(session, ticket, log_id)
            .unwrap_or_else(|status| Export::failed(ExportError::Status(status))))
    }

    /// Starts the definition of the value a ticket will name.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#publish(SessionState, ByteBuffer,
    /// String, Runnable)`
    pub fn publish(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        self.log_name(ticket, log_id)?;
        self.resolver(ticket[0], log_id)?
            .publish(session, ticket, log_id, on_publish)
    }

    /// Makes `ticket` name the value of `source` once that is available.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#publish(SessionState, Ticket,
    /// String, Runnable, ExportErrorHandler, ExportObject)`,
    /// `TicketResolver#publish(.., ExportErrorHandler, ExportObject)`
    pub fn publish_from(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
        on_error: impl FnOnce(Status) + Send + 'static,
        source: Export,
    ) -> Result<(), Status> {
        self.publish(session, ticket, log_id, on_publish)?
            .on_error(on_error)
            .require([source])
            .submit(pass_through)?;
        Ok(())
    }
}

/// Work that exports the value of its single dependency.
pub(super) fn pass_through(mut inputs: Vec<ExportValue>) -> Result<ExportValue, ExportError> {
    Ok(inputs.remove(0))
}

/// Resolves export tickets: `e` followed by the export id.
///
/// Behavior: `io.deephaven.server.session.ExportTicketResolver`
pub struct ExportTicketResolver;

impl TicketResolver for ExportTicketResolver {
    fn route(&self) -> u8 {
        EXPORT_TICKET_ROUTE
    }

    fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status> {
        Ok(export_log_name(ticket_to_export_id(ticket, log_id)?))
    }

    fn resolve(
        &self,
        session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status> {
        let session = session.ok_or_else(|| {
            Status::unauthenticated(format!(
                "Could not resolve '{log_id}': no exports can exist without an active session"
            ))
        })?;
        session.get_export(ticket_to_export_id(ticket, log_id)?)
    }

    fn publish(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        let to_publish = session.new_export_for_ticket(ticket, log_id)?;
        if let Some(on_publish) = on_publish {
            session
                .non_export()?
                .require([to_publish.export().clone()])
                .submit(move |_| {
                    on_publish();
                    Ok(ExportValue::unit())
                })?;
        }
        Ok(to_publish)
    }
}
