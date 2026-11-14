//! Routes a ticket to the resolver for its first byte, and a Flight path descriptor to the
//! resolver for its first path element. Export tickets are resolved here and shared tickets
//! in [`super::shared`]; the services that own the other ticket kinds register their
//! resolvers with the router.

use std::collections::BTreeMap;
use std::sync::Arc;

use oxhaven_proto::flight::flight_descriptor::DescriptorType;
use oxhaven_proto::flight::FlightDescriptor;
use tonic::Status;

use super::export::{Export, ExportBuilder, ExportError, ExportValue};
use super::state::Session;
use super::ticket::{
    descriptor_to_export_id, export_id_to_bytes, export_log_name, ticket_to_export_id,
    EXPORT_DESCRIPTOR_ROUTE, EXPORT_TICKET_ROUTE,
};

/// What `GetFlightInfo` and `GetSchema` need to answer for a path descriptor.
pub struct DescribedFlight {
    /// The export the descriptor names.
    pub export: Export,
    /// The ticket to read the flight by, or why the descriptor has none. It is looked at
    /// only once the export holds a table.
    pub ticket: Result<Vec<u8>, Status>,
    /// The answer if the export holds something other than a table.
    pub not_a_table: Status,
}

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

    /// Makes `ticket` name the value of `source`. `on_publish` runs once the ticket can be
    /// resolved, `on_error` if it never will.
    ///
    /// Behavior: `TicketResolver#publish(SessionState, ByteBuffer, String, Runnable,
    /// ExportErrorHandler, ExportObject)`
    fn publish_from(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
        on_error: Box<dyn FnOnce(Status) + Send>,
        source: Export,
    ) -> Result<(), Status> {
        self.publish(session, ticket, log_id, on_publish)?
            .on_error(on_error)
            .require([source])
            .submit(pass_through)?;
        Ok(())
    }

    /// The first path element of the Flight descriptors this resolver handles, if it
    /// handles any. The two descriptor methods are called only for a resolver that has one.
    ///
    /// Behavior: `io.deephaven.server.session.PathResolverPrefixedBase#flightDescriptorRoute`
    fn descriptor_route(&self) -> Option<&'static str> {
        None
    }

    /// The flight a path descriptor names. `session` is `None` for a call without a session.
    ///
    /// Behavior: `TicketResolver#flightInfoFor`
    fn flight_for(
        &self,
        _session: Option<&Arc<Session>>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<DescribedFlight, Status> {
        Err(no_path_resolver(descriptor, log_id))
    }

    /// Starts the definition of the value a path descriptor will name.
    ///
    /// Behavior: `TicketResolver#publish(SessionState, Flight.FlightDescriptor, String,
    /// Runnable)`
    fn publish_descriptor(
        &self,
        _session: &Arc<Session>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<ExportBuilder, Status> {
        Err(no_path_resolver(descriptor, log_id))
    }
}

fn no_path_resolver(descriptor: &FlightDescriptor, log_id: &str) -> Status {
    Status::failed_precondition(format!(
        "Could not resolve '{log_id}': no resolver for path route '{}'",
        descriptor.path.first().map_or("", String::as_str)
    ))
}

/// Behavior: `io.deephaven.server.session.TicketRouter`
pub struct TicketRouter {
    resolvers: BTreeMap<u8, Arc<dyn TicketResolver>>,
    path_resolvers: BTreeMap<&'static str, Arc<dyn TicketResolver>>,
}

impl TicketRouter {
    /// Fails if two resolvers claim the same route byte or the same descriptor route.
    pub fn new(resolvers: Vec<Arc<dyn TicketResolver>>) -> Result<Self, String> {
        let mut by_route = BTreeMap::new();
        let mut by_path = BTreeMap::new();
        for resolver in resolvers {
            let route = resolver.route();
            if by_route.insert(route, resolver.clone()).is_some() {
                return Err(format!(
                    "Duplicate ticket resolver for ticket route {route}"
                ));
            }
            if let Some(path) = resolver.descriptor_route() {
                if by_path.insert(path, resolver).is_some() {
                    return Err(format!(
                        "Duplicate ticket resolver for descriptor route {path}"
                    ));
                }
            }
        }
        Ok(Self {
            resolvers: by_route,
            path_resolvers: by_path,
        })
    }

    /// The resolver for a descriptor: the one whose route is the first element of its path.
    /// No resolver takes commands.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#getResolver(FlightDescriptor,
    /// String)`, `TicketRouter#getPathResolver`, `TicketRouter#getCommandResolver`
    fn path_resolver(
        &self,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<&Arc<dyn TicketResolver>, Status> {
        let unresolved =
            |why: &str| Status::failed_precondition(format!("Could not resolve '{log_id}': {why}"));
        if descriptor.r#type == DescriptorType::Cmd as i32 {
            return Err(unresolved("no resolver for command"));
        }
        if descriptor.r#type != DescriptorType::Path as i32 {
            return Err(unresolved("unexpected type"));
        }
        let route = descriptor
            .path
            .first()
            .ok_or_else(|| unresolved("flight descriptor does not have route path"))?;
        self.path_resolvers
            .get(route.as_str())
            .ok_or_else(|| no_path_resolver(descriptor, log_id))
    }

    /// The flight a descriptor names. Unlike [`TicketRouter::resolve`], a failure of the
    /// resolver is an error of the call.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#flightInfoFor`
    pub fn flight_for(
        &self,
        session: Option<&Arc<Session>>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<DescribedFlight, Status> {
        self.path_resolver(descriptor, log_id)?
            .flight_for(session, descriptor, log_id)
    }

    /// Starts the definition of the value a descriptor will name.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#publish(SessionState,
    /// Flight.FlightDescriptor, String, Runnable)`
    pub fn publish_descriptor(
        &self,
        session: &Arc<Session>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<ExportBuilder, Status> {
        self.path_resolver(descriptor, log_id)?
            .publish_descriptor(session, descriptor, log_id)
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

    /// Makes `ticket` name the value of `source`; when and for how long is up to the
    /// resolver of the ticket.
    ///
    /// Behavior: `io.deephaven.server.session.TicketRouter#publish(SessionState, Ticket,
    /// String, Runnable, ExportErrorHandler, ExportObject)`
    pub fn publish_from(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
        on_error: impl FnOnce(Status) + Send + 'static,
        source: Export,
    ) -> Result<(), Status> {
        self.log_name(ticket, log_id)?;
        self.resolver(ticket[0], log_id)?.publish_from(
            session,
            ticket,
            log_id,
            on_publish,
            Box::new(on_error),
            source,
        )
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

    fn descriptor_route(&self) -> Option<&'static str> {
        Some(EXPORT_DESCRIPTOR_ROUTE)
    }

    /// Behavior: `io.deephaven.server.session.ExportTicketResolver#flightInfoFor`
    fn flight_for(
        &self,
        session: Option<&Arc<Session>>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<DescribedFlight, Status> {
        let export_id = descriptor_to_export_id(descriptor, log_id)?;
        let session = session.ok_or_else(|| {
            Status::unauthenticated(format!(
                "Could not resolve '{log_id}': no exports can exist without a session to search"
            ))
        })?;
        Ok(DescribedFlight {
            export: session.get_export(export_id)?,
            ticket: Ok(export_id_to_bytes(export_id)),
            not_a_table: Status::not_found(format!(
                "Could not resolve '{log_id}': flight '{descriptor:?}' not found"
            )),
        })
    }

    /// Behavior: `io.deephaven.server.session.ExportTicketResolver#publish(SessionState,
    /// Flight.FlightDescriptor, String, Runnable)`
    fn publish_descriptor(
        &self,
        session: &Arc<Session>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<ExportBuilder, Status> {
        session.new_export(descriptor_to_export_id(descriptor, log_id)?)
    }
}
