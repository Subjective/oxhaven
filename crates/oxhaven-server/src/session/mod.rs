//! Sessions: authentication, session tokens, export tickets and the `SessionService`.
//!
//! How a service uses this module:
//!
//! - [`current_session`] gives a handler the session of its call. The [`SessionLayer`] around
//!   the whole server has already found or created it from the call's headers.
//! - A handler that produces a value for a client ticket defines an export:
//!   `session.new_export_for_ticket(..)?.require(inputs).on_error(..).on_success(..).submit(work)`.
//!   The work runs on the session's [`Executor`] once the required exports are available and
//!   receives their values. [`Reply`] turns the two handlers into the response of a unary call.
//! - Inputs come from [`TicketRouter::resolve`], which accepts every kind of ticket that has
//!   a registered [`TicketResolver`].
//! - A value is an [`ExportValue`]: a table, or any other object.

mod auth;
mod executor;
mod export;
mod flight;
mod grpc;
mod interceptor;
mod router;
mod state;
mod store;
mod ticket;

#[cfg(test)]
mod tests;

pub use auth::{
    random_psk, AuthContext, AuthHandler, AuthHandlers, ANONYMOUS_AUTH_TYPE, BEARER_PREFIX,
    PSK_AUTH_TYPE,
};
pub use executor::{Executor, Job, TokioExecutor};
pub use export::{
    is_failure, is_terminal, Export, ExportBuilder, ExportError, ExportFailure, ExportState,
    ExportValue,
};
pub use flight::{handshake, HandshakeOnlyFlight, HandshakeStream};
pub use grpc::{Reply, SessionGrpc};
pub use interceptor::{current_session, optional_session, SessionLayer, SessionMiddleware};
pub use router::{ExportTicketResolver, TicketResolver, TicketRouter};
pub use state::{Clock, ExportNotifications, OnCloseId, Session, TokenExpiration};
pub use store::{
    system_clock, AuthenticationFailed, SessionStore, TerminationNotifications,
    DEFAULT_TOKEN_EXPIRE_MS, MIN_TOKEN_EXPIRE_MS,
};
pub use ticket::{
    descriptor_to_export_id, export_id_to_bytes, export_id_to_ticket, export_log_name,
    ticket_to_export_id, EXPORT_DESCRIPTOR_ROUTE, EXPORT_TICKET_ROUTE, NON_EXPORT_ID,
};
