//! Application field tickets: `a/<application id>/f/<field name>`.

use std::sync::Arc;

use tonic::Status;

use super::fields::{Application, Applications};
use crate::session::{Export, ExportBuilder, ExportValue, Session, TicketResolver};

/// The first byte of every application field ticket.
///
/// Behavior: `io.deephaven.proto.util.ApplicationTicketHelper#TICKET_PREFIX`
pub const APPLICATION_TICKET_ROUTE: u8 = b'a';

/// The ticket of the field `field_name` of the application `application_id`.
///
/// Behavior: `io.deephaven.proto.util.ApplicationTicketHelper#applicationFieldToBytes`
pub fn application_field_ticket(application_id: &str, field_name: &str) -> Vec<u8> {
    format!("a/{application_id}/f/{field_name}").into_bytes()
}

/// Resolves application field tickets against the applications of the server.
///
/// Behavior: `io.deephaven.server.appmode.ApplicationTicketResolver`
pub struct ApplicationTicketResolver {
    applications: Arc<Applications>,
}

impl ApplicationTicketResolver {
    pub fn new(applications: Arc<Applications>) -> Self {
        Self { applications }
    }

    /// The application and field a ticket names. The ticket is read as text with three
    /// slashes: the application id is between the first and the second, the field name is
    /// everything after the third, which may hold further slashes. What stands before the
    /// first slash and between the second and the third is not looked at, as in Java. Bytes
    /// that are not UTF-8 read as U+FFFD.
    ///
    /// Behavior: `ApplicationTicketResolver#appFieldIdFor(ByteBuffer, String)`
    fn field_of(&self, ticket: &[u8], log_id: &str) -> Result<(Application, String), Status> {
        let text = String::from_utf8_lossy(ticket);
        let mut parts = text.splitn(4, '/');
        let (Some(_route), Some(application_id), Some(_segment), Some(field_name)) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(Status::failed_precondition(format!(
                "Could not resolve '{log_id}': ticket does conform to expected format"
            )));
        };
        let application = self.applications.get(application_id).ok_or_else(|| {
            Status::not_found(format!(
                "Could not resolve '{log_id}': no application exists with the identifier: \
                 {application_id}"
            ))
        })?;
        Ok((application, field_name.to_string()))
    }
}

/// Behavior: `ApplicationTicketResolver#getLogNameFor(AppFieldId)`
fn field_log_name(application: &Application, field_name: &str) -> String {
    format!("app/{}/field/{field_name}", application.id())
}

impl TicketResolver for ApplicationTicketResolver {
    fn route(&self) -> u8 {
        APPLICATION_TICKET_ROUTE
    }

    /// Behavior: `ApplicationTicketResolver#getLogNameFor(ByteBuffer, String)`
    fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status> {
        let (application, field_name) = self.field_of(ticket, log_id)?;
        Ok(field_log_name(&application, &field_name))
    }

    /// The value of the field now. A session is not needed.
    ///
    /// Behavior: `ApplicationTicketResolver#resolve(SessionState, ByteBuffer, String)`
    fn resolve(
        &self,
        _session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status> {
        let (application, field_name) = self.field_of(ticket, log_id)?;
        let value: ExportValue = application.field(&field_name).ok_or_else(|| {
            Status::not_found(format!(
                "Could not resolve '{log_id}': field '{}' not found",
                field_log_name(&application, &field_name)
            ))
        })?;
        Ok(Export::ready(value))
    }

    /// Behavior: `ApplicationTicketResolver#publish(SessionState, ByteBuffer, String, Runnable)`
    fn publish(
        &self,
        _session: &Arc<Session>,
        _ticket: &[u8],
        log_id: &str,
        _on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        Err(Status::failed_precondition(format!(
            "Could not publish '{log_id}': application tickets cannot be published to"
        )))
    }
}
