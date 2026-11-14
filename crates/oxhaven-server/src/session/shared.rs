//! Shared tickets: `h` followed by an identifier the client chooses, at least one byte. A
//! session publishes one of its exports under such a ticket, and any session can then
//! resolve the ticket, or the Flight path descriptor `["shared", "<identifier in hex>"]`.
//!
//! A shared ticket does not keep what it names alive. It names the publisher's export, not
//! the value: it resolves for as long as that export exists, which is until the publisher
//! releases it or the publisher's session ends. Values other sessions have already taken
//! from it are theirs and stay.
//!
//! Java holds the published exports in a map with weak values, so there an entry outlives
//! its export until the next garbage collection: in between, the ticket resolves to the
//! dead export (`FAILED_PRECONDITION`, "Export in state DEPENDENCY_FAILED") and cannot be
//! published to again (`ALREADY_EXISTS`). Here the entry ends with the export, which is
//! what Java answers once it has collected.
//!
//! Java passes every resolved value through the server's authorization hook and answers
//! `NOT_FOUND` for one the hook withholds. There is no such hook here: every session may
//! resolve every shared ticket, as in the reference server's default configuration.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use oxhaven_proto::flight::flight_descriptor::DescriptorType;
use oxhaven_proto::flight::FlightDescriptor;
use tonic::Status;
use uuid::Uuid;

use super::export::{Export, ExportBuilder, WeakExport};
use super::router::{pass_through, DescribedFlight, TicketResolver};
use super::state::Session;
use super::ticket::{descriptor_log_name, descriptor_to_export_id, export_id_to_bytes};

/// The first byte of every shared ticket.
///
/// Behavior: `io.deephaven.proto.util.SharedTicketHelper#TICKET_PREFIX`
pub const SHARED_TICKET_ROUTE: u8 = b'h';

/// The first path element of a Flight descriptor that names a shared ticket.
///
/// Behavior: `io.deephaven.proto.util.SharedTicketHelper#FLIGHT_DESCRIPTOR_ROUTE`
pub const SHARED_DESCRIPTOR_ROUTE: &str = "shared";

/// The ticket for a shared identifier.
///
/// Behavior: `io.deephaven.proto.util.SharedTicketHelper#idToBytes`
pub fn shared_id_to_bytes(shared_id: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + shared_id.len());
    bytes.push(SHARED_TICKET_ROUTE);
    bytes.extend_from_slice(shared_id);
    bytes
}

/// Behavior: `io.deephaven.proto.util.ByteHelper#byteBufToHex`
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The bytes a string of hexadecimal digit pairs spells, in either case; `None` for an odd
/// length or any other character.
///
/// Behavior: `com.google.protobuf.ByteString#fromHex`
fn from_hex(text: &str) -> Option<Vec<u8>> {
    let digits = text.as_bytes();
    if digits.len() % 2 != 0 {
        return None;
    }
    digits
        .chunks(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some((high << 4 | low) as u8)
        })
        .collect()
}

/// The shared identifier in ticket bytes. `log_id` names the request field in the error.
///
/// Behavior: `io.deephaven.server.session.SharedTicketResolver#idForTicket`
fn id_for_ticket<'a>(ticket: &'a [u8], log_id: &str) -> Result<&'a [u8], Status> {
    match ticket {
        [] => Err(Status::failed_precondition(format!(
            "Could not resolve '{log_id}': no ticket supplied"
        ))),
        [SHARED_TICKET_ROUTE, id @ ..] if !id.is_empty() => Ok(id),
        _ => Err(Status::failed_precondition(format!(
            "Could not resolve '{log_id}': found 0x{} (hex)",
            hex(ticket)
        ))),
    }
}

/// The shared identifier a path descriptor `["shared", "<hex>"]` names. Hex that does not
/// parse is a `NumberFormatException` in Java, which reaches the client as
/// `INVALID_ARGUMENT` with only the id of a log entry.
///
/// Behavior: `io.deephaven.server.session.SharedTicketResolver#idForDescriptor`
fn id_for_descriptor(descriptor: &FlightDescriptor, log_id: &str) -> Result<Vec<u8>, Status> {
    if descriptor.r#type != DescriptorType::Path as i32 {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': only paths are supported"
        )));
    }
    if descriptor.path.len() != 2 {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': unexpected path length (found: {}, expected: 2)",
            descriptor_log_name(descriptor)
        )));
    }
    if descriptor.path[0] != SHARED_DESCRIPTOR_ROUTE {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': unexpected path (found: {}, expected: {SHARED_DESCRIPTOR_ROUTE})",
            descriptor_log_name(descriptor)
        )));
    }
    from_hex(&descriptor.path[1]).ok_or_else(|| {
        let error_id = Uuid::new_v4();
        tracing::error!(
            "Internal Error '{error_id}': invalid hex in shared descriptor '{}'",
            descriptor.path[1]
        );
        Status::invalid_argument(format!("Details Logged w/ID '{error_id}'"))
    })
}

/// Behavior: `io.deephaven.server.session.SharedTicketResolver#newNotFoundSRE`
fn not_found(log_id: &str, shared_id: &[u8]) -> Status {
    Status::not_found(format!(
        "Could not resolve '{log_id}': ticket '{}' not found",
        hex(shared_id)
    ))
}

/// Behavior: `io.deephaven.server.session.SharedTicketResolver#failDueToBadSource`
fn bad_source(log_id: &str, shared_id: &[u8]) -> Status {
    Status::failed_precondition(format!(
        "Could not publish '{log_id}' to shared ticket '{}' (hex): can only publish directly \
         from a session export to a shared ticket",
        hex(shared_id)
    ))
}

fn no_session(log_id: &str) -> Status {
    Status::unauthenticated(format!(
        "Could not resolve '{log_id}': no session to handoff to"
    ))
}

/// Resolves shared tickets, for every session of the server.
///
/// Behavior: `io.deephaven.server.session.SharedTicketResolver`
#[derive(Default)]
pub struct SharedTicketResolver {
    /// The published exports by shared identifier. An entry whose export is gone is as good
    /// as absent; such entries are dropped when the next ticket is published.
    shared: Mutex<HashMap<Vec<u8>, WeakExport>>,
}

impl SharedTicketResolver {
    pub fn new() -> Self {
        Self::default()
    }

    fn lookup(&self, shared_id: &[u8]) -> Option<Export> {
        let shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
        shared.get(shared_id)?.upgrade()
    }

    /// The number of identifiers held, dead or alive, for tests.
    #[cfg(test)]
    pub(super) fn entry_count(&self) -> usize {
        self.shared.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

impl TicketResolver for SharedTicketResolver {
    fn route(&self) -> u8 {
        SHARED_TICKET_ROUTE
    }

    /// Behavior: `SharedTicketResolver#getLogNameFor`
    fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status> {
        Ok(format!(
            "{SHARED_DESCRIPTOR_ROUTE}/{}",
            hex(id_for_ticket(ticket, log_id)?)
        ))
    }

    /// Work of the resolving session that waits for the published export and takes its
    /// value, so that the publisher's export is handed over rather than shared: releasing
    /// what this returns does not touch the publisher's export.
    ///
    /// Behavior: `SharedTicketResolver#resolve(SessionState, ByteBuffer, String)`
    fn resolve(
        &self,
        session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status> {
        let shared_id = id_for_ticket(ticket, log_id)?;
        let session = session.ok_or_else(|| no_session(log_id))?;
        let published = self
            .lookup(shared_id)
            .ok_or_else(|| not_found(log_id, shared_id))?;
        session
            .non_export()?
            .require([published])
            .submit(pass_through)
    }

    /// Refused: a shared ticket takes an existing export, not a new value.
    ///
    /// Behavior: `SharedTicketResolver#publish(SessionState, ByteBuffer, String, Runnable)`
    fn publish(
        &self,
        _session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        _on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        Err(bad_source(log_id, id_for_ticket(ticket, log_id)?))
    }

    /// Publishes the export `source` under the ticket, at once and whatever its state: an
    /// export that is still pending, or not even defined yet, can be published, and whoever
    /// resolves the ticket waits for it. Anything that is not an export of a session (the
    /// resolution of a scope, application or shared ticket, or of a ticket that failed to
    /// resolve) is refused. An identifier that is taken is reported to `on_error`.
    ///
    /// Behavior: `SharedTicketResolver#publish(SessionState, ByteBuffer, String, Runnable,
    /// ExportErrorHandler, ExportObject)`
    fn publish_from(
        &self,
        _session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
        on_error: Box<dyn FnOnce(Status) + Send>,
        source: Export,
    ) -> Result<(), Status> {
        let shared_id = id_for_ticket(ticket, log_id)?;
        if source.is_non_export() {
            return Err(bad_source(log_id, shared_id));
        }
        let taken = {
            let mut shared = self.shared.lock().unwrap_or_else(|e| e.into_inner());
            shared.retain(|_, export| export.upgrade().is_some());
            if shared.contains_key(shared_id) {
                true
            } else {
                shared.insert(shared_id.to_vec(), source.downgrade());
                false
            }
        };
        // the handlers run outside the lock: they answer the call
        if taken {
            on_error(Status::already_exists(format!(
                "Could not publish '{log_id}' to shared ticket '{}' (hex): destination already \
                 exists",
                hex(shared_id)
            )));
        } else if let Some(on_publish) = on_publish {
            on_publish();
        }
        Ok(())
    }

    fn descriptor_route(&self) -> Option<&'static str> {
        Some(SHARED_DESCRIPTOR_ROUTE)
    }

    /// The published export a descriptor names. The ticket of the flight is, as in Java,
    /// not the shared ticket but the export ticket whose id is the second path element read
    /// as a decimal number: the flight of `["shared", "1234"]` (the identifier 0x12 0x34)
    /// is to be read by the ticket of export 1234, and a descriptor whose hex is not also a
    /// decimal number, such as `["shared", "abcd"]`, has no flight info at all. That is a
    /// defect of the reference implementation, kept because clients can observe it.
    ///
    /// Behavior: `SharedTicketResolver#flightInfoFor`
    fn flight_for(
        &self,
        session: Option<&Arc<Session>>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<DescribedFlight, Status> {
        if session.is_none() {
            return Err(no_session(log_id));
        }
        let shared_id = id_for_descriptor(descriptor, log_id)?;
        let export = self
            .lookup(&shared_id)
            .ok_or_else(|| not_found(log_id, &shared_id))?;
        Ok(DescribedFlight {
            export,
            ticket: descriptor_to_export_id(descriptor, log_id).map(export_id_to_bytes),
            not_a_table: not_found(log_id, &shared_id),
        })
    }

    /// Refused, like publishing a new value to a shared ticket.
    ///
    /// Behavior: `SharedTicketResolver#publish(SessionState, Flight.FlightDescriptor,
    /// String, Runnable)`
    fn publish_descriptor(
        &self,
        _session: &Arc<Session>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<ExportBuilder, Status> {
        Err(bad_source(log_id, &id_for_descriptor(descriptor, log_id)?))
    }
}
