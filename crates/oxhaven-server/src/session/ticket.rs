//! Export ticket byte layout: the route byte `e` followed by the export id as a little-endian
//! `i32`.

use oxhaven_proto::flight::{flight_descriptor::DescriptorType, FlightDescriptor};
use oxhaven_proto::grpc::Ticket;
use tonic::Status;

/// The first byte of every export ticket.
///
/// Behavior: `io.deephaven.proto.util.ExportTicketHelper#TICKET_PREFIX`
pub const EXPORT_TICKET_ROUTE: u8 = b'e';

/// The first path element of a Flight descriptor that names an export.
///
/// Behavior: `io.deephaven.proto.util.ExportTicketHelper#FLIGHT_DESCRIPTOR_ROUTE`
pub const EXPORT_DESCRIPTOR_ROUTE: &str = "export";

/// The id of work that depends on exports but exports nothing itself.
///
/// Behavior: `io.deephaven.server.session.SessionState#NON_EXPORT_ID`
pub const NON_EXPORT_ID: i32 = 0;

/// Behavior: `io.deephaven.proto.util.ExportTicketHelper#exportIdToBytes`
pub fn export_id_to_bytes(export_id: i32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5);
    bytes.push(EXPORT_TICKET_ROUTE);
    bytes.extend_from_slice(&export_id.to_le_bytes());
    bytes
}

/// Behavior: `io.deephaven.proto.util.ExportTicketHelper#wrapExportIdInTicket`
pub fn export_id_to_ticket(export_id: i32) -> Ticket {
    Ticket {
        ticket: export_id_to_bytes(export_id),
    }
}

/// Reads the export id out of ticket bytes. `log_id` names the request field in the error.
///
/// Behavior: `io.deephaven.proto.util.ExportTicketHelper#ticketToExportIdInternal`
pub fn ticket_to_export_id(ticket: &[u8], log_id: &str) -> Result<i32, Status> {
    if ticket.is_empty() {
        return Err(Status::failed_precondition(format!(
            "Could not resolve ticket '{log_id}': ticket was not provided"
        )));
    }
    match ticket {
        [EXPORT_TICKET_ROUTE, id @ ..] if id.len() == 4 => {
            Ok(i32::from_le_bytes([id[0], id[1], id[2], id[3]]))
        }
        _ => Err(Status::failed_precondition(format!(
            "Could not resolve ticket '{log_id}': found 0x{} (hex)",
            hex(ticket)
        ))),
    }
}

/// Reads the export id out of a Flight path descriptor `["export", "<id>"]`. The first path
/// element is not checked here: the caller has already used it to choose the export route.
///
/// Behavior: `io.deephaven.proto.flight.util.FlightExportTicketHelper#descriptorToExportId`
pub fn descriptor_to_export_id(descriptor: &FlightDescriptor, log_id: &str) -> Result<i32, Status> {
    if descriptor.r#type != DescriptorType::Path as i32 {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': not a path"
        )));
    }
    if descriptor.path.len() != 2 {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': unexpected path length (found: {}, expected: 2)",
            descriptor.path.len()
        )));
    }
    descriptor.path[1].parse::<i32>().map_err(|_| {
        Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': export id not numeric (found: {})",
            descriptor.path[1]
        ))
    })
}

/// Behavior: `io.deephaven.proto.util.ExportTicketHelper#toReadableString`
pub fn export_log_name(export_id: i32) -> String {
    format!("{EXPORT_DESCRIPTOR_ROUTE}/{export_id}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn export_ticket_is_route_byte_then_little_endian_id() {
        assert_eq!(export_id_to_bytes(1), [b'e', 1, 0, 0, 0]);
        assert_eq!(export_id_to_bytes(0x0403_0201), [b'e', 1, 2, 3, 4]);
        assert_eq!(export_id_to_bytes(-1), [b'e', 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(export_id_to_bytes(i32::MIN), [b'e', 0, 0, 0, 0x80]);
    }

    #[test]
    fn ticket_round_trips_every_sign() {
        for id in [0, 1, -1, 77, i32::MAX, i32::MIN, 0x1234_5678] {
            let ticket = export_id_to_ticket(id);
            assert_eq!(ticket_to_export_id(&ticket.ticket, "id").unwrap(), id);
        }
    }

    #[test]
    fn malformed_tickets_are_failed_precondition() {
        for bad in [
            &b""[..],
            &b"e"[..],
            &[b'e', 1, 0, 0][..],
            &[b'e', 1, 0, 0, 0, 0][..],
            &[b's', 1, 0, 0, 0][..],
        ] {
            let err = ticket_to_export_id(bad, "id").unwrap_err();
            assert_eq!(err.code(), Code::FailedPrecondition, "{bad:?}");
        }
    }

    #[test]
    fn descriptor_names_the_export_in_its_second_path_element() {
        let descriptor = |path: &[&str], r#type: DescriptorType| FlightDescriptor {
            r#type: r#type as i32,
            cmd: Default::default(),
            path: path.iter().map(|s| s.to_string()).collect(),
        };
        let ok = descriptor(&["export", "-12"], DescriptorType::Path);
        assert_eq!(descriptor_to_export_id(&ok, "d").unwrap(), -12);
        for bad in [
            descriptor(&["export", "12"], DescriptorType::Cmd),
            descriptor(&["export"], DescriptorType::Path),
            descriptor(&["export", "1", "2"], DescriptorType::Path),
            descriptor(&["export", "x"], DescriptorType::Path),
            descriptor(&["export", "99999999999"], DescriptorType::Path),
        ] {
            let err = descriptor_to_export_id(&bad, "d").unwrap_err();
            assert_eq!(err.code(), Code::FailedPrecondition, "{bad:?}");
        }
    }

    #[test]
    fn log_name_is_route_and_id() {
        assert_eq!(export_log_name(-3), "export/-3");
    }
}
