//! Encode-then-decode round trips for messages of every protobuf service, and exact wire bytes
//! for a few of them.
//!
//! No Java counterpart. The expected bytes are worked out by hand from the field numbers and
//! types in the vendored `.proto` files and the protobuf encoding rules, so they fail if a
//! generated field has the wrong tag or wire type.

use std::collections::HashMap;
use std::fmt::Debug;

use oxhaven_proto::grpc::batch_table_request::operation::Op;
use oxhaven_proto::grpc::batch_table_request::Operation;
use oxhaven_proto::grpc::sort_descriptor::SortDirection;
use oxhaven_proto::grpc::{
    config_value, stream_request, table_reference, AddTableRequest, BatchTableRequest, ConfigValue,
    ConfigurationConstantsResponse, ConnectRequest, EmptyTableRequest, ExportRequest, FieldInfo,
    FieldsChangeUpdate, HandshakeRequest, PartitionByRequest, RollupRequest, SaveFileRequest,
    SelectOrUpdateRequest, SortDescriptor, SortTableRequest, StreamRequest, TableReference, Ticket,
    TypedTicket, UnstructuredFilterTableRequest,
};
use oxhaven_proto::script::execute_command_request::SystemicType;
use oxhaven_proto::script::ExecuteCommandRequest;
use prost::Message;

/// Encodes `message`, decodes the bytes, and requires the result to equal the original. Returns
/// the bytes.
fn round_trip<M: Message + Default + PartialEq + Debug>(message: &M) -> Vec<u8> {
    let bytes = message.encode_to_vec();
    let decoded = M::decode(bytes.as_slice()).expect("decode what was just encoded");
    assert_eq!(&decoded, message);
    bytes
}

fn export_ticket(id: u8) -> Ticket {
    Ticket {
        ticket: vec![b'e', id, 0, 0, 0],
    }
}

fn batch_offset(offset: i32) -> TableReference {
    TableReference {
        r#ref: Some(table_reference::Ref::BatchOffset(offset)),
    }
}

#[test]
fn ticket_has_the_expected_bytes() {
    // ticket.proto: `bytes ticket = 1;` -> tag (1 << 3) | 2 = 0x0a, length 5, payload.
    let bytes = round_trip(&export_ticket(1));
    assert_eq!(bytes, [0x0a, 0x05, b'e', 0x01, 0x00, 0x00, 0x00]);
}

#[test]
fn typed_ticket_has_the_expected_bytes() {
    let typed = TypedTicket {
        ticket: Some(export_ticket(2)),
        r#type: "Table".to_owned(),
    };
    // ticket.proto: `Ticket ticket = 1; string type = 2;`
    let mut expected = vec![0x0a, 0x07, 0x0a, 0x05, b'e', 0x02, 0x00, 0x00, 0x00];
    expected.extend_from_slice(&[0x12, 0x05]);
    expected.extend_from_slice(b"Table");
    assert_eq!(round_trip(&typed), expected);
}

#[test]
#[allow(deprecated)] // session.proto marks both fields deprecated; they are still on the wire.
fn session_handshake_request_has_the_expected_bytes() {
    let request = HandshakeRequest {
        auth_protocol: 1,
        payload: b"hi".to_vec(),
    };
    // session.proto: `sint32 auth_protocol = 1;` -> tag 0x08, zigzag(1) = 2.
    // `bytes payload = 2;` -> tag 0x12, length 2.
    assert_eq!(round_trip(&request), [0x08, 0x02, 0x12, 0x02, b'h', b'i']);

    // zigzag(-1) = 1: a plain int32 encoding would be ten bytes instead.
    let negative = HandshakeRequest {
        auth_protocol: -1,
        payload: Vec::new(),
    };
    assert_eq!(round_trip(&negative), [0x08, 0x01]);
}

#[test]
fn session_export_request_round_trips() {
    let request = ExportRequest {
        source_id: Some(Ticket {
            ticket: b"s/my_table".to_vec(),
        }),
        result_id: Some(export_ticket(3)),
    };
    round_trip(&request);
}

#[test]
fn table_batch_request_round_trips_and_keeps_its_operations() {
    let request = BatchTableRequest {
        ops: vec![
            Operation {
                op: Some(Op::EmptyTable(EmptyTableRequest {
                    result_id: None,
                    size: 100,
                })),
            },
            Operation {
                op: Some(Op::Update(SelectOrUpdateRequest {
                    result_id: None,
                    source_id: Some(batch_offset(0)),
                    column_specs: vec!["X = i".to_owned(), "Y = X * 2".to_owned()],
                })),
            },
            Operation {
                op: Some(Op::UnstructuredFilter(UnstructuredFilterTableRequest {
                    result_id: None,
                    source_id: Some(batch_offset(1)),
                    filters: vec!["X > 10".to_owned()],
                })),
            },
            Operation {
                op: Some(Op::Sort(SortTableRequest {
                    result_id: Some(export_ticket(1)),
                    source_id: Some(batch_offset(2)),
                    sorts: vec![SortDescriptor {
                        column_name: "Y".to_owned(),
                        is_absolute: false,
                        direction: SortDirection::Descending as i32,
                    }],
                })),
            },
        ],
    };
    let bytes = round_trip(&request);

    let decoded = BatchTableRequest::decode(bytes.as_slice()).expect("decode");
    let kinds: Vec<&str> = decoded
        .ops
        .iter()
        .map(|operation| match operation.op {
            Some(Op::EmptyTable(_)) => "empty_table",
            Some(Op::Update(_)) => "update",
            Some(Op::UnstructuredFilter(_)) => "unstructured_filter",
            Some(Op::Sort(_)) => "sort",
            _ => "other",
        })
        .collect();
    assert_eq!(
        kinds,
        ["empty_table", "update", "unstructured_filter", "sort"]
    );
}

#[test]
fn table_batch_operation_has_the_expected_bytes() {
    let request = BatchTableRequest {
        ops: vec![Operation {
            op: Some(Op::EmptyTable(EmptyTableRequest {
                result_id: None,
                size: 3,
            })),
        }],
    };
    // table.proto: BatchTableRequest `repeated Operation ops = 1;` -> 0x0a, length 4.
    // Operation oneof `EmptyTableRequest empty_table = 1;` -> 0x0a, length 2.
    // EmptyTableRequest `sint64 size = 2;` -> tag 0x10, zigzag(3) = 6.
    assert_eq!(round_trip(&request), [0x0a, 0x04, 0x0a, 0x02, 0x10, 0x06]);
}

#[test]
fn table_reference_batch_offset_has_the_expected_bytes() {
    // table.proto: TableReference oneof `sint32 batch_offset = 2;` -> tag 0x10, zigzag(2) = 4.
    assert_eq!(round_trip(&batch_offset(2)), [0x10, 0x04]);
}

#[test]
fn application_fields_change_update_round_trips() {
    let field = FieldInfo {
        typed_ticket: Some(TypedTicket {
            ticket: Some(Ticket {
                ticket: b"s/t".to_vec(),
            }),
            r#type: "Table".to_owned(),
        }),
        field_name: "t".to_owned(),
        field_description: "query scope variable".to_owned(),
        application_name: "scope".to_owned(),
        application_id: "scope".to_owned(),
    };
    let update = FieldsChangeUpdate {
        created: vec![field.clone()],
        updated: Vec::new(),
        removed: vec![field],
    };
    round_trip(&update);
}

#[test]
fn config_constants_response_round_trips() {
    let mut config_values = HashMap::new();
    config_values.insert(
        "deephaven.version".to_owned(),
        ConfigValue {
            kind: Some(config_value::Kind::StringValue("42.9".to_owned())),
        },
    );
    let response = ConfigurationConstantsResponse { config_values };
    let bytes = round_trip(&response);

    // config.proto: ConfigValue oneof `string string_value = 3;` -> tag (3 << 3) | 2 = 0x1a.
    let value = ConfigValue {
        kind: Some(config_value::Kind::StringValue("42.9".to_owned())),
    };
    assert_eq!(value.encode_to_vec(), [0x1a, 0x04, b'4', b'2', b'.', b'9']);
    assert!(bytes.ends_with(&value.encode_to_vec()));
}

#[test]
fn console_execute_command_request_has_the_expected_bytes() {
    let request = ExecuteCommandRequest {
        console_id: Some(export_ticket(1)),
        code: "x=1".to_owned(),
        systemic: Some(SystemicType::ExecuteSystemic as i32),
    };
    // console.proto: `Ticket console_id = 1;`, `string code = 3;` (2 is reserved),
    // `optional SystemicType systemic = 4;` with EXECUTE_SYSTEMIC = 2.
    let mut expected = vec![0x0a, 0x07, 0x0a, 0x05, b'e', 0x01, 0x00, 0x00, 0x00];
    expected.extend_from_slice(&[0x1a, 0x03, b'x', b'=', b'1']);
    expected.extend_from_slice(&[0x20, 0x02]);
    assert_eq!(round_trip(&request), expected);
}

#[test]
fn hierarchical_table_rollup_request_round_trips() {
    let request = RollupRequest {
        result_rollup_table_id: Some(export_ticket(4)),
        source_table_id: Some(export_ticket(3)),
        aggregations: Vec::new(),
        include_constituents: true,
        group_by_columns: vec!["Sym".to_owned(), "Exchange".to_owned()],
    };
    round_trip(&request);
}

#[test]
fn input_table_add_request_round_trips() {
    let request = AddTableRequest {
        input_table: Some(export_ticket(5)),
        table_to_add: Some(export_ticket(6)),
    };
    round_trip(&request);
}

#[test]
fn object_stream_request_round_trips() {
    let request = StreamRequest {
        message: Some(stream_request::Message::Connect(ConnectRequest {
            source_id: Some(TypedTicket {
                ticket: Some(export_ticket(7)),
                r#type: "Figure".to_owned(),
            }),
        })),
    };
    round_trip(&request);
}

#[test]
fn partitioned_table_partition_by_request_round_trips() {
    let request = PartitionByRequest {
        table_id: Some(export_ticket(8)),
        result_id: Some(export_ticket(9)),
        key_column_names: vec!["Sym".to_owned()],
        drop_keys: true,
    };
    round_trip(&request);
}

#[test]
fn storage_save_file_request_round_trips() {
    let request = SaveFileRequest {
        allow_overwrite: true,
        path: "/notebooks/a.py".to_owned(),
        contents: b"print(1)\n".to_vec(),
    };
    round_trip(&request);
}

#[test]
fn browser_flight_next_response_has_the_expected_bytes() {
    let response = oxhaven_proto::browser_flight::BrowserNextResponse {
        app_metadata: vec![1, 2, 3],
    };
    // BrowserFlight.proto: `bytes app_metadata = 1;`
    assert_eq!(round_trip(&response), [0x0a, 0x03, 0x01, 0x02, 0x03]);
}

#[test]
fn flight_types_are_those_of_the_arrow_flight_crate() {
    // The BrowserFlight stubs take Arrow Flight messages; this only compiles if `flight` is the
    // crate those stubs were generated against.
    let data: arrow_flight::FlightData = oxhaven_proto::flight::FlightData {
        app_metadata: vec![9].into(),
        ..Default::default()
    };
    round_trip(&data);
}
