//! The object service over the wire. The statuses, texts and messages are what the Java
//! reference (42.9) answered to the same calls; `docs/PARTITIONED.md` lists them.

// FetchObject is deprecated in the protocol and still what clients without streams call
#![allow(deprecated)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::{ClientData, ConnectRequest, PartitionedTableDescriptor, Ticket};
use prost::Message;
use tonic::Code;

use super::*;
use crate::flight::tests::support::{schema_of_ipc, Server};
use crate::partitioned::{export_value, partition};
use crate::session::export_id_to_ticket;

fn rows() -> Arc<dyn Table> {
    let schema = Schema::new(vec![
        Field::new("K", DataType::Int32, true),
        Field::new("V", DataType::Int32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1, 2, 1])),
        Arc::new(Int32Array::from(vec![10, 20, 30])),
    ];
    let definition = TableDefinition::new(vec![
        ColumnDefinition::new("K", ColumnType::Int),
        ColumnDefinition::new("V", ColumnType::Int),
    ])
    .unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema), columns).unwrap();
    in_memory_table(definition, &[batch]).unwrap()
}

/// A server whose session has a table as export 1 and its partition by `K` as export 2.
async fn server() -> Server {
    let server = Server::start().await;
    let table = rows();
    let partitioned = partition(&table, &["K".to_string()], true).unwrap();
    server.export(1, table);
    server
        .session()
        .new_export(2)
        .unwrap()
        .submit(move |_| Ok(export_value(partitioned)))
        .unwrap();
    server
}

fn typed(id: Option<i32>, type_name: &str) -> Option<TypedTicket> {
    Some(TypedTicket {
        ticket: id.map(export_id_to_ticket),
        r#type: type_name.to_string(),
    })
}

fn fetch(id: Option<i32>, type_name: &str) -> FetchObjectRequest {
    FetchObjectRequest {
        source_id: typed(id, type_name),
    }
}

fn connect(id: i32, type_name: &str) -> StreamRequest {
    StreamRequest {
        message: Some(ClientMessage::Connect(ConnectRequest {
            source_id: typed(Some(id), type_name),
        })),
    }
}

fn data() -> StreamRequest {
    StreamRequest {
        message: Some(ClientMessage::Data(ClientData::default())),
    }
}

/// Checks the message a client that opens the partition of [`server`] is sent, and
/// returns the ticket of the table it refers to.
fn check_opened(server: &Server, payload: &[u8], references: &[TypedTicket]) -> Ticket {
    let descriptor = PartitionedTableDescriptor::decode(payload).unwrap();
    assert_eq!(descriptor.key_column_names, ["K"]);
    assert_eq!(descriptor.constituent_column_name, "__CONSTITUENT__");
    assert!(descriptor.unique_keys);
    assert!(!descriptor.constituent_changes_permitted);
    // the keys were dropped
    let schema = schema_of_ipc(&descriptor.constituent_definition_schema);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["V"]);

    let [reference] = references else {
        panic!("one reference, not {references:?}");
    };
    assert_eq!(reference.r#type, "Table");
    let ticket = reference.ticket.clone().unwrap();
    // an export the server chose: a negative id, whose value is the underlying table
    let id = crate::session::ticket_to_export_id(&ticket.ticket, "reference").unwrap();
    assert!(id < 0, "{id}");
    let value = server.session().get_export(id).unwrap().value().unwrap();
    let underlying = value.as_table().unwrap();
    let columns = underlying.definition().columns();
    assert_eq!(columns[0].name, "K");
    assert_eq!(columns[1].name, "__CONSTITUENT__");
    assert_eq!(underlying.row_set().size(), 2);
    ticket
}

#[tokio::test]
async fn fetch_object_answers_what_java_answers() {
    let server = &server().await;
    let service = || ObjectServiceClient::new(server.channel.clone());
    let refused = |request: FetchObjectRequest| async move {
        let status = service()
            .fetch_object(server.authorized(request))
            .await
            .unwrap_err();
        (status.code(), status.message().to_string())
    };

    let fetched = service()
        .fetch_object(server.authorized(fetch(Some(2), "PartitionedTable")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.r#type, "PartitionedTable");
    let first = check_opened(server, &fetched.data, &fetched.typed_export_ids);
    // every fetch exports the table again
    let again = service()
        .fetch_object(server.authorized(fetch(Some(2), "PartitionedTable")))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(check_opened(server, &again.data, &again.typed_export_ids), first);

    assert_eq!(
        refused(fetch(Some(2), "")).await,
        (Code::InvalidArgument, "No type supplied".to_string())
    );
    assert_eq!(
        refused(fetch(None, "PartitionedTable")).await,
        (Code::InvalidArgument, "No ticket supplied".to_string())
    );
    assert_eq!(
        refused(fetch(Some(2), "Table")).await,
        (
            Code::FailedPrecondition,
            "Unexpected ObjectType, expected type 'Table', actual type 'PartitionedTable'"
                .to_string()
        )
    );
    assert_eq!(
        refused(fetch(Some(1), "PartitionedTable")).await,
        (
            Code::NotFound,
            "No ObjectType found, expected type 'PartitionedTable'".to_string()
        )
    );
    let mut missing = fetch(None, "PartitionedTable");
    missing.source_id.as_mut().unwrap().ticket = Some(Ticket {
        ticket: b"s/nope".to_vec(),
    });
    assert_eq!(
        refused(missing).await,
        (
            Code::NotFound,
            "Could not resolve 'sourceId: variable 'nope' not found".to_string()
        )
    );
}

#[tokio::test]
async fn a_message_stream_sends_the_object_once_and_ends_when_the_client_does() {
    let server = &server().await;
    let service = || ObjectServiceClient::new(server.channel.clone());

    let (requests, sent) = tokio::sync::mpsc::unbounded_channel();
    let mut responses = service()
        .message_stream(server.authorized(UnboundedReceiverStream::new(sent)))
        .await
        .unwrap()
        .into_inner();
    requests.send(connect(2, "PartitionedTable")).unwrap();
    let opened = responses.message().await.unwrap().unwrap();
    let Some(ServerMessage::Data(opened)) = opened.message else {
        panic!("a data message");
    };
    check_opened(server, &opened.payload, &opened.exported_references);
    // what the client sends afterwards is ignored, and the stream stays open
    requests.send(data()).unwrap();
    let waiting = tokio::time::timeout(std::time::Duration::from_millis(200), responses.message());
    assert!(waiting.await.is_err(), "nothing more is sent");
    drop(requests);
    assert!(responses.message().await.unwrap().is_none());

    // what is refused ends the stream with its status
    for (messages, code, text) in [
        (
            vec![data()],
            Code::InvalidArgument,
            "Data message sent before Connect message",
        ),
        (
            vec![connect(2, "PartitionedTable"), connect(2, "PartitionedTable")],
            Code::FailedPrecondition,
            "Already sent a connect request, cannot send another",
        ),
        (
            vec![connect(2, "")],
            Code::InvalidArgument,
            "No type supplied",
        ),
        (
            vec![connect(1, "PartitionedTable")],
            Code::NotFound,
            "No ObjectType found, expected type 'PartitionedTable'",
        ),
        (
            vec![StreamRequest { message: None }],
            Code::InvalidArgument,
            "io.deephaven.proto.backplane.grpc.StreamRequest must have oneof message.",
        ),
    ] {
        let (requests, sent) = tokio::sync::mpsc::unbounded_channel();
        let mut responses = service()
            .message_stream(server.authorized(UnboundedReceiverStream::new(sent)))
            .await
            .unwrap()
            .into_inner();
        for message in messages {
            requests.send(message).unwrap();
        }
        let status = loop {
            match responses.message().await {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("the stream ended without {text:?}"),
                Err(status) => break status,
            }
        };
        assert_eq!(status.code(), code, "{status:?}");
        assert!(status.message().starts_with(text), "{status:?}");
    }
}
