//! Shared tickets over gRPC, between two sessions of one server. Every status code and
//! message asserted here is what the Java reference server (Deephaven 42.9) answered to the
//! same request, with its published exports collected (see the module docs of `shared`).

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::flight::flight_service_client::FlightServiceClient;
use oxhaven_proto::flight::{FlightData, FlightInfo, SchemaResult, Ticket as FlightTicket};
use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::{ExportRequest, PublishRequest, ReleaseRequest, Ticket};
use tonic::{Code, Request, Status};

use super::*;
use crate::flight::tests::support::path_descriptor;
use crate::test_support::{with_auth, TestServer};

fn ints(values: &[i32]) -> Arc<dyn Table> {
    let definition =
        TableDefinition::new(vec![ColumnDefinition::new("K", ColumnType::Int)]).unwrap();
    let schema = Schema::new(vec![Field::new("K", ColumnType::Int.arrow_type(), true)]);
    let column: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![column]).unwrap();
    in_memory_table(definition, &[batch]).unwrap()
}

fn ticket(bytes: &[u8]) -> Option<Ticket> {
    Some(Ticket {
        ticket: bytes.to_vec(),
    })
}

/// One logged-in client of the server, with a session of its own.
struct Peer<'a> {
    server: &'a TestServer,
    bearer: String,
}

impl<'a> Peer<'a> {
    async fn login(server: &'a TestServer) -> Peer<'a> {
        Peer {
            server,
            bearer: server.login().await,
        }
    }

    fn session(&self) -> Arc<Session> {
        self.server.session(&self.bearer)
    }

    fn sessions(&self) -> SessionServiceClient<tonic::transport::Channel> {
        SessionServiceClient::new(self.server.channel.clone())
    }

    fn flight(&self) -> FlightServiceClient<tonic::transport::Channel> {
        FlightServiceClient::new(self.server.channel.clone())
    }

    /// Exports `table` and returns the ticket of the export.
    fn export(&self, table: Arc<dyn Table>) -> Vec<u8> {
        let export = self
            .session()
            .new_server_side_export(ExportValue::Table(table))
            .unwrap();
        export.ticket().ticket
    }

    async fn publish(&self, source: &[u8], result: &[u8]) -> Result<(), Status> {
        let request = PublishRequest {
            source_id: ticket(source),
            result_id: ticket(result),
        };
        self.sessions()
            .publish_from_ticket(with_auth(request, &self.bearer))
            .await
            .map(|_| ())
    }

    async fn export_from(&self, source: &[u8], result: i32) -> Result<(), Status> {
        let request = ExportRequest {
            source_id: ticket(source),
            result_id: Some(export_id_to_ticket(result)),
        };
        self.sessions()
            .export_from_ticket(with_auth(request, &self.bearer))
            .await
            .map(|_| ())
    }

    async fn release(&self, id: &[u8]) {
        self.sessions()
            .release(with_auth(ReleaseRequest { id: ticket(id) }, &self.bearer))
            .await
            .unwrap();
    }

    async fn do_get(&self, ticket: &[u8]) -> Result<Vec<FlightData>, Status> {
        let ticket = FlightTicket {
            ticket: ticket.to_vec().into(),
        };
        let mut stream = self
            .flight()
            .do_get(with_auth(ticket, &self.bearer))
            .await?
            .into_inner();
        let mut messages = Vec::new();
        while let Some(message) = stream.message().await? {
            messages.push(message);
        }
        Ok(messages)
    }

    async fn info(&self, path: &[&str]) -> Result<FlightInfo, Status> {
        self.flight()
            .get_flight_info(with_auth(path_descriptor(path), &self.bearer))
            .await
            .map(|response| response.into_inner())
    }

    async fn schema(&self, path: &[&str]) -> Result<SchemaResult, Status> {
        self.flight()
            .get_schema(with_auth(path_descriptor(path), &self.bearer))
            .await
            .map(|response| response.into_inner())
    }

    /// The answer to a `DoPut` whose first message names `path`.
    async fn do_put(&self, path: &[&str]) -> Result<(), Status> {
        let first = FlightData {
            flight_descriptor: Some(path_descriptor(path)),
            ..Default::default()
        };
        let mut stream = self
            .flight()
            .do_put(with_auth(tokio_stream::iter(vec![first]), &self.bearer))
            .await?
            .into_inner();
        while stream.message().await?.is_some() {}
        Ok(())
    }
}

/// The code and message of a refusal.
fn refusal<T: std::fmt::Debug>(result: Result<T, Status>) -> (Code, String) {
    let status = result.expect_err("the call should have been refused");
    (status.code(), status.message().to_string())
}

fn not_found(log_id: &str, hex: &str) -> (Code, String) {
    (
        Code::NotFound,
        format!("Could not resolve '{log_id}': ticket '{hex}' not found"),
    )
}

#[tokio::test]
async fn two_sessions_share_a_table() {
    let server = TestServer::start().await;
    let (publisher, reader) = (Peer::login(&server).await, Peer::login(&server).await);
    assert!(!Arc::ptr_eq(&publisher.session(), &reader.session()));
    let source = publisher.export(ints(&[4, 5]));
    let rows = publisher.do_get(&source).await.unwrap();
    assert_eq!(rows.len(), 2, "a schema and one batch");

    publisher.publish(&source, b"h\x12\x34").await.unwrap();
    // one export may be published under several identifiers, of any length and content
    publisher.publish(&source, b"hX").await.unwrap();
    publisher
        .publish(&source, b"h/some/long id \xff\x00 text")
        .await
        .unwrap();

    // the other session reads the table by the ticket, and so does the publisher
    assert_eq!(reader.do_get(b"h\x12\x34").await.unwrap(), rows);
    assert_eq!(reader.do_get(b"hX").await.unwrap(), rows);
    assert_eq!(
        reader
            .do_get(b"h/some/long id \xff\x00 text")
            .await
            .unwrap(),
        rows
    );
    assert_eq!(publisher.do_get(b"h\x12\x34").await.unwrap(), rows);

    // and makes it an export of its own
    reader.export_from(b"h\x12\x34", 7).await.unwrap();
    assert_eq!(reader.do_get(&export_id_to_bytes(7)).await.unwrap(), rows);
    let copy = reader.session().get_export(7).unwrap();
    assert_eq!(
        copy.value().unwrap().as_table().unwrap().row_set().size(),
        2
    );

    // releasing that export leaves the shared ticket alone
    reader.release(&export_id_to_bytes(7)).await;
    assert_eq!(reader.do_get(b"h\x12\x34").await.unwrap(), rows);
}

#[tokio::test]
async fn refused_publishes_get_the_status_and_message_of_java() {
    let server = TestServer::start().await;
    let (publisher, other) = (Peer::login(&server).await, Peer::login(&server).await);
    let source = publisher.export(ints(&[4, 5]));
    let theirs = other.export(ints(&[6]));
    publisher.publish(&source, b"h\x12\x34").await.unwrap();

    let taken = (
        Code::AlreadyExists,
        "Could not publish 'resultId' to shared ticket '1234' (hex): destination already exists"
            .to_string(),
    );
    assert_eq!(
        refusal(publisher.publish(&source, b"h\x12\x34").await),
        taken
    );
    assert_eq!(refusal(other.publish(&theirs, b"h\x12\x34").await), taken);
    // the first publisher keeps the identifier
    assert_eq!(
        other.do_get(b"h\x12\x34").await.unwrap(),
        publisher.do_get(&source).await.unwrap()
    );

    let shape = |log_id: &str| {
        (
            Code::FailedPrecondition,
            format!("Could not resolve '{log_id}': found 0x68 (hex)"),
        )
    };
    assert_eq!(
        refusal(publisher.publish(&source, b"h").await),
        shape("resultId")
    );
    assert_eq!(
        refusal(publisher.publish(b"h", b"hzz").await),
        shape("sourceId")
    );

    // only an export of a session can be published: not what another kind of ticket
    // resolves to, and not a ticket that failed to resolve
    let not_an_export = |hex: &str| {
        (
            Code::FailedPrecondition,
            format!(
                "Could not publish 'resultId' to shared ticket '{hex}' (hex): can only publish \
                 directly from a session export to a shared ticket"
            ),
        )
    };
    assert_eq!(
        refusal(other.publish(b"h\x12\x34", b"hfromshared").await),
        not_an_export("66726f6d736861726564")
    );
    assert_eq!(
        refusal(other.publish(b"hmissing", b"hfrommissing").await),
        not_an_export("66726f6d6d697373696e67")
    );
    assert_eq!(
        refusal(publisher.publish(b"s/nope", b"hfromscope").await),
        not_an_export("66726f6d73636f7065")
    );
    // and a refused publish publishes nothing
    for (ticket, hex) in [
        (&b"hfromshared"[..], "66726f6d736861726564"),
        (b"hfrommissing", "66726f6d6d697373696e67"),
        (b"hfromscope", "66726f6d73636f7065"),
    ] {
        assert_eq!(refusal(other.do_get(ticket).await), not_found("table", hex));
    }

    let missing = PublishRequest {
        source_id: ticket(&source),
        result_id: None,
    };
    let status = publisher
        .sessions()
        .publish_from_ticket(with_auth(missing, &publisher.bearer))
        .await
        .unwrap_err();
    assert_eq!(
        (status.code(), status.message()),
        (Code::InvalidArgument, "Result ticket not supplied")
    );
    let unauthenticated = publisher
        .sessions()
        .publish_from_ticket(Request::new(PublishRequest {
            source_id: ticket(&source),
            result_id: ticket(b"hnosession"),
        }))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn unresolvable_shared_tickets_get_the_status_and_message_of_java() {
    let server = TestServer::start().await;
    let reader = Peer::login(&server).await;
    assert_eq!(
        refusal(reader.do_get(b"h\x99\x99").await),
        not_found("table", "9999")
    );
    assert_eq!(
        refusal(reader.do_get(b"h").await),
        (
            Code::FailedPrecondition,
            "Could not resolve 'table': found 0x68 (hex)".to_string()
        )
    );
    assert_eq!(
        refusal(reader.export_from(b"h\x99\x99", 8).await),
        not_found("sourceId", "9999")
    );
    assert_eq!(
        refusal(reader.export_from(b"h", 9).await),
        (
            Code::FailedPrecondition,
            "Could not resolve 'sourceId': found 0x68 (hex)".to_string()
        )
    );
    // the export that named an unknown shared ticket has failed; the malformed one was
    // never made
    assert_eq!(
        reader.session().get_export(8).unwrap().state(),
        ExportState::DependencyFailed
    );
    assert!(reader.session().get_export_if_exists(9).unwrap().is_none());
}

#[tokio::test]
async fn shared_ticket_ends_when_the_published_export_is_released() {
    let server = TestServer::start().await;
    let (publisher, reader) = (Peer::login(&server).await, Peer::login(&server).await);
    let source = publisher.export(ints(&[8]));
    let other = publisher.export(ints(&[4, 5]));
    publisher.publish(&source, b"hlife").await.unwrap();
    publisher.publish(&source, b"hlife2").await.unwrap();
    let rows = reader.do_get(b"hlife").await.unwrap();
    reader.export_from(b"hlife", 20).await.unwrap();

    publisher.release(&source).await;
    assert_eq!(
        refusal(reader.do_get(b"hlife").await),
        not_found("table", "6c696665")
    );
    assert_eq!(
        refusal(reader.do_get(b"hlife2").await),
        not_found("table", "6c69666532")
    );
    assert_eq!(
        refusal(reader.info(&["shared", "6c696665"]).await),
        not_found("request", "6c696665")
    );
    // what the other session took before is its own
    assert_eq!(reader.do_get(&export_id_to_bytes(20)).await.unwrap(), rows);

    // the identifier is free again
    publisher.publish(&other, b"hlife").await.unwrap();
    assert_eq!(
        reader.do_get(b"hlife").await.unwrap(),
        publisher.do_get(&other).await.unwrap()
    );

    // an export that was released before it is published names nothing either
    publisher.publish(&source, b"hgone").await.unwrap();
    assert_eq!(
        refusal(reader.do_get(b"hgone").await),
        not_found("table", "676f6e65")
    );
}

#[tokio::test]
async fn shared_ticket_ends_with_the_publishing_session() {
    let server = TestServer::start().await;
    let (publisher, reader) = (Peer::login(&server).await, Peer::login(&server).await);
    let source = publisher.export(ints(&[6, 7]));
    publisher.publish(&source, b"hclose").await.unwrap();
    let rows = reader.do_get(b"hclose").await.unwrap();
    reader.export_from(b"hclose", 30).await.unwrap();

    server.state.sessions.close_session(&publisher.session());
    assert_eq!(
        refusal(reader.do_get(b"hclose").await),
        not_found("table", "636c6f7365")
    );
    assert_eq!(
        refusal(reader.export_from(b"hclose", 31).await),
        not_found("sourceId", "636c6f7365")
    );
    // the table the other session took outlives the session it came from
    assert_eq!(reader.do_get(&export_id_to_bytes(30)).await.unwrap(), rows);

    // and another session can take the identifier over
    let mine = reader.export(ints(&[1]));
    reader.publish(&mine, b"hclose").await.unwrap();
    assert_eq!(
        reader.do_get(b"hclose").await.unwrap(),
        reader.do_get(&mine).await.unwrap()
    );
}

#[tokio::test]
async fn an_export_can_be_published_before_it_is_defined() {
    let server = TestServer::start().await;
    let (publisher, reader) = (Peer::login(&server).await, Peer::login(&server).await);
    publisher
        .publish(&export_id_to_bytes(50), b"hpending")
        .await
        .unwrap();
    let waiting = tokio::spawn({
        let (flight, bearer) = (reader.flight(), reader.bearer.clone());
        async move {
            let ticket = FlightTicket {
                ticket: b"hpending".to_vec().into(),
            };
            let mut stream = flight
                .clone()
                .do_get(with_auth(ticket, &bearer))
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok::<_, Status>(messages)
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiting.is_finished(), "the reader waits for the export");

    let table = ints(&[4, 5]);
    publisher
        .session()
        .new_export(50)
        .unwrap()
        .submit(move |_| Ok(ExportValue::Table(table)))
        .unwrap();
    let rows = waiting.await.unwrap().unwrap();
    assert_eq!(
        rows,
        publisher.do_get(&export_id_to_bytes(50)).await.unwrap()
    );
}

#[tokio::test]
async fn a_failed_export_fails_those_who_resolve_its_shared_ticket_the_same_way() {
    let server = TestServer::start().await;
    let (publisher, reader) = (Peer::login(&server).await, Peer::login(&server).await);
    let failure = Status::not_found("Could not resolve 'sourceId: variable 'nope' not found");
    publisher
        .session()
        .new_export(60)
        .unwrap()
        .submit({
            let failure = failure.clone();
            move |_| Err(failure.into())
        })
        .unwrap();
    // wait for the work to have failed
    refusal(publisher.do_get(&export_id_to_bytes(60)).await);
    publisher
        .publish(&export_id_to_bytes(60), b"hfailed")
        .await
        .unwrap();

    let same = (failure.code(), failure.message().to_string());
    assert_eq!(refusal(reader.do_get(b"hfailed").await), same);
    assert_eq!(refusal(reader.export_from(b"hfailed", 61).await), same);
    assert_eq!(
        refusal(reader.info(&["shared", "6661696c6564"]).await),
        same
    );
}

#[tokio::test]
async fn flight_descriptors_of_shared_tickets_answer_what_java_answers() {
    let server = TestServer::start().await;
    let (publisher, reader) = (Peer::login(&server).await, Peer::login(&server).await);
    let source = publisher.export(ints(&[4, 5]));
    publisher.publish(&source, b"h\x12\x34").await.unwrap();
    publisher.publish(&source, b"h\xab\xcd").await.unwrap();
    publisher.publish(&source, b"h1").await.unwrap();

    // The flight is answered with an export ticket: the hex text read as a decimal number.
    let info = reader.info(&["shared", "1234"]).await.unwrap();
    assert_eq!(
        info.flight_descriptor,
        Some(path_descriptor(&["shared", "1234"]))
    );
    assert_eq!(info.endpoint.len(), 1);
    assert_eq!(
        info.endpoint[0].ticket.as_ref().unwrap().ticket.as_ref(),
        export_id_to_bytes(1234)
    );
    assert_eq!((info.total_records, info.total_bytes), (2, -1));
    let own = publisher
        .flight()
        .get_flight_info(with_auth(
            path_descriptor(&["export", &source_id(&source).to_string()]),
            &publisher.bearer,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.schema, own.schema);
    let info = reader.info(&["shared", "31"]).await.unwrap();
    assert_eq!(
        info.endpoint[0].ticket.as_ref().unwrap().ticket.as_ref(),
        export_id_to_bytes(31)
    );
    assert_eq!(
        reader.schema(&["shared", "1234"]).await.unwrap().schema,
        own.schema
    );

    // so hex that is not a decimal number has no flight, in either case of the digits
    for hex in ["abcd", "ABCD"] {
        let not_numeric = (
            Code::FailedPrecondition,
            format!(
                "Could not resolve descriptor 'request': export id not numeric (found: \
                 /shared/{hex})"
            ),
        );
        assert_eq!(refusal(reader.info(&["shared", hex]).await), not_numeric);
        assert_eq!(refusal(reader.schema(&["shared", hex]).await), not_numeric);
    }

    assert_eq!(
        refusal(reader.info(&["shared", "9999"]).await),
        not_found("request", "9999")
    );
    assert_eq!(
        refusal(reader.info(&["shared", "ffff"]).await),
        not_found("request", "ffff")
    );
    assert_eq!(
        refusal(reader.info(&["shared", ""]).await),
        not_found("request", "")
    );
    assert_eq!(
        refusal(reader.schema(&["shared", "9999"]).await),
        not_found("request", "9999")
    );
    let length = |found: &str, log_id: &str| {
        (
            Code::FailedPrecondition,
            format!(
                "Could not resolve descriptor '{log_id}': unexpected path length (found: \
                 {found}, expected: 2)"
            ),
        )
    };
    assert_eq!(
        refusal(reader.info(&["shared"]).await),
        length("/shared", "request")
    );
    assert_eq!(
        refusal(reader.info(&["shared", "12", "34"]).await),
        length("/shared/12/34", "request")
    );
    assert_eq!(
        refusal(reader.info(&["Shared", "1234"]).await),
        (
            Code::FailedPrecondition,
            "Could not resolve 'request': no resolver for path route 'Shared'".to_string()
        )
    );
    // hex that does not parse is an internal error in Java
    for bad in ["zz", "123", "-1"] {
        let (code, message) = refusal(reader.info(&["shared", bad]).await);
        assert_eq!(code, Code::InvalidArgument, "{bad}");
        assert!(message.starts_with("Details Logged w/ID '"), "{message}");
    }

    // without a session the answer comes before the descriptor is looked at
    for path in [&["shared", "1234"][..], &["shared"]] {
        let status = reader
            .flight()
            .get_flight_info(Request::new(path_descriptor(path)))
            .await
            .unwrap_err();
        assert_eq!(
            (status.code(), status.message()),
            (
                Code::Unauthenticated,
                "Could not resolve 'request': no session to handoff to"
            )
        );
    }

    // nothing can be uploaded to a shared ticket
    for hex in ["1234", "5678"] {
        assert_eq!(
            refusal(reader.do_put(&["shared", hex]).await),
            (
                Code::FailedPrecondition,
                format!(
                    "Could not publish 'Flight.Descriptor' to shared ticket '{hex}' (hex): can \
                     only publish directly from a session export to a shared ticket"
                )
            )
        );
    }
    assert_eq!(
        refusal(reader.do_put(&["shared"]).await),
        length("/shared", "Flight.Descriptor")
    );
    let (code, message) = refusal(reader.do_put(&["shared", "zz"]).await);
    assert_eq!(code, Code::InvalidArgument);
    assert!(message.starts_with("Details Logged w/ID '"), "{message}");
}

fn source_id(ticket: &[u8]) -> i32 {
    ticket_to_export_id(ticket, "test").unwrap()
}
