//! The object service over the wire. The statuses, texts and messages are what the Java
//! reference (42.9) answered to the same calls; `docs/PARTITIONED.md` lists them.

// FetchObject is deprecated in the protocol and still what clients without streams call
#![allow(deprecated)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::{ClientData, ConnectRequest, PartitionedTableDescriptor, Ticket};
use prost::Message;
use tonic::{Code, Request, Status, Streaming};

use oxhaven_proto::grpc::stream_response::Message as ServerMessage;

use super::*;
use crate::flight::tests::support::{schema_of_ipc, Server};
use crate::partitioned::{export_value, partition};
use crate::session::{export_id_to_ticket, ExportError, ExportValue};

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
    assert_ne!(
        check_opened(server, &again.data, &again.typed_export_ids),
        first
    );

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
async fn a_message_stream_sends_an_object_that_is_only_fetched_and_ends() {
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
    // The type sends its one message and closes, which ends the stream though the client
    // has not closed its side: Java 42.9 answers a `PartitionedTable` and a fetch-only
    // type of a plugin this way.
    let ended = tokio::time::timeout(std::time::Duration::from_secs(5), responses.message());
    assert!(ended.await.expect("the stream ends").unwrap().is_none());
    drop(requests);

    // What is refused ends the stream with its status. A second connect request is among
    // it, which takes a stream that is still open: see
    // `the_object_is_told_of_the_end_of_a_stream_it_did_not_end`.
    for (messages, code, text) in [
        (
            vec![data()],
            Code::InvalidArgument,
            "Data message sent before Connect message",
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

// ---- a type that talks, for what `PartitionedTable` cannot show ----

/// An object of the test's own type, [`TalkerType`], and what was done to it.
struct Talker {
    /// What it does when a client connects: see [`TalkerType::connect`].
    mode: &'static str,
    log: Mutex<Vec<String>>,
}

impl Talker {
    fn note(&self, what: String) {
        self.log.lock().unwrap().push(what);
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Waits until the object was told that its stream is over.
    async fn closed(&self) {
        for _ in 0..200 {
            if self.log().iter().any(|line| line == "close") {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("never told of the end: {:?}", self.log());
    }

    /// Checks that the object was not told that its stream is over, some time after it
    /// would have been.
    async fn not_closed(&self) {
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !self.log().iter().any(|line| line == "close"),
            "{:?}",
            self.log()
        );
    }
}

const TALKER: &str = "oxhaven.test.Talker";

struct TalkerType;

struct TalkerStream {
    talker: Arc<Talker>,
    connection: Arc<dyn types::MessageStream>,
}

impl ObjectType for TalkerType {
    fn name(&self) -> &str {
        TALKER
    }

    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError> {
        Ok(value.downcast_ref::<Arc<Talker>>().is_some())
    }

    fn connect(
        &self,
        value: &ExportValue,
        connection: Arc<dyn types::MessageStream>,
    ) -> Result<Arc<dyn types::MessageStream>, ExportError> {
        let talker = value.downcast_ref::<Arc<Talker>>().unwrap().clone();
        talker.note("connect".to_string());
        match talker.mode {
            "silent" => {}
            "shut" => connection.on_close(),
            "refuse" => return Err(ExportError::Internal("refused".to_string())),
            "once" | "twice" => {
                if talker.mode == "twice" {
                    connection.on_data(b"first".to_vec(), vec![ExportValue::Table(rows())])?;
                }
                let references = vec![
                    ExportValue::Table(rows()),
                    ExportValue::object(talker.clone()),
                    ExportValue::object(5_i32),
                ];
                connection.on_data(b"once".to_vec(), references)?;
                connection.on_close();
            }
            _ => connection.on_data(b"hello".to_vec(), Vec::new())?,
        }
        Ok(Arc::new(TalkerStream { talker, connection }))
    }
}

impl types::MessageStream for TalkerStream {
    /// Sends the message back, with its references; `close` closes the stream instead,
    /// `fail` fails, and `late` tries to send after it closed the stream.
    fn on_data(&self, payload: Vec<u8>, references: Vec<ExportValue>) -> Result<(), ExportError> {
        let text = String::from_utf8_lossy(&payload).to_string();
        self.talker
            .note(format!("data {text} {}", references.len()));
        match text.as_str() {
            "close" => self.connection.on_close(),
            "fail" => return Err(ExportError::Internal("failed".to_string())),
            "late" => {
                self.connection.on_close();
                let refused = self.connection.on_data(payload, references).unwrap_err();
                let ExportError::Internal(refused) = refused else {
                    panic!("a status");
                };
                self.talker.note(refused);
            }
            _ => self.connection.on_data(payload, references)?,
        }
        Ok(())
    }

    fn on_close(&self) {
        self.talker.note("close".to_string());
    }
}

/// Makes a talker export `id` of the session; its type is registered with the first.
fn talker(server: &Server, id: i32, mode: &'static str) -> Arc<Talker> {
    static REGISTERED: std::sync::Once = std::sync::Once::new();
    REGISTERED.call_once(|| types::registry().register(Arc::new(TalkerType)).unwrap());
    let talker = Arc::new(Talker {
        mode,
        log: Mutex::new(Vec::new()),
    });
    let exported = talker.clone();
    server
        .session()
        .new_export(id)
        .unwrap()
        .submit(move |_| Ok(ExportValue::object(exported)))
        .unwrap();
    talker
}

fn say(payload: &str, references: &[i32]) -> StreamRequest {
    StreamRequest {
        message: Some(ClientMessage::Data(ClientData {
            payload: payload.as_bytes().to_vec(),
            references: references
                .iter()
                .map(|id| typed(Some(*id), "").unwrap())
                .collect(),
        })),
    }
}

type Sender = tokio::sync::mpsc::UnboundedSender<StreamRequest>;

/// Opens a message stream and sends `first` on it.
async fn stream(server: &Server, first: StreamRequest) -> (Sender, Streaming<StreamResponse>) {
    let (requests, sent) = tokio::sync::mpsc::unbounded_channel();
    requests.send(first).unwrap();
    let responses = ObjectServiceClient::new(server.channel.clone())
        .message_stream(server.authorized(UnboundedReceiverStream::new(sent)))
        .await
        .unwrap()
        .into_inner();
    (requests, responses)
}

/// The next message of the stream: its bytes as text, and the type of each reference,
/// which must be an export the server chose.
async fn heard(responses: &mut Streaming<StreamResponse>) -> (String, Vec<String>) {
    let next = tokio::time::timeout(Duration::from_secs(5), responses.message());
    let message = next
        .await
        .expect("a message")
        .unwrap()
        .expect("a message, not the end");
    let Some(ServerMessage::Data(data)) = message.message else {
        panic!("a data message");
    };
    let types = data
        .exported_references
        .iter()
        .map(|reference| {
            let ticket = &reference.ticket.as_ref().unwrap().ticket;
            let id = crate::session::ticket_to_export_id(ticket, "reference").unwrap();
            assert!(id < 0, "{id}");
            reference.r#type.clone()
        })
        .collect();
    (String::from_utf8(data.payload).unwrap(), types)
}

/// How the stream ends: `Ok` for its end, or the status.
async fn end(responses: &mut Streaming<StreamResponse>) -> Result<(), (Code, String)> {
    let next = tokio::time::timeout(Duration::from_secs(5), responses.message());
    match next.await.expect("the stream ends") {
        Ok(None) => Ok(()),
        Ok(Some(message)) => panic!("the end, not {message:?}"),
        Err(status) => Err((status.code(), status.message().to_string())),
    }
}

fn details_logged(end: Result<(), (Code, String)>) {
    let (code, text) = end.unwrap_err();
    assert_eq!(code, Code::InvalidArgument);
    assert!(text.starts_with("Details Logged w/ID '"), "{text}");
}

struct Named(&'static str, bool);

impl ObjectType for Named {
    fn name(&self) -> &str {
        self.0
    }

    /// Every number is of the type if the type takes any; a string fails the question.
    fn is_type(&self, value: &ExportValue) -> Result<bool, ExportError> {
        if value.downcast_ref::<String>().is_some() {
            return Err(ExportError::Internal("no answer".to_string()));
        }
        Ok(self.1 && value.downcast_ref::<i32>().is_some())
    }

    fn connect(
        &self,
        _value: &ExportValue,
        _connection: Arc<dyn types::MessageStream>,
    ) -> Result<Arc<dyn types::MessageStream>, ExportError> {
        Ok(Arc::new(Noop))
    }
}

#[test]
fn a_type_is_registered_under_a_name_java_would_take() {
    let types = ObjectTypes::new();
    let register = |name| types.register(Arc::new(Named(name, true)));
    assert_eq!(register("my.Type"), Ok(()));
    assert_eq!(register("a name with spaces"), Ok(()));
    for name in ["Table", "table", "TreeTable", "RollupTable", ""] {
        assert_eq!(
            register(name),
            Err(format!("Unable to register type, name is reserved: {name}"))
        );
    }
    for name in ["class", "Int", "TRUE", "null", "_"] {
        assert_eq!(
            register(name),
            Err(format!("Unable to register type, name is keyword: {name}"))
        );
    }
    // not keywords of Java, whatever they mean in it
    for name in ["var", "record", "non-sealed"] {
        assert_eq!(register(name), Ok(()));
    }
    // a name is taken whatever its case; so are those of the types Java has built in
    for name in ["my.type", "MY.TYPE", "Figure", "HierarchicalTable"] {
        assert_eq!(
            register(name),
            Err(format!(
                "Unable to register type, type name already registered: {name}"
            ))
        );
    }
    assert_eq!(
        types.names(),
        [
            "my.Type",
            "a name with spaces",
            "var",
            "record",
            "non-sealed"
        ]
    );
    // the server's own registry has the partitioned table before anything else
    assert_eq!(registry().names()[0], "PartitionedTable");
    assert_eq!(
        registry().register(Arc::new(Named("partitionedtable", true))),
        Err("Unable to register type, type name already registered: partitionedtable".to_string())
    );
}

#[test]
fn the_type_of_a_value_is_the_first_registered_that_takes_it() {
    let types = ObjectTypes::new();
    for (name, takes) in [("none", false), ("first", true), ("second", true)] {
        types.register(Arc::new(Named(name, takes))).unwrap();
    }
    let number = ExportValue::object(5_i32);
    assert_eq!(types.type_name(&number).unwrap().as_deref(), Some("first"));
    assert_eq!(types.type_name(&ExportValue::object(5_i64)).unwrap(), None);
    assert_eq!(
        types
            .type_name(&ExportValue::Table(rows()))
            .unwrap()
            .as_deref(),
        Some("Table")
    );
    assert_eq!(types.instance("first", &number).unwrap().name(), "first");
    let Err(ExportError::Status(status)) = types.instance("second", &number) else {
        panic!("a status");
    };
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "Unexpected ObjectType, expected type 'second', actual type 'first'"
    );
    // a type that fails to say whether a value is its own fails the lookup
    let text = ExportValue::object("text".to_string());
    assert!(matches!(
        types.type_name(&text),
        Err(ExportError::Internal(_))
    ));
    assert!(matches!(
        types.instance("first", &text),
        Err(ExportError::Internal(_))
    ));
}

#[tokio::test]
async fn fetch_object_wants_one_message_and_the_end_from_a_type_that_talks() {
    let server = &Server::start().await;
    let fetched = |id: i32| async move {
        ObjectServiceClient::new(server.channel.clone())
            .fetch_object(server.authorized(fetch(Some(id), TALKER)))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| (status.code(), status.message().to_string()))
    };

    let once = talker(server, 1, "once");
    let response = fetched(1).await.unwrap();
    assert_eq!(
        (response.r#type.as_str(), response.data.as_slice()),
        (TALKER, &b"once"[..])
    );
    let types: Vec<&str> = response
        .typed_export_ids
        .iter()
        .map(|t| t.r#type.as_str())
        .collect();
    // a table, an object of a type, and an object of none
    assert_eq!(types, ["Table", TALKER, ""]);
    for reference in &response.typed_export_ids {
        let ticket = &reference.ticket.as_ref().unwrap().ticket;
        let id = crate::session::ticket_to_export_id(ticket, "reference").unwrap();
        assert!(id < 0, "{id}");
        assert!(server.session().get_export(id).unwrap().value().is_some());
    }
    // the stream the type answered with is not told anything
    once.not_closed().await;
    assert_eq!(once.log(), ["connect"]);

    // the last message before the end is the answer
    talker(server, 2, "twice");
    let response = fetched(2).await.unwrap();
    assert_eq!(response.data, b"once");
    assert_eq!(response.typed_export_ids.len(), 3);

    talker(server, 3, "talk");
    assert_eq!(
        fetched(3).await.unwrap_err(),
        (
            Code::InvalidArgument,
            "Plugin didn't close response, use MessageStream instead for this object".to_string()
        )
    );
    for (id, mode) in [(4, "silent"), (5, "shut")] {
        talker(server, id, mode);
        assert_eq!(
            fetched(id).await.unwrap_err(),
            (
                Code::InvalidArgument,
                "Plugin didn't send a response before returning from clientConnection()"
                    .to_string()
            ),
            "{mode}"
        );
    }
    talker(server, 6, "refuse");
    details_logged(fetched(6).await.map(|_| ()));
}

#[tokio::test]
async fn a_message_stream_carries_messages_and_references_both_ways() {
    let server = &Server::start().await;
    server.export(1, rows());
    let object = talker(server, 2, "talk");

    let (requests, mut responses) = stream(server, connect(2, TALKER)).await;
    assert_eq!(heard(&mut responses).await, ("hello".to_string(), vec![]));
    requests.send(say("one", &[1, 2])).unwrap();
    requests.send(say("two", &[])).unwrap();
    assert_eq!(
        heard(&mut responses).await,
        (
            "one".to_string(),
            vec!["Table".to_string(), TALKER.to_string()]
        )
    );
    assert_eq!(heard(&mut responses).await, ("two".to_string(), vec![]));
    // the client closes its side: the stream ends, and the object is told
    drop(requests);
    assert_eq!(end(&mut responses).await, Ok(()));
    object.closed().await;
    assert_eq!(
        object.log(),
        ["connect", "data one 2", "data two 0", "close"]
    );

    // an object that sends nothing when it is opened has a stream all the same
    talker(server, 3, "silent");
    let (requests, mut responses) = stream(server, connect(3, TALKER)).await;
    requests.send(say("anyone", &[])).unwrap();
    assert_eq!(heard(&mut responses).await, ("anyone".to_string(), vec![]));
}

#[tokio::test]
async fn the_object_is_told_of_the_end_of_a_stream_it_did_not_end() {
    let server = &Server::start().await;

    // the object closes: the stream ends, later messages go nowhere, it is not told
    let object = talker(server, 1, "talk");
    let (requests, mut responses) = stream(server, connect(1, TALKER)).await;
    assert_eq!(heard(&mut responses).await.0, "hello");
    requests.send(say("late", &[])).unwrap();
    requests.send(say("unheard", &[])).unwrap();
    assert_eq!(end(&mut responses).await, Ok(()));
    object.not_closed().await;
    assert_eq!(
        object.log(),
        [
            "connect",
            "data late 0",
            "io.deephaven.plugin.type.ObjectCommunicationException: \
             java.lang.IllegalStateException: Stream is already completed, no further calls \
             are allowed"
        ]
    );

    // the object fails on a message: the client is told, and so is the object
    let object = talker(server, 2, "talk");
    let (requests, mut responses) = stream(server, connect(2, TALKER)).await;
    assert_eq!(heard(&mut responses).await.0, "hello");
    requests.send(say("fail", &[])).unwrap();
    details_logged(end(&mut responses).await);
    object.closed().await;

    // a reference that does not resolve fails the stream the same way
    let object = talker(server, 3, "talk");
    let (requests, mut responses) = stream(server, connect(3, TALKER)).await;
    assert_eq!(heard(&mut responses).await.0, "hello");
    let mut message = say("x", &[]);
    if let Some(ClientMessage::Data(data)) = &mut message.message {
        data.references.push(TypedTicket {
            ticket: Some(Ticket {
                ticket: b"s/nope".to_vec(),
            }),
            r#type: String::new(),
        });
    }
    requests.send(message).unwrap();
    assert_eq!(
        end(&mut responses).await,
        Err((
            Code::NotFound,
            "Could not resolve 'ticket: variable 'nope' not found".to_string()
        ))
    );
    object.closed().await;
    assert_eq!(object.log(), ["connect", "close"]);

    // the client gives the call up
    let object = talker(server, 4, "talk");
    let (requests, mut responses) = stream(server, connect(4, TALKER)).await;
    assert_eq!(heard(&mut responses).await.0, "hello");
    drop(responses);
    drop(requests);
    object.closed().await;

    // an object that refuses the client was never opened, so there is nothing to tell
    let object = talker(server, 5, "refuse");
    let (_requests, mut responses) = stream(server, connect(5, TALKER)).await;
    details_logged(end(&mut responses).await);
    object.not_closed().await;

    // a message the service refuses ends the stream without a word to the object
    let object = talker(server, 6, "talk");
    let (requests, mut responses) = stream(server, connect(6, TALKER)).await;
    assert_eq!(heard(&mut responses).await.0, "hello");
    requests.send(connect(6, TALKER)).unwrap();
    assert_eq!(
        end(&mut responses).await,
        Err((
            Code::FailedPrecondition,
            "Already sent a connect request, cannot send another".to_string()
        ))
    );
    object.not_closed().await;

    // the session ends: the call is cancelled, and the object is not told
    let object = talker(server, 7, "talk");
    let (_requests, mut responses) = stream(server, connect(7, TALKER)).await;
    assert_eq!(heard(&mut responses).await.0, "hello");
    server.state.sessions.close_session(&server.session());
    assert_eq!(
        end(&mut responses).await,
        Err((Code::Cancelled, "Session closed".to_string()))
    );
    object.not_closed().await;
}

/// A request with the stream headers of a browser.
fn framed<T>(server: &Server, message: T, at: (i32, i32), half_close: bool) -> Request<T> {
    let (stream, sequence) = at;
    let mut request = server.authorized(message);
    let metadata = request.metadata_mut();
    metadata.insert(
        "x-deephaven-stream-ticket",
        stream.to_string().parse().unwrap(),
    );
    metadata.insert(
        "x-deephaven-stream-sequence",
        sequence.to_string().parse().unwrap(),
    );
    if half_close {
        metadata.insert("x-deephaven-stream-halfclose", "1".parse().unwrap());
    }
    request
}

#[tokio::test]
async fn a_browser_sends_the_messages_of_a_stream_as_calls() {
    let server = &Server::start().await;
    let service = || ObjectServiceClient::new(server.channel.clone());
    let refused = |status: Status| (status.code(), status.message().to_string());

    let object = talker(server, 1, "talk");
    let mut responses = service()
        .open_message_stream(framed(server, connect(1, TALKER), (8, 0), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(heard(&mut responses).await.0, "hello");
    // the calls arrive out of order, and the messages are handled in theirs
    service()
        .next_message_stream(framed(server, say("two", &[]), (8, 2), false))
        .await
        .unwrap();
    service()
        .next_message_stream(framed(server, say("one", &[]), (8, 1), false))
        .await
        .unwrap();
    assert_eq!(heard(&mut responses).await.0, "one");
    assert_eq!(heard(&mut responses).await.0, "two");
    service()
        .next_message_stream(framed(server, StreamRequest::default(), (8, 3), true))
        .await
        .unwrap();
    assert_eq!(end(&mut responses).await, Ok(()));
    object.closed().await;

    // After the object closed the stream, a call for it is still answered, and its
    // message goes nowhere: the client's side is an export until the client ends it.
    let object = talker(server, 2, "talk");
    let mut responses = service()
        .open_message_stream(framed(server, connect(2, TALKER), (9, 0), false))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(heard(&mut responses).await.0, "hello");
    service()
        .next_message_stream(framed(server, say("close", &[]), (9, 1), false))
        .await
        .unwrap();
    assert_eq!(end(&mut responses).await, Ok(()));
    service()
        .next_message_stream(framed(server, say("unheard", &[]), (9, 2), false))
        .await
        .unwrap();
    service()
        .next_message_stream(framed(server, StreamRequest::default(), (9, 3), true))
        .await
        .unwrap();
    assert_eq!(object.log(), ["connect", "data close 0"]);

    // what a stream refuses is the status of the call that opened it
    let status = service()
        .open_message_stream(framed(server, say("early", &[]), (10, 0), false))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap_err();
    assert_eq!(
        refused(status),
        (
            Code::InvalidArgument,
            "Data message sent before Connect message".to_string()
        )
    );

    let status = service()
        .open_message_stream(server.authorized(connect(1, TALKER)))
        .await
        .unwrap_err();
    assert_eq!(
        refused(status),
        (
            Code::InvalidArgument,
            "no x-deephaven-stream headers, cannot handle open request".to_string()
        )
    );
    let status = service()
        .next_message_stream(server.authorized(say("x", &[])))
        .await
        .unwrap_err();
    assert_eq!(
        refused(status),
        (
            Code::InvalidArgument,
            "no x-deephaven-stream headers, cannot handle next request".to_string()
        )
    );
}
