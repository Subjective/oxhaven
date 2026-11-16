//! Tests of the object types that Python plugins register.
//!
//! The expectations are what Java 42.9 answered to a plugin of the same shape, installed
//! beside its `deephaven-server` wheel; `docs/OBJECTS.md` lists them. A process has one
//! registry of object types, so the plugin of these tests is registered once, by the first
//! test that needs it, and stays.

// FetchObject is deprecated in the protocol and still what clients without streams call
#![allow(deprecated)]

use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use oxhaven_proto::grpc::object_service_client::ObjectServiceClient;
use oxhaven_proto::grpc::stream_request::Message as ClientMessage;
use oxhaven_proto::grpc::stream_response::Message as ServerMessage;
use oxhaven_proto::grpc::{
    ClientData, ConnectRequest, FetchObjectRequest, FetchObjectResponse, StreamRequest,
    StreamResponse, Ticket, TypedTicket,
};
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{ExecuteCommandRequest, StartConsoleRequest};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Code, Streaming};

use super::super::Variable;
use super::tests::Python;
use crate::object::types::registry;
use crate::session::{export_id_to_ticket, ticket_to_export_id};
use crate::test_support::with_auth;

/// The package of the tests' plugin: a module, found by the entry point of a distribution
/// beside it, as `pip` installs one.
const PLUGIN: &str = r#"
from deephaven.plugin import Registration
from deephaven.plugin.object_type import BidirectionalObjectType, FetchOnlyObjectType, MessageStream


class Fetched:
    def __init__(self, payload=b"", references=(), fail=None, how=None):
        self.payload, self.references, self.fail, self.how = payload, references, fail, how
        self.log = []


class FetchedType(FetchOnlyObjectType):
    @property
    def name(self):
        return "oxplug.Fetched"

    def is_type(self, obj):
        return isinstance(obj, Fetched)

    def to_bytes(self, exporter, obj):
        if obj.fail:
            raise ValueError(obj.fail)
        if obj.how is not None:
            try:
                exporter.reference(obj, *obj.how)
            except Exception as error:
                obj.log.append(f"{type(error).__name__}: {error}")
        indexes = [exporter.reference(reference).index for reference in obj.references]
        obj.log.append(indexes)
        return obj.payload


class Chat:
    def __init__(self, hello=b"hello"):
        self.hello = hello
        self.log = []


class ChatStream(MessageStream):
    def __init__(self, chat, connection):
        super().__init__()
        self.chat, self.connection = chat, connection

    def on_data(self, payload, references):
        kinds = [type(reference).__name__ for reference in references]
        self.chat.log.append(f"data {type(payload).__name__} {bytes(payload)!r} {kinds}")
        if payload == b"close":
            self.connection.on_close()
            try:
                self.connection.on_data(b"late", [])
            except Exception as error:
                self.chat.log.append(f"{type(error).__name__}: {error}")
        elif payload == b"raise":
            raise ValueError("the plugin failed")
        elif payload == b"size":
            self.connection.on_data(str(references[0].size).encode(), [])
        elif payload == b"kinds":
            self.connection.on_data(bytearray(b"a bytearray"), (references[0],))
            try:
                self.connection.on_data("a str", [])
            except Exception as error:
                self.chat.log.append(f"{type(error).__name__}: {error}")
        else:
            self.connection.on_data(b"echo:" + payload, references)

    def on_close(self):
        self.chat.log.append("close")
        raise ValueError("ignored")


class ChatType(BidirectionalObjectType):
    @property
    def name(self):
        return "oxplug.Chat"

    def is_type(self, obj):
        return isinstance(obj, Chat)

    def create_client_connection(self, obj, connection):
        obj.log.append(f"connect {type(connection).__name__}")
        if obj.hello is None:
            raise ValueError("nobody home")
        connection.on_data(obj.hello, [])
        return ChatStream(obj, connection)


class Picky:
    def __init__(self, answer):
        self.answer = answer


class PickyType(FetchOnlyObjectType):
    @property
    def name(self):
        return "oxplug.Picky"

    def is_type(self, obj):
        if not isinstance(obj, Picky):
            return False
        if obj.answer == "raise":
            raise ValueError("picky")
        return obj.answer

    def to_bytes(self, exporter, obj):
        return b"picky"


class Both(Fetched, Chat):
    pass


class Named(FetchOnlyObjectType):
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    def is_type(self, obj):
        return False

    def to_bytes(self, exporter, obj):
        return b""


class OxplugRegistration(Registration):
    @classmethod
    def register_into(cls, callback):
        # a class, which is created, and an object
        callback.register(FetchedType)
        callback.register(ChatType())
        callback.register(PickyType)
"#;

static REGISTERED: AtomicBool = AtomicBool::new(false);

/// A Python console whose process has the plugin of these tests, with its module's names
/// imported.
async fn plugged() -> Python {
    let python = Python::start().await;
    // the tests take turns, so this one is the only reader and writer
    if !REGISTERED.load(Ordering::SeqCst) {
        let base = std::env::temp_dir().join(format!("oxhaven-plugin-{}", uuid::Uuid::new_v4()));
        let distribution = base.join("oxplug-0.1.dist-info");
        fs::create_dir_all(&distribution).unwrap();
        fs::write(base.join("oxplug.py"), PLUGIN).unwrap();
        fs::write(
            distribution.join("METADATA"),
            "Metadata-Version: 2.1\nName: oxplug\nVersion: 0.1\n",
        )
        .unwrap();
        fs::write(
            distribution.join("entry_points.txt"),
            "[deephaven.plugin]\nregistration_cls = oxplug:OxplugRegistration\n",
        )
        .unwrap();
        // What the server does when it starts, for this one distribution: the environment
        // of the tests may have plugins of its own, which the server registered then.
        python.run(&format!(
            "import importlib, sys\n\
             sys.path.insert(0, {base:?})\n\
             importlib.invalidate_caches()\n\
             import deephaven.plugin\n\
             from deephaven_internal.plugin.register import RegistrationAdapter\n\
             found = [e for e in deephaven.plugin.collect_registration_entrypoints() \
             if e.value == 'oxplug:OxplugRegistration']\n\
             assert len(found) == 1, found\n\
             callback = RegistrationAdapter()\n\
             found[0].load().register_into(callback)\n\
             assert callback.object_types == 3\n\
             sys.path.remove({base:?})\n\
             del importlib, sys, deephaven, RegistrationAdapter, found, callback\n"
        ));
        fs::remove_dir_all(&base).unwrap();
        REGISTERED.store(true, Ordering::SeqCst);
    }
    python.run("from oxplug import *\nfrom deephaven import empty_table");
    python
}

fn listed(variables: &[Variable]) -> Vec<(&str, &str)> {
    let mut listed: Vec<(&str, &str)> = variables
        .iter()
        .map(|variable| (variable.name.as_str(), variable.type_name.as_str()))
        .collect();
    listed.sort_unstable();
    listed
}

fn scope(name: &str, type_name: &str) -> Option<TypedTicket> {
    Some(TypedTicket {
        ticket: Some(Ticket {
            ticket: format!("s/{name}").into_bytes(),
        }),
        r#type: type_name.to_string(),
    })
}

/// A client of the object service, logged in.
struct Client<'a> {
    python: &'a Python,
    bearer: String,
}

impl<'a> Client<'a> {
    async fn new(python: &'a Python) -> Self {
        let bearer = python.server.login().await;
        Self { python, bearer }
    }

    fn service(&self) -> ObjectServiceClient<tonic::transport::Channel> {
        ObjectServiceClient::new(self.python.server.channel.clone())
    }

    async fn fetch(
        &self,
        name: &str,
        type_name: &str,
    ) -> Result<FetchObjectResponse, tonic::Status> {
        let request = FetchObjectRequest {
            source_id: scope(name, type_name),
        };
        let mut service = self.service();
        let answer = service.fetch_object(with_auth(request, &self.bearer));
        answer.await.map(tonic::Response::into_inner)
    }

    /// The type names of the references of a message, each of which must be an export the
    /// server chose and that has a value.
    fn types(&self, references: &[TypedTicket]) -> Vec<String> {
        let session = self.python.server.session(&self.bearer);
        references
            .iter()
            .map(|reference| {
                let ticket = &reference.ticket.as_ref().unwrap().ticket;
                let id = ticket_to_export_id(ticket, "reference").unwrap();
                assert!(id < 0, "{id}");
                assert!(session.get_export(id).unwrap().value().is_some());
                reference.r#type.clone()
            })
            .collect()
    }

    /// Opens a message stream to a variable.
    async fn connect(&self, name: &str, type_name: &str) -> Conversation<'_> {
        let (requests, sent) = tokio::sync::mpsc::unbounded_channel();
        let connect = ConnectRequest {
            source_id: scope(name, type_name),
        };
        requests
            .send(StreamRequest {
                message: Some(ClientMessage::Connect(connect)),
            })
            .unwrap();
        let request = with_auth(UnboundedReceiverStream::new(sent), &self.bearer);
        let responses = self.service().message_stream(request).await.unwrap();
        Conversation {
            client: self,
            requests,
            responses: responses.into_inner(),
        }
    }
}

struct Conversation<'a> {
    client: &'a Client<'a>,
    requests: tokio::sync::mpsc::UnboundedSender<StreamRequest>,
    responses: Streaming<StreamResponse>,
}

impl Conversation<'_> {
    fn say(&self, payload: &[u8], references: &[&str]) {
        let data = ClientData {
            payload: payload.to_vec(),
            references: references
                .iter()
                .map(|name| scope(name, "").unwrap())
                .collect(),
        };
        self.requests
            .send(StreamRequest {
                message: Some(ClientMessage::Data(data)),
            })
            .unwrap();
    }

    /// The next message: its bytes and the types of its references.
    async fn heard(&mut self) -> (Vec<u8>, Vec<String>) {
        let next = tokio::time::timeout(Duration::from_secs(10), self.responses.message());
        let message = next.await.expect("a message").unwrap();
        let Some(ServerMessage::Data(data)) = message.expect("a message, not the end").message
        else {
            panic!("a data message");
        };
        let types = self.client.types(&data.exported_references);
        (data.payload, types)
    }

    /// How the stream ends: `Ok` for its end, or the status.
    async fn end(&mut self) -> Result<(), tonic::Status> {
        let next = tokio::time::timeout(Duration::from_secs(10), self.responses.message());
        match next.await.expect("the stream ends") {
            Ok(None) => Ok(()),
            Ok(Some(message)) => panic!("the end, not {message:?}"),
            Err(status) => Err(status),
        }
    }
}

fn details_logged(status: &tonic::Status) {
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert!(
        status.message().starts_with("Details Logged w/ID '"),
        "{status:?}"
    );
}

/// Waits until `condition`, a Python expression, holds; fails with `shown` if it never does.
async fn eventually(python: &Python, condition: &str, shown: &str) {
    for _ in 0..200 {
        if python
            .console
            .evaluate(&format!("assert {condition}"))
            .error
            .is_none()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    python.run(&format!("assert {condition}, {shown}"));
}

#[tokio::test]
async fn the_plugins_of_the_environment_are_registered_in_their_order() {
    let _python = plugged().await;
    let names = registry().names();
    let place = |name: &str| {
        let found = names.iter().position(|registered| registered == name);
        found.unwrap_or_else(|| panic!("{name} is not registered: {names:?}"))
    };
    // the server's own types come before those of the environment
    assert_eq!(place("PartitionedTable"), 0);
    assert!(place("pandas.DataFrame") < place("oxplug.Fetched"));
    assert_eq!(place("oxplug.Chat"), place("oxplug.Fetched") + 1);
    assert_eq!(place("oxplug.Picky"), place("oxplug.Fetched") + 2);
}

#[tokio::test]
async fn a_variable_of_a_registered_type_is_listed_with_the_type() {
    let python = plugged().await;
    let changes = python.run(
        "t = empty_table(3)\nf = Fetched(b'x')\nc = Chat()\nn = 5\nu = object()\nboth = Both()",
    );
    // an object of two types is of the one that was registered first
    assert_eq!(
        listed(&changes.created),
        [
            ("both", "oxplug.Fetched"),
            ("c", "oxplug.Chat"),
            ("f", "oxplug.Fetched"),
            ("t", "Table")
        ]
    );
    let changes = python.run("f = Fetched(b'y')");
    assert_eq!(listed(&changes.updated), [("f", "oxplug.Fetched")]);
    assert!(changes.created.is_empty() && changes.removed.is_empty());
    // another type: a new variable in place of the old
    let changes = python.run("f = Chat()");
    assert_eq!(listed(&changes.created), [("f", "oxplug.Chat")]);
    assert_eq!(listed(&changes.removed), [("f", "oxplug.Fetched")]);
    let changes = python.run("f = 5");
    assert_eq!(listed(&changes.removed), [("f", "oxplug.Chat")]);
    assert!(changes.created.is_empty() && changes.updated.is_empty());
    // an object of a subclass is of the type its class is of
    let changes = python.run("class Sub(Fetched):\n    pass\nsub = Sub()\ndel c");
    assert_eq!(listed(&changes.created), [("sub", "oxplug.Fetched")]);
    assert_eq!(listed(&changes.removed), [("c", "oxplug.Chat")]);
}

#[tokio::test]
async fn a_data_frame_is_of_a_type_once_pandas_is_loaded() {
    let python = plugged().await;
    // Without pandas in the environment a module of its name stands in for it: the type
    // asks only whether the object is of the class `DataFrame` of the loaded module.
    let changes = python.run(
        "import sys, types\n\
         before = sys.modules.get('pandas')\n\
         class DataFrame:\n    pass\n\
         early = DataFrame()\n\
         sys.modules.pop('pandas', None)\n",
    );
    assert_eq!(listed(&changes.created), []);
    let changes = python.run(
        "stand_in = types.ModuleType('pandas')\n\
         stand_in.DataFrame = DataFrame\n\
         sys.modules['pandas'] = stand_in\n\
         frame = DataFrame()\n",
    );
    // `early` is one too now, but a variable is looked at again only when it changes
    assert!(listed(&changes.created).contains(&("frame", "pandas.DataFrame")));
    let changes = python.run("del frame");
    assert_eq!(listed(&changes.removed), [("frame", "pandas.DataFrame")]);
    python.run(
        "del sys.modules['pandas']\n\
         if before is not None:\n    sys.modules['pandas'] = before\n",
    );
}

#[tokio::test]
async fn an_object_that_is_only_fetched_answers_with_its_bytes_and_references() {
    let python = plugged().await;
    let client = Client::new(&python).await;
    python.run(
        "t = empty_table(3)\nc = Chat()\n\
         f = Fetched(b'bytes', [t, c, 7, 'text', t])\n\
         view = Fetched(memoryview(b'a view'))\n\
         array = Fetched(bytearray(b'an array'))\n",
    );
    let fetched = client.fetch("f", "oxplug.Fetched").await.unwrap();
    assert_eq!(fetched.r#type, "oxplug.Fetched");
    assert_eq!(fetched.data, b"bytes");
    // a table, an object of a type, objects of none; each reference is an export of its own
    assert_eq!(
        client.types(&fetched.typed_export_ids),
        ["Table", "oxplug.Chat", "", "", "Table"]
    );
    assert_ne!(
        fetched.typed_export_ids[0].ticket,
        fetched.typed_export_ids[4].ticket
    );
    python.run("assert f.log == [[0, 1, 2, 3, 4]], f.log");
    assert_eq!(
        client.fetch("view", "oxplug.Fetched").await.unwrap().data,
        b"a view"
    );
    assert_eq!(
        client.fetch("array", "oxplug.Fetched").await.unwrap().data,
        b"an array"
    );

    // what a type answers that is no bytes fails the fetch, as an exception does
    python.run(
        "a_str = Fetched('a str')\nnothing = Fetched(None)\nnumber = Fetched(5)\n\
         failing = Fetched(fail='to_bytes failed')\nto_none = Fetched(b'x', [None])\n",
    );
    for name in ["a_str", "nothing", "number", "failing"] {
        details_logged(&client.fetch(name, "oxplug.Fetched").await.unwrap_err());
    }
    // a reference to None, which Java exports as an export no client can use, is refused
    details_logged(&client.fetch("to_none", "oxplug.Fetched").await.unwrap_err());

    // the two arguments of `Exporter.reference` that Java refuses
    python.run("known = Fetched(b'x', how=(False, True))\nold = Fetched(b'x', how=(True, False))");
    client.fetch("known", "oxplug.Fetched").await.unwrap();
    client.fetch("old", "oxplug.Fetched").await.unwrap();
    python.run(
        "assert known.log[0] == 'RuntimeError: java.lang.IllegalArgumentException: \
         allowUnknownType must be true', known.log\n\
         assert old.log[0] == 'RuntimeError: java.lang.IllegalArgumentException: \
         forceNew must be true', old.log",
    );

    let status = client.fetch("c", "oxplug.Fetched").await.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "Unexpected ObjectType, expected type 'oxplug.Fetched', actual type 'oxplug.Chat'"
    );
    python.run("n = 5");
    let status = client.fetch("n", "oxplug.Fetched").await.unwrap_err();
    assert_eq!(status.code(), Code::NotFound);
    assert_eq!(
        status.message(),
        "No ObjectType found, expected type 'oxplug.Fetched'"
    );
    // an object that talks is not fetched
    let status = client.fetch("c", "oxplug.Chat").await.unwrap_err();
    assert_eq!(
        status.message(),
        "Plugin didn't close response, use MessageStream instead for this object"
    );
}

#[tokio::test]
async fn an_object_that_talks_exchanges_messages_with_its_client() {
    let python = plugged().await;
    let client = Client::new(&python).await;
    python.run("t = empty_table(3)\nc = Chat()\nf = Fetched(b'x')\nn = 5");

    let mut chat = client.connect("c", "oxplug.Chat").await;
    assert_eq!(chat.heard().await, (b"hello".to_vec(), vec![]));
    chat.say(b"one", &["t", "f", "n"]);
    chat.say(b"size", &["t"]);
    chat.say(b"kinds", &["t"]);
    assert_eq!(
        chat.heard().await,
        (
            b"echo:one".to_vec(),
            vec![
                "Table".to_string(),
                "oxplug.Fetched".to_string(),
                String::new()
            ]
        )
    );
    // a table arrives as the package's `Table`, whose methods work
    assert_eq!(chat.heard().await, (b"3".to_vec(), vec![]));
    assert_eq!(
        chat.heard().await,
        (b"a bytearray".to_vec(), vec!["Table".to_string()])
    );
    // the client closes its side: the stream ends, and the plugin is told; what the
    // plugin raises then is ignored
    drop(chat.requests);
    let mut responses = chat.responses;
    assert!(responses.message().await.unwrap().is_none());
    eventually(&python, "c.log[-1] == 'close'", "c.log").await;
    python.run(
        "assert c.log == [\n\
         'connect ClientResponseStreamAdapter',\n\
         \"data bytes b'one' ['Table', 'Fetched', 'int']\",\n\
         \"data bytes b'size' ['Table']\",\n\
         \"data bytes b'kinds' ['Table']\",\n\
         \"TypeError: 'str' object cannot be interpreted as an integer\",\n\
         'close'], c.log",
    );

    // the plugin closes: the stream ends, the plugin's own stream is not told, and what
    // it sends afterwards is refused
    python.run("c = Chat()");
    let mut chat = client.connect("c", "oxplug.Chat").await;
    chat.heard().await;
    chat.say(b"close", &[]);
    chat.say(b"unheard", &[]);
    assert!(chat.end().await.is_ok());
    tokio::time::sleep(Duration::from_millis(150)).await;
    python.run(
        "assert c.log == [\n\
         'connect ClientResponseStreamAdapter',\n\
         \"data bytes b'close' []\",\n\
         'RuntimeError: io.deephaven.plugin.type.ObjectCommunicationException: \
         java.lang.IllegalStateException: Stream is already completed, no further calls are \
         allowed'], c.log",
    );

    // the plugin raises: the client is told no more than an id, and the plugin is told
    // that the stream is over
    python.run("c = Chat()");
    let mut chat = client.connect("c", "oxplug.Chat").await;
    chat.heard().await;
    chat.say(b"raise", &[]);
    details_logged(&chat.end().await.unwrap_err());
    eventually(&python, "c.log[-1] == 'close'", "c.log").await;

    // the client gives the call up
    python.run("c = Chat()");
    let mut chat = client.connect("c", "oxplug.Chat").await;
    chat.heard().await;
    drop(chat);
    eventually(&python, "c.log[-1] == 'close'", "c.log").await;

    // a plugin that fails to open the object
    python.run("c = Chat(None)");
    let mut chat = client.connect("c", "oxplug.Chat").await;
    details_logged(&chat.end().await.unwrap_err());

    // an object that is only fetched, as a stream: its message, and the end
    let mut fetched = client.connect("f", "oxplug.Fetched").await;
    assert_eq!(fetched.heard().await, (b"x".to_vec(), vec![]));
    assert!(fetched.end().await.is_ok());
}

#[tokio::test]
async fn a_type_that_fails_to_say_whether_an_object_is_its_own_fails_the_command() {
    let python = plugged().await;
    // what counts as true in Python is a yes
    let changes = python.run("truthy = Picky('yes')");
    assert_eq!(listed(&changes.created), [("truthy", "oxplug.Picky")]);

    let bearer = python.server.login().await;
    let mut console = ConsoleServiceClient::new(python.server.channel.clone());
    let start = StartConsoleRequest {
        result_id: Some(export_id_to_ticket(1)),
        session_type: "python".to_string(),
    };
    console
        .start_console(with_auth(start, &bearer))
        .await
        .unwrap();
    let mut execute = async |code: &str| {
        let request = ExecuteCommandRequest {
            console_id: Some(export_id_to_ticket(1)),
            code: code.to_string(),
            ..Default::default()
        };
        console.execute_command(with_auth(request, &bearer)).await
    };
    // the code runs, and the call fails all the same
    details_logged(
        &execute("raising = Picky('raise')\nran = [1]")
            .await
            .unwrap_err(),
    );
    // the next command starts from the variables as they are, so it succeeds
    let answer = execute("assert ran == [1]\nran.append(2)")
        .await
        .unwrap()
        .into_inner();
    assert_eq!(answer.error_message, "");
    // taking the variable away asks the type about the old value, which fails again
    details_logged(&execute("del raising").await.unwrap_err());
    python.run("assert ran == [1, 2] and 'raising' not in globals()");

    // None is no answer: Java fails to make a boolean of it
    let changes = python.console.evaluate("silent = Picky(None)");
    assert_eq!(changes.error, None);
    let failure = changes.failure.expect("a failure");
    assert!(
        failure.starts_with("java.lang.NullPointerException"),
        "{failure}"
    );
    assert!(changes.created.is_empty());
    let changes = python.console.evaluate("del silent");
    assert!(changes.failure.is_some());
    let changes = python.console.evaluate("raising = Picky('raise')");
    let failure = changes.failure.expect("a failure");
    assert!(
        failure.starts_with("Error in Python interpreter:"),
        "{failure}"
    );
    assert!(failure.contains("ValueError: picky"), "{failure}");
    python.console.evaluate("del raising");
}

#[tokio::test]
async fn a_registration_is_refused_as_java_refuses_it() {
    let python = plugged().await;
    python.run("from deephaven_internal.plugin.register import RegistrationAdapter");
    let refused = |plugin: &str| {
        let changes = python
            .console
            .evaluate(&format!("RegistrationAdapter().register({plugin})"));
        let error = changes.error.expect("an error");
        error
            .trim_end_matches('\n')
            .lines()
            .last()
            .unwrap()
            .to_string()
    };
    let illegal = "RuntimeError: java.lang.IllegalArgumentException: Unable to register type";
    assert_eq!(
        refused("Named('Table')"),
        format!("{illegal}, name is reserved: Table")
    );
    assert_eq!(
        refused("Named('')"),
        format!("{illegal}, name is reserved: ")
    );
    assert_eq!(
        refused("Named('Class')"),
        format!("{illegal}, name is keyword: Class")
    );
    assert_eq!(
        refused("Named('OXPLUG.fetched')"),
        format!("{illegal}, type name already registered: OXPLUG.fetched")
    );
    assert_eq!(
        refused("Named('PartitionedTable')"),
        format!("{illegal}, type name already registered: PartitionedTable")
    );
    assert_eq!(
        refused("object()"),
        "NotImplementedError: Unexpected type: <class 'object'>"
    );
    // A JS plugin is not served. It is left out with a warning, so that the object types
    // of its package are registered.
    let before = registry().names();
    python.run(
        "from deephaven.plugin.js import JsPlugin\n\
         import pathlib\n\
         class Js(JsPlugin):\n\
         \x20   name = 'js'\n\
         \x20   version = '0.1'\n\
         \x20   main = 'index.js'\n\
         \x20   def path(self):\n\
         \x20       return pathlib.Path('.')\n\
         adapter = RegistrationAdapter()\n\
         adapter.register(Js)\n\
         assert adapter.object_types == 0\n",
    );
    assert_eq!(registry().names(), before);
}
