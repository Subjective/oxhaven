//! Tests of the console service with a script session that needs no interpreter, so that
//! they run in every build: what is tested is the service, the scope tickets and the list of
//! fields. The expectations are what the Java reference answered to the same calls
//! (`oracle/console/probe.py`). The Python session has its own tests in `python::tests`.
//!
//! What Java answers to `GetConsoleTypes` without a console is replayed in `crate::ide`,
//! where the refusals of the other calls are checked too.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxhaven_engine::table::empty_table;
use oxhaven_proto::flight::flight_descriptor::DescriptorType;
use oxhaven_proto::flight::flight_service_client::FlightServiceClient;
use oxhaven_proto::flight::FlightDescriptor;
use oxhaven_proto::grpc::application_service_client::ApplicationServiceClient;
use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{
    ExportRequest, ExportedTableCreationResponse, FetchTableRequest, FieldsChangeUpdate,
    HandshakeRequest, ListFieldsRequest, PublishRequest, TableReference, Ticket,
};
use oxhaven_proto::script::auto_complete_request::Request as CompletionRequest;
use oxhaven_proto::script::auto_complete_response::Response as CompletionResponse;
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{
    AutoCompleteRequest, BindTableToVariableRequest, CancelAutoCompleteRequest,
    CancelCommandRequest, ExecuteCommandRequest, ExecuteCommandResponse, GetCompletionItemsRequest,
    GetConsoleTypesRequest, GetHeapInfoRequest, LogSubscriptionRequest, StartConsoleRequest,
};
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing_subscriber::layer::SubscriberExt;

use super::completer::Completer;
use super::logs::{LogBuffer, LogUpdates, STDERR, STDOUT};
use super::{Changes, Console, ScriptSession, Stop};
use crate::app::FormulaValue;
use crate::session::{export_id_to_ticket, ExportValue};
use crate::test_support::{with_auth, TestServer};
use crate::ServerState;

mod completion;

/// A script session whose language is one command per line: `make <name> <rows>` sets a
/// variable to a new table, `note <name> <text>` sets one to a text, `drop <name>` removes
/// one, `print <text>` writes a line to standard output, `wait` never ends unless the
/// command is stopped, and anything else is an error that ends the code.
struct WordSession {
    variables: Mutex<BTreeMap<String, ExportValue>>,
    /// The variables as they were when they were last looked at.
    seen: Mutex<BTreeMap<String, ExportValue>>,
    logs: Arc<LogBuffer>,
    /// What answers completion requests, if a test gave the session something that does.
    completer: Mutex<Option<Arc<dyn Completer>>>,
}

impl WordSession {
    fn set(&self, name: &str, value: ExportValue) {
        self.variables
            .lock()
            .unwrap()
            .insert(name.to_string(), value);
    }
}

impl ScriptSession for WordSession {
    fn script_type(&self) -> &str {
        "Words"
    }

    fn evaluate(&self, code: &str, stop: &Arc<Stop>) -> Changes {
        let mut error = None;
        if stop.is_requested() {
            return Changes {
                error: Some("stopped before it started".to_string()),
                ..Default::default()
            };
        }
        for line in code.lines() {
            let words: Vec<&str> = line.split_whitespace().collect();
            match words.as_slice() {
                ["make", name, rows] => {
                    self.set(name, ExportValue::Table(empty_table(rows.parse().unwrap())))
                }
                ["note", name, text] => self.set(name, ExportValue::object(text.to_string())),
                ["drop", name] => {
                    self.variables.lock().unwrap().remove(*name);
                }
                ["print", text @ ..] => self.logs.record(STDOUT, format!("{}\n", text.join(" "))),
                ["wait"] => {
                    while !stop.is_requested() {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    self.logs.record(STDOUT, "stopped\n");
                    error = Some("stopped".to_string());
                    break;
                }
                _ => {
                    error = Some(format!("not a command: {line}"));
                    break;
                }
            }
        }
        Changes {
            error,
            ..self.observe()
        }
    }

    fn observe(&self) -> Changes {
        let now = self.variables.lock().unwrap().clone();
        let mut seen = self.seen.lock().unwrap();
        let mut changes = Changes::default();
        for (name, value) in &now {
            changes.note(name, seen.get(name), Some(value));
        }
        for (name, value) in seen.iter() {
            if !now.contains_key(name) {
                changes.note(name, Some(value), None);
            }
        }
        *seen = now;
        changes
    }

    fn put(&self, name: &str, value: ExportValue) -> Result<Changes, String> {
        self.set(name, value);
        Ok(self.observe())
    }

    fn get(&self, name: &str) -> Option<ExportValue> {
        self.variables.lock().unwrap().get(name).cloned()
    }

    fn names(&self) -> Vec<String> {
        self.variables.lock().unwrap().keys().cloned().collect()
    }

    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String> {
        let variables = self.variables.lock().unwrap();
        Ok(names
            .iter()
            .map(|name| {
                let value = variables.get(*name)?;
                Some(match value.downcast_ref::<String>() {
                    Some(text) => FormulaValue::String(text.clone()),
                    None => FormulaValue::Other("a table".to_string()),
                })
            })
            .collect())
    }

    fn completer(&self) -> Result<Option<Arc<dyn Completer>>, String> {
        Ok(self.completer.lock().unwrap().clone())
    }
}

/// Gives `state` the word console and returns its session.
fn word_console(state: &mut ServerState) -> Arc<WordSession> {
    let session = Arc::new(WordSession {
        variables: Mutex::default(),
        seen: Mutex::default(),
        logs: state.logs.clone(),
        completer: Mutex::default(),
    });
    let console = Console::new(session.clone(), state.applications.clone());
    state.scope.hold(console.clone());
    state.console = Some(console);
    session
}

fn with_word_console(state: &mut ServerState) {
    word_console(state);
}

/// A logged-in client of a server with the word console, and the console it started as
/// export 1.
struct Client {
    server: TestServer,
    bearer: String,
    console: ConsoleServiceClient<Channel>,
}

impl Client {
    async fn start() -> Self {
        let server = TestServer::start_with(with_word_console).await;
        let bearer = server.login().await;
        let mut console = ConsoleServiceClient::new(server.channel.clone());
        let started = console
            .start_console(with_auth(
                StartConsoleRequest {
                    result_id: Some(export_id_to_ticket(1)),
                    session_type: "words".to_string(),
                },
                &bearer,
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(started.result_id, Some(export_id_to_ticket(1)));
        Self {
            server,
            bearer,
            console,
        }
    }

    async fn execute_on(
        &mut self,
        console_id: Option<Ticket>,
        code: &str,
    ) -> Result<ExecuteCommandResponse, Status> {
        let request = ExecuteCommandRequest {
            console_id,
            code: code.to_string(),
            ..Default::default()
        };
        let call = self
            .console
            .execute_command(with_auth(request, &self.bearer));
        tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .expect("an answer within five seconds")
            .map(tonic::Response::into_inner)
    }

    async fn execute(&mut self, code: &str) -> ExecuteCommandResponse {
        self.execute_on(Some(export_id_to_ticket(1)), code)
            .await
            .unwrap()
    }

    async fn fetch(
        &self,
        source: &[u8],
        result: i32,
    ) -> Result<ExportedTableCreationResponse, Status> {
        let request = FetchTableRequest {
            source_id: Some(TableReference {
                r#ref: Some(Ref::Ticket(Ticket {
                    ticket: source.to_vec(),
                })),
            }),
            result_id: Some(export_id_to_ticket(result)),
        };
        TableServiceClient::new(self.server.channel.clone())
            .fetch_table(with_auth(request, &self.bearer))
            .await
            .map(tonic::Response::into_inner)
    }
}

/// `kind name ticket type description application` of each change, in the order of the
/// message: created, updated, removed.
fn described(changes: &FieldsChangeUpdate) -> Vec<String> {
    let kinds = [
        ("created", &changes.created),
        ("updated", &changes.updated),
        ("removed", &changes.removed),
    ];
    kinds
        .iter()
        .flat_map(|(kind, fields)| {
            fields.iter().map(move |field| {
                let typed = field.typed_ticket.as_ref().unwrap();
                format!(
                    "{kind} {} {} {} [{}] [{}] [{}]",
                    field.field_name,
                    String::from_utf8_lossy(&typed.ticket.as_ref().unwrap().ticket),
                    typed.r#type,
                    field.field_description,
                    field.application_id,
                    field.application_name
                )
            })
        })
        .collect()
}

fn changes_of(response: &ExecuteCommandResponse) -> Vec<String> {
    described(response.changes.as_ref().expect("changes are always set"))
}

#[tokio::test]
async fn without_a_script_session_there_is_no_console_type_and_no_console_call() {
    let server = TestServer::start().await;
    let bearer = server.login().await;
    let mut console = ConsoleServiceClient::new(server.channel.clone());

    // without a session, as the web IDE asks before it logs in
    let types = console
        .get_console_types(GetConsoleTypesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(types.console_types.is_empty());

    let requests = tokio_stream::iter(vec![AutoCompleteRequest::default()]);
    let refused = console
        .auto_complete_stream(with_auth(requests, &bearer))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unimplemented);
    assert_eq!(
        refused.message(),
        "ConsoleService/AutoCompleteStream is not implemented: this server has no script session"
    );
}

#[tokio::test]
async fn the_console_type_is_the_language_in_lower_case_and_starts_in_any_case() {
    let mut client = Client::start().await;
    let types = client
        .console
        .get_console_types(GetConsoleTypesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(types.console_types, ["words"]);

    let start = |id: i32, session_type: &str| StartConsoleRequest {
        result_id: Some(export_id_to_ticket(id)),
        session_type: session_type.to_string(),
    };
    let unauthenticated = client
        .console
        .start_console(start(2, "words"))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let unknown = client
        .console
        .start_console(with_auth(start(2, "groovy"), &client.bearer))
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::FailedPrecondition);
    assert_eq!(unknown.message(), "session type 'groovy' is not supported");

    // the refused request did not take the export id
    client
        .console
        .start_console(with_auth(start(2, "WORDS"), &client.bearer))
        .await
        .unwrap();
    let taken = client
        .console
        .start_console(with_auth(start(1, "words"), &client.bearer))
        .await
        .unwrap_err();
    assert_eq!(taken.code(), Code::InvalidArgument);
    assert!(taken.message().starts_with("Details Logged w/ID '"));
}

#[tokio::test]
async fn a_command_reports_its_variables_and_scope_tickets_resolve_to_them() {
    let mut client = Client::start().await;

    let created = client.execute("make t 5").await;
    assert_eq!(
        changes_of(&created),
        ["created t s/t Table [query scope variable] [scope] []"]
    );
    assert_eq!(created.error_message, "");
    assert!(created.start_timestamp > 0 && created.end_timestamp >= created.start_timestamp);
    let fetched = client.fetch(b"s/t", 10).await.unwrap();
    assert!(fetched.success && fetched.is_static);
    assert_eq!(fetched.size, 5);

    let updated = client.execute("make t 3").await;
    assert_eq!(
        changes_of(&updated),
        ["updated t s/t Table [query scope variable] [scope] []"]
    );
    assert_eq!(client.fetch(b"s/t", 11).await.unwrap().size, 3);

    let mixed = client.execute("make a 1\nmake b 2\ndrop t").await;
    assert_eq!(
        changes_of(&mixed),
        [
            "created a s/a Table [query scope variable] [scope] []",
            "created b s/b Table [query scope variable] [scope] []",
            "removed t s/t Table [query scope variable] [scope] []",
        ]
    );
    let gone = client.fetch(b"s/t", 12).await.unwrap_err();
    assert_eq!(gone.code(), Code::NotFound);

    assert_eq!(changes_of(&client.execute("").await), [""; 0]);
}

#[tokio::test]
async fn an_error_in_the_code_is_an_answer_that_still_lists_what_changed() {
    let mut client = Client::start().await;
    let failed = client.execute("make v 1\nexplode\nmake w 1").await;
    assert_eq!(failed.error_message, "not a command: explode");
    assert_eq!(
        changes_of(&failed),
        ["created v s/v Table [query scope variable] [scope] []"]
    );
    assert_eq!(client.fetch(b"s/v", 10).await.unwrap().size, 1);
    assert_eq!(
        client.fetch(b"s/w", 11).await.unwrap_err().code(),
        Code::NotFound
    );
}

#[tokio::test]
async fn a_command_needs_a_console_id_that_names_a_console() {
    let mut client = Client::start().await;

    let missing = client.execute_on(None, "make t 1").await.unwrap_err();
    assert_eq!(missing.code(), Code::FailedPrecondition);
    assert_eq!(missing.message(), "No consoleId supplied");

    client.execute("make t 1").await;
    client.fetch(b"s/t", 10).await.unwrap();
    let no_console = client
        .execute_on(Some(export_id_to_ticket(10)), "make u 1")
        .await
        .unwrap_err();
    assert_eq!(no_console.code(), Code::InvalidArgument);
    assert!(no_console.message().starts_with("Details Logged w/ID '"));
    assert!(client.server.state.scope.get("u").is_none());
}

#[tokio::test]
async fn listening_clients_are_told_of_scope_variables() {
    let mut client = Client::start().await;
    client.execute("make early 1").await;

    let mut fields = ApplicationServiceClient::new(client.server.channel.clone())
        .list_fields(with_auth(ListFieldsRequest {}, &client.bearer))
        .await
        .unwrap()
        .into_inner();
    let mut next = async || {
        let message = tokio::time::timeout(Duration::from_secs(5), fields.next()).await;
        described(
            &message
                .expect("a message within five seconds")
                .unwrap()
                .unwrap(),
        )
    };
    assert_eq!(
        next().await,
        ["created early s/early Table [query scope variable] [scope] []"]
    );

    client.execute("make t 5").await;
    assert_eq!(
        next().await,
        ["created t s/t Table [query scope variable] [scope] []"]
    );
    // within one interval: replaced is updated, created and dropped is not mentioned
    let console = client.server.state.console.clone().unwrap();
    console.evaluate("make t 3\nmake brief 1");
    console.evaluate("drop brief\ndrop early");
    assert_eq!(
        next().await,
        [
            "updated t s/t Table [query scope variable] [scope] []",
            "removed early s/early Table [query scope variable] [scope] []",
        ]
    );
}

#[tokio::test]
async fn an_exported_table_becomes_a_variable() {
    let mut client = Client::start().await;
    client.execute("make t 4").await;
    client.fetch(b"s/t", 10).await.unwrap();

    let bind = |console_id: Option<Ticket>, name: &str, table_id: Option<Ticket>| {
        BindTableToVariableRequest {
            console_id,
            variable_name: name.to_string(),
            table_id,
        }
    };
    let table = Some(export_id_to_ticket(10));
    let console = Some(export_id_to_ticket(1));
    for (console_id, name, export) in [(console.clone(), "bound", 11), (None, "no_console", 12)] {
        client
            .console
            .bind_table_to_variable(with_auth(
                bind(console_id, name, table.clone()),
                &client.bearer,
            ))
            .await
            .unwrap();
        let fetched = client.fetch(format!("s/{name}").as_bytes(), export).await;
        assert_eq!(fetched.unwrap().size, 4);
    }

    let no_table = client
        .console
        .bind_table_to_variable(with_auth(bind(console, "x", None), &client.bearer))
        .await
        .unwrap_err();
    assert_eq!(no_table.code(), Code::FailedPrecondition);
    assert_eq!(no_table.message(), "No source tableId supplied");

    // a console id has to name a console
    let not_a_console = client
        .console
        .bind_table_to_variable(with_auth(bind(table.clone(), "y", table), &client.bearer))
        .await
        .unwrap_err();
    assert_eq!(not_a_console.code(), Code::InvalidArgument);
    assert!(client.server.state.scope.get("y").is_none());
}

#[tokio::test]
async fn printed_lines_reach_log_subscribers_by_level_and_time() {
    let mut client = Client::start().await;
    client.execute("print before").await;
    client.server.state.logs.record(STDERR, "an error line\n");

    let subscribe = |request: LogSubscriptionRequest| {
        let mut console = client.console.clone();
        let bearer = client.bearer.clone();
        async move {
            console
                .subscribe_to_logs(with_auth(request, &bearer))
                .await
                .unwrap()
                .into_inner()
        }
    };
    let mut all = subscribe(LogSubscriptionRequest::default()).await;
    let first = all.next().await.unwrap().unwrap();
    assert_eq!(
        (first.log_level.as_str(), first.message.as_str()),
        ("STDOUT", "before\n")
    );
    assert!(first.micros > 0);
    let second = all.next().await.unwrap().unwrap();
    assert_eq!(
        (second.log_level.as_str(), second.message.as_str()),
        ("STDERR", "an error line\n")
    );
    assert!(second.micros >= first.micros);

    // only one level, and nothing the client has seen
    let mut later = subscribe(LogSubscriptionRequest {
        last_seen_log_timestamp: second.micros,
        levels: vec!["STDOUT".to_string()],
    })
    .await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    client.server.state.logs.record(STDERR, "not wanted\n");
    client.execute("print after").await;
    let only = later.next().await.unwrap().unwrap();
    assert_eq!(only.message, "after\n");
    assert_eq!(all.next().await.unwrap().unwrap().message, "not wanted\n");
    assert_eq!(all.next().await.unwrap().unwrap().message, "after\n");
}

#[tokio::test]
async fn completion_finds_nothing_and_nothing_can_be_cancelled() {
    let mut client = Client::start().await;
    let requests = vec![
        AutoCompleteRequest {
            request_id: 1,
            ..Default::default()
        },
        AutoCompleteRequest {
            request_id: 2,
            request: Some(CompletionRequest::GetCompletionItems(
                GetCompletionItemsRequest::default(),
            )),
            ..Default::default()
        },
    ];
    let answers: Vec<_> = client
        .console
        .auto_complete_stream(with_auth(tokio_stream::iter(requests), &client.bearer))
        .await
        .unwrap()
        .into_inner()
        .collect()
        .await;
    let [first, second] = answers.as_slice() else {
        panic!("two answers, then the end: {answers:?}");
    };
    let (first, second) = (first.as_ref().unwrap(), second.as_ref().unwrap());
    assert!(first.success && first.request_id == 1 && first.response.is_none());
    assert!(second.success && second.request_id == 2);
    #[allow(deprecated)]
    match &second.response {
        Some(CompletionResponse::CompletionItems(items)) => {
            assert!(items.success && items.request_id == 2 && items.items.is_empty());
        }
        other => panic!("completion items, not {other:?}"),
    }

    let cancel_command = client
        .console
        .cancel_command(with_auth(CancelCommandRequest::default(), &client.bearer))
        .await
        .unwrap_err();
    assert_eq!(cancel_command.code(), Code::Unimplemented);
    assert_eq!(
        cancel_command.message(),
        "Method io.deephaven.proto.backplane.script.grpc.ConsoleService/CancelCommand is \
         unimplemented"
    );
    let cancel_completion = client
        .console
        .cancel_auto_complete(with_auth(
            CancelAutoCompleteRequest::default(),
            &client.bearer,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        cancel_completion.message(),
        "Method io.deephaven.proto.backplane.script.grpc.ConsoleService/CancelAutoComplete is \
         unimplemented"
    );
}

#[tokio::test]
async fn heap_info_is_the_memory_of_the_process() {
    let mut client = Client::start().await;
    let heap = client
        .console
        .get_heap_info(with_auth(GetHeapInfoRequest {}, &client.bearer))
        .await
        .unwrap()
        .into_inner();
    assert!(heap.total_memory > 0 && heap.total_memory <= heap.max_memory);
    assert!((0..=heap.total_memory).contains(&heap.free_memory));
    // the figures are those of `memory`, which has the tests of each
    let now = super::memory::process_memory().unwrap();
    assert_eq!(heap.max_memory, now.max);
}

/// The records a stream has now: `(level, message)`, or the status that ended it.
fn waiting(updates: &mut LogUpdates) -> Vec<Result<(String, String), Code>> {
    let mut records = Vec::new();
    while let Ok(next) = updates.try_recv() {
        records.push(match next {
            Ok(record) => Ok((record.log_level, record.message)),
            Err(status) => Err(status.code()),
        });
    }
    records
}

#[test]
fn the_servers_log_is_recorded_under_its_level_as_thread_logger_message() {
    let buffer = Arc::new(LogBuffer::new());
    let subscriber = tracing_subscriber::registry().with(super::logs::layer(buffer.clone()));
    let logging = std::thread::Builder::new().name("worker-1".to_string());
    let logged = logging.spawn(move || {
        tracing::subscriber::with_default(subscriber, || {
            let count = 3;
            tracing::info!("Listening on {}", "127.0.0.1:10000");
            tracing::warn!(count, name = "t", "slow cycle");
            tracing::error!(target: "oxhaven_server::console::python::interrupt", "failed");
            tracing::debug!(target: "engine", "detail");
            tracing::trace!(target: "engine", "more detail");
        });
    });
    logged.unwrap().join().unwrap();

    let mut all = buffer.subscribe(LogSubscriptionRequest::default());
    let records: Vec<(String, String)> = waiting(&mut all).into_iter().flatten().collect();
    // the thread in 20 characters, the module path in 25 with its leading segments cut
    let expected = [
        (
            "INFO",
            "worker-1             | o::console::tests         | Listening on 127.0.0.1:10000",
        ),
        (
            "WARN",
            "worker-1             | o::console::tests         | slow cycle count=3 name=t",
        ),
        (
            "ERROR",
            "worker-1             | o::c::python::interrupt   | failed",
        ),
        (
            "DEBUG",
            "worker-1             | engine                    | detail",
        ),
        (
            "TRACE",
            "worker-1             | engine                    | more detail",
        ),
    ];
    let expected: Vec<(String, String)> = expected
        .iter()
        .map(|(level, text)| (level.to_string(), text.to_string()))
        .collect();
    assert_eq!(records, expected);

    // a longer name keeps its end, as Java's "r-Scheduler-Serial-1"
    let subscriber = tracing_subscriber::registry().with(super::logs::layer(buffer.clone()));
    let logging =
        std::thread::Builder::new().name("DeephavenApiServer-Scheduler-Serial-1".to_string());
    let logged = logging.spawn(move || {
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "server::integrations::PythonDeephavenSession", "long");
        });
    });
    logged.unwrap().join().unwrap();
    assert_eq!(
        waiting(&mut all),
        [Ok((
            "INFO".to_string(),
            "r-Scheduler-Serial-1 | i::PythonDeephavenSession | long".to_string()
        ))]
    );

    // a client names the levels it wants, as for what scripts print
    let mut errors = buffer.subscribe(LogSubscriptionRequest {
        levels: vec!["ERROR".to_string()],
        ..Default::default()
    });
    assert_eq!(waiting(&mut errors).len(), 1);
}

#[test]
fn a_client_that_falls_behind_is_ended_and_the_others_are_not() {
    // four records may wait for a client: twice the history
    let buffer = LogBuffer::with_sizes(2, 3);
    buffer.record(STDOUT, "kept 1\n");
    buffer.record(STDOUT, "kept 2\n");
    let mut slow = buffer.subscribe(LogSubscriptionRequest::default());
    let mut reading = buffer.subscribe(LogSubscriptionRequest::default());
    let mut errors_only = buffer.subscribe(LogSubscriptionRequest {
        levels: vec![STDERR.to_string()],
        ..Default::default()
    });
    assert_eq!(buffer.subscriber_count(), 3);

    for number in 3..=4 {
        buffer.record(STDOUT, format!("line {number}\n"));
        assert_eq!(waiting(&mut reading).len(), if number == 3 { 3 } else { 1 });
    }
    // the fifth record finds four waiting for the client that does not read
    buffer.record(STDOUT, "line 5\n");
    assert_eq!(buffer.subscriber_count(), 2);
    assert_eq!(waiting(&mut reading).len(), 1);

    // Java sends the status and not what was waiting
    let status = slow.try_recv().unwrap().unwrap_err();
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert_eq!(
        status.message(),
        "Too slow: the client or network may be too slow to keep up with the logging rates, \
         or there may be logging bursts that exceed the available buffer size. The buffer \
         size can be configured through the server property \
         'deephaven.console.subscribeToLogs.bufferSize'."
    );
    assert!(slow.try_recv().is_err());

    // records a client did not ask for do not count against it
    assert_eq!(waiting(&mut errors_only), []);
    buffer.record(STDERR, "an error\n");
    assert_eq!(waiting(&mut errors_only).len(), 1);
}

#[tokio::test]
async fn a_client_that_does_not_read_its_log_stream_is_refused_when_it_does() {
    let small = |state: &mut ServerState| {
        state.logs = Arc::new(LogBuffer::with_sizes(4, 8));
        with_word_console(state);
    };
    let server = TestServer::start_with(small).await;
    let bearer = server.login().await;
    let mut console = ConsoleServiceClient::new(server.channel.clone());
    let mut stream = console
        .subscribe_to_logs(with_auth(LogSubscriptionRequest::default(), &bearer))
        .await
        .unwrap()
        .into_inner();

    // More than the connection lets the server send to a client that reads nothing, so
    // that the records wait in the server.
    let line = "x".repeat(1024);
    for _ in 0..4096 {
        server.state.logs.record(STDOUT, line.as_str());
        if server.state.logs.subscriber_count() == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        server.state.logs.subscriber_count(),
        0,
        "the client was not ended"
    );

    let mut received = 0;
    let status = loop {
        match stream.next().await.expect("a status ends the stream") {
            Ok(record) => {
                assert_eq!(record.message, line);
                received += 1;
            }
            Err(status) => break status,
        }
    };
    assert!(received > 0 && received < 4096, "{received}");
    assert_eq!(status.code(), Code::ResourceExhausted);
    assert!(status.message().starts_with("Too slow: "), "{status}");
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn the_log_stream_ends_with_its_session() {
    let mut client = Client::start().await;
    let mut stream = client
        .console
        .subscribe_to_logs(with_auth(LogSubscriptionRequest::default(), &client.bearer))
        .await
        .unwrap()
        .into_inner();
    client.execute("print one").await;
    assert_eq!(stream.next().await.unwrap().unwrap().message, "one\n");
    assert_eq!(client.server.state.logs.subscriber_count(), 1);

    // a stream the client ends is forgotten, and its session keeps no callback for it
    let other = client
        .console
        .subscribe_to_logs(with_auth(LogSubscriptionRequest::default(), &client.bearer))
        .await
        .unwrap();
    drop(other);

    SessionServiceClient::new(client.server.channel.clone())
        .close_session(with_auth(HandshakeRequest::default(), &client.bearer))
        .await
        .unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(5), stream.next());
    let status = ended.await.expect("the stream ends").unwrap().unwrap_err();
    assert_eq!(status.code(), Code::Cancelled);
    assert_eq!(status.message(), "Session closed");
    assert!(stream.next().await.is_none());
    client.server.state.logs.record(STDOUT, "after\n");
    assert_eq!(client.server.state.logs.subscriber_count(), 0);
}

impl Client {
    /// Sends `code` and gives up on the answer after `after`, which cancels the call as a
    /// client does that stops waiting.
    fn execute_and_cancel(
        &self,
        code: &str,
        after: Duration,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let request = ExecuteCommandRequest {
            console_id: Some(export_id_to_ticket(1)),
            code: code.to_string(),
            ..Default::default()
        };
        let request = with_auth(request, &self.bearer);
        let mut console = self.console.clone();
        async move {
            let answer = tokio::time::timeout(after, console.execute_command(request)).await;
            assert!(answer.is_err(), "the command was answered: {answer:?}");
        }
    }

    /// What the commands have printed, once `count` lines are there.
    async fn printed(&self, count: usize) -> Vec<String> {
        for _ in 0..500 {
            let mut updates = self
                .server
                .state
                .logs
                .subscribe(LogSubscriptionRequest::default());
            let lines: Vec<String> = waiting(&mut updates)
                .into_iter()
                .map(|record| record.unwrap().1)
                .collect();
            if lines.len() >= count {
                return lines;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fewer than {count} lines were printed within five seconds");
    }
}

#[tokio::test]
async fn a_command_whose_call_the_client_cancels_is_stopped() {
    let mut client = Client::start().await;
    client
        .execute_and_cancel(
            "make before 1\nwait\nmake after 1",
            Duration::from_millis(300),
        )
        .await;
    assert_eq!(client.printed(1).await, ["stopped\n"]);

    // the session's queue is free again, and what the command did before it was stopped
    // is there
    let next = client.execute("print next").await;
    assert_eq!(next.error_message, "");
    assert!(client.server.state.scope.get("before").is_some());
    assert!(client.server.state.scope.get("after").is_none());
}

#[tokio::test]
async fn a_cancelled_command_that_has_not_started_does_not_run() {
    let mut client = Client::start().await;
    let running = tokio::spawn(client.execute_and_cancel("wait", Duration::from_millis(600)));
    tokio::time::sleep(Duration::from_millis(100)).await;
    // waits behind the first on the session's queue, and is given up before the first
    client
        .execute_and_cancel("make queued 1\nprint queued", Duration::from_millis(200))
        .await;
    running.await.unwrap();

    let next = client.execute("print next").await;
    assert_eq!(next.error_message, "");
    assert_eq!(client.printed(2).await, ["stopped\n", "next\n"]);
    assert!(client.server.state.scope.get("queued").is_none());
}

#[tokio::test]
async fn a_running_command_is_stopped_when_its_session_ends() {
    let client = Client::start().await;
    let mut console = client.console.clone();
    let request = ExecuteCommandRequest {
        console_id: Some(export_id_to_ticket(1)),
        code: "wait".to_string(),
        ..Default::default()
    };
    let call = tokio::spawn({
        let request = with_auth(request, &client.bearer);
        async move { console.execute_command(request).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    SessionServiceClient::new(client.server.channel.clone())
        .close_session(with_auth(HandshakeRequest::default(), &client.bearer))
        .await
        .unwrap();
    assert_eq!(client.printed(1).await, ["stopped\n"]);
    // and the call ends, with the answer of the stopped command or the end of its export
    let answer = tokio::time::timeout(Duration::from_secs(5), call);
    answer.await.expect("the call ends").unwrap().ok();
}

/// A client of a server with the word console, and the session of that console, so that a
/// test can change a variable as a thread of a real session would: outside any command.
async fn client_and_session() -> (Client, Arc<WordSession>) {
    let session = Arc::new(Mutex::new(None));
    let server = TestServer::start_with(|state| {
        *session.lock().unwrap() = Some(word_console(state));
    })
    .await;
    let bearer = server.login().await;
    let console = ConsoleServiceClient::new(server.channel.clone());
    let client = Client {
        server,
        bearer,
        console,
    };
    let session = session.lock().unwrap().take().unwrap();
    (client, session)
}

async fn next_fields(fields: &mut tonic::Streaming<FieldsChangeUpdate>) -> Vec<String> {
    let message = tokio::time::timeout(Duration::from_secs(5), fields.next()).await;
    described(
        &message
            .expect("a message within five seconds")
            .unwrap()
            .unwrap(),
    )
}

#[tokio::test]
async fn a_variable_that_is_no_table_resolves_as_an_object_and_is_not_listed() {
    let mut client = Client::start().await;
    let noted = client.execute("make t 2\nnote n hello").await;
    assert_eq!(
        changes_of(&noted),
        ["created t s/t Table [query scope variable] [scope] []"]
    );

    // Java: the variable resolves, and is not a table
    let not_a_table = client.fetch(b"s/n", 3).await.unwrap_err();
    assert_eq!(not_a_table.code(), Code::InvalidArgument);
    assert!(not_a_table.message().starts_with("Details Logged w/ID '"));
    let missing = client.fetch(b"s/nothing", 4).await.unwrap_err();
    assert_eq!(missing.code(), Code::NotFound);
    assert_eq!(
        missing.message(),
        "Could not resolve 'sourceId: variable 'nothing' not found"
    );

    // as an object it can be exported, and published under another name
    let mut sessions = SessionServiceClient::new(client.server.channel.clone());
    sessions
        .export_from_ticket(with_auth(
            ExportRequest {
                source_id: Some(Ticket {
                    ticket: b"s/n".to_vec(),
                }),
                result_id: Some(export_id_to_ticket(5)),
            },
            &client.bearer,
        ))
        .await
        .unwrap();
    sessions
        .publish_from_ticket(with_auth(
            PublishRequest {
                source_id: Some(export_id_to_ticket(5)),
                result_id: Some(Ticket {
                    ticket: b"s/m".to_vec(),
                }),
            },
            &client.bearer,
        ))
        .await
        .unwrap();
    let scope = &client.server.state.scope;
    let published = scope.get("m").unwrap();
    assert_eq!(published.downcast_ref::<String>().unwrap(), "hello");
    assert_eq!(scope.names(), ["m", "n", "t"]);

    // a table that becomes something else is removed from the list
    let replaced = client.execute("note t now").await;
    assert_eq!(
        changes_of(&replaced),
        ["removed t s/t Table [query scope variable] [scope] []"]
    );
}

#[tokio::test]
async fn a_table_published_to_a_scope_ticket_is_a_variable_of_the_session() {
    let (mut client, session) = client_and_session().await;
    let mut fields = ApplicationServiceClient::new(client.server.channel.clone())
        .list_fields(with_auth(ListFieldsRequest {}, &client.bearer))
        .await
        .unwrap()
        .into_inner();
    assert!(next_fields(&mut fields).await.is_empty());

    client
        .console
        .start_console(with_auth(
            StartConsoleRequest {
                result_id: Some(export_id_to_ticket(1)),
                session_type: "words".to_string(),
            },
            &client.bearer,
        ))
        .await
        .unwrap();
    client.execute("make t 4").await;
    assert_eq!(
        next_fields(&mut fields).await,
        ["created t s/t Table [query scope variable] [scope] []"]
    );
    client.fetch(b"s/t", 2).await.unwrap();
    SessionServiceClient::new(client.server.channel.clone())
        .publish_from_ticket(with_auth(
            PublishRequest {
                source_id: Some(export_id_to_ticket(2)),
                result_id: Some(Ticket {
                    ticket: b"s/published".to_vec(),
                }),
            },
            &client.bearer,
        ))
        .await
        .unwrap();

    // the session holds it, the listener is told without a command, and it resolves
    let held = session.get("published").unwrap();
    let t = session.get("t").unwrap();
    assert!(Arc::ptr_eq(held.as_table().unwrap(), t.as_table().unwrap()));
    assert_eq!(
        next_fields(&mut fields).await,
        ["created published s/published Table [query scope variable] [scope] []"]
    );
    let fetched = client.fetch(b"s/published", 3).await.unwrap();
    assert_eq!(fetched.size, 4);
}

#[tokio::test]
async fn a_variable_changed_outside_a_command_is_seen_when_the_scope_is_observed() {
    let (mut client, session) = client_and_session().await;
    let console = client.server.state.console.clone().unwrap();
    let mut fields = ApplicationServiceClient::new(client.server.channel.clone())
        .list_fields(with_auth(ListFieldsRequest {}, &client.bearer))
        .await
        .unwrap()
        .into_inner();
    assert!(next_fields(&mut fields).await.is_empty());

    // nobody looks: the variable resolves, and no listener is told
    session.set("threaded", ExportValue::Table(empty_table(7)));
    assert_eq!(client.fetch(b"s/threaded", 2).await.unwrap().size, 7);
    let quiet = tokio::time::timeout(Duration::from_millis(300), fields.next()).await;
    assert!(quiet.is_err(), "told without an observation: {quiet:?}");

    console.observe();
    assert_eq!(
        next_fields(&mut fields).await,
        ["created threaded s/threaded Table [query scope variable] [scope] []"]
    );

    // a command reports what it changed, not what changed before it; the listener is
    // told of both
    session.set("before", ExportValue::Table(empty_table(1)));
    client
        .console
        .start_console(with_auth(
            StartConsoleRequest {
                result_id: Some(export_id_to_ticket(1)),
                session_type: "words".to_string(),
            },
            &client.bearer,
        ))
        .await
        .unwrap();
    let ran = client.execute("make during 1").await;
    assert_eq!(
        changes_of(&ran),
        ["created during s/during Table [query scope variable] [scope] []"]
    );
    let mut told = next_fields(&mut fields).await;
    if told.len() == 1 {
        told.extend(next_fields(&mut fields).await);
    }
    assert_eq!(
        told,
        [
            "created before s/before Table [query scope variable] [scope] []",
            "created during s/during Table [query scope variable] [scope] []"
        ]
    );

    // the watch looks by itself
    console.watch(Duration::from_millis(20));
    session.set("watched", ExportValue::Table(empty_table(1)));
    assert_eq!(
        next_fields(&mut fields).await,
        ["created watched s/watched Table [query scope variable] [scope] []"]
    );
}

#[tokio::test]
async fn a_scope_variable_that_holds_a_table_is_a_flight() {
    let mut client = Client::start().await;
    client.execute("make t 5\nnote n hello").await;
    let mut flights = FlightServiceClient::new(client.server.channel.clone());
    let descriptor = |path: &[&str]| FlightDescriptor {
        r#type: DescriptorType::Path as i32,
        path: path.iter().map(|part| part.to_string()).collect(),
        ..Default::default()
    };
    let info = flights
        .get_flight_info(with_auth(descriptor(&["scope", "t"]), &client.bearer))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.total_records, 5);
    let ticket = info.endpoint[0].ticket.as_ref().unwrap();
    assert_eq!(ticket.ticket.as_ref(), b"s/t");

    for (path, message) in [
        (
            &["scope", "n"][..],
            "Could not resolve 'request: variable 'n' not found",
        ),
        (
            &["scope", "nothing"][..],
            "Could not resolve 'request: variable 'nothing' not found",
        ),
    ] {
        let status = flights
            .get_flight_info(with_auth(descriptor(path), &client.bearer))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::NotFound, "{path:?}");
        assert_eq!(status.message(), message);
    }
    let too_long = flights
        .get_flight_info(with_auth(descriptor(&["scope", "t", "u"]), &client.bearer))
        .await
        .unwrap_err();
    assert_eq!(too_long.code(), Code::FailedPrecondition);
    assert_eq!(
        too_long.message(),
        "Could not resolve descriptor 'request': unexpected path length (found: /scope/t/u, \
         expected: 2)"
    );
}

#[test]
fn a_change_counts_only_for_values_clients_can_open() {
    let table = |rows| ExportValue::Table(empty_table(rows));
    let (a, b, text) = (table(1), table(2), ExportValue::object("text".to_string()));
    let noted = |from: Option<&ExportValue>, to: Option<&ExportValue>| {
        let mut changes = Changes::default();
        changes.note("v", from, to);
        let kinds = [
            (!changes.created.is_empty()).then_some("created"),
            (!changes.updated.is_empty()).then_some("updated"),
            (!changes.removed.is_empty()).then_some("removed"),
        ];
        kinds.into_iter().flatten().collect::<Vec<_>>().join(",")
    };
    assert_eq!(noted(None, Some(&a)), "created");
    assert_eq!(noted(Some(&a), Some(&b)), "updated");
    assert_eq!(noted(Some(&a), Some(&a.clone())), "");
    assert_eq!(noted(Some(&a), None), "removed");
    assert_eq!(noted(Some(&a), Some(&text)), "removed");
    assert_eq!(noted(Some(&text), Some(&a)), "created");
    assert_eq!(noted(Some(&text), Some(&text)), "");
    assert_eq!(noted(None, Some(&text)), "");
    assert_eq!(noted(None, None), "");
}
