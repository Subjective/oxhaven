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

use oxhaven_contracts::table::Table;
use oxhaven_engine::table::empty_table;
use oxhaven_proto::grpc::application_service_client::ApplicationServiceClient;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{
    ExportedTableCreationResponse, FetchTableRequest, FieldsChangeUpdate, ListFieldsRequest,
    TableReference, Ticket,
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

use super::logs::{LogBuffer, STDERR, STDOUT};
use super::{Changes, Console, ScriptSession, Variable};
use crate::session::{export_id_to_ticket, ExportValue};
use crate::test_support::{with_auth, TestServer};
use crate::ServerState;

/// A script session whose language is one command per line: `make <name> <rows>` sets a
/// variable to a new table, `drop <name>` removes one, `print <text>` writes a line to
/// standard output, and anything else is an error that ends the code.
struct WordSession {
    variables: Mutex<BTreeMap<String, Arc<dyn Table>>>,
    logs: Arc<LogBuffer>,
}

fn variable(name: &str, table: Option<&Arc<dyn Table>>) -> Variable {
    Variable {
        name: name.to_string(),
        type_name: "Table".to_string(),
        value: table.cloned().map(ExportValue::Table),
    }
}

impl WordSession {
    fn set(&self, name: &str, table: Arc<dyn Table>, changes: &mut Changes) {
        let created = variable(name, Some(&table));
        match self
            .variables
            .lock()
            .unwrap()
            .insert(name.to_string(), table)
        {
            Some(_) => changes.updated.push(created),
            None => changes.created.push(created),
        }
    }
}

impl ScriptSession for WordSession {
    fn script_type(&self) -> &str {
        "Words"
    }

    fn evaluate(&self, code: &str) -> Changes {
        let mut changes = Changes::default();
        for line in code.lines() {
            let words: Vec<&str> = line.split_whitespace().collect();
            match words.as_slice() {
                ["make", name, rows] => {
                    self.set(name, empty_table(rows.parse().unwrap()), &mut changes)
                }
                ["drop", name] => {
                    if self.variables.lock().unwrap().remove(*name).is_some() {
                        changes.removed.push(variable(name, None));
                    }
                }
                ["print", text @ ..] => self.logs.record(STDOUT, format!("{}\n", text.join(" "))),
                _ => {
                    changes.error = Some(format!("not a command: {line}"));
                    break;
                }
            }
        }
        changes
    }

    fn put_table(&self, name: &str, table: Arc<dyn Table>) -> Result<Changes, String> {
        let mut changes = Changes::default();
        self.set(name, table, &mut changes);
        Ok(changes)
    }
}

fn with_word_console(state: &mut ServerState) {
    let session = WordSession {
        variables: Mutex::default(),
        logs: state.logs.clone(),
    };
    state.console = Some(Console::new(
        Arc::new(session),
        state.scope.clone(),
        state.applications.clone(),
    ));
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
    assert_eq!(heap.free_memory, 0);
}
