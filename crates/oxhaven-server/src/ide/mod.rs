//! Replays the calls of the web IDE path that were recorded from the Java reference
//! (`fixtures/java.txt`, written by `fixtures/capture.py`) against this server and compares
//! the answers.
//!
//! A recorded section is one session: its cases are sent in the recorded order to a new
//! server that offers the demo application, with the recorded request bytes. Every answer
//! has to be the recorded one, except for the cases of [`DIFFERENCES`], which have to be
//! what that table says. A status is compared by code and message, a table response by its
//! fields and its decoded schema, anything else by its bytes.

// RefreshSessionToken and CloseSession take a deprecated message
#![allow(deprecated)]

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_engine::table::TimeSource;
use oxhaven_proto::grpc::application_service_client::ApplicationServiceClient;
use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::storage_service_client::StorageServiceClient;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{
    ApplyPreviewColumnsRequest, BatchTableRequest, ExportRequest, ExportedTableCreationResponse,
    FetchTableRequest, HandshakeRequest, ListFieldsRequest, ListItemsRequest, PublishRequest,
};
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{
    AutoCompleteRequest, BindTableToVariableRequest, CancelAutoCompleteRequest,
    CancelCommandRequest, ExecuteCommandRequest, GetConsoleTypesRequest, GetHeapInfoRequest,
    LogSubscriptionRequest, StartConsoleRequest,
};
use prost::Message;
use tokio_stream::StreamExt;
use tonic::{Request, Status, Streaming};

use crate::app::demo;
use crate::table::support::OpContext;
use crate::test_support::{with_auth, TestServer};

const RECORDING: &str = include_str!("fixtures/java.txt");

/// How long a stream is watched for a further message before it counts as staying open.
const STAYS_OPEN: Duration = Duration::from_millis(300);

/// One recorded call and what Java answered.
#[derive(Clone, Debug)]
pub(crate) struct Case {
    pub(crate) name: String,
    pub(crate) call: String,
    pub(crate) authenticated: bool,
    pub(crate) request: Vec<u8>,
    /// `OK`, `OPEN` for a stream that stayed open, or `<CODE>: <message>`.
    pub(crate) status: String,
    pub(crate) messages: Vec<Vec<u8>>,
}

fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

/// Undoes the escaping of `capture.py#status_line`.
fn unescape(text: &str) -> String {
    let mut plain = String::new();
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => match characters.next() {
                Some('n') => plain.push('\n'),
                Some(other) => plain.push(other),
                None => plain.push('\\'),
            },
            other => plain.push(other),
        }
    }
    plain
}

/// The cases of one section of the recording, in order.
pub(crate) fn recorded(section: &str) -> Vec<Case> {
    let mut cases: Vec<Case> = Vec::new();
    let mut inside = false;
    for line in RECORDING.lines() {
        if let Some(name) = line.strip_prefix("## ") {
            inside = name == section;
        } else if !inside || line.starts_with('#') || line.is_empty() {
        } else if let Some(name) = line.strip_prefix("== ") {
            cases.push(Case {
                name: name.to_string(),
                call: String::new(),
                authenticated: true,
                request: Vec::new(),
                status: String::new(),
                messages: Vec::new(),
            });
        } else {
            let case = cases.last_mut().expect("a line before the first case");
            let (key, value) = line.split_once(':').expect("key: value");
            let value = value.strip_prefix(' ').unwrap_or(value);
            match key {
                "call" => case.call = value.to_string(),
                "auth" => case.authenticated = value != "none",
                "request" => case.request = unhex(value),
                "status" => case.status = unescape(value),
                "message" => case.messages.push(unhex(value)),
                other => panic!("unknown line {other} in case {}", case.name),
            }
        }
    }
    assert!(!cases.is_empty(), "no section {section} in the recording");
    cases
}

/// The recorded case `name` of `section`.
pub(crate) fn recorded_case(section: &str, name: &str) -> Case {
    recorded(section)
        .into_iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("no case {name} in section {section}"))
}

/// What a case has to answer on this server where that is not what Java answered.
struct Difference {
    section: &'static str,
    case: &'static str,
    /// The status line, or its beginning.
    status: &'static str,
    /// The section and case whose recorded messages this server answers with; no messages
    /// if absent.
    messages_of: Option<(&'static str, &'static str)>,
    why: &'static str,
}

const fn differs(
    section: &'static str,
    case: &'static str,
    status: &'static str,
    why: &'static str,
) -> Difference {
    Difference {
        section,
        case,
        status,
        messages_of: None,
        why,
    }
}

const NO_CONSOLE: &str = "this server has no script session; every console call but \
                          GetConsoleTypes is UNIMPLEMENTED";
const NO_DISPLAY_WRAPPER: &str = "unported: ColumnPreviewManager#makePreviewTable needs the \
                                  DisplayWrapper column type";
const GLOB_SUBSET: &str = "unported: character classes, groups and syntax errors of \
                           sun.nio.fs.Globs#toUnixRegexPattern";

const DIFFERENCES: &[Difference] = &[
    differs(
        "console_disabled",
        "preview_static_int_not_listed",
        "UNIMPLEMENTED: not ported: io.deephaven.engine.table.impl.preview.ColumnPreviewManager#makePreviewTable",
        NO_DISPLAY_WRAPPER,
    ),
    differs(
        "console_disabled",
        "preview_static_one_other_type",
        "UNIMPLEMENTED: not ported: io.deephaven.engine.table.impl.preview.ColumnPreviewManager#makePreviewTable",
        NO_DISPLAY_WRAPPER,
    ),
    differs(
        "console_disabled",
        "storage_outside_and_back",
        "INVALID_ARGUMENT: Invalid path: ../storage/layouts",
        "the storage of this server has no directory name outside itself to come back through",
    ),
    differs(
        "console_disabled",
        "storage_bad_glob",
        "UNIMPLEMENTED: StorageService/ListItems: the filter_glob \"[\" is not supported",
        GLOB_SUBSET,
    ),
    differs(
        "console_disabled",
        "storage_class_glob",
        "UNIMPLEMENTED: StorageService/ListItems: the filter_glob \"[ln]*\" is not supported",
        GLOB_SUBSET,
    ),
    differs(
        "console_disabled",
        "storage_group_glob",
        "UNIMPLEMENTED: StorageService/ListItems: the filter_glob \"{layouts,x}\" is not \
         supported",
        GLOB_SUBSET,
    ),
    differs(
        "console_disabled",
        "console_start",
        "UNIMPLEMENTED: ConsoleService/StartConsole is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_heap",
        "UNIMPLEMENTED: ConsoleService/GetHeapInfo is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_logs",
        "UNIMPLEMENTED: ConsoleService/SubscribeToLogs is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_execute",
        "UNIMPLEMENTED: ConsoleService/ExecuteCommand is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_cancel",
        "UNIMPLEMENTED: ConsoleService/CancelCommand is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_bind",
        "UNIMPLEMENTED: ConsoleService/BindTableToVariable is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_cancel_autocomplete",
        "UNIMPLEMENTED: ConsoleService/CancelAutoComplete is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_open_autocomplete",
        "UNIMPLEMENTED: ConsoleService/OpenAutoCompleteStream is not implemented",
        NO_CONSOLE,
    ),
    differs(
        "console_disabled",
        "console_next_autocomplete",
        "UNIMPLEMENTED: ConsoleService/NextAutoCompleteStream is not implemented",
        NO_CONSOLE,
    ),
    Difference {
        section: "with_console",
        case: "console_types",
        status: "OK",
        messages_of: Some(("console_disabled", "console_types")),
        why: NO_CONSOLE,
    },
    Difference {
        section: "with_console",
        case: "console_types_unauthenticated",
        status: "OK",
        messages_of: Some(("console_disabled", "console_types")),
        why: NO_CONSOLE,
    },
    Difference {
        section: "with_console",
        case: "list_fields_after_publish",
        status: "OPEN",
        messages_of: Some(("console_disabled", "list_fields")),
        why: "scope variables are not listed, as on a Java server whose console is disabled",
    },
];

/// The clock of the demo application's ticking table, moved by the test.
pub(crate) struct ManualClock(AtomicI64);

impl ManualClock {
    /// The time the clock starts at, in nanoseconds since the epoch: a whole second.
    pub(crate) const START: i64 = 1_700_000_000_000_000_000;

    pub(crate) fn advance(&self, duration: Duration) {
        self.0
            .fetch_add(duration.as_nanos() as i64, Ordering::SeqCst);
    }
}

impl TimeSource for ManualClock {
    fn now_nanos(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Runs one cycle of the server's update graph.
pub(crate) async fn run_cycle(server: &TestServer) {
    let graph = server.state.graph.clone();
    tokio::task::spawn_blocking(move || graph.run_cycle())
        .await
        .unwrap();
}

/// A server that offers the demo application, with `ide_ticking` at its full three rows,
/// as the Java server was when it was recorded, and the clock its ticking table reads. No
/// cycle runs unless the test runs one; the clock is one second past the last cycle.
pub(crate) async fn demo_server() -> (TestServer, Arc<ManualClock>) {
    let server = TestServer::start().await;
    let clock = Arc::new(ManualClock(AtomicI64::new(ManualClock::START)));
    let context = OpContext {
        clock: clock.clone(),
        ..server.state.op_context()
    };
    demo::install(&server.state.applications, &context).unwrap();
    // the first cycle adds the row of the current half second, the second the two after it
    for _ in 0..2 {
        run_cycle(&server).await;
        clock.advance(Duration::from_secs(1));
    }
    (server, clock)
}

/// What this server answered to one case, in the terms of the recording.
#[derive(Debug)]
pub(crate) struct Outcome {
    pub(crate) status: String,
    pub(crate) messages: Vec<Vec<u8>>,
}

/// `FAILED_PRECONDITION` for `Code::FailedPrecondition`.
fn code_name(code: tonic::Code) -> String {
    let mut name = String::new();
    for character in format!("{code:?}").chars() {
        if character.is_uppercase() && !name.is_empty() {
            name.push('_');
        }
        name.extend(character.to_uppercase());
    }
    name
}

fn status_line(status: &Status) -> String {
    format!("{}: {}", code_name(status.code()), status.message())
}

fn unary<T: Message>(result: Result<tonic::Response<T>, Status>) -> Outcome {
    match result {
        Ok(response) => Outcome {
            status: "OK".to_string(),
            messages: vec![response.into_inner().encode_to_vec()],
        },
        Err(status) => Outcome {
            status: status_line(&status),
            messages: Vec::new(),
        },
    }
}

/// Reads a server stream as `capture.py#stream` does: the first message, then `after_first`
/// and the rest of the stream if there is one, and otherwise a look at whether the stream
/// stays open.
async fn stream<T: Message>(
    result: Result<tonic::Response<Streaming<T>>, Status>,
    after_first: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
) -> Outcome {
    let mut responses = match result {
        Ok(response) => response.into_inner(),
        Err(status) => {
            return Outcome {
                status: status_line(&status),
                messages: Vec::new(),
            }
        }
    };
    let mut messages = Vec::new();
    let mut after_first = after_first;
    let to_the_end = after_first.is_some();
    loop {
        let next = if messages.is_empty() || to_the_end {
            responses.next().await
        } else {
            match tokio::time::timeout(STAYS_OPEN, responses.next()).await {
                Ok(next) => next,
                Err(_) => {
                    return Outcome {
                        status: "OPEN".to_string(),
                        messages,
                    }
                }
            }
        };
        match next {
            Some(Ok(message)) => {
                messages.push(message.encode_to_vec());
                if let Some(action) = after_first.take() {
                    action.await;
                }
            }
            Some(Err(status)) => {
                return Outcome {
                    status: status_line(&status),
                    messages,
                }
            }
            None => {
                return Outcome {
                    status: "OK".to_string(),
                    messages,
                }
            }
        }
    }
}

/// Sends the request of `case` to `server`.
async fn send(server: &TestServer, bearer: &str, case: &Case) -> Outcome {
    fn request<T: Message + Default>(case: &Case, bearer: &str) -> Request<T> {
        let message = T::decode(case.request.as_slice()).expect("the recorded request");
        if case.authenticated {
            with_auth(message, bearer)
        } else {
            Request::new(message)
        }
    }
    let channel = server.channel.clone();
    let (service, method) = case.call.split_once('/').expect("Service/Method");
    match (service, method) {
        ("TableService", "FetchTable") => unary(
            TableServiceClient::new(channel)
                .fetch_table(request::<FetchTableRequest>(case, bearer))
                .await,
        ),
        ("TableService", "ApplyPreviewColumns") => unary(
            TableServiceClient::new(channel)
                .apply_preview_columns(request::<ApplyPreviewColumnsRequest>(case, bearer))
                .await,
        ),
        ("TableService", "Batch") => {
            let responses = TableServiceClient::new(channel)
                .batch(request::<BatchTableRequest>(case, bearer))
                .await;
            stream(responses, Some(Box::pin(async {}))).await
        }
        ("SessionService", "ExportFromTicket") => unary(
            SessionServiceClient::new(channel)
                .export_from_ticket(request::<ExportRequest>(case, bearer))
                .await,
        ),
        ("SessionService", "PublishFromTicket") => unary(
            SessionServiceClient::new(channel)
                .publish_from_ticket(request::<PublishRequest>(case, bearer))
                .await,
        ),
        ("StorageService", "ListItems") => unary(
            StorageServiceClient::new(channel)
                .list_items(request::<ListItemsRequest>(case, bearer))
                .await,
        ),
        ("ApplicationService", "ListFields") => {
            let responses = ApplicationServiceClient::new(channel.clone())
                .list_fields(request::<ListFieldsRequest>(case, bearer))
                .await;
            let after_first = (case.name == "list_fields_session_closed").then(|| {
                let close = with_auth(HandshakeRequest::default(), bearer);
                let action: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                    Box::pin(async move {
                        SessionServiceClient::new(channel)
                            .close_session(close)
                            .await
                            .unwrap();
                    });
                action
            });
            stream(responses, after_first).await
        }
        ("ConsoleService", method) => {
            let mut console = ConsoleServiceClient::new(channel);
            match method {
                "GetConsoleTypes" => unary(
                    console
                        .get_console_types(request::<GetConsoleTypesRequest>(case, bearer))
                        .await,
                ),
                "StartConsole" => unary(
                    console
                        .start_console(request::<StartConsoleRequest>(case, bearer))
                        .await,
                ),
                "GetHeapInfo" => unary(
                    console
                        .get_heap_info(request::<GetHeapInfoRequest>(case, bearer))
                        .await,
                ),
                "SubscribeToLogs" => {
                    let responses = console
                        .subscribe_to_logs(request::<LogSubscriptionRequest>(case, bearer))
                        .await;
                    stream(responses, None).await
                }
                "ExecuteCommand" => unary(
                    console
                        .execute_command(request::<ExecuteCommandRequest>(case, bearer))
                        .await,
                ),
                "CancelCommand" => unary(
                    console
                        .cancel_command(request::<CancelCommandRequest>(case, bearer))
                        .await,
                ),
                "BindTableToVariable" => unary(
                    console
                        .bind_table_to_variable(request::<BindTableToVariableRequest>(case, bearer))
                        .await,
                ),
                "CancelAutoComplete" => unary(
                    console
                        .cancel_auto_complete(request::<CancelAutoCompleteRequest>(case, bearer))
                        .await,
                ),
                "OpenAutoCompleteStream" => {
                    let responses = console
                        .open_auto_complete_stream(request::<AutoCompleteRequest>(case, bearer))
                        .await;
                    stream(responses, None).await
                }
                "NextAutoCompleteStream" => unary(
                    console
                        .next_auto_complete_stream(request::<AutoCompleteRequest>(case, bearer))
                        .await,
                ),
                other => panic!("no replay of ConsoleService/{other}"),
            }
        }
        other => panic!("no replay of {other:?}"),
    }
}

/// The Arrow schema of a table response, with every field's metadata.
pub(crate) fn schema_of(response: &ExportedTableCreationResponse) -> Schema {
    let reader = StreamReader::try_new(std::io::Cursor::new(response.schema_header.clone()), None)
        .expect("a schema message");
    reader.schema().as_ref().clone()
}

/// A table response as what is compared: everything but the bytes of the schema message,
/// which is compared as a schema. This server's encoding of the same schema is 8 bytes
/// longer than Java's (task 2.2).
fn table_response(bytes: &[u8]) -> (ExportedTableCreationResponse, Schema) {
    let mut response = ExportedTableCreationResponse::decode(bytes).expect("a table response");
    let schema = schema_of(&response);
    response.schema_header = Vec::new();
    (response, schema)
}

/// How `ours` differs from what was `expected`, in words; empty if it does not.
fn compare(call: &str, expected_status: &str, expected: &[Vec<u8>], ours: &Outcome) -> Vec<String> {
    let mut problems = Vec::new();
    // an error id is new every time
    let comparable = |status: &str| match status.split_once("Details Logged w/ID '") {
        Some((before, _)) => format!("{before}Details Logged w/ID '"),
        None => status.to_string(),
    };
    if comparable(expected_status) != comparable(&ours.status) {
        problems.push(format!(
            "status {:?}, expected {expected_status:?}",
            ours.status
        ));
    }
    if expected.len() != ours.messages.len() {
        problems.push(format!(
            "{} messages, expected {}",
            ours.messages.len(),
            expected.len()
        ));
        return problems;
    }
    let is_table = matches!(
        call,
        "TableService/FetchTable" | "TableService/ApplyPreviewColumns" | "TableService/Batch"
    );
    if is_table {
        // the operations of a batch answer in the order they finish
        let sorted = |messages: &[Vec<u8>]| {
            let mut responses: Vec<_> = messages.iter().map(|m| table_response(m)).collect();
            responses.sort_by_key(|(response, _)| {
                response.result_id.as_ref().map(Message::encode_to_vec)
            });
            responses
        };
        for (expected, ours) in sorted(expected).into_iter().zip(sorted(&ours.messages)) {
            if expected != ours {
                problems.push(format!("answered {ours:?}, expected {expected:?}"));
            }
        }
    } else if expected != ours.messages {
        problems.push(format!(
            "answered {:02x?}, expected {expected:02x?}",
            ours.messages
        ));
    }
    problems
}

/// Replays one section and returns what is wrong, one line per case.
async fn replay(section: &'static str) -> Vec<String> {
    let (server, _clock) = demo_server().await;
    let bearer = server.login().await;
    let cases = recorded(section);
    let mut problems = Vec::new();
    let mut replayed = 0;
    for case in &cases {
        if case.call == "FlightService/DoGet" {
            // not a protobuf answer; see `the_static_table_holds_the_rows_java_sends`
            continue;
        }
        let ours = send(&server, &bearer, case).await;
        replayed += 1;
        let difference = DIFFERENCES
            .iter()
            .find(|d| d.section == section && d.case == case.name);
        let found = match difference {
            None => compare(&case.call, &case.status, &case.messages, &ours),
            Some(difference) => {
                let messages = match difference.messages_of {
                    None => Vec::new(),
                    Some((section, case)) => recorded_case(section, case).messages,
                };
                let mut found = Vec::new();
                if !ours.status.starts_with(difference.status) {
                    found.push(format!(
                        "status {:?}, expected one that starts with {:?} ({})",
                        ours.status, difference.status, difference.why
                    ));
                }
                found.extend(compare(&case.call, &ours.status, &messages, &ours));
                // a listed difference that is none any more has to leave the list
                if compare(&case.call, &case.status, &case.messages, &ours).is_empty() {
                    found.push("answers as Java does, but is listed as a difference".to_string());
                }
                found
            }
        };
        problems.extend(
            found
                .into_iter()
                .map(|problem| format!("{}: {problem}", case.name)),
        );
    }
    assert!(replayed > 5, "only {replayed} cases replayed");
    problems
}

#[tokio::test]
async fn calls_answer_what_java_answers_with_its_console_disabled() {
    let problems = replay("console_disabled").await;
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[tokio::test]
async fn calls_answer_what_java_answers_with_a_console_except_for_the_console() {
    let problems = replay("with_console").await;
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn every_listed_difference_names_a_recorded_case() {
    for difference in DIFFERENCES {
        recorded_case(difference.section, difference.case);
        assert!(!difference.why.is_empty());
    }
}

#[test]
fn the_recording_has_the_calls_of_the_ide() {
    let cases = recorded("console_disabled");
    for call in [
        "ApplicationService/ListFields",
        "TableService/FetchTable",
        "TableService/ApplyPreviewColumns",
        "StorageService/ListItems",
        "ConsoleService/GetConsoleTypes",
    ] {
        assert!(cases.iter().any(|case| case.call == call), "{call}");
    }
    assert_eq!(
        code_name(tonic::Code::FailedPrecondition),
        "FAILED_PRECONDITION"
    );
    assert_eq!(code_name(tonic::Code::Ok), "OK");
    assert_eq!(unescape("a\\nb\\\\n"), "a\nb\\n");
}

/// The rows of `ide_static` are the ones Java's application sends through Flight `DoGet`.
#[tokio::test]
async fn the_static_table_holds_the_rows_java_sends() {
    let case = recorded_case("console_disabled", "data_static");
    let reader = StreamReader::try_new(std::io::Cursor::new(case.messages[0].clone()), None)
        .expect("an Arrow stream");
    let schema = reader.schema();
    let batches: Vec<_> = reader.map(|batch| batch.unwrap()).collect();
    let java = arrow::compute::concat_batches(&schema, &batches).unwrap();

    let ours = demo::static_table().unwrap();
    let columns = ours.definition().columns();
    assert_eq!(
        columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
        schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(ours.row_set().size(), java.num_rows() as u64);
    for (index, column) in columns.iter().enumerate() {
        let values: ArrayRef = ours.column(index).chunk(&ours.row_set()).unwrap();
        assert_eq!(&values, java.column(index), "{}", column.name);
    }
}
