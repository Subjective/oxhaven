//! Tests of completion in a Python session, over the completion stream of a client.
//!
//! What `jedi` answers is asserted where the answer is about code of the test itself or
//! does not depend on a docstring of the `deephaven` package: those answers are what Java
//! 42.9 gave to the same messages.
//!
//! `jedi` and `docstring_parser` are packages of the environment the tests run in
//! (`VIRTUAL_ENV`, `docs/CONSOLE.md`). In an environment without them the tests that need
//! them say so and pass, unless `OXHAVEN_REQUIRE_JEDI` is set: then they fail.

use std::time::Duration;

use oxhaven_proto::script::auto_complete_request::Request as CompletionRequest;
use oxhaven_proto::script::auto_complete_response::Response as CompletionResponse;
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{
    AutoCompleteRequest, AutoCompleteResponse, CompletionItem, DocumentRange,
    ExecuteCommandRequest, GetCompletionItemsResponse, GetDiagnosticRequest, GetHoverRequest,
    GetPullDiagnosticResponse, GetSignatureHelpRequest, MarkupContent, StartConsoleRequest,
    TextEdit,
};
use tonic::transport::Channel;
use tonic::Code;

use super::super::tests::completion::{
    at, change, failed, items, labels, message, named, open, Stream, URI,
};
use super::tests::Python;
use crate::session::export_id_to_ticket;
use crate::test_support::with_auth;

/// A client with a Python console, export 1.
struct Session {
    python: Python,
    console: ConsoleServiceClient<Channel>,
    bearer: String,
}

impl Session {
    async fn start() -> Self {
        let python = Python::start().await;
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
        Self {
            python,
            console,
            bearer,
        }
    }

    /// As [`Session::start`]; `None`, with a notice, where `jedi` is not installed.
    async fn with_jedi(test: &str) -> Option<Self> {
        let session = Self::start().await;
        let found = session
            .python
            .console
            .evaluate("import jedi, docstring_parser");
        let Some(missing) = found.error else {
            return Some(session);
        };
        assert!(
            std::env::var_os("OXHAVEN_REQUIRE_JEDI").is_none(),
            "OXHAVEN_REQUIRE_JEDI is set and the packages are not installed: {missing}"
        );
        eprintln!("{test}: NOT RUN, the Python environment has no jedi or no docstring_parser");
        None
    }

    async fn stream(&mut self) -> Stream {
        Stream::of(&mut self.console, &self.bearer).await
    }

    /// A command of the client, which must not fail.
    fn command(&self, code: &str) -> impl std::future::Future<Output = ()> + Send + 'static {
        let request = ExecuteCommandRequest {
            console_id: Some(export_id_to_ticket(1)),
            code: code.to_string(),
            ..Default::default()
        };
        let request = with_auth(request, &self.bearer);
        let mut console = self.console.clone();
        async move {
            let answer = console.execute_command(request).await.unwrap();
            assert_eq!(answer.into_inner().error_message, "");
        }
    }
}

fn items_at(request_id: i32, version: i32, line: i32, character: i32) -> AutoCompleteRequest {
    let mut request = items(request_id, URI, version, character);
    if let Some(CompletionRequest::GetCompletionItems(items)) = &mut request.request {
        items.position = at(line, character);
    }
    request
}

fn signature(request_id: i32, character: i32) -> AutoCompleteRequest {
    let help = GetSignatureHelpRequest {
        context: None,
        text_document: named(URI, 1),
        position: at(0, character),
    };
    message(request_id, CompletionRequest::GetSignatureHelp(help))
}

fn hover(request_id: i32, character: i32) -> AutoCompleteRequest {
    let hover = GetHoverRequest {
        text_document: named(URI, 1),
        position: at(0, character),
    };
    message(request_id, CompletionRequest::GetHover(hover))
}

fn markdown(value: &str) -> Option<MarkupContent> {
    Some(MarkupContent {
        kind: "markdown".to_string(),
        value: value.to_string(),
    })
}

fn found(answer: AutoCompleteResponse) -> Vec<CompletionItem> {
    assert!(answer.success, "{answer:?}");
    match answer.response {
        Some(CompletionResponse::CompletionItems(found)) => found.items,
        other => panic!("completion items, not {other:?}"),
    }
}

const MINE: &str = "from deephaven import empty_table
t = empty_table(3).update(['X = i'])
def mine(a: int, b: str = 'x') -> int:
    '''Adds things.

    Args:
        a (int): first
        b (str): second

    Returns:
        an int
    '''
    return a
";

#[tokio::test]
async fn jedi_answers_from_the_variables_of_the_session() {
    let Some(mut session) =
        Session::with_jedi("jedi_answers_from_the_variables_of_the_session").await
    else {
        return;
    };
    session.command(MINE).await;
    let mut stream = session.stream().await;

    // a name the session imported
    stream.send(open(URI, 1, "emp"));
    stream.send(items(2, URI, 1, 3));
    let answer = stream.answer().await;
    assert_eq!(answer.request_id, 2);
    let mut found_items = found(answer);
    assert_eq!(found_items.len(), 1);
    // the docstring is this server's own text
    let documentation = found_items[0].documentation.take().unwrap();
    assert_eq!(documentation.kind, "plaintext");
    assert!(documentation.value.starts_with("Creates a table"));
    let expected = CompletionItem {
        start: 0,
        length: 11,
        label: "empty_table".to_string(),
        kind: 3,
        detail: "empty_table(size: int)".to_string(),
        text_edit: Some(TextEdit {
            range: Some(DocumentRange {
                start: at(0, 0),
                end: at(0, 3),
            }),
            text: "empty_table".to_string(),
        }),
        sort_text: "00000".to_string(),
        insert_text_format: 2,
        ..Default::default()
    };
    assert_eq!(found_items, [expected]);

    // an attribute of a variable that holds a table: jedi looks at the object
    stream.send(open(URI, 1, "t.he"));
    stream.send(items(3, URI, 1, 4));
    let answer = stream.answer().await;
    assert_eq!(labels(&answer), ["head", "head_by", "head_pct"]);
    let found_items = found(answer);
    assert_eq!(found_items[1].sort_text, "00001");
    assert_eq!((found_items[1].start, found_items[1].length), (2, 7));

    // a function the session defined: its signature, its docstring as Java renders it
    stream.send(open(URI, 1, "mine(1, "));
    stream.send(signature(4, 8));
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (4, true));
    let Some(CompletionResponse::Signatures(help)) = answer.response else {
        panic!("signatures, not {:?}", answer.response);
    };
    assert_eq!(help.signatures.len(), 1);
    let mine = &help.signatures[0];
    assert_eq!(mine.label, "mine(a: int, b: str='x')");
    assert_eq!(
        mine.documentation,
        markdown(
            "Adds things.\n\n#### **Parameters**\n\n> **a**: *int*  \n> first\n\n\
             > **b**: *str* ⋅ (default: *'x'*)  \n> second\n\n#### **Returns**\n\n> an int"
        )
    );
    let parameters: Vec<_> = mine
        .parameters
        .iter()
        .map(|parameter| (parameter.label.as_str(), parameter.documentation.clone()))
        .collect();
    assert_eq!(
        parameters,
        [
            ("a: int", markdown("##### **a** : *int*\n\nfirst\n\n---")),
            (
                "b: str='x'",
                markdown("##### **b** : *str*\n\nsecond\n\n---")
            ),
        ]
    );
    assert_eq!(mine.active_parameter, Some(1));

    stream.send(open(URI, 1, "mine"));
    stream.send(hover(5, 2));
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (5, true));
    let Some(CompletionResponse::Hover(hover)) = answer.response else {
        panic!("a hover, not {:?}", answer.response);
    };
    assert_eq!(hover.range, None);
    assert_eq!(
        hover.contents,
        markdown(
            "```python\ndef mine(a: int, b: str='x')\n```\n---\n```plaintext\nAdds things.\n\n\
             Args:\n    a (int): first\n    b (str): second\n\nReturns:\n    an int\n```"
        )
    );

    // Java has no diagnostics either
    let diagnostic = GetDiagnosticRequest {
        text_document: named(URI, 1),
        ..Default::default()
    };
    stream.send(message(6, CompletionRequest::GetDiagnostic(diagnostic)));
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (6, true));
    assert_eq!(
        answer.response,
        Some(CompletionResponse::Diagnostic(
            GetPullDiagnosticResponse::default()
        ))
    );
}

/// A client counts in UTF-16 code units, and so does the document `jedi` is given.
#[tokio::test]
async fn columns_after_a_character_of_two_code_units_are_the_client_s() {
    let name = "columns_after_a_character_of_two_code_units_are_the_client_s";
    let Some(mut session) = Session::with_jedi(name).await else {
        return;
    };
    session.command("from deephaven import empty_table").await;
    let mut stream = session.stream().await;
    stream.send(open(URI, 1, "s = '\u{1F600}'; "));
    stream.send(change(URI, 2, at(0, 10), 0, "emp"));
    stream.send(items_at(1, 2, 0, 13));
    let found_items = found(stream.answer().await);
    assert_eq!(found_items.len(), 1);
    assert_eq!(found_items[0].label, "empty_table");
    assert_eq!(found_items[0].start, 10);
    let range = found_items[0].text_edit.as_ref().unwrap().range.unwrap();
    assert_eq!((range.start, range.end), (at(0, 10), at(0, 13)));
    // the document is that text when it is read back
    session
        .command(&format!(
            "_d = jedi_settings.get_doc('{URI}')\n\
             assert len(_d) == 13 and [ord(c) for c in _d[5:7]] == [0xd83d, 0xde00], _d[5:7]"
        ))
        .await;
}

/// Observed on Java 42.9: the script reaches the completer as the variable
/// `jedi_settings`; switched off, every request is answered without success; deleted, it
/// is there again for the next stream.
#[tokio::test]
async fn a_script_switches_completion_off_and_cannot_lose_it() {
    let name = "a_script_switches_completion_off_and_cannot_lose_it";
    let Some(mut session) = Session::with_jedi(name).await else {
        return;
    };
    session.command("from deephaven import empty_table").await;
    let mut stream = session.stream().await;
    stream.send(open(URI, 1, "emp"));
    stream.send(items(1, URI, 1, 3));
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);

    session.command("jedi_settings.mode = 'OFF'").await;
    stream.send(items(2, URI, 1, 3));
    assert_eq!(stream.answer().await, failed(2));
    stream.send(signature(3, 3));
    assert_eq!(stream.answer().await, failed(3));
    session.command("jedi_settings.mode = 'STRONG'").await;
    stream.send(items(4, URI, 1, 3));
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);

    // without the variable a message about a document ends the stream
    session.command("del jedi_settings").await;
    stream.send(open(URI, 1, "emp"));
    let status = stream.next().await.expect("a status").unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().starts_with("Details Logged w/ID '"),
        "{status:?}"
    );
    // and the next stream has it again, with the documents it had
    let mut stream = session.stream().await;
    stream.send(items(5, URI, 1, 3));
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);
}

/// A command holds the update graph's lock and the interpreter for as long as it runs. A
/// completion takes neither the graph's lock nor a turn behind the command: it is
/// answered while the command sleeps, and while the command computes, when the
/// interpreter switches between the two threads.
#[tokio::test]
async fn a_completion_is_answered_while_a_command_runs() {
    let name = "a_completion_is_answered_while_a_command_runs";
    let Some(mut session) = Session::with_jedi(name).await else {
        return;
    };
    session.command("from deephaven import empty_table").await;
    // a stream that is open, and one that is opened while the command runs
    let mut before = session.stream().await;
    before.send(open(URI, 1, "emp"));
    before.send(items(1, URI, 1, 3));
    assert_eq!(labels(&before.answer().await), ["empty_table"]);

    let commands = [
        "import time\ntime.sleep(6)",
        "import time\n_end = time.time() + 6\nwhile time.time() < _end:\n    pass",
    ];
    for (request_id, code) in (2..).zip(commands) {
        let running = tokio::spawn(session.command(code));
        tokio::time::sleep(Duration::from_millis(500)).await;
        before.send(items(request_id, URI, 1, 3));
        let answer = before.answer().await;
        assert_eq!(answer.request_id, request_id);
        assert_eq!(labels(&answer), ["empty_table"]);
        let mut during = session.stream().await;
        during.send(open(URI, 1, "empty_t"));
        during.send(items(request_id, URI, 1, 7));
        assert_eq!(labels(&during.answer().await), ["empty_table"]);
        assert!(!running.is_finished(), "the answers waited for: {code}");
        running.await.unwrap();
        before.send(open(URI, 1, "emp"));
    }
}

/// Where the completer's package cannot be imported the session answers as a session
/// without completion does, and goes on doing so: Java warns once and never looks again.
/// This needs no `jedi`: the test hides it.
#[tokio::test]
async fn a_session_without_jedi_answers_that_nothing_was_found() {
    let mut session = Session::start().await;
    session
        .command(
            "import sys\n\
             _hidden = {k: sys.modules.pop(k) for k in list(sys.modules)\n    \
             if k == 'jedi' or k.startswith('deephaven_internal.auto_completer')}\n\
             sys.modules['jedi'] = None",
        )
        .await;
    #[allow(deprecated)]
    let nothing = |request_id: i32| AutoCompleteResponse {
        request_id,
        success: true,
        response: Some(CompletionResponse::CompletionItems(
            GetCompletionItemsResponse {
                success: true,
                request_id,
                ..Default::default()
            },
        )),
    };
    // Java's observer for such a session answers every message, with nothing
    let empty = |request_id: i32| AutoCompleteResponse {
        request_id,
        success: true,
        response: None,
    };
    let mut stream = session.stream().await;
    stream.send(open(URI, 1, "emp"));
    assert_eq!(stream.answer().await, empty(0));
    stream.send(items(1, URI, 1, 3));
    assert_eq!(stream.answer().await, nothing(1));
    stream.send(signature(2, 3));
    assert_eq!(stream.answer().await, empty(2));
    // a document that was never opened is no error for it
    stream.send(change("inmemory://never/opened", 2, at(0, 0), 0, "x"));
    assert_eq!(stream.answer().await, empty(0));

    session
        .command("del sys.modules['jedi']\nsys.modules.update(_hidden)")
        .await;
    assert!(session
        .python
        .server
        .state
        .scope
        .get("jedi_settings")
        .is_none());
    let mut stream = session.stream().await;
    stream.send(items(4, URI, 1, 3));
    assert_eq!(stream.answer().await, nothing(4));
}
