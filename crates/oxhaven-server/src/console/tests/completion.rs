//! Tests of the completion stream and of its browser pair, with a completer that needs no
//! interpreter. What a client is answered in each case is what Java 42.9 answered to the
//! same messages with `jedi` behind them; the Python completer has its own tests
//! (`console/python/completer_tests.rs`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::{HandshakeRequest, Ticket};
use oxhaven_proto::script::auto_complete_request::Request as CompletionRequest;
use oxhaven_proto::script::auto_complete_response::Response as CompletionResponse;
use oxhaven_proto::script::change_document_request::TextDocumentContentChangeEvent;
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{
    AutoCompleteRequest, AutoCompleteResponse, BrowserNextResponse, ChangeDocumentRequest,
    CloseDocumentRequest, CompletionItem, DocumentRange, GetCompletionItemsRequest,
    GetDiagnosticRequest, GetHeapInfoRequest, GetHoverRequest, GetPullDiagnosticResponse,
    GetSignatureHelpRequest, MarkupContent, OpenDocumentRequest, Position, StartConsoleRequest,
    TextDocumentItem, TextEdit, VersionedTextDocumentIdentifier,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tonic::{Code, Request, Status, Streaming};

use super::super::completer::{Completer, Completion, Signature};
use super::{client_and_session, Client};
use crate::session::export_id_to_ticket;
use crate::test_support::with_auth;

pub(in crate::console) const URI: &str = "inmemory://test/1";

/// A completer that knows four names, one signature and what it is told of documents.
#[derive(Default)]
struct Words {
    /// Text and version, by URI.
    documents: Mutex<BTreeMap<String, (Vec<u16>, i32)>>,
    off: AtomicBool,
}

const NAMES: [&str; 4] = ["empty_table", "emit", "time_table", "_empty"];

impl Words {
    /// The identifier that ends at a position of the newest version of a document;
    /// `None` for another version.
    fn word(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Option<String>, String> {
        let documents = self.documents.lock().unwrap();
        let (text, newest) = documents.get(uri).ok_or(format!("KeyError: '{uri}'"))?;
        if *newest != version {
            return Ok(None);
        }
        let text = String::from_utf16(text).unwrap();
        let line = text.split('\n').nth((line - 1) as usize);
        let before = line
            .and_then(|line| line.get(..column as usize))
            .ok_or("ValueError: the position is outside the document")?;
        let start = before
            .rfind(|character: char| !(character.is_alphanumeric() || character == '_'))
            .map_or(0, |at| at + 1);
        Ok(Some(before[start..].to_string()))
    }
}

impl Completer for Words {
    fn open_document(&self, text: &str, uri: &str, version: i32) -> Result<(), String> {
        let text = text.encode_utf16().collect();
        let mut documents = self.documents.lock().unwrap();
        documents.insert(uri.to_string(), (text, version));
        Ok(())
    }

    fn document(&self, uri: &str) -> Result<Vec<u16>, String> {
        let documents = self.documents.lock().unwrap();
        let document = documents.get(uri).ok_or(format!("KeyError: '{uri}'"))?;
        Ok(document.0.clone())
    }

    fn update_document(&self, text: &[u16], uri: &str, version: i32) -> Result<(), String> {
        let mut documents = self.documents.lock().unwrap();
        documents.insert(uri.to_string(), (text.to_vec(), version));
        Ok(())
    }

    fn close_document(&self, uri: &str) -> Result<(), String> {
        let mut documents = self.documents.lock().unwrap();
        documents.remove(uri).ok_or(format!("KeyError: '{uri}'"))?;
        Ok(())
    }

    fn is_enabled(&self) -> Result<bool, String> {
        Ok(!self.off.load(Ordering::SeqCst))
    }

    fn completions(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Vec<Completion>, String> {
        let Some(word) = self
            .word(uri, version, line, column)?
            .filter(|word| !word.is_empty())
        else {
            return Ok(Vec::new());
        };
        let names = NAMES.iter().filter(|name| name.starts_with(&word));
        Ok(names
            .map(|name| Completion {
                name: name.to_string(),
                start: column - word.len() as i32,
                detail: format!("{name}(size)"),
                docstring: format!("About {name}."),
                kind: 3,
            })
            .collect())
    }

    fn signatures(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Vec<Signature>, String> {
        if self.word(uri, version, line, column)?.is_none() {
            return Ok(Vec::new());
        }
        Ok(vec![Signature {
            label: "f(a, b=2)".to_string(),
            documentation: "Does **f**.".to_string(),
            parameters: vec![
                ("a".to_string(), "the first".to_string()),
                ("b=2".to_string(), String::new()),
            ],
            active_parameter: -1,
        }])
    }

    fn hover(&self, uri: &str, version: i32, line: i32, column: i32) -> Result<String, String> {
        let word = self.word(uri, version, line, column)?;
        Ok(word.map_or(String::new(), |word| format!("about `{word}`")))
    }
}

pub(in crate::console) fn console_id() -> Option<Ticket> {
    Some(export_id_to_ticket(1))
}

pub(in crate::console) fn message(
    request_id: i32,
    request: CompletionRequest,
) -> AutoCompleteRequest {
    AutoCompleteRequest {
        console_id: console_id(),
        request_id,
        request: Some(request),
    }
}

pub(in crate::console) fn named(
    uri: &str,
    version: i32,
) -> Option<VersionedTextDocumentIdentifier> {
    Some(VersionedTextDocumentIdentifier {
        uri: uri.to_string(),
        version,
    })
}

pub(in crate::console) fn at(line: i32, character: i32) -> Option<Position> {
    Some(Position { line, character })
}

pub(in crate::console) fn open(uri: &str, version: i32, text: &str) -> AutoCompleteRequest {
    #[allow(deprecated)]
    let open = OpenDocumentRequest {
        console_id: None,
        text_document: Some(TextDocumentItem {
            uri: uri.to_string(),
            language_id: "python".to_string(),
            version,
            text: text.to_string(),
        }),
    };
    message(0, CompletionRequest::OpenDocument(open))
}

/// An edit that puts `text` in place of `length` code units at a position.
pub(in crate::console) fn change(
    uri: &str,
    version: i32,
    start: Option<Position>,
    length: i32,
    text: &str,
) -> AutoCompleteRequest {
    #[allow(deprecated)]
    let change = ChangeDocumentRequest {
        console_id: None,
        text_document: named(uri, version),
        content_changes: vec![TextDocumentContentChangeEvent {
            range: Some(DocumentRange { start, end: None }),
            range_length: length,
            text: text.to_string(),
        }],
    };
    message(0, CompletionRequest::ChangeDocument(change))
}

pub(in crate::console) fn close(uri: &str) -> AutoCompleteRequest {
    #[allow(deprecated)]
    let close = CloseDocumentRequest {
        console_id: None,
        text_document: named(uri, 0),
    };
    message(0, CompletionRequest::CloseDocument(close))
}

pub(in crate::console) fn items(
    request_id: i32,
    uri: &str,
    version: i32,
    character: i32,
) -> AutoCompleteRequest {
    #[allow(deprecated)]
    let items = GetCompletionItemsRequest {
        console_id: None,
        context: None,
        text_document: named(uri, version),
        position: at(0, character),
        request_id: 0,
    };
    message(request_id, CompletionRequest::GetCompletionItems(items))
}

/// The labels of the items of an answer, which must be a successful one with items.
pub(in crate::console) fn labels(answer: &AutoCompleteResponse) -> Vec<&str> {
    assert!(answer.success, "{answer:?}");
    match &answer.response {
        Some(CompletionResponse::CompletionItems(items)) => {
            items.items.iter().map(|item| item.label.as_str()).collect()
        }
        other => panic!("completion items, not {other:?}"),
    }
}

pub(in crate::console) fn failed(request_id: i32) -> AutoCompleteResponse {
    AutoCompleteResponse {
        request_id,
        success: false,
        response: None,
    }
}

/// A client whose session's console, export 1, completes with [`Words`].
async fn start() -> (Client, Arc<Words>) {
    let (mut client, session) = client_and_session().await;
    let words = Arc::new(Words::default());
    *session.completer.lock().unwrap() = Some(words.clone());
    let start = StartConsoleRequest {
        result_id: console_id(),
        session_type: "words".to_string(),
    };
    client
        .console
        .start_console(with_auth(start, &client.bearer))
        .await
        .unwrap();
    (client, words)
}

/// One completion stream of a client: the messages go in, the answers come out.
pub(in crate::console) struct Stream {
    pub(in crate::console) send: Option<mpsc::UnboundedSender<AutoCompleteRequest>>,
    pub(in crate::console) answers: Streaming<AutoCompleteResponse>,
}

impl Stream {
    async fn open(client: &mut Client) -> Self {
        Self::of(&mut client.console, &client.bearer).await
    }

    /// A stream of the client that logged in as `bearer`.
    pub(in crate::console) async fn of(
        console: &mut ConsoleServiceClient<Channel>,
        bearer: &str,
    ) -> Self {
        let (send, messages) = mpsc::unbounded_channel();
        let messages = UnboundedReceiverStream::new(messages);
        let answers = console
            .auto_complete_stream(with_auth(messages, bearer))
            .await
            .unwrap()
            .into_inner();
        Self {
            send: Some(send),
            answers,
        }
    }

    pub(in crate::console) fn send(&self, message: AutoCompleteRequest) {
        self.send.as_ref().unwrap().send(message).unwrap();
    }

    /// The next answer, the status that ends the stream, or `None` at its end.
    pub(in crate::console) async fn next(
        &mut self,
    ) -> Option<Result<AutoCompleteResponse, Status>> {
        next_of(&mut self.answers).await
    }

    pub(in crate::console) async fn answer(&mut self) -> AutoCompleteResponse {
        self.next().await.expect("an answer").expect("an answer")
    }

    /// The status the stream ends with after `message`, which is not answered.
    async fn refused(client: &mut Client, message: AutoCompleteRequest) -> Status {
        let mut stream = Self::open(client).await;
        stream.send(message);
        let status = stream.next().await.expect("a status").unwrap_err();
        assert!(stream.next().await.is_none());
        status
    }
}

async fn next_of(
    answers: &mut Streaming<AutoCompleteResponse>,
) -> Option<Result<AutoCompleteResponse, Status>> {
    // the first answer of `jedi` in a process takes seconds
    tokio::time::timeout(Duration::from_secs(60), answers.next())
        .await
        .expect("an answer or the end within a minute")
}

#[tokio::test]
async fn requests_are_answered_from_the_documents_the_stream_was_sent() {
    let (mut client, _) = start().await;
    let mut stream = Stream::open(&mut client).await;
    // a message about a document is not answered: the first answer is the request's
    stream.send(open(URI, 1, "x = 1\nem"));
    let mut second_line = items(2, URI, 1, 2);
    if let Some(CompletionRequest::GetCompletionItems(items)) = &mut second_line.request {
        items.position = at(1, 2);
    }
    stream.send(second_line);
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (2, true));
    let Some(CompletionResponse::CompletionItems(found)) = answer.response else {
        panic!("completion items, not {:?}", answer.response);
    };
    // Java sets the two deprecated fields: success, and the id inside the request
    #[allow(deprecated)]
    {
        assert!(found.success);
        assert_eq!(found.request_id, 0);
    }
    let item = |name: &str, sort_text: &str| CompletionItem {
        start: 0,
        length: name.len() as i32,
        label: name.to_string(),
        kind: 3,
        detail: format!("{name}(size)"),
        text_edit: Some(TextEdit {
            range: Some(DocumentRange {
                start: at(1, 0),
                end: at(1, 2),
            }),
            text: name.to_string(),
        }),
        sort_text: sort_text.to_string(),
        insert_text_format: 2,
        documentation: Some(MarkupContent {
            kind: "plaintext".to_string(),
            value: format!("About {name}."),
        }),
        ..Default::default()
    };
    assert_eq!(
        found.items,
        [item("empty_table", "00000"), item("emit", "00001")]
    );

    // an edit is applied to the document, under its new version
    stream.send(change(URI, 2, at(1, 2), 0, "p"));
    stream.send(items(3, URI, 2, 0));
    assert_eq!(labels(&stream.answer().await), [] as [&str; 0]);
    let mut third = items(4, URI, 2, 3);
    if let Some(CompletionRequest::GetCompletionItems(items)) = &mut third.request {
        items.position = at(1, 3);
    }
    stream.send(third);
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);

    let help = GetSignatureHelpRequest {
        context: None,
        text_document: named(URI, 2),
        position: at(0, 1),
    };
    stream.send(message(5, CompletionRequest::GetSignatureHelp(help)));
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (5, true));
    let Some(CompletionResponse::Signatures(help)) = answer.response else {
        panic!("signatures, not {:?}", answer.response);
    };
    assert_eq!((help.active_signature, help.active_parameter), (None, None));
    let [signature] = &help.signatures[..] else {
        panic!("one signature, not {:?}", help.signatures);
    };
    let markdown = |value: &str| {
        Some(MarkupContent {
            kind: "markdown".to_string(),
            value: value.to_string(),
        })
    };
    assert_eq!(signature.label, "f(a, b=2)");
    assert_eq!(signature.documentation, markdown("Does **f**."));
    assert_eq!(signature.active_parameter, Some(-1));
    let parameters = signature.parameters.iter();
    let parameters: Vec<_> = parameters
        .map(|parameter| (parameter.label.as_str(), parameter.documentation.clone()))
        .collect();
    assert_eq!(
        parameters,
        [("a", markdown("the first")), ("b=2", markdown(""))]
    );

    let hover = GetHoverRequest {
        text_document: named(URI, 2),
        position: at(1, 3),
    };
    stream.send(message(6, CompletionRequest::GetHover(hover)));
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (6, true));
    let Some(CompletionResponse::Hover(hover)) = answer.response else {
        panic!("a hover, not {:?}", answer.response);
    };
    assert_eq!(
        (hover.contents, hover.range),
        (markdown("about `emp`"), None)
    );

    let diagnostic = GetDiagnosticRequest {
        text_document: named(URI, 2),
        ..Default::default()
    };
    stream.send(message(7, CompletionRequest::GetDiagnostic(diagnostic)));
    let answer = stream.answer().await;
    assert_eq!((answer.request_id, answer.success), (7, true));
    let nothing = CompletionResponse::Diagnostic(GetPullDiagnosticResponse::default());
    assert_eq!(answer.response, Some(nothing));

    // the documents are the completer's: another stream asks about the same one
    let mut other = Stream::open(&mut client).await;
    let mut asked = items(8, URI, 2, 3);
    if let Some(CompletionRequest::GetCompletionItems(items)) = &mut asked.request {
        items.position = at(1, 3);
    }
    other.send(asked);
    assert_eq!(labels(&other.answer().await), ["empty_table"]);
}

#[tokio::test]
async fn an_edit_that_starts_outside_the_document_is_ignored() {
    let (mut client, words) = start().await;
    let mut stream = Stream::open(&mut client).await;
    stream.send(open(URI, 1, "emp"));
    stream.send(change(URI, 2, at(4, 0), 0, "zzz"));
    // the version is still 1, so a request for 2 finds nothing
    stream.send(items(1, URI, 1, 3));
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);
    stream.send(items(2, URI, 2, 3));
    assert_eq!(labels(&stream.answer().await), [] as [&str; 0]);
    let documents = words.documents.lock().unwrap().clone();
    assert_eq!(String::from_utf16(&documents[URI].0).unwrap(), "emp");
}

#[tokio::test]
async fn a_request_that_fails_is_answered_without_success() {
    let (mut client, words) = start().await;
    let mut stream = Stream::open(&mut client).await;
    // a document that is not open
    stream.send(items(1, "inmemory://none", 1, 0));
    assert_eq!(stream.answer().await, failed(1));
    // a position outside the document
    stream.send(open(URI, 1, "emp"));
    stream.send(items(2, URI, 1, 30));
    assert_eq!(stream.answer().await, failed(2));
    // an export that is no console
    client.execute("make t 1").await;
    client.fetch(b"s/t", 9).await.unwrap();
    let mut of_a_table = items(3, URI, 1, 3);
    of_a_table.console_id = Some(export_id_to_ticket(9));
    stream.send(of_a_table);
    assert_eq!(stream.answer().await, failed(3));
    // a completer that is switched off, whatever the request
    words.off.store(true, Ordering::SeqCst);
    stream.send(items(4, URI, 1, 3));
    assert_eq!(stream.answer().await, failed(4));
    let diagnostic = GetDiagnosticRequest::default();
    stream.send(message(5, CompletionRequest::GetDiagnostic(diagnostic)));
    assert_eq!(stream.answer().await, failed(5));
    words.off.store(false, Ordering::SeqCst);
    stream.send(items(6, URI, 1, 3));
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);
}

#[tokio::test]
async fn a_client_from_before_the_outer_fields_is_answered_by_the_inner_ones() {
    let (mut client, _) = start().await;
    let mut stream = Stream::open(&mut client).await;
    stream.send(open(URI, 1, "emp"));
    #[allow(deprecated)]
    let old = GetCompletionItemsRequest {
        console_id: console_id(),
        context: None,
        text_document: named(URI, 1),
        position: at(0, 3),
        request_id: 15,
    };
    stream.send(AutoCompleteRequest {
        console_id: None,
        request_id: 0,
        request: Some(CompletionRequest::GetCompletionItems(old)),
    });
    let answer = stream.answer().await;
    assert_eq!(answer.request_id, 15);
    assert_eq!(labels(&answer), ["empty_table"]);
    #[allow(deprecated)]
    match &answer.response {
        Some(CompletionResponse::CompletionItems(items)) => assert_eq!(items.request_id, 15),
        other => panic!("completion items, not {other:?}"),
    }
}

#[tokio::test]
async fn a_message_the_stream_cannot_handle_ends_it() {
    let (mut client, _) = start().await;
    let nothing = AutoCompleteRequest {
        console_id: console_id(),
        request_id: 11,
        request: None,
    };
    let status = Stream::refused(&mut client, nothing).await;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "Autocomplete command missing request");

    // Java fails in the completer and logs the exception under an id
    let unknown = "inmemory://none";
    for message in [
        change(unknown, 2, at(0, 0), 0, "a"),
        close(unknown),
        // an edit that reaches before the document, Java's StringIndexOutOfBoundsException
        change(URI, 2, at(0, 3), -10, "emp"),
    ] {
        let mut stream = Stream::open(&mut client).await;
        stream.send(open(URI, 1, "abcdef"));
        stream.send(message);
        let status = stream.next().await.expect("a status").unwrap_err();
        assert_eq!(status.code(), Code::InvalidArgument);
        assert!(
            status.message().starts_with("Details Logged w/ID '"),
            "{status}"
        );
        assert!(stream.next().await.is_none());
    }

    let mut not_a_ticket = items(17, URI, 1, 3);
    not_a_ticket.console_id = Some(Ticket {
        ticket: b"zzz".to_vec(),
    });
    let status = Stream::refused(&mut client, not_a_ticket).await;
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(
        status
            .message()
            .starts_with("Could not resolve ticket 'consoleId': found 0x"),
        "{status}"
    );
    let mut no_console = items(18, URI, 1, 3);
    no_console.console_id = None;
    let status = Stream::refused(&mut client, no_console).await;
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "Could not resolve ticket 'consoleId': ticket was not provided"
    );
}

#[tokio::test]
async fn the_stream_ends_without_an_error_with_the_client_and_with_the_session() {
    let (mut client, _) = start().await;
    let mut stream = Stream::open(&mut client).await;
    stream.send(open(URI, 1, "emp"));
    stream.send(items(1, URI, 1, 3));
    assert_eq!(labels(&stream.answer().await), ["empty_table"]);
    // the client ends its side
    stream.send.take();
    assert!(stream.next().await.is_none());

    // a request on a console that is not exported yet waits, and the stream stays open
    let mut stream = Stream::open(&mut client).await;
    let mut waits = items(2, URI, 1, 3);
    waits.console_id = Some(export_id_to_ticket(77));
    stream.send(waits);
    let waited = tokio::time::timeout(Duration::from_millis(300), stream.answers.next());
    assert!(waited.await.is_err(), "no answer and no end");
    // a stream with nothing waiting ends without an error when the session closes
    let mut idle = Stream::open(&mut client).await;
    idle.send(open(URI, 1, "emp"));
    idle.send(items(3, URI, 1, 3));
    idle.answer().await;

    SessionServiceClient::new(client.server.channel.clone())
        .close_session(with_auth(HandshakeRequest::default(), &client.bearer))
        .await
        .unwrap();
    assert!(idle.next().await.is_none());
    // the request that waited fails with its console, which ends its stream, as in Java
    let status = stream.next().await.expect("a status").unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        "Export in state DEPENDENCY_CANCELLED (related parent export id: 77)"
    );
}

/// A message with the stream headers a browser sends: the ticket, the sequence, and the
/// half close if `half_close`.
fn browser<T>(
    message: T,
    bearer: &str,
    ticket: Option<&str>,
    sequence: Option<&str>,
    half_close: bool,
) -> Request<T> {
    let mut request = with_auth(message, bearer);
    let metadata = request.metadata_mut();
    if let Some(ticket) = ticket {
        metadata.insert("x-deephaven-stream-ticket", ticket.parse().unwrap());
    }
    if let Some(sequence) = sequence {
        metadata.insert("x-deephaven-stream-sequence", sequence.parse().unwrap());
    }
    if half_close {
        metadata.insert("x-deephaven-stream-halfclose", "1".parse().unwrap());
    }
    request
}

impl Client {
    /// Opens the browser stream `ticket` with a message that opens the document.
    async fn browser_open(&mut self, ticket: i32) -> Streaming<AutoCompleteResponse> {
        let ticket = ticket.to_string();
        let request = browser(
            open(URI, 1, "emp"),
            &self.bearer,
            Some(&ticket),
            Some("0"),
            false,
        );
        let opened = self.console.open_auto_complete_stream(request).await;
        opened.unwrap().into_inner()
    }

    async fn browser_open_refused(
        &mut self,
        ticket: Option<&str>,
        sequence: Option<&str>,
    ) -> Status {
        let request = browser(open(URI, 1, "emp"), &self.bearer, ticket, sequence, false);
        let opened = self.console.open_auto_complete_stream(request).await;
        opened.expect_err("the call is refused")
    }

    async fn browser_next(
        &mut self,
        message: AutoCompleteRequest,
        ticket: i32,
        sequence: i32,
        half_close: bool,
    ) -> Result<BrowserNextResponse, Status> {
        let (ticket, sequence) = (ticket.to_string(), sequence.to_string());
        let request = browser(
            message,
            &self.bearer,
            Some(&ticket),
            Some(&sequence),
            half_close,
        );
        let call = self.console.next_auto_complete_stream(request);
        let answered = tokio::time::timeout(Duration::from_secs(5), call).await;
        answered
            .expect("an answer within five seconds")
            .map(tonic::Response::into_inner)
    }

    /// Sends messages until one is refused because the stream has ended, which the stream
    /// does a moment after the message that ended it was taken.
    async fn refused_once_ended(&mut self, ticket: i32, first_sequence: i32) -> Status {
        for sequence in first_sequence..first_sequence + 100 {
            match self
                .browser_next(items(0, URI, 1, 3), ticket, sequence, false)
                .await
            {
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                Err(status) => return status,
            }
        }
        panic!("the stream did not end");
    }
}

fn logged(status: &Status) {
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(
        status.message().starts_with("Details Logged w/ID '"),
        "{status}"
    );
}

const NOT_DELIVERED: &str = "stream ended before this message arrived; sequence";

#[tokio::test]
async fn a_browser_sends_the_stream_as_one_call_per_message() {
    let (mut client, _) = start().await;
    let mut answers = client.browser_open(30).await;
    client
        .browser_next(items(2, URI, 1, 3), 30, 1, false)
        .await
        .unwrap();
    let answer = next_of(&mut answers).await.unwrap().unwrap();
    assert_eq!(answer.request_id, 2);
    assert_eq!(labels(&answer), ["empty_table"]);
    // the message of the half close is not delivered: this one would end the stream
    // with a status
    let ignored = AutoCompleteRequest::default();
    client.browser_next(ignored, 30, 2, true).await.unwrap();
    assert!(next_of(&mut answers).await.is_none());

    // the stream is gone: a message for it is refused, a half close is not
    let status = client
        .browser_next(items(3, URI, 1, 3), 30, 3, false)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Cancelled);
    assert_eq!(
        status.message(),
        format!("{NOT_DELIVERED} 3 was not delivered")
    );
    let closing = AutoCompleteRequest::default();
    client.browser_next(closing, 30, 3, true).await.unwrap();

    // its id cannot name another stream. Java does not notice: the call stays open and
    // no message reaches it.
    let mut again = client.browser_open(30).await;
    let waited = tokio::time::timeout(Duration::from_millis(300), again.next());
    assert!(waited.await.is_err(), "no answer and no end");
    let status = client
        .browser_next(items(4, URI, 1, 3), 30, 1, false)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Cancelled);
}

#[tokio::test]
async fn browser_messages_are_delivered_in_the_order_of_their_sequence() {
    let (mut client, _) = start().await;
    let mut answers = client.browser_open(31).await;
    // a message that arrives early waits for those before it
    client
        .browser_next(items(5, URI, 1, 0), 31, 3, false)
        .await
        .unwrap();
    client
        .browser_next(items(4, URI, 1, 3), 31, 2, false)
        .await
        .unwrap();
    let waited = tokio::time::timeout(Duration::from_millis(300), answers.next());
    assert!(
        waited.await.is_err(),
        "nothing is delivered before sequence 1"
    );
    client
        .browser_next(change(URI, 2, at(0, 0), 3, "tim"), 31, 1, false)
        .await
        .unwrap();
    // 2 is for the version before the edit of 1 and so finds nothing; had it been
    // delivered before the edit it would have found a name
    let mut seen = Vec::new();
    for _ in 0..2 {
        let answer = next_of(&mut answers).await.unwrap().unwrap();
        seen.push((answer.request_id, labels(&answer).len()));
    }
    seen.sort_unstable();
    assert_eq!(seen, [(4, 0), (5, 0)]);

    // a sequence that was delivered is refused, and the stream goes on
    let status = client
        .browser_next(items(6, URI, 2, 3), 31, 1, false)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::OutOfRange);
    assert_eq!(
        status.message(),
        "Duplicate sequence sent: next seq=4 recv seq=1"
    );
    client
        .browser_next(items(7, URI, 2, 3), 31, 4, false)
        .await
        .unwrap();
    let answer = next_of(&mut answers).await.unwrap().unwrap();
    assert_eq!(answer.request_id, 7);
    assert_eq!(labels(&answer), ["time_table"]);

    // a half close that arrives early ends the stream when the messages before it have
    // been delivered; nothing may follow it, and there is one
    let closing = AutoCompleteRequest::default();
    client
        .browser_next(closing.clone(), 31, 6, true)
        .await
        .unwrap();
    let status = client
        .browser_next(items(8, URI, 2, 3), 31, 7, false)
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Aborted);
    assert_eq!(
        status.message(),
        "Sequence sent after half close: closed seq=6 recv seq=7"
    );
    let status = client.browser_next(closing, 31, 5, true).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Already half closed: closed seq=6 recv seq=5"
    );
    client.browser_next(close(URI), 31, 5, false).await.unwrap();
    assert!(next_of(&mut answers).await.is_none());
}

#[tokio::test]
async fn a_next_call_that_arrives_before_its_open_call_waits_for_it() {
    let (mut client, _) = start().await;
    let mut early = client.console.clone();
    let request = browser(
        items(13, URI, 1, 3),
        &client.bearer,
        Some("34"),
        Some("1"),
        false,
    );
    let next = tokio::spawn(async move { early.next_auto_complete_stream(request).await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!next.is_finished());
    let mut answers = client.browser_open(34).await;
    next.await.unwrap().unwrap();
    let answer = next_of(&mut answers).await.unwrap().unwrap();
    assert_eq!(answer.request_id, 13);
}

#[tokio::test]
async fn the_stream_headers_of_a_browser_call_are_checked() {
    let (mut client, _) = start().await;
    let status = client.browser_open_refused(None, None).await;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "no x-deephaven-stream headers, cannot handle open request"
    );
    let either = "Either both x-deephaven-stream-ticket and x-deephaven-stream-sequence must be \
                  provided, or neither";
    for (ticket, sequence) in [(Some("20"), None), (None, Some("0"))] {
        let status = client.browser_open_refused(ticket, sequence).await;
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), either);
    }
    // Java reads the headers on every call of the service
    let heap_info = browser(
        GetHeapInfoRequest {},
        &client.bearer,
        Some("40"),
        None,
        false,
    );
    let status = client.console.get_heap_info(heap_info).await.unwrap_err();
    assert_eq!(status.message(), either);
    // numbers that are none, an id that is not a client's, and the console's own id
    for (ticket, sequence) in [
        ("abc", "0"),
        ("21", "x"),
        ("0", "0"),
        ("-5", "0"),
        ("1", "0"),
    ] {
        logged(
            &client
                .browser_open_refused(Some(ticket), Some(sequence))
                .await,
        );
    }
    let status = client.browser_open_refused(Some("22"), Some("1")).await;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "open request must have sequence 0, received 1"
    );

    let no_headers = "no x-deephaven-stream headers, cannot handle next request";
    let bearer = client.bearer.clone();
    let next = |ticket, sequence, half_close| {
        browser(items(8, URI, 1, 3), &bearer, ticket, sequence, half_close)
    };
    let mut console = client.console.clone();
    let mut refused = async |request| {
        console
            .next_auto_complete_stream(request)
            .await
            .unwrap_err()
    };
    for request in [next(None, None, false), next(None, None, true)] {
        let status = refused(request).await;
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), no_headers);
    }
    for sequence in ["0", "-1"] {
        let status = refused(next(Some("32"), Some(sequence), false)).await;
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            status.message(),
            format!("next request must have a positive sequence, received {sequence}")
        );
    }
    logged(&refused(next(Some("q"), Some("1"), false)).await);
    // an export that is no stream
    client.execute("make t 1").await;
    client.fetch(b"s/t", 37).await.unwrap();
    logged(&refused(next(Some("37"), Some("1"), false)).await);
}

#[tokio::test]
async fn a_browser_stream_that_opens_with_a_half_close_is_over_at_once() {
    let (mut client, words) = start().await;
    for ticket in [None, Some("23")] {
        let sequence = ticket.map(|_| "0");
        let request = browser(open(URI, 1, "emp"), &client.bearer, ticket, sequence, true);
        let opened = client.console.open_auto_complete_stream(request).await;
        let mut answers = opened.unwrap().into_inner();
        assert!(next_of(&mut answers).await.is_none());
    }
    // the message was not delivered
    assert!(words.documents.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_message_a_browser_stream_cannot_handle_ends_it_and_leaves_its_call_open() {
    let (mut client, _) = start().await;
    let mut answers = client.browser_open(32).await;
    // the call that brings the message is answered: the message was taken
    let nothing = AutoCompleteRequest::default();
    client.browser_next(nothing, 32, 1, false).await.unwrap();
    let status = client.refused_once_ended(32, 2).await;
    assert_eq!(status.code(), Code::Cancelled);
    assert!(status.message().starts_with(NOT_DELIVERED), "{status}");
    // Java hands the failure to the stream, which ignores it: the open call is told
    // nothing, until the session ends
    let waited = tokio::time::timeout(Duration::from_millis(300), answers.next());
    assert!(waited.await.is_err(), "no status and no end");
    SessionServiceClient::new(client.server.channel.clone())
        .close_session(with_auth(HandshakeRequest::default(), &client.bearer))
        .await
        .unwrap();
    assert!(next_of(&mut answers).await.is_none());
}

#[tokio::test]
async fn a_browser_stream_ends_when_its_open_call_or_its_session_does() {
    let (mut client, _) = start().await;
    let answers = client.browser_open(35).await;
    client
        .browser_next(items(1, URI, 1, 3), 35, 1, false)
        .await
        .unwrap();
    // the client gives the call up
    drop(answers);
    let status = client.refused_once_ended(35, 2).await;
    assert_eq!(status.code(), Code::Cancelled);
    assert!(status.message().starts_with(NOT_DELIVERED), "{status}");
    let closing = AutoCompleteRequest::default();
    client.browser_next(closing, 35, 200, true).await.unwrap();

    let mut answers = client.browser_open(50).await;
    SessionServiceClient::new(client.server.channel.clone())
        .close_session(with_auth(HandshakeRequest::default(), &client.bearer))
        .await
        .unwrap();
    assert!(next_of(&mut answers).await.is_none());
}
