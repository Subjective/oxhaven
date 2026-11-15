//! Completion for the console: what `AutoCompleteStream` and its browser pair answer.
//!
//! A client opens one stream per console and sends a message per keystroke: the document
//! it edits (`open_document`, then `change_document` with the edit, `close_document`), and
//! requests about a position in it (`get_completion_items`, `get_signature_help`,
//! `get_hover`, `get_diagnostic`). The language side answers them ([`Completer`]); for
//! Python that is the library `jedi` running in the script session
//! (`console/python/completer.rs`). A session that has no completer, which includes a
//! Python session whose environment has no `jedi`, answers every request "success, nothing
//! found" and keeps no documents.
//!
//! What matches Java, observed on Java 42.9 (`oracle/console/probe.py` has the cases that
//! are kept):
//!
//! - **Documents belong to the completer, not to the stream.** They are kept by URI, so a
//!   second stream can ask about a document the first opened. The messages about documents
//!   are handled in the order they arrive and are not answered.
//! - **Requests are answered when the console's export is ready**, each by itself, so two
//!   answers may arrive in either order. A request for a version of a document that is no
//!   longer the newest is answered with nothing; one that fails (a document that is not
//!   open, a position outside it, a completer that is switched off) is answered with
//!   `success: false`.
//! - **A message the stream cannot handle ends it**: one without a request
//!   (`INVALID_ARGUMENT`), a request whose console id is no export ticket
//!   (`FAILED_PRECONDITION`), a change or a close of a document that is not open
//!   (`INVALID_ARGUMENT`, with the id the details are logged under).
//! - **The stream ends without an error** when the client ends its side, and when the
//!   session of the call ends.
//!
//! # Sharing the interpreter with a running command
//!
//! A completion takes the interpreter lock on the thread that answers it, for as long as
//! `jedi` runs, and takes no other lock: **not the update graph's**, and not the session's
//! snapshot lock, both of which a command holds for as long as it runs. So a completion
//! never waits for a command to end, and a cycle never waits for a completion.
//!
//! A command that is running Python gives the interpreter lock up when another thread
//! asks, at the latest after the interpreter's switch interval (5 ms), and at once while it
//! sleeps, waits or runs an engine operation; the completion then runs with the same right
//! to the lock as the command, each giving way to the other every interval. That is what
//! Java does: its completion calls into the interpreter through jpy, which takes the lock
//! between two byte codes of the command. The command is slowed for the tens of
//! milliseconds a completion takes and is never stopped; a completion waits for native
//! code that keeps the lock (a C extension that computes without releasing it), in Java as
//! here. The lock rule of `docs/PYTHON_UDF.md` is kept: the thread that holds the
//! interpreter lock for a completion waits for nothing a cycle holds.
//!
//! Java sets the completer up by evaluating a script in the session when a stream is
//! opened, which waits for a running command. This server sets it up under the
//! interpreter lock alone, so opening a stream does not wait either.
//!
//! One thing is this server's own: `jedi` is asked for one answer at a time (a lock taken
//! before the interpreter lock), where Java lets its worker threads call it concurrently.
//! `jedi` keeps caches that are not made for that. Changes to documents do not take that
//! lock, so a completion that a newer version of its document overtakes still gives up.

use std::sync::{Arc, Mutex, PoisonError};

use oxhaven_proto::script::auto_complete_request::Request as CompletionRequest;
use oxhaven_proto::script::auto_complete_response::Response as CompletionResponse;
use oxhaven_proto::script::change_document_request::TextDocumentContentChangeEvent;
use oxhaven_proto::script::{
    AutoCompleteRequest, AutoCompleteResponse, CompletionItem, DocumentRange,
    GetCompletionItemsRequest, GetCompletionItemsResponse, GetHoverResponse,
    GetPullDiagnosticResponse, GetSignatureHelpResponse, MarkupContent, ParameterInformation,
    Position, SignatureInformation, TextEdit,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;
use uuid::Uuid;

use super::browser::BrowserStream;
use super::console_of;
use crate::session::{ticket_to_export_id, ExportValue, OnCloseId, Session};

/// One completion at a position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    /// The whole name, of which the document holds a prefix.
    pub name: String,
    /// The column, in the line of the position, at which the name starts.
    pub start: i32,
    /// A line about the name: the signature of a function, the kind of anything else.
    pub detail: String,
    pub docstring: String,
    /// The `CompletionItemKind` of the language server protocol.
    pub kind: i32,
}

/// One signature of the call a position is in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature {
    pub label: String,
    /// Markdown.
    pub documentation: String,
    /// The label and the documentation, in Markdown, of each parameter.
    pub parameters: Vec<(String, String)>,
    /// The index of the parameter the position is at, or -1.
    pub active_parameter: i32,
}

/// The language side of completion. Lines count from 1 and columns from 0, as `jedi`
/// counts them; the service adds the one to the line a client sends.
///
/// A document is text in UTF-16 code units, which is what a client's positions count and
/// what Java's `String` holds; [`Completer::document`] and [`Completer::update_document`]
/// pass it in that form so that an edit can cut it where Java does.
///
/// Every method may block, and an `Err` holds the text of what went wrong, for the log.
///
/// Behavior: the methods of the Python object `jedi_settings` that
/// `io.deephaven.server.console.completer.PythonAutoCompleteObserver` calls
pub trait Completer: Send + Sync {
    /// Behavior: `Completer.open_doc`
    fn open_document(&self, text: &str, uri: &str, version: i32) -> Result<(), String>;

    /// The text of an open document.
    ///
    /// Behavior: `Completer.get_doc`
    fn document(&self, uri: &str) -> Result<Vec<u16>, String>;

    /// Behavior: `Completer.update_doc`
    fn update_document(&self, text: &[u16], uri: &str, version: i32) -> Result<(), String>;

    /// Behavior: `Completer.close_doc`
    fn close_document(&self, uri: &str) -> Result<(), String>;

    /// Whether requests are answered at all.
    ///
    /// Behavior: `Completer.is_enabled`
    fn is_enabled(&self) -> Result<bool, String>;

    /// Behavior: `Completer.do_completion`
    fn completions(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Vec<Completion>, String>;

    /// Behavior: `Completer.do_signature_help`
    fn signatures(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Vec<Signature>, String>;

    /// Markdown about the name at a position; empty when there is nothing to say.
    ///
    /// Behavior: `Completer.do_hover`
    fn hover(&self, uri: &str, version: i32, line: i32, column: i32) -> Result<String, String>;
}

/// The status of a failure that Java answers by logging the exception under a new id.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
pub(super) fn logged(text: &str) -> Status {
    let error_id = Uuid::new_v4();
    tracing::error!("Internal Error '{error_id}' {text}");
    Status::invalid_argument(format!("Details Logged w/ID '{error_id}'"))
}

/// Why an edit cannot be applied to a document.
#[derive(Debug, PartialEq, Eq)]
pub struct OutOfBounds {
    /// The index the text after the edit would start at.
    pub begin: i32,
    pub length: usize,
}

/// The offset of a position in a document, in code units; `None` if the document has
/// fewer lines. The column is added without a look at the length of the line, with the
/// arithmetic of a Java `int`, so the offset may be negative or beyond the document.
///
/// Behavior: `io.deephaven.lang.parse.LspTools#getOffsetFromPosition`
fn offset_of(document: &[u16], position: &Position) -> Option<i32> {
    const LINE_END: u16 = b'\n' as u16;
    let mut offset = 0usize;
    for _ in 0..position.line.max(0) {
        let line_end = document[offset..]
            .iter()
            .position(|unit| *unit == LINE_END)?;
        offset += line_end + 1;
    }
    Some((offset as i32).wrapping_add(position.character))
}

/// A document after the edits of one `change_document` message, in their order. `None`
/// if an edit starts on a line the document does not have, or before its first character:
/// the whole message is then ignored, the edits before that one too.
///
/// An edit replaces `range_length` code units from the start of its range; the end of the
/// range is not looked at. A start beyond the document replaces everything, and a length
/// that reaches before the start of the document is the failure Java's `substring` raises.
///
/// Behavior: `io.deephaven.lang.parse.CompletionParser#updateDocumentChanges`
pub fn apply_changes(
    mut document: Vec<u16>,
    changes: &[TextDocumentContentChangeEvent],
) -> Result<Option<Vec<u16>>, OutOfBounds> {
    for change in changes {
        let start = change.range.as_ref().and_then(|range| range.start.as_ref());
        let start = start.cloned().unwrap_or_default();
        let Some(offset) = offset_of(&document, &start).filter(|offset| *offset >= 0) else {
            return Ok(None);
        };
        // a document is far shorter than two gigabytes
        let length = document.len() as i32;
        let prefix = if offset > 0 && offset <= length {
            &document[..offset as usize]
        } else {
            &[]
        };
        let begin = offset.wrapping_add(change.range_length);
        let suffix = if begin >= length {
            &[]
        } else if begin < 0 {
            return Err(OutOfBounds {
                begin,
                length: document.len(),
            });
        } else {
            &document[begin as usize..]
        };
        document = prefix
            .iter()
            .copied()
            .chain(change.text.encode_utf16())
            .chain(suffix.iter().copied())
            .collect();
    }
    Ok(Some(document))
}

/// The text items are sorted by so that they stay in the order they were found in: the
/// index in base 36, at least five digits.
///
/// Behavior: `io.deephaven.lang.completion.ChunkerCompleter#sortable`
fn sortable(index: usize) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut digits = Vec::new();
    let mut rest = index;
    loop {
        digits.push(DIGITS[rest % 36]);
        rest /= 36;
        if rest == 0 {
            break;
        }
    }
    while digits.len() < 5 {
        digits.push(b'0');
    }
    digits.iter().rev().map(|digit| *digit as char).collect()
}

fn markup(kind: &str, value: String) -> Option<MarkupContent> {
    Some(MarkupContent {
        kind: kind.to_string(),
        value,
    })
}

/// Behavior: `PythonAutoCompleteObserver#getCompletionItems`
fn completion_items(
    completer: &dyn Completer,
    request: &GetCompletionItemsRequest,
) -> Result<GetCompletionItemsResponse, String> {
    let document = request.text_document.clone().unwrap_or_default();
    let position = request.position.unwrap_or_default();
    let found = completer.completions(
        &document.uri,
        document.version,
        position.line.wrapping_add(1),
        position.character,
    )?;
    let items = found.into_iter().enumerate().map(|(index, completion)| {
        let at = |character| {
            Some(Position {
                line: position.line,
                character,
            })
        };
        CompletionItem {
            start: completion.start,
            // the length of a Java String
            length: completion.name.encode_utf16().count() as i32,
            label: completion.name.clone(),
            kind: completion.kind,
            detail: completion.detail,
            text_edit: Some(TextEdit {
                range: Some(DocumentRange {
                    start: at(completion.start),
                    end: at(position.character),
                }),
                text: completion.name,
            }),
            sort_text: sortable(index),
            insert_text_format: 2,
            documentation: markup("plaintext", completion.docstring),
            ..Default::default()
        }
    });
    // the two fields are deprecated in the protocol and Java sets them
    #[allow(deprecated)]
    Ok(GetCompletionItemsResponse {
        items: items.collect(),
        request_id: request.request_id,
        success: true,
    })
}

/// Behavior: `PythonAutoCompleteObserver#getSignatureHelp`
fn signature_help(
    completer: &dyn Completer,
    document: Option<&oxhaven_proto::script::VersionedTextDocumentIdentifier>,
    position: Option<&Position>,
) -> Result<GetSignatureHelpResponse, String> {
    let (uri, version) = document.map_or(("", 0), |document| (&document.uri, document.version));
    let (line, character) = position.map_or((0, 0), |at| (at.line, at.character));
    let found = completer.signatures(uri, version, line.wrapping_add(1), character)?;
    let signatures = found.into_iter().map(|signature| SignatureInformation {
        label: signature.label,
        documentation: markup("markdown", signature.documentation),
        parameters: signature
            .parameters
            .into_iter()
            .map(|(label, documentation)| ParameterInformation {
                label,
                documentation: markup("markdown", documentation),
            })
            .collect(),
        active_parameter: Some(signature.active_parameter),
    });
    Ok(GetSignatureHelpResponse {
        signatures: signatures.collect(),
        active_signature: None,
        active_parameter: None,
    })
}

/// Behavior: `PythonAutoCompleteObserver#getHover`, which leaves the range out because
/// the IDE finds the word under the pointer by itself
fn hover(
    completer: &dyn Completer,
    document: Option<&oxhaven_proto::script::VersionedTextDocumentIdentifier>,
    position: Option<&Position>,
) -> Result<GetHoverResponse, String> {
    let (uri, version) = document.map_or(("", 0), |document| (&document.uri, document.version));
    let (line, character) = position.map_or((0, 0), |at| (at.line, at.character));
    let text = completer.hover(uri, version, line.wrapping_add(1), character)?;
    Ok(GetHoverResponse {
        contents: markup("markdown", text),
        range: None,
    })
}

/// The id a request is answered under: its own, or, from a client older than that field,
/// the one inside its `get_completion_items`.
///
/// Behavior: `PythonAutoCompleteObserver#handleAutocompleteRequest`
fn request_id(request: &AutoCompleteRequest) -> i32 {
    if request.request_id > 0 {
        return request.request_id;
    }
    match &request.request {
        #[allow(deprecated)]
        Some(CompletionRequest::GetCompletionItems(items)) => items.request_id,
        _ => 0,
    }
}

/// The answer to a request about a position, once the export its console id names is
/// there. Whatever fails is `success: false` and nothing else: an export that is no
/// console, a completer that is switched off, a document that is not open, a position
/// outside the document.
///
/// Behavior: `PythonAutoCompleteObserver#handleAutocompleteRequest`
fn answer(
    completer: &dyn Completer,
    console: &ExportValue,
    request: &AutoCompleteRequest,
) -> AutoCompleteResponse {
    let request_id = request_id(request);
    let answered = (|| {
        // Java casts the export's value to its script session and asks that for the
        // completer; every console is the one script session
        console_of(console).map_err(|_| "the console id does not name a console".to_string())?;
        if !completer.is_enabled()? {
            return Err("Ignoring completion request because jedi is disabled".to_string());
        }
        Ok(match &request.request {
            Some(CompletionRequest::GetCompletionItems(items)) => {
                CompletionResponse::CompletionItems(completion_items(completer, items)?)
            }
            Some(CompletionRequest::GetSignatureHelp(help)) => {
                CompletionResponse::Signatures(signature_help(
                    completer,
                    help.text_document.as_ref(),
                    help.position.as_ref(),
                )?)
            }
            Some(CompletionRequest::GetHover(at)) => CompletionResponse::Hover(hover(
                completer,
                at.text_document.as_ref(),
                at.position.as_ref(),
            )?),
            // Java has no diagnostics for Python (deephaven-core issue 3614)
            _ => CompletionResponse::Diagnostic(GetPullDiagnosticResponse::default()),
        })
    })();
    match answered {
        Ok(response) => AutoCompleteResponse {
            request_id,
            success: true,
            response: Some(response),
        },
        Err(text) => {
            // Java logs these at this level unless `deephaven.console.autocomplete.quiet`
            // is turned off, which this server has no flag for
            tracing::trace!("Exception occurred during autocomplete: {text}");
            AutoCompleteResponse {
                request_id,
                success: false,
                response: None,
            }
        }
    }
}

/// The answer of a session without a completer: "success, nothing found".
///
/// Behavior: `ConsoleServiceGrpcImpl.NoopAutoCompleteObserver#onNext`
fn nothing_found(request: &AutoCompleteRequest) -> AutoCompleteResponse {
    let response = match request.request {
        // for clients from before the other requests existed
        Some(CompletionRequest::GetCompletionItems(_)) => {
            // the two fields are deprecated in the protocol and Java sets them
            #[allow(deprecated)]
            Some(CompletionResponse::CompletionItems(
                GetCompletionItemsResponse {
                    success: true,
                    request_id: request.request_id,
                    ..Default::default()
                },
            ))
        }
        _ => None,
    };
    AutoCompleteResponse {
        request_id: request.request_id,
        success: true,
        response,
    }
}

type Answer = Result<AutoCompleteResponse, Status>;

/// Where the answers of one stream go, until the stream is ended. Whoever may still
/// answer holds this; ending the stream is taking the sender out, not dropping the last
/// holder, so that an answer that comes late is dropped and does not keep the stream
/// open.
struct Outlet(Mutex<Option<mpsc::UnboundedSender<Answer>>>);

impl Outlet {
    fn send(&self, answer: Answer) {
        if let Some(sender) = &*self.0.lock().unwrap_or_else(PoisonError::into_inner) {
            let _ = sender.send(answer);
        }
    }

    /// Ends the stream without an error.
    fn complete(&self) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
    }

    /// Ends the stream with `status`.
    fn fail(&self, status: Status) {
        let sender = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(sender) = sender {
            let _ = sender.send(Err(status));
        }
    }
}

/// The answers of one stream, as the call's response. When the call ends, whether the
/// stream was ended or the client gave up on it, the stream stops answering and forgets
/// its session, and a browser stream is told that its call is over.
pub struct Answers {
    answers: UnboundedReceiverStream<Answer>,
    outlet: Arc<Outlet>,
    session: Arc<Session>,
    on_close: Option<OnCloseId>,
    browser: Option<Arc<BrowserStream<AutoCompleteRequest>>>,
}

impl Stream for Answers {
    type Item = Answer;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.answers).poll_next(context)
    }
}

impl Drop for Answers {
    fn drop(&mut self) {
        if let Some(on_close) = self.on_close.take() {
            self.session.remove_on_close(on_close);
        }
        self.outlet.complete();
        if let Some(browser) = &self.browser {
            browser.cancelled();
        }
    }
}

/// Handles one message. `Err` is a message the stream cannot handle, which Java throws
/// out of the stream's `onNext`.
///
/// Behavior: `PythonAutoCompleteObserver#onNext`
async fn on_next(
    completer: &Arc<dyn Completer>,
    session: &Arc<Session>,
    outlet: &Arc<Outlet>,
    request: AutoCompleteRequest,
) -> Result<(), Status> {
    // the completer may block: it runs Python
    async fn blocking<T: Send + 'static>(
        work: impl FnOnce() -> Result<T, String> + Send + 'static,
    ) -> Result<T, Status> {
        match tokio::task::spawn_blocking(work).await {
            Ok(done) => done.map_err(|text| logged(&text)),
            Err(error) => Err(logged(&error.to_string())),
        }
    }
    let completer = completer.clone();
    match request.request {
        Some(CompletionRequest::OpenDocument(open)) => {
            let document = open.text_document.unwrap_or_default();
            blocking(move || {
                completer.open_document(&document.text, &document.uri, document.version)
            })
            .await
        }
        Some(CompletionRequest::ChangeDocument(change)) => {
            let document = change.text_document.unwrap_or_default();
            blocking(move || {
                let text = completer.document(&document.uri)?;
                match apply_changes(text, &change.content_changes) {
                    Ok(Some(text)) => {
                        completer.update_document(&text, &document.uri, document.version)
                    }
                    Ok(None) => {
                        tracing::warn!(
                            "Invalid change in document {}[{}]",
                            document.uri,
                            document.version
                        );
                        Ok(())
                    }
                    Err(OutOfBounds { begin, length }) => Err(format!(
                        "java.lang.StringIndexOutOfBoundsException: begin {begin}, end \
                         {length}, length {length}"
                    )),
                }
            })
            .await
        }
        Some(CompletionRequest::CloseDocument(close)) => {
            let document = close.text_document.unwrap_or_default();
            blocking(move || completer.close_document(&document.uri)).await
        }
        None => Err(Status::invalid_argument(
            "Autocomplete command missing request",
        )),
        Some(_) => {
            // from a client older than the field of the message, the console id is in
            // `get_completion_items`, the one request there was
            let console_id = match (&request.console_id, &request.request) {
                (Some(console_id), _) => console_id.ticket.clone(),
                #[allow(deprecated)]
                (None, Some(CompletionRequest::GetCompletionItems(items))) => items
                    .console_id
                    .as_ref()
                    .map(|console_id| console_id.ticket.clone())
                    .unwrap_or_default(),
                (None, _) => Vec::new(),
            };
            let console = session.get_export(ticket_to_export_id(&console_id, "consoleId")?)?;
            let (failed, answered) = (outlet.clone(), outlet.clone());
            session
                .non_export()?
                .require([console])
                .on_error(move |status| failed.fail(status))
                .submit(move |inputs| {
                    answered.send(Ok(answer(completer.as_ref(), &inputs[0], &request)));
                    Ok(ExportValue::unit())
                })?;
            Ok(())
        }
    }
}

/// Answers the messages of one stream of `session`, which arrive as `requests`, with
/// `completer`, or with "nothing found" when there is none.
///
/// The stream ends without an error when `requests` ends and when the session does. A
/// message that cannot be handled ends it with its status; unless the messages come from
/// a browser stream, which is then over while the call that opened it stays open and is
/// told nothing. That is what Java does: its browser stream hands the failure to the
/// stream's `onError`, which ignores it.
///
/// Behavior: `PythonAutoCompleteObserver`, `ConsoleServiceGrpcImpl.NoopAutoCompleteObserver`
/// and `io.deephaven.server.session.SessionCloseableObserver`
pub(super) fn serve<R>(
    mut requests: R,
    completer: Option<Arc<dyn Completer>>,
    session: &Arc<Session>,
    browser: Option<Arc<BrowserStream<AutoCompleteRequest>>>,
) -> Result<Answers, Status>
where
    R: Stream<Item = Result<AutoCompleteRequest, Status>> + Send + Unpin + 'static,
{
    let (sender, receiver) = mpsc::unbounded_channel();
    let outlet = Arc::new(Outlet(Mutex::new(Some(sender))));
    let on_close = session.add_on_close({
        let outlet = outlet.clone();
        move || outlet.complete()
    })?;
    let answers = Answers {
        answers: UnboundedReceiverStream::new(receiver),
        outlet: outlet.clone(),
        session: session.clone(),
        on_close: Some(on_close),
        browser: browser.clone(),
    };
    let session = session.clone();
    tokio::spawn(async move {
        // a failure of the client's side is the end of the call, and of the stream
        while let Some(Ok(request)) = requests.next().await {
            let handled = match &completer {
                Some(completer) => on_next(completer, &session, &outlet, request).await,
                None => {
                    outlet.send(Ok(nothing_found(&request)));
                    Ok(())
                }
            };
            if let Err(status) = handled {
                match &browser {
                    Some(browser) => {
                        tracing::error!("closing browser stream on unexpected exception: {status}");
                        browser.failed();
                    }
                    None => outlet.fail(status),
                }
                return;
            }
        }
        outlet.complete();
    });
    Ok(answers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn units(text: &str) -> Vec<u16> {
        text.encode_utf16().collect()
    }

    fn edit(line: i32, character: i32, length: i32, text: &str) -> TextDocumentContentChangeEvent {
        TextDocumentContentChangeEvent {
            range: Some(DocumentRange {
                start: Some(Position { line, character }),
                // Java does not read the end
                end: Some(Position {
                    line: 99,
                    character: 99,
                }),
            }),
            range_length: length,
            text: text.to_string(),
        }
    }

    fn edited(document: &str, edits: &[TextDocumentContentChangeEvent]) -> Option<String> {
        let changed = apply_changes(units(document), edits).unwrap();
        changed.map(|units| String::from_utf16(&units).unwrap())
    }

    /// The expectations are what Java 42.9 did with the same edits, read off the
    /// completions it then gave.
    #[test]
    fn an_edit_replaces_a_number_of_code_units_from_its_start() {
        assert_eq!(edited("", &[edit(0, 0, 0, "e")]).unwrap(), "e");
        assert_eq!(edited("em", &[edit(0, 2, 0, "p")]).unwrap(), "emp");
        assert_eq!(
            edited("xxxemp\nyy", &[edit(0, 0, 3, "")]).unwrap(),
            "emp\nyy"
        );
        assert_eq!(
            edited("a\nbcd\ne", &[edit(1, 1, 2, "X")]).unwrap(),
            "a\nbX\ne"
        );
        // the edits of one message apply in their order, each to the result of the last
        assert_eq!(
            edited("", &[edit(0, 0, 0, "ac"), edit(0, 1, 0, "b")]).unwrap(),
            "abc"
        );
        // without a range the edit is at the start
        let whole = TextDocumentContentChangeEvent {
            range: None,
            range_length: 3,
            text: "new".to_string(),
        };
        assert_eq!(edited("old text", &[whole]).unwrap(), "new text");
    }

    #[test]
    fn positions_count_utf16_code_units() {
        // the face is two code units, so the end of the text is at 10
        assert_eq!(
            edited("s = '\u{1F600}'; ", &[edit(0, 10, 0, "emp")]).unwrap(),
            "s = '\u{1F600}'; emp"
        );
        let cut = apply_changes(units("\u{1F600}"), &[edit(0, 1, 0, "x")]).unwrap();
        assert_eq!(cut.unwrap(), [0xD83D, b'x' as u16, 0xDE00]);
    }

    #[test]
    fn an_edit_outside_the_document_does_what_java_does() {
        // more lines than there are, or before the first character: the message is ignored,
        // the edits before the bad one too
        assert_eq!(edited("emp", &[edit(4, 0, 0, "zzz")]), None);
        assert_eq!(edited("abcdef", &[edit(0, -3, 0, "emp")]), None);
        assert_eq!(
            edited("abc", &[edit(0, 0, 0, "x"), edit(2, 0, 0, "y")]),
            None
        );
        // a start beyond the end keeps nothing of the document
        assert_eq!(edited("emp", &[edit(0, 40, 0, "zzz")]).unwrap(), "zzz");
        // the column is not held to its line
        assert_eq!(edited("ab\ncd", &[edit(0, 4, 0, "X")]).unwrap(), "ab\ncXd");
        assert_eq!(
            edited("abc\ndef", &[edit(1, -2, 0, "X")]).unwrap(),
            "abXc\ndef"
        );
        // a length beyond the end removes the rest
        assert_eq!(edited("abcdef", &[edit(0, 0, 100, "emp")]).unwrap(), "emp");
        // a negative length keeps text twice
        assert_eq!(
            edited("abcdef", &[edit(0, 3, -1, "emp")]).unwrap(),
            "abcempcdef"
        );
        // and one that reaches before the document fails
        assert_eq!(
            apply_changes(units("abcdef"), &[edit(0, 3, -10, "emp")]),
            Err(OutOfBounds {
                begin: -7,
                length: 6
            })
        );
        // a sum beyond a Java int wraps to before the document
        assert!(apply_changes(units("abcdef"), &[edit(0, 3, i32::MAX, "emp")]).is_err());
    }

    #[test]
    fn items_keep_their_order_by_a_sortable_index() {
        assert_eq!(sortable(0), "00000");
        assert_eq!(sortable(35), "0000z");
        assert_eq!(sortable(36), "00010");
        assert_eq!(sortable(36 * 36 * 36 * 36 * 36), "100000");
    }
}
