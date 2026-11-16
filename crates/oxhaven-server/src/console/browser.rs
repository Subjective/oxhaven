//! A client stream made of unary calls, for browsers.
//!
//! gRPC-web over `https://` cannot stream to the server, so the web client splits a call
//! with a stream in both directions in two: an `Open` call, which carries the first
//! message and whose response is the response stream, and one unary `Next` call per later
//! message. Three request headers tie them together:
//!
//! | Header | Holds |
//! | --- | --- |
//! | `x-deephaven-stream-ticket` | an export id of the client's session, which names the stream |
//! | `x-deephaven-stream-sequence` | the place of the message in the stream; the `Open` call is 0 |
//! | `x-deephaven-stream-halfclose` | present on the message that ends the client's side; its payload is not delivered |
//!
//! The stream is an export of the session under that id for as long as it lasts, which is
//! how a `Next` call finds it and why one that arrives before its `Open` call waits for
//! it. Messages are delivered in the order of their sequence, whatever order the calls
//! arrive in. The stream ends when the message with the half close is reached, when the
//! client gives up the `Open` call, when the session ends and when the service fails on a
//! message; its export is then gone, and a `Next` call for it is refused unless it is a
//! half close.
//!
//! What a client is answered in each case was observed on Java 42.9 and is in the tests.
//! Only the mode of delivery that the console uses is here: Java's other mode, in which
//! a message that is overtaken is dropped, is the one of `BrowserFlightService`, which
//! this server does not have (`tasks/2.6-config-grpc-web.md`).
//!
//! Behavior: `io.deephaven.server.browserstreaming.BrowserStream` (mode `IN_ORDER`),
//! `BrowserStreamInterceptor`, and
//! `io.deephaven.server.util.GrpcServiceOverrideBuilder.BrowserStreamMethod`

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::mpsc;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use super::completer::logged;
use crate::session::{
    current_session, is_terminal, Export, ExportError, ExportValue, OnCloseId, Reply, Session,
};

const TICKET_HEADER: &str = "x-deephaven-stream-ticket";
const SEQUENCE_HEADER: &str = "x-deephaven-stream-sequence";
const HALF_CLOSE_HEADER: &str = "x-deephaven-stream-halfclose";

/// What the headers of a call say of its place in a browser stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamData {
    /// The export id that names the stream; `None` on a call that opens a stream and ends
    /// its client side at once, which needs no name.
    pub export_id: Option<i32>,
    pub sequence: i32,
    pub half_close: bool,
}

/// Reads the stream headers of a call; `None` if it has none. Java reads them on every
/// call of a service that has a browser pair, so every call of the console service is
/// refused when they are not well formed.
///
/// Behavior: `BrowserStreamInterceptor#interceptCall`
pub fn stream_data(metadata: &MetadataMap) -> Result<Option<StreamData>, Status> {
    // gRPC Java reads the last value of a header
    let last = |name: &str| metadata.get_all(name).iter().next_back();
    let (ticket, sequence) = (last(TICKET_HEADER), last(SEQUENCE_HEADER));
    let half_close = metadata.contains_key(HALF_CLOSE_HEADER);
    let number = |value: &tonic::metadata::AsciiMetadataValue| {
        let text = value.to_str().unwrap_or("");
        text.parse::<i32>().map_err(|_| {
            logged(&format!(
                "java.lang.NumberFormatException: For input string: \"{text}\""
            ))
        })
    };
    match (ticket, sequence) {
        (Some(ticket), Some(sequence)) => Ok(Some(StreamData {
            export_id: Some(number(ticket)?),
            sequence: number(sequence)?,
            half_close,
        })),
        (None, None) if half_close => Ok(Some(StreamData {
            export_id: None,
            sequence: 0,
            half_close,
        })),
        (None, None) => Ok(None),
        _ => Err(Status::invalid_argument(format!(
            "Either both {TICKET_HEADER} and {SEQUENCE_HEADER} must be provided, or neither"
        ))),
    }
}

struct State<T> {
    /// The sequence of the message that is delivered next.
    next: i64,
    /// The sequence of the message that ends the client's side, once it has arrived.
    half_closed: Option<i32>,
    /// Messages that arrived before the one that is next, by sequence, and whether each
    /// is the half close. One that arrives for a sequence that is already waiting stays
    /// behind it.
    waiting: BTreeMap<i32, Vec<(T, bool)>>,
    /// Where messages are delivered; `None` once the stream has ended.
    deliver: Option<mpsc::UnboundedSender<T>>,
    /// The export that names the stream, until it has ended.
    export: Option<Export>,
    on_close: Option<OnCloseId>,
}

/// The client side of one stream that a browser sends as calls: puts the messages in
/// their order and hands them on. See the [module documentation](self).
pub struct BrowserStream<T> {
    session: Arc<Session>,
    state: Mutex<State<T>>,
}

impl<T: Send + Sync + 'static> BrowserStream<T> {
    /// Starts the stream that the headers `data` of an `Open` call name, in the session
    /// of the call, and exports it under its id. Returns it with the end the service
    /// reads the messages from, in order; that end closes when the stream ends.
    ///
    /// The first message is not delivered by this: pass it to
    /// [`BrowserStream::on_message`], with the same headers, once the service reads.
    ///
    /// An id that cannot be exported under (not positive, or in use) refuses the call. An
    /// id that was released is not noticed, as in Java: the stream exists, the first
    /// message is delivered, and no `Next` call ever reaches it.
    ///
    /// Behavior: the constructor of `BrowserStream`
    pub fn open(
        session: &Arc<Session>,
        data: &StreamData,
    ) -> Result<(Arc<Self>, mpsc::UnboundedReceiver<T>), Status> {
        let (deliver, messages) = mpsc::unbounded_channel();
        let builder = match data.export_id {
            Some(export_id) if !data.half_close => {
                if export_id <= 0 {
                    return Err(logged(
                        "java.lang.IllegalArgumentException: exportIds <= 0 are reserved for \
                         server allocation only",
                    ));
                }
                Some(session.new_export(export_id)?)
            }
            _ => None,
        };
        let stream = Arc::new(Self {
            session: session.clone(),
            state: Mutex::new(State {
                next: 0,
                half_closed: None,
                waiting: BTreeMap::new(),
                deliver: Some(deliver),
                export: builder.as_ref().map(|builder| builder.export().clone()),
                on_close: None,
            }),
        });
        let on_close = session.add_on_close({
            let stream = stream.clone();
            move || stream.end()
        })?;
        stream.lock().on_close = Some(on_close);
        if let Some(builder) = builder {
            let exported = stream.clone();
            if let Err(status) = builder.submit(move |_| Ok(ExportValue::object(exported))) {
                // the id belongs to something else, which must stay as it is
                stream.lock().export = None;
                stream.end();
                return Err(status);
            }
        }
        Ok((stream, messages))
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the message of a call whose headers are `data`: delivers it if it is next,
    /// and every waiting message that follows it without a gap; otherwise keeps it until
    /// those before it have arrived. A message for a stream that has ended is dropped.
    ///
    /// Refuses a message after the half close (`ABORTED`), a second half close
    /// (`INVALID_ARGUMENT`) and a sequence that was delivered already (`OUT_OF_RANGE`);
    /// the stream goes on.
    ///
    /// Behavior: `BrowserStream#onMessageReceived`
    pub fn on_message(&self, message: T, data: &StreamData) -> Result<(), Status> {
        let sequence = data.sequence;
        let mut state = self.lock();
        if state.deliver.is_none() {
            return Ok(());
        }
        if let Some(closed) = state.half_closed.filter(|closed| sequence > *closed) {
            return Err(Status::aborted(format!(
                "Sequence sent after half close: closed seq={closed} recv seq={sequence}"
            )));
        }
        if data.half_close {
            if let Some(closed) = state.half_closed {
                return Err(Status::invalid_argument(format!(
                    "Already half closed: closed seq={closed} recv seq={sequence}"
                )));
            }
            state.half_closed = Some(sequence);
        }
        if i64::from(sequence) < state.next {
            return Err(Status::out_of_range(format!(
                "Duplicate sequence sent: next seq={} recv seq={sequence}",
                state.next
            )));
        }
        if i64::from(sequence) != state.next {
            let waiting = state.waiting.entry(sequence).or_default();
            waiting.push((message, data.half_close));
            return Ok(());
        }
        let mut current = Some((message, data.half_close));
        while let Some((message, half_close)) = current.take() {
            state.next += 1;
            if half_close {
                drop(state);
                self.end();
                return Ok(());
            }
            if let Some(deliver) = &state.deliver {
                let _ = deliver.send(message);
            }
            // the earliest message that waits is delivered if it is next. One that is not,
            // a second message for a sequence that has gone by among them, stays where it
            // is, and so does everything behind it, as in Java.
            let next = state.next;
            let Some(mut first) = state.waiting.first_entry() else {
                break;
            };
            if i64::from(*first.key()) == next {
                current = Some(first.get_mut().remove(0));
                if first.get().is_empty() {
                    first.remove();
                }
            }
        }
        Ok(())
    }

    /// Ends the stream: nothing more is delivered, what waits is dropped, the service's
    /// end of the messages closes, and the stream is no longer an export. Does nothing
    /// the second time.
    ///
    /// Behavior: `BrowserStream#markEnded` and `#releaseExport`, which every way a stream
    /// ends goes through
    fn end(&self) {
        let (export, on_close) = {
            let mut state = self.lock();
            state.deliver = None;
            state.waiting.clear();
            (state.export.take(), state.on_close.take())
        };
        if let Some(on_close) = on_close {
            self.session.remove_on_close(on_close);
        }
        if let Some(export) = export {
            // not released: its work may not have run yet
            export.cancel();
        }
    }

    /// The call that opened the stream is over: the client gave it up, or it was
    /// answered to its end.
    ///
    /// Behavior: `BrowserStream#onCancel`
    pub fn cancelled(&self) {
        self.end();
    }

    /// The service failed on a message.
    ///
    /// Behavior: `BrowserStream#onError`
    pub fn failed(&self) {
        self.end();
    }
}

/// The stream headers of a call that opens a stream, which must have them and be the
/// first message.
///
/// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeOpen`
pub fn open_headers(data: Option<StreamData>) -> Result<StreamData, Status> {
    let Some(data) = data else {
        return Err(Status::invalid_argument(
            "no x-deephaven-stream headers, cannot handle open request",
        ));
    };
    if data.sequence != 0 {
        // the messages are delivered in order from 0, so the stream would wait for
        // ever for a message that cannot come
        return Err(Status::invalid_argument(format!(
            "open request must have sequence 0, received {}",
            data.sequence
        )));
    }
    Ok(data)
}

/// Takes a later message of a stream of messages `T` that an `Open` call started, named
/// by the stream headers of the call. Answered, with the empty message `N` of the
/// service, when the message has been taken, which for one that arrives before the
/// messages it follows is before it is handled.
///
/// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeNext`
pub async fn next<T, N>(request: Request<T>) -> Result<Response<N>, Status>
where
    T: Send + Sync + 'static,
    N: Default + Send + 'static,
{
    let no_stream_headers =
        || Status::invalid_argument("no x-deephaven-stream headers, cannot handle next request");
    let data = stream_data(request.metadata())?.ok_or_else(no_stream_headers)?;
    let export_id = data.export_id.ok_or_else(no_stream_headers)?;
    if data.sequence <= 0 {
        // sequence 0 is the message of the open call
        return Err(Status::invalid_argument(format!(
            "next request must have a positive sequence, received {}",
            data.sequence
        )));
    }
    let session = current_session(&request)?;
    let stream = session.get_export(export_id)?;
    if is_terminal(stream.state()) {
        // The stream has ended, most often because the client gave up its open call
        // and this call was on its way. A half close has nothing to deliver; a message
        // has, and the client is told that it was not.
        if data.half_close {
            return Ok(Response::new(N::default()));
        }
        return Err(Status::cancelled(format!(
            "stream ended before this message arrived; sequence {} was not delivered",
            data.sequence
        )));
    }
    let message = request.into_inner();
    let reply = Reply::new();
    let (on_error, on_success) = (reply.completer(), reply.completer());
    session
        .non_export()?
        .require([stream])
        .on_error(move |status| on_error(Err(status)))
        .submit(move |inputs| {
            // Java casts and fails with a ClassCastException
            let stream = inputs[0]
                .downcast_ref::<Arc<BrowserStream<T>>>()
                .ok_or_else(|| {
                    ExportError::Internal(
                        "the stream ticket does not name a browser stream".to_string(),
                    )
                })?;
            stream
                .on_message(message, &data)
                .map_err(ExportError::Status)?;
            on_success(Ok(N::default()));
            Ok(ExportValue::unit())
        })?;
    reply.wait().await
}

#[cfg(test)]
mod tests {
    use tonic::metadata::MetadataValue;
    use tonic::Code;

    use super::*;

    fn headers(pairs: &[(&'static str, &str)]) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        for (name, value) in pairs {
            metadata.append(*name, MetadataValue::try_from(*value).unwrap());
        }
        metadata
    }

    /// The statuses are those of Java 42.9 for the same headers on
    /// `OpenAutoCompleteStream`.
    #[test]
    fn the_headers_name_a_stream_a_sequence_and_a_half_close() {
        assert_eq!(stream_data(&headers(&[])).unwrap(), None);
        let both = [(TICKET_HEADER, "7"), (SEQUENCE_HEADER, "3")];
        let data = stream_data(&headers(&both)).unwrap().unwrap();
        assert_eq!(
            (data.export_id, data.sequence, data.half_close),
            (Some(7), 3, false)
        );
        let closing = [
            (TICKET_HEADER, "7"),
            (SEQUENCE_HEADER, "-1"),
            (HALF_CLOSE_HEADER, ""),
        ];
        let data = stream_data(&headers(&closing)).unwrap().unwrap();
        assert_eq!(
            (data.export_id, data.sequence, data.half_close),
            (Some(7), -1, true)
        );
        // a half close alone opens a stream that is over at once
        let data = stream_data(&headers(&[(HALF_CLOSE_HEADER, "1")]))
            .unwrap()
            .unwrap();
        assert_eq!(
            (data.export_id, data.sequence, data.half_close),
            (None, 0, true)
        );
        // the last value of a repeated header counts
        let twice = [
            (TICKET_HEADER, "7"),
            (TICKET_HEADER, "8"),
            (SEQUENCE_HEADER, "0"),
        ];
        assert_eq!(
            stream_data(&headers(&twice)).unwrap().unwrap().export_id,
            Some(8)
        );

        for one in [[(TICKET_HEADER, "7")], [(SEQUENCE_HEADER, "0")]] {
            let status = stream_data(&headers(&one)).unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument);
            assert_eq!(
                status.message(),
                "Either both x-deephaven-stream-ticket and x-deephaven-stream-sequence must be \
                 provided, or neither"
            );
        }
        for bad in [
            [(TICKET_HEADER, "abc"), (SEQUENCE_HEADER, "0")],
            [(TICKET_HEADER, "7"), (SEQUENCE_HEADER, "x")],
            [(TICKET_HEADER, "7"), (SEQUENCE_HEADER, "99999999999")],
        ] {
            let status = stream_data(&headers(&bad)).unwrap_err();
            assert_eq!(status.code(), Code::InvalidArgument);
            assert!(
                status.message().starts_with("Details Logged w/ID '"),
                "{status}"
            );
        }
    }
}
