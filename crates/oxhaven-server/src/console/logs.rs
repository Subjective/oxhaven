//! The log lines `ConsoleService/SubscribeToLogs` sends: the server's log and what scripts
//! print.
//!
//! A [`LogBuffer`] keeps the latest records and hands every record to the clients that
//! listen. Two things write into it:
//!
//! - **The server's own log.** [`layer`] is a `tracing` layer that records every event the
//!   process logs under its level, which has Java's name (`ERROR`, `WARN`, `INFO`, `DEBUG`,
//!   `TRACE`), as the Java server's `LogBufferAppender` does. The text is laid out as that
//!   appender's pattern lays it out, `thread | logger | message`, with the name of the
//!   thread and the module path of the event where Java has the thread and the logger.
//! - **What scripts print.** A script session writes what its language prints to standard
//!   output and standard error under the levels `STDOUT` and `STDERR`, which is how a
//!   client sees the output of a command: `ExecuteCommand` does not carry it.
//!
//! What is *not* captured, here as in Java: bytes written to the file descriptors 1 and 2,
//! by native code in the process or by a subprocess a script starts. Java captures
//! `System.out` and `System.err`, the objects, and this server captures Python's
//! `sys.stdout` and `sys.stderr`, the objects; `os.write(1, ...)` and `os.system("echo")`
//! reach the terminal of either server and not its clients. One difference follows from
//! the languages: what Java code prints to `System.out` is in Java's buffer under
//! `STDOUT`, and nothing in this server prints except through the log.
//!
//! The Java server of the oracle sends every log line a second time, split into lines
//! under `STDERR`. That is its logging configuration (`oracle/bin/logback-oracle.xml`
//! adds an appender that writes to the captured `System.err`), not the server: the
//! configuration the server ships with writes around the captured stream "so we don't
//! double up messages to the LogBuffer" (`server/jetty-app/src/main/resources/logback.xml`).
//! The copy is not reproduced.
//!
//! A client that does not read as fast as records arrive is ended, as in Java, when the
//! records waiting for it exceed [`CLIENT_BUFFER_SIZE`]; and every stream of a session
//! ends when the session does. Java gathers records for 100 ms before it sends them
//! (`ConsoleServiceGrpcImpl#SUBSCRIBE_TO_LOGS_SEND_MILLIS`); this server sends each when it
//! is written, which a client can tell only by the delay.

use std::collections::VecDeque;
use std::fmt::{self, Write as _};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use oxhaven_proto::script::{LogSubscriptionData, LogSubscriptionRequest};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};
use tokio_stream::Stream;
use tonic::Status;
use tracing::field::{Field, Visit};
use tracing::Event;
use tracing_subscriber::layer::{Context as LayerContext, Layer};

use crate::session::{OnCloseId, Session};

/// The level of what a script writes to standard output.
///
/// Behavior: `io.deephaven.io.log.LogLevel#STDOUT`
pub const STDOUT: &str = "STDOUT";

/// The level of what a script writes to standard error.
///
/// Behavior: `io.deephaven.io.log.LogLevel#STDERR`
pub const STDERR: &str = "STDERR";

/// How many records are kept for a client that subscribes later.
///
/// Behavior: the default of the property `logBuffer.history` in
/// `io.deephaven.server.runner.MainHelper`
const HISTORY_SIZE: usize = 1024;

/// How many records may wait for one client before it is ended as too slow.
///
/// Behavior: the default of `ConsoleServiceGrpcImpl#SUBSCRIBE_TO_LOGS_BUFFER_SIZE`
pub const CLIENT_BUFFER_SIZE: usize = 32768;

/// The name of the Java server property that sets the buffer size, which the text of the
/// status names. This server has no such property; the size is [`CLIENT_BUFFER_SIZE`].
///
/// Behavior: `ConsoleServiceGrpcImpl#SUBSCRIBE_TO_LOGS_BUFFER_SIZE_PROP`
const BUFFER_SIZE_PROPERTY: &str = "deephaven.console.subscribeToLogs.bufferSize";

/// The status that ends the stream of a client that did not keep up.
///
/// Behavior: `ConsoleServiceGrpcImpl.LogsClient#run`
fn too_slow() -> Status {
    Status::resource_exhausted(format!(
        "Too slow: the client or network may be too slow to keep up with the logging rates, \
         or there may be logging bursts that exceed the available buffer size. The buffer \
         size can be configured through the server property '{BUFFER_SIZE_PROPERTY}'."
    ))
}

/// The status that ends a stream whose session ended.
///
/// Behavior: `SessionServiceGrpcImpl.SessionServiceCallListener#SESSION_CLOSED`
fn session_closed() -> Status {
    Status::cancelled("Session closed")
}

/// Why a stream ends, as the buffer tells the stream.
const OPEN: u8 = 0;
const TOO_SLOW: u8 = 1;
const SESSION_CLOSED: u8 = 2;

/// The stream of one `SubscribeToLogs` call: records, and at most one status that ends it.
/// The records still waiting when the stream is ended are not sent, as in Java.
pub struct LogUpdates {
    receiver: mpsc::Receiver<LogSubscriptionData>,
    end: Arc<AtomicU8>,
    finished: bool,
    /// The callback that ends this stream with its session, to be forgotten with it.
    on_close: Option<(Weak<Session>, OnCloseId)>,
}

impl LogUpdates {
    /// The status the buffer ended this stream with, once.
    fn ended(&mut self) -> Option<Status> {
        let status = match self.end.load(Ordering::Acquire) {
            TOO_SLOW => too_slow(),
            SESSION_CLOSED => session_closed(),
            _ => return None,
        };
        self.finished = true;
        Some(status)
    }

    /// The next record or the status that ends the stream, if one is there now.
    pub fn try_recv(&mut self) -> Result<Result<LogSubscriptionData, Status>, TryRecvError> {
        if self.finished {
            return Err(TryRecvError::Disconnected);
        }
        if let Some(status) = self.ended() {
            return Ok(Err(status));
        }
        self.receiver.try_recv().map(Ok)
    }
}

impl Stream for LogUpdates {
    type Item = Result<LogSubscriptionData, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        if let Some(status) = self.ended() {
            return Poll::Ready(Some(Err(status)));
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(record)) => Poll::Ready(Some(Ok(record))),
            // The buffer dropped the sender after it set the reason, and the channel is
            // read only when it is empty: look at the reason again.
            Poll::Ready(None) => {
                let status = self.ended();
                self.finished = true;
                Poll::Ready(status.map(Err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for LogUpdates {
    fn drop(&mut self) {
        if let Some((session, id)) = self.on_close.take() {
            if let Some(session) = session.upgrade() {
                session.remove_on_close(id);
            }
        }
    }
}

struct Subscriber {
    id: u64,
    request: LogSubscriptionRequest,
    sender: mpsc::Sender<LogSubscriptionData>,
    end: Arc<AtomicU8>,
}

struct State {
    history: VecDeque<LogSubscriptionData>,
    subscribers: Vec<Subscriber>,
    next_subscriber: u64,
    /// The time of the latest record, so that no record is older than the one before it.
    last_micros: i64,
}

/// See the [module documentation](self).
///
/// Behavior: `io.deephaven.io.logger.LogBuffer`
pub struct LogBuffer {
    state: Mutex<State>,
    history_size: usize,
    client_buffer_size: usize,
}

impl Default for LogBuffer {
    fn default() -> Self {
        Self::with_sizes(HISTORY_SIZE, CLIENT_BUFFER_SIZE)
    }
}

/// Whether a client that sent `request` is sent `record`.
///
/// Behavior: `ConsoleServiceGrpcImpl.LogsClient#record`
fn wanted(request: &LogSubscriptionRequest, record: &LogSubscriptionData) -> bool {
    (request.levels.is_empty() || request.levels.contains(&record.log_level))
        && record.micros > request.last_seen_log_timestamp
}

impl LogBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    /// A buffer that keeps `history_size` records, at least one, and ends a client that `client_buffer_size`
    /// records wait for. A client's buffer holds at least twice the history, so that the
    /// records it is sent when it subscribes cannot fill it.
    ///
    /// Behavior: `ConsoleServiceGrpcImpl.LogsClient#LogsClient`
    pub fn with_sizes(history_size: usize, client_buffer_size: usize) -> Self {
        Self {
            state: Mutex::new(State {
                history: VecDeque::new(),
                subscribers: Vec::new(),
                next_subscriber: 0,
                last_micros: 0,
            }),
            history_size: history_size.max(1),
            client_buffer_size: client_buffer_size.max(history_size * 2).max(1),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a record with the time of now and sends it to the clients that want it. A
    /// client whose buffer is full is ended as too slow.
    ///
    /// Behavior: `LogBuffer#record`, `ConsoleServiceGrpcImpl.LogsClient#enqueue`
    pub fn record(&self, level: &str, message: impl Into<String>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_micros() as i64);
        let mut state = self.lock();
        state.last_micros = now.max(state.last_micros);
        let record = LogSubscriptionData {
            micros: state.last_micros,
            log_level: level.to_string(),
            message: message.into(),
        };
        state.subscribers.retain(|subscriber| {
            if !wanted(&subscriber.request, &record) {
                return true;
            }
            match subscriber.sender.try_send(record.clone()) {
                Ok(()) => true,
                Err(TrySendError::Full(_)) => {
                    subscriber.end.store(TOO_SLOW, Ordering::Release);
                    false
                }
                // a client that cannot be sent to is gone
                Err(TrySendError::Closed(_)) => false,
            }
        });
        if state.history.len() >= self.history_size {
            state.history.pop_front();
        }
        state.history.push_back(record);
    }

    /// Starts a stream: the records that are kept, then each new one, as far as `request`
    /// wants them. The stream ends when the client stops reading it, or with
    /// `RESOURCE_EXHAUSTED` when the client reads too slowly.
    ///
    /// Behavior: `LogBuffer#subscribe`, which replays the history
    pub fn subscribe(&self, request: LogSubscriptionRequest) -> LogUpdates {
        self.add(request).1
    }

    /// As [`LogBuffer::subscribe`], for a client's call: the stream also ends, with
    /// `CANCELLED`, when `session` does.
    ///
    /// Behavior: `SessionServiceGrpcImpl.SessionServiceInterceptor#CANCEL_RPC_ON_SESSION_CLOSE`
    pub fn subscribe_for(
        self: &Arc<Self>,
        request: LogSubscriptionRequest,
        session: &Arc<Session>,
    ) -> Result<LogUpdates, Status> {
        let (id, mut updates) = self.add(request);
        let buffer = Arc::downgrade(self);
        let registered = session.add_on_close(move || {
            if let Some(buffer) = buffer.upgrade() {
                buffer.end(id, SESSION_CLOSED);
            }
        });
        match registered {
            Ok(on_close) => {
                updates.on_close = Some((Arc::downgrade(session), on_close));
                Ok(updates)
            }
            Err(status) => {
                self.end(id, OPEN);
                Err(status)
            }
        }
    }

    fn add(&self, request: LogSubscriptionRequest) -> (u64, LogUpdates) {
        let (sender, receiver) = mpsc::channel(self.client_buffer_size);
        let end = Arc::new(AtomicU8::new(OPEN));
        let mut state = self.lock();
        for record in &state.history {
            if wanted(&request, record) {
                // the buffer holds at least twice the history
                let _ = sender.try_send(record.clone());
            }
        }
        let id = state.next_subscriber;
        state.next_subscriber += 1;
        state.subscribers.push(Subscriber {
            id,
            request,
            sender,
            end: end.clone(),
        });
        let updates = LogUpdates {
            receiver,
            end,
            finished: false,
            on_close: None,
        };
        (id, updates)
    }

    /// Ends the stream of subscriber `id` for `reason`, if it has not ended yet.
    fn end(&self, id: u64, reason: u8) {
        let mut state = self.lock();
        if let Some(position) = state.subscribers.iter().position(|s| s.id == id) {
            let subscriber = state.subscribers.remove(position);
            subscriber.end.store(reason, Ordering::Release);
        }
    }

    /// The number of streams the buffer still sends to, for tests.
    #[cfg(test)]
    pub(super) fn subscriber_count(&self) -> usize {
        self.lock().subscribers.len()
    }
}

/// The widths of the thread and logger columns of a log record's text.
///
/// Behavior: the pattern `%-20.20thread | %-25.25logger{25} | %m` of the `LOGBUFFER`
/// appender in the Java server's `logback.xml`
const THREAD_WIDTH: usize = 20;
const LOGGER_WIDTH: usize = 25;

/// `text` in exactly `width` characters: the end of a longer text, a shorter one padded
/// on the right. This is what logback's `%-N.N` does.
fn fit(text: &str, width: usize) -> String {
    let length = text.chars().count();
    let tail: String = text.chars().skip(length.saturating_sub(width)).collect();
    format!("{tail:<width$}")
}

/// A module path shortened towards `width` characters by cutting its leading segments to
/// their first letter, one after the other, as logback's `%logger{N}` shortens the
/// packages of a class name: `oxhaven_server::console::python` is `o::c::python` at 12.
fn abbreviated(target: &str, width: usize) -> String {
    let mut segments: Vec<String> = target.split("::").map(str::to_string).collect();
    let last = segments.len().saturating_sub(1);
    for index in 0..last {
        if segments.join("::").chars().count() <= width {
            break;
        }
        segments[index] = segments[index].chars().take(1).collect();
    }
    segments.join("::")
}

/// The text of a log record: `thread | logger | message`.
fn layout(thread: &str, target: &str, message: &str) -> String {
    format!(
        "{} | {} | {message}",
        fit(thread, THREAD_WIDTH),
        fit(&abbreviated(target, LOGGER_WIDTH), LOGGER_WIDTH)
    )
}

/// Collects the message of an event, and its other fields as `name=value` after it.
#[derive(Default)]
struct EventText {
    message: String,
    fields: String,
}

impl Visit for EventText {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            let _ = write!(self.fields, " {}={value}", field.name());
        }
    }
}

/// See [`layer`].
pub struct LogLayer {
    buffer: Arc<LogBuffer>,
}

impl<S: tracing::Subscriber> Layer<S> for LogLayer {
    fn on_event(&self, event: &Event<'_>, _context: LayerContext<'_, S>) {
        let mut text = EventText::default();
        event.record(&mut text);
        text.message.push_str(&text.fields);
        let metadata = event.metadata();
        let thread = std::thread::current();
        self.buffer.record(
            // tracing's names for its levels are Java's
            metadata.level().as_str(),
            layout(
                thread.name().unwrap_or_default(),
                metadata.target(),
                &text.message,
            ),
        );
    }
}

/// A `tracing` layer that writes every event it is given into `buffer`, under the event's
/// level. Which events it is given is for the subscriber it is added to; the server
/// (`main.rs`) logs from `INFO` up, the root level of the Java server's configuration.
///
/// Behavior: `io.deephaven.logback.LogBufferAppender`
pub fn layer(buffer: Arc<LogBuffer>) -> LogLayer {
    LogLayer { buffer }
}
