//! The log lines `ConsoleService/SubscribeToLogs` sends: what a script printed.
//!
//! A [`LogBuffer`] keeps the latest records and hands every record to the clients that
//! listen. A script session writes what its language prints to standard output and standard
//! error into it, under the levels `STDOUT` and `STDERR`, which is how a client sees the
//! output of a command: `ExecuteCommand` does not carry it.
//!
//! Java's buffer also holds the server's own log (every line twice: once under its level,
//! once as the line the console appender wrote to `STDERR`). This buffer holds what scripts
//! print and nothing else.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use oxhaven_proto::script::{LogSubscriptionData, LogSubscriptionRequest};
use tokio::sync::mpsc;
use tonic::Status;

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

/// The stream of one `SubscribeToLogs` call.
pub type LogUpdates = mpsc::UnboundedReceiver<Result<LogSubscriptionData, Status>>;

struct Subscriber {
    request: LogSubscriptionRequest,
    sender: mpsc::UnboundedSender<Result<LogSubscriptionData, Status>>,
}

#[derive(Default)]
struct State {
    history: VecDeque<LogSubscriptionData>,
    subscribers: Vec<Subscriber>,
    /// The time of the latest record, so that no record is older than the one before it.
    last_micros: i64,
}

/// See the [module documentation](self).
///
/// Behavior: `io.deephaven.io.logger.LogBuffer`
#[derive(Default)]
pub struct LogBuffer {
    state: Mutex<State>,
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

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a record with the time of now and sends it to the clients that want it.
    ///
    /// Behavior: `LogBuffer#record`
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
        // a client that cannot be sent to is gone
        state.subscribers.retain(|subscriber| {
            !wanted(&subscriber.request, &record)
                || subscriber.sender.send(Ok(record.clone())).is_ok()
        });
        if state.history.len() == HISTORY_SIZE {
            state.history.pop_front();
        }
        state.history.push_back(record);
    }

    /// Starts a stream: the records that are kept, then each new one, as far as `request`
    /// wants them. The stream ends when the client stops reading it.
    ///
    /// Behavior: `LogBuffer#subscribe`, which replays the history
    pub fn subscribe(&self, request: LogSubscriptionRequest) -> LogUpdates {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut state = self.lock();
        for record in &state.history {
            if wanted(&request, record) {
                let _ = sender.send(Ok(record.clone()));
            }
        }
        state.subscribers.push(Subscriber { request, sender });
        receiver
    }
}
