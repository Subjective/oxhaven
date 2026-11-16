//! The server's end of a connection to a client: what an object type sends through.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use oxhaven_proto::grpc::stream_response::Message as ServerMessage;
use oxhaven_proto::grpc::{ServerData, StreamResponse, TypedTicket};
use tokio::sync::{mpsc, watch};
use tonic::Status;

use super::types::{registry, MessageStream};
use crate::session::{ExportError, ExportValue, Session};

/// How a stream came to its end.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Ended {
    /// The object or the client closed it, and the client was told so.
    Completed,
    /// The client was sent a status.
    Failed,
    /// The call is over: the client cancelled it or went away.
    Gone,
}

type Responses = mpsc::UnboundedSender<Result<StreamResponse, Status>>;

/// Where the messages of one stream go to its client, until the stream ends. The first
/// end counts.
pub(super) struct Outlet {
    responses: Mutex<Option<Responses>>,
    ended: watch::Sender<Option<Ended>>,
}

impl Outlet {
    pub(super) fn new() -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<Result<StreamResponse, Status>>,
    ) {
        let (responses, receiver) = mpsc::unbounded_channel();
        let outlet = Arc::new(Self {
            responses: Mutex::new(Some(responses)),
            ended: watch::Sender::new(None),
        });
        (outlet, receiver)
    }

    fn lock(&self) -> MutexGuard<'_, Option<Responses>> {
        self.responses
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn end(&self, how: Ended, last: Option<Status>) {
        let Some(responses) = self.lock().take() else {
            return;
        };
        if let Some(status) = last {
            let _ = responses.send(Err(status));
        }
        drop(responses);
        self.ended.send_replace(Some(how));
    }

    /// Sends a message, unless the stream has ended.
    fn send(&self, response: StreamResponse) -> Result<(), Ended> {
        match self.lock().as_ref() {
            Some(responses) => responses.send(Ok(response)).map_err(|_| Ended::Gone),
            None => Err(self.ended().unwrap_or(Ended::Gone)),
        }
    }

    pub(super) fn complete(&self) {
        self.end(Ended::Completed, None);
    }

    pub(super) fn fail(&self, status: Status) {
        self.end(Ended::Failed, Some(status));
    }

    pub(super) fn gone(&self) {
        self.end(Ended::Gone, None);
    }

    pub(super) fn ended(&self) -> Option<Ended> {
        *self.ended.borrow()
    }

    /// Resolves when the stream has ended, with how.
    pub(super) async fn over(&self) -> Ended {
        let mut ended = self.ended.subscribe();
        let seen = ended.wait_for(Option::is_some).await;
        // the sender lives as long as `self`
        seen.ok().and_then(|ended| *ended).unwrap_or(Ended::Gone)
    }
}

#[derive(Default)]
struct Single {
    data: Option<ServerData>,
    closed: bool,
}

enum Sink {
    /// A `MessageStream` call.
    Stream(Arc<Outlet>),
    /// A `FetchObject` call, which answers with the last message sent.
    Single(Mutex<Single>),
}

/// What an object type sends to one client with. Every object a message refers to is
/// exported to the client's session under an id the server chooses, and the message holds
/// the tickets with the type names the client can open them by.
///
/// Behavior: `ObjectServiceGrpcImpl.PluginMessageSender`
pub(super) struct ClientConnection {
    session: Arc<Session>,
    sink: Sink,
}

impl ClientConnection {
    pub(super) fn stream(session: Arc<Session>, outlet: Arc<Outlet>) -> Self {
        Self {
            session,
            sink: Sink::Stream(outlet),
        }
    }

    pub(super) fn single(session: Arc<Session>) -> Self {
        Self {
            session,
            sink: Sink::Single(Mutex::default()),
        }
    }

    /// For a `FetchObject` call: the last message sent, and whether the object closed.
    pub(super) fn sent(&self) -> (Option<ServerData>, bool) {
        match &self.sink {
            Sink::Stream(_) => (None, false),
            Sink::Single(single) => {
                let single = single.lock().unwrap_or_else(PoisonError::into_inner);
                (single.data.clone(), single.closed)
            }
        }
    }
}

/// The text of the exception an object type gets from Java when it sends on a stream that
/// has ended, as Python shows it (without the stack).
fn ended_text(ended: Ended) -> &'static str {
    match ended {
        Ended::Completed | Ended::Failed => {
            "io.deephaven.plugin.type.ObjectCommunicationException: \
             java.lang.IllegalStateException: Stream is already completed, no further calls are \
             allowed"
        }
        Ended::Gone => {
            "io.deephaven.plugin.type.ObjectCommunicationException: \
             io.grpc.StatusRuntimeException: CANCELLED: call already cancelled. Use \
             ServerCallStreamObserver.setOnCancelHandler() to disable this exception"
        }
    }
}

impl MessageStream for ClientConnection {
    /// Behavior: `PluginMessageSender#onData`, which exports the references before it
    /// sends, and releases them again when the message cannot be sent
    fn on_data(&self, payload: Vec<u8>, references: Vec<ExportValue>) -> Result<(), ExportError> {
        let mut exports = Vec::with_capacity(references.len());
        let sent = (|| {
            let mut exported_references = Vec::with_capacity(references.len());
            for reference in references {
                // an object no type knows is exported without a type name: the client
                // cannot open it, and can hand it back
                let type_name = registry().type_name(&reference)?.unwrap_or_default();
                let export = self.session.new_server_side_export(reference)?;
                exported_references.push(TypedTicket {
                    ticket: Some(export.ticket()),
                    r#type: type_name,
                });
                exports.push(export);
            }
            let data = ServerData {
                payload,
                exported_references,
            };
            match &self.sink {
                Sink::Stream(outlet) => outlet
                    .send(StreamResponse {
                        message: Some(ServerMessage::Data(data)),
                    })
                    .map_err(|ended| ExportError::Internal(ended_text(ended).to_string())),
                Sink::Single(single) => {
                    single.lock().unwrap_or_else(PoisonError::into_inner).data = Some(data);
                    Ok(())
                }
            }
        })();
        if sent.is_err() {
            for export in exports {
                export.cancel();
            }
        }
        sent
    }

    /// Behavior: `PluginMessageSender#onClose`
    fn on_close(&self) {
        match &self.sink {
            Sink::Stream(outlet) => outlet.complete(),
            Sink::Single(single) => {
                single.lock().unwrap_or_else(PoisonError::into_inner).closed = true;
            }
        }
    }
}
