//! The object service: how a client opens a server object that is not a table.
//!
//! `FetchObject` answers with the one message of an object that is only fetched.
//! `MessageStream` is a conversation with an object: the client's first message names it,
//! the object's type ([`types`]) answers and may go on answering, and each later message
//! of the client is handed to the object. `OpenMessageStream` and `NextMessageStream` are
//! the same stream for a browser that cannot keep a call open in both directions
//! (`console::browser`).
//!
//! What a client is answered in each case was observed on Java 42.9 with a plugin written
//! for the purpose; `docs/OBJECTS.md` has the cases, and the tests keep them.
//!
//! Behavior: `io.deephaven.server.object.ObjectServiceGrpcImpl`,
//! `io.deephaven.server.object.ObjectServiceGrpcBinding`

mod connection;
#[cfg(test)]
mod tests;
pub mod types;

use std::sync::{Arc, Mutex, PoisonError};

use oxhaven_proto::grpc::object_service_server::ObjectService;
use oxhaven_proto::grpc::stream_request::Message as ClientMessage;
use oxhaven_proto::grpc::{
    BrowserNextResponse, FetchObjectRequest, FetchObjectResponse, StreamRequest, StreamResponse,
    TypedTicket,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use self::connection::{ClientConnection, Ended, Outlet};
use self::types::{registry, MessageStream};
use crate::console::browser::{self, stream_data, BrowserStream};
use crate::session::{
    current_session, Export, ExportError, ExportValue, Reply, Session, TicketRouter,
};

pub use self::types::{send_once, Noop, ObjectType, ObjectTypes, TABLE_TYPE_NAME};

/// The type and ticket bytes of the object a request names.
///
/// Behavior: the checks at the start of `ObjectServiceGrpcImpl#fetchObject` and of a
/// connect request in `SendMessageObserver#onNext`
fn named(source: Option<TypedTicket>) -> Result<(String, Vec<u8>), Status> {
    let source = source.unwrap_or_default();
    if source.r#type.is_empty() {
        return Err(Status::invalid_argument("No type supplied"));
    }
    let ticket = source
        .ticket
        .map(|ticket| ticket.ticket)
        .unwrap_or_default();
    if ticket.is_empty() {
        return Err(Status::invalid_argument("No ticket supplied"));
    }
    Ok((source.r#type, ticket))
}

pub struct ObjectGrpc {
    router: Arc<TicketRouter>,
}

impl ObjectGrpc {
    pub fn new(router: Arc<TicketRouter>) -> Self {
        Self { router }
    }
}

/// What takes the client's messages, once the connect request has been answered.
type Opened = Arc<Mutex<Option<Arc<dyn MessageStream>>>>;

/// The work of one message of the client, to run once the exports it names have values.
type Work = Box<dyn FnOnce(Vec<ExportValue>) -> Result<(), ExportError> + Send>;

/// One `MessageStream` call.
struct Conversation {
    router: Arc<TicketRouter>,
    session: Arc<Session>,
    outlet: Arc<Outlet>,
    opened: Opened,
    seen_connect: bool,
}

impl Conversation {
    /// Checks a message and says what handling it needs. An error here is one Java throws
    /// out of `onNext`: the call ends with it, and the object is not told.
    ///
    /// Behavior: `SendMessageObserver#onNext`
    fn accept(&mut self, request: StreamRequest) -> Result<(Vec<Export>, Work), Status> {
        match request.message {
            None => Err(Status::invalid_argument(
                "io.deephaven.proto.backplane.grpc.StreamRequest must have oneof message. Note: \
                 this may also indicate that the server is older than the client and doesn't \
                 know about this new oneof option.",
            )),
            Some(ClientMessage::Connect(_)) if self.seen_connect => Err(
                Status::failed_precondition("Already sent a connect request, cannot send another"),
            ),
            Some(ClientMessage::Connect(connect)) => {
                self.seen_connect = true;
                let (expected, ticket) = named(connect.source_id)?;
                let object = self
                    .router
                    .resolve(Some(&self.session), &ticket, "sourceId")?;
                let (session, outlet) = (self.session.clone(), self.outlet.clone());
                let opened = self.opened.clone();
                let work: Work = Box::new(move |inputs| {
                    let object_type = registry().instance(&expected, &inputs[0])?;
                    let connection = Arc::new(ClientConnection::stream(session, outlet));
                    let stream = object_type.connect(&inputs[0], connection)?;
                    *opened.lock().unwrap_or_else(PoisonError::into_inner) = Some(stream);
                    Ok(())
                });
                Ok((vec![object], work))
            }
            Some(ClientMessage::Data(_)) if !self.seen_connect => Err(Status::invalid_argument(
                "Data message sent before Connect message",
            )),
            Some(ClientMessage::Data(data)) => {
                let mut references = Vec::with_capacity(data.references.len());
                for reference in data.references {
                    let ticket = reference.ticket.unwrap_or_default().ticket;
                    references.push(self.router.resolve(Some(&self.session), &ticket, "ticket")?);
                }
                let opened = self.opened.clone();
                let work: Work = Box::new(move |inputs| {
                    let stream = opened
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .clone();
                    match stream {
                        Some(stream) => stream.on_data(data.payload, inputs),
                        // the connect request failed, and with it the stream
                        None => Ok(()),
                    }
                });
                Ok((references, work))
            }
        }
    }

    /// Runs the work of one message when its exports have values, and waits for it, so
    /// that the messages of a stream are handled one after the other in their order.
    /// Returns early, with the work still waiting, when the client has gone.
    ///
    /// Behavior: `SendMessageObserver#runOrEnqueue`, `EnqueuedStreamOperation#run`
    async fn run(&self, dependencies: Vec<Export>, work: Work) -> Result<(), Status> {
        let reply = Reply::new();
        let (failed, done) = (reply.completer(), reply.completer());
        let outlet = self.outlet.clone();
        self.session
            .non_export()?
            .require(dependencies)
            .on_error(move |status| failed(Err(status)))
            .submit(move |inputs| {
                // the stream may have ended while this waited for its turn or its exports
                if outlet.ended().is_none() {
                    work(inputs)?;
                }
                done(Ok(()));
                Ok(ExportValue::unit())
            })?;
        let finished = reply.wait();
        tokio::pin!(finished);
        loop {
            tokio::select! {
                result = &mut finished => return result.map(|_| ()),
                ended = self.outlet.over() => {
                    if ended == Ended::Gone {
                        return Ok(());
                    }
                    // the object ended the stream from inside this work, or the session
                    // ended and fails the work: either way it finishes
                    return finished.await.map(|_| ());
                }
            }
        }
    }

    /// Tells the object that its stream is over, if the stream was opened. What the
    /// object does then is its own business: an error is ignored.
    ///
    /// Behavior: `SendMessageObserver#closeMessageStream`
    async fn close_opened(&self) {
        let stream = self
            .opened
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(stream) = stream {
            // the object may run Python
            let _ = tokio::task::spawn_blocking(move || stream.on_close()).await;
        }
    }

    /// Handles the client's messages until the stream is over.
    ///
    /// How a stream ends, and whether the object is told (`on_close`), as observed:
    ///
    /// | The end | The client sees | The object is told |
    /// | --- | --- | --- |
    /// | the client closes its side | the end of the stream, after the answers to its messages | yes |
    /// | the client cancels the call, or is gone | | yes |
    /// | the object closes the stream | the end of the stream | no |
    /// | handling a message fails (the object raises, an export fails) | the status | yes, if the stream was opened |
    /// | a message is refused (no message, data before connect, a second connect) | the status | no |
    /// | the session ends | `CANCELLED`, `Session closed` | no |
    ///
    /// Behavior: `SendMessageObserver`
    async fn converse<R>(mut self, mut requests: R)
    where
        R: Stream<Item = Result<StreamRequest, Status>> + Send + Unpin + 'static,
    {
        loop {
            let request = tokio::select! {
                biased;
                ended = self.outlet.over() => {
                    if ended == Ended::Gone {
                        self.close_opened().await;
                    }
                    return;
                }
                request = requests.next() => request,
            };
            let request = match request {
                Some(Ok(request)) => request,
                Some(Err(_)) => {
                    // the client's side failed: the call is over
                    self.outlet.gone();
                    self.close_opened().await;
                    return;
                }
                None => {
                    // Behavior: `SendMessageObserver#onCompleted`, which comes after the
                    // messages before it have been handled
                    self.outlet.complete();
                    self.close_opened().await;
                    return;
                }
            };
            let (dependencies, work) = match self.accept(request) {
                Ok(operation) => operation,
                Err(status) => {
                    self.outlet.fail(status);
                    return;
                }
            };
            if let Err(status) = self.run(dependencies, work).await {
                // Behavior: `SendMessageObserver#onError`
                self.outlet.fail(status);
                self.close_opened().await;
                return;
            }
        }
    }
}

/// The messages of one stream to its client, as the call's response. When the call is
/// over, however that came, the stream is: what the object sends from then on is refused.
pub struct Responses {
    responses: UnboundedReceiverStream<Result<StreamResponse, Status>>,
    outlet: Arc<Outlet>,
}

impl Stream for Responses {
    type Item = Result<StreamResponse, Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.responses).poll_next(context)
    }
}

impl Drop for Responses {
    fn drop(&mut self) {
        self.outlet.gone();
    }
}

impl ObjectGrpc {
    /// Starts a stream of `session` whose messages arrive as `requests`.
    ///
    /// Behavior: `ObjectServiceGrpcImpl#messageStream`; the end of the session ends the
    /// call as `io.deephaven.server.session.SessionServiceGrpcImpl.SessionServiceInterceptor`
    /// does
    fn serve<R>(
        &self,
        session: Arc<Session>,
        requests: R,
        browser: Option<Arc<BrowserStream<StreamRequest>>>,
    ) -> Result<Responses, Status>
    where
        R: Stream<Item = Result<StreamRequest, Status>> + Send + Unpin + 'static,
    {
        let (outlet, responses) = Outlet::new();
        let on_close = session.add_on_close({
            let outlet = outlet.clone();
            move || outlet.fail(Status::cancelled("Session closed"))
        })?;
        let conversation = Conversation {
            router: self.router.clone(),
            session: session.clone(),
            outlet: outlet.clone(),
            opened: Opened::default(),
            seen_connect: false,
        };
        tokio::spawn(async move {
            conversation.converse(requests).await;
            session.remove_on_close(on_close);
            if let Some(browser) = browser {
                // the stream is no longer an export, and a later message for it is refused
                browser.cancelled();
            }
        });
        Ok(Responses {
            responses: UnboundedReceiverStream::new(responses),
            outlet,
        })
    }
}

#[tonic::async_trait]
impl ObjectService for ObjectGrpc {
    /// Opens the object and answers with the message it sends, if it sends one and then
    /// closes; an object that keeps its stream open is for `MessageStream`.
    ///
    /// Behavior: `ObjectServiceGrpcImpl#fetchObject`
    async fn fetch_object(
        &self,
        request: Request<FetchObjectRequest>,
    ) -> Result<Response<FetchObjectResponse>, Status> {
        stream_data(request.metadata())?;
        let session = current_session(&request)?;
        let (expected, ticket) = named(request.into_inner().source_id)?;
        self.router.log_name(&ticket, "sourceId")?;
        let object = self.router.resolve(Some(&session), &ticket, "sourceId")?;
        let reply = Reply::new();
        let (fail, succeed) = (reply.completer(), reply.completer());
        let owner = session.clone();
        session
            .non_export()?
            .require([object])
            .on_error(move |status| fail(Err(status)))
            .submit(move |inputs| {
                let object_type = registry().instance(&expected, &inputs[0])?;
                let connection = Arc::new(ClientConnection::single(owner));
                object_type.connect(&inputs[0], connection.clone())?;
                let (data, closed) = connection.sent();
                let Some(data) = data else {
                    connection.on_close();
                    return Err(Status::invalid_argument(
                        "Plugin didn't send a response before returning from clientConnection()",
                    )
                    .into());
                };
                if !closed {
                    connection.on_close();
                    return Err(Status::invalid_argument(
                        "Plugin didn't close response, use MessageStream instead for this object",
                    )
                    .into());
                }
                succeed(Ok(FetchObjectResponse {
                    r#type: expected,
                    data: data.payload,
                    typed_export_ids: data.exported_references,
                }));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }

    type MessageStreamStream = Responses;

    /// Behavior: `ObjectServiceGrpcImpl#messageStream`
    async fn message_stream(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Self::MessageStreamStream>, Status> {
        stream_data(request.metadata())?;
        let session = current_session(&request)?;
        let responses = self.serve(session, request.into_inner(), None)?;
        Ok(Response::new(responses))
    }

    type OpenMessageStreamStream = Responses;

    /// `MessageStream` for a browser: this call carries the first message and its
    /// response is the response stream; the later messages come one per
    /// `NextMessageStream` call.
    ///
    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeOpen`, as
    /// `ObjectServiceGrpcBinding#bindService` sets it up
    async fn open_message_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::OpenMessageStreamStream>, Status> {
        let data = browser::open_headers(stream_data(request.metadata())?)?;
        let session = current_session(&request)?;
        let (stream, messages) = BrowserStream::open(&session, &data)?;
        let messages = UnboundedReceiverStream::new(messages).map(Ok);
        let responses = self.serve(session, messages, Some(stream.clone()))?;
        // the first message is in order by itself, so nothing refuses it
        stream.on_message(request.into_inner(), &data)?;
        Ok(Response::new(responses))
    }

    /// A later message of a stream that `OpenMessageStream` opened, named by the stream
    /// headers of the call.
    ///
    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeNext`
    async fn next_message_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        browser::next(request).await
    }
}
