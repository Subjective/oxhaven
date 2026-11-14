//! The `SessionService` gRPC service.

use std::sync::{Arc, Mutex};

use oxhaven_proto::grpc::session_service_server::SessionService;
use oxhaven_proto::grpc::{
    CloseSessionResponse, ExportNotification, ExportNotificationRequest, ExportRequest,
    ExportResponse, HandshakeRequest, HandshakeResponse, PublishRequest, PublishResponse,
    ReleaseRequest, ReleaseResponse, TerminationNotificationRequest,
    TerminationNotificationResponse,
};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use super::interceptor::current_session;
use super::router::{pass_through, TicketRouter};
use super::state::TokenExpiration;
use super::store::SessionStore;
use super::ticket::ticket_to_export_id;

/// The name of the header that carries the session token.
const AUTHORIZATION_HEADER: &str = "authorization";

type ReplySender<T> = Arc<Mutex<Option<oneshot::Sender<Result<T, Status>>>>>;

/// The two ends of a unary reply that export handlers complete: the first handler to run
/// decides the reply.
pub struct Reply<T> {
    sender: ReplySender<T>,
    receiver: oneshot::Receiver<Result<T, Status>>,
}

impl<T: Send + 'static> Reply<T> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (sender, receiver) = oneshot::channel();
        Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver,
        }
    }

    /// A handler that completes the reply with `result`.
    pub fn completer(&self) -> impl FnOnce(Result<T, Status>) + Send + 'static {
        let sender = self.sender.clone();
        move |result| {
            let sender = sender.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some(sender) = sender {
                let _ = sender.send(result);
            }
        }
    }

    /// Waits for a handler to complete the reply.
    pub async fn wait(self) -> Result<Response<T>, Status> {
        drop(self.sender);
        match self.receiver.await {
            Ok(result) => result.map(Response::new),
            // every handler was dropped without running
            Err(_) => Err(Status::cancelled("Export ended without a result")),
        }
    }
}

/// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl`
pub struct SessionGrpc {
    store: Arc<SessionStore>,
    router: Arc<TicketRouter>,
}

impl SessionGrpc {
    pub fn new(store: Arc<SessionStore>, router: Arc<TicketRouter>) -> Self {
        Self { store, router }
    }

    // the whole message is deprecated along with the two calls that return it
    #[allow(deprecated)]
    fn handshake_response(&self, expiration: TokenExpiration) -> HandshakeResponse {
        HandshakeResponse {
            metadata_header: AUTHORIZATION_HEADER.as_bytes().to_vec(),
            session_token: expiration.bearer().into_bytes(),
            token_deadline_time_millis: expiration.deadline_millis,
            token_expiration_delay_millis: self.store.expiration_delay_ms(),
        }
    }
}

#[allow(deprecated)]
fn check_protocol(request: &HandshakeRequest) -> Result<(), Status> {
    if request.auth_protocol != 0 {
        return Err(Status::invalid_argument("Protocol version not allowed."));
    }
    Ok(())
}

#[tonic::async_trait]
impl SessionService for SessionGrpc {
    /// Refused. In the reference this deprecated call creates a superuser session for any
    /// caller, without credentials, whatever authentication the server is configured with.
    /// Porting that would let a caller bypass the pre-shared key, so it is left out until
    /// the owner decides otherwise; clients authenticate through the `authorization` header
    /// or the Flight handshake.
    ///
    /// Behavior (not ported): `io.deephaven.server.session.SessionServiceGrpcImpl#newSession`
    async fn new_session(
        &self,
        _request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        Err(Status::unauthenticated(
            "NewSession is not supported; authenticate with the authorization header or the \
             Flight handshake",
        ))
    }

    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#refreshSessionToken`
    async fn refresh_session_token(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        check_protocol(request.get_ref())?;
        let session = current_session(&request)?;
        let expiration = self
            .store
            .refresh_token(&session)
            .ok_or_else(|| Status::unauthenticated("session has expired"))?;
        Ok(Response::new(self.handshake_response(expiration)))
    }

    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#closeSession`
    async fn close_session(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<CloseSessionResponse>, Status> {
        check_protocol(request.get_ref())?;
        let session = current_session(&request)?;
        self.store.close_session(&session);
        Ok(Response::new(CloseSessionResponse {}))
    }

    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#release`
    async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let session = current_session(&request)?;
        let id = request
            .into_inner()
            .id
            .ok_or_else(|| Status::invalid_argument("Release ticket not supplied"))?;
        let export = session
            .get_export_if_exists(ticket_to_export_id(&id.ticket, "id")?)?
            .ok_or_else(|| Status::unavailable("Export not yet defined"))?;
        // an export already in a terminal state has nothing left to release
        export.cancel();
        Ok(Response::new(ReleaseResponse {}))
    }

    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#exportFromTicket`
    async fn export_from_ticket(
        &self,
        request: Request<ExportRequest>,
    ) -> Result<Response<ExportResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let source_id = request
            .source_id
            .ok_or_else(|| Status::invalid_argument("Source ticket not supplied"))?;
        let result_id = request
            .result_id
            .ok_or_else(|| Status::invalid_argument("Result ticket not supplied"))?;
        // Java names the source in the call's description before resolving it
        self.router.log_name(&source_id.ticket, "sourceId")?;
        let source = self
            .router
            .resolve(Some(&session), &source_id.ticket, "sourceId")?;

        let reply = Reply::new();
        let on_error = reply.completer();
        let on_success = reply.completer();
        session
            .new_export_for_ticket(&result_id.ticket, "resultId")?
            .require([source])
            .on_error(move |status| on_error(Err(status)))
            .on_success(move |_| on_success(Ok(ExportResponse {})))
            .submit(pass_through)?;
        reply.wait().await
    }

    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#publishFromTicket`
    async fn publish_from_ticket(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let session = current_session(&request)?;
        let request = request.into_inner();
        let source_id = request
            .source_id
            .ok_or_else(|| Status::invalid_argument("Source ticket not supplied"))?;
        let result_id = request
            .result_id
            .ok_or_else(|| Status::invalid_argument("Result ticket not supplied"))?;
        self.router.log_name(&source_id.ticket, "sourceId")?;
        let source = self
            .router
            .resolve(Some(&session), &source_id.ticket, "sourceId")?;

        let reply = Reply::new();
        let on_publish = reply.completer();
        let on_error = reply.completer();
        self.router.publish_from(
            &session,
            &result_id.ticket,
            "resultId",
            Some(Box::new(move || on_publish(Ok(PublishResponse {})))),
            move |status| on_error(Err(status)),
            source,
        )?;
        reply.wait().await
    }

    type ExportNotificationsStream = UnboundedReceiverStream<Result<ExportNotification, Status>>;

    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#exportNotifications`
    async fn export_notifications(
        &self,
        request: Request<ExportNotificationRequest>,
    ) -> Result<Response<Self::ExportNotificationsStream>, Status> {
        let session = current_session(&request)?;
        Ok(Response::new(UnboundedReceiverStream::new(
            session.add_export_listener()?,
        )))
    }

    /// Answers when the server shuts down, or fails with `UNAUTHENTICATED` when the session
    /// ends first.
    ///
    /// Behavior: `io.deephaven.server.session.SessionServiceGrpcImpl#terminationNotification`
    async fn termination_notification(
        &self,
        request: Request<TerminationNotificationRequest>,
    ) -> Result<Response<TerminationNotificationResponse>, Status> {
        let session = current_session(&request)?;
        let mut notifications = self.store.add_termination_listener(&session)?;
        match notifications.recv().await {
            Some(notification) => notification.map(Response::new),
            None => Err(Status::cancelled("Session closed")),
        }
    }
}
