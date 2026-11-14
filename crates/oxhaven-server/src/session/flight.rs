//! The Flight `Handshake` call, which is the other way a client authenticates. The Flight
//! service that carries it is `crate::flight::FlightGrpc`.

use std::sync::Arc;

use oxhaven_proto::flight::{HandshakeRequest, HandshakeResponse};
use oxhaven_proto::grpc::WrappedAuthenticationRequest;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Status, Streaming};
use uuid::Uuid;

use super::auth::{authentication_details_invalid, BEARER_PREFIX};
use super::interceptor::optional_session;
use super::state::TokenExpiration;
use super::store::SessionStore;

/// The response stream of [`handshake`].
pub type HandshakeStream = UnboundedReceiverStream<Result<HandshakeResponse, Status>>;

/// What one handshake message leads to.
#[derive(Debug, PartialEq, Eq)]
enum Login {
    /// Reply with this token and finish.
    Token(TokenExpiration),
    /// Fail the call.
    Refused,
    /// Say nothing; a bearer token that names no live session gets no reply.
    Ignored,
}

/// Behavior: `FlightServiceGrpcImpl.HandshakeObserver#onNext`. The reference first offers
/// the payload to a `Basic` handler; none exists for anonymous or pre-shared key
/// authentication, so that step is absent here.
fn login(store: &SessionStore, payload: &[u8]) -> Login {
    let Ok(wrapped) = WrappedAuthenticationRequest::decode(payload) else {
        return Login::Refused;
    };
    if wrapped.r#type == BEARER_PREFIX.trim() {
        // a client on Flight Auth 1.0 may be renewing an existing session
        let token = std::str::from_utf8(&wrapped.payload)
            .ok()
            .and_then(|token| Uuid::parse_str(token).ok());
        if let Some(token) = token {
            return store
                .session_for_token(token)
                .and_then(|session| session.expiration())
                .map_or(Login::Ignored, Login::Token);
        }
    }
    store
        .auth_handlers()
        .get(&wrapped.r#type)
        .and_then(|handler| handler.login_handshake(&wrapped.payload))
        .and_then(|auth| store.new_session(auth).expiration())
        .map_or(Login::Refused, Login::Token)
}

/// Authenticates over a Flight handshake. Each message carries a
/// `WrappedAuthenticationRequest`; the reply to the first accepted one is the session token,
/// without the `Bearer ` prefix, after which the call completes.
///
/// A call that already authenticated through its headers gets no message, because some
/// clients break on one, and completes when the client does; the token is in the response
/// headers.
///
/// Behavior: `io.deephaven.server.arrow.FlightServiceGrpcImpl#handshake`,
/// `FlightServiceGrpcImpl.HandshakeObserver`
pub fn handshake(
    store: Arc<SessionStore>,
    request: Request<Streaming<HandshakeRequest>>,
) -> HandshakeStream {
    let authenticated = optional_session(&request).is_some();
    let mut messages = request.into_inner();
    let (responses, stream) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let message = match messages.message().await {
                Ok(Some(message)) => message,
                Ok(None) if authenticated => return,
                Ok(None) => {
                    let _ = responses.send(Err(Status::unauthenticated(
                        "no authentication details provided",
                    )));
                    return;
                }
                // the client is gone
                Err(_) => return,
            };
            if authenticated {
                continue;
            }
            match login(&store, &message.payload) {
                Login::Token(expiration) => {
                    let _ = responses.send(Ok(HandshakeResponse {
                        protocol_version: 0,
                        payload: expiration.token.to_string().into_bytes().into(),
                    }));
                    return;
                }
                Login::Refused => {
                    let _ = responses.send(Err(authentication_details_invalid()));
                    return;
                }
                Login::Ignored => {}
            }
        }
    });
    UnboundedReceiverStream::new(stream)
}
