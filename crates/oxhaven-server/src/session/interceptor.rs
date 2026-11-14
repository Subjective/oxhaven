//! Finds the session of each call from its headers, and returns the session's current token
//! with each response.
//!
//! This is a tower layer around every service of the server, so that a call to any service
//! can authenticate and every response refreshes the client's token.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use base64::Engine;
use http::header::{HeaderMap, HeaderValue, AUTHORIZATION, COOKIE, SET_COOKIE};
use tonic::Status;
use tower::{Layer, Service};
use uuid::Uuid;

use super::auth::authentication_details_invalid;
use super::state::Session;
use super::store::SessionStore;

/// The Flight Auth 1.0 header that carries a session token, as binary metadata.
///
/// Behavior: `org.apache.arrow.flight.auth.AuthConstants#TOKEN_NAME`
const AUTH_TOKEN_HEADER: &str = "auth-token-bin";
const AUTH_COOKIE_REQUEST_HEADER: &str = "x-deephaven-auth-cookie-request";
const AUTH_COOKIE: &str = "deephaven-auth-cookie";

/// The session of a call, as a request extension. Set on every call that reaches a service.
#[derive(Clone)]
struct CurrentSession(Option<Arc<Session>>);

/// The session of this call, if its headers named or created one and it has not expired.
///
/// Behavior: `io.deephaven.server.session.SessionService#getOptionalSession`
pub fn optional_session<T>(request: &tonic::Request<T>) -> Option<Arc<Session>> {
    request
        .extensions()
        .get::<CurrentSession>()
        .and_then(|current| current.0.clone())
        .filter(|session| !session.is_expired())
}

/// The session of this call, or `UNAUTHENTICATED`.
///
/// Behavior: `io.deephaven.server.session.SessionService#getCurrentSession`
pub fn current_session<T>(request: &tonic::Request<T>) -> Result<Arc<Session>, Status> {
    optional_session(request).ok_or_else(|| Status::new(tonic::Code::Unauthenticated, ""))
}

/// The last value of a header, which is the one gRPC Java reads.
fn last<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a HeaderValue> {
    headers.get_all(name).iter().next_back()
}

/// Behavior: `io.deephaven.server.session.AuthCookie#parseAuthCookie`
fn parse_auth_cookie(headers: &HeaderMap) -> Option<Uuid> {
    let cookie = last(headers, COOKIE.as_str())?.to_str().ok()?;
    match cookie.split('=').collect::<Vec<_>>()[..] {
        [AUTH_COOKIE, token] => Uuid::parse_str(token).ok(),
        _ => None,
    }
}

/// Looks for a session token in the Flight Auth 1.0 header, then in the auth cookie, then in
/// the `authorization` header, which can also carry credentials for a new session. A call
/// with none of them proceeds without a session; a call whose `authorization` header is
/// refused does not proceed.
///
/// Behavior: `SessionServiceGrpcImpl.SessionServiceInterceptor#interceptCall`
fn authenticate(store: &SessionStore, headers: &HeaderMap) -> Result<Option<Arc<Session>>, Status> {
    let flight_token = last(headers, AUTH_TOKEN_HEADER).and_then(|value| {
        let encoded = value.as_bytes();
        let unpadded = encoded
            .iter()
            .rposition(|&b| b != b'=')
            .map_or(&encoded[..0], |end| &encoded[..=end]);
        let token = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(unpadded)
            .ok()?;
        Uuid::parse_str(std::str::from_utf8(&token).ok()?).ok()
    });
    if let Some(session) = flight_token.and_then(|token| store.session_for_token(token)) {
        return Ok(Some(session));
    }
    if let Some(session) =
        parse_auth_cookie(headers).and_then(|token| store.session_for_token(token))
    {
        return Ok(Some(session));
    }
    match last(headers, AUTHORIZATION.as_str()) {
        None => Ok(None),
        Some(value) => store
            .session_for_auth_header(&String::from_utf8_lossy(value.as_bytes()))
            .map(Some)
            .map_err(|_| authentication_details_invalid()),
    }
}

/// Adds the session's token to the response headers, rotating it first if it is old enough.
/// A response that is only a status carries its status in the same header block, so this
/// covers Java's fallback of adding the token to the trailers.
///
/// Behavior: `SessionServiceGrpcImpl.InterceptedCall#addHeaders`
fn add_token(
    store: &SessionStore,
    session: &Arc<Session>,
    set_cookie: bool,
    headers: &mut HeaderMap,
) {
    let Some(expiration) = store.refresh_token(session) else {
        return;
    };
    if let Ok(bearer) = HeaderValue::from_str(&expiration.bearer()) {
        headers.insert(AUTHORIZATION, bearer);
    }
    if set_cookie {
        if let Ok(cookie) = HeaderValue::from_str(&format!("{AUTH_COOKIE}={}", expiration.token)) {
            headers.insert(SET_COOKIE, cookie);
        }
    }
}

/// Wraps the server's services with session lookup.
#[derive(Clone)]
pub struct SessionLayer {
    store: Arc<SessionStore>,
}

impl SessionLayer {
    pub fn new(store: Arc<SessionStore>) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for SessionLayer {
    type Service = SessionMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SessionMiddleware {
            store: self.store.clone(),
            inner,
        }
    }
}

#[derive(Clone)]
pub struct SessionMiddleware<S> {
    store: Arc<SessionStore>,
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for SessionMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<ReqBody>) -> Self::Future {
        // the instance that was polled ready is the one that takes the call
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let store = self.store.clone();
        Box::pin(async move {
            let session = match authenticate(&store, request.headers()) {
                Ok(session) => session,
                Err(status) => return Ok(status.into_http()),
            };
            let set_cookie = last(request.headers(), AUTH_COOKIE_REQUEST_HEADER)
                .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"true"));
            request
                .extensions_mut()
                .insert(CurrentSession(session.clone()));
            let mut response = inner.call(request).await?;
            if let Some(session) = &session {
                add_token(&store, session, set_cookie, response.headers_mut());
            }
            Ok(response)
        })
    }
}
