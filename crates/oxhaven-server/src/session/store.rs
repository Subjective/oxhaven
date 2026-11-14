//! All sessions of the server: creating them from credentials, finding them by token,
//! rotating and expiring tokens, and telling clients that the server is going away.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use oxhaven_proto::grpc::TerminationNotificationResponse;
use tokio::sync::{mpsc, Notify};
use tonic::Status;
use uuid::Uuid;

use super::auth::{AuthContext, AuthHandlers, BEARER_PREFIX};
use super::executor::Executor;
use super::state::{Clock, Session, TokenExpiration};

/// The shortest token lifetime the server accepts.
///
/// Behavior: `io.deephaven.server.session.SessionService#MIN_COOKIE_EXPIRE_MS`
pub const MIN_TOKEN_EXPIRE_MS: i64 = 10_000;

/// The token lifetime of the reference server's default configuration
/// (`http.session.durationMs` in `dh-defaults.prop`).
pub const DEFAULT_TOKEN_EXPIRE_MS: i64 = 300_000;

/// The credentials of a call matched no session and no handler accepted them.
///
/// Behavior: `io.deephaven.auth.AuthenticationException`
#[derive(Debug, PartialEq, Eq)]
pub struct AuthenticationFailed;

/// The receiving end of a `TerminationNotification` subscription.
pub type TerminationNotifications =
    mpsc::UnboundedReceiver<Result<TerminationNotificationResponse, Status>>;

/// Emptied by whichever comes first, the shutdown notification or the end of the session, so
/// that the stream gets exactly one of the two endings.
type TerminationSender =
    Arc<Mutex<Option<mpsc::UnboundedSender<Result<TerminationNotificationResponse, Status>>>>>;

struct Token {
    deadline_millis: i64,
    session: Arc<Session>,
}

#[derive(Default)]
struct Tokens {
    by_token: HashMap<Uuid, Token>,
    /// Every token that has not been forgotten, by deadline; the sweep takes from the front.
    by_deadline: BTreeSet<(i64, Uuid)>,
}

impl Tokens {
    fn forget(&mut self, token: Uuid) -> Option<Token> {
        let forgotten = self.by_token.remove(&token)?;
        self.by_deadline.remove(&(forgotten.deadline_millis, token));
        Some(forgotten)
    }
}

/// Behavior: `io.deephaven.server.session.SessionService`
pub struct SessionStore {
    clock: Clock,
    executor: Arc<dyn Executor>,
    handlers: AuthHandlers,
    token_expire_ms: i64,
    /// A token younger than this is not replaced when the session is used.
    token_rotate_ms: i64,
    tokens: Mutex<Tokens>,
    termination_listeners: Mutex<Vec<TerminationSender>>,
    /// Wakes the cleanup task when a token is issued.
    token_issued: Notify,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// The server's wall clock.
pub fn system_clock() -> Clock {
    Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since_epoch| since_epoch.as_millis() as i64)
    })
}

impl SessionStore {
    /// Fails if `token_expire_ms` is below [`MIN_TOKEN_EXPIRE_MS`], or so large that a
    /// deadline could overflow.
    ///
    /// Behavior: the constructor of `io.deephaven.server.session.SessionService`
    pub fn new(
        clock: Clock,
        executor: Arc<dyn Executor>,
        handlers: AuthHandlers,
        token_expire_ms: i64,
    ) -> Result<Arc<Self>, String> {
        if token_expire_ms < MIN_TOKEN_EXPIRE_MS {
            return Err(format!(
                "session.tokenExpireMs is set too low. It is configured to {token_expire_ms}ms \
                 (minimum is {MIN_TOKEN_EXPIRE_MS}ms)."
            ));
        }
        if token_expire_ms >= i64::MAX / 1_000_000 {
            return Err("session.tokenExpireMs is set too high.".to_string());
        }
        Ok(Arc::new(SessionStore {
            clock,
            executor,
            handlers,
            token_expire_ms,
            // loose enough for any reasonable client, tight enough to stop rotation spam
            token_rotate_ms: token_expire_ms / 5,
            tokens: Mutex::default(),
            termination_listeners: Mutex::default(),
            token_issued: Notify::new(),
        }))
    }

    /// The configured token lifetime.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#getExpirationDelayMs`
    pub fn expiration_delay_ms(&self) -> i64 {
        self.token_expire_ms
    }

    pub fn auth_handlers(&self) -> &AuthHandlers {
        &self.handlers
    }

    /// Behavior: `io.deephaven.server.session.SessionService#newSession`
    pub fn new_session(&self, auth: AuthContext) -> Arc<Session> {
        let session = Session::new(auth, self.clock.clone(), self.executor.clone());
        self.rotate(&session, true);
        tracing::debug!("SessionState{{{}}}: session initialized", session.id());
        session
    }

    /// Gives the session a new token if its current one is old enough, and returns the
    /// current token; `None` if the session has expired.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#refreshToken`
    pub fn refresh_token(&self, session: &Arc<Session>) -> Option<TokenExpiration> {
        self.rotate(session, false)
    }

    /// Behavior: `io.deephaven.server.session.SessionService#checkTokenAndRotate`
    fn rotate(&self, session: &Arc<Session>, initial: bool) -> Option<TokenExpiration> {
        let now = (self.clock)();
        // Held to the end, so that the end of the session either precedes this (and no token
        // is issued) or follows it (and close_session forgets the new token).
        let mut slot = session.expiration_slot();
        if !initial {
            let current = (*slot).filter(|current| current.deadline_millis > now)?;
            if current.deadline_millis - self.token_expire_ms + self.token_rotate_ms > now {
                return Some(current);
            }
        }
        let deadline_millis = now + self.token_expire_ms;
        let mut tokens = lock(&self.tokens);
        let token = loop {
            let token = Uuid::new_v4();
            if !tokens.by_token.contains_key(&token) {
                break token;
            }
        };
        tokens.by_token.insert(
            token,
            Token {
                deadline_millis,
                session: session.clone(),
            },
        );
        tokens.by_deadline.insert((deadline_millis, token));
        drop(tokens);
        let expiration = TokenExpiration {
            token,
            deadline_millis,
        };
        *slot = Some(expiration);
        drop(slot);
        self.token_issued.notify_one();
        Some(expiration)
    }

    /// The session a token belongs to, if the token and the session are still good. Older
    /// tokens of a session keep working until their own deadlines, so that requests sent
    /// before a rotation was seen still authenticate.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#getSessionForToken`
    pub fn session_for_token(&self, token: Uuid) -> Option<Arc<Session>> {
        let (deadline_millis, session) = {
            let tokens = lock(&self.tokens);
            let found = tokens.by_token.get(&token)?;
            (found.deadline_millis, found.session.clone())
        };
        if session.is_expired() || deadline_millis <= (self.clock)() {
            return None;
        }
        Some(session)
    }

    /// The session for an `authorization` header value: an existing one for
    /// `Bearer <token>`, or a new one if a handler accepts `<auth type> <payload>`. The auth
    /// type may also follow `Bearer `, for clients that can only send bearer values.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#getSessionForAuthToken`
    pub fn session_for_auth_header(
        &self,
        value: &str,
    ) -> Result<Arc<Session>, AuthenticationFailed> {
        fn split(value: &str) -> (&str, &str) {
            value.split_once(' ').unwrap_or((value, ""))
        }
        let nested = value.strip_prefix(BEARER_PREFIX);
        if let Some(token) = nested {
            if let Some(session) = Uuid::parse_str(token)
                .ok()
                .and_then(|token| self.session_for_token(token))
            {
                return Ok(session);
            }
        }
        // the header's own type first, which lets a handler claim `Bearer`; then the type
        // nested in the bearer value
        for (auth_type, payload) in std::iter::once(split(value)).chain(nested.map(split)) {
            if let Some(auth) = self
                .handlers
                .get(auth_type)
                .and_then(|handler| handler.login_header(payload))
            {
                return Ok(self.new_session(auth));
            }
        }
        tracing::info!(
            "No AuthenticationRequestHandler registered for type {}",
            split(value).0
        );
        Err(AuthenticationFailed)
    }

    /// Ends a session now and forgets its tokens.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#closeSession`
    pub fn close_session(&self, session: &Arc<Session>) {
        session.on_expired();
        let mut tokens = lock(&self.tokens);
        let of_session: Vec<Uuid> = tokens
            .by_token
            .iter()
            .filter(|(_, token)| Arc::ptr_eq(&token.session, session))
            .map(|(token, _)| *token)
            .collect();
        for token in of_session {
            tokens.forget(token);
        }
    }

    /// Forgets every token whose deadline has passed and ends the sessions left without a
    /// live token. Returns the next deadline, if any token remains.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService.SessionCleanupJob#run`
    pub fn sweep(&self) -> Option<i64> {
        let now = (self.clock)();
        let mut lapsed = Vec::new();
        let next = {
            let mut tokens = lock(&self.tokens);
            loop {
                match tokens.by_deadline.first().copied() {
                    Some((deadline_millis, token)) if deadline_millis <= now => {
                        lapsed.extend(tokens.forget(token));
                    }
                    next => break next.map(|(deadline_millis, _)| deadline_millis),
                }
            }
        };
        for token in lapsed {
            // other tokens of the session may still be good; it ends with its newest one
            if token.session.is_expired() {
                token.session.on_expired();
            }
        }
        next
    }

    /// Sweeps whenever a token deadline passes, until the store is dropped.
    pub fn spawn_cleanup(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let store: Weak<SessionStore> = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let Some(store) = store.upgrade() else { return };
                let wait = match store.sweep() {
                    Some(deadline_millis) => {
                        Duration::from_millis((deadline_millis - (store.clock)()).max(1) as u64)
                    }
                    // nothing outstanding; look again in case the store has been dropped
                    None => Duration::from_secs(60),
                };
                let issued = store.token_issued.notified();
                tokio::pin!(issued);
                // register for the wake-up before letting go of the store
                issued.as_mut().enable();
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = &mut issued => {}
                }
            }
        })
    }

    /// Subscribes to the one notification sent when the server shuts down. The stream fails
    /// with `UNAUTHENTICATED` if the session ends first.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#addTerminationListener`,
    /// `SessionService.TerminationNotificationListener`
    pub fn add_termination_listener(
        &self,
        session: &Arc<Session>,
    ) -> Result<TerminationNotifications, Status> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let sender: TerminationSender = Arc::new(Mutex::new(Some(sender)));
        let on_close = sender.clone();
        session.add_on_close(move || {
            if let Some(sender) = lock(&on_close).take() {
                let _ = sender.send(Err(Status::unauthenticated("Session has ended")));
            }
        })?;
        let mut listeners = lock(&self.termination_listeners);
        // forget the listeners whose clients or sessions are gone
        listeners.retain(|listener| {
            lock(listener)
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
        });
        listeners.push(sender);
        Ok(receiver)
    }

    /// Tells every termination listener that the server is stopping normally, then ends
    /// every session.
    ///
    /// Behavior: `io.deephaven.server.session.SessionService#onShutdown`
    pub fn on_shutdown(&self) {
        let listeners = std::mem::take(&mut *lock(&self.termination_listeners));
        for listener in listeners {
            if let Some(sender) = lock(&listener).take() {
                let _ = sender.send(Ok(TerminationNotificationResponse {
                    abnormal_termination: false,
                    ..Default::default()
                }));
            }
        }
        let sessions: Vec<Arc<Session>> = lock(&self.tokens)
            .by_token
            .values()
            .map(|token| token.session.clone())
            .collect();
        for session in sessions {
            session.on_expired();
        }
    }
}
