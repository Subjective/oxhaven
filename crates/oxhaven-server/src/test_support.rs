//! A server on a local port for tests that speak gRPC to it.

use crate::session::{AuthHandler, AuthHandlers, Session, DEFAULT_TOKEN_EXPIRE_MS, PSK_AUTH_TYPE};
use crate::{serve, ServerConfig, ServerState};
use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::HandshakeRequest;
use std::sync::Arc;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic::Request;

pub(crate) const PSK: &str = "test-key";

/// Certificates made for a test run and a TLS relay, shared with `tests/tls_system_roots.rs`.
#[cfg(feature = "tls")]
#[path = "../tests/tls/support.rs"]
pub(crate) mod tls;

/// A running server with pre-shared key and anonymous authentication. Its update graph
/// runs a cycle only when a test asks: `server.state.graph.run_cycle()`.
pub(crate) struct TestServer {
    pub(crate) state: ServerState,
    pub(crate) channel: Channel,
    /// Where the server listens.
    pub(crate) address: std::net::SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
}

impl TestServer {
    pub(crate) async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    /// As [`TestServer::start`], with `configure` run on the state before it is served.
    pub(crate) async fn start_with(configure: impl FnOnce(&mut ServerState)) -> Self {
        Self::start_expiring(DEFAULT_TOKEN_EXPIRE_MS, configure).await
    }

    /// As [`TestServer::start_with`], with session tokens that last `token_expire_ms`.
    pub(crate) async fn start_expiring(
        token_expire_ms: i64,
        configure: impl FnOnce(&mut ServerState),
    ) -> Self {
        let mut state = ServerState::new(ServerConfig {
            auth_handlers: AuthHandlers::new(vec![
                AuthHandler::psk(PSK).unwrap(),
                AuthHandler::Anonymous,
            ])
            .unwrap(),
            token_expire_ms,
            target_cycle_duration: None,
        })
        .unwrap();
        configure(&mut state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, stopped) = oneshot::channel::<()>();
        tokio::spawn({
            let state = state.clone();
            async move {
                serve(state, listener, async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
            }
        });
        let channel = Channel::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            state,
            channel,
            address,
            shutdown: Some(shutdown),
        }
    }

    /// Logs in with the pre-shared key and returns the bearer header value.
    pub(crate) async fn login(&self) -> String {
        #[allow(deprecated)]
        let response = SessionServiceClient::new(self.channel.clone())
            .refresh_session_token(with_auth(
                HandshakeRequest::default(),
                &format!("{PSK_AUTH_TYPE} {PSK}"),
            ))
            .await
            .unwrap();
        response
            .metadata()
            .get("authorization")
            .expect("a bearer token in the response headers")
            .to_str()
            .unwrap()
            .to_string()
    }

    pub(crate) fn session(&self, bearer: &str) -> Arc<Session> {
        self.state
            .sessions
            .session_for_auth_header(bearer)
            .expect("a live session for the bearer token")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

pub(crate) fn with_auth<T>(message: T, authorization: &str) -> Request<T> {
    let mut request = Request::new(message);
    request
        .metadata_mut()
        .insert("authorization", authorization.parse().unwrap());
    request
}
