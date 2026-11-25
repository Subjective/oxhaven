//! The server's one port.
//!
//! Java serves everything from one Jetty port, and so does this module: native gRPC over
//! HTTP/2 without TLS (pydeephaven, the Java client), HTTP/1.1 for the page load of the web
//! IDE, the websocket that the browser then makes its gRPC calls on, and gRPC-web for
//! browsers on an `https://` page behind a proxy that terminates TLS. With a certificate
//! ([`serve_tls`], [`tls`]) the same port serves all of it over TLS, except the websocket,
//! which Java leaves out there: a browser on the `https://` page makes its calls as
//! gRPC-web over HTTP/2.
//!
//! A request goes to one of four places:
//!
//! 1. A websocket handshake at `/{service}/{method}` becomes a gRPC transport
//!    ([`websocket`]).
//! 2. A request with a gRPC content type goes to the services, through the gRPC-web
//!    translation if it asks for it ([`browser`]).
//! 3. A CORS preflight is answered here ([`browser`]).
//! 4. Anything else is a static file of the web root ([`crate::web`]).
//!
//! Every gRPC call, however it arrives, reaches the same service stack: the session layer
//! around the services of [`crate::ServerState::routes`].
//!
//! Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer`

mod browser;
pub mod tls;
mod websocket;

#[cfg(test)]
mod tests;
#[cfg(all(test, feature = "tls"))]
mod tls_tests;

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use http::header::{HeaderValue, ALLOW, ORIGIN};
use http::{Method, Request, Response, StatusCode, Version};
use http_body_util::Empty;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tower::util::BoxCloneSyncService;
use tower::Layer;

use crate::session::SessionLayer;
use crate::web::WebRoot;
use crate::ServerState;
pub use tls::{ClientAuth, TlsAcceptor, TlsConfig};
use websocket::WebsocketContext;

/// The body of every request to the services and of every response of the server.
pub(crate) type Body = tonic::body::Body;

/// The services behind the session layer, as native gRPC over HTTP/2 addresses them.
pub(crate) type GrpcService = BoxCloneSyncService<Request<Body>, Response<Body>, Infallible>;

/// The HTTP methods the server takes; any other is answered `405`.
///
/// Behavior: `io.deephaven.server.jetty.JettyConfig#DEFAULT_ALLOWED_METHODS`,
/// `AllowedHttpMethodsCustomizer#customize`
const ALLOWED_METHODS: [Method; 3] = [Method::GET, Method::POST, Method::OPTIONS];

/// How long open websockets get to finish after the server has begun to shut down.
const WEBSOCKET_SHUTDOWN_WAIT: Duration = Duration::from_secs(6);

pub(crate) fn empty_response(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::new(Empty::new()));
    *response.status_mut() = status;
    response
}

/// Where requests go.
struct Front {
    grpc: GrpcService,
    web: Option<Arc<WebRoot>>,
    websockets: WebsocketContext,
    /// The server's TLS, when it serves TLS: requests must then name a host its
    /// certificate names, and there are no websockets.
    tls: Option<Arc<TlsAcceptor>>,
}

impl Front {
    /// Answers a request that arrived on a connection to `local`. An error is a stream
    /// that is reset instead of answered, which only a gRPC call over HTTP/2 can be: see
    /// [`browser::call`].
    async fn handle(
        &self,
        request: Request<Incoming>,
        local: SocketAddr,
    ) -> Result<Response<Body>, h2::Error> {
        if let Some(tls) = &self.tls {
            if !tls.host_matches(&request, local) {
                return Ok(tls::invalid_sni(&request));
            }
        }
        let origin = request.headers().get(ORIGIN).cloned();
        let preflight = browser::is_preflight(&request);
        let mut response = if preflight {
            browser::preflight()
        } else {
            self.route(request).await?
        };
        browser::add_response_headers(&mut response, origin.as_ref(), preflight);
        Ok(response)
    }

    async fn route(&self, request: Request<Incoming>) -> Result<Response<Body>, h2::Error> {
        if !ALLOWED_METHODS.contains(request.method()) {
            return Ok(crate::web::error(StatusCode::METHOD_NOT_ALLOWED));
        }
        // Java's default `http.websockets` is none over TLS (`JettyConfig#websocketsOrDefault`):
        // the upgrade is then a request for a file, which is not found
        if self.tls.is_none() && websocket::is_upgrade(&request) {
            return Ok(websocket::accept(self.websockets.clone(), request));
        }
        if browser::is_grpc(request.headers()) {
            if request.version() != Version::HTTP_2 {
                return Ok(browser::not_http2(request.headers()));
            }
            return browser::call(self.grpc.clone(), request.map(Body::new)).await;
        }
        Ok(match *request.method() {
            Method::GET => {
                let plugins = crate::web::plugins::registry();
                crate::web::get(self.web.as_deref(), plugins, &request).await
            }
            Method::OPTIONS => {
                // Java's file servlet answers this, and names methods of its own
                let mut response = empty_response(StatusCode::OK);
                response
                    .headers_mut()
                    .insert(ALLOW, HeaderValue::from_static("GET, HEAD, OPTIONS"));
                response
            }
            _ => crate::web::error(StatusCode::METHOD_NOT_ALLOWED),
        })
    }
}

/// Serves `state` on `listener`, without TLS, until `shutdown` completes: gRPC over HTTP/2,
/// and over HTTP/1.1 the static files, the gRPC websocket and CORS preflights. Clients with
/// a termination subscription are then told, every session is ended, and open calls are
/// given the chance to finish.
pub(crate) async fn serve(
    state: ServerState,
    listener: TcpListener,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    serve_with(state, listener, None, shutdown).await
}

/// As [`crate::serve`], over TLS with `tls`: a connection speaks HTTP/2 when its client
/// agrees to `h2` by ALPN, otherwise HTTP/1.1, and the browser makes its gRPC calls as
/// gRPC-web over HTTP/2, without websockets ([`tls`]).
///
/// Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer#createConnector`
pub async fn serve_tls(
    state: ServerState,
    listener: TcpListener,
    tls: Arc<TlsAcceptor>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    serve_with(state, listener, Some(tls), shutdown).await
}

async fn serve_with(
    state: ServerState,
    listener: TcpListener,
    tls: Option<Arc<TlsAcceptor>>,
    shutdown: impl Future<Output = ()>,
) -> std::io::Result<()> {
    let grpc: GrpcService =
        BoxCloneSyncService::new(SessionLayer::new(state.sessions.clone()).layer(state.routes()));
    let (closing, closing_seen) = watch::channel(false);
    let (alive, mut all_closed) = mpsc::channel::<()>(1);
    let front = Arc::new(Front {
        grpc: grpc.clone(),
        web: state.web_root.clone(),
        websockets: WebsocketContext {
            grpc,
            closing: closing_seen,
            alive,
        },
        tls: tls.clone(),
    });
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder.http1().timer(TokioTimer::new());
    builder.http2().timer(TokioTimer::new());
    let graceful = GracefulShutdown::new();
    let mut shutdown = pin!(shutdown);
    loop {
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => {
                    // out of file descriptors, or the peer was gone already: keep serving
                    tracing::warn!("accept failed: {error}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = &mut shutdown => break,
        };
        let _ = stream.set_nodelay(true);
        let Ok(local) = stream.local_addr() else {
            continue;
        };
        let front = front.clone();
        let service = service_fn(move |request| {
            let front = front.clone();
            async move { front.handle(request, local).await }
        });
        let watcher = graceful.watcher();
        match &tls {
            None => {
                let connection = watcher.watch(
                    builder
                        .serve_connection_with_upgrades(TokioIo::new(stream), service)
                        .into_owned(),
                );
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        tracing::debug!("connection ended: {error}");
                    }
                });
            }
            Some(tls) => {
                let (tls, builder) = (tls.clone(), builder.clone());
                tokio::spawn(async move {
                    serve_tls_connection(&tls, stream, builder, service, watcher).await
                });
            }
        }
    }
    drop(listener);
    state.sessions.on_shutdown();
    let _ = closing.send(true);
    graceful.shutdown().await;
    // every websocket holds a sender; the channel closes when the last one is gone
    drop(front);
    let _ = tokio::time::timeout(WEBSOCKET_SHUTDOWN_WAIT, all_closed.recv()).await;
    Ok(())
}

/// How long a client has to finish its TLS handshake: Jetty's idle timeout of a connector.
///
/// Behavior: `org.eclipse.jetty.server.AbstractConnector` (an idle timeout of 30 seconds)
#[cfg(feature = "tls")]
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// The handshake of a TLS connection, then the connection in the protocol it agreed to.
#[cfg(feature = "tls")]
async fn serve_tls_connection<S>(
    tls: &TlsAcceptor,
    stream: tokio::net::TcpStream,
    builder: auto::Builder<TokioExecutor>,
    service: S,
    watcher: hyper_util::server::graceful::Watcher,
) where
    S: hyper::service::Service<Request<Incoming>, Response = Response<Body>, Error = h2::Error>
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let accepted = tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(stream)).await;
    let (stream, protocol) = match accepted {
        Ok(Ok(accepted)) => accepted,
        Ok(Err(problem)) => {
            tracing::debug!("TLS connection refused: {problem}");
            return;
        }
        Err(_) => {
            tracing::debug!("TLS connection refused: the handshake took too long");
            return;
        }
    };
    let builder = match protocol {
        tls::Protocol::Http2 => builder.http2_only(),
        tls::Protocol::Http1 => builder.http1_only(),
    };
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    if let Err(error) = watcher.watch(connection.into_owned()).await {
        tracing::debug!("connection ended: {error}");
    }
}

#[cfg(not(feature = "tls"))]
async fn serve_tls_connection<S>(
    tls: &TlsAcceptor,
    _stream: tokio::net::TcpStream,
    _builder: auto::Builder<TokioExecutor>,
    _service: S,
    _watcher: hyper_util::server::graceful::Watcher,
) {
    tls.never()
}
