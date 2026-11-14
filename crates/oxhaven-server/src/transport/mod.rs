//! The server's one port.
//!
//! Java serves everything from one Jetty port, and so does this module: native gRPC over
//! HTTP/2 without TLS (pydeephaven, the Java client), HTTP/1.1 for the page load of the web
//! IDE, the websocket that the browser then makes its gRPC calls on, and gRPC-web for
//! browsers on an `https://` page behind a proxy that terminates TLS.
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
mod websocket;

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
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
}

impl Front {
    async fn handle(&self, request: Request<Incoming>) -> Response<Body> {
        let origin = request.headers().get(ORIGIN).cloned();
        let preflight = browser::is_preflight(&request);
        let mut response = if preflight {
            browser::preflight()
        } else {
            self.route(request).await
        };
        browser::add_response_headers(&mut response, origin.as_ref(), preflight);
        response
    }

    async fn route(&self, request: Request<Incoming>) -> Response<Body> {
        if !ALLOWED_METHODS.contains(request.method()) {
            return crate::web::error(StatusCode::METHOD_NOT_ALLOWED);
        }
        if websocket::is_upgrade(&request) {
            return websocket::accept(self.websockets.clone(), request);
        }
        if browser::is_grpc(request.headers()) {
            if request.version() != Version::HTTP_2 {
                return browser::not_http2(request.headers());
            }
            return browser::call(self.grpc.clone(), request.map(Body::new)).await;
        }
        match *request.method() {
            Method::GET => crate::web::get(self.web.as_deref(), &request).await,
            Method::OPTIONS => {
                // Java's file servlet answers this, and names methods of its own
                let mut response = empty_response(StatusCode::OK);
                response
                    .headers_mut()
                    .insert(ALLOW, HeaderValue::from_static("GET, HEAD, OPTIONS"));
                response
            }
            _ => crate::web::error(StatusCode::METHOD_NOT_ALLOWED),
        }
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
        let front = front.clone();
        let service = service_fn(move |request| {
            let front = front.clone();
            async move { Ok::<_, Infallible>(front.handle(request).await) }
        });
        let connection = graceful.watch(
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
    drop(listener);
    state.sessions.on_shutdown();
    let _ = closing.send(true);
    graceful.shutdown().await;
    // every websocket holds a sender; the channel closes when the last one is gone
    drop(front);
    let _ = tokio::time::timeout(WEBSOCKET_SHUTDOWN_WAIT, all_closed.recv()).await;
    Ok(())
}
