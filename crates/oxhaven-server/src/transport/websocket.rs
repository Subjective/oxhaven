//! gRPC over a websocket, as browsers on a plain `http://` page use it.
//!
//! A websocket carries gRPC calls as messages. With the subprotocol
//! `grpc-websockets-multiplex` one socket carries any number of calls, each message starting
//! with a stream id; with `grpc-websockets` a socket carries one call, whose method is the
//! path of the upgrade request. The Deephaven JS API uses only the first.
//!
//! Each call becomes an HTTP/2-shaped request to the same service stack that native gRPC
//! clients reach, so every registered service works here without code of its own.
//!
//! A client message is, after the stream id of the multiplexed protocol:
//!
//! - first message of a stream: the request headers as text, `name: value\r\n` each. The
//!   multiplexed protocol names the method in the header `grpc-websockets-path`.
//! - `0x00` + a piece of the gRPC message stream (length-prefixed messages, split anywhere).
//! - `0x01`: the client has finished sending. The stream id has its sign bit set.
//!
//! A server message is, after the stream id:
//!
//! - a frame `0x80, length (4 bytes), text` with the response headers,
//! - gRPC length-prefixed messages,
//! - a frame `0x80, length, text` with the trailers; the stream id has its sign bit set. A
//!   call that fails before its first message sends only this.
//!
//! Stream id `0x7fffffff` is the transport's GO_AWAY: the server sends it when it shuts down
//! and the client acknowledges with a finish message on that id.
//!
//! Behavior: `io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream`,
//! `MultiplexedWebsocketStreamImpl`, `WebSocketServerStream`, `WebsocketStreamImpl`,
//! `AbstractWebSocketServerStream#readHeaders`

use std::collections::HashMap;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use http::header::{
    HeaderMap, HeaderName, HeaderValue, CONNECTION, CONTENT_TYPE, SEC_WEBSOCKET_ACCEPT,
    SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, TE, UPGRADE,
};
use http::{Method, Request, Response, StatusCode, Version};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use hyper_util::rt::TokioIo;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tonic::{Code, Status};
use tower::ServiceExt;

use super::{empty_response, Body, GrpcService};

#[cfg(test)]
mod tests;

/// Behavior: `MultiplexedWebSocketServerStream#GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL`
pub(crate) const MULTIPLEX_PROTOCOL: &str = "grpc-websockets-multiplex";
/// Behavior: `WebSocketServerStream#GRPC_WEBSOCKETS_PROTOCOL`
pub(crate) const PER_SOCKET_PROTOCOL: &str = "grpc-websockets";
/// The request header of the multiplexed protocol that names the method.
///
/// Behavior: `MultiplexedWebSocketServerStream#PATH`
const PATH_HEADER: &str = "grpc-websockets-path";

/// The stream id that stands for the whole socket in the GO_AWAY exchange.
const GO_AWAY: i32 = i32::MAX;
const LAST: u32 = 1 << 31;
/// The flag byte of a frame that holds headers or trailers instead of a message.
const METADATA_FRAME: u8 = 0x80;

/// How long a socket may stay open after the server has told it to go away.
const GO_AWAY_GRACE: Duration = Duration::from_secs(5);
/// Messages waiting to be written to one socket before the calls that produce them wait.
const OUTBOUND_QUEUE: usize = 64;

/// What a websocket needs from the server it belongs to.
#[derive(Clone)]
pub(crate) struct WebsocketContext {
    pub(crate) grpc: GrpcService,
    /// Becomes true when the server shuts down.
    pub(crate) closing: watch::Receiver<bool>,
    /// Held by each open socket; the server waits for all clones to drop.
    #[expect(dead_code, reason = "held, not used: its drop is the signal")]
    pub(crate) alive: mpsc::Sender<()>,
}

/// Whether this is a websocket handshake.
pub(crate) fn is_upgrade<B>(request: &Request<B>) -> bool {
    let has = |name: HeaderName, token: &str| {
        request.headers().get_all(name).iter().any(|value| {
            value.to_str().is_ok_and(|v| {
                v.split(',')
                    .any(|part| part.trim().eq_ignore_ascii_case(token))
            })
        })
    };
    request.method() == Method::GET
        && request.version() == Version::HTTP_11
        && has(UPGRADE, "websocket")
        && has(CONNECTION, "upgrade")
}

/// The endpoint is `/{service}/{method}`: exactly two path segments. The multiplexed
/// protocol ignores what they are.
///
/// Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer` (the websocket endpoint)
fn is_endpoint(path: &str) -> bool {
    let mut segments = path.strip_prefix('/').unwrap_or(path).split('/');
    matches!(
        (segments.next(), segments.next(), segments.next()),
        (Some(service), Some(method), None) if !service.is_empty() && !method.is_empty()
    )
}

/// The first subprotocol of the client's list that the server has. Java was seen to choose
/// this way: offered `grpc-websockets, grpc-websockets-multiplex` it answers
/// `grpc-websockets`, and the other way around `grpc-websockets-multiplex`.
fn choose_protocol(headers: &HeaderMap) -> Option<&'static str> {
    headers
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .find_map(|offered| match offered.trim() {
            MULTIPLEX_PROTOCOL => Some(MULTIPLEX_PROTOCOL),
            PER_SOCKET_PROTOCOL => Some(PER_SOCKET_PROTOCOL),
            _ => None,
        })
}

/// Answers a websocket handshake and, once the connection has switched protocols, serves
/// gRPC on it. A socket without a subprotocol the server knows is accepted and then closed,
/// as Java does.
///
/// Behavior: `io.grpc.servlet.web.websocket.GrpcWebsocket#onOpen`
pub(crate) fn accept<B: Send + 'static>(
    context: WebsocketContext,
    mut request: Request<B>,
) -> Response<Body> {
    if !is_endpoint(request.uri().path()) {
        return crate::web::error(StatusCode::NOT_FOUND);
    }
    let version = request.headers().get(SEC_WEBSOCKET_VERSION);
    let Some(key) = request.headers().get(SEC_WEBSOCKET_KEY) else {
        return empty_response(StatusCode::BAD_REQUEST);
    };
    if version.is_none_or(|v| v.as_bytes() != b"13") {
        let mut response = empty_response(StatusCode::BAD_REQUEST);
        response
            .headers_mut()
            .insert(SEC_WEBSOCKET_VERSION, HeaderValue::from_static("13"));
        return response;
    }
    let accept_key = derive_accept_key(key.as_bytes());
    let protocol = choose_protocol(request.headers());
    // gRPC method names carry no leading slash
    let method = request.uri().path().trim_start_matches('/').to_string();
    let upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let io = match upgrade.await {
            Ok(io) => io,
            Err(error) => {
                tracing::debug!("websocket upgrade failed: {error}");
                return;
            }
        };
        // Java lifts the container's message size limit and idle timeout
        // (AbstractWebSocketServerStream#onOpen)
        let config = WebSocketConfig::default()
            .max_message_size(None)
            .max_frame_size(None);
        let socket =
            WebSocketStream::from_raw_socket(TokioIo::new(io), Role::Server, Some(config)).await;
        let framing = match protocol {
            Some(MULTIPLEX_PROTOCOL) => Framing::Multiplexed,
            Some(_) => Framing::PerSocket { method },
            None => {
                close(socket, CloseCode::Protocol, "Unsupported subprotocol").await;
                return;
            }
        };
        Connection::new(context, framing).run(socket).await;
    });

    let mut response = empty_response(StatusCode::SWITCHING_PROTOCOLS);
    let headers = response.headers_mut();
    headers.insert(UPGRADE, HeaderValue::from_static("websocket"));
    headers.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    if let Ok(value) = HeaderValue::from_str(&accept_key) {
        headers.insert(SEC_WEBSOCKET_ACCEPT, value);
    }
    if let Some(protocol) = protocol {
        headers.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(protocol));
    }
    response
}

async fn close<S>(mut socket: WebSocketStream<S>, code: CloseCode, reason: &'static str)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = CloseFrame {
        code,
        reason: reason.into(),
    };
    let _ = socket.close(Some(frame)).await;
    // read until the peer has answered the close, so that the frame is not lost
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(Ok(_)) = socket.next().await {}
    })
    .await;
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

enum Framing {
    Multiplexed,
    PerSocket { method: String },
}

/// The GO_AWAY exchange. Between the server's GO_AWAY and the client's acknowledgement a
/// new stream is still accepted, since the client may have sent it before it saw the
/// GO_AWAY.
///
/// Behavior: `MultiplexedWebSocketServerStream.ClosedState`
#[derive(PartialEq)]
enum Closing {
    Open,
    Announced,
    Acknowledged,
}

/// What a call writes to its socket.
enum Outbound {
    Message(Bytes),
    /// The single call of a per-call socket is over.
    Finished,
}

type RequestPiece = Result<Frame<Bytes>, Status>;

struct Connection {
    context: WebsocketContext,
    framing: Framing,
    /// The request bodies of the streams the client has not finished, by stream id.
    open: HashMap<i32, mpsc::UnboundedSender<RequestPiece>>,
    /// The calls in progress. Dropping this cancels them.
    calls: JoinSet<()>,
    outbound: mpsc::Sender<Outbound>,
    outbound_queue: mpsc::Receiver<Outbound>,
    closing: Closing,
    /// A per-call socket has seen its headers.
    started: bool,
}

impl Connection {
    fn new(context: WebsocketContext, framing: Framing) -> Self {
        let (outbound, outbound_queue) = mpsc::channel(OUTBOUND_QUEUE);
        Self {
            context,
            framing,
            open: HashMap::new(),
            calls: JoinSet::new(),
            outbound,
            outbound_queue,
            closing: Closing::Open,
            started: false,
        }
    }

    async fn run<S>(mut self, mut socket: WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut shutdown = self.context.closing.clone();
        let mut deadline = None;
        let protocol_error = loop {
            tokio::select! {
                message = socket.next() => match message {
                    Some(Ok(Message::Binary(data))) => {
                        if let Err(reason) = self.on_message(data) {
                            break Some(reason);
                        }
                    }
                    Some(Ok(Message::Text(_))) => break Some("Can't read string payloads"),
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break None,
                },
                Some(outbound) = self.outbound_queue.recv() => match outbound {
                    Outbound::Message(data) => {
                        if socket.send(Message::Binary(data)).await.is_err() {
                            break None;
                        }
                    }
                    Outbound::Finished => {
                        self.fail_open_streams();
                        let _ = socket.close(None).await;
                        break None;
                    }
                },
                Some(_) = self.calls.join_next(), if !self.calls.is_empty() => {}
                changed = shutdown.changed(), if self.closing == Closing::Open => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.closing = Closing::Announced;
                        deadline = Some(tokio::time::Instant::now() + GO_AWAY_GRACE);
                        if matches!(self.framing, Framing::Multiplexed) {
                            let go_away = Bytes::copy_from_slice(&GO_AWAY.to_be_bytes());
                            if socket.send(Message::Binary(go_away)).await.is_err() {
                                break None;
                            }
                        }
                    }
                }
                () = sleep_until(deadline) => break None,
            }
        };
        // the calls end here, whatever state they are in: there is nobody left to answer
        self.fail_open_streams();
        self.calls.shutdown().await;
        match protocol_error {
            Some(reason) => close(socket, CloseCode::Protocol, reason).await,
            None => {
                let _ = socket.close(None).await;
            }
        }
    }

    /// Ends the request side of every unfinished stream with an error, so that a handler
    /// reading its request stream does not take the end of the socket for a half close.
    ///
    /// Behavior: `MultiplexedWebSocketServerStream#onError`
    fn fail_open_streams(&mut self) {
        for (_, body) in self.open.drain() {
            let _ = body.send(Err(Status::new(Code::Unknown, "websocket closed")));
        }
    }

    /// Takes one binary message. An error is the reason to close the socket with.
    ///
    /// Behavior: `MultiplexedWebSocketServerStream#onMessage(ByteBuffer)`,
    /// `WebSocketServerStream#onMessage(ByteBuffer)`
    fn on_message(&mut self, mut data: Bytes) -> Result<(), &'static str> {
        let (id, last) = match self.framing {
            Framing::Multiplexed => {
                if data.len() < 4 {
                    return Err("Unexpected empty message");
                }
                let prefix = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let _ = data.split_to(4);
                ((prefix & !LAST) as i32, prefix & LAST != 0)
            }
            Framing::PerSocket { .. } => (0, false),
        };
        let multiplexed = matches!(self.framing, Framing::Multiplexed);
        if multiplexed && last && id == GO_AWAY {
            if self.closing != Closing::Announced {
                return Err("Unexpected close ACK");
            }
            self.closing = Closing::Acknowledged;
            return Ok(());
        }
        if data.is_empty() {
            if let Some(body) = self.open.remove(&id) {
                let _ = body.send(Err(Status::new(Code::Unknown, "")));
            }
            return Err("Unexpected empty message");
        }
        let known = match self.framing {
            Framing::Multiplexed => self.open.contains_key(&id),
            Framing::PerSocket { .. } => self.started,
        };
        if !known {
            if self.closing == Closing::Acknowledged {
                return Err("Stream created after closing initiated");
            }
            self.started = true;
            self.start_call(id, &data);
            return Ok(());
        }
        let control = data.split_to(1)[0];
        if control == 1 {
            // the client has finished sending
            let body = self.open.remove(&id);
            if !data.is_empty() {
                if let Some(body) = body {
                    let _ = body.send(Err(Status::new(Code::Unknown, "")));
                }
                return Err("Unexpected bytes in close message");
            }
            return Ok(());
        }
        if let Some(body) = self.open.get(&id) {
            // a call that has already answered no longer reads its request
            let _ = body.send(Ok(Frame::data(data)));
        }
        Ok(())
    }

    fn start_call(&mut self, id: i32, header_text: &[u8]) {
        let mut headers = read_headers(header_text);
        let method = match &self.framing {
            Framing::Multiplexed => headers
                .remove(PATH_HEADER)
                .and_then(|path| path.to_str().ok().map(str::to_string)),
            Framing::PerSocket { method } => Some(method.clone()),
        };
        let prefix = match self.framing {
            Framing::Multiplexed => Some(id),
            Framing::PerSocket { .. } => None,
        };
        let (body, pieces) = mpsc::unbounded_channel::<RequestPiece>();
        self.open.insert(id, body);
        let uri = method
            .map(|method| format!("/{}", method.trim_start_matches('/')))
            .and_then(|path| path.parse::<http::Uri>().ok());
        let outbound = self.outbound.clone();
        let Some(uri) = uri else {
            // No Java counterpart: a stream that names no method cannot be routed. It is
            // answered like a method the server does not have.
            self.calls.spawn(async move {
                let status = Status::new(Code::Unimplemented, "Method not found");
                let _ = send_metadata(&outbound, prefix, true, &status_headers(&status)).await;
            });
            return;
        };
        let mut request = Request::new(Body::new(StreamBody::new(UnboundedReceiverStream::new(
            pieces,
        ))));
        *request.method_mut() = Method::POST;
        *request.uri_mut() = uri;
        *request.version_mut() = Version::HTTP_2;
        *request.headers_mut() = headers;
        let grpc = self.context.grpc.clone();
        self.calls.spawn(respond(grpc, request, outbound, prefix));
    }
}

/// Reads the header text of a stream's first message into HTTP headers. A line is
/// `name: value`; the content type is replaced by gRPC's own whatever the client said, and
/// `te: trailers` is added, as a native gRPC call has it. A `-bin` value stays in base64,
/// which is how HTTP headers carry it.
///
/// Behavior: `AbstractWebSocketServerStream#readHeaders`
fn read_headers(text: &[u8]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for line in text.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            continue;
        };
        let (name, value) = (&line[..colon], &line[colon + 1..]);
        let value = value.strip_prefix(b" ").unwrap_or(value);
        // a name or value that HTTP could not carry is dropped: the call then proceeds
        // without it, as it would behind a proxy that removed it
        let Ok(name) = HeaderName::from_bytes(name) else {
            continue;
        };
        if name == CONTENT_TYPE {
            continue;
        }
        if let Ok(value) = HeaderValue::from_bytes(value) {
            headers.append(name, value);
        }
    }
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/grpc+proto"),
    );
    headers.insert(TE, HeaderValue::from_static("trailers"));
    headers
}

/// Headers or trailers as the text of a metadata frame: the flag byte, the length of the
/// text, and `name: value\r\n` for each.
///
/// Behavior: `MultiplexedWebsocketStreamImpl.Sink#writeMetadataToStream`,
/// `io.grpc.servlet.jakarta.web.GrpcWebServletResponse#createTrailerPayload`
pub(crate) fn metadata_frame(headers: &HeaderMap) -> Bytes {
    let mut text = BytesMut::new();
    for (name, value) in headers {
        text.put_slice(name.as_str().as_bytes());
        text.put_slice(b": ");
        text.put_slice(value.as_bytes());
        text.put_slice(b"\r\n");
    }
    let mut frame = BytesMut::with_capacity(text.len() + 5);
    frame.put_u8(METADATA_FRAME);
    frame.put_u32(text.len() as u32);
    frame.put_slice(&text);
    frame.freeze()
}

fn status_headers(status: &Status) -> HeaderMap {
    let mut headers = HeaderMap::new();
    // a status always fits in headers; the fallback is unreachable
    let _ = status.add_header(&mut headers);
    headers
}

async fn send(
    outbound: &mpsc::Sender<Outbound>,
    prefix: Option<i32>,
    last: bool,
    payload: &[u8],
) -> Result<(), ()> {
    let mut message = BytesMut::with_capacity(payload.len() + 4);
    if let Some(id) = prefix {
        message.put_u32(id as u32 | if last { LAST } else { 0 });
    }
    message.put_slice(payload);
    outbound
        .send(Outbound::Message(message.freeze()))
        .await
        .map_err(|_| ())
}

/// Sends headers or trailers. On a per-call socket the frame prefix and the text are two
/// messages, and the socket closes after the trailers.
///
/// Behavior: `MultiplexedWebsocketStreamImpl.Sink`, `WebsocketStreamImpl.Sink`
async fn send_metadata(
    outbound: &mpsc::Sender<Outbound>,
    prefix: Option<i32>,
    last: bool,
    headers: &HeaderMap,
) -> Result<(), ()> {
    let frame = metadata_frame(headers);
    if prefix.is_some() {
        return send(outbound, prefix, last, &frame).await;
    }
    send(outbound, None, false, &frame[..5]).await?;
    send(outbound, None, false, &frame[5..]).await?;
    if last {
        outbound.send(Outbound::Finished).await.map_err(|_| ())?;
    }
    Ok(())
}

/// Runs one call and writes its response to the socket.
async fn respond(
    grpc: GrpcService,
    request: Request<Body>,
    outbound: mpsc::Sender<Outbound>,
    prefix: Option<i32>,
) {
    let response = match grpc.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let _: Result<(), ()> = async {
        let (mut parts, mut body) = response.into_parts();
        // the websocket has no content type of its own to announce
        parts.headers.remove(CONTENT_TYPE);
        if parts.headers.contains_key("grpc-status") {
            // the call failed before its first message: the headers are the trailers
            return send_metadata(&outbound, prefix, true, &parts.headers).await;
        }
        send_metadata(&outbound, prefix, false, &parts.headers).await?;
        loop {
            let trailers = match body.frame().await {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(data) => {
                        if !data.is_empty() {
                            send(&outbound, prefix, false, &data).await?;
                        }
                        continue;
                    }
                    Err(frame) => frame.into_trailers().unwrap_or_default(),
                },
                Some(Err(status)) => status_headers(&status),
                None => status_headers(&Status::new(
                    Code::Unknown,
                    "the response ended without a status",
                )),
            };
            return send_metadata(&outbound, prefix, true, &trailers).await;
        }
    }
    .await;
}
