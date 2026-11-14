//! The port as a browser uses it: the real services through a websocket, and plain
//! HTTP/1.1 requests.

use std::net::SocketAddr;

use super::*;
use crate::session::{
    export_id_to_ticket, AuthHandler, AuthHandlers, DEFAULT_TOKEN_EXPIRE_MS, PSK_AUTH_TYPE,
};
use crate::ServerConfig;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::{HeaderMap, CONTENT_TYPE};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use oxhaven_proto::grpc::{
    ConfigurationConstantsRequest, ConfigurationConstantsResponse, EmptyTableRequest,
    ExportedTableCreationResponse, ExportedTableUpdateMessage, ExportedTableUpdatesRequest,
    TerminationNotificationRequest, TerminationNotificationResponse,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tonic::Status;

const PSK: &str = "test-key";
const CONFIG: &str = "io.deephaven.proto.backplane.grpc.ConfigService";
const SESSION: &str = "io.deephaven.proto.backplane.grpc.SessionService";
const TABLE: &str = "io.deephaven.proto.backplane.grpc.TableService";
const LAST: u32 = 1 << 31;

struct Server {
    address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
}

async fn start(web_root: Option<&std::path::Path>) -> Server {
    let mut state = ServerState::new(ServerConfig {
        auth_handlers: AuthHandlers::new(vec![AuthHandler::psk(PSK).unwrap()]).unwrap(),
        token_expire_ms: DEFAULT_TOKEN_EXPIRE_MS,
        target_cycle_duration: None,
    })
    .unwrap();
    state.web_root = web_root.map(|dir| Arc::new(WebRoot::open(dir).unwrap()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown, stopped) = oneshot::channel::<()>();
    tokio::spawn(async move {
        serve(state, listener, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    Server {
        address,
        shutdown: Some(shutdown),
    }
}

type Socket = WebSocketStream<TcpStream>;

/// Opens a websocket the way the JS API does: to the path of its first call, offering
/// both subprotocols. Returns the socket and the subprotocol the server chose.
async fn open(server: &Server, path: &str, protocols: &str) -> (Socket, Option<String>) {
    let mut request = format!("ws://{}{path}", server.address)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", protocols.parse().unwrap());
    let stream = TcpStream::connect(server.address).await.unwrap();
    let (socket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap();
    let chosen = response
        .headers()
        .get("sec-websocket-protocol")
        .map(|value| value.to_str().unwrap().to_string());
    (socket, chosen)
}

async fn send(socket: &mut Socket, id: u32, payload: &[u8]) {
    let mut bytes = id.to_be_bytes().to_vec();
    bytes.extend_from_slice(payload);
    socket.send(Message::Binary(bytes.into())).await.unwrap();
}

/// Makes a call with one request message and finishes sending.
async fn call(
    socket: &mut Socket,
    id: u32,
    method: &str,
    authorization: &str,
    request: &impl prost::Message,
) {
    let headers = format!(
        "grpc-websockets-path: {method}\r\ncontent-type: application/grpc-web+proto\r\n\
         x-grpc-web: 1\r\nauthorization: {authorization}\r\n"
    );
    send(socket, id, headers.as_bytes()).await;
    let message = request.encode_to_vec();
    let mut data = vec![0, 0];
    data.extend_from_slice(&(message.len() as u32).to_be_bytes());
    data.extend_from_slice(&message);
    send(socket, id, &data).await;
    send(socket, id | LAST, &[1]).await;
}

async fn next(socket: &mut Socket) -> (u32, bool, Bytes) {
    let received = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .expect("a message within five seconds");
    match received {
        Some(Ok(Message::Binary(mut bytes))) => {
            let prefix = u32::from_be_bytes(bytes.split_to(4)[..].try_into().unwrap());
            (prefix & !LAST, prefix & LAST != 0, bytes)
        }
        other => panic!("expected a binary message, got {other:?}"),
    }
}

/// The value of a header in a metadata frame.
fn header(frame: &[u8], name: &str) -> Option<String> {
    assert_eq!(frame[0], 0x80);
    std::str::from_utf8(&frame[5..])
        .unwrap()
        .split("\r\n")
        .find_map(|line| line.strip_prefix(&format!("{name}: ")).map(str::to_string))
}

fn decode<M: prost::Message + Default>(frame: &[u8]) -> M {
    assert_eq!(frame[0], 0, "a message frame");
    assert_eq!(
        u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize,
        frame.len() - 5
    );
    M::decode(&frame[5..]).unwrap()
}

#[tokio::test]
async fn a_browser_session_runs_on_one_websocket() {
    let mut server = start(None).await;
    let (mut socket, chosen) = open(
        &server,
        &format!("/{CONFIG}/GetConfigurationConstants"),
        "grpc-websockets-multiplex, grpc-websockets",
    )
    .await;
    assert_eq!(chosen.as_deref(), Some("grpc-websockets-multiplex"));

    // stream 0, unary: the login. The key is in the call's headers, the token in the
    // response's.
    call(
        &mut socket,
        0,
        &format!("{CONFIG}/GetConfigurationConstants"),
        &format!("{PSK_AUTH_TYPE} {PSK}"),
        &ConfigurationConstantsRequest {},
    )
    .await;
    let (id, last, frame) = next(&mut socket).await;
    assert_eq!((id, last), (0, false));
    let bearer = header(&frame, "authorization").expect("a token in the response headers");
    assert!(bearer.starts_with("Bearer "), "{bearer}");
    assert_eq!(header(&frame, "content-type"), None);
    let constants: ConfigurationConstantsResponse = decode(&next(&mut socket).await.2);
    assert!(constants
        .config_values
        .contains_key("http.session.durationMs"));
    let (id, last, frame) = next(&mut socket).await;
    assert_eq!((id, last), (0, true));
    assert_eq!(header(&frame, "grpc-status").as_deref(), Some("0"));

    // stream 1 stays pending for the life of the page
    call(
        &mut socket,
        1,
        &format!("{SESSION}/TerminationNotification"),
        &bearer,
        &TerminationNotificationRequest {},
    )
    .await;
    // stream 2, server streaming: answers with headers and then waits for exports
    call(
        &mut socket,
        2,
        &format!("{TABLE}/ExportedTableUpdates"),
        &bearer,
        &ExportedTableUpdatesRequest {},
    )
    .await;
    let (id, last, frame) = next(&mut socket).await;
    assert_eq!((id, last), (2, false));
    assert_eq!(frame[0], 0x80);
    // stream 3, unary, while 1 and 2 are open
    call(
        &mut socket,
        3,
        &format!("{TABLE}/EmptyTable"),
        &bearer,
        &EmptyTableRequest {
            result_id: Some(export_id_to_ticket(1)),
            size: 5,
        },
    )
    .await;
    let mut created = None;
    let mut update = None;
    while created.is_none() || update.is_none() {
        let (id, _, frame) = next(&mut socket).await;
        match (id, frame[0]) {
            (3, 0) => created = Some(decode::<ExportedTableCreationResponse>(&frame)),
            (2, 0) => update = Some(decode::<ExportedTableUpdateMessage>(&frame)),
            (3, 0x80) => {}
            other => panic!("unexpected message {other:?}"),
        }
    }
    assert!(created.unwrap().success);
    let update = update.unwrap();
    assert_eq!(update.export_id, Some(export_id_to_ticket(1)));
    assert_eq!(update.size, 5);

    // the server shuts down: the pending call is answered and the socket told to go away
    server.shutdown.take().unwrap().send(()).unwrap();
    let mut notified = false;
    let mut go_away = false;
    while !notified || !go_away {
        let (id, last, frame) = next(&mut socket).await;
        match (id, frame.first()) {
            (1, Some(0)) => {
                let _: TerminationNotificationResponse = decode(&frame);
                notified = true;
            }
            (0x7fff_ffff, None) => {
                assert!(!last);
                go_away = true;
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn a_wrong_key_fails_the_call_in_one_message() {
    let server = start(None).await;
    let (mut socket, _) = open(
        &server,
        &format!("/{CONFIG}/GetConfigurationConstants"),
        "grpc-websockets-multiplex",
    )
    .await;
    call(
        &mut socket,
        0,
        &format!("{CONFIG}/GetConfigurationConstants"),
        &format!("{PSK_AUTH_TYPE} wrong"),
        &ConfigurationConstantsRequest {},
    )
    .await;
    let (id, last, frame) = next(&mut socket).await;
    assert_eq!((id, last), (0, true));
    assert_eq!(header(&frame, "grpc-status").as_deref(), Some("16"));
    assert_eq!(
        header(&frame, "grpc-message").as_deref(),
        Some("Authentication details invalid")
    );
}

#[tokio::test]
async fn a_method_no_service_has_is_unimplemented() {
    let server = start(None).await;
    let (mut socket, _) = open(&server, "/a/b", "grpc-websockets-multiplex").await;
    call(
        &mut socket,
        0,
        "io.deephaven.proto.backplane.grpc.NoSuchService/Method",
        &format!("{PSK_AUTH_TYPE} {PSK}"),
        &ConfigurationConstantsRequest {},
    )
    .await;
    let (id, last, frame) = next(&mut socket).await;
    assert_eq!((id, last), (0, true));
    assert_eq!(header(&frame, "grpc-status").as_deref(), Some("12"));
}

#[tokio::test]
async fn a_socket_without_a_known_subprotocol_is_closed() {
    let server = start(None).await;
    // by hand: a websocket client library refuses a handshake that chose no subprotocol
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    stream
        .write_all(
            b"GET /a/b HTTP/1.1\r\nhost: test\r\nconnection: Upgrade\r\nupgrade: websocket\r\n\
              sec-websocket-version: 13\r\nsec-websocket-key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
              sec-websocket-protocol: something-else\r\n\r\n",
        )
        .await
        .unwrap();
    let mut received = Vec::new();
    let reason = b"Unsupported subprotocol";
    let found = tokio::time::timeout(Duration::from_secs(5), async {
        let mut buffer = [0u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            received.extend_from_slice(&buffer[..read]);
            if read == 0 || received.windows(reason.len()).any(|w| w == reason) {
                break;
            }
        }
    })
    .await;
    let text = String::from_utf8_lossy(&received).to_lowercase();
    assert!(found.is_ok(), "no close frame: {text}");
    assert!(text.starts_with("http/1.1 101 "), "{text}");
    assert!(text.contains(
        "sec-websocket-accept: s3ppLMBiTxaQ9kYGzzhZRbK+xOo="
            .to_lowercase()
            .as_str()
    ));
    assert!(!text.contains("sec-websocket-protocol"), "{text}");
    // a close frame with code 1002, protocol error, and the reason
    let frame = received.len() - reason.len() - 4;
    assert_eq!(
        received[frame..frame + 4],
        [0x88, 2 + reason.len() as u8, 0x03, 0xea]
    );
    assert!(received.ends_with(reason));
}

/// Sends one HTTP/1.1 request and returns the status line, the headers in lower case and
/// the body.
async fn http1(server: &Server, request: &str) -> (String, Vec<String>, String) {
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    let request = request.replace('\n', "\r\n");
    stream
        .write_all(format!("{request}host: test\r\nconnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response).to_string();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let mut lines = head.split("\r\n").map(str::to_string);
    let status = lines.next().unwrap();
    let mut headers: Vec<String> = lines.filter(|line| !line.starts_with("date:")).collect();
    headers.sort();
    (status, headers, body.to_string())
}

fn has(headers: &[String], line: &str) -> bool {
    headers.iter().any(|header| header == line)
}

#[tokio::test]
async fn grpc_over_http1_is_refused_with_a_status() {
    let server = start(None).await;
    for (sent, answered) in [
        ("application/grpc-web+proto", "application/grpc-web"),
        ("application/grpc", "application/grpc"),
    ] {
        let (status, headers, body) = http1(
            &server,
            &format!(
                "POST /{CONFIG}/GetAuthenticationConstants HTTP/1.1\ncontent-type: {sent}\n\
                 content-length: 0\n"
            ),
        )
        .await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert!(has(&headers, "grpc-status: 13"), "{headers:?}");
        assert!(
            has(&headers, &format!("content-type: {answered}")),
            "{headers:?}"
        );
        assert!(headers
            .iter()
            .any(|h| h.starts_with("grpc-message: The server connection is not using http2")));
        assert_eq!(body, "");
    }
}

#[tokio::test]
async fn a_preflight_allows_any_origin_to_post() {
    let server = start(None).await;
    let (status, headers, _) = http1(
        &server,
        &format!(
            "OPTIONS /{CONFIG}/GetAuthenticationConstants HTTP/1.1\norigin: http://example.com\n\
             access-control-request-method: POST\n\
             access-control-request-headers: content-type,x-grpc-web\n"
        ),
    )
    .await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(has(
        &headers,
        "access-control-allow-origin: http://example.com"
    ));
    assert!(has(&headers, "access-control-allow-methods: POST"));
    assert!(has(&headers, "access-control-max-age: 60"));
    assert!(has(&headers, "vary: Origin"));
    let allowed = headers
        .iter()
        .find_map(|h| h.strip_prefix("access-control-allow-headers: "))
        .unwrap();
    for name in [
        "content-type",
        "x-grpc-web",
        "Authorization",
        "x-deephaven-stream-ticket",
        "x-deephaven-stream-sequence",
        "x-deephaven-stream-halfclose",
    ] {
        assert!(allowed.split(',').any(|h| h == name), "{name} in {allowed}");
    }
    assert!(!headers
        .iter()
        .any(|h| h.starts_with("access-control-expose-headers")));
}

#[tokio::test]
async fn every_response_has_the_configured_headers_and_names_an_origin_it_was_given() {
    let server = start(None).await;
    let (status, headers, _) = http1(&server, "GET /nothing HTTP/1.1\n").await;
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    for line in [
        "cross-origin-embedder-policy: require-corp",
        "cross-origin-opener-policy: same-origin",
        "cross-origin-resource-policy: same-origin",
        "referrer-policy: no-referrer",
        "x-content-type-options: nosniff",
        "x-xss-protection: 1; mode=block",
    ] {
        assert!(has(&headers, line), "{line} in {headers:?}");
    }
    assert!(!headers
        .iter()
        .any(|h| h.starts_with("access-control-allow-origin")));

    let (_, headers, _) = http1(&server, "GET /nothing HTTP/1.1\norigin: http://a.test\n").await;
    assert!(has(&headers, "access-control-allow-origin: http://a.test"));
    assert!(has(
        &headers,
        "access-control-expose-headers: \
         grpc-status,content-type,Authorization,grpc-message,grpc-status-details-bin"
    ));
}

#[tokio::test]
async fn only_get_post_and_options_are_taken() {
    let server = start(None).await;
    for method in ["HEAD", "PUT", "DELETE"] {
        let (status, _, _) = http1(&server, &format!("{method} /ide/ HTTP/1.1\n")).await;
        assert_eq!(status, "HTTP/1.1 405 Method Not Allowed", "{method}");
    }
    // whatever the request is otherwise
    let (status, headers, _) = http1(
        &server,
        "PUT /a/b HTTP/1.1\ncontent-type: application/grpc\ncontent-length: 0\n",
    )
    .await;
    assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
    assert!(!headers.iter().any(|h| h.starts_with("grpc-status")));
    let (status, _, _) = http1(&server, "POST /ide/ HTTP/1.1\ncontent-length: 0\n").await;
    assert_eq!(status, "HTTP/1.1 405 Method Not Allowed");
    let (status, headers, _) = http1(&server, "OPTIONS /ide/ HTTP/1.1\n").await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(has(&headers, "allow: GET, HEAD, OPTIONS"));
}

#[tokio::test]
async fn the_web_root_is_served_and_its_absence_is_not_found() {
    let dir = std::env::temp_dir().join(format!("oxhaven-transport-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(dir.join("ide")).unwrap();
    std::fs::write(dir.join("ide/index.html"), "<html>ide</html>").unwrap();

    let with = start(Some(&dir)).await;
    let (status, headers, _) = http1(&with, "GET /?psk=abc HTTP/1.1\n").await;
    assert_eq!(status, "HTTP/1.1 302 Found");
    assert!(has(&headers, "location: /ide/?psk=abc"));
    let (status, headers, body) = http1(&with, "GET /ide/ HTTP/1.1\n").await;
    assert_eq!(status, "HTTP/1.1 200 OK");
    assert!(has(&headers, "content-type: text/html"));
    assert_eq!(body, "<html>ide</html>");

    let without = start(None).await;
    let (status, _, _) = http1(&without, "GET /ide/ HTTP/1.1\n").await;
    assert_eq!(status, "HTTP/1.1 404 Not Found");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A service that answers with the content type it was called with as its one message.
fn content_type_echo() -> GrpcService {
    BoxCloneSyncService::new(tower::service_fn(|request: Request<Body>| async move {
        if request.uri().path() == "/slow" {
            std::future::pending::<()>().await;
        }
        let seen = request.headers().get(CONTENT_TYPE).unwrap().as_bytes();
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let frames: Vec<Result<Frame<Bytes>, Status>> = vec![
            Ok(Frame::data(Bytes::copy_from_slice(seen))),
            Ok(Frame::trailers(trailers)),
        ];
        let mut response = Response::new(Body::new(StreamBody::new(tokio_stream::iter(frames))));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, "application/grpc".parse().unwrap());
        Ok(response)
    }))
}

fn grpc_request(path: &str, content_type: &str) -> Request<Body> {
    let mut request = Request::new(Body::default());
    *request.uri_mut() = path.parse().unwrap();
    request
        .headers_mut()
        .insert(CONTENT_TYPE, content_type.parse().unwrap());
    request
}

#[tokio::test]
async fn a_grpc_web_call_is_a_grpc_call_with_its_trailers_in_the_body() {
    let response = browser::call(
        content_type_echo(),
        grpc_request("/x", "application/grpc-web+proto"),
    )
    .await;
    assert_eq!(response.headers()[CONTENT_TYPE], "application/grpc-web");
    let body = response.into_body().collect().await.unwrap();
    assert!(body.trailers().is_none());
    let mut expected = b"application/grpc+proto".to_vec();
    expected.extend_from_slice(&[0x80, 0, 0, 0, 16]);
    expected.extend_from_slice(b"grpc-status: 0\r\n");
    assert_eq!(body.to_bytes(), Bytes::from(expected));
}

#[tokio::test]
async fn a_native_grpc_call_passes_through_unchanged() {
    let response = browser::call(content_type_echo(), grpc_request("/x", "application/grpc")).await;
    assert_eq!(response.headers()[CONTENT_TYPE], "application/grpc");
    let body = response.into_body().collect().await.unwrap();
    assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(body.to_bytes(), Bytes::from_static(b"application/grpc"));
}

#[tokio::test]
async fn a_call_without_an_answer_ends_at_the_clients_timeout() {
    let mut request = grpc_request("/slow", "application/grpc");
    request
        .headers_mut()
        .insert("grpc-timeout", "20m".parse().unwrap());
    let response = tokio::time::timeout(
        Duration::from_secs(5),
        browser::call(content_type_echo(), request),
    )
    .await
    .expect("the call ends");
    assert_eq!(response.headers()["grpc-status"], "1");
    assert_eq!(response.headers()["grpc-message"], "Timeout%20expired");
}
