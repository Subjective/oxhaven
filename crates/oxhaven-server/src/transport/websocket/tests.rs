//! The websocket framing against a service that echoes, so that every kind of call and
//! every way a stream can end is seen without the real services.

use std::sync::{Arc, Mutex};

use super::*;
use tokio::io::DuplexStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::ReceiverStream;
use tower::util::BoxCloneSyncService;

type Client = WebSocketStream<DuplexStream>;

/// What the echo service saw happen to its calls.
type Events = Arc<Mutex<Vec<String>>>;

/// A service that answers every call with the request's path and `authorization` in its
/// headers and then sends back each piece of the request body as it arrives. When the
/// request ends it sends trailers. A path ending in `Refuse` fails before any message,
/// and one ending in `Pending` never answers.
fn echo(events: Events) -> GrpcService {
    BoxCloneSyncService::new(tower::service_fn(move |request: Request<Body>| {
        let events = events.clone();
        async move {
            let path = request.uri().path().to_string();
            events.lock().unwrap().push(format!("call {path}"));
            if path.ends_with("Refuse") {
                return Ok(Status::new(Code::PermissionDenied, "refused").into_http());
            }
            if path.ends_with("Pending") {
                struct Dropped(Events);
                impl Drop for Dropped {
                    fn drop(&mut self) {
                        self.0
                            .lock()
                            .unwrap()
                            .push("pending call dropped".to_string());
                    }
                }
                let _dropped = Dropped(events.clone());
                std::future::pending::<()>().await;
            }
            let (parts, mut body) = request.into_parts();
            let (frames, response_frames) = mpsc::channel::<Result<Frame<Bytes>, Status>>(8);
            tokio::spawn(async move {
                loop {
                    let piece = tokio::select! {
                        biased;
                        piece = body.frame() => piece,
                        () = frames.closed() => {
                            events.lock().unwrap().push(format!("cancelled {path}"));
                            return;
                        }
                    };
                    match piece {
                        Some(Ok(frame)) => {
                            let _ = frames.send(Ok(frame)).await;
                        }
                        Some(Err(status)) => {
                            events
                                .lock()
                                .unwrap()
                                .push(format!("request failed {path}: {:?}", status.code()));
                            return;
                        }
                        None => {
                            events.lock().unwrap().push(format!("half closed {path}"));
                            let mut trailers = HeaderMap::new();
                            trailers.insert("grpc-status", HeaderValue::from_static("0"));
                            let _ = frames.send(Ok(Frame::trailers(trailers))).await;
                            return;
                        }
                    }
                }
            });
            let mut response = Response::new(Body::new(StreamBody::new(ReceiverStream::new(
                response_frames,
            ))));
            let headers = response.headers_mut();
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
            headers.insert("x-path", HeaderValue::from_str(parts.uri.path()).unwrap());
            for name in ["authorization", "content-type", "te", "x-bin"] {
                if let Some(value) = parts.headers.get(name) {
                    headers.insert(
                        HeaderName::from_bytes(format!("x-seen-{name}").as_bytes()).unwrap(),
                        value.clone(),
                    );
                }
            }
            Ok(response)
        }
    }))
}

struct Harness {
    client: Client,
    events: Events,
    closing: watch::Sender<bool>,
    /// Closes when the connection has ended.
    ended: UnboundedReceiver<()>,
}

async fn connect(framing: Framing) -> Harness {
    let events = Events::default();
    let (closing, closing_seen) = watch::channel(false);
    let (alive, _) = mpsc::channel(1);
    let context = WebsocketContext {
        grpc: echo(events.clone()),
        closing: closing_seen,
        alive,
    };
    let (near, far) = tokio::io::duplex(1 << 16);
    let server = WebSocketStream::from_raw_socket(far, Role::Server, None).await;
    let client = WebSocketStream::from_raw_socket(near, Role::Client, None).await;
    let (done, ended) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        Connection::new(context, framing).run(server).await;
        drop(done);
    });
    Harness {
        client,
        events,
        closing,
        ended,
    }
}

fn message(id: u32, payload: &[u8]) -> Message {
    let mut bytes = id.to_be_bytes().to_vec();
    bytes.extend_from_slice(payload);
    Message::Binary(bytes.into())
}

fn headers(id: u32, method: &str, more: &str) -> Message {
    message(
        id,
        format!(
            "grpc-websockets-path: {method}\r\ncontent-type: application/grpc-web+proto\r\n\
             x-grpc-web: 1\r\n{more}"
        )
        .as_bytes(),
    )
}

fn data(id: u32, payload: &[u8]) -> Message {
    let mut bytes = vec![0];
    bytes.extend_from_slice(payload);
    message(id, &bytes)
}

fn finish(id: u32) -> Message {
    message(id | LAST, &[1])
}

/// The next binary message, as stream id, whether it is the stream's last, and payload.
async fn next(client: &mut Client) -> (i32, bool, Bytes) {
    let received = tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("a message within five seconds");
    match received {
        Some(Ok(Message::Binary(mut bytes))) => {
            let prefix = u32::from_be_bytes(bytes.split_to(4)[..].try_into().unwrap());
            ((prefix & !LAST) as i32, prefix & LAST != 0, bytes)
        }
        other => panic!("expected a binary message, got {other:?}"),
    }
}

/// The header lines of a metadata frame, sorted: their order on the wire is not defined.
fn metadata(frame: &[u8]) -> Vec<String> {
    assert_eq!(
        frame[0], METADATA_FRAME,
        "a metadata frame starts with 0x80"
    );
    let length = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
    assert_eq!(length, frame.len() - 5, "the length covers the header text");
    let text = std::str::from_utf8(&frame[5..]).unwrap();
    assert!(text.ends_with("\r\n"));
    let mut lines: Vec<String> = text.trim_end().split("\r\n").map(str::to_string).collect();
    lines.sort();
    lines
}

async fn receive(client: &mut Client) -> Bytes {
    match tokio::time::timeout(Duration::from_secs(5), client.next()).await {
        Ok(Some(Ok(Message::Binary(bytes)))) => bytes,
        other => panic!("expected a binary message, got {other:?}"),
    }
}

async fn close_reason(client: &mut Client) -> (CloseCode, String) {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("a close within five seconds")
        {
            Some(Ok(Message::Close(Some(frame)))) => return (frame.code, frame.reason.to_string()),
            Some(Ok(Message::Close(None))) => return (CloseCode::Status, String::new()),
            Some(Ok(_)) => {}
            other => panic!("expected a close frame, got {other:?}"),
        }
    }
}

async fn events_when(events: &Events, wanted: &str) -> Vec<String> {
    for _ in 0..500 {
        if events.lock().unwrap().iter().any(|event| event == wanted) {
            return events.lock().unwrap().clone();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("never saw {wanted:?}: {:?}", events.lock().unwrap());
}

#[tokio::test]
async fn a_unary_call_is_headers_a_message_and_closing_trailers() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(
            7,
            "pkg.Service/Method",
            "authorization: Bearer abc\r\n",
        ))
        .await
        .unwrap();
    // the JS client sends the five byte prefix and the message apart
    h.client.send(data(7, &[0, 0, 0, 0, 3])).await.unwrap();
    h.client.send(data(7, b"abc")).await.unwrap();
    h.client.send(finish(7)).await.unwrap();

    let (id, last, payload) = next(&mut h.client).await;
    assert_eq!((id, last), (7, false));
    assert_eq!(
        metadata(&payload),
        vec![
            "x-path: /pkg.Service/Method",
            "x-seen-authorization: Bearer abc",
            // whatever the client called it, the services see a gRPC call
            "x-seen-content-type: application/grpc+proto",
            "x-seen-te: trailers",
        ]
    );
    assert_eq!(
        next(&mut h.client).await,
        (7, false, Bytes::from_static(&[0, 0, 0, 0, 3]))
    );
    assert_eq!(
        next(&mut h.client).await,
        (7, false, Bytes::from_static(b"abc"))
    );
    let (id, last, payload) = next(&mut h.client).await;
    assert_eq!((id, last), (7, true));
    assert_eq!(metadata(&payload), vec!["grpc-status: 0"]);
}

#[tokio::test]
async fn a_bidirectional_call_answers_before_the_client_has_finished() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(1, "pkg.Service/Bidi", ""))
        .await
        .unwrap();
    assert!(!next(&mut h.client).await.1);
    for round in [b"one", b"two"] {
        h.client.send(data(1, round)).await.unwrap();
        assert_eq!(
            next(&mut h.client).await,
            (1, false, Bytes::from_static(round))
        );
    }
    h.client.send(finish(1)).await.unwrap();
    let (id, last, payload) = next(&mut h.client).await;
    assert_eq!((id, last), (1, true));
    assert_eq!(metadata(&payload), vec!["grpc-status: 0"]);
    assert_eq!(
        events_when(&h.events, "half closed /pkg.Service/Bidi").await,
        vec!["call /pkg.Service/Bidi", "half closed /pkg.Service/Bidi"]
    );
}

#[tokio::test]
async fn streams_of_one_socket_do_not_mix() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(0, "pkg.Service/A", ""))
        .await
        .unwrap();
    assert_eq!(
        metadata(&next(&mut h.client).await.2)[0],
        "x-path: /pkg.Service/A"
    );
    h.client
        .send(headers(1, "pkg.Service/B", ""))
        .await
        .unwrap();
    assert_eq!(
        metadata(&next(&mut h.client).await.2)[0],
        "x-path: /pkg.Service/B"
    );
    h.client.send(data(1, b"for b")).await.unwrap();
    assert_eq!(
        next(&mut h.client).await,
        (1, false, Bytes::from_static(b"for b"))
    );
    h.client.send(data(0, b"for a")).await.unwrap();
    assert_eq!(
        next(&mut h.client).await,
        (0, false, Bytes::from_static(b"for a"))
    );
    h.client.send(finish(0)).await.unwrap();
    assert_eq!(next(&mut h.client).await.0, 0);
    // stream 1 is still open
    h.client.send(data(1, b"more")).await.unwrap();
    assert_eq!(
        next(&mut h.client).await,
        (1, false, Bytes::from_static(b"more"))
    );
}

#[tokio::test]
async fn a_call_that_fails_before_a_message_is_one_closing_message() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(3, "pkg.Service/Refuse", ""))
        .await
        .unwrap();
    let (id, last, payload) = next(&mut h.client).await;
    assert_eq!((id, last), (3, true));
    assert_eq!(
        metadata(&payload),
        vec!["grpc-message: refused", "grpc-status: 7"]
    );
    // the status is the first line
    assert!(payload[5..].starts_with(b"grpc-status: 7\r\n"));
}

#[tokio::test]
async fn a_stream_without_a_path_is_answered_unimplemented() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(message(2, b"content-type: application/grpc-web+proto\r\n"))
        .await
        .unwrap();
    let (id, last, payload) = next(&mut h.client).await;
    assert_eq!((id, last), (2, true));
    assert!(metadata(&payload).contains(&"grpc-status: 12".to_string()));
    assert!(h.events.lock().unwrap().is_empty(), "no call was made");
}

#[tokio::test]
async fn binary_headers_stay_in_base64() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(0, "pkg.Service/Method", "x-bin: AAEC\r\n"))
        .await
        .unwrap();
    let lines = metadata(&next(&mut h.client).await.2);
    assert!(
        lines.contains(&"x-seen-x-bin: AAEC".to_string()),
        "{lines:?}"
    );
}

#[tokio::test]
async fn closing_the_socket_cancels_its_calls() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(0, "pkg.Service/Pending", ""))
        .await
        .unwrap();
    h.client
        .send(headers(1, "pkg.Service/Stream", ""))
        .await
        .unwrap();
    assert_eq!(next(&mut h.client).await.0, 1);
    // a pending call sends nothing
    assert!(
        tokio::time::timeout(Duration::from_millis(200), h.client.next())
            .await
            .is_err()
    );
    h.client.close(None).await.unwrap();
    let events = events_when(&h.events, "pending call dropped").await;
    assert!(!events.contains(&"half closed /pkg.Service/Stream".to_string()));
    // the open request stream ends with an error, not as if the client had finished
    events_when(&h.events, "request failed /pkg.Service/Stream: Unknown").await;
    assert_eq!(h.ended.recv().await, None);
}

#[tokio::test]
async fn a_text_message_is_a_protocol_error() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(0, "pkg.Service/Stream", ""))
        .await
        .unwrap();
    next(&mut h.client).await;
    h.client.send(Message::text("hello")).await.unwrap();
    assert_eq!(
        close_reason(&mut h.client).await,
        (
            CloseCode::Protocol,
            "Can't read string payloads".to_string()
        )
    );
    events_when(&h.events, "request failed /pkg.Service/Stream: Unknown").await;
}

#[tokio::test]
async fn a_message_with_nothing_after_the_stream_id_is_a_protocol_error() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client.send(message(0, b"")).await.unwrap();
    assert_eq!(
        close_reason(&mut h.client).await,
        (CloseCode::Protocol, "Unexpected empty message".to_string())
    );
}

#[tokio::test]
async fn bytes_after_the_finish_byte_are_a_protocol_error() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client
        .send(headers(0, "pkg.Service/Stream", ""))
        .await
        .unwrap();
    next(&mut h.client).await;
    h.client.send(message(LAST, &[1, 2])).await.unwrap();
    assert_eq!(
        close_reason(&mut h.client).await,
        (
            CloseCode::Protocol,
            "Unexpected bytes in close message".to_string()
        )
    );
}

#[tokio::test]
async fn shutdown_sends_go_away_and_the_acknowledgement_ends_new_streams() {
    let mut h = connect(Framing::Multiplexed).await;
    h.closing.send(true).unwrap();
    assert_eq!(next(&mut h.client).await, (GO_AWAY, false, Bytes::new()));
    // a stream sent before the client saw the GO_AWAY is still served
    h.client
        .send(headers(0, "pkg.Service/Method", ""))
        .await
        .unwrap();
    assert_eq!(next(&mut h.client).await.0, 0);
    h.client.send(finish(GO_AWAY as u32)).await.unwrap();
    h.client
        .send(headers(1, "pkg.Service/Method", ""))
        .await
        .unwrap();
    assert_eq!(
        close_reason(&mut h.client).await,
        (
            CloseCode::Protocol,
            "Stream created after closing initiated".to_string()
        )
    );
}

#[tokio::test]
async fn an_acknowledgement_without_a_go_away_is_a_protocol_error() {
    let mut h = connect(Framing::Multiplexed).await;
    h.client.send(finish(GO_AWAY as u32)).await.unwrap();
    assert_eq!(
        close_reason(&mut h.client).await,
        (CloseCode::Protocol, "Unexpected close ACK".to_string())
    );
}

#[tokio::test]
async fn a_socket_per_call_has_no_stream_ids_and_closes_after_the_trailers() {
    let mut h = connect(Framing::PerSocket {
        method: "pkg.Service/Method".to_string(),
    })
    .await;
    let binary = |bytes: &[u8]| Message::Binary(Bytes::copy_from_slice(bytes));
    h.client
        .send(binary(b"content-type: application/grpc-web+proto\r\n"))
        .await
        .unwrap();
    // headers and trailers are each two messages: the frame prefix, then the text
    let prefix = receive(&mut h.client).await;
    let text = receive(&mut h.client).await;
    assert_eq!(prefix[0], METADATA_FRAME);
    assert_eq!(
        u32::from_be_bytes(prefix[1..].try_into().unwrap()) as usize,
        text.len()
    );
    assert!(std::str::from_utf8(&text)
        .unwrap()
        .contains("x-path: /pkg.Service/Method\r\n"));
    h.client.send(binary(&[0, b'h', b'i'])).await.unwrap();
    assert_eq!(receive(&mut h.client).await, Bytes::from_static(b"hi"));
    h.client.send(binary(&[1])).await.unwrap();
    assert_eq!(receive(&mut h.client).await[0], METADATA_FRAME);
    assert_eq!(
        receive(&mut h.client).await,
        Bytes::from_static(b"grpc-status: 0\r\n")
    );
    assert!(matches!(
        h.client.next().await,
        Some(Ok(Message::Close(_))) | None
    ));
}

#[test]
fn the_first_offered_subprotocol_the_server_has_is_chosen() {
    let offer = |value: &'static str| {
        let mut headers = HeaderMap::new();
        headers.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(value));
        choose_protocol(&headers)
    };
    assert_eq!(
        offer("grpc-websockets-multiplex, grpc-websockets"),
        Some(MULTIPLEX_PROTOCOL)
    );
    assert_eq!(
        offer("grpc-websockets, grpc-websockets-multiplex"),
        Some(PER_SOCKET_PROTOCOL)
    );
    assert_eq!(
        offer("other,grpc-websockets-multiplex"),
        Some(MULTIPLEX_PROTOCOL)
    );
    assert_eq!(offer("other"), None);
    assert_eq!(choose_protocol(&HeaderMap::new()), None);
}

#[test]
fn a_status_message_is_encoded_only_where_it_must_be() {
    // as the services write it: spaces and all punctuation escaped
    let status = Status::new(Code::Internal, "a b: 100% \u{e9}\n");
    let frame = metadata_frame(&status_headers(&status));
    assert_eq!(
        std::str::from_utf8(&frame[5..]).unwrap(),
        "grpc-status: 13\r\ngrpc-message: a b: 100%25 %C3%A9%0A\r\n"
    );
    assert_eq!(grpc_message(b"plain"), b"plain");
    // a percent sign that starts no escape is itself escaped
    assert_eq!(grpc_message(b"50%"), b"50%25");
}

#[test]
fn a_metadata_frame_leaves_out_the_content_length() {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", HeaderValue::from_static("0"));
    headers.insert("grpc-status", HeaderValue::from_static("12"));
    assert_eq!(&metadata_frame(&headers)[5..], b"grpc-status: 12\r\n");
}

#[test]
fn the_endpoint_is_two_path_segments() {
    assert!(is_endpoint("/pkg.Service/Method"));
    assert!(is_endpoint("/ide/x"));
    assert!(!is_endpoint("/x"));
    assert!(!is_endpoint("/a/b/c"));
    assert!(!is_endpoint("/a/"));
    assert!(!is_endpoint("/"));
}
