//! What a browser needs around a gRPC call over HTTP: the gRPC-web translation, CORS, and
//! the headers Java adds to every response.

use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http::header::{
    HeaderMap, HeaderName, HeaderValue, ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
    ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE,
    ACCESS_CONTROL_REQUEST_METHOD, CONTENT_TYPE, ORIGIN, VARY,
};
use http::{Method, Request, Response, StatusCode};
use http_body::Frame;
use tonic::Status;
use tower::ServiceExt;

use super::websocket::metadata_frame;
use super::{empty_response, Body, GrpcService};

const GRPC: &str = "application/grpc";
const GRPC_WEB: &str = "application/grpc-web";

fn content_type(headers: &HeaderMap) -> &str {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
}

/// Whether the request is a gRPC call, in the native or the gRPC-web form.
///
/// Behavior: `io.grpc.servlet.jakarta.ServletAdapter#isGrpc`
pub(crate) fn is_grpc(headers: &HeaderMap) -> bool {
    content_type(headers).contains(GRPC)
}

/// Behavior: `io.grpc.servlet.jakarta.web.GrpcWebFilter#isGrpcWeb`
fn is_grpc_web(headers: &HeaderMap) -> bool {
    content_type(headers).starts_with(GRPC_WEB)
}

/// The answer to a gRPC call that did not arrive over HTTP/2: gRPC needs trailers and
/// streams in both directions, which HTTP/1.1 does not give it. A browser on a plain
/// `http://` page uses the websocket instead.
///
/// Behavior: `io.deephaven.server.jetty.GrpcFilter#doFilter`
pub(crate) fn not_http2(request_headers: &HeaderMap) -> Response<Body> {
    let mut response = empty_response(StatusCode::OK);
    let headers = response.headers_mut();
    let content_type = if is_grpc_web(request_headers) {
        GRPC_WEB
    } else {
        GRPC
    };
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert("grpc-status", HeaderValue::from_static("13"));
    headers.insert(
        "grpc-message",
        HeaderValue::from_static(
            "The server connection is not using http2, so streams and metadata may not behave \
             as expected. The client may be connecting improperly, or a proxy may be \
             interfering.",
        ),
    );
    response
}

/// The time the client gives the call, from its `grpc-timeout` header: a number of at most
/// eight digits and a unit.
fn grpc_timeout(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get("grpc-timeout")?.to_str().ok()?;
    let (amount, unit) = value.split_at(value.len().checked_sub(1)?);
    if amount.is_empty() || amount.len() > 8 {
        return None;
    }
    let amount: u64 = amount.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(amount * 60 * 60),
        "M" => Duration::from_secs(amount * 60),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return None,
    })
}

/// Whether `headers`, the headers or trailers that end a call, carry a status message that
/// Java's server does not send: one that ends in a space.
///
/// gRPC writes the message into the `grpc-message` field with only the bytes outside
/// printable ASCII and the percent sign escaped, so a message that ends in a space is a
/// field value that ends in a space, which HTTP/2 forbids (RFC 9113, section 8.2.1). Seen
/// from a client of the reference server, for unary and for streaming calls: the status
/// never arrives, the stream is reset with `CANCEL`, and the call ends as `CANCELLED`. A
/// message that ends in a tab or a line feed is escaped and arrives. The messages this
/// happens to end in text the client sent: `invalid table reference: ` for a `Batch`
/// operation whose table reference is unset, and `column(s) not found: A ` for a column
/// name that ends in a space.
///
/// tonic escapes the space as well, so here the field value ends in `%20`; a percent sign
/// of the message itself is written `%25`.
fn has_unsendable_message(headers: &HeaderMap) -> bool {
    headers.get("grpc-message").is_some_and(|message| {
        let message = message.as_bytes();
        message.ends_with(b"%20") || message.ends_with(b" ")
    })
}

/// The stream error that makes hyper reset a stream with `CANCEL`.
fn cancel_stream() -> h2::Error {
    h2::Error::from(h2::Reason::CANCEL)
}

/// A response body that resets its stream instead of sending trailers Java would not send:
/// see [`has_unsendable_message`].
///
/// The messages before the trailers may not reach the client: h2 drops the frames of a
/// stream that it has not written yet when the stream is reset. A client of the reference
/// server has the same race, and receives all, some or none of the responses of a batch
/// that ends this way from one run to the next.
struct ResetOnUnsendableTrailers {
    body: Body,
}

impl http_body::Body for ResetOnUnsendableTrailers {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let frame = ready!(Pin::new(&mut self.body).poll_frame(cx));
        if let Some(Ok(frame)) = &frame {
            if frame.trailers_ref().is_some_and(has_unsendable_message) {
                // hyper resets the stream with the reason of an `h2::Error` it finds among
                // the sources of a body's error
                let mut reset = Status::cancelled("the status message ends in a space");
                reset.set_source(std::sync::Arc::new(cancel_stream()));
                return Poll::Ready(Some(Err(reset)));
            }
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

/// Makes a gRPC call that arrived over HTTP/2. A gRPC-web call is the same call with
/// another content type, and its trailers travel as the last frame of the response body.
///
/// A call that has not produced its response headers within the client's `grpc-timeout` is
/// cancelled. This is what the tonic server did for the calls before this module replaced
/// it, and no more: Java also ends a streaming call at its deadline.
///
/// A native call whose status Java's server would not send is not answered: the error
/// returned here resets its stream, at once if the status is the whole response, and
/// otherwise where its trailers would have been. See [`has_unsendable_message`]. What the
/// reference server does with such a status in a gRPC-web response, where the trailers
/// are part of the body, has not been observed, and it is sent as it is.
///
/// Behavior: `io.grpc.servlet.jakarta.web.GrpcWebFilter#doFilter`,
/// `GrpcWebServletRequest#getContentType`, `GrpcWebServletResponse#setContentType`
pub(crate) async fn call(
    grpc: GrpcService,
    mut request: Request<Body>,
) -> Result<Response<Body>, h2::Error> {
    let web = is_grpc_web(request.headers());
    if web {
        let translated = content_type(request.headers()).replacen(GRPC_WEB, GRPC, 1);
        if let Ok(value) = HeaderValue::from_str(&translated) {
            request.headers_mut().insert(CONTENT_TYPE, value);
        }
    }
    let timeout = grpc_timeout(request.headers());
    let call = grpc.oneshot(request);
    let response = match timeout {
        None => call.await,
        Some(timeout) => match tokio::time::timeout(timeout, call).await {
            Ok(response) => response,
            Err(_) => Ok(Status::cancelled("Timeout expired").into_http()),
        },
    };
    let response = match response {
        Ok(response) => response,
        Err(never) => match never {},
    };
    if !web {
        if has_unsendable_message(response.headers()) {
            return Err(cancel_stream());
        }
        return Ok(response.map(|body| Body::new(ResetOnUnsendableTrailers { body })));
    }
    let (mut parts, body) = response.into_parts();
    let translated = content_type(&parts.headers).replacen(GRPC, GRPC_WEB, 1);
    if let Ok(value) = HeaderValue::from_str(&translated) {
        parts.headers.insert(CONTENT_TYPE, value);
    }
    Ok(Response::from_parts(
        parts,
        Body::new(TrailersInBody { body, done: false }),
    ))
}

/// A response body whose trailers are written as a last data frame, since a browser cannot
/// read HTTP trailers.
///
/// Behavior: `io.grpc.servlet.jakarta.web.GrpcWebServletResponse#writeTrailers`
struct TrailersInBody {
    body: Body,
    done: bool,
}

impl http_body::Body for TrailersInBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if self.done {
            return Poll::Ready(None);
        }
        let frame = match ready!(Pin::new(&mut self.body).poll_frame(cx)) {
            Some(Ok(frame)) => frame,
            other => return Poll::Ready(other),
        };
        Poll::Ready(Some(Ok(match frame.into_trailers() {
            Ok(trailers) => {
                self.done = true;
                Frame::data(metadata_frame(&trailers))
            }
            Err(data) => data,
        })))
    }
}

/// Whether the request is a CORS preflight.
pub(crate) fn is_preflight<B>(request: &Request<B>) -> bool {
    request.method() == Method::OPTIONS
        && request.headers().contains_key(ORIGIN)
        && request
            .headers()
            .contains_key(ACCESS_CONTROL_REQUEST_METHOD)
}

/// The request headers a page of another origin may send, in the order Java was seen to
/// list them.
///
/// Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer#createCrossOriginHandler`
const ALLOWED_HEADERS: &str = "content-type,Auth-Token-bin,grpc-encoding,grpc-accept-encoding,\
x-deephaven-stream-sequence,Authorization,x-user-agent,content-encoding,x-grpc-web,\
Access-Control-Allow-Origin,Origin,grpc-timeout,accept-encoding,x-deephaven-stream-halfclose,\
x-deephaven-stream-ticket";

/// The response headers a page of another origin may read.
///
/// Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer#createCrossOriginHandler`
const EXPOSED_HEADERS: &str =
    "grpc-status,content-type,Authorization,grpc-message,grpc-status-details-bin";

/// Answers a preflight: any origin may `POST` with the headers gRPC, gRPC-web, Flight and
/// the browser streams use. Java answers the same whatever method and headers the
/// preflight asks about.
///
/// Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer#createCrossOriginHandler`
pub(crate) fn preflight() -> Response<Body> {
    let mut response = empty_response(StatusCode::OK);
    let headers = response.headers_mut();
    headers.insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("POST"),
    );
    headers.insert(
        ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(ALLOWED_HEADERS),
    );
    headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("60"));
    response
}

/// The headers Java adds to every response by configuration.
///
/// Behavior: `props/configs/src/main/resources/dh-defaults.prop` (`http.add.header.*`),
/// `io.deephaven.server.jetty.ConfiguredHeadersCustomizer`
const EXTRA_HEADERS: [(&str, &str); 6] = [
    ("referrer-policy", "no-referrer"),
    ("x-content-type-options", "nosniff"),
    ("cross-origin-resource-policy", "same-origin"),
    ("cross-origin-embedder-policy", "require-corp"),
    ("cross-origin-opener-policy", "same-origin"),
    ("x-xss-protection", "1; mode=block"),
];

/// Adds the configured headers, and for a request that names its origin the CORS headers:
/// every origin is allowed.
///
/// Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer#createCrossOriginHandler`
pub(crate) fn add_response_headers(
    response: &mut Response<Body>,
    origin: Option<&HeaderValue>,
    preflight: bool,
) {
    let headers = response.headers_mut();
    for (name, value) in EXTRA_HEADERS {
        headers.insert(
            HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
    headers.append(VARY, HeaderValue::from_static("Origin"));
    if let Some(origin) = origin {
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
        if !preflight {
            headers.insert(
                ACCESS_CONTROL_EXPOSE_HEADERS,
                HeaderValue::from_static(EXPOSED_HEADERS),
            );
        }
    }
}
