//! Flight's `DoPut` for a browser: the client stream as an `OpenDoPut` call and one
//! `NextDoPut` call per later message ([`crate::console::browser`] has how the calls
//! make a stream). The web client uploads a table this way whatever its transport, and
//! the key table of a hierarchical table's view is such an upload.
//!
//! The browser forms of `Handshake` and `DoExchange` are not ported; the web client
//! makes those calls on the Flight service itself.
//!
//! Behavior: `io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding#bindService`

use std::pin::Pin;
use std::sync::Arc;

use oxhaven_proto::browser_flight::browser_flight_service_server::BrowserFlightService;
use oxhaven_proto::browser_flight::BrowserNextResponse;
use oxhaven_proto::flight::{FlightData, HandshakeRequest, HandshakeResponse, PutResult};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use super::service::put_stream;
use crate::console::browser::{stream_data, BrowserStream};
use crate::session::{current_session, is_terminal, ExportError, ExportValue, Reply, TicketRouter};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn not_ported<T>(method: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!(
        "not ported: io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding#bindService: \
         the browser stream {method}"
    )))
}

pub struct BrowserFlightGrpc {
    router: Arc<TicketRouter>,
}

impl BrowserFlightGrpc {
    pub fn new(router: Arc<TicketRouter>) -> Self {
        Self { router }
    }
}

#[tonic::async_trait]
impl BrowserFlightService for BrowserFlightGrpc {
    type OpenHandshakeStream = BoxStream<HandshakeResponse>;

    async fn open_handshake(
        &self,
        _request: Request<HandshakeRequest>,
    ) -> Result<Response<Self::OpenHandshakeStream>, Status> {
        not_ported("OpenHandshake")
    }

    async fn next_handshake(
        &self,
        _request: Request<HandshakeRequest>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        not_ported("NextHandshake")
    }

    type OpenDoPutStream = BoxStream<PutResult>;

    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeOpen` around
    /// `FlightServiceGrpcImpl#doPutCustom`
    async fn open_do_put(
        &self,
        request: Request<FlightData>,
    ) -> Result<Response<Self::OpenDoPutStream>, Status> {
        let data = stream_data(request.metadata())?;
        let session = current_session(&request)?;
        let Some(data) = data else {
            return Err(Status::invalid_argument(
                "no x-deephaven-stream headers, cannot handle open request",
            ));
        };
        if data.sequence != 0 {
            // the messages are delivered in order from 0, so the stream would wait for
            // ever for a message that cannot come
            return Err(Status::invalid_argument(format!(
                "open request must have sequence 0, received {}",
                data.sequence
            )));
        }
        let (stream, messages) = BrowserStream::open(&session, &data)?;
        let messages = UnboundedReceiverStream::new(messages).map(Ok);
        let ended = stream.clone();
        let responses = put_stream(session, self.router.clone(), messages, move || {
            ended.cancelled()
        })?;
        // the first message is in order by itself, so nothing refuses it
        stream.on_message(request.into_inner(), &data)?;
        Ok(Response::new(responses))
    }

    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeNext`
    async fn next_do_put(
        &self,
        request: Request<FlightData>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        let no_headers = || {
            Status::invalid_argument("no x-deephaven-stream headers, cannot handle next request")
        };
        let data = stream_data(request.metadata())?.ok_or_else(no_headers)?;
        let export_id = data.export_id.ok_or_else(no_headers)?;
        if data.sequence <= 0 {
            // sequence 0 is the message of the open call
            return Err(Status::invalid_argument(format!(
                "next request must have a positive sequence, received {}",
                data.sequence
            )));
        }
        let session = current_session(&request)?;
        let stream = session.get_export(export_id)?;
        if is_terminal(stream.state()) {
            // The stream has ended. A half close has nothing to deliver; a message has,
            // and the client is told that it was not.
            if data.half_close {
                return Ok(Response::new(BrowserNextResponse::default()));
            }
            return Err(Status::cancelled(format!(
                "stream ended before this message arrived; sequence {} was not delivered",
                data.sequence
            )));
        }
        let message = request.into_inner();
        let reply = Reply::new();
        let (on_error, on_success) = (reply.completer(), reply.completer());
        session
            .non_export()?
            .require([stream])
            .on_error(move |status| on_error(Err(status)))
            .submit(move |inputs| {
                // Java casts and fails with a ClassCastException
                let stream = inputs[0]
                    .downcast_ref::<Arc<BrowserStream<FlightData>>>()
                    .ok_or_else(|| {
                        ExportError::Internal(
                            "the stream ticket does not name a browser stream".to_string(),
                        )
                    })?;
                stream
                    .on_message(message, &data)
                    .map_err(ExportError::Status)?;
                on_success(Ok(BrowserNextResponse::default()));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }

    type OpenDoExchangeStream = BoxStream<FlightData>;

    async fn open_do_exchange(
        &self,
        _request: Request<FlightData>,
    ) -> Result<Response<Self::OpenDoExchangeStream>, Status> {
        not_ported("OpenDoExchange")
    }

    async fn next_do_exchange(
        &self,
        _request: Request<FlightData>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        not_ported("NextDoExchange")
    }
}
