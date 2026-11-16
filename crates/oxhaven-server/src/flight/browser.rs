//! Flight for browsers: `DoPut` as an `Open` call and `Next` calls.
//!
//! The web client uploads a table (`newTable`) through `OpenDoPut` and `NextDoPut`
//! whatever its transport, also over the websocket, where it could stream. It does so to
//! open a constituent of a partitioned table, whose key it uploads as a table of one row;
//! the series of a figure made with `by` are such constituents. The calls make one
//! `DoPut` call ([`super::service::put`]), tied together as `console::browser` says.
//!
//! `OpenHandshake` and `OpenDoExchange` with their `Next` calls are not here; the web
//! client of 42.9 logs in with a unary call and subscribes over the websocket.
//!
//! Behavior: `io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding`

use std::sync::Arc;

use oxhaven_proto::browser_flight::browser_flight_service_server::BrowserFlightService;
use oxhaven_proto::browser_flight::BrowserNextResponse;
use oxhaven_proto::flight::{FlightData, HandshakeRequest, HandshakeResponse, PutResult};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use super::service::{put, BoxStream};
use crate::console::browser::{self, stream_data, BrowserStream};
use crate::session::{current_session, TicketRouter};

fn not_ported<T>(method: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!(
        "not ported yet: io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding: {method}"
    )))
}

/// The `BrowserFlightService` of the server.
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

    /// `DoPut` for a browser: this call carries the first message and its response is
    /// the response stream; the later messages come one per `NextDoPut` call.
    ///
    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeOpen`, as
    /// `BrowserFlightServiceGrpcBinding#bindService` sets it up
    async fn open_do_put(
        &self,
        request: Request<FlightData>,
    ) -> Result<Response<Self::OpenDoPutStream>, Status> {
        let data = browser::open_headers(stream_data(request.metadata())?)?;
        let session = current_session(&request)?;
        let (stream, messages) = BrowserStream::open(&session, &data)?;
        let messages = UnboundedReceiverStream::new(messages).map(Ok);
        let responses = put(session, self.router.clone(), messages, Some(stream.clone()))?;
        // the first message is in order by itself, so nothing refuses it
        stream.on_message(request.into_inner(), &data)?;
        Ok(responses)
    }

    /// A later message of a stream that `OpenDoPut` opened, named by the stream headers
    /// of the call.
    ///
    /// Behavior: `GrpcServiceOverrideBuilder.BrowserStreamMethod#invokeNext`
    async fn next_do_put(
        &self,
        request: Request<FlightData>,
    ) -> Result<Response<BrowserNextResponse>, Status> {
        browser::next(request).await
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
