//! The Arrow Flight service.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use oxhaven_contracts::table::Table;
use oxhaven_proto::barrage::{BarrageMessageType, BarrageMessageWrapper, MAGIC as BARRAGE_MAGIC};
use oxhaven_proto::flight::flight_service_server::FlightService;
use oxhaven_proto::flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, PollInfo, PutResult, SchemaResult, Ticket,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use super::encode::schema_message;
use super::upload::{not_ported as not_ported_status, TableUpload, UploadError};
use super::wire::wire_schema;
use crate::barrage::{snapshot_request_messages, Barrage, WireOptions};
use crate::schema::schema_ipc_bytes;
use crate::session::{
    current_session, handshake, optional_session, DescribedFlight, Export, ExportBuilder,
    ExportError, ExportValue, HandshakeStream, Reply, Session, SessionStore, TicketRouter,
};

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn not_ported<T>(java_method: &str) -> Result<T, Status> {
    Err(Status::unimplemented(format!(
        "not ported yet: io.deephaven.server.arrow.FlightServiceGrpcImpl#{java_method}"
    )))
}

fn boxed<T: Send + 'static>(
    receiver: mpsc::UnboundedReceiver<Result<T, Status>>,
) -> Response<BoxStream<T>> {
    Response::new(Box::pin(UnboundedReceiverStream::new(receiver)))
}

/// What `GetFlightInfo` answers for a table: its schema, one endpoint with the ticket to
/// read it by, and its size, which is unknown (-1) for a refreshing table.
///
/// Behavior: `io.deephaven.server.session.TicketRouter#getFlightInfo`
pub fn flight_info(
    table: &Arc<dyn Table>,
    descriptor: FlightDescriptor,
    ticket: Vec<u8>,
) -> FlightInfo {
    FlightInfo {
        schema: schema_ipc_bytes(&wire_schema(table)).into(),
        flight_descriptor: Some(descriptor),
        endpoint: vec![FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket.into(),
            }),
            ..Default::default()
        }],
        total_records: if table.is_refreshing() {
            -1
        } else {
            table.row_set().size() as i64
        },
        total_bytes: -1,
        ..Default::default()
    }
}

/// Behavior: `io.deephaven.server.arrow.FlightServiceGrpcImpl`
pub struct FlightGrpc {
    store: Arc<SessionStore>,
    router: Arc<TicketRouter>,
    barrage: Arc<Barrage>,
}

impl FlightGrpc {
    /// Must be called inside a tokio runtime: Barrage sends updates from its blocking
    /// threads.
    pub fn new(store: Arc<SessionStore>, router: Arc<TicketRouter>) -> Self {
        Self {
            store,
            router,
            barrage: Barrage::on_runtime(tokio::runtime::Handle::current()),
        }
    }

    /// Answers a unary call with something computed from the flight info of the table a
    /// descriptor names, once that table is exported. Exports and shared tickets can be
    /// named by a descriptor, and so can scope variables; the application resolver has no
    /// route yet.
    ///
    /// Behavior: `FlightServiceGrpcImpl#getFlightInfo`, `#getSchema`, and
    /// `flightInfoFor` of `io.deephaven.server.session.ExportTicketResolver` and
    /// `SharedTicketResolver`
    async fn with_flight_info<T: Send + 'static>(
        &self,
        request: Request<FlightDescriptor>,
        answer: impl FnOnce(FlightInfo) -> T + Send + 'static,
    ) -> Result<Response<T>, Status> {
        let session = optional_session(&request);
        let descriptor = request.into_inner();
        let log_id = "request";
        let DescribedFlight {
            export,
            ticket,
            not_a_table,
        } = self
            .router
            .flight_for(session.as_ref(), &descriptor, log_id)?;
        // a resolver that answers without a session has refused the call above
        let session = session
            .ok_or_else(|| Status::unauthenticated("Could not resolve 'request': no session"))?;
        let reply = Reply::new();
        let (on_error, on_success) = (reply.completer(), reply.completer());
        session
            .non_export()?
            .require([export])
            .on_error(move |status| on_error(Err(status)))
            .submit(move |inputs| {
                let Some(table) = inputs[0].as_table() else {
                    return Err(not_a_table.into());
                };
                let info = flight_info(table, descriptor, ticket?);
                on_success(Ok(answer(info)));
                Ok(ExportValue::unit())
            })?;
        reply.wait().await
    }
}

/// The messages of a `DoGet` of `table`: its schema, then its rows as record batches.
///
/// A refreshing table without rows still gives one record batch; a static one gives none.
///
/// Behavior: `io.deephaven.server.arrow.ArrowFlightUtil#DoGetCustom`,
/// `io.deephaven.extensions.barrage.util.BarrageUtil#createAndSendSnapshot`,
/// `BarrageUtil#createAndSendStaticSnapshot`
pub fn do_get_messages(table: &Arc<dyn Table>) -> Result<Vec<FlightData>, ExportError> {
    // The schema is computed once: the batches are written the way it describes them.
    let schema = wire_schema(table);
    let nullable: Vec<bool> = table
        .definition()
        .columns()
        .iter()
        .map(|column| {
            schema
                .field_with_name(&column.name)
                .map_or(true, |field| field.is_nullable())
        })
        .collect();
    // Java attaches a `BarrageUpdateMetadata` to the first batch of each part of the
    // snapshot, which a Flight client ignores.
    let batches =
        snapshot_request_messages(table, &nullable, None, None, false, WireOptions::default())?;
    let mut messages = Vec::with_capacity(1 + batches.len());
    messages.push(schema_message(&schema));
    messages.extend(batches);
    Ok(messages)
}

/// What a `DoPut` call has received so far.
struct Put {
    session: Arc<Session>,
    router: Arc<TicketRouter>,
    descriptor: Option<FlightDescriptor>,
    /// The definition of the export the table goes to, until it is submitted.
    builder: Option<ExportBuilder>,
    upload: TableUpload,
    responses: mpsc::UnboundedSender<Result<PutResult, Status>>,
}

impl Put {
    /// Behavior: `io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver#processMessage`
    fn message(&mut self, message: FlightData) -> Result<(), UploadError> {
        if let Some(descriptor) = message.flight_descriptor {
            match &self.descriptor {
                Some(first) if *first != descriptor => {
                    return Err(Status::invalid_argument(
                        "additional flight descriptor sent does not match original descriptor",
                    )
                    .into());
                }
                Some(_) => {}
                None => {
                    // Behavior: TicketRouter#publish(SessionState, FlightDescriptor, ..)
                    let builder = self.router.publish_descriptor(
                        &self.session,
                        &descriptor,
                        "Flight.Descriptor",
                    )?;
                    let responses = self.responses.clone();
                    self.builder = Some(builder.on_error(move |status| {
                        let _ = responses.send(Err(status));
                    }));
                    self.descriptor = Some(descriptor);
                }
            }
        }
        // A Barrage client may say how it serialized the data (columns as lists, Deephaven
        // nulls). Anything else in `app_metadata` is ignored, as in Java.
        let options = flatbuffers::root::<BarrageMessageWrapper>(&message.app_metadata)
            .ok()
            .filter(|wrapper| {
                wrapper.magic() == BARRAGE_MAGIC
                    && wrapper.msg_type() == BarrageMessageType::BarrageSerializationOptions
            });
        if options.is_some() {
            return Err(not_ported_status(
                "io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver#processMessage",
                "BarrageSerializationOptions on an upload",
            )
            .into());
        }
        if message.data_header.is_empty() {
            return Ok(());
        }
        if self
            .upload
            .message(&message.data_header, &message.data_body)?
        {
            // nothing to report, but the batch is acknowledged
            let _ = self.responses.send(Ok(PutResult::default()));
        }
        Ok(())
    }

    /// Fails the export with the error, which reaches the client through the export's error
    /// handler; without an export the client is told directly.
    ///
    /// Behavior: `ArrowFlightUtil.DoPutObserver#failStream`
    fn fail(&mut self, error: UploadError) {
        let export_error = match error {
            UploadError::Status(status) => ExportError::Status(status),
            UploadError::Internal(text) => ExportError::Internal(text),
        };
        match self.builder.take() {
            Some(builder) => {
                let error = export_error.clone();
                if let Err(status) = builder.submit(move |_| Err(error)) {
                    let _ = self.responses.send(Err(status));
                }
            }
            None => {
                let status = match export_error {
                    ExportError::Status(status) => status,
                    ExportError::Internal(text) => {
                        // Behavior: SessionService.ErrorTransformer#transform
                        let error_id = uuid::Uuid::new_v4();
                        tracing::error!("Internal Error '{error_id}' {text}");
                        Status::invalid_argument(format!("Details Logged w/ID '{error_id}'"))
                    }
                };
                let _ = self.responses.send(Err(status));
            }
        }
    }

    /// Behavior: `ArrowFlightUtil.DoPutObserver#onCompleted`
    fn complete(mut self) {
        let Some(builder) = self.builder.take() else {
            let _ = self.responses.send(Err(Status::invalid_argument(
                "Result flight descriptor never provided",
            )));
            return;
        };
        let table = match std::mem::take(&mut self.upload).finish() {
            Ok(table) => table,
            Err(UploadError::Status(status)) => {
                // Java throws here without touching the export, which stays undefined
                let _ = self.responses.send(Err(status));
                return;
            }
            Err(error) => {
                self.builder = Some(builder);
                return self.fail(error);
            }
        };
        let responses = self.responses.clone();
        // dropping the last sender completes the call
        let submitted = builder
            .on_success(move |_| drop(responses))
            .submit(move |_| Ok(ExportValue::Table(table)));
        if let Err(status) = submitted {
            let _ = self.responses.send(Err(status));
        }
    }
}

#[tonic::async_trait]
impl FlightService for FlightGrpc {
    type HandshakeStream = HandshakeStream;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<oxhaven_proto::flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(handshake(self.store.clone(), request)))
    }

    /// No kind of ticket that exists so far lists flights: exports are not listed.
    ///
    /// Behavior: `FlightServiceGrpcImpl#listFlights`,
    /// `io.deephaven.server.session.ExportTicketResolver#forAllFlightInfo`
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        if !request.get_ref().expression.is_empty() {
            return Err(Status::invalid_argument(
                "Criteria expressions are not supported",
            ));
        }
        Ok(Response::new(Box::pin(tokio_stream::empty())))
    }

    /// Behavior: `FlightServiceGrpcImpl#getFlightInfo`
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.with_flight_info(request, |info| info).await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        // the reference does not implement it either
        Err(Status::unimplemented(
            "Method arrow.flight.protocol.FlightService/PollFlightInfo is unimplemented",
        ))
    }

    /// Behavior: `FlightServiceGrpcImpl#getSchema`
    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.with_flight_info(request, |info| SchemaResult {
            schema: info.schema,
        })
        .await
    }

    /// Behavior: `FlightServiceGrpcImpl#doGetCustom`,
    /// `io.deephaven.server.arrow.ArrowFlightUtil#DoGetCustom`
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let session = current_session(&request)?;
        let ticket = request.into_inner().ticket;
        let log_name = self.router.log_name(&ticket, "table")?;
        let export: Export = self.router.resolve(Some(&session), &ticket, "table")?;
        let (responses, receiver) = mpsc::unbounded_channel();
        let failures = responses.clone();
        session
            .non_export()?
            .require([export])
            .on_error(move |status| {
                let _ = failures.send(Err(status));
            })
            .submit(move |inputs| {
                let Some(table) = inputs[0].as_table() else {
                    return Err(Status::failed_precondition(format!(
                        "Ticket ({log_name}) is not a subscribable table."
                    ))
                    .into());
                };
                for message in do_get_messages(table)? {
                    if responses.send(Ok(message)).is_err() {
                        // the client is gone
                        break;
                    }
                }
                Ok(ExportValue::unit())
            })?;
        Ok(boxed(receiver))
    }

    /// Uploads a table into the export a path descriptor names. The first message carries
    /// the descriptor and the schema; each record batch after it is acknowledged with an
    /// empty `PutResult`; the call completes once the table is exported.
    ///
    /// Behavior: `FlightServiceGrpcImpl#doPutCustom`,
    /// `io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver`
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let session = current_session(&request)?;
        let mut messages = request.into_inner();
        let (responses, receiver) = mpsc::unbounded_channel();

        // The end of the session ends the call, and whatever arrives afterwards is ignored.
        let abandoned = Arc::new(AtomicBool::new(false));
        let on_close = session.add_on_close({
            let abandoned = abandoned.clone();
            let responses = responses.clone();
            move || {
                abandoned.store(true, Ordering::SeqCst);
                let _ = responses.send(Err(Status::unauthenticated("Session expired")));
            }
        })?;

        let mut put = Put {
            session: session.clone(),
            router: self.router.clone(),
            descriptor: None,
            builder: None,
            upload: TableUpload::default(),
            responses,
        };
        tokio::spawn(async move {
            loop {
                let outcome = match messages.message().await {
                    Ok(Some(message)) => {
                        if abandoned.load(Ordering::SeqCst) {
                            continue;
                        }
                        put.message(message)
                    }
                    Ok(None) => break,
                    // the client cancelled, or the connection broke
                    Err(status) => Err(status.into()),
                };
                if let Err(error) = outcome {
                    put.fail(error);
                    session.remove_on_close(on_close);
                    return;
                }
            }
            session.remove_on_close(on_close);
            if !abandoned.load(Ordering::SeqCst) {
                put.complete();
            }
        });
        Ok(boxed(receiver))
    }

    /// Barrage: subscriptions to tables and snapshots of them; see [`crate::barrage`].
    ///
    /// Behavior: `FlightServiceGrpcImpl#doExchangeCustom`
    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let session = current_session(&request)?;
        let responses =
            self.barrage
                .exchange(session, self.router.clone(), request.into_inner())?;
        Ok(boxed(responses))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        not_ported("doAction")
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        not_ported("listActions")
    }
}
