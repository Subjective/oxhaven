//! Flight's `DoExchange`: the requests a Barrage client sends, and the stream it is answered
//! on.
//!
//! Every request is a `FlightData` whose `app_metadata` is a `BarrageMessageWrapper`. The
//! first request decides what the call is: a subscription, whose later requests change its
//! viewport and columns, or a snapshot.
//!
//! What differs from Java:
//!
//! - A request that is not the flatbuffer it claims to be, or whose viewport is not an
//!   encoded row set, is answered `INVALID_ARGUMENT`. Java reads such bytes unchecked.
//! - `preview_list_length_limit` has nothing to apply to, as no column type is a list.
//! - A call that never sent a request is completed when the client closes its side. Java
//!   leaves it open.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::Table;
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageMessageWrapper, BarrageSnapshotRequest, BarrageSubscriptionRequest,
    MAGIC,
};
use oxhaven_proto::flight::FlightData;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use super::codec::{decode_row_set, ColumnSet};
use super::producer::{nullable_columns, Job, Listener, Producer, Scheduler, SubscriptionId};
use super::snapshot::snapshot_request_messages;
use super::writer::{batch_options, WireOptions};
use crate::flight::{schema_message, wire_schema};
use crate::session::{ExportError, ExportValue, OnCloseId, Session, TicketRouter};
use crate::table::support::internal_status;

/// Behavior: `io.deephaven.server.barrage.BarrageRequestHelpers#DEFAULT_MIN_UPDATE_INTERVAL_MS`
const DEFAULT_MIN_UPDATE_INTERVAL_MS: i64 = 1000;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs propagation jobs on the blocking threads of a tokio runtime.
struct TokioScheduler(Handle);

impl Scheduler for TokioScheduler {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as i64)
    }

    fn run_immediately(&self, job: Job) {
        self.0.spawn_blocking(job);
    }

    fn run_at(&self, time_ms: i64, job: Job) {
        let delay = Duration::from_millis((time_ms - self.now_ms()).max(0) as u64);
        let handle = self.0.clone();
        self.0.spawn(async move {
            tokio::time::sleep(delay).await;
            handle.spawn_blocking(job);
        });
    }
}

/// The response stream of a call, as the listener of a subscription.
///
/// Behavior: `io.deephaven.server.arrow.ArrowFlightUtil.MessageViewAdapter`
struct StreamListener {
    sender: Mutex<Option<mpsc::UnboundedSender<Result<FlightData, Status>>>>,
}

impl Listener for StreamListener {
    fn on_next(&self, messages: Vec<FlightData>) {
        if let Some(sender) = &*lock(&self.sender) {
            for message in messages {
                if sender.send(Ok(message)).is_err() {
                    // the client is gone
                    break;
                }
            }
        }
    }

    fn on_error(&self, status: Status) {
        if let Some(sender) = lock(&self.sender).take() {
            let _ = sender.send(Err(status));
        }
    }

    fn on_completed(&self) {
        // dropping the sender ends the stream
        lock(&self.sender).take();
    }
}

/// What a subscription or snapshot request asks for.
struct TableRequest {
    ticket: Option<Vec<u8>>,
    /// `None` for every column.
    columns: Option<ColumnSet>,
    /// `None` for the whole table.
    viewport: Option<RowSet>,
    reverse_viewport: bool,
    options: WireOptions,
    min_update_interval_ms: i64,
}

fn malformed(what: &str, problem: impl std::fmt::Display) -> Status {
    Status::invalid_argument(format!("malformed {what}: {problem}"))
}

/// Behavior: `io.deephaven.server.barrage.BarrageRequestHelpers#getViewport`
fn viewport_of(bytes: Option<&[u8]>, what: &str) -> Result<Option<RowSet>, Status> {
    bytes
        .map(|bytes| decode_row_set(bytes).map_err(|problem| malformed(what, problem)))
        .transpose()
}

impl TableRequest {
    /// Behavior: `io.deephaven.server.barrage.BarrageRequestHelpers`,
    /// `io.deephaven.extensions.barrage.BarrageSubscriptionOptions#of`
    fn subscription(payload: &[u8]) -> Result<Self, Status> {
        let what = "BarrageSubscriptionRequest";
        let request = flatbuffers::root::<BarrageSubscriptionRequest>(payload)
            .map_err(|problem| malformed(what, problem))?;
        let options = request.subscription_options();
        Ok(Self {
            ticket: request.ticket().map(|ticket| ticket.bytes().to_vec()),
            columns: request
                .columns()
                .map(|columns| ColumnSet::from_bytes(columns.bytes())),
            viewport: viewport_of(request.viewport().map(|viewport| viewport.bytes()), what)?,
            reverse_viewport: request.reverse_viewport(),
            options: options.map_or_else(WireOptions::default, |options| WireOptions {
                use_deephaven_nulls: options.use_deephaven_nulls(),
                columns_as_list: options.columns_as_list(),
                batch: batch_options(options.batch_size(), options.max_message_size()),
            }),
            // Behavior: BarrageRequestHelpers#getMinUpdateIntervalMs
            min_update_interval_ms: match options.map(|options| options.min_update_interval_ms()) {
                None | Some(0) => DEFAULT_MIN_UPDATE_INTERVAL_MS,
                Some(interval) => i64::from(interval),
            },
        })
    }

    /// Behavior: `io.deephaven.server.barrage.BarrageRequestHelpers`,
    /// `io.deephaven.extensions.barrage.BarrageSnapshotOptions#of`
    fn snapshot(payload: &[u8]) -> Result<Self, Status> {
        let what = "BarrageSnapshotRequest";
        let request = flatbuffers::root::<BarrageSnapshotRequest>(payload)
            .map_err(|problem| malformed(what, problem))?;
        let options = request.snapshot_options();
        Ok(Self {
            ticket: request.ticket().map(|ticket| ticket.bytes().to_vec()),
            columns: request
                .columns()
                .map(|columns| ColumnSet::from_bytes(columns.bytes())),
            viewport: viewport_of(request.viewport().map(|viewport| viewport.bytes()), what)?,
            reverse_viewport: request.reverse_viewport(),
            options: options.map_or_else(WireOptions::default, |options| WireOptions {
                use_deephaven_nulls: options.use_deephaven_nulls(),
                columns_as_list: false,
                batch: batch_options(options.batch_size(), options.max_message_size()),
            }),
            min_update_interval_ms: DEFAULT_MIN_UPDATE_INTERVAL_MS,
        })
    }
}

/// What a call turned out to be, by its first request.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Subscription,
    Snapshot,
}

/// A snapshot call is completed once the snapshot is sent and the client has closed its
/// side, whichever comes last.
///
/// Behavior: `io.deephaven.server.arrow.ArrowFlightUtil.HalfClosedState`
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum HalfClosed {
    #[default]
    DontClose,
    ClientClosed,
    FinishedSending,
    Closed,
}

#[derive(Default)]
struct ExchangeState {
    closed: bool,
    kind: Option<Kind>,
    subscription: Option<(Arc<Producer>, SubscriptionId)>,
    /// The requests received while the table of the first one is not exported yet; the
    /// first one is at the front.
    pre_export: Option<VecDeque<TableRequest>>,
    half_closed: HalfClosed,
}

/// One `DoExchange` call.
///
/// Behavior: `io.deephaven.server.arrow.ArrowFlightUtil.DoExchangeMarshaller`,
/// `io.deephaven.server.barrage.BarrageSubscriptionRequestHandler`,
/// `io.deephaven.server.barrage.BarrageSnapshotRequestHandler`
struct Exchange {
    barrage: Arc<Barrage>,
    session: Arc<Session>,
    router: Arc<TicketRouter>,
    listener: Arc<StreamListener>,
    /// Starts the messages of the errors Java words with the marshaller's identity.
    prefix: String,
    on_close: Mutex<Option<OnCloseId>>,
    state: Mutex<ExchangeState>,
}

impl Exchange {
    /// Behavior: `ArrowFlightUtil.DoExchangeMarshaller#onNext`
    fn on_message(self: &Arc<Self>, message: &FlightData, first: &mut bool) -> Result<(), Status> {
        // Behavior: BarrageProtoUtil#parseProtoMessage: metadata that is not a wrapper
        // with the magic number counts as no metadata
        let wrapper = flatbuffers::root::<BarrageMessageWrapper>(&message.app_metadata)
            .ok()
            .filter(|wrapper| wrapper.magic() == MAGIC);
        let kind = {
            let mut state = lock(&self.state);
            match (state.kind, &wrapper) {
                // the handler checks the message type
                (Some(kind), _) => kind,
                (None, Some(wrapper)) => {
                    let kind = match wrapper.msg_type() {
                        BarrageMessageType::BarrageSubscriptionRequest => Kind::Subscription,
                        BarrageMessageType::BarrageSnapshotRequest => Kind::Snapshot,
                        other => {
                            return Err(Status::invalid_argument(format!(
                                "{}received a message with unhandled BarrageMessageType: {}",
                                self.prefix, other.0
                            )));
                        }
                    };
                    state.kind = Some(kind);
                    kind
                }
                (None, None) => {
                    // An official Flight client may send a first message without
                    // metadata; only the first.
                    if !*first {
                        return Err(Status::invalid_argument(format!(
                            "{}failed to receive Barrage request metadata",
                            self.prefix
                        )));
                    }
                    *first = false;
                    return Ok(());
                }
            }
        };
        // Java fails with a NullPointerException here
        let wrapper = wrapper
            .ok_or_else(|| internal_status("a DoExchange message without Barrage metadata"))?;
        match kind {
            Kind::Subscription => self.subscription_request(&wrapper),
            Kind::Snapshot => self.snapshot_request(&wrapper),
        }
    }

    /// Behavior: `BarrageSubscriptionRequestHandler#handleMessage`, `#validateMessage`
    fn subscription_request(
        self: &Arc<Self>,
        wrapper: &BarrageMessageWrapper<'_>,
    ) -> Result<(), Status> {
        if wrapper.msg_type() != BarrageMessageType::BarrageSubscriptionRequest {
            return Err(Status::invalid_argument(
                "Request type cannot be changed after initialization, expected BarrageSubscriptionRequest metadata",
            ));
        }
        let payload = wrapper
            .msg_payload()
            .ok_or_else(|| Status::invalid_argument("Subscription request not supplied"))?;
        let request = TableRequest::subscription(payload.bytes())?;
        let ticket = {
            let mut state = lock(&self.state);
            if let Some((producer, id)) = &state.subscription {
                return apply(producer, *id, request);
            }
            if state.closed {
                return Ok(());
            }
            if let Some(waiting) = &mut state.pre_export {
                waiting.push_back(request);
                return Ok(());
            }
            let Some(ticket) = request.ticket.clone() else {
                self.listener
                    .on_error(Status::invalid_argument("Ticket not specified."));
                return Ok(());
            };
            state.pre_export = Some(VecDeque::from([request]));
            ticket
        };
        let log_name = self.router.log_name(&ticket, "table")?;
        let export = self.router.resolve(Some(&self.session), &ticket, "table")?;
        let (this, failed) = (self.clone(), self.clone());
        self.session
            .non_export()?
            .require([export])
            .on_error(move |status| {
                // Behavior: DoExchangeMarshaller#onError
                failed.listener.on_error(status);
                failed.try_close();
            })
            .submit(move |inputs| this.on_export_resolved(&inputs[0], &log_name))?;
        Ok(())
    }

    /// Subscribes to the table of the first request, then applies the requests that
    /// arrived meanwhile.
    ///
    /// Behavior: `BarrageSubscriptionRequestHandler#onExportResolved`,
    /// `io.deephaven.server.barrage.TableExchangeMarshaller#subscribe`
    fn on_export_resolved(
        &self,
        value: &ExportValue,
        log_name: &str,
    ) -> Result<ExportValue, ExportError> {
        let request = {
            let mut state = lock(&self.state);
            if state.closed {
                state.pre_export = None;
                return Ok(ExportValue::unit());
            }
            state.pre_export.as_mut().and_then(VecDeque::pop_front)
        };
        let Some(request) = request else {
            return Ok(ExportValue::unit());
        };
        let Some(table) = value.as_table() else {
            self.listener.on_error(Status::failed_precondition(format!(
                "Ticket ({log_name}) is not a subscribable table."
            )));
            return Ok(ExportValue::unit());
        };
        if table.is_failed() {
            return Err(Status::failed_precondition("Table is already failed").into());
        }
        let producer = self.barrage.producer(table, request.min_update_interval_ms);
        let id = producer.add_subscription(
            self.listener.clone(),
            request.options,
            request.columns,
            request.viewport,
            request.reverse_viewport,
        );
        let mut state = lock(&self.state);
        let waiting = state.pre_export.take().unwrap_or_default();
        let Some(id) = id else {
            return Ok(ExportValue::unit());
        };
        if state.closed {
            producer.remove_subscription(id);
            return Ok(ExportValue::unit());
        }
        state.subscription = Some((producer.clone(), id));
        for request in waiting {
            apply(&producer, id, request)?;
        }
        Ok(ExportValue::unit())
    }

    /// Behavior: `BarrageSnapshotRequestHandler#handleMessage`, `#validateMessage`,
    /// `io.deephaven.server.barrage.TableExchangeMarshaller#snapshot`
    fn snapshot_request(
        self: &Arc<Self>,
        wrapper: &BarrageMessageWrapper<'_>,
    ) -> Result<(), Status> {
        if wrapper.msg_type() != BarrageMessageType::BarrageSnapshotRequest {
            return Err(Status::invalid_argument(
                "Request type cannot be changed after initialization, expected BarrageSnapshotRequest metadata",
            ));
        }
        // Java fails with a NullPointerException without a payload
        let payload = wrapper
            .msg_payload()
            .ok_or_else(|| internal_status("a BarrageSnapshotRequest without a payload"))?;
        let request = TableRequest::snapshot(payload.bytes())?;
        let ticket = request.ticket.clone().unwrap_or_default();
        let log_name = self.router.log_name(&ticket, "table")?;
        let export = self.router.resolve(Some(&self.session), &ticket, "table")?;
        let (failed, sent, listener) = (self.clone(), self.clone(), self.listener.clone());
        self.session
            .non_export()?
            .require([export])
            .on_error(move |status| failed.listener.on_error(status))
            .on_success(move |_| {
                let mut state = lock(&sent.state);
                state.half_closed = match state.half_closed {
                    HalfClosed::DontClose => HalfClosed::FinishedSending,
                    // A second snapshot on the call: Java fails here with "Can't finish
                    // streaming twice", and the client sees the call completed.
                    _ => HalfClosed::Closed,
                };
                if state.half_closed == HalfClosed::Closed {
                    sent.listener.on_completed();
                }
            })
            .submit(move |inputs| {
                let Some(table) = inputs[0].as_table() else {
                    return Err(Status::failed_precondition(format!(
                        "Ticket ({log_name}) is has no associated exchange marshaller."
                    ))
                    .into());
                };
                if table.is_failed() {
                    return Err(Status::failed_precondition("Table is already failed").into());
                }
                let schema = wire_schema(table);
                listener.on_next(vec![schema_message(&schema)]);
                listener.on_next(snapshot_request_messages(
                    table,
                    &nullable_columns(table, &schema),
                    request.columns.as_ref(),
                    request.viewport.as_ref(),
                    request.reverse_viewport,
                    request.options,
                )?);
                Ok(ExportValue::unit())
            })?;
        Ok(())
    }

    /// Ends the call's subscription or, for a snapshot, notes that the client is done.
    ///
    /// Behavior: `DoExchangeMarshaller#close`, `BarrageSubscriptionRequestHandler#close`,
    /// `BarrageSnapshotRequestHandler#close`
    fn close(&self) {
        let mut state = lock(&self.state);
        if state.closed {
            return;
        }
        state.closed = true;
        match state.kind {
            Some(Kind::Subscription) => {
                state.pre_export = None;
                match state.subscription.take() {
                    // the producer completes the stream
                    Some((producer, id)) => producer.remove_subscription(id),
                    None => self.listener.on_completed(),
                }
            }
            Some(Kind::Snapshot) => {
                state.half_closed = match state.half_closed {
                    HalfClosed::DontClose => HalfClosed::ClientClosed,
                    HalfClosed::FinishedSending => HalfClosed::Closed,
                    other => other,
                };
                if state.half_closed == HalfClosed::Closed {
                    self.listener.on_completed();
                }
            }
            None => self.listener.on_completed(),
        }
    }

    /// Closes the call unless the end of the session has, or is about to.
    ///
    /// Behavior: `DoExchangeMarshaller#tryClose`
    fn try_close(&self) {
        let registered = lock(&self.on_close).take();
        if registered.is_some_and(|id| self.session.remove_on_close(id)) {
            self.close();
        }
    }
}

/// Behavior: `BarrageSubscriptionRequestHandler#apply`,
/// `TableExchangeMarshaller.Subscription#update`
fn apply(producer: &Producer, id: SubscriptionId, request: TableRequest) -> Result<(), Status> {
    let found = producer.update_subscription(
        id,
        request.viewport,
        request.columns,
        request.reverse_viewport,
    );
    if found {
        Ok(())
    } else {
        Err(Status::not_found("Subscription was not found."))
    }
}

/// The Barrage side of the Flight service: the producers of the tables that have
/// subscribers, one per table and update interval.
pub struct Barrage {
    scheduler: Arc<dyn Scheduler>,
    /// By the address of the table, which a live producer keeps from being reused, and the
    /// update interval.
    producers: Mutex<HashMap<(usize, i64), Weak<Producer>>>,
}

impl Barrage {
    pub fn new(scheduler: Arc<dyn Scheduler>) -> Arc<Self> {
        Arc::new(Self {
            scheduler,
            producers: Mutex::new(HashMap::new()),
        })
    }

    /// Runs propagation jobs on the blocking threads of `runtime`.
    pub fn on_runtime(runtime: Handle) -> Arc<Self> {
        Self::new(Arc::new(TokioScheduler(runtime)))
    }

    /// The producer of `table` that sends updates no more often than every
    /// `update_interval_ms`: the one there is, or a new one.
    ///
    /// Behavior: `io.deephaven.server.barrage.BarrageMessageProducer.Operation#getMemoizedOperationKey`
    pub fn producer(&self, table: &Arc<dyn Table>, update_interval_ms: i64) -> Arc<Producer> {
        let key = (Arc::as_ptr(table).cast::<()>() as usize, update_interval_ms);
        let mut producers = lock(&self.producers);
        if let Some(producer) = producers.get(&key).and_then(Weak::upgrade) {
            return producer;
        }
        producers.retain(|_, producer| producer.strong_count() > 0);
        let producer = Producer::new(table.clone(), self.scheduler.clone(), update_interval_ms);
        producers.insert(key, Arc::downgrade(&producer));
        producer
    }

    /// Serves one `DoExchange` call: reads `requests` until the client closes its side,
    /// and answers on the returned stream.
    ///
    /// Behavior: `io.deephaven.server.arrow.FlightServiceGrpcImpl#doExchangeCustom`
    pub fn exchange(
        self: &Arc<Self>,
        session: Arc<Session>,
        router: Arc<TicketRouter>,
        mut requests: Streaming<FlightData>,
    ) -> Result<mpsc::UnboundedReceiver<Result<FlightData, Status>>, Status> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let exchange = Arc::new(Exchange {
            barrage: self.clone(),
            session,
            router,
            listener: Arc::new(StreamListener {
                sender: Mutex::new(Some(sender)),
            }),
            prefix: String::new(),
            on_close: Mutex::new(None),
            state: Mutex::new(ExchangeState::default()),
        });
        let prefix = format!(
            "DoExchangeMarshaller{{{:x}}}: ",
            Arc::as_ptr(&exchange) as usize
        );
        let mut exchange = exchange;
        if let Some(exchange) = Arc::get_mut(&mut exchange) {
            exchange.prefix = prefix;
        }
        // the end of the session ends the call
        let on_close = exchange.session.add_on_close({
            let exchange = Arc::downgrade(&exchange);
            move || {
                if let Some(exchange) = exchange.upgrade() {
                    exchange.close();
                }
            }
        })?;
        *lock(&exchange.on_close) = Some(on_close);

        tokio::spawn(async move {
            let mut first = true;
            loop {
                match requests.message().await {
                    Ok(Some(message)) => {
                        if let Err(status) = exchange.on_message(&message, &mut first) {
                            exchange.listener.on_error(status);
                            break;
                        }
                    }
                    // Behavior: DoExchangeMarshaller#onCompleted
                    Ok(None) => break,
                    // the client cancelled, or the connection broke
                    // Behavior: DoExchangeMarshaller#onError, #onCancel
                    Err(status) => {
                        exchange.listener.on_error(status);
                        break;
                    }
                }
            }
            exchange.try_close();
        });
        Ok(receiver)
    }
}
