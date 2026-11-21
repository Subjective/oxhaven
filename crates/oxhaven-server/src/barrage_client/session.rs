//! A session with another server, and the tables fetched, snapshotted and subscribed
//! through it.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use arrow::array::{new_null_array, ArrayRef};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use flatbuffers::FlatBufferBuilder;
use oxhaven_contracts::graph::{locked, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::table::{uploaded_table, QueryTable, TableSpec};
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageMessageWrapper, BarrageMessageWrapperArgs, BarrageSnapshotOptions,
    BarrageSnapshotOptionsArgs, BarrageSnapshotRequest, BarrageSnapshotRequestArgs,
    BarrageSubscriptionOptions, BarrageSubscriptionOptionsArgs, BarrageSubscriptionRequest,
    BarrageSubscriptionRequestArgs, MAGIC,
};
use oxhaven_proto::flight::flight_service_client::FlightServiceClient;
use oxhaven_proto::flight::FlightData;
use oxhaven_proto::grpc::config_service_client::ConfigServiceClient;
use oxhaven_proto::grpc::config_value::Kind;
use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{
    ConfigurationConstantsRequest, FetchTableRequest, HandshakeRequest, ReleaseRequest,
    TableReference, Ticket,
};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};

use super::error::{protocol, ClientError};
use super::message::{MessageReader, ReadOptions, RemoteMessage};
use super::replica::{live_table, Inbox, Replica, Wake};
use super::schema::RemoteSchema;
use crate::barrage::codec::{encode_row_set, ColumnSet};
use crate::flight::schema_from_ipc_bytes;
use crate::session::export_id_to_bytes;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The largest message the client accepts from a server.
///
/// Behavior: `io.deephaven.client.impl.ClientConfig#DEFAULT_MAX_INBOUND_MESSAGE_SIZE`
pub const DEFAULT_MAX_INBOUND_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// The authentication type of a pre-shared key.
///
/// Behavior: the class name of `io.deephaven.authentication.psk.PskAuthenticationHandler`
pub const PSK_AUTH_TYPE: &str = "io.deephaven.authentication.psk.PskAuthenticationHandler";

/// How often the server is pinged when it does not say how long its sessions last.
///
/// Behavior: `io.deephaven.client.impl.SessionImpl#create` ("defaulting to pinging the
/// server every minute")
const DEFAULT_PING_FREQUENCY: Duration = Duration::from_secs(60);

/// Which server to connect to, and as whom.
///
/// Port of the parts of `io.deephaven.client.impl.ClientConfig` and
/// `io.deephaven.client.impl.SessionImplConfig` that Python's
/// `deephaven.barrage.barrage_session` sets.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    /// `dh://` when set, `dh+plain://` otherwise.
    pub use_tls: bool,
    /// PEM encoded root certificates for a TLS connection, which then trusts these alone;
    /// `None` for the system's.
    pub tls_root_certs: Option<Vec<u8>>,
    /// The value of the `Authorization` header of the first call: a type, or a type and
    /// a value separated by a space.
    ///
    /// Behavior: `SessionImplConfig#authenticationTypeAndValue`, default `Anonymous`
    pub authorization: String,
    /// Headers sent with every call.
    ///
    /// Behavior: `ClientConfig#extraHeaders`
    pub extra_headers: Vec<(String, String)>,
    /// Behavior: `ClientConfig#maxInboundMessageSize`
    pub max_inbound_message_size: usize,
}

impl ClientConfig {
    /// A plaintext, anonymous connection to `host:port`.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        ClientConfig {
            host: host.into(),
            port,
            use_tls: false,
            tls_root_certs: None,
            authorization: "Anonymous".to_string(),
            extra_headers: Vec::new(),
            max_inbound_message_size: DEFAULT_MAX_INBOUND_MESSAGE_SIZE,
        }
    }

    /// Authenticates with `auth_type` and `auth_token`. An empty token sends the type
    /// alone.
    ///
    /// Behavior: `deephaven.barrage.barrage_session` (`auth_type`, `auth_token`)
    pub fn with_auth(mut self, auth_type: &str, auth_token: &str) -> Self {
        self.authorization = if auth_token.is_empty() {
            auth_type.to_string()
        } else {
            format!("{auth_type} {auth_token}")
        };
        self
    }

    /// Authenticates with the pre-shared key `key`.
    pub fn with_psk(self, key: &str) -> Self {
        self.with_auth(PSK_AUTH_TYPE, key)
    }
}

/// What a subscription asks of the server.
///
/// Port of `io.deephaven.extensions.barrage.BarrageSubscriptionOptions`; the default is
/// the default of its builder.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SubscriptionOptions {
    /// Nulls of primitive columns as Deephaven's null values and not in validity buffers.
    pub use_deephaven_nulls: bool,
    /// Each column wrapped in a list, so that columns may have different lengths.
    pub columns_as_list: bool,
    /// The shortest time between two updates, in milliseconds; 0 for the server's.
    pub min_update_interval_ms: i32,
    /// The most rows in one record batch; 0 for the server's.
    pub batch_size: i32,
    /// The largest message, in bytes; 0 for the server's.
    pub max_message_size: i32,
}

impl SubscriptionOptions {
    /// The options Java's URI resolver and Python's `BarrageSession.subscribe` use.
    ///
    /// Behavior: `io.deephaven.server.uri.BarrageTableResolver#SUB_OPTIONS`
    pub const RESOLVER: SubscriptionOptions = SubscriptionOptions {
        use_deephaven_nulls: true,
        columns_as_list: false,
        min_update_interval_ms: 0,
        batch_size: 0,
        max_message_size: 0,
    };
}

/// What a snapshot asks of the server.
///
/// Port of `io.deephaven.extensions.barrage.BarrageSnapshotOptions` with the viewport and
/// columns of `io.deephaven.client.impl.BarrageSnapshot#partialTable`; the default is the
/// default of the builder and the whole table.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct SnapshotOptions {
    pub use_deephaven_nulls: bool,
    /// The most rows in one record batch; 0 for the server's.
    pub batch_size: i32,
    /// The largest message, in bytes; 0 for the server's.
    pub max_message_size: i32,
    /// The positions of the rows wanted; `None` for all rows.
    pub viewport: Option<RowSet>,
    /// Whether `viewport` counts from the end of the table.
    pub reverse_viewport: bool,
    /// The names of the columns wanted; `None` for all columns. The table has every
    /// column, and the cells of the others are null.
    pub columns: Option<Vec<String>>,
}

impl SnapshotOptions {
    /// The options Java's URI resolver and Python's `BarrageSession.snapshot` use.
    ///
    /// Behavior: `io.deephaven.server.uri.BarrageTableResolver#SNAP_OPTIONS`
    pub fn resolver() -> Self {
        SnapshotOptions {
            use_deephaven_nulls: true,
            ..SnapshotOptions::default()
        }
    }
}

/// A table of the server, exported to this session.
///
/// Port of what `io.deephaven.client.impl.TableHandle` knows of its table.
#[derive(Clone, Debug)]
pub struct RemoteTable {
    /// The export of this session that holds the table on the server.
    pub export_id: i32,
    pub schema: RemoteSchema,
    /// Whether the server says the table never changes.
    pub is_static: bool,
    /// The size the table had when it was fetched.
    pub size: i64,
}

/// The bearer token the server last sent, and the one before it.
///
/// Behavior: `io.deephaven.client.impl.BearerHandler.BearerTokenState`
#[derive(Default)]
struct Bearer {
    previous: Option<String>,
    current: Option<String>,
}

struct Inner {
    channel: Channel,
    target: String,
    max_inbound_message_size: usize,
    extra_headers: Vec<(MetadataKey<Ascii>, MetadataValue<Ascii>)>,
    bearer: Mutex<Bearer>,
    next_export: AtomicI32,
    /// `true` once the session is closed.
    closed: watch::Sender<bool>,
    ping: Mutex<Option<tokio::task::JoinHandle<()>>>,
    graph: Arc<dyn UpdateGraph>,
    wake: Option<Wake>,
}

/// A session with another Deephaven-compatible server: what
/// `io.deephaven.client.impl.BarrageSession` is to Java's server.
///
/// Clones share the session. The tables a session subscribes keep it open; closing it
/// fails them.
#[derive(Clone)]
pub struct BarrageSession {
    inner: Arc<Inner>,
}

const AUTHORIZATION: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

fn header(
    name: &str,
    value: &str,
) -> Result<(MetadataKey<Ascii>, MetadataValue<Ascii>), ClientError> {
    let key = MetadataKey::<Ascii>::from_bytes(name.to_ascii_lowercase().as_bytes())
        .map_err(|_| ClientError::Config(format!("'{name}' is not a header name")))?;
    let value = MetadataValue::<Ascii>::try_from(value)
        .map_err(|_| ClientError::Config(format!("the value of header '{name}' is not ASCII")))?;
    Ok((key, value))
}

impl Inner {
    /// Remembers the bearer token of `metadata`, if it has one.
    ///
    /// Behavior: `BearerHandler#parseBearerToken` (the last `Bearer` value wins),
    /// `BearerHandler#setBearerToken`
    fn observe(&self, metadata: &MetadataMap) {
        let token = metadata
            .get_all(AUTHORIZATION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .filter_map(|value| value.strip_prefix(BEARER_PREFIX))
            .next_back()
            .map(str::to_string);
        let Some(token) = token else {
            return;
        };
        let mut bearer = lock(&self.bearer);
        let known = bearer.current.as_deref() == Some(token.as_str())
            || bearer.previous.as_deref() == Some(token.as_str());
        if !known {
            bearer.previous = bearer.current.take();
            bearer.current = Some(token);
        }
    }

    /// A status of a call, after its metadata has been looked at for a token.
    fn failed(&self, status: Status) -> ClientError {
        self.observe(status.metadata());
        ClientError::Status(status)
    }

    /// `message` as a request with the session's headers.
    ///
    /// Behavior: `BearerHandler#applyRequestMetadata`, and the extra headers of
    /// `io.deephaven.client.impl.ChannelHelper#channelBuilder`
    fn request<T>(&self, message: T) -> Result<Request<T>, ClientError> {
        if *self.closed.borrow() {
            return Err(ClientError::Closed);
        }
        let mut request = Request::new(message);
        self.decorate(request.metadata_mut(), None)?;
        Ok(request)
    }

    fn decorate(
        &self,
        metadata: &mut MetadataMap,
        authorization: Option<&str>,
    ) -> Result<(), ClientError> {
        for (key, value) in &self.extra_headers {
            metadata.insert(key.clone(), value.clone());
        }
        let authorization = match authorization {
            Some(first) => first.to_string(),
            None => match &lock(&self.bearer).current {
                Some(token) => format!("{BEARER_PREFIX}{token}"),
                None => return Ok(()),
            },
        };
        let value = MetadataValue::<Ascii>::try_from(authorization.as_str())
            .map_err(|_| ClientError::Config("the Authorization value is not ASCII".to_string()))?;
        metadata.insert(AUTHORIZATION, value);
        Ok(())
    }

    fn config(&self) -> ConfigServiceClient<Channel> {
        ConfigServiceClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_inbound_message_size)
    }

    fn sessions(&self) -> SessionServiceClient<Channel> {
        SessionServiceClient::new(self.channel.clone())
    }

    fn tables(&self) -> TableServiceClient<Channel> {
        TableServiceClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_inbound_message_size)
    }

    fn flight(&self) -> FlightServiceClient<Channel> {
        FlightServiceClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_inbound_message_size)
    }

    /// Asks the server for its configuration: the call that authenticates a new session
    /// and keeps an open one alive. Returns how long the server says a session lasts.
    ///
    /// Behavior: `io.deephaven.client.impl.Authentication#authenticate`,
    /// `SessionImpl#create`, `SessionImpl.SessionImpl#keepAlive`
    async fn configuration(
        &self,
        authorization: Option<&str>,
    ) -> Result<Option<Duration>, ClientError> {
        let mut request = Request::new(ConfigurationConstantsRequest {});
        self.decorate(request.metadata_mut(), authorization)?;
        let response = self
            .config()
            .get_configuration_constants(request)
            .await
            .map_err(|status| self.failed(status))?;
        self.observe(response.metadata());
        let duration = response
            .get_ref()
            .config_values
            .get("http.session.durationMs")
            .and_then(|value| match &value.kind {
                Some(Kind::StringValue(text)) => text.parse::<u64>().ok(),
                None => None,
            })
            .map(Duration::from_millis);
        Ok(duration)
    }

    /// Lets go of an export, without waiting for the server's answer.
    ///
    /// Behavior: `io.deephaven.client.impl.ExportStates#release` through
    /// `io.deephaven.client.impl.TableHandle#close`
    fn release_later(self: &Arc<Self>, export_id: i32) {
        let inner = self.clone();
        tokio::spawn(async move {
            let _ = inner.release(export_id).await;
        });
    }

    async fn release(&self, export_id: i32) -> Result<(), ClientError> {
        let request = self.request(ReleaseRequest {
            id: Some(Ticket {
                ticket: export_id_to_bytes(export_id),
            }),
        })?;
        let response = self
            .sessions()
            .release(request)
            .await
            .map_err(|status| self.failed(status))?;
        self.observe(response.metadata());
        Ok(())
    }
}

/// Pings the server until the session is closed or dropped.
async fn keep_alive(session: Weak<Inner>, frequency: Duration) {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + frequency, frequency);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let Some(inner) = session.upgrade() else {
            return;
        };
        if *inner.closed.borrow() {
            return;
        }
        // Java logs a failed ping and pings again
        let _ = inner.configuration(None).await;
    }
}

/// A Barrage request as the `app_metadata` of a Flight message.
fn wrapped(msg_type: BarrageMessageType, payload: &[u8]) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let msg_payload = Some(builder.create_vector(&signed(payload)));
    let wrapper = BarrageMessageWrapper::create(
        &mut builder,
        &BarrageMessageWrapperArgs {
            magic: MAGIC,
            msg_type,
            msg_payload,
        },
    );
    builder.finish(wrapper, None);
    FlightData {
        app_metadata: builder.finished_data().to_vec().into(),
        ..FlightData::default()
    }
}

fn signed(bytes: &[u8]) -> Vec<i8> {
    bytes.iter().map(|byte| *byte as i8).collect()
}

/// The request of a full subscription to the table behind `ticket`.
///
/// Behavior: `io.deephaven.client.impl.BarrageSubscriptionImpl#makeRequestInternal` with
/// no viewport and no columns
fn subscription_request(ticket: &[u8], options: &SubscriptionOptions) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let ticket = Some(builder.create_vector(&signed(ticket)));
    let subscription_options = Some(BarrageSubscriptionOptions::create(
        &mut builder,
        &BarrageSubscriptionOptionsArgs {
            use_deephaven_nulls: options.use_deephaven_nulls,
            min_update_interval_ms: options.min_update_interval_ms,
            batch_size: options.batch_size,
            max_message_size: options.max_message_size,
            columns_as_list: options.columns_as_list,
            ..Default::default()
        },
    ));
    let request = BarrageSubscriptionRequest::create(
        &mut builder,
        &BarrageSubscriptionRequestArgs {
            ticket,
            subscription_options,
            ..Default::default()
        },
    );
    builder.finish(request, None);
    wrapped(
        BarrageMessageType::BarrageSubscriptionRequest,
        builder.finished_data(),
    )
}

/// The static table of a snapshot: the rows of `rows`, flat only when the remote's schema
/// says the remote table is, as the subscription's table is. A table that holds its rows
/// at `0..size` is not therefore flat: Java's snapshot of a table that is not flat does not
/// say it is.
///
/// The other attributes of the schema (`BlinkTable` among them) are left off a snapshot:
/// `PARITY.md` A28 (owner item 7.25, answered: keep it so).
///
/// Behavior: `io.deephaven.extensions.barrage.table.BarrageTable#make` (`setFlat` of
/// `BarrageRedirectedTable`), which `BarrageSnapshotImpl` uses as `BarrageSubscriptionImpl`
/// does
pub(super) fn snapshot_table(schema: &RemoteSchema, rows: &dyn Table) -> Arc<dyn Table> {
    let columns = (0..schema.definition.columns().len())
        .map(|index| rows.column(index))
        .collect();
    let mut spec = TableSpec::new(
        schema.definition.clone(),
        (*rows.row_set()).clone(),
        columns,
    );
    if schema.is_flat() {
        spec = spec.flat();
    }
    QueryTable::new_static(spec)
}

/// The request of a snapshot of the table behind `ticket`.
///
/// Behavior: `io.deephaven.client.impl.BarrageSnapshotImpl#makeRequestInternal`
fn snapshot_request(
    ticket: &[u8],
    options: &SnapshotOptions,
    columns: Option<&ColumnSet>,
) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let ticket = Some(builder.create_vector(&signed(ticket)));
    let columns = columns.map(|columns| builder.create_vector(&signed(&columns.to_bytes())));
    let viewport = options
        .viewport
        .as_ref()
        .map(|rows| builder.create_vector(&signed(&encode_row_set(rows))));
    let snapshot_options = Some(BarrageSnapshotOptions::create(
        &mut builder,
        &BarrageSnapshotOptionsArgs {
            use_deephaven_nulls: options.use_deephaven_nulls,
            batch_size: options.batch_size,
            max_message_size: options.max_message_size,
            ..Default::default()
        },
    ));
    let request = BarrageSnapshotRequest::create(
        &mut builder,
        &BarrageSnapshotRequestArgs {
            ticket,
            columns,
            viewport,
            snapshot_options,
            reverse_viewport: options.reverse_viewport,
        },
    );
    builder.finish(request, None);
    wrapped(
        BarrageMessageType::BarrageSnapshotRequest,
        builder.finished_data(),
    )
}

fn failure(message: String) -> TableFailure {
    TableFailure { message }
}

/// What a table of this session fails with when its subscription ends with `status`.
///
/// Behavior: `io.deephaven.client.impl.BarrageSubscriptionImpl.DoExchangeObserver#onError`
fn subscription_error(name: &str, problem: &ClientError) -> TableFailure {
    failure(format!("Barrage subscription error for {name}: {problem}"))
}

/// `problem` and the errors that caused it, outermost first: tonic's own text of a failed
/// connection is only "transport error".
fn with_causes(problem: &dyn std::error::Error) -> String {
    let mut text = problem.to_string();
    let mut cause = problem.source();
    while let Some(next) = cause {
        let next_text = next.to_string();
        // tonic's wrappers repeat what they wrap
        if !text.ends_with(&next_text) {
            text.push_str(": ");
            text.push_str(&next_text);
        }
        cause = next.source();
    }
    text
}

/// A channel over TLS ([`super::tls`]).
#[cfg(feature = "tls")]
async fn secure_channel(
    config: &ClientConfig,
    endpoint: Endpoint,
) -> Result<Result<Channel, tonic::transport::Error>, ClientError> {
    super::tls::connect(config, endpoint).await
}

/// A build without the `tls` feature does not link OpenSSL, so it refuses TLS by name.
#[cfg(not(feature = "tls"))]
async fn secure_channel(
    _: &ClientConfig,
    _: Endpoint,
) -> Result<Result<Channel, tonic::transport::Error>, ClientError> {
    Err(ClientError::Config(
        "TLS connections need the `tls` feature of oxhaven-server (on with `python`), \
         which this build does not have"
            .to_string(),
    ))
}

/// The messages of one `DoExchange` call.
struct Exchange {
    /// Open for as long as the call should stay open: the server ends a subscription
    /// whose client stops sending.
    requests: mpsc::Sender<FlightData>,
    responses: Streaming<FlightData>,
    reader: MessageReader,
}

impl Exchange {
    /// The next snapshot or update of the stream, or `None` when the server ends it.
    async fn next(&mut self, session: &Inner) -> Result<Option<RemoteMessage>, ClientError> {
        loop {
            match self.responses.message().await {
                Ok(Some(data)) => {
                    if let Some(message) = self.reader.accept(&data)? {
                        return Ok(Some(message));
                    }
                }
                Ok(None) => {
                    if let Ok(Some(trailers)) = self.responses.trailers().await {
                        session.observe(&trailers);
                    }
                    if self.reader.is_mid_message() {
                        return Err(protocol("the stream ended in the middle of a message"));
                    }
                    return Ok(None);
                }
                Err(status) => return Err(session.failed(status)),
            }
        }
    }
}

impl BarrageSession {
    /// Connects to the server of `config` and authenticates.
    ///
    /// The tables the session subscribes are roots of `graph`. `wake`, if given, is called
    /// when something has arrived for one of them, so that a graph that runs on a timer
    /// can run a cycle sooner.
    ///
    /// Behavior: `io.deephaven.client.impl.ChannelHelper#channelBuilder`,
    /// `io.deephaven.client.impl.SessionImpl#create`,
    /// `io.deephaven.client.impl.BarrageSession#create`
    pub async fn connect(
        config: ClientConfig,
        graph: Arc<dyn UpdateGraph>,
        wake: Option<Wake>,
    ) -> Result<Self, ClientError> {
        if config.tls_root_certs.is_some() && !config.use_tls {
            // Behavior: `deephaven.barrage.barrage_session`
            return Err(ClientError::Config(
                "tls_root_certs is provided but use_tls is False".to_string(),
            ));
        }
        let extra_headers = config
            .extra_headers
            .iter()
            .map(|(name, value)| header(name, value))
            .collect::<Result<Vec<_>, _>>()?;
        let target = format!("{}:{}", config.host, config.port);
        let scheme = if config.use_tls { "https" } else { "http" };
        let endpoint = Endpoint::from_shared(format!("{scheme}://{target}"))
            .map_err(|problem| ClientError::Config(format!("'{target}': {problem}")))?;
        let connected = if config.use_tls {
            secure_channel(&config, endpoint).await?
        } else {
            endpoint.connect().await
        };
        let channel = connected.map_err(|problem| {
            ClientError::Connect(format!("{target}: {}", with_causes(&problem)))
        })?;
        let (closed, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            channel,
            target,
            max_inbound_message_size: config.max_inbound_message_size,
            extra_headers,
            bearer: Mutex::new(Bearer::default()),
            next_export: AtomicI32::new(1),
            closed,
            ping: Mutex::new(None),
            graph,
            wake,
        });
        let lasts = inner.configuration(Some(&config.authorization)).await?;
        if lock(&inner.bearer).current.is_none() {
            // Behavior: `Authentication#throwOnError` ("Authentication did not complete")
            return Err(protocol(
                "the server did not send a bearer token in answer to the authentication",
            ));
        }
        let frequency = lasts
            .map(|lasts| lasts / 3)
            .filter(|frequency| !frequency.is_zero())
            .unwrap_or(DEFAULT_PING_FREQUENCY);
        let ping = tokio::spawn(keep_alive(Arc::downgrade(&inner), frequency));
        *lock(&inner.ping) = Some(ping);
        Ok(BarrageSession { inner })
    }

    /// `host:port` of the server.
    pub fn target(&self) -> &str {
        &self.inner.target
    }

    pub fn is_closed(&self) -> bool {
        *self.inner.closed.borrow()
    }

    /// The `Authorization` value the session sends now.
    #[cfg(test)]
    pub(crate) fn bearer_for_tests(&self) -> String {
        format!(
            "{BEARER_PREFIX}{}",
            lock(&self.inner.bearer).current.clone().unwrap_or_default()
        )
    }

    /// Exports the table behind `ticket` to this session and returns what the server says
    /// of it. The caller releases the export.
    ///
    /// Behavior: `io.deephaven.client.impl.TableHandle#of` of
    /// `io.deephaven.qst.table.TicketTable`, a `FetchTableRequest`
    pub async fn fetch(&self, ticket: &[u8]) -> Result<RemoteTable, ClientError> {
        let inner = &self.inner;
        let export_id = inner.next_export.fetch_add(1, Ordering::Relaxed);
        let request = inner.request(FetchTableRequest {
            source_id: Some(TableReference {
                r#ref: Some(Ref::Ticket(Ticket {
                    ticket: ticket.to_vec(),
                })),
            }),
            result_id: Some(Ticket {
                ticket: export_id_to_bytes(export_id),
            }),
        })?;
        let response = match inner.tables().fetch_table(request).await {
            Ok(response) => response,
            Err(status) => {
                // the export exists on the server, failed
                inner.release_later(export_id);
                return Err(inner.failed(status));
            }
        };
        inner.observe(response.metadata());
        let response = response.into_inner();
        let described = (|| {
            if !response.success {
                return Err(ClientError::Status(Status::unknown(
                    response.error_info.clone(),
                )));
            }
            let arrow = schema_from_ipc_bytes(&response.schema_header).map_err(|status| {
                protocol(format!(
                    "unreadable schema of the table: {}",
                    status.message()
                ))
            })?;
            RemoteSchema::of(arrow)
        })();
        match described {
            Ok(schema) => Ok(RemoteTable {
                export_id,
                schema,
                is_static: response.is_static,
                size: response.size,
            }),
            Err(problem) => {
                inner.release_later(export_id);
                Err(problem)
            }
        }
    }

    /// Releases an export of this session on the server.
    ///
    /// Behavior: `io.deephaven.client.impl.Session#release`
    pub async fn release(&self, export_id: i32) -> Result<(), ClientError> {
        self.inner.release(export_id).await
    }

    async fn exchange(
        &self,
        table: &RemoteTable,
        request: FlightData,
        options: ReadOptions,
    ) -> Result<Exchange, ClientError> {
        let inner = &self.inner;
        let reader = MessageReader::new(
            table.schema.definition.clone(),
            &table.schema.arrow,
            options,
        )?;
        let (requests, receiver) = mpsc::channel(4);
        requests
            .send(request)
            .await
            .map_err(|_| ClientError::Closed)?;
        let call = inner.request(ReceiverStream::new(receiver))?;
        let response = inner
            .flight()
            .do_exchange(call)
            .await
            .map_err(|status| inner.failed(status))?;
        inner.observe(response.metadata());
        Ok(Exchange {
            requests,
            responses: response.into_inner(),
            reader,
        })
    }

    /// The table behind `ticket` as it is now: a static table with the server's rows in
    /// the server's order.
    ///
    /// Behavior: `io.deephaven.client.impl.BarrageSession#snapshot`, then
    /// `io.deephaven.client.impl.BarrageSnapshot#entireTable` or `#partialTable`
    pub async fn snapshot(
        &self,
        ticket: &[u8],
        options: SnapshotOptions,
    ) -> Result<Arc<dyn Table>, ClientError> {
        let table = self.fetch(ticket).await?;
        let result = self.snapshot_of(&table, &options).await;
        self.inner.release_later(table.export_id);
        result
    }

    /// As [`BarrageSession::snapshot`], of a table already fetched. The export is left
    /// to the caller.
    pub async fn snapshot_of(
        &self,
        table: &RemoteTable,
        options: &SnapshotOptions,
    ) -> Result<Arc<dyn Table>, ClientError> {
        let definition = &table.schema.definition;
        let columns = match &options.columns {
            None => None,
            Some(names) => {
                let mut set = ColumnSet::of([]);
                for name in names {
                    let index = definition.index_of(name).ok_or_else(|| {
                        ClientError::Config(format!("the table has no column '{name}'"))
                    })?;
                    set.insert(index);
                }
                Some(set)
            }
        };
        let request = snapshot_request(
            &export_id_to_bytes(table.export_id),
            options,
            columns.as_ref(),
        );
        let mut exchange = self
            .exchange(table, request, ReadOptions::default())
            .await?;
        // Behavior: `BarrageSnapshotImpl#partialTable` half-closes after the request
        let (closed, _) = mpsc::channel(1);
        drop(std::mem::replace(&mut exchange.requests, closed));

        let fields: Vec<Field> = definition
            .columns()
            .iter()
            .zip(exchange.reader.stored_types())
            .map(|(column, data_type)| Field::new(column.name.clone(), data_type, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let mut batches = Vec::new();
        while let Some(message) = exchange.next(&self.inner).await? {
            // Behavior: `BarrageTable#handleBarrageMessage` of a snapshot table takes the
            // included rows only; a server sends nothing else in answer to a snapshot
            if !message.rows_removed.is_empty()
                || !message.shifted.is_empty()
                || message
                    .mod_columns
                    .iter()
                    .any(|column| !column.rows_modified.is_empty())
            {
                return Err(protocol("a snapshot removes, shifts or modifies rows"));
            }
            let rows = message.rows_included.size() as usize;
            if rows == 0 {
                continue;
            }
            let cells: Vec<ArrayRef> = message
                .add_columns
                .iter()
                .zip(schema.fields())
                .map(|(cells, field)| {
                    if cells.len() == rows {
                        cells.clone()
                    } else {
                        // a column that was not asked for
                        new_null_array(field.data_type(), rows)
                    }
                })
                .collect();
            let batch = RecordBatch::try_new_with_options(
                schema.clone(),
                cells,
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .map_err(protocol)?;
            batches.push(batch);
        }
        let rows = uploaded_table(definition.clone(), &batches)?;
        Ok(snapshot_table(&table.schema, rows.as_ref()))
    }

    /// The table behind `ticket`, kept up to date: a refreshing table of the session's
    /// update graph with the server's rows at the server's row keys.
    ///
    /// Returns once the server has sent the whole table, as Java's `entireTable` future
    /// completes. The table fails when the subscription ends: when the server ends it,
    /// when the connection is lost and when the session is closed. Dropping the table
    /// ends the subscription and releases the export.
    ///
    /// Behavior: `io.deephaven.client.impl.BarrageSession#subscribe`, then
    /// `io.deephaven.client.impl.BarrageSubscription#entireTable`
    pub async fn subscribe(
        &self,
        ticket: &[u8],
        options: SubscriptionOptions,
    ) -> Result<Arc<dyn Table>, ClientError> {
        let table = self.fetch(ticket).await?;
        let export_id = table.export_id;
        let result = self.subscribe_to(table, options).await;
        if result.is_err() {
            self.inner.release_later(export_id);
        }
        result
    }

    /// Subscribes to a fetched table. On success the subscription owns the export.
    async fn subscribe_to(
        &self,
        table: RemoteTable,
        options: SubscriptionOptions,
    ) -> Result<Arc<dyn Table>, ClientError> {
        let inner = self.inner.clone();
        let mut closed = inner.closed.subscribe();
        let request = subscription_request(&export_id_to_bytes(table.export_id), &options);
        let mut exchange = self
            .exchange(
                &table,
                request,
                ReadOptions {
                    columns_as_list: options.columns_as_list,
                },
            )
            .await?;

        // Until the server says it sends the whole table, the rows it sends are only
        // collected: the table is not shown before it is complete.
        //
        // Behavior: `BarrageSubscriptionImpl.CheckForCompletion`, and the `incomplete`
        // table of `BarrageRedirectedTable#applyUpdates`
        let mut replica = Replica::new(&table.schema);
        let blink = table.schema.is_blink();
        let mut early = Vec::new();
        loop {
            let message = tokio::select! {
                _ = closed.wait_for(|closed| *closed) => return Err(ClientError::Closed),
                message = exchange.next(&inner) => message?,
            };
            let Some(message) = message else {
                // Behavior: `DoExchangeObserver#onCompleted`
                return Err(protocol("Barrage subscription closed by server"));
            };
            let acknowledged = message.is_whole_table(table.schema.definition.columns().len());
            if blink {
                // the rows of a blink table that arrive before it is shown are its
                // first rows
                early.push(message);
            } else {
                replica.apply(std::slice::from_ref(&message))?;
            }
            if acknowledged {
                break;
            }
        }
        if blink {
            replica.apply(&early)?;
        }
        drop(early);

        let inbox = Inbox::new(inner.wake.clone());
        let live = {
            let graph = inner.graph.clone();
            let schema = table.schema.clone();
            let inbox = inbox.clone();
            tokio::task::spawn_blocking(move || {
                locked(graph.as_ref(), || {
                    live_table(graph.clone(), &schema, replica, inbox)
                })
            })
            .await
            .map_err(|problem| protocol(format!("the table could not be made: {problem}")))?
        };

        let name = format!("{}/export {}", inner.target, table.export_id);
        let export_id = table.export_id;
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    _ = inbox.released() => break,
                    _ = closed.wait_for(|closed| *closed) => {
                        inbox.fail(subscription_error(&name, &ClientError::Closed));
                        break;
                    }
                    next = exchange.next(&inner) => next,
                };
                match next {
                    Ok(Some(message)) => {
                        if !inbox.deliver(message) {
                            break;
                        }
                    }
                    Ok(None) => {
                        // Behavior: `DoExchangeObserver#onCompleted`
                        inbox.fail(failure("Barrage subscription closed by server".to_string()));
                        break;
                    }
                    Err(problem) => {
                        inbox.fail(subscription_error(&name, &problem));
                        break;
                    }
                }
            }
            // ends the call, then lets go of the table on the server
            drop(exchange);
            if !*inner.closed.borrow() {
                let _ = inner.release(export_id).await;
            }
        });
        Ok(live)
    }

    /// Closes the session on the server, which releases its exports, and fails the tables
    /// it subscribed. Closing a closed session does nothing.
    ///
    /// Behavior: `io.deephaven.client.impl.SessionImpl#closeFuture`, and the
    /// `shutdownNow` of the channel in `deephaven.barrage.BarrageSession.close`
    pub async fn close(&self) -> Result<(), ClientError> {
        let inner = &self.inner;
        if *inner.closed.borrow() {
            return Ok(());
        }
        if let Some(ping) = lock(&inner.ping).take() {
            ping.abort();
        }
        let request = inner.request(HandshakeRequest::default());
        inner.closed.send_replace(true);
        let response = inner
            .sessions()
            .close_session(request?)
            .await
            .map_err(ClientError::Status)?;
        drop(response);
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(ping) = lock(&self.ping).take() {
            ping.abort();
        }
    }
}
