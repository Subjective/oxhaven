//! Tests of the Flight service against what the Java reference server answered
//! (`fixtures/`, written by `fixtures/capture.py`).

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use oxhaven_column::FlatSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table};
use oxhaven_engine::table::{in_memory_table, QueryTable, TableSpec};
use oxhaven_proto::flight::flight_descriptor::DescriptorType;
use oxhaven_proto::flight::{Criteria, FlightData, FlightDescriptor, FlightInfo, SchemaResult};
use prost::Message;
use tonic::{Code, Status};

use super::*;
use crate::session::{export_id_to_bytes, ExportValue};

pub(crate) mod support {
    //! A server on a local port, the fixtures, and what the tests compare.

    // RefreshSessionToken is deprecated in the protocol and still the plainest way to log in
    #![allow(deprecated)]

    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::datatypes::Schema;
    use oxhaven_contracts::table::Table;
    use oxhaven_proto::flight::flight_service_client::FlightServiceClient;
    use oxhaven_proto::flight::{FlightData, FlightDescriptor, PutResult, Ticket};
    use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
    use oxhaven_proto::grpc::HandshakeRequest;
    use prost::Message;
    use tokio::sync::oneshot;
    use tonic::transport::Channel;
    use tonic::{Code, Request, Status};

    use crate::session::{
        export_id_to_bytes, AuthHandler, AuthHandlers, ExportValue, Session,
        DEFAULT_TOKEN_EXPIRE_MS, PSK_AUTH_TYPE,
    };
    use crate::{serve, ServerConfig, ServerState};

    const PSK: &str = "s3cret";

    /// How long a call is waited for before it counts as never answered. The Java captures
    /// waited 15 seconds; nothing here takes longer than a few milliseconds to answer.
    const NEVER: Duration = Duration::from_millis(400);

    pub(crate) struct Server {
        pub(crate) state: ServerState,
        pub(crate) channel: Channel,
        /// The `authorization` header of the session every call of the test uses.
        pub(crate) bearer: String,
        _shutdown: oneshot::Sender<()>,
    }

    impl Server {
        /// Starts a server and logs in with the pre-shared key.
        pub(crate) async fn start() -> Self {
            let state = ServerState::new(ServerConfig {
                auth_handlers: AuthHandlers::new(vec![AuthHandler::psk(PSK).unwrap()]).unwrap(),
                token_expire_ms: DEFAULT_TOKEN_EXPIRE_MS,
                target_cycle_duration: None,
            })
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, stopped) = oneshot::channel::<()>();
            tokio::spawn({
                let state = state.clone();
                async move {
                    serve(state, listener, async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
                }
            });
            let channel = Channel::from_shared(format!("http://{address}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            let mut login = Request::new(HandshakeRequest::default());
            login.metadata_mut().insert(
                "authorization",
                format!("{PSK_AUTH_TYPE} {PSK}").parse().unwrap(),
            );
            let response = SessionServiceClient::new(channel.clone())
                .refresh_session_token(login)
                .await
                .unwrap();
            let bearer = response.metadata().get("authorization").unwrap();
            Self {
                state,
                channel,
                bearer: bearer.to_str().unwrap().to_owned(),
                _shutdown: shutdown,
            }
        }

        pub(crate) fn session(&self) -> Arc<Session> {
            self.state
                .sessions
                .session_for_auth_header(&self.bearer)
                .expect("a live session for the bearer token")
        }

        pub(crate) fn authorized<T>(&self, message: T) -> Request<T> {
            let mut request = Request::new(message);
            request
                .metadata_mut()
                .insert("authorization", self.bearer.parse().unwrap());
            request
        }

        pub(crate) fn flight(&self) -> FlightServiceClient<Channel> {
            FlightServiceClient::new(self.channel.clone()).max_decoding_message_size(usize::MAX)
        }

        /// Makes `table` export `id` of the session.
        pub(crate) fn export(&self, id: i32, table: Arc<dyn Table>) {
            self.session()
                .new_export(id)
                .unwrap()
                .submit(move |_| Ok(ExportValue::Table(table)))
                .unwrap();
        }

        /// The schema `GetSchema` answers for export `id`.
        pub(crate) async fn get_schema(&self, id: i32) -> Result<Schema, Status> {
            let answer = self
                .flight()
                .get_schema(self.authorized(export_descriptor(id)))
                .await?;
            Ok(schema_of_ipc(&answer.into_inner().schema))
        }

        /// Every message `DoGet` answers for a ticket.
        pub(crate) async fn do_get_ticket(
            &self,
            ticket: Vec<u8>,
        ) -> Result<Vec<FlightData>, Status> {
            let ticket = Ticket {
                ticket: ticket.into(),
            };
            let mut stream = self
                .flight()
                .do_get(self.authorized(ticket))
                .await?
                .into_inner();
            let mut messages = Vec::new();
            while let Some(message) = stream.message().await? {
                messages.push(message);
            }
            Ok(messages)
        }

        pub(crate) async fn do_get(&self, id: i32) -> Result<Vec<FlightData>, Status> {
            self.do_get_ticket(export_id_to_bytes(id)).await
        }

        /// Sends `messages` as one `DoPut` and returns what the server answered.
        pub(crate) async fn do_put(
            &self,
            messages: Vec<FlightData>,
        ) -> Result<Vec<PutResult>, Status> {
            let mut stream = self
                .flight()
                .do_put(self.authorized(tokio_stream::iter(messages)))
                .await?
                .into_inner();
            let mut results = Vec::new();
            while let Some(result) = stream.message().await? {
                results.push(result);
            }
            Ok(results)
        }
    }

    pub(crate) fn export_descriptor(id: i32) -> FlightDescriptor {
        path_descriptor(&["export", &id.to_string()])
    }

    pub(crate) fn path_descriptor(path: &[&str]) -> FlightDescriptor {
        FlightDescriptor {
            r#type: oxhaven_proto::flight::flight_descriptor::DescriptorType::Path as i32,
            path: path.iter().map(|part| (*part).to_owned()).collect(),
            ..Default::default()
        }
    }

    /// The name of the status code a call ends with, as `capture.py` writes it. A call that
    /// is not answered in time is `DEADLINE_EXCEEDED`, which is what the capture saw of the
    /// calls Java never answers.
    pub(crate) async fn code_of<T>(call: impl Future<Output = Result<T, Status>>) -> &'static str {
        let code = match tokio::time::timeout(NEVER, call).await {
            Err(_) => return "DEADLINE_EXCEEDED",
            Ok(Ok(_)) => return "OK",
            Ok(Err(status)) => status.code(),
        };
        match code {
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::FailedPrecondition => "FAILED_PRECONDITION",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::Unauthenticated => "UNAUTHENTICATED",
            Code::NotFound => "NOT_FOUND",
            Code::DataLoss => "DATA_LOSS",
            other => panic!("a status code no fixture has: {other:?}"),
        }
    }

    pub(crate) fn fixture(name: &str) -> Vec<u8> {
        let path = format!("{}/src/flight/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        std::fs::read(&path).unwrap_or_else(|problem| panic!("{path}: {problem}"))
    }

    /// The messages of a `.put` or `.doget` fixture.
    pub(crate) fn fixture_messages(name: &str) -> Vec<FlightData> {
        let bytes = fixture(name);
        let mut rest = bytes.as_slice();
        let mut messages = Vec::new();
        while !rest.is_empty() {
            let size = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            messages.push(FlightData::decode(&rest[4..4 + size]).unwrap());
            rest = &rest[4 + size..];
        }
        messages
    }

    /// The lines of a `.status` fixture.
    pub(crate) fn fixture_lines(name: &str) -> Vec<String> {
        String::from_utf8(fixture(name))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// The schema of an encapsulated IPC schema message.
    pub(crate) fn schema_of_ipc(bytes: &[u8]) -> Schema {
        super::schema_from_ipc_bytes(bytes).unwrap()
    }

    /// The schema Java answered `GetSchema` with in a case.
    pub(crate) fn fixture_schema(case: &str) -> Schema {
        let result = oxhaven_proto::flight::SchemaResult::decode(
            fixture(&format!("{case}.schema")).as_slice(),
        )
        .unwrap();
        schema_of_ipc(&result.schema)
    }

    /// The schema in the first message of a `DoGet`.
    pub(crate) fn schema_of_message(message: &FlightData) -> Schema {
        let parsed = arrow::ipc::root_as_message(&message.data_header).unwrap();
        let schema = parsed.header_as_schema().expect("a schema message");
        assert!(message.data_body.is_empty());
        arrow::ipc::convert::fb_to_schema(schema)
    }

    /// What a record batch message says and carries, without the layout of its flatbuffer.
    #[derive(PartialEq, Eq, Debug)]
    pub(crate) struct Batch {
        pub(crate) rows: i64,
        /// Per field: length and null count.
        pub(crate) nodes: Vec<(i64, i64)>,
        /// Per buffer: offset and length in the body.
        pub(crate) buffers: Vec<(i64, i64)>,
        pub(crate) body: Vec<u8>,
    }

    pub(crate) fn batch_of(message: &FlightData) -> Batch {
        let parsed = arrow::ipc::root_as_message(&message.data_header).unwrap();
        let batch = parsed
            .header_as_record_batch()
            .expect("a record batch message");
        assert_eq!(parsed.version(), arrow::ipc::MetadataVersion::V5);
        assert_eq!(parsed.bodyLength(), message.data_body.len() as i64);
        Batch {
            rows: batch.length(),
            nodes: batch
                .nodes()
                .map(|nodes| nodes.iter().map(|n| (n.length(), n.null_count())).collect())
                .unwrap_or_default(),
            buffers: batch
                .buffers()
                .map(|buffers| buffers.iter().map(|b| (b.offset(), b.length())).collect())
                .unwrap_or_default(),
            body: message.data_body.to_vec(),
        }
    }

    /// Checks the messages of a `DoGet` against the ones Java sent in a case: the same
    /// schema, then the same record batches, byte for byte in the body.
    pub(crate) fn assert_do_get_matches(case: &str, ours: &[FlightData], expected_schema: &Schema) {
        let java = fixture_messages(&format!("{case}.doget"));
        assert_eq!(
            schema_of_message(&ours[0]),
            *expected_schema,
            "{case}: schema"
        );
        let ours: Vec<Batch> = ours[1..].iter().map(batch_of).collect();
        let java: Vec<Batch> = java[1..].iter().map(batch_of).map(as_sent_here).collect();
        assert_eq!(ours, java, "{case}: record batches");
    }

    /// Java's batch as this server sends it: where Java's body is empty, eight zero bytes
    /// (see `BatchWriter::finish`). Java writes the body field with length zero, which
    /// prost cannot write; everything else is compared as Java sent it.
    fn as_sent_here(mut batch: Batch) -> Batch {
        if batch.body.is_empty() && batch.buffers.iter().all(|&(_, length)| length == 0) {
            batch.body = vec![0; 8];
        }
        batch
    }
}

use support::*;

fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    TableDefinition::new(
        columns
            .iter()
            .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
            .collect(),
    )
    .unwrap()
}

fn every_type() -> TableDefinition {
    definition(&[
        ("Bo", ColumnType::Boolean),
        ("By", ColumnType::Byte),
        ("Ch", ColumnType::Char),
        ("Sh", ColumnType::Short),
        ("In", ColumnType::Int),
        ("Lo", ColumnType::Long),
        ("Fl", ColumnType::Float),
        ("Do", ColumnType::Double),
        ("St", ColumnType::String),
        ("Ti", ColumnType::Instant),
    ])
}

fn batch_for(definition: &TableDefinition, columns: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// The table `capture.py` makes with its formulas for the rows in `rows`; row `null_row`
/// is null in every column.
fn engine_table(rows: std::ops::Range<i64>, null_row: Option<i64>) -> Arc<dyn Table> {
    fn cells<T>(
        rows: &std::ops::Range<i64>,
        null_row: Option<i64>,
        value: impl Fn(i64) -> T,
    ) -> Vec<Option<T>> {
        rows.clone()
            .map(|i| (Some(i) != null_row).then(|| value(i)))
            .collect()
    }
    let r = &rows;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(cells(r, null_row, |i| i == 0))),
        Arc::new(Int8Array::from(cells(r, null_row, |i| (i + 1) as i8))),
        Arc::new(UInt16Array::from(cells(r, null_row, |i| {
            b'a' as u16 + i as u16
        }))),
        Arc::new(Int16Array::from(cells(r, null_row, |i| (i + 1) as i16))),
        Arc::new(Int32Array::from(cells(r, null_row, |i| (i + 1) as i32))),
        Arc::new(Int64Array::from(cells(r, null_row, |i| i + 1))),
        Arc::new(Float32Array::from(cells(r, null_row, |i| i as f32 + 1.5))),
        Arc::new(Float64Array::from(cells(r, null_row, |i| i as f64 + 1.5))),
        Arc::new(StringArray::from(cells(r, null_row, |i| format!("s{i}")))),
        Arc::new(
            TimestampNanosecondArray::from(cells(r, null_row, |i| 1_700_000_000_000_000_001 + i))
                .with_timezone("UTC"),
        ),
    ];
    let definition = every_type();
    let batch = batch_for(&definition, columns);
    in_memory_table(definition, &[batch]).unwrap()
}

fn fixture_info(case: &str) -> FlightInfo {
    FlightInfo::decode(fixture(&format!("{case}.info")).as_slice()).unwrap()
}

/// Checks what `GetFlightInfo` answers against Java's answer in a case, with the schema
/// compared as a schema and everything else exactly.
fn assert_info_matches(case: &str, ours: &FlightInfo, expected_schema: &Schema) {
    let java = fixture_info(case);
    assert_eq!(schema_of_ipc(&ours.schema), *expected_schema, "{case}");
    let without_schema = |info: &FlightInfo| FlightInfo {
        schema: Default::default(),
        ..info.clone()
    };
    assert_eq!(without_schema(ours), without_schema(&java), "{case}");
}

/// The export a case's fixtures were captured from.
fn export_of(info: &FlightInfo) -> i32 {
    let descriptor = info.flight_descriptor.as_ref().unwrap();
    descriptor.path[1].parse().unwrap()
}

#[test]
fn engine_tables_of_every_column_type_match_java() {
    for (case, table) in [
        ("engine_nulls", engine_table(0..3, Some(1))),
        ("engine_no_nulls", engine_table(0..3, None)),
        ("engine_empty", engine_table(0..0, None)),
    ] {
        let java_schema = fixture_schema(case);
        assert_eq!(wire_schema(&table), java_schema, "{case}");
        // Java answers GetFlightInfo with the same schema bytes as GetSchema
        let java_info = fixture_info(case);
        assert_eq!(
            java_info.schema,
            SchemaResult::decode(fixture(&format!("{case}.schema")).as_slice())
                .unwrap()
                .schema
        );
        let id = export_of(&java_info);
        let info = flight_info(&table, export_descriptor(id), export_id_to_bytes(id));
        assert_info_matches(case, &info, &java_schema);
        let messages = do_get_messages(&table).unwrap();
        assert_do_get_matches(case, &messages, &java_schema);
    }
    // a static table without rows is described and nothing more
    assert_eq!(do_get_messages(&engine_table(0..0, None)).unwrap().len(), 1);
}

#[test]
fn arrow_type_of_every_column_type_is_the_one_java_sends() {
    let java = fixture_schema("engine_nulls");
    let definition = every_type();
    assert_eq!(java.fields().len(), definition.columns().len());
    for (field, column) in java.fields().iter().zip(definition.columns()) {
        assert_eq!(field.name(), &column.name);
        assert_eq!(*field.data_type(), column.column_type.arrow_type());
    }
    assert_eq!(
        *java.field_with_name("Ti").unwrap().data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
}

/// A table whose row keys are not its positions, as a filter leaves it.
#[test]
fn table_that_is_not_flat_is_sent_in_position_order() {
    let definition = definition(&[("In", ColumnType::Int), ("St", ColumnType::String)]);
    let ints: ArrayRef = Arc::new(Int32Array::from((0..6).collect::<Vec<i32>>()));
    let strings: ArrayRef = Arc::new(StringArray::from(
        (0..6).map(|i| format!("s{i}")).collect::<Vec<_>>(),
    ));
    let columns: Vec<Arc<dyn ColumnSource>> = vec![
        Arc::new(FlatSource::new(ColumnType::Int, ints).unwrap()),
        Arc::new(FlatSource::new(ColumnType::String, strings).unwrap()),
    ];
    let table: Arc<dyn Table> = QueryTable::new_static(TableSpec::new(
        definition,
        RowSet::from_keys([1, 3, 5]),
        columns,
    ));
    let java_schema = fixture_schema("engine_filtered");
    let ours = wire_schema(&table);
    // The schema metadata is not compared: Java marks the result of a static `where` as
    // add-only and append-only, which is the filter operation's to decide, not this table's.
    assert_eq!(ours.fields(), java_schema.fields());
    let messages = do_get_messages(&table).unwrap();
    assert_do_get_matches("engine_filtered", &messages, &ours);
    let info = flight_info(&table, export_descriptor(7), export_id_to_bytes(7));
    assert_eq!(info.total_records, 3);
    assert_eq!(fixture_info("engine_filtered").total_records, 3);
}

/// The int values of record batches of one `int` column without nulls.
fn ints_of(messages: &[FlightData]) -> Vec<Vec<i32>> {
    messages
        .iter()
        .map(|message| {
            let batch = batch_of(message);
            assert_eq!(batch.nodes, vec![(batch.rows, 0)]);
            assert_eq!(batch.buffers[0].1, 0, "no validity buffer without nulls");
            let (offset, length) = batch.buffers[1];
            batch.body[offset as usize..(offset + length) as usize]
                .chunks(4)
                .map(|cell| i32::from_le_bytes(cell.try_into().unwrap()))
                .take(batch.rows as usize)
                .collect()
        })
        .collect()
}

#[test]
fn many_rows_are_sent_in_several_batches_with_the_first_of_4096() {
    let definition = definition(&[("In", ColumnType::Int)]);
    let ints: ArrayRef = Arc::new(Int32Array::from((0..10_000).collect::<Vec<i32>>()));
    let batch = batch_for(&definition, vec![ints]);
    let table: Arc<dyn Table> = in_memory_table(definition, &[batch]).unwrap();
    let java_schema = fixture_schema("engine_many_rows");
    assert_eq!(wire_schema(&table), java_schema);

    let java = ints_of(&fixture_messages("engine_many_rows.doget")[1..]);
    let ours = ints_of(&do_get_messages(&table).unwrap()[1..]);
    // Java cuts the snapshot of a static table into pieces of 4096 rows before it sizes
    // batches; batch boundaries after the first batch are not compared (PARITY.md A6).
    assert_eq!(
        java.iter().map(Vec::len).collect::<Vec<_>>(),
        [4096, 4096, 1808]
    );
    assert_eq!(ours[0], java[0]);
    assert!(ours.len() > 1);
    assert_eq!(ours.concat(), java.concat());
    assert_eq!(ours.concat(), (0..10_000).collect::<Vec<i32>>());
}

#[test]
fn batches_follow_the_client_options_and_the_message_size_limit() {
    let ints: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
    let sizes = |options: BatchOptions| -> Vec<usize> {
        let messages = record_batch_messages(
            std::slice::from_ref(&ints),
            &[true],
            10,
            options,
            b"meta".to_vec(),
            false,
        )
        .unwrap();
        // the metadata goes with the first message only
        assert_eq!(&messages[0].app_metadata[..], b"meta");
        assert!(messages[1..].iter().all(|m| m.app_metadata.is_empty()));
        ints_of(&messages).iter().map(Vec::len).collect()
    };
    assert_eq!(sizes(BatchOptions::default()), [10]);
    let by_rows = BatchOptions {
        batch_size: 4,
        max_message_size: 0,
        ..Default::default()
    };
    assert_eq!(sizes(by_rows), [4, 4, 2]);
    // One row is a header of well over 100 bytes and 8 bytes of body: a message limit
    // smaller than that sends one row at a time, however large the message comes out.
    let by_bytes = BatchOptions {
        batch_size: 0,
        max_message_size: 100,
        ..Default::default()
    };
    assert_eq!(sizes(by_bytes), [1; 10]);

    let no_rows = |send_empty| {
        record_batch_messages(
            &[ints.slice(0, 0)],
            &[true],
            0,
            BatchOptions::default(),
            b"meta".to_vec(),
            send_empty,
        )
        .unwrap()
    };
    assert!(no_rows(false).is_empty());
    let empty = no_rows(true);
    assert_eq!(empty.len(), 1);
    assert_eq!(batch_of(&empty[0]).rows, 0);
    assert_eq!(&empty[0].app_metadata[..], b"meta");
}

/// Deephaven's reader takes a record batch message only with its body field
/// (`BarrageMessageReaderImpl#parseFrom`, "Missing body tag"), and protobuf leaves an empty
/// `data_body` out: a batch without bytes is sent with an aligned body that no buffer reads.
#[test]
fn a_batch_without_bytes_still_carries_a_body() {
    // field 1000, length-delimited: (1000 << 3) | 2 as a varint
    const BODY_TAG: [u8; 2] = [0xc2, 0x3e];
    let has_body_field = |message: &FlightData| {
        message
            .encode_to_vec()
            .windows(BODY_TAG.len())
            .any(|w| w == BODY_TAG)
    };
    let ints: ArrayRef = Arc::new(Int32Array::from((0..10).collect::<Vec<i32>>()));
    let cases: [(Vec<ArrayRef>, usize); 3] = [
        // a table, or an update, without rows
        (vec![ints.slice(0, 0)], 0),
        // a table without columns
        (vec![], 5),
        (vec![], 0),
    ];
    for (columns, rows) in cases {
        let nullable = vec![true; columns.len()];
        let messages = record_batch_messages(
            &columns,
            &nullable,
            rows,
            BatchOptions::default(),
            Vec::new(),
            true,
        )
        .unwrap();
        let batch = batch_of(&messages[0]);
        assert_eq!(batch.rows, rows as i64);
        assert_eq!(batch.body, [0; 8]);
        assert!(batch.buffers.iter().all(|&(_, length)| length == 0));
        assert!(has_body_field(&messages[0]));
    }
    let some = record_batch_messages(
        std::slice::from_ref(&ints),
        &[true],
        10,
        BatchOptions::default(),
        Vec::new(),
        false,
    )
    .unwrap();
    assert!(has_body_field(&some[0]));
    assert_eq!(batch_of(&some[0]).body.len() % 8, 0);
}

#[test]
fn bitmaps_are_whole_words_and_sentinels_lie_under_nulls() {
    // 70 rows need two 64-bit words of validity
    let ints: ArrayRef = Arc::new(Int32Array::from(
        (0..70).map(|i| (i != 65).then_some(i)).collect::<Vec<_>>(),
    ));
    let bools: ArrayRef = Arc::new(BooleanArray::from(
        (0..70)
            .map(|i| (i != 3).then_some(i % 2 == 0))
            .collect::<Vec<_>>(),
    ));
    let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("ab"), None, Some("c")]));

    let batch = encode_batch(&[ints.clone(), bools], &[true, true], 0, 70).unwrap();
    let message = FlightData {
        data_header: batch.header.into(),
        data_body: batch.body.into(),
        ..Default::default()
    };
    let batch = batch_of(&message);
    assert_eq!(batch.nodes, [(70, 1), (70, 1)]);
    assert_eq!(batch.buffers, [(0, 16), (16, 280), (296, 16), (312, 16)]);
    let validity = u128::from_le_bytes(batch.body[..16].try_into().unwrap());
    assert_eq!(validity, ((1u128 << 70) - 1) & !(1 << 65));
    let cell = |row: usize| i32::from_le_bytes(batch.body[16 + 4 * row..][..4].try_into().unwrap());
    assert_eq!((cell(64), cell(65), cell(66)), (64, i32::MIN, 66));
    let values = u128::from_le_bytes(batch.body[312..328].try_into().unwrap());
    assert_eq!(
        values & 0b1111_1111,
        0b0101_0101,
        "the null boolean is written false"
    );

    // a field the schema calls non-nullable has no validity and counts no nulls
    let batch = encode_batch(&[ints], &[false], 0, 70).unwrap();
    let message = FlightData {
        data_header: batch.header.into(),
        data_body: batch.body.into(),
        ..Default::default()
    };
    let batch = batch_of(&message);
    assert_eq!(batch.nodes, [(70, 0)]);
    assert_eq!(batch.buffers, [(0, 0), (0, 280)]);

    // a slice of strings: offsets start at 0, a null string has no bytes
    let batch = encode_batch(&[strings], &[true], 1, 2).unwrap();
    let message = FlightData {
        data_header: batch.header.into(),
        data_body: batch.body.into(),
        ..Default::default()
    };
    let batch = batch_of(&message);
    assert_eq!(batch.nodes, [(2, 1)]);
    assert_eq!(batch.buffers, [(0, 8), (8, 16), (24, 8)]);
    assert_eq!(batch.body[0], 0b10);
    let offsets: Vec<i32> = batch.body[8..20]
        .chunks(4)
        .map(|cell| i32::from_le_bytes(cell.try_into().unwrap()))
        .collect();
    assert_eq!(offsets, [0, 0, 1]);
    assert_eq!(batch.body[24], b'c');
}

const PUT_CASES: [&str; 16] = [
    "put_plain",
    "put_typed",
    "put_no_nulls",
    "put_three_batches",
    "put_schema_only",
    "put_zero_row_batch",
    "put_zero_then_rows",
    "put_rows_then_zero",
    "put_timestamp_no_zone",
    "put_not_nullable",
    "put_sentinels",
    "put_list_arrays",
    "put_list_long_arrays",
    "put_list_nested_arrays",
    "put_list_vectors",
    "put_list_vectors_of_arrays",
];

#[tokio::test]
async fn uploads_answer_what_java_answers() {
    let server = Server::start().await;
    for case in PUT_CASES {
        let messages = fixture_messages(&format!("{case}.put"));
        let batches = messages.len() - 1;
        let java_info = fixture_info(case);
        let id = export_of(&java_info);
        let java_schema = fixture_schema(case);

        let acknowledged = server
            .do_put(messages)
            .await
            .unwrap_or_else(|status| panic!("{case}: {status:?}"));
        // one empty result per record batch
        assert_eq!(acknowledged.len(), batches, "{case}");
        assert!(acknowledged
            .iter()
            .all(|result| result.app_metadata.is_empty()));

        assert_eq!(server.get_schema(id).await.unwrap(), java_schema, "{case}");
        let info = server
            .flight()
            .get_flight_info(server.authorized(export_descriptor(id)))
            .await
            .unwrap()
            .into_inner();
        assert_info_matches(case, &info, &java_schema);
        let sent = server.do_get(id).await.unwrap();
        assert_do_get_matches(case, &sent, &java_schema);
    }
}

/// The types the web IDE asks `ApplyPreviewColumns` to leave as they are.
const UNPREVIEWED_TYPES: [&str; 23] = [
    "byte",
    "java.lang.Byte",
    "char",
    "java.lang.Character",
    "short",
    "java.lang.Short",
    "int",
    "java.lang.Integer",
    "long",
    "java.lang.Long",
    "float",
    "java.lang.Float",
    "double",
    "java.lang.Double",
    "boolean",
    "java.lang.Boolean",
    "java.lang.String",
    "java.math.BigDecimal",
    "java.math.BigInteger",
    "java.time.Instant",
    "java.time.ZonedDateTime",
    "java.time.LocalDate",
    "java.time.LocalTime",
];

#[tokio::test]
async fn previews_of_uploaded_lists_answer_what_java_answers() {
    use oxhaven_proto::grpc::table_service_client::TableServiceClient;
    use oxhaven_proto::grpc::{table_reference, ApplyPreviewColumnsRequest, TableReference};

    let server = Server::start().await;
    for (source, case) in [
        ("put_list_arrays", "preview_list_arrays"),
        ("put_list_long_arrays", "preview_list_long_arrays"),
        ("put_list_vectors", "preview_list_vectors"),
    ] {
        let messages = fixture_messages(&format!("{source}.put"));
        server.do_put(messages).await.unwrap();
        let uploaded = export_of(&fixture_info(source));
        let id = export_of(&fixture_info(case));
        let java_schema = fixture_schema(case);

        let request = ApplyPreviewColumnsRequest {
            source_id: Some(TableReference {
                r#ref: Some(table_reference::Ref::Ticket(
                    crate::session::export_id_to_ticket(uploaded),
                )),
            }),
            result_id: Some(crate::session::export_id_to_ticket(id)),
            convert_arrays: true,
            unpreviewed_types: UNPREVIEWED_TYPES.map(str::to_owned).to_vec(),
        };
        let created = TableServiceClient::new(server.channel.clone())
            .apply_preview_columns(server.authorized(request))
            .await
            .unwrap_or_else(|status| panic!("{case}: {status:?}"))
            .into_inner();
        assert!(created.success && created.is_static, "{case}: {created:?}");
        assert_eq!(schema_of_ipc(&created.schema_header), java_schema, "{case}");

        assert_eq!(server.get_schema(id).await.unwrap(), java_schema, "{case}");
        let info = server
            .flight()
            .get_flight_info(server.authorized(export_descriptor(id)))
            .await
            .unwrap()
            .into_inner();
        assert_info_matches(case, &info, &java_schema);
        let sent = server.do_get(id).await.unwrap();
        assert_do_get_matches(case, &sent, &java_schema);
    }
}

#[tokio::test]
async fn zero_row_batches_do_not_disturb_the_rows() {
    // A zero-row batch makes Java insert the row range `0..-1` into the row set of the table;
    // the captures still show the right table, and so must this one: 3 rows, none, then 1.
    let server = Server::start().await;
    let messages = fixture_messages("put_rows_then_zero.put");
    let id = export_of(&fixture_info("put_rows_then_zero"));
    server.do_put(messages).await.unwrap();
    let table = server.session().get_export(id).unwrap().value().unwrap();
    let table = table.as_table().unwrap().clone();
    assert_eq!(table.row_set().size(), 4);
    assert!(table.is_flat());
    let snapshot = snapshot(&table).unwrap();
    let ints = snapshot.columns[4]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ints.values(), &[1, i32::MAX, 1, 1]);

    let empty = export_of(&fixture_info("put_zero_row_batch"));
    server
        .do_put(fixture_messages("put_zero_row_batch.put"))
        .await
        .unwrap();
    let table = server.session().get_export(empty).unwrap().value().unwrap();
    assert_eq!(table.as_table().unwrap().row_set().size(), 0);
}

/// The calls of a `refused_*.status` fixture against this server.
async fn refused(server: &Server, case: &str, id: i32) -> Vec<String> {
    let messages = fixture_messages(&format!("{case}.put"));
    vec![
        format!("DoPut {}", code_of(server.do_put(messages)).await),
        format!("GetSchema {}", code_of(server.get_schema(id)).await),
        format!("DoGet {}", code_of(server.do_get(id)).await),
    ]
}

#[tokio::test]
async fn refused_uploads_get_the_status_codes_of_java() {
    let server = Server::start().await;
    for (case, id) in [
        ("refused_no_descriptor", 2001),
        ("refused_batch_before_schema", 2002),
        ("refused_no_schema", 2003),
        ("refused_second_descriptor", 2004),
        ("refused_second_schema", 2006),
        ("refused_garbage_batch", 2009),
        ("refused_too_few_columns", 2010),
        ("refused_list_component", 2101),
        ("refused_list_object_vector_of_int", 2102),
        ("refused_list_object_vector_of_object_vector", 2103),
        ("refused_list_as_int", 2104),
        ("refused_int_as_array", 2105),
        ("refused_large_list", 2106),
    ] {
        let java = fixture_lines(&format!("{case}.status"));
        assert_eq!(refused(&server, case, id).await, java, "{case}");
    }

    // the same upload twice: the second is refused and the first table stays
    let first = fixture_messages("refused_export_exists.put");
    server.do_put(first).await.unwrap();
    assert_eq!(
        refused(&server, "refused_export_exists", 2007).await,
        fixture_lines("refused_export_exists.status")
    );
}

#[tokio::test]
async fn an_upload_to_a_scope_descriptor_is_a_variable_as_in_java() {
    let server = Server::start().await;
    // Java resolves `scope/x` with its query scope resolver and publishes the table as the
    // variable `x`. No export is made, so the reads of the export id wait for one.
    assert_eq!(
        refused(&server, "refused_not_an_export", 2008).await,
        fixture_lines("refused_not_an_export.status")
    );
    let variable = server.state.scope.get("x").expect("the variable x");
    assert!(variable.as_table().is_some());
}

#[tokio::test]
async fn uploads_java_accepts_and_this_server_does_not() {
    let server = Server::start().await;
    // Java renames the columns `a b` and `1x` to legal names and then fails every read of
    // the table. Renaming is not ported: the upload is refused and says so.
    assert_eq!(
        fixture_lines("refused_illegal_names.status"),
        [
            "DoPut OK",
            "GetSchema FAILED_PRECONDITION",
            "DoGet INVALID_ARGUMENT"
        ]
    );
    assert_eq!(
        refused(&server, "refused_illegal_names", 1999).await,
        [
            "DoPut UNIMPLEMENTED",
            "GetSchema UNIMPLEMENTED",
            "DoGet UNIMPLEMENTED"
        ]
    );
}

#[tokio::test]
async fn calls_that_name_no_table_get_the_status_codes_of_java() {
    let server = &Server::start().await;
    let flight = || server.flight();
    let schema = |descriptor: FlightDescriptor| async {
        flight().get_schema(server.authorized(descriptor)).await
    };
    let info = |descriptor: FlightDescriptor| async {
        flight()
            .get_flight_info(server.authorized(descriptor))
            .await
    };
    let list = |expression: &'static [u8]| async move {
        let criteria = Criteria {
            expression: expression.into(),
        };
        let mut stream = flight()
            .list_flights(server.authorized(criteria))
            .await?
            .into_inner();
        let mut listed = 0;
        while stream.message().await?.is_some() {
            listed += 1;
        }
        Ok::<_, Status>(listed)
    };
    let command = FlightDescriptor {
        r#type: DescriptorType::Cmd as i32,
        cmd: b"x".to_vec().into(),
        ..Default::default()
    };
    let ours = [
        (
            "DoGet unknown export",
            code_of(server.do_get(999_999)).await,
        ),
        (
            "DoGet empty ticket",
            code_of(server.do_get_ticket(Vec::new())).await,
        ),
        (
            "DoGet unknown route",
            code_of(server.do_get_ticket(b"zzz".to_vec())).await,
        ),
        (
            "DoGet malformed export ticket",
            code_of(server.do_get_ticket(b"e12".to_vec())).await,
        ),
        (
            "GetSchema unknown export",
            code_of(server.get_schema(999_999)).await,
        ),
        (
            "GetSchema unknown route",
            code_of(schema(path_descriptor(&["nowhere", "1"]))).await,
        ),
        (
            "GetSchema empty path",
            code_of(schema(path_descriptor(&[]))).await,
        ),
        ("GetSchema command", code_of(schema(command)).await),
        (
            "GetSchema unknown type",
            code_of(schema(FlightDescriptor::default())).await,
        ),
        (
            "GetSchema export id not a number",
            code_of(schema(path_descriptor(&["export", "x"]))).await,
        ),
        (
            "GetSchema export path too long",
            code_of(schema(path_descriptor(&["export", "1", "2"]))).await,
        ),
        (
            "GetFlightInfo unknown export",
            code_of(info(export_descriptor(999_999))).await,
        ),
        (
            "GetFlightInfo unknown route",
            code_of(info(path_descriptor(&["nowhere", "1"]))).await,
        ),
        ("ListFlights", code_of(list(b"")).await),
        ("ListFlights with an expression", code_of(list(b"x")).await),
    ];
    let ours: Vec<String> = ours
        .iter()
        .map(|(call, code)| format!("{call}: {code}"))
        .collect();
    assert_eq!(ours, fixture_lines("calls.status"));
    assert_eq!(list(b"").await.unwrap(), 0);
}

#[tokio::test]
async fn calls_wait_for_an_export_that_is_defined_later() {
    let server = Server::start().await;
    let schema = tokio::spawn({
        let (flight, request) = (server.flight(), server.authorized(export_descriptor(5)));
        async move { flight.clone().get_schema(request).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!schema.is_finished());
    server.export(5, engine_table(0..3, Some(1)));
    let answer = schema.await.unwrap().unwrap().into_inner();
    assert_eq!(
        schema_of_ipc(&answer.schema),
        fixture_schema("engine_nulls")
    );
}

#[tokio::test]
async fn flight_calls_need_a_session_and_a_table() {
    let server = Server::start().await;
    let unauthenticated = server
        .flight()
        .do_get(oxhaven_proto::flight::Ticket {
            ticket: export_id_to_bytes(1).into(),
        })
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    server
        .session()
        .new_export(1)
        .unwrap()
        .submit(|_| Ok(ExportValue::object(7_i32)))
        .unwrap();
    let not_a_table = server.do_get(1).await.unwrap_err();
    assert_eq!(not_a_table.code(), Code::FailedPrecondition);
    assert!(not_a_table
        .message()
        .contains("is not a subscribable table"));
    let not_a_table = server.get_schema(1).await.unwrap_err();
    assert_eq!(not_a_table.code(), Code::NotFound);

    for call in ["do_action", "list_actions"] {
        let status = match call {
            "do_action" => server
                .flight()
                .do_action(server.authorized(oxhaven_proto::flight::Action::default()))
                .await
                .map(|_| ())
                .unwrap_err(),
            _ => server
                .flight()
                .list_actions(server.authorized(oxhaven_proto::flight::Empty {}))
                .await
                .map(|_| ())
                .unwrap_err(),
        };
        assert_eq!(status.code(), Code::Unimplemented, "{call}");
        assert!(status.message().starts_with("not ported yet: "), "{call}");
    }
}

#[tokio::test]
async fn upload_ends_unauthenticated_when_its_session_ends() {
    let server = Server::start().await;
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<FlightData>();
    let mut messages = fixture_messages("put_plain.put").into_iter();
    sender.send(messages.next().unwrap()).unwrap();
    let request = server.authorized(tokio_stream::wrappers::UnboundedReceiverStream::new(
        receiver,
    ));
    let mut results = server.flight().do_put(request).await.unwrap().into_inner();
    sender.send(messages.next().unwrap()).unwrap();
    assert!(results.message().await.unwrap().is_some());

    server.state.sessions.close_session(&server.session());
    let status = results.message().await.unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
    drop(sender);
}

fn ipc_parts(batch: &RecordBatch) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let schema = crate::schema::schema_message(&batch.schema());
    let columns: Vec<ArrayRef> = batch.columns().to_vec();
    let nullable = vec![true; columns.len()];
    let encoded = encode_batch(&columns, &nullable, 0, batch.num_rows()).unwrap();
    (schema, encoded.header, encoded.body)
}

fn status_of(error: UploadError) -> Status {
    match error {
        UploadError::Status(status) => status,
        UploadError::Internal(text) => panic!("an internal error: {text}"),
    }
}

#[test]
fn uploaded_sentinel_values_are_nulls() {
    // A client that writes Deephaven's null values without validity bits means nulls: Java
    // stores what it reads, and its null of an int is the smallest int.
    let schema = Arc::new(Schema::new(vec![
        Field::new("In", DataType::Int32, true),
        Field::new("Do", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, i32::MIN])),
            Arc::new(Float64Array::from(vec![-f64::MAX, 2.0])),
        ],
    )
    .unwrap();
    let (schema, header, body) = ipc_parts(&batch);
    let mut upload = TableUpload::default();
    assert!(!upload.message(&schema, &[]).unwrap());
    assert!(upload.message(&header, &body).unwrap());
    let table = upload.finish().unwrap();
    let rows = snapshot(&table).unwrap();
    assert_eq!(rows.rows, 2);
    assert_eq!(
        (rows.columns[0].null_count(), rows.columns[0].is_null(1)),
        (1, true)
    );
    assert_eq!(
        (rows.columns[1].null_count(), rows.columns[1].is_null(0)),
        (1, true)
    );
}

#[test]
fn upload_refuses_what_it_cannot_read() {
    let definition = definition(&[("In", ColumnType::Int)]);
    let batch = batch_for(&definition, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]);
    let (schema, header, body) = ipc_parts(&batch);

    let mut upload = TableUpload::default();
    let status = status_of(upload.message(&header, &body).unwrap_err());
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Schema must be processed before record-batch messages"
    );

    let status = status_of(TableUpload::default().finish().map(|_| ()).unwrap_err());
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(status.message(), "Result flight schema never provided");

    // a header that describes more bytes than the body has
    let mut upload = TableUpload::default();
    upload.message(&schema, &[]).unwrap();
    assert!(matches!(
        upload.message(&header, &body[..body.len() - 8]),
        Err(UploadError::Internal(_))
    ));
    // bytes that are no message at all
    let mut upload = TableUpload::default();
    upload.message(&schema, &[]).unwrap();
    assert!(upload.message(&[1, 2, 3], &[]).is_err());
}

fn typed(java_type: &str) -> std::collections::HashMap<String, String> {
    [("deephaven:type".to_owned(), java_type.to_owned())].into()
}

#[test]
fn schemas_this_server_cannot_store_are_unimplemented() {
    let unsupported = |field: Field| {
        let status = status_of(convert_schema(&Schema::new(vec![field])).unwrap_err());
        assert_eq!(status.code(), Code::Unimplemented, "{status:?}");
        assert!(
            status.message().starts_with("not ported yet: "),
            "{status:?}"
        );
    };
    unsupported(Field::new("X", DataType::Date32, true));
    unsupported(Field::new("X", DataType::Int32, true).with_metadata(typed("long")));
    unsupported(Field::new("X", DataType::Utf8, true).with_metadata(typed("java.math.BigDecimal")));
    unsupported(Field::new("a b", DataType::Int32, true));

    let duplicate = status_of(
        convert_schema(&Schema::new(vec![
            Field::new("X", DataType::Int32, true),
            Field::new("X", DataType::Int32, true),
        ]))
        .unwrap_err(),
    );
    assert_eq!(duplicate.code(), Code::InvalidArgument);

    let converted = convert_schema(&Schema::new(vec![
        Field::new("Ch", DataType::UInt16, true),
        Field::new("Ti", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("Bo", DataType::Boolean, true).with_metadata(typed("boolean")),
    ]))
    .unwrap();
    assert_eq!(
        converted,
        definition(&[
            ("Ch", ColumnType::Char),
            ("Ti", ColumnType::Instant),
            ("Bo", ColumnType::Boolean)
        ])
    );
}

/// Recorded from Java 2026-11-15 by uploading each field and reading the schema of the
/// table back (the class names Java gave the column), and for the refusals the status.
#[test]
fn list_fields_are_the_arrays_and_vectors_java_makes_of_them() {
    use ColumnType as T;
    const VECTOR: &str = "io.deephaven.vector.";
    let list = |element: DataType| DataType::List(Arc::new(Field::new("item", element, true)));
    let field = |data_type: DataType, entries: &[(&str, &str)]| {
        let metadata: std::collections::HashMap<String, String> = entries
            .iter()
            .map(|(key, value)| (format!("deephaven:{key}"), (*value).to_owned()))
            .collect();
        Field::new("X", data_type, true).with_metadata(metadata)
    };
    let converted = |field: Field| {
        convert_schema(&Schema::new(vec![field])).map(|definition| {
            let column = &definition.columns()[0];
            column.column_type.clone()
        })
    };
    let ints = || list(DataType::Int32);
    let texts = || list(DataType::Utf8);
    let object_vector = format!("{VECTOR}ObjectVector");
    let int_vector = format!("{VECTOR}IntVector");

    for (field, expected) in [
        // without a type: arrays, whatever the depth
        (field(ints(), &[]), T::array(T::Int)),
        (field(list(DataType::Int8), &[]), T::array(T::Byte)),
        (field(list(DataType::Boolean), &[]), T::array(T::Boolean)),
        (
            field(list(DataType::Timestamp(TimeUnit::Nanosecond, None)), &[]),
            T::array(T::Instant),
        ),
        (field(list(texts()), &[]), T::array(T::array(T::String))),
        // a component type that agrees changes nothing
        (field(ints(), &[("componentType", "int")]), T::array(T::Int)),
        // the named class
        (field(ints(), &[("type", "int[]")]), T::array(T::Int)),
        (field(ints(), &[("type", &int_vector)]), T::vector(T::Int)),
        (
            field(list(ints()), &[("type", &format!("{int_vector}[]"))]),
            T::array(T::vector(T::Int)),
        ),
        (
            field(list(texts()), &[("type", "java.lang.String[][]")]),
            T::array(T::array(T::String)),
        ),
        // a vector of objects: of the elements' default type, or of the component type
        (
            field(texts(), &[("type", &object_vector)]),
            T::vector(T::String),
        ),
        (
            field(list(DataType::Boolean), &[("type", &object_vector)]),
            T::vector(T::Boolean),
        ),
        (
            field(
                list(ints()),
                &[("type", &object_vector), ("componentType", "int[]")],
            ),
            T::vector(T::array(T::Int)),
        ),
        (
            field(
                list(ints()),
                &[("type", &object_vector), ("componentType", &int_vector)],
            ),
            T::vector(T::vector(T::Int)),
        ),
    ] {
        let described = format!("{field:?}");
        match converted(field) {
            Ok(column_type) => assert_eq!(column_type, expected, "{described}"),
            Err(error) => panic!("{described}: {error:?}"),
        }
    }

    // Java throws an IllegalArgumentException for these, which the client sees as an
    // error id and later readers of the export as a failed dependency.
    for (field, message) in [
        (
            field(ints(), &[("type", "int[]"), ("componentType", "long")]),
            "Invalid component type long for data type int[]",
        ),
        (
            field(ints(), &[("componentType", "long")]),
            "Invalid component type long for data type int[]",
        ),
        (
            field(ints(), &[("type", &int_vector), ("componentType", "long")]),
            "Invalid component type long for data type io.deephaven.vector.IntVector",
        ),
        (
            field(ints(), &[("type", &object_vector)]),
            "Invalid component type int for data type io.deephaven.vector.IntVector",
        ),
        (
            field(
                ints(),
                &[("type", &object_vector), ("componentType", "int")],
            ),
            "Invalid component type int for data type io.deephaven.vector.IntVector",
        ),
    ] {
        let described = format!("{field:?}");
        match converted(field) {
            Err(UploadError::Internal(text)) => assert_eq!(text, message, "{described}"),
            other => panic!("{described}: {other:?}"),
        }
    }

    // and answers these with a status
    let code = |field: Field| {
        let described = format!("{field:?}");
        match converted(field) {
            Err(UploadError::Status(status)) => status.code(),
            other => panic!("{described}: {other:?}"),
        }
    };
    let invalid = Code::InvalidArgument;
    assert_eq!(code(field(ints(), &[("type", "int")])), invalid);
    assert_eq!(code(field(DataType::Int32, &[("type", "int[]")])), invalid);
    assert_eq!(
        code(field(DataType::Int32, &[("type", &object_vector)])),
        invalid
    );
    let large = DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
    assert_eq!(code(field(large, &[])), invalid);
    let object_vectors = format!("{object_vector}[]");
    assert_eq!(
        code(field(list(texts()), &[("type", &object_vectors)])),
        invalid
    );
    assert_eq!(
        code(field(
            list(texts()),
            &[("type", &object_vector), ("componentType", &object_vector)]
        )),
        invalid
    );

    // What Java reads and this server does not yet.
    let unported = Code::Unimplemented;
    // a list of longs read as ints, and Java's INVALID_ARGUMENT for ints read as strings
    assert_eq!(
        code(field(list(DataType::Int64), &[("type", "int[]")])),
        unported
    );
    assert_eq!(
        code(field(ints(), &[("type", "java.lang.String[]")])),
        unported
    );
    // an ObjectVector of boxed ints
    assert_eq!(
        code(field(
            ints(),
            &[
                ("type", &object_vector),
                ("componentType", "java.lang.Integer")
            ]
        )),
        unported
    );
    // `boolean[]`, which has no nulls
    let flags = DataType::List(Arc::new(Field::new("item", DataType::Boolean, false)));
    assert_eq!(code(field(flags, &[])), unported);
    assert_eq!(code(field(list(DataType::Binary), &[])), unported);
}

mod locking {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use oxhaven_contracts::graph::Dependency;
    use oxhaven_contracts::graph::{ClockValue, Notification, UpdateGraph, UpdateSource};
    use oxhaven_contracts::table::{TableAttributes, UpdateListener};

    use super::*;

    /// Counts how often its shared lock is taken and whether it is held.
    #[derive(Default)]
    struct CountingGraph {
        taken: AtomicUsize,
        held: AtomicUsize,
    }

    impl UpdateGraph for CountingGraph {
        fn clock(&self) -> ClockValue {
            unreachable!()
        }
        fn add_source(&self, _source: &Arc<dyn UpdateSource>) {}
        fn add_notification(&self, _notification: Box<dyn Notification>) {}
        fn sources_satisfied(&self, _step: u64) -> bool {
            true
        }
        fn run_cycle(&self) {}
        fn with_shared_lock(&self, f: &mut dyn FnMut()) {
            self.taken.fetch_add(1, Ordering::SeqCst);
            self.held.fetch_add(1, Ordering::SeqCst);
            f();
            self.held.fetch_sub(1, Ordering::SeqCst);
        }
        fn current_thread_processes_updates(&self) -> bool {
            false
        }
    }

    /// A table that notes whether the lock of its graph was held at each read.
    struct Watched {
        inner: Arc<dyn Table>,
        graph: Arc<CountingGraph>,
        refreshing: bool,
        reads_without_lock: AtomicUsize,
    }

    impl Watched {
        fn note(&self) {
            if self.graph.held.load(Ordering::SeqCst) == 0 {
                self.reads_without_lock.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl Dependency for Watched {
        fn satisfied(&self, step: u64) -> bool {
            self.inner.satisfied(step)
        }
    }

    impl Table for Watched {
        fn definition(&self) -> &TableDefinition {
            self.inner.definition()
        }
        fn row_set(&self) -> Arc<RowSet> {
            self.note();
            self.inner.row_set()
        }
        fn prev_row_set(&self) -> Arc<RowSet> {
            self.inner.prev_row_set()
        }
        fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
            self.note();
            self.inner.column(index)
        }
        fn is_refreshing(&self) -> bool {
            self.refreshing
        }
        fn attributes(&self) -> TableAttributes {
            self.inner.attributes()
        }
        fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
            Some(self.graph.clone())
        }
        fn add_listener(&self, _listener: &Arc<dyn UpdateListener>) {}
    }

    #[test]
    fn refreshing_table_is_read_under_the_shared_lock_of_its_graph() {
        for refreshing in [true, false] {
            let graph = Arc::new(CountingGraph::default());
            let watched = Arc::new(Watched {
                inner: engine_table(0..3, Some(1)),
                graph: graph.clone(),
                refreshing,
                reads_without_lock: AtomicUsize::new(0),
            });
            let table: Arc<dyn Table> = watched.clone();
            let rows = snapshot(&table).unwrap();
            assert_eq!((rows.rows, rows.columns.len()), (3, 10));
            let unlocked_reads = watched.reads_without_lock.load(Ordering::SeqCst);
            if refreshing {
                // every read in one hold of the lock
                assert_eq!(graph.taken.load(Ordering::SeqCst), 1);
                assert_eq!(unlocked_reads, 0);
            } else {
                assert_eq!(graph.taken.load(Ordering::SeqCst), 0);
                assert_eq!(unlocked_reads, 11);
            }
        }
    }
}

/// The web client uploads a table as an `OpenDoPut` call with the first message and a
/// `NextDoPut` call for each later one, the last of which ends the client's side.
#[tokio::test]
async fn a_browser_upload_is_the_upload_of_its_messages_in_order() {
    use oxhaven_proto::browser_flight::browser_flight_service_client::BrowserFlightServiceClient;

    let server = Server::start().await;
    let case = "put_three_batches";
    let messages = fixture_messages(&format!("{case}.put"));
    let batches = messages.len() - 1;
    let id = export_of(&fixture_info(case));
    let java_schema = fixture_schema(case);
    let client = || BrowserFlightServiceClient::new(server.channel.clone());
    // the export that names the stream
    let stream = 900;
    let call = |message: FlightData, sequence: usize, half_close: bool| {
        let mut request = server.authorized(message);
        let metadata = request.metadata_mut();
        metadata.insert(
            "x-deephaven-stream-ticket",
            stream.to_string().parse().unwrap(),
        );
        metadata.insert(
            "x-deephaven-stream-sequence",
            sequence.to_string().parse().unwrap(),
        );
        if half_close {
            metadata.insert("x-deephaven-stream-halfclose", "1".parse().unwrap());
        }
        request
    };

    let mut messages = messages.into_iter().enumerate();
    let (_, first) = messages.next().unwrap();
    let mut results = client()
        .open_do_put(call(first, 0, false))
        .await
        .unwrap()
        .into_inner();
    // the later messages out of order: the stream waits for the one that is next
    let mut later: Vec<(usize, FlightData)> = messages.collect();
    later.swap(0, 1);
    for (sequence, message) in later {
        client()
            .next_do_put(call(message, sequence, false))
            .await
            .unwrap();
    }
    client()
        .next_do_put(call(FlightData::default(), batches + 1, true))
        .await
        .unwrap();
    let mut acknowledged = 0;
    while let Some(result) = results.message().await.unwrap() {
        assert!(result.app_metadata.is_empty());
        acknowledged += 1;
    }
    assert_eq!(acknowledged, batches);
    assert_eq!(server.get_schema(id).await.unwrap(), java_schema);
    let sent = server.do_get(id).await.unwrap();
    assert_do_get_matches(case, &sent, &java_schema);

    // a call without the headers of a stream, and the streams that are not ported
    let status = client()
        .open_do_put(server.authorized(FlightData::default()))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "no x-deephaven-stream headers, cannot handle open request"
    );
    let status = client()
        .open_do_exchange(call(FlightData::default(), 0, false))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Unimplemented);
    assert!(
        status.message().starts_with(
            "not ported: io.deephaven.server.arrow.BrowserFlightServiceGrpcBinding#bindService"
        ),
        "{status:?}"
    );
}
