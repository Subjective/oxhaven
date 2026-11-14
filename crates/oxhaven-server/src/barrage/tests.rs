//! Barrage against what the Java server sent (`fixtures/`), and against a model of a client.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{
    make_array, new_empty_array, Array, ArrayData, ArrayRef, BooleanArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use flatbuffers::FlatBufferBuilder;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::table::in_memory_table;
use oxhaven_proto::barrage::{
    BarrageMessageType, BarrageMessageWrapper, BarrageMessageWrapperArgs, BarrageSnapshotRequest,
    BarrageSnapshotRequestArgs, BarrageSubscriptionRequest, BarrageSubscriptionRequestArgs,
    BarrageUpdateMetadata, MAGIC,
};
use oxhaven_proto::flight::FlightData;
use oxhaven_proto::grpc::create_input_table_request::input_table_kind::{InMemoryKeyBacked, Kind};
use oxhaven_proto::grpc::create_input_table_request::{Definition, InputTableKind};
use oxhaven_proto::grpc::CreateInputTableRequest;
use oxhaven_testkit::{definition, RandomUpdates, ScriptedTable};
use prost::Message;
use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use super::codec::{
    decode_row_set, decode_shift_data, encode_row_set, encode_shift_data, ColumnSet,
};
use super::producer::{Job, Listener, Producer, Scheduler};
use super::writer::WireOptions;
use crate::flight::tests::support::Server;
use crate::flight::{do_get_messages, wire_schema};
use crate::inputtable::create_input_table;
use crate::schema::schema_ipc_bytes;
use crate::session::ticket_to_export_id;

// ---------------------------------------------------------------------------------------
// Decoding, as a client does
// ---------------------------------------------------------------------------------------

type Cell = Option<String>;

fn cells(array: &ArrayRef) -> Vec<Cell> {
    // instants as their nanoseconds
    let array = match array.data_type() {
        DataType::Timestamp(_, _) => arrow::compute::cast(array, &DataType::Int64).unwrap(),
        _ => array.clone(),
    };
    (0..array.len())
        .map(|row| {
            array
                .is_valid(row)
                .then(|| array_value_to_string(&array, row).unwrap())
        })
        .collect()
}

fn schema_of(message: &FlightData) -> Option<Schema> {
    let parsed = arrow::ipc::root_as_message(&message.data_header).ok()?;
    arrow::ipc::convert::try_fb_to_schema(parsed.header_as_schema()?).ok()
}

/// The columns of a record batch message. Barrage columns need not have the same length,
/// so this is not a `RecordBatch`.
fn columns_of(message: &FlightData, schema: &Schema) -> Vec<ArrayRef> {
    let parsed = arrow::ipc::root_as_message(&message.data_header).unwrap();
    let batch = parsed.header_as_record_batch().expect("a record batch");
    let nodes = batch.nodes().unwrap();
    let buffers = batch.buffers().unwrap();
    assert_eq!(nodes.len(), schema.fields().len());
    let mut next_buffer = 0;
    let mut columns = Vec::new();
    for (field, node) in schema.fields().iter().zip(nodes.iter()) {
        let count = match field.data_type() {
            DataType::Utf8 => 3,
            _ => 2,
        };
        let mut parts: Vec<Buffer> = (next_buffer..next_buffer + count)
            .map(|index| {
                let buffer = buffers.get(index);
                let start = buffer.offset() as usize;
                Buffer::from(&message.data_body[start..start + buffer.length() as usize])
            })
            .collect();
        next_buffer += count;
        let length = node.length() as usize;
        if length == 0 {
            columns.push(new_empty_array(field.data_type()));
            continue;
        }
        let validity = parts.remove(0);
        let nulls = (node.null_count() > 0).then_some(validity);
        let data = ArrayData::try_new(field.data_type().clone(), length, nulls, 0, parts, vec![])
            .unwrap_or_else(|problem| panic!("{}: {problem}", field.name()));
        assert_eq!(
            data.null_count() as i64,
            node.null_count(),
            "{}",
            field.name()
        );
        columns.push(make_array(data));
    }
    assert_eq!(next_buffer, buffers.len());
    columns
}

/// One Barrage message: its metadata, and the rows of all its record batches.
#[derive(Debug, Clone, PartialEq)]
struct Update {
    first_seq: i64,
    last_seq: i64,
    is_snapshot: bool,
    effective_viewport: Option<RowSet>,
    effective_reverse_viewport: bool,
    effective_column_set: Option<Vec<u8>>,
    added_rows: RowSet,
    removed_rows: RowSet,
    shift_data: Option<RowSetShiftData>,
    added_rows_included: Option<RowSet>,
    /// `None` when the message has no mod column nodes at all.
    modified_rows: Option<Vec<RowSet>>,
    table_size: i64,
    /// Per column, the cells of the included rows, then of the modified rows.
    added: Vec<Vec<Cell>>,
    modified: Vec<Vec<Cell>>,
}

fn metadata_of(message: &FlightData) -> Option<Vec<u8>> {
    let wrapper = flatbuffers::root::<BarrageMessageWrapper>(&message.app_metadata).ok()?;
    assert_eq!(wrapper.magic(), MAGIC);
    assert_eq!(
        wrapper.msg_type(),
        BarrageMessageType::BarrageUpdateMetadata
    );
    Some(wrapper.msg_payload().unwrap().bytes().to_vec())
}

/// The row sets of `metadata`, as the bytes they were sent as.
fn row_set_bytes(metadata: &[u8]) -> Vec<Vec<u8>> {
    let metadata = flatbuffers::root::<BarrageUpdateMetadata>(metadata).unwrap();
    let mut all = Vec::new();
    for rows in [
        metadata.effective_viewport(),
        metadata.added_rows(),
        metadata.removed_rows(),
        metadata.added_rows_included(),
    ]
    .into_iter()
    .flatten()
    {
        all.push(rows.bytes().to_vec());
    }
    for node in metadata.mod_column_nodes().into_iter().flatten() {
        all.extend(node.modified_rows().map(|rows| rows.bytes().to_vec()));
    }
    all
}

/// Groups record batch messages into Barrage messages: one starts with each batch that
/// has metadata, and its first rows are the included rows.
fn updates_of(messages: &[FlightData], schema: &Schema) -> Vec<Update> {
    let mut updates: Vec<Update> = Vec::new();
    let mut added_left = 0;
    for message in messages {
        let columns = columns_of(message, schema);
        if let Some(bytes) = metadata_of(message) {
            let metadata = flatbuffers::root::<BarrageUpdateMetadata>(&bytes).unwrap();
            let rows = |bytes: Option<flatbuffers::Vector<'_, i8>>| {
                bytes.map(|bytes| decode_row_set(bytes.bytes()).unwrap())
            };
            let update = Update {
                first_seq: metadata.first_seq(),
                last_seq: metadata.last_seq(),
                is_snapshot: metadata.is_snapshot(),
                effective_viewport: rows(metadata.effective_viewport()),
                effective_reverse_viewport: metadata.effective_reverse_viewport(),
                effective_column_set: metadata
                    .effective_column_set()
                    .map(|columns| columns.bytes().to_vec()),
                added_rows: rows(metadata.added_rows()).unwrap_or_default(),
                removed_rows: rows(metadata.removed_rows()).unwrap_or_default(),
                shift_data: metadata
                    .shift_data()
                    .map(|bytes| decode_shift_data(bytes.bytes()).unwrap()),
                added_rows_included: rows(metadata.added_rows_included()),
                modified_rows: metadata.mod_column_nodes().map(|nodes| {
                    nodes
                        .iter()
                        .map(|node| rows(node.modified_rows()).unwrap_or_default())
                        .collect()
                }),
                table_size: metadata.table_size(),
                added: vec![Vec::new(); columns.len()],
                modified: vec![Vec::new(); columns.len()],
            };
            added_left = update
                .added_rows_included
                .as_ref()
                .unwrap_or(&update.added_rows)
                .size() as usize;
            updates.push(update);
        }
        let update = updates.last_mut().expect("the first batch has metadata");
        let rows = columns.iter().map(|column| column.len()).max().unwrap_or(0);
        let into = if added_left > 0 {
            assert!(rows <= added_left, "a batch of included and modified rows");
            added_left -= rows;
            &mut update.added
        } else {
            &mut update.modified
        };
        for (cells_of_column, column) in into.iter_mut().zip(&columns) {
            cells_of_column.extend(cells(column));
        }
    }
    assert_eq!(added_left, 0, "included rows that were not sent");
    updates
}

/// What a client holds of a table after the messages it was sent.
///
/// A subscriber to the whole table holds rows by key. A viewport subscriber holds the
/// rows of its viewport in order: removed rows are positions in the viewport as it was,
/// included and modified rows positions in the viewport as it is.
#[derive(Debug, Clone, PartialEq, Default)]
struct Replica {
    by_key: BTreeMap<i64, Vec<Cell>>,
    in_order: Vec<Vec<Cell>>,
    viewport: Option<RowSet>,
    reverse_viewport: bool,
    columns: Option<Vec<u8>>,
}

impl Replica {
    fn apply(&mut self, update: &Update, is_viewport: bool) {
        if update.is_snapshot {
            self.viewport = update.effective_viewport.clone();
            self.reverse_viewport = update.effective_reverse_viewport;
            self.columns = update.effective_column_set.clone();
        }
        let included = update
            .added_rows_included
            .as_ref()
            .unwrap_or(&update.added_rows);
        let row = |position: usize| -> Vec<Cell> {
            update
                .added
                .iter()
                .map(|column| column.get(position).cloned().flatten())
                .collect()
        };
        let empty = Vec::new();
        let modified_rows = update.modified_rows.as_ref().unwrap_or(&empty);
        if is_viewport {
            assert!(
                update.shift_data.is_none(),
                "a viewport client is sent no shifts"
            );
            assert!(update.added_rows_included.is_none());
            let mut survivors = Vec::new();
            for (position, cells) in std::mem::take(&mut self.in_order).into_iter().enumerate() {
                if !update.removed_rows.contains(position as i64) {
                    survivors.push(cells);
                }
            }
            assert!(
                update.removed_rows.last_row_key().is_none_or(|last| {
                    (last as usize) < survivors.len() + update.removed_rows.size() as usize
                }),
                "a removed position outside the viewport"
            );
            let mut survivors = survivors.into_iter();
            let size = survivors.len() + included.size() as usize;
            let mut next_added = 0;
            for position in 0..size {
                if included.contains(position as i64) {
                    self.in_order.push(row(next_added));
                    next_added += 1;
                } else {
                    self.in_order
                        .push(survivors.next().expect("a surviving row"));
                }
            }
            assert_eq!(
                next_added,
                included.size() as usize,
                "an added position past the end"
            );
            for (column, rows) in modified_rows.iter().enumerate() {
                for (position, key) in rows.iter().enumerate() {
                    self.in_order[key as usize][column] = update.modified[column][position].clone();
                }
            }
        } else {
            for key in update.removed_rows.iter() {
                self.by_key.remove(&key);
            }
            if let Some(shifted) = &update.shift_data {
                let moved: BTreeMap<i64, Vec<Cell>> = std::mem::take(&mut self.by_key)
                    .into_iter()
                    .map(|(key, cells)| {
                        let delta = shifted
                            .ranges()
                            .iter()
                            .find(|shift| shift.first <= key && key <= shift.last)
                            .map_or(0, |shift| shift.delta);
                        (key + delta, cells)
                    })
                    .collect();
                self.by_key = moved;
            }
            for (position, key) in included.iter().enumerate() {
                self.by_key.insert(key, row(position));
            }
            for (column, rows) in modified_rows.iter().enumerate() {
                for (position, key) in rows.iter().enumerate() {
                    self.by_key
                        .get_mut(&key)
                        .expect("a modified row the client has")[column] =
                        update.modified[column][position].clone();
                }
            }
        }
    }

    fn rows(&self, is_viewport: bool) -> Vec<Vec<Cell>> {
        if is_viewport {
            self.in_order.clone()
        } else {
            self.by_key.values().cloned().collect()
        }
    }
}

/// The rows of `table` at `keys`, with the cells of columns outside `columns` empty.
fn table_rows(
    table: &Arc<dyn Table>,
    keys: &RowSet,
    columns: Option<&ColumnSet>,
) -> Vec<Vec<Cell>> {
    let count = table.definition().columns().len();
    let data: Vec<Vec<Cell>> = (0..count)
        .map(|index| {
            if columns.is_none_or(|columns| columns.contains(index)) {
                cells(&table.column(index).chunk(keys).unwrap())
            } else {
                vec![None; keys.size() as usize]
            }
        })
        .collect();
    (0..keys.size() as usize)
        .map(|row| data.iter().map(|column| column[row].clone()).collect())
        .collect()
}

// ---------------------------------------------------------------------------------------
// The producer against the model
// ---------------------------------------------------------------------------------------

/// Runs jobs when the test says so.
#[derive(Default)]
struct ManualScheduler {
    now: AtomicI64,
    jobs: Mutex<Vec<Job>>,
}

impl ManualScheduler {
    fn drain(&self) {
        loop {
            let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
            if jobs.is_empty() {
                return;
            }
            self.now.fetch_add(1, Ordering::SeqCst);
            for job in jobs {
                job();
            }
        }
    }
}

impl Scheduler for ManualScheduler {
    fn now_ms(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
    fn run_immediately(&self, job: Job) {
        self.jobs.lock().unwrap().push(job);
    }
    fn run_at(&self, _time_ms: i64, job: Job) {
        self.jobs.lock().unwrap().push(job);
    }
}

#[derive(Default)]
struct Collector {
    messages: Mutex<Vec<FlightData>>,
    ended: Mutex<Option<Result<(), Status>>>,
}

impl Listener for Collector {
    fn on_next(&self, messages: Vec<FlightData>) {
        self.messages.lock().unwrap().extend(messages);
    }
    fn on_error(&self, status: Status) {
        self.ended.lock().unwrap().get_or_insert(Err(status));
    }
    fn on_completed(&self) {
        self.ended.lock().unwrap().get_or_insert(Ok(()));
    }
}

/// A subscriber of the model test: what it asked for, and what it holds.
struct Subscriber {
    collector: Arc<Collector>,
    id: super::producer::SubscriptionId,
    is_viewport: bool,
    viewport: Option<RowSet>,
    reverse: bool,
    columns: Option<ColumnSet>,
    schema: Option<Schema>,
    replica: Replica,
}

impl Subscriber {
    fn new(
        producer: &Producer,
        viewport: Option<RowSet>,
        reverse: bool,
        columns: Option<ColumnSet>,
    ) -> Self {
        let collector = Arc::new(Collector::default());
        let id = producer
            .add_subscription(
                collector.clone(),
                WireOptions::default(),
                columns.clone(),
                viewport.clone(),
                reverse,
            )
            .unwrap();
        Self {
            collector,
            id,
            is_viewport: viewport.is_some(),
            viewport,
            reverse,
            columns,
            schema: None,
            replica: Replica::default(),
        }
    }

    /// Applies what was sent, and checks that the client now holds what the table has.
    fn catch_up(&mut self, table: &Arc<dyn Table>, context: &str) {
        let mut messages = std::mem::take(&mut *self.collector.messages.lock().unwrap());
        let ended = self.collector.ended.lock().unwrap().clone();
        assert!(ended.is_none(), "{context}: ended {ended:?}");
        if self.schema.is_none() {
            assert!(!messages.is_empty(), "{context}: no first snapshot");
            self.schema = Some(schema_of(&messages.remove(0)).expect("the schema comes first"));
        }
        let schema = self.schema.as_ref().unwrap();
        for update in updates_of(&messages, schema) {
            self.replica.apply(&update, self.is_viewport);
        }
        let row_set = table.row_set();
        let keys = match &self.viewport {
            Some(viewport) => row_set.sub_set_for_positions(viewport, self.reverse),
            None => (*row_set).clone(),
        };
        // a client that drops a column keeps what it had of it; only the columns it
        // subscribes to are compared
        let mut held = self.replica.rows(self.is_viewport);
        for row in &mut held {
            for (index, cell) in row.iter_mut().enumerate() {
                if self
                    .columns
                    .as_ref()
                    .is_some_and(|columns| !columns.contains(index))
                {
                    *cell = None;
                }
            }
        }
        assert_eq!(
            held,
            table_rows(table, &keys, self.columns.as_ref()),
            "{context}"
        );
        if !self.is_viewport {
            let held: Vec<i64> = self.replica.by_key.keys().copied().collect();
            assert_eq!(held, keys.iter().collect::<Vec<_>>(), "{context}: keys");
        }
        assert_eq!(self.replica.viewport, self.viewport, "{context}: viewport");
        assert_eq!(self.replica.reverse_viewport, self.reverse, "{context}");
    }
}

/// A small deterministic generator for the viewports of the model test.
struct Dice(u64);

impl Dice {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }

    fn viewport(&mut self) -> RowSet {
        let first = self.below(12) as i64;
        let mut rows = RowSet::from_range(first, first + self.below(6) as i64);
        if self.below(3) == 0 {
            let second = first + 8 + self.below(5) as i64;
            rows.insert(&RowSet::from_range(second, second + self.below(4) as i64));
        }
        rows
    }
}

/// Subscribers of every kind follow a table through random updates, some of them sent
/// several cycles at once, and random changes of their viewports and columns: after every
/// propagation each holds exactly the rows of the table it asked for.
#[test]
fn clients_hold_the_rows_of_the_table_after_every_propagation() {
    let mut deltas = 0;
    for seed in 0..60 {
        let stepper = SteppedUpdateGraph::new();
        let graph: Arc<dyn UpdateGraph> = stepper.clone();
        let mut random = RandomUpdates::new(seed);
        let mut dice = Dice(seed ^ 0x5eed);
        let definition = definition(&[
            ("Sym", ColumnType::String),
            ("N", ColumnType::Int),
            ("Px", ColumnType::Double),
            ("L", ColumnType::Long),
        ]);
        let initial = RowSet::from_keys([2, 3, 7, 11, 12, 20, 21, 30]);
        let columns = definition
            .columns()
            .iter()
            .map(|column| random.values(&column.column_type, initial.size()))
            .collect();
        let source = ScriptedTable::new(graph, definition, initial, columns);
        let table: Arc<dyn Table> = source.table();
        let scheduler = Arc::new(ManualScheduler::default());
        let producer = Producer::new(table.clone(), scheduler.clone(), 0);

        let mut subscribers = vec![
            Subscriber::new(&producer, None, false, None),
            Subscriber::new(&producer, Some(RowSet::from_range(1, 4)), false, None),
            Subscriber::new(&producer, Some(RowSet::from_range(0, 2)), true, None),
            Subscriber::new(
                &producer,
                Some(RowSet::from_range(0, 50)),
                false,
                Some(ColumnSet::of([1, 3])),
            ),
        ];
        scheduler.drain();
        for (index, subscriber) in subscribers.iter_mut().enumerate() {
            subscriber.catch_up(
                &table,
                &format!("seed {seed}, subscriber {index}, at first"),
            );
        }

        for cycle in 0..60 {
            source.push(random.next_change(&*table, 6));
            stepper.run_cycle();
            // now and then a client changes what it looks at
            if dice.below(4) == 0 {
                let index = 1 + dice.below(3) as usize;
                let subscriber = &mut subscribers[index];
                subscriber.viewport = Some(dice.viewport());
                if dice.below(3) == 0 {
                    subscriber.reverse = !subscriber.reverse;
                }
                if index == 3 {
                    subscriber.columns = match dice.below(3) {
                        0 => None,
                        1 => Some(ColumnSet::of([0, 2])),
                        _ => Some(ColumnSet::of([1, 2, 3])),
                    };
                }
                assert!(producer.update_subscription(
                    subscriber.id,
                    subscriber.viewport.clone(),
                    subscriber.columns.clone(),
                    subscriber.reverse,
                ));
            }
            // and now and then several cycles are sent as one message
            if dice.below(3) == 0 {
                continue;
            }
            scheduler.drain();
            for (index, subscriber) in subscribers.iter_mut().enumerate() {
                let before = subscriber.collector.messages.lock().unwrap().len();
                deltas += before;
                subscriber.catch_up(
                    &table,
                    &format!("seed {seed}, subscriber {index}, cycle {cycle}"),
                );
            }
        }
        // a removed subscription is forgotten and its stream completed
        let removed = subscribers.pop().unwrap();
        producer.remove_subscription(removed.id);
        scheduler.drain();
        assert_eq!(producer.subscription_count(), 3);
        assert!(matches!(
            *removed.collector.ended.lock().unwrap(),
            Some(Ok(()))
        ));
    }
    assert!(deltas > 1000, "the test saw few messages: {deltas}");
}

/// A table that fails ends its subscriptions with the error Java gives for any failure
/// that is not a status: the id of a log entry.
#[test]
fn a_failed_table_ends_its_subscriptions_with_an_error() {
    let stepper = SteppedUpdateGraph::new();
    let graph: Arc<dyn UpdateGraph> = stepper.clone();
    let definition = definition(&[("N", ColumnType::Int)]);
    let source = ScriptedTable::new(
        graph,
        definition,
        RowSet::flat(2),
        vec![oxhaven_testkit::ints([1, 2])],
    );
    let table: Arc<dyn Table> = source.table();
    let scheduler = Arc::new(ManualScheduler::default());
    let producer = Producer::new(table, scheduler.clone(), 0);
    let subscriber = Subscriber::new(&producer, None, false, None);
    scheduler.drain();
    source.push_failure("boom");
    stepper.run_cycle();
    scheduler.drain();
    let ended = subscriber.collector.ended.lock().unwrap().clone();
    let status = ended.expect("the stream ended").unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().starts_with("Details Logged w/ID '"),
        "{status:?}"
    );
    assert_eq!(producer.subscription_count(), 0);
}

// ---------------------------------------------------------------------------------------
// The fixtures
// ---------------------------------------------------------------------------------------

/// One step of a captured call: what the script did, what the client sent, what Java
/// sent, and how Java ended the call if it did in this step.
#[derive(Default, Debug)]
struct Step {
    name: String,
    sent: Vec<FlightData>,
    received: Vec<FlightData>,
    ended: Option<String>,
}

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/src/barrage/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|problem| panic!("{path}: {problem}"))
}

fn steps_of(case: &str) -> Vec<Step> {
    let bytes = fixture(&format!("{case}.exchange"));
    let mut rest = bytes.as_slice();
    let mut steps: Vec<Step> = Vec::new();
    while !rest.is_empty() {
        let size = u32::from_le_bytes(rest[1..5].try_into().unwrap()) as usize;
        let (kind, data) = (rest[0], &rest[5..5 + size]);
        rest = &rest[5 + size..];
        if kind == 2 {
            steps.push(Step {
                name: String::from_utf8(data.to_vec()).unwrap(),
                ..Step::default()
            });
            continue;
        }
        let step = steps.last_mut().expect("a step comes first");
        match kind {
            0 => step.sent.push(FlightData::decode(data).unwrap()),
            1 => step.received.push(FlightData::decode(data).unwrap()),
            3 => step.ended = Some(String::from_utf8(data.to_vec()).unwrap()),
            other => panic!("record kind {other}"),
        }
    }
    steps
}

/// The export a captured request names.
fn export_of(request: &FlightData) -> i32 {
    let wrapper = flatbuffers::root::<BarrageMessageWrapper>(&request.app_metadata).unwrap();
    let payload = wrapper.msg_payload().unwrap().bytes();
    let ticket = if wrapper.msg_type() == BarrageMessageType::BarrageSnapshotRequest {
        flatbuffers::root::<BarrageSnapshotRequest>(payload)
            .unwrap()
            .ticket()
    } else {
        flatbuffers::root::<BarrageSubscriptionRequest>(payload)
            .unwrap()
            .ticket()
    };
    ticket_to_export_id(ticket.unwrap().bytes(), "ticket").unwrap()
}

fn is_viewport_request(request: &FlightData) -> bool {
    let wrapper = flatbuffers::root::<BarrageMessageWrapper>(&request.app_metadata).unwrap();
    let payload = wrapper.msg_payload().unwrap().bytes();
    flatbuffers::root::<BarrageSubscriptionRequest>(payload)
        .unwrap()
        .viewport()
        .is_some()
}

/// How long this server is given to send what a step causes. Subscriptions ask for
/// updates every 100 ms.
const QUIET: Duration = Duration::from_millis(600);

/// A `DoExchange` call to a test server.
struct Call {
    requests: Option<mpsc::UnboundedSender<FlightData>>,
    responses: Streaming<FlightData>,
    ended: Option<String>,
}

impl Call {
    async fn open(server: &Server) -> Self {
        let (requests, receiver) = mpsc::unbounded_channel::<FlightData>();
        let request = server.authorized(tokio_stream::wrappers::UnboundedReceiverStream::new(
            receiver,
        ));
        let responses = server
            .flight()
            .do_exchange(request)
            .await
            .unwrap()
            .into_inner();
        Self {
            requests: Some(requests),
            responses,
            ended: None,
        }
    }

    fn send(&self, message: FlightData) {
        self.requests.as_ref().unwrap().send(message).unwrap();
    }

    /// What the server sends until it has been quiet for a while or ends the call.
    async fn collect(&mut self) -> Vec<FlightData> {
        let mut messages = Vec::new();
        while self.ended.is_none() {
            match tokio::time::timeout(QUIET, self.responses.message()).await {
                Err(_) => break,
                Ok(Ok(Some(message))) => messages.push(message),
                Ok(Ok(None)) => self.ended = Some("OK: ".to_owned()),
                Ok(Err(status)) => {
                    self.ended = Some(format!(
                        "{}: {}",
                        code_name(status.code()),
                        status.message()
                    ));
                }
            }
        }
        messages
    }

    /// Closes the client's side.
    async fn close(&mut self) -> Vec<FlightData> {
        self.requests = None;
        self.collect().await
    }
}

/// A status code as Java names it.
fn code_name(code: tonic::Code) -> String {
    let mut name = String::new();
    for letter in format!("{code:?}").chars() {
        if letter.is_uppercase() && !name.is_empty() {
            name.push('_');
        }
        name.push(letter.to_ascii_uppercase());
    }
    name
}

fn nullable_schema(columns: &[(&str, DataType)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// The table `capture.py` calls `every_type`: three rows, the second null in every column.
fn every_type() -> Arc<dyn Table> {
    fn column<T>(value: impl Fn(i64) -> T) -> Vec<Option<T>> {
        (0..3).map(|i| (i != 1).then(|| value(i))).collect()
    }
    let columns: Vec<(&str, ColumnType, ArrayRef)> = vec![
        (
            "Bo",
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(column(|i| i == 0))),
        ),
        (
            "By",
            ColumnType::Byte,
            Arc::new(Int8Array::from(column(|i| (i + 1) as i8))),
        ),
        (
            "Ch",
            ColumnType::Char,
            Arc::new(UInt16Array::from(column(|i| b'a' as u16 + i as u16))),
        ),
        (
            "Sh",
            ColumnType::Short,
            Arc::new(Int16Array::from(column(|i| (i + 1) as i16))),
        ),
        (
            "In",
            ColumnType::Int,
            Arc::new(Int32Array::from(column(|i| (i + 1) as i32))),
        ),
        (
            "Lo",
            ColumnType::Long,
            Arc::new(Int64Array::from(column(|i| i + 1))),
        ),
        (
            "Fl",
            ColumnType::Float,
            Arc::new(Float32Array::from(column(|i| i as f32 + 1.5))),
        ),
        (
            "Do",
            ColumnType::Double,
            Arc::new(Float64Array::from(column(|i| i as f64 + 1.5))),
        ),
        (
            "St",
            ColumnType::String,
            Arc::new(StringArray::from(column(|i| format!("s{i}")))),
        ),
        (
            "Ti",
            ColumnType::Instant,
            Arc::new(
                TimestampNanosecondArray::from(column(|i| 1_700_000_000_000_000_001 + i))
                    .with_timezone("UTC"),
            ),
        ),
    ];
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let definition = definition(&names);
    let schema = nullable_schema(
        &names
            .iter()
            .map(|(name, column_type)| (*name, column_type.arrow_type()))
            .collect::<Vec<_>>(),
    );
    let arrays = columns.into_iter().map(|(_, _, array)| array).collect();
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    in_memory_table(definition, &[batch]).unwrap()
}

/// The table `capture.py` calls `many_rows`.
fn many_rows() -> Arc<dyn Table> {
    let ints: ArrayRef = Arc::new(Int32Array::from((0..10_000).collect::<Vec<i32>>()));
    let batch =
        RecordBatch::try_new(nullable_schema(&[("In", DataType::Int32)]), vec![ints]).unwrap();
    in_memory_table(definition(&[("In", ColumnType::Int)]), &[batch]).unwrap()
}

fn input_schema() -> Arc<Schema> {
    nullable_schema(&[
        ("K", DataType::Int32),
        ("S", DataType::Utf8),
        ("V", DataType::Float64),
    ])
}

/// The rows `capture.py` makes with `rows(keys, changed)`.
fn input_rows(keys: &[i32], changed: bool) -> RecordBatch {
    let s: Vec<Option<String>> = keys
        .iter()
        .map(|k| match (changed, k) {
            (true, k) => Some(format!("m{k}")),
            (false, 4) => None,
            (false, k) => Some(format!("s{k}")),
        })
        .collect();
    let v: Vec<Option<f64>> = keys
        .iter()
        .map(|k| match (changed, k) {
            (true, k) => Some(10.0 * f64::from(*k)),
            (false, 4) => None,
            (false, k) => Some(f64::from(*k) + 0.5),
        })
        .collect();
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(Int32Array::from(keys.to_vec())),
            Arc::new(StringArray::from(s)),
            Arc::new(Float64Array::from(v)),
        ],
    )
    .unwrap()
}

fn input_keys(keys: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        nullable_schema(&[("K", DataType::Int32)]),
        vec![Arc::new(Int32Array::from(keys.to_vec()))],
    )
    .unwrap()
}

/// The keyed input table of `capture.py`, with the keys 1 to `rows`.
fn keyed_input_table(server: &Server, rows: i32) -> Arc<dyn Table> {
    let request = CreateInputTableRequest {
        result_id: None,
        kind: Some(InputTableKind {
            kind: Some(Kind::InMemoryKeyBacked(InMemoryKeyBacked {
                key_columns: vec!["K".to_owned()],
            })),
        }),
        definition: Some(Definition::Schema(schema_ipc_bytes(&input_schema()))),
    };
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let table = create_input_table(graph, &request, None).unwrap();
    if rows > 0 {
        let keys: Vec<i32> = (1..=rows).collect();
        table
            .input_table()
            .unwrap()
            .add(input_rows(&keys, false))
            .unwrap();
        server.state.graph.run_cycle();
    }
    table
}

/// Does to `table` what `capture.py` did in the step called `name`.
fn act(server: &Server, table: &Arc<dyn Table>, name: &str) {
    if matches!(name, "subscribe" | "viewport" | "request" | "close") {
        return;
    }
    let input = table.input_table().unwrap();
    match name {
        "add 7 8" => input.add(input_rows(&[7, 8], false)).unwrap(),
        "change 2" => input.add(input_rows(&[2], true)).unwrap(),
        "delete 1" => input.delete(input_keys(&[1])).unwrap(),
        "add 9, change 3 4, delete 5" => {
            input.add(input_rows(&[9], false)).unwrap();
            input.add(input_rows(&[3, 4], true)).unwrap();
            input.delete(input_keys(&[5])).unwrap();
        }
        "add 10, delete 2" => {
            input.add(input_rows(&[10], false)).unwrap();
            input.delete(input_keys(&[2])).unwrap();
        }
        other => panic!("a step tests.rs does not know: {other}"),
    }
    server.state.graph.run_cycle();
}

/// What is compared of a message apart from the state it leaves the client in: all of
/// its metadata but the sequence numbers, which are steps of the server's clock.
fn without_sequence(update: &Update) -> Update {
    Update {
        first_seq: 0,
        last_seq: 0,
        ..update.clone()
    }
}

/// Replays a captured call on this server against `table`, and checks after every step
/// that the client holds what Java's client held. With `exact`, every message must also
/// be the one Java sent, metadata and rows; without, only the first of each step that is
/// a snapshot.
async fn replay(case: &str, table: impl Fn(&Server) -> Arc<dyn Table>, exact: bool) {
    let server = Server::start().await;
    let table = table(&server);
    let steps = steps_of(case);
    let first = &steps[0].sent[0];
    server.export(export_of(first), table.clone());
    let is_subscription = case.starts_with("sub_");
    let mut is_viewport = is_subscription && is_viewport_request(first);
    let mut call = Call::open(&server).await;
    let (mut schema, mut java_replica, mut our_replica) =
        (None, Replica::default(), Replica::default());
    for step in &steps {
        let context = format!("{case}, step {}", step.name);
        act(&server, &table, &step.name);
        for request in &step.sent {
            if is_subscription {
                is_viewport = is_viewport_request(request);
            }
            call.send(request.clone());
        }
        let mut ours = if step.name == "close" {
            call.close().await
        } else {
            call.collect().await
        };
        let mut java = step.received.clone();
        if schema.is_none() {
            let java_schema = schema_of(&java.remove(0)).expect("Java's schema");
            assert!(!ours.is_empty(), "{context}: nothing was sent");
            assert_eq!(
                schema_of(&ours.remove(0)),
                Some(java_schema.clone()),
                "{context}"
            );
            assert_eq!(wire_schema(&table), java_schema);
            schema = Some(java_schema);
        }
        let schema = schema.as_ref().unwrap();
        let java = updates_of(&java, schema);
        let ours = updates_of(&ours, schema);
        if exact {
            let java: Vec<Update> = java.iter().map(without_sequence).collect();
            let ours: Vec<Update> = ours.iter().map(without_sequence).collect();
            assert_eq!(ours, java, "{context}");
        } else if let Some(snapshot) = java.first().filter(|update| update.is_snapshot) {
            assert_eq!(
                ours.first().map(without_sequence),
                Some(without_sequence(snapshot)),
                "{context}"
            );
        }
        if table.is_refreshing() && is_subscription && step.name != "close" {
            assert!(!ours.is_empty(), "{context}: nothing was sent");
        }
        if !table.is_refreshing() {
            // a static table has no clock
            let sequence = |updates: &[Update]| -> Vec<(i64, i64)> {
                updates.iter().map(|u| (u.first_seq, u.last_seq)).collect()
            };
            assert_eq!(sequence(&ours), sequence(&java), "{context}");
        }
        for update in &java {
            java_replica.apply(update, is_viewport);
        }
        for update in &ours {
            our_replica.apply(update, is_viewport);
        }
        assert_eq!(our_replica, java_replica, "{context}");
        if step.name == "close" {
            // Java leaves a subscription's call open when the client closes its side;
            // this server completes it
            let java_end = step.ended.as_deref();
            let expected = if is_subscription {
                "OK: "
            } else {
                java_end.unwrap()
            };
            assert!(
                !is_subscription || java_end == Some("never ended"),
                "{context}"
            );
            assert_eq!(call.ended.as_deref(), Some(expected), "{context}");
        } else {
            assert_eq!(call.ended, None, "{context}");
        }
    }
    assert!(
        !java_replica.rows(is_viewport).is_empty() || case.contains("empty"),
        "{case}: the fixture leaves the client without rows"
    );
}

#[tokio::test]
async fn subscriptions_to_a_ticking_table_leave_clients_with_what_java_leaves_them() {
    for case in ["sub_full", "sub_viewport", "sub_reverse", "sub_columns"] {
        // Java cut the changes of a step into messages by its own timing
        replay(case, |server| keyed_input_table(server, 6), false).await;
    }
}

#[tokio::test]
async fn subscriptions_to_static_tables_are_sent_what_java_sends() {
    replay("sub_static_viewport", |_| every_type(), true).await;
    replay("sub_static_full", |_| every_type(), true).await;
    // Java sizes the parts of a large snapshot by how long the last one took
    replay("sub_static_many", |_| many_rows(), false).await;
}

#[tokio::test]
async fn snapshot_requests_are_sent_what_java_sends() {
    replay("snap_static", |_| every_type(), true).await;
    replay("snap_static_nulls", |_| every_type(), true).await;
    replay("snap_static_viewport", |_| every_type(), true).await;
    replay("snap_static_reverse", |_| many_rows(), true).await;
    replay("snap_static_many", |_| many_rows(), false).await;
    for (case, rows) in [
        ("snap_ticking", 6),
        ("snap_ticking_viewport", 6),
        ("snap_ticking_reverse", 6),
        ("snap_ticking_empty", 0),
    ] {
        replay(case, |server| keyed_input_table(server, rows), true).await;
    }
}

/// Every row set Java sent is, encoded by this server, the bytes Java sent.
#[test]
fn row_sets_are_encoded_to_the_bytes_java_sends() {
    let mut seen = 0;
    let directory = format!("{}/src/barrage/fixtures", env!("CARGO_MANIFEST_DIR"));
    for entry in std::fs::read_dir(directory).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        let Some(case) = name.strip_suffix(".exchange") else {
            continue;
        };
        for message in steps_of(case).iter().flat_map(|step| &step.received) {
            if message.app_metadata.is_empty() {
                continue;
            }
            let metadata = metadata_of(message).unwrap();
            for bytes in row_set_bytes(&metadata) {
                let rows = decode_row_set(&bytes).unwrap();
                assert_eq!(encode_row_set(&rows), bytes, "{case}: {rows:?}");
                seen += 1;
            }
            let parsed = flatbuffers::root::<BarrageUpdateMetadata>(&metadata).unwrap();
            if let Some(bytes) = parsed.shift_data() {
                let shifted = decode_shift_data(bytes.bytes()).unwrap();
                assert_eq!(encode_shift_data(&shifted), bytes.bytes(), "{case}");
                seen += 1;
            }
        }
    }
    assert!(seen > 100, "few row sets in the fixtures: {seen}");
}

/// The metadata Java attaches to the batches of a `DoGet`, which the Flight fixtures hold.
#[test]
fn do_get_carries_the_metadata_java_attaches() {
    use crate::flight::tests::support::fixture_messages;
    for (case, table) in [
        ("engine_nulls", every_type()),
        ("engine_many_rows", many_rows()),
    ] {
        let java = fixture_messages(&format!("{case}.doget"));
        let ours = do_get_messages(&table).unwrap();
        let schema = schema_of(&java[0]).unwrap();
        let java = updates_of(&java[1..], &schema);
        let ours = updates_of(&ours[1..], &schema);
        assert!(!java.is_empty());
        assert_eq!(ours, java, "{case}");
    }
}

// ---------------------------------------------------------------------------------------
// Malformed calls
// ---------------------------------------------------------------------------------------

fn signed(bytes: &[u8]) -> Vec<i8> {
    bytes.iter().map(|byte| *byte as i8).collect()
}

fn wrapped(msg_type: BarrageMessageType, payload: Option<&[u8]>, magic: u32) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let msg_payload = payload.map(|payload| builder.create_vector(&signed(payload)));
    let wrapper = BarrageMessageWrapper::create(
        &mut builder,
        &BarrageMessageWrapperArgs {
            magic,
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

fn subscription(ticket: Option<&[u8]>, viewport: Option<&RowSet>) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let ticket = ticket.map(|ticket| builder.create_vector(&signed(ticket)));
    let viewport = viewport.map(|rows| builder.create_vector(&signed(&encode_row_set(rows))));
    let request = BarrageSubscriptionRequest::create(
        &mut builder,
        &BarrageSubscriptionRequestArgs {
            ticket,
            viewport,
            ..Default::default()
        },
    );
    builder.finish(request, None);
    wrapped(
        BarrageMessageType::BarrageSubscriptionRequest,
        Some(builder.finished_data()),
        MAGIC,
    )
}

fn snapshot_request(ticket: &[u8]) -> FlightData {
    let mut builder = FlatBufferBuilder::new();
    let ticket = Some(builder.create_vector(&signed(ticket)));
    let request = BarrageSnapshotRequest::create(
        &mut builder,
        &BarrageSnapshotRequestArgs {
            ticket,
            ..Default::default()
        },
    );
    builder.finish(request, None);
    wrapped(
        BarrageMessageType::BarrageSnapshotRequest,
        Some(builder.finished_data()),
        MAGIC,
    )
}

/// How a call ends that sends `requests`, in the words of `statuses.txt`.
async fn ending(server: &Server, requests: Vec<FlightData>) -> String {
    let mut call = Call::open(server).await;
    for request in requests {
        call.send(request);
        call.collect().await;
    }
    let ended = match call.ended.clone() {
        Some(ended) => ended,
        None => {
            call.close().await;
            format!(
                "open until the client closes, then {}",
                call.ended.as_deref().unwrap_or("never ended")
            )
        }
    };
    // the two things Java words with an identity: the marshaller and the log entry
    let mut normal = String::new();
    let mut rest = ended.as_str();
    for (start, end) in [
        ("DoExchangeMarshaller{", "}"),
        ("Details Logged w/ID '", "'"),
    ] {
        if let Some(at) = rest.find(start) {
            let after = at + start.len();
            let close = after + rest[after..].find(end).unwrap();
            normal.push_str(&rest[..after]);
            normal.push('#');
            rest = &rest[close..];
        }
    }
    normal + rest
}

#[tokio::test]
async fn malformed_calls_end_as_they_do_in_java() {
    let server = Server::start().await;
    server.export(1, every_type());
    let table = crate::session::export_id_to_bytes(1);
    let missing = crate::session::export_id_to_bytes(999_999);
    let whole = || subscription(Some(&table), None);
    let viewport = || subscription(Some(&table), Some(&RowSet::flat(1)));
    let nothing = FlightData::default;
    let sub = BarrageMessageType::BarrageSubscriptionRequest;
    let cases: Vec<(&str, Vec<FlightData>)> = vec![
        (
            "a message type that is not a request",
            vec![wrapped(
                BarrageMessageType::BarrageSerializationOptions,
                Some(&[]),
                MAGIC,
            )],
        ),
        (
            "no metadata, then a subscription",
            vec![nothing(), viewport()],
        ),
        ("no metadata twice", vec![nothing(), nothing()]),
        (
            "metadata with another magic number",
            vec![wrapped(sub, Some(&[]), 1), nothing()],
        ),
        (
            "a subscription without a payload",
            vec![wrapped(sub, None, MAGIC)],
        ),
        (
            "a subscription without a ticket",
            vec![subscription(None, None)],
        ),
        (
            "a subscription to an export that does not exist",
            vec![subscription(Some(&missing), None)],
        ),
        (
            "a subscription with a ticket of no known kind",
            vec![subscription(Some(b"?abc"), None)],
        ),
        (
            "a subscription, then a snapshot request",
            vec![whole(), snapshot_request(&table)],
        ),
        ("a subscription, then no metadata", vec![whole(), nothing()]),
        (
            "a subscription to the whole table, then a viewport",
            vec![whole(), viewport()],
        ),
        (
            "a viewport, then the whole table",
            vec![viewport(), whole()],
        ),
        (
            "a snapshot, then a subscription",
            vec![snapshot_request(&table), whole()],
        ),
        (
            "a snapshot of an export that does not exist",
            vec![snapshot_request(&missing)],
        ),
        (
            "two snapshots",
            vec![snapshot_request(&table), snapshot_request(&table)],
        ),
    ];
    let java: BTreeMap<String, String> = String::from_utf8(fixture("statuses.txt"))
        .unwrap()
        .lines()
        .map(|line| {
            let (what, ended) = line.split_once(": ").unwrap();
            (what.to_owned(), ended.to_owned())
        })
        .collect();
    assert_eq!(
        java.len(),
        cases.len(),
        "a case of statuses.txt is not replayed"
    );
    for (what, requests) in cases {
        let ours = ending(&server, requests).await;
        let expected = &java[what];
        // Java never completes a subscription's call after the client closed its side;
        // this server does
        let expected = match (what, expected.as_str()) {
            (
                "no metadata, then a subscription" | "a viewport, then the whole table",
                "open until the client closes, then never ended",
            ) => "open until the client closes, then OK: ",
            (_, expected) => expected,
        };
        assert_eq!(ours, expected, "{what}");
    }
}
