//! Change data capture from a scripted record source, through a schema registry that the
//! test serves, on a graph the test steps.
//!
//! The expected rows, texts and requests are the reference's, from the probes recorded in
//! `tasks/7.5.3-kafka-cdc.md` (the case names are in the comments).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::Int32Type;
use bytes::Bytes;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{consume_cdc_from, CdcOptions, CdcSpec, KafkaError, Record, RecordSource};
use oxhaven_testkit::ConsistencyChecker;

/// A schema registry that answers each path with what the test gave it, and 404 with
/// Confluent's error body otherwise, and keeps the paths it was asked for.
struct Registry {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Registry {
    fn serve(answers: Vec<(String, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let answers: HashMap<String, String> = answers.into_iter().collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut request = Vec::new();
                let mut buffer = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buffer[..n]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split(' ').nth(1).unwrap_or("").to_owned();
                seen.lock().unwrap().push(path.clone());
                let (status, body) = match answers.get(&path) {
                    Some(body) => (200, body.clone()),
                    None => (
                        404,
                        r#"{"error_code":40401,"message":"Subject not found."}"#.to_owned(),
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self { url, requests }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn quoted(schema: &str) -> String {
    let schema = schema.replace('\\', "\\\\").replace('"', "\\\"");
    schema.replace('\n', "\\n")
}

/// The answers for `subject` holding `schema` under `id`: its latest version, the schema
/// by id as `CdcTools` fetches it, and as the deserializer of `topic`'s records does.
fn subject(subject: &str, id: i32, schema: &str) -> Vec<(String, String)> {
    let schema = quoted(schema);
    vec![
        (
            format!("/subjects/{subject}/versions/latest"),
            format!(r#"{{"subject":"{subject}","version":1,"id":{id},"schema":"{schema}"}}"#),
        ),
        (
            format!("/schemas/ids/{id}?fetchMaxId=false&subject="),
            format!(r#"{{"schema":"{schema}"}}"#),
        ),
        (
            format!("/schemas/ids/{id}?fetchMaxId=false&subject={subject}"),
            format!(r#"{{"schema":"{schema}"}}"#),
        ),
    ]
}

/// The key of the Postgres connector of Debezium for a table whose primary key is `id`.
const KEY: &str = r#"{"type":"record","name":"Key","namespace":"db.inv.people","fields":[
    {"name":"id","type":{"type":"int","connect.default":0},"default":0}],
    "connect.name":"db.inv.people.Key"}"#;

/// Its value: the envelope of a row of `id` and `name`, with Debezium's `source` (cut
/// down), `transaction` and timestamps.
const VALUE: &str = r#"{"type":"record","name":"Envelope","namespace":"db.inv.people","fields":[
    {"name":"before","type":["null",{"type":"record","name":"Value","fields":[
        {"name":"id","type":{"type":"int","connect.default":0},"default":0},
        {"name":"name","type":["null","string"],"default":null}],
        "connect.name":"db.inv.people.Value"}],"default":null},
    {"name":"after","type":["null","Value"],"default":null},
    {"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql",
        "fields":[{"name":"version","type":"string"},{"name":"ts_ms","type":"long"},
        {"name":"snapshot","type":[{"type":"string","connect.version":1,
            "connect.parameters":{"allowed":"true,last,false,incremental"},
            "connect.default":"false","connect.name":"io.debezium.data.Enum"},"null"],
            "default":"false"},
        {"name":"lsn","type":["null","long"],"default":null}]}},
    {"name":"transaction","type":["null",{"type":"record","name":"block","namespace":"event",
        "fields":[{"name":"id","type":"string"},{"name":"total_order","type":"long"},
        {"name":"data_collection_order","type":"long"}]}],"default":null},
    {"name":"op","type":"string"},
    {"name":"ts_ms","type":["null","long"],"default":null}],
    "connect.name":"db.inv.people.Envelope"}"#;

fn zigzag(value: i64, out: &mut Vec<u8>) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn string(text: &str, out: &mut Vec<u8>) {
    zigzag(text.len() as i64, out);
    out.extend_from_slice(text.as_bytes());
}

fn framed(id: i32, body: Vec<u8>) -> Vec<u8> {
    let mut out = vec![0];
    out.extend_from_slice(&id.to_be_bytes());
    out.extend(body);
    out
}

fn key(id: i64) -> Vec<u8> {
    let mut out = Vec::new();
    zigzag(id, &mut out);
    framed(1, out)
}

/// A row of the table, or `None` for a missing `before` or `after`.
fn row(row: Option<(i64, Option<&str>)>, out: &mut Vec<u8>) {
    let Some((id, name)) = row else {
        zigzag(0, out);
        return;
    };
    zigzag(1, out);
    zigzag(id, out);
    match name {
        None => zigzag(0, out),
        Some(name) => {
            zigzag(1, out);
            string(name, out);
        }
    }
}

/// A change event.
fn event(
    op: &str,
    before: Option<(i64, Option<&str>)>,
    after: Option<(i64, Option<&str>)>,
) -> Vec<u8> {
    let mut out = Vec::new();
    row(before, &mut out);
    row(after, &mut out);
    string("2.7.0.Final", &mut out);
    zigzag(1_700_000_000_000, &mut out);
    zigzag(0, &mut out);
    string("false", &mut out);
    zigzag(0, &mut out);
    zigzag(0, &mut out);
    string(op, &mut out);
    zigzag(1, &mut out);
    zigzag(1_700_000_000_100, &mut out);
    framed(2, out)
}

fn record(offset: i64, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Record {
    Record {
        partition: 0,
        offset,
        timestamp: 1_700_000_000_000,
        key: key.map(Bytes::from),
        value: value.map(Bytes::from),
    }
}

/// The events of the main probe: two rows read in the snapshot, a row created, the first
/// updated, the second deleted and its tombstone, a row created and deleted without a
/// tombstone.
fn main_events() -> Vec<Record> {
    vec![
        record(
            0,
            Some(key(1001)),
            Some(event("r", None, Some((1001, Some("Sally"))))),
        ),
        record(
            1,
            Some(key(1002)),
            Some(event("r", None, Some((1002, Some("George"))))),
        ),
        record(
            2,
            Some(key(1003)),
            Some(event("c", None, Some((1003, None)))),
        ),
        record(
            3,
            Some(key(1001)),
            Some(event(
                "u",
                Some((1001, Some("Sally"))),
                Some((1001, Some("Sal"))),
            )),
        ),
        record(
            4,
            Some(key(1002)),
            Some(event("d", Some((1002, Some("George"))), None)),
        ),
        record(5, Some(key(1002)), None),
        record(
            6,
            Some(key(1004)),
            Some(event("c", None, Some((1004, Some("Anne"))))),
        ),
        record(
            7,
            Some(key(1004)),
            Some(event("d", Some((1004, Some("Anne"))), None)),
        ),
    ]
}

struct Scripted {
    script: Receiver<Vec<Record>>,
    polled: Sender<()>,
}

impl RecordSource for Scripted {
    fn poll(&mut self) -> Result<Vec<Record>, KafkaError> {
        let _ = self.polled.send(());
        Ok(self
            .script
            .recv_timeout(Duration::from_millis(20))
            .unwrap_or_default())
    }
}

struct Stream {
    graph: Arc<SteppedUpdateGraph>,
    table: Arc<dyn Table>,
    checker: Arc<ConsistencyChecker>,
    script: Sender<Vec<Record>>,
    polled: Receiver<()>,
}

fn registry() -> Registry {
    let mut answers = subject("db.inv.people-key", 1, KEY);
    answers.extend(subject("db.inv.people-value", 2, VALUE));
    Registry::serve(answers)
}

fn options(registry: &Registry, spec: CdcSpec, blink: bool) -> CdcOptions {
    CdcOptions {
        config: vec![("schema.registry.url".to_owned(), registry.url.clone())],
        spec,
        partitions: None,
        blink,
        drop_columns: Vec::new(),
    }
}

fn consume(options: &CdcOptions) -> Result<Stream, KafkaError> {
    let graph = SteppedUpdateGraph::new();
    let (script, script_end) = channel();
    let (polled_end, polled) = channel();
    let source = Scripted {
        script: script_end,
        polled: polled_end,
    };
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let table = consume_cdc_from(shared, options, Box::new(source))?;
    let checker = ConsistencyChecker::attach(table.clone());
    Ok(Stream {
        graph,
        table,
        checker,
        script,
        polled,
    })
}

fn error(options: &CdcOptions) -> String {
    match consume(options) {
        Ok(_) => panic!("the call succeeds"),
        Err(error) => error.to_string(),
    }
}

impl Stream {
    fn cycle(&self, records: Vec<Record>) {
        self.script.send(records).unwrap();
        while self.polled.try_recv().is_ok() {}
        let _ = self.polled.recv_timeout(Duration::from_secs(10));
        let _ = self.polled.recv_timeout(Duration::from_secs(10));
        self.graph.run_cycle();
        self.checker.assert_consistent();
    }

    fn columns(&self) -> Vec<(String, ColumnType)> {
        let columns = self.table.definition().columns().iter();
        columns
            .map(|c| (c.name.clone(), c.column_type.clone()))
            .collect()
    }

    fn column(&self, name: &str) -> ArrayRef {
        let index = self.table.definition().index_of(name).unwrap();
        self.table
            .column(index)
            .chunk(&self.table.row_set())
            .unwrap()
    }

    fn ints(&self, name: &str) -> Vec<Option<i32>> {
        let column = self.column(name);
        column.as_primitive::<Int32Type>().iter().collect()
    }

    fn strings(&self, name: &str) -> Vec<Option<String>> {
        let column = self.column(name);
        let cells = column.as_string::<i32>().iter();
        cells.map(|cell| cell.map(str::to_owned)).collect()
    }
}

fn some(texts: &[Option<&str>]) -> Vec<Option<String>> {
    texts.iter().map(|t| t.map(str::to_owned)).collect()
}

/// Probe main: the last event of each key, without the keys whose last event is a delete;
/// a tombstone after a delete shows its key again with null columns.
#[test]
fn the_current_rows_are_the_last_event_of_each_key_that_is_not_a_delete() {
    let registry = registry();
    let stream = consume(&options(
        &registry,
        CdcSpec::short("db", "inv", "people"),
        false,
    ))
    .unwrap();
    assert_eq!(
        stream.columns(),
        [
            ("id".to_owned(), ColumnType::Int),
            ("name".to_owned(), ColumnType::String)
        ]
    );
    assert!(!stream.table.attributes().blink);
    assert!(stream.table.is_refreshing());
    // The value's schema, then the key's.
    assert_eq!(
        registry.requests(),
        [
            "/subjects/db.inv.people-value/versions/latest",
            "/schemas/ids/2?fetchMaxId=false&subject=",
            "/subjects/db.inv.people-key/versions/latest",
            "/schemas/ids/1?fetchMaxId=false&subject=",
        ]
    );

    let events = main_events();
    stream.cycle(events[..4].to_vec());
    assert_eq!(stream.ints("id"), [Some(1001), Some(1002), Some(1003)]);
    assert_eq!(
        stream.strings("name"),
        some(&[Some("Sal"), Some("George"), None])
    );
    stream.cycle(events[4..5].to_vec());
    assert_eq!(stream.ints("id"), [Some(1001), Some(1003)]);
    stream.cycle(events[5..].to_vec());
    assert_eq!(stream.ints("id"), [Some(1001), Some(1002), Some(1003)]);
    assert_eq!(stream.strings("name"), some(&[Some("Sal"), None, None]));
    stream.cycle(Vec::new());
    assert_eq!(stream.ints("id").len(), 3);
}

/// Probe main_blink: the events, with the key's columns, the `after` columns and `op`; a
/// tombstone is a row of the key with a null `op`.
#[test]
fn a_blink_table_shows_the_events_with_their_op() {
    let registry = registry();
    let stream = consume(&options(
        &registry,
        CdcSpec::short("db", "inv", "people"),
        true,
    ))
    .unwrap();
    assert_eq!(
        stream.columns(),
        [
            ("id".to_owned(), ColumnType::Int),
            ("name".to_owned(), ColumnType::String),
            ("op".to_owned(), ColumnType::String),
        ]
    );
    assert!(stream.table.attributes().blink);
    stream.cycle(main_events()[3..6].to_vec());
    assert_eq!(stream.ints("id"), [Some(1001), Some(1002), Some(1002)]);
    assert_eq!(stream.strings("name"), some(&[Some("Sal"), None, None]));
    assert_eq!(stream.strings("op"), some(&[Some("u"), Some("d"), None]));
    stream.cycle(Vec::new());
    assert_eq!(stream.ints("id"), []);
}

/// Probe drop_op and jdrop_email: the columns given are left out, after `op`'s filter.
#[test]
fn dropped_columns_are_left_out() {
    let registry = registry();
    let mut options = options(&registry, CdcSpec::short("db", "inv", "people"), false);
    options.drop_columns = vec!["name".to_owned(), "name".to_owned()];
    let stream = consume(&options).unwrap();
    assert_eq!(stream.columns(), [("id".to_owned(), ColumnType::Int)]);
    stream.cycle(main_events());
    assert_eq!(stream.ints("id"), [Some(1001), Some(1002), Some(1003)]);

    // Probe jdrop_bad.
    options.drop_columns = vec!["nope".to_owned()];
    assert_eq!(
        error(&options),
        "Unknown column names [nope], available column names are [id, name, op]"
    );
}

/// Probes long_v1, long_v2, missing, long_sub_none, long_topic_none and same_subject.
#[test]
fn the_specs_fetch_their_subjects_as_java_does() {
    let registry = registry();
    let long = |key: Option<&str>, value: Option<&str>, version: &str| CdcSpec {
        topic: Some("db.inv.people".to_owned()),
        key_subject: key.map(str::to_owned),
        key_version: Some(version.to_owned()),
        value_subject: value.map(str::to_owned),
        value_version: Some("latest".to_owned()),
    };
    let key = Some("db.inv.people-key");
    let value = Some("db.inv.people-value");
    let unchecked =
        |text: &str| format!("io.deephaven.UncheckedDeephavenException: {text} (caused by {text})");
    assert_eq!(
        error(&options(&registry, long(key, value, "2"), false)),
        unchecked(
            "io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: \
             Subject not found.; error code: 40401"
        )
    );
    assert_eq!(
        registry.requests()[2..],
        ["/subjects/db.inv.people-key/versions/2?deleted=false"]
    );
    assert_eq!(
        error(&options(&registry, long(key, None, "latest"), false)),
        "java.lang.NullPointerException"
    );
    assert_eq!(
        error(&options(&registry, long(None, value, "latest"), false)),
        "java.lang.NullPointerException"
    );
    let mut no_topic = long(key, value, "latest");
    no_topic.topic = None;
    assert!(error(&options(&registry, no_topic, false)).starts_with(
        "not ported yet: org.apache.kafka.clients.consumer.KafkaConsumer#partitionsFor"
    ));

    // One client: the latest version of a subject and a schema by id are fetched once,
    // a version by number each time.
    let before = registry.requests().len();
    let same = long(key, key, "latest");
    assert_eq!(
        error(&options(&registry, same, false)),
        "java.lang.IllegalArgumentException: Supplied ColumnDefinitions include duplicate \
         names [id]"
    );
    assert_eq!(
        registry.requests()[before..],
        [
            "/subjects/db.inv.people-key/versions/latest",
            "/schemas/ids/1?fetchMaxId=false&subject=",
        ]
    );
    // Every name that comes again is listed.
    assert_eq!(
        error(&options(&registry, long(value, value, "latest"), false)),
        "java.lang.IllegalArgumentException: Supplied ColumnDefinitions include duplicate \
         names [before__id, before__name, after__id, after__name, source__version, \
         source__ts_ms, source__snapshot, source__lsn, transaction__id, \
         transaction__total_order, transaction__data_collection_order, op, ts_ms]"
    );
}

/// Probes noop and keymiss: a value without `op`, and a key whose field the `after` record
/// does not have.
#[test]
fn an_envelope_without_op_or_the_keys_field_fails_with_javas_text() {
    let no_op = VALUE.replace(r#"{"name":"op","type":"string"},"#, "");
    let other_key = KEY.replace(r#""name":"id""#, r#""name":"kid""#);
    let mut answers = subject("a-key", 1, KEY);
    answers.extend(subject("a-value", 2, &no_op));
    answers.extend(subject("b-key", 3, &other_key));
    answers.extend(subject("b-value", 4, VALUE));
    let decimal = VALUE.replace(
        r#"{"name":"name","type":["null","string"],"default":null}"#,
        r#"{"name":"name","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}],"default":null}"#,
    );
    answers.extend(subject("c-key", 5, KEY));
    answers.extend(subject("c-value", 6, &decimal));
    let registry = Registry::serve(answers);
    let spec = |name: &str| CdcSpec {
        topic: Some(name.to_owned()),
        key_subject: Some(format!("{name}-key")),
        key_version: Some("latest".to_owned()),
        value_subject: Some(format!("{name}-value")),
        value_version: Some("latest".to_owned()),
    };
    assert_eq!(
        error(&options(&registry, spec("a"), false)),
        "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [op], \
         available column names are [KafkaPartition, KafkaOffset, KafkaTimestamp, id, \
         before__id, before__name, after__id, after__name, source__version, source__ts_ms, \
         source__snapshot, source__lsn, transaction__id, transaction__total_order, \
         transaction__data_collection_order, ts_ms, name]"
    );
    assert_eq!(
        error(&options(&registry, spec("b"), false)),
        "io.deephaven.engine.table.impl.NoSuchColumnException: aggregation: not all group-by \
         columns [kid] are present in input table with columns [id, name, op]. Missing \
         columns: [kid]"
    );
    // A Debezium decimal, among the current rows and among the events.
    for stream_table in [false, true] {
        let table = consume(&options(&registry, spec("c"), stream_table)).unwrap();
        assert_eq!(
            table.columns()[1],
            ("name".to_owned(), ColumnType::BigDecimal)
        );
    }
    // A blink table does not group by the key (probe keymiss_blink).
    let stream = consume(&options(&registry, spec("b"), true)).unwrap();
    assert_eq!(
        stream.columns(),
        [
            ("id".to_owned(), ColumnType::Int),
            ("name".to_owned(), ColumnType::String),
            ("op".to_owned(), ColumnType::String),
        ]
    );
}

/// A change event without `before`, whose `name` is a decimal of scale 2 (Debezium's
/// `connect.decimal`), written as Avro writes a `bytes` decimal: the unscaled value's two's
/// complement bytes.
fn decimal_event(op: &str, after: Option<(i64, Option<&[u8]>)>) -> Vec<u8> {
    let mut out = Vec::new();
    zigzag(0, &mut out);
    match after {
        None => zigzag(0, &mut out),
        Some((id, name)) => {
            zigzag(1, &mut out);
            zigzag(id, &mut out);
            match name {
                None => zigzag(0, &mut out),
                Some(unscaled) => {
                    zigzag(1, &mut out);
                    zigzag(unscaled.len() as i64, &mut out);
                    out.extend_from_slice(unscaled);
                }
            }
        }
    }
    string("2.7.0.Final", &mut out);
    zigzag(1_700_000_000_000, &mut out);
    zigzag(0, &mut out);
    string("false", &mut out);
    zigzag(0, &mut out);
    zigzag(0, &mut out);
    string(op, &mut out);
    zigzag(1, &mut out);
    zigzag(1_700_000_000_100, &mut out);
    framed(2, out)
}

/// The current rows keep a decimal column: each key shows the decimal of its last event,
/// with the schema's scale, as Java's `last_by` of the events shows the row's `BigDecimal`.
#[test]
fn the_current_rows_keep_a_decimal_column() {
    let decimal = VALUE.replace(
        r#"{"name":"name","type":["null","string"],"default":null}"#,
        r#"{"name":"name","type":["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}],"default":null}"#,
    );
    let mut answers = subject("db.inv.people-key", 1, KEY);
    answers.extend(subject("db.inv.people-value", 2, &decimal));
    let registry = Registry::serve(answers);
    let stream = consume(&options(
        &registry,
        CdcSpec::short("db", "inv", "people"),
        false,
    ))
    .unwrap();
    assert_eq!(
        stream.columns()[1],
        ("name".to_owned(), ColumnType::BigDecimal)
    );
    // 12.34 and then 5.00 for 1001, null for 1002, and 1003 deleted.
    let event = |op, id, name: Option<&[u8]>| decimal_event(op, Some((id, name)));
    stream.cycle(vec![
        record(
            0,
            Some(key(1001)),
            Some(event("r", 1001, Some(&[0x04, 0xd2]))),
        ),
        record(1, Some(key(1002)), Some(event("c", 1002, None))),
        record(2, Some(key(1003)), Some(event("c", 1003, Some(&[0x01])))),
    ]);
    stream.cycle(vec![
        record(
            3,
            Some(key(1001)),
            Some(event("u", 1001, Some(&[0x01, 0xf4]))),
        ),
        record(4, Some(key(1003)), Some(decimal_event("d", None))),
    ]);
    assert_eq!(stream.ints("id"), [Some(1001), Some(1002)]);
    let names = stream.column("name");
    let names = names.as_binary::<i32>();
    let (scale, unscaled) = oxhaven_column::java_values::big_decimal_parts(names.value(0)).unwrap();
    assert_eq!(
        oxhaven_column::java_values::big_decimal_text(scale, unscaled),
        "5.00"
    );
    assert!(names.is_null(1));
}
