//! Avro records and the topic, partition and timestamp columns, published to a scripted
//! record sink with a schema registry the test serves. The bytes, requests and texts
//! expected here are those the Java reference gave for the same tables in the probes of
//! `tasks/7.5.2-kafka-produce-avro.md` (named in each test).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray,
};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{
    produce_to, AvroProduceSpec, KafkaError, OutRecord, ProduceOptions, ProduceSpec, Publication,
    RecordSink,
};
use oxhaven_testkit::{ReferenceTable, ScriptedTable, TableChange};

/// A schema registry that answers `METHOD path` with what the test gave it, a POST to
/// `/subjects/.../versions` with id 7, and 404 otherwise; it keeps each request with its
/// body.
struct Registry {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl Registry {
    fn serve(answers: &[(&str, &str)]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let answers: HashMap<String, String> = answers
            .iter()
            .map(|(request, body)| (request.to_string(), body.to_string()))
            .collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                        let length = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .map_or(0, |n| n.trim().parse::<usize>().unwrap());
                        if request.len() >= end + 4 + length {
                            break;
                        }
                    }
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buffer[..n]),
                    }
                }
                let text = String::from_utf8_lossy(&request).to_string();
                let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
                let mut words = head.split(' ');
                let key = format!(
                    "{} {}",
                    words.next().unwrap_or(""),
                    words.next().unwrap_or("")
                );
                seen.lock()
                    .unwrap()
                    .push(format!("{key} {body}").trim_end().to_owned());
                let (status, answer) = match answers.get(&key) {
                    Some(answer) => (200, answer.clone()),
                    None if key.starts_with("POST /subjects/") => (200, r#"{"id":7}"#.to_owned()),
                    None => (
                        404,
                        r#"{"error_code":40401,"message":"Subject 'x' not found."}"#.to_owned(),
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{answer}",
                    answer.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self { url, requests }
    }

    /// Serves `schema` as version 1 of `subject`, with id 3.
    fn with_subject(subject: &str, schema: &str) -> Self {
        let quoted = serde_quote(schema);
        Self::serve(&[
            (
                &format!("GET /subjects/{subject}/versions/latest"),
                &format!(r#"{{"subject":"{subject}","version":1,"id":3,"schema":{quoted}}}"#),
            ),
            (
                "GET /schemas/ids/3?fetchMaxId=false&subject=",
                &format!(r#"{{"schema":{quoted}}}"#),
            ),
        ])
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

fn serde_quote(text: &str) -> String {
    let mut out = String::from("\"");
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[derive(Clone, Default)]
struct Sent {
    records: Arc<Mutex<Vec<OutRecord>>>,
}

struct Sink {
    sent: Sent,
}

impl RecordSink for Sink {
    fn send(&mut self, record: OutRecord) {
        self.sent.records.lock().unwrap().push(record);
    }

    fn flush(&mut self) -> Result<(), KafkaError> {
        Ok(())
    }
}

impl Sent {
    fn publish(
        &self,
        table: Arc<dyn Table>,
        options: &ProduceOptions,
    ) -> Result<Publication, KafkaError> {
        let sent = self.clone();
        produce_to(table, options, &move || {
            Ok(Box::new(Sink { sent: sent.clone() }))
        })
    }

    fn take(&self) -> Vec<OutRecord> {
        std::mem::take(&mut *self.records.lock().unwrap())
    }

    /// The values sent, in hexadecimal.
    fn values(&self) -> Vec<Option<String>> {
        self.take().into_iter().map(|r| r.value.map(hex)).collect()
    }
}

fn hex(bytes: Vec<u8>) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    let columns = columns
        .iter()
        .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
        .collect();
    TableDefinition::new(columns).unwrap()
}

const T1_COLUMNS: [(&str, ColumnType); 7] = [
    ("I", ColumnType::Int),
    ("L", ColumnType::Long),
    ("D", ColumnType::Double),
    ("F", ColumnType::Float),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
    ("T", ColumnType::Instant),
];

/// The probes' table `t1`.
fn t1_columns() -> Vec<ArrayRef> {
    vec![
        Arc::new(Int32Array::from(vec![Some(1), None, Some(-7)])),
        Arc::new(Int64Array::from(vec![Some(5), Some(6), None])),
        Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-0.0)])),
        Arc::new(Float32Array::from(vec![Some(2.5), Some(3.0), None])),
        Arc::new(StringArray::from(vec![Some("a"), None, Some("é")])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(1_704_164_645_123_456_000),
                None,
                Some(1_000_000),
            ])
            .with_timezone("UTC"),
        ),
    ]
}

fn t1() -> Arc<dyn Table> {
    ReferenceTable::new_static(definition(&T1_COLUMNS), RowSet::flat(3), t1_columns())
}

const NULLABLE: &str = r#"{"type":"record","name":"RN","fields":[{"name":"I","type":["null","int"]},{"name":"L","type":["null","long"]},{"name":"D","type":["null","double"]},{"name":"F","type":["null","float"]},{"name":"S","type":["null","string"]},{"name":"B","type":["null","boolean"]},{"name":"T","type":["null",{"type":"long","logicalType":"timestamp-micros"}]}]}"#;
const FULL: &str = r#"{"type":"record","name":"R","namespace":"p.q","fields":[{"name":"I","type":"int"},{"name":"L","type":"long"},{"name":"D","type":"double"},{"name":"F","type":"float"},{"name":"S","type":"string"},{"name":"B","type":"boolean"},{"name":"T","type":{"type":"long","logicalType":"timestamp-micros"}}]}"#;

fn options(registry: &Registry, key: ProduceSpec, value: ProduceSpec) -> ProduceOptions {
    ProduceOptions {
        config: vec![
            ("bootstrap.servers".to_owned(), "unused".to_owned()),
            ("schema.registry.url".to_owned(), registry.url.clone()),
        ],
        topic: "t".to_owned(),
        partition: None,
        topic_column: None,
        partition_column: None,
        timestamp_column: None,
        key,
        value,
        last_by: false,
        publish_initial: true,
    }
}

fn avro(subject: &str) -> AvroProduceSpec {
    AvroProduceSpec {
        subject: subject.to_owned(),
        version: Some("latest".to_owned()),
        ..AvroProduceSpec::default()
    }
}

fn register_request(subject: &str, schema: &str) -> String {
    format!(
        "POST /subjects/{subject}/versions?normalize=false {{\"schemaType\":\"AVRO\",\"schema\":{}}}",
        serde_quote(schema)
    )
}

/// Probe a5n: every field a union with null, the schema registered under `t-value`, and
/// each record the magic byte, the id, then the values.
#[test]
fn rows_are_avro_records_in_confluent_s_wire_format() {
    let registry = Registry::with_subject("s_nullable", NULLABLE);
    let sent = Sent::default();
    sent.publish(
        t1(),
        &options(
            &registry,
            ProduceSpec::Ignore,
            ProduceSpec::Avro(avro("s_nullable")),
        ),
    )
    .unwrap();
    assert_eq!(
        sent.values(),
        [
            Some(
                "00000000070202020a02000000000000f83f020000204002026102010280d6fd84b8fb8606"
                    .to_owned()
            ),
            Some("000000000700020c00020000404000020000".to_owned()),
            Some("0000000007020d00020000000000000080000204c3a90002d00f".to_owned()),
        ]
    );
    assert_eq!(
        registry.requests(),
        [
            "GET /subjects/s_nullable/versions/latest".to_owned(),
            "GET /schemas/ids/3?fetchMaxId=false&subject=".to_owned(),
            register_request("t-value", NULLABLE),
        ]
    );
}

/// Probe a3: a null in a field that is not a union fails at that record, after the
/// records before it were sent, with the cause chain of Java's serializer.
#[test]
fn a_null_in_a_field_without_null_fails_as_java_s_writer_fails() {
    let registry = Registry::with_subject("s_full", FULL);
    let sent = Sent::default();
    let error = sent
        .publish(
            t1(),
            &options(
                &registry,
                ProduceSpec::Ignore,
                ProduceSpec::Avro(avro("s_full")),
            ),
        )
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        "org.apache.kafka.common.errors.SerializationException: Error serializing Avro message \
         (caused by java.lang.NullPointerException: null value for (non-nullable) int at R.I \
         (caused by java.lang.NullPointerException: Cannot invoke \"java.lang.Number.intValue()\" \
         because \"datum\" is null))"
    );
    assert_eq!(
        sent.values(),
        [Some(
            "0000000007020a000000000000f83f0000204002610180d6fd84b8fb8606".to_owned()
        )]
    );
}

/// Probes a7 and a6: the schema made from the columns, registered under the spec's name
/// then under the topic's subject; a Boolean column cannot be written into it. Fields are
/// paired with the columns left by position.
#[test]
fn a_published_schema_is_made_from_the_columns_as_java_makes_it() {
    let registry = Registry::serve(&[]);
    let spec = AvroProduceSpec {
        subject: "pub".to_owned(),
        version: Some("latest".to_owned()),
        publish_schema: true,
        namespace: Some("ns.x".to_owned()),
        ..AvroProduceSpec::default()
    };
    let sent = Sent::default();
    let error = sent
        .publish(
            t1(),
            &options(&registry, ProduceSpec::Ignore, ProduceSpec::Avro(spec)),
        )
        .err()
        .unwrap();
    let schema = r#"{"type":"record","name":"pub","namespace":"ns.x","fields":[{"name":"I","type":["int","null"]},{"name":"L","type":["long","null"]},{"name":"D","type":["double","null"]},{"name":"F","type":["float","null"]},{"name":"S","type":["string","null"]},{"name":"B","type":[{"type":"bytes","dhType":"java.lang.Boolean"},"null"]},{"name":"T","type":[{"type":"long","logicalType":"timestamp-micros"},"null"]}]}"#;
    assert_eq!(
        registry.requests(),
        [
            register_request("pub", schema),
            register_request("t-value", schema)
        ]
    );
    assert_eq!(
        error.to_string(),
        "org.apache.kafka.common.errors.SerializationException: Error serializing Avro message \
         (caused by org.apache.avro.UnresolvedUnionException: Not in union \
         [{\"type\":\"bytes\",\"dhType\":\"java.lang.Boolean\"},\"null\"]: true (field=B))"
    );

    let registry = Registry::with_subject("s_nullable", NULLABLE);
    let excluding = AvroProduceSpec {
        exclude: Some(vec!["L".to_owned()]),
        ..avro("s_nullable")
    };
    let error = Sent::default()
        .publish(
            t1(),
            &options(&registry, ProduceSpec::Ignore, ProduceSpec::Avro(excluding)),
        )
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        "java.lang.IllegalArgumentException: field B for column T has no logical type."
    );
}

/// Probes c1, c2 and c7: the columns come first, a null cell takes the default, and a
/// record with no topic or a negative timestamp fails where `ProducerRecord` fails.
#[test]
fn topic_partition_and_timestamp_columns_set_each_record() {
    let table = ReferenceTable::new_static(
        definition(&[
            ("Top", ColumnType::String),
            ("P", ColumnType::Int),
            ("T", ColumnType::Instant),
            ("V", ColumnType::Int),
        ]),
        RowSet::flat(3),
        vec![
            Arc::new(StringArray::from(vec![Some("tx"), None, Some("ty")])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(0)])),
            Arc::new(
                TimestampNanosecondArray::from(vec![
                    Some(1_704_164_645_123_456_000),
                    None,
                    Some(1_999_999),
                ])
                .with_timezone("UTC"),
            ),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
        ],
    );
    let registry = Registry::serve(&[]);
    let mut options = options(
        &registry,
        ProduceSpec::Ignore,
        ProduceSpec::Simple("V".to_owned()),
    );
    options.config.pop();
    options.topic = "c1".to_owned();
    options.topic_column = Some("Top".to_owned());
    options.partition_column = Some("P".to_owned());
    options.timestamp_column = Some("T".to_owned());
    let sent = Sent::default();
    sent.publish(table.clone(), &options).unwrap();
    let records: Vec<(String, Option<i32>, Option<i64>)> = sent
        .take()
        .into_iter()
        .map(|r| (r.topic, r.partition, r.timestamp))
        .collect();
    assert_eq!(
        records,
        [
            ("tx".to_owned(), Some(1), Some(1_704_164_645_123)),
            ("c1".to_owned(), None, None),
            ("ty".to_owned(), Some(0), Some(1)),
        ]
    );

    options.topic = String::new();
    let error = sent.publish(table.clone(), &options).err().unwrap();
    assert_eq!(
        error.to_string(),
        "java.lang.IllegalArgumentException: Topic cannot be null."
    );
    assert_eq!(sent.take().len(), 1, "the record before it was sent");

    options.topic_column = None;
    let error = sent.publish(table.clone(), &options).err().unwrap();
    assert_eq!(
        error.to_string(),
        "java.lang.IllegalArgumentException: Must set topic or topicColumn (or both)"
    );
    options.topic = "c".to_owned();
    options.partition_column = Some("V".to_owned());
    options.timestamp_column = Some("P".to_owned());
    let error = sent.publish(table, &options).err().unwrap();
    assert_eq!(
        error.to_string(),
        "java.lang.ClassCastException: Cannot convert [P] of type int to type java.time.Instant"
    );
}

/// A removed row of a refreshing table is a record of its previous key, written in Avro,
/// and no value.
#[test]
fn a_removed_row_is_its_previous_avro_key_without_a_value() {
    const KEY: &str =
        r#"{"type":"record","name":"K","fields":[{"name":"I","type":["null","int"]}]}"#;
    let graph = SteppedUpdateGraph::new();
    let script = ScriptedTable::new(
        graph.clone(),
        definition(&[("I", ColumnType::Int)]),
        RowSet::flat(2),
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(-7)]))],
    );
    let registry = Registry::with_subject("s_key", KEY);
    let sent = Sent::default();
    let _publication = sent
        .publish(
            script.table(),
            &options(
                &registry,
                ProduceSpec::Avro(avro("s_key")),
                ProduceSpec::Ignore,
            ),
        )
        .unwrap();
    let keys = |records: Vec<OutRecord>| -> Vec<Option<String>> {
        records.into_iter().map(|r| r.key.map(hex)).collect()
    };
    // Probe a2: the key `-key` subject, and the same bytes as Java's.
    assert_eq!(
        keys(sent.take()),
        [
            Some("00000000070202".to_owned()),
            Some("0000000007020d".to_owned())
        ]
    );
    script.push(TableChange::new().remove([1]));
    graph.run_cycle();
    let removed = sent.take();
    assert_eq!(removed[0].value, None);
    assert_eq!(keys(removed), [Some("0000000007020d".to_owned())]);
    assert_eq!(registry.requests()[2], register_request("t-key", KEY));
    assert_eq!(
        registry.requests().len(),
        3,
        "the schema is registered once"
    );
}
