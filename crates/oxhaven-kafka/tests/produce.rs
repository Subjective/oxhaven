//! Tables published to a scripted record sink, on a graph the test steps. Every record
//! expected here is what the Java reference published to a broker for the same table and
//! changes (`docs/KAFKA.md`, "Producing").
//!
//! The broker tests are in `tests/broker.rs`.

use std::sync::{Arc, Mutex};

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, UInt16Array,
};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::ModifiedColumnSet;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::table::{stream_table, StreamCallbacks};
use oxhaven_kafka::{
    produce_to, JsonProduceSpec, KafkaError, OutRecord, ProduceOptions, ProduceSpec, Publication,
    RecordSink,
};
use oxhaven_testkit::{ReferenceTable, ScriptedTable, TableChange};

/// What a publication sent, flush by flush.
#[derive(Clone, Default)]
struct Sent {
    flushed: Arc<Mutex<Vec<Vec<OutRecord>>>>,
    /// The failure the next flush reports.
    failing: Arc<Mutex<Option<String>>>,
    closed: Arc<Mutex<bool>>,
}

struct Sink {
    sent: Sent,
    pending: Vec<OutRecord>,
}

impl RecordSink for Sink {
    fn send(&mut self, record: OutRecord) {
        self.pending.push(record);
    }

    fn flush(&mut self) -> Result<(), KafkaError> {
        let records = std::mem::take(&mut self.pending);
        self.sent.flushed.lock().unwrap().push(records);
        match self.sent.failing.lock().unwrap().take() {
            Some(failure) => Err(KafkaError::Broker(failure)),
            None => Ok(()),
        }
    }
}

impl Drop for Sink {
    fn drop(&mut self) {
        *self.sent.closed.lock().unwrap() = true;
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
            Ok(Box::new(Sink {
                sent: sent.clone(),
                pending: Vec::new(),
            }))
        })
    }

    /// The records of each flush since the last call, as (key, value) text.
    fn take(&self) -> Vec<Vec<(Option<String>, Option<String>)>> {
        let text = |bytes: Option<Vec<u8>>| bytes.map(|bytes| hex_or_text(&bytes));
        let flushed = std::mem::take(&mut *self.flushed.lock().unwrap());
        flushed
            .into_iter()
            .map(|records| {
                records
                    .into_iter()
                    .map(|record| (text(record.key), text(record.value)))
                    .collect()
            })
            .collect()
    }

    fn partitions(&self) -> Vec<Option<i32>> {
        let flushed = self.flushed.lock().unwrap();
        flushed
            .iter()
            .flatten()
            .map(|record| record.partition)
            .collect()
    }
}

/// Text as it is; anything else as hexadecimal with a `0x` in front.
fn hex_or_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) if text.chars().all(|c| !c.is_control()) => text.to_owned(),
        _ => {
            let digits: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("0x{digits}")
        }
    }
}

fn some(text: &str) -> Option<String> {
    Some(text.to_owned())
}

fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    let columns = columns
        .iter()
        .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
        .collect();
    TableDefinition::new(columns).unwrap()
}

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn options(key: ProduceSpec, value: ProduceSpec) -> ProduceOptions {
    ProduceOptions {
        config: vec![("bootstrap.servers".to_owned(), "unused".to_owned())],
        topic: "t".to_owned(),
        partition: None,
        key,
        value,
        last_by: false,
        publish_initial: true,
    }
}

fn simple(column: &str) -> ProduceSpec {
    ProduceSpec::Simple(column.to_owned())
}

fn json(spec: JsonProduceSpec) -> ProduceSpec {
    ProduceSpec::Json(spec)
}

fn include(columns: &[&str]) -> JsonProduceSpec {
    JsonProduceSpec {
        include: Some(columns.iter().map(|column| (*column).to_owned()).collect()),
        ..JsonProduceSpec::default()
    }
}

/// Seen in the probes (`p1`): Kafka's serializers, big-endian, and a null cell is a
/// record without that key or value. The producer of a static table is closed at once.
#[test]
fn a_static_table_is_published_once_with_kafka_s_serializers() {
    let table = ReferenceTable::new_static(
        definition(&[
            ("S", ColumnType::String),
            ("I", ColumnType::Int),
            ("H", ColumnType::Short),
            ("L", ColumnType::Long),
            ("D", ColumnType::Double),
            ("F", ColumnType::Float),
        ]),
        RowSet::flat(4),
        vec![
            strings(&[Some("a"), None, Some("é"), Some("")]),
            ints(&[Some(1), None, Some(-2), Some(i32::MAX)]),
            Arc::new(Int16Array::from(vec![Some(1), None, Some(3), Some(4)])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(-1)])),
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                None,
                Some(f64::NAN),
                Some(-0.0),
            ])),
            Arc::new(Float32Array::from(vec![
                Some(1.5),
                None,
                Some(f32::INFINITY),
                Some(0.1),
            ])),
        ],
    );
    let sent = Sent::default();
    let cases = [
        (
            "S",
            "I",
            vec![
                (some("a"), some("0x00000001")),
                (None, None),
                (some("é"), some("0xfffffffe")),
                (some(""), some("0x7fffffff")),
            ],
        ),
        (
            "H",
            "L",
            vec![
                (some("0x0001"), some("0x0000000000000001")),
                (None, None),
                (some("0x0003"), some("0x0000000000000003")),
                (some("0x0004"), some("0xffffffffffffffff")),
            ],
        ),
    ];
    for (key, value, expected) in cases {
        let publication = sent.publish(table.clone(), &options(simple(key), simple(value)));
        let publication = publication.unwrap();
        assert_eq!(sent.take(), [expected]);
        assert!(
            *sent.closed.lock().unwrap(),
            "the producer of a static table is closed"
        );
        publication.stop().unwrap();
        assert!(publication
            .stop()
            .unwrap_err()
            .to_string()
            .contains("LivenessScope"));
    }
    let _ = sent
        .publish(table.clone(), &options(simple("D"), simple("F")))
        .unwrap();
    let raw: Vec<_> = sent.flushed.lock().unwrap().drain(..).flatten().collect();
    let keys: Vec<_> = raw.iter().map(|record| record.key.clone()).collect();
    let values: Vec<_> = raw.iter().map(|record| record.value.clone()).collect();
    assert_eq!(
        keys,
        [
            Some(1.5f64.to_bits().to_be_bytes().to_vec()),
            None,
            Some(vec![0x7f, 0xf8, 0, 0, 0, 0, 0, 0]),
            Some(vec![0x80, 0, 0, 0, 0, 0, 0, 0]),
        ]
    );
    assert_eq!(
        values,
        [
            Some(vec![0x3f, 0xc0, 0, 0]),
            None,
            Some(vec![0x7f, 0x80, 0, 0]),
            Some(vec![0x3d, 0xcc, 0xcc, 0xcd]),
        ]
    );

    let _ = sent
        .publish(table.clone(), &options(ProduceSpec::Ignore, simple("S")))
        .unwrap();
    assert_eq!(
        sent.take(),
        [vec![
            (None, some("a")),
            (None, None),
            (None, some("é")),
            (None, some(""))
        ]]
    );

    // Rows go out in chunks of 2048 (`PublishToKafka.CHUNK_SIZE`), each row once, in order.
    let count = 2 * 2048 + 1;
    let many = ReferenceTable::new_static(
        definition(&[("I", ColumnType::Int)]),
        RowSet::flat(count as u64),
        vec![ints(&(0..count).map(Some).collect::<Vec<_>>())],
    );
    let _ = sent
        .publish(many, &options(ProduceSpec::Ignore, simple("I")))
        .unwrap();
    let raw: Vec<_> = sent.flushed.lock().unwrap().drain(..).flatten().collect();
    let values: Vec<_> = raw
        .iter()
        .map(|record| record.value.clone().unwrap())
        .collect();
    let expected: Vec<_> = (0..count).map(|i| i.to_be_bytes().to_vec()).collect();
    assert_eq!(values, expected);
}

fn json_table() -> Arc<ReferenceTable> {
    ReferenceTable::new_static(
        definition(&[
            ("B", ColumnType::Boolean),
            ("C", ColumnType::Char),
            ("I", ColumnType::Int),
            ("L", ColumnType::Long),
            ("F", ColumnType::Float),
            ("D", ColumnType::Double),
            ("S", ColumnType::String),
        ]),
        RowSet::flat(3),
        vec![
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(UInt16Array::from(vec![Some(97), None, Some(0xD800)])),
            ints(&[Some(1), None, Some(2)]),
            Arc::new(Int64Array::from(vec![Some(1 << 62), None, Some(-1)])),
            Arc::new(Float32Array::from(vec![Some(0.1), None, Some(f32::NAN)])),
            Arc::new(Float64Array::from(vec![Some(1e7), None, Some(1e-4)])),
            strings(&[Some("x\"\\\n\u{1}é"), None, Some("")]),
        ],
    )
}

/// Seen in the probes (`j1`, `j2`, `j5`, `j9`, `j13`, `j15`): Jackson's text, fields in
/// the order of the columns, nulls left out or written, a repeated field in its first
/// place with the last value, nested objects that stay when empty.
#[test]
fn json_is_written_as_jackson_writes_it() {
    let sent = Sent::default();
    let publish = |spec: JsonProduceSpec| {
        sent.publish(json_table(), &options(ProduceSpec::Ignore, json(spec)))
            .unwrap();
        sent.take()
            .concat()
            .into_iter()
            .map(|(_, value)| value.unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        publish(JsonProduceSpec::default()),
        [
            r#"{"B":true,"C":"a","I":1,"L":4611686018427387904,"F":0.1,"D":1.0E7,"S":"x\"\\\n\u0001é"}"#,
            "{}",
            r#"{"B":false,"C":"?","I":2,"L":-1,"F":"NaN","D":1.0E-4,"S":""}"#,
        ]
    );
    let nulls = JsonProduceSpec {
        output_nulls: true,
        ..include(&["I", "S"])
    };
    assert_eq!(publish(nulls)[1], r#"{"I":null,"S":null}"#);
    let same_field = JsonProduceSpec {
        mapping: vec![("I".into(), "q".into()), ("L".into(), "q".into())],
        ..include(&["I", "L"])
    };
    assert_eq!(
        publish(same_field),
        [r#"{"q":4611686018427387904}"#, "{}", r#"{"q":-1}"#]
    );
    let nested = JsonProduceSpec {
        mapping: vec![("I".into(), "_a__b_".into())],
        nested_delimiter: Some("_".into()),
        ..include(&["I"])
    };
    assert_eq!(
        publish(nested),
        [
            r#"{"":{"a":{"":{"b":1}}}}"#,
            r#"{"":{"a":{"":{}}}}"#,
            r#"{"":{"a":{"":{"b":2}}}}"#,
        ]
    );
    assert_eq!(publish(include(&["I", "I"]))[0], r#"{"I":1}"#);
    let stamped = JsonProduceSpec {
        timestamp_field: Some("ts".into()),
        ..include(&["I"])
    };
    let stamped = publish(stamped);
    let stamp = |text: &str| text.split("\"ts\":").nth(1).unwrap().to_owned();
    assert!(stamped[1].starts_with("{\"ts\":\""), "{}", stamped[1]);
    assert_eq!(stamp(&stamped[0]), stamp(&stamped[2]), "one time per chunk");
}

/// Seen in the probes (`j6`, `j7`, `j14`, `p5` `cbk`): what Jackson's tree and Java's
/// split refuse fails the call, and nothing of the chunk is sent.
#[test]
fn json_that_java_cannot_build_fails_the_call() {
    let sent = Sent::default();
    let failure = |spec: JsonProduceSpec| {
        let options = options(ProduceSpec::Ignore, json(spec));
        let error = sent.publish(json_table(), &options).err().expect("refused");
        let flushed = sent.take();
        assert!(flushed.iter().all(Vec::is_empty), "{flushed:?}");
        error.to_string()
    };
    let scalar_then_nested = JsonProduceSpec {
        mapping: vec![("I".into(), "a".into()), ("L".into(), "a_b".into())],
        nested_delimiter: Some("_".into()),
        output_nulls: true,
        ..include(&["I", "L"])
    };
    assert_eq!(
        failure(scalar_then_nested),
        "java.lang.ClassCastException: class com.fasterxml.jackson.databind.node.IntNode \
         cannot be cast to class com.fasterxml.jackson.databind.node.ObjectNode \
         (com.fasterxml.jackson.databind.node.IntNode and \
         com.fasterxml.jackson.databind.node.ObjectNode are in unnamed module of loader 'app')"
    );
    let only_delimiters = JsonProduceSpec {
        mapping: vec![("I".into(), "__".into())],
        nested_delimiter: Some("__".into()),
        ..include(&["I"])
    };
    assert_eq!(
        failure(only_delimiters),
        "java.lang.IllegalArgumentException: Unknown column name I for table (caused by \
         java.lang.ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 0)"
    );
    let timestamp_of_delimiters = JsonProduceSpec {
        timestamp_field: Some("__".into()),
        nested_delimiter: Some("__".into()),
        ..include(&["I"])
    };
    assert_eq!(
        failure(timestamp_of_delimiters),
        "java.lang.ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 0"
    );
    assert_eq!(
        failure(include(&["I", "Nope"])),
        "java.lang.IllegalArgumentException: includeColumns contains names not found in \
         table columns: [Nope]"
    );
    let regular = JsonProduceSpec {
        nested_delimiter: Some(".".into()),
        ..include(&["I"])
    };
    assert!(failure(regular).starts_with("not ported yet: java.lang.String#split"));
}

struct Ticking {
    graph: Arc<SteppedUpdateGraph>,
    script: Arc<ScriptedTable>,
}

/// A refreshing table of keys `K` and values `V` and `W`, rows 0 to 2: `a 1 x`, `b 2 y`,
/// `c 3 z`.
fn ticking() -> Ticking {
    let graph = SteppedUpdateGraph::new();
    let script = ScriptedTable::new(
        graph.clone(),
        definition(&[
            ("K", ColumnType::String),
            ("V", ColumnType::Int),
            ("W", ColumnType::String),
        ]),
        RowSet::flat(3),
        vec![
            strings(&[Some("a"), Some("b"), Some("c")]),
            ints(&[Some(1), Some(2), Some(3)]),
            strings(&[Some("x"), Some("y"), Some("z")]),
        ],
    );
    Ticking { graph, script }
}

impl Ticking {
    fn table(&self) -> Arc<dyn Table> {
        self.script.table()
    }

    fn step(&self, change: TableChange) {
        self.script.push(change);
        self.graph.run_cycle();
    }
}

/// A row is removed, one modified and one added in one cycle: the removed row's old key
/// with no value first, then the added and modified rows in row key order (`PublishToKafka`;
/// seen in the probes `r4` and `r5`).
#[test]
fn a_cycle_publishes_removes_then_adds_and_modifies_in_row_order() {
    let ticking = ticking();
    let sent = Sent::default();
    let publication = sent
        .publish(ticking.table(), &options(simple("K"), simple("V")))
        .unwrap();
    assert_eq!(sent.take().len(), 1);
    ticking.step(
        TableChange::new()
            .remove([0])
            .add(
                [5],
                vec![
                    strings(&[Some("d")]),
                    ints(&[Some(4)]),
                    strings(&[Some("w")]),
                ],
            )
            .modify([2], [(1, ints(&[Some(30)]))]),
    );
    assert_eq!(
        sent.take(),
        [vec![
            (some("a"), None),
            (some("c"), some("0x0000001e")),
            (some("d"), some("0x00000004")),
        ]]
    );
    // A modify of a column that is not in the value is not published (probe `r7`).
    ticking.step(TableChange::new().modify([1], [(2, strings(&[Some("yy")]))]));
    assert_eq!(sent.take(), Vec::<Vec<_>>::new());
    // ... unless the value holds it.
    drop(publication);
    let publication = sent
        .publish(
            ticking.table(),
            &ProduceOptions {
                publish_initial: false,
                ..options(simple("K"), json(include(&["W"])))
            },
        )
        .unwrap();
    assert_eq!(sent.take(), Vec::<Vec<_>>::new());
    ticking.step(TableChange::new().modify([1], [(2, strings(&[Some("y2")]))]));
    assert_eq!(sent.take(), [vec![(some("b"), some(r#"{"W":"y2"}"#))]]);

    publication.stop().unwrap();
    ticking.step(TableChange::new().remove([1]));
    assert_eq!(sent.take(), Vec::<Vec<_>>::new(), "nothing after stop");
}

/// A key column that is modified fails the listener, which then publishes nothing more
/// (probe `assert`). An update whose modified columns are `ALL` makes Java's listener
/// throw a `NullPointerException`; it is refused by name at the same point: before
/// anything is sent with a key, after the removes without one (probes `r1`, `r2`).
#[test]
fn modified_keys_and_modified_columns_all_stop_the_publication() {
    let ticking = ticking();
    let sent = Sent::default();
    let publication = sent
        .publish(ticking.table(), &options(simple("W"), simple("V")))
        .unwrap();
    sent.take();
    ticking.step(TableChange::new().modify([0], [(2, strings(&[Some("x2")]))]));
    assert_eq!(sent.take(), Vec::<Vec<_>>::new());
    let failure = publication.failure().expect("the publication failed");
    assert!(
        failure.contains("Key columns should never be modified"),
        "{failure}"
    );
    ticking.step(TableChange::new().remove([1]));
    assert_eq!(sent.take(), Vec::<Vec<_>>::new());

    for (key, published) in [
        (ProduceSpec::Ignore, vec![(None, None)]),
        (simple("K"), vec![]),
    ] {
        let ticking = self::ticking();
        let sent = Sent::default();
        let publication = sent
            .publish(ticking.table(), &options(key, simple("V")))
            .unwrap();
        sent.take();
        let change = TableChange::new()
            .remove([0])
            .modify([1], [(1, ints(&[Some(20)]))]);
        let mut reported = change.update.clone();
        reported.modified_columns = ModifiedColumnSet::All;
        ticking.script.push_misreported(change, reported);
        ticking.graph.run_cycle();
        let flushed = sent.take();
        assert_eq!(flushed.concat(), published);
        let failure = publication.failure().expect("refused");
        assert!(
            failure.contains("ModifiedColumnSet#containsAny"),
            "{failure}"
        );
    }
}

/// `last_by_key_columns`: the last row of each key, and each change of that (probe `r4`).
#[test]
fn last_by_publishes_the_last_row_of_each_key() {
    let ticking = ticking();
    let sent = Sent::default();
    let mut options = options(simple("W"), simple("V"));
    options.last_by = true;
    let _publication = sent.publish(ticking.table(), &options).unwrap();
    assert_eq!(
        sent.take(),
        [vec![
            (some("x"), some("0x00000001")),
            (some("y"), some("0x00000002")),
            (some("z"), some("0x00000003")),
        ]]
    );
    // `c` moves from group `z` to group `y`, and `a` leaves group `x`.
    ticking.step(
        TableChange::new()
            .remove([0])
            .modify([2], [(2, strings(&[Some("y")])), (1, ints(&[Some(9)]))]),
    );
    assert_eq!(
        sent.take(),
        [vec![
            (some("x"), None),
            (some("z"), None),
            (some("y"), some("0x00000009")),
        ]]
    );
}

/// The removes of a blink table are not published (probe `r8`); a table that fails ends
/// the publication.
#[test]
fn a_blink_table_publishes_its_adds_only() {
    let graph = SteppedUpdateGraph::new();
    let callbacks = StreamCallbacks {
        on_flush: None,
        on_shutdown: None,
    };
    let (table, publisher) = stream_table(
        graph.clone(),
        definition(&[("K", ColumnType::String), ("V", ColumnType::Int)]),
        callbacks,
    );
    let sent = Sent::default();
    let publication = sent
        .publish(table.clone(), &options(simple("K"), simple("V")))
        .unwrap();
    assert_eq!(sent.take(), Vec::<Vec<_>>::new());
    publisher
        .add(vec![
            strings(&[Some("a"), Some("b")]),
            ints(&[Some(1), Some(2)]),
        ])
        .unwrap();
    graph.run_cycle();
    publisher
        .add(vec![strings(&[Some("c")]), ints(&[Some(3)])])
        .unwrap();
    graph.run_cycle();
    assert_eq!(
        sent.take(),
        [
            vec![
                (some("a"), some("0x00000001")),
                (some("b"), some("0x00000002"))
            ],
            vec![(some("c"), some("0x00000003"))],
        ]
    );
    publisher.fail(oxhaven_contracts::table::TableFailure {
        message: "boom".to_owned(),
    });
    graph.run_cycle();
    assert!(publication.failure().unwrap().contains("boom"));
}

/// A send that fails fails the call when it publishes the rows a table has, and the
/// publication afterwards; nothing more is published after that.
#[test]
fn a_failed_send_fails_the_call_or_the_publication() {
    let ticking = ticking();
    let sent = Sent::default();
    *sent.failing.lock().unwrap() = Some("KafkaProducer reported send failure".into());
    let error = sent
        .publish(ticking.table(), &options(simple("K"), simple("V")))
        .err()
        .unwrap();
    assert_eq!(error.to_string(), "KafkaProducer reported send failure");

    let publication = sent
        .publish(ticking.table(), &options(simple("K"), simple("V")))
        .unwrap();
    sent.take();
    *sent.failing.lock().unwrap() = Some("down".into());
    ticking.step(TableChange::new().remove([0]));
    assert_eq!(sent.take().concat(), [(some("a"), None)]);
    assert_eq!(publication.failure().as_deref(), Some("down"));
    ticking.step(TableChange::new().remove([1]));
    assert_eq!(sent.take(), Vec::<Vec<_>>::new());
}

/// The partition of `partition`, and Java's refusal of a negative one at the first
/// record (probe `p5` `cbn`, `cbn2`); the checks of `KafkaPublishOptions`.
#[test]
fn partitions_and_the_checks_of_the_options() {
    let ticking = ticking();
    let sent = Sent::default();
    let mut options = options(simple("K"), simple("V"));
    options.partition = Some(1);
    let _publication = sent.publish(ticking.table(), &options).unwrap();
    assert_eq!(sent.partitions(), [Some(1); 3]);
    sent.take();
    options.partition = Some(-1);
    let error = sent.publish(ticking.table(), &options).err().unwrap();
    assert_eq!(
        error.to_string(),
        "java.lang.IllegalArgumentException: Invalid partition: -1. Partition number should \
         always be non-negative or null."
    );
    let empty = ReferenceTable::new_static(
        definition(&[("V", ColumnType::Int)]),
        RowSet::empty(),
        vec![ints(&[])],
    );
    let mut empty_options = self::options(ProduceSpec::Ignore, simple("V"));
    empty_options.partition = Some(-1);
    assert!(sent.publish(empty, &empty_options).is_ok());

    let refused = |options: ProduceOptions| {
        let table = ReferenceTable::new_static(
            definition(&[("V", ColumnType::Int)]),
            RowSet::flat(1),
            vec![ints(&[Some(1)])],
        );
        sent.publish(table, &options).err().unwrap().to_string()
    };
    assert!(
        refused(self::options(ProduceSpec::Ignore, ProduceSpec::Ignore))
            .contains("keySpec and valueSpec can't both be ignore specs")
    );
    let mut later = self::options(ProduceSpec::Ignore, simple("V"));
    later.publish_initial = false;
    assert!(refused(later).contains("publishInitial==false && table.isRefreshing() == false"));
    let mut untitled = self::options(ProduceSpec::Ignore, simple("V"));
    untitled.topic = String::new();
    assert!(refused(untitled).contains("Must set topic or topicColumn (or both)"));
    assert!(refused(self::options(simple("Nope"), simple("V"))).contains("because \"cd\" is null"));
}

#[test]
fn the_graph_needs_no_lock_from_the_caller() {
    // `produce_to` takes the shared lock itself; a caller holding it already is fine too.
    let ticking = ticking();
    let sent = Sent::default();
    let graph: Arc<dyn UpdateGraph> = ticking.graph.clone();
    let publication = oxhaven_contracts::graph::locked(&*graph, || {
        sent.publish(ticking.table(), &options(simple("K"), simple("V")))
    });
    assert!(publication.is_ok());
}
