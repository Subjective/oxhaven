//! Topics consumed from a real broker. Ignored unless asked for: `tests/broker.sh` starts a
//! broker in Docker on this machine, fills the topics these tests read and runs them. Tests
//! that need records of their own produce them with librdkafka's producer, into topics
//! named after the time, so that a broker can be used for more than one run.
//!
//! The broker's address comes from `OXHAVEN_KAFKA_BROKER` and must be on this machine: a
//! test never connects anywhere else.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{Int32Type, Int64Type};
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{
    consume, consume_to_partitioned_table, AvroMapping, AvroProduceSpec, AvroSchema,
    ConsumeOptions, InitialOffset, JsonProduceSpec, Offsets, ProduceOptions, ProduceSpec,
    ProtobufProtocol, Spec, TableType,
};
use oxhaven_testkit::{ReferenceTable, ScriptedTable, TableChange};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

fn broker() -> String {
    let address = std::env::var("OXHAVEN_KAFKA_BROKER")
        .expect("OXHAVEN_KAFKA_BROKER names the broker; run crates/oxhaven-kafka/tests/broker.sh");
    let host = address
        .rsplit_once(':')
        .map_or(address.as_str(), |(host, _)| host);
    assert!(
        host == "127.0.0.1" || host == "localhost",
        "the broker must be on this machine, not at {host}"
    );
    address
}

fn options(topic: &str, offsets: Offsets, value: Spec, table_type: TableType) -> ConsumeOptions {
    ConsumeOptions {
        config: vec![("bootstrap.servers".to_owned(), broker())],
        topic: topic.to_owned(),
        partitions: None,
        offsets,
        key: Spec::Simple {
            column: Some("K".to_owned()),
            data_type: Some(ColumnType::String),
        },
        value,
        table_type,
    }
}

fn json() -> Spec {
    Spec::Json {
        columns: vec![
            ("N".to_owned(), ColumnType::Long),
            ("S".to_owned(), ColumnType::String),
        ],
        mapping: None,
    }
}

/// Runs cycles until `table` has `rows` rows, for at most twenty seconds.
fn wait_for(graph: &SteppedUpdateGraph, table: &dyn Table, rows: u64) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while table.row_set().size() != rows && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
    }
    assert!(!table.is_failed(), "the table failed");
    assert_eq!(table.row_set().size(), rows);
}

fn longs(table: &dyn Table, name: &str) -> Vec<Option<i64>> {
    let index = table.definition().index_of(name).unwrap();
    let column = table.column(index).chunk(&table.row_set()).unwrap();
    column.as_primitive::<Int64Type>().iter().collect()
}

fn strings(table: &dyn Table, name: &str) -> Vec<Option<String>> {
    let index = table.definition().index_of(name).unwrap();
    let column = table.column(index).chunk(&table.row_set()).unwrap();
    let cells = column.as_string::<i32>().iter();
    cells.map(|cell| cell.map(str::to_owned)).collect()
}

const BEGINNING: Offsets = Offsets::Everywhere(InitialOffset::Beginning);

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_topic_is_consumed_from_its_beginning_and_from_an_offset() {
    let graph = SteppedUpdateGraph::new();
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let all = options("oxhaven_one", BEGINNING, json(), TableType::Append);
    let table = consume(shared.clone(), &all).unwrap();
    wait_for(&graph, table.as_ref(), 4);
    assert_eq!(
        longs(table.as_ref(), "KafkaOffset"),
        [Some(0), Some(1), Some(2), Some(3)]
    );
    assert_eq!(
        longs(table.as_ref(), "N"),
        [Some(1), Some(2), None, Some(4)]
    );
    let text = |text: &str| Some(text.to_owned());
    assert_eq!(
        strings(table.as_ref(), "S"),
        [text("a"), text("b"), text("1.50"), None]
    );
    assert_eq!(
        strings(table.as_ref(), "K"),
        [text("k1"), text("k2"), text("k3"), text("k4")]
    );

    let later = Offsets::ByPartition(vec![(0, 2)]);
    let later = options("oxhaven_one", later, json(), TableType::Append);
    let table = consume(shared.clone(), &later).unwrap();
    wait_for(&graph, table.as_ref(), 2);
    assert_eq!(longs(table.as_ref(), "KafkaOffset"), [Some(2), Some(3)]);

    let end = Offsets::Everywhere(InitialOffset::End);
    let end = options("oxhaven_one", end, json(), TableType::Append);
    let table = consume(shared, &end).unwrap();
    for _ in 0..10 {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
    }
    assert_eq!(table.row_set().size(), 0);
    assert!(!table.is_failed());
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn every_partition_is_read_in_order_and_one_can_be_chosen() {
    let graph = SteppedUpdateGraph::new();
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let text = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::String),
    };
    let all = options("oxhaven_two", BEGINNING, text.clone(), TableType::Append);
    let table = consume(shared.clone(), &all).unwrap();
    wait_for(&graph, table.as_ref(), 6);
    let index = table.definition().index_of("KafkaPartition").unwrap();
    let partitions = table.column(index).chunk(&table.row_set()).unwrap();
    let partitions = partitions.as_primitive::<Int32Type>().values().to_vec();
    let offsets = longs(table.as_ref(), "KafkaOffset");
    for partition in [0, 1] {
        // The offsets of one partition appear in order, from 0 and without a gap.
        let of_partition: Vec<Option<i64>> = (partitions.iter().zip(&offsets))
            .filter(|(p, _)| **p == partition)
            .map(|(_, offset)| *offset)
            .collect();
        let expected: Vec<Option<i64>> = (0..of_partition.len() as i64).map(Some).collect();
        assert_eq!(of_partition, expected, "partition {partition}");
    }
    let mut values = strings(table.as_ref(), "V");
    values.sort();
    let expected = ["v1", "v2", "v3", "v4", "v5", "v6"].map(|v| Some(v.to_owned()));
    assert_eq!(values, expected);

    let in_one = partitions.iter().filter(|p| **p == 1).count() as u64;
    assert!(in_one > 0 && in_one < 6, "the keys fall in both partitions");
    let mut one = options("oxhaven_two", BEGINNING, text, TableType::Append);
    one.partitions = Some(vec![1]);
    let table = consume(shared, &one).unwrap();
    wait_for(&graph, table.as_ref(), in_one);
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_blink_table_empties_and_a_missing_topic_has_no_rows() {
    let graph = SteppedUpdateGraph::new();
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let blink = options("oxhaven_one", BEGINNING, json(), TableType::Blink);
    let table = consume(shared.clone(), &blink).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut seen = 0;
    while seen < 4 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
        seen += table.row_set().size();
    }
    assert_eq!(seen, 4);
    graph.run_cycle();
    assert_eq!(table.row_set().size(), 0);

    let missing = options(
        "oxhaven_no_such_topic",
        BEGINNING,
        json(),
        TableType::Append,
    );
    let table = consume(shared, &missing).unwrap();
    graph.run_cycle();
    assert_eq!(table.row_set().size(), 0);
    assert!(!table.is_failed());
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_partitioned_table_has_a_constituent_per_partition() {
    let graph = SteppedUpdateGraph::new();
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let text = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::String),
    };
    let all = options("oxhaven_two", BEGINNING, text.clone(), TableType::Append);
    let partitioned = consume_to_partitioned_table(shared.clone(), &all).unwrap();
    // The table of tables shows its constituents from the next cycle on, as in Java.
    assert_eq!(partitioned.table().row_set().size(), 0);
    let merged = partitioned.merge().unwrap();
    wait_for(&graph, merged.as_ref(), 6);
    let underlying = partitioned.table();
    let index = underlying.definition().index_of("Partition").unwrap();
    let keys = underlying
        .column(index)
        .chunk(&underlying.row_set())
        .unwrap();
    assert_eq!(keys.as_primitive::<Int32Type>().values(), &[0, 1]);
    let mut total = 0;
    for (key, constituent) in [0, 1].into_iter().zip(partitioned.constituents().unwrap()) {
        let index = constituent.definition().index_of("KafkaPartition").unwrap();
        let rows = constituent.row_set();
        let partitions = constituent.column(index).chunk(&rows).unwrap();
        assert!(partitions
            .as_primitive::<Int32Type>()
            .values()
            .iter()
            .all(|p| *p == key));
        let offsets = longs(constituent.as_ref(), "KafkaOffset");
        let expected: Vec<Option<i64>> = (0..rows.size() as i64).map(Some).collect();
        assert_eq!(offsets, expected, "partition {key}");
        total += rows.size();
    }
    assert_eq!(total, 6);

    // Records that arrive later reach their partition's constituent.
    let topic = fresh("partitioned");
    produce(&topic, "none", &["a"]);
    let blink = options(&topic, BEGINNING, text, TableType::Blink);
    let partitioned = consume_to_partitioned_table(shared.clone(), &blink).unwrap();
    let merged = partitioned.merge().unwrap();
    // A blink constituent shows each record for one cycle.
    let seen = |count: usize| {
        let mut seen = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while seen.len() < count && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
            graph.run_cycle();
            seen.extend(strings(merged.as_ref(), "V").into_iter().flatten());
        }
        seen
    };
    assert_eq!(seen(1), ["a"]);
    produce(&topic, "none", &["b", "c"]);
    assert_eq!(seen(2), ["b", "c"]);
    graph.run_cycle();
    assert_eq!(merged.row_set().size(), 0);

    // A topic the request makes the broker create has no partition yet, in Java's answer.
    let missing = options(&fresh("missing"), BEGINNING, json(), TableType::Append);
    let error = consume_to_partitioned_table(shared, &missing)
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .starts_with("java.lang.NullPointerException"),
        "{error}"
    );
}

/// A name no earlier run of these tests used.
fn fresh(what: &str) -> String {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    format!("oxhaven_{what}_{}", now.unwrap().as_millis())
}

/// Appends `values` to partition 0 of `topic`, which the broker creates if it must.
fn produce(topic: &str, codec: &str, values: &[&str]) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .set("compression.codec", codec)
        .create()
        .unwrap();
    for value in values {
        let record = BaseRecord::<(), str>::to(topic)
            .partition(0)
            .payload(*value);
        producer.send(record).map_err(|(error, _)| error).unwrap();
    }
    producer.flush(Duration::from_secs(60)).unwrap();
}

/// A table of the values of `topic` as text, with `properties` for the consumer.
fn values_of(
    graph: &Arc<SteppedUpdateGraph>,
    topic: &str,
    offsets: InitialOffset,
    properties: &[(&str, &str)],
) -> Arc<dyn Table> {
    let text = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::String),
    };
    let offsets = Offsets::Everywhere(offsets);
    let mut options = options(topic, offsets, text, TableType::Append);
    options.key = Spec::Ignore;
    for (name, value) in properties {
        let pair = ((*name).to_owned(), (*value).to_owned());
        options.config.push(pair);
    }
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    consume(shared, &options).unwrap()
}

/// Runs cycles for a second and a half: three polls of the consumer.
fn settle(graph: &SteppedUpdateGraph) {
    for _ in 0..15 {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
    }
}

/// Keeps the failure a table tells its listeners.
#[derive(Default)]
struct Failures(Mutex<Option<String>>);

impl Dependency for Failures {
    fn satisfied(&self, _step: u64) -> bool {
        true
    }
}

impl UpdateListener for Failures {
    fn on_update(&self, _update: &TableUpdate) -> Result<(), TableFailure> {
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        *self.0.lock().unwrap() = Some(failure.message.clone());
    }
}

/// Runs cycles until `table`, which no cycle has run for yet, has failed, and returns why.
fn wait_for_failure(graph: &SteppedUpdateGraph, table: &Arc<dyn Table>) -> String {
    let failures = Arc::new(Failures::default());
    let listener: Arc<dyn UpdateListener> = failures.clone();
    table.add_listener(&listener);
    let deadline = Instant::now() + Duration::from_secs(20);
    while !table.is_failed() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
    }
    let failure = failures.0.lock().unwrap().clone();
    failure.expect("the table fails")
}

/// Waits until `group` has committed `offset` for partition 0 of `topic`.
fn wait_for_commit(group: &str, topic: &str, offset: i64) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .set("group.id", group)
        .create()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut asked = TopicPartitionList::new();
        asked.add_partition(topic, 0);
        let committed = consumer.committed_offsets(asked, Duration::from_secs(10));
        let committed = committed.unwrap();
        let committed = committed.find_partition(topic, 0).unwrap().offset();
        if committed == Offset::Offset(offset) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{group} has committed {committed:?} for {topic}, not {offset}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn without_offsets_and_without_a_group_auto_offset_reset_decides() {
    let graph = SteppedUpdateGraph::new();
    let topic = fresh("reset");
    produce(&topic, "none", &["v0", "v1", "v2"]);

    // Java's default is `latest`: only what arrives from now on.
    let latest = values_of(&graph, &topic, InitialOffset::DontSeek, &[]);
    let earliest = [("auto.offset.reset", "earliest")];
    let earliest = values_of(&graph, &topic, InitialOffset::DontSeek, &earliest);
    wait_for(&graph, earliest.as_ref(), 3);
    settle(&graph);
    assert_eq!(latest.row_set().size(), 0);
    produce(&topic, "none", &["v3"]);
    wait_for(&graph, latest.as_ref(), 1);
    assert_eq!(longs(latest.as_ref(), "KafkaOffset"), [Some(3)]);
    wait_for(&graph, earliest.as_ref(), 4);

    let none = [("auto.offset.reset", "none")];
    let none = values_of(&graph, &topic, InitialOffset::DontSeek, &none);
    let failure = wait_for_failure(&graph, &none);
    let expected = format!("Undefined offset with no reset policy for partitions: [{topic}-0]");
    assert!(failure.contains(&expected), "{failure}");
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_group_commits_its_position_and_the_next_table_starts_there() {
    let graph = SteppedUpdateGraph::new();
    let topic = fresh("group");
    let group = fresh("g");
    produce(&topic, "none", &["v0", "v1", "v2"]);
    let grouped = [
        ("group.id", group.as_str()),
        ("auto.offset.reset", "earliest"),
    ];

    // Nothing is committed yet, so the policy decides; the position is then committed,
    // every five seconds and when the consumer closes with its table.
    let first = values_of(&graph, &topic, InitialOffset::DontSeek, &grouped);
    wait_for(&graph, first.as_ref(), 3);
    drop(first);
    wait_for_commit(&group, &topic, 3);

    produce(&topic, "none", &["v3"]);
    let second = values_of(&graph, &topic, InitialOffset::DontSeek, &grouped);
    wait_for(&graph, second.as_ref(), 1);
    assert_eq!(longs(second.as_ref(), "KafkaOffset"), [Some(3)]);
    assert_eq!(strings(second.as_ref(), "V"), [Some("v3".to_owned())]);
    drop(second);
    wait_for_commit(&group, &topic, 4);

    // A position is committed without a record having been read from it: here the end of
    // the topic, while the table is still open.
    let at_end = fresh("e");
    let properties = [
        ("group.id", at_end.as_str()),
        ("auto.commit.interval.ms", "500"),
    ];
    let quiet = values_of(&graph, &topic, InitialOffset::End, &properties);
    wait_for_commit(&at_end, &topic, 4);
    assert_eq!(quiet.row_set().size(), 0);

    // With `enable.auto.commit` off, nothing is.
    let silent = fresh("s");
    let properties = [
        ("group.id", silent.as_str()),
        ("enable.auto.commit", "false"),
        ("auto.offset.reset", "earliest"),
    ];
    let table = values_of(&graph, &topic, InitialOffset::DontSeek, &properties);
    wait_for(&graph, table.as_ref(), 4);
    drop(table);
    std::thread::sleep(Duration::from_secs(2));
    let again = values_of(&graph, &topic, InitialOffset::DontSeek, &properties);
    wait_for(&graph, again.as_ref(), 4);
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn compressed_batches_are_read_except_gzip() {
    let graph = SteppedUpdateGraph::new();
    for codec in ["snappy", "lz4", "zstd"] {
        let topic = fresh(codec);
        // Long and repetitive, so that the batch is in fact stored compressed.
        let long = format!("{codec} ").repeat(400);
        produce(&topic, codec, &[&long, "short"]);
        let table = values_of(&graph, &topic, InitialOffset::Beginning, &[]);
        wait_for(&graph, table.as_ref(), 2);
        let expected = [Some(long), Some("short".to_owned())];
        assert_eq!(strings(table.as_ref(), "V"), expected, "{codec}");
    }

    // `oxhaven_gzip` is filled by broker.sh: librdkafka without zlib cannot produce it.
    let table = values_of(&graph, "oxhaven_gzip", InitialOffset::Beginning, &[]);
    let failure = wait_for_failure(&graph, &table);
    let java = "org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator";
    assert!(failure.contains(java), "{failure}");
}

#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn the_topics_of_a_broker_are_listed_without_the_internal_ones() {
    // Asking for a group's offsets makes the broker create its internal topic for them.
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .set("group.id", fresh("t"))
        .create()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut asked = TopicPartitionList::new();
        asked.add_partition("oxhaven_one", 0);
        let answered = consumer.committed_offsets(asked, Duration::from_secs(10));
        let all = consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        let internal = |topic: &rdkafka::metadata::MetadataTopic| topic.name().starts_with("__");
        if answered.is_ok() && all.topics().iter().any(internal) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the broker made no internal topic"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    let config = vec![("bootstrap.servers".to_owned(), broker())];
    let topics = oxhaven_kafka::topics(&config).unwrap();
    for topic in ["oxhaven_one", "oxhaven_two"] {
        assert_eq!(topics.iter().filter(|name| *name == topic).count(), 1);
    }
    let internal = topics.iter().any(|name| name.starts_with("__"));
    assert!(!internal, "{topics:?}");
}

/// Run alone, by broker.sh, with the command that restarts the broker in
/// `OXHAVEN_KAFKA_RESTART`: the other tests would lose their broker.
#[test]
#[ignore = "needs a broker and restarts it: crates/oxhaven-kafka/tests/broker.sh"]
fn a_table_goes_on_after_its_broker_restarts() {
    let restart = std::env::var("OXHAVEN_KAFKA_RESTART")
        .expect("OXHAVEN_KAFKA_RESTART is the command that restarts the broker");
    let graph = SteppedUpdateGraph::new();
    let topic = fresh("restart");
    produce(&topic, "none", &["before"]);
    let table = values_of(&graph, &topic, InitialOffset::Beginning, &[]);
    wait_for(&graph, table.as_ref(), 1);

    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&restart)
        .status();
    assert!(status.unwrap().success(), "{restart} failed");
    settle(&graph);
    assert!(
        !table.is_failed(),
        "the table failed while its broker was away"
    );
    // The producer waits for the broker as the consumer does.
    produce(&topic, "none", &["after"]);
    let deadline = Instant::now() + Duration::from_secs(60);
    while table.row_set().size() != 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        graph.run_cycle();
    }
    assert!(
        !table.is_failed(),
        "the table failed after its broker came back"
    );
    let expected = [Some("before".to_owned()), Some("after".to_owned())];
    assert_eq!(strings(table.as_ref(), "V"), expected);
}

/// A table of the JSON values of `topic` from its beginning, without the key.
fn json_of(
    graph: &Arc<SteppedUpdateGraph>,
    topic: &str,
    columns: &[(&str, ColumnType)],
    mapping: &[(&str, &str)],
) -> Arc<dyn Table> {
    let owned = |(a, b): &(&str, &str)| ((*a).to_owned(), (*b).to_owned());
    let value = Spec::Json {
        columns: columns
            .iter()
            .map(|(name, column_type)| ((*name).to_owned(), column_type.clone()))
            .collect(),
        mapping: Some(mapping.iter().map(owned).collect()),
    };
    let mut options = options(topic, BEGINNING, value, TableType::Append);
    options.key = Spec::Ignore;
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    consume(shared, &options).unwrap()
}

/// The values the Java reference gave for the same records (task 7.4.3).
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn json_fields_are_found_by_pointers_and_coerced_as_java_does() {
    use arrow::datatypes::{Float64Type, Int16Type, TimestampNanosecondType, UInt16Type};
    let topic = fresh("json");
    produce(
        &topic,
        "none",
        &[
            r#"{"a": {"b": [10, {"c": "12.9"}]}, "x/y": 2.9, "s": {"k": [1.50, -0, 1e3]}, "c": "xy", "t": 1700000000, "f": "1.5f"}"#,
            r#"[7, "-3", {"a": 1}]"#,
            r#"{"c": 65535, "t": "2024-01-02T03:04:05Z", "x/y": "Infinity", "s": "😀"}"#,
        ],
    );
    let graph = SteppedUpdateGraph::new();
    let table = json_of(
        &graph,
        &topic,
        &[
            ("Deep", ColumnType::Int),
            ("Slash", ColumnType::Long),
            ("s", ColumnType::String),
            ("c", ColumnType::Char),
            ("t", ColumnType::Instant),
            ("f", ColumnType::Double),
            ("First", ColumnType::Short),
            ("Second", ColumnType::Int),
        ],
        &[
            ("/a/b/1/c", "Deep"),
            ("x/y", "Slash"),
            ("/0", "First"),
            ("/1", "Second"),
        ],
    );
    wait_for(&graph, table.as_ref(), 3);
    let column = |name: &str| {
        let index = table.definition().index_of(name).unwrap();
        table.column(index).chunk(&table.row_set()).unwrap()
    };
    let deep: Vec<_> = column("Deep").as_primitive::<Int32Type>().iter().collect();
    assert_eq!(deep, [Some(12), None, None]);
    assert_eq!(
        longs(table.as_ref(), "Slash"),
        [Some(2), None, Some(i64::MAX)]
    );
    let text = |text: &str| Some(text.to_owned());
    assert_eq!(
        strings(table.as_ref(), "s"),
        [text(r#"{"k":[1.50,0,1E+3]}"#), None, text("\u{1f600}")]
    );
    let chars: Vec<_> = column("c").as_primitive::<UInt16Type>().iter().collect();
    assert_eq!(chars, [Some(u16::from(b'x')), None, None]);
    let times = column("t");
    let times: Vec<_> = times
        .as_primitive::<TimestampNanosecondType>()
        .iter()
        .collect();
    assert_eq!(
        times,
        [
            Some(1_700_000_000_000_000_000),
            None,
            Some(1_704_164_645_000_000_000)
        ]
    );
    let doubles: Vec<_> = column("f").as_primitive::<Float64Type>().iter().collect();
    assert_eq!(doubles, [Some(1.5), None, None]);
    let first: Vec<_> = column("First").as_primitive::<Int16Type>().iter().collect();
    assert_eq!(first, [None, Some(7), None]);
    let second: Vec<_> = column("Second")
        .as_primitive::<Int32Type>()
        .iter()
        .collect();
    assert_eq!(second, [None, Some(-3), None]);
}

/// The failures the Java reference gave for the same records (task 7.4.3).
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn json_records_java_cannot_store_fail_the_table() {
    let graph = SteppedUpdateGraph::new();
    let deep = format!("{{\"v\": {}1{}}}", "[".repeat(1000), "]".repeat(1000));
    /// A record, its columns, its mapping and the failure Java gave.
    type Case<'a> = (
        &'a str,
        &'a [(&'a str, ColumnType)],
        &'a [(&'a str, &'a str)],
        &'a str,
    );
    let cases: [Case; 4] = [
        (
            r#"{"a": 1, "b": 2}"#,
            &[("X", ColumnType::Int), ("Y", ColumnType::Int)],
            &[("a", "X"), ("b", "X")],
            "java.lang.IllegalStateException: Publisher chunks have size mismatch: 1, 1, 1, 2, 1",
        ),
        (
            r#"{"v": {"a": 1}}"#,
            &[("B", ColumnType::Boolean)],
            &[("/v", "B")],
            r#"io.deephaven.UncheckedDeephavenException: value {"a":1} not recognized as Boolean for field /v"#,
        ),
        (
            r#"{"t": 9223372036854775807}"#,
            &[("t", ColumnType::Instant)],
            &[],
            "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: Numeric overflow \
             detected during conversion of 9223372036 to nanoseconds",
        ),
        (
            &deep,
            &[("w", ColumnType::Int)],
            &[],
            "Document nesting depth (1001) exceeds the maximum allowed (1000)",
        ),
    ];
    for (record, columns, mapping, failure) in cases {
        let topic = fresh("json_failure");
        produce(&topic, "none", &[record]);
        let table = json_of(&graph, &topic, columns, mapping);
        let message = wait_for_failure(&graph, &table);
        assert!(message.contains(failure), "{failure}: {message}");
    }
}

/// Behavior: org.apache.kafka.common.serialization.ByteArrayDeserializer
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_simple_byte_array_column_holds_the_bytes_of_each_value() {
    let topic = fresh("bytes");
    produce(&topic, "none", &["ab", "", "\u{e9}"]);
    let graph = SteppedUpdateGraph::new();
    let value = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::array(ColumnType::Byte)),
    };
    let mut options = options(&topic, BEGINNING, value, TableType::Append);
    options.key = Spec::Ignore;
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let table = consume(shared, &options).unwrap();
    wait_for(&graph, table.as_ref(), 3);
    let index = table.definition().index_of("V").unwrap();
    let column = table.column(index).chunk(&table.row_set()).unwrap();
    let cells: Vec<Vec<i8>> = column
        .as_list::<i32>()
        .iter()
        .map(|cell| {
            let cell = cell.expect("a value is never null here");
            let bytes = cell.as_primitive::<arrow::datatypes::Int8Type>();
            bytes.values().to_vec()
        })
        .collect();
    assert_eq!(cells, [vec![97, 98], vec![], vec![-61, -87]]);
}

/// A schema registry on this machine that answers the paths it is given and 404 to any
/// other, as `tests/avro.rs` serves one: the round trip needs no registry image. A request
/// is read to the end of its body, so that a producer's POST is answered.
fn serve_registry(answers: Vec<(String, String)>) -> String {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { return };
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let length = head
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
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
            let request = String::from_utf8_lossy(&request);
            let path = request.split(' ').nth(1).unwrap_or("");
            let found = answers.iter().find(|(p, _)| p == path);
            let (status, body) = match found {
                Some((_, body)) => (200, body.as_str()),
                None => (404, r#"{"error_code":40403,"message":"Schema not found"}"#),
            };
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    url
}

/// Behavior: io.deephaven.kafka.KafkaTools.Consume#avroSpec, with the key's schema given
/// as text and the value's fetched from the registry by subject; the value was written
/// with an older schema whose `n` is an int.
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn avro_keys_and_values_are_read_through_a_schema_registry() {
    let topic = fresh("avro");
    let key_schema = r#"{"type":"record","name":"K","fields":[{"name":"k","type":"string"}]}"#;
    let writer = r#"{"type":"record","name":"V","fields":[{"name":"n","type":"int"}]}"#;
    let reader = r#"{"type":"record","name":"V","fields":[{"name":"n","type":"long"},{"name":"s","type":["null","string"],"default":null}]}"#;
    let schema = |text: &str| format!(r#"{{"schema":"{}"}}"#, text.replace('"', "\\\""));
    let registry = serve_registry(vec![
        (
            format!("/subjects/{topic}-value/versions/latest"),
            r#"{"id":3,"version":2}"#.to_owned(),
        ),
        (
            "/schemas/ids/3?fetchMaxId=false&subject=".to_owned(),
            schema(reader),
        ),
        (
            format!("/schemas/ids/1?fetchMaxId=false&subject={topic}-key"),
            schema(key_schema),
        ),
        (
            format!("/schemas/ids/2?fetchMaxId=false&subject={topic}-value"),
            schema(writer),
        ),
    ]);
    let framed = |id: i32, body: &[u8]| [&[0][..], &id.to_be_bytes(), body].concat();
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .create()
        .unwrap();
    // Zig-zag: "a" is length 1 (2); 5 is 10 and -1 is 1.
    for (key, value) in [(&b"\x02a"[..], &b"\x0a"[..]), (b"\x02b", b"\x01")] {
        let (key, value) = (framed(1, key), framed(2, value));
        let record = BaseRecord::to(&topic)
            .partition(0)
            .key(&key)
            .payload(&value);
        producer.send(record).map_err(|(error, _)| error).unwrap();
    }
    producer.flush(Duration::from_secs(60)).unwrap();

    let graph = SteppedUpdateGraph::new();
    let value = Spec::Avro {
        schema: AvroSchema::Registry {
            subject: format!("{topic}-value"),
            version: Some("latest".to_owned()),
        },
        mapping: AvroMapping::Direct,
    };
    let mut options = options(&topic, BEGINNING, value, TableType::Append);
    options.key = Spec::Avro {
        schema: AvroSchema::Text(key_schema.to_owned()),
        mapping: AvroMapping::Only(vec![("k".to_owned(), Some("K".to_owned()))]),
    };
    options
        .config
        .push(("schema.registry.url".to_owned(), registry));
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let table = consume(shared, &options).unwrap();
    wait_for(&graph, table.as_ref(), 2);
    assert_eq!(
        strings(table.as_ref(), "K"),
        [Some("a".to_owned()), Some("b".to_owned())]
    );
    assert_eq!(longs(table.as_ref(), "n"), [Some(5), Some(-1)]);
    assert_eq!(strings(table.as_ref(), "s"), [None, None]);
}

/// Behavior: io.deephaven.kafka.KafkaTools.Consume#protobufSpec, the value's schema fetched
/// from the registry with the schema it imports, records in Confluent's framing with the
/// message indexes; a message left out of a record is null below it.
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn protobuf_values_are_read_through_a_schema_registry() {
    let topic = fresh("protobuf");
    let quoted = |text: &str| text.replace('"', "\\\"").replace('\n', "\\n");
    let dep = "syntax = \"proto3\";\npackage dep;\nmessage In { int32 x = 1; }\n";
    let main = "syntax = \"proto3\";\npackage m;\nimport \"dep.proto\";\n\
                message Other { int32 o = 1; }\n\
                message V { string s = 1; dep.In in = 2; repeated int64 r = 3; }\n";
    let registry = serve_registry(vec![
        (
            format!("/subjects/{topic}-value/versions/latest"),
            r#"{"id":7,"version":1,"schemaType":"PROTOBUF"}"#.to_owned(),
        ),
        (
            format!("/schemas/ids/7?fetchMaxId=false&subject={topic}-value"),
            format!(
                r#"{{"schemaType":"PROTOBUF","schema":"{}","references":[{{"name":"dep.proto","subject":"dep","version":1}}]}}"#,
                quoted(main)
            ),
        ),
        (
            "/subjects/dep/versions/1?deleted=true".to_owned(),
            format!(r#"{{"schemaType":"PROTOBUF","schema":"{}"}}"#, quoted(dep)),
        ),
    ]);
    // Magic byte, schema id 7, the indexes [1] (count 1 and index 1, zig-zag), then V.
    let framed = |body: &[u8]| [&[0, 0, 0, 0, 7, 2, 2][..], body].concat();
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .create()
        .unwrap();
    // { s: "a", in: { x: 5 }, r: [1, 2] } and { s: "b" }
    let values = [
        framed(b"\x0a\x01a\x12\x02\x08\x05\x1a\x02\x01\x02"),
        framed(b"\x0a\x01b"),
    ];
    for value in &values {
        let record = BaseRecord::<(), [u8]>::to(&topic)
            .partition(0)
            .payload(value);
        producer.send(record).map_err(|(error, _)| error).unwrap();
    }
    producer.flush(Duration::from_secs(60)).unwrap();

    let graph = SteppedUpdateGraph::new();
    let value = Spec::Protobuf {
        subject: format!("{topic}-value"),
        version: None,
        message: Some("V".to_owned()),
        include: None,
        protocol: ProtobufProtocol::Serdes,
    };
    let mut options = options(&topic, BEGINNING, value, TableType::Append);
    options.key = Spec::Ignore;
    options
        .config
        .push(("schema.registry.url".to_owned(), registry));
    let shared: Arc<dyn UpdateGraph> = graph.clone();
    let table = consume(shared, &options).unwrap();
    wait_for(&graph, table.as_ref(), 2);
    assert_eq!(
        strings(table.as_ref(), "s"),
        [Some("a".to_owned()), Some("b".to_owned())]
    );
    let column = |name: &str| {
        let index = table.definition().index_of(name).unwrap();
        table.column(index).chunk(&table.row_set()).unwrap()
    };
    let x: Vec<Option<i32>> = column("in_x").as_primitive::<Int32Type>().iter().collect();
    assert_eq!(x, [Some(5), None]);
    let r: Vec<Vec<i64>> = column("r")
        .as_list::<i32>()
        .iter()
        .map(|list| list.unwrap().as_primitive::<Int64Type>().values().to_vec())
        .collect();
    assert_eq!(r, [vec![1, 2], vec![]]);
}

// Producing. Every record expected here is what the Java reference published to a broker
// for the same table (docs/KAFKA.md, "Producing").

fn produce_options(topic: &str, key: ProduceSpec, value: ProduceSpec) -> ProduceOptions {
    ProduceOptions {
        config: vec![("bootstrap.servers".to_owned(), broker())],
        topic: topic.to_owned(),
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

fn simple(column: &str) -> ProduceSpec {
    ProduceSpec::Simple(column.to_owned())
}

fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
    let columns = columns
        .iter()
        .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
        .collect();
    TableDefinition::new(columns).unwrap()
}

fn texts(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// A static table of keys `K` and values `V`: `v` and the key.
fn keyed(keys: &[&str]) -> Arc<dyn Table> {
    let values: Vec<String> = keys.iter().map(|key| format!("v{key}")).collect();
    let values: Vec<Option<&str>> = values.iter().map(|value| Some(value.as_str())).collect();
    let keys: Vec<Option<&str>> = keys.iter().map(|key| Some(*key)).collect();
    ReferenceTable::new_static(
        definition(&[("K", ColumnType::String), ("V", ColumnType::String)]),
        RowSet::flat(keys.len() as u64),
        vec![texts(&keys), texts(&values)],
    )
}

type Record = (i32, Option<String>, Option<String>);

/// The partition, key and value of the records of `topic` from its beginning, until
/// `count` have been read or nothing more came for two seconds; at most twenty seconds.
fn records_of(topic: &str, count: usize) -> Vec<Record> {
    let records = timed_records_of(topic, count);
    let records = records
        .into_iter()
        .map(|(partition, _, key, value)| (partition, key, value));
    records.collect()
}

/// A record's partition, timestamp in milliseconds, key and value.
type TimedRecord = (i32, Option<i64>, Option<String>, Option<String>);

/// As [`records_of`], with each record's timestamp.
fn timed_records_of(topic: &str, count: usize) -> Vec<TimedRecord> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker())
        .set("group.id", fresh("reader"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
    let text = |bytes: Option<&[u8]>| bytes.map(|b| String::from_utf8(b.to_vec()).unwrap());
    let mut records = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = Instant::now();
    while records.len() < count && Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(200)) {
            Some(message) => {
                let message = message.unwrap();
                let record = (
                    message.partition(),
                    message.timestamp().to_millis(),
                    text(message.key()),
                    text(message.payload()),
                );
                records.push(record);
                last = Instant::now();
            }
            None if !records.is_empty() && last.elapsed() > Duration::from_secs(2) => break,
            None => {}
        }
    }
    records
}

/// A table's rows go to a topic and come back as they were; a removed row comes back as a
/// record of its key and no value (`PublishToKafka`; probes `p1`, `r4`).
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_table_published_to_a_topic_is_consumed_back() {
    let topic = fresh("round_trip");
    let graph = SteppedUpdateGraph::new();
    let script = ScriptedTable::new(
        graph.clone(),
        definition(&[
            ("K", ColumnType::String),
            ("N", ColumnType::Long),
            ("S", ColumnType::String),
        ]),
        RowSet::flat(2),
        vec![
            texts(&[Some("k1"), Some("k2")]),
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            texts(&[Some("a"), None]),
        ],
    );
    let value = ProduceSpec::Json(JsonProduceSpec {
        include: Some(vec!["N".to_owned(), "S".to_owned()]),
        ..JsonProduceSpec::default()
    });
    let published = produce_options(&topic, simple("K"), value);
    let publication = oxhaven_kafka::produce(script.table(), &published).unwrap();
    script.push(TableChange::new().remove([0]));
    graph.run_cycle();
    assert_eq!(publication.failure(), None);

    let consumed = SteppedUpdateGraph::new();
    let text = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::String),
    };
    let shared: Arc<dyn UpdateGraph> = consumed.clone();
    let back = consume(shared, &options(&topic, BEGINNING, text, TableType::Append)).unwrap();
    wait_for(&consumed, back.as_ref(), 3);
    let some = |text: &str| Some(text.to_owned());
    assert_eq!(
        strings(back.as_ref(), "K"),
        [some("k1"), some("k2"), some("k1")]
    );
    assert_eq!(
        strings(back.as_ref(), "V"),
        [some(r#"{"N":1,"S":"a"}"#), some(r#"{"N":2}"#), None]
    );
    publication.stop().unwrap();
}

/// A key's partition is Java's: `murmur2` of its bytes (probe `p3`: of a topic of two
/// partitions, keys `a`, `b`, `c` and `e` go to partition 0, `d` and `f` to 1), unless
/// `partition` chooses one for every record.
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn records_go_to_the_partition_of_their_key_or_the_one_chosen() {
    // tests/broker.sh creates this topic with two partitions; a run finds its own records
    // by their values.
    let topic = "oxhaven_produce_two";
    let keys = ["a", "b", "c", "d", "e", "f"];
    let options = produce_options(topic, simple("K"), simple("V"));
    oxhaven_kafka::produce(keyed(&keys), &options).unwrap();
    let run = fresh("run");
    let chosen: Vec<String> = keys.iter().map(|key| format!("{run}_{key}")).collect();
    let chosen: Vec<&str> = chosen.iter().map(String::as_str).collect();
    let mut one = produce_options(topic, simple("K"), simple("V"));
    one.partition = Some(1);
    oxhaven_kafka::produce(keyed(&chosen), &one).unwrap();

    let records = records_of(topic, usize::MAX);
    // The last record of each key: earlier runs published the same keys.
    let partition_of = |key: &str| {
        let record = records
            .iter()
            .rev()
            .find(|(_, k, _)| k.as_deref() == Some(key));
        let record = record.unwrap_or_else(|| panic!("{key} was not read: {records:?}"));
        assert_eq!(record.2, Some(format!("v{key}")));
        record.0
    };
    let partitions: Vec<i32> = keys.iter().map(|key| partition_of(key)).collect();
    assert_eq!(partitions, [0, 0, 0, 1, 0, 1]);
    for key in &chosen {
        assert_eq!(partition_of(key), 1, "{key}");
    }
}

/// A partition the topic does not have, or a broker that does not answer: the records
/// fail after `max.block.ms` with Java's `TimeoutException`, and so does the call (probes
/// `p4`, `cbx`).
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn a_partition_or_a_broker_that_is_not_there_times_out() {
    let mut options = produce_options("oxhaven_produce_two", simple("K"), simple("V"));
    options.partition = Some(5);
    options
        .config
        .push(("max.block.ms".to_owned(), "500".to_owned()));
    let started = Instant::now();
    let error = oxhaven_kafka::produce(keyed(&["a", "b"]), &options)
        .err()
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(500));
    assert_eq!(
        error.to_string(),
        "io.deephaven.kafka.publish.KafkaPublisherException: KafkaProducer reported send \
         failure: org.apache.kafka.common.errors.TimeoutException: Partition 5 of topic \
         oxhaven_produce_two with partition count 2 is not present in metadata after 500 ms."
    );

    let topic = fresh("unreachable");
    let mut options = produce_options(&topic, ProduceSpec::Ignore, simple("V"));
    // Nothing listens on port 1 of this machine.
    options.config = vec![
        ("bootstrap.servers".to_owned(), "127.0.0.1:1".to_owned()),
        ("max.block.ms".to_owned(), "500".to_owned()),
    ];
    let error = oxhaven_kafka::produce(keyed(&["a"]), &options)
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        format!(
            "io.deephaven.kafka.publish.KafkaPublisherException: KafkaProducer reported send \
             failure: org.apache.kafka.common.errors.TimeoutException: Topic {topic} not \
             present in metadata after 500 ms."
        )
    );
}

/// Rows published as Avro records go out in Confluent's wire format, with the id the
/// registry gives the schema under the topic's subject, and come back through the
/// consumer (`AvroImpl`, Confluent's `KafkaAvroSerializer`; probes a1, a2).
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn avro_records_published_through_a_registry_are_consumed_back() {
    let topic = fresh("avro_out");
    let schema = r#"{"type":"record","name":"V","fields":[{"name":"N","type":"long"},{"name":"S","type":["null","string"]}]}"#;
    let quoted = format!(r#"{{"schema":"{}"}}"#, schema.replace('"', "\\\""));
    let registry = serve_registry(vec![
        (
            "/subjects/v/versions/latest".to_owned(),
            r#"{"id":3,"version":1}"#.to_owned(),
        ),
        (
            "/schemas/ids/3?fetchMaxId=false&subject=".to_owned(),
            quoted.clone(),
        ),
        (
            format!("/subjects/{topic}-value/versions?normalize=false"),
            r#"{"id":9}"#.to_owned(),
        ),
        (
            format!("/subjects/{topic}-value/versions/latest"),
            r#"{"id":9,"version":1}"#.to_owned(),
        ),
        (
            "/schemas/ids/9?fetchMaxId=false&subject=".to_owned(),
            quoted.clone(),
        ),
        (
            format!("/schemas/ids/9?fetchMaxId=false&subject={topic}-value"),
            quoted,
        ),
    ]);
    let table = ReferenceTable::new_static(
        definition(&[
            ("K", ColumnType::String),
            ("N", ColumnType::Long),
            ("S", ColumnType::String),
        ]),
        RowSet::flat(2),
        vec![
            texts(&[Some("k1"), Some("k2")]),
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            texts(&[Some("a"), None]),
        ],
    );
    let value = ProduceSpec::Avro(AvroProduceSpec {
        subject: "v".to_owned(),
        version: Some("latest".to_owned()),
        ..AvroProduceSpec::default()
    });
    let mut published = produce_options(&topic, simple("K"), value);
    published
        .config
        .push(("schema.registry.url".to_owned(), registry.clone()));
    oxhaven_kafka::produce(table, &published).unwrap();
    // Zig-zag: N 1 is 2 and 2 is 4; S is branch 1 (2) and "a" of length 1 (2), or branch 0.
    assert_eq!(
        records_of(&topic, 2),
        [
            (
                0,
                Some("k1".to_owned()),
                Some("\0\0\0\0\x09\x02\x02\x02a".to_owned())
            ),
            (
                0,
                Some("k2".to_owned()),
                Some("\0\0\0\0\x09\x04\x00".to_owned())
            ),
        ]
    );

    let consumed = SteppedUpdateGraph::new();
    let value = Spec::Avro {
        schema: AvroSchema::Registry {
            subject: format!("{topic}-value"),
            version: Some("latest".to_owned()),
        },
        mapping: AvroMapping::Direct,
    };
    let mut options = options(&topic, BEGINNING, value, TableType::Append);
    options
        .config
        .push(("schema.registry.url".to_owned(), registry));
    let shared: Arc<dyn UpdateGraph> = consumed.clone();
    let back = consume(shared, &options).unwrap();
    wait_for(&consumed, back.as_ref(), 2);
    let some = |text: &str| Some(text.to_owned());
    assert_eq!(strings(back.as_ref(), "K"), [some("k1"), some("k2")]);
    assert_eq!(longs(back.as_ref(), "N"), [Some(1), Some(2)]);
    assert_eq!(strings(back.as_ref(), "S"), [some("a"), None]);
}

/// A record's topic, partition and timestamp come from its row's columns, and a null cell
/// takes the call's topic, the key's partition and the time of sending (`PublishToKafka`;
/// probes c1, c2: a timestamp is the column's nanoseconds truncated to milliseconds).
#[test]
#[ignore = "needs a broker: crates/oxhaven-kafka/tests/broker.sh"]
fn records_take_their_topic_partition_and_timestamp_from_columns() {
    let run = fresh("columns");
    let other = format!("{run}_other");
    let keys = [format!("{run}_a"), format!("{run}_b")];
    let table = ReferenceTable::new_static(
        definition(&[
            ("Top", ColumnType::String),
            ("P", ColumnType::Int),
            ("T", ColumnType::Instant),
            ("K", ColumnType::String),
            ("V", ColumnType::String),
        ]),
        RowSet::flat(2),
        vec![
            texts(&[None, Some(&other)]),
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(1_704_164_645_123_999_999), None])
                    .with_timezone("UTC"),
            ),
            texts(&[Some(&keys[0]), Some(&keys[1])]),
            texts(&[Some("va"), Some("vb")]),
        ],
    );
    // tests/broker.sh creates this topic with two partitions.
    let mut options = produce_options("oxhaven_produce_two", simple("K"), simple("V"));
    options.topic_column = Some("Top".to_owned());
    options.partition_column = Some("P".to_owned());
    options.timestamp_column = Some("T".to_owned());
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    let before = before.unwrap().as_millis() as i64;
    oxhaven_kafka::produce(table, &options).unwrap();

    let first = timed_records_of("oxhaven_produce_two", usize::MAX);
    let first = first
        .iter()
        .find(|record| record.2.as_deref() == Some(&keys[0]));
    assert_eq!(
        first,
        Some(&(
            1,
            Some(1_704_164_645_123),
            Some(keys[0].clone()),
            Some("va".to_owned())
        ))
    );
    let second = timed_records_of(&other, 1);
    assert_eq!(second.len(), 1, "{second:?}");
    let (partition, timestamp, key, value) = second[0].clone();
    assert_eq!(
        (partition, key, value),
        (0, Some(keys[1].clone()), Some("vb".to_owned()))
    );
    assert!(
        timestamp.is_some_and(|t| t >= before),
        "{timestamp:?} is before {before}"
    );
}
