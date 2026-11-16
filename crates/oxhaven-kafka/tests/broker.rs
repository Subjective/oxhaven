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
use arrow::datatypes::{Int32Type, Int64Type};
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{consume, ConsumeOptions, InitialOffset, Offsets, Spec, TableType};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

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
