//! Topics consumed from a real broker. Ignored unless asked for: `tests/broker.sh` starts a
//! broker in Docker on this machine, fills the topics these tests read and runs them.
//!
//! The broker's address comes from `OXHAVEN_KAFKA_BROKER` and must be on this machine: a
//! test never connects anywhere else.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::AsArray;
use arrow::datatypes::{Int32Type, Int64Type};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{consume, ConsumeOptions, InitialOffset, KafkaError, Offsets, Spec, TableType};

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
    let table = consume(shared.clone(), &missing).unwrap();
    graph.run_cycle();
    assert_eq!(table.row_set().size(), 0);
    assert!(!table.is_failed());

    let committed = Offsets::Everywhere(InitialOffset::DontSeek);
    let committed = options("oxhaven_one", committed, json(), TableType::Append);
    let Err(refused) = consume(shared, &committed) else {
        panic!("DONT_SEEK is not built");
    };
    assert!(matches!(&refused, KafkaError::NotPorted { java, .. }
        if java == "org.apache.kafka.clients.consumer.KafkaConsumer#position"));
}
