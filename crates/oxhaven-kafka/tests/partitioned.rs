//! Partitioned tables consumed from a scripted record source, on a graph the test steps.
//!
//! The broker tests are in `tests/broker.rs`.

use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{Int32Type, Int64Type};
use bytes::Bytes;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnType, TableClass};
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::partitioned::PartitionedTable;
use oxhaven_kafka::{
    consume_partitioned_from, KafkaError, Plan, Record, RecordSource, Spec, TableType,
};
use oxhaven_testkit::ConsistencyChecker;

/// Returns what the test sends, one message per poll, and tells the test when a poll
/// starts: everything sent before has then been handed to the tables.
struct Scripted {
    script: Receiver<Result<Vec<Record>, KafkaError>>,
    polled: Sender<()>,
}

impl RecordSource for Scripted {
    fn poll(&mut self) -> Result<Vec<Record>, KafkaError> {
        let _ = self.polled.send(());
        match self.script.recv_timeout(Duration::from_millis(20)) {
            Ok(step) => step,
            Err(_) => Ok(Vec::new()),
        }
    }
}

struct Topic {
    graph: Arc<SteppedUpdateGraph>,
    partitioned: PartitionedTable,
    script: Sender<Result<Vec<Record>, KafkaError>>,
    polled: Receiver<()>,
}

fn record(partition: i32, offset: i64, value: Option<&str>) -> Record {
    Record {
        partition,
        offset,
        timestamp: 1_700_000_000_000 + offset,
        key: None,
        value: value.map(|value| Bytes::copy_from_slice(value.as_bytes())),
    }
}

fn plan() -> Plan {
    let value = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::String),
    };
    Plan::new("test", &[], &Spec::Ignore, &value).unwrap()
}

fn plan_of_ints() -> Plan {
    let value = Spec::Simple {
        column: Some("V".to_owned()),
        data_type: Some(ColumnType::Int),
    };
    Plan::new("test", &[], &Spec::Ignore, &value).unwrap()
}

impl Topic {
    fn new(plan: Plan, partitions: &[i32], table_type: TableType) -> Self {
        let graph = SteppedUpdateGraph::new();
        let (script, script_end) = channel();
        let (polled_end, polled) = channel();
        let source = Scripted {
            script: script_end,
            polled: polled_end,
        };
        let shared: Arc<dyn UpdateGraph> = graph.clone();
        let partitioned =
            consume_partitioned_from(shared, plan, Box::new(source), partitions, table_type, "t")
                .unwrap();
        Self {
            graph,
            partitioned,
            script,
            polled,
        }
    }

    /// Hands `step` to the polling thread, waits until it has dealt with it, and runs a
    /// cycle.
    fn cycle(&self, step: Result<Vec<Record>, KafkaError>) {
        self.script.send(step).unwrap();
        while self.polled.try_recv().is_ok() {}
        let _ = self.polled.recv_timeout(Duration::from_secs(10));
        let _ = self.polled.recv_timeout(Duration::from_secs(10));
        self.graph.run_cycle();
    }

    fn constituent(&self, at: usize) -> Arc<dyn Table> {
        self.partitioned.constituents().unwrap()[at].clone()
    }
}

fn column(table: &Arc<dyn Table>, name: &str) -> ArrayRef {
    let index = table.definition().index_of(name).unwrap();
    table.column(index).chunk(&table.row_set()).unwrap()
}

fn strings(table: &Arc<dyn Table>) -> Vec<Option<String>> {
    let column = column(table, "V");
    let cells = column.as_string::<i32>().iter();
    cells.map(|cell| cell.map(str::to_owned)).collect()
}

fn ints(table: &Arc<dyn Table>, name: &str) -> Vec<i32> {
    column(table, name)
        .as_primitive::<Int32Type>()
        .values()
        .to_vec()
}

#[test]
fn each_partition_is_a_constituent_from_the_next_cycle() {
    let topic = Topic::new(plan(), &[1, 0], TableType::Blink);
    let partitioned = &topic.partitioned;
    let underlying = partitioned.table().clone();
    let definition = underlying.definition();
    let names: Vec<_> = definition
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.column_type.clone()))
        .collect();
    assert_eq!(
        names,
        [
            ("Partition", ColumnType::Int),
            ("Table", ColumnType::Table(TableClass::Table)),
        ]
    );
    assert_eq!(partitioned.key_columns(), ["Partition"]);
    assert!(partitioned.unique_keys());
    assert_eq!(partitioned.constituent_column(), "Table");
    assert!(partitioned.constituent_changes_permitted());
    assert_eq!(partitioned.constituent_definition(), plan().definition());
    assert!(underlying.is_refreshing() && underlying.is_flat());
    assert!(!underlying.attributes().add_only && !underlying.attributes().blink);
    // Java's table of tables shows its constituents from the next cycle on.
    assert_eq!(underlying.row_set().size(), 0);
    let checker = ConsistencyChecker::attach(underlying.clone());
    let merged = partitioned.merge().unwrap();
    let merge_checker = ConsistencyChecker::attach(merged.clone());

    topic.cycle(Ok(vec![record(0, 0, Some("a")), record(1, 0, Some("b"))]));
    checker.assert_consistent();
    merge_checker.assert_consistent();
    assert_eq!(ints(&underlying, "Partition"), [1, 0]);
    let (first, second) = (topic.constituent(0), topic.constituent(1));
    assert!(first.attributes().blink && first.is_refreshing());
    assert_eq!(strings(&first), [Some("b".to_owned())]);
    assert_eq!(strings(&second), [Some("a".to_owned())]);
    assert_eq!(ints(&first, "KafkaPartition"), [1]);
    // The merge shows the constituents in the order of the table's rows.
    assert_eq!(
        strings(&merged),
        [Some("b".to_owned()), Some("a".to_owned())]
    );

    topic.cycle(Ok(vec![
        record(0, 1, Some("c")),
        record(0, 2, None),
        record(1, 1, Some("d")),
    ]));
    merge_checker.assert_consistent();
    assert_eq!(strings(&first), [Some("d".to_owned())]);
    assert_eq!(strings(&second), [Some("c".to_owned()), None]);
    let offsets = column(&second, "KafkaOffset");
    assert_eq!(offsets.as_primitive::<Int64Type>().values(), &[1, 2]);
    // No more rows of the table of tables.
    assert_eq!(underlying.row_set().size(), 2);

    topic.cycle(Ok(Vec::new()));
    merge_checker.assert_consistent();
    assert_eq!(merged.row_set().size(), 0);
}

#[test]
fn append_and_ring_constituents_keep_their_rows() {
    let topic = Topic::new(plan(), &[0, 1], TableType::Append);
    let merged = topic.partitioned.merge().unwrap();
    let checker = ConsistencyChecker::attach(merged.clone());
    topic.cycle(Ok(vec![record(0, 0, Some("a")), record(1, 0, Some("b"))]));
    topic.cycle(Ok(vec![record(0, 1, Some("c"))]));
    checker.assert_consistent();
    let appended = topic.constituent(0);
    assert!(appended.attributes().append_only);
    assert_eq!(
        strings(&appended),
        [Some("a".to_owned()), Some("c".to_owned())]
    );
    assert_eq!(
        strings(&merged),
        [
            Some("a".to_owned()),
            Some("c".to_owned()),
            Some("b".to_owned())
        ]
    );

    let topic = Topic::new(plan(), &[0], TableType::Ring(2));
    let merged = topic.partitioned.merge().unwrap();
    let checker = ConsistencyChecker::attach(merged.clone());
    topic.cycle(Ok(vec![
        record(0, 0, Some("a")),
        record(0, 1, Some("b")),
        record(0, 2, Some("c")),
    ]));
    topic.cycle(Ok(vec![record(0, 3, Some("d"))]));
    checker.assert_consistent();
    assert_eq!(
        strings(&topic.constituent(0)),
        [Some("c".to_owned()), Some("d".to_owned())]
    );
}

#[test]
fn a_record_that_cannot_be_read_fails_its_partition_alone() {
    let topic = Topic::new(plan_of_ints(), &[0, 1], TableType::Append);
    topic.cycle(Ok(vec![
        record(0, 0, Some("abcd")),
        record(1, 0, Some("efgh")),
    ]));
    let (zero, one) = (topic.constituent(0), topic.constituent(1));
    assert_eq!(zero.row_set().size(), 1);
    let merged = topic.partitioned.merge().unwrap();
    // Partition 0 is read first; the bad record of partition 1 fails its table and ends
    // the consumption, as KafkaIngester#pollOnce does.
    topic.cycle(Ok(vec![
        record(0, 1, Some("ijkl")),
        record(1, 1, Some("xy")),
    ]));
    assert!(!zero.is_failed());
    assert_eq!(zero.row_set().size(), 2);
    assert!(one.is_failed());
    assert!(merged.is_failed());
    assert!(!topic.partitioned.table().is_failed());
    // Nothing is read any more.
    assert_eq!(
        topic.polled.recv_timeout(Duration::from_secs(5)),
        Err(RecvTimeoutError::Disconnected)
    );
}

#[test]
fn a_failed_poll_fails_every_constituent() {
    let topic = Topic::new(plan(), &[0, 1], TableType::Blink);
    topic.cycle(Ok(Vec::new()));
    let (zero, one) = (topic.constituent(0), topic.constituent(1));
    topic.cycle(Err(KafkaError::Broker("gone".to_owned())));
    assert!(zero.is_failed() && one.is_failed());
}

#[test]
fn records_of_partitions_not_read_are_dropped() {
    let topic = Topic::new(plan(), &[1], TableType::Append);
    topic.cycle(Ok(vec![record(0, 0, Some("a")), record(1, 0, Some("b"))]));
    assert_eq!(ints(topic.partitioned.table(), "Partition"), [1]);
    assert_eq!(strings(&topic.constituent(0)), [Some("b".to_owned())]);
}

#[test]
fn no_partition_is_java_s_null_pointer_exception() {
    let graph = SteppedUpdateGraph::new();
    let (_script, script_end) = channel();
    let (polled_end, _polled) = channel();
    let source = Scripted {
        script: script_end,
        polled: polled_end,
    };
    let made = consume_partitioned_from(
        graph as Arc<dyn UpdateGraph>,
        plan(),
        Box::new(source),
        &[],
        TableType::Blink,
        "t",
    );
    let error = made.err().unwrap().to_string();
    assert!(
        error.starts_with("java.lang.NullPointerException: Cannot invoke"),
        "{error}"
    );
}

#[test]
fn dropping_the_partitioned_table_stops_the_consumption() {
    let topic = Topic::new(plan(), &[0, 1], TableType::Ring(3));
    topic.cycle(Ok(vec![record(0, 0, Some("a"))]));
    let Topic {
        graph,
        partitioned,
        script,
        polled,
    } = topic;
    drop(partitioned);
    graph.run_cycle();
    let mut ended = false;
    for _ in 0..500 {
        if let Err(RecvTimeoutError::Disconnected) = polled.recv_timeout(Duration::from_millis(20))
        {
            ended = true;
            break;
        }
    }
    assert!(ended, "the polling thread goes on");
    drop(script);
}

#[test]
fn a_merge_fails_with_a_failed_constituent() {
    let topic = Topic::new(plan_of_ints(), &[0, 1], TableType::Append);
    // Made before any constituent shows; the one of partition 1 fails in the cycle that
    // shows it, and Java's merge fails as it cannot listen to it.
    let early = topic.partitioned.merge().unwrap();
    topic.cycle(Ok(vec![
        record(0, 0, Some("abcd")),
        record(1, 0, Some("xy")),
    ]));
    assert!(topic.constituent(1).is_failed());
    assert!(early.is_failed());
    let error = topic.partitioned.merge().err().unwrap().to_string();
    assert_eq!(
        error,
        "io.deephaven.engine.exceptions.TableAlreadyFailedException: Can not listen to failed \
         table QueryTable"
    );
}
