//! Tables consumed from a scripted record source, on a graph the test steps.
//!
//! The broker tests are in `tests/broker.rs`.

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{Float64Type, Int32Type, Int64Type, TimestampNanosecondType};
use bytes::Bytes;
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_kafka::{consume_from, KafkaError, Plan, Record, RecordSource, Spec, TableType};
use oxhaven_testkit::ConsistencyChecker;

/// Returns what the test sends, one message per poll, and tells the test when a poll that
/// follows a message starts: everything sent before has then been handed to the table.
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

struct Topic {
    graph: Arc<SteppedUpdateGraph>,
    table: Arc<dyn Table>,
    checker: Arc<ConsistencyChecker>,
    failures: Arc<Failures>,
    script: Sender<Result<Vec<Record>, KafkaError>>,
    polled: Receiver<()>,
}

fn record(offset: i64, key: Option<&str>, value: Option<&[u8]>) -> Record {
    Record {
        partition: 2,
        offset,
        timestamp: 1_700_000_000_000 + offset,
        key: key.map(|key| Bytes::copy_from_slice(key.as_bytes())),
        value: value.map(Bytes::copy_from_slice),
    }
}

fn config(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    let owned = |(k, v): &(&str, &str)| (k.to_string(), v.to_string());
    pairs.iter().map(owned).collect()
}

fn simple(column: &str, data_type: ColumnType) -> Spec {
    Spec::Simple {
        column: Some(column.to_owned()),
        data_type: Some(data_type),
    }
}

impl Topic {
    fn new(plan: Plan, table_type: TableType) -> Self {
        let graph = SteppedUpdateGraph::new();
        let (script, script_end) = channel();
        let (polled_end, polled) = channel();
        let source = Scripted {
            script: script_end,
            polled: polled_end,
        };
        let shared: Arc<dyn UpdateGraph> = graph.clone();
        let table = consume_from(shared, plan, Box::new(source), table_type, "test").unwrap();
        let checker = ConsistencyChecker::attach(table.clone());
        let failures = Arc::new(Failures::default());
        let listener: Arc<dyn UpdateListener> = failures.clone();
        table.add_listener(&listener);
        Self {
            graph,
            table,
            checker,
            failures,
            script,
            polled,
        }
    }

    /// Hands `step` to the polling thread, waits until it has dealt with it, and runs a
    /// cycle.
    fn cycle(&self, step: Result<Vec<Record>, KafkaError>) {
        self.failing_cycle(step);
        self.checker.assert_consistent();
    }

    /// The same without the consistency check, which reports a failed table.
    fn failing_cycle(&self, step: Result<Vec<Record>, KafkaError>) {
        self.script.send(step).unwrap();
        // One poll takes the step; the start of the next (or the end of the thread, after
        // an error) says it was handed over.
        while self.polled.try_recv().is_ok() {}
        let _ = self.polled.recv_timeout(Duration::from_secs(10));
        let _ = self.polled.recv_timeout(Duration::from_secs(10));
        self.graph.run_cycle();
    }

    fn column(&self, name: &str) -> ArrayRef {
        let index = self.table.definition().index_of(name).unwrap();
        self.table
            .column(index)
            .chunk(&self.table.row_set())
            .unwrap()
    }

    fn longs(&self, name: &str) -> Vec<Option<i64>> {
        self.column(name)
            .as_primitive::<Int64Type>()
            .iter()
            .collect()
    }

    fn strings(&self, name: &str) -> Vec<Option<String>> {
        let column = self.column(name);
        let cells = column.as_string::<i32>().iter();
        cells.map(|cell| cell.map(str::to_owned)).collect()
    }
}

fn names(plan: &Plan) -> Vec<(String, ColumnType)> {
    let columns = plan.definition().columns().iter();
    columns
        .map(|c| (c.name.clone(), c.column_type.clone()))
        .collect()
}

#[test]
fn a_blink_table_shows_each_batch_for_one_cycle() {
    let plan = Plan::new("test", &[], &Spec::Ignore, &simple("V", ColumnType::String)).unwrap();
    assert_eq!(
        names(&plan),
        [
            ("KafkaPartition".to_owned(), ColumnType::Int),
            ("KafkaOffset".to_owned(), ColumnType::Long),
            ("KafkaTimestamp".to_owned(), ColumnType::Instant),
            ("V".to_owned(), ColumnType::String),
        ]
    );
    let topic = Topic::new(plan, TableType::Blink);
    assert!(topic.table.attributes().blink);
    assert!(topic.table.is_refreshing());
    assert_eq!(topic.table.row_set().size(), 0);

    topic.cycle(Ok(vec![
        record(0, None, Some(b"a")),
        record(1, None, None),
        record(2, None, Some("é".as_bytes())),
    ]));
    assert_eq!(topic.longs("KafkaOffset"), [Some(0), Some(1), Some(2)]);
    assert_eq!(
        topic.strings("V"),
        [Some("a".to_owned()), None, Some("é".to_owned())]
    );
    let partitions = topic.column("KafkaPartition");
    assert_eq!(partitions.as_primitive::<Int32Type>().values(), &[2, 2, 2]);
    let times = topic.column("KafkaTimestamp");
    let times = times.as_primitive::<TimestampNanosecondType>();
    assert_eq!(times.value(1), 1_700_000_000_001_000_000);

    topic.cycle(Ok(vec![record(3, None, Some(b"d"))]));
    assert_eq!(topic.longs("KafkaOffset"), [Some(3)]);
    assert_eq!(topic.strings("V"), [Some("d".to_owned())]);

    // Nothing arrived: the rows of the last cycle leave.
    topic.cycle(Ok(Vec::new()));
    assert_eq!(topic.table.row_set().size(), 0);
}

#[test]
fn an_append_only_table_keeps_every_row() {
    let properties = config(&[
        ("deephaven.timestamp.column.name", ""),
        ("deephaven.partition.column.name", ""),
        ("deephaven.offset.column.name", "Off"),
        ("deephaven.keybytes.column.name", "KeySize"),
        ("deephaven.valuebytes.column.name", "ValueSize"),
        ("deephaven.key.column.type", "string"),
        (
            "value.deserializer",
            "org.apache.kafka.common.serialization.LongDeserializer",
        ),
    ]);
    let from_properties = Spec::Simple {
        column: None,
        data_type: None,
    };
    let plan = Plan::new("test", &properties, &from_properties, &from_properties).unwrap();
    assert_eq!(
        names(&plan),
        [
            ("Off".to_owned(), ColumnType::Long),
            ("KeySize".to_owned(), ColumnType::Int),
            ("ValueSize".to_owned(), ColumnType::Int),
            ("KafkaKey".to_owned(), ColumnType::String),
            ("KafkaValue".to_owned(), ColumnType::Long),
        ]
    );
    let topic = Topic::new(plan, TableType::Append);
    let attributes = topic.table.attributes();
    assert!(attributes.append_only && attributes.add_only && !attributes.blink);

    topic.cycle(Ok(vec![
        record(0, Some("k0"), Some(&7i64.to_be_bytes())),
        record(1, None, Some(&i64::MIN.to_be_bytes())),
    ]));
    topic.cycle(Ok(Vec::new()));
    topic.cycle(Ok(vec![record(2, Some("k2"), None)]));
    assert_eq!(topic.longs("Off"), [Some(0), Some(1), Some(2)]);
    // Java's null long is a null cell.
    assert_eq!(topic.longs("KafkaValue"), [Some(7), None, None]);
    assert_eq!(
        topic.strings("KafkaKey"),
        [Some("k0".to_owned()), None, Some("k2".to_owned())]
    );
    let sizes = topic.column("ValueSize");
    let sizes: Vec<Option<i32>> = sizes.as_primitive::<Int32Type>().iter().collect();
    assert_eq!(sizes, [Some(8), Some(8), None]);
}

#[test]
fn a_ring_table_keeps_the_last_rows() {
    let plan = Plan::new("test", &[], &Spec::Ignore, &simple("V", ColumnType::String)).unwrap();
    let topic = Topic::new(plan, TableType::Ring(2));
    topic.cycle(Ok(vec![
        record(0, None, Some(b"a")),
        record(1, None, Some(b"b")),
        record(2, None, Some(b"c")),
    ]));
    topic.cycle(Ok(vec![record(3, None, Some(b"d"))]));
    assert_eq!(topic.longs("KafkaOffset"), [Some(2), Some(3)]);
}

#[test]
fn json_fields_fill_the_columns_named_for_them() {
    let value = Spec::Json {
        columns: vec![
            ("Sym".to_owned(), ColumnType::String),
            ("Qty".to_owned(), ColumnType::Int),
            ("Px".to_owned(), ColumnType::Double),
            ("Big".to_owned(), ColumnType::Long),
            ("Live".to_owned(), ColumnType::Boolean),
        ],
        mapping: Some(vec![
            ("symbol".to_owned(), "Sym".to_owned()),
            ("/a~1b".to_owned(), "Px".to_owned()),
        ]),
    };
    let plan = Plan::new("test", &[], &Spec::Ignore, &value).unwrap();
    let topic = Topic::new(plan, TableType::Append);
    topic.cycle(Ok(vec![
        record(
            0,
            None,
            Some(
                br#"{"symbol": "X", "Qty": 3, "a/b": 1.25, "Big": 9007199254740993, "Live": true}"#,
            ),
        ),
        record(
            1,
            None,
            Some(br#"{"Sym": "ignored", "Qty": 4294967297, "a/b": 2, "Live": "f"}"#),
        ),
        record(
            2,
            None,
            Some(br#"{"symbol": 1.50, "Qty": null, "Big": 2147483648}"#),
        ),
        record(3, None, Some(b"17")),
    ]));
    assert_eq!(
        topic.strings("Sym"),
        [Some("X".to_owned()), None, Some("1.50".to_owned()), None]
    );
    let quantities = topic.column("Qty");
    let quantities: Vec<Option<i32>> = quantities.as_primitive::<Int32Type>().iter().collect();
    // 4294967297 keeps its low 32 bits.
    assert_eq!(quantities, [Some(3), Some(1), None, None]);
    let prices = topic.column("Px");
    let prices: Vec<Option<f64>> = prices.as_primitive::<Float64Type>().iter().collect();
    assert_eq!(prices, [Some(1.25), Some(2.0), None, None]);
    assert_eq!(
        topic.longs("Big"),
        [Some(9_007_199_254_740_993), None, Some(2_147_483_648), None]
    );
    let live = topic.column("Live");
    let live: Vec<Option<bool>> = live.as_boolean().iter().collect();
    assert_eq!(live, [Some(true), Some(false), None, None]);
}

fn failure_of(topic: &Topic) -> String {
    assert!(topic.table.is_failed());
    let failure = topic.failures.0.lock().unwrap().clone();
    failure.expect("the listener was told of the failure")
}

#[test]
fn a_record_that_cannot_become_a_row_fails_the_table() {
    let value = Spec::Json {
        columns: vec![("Qty".to_owned(), ColumnType::Int)],
        mapping: None,
    };
    let plan = Plan::new("test", &[], &Spec::Ignore, &value).unwrap();
    let topic = Topic::new(plan, TableType::Blink);
    topic.cycle(Ok(vec![record(0, None, Some(br#"{"Qty": 1}"#))]));
    assert!(!topic.table.is_failed());
    topic.failing_cycle(Ok(vec![record(1, None, Some(br#"{"Qty": "#))]));
    let message = failure_of(&topic);
    assert!(
        message.contains("Failed to parse JSON string."),
        "{message}"
    );

    let plan = Plan::new("test", &[], &Spec::Ignore, &simple("V", ColumnType::Int)).unwrap();
    let topic = Topic::new(plan, TableType::Append);
    topic.failing_cycle(Ok(vec![record(0, None, Some(b"12345"))]));
    let message = failure_of(&topic);
    assert_eq!(
        message,
        "org.apache.kafka.common.errors.RecordDeserializationException: Error deserializing \
         VALUE for partition test-2 at offset 0. If needed, please seek past the record to \
         continue consumption."
    );
}

#[test]
fn a_source_that_fails_fails_the_table() {
    let plan = Plan::new("test", &[], &Spec::Ignore, &simple("V", ColumnType::String)).unwrap();
    let topic = Topic::new(plan, TableType::Blink);
    topic.failing_cycle(Err(KafkaError::Broker("the broker left".to_owned())));
    assert_eq!(failure_of(&topic), "the broker left");
}

#[test]
fn what_is_not_built_is_refused_by_the_name_of_what_java_does() {
    let refused = |key: &Spec, value: &Spec, properties: &[(&str, &str)]| {
        Plan::new("test", &config(properties), key, value)
            .unwrap_err()
            .to_string()
    };
    let json = |column_type| Spec::Json {
        columns: vec![("C".to_owned(), column_type)],
        mapping: None,
    };
    let from_properties = Spec::Simple {
        column: None,
        data_type: None,
    };
    assert_eq!(
        refused(&Spec::Ignore, &Spec::Ignore, &[]),
        "java.lang.IllegalArgumentException: can't ignore both key and value: keySpec and \
         valueSpec can't both be ignore specs"
    );
    assert_eq!(
        refused(&Spec::Ignore, &json(ColumnType::Instant), &[]),
        "not ported yet: io.deephaven.kafka.ingest.JsonNodeInstantFieldCopier: the JSON column \
         C of type Instant"
    );
    assert!(refused(&Spec::Ignore, &json(ColumnType::Char), &[])
        .starts_with("not ported yet: io.deephaven.kafka.ingest.JsonNodeCharFieldCopier"));
    assert!(
        refused(&Spec::Ignore, &simple("V", ColumnType::Boolean), &[])
            .starts_with("not ported yet: io.deephaven.kafka.SimpleImpl.SerDeserVisitor")
    );
    assert!(refused(
        &Spec::Ignore,
        &from_properties,
        &[("deephaven.value.column.type", "byte[]")]
    )
    .starts_with("not ported yet: io.deephaven.kafka.SimpleImpl.SerDeserVisitor"));
    assert_eq!(
        refused(
            &Spec::Ignore,
            &from_properties,
            &[("deephaven.value.column.type", "bool")]
        ),
        "java.lang.IllegalArgumentException: Property deephaven.value.column.type value bool \
         not supported"
    );
    assert!(refused(&Spec::Ignore, &from_properties, &[])
        .contains("Unable to find the type for column 'KafkaValue' (value_spec)"));
    let nested = Spec::Json {
        columns: vec![("C".to_owned(), ColumnType::Int)],
        mapping: Some(vec![("/a/b".to_owned(), "C".to_owned())]),
    };
    assert!(refused(&Spec::Ignore, &nested, &[])
        .starts_with("not ported yet: com.fasterxml.jackson.databind.JsonNode#at(JsonPointer)"));
    let duplicate = Spec::Json {
        columns: vec![("KafkaOffset".to_owned(), ColumnType::Int)],
        mapping: None,
    };
    assert!(refused(&Spec::Ignore, &duplicate, &[]).contains("duplicate names [KafkaOffset]"));
}
