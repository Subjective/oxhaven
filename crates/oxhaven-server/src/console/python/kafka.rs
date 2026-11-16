//! `deephaven.stream.kafka` reaches the Kafka client here.
//!
//! Java has no gRPC call that consumes a topic, so this is not a table service operation:
//! the Python module calls [`kafka_consume`] with the arguments of
//! `deephaven.stream.kafka.consumer.consume`, and the work is `oxhaven_kafka::consume`
//! (`docs/KAFKA.md`). [`kafka_topics`] is `deephaven.stream.kafka.topics`.

use std::sync::Arc;

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_kafka::{ConsumeOptions, InitialOffset, KafkaError, Offsets, Spec, TableType};
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::Value;

use super::TableHandle;
use crate::schema::column_type_for_java_names;

fn exception(error: KafkaError) -> PyErr {
    let text = error.to_string();
    match error {
        KafkaError::NotPorted { .. } => PyNotImplementedError::new_err(text),
        _ => PyRuntimeError::new_err(text),
    }
}

fn column_type(name: &str) -> PyResult<ColumnType> {
    column_type_for_java_names(name, None)
        .ok_or_else(|| PyValueError::new_err(format!("the engine has no column type for {name}")))
}

/// A key or value spec, as `deephaven.stream.kafka.consumer.KeyValueSpec` writes it:
/// `{"kind": "ignore"}`, `{"kind": "simple", "column": name or null, "type": Java class
/// name or null}` or `{"kind": "json", "columns": [[name, Java class name]], "mapping":
/// [[field, column]] or null}`.
fn spec(json: &str) -> PyResult<Spec> {
    let bad = || PyValueError::new_err(format!("not a key or value spec: {json}"));
    let value: Value = serde_json::from_str(json).map_err(|_| bad())?;
    let text = |value: &Value| value.as_str().map(str::to_owned).ok_or_else(bad);
    let pairs = |value: &Value| -> PyResult<Vec<(String, String)>> {
        let pair = |pair: &Value| Ok((text(&pair[0])?, text(&pair[1])?));
        value.as_array().ok_or_else(bad)?.iter().map(pair).collect()
    };
    match value["kind"].as_str() {
        Some("ignore") => Ok(Spec::Ignore),
        Some("simple") => Ok(Spec::Simple {
            column: value["column"].as_str().map(str::to_owned),
            data_type: value["type"].as_str().map(column_type).transpose()?,
        }),
        Some("json") => {
            let mut columns = Vec::new();
            for (name, type_name) in pairs(&value["columns"])? {
                columns.push((name, column_type(&type_name)?));
            }
            let mapping = match &value["mapping"] {
                Value::Null => None,
                mapping => Some(pairs(mapping)?),
            };
            Ok(Spec::Json { columns, mapping })
        }
        _ => Err(bad()),
    }
}

/// Consumes `topic` into a table. `all_offsets` is where to start in every partition, as
/// Java's `SEEK_TO_BEGINNING`, `DONT_SEEK` or `SEEK_TO_END`; without it `offsets` says so
/// per partition. `table_type` is `blink`, `append` or `ring`, the last with `capacity`.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(super) fn kafka_consume(
    py: Python<'_>,
    config: Vec<(String, String)>,
    topic: &str,
    partitions: Option<Vec<i32>>,
    all_offsets: Option<i64>,
    offsets: Vec<(i32, i64)>,
    key_spec: &str,
    value_spec: &str,
    table_type: &str,
    capacity: i32,
) -> PyResult<TableHandle> {
    let options = ConsumeOptions {
        config,
        topic: topic.to_owned(),
        partitions,
        offsets: match all_offsets {
            Some(offset) => Offsets::Everywhere(InitialOffset::from_java(offset)),
            None => Offsets::ByPartition(offsets),
        },
        key: spec(key_spec)?,
        value: spec(value_spec)?,
        table_type: match table_type {
            "blink" => TableType::Blink,
            "append" => TableType::Append,
            "ring" => TableType::Ring(capacity),
            other => return Err(PyValueError::new_err(format!("no table type {other}"))),
        },
    };
    let graph: Arc<dyn UpdateGraph> = super::runtime()?.context.graph.clone();
    let table = py
        .detach(|| oxhaven_kafka::consume(graph, &options))
        .map_err(exception)?;
    Ok(TableHandle { table })
}

/// The names of the topics of the broker `config` names.
#[pyfunction]
pub(super) fn kafka_topics(py: Python<'_>, config: Vec<(String, String)>) -> PyResult<Vec<String>> {
    py.detach(|| oxhaven_kafka::topics(&config))
        .map_err(exception)
}

#[cfg(test)]
mod tests {
    use super::super::tests::ONE_INTERPRETER;
    use crate::test_support::TestServer;

    /// No broker is needed: the calls that reach one name a closed port of this machine.
    #[tokio::test]
    async fn deephaven_kafka_consumer_checks_refuses_and_raises_dherror() {
        let _turn = ONE_INTERPRETER.lock().await;
        let server = TestServer::start_with(|state| super::super::start(state).unwrap()).await;
        let console = server.state.console.clone().unwrap();
        const SETUP: &str = "from deephaven import dtypes\n\
            from deephaven.stream.kafka import consumer as kc\n\
            CFG = {'bootstrap.servers': '127.0.0.1:1', 'default.api.timeout.ms': '1500'}\n\
            BEGIN = kc.ALL_PARTITIONS_SEEK_TO_BEGINNING\n\
            V = kc.simple_spec('V', dtypes.int32)\n";
        let failure = |code: &str, message: &str| {
            let error = console.evaluate(&format!("{SETUP}{code}")).error;
            let error = error.expect("the call fails");
            let last = error.trim_end().lines().last().unwrap().to_string();
            let prefix = format!("deephaven.dherror.DHError: {message} : ");
            let text = last.strip_prefix(&prefix);
            text.unwrap_or_else(|| panic!("not a DHError of {message}: {last}"))
                .to_string()
        };
        const CONSUME: &str = "failed to consume a Kafka stream.";
        const IGNORED_KEY: &str = "value_spec=V, key_spec=kc.KeyValueSpec.IGNORE";

        let checked = console.evaluate(&format!(
            "{SETUP}\
             assert (kc.SEEK_TO_BEGINNING, kc.DONT_SEEK, kc.SEEK_TO_END) == (-1, -2, -3)\n\
             assert kc.ALL_PARTITIONS_SEEK_TO_END == {{-1: kc.SEEK_TO_END}}\n\
             spec = kc.json_spec({{'A': dtypes.long, 'B': dtypes.string}}, mapping={{'a': 'A'}})\n\
             assert spec.j_object == {{'kind': 'json', 'columns': [('A', 'long'), \
             ('B', 'java.lang.String')], 'mapping': [('a', 'A')]}}, spec.j_object\n\
             assert V.j_object == {{'kind': 'simple', 'column': 'V', 'type': 'int'}}\n\
             assert kc.TableType.ring(5).j_object == ('ring', 5)\n\
             assert kc.TableType.Append.j_object == ('append', 0)\n"
        ));
        assert_eq!(checked.error, None);

        let unreachable = failure(
            &format!("kc.consume(CFG, 't', offsets=BEGIN, {IGNORED_KEY})"),
            CONSUME,
        );
        assert!(
            unreachable.starts_with("no broker of bootstrap.servers=\"127.0.0.1:1\" answers"),
            "{unreachable}"
        );
        assert_eq!(
            failure(
                "kc.consume(CFG, 't', key_spec=kc.KeyValueSpec.IGNORE, \
                 value_spec=kc.KeyValueSpec.IGNORE)",
                CONSUME
            ),
            "at least one argument for 'key' or 'value' must be different from \
             KeyValueSpec.IGNORE"
        );
        assert_eq!(
            failure(
                &format!("kc.consume({{}}, 't', offsets=BEGIN, {IGNORED_KEY})"),
                CONSUME
            ),
            "org.apache.kafka.common.config.ConfigException: Missing required configuration \
             \"bootstrap.servers\" which has no default value."
        );
        let from_properties = failure("kc.consume(CFG, 't', offsets=BEGIN)", CONSUME);
        assert!(
            from_properties.contains("Unable to find the type for column 'KafkaKey' (key_spec)"),
            "{from_properties}"
        );
        assert_eq!(
            failure(
                &format!(
                    "kc.consume({{**CFG, 'security.protocol': 'SSL'}}, 't', offsets=BEGIN, \
                     {IGNORED_KEY})"
                ),
                CONSUME
            ),
            "not ported yet: \
             org.apache.kafka.common.network.ChannelBuilders#clientChannelBuilder: \
             security.protocol=SSL"
        );
        // Without `offsets` the call is no longer refused: it reaches for the broker too.
        let unsought = failure(&format!("kc.consume(CFG, 't', {IGNORED_KEY})"), CONSUME);
        assert!(unsought.starts_with("no broker of"), "{unsought}");
        let listed = failure(
            "from deephaven.stream.kafka import topics\ntopics(CFG)",
            "failed to list Kafka topics",
        );
        assert!(
            listed.starts_with("Failed to list Kafka Topics"),
            "{listed}"
        );
        assert_eq!(
            failure(
                &format!("kc.consume({{**CFG, 'enable.auto.commit': 'true'}}, 't', {IGNORED_KEY})"),
                CONSUME
            ),
            "org.apache.kafka.common.errors.InvalidConfigurationException: \
             enable.auto.commit cannot be set to true when default group id (null) is used."
        );
        // The reference hands the properties to Java as they are, and only text fits.
        for (value, refusal) in [
            ("5", "no matching Java method overloads found"),
            ("None", "java.lang.NullPointerException"),
        ] {
            let code = format!("kc.consume({{**CFG, 'max.poll.records': {value}}}, 't')");
            assert_eq!(failure(&code, CONSUME), refusal);
        }
        assert_eq!(
            failure("kc.consume_to_partitioned_table(CFG, 't')", CONSUME),
            "not ported: io.deephaven.kafka.KafkaTools#consumeToPartitionedTable"
        );
        assert_eq!(
            failure(
                "kc.avro_spec('schema')",
                "failed to create a Kafka key/value spec"
            ),
            "not ported: io.deephaven.kafka.KafkaTools.Consume#avroSpec"
        );
        for (call, java) in [
            (
                "kc.protobuf_spec(schema='s')",
                "io.deephaven.kafka.KafkaTools.Consume#protobufSpec",
            ),
            (
                "kc.object_processor_spec(None)",
                "io.deephaven.kafka.KafkaTools.Consume#objectProcessorSpec",
            ),
        ] {
            let error = console.evaluate(&format!("{SETUP}{call}")).error.unwrap();
            let last = error.trim_end().lines().last().unwrap().to_string();
            assert!(last.ends_with(&format!("not ported: {java}")), "{last}");
        }
    }
}
