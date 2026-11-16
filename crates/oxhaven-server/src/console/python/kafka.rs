//! `deephaven.stream.kafka` reaches the Kafka client here.
//!
//! Java has no gRPC call that consumes a topic, so this is not a table service operation:
//! the Python module calls [`kafka_consume`] with the arguments of
//! `deephaven.stream.kafka.consumer.consume`, and the work is `oxhaven_kafka::consume`
//! (`docs/KAFKA.md`). [`kafka_topics`] is `deephaven.stream.kafka.topics`.

use std::sync::Arc;

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_kafka::{
    AvroMapping, AvroProduceSpec, AvroSchema, ConsumeOptions, InitialOffset, JsonProduceSpec,
    KafkaError, Offsets, ProduceOptions, ProduceSpec, Publication, Spec, TableType,
};
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
        Some("avro") => {
            let schema = match (&value["schema"], &value["subject"]) {
                (Value::String(text), _) => AvroSchema::Text(text.clone()),
                (_, Value::String(subject)) => AvroSchema::Registry {
                    subject: subject.clone(),
                    version: value["version"].as_str().map(str::to_owned),
                },
                _ => return Err(bad()),
            };
            let avro_pairs = |value: &Value| -> PyResult<Vec<(String, Option<String>)>> {
                let pair =
                    |pair: &Value| Ok((text(&pair[0])?, pair[1].as_str().map(str::to_owned)));
                value.as_array().ok_or_else(bad)?.iter().map(pair).collect()
            };
            let mapping = match &value["mapping"] {
                Value::String(direct) if direct == "direct" => AvroMapping::Direct,
                Value::Object(kind) => match (kind.get("identity"), kind.get("only")) {
                    (Some(pairs), None) => AvroMapping::Identity(avro_pairs(pairs)?),
                    (None, Some(pairs)) => AvroMapping::Only(avro_pairs(pairs)?),
                    _ => return Err(bad()),
                },
                _ => return Err(bad()),
            };
            Ok(Spec::Avro { schema, mapping })
        }
        _ => Err(bad()),
    }
}

/// Reads an Avro schema given as text, as `avro_spec` does when it is called.
#[pyfunction]
pub(super) fn kafka_avro_schema(text: &str) -> PyResult<()> {
    oxhaven_kafka::check_avro_schema(text).map_err(exception)
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

/// A key or value spec of `deephaven.stream.kafka.producer.KeyValueSpec`:
/// `{"kind": "ignore"}`, `{"kind": "simple", "column": name}` or `{"kind": "json",
/// "include": [names] or null, "mapping": [[column, field]] or null, "nested_delim": text
/// or null, "output_nulls": bool, "timestamp_field": text or null}` or `{"kind": "avro",
/// "subject", "version", "mapping": [[field, column]] or null, "timestamp_field",
/// "include_only", "exclude": [names] or null, "publish_schema": bool, "namespace",
/// "column_properties": [[name, value]]}`.
fn produce_spec(json: &str) -> PyResult<ProduceSpec> {
    let bad = || PyValueError::new_err(format!("not a key or value spec: {json}"));
    let value: Value = serde_json::from_str(json).map_err(|_| bad())?;
    let text = |value: &Value| value.as_str().map(str::to_owned).ok_or_else(bad);
    let optional = |value: &Value| match value {
        Value::Null => Ok(None),
        value => text(value).map(Some),
    };
    match value["kind"].as_str() {
        Some("ignore") => Ok(ProduceSpec::Ignore),
        Some("simple") => Ok(ProduceSpec::Simple(text(&value["column"])?)),
        Some("json") => {
            let include = match &value["include"] {
                Value::Null => None,
                names => Some(
                    names
                        .as_array()
                        .ok_or_else(bad)?
                        .iter()
                        .map(text)
                        .collect::<PyResult<_>>()?,
                ),
            };
            let mapping = match &value["mapping"] {
                Value::Null => Vec::new(),
                pairs => pairs
                    .as_array()
                    .ok_or_else(bad)?
                    .iter()
                    .map(|pair| Ok((text(&pair[0])?, text(&pair[1])?)))
                    .collect::<PyResult<_>>()?,
            };
            Ok(ProduceSpec::Json(JsonProduceSpec {
                include,
                exclude: None,
                mapping,
                nested_delimiter: optional(&value["nested_delim"])?,
                output_nulls: value["output_nulls"].as_bool().ok_or_else(bad)?,
                timestamp_field: optional(&value["timestamp_field"])?,
            }))
        }
        Some("avro") => {
            let names = |value: &Value| -> PyResult<Option<Vec<String>>> {
                match value {
                    Value::Null => Ok(None),
                    names => names
                        .as_array()
                        .ok_or_else(bad)?
                        .iter()
                        .map(text)
                        .collect::<PyResult<_>>()
                        .map(Some),
                }
            };
            let pairs = |value: &Value| -> PyResult<Option<Vec<(String, String)>>> {
                match value {
                    Value::Null => Ok(None),
                    pairs => pairs
                        .as_array()
                        .ok_or_else(bad)?
                        .iter()
                        .map(|pair| Ok((text(&pair[0])?, text(&pair[1])?)))
                        .collect::<PyResult<_>>()
                        .map(Some),
                }
            };
            Ok(ProduceSpec::Avro(AvroProduceSpec {
                subject: text(&value["subject"])?,
                version: optional(&value["version"])?,
                mapping: pairs(&value["mapping"])?,
                timestamp_field: optional(&value["timestamp_field"])?,
                include_only: names(&value["include_only"])?,
                exclude: names(&value["exclude"])?,
                publish_schema: value["publish_schema"].as_bool().ok_or_else(bad)?,
                namespace: optional(&value["namespace"])?,
                column_properties: pairs(&value["column_properties"])?.unwrap_or_default(),
            }))
        }
        _ => Err(bad()),
    }
}

/// A table being published; `stop` is the callback `produce` returns.
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct KafkaPublication {
    publication: Publication,
}

#[pymethods]
impl KafkaPublication {
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.publication.stop()).map_err(exception)
    }

    /// Why publishing stopped by itself, if it did.
    fn failure(&self) -> Option<String> {
        self.publication.failure()
    }
}

/// Publishes `table` to `topic` with the arguments of
/// `deephaven.stream.kafka.producer.produce`.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(super) fn kafka_produce(
    py: Python<'_>,
    table: &TableHandle,
    config: Vec<(String, String)>,
    topic: Option<&str>,
    partition: Option<i32>,
    key_spec: &str,
    value_spec: &str,
    last_by: bool,
    publish_initial: bool,
    columns: (Option<String>, Option<String>, Option<String>),
) -> PyResult<KafkaPublication> {
    let (topic_column, partition_column, timestamp_column) = columns;
    let options = ProduceOptions {
        config,
        topic: topic.unwrap_or_default().to_owned(),
        partition,
        topic_column,
        partition_column,
        timestamp_column,
        key: produce_spec(key_spec)?,
        value: produce_spec(value_spec)?,
        last_by,
        publish_initial,
    };
    let table = table.table.clone();
    let publication = py
        .detach(|| oxhaven_kafka::produce(table, &options))
        .map_err(exception)?;
    Ok(KafkaPublication { publication })
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
             spec = kc.json_spec({{'C': dtypes.char, 'T': dtypes.Instant}})\n\
             assert spec.j_object['columns'] == [('C', 'char'), ('T', 'java.time.Instant')]\n\
             assert kc.simple_spec('V', dtypes.byte_array).j_object['type'] == 'byte[]'\n\
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
        assert_eq!(
            failure(
                "kc.consume(CFG, 't', offsets=BEGIN, key_spec=kc.KeyValueSpec.IGNORE, \
                 value_spec=kc.json_spec({'X': dtypes.int32}, mapping={'a': 'Z'}))",
                CONSUME
            ),
            "java.lang.IllegalArgumentException: Column not found in Deephaven table: -1"
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
        // A schema given as text is read when the spec is made; a subject is not looked up
        // until `consume`.
        let specs = console.evaluate(&format!(
            "{SETUP}\
             s = kc.avro_spec('subj')\n\
             assert s.j_object == {{'kind': 'avro', 'mapping': 'direct', 'subject': 'subj', \
             'version': 'latest'}}, s.j_object\n\
             s = kc.avro_spec('subj', schema_version='2', mapping={{'a': 'A'}}, mapped_only=True)\n\
             assert s.j_object['mapping'] == {{'only': [('a', 'A')]}}, s.j_object\n\
             assert s.j_object['version'] == '2'\n\
             text = '{{\"type\": \"record\", \"name\": \"R\", \"fields\": []}}'\n\
             s = kc.avro_spec(text, mapping={{'a': None}})\n\
             assert s.j_object == {{'kind': 'avro', 'mapping': {{'identity': [('a', None)]}}, \
             'schema': text}}, s.j_object\n\
             assert kc.avro_spec(text, mapping={{}}).j_object['mapping'] == 'direct'\n"
        ));
        assert_eq!(specs.error, None);
        assert_eq!(
            failure(
                "kc.avro_spec('{\"type\": \"record\", \"fields\": []}')",
                "failed to create a Kafka key/value spec"
            ),
            "org.apache.avro.SchemaParseException: No name in schema: \
             {\"type\":\"record\",\"fields\":[]}"
        );
        assert_eq!(
            failure(
                "kc.consume(CFG, 't', key_spec=kc.KeyValueSpec.IGNORE, \
                 value_spec=kc.avro_spec('{\"type\": \"record\", \"name\": \"R\", \"fields\": []}'))",
                CONSUME
            ),
            "org.apache.kafka.common.config.ConfigException: Missing required configuration \
             \"schema.registry.url\" which has no default value."
        );
        assert_eq!(
            failure(
                "kc.consume({**CFG, 'schema.registry.url': 'https://127.0.0.1:1'}, 't', \
                 key_spec=kc.KeyValueSpec.IGNORE, value_spec=kc.avro_spec('s'))",
                CONSUME
            ),
            "not ported yet: \
             io.confluent.kafka.schemaregistry.client.rest.RestService#setSslSocketFactory: \
             a schema registry over TLS (https://127.0.0.1:1)"
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

    /// No broker is needed: a table without rows sends nothing, and the one call that
    /// sends names a closed port of this machine. The texts are the reference's.
    #[tokio::test]
    async fn deephaven_kafka_producer_checks_refuses_and_raises_dherror() {
        let _turn = ONE_INTERPRETER.lock().await;
        let server = TestServer::start_with(|state| super::super::start(state).unwrap()).await;
        let console = server.state.console.clone().unwrap();
        const SETUP: &str = "from deephaven import empty_table, time_table\n\
            from deephaven.stream.kafka import producer as kp\n\
            CFG = {'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': '500'}\n\
            EMPTY = empty_table(0).update(['K = `k`', 'V = i'])\n\
            V = kp.simple_spec('V')\n\
            IGNORE = kp.KeyValueSpec.IGNORE\n";
        let failure = |code: &str, message: &str| {
            let error = console.evaluate(&format!("{SETUP}{code}")).error;
            let error = error.expect("the call fails");
            let last = error.trim_end().lines().last().unwrap().to_string();
            let prefix = format!("deephaven.dherror.DHError: {message} : ");
            let text = last.strip_prefix(&prefix);
            text.unwrap_or_else(|| panic!("not a DHError of {message}: {last}"))
                .to_string()
        };
        const PRODUCE: &str = "failed to start producing Kafka messages.";
        const SPEC: &str = "failed to create a Kafka key/value spec.";

        let checked = console.evaluate(&format!(
            "{SETUP}\
             assert IGNORE.j_object == {{'kind': 'ignore'}}\n\
             assert V.j_object == {{'kind': 'simple', 'column': 'V'}}\n\
             spec = kp.json_spec(['V', 'K'], mapping={{'V': 'a.b'}}, nested_delim='.', \
             output_nulls=True, timestamp_field='ts')\n\
             assert spec.j_object == {{'kind': 'json', 'include': ['V', 'K'], \
             'mapping': [['V', 'a.b']], 'nested_delim': '.', 'output_nulls': True, \
             'timestamp_field': 'ts'}}, spec.j_object\n\
             assert kp.json_spec().j_object['include'] is None\n\
             stop = kp.produce(EMPTY, CFG, 't', key_spec=kp.simple_spec('K'), value_spec=V)\n\
             stop()\n\
             try:\n    stop()\n    raise AssertionError('stopped twice')\n\
             except Exception as e:\n    assert 'failed to stop publishing to Kafka' in str(e), e\n"
        ));
        assert_eq!(checked.error, None);

        for (code, refusal) in [
            (
                "kp.produce(EMPTY, CFG, 't', key_spec=IGNORE, value_spec=IGNORE)",
                "at least one argument for 'key_spec' or 'value_spec' must be different from \
                 KeyValueSpec.IGNORE",
            ),
            (
                "kp.produce(EMPTY, CFG, 't', IGNORE, V, publish_initial=False)",
                "publish_initial == False and table.is_refreshing == False",
            ),
            (
                "kp.produce(EMPTY, CFG, '', IGNORE, V)",
                "java.lang.IllegalArgumentException: Must set topic or topicColumn (or both)",
            ),
            (
                "kp.produce(EMPTY, CFG, 't', IGNORE, V, topic_col='V')",
                "java.lang.ClassCastException: Cannot convert [V] of type int to type \
                 java.lang.CharSequence",
            ),
            (
                "kp.produce(EMPTY, CFG, 't', IGNORE, V, partition_col='K')",
                "java.lang.ClassCastException: Cannot convert [K] of type java.lang.String to \
                 type int",
            ),
            (
                "kp.produce(EMPTY, CFG, 't', IGNORE, V, timestamp_col='Nope')",
                "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names \
                 [Nope], available column names are [K, V]",
            ),
            (
                "kp.produce(EMPTY, CFG, None, IGNORE, V)",
                "java.lang.IllegalArgumentException: Must set topic or topicColumn (or both)",
            ),
            (
                "kp.produce(EMPTY, CFG, 't', IGNORE, kp.avro_spec('s'))",
                "org.apache.kafka.common.config.ConfigException: Missing required \
                 configuration \"schema.registry.url\" which has no default value.",
            ),
            (
                "kp.produce(EMPTY, {}, 't', IGNORE, V)",
                "org.apache.kafka.common.config.ConfigException: Missing required \
                 configuration \"bootstrap.servers\" which has no default value.",
            ),
            (
                "kp.produce(EMPTY, {**CFG, 'acks': '2'}, 't', IGNORE, V)",
                "org.apache.kafka.common.config.ConfigException: Invalid value 2 for \
                 configuration acks: String must be one of: all, -1, 0, 1",
            ),
        ] {
            assert_eq!(failure(code, PRODUCE), refusal, "{code}");
        }
        let missing = failure(
            "kp.produce(EMPTY, CFG, 't', IGNORE, kp.simple_spec('X'))",
            PRODUCE,
        );
        assert!(missing.contains("because \"cd\" is null"), "{missing}");
        let gzip = failure(
            "kp.produce(EMPTY, {**CFG, 'compression.type': 'gzip'}, 't', IGNORE, V)",
            PRODUCE,
        );
        assert!(
            gzip.starts_with("not ported yet: org.apache.kafka.common.compress.GzipCompression"),
            "{gzip}"
        );
        let unreachable = failure(
            "kp.produce(empty_table(2).update('V = i'), CFG, 't', IGNORE, V)",
            PRODUCE,
        );
        assert_eq!(
            unreachable,
            "io.deephaven.kafka.publish.KafkaPublisherException: KafkaProducer reported send \
             failure: org.apache.kafka.common.errors.TimeoutException: Topic t not present in \
             metadata after 500 ms."
        );

        assert_eq!(
            failure("kp.json_spec(['V'], exclude_columns=['K'])", SPEC),
            "One of include_columns and exclude_columns must be None."
        );
        assert_eq!(
            failure("kp.json_spec(exclude_columns=['K'])", SPEC),
            "no matching Java method overloads found"
        );
        assert_eq!(
            failure(
                "kp.avro_spec('s', schema_version='2', publish_schema=True)",
                SPEC
            ),
            "java.lang.IllegalArgumentException: schemaVersion must be null or \"latest\" when \
             publishSchema=true"
        );
        let checked = console.evaluate(&format!(
            "{SETUP}\
             spec = kp.avro_spec('s', field_to_col_mapping={{'a': 'K'}}, timestamp_field='ts', \
             exclude_columns=['V'], publish_schema=True, schema_namespace='n', \
             column_properties={{'K.x': 'y'}})\n\
             assert spec.j_object == {{'kind': 'avro', 'subject': 's', 'version': 'latest', \
             'mapping': [['a', 'K']], 'timestamp_field': 'ts', 'include_only': None, \
             'exclude': ['V'], 'publish_schema': True, 'namespace': 'n', \
             'column_properties': [('K.x', 'y')]}}, spec.j_object\n"
        ));
        assert_eq!(checked.error, None);
    }
}
