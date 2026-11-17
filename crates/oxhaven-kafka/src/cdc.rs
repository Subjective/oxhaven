//! Change data capture: a topic of Debezium change events as the database table they
//! describe (`deephaven.stream.kafka.cdc`).
//!
//! Java's `CdcTools` is a few table operations over an Avro consumption: the value's and
//! the key's schemas are fetched from the registry, the topic is read from its beginning
//! into a blink table, that table is narrowed to the key's columns, the `after` record's
//! columns under their own names and `op`, and, unless a blink table is asked for, the
//! last row of each key is kept and a key whose last event is a delete (`op` is `d`) is
//! left out. A tombstone (a record with a key and no value) is a row whose `op` is null,
//! so it is kept: the deleted key shows again, with null columns, as in Java.
//! `docs/KAFKA.md`, "Change data capture", has what was probed.

use std::sync::Arc;

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::table::Table;
use oxhaven_engine::ops::agg::{aggregate_all_with, AggSpec, Pair};
use oxhaven_engine::ops::{filter, select};
use oxhaven_expr::Compiler;

use crate::avro_spec::{AvroMapping, AvroSchema};
use crate::registry::Registry;
use crate::stream::consume_from;
use crate::{client, not_ported, InitialOffset, KafkaError, Plan, RecordSource, Spec, TableType};

/// Behavior: io.deephaven.kafka.CdcTools#CDC_AFTER_COLUMN_PREFIX, `after` and
/// io.deephaven.kafka.KafkaTools#NESTED_FIELD_COLUMN_NAME_SEPARATOR
const AFTER_PREFIX: &str = "after__";
/// Behavior: io.deephaven.kafka.CdcTools#CDC_OP_COLUMN_NAME
const OP_COLUMN: &str = "op";
/// Behavior: io.deephaven.kafka.CdcTools#CDC_DELETE_OP_VALUE, in the filter
/// `consumeToTable` applies
const NOT_DELETED: &str = "op != `d`";
/// Behavior: io.deephaven.kafka.KafkaTools#AVRO_LATEST_VERSION
const LATEST: &str = "latest";

/// Which topic a stream of change events is on, and the registry subjects and versions of
/// its keys' and values' schemas. `None` is Java's null.
///
/// Behavior: io.deephaven.kafka.CdcTools.CdcSpec
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CdcSpec {
    pub topic: Option<String>,
    pub key_subject: Option<String>,
    pub key_version: Option<String>,
    pub value_subject: Option<String>,
    pub value_version: Option<String>,
}

impl CdcSpec {
    /// The stream of a database table by Debezium's names: the topic
    /// `server.database.table`, and the subjects of that topic with `-key` and `-value`, at
    /// their latest versions.
    ///
    /// Behavior: io.deephaven.kafka.CdcTools#cdcShortSpec, #cdcTopicName,
    /// #cdcKeyAvroSchemaName and #cdcValueAvroSchemaName
    pub fn short(server: &str, database: &str, table: &str) -> Self {
        let topic = format!("{server}.{database}.{table}");
        Self {
            key_subject: Some(format!("{topic}-key")),
            key_version: Some(LATEST.to_owned()),
            value_subject: Some(format!("{topic}-value")),
            value_version: Some(LATEST.to_owned()),
            topic: Some(topic),
        }
    }
}

/// The arguments of [`consume_cdc`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CdcOptions {
    /// The properties, for the consumer and for the registry.
    pub config: Vec<(String, String)>,
    pub spec: CdcSpec,
    /// The partitions to read; all of them if `None`.
    pub partitions: Option<Vec<i32>>,
    /// The change events as a blink table, with their `op`, rather than the current rows.
    pub blink: bool,
    /// Columns to leave out of the result.
    pub drop_columns: Vec<String>,
}

/// Java's `NullPointerException` for a null subject: Confluent's client refuses it before
/// it asks (probes long_sub_none, raw_sub_none).
fn null_subject() -> KafkaError {
    KafkaError::Invalid("java.lang.NullPointerException".to_owned())
}

/// A null topic, which Java's consumer sends to the broker and which fails with the
/// broker's error (probes long_topic_none, raw_topic_none).
fn null_topic() -> KafkaError {
    not_ported(
        "org.apache.kafka.clients.consumer.KafkaConsumer#partitionsFor",
        "a null topic, which Java asks the broker about and fails with its \
         UnknownServerException",
    )
}

fn avro_text(text: String) -> Spec {
    Spec::Avro {
        schema: AvroSchema::Text(text),
        mapping: AvroMapping::Direct,
    }
}

/// The plan of the stream of change events and the names of the key's fields, from the
/// value's schema and then the key's, fetched by one client of the registry.
///
/// Behavior: io.deephaven.kafka.CdcTools#consumeToTable, up to its call of
/// `KafkaTools#consumeToTable` with `avroSpec` of each schema; #fieldNames
fn stream_plan(options: &CdcOptions) -> Result<(Plan, Vec<String>), KafkaError> {
    let spec = &options.spec;
    let registry = Registry::from_config(&options.config)?;
    let fetch = |subject: &Option<String>, version: &Option<String>| {
        let subject = subject.as_deref().ok_or_else(null_subject)?;
        registry.subject_schema_text(subject, version.as_deref())
    };
    let value = fetch(&spec.value_subject, &spec.value_version)?;
    let key = fetch(&spec.key_subject, &spec.key_version)?;
    let key_schema = crate::avro::parse(&key).map_err(KafkaError::Invalid)?;
    let topic = spec.topic.as_deref().unwrap_or_default();
    let plan = Plan::new(topic, &options.config, &avro_text(key), &avro_text(value))?;
    // `Plan::new` refuses a key's schema that is not a record.
    let fields = key_schema
        .record_fields(&key_schema.root)
        .unwrap_or_default()
        .iter()
        .map(|field| field.name.clone())
        .collect();
    Ok((plan, fields))
}

fn invalid(error: impl ToString) -> KafkaError {
    KafkaError::Invalid(error.to_string())
}

/// The table of a database table from the stream of its change events, `stream`, a blink
/// table of the columns of the key's and the value's schemas: the current row of each key,
/// or with `blink` the events themselves with their `op`. `key_fields` names the key's
/// columns; `drop_columns` lists columns to leave out.
///
/// Behavior: io.deephaven.kafka.CdcTools#consumeToTable after the consumption,
/// #narrowerStreamingTableViewExpressions and #dbTableColumnNames
pub fn current_rows(
    stream: &Arc<dyn Table>,
    key_fields: &[String],
    blink: bool,
    drop_columns: &[String],
) -> Result<Arc<dyn Table>, KafkaError> {
    let parent: Vec<&str> = stream
        .definition()
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut view = Vec::new();
    let mut made = Vec::new();
    for column in parent.iter().filter_map(|n| n.strip_prefix(AFTER_PREFIX)) {
        if key_fields.iter().any(|key| key == column) {
            view.push(column.to_owned());
        } else {
            view.push(format!("{column}={AFTER_PREFIX}{column}"));
            if !parent.contains(&column) && !made.contains(&column) {
                made.push(column);
            }
        }
    }
    if !parent.contains(&OP_COLUMN) {
        // Behavior: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer,
        // which lists the parent's columns and those the view made before `op`
        // (probe noop).
        let available: Vec<&str> = parent.iter().chain(&made).copied().collect();
        return Err(invalid(format!(
            "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names \
             [{OP_COLUMN}], available column names are [{}]",
            available.join(", ")
        )));
    }
    view.push(OP_COLUMN.to_owned());
    let narrow = select::view(stream, &view, &Compiler).map_err(invalid)?;
    if blink {
        return select::drop_columns(&narrow, drop_columns).map_err(invalid);
    }
    let columns: Vec<&str> = narrow
        .definition()
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let missing: Vec<&str> = key_fields
        .iter()
        .map(String::as_str)
        .filter(|key| !columns.contains(key))
        .collect();
    if !missing.is_empty() {
        // Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#checkGroupByColumns
        // (probe keymiss)
        return Err(invalid(format!(
            "io.deephaven.engine.table.impl.NoSuchColumnException: aggregation: not all \
             group-by columns [{}] are present in input table with columns [{}]. Missing \
             columns: [{}]",
            key_fields.join(", "),
            columns.join(", "),
            missing.join(", ")
        )));
    }
    let keys: Vec<&str> = key_fields.iter().map(String::as_str).collect();
    let pairs = columns
        .iter()
        .filter(|column| !keys.contains(column))
        .map(|column| Pair::of(*column));
    // Behavior: io.deephaven.engine.table.impl.QueryTable#lastBy, `aggAllBy` of the last
    let last = aggregate_all_with(&narrow, AggSpec::Last, pairs, &keys, &Compiler)
        .map_err(invalid)?;
    let current = filter::unstructured_filter(&last, &[NOT_DELETED], &Compiler).map_err(invalid)?;
    let mut dropped = drop_columns.to_vec();
    dropped.push(OP_COLUMN.to_owned());
    select::drop_columns(&current, &dropped).map_err(invalid)
}

/// Consumes a topic of change events into the table they describe: the current row of
/// each key, or with `blink` the events. Every partition asked for is read from its
/// beginning.
///
/// Behavior: io.deephaven.kafka.CdcTools#consumeToTable
pub fn consume_cdc(
    graph: Arc<dyn UpdateGraph>,
    options: &CdcOptions,
) -> Result<Arc<dyn Table>, KafkaError> {
    let (plan, key_fields) = stream_plan(options)?;
    let topic = options.spec.topic.as_deref().ok_or_else(null_topic)?;
    let source = connect(&options.config, topic, options.partitions.as_deref())?;
    let stream = consume_from(graph, plan, Box::new(source), TableType::Blink, topic)?;
    current_rows(&stream, &key_fields, options.blink, &options.drop_columns)
}

/// [`consume_cdc`] over any [`RecordSource`]; tests script one.
pub fn consume_cdc_from(
    graph: Arc<dyn UpdateGraph>,
    options: &CdcOptions,
    source: Box<dyn RecordSource>,
) -> Result<Arc<dyn Table>, KafkaError> {
    let (plan, key_fields) = stream_plan(options)?;
    let topic = options.spec.topic.as_deref().ok_or_else(null_topic)?;
    let stream = consume_from(graph, plan, source, TableType::Blink, topic)?;
    current_rows(&stream, &key_fields, options.blink, &options.drop_columns)
}

/// The client of `topic`, reading `partitions` (all for `None`) from their beginning.
///
/// Behavior: io.deephaven.kafka.KafkaTools#ALL_PARTITIONS_SEEK_TO_BEGINNING, which both
/// of `CdcTools`' consumptions use
fn connect(
    config: &[(String, String)],
    topic: &str,
    partitions: Option<&[i32]>,
) -> Result<client::ClientSource, KafkaError> {
    let wanted = |partition: i32| partitions.is_none_or(|listed| listed.contains(&partition));
    client::ClientSource::connect(config, topic, &wanted, &|_| InitialOffset::Beginning)
}

/// The plan of the raw stream of change events: every field of the key's and the value's
/// schemas, fetched from the registry as an `avro_spec` of each subject fetches it, the
/// key's first.
///
/// Behavior: io.deephaven.kafka.CdcTools#consumeRawToTable, through
/// io.deephaven.kafka.KafkaTools#getConsumeStruct
fn raw_plan(config: &[(String, String)], spec: &CdcSpec) -> Result<Plan, KafkaError> {
    let registry_spec = |subject: &Option<String>, version: &Option<String>| {
        subject.as_ref().map(|subject| Spec::Avro {
            schema: AvroSchema::Registry {
                subject: subject.clone(),
                version: version.clone(),
            },
            mapping: AvroMapping::Direct,
        })
    };
    let key = registry_spec(&spec.key_subject, &spec.key_version);
    let value = registry_spec(&spec.value_subject, &spec.value_version);
    let topic = spec.topic.as_deref().unwrap_or_default();
    match (key, value) {
        (Some(key), Some(value)) => Plan::new(topic, config, &key, &value),
        (None, _) => {
            Registry::from_config(config)?;
            Err(null_subject())
        }
        // The key's schema is fetched before the null subject of the value fails (probe
        // raw_vsub_none).
        (Some(_), None) => {
            let registry = Registry::from_config(config)?;
            let subject = spec.key_subject.as_deref().unwrap_or_default();
            registry.subject_schema_text(subject, spec.key_version.as_deref())?;
            Err(null_subject())
        }
    }
}

/// Consumes a topic of change events as it is, every field of the key's and the value's
/// schemas a column, into a table of `table_type`, from the beginning of each partition.
/// A `table_type` of `None` is Java's null, which fails once the schemas are fetched.
///
/// Behavior: io.deephaven.kafka.CdcTools#consumeRawToTable
pub fn consume_cdc_raw(
    graph: Arc<dyn UpdateGraph>,
    config: &[(String, String)],
    spec: &CdcSpec,
    partitions: Option<&[i32]>,
    table_type: Option<TableType>,
) -> Result<Arc<dyn Table>, KafkaError> {
    let plan = raw_plan(config, spec)?;
    // Behavior: io.deephaven.kafka.KafkaTools#consumeToTableAndAdapter, whose registrar
    // walks the table type before the consumer is made (probes raw_tt_none_notopic,
    // raw_tt_none_nobroker)
    let Some(table_type) = table_type else {
        return Err(KafkaError::Invalid(
            "java.lang.NullPointerException: Cannot invoke \"io.deephaven.kafka.KafkaTools\
             $TableType.walk(io.deephaven.kafka.KafkaTools$TableType$Visitor)\" because \
             \"tableType\" is null"
                .to_owned(),
        ));
    };
    let topic = spec.topic.as_deref().ok_or_else(null_topic)?;
    let source = connect(config, topic, partitions)?;
    consume_from(graph, plan, Box::new(source), table_type, topic)
}
