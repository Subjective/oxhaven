//! Kafka topics as tables. `docs/KAFKA.md` has the design and the list of what is not built.
//!
//! | Item | What it is |
//! | --- | --- |
//! | [`consume`] | The table of a topic: `KafkaTools#consumeToTable` |
//! | [`Plan`] | The columns of that table and how a record becomes a row |
//! | [`consume_from`] | The same table over any [`RecordSource`]; tests script one |
//! | [`wire::WireSource`] | The record source that talks to brokers |
//!
//! A thread polls the source, turns each batch of records into columns and leaves them for
//! the table, which shows what arrived since its last update cycle on the next one: as its
//! only rows if it is a blink table, after the rows it has if it is append-only.

pub mod json;
mod plan;
mod stream;
pub mod wire;

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;

pub use plan::Plan;
pub use stream::consume_from;

/// Why a topic cannot be consumed, or why its table failed.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum KafkaError {
    /// Java does this and Oxhaven does not yet. `java` names the class or method.
    NotPorted { java: String, what: String },
    /// An argument or a property Java refuses too.
    Invalid(String),
    /// A broker could not be reached or answered with an error.
    Broker(String),
    /// A record that cannot become a row, in the words of Java's exception.
    Record(String),
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KafkaError::NotPorted { java, what } => write!(f, "not ported yet: {java}: {what}"),
            KafkaError::Invalid(text) | KafkaError::Broker(text) | KafkaError::Record(text) => {
                f.write_str(text)
            }
        }
    }
}

impl std::error::Error for KafkaError {}

pub(crate) fn not_ported(java: &str, what: &str) -> KafkaError {
    KafkaError::NotPorted {
        java: java.to_owned(),
        what: what.to_owned(),
    }
}

/// One record of a topic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Record {
    pub partition: i32,
    pub offset: i64,
    /// Milliseconds since the epoch, or -1 for none.
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

/// Where records come from. `poll` returns the records that arrived since the last call,
/// after waiting a short while if there are none. An error ends the consumption.
pub trait RecordSource: Send {
    fn poll(&mut self) -> Result<Vec<Record>, KafkaError>;
}

/// Where in a partition to start.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InitialOffset {
    Beginning,
    End,
    /// Where the consumer group left off.
    DontSeek,
    At(i64),
}

impl InitialOffset {
    /// Behavior: io.deephaven.kafka.ingest.KafkaIngester#SEEK_TO_BEGINNING, #DONT_SEEK
    /// and #SEEK_TO_END
    pub fn from_java(offset: i64) -> Self {
        match offset {
            -1 => InitialOffset::Beginning,
            -2 => InitialOffset::DontSeek,
            -3 => InitialOffset::End,
            offset => InitialOffset::At(offset),
        }
    }
}

/// Where to start in each partition.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Offsets {
    Everywhere(InitialOffset),
    /// Per partition; a partition that is not listed is not sought.
    ///
    /// Behavior: io.deephaven.kafka.KafkaTools#partitionToOffsetFromParallelArrays
    ByPartition(Vec<(i32, i64)>),
}

impl Offsets {
    fn of(&self, partition: i32) -> InitialOffset {
        match self {
            Offsets::Everywhere(offset) => *offset,
            // Java puts the pairs in a map in order, so the last of a partition wins.
            Offsets::ByPartition(pairs) => pairs
                .iter()
                .rev()
                .find(|(listed, _)| *listed == partition)
                .map_or(InitialOffset::DontSeek, |(_, offset)| {
                    InitialOffset::from_java(*offset)
                }),
        }
    }
}

/// How the key or the value of a record becomes columns.
#[derive(Clone, PartialEq, Debug)]
pub enum Spec {
    /// No columns. Behavior: `KafkaTools.Consume#IGNORE`
    Ignore,
    /// One column. Without a name or a type, the properties give them.
    ///
    /// Behavior: `KafkaTools.Consume#simpleSpec`, `KafkaTools#FROM_PROPERTIES`
    Simple {
        column: Option<String>,
        data_type: Option<ColumnType>,
    },
    /// The columns named, each from a field of a JSON object. `mapping` pairs a field, or a
    /// JSON pointer if it starts with `/`, with the column it fills; a column without a
    /// pair is filled from the field of its own name.
    ///
    /// Behavior: `KafkaTools.Consume#jsonSpec`
    Json {
        columns: Vec<(String, ColumnType)>,
        mapping: Option<Vec<(String, String)>>,
    },
}

/// Behavior: `io.deephaven.kafka.KafkaTools.TableType`
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TableType {
    Blink,
    Append,
    Ring(i32),
}

/// The arguments of [`consume`].
#[derive(Clone, PartialEq, Debug)]
pub struct ConsumeOptions {
    /// The properties, for the consumer and for the table.
    pub config: Vec<(String, String)>,
    pub topic: String,
    /// The partitions to read; all of them if `None`.
    pub partitions: Option<Vec<i32>>,
    pub offsets: Offsets,
    pub key: Spec,
    pub value: Spec,
    pub table_type: TableType,
}

fn property<'a>(config: &'a [(String, String)], name: &str) -> Option<&'a str> {
    // Later pairs replace earlier ones, as in the `Properties` Java is given.
    let found = config.iter().rev().find(|(key, _)| key == name);
    found.map(|(_, value)| value.as_str())
}

/// The consumer properties whose effect is not built. Others that a Java consumer knows
/// tune it and do not change which rows appear; they are accepted and have no effect.
fn refuse_properties(config: &[(String, String)]) -> Result<(), KafkaError> {
    if let Some(protocol) = property(config, "security.protocol") {
        if !protocol.eq_ignore_ascii_case("PLAINTEXT") {
            return Err(not_ported(
                "org.apache.kafka.common.network.ChannelBuilders#clientChannelBuilder",
                &format!("security.protocol={protocol}"),
            ));
        }
    }
    if let Some(level) = property(config, "isolation.level") {
        if level != "read_uncommitted" {
            return Err(not_ported(
                "org.apache.kafka.clients.consumer.internals.FetchCollector",
                &format!("isolation.level={level}"),
            ));
        }
    }
    Ok(())
}

/// Consumes a topic into a table that changes as records arrive.
///
/// The partitions are read by one thread, which stops when the table is dropped. A record
/// that cannot become a row, and a broker that fails, fail the table.
///
/// Behavior: io.deephaven.kafka.KafkaTools#consumeToTable
pub fn consume(
    graph: Arc<dyn UpdateGraph>,
    options: &ConsumeOptions,
) -> Result<Arc<dyn Table>, KafkaError> {
    let plan = Plan::new(
        &options.topic,
        &options.config,
        &options.key,
        &options.value,
    )?;
    refuse_properties(&options.config)?;
    // Behavior: org.apache.kafka.clients.consumer.ConsumerConfig, where the property is
    // required
    let bootstrap = property(&options.config, "bootstrap.servers").ok_or_else(|| {
        KafkaError::Invalid(
            "org.apache.kafka.common.config.ConfigException: Missing required configuration \
             \"bootstrap.servers\" which has no default value."
                .to_owned(),
        )
    })?;
    let wanted = |partition: i32| {
        let listed = options.partitions.as_ref();
        listed.is_none_or(|listed| listed.contains(&partition))
    };
    let initial = |partition: i32| options.offsets.of(partition);
    let source = wire::WireSource::connect(bootstrap, &options.topic, &wanted, &initial)?;
    consume_from(
        graph,
        plan,
        Box::new(source),
        options.table_type,
        &options.topic,
    )
}
