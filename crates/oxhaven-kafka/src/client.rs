//! The consumer that talks to brokers: librdkafka, through the `rdkafka` crate.
//!
//! It does what `org.apache.kafka.clients.consumer.KafkaConsumer` does for
//! `io.deephaven.kafka.ingest.KafkaIngester`: the partitions of a topic are assigned by
//! hand, never by a consumer group's rebalance, and each starts where the caller says. A
//! group named in `group.id` is only where positions are committed to and read from.
//! `docs/KAFKA.md`, "The client library", says how it is built and what is left off.

use std::time::{Duration, Instant};

use bytes::Bytes;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError as ClientError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};

use crate::config::{ClientSettings, Reset};
use crate::{not_ported, property, InitialOffset, KafkaError, Record, RecordSource};

/// How long one poll waits for the first record, as the half second (at most)
/// `KafkaIngester#consumerLoop` passes to `KafkaConsumer#poll`.
const POLL_WAIT: Duration = Duration::from_millis(500);

/// The record source over librdkafka.
pub struct ClientSource {
    consumer: BaseConsumer,
    topic: String,
    partitions: Vec<i32>,
    /// Whether positions are recorded for the group to commit.
    commits: bool,
    max_poll_records: usize,
    /// An error the next poll returns: Java's consumer is made without one and throws it
    /// from `poll`, so the table exists first and then fails.
    failure: Option<KafkaError>,
}

fn broker_error(what: &str, error: impl std::fmt::Display) -> KafkaError {
    KafkaError::Broker(format!("{what}: {error}"))
}

/// The partitions of `topic`, in order. A topic the broker does not have has none, also
/// when this very question makes the broker create it: the table then never has rows, as
/// in Java (seen in the probes of `docs/KAFKA.md`), and a partitioned table cannot be made.
///
/// Java asks once, and the answer for a topic the broker creates on that request has no
/// partitions. librdkafka asks again until the new topic has them, so the topics that
/// exist are listed first, with a request that creates none.
///
/// Behavior: org.apache.kafka.clients.consumer.KafkaConsumer#partitionsFor
fn partitions_of(
    consumer: &BaseConsumer,
    servers: &str,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, KafkaError> {
    let unanswered = |error| {
        broker_error(
            &format!("no broker of bootstrap.servers={servers:?} answers"),
            error,
        )
    };
    let existing = consumer.fetch_metadata(None, timeout).map_err(unanswered)?;
    let existed = existing
        .topics()
        .iter()
        .any(|found| found.name() == topic && found.error().is_none());
    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(unanswered)?;
    if !existed {
        return Ok(Vec::new());
    }
    let found = metadata.topics().iter().find(|found| found.name() == topic);
    let mut partitions: Vec<i32> = found
        .map(|found| found.partitions().iter().map(|p| p.id()).collect())
        .unwrap_or_default();
    partitions.sort_unstable();
    Ok(partitions)
}

/// The offsets the consumer's group has committed for `partitions` of `topic`.
///
/// A broker that has not found or loaded the group's coordinator yet says so, most often
/// to the first consumer group it ever serves; Java asks again until the time is up.
///
/// Behavior: org.apache.kafka.clients.consumer.internals.ConsumerCoordinator
/// #fetchCommittedOffsets, which retries what is a `RetriableException`
fn committed_offsets(
    consumer: &BaseConsumer,
    topic: &str,
    partitions: &[i32],
    timeout: Duration,
) -> Result<TopicPartitionList, KafkaError> {
    use RDKafkaErrorCode as Code;
    let deadline = Instant::now() + timeout;
    loop {
        let mut asked = TopicPartitionList::new();
        for partition in partitions {
            asked.add_partition(topic, *partition);
        }
        let left = deadline.saturating_duration_since(Instant::now());
        let error = match consumer.committed_offsets(asked, left) {
            Ok(committed) => return Ok(committed),
            Err(error) => error,
        };
        let retriable = matches!(
            error.rdkafka_error_code(),
            Some(
                Code::NotCoordinator
                    | Code::CoordinatorNotAvailable
                    | Code::CoordinatorLoadInProgress
            )
        );
        if !retriable || Instant::now() >= deadline {
            return Err(broker_error("cannot read the committed offsets", error));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

impl ClientSource {
    /// Finds the partitions of `topic` that `wanted` accepts and places the consumer in
    /// each where `initial` says.
    ///
    /// Behavior: io.deephaven.kafka.ingest.KafkaIngester#KafkaIngester (`partitionsFor`,
    /// `assign`, then a seek per partition), and for `DONT_SEEK`
    /// org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils and
    /// ConsumerCoordinator#initWithCommittedOffsetsIfNeeded: the offset the group
    /// committed, or else what `auto.offset.reset` says
    pub fn connect(
        config: &[(String, String)],
        topic: &str,
        wanted: &dyn Fn(i32) -> bool,
        initial: &dyn Fn(i32) -> InitialOffset,
    ) -> Result<Self, KafkaError> {
        let settings = ClientSettings::new(config)?;
        let servers = property(config, "bootstrap.servers").unwrap_or_default();
        let consumer: BaseConsumer = settings
            .client
            .create()
            .map_err(|error| KafkaError::Invalid(format!("the Kafka consumer: {error}")))?;
        let timeout = settings.api_timeout;
        let mut partitions = partitions_of(&consumer, servers, topic, timeout)?;
        partitions.retain(|partition| wanted(*partition));

        let unsought: Vec<i32> = (partitions.iter().copied())
            .filter(|partition| initial(*partition) == InitialOffset::DontSeek)
            .collect();
        let mut committed = TopicPartitionList::new();
        if settings.group.is_some() && !unsought.is_empty() {
            committed = committed_offsets(&consumer, topic, &unsought, timeout)?;
        }

        let mut failure = None;
        let mut undefined = Vec::new();
        let mut assignment = TopicPartitionList::new();
        for partition in &partitions {
            let edge = |low: bool| -> Result<i64, KafkaError> {
                let (first, next) = consumer
                    .fetch_watermarks(topic, *partition, timeout)
                    .map_err(|error| {
                        broker_error(
                            &format!("cannot read the offsets of {topic}-{partition}"),
                            error,
                        )
                    })?;
                Ok(if low { first } else { next })
            };
            let offset = match initial(*partition) {
                InitialOffset::Beginning => edge(true)?,
                InitialOffset::End => edge(false)?,
                InitialOffset::At(offset) => offset,
                InitialOffset::DontSeek => {
                    let found = committed.find_partition(topic, *partition);
                    match (found.map(|found| found.offset()), settings.reset) {
                        (Some(Offset::Offset(offset)), _) => offset,
                        (_, Reset::Earliest) => edge(true)?,
                        (_, Reset::Latest) => edge(false)?,
                        (_, Reset::None) => {
                            undefined.push(format!("{topic}-{partition}"));
                            continue;
                        }
                    }
                }
            };
            assignment
                .add_partition_offset(topic, *partition, Offset::Offset(offset))
                .map_err(|error| broker_error("cannot place the consumer", error))?;
        }
        if !undefined.is_empty() {
            // Behavior: org.apache.kafka.clients.consumer.NoOffsetForPartitionException
            failure = Some(KafkaError::Broker(format!(
                "org.apache.kafka.clients.consumer.NoOffsetForPartitionException: Undefined \
                 offset with no reset policy for partitions: [{}]",
                undefined.join(", ")
            )));
        } else if assignment.count() > 0 {
            consumer
                .assign(&assignment)
                .map_err(|error| broker_error("cannot assign the partitions", error))?;
            if settings.group.is_some() {
                // Behavior: ConsumerCoordinator#maybeAutoCommitOffsetsAsync commits the
                // position of every assigned partition, also of one that has delivered
                // nothing. librdkafka commits what is stored, so the positions are.
                consumer
                    .store_offsets(&assignment)
                    .map_err(|error| broker_error("cannot record the positions", error))?;
            }
        }
        Ok(Self {
            consumer,
            topic: topic.to_owned(),
            partitions,
            commits: settings.group.is_some(),
            max_poll_records: settings.max_poll_records,
            failure,
        })
    }

    /// The partitions being read, in order.
    pub fn partitions(&self) -> &[i32] {
        &self.partitions
    }
}

/// Whether an error librdkafka reports while polling ends the consumption. librdkafka
/// reports every broker it loses and every request it will retry; Java's consumer
/// retries those in silence and throws from `poll` only what it cannot recover from.
///
/// Behavior: org.apache.kafka.clients.consumer.KafkaConsumer#poll, the exceptions it
/// documents (`InvalidOffsetException`, `AuthorizationException`, `KafkaException` for
/// "any other unrecoverable errors"). `grouped` is whether the properties name a group: a
/// consumer without one asks nothing of a group in Java, so nothing about one can fail it.
fn ends_consumption(error: &ClientError, grouped: bool) -> Option<KafkaError> {
    let code = match error {
        ClientError::MessageConsumptionFatal(code) => {
            return Some(broker_error("the Kafka consumer failed", code));
        }
        ClientError::MessageConsumption(code) => *code,
        _ => return None,
    };
    use RDKafkaErrorCode as Code;
    match code {
        // What librdkafka's reader of record batches says of a codec it was built
        // without. That is gzip: `docs/KAFKA.md`, "What is left off".
        Code::NotImplemented => Some(not_ported(
            "org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator",
            "a record batch compressed with gzip",
        )),
        Code::GroupAuthorizationFailed if !grouped => None,
        Code::AutoOffsetReset
        | Code::OffsetOutOfRange
        | Code::TopicAuthorizationFailed
        | Code::GroupAuthorizationFailed
        | Code::ClusterAuthorizationFailed
        | Code::BadMessage
        | Code::BadCompression
        | Code::InvalidMessage
        | Code::InvalidMessageSize => Some(broker_error("the Kafka consumer failed", code)),
        _ => None,
    }
}

impl RecordSource for ClientSource {
    /// Behavior: org.apache.kafka.clients.consumer.KafkaConsumer#poll: at most
    /// `max.poll.records` records, as soon as there is one.
    fn poll(&mut self) -> Result<Vec<Record>, KafkaError> {
        if let Some(failure) = self.failure.take() {
            return Err(failure);
        }
        let mut records = Vec::new();
        let mut wait = POLL_WAIT;
        while records.len() < self.max_poll_records {
            match self.consumer.poll(wait) {
                None => break,
                Some(Ok(message)) => records.push(Record {
                    partition: message.partition(),
                    offset: message.offset(),
                    timestamp: message.timestamp().to_millis().unwrap_or(-1),
                    key: message.key().map(Bytes::copy_from_slice),
                    value: message.payload().map(Bytes::copy_from_slice),
                }),
                Some(Err(error)) => {
                    if let Some(failure) = ends_consumption(&error, self.commits) {
                        if records.is_empty() {
                            return Err(failure);
                        }
                        self.failure = Some(failure);
                        break;
                    }
                }
            }
            wait = Duration::ZERO;
        }
        // The records of a partition stay in offset order; partitions are in number order
        // (`docs/KAFKA.md`, "For the owner's ruling", on what Java does here).
        records.sort_by_key(|record| record.partition);
        if self.commits {
            // Behavior: SubscriptionState#allConsumed: what is committed is the offset
            // after the last record `poll` returned, whether or not its row is in the
            // table yet.
            let last_of_partition = (records.iter().enumerate())
                .filter(|(index, record)| {
                    let next = records.get(index + 1);
                    next.is_none_or(|next| next.partition != record.partition)
                })
                .map(|(_, record)| record);
            for record in last_of_partition {
                // This call of librdkafka takes the offset of a record and stores the next.
                let stored =
                    (self.consumer).store_offset(&self.topic, record.partition, record.offset);
                stored.map_err(|error| broker_error("cannot record the position", error))?;
            }
        }
        Ok(records)
    }
}

/// Topics Java's listing leaves out.
///
/// Behavior: org.apache.kafka.common.internals.Topic#isInternal, through
/// `ListTopicsOptions#shouldListInternal`, which is false
const INTERNAL_TOPICS: [&str; 3] = [
    "__consumer_offsets",
    "__transaction_state",
    "__share_group_state",
];

/// Behavior: java.lang.String#hashCode and java.util.HashMap#hash
pub(crate) fn java_hash(text: &str) -> u32 {
    let hash = (text.encode_utf16()).fold(0u32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(u32::from(unit))
    });
    hash ^ (hash >> 16)
}

/// `names` in the order a Java `HashMap` that received them in this order iterates its
/// keys: by bucket, and within a bucket in the order of arrival.
///
/// Not reproduced: a bucket of more than eight names in a table of at least 64 buckets,
/// which Java turns into a tree ordered by hash.
///
/// Behavior: java.util.HashMap#putVal, #resize and HashMap.HashIterator
fn java_hash_map_order(mut names: Vec<String>) -> Vec<String> {
    let mut buckets: usize = 16;
    while names.len() * 4 > buckets * 3 {
        buckets *= 2;
    }
    names.sort_by_key(|name| java_hash(name) as usize & (buckets - 1));
    names
}

/// The names of the topics a broker has, in the order Java returns them.
///
/// Behavior: io.deephaven.kafka.KafkaTools#listTopics: the key set of the `HashMap`
/// org.apache.kafka.clients.admin.KafkaAdminClient#listTopics fills
pub fn topics(config: &[(String, String)]) -> Result<Vec<String>, KafkaError> {
    let settings = ClientSettings::new(config)?;
    let consumer: BaseConsumer = settings
        .client
        .create()
        .map_err(|error| KafkaError::Invalid(format!("the Kafka client: {error}")))?;
    let metadata = consumer
        .fetch_metadata(None, settings.api_timeout)
        .map_err(|error| broker_error("Failed to list Kafka Topics", error))?;
    let names = (metadata.topics().iter())
        .map(|topic| topic.name().to_owned())
        .filter(|name| !INTERNAL_TOPICS.contains(&name.as_str()))
        .collect();
    Ok(java_hash_map_order(names))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_come_in_the_order_of_a_java_hash_map() {
        // "a".hashCode() is 97, "b" 98, "q" 113: buckets 1, 2 and 1 of 16.
        let names = |names: &[&str]| names.iter().map(|name| (*name).to_owned()).collect();
        assert_eq!(java_hash("a"), 97);
        assert_eq!(
            java_hash_map_order(names(&["b", "q", "a"])),
            ["q", "a", "b"]
        );
        // Thirteen names make Java double the table to 32 buckets, where "q" is in 17.
        let many: Vec<String> = "abcdefghijklq".chars().map(String::from).collect();
        let ordered = java_hash_map_order(many);
        assert_eq!(ordered.join(""), "abcdefghijklq");
        let twelve: Vec<String> = "bcdefghijklq".chars().map(String::from).collect();
        assert_eq!(java_hash_map_order(twelve).join(""), "qbcdefghijkl");
    }

    #[test]
    fn only_what_java_cannot_recover_from_ends_the_consumption() {
        use RDKafkaErrorCode as Code;
        let of = |code, grouped| ends_consumption(&ClientError::MessageConsumption(code), grouped);
        let consumption = |code| of(code, true);
        assert_eq!(of(Code::GroupAuthorizationFailed, false), None);
        assert!(consumption(Code::GroupAuthorizationFailed).is_some());
        for code in [
            Code::BrokerTransportFailure,
            Code::AllBrokersDown,
            Code::NotLeaderForPartition,
        ] {
            assert_eq!(consumption(code), None, "{code}");
        }
        for code in [Code::AutoOffsetReset, Code::TopicAuthorizationFailed] {
            assert!(
                matches!(consumption(code), Some(KafkaError::Broker(_))),
                "{code}"
            );
        }
        let refused = consumption(Code::NotImplemented);
        assert!(matches!(refused, Some(KafkaError::NotPorted { java, .. })
            if java == "org.apache.kafka.common.record.DefaultRecordBatch#compressedIterator"));
        let fatal = ClientError::MessageConsumptionFatal(Code::Fatal);
        assert!(ends_consumption(&fatal, false).is_some());
    }
}
