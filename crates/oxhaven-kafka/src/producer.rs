//! The producer that talks to brokers: librdkafka's, through the `rdkafka` crate.
//!
//! It stands where `PublishToKafka` has a `KafkaProducer`: records are sent with a callback
//! that counts them and keeps the first failure, and a flush waits for all of them.
//! `docs/KAFKA.md`, "Producing", has how its properties are translated.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::sleep;
use std::time::{Duration, Instant};

use rdkafka::error::{KafkaError as ClientError, RDKafkaErrorCode};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use rdkafka::util::Timeout;
use rdkafka::ClientContext;

use crate::config::ProducerSettings;
use crate::produce::{OutRecord, RecordSink};
use crate::KafkaError;

/// What the delivery callbacks report.
///
/// Behavior: io.deephaven.kafka.publish.PublishToKafka.PublicationGuard#onCompletion
#[derive(Default)]
pub(crate) struct Deliveries {
    completed: AtomicU64,
    failure: Mutex<Option<String>>,
}

impl Deliveries {
    fn fail(&self, failure: String) {
        let mut first = self.failure.lock().unwrap_or_else(|p| p.into_inner());
        first.get_or_insert(failure);
    }
}

impl ClientContext for Deliveries {}

impl ProducerContext for Deliveries {
    type DeliveryOpaque = ();

    fn delivery(&self, result: &DeliveryResult<'_>, _: ()) {
        self.completed.fetch_add(1, Ordering::SeqCst);
        if let Err((error, _)) = result {
            self.fail(error.to_string());
        }
    }
}

/// The record sink over librdkafka.
pub struct ClientSink {
    producer: BaseProducer<Deliveries>,
    topic: String,
    max_block: Duration,
    /// The number of partitions of the topic, once its metadata has been seen.
    partitions: Option<i32>,
    /// Whether this publication found no metadata in time.
    no_metadata: bool,
    sent: u64,
}

impl ClientSink {
    /// Makes the producer `config` describes, for `topic`. Nothing is asked of a broker
    /// yet, as in Java.
    ///
    /// Behavior: org.apache.kafka.clients.producer.KafkaProducer#KafkaProducer
    pub fn connect(config: &[(String, String)], topic: &str) -> Result<Self, KafkaError> {
        let settings = ProducerSettings::new(config)?;
        let producer = settings
            .client
            .create_with_context(Deliveries::default())
            .map_err(|error| KafkaError::Invalid(format!("the Kafka producer: {error}")))?;
        Ok(Self {
            producer,
            topic: topic.to_owned(),
            max_block: settings.max_block,
            partitions: None,
            no_metadata: false,
            sent: 0,
        })
    }

    /// Waits up to `max.block.ms` for the topic's metadata, and for `partition` in it.
    ///
    /// Behavior: org.apache.kafka.clients.producer.KafkaProducer#waitOnMetadata, whose
    /// `TimeoutException` the send's callback gets. Java waits again for each record it
    /// sends while the metadata is missing; this waits once per publication.
    fn await_metadata(&mut self, partition: Option<i32>) -> Result<(), String> {
        let known = |partitions: Option<i32>| match (partitions, partition) {
            (Some(count), Some(partition)) => partition < count,
            (Some(_), None) => true,
            (None, _) => false,
        };
        if known(self.partitions) {
            return Ok(());
        }
        let deadline = Instant::now() + self.max_block;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let metadata = self
                .producer
                .client()
                .fetch_metadata(Some(&self.topic), left.max(Duration::from_millis(1)));
            if let Ok(metadata) = metadata {
                let topic = metadata.topics().iter().find(|t| t.name() == self.topic);
                let topic = topic.filter(|t| t.error().is_none() && !t.partitions().is_empty());
                self.partitions = topic.map(|topic| topic.partitions().len() as i32);
            }
            if known(self.partitions) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                let millis = self.max_block.as_millis();
                return Err(match (self.partitions, partition) {
                    (Some(count), Some(partition)) => format!(
                        "org.apache.kafka.common.errors.TimeoutException: Partition \
                         {partition} of topic {} with partition count {count} is not present \
                         in metadata after {millis} ms.",
                        self.topic
                    ),
                    _ => format!(
                        "org.apache.kafka.common.errors.TimeoutException: Topic {} not \
                         present in metadata after {millis} ms.",
                        self.topic
                    ),
                });
            }
            sleep(Duration::from_millis(100).min(left));
        }
    }
}

impl RecordSink for ClientSink {
    /// Behavior: org.apache.kafka.clients.producer.KafkaProducer#send(ProducerRecord,
    /// Callback): what fails goes to the callback
    fn send(&mut self, record: OutRecord) {
        self.sent += 1;
        if self.no_metadata {
            // Java waits for the metadata again for each record and fails it again; this
            // does not wait again within one publication.
            self.producer
                .context()
                .completed
                .fetch_add(1, Ordering::SeqCst);
            return;
        }
        if let Err(failure) = self.await_metadata(record.partition) {
            self.no_metadata = true;
            let context = self.producer.context();
            context.completed.fetch_add(1, Ordering::SeqCst);
            context.fail(failure);
            return;
        }
        let mut message: BaseRecord<'_, [u8], [u8], ()> = BaseRecord::to(&self.topic);
        message.partition = record.partition;
        message.key = record.key.as_deref();
        message.payload = record.value.as_deref();
        loop {
            match self.producer.send(message) {
                Ok(()) => return,
                Err((ClientError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                    message = back;
                    self.producer.poll(Duration::from_millis(100));
                }
                Err((error, _)) => {
                    let context = self.producer.context();
                    context.completed.fetch_add(1, Ordering::SeqCst);
                    context.fail(error.to_string());
                    return;
                }
            }
        }
    }

    /// Behavior: PublishToKafka.PublicationGuard#close: `flush`, then the first failure
    /// of a send, then the counts
    fn flush(&mut self) -> Result<(), KafkaError> {
        let flushed = self.producer.flush(Timeout::Never);
        let context = self.producer.context();
        let completed = context.completed.swap(0, Ordering::SeqCst);
        let failure = context
            .failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let sent = std::mem::take(&mut self.sent);
        self.no_metadata = false;
        let publisher = "io.deephaven.kafka.publish.KafkaPublisherException";
        if let Err(error) = flushed {
            return Err(KafkaError::Broker(format!(
                "{publisher}: KafkaProducer reported flush failure: {error}"
            )));
        }
        if let Some(failure) = failure {
            return Err(KafkaError::Broker(format!(
                "{publisher}: KafkaProducer reported send failure: {failure}"
            )));
        }
        if sent != completed {
            return Err(KafkaError::Broker(format!(
                "{publisher}: Sent count {sent} does not match completed count {completed}"
            )));
        }
        Ok(())
    }
}
