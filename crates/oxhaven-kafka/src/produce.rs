//! Publishing a table to a topic: `KafkaTools#produceFromTable`.
//!
//! The rows the table has are published when the call is made (unless asked not to), and
//! then each cycle's changes as Java's `PublishToKafka` publishes them: a removed row is a
//! record of its old key and no value, an added row a record of its key and value, a
//! modified row one too where a value column changed; a blink table's removes are not
//! published. Each publication waits until the broker has every record, holding up the
//! cycle, as Java's does. `docs/KAFKA.md`, "Producing", has the rest.

use std::sync::{Arc, Mutex, MutexGuard};

use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_engine::graph::{ParentListener, UpdateHandler};
use oxhaven_engine::ops::agg::{aggregate, AggSpec, Aggregation, Pair};

use crate::serialize::{Encoding, JsonSerializer, JsonShape, Serializer};
use crate::{not_ported, KafkaError};

/// Behavior: io.deephaven.kafka.publish.PublishToKafka#CHUNK_SIZE: rows are serialized
/// and sent this many at a time, so a row that cannot be written stops a publication
/// after the chunks before it were sent.
const CHUNK_SIZE: u64 = 2048;

/// How the key or the value of a record is made from a row.
#[derive(Clone, PartialEq, Debug)]
pub enum ProduceSpec {
    /// No key or no value. Behavior: `KafkaTools.Produce#IGNORE`
    Ignore,
    /// One column, with Kafka's serializer for its type.
    /// Behavior: `KafkaTools.Produce#simpleSpec`
    Simple(String),
    /// A JSON object of columns. Behavior: `KafkaTools.Produce#jsonSpec`
    Json(JsonProduceSpec),
}

/// The arguments of `KafkaTools.Produce#jsonSpec`.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct JsonProduceSpec {
    /// The columns to write, in this order; all of the table's without it.
    pub include: Option<Vec<String>>,
    /// The columns not to write, when `include` is not given.
    pub exclude: Option<Vec<String>>,
    /// A field name per column; a column not listed is written to the field of its name.
    pub mapping: Vec<(String, String)>,
    /// Splits field names into nested objects (`a__b` is field `b` in object `a`).
    pub nested_delimiter: Option<String>,
    /// Whether a null cell is written as `null`, or left out.
    pub output_nulls: bool,
    /// A field for the time each record is made, in nanoseconds since the epoch, as text.
    pub timestamp_field: Option<String>,
}

/// The arguments of [`produce`]: Java's `KafkaPublishOptions` without the columns of
/// topics, partitions and timestamps (task 7.5.2).
#[derive(Clone, PartialEq, Debug)]
pub struct ProduceOptions {
    /// The properties of the producer.
    pub config: Vec<(String, String)>,
    pub topic: String,
    /// The partition of every record; Kafka's partitioner chooses without it.
    pub partition: Option<i32>,
    pub key: ProduceSpec,
    pub value: ProduceSpec,
    /// Publishes `last_by` of the table by the key's columns.
    pub last_by: bool,
    /// Publishes the rows the table has now, and not only its later changes.
    pub publish_initial: bool,
}

/// A record to send: the topic is the sink's.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OutRecord {
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// Where records go. `send` queues a record and never fails: what goes wrong is reported
/// by the next `flush`, which returns once every record sent before it was written or
/// failed, as Java's `KafkaProducer#send` with a callback followed by `flush`.
pub trait RecordSink: Send {
    fn send(&mut self, record: OutRecord);
    fn flush(&mut self) -> Result<(), KafkaError>;
}

impl JsonProduceSpec {
    /// Behavior: io.deephaven.kafka.JsonImpl.JsonProduce#getColumnNames
    fn columns(&self, definition: &TableDefinition) -> Result<Vec<String>, KafkaError> {
        let all = definition.columns().iter().map(|column| column.name.clone());
        match (&self.include, &self.exclude) {
            (Some(_), Some(_)) => Err(KafkaError::Invalid(
                "java.lang.IllegalArgumentException: Can't have both excludeColumns and \
                 includeColumns not null"
                    .to_owned(),
            )),
            (Some(include), None) => {
                let missing: Vec<&str> = include
                    .iter()
                    .filter(|name| definition.index_of(name).is_none())
                    .map(String::as_str)
                    .collect();
                if !missing.is_empty() {
                    return Err(KafkaError::Invalid(format!(
                        "java.lang.IllegalArgumentException: includeColumns contains names \
                         not found in table columns: [{}]",
                        missing.join(", ")
                    )));
                }
                Ok(include.clone())
            }
            (None, Some(exclude)) => Ok(all.filter(|name| !exclude.contains(name)).collect()),
            (None, None) => Ok(all.collect()),
        }
    }

    /// Behavior: io.deephaven.kafka.JsonImpl.JsonProduce#getFieldNames
    fn field_of<'a>(&'a self, column: &'a str) -> &'a str {
        let mapped = self.mapping.iter().rev().find(|(name, _)| name == column);
        mapped.map_or(column, |(_, field)| field.as_str())
    }
}

/// A spec's columns and its serializer for `definition`.
///
/// Behavior: `KeyOrValueSpec#getSerializer` (and `Serializer#configure`), then
/// `#getColumnNames`, for each spec in turn
fn plan(
    spec: &ProduceSpec,
    definition: &TableDefinition,
    config: &[(String, String)],
    is_key: bool,
) -> Result<Option<(Vec<String>, Planned)>, KafkaError> {
    match spec {
        ProduceSpec::Ignore => Ok(None),
        ProduceSpec::Simple(column) => {
            let encoding = Encoding::of(config, is_key);
            let serializer = Serializer::simple(definition, column, encoding?)?;
            Ok(Some((vec![column.clone()], Planned::Ready(serializer))))
        }
        ProduceSpec::Json(json) => {
            let encoding = Encoding::of(config, is_key)?;
            Ok(Some((json.columns(definition)?, Planned::Json(encoding))))
        }
    }
}

/// A serializer, or what a JSON spec's needs once the table to publish is known.
enum Planned {
    Ready(Serializer),
    Json(Encoding),
}

impl Planned {
    /// Behavior: KeyOrValueSpec#getKeyOrValueSerializer of the table published
    fn serializer(
        self,
        spec: &ProduceSpec,
        columns: &[String],
        original: &TableDefinition,
        published: &TableDefinition,
    ) -> Result<Serializer, KafkaError> {
        match (self, spec) {
            (Planned::Ready(serializer), _) => Ok(serializer.over(original, published)),
            (Planned::Json(encoding), ProduceSpec::Json(json)) => {
                let shape = JsonShape {
                    fields: columns
                        .iter()
                        .map(|column| (column.as_str(), json.field_of(column)))
                        .collect(),
                    nested_delimiter: json.nested_delimiter.as_deref(),
                    output_nulls: json.output_nulls,
                    timestamp_field: json.timestamp_field.as_deref(),
                };
                Ok(Serializer::Json(JsonSerializer::new(
                    published, &shape, encoding,
                )?))
            }
            (Planned::Json(_), _) => unreachable!("only a JSON spec waits for its table"),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The state of one publication: Java's `PublishToKafka` and its listener.
struct Publisher {
    table: Arc<dyn Table>,
    key: Option<(Serializer, Vec<usize>)>,
    value: Option<(Serializer, Vec<usize>)>,
    partition: Option<i32>,
    blink: bool,
    topic: String,
    /// `None` once publishing stopped.
    sink: Mutex<Option<Box<dyn RecordSink>>>,
    failure: Mutex<Option<String>>,
}

impl Publisher {
    /// Sends a record per row of `rows`, from the values before this cycle when `prev`,
    /// with no value unless `values`.
    ///
    /// Behavior: io.deephaven.kafka.publish.PublishToKafka#publishMessages
    fn publish(
        &self,
        sink: &mut dyn RecordSink,
        rows: &RowSet,
        prev: bool,
        values: bool,
        sent: &mut bool,
    ) -> Result<(), KafkaError> {
        if rows.is_empty() {
            return Ok(());
        }
        *sent = true;
        let mut start = 0;
        while start < rows.size() {
            let end = (start + CHUNK_SIZE).min(rows.size());
            let chunk = rows.sub_set_by_position_range(start, end);
            start = end;
            let side = |side: &Option<(Serializer, Vec<usize>)>| match side {
                Some((serializer, _)) => serializer.serialize(&*self.table, &chunk, prev),
                None => Ok(vec![None; chunk.size() as usize]),
            };
            let keys = side(&self.key)?;
            let values = if values {
                side(&self.value)?
            } else {
                vec![None; chunk.size() as usize]
            };
            for (key, value) in keys.into_iter().zip(values) {
                if let Some(partition) = self.partition.filter(|partition| *partition < 0) {
                    // Behavior: org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord
                    return Err(KafkaError::Invalid(format!(
                        "java.lang.IllegalArgumentException: Invalid partition: {partition}. \
                         Partition number should always be non-negative or null."
                    )));
                }
                sink.send(OutRecord {
                    partition: self.partition,
                    key,
                    value,
                });
            }
        }
        Ok(())
    }

    /// Runs `body`, which sends, then waits for what it sent. An error of the body wins
    /// over one of the wait, but the records sent before it are still waited for.
    ///
    /// Behavior: io.deephaven.kafka.publish.PublishToKafka.PublicationGuard#close
    fn guarded(
        &self,
        body: impl FnOnce(&mut dyn RecordSink, &mut bool) -> Result<(), KafkaError>,
    ) -> Result<(), KafkaError> {
        let mut sink = lock(&self.sink);
        let Some(sink) = sink.as_mut() else {
            return Ok(());
        };
        let mut sent = false;
        let outcome = body(&mut **sink, &mut sent);
        let flushed = if sent { sink.flush() } else { Ok(()) };
        outcome.and(flushed)
    }
}

/// Java's listener reads `ModifiedColumnSet#containsAny` of its own set of columns with
/// the update's set as the argument, which throws a `NullPointerException` when the
/// update's set is `ModifiedColumnSet.ALL`; the listener then fails and publishes nothing
/// more. That is refused here, at the same point, by name.
fn modified_any(
    columns: &[usize],
    modified: &ModifiedColumnSet,
) -> Result<bool, KafkaError> {
    match modified {
        ModifiedColumnSet::All => Err(not_ported(
            "io.deephaven.engine.table.ModifiedColumnSet#containsAny",
            "a change whose modified columns are ModifiedColumnSet.ALL, on which the \
             reference's PublishToKafka listener throws a NullPointerException and stops",
        )),
        modified => Ok(modified.contains_any(columns)),
    }
}

impl UpdateHandler for Publisher {
    /// Behavior: io.deephaven.kafka.publish.PublishToKafka.PublishListener#onUpdate
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        let failure = |error: KafkaError| TableFailure {
            message: error.to_string(),
        };
        if let Some((_, key_columns)) = &self.key {
            if modified_any(key_columns, &update.modified_columns).map_err(failure)? {
                return Err(TableFailure {
                    message: "io.deephaven.base.verify.AssertionFailure: Assertion failed: \
                              asserted !keysModified.containsAny(upstream.modifiedColumnSet()), \
                              instead Key columns should never be modified."
                        .to_owned(),
                });
            }
        }
        self.guarded(|sink, sent| {
            if self.blink {
                // Removes of a blink table are never published, and it has no modifies.
                return self.publish(sink, &update.added, false, true, sent);
            }
            self.publish(sink, &update.removed, true, false, sent)?;
            let values_modified = match &self.value {
                Some((_, value_columns)) => {
                    modified_any(value_columns, &update.modified_columns)?
                }
                None => false,
            };
            if values_modified {
                let changed = update.added.union(&update.modified);
                self.publish(sink, &changed, false, true, sent)
            } else {
                self.publish(sink, &update.added, false, true, sent)
            }
        })
        .map_err(failure)
    }

    /// The table failed, or a publication did: nothing more is published. Java logs it
    /// (`AsyncClientErrorNotifier`, "Error in table update"); so does this, and keeps it
    /// for [`Publication::failure`].
    fn on_failure(&self, failure: &TableFailure) {
        eprintln!(
            "Error in table update of the Kafka publisher of topic {}: {}",
            self.topic, failure.message
        );
        lock(&self.failure).get_or_insert_with(|| failure.message.clone());
    }
}

/// A table being published, and the way to stop: the `Runnable` Java returns.
pub struct Publication {
    running: Mutex<Option<Running>>,
    publisher: Arc<Publisher>,
}

struct Running {
    _listener: Option<Arc<dyn UpdateListener>>,
}

impl Publication {
    /// Stops publishing; the producer is closed. A second call fails, as Java's
    /// releases a liveness scope twice.
    ///
    /// Behavior: io.deephaven.engine.liveness.LivenessScope#release, of the scope
    /// `KafkaTools#produceFromTable` returns
    pub fn stop(&self) -> Result<(), KafkaError> {
        let Some(running) = lock(&self.running).take() else {
            return Err(KafkaError::Invalid(
                "java.lang.IllegalStateException: LivenessScope's reference count has been \
                 decreased more than increased"
                    .to_owned(),
            ));
        };
        drop(running);
        // Waits for a publication under way, then closes the producer.
        lock(&self.publisher.sink).take();
        Ok(())
    }

    /// Why publishing stopped by itself: the table failed, or a record could not be made
    /// or sent. Java logs it and nothing else sees it.
    pub fn failure(&self) -> Option<String> {
        lock(&self.publisher.failure).clone()
    }
}

/// Publishes `table` to Kafka through a producer over librdkafka.
///
/// For a refreshing table, call it with or without the update graph's shared lock; it
/// takes it. It returns once the rows the table has are written, or fails if they could
/// not be. Dropping the result stops publishing, as calling [`Publication::stop`] does.
///
/// Behavior: io.deephaven.kafka.KafkaTools#produceFromTable(KafkaPublishOptions)
pub fn produce(
    table: Arc<dyn Table>,
    options: &ProduceOptions,
) -> Result<Publication, KafkaError> {
    produce_to(table, options, &|| {
        Ok(Box::new(crate::producer::ClientSink::connect(
            &options.config,
            &options.topic,
        )?))
    })
}

/// [`produce`] to any [`RecordSink`]; `sink` makes it where Java makes its producer, after
/// the specs are checked.
pub fn produce_to(
    table: Arc<dyn Table>,
    options: &ProduceOptions,
    sink: &dyn Fn() -> Result<Box<dyn RecordSink>, KafkaError>,
) -> Result<Publication, KafkaError> {
    // Behavior: KafkaPublishOptions#checkNotBothIgnore, #checkPublishInitial, #checkTopic
    if options.key == ProduceSpec::Ignore && options.value == ProduceSpec::Ignore {
        return Err(KafkaError::Invalid(
            "java.lang.IllegalArgumentException: keySpec and valueSpec can't both be ignore \
             specs"
                .to_owned(),
        ));
    }
    if !options.publish_initial && !table.is_refreshing() {
        return Err(KafkaError::Invalid(
            "java.lang.IllegalArgumentException: publishInitial==false && \
             table.isRefreshing() == false"
                .to_owned(),
        ));
    }
    if options.topic.is_empty() {
        return Err(KafkaError::Invalid(
            "java.lang.IllegalArgumentException: Must set topic or topicColumn (or both)"
                .to_owned(),
        ));
    }
    match table.update_graph() {
        Some(graph) => locked(&*graph, || start(table, options, sink)),
        None => start(table, options, sink),
    }
}

/// Behavior: KafkaTools#produceFromTable and PublishToKafka#PublishToKafka
fn start(
    table: Arc<dyn Table>,
    options: &ProduceOptions,
    sink: &dyn Fn() -> Result<Box<dyn RecordSink>, KafkaError>,
) -> Result<Publication, KafkaError> {
    let original = table.definition().clone();
    let key = plan(&options.key, &original, &options.config, true)?;
    let value = plan(&options.value, &original, &options.config, false)?;
    let last_by = options.last_by && options.key != ProduceSpec::Ignore;
    let published = match (&key, last_by) {
        (Some((key_columns, _)), true) => last_by_of(&table, key_columns)?,
        _ => table,
    };
    let definition = published.definition().clone();
    let index_of = |columns: &[String]| -> Vec<usize> {
        let index = |name: &String| definition.index_of(name);
        columns.iter().filter_map(index).collect()
    };
    let finish = |planned: Option<(Vec<String>, Planned)>, spec: &ProduceSpec| {
        planned
            .map(|(columns, planned)| {
                let serializer = planned.serializer(spec, &columns, &original, &definition)?;
                Ok((serializer, index_of(&columns)))
            })
            .transpose()
    };
    let key = finish(key, &options.key)?;
    let value = finish(value, &options.value)?;
    let publisher = Arc::new(Publisher {
        blink: published.attributes().blink,
        table: published.clone(),
        key,
        value,
        partition: options.partition,
        topic: options.topic.clone(),
        sink: Mutex::new(Some(sink()?)),
        failure: Mutex::new(None),
    });
    if options.publish_initial {
        let rows = published.row_set();
        publisher.guarded(|sink, sent| publisher.publish(sink, &rows, false, true, sent))?;
    }
    let listener = if published.is_refreshing() {
        let handler: Arc<dyn UpdateHandler> = publisher.clone();
        Some(ParentListener::subscribe(
            published,
            Arc::downgrade(&handler),
        ))
    } else {
        // Behavior: PublishToKafka closes the producer of a static table at once.
        lock(&publisher.sink).take();
        None
    };
    Ok(Publication {
        running: Mutex::new(Some(Running {
            _listener: listener,
        })),
        publisher,
    })
}

/// Behavior: io.deephaven.api.TableOperations#lastBy(String...), which is `aggAllBy` of
/// `AggSpec.last()`
fn last_by_of(table: &Arc<dyn Table>, keys: &[String]) -> Result<Arc<dyn Table>, KafkaError> {
    let pairs: Vec<Pair> = table
        .definition()
        .columns()
        .iter()
        .filter(|column| !keys.contains(&column.name))
        .map(|column| Pair::of(column.name.as_str()))
        .collect();
    let aggregations = if pairs.is_empty() {
        Vec::new()
    } else {
        vec![Aggregation::columns(AggSpec::Last, pairs)]
    };
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    aggregate(table, &aggregations, false, &keys)
        .map_err(|error| KafkaError::Invalid(error.to_string()))
}
