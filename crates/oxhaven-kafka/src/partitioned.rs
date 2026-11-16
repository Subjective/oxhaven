//! A topic consumed into a partitioned table: one constituent per partition of the topic.
//!
//! Java makes, for each partition its consumer is assigned, a stream table of its own and
//! the table of the requested type over it, and adds it to a table of tables with the
//! columns `Partition` and `Table`. That table shows a constituent from the update cycle
//! after it was added. One thread polls every partition, as for one table, and hands each
//! partition's records to its constituent.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::ops::partitioned::PartitionedTable;
use oxhaven_engine::table::{
    stream_table, tracked_source, QueryTable, RootSource, StreamCallbacks, StreamPublisher,
    TableSpec,
};

use crate::stream::{failure, now_nanos, of_type};
use crate::{client, ConsumeOptions, KafkaError, Plan, Record, RecordSource, TableType};

/// The key column: the partition of the constituent.
///
/// Behavior: KafkaTools.StreamPartitionedQueryTable#PARTITION_COLUMN_NAME
pub const PARTITION_COLUMN: &str = "Partition";

/// The constituent column. Behavior: KafkaTools.StreamPartitionedQueryTable#CONSTITUENT_COLUMN_NAME
pub const CONSTITUENT_COLUMN: &str = "Table";

/// Consumes a topic into a partitioned table with one constituent per partition read.
///
/// Behavior: io.deephaven.kafka.KafkaTools#consumeToPartitionedTable
pub fn consume_to_partitioned_table(
    graph: Arc<dyn UpdateGraph>,
    options: &ConsumeOptions,
) -> Result<PartitionedTable, KafkaError> {
    let plan = Plan::new(
        &options.topic,
        &options.config,
        &options.key,
        &options.value,
    )?;
    let wanted = |partition: i32| {
        let listed = options.partitions.as_ref();
        listed.is_none_or(|listed| listed.contains(&partition))
    };
    let initial = |partition: i32| options.offsets.of(partition);
    let source = client::ClientSource::connect(&options.config, &options.topic, &wanted, &initial)?;
    let partitions = source.partitions().to_vec();
    consume_partitioned_from(
        graph,
        plan,
        Box::new(source),
        &partitions,
        options.table_type,
        &options.topic,
    )
}

/// The partitions whose constituents are still alive, and their publishers.
type Live = Arc<Mutex<BTreeMap<i32, StreamPublisher>>>;

/// The partitioned table of the records `source` returns, which are of `partitions`, with
/// a constituent of `table_type` and the columns of `plan` for each partition, in the order
/// given. A thread named after `topic` polls the source until every constituent is dropped,
/// a record cannot be read (its partition's constituent fails), or the source fails (every
/// constituent fails).
///
/// Behavior: io.deephaven.kafka.KafkaTools#consumeToPartitionedTable, after the consumer is
/// made; the per-partition registrar and KafkaIngester#shutdownPartition
pub fn consume_partitioned_from(
    graph: Arc<dyn UpdateGraph>,
    plan: Plan,
    source: Box<dyn RecordSource>,
    partitions: &[i32],
    table_type: TableType,
    topic: &str,
) -> Result<PartitionedTable, KafkaError> {
    if partitions.is_empty() {
        // Java makes the table of tables when the first partition is registered, and with
        // none it calls a method of null.
        return Err(KafkaError::Invalid(NO_PARTITION.to_owned()));
    }
    // Made first, so that it refreshes before its constituents, as Java's
    // `UpdateSourceCombiner` makes it.
    let (underlying, pending) = constituent_table(&graph);
    let live: Live = Arc::default();
    let stopped = Arc::new(AtomicBool::new(false));
    let mut constituents = Vec::with_capacity(partitions.len());
    for &partition in partitions {
        let (gone, stop) = (live.clone(), stopped.clone());
        let callbacks = StreamCallbacks {
            on_flush: None,
            // Behavior: KafkaIngester#shutdownPartition: the partition is no longer read,
            // and the consumer stops when none is left.
            on_shutdown: Some(Box::new(move || {
                let mut gone = gone.lock().unwrap_or_else(PoisonError::into_inner);
                gone.remove(&partition);
                if gone.is_empty() {
                    stop.store(true, Ordering::Release);
                }
            })),
        };
        let (blink, publisher) = stream_table(graph.clone(), plan.definition().clone(), callbacks);
        live.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(partition, publisher);
        constituents.push((partition, of_type(blink, table_type)?));
    }
    pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .extend(constituents);
    let definition = plan.definition().clone();
    let thread_live = live.clone();
    std::thread::Builder::new()
        .name(format!("Kafka-{topic}"))
        .spawn(move || poll_loop(source, plan, thread_live, stopped))
        .map_err(|e| KafkaError::Broker(format!("cannot start the polling thread: {e}")))?;
    let made = PartitionedTable::new(
        underlying,
        vec![PARTITION_COLUMN.to_owned()],
        true,
        CONSTITUENT_COLUMN.to_owned(),
        definition,
        true,
    );
    made.map_err(|error| KafkaError::Invalid(error.to_string()))
}

/// The text of Java's `NullPointerException` for a topic with no partition to read.
const NO_PARTITION: &str = "java.lang.NullPointerException: Cannot invoke \
    \"io.deephaven.kafka.KafkaTools$StreamPartitionedQueryTable.toPartitionedTable()\" \
    because the return value of \"java.util.concurrent.atomic.AtomicReference.get()\" is null";

/// Behavior: KafkaIngester#consumerLoop and #pollOnce with a consumer per partition: each
/// partition's records go to its constituent; a record that cannot be read fails that
/// constituent alone and ends the loop; a failed poll fails every constituent
/// (`notifyAllConsumersOnFailure`). Records of a partition whose constituent is gone are
/// dropped.
fn poll_loop(mut source: Box<dyn RecordSource>, plan: Plan, live: Live, stopped: Arc<AtomicBool>) {
    let publishers = |live: &Live| -> BTreeMap<i32, StreamPublisher> {
        live.lock().unwrap_or_else(PoisonError::into_inner).clone()
    };
    while !stopped.load(Ordering::Acquire) {
        let records = match source.poll() {
            Ok(records) => records,
            Err(error) => {
                let error = failure(error);
                for publisher in publishers(&live).values() {
                    publisher.fail(error.clone());
                }
                return;
            }
        };
        if records.is_empty() {
            continue;
        }
        let mut by_partition: BTreeMap<i32, Vec<Record>> = BTreeMap::new();
        for record in records {
            by_partition.entry(record.partition).or_default().push(record);
        }
        let received = now_nanos();
        let publishers = publishers(&live);
        for (partition, records) in by_partition {
            let Some(publisher) = publishers.get(&partition) else {
                continue;
            };
            let added = plan
                .decode(&records, received)
                .map_err(failure)
                .and_then(|batch| publisher.add(batch));
            if let Err(error) = added {
                publisher.fail(error);
                return;
            }
        }
    }
}

/// What the table of tables has yet to show: partitions and their constituents.
type Pending = Arc<Mutex<Vec<(i32, Arc<dyn Table>)>>>;

/// The table of tables, without rows, and where to leave the constituents it is to show
/// on its next refresh. It is flat and refreshing, with no other attribute.
///
/// Behavior: KafkaTools.StreamPartitionedQueryTable (the constructor, #enqueueAdd, #run)
fn constituent_table(graph: &Arc<dyn UpdateGraph>) -> (Arc<dyn Table>, Pending) {
    let definition = TableDefinition::new(vec![
        ColumnDefinition::new(PARTITION_COLUMN, ColumnType::Int),
        ColumnDefinition::new(CONSTITUENT_COLUMN, ColumnType::Table(TableClass::Table)),
    ])
    .expect("two names");
    let partitions = tracked_source(graph, ColumnType::Int);
    let tables = tracked_source(graph, ColumnType::Table(TableClass::Table));
    let columns: Vec<Arc<dyn ColumnSource>> = vec![partitions.clone(), tables.clone()];
    let spec = TableSpec::new(definition, RowSet::empty(), columns).flat();
    let pending: Pending = Arc::default();
    let shown = pending.clone();
    let table = QueryTable::root(graph.clone(), spec, |table| RootSource {
        source: Arc::new(Shower {
            table,
            partitions,
            tables,
            pending: shown,
        }),
        input: None,
    });
    (table, pending)
}

/// The update source of the table of tables.
struct Shower {
    table: Weak<QueryTable>,
    partitions: Arc<WritableSource>,
    tables: Arc<WritableSource>,
    pending: Pending,
}

impl UpdateSource for Shower {
    /// Behavior: KafkaTools.StreamPartitionedQueryTable#run: the constituents added since
    /// the last refresh become rows after the last one.
    fn refresh(&self) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        let added = std::mem::take(&mut *self.pending.lock().unwrap_or_else(PoisonError::into_inner));
        if added.is_empty() || table.is_failed() {
            return;
        }
        let first = table.row_set().last_row_key().map_or(0, |last| last + 1);
        let rows = RowSet::from_range(first, first + added.len() as RowKey - 1);
        let partitions: ArrayRef = Arc::new(Int32Array::from_iter_values(
            added.iter().map(|(partition, _)| *partition),
        ));
        let handles: ArrayRef = Arc::new(Int64Array::from_iter_values(
            added
                .iter()
                .map(|(_, constituent)| oxhaven_column::tables::register(constituent)),
        ));
        let written = self
            .partitions
            .fill(&rows, &partitions)
            .and_then(|()| self.tables.fill(&rows, &handles));
        match written {
            Ok(()) => table.notify(TableUpdate {
                added: rows,
                ..TableUpdate::empty()
            }),
            Err(error) => table.fail(error),
        }
    }
}
