//! The table of a stream of records: the thread that polls, and the root table that shows
//! on each update cycle what the thread decoded since the last one.
//!
//! Java builds this from a `KafkaStreamPublisher`, a `StreamToBlinkTableAdapter` and, for an
//! append-only table, `BlinkTableTools#blinkToAppendOnly` on top. Here the root table adds
//! the rows itself in either way. `docs/KAFKA.md`, "The engine side", says what a general
//! adapter in the engine would take over.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::table::{ring_table, tracked_source, QueryTable, RootSource, TableSpec};

use crate::{KafkaError, Plan, RecordSource, TableType};

/// What the polling thread leaves for the table.
#[derive(Default)]
struct Pending {
    batches: Vec<Vec<ArrayRef>>,
    failure: Option<TableFailure>,
}

struct Shared {
    pending: Mutex<Pending>,
    /// Set when the table is dropped; the thread then ends.
    stopped: AtomicBool,
}

struct Refresher {
    table: Weak<QueryTable>,
    columns: Vec<Arc<WritableSource>>,
    blink: bool,
    shared: Arc<Shared>,
}

impl Drop for Refresher {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
    }
}

fn lock(pending: &Mutex<Pending>) -> MutexGuard<'_, Pending> {
    pending.lock().unwrap_or_else(PoisonError::into_inner)
}

fn failure(error: impl ToString) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

impl Refresher {
    /// Writes the rows of `batches` and returns the change they make.
    ///
    /// Behavior: io.deephaven.stream.StreamToBlinkTableAdapter#doRefresh for a blink table;
    /// io.deephaven.engine.table.impl.BlinkTableTools#blinkToAppendOnly on top of it for
    /// an append-only one
    fn apply(
        &self,
        table: &QueryTable,
        batches: &[Vec<ArrayRef>],
    ) -> Result<Option<TableUpdate>, TableFailure> {
        let old_size = table.row_set().size();
        let new_rows: u64 = batches
            .iter()
            .map(|batch| batch.first().map_or(0, |column| column.len()) as u64)
            .sum();
        let (first, removed) = if self.blink {
            (0, RowSet::flat(old_size))
        } else {
            (old_size, RowSet::empty())
        };
        if new_rows == 0 && removed.is_empty() {
            return Ok(None);
        }
        let added = if new_rows == 0 {
            RowSet::empty()
        } else {
            RowSet::from_range(first as RowKey, (first + new_rows - 1) as RowKey)
        };
        for (index, column) in self.columns.iter().enumerate() {
            if new_rows > 0 {
                let parts: Vec<&dyn Array> =
                    batches.iter().map(|batch| batch[index].as_ref()).collect();
                let values = arrow::compute::concat(&parts).map_err(failure)?;
                column.fill(&added, &values)?;
            }
            if self.blink && old_size > new_rows {
                column.remove(&RowSet::from_range(
                    new_rows as RowKey,
                    old_size as RowKey - 1,
                ));
            }
        }
        Ok(Some(TableUpdate {
            added,
            removed,
            ..TableUpdate::empty()
        }))
    }
}

impl UpdateSource for Refresher {
    /// Behavior: io.deephaven.stream.StreamToBlinkTableAdapter#run: a failure is delivered
    /// in place of the rows that arrived before it in the same cycle.
    fn refresh(&self) {
        let pending = std::mem::take(&mut *lock(&self.shared.pending));
        let Some(table) = self.table.upgrade() else {
            return;
        };
        if table.is_failed() {
            return;
        }
        if let Some(failure) = pending.failure {
            table.fail(failure);
            return;
        }
        match self.apply(&table, &pending.batches) {
            Ok(Some(update)) => table.notify(update),
            Ok(None) => {}
            Err(failure) => table.fail(failure),
        }
    }
}

fn now_nanos() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH);
    since.map_or(0, |since| since.as_nanos() as i64)
}

/// Behavior: io.deephaven.kafka.ingest.KafkaIngester#consumerLoop and #pollOnce. A record
/// that cannot become a row fails the table, as Java's `StreamConsumer#acceptFailure` does.
fn poll_loop(mut source: Box<dyn RecordSource>, plan: Plan, shared: Arc<Shared>) {
    while !shared.stopped.load(Ordering::Acquire) {
        let decoded = source.poll().and_then(|records| {
            if records.is_empty() {
                Ok(None)
            } else {
                plan.decode(&records, now_nanos()).map(Some)
            }
        });
        let mut pending = lock(&shared.pending);
        match decoded {
            Ok(None) => {}
            Ok(Some(batch)) => pending.batches.push(batch),
            Err(error) => {
                pending.failure = Some(failure(error));
                return;
            }
        }
    }
}

/// The table of the records `source` returns, with the columns of `plan`. A thread named
/// after `topic` polls the source until the table is dropped or the source fails.
///
/// Behavior: io.deephaven.kafka.KafkaTools#consumeToTable, after the consumer is made
pub fn consume_from(
    graph: Arc<dyn UpdateGraph>,
    plan: Plan,
    source: Box<dyn RecordSource>,
    table_type: TableType,
    topic: &str,
) -> Result<Arc<dyn Table>, KafkaError> {
    let definition = plan.definition().clone();
    let columns: Vec<Arc<WritableSource>> = definition
        .columns()
        .iter()
        .map(|column| tracked_source(&graph, column.column_type.clone()))
        .collect();
    let blink = table_type != TableType::Append;
    let mut attributes = TableAttributes::default();
    if blink {
        attributes.blink = true;
    } else {
        attributes.add_only = true;
        attributes.append_only = true;
    }
    let sources = columns
        .iter()
        .map(|column| column.clone() as Arc<dyn ColumnSource>)
        .collect();
    let mut spec = TableSpec::new(definition, RowSet::empty(), sources).with_attributes(attributes);
    spec.flat = true;
    let shared = Arc::new(Shared {
        pending: Mutex::new(Pending::default()),
        stopped: AtomicBool::new(false),
    });
    let table = QueryTable::root(graph, spec, |table| RootSource {
        source: Arc::new(Refresher {
            table,
            columns,
            blink,
            shared: shared.clone(),
        }),
        input: None,
    });
    std::thread::Builder::new()
        .name(format!("Kafka-{topic}"))
        .spawn(move || poll_loop(source, plan, shared))
        .map_err(|e| KafkaError::Broker(format!("cannot start the polling thread: {e}")))?;
    match table_type {
        TableType::Blink | TableType::Append => Ok(table),
        // Behavior: io.deephaven.engine.table.impl.sources.ring.RingTableTools#of
        TableType::Ring(capacity) => {
            let blink: Arc<dyn Table> = table;
            let ring = ring_table(&blink, capacity, true);
            Ok(ring.map_err(|failure| KafkaError::Invalid(failure.message))?)
        }
    }
}
