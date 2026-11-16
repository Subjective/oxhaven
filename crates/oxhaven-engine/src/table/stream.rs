//! Stream tables: blink tables that show, on each update cycle, the rows a publisher handed
//! over since the last one.
//!
//! [`stream_table`] makes the table and its [`StreamPublisher`]. Any thread may `add` rows
//! or `fail` the table through the publisher; the table's update source takes what arrived
//! once per cycle. The table owns the update source, so the stream ends when the table is
//! dropped: the publisher is then no longer alive and the shutdown callback runs.
//!
//! Behavior: io.deephaven.stream.StreamToBlinkTableAdapter, io.deephaven.stream.TablePublisher
//! and io.deephaven.stream.TableStreamPublisherImpl

use super::{failure, tracked_source, QueryTable, RootSource, TableSpec};
use arrow::array::{Array, ArrayRef};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{locked, UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{ColumnSource, InputTable, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

/// Called at the start of every update cycle, before the rows that arrived are taken: a
/// publisher that batches rows can add them now. An error fails the table.
pub type FlushCallback = Box<dyn Fn() -> Result<(), TableFailure> + Send + Sync>;

/// Called once, when the table can no longer show rows: it was dropped, or it failed.
pub type ShutdownCallback = Box<dyn FnOnce() + Send>;

/// What a stream table calls back.
///
/// Behavior: the `onFlushCallback` and `onShutdownCallback` of
/// io.deephaven.stream.TablePublisher#of
#[derive(Default)]
pub struct StreamCallbacks {
    pub on_flush: Option<FlushCallback>,
    pub on_shutdown: Option<ShutdownCallback>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What arrived since the last cycle.
#[derive(Default)]
struct Pending {
    /// Column arrays in definition order, one entry per `add`, in the order of the adds.
    batches: Vec<Vec<ArrayRef>>,
    failures: Vec<TableFailure>,
}

/// The state the publisher and the table's update source share.
struct Stream {
    definition: TableDefinition,
    /// Behavior: StreamToBlinkTableAdapter#alive
    alive: AtomicBool,
    pending: Mutex<Pending>,
    /// Taken when the stream ends, which also ends a reference cycle through a callback
    /// that holds the publisher.
    on_flush: Mutex<Option<Arc<FlushCallback>>>,
    on_shutdown: Mutex<Option<ShutdownCallback>>,
}

impl Stream {
    fn alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    /// Ends the stream once: nothing more is taken and the shutdown callback runs.
    ///
    /// Behavior: StreamToBlinkTableAdapter#close, TableStreamPublisherImpl#shutdown
    fn close(&self) {
        if !self.alive.swap(false, Ordering::AcqRel) {
            return;
        }
        drop(lock(&self.on_flush).take());
        *lock(&self.pending) = Pending::default();
        let shutdown = lock(&self.on_shutdown).take();
        if let Some(shutdown) = shutdown {
            shutdown();
        }
    }
}

/// The write side of a stream table. Clones publish to the same table.
#[derive(Clone)]
pub struct StreamPublisher {
    stream: Arc<Stream>,
}

impl StreamPublisher {
    /// The definition of the table.
    pub fn definition(&self) -> &TableDefinition {
        &self.stream.definition
    }

    /// Whether the table can still show rows. Once false it stays false: the table was
    /// dropped or has failed (or a failure is about to be delivered to it), and rows added
    /// are dropped.
    ///
    /// Behavior: io.deephaven.stream.TablePublisher#isAlive
    pub fn is_alive(&self) -> bool {
        self.stream.alive()
    }

    /// Hands rows over for the next cycle: one array per column of the definition, in its
    /// order, all of one length. All the rows of one call show in the same cycle, after the
    /// rows of earlier calls. Rows added once the stream has ended, or after a failure that
    /// is not delivered yet, are dropped without an error.
    ///
    /// An error, and nothing added, when the arrays do not fit the definition.
    ///
    /// Behavior: StreamToBlinkTableAdapter#accept
    pub fn add(&self, columns: Vec<ArrayRef>) -> Result<(), TableFailure> {
        let declared = self.stream.definition.columns();
        if columns.len() != declared.len() {
            return Err(failure(format!(
                "java.lang.IllegalStateException: StreamConsumer data length = {} chunks, \
                 expected {}",
                columns.len(),
                declared.len()
            )));
        }
        for (column, values) in declared.iter().zip(&columns) {
            if *values.data_type() != column.column_type.arrow_type() {
                return Err(failure(format!(
                    "column {} is {}, expected {}",
                    column.name,
                    values.data_type(),
                    column.column_type.arrow_type()
                )));
            }
        }
        let size = columns.first().map_or(0, |values| values.len());
        if columns.iter().any(|values| values.len() != size) {
            return Err(failure(
                "io.deephaven.base.verify.AssertionFailure: Assertion failed: asserted \
                 data[0].size() == data[ii].size()",
            ));
        }
        if !self.stream.alive() {
            return Ok(());
        }
        let mut pending = lock(&self.stream.pending);
        if pending.failures.is_empty() && size > 0 {
            pending.batches.push(columns);
        }
        Ok(())
    }

    /// Hands over a snapshot of the rows `table` has now, read from its columns named as
    /// the definition's, which must have the definition's types; other columns are
    /// ignored. A refreshing table is read under its update graph's shared lock.
    ///
    /// Behavior: io.deephaven.stream.TablePublisher#add,
    /// io.deephaven.stream.TableStreamPublisherImpl#add and its `FillChunks`, which read
    /// the columns with io.deephaven.engine.table.Table#getColumnSource(ColumnDefinition)
    pub fn add_table(&self, table: &Arc<dyn Table>) -> Result<(), TableFailure> {
        let available = table.definition();
        let mut indices = Vec::with_capacity(self.stream.definition.columns().len());
        for column in self.stream.definition.columns() {
            let Some(index) = available.index_of(&column.name) else {
                let names: Vec<&str> = available
                    .columns()
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                return Err(failure(format!(
                    "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column \
                     names [{}], available column names are [{}]",
                    column.name,
                    names.join(", ")
                )));
            };
            let found = &available.columns()[index].column_type;
            if *found != column.column_type {
                return Err(failure(format!(
                    "java.lang.ClassCastException: Cannot convert ColumnSource[{}] of type {:?} \
                     to type {:?}",
                    column.name, found, column.column_type
                )));
            }
            indices.push(index);
        }
        let read = || {
            let rows = table.row_set();
            indices
                .iter()
                .map(|&index| table.column(index).chunk(&rows))
                .collect::<Result<Vec<_>, _>>()
        };
        let columns = match table.update_graph() {
            Some(graph) if table.is_refreshing() => locked(graph.as_ref(), read),
            _ => read(),
        }?;
        self.add(columns)
    }

    /// Fails the table on the next cycle, in place of the rows that arrived. The stream
    /// then ends. Nothing happens once it has ended. Failures handed over before one cycle
    /// are delivered together.
    ///
    /// Behavior: io.deephaven.stream.TablePublisher#publishFailure,
    /// StreamToBlinkTableAdapter#acceptFailure
    pub fn fail(&self, failure: TableFailure) {
        if !self.stream.alive() {
            return;
        }
        lock(&self.stream.pending).failures.push(failure);
    }
}

/// A blink table without rows, with the columns of `definition`, and the publisher that
/// feeds it. The table is flat and refreshing, has the blink attribute and no other, and
/// is a root of `graph`.
///
/// On each cycle the table calls `on_flush`, then shows exactly the rows added since the
/// last cycle and removes the ones it showed before, or fails with the failures handed
/// over, which are delivered before any row that arrived with them. `on_shutdown` runs
/// once, on the thread that ends the stream: the cycle thread that delivers a failure, or
/// the thread that drops the table.
///
/// Behavior: io.deephaven.stream.TablePublisher#of, StreamToBlinkTableAdapter (the
/// constructor, #run and #doRefresh)
pub fn stream_table(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    callbacks: StreamCallbacks,
) -> (Arc<QueryTable>, StreamPublisher) {
    stream_table_with_input(graph, definition, callbacks, |_| None)
}

/// [`stream_table`], with the `InputTable` attribute `input` makes of the publisher.
pub(super) fn stream_table_with_input(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    callbacks: StreamCallbacks,
    input: impl FnOnce(&StreamPublisher) -> Option<Arc<dyn InputTable>>,
) -> (Arc<QueryTable>, StreamPublisher) {
    let columns: Vec<Arc<WritableSource>> = definition
        .columns()
        .iter()
        .map(|column| tracked_source(&graph, column.column_type.clone()))
        .collect();
    let mut attributes = TableAttributes::default();
    attributes.blink = true;
    let spec = TableSpec::new(
        definition.clone(),
        RowSet::empty(),
        columns
            .iter()
            .map(|column| column.clone() as Arc<dyn ColumnSource>)
            .collect(),
    )
    .with_attributes(attributes)
    .flat();
    let publisher = StreamPublisher {
        stream: Arc::new(Stream {
            definition,
            alive: AtomicBool::new(true),
            pending: Mutex::new(Pending::default()),
            on_flush: Mutex::new(callbacks.on_flush.map(Arc::new)),
            on_shutdown: Mutex::new(callbacks.on_shutdown),
        }),
    };
    let input = input(&publisher);
    let stream = publisher.stream.clone();
    let table = QueryTable::root(graph, spec, |table| RootSource {
        source: Arc::new(Refresher {
            table,
            columns,
            stream,
        }),
        input,
    });
    (table, publisher)
}

/// The update source of a stream table.
struct Refresher {
    table: Weak<QueryTable>,
    columns: Vec<Arc<WritableSource>>,
    stream: Arc<Stream>,
}

impl Drop for Refresher {
    /// The table is gone.
    ///
    /// Behavior: the `destroy` of the table StreamToBlinkTableAdapter makes, which closes
    /// the adapter
    fn drop(&mut self) {
        self.stream.close();
    }
}

impl Refresher {
    /// Fails the table with `failures` and ends the stream.
    ///
    /// Behavior: StreamToBlinkTableAdapter#deliverFailures and #deliverFailure, with
    /// io.deephaven.util.MultiException#maybeWrapInMultiException for several
    fn deliver(&self, table: &QueryTable, mut failures: Vec<TableFailure>) {
        let failure = if failures.len() == 1 {
            failures.remove(0)
        } else {
            let mut message = "Multiple errors encountered while ingesting stream:\n".to_owned();
            for (index, failure) in failures.iter().enumerate() {
                message.push_str(&format!("Cause {index}: {}\n", failure.message));
            }
            TableFailure { message }
        };
        table.fail(failure);
        self.stream.close();
    }

    /// Makes `batches` the rows of `table` and returns the update that says so.
    ///
    /// Behavior: StreamToBlinkTableAdapter#doRefresh
    fn replace_rows(
        &self,
        table: &QueryTable,
        batches: &[Vec<ArrayRef>],
    ) -> Result<Option<TableUpdate>, TableFailure> {
        let old_size = table.row_set().size();
        let new_size: u64 = batches
            .iter()
            .map(|batch| batch.first().map_or(0, |column| column.len()) as u64)
            .sum();
        if old_size == 0 && new_size == 0 {
            return Ok(None);
        }
        let added = RowSet::flat(new_size);
        for (index, column) in self.columns.iter().enumerate() {
            if new_size > 0 {
                let parts: Vec<&dyn Array> =
                    batches.iter().map(|batch| batch[index].as_ref()).collect();
                let values = arrow::compute::concat(&parts).map_err(|e| failure(e.to_string()))?;
                column.fill(&added, &values)?;
            }
            if old_size > new_size {
                column.remove(&RowSet::from_range(
                    new_size as RowKey,
                    old_size as RowKey - 1,
                ));
            }
        }
        Ok(Some(TableUpdate {
            added,
            removed: RowSet::flat(old_size),
            ..TableUpdate::empty()
        }))
    }
}

impl UpdateSource for Refresher {
    /// Behavior: StreamToBlinkTableAdapter#run and #doRefresh: failures first, then the
    /// flush callback, then failures the callback handed over, then the rows
    fn refresh(&self) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        if !self.stream.alive() {
            return;
        }
        if table.is_failed() {
            return self.stream.close();
        }
        let failures = std::mem::take(&mut lock(&self.stream.pending).failures);
        if !failures.is_empty() {
            return self.deliver(&table, failures);
        }
        let flush = lock(&self.stream.on_flush).clone();
        if let Some(flush) = flush {
            if let Err(e) = flush() {
                return self.deliver(&table, vec![e]);
            }
        }
        let batches = {
            let mut pending = lock(&self.stream.pending);
            if !pending.failures.is_empty() {
                let failures = std::mem::take(&mut pending.failures);
                drop(pending);
                return self.deliver(&table, failures);
            }
            std::mem::take(&mut pending.batches)
        };
        match self.replace_rows(&table, &batches) {
            Ok(Some(update)) => table.notify(update),
            Ok(None) => {}
            Err(e) => self.deliver(&table, vec![e]),
        }
    }
}
