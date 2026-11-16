//! Input tables: tables whose rows are written through the `InputTable` contract.

use super::{
    arrange_columns, failure, mark, tracked_source, Marks, QueryTable, RootSource, TableSpec,
};
use arrow::array::{Array, ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::{Float32Type, Float64Type};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, InputTable, Table, TableAttributes, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An input table that only grows: each `add` appends its rows after the existing ones.
/// The table is flat and append-only, and `delete` is an error.
///
/// `initial`, if given, holds the rows the table starts with.
///
/// Behavior: io.deephaven.engine.table.impl.util.AppendOnlyArrayBackedInputTable#make
pub fn append_only_input_table(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    initial: Option<&RecordBatch>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let mut attributes = TableAttributes::default();
    attributes.add_only = true;
    attributes.append_only = true;
    make(graph, definition, None, attributes, initial)
}

/// An input table with key columns: `add` replaces the row that has the same key, or adds
/// one, and `delete` removes the rows with the given keys. Within one batch the last row
/// for a key wins. A key that is deleted and added again gets its old row key back.
///
/// `initial`, if given, holds the rows the table starts with.
///
/// An error if a key column is not in the definition.
///
/// Whether two floating point keys are one key depends on how many key columns there are:
/// Java compares the values of one key column as boxed objects and those of two or three
/// as a typed tuple (`Writer::key_bytes`).
///
/// # Panics
/// With `unported!` for a vector or array key column, and for no key columns at all.
///
/// Behavior: io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable#make
pub fn keyed_input_table(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    key_columns: &[&str],
    initial: Option<&RecordBatch>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let mut keys = Vec::with_capacity(key_columns.len());
    let mut missing = Vec::new();
    for name in key_columns {
        match definition.index_of(name) {
            Some(index) => keys.push(index),
            None => missing.push(*name),
        }
    }
    if !missing.is_empty() {
        return Err(failure(format!(
            "Missing key columns in definition: {missing:?}"
        )));
    }
    if keys.is_empty() {
        unported!(
            "io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable#makeKeySource",
            "a keyed input table without key columns"
        );
    }
    for &index in &keys {
        // Java keys such a column by the vector or array as an object.
        if definition.columns()[index]
            .column_type
            .list_element()
            .is_some()
        {
            unported!(
                "io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable#makeKeySource",
                "vector or array key column {}",
                definition.columns()[index].name
            );
        }
    }
    make(
        graph,
        definition,
        Some(keys),
        TableAttributes::default(),
        initial,
    )
}

/// An input table that shows each added row for one update cycle: the rows added since the
/// last cycle are the table's rows during the next one, and are removed by the cycle after
/// it. The table is flat, starts without rows and has the blink attribute. It has no key
/// columns, and `delete` is an error. It is a copy, as [`super::copied`] says of one: Java
/// makes it by adding the `InputTable` attribute to its publisher's table.
///
/// `add` returns once the rows are handed over, before a cycle has shown them. Java's `add`
/// reads the columns it needs by name and ignores the others; here the batch must have
/// exactly the table's columns, which is what Java's service checks before it adds.
///
/// Behavior: io.deephaven.stream.TablePublisher#of and #inputTable,
/// io.deephaven.stream.StreamToBlinkTableAdapter
pub fn blink_input_table(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
) -> Arc<QueryTable> {
    let columns: Vec<Arc<WritableSource>> = definition
        .columns()
        .iter()
        .map(|column| tracked_source(&graph, column.column_type.clone()))
        .collect();
    let mut attributes = TableAttributes::default();
    attributes.blink = true;
    let mut spec = TableSpec::new(
        definition.clone(),
        RowSet::empty(),
        columns
            .iter()
            .map(|c| c.clone() as Arc<dyn ColumnSource>)
            .collect(),
    )
    .with_attributes(attributes);
    spec.flat = true;
    let writer = Writer {
        definition,
        key_columns: None,
        columns,
    };
    let table = QueryTable::root(graph, spec, |table| {
        let input = Arc::new(BlinkInput {
            table,
            writer,
            buffer: Mutex::new(Vec::new()),
        });
        RootSource {
            source: input.clone(),
            input: Some(input),
        }
    });
    // Behavior: TablePublisher#inputTable, whose `withAttributes` copies the adapter's
    // blink table
    let marks = Marks {
        copy_of_blink: Some(true),
        ..Marks::default()
    };
    mark(&table, marks);
    table
}

fn make(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    key_columns: Option<Vec<usize>>,
    attributes: TableAttributes,
    initial: Option<&RecordBatch>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let columns: Vec<Arc<WritableSource>> = definition
        .columns()
        .iter()
        .map(|column| tracked_source(&graph, column.column_type.clone()))
        .collect();
    let mut state = RowState {
        next_row: 0,
        row_of_key: BTreeMap::new(),
    };
    let writer = Writer {
        definition: definition.clone(),
        key_columns,
        columns,
    };

    // Behavior: BaseArrayBackedInputTable#processInitial. The graph is idle or the table is
    // not yet visible, so these writes record no previous values.
    let mut changes = Changes::new(RowSet::empty());
    if let Some(initial) = initial {
        let batch = writer.arrange_rows(initial)?;
        writer.add(&mut state, &batch, &mut changes)?;
    }
    let rows: RowSet = changes.added.iter().copied().collect();

    let mut spec = TableSpec::new(
        definition,
        rows,
        writer
            .columns
            .iter()
            .map(|c| c.clone() as Arc<dyn ColumnSource>)
            .collect(),
    )
    .with_attributes(attributes);
    spec.flat = writer.key_columns.is_none();

    Ok(QueryTable::root(graph.clone(), spec, |table| {
        let input = Arc::new(Input {
            table,
            graph,
            writer,
            state: Mutex::new(state),
            queue: Mutex::new(Queue::default()),
        });
        RootSource {
            source: input.clone(),
            input: Some(input),
        }
    }))
}

/// The columns of an input table and how a batch maps onto them.
struct Writer {
    definition: TableDefinition,
    /// Indices of the key columns; `None` for an append-only table.
    key_columns: Option<Vec<usize>>,
    columns: Vec<Arc<WritableSource>>,
}

/// Where rows go.
struct RowState {
    /// The next row key never used.
    next_row: RowKey,
    /// Keyed tables only: the row key of each key ever seen, or `-(row key + 1)` while the
    /// key is deleted. Keys are the row-format bytes of the key columns, which order and
    /// compare the same on every run.
    row_of_key: BTreeMap<Box<[u8]>, RowKey>,
}

/// The net effect of one cycle's changes on the row set.
///
/// Behavior: io.deephaven.engine.table.impl.UpdatableTable.RowSetChangeRecorderImpl
struct Changes {
    /// The table's rows before this cycle.
    before: RowSet,
    added: BTreeSet<RowKey>,
    removed: BTreeSet<RowKey>,
    modified: BTreeSet<RowKey>,
}

impl Changes {
    fn new(before: RowSet) -> Self {
        Self {
            before,
            added: BTreeSet::new(),
            removed: BTreeSet::new(),
            modified: BTreeSet::new(),
        }
    }

    fn add(&mut self, key: RowKey) {
        // A row removed and added back within one cycle looks modified.
        if self.removed.remove(&key) {
            self.modified.insert(key);
        } else {
            self.added.insert(key);
        }
    }

    fn remove(&mut self, key: RowKey) {
        if self.before.contains(key) {
            self.removed.insert(key);
        }
        self.added.remove(&key);
        self.modified.remove(&key);
    }

    fn modify(&mut self, key: RowKey) {
        // A row added and then modified within one cycle stays added.
        if !self.added.contains(&key) {
            self.modified.insert(key);
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.UpdatableTable#run
    fn into_update(self) -> TableUpdate {
        let modified_columns = if self.modified.is_empty() {
            ModifiedColumnSet::empty()
        } else {
            ModifiedColumnSet::All
        };
        TableUpdate {
            added: self.added.into_iter().collect(),
            removed: self.removed.into_iter().collect(),
            modified: self.modified.into_iter().collect(),
            modified_columns,
            ..TableUpdate::empty()
        }
    }
}

impl Writer {
    /// The columns of `batch` in definition order, or an error if it is not a batch of
    /// this table's rows.
    ///
    /// Behavior: io.deephaven.engine.util.input.InputTableUpdater#validateAddOrModify
    fn arrange_rows(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, TableFailure> {
        let declared: Vec<&ColumnDefinition> = self.definition.columns().iter().collect();
        arrange_columns(&declared, batch)
    }

    /// The key columns of `batch` in key order, or an error if it has other columns.
    ///
    /// Behavior: io.deephaven.engine.util.input.InputTableUpdater#validateDelete
    fn arrange_keys(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, TableFailure> {
        let Some(key_columns) = &self.key_columns else {
            return Err(failure("Table doesn't support delete operation"));
        };
        let declared: Vec<&ColumnDefinition> = key_columns
            .iter()
            .map(|&index| &self.definition.columns()[index])
            .collect();
        arrange_columns(&declared, batch)
            .map_err(|e| failure(format!("Invalid Key Table Definition: {e}")))
    }

    /// One byte string per row of `key_values`, equal exactly when Java takes the keys for
    /// one key.
    ///
    /// Java looks a key up in a hash map of objects. The key of one key column is the boxed
    /// value, and of four or more an array of boxed values, so two floating point values
    /// are one key when `Double#equals` says so: every NaN is one key, and `0.0` and `-0.0`
    /// are two. The key of two or three key columns is a typed tuple, which compares with
    /// `DoubleComparisons#eq`: every NaN is one key, and `0.0` and `-0.0` are one key too.
    /// A null is a key of its own in every case.
    ///
    /// Behavior: KeyedArrayBackedInputTable#makeKeySource,
    /// io.deephaven.engine.table.impl.tuplesource.TupleSourceFactory#makeTupleSource,
    /// java.lang.Double#equals, io.deephaven.util.compare.DoubleComparisons#eq, and the
    /// same of `Float`
    fn key_bytes(&self, key_values: &[ArrayRef]) -> Result<Vec<Box<[u8]>>, TableFailure> {
        let zeros_are_one = matches!(key_values.len(), 2 | 3);
        let key_values: Vec<ArrayRef> = key_values
            .iter()
            .map(|values| one_bit_pattern_per_key(values, zeros_are_one))
            .collect();
        let fields = key_values
            .iter()
            .map(|values| SortField::new(values.data_type().clone()))
            .collect();
        let converter = RowConverter::new(fields).map_err(|e| failure(e.to_string()))?;
        let rows = converter
            .convert_columns(&key_values)
            .map_err(|e| failure(e.to_string()))?;
        Ok(rows.iter().map(|row| row.as_ref().into()).collect())
    }

    /// Behavior: AppendOnlyArrayBackedInputTable#processPendingTable and
    /// KeyedArrayBackedInputTable#processPendingTable
    fn add(
        &self,
        state: &mut RowState,
        values: &[ArrayRef],
        changes: &mut Changes,
    ) -> Result<(), TableFailure> {
        let size = values.first().map_or(0, |column| column.len());
        let Some(key_columns) = &self.key_columns else {
            if size == 0 {
                return Ok(());
            }
            let first = state.next_row;
            state.next_row += size as RowKey;
            let rows = RowSet::from_range(first, state.next_row - 1);
            for key in rows.iter() {
                changes.add(key);
            }
            for (column, values) in self.columns.iter().zip(values) {
                column.fill(&rows, values)?;
            }
            return Ok(());
        };

        let key_values: Vec<ArrayRef> = key_columns.iter().map(|&i| values[i].clone()).collect();
        let first_new = state.next_row;
        let mut destinations = Vec::with_capacity(size);
        for key in self.key_bytes(&key_values)? {
            let row = match state.row_of_key.get(&key).copied() {
                None => {
                    state.next_row += 1;
                    state.next_row - 1
                }
                // The key was deleted: it gets its row back.
                Some(deleted) if deleted < 0 => {
                    let row = -(deleted + 1);
                    changes.add(row);
                    row
                }
                Some(row) => {
                    // A row first seen earlier in this batch is recorded as added below.
                    if row < first_new {
                        changes.modify(row);
                    }
                    row
                }
            };
            state.row_of_key.insert(key, row);
            destinations.push(row);
        }
        for row in first_new..state.next_row {
            changes.add(row);
        }
        for (column, values) in self.columns.iter().zip(values) {
            column.fill_by_keys(&destinations, values)?;
        }
        Ok(())
    }

    /// Behavior: KeyedArrayBackedInputTable#processPendingDelete
    fn delete(
        &self,
        state: &mut RowState,
        key_values: &[ArrayRef],
        changes: &mut Changes,
    ) -> Result<(), TableFailure> {
        let mut deleted = Vec::new();
        for key in self.key_bytes(key_values)? {
            if let Some(row) = state.row_of_key.get_mut(&key) {
                if *row >= 0 {
                    changes.remove(*row);
                    deleted.push(*row);
                    *row = -(*row + 1);
                }
            }
        }
        // Release the values. The key keeps its row key for a later add.
        let deleted: RowSet = deleted.into_iter().collect();
        for column in &self.columns {
            column.remove(&deleted);
        }
        Ok(())
    }
}

/// The floating point values of `values` with one bit pattern for every NaN and, with
/// `zeros_are_one`, for both zeros, so that values which are one key have the same bytes.
/// Values of other types are returned as they are.
fn one_bit_pattern_per_key(values: &ArrayRef, zeros_are_one: bool) -> ArrayRef {
    if let Some(doubles) = values.as_any().downcast_ref::<Float64Array>() {
        let key = |value: f64| match value {
            value if value.is_nan() => f64::NAN,
            value if zeros_are_one && value == 0.0 => 0.0,
            value => value,
        };
        Arc::new(doubles.unary::<_, Float64Type>(key))
    } else if let Some(floats) = values.as_any().downcast_ref::<Float32Array>() {
        let key = |value: f32| match value {
            value if value.is_nan() => f32::NAN,
            value if zeros_are_one && value == 0.0 => 0.0,
            value => value,
        };
        Arc::new(floats.unary::<_, Float32Type>(key))
    } else {
        values.clone()
    }
}

#[derive(Default)]
struct Queue {
    pending: Vec<Pending>,
    /// The sequence number of the newest change queued.
    enqueued: u64,
    /// The sequence number of the newest change applied.
    processed: u64,
    /// Why changes can no longer be applied, once that is so.
    broken: Option<TableFailure>,
}

struct Pending {
    delete: bool,
    values: Vec<ArrayRef>,
}

/// The update source and the write side of an input table.
///
/// Behavior: io.deephaven.engine.table.impl.util.BaseArrayBackedInputTable
struct Input {
    table: Weak<QueryTable>,
    graph: Arc<dyn UpdateGraph>,
    writer: Writer,
    state: Mutex<RowState>,
    queue: Mutex<Queue>,
}

impl Input {
    /// Queues a change and waits until an update cycle has applied it.
    ///
    /// Java queues the change, asks its periodic graph for a refresh and waits for it.
    /// `run_cycle` is that request and wait: a graph with its own driver thread, as the
    /// server's has, runs the cycle there and wakes this thread when it has finished; a
    /// stepped graph runs the cycle on this thread.
    ///
    /// Behavior: BaseArrayBackedInputTable.ArrayBackedInputTableUpdater#add, #delete,
    /// #checkBlockingEditSafety and #waitForSequence
    fn submit(&self, delete: bool, values: Vec<ArrayRef>) -> Result<(), TableFailure> {
        if self.graph.current_thread_processes_updates() {
            return Err(failure(
                "Attempted to make a blocking input table edit from a listener or notification",
            ));
        }
        if values.first().map_or(0, |column| column.len()) == 0 {
            return Ok(());
        }
        let sequence = {
            let mut queue = lock(&self.queue);
            if let Some(broken) = &queue.broken {
                return Err(broken.clone());
            }
            queue.enqueued += 1;
            queue.pending.push(Pending { delete, values });
            queue.enqueued
        };
        loop {
            {
                let queue = lock(&self.queue);
                if let Some(broken) = &queue.broken {
                    return Err(broken.clone());
                }
                if queue.processed >= sequence {
                    return Ok(());
                }
            }
            self.graph.run_cycle();
        }
    }

    /// Stops accepting changes and drops the ones not applied.
    fn give_up(&self, why: TableFailure) {
        let mut queue = lock(&self.queue);
        queue.pending.clear();
        queue.broken = Some(why);
    }
}

impl InputTable for Input {
    fn key_columns(&self) -> Vec<String> {
        let columns = self.writer.definition.columns();
        self.writer
            .key_columns
            .iter()
            .flatten()
            .map(|&index| columns[index].name.clone())
            .collect()
    }

    /// Behavior: io.deephaven.engine.util.input.InputTableUpdater#getValueNames
    fn value_columns(&self) -> Vec<String> {
        let keys = self.writer.key_columns.as_deref().unwrap_or_default();
        let columns = self.writer.definition.columns();
        (0..columns.len())
            .filter(|index| !keys.contains(index))
            .map(|index| columns[index].name.clone())
            .collect()
    }

    /// # Panics
    /// If the calling thread holds the graph's shared lock: the cycle that applies the
    /// change could never run.
    fn add(&self, rows: RecordBatch) -> Result<(), TableFailure> {
        self.submit(false, self.writer.arrange_rows(&rows)?)
    }

    /// # Panics
    /// As [`InputTable::add`].
    fn delete(&self, keys: RecordBatch) -> Result<(), TableFailure> {
        self.submit(true, self.writer.arrange_keys(&keys)?)
    }
}

impl UpdateSource for Input {
    /// Behavior: io.deephaven.engine.table.impl.UpdatableTable#run and
    /// BaseArrayBackedInputTable#processPending
    fn refresh(&self) {
        let (pending, last) = {
            let mut queue = lock(&self.queue);
            (std::mem::take(&mut queue.pending), queue.enqueued)
        };
        if pending.is_empty() {
            return;
        }
        let Some(table) = self.table.upgrade() else {
            return self.give_up(failure("the input table no longer exists"));
        };
        if table.is_failed() {
            return self.give_up(failure("the input table has failed"));
        }

        let mut state = lock(&self.state);
        let mut changes = Changes::new((*table.row_set()).clone());
        for change in &pending {
            let applied = if change.delete {
                self.writer.delete(&mut state, &change.values, &mut changes)
            } else {
                self.writer.add(&mut state, &change.values, &mut changes)
            };
            if let Err(e) = applied {
                // The columns may hold part of the change. The table cannot go on.
                table.fail(e.clone());
                return self.give_up(e);
            }
        }
        drop(state);
        table.notify(changes.into_update());
        if table.is_failed() {
            return self.give_up(failure("the input table has failed"));
        }
        lock(&self.queue).processed = last;
    }
}

/// The update source and the write side of a blink input table.
///
/// Behavior: io.deephaven.stream.TableStreamPublisherImpl and
/// io.deephaven.stream.StreamToBlinkTableAdapter
struct BlinkInput {
    table: Weak<QueryTable>,
    writer: Writer,
    /// The batches added since the last cycle, in the order they were added.
    buffer: Mutex<Vec<Vec<ArrayRef>>>,
}

impl BlinkInput {
    /// The table, while it can still show rows.
    fn alive(&self) -> Option<Arc<QueryTable>> {
        self.table.upgrade().filter(|table| !table.is_failed())
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
        for (index, column) in self.writer.columns.iter().enumerate() {
            if new_size > 0 {
                let parts: Vec<&dyn arrow::array::Array> =
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

impl InputTable for BlinkInput {
    /// Behavior: TableStreamPublisherImpl.InputTableAdapter#getKeyNames
    fn key_columns(&self) -> Vec<String> {
        Vec::new()
    }

    /// Behavior: io.deephaven.engine.util.input.InputTableUpdater#getValueNames
    fn value_columns(&self) -> Vec<String> {
        let columns = self.writer.definition.columns();
        columns.iter().map(|column| column.name.clone()).collect()
    }

    /// Hands the rows over for the next cycle. Rows added to a table that no longer exists
    /// or has failed are dropped without an error.
    ///
    /// Behavior: TableStreamPublisherImpl#add, StreamToBlinkTableAdapter#accept
    fn add(&self, rows: RecordBatch) -> Result<(), TableFailure> {
        let values = self.writer.arrange_rows(&rows)?;
        // Checked under the lock `refresh` empties the buffer under, so that nothing is
        // buffered after the last refresh that could have shown it.
        let mut buffer = lock(&self.buffer);
        if self.alive().is_some() && rows.num_rows() > 0 {
            buffer.push(values);
        }
        Ok(())
    }

    /// Behavior: io.deephaven.engine.util.input.InputTableUpdater#delete
    fn delete(&self, _keys: RecordBatch) -> Result<(), TableFailure> {
        Err(failure("Table does not support deletes"))
    }
}

impl UpdateSource for BlinkInput {
    /// Behavior: StreamToBlinkTableAdapter#run
    fn refresh(&self) {
        let batches = std::mem::take(&mut *lock(&self.buffer));
        let Some(table) = self.alive() else {
            return;
        };
        match self.replace_rows(&table, &batches) {
            Ok(Some(update)) => table.notify(update),
            Ok(None) => {}
            Err(e) => table.fail(e),
        }
    }
}
