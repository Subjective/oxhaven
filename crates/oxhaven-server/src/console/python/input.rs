//! Input tables written from Python: `InputTable.add` and `delete`, their asynchronous
//! forms, and the table of a `DynamicTableWriter`.
//!
//! A change is checked, and its rows are read, by the code the input table service uses
//! (`crate::inputtable`), on the thread that asks for it. What differs between the forms is
//! who waits for the update cycle that applies it:
//!
//! - **The synchronous forms** wait on the calling thread. A console command holds the
//!   update graph's shared lock, which keeps cycles out, so it runs inside
//!   `oxhaven_engine::graph::yielding_to_cycles`: the wait gives the lock up and takes it
//!   again, and the command then reads its own change. Java's command holds the exclusive
//!   lock and gives that up the same way, by waiting on the lock's condition. The
//!   interpreter lock is released first and taken again last, which keeps the order of
//!   `docs/PYTHON_UDF.md`: update graph lock, then interpreter lock.
//! - **The asynchronous forms** and the rows of a writer are handed to one thread, the
//!   writer thread, which applies them in the order they were handed over and then calls
//!   the Python callbacks. It holds no lock while it waits. Changes that are waiting
//!   together are applied in one cycle where they can be made one change: additions to one
//!   table, or deletions from one.
//!
//! A synchronous change made while asynchronous ones are waiting goes through the writer
//! thread too, so that it is applied after them, as Java's one queue per table does.

#[cfg(test)]
mod tests;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::{locked, UpdateGraph};
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{InputTable, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::table::{append_only_input_table, QueryTable, TableSpec};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use super::{error_text, input_values, runtime, TableHandle};
use crate::inputtable::{checked_add, checked_delete, Change};
use crate::session::{ExportError, ExportValue};

/// Told how a change ended, on the writer thread.
type Done = Box<dyn FnOnce(Result<(), String>) + Send>;

/// A change for the writer thread; without a change when there was no row to change, which
/// still ends in its turn.
struct Job {
    change: Option<Change>,
    done: Done,
}

/// The way to the writer thread.
struct Writer {
    jobs: Sender<Job>,
    /// The jobs handed over and not ended.
    waiting: Arc<AtomicUsize>,
}

impl Writer {
    /// The writer thread of the process, started at the first use.
    fn get() -> &'static Writer {
        static WRITER: OnceLock<Writer> = OnceLock::new();
        WRITER.get_or_init(|| {
            let (jobs, queue) = channel();
            let waiting = Arc::new(AtomicUsize::new(0));
            let counted = waiting.clone();
            let spawned = std::thread::Builder::new()
                .name("input-table-writer".to_string())
                .spawn(move || write(&queue, &counted));
            if let Err(e) = spawned {
                panic!("cannot start the input table writer thread: {e}");
            }
            Writer { jobs, waiting }
        })
    }

    fn submit(&self, change: Option<Change>, done: Done) {
        self.waiting.fetch_add(1, Ordering::SeqCst);
        if let Err(refused) = self.jobs.send(Job { change, done }) {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            (refused.0.done)(Err("the input table writer thread has ended".to_string()));
        }
    }
}

/// Whether `next` can be made part of `change`: the same kind of change to the same input
/// table, with the columns in the same order.
fn joins(change: &Change, next: &Change) -> bool {
    change.delete == next.delete
        && std::ptr::addr_eq(Arc::as_ptr(&change.input), Arc::as_ptr(&next.input))
        && change.rows.schema() == next.rows.schema()
}

/// The writer thread: takes every job that is waiting, applies the changes in order, each
/// run of changes that [`joins`] as one, and tells each job how its change ended.
fn write(queue: &Receiver<Job>, waiting: &AtomicUsize) {
    while let Ok(first) = queue.recv() {
        let mut jobs = vec![first];
        jobs.extend(queue.try_iter());

        let mut runs: Vec<(Option<Change>, Vec<Done>)> = Vec::new();
        for Job { change, done } in jobs {
            let joined = match (runs.last_mut(), &change) {
                (Some((Some(run), _)), Some(next)) if joins(run, next) => {
                    concat_batches(&run.rows.schema(), [&run.rows, &next.rows]).ok()
                }
                _ => None,
            };
            match (joined, runs.last_mut()) {
                (Some(rows), Some((Some(run), dones))) => {
                    run.rows = rows;
                    dones.push(done);
                }
                _ => runs.push((change, vec![done])),
            }
        }

        for (change, dones) in runs {
            let ended = match change {
                None => Ok(()),
                // The engine panics when the graph has stopped; the thread must go on to
                // tell the jobs that are waiting.
                Some(change) => catch_unwind(AssertUnwindSafe(|| change.apply()))
                    .unwrap_or_else(|_| {
                        Err(ExportError::Internal(
                            "the change to the input table could not be applied".to_string(),
                        ))
                    })
                    .map_err(error_text),
            };
            for done in dones {
                waiting.fetch_sub(1, Ordering::SeqCst);
                done(ended.clone());
            }
        }
    }
}

/// Makes `change` and returns once it is visible, after every change handed to the writer
/// thread before. The calling thread must not hold the interpreter lock; it may hold the
/// graph's shared lock inside `yielding_to_cycles`.
///
/// Behavior: `BaseArrayBackedInputTable.ArrayBackedInputTableUpdater#checkBlockingEditSafety`,
/// whose texts these are: for a thread that runs a cycle, and for one that took the shared
/// lock with `deephaven.update_graph.shared_lock` (a command's own hold is given up for the
/// wait, and so is an exclusive one)
fn apply_in_order(graph: &dyn UpdateGraph, change: Change) -> Result<(), ExportError> {
    if graph.current_thread_processes_updates() {
        return Err(ExportError::Internal(
            "java.lang.UnsupportedOperationException: Attempted to make a blocking input table \
             edit from a listener or notification. This is unsupported, because it will block \
             the update graph from making progress and hang indefinitely."
                .to_string(),
        ));
    }
    if super::listener::holds_the_shared_lock_only() {
        return Err(ExportError::Internal(
            "java.lang.UnsupportedOperationException: Attempted to make a blocking input table \
             edit while holding the update graph's shared lock. This is unsupported, because it \
             will block the update graph from making progress and hang indefinitely."
                .to_string(),
        ));
    }
    let writer = Writer::get();
    if writer.waiting.load(Ordering::SeqCst) == 0 {
        return change.apply();
    }
    let (ended, end) = channel();
    writer.submit(
        Some(change),
        Box::new(move |result| {
            let _ = ended.send(result);
        }),
    );
    loop {
        // The writer thread waits for cycles that this thread may be keeping out.
        match end.recv_timeout(Duration::from_millis(1)) {
            Ok(result) => return result.map_err(ExportError::Internal),
            Err(RecvTimeoutError::Timeout) => graph.run_cycle(),
            Err(RecvTimeoutError::Disconnected) => {
                return Err(ExportError::Internal(
                    "the input table writer thread has ended".to_string(),
                ))
            }
        }
    }
}

fn checked(
    target: Arc<dyn Table>,
    rows: Arc<dyn Table>,
    delete: bool,
) -> Result<Option<Change>, ExportError> {
    let (target, rows) = (ExportValue::Table(target), ExportValue::Table(rows));
    if delete {
        checked_delete(&target, &rows)
    } else {
        checked_add(&target, &rows)
    }
}

fn raised(error: ExportError) -> PyErr {
    PyRuntimeError::new_err(error_text(error))
}

/// Adds the rows of `rows` to the input table `target`, or with `delete` deletes the rows
/// whose keys `rows` holds, and returns once the change is visible in `target`.
///
/// Behavior: `io.deephaven.engine.util.input.InputTableUpdater#add`, `#delete`
#[pyfunction]
fn input_table_change(
    py: Python<'_>,
    target: PyRef<'_, TableHandle>,
    rows: PyRef<'_, TableHandle>,
    delete: bool,
) -> PyResult<()> {
    let runtime = runtime()?;
    let (target, rows) = (target.table.clone(), rows.table.clone());
    py.detach(|| match checked(target, rows, delete)? {
        Some(change) => apply_in_order(runtime.context.graph.as_ref(), change),
        None => Ok(()),
    })
    .map_err(raised)
}

/// As [`input_table_change`], without waiting: the change is checked and its rows are read
/// now, and it is made later, in the order of the calls. Then `on_success` is called
/// without arguments, or `on_error` with the text of the failure. An exception a callback
/// raises is logged.
///
/// Behavior: `InputTableUpdater#addAsync`, `#deleteAsync`,
/// `io.deephaven.integrations.python.PythonInputTableStatusListenerAdapter`
#[pyfunction]
fn input_table_change_async(
    py: Python<'_>,
    target: PyRef<'_, TableHandle>,
    rows: PyRef<'_, TableHandle>,
    delete: bool,
    on_success: Option<Py<PyAny>>,
    on_error: Py<PyAny>,
) -> PyResult<()> {
    let (target, rows) = (target.table.clone(), rows.table.clone());
    let change = py
        .detach(|| checked(target, rows, delete))
        .map_err(raised)?;
    let done: Done = Box::new(move |ended| {
        Python::attach(|py| {
            let called = match ended {
                Ok(()) => on_success.map(|call| call.call0(py)).transpose().map(drop),
                Err(text) => on_error.call1(py, (text,)).map(drop),
            };
            if let Err(error) = called {
                tracing::error!("a callback of an input table change failed: {error}");
            }
        });
    });
    Writer::get().submit(change, done);
    Ok(())
}

/// The names of the key columns and of the value columns of the input table `target`
/// writes to; `None` if it is not an input table.
///
/// Behavior: `InputTableUpdater#from`, `#getKeyNames`, `#getValueNames`
#[pyfunction]
fn input_table_names(target: PyRef<'_, TableHandle>) -> Option<(Vec<String>, Vec<String>)> {
    let input = target.table.input_table()?;
    Some((input.key_columns(), input.value_columns()))
}

/// The write side of the table of a `DynamicTableWriter`.
#[pyclass(frozen, module = "_oxhaven")]
struct TableWriter {
    input: Arc<dyn InputTable>,
    definition: TableDefinition,
    schema: SchemaRef,
}

#[pymethods]
impl TableWriter {
    /// Hands one row to the writer thread: `values` holds one value per column, as
    /// `deephaven.dtypes.array` would hold it. The row is in the table after a later
    /// update cycle. A row with a value that is not of its column's type is not written.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.util.DynamicTableWriter#logRowPermissive`,
    /// whose text for a wrong number of values this is
    fn write_row(&self, py: Python<'_>, values: Vec<Bound<'_, PyAny>>) -> PyResult<()> {
        let columns = self.definition.columns();
        if values.len() != columns.len() {
            return Err(PyRuntimeError::new_err(format!(
                "java.lang.RuntimeException: Incompatible logRowPermissive call, values length={} \
                 != setters={}",
                values.len(),
                columns.len()
            )));
        }
        let mut arrays = Vec::with_capacity(columns.len());
        for (column, value) in columns.iter().zip(values) {
            let cell = PyList::new(py, [value])?;
            arrays.push(input_values(&column.column_type, cell.as_any())?);
        }
        let rows = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|problem| PyRuntimeError::new_err(problem.to_string()))?;
        let change = Change {
            input: self.input.clone(),
            rows,
            delete: false,
        };
        let done: Done = Box::new(|ended| {
            if let Err(text) = ended {
                tracing::error!("a row of a DynamicTableWriter was not written: {text}");
            }
        });
        Writer::get().submit(Some(change), done);
        Ok(())
    }
}

/// A writer and its table, which has the columns of `source` and no rows. The table is
/// add-only and append-only and is not an input table: it follows one that only the
/// writer reaches.
///
/// Behavior: `io.deephaven.engine.table.impl.util.DynamicTableWriter#getTable`, an
/// `UpdateSourceQueryTable` with the `ADD_ONLY` and `APPEND_ONLY` attributes
#[pyfunction]
fn table_writer(
    py: Python<'_>,
    source: PyRef<'_, TableHandle>,
) -> PyResult<(TableWriter, TableHandle)> {
    let runtime = runtime()?;
    let definition = source.table.definition().clone();
    let made = py.detach(|| -> Result<_, TableFailure> {
        let graph = runtime.context.graph.clone();
        let written = append_only_input_table(graph.clone(), definition.clone(), None)?;
        let input = written.input_table().ok_or_else(|| TableFailure {
            message: "the engine made an input table without a write side".to_string(),
        })?;
        let written: Arc<dyn Table> = written;
        let shown = locked(graph.as_ref(), || {
            QueryTable::derive(&written, || {
                let columns = (0..definition.columns().len())
                    .map(|index| written.column(index))
                    .collect();
                let rows = (*written.row_set()).clone();
                let mut attributes = TableAttributes::default();
                attributes.add_only = true;
                attributes.append_only = true;
                let spec = TableSpec::new(definition.clone(), rows, columns)
                    .with_attributes(attributes)
                    .flat();
                let follow = |_: &QueryTable, upstream: &TableUpdate| Ok(upstream.clone());
                Ok((spec, follow))
            })
        })?;
        Ok((input, shown))
    });
    let (input, shown) = made.map_err(|failure| PyRuntimeError::new_err(failure.message))?;
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
        .collect();
    let writer = TableWriter {
        input,
        definition,
        schema: Arc::new(Schema::new(fields)),
    };
    Ok((writer, TableHandle { table: shown }))
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<TableWriter>()?;
    native.add_function(wrap_pyfunction!(input_table_change, native)?)?;
    native.add_function(wrap_pyfunction!(input_table_change_async, native)?)?;
    native.add_function(wrap_pyfunction!(input_table_names, native)?)?;
    native.add_function(wrap_pyfunction!(table_writer, native)?)?;
    Ok(())
}
