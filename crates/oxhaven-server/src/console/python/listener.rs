//! Table listeners written in Python, the update graph's lock as Python holds it, and
//! `Table.await_update`: what `deephaven.table_listener` and `deephaven.update_graph` are
//! built on.
//!
//! # Listeners
//!
//! A [`Listener`] is the engine's merged listener over one table (`listen`) or several
//! (`merged_listen`), with the tables of `dependencies` as further things to wait for. Once
//! per cycle in which a table changed, the cycle thread calls the Python function with one
//! [`UpdateData`] per table that changed. An `UpdateData` reads the rows of the change from
//! the table's columns, the removed rows and the rows before a modification from their
//! previous values, and reads nothing once the call has returned, as a Java `TableUpdate`
//! that has been released. What Java does was observed on Java 42.9; `docs/CONSOLE.md`,
//! "Listeners, locks, liveness scopes and execution contexts", has the list.
//!
//! - An exception in the function ends the listener: `on_error` is called once with the
//!   text of the error, the function is never called again, and the table and everything
//!   downstream of it go on. A failure of the table reaches `on_error` the same way.
//! - The Python handle owns the listener. Stopping it, or dropping the handle, ends it; a
//!   table holds its listeners weakly.
//!
//! # The lock rule
//!
//! > **Update graph lock first, interpreter lock second. A thread that holds the
//! > interpreter lock never waits for the update graph lock, or for anything a cycle
//! > holds.**
//!
//! This is the rule of `docs/PYTHON_UDF.md`, and a listener's function is the second thing
//! after a formula that runs Python inside a cycle. How each path here keeps it:
//!
//! - **A callback** runs on the cycle thread, which holds the graph's exclusive lock, and
//!   takes the interpreter lock for the call. It can wait only for a Python thread that is
//!   running, and a Python thread never waits for the graph with the interpreter lock held.
//! - **Inside a callback** the thread is the cycle's: table operations and reads of the
//!   update take no lock, and [`graph_lock`], [`await_update`] and a blocking write to an
//!   input table are refused with Java's text, because each would wait for the cycle that
//!   is running the callback.
//! - **`shared_lock` and `exclusive_lock`** ([`graph_lock`]) release the interpreter lock
//!   while they wait for the graph lock and take it again once they hold it. The Python
//!   code of the `with` block then runs with both, in the order of the rule.
//! - **Starting a listener** with a replay calls Python under the graph's shared lock, which
//!   it took with the interpreter lock released.
//! - **`await_update`** releases the interpreter lock, then gives up the thread's holds of
//!   the graph lock for the wait, and takes them back before the interpreter lock.
//!
//! The test `tests::callbacks_commands_and_locking_threads_all_make_progress` runs all of
//! these against each other and fails when one stops; with the `py.detach` of
//! [`graph_lock`] taken out it does not finish.
//!
//! # What a lock is here
//!
//! A console command of this server holds the graph's shared lock where Java's holds the
//! exclusive one (`docs/CONSOLE.md`, "How it fits together"). To a script the command's
//! hold is the exclusive lock, as in Java: `has_exclusive_lock` is true in a command, and
//! `exclusive_lock` and `shared_lock` nest in it without waiting. On any other thread the
//! two are the two sides of the engine's lock.

use std::cell::Cell;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use oxhaven_contracts::graph::{locked, Dependency, UpdateGraph};
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_engine::graph::{MergedHandler, MergedListener};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyList;

use super::data::{cells, numpy_values};
use super::ops::{failure, guarded};
use super::TableHandle;
use crate::session::ExportError;
use crate::update_graph::PeriodicUpdateGraph;

/// The update graph of the server whose Python session was started last: the one graph a
/// script's tables have.
static GRAPH: RwLock<Option<Arc<PeriodicUpdateGraph>>> = RwLock::new(None);

/// Makes `graph` the graph that the locks of `deephaven.update_graph` are the lock of.
pub(super) fn install(graph: Arc<PeriodicUpdateGraph>) {
    *GRAPH.write().unwrap_or_else(PoisonError::into_inner) = Some(graph);
}

fn graph() -> PyResult<Arc<PeriodicUpdateGraph>> {
    GRAPH
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .ok_or_else(|| PyRuntimeError::new_err("the server has no Python session"))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Behavior: `io.deephaven.engine.updategraph.UpdateGraphLock#checkForIllegalLockFromRefreshThread`,
/// whose text this is
fn refused_in_a_cycle() -> PyErr {
    PyRuntimeError::new_err(
        "java.lang.UnsupportedOperationException: Non-terminal notifications must not lock the \
         update graph",
    )
}

/// Behavior: `io.deephaven.engine.updategraph.UpdateGraphLock.ExclusiveLock#checkForUpgradeAttempt`,
/// whose text this is
fn refused_upgrade() -> PyErr {
    PyRuntimeError::new_err(
        "java.lang.UnsupportedOperationException: Cannot upgrade a shared lock to an exclusive \
         lock",
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Shared,
    Exclusive,
}

/// The holds of the graph lock that Python code took on one thread.
#[derive(Clone, Copy, Default)]
struct Holds {
    shared: usize,
    exclusive: usize,
    /// The side of the engine's lock the outermost of them took; `None` when they nest in
    /// a command, whose own hold they stand on.
    engine: Option<Side>,
}

thread_local! {
    static HOLDS: Cell<Holds> = const { Cell::new(Holds { shared: 0, exclusive: 0, engine: None }) };
}

/// Whether the thread is running a console command, given that `main` says it is the
/// interpreter's main thread: the command holds the engine's shared lock, and nothing
/// Python took accounts for that hold.
fn in_command(graph: &PeriodicUpdateGraph, holds: Holds, main: bool) -> bool {
    main && holds.engine.is_none() && graph.stepped().holds_shared()
}

/// Takes the shared or the exclusive side of the update graph's lock for the calling
/// thread, until [`graph_unlock`]. `main` says whether the thread is the interpreter's
/// main thread, where commands run. See the [module documentation](self).
///
/// Behavior: `io.deephaven.engine.updategraph.UpdateGraphLock.SharedLock#lock`,
/// `ExclusiveLock#lock`: both sides are re-entrant, the shared side can be taken under the
/// exclusive one and not the reverse, and neither can be taken by a thread that runs a
/// cycle
#[pyfunction]
fn graph_lock(py: Python<'_>, exclusive: bool, main: bool) -> PyResult<()> {
    let graph = graph()?;
    if graph.current_thread_processes_updates() {
        return Err(refused_in_a_cycle());
    }
    let mut holds = HOLDS.get();
    let nested = holds.exclusive > 0 || in_command(&graph, holds, main);
    if exclusive {
        if !nested {
            if graph.stepped().holds_shared() {
                return Err(refused_upgrade());
            }
            // the rule: wait for the graph lock without the interpreter lock
            py.detach(|| graph.stepped().acquire_exclusive());
            holds.engine = Some(Side::Exclusive);
        }
        holds.exclusive += 1;
    } else {
        if !nested && holds.shared == 0 {
            py.detach(|| graph.stepped().acquire_shared());
            holds.engine = Some(Side::Shared);
        }
        holds.shared += 1;
    }
    HOLDS.set(holds);
    Ok(())
}

/// Gives up one hold that [`graph_lock`] took on the calling thread.
///
/// Behavior: `java.util.concurrent.locks.ReentrantReadWriteLock`, which refuses an unlock
/// by a thread that does not hold the lock, and under which a thread that gives up the
/// write lock keeps a read lock it took meanwhile
#[pyfunction]
fn graph_unlock(py: Python<'_>, exclusive: bool) -> PyResult<()> {
    let graph = graph()?;
    let mut holds = HOLDS.get();
    let count = if exclusive {
        &mut holds.exclusive
    } else {
        &mut holds.shared
    };
    if *count == 0 {
        return Err(PyRuntimeError::new_err(
            "java.lang.IllegalMonitorStateException: attempt to unlock the update graph lock, \
             not locked by current thread",
        ));
    }
    *count -= 1;
    match (holds.engine, exclusive) {
        (Some(Side::Exclusive), true) if holds.exclusive == 0 => {
            graph.stepped().release_exclusive();
            holds.engine = None;
            if holds.shared > 0 {
                py.detach(|| graph.stepped().acquire_shared());
                holds.engine = Some(Side::Shared);
            }
        }
        (Some(Side::Shared), false) if holds.shared == 0 => {
            graph.stepped().release_shared();
            holds.engine = None;
        }
        _ => {}
    }
    HOLDS.set(holds);
    Ok(())
}

/// Whether the calling thread holds the shared side and the exclusive side of the update
/// graph's lock, as a script is told: a command holds the exclusive one.
///
/// Behavior: `UpdateGraphLock.SharedLock#isHeldByCurrentThread`,
/// `ExclusiveLock#isHeldByCurrentThread`
#[pyfunction]
fn graph_holds(main: bool) -> PyResult<(bool, bool)> {
    let graph = graph()?;
    let holds = HOLDS.get();
    let command = !graph.current_thread_processes_updates() && in_command(&graph, holds, main);
    Ok((holds.shared > 0, holds.exclusive > 0 || command))
}

/// Whether the calling thread is running an update cycle: a listener's function, or a
/// function called from a formula.
///
/// Behavior: `io.deephaven.engine.updategraph.UpdateGraph#currentThreadProcessesUpdates`
#[pyfunction]
fn graph_in_cycle() -> PyResult<bool> {
    Ok(graph()?.current_thread_processes_updates())
}

/// A table and its change in one cycle.
type Change = (Arc<dyn Table>, Arc<TableUpdate>);

/// The change of one table in one cycle, as a Python listener reads it.
#[pyclass(frozen, module = "_oxhaven")]
struct UpdateData {
    /// The table and its change; `None` once the call it was made for has returned, so
    /// that an update a script keeps does not keep the table.
    change: Mutex<Option<Change>>,
}

impl UpdateData {
    fn new(table: Arc<dyn Table>, update: Arc<TableUpdate>) -> Self {
        Self {
            change: Mutex::new(Some((table, update))),
        }
    }

    /// Every row of `table` as added: what a listener that asks for a replay is given.
    ///
    /// Behavior: `io.deephaven.integrations.python.PythonReplayListenerAdapter#replay`
    fn replay(table: Arc<dyn Table>) -> Self {
        let update = TableUpdate {
            added: (*table.row_set()).clone(),
            ..TableUpdate::empty()
        };
        Self::new(table, Arc::new(update))
    }

    /// Behavior: `io.deephaven.engine.table.impl.TableUpdateImpl#release`
    fn release(&self) {
        lock(&self.change).take();
    }
}

#[pymethods]
impl UpdateData {
    /// The values of the columns `columns` for the rows `rows` of the change: `added`,
    /// `removed`, `modified` or `modified_prev`, the last two being the modified rows now
    /// and before the change. With `count`, only that many rows from the `first`-th on.
    /// Each column is what `_oxhaven.column_values` gives with `numpy`, and a list of
    /// Python values without. `None` once the listener's call has returned.
    ///
    /// The removed rows and the rows before a modification are read from the previous
    /// values of the columns, at the keys the rows had before the cycle. Java's reader
    /// reads the previous values at the keys the modified rows have now, which are other
    /// rows' when the cycle shifted them.
    ///
    /// Behavior: `io.deephaven.integrations.python.PythonTableDataReader#readChunkColumnMajor`
    /// as `deephaven.table_listener.TableUpdate` calls it
    #[pyo3(signature = (rows, columns, numpy, first=0, count=None))]
    fn read<'py>(
        &self,
        py: Python<'py>,
        rows: &str,
        columns: Vec<String>,
        numpy: bool,
        first: u64,
        count: Option<u64>,
    ) -> PyResult<Option<Vec<Bound<'py, PyAny>>>> {
        let Some((table, update)) = lock(&self.change).clone() else {
            return Ok(None);
        };
        let (keys, previous) = match rows {
            "added" => (update.added.clone(), false),
            "removed" => (update.removed.clone(), true),
            "modified" => (update.modified.clone(), false),
            "modified_prev" => {
                let before = update.shifted.inverse().apply(&update.modified);
                let before = before.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                (before, true)
            }
            other => {
                return Err(PyValueError::new_err(format!(
                    "not rows of an update: {other}"
                )))
            }
        };
        let keys = match count {
            Some(count) => keys.sub_set_by_position_range(first, first.saturating_add(count)),
            None => keys,
        };
        let definition = table.definition();
        let indices = columns
            .iter()
            .map(|name| {
                definition.index_of(name).ok_or_else(|| {
                    PyRuntimeError::new_err(format!("the table has no column {name}"))
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let read = py.detach(|| {
            guarded(std::slice::from_ref(&table), || {
                let chunks = indices.iter().map(|index| {
                    let source = table.column(*index);
                    Ok(if previous {
                        source.prev_chunk(&keys)?
                    } else {
                        source.chunk(&keys)?
                    })
                });
                chunks.collect::<Result<Vec<_>, ExportError>>()
            })
        });
        let read = read.map_err(failure)?;
        let types = indices.iter().map(|index| &definition.columns()[*index]);
        let values = types.zip(&read).map(|(column, values)| {
            if numpy {
                numpy_values(py, &column.column_type, values)
            } else {
                Ok(cells(py, &column.column_type, values, false)?.into_any())
            }
        });
        values.collect::<PyResult<Vec<_>>>().map(Some)
    }

    /// The names of the columns that the modified rows may have changed in; none when
    /// every column may have. `None` once the listener's call has returned.
    ///
    /// Behavior: `io.deephaven.engine.table.ModifiedColumnSet#dirtyColumnNames`, which is
    /// empty for `ModifiedColumnSet#ALL`
    fn modified_columns(&self) -> Option<Vec<String>> {
        let (table, update) = lock(&self.change).clone()?;
        let columns = table.definition().columns().iter();
        Some(match &update.modified_columns {
            ModifiedColumnSet::All => Vec::new(),
            ModifiedColumnSet::Columns(changed) => columns
                .enumerate()
                .filter(|(index, _)| changed.contains(index))
                .map(|(_, column)| column.name.clone())
                .collect(),
        })
    }
}

/// What a [`Listener`] does with the changes of its tables.
struct Calls {
    tables: Vec<Arc<dyn Table>>,
    /// Called with a list of an [`UpdateData`] or `None` per table and whether this is a
    /// replay. For a cycle it returns `None`, or the text of the exception that the
    /// listener's function raised.
    deliver: Py<PyAny>,
    /// Called with the text of a failure.
    on_error: Py<PyAny>,
}

impl Calls {
    /// Calls `deliver` with `updates`, which read nothing after the call.
    fn call<'py>(
        &self,
        py: Python<'py>,
        updates: Vec<Option<UpdateData>>,
        replay: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates = updates
            .into_iter()
            .map(|update| update.map(|update| Py::new(py, update)).transpose())
            .collect::<PyResult<Vec<_>>>()?;
        let outcome =
            PyList::new(py, &updates).and_then(|list| self.deliver.bind(py).call1((list, replay)));
        for update in updates.iter().flatten() {
            update.get().release();
        }
        outcome
    }
}

impl MergedHandler for Calls {
    /// Runs on the cycle thread, which holds the graph's exclusive lock and takes the
    /// interpreter lock here: graph first, interpreter second.
    ///
    /// Behavior: `PythonReplayListenerAdapter#onUpdate`,
    /// `io.deephaven.integrations.python.PythonMergedListenerAdapter#process`
    fn process(&self, updates: &[Option<Arc<TableUpdate>>]) -> Result<(), TableFailure> {
        Python::attach(|py| {
            let updates = updates.iter().zip(&self.tables);
            let updates = updates
                .map(|(update, table)| {
                    let update = update.as_ref()?;
                    Some(UpdateData::new(table.clone(), update.clone()))
                })
                .collect();
            let raised = self
                .call(py, updates, false)
                .and_then(|text| text.extract::<Option<String>>());
            match raised {
                Ok(None) => Ok(()),
                Ok(Some(message)) => Err(TableFailure { message }),
                Err(error) => Err(TableFailure {
                    message: error.to_string(),
                }),
            }
        })
    }

    /// Behavior: `PythonReplayListenerAdapter#onFailureInternal`, which logs an exception
    /// of the Python callback under this text and goes on
    fn on_failure(&self, failure: &TableFailure) {
        Python::attach(|py| {
            if let Err(error) = self.on_error.call1(py, (failure.message.as_str(),)) {
                tracing::error!("Python on_error callback failed: {error}");
            }
        });
    }
}

/// A Python listener of one table or of several, with the tables it waits for.
///
/// Behavior: `io.deephaven.integrations.python.PythonReplayListenerAdapter`,
/// `PythonMergedListenerAdapter`
#[pyclass(frozen, module = "_oxhaven")]
struct Listener {
    /// `None` once a liveness scope has released the listener.
    parts: Mutex<Option<Parts>>,
}

#[derive(Clone)]
struct Parts {
    calls: Arc<Calls>,
    graph: Arc<dyn UpdateGraph>,
    dependencies: Vec<Arc<dyn Dependency>>,
    /// The engine's listener while this one is started. The tables hold it weakly.
    subscription: Option<Arc<MergedListener>>,
}

#[pymethods]
impl Listener {
    /// A listener of `tables`, which must be refreshing, that is not started.
    #[new]
    fn new(
        tables: Vec<PyRef<'_, TableHandle>>,
        dependencies: Vec<PyRef<'_, TableHandle>>,
        deliver: Py<PyAny>,
        on_error: Py<PyAny>,
    ) -> PyResult<Self> {
        let tables: Vec<Arc<dyn Table>> = tables.iter().map(|table| table.table.clone()).collect();
        let graph = tables.iter().find_map(|table| table.update_graph());
        let graph = match graph {
            Some(graph) if tables.iter().all(|table| table.is_refreshing()) => graph,
            _ => return Err(PyRuntimeError::new_err("table must be a refreshing table.")),
        };
        let dependencies = dependencies.iter().map(|table| {
            let dependency: Arc<dyn Dependency> = table.table.clone();
            dependency
        });
        let parts = Parts {
            dependencies: dependencies.collect(),
            calls: Arc::new(Calls {
                tables,
                deliver,
                on_error,
            }),
            graph,
            subscription: None,
        };
        Ok(Self {
            parts: Mutex::new(Some(parts)),
        })
    }

    /// Starts listening. With `replay` the function is first called on this thread with
    /// every row of every table as added, and what it raises is raised here; the replay
    /// and the subscription are under one hold of the graph's shared lock, so no change
    /// falls between them.
    ///
    /// Behavior: `deephaven.table_listener.TableListenerHandle#start`,
    /// `MergedListenerHandle#start`
    fn start(&self, py: Python<'_>, replay: bool) -> PyResult<()> {
        let Some(Parts {
            calls,
            graph,
            dependencies,
            ..
        }) = lock(&self.parts).clone()
        else {
            return Err(PyRuntimeError::new_err(
                "io.deephaven.engine.liveness.LivenessStateException: the listener was \
                 released with its liveness scope and is no longer live",
            ));
        };
        // the rule: the graph lock is taken without the interpreter lock, which the
        // replay then takes under it
        py.detach(|| {
            locked(graph.as_ref(), || {
                if replay {
                    Python::attach(|py| {
                        let updates = calls.tables.iter();
                        let updates = updates
                            .map(|table| Some(UpdateData::replay(table.clone())))
                            .collect();
                        calls.call(py, updates, true).map(drop)
                    })?;
                }
                let handler: Weak<dyn MergedHandler> = Arc::downgrade(&calls) as _;
                let subscription = MergedListener::subscribe(
                    graph.clone(),
                    calls.tables.clone(),
                    dependencies,
                    handler,
                );
                if let Some(parts) = lock(&self.parts).as_mut() {
                    parts.subscription = Some(subscription);
                }
                Ok(())
            })
        })
    }

    /// Stops listening. A call that the running cycle has already queued is still made.
    ///
    /// Behavior: `io.deephaven.engine.table.Table#removeUpdateListener`
    fn stop(&self) {
        if let Some(parts) = lock(&self.parts).as_mut() {
            parts.subscription = None;
        }
    }

    /// Stops listening and gives up the tables and the functions, for good.
    fn release(&self) {
        lock(&self.parts).take();
    }
}

/// The listener of [`await_update`]: says when its table changed or failed.
struct Awaited {
    table: Arc<dyn Table>,
    changed: Mutex<bool>,
    wake: Condvar,
}

impl Awaited {
    fn signal(&self) {
        *lock(&self.changed) = true;
        self.wake.notify_all();
    }
}

impl Dependency for Awaited {
    fn satisfied(&self, step: u64) -> bool {
        self.table.satisfied(step)
    }
}

impl UpdateListener for Awaited {
    fn on_update(&self, _: &TableUpdate) -> Result<(), TableFailure> {
        self.signal();
        Ok(())
    }

    fn on_failure(&self, _: &TableFailure) {
        self.signal();
    }
}

/// How long [`await_update`] waits before it looks for a request to stop the command.
const AWAIT_SLICE: Duration = Duration::from_millis(50);

/// Waits until the refreshing table `source` changes, or for `timeout_ms` milliseconds;
/// returns whether it changed. The thread gives up its holds of the graph lock for the
/// wait, as a command must for a cycle to run, and has them again when this returns. A
/// request to stop the command ends the wait.
///
/// Behavior: `io.deephaven.engine.table.impl.BaseTable#awaitUpdate`, which waits on a
/// condition of the exclusive lock, and so is refused where that lock is: in a cycle, and
/// under the shared lock
#[pyfunction]
#[pyo3(signature = (source, timeout_ms=None))]
fn await_update(
    py: Python<'_>,
    source: PyRef<'_, TableHandle>,
    timeout_ms: Option<i64>,
) -> PyResult<bool> {
    let graph = graph()?;
    if graph.current_thread_processes_updates() {
        return Err(refused_in_a_cycle());
    }
    if HOLDS.get().engine == Some(Side::Shared) {
        return Err(refused_upgrade());
    }
    let awaited = Arc::new(Awaited {
        table: source.table.clone(),
        changed: Mutex::new(false),
        wake: Condvar::new(),
    });
    let listener: Arc<dyn UpdateListener> = awaited.clone();
    let deadline = timeout_ms
        .map(|timeout| Instant::now() + Duration::from_millis(timeout.max(0).unsigned_abs()));
    py.detach(|| {
        locked(graph.as_ref(), || awaited.table.add_listener(&listener));
        graph.stepped().waiting_for_cycle(|| loop {
            let slice = match deadline {
                Some(deadline) => deadline
                    .saturating_duration_since(Instant::now())
                    .min(AWAIT_SLICE),
                None => AWAIT_SLICE,
            };
            let changed = lock(&awaited.changed);
            let (changed, _) = awaited
                .wake
                .wait_timeout_while(changed, slice, |changed| !*changed)
                .unwrap_or_else(PoisonError::into_inner);
            if *changed {
                return Ok(true);
            }
            drop(changed);
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Ok(false);
            }
            // no graph lock is held here, and the interpreter lock is given back before
            // the graph lock is taken again
            Python::attach(|py| py.check_signals())?;
        })
    })
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<Listener>()?;
    native.add_class::<UpdateData>()?;
    native.add_function(wrap_pyfunction!(graph_lock, native)?)?;
    native.add_function(wrap_pyfunction!(graph_unlock, native)?)?;
    native.add_function(wrap_pyfunction!(graph_holds, native)?)?;
    native.add_function(wrap_pyfunction!(graph_in_cycle, native)?)?;
    native.add_function(wrap_pyfunction!(await_update, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
