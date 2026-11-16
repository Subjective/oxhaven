//! Partitioned tables for the `deephaven` package: the handle its `PartitionedTable`
//! wraps, and the functions that make one.
//!
//! Each method is the engine's operation of that name (`oxhaven_engine::ops::partitioned`),
//! run as the partitioned table service runs it: under the update graph's shared lock when
//! a table is refreshing, and with what is not ported answered by the name of the Java
//! method.

use std::sync::Arc;

use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::ops::partitioned::{of_aggregation, Transformer};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use super::ops::failure;
use super::TableHandle;
use crate::partitioned::{guarded, merge, partition, Partitioned};
use crate::session::ExportError;

/// A reference to a partitioned table of the engine, as Python holds it. The Python
/// `PartitionedTable` class wraps one.
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct PartitionedHandle {
    pub(super) partitioned: Partitioned,
}

fn handle(partitioned: Partitioned) -> PartitionedHandle {
    PartitionedHandle { partitioned }
}

#[pymethods]
impl PartitionedHandle {
    /// Behavior: `io.deephaven.engine.table.PartitionedTable#table`
    fn table(&self) -> TableHandle {
        TableHandle {
            table: self.partitioned.table().clone(),
        }
    }

    /// Behavior: `PartitionedTable#keyColumnNames`
    fn key_columns(&self) -> Vec<String> {
        self.partitioned.key_columns().to_vec()
    }

    /// Behavior: `PartitionedTable#uniqueKeys`
    fn unique_keys(&self) -> bool {
        self.partitioned.unique_keys()
    }

    /// Behavior: `PartitionedTable#constituentColumnName`
    fn constituent_column(&self) -> String {
        self.partitioned.constituent_column().to_string()
    }

    /// The key columns that every constituent has too, with the value of its row of the
    /// underlying table in every row.
    ///
    /// Behavior: `PartitionedTableImpl#getConsistentKeyColumnNames`
    fn consistent_key_columns(&self) -> Vec<String> {
        self.partitioned.consistent_key_columns().to_vec()
    }

    /// Whether `other` has the same underlying table.
    fn is_same(&self, other: &PartitionedHandle) -> bool {
        Arc::ptr_eq(self.partitioned.table(), other.partitioned.table())
    }

    /// Behavior: `PartitionedTable#constituentChangesPermitted`
    fn constituent_changes_permitted(&self) -> bool {
        self.partitioned.constituent_changes_permitted()
    }

    /// A table without rows that has the columns of a constituent: what the definition
    /// of the constituents is read from, and what a transform is tried on.
    ///
    /// Behavior: `PartitionedTable#constituentDefinition`,
    /// `io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#emptyConstituent`
    fn empty_constituent(&self) -> PyResult<TableHandle> {
        let table = self.partitioned.empty_constituent();
        table
            .map(|table| TableHandle { table })
            .map_err(|error| failure(error.into()))
    }

    /// Behavior: `PartitionedTable#merge`
    fn merge(&self, py: Python<'_>) -> PyResult<TableHandle> {
        py.detach(|| merge(&self.partitioned))
            .map(|table| TableHandle { table })
            .map_err(failure)
    }

    /// The constituents now, in the order of the rows of the underlying table.
    ///
    /// Behavior: `PartitionedTable#constituents`
    fn constituents(&self, py: Python<'_>) -> PyResult<Vec<TableHandle>> {
        let partitioned = &self.partitioned;
        py.detach(|| guarded(&[partitioned.table()], || Ok(partitioned.constituents()?)))
            .map(|tables| {
                let handles = tables.into_iter().map(|table| TableHandle { table });
                handles.collect()
            })
            .map_err(failure)
    }

    /// This partitioned table with `table`, a filter or a sort of its underlying table,
    /// as its underlying table. A sort (`sorted`) was observed to leave the result
    /// without consistent key columns, as the constructor call of `#sort` reads: a
    /// proxy of it applies a filter of the key columns to every constituent.
    ///
    /// Behavior: the result of `PartitionedTableImpl#filter` and `#sort`
    #[pyo3(signature = (table, sorted=false))]
    fn with_table(&self, table: &TableHandle, sorted: bool) -> PyResult<PartitionedHandle> {
        let replaced = self.partitioned.with_table(table.table.clone());
        replaced
            .map(|partitioned| match sorted {
                true => partitioned.with_consistent_key_columns(&[]),
                false => partitioned,
            })
            .map(|partitioned| handle(Arc::new(partitioned)))
            .map_err(|error| failure(error.into()))
    }

    /// The constituent whose key is the row of `keys`, a table of one row with the key
    /// columns, or `None` if no constituent has that key.
    ///
    /// Behavior: `PartitionedTableImpl#constituentFor`, after it has checked the number
    /// of values
    fn constituent_for(&self, py: Python<'_>, keys: &TableHandle) -> PyResult<Option<TableHandle>> {
        let (partitioned, keys) = (&self.partitioned, &keys.table);
        py.detach(|| {
            guarded(&[partitioned.table(), keys], || {
                let rows = partitioned.rows_for_keys(keys)?;
                Ok(partitioned.single_constituent(&rows)?)
            })
        })
        .map(|found| found.map(|table| TableHandle { table }))
        .map_err(failure)
    }

    /// A partitioned table whose constituents are what `function` makes of the
    /// constituents of this one. `function` takes the handle of a table and answers the
    /// handle of a table, or `None`; it is called now for every constituent and for a
    /// table without rows, and in the update cycle for every constituent that appears.
    /// `dependencies` are the refreshing tables it reads besides.
    ///
    /// Behavior: `PartitionedTableImpl#transform`
    fn transform(
        &self,
        py: Python<'_>,
        function: Py<PyAny>,
        dependencies: Vec<PyRef<'_, TableHandle>>,
    ) -> PyResult<PartitionedHandle> {
        let dependencies: Vec<Arc<dyn Table>> = dependencies
            .iter()
            .map(|table| table.table.clone())
            .collect();
        let (partitioned, function) = (&self.partitioned, transformer(function));
        py.detach(|| {
            let mut locked = vec![partitioned.table()];
            locked.extend(&dependencies);
            guarded(&locked, || {
                Ok(partitioned.transform(function, &dependencies)?)
            })
        })
        .map(|partitioned| handle(Arc::new(partitioned)))
        .map_err(failure)
    }

    /// A partitioned table with a constituent for each key that this one and `other`
    /// both have: what `function` makes of the two constituents of the key. As
    /// [`PartitionedHandle::transform`], with a function of two tables.
    ///
    /// Behavior: `PartitionedTableImpl#partitionedTransform`
    fn partitioned_transform(
        &self,
        py: Python<'_>,
        other: &PartitionedHandle,
        function: Py<PyAny>,
        dependencies: Vec<PyRef<'_, TableHandle>>,
    ) -> PyResult<PartitionedHandle> {
        let dependencies: Vec<Arc<dyn Table>> = dependencies
            .iter()
            .map(|table| table.table.clone())
            .collect();
        let (partitioned, other) = (&self.partitioned, &other.partitioned);
        let function = transformer(function);
        py.detach(|| {
            let mut locked = vec![partitioned.table(), other.table()];
            locked.extend(&dependencies);
            guarded(&locked, || {
                Ok(partitioned.partitioned_transform(other, function, &dependencies)?)
            })
        })
        .map(|partitioned| handle(Arc::new(partitioned)))
        .map_err(failure)
    }
}

/// `function` as the engine calls it: with the handles of the tables, under the
/// interpreter lock, which is taken for the call. The thread is one that holds the update
/// graph's lock or runs its cycle, so the order is graph, interpreter
/// (`docs/PYTHON_UDF.md`, "Locks").
///
/// Behavior: `io.deephaven.integrations.python.PythonFunction.PythonUnaryOperator#apply`
/// and `PythonBinaryOperator#apply`, whose answer is cast to a table
fn transformer(function: Py<PyAny>) -> Transformer {
    Arc::new(move |tables: &[Arc<dyn Table>]| {
        Python::attach(|py| {
            let handles = tables
                .iter()
                .map(|table| {
                    let table = table.clone();
                    Py::new(py, TableHandle { table })
                })
                .collect::<PyResult<Vec<_>>>()?;
            let made = function.bind(py).call1(PyTuple::new(py, handles)?)?;
            if made.is_none() {
                return Ok(None);
            }
            let made = made.cast::<TableHandle>().map_err(|_| {
                PyTypeError::new_err("the transform function did not answer a table")
            })?;
            Ok(Some(made.get().table.clone()))
        })
        .map_err(|error: PyErr| TableFailure {
            message: format!("Error in the transform function: {error}"),
        })
    })
}

/// Behavior: `io.deephaven.engine.table.impl.QueryTable#partitionBy`
#[pyfunction]
fn partition_by(
    py: Python<'_>,
    source: &TableHandle,
    by: Vec<String>,
    drop_keys: bool,
) -> PyResult<PartitionedHandle> {
    py.detach(|| partition(&source.table, &by, drop_keys))
        .map(handle)
        .map_err(failure)
}

/// The partitioned table of `aggregated`, the aggregation of `source` by the columns
/// `by` whose partition aggregation is in `constituent_column`.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#partitionedAggBy`, after its
/// `aggBy`
#[pyfunction]
fn partitioned_aggregation(
    source: &TableHandle,
    aggregated: &TableHandle,
    by: Vec<String>,
    constituent_column: &str,
    include_keys: bool,
) -> PyResult<PartitionedHandle> {
    let keys: Vec<&str> = by.iter().map(String::as_str).collect();
    let made = of_aggregation(
        &source.table,
        aggregated.table.clone(),
        &keys,
        constituent_column,
        include_keys,
    );
    made.map(|partitioned| handle(Arc::new(partitioned)))
        .map_err(|error| failure(ExportError::from(error)))
}

/// Adds the class and the functions of this module to the native module `_oxhaven`.
pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<PartitionedHandle>()?;
    native.add_function(wrap_pyfunction!(partition_by, native)?)?;
    native.add_function(wrap_pyfunction!(partitioned_aggregation, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! What a script sees of a partitioned table is in the script scenarios
    //! `partitioned_*`, recorded from Java; these are the parts no scenario reaches.

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    use oxhaven_contracts::graph::UpdateGraph;
    use oxhaven_contracts::table::Table;

    use super::super::tests::Python;
    use crate::partitioned::partitioned_of;

    /// Runs cycles until `done` says so of the session, for at most ten seconds.
    fn cycles_until(python: &Python, done: &str) {
        for _ in 0..2000 {
            python.server.state.graph.run_cycle();
            let changes = python.console.evaluate(&format!("assert {done}"));
            if changes.error.is_none() {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("never: {done}");
    }

    #[tokio::test]
    async fn a_cycle_calls_the_transform_function_for_a_new_constituent() {
        let python = Python::start().await;
        python.run(
            "import threading\n\
             from deephaven import time_table\n\
             source = time_table(1_000_000).update('K = ii')\n\
             pt = source.partition_by('K')\n\
             threads = set()\n\
             def last(table):\n\
             \x20   threads.add(threading.get_ident())\n\
             \x20   return table.tail(1)\n\
             made = pt.transform(last)\n\
             merged = made.merge()\n\
             command = threading.get_ident()\n\
             at_first = merged.size\n\
             threads.clear()\n",
        );
        cycles_until(&python, "merged.size >= at_first + 3");
        // every row is a key of its own, and each was given to the function in a cycle
        python.run(
            "assert threads and command not in threads, threads\n\
             assert merged.size == source.size == pt.table.size == made.table.size\n\
             assert not made.table.is_failed and not merged.is_failed\n",
        );
    }

    #[tokio::test]
    async fn a_function_that_fails_in_a_cycle_fails_the_transform_and_nothing_else() {
        let python = Python::start().await;
        python.run(
            "from deephaven import time_table\n\
             source = time_table(1_000_000).update('K = ii')\n\
             pt = source.partition_by('K')\n\
             allowed = True\n\
             def fragile(table):\n\
             \x20   if not allowed:\n\
             \x20       raise ValueError('not now')\n\
             \x20   return table.tail(1)\n\
             broken = pt.transform(fragile)\n\
             broken_rows = broken.merge()\n\
             fine = pt.transform(lambda table: table.tail(1))\n\
             fine_rows = fine.merge()\n\
             at_first = fine_rows.size\n\
             allowed = False\n",
        );
        cycles_until(&python, "fine_rows.size >= at_first + 2");
        python.run(
            "assert broken.table.is_failed and broken_rows.is_failed\n\
             assert not fine.table.is_failed and not fine_rows.is_failed\n\
             assert not pt.table.is_failed\n\
             allowed = True\n\
             again = pt.transform(fragile).merge()\n\
             at_first = again.size\n",
        );
        // the session and the next cycles go on, and formulas have their scope
        cycles_until(&python, "again.size >= at_first + 2");
        python.run("assert not again.is_failed\nassert source.where('K >= 0').size > 0\n");
    }

    /// The lock rule of `docs/PYTHON_UDF.md` for a transform function: cycles that call
    /// it, commands that transform, and a Python thread that does, all make progress.
    #[tokio::test]
    async fn a_cycle_calls_transform_functions_while_commands_run() {
        let python = Python::start().await;
        python.run(
            "import threading\n\
             from deephaven import time_table\n\
             source = time_table(2_000_000).update('K = ii % 300')\n\
             pt = source.partition_by('K')\n\
             made = pt.transform(lambda table: table.tail(1))\n\
             merged = made.merge()\n\
             running = True\n\
             rounds = 0\n\
             def _work():\n\
             \x20   global rounds\n\
             \x20   while running:\n\
             \x20       pt.transform(lambda table: table.head(1)).merge()\n\
             \x20       pt.partitioned_transform(made, lambda a, b: a.tail(2)).merge()\n\
             \x20       rounds += 1\n\
             _thread = threading.Thread(target=_work)\n\
             _thread.start()\n",
        );
        let stop = Arc::new(AtomicBool::new(false));
        let (done, finished) = mpsc::channel();
        let cycling = std::thread::spawn({
            let graph = python.server.state.graph.clone();
            let (stop, done) = (stop.clone(), done.clone());
            move || {
                let mut cycles = 0;
                while !stop.load(Ordering::Relaxed) {
                    graph.run_cycle();
                    cycles += 1;
                }
                done.send(("cycles", cycles)).unwrap();
            }
        });
        let commands = std::thread::spawn({
            let console = python.console.clone();
            move || {
                for round in 0..30 {
                    let changes = console.evaluate(
                        "heads = pt.transform(lambda table: table.head(1))\n\
                         pairs = heads.partitioned_transform(made, lambda a, b: b)\n\
                         assert pairs.merge().size <= 300\n",
                    );
                    assert_eq!(changes.error, None, "round {round}");
                    std::thread::sleep(Duration::from_millis(2));
                }
                done.send(("commands", 30)).unwrap();
            }
        });

        // a deadlock is a party that never reports
        let wait = Duration::from_secs(120);
        let first = finished.recv_timeout(wait).expect("the commands finish");
        assert_eq!(first, ("commands", 30));
        stop.store(true, Ordering::Relaxed);
        let (who, cycles) = finished.recv_timeout(wait).expect("the cycles finish");
        assert_eq!(who, "cycles");
        assert!(cycles >= 5, "{cycles} cycles");
        commands.join().unwrap();
        cycling.join().unwrap();
        python.run(
            "running = False\n_thread.join(60)\nassert not _thread.is_alive()\n\
             assert rounds > 0\nassert not merged.is_failed and merged.size > 0\n",
        );
        let merged: Arc<dyn Table> = python.table("merged");
        assert!(!merged.is_failed());
    }

    #[tokio::test]
    async fn a_partitioned_table_variable_is_listed_and_opened_as_one() {
        let python = Python::start().await;
        let changes = python.run(
            "from deephaven import empty_table\n\
             pt = empty_table(6).update('K = i % 2').partition_by('K')\n",
        );
        let [variable] = changes.created.as_slice() else {
            panic!("one variable, not {:?}", changes.created);
        };
        assert_eq!(
            (variable.name.as_str(), variable.type_name.as_str()),
            ("pt", "PartitionedTable")
        );
        let value = python.server.state.scope.get("pt").unwrap();
        let partitioned = partitioned_of(&value).expect("a partitioned table");
        assert_eq!(partitioned.key_columns(), ["K"]);
        assert_eq!(partitioned.table().row_set().size(), 2);
    }

    #[tokio::test]
    async fn formulas_work_again_after_a_transform_whose_function_failed() {
        let python = Python::start().await;
        python.run(
            "from deephaven import empty_table, DHError\n\
             pt = empty_table(6).update('K = i % 2').partition_by('K')\n\
             def broken(table):\n\
             \x20   raise ValueError('no')\n\
             for function in (broken, lambda table: table.update('W = K + 1')):\n\
             \x20   try:\n\
             \x20       pt.transform(function)\n\
             \x20       raise AssertionError('the transform must fail')\n\
             \x20   except DHError:\n\
             \x20       pass\n\
             assert pt.table.where('K > 0').update('W = K + 1').size == 1\n\
             inner = pt.transform(lambda table: pt.transform(lambda t: t.head(1)).merge())\n\
             assert inner.merge().size == 4\n\
             assert pt.merge().where('K > 0').size == 3\n",
        );
    }
}
