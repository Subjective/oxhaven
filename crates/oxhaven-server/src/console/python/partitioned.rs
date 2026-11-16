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
    /// as its underlying table.
    ///
    /// Behavior: the result of `PartitionedTableImpl#filter` and `#sort`
    fn with_table(&self, table: &TableHandle) -> PyResult<PartitionedHandle> {
        let replaced = self.partitioned.with_table(table.table.clone());
        replaced
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
        let dependencies: Vec<Arc<dyn Table>> =
            dependencies.iter().map(|table| table.table.clone()).collect();
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
        let dependencies: Vec<Arc<dyn Table>> =
            dependencies.iter().map(|table| table.table.clone()).collect();
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

    use super::super::tests::Python;
    use crate::partitioned::partitioned_of;

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
