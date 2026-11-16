//! Hierarchical tables for the `deephaven` package: the handle its `RollupTable` wraps,
//! the function that makes one, and the recorder of the operations on its nodes.
//!
//! A rollup is made as the hierarchical table service makes it
//! (`crate::hierarchical::make_rollup`): under the update graph's shared lock when the
//! source is refreshing, and with what is not ported answered by the name of the Java
//! method.

use oxhaven_proto::grpc::batch_table_request::operation::Op;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use std::sync::Arc;

use oxhaven_engine::ops::filter::CheckedFilters;
use oxhaven_engine::ops::hierarchical::{merged_sorts, Hierarchy, RollupTable};
use oxhaven_engine::ops::sort::SortColumn;

use super::ops::failure;
use super::{parse_operation, runtime, TableHandle};
use crate::hierarchical::{make_rollup, Hierarchical, HierarchicalTable};
use crate::partitioned::guarded;
use crate::session::ExportError;
use crate::table::ops::{adapt_aggregations, validate_aggregation};

/// A reference to a hierarchical table of the engine, as Python holds it. The Python
/// `RollupTable` class wraps one.
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct HierarchicalHandle {
    pub(super) table: Hierarchical,
}

impl HierarchicalHandle {
    fn rollup(&self) -> &Arc<RollupTable> {
        match self.table.as_ref() {
            HierarchicalTable::Rollup(rollup) => rollup,
        }
    }
}

/// What Java throws as an `IllegalArgumentException`.
fn illegal(message: &str) -> PyErr {
    PyRuntimeError::new_err(format!("java.lang.IllegalArgumentException: {message}"))
}

#[pymethods]
impl HierarchicalHandle {
    /// Behavior: `io.deephaven.engine.table.hierarchical.HierarchicalTable#getSource`
    fn source(&self) -> TableHandle {
        TableHandle {
            table: self.table.source(),
        }
    }

    /// A recorder of operations on the nodes of the type named `node_type`, `Aggregated`
    /// or `Constituent`, with nothing recorded.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#makeNodeOperationsRecorder`
    fn recorder(&self, node_type: &str) -> PyResult<RecorderHandle> {
        match node_type {
            "Aggregated" => Ok(RecorderHandle {
                table: self.table.clone(),
                sorts: Vec::new(),
            }),
            // a rollup here has no constituents yet (task 7.8.3)
            "Constituent" => Err(illegal("Rollup does not have constituent nodes")),
            other => Err(illegal(&format!("Unrecognized node type: {other}"))),
        }
    }

    /// This rollup with what `recorders` recorded applied to its nodes, each after the
    /// ones before it; this rollup itself when they recorded nothing. The sorts were
    /// checked when they were recorded, for the rollup the recorder was made of, and as
    /// in Java are not checked again for this one.
    ///
    /// Behavior: `RollupTableImpl#withNodeOperations`
    fn with_node_operations(
        slf: Bound<'_, Self>,
        recorders: Vec<PyRef<'_, RecorderHandle>>,
    ) -> PyResult<Py<Self>> {
        if recorders.iter().all(|recorder| recorder.sorts.is_empty()) {
            return Ok(slf.unbind());
        }
        let mut rollup = slf.get().rollup().clone();
        for recorder in &recorders {
            if !recorder.sorts.is_empty() {
                rollup = Arc::new(rollup.with_sorts(&recorder.sorts));
            }
        }
        let table = Arc::new(HierarchicalTable::Rollup(rollup));
        Py::new(slf.py(), HierarchicalHandle { table })
    }

    /// This rollup of the rows of its source that every filter of `filters` accepts;
    /// this rollup itself without filters.
    ///
    /// Behavior: `RollupTableImpl#withFilter`
    fn with_filters(slf: Bound<'_, Self>, filters: Vec<String>) -> PyResult<Py<Self>> {
        if filters.is_empty() {
            return Ok(slf.unbind());
        }
        let py = slf.py();
        let rollup = slf.get().rollup().clone();
        let runtime = runtime()?;
        let table = py
            .detach(|| {
                let compiler = runtime.context.compiler.as_ref();
                let source = rollup.source().clone();
                guarded(&[&source], || {
                    let filters = CheckedFilters::parse(&filters, source.definition(), compiler)?;
                    let filtered = rollup.with_filters(&filters, compiler)?;
                    Ok(Arc::new(HierarchicalTable::Rollup(Arc::new(filtered))))
                })
            })
            .map_err(failure)?;
        Py::new(py, HierarchicalHandle { table })
    }
}

/// What a script has recorded to apply to the nodes of a rollup: sorts, the one that
/// decides first in front. The Python `RollupNodeOperationsRecorder` wraps one.
///
/// Behavior: `io.deephaven.engine.table.impl.hierarchical.RollupNodeOperationsRecorder`
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct RecorderHandle {
    /// The rollup the recorder was made of, whose columns its sorts are checked against.
    table: Hierarchical,
    sorts: Vec<SortColumn>,
}

#[pymethods]
impl RecorderHandle {
    /// This recorder with a sort by `columns` in front of what it has. A name that
    /// starts with `__ABS__` sorts by the absolute value of the column named after it.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.BaseNodeOperationsRecorder#sort`
    /// and `#sortDescending`, and
    /// `io.deephaven.engine.table.impl.AbsoluteSortColumnConventions#isAbsoluteColumnName`
    fn sort(&self, py: Python<'_>, columns: Vec<String>, descending: bool) -> PyResult<Self> {
        let sorts: Vec<SortColumn> = columns
            .iter()
            .map(|name| {
                let (name, absolute) = match name.strip_prefix("__ABS__") {
                    Some(base) => (base, true),
                    None => (name.as_str(), false),
                };
                let sort = if descending {
                    SortColumn::descending(name)
                } else {
                    SortColumn::ascending(name)
                };
                if absolute {
                    sort.absolute()
                } else {
                    sort
                }
            })
            .collect();
        let table = &self.table;
        py.detach(|| {
            guarded(&[], || match table.as_ref() {
                HierarchicalTable::Rollup(rollup) => Ok(rollup.check_sorts(&sorts)?),
            })
        })
        .map_err(failure)?;
        Ok(RecorderHandle {
            table: self.table.clone(),
            sorts: merged_sorts(&sorts, &self.sorts),
        })
    }
}

/// The rollup of `source` with the aggregations and group-by columns of `json`, an
/// `AggregateRequest` in protobuf JSON form as `table_op` takes one.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#rollup`
#[pyfunction]
fn rollup(
    py: Python<'_>,
    json: &str,
    source: &TableHandle,
    include_constituents: bool,
) -> PyResult<HierarchicalHandle> {
    let Op::Aggregate(request) = parse_operation(json).map_err(PyValueError::new_err)? else {
        return Err(PyValueError::new_err("a rollup takes an aggregate request"));
    };
    let runtime = runtime()?;
    let source = &source.table;
    py.detach(|| {
        for aggregation in &request.aggregations {
            validate_aggregation(aggregation).map_err(ExportError::from)?;
        }
        let aggregations = adapt_aggregations(&request.aggregations)?;
        make_rollup(
            &runtime.context,
            source,
            &aggregations,
            include_constituents,
            &request.group_by_columns,
        )
    })
    .map(|table| HierarchicalHandle { table })
    .map_err(failure)
}

/// Adds the class and the function of this module to the native module `_oxhaven`.
pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<HierarchicalHandle>()?;
    native.add_class::<RecorderHandle>()?;
    native.add_function(wrap_pyfunction!(rollup, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! What a client sees of a rollup a script makes is in the script scenarios
    //! `rollup_*`, recorded from Java; these are the parts no scenario reaches.

    use super::super::tests::Python;
    use crate::hierarchical::hierarchical_of;

    #[tokio::test]
    async fn a_rollup_variable_is_listed_and_opened_as_a_hierarchical_table() {
        let python = Python::start().await;
        let changes = python.run(
            "from deephaven import empty_table, agg\n\
             source = empty_table(6).update(['K = i % 2', 'V = i'])\n\
             r = source.rollup(aggs=[agg.sum_('V')], by='K')\n\
             assert r.by == ('K',) and not r.include_constituents and len(r.aggs) == 1\n\
             again = r\n",
        );
        let types: Vec<(&str, &str)> = changes
            .created
            .iter()
            .map(|variable| (variable.name.as_str(), variable.type_name.as_str()))
            .collect();
        assert!(types.contains(&("r", "HierarchicalTable")), "{types:?}");
        let value = python.server.state.scope.get("r").unwrap();
        let table = hierarchical_of(&value).expect("a hierarchical table");
        assert_eq!(table.source().row_set().size(), 6);
    }

    #[tokio::test]
    async fn what_a_rollup_does_not_have_yet_is_refused_by_name() {
        let python = Python::start().await;
        python.run(
            "from deephaven import empty_table, agg, DHError\n\
             source = empty_table(6).update(['K = i % 2', 'V = i'])\n\
             r = source.rollup(aggs=[agg.sum_('V')], by='K')\n\
             def refused(call, java):\n\
             \x20   try:\n\
             \x20       call()\n\
             \x20   except (DHError, NotImplementedError) as e:\n\
             \x20       assert java in str(e), str(e)\n\
             \x20   else:\n\
             \x20       raise AssertionError('must be refused: ' + java)\n\
             from deephaven.table import NodeType\n\
             recorder = r.node_operation_recorder(NodeType.AGGREGATED)\n\
             refused(lambda: r.with_update_view('W = V'), 'RollupTableImpl#withUpdateView')\n\
             refused(lambda: recorder.update_view('W = V'), 'RollupNodeOperationsRecorder#updateView')\n\
             refused(lambda: recorder.format_column('V = RED'), 'RollupNodeOperationsRecorder#formatColumns')\n\
             refused(lambda: recorder.format_row_where('V > 1', 'RED'), 'RollupNodeOperationsRecorder#formatRowWhere')\n\
             refused(lambda: recorder.format_column_where('V', 'V > 1', 'RED'), 'RollupNodeOperationsRecorder#formatColumnWhere')\n\
             # Java takes a filter by the position of a row for one of group-by columns\n\
             refused(lambda: r.with_filters('i > 2'), 'RollupTableImpl#withFilter')\n\
             assert r.with_filters('K > 0').by == r.by\n\
             assert r.with_node_operations([recorder.sort_descending('V')]).by == r.by\n\
             refused(lambda: source.rollup(aggs=[agg.sum_('V')], by='K', include_constituents=True), 'RollupTableImpl#makeRollup')\n\
             refused(lambda: source.rollup(aggs=[agg.median('V')], by='K'), 'AggMedian is not supported for rollup()')\n\
             refused(lambda: source.tree('K', 'V'), 'QueryTable#tree')\n",
        );
    }
}
