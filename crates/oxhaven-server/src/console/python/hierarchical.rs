//! Hierarchical tables for the `deephaven` package: the handle its `RollupTable` wraps,
//! and the function that makes one.
//!
//! A rollup is made as the hierarchical table service makes it
//! (`crate::hierarchical::make_rollup`): under the update graph's shared lock when the
//! source is refreshing, and with what is not ported answered by the name of the Java
//! method.

use oxhaven_proto::grpc::batch_table_request::operation::Op;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use super::ops::failure;
use super::{parse_operation, runtime, TableHandle};
use crate::hierarchical::{make_rollup, Hierarchical};
use crate::session::ExportError;
use crate::table::ops::{adapt_aggregations, validate_aggregation};

/// A reference to a hierarchical table of the engine, as Python holds it. The Python
/// `RollupTable` class wraps one.
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct HierarchicalHandle {
    pub(super) table: Hierarchical,
}

#[pymethods]
impl HierarchicalHandle {
    /// Behavior: `io.deephaven.engine.table.hierarchical.HierarchicalTable#getSource`
    fn source(&self) -> TableHandle {
        TableHandle {
            table: self.table.source(),
        }
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
             refused(lambda: r.with_filters('K > 0'), 'RollupTableImpl#withFilter')\n\
             refused(lambda: r.with_update_view('W = V'), 'RollupTableImpl#withUpdateView')\n\
             refused(lambda: r.node_operation_recorder(None), 'RollupTableImpl#makeNodeOperationsRecorder')\n\
             refused(lambda: r.with_node_operations([]), 'RollupTableImpl#withNodeOperations')\n\
             refused(lambda: source.rollup(aggs=[agg.sum_('V')], by='K', include_constituents=True), 'RollupTableImpl#makeRollup')\n\
             refused(lambda: source.rollup(aggs=[agg.median('V')], by='K'), 'AggMedian is not supported for rollup()')\n\
             refused(lambda: source.tree('K', 'V'), 'QueryTable#tree')\n",
        );
    }
}
