//! Hierarchical tables for the `deephaven` package: the handle its `RollupTable` and
//! `TreeTable` wrap, the functions that make one, and the recorder of the operations on
//! its nodes.
//!
//! A rollup is made as the hierarchical table service makes it
//! (`crate::hierarchical::make_rollup`): under the update graph's shared lock when the
//! source is refreshing, and with what is not ported answered by the name of the Java
//! method.

use oxhaven_proto::grpc::batch_table_request::operation::Op;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use std::sync::Arc;

use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_engine::ops::filter::CheckedFilters;
use oxhaven_engine::ops::hierarchical::{
    HierarchicalError, NodeOperations, RollupTable, TreeTable,
};
use oxhaven_engine::ops::select::format::conditional_format;
use oxhaven_engine::ops::sort::SortColumn;

use super::ops::failure;
use super::{parse_operation, runtime, TableHandle};
use crate::hierarchical::{make_rollup, make_tree, Hierarchical, HierarchicalTable};
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
    fn rollup(&self) -> PyResult<&Arc<RollupTable>> {
        match self.table.as_ref() {
            HierarchicalTable::Rollup(rollup) => Ok(rollup),
            HierarchicalTable::Tree(_) => Err(PyValueError::new_err("not a rollup")),
        }
    }

    fn tree(&self) -> PyResult<&Arc<TreeTable>> {
        match self.table.as_ref() {
            HierarchicalTable::Tree(tree) => Ok(tree),
            HierarchicalTable::Rollup(_) => Err(PyValueError::new_err("not a tree")),
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

    /// The identifier and the parent column of a tree; `None` for a rollup.
    fn tree_columns(&self) -> Option<(String, String)> {
        match self.table.as_ref() {
            HierarchicalTable::Tree(tree) => Some((
                tree.identifier_column().to_string(),
                tree.parent_identifier_column().to_string(),
            )),
            HierarchicalTable::Rollup(_) => None,
        }
    }

    /// A recorder of operations on the nodes of a tree, with nothing recorded.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.TreeTableImpl#makeNodeOperationsRecorder`
    fn tree_recorder(&self) -> PyResult<RecorderHandle> {
        let tree = self.tree()?;
        Ok(RecorderHandle {
            table: self.table.clone(),
            constituents: false,
            operations: tree.node_operations(),
        })
    }

    /// A recorder of operations on the nodes of the type named `node_type`, `Aggregated`
    /// or `Constituent`, with nothing recorded.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#makeNodeOperationsRecorder`
    fn recorder(&self, node_type: &str) -> PyResult<RecorderHandle> {
        let rollup = self.rollup()?;
        let constituents = match node_type {
            "Aggregated" => false,
            "Constituent" => {
                rollup
                    .assert_includes_constituents()
                    .map_err(|error| illegal(&error.to_string()))?;
                true
            }
            other => return Err(illegal(&format!("Unrecognized node type: {other}"))),
        };
        let operations = rollup
            .node_operations(constituents)
            .map_err(|error| illegal(&error.to_string()))?;
        Ok(RecorderHandle {
            table: self.table.clone(),
            constituents,
            operations,
        })
    }

    /// This table with what `recorders` recorded applied to its nodes, each after the
    /// ones before it; this table itself when they recorded nothing. The sorts were
    /// checked when they were recorded, for the table the recorder was made of, and as
    /// in Java are not checked again for this one. A table whose nodes (of a type) have
    /// operations takes no recorder that adds a column to them, as in Java.
    ///
    /// Behavior: `RollupTableImpl#withNodeOperations`, `TreeTableImpl#withNodeOperations`
    fn with_node_operations(
        slf: Bound<'_, Self>,
        recorders: Vec<PyRef<'_, RecorderHandle>>,
    ) -> PyResult<Py<Self>> {
        if recorders.iter().all(|recorder| recorder.is_empty()) {
            return Ok(slf.unbind());
        }
        let recorded = recorders.iter().filter(|recorder| !recorder.is_empty());
        let table = Arc::new(match slf.get().table.as_ref() {
            HierarchicalTable::Rollup(rollup) => {
                let mut result = rollup.clone();
                for recorder in recorded {
                    if recorder.is_tree() {
                        return Err(PyValueError::new_err("not a recorder of a rollup"));
                    }
                    // a recorder made of a rollup that has constituents, applied to one
                    // that has none, is refused here too
                    result = Arc::new(
                        result
                            .with_node_operations(recorder.constituents, &recorder.operations)
                            .map_err(|error| illegal(&error.to_string()))?,
                    );
                }
                HierarchicalTable::Rollup(result)
            }
            HierarchicalTable::Tree(tree) => {
                let mut result = tree.clone();
                for recorder in recorded {
                    if !recorder.is_tree() {
                        return Err(PyValueError::new_err("not a recorder of a tree"));
                    }
                    result = Arc::new(
                        result
                            .with_node_operations(&recorder.operations)
                            .map_err(|error| illegal(&error.to_string()))?,
                    );
                }
                HierarchicalTable::Tree(result)
            }
        });
        Py::new(slf.py(), HierarchicalHandle { table })
    }

    /// This table of the rows of its source that every filter of `filters` accepts, and
    /// for a tree of their ancestors; this table itself without filters.
    ///
    /// Behavior: `RollupTableImpl#withFilter`, `TreeTableImpl#withFilter`
    fn with_filters(slf: Bound<'_, Self>, filters: Vec<String>) -> PyResult<Py<Self>> {
        if filters.is_empty() {
            return Ok(slf.unbind());
        }
        let py = slf.py();
        let table = slf.get().table.clone();
        let runtime = runtime()?;
        let table = py
            .detach(|| {
                let compiler = runtime.context.compiler.as_ref();
                let source = table.source();
                guarded(&[&source], || {
                    let filters = CheckedFilters::parse(&filters, source.definition(), compiler)?;
                    Ok(Arc::new(match table.as_ref() {
                        HierarchicalTable::Rollup(rollup) => HierarchicalTable::Rollup(Arc::new(
                            rollup.with_filters(&filters, compiler)?,
                        )),
                        HierarchicalTable::Tree(tree) => HierarchicalTable::Tree(Arc::new(
                            tree.with_filters(&filters, compiler)?,
                        )),
                    }))
                })
            })
            .map_err(failure)?;
        Py::new(py, HierarchicalHandle { table })
    }
}

/// What a script has recorded to apply to the nodes of a rollup or a tree: sorts, the
/// one that decides first in front, formats, for a rollup update views, and for a tree
/// filters. The Python `RollupNodeOperationsRecorder` and `TreeNodeOperationsRecorder`
/// wrap one.
///
/// Behavior: `io.deephaven.engine.table.impl.hierarchical.RollupNodeOperationsRecorder`,
/// `TreeNodeOperationsRecorder`
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct RecorderHandle {
    /// The table the recorder was made of, whose columns its sorts are checked against.
    table: Hierarchical,
    /// Whether it records for the constituent nodes of a rollup: the rows of the source.
    constituents: bool,
    /// What was recorded.
    operations: NodeOperations,
}

impl RecorderHandle {
    fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    fn is_tree(&self) -> bool {
        matches!(self.table.as_ref(), HierarchicalTable::Tree(_))
    }

    /// This recorder with what `record` adds to its operations. The formulas are
    /// compiled against the columns of the nodes and what the recorder added.
    fn recorded(
        &self,
        py: Python<'_>,
        record: impl Fn(&NodeOperations, &dyn FormulaCompiler) -> Result<NodeOperations, HierarchicalError>
            + Send
            + Sync,
    ) -> PyResult<Self> {
        let runtime = runtime()?;
        let operations = py
            .detach(|| {
                guarded(&[], || {
                    Ok(record(&self.operations, runtime.context.compiler.as_ref())?)
                })
            })
            .map_err(failure)?;
        Ok(RecorderHandle {
            table: self.table.clone(),
            constituents: self.constituents,
            operations,
        })
    }
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
                HierarchicalTable::Rollup(rollup) => {
                    Ok(rollup.check_recorded_sorts(self.constituents, &self.operations, &sorts)?)
                }
                // a tree's recorder adds no column a sort may name
                HierarchicalTable::Tree(tree) => Ok(tree.check_sorts(&sorts)?),
            })
        })
        .map_err(failure)?;
        Ok(RecorderHandle {
            table: self.table.clone(),
            constituents: self.constituents,
            operations: self.operations.sort(&sorts),
        })
    }

    /// This recorder of a rollup with the columns of `formulas` added to the nodes.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.RollupNodeOperationsRecorder#updateView`
    fn update_view(&self, py: Python<'_>, formulas: Vec<String>) -> PyResult<Self> {
        if self.is_tree() {
            // Java's tree recorder has no update view; the Python class offers none
            return Err(PyValueError::new_err("not a recorder of a rollup"));
        }
        self.recorded(py, |operations, compiler| {
            operations.update_view(&formulas, compiler)
        })
    }

    /// This recorder of a tree with the filters `filters` of the rows of its nodes.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.TreeNodeOperationsRecorder#where`
    fn where_filters(&self, py: Python<'_>, filters: Vec<String>) -> PyResult<Self> {
        if !self.is_tree() {
            // Java's rollup recorder has no filters; the Python class offers none
            return Err(PyValueError::new_err("not a recorder of a tree"));
        }
        self.recorded(py, |operations, compiler| {
            operations.where_filters(&filters, compiler)
        })
    }

    /// This recorder of a rollup with the formats `formats` of columns of the nodes.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.hierarchical.BaseNodeOperationsRecorder#formatColumns`
    fn format_columns(&self, py: Python<'_>, formats: Vec<String>) -> PyResult<Self> {
        self.recorded(py, |operations, compiler| {
            operations.format_columns(&formats, compiler)
        })
    }

    /// This recorder of a rollup with the format `format` of `column`, `*` for the row,
    /// in the rows where `condition` holds.
    ///
    /// Behavior: `BaseNodeOperationsRecorder#formatColumnWhere` and `#formatRowWhere`
    fn format_where(
        &self,
        py: Python<'_>,
        column: &str,
        condition: &str,
        format: &str,
    ) -> PyResult<Self> {
        let formats = [conditional_format(column, condition, format)];
        self.recorded(py, |operations, compiler| {
            operations.format_columns(&formats, compiler)
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

/// `source` as a tree by its columns `id_column` and `parent_column`, with orphans made
/// children of the root if `promote_orphans`.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#tree`, after
/// `io.deephaven.engine.table.hierarchical.TreeTable#promoteOrphans`
#[pyfunction]
fn tree(
    py: Python<'_>,
    source: &TableHandle,
    id_column: &str,
    parent_column: &str,
    promote_orphans: bool,
) -> PyResult<HierarchicalHandle> {
    let runtime = runtime()?;
    let source = &source.table;
    py.detach(|| {
        make_tree(
            &runtime.context,
            source,
            id_column,
            parent_column,
            promote_orphans,
        )
    })
    .map(|table| HierarchicalHandle { table })
    .map_err(failure)
}

/// Adds the classes and the functions of this module to the native module `_oxhaven`.
pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<HierarchicalHandle>()?;
    native.add_class::<RecorderHandle>()?;
    native.add_function(wrap_pyfunction!(rollup, native)?)?;
    native.add_function(wrap_pyfunction!(tree, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    //! What a client sees of a rollup or a tree a script makes is in the script scenarios
    //! `rollup_*` and `tree_*`, recorded from Java; these are the parts no scenario
    //! reaches.

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
             \x20   except (DHError, NotImplementedError, RuntimeError) as e:\n\
             \x20       assert java in str(e), str(e)\n\
             \x20   else:\n\
             \x20       raise AssertionError('must be refused: ' + java)\n\
             from deephaven.table import NodeType\n\
             recorder = r.node_operation_recorder(NodeType.AGGREGATED)\n\
             viewed = r.with_update_view('W = V')\n\
             assert viewed.by == r.by\n\
             # Java's rollup takes one set of operations that add columns per node type\n\
             refused(lambda: viewed.with_update_view('X = V'), 'Incompatible operation recorders')\n\
             refused(lambda: recorder.update_view('V = 1'), 'Column V already exists')\n\
             refused(lambda: recorder.update_view('W = i'), 'updateView does not support virtual row variables')\n\
             refused(lambda: recorder.format_column('Nope = RED'), 'Unknown columns: [Nope]')\n\
             refused(lambda: recorder.format_column('V RED'), 'Illegal format specification: V RED')\n\
             refused(lambda: recorder.sort('V__TABLE_STYLE_FORMAT'), 'Unknown column names [V__TABLE_STYLE_FORMAT]')\n\
             formats = recorder.update_view('W = V * 2').format_column('W = RED')\n\
             formats = formats.format_row_where('V > 1', 'RED').format_column_where('V', 'V > 1', 'BLUE')\n\
             assert r.with_node_operations([formats.sort_descending('W')]).by == r.by\n\
             refused(lambda: viewed.with_node_operations([formats]), 'Incompatible operation recorders')\n\
             # Java takes a filter by the position of a row for one of group-by columns\n\
             refused(lambda: r.with_filters('i > 2'), 'RollupTableImpl#withFilter')\n\
             assert r.with_filters('K > 0').by == r.by\n\
             assert r.with_node_operations([recorder.sort_descending('V')]).by == r.by\n\
             refused(lambda: r.node_operation_recorder(NodeType.CONSTITUENT), 'Rollup does not have constituent nodes')\n\
             c = source.rollup(aggs=[agg.sum_('V')], by='K', include_constituents=True)\n\
             by_v = c.node_operation_recorder(NodeType.CONSTITUENT).sort_descending('V')\n\
             assert c.with_node_operations([by_v]).include_constituents\n\
             refused(lambda: r.with_node_operations([by_v]), 'Rollup does not have constituent nodes')\n\
             refused(lambda: source.rollup(aggs=[agg.median('V')], by='K'), 'AggMedian is not supported for rollup()')\n\
             # Java applies a reaggregating formula to the level below\n\
             refused(lambda: source.rollup(aggs=[agg.formula('S = sum(V)', reaggregating=True)], by='K'), 'RollupReaggregatedConverter#visit(Formula)')\n\
             assert source.rollup(aggs=[agg.formula('S = sum(V) + __FORMULA_DEPTH__'), agg.count_where('C', 'V > 2')], by='K').by == ('K',)\n\
             assert source.rollup(aggs=[agg.group('G = V')], by='K').by == ('K',)\n",
        );
    }

    #[tokio::test]
    async fn a_tree_variable_is_listed_and_opened_as_a_hierarchical_table() {
        let python = Python::start().await;
        let changes = python.run(
            "from deephaven import empty_table\n\
             source = empty_table(6).update(['Id = i', 'Parent = i == 0 ? NULL_INT : (int)(i / 3)'])\n\
             t = source.tree('Id', 'Parent')\n\
             assert t.id_col == 'Id' and t.parent_col == 'Parent'\n\
             promoted = source.tree(id_col='Id', parent_col='Parent', promote_orphans=True)\n\
             sorted_ = t.with_node_operations(t.node_operation_recorder().sort_descending('Id'))\n\
             assert sorted_.id_col == 'Id' and sorted_.parent_col == 'Parent'\n\
             filtered = t.with_filters(['Id > 3'])\n\
             assert filtered.id_col == 'Id' and filtered.parent_col == 'Parent'\n",
        );
        let types: Vec<(&str, &str)> = changes
            .created
            .iter()
            .map(|variable| (variable.name.as_str(), variable.type_name.as_str()))
            .collect();
        for name in ["t", "promoted", "sorted_", "filtered"] {
            assert!(types.contains(&(name, "HierarchicalTable")), "{types:?}");
        }
        let value = python.server.state.scope.get("t").unwrap();
        let table = hierarchical_of(&value).expect("a hierarchical table");
        assert_eq!(table.source().row_set().size(), 6);
        // rows 4 and 5, their parent 1 and its parent 0
        let value = python.server.state.scope.get("filtered").unwrap();
        let table = hierarchical_of(&value).expect("a hierarchical table");
        assert_eq!(table.source().row_set().size(), 4);
    }

    #[tokio::test]
    async fn the_recorder_of_a_tree_and_what_it_refuses() {
        let python = Python::start().await;
        python.run(
            "from deephaven import empty_table, agg, DHError\n\
             source = empty_table(6).update(['Id = i', 'Parent = i == 0 ? NULL_INT : (int)(i / 3)', 'Name = `n` + i'])\n\
             t = source.tree('Id', 'Parent')\n\
             def refused(call, java):\n\
             \x20   try:\n\
             \x20       call()\n\
             \x20   except (DHError, NotImplementedError, RuntimeError) as e:\n\
             \x20       assert java in str(e), str(e)\n\
             \x20   else:\n\
             \x20       raise AssertionError('must be refused: ' + java)\n\
             recorder = t.node_operation_recorder()\n\
             refused(lambda: recorder.where('Id_[i - 1] > 1'), 'TreeNodeOperationsRecorder#where')\n\
             refused(lambda: recorder.where(object()), 'TreeNodeOperationsRecorder#where(Filter)')\n\
             formatted = t.with_node_operations(recorder.format_column('Id = RED').format_row_where('Id > 1', 'BLUE'))\n\
             formatted = formatted.with_node_operations(formatted.node_operation_recorder().format_column_where('Id', 'Id > 1', 'BLUE').where('Id > 1'))\n\
             assert formatted.id_col == 'Id'\n\
             # a format of another column changes the columns of the nodes\n\
             refused(lambda: formatted.with_node_operations(formatted.node_operation_recorder().format_column('Name = RED')), 'Incompatible operation recorders')\n\
             # what Java refuses, in Java's words\n\
             refused(lambda: t.with_node_operations(recorder.sort('Id')).with_node_operations(recorder.format_column('Id = RED')), 'Incompatible operation recorders')\n\
             refused(lambda: formatted.with_node_operations(recorder.where('Id > 1')), 'Incompatible operation recorders')\n\
             refused(lambda: formatted.node_operation_recorder().sort('Id__TABLE_STYLE_FORMAT'), 'Unknown column names [Id__TABLE_STYLE_FORMAT]')\n\
             refused(lambda: recorder.format_column('Nope = RED'), 'Unknown columns: [Nope]')\n\
             refused(lambda: source.tree('Nope', 'Parent'), 'tree identifier column: Unknown column names [Nope]')\n\
             refused(lambda: source.tree('Id', 'Nope'), 'tree parent column: Unknown column names [Nope]')\n\
             refused(lambda: source.tree('Id', 'Name'), 'tree parent and identifier columns must have the same data type')\n\
             refused(lambda: t.with_node_operations(recorder.sort('Nope')), 'Nope')\n\
             refused(lambda: t.with_filters('Nope > 1'), 'Nope')\n",
        );
    }
}
