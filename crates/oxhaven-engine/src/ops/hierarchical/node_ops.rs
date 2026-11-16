//! The operations recorded for the nodes of a hierarchical table that add columns to
//! them: update views and formats.
//!
//! [`NodeOperations`] is the recorder: the columns of the nodes it was made for, with the
//! columns its operations add, and the formulas of those. [`node_table`] is a node's table
//! with the recorded columns, each computed when it is read.

use super::HierarchicalError;
use crate::ops::filter::RowFilters;
use crate::ops::select::format::{format_assignments, is_formatting_column};
use crate::ops::sort::SortColumn;
use arrow::array::ArrayRef;
use oxhaven_contracts::expr::{Formula, FormulaCompiler};
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::UpdateListener;
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use std::sync::Arc;

/// A column an operation adds to the nodes of one type.
#[derive(Clone)]
pub struct NodeColumn {
    pub name: String,
    pub formula: Arc<dyn Formula>,
}

/// What was recorded for the nodes of one type of a hierarchical table.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.RollupNodeOperationsRecorder and
/// BaseNodeOperationsRecorder, whose result definition is the definition the recorder
/// was made with and the columns its formats and update views add, each where it was
/// first added
#[derive(Clone)]
pub struct NodeOperations {
    definition: Vec<ColumnDefinition>,
    formats: Vec<NodeColumn>,
    update_views: Vec<NodeColumn>,
    sorts: Vec<SortColumn>,
    /// The filters of the rows of a node of a tree, one set per `where`, in order.
    filters: Vec<Arc<RowFilters>>,
}

fn illegal(message: impl Into<String>) -> HierarchicalError {
    HierarchicalError::IllegalArgument(message.into())
}

impl NodeOperations {
    /// A recorder for nodes with the columns `definition`, with nothing recorded.
    pub fn new(definition: Vec<ColumnDefinition>) -> NodeOperations {
        NodeOperations {
            definition,
            formats: Vec::new(),
            update_views: Vec::new(),
            sorts: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Behavior: RollupNodeOperationsRecorder#isEmpty, TreeNodeOperationsRecorder#isEmpty
    pub fn is_empty(&self) -> bool {
        self.formats.is_empty()
            && self.update_views.is_empty()
            && self.sorts.is_empty()
            && self.filters.is_empty()
    }

    /// The columns of the nodes with what was recorded.
    ///
    /// Behavior: RollupNodeOperationsRecorder#getResultDefinition
    pub fn definition(&self) -> &[ColumnDefinition] {
        &self.definition
    }

    pub fn formats(&self) -> &[NodeColumn] {
        &self.formats
    }

    pub fn update_views(&self) -> &[NodeColumn] {
        &self.update_views
    }

    pub fn sorts(&self) -> &[SortColumn] {
        &self.sorts
    }

    /// Whether the operations add a column to the nodes.
    pub fn adds_columns(&self) -> bool {
        !self.formats.is_empty() || !self.update_views.is_empty()
    }

    /// The names a sort or a format may name: the columns that are no formatting columns.
    pub fn visible_names(&self) -> Vec<&str> {
        self.definition
            .iter()
            .map(|column| column.name.as_str())
            .filter(|name| !is_formatting_column(name))
            .collect()
    }

    fn table_definition(&self) -> Result<TableDefinition, HierarchicalError> {
        TableDefinition::new(self.definition.clone()).map_err(|error| illegal(format!("{error:?}")))
    }

    /// Compiles `assignments` one after another against the columns so far, and gives
    /// each column to `record`, which says whether the column is kept.
    fn compile(
        &mut self,
        assignments: &[String],
        compiler: &dyn FormulaCompiler,
        mut record: impl FnMut(&mut NodeOperations, NodeColumn) -> bool,
    ) -> Result<(), HierarchicalError> {
        for text in assignments {
            let failed = |error: oxhaven_contracts::expr::FormulaError| {
                HierarchicalError::Formula(format!("{text}: {error}"))
            };
            let assignment = compiler.parse_assignment(text).map_err(failed)?;
            let formula = compiler
                .compile(&assignment.expr, &self.table_definition()?)
                .map_err(failed)?;
            let column_type = formula.result_type().clone();
            let column = NodeColumn {
                name: assignment.name.clone(),
                formula,
            };
            let place = self
                .definition
                .iter()
                .position(|column| column.name == assignment.name);
            if !record(self, column) {
                continue;
            }
            match place {
                Some(place) => self.definition[place].column_type = column_type,
                None => self
                    .definition
                    .push(ColumnDefinition::new(assignment.name, column_type)),
            }
        }
        Ok(())
    }

    /// This recorder with the columns of `assignments` (`Name = formula`) added to the
    /// nodes. A column the nodes have is refused, and so is a formula that reads the
    /// position or the key of a row; of two columns of one name the first is kept.
    ///
    /// Behavior: RollupNodeOperationsRecorder#updateView and #mergeUpdateViews
    pub fn update_view(
        &self,
        assignments: &[String],
        compiler: &dyn FormulaCompiler,
    ) -> Result<NodeOperations, HierarchicalError> {
        let mut names = Vec::with_capacity(assignments.len());
        for text in assignments {
            let assignment = compiler
                .parse_assignment(text)
                .map_err(|error| HierarchicalError::Formula(format!("{text}: {error}")))?;
            if self
                .definition
                .iter()
                .any(|column| column.name == assignment.name)
            {
                return Err(illegal(format!(
                    "Column {} already exists",
                    assignment.name
                )));
            }
            names.push(assignment.name);
        }
        let mut result = self.clone();
        let mut positional = Vec::new();
        let mut index = 0;
        result.compile(assignments, compiler, |result, column| {
            if column.formula.uses_row_variables() {
                positional.push(assignments[index].replace(' ', ""));
            }
            index += 1;
            let first = !result
                .update_views
                .iter()
                .any(|kept| kept.name == column.name);
            if first {
                result.update_views.push(column);
            }
            first
        })?;
        if !positional.is_empty() {
            return Err(illegal(format!(
                "updateView does not support virtual row variables. Invalid columns: [{}]",
                positional.join(", ")
            )));
        }
        Ok(result)
    }

    /// This recorder with the formats `formats` (`Column = format`, as
    /// [`crate::ops::select::format`] reads one) of the nodes.
    ///
    /// Behavior: BaseNodeOperationsRecorder#formatColumns, which is the table's own
    /// `formatColumns` on the recorder's result definition, and #mergeFormats
    pub fn format_columns(
        &self,
        formats: &[String],
        compiler: &dyn FormulaCompiler,
    ) -> Result<NodeOperations, HierarchicalError> {
        let assignments =
            format_assignments(formats, &self.visible_names()).map_err(|error| match error {
                crate::ops::select::SelectError::IllegalArgument(message) => illegal(message),
                other => HierarchicalError::Formula(other.to_string()),
            })?;
        let mut result = self.clone();
        result.compile(&assignments, compiler, |result, column| {
            result.formats.push(column);
            true
        })?;
        Ok(result)
    }

    /// This recorder with the filters `filters` of the rows of a node of a tree, each read
    /// as `Table.where` reads a filter string, against the columns of the nodes and the
    /// formatting columns recorded before; a node keeps the rows that pass every filter.
    /// No filter is no change. A filter that reads a column at other rows (`Column_`) is
    /// refused as not ported.
    ///
    /// Behavior: io.deephaven.engine.table.impl.hierarchical.TreeNodeOperationsRecorder#where,
    /// whose filters are initialized for the recorder's result definition, and #mergeFilters
    pub fn where_filters(
        &self,
        filters: &[String],
        compiler: &dyn FormulaCompiler,
    ) -> Result<NodeOperations, HierarchicalError> {
        if filters.is_empty() {
            return Ok(self.clone());
        }
        let parsed = RowFilters::parse(filters, &self.table_definition()?, compiler)
            .map_err(HierarchicalError::Formula)?;
        if parsed.reads_arrays() {
            return Err(HierarchicalError::Unsupported(format!(
                "not ported: io.deephaven.engine.table.impl.hierarchical.TreeNodeOperationsRecorder#where: \
                 a filter of the rows of a node that reads a column at other rows: {}",
                filters.join(", ")
            )));
        }
        let mut result = self.clone();
        result.filters.push(Arc::new(parsed));
        Ok(result)
    }

    /// This recorder with `sorts`, which the caller has checked, in front of its sorts.
    ///
    /// Behavior: BaseNodeOperationsRecorder#sort and #mergeSortColumns
    pub fn sort(&self, sorts: &[SortColumn]) -> NodeOperations {
        NodeOperations {
            sorts: super::merged_sorts(sorts, &self.sorts),
            ..self.clone()
        }
    }
}

/// The columns recorded for the nodes of one type of a hierarchical table, as the table
/// keeps them, and for a tree the filters of its nodes.
#[derive(Clone, Default)]
pub(super) struct NodeColumns {
    pub(super) update_views: Vec<NodeColumn>,
    pub(super) formats: Vec<NodeColumn>,
    pub(super) filters: Vec<Arc<RowFilters>>,
}

impl NodeColumns {
    /// Whether no column is recorded; filters are not columns.
    pub(super) fn is_empty(&self) -> bool {
        self.update_views.is_empty() && self.formats.is_empty()
    }

    /// These columns and those of `operations` after them. Of two update views of one
    /// name the first stays; formats are all kept, and a later one of a name decides.
    /// The filters of `operations` follow these.
    ///
    /// Behavior: RollupNodeOperationsRecorder#withOperations,
    /// TreeNodeOperationsRecorder#withOperations and #mergeFilters
    pub(super) fn with(&self, operations: &NodeOperations) -> NodeColumns {
        let mut result = self.clone();
        for view in &operations.update_views {
            if !result
                .update_views
                .iter()
                .any(|kept| kept.name == view.name)
            {
                result.update_views.push(view.clone());
            }
        }
        result.formats.extend(operations.formats.iter().cloned());
        result.filters.extend(operations.filters.iter().cloned());
        result
    }
}

/// A column of nulls.
struct NullSource(ColumnType);

impl ColumnSource for NullSource {
    fn column_type(&self) -> &ColumnType {
        &self.0
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        Ok(arrow::array::new_null_array(
            &self.0.arrow_type(),
            rows.size() as usize,
        ))
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }
}

/// A column computed from the columns before it when it is read.
struct ComputedSource {
    column_type: ColumnType,
    formula: Arc<dyn Formula>,
    table: Arc<NodeTable>,
}

impl ColumnSource for ComputedSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.formula.evaluate(self.table.as_ref(), rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.formula.evaluate(self.table.as_ref(), rows, true)
    }
}

/// The rows of a table with other columns: those of the table, columns of nulls, and
/// columns computed when they are read. It is read during one snapshot and listened to
/// by no one.
struct NodeTable {
    rows: Arc<dyn Table>,
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
}

impl Dependency for NodeTable {
    fn satisfied(&self, step: u64) -> bool {
        self.rows.satisfied(step)
    }
}

impl Table for NodeTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn row_set(&self) -> Arc<RowSet> {
        self.rows.row_set()
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        self.rows.prev_row_set()
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        self.columns[index].clone()
    }

    fn is_refreshing(&self) -> bool {
        self.rows.is_refreshing()
    }

    fn attributes(&self) -> TableAttributes {
        self.rows.attributes()
    }

    fn is_flat(&self) -> bool {
        self.rows.is_flat()
    }

    fn is_failed(&self) -> bool {
        self.rows.is_failed()
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        self.rows.update_graph()
    }

    fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
        self.rows.add_listener(listener)
    }
}

/// `base`, the table of a node, with a column of nulls for each of `nulls` it does not
/// have, then the update views and then the formats of `columns`, each computed from the
/// columns before it; a column named again takes the place of the earlier one.
///
/// Behavior: RollupTableImpl#applyNodeFormatsAndFilters, which is
/// RollupNodeOperationsRecorder#applyUpdateViews and then
/// BaseNodeOperationsRecorder#applyFormats, each an `updateView` of the node's table
pub(super) fn node_table(
    base: &Arc<dyn Table>,
    nulls: &[ColumnDefinition],
    columns: &NodeColumns,
) -> Result<Arc<dyn Table>, HierarchicalError> {
    let definition = base.definition();
    let mut defined: Vec<ColumnDefinition> = definition.columns().to_vec();
    let mut sources: Vec<Arc<dyn ColumnSource>> =
        (0..defined.len()).map(|index| base.column(index)).collect();
    for null in nulls {
        if definition.index_of(&null.name).is_none() {
            defined.push(null.clone());
            sources.push(Arc::new(NullSource(null.column_type.clone())));
        }
    }
    let table_of = |defined: &[ColumnDefinition], sources: &[Arc<dyn ColumnSource>]| {
        TableDefinition::new(defined.to_vec())
            .map(|definition| {
                Arc::new(NodeTable {
                    rows: base.clone(),
                    definition,
                    columns: sources.to_vec(),
                })
            })
            .map_err(|error| illegal(format!("{error:?}")))
    };
    for column in columns.update_views.iter().chain(&columns.formats) {
        let column_type = column.formula.result_type().clone();
        let source: Arc<dyn ColumnSource> = Arc::new(ComputedSource {
            column_type: column_type.clone(),
            formula: column.formula.clone(),
            table: table_of(&defined, &sources)?,
        });
        match defined.iter().position(|kept| kept.name == column.name) {
            Some(place) => {
                defined[place].column_type = column_type;
                sources[place] = source;
            }
            None => {
                defined.push(ColumnDefinition::new(column.name.clone(), column_type));
                sources.push(source);
            }
        }
    }
    Ok(table_of(&defined, &sources)?)
}

/// A node's table restricted to the rows its filters keep. Like [`NodeTable`] it is read
/// during one snapshot and listened to by no one, so the rows are found when it is made:
/// the current rows, and for a refreshing table the previous rows by the previous values.
struct FilteredNode {
    table: Arc<dyn Table>,
    rows: Arc<RowSet>,
    prev_rows: Arc<RowSet>,
}

impl Dependency for FilteredNode {
    fn satisfied(&self, step: u64) -> bool {
        self.table.satisfied(step)
    }
}

impl Table for FilteredNode {
    fn definition(&self) -> &TableDefinition {
        self.table.definition()
    }

    fn row_set(&self) -> Arc<RowSet> {
        self.rows.clone()
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        self.prev_rows.clone()
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        self.table.column(index)
    }

    fn is_refreshing(&self) -> bool {
        self.table.is_refreshing()
    }

    fn attributes(&self) -> TableAttributes {
        self.table.attributes()
    }

    fn is_flat(&self) -> bool {
        false
    }

    fn is_failed(&self) -> bool {
        self.table.is_failed()
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        self.table.update_graph()
    }

    fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
        self.table.add_listener(listener)
    }
}

/// The rows of `table`, the table of a node, that pass every filter of `filters`, each
/// set applied to the rows the ones before it kept; `table` itself without filters.
///
/// Behavior: TreeNodeOperationsRecorder#applyFilters, a `where` of the node's formatted
/// table
pub(super) fn filtered_node(
    table: Arc<dyn Table>,
    filters: &[Arc<RowFilters>],
) -> Result<Arc<dyn Table>, HierarchicalError> {
    if filters.is_empty() {
        return Ok(table);
    }
    let kept = |rows: &RowSet, use_prev: bool| -> Result<RowSet, TableFailure> {
        let mut rows = rows.clone();
        for set in filters {
            rows = set.accepted(table.as_ref(), &rows, use_prev)?;
        }
        Ok(rows)
    };
    let rows = Arc::new(kept(&table.row_set(), false)?);
    let prev_rows = if table.is_refreshing() {
        Arc::new(kept(&table.prev_row_set(), true)?)
    } else {
        rows.clone()
    };
    Ok(Arc::new(FilteredNode {
        table,
        rows,
        prev_rows,
    }))
}
