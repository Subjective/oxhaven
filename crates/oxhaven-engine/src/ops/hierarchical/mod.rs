//! Hierarchical tables: a rollup, and the view through which a client reads part of one.
//!
//! A hierarchical table is not a table. It is a [`Hierarchy`]: nodes, each with a table of
//! its child rows. A client reads a [`View`], which is a hierarchy and a key table saying
//! which nodes are expanded; [`View::snapshot`] gives a range of the rows of the
//! depth-first walk of the expanded nodes. `docs/HIERARCHICAL.md` is the design.
//!
//! [`RollupTable`] is the one hierarchy so far. The walk ([`walk`]) knows nothing about
//! rollups; a tree is another implementation of [`Hierarchy`].

mod keys;
mod rollup;
#[cfg(test)]
mod tests;
mod walk;

pub use keys::Part;
pub use rollup::{rollup, RollupTable};
pub use walk::Snapshot;

use crate::ops::agg::AggregateError;
use crate::ops::partitioned::PartitionedError;
use crate::ops::sort::SortColumn;
use arrow::array::ArrayRef;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{Table, TableFailure};
use std::fmt;
use std::sync::Arc;

/// The index of the depth column among the columns of a snapshot.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl#ROW_DEPTH_COLUMN_INDEX
pub const DEPTH_COLUMN: usize = 0;
/// The index of the column that says whether a row is expanded.
///
/// Behavior: HierarchicalTableImpl#ROW_EXPANDED_COLUMN_INDEX
pub const EXPANDED_COLUMN: usize = 1;
/// Behavior: HierarchicalTableImpl#ROW_DEPTH_COLUMN
pub const DEPTH_COLUMN_NAME: &str = "__DEPTH__";
/// Behavior: HierarchicalTableImpl#ROW_EXPANDED_COLUMN
pub const EXPANDED_COLUMN_NAME: &str = "__EXPANDED__";

pub(crate) fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// Why a hierarchical table could not be made or read.
#[derive(Clone, PartialEq, Debug)]
pub enum HierarchicalError {
    /// An aggregation of a rollup refused.
    Aggregate(AggregateError),
    /// What Java reports as `IllegalArgumentException`.
    IllegalArgument(String),
    /// What Java reports as `UnsupportedOperationException`.
    Unsupported(String),
    Table(TableFailure),
}

impl fmt::Display for HierarchicalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HierarchicalError::Aggregate(error) => error.fmt(f),
            HierarchicalError::IllegalArgument(message)
            | HierarchicalError::Unsupported(message) => f.write_str(message),
            HierarchicalError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for HierarchicalError {}

impl From<TableFailure> for HierarchicalError {
    fn from(failure: TableFailure) -> Self {
        HierarchicalError::Table(failure)
    }
}

impl From<AggregateError> for HierarchicalError {
    fn from(error: AggregateError) -> Self {
        HierarchicalError::Aggregate(error)
    }
}

impl From<PartitionedError> for HierarchicalError {
    fn from(error: PartitionedError) -> Self {
        match error {
            PartitionedError::Aggregate(error) => HierarchicalError::Aggregate(error),
            PartitionedError::Table(failure) => HierarchicalError::Table(failure),
            other => HierarchicalError::Unsupported(other.to_string()),
        }
    }
}

/// `sorts` in front of the `earlier` sorts of the nodes of a hierarchical table: the new
/// ones decide first. A column named again, absolute or not as before, is taken where it
/// is first named.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.BaseNodeOperationsRecorder#mergeSortColumns,
/// in which a sort by an absolute value has a column name of its own
pub fn merged_sorts(sorts: &[SortColumn], earlier: &[SortColumn]) -> Vec<SortColumn> {
    let mut merged: Vec<SortColumn> = Vec::new();
    for sort in sorts.iter().chain(earlier) {
        let named =
            |kept: &SortColumn| kept.column == sort.column && kept.absolute == sort.absolute;
        if !merged.iter().any(named) {
            merged.push(sort.clone());
        }
    }
    merged
}

/// What names a node, in a key table and between snapshots.
///
/// Behavior: the node keys of HierarchicalTableImpl; for a rollup,
/// io.deephaven.engine.table.impl.hierarchical.RollupNodeKeySource
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum NodeKey {
    /// The node whose child table is the root table.
    Root,
    /// Any other node, by key values.
    Path(Vec<Part>),
}

/// A node as the walk passes it around: how deep it is (the root is 0) and the row key of
/// its row in its parent's child table.
///
/// Behavior: the node ids of RollupTableImpl#makeNodeId
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NodeId {
    pub depth: u32,
    pub row: RowKey,
}

/// What a key table asks for a node.
///
/// Behavior: HierarchicalTableImpl.VisitAction
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    /// Show the node's children.
    Expand,
    /// Show its children and theirs, all the way down, except below a node that is
    /// contracted or has an [`Action::Expand`] of its own.
    ExpandAll,
    /// As [`Action::ExpandAll`], for this many further levels.
    ExpandToDepth(u32),
    /// Under an expand-all: do not show this node's children.
    Contract,
}

impl Action {
    /// The action of a value of a key table's action column.
    ///
    /// Behavior: HierarchicalTableImpl.VisitAction#lookup and #lookupDepth
    pub fn of_wire(value: i8) -> Result<Action, HierarchicalError> {
        const EXPAND_TO_DEPTH_BASE: i8 = 0b0111_0000;
        match value {
            0b001 => Ok(Action::Expand),
            0b011 => Ok(Action::ExpandAll),
            0b100 => Ok(Action::Contract),
            deeper if deeper >= EXPAND_TO_DEPTH_BASE => Ok(Action::ExpandToDepth(
                (deeper - EXPAND_TO_DEPTH_BASE) as u32,
            )),
            other => Err(HierarchicalError::IllegalArgument(format!(
                "Unrecognized key table action {other}"
            ))),
        }
    }
}

/// Where a node key leads.
pub struct Located {
    pub node: NodeId,
    /// The row key of the node's row in its parent's child table; `None` for the root.
    pub row_in_parent: Option<RowKey>,
}

/// The key of a node's parent.
pub enum Parent {
    /// The node is the root.
    None,
    Key(NodeKey),
    /// The node has no parent and is not the root: it cannot be reached.
    Orphan,
}

/// Which rows of a level can be expanded.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Expandable {
    All,
    None,
}

/// Nodes and their child tables: what the walk of a snapshot needs to know of a
/// hierarchical table.
///
/// Behavior: the abstract methods of
/// io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl
pub trait Hierarchy: Send + Sync {
    /// The table the hierarchy is made from.
    fn source(&self) -> &Arc<dyn Table>;

    /// The columns of a snapshot: the depth, whether the row is expanded, then the
    /// columns of the nodes.
    ///
    /// Behavior: HierarchicalTable#getAvailableColumnDefinitions
    fn columns(&self) -> &TableDefinition;

    /// The nodes that are expanded before any row of a key table.
    fn default_expansions(&self) -> Vec<NodeKey>;

    /// The node key of each row of `key_table`, in the order of its rows.
    fn node_keys(&self, key_table: &dyn Table) -> Result<Vec<NodeKey>, HierarchicalError>;

    /// The node of `key`, or `None` if the hierarchy has no such node now or the node
    /// cannot be expanded.
    fn locate(&self, key: &NodeKey) -> Result<Option<Located>, HierarchicalError>;

    fn parent(&self, key: &NodeKey) -> Parent;

    fn root(&self) -> NodeId;

    /// The table of the child rows of `node`, or `None` if it has none.
    fn children(&self, node: NodeId) -> Result<Option<Arc<dyn Table>>, HierarchicalError>;

    /// The rows of `table`, the child table of a node whose rows are at `depth`, in the
    /// order they are shown, or `None` when that is the order of the table.
    ///
    /// Behavior: HierarchicalTableImpl#applyNodeSorts
    fn order(
        &self,
        _depth: u32,
        _table: &dyn Table,
    ) -> Result<Option<Vec<RowKey>>, HierarchicalError> {
        Ok(None)
    }

    /// Whether the rows at `depth` (the rows of the root table are at 1) can be expanded.
    fn rows_expandable(&self, depth: u32) -> Expandable;

    /// The values of column `column` of [`Hierarchy::columns`], which is neither the
    /// depth nor the expanded column, for `rows` of `table`, the child table of a node
    /// whose rows are at `depth`.
    fn fill(
        &self,
        depth: u32,
        table: &dyn Table,
        rows: &RowSet,
        column: usize,
    ) -> Result<ArrayRef, HierarchicalError>;
}

/// A hierarchical table as one client looks at it: with the nodes expanded that its key
/// table names.
///
/// Behavior: io.deephaven.server.hierarchicaltable.HierarchicalTableView
#[derive(Clone)]
pub struct View {
    hierarchy: Arc<dyn Hierarchy>,
    keys: Option<Arc<dyn Table>>,
    action_column: Option<String>,
}

impl View {
    /// A view of `hierarchy` with its default expansions, then those of the rows of
    /// `keys`. `action_column` names the byte column of `keys` that holds each row's
    /// action; without it every row expands its node.
    pub fn new(
        hierarchy: Arc<dyn Hierarchy>,
        keys: Option<Arc<dyn Table>>,
        action_column: Option<String>,
    ) -> View {
        View {
            hierarchy,
            keys,
            action_column,
        }
    }

    pub fn hierarchy(&self) -> &Arc<dyn Hierarchy> {
        &self.hierarchy
    }

    pub fn key_table(&self) -> Option<&Arc<dyn Table>> {
        self.keys.as_ref()
    }

    /// Rows `first .. first + count` of the walk of the expanded nodes, with the columns
    /// `columns` of [`Hierarchy::columns`] (`None`: all), and the number of rows of the
    /// whole walk. Fewer rows are returned where the walk ends earlier.
    ///
    /// Runs under the shared lock of the update graph when the source or the key table
    /// is refreshing.
    ///
    /// Behavior: HierarchicalTableImpl#snapshot
    pub fn snapshot(
        &self,
        columns: Option<&[usize]>,
        first: u64,
        count: u64,
    ) -> Result<Snapshot, HierarchicalError> {
        let all: Vec<usize>;
        let columns = match columns {
            Some(columns) => columns,
            None => {
                all = (0..self.hierarchy.columns().columns().len()).collect();
                &all
            }
        };
        if !columns.contains(&DEPTH_COLUMN) || !columns.contains(&EXPANDED_COLUMN) {
            return Err(HierarchicalError::IllegalArgument(
                "Request must include the \"row expanded\" and \"row depth\" columns".to_string(),
            ));
        }
        let read = || {
            let directives = walk::directives(
                self.hierarchy.as_ref(),
                self.keys.as_deref(),
                self.action_column.as_deref(),
            )?;
            walk::snapshot(self.hierarchy.as_ref(), directives, columns, first, count)
        };
        let source = self.hierarchy.source();
        let graph = [Some(source), self.keys.as_ref()]
            .into_iter()
            .flatten()
            .filter(|table| table.is_refreshing())
            .find_map(|table| table.update_graph());
        match graph {
            Some(graph) => locked(graph.as_ref(), read),
            None => read(),
        }
    }
}
