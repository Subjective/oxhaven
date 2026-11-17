//! A tree: the rows of a table under the rows their parent column names.

use super::keys::{parts, KeyIndex};
use super::node_ops::{filtered_node, node_table, NodeColumns, NodeOperations};
use super::{
    failure, merged_sorts, Expandable, HierarchicalError, Hierarchy, Located, NodeId, NodeKey,
    Parent, Part, DEPTH_COLUMN_NAME, EXPANDED_COLUMN_NAME,
};
use crate::ops::agg::{aggregate, describe_for_compatibility, Aggregation};
use crate::ops::filter::{CheckedFilters, FilterError};
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use crate::ops::partitioned::{of_aggregation, tables_of, PartitionedTable};
use crate::ops::select::view;
use crate::ops::sort::{check_sort_columns, restrict_sort_to, sorted_rows, SortColumn, SortError};
use crate::table::{sortable_columns, QueryTable, TableSpec};
use arrow::array::{Array, ArrayRef};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::graph::Dependency;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// The column of the tables of children in the partition of a tree's source.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.TreeTableImpl#TREE_COLUMN
const TREE_COLUMN: &str = "__TREE__";

/// The row of each identifier in the table a tree was first made of.
///
/// The map is made anew in every cycle that adds, removes or shifts a row or changes an
/// identifier. A second row with an identifier fails it: for a static table the tree is
/// refused, and for a refreshing one the map stays as it was before that cycle for
/// good, as Java's does, whose aggregation fails and is read all the same.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.TreeSourceRowLookup, over
/// io.deephaven.engine.table.impl.by.UniqueRowKeyChunkedOperator
pub(super) struct RowLookup {
    rows: Arc<Mutex<HashMap<Part, RowKey>>>,
    /// Listens to the table and says when the map is up to date in a cycle.
    listener: Arc<QueryTable>,
}

/// Behavior: UniqueRowKeyChunkedOperator#addChunk, as
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper reports its refusal
fn rows_by_identifier(
    source: &dyn Table,
    id_column: usize,
) -> Result<HashMap<Part, RowKey>, TableFailure> {
    let rows = source.row_set();
    let identifiers = parts(&source.column(id_column).chunk(&rows)?)?;
    let mut by_identifier = HashMap::with_capacity(identifiers.len());
    for (identifier, row) in identifiers.into_iter().zip(rows.iter()) {
        if by_identifier.insert(identifier, row).is_some() {
            return Err(failure(
                "Failed to add data, inputcolumns=[], outputs=[__ROW_KEY__]: the identifier of \
                 a row of a tree is that of another row",
            ));
        }
    }
    Ok(by_identifier)
}

impl RowLookup {
    fn new(source: &Arc<dyn Table>, id_column: usize) -> Result<RowLookup, TableFailure> {
        let rows = Arc::new(Mutex::new(HashMap::new()));
        let listener = QueryTable::derive(source, || {
            *rows.lock().unwrap_or_else(PoisonError::into_inner) =
                rows_by_identifier(source.as_ref(), id_column)?;
            // a table of nothing: it is there to be told of the source's changes
            let spec = TableSpec::new(
                TableDefinition::new(Vec::new()).map_err(|_| failure("no definition"))?,
                RowSet::empty(),
                Vec::new(),
            );
            let (source, rows) = (source.clone(), rows.clone());
            let operator = move |_result: &QueryTable, upstream: &TableUpdate| {
                let moved = !upstream.added.is_empty()
                    || !upstream.removed.is_empty()
                    || !upstream.shifted.is_empty();
                let renamed =
                    !upstream.modified.is_empty() && upstream.modified_columns.contains(id_column);
                if moved || renamed {
                    let current = rows_by_identifier(source.as_ref(), id_column)?;
                    *rows.lock().unwrap_or_else(PoisonError::into_inner) = current;
                }
                Ok(TableUpdate::empty())
            };
            Ok((spec, operator))
        })?;
        Ok(RowLookup { rows, listener })
    }

    /// The row of `identifier`, which may be a row the table does not have now.
    ///
    /// Behavior: TreeSourceRowLookup#get
    pub(super) fn get(&self, identifier: &Part) -> Option<RowKey> {
        self.rows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(identifier)
            .copied()
    }

    /// What has finished for a step when the map is up to date.
    pub(super) fn dependency(&self) -> Arc<dyn Dependency> {
        self.listener.clone()
    }
}

/// The tables of children of a tree's source: the source partitioned by the parent
/// column, in which a parent that loses every child keeps its empty table.
///
/// Behavior: TreeTableImpl#computeTree
struct Nodes {
    partitioned: PartitionedTable,
    index: KeyIndex,
    cell: usize,
}

impl Nodes {
    fn new(source: &Arc<dyn Table>, parent_column: &str) -> Result<Nodes, HierarchicalError> {
        let aggregated = aggregate(
            source,
            &[Aggregation::Partition {
                column: TREE_COLUMN.to_string(),
                include_group_by_columns: true,
            }],
            true,
            &[parent_column],
        )?;
        let partitioned = of_aggregation(source, aggregated, &[parent_column], TREE_COLUMN, true)?;
        let underlying = partitioned.table().clone();
        let cell = underlying
            .definition()
            .index_of(partitioned.constituent_column())
            .ok_or_else(|| failure("a partition without its constituent column"))?;
        Ok(Nodes {
            index: KeyIndex::new(underlying, vec![0]),
            partitioned,
            cell,
        })
    }

    /// The table of the rows whose parent is `identifier`, if there ever was such a row.
    ///
    /// Behavior: TreeTableImpl#nodeKeyToNodeId and #nodeIdToNodeBaseTable
    fn children(&self, identifier: &Part) -> Result<Option<Arc<dyn Table>>, HierarchicalError> {
        let Some(row) = self.index.find(std::slice::from_ref(identifier))? else {
            return Ok(None);
        };
        let handles = self
            .partitioned
            .table()
            .column(self.cell)
            .chunk(&RowSet::from_range(row, row))?;
        Ok(tables_of(&handles)?.pop().flatten())
    }
}

/// The rows of a table as a tree: the children of a row are the rows whose parent
/// column has the row's identifier, and the rows without a parent are the children of
/// the root. A row whose parent is the identifier of no row is an orphan and is not
/// shown.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.TreeTableImpl
pub struct TreeTable {
    source: Arc<dyn Table>,
    id_column: usize,
    parent_column: usize,
    nodes: Arc<Nodes>,
    /// Of the table the tree was first made of, which a filtered tree shares.
    lookup: Arc<RowLookup>,
    /// The identifier that names the root: null.
    root_key: Part,
    /// The columns of a snapshot: the structural ones, then those of the nodes.
    columns: TableDefinition,
    /// What the rows of every node are sorted by, the column that decides first.
    sorts: Vec<SortColumn>,
    /// The formats of the rows of every node, and the filters they pass.
    node_columns: NodeColumns,
    /// The columns a client may sort by, when the source's sorting was restricted.
    sortable: Option<Vec<String>>,
}

/// Behavior: io.deephaven.engine.table.ColumnDefinition#describeForCompatibility
fn described(column: &ColumnDefinition) -> String {
    describe_for_compatibility(&column.name, &column.column_type)
}

/// `source` as a tree by its columns `id_column` and `parent_column`. A blink table is
/// refused before its columns are looked at.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#tree, TreeTableImpl#makeTree
pub fn tree(
    source: &Arc<dyn Table>,
    id_column: &str,
    parent_column: &str,
) -> Result<TreeTable, HierarchicalError> {
    if source.attributes().blink {
        return Err(HierarchicalError::Unsupported(
            "Blink tables do not support tree; use BlinkTableTools.blinkToAppendOnly to \
             accumulate full history"
                .to_string(),
        ));
    }
    let definition = source.definition();
    let column = |name: &str, what: &str| {
        definition.index_of(name).ok_or_else(|| {
            let available: Vec<&str> = definition
                .columns()
                .iter()
                .map(|column| column.name.as_str())
                .collect();
            HierarchicalError::IllegalArgument(format!(
                "tree {what} column: Unknown column names [{name}], available column names \
                 are [{}]",
                available.join(", ")
            ))
        })
    };
    let id = column(id_column, "identifier")?;
    let parent = column(parent_column, "parent")?;
    let (id_definition, parent_definition) =
        (&definition.columns()[id], &definition.columns()[parent]);
    if id_definition.column_type != parent_definition.column_type {
        return Err(HierarchicalError::IllegalArgument(format!(
            "tree parent and identifier columns must have the same data type, but parent is {} \
             and identifier is {}",
            described(parent_definition),
            described(id_definition)
        )));
    }
    let nodes = Nodes::new(source, parent_column)?;
    let lookup = RowLookup::new(source, id)?;
    let root_key = parts(&arrow::array::new_null_array(
        &id_definition.column_type.arrow_type(),
        1,
    ))?
    .pop()
    .ok_or_else(|| failure("no key for the root of a tree"))?;
    // Behavior: TreeTableImpl#computeAvailableColumnDefinitions
    let mut columns = vec![
        ColumnDefinition::new(DEPTH_COLUMN_NAME, ColumnType::Int),
        ColumnDefinition::new(EXPANDED_COLUMN_NAME, ColumnType::Boolean),
    ];
    columns.extend(definition.columns().iter().cloned());
    let columns = TableDefinition::new(columns).map_err(|duplicate| {
        HierarchicalError::IllegalArgument(format!("Duplicate column name {}", duplicate.0))
    })?;
    Ok(TreeTable {
        source: source.clone(),
        id_column: id,
        parent_column: parent,
        nodes: Arc::new(nodes),
        lookup: Arc::new(lookup),
        root_key,
        columns,
        sorts: Vec::new(),
        node_columns: NodeColumns::default(),
        // Behavior: TreeTableImpl#makeTreeInternal, which copies the restriction whole
        sortable: sortable_columns(source.as_ref()),
    })
}

/// `source` with null for the parent of every row whose parent is the identifier of no
/// row: as a tree, such rows are children of the root. The rows and the other columns
/// are the source's.
///
/// Behavior: io.deephaven.engine.table.hierarchical.TreeTable#promoteOrphans
pub fn promote_orphans(
    source: &Arc<dyn Table>,
    id_column: &str,
    parent_column: &str,
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, HierarchicalError> {
    const MATCHED: &str = "__MATCHED_PARENT_IDENTIFIER__";
    let illegal =
        |error: &dyn std::fmt::Display| HierarchicalError::IllegalArgument(error.to_string());
    let joined = natural_join(
        source,
        source,
        &[JoinMatch::new(parent_column, id_column)],
        &[JoinAddition::renamed(MATCHED, id_column)],
        NaturalJoinType::ErrorOnDuplicate,
    )
    .map_err(|error| illegal(&error))?;
    // Java's join copies the sortable columns of its left table, which the join here
    // does not yet; the view then keeps those it does not replace.
    let joined = match sortable_columns(&**source) {
        Some(sortable) => restrict_sort_to(&joined, &sortable).map_err(|error| illegal(&error))?,
        None => joined,
    };
    let columns: Vec<String> = source
        .definition()
        .columns()
        .iter()
        .map(|column| {
            if column.name == parent_column {
                format!("{parent_column} = isNull({MATCHED}) ? null : {parent_column}")
            } else {
                column.name.clone()
            }
        })
        .collect();
    view(&joined, &columns, compiler).map_err(|error| illegal(&error))
}

impl TreeTable {
    /// Behavior: TreeTable#getIdentifierColumn
    pub fn identifier_column(&self) -> &str {
        &self.source.definition().columns()[self.id_column].name
    }

    /// Behavior: TreeTable#getParentIdentifierColumn
    pub fn parent_identifier_column(&self) -> &str {
        &self.source.definition().columns()[self.parent_column].name
    }

    /// What the rows of every node are sorted by, the column that decides first.
    pub fn sorts(&self) -> &[SortColumn] {
        &self.sorts
    }

    /// The columns of a snapshot: the depth, whether the row is expanded, then the
    /// columns of the source.
    pub fn snapshot_definition(&self) -> &TableDefinition {
        &self.columns
    }

    /// The columns a client may sort by, or `None` when it may sort by any.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#getSortableColumns
    pub fn sortable_columns(&self) -> Option<&[String]> {
        self.sortable.as_deref()
    }

    fn with_source(&self, source: Arc<dyn Table>, nodes: Arc<Nodes>) -> TreeTable {
        TreeTable {
            source,
            id_column: self.id_column,
            parent_column: self.parent_column,
            nodes,
            lookup: self.lookup.clone(),
            root_key: self.root_key.clone(),
            columns: self.columns.clone(),
            sorts: self.sorts.clone(),
            node_columns: self.node_columns.clone(),
            sortable: self.sortable.clone(),
        }
    }

    /// The columns of the nodes: those of the source, then the formatting columns.
    ///
    /// Behavior: TreeTableImpl#getNodeDefinition
    pub fn node_definition(&self) -> &[ColumnDefinition] {
        &self.columns.columns()[2..]
    }

    /// A recorder of operations on the nodes of this tree, with nothing recorded.
    ///
    /// Behavior: TreeTableImpl#makeNodeOperationsRecorder
    pub fn node_operations(&self) -> NodeOperations {
        NodeOperations::new(self.node_definition().to_vec())
    }

    /// Whether the nodes have operations: Java keeps a recorder for them then.
    fn operated(&self) -> bool {
        !self.sorts.is_empty()
            || !self.node_columns.is_empty()
            || !self.node_columns.filters.is_empty()
    }

    /// This tree with what `operations` recorded for its nodes: the formats after those
    /// the nodes have, the filters after theirs, and the sorts in front of theirs. The
    /// tree itself when nothing was recorded. The sorts were checked when they were
    /// recorded.
    ///
    /// Java keeps one recorder for the nodes and merges a second into it only when both
    /// give the nodes the same columns: so a recorder with formats is refused for nodes
    /// that have operations of any kind, unless it was made of a tree whose nodes have
    /// those formats already.
    ///
    /// Behavior: TreeTableImpl#withNodeOperations and #accumulateOperations, which is
    /// TreeNodeOperationsRecorder#withOperations
    pub fn with_node_operations(
        &self,
        operations: &NodeOperations,
    ) -> Result<TreeTable, HierarchicalError> {
        if operations.is_empty() {
            return Ok(self.with_source(self.source.clone(), self.nodes.clone()));
        }
        if self.operated() && operations.definition() != self.node_definition() {
            return Err(HierarchicalError::IllegalArgument(
                "Incompatible operation recorders; compatible recorders must be created from \
                 the same table"
                    .to_string(),
            ));
        }
        let mut result = self.with_source(self.source.clone(), self.nodes.clone());
        let mut columns = self.columns.columns()[..2].to_vec();
        columns.extend(operations.definition().iter().cloned());
        result.columns = TableDefinition::new(columns).map_err(|duplicate| {
            HierarchicalError::IllegalArgument(format!("Duplicate column name {}", duplicate.0))
        })?;
        result.sorts = merged_sorts(operations.sorts(), &self.sorts);
        result.node_columns = self.node_columns.with(operations);
        Ok(result)
    }

    /// The table of the rows of a node as a snapshot reads them: `base`, the rows whose
    /// parent is the node, with the formatting columns, and of those the rows that pass
    /// the filters.
    ///
    /// Behavior: TreeTableImpl#applyNodeFormatsAndFilters
    fn node(&self, base: Arc<dyn Table>) -> Result<Arc<dyn Table>, HierarchicalError> {
        let formatted = if self.node_columns.is_empty() {
            base
        } else {
            node_table(&base, &[], &self.node_columns)?
        };
        filtered_node(formatted, &self.node_columns.filters)
    }

    /// Whether the nodes of this tree can be sorted by `sorts`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.hierarchical.BaseNodeOperationsRecorder.SortRecordingTableAdapter#sort
    /// and #absoluteSelectColumns
    pub fn check_sorts(&self, sorts: &[SortColumn]) -> Result<(), HierarchicalError> {
        check_sort_columns(self.source.as_ref(), sorts).map_err(|error| match error {
            SortError::NoSuchColumn(missing) => {
                // the formatting columns are not named
                let available: Vec<&str> = self
                    .source
                    .definition()
                    .columns()
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                HierarchicalError::IllegalArgument(format!(
                    "Unknown column names [{}], available column names are [{}]",
                    missing.join(", "),
                    available.join(", ")
                ))
            }
            SortError::Table(failure) => HierarchicalError::Table(failure),
            other => HierarchicalError::IllegalArgument(other.to_string()),
        })
    }

    /// This tree with the rows of every node sorted by `sorts`, and where those leave
    /// rows equal by what they were sorted by before. The sorts are not checked:
    /// [`TreeTable::check_sorts`] does that.
    ///
    /// Behavior: TreeTableImpl#withNodeOperations with the sorts of a recorder, and
    /// BaseNodeOperationsRecorder#mergeSortColumns
    pub fn with_sorts(&self, sorts: &[SortColumn]) -> TreeTable {
        let mut sorted = self.with_source(self.source.clone(), self.nodes.clone());
        sorted.sorts = merged_sorts(sorts, &self.sorts);
        sorted
    }

    /// This tree of the rows of its source that every filter accepts and of their
    /// ancestors. The filters are made for the columns of the source
    /// ([`CheckedFilters`]). The filtered table becomes the source, so that a second
    /// filter may accept a row that was kept as an ancestor. The sorts are kept.
    ///
    /// Behavior: TreeTableImpl#withFilter with no node-filter columns, which makes every
    /// filter one of the source
    pub fn with_filters(
        &self,
        filters: &CheckedFilters,
        compiler: &dyn FormulaCompiler,
    ) -> Result<TreeTable, HierarchicalError> {
        let filtered = super::tree_filter::filtered(
            &self.source,
            &self.lookup,
            self.parent_column,
            filters,
            compiler,
        )
        .map_err(|error| match error {
            FilterError::Invalid(message) => HierarchicalError::IllegalArgument(message),
            FilterError::Failed(failure) => HierarchicalError::Table(failure),
        })?;
        let nodes = Nodes::new(&filtered, self.parent_identifier_column())?;
        Ok(self.with_source(filtered, Arc::new(nodes)))
    }

    /// The node key of an identifier as a column holds it.
    fn key_of(&self, identifiers: &ArrayRef, at: usize, part: Part) -> NodeKey {
        if identifiers.is_null(at) {
            NodeKey::Root
        } else {
            NodeKey::Path(vec![part])
        }
    }

    /// The row of the node of `identifier` in the source, if the source has it now.
    ///
    /// Behavior: TreeTableImpl#findRowKeyInParentUnsorted
    fn row_of(&self, identifier: &Part) -> Option<RowKey> {
        self.lookup
            .get(identifier)
            .filter(|&row| self.source.row_set().contains(row))
    }

    /// The children of the root are the rows without a parent; those of another node the
    /// rows whose parent is the identifier of the node's row, `node.row` of the source.
    ///
    /// # Panics
    /// With `unported!` for a row without an identifier, which only an expansion of
    /// every row or to a depth reaches. In Java its children are the rows of the root,
    /// itself among them: expanded to a depth, the rows of the root that follow it show
    /// the depth of the deepest visit, and with every row expanded Java's stack
    /// overflows and the process ends.
    ///
    /// Behavior: TreeTableImpl#makeChildNodeIdLookup and #nodeIdToNodeBaseTable
    fn base_children(&self, node: NodeId) -> Result<Option<Arc<dyn Table>>, HierarchicalError> {
        if node.depth == 0 {
            return self.nodes.children(&self.root_key);
        }
        let identifiers = self
            .source
            .column(self.id_column)
            .chunk(&RowSet::from_range(node.row, node.row))?;
        if identifiers.is_null(0) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.hierarchical.TreeTableImpl#makeChildNodeIdLookup",
                "the expansion of a row without an identifier, whose children are the rows \
                 of the root"
            );
        }
        match parts(&identifiers)?.pop() {
            Some(identifier) => self.nodes.children(&identifier),
            None => Ok(None),
        }
    }

    fn identifier(key: &NodeKey) -> Result<&Part, HierarchicalError> {
        match key {
            NodeKey::Path(key) if key.len() == 1 => Ok(&key[0]),
            _ => Err(failure("a node key of a tree is one identifier").into()),
        }
    }
}

impl Hierarchy for TreeTable {
    fn source(&self) -> &Arc<dyn Table> {
        &self.source
    }

    fn columns(&self) -> &[ColumnDefinition] {
        self.columns.columns()
    }

    /// Behavior: TreeTableImpl#getDefaultExpansionNodeKeys
    fn default_expansions(&self) -> Vec<NodeKey> {
        vec![NodeKey::Root]
    }

    /// Behavior: TreeTableImpl#makeNodeKeySource; a null identifier names the root
    /// (#isRootNodeKey)
    fn node_keys(&self, key_table: &dyn Table) -> Result<Vec<NodeKey>, HierarchicalError> {
        let name = self.identifier_column();
        let column_type = &self.source.definition().columns()[self.id_column].column_type;
        let definition = key_table.definition();
        let column = definition
            .index_of(name)
            .filter(|&index| &definition.columns()[index].column_type == column_type)
            .ok_or_else(|| {
                HierarchicalError::IllegalArgument(format!(
                    "The key table has no column {name} of the type the tree has"
                ))
            })?;
        let identifiers = key_table.column(column).chunk(&key_table.row_set())?;
        Ok(parts(&identifiers)?
            .into_iter()
            .enumerate()
            .map(|(at, part)| self.key_of(&identifiers, at, part))
            .collect())
    }

    /// A node is one whose identifier was ever the parent of a row and whose own row the
    /// source has now.
    ///
    /// Behavior: HierarchicalTableImpl.LinkedDirectiveFactory#newValue and
    /// #linkedDirectiveInvalid, over TreeTableImpl#nodeKeyToNodeId and
    /// #findRowKeyInParentUnsorted
    fn locate(&self, key: &NodeKey) -> Result<Option<Located>, HierarchicalError> {
        if *key == NodeKey::Root {
            return Ok(Some(Located {
                node: self.root(),
                row_in_parent: None,
            }));
        }
        let identifier = Self::identifier(key)?;
        if self.nodes.children(identifier)?.is_none() {
            return Ok(None);
        }
        Ok(self.row_of(identifier).map(|row| Located {
            node: NodeId { depth: 1, row },
            row_in_parent: Some(row),
        }))
    }

    /// Java takes a parent that is not a node, for a node that has a row, as a state it
    /// cannot be in and fails the snapshot: a key table that names an orphan which has or
    /// had children fails every snapshot of its view.
    ///
    /// Behavior: TreeTableImpl#findParentNodeKey, and the assertion of
    /// HierarchicalTableImpl#linkKeyTableNodeDirectives for a parent that is no node
    fn parent(&self, key: &NodeKey) -> Result<Parent, HierarchicalError> {
        if *key == NodeKey::Root {
            return Ok(Parent::None);
        }
        let Some(row) = self.row_of(Self::identifier(key)?) else {
            return Ok(Parent::Orphan);
        };
        let parents = self
            .source
            .column(self.parent_column)
            .chunk(&RowSet::from_range(row, row))?;
        let part = parts(&parents)?
            .pop()
            .ok_or_else(|| failure("a tree's source has no such row"))?;
        let parent = self.key_of(&parents, 0, part);
        if self.locate(&parent)?.is_none() {
            return Err(failure(
                "Assertion failed: asserted Failed to find ancestor for existing key table \
                 directive is never executed.",
            )
            .into());
        }
        Ok(Parent::Key(parent))
    }

    fn root(&self) -> NodeId {
        NodeId { depth: 0, row: 0 }
    }

    /// The rows of the node of `node` with its formats, that pass its filters.
    ///
    /// Behavior: TreeTableImpl#nodeIdToNodeBaseTable and #applyNodeFormatsAndFilters
    fn children(&self, node: NodeId) -> Result<Option<Arc<dyn Table>>, HierarchicalError> {
        let Some(base) = self.base_children(node)? else {
            return Ok(None);
        };
        self.node(base).map(Some)
    }

    /// Behavior: TreeTableImpl#applyNodeSorts, which is
    /// BaseNodeOperationsRecorder#applySorts
    fn order(
        &self,
        _depth: u32,
        table: &dyn Table,
    ) -> Result<Option<Vec<RowKey>>, HierarchicalError> {
        if self.sorts.is_empty() {
            return Ok(None);
        }
        sorted_rows(table, &table.row_set(), &self.sorts)
            .map(Some)
            .map_err(|error| match error {
                SortError::Table(failure) => HierarchicalError::Table(failure),
                other => HierarchicalError::IllegalArgument(other.to_string()),
            })
    }

    /// Behavior: TreeTableImpl#levelExpandable
    fn rows_expandable(&self, _depth: u32) -> Expandable {
        Expandable::ByRow
    }

    /// A row can be expanded if a row has its identifier for parent now, and passes the
    /// filters of the nodes when they have some.
    ///
    /// Behavior: TreeTableImpl#nodeIdExpandable, which reads the traversal table: the
    /// node's table filtered when the nodes have filters (#hasNodeFiltersToApply)
    fn expandable_rows(
        &self,
        table: &dyn Table,
        rows: &RowSet,
    ) -> Result<Vec<bool>, HierarchicalError> {
        let identifiers = parts(&table.column(self.id_column).chunk(rows)?)?;
        let filtered = !self.node_columns.filters.is_empty();
        identifiers
            .iter()
            .map(|identifier| {
                let Some(children) = self.nodes.children(identifier)? else {
                    return Ok(false);
                };
                let children = if filtered {
                    self.node(children)?
                } else {
                    children
                };
                Ok(!children.row_set().is_empty())
            })
            .collect()
    }

    /// Behavior: TreeTableImpl#makeOrFillChunkSourceArray
    fn fill(
        &self,
        _depth: u32,
        table: &dyn Table,
        rows: &RowSet,
        column: usize,
    ) -> Result<ArrayRef, HierarchicalError> {
        Ok(table.column(column - 2).chunk(rows)?)
    }
}
