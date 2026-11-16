//! A rollup: the aggregations of a table at every level of a key hierarchy.

use super::keys::{parts, KeyIndex};
use super::{
    Expandable, HierarchicalError, Hierarchy, Located, NodeId, NodeKey, Parent, Part,
    DEPTH_COLUMN_NAME, EXPANDED_COLUMN_NAME,
};
use crate::ops::agg::{aggregate, aggregate_with, AggSpec, Aggregation};
use crate::ops::filter::{CheckedFilters, FilterError};
use crate::ops::partitioned::{partition_by, tables_of, PartitionedTable};
use crate::ops::select::drop_columns;
use crate::ops::sort::{check_sort_columns, sorted_rows, SortColumn, SortError};
use crate::table::sortable_columns;
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::Int32Type;
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use std::sync::Arc;

/// The children of the rows of one level: the next level partitioned by this level's
/// keys, and where each key's row is in the partition's underlying table.
struct ChildParts {
    partitioned: PartitionedTable,
    index: KeyIndex,
}

/// The levels of a rollup and how a node is found in them. Rollups that differ only in
/// how their nodes are sorted share them.
struct Levels {
    /// One per level, 0 to the number of group-by columns.
    tables: Vec<Arc<dyn Table>>,
    /// Per level, the row of each key.
    indexes: Vec<KeyIndex>,
    /// `children[d]` for `d >= 1` and below the last level: the rows of level `d + 1` by
    /// the keys of level `d`. Level 0 has one row, whose children are level 1.
    children: Vec<Option<ChildParts>>,
}

/// The aggregations of a table by every prefix of its group-by columns.
///
/// Level `d` is the source aggregated by the first `d` group-by columns: level 0 is the
/// one row of the grand total. The children of a row of level `d` are the rows of level
/// `d + 1` with the same first `d` keys.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.RollupTableImpl. Java makes the
/// coarser levels by aggregating the finer ones again; here every level aggregates the
/// source (`docs/HIERARCHICAL.md`, "Decision").
pub struct RollupTable {
    source: Arc<dyn Table>,
    /// What the levels aggregate: the source, or the rows of it that the filters on
    /// group-by columns accept.
    base: Arc<dyn Table>,
    aggregations: Vec<Aggregation>,
    group_by: Vec<String>,
    levels: Arc<Levels>,
    /// The columns of a snapshot.
    columns: TableDefinition,
    /// What the rows of every node are sorted by, the column that decides first.
    sorts: Vec<SortColumn>,
    /// The columns a client may sort by, when the source's sorting was restricted.
    sortable: Option<Vec<String>>,
}

/// The name Java refuses `aggregation` in a rollup by, if it does.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor.UnsupportedRollupAggregations,
/// and RollupBaseConverter#visit(Partition) for a rollup without constituents
fn refused_by_java(aggregation: &Aggregation) -> Option<&'static str> {
    match aggregation {
        Aggregation::Columns { spec, .. } => match spec {
            AggSpec::Median { .. } => Some("Median"),
            AggSpec::Percentile { .. } => Some("Percentile"),
            AggSpec::Formula { .. } => Some("Formula"),
            _ => None,
        },
        Aggregation::Partition { .. } => Some("Partition"),
        _ => None,
    }
}

/// What Java's way of making the coarser levels, aggregating the finer ones again, refuses
/// or answers differently from aggregating the source, which is how the levels are made
/// here.
///
/// A coarser level has a null column for every group-by column it does not group by, and
/// Java counts those among the outputs: an aggregation's output of such a name is a
/// duplicate. And Java sorts the rows of a finer level for a sorted first or last by that
/// level's columns of the sort columns' names: it fails when there is none
/// (`NoSuchColumnException`), and where there is one that is not the sort column's own
/// values the result is another. Only a sort column that the aggregation itself carries
/// under its own name gives what the source gives; the rest is refused until it is ruled
/// whether Java's answers are to be reproduced.
///
/// Behavior: the constructor of io.deephaven.engine.table.impl.by.AggregationProcessor
/// (`RollupAggregationOutputs`), and
/// AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator
fn check_reaggregation(
    aggregations: &[Aggregation],
    group_by: &[&str],
) -> Result<(), HierarchicalError> {
    let mut duplicates = Vec::new();
    for (index, key) in group_by.iter().enumerate() {
        if group_by[..index].contains(key) {
            continue;
        }
        let outputs = aggregations.iter().map(|aggregation| match aggregation {
            Aggregation::Columns { pairs, .. } => {
                pairs.iter().filter(|pair| pair.output == *key).count()
            }
            Aggregation::Count { column } => usize::from(column == key),
            _ => 0,
        });
        let used = outputs.sum::<usize>() + group_by.iter().filter(|other| *other == key).count();
        if used > 1 {
            duplicates.push(format!("{key} used {used} times"));
        }
    }
    if !duplicates.is_empty() {
        return Err(HierarchicalError::IllegalArgument(format!(
            "Duplicate output columns found: {}",
            duplicates.join(", ")
        )));
    }
    for aggregation in aggregations {
        let Aggregation::Columns { spec, pairs } = aggregation else {
            continue;
        };
        let (AggSpec::SortedFirst { columns } | AggSpec::SortedLast { columns }) = spec else {
            continue;
        };
        let carried = |column: &String| {
            pairs
                .iter()
                .any(|pair| pair.output == *column && pair.input == *column)
        };
        if !columns.iter().all(carried) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator",
                "a sorted first or last in a rollup with group-by columns, sorted by a column that is not one of its own results"
            );
        }
    }
    Ok(())
}

/// `source` rolled up by `group_by` with `aggregations`. `compiler` compiles what the
/// aggregations hold of formulas.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#rollup,
/// RollupTableImpl#makeRollup
pub fn rollup(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    include_constituents: bool,
    group_by: &[&str],
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<RollupTable, HierarchicalError> {
    for aggregation in aggregations {
        if let Some(name) = refused_by_java(aggregation) {
            return Err(HierarchicalError::Unsupported(format!(
                "Agg{name} is not supported for rollup()"
            )));
        }
        match aggregation {
            Aggregation::CountWhere { .. } => oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor.RollupBaseConverter#visit(CountWhere)",
                "a count where in a rollup"
            ),
            Aggregation::Formula { .. } => oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor.RollupBaseConverter#visit(Formula)",
                "a formula aggregation in a rollup"
            ),
            Aggregation::Columns {
                spec: AggSpec::Group,
                ..
            } => oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor.RollupBaseConverter#visit(AggSpecGroup)",
                "a group aggregation in a rollup"
            ),
            _ => {}
        }
    }
    if !group_by.is_empty() {
        check_reaggregation(aggregations, group_by)?;
    }
    if include_constituents {
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#makeRollup",
            "a rollup that includes constituents"
        );
    }
    if source.attributes().blink {
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.by.AggregationProcessor#forRollupBase",
            "a rollup of a blink table"
        );
    }
    let finest = build_levels(source, aggregations, group_by, compiler)?;
    // Behavior: RollupTableImpl#makeRollup, which copies the restriction of the source's
    // sorting for the columns the rollup has
    let sortable = sortable_columns(source.as_ref()).map(|names| {
        names
            .into_iter()
            .filter(|name| finest.1.index_of(name).is_some())
            .collect()
    });
    Ok(RollupTable {
        source: source.clone(),
        base: source.clone(),
        aggregations: aggregations.to_vec(),
        group_by: group_by.iter().map(|name| name.to_string()).collect(),
        levels: Arc::new(finest.0),
        columns: finest.1,
        sorts: Vec::new(),
        sortable,
    })
}

/// The levels of `base` rolled up by `group_by`, and the columns of a snapshot of them.
///
/// Behavior: RollupTableImpl#makeRollupInternal
fn build_levels(
    base: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    group_by: &[&str],
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<(Levels, TableDefinition), HierarchicalError> {
    let source = base;
    // A rollup without aggregations has the group-by columns alone: its levels are counted,
    // which an aggregation without aggregations cannot be, and the count is left out.
    let mut hidden = "__ROLLUP_ROWS__".to_string();
    while source.definition().index_of(&hidden).is_some() {
        hidden.push('_');
    }
    let counted = [Aggregation::count(hidden.clone())];
    let (aggregated, hidden) = if aggregations.is_empty() {
        (&counted[..], Some(hidden))
    } else {
        (aggregations, None)
    };
    let level = |keys: &[&str]| -> Result<Arc<dyn Table>, HierarchicalError> {
        let level = match compiler {
            Some(compiler) => aggregate_with(source, aggregated, false, keys, compiler),
            None => aggregate(source, aggregated, false, keys),
        }?;
        match &hidden {
            Some(hidden) => drop_columns(&level, &[hidden])
                .map_err(|error| HierarchicalError::IllegalArgument(error.to_string())),
            None => Ok(level),
        }
    };
    let levels = (0..=group_by.len())
        .map(|depth| level(&group_by[..depth]))
        .collect::<Result<Vec<_>, _>>()?;
    let indexes = levels
        .iter()
        .enumerate()
        .map(|(depth, level)| KeyIndex::new(level.clone(), (0..depth).collect()))
        .collect();
    let mut children = Vec::with_capacity(levels.len());
    for depth in 0..levels.len() {
        children.push(if depth == 0 || depth + 1 >= levels.len() {
            None
        } else {
            let partitioned = partition_by(&levels[depth + 1], &group_by[..depth], false)?;
            let index = KeyIndex::new(partitioned.table().clone(), (0..depth).collect());
            Some(ChildParts { partitioned, index })
        });
    }
    // The node columns are those of the finest level: the group-by columns, then the
    // results. Behavior: RollupTableImpl#computeAvailableColumnDefinitions
    let finest = levels[group_by.len()].definition();
    let mut columns = vec![
        ColumnDefinition::new(DEPTH_COLUMN_NAME, ColumnType::Int),
        ColumnDefinition::new(EXPANDED_COLUMN_NAME, ColumnType::Boolean),
    ];
    columns.extend(finest.columns().iter().cloned());
    let columns = TableDefinition::new(columns).map_err(|duplicate| {
        HierarchicalError::IllegalArgument(format!("Duplicate column name {}", duplicate.0))
    })?;
    Ok((
        Levels {
            tables: levels,
            indexes,
            children,
        },
        columns,
    ))
}

impl RollupTable {
    /// Behavior: RollupTable#getAggregations
    pub fn aggregations(&self) -> &[Aggregation] {
        &self.aggregations
    }

    /// Behavior: RollupTable#getGroupByColumns
    pub fn group_by(&self) -> &[String] {
        &self.group_by
    }

    /// Behavior: RollupTable#includesConstituents
    pub fn includes_constituents(&self) -> bool {
        false
    }

    /// What the rows of every node are sorted by, the column that decides first.
    pub fn sorts(&self) -> &[SortColumn] {
        &self.sorts
    }

    /// The columns a client may sort by, or `None` when it may sort by any: the columns
    /// the source's sorting was restricted to, of those the rollup has.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#getSortableColumns
    pub fn sortable_columns(&self) -> Option<&[String]> {
        self.sortable.as_deref()
    }

    /// Whether the nodes of this rollup can be sorted by `sorts`: its nodes have the
    /// columns, of types that sort, and the absolute ones of types that have an absolute
    /// value.
    ///
    /// # Panics
    /// With `unported!` for a column of a type the engine's sort does not have, such as
    /// the vectors of a distinct aggregation.
    ///
    /// Behavior: io.deephaven.engine.table.impl.hierarchical.BaseNodeOperationsRecorder.SortRecordingTableAdapter#sort
    /// and #absoluteSelectColumns
    pub fn check_sorts(&self, sorts: &[SortColumn]) -> Result<(), HierarchicalError> {
        let finest = &self.levels.tables[self.group_by.len()];
        check_sort_columns(finest.as_ref(), sorts).map_err(|error| match error {
            SortError::NoSuchColumn(missing) => {
                let available: Vec<&str> = self.columns.columns()[2..]
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

    /// This rollup with the rows of every node sorted by `sorts`, and where those leave
    /// rows equal by what they were sorted by before. A column named again, absolute or
    /// not as before, is taken where it is first named. The sorts are not checked:
    /// [`RollupTable::check_sorts`] does that, for the rollup they were recorded for.
    ///
    /// Behavior: RollupTableImpl#withNodeOperations with the sorts of a recorder for
    /// aggregated nodes, and BaseNodeOperationsRecorder#mergeSortColumns
    pub fn with_sorts(&self, sorts: &[SortColumn]) -> RollupTable {
        let mut merged: Vec<SortColumn> = Vec::new();
        for sort in sorts.iter().chain(&self.sorts) {
            let named = |earlier: &SortColumn| {
                earlier.column == sort.column && earlier.absolute == sort.absolute
            };
            if !merged.iter().any(named) {
                merged.push(sort.clone());
            }
        }
        RollupTable {
            source: self.source.clone(),
            base: self.base.clone(),
            aggregations: self.aggregations.clone(),
            group_by: self.group_by.clone(),
            levels: self.levels.clone(),
            columns: self.columns.clone(),
            sorts: merged,
            sortable: self.sortable.clone(),
        }
    }

    /// Why `filters`, made for the columns of the source, may not filter this rollup, if
    /// they may not: a filter reads a column that an aggregation reads, or a column at
    /// other rows.
    ///
    /// Behavior: RollupTableImpl#initializeAndValidateFilters
    pub fn check_filters(&self, filters: &CheckedFilters) -> Result<(), String> {
        let aggregated = |name: &str| {
            self.aggregations
                .iter()
                .any(|aggregation| match aggregation {
                    Aggregation::Columns { pairs, .. } => {
                        pairs.iter().any(|pair| pair.input == name)
                    }
                    _ => false,
                })
        };
        let valid: Vec<&str> = self
            .source
            .definition()
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .filter(|name| !aggregated(name))
            .collect();
        for filter in filters.reads() {
            let invalid: Vec<&str> = filter
                .columns
                .iter()
                .map(String::as_str)
                .filter(|name| !valid.contains(name))
                .collect();
            if !invalid.is_empty() {
                return Err(format!(
                    "Invalid filter found: {} may only use non-aggregation columns, which are \
                     [{}], but has used [{}]",
                    filter.text,
                    valid.join(", "),
                    invalid.join(", ")
                ));
            }
            if filter.arrays {
                return Err(format!(
                    "Invalid filter found: {} may not use column arrays",
                    filter.text
                ));
            }
        }
        Ok(())
    }

    /// This rollup of the rows of the source that every filter accepts. The filters are
    /// made for the columns of the source ([`CheckedFilters`]) and may read every column
    /// that no aggregation reads. With filters that read group-by columns only the source
    /// stays the source; otherwise the filtered table becomes it. The sorts are kept.
    ///
    /// Java filters the finest level when the filters read group-by columns only, and
    /// aggregates the coarser levels from that; a group is wholly kept or wholly left
    /// out, so aggregating the filtered source gives the same levels in the same order.
    ///
    /// # Panics
    /// With `unported!` for a filter that uses `i`, `ii` or `k`: Java accepts one as a
    /// filter of group-by columns, and it then counts the rows of the finest level.
    ///
    /// Behavior: RollupTableImpl#withFilter and #rebase
    pub fn with_filters(
        &self,
        filters: &CheckedFilters,
        compiler: &dyn FormulaCompiler,
    ) -> Result<RollupTable, HierarchicalError> {
        self.check_filters(filters)
            .map_err(HierarchicalError::IllegalArgument)?;
        if filters.reads().iter().any(|filter| filter.row_variables) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#withFilter",
                "a filter of a rollup that uses i, ii or k, which Java evaluates on the rows of the finest level"
            );
        }
        let base = filters
            .apply(&self.base, compiler)
            .map_err(|error| match error {
                FilterError::Invalid(message) => HierarchicalError::IllegalArgument(message),
                FilterError::Failed(failure) => HierarchicalError::Table(failure),
            })?;
        let only_group_by = filters.reads().iter().all(|filter| {
            filter
                .columns
                .iter()
                .all(|name| self.group_by.contains(name))
        });
        let group_by: Vec<&str> = self.group_by.iter().map(String::as_str).collect();
        let (levels, columns) = build_levels(&base, &self.aggregations, &group_by, Some(compiler))?;
        Ok(RollupTable {
            source: if only_group_by {
                self.source.clone()
            } else {
                base.clone()
            },
            base,
            aggregations: self.aggregations.clone(),
            group_by: self.group_by.clone(),
            levels: Arc::new(levels),
            columns,
            sorts: self.sorts.clone(),
            sortable: self.sortable.clone(),
        })
    }

    /// The source aggregated by the first `depth` group-by columns.
    pub fn level(&self, depth: usize) -> Option<&Arc<dyn Table>> {
        self.levels.tables.get(depth)
    }

    /// The key values of row `row` of level `width`, which has `width` key columns.
    fn key_of(&self, width: usize, row: i64) -> Result<Vec<Part>, HierarchicalError> {
        let rows = RowSet::from_range(row, row);
        let level = &self.levels.tables[width];
        (0..width)
            .map(|column| {
                let mut values = parts(&level.column(column).chunk(&rows)?)?;
                values
                    .pop()
                    .ok_or_else(|| super::failure("a rollup level has no such row").into())
            })
            .collect()
    }
}

impl Hierarchy for RollupTable {
    fn source(&self) -> &Arc<dyn Table> {
        &self.source
    }

    fn columns(&self) -> &TableDefinition {
        &self.columns
    }

    /// Behavior: RollupTableImpl#getDefaultExpansionNodeKeys
    fn default_expansions(&self) -> Vec<NodeKey> {
        if self.group_by.is_empty() {
            vec![NodeKey::Root]
        } else {
            vec![NodeKey::Root, NodeKey::Path(Vec::new())]
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.hierarchical.RollupNodeKeySource#fillFromGroupByValues,
    /// with the columns RollupTableImpl#makeNodeKeySource takes from the key table
    fn node_keys(&self, key_table: &dyn Table) -> Result<Vec<NodeKey>, HierarchicalError> {
        let definition = key_table.definition();
        let column = |name: &str, column_type: &ColumnType| {
            definition
                .index_of(name)
                .filter(|&index| &definition.columns()[index].column_type == column_type)
                .ok_or_else(|| {
                    HierarchicalError::IllegalArgument(format!(
                        "The key table has no column {name} of the type the rollup has"
                    ))
                })
        };
        let rows = key_table.row_set();
        let depth_column = column(DEPTH_COLUMN_NAME, &ColumnType::Int)?;
        // Java asks the key table for every group-by column, of the rollup's type.
        // Behavior: RollupTableImpl#makeNodeKeySource
        let key_columns = self
            .group_by
            .iter()
            .enumerate()
            .map(|(index, name)| column(name, &self.columns.columns()[2 + index].column_type))
            .collect::<Result<Vec<_>, _>>()?;
        let depths = key_table.column(depth_column).chunk(&rows)?;
        let depths = depths
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| super::failure("the depth column of a key table is not int"))?;
        let max_depth = self.group_by.len() as i64 + 1;
        // Java reads the key columns of the widest key only.
        let widest = depths
            .iter()
            .map(|depth| i64::from(depth.unwrap_or(i32::MIN)) - 1)
            .max()
            .unwrap_or(0)
            .clamp(0, self.group_by.len() as i64) as usize;
        let mut values: Vec<Vec<Part>> = Vec::with_capacity(widest);
        for &key_column in key_columns.iter().take(widest) {
            values.push(parts(&key_table.column(key_column).chunk(&rows)?)?);
        }
        depths
            .iter()
            .enumerate()
            .map(|(row, depth)| {
                // Java's null int is a depth like any other here, and not a valid one
                let depth = i64::from(depth.unwrap_or(i32::MIN));
                if depth < 0 || depth > max_depth {
                    return Err(HierarchicalError::IllegalArgument(format!(
                        "Invalid depth {depth}, maximum for this rollup is {max_depth}"
                    )));
                }
                Ok(if depth == 0 {
                    NodeKey::Root
                } else {
                    NodeKey::Path(
                        values[..depth as usize - 1]
                            .iter()
                            .map(|column| column[row].clone())
                            .collect(),
                    )
                })
            })
            .collect()
    }

    /// Behavior: RollupTableImpl#nodeKeyToNodeId and #findRowKeyInParentUnsorted
    fn locate(&self, key: &NodeKey) -> Result<Option<Located>, HierarchicalError> {
        let NodeKey::Path(key) = key else {
            return Ok(Some(Located {
                node: self.root(),
                row_in_parent: None,
            }));
        };
        // a row of the last level has no children
        if key.len() >= self.group_by.len() {
            return Ok(None);
        }
        Ok(self.levels.indexes[key.len()]
            .find(key)?
            .map(|row| Located {
                node: NodeId {
                    depth: key.len() as u32 + 1,
                    row,
                },
                row_in_parent: Some(row),
            }))
    }

    /// Behavior: RollupTableImpl#findParentNodeKey
    fn parent(&self, key: &NodeKey) -> Parent {
        match key {
            NodeKey::Root => Parent::None,
            NodeKey::Path(key) => Parent::Key(match key.split_last() {
                None => NodeKey::Root,
                Some((_, parent)) => NodeKey::Path(parent.to_vec()),
            }),
        }
    }

    fn root(&self) -> NodeId {
        NodeId { depth: 0, row: 0 }
    }

    /// Behavior: RollupTableImpl#nodeIdToNodeBaseTable
    fn children(&self, node: NodeId) -> Result<Option<Arc<dyn Table>>, HierarchicalError> {
        let depth = node.depth as usize;
        if depth <= 1 {
            // the root's child is the grand total, whose children are all of level 1
            return Ok(self.levels.tables.get(depth).cloned());
        }
        let Some(Some(parts)) = self.levels.children.get(depth - 1) else {
            return Ok(None);
        };
        let key = self.key_of(depth - 1, node.row)?;
        let Some(row) = parts.index.find(&key)? else {
            return Ok(None);
        };
        let underlying = parts.partitioned.table();
        let cell = underlying
            .definition()
            .index_of(parts.partitioned.constituent_column())
            .ok_or_else(|| super::failure("a partition without its constituent column"))?;
        let handles = underlying
            .column(cell)
            .chunk(&RowSet::from_range(row, row))?;
        Ok(tables_of(&handles)?.pop().flatten())
    }

    /// A coarser level does not have the group-by columns below its own; Java has them
    /// there as columns of nulls, by which every row is equal, so a sort by one is left
    /// out.
    ///
    /// Behavior: RollupTableImpl#applyNodeSorts, which is
    /// BaseNodeOperationsRecorder#applySorts
    fn order(
        &self,
        depth: u32,
        table: &dyn Table,
    ) -> Result<Option<Vec<RowKey>>, HierarchicalError> {
        // the rows at `depth` are rows of level `depth - 1`, which has that many keys
        let keys = (depth as usize).saturating_sub(1);
        let sorts: Vec<SortColumn> = self
            .sorts
            .iter()
            .filter(|sort| {
                self.group_by
                    .iter()
                    .position(|key| *key == sort.column)
                    .is_none_or(|key| key < keys)
            })
            .cloned()
            .collect();
        if sorts.is_empty() {
            return Ok(None);
        }
        sorted_rows(table, &table.row_set(), &sorts)
            .map(Some)
            .map_err(|error| match error {
                SortError::Table(failure) => HierarchicalError::Table(failure),
                other => HierarchicalError::IllegalArgument(other.to_string()),
            })
    }

    /// Behavior: RollupTableImpl#levelExpandable
    fn rows_expandable(&self, depth: u32) -> Expandable {
        if (depth as usize) <= self.group_by.len() {
            Expandable::All
        } else {
            Expandable::None
        }
    }

    /// Behavior: RollupTableImpl#makeOrFillChunkSourceArray, and the null columns of
    /// io.deephaven.engine.table.impl.by.rollup.NullColumns for the keys a level does not
    /// group by
    fn fill(
        &self,
        depth: u32,
        table: &dyn Table,
        rows: &RowSet,
        column: usize,
    ) -> Result<ArrayRef, HierarchicalError> {
        // the rows at `depth` are rows of level `depth - 1`, which has that many keys
        let keys = depth as usize - 1;
        let node_column = column - 2;
        let all_keys = self.group_by.len();
        if node_column < all_keys && node_column >= keys {
            let column_type = self.columns.columns()[column].column_type.arrow_type();
            return Ok(arrow::array::new_null_array(
                &column_type,
                rows.size() as usize,
            ));
        }
        let in_level = if node_column < all_keys {
            node_column
        } else {
            node_column - all_keys + keys
        };
        Ok(table.column(in_level).chunk(rows)?)
    }
}
