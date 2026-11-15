//! A rollup: the aggregations of a table at every level of a key hierarchy.

use super::keys::{parts, KeyIndex};
use super::{
    Expandable, HierarchicalError, Hierarchy, Located, NodeId, NodeKey, Parent, Part,
    DEPTH_COLUMN_NAME, EXPANDED_COLUMN_NAME,
};
use crate::ops::agg::{aggregate, aggregate_with, AggSpec, Aggregation};
use crate::ops::partitioned::{partition_by, tables_of, PartitionedTable};
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::Int32Type;
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use std::sync::Arc;

/// The children of the rows of one level: the next level partitioned by this level's
/// keys, and where each key's row is in the partition's underlying table.
struct ChildParts {
    partitioned: PartitionedTable,
    index: KeyIndex,
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
    aggregations: Vec<Aggregation>,
    group_by: Vec<String>,
    /// One per level, 0 to the number of group-by columns.
    levels: Vec<Arc<dyn Table>>,
    /// Per level, the row of each key.
    indexes: Vec<KeyIndex>,
    /// `children[d]` for `d >= 1` and below the last level: the rows of level `d + 1` by
    /// the keys of level `d`. Level 0 has one row, whose children are level 1.
    children: Vec<Option<ChildParts>>,
    /// The columns of a snapshot.
    columns: TableDefinition,
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
    let level = |keys: &[&str]| match compiler {
        Some(compiler) => aggregate_with(source, aggregations, false, keys, compiler),
        None => aggregate(source, aggregations, false, keys),
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
    Ok(RollupTable {
        source: source.clone(),
        aggregations: aggregations.to_vec(),
        group_by: group_by.iter().map(|name| name.to_string()).collect(),
        levels,
        indexes,
        children,
        columns,
    })
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

    /// The source aggregated by the first `depth` group-by columns.
    pub fn level(&self, depth: usize) -> Option<&Arc<dyn Table>> {
        self.levels.get(depth)
    }

    /// The key values of row `row` of level `width`, which has `width` key columns.
    fn key_of(&self, width: usize, row: i64) -> Result<Vec<Part>, HierarchicalError> {
        let rows = RowSet::from_range(row, row);
        let level = &self.levels[width];
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
        let depths = key_table
            .column(column(DEPTH_COLUMN_NAME, &ColumnType::Int)?)
            .chunk(&rows)?;
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
        for (index, name) in self.group_by.iter().take(widest).enumerate() {
            let key_type = &self.columns.columns()[2 + index].column_type;
            values.push(parts(
                &key_table.column(column(name, key_type)?).chunk(&rows)?,
            )?);
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
        Ok(self.indexes[key.len()].find(key)?.map(|row| Located {
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
            return Ok(self.levels.get(depth).cloned());
        }
        let Some(Some(parts)) = self.children.get(depth - 1) else {
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
