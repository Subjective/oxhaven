//! Partitioned tables: a table with a column of tables, its constituents.
//!
//! `docs/PARTITIONED.md` is the design. In short: a cell of a table column is a handle
//! ([`resolve`]); the partition aggregation ([`Partition`]) makes one constituent per group
//! and tells each what changed in it; [`PartitionedTable`] is the underlying table and the
//! facts Java keeps beside it.

mod cells;
mod merge;
mod partition;
#[cfg(test)]
mod tests;
mod transform;
#[cfg(test)]
mod transform_tests;
mod validate;
#[cfg(test)]
mod validate_tests;

pub use cells::{resolve, TableCells, TableColumn};
pub use partition::{Partition, ResultLink};
pub use transform::Transformer;
pub use validate::require_matching_keys;

use crate::ops::agg::{aggregate, AggregateError, Aggregation};
use crate::ops::distinct::where_in;
use crate::ops::join::JoinMatch;
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::Int64Type;
use oxhaven_contracts::graph::{locked, Dependency, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{
    ColumnSource, InputTable, Table, TableAttributes, TableFailure, UpdateListener,
};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, Weak};

/// The name Java gives the constituent column of a `partitionBy`.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#CONSTITUENT
pub const CONSTITUENT: &str = "__CONSTITUENT__";

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// Why a partitioned table could not be made or used.
#[derive(Clone, PartialEq, Debug)]
pub enum PartitionedError {
    /// A blink table cannot be partitioned.
    Blink,
    /// The aggregation behind `partition_by` refused.
    Aggregate(AggregateError),
    /// What Java reports as `IllegalArgumentException`.
    IllegalArgument(String),
    /// What Java reports as `UnsupportedOperationException`.
    Unsupported(String),
    /// What Java reports as `IllegalStateException`.
    IllegalState(String),
    /// What Java reports as `NullPointerException`.
    NullPointer(String),
    Table(TableFailure),
}

impl fmt::Display for PartitionedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionedError::Blink => f.write_str(
                "Blink tables do not support partitionBy; use \
                 BlinkTableTools.blinkToAppendOnly to accumulate full history",
            ),
            PartitionedError::Aggregate(error) => error.fmt(f),
            PartitionedError::IllegalArgument(message)
            | PartitionedError::Unsupported(message)
            | PartitionedError::IllegalState(message)
            | PartitionedError::NullPointer(message) => f.write_str(message),
            PartitionedError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for PartitionedError {}

impl From<TableFailure> for PartitionedError {
    fn from(failure: TableFailure) -> Self {
        PartitionedError::Table(failure)
    }
}

impl From<AggregateError> for PartitionedError {
    fn from(error: AggregateError) -> Self {
        PartitionedError::Aggregate(error)
    }
}

/// A table of tables: the underlying table, which has the key columns and one column of
/// tables, and what Java's `PartitionedTable` says about it.
///
/// Behavior: io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl
#[derive(Clone)]
pub struct PartitionedTable {
    table: Arc<dyn Table>,
    key_columns: Vec<String>,
    unique_keys: bool,
    constituent_column: String,
    constituent_definition: TableDefinition,
    constituent_changes_permitted: bool,
    /// The key columns that every constituent has too, with the value of its row of the
    /// underlying table in every row.
    ///
    /// Behavior: PartitionedTableImpl's `consistentKeyColumnNames`
    consistent_key_columns: Vec<String>,
    /// The result of the last `merge`, while someone still has it.
    ///
    /// Behavior: the `memoizedMerge` of PartitionedTableImpl#merge
    merged: Arc<Mutex<Option<Weak<dyn Table>>>>,
}

impl PartitionedTable {
    /// Behavior: PartitionedTableImpl's constructor and
    /// io.deephaven.engine.table.PartitionedTableFactory#of, whose checks these are: the
    /// key columns and the constituent column are columns of `table`, and the constituent
    /// column holds tables.
    pub fn new(
        table: Arc<dyn Table>,
        key_columns: Vec<String>,
        unique_keys: bool,
        constituent_column: String,
        constituent_definition: TableDefinition,
        constituent_changes_permitted: bool,
    ) -> Result<PartitionedTable, PartitionedError> {
        let definition = table.definition();
        for name in &key_columns {
            if definition.index_of(name).is_none() {
                return Err(PartitionedError::IllegalArgument(format!(
                    "Partitioned table does not have a key column named {name}"
                )));
            }
        }
        let holds_tables = definition
            .index_of(&constituent_column)
            .map(|index| &definition.columns()[index].column_type)
            .is_some_and(|column_type| matches!(column_type, ColumnType::Table(_)));
        if !holds_tables {
            return Err(PartitionedError::IllegalArgument(format!(
                "Partitioned table does not have a constituent column of tables named \
                 {constituent_column}"
            )));
        }
        Ok(PartitionedTable {
            table,
            key_columns,
            unique_keys,
            constituent_column,
            constituent_definition,
            constituent_changes_permitted,
            consistent_key_columns: Vec::new(),
            merged: Arc::default(),
        })
    }

    /// The key columns that every constituent has too, with the value of its row of the
    /// underlying table in every row: a filter on them may be applied to the underlying
    /// table in the place of every constituent. A partition that keeps the key columns
    /// has them all; a transform has none unless it is told.
    ///
    /// Behavior: PartitionedTableImpl#getConsistentKeyColumnNames
    pub fn consistent_key_columns(&self) -> &[String] {
        &self.consistent_key_columns
    }

    /// This partitioned table with `names`, those of them that are key columns, as its
    /// consistent key columns. The caller says that each is one.
    ///
    /// Behavior: the `consistentKeyColumnNames` of PartitionedTableImpl's constructor
    pub fn with_consistent_key_columns(mut self, names: &[String]) -> PartitionedTable {
        self.consistent_key_columns = self
            .key_columns
            .iter()
            .filter(|key| names.contains(key))
            .cloned()
            .collect();
        self
    }

    /// The underlying table. Behavior: PartitionedTable#table
    pub fn table(&self) -> &Arc<dyn Table> {
        &self.table
    }

    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }

    pub fn unique_keys(&self) -> bool {
        self.unique_keys
    }

    pub fn constituent_column(&self) -> &str {
        &self.constituent_column
    }

    pub fn constituent_definition(&self) -> &TableDefinition {
        &self.constituent_definition
    }

    pub fn constituent_changes_permitted(&self) -> bool {
        self.constituent_changes_permitted
    }

    /// This partitioned table with another underlying table, which has the same columns:
    /// a filter or a sort of the underlying table. `unique_keys` is kept, as both keep it.
    ///
    /// Behavior: PartitionedTableImpl#filter and #sort
    pub fn with_table(&self, table: Arc<dyn Table>) -> Result<PartitionedTable, PartitionedError> {
        let replaced = PartitionedTable::new(
            table,
            self.key_columns.clone(),
            self.unique_keys,
            self.constituent_column.clone(),
            self.constituent_definition.clone(),
            self.constituent_changes_permitted,
        )?;
        Ok(replaced.with_consistent_key_columns(&self.consistent_key_columns))
    }

    /// This partitioned table as one whose constituents may come and go when its
    /// underlying table refreshes, whose merge then takes no attribute from them.
    ///
    /// Behavior: PartitionedTableServiceGrpcImpl#getConstituents, which makes the
    /// partitioned table of the rows a key table asks for with
    /// `constituentChangesPermitted` true, whatever the partitioned table itself has, and
    /// PartitionedTableImpl's constructor, which keeps that only for a refreshing table.
    /// Seen on Java 42.9: the merge for a static key table has the constituents'
    /// add-only and append-only, and the merge for a refreshing one has neither.
    pub fn with_constituent_changes_permitted(mut self) -> PartitionedTable {
        self.constituent_changes_permitted = self.table.is_refreshing();
        self
    }

    fn column(&self) -> Arc<dyn ColumnSource> {
        let index = self
            .table
            .definition()
            .index_of(&self.constituent_column)
            .expect("checked when the partitioned table was made");
        self.table.column(index)
    }

    /// The constituents now, in the order of the underlying table's rows. A constituent
    /// of a refreshing partitioned table keeps the partitioned table alive, so that it
    /// goes on being updated.
    ///
    /// Behavior: PartitionedTableImpl#constituents
    pub fn constituents(&self) -> Result<Vec<Arc<dyn Table>>, PartitionedError> {
        let read = || -> Result<Vec<Arc<dyn Table>>, PartitionedError> {
            let handles = self.column().chunk(&self.table.row_set())?;
            tables_of(&handles)?
                .into_iter()
                .map(|table| {
                    table
                        .map(|table| self.held(table))
                        .ok_or_else(|| failure("a partitioned table has a null constituent").into())
                })
                .collect()
        };
        match self.table.update_graph() {
            Some(graph) if self.table.is_refreshing() => locked(&*graph, read),
            _ => read(),
        }
    }

    /// The constituent of the one row of `rows`, a table with the columns of the
    /// underlying table (a filter of it), or `None` when it has no row.
    ///
    /// Behavior: PartitionedTableImpl#constituentFor, after its filter
    pub fn single_constituent(
        &self,
        rows: &Arc<dyn Table>,
    ) -> Result<Option<Arc<dyn Table>>, PartitionedError> {
        let mut found = self.with_table(rows.clone())?.constituents()?;
        if found.len() > 1 {
            return Err(PartitionedError::Unsupported(format!(
                "Result size mismatch: expected 0 or 1 results, instead found {}",
                found.len()
            )));
        }
        Ok(found.pop())
    }

    /// The rows of the underlying table whose keys are rows of `keys`, a table with the
    /// key columns.
    ///
    /// Behavior: the `whereIn` of
    /// io.deephaven.server.partitionedtable.PartitionedTableServiceGrpcImpl#getConstituents
    pub fn rows_for_keys(&self, keys: &Arc<dyn Table>) -> Result<Arc<dyn Table>, PartitionedError> {
        let on: Vec<JoinMatch> = self
            .key_columns
            .iter()
            .map(|name| JoinMatch::same(name.as_str()))
            .collect();
        where_in(&self.table, keys, &on)
            .map_err(|error| PartitionedError::IllegalArgument(error.to_string()))
    }

    fn held(&self, table: Arc<dyn Table>) -> Arc<dyn Table> {
        if table.is_refreshing() && self.table.is_refreshing() {
            Arc::new(Held {
                table,
                _owner: self.table.clone(),
            })
        } else {
            table
        }
    }

    /// The rows of every constituent, one constituent after the other in the order of the
    /// underlying table's rows.
    ///
    /// Behavior: PartitionedTableImpl#merge
    pub fn merge(&self) -> Result<Arc<dyn Table>, PartitionedError> {
        let mut memo = self.merged.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(merged) = memo.as_ref().and_then(Weak::upgrade) {
            if !merged.is_failed() {
                return Ok(merged);
            }
        }
        let merged = merge::merge(self)?;
        *memo = Some(Arc::downgrade(&merged));
        Ok(merged)
    }

    /// A table without rows that has the columns of a constituent.
    ///
    /// Behavior: PartitionedTableImpl#emptyConstituent
    pub fn empty_constituent(&self) -> Result<Arc<dyn Table>, PartitionedError> {
        Ok(merge::empty(self.constituent_definition.clone())?)
    }
}

/// The tables of the cells of `handles`, a chunk of a table column; `None` for a null
/// cell. An error names a cell whose table is gone.
pub fn tables_of(handles: &ArrayRef) -> Result<Vec<Option<Arc<dyn Table>>>, TableFailure> {
    let handles = handles
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| failure("a column of tables must hold handles"))?;
    handles
        .iter()
        .map(|handle| match handle {
            None => Ok(None),
            Some(handle) => resolve(handle).map(Some).ok_or_else(|| {
                failure(
                    "unported: io.deephaven.engine.table.impl.sources.ObjectArraySource#get: \
                     a constituent read from a copy of a partitioned table's cells, after \
                     the partitioned table it was made by is gone",
                )
            }),
        })
        .collect()
}

/// Splits `parent` into one constituent per distinct value of the `keys` columns. With
/// `drop_keys` the constituents do not have the key columns. With no key column there is
/// one constituent, with every row.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#partitionBy
pub fn partition_by(
    parent: &Arc<dyn Table>,
    keys: &[&str],
    drop_keys: bool,
) -> Result<PartitionedTable, PartitionedError> {
    if parent.attributes().blink {
        return Err(PartitionedError::Blink);
    }
    let partitioned = aggregate(
        parent,
        &[Aggregation::Partition {
            column: CONSTITUENT.to_string(),
            include_group_by_columns: !drop_keys,
        }],
        false,
        keys,
    )?;
    of_aggregation(parent, partitioned, keys, CONSTITUENT, !drop_keys)
}

/// The partitioned table of `aggregated`, an aggregation of `parent` by `keys` that has a
/// partition aggregation in the column `constituent_column`, made with or without the key
/// columns in its constituents (`include_keys`).
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#partitionedAggBy, after its `aggBy`
pub fn of_aggregation(
    parent: &Arc<dyn Table>,
    aggregated: Arc<dyn Table>,
    keys: &[&str],
    constituent_column: &str,
    include_keys: bool,
) -> Result<PartitionedTable, PartitionedError> {
    // Key columns named twice count once, as in Java's `LinkedHashSet` of them.
    let mut key_columns: Vec<String> = Vec::new();
    for key in keys {
        if !key_columns.iter().any(|seen| seen == key) {
            key_columns.push(key.to_string());
        }
    }
    let constituent_definition = constituent_definition(parent.definition(), keys, include_keys);
    let consistent = match include_keys {
        true => key_columns.clone(),
        false => Vec::new(),
    };
    let partitioned = PartitionedTable::new(
        aggregated,
        key_columns,
        true,
        constituent_column.to_string(),
        constituent_definition,
        parent.is_refreshing(),
    )?;
    Ok(partitioned.with_consistent_key_columns(&consistent))
}

/// The definition of the constituents of a partition of a table with `definition`.
pub(crate) fn constituent_definition(
    definition: &TableDefinition,
    keys: &[&str],
    include_keys: bool,
) -> TableDefinition {
    let columns = definition
        .columns()
        .iter()
        .filter(|column| include_keys || !keys.contains(&column.name.as_str()))
        .cloned()
        .collect();
    TableDefinition::new(columns).expect("a subset of a definition has unique names")
}

/// A constituent as it is handed out: the constituent, and the table whose operator
/// keeps it up to date.
struct Held {
    table: Arc<dyn Table>,
    _owner: Arc<dyn Table>,
}

impl Dependency for Held {
    fn satisfied(&self, step: u64) -> bool {
        self.table.satisfied(step)
    }
}

impl Table for Held {
    fn definition(&self) -> &TableDefinition {
        self.table.definition()
    }
    fn row_set(&self) -> Arc<RowSet> {
        self.table.row_set()
    }
    fn prev_row_set(&self) -> Arc<RowSet> {
        self.table.prev_row_set()
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
        self.table.is_flat()
    }
    fn is_failed(&self) -> bool {
        self.table.is_failed()
    }
    fn input_table(&self) -> Option<Arc<dyn InputTable>> {
        self.table.input_table()
    }
    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        self.table.update_graph()
    }
    fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
        self.table.add_listener(listener)
    }
}
