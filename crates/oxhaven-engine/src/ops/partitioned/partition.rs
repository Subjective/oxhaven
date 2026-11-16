//! The partition aggregation: one constituent table per group.

use super::cells::{TableCells, TableColumn};
use crate::ops::agg::{AggOperator, Effect, GroupReader, Run};
use crate::table::{QueryTable, TableSpec};
use arrow::array::{ArrayRef, Int64Array};
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

/// When the constituents of one partition aggregation have finished for a step: when the
/// aggregation's result has, which is set once the result exists. Until then, and after
/// the result is gone, nothing will notify them.
///
/// Behavior: the dependency of a constituent on the aggregation's listener, the
/// `addParentReference` of
/// io.deephaven.engine.table.impl.by.PartitionByChunkedOperator#linkTableReferences
#[derive(Default)]
pub struct ResultLink(OnceLock<Weak<dyn Dependency>>);

impl ResultLink {
    /// Names the aggregation's result. Call it under the lock the result was made under.
    pub fn set(&self, result: Weak<dyn Dependency>) {
        let _ = self.0.set(result);
    }
}

impl Dependency for ResultLink {
    fn satisfied(&self, step: u64) -> bool {
        match self.0.get().and_then(Weak::upgrade) {
            Some(result) => result.satisfied(step),
            None => true,
        }
    }
}

/// What one cycle did to one group, as the framework told it.
#[derive(Default)]
struct Pending {
    added: Vec<RowKey>,
    /// In the parent's previous key space.
    removed: Vec<RowKey>,
    modified: Vec<RowKey>,
    shifted: bool,
}

/// The partition aggregation: the result column holds, for each group, a table of the
/// group's rows. The table is a view of the parent: it has the parent's columns (without
/// the group-by columns unless they are included) and the group's rows as its row set.
///
/// A constituent is made when its group is first seen and lives as long as the result. In
/// a cycle it is told what was added to, removed from, modified in and shifted in its
/// group; a group that loses every row leaves an empty constituent, which is told of the
/// rows that come back. A change inside a constituent is never a change of its cell.
///
/// Behavior: io.deephaven.engine.table.impl.by.PartitionByChunkedOperator
pub struct Partition {
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
    attributes: TableAttributes,
    /// `None` for a static parent, whose constituents are static.
    graph: Option<Arc<dyn UpdateGraph>>,
    link: Arc<ResultLink>,
    /// The column of each constituent column in the parent.
    parent_columns: Vec<usize>,
    /// Whether each constituent column is a group-by column.
    is_key: Vec<bool>,
    /// The constituent of each slot and its handle.
    tables: Vec<(i64, Arc<QueryTable>)>,
    cells: Arc<TableCells>,
    pending: HashMap<usize, Pending>,
    /// The parent's shifts of the current cycle.
    shifts: RowSetShiftData,
    /// The constituents' modified columns of the current cycle; empty when no column that
    /// a constituent reports was modified.
    modified_columns: ModifiedColumnSet,
}

impl Partition {
    /// The partition of `parent` grouped by its columns `keys` (indexes).
    ///
    /// Behavior: PartitionByChunkedOperator's constructor;
    /// io.deephaven.engine.table.impl.by.AggregationProcessor's `visit(Partition)` for
    /// the columns, which are `dropColumns(keys)` of the parent unless included; and
    /// io.deephaven.engine.table.impl.BaseTable.CopyAttributeOperation#PartitionBy for
    /// the attributes: add-only and append-only are copied (and blink, which is refused
    /// before this is made), also from a static parent that has them, as the result of
    /// `where` or `head` has.
    pub fn new(
        parent: &Arc<dyn Table>,
        keys: &[usize],
        include_keys: bool,
        link: Arc<ResultLink>,
    ) -> Partition {
        let parent_columns: Vec<usize> = (0..parent.definition().columns().len())
            .filter(|index| include_keys || !keys.contains(index))
            .collect();
        let definition = TableDefinition::new(
            parent_columns
                .iter()
                .map(|&index| parent.definition().columns()[index].clone())
                .collect(),
        )
        .expect("a subset of a definition has unique names");
        let refreshing = parent.is_refreshing();
        let mut attributes = TableAttributes::default();
        attributes.add_only = parent.attributes().add_only;
        attributes.append_only = parent.attributes().append_only;
        Partition {
            definition,
            columns: parent_columns
                .iter()
                .map(|&index| parent.column(index))
                .collect(),
            attributes,
            graph: parent.update_graph().filter(|_| refreshing),
            link,
            is_key: parent_columns
                .iter()
                .map(|index| keys.contains(index))
                .collect(),
            parent_columns,
            tables: Vec::new(),
            cells: Arc::default(),
            pending: HashMap::new(),
            shifts: RowSetShiftData::empty(),
            modified_columns: ModifiedColumnSet::empty(),
        }
    }

    /// The columns of the parent whose modification a constituent reports: those it has
    /// that are not group-by columns.
    ///
    /// Behavior: the `retainedResultColumnNames` of PartitionByChunkedOperator's
    /// constructor
    pub fn watched_columns(&self) -> Vec<usize> {
        self.parent_columns
            .iter()
            .zip(&self.is_key)
            .filter(|(_, is_key)| !**is_key)
            .map(|(&index, _)| index)
            .collect()
    }

    fn pending(&mut self, slot: usize) -> &mut Pending {
        self.pending.entry(slot).or_default()
    }

    /// Behavior: PartitionByChunkedOperator#makeSubTable
    fn make(&mut self, rows: RowSet) {
        let spec = TableSpec::new(self.definition.clone(), rows, self.columns.clone())
            .with_attributes(self.attributes);
        let table = match &self.graph {
            Some(graph) => QueryTable::dependent(graph.clone(), spec, self.link.clone()),
            None => QueryTable::new_static(spec),
        };
        let handle = self.cells.register(table.clone());
        self.tables.push((handle, table));
    }

    /// The parent's shifts that move a row of `rows`, the rows of a constituent before
    /// the cycle without those removed.
    ///
    /// Behavior: PartitionByChunkedOperator#appendShifts, which builds the same from the
    /// shifted rows one by one
    fn shifts_of(&self, rows: &RowSet) -> Result<RowSetShiftData, TableFailure> {
        let ranges = self
            .shifts
            .ranges()
            .iter()
            .filter(|range| {
                !rows
                    .sub_set_by_key_range(range.first, range.last)
                    .is_empty()
            })
            .copied()
            .collect();
        RowSetShiftData::new(ranges).map_err(|error| TableFailure {
            message: error.to_string(),
        })
    }

    /// Behavior: PartitionByChunkedOperator#propagateResurrectedDestinations,
    /// #propagateUpdatesToRemovedDestinations and #propagateUpdatesToModifiedDestinations
    fn notify(&self, slot: usize, pending: Pending) -> Result<(), TableFailure> {
        let table = &self.tables[slot].1;
        let removed = RowSet::from_keys(pending.removed);
        let shifted = if pending.shifted {
            self.shifts_of(&table.row_set().minus(&removed))?
        } else {
            RowSetShiftData::empty()
        };
        let modified = if self.modified_columns.is_empty() {
            RowSet::empty()
        } else {
            RowSet::from_keys(pending.modified)
        };
        let update = TableUpdate {
            added: RowSet::from_keys(pending.added),
            removed,
            modified_columns: if modified.is_empty() {
                ModifiedColumnSet::empty()
            } else {
                self.modified_columns.clone()
            },
            modified,
            shifted,
        };
        if !update.is_empty() && !table.is_failed() {
            table.notify(update);
        }
        Ok(())
    }
}

impl AggOperator for Partition {
    /// Behavior: PartitionByChunkedOperator#getResultColumns, an `ObjectArraySource` of
    /// `QueryTable`
    fn result_type(&self) -> ColumnType {
        ColumnType::Table(TableClass::QueryTable)
    }

    /// Behavior: PartitionByChunkedOperator#requiresRowKeys
    fn needs_group_rows(&self) -> bool {
        true
    }

    fn ensure_slots(&mut self, _slots: usize) {}

    fn begin_cycle(&mut self, upstream: &TableUpdate) {
        self.pending.clear();
        self.shifts = upstream.shifted.clone();
        // Behavior: PartitionByChunkedOperator#resetForStep
        self.modified_columns = if upstream.modified.is_empty() {
            ModifiedColumnSet::empty()
        } else {
            ModifiedColumnSet::of((0..self.parent_columns.len()).filter(|&column| {
                !self.is_key[column]
                    && upstream
                        .modified_columns
                        .contains(self.parent_columns[column])
            }))
        };
    }

    /// Behavior: PartitionByChunkedOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.pending(slot).added.extend(run.row_keys());
        Effect::RECOMPUTE
    }

    /// Behavior: PartitionByChunkedOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.pending(slot).removed.extend(run.row_keys());
        Effect::RECOMPUTE
    }

    /// Behavior: PartitionByChunkedOperator#modifyRowKeys
    fn modify(&mut self, slot: usize, _old: &Run<'_>, new: &Run<'_>) -> Effect {
        self.pending(slot).modified.extend(new.row_keys());
        Effect::RECOMPUTE
    }

    /// Behavior: PartitionByChunkedOperator#shiftChunk
    fn shifted(&mut self, slot: usize, _before: &RowSet, _after: &RowSet) {
        self.pending(slot).shifted = true;
    }

    fn recomputes_shifted(&self) -> bool {
        true
    }

    /// Behavior: PartitionByChunkedOperator#propagateInitialState: a group that is shown
    /// without rows has an empty constituent.
    fn shown_empty(&mut self, _slot: usize) -> Effect {
        Effect::RECOMPUTE
    }

    /// Makes the constituents of the new groups and tells the others what changed. No
    /// cell changes: a group keeps its table.
    ///
    /// Behavior: PartitionByChunkedOperator#propagateInitialState and #propagateUpdates
    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        for &slot in slots {
            let pending = self.pending.remove(&slot).unwrap_or_default();
            if slot < self.tables.len() {
                self.notify(slot, pending)?;
                continue;
            }
            // Slots are handed out in order, so a slot without a table is new, and any
            // before it that were never given rows are empty.
            while self.tables.len() < slot {
                self.make(RowSet::empty());
            }
            self.make(groups.rows(slot).clone());
        }
        Ok(vec![false; slots.len()])
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        let handles: Int64Array = slots
            .iter()
            .map(|&slot| self.tables.get(slot).map(|(handle, _)| *handle))
            .collect();
        Arc::new(handles)
    }

    /// Behavior: an `ObjectArraySource` holds its tables; and
    /// PartitionByChunkedOperator#initializeRefreshing, "This operator never reports
    /// modifications"
    fn names_result(&self) -> bool {
        false
    }

    fn result_column(&self, written: Arc<dyn ColumnSource>) -> Arc<dyn ColumnSource> {
        Arc::new(TableColumn::new(written, self.cells.clone()))
    }

    /// Behavior: PartitionByChunkedOperator#propagateFailure
    fn failed(&mut self, failure: &TableFailure) {
        for (_, table) in &self.tables {
            if table.is_refreshing() && !table.is_failed() {
                table.fail(failure.clone());
            }
        }
    }
}
