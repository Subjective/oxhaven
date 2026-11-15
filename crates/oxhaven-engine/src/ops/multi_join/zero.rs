//! The multi join without key columns: one row, at row key 0, while any table has a row.
//! Every table may have one row at most.

use super::plan::Plan;
use super::{failure, MultiJoinError};
use crate::table::{MergedOperator, QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::{Arc, Mutex, PoisonError};

/// The incremental half of a multi join without key columns.
pub(super) struct ZeroKey {
    plan: Plan,
    tables: Vec<Arc<dyn Table>>,
    /// Per table, its row, or `NULL_ROW_KEY`.
    rows: Mutex<Vec<RowKey>>,
    /// Per table, result row 0 to the table's row, with no entry when it has none.
    redirections: Vec<Arc<WritableRowRedirection>>,
    modified_columns: Vec<(ModifiedColumnTransformer, ModifiedColumnSet)>,
}

impl ZeroKey {
    /// Points `table`'s columns at the first row it has now.
    fn follow(&self, rows: &mut [RowKey], table: usize) {
        let first = self.tables[table].row_set().first_row_key();
        rows[table] = first.unwrap_or(NULL_ROW_KEY);
        match first {
            Some(row) => self.redirections[table].put(0, row),
            None => self.redirections[table].remove(0),
        };
    }
}

impl MergedOperator for ZeroKey {
    /// Java keeps the modified columns of earlier cycles in the set it reports, since it
    /// never clears it; here a cycle reports its own.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MultiJoinTableImpl.MultiJoinZeroKeyMergedListener#process
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        let mut rows = self.rows.lock().unwrap_or_else(PoisonError::into_inner);
        let mut columns = ModifiedColumnSet::empty();
        let mut result_modified = false;
        for (table, update) in upstream.iter().enumerate() {
            let Some(update) = update else {
                continue;
            };
            let (transformer, all) = &self.modified_columns[table];
            if !update.removed.is_empty() || !update.added.is_empty() {
                if self.tables[table].row_set().size() > 1 {
                    return Err(failure(MultiJoinError::ZeroKeyRows {
                        table,
                        in_cycle: true,
                    }));
                }
                result_modified = true;
                self.follow(&mut rows, table);
                columns = columns.union(all);
            } else if !update.modified.is_empty() {
                // A change to columns the join does not show is not passed on.
                let added = self.plan.tables[table].added_sources();
                if update.modified_columns.contains_any(&added) {
                    result_modified = true;
                    columns = columns.union(&transformer.transform(&update.modified_columns));
                }
                self.follow(&mut rows, table);
            } else if !update.shifted.is_empty() {
                self.follow(&mut rows, table);
            }
        }

        let has_row = rows.iter().any(|row| *row != NULL_ROW_KEY);
        let had_row = !result.row_set().is_empty();
        let mut downstream = TableUpdate::empty();
        if has_row && !had_row {
            downstream.added = RowSet::flat(1);
        } else if !has_row && had_row {
            downstream.removed = RowSet::flat(1);
        } else if has_row && result_modified {
            downstream.modified = RowSet::flat(1);
            downstream.modified_columns = columns;
        }
        Ok(downstream)
    }
}

/// Reads the tables and returns the result's initial state with the operator.
///
/// Behavior: io.deephaven.engine.table.impl.MultiJoinTableImpl#doMultiJoinZeroKey, which
/// looks for a table's added columns right after it has counted the table's rows
pub(super) fn start(
    plan: Plan,
    tables: &[Arc<dyn Table>],
) -> Result<(TableSpec, ZeroKey), MultiJoinError> {
    let redirections: Vec<Arc<WritableRowRedirection>> = tables
        .iter()
        .map(|_| Arc::new(WritableRowRedirection::new()))
        .collect();
    let mut rows = vec![NULL_ROW_KEY; tables.len()];
    let mut columns: Vec<Arc<dyn ColumnSource>> = Vec::new();
    for (index, table) in tables.iter().enumerate() {
        let now = table.row_set();
        if now.size() > 1 {
            return Err(MultiJoinError::ZeroKeyRows {
                table: index,
                in_cycle: false,
            });
        }
        if let Some(row) = now.first_row_key() {
            rows[index] = row;
            redirections[index].put(0, row);
        }
        if let Some(missing) = &plan.tables[index].missing {
            return Err(missing.clone());
        }
        let redirection: Arc<dyn RowRedirection> = redirections[index].clone();
        for (_, source) in &plan.tables[index].additions {
            columns.push(Arc::new(RedirectedSource::new(
                redirection.clone(),
                table.column(*source),
            )));
        }
    }
    let graph = tables
        .iter()
        .filter(|table| table.is_refreshing())
        .find_map(|table| table.update_graph());
    if let Some(graph) = graph {
        for redirection in &redirections {
            let clock: Arc<dyn Clock> = Arc::new(graph.clone());
            redirection.start_tracking_prev(clock);
        }
    }

    let has_row = rows.iter().any(|row| *row != NULL_ROW_KEY);
    let spec = TableSpec::new(
        plan.definition(tables),
        RowSet::flat(u64::from(has_row)),
        columns,
    );
    let modified_columns = plan.modified_columns();
    let join = ZeroKey {
        plan,
        tables: tables.to_vec(),
        rows: Mutex::new(rows),
        redirections,
        modified_columns,
    };
    Ok((spec, join))
}
