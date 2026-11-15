//! The multi join on key columns.
//!
//! Every key that a table has ever had owns a row of the result for good: the first key at
//! row key 0, the next at 1, and so on. The result's row set holds the rows whose key some
//! table has now. The added columns of a table read that table through a row redirection
//! from result row to table row; the key columns are stored.
//!
//! Java has a hash table per combination of key types, one kind for static tables and one
//! for refreshing ones. This is one ordered map from key to row for both.

use super::plan::{as_key_type, Plan};
use super::{failure, MultiJoinError};
use crate::ops::join::keys::{key_text, Key};
use crate::table::{tracked_source, MergedOperator, QueryTable, TableSpec};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use oxhaven_column::{
    Clock, RedirectedSource, RowRedirection, WritableRowRedirection, WritableSource,
};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

/// What a table did to a row of the result in a cycle.
///
/// Behavior: the flags of io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker
const ADD: u8 = 1;
const REMOVE: u8 = 2;
const MODIFY: u8 = 4;
const SHIFT: u8 = 8;

/// What a cycle did to one row of the result, per table.
///
/// Behavior: an entry of io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker
struct Touch {
    /// The table's row for this result row before the cycle (`NULL_ROW_KEY` for none), or
    /// `None` if the cycle did not change it. A table that only modified its row in place
    /// leaves `None`.
    before: Vec<Option<RowKey>>,
    flags: Vec<u8>,
}

/// The result rows a cycle touched.
struct Touched {
    tables: usize,
    rows: BTreeMap<usize, Touch>,
}

impl Touched {
    fn entry(&mut self, row: usize) -> &mut Touch {
        let tables = self.tables;
        self.rows.entry(row).or_insert_with(|| Touch {
            before: vec![None; tables],
            flags: vec![0; tables],
        })
    }

    /// Notes that `table` changed which of its rows `row` shows; `before` is the one it
    /// showed until now.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker#addSlot
    fn redirected(&mut self, row: usize, table: usize, before: RowKey, flag: u8) {
        let touch = self.entry(row);
        touch.flags[table] |= flag;
        touch.before[table].get_or_insert(before);
    }

    /// Notes that `table` changed the values of the row that `row` shows.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker#modifySlot
    fn modified(&mut self, row: usize, table: usize) {
        self.entry(row).flags[table] |= MODIFY;
    }
}

struct State {
    /// The result row of every key a table has ever had.
    slots: BTreeMap<Key, usize>,
    /// Per table, the table's row for each result row, or `NULL_ROW_KEY`.
    rows: Vec<Vec<RowKey>>,
}

impl State {
    fn size(&self) -> usize {
        self.rows[0].len()
    }
}

/// The incremental half of a multi join on key columns, and its state.
pub(super) struct Keyed {
    plan: Plan,
    tables: Vec<Arc<dyn Table>>,
    state: Mutex<State>,
    /// Per table, result row to table row. A result row without a row of the table has no
    /// entry.
    redirections: Vec<Arc<WritableRowRedirection>>,
    key_columns: Vec<Arc<WritableSource>>,
    /// Per table, how its modified columns show in the result, and all the columns it adds.
    modified_columns: Vec<(ModifiedColumnTransformer, ModifiedColumnSet)>,
}

impl Keyed {
    fn set(&self, state: &mut State, table: usize, row: usize, inner: RowKey) {
        state.rows[table][row] = inner;
        if inner == NULL_ROW_KEY {
            self.redirections[table].remove(row as RowKey);
        } else {
            self.redirections[table].put(row as RowKey, inner);
        }
    }

    fn key_text(&self, key: &Key) -> String {
        key_text(key, &self.plan.key_types())
    }

    /// The result row of the key of a row that `table` had before this cycle.
    ///
    /// Behavior: the `IllegalStateException` "Matching row not found" of the `remove`,
    /// `shift` and `modify` of the hashers under
    /// io.deephaven.engine.table.impl.multijoin.typed.incopen
    fn slot_of(&self, state: &State, table: usize, key: &Key) -> Result<usize, TableFailure> {
        state.slots.get(key).copied().ok_or_else(|| {
            failure(format!(
                "Matching row not found for {} in table {table}.",
                self.key_text(key)
            ))
        })
    }

    /// Gives each of `rows`, which `table` has now, the result row of its key, and a new
    /// result row to a key no table has had. `touched` is absent for the initial rows.
    ///
    /// Behavior: `buildFromTable` of the hashers under
    /// io.deephaven.engine.table.impl.multijoin.typed.staticopen and `.incopen`. A row
    /// added to a result row that this cycle made is noted like any other; `on_update`
    /// leaves those rows out.
    fn add_rows(
        &self,
        state: &mut State,
        table: usize,
        rows: &RowSet,
        mut touched: Option<(&mut Touched, u8)>,
    ) -> Result<(), MultiJoinError> {
        if rows.is_empty() {
            return Ok(());
        }
        let plan = &self.plan.tables[table];
        if let Some(unreadable) = &plan.unreadable {
            return Err(unreadable.clone());
        }
        let source = &*self.tables[table];
        let keys = plan.read_keys(source, rows, false)?;
        let mut fresh_rows: Vec<RowKey> = Vec::new();
        let mut fresh_keys: Vec<RowKey> = Vec::new();
        for (inner, key) in rows.iter().zip(keys) {
            if let Some(&row) = state.slots.get(&key) {
                if state.rows[table][row] != NULL_ROW_KEY {
                    return Err(MultiJoinError::DuplicateKey {
                        key: self.key_text(&key),
                        table,
                    });
                }
                self.set(state, table, row, inner);
                if let Some((touched, flag)) = &mut touched {
                    touched.redirected(row, table, NULL_ROW_KEY, *flag);
                }
            } else {
                let row = state.size();
                for of_table in &mut state.rows {
                    of_table.push(NULL_ROW_KEY);
                }
                state.slots.insert(key, row);
                self.set(state, table, row, inner);
                fresh_rows.push(row as RowKey);
                fresh_keys.push(inner);
            }
        }
        // A new key shows the values of the row that brought it.
        if !fresh_rows.is_empty() {
            for (position, column) in self.key_columns.iter().enumerate() {
                let from = source.column(plan.keys.columns[position]);
                let values = from.chunk_by_keys(&fresh_keys, false)?;
                let values = as_key_type(values, from.column_type(), column.column_type())?;
                column.fill_by_keys(&fresh_rows, &values)?;
            }
        }
        Ok(())
    }

    /// Takes each of `rows`, which `table` had before this cycle, from the result row of
    /// its key.
    ///
    /// Behavior: `remove` of the hashers under
    /// io.deephaven.engine.table.impl.multijoin.typed.incopen
    fn remove_rows(
        &self,
        state: &mut State,
        table: usize,
        rows: &RowSet,
        touched: &mut Touched,
        flag: u8,
    ) -> Result<(), TableFailure> {
        let keys = self.plan.tables[table].read_keys(&*self.tables[table], rows, true)?;
        for (inner, key) in rows.iter().zip(keys) {
            let row = self.slot_of(state, table, &key)?;
            let before = state.rows[table][row];
            if before != inner {
                return Err(failure(format!(
                    "the row of {} in table {table} is {before}, not {inner}",
                    self.key_text(&key)
                )));
            }
            self.set(state, table, row, NULL_ROW_KEY);
            touched.redirected(row, table, before, flag);
        }
        Ok(())
    }

    /// Applies the change of one table to the result rows.
    ///
    /// Behavior: the body of the loop over the recorders in
    /// io.deephaven.engine.table.impl.MultiJoinTableImpl.MultiJoinMergedListener#process:
    /// removes, the old keys of modified rows if a key column changed, shifts, the rows
    /// modified in place, the new keys of modified rows, adds.
    fn apply(
        &self,
        state: &mut State,
        table: usize,
        update: &TableUpdate,
        touched: &mut Touched,
    ) -> Result<(), TableFailure> {
        let plan = &self.plan.tables[table];
        let source = &*self.tables[table];
        let keys_modified =
            !update.modified.is_empty() && update.modified_columns.contains_any(&plan.key_sources);

        self.remove_rows(state, table, &update.removed, touched, REMOVE)?;
        let mut leaving = update.removed.clone();
        if keys_modified {
            let before = modified_pre_shift(update).map_err(failure)?;
            self.remove_rows(state, table, &before, touched, MODIFY)?;
            leaving.insert(&before);
        }

        if !update.shifted.is_empty() {
            let staying = source.prev_row_set().minus(&leaving);
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                let keys = plan.read_keys(source, &rows, true)?;
                for (inner, key) in rows.iter().zip(keys) {
                    let row = self.slot_of(state, table, &key)?;
                    self.set(state, table, row, inner + range.delta);
                    touched.redirected(row, table, inner, SHIFT);
                }
            }
        }

        if keys_modified {
            self.add_rows(state, table, &update.modified, Some((touched, MODIFY)))
                .map_err(failure)?;
        } else if !update.modified.is_empty()
            && update.modified_columns.contains_any(&plan.added_sources())
        {
            // A change to columns the join does not show is not passed on.
            let keys = plan.read_keys(source, &update.modified, false)?;
            for key in keys {
                let row = self.slot_of(state, table, &key)?;
                touched.modified(row, table);
            }
        }
        self.add_rows(state, table, &update.added, Some((touched, ADD)))
            .map_err(failure)
    }
}

impl MergedOperator for Keyed {
    /// Behavior: io.deephaven.engine.table.impl.MultiJoinTableImpl.MultiJoinMergedListener#process
    fn on_update(
        &self,
        _result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let tables = self.tables.len();
        let original_size = state.size();
        let mut touched = Touched {
            tables,
            rows: BTreeMap::new(),
        };
        for (table, update) in upstream.iter().enumerate() {
            if let Some(update) = update {
                self.apply(&mut state, table, update, &mut touched)?;
            }
        }

        let mut added: Vec<RowKey> = (original_size..state.size())
            .map(|row| row as RowKey)
            .collect();
        let mut removed: Vec<RowKey> = Vec::new();
        let mut modified: Vec<RowKey> = Vec::new();
        // The tables that show another row, or no row, in a modified row of the result.
        let mut redirected = vec![false; tables];
        for (&row, touch) in &touched.rows {
            // A row this cycle made is added, whatever else happened to it.
            if row >= original_size {
                continue;
            }
            let mut all_null = true;
            let mut row_modified = false;
            let mut nulls_before = 0;
            let mut redirected_here = vec![false; tables];
            for table in 0..tables {
                let now = state.rows[table][row];
                let flags = touch.flags[table];
                if now != NULL_ROW_KEY {
                    all_null = false;
                }
                match touch.before[table] {
                    Some(before) => {
                        if before == NULL_ROW_KEY {
                            nulls_before += 1;
                        }
                        // Anything but a shift that leaves another row here, or none,
                        // changes every column of the table.
                        if flags & (ADD | REMOVE) != 0 {
                            redirected_here[table] = true;
                            row_modified = true;
                        } else if now != before {
                            redirected_here[table] = flags & MODIFY != 0;
                            row_modified |= redirected_here[table];
                        } else {
                            row_modified |= flags & MODIFY != 0;
                        }
                    }
                    // The table has no row here and never changed that.
                    None if now == NULL_ROW_KEY => nulls_before += 1,
                    // The table's row was modified where it is.
                    None => row_modified |= flags & (ADD | REMOVE | MODIFY) != 0,
                }
            }
            if all_null {
                removed.push(row as RowKey);
            } else if nulls_before == tables {
                added.push(row as RowKey);
            } else if row_modified {
                modified.push(row as RowKey);
                for (any, here) in redirected.iter_mut().zip(redirected_here) {
                    *any |= here;
                }
            }
        }

        let mut downstream = TableUpdate::empty();
        downstream.added = RowSet::from_keys(added);
        downstream.removed = RowSet::from_keys(removed);
        downstream.modified = RowSet::from_keys(modified);
        if !downstream.modified.is_empty() {
            let mut columns = ModifiedColumnSet::empty();
            for (table, update) in upstream.iter().enumerate() {
                // A static table modifies nothing.
                if !self.tables[table].is_refreshing() {
                    continue;
                }
                let (transformer, all) = &self.modified_columns[table];
                if redirected[table] {
                    columns = columns.union(all);
                } else if let Some(update) = update.as_ref().filter(|u| !u.modified.is_empty()) {
                    columns = columns.union(&transformer.transform(&update.modified_columns));
                }
            }
            downstream.modified_columns = columns;
        }
        Ok(downstream)
    }
}

/// Reads the tables and returns the result's initial state with the operator.
///
/// Behavior: io.deephaven.engine.table.impl.MultiJoinTableImpl#bucketedMultiJoin
pub(super) fn start(
    plan: Plan,
    tables: &[Arc<dyn Table>],
) -> Result<(TableSpec, Keyed), MultiJoinError> {
    let graph = tables
        .iter()
        .filter(|table| table.is_refreshing())
        .find_map(|table| table.update_graph());
    let key_columns: Vec<Arc<WritableSource>> = plan
        .keys
        .iter()
        .map(|(_, column_type)| match &graph {
            Some(graph) => tracked_source(graph, column_type.clone()),
            None => Arc::new(WritableSource::new(column_type.clone())),
        })
        .collect();
    let redirections: Vec<Arc<WritableRowRedirection>> = tables
        .iter()
        .map(|_| Arc::new(WritableRowRedirection::new()))
        .collect();
    let modified_columns = plan.modified_columns();
    let join = Keyed {
        plan,
        tables: tables.to_vec(),
        state: Mutex::new(State {
            slots: BTreeMap::new(),
            rows: vec![Vec::new(); tables.len()],
        }),
        redirections,
        key_columns,
        modified_columns,
    };

    let size = {
        let mut state = join.state.lock().unwrap_or_else(PoisonError::into_inner);
        for (index, table) in tables.iter().enumerate() {
            join.add_rows(&mut state, index, &table.row_set(), None)?;
        }
        state.size()
    };
    // Java looks for the added columns after it has read every table's keys.
    if let Some(missing) = join.plan.missing() {
        return Err(missing);
    }
    if let Some(graph) = graph {
        for redirection in &join.redirections {
            let clock: Arc<dyn Clock> = Arc::new(graph.clone());
            redirection.start_tracking_prev(clock);
        }
    }

    let mut columns: Vec<Arc<dyn ColumnSource>> = Vec::new();
    for column in &join.key_columns {
        columns.push(column.clone());
    }
    for (index, table) in tables.iter().enumerate() {
        let redirection: Arc<dyn RowRedirection> = join.redirections[index].clone();
        for (_, source) in &join.plan.tables[index].additions {
            columns.push(Arc::new(RedirectedSource::new(
                redirection.clone(),
                table.column(*source),
            )));
        }
    }
    let spec = TableSpec::new(
        join.plan.definition(tables),
        RowSet::flat(size as u64),
        columns,
    );
    Ok((spec, join))
}
