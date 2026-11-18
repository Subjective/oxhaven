//! `select_distinct`, and `where_in` and `where_not_in` ([`where_in`], in `where_in.rs`),
//! which Java builds on it. Task 4.8.
//!
//! In Java `select_distinct` is an aggregation without aggregations: one row per group of
//! the key columns, with the key columns alone. The rules of an aggregation's result
//! therefore hold (PARITY.md, "Order guarantees per operation"):
//!
//! - A group's row key is its number in the order in which the keys were first met, and it
//!   never changes. The result's rows are the groups that have rows, so the result is in
//!   the order of first appearance.
//! - While the source ticks, a new key is appended, a group that loses its last row leaves
//!   the result, and a group that gets rows again comes back at its old row key, not at
//!   the end.
//! - The key column shows the value of the row that created the group. That matters for
//!   `-0.0` and `0.0`, which are one key: a group made by `-0.0` shows `-0.0` for good,
//!   even after that row has gone and a row with `0.0` has brought the group back.
//!
//! This module does not use `ops::agg`. The key encoding ([`keys`]) and the bookkeeping of
//! groups ([`Groups`]) are what a keyed aggregation needs too.

mod keys;
mod where_in;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod where_in_tests;

pub use where_in::{where_in, where_not_in, WhereInError};

use crate::ops::agg::slot_source::ResultColumn;
use crate::ops::key_table::KeyTable as GroupKeys;
use crate::table::{memoized, CopyType, QueryTable, TableSpec};
use crate::update::modified_pre_shift;
use arrow::array::{ArrayRef, UInt64Array};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

/// Why a `select_distinct` was refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DistinctError {
    /// Columns the table does not have, in the order they were asked for. The Java server
    /// answers `FAILED_PRECONDITION` with "column(s) not found".
    ///
    /// Behavior: io.deephaven.server.table.ops.SelectDistinctGrpcImpl#create
    NoSuchColumn(Vec<String>),
    /// The table has failed, or reading it failed.
    Table(TableFailure),
}

impl fmt::Display for DistinctError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DistinctError::NoSuchColumn(names) => {
                write!(f, "column(s) not found: {}", names.join(", "))
            }
            DistinctError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for DistinctError {}

impl From<TableFailure> for DistinctError {
    fn from(failure: TableFailure) -> Self {
        DistinctError::Table(failure)
    }
}

/// Rows read at a time.
const CHUNK_SIZE: usize = 4096;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The distinct combinations of values of `columns` in `source`, in the order in which
/// they first appear, as a table of those columns. A column named twice counts once, at
/// its first place.
///
/// Without columns the result has no columns, and one row while the source has rows.
///
/// A static source gives a static result. The result is flat if the source is static,
/// add-only, append-only or a blink table, and it has the add-only and the append-only
/// attribute if the source has them, which for these attributes includes every static
/// source. Of a blink table the result keeps every key it has ever seen.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#selectDistinct, which is
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation with
/// io.deephaven.engine.table.impl.by.AggregationProcessor#forSelectDistinct
pub fn select_distinct(
    source: &Arc<dyn Table>,
    columns: &[String],
) -> Result<Arc<dyn Table>, DistinctError> {
    let declared = source.definition();
    let missing: Vec<String> = columns
        .iter()
        .filter(|name| declared.index_of(name).is_none())
        .cloned()
        .collect();
    if !missing.is_empty() {
        return Err(DistinctError::NoSuchColumn(missing));
    }
    let mut indices: Vec<usize> = Vec::with_capacity(columns.len());
    for name in columns {
        let index = declared.index_of(name).expect("checked above");
        if !indices.contains(&index) {
            indices.push(index);
        }
    }
    let result = if indices.is_empty() {
        without_keys(source)?
    } else {
        with_keys(source, indices)?
    };
    // Behavior: QueryTable#selectDistinct memoizes its result under the key of an
    // aggregation
    Ok(memoized(source, CopyType::None, true, result)?)
}

/// The add-only and append-only attributes of the result: those of the source as Java
/// reads them, where a static table is both.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#isAddOnly and #isAppendOnly, as
/// io.deephaven.engine.table.impl.QueryTable#selectDistinct copies them
fn attributes(source: &dyn Table) -> TableAttributes {
    let of_source = source.attributes();
    let fixed = !source.is_refreshing();
    let mut attributes = TableAttributes::default();
    attributes.add_only = fixed || of_source.add_only || of_source.append_only;
    attributes.append_only =
        fixed || of_source.append_only || (of_source.add_only && source.is_flat());
    attributes
}

/// Java asserts that a blink table neither modifies nor shifts rows, and an aggregation of
/// it ignores the rows it removes.
///
/// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#adjustForBlinkTable
fn check_blink(update: &TableUpdate) -> Result<(), TableFailure> {
    if update.modified.is_empty() && update.shifted.is_empty() {
        Ok(())
    } else {
        Err(failure("a blink table modified or shifted rows"))
    }
}

/// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#noKeyAggregation
/// without `preserveEmpty`
fn without_keys(source: &Arc<dyn Table>) -> Result<Arc<QueryTable>, TableFailure> {
    let definition = TableDefinition::new(Vec::new()).expect("no column has no duplicate");
    let attributes = attributes(&**source);
    let blink = source.attributes().blink;
    let parent = source.clone();
    QueryTable::derive(source, move || {
        let size = u64::from(!parent.row_set().is_empty());
        let spec = TableSpec::new(definition, RowSet::flat(size), Vec::new())
            .with_attributes(attributes)
            .flat();
        let operator = move |result: &QueryTable, update: &TableUpdate| {
            if blink {
                check_blink(update)?;
            }
            let shown = !result.row_set().is_empty();
            // The row of a blink table's result stays once it is there.
            let shows = (blink && shown) || !parent.row_set().is_empty();
            let mut downstream = TableUpdate::empty();
            match (shown, shows) {
                (false, true) => downstream.added = RowSet::flat(1),
                (true, false) => downstream.removed = RowSet::flat(1),
                _ => {}
            }
            Ok(downstream)
        };
        Ok((spec, operator))
    })
}

/// The groups met so far.
struct State {
    /// The number of each key's group, which is its row key in the result. A group keeps
    /// its number when it has lost its rows.
    keys: GroupKeys,
    /// How many rows of the source each group has.
    rows: Vec<u64>,
    /// The groups whose number of rows changed in this cycle, with the number they had
    /// before, in the order first touched; `seen` marks them.
    touched: Vec<(usize, u64)>,
    seen: Vec<bool>,
}

impl State {
    fn touch(&mut self, group: usize) {
        if !self.seen[group] {
            self.seen[group] = true;
            self.touched.push((group, self.rows[group]));
        }
    }

    /// Takes a row out of each group of `groups`.
    fn leave(&mut self, groups: &[usize]) -> Result<(), TableFailure> {
        for &group in groups {
            if self.rows[group] == 0 {
                return Err(no_rows());
            }
            self.touch(group);
            self.rows[group] -= 1;
        }
        Ok(())
    }

    /// Makes room for the groups made since the last call.
    fn grow(&mut self) {
        let groups = self.keys.len();
        self.rows.resize(groups, 0);
        self.seen.resize(groups, false);
    }

    /// Puts a row into `group`, recording the change when `track` is set.
    fn enter(&mut self, group: usize, track: bool) {
        if track {
            self.touch(group);
        }
        self.rows[group] += 1;
    }
}

fn no_rows() -> TableFailure {
    failure("a row left the source whose key has no rows in the select_distinct")
}

/// A `select_distinct` by key columns: the groups and the key values of the result.
struct Groups {
    source: Arc<dyn Table>,
    /// The key columns in the source.
    indices: Vec<usize>,
    blink: bool,
    /// The key columns of the result, written at a group's number when the group is made.
    columns: Vec<ResultColumn>,
    state: Mutex<State>,
}

impl Groups {
    /// The values of the key columns at `rows`, now or as of the previous cycle.
    fn values(&self, rows: &RowSet, previous: bool) -> Result<Vec<ArrayRef>, TableFailure> {
        let values = self.indices.iter().map(|index| {
            let column = self.source.column(*index);
            if previous {
                column.prev_chunk(rows)
            } else {
                column.chunk(rows)
            }
        });
        values.collect()
    }

    /// The groups of the keys at `rows` as of the previous cycle; an error if one has
    /// never been seen.
    fn groups_before(&self, state: &mut State, rows: &RowSet) -> Result<Vec<usize>, TableFailure> {
        let mut groups = Vec::with_capacity(rows.size() as usize);
        if state
            .keys
            .find_all(&self.values(rows, true)?, &mut groups)?
        {
            Ok(groups)
        } else {
            Err(no_rows())
        }
    }

    /// Writes the key values of the groups made from the rows at `made`, which are
    /// positions in `values`, in the order the groups were made. `first` is the number of
    /// the first of them.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#copyKeyColumns,
    /// which the listener calls for the new groups alone
    fn write_keys(
        &self,
        first: usize,
        made: &[usize],
        values: &[ArrayRef],
    ) -> Result<(), TableFailure> {
        if made.is_empty() {
            return Ok(());
        }
        let at: Vec<RowKey> = (first..first + made.len()).map(|g| g as RowKey).collect();
        let made: UInt64Array = made.iter().map(|&p| p as u64).collect();
        for (column, values) in self.columns.iter().zip(values) {
            let values = arrow::compute::take(values.as_ref(), &made, None)
                .map_err(|e| failure(e.to_string()))?;
            column.fill_by_keys(&at, &values)?;
        }
        Ok(())
    }

    /// Puts the source's `rows` into their groups, in row order, a chunk at a time. With
    /// `track`, the groups whose number of rows changed are recorded.
    fn enter(&self, state: &mut State, rows: &RowSet, track: bool) -> Result<(), TableFailure> {
        let mut groups = Vec::new();
        let mut made = Vec::new();
        for start in (0..rows.size()).step_by(CHUNK_SIZE) {
            let chunk = rows.sub_set_by_position_range(start, start + CHUNK_SIZE as u64);
            let values = self.values(&chunk, false)?;
            let first = state.keys.len();
            groups.clear();
            made.clear();
            state.keys.find_or_create(&values, &mut groups, &mut made)?;
            state.grow();
            for &group in &groups {
                state.enter(group, track);
            }
            self.write_keys(first, &made, &values)?;
        }
        Ok(())
    }

    /// Brings the groups up to the source's `update` and returns the result's change.
    ///
    /// The rows removed leave their groups first. Then every modified row whose key
    /// changed moves to the group of its new key, in row order, and then the added rows
    /// enter theirs, in row order: a key that a modified row brings is met before a key
    /// that an added row brings. Shifts move no row to another group.
    ///
    /// The result adds the groups that had no rows and have some now, which the new groups
    /// are part of, and removes the groups that had rows and have none. Java reports no
    /// group as modified, because the only operator, the count of a group's rows, is not
    /// shown.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper.KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys
    /// and #splitKeyModificationsAndDoKeyChangeRemoves
    fn update(&self, update: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let state = &mut *state;
        let result = self.apply(state, update);
        // The record of this cycle goes, whatever happened.
        let touched = std::mem::take(&mut state.touched);
        for &(group, _) in &touched {
            state.seen[group] = false;
        }
        result?;
        let changed = |appears: bool| {
            let mut groups: Vec<RowKey> = touched
                .iter()
                .filter(|(group, before)| {
                    let now = state.rows[*group];
                    if appears {
                        *before == 0 && now > 0
                    } else {
                        *before > 0 && now == 0
                    }
                })
                .map(|(group, _)| *group as RowKey)
                .collect();
            groups.sort_unstable();
            RowSet::from_keys(groups)
        };
        Ok(TableUpdate {
            added: changed(true),
            removed: changed(false),
            ..TableUpdate::empty()
        })
    }

    fn apply(&self, state: &mut State, update: &TableUpdate) -> Result<(), TableFailure> {
        if self.blink {
            check_blink(update)?;
        } else if !update.removed.is_empty() {
            let groups = self.groups_before(state, &update.removed)?;
            state.leave(&groups)?;
        }
        let keys_modified = update.modified_columns.contains_any(&self.indices);
        if keys_modified && !update.modified.is_empty() {
            let before = modified_pre_shift(update).map_err(|e| failure(e.to_string()))?;
            let old = self.groups_before(state, &before)?;
            let values = self.values(&update.modified, false)?;
            let first = state.keys.len();
            let mut new = Vec::with_capacity(old.len());
            let mut made = Vec::new();
            state.keys.find_or_create(&values, &mut new, &mut made)?;
            state.grow();
            for (&old, &new) in old.iter().zip(&new) {
                if old != new {
                    state.leave(&[old])?;
                    state.enter(new, true);
                }
            }
            self.write_keys(first, &made, &values)?;
        }
        if !update.added.is_empty() {
            self.enter(state, &update.added, true)?;
        }
        Ok(())
    }
}

/// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation
fn with_keys(
    source: &Arc<dyn Table>,
    indices: Vec<usize>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let declared = source.definition().columns();
    let columns: Vec<ColumnDefinition> = indices
        .iter()
        .map(|index| declared[*index].clone())
        .collect();
    let graph = source.update_graph().filter(|_| source.is_refreshing());
    let of_source = source.attributes();
    let key_types: Vec<ColumnType> = columns
        .iter()
        .map(|column| column.column_type.clone())
        .collect();
    let data_types: Vec<_> = key_types.iter().map(ColumnType::arrow_type).collect();
    let groups = Groups {
        source: source.clone(),
        columns: columns
            .iter()
            .map(|column| ResultColumn::new(column.column_type.clone(), graph.as_ref()))
            .collect(),
        indices,
        blink: of_source.blink,
        state: Mutex::new(State {
            keys: GroupKeys::for_types(&data_types)?,
            rows: Vec::new(),
            touched: Vec::new(),
            seen: Vec::new(),
        }),
    };
    let definition = TableDefinition::new(columns).expect("each key column is taken once");
    let attributes = attributes(&**source);
    // Without removals the groups with rows are the groups, `0..size`.
    let flat = graph.is_none() || of_source.add_only || of_source.append_only || of_source.blink;
    QueryTable::derive(source, move || {
        for key_type in &key_types {
            keys::check(key_type)?;
        }
        let size = {
            let mut state = groups.state.lock().unwrap_or_else(PoisonError::into_inner);
            groups.enter(&mut state, &groups.source.row_set(), false)?;
            state.rows.len() as u64
        };
        // A static result shows the keys where the aggregation keeps them, and a
        // refreshing one has copies of them: see `Aggregator::columns`.
        let refreshing = groups.source.is_refreshing();
        let sources = groups.columns.iter().map(|source| {
            if refreshing {
                source.source()
            } else {
                source.shared()
            }
        });
        let mut spec = TableSpec::new(definition, RowSet::flat(size), sources.collect())
            .with_attributes(attributes);
        if flat {
            spec = spec.flat();
        }
        let operator = move |_result: &QueryTable,
                             update: &TableUpdate|
              -> Result<TableUpdate, TableFailure> { groups.update(update) };
        Ok((spec, operator))
    })
}
