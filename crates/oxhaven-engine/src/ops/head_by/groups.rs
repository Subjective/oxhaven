//! The groups of a `head_by` or `tail_by`: the rows of the source by key, and the first or
//! last rows of each group as the array columns that `ungroup` reads.

use crate::ops::agg::keys::KeyTable;
use crate::ops::ungroup::sources::ArrayColumn;
use crate::update::modified_pre_shift;
use arrow::array::{ArrayRef, UInt64Array};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{ClockPhase, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The key columns of the source and of the groups.
struct Keys {
    table: KeyTable,
    /// The indexes of the key columns in the source.
    columns: Vec<usize>,
    /// The key of slot `s` at row key `s`: the key of the first row seen with it.
    results: Vec<Arc<WritableSource>>,
}

struct State {
    /// `None` when there are no key columns: all rows are in slot 0.
    keys: Option<Keys>,
    /// The source rows of each slot.
    rows: Vec<RowSet>,
    /// The rows that the slots changed in the cycle `saved_step` had before it.
    saved: HashMap<usize, RowSet>,
    saved_step: Option<u64>,
}

impl State {
    /// Keeps what `slot` had at the start of the cycle, before its first change.
    fn save(&mut self, slot: usize) {
        if !self.saved.contains_key(&slot) {
            self.saved.insert(slot, self.rows[slot].clone());
        }
    }
}

/// What one cycle did to the groups.
#[derive(Default)]
struct Cycle {
    /// The slots that gained or lost rows, with whether they had rows before.
    touched: BTreeMap<usize, bool>,
    /// The slots whose groups Java reports as changed.
    changed: BTreeSet<usize>,
}

/// The rows of a table by the key they have in some of its columns.
///
/// A key gets a slot when it is first seen and keeps it for good; the group of slot `s` is
/// row `s` of the grouped table, which has a row for every slot that has rows. So the
/// groups are in the order their keys were first seen, and a group that loses all its rows
/// and gets rows again is back at its old place.
///
/// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper with one
/// io.deephaven.engine.table.impl.by.GroupByChunkedOperator for all the columns that are
/// not keys, which is the `aggBy` of io.deephaven.engine.table.impl.QueryTable#headOrTailBy
pub(super) struct Groups {
    source: Arc<dyn Table>,
    /// The graph of a refreshing source.
    graph: Option<Arc<dyn UpdateGraph>>,
    /// The columns of the source that are not keys.
    value_columns: Vec<usize>,
    /// How many rows of a group are kept.
    rows: u64,
    /// Whether they are its first rows, or else its last.
    head: bool,
    state: Mutex<State>,
}

impl Groups {
    /// The groups of `source` by `key_columns`, which are indexes of its columns, before
    /// any row is read. `key_sources` hold the keys of the groups.
    pub(super) fn new(
        source: Arc<dyn Table>,
        key_columns: Vec<usize>,
        key_sources: Vec<Arc<WritableSource>>,
        rows: u64,
        head: bool,
    ) -> Result<Groups, TableFailure> {
        let declared = source.definition().columns();
        let value_columns = (0..declared.len())
            .filter(|column| !key_columns.contains(column))
            .collect();
        let keys = if key_columns.is_empty() {
            None
        } else {
            let key_types: Vec<ColumnType> = key_columns
                .iter()
                .map(|&column| declared[column].column_type.clone())
                .collect();
            Some(Keys {
                table: KeyTable::new(&key_types)?,
                columns: key_columns,
                results: key_sources,
            })
        };
        // Without keys there is one group, which exists before it has rows.
        let slots = if keys.is_none() { 1 } else { 0 };
        let graph = source.update_graph().filter(|_| source.is_refreshing());
        Ok(Groups {
            source,
            graph,
            value_columns,
            rows,
            head,
            state: Mutex::new(State {
                keys,
                rows: vec![RowSet::empty(); slots],
                saved: HashMap::new(),
                saved_step: None,
            }),
        })
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The slot of each of `rows`, by their previous or current keys. With `create`, a key
    /// not seen before gets a slot, in the order of the rows, and its key is written.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.OperatorAggregationStateManager#add and
    /// IncrementalOperatorAggregationStateManager#remove / #findModifications
    fn slots_of(
        &self,
        state: &mut State,
        rows: &RowSet,
        previous: bool,
        create: bool,
    ) -> Result<Vec<usize>, TableFailure> {
        let Some(keys) = &mut state.keys else {
            return Ok(vec![0; rows.size() as usize]);
        };
        let arrays = keys
            .columns
            .iter()
            .map(|&column| {
                let source = self.source.column(column);
                if previous {
                    source.prev_chunk(rows)
                } else {
                    source.chunk(rows)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !create {
            return keys.table.find(&arrays);
        }
        let (slots, created) = keys.table.find_or_create(&arrays)?;
        if !created.is_empty() {
            let slot_count = keys.table.len();
            let new_slots: Vec<RowKey> = (slot_count - created.len()..slot_count)
                .map(|slot| slot as RowKey)
                .collect();
            let positions: UInt64Array = created.iter().map(|&p| p as u64).collect();
            for (array, result) in arrays.iter().zip(&keys.results) {
                let first_seen = arrow::compute::take(array, &positions, None)
                    .map_err(|error| failure(error.to_string()))?;
                result.fill_by_keys(&new_slots, &first_seen)?;
            }
            state.rows.resize(slot_count, RowSet::empty());
        }
        Ok(slots)
    }

    /// `rows` of the source by slot, the slots increasing.
    fn by_slot(
        &self,
        state: &mut State,
        rows: &RowSet,
        previous: bool,
        create: bool,
    ) -> Result<BTreeMap<usize, Vec<RowKey>>, TableFailure> {
        let slots = self.slots_of(state, rows, previous, create)?;
        let mut by_slot: BTreeMap<usize, Vec<RowKey>> = BTreeMap::new();
        for (slot, row) in slots.into_iter().zip(rows.iter()) {
            by_slot.entry(slot).or_default().push(row);
        }
        Ok(by_slot)
    }

    /// Takes the rows of `by_slot` out of their groups.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.GroupByChunkedOperator#removeChunk
    fn take_out(
        state: &mut State,
        cycle: &mut Cycle,
        by_slot: BTreeMap<usize, Vec<RowKey>>,
    ) -> Result<(), TableFailure> {
        for (slot, rows) in by_slot {
            let rows = RowSet::from_keys(rows);
            if !rows.is_subset_of(&state.rows[slot]) {
                return Err(failure("head_by: a row left a group that it was not in"));
            }
            state.save(slot);
            let had_rows = !state.rows[slot].is_empty();
            cycle.touched.entry(slot).or_insert(had_rows);
            cycle.changed.insert(slot);
            state.rows[slot].remove(&rows);
        }
        Ok(())
    }

    /// Puts `rows` of the source, with the keys they have now, into their groups.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.GroupByChunkedOperator#addChunk
    fn put_in(
        &self,
        state: &mut State,
        cycle: &mut Cycle,
        rows: &RowSet,
    ) -> Result<(), TableFailure> {
        for (slot, rows) in self.by_slot(state, rows, false, true)? {
            state.save(slot);
            let had_rows = !state.rows[slot].is_empty();
            cycle.touched.entry(slot).or_insert(had_rows);
            cycle.changed.insert(slot);
            state.rows[slot].insert(&RowSet::from_keys(rows));
        }
        Ok(())
    }

    /// Groups the rows the source has now. Returns the rows of the grouped table: the
    /// slots that have rows.
    ///
    /// Behavior: ChunkedOperatorAggregationHelper#initialBucketedKeyAddition and
    /// #doNoKeyAddition
    pub(super) fn initial(&self) -> Result<RowSet, TableFailure> {
        let mut state = self.state();
        let state = &mut *state;
        self.put_in(state, &mut Cycle::default(), &self.source.row_set())?;
        state.saved.clear();
        let slots = state.rows.iter().enumerate();
        let present = slots.filter(|(_, rows)| !rows.is_empty());
        Ok(RowSet::from_keys(present.map(|(slot, _)| slot as RowKey)))
    }

    /// Moves the rows the source shifted, other than `moved_pre`, within their groups. No
    /// group is reported as changed for it: a shift moves no row past another, so the
    /// first and the last rows of a group stay the same rows.
    ///
    /// Behavior: KeyedUpdateContext#doShifts, ChunkedOperatorAggregationHelper#doNoKeyShifts
    /// and GroupByChunkedOperator#shiftChunk without an exposed row set column
    fn shift(
        &self,
        state: &mut State,
        update: &TableUpdate,
        moved_pre: &RowSet,
    ) -> Result<(), TableFailure> {
        if update.shifted.is_empty() {
            return Ok(());
        }
        let mut survivors = self.source.prev_row_set().minus(&update.removed);
        survivors.remove(moved_pre);
        let mut affected = RowSet::empty();
        for shift in update.shifted.ranges() {
            affected.insert(&survivors.sub_set_by_key_range(shift.first, shift.last));
        }
        for (slot, rows) in self.by_slot(state, &affected, true, false)? {
            let before = RowSet::from_keys(rows);
            let after = update.shifted.apply(&before);
            let after = after.map_err(|error| failure(error.to_string()))?;
            state.save(slot);
            state.rows[slot].remove(&before);
            state.rows[slot].insert(&after);
        }
        Ok(())
    }

    /// Brings the groups up to date with one change of the source and returns the change
    /// of the grouped table, whose columns are the keys and then the other columns of the
    /// source.
    ///
    /// The order is Java's: removed rows leave their groups; modified rows whose key
    /// changed leave theirs; shifts are applied; the groups of modified rows that kept
    /// their key are noted, if a column that is not a key was modified; rows whose key
    /// changed join their new groups; added rows join theirs. New keys therefore get
    /// their slots, and so their place in the result, in the order: keys of re-keyed rows,
    /// then keys of added rows.
    ///
    /// A group is added when it gets its first row and removed when it loses its last.
    /// A group that stays is modified if rows joined or left it, or if a row of it was
    /// modified in a column that is not a key, wherever in the group that row is. The
    /// modified columns are all the columns that are not keys if any group gained or lost
    /// a row, and otherwise those of them the source modified.
    ///
    /// Behavior: ChunkedOperatorAggregationHelper.KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys,
    /// the `processNoKeyUpdate` of ChunkedOperatorAggregationHelper#noKeyAggregation, and
    /// GroupByChunkedOperator#resetForStep, #modifyRowKeys and its
    /// `InputToResultModifiedColumnSetFactory`
    pub(super) fn advance(&self, update: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let mut state = self.state();
        let state = &mut *state;
        let step = self.graph.as_ref().map(|graph| graph.clock().step);
        if state.saved_step != step {
            state.saved.clear();
            state.saved_step = step;
        }
        let mut cycle = Cycle::default();

        let removed = self.by_slot(state, &update.removed, true, false)?;
        Self::take_out(state, &mut cycle, removed)?;
        let mut modified_in_place = false;
        if update.modified.is_empty() {
            self.shift(state, update, &RowSet::empty())?;
        } else {
            let pre = modified_pre_shift(update).map_err(|error| failure(error.to_string()))?;
            let key_columns = state.keys.as_ref().map(|keys| keys.columns.as_slice());
            let keys_modified =
                key_columns.is_some_and(|columns| update.modified_columns.contains_any(columns));
            let mut same = update.modified.clone();
            let mut moved_post = RowSet::empty();
            let mut moved_pre = RowSet::empty();
            if keys_modified {
                // Behavior: KeyedUpdateContext#splitKeyModificationsAndDoKeyChangeRemoves
                let old = self.slots_of(state, &pre, true, false)?;
                let new = self.slots_of(state, &update.modified, false, true)?;
                let rows = pre.iter().zip(update.modified.iter());
                let mut leaving: BTreeMap<usize, Vec<RowKey>> = BTreeMap::new();
                let mut arriving = Vec::new();
                for ((then, now), (old, new)) in rows.zip(old.into_iter().zip(new)) {
                    if old != new {
                        leaving.entry(old).or_default().push(then);
                        arriving.push(now);
                    }
                }
                moved_pre = RowSet::from_keys(leaving.values().flatten().copied());
                moved_post = RowSet::from_keys(arriving);
                same.remove(&moved_post);
                Self::take_out(state, &mut cycle, leaving)?;
            }
            self.shift(state, update, &moved_pre)?;
            if update.modified_columns.contains_any(&self.value_columns) {
                let slots = self.slots_of(state, &same, false, false)?;
                modified_in_place = !slots.is_empty();
                cycle.changed.extend(slots);
            }
            self.put_in(state, &mut cycle, &moved_post)?;
        }
        self.put_in(state, &mut cycle, &update.added)?;

        let mut added = Vec::new();
        let mut removed = Vec::new();
        for (&slot, &had_rows) in &cycle.touched {
            let has_rows = !state.rows[slot].is_empty();
            if !had_rows && has_rows {
                added.push(slot as RowKey);
            } else if had_rows && !has_rows {
                removed.push(slot as RowKey);
            }
        }
        let added = RowSet::from_keys(added);
        let modified = cycle.changed.iter().map(|&slot| slot as RowKey);
        let modified = RowSet::from_keys(
            modified.filter(|&slot| !state.rows[slot as usize].is_empty() && !added.contains(slot)),
        );
        // The columns of the grouped table: the keys, then the others in their order.
        let first = self.source.definition().columns().len() - self.value_columns.len();
        let values = self.value_columns.iter().enumerate();
        let modified_columns = if !cycle.touched.is_empty() {
            ModifiedColumnSet::of(values.map(|(place, _)| first + place))
        } else if modified_in_place {
            let values = values.filter(|(_, column)| update.modified_columns.contains(**column));
            ModifiedColumnSet::of(values.map(|(place, _)| first + place))
        } else {
            ModifiedColumnSet::empty()
        };
        Ok(TableUpdate {
            added,
            removed: RowSet::from_keys(removed),
            modified,
            modified_columns,
            ..TableUpdate::empty()
        })
    }

    /// The rows `slot` has, or had at the end of the previous cycle.
    fn rows_of<'a>(&self, state: &'a State, slot: RowKey, previous: bool) -> &'a RowSet {
        let slot = slot as usize;
        let clock = self.graph.as_ref().map(|graph| graph.clock());
        let in_changing_cycle = clock.is_some_and(|clock| {
            clock.phase == ClockPhase::Updating && state.saved_step == Some(clock.step)
        });
        match state.saved.get(&slot) {
            Some(saved) if previous && in_changing_cycle => saved,
            _ => &state.rows[slot],
        }
    }

    /// Where the kept rows of a group of `size` rows start in it, and how many they are.
    ///
    /// Behavior: the formulas of io.deephaven.engine.table.impl.QueryTable#headOrTailBy,
    /// `subVector(0, nRows)` and `subVector(size() - nRows, size())` of a group that has
    /// more than `nRows` rows, and the whole group otherwise
    fn window(&self, size: u64) -> (u64, u64) {
        let kept = size.min(self.rows);
        if self.head {
            (0, kept)
        } else {
            (size - kept, kept)
        }
    }
}

/// A column of the source that is not a key, as the grouped table has it: for each group
/// the values in its first or last rows, in the order of the rows.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregateColumnSource over the row sets of
/// the groups, cut by the `updateView` of
/// io.deephaven.engine.table.impl.QueryTable#headOrTailBy
pub(super) struct Kept {
    pub(super) groups: Arc<Groups>,
    pub(super) column: Arc<dyn ColumnSource>,
}

impl ArrayColumn for Kept {
    fn element_type(&self) -> &ColumnType {
        self.column.column_type()
    }

    fn sizes(&self, rows: &RowSet, previous: bool) -> Result<Vec<u64>, TableFailure> {
        let state = self.groups.state();
        let sizes = rows.iter().map(|slot| {
            let size = self.groups.rows_of(&state, slot, previous).size();
            self.groups.window(size).1
        });
        Ok(sizes.collect())
    }

    fn elements(&self, cells: &[(RowKey, u64)], previous: bool) -> Result<ArrayRef, TableFailure> {
        let state = self.groups.state();
        let rows = cells.iter().map(|&(slot, offset)| {
            let rows = self.groups.rows_of(&state, slot, previous);
            let (start, kept) = self.groups.window(rows.size());
            match rows.get(start + offset) {
                Some(row) if offset < kept => row,
                _ => NULL_ROW_KEY,
            }
        });
        let rows: Vec<RowKey> = rows.collect();
        self.column.chunk_by_keys(&rows, previous)
    }
}
