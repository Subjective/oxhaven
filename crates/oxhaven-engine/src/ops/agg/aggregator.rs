//! The aggregation framework: rows to slots, slots to operators, operators to result rows.

use super::keys::KeyTable;
use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{failure, Run, Values};
use crate::table::tracked_source;
use crate::update::modified_pre_shift;
use arrow::array::{ArrayRef, UInt64Array};
use oxhaven_column::WritableSource;
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::ops::Range;
use std::sync::Arc;

/// Rows processed at a time. Operators are called once per group per chunk, and for a
/// `long` sum that passes over the null sentinel the result depends on where the chunks
/// end, so this must be Java's size.
///
/// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#CHUNK_SIZE
const CHUNK_SIZE: u64 = 4096;

const MODIFIED: u8 = 1;
const RECOMPUTE: u8 = 2;

/// What one operator said about each slot during the current cycle.
#[derive(Default)]
struct Marks {
    state: Vec<u8>,
    /// The slots with a non-zero state.
    touched: Vec<usize>,
}

impl Marks {
    fn note(&mut self, slot: usize, effect: Effect) {
        let bits = if effect.modified { MODIFIED } else { 0 }
            | if effect.recompute { RECOMPUTE } else { 0 };
        if bits == 0 {
            return;
        }
        if self.state.len() <= slot {
            self.state.resize(slot + 1, 0);
        }
        if self.state[slot] == 0 {
            self.touched.push(slot);
        }
        self.state[slot] |= bits;
    }

    /// The slots marked with `bit`, increasing.
    fn marked(&self, bit: u8) -> Vec<usize> {
        let mut slots: Vec<usize> = self
            .touched
            .iter()
            .copied()
            .filter(|&slot| self.state[slot] & bit != 0)
            .collect();
        slots.sort_unstable();
        slots
    }

    /// Whether any slot is marked with `bit`.
    fn any(&self, bit: u8) -> bool {
        self.touched.iter().any(|&slot| self.state[slot] & bit != 0)
    }

    fn clear(&mut self) {
        for slot in self.touched.drain(..) {
            self.state[slot] = 0;
        }
    }
}

/// One operator and the columns of the parent it reads.
pub(super) struct Planned {
    pub op: Box<dyn AggOperator>,
    /// The index of the input column in the parent, if the operator has one.
    pub input: Option<usize>,
    /// The index of the weight column, for the weighted aggregations.
    pub weight: Option<usize>,
    /// Other columns the operator reads itself, whose modification it must be told of.
    pub reads: Vec<usize>,
}

impl Planned {
    pub fn new(op: Box<dyn AggOperator>, input: Option<usize>) -> Planned {
        Planned {
            op,
            input,
            weight: None,
            reads: Vec::new(),
        }
    }

    /// Whether a modification of `column` concerns the operator.
    fn watches(&self, column: usize) -> bool {
        self.input == Some(column) || self.weight == Some(column) || self.reads.contains(&column)
    }
}

/// Where the columns of the result are. Java puts the key columns first and then the
/// operators' columns, into a map by name that keeps the place of a name it already has:
/// an operator's column with the name of a key column takes that key column's place, and
/// the key column is not in the result.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationContext#getResultColumns
/// (`keyColumns.putAll(resultColumns)` on a `LinkedHashMap`)
pub(super) struct Layout {
    /// The result column of each key column, or `None` for one an operator replaced.
    pub keys: Vec<Option<usize>>,
    /// The result column of each operator.
    pub ops: Vec<usize>,
}

/// One operator with its input columns and result column.
struct OpColumn {
    plan: Planned,
    result: Arc<WritableSource>,
    marks: Marks,
}

struct Keys {
    table: KeyTable,
    /// The indexes of the key columns in the parent.
    columns: Vec<usize>,
    /// The key columns of the result: the key of slot `s` at row key `s`.
    results: Vec<Arc<WritableSource>>,
}

/// The positions of a chunk brought together by slot.
struct Grouped {
    /// Positions, by slot and then increasing.
    order: Vec<usize>,
    /// Each slot with its range of `order`.
    runs: Vec<(usize, Range<usize>)>,
}

/// Behavior: the run finding of
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper (`findSlotRuns`
/// after a stable sort by slot): each group's rows stay in row order.
fn group(positions: impl Iterator<Item = usize>, slots: &[usize]) -> Grouped {
    let mut order: Vec<usize> = positions.collect();
    order.sort_by_key(|&position| slots[position]);
    let mut runs: Vec<(usize, Range<usize>)> = Vec::new();
    for (index, &position) in order.iter().enumerate() {
        match runs.last_mut() {
            Some((slot, range)) if *slot == slots[position] => range.end = index + 1,
            _ => runs.push((slots[position], index..index + 1)),
        }
    }
    Grouped { order, runs }
}

/// `rows` in pieces of at most [`CHUNK_SIZE`] rows, in order.
fn chunks(rows: &RowSet) -> impl Iterator<Item = RowSet> + '_ {
    let ranges = rows.ranges();
    let mut index = 0;
    let mut offset = 0u64;
    std::iter::from_fn(move || {
        let mut piece = Vec::new();
        let mut room = CHUNK_SIZE;
        while room > 0 && index < ranges.len() {
            let range = ranges[index];
            let first = range.first() + offset as RowKey;
            let taken = room.min(range.size() - offset);
            piece.push(RowRange::new(first, first + taken as RowKey - 1));
            room -= taken;
            offset += taken;
            if offset == range.size() {
                index += 1;
                offset = 0;
            }
        }
        (!piece.is_empty()).then(|| RowSet::from_ranges(piece))
    })
}

fn read(
    parent: &Arc<dyn Table>,
    column: usize,
    rows: &RowSet,
    use_prev: bool,
) -> Result<ArrayRef, TableFailure> {
    let source = parent.column(column);
    if use_prev {
        source.prev_chunk(rows)
    } else {
        source.chunk(rows)
    }
}

fn values_of(decoded: &[(usize, Values)], column: Option<usize>) -> Option<&Values> {
    let column = column?;
    decoded
        .iter()
        .find(|(decoded_column, _)| *decoded_column == column)
        .map(|(_, values)| values)
}

/// The state of one aggregation: the slot of every key, the operators, and how many rows
/// each slot has.
///
/// The result has one row per slot that has rows, at row key `slot`. Slots are numbered in
/// the order their keys are first seen, so the result is in that order, and a group that
/// loses all its rows and gets rows again returns to its old row.
///
/// Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper, both its
/// keyed path (`aggregation`, `KeyedUpdateContext`) and its path without keys
/// (`noKeyAggregation`). Without keys there is one slot, 0.
pub(super) struct Aggregator {
    parent: Arc<dyn Table>,
    parent_types: Vec<ColumnType>,
    /// `None` when aggregating without keys.
    keys: Option<Keys>,
    ops: Vec<OpColumn>,
    slots: usize,
    /// The number of parent rows in each slot.
    row_counts: Vec<u64>,
    /// The parent rows of each slot, kept only if an operator needs them.
    group_rows: Option<Vec<RowSet>>,
    /// Where the result has each key column and each operator's column.
    layout: Layout,
    preserve_empty: bool,
    blink: bool,
    /// The slots whose row count was touched this cycle, with the count they started with.
    touched: Vec<usize>,
    seen: Vec<bool>,
    before: Vec<u64>,
}

impl Aggregator {
    /// `key_columns` and the operators' inputs are indexes of columns of `parent`. With no
    /// key columns the aggregation is the one without keys.
    pub fn new(
        parent: Arc<dyn Table>,
        key_columns: Vec<usize>,
        operators: Vec<Planned>,
        preserve_empty: bool,
        layout: Layout,
    ) -> Result<Aggregator, TableFailure> {
        let graph = parent.update_graph().filter(|_| parent.is_refreshing());
        let source = |column_type: ColumnType| match &graph {
            Some(graph) => tracked_source(graph, column_type),
            None => Arc::new(WritableSource::new(column_type)),
        };
        let parent_types: Vec<ColumnType> = parent
            .definition()
            .columns()
            .iter()
            .map(|column| column.column_type.clone())
            .collect();
        let keys = if key_columns.is_empty() {
            None
        } else {
            let key_types: Vec<ColumnType> = key_columns
                .iter()
                .map(|&column| parent_types[column].clone())
                .collect();
            Some(Keys {
                table: KeyTable::new(&key_types)?,
                results: key_types.into_iter().map(&source).collect(),
                columns: key_columns,
            })
        };
        let ops: Vec<OpColumn> = operators
            .into_iter()
            .map(|plan| OpColumn {
                result: source(plan.op.result_type()),
                plan,
                marks: Marks::default(),
            })
            .collect();
        let group_rows = ops
            .iter()
            .any(|column| column.plan.op.needs_group_rows())
            .then(Vec::new);
        let blink = parent.attributes().blink;
        let mut aggregator = Aggregator {
            parent,
            parent_types,
            keys,
            ops,
            slots: 0,
            row_counts: Vec::new(),
            group_rows,
            layout,
            preserve_empty,
            blink,
            touched: Vec::new(),
            seen: Vec::new(),
            before: Vec::new(),
        };
        if aggregator.keys.is_none() {
            aggregator.ensure_slots(1);
        }
        Ok(aggregator)
    }

    /// The result's columns, where the [`Layout`] puts them.
    pub fn columns(&self) -> Vec<Arc<dyn ColumnSource>> {
        let keys = self.keys.iter().flat_map(|keys| keys.results.iter());
        let places = self.layout.keys.iter().copied();
        let mut placed: Vec<(usize, Arc<dyn ColumnSource>)> = keys
            .zip(places)
            .filter_map(|(source, place)| Some((place?, source.clone() as Arc<dyn ColumnSource>)))
            .collect();
        for (column, &place) in self.ops.iter().zip(&self.layout.ops) {
            placed.push((place, column.result.clone()));
        }
        placed.sort_by_key(|(place, _)| *place);
        placed.into_iter().map(|(_, source)| source).collect()
    }

    /// The failure of an operator that reads the parent itself, if one failed.
    fn check_operators(&mut self) -> Result<(), TableFailure> {
        match self
            .ops
            .iter_mut()
            .find_map(|column| column.plan.op.take_failure())
        {
            Some(failure) => Err(failure),
            None => Ok(()),
        }
    }

    /// Aggregates the parent's current rows. Returns the result's row set.
    ///
    /// Behavior: ChunkedOperatorAggregationHelper#initialBucketedKeyAddition and
    /// #doNoKeyAddition; the size of the result without keys is that of #noKeyAggregation.
    pub fn initial(&mut self) -> Result<RowSet, TableFailure> {
        let rows = self.parent.row_set();
        self.process(&rows, false, true)?;
        if self.preserve_empty {
            for slot in (0..self.slots).filter(|&slot| self.row_counts[slot] == 0) {
                for column in &mut self.ops {
                    let effect = column.plan.op.shown_empty(slot);
                    column.marks.note(slot, effect);
                }
            }
        }
        self.recompute()?;
        let all: Vec<usize> = (0..self.slots).collect();
        self.write(&all, |_| true)?;
        self.end_cycle();
        Ok(RowSet::from_keys(
            all.into_iter()
                .filter(|&slot| self.present(slot))
                .map(|slot| slot as RowKey),
        ))
    }

    fn present(&self, slot: usize) -> bool {
        self.preserve_empty || self.row_counts[slot] > 0
    }

    fn ensure_slots(&mut self, slots: usize) {
        if slots <= self.slots {
            return;
        }
        self.slots = slots;
        for column in &mut self.ops {
            column.plan.op.ensure_slots(slots);
        }
        self.row_counts.resize(slots, 0);
        self.seen.resize(slots, false);
        self.before.resize(slots, 0);
        if let Some(group_rows) = &mut self.group_rows {
            group_rows.resize(slots, RowSet::empty());
        }
    }

    /// The slot of each of `rows`, by their previous or current keys. With `create`, keys
    /// not seen before get slots, and their key columns are written.
    ///
    /// Behavior: OperatorAggregationStateManager#add and
    /// IncrementalOperatorAggregationStateManager#remove / #findModifications; the key
    /// columns of a new slot are those of the first row seen with the key.
    fn slots_of(
        &mut self,
        rows: &RowSet,
        use_prev: bool,
        create: bool,
    ) -> Result<Vec<usize>, TableFailure> {
        let Some(keys) = &mut self.keys else {
            return Ok(vec![0; rows.size() as usize]);
        };
        let arrays = keys
            .columns
            .iter()
            .map(|&column| read(&self.parent, column, rows, use_prev))
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
            self.ensure_slots(slot_count);
        }
        Ok(slots)
    }

    /// The chunks at `rows` of the input and weight columns of the operators that `wanted`
    /// selects, decoded.
    fn decode(
        &self,
        rows: &RowSet,
        use_prev: bool,
        wanted: impl Fn(&Planned) -> bool,
    ) -> Result<Vec<(usize, Values)>, TableFailure> {
        let mut decoded: Vec<(usize, Values)> = Vec::new();
        let columns = self
            .ops
            .iter()
            .filter(|column| wanted(&column.plan))
            .flat_map(|column| column.plan.input.into_iter().chain(column.plan.weight));
        for column in columns {
            if values_of(&decoded, Some(column)).is_none() {
                let array = read(&self.parent, column, rows, use_prev)?;
                decoded.push((column, Values::decode(&self.parent_types[column], &array)?));
            }
        }
        Ok(decoded)
    }

    fn touch(&mut self, slot: usize) {
        if !self.seen[slot] {
            self.seen[slot] = true;
            self.before[slot] = self.row_counts[slot];
            self.touched.push(slot);
        }
    }

    /// Adds the grouped rows of one chunk to their slots, or removes them.
    fn apply(
        &mut self,
        rows: &RowSet,
        row_keys: &[RowKey],
        grouped: &Grouped,
        use_prev: bool,
        add: bool,
    ) -> Result<(), TableFailure> {
        let decoded = self.decode(rows, use_prev, |_| true)?;
        for column in &mut self.ops {
            let values = values_of(&decoded, column.plan.input);
            let weights = values_of(&decoded, column.plan.weight);
            for (slot, range) in &grouped.runs {
                let run =
                    Run::new(values, row_keys, &grouped.order[range.clone()]).with_weights(weights);
                let effect = if add {
                    column.plan.op.add(*slot, &run)
                } else {
                    column.plan.op.remove(*slot, &run)
                };
                column.marks.note(*slot, effect);
            }
        }
        self.check_operators()?;
        for (slot, range) in &grouped.runs {
            let slot = *slot;
            self.touch(slot);
            let count = range.len() as u64;
            self.row_counts[slot] = if add {
                self.row_counts[slot] + count
            } else {
                self.row_counts[slot].checked_sub(count).ok_or_else(|| {
                    failure("aggregation: more rows removed from a group than it has")
                })?
            };
            if let Some(group_rows) = &mut self.group_rows {
                let keys =
                    RowSet::from_keys(grouped.order[range.clone()].iter().map(|&p| row_keys[p]));
                if add {
                    group_rows[slot].insert(&keys);
                } else {
                    group_rows[slot].remove(&keys);
                }
            }
        }
        Ok(())
    }

    /// Adds `rows` (current keys and values) to their groups, or removes `rows` (previous
    /// keys and values) from theirs.
    ///
    /// Behavior: KeyedUpdateContext#doInserts and #doRemoves;
    /// ChunkedOperatorAggregationHelper#doNoKeyAddition and #doNoKeyRemoval
    fn process(&mut self, rows: &RowSet, use_prev: bool, add: bool) -> Result<(), TableFailure> {
        for chunk in chunks(rows) {
            let row_keys: Vec<RowKey> = chunk.iter().collect();
            let slots = self.slots_of(&chunk, use_prev, add)?;
            let grouped = group(0..row_keys.len(), &slots);
            self.apply(&chunk, &row_keys, &grouped, use_prev, add)?;
        }
        Ok(())
    }

    /// Takes the modified rows whose key changed out of their old groups. `pre` and `post`
    /// are the modified rows before and after the shift. The rows that changed group are
    /// appended to `moved_pre` and `moved_post`; the others are returned.
    ///
    /// Behavior: KeyedUpdateContext#splitKeyModificationsAndDoKeyChangeRemoves
    fn split_key_changes(
        &mut self,
        pre: &RowSet,
        post: &RowSet,
        moved_pre: &mut Vec<RowKey>,
        moved_post: &mut Vec<RowKey>,
    ) -> Result<(RowSet, RowSet), TableFailure> {
        let mut same_pre = Vec::new();
        let mut same_post = Vec::new();
        for (pre_chunk, post_chunk) in chunks(pre).zip(chunks(post)) {
            let pre_keys: Vec<RowKey> = pre_chunk.iter().collect();
            let post_keys: Vec<RowKey> = post_chunk.iter().collect();
            let old = self.slots_of(&pre_chunk, true, false)?;
            let new = self.slots_of(&post_chunk, false, true)?;
            let mut moved = Vec::new();
            for position in 0..pre_keys.len() {
                if old[position] == new[position] {
                    same_pre.push(pre_keys[position]);
                    same_post.push(post_keys[position]);
                } else {
                    moved.push(position);
                    moved_pre.push(pre_keys[position]);
                    moved_post.push(post_keys[position]);
                }
            }
            if !moved.is_empty() {
                let grouped = group(moved.into_iter(), &old);
                self.apply(&pre_chunk, &pre_keys, &grouped, true, false)?;
            }
        }
        Ok((RowSet::from_keys(same_pre), RowSet::from_keys(same_post)))
    }

    /// Moves the shifted rows within the row sets of their groups. Does nothing if no
    /// operator needs group rows. A shift changes no value and no order, so no result;
    /// the operators that need group rows are told, for the row keys they keep.
    ///
    /// Behavior: KeyedUpdateContext#doShifts and
    /// ChunkedOperatorAggregationHelper#doNoKeyShifts, which run only for operators with
    /// `requiresRowKeys`; IterativeChunkedAggregationOperator#shiftChunk is where Java's
    /// operators move their own row sets.
    fn shift_group_rows(
        &mut self,
        update: &TableUpdate,
        moved_pre: &[RowKey],
    ) -> Result<(), TableFailure> {
        if self.group_rows.is_none() || update.shifted.is_empty() {
            return Ok(());
        }
        let mut survivors = self.parent.prev_row_set().minus(&update.removed);
        survivors.remove(&RowSet::from_keys(moved_pre.iter().copied()));
        let mut affected = RowSet::empty();
        for shift in update.shifted.ranges() {
            affected.insert(&survivors.sub_set_by_key_range(shift.first, shift.last));
        }
        let mut rows_by_slot: Vec<(usize, RowKey)> = Vec::new();
        for chunk in chunks(&affected) {
            let slots = self.slots_of(&chunk, true, false)?;
            rows_by_slot.extend(slots.into_iter().zip(chunk.iter()));
        }
        rows_by_slot.sort_by_key(|&(slot, _)| slot);
        for run in rows_by_slot.chunk_by(|a, b| a.0 == b.0) {
            let slot = run[0].0;
            let before = RowSet::from_keys(run.iter().map(|&(_, row_key)| row_key));
            let after = update
                .shifted
                .apply(&before)
                .map_err(|error| failure(error.to_string()))?;
            if let Some(group_rows) = &mut self.group_rows {
                group_rows[slot].remove(&before);
                group_rows[slot].insert(&after);
            }
            for column in &mut self.ops {
                if column.plan.op.needs_group_rows() {
                    column.plan.op.shifted(slot, &before, &after);
                }
            }
        }
        Ok(())
    }

    /// Gives the modified rows that stayed in their group to the operators that read a
    /// column among `modified_columns`.
    ///
    /// Behavior: KeyedUpdateContext#doSameSlotModifies and
    /// ChunkedOperatorAggregationHelper#doNoKeyModifications
    fn modify_same_slot(
        &mut self,
        pre: &RowSet,
        post: &RowSet,
        modified_columns: &ModifiedColumnSet,
    ) -> Result<(), TableFailure> {
        let column_count = self.parent_types.len();
        let wanted = |plan: &Planned| {
            (0..column_count)
                .any(|column| modified_columns.contains(column) && plan.watches(column))
        };
        if !self.ops.iter().any(|column| wanted(&column.plan)) {
            return Ok(());
        }
        for (pre_chunk, post_chunk) in chunks(pre).zip(chunks(post)) {
            let pre_keys: Vec<RowKey> = pre_chunk.iter().collect();
            let post_keys: Vec<RowKey> = post_chunk.iter().collect();
            let slots = self.slots_of(&post_chunk, false, false)?;
            let grouped = group(0..post_keys.len(), &slots);
            let old = self.decode(&pre_chunk, true, wanted)?;
            let new = self.decode(&post_chunk, false, wanted)?;
            for column in &mut self.ops {
                if !wanted(&column.plan) {
                    continue;
                }
                let (input, weight) = (column.plan.input, column.plan.weight);
                for (slot, range) in &grouped.runs {
                    let positions = &grouped.order[range.clone()];
                    let old_run = Run::new(values_of(&old, input), &pre_keys, positions)
                        .with_weights(values_of(&old, weight));
                    let new_run = Run::new(values_of(&new, input), &post_keys, positions)
                        .with_weights(values_of(&new, weight));
                    let effect = column.plan.op.modify(*slot, &old_run, &new_run);
                    column.marks.note(*slot, effect);
                }
            }
            self.check_operators()?;
        }
        Ok(())
    }

    /// Brings the aggregation up to date with one change of the parent and returns the
    /// change of the result.
    ///
    /// The order is Java's: removed rows leave their groups; modified rows whose key
    /// changed leave theirs; shifts are applied; modified rows that kept their key are
    /// given to the operators whose input changed; rows whose key changed join their new
    /// groups; added rows join theirs. New groups therefore get their slots, and so their
    /// place in the result, in the order: keys of re-keyed rows, then keys of added rows.
    ///
    /// Behavior: ChunkedOperatorAggregationHelper.KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys
    /// and the `processNoKeyUpdate` of ChunkedOperatorAggregationHelper#noKeyAggregation
    pub fn on_update(&mut self, update: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let result = self.advance(update);
        if result.is_err() {
            self.end_cycle();
        }
        result
    }

    fn advance(&mut self, update: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let slots_at_start = self.slots;
        if self.blink {
            // Behavior: ChunkedOperatorAggregationHelper#adjustForBlinkTable. The rows of
            // a blink table that leave it stay aggregated.
            if !update.modified.is_empty() || !update.shifted.is_empty() {
                return Err(failure(
                    "aggregation: a blink table modified or shifted rows",
                ));
            }
        } else {
            self.process(&update.removed, true, false)?;
        }
        let mut moved_pre = Vec::new();
        let mut moved_post = Vec::new();
        if update.modified.is_empty() {
            self.shift_group_rows(update, &moved_pre)?;
        } else {
            let pre = modified_pre_shift(update).map_err(|error| failure(error.to_string()))?;
            let keys_modified = self
                .keys
                .as_ref()
                .is_some_and(|keys| update.modified_columns.contains_any(&keys.columns));
            let (same_pre, same_post) = if keys_modified {
                self.split_key_changes(&pre, &update.modified, &mut moved_pre, &mut moved_post)?
            } else {
                (pre, update.modified.clone())
            };
            self.shift_group_rows(update, &moved_pre)?;
            self.modify_same_slot(&same_pre, &same_post, &update.modified_columns)?;
            self.process(&RowSet::from_keys(moved_post), false, true)?;
        }
        self.process(&update.added, false, true)?;
        self.recompute()?;
        let downstream = self.downstream(slots_at_start)?;
        self.end_cycle();
        Ok(downstream)
    }

    /// Lets the operators that could not answer from their state read their groups.
    fn recompute(&mut self) -> Result<(), TableFailure> {
        let group_rows: &[RowSet] = self.group_rows.as_deref().unwrap_or(&[]);
        let keys: &[Arc<WritableSource>] = self.keys.as_ref().map_or(&[], |keys| &keys.results);
        for column in &mut self.ops {
            let pending = column.marks.marked(RECOMPUTE);
            if pending.is_empty() {
                continue;
            }
            let input = column.plan.input.map(|input| self.parent.column(input));
            let reader = GroupReader::new(group_rows, input.as_deref(), keys);
            let changed = column.plan.op.recompute(&pending, &reader)?;
            for (&slot, changed) in pending.iter().zip(changed) {
                if changed {
                    column.marks.note(slot, Effect::MODIFIED);
                }
            }
        }
        Ok(())
    }

    /// Writes the results of `slots`, and of the slots each operator marked modified that
    /// `also` selects, to the result columns. Returns, per operator, the marked slots
    /// written that are not among `slots`.
    fn write(
        &mut self,
        slots: &[usize],
        also: impl Fn(usize) -> bool,
    ) -> Result<Vec<Vec<usize>>, TableFailure> {
        let mut modified_by_op = Vec::with_capacity(self.ops.len());
        for column in &self.ops {
            let modified: Vec<usize> = column
                .marks
                .marked(MODIFIED)
                .into_iter()
                .filter(|&slot| also(slot) && slots.binary_search(&slot).is_err())
                .collect();
            let written: Vec<usize> = slots.iter().chain(&modified).copied().collect();
            let row_keys: Vec<RowKey> = written.iter().map(|&slot| slot as RowKey).collect();
            column
                .result
                .fill_by_keys(&row_keys, &column.plan.op.results(&written))?;
            modified_by_op.push(modified);
        }
        Ok(modified_by_op)
    }

    /// The change of the result, after writing the changed results.
    ///
    /// A row is added when its group gets its first row and removed when it loses its
    /// last (with `preserve_empty`, a row is added for a new group and never removed). A
    /// row that stays is modified if an operator said its result changed. The modified
    /// columns are the results of the operators that said so of any group, also of one
    /// whose row is added or removed. Nothing is ever shifted.
    ///
    /// Behavior: KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys with
    /// ChunkedOperatorAggregationHelper#extractDownstreamModifiedColumnSet, and the
    /// `processNoKeyUpdate` of ChunkedOperatorAggregationHelper#noKeyAggregation
    fn downstream(&mut self, slots_at_start: usize) -> Result<TableUpdate, TableFailure> {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        for &slot in &self.touched {
            let had_rows = self.before[slot] > 0;
            let has_rows = self.row_counts[slot] > 0;
            if slot >= slots_at_start || (!self.preserve_empty && !had_rows && has_rows) {
                added.push(slot);
            } else if !self.preserve_empty && had_rows && !has_rows {
                removed.push(slot);
            }
        }
        added.sort_unstable();
        let present: Vec<bool> = (0..self.slots).map(|slot| self.present(slot)).collect();
        let modified_by_op = self.write(&added, |slot| present[slot])?;
        let mut modified = Vec::new();
        let mut modified_columns = Vec::new();
        for slots in modified_by_op {
            modified.extend(slots.into_iter().map(|slot| slot as RowKey));
        }
        // Behavior: ChunkedOperatorAggregationHelper#extractDownstreamModifiedColumnSet
        // with `modifiedOperators`, which is set for an operator that said in any chunk
        // that some group's state changed, whatever became of the group. So if any row is
        // modified, the modified columns are those of every operator whose result changed
        // for any group this cycle, also for one that is new or gone.
        if !modified.is_empty() {
            for (index, column) in self.ops.iter().enumerate() {
                if column.marks.any(MODIFIED) && column.plan.op.names_result() {
                    modified_columns.push(self.layout.ops[index]);
                }
            }
        }
        let row_set = |slots: Vec<usize>| RowSet::from_keys(slots.into_iter().map(|s| s as RowKey));
        Ok(TableUpdate {
            added: row_set(added),
            removed: row_set(removed),
            modified: RowSet::from_keys(modified),
            modified_columns: ModifiedColumnSet::of(modified_columns),
            ..TableUpdate::empty()
        })
    }

    fn end_cycle(&mut self) {
        for column in &mut self.ops {
            column.marks.clear();
        }
        for slot in self.touched.drain(..) {
            self.seen[slot] = false;
        }
    }
}
