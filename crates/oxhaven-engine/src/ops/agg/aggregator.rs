//! The aggregation framework: rows to slots, slots to operators, operators to result rows.

use super::keys::KeyTable;
use super::operator::{
    AggOperator, Effect, FlatGroups, GroupReader, Marks, Rows, MODIFIED, RECOMPUTE,
};
use super::slot_source::ResultColumn;
use super::values::{failure, Run, Runs, Values};
use crate::update::modified_pre_shift;
use arrow::array::{ArrayRef, UInt64Array};
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::{RowKey, RowSet};
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

/// What `Aggregator::chunk_index` holds for a slot that the chunk at hand has not met.
const NOT_IN_CHUNK: u32 = u32::MAX;

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
    result: ResultColumn,
    marks: Marks,
}

struct Keys {
    table: KeyTable,
    /// The indexes of the key columns in the parent.
    columns: Vec<usize>,
    /// The key columns of the result: the key of slot `s` at row key `s`.
    results: Vec<ResultColumn>,
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
    let positions: Vec<usize> = positions.collect();
    let (low, high) = positions.iter().fold((usize::MAX, 0), |(low, high), &p| {
        (low.min(slots[p]), high.max(slots[p]))
    });
    debug_assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    if !positions.is_empty() && high - low < 4 * positions.len() + 1024 {
        group_by_counting(&positions, slots, low, high)
    } else {
        group_by_sorting(positions, slots)
    }
}

/// [`group`] by a comparison sort, for slots spread wide.
fn group_by_sorting(positions: Vec<usize>, slots: &[usize]) -> Grouped {
    // Sorting (slot, position) pairs packed in one integer keeps each slot's positions
    // increasing without a stable sort. A chunk has at most `CHUNK_SIZE` positions.
    let mut packed: Vec<u64> = positions
        .into_iter()
        .map(|position| (slots[position] as u64) << 32 | position as u64)
        .collect();
    packed.sort_unstable();
    let mut order = Vec::with_capacity(packed.len());
    let mut runs: Vec<(usize, Range<usize>)> = Vec::new();
    for (index, &entry) in packed.iter().enumerate() {
        let (slot, position) = (
            (entry >> 32) as usize,
            (entry & u64::from(u32::MAX)) as usize,
        );
        order.push(position);
        match runs.last_mut() {
            Some((last, range)) if *last == slot => range.end = index + 1,
            _ => runs.push((slot, index..index + 1)),
        }
    }
    Grouped { order, runs }
}

/// [`group`] for positions whose slots lie in `low..=high`, a range not much larger than
/// the number of positions (a chunk of a table with few groups): a counting sort by slot,
/// which keeps each slot's positions in the order given, instead of a comparison sort.
fn group_by_counting(positions: &[usize], slots: &[usize], low: usize, high: usize) -> Grouped {
    let mut starts = vec![0usize; high - low + 2];
    for &position in positions {
        starts[slots[position] - low + 1] += 1;
    }
    for index in 1..starts.len() {
        starts[index] += starts[index - 1];
    }
    let mut runs: Vec<(usize, Range<usize>)> = Vec::new();
    for (offset, window) in starts.windows(2).enumerate() {
        if window[0] < window[1] {
            runs.push((low + offset, window[0]..window[1]));
        }
    }
    let mut order = vec![0usize; positions.len()];
    for &position in positions {
        let next = &mut starts[slots[position] - low];
        order[*next] = position;
        *next += 1;
    }
    Grouped { order, runs }
}

/// The keys of `rows`, increasing, in place of what `out` held.
fn row_keys_of(rows: &RowSet, out: &mut Vec<RowKey>) {
    out.clear();
    for range in rows.ranges() {
        out.extend(range.first()..=range.last());
    }
}

/// `rows` in pieces of at most [`CHUNK_SIZE`] rows, in order.
fn chunks(rows: &RowSet) -> impl Iterator<Item = RowSet> + '_ {
    (0..rows.size())
        .step_by(CHUNK_SIZE as usize)
        .map(|start| rows.sub_set_by_position_range(start, start + CHUNK_SIZE))
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
    /// While [`Aggregator::initial`] adds the parent's rows, if group rows are kept: the
    /// slot of each row added, in row order, from which the rows of every group are
    /// brought together once at the end, instead of a row set insertion per run of each
    /// chunk.
    initial_slots: Option<Vec<u32>>,
    /// For [`Aggregator::add_rows`]: where each slot met in the chunk at hand is in
    /// `distinct`, the slots met, and where each row's slot is. Between chunks no slot
    /// has a place (`NOT_IN_CHUNK`) and the other two are empty.
    chunk_index: Vec<u32>,
    distinct: Vec<usize>,
    chunk_groups: Vec<u32>,
    /// While [`Aggregator::initial`] adds the parent's rows: the key columns of the rows
    /// that made a slot, a chunk after another, which are written to the result's key
    /// columns once at the end instead of a piece per chunk.
    initial_keys: Option<Vec<Vec<ArrayRef>>>,
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
        let source = |column_type: ColumnType| ResultColumn::new(column_type, graph.as_ref());
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
            initial_slots: None,
            chunk_index: Vec::new(),
            distinct: Vec::new(),
            chunk_groups: Vec::new(),
            initial_keys: None,
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
            .filter_map(|(source, place)| {
                let place = place?;
                // Behavior: ChunkedOperatorAggregationHelper#aggregation, which copies the
                // keys of a refreshing parent out of the hash table and for a static
                // parent shows the hash table's own sources through a redirection.
                let source = if self.parent.is_refreshing() {
                    source.source()
                } else {
                    source.shared()
                };
                Some((place, source))
            })
            .collect();
        for (column, &place) in self.ops.iter().zip(&self.layout.ops) {
            let source = if column.plan.op.holds_results() {
                column.result.source()
            } else {
                column.result.shared()
            };
            placed.push((place, column.plan.op.result_column(source)));
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
        if self.group_rows.is_some() {
            self.initial_slots = Some(Vec::with_capacity(rows.size() as usize));
        }
        // Every slot is written below, whatever an operator says changed.
        for column in &mut self.ops {
            column.marks.note_recompute_only(true);
        }
        let slots_before = self.slots;
        self.initial_keys = Some(Vec::new());
        let added = self.process(&rows, false, true);
        let initial_slots = self.initial_slots.take();
        let initial_keys = self.initial_keys.take();
        added?;
        self.write_keys(slots_before, initial_keys.unwrap_or_default())?;
        // The rows of each group, for the operators that read them: as flat arrays here,
        // and as row sets afterwards only where a later cycle can ask for them.
        let flat = initial_slots.map(|slots| FlatGroups::new(&self.row_counts, &slots, &rows));
        if let (Some(group_rows), Some(flat), true) =
            (&mut self.group_rows, &flat, self.parent.is_refreshing())
        {
            for (slot, group) in group_rows.iter_mut().enumerate() {
                let keys = flat.of(slot);
                if !keys.is_empty() {
                    group.insert(&RowSet::from_keys(keys.iter().copied()));
                }
            }
        }
        if self.preserve_empty {
            for slot in (0..self.slots).filter(|&slot| self.row_counts[slot] == 0) {
                for column in &mut self.ops {
                    let effect = column.plan.op.shown_empty(slot);
                    column.marks.note(slot, effect);
                }
            }
        }
        self.recompute(flat.as_ref())?;
        drop(flat);
        let all: Vec<usize> = (0..self.slots).collect();
        for column in &self.ops {
            column.result.fill_from(0, &column.plan.op.results(&all))?;
        }
        self.forget_keys_of_absent_groups()?;
        for column in &mut self.ops {
            column.marks.note_recompute_only(false);
        }
        self.end_cycle();
        if all.iter().all(|&slot| self.present(slot)) {
            return Ok(RowSet::flat(self.slots as u64));
        }
        Ok(RowSet::from_keys(
            all.into_iter()
                .filter(|&slot| self.present(slot))
                .map(|slot| slot as RowKey),
        ))
    }

    /// Writes the key columns of the slots from `first` on, which `pieces` hold a chunk
    /// after another.
    fn write_keys(&mut self, first: usize, pieces: Vec<Vec<ArrayRef>>) -> Result<(), TableFailure> {
        let Some(keys) = &self.keys else {
            return Ok(());
        };
        if pieces.is_empty() {
            return Ok(());
        }
        for (index, result) in keys.results.iter().enumerate() {
            let column: Vec<&dyn arrow::array::Array> =
                pieces.iter().map(|piece| piece[index].as_ref()).collect();
            let values =
                arrow::compute::concat(&column).map_err(|error| failure(error.to_string()))?;
            result.fill_from(first, &values)?;
        }
        Ok(())
    }

    /// Whether some group has no rows.
    pub fn has_empty_group(&self) -> bool {
        self.keys.is_some() && self.row_counts.contains(&0)
    }

    /// Makes null the keys the result shows for the initial groups that are not in it at
    /// the start: those a refreshing parent has no row for, without `preserve_empty`. The
    /// slot keeps its key in the key table, so the group's rows still find it, and the
    /// result shows the group with null keys from its first row on, for as long as it
    /// lives. A formula that reads a key column reads that null too.
    ///
    /// Behavior: ChunkedOperatorAggregationHelper#aggregation, which for a refreshing
    /// parent copies the keys into the result's own columns: #copyKeyColumns for the
    /// initial row set, and in KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys for
    /// the states made in a cycle (`newStates`). A state that
    /// #initialKeyTableAddition made and that gets its first row later is neither, so
    /// its keys are never copied. Observed on 42.9 for every aggregation, with one and
    /// two key columns, on append-only and keyed tables, and after the group lost its
    /// rows and got rows again.
    fn forget_keys_of_absent_groups(&mut self) -> Result<(), TableFailure> {
        let Some(keys) = &self.keys else {
            return Ok(());
        };
        if self.preserve_empty || !self.parent.is_refreshing() {
            return Ok(());
        }
        let absent: Vec<RowKey> = (0..self.slots)
            .filter(|&slot| self.row_counts[slot] == 0)
            .map(|slot| slot as RowKey)
            .collect();
        if absent.is_empty() {
            return Ok(());
        }
        for result in &keys.results {
            let nulls =
                arrow::array::new_null_array(&result.column_type().arrow_type(), absent.len());
            result.fill_by_keys(&absent, &nulls)?;
        }
        Ok(())
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
        self.chunk_index.resize(slots, NOT_IN_CHUNK);
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
        self.find_or_create(&arrays)
    }

    /// The slot of each row of `arrays`, the key columns of some rows, with a new slot for
    /// each key not seen before, whose key columns are written.
    fn find_or_create(&mut self, arrays: &[ArrayRef]) -> Result<Vec<usize>, TableFailure> {
        let Some(keys) = &mut self.keys else {
            return Ok(vec![0; arrays.first().map_or(0, |array| array.len())]);
        };
        let (slots, created) = keys.table.find_or_create(arrays)?;
        if !created.is_empty() {
            let slot_count = keys.table.len();
            let new_slots: Vec<RowKey> = match self.initial_keys {
                Some(_) => Vec::new(),
                None => (slot_count - created.len()..slot_count)
                    .map(|slot| slot as RowKey)
                    .collect(),
            };
            let positions: UInt64Array = created.iter().map(|&p| p as u64).collect();
            let first_seen = arrays
                .iter()
                .map(|array| {
                    arrow::compute::take(array, &positions, None)
                        .map_err(|error| failure(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            match &mut self.initial_keys {
                Some(later) => later.push(first_seen),
                None => {
                    for (first_seen, result) in first_seen.iter().zip(&keys.results) {
                        result.fill_by_keys(&new_slots, first_seen)?;
                    }
                }
            }
            self.ensure_slots(slot_count);
        }
        Ok(slots)
    }

    /// Gives every key of `initial_groups` a slot, in the order of its rows, before any
    /// row of the parent is read: the result has those groups first, and with
    /// `preserve_empty` also the ones the parent has no row for. `columns` are the key
    /// columns in `initial_groups`, in the order of the group-by columns. The table is
    /// read once, as it is now; what it does later changes nothing.
    ///
    /// Behavior: ChunkedOperatorAggregationHelper#initialKeyTableAddition and
    /// #makeInitializedStateManager
    pub fn seed(
        &mut self,
        initial_groups: &Arc<dyn Table>,
        columns: &[usize],
    ) -> Result<(), TableFailure> {
        let read_keys = |aggregator: &mut Aggregator| {
            let rows = initial_groups.row_set();
            for chunk in chunks(&rows) {
                let arrays = columns
                    .iter()
                    .map(|&column| initial_groups.column(column).chunk(&chunk))
                    .collect::<Result<Vec<_>, _>>()?;
                aggregator.find_or_create(&arrays)?;
            }
            Ok::<(), TableFailure>(())
        };
        match initial_groups
            .update_graph()
            .filter(|_| initial_groups.is_refreshing())
        {
            Some(graph) => locked(&*graph, || read_keys(self)),
            None => read_keys(self),
        }?;
        for column in &mut self.ops {
            for slot in 0..self.slots {
                column.plan.op.seeded(slot);
            }
        }
        Ok(())
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

    #[inline]
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
        let mut effects = Vec::with_capacity(grouped.runs.len());
        for column in &mut self.ops {
            let values = values_of(&decoded, column.plan.input);
            let weights = values_of(&decoded, column.plan.weight);
            let runs = Runs::new(values, weights, row_keys, &grouped.order, &grouped.runs);
            effects.clear();
            if add {
                column.plan.op.add_runs(&runs, &mut effects);
            } else {
                column.plan.op.remove_runs(&runs, &mut effects);
            }
            assert_eq!(effects.len(), grouped.runs.len(), "one effect per group");
            for ((slot, _), effect) in grouped.runs.iter().zip(&effects) {
                column.marks.note(*slot, *effect);
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
            if self.initial_slots.is_some() {
                // `Aggregator::initial` brings the rows of each group together itself.
            } else if let Some(group_rows) = &mut self.group_rows {
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

    /// [`Aggregator::apply`] for added rows when every operator takes them in row order
    /// ([`AggOperator::adds_rows`](super::operator::AggOperator::adds_rows)): the rows
    /// are not grouped by slot. The operators are told which slots the chunk has, each
    /// once, and what they note of those comes to the same marks and counts.
    fn add_rows(
        &mut self,
        rows: &RowSet,
        row_keys: &[RowKey],
        slots: &[usize],
    ) -> Result<(), TableFailure> {
        let decoded = self.decode(rows, false, |_| true)?;
        let mut distinct = std::mem::take(&mut self.distinct);
        let mut groups = std::mem::take(&mut self.chunk_groups);
        groups.clear();
        for &slot in slots {
            let mut index = self.chunk_index[slot];
            if index == NOT_IN_CHUNK {
                index = distinct.len() as u32;
                self.chunk_index[slot] = index;
                distinct.push(slot);
                self.touch(slot);
            }
            groups.push(index);
            self.row_counts[slot] += 1;
        }
        for column in &mut self.ops {
            column.marks.reserve(self.slots);
            let chunk = Rows {
                slots,
                distinct: &distinct,
                groups: &groups,
                row_keys,
                values: values_of(&decoded, column.plan.input),
            };
            column.plan.op.add_rows(&chunk, &mut column.marks);
        }
        for slot in distinct.drain(..) {
            self.chunk_index[slot] = NOT_IN_CHUNK;
        }
        self.distinct = distinct;
        self.chunk_groups = groups;
        self.check_operators()
    }

    /// Adds `rows` (current keys and values) to their groups, or removes `rows` (previous
    /// keys and values) from theirs.
    ///
    /// Behavior: KeyedUpdateContext#doInserts and #doRemoves;
    /// ChunkedOperatorAggregationHelper#doNoKeyAddition and #doNoKeyRemoval
    fn process(&mut self, rows: &RowSet, use_prev: bool, add: bool) -> Result<(), TableFailure> {
        // Group rows kept as row sets need each group's rows of a chunk together; while
        // the first result is made they are not kept so.
        let by_row = add
            && (self.group_rows.is_none() || self.initial_slots.is_some())
            && self.ops.iter().all(|column| column.plan.op.adds_rows());
        let mut row_keys: Vec<RowKey> = Vec::new();
        for chunk in chunks(rows) {
            row_keys_of(&chunk, &mut row_keys);
            let slots = self.slots_of(&chunk, use_prev, add)?;
            if by_row {
                self.add_rows(&chunk, &row_keys, &slots)?;
            } else {
                let grouped = group(0..row_keys.len(), &slots);
                self.apply(&chunk, &row_keys, &grouped, use_prev, add)?;
            }
            if let Some(initial_slots) = &mut self.initial_slots {
                initial_slots.extend(slots.iter().map(|&slot| slot as u32));
            }
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
                    if column.plan.op.recomputes_shifted() {
                        column.marks.note(slot, Effect::RECOMPUTE);
                    }
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

    /// Tells the operators that the result has failed.
    pub fn on_failure(&mut self, failure: &TableFailure) {
        for column in &mut self.ops {
            column.plan.op.failed(failure);
        }
    }

    fn advance(&mut self, update: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let slots_at_start = self.slots;
        for column in &mut self.ops {
            column.plan.op.begin_cycle(update);
        }
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
        self.recompute(None)?;
        let downstream = self.downstream(slots_at_start)?;
        self.end_cycle();
        Ok(downstream)
    }

    /// Lets the operators that could not answer from their state read their groups: from
    /// `flat` while the first result is made, from the row sets of the groups otherwise.
    fn recompute(&mut self, flat: Option<&FlatGroups>) -> Result<(), TableFailure> {
        let group_rows: &[RowSet] = self.group_rows.as_deref().unwrap_or(&[]);
        let keys: &[ResultColumn] = self.keys.as_ref().map_or(&[], |keys| &keys.results);
        for column in &mut self.ops {
            let pending = column.marks.marked(RECOMPUTE);
            if pending.is_empty() {
                continue;
            }
            let input = column.plan.input.map(|input| self.parent.column(input));
            let reader = GroupReader::new(group_rows, input.as_deref(), keys).with_flat(flat);
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
    /// written that are not among `slots`. Without `also`, only `slots` are written.
    fn write(
        &mut self,
        slots: &[usize],
        also: Option<&dyn Fn(usize) -> bool>,
    ) -> Result<Vec<Vec<usize>>, TableFailure> {
        let mut modified_by_op = Vec::with_capacity(self.ops.len());
        for column in &self.ops {
            let modified: Vec<usize> = match also {
                Some(also) => column
                    .marks
                    .marked(MODIFIED)
                    .into_iter()
                    .filter(|&slot| also(slot) && slots.binary_search(&slot).is_err())
                    .collect(),
                None => Vec::new(),
            };
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
        let modified_by_op = self.write(&added, Some(&|slot| present[slot]))?;
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

#[cfg(test)]
mod grouping_tests {
    use super::*;

    /// Both ways of grouping give the same order and runs, for slots in a narrow range
    /// and in a wide one, for every position or some of them.
    #[test]
    fn counting_and_sorting_group_alike() {
        let mut state = 17u64;
        let mut next = |n: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            ((state >> 33) % n) as usize
        };
        for (spread, base) in [(1, 0), (3, 5), (10, 1000), (5000, 7), (1 << 20, 3)] {
            let slots: Vec<usize> = (0..300).map(|_| base + next(spread)).collect();
            let all: Vec<usize> = (0..slots.len()).collect();
            let some: Vec<usize> = all.iter().copied().filter(|p| p % 3 != 1).collect();
            for positions in [all, some] {
                let (low, high) = positions.iter().fold((usize::MAX, 0), |(l, h), &p| {
                    (l.min(slots[p]), h.max(slots[p]))
                });
                let counted = group_by_counting(&positions, &slots, low, high);
                let sorted = group_by_sorting(positions.clone(), &slots);
                assert_eq!(counted.order, sorted.order, "spread {spread}");
                assert_eq!(counted.runs, sorted.runs, "spread {spread}");
                let chosen = group(positions.into_iter(), &slots);
                assert_eq!(chosen.order, sorted.order);
                assert_eq!(chosen.runs, sorted.runs);
            }
        }
    }
}
