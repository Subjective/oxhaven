//! The interface between the aggregation framework and one aggregation.

use super::slot_source::ResultColumn;
use super::values::{Run, Runs, Values};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::borrow::Cow;
use std::sync::Arc;

/// What a call did to a group's result. Both can be true: a removal that needs the
/// group read again, followed by an addition that changed the result.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Effect {
    /// The result may have changed. The framework writes it and reports the row modified.
    pub modified: bool,
    /// The operator cannot tell from the rows it was given. The framework calls
    /// [`AggOperator::recompute`] for the group once all rows of this cycle are processed.
    /// Only an operator that [needs group rows](AggOperator::needs_group_rows) may ask.
    pub recompute: bool,
}

impl Effect {
    /// The result is what it was.
    pub const UNCHANGED: Effect = Effect {
        modified: false,
        recompute: false,
    };
    pub const MODIFIED: Effect = Effect {
        modified: true,
        recompute: false,
    };
    pub const RECOMPUTE: Effect = Effect {
        modified: false,
        recompute: true,
    };

    /// The effect of two calls on one group.
    pub fn and(self, other: Effect) -> Effect {
        Effect {
            modified: self.modified || other.modified,
            recompute: self.recompute || other.recompute,
        }
    }

    pub fn modified_if(changed: bool) -> Effect {
        if changed {
            Effect::MODIFIED
        } else {
            Effect::UNCHANGED
        }
    }
}

/// The parent's rows by group as two flat arrays, as [`Aggregator::initial`] has them
/// before any row set is made: the rows of group `slot` are
/// `row_keys[starts[slot]..starts[slot + 1]]`, increasing.
///
/// [`Aggregator::initial`]: super::aggregator::Aggregator::initial
pub struct FlatGroups {
    row_keys: Vec<RowKey>,
    starts: Vec<usize>,
}

impl FlatGroups {
    /// Brings `rows` together by group, each group's rows increasing: the row at
    /// position `i` of `rows` is in group `slots[i]`, and `counts[slot]` is the number of
    /// rows of group `slot`. A counting sort.
    pub fn new(counts: &[u64], slots: &[u32], rows: &RowSet) -> FlatGroups {
        let mut starts = Vec::with_capacity(counts.len() + 1);
        let mut total = 0usize;
        starts.push(0);
        for &count in counts {
            total += count as usize;
            starts.push(total);
        }
        assert_eq!(
            total,
            slots.len(),
            "the row counts of the groups are of the rows"
        );
        let mut next: Vec<usize> = starts[..counts.len()].to_vec();
        let mut sorted = vec![0; total];
        let mut slots = slots.iter();
        for range in rows.ranges() {
            for (row_key, &slot) in (range.first()..=range.last()).zip(&mut slots) {
                let at = &mut next[slot as usize];
                sorted[*at] = row_key;
                *at += 1;
            }
        }
        FlatGroups {
            row_keys: sorted,
            starts,
        }
    }

    /// The rows of group `slot`, increasing.
    pub fn of(&self, slot: usize) -> &[RowKey] {
        &self.row_keys[self.starts[slot]..self.starts[slot + 1]]
    }
}

/// The rows of each group and the operator's input column, for [`AggOperator::recompute`].
pub struct GroupReader<'a> {
    rows: &'a [RowSet],
    /// The rows of each group while the first result is made, when `rows` are not yet.
    flat: Option<&'a FlatGroups>,
    input: Option<&'a dyn ColumnSource>,
    keys: &'a [ResultColumn],
}

impl<'a> GroupReader<'a> {
    pub(super) fn new(
        rows: &'a [RowSet],
        input: Option<&'a dyn ColumnSource>,
        keys: &'a [ResultColumn],
    ) -> Self {
        GroupReader {
            rows,
            flat: None,
            input,
            keys,
        }
    }

    /// The reader with the rows of each group read from `flat` instead of the row sets.
    pub(super) fn with_flat(self, flat: Option<&'a FlatGroups>) -> Self {
        GroupReader { flat, ..self }
    }

    /// The values of key column `index` of the aggregation, one per group: the key of
    /// group `slot` is at row key `slot`. Read current values only.
    ///
    /// # Panics
    /// If the aggregation has no such key column.
    pub fn key(&self, index: usize) -> &'a dyn ColumnSource {
        self.keys[index].as_source()
    }

    /// The parent's rows that are in group `slot` now, in the parent's current key space.
    /// While the first result is made this builds the row set; an operator that wants
    /// the keys, the ends or whether there are any asks for those.
    pub fn rows(&self, slot: usize) -> Cow<'a, RowSet> {
        match self.flat {
            Some(flat) => Cow::Owned(RowSet::from_keys(flat.of(slot).iter().copied())),
            None => Cow::Borrowed(&self.rows[slot]),
        }
    }

    /// Appends the row keys of group `slot`, increasing, to `out`.
    pub fn extend_row_keys(&self, slot: usize, out: &mut Vec<RowKey>) {
        match self.flat {
            Some(flat) => out.extend_from_slice(flat.of(slot)),
            None => out.extend(self.rows[slot].iter()),
        }
    }

    /// Whether group `slot` has no rows.
    pub fn is_empty(&self, slot: usize) -> bool {
        match self.flat {
            Some(flat) => flat.of(slot).is_empty(),
            None => self.rows[slot].is_empty(),
        }
    }

    /// The first row key of group `slot`, if it has rows.
    pub fn first_row_key(&self, slot: usize) -> Option<RowKey> {
        match self.flat {
            Some(flat) => flat.of(slot).first().copied(),
            None => self.rows[slot].first_row_key(),
        }
    }

    /// The last row key of group `slot`, if it has rows.
    pub fn last_row_key(&self, slot: usize) -> Option<RowKey> {
        match self.flat {
            Some(flat) => flat.of(slot).last().copied(),
            None => self.rows[slot].last_row_key(),
        }
    }

    /// The operator's input column. Read current values only.
    ///
    /// # Panics
    /// If the operator has no input column.
    pub fn input(&self) -> &'a dyn ColumnSource {
        self.input.expect("the operator has no input column")
    }
}

/// The added rows of one chunk in row order, not brought together by group: what
/// [`AggOperator::add_rows`] is given.
pub struct Rows<'a> {
    /// The slot each row joins.
    pub slots: &'a [usize],
    /// Each of those slots once, in the order of its first row.
    pub distinct: &'a [usize],
    /// Where each row's slot is in `distinct`: a number below `distinct.len()` that an
    /// operator can keep what a chunk brought each slot by, in an array as small as the
    /// chunk.
    pub groups: &'a [u32],
    /// The row key of each row, increasing.
    pub row_keys: &'a [RowKey],
    /// The chunk of the operator's input column; `None` for an operator without one.
    pub values: Option<&'a Values>,
}

/// One aggregation of one input column into one result column, for every group.
///
/// A group is a slot: a small integer that is stable for the life of the result. The
/// framework finds each row's slot, brings the rows of a slot within a chunk together, and
/// calls the operator once per slot and chunk, so an operator never sees keys and never
/// hashes. It then writes the results of the slots the operator said had changed.
///
/// An operator states what it needs:
///
/// - Running state that can retract (sum, count, avg) implements `add` and `remove` and
///   nothing else.
/// - State that a removal can invalidate (min, max, first, last on a table that removes
///   rows) returns `true` from `needs_group_rows` and [`Effect::RECOMPUTE`] when it cannot
///   answer from its state; the framework then keeps the rows of every group and calls
///   `recompute`. If no operator of an aggregation needs them, group rows are not kept.
///
/// Calls arrive in the order of an update: removed rows with their previous values, then
/// modified rows, then added rows. A slot that has lost all its rows may get rows again,
/// and must then behave as a new one.
///
/// Behavior: io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator. The
/// per-type Java operators are one generic operator here; `requiresRowKeys` is
/// `needs_group_rows`, and the row-set bookkeeping Java leaves to each such operator is
/// done once by the framework.
pub trait AggOperator: Send {
    /// The type of the result column.
    fn result_type(&self) -> ColumnType;

    /// Whether the framework must keep the rows of each group for this operator.
    fn needs_group_rows(&self) -> bool {
        false
    }

    /// Whether Java's result column holds a value for each group, as the array sources of
    /// most operators do. The others read the rows of the parent that a group names; a
    /// static `select` copies such a column instead of passing it through.
    ///
    /// Behavior: whether the operator's `resultColumns` are an
    /// io.deephaven.engine.table.impl.sources.InMemoryColumnSource
    fn holds_results(&self) -> bool {
        true
    }

    /// Makes slots `0..slots` usable. New slots start empty.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#ensureCapacity
    fn ensure_slots(&mut self, slots: usize);

    /// Rows that joined `slot`.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#addChunk
    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect;

    /// Rows that left `slot`, with the values and row keys they had before this cycle.
    /// Never called for a static, add-only or blink parent.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#removeChunk
    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect;

    /// [`AggOperator::add`] for every group of a chunk, in order, appending the effect on
    /// each to `effects`. An operator whose work per group is small takes them all in one
    /// call; the default calls `add` once per group.
    fn add_runs(&mut self, runs: &Runs<'_>, effects: &mut Vec<Effect>) {
        effects.extend(runs.iter().map(|(slot, run)| self.add(slot, &run)));
    }

    /// Whether the operator takes the added rows of a chunk in row order
    /// ([`AggOperator::add_rows`]), without the rows of a group brought together. When
    /// every operator of an aggregation does, the framework does not group the added
    /// rows of a chunk by slot.
    fn adds_rows(&self) -> bool {
        false
    }

    /// The added rows of one chunk, in row order. Notes the effect on each slot in
    /// `marks`; the state and the marks afterwards must be what [`AggOperator::add_runs`]
    /// leaves for the same rows grouped, so an operator whose result depends on which
    /// rows of a group came in one chunk (a sum) keeps what the chunk brought each slot
    /// apart until the chunk's last row. Only called when [`AggOperator::adds_rows`] says
    /// so.
    fn add_rows(&mut self, rows: &Rows<'_>, marks: &mut Marks) {
        let _ = (rows, marks);
        unreachable!("add_rows of an operator that does not take rows in row order");
    }

    /// [`AggOperator::remove`] for every group of a chunk, as
    /// [`AggOperator::add_runs`] is for `add`.
    fn remove_runs(&mut self, runs: &Runs<'_>, effects: &mut Vec<Effect>) {
        effects.extend(runs.iter().map(|(slot, run)| self.remove(slot, &run)));
    }

    /// Rows of `slot` whose input value may have changed: `old` and `new` are the same rows
    /// before and after. Only called when the parent reported the input column modified.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#modifyChunk, whose default is also a
    /// remove followed by an add.
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let removed = self.remove(slot, old);
        removed.and(self.add(slot, new))
    }

    /// Brings the state of `slots` up to date from the rows of each group. Returns, for
    /// each, whether its result changed. Called for the slots that asked for it, after the
    /// cycle's rows have all been processed.
    fn recompute(
        &mut self,
        slots: &[usize],
        _groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        Ok(vec![false; slots.len()])
    }

    /// The rows `before` of group `slot` are now the rows `after`, in the same order. Only
    /// called for an operator that [needs group rows](AggOperator::needs_group_rows).
    ///
    /// Behavior: IterativeChunkedAggregationOperator#shiftChunk
    fn shifted(&mut self, _slot: usize, _before: &RowSet, _after: &RowSet) {}

    /// A failure of the calls since this was last asked, for an operator that reads the
    /// parent itself while it is given rows. The framework asks after every chunk and
    /// fails the result with it.
    fn take_failure(&mut self) -> Option<TableFailure> {
        None
    }

    /// What a group without rows that the first result shows (one kept by
    /// `preserve_empty`, which is then the only group of an aggregation without keys)
    /// does to the operator, which was given no rows for it.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#propagateInitialState
    fn shown_empty(&mut self, _slot: usize) -> Effect {
        Effect::UNCHANGED
    }

    /// Tells the operator that `slot` is an initial group, before any row is given. Java
    /// gives every operator an empty chunk for each initial group, through the path of an
    /// aggregation without keys; this is for an operator that an empty chunk changes.
    ///
    /// Behavior: the `doIndexedAddition` of
    /// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#initialKeyTableAddition,
    /// which calls the singleton IterativeChunkedAggregationOperator#addChunk
    fn seeded(&mut self, _slot: usize) {}

    /// Whether a change of this operator's results puts its column into the modified
    /// column set of the result's update.
    ///
    /// Behavior: the `UnaryOperator<ModifiedColumnSet>` an operator returns from
    /// IterativeChunkedAggregationOperator#initializeRefreshing, which names the result
    /// column for every operator here but a formula that reads no column
    fn names_result(&self) -> bool {
        true
    }

    /// The results of `slots`, as an array of `result_type`.
    fn results(&self, slots: &[usize]) -> ArrayRef;

    /// The parent's change of the cycle that begins, before any row of it is given. For
    /// an operator that passes parts of it on.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#resetForStep
    fn begin_cycle(&mut self, _upstream: &TableUpdate) {}

    /// Whether a group whose rows were shifted, and nothing else, is given to
    /// [`AggOperator::recompute`].
    fn recomputes_shifted(&self) -> bool {
        false
    }

    /// The column the result shows, given the column the framework writes this operator's
    /// results to. For an operator whose results are owned by something that must live
    /// as long as the column.
    fn result_column(&self, written: Arc<dyn ColumnSource>) -> Arc<dyn ColumnSource> {
        written
    }

    /// The result table has failed.
    ///
    /// Behavior: IterativeChunkedAggregationOperator#propagateFailure
    fn failed(&mut self, _failure: &TableFailure) {}
}

pub(super) const MODIFIED: u8 = 1;
pub(super) const RECOMPUTE: u8 = 2;

/// What one operator said about each slot during the current cycle.
#[derive(Default)]
pub struct Marks {
    state: Vec<u8>,
    /// The slots with a non-zero state.
    touched: Vec<usize>,
    /// Whether [`MODIFIED`] is not noted: while the first result is made, every slot is
    /// written whatever its marks.
    recompute_only: bool,
}

impl Marks {
    /// Notes `effect` on `slot`. Whether that changed what is noted of the slot.
    #[inline]
    pub fn note(&mut self, slot: usize, effect: Effect) -> bool {
        let bits = if effect.modified { MODIFIED } else { 0 }
            | if effect.recompute { RECOMPUTE } else { 0 };
        if bits == 0 || (self.recompute_only && !effect.recompute) {
            return false;
        }
        if self.state.len() <= slot {
            self.state.resize(slot + 1, 0);
        }
        let before = self.state[slot];
        if before == 0 {
            self.touched.push(slot);
        }
        self.state[slot] = before | bits;
        before | bits != before
    }

    /// The slots marked with `bit`, increasing.
    pub(super) fn marked(&self, bit: u8) -> Vec<usize> {
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
    pub(super) fn any(&self, bit: u8) -> bool {
        self.touched.iter().any(|&slot| self.state[slot] & bit != 0)
    }

    /// Makes [`Marks::note`] of slots below `slots` not grow the marks.
    pub(super) fn reserve(&mut self, slots: usize) {
        if self.state.len() < slots {
            self.state.resize(slots, 0);
        }
    }

    /// Whether only [`RECOMPUTE`] is noted from now on.
    pub(super) fn note_recompute_only(&mut self, on: bool) {
        self.recompute_only = on;
    }

    pub(super) fn clear(&mut self) {
        for slot in self.touched.drain(..) {
            self.state[slot] = 0;
        }
    }
}
