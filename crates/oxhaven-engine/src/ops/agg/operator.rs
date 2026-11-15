//! The interface between the aggregation framework and one aggregation.

use super::values::Run;
use arrow::array::ArrayRef;
use oxhaven_column::WritableSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
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

/// The rows of each group and the operator's input column, for [`AggOperator::recompute`].
pub struct GroupReader<'a> {
    rows: &'a [RowSet],
    input: Option<&'a dyn ColumnSource>,
    keys: &'a [Arc<WritableSource>],
}

impl<'a> GroupReader<'a> {
    pub(super) fn new(
        rows: &'a [RowSet],
        input: Option<&'a dyn ColumnSource>,
        keys: &'a [Arc<WritableSource>],
    ) -> Self {
        GroupReader { rows, input, keys }
    }

    /// The values of key column `index` of the aggregation, one per group: the key of
    /// group `slot` is at row key `slot`. Read current values only.
    ///
    /// # Panics
    /// If the aggregation has no such key column.
    pub fn key(&self, index: usize) -> &'a dyn ColumnSource {
        &*self.keys[index]
    }

    /// The parent's rows that are in group `slot` now, in the parent's current key space.
    pub fn rows(&self, slot: usize) -> &'a RowSet {
        &self.rows[slot]
    }

    /// The operator's input column. Read current values only.
    ///
    /// # Panics
    /// If the operator has no input column.
    pub fn input(&self) -> &'a dyn ColumnSource {
        self.input.expect("the operator has no input column")
    }
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
}
