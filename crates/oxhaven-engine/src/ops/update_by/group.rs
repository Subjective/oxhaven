//! Rolling group: each row's result is the vector of the input values over the row's window.
//!
//! Java's result column is a view: it keeps, per row, the bucket's row set and where the
//! window starts and ends relative to the row's position in it, and reads the input column
//! through that slice when asked. Here the vectors are stored, like every other result of
//! `update_by` and like the vectors of the group aggregation; they are written again for
//! every row whose slice or whose values in it changed.

use super::failure;
use super::rolling::{Slide, Stamped, Ticks, Time};
use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::Arc;

/// The type of the result column for an input column of type `input`: the vector of that
/// type, whatever it is.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.rollinggroup.RollingGroupOperator#initializeSources,
/// which makes each result with
/// io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource#makeSliced
pub(super) fn output_type(input: &ColumnType) -> ColumnType {
    ColumnType::vector(input.clone())
}

/// One vector per row set of `slices`: the values of `input` at its rows, in order. A row
/// set without rows gives an empty vector, never a null.
///
/// Behavior: `get` and `fillChunk` of
/// io.deephaven.engine.table.impl.sources.aggregate.SlicedIntAggregateColumnSource and its
/// siblings, for a row that has a slice
pub(super) fn vectors(
    input: &dyn ColumnSource,
    slices: impl IntoIterator<Item = RowSet>,
) -> Result<ArrayRef, TableFailure> {
    let mut keys: Vec<RowKey> = Vec::new();
    let mut lengths: Vec<usize> = Vec::new();
    for slice in slices {
        keys.extend(slice.iter());
        lengths.push(slice.size() as usize);
    }
    // The vectors written at once are one Arrow list array, whose offsets are 32 bits wide.
    if i32::try_from(keys.len()).is_err() {
        return Err(failure(
            "rolling group: more than 2^31 - 1 values in the windows computed at once",
        ));
    }
    let elements = input.chunk_by_keys(&keys, false)?;
    let DataType::List(field) = output_type(input.column_type()).arrow_type() else {
        unreachable!("a vector is an Arrow list");
    };
    let lists = ListArray::try_new(field, OffsetBuffer::from_lengths(lengths), elements, None)
        .map_err(|error| failure(error.to_string()))?;
    Ok(Arc::new(lists))
}

impl Ticks {
    /// The rows of the bucket `rows` in the window of the row at `position`: the positions
    /// from `position - rev + 1` to `position + fwd`, cut to the bucket.
    ///
    /// Behavior: the constant offsets `-reverseWindowScaleUnits + 1` and
    /// `forwardWindowScaleUnits + 1` that RollingGroupOperator#initializeSources gives the
    /// result column, and the clamps of SlicedIntAggregateColumnSource#get. A window that
    /// holds no row gives an empty vector; the operator's `accumulateRolling` returns before
    /// it pops anything, so the assertion that fails the other rolling operators on a
    /// window of size zero is not reached.
    pub(super) fn slice(self, rows: &RowSet, position: i64) -> RowSet {
        let size = rows.size() as i64;
        let start = (position - self.rev + 1).clamp(0, size);
        let end = (position + self.fwd + 1).clamp(0, size);
        rows.sub_set_by_position_range(start as u64, end as u64)
    }
}

/// For each slide of a window in time over the timestamped `influencers`, the rows of the
/// bucket `rows` from the first row in the window to the last. Rows without a timestamp
/// that lie between two rows of the window are among them: Java keeps the positions of the
/// first and the last row of the window in the bucket, and the vector is every row from one
/// to the other. A window that holds no row gives no rows.
///
/// Behavior: RollingGroupOperator.Context#push, #pop and #writeToOutputChunk, whose
/// `startPos` and `endPos` are positions in the bucket's row set, and
/// SlicedIntAggregateColumnSource#get, which takes that range of positions
pub(super) fn time_slices(rows: &RowSet, influencers: &RowSet, slides: &[Slide]) -> Vec<RowSet> {
    let keys: Vec<RowKey> = influencers.iter().collect();
    let (mut head, mut tail) = (0, 0);
    slides
        .iter()
        .map(|slide| {
            head += slide.pop;
            tail += slide.push;
            if head < tail {
                rows.sub_set_by_key_range(keys[head], keys[tail - 1])
            } else {
                RowSet::empty()
            }
        })
        .collect()
}

impl Stamped {
    /// The timestamped rows whose vector, over a window in time, holds a row without a
    /// timestamp that stands after `before` timestamped rows of the bucket: those whose
    /// window holds both the timestamped row before it and the one after it. None if it has
    /// no timestamped row on either side.
    ///
    /// No Java counterpart, and a difference from Java: such a row is in no window, so
    /// io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTime#computeAffectedRowsAndOperators
    /// recomputes nothing when one is added, removed or modified. Java's vectors around it
    /// are views and change all the same: they show its new value without reporting a
    /// modified row, and when it is added or removed the unchanged start and end offsets
    /// of the rows after it select other rows than their windows hold.
    pub(super) fn holding_gap(&self, before: usize, time: Time) -> RowSet {
        if before == 0 || before >= self.stamps().len() {
            return RowSet::empty();
        }
        let stamps = self.stamps();
        let first = stamps[before].wrapping_sub(time.fwd);
        let last = stamps[before - 1].wrapping_add(time.rev);
        let start = stamps.partition_point(|s| *s < first);
        let end = stamps.partition_point(|s| *s <= last);
        RowSet::from_keys(self.keys()[start..end.max(start)].iter().copied())
    }

    /// How many timestamped rows have a key below `key`.
    pub(super) fn count_before(&self, key: RowKey) -> usize {
        self.keys().partition_point(|k| *k < key)
    }
}
