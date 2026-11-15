//! Rolling group: each row's result is the vector of the input values over the row's window.
//!
//! Java's result column is a view: it keeps, per row, the bucket's row set and where the
//! window starts and ends relative to the row's position in it, and reads the input column
//! through that slice when asked. Here the vectors are stored, like every other result of
//! `update_by` and like the vectors of the group aggregation; they are written again for
//! every row whose slice or whose values in it changed.

use super::failure;
use super::rolling::{Slide, Ticks};
use arrow::array::{ArrayRef, ListArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::compute::take;
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
    lists(input.column_type(), lengths, input.chunk_by_keys(&keys, false)?)
}

/// A vector column of cells of `lengths` elements, taken from `elements` in order.
fn lists(
    element: &ColumnType,
    lengths: Vec<usize>,
    elements: ArrayRef,
) -> Result<ArrayRef, TableFailure> {
    let DataType::List(field) = output_type(element).arrow_type() else {
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

/// One vector per slide of a window in time: the values of the rows in the window, of
/// `influencers`, the input values of the timestamped influencer rows in order. A window
/// that holds no row gives an empty vector. Rows without a timestamp are in no vector,
/// wherever they stand: the positions Java keeps for the first and the last row of a window
/// are positions among the bucket's rows that have a timestamp, and the result column
/// slices that row set.
///
/// Behavior: RollingGroupOperator.Context#push, #pop and #writeToOutputChunk and
/// RollingGroupOperator#initializeRolling, with the `timestampValidRowSet` that
/// io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingBase#processWindowBucketOperatorSet
/// hands them as the bucket's row set, and SlicedIntAggregateColumnSource#get
pub(super) fn windows(
    element: &ColumnType,
    influencers: &ArrayRef,
    slides: &[Slide],
) -> Result<ArrayRef, TableFailure> {
    let (mut head, mut tail) = (0, 0);
    let mut lengths = Vec::with_capacity(slides.len());
    let mut indices: Vec<u32> = Vec::new();
    for slide in slides {
        head += slide.pop;
        tail += slide.push;
        lengths.push(tail.saturating_sub(head));
        indices.extend((head..tail).map(|index| index as u32));
        if i32::try_from(indices.len()).is_err() {
            return Err(failure(
                "rolling group: more than 2^31 - 1 values in the windows computed at once",
            ));
        }
    }
    let elements = take(influencers.as_ref(), &UInt32Array::from(indices), None)
        .map_err(|error| failure(error.to_string()))?;
    lists(element, lengths, elements)
}
