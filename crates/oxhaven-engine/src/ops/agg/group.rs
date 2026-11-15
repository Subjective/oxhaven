//! Group: the values of a column in the rows of each group, as one vector per group.

use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{failure, Run};
use arrow::array::{new_empty_array, ArrayRef, ListArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::collections::HashMap;
use std::sync::Arc;

/// The values of the input column in the rows of each group, in the order of the parent's
/// row set, as a vector of the input column's type. Nulls are elements like any other, and
/// a group without rows (one kept by `preserve_empty`) is an empty vector, never a null.
///
/// A group's vector is read again from the group's rows whenever a row joins or leaves the
/// group, or a row that stays in it is modified in the input column, and the group's
/// result row is then reported as modified whether or not the values differ. A shift of
/// the parent's rows moves no value and is not reported.
///
/// Java's result column is a view of the input column through the row set of each group
/// (`AggregateColumnSource`), read when someone asks. Here the vectors are stored, like
/// every other aggregation result; they hold the same values at the same times.
///
/// Behavior: io.deephaven.engine.table.impl.by.GroupByChunkedOperator (`addChunk`,
/// `removeChunk` and `modifyRowKeys` report the group modified; `shiftChunk` reports
/// nothing without an exposed row set column; `propagateInitialState` gives a group
/// without rows an empty row set) over
/// io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource#make
pub struct Group {
    input: Arc<dyn ColumnSource>,
    result_type: ColumnType,
    /// The elements of the groups read by the last [`AggOperator::recompute`], group
    /// after group.
    elements: ArrayRef,
    /// Where each of those groups is in `elements`: its first element and its length.
    spans: HashMap<usize, (usize, usize)>,
}

impl Group {
    /// The group aggregation of column `input`, whose type is `input_type`.
    pub fn new(input: Arc<dyn ColumnSource>, input_type: &ColumnType) -> Group {
        Group {
            elements: new_empty_array(&input_type.arrow_type()),
            input,
            result_type: ColumnType::vector(input_type.clone()),
            spans: HashMap::new(),
        }
    }
}

impl AggOperator for Group {
    /// Behavior: AggregateColumnSource#make, by way of
    /// io.deephaven.vector.VectorFactory#forElementType: the vector of the element type.
    fn result_type(&self) -> ColumnType {
        self.result_type.clone()
    }

    /// Behavior: GroupByChunkedOperator#getResultColumns, each an
    /// io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource
    fn holds_results(&self) -> bool {
        false
    }

    fn needs_group_rows(&self) -> bool {
        true
    }

    fn ensure_slots(&mut self, _slots: usize) {}

    fn add(&mut self, _slot: usize, _run: &Run<'_>) -> Effect {
        Effect::RECOMPUTE
    }

    fn remove(&mut self, _slot: usize, _run: &Run<'_>) -> Effect {
        Effect::RECOMPUTE
    }

    fn modify(&mut self, _slot: usize, _old: &Run<'_>, _new: &Run<'_>) -> Effect {
        Effect::RECOMPUTE
    }

    /// Reads the groups again. Every one of them counts as changed: Java reports a group
    /// whose rows came, went or were modified without comparing its values.
    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        let mut row_keys: Vec<RowKey> = Vec::new();
        self.spans.clear();
        for &slot in slots {
            let first = row_keys.len();
            row_keys.extend(groups.rows(slot).iter());
            self.spans.insert(slot, (first, row_keys.len() - first));
        }
        // The vectors written in one cycle are one Arrow list array, whose offsets are 32
        // bits wide.
        if i32::try_from(row_keys.len()).is_err() {
            return Err(failure(
                "group: more than 2^31 - 1 rows in the groups that changed in one cycle",
            ));
        }
        self.elements = self.input.chunk_by_keys(&row_keys, false)?;
        Ok(vec![true; slots.len()])
    }

    /// The vectors of `slots`. A slot that was not read by the last `recompute` has had no
    /// rows yet, and its vector is empty.
    fn results(&self, slots: &[usize]) -> ArrayRef {
        let spans: Vec<(usize, usize)> = slots
            .iter()
            .map(|slot| self.spans.get(slot).copied().unwrap_or((0, 0)))
            .collect();
        let positions: UInt32Array = spans
            .iter()
            .flat_map(|&(first, length)| (first..first + length).map(|position| position as u32))
            .collect();
        let elements = arrow::compute::take(&self.elements, &positions, None)
            .expect("positions within the elements read");
        let DataType::List(field) = self.result_type.arrow_type() else {
            unreachable!("a vector is an Arrow list");
        };
        let offsets = OffsetBuffer::from_lengths(spans.iter().map(|&(_, length)| length));
        Arc::new(ListArray::new(field, offsets, elements, None))
    }
}
