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
        // Asked for in the order they were read, as the framework does after a recompute,
        // the vectors are one stretch of the elements, which is used as it is.
        let start = spans
            .iter()
            .find(|&&(_, length)| length > 0)
            .map_or(0, |s| s.0);
        let mut end = start;
        let in_order = spans.iter().all(|&(first, length)| {
            let next = length == 0 || first == end;
            end += length;
            next
        });
        let elements = if in_order {
            self.elements.slice(start, end - start)
        } else {
            let positions: UInt32Array = spans
                .iter()
                .flat_map(|&(first, length)| {
                    (first..first + length).map(|position| position as u32)
                })
                .collect();
            arrow::compute::take(&self.elements, &positions, None)
                .expect("positions within the elements read")
        };
        let DataType::List(field) = self.result_type.arrow_type() else {
            unreachable!("a vector is an Arrow list");
        };
        let offsets = OffsetBuffer::from_lengths(spans.iter().map(|&(_, length)| length));
        Arc::new(ListArray::new(field, offsets, elements, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use oxhaven_column::FlatSource;
    use oxhaven_contracts::rowset::RowSet;

    /// The vectors of `slots` as lists of values.
    fn lists(group: &Group, slots: &[usize]) -> Vec<Vec<Option<i32>>> {
        let results = group.results(slots);
        let lists = results
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("a list");
        (0..lists.len())
            .map(|index| {
                let values = lists.value(index);
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("ints")
                    .iter()
                    .collect()
            })
            .collect()
    }

    #[test]
    fn vectors_asked_for_in_any_order_hold_their_group_s_values() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(12),
            Some(13),
            Some(14),
        ]));
        let input: Arc<dyn ColumnSource> =
            Arc::new(FlatSource::new(ColumnType::Int, values).expect("a column"));
        let mut group = Group::new(input.clone(), &ColumnType::Int);
        // Group 0 has rows 1 and 3, group 1 none, group 2 rows 0, 2 and 4; group 3 is
        // never read.
        let rows = [
            RowSet::from_keys([1, 3]),
            RowSet::empty(),
            RowSet::from_keys([0, 2, 4]),
            RowSet::empty(),
        ];
        let reader = GroupReader::new(&rows, Some(&*input), &[]);
        group.recompute(&[0, 1, 2], &reader).expect("the groups");
        let (zero, empty, two) = (
            vec![None, Some(13)],
            vec![],
            vec![Some(10), Some(12), Some(14)],
        );
        // In the order read, from the first group read, from a later one, and in other
        // orders, with groups without rows and a group never read among them.
        assert_eq!(
            lists(&group, &[0, 1, 2]),
            [zero.clone(), empty.clone(), two.clone()]
        );
        assert_eq!(
            lists(&group, &[1, 2, 3]),
            [empty.clone(), two.clone(), empty.clone()]
        );
        assert_eq!(lists(&group, &[2]), std::slice::from_ref(&two));
        assert_eq!(lists(&group, &[2, 0]), [two.clone(), zero.clone()]);
        assert_eq!(lists(&group, &[0, 0]), [zero.clone(), zero.clone()]);
        assert_eq!(lists(&group, &[3, 1]), [empty.clone(), empty]);
        assert_eq!(
            group.results(&[0, 1, 2]).data_type(),
            &ColumnType::vector(ColumnType::Int).arrow_type()
        );
    }
}
