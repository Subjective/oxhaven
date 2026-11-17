//! Formula: a formula evaluated over the values of each group, given as vectors.

use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{failure, Run};
use crate::ops::select::ArrayFormula;
use arrow::array::{new_null_array, ArrayRef, ListArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::collections::HashMap;
use std::sync::Arc;

/// Where a formula aggregation takes the values of one of its inputs.
pub enum FormulaSource {
    /// A column of the parent that is not grouped by: the formula sees the values in the
    /// rows of the group, in the order of the parent's row set, as a vector of the
    /// column's type.
    Grouped {
        source: Arc<dyn ColumnSource>,
        column_type: ColumnType,
    },
    /// The key column of the aggregation with this index: the formula sees the group's key.
    Key(usize),
    /// One value for every group: the array holds it as its one element.
    Constant(ArrayRef),
    /// An input the formula does not read, of this Arrow type. A formula that reads
    /// nothing is given one, so that there is an array to tell the number of groups by.
    Unread(DataType),
}

/// A column that a formula aggregation may read and the parent does not have: one value,
/// the same for every group, which takes the place of a column of the parent of its name.
///
/// Behavior: the extra column definitions that
/// io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#prepareFormula adds to
/// the table's (`maybeInitializeVectorColumns`), read as key columns
#[derive(Clone, Debug)]
pub struct FormulaConstant {
    pub name: String,
    pub column_type: ColumnType,
    /// The value, as an array of one element of the type.
    pub value: ArrayRef,
}

/// The value of a formula for each group.
///
/// The formula is evaluated for a group when the first result is made, and again whenever
/// a row joins or leaves the group or a row that stays in it is modified in a column the
/// formula reads as a vector. The group's result row is then reported as modified, whether
/// or not the value differs; a shift of the parent's rows is not reported. A group that
/// has lost its rows is evaluated, over empty vectors, only if `preserve_empty` keeps its
/// row. A formula that fails fails the result.
///
/// Java evaluates every modified row of the result again, also one that only another
/// aggregation changed. A formula gives the same value for the same vectors, so that
/// cannot be seen.
///
/// Behavior: io.deephaven.engine.table.impl.by.FormulaChunkedOperator and
/// io.deephaven.engine.table.impl.by.FormulaMultiColumnChunkedOperator
/// (`propagateInitialState` and `propagateUpdates` copy the formula's values for the
/// result's added and modified rows), each over a
/// io.deephaven.engine.table.impl.by.GroupByChunkedOperator of its inputs, whose `addChunk`,
/// `removeChunk` and `modifyRowKeys` report the group modified
pub struct FormulaAgg {
    formula: ArrayFormula,
    /// One per input of `formula`, in its order.
    sources: Vec<FormulaSource>,
    evaluate_empty: bool,
    names_result: bool,
    /// The values the last [`AggOperator::recompute`] evaluated, and where each group's is.
    values: ArrayRef,
    places: HashMap<usize, usize>,
}

impl FormulaAgg {
    /// `formula` over `sources`, one for each of its inputs in their order.
    /// `evaluate_empty` says that a group without rows keeps its result row. A formula
    /// with `names_result` unset never has its column among the modified columns.
    pub(crate) fn new(
        formula: ArrayFormula,
        sources: Vec<FormulaSource>,
        evaluate_empty: bool,
        names_result: bool,
    ) -> FormulaAgg {
        FormulaAgg {
            values: new_null_array(&formula.result_type().arrow_type(), 0),
            formula,
            sources,
            evaluate_empty,
            names_result,
            places: HashMap::new(),
        }
    }
}

impl AggOperator for FormulaAgg {
    /// Behavior: FormulaChunkedOperator#FormulaChunkedOperator and
    /// FormulaMultiColumnChunkedOperator#FormulaMultiColumnChunkedOperator, which make the
    /// result column of the formula's `getReturnedType`
    fn result_type(&self) -> ColumnType {
        self.formula.result_type().clone()
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

    fn shown_empty(&mut self, _slot: usize) -> Effect {
        Effect::RECOMPUTE
    }

    fn names_result(&self) -> bool {
        self.names_result
    }

    /// Evaluates the formula for the groups. Every one of them counts as changed, also one
    /// that is not evaluated because its row is leaving the result: Java reports a group
    /// whose rows came, went or were modified without comparing values.
    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        let evaluated: Vec<usize> = slots
            .iter()
            .copied()
            .filter(|&slot| self.evaluate_empty || !groups.rows(slot).is_empty())
            .collect();
        let mut row_keys: Vec<RowKey> = Vec::new();
        let mut lengths: Vec<usize> = Vec::with_capacity(evaluated.len());
        for &slot in &evaluated {
            let before = row_keys.len();
            row_keys.extend(groups.rows(slot).iter());
            lengths.push(row_keys.len() - before);
        }
        // The vectors of one input are one Arrow list array, whose offsets are 32 bits wide.
        if i32::try_from(row_keys.len()).is_err() {
            return Err(failure(
                "formula: more than 2^31 - 1 rows in the groups that changed in one cycle",
            ));
        }
        let slot_keys: Vec<RowKey> = evaluated.iter().map(|&slot| slot as RowKey).collect();
        let mut inputs: Vec<ArrayRef> = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            inputs.push(match source {
                FormulaSource::Grouped {
                    source,
                    column_type,
                } => {
                    let elements = source.chunk_by_keys(&row_keys, false)?;
                    let DataType::List(field) =
                        ColumnType::vector(column_type.clone()).arrow_type()
                    else {
                        unreachable!("a vector is an Arrow list");
                    };
                    let offsets = OffsetBuffer::from_lengths(lengths.iter().copied());
                    Arc::new(ListArray::new(field, offsets, elements, None))
                }
                FormulaSource::Key(index) => groups.key(*index).chunk_by_keys(&slot_keys, false)?,
                FormulaSource::Constant(value) => {
                    let first = UInt32Array::from(vec![0; evaluated.len()]);
                    arrow::compute::take(&**value, &first, None)
                        .map_err(|error| failure(format!("formula: {error}")))?
                }
                FormulaSource::Unread(data_type) => new_null_array(data_type, evaluated.len()),
            });
        }
        self.values = if evaluated.is_empty() {
            new_null_array(&self.formula.result_type().arrow_type(), 0)
        } else {
            self.formula.evaluate(&inputs)?
        };
        self.places = evaluated
            .iter()
            .enumerate()
            .map(|(place, &slot)| (slot, place))
            .collect();
        Ok(vec![true; slots.len()])
    }

    /// The values of `slots`. A slot the last `recompute` did not evaluate has no row in
    /// the result, and its cell is a null.
    fn results(&self, slots: &[usize]) -> ArrayRef {
        // One null after the values, for the slots without one.
        let absent = self.values.len();
        let nulls = new_null_array(self.values.data_type(), 1);
        let values = arrow::compute::concat(&[&*self.values, &*nulls])
            .expect("two arrays of the result type");
        let positions: UInt32Array = slots
            .iter()
            .map(|slot| self.places.get(slot).copied().unwrap_or(absent) as u32)
            .collect();
        arrow::compute::take(&values, &positions, None).expect("positions within the values")
    }
}
