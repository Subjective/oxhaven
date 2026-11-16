//! Column arrays: `Col_`, the whole of a column as a vector, and what a formula does with
//! one.
//!
//! Java gives a formula that names `Col_` a vector over the table's rows, in row order,
//! which reads the column when it is asked for a value. Here a column array is a vector
//! value like the cell of a vector column, with the same methods and functions, and three
//! ways to avoid holding one copy of the column per row:
//!
//! - `Col_[index]` and `Col_.get(index)` read the column at the rows the indexes name and
//!   at no other;
//! - a part of a formula that is made of column arrays and literals alone has one value
//!   for every row, and is evaluated for one row;
//! - anything else, such as `Col_.subVector(i, i + 2)`, gets the column once per row, and
//!   is refused when that is more than a list array can hold.

use super::{arrow_failure, failure, without_null_values, Batch};
use crate::functions::vectors::VectorFn;
use crate::functions::Kernel;
use crate::types::{Node, Typed};
use arrow::array::{new_empty_array, Array, ArrayRef, AsArray, Int64Array, ListArray, UInt32Array};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{Field, Int64Type};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::{Arc, OnceLock};

/// One column of the table a formula is evaluated against, as the formula's `Col_`.
///
/// Behavior: the vector `io.deephaven.engine.table.vectors.ColumnVectors#of` makes for
/// `DhFormulaColumn#generateConstructor`, over the table's row set, and over the previous
/// row set and previous values for a previous-value read.
pub(super) struct ColumnArray {
    source: Arc<dyn ColumnSource>,
    /// The table's rows, in the order of the vector.
    rows: Arc<RowSet>,
    use_prev: bool,
    /// Every value, read at most once per evaluation.
    whole: OnceLock<Result<ArrayRef, TableFailure>>,
}

impl ColumnArray {
    pub(super) fn new(source: Arc<dyn ColumnSource>, rows: Arc<RowSet>, use_prev: bool) -> Self {
        Self {
            source,
            rows,
            use_prev,
            whole: OnceLock::new(),
        }
    }

    fn read(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let values = if self.use_prev {
            self.source.prev_chunk(rows)?
        } else {
            self.source.chunk(rows)?
        };
        if values.len() as u64 != rows.size() {
            return Err(failure(format!(
                "a column array returned {} values for {} rows",
                values.len(),
                rows.size()
            )));
        }
        Ok(without_null_values(values))
    }

    fn whole(&self) -> Result<ArrayRef, TableFailure> {
        self.whole.get_or_init(|| self.read(&self.rows)).clone()
    }

    /// The value at each position, null for a null position and for one that is not a
    /// row of the table.
    ///
    /// Behavior: `IntVector#get(long)` and its relatives as the column vectors implement
    /// them (`io.deephaven.engine.table.vectors.IntVectorColumnWrapper#get`): the null
    /// value outside `0..size()`. Only the rows named are read.
    fn at(&self, positions: &Int64Array) -> Result<ArrayRef, TableFailure> {
        let size = self.rows.size();
        let in_range = |row: usize| {
            positions
                .is_valid(row)
                .then(|| positions.value(row))
                .filter(|&position| position >= 0 && (position as u64) < size)
        };
        if let Some(Ok(whole)) = self.whole.get() {
            let indices: arrow::array::UInt64Array = (0..positions.len())
                .map(|row| in_range(row).map(|position| position as u64))
                .collect();
            return arrow::compute::take(whole.as_ref(), &indices, None).map_err(arrow_failure);
        }
        // The distinct positions asked for, in order: usually they are in order already.
        let mut distinct: Vec<i64> = (0..positions.len()).filter_map(in_range).collect();
        if !distinct.windows(2).all(|pair| pair[0] < pair[1]) {
            distinct.sort_unstable();
            distinct.dedup();
        }
        let wanted = RowSet::from_keys(distinct.iter().map(|&position| position as RowKey));
        let values = self.read(&self.rows.sub_set_for_positions(&wanted, false))?;
        let indices: UInt32Array = (0..positions.len())
            .map(|row| {
                in_range(row).and_then(|position| {
                    distinct
                        .binary_search(&position)
                        .ok()
                        .map(|index| index as u32)
                })
            })
            .collect();
        arrow::compute::take(values.as_ref(), &indices, None).map_err(arrow_failure)
    }

    /// The vector as the cell of `rows` rows.
    fn repeated(&self, rows: usize) -> Result<ArrayRef, TableFailure> {
        let whole = self.whole()?;
        let field = Arc::new(Field::new("", whole.data_type().clone(), true));
        if rows == 0 {
            let empty = new_empty_array(whole.data_type());
            return Ok(Arc::new(ListArray::new(
                field,
                OffsetBuffer::new_empty(),
                empty,
                None,
            )));
        }
        let length = whole.len();
        too_many_elements(length, rows)?;
        let elements = if rows == 1 {
            whole
        } else {
            let indices: UInt32Array = (0..rows).flat_map(|_| 0..length as u32).map(Some).collect();
            arrow::compute::take(whole.as_ref(), &indices, None).map_err(arrow_failure)?
        };
        Ok(Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths(std::iter::repeat_n(length, rows)),
            elements,
            None,
        )))
    }
}

/// Refuses `rows` vectors of `length` elements, which a list array cannot hold. Java holds
/// one vector and a reference to it per row.
pub(super) fn too_many_elements(length: usize, rows: usize) -> Result<(), TableFailure> {
    if length
        .checked_mul(rows)
        .is_none_or(|elements| elements > i32::MAX as usize)
    {
        return Err(failure(format!(
            "a vector of {length} values in each of {rows} rows is more than a column of \
             this server can hold (io.deephaven.engine.table.vectors.ColumnVectors#of)"
        )));
    }
    Ok(())
}

/// Whether `expr` has the same value in every row because it is made of column arrays and
/// literals only. A function of the embedder is left out: it may answer differently each
/// time it is called.
fn same_in_every_row(expr: &Typed) -> bool {
    fn walk(expr: &Typed, arrays: &mut bool) -> bool {
        let mut all = |parts: &[&Typed]| parts.iter().all(|part| walk(part, arrays));
        match &expr.node {
            Node::ColumnArray(_) => {
                *arrays = true;
                true
            }
            Node::Literal(_) | Node::Null => true,
            Node::Column(_) | Node::RowVariable(_) | Node::External { .. } => false,
            Node::Negate(operand)
            | Node::Not(operand)
            | Node::IsNull(operand)
            | Node::ObjectText(operand)
            | Node::NewString(operand)
            | Node::Cast(operand)
            | Node::Widen(operand) => all(&[operand]),
            Node::Arithmetic { left, right, .. }
            | Node::BooleanBitwise { left, right, .. }
            | Node::Compare { left, right, .. }
            | Node::Logical { left, right, .. }
            | Node::Concat { left, right }
            | Node::Elementwise { left, right, .. } => all(&[left, right]),
            Node::Ternary {
                condition,
                if_true,
                if_false,
            } => all(&[condition, if_true, if_false]),
            Node::Call { kernel, arguments } => {
                !kernel.is_volatile() && arguments.iter().all(|part| walk(part, arrays))
            }
            Node::StringMethod {
                receiver,
                arguments,
                ..
            } => walk(receiver, arrays) && arguments.iter().all(|part| walk(part, arrays)),
        }
    }
    let mut arrays = false;
    walk(expr, &mut arrays) && arrays
}

/// Evaluates `expr` when it is one of the three uses of a column array this module knows;
/// `None` leaves it to the caller.
pub(super) fn evaluate(expr: &Typed, batch: &Batch) -> Result<Option<ArrayRef>, TableFailure> {
    match &expr.node {
        Node::Call {
            kernel: Kernel::Vector(VectorFn::Get),
            arguments,
        } => {
            if let [Typed {
                node: Node::ColumnArray(slot),
                ..
            }, index] = arguments.as_slice()
            {
                let index = super::evaluate(index, batch)?;
                return batch.arrays[*slot]
                    .at(index.as_primitive::<Int64Type>())
                    .map(Some);
            }
        }
        Node::ColumnArray(slot) => return batch.arrays[*slot].repeated(batch.rows).map(Some),
        _ => {}
    }
    if batch.rows > 1 && same_in_every_row(expr) {
        let once = super::evaluate(expr, &batch.first_row())?;
        if let Some(list) = once.as_list_opt::<i32>() {
            too_many_elements(list.values().len(), batch.rows)?;
        }
        let indices = UInt32Array::from(vec![0_u32; batch.rows]);
        return arrow::compute::take(once.as_ref(), &indices, None)
            .map(Some)
            .map_err(arrow_failure);
    }
    Ok(None)
}
