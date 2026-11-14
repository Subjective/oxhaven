//! Group keys to slots.

use super::values::failure;
use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::row::{RowConverter, Rows, SortField};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::collections::HashMap;
use std::sync::Arc;

/// The slot of every key seen so far. Slots are handed out in the order keys are first
/// seen, starting at zero, and a key keeps its slot for good, also while it has no rows.
///
/// Two rows have the same key when every key column is equal by the grouping rule of
/// PARITY.md: null is a value equal to itself, every NaN is the same key, and `-0.0` is
/// the same key as `0.0`.
///
/// Behavior: io.deephaven.engine.table.impl.by.OperatorAggregationStateManager, whose
/// `outputPosition` is the slot.
pub(crate) struct KeyTable {
    converter: RowConverter,
    /// Looked up only, never iterated, so its order cannot reach a result.
    slots: HashMap<Box<[u8]>, usize>,
}

impl KeyTable {
    pub fn new(key_types: &[ColumnType]) -> Result<KeyTable, TableFailure> {
        // Java hashes a vector or an array key as an object; when two of them are one
        // key, and what a NaN or a `-0.0` element does to that, is not ported.
        if let Some(list) = key_types.iter().find(|t| t.list_element().is_some()) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
                "a group key column of type {list:?}"
            );
        }
        let fields = key_types
            .iter()
            .map(|key_type| SortField::new(key_type.arrow_type()))
            .collect();
        Ok(KeyTable {
            converter: RowConverter::new(fields).map_err(|error| failure(error.to_string()))?,
            slots: HashMap::new(),
        })
    }

    /// The number of slots handed out.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    fn encode(&self, key_columns: &[ArrayRef]) -> Result<Rows, TableFailure> {
        let canonical: Vec<ArrayRef> = key_columns.iter().map(canonical_floats).collect();
        self.converter
            .convert_columns(&canonical)
            .map_err(|error| failure(error.to_string()))
    }

    /// The slot of each row of `key_columns`, with a new slot for each key not seen before.
    /// Also returns the positions of the rows that got a new slot, in slot order.
    pub fn find_or_create(
        &mut self,
        key_columns: &[ArrayRef],
    ) -> Result<(Vec<usize>, Vec<usize>), TableFailure> {
        let rows = self.encode(key_columns)?;
        let mut slots = Vec::with_capacity(rows.num_rows());
        let mut created = Vec::new();
        for (position, row) in rows.iter().enumerate() {
            let key: &[u8] = row.as_ref();
            let slot = match self.slots.get(key) {
                Some(&slot) => slot,
                None => {
                    let slot = self.slots.len();
                    self.slots.insert(key.into(), slot);
                    created.push(position);
                    slot
                }
            };
            slots.push(slot);
        }
        Ok((slots, created))
    }

    /// The slot of each row of `key_columns`. An error if a key was never seen, which
    /// means the parent reported a row that the aggregation was never given.
    pub fn find(&self, key_columns: &[ArrayRef]) -> Result<Vec<usize>, TableFailure> {
        let rows = self.encode(key_columns)?;
        rows.iter()
            .map(|row| {
                let key: &[u8] = row.as_ref();
                self.slots.get(key).copied().ok_or_else(|| {
                    failure("aggregation: a removed or modified row has a key that was never added")
                })
            })
            .collect()
    }
}

/// `array` with every NaN made the same NaN and `-0.0` made `0.0`, so that equal keys
/// encode equally. Arrays of other types are returned as they are.
fn canonical_floats(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float64 => {
            let result: Float64Array = array.as_primitive::<Float64Type>().unary(|v| {
                if v.is_nan() {
                    f64::NAN
                } else if v == 0.0 {
                    0.0
                } else {
                    v
                }
            });
            Arc::new(result)
        }
        DataType::Float32 => {
            let result: Float32Array = array.as_primitive::<Float32Type>().unary(|v| {
                if v.is_nan() {
                    f32::NAN
                } else if v == 0.0 {
                    0.0
                } else {
                    v
                }
            });
            Arc::new(result)
        }
        _ => array.clone(),
    }
}
