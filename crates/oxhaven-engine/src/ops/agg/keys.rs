//! Group keys to slots.

use super::values::failure;
use crate::ops::key_table::KeyTable as GroupKeys;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

pub(crate) use crate::ops::key_table::canonical_floats;
use crate::ops::key_table::is_ported_key;
use arrow::array::ArrayRef;

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
    keys: GroupKeys,
}

impl KeyTable {
    /// A vector key column is a key like any other when [`groupable_vector`](crate::ops::key_table::groupable_vector) takes its
    /// type; an array key column is refused before this (`aggregate_inner`).
    pub fn new(key_types: &[ColumnType]) -> Result<KeyTable, TableFailure> {
        if let Some(list) = key_types.iter().find(|t| !is_ported_key(t)) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
                "a group key column of type {list:?}: Java's groups of vectors of float or \
                 double elements depend on the class of the vector objects (-0.0 and 0.0), \
                 and those of vectors of arrays on the identity of the arrays"
            );
        }
        let data_types: Vec<_> = key_types.iter().map(ColumnType::arrow_type).collect();
        Ok(KeyTable {
            keys: GroupKeys::for_types(&data_types)?,
        })
    }

    /// The number of slots handed out.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// The slot of each row of `key_columns`, with a new slot for each key not seen before.
    /// Also returns the positions of the rows that got a new slot, in slot order.
    pub fn find_or_create(
        &mut self,
        key_columns: &[ArrayRef],
    ) -> Result<(Vec<usize>, Vec<usize>), TableFailure> {
        let mut slots = Vec::new();
        let mut created = Vec::new();
        self.keys
            .find_or_create(key_columns, &mut slots, &mut created)?;
        Ok((slots, created))
    }

    /// The slot of each row of `key_columns`. An error if a key was never seen, which
    /// means the parent reported a row that the aggregation was never given.
    pub fn find(&mut self, key_columns: &[ArrayRef]) -> Result<Vec<usize>, TableFailure> {
        let mut slots = Vec::new();
        if self.keys.find_all(key_columns, &mut slots)? {
            Ok(slots)
        } else {
            Err(failure(
                "aggregation: a removed or modified row has a key that was never added",
            ))
        }
    }
}
