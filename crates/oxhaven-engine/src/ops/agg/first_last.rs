//! First and last.

use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{build, Run, Scalar, Values};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

/// How the first or last row of a group is found, which depends on what the parent can do.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FirstLastMode {
    /// Rows are only added: the row with the least or greatest row key so far.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.StaticFirstOrLastChunkedOperator and
    /// io.deephaven.engine.table.impl.by.AddOnlyFirstOrLastChunkedOperator
    ByRowKey,
    /// A blink parent, whose rows last one cycle and whose row keys repeat: the first row
    /// the group was ever given, or the last row of the latest rows it was given.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.BlinkFirstChunkedOperator and
    /// io.deephaven.engine.table.impl.by.BlinkLastChunkedOperator
    Arrival,
    /// Rows come, go, change and move: the first or last row of the group's rows, read
    /// again whenever the group changes.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.FirstOrLastChunkedOperator
    Tracked,
}

/// The value of a column in the first or last row of each group, by row key. Nulls are
/// values like any other.
pub struct FirstLast {
    column_type: ColumnType,
    first: bool,
    mode: FirstLastMode,
    /// The row the value was taken from, or `NULL_ROW_KEY`.
    row_key: Vec<RowKey>,
    value: Vec<Scalar>,
}

impl FirstLast {
    pub fn new(column_type: ColumnType, first: bool, mode: FirstLastMode) -> FirstLast {
        FirstLast {
            column_type,
            first,
            mode,
            row_key: Vec::new(),
            value: Vec::new(),
        }
    }

    fn tracked(&self) -> Effect {
        if self.mode == FirstLastMode::Tracked {
            Effect::RECOMPUTE
        } else {
            Effect::UNCHANGED
        }
    }
}

impl AggOperator for FirstLast {
    fn result_type(&self) -> ColumnType {
        self.column_type.clone()
    }

    fn needs_group_rows(&self) -> bool {
        self.mode == FirstLastMode::Tracked
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.row_key.len() < slots {
            self.row_key.resize(slots, NULL_ROW_KEY);
            self.value.resize(slots, Scalar::Null);
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        // A run is in row key order, so its first and last rows are its candidates.
        let Some((row_key, value)) = run.end(self.first) else {
            return Effect::UNCHANGED;
        };
        let current = self.row_key[slot];
        let takes = match self.mode {
            FirstLastMode::Tracked => return Effect::RECOMPUTE,
            FirstLastMode::ByRowKey if self.first => current == NULL_ROW_KEY || row_key < current,
            FirstLastMode::ByRowKey => current == NULL_ROW_KEY || row_key > current,
            FirstLastMode::Arrival => !self.first || current == NULL_ROW_KEY,
        };
        if takes {
            self.row_key[slot] = row_key;
            self.value[slot] = value;
        }
        Effect::modified_if(takes)
    }

    fn remove(&mut self, _slot: usize, _run: &Run<'_>) -> Effect {
        self.tracked()
    }

    fn shifted(&mut self, _slot: usize) -> Effect {
        self.tracked()
    }

    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        let row_keys: Vec<RowKey> = slots
            .iter()
            .map(|&slot| {
                let rows = groups.rows(slot);
                let row_key = if self.first {
                    rows.first_row_key()
                } else {
                    rows.last_row_key()
                };
                row_key.unwrap_or(NULL_ROW_KEY)
            })
            .collect();
        let values = groups.input().chunk_by_keys(&row_keys, false)?;
        let values = Values::decode(&self.column_type, &values)?;
        Ok(slots
            .iter()
            .enumerate()
            .map(|(position, &slot)| {
                let value = values.scalar(position);
                let changed = !value.same(&self.value[slot]);
                self.row_key[slot] = row_keys[position];
                self.value[slot] = value;
                changed
            })
            .collect())
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        build(
            &self.column_type,
            slots.iter().map(|&slot| &self.value[slot]),
        )
    }
}
