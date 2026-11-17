//! First and last.

use super::operator::{AggOperator, Effect, GroupReader};
use super::slot_source::SlotSource;
use super::values::{build, failure, Run, Scalar, Values};
use arrow::array::{Array, ArrayRef};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::Arc;

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
    /// The row the value was taken from, or `NULL_ROW_KEY`. When `Tracked`, as of the last
    /// recompute: a shift may have moved the row since.
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

    /// Whether the rows of `run`, which leave group `slot`, include the row it shows.
    ///
    /// Behavior: FirstOrLastChunkedOperator#updateRedirections
    fn leaves(&self, slot: usize, run: &Run<'_>) -> Effect {
        if self.mode != FirstLastMode::Tracked {
            return Effect::UNCHANGED;
        }
        let current = self.row_key[slot];
        Effect {
            modified: run.row_keys().any(|row_key| row_key == current),
            recompute: true,
        }
    }
}

impl AggOperator for FirstLast {
    fn result_type(&self) -> ColumnType {
        self.column_type.clone()
    }

    /// Behavior: FirstOrLastChunkedOperator#resultColumns, each an
    /// io.deephaven.engine.table.impl.sources.RedirectedColumnSource
    fn holds_results(&self) -> bool {
        false
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
            // Behavior: FirstOrLastChunkedOperator#updateRedirections, which reports the
            // group modified when the row it shows is another one, whatever its value.
            FirstLastMode::Tracked => {
                let beyond = current == NULL_ROW_KEY
                    || if self.first {
                        row_key < current
                    } else {
                        row_key > current
                    };
                return Effect {
                    modified: beyond,
                    recompute: true,
                };
            }
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

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.leaves(slot, run)
    }

    /// Behavior: FirstOrLastChunkedOperator#modifyRowKeys, which Java calls for the
    /// modified rows of a group whatever column changed (the operator has no input
    /// columns, see AggregationProcessor#addFirstOrLastOperators): the result is modified
    /// when the group's first or last row is among them.
    fn modify(&mut self, slot: usize, _old: &Run<'_>, new: &Run<'_>) -> Effect {
        let current = self.row_key[slot];
        Effect {
            modified: new.row_keys().any(|row_key| row_key == current),
            recompute: true,
        }
    }

    /// Behavior: FirstOrLastChunkedOperator#shiftChunk, which moves the row the group
    /// shows without reporting the group modified.
    fn shifted(&mut self, slot: usize, before: &RowSet, after: &RowSet) {
        let current = self.row_key[slot];
        if let Some((_, moved)) = before
            .iter()
            .zip(after.iter())
            .find(|&(row_key, _)| row_key == current)
        {
            self.row_key[slot] = moved;
        }
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

/// [`FirstLast`] for a column of vectors or arrays: the whole cell of the group's first or
/// last row, a null cell as a null. The cell is read from the column by the row's key,
/// where [`FirstLast`] keeps the values it is given, which have no form for a list.
///
/// A row that takes the place of the first or last is reported as [`FirstLast`] reports
/// it: always when rows are only added, and by comparing the cells when they can also go.
///
/// Behavior: the operators [`FirstLastMode`] names, which keep a row key per group and
/// show the input column through it, whatever its type
pub struct FirstLastCell {
    input: Arc<dyn ColumnSource>,
    column_type: ColumnType,
    first: bool,
    mode: FirstLastMode,
    row_key: Vec<RowKey>,
    /// The cell of each group, by slot; a group that has had no rows shows a null.
    cells: SlotSource,
}

impl FirstLastCell {
    pub fn new(
        input: Arc<dyn ColumnSource>,
        column_type: ColumnType,
        first: bool,
        mode: FirstLastMode,
    ) -> FirstLastCell {
        FirstLastCell {
            input,
            cells: SlotSource::new(column_type.clone(), None),
            column_type,
            first,
            mode,
            row_key: Vec::new(),
        }
    }
}

impl FirstLastCell {
    /// Whether the rows of `run`, which leave group `slot`, include the row it shows.
    ///
    /// Behavior: FirstOrLastChunkedOperator#updateRedirections
    fn leaves(&self, slot: usize, run: &Run<'_>) -> Effect {
        if self.mode != FirstLastMode::Tracked {
            return Effect::UNCHANGED;
        }
        let current = self.row_key[slot];
        Effect {
            modified: run.row_keys().any(|row_key| row_key == current),
            recompute: true,
        }
    }
}

impl AggOperator for FirstLastCell {
    fn result_type(&self) -> ColumnType {
        self.column_type.clone()
    }

    /// Behavior: FirstOrLastChunkedOperator#resultColumns, each an
    /// io.deephaven.engine.table.impl.sources.RedirectedColumnSource
    fn holds_results(&self) -> bool {
        false
    }

    fn needs_group_rows(&self) -> bool {
        self.mode == FirstLastMode::Tracked
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.row_key.len() < slots {
            self.row_key.resize(slots, NULL_ROW_KEY);
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let mut row_keys = run.row_keys();
        let candidate = if self.first {
            row_keys.next()
        } else {
            row_keys.last()
        };
        let Some(row_key) = candidate else {
            return Effect::UNCHANGED;
        };
        let current = self.row_key[slot];
        let takes = match self.mode {
            // Behavior: FirstOrLastChunkedOperator#updateRedirections, which reports the
            // group modified when the row it shows is another one, whatever its value.
            FirstLastMode::Tracked => {
                let beyond = current == NULL_ROW_KEY
                    || if self.first {
                        row_key < current
                    } else {
                        row_key > current
                    };
                return Effect {
                    modified: beyond,
                    recompute: true,
                };
            }
            FirstLastMode::ByRowKey if self.first => current == NULL_ROW_KEY || row_key < current,
            FirstLastMode::ByRowKey => current == NULL_ROW_KEY || row_key > current,
            FirstLastMode::Arrival => !self.first || current == NULL_ROW_KEY,
        };
        if !takes {
            return Effect::UNCHANGED;
        }
        self.row_key[slot] = row_key;
        // The cell is read once the chunk's rows are all seen.
        Effect::RECOMPUTE
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.leaves(slot, run)
    }

    /// Behavior: FirstOrLastChunkedOperator#modifyRowKeys, which Java calls for the
    /// modified rows of a group whatever column changed (the operator has no input
    /// columns, see AggregationProcessor#addFirstOrLastOperators): the result is modified
    /// when the group's first or last row is among them.
    fn modify(&mut self, slot: usize, _old: &Run<'_>, new: &Run<'_>) -> Effect {
        let current = self.row_key[slot];
        Effect {
            modified: new.row_keys().any(|row_key| row_key == current),
            recompute: true,
        }
    }

    /// Behavior: FirstOrLastChunkedOperator#shiftChunk, which moves the row the group
    /// shows without reporting the group modified.
    fn shifted(&mut self, slot: usize, before: &RowSet, after: &RowSet) {
        let current = self.row_key[slot];
        if let Some((_, moved)) = before
            .iter()
            .zip(after.iter())
            .find(|&(row_key, _)| row_key == current)
        {
            self.row_key[slot] = moved;
        }
    }

    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        let tracked = self.mode == FirstLastMode::Tracked;
        if tracked {
            for &slot in slots {
                let rows = groups.rows(slot);
                let row_key = if self.first {
                    rows.first_row_key()
                } else {
                    rows.last_row_key()
                };
                self.row_key[slot] = row_key.unwrap_or(NULL_ROW_KEY);
            }
        }
        let row_keys: Vec<RowKey> = slots.iter().map(|&slot| self.row_key[slot]).collect();
        let cells = self.input.chunk_by_keys(&row_keys, false)?;
        if cells.len() != slots.len() {
            return Err(failure("first or last: the column gave too few cells"));
        }
        let slot_keys: Vec<RowKey> = slots.iter().map(|&slot| slot as RowKey).collect();
        let changed = if tracked {
            // A group that has had no rows reads as null here, so it changed when its
            // cell is not a null.
            let old = self.cells.chunk_by_keys(&slot_keys, false)?;
            (0..slots.len())
                .map(|position| {
                    old.slice(position, 1).as_ref() != cells.slice(position, 1).as_ref()
                })
                .collect()
        } else {
            // A row took the place of another: reported as it is for a scalar.
            vec![true; slots.len()]
        };
        self.cells.fill_by_keys(&slot_keys, &cells)?;
        Ok(changed)
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        let slot_keys: Vec<RowKey> = slots.iter().map(|&slot| slot as RowKey).collect();
        self.cells
            .chunk_by_keys(&slot_keys, false)
            .expect("cells of the column's type")
    }
}
