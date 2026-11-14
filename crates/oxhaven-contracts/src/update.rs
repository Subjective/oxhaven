//! The change a table reports to its listeners in one update cycle.
//!
//! Port of `io.deephaven.engine.table.TableUpdate` and
//! `io.deephaven.engine.table.ModifiedColumnSet`.

use crate::rowset::RowSet;
use crate::shift::{RowSetShiftData, ShiftError};
use std::collections::BTreeSet;
use std::fmt;

/// Which columns changed for the rows in [`TableUpdate::modified`]. Columns are identified by
/// their index in the table's definition.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ModifiedColumnSet {
    /// Port of `ModifiedColumnSet#ALL`: every column may have changed.
    All,
    /// Exactly these columns may have changed. Empty is `ModifiedColumnSet#EMPTY`.
    Columns(BTreeSet<usize>),
}

impl ModifiedColumnSet {
    pub fn empty() -> Self {
        ModifiedColumnSet::Columns(BTreeSet::new())
    }

    pub fn of(columns: impl IntoIterator<Item = usize>) -> Self {
        ModifiedColumnSet::Columns(columns.into_iter().collect())
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, ModifiedColumnSet::Columns(c) if c.is_empty())
    }

    pub fn contains(&self, column: usize) -> bool {
        match self {
            ModifiedColumnSet::All => true,
            ModifiedColumnSet::Columns(c) => c.contains(&column),
        }
    }

    /// Port of `ModifiedColumnSet#containsAny`.
    pub fn contains_any(&self, columns: &[usize]) -> bool {
        columns.iter().any(|c| self.contains(*c))
    }

    pub fn union(&self, other: &ModifiedColumnSet) -> ModifiedColumnSet {
        match (self, other) {
            (ModifiedColumnSet::Columns(a), ModifiedColumnSet::Columns(b)) => {
                ModifiedColumnSet::Columns(a | b)
            }
            _ => ModifiedColumnSet::All,
        }
    }
}

/// One cycle's change to a table.
///
/// Key spaces, as in Java:
/// - `removed` is in the previous key space.
/// - `shifted` applies to the previous rows that were not removed.
/// - `added` and `modified` are in the current (post-shift) key space.
///
/// The current row set is therefore `shifted.apply(prev - removed) + added`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TableUpdate {
    pub added: RowSet,
    pub removed: RowSet,
    pub modified: RowSet,
    pub shifted: RowSetShiftData,
    pub modified_columns: ModifiedColumnSet,
}

/// Why an update is not a valid change from a given previous row set.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum UpdateError {
    /// `removed` names rows that were not in the previous row set.
    RemovedNotPresent(RowSet),
    Shift(ShiftError),
    /// `added` names rows that already exist after removes and shifts.
    AddedAlreadyPresent(RowSet),
    /// `modified` names rows that are not in the current row set.
    ModifiedNotPresent(RowSet),
    /// A row is both added and modified.
    AddedAndModified(RowSet),
    /// `modified` is non-empty with no modified columns, or the reverse.
    ModifiedColumnsMismatch,
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdateError::RemovedNotPresent(r) => write!(f, "removed rows not in previous: {r:?}"),
            UpdateError::Shift(e) => write!(f, "{e}"),
            UpdateError::AddedAlreadyPresent(r) => write!(f, "added rows already present: {r:?}"),
            UpdateError::ModifiedNotPresent(r) => write!(f, "modified rows not in current: {r:?}"),
            UpdateError::AddedAndModified(r) => write!(f, "rows both added and modified: {r:?}"),
            UpdateError::ModifiedColumnsMismatch => {
                write!(
                    f,
                    "modified rows and modified columns must both be empty or both non-empty"
                )
            }
        }
    }
}

impl std::error::Error for UpdateError {}

impl TableUpdate {
    /// An update that changes nothing.
    pub fn empty() -> Self {
        Self {
            added: RowSet::empty(),
            removed: RowSet::empty(),
            modified: RowSet::empty(),
            shifted: RowSetShiftData::empty(),
            modified_columns: ModifiedColumnSet::empty(),
        }
    }

    /// Port of `TableUpdate#empty()`: true when nothing was added, removed, modified or shifted.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.modified.is_empty()
            && self.shifted.is_empty()
    }

    /// Applies this update to the previous row set, returning the current row set, or the first
    /// rule the update breaks.
    ///
    /// Port of the checks in `TableUpdate#validate`. Every table's updates must pass this; the
    /// self-consistency checker runs it on every cycle.
    pub fn apply_to(&self, prev: &RowSet) -> Result<RowSet, UpdateError> {
        let missing = self.removed.minus(prev);
        if !missing.is_empty() {
            return Err(UpdateError::RemovedNotPresent(missing));
        }
        let survivors = self
            .shifted
            .apply(&prev.minus(&self.removed))
            .map_err(UpdateError::Shift)?;
        let clash = self.added.intersect(&survivors);
        if !clash.is_empty() {
            return Err(UpdateError::AddedAlreadyPresent(clash));
        }
        let current = survivors.union(&self.added);
        let absent = self.modified.minus(&current);
        if !absent.is_empty() {
            return Err(UpdateError::ModifiedNotPresent(absent));
        }
        let both = self.modified.intersect(&self.added);
        if !both.is_empty() {
            return Err(UpdateError::AddedAndModified(both));
        }
        if self.modified.is_empty() != self.modified_columns.is_empty() {
            return Err(UpdateError::ModifiedColumnsMismatch);
        }
        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shift::ShiftRange;

    fn base() -> TableUpdate {
        TableUpdate::empty()
    }

    #[test]
    fn apply_removes_then_shifts_then_adds() {
        let prev = RowSet::from_keys([0, 1, 2, 3]);
        let update = TableUpdate {
            removed: RowSet::from_keys([1]),
            shifted: RowSetShiftData::new(vec![ShiftRange {
                first: 2,
                last: 3,
                delta: -1,
            }])
            .unwrap(),
            added: RowSet::from_keys([3]),
            modified: RowSet::from_keys([0]),
            modified_columns: ModifiedColumnSet::of([1]),
        };
        assert_eq!(update.apply_to(&prev).unwrap(), RowSet::from_range(0, 3));
    }

    #[test]
    fn empty_update_keeps_the_row_set() {
        let prev = RowSet::from_keys([4, 9]);
        assert!(base().is_empty());
        assert_eq!(base().apply_to(&prev).unwrap(), prev);
    }

    #[test]
    fn each_rule_is_enforced() {
        let prev = RowSet::from_keys([0, 1, 2]);
        let removed_missing = TableUpdate {
            removed: RowSet::from_keys([7]),
            ..base()
        };
        assert!(matches!(
            removed_missing.apply_to(&prev),
            Err(UpdateError::RemovedNotPresent(_))
        ));
        let added_present = TableUpdate {
            added: RowSet::from_keys([2]),
            ..base()
        };
        assert!(matches!(
            added_present.apply_to(&prev),
            Err(UpdateError::AddedAlreadyPresent(_))
        ));
        let modified_missing = TableUpdate {
            modified: RowSet::from_keys([9]),
            modified_columns: ModifiedColumnSet::All,
            ..base()
        };
        assert!(matches!(
            modified_missing.apply_to(&prev),
            Err(UpdateError::ModifiedNotPresent(_))
        ));
        let added_and_modified = TableUpdate {
            added: RowSet::from_keys([5]),
            modified: RowSet::from_keys([5]),
            modified_columns: ModifiedColumnSet::All,
            ..base()
        };
        assert!(matches!(
            added_and_modified.apply_to(&prev),
            Err(UpdateError::AddedAndModified(_))
        ));
        let no_columns = TableUpdate {
            modified: RowSet::from_keys([0]),
            ..base()
        };
        assert_eq!(
            no_columns.apply_to(&prev),
            Err(UpdateError::ModifiedColumnsMismatch)
        );
        let columns_without_rows = TableUpdate {
            modified_columns: ModifiedColumnSet::of([0]),
            ..base()
        };
        assert_eq!(
            columns_without_rows.apply_to(&prev),
            Err(UpdateError::ModifiedColumnsMismatch)
        );
        let collision = TableUpdate {
            shifted: RowSetShiftData::new(vec![ShiftRange {
                first: 0,
                last: 0,
                delta: 1,
            }])
            .unwrap(),
            ..base()
        };
        assert!(matches!(
            collision.apply_to(&prev),
            Err(UpdateError::Shift(_))
        ));
    }

    #[test]
    fn modified_column_set_union_and_lookup() {
        let a = ModifiedColumnSet::of([0, 2]);
        assert!(a.contains(2) && !a.contains(1));
        assert!(a.contains_any(&[1, 2]) && !a.contains_any(&[1, 3]));
        assert_eq!(
            a.union(&ModifiedColumnSet::of([1])),
            ModifiedColumnSet::of([0, 1, 2])
        );
        assert_eq!(a.union(&ModifiedColumnSet::All), ModifiedColumnSet::All);
        assert!(ModifiedColumnSet::empty().is_empty() && !ModifiedColumnSet::All.is_empty());
    }
}
