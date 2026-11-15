//! Count where: the number of rows of each group that pass filters.

use super::operator::{AggOperator, Effect};
use super::values::Run;
use crate::ops::filter::RowFilters;
use arrow::array::{ArrayRef, Int64Array};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use std::sync::Arc;

/// The number of rows of each group that every filter accepts, as a `long`. A group
/// without such rows counts 0, also a group that `preserve_empty` keeps without rows.
///
/// The filters are those of `where`, run in the order given, each on the rows the ones
/// before it accepted. Rows that leave a group are filtered again by the values they had,
/// and modified rows before and after, so a filter must give the same answer for the same
/// values.
///
/// With key columns every group that is given rows is reported modified, whether or not
/// one of them passed; without, only when one passed. A removal or a modification is
/// reported when it changes the count. That is what Java's two forms of each call do.
///
/// A filter that fails on a row, as a formula that evaluates to null does, fails the
/// aggregation.
///
/// Behavior: io.deephaven.engine.table.impl.by.CountWhereOperator (the bucketed
/// `addChunk` sets `stateModified` for every destination; the singleton one returns
/// `count > 0`; the result column is a `LongArraySource` that is not filled with nulls)
pub struct CountWhere {
    parent: Arc<dyn Table>,
    filters: RowFilters,
    keyed: bool,
    counts: Vec<i64>,
    failure: Option<TableFailure>,
}

impl CountWhere {
    /// Counts the rows of `parent` that `filters` accept. `keyed` says the aggregation has
    /// key columns.
    pub(crate) fn new(parent: Arc<dyn Table>, filters: RowFilters, keyed: bool) -> CountWhere {
        CountWhere {
            parent,
            filters,
            keyed,
            counts: Vec::new(),
            failure: None,
        }
    }

    /// How many rows of `run` pass, by their previous or current values. After a failure
    /// nothing passes; the framework fails the result when the chunk is done.
    fn passing(&mut self, run: &Run<'_>, use_prev: bool) -> i64 {
        if self.failure.is_some() {
            return 0;
        }
        let rows = RowSet::from_keys(run.row_keys());
        match self.filters.accepted(&*self.parent, &rows, use_prev) {
            Ok(passed) => passed.size() as i64,
            Err(failure) => {
                self.failure = Some(failure);
                0
            }
        }
    }
}

impl AggOperator for CountWhere {
    fn result_type(&self) -> ColumnType {
        ColumnType::Long
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.counts.len() < slots {
            self.counts.resize(slots, 0);
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let passed = self.passing(run, false);
        self.counts[slot] += passed;
        Effect::modified_if(self.keyed || passed > 0)
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let passed = self.passing(run, true);
        self.counts[slot] -= passed;
        Effect::modified_if(passed > 0)
    }

    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        let change = self.passing(new, false) - self.passing(old, true);
        self.counts[slot] += change;
        Effect::modified_if(change != 0)
    }

    fn take_failure(&mut self) -> Option<TableFailure> {
        self.failure.take()
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            slots.iter().map(|&slot| self.counts[slot]),
        ))
    }
}
