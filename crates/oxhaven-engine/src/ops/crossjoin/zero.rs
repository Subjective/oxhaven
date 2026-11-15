//! The cross join without match columns: every left row with every right row.
//!
//! A left row's region holds one result row per right row, at the right row key. The bits
//! start at the reserved number (1 when both tables are static) or the bits of the right
//! table's last row key if that is more, and grow when the right table's last row key
//! needs more.

use super::layout::{min_bits, Layout};
use super::region::{Downstream, InnerChange};
use crate::update::ModifiedColumnTransformer;
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::Arc;

fn failure(message: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: message.to_string(),
    }
}

/// Behavior: io.deephaven.engine.table.impl.CrossJoinHelper#validateZeroKeyIndexSpace
pub(super) fn check_key_space(
    left_last: RowKey,
    right_last: RowKey,
    reserved: u32,
) -> Result<(), String> {
    let left_bits = min_bits(left_last);
    let right_bits = min_bits(right_last);
    let reserved = reserved.max(right_bits);
    if left_bits + reserved > 63 {
        return Err(format!(
            "join with zero key columns out of rowSet space (left reqBits + right reserveBits > \
             63); (left table: {{maxIndex: {left_last} reqBits: {left_bits}}}) X (right table: \
             {{maxIndex: {right_last} reqBits: {right_bits} reservedBits: {reserved}}}) exceeds \
             Long.MAX_VALUE. Consider flattening either table or reserving fewer right bits if \
             possible."
        ));
    }
    Ok(())
}

/// The rows of a result whose left table has `left` and right table `right`, at `bits`.
pub(super) fn rows(left: &RowSet, right: &RowSet, bits: u32) -> RowSet {
    let mut ranges = Vec::with_capacity(left.size() as usize * right.ranges().len());
    if !right.is_empty() {
        for row in left.iter() {
            let offset = row << bits;
            ranges.extend(
                right
                    .ranges()
                    .iter()
                    .map(|range| RowRange::new(range.first() + offset, range.last() + offset)),
            );
        }
    }
    RowSet::from_ranges(ranges)
}

/// The incremental half of a cross join without match columns.
pub(super) struct ZeroKey {
    pub(super) left: Arc<dyn Table>,
    pub(super) right: Arc<dyn Table>,
    pub(super) layout: Arc<Layout>,
    pub(super) left_columns: ModifiedColumnTransformer,
    pub(super) right_columns: ModifiedColumnTransformer,
}

impl ZeroKey {
    /// Behavior: the `onUpdate` function of
    /// io.deephaven.engine.table.impl.CrossJoinHelper#zeroKeyColumnsJoin. Java listens to
    /// a refreshing table only when the other is refreshing or not empty; with a static
    /// empty partner the result is static and this is never called (`unheard` in `mod.rs`).
    pub(super) fn on_update(
        &self,
        left_update: Option<&TableUpdate>,
        right_update: Option<&TableUpdate>,
    ) -> Result<TableUpdate, TableFailure> {
        let (left, right) = (&*self.left, &*self.right);
        let left_now = left.row_set();
        let right_now = right.row_set();
        let right_last = right_now.last_row_key().unwrap_or(-1);
        let mut state = self.layout.write();
        let prev_bits = state.bits(false);
        let bits = prev_bits.max(min_bits(right_last));
        check_key_space(left_now.last_row_key().unwrap_or(-1), right_last, bits)
            .map_err(failure)?;
        state.set_bits(bits);
        drop(state);

        let change = match right_update {
            Some(update) => InnerChange::keys(
                (*right.prev_row_set()).clone(),
                (*right_now).clone(),
                update.removed.clone(),
                update.added.clone(),
                update.modified.clone(),
                &update.shifted,
            ),
            None => InnerChange::unchanged((*right_now).clone()),
        };

        let mut down = Downstream::default();
        let no_change = TableUpdate::empty();
        let left_change = left_update.unwrap_or(&no_change);
        for row in left_change.removed.iter() {
            down.remove_region(row << prev_bits, &change.prev);
        }
        let visit = if right_update.is_some() || bits != prev_bits {
            left_now.minus(&left_change.added)
        } else {
            let staying = left.prev_row_set().minus(&left_change.removed);
            let mut visit = left_change.modified.clone();
            for range in left_change.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                visit.insert(&rows.shift(range.delta).map_err(failure)?);
            }
            visit
        };
        let back: RowSetShiftData = left_change.shifted.inverse();
        for row in visit.iter() {
            down.survivor(
                back.apply_to_key(row) << prev_bits,
                prev_bits,
                row << bits,
                &change,
                left_change.modified.contains(row),
            );
        }
        for row in left_change.added.iter() {
            down.add_region(row << bits, &change.curr);
        }

        let mut columns = ModifiedColumnSet::empty();
        if !left_change.modified.is_empty() {
            columns = columns.union(&self.left_columns.transform(&left_change.modified_columns));
        }
        if let Some(update) = right_update.filter(|update| !update.modified.is_empty()) {
            columns = columns.union(&self.right_columns.transform(&update.modified_columns));
        }
        down.finish(columns)
    }
}
