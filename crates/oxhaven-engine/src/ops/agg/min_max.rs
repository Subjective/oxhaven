//! Minimum and maximum.

use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{build, Run, Scalar, Values};
use arrow::array::ArrayRef;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;

/// Rows read at a time when a group is recomputed.
const CHUNK_SIZE: u64 = 4096;

/// The least or greatest non-null value of a column of any type, by Java's ordering (see
/// [`Scalar::compare`]). Null when the group has no non-null value. A NaN in the group
/// makes both the minimum and the maximum NaN.
///
/// Among values that compare equal (`0.0` and `-0.0`), the one kept is the first one
/// given. Java keeps a sorted multiset on tables that remove rows, and which zero that
/// shows depends on its sort and merge history; that is not reproduced.
///
/// On a parent that never removes rows the state is the best value so far. On one that
/// does, a removal of the best value cannot be answered from that, so the operator asks
/// for the group's rows and reads them again.
///
/// Behavior: io.deephaven.engine.table.impl.by.DoubleChunkedAddOnlyMinMaxOperator (and its
/// per-type copies) for the first
/// case and io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator for the
/// second, where the multiset is replaced by the recompute.
pub struct MinMax {
    column_type: ColumnType,
    minimum: bool,
    /// Whether rows can leave a group.
    retracts: bool,
    /// The best value that is not NaN, or null.
    best: Vec<Scalar>,
    /// The number of NaN values.
    nan: Vec<u64>,
}

impl MinMax {
    pub fn new(column_type: ColumnType, minimum: bool, retracts: bool) -> MinMax {
        MinMax {
            column_type,
            minimum,
            retracts,
            best: Vec::new(),
            nan: Vec::new(),
        }
    }

    fn better(&self, candidate: &Scalar, best: &Scalar) -> bool {
        let wanted = if self.minimum {
            Ordering::Less
        } else {
            Ordering::Greater
        };
        best.is_null() || candidate.compare(best) == wanted
    }

    /// Takes `value` into `best` and `nan`. Whether either changed.
    fn take(&self, value: Scalar, best: &mut Scalar, nan: &mut u64) -> bool {
        if value.is_null() {
            false
        } else if value.is_nan() {
            *nan += 1;
            true
        } else if self.better(&value, best) {
            *best = value;
            true
        } else {
            false
        }
    }

    fn result(&self, slot: usize) -> Scalar {
        if self.nan[slot] > 0 {
            Scalar::Double(f64::NAN)
        } else {
            self.best[slot].clone()
        }
    }
}

impl AggOperator for MinMax {
    fn result_type(&self) -> ColumnType {
        self.column_type.clone()
    }

    fn needs_group_rows(&self) -> bool {
        self.retracts
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.best.len() < slots {
            self.best.resize(slots, Scalar::Null);
            self.nan.resize(slots, 0);
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let before = self.result(slot);
        let mut best = std::mem::replace(&mut self.best[slot], Scalar::Null);
        let mut nan = self.nan[slot];
        for (_, value) in run.scalars() {
            self.take(value, &mut best, &mut nan);
        }
        self.best[slot] = best;
        self.nan[slot] = nan;
        Effect::modified_if(!before.same(&self.result(slot)))
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let before = self.result(slot);
        for (_, value) in run.scalars() {
            if value.is_nan() {
                self.nan[slot] = self.nan[slot].saturating_sub(1);
            } else if !value.is_null() && value.compare(&self.best[slot]) == Ordering::Equal {
                return Effect::Recompute;
            }
        }
        Effect::modified_if(!before.same(&self.result(slot)))
    }

    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        let input = groups.input();
        let mut changed = Vec::with_capacity(slots.len());
        for &slot in slots {
            let before = self.result(slot);
            let rows = groups.rows(slot);
            let mut best = Scalar::Null;
            let mut nan = 0;
            let mut start = 0;
            while start < rows.size() {
                let chunk = rows.sub_set_by_position_range(start, start + CHUNK_SIZE);
                let values = Values::decode(&self.column_type, &input.chunk(&chunk)?)?;
                for position in 0..values.len() {
                    self.take(values.scalar(position), &mut best, &mut nan);
                }
                start += CHUNK_SIZE;
            }
            self.best[slot] = best;
            self.nan[slot] = nan;
            changed.push(!before.same(&self.result(slot)));
        }
        Ok(changed)
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        let results: Vec<Scalar> = slots.iter().map(|&slot| self.result(slot)).collect();
        build(&self.column_type, results.iter())
    }
}
