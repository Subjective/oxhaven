//! Sorted first and sorted last: the value of a column in the row of a group that sorts
//! first or last by other columns.

use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{build, Run, Scalar, Values};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::cmp::Ordering;
use std::sync::Arc;

/// Rows read at a time when a group is read again.
const CHUNK_SIZE: u64 = 4096;

/// A sort column of the parent.
#[derive(Clone)]
pub struct SortColumn {
    pub source: Arc<dyn ColumnSource>,
    pub column_type: ColumnType,
}

/// The sort key of one row: its value in each sort column.
type SortKey = Vec<Scalar>;

/// Java's ordering of sort keys: column by column, null first, then
/// [`Scalar::compare`], under which `0.0` equals `-0.0` and NaN is last.
///
/// Behavior: io.deephaven.engine.table.impl.ssa.DoubleSegmentedSortedArray#doComparison
/// (and its per-type copies), and the `compareTo` of the tuples of
/// io.deephaven.tuple.generated for two and three sort columns
fn compare_keys(a: &SortKey, b: &SortKey) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        let order = match (x.is_null(), y.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => x.compare(y),
        };
        if order != Ordering::Equal {
            return order;
        }
    }
    Ordering::Equal
}

/// The value of an output column in the row of each group whose sort key is least
/// (`first`) or greatest. Among rows with equal keys, sorted first takes the one with the
/// least row key and sorted last the one with the greatest. Nulls in the sort columns are
/// values, sorted first. Null for an empty group.
///
/// The best row of a group is found again from the group's rows whenever the group
/// changes. Java keeps a sorted array per group instead; the answer is the same.
///
/// Behavior: io.deephaven.engine.table.impl.by.SortedFirstOrLastChunkedOperator (a sorted
/// array of value and row key per group, whose first or last entry is the answer) and
/// io.deephaven.engine.table.impl.by.DoubleAddOnlySortedFirstOrLastChunkedOperator (and its
/// per-type copies) for a static or add-only parent, which keep the best row so far
pub struct SortedFirstLast {
    first: bool,
    sort: Vec<SortColumn>,
    output: Arc<dyn ColumnSource>,
    output_type: ColumnType,
    /// The best row of each group and its sort key.
    best: Vec<(RowKey, SortKey)>,
    value: Vec<Scalar>,
}

impl SortedFirstLast {
    pub fn new(
        first: bool,
        sort: Vec<SortColumn>,
        output: Arc<dyn ColumnSource>,
        output_type: ColumnType,
    ) -> SortedFirstLast {
        SortedFirstLast {
            first,
            sort,
            output,
            output_type,
            best: Vec::new(),
            value: Vec::new(),
        }
    }

    /// The sort keys of `rows`, current values.
    fn keys(&self, rows: &RowSet) -> Result<Vec<SortKey>, TableFailure> {
        let mut columns = Vec::with_capacity(self.sort.len());
        for column in &self.sort {
            columns.push(Values::decode(
                &column.column_type,
                &column.source.chunk(rows)?,
            )?);
        }
        Ok((0..rows.size() as usize)
            .map(|position| columns.iter().map(|c| c.scalar(position)).collect())
            .collect())
    }

    /// Whether the row `row_key` with `key` is better than the current best.
    ///
    /// A `Period` sort column, which the planner allows only alone and on a static
    /// parent, fails when two periods that are not null are compared.
    ///
    /// Behavior: DoubleAddOnlySortedFirstOrLastChunkedOperator#addChunk (`leq`/`lt` for
    /// first, `geq`/`gt` for last, depending on which row key is smaller), and
    /// ObjectAddOnlySortedFirstOrLastChunkedOperator#addChunk, whose
    /// io.deephaven.util.compare.ObjectComparisons#compare casts a value to `Comparable`
    fn better(
        &self,
        row_key: RowKey,
        key: &SortKey,
        best: &(RowKey, SortKey),
    ) -> Result<bool, TableFailure> {
        if best.0 == NULL_ROW_KEY {
            return Ok(true);
        }
        if key
            .iter()
            .zip(&best.1)
            .any(|(a, b)| a.is_period() && b.is_period())
        {
            return Err(TableFailure {
                message: "java.lang.ClassCastException: class java.time.Period cannot be cast \
                          to class java.lang.Comparable"
                    .into(),
            });
        }
        let order = compare_keys(key, &best.1);
        Ok(if self.first {
            order == Ordering::Less || (order == Ordering::Equal && row_key < best.0)
        } else {
            order == Ordering::Greater || (order == Ordering::Equal && row_key > best.0)
        })
    }

    /// The best row of `rows`, with its key, from the rows in order as a static parent's
    /// operator sees them.
    fn best_of(&self, rows: &RowSet) -> Result<(RowKey, SortKey), TableFailure> {
        let mut best: (RowKey, SortKey) = (NULL_ROW_KEY, Vec::new());
        let mut start = 0;
        while start < rows.size() {
            let chunk = rows.sub_set_by_position_range(start, start + CHUNK_SIZE);
            for (row_key, key) in chunk.iter().zip(self.keys(&chunk)?) {
                if self.better(row_key, &key, &best)? {
                    best = (row_key, key);
                }
            }
            start += CHUNK_SIZE;
        }
        Ok(best)
    }
}

impl AggOperator for SortedFirstLast {
    fn result_type(&self) -> ColumnType {
        self.output_type.clone()
    }

    /// Behavior: SortedFirstOrLastChunkedOperator#getResultColumns, each an
    /// io.deephaven.engine.table.impl.sources.RedirectedColumnSource
    fn holds_results(&self) -> bool {
        false
    }

    fn needs_group_rows(&self) -> bool {
        true
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.best.len() < slots {
            self.best.resize(slots, (NULL_ROW_KEY, Vec::new()));
            self.value.resize(slots, Scalar::Null);
        }
    }

    fn add(&mut self, _slot: usize, _run: &Run<'_>) -> Effect {
        Effect::RECOMPUTE
    }

    fn remove(&mut self, _slot: usize, _run: &Run<'_>) -> Effect {
        Effect::RECOMPUTE
    }

    fn modify(&mut self, _slot: usize, _old: &Run<'_>, _new: &Run<'_>) -> Effect {
        Effect::RECOMPUTE
    }

    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        let mut row_keys = Vec::with_capacity(slots.len());
        for &slot in slots {
            self.best[slot] = self.best_of(groups.rows(slot))?;
            row_keys.push(self.best[slot].0);
        }
        let values = Values::decode(
            &self.output_type,
            &self.output.chunk_by_keys(&row_keys, false)?,
        )?;
        Ok(slots
            .iter()
            .enumerate()
            .map(|(position, &slot)| {
                let value = values.scalar(position);
                let changed = !value.same(&self.value[slot]);
                self.value[slot] = value;
                changed
            })
            .collect())
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        build(
            &self.output_type,
            slots.iter().map(|&slot| &self.value[slot]),
        )
    }
}
