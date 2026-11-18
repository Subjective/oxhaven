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
/// With `boxed`, for more than three sort columns, a floating point value is ordered as a
/// boxed `Double` or `Float` is: `-0.0` before `0.0`.
///
/// Behavior: io.deephaven.engine.table.impl.ssa.DoubleSegmentedSortedArray#doComparison
/// (and its per-type copies), and the `compareTo` of the tuples of
/// io.deephaven.tuple.generated for two and three sort columns; for more,
/// io.deephaven.tuple.ArrayTuple#compareTo over the boxed values of
/// io.deephaven.engine.table.impl.tuplesource.MultiColumnTupleSource, which compares with
/// io.deephaven.util.compare.ObjectComparisons#compare, and so java.lang.Double#compareTo
fn compare_keys(a: &SortKey, b: &SortKey, boxed: bool) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        let order = match (x, y) {
            _ if x.is_null() || y.is_null() => y.is_null().cmp(&x.is_null()),
            // Every NaN is one value to Java, after every other, whatever its bits.
            (Scalar::Double(x), Scalar::Double(y)) if boxed => match (x.is_nan(), y.is_nan()) {
                (false, false) => x.total_cmp(y),
                (x_nan, y_nan) => x_nan.cmp(&y_nan),
            },
            _ => x.compare(y),
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
///
/// A blink parent's rows stay aggregated after they leave it: each cycle's added rows are
/// compared with the best key kept so far and its value. Sorted first takes a row whose
/// key is less, so among equal keys the one seen first; sorted last takes one whose key is
/// greater or equal, so the one seen last.
///
/// Behavior: io.deephaven.engine.table.impl.by.DoubleBlinkSortedFirstOrLastChunkedOperator
/// (and its per-type copies) for a blink parent
pub struct SortedFirstLast {
    first: bool,
    /// More than three sort columns: see [`compare_keys`].
    boxed: bool,
    /// The parent is a blink table.
    blink: bool,
    sort: Vec<SortColumn>,
    output: Arc<dyn ColumnSource>,
    output_type: ColumnType,
    /// The best row of each group and its sort key.
    best: Vec<(RowKey, SortKey)>,
    value: Vec<Scalar>,
    /// For a blink parent, the rows each group was given in this cycle, in row order.
    arrived: Vec<Vec<RowKey>>,
}

impl SortedFirstLast {
    pub fn new(
        first: bool,
        blink: bool,
        sort: Vec<SortColumn>,
        output: Arc<dyn ColumnSource>,
        output_type: ColumnType,
    ) -> SortedFirstLast {
        SortedFirstLast {
            first,
            boxed: sort.len() > 3,
            blink,
            sort,
            output,
            output_type,
            best: Vec::new(),
            value: Vec::new(),
            arrived: Vec::new(),
        }
    }

    /// The type the output column's cells are read and written as: a cell of a column of
    /// tables is its handle, copied as a `long` (the result's source keeps the table).
    fn cell_type(&self) -> ColumnType {
        match self.output_type {
            ColumnType::Table(_) => ColumnType::Long,
            ref other => other.clone(),
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
        Ok(Self::rows_of(&columns, rows.size() as usize))
    }

    /// The sort keys of the rows `row_keys`, in that order, current values.
    fn keys_of(&self, row_keys: &[RowKey]) -> Result<Vec<SortKey>, TableFailure> {
        let mut columns = Vec::with_capacity(self.sort.len());
        for column in &self.sort {
            columns.push(Values::decode(
                &column.column_type,
                &column.source.chunk_by_keys(row_keys, false)?,
            )?);
        }
        Ok(Self::rows_of(&columns, row_keys.len()))
    }

    fn rows_of(columns: &[Values], rows: usize) -> Vec<SortKey> {
        (0..rows)
            .map(|position| columns.iter().map(|c| c.scalar(position)).collect())
            .collect()
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
        let order = compare_keys(key, &best.1, self.boxed);
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

impl SortedFirstLast {
    /// Compares the rows each of `slots` was given in this cycle with its best so far, and
    /// takes the value of a better one. The best row's sort key is kept, and its row key
    /// only to read the value: a blink parent's row keys mean nothing after the cycle.
    /// A group's result changed when it took a row, as Java reports it.
    ///
    /// Behavior: DoubleBlinkSortedFirstOrLastChunkedOperator#addChunk (`lt` for first, `geq`
    /// for last)
    fn recompute_blink(&mut self, slots: &[usize]) -> Result<Vec<bool>, TableFailure> {
        let mut taken = Vec::with_capacity(slots.len());
        let mut row_keys = Vec::new();
        for &slot in slots {
            let arrived = std::mem::take(&mut self.arrived[slot]);
            let mut best: Option<(RowKey, SortKey)> = None;
            for (&row_key, key) in arrived.iter().zip(self.keys_of(&arrived)?) {
                let better = match &best {
                    Some((_, best)) => self.blink_better(&key, best),
                    None if self.best[slot].0 == NULL_ROW_KEY => true,
                    None => self.blink_better(&key, &self.best[slot].1),
                };
                if better {
                    best = Some((row_key, key));
                }
            }
            taken.push(best.is_some());
            if let Some(best) = best {
                row_keys.push(best.0);
                self.best[slot] = best;
            }
        }
        let values = Values::decode(
            &self.cell_type(),
            &self.output.chunk_by_keys(&row_keys, false)?,
        )?;
        let mut position = 0;
        for (&slot, &took) in slots.iter().zip(&taken) {
            if took {
                self.value[slot] = values.scalar(position);
                position += 1;
            }
        }
        Ok(taken)
    }

    fn blink_better(&self, key: &SortKey, best: &SortKey) -> bool {
        let order = compare_keys(key, best, self.boxed);
        if self.first {
            order == Ordering::Less
        } else {
            order != Ordering::Less
        }
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
        !self.blink
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.best.len() < slots {
            self.best.resize(slots, (NULL_ROW_KEY, Vec::new()));
            self.value.resize(slots, Scalar::Null);
            if self.blink {
                self.arrived.resize(slots, Vec::new());
            }
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        if self.blink {
            self.arrived[slot].extend(run.row_keys());
        }
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
        if self.blink {
            return self.recompute_blink(slots);
        }
        let mut row_keys = Vec::with_capacity(slots.len());
        for &slot in slots {
            self.best[slot] = self.best_of(&groups.rows(slot))?;
            row_keys.push(self.best[slot].0);
        }
        let values = Values::decode(
            &self.cell_type(),
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
            &self.cell_type(),
            slots.iter().map(|&slot| &self.value[slot]),
        )
    }
}
