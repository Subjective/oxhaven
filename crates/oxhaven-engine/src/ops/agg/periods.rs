//! The ordering aggregations of a `Period` column, which Java can answer only for a group
//! whose periods are all one value.

use super::operator::{AggOperator, Effect, GroupReader};
use super::values::{Run, Scalar};
use arrow::array::ArrayRef;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A `Period` as its years, months and days.
type Fields = (i32, i32, i32);

/// `min`, `max`, `median`, percentile, `count_distinct`, `unique` or `distinct` of a
/// `Period` column: the operator `inner`, which fails the result where Java casts a period
/// to `Comparable`, which it is not (`java.lang.ClassCastException`).
///
/// Behavior, seen on Java 42.9 (`tasks/4.25-time-type-ports.md`, Notes):
/// - The minimum and maximum of a static or add-only parent
///   (ObjectChunkedAddOnlyMinMaxOperator) compare each period with the best so far through
///   io.deephaven.util.compare.ObjectComparisons#compare, which returns 0 for one object
///   without a cast: a group fails once it holds two periods that differ. (Two equal
///   periods that are different objects fail there too; this server has no object
///   identity and answers them, as Java answers the periods of one formula constant.)
/// - The others, and the minimum and maximum of any other parent, keep a segmented sorted
///   multiset, whose sort kernel casts: a group fails when the rows given to it at once
///   hold two periods, even one object twice. A period given later that `equals` the one
///   the group holds is counted without a cast; one that differs fails.
/// - A group of nulls and one period is answered.
pub struct PeriodGuard {
    inner: Box<dyn AggOperator>,
    /// For each slot, how many rows hold each period that is not null.
    held: Vec<BTreeMap<Fields, u64>>,
    /// Whether the operator sorts the rows it is given at once (a sorted multiset).
    sorts_runs: bool,
    failure: Option<TableFailure>,
}

impl PeriodGuard {
    pub fn new(inner: Box<dyn AggOperator>, sorts_runs: bool) -> PeriodGuard {
        PeriodGuard {
            inner,
            held: Vec::new(),
            sorts_runs,
            failure: None,
        }
    }

    fn fields<'a>(run: &Run<'a>) -> impl Iterator<Item = Fields> + 'a {
        run.scalars().filter_map(|(_, value)| match value {
            Scalar::Period {
                years,
                months,
                days,
            } => Some((years, months, days)),
            _ => None,
        })
    }

    fn count(&mut self, slot: usize, run: &Run<'_>, add: bool) {
        let held = &mut self.held[slot];
        let mut given = 0;
        for fields in Self::fields(run) {
            given += 1;
            if add {
                *held.entry(fields).or_insert(0) += 1;
            } else if let Some(count) = held.get_mut(&fields) {
                *count -= 1;
                if *count == 0 {
                    held.remove(&fields);
                }
            }
        }
        let sorted_two = self.sorts_runs && given > 1;
        if add && (held.len() > 1 || sorted_two) && self.failure.is_none() {
            self.failure = Some(TableFailure {
                message: "java.lang.ClassCastException: class java.time.Period cannot be cast \
                          to class java.lang.Comparable"
                    .into(),
            });
        }
    }
}

impl AggOperator for PeriodGuard {
    fn result_type(&self) -> ColumnType {
        self.inner.result_type()
    }

    fn needs_group_rows(&self) -> bool {
        self.inner.needs_group_rows()
    }

    fn holds_results(&self) -> bool {
        self.inner.holds_results()
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.held.len() < slots {
            self.held.resize_with(slots, BTreeMap::new);
        }
        self.inner.ensure_slots(slots);
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.count(slot, run, true);
        self.inner.add(slot, run)
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        self.count(slot, run, false);
        self.inner.remove(slot, run)
    }

    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        self.count(slot, old, false);
        self.count(slot, new, true);
        self.inner.modify(slot, old, new)
    }

    fn recompute(
        &mut self,
        slots: &[usize],
        groups: &GroupReader<'_>,
    ) -> Result<Vec<bool>, TableFailure> {
        self.inner.recompute(slots, groups)
    }

    fn shifted(&mut self, slot: usize, before: &RowSet, after: &RowSet) {
        self.inner.shifted(slot, before, after);
    }

    fn take_failure(&mut self) -> Option<TableFailure> {
        self.failure.take().or_else(|| self.inner.take_failure())
    }

    fn shown_empty(&mut self, slot: usize) -> Effect {
        self.inner.shown_empty(slot)
    }

    fn seeded(&mut self, slot: usize) {
        self.inner.seeded(slot);
    }

    fn names_result(&self) -> bool {
        self.inner.names_result()
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        self.inner.results(slots)
    }

    fn begin_cycle(&mut self, upstream: &TableUpdate) {
        self.inner.begin_cycle(upstream);
    }

    fn recomputes_shifted(&self) -> bool {
        self.inner.recomputes_shifted()
    }

    fn result_column(&self, written: Arc<dyn ColumnSource>) -> Arc<dyn ColumnSource> {
        self.inner.result_column(written)
    }

    fn failed(&mut self, failure: &TableFailure) {
        self.inner.failed(failure);
    }
}
