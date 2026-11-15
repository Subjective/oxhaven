//! Aggregations of the sorted multiset of a group's values: count distinct, distinct,
//! unique, median and percentile.
//!
//! Java keeps a segmented sorted multiset per group. Here it is a `BTreeMap` from the value
//! to its count. Each entry also keeps the value it was created with: values that Java's
//! comparisons call equal (`0.0` and `-0.0`) share an entry, and the entry shows the bits of
//! the value that created it, as Java's multiset does.

use super::operator::{AggOperator, Effect};
use super::values::{build, Run, Scalar};
use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use oxhaven_contracts::schema::ColumnType;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A value as a multiset key: null first, then Java's ordering ([`Scalar::compare`]), in
/// which `0.0` equals `-0.0` and NaN equals NaN.
#[derive(Clone, Debug)]
struct Key(Scalar);

impl PartialEq for Key {
    fn eq(&self, other: &Key) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Key) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Key) -> Ordering {
        match (self.0.is_null(), other.0.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.0.compare(&other.0),
        }
    }
}

/// The order Java sorts a chunk in before compacting it: `Arrays.sort`, under which `-0.0`
/// comes before `0.0`. Otherwise Java's ordering.
///
/// Behavior: io.deephaven.function.ArraySort#sort(double[], int, int)
fn chunk_order(a: &Scalar, b: &Scalar) -> Ordering {
    Key(a.clone())
        .cmp(&Key(b.clone()))
        .then_with(|| match (a, b) {
            (Scalar::Double(x), Scalar::Double(y)) if *x == 0.0 && *y == 0.0 => {
                y.is_sign_negative().cmp(&x.is_sign_negative())
            }
            _ => Ordering::Equal,
        })
}

/// The distinct values of a run and how often each occurs, sorted, each shown as the first
/// of its equal values in sorted order. Nulls are dropped unless `nulls`, NaNs unless
/// `nans`.
///
/// Behavior: io.deephaven.engine.table.impl.util.compact.DoubleCompactKernel#compactAndCount
/// (and its per-type copies)
fn compact(values: impl Iterator<Item = Scalar>, nulls: bool, nans: bool) -> Vec<(Scalar, u64)> {
    let mut sorted: Vec<Scalar> = values
        .filter(|value| (nulls || !value.is_null()) && (nans || !value.is_nan()))
        .collect();
    sorted.sort_by(chunk_order);
    let mut runs: Vec<(Scalar, u64)> = Vec::new();
    for value in sorted {
        match runs.last_mut() {
            Some((last, count)) if Key(last.clone()) == Key(value.clone()) => *count += 1,
            _ => runs.push((value, 1)),
        }
    }
    runs
}

/// One group's values.
#[derive(Clone, Default, Debug)]
struct Multiset {
    entries: BTreeMap<Key, (Scalar, u64)>,
    total: u64,
}

impl Multiset {
    /// Behavior: io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset#insert;
    /// a value already present keeps the bits it was first inserted with. Returns whether
    /// a value was inserted that the group did not have.
    fn insert(&mut self, runs: &[(Scalar, u64)]) -> bool {
        let distinct = self.entries.len();
        for (value, count) in runs {
            self.entries
                .entry(Key(value.clone()))
                .or_insert_with(|| (value.clone(), 0))
                .1 += count;
            self.total += count;
        }
        self.entries.len() != distinct
    }

    /// Behavior: io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset#remove
    ///
    /// # Panics
    /// If a value is removed more often than it was inserted: the framework only removes
    /// rows it added.
    ///
    /// Returns whether the group lost a value altogether.
    fn remove(&mut self, runs: &[(Scalar, u64)]) -> bool {
        let distinct = self.entries.len();
        for (value, count) in runs {
            let key = Key(value.clone());
            let entry = self
                .entries
                .get_mut(&key)
                .expect("a removed value is in the group");
            entry.1 = entry
                .1
                .checked_sub(*count)
                .expect("a value is not removed more often than it was added");
            self.total -= count;
            if entry.1 == 0 {
                self.entries.remove(&key);
            }
        }
        self.entries.len() != distinct
    }

    /// The value at sorted position `index`, counting repeats.
    fn nth(&self, index: u64) -> &Scalar {
        let mut seen = 0;
        for (value, count) in self.entries.values() {
            seen += count;
            if index < seen {
                return value;
            }
        }
        panic!(
            "position {index} is beyond the {} values of the group",
            self.total
        )
    }
}

/// Values with how many times each occurs.
type Counted = Vec<(Scalar, u64)>;

/// The net change of a modification: what the old values have that the new ones do not,
/// and the reverse, with the values they share cancelled.
///
/// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.DoubleCompactModifications#compactAndCountModifications
fn net(old: Counted, new: Counted) -> (Counted, Counted) {
    let mut added: BTreeMap<Key, (Scalar, u64)> = new
        .into_iter()
        .map(|(value, count)| (Key(value.clone()), (value, count)))
        .collect();
    let mut removed = Vec::new();
    for (value, count) in old {
        match added.get_mut(&Key(value.clone())) {
            Some(entry) => {
                let shared = entry.1.min(count);
                entry.1 -= shared;
                if count > shared {
                    removed.push((value, count - shared));
                }
            }
            None => removed.push((value, count)),
        }
    }
    let added = added
        .into_values()
        .filter(|(_, count)| *count > 0)
        .collect();
    (removed, added)
}

/// What is computed from a group's multiset.
#[derive(Clone, Debug)]
pub enum SetMode {
    /// The number of distinct values; null for a group without one.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.count.DoubleChunkedCountDistinctOperator#setResult
    CountDistinct,
    /// The only distinct value; `non_unique` when there are several, null when none.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.unique.DoubleChunkedUniqueOperator
    Unique { non_unique: Scalar },
    /// The distinct values, sorted, as a vector: null first, NaN last, and of `0.0` and
    /// `-0.0` the one the group was given first. A group that has no values is a null, not
    /// an empty vector, except in an aggregation without keys (`singleton`), where Java
    /// makes the group's set when it is first given rows, whatever they hold, and drops it
    /// only when a removal empties it.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.DoubleChunkedDistinctOperator,
    /// whose bucketed `addChunk` skips a run without values and whose singleton `addChunk`
    /// calls `ssmForSlot` first
    Distinct { singleton: bool },
    /// The value at a percentile of the sorted non-null values; with `average`, for `int`,
    /// `long`, `float` and `double` columns, the mean of the two middle values when the
    /// percentile falls between them.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ssmpercentile.SsmChunkedPercentileOperator
    Percentile { percentile: f64, average: bool },
}

/// One of [`SetMode`] for a column of any type.
pub struct SortedValues {
    column_type: ColumnType,
    mode: SetMode,
    /// Whether nulls and NaNs are values. Count distinct and unique count both or neither;
    /// a percentile counts NaN and never null.
    nulls: bool,
    nans: bool,
    sets: Vec<Multiset>,
    /// For [`SetMode::Distinct`]: whether Java has a set for the group, which is what
    /// tells an empty vector from a null.
    present: Vec<bool>,
}

/// What a group's set amounts to for [`SetMode::Unique`].
enum UniqueState {
    Empty,
    Unique(Scalar),
    NonUnique,
}

impl SortedValues {
    /// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor#makeCountDistinctOperator
    pub fn count_distinct(column_type: ColumnType, count_nulls: bool) -> SortedValues {
        SortedValues::new(
            column_type,
            SetMode::CountDistinct,
            count_nulls,
            count_nulls,
        )
    }

    /// `non_unique` must be null or a value of the column's shape.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor#makeUniqueOperator
    pub fn unique(
        column_type: ColumnType,
        include_nulls: bool,
        non_unique: Scalar,
    ) -> SortedValues {
        SortedValues::new(
            column_type,
            SetMode::Unique { non_unique },
            include_nulls,
            include_nulls,
        )
    }

    /// `singleton` says the aggregation has no key columns; see [`SetMode::Distinct`].
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor#makeDistinctOperator
    pub fn distinct(column_type: ColumnType, include_nulls: bool, singleton: bool) -> SortedValues {
        SortedValues::new(
            column_type,
            SetMode::Distinct { singleton },
            include_nulls,
            include_nulls,
        )
    }

    /// Behavior: io.deephaven.engine.table.impl.by.ssmpercentile.SsmChunkedPercentileOperator#SsmChunkedPercentileOperator
    pub fn percentile(column_type: ColumnType, percentile: f64, average: bool) -> SortedValues {
        // An Instant is never averaged.
        let average = average && column_type != ColumnType::Instant;
        SortedValues::new(
            column_type,
            SetMode::Percentile {
                percentile,
                average,
            },
            false,
            true,
        )
    }

    fn new(column_type: ColumnType, mode: SetMode, nulls: bool, nans: bool) -> SortedValues {
        SortedValues {
            column_type,
            mode,
            nulls,
            nans,
            sets: Vec::new(),
            present: Vec::new(),
        }
    }

    fn unique_state(&self, slot: usize) -> UniqueState {
        let entries = &self.sets[slot].entries;
        match entries.len() {
            0 => UniqueState::Empty,
            1 => UniqueState::Unique(entries.values().next().expect("one entry").0.clone()),
            _ => UniqueState::NonUnique,
        }
    }

    /// Whether Java reports the group's result changed by one step of `unique` from
    /// `before` to the group's state now: always when the group becomes or stops being
    /// empty, never while it stays non-unique or keeps its one value, and when it goes
    /// between one value and several only if that value is not the sentinel, by Java's
    /// equality, under which `-0.0` is `0.0`.
    ///
    /// Behavior: DoubleChunkedUniqueOperator#addToState, #removeFromState, #finishFromSsm
    fn unique_step(&self, slot: usize, before: &UniqueState, non_unique: &Scalar) -> bool {
        let equal = |a: &Scalar, b: &Scalar| Key(a.clone()) == Key(b.clone());
        match (before, self.unique_state(slot)) {
            (UniqueState::Empty, UniqueState::Empty) => false,
            (UniqueState::Empty, _) | (_, UniqueState::Empty) => true,
            (UniqueState::NonUnique, UniqueState::NonUnique) => false,
            (UniqueState::Unique(held), UniqueState::Unique(now)) => !equal(held, &now),
            (UniqueState::Unique(value), UniqueState::NonUnique) => !equal(value, non_unique),
            (UniqueState::NonUnique, UniqueState::Unique(value)) => !equal(&value, non_unique),
        }
    }

    /// The distinct values of `slots`, one vector each.
    fn vectors(&self, slots: &[usize]) -> ArrayRef {
        let mut values: Vec<&Scalar> = Vec::new();
        let mut lengths = Vec::with_capacity(slots.len());
        let mut valid = Vec::with_capacity(slots.len());
        for &slot in slots {
            let entries = &self.sets[slot].entries;
            values.extend(entries.values().map(|(value, _)| value));
            lengths.push(entries.len());
            valid.push(self.present[slot]);
        }
        let DataType::List(field) = self.result_type().arrow_type() else {
            unreachable!("a vector is an Arrow list");
        };
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            build(&self.column_type, values.into_iter()),
            Some(NullBuffer::from(valid)),
        ))
    }

    /// Whether the mean of two middle values is taken: only for these four types.
    fn averages(&self) -> bool {
        matches!(self.mode, SetMode::Percentile { average: true, .. })
            && matches!(
                self.column_type,
                ColumnType::Int | ColumnType::Long | ColumnType::Float | ColumnType::Double
            )
    }

    fn runs(&self, run: &Run<'_>) -> Vec<(Scalar, u64)> {
        compact(run.scalars().map(|(_, value)| value), self.nulls, self.nans)
    }

    /// The result of `slot`.
    fn value(&self, slot: usize) -> Scalar {
        let set = &self.sets[slot];
        match &self.mode {
            SetMode::CountDistinct if set.entries.is_empty() => Scalar::Null,
            SetMode::CountDistinct => Scalar::Long(set.entries.len() as i64),
            SetMode::Unique { non_unique } => match set.entries.len() {
                0 => Scalar::Null,
                1 => set.entries.values().next().expect("one entry").0.clone(),
                _ => non_unique.clone(),
            },
            SetMode::Percentile { percentile, .. } => self.percentile_of(set, *percentile),
            SetMode::Distinct { .. } => unreachable!("distinct has no scalar result"),
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.by.ssmpercentile.DoublePercentileTypeHelper#setResult
    /// and io.deephaven.engine.table.impl.by.ssmpercentile.DoublePercentileTypeMedianHelper#setResult,
    /// with their per-type copies
    fn percentile_of(&self, set: &Multiset, percentile: f64) -> Scalar {
        let total = set.total;
        if total == 0 {
            return Scalar::Null;
        }
        // Only the float and double helpers look for NaN, which sorts last.
        if set
            .entries
            .last_key_value()
            .is_some_and(|(_, (v, _))| v.is_nan())
        {
            return Scalar::Double(f64::NAN);
        }
        let position = (total - 1) as f64 * percentile;
        if !self.averages() {
            // `Math.round`, which for a value that is not negative rounds half up.
            let target = position.round() as i64 + 1;
            return set.nth(target as u64 - 1).clone();
        }
        // `(int)` truncates and saturates.
        let target = (position as i32) as i64 + 1;
        let low = set.nth(target as u64 - 1);
        if target as u64 * 2 != total {
            return match self.column_type {
                ColumnType::Int | ColumnType::Long => {
                    Scalar::Double(low.long().expect("an integer") as f64)
                }
                _ => low.clone(),
            };
        }
        let high = set.nth(target as u64);
        match self.column_type {
            ColumnType::Int => {
                let sum = (low.long().expect("an int") as i32)
                    .wrapping_add(high.long().expect("an int") as i32);
                Scalar::Double(f64::from(sum) / 2.0)
            }
            ColumnType::Long => {
                let sum = low
                    .long()
                    .expect("a long")
                    .wrapping_add(high.long().expect("a long"));
                Scalar::Double(sum as f64 / 2.0)
            }
            ColumnType::Float => {
                let sum =
                    low.double().expect("a float") as f32 + high.double().expect("a float") as f32;
                Scalar::Double(f64::from(sum / 2.0))
            }
            _ => Scalar::Double(
                (low.double().expect("a double") + high.double().expect("a double")) / 2.0,
            ),
        }
    }
}

impl AggOperator for SortedValues {
    fn result_type(&self) -> ColumnType {
        match self.mode {
            SetMode::CountDistinct => ColumnType::Long,
            SetMode::Distinct { .. } => ColumnType::vector(self.column_type.clone()),
            _ if self.averages()
                && matches!(self.column_type, ColumnType::Int | ColumnType::Long) =>
            {
                ColumnType::Double
            }
            _ => self.column_type.clone(),
        }
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.sets.len() < slots {
            self.sets.resize(slots, Multiset::default());
            self.present.resize(slots, false);
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let runs = self.runs(run);
        match &self.mode {
            // Java reports the group modified only for a value it did not have. Without
            // keys a group that had no set gets one, which shows as an empty vector where
            // a null was; Java does not report that, and here it is, so that no cell
            // changes unannounced.
            SetMode::Distinct { singleton } => {
                let appeared = *singleton && !run.is_empty() && !self.present[slot];
                if appeared || !runs.is_empty() {
                    self.present[slot] = true;
                }
                Effect::modified_if(self.sets[slot].insert(&runs) || appeared)
            }
            _ if runs.is_empty() => Effect::UNCHANGED,
            SetMode::Unique { non_unique } => {
                let before = self.unique_state(slot);
                self.sets[slot].insert(&runs);
                Effect::modified_if(self.unique_step(slot, &before, non_unique))
            }
            _ => {
                let before = self.value(slot);
                self.sets[slot].insert(&runs);
                Effect::modified_if(!before.same(&self.value(slot)))
            }
        }
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let runs = self.runs(run);
        if runs.is_empty() {
            return Effect::UNCHANGED;
        }
        match &self.mode {
            SetMode::Distinct { .. } => {
                let lost = self.sets[slot].remove(&runs);
                self.present[slot] = !self.sets[slot].entries.is_empty();
                Effect::modified_if(lost)
            }
            SetMode::Unique { non_unique } => {
                let before = self.unique_state(slot);
                self.sets[slot].remove(&runs);
                Effect::modified_if(self.unique_step(slot, &before, non_unique))
            }
            _ => {
                let before = self.value(slot);
                self.sets[slot].remove(&runs);
                Effect::modified_if(!before.same(&self.value(slot)))
            }
        }
    }

    /// Count distinct, distinct and unique apply the net change; the percentile removes
    /// the old values and then inserts the new ones. Unique reports the removal and the
    /// insertion as two steps unless the group had several values, so a group whose one
    /// value is replaced is reported whatever the sentinel is.
    ///
    /// Behavior: DoubleChunkedCountDistinctOperator#modifyChunk,
    /// DoubleChunkedDistinctOperator#modifyChunk, DoubleChunkedUniqueOperator#modifyChunk
    /// and #modifyState, SsmChunkedPercentileOperator#modifyChunk
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        match &self.mode {
            SetMode::Percentile { .. } => {
                let before = self.value(slot);
                let (removed, added) = (self.runs(old), self.runs(new));
                self.sets[slot].remove(&removed);
                self.sets[slot].insert(&added);
                Effect::modified_if(!before.same(&self.value(slot)))
            }
            SetMode::CountDistinct => {
                let before = self.value(slot);
                let (removed, added) = net(self.runs(old), self.runs(new));
                self.sets[slot].remove(&removed);
                self.sets[slot].insert(&added);
                Effect::modified_if(!before.same(&self.value(slot)))
            }
            SetMode::Distinct { .. } => {
                let (removed, added) = net(self.runs(old), self.runs(new));
                if removed.is_empty() && added.is_empty() {
                    return Effect::UNCHANGED;
                }
                // The values that go and the values that come are different values, so
                // the vector changes if either changes the set.
                let lost = self.sets[slot].remove(&removed);
                let gained = self.sets[slot].insert(&added);
                self.present[slot] = !self.sets[slot].entries.is_empty();
                Effect::modified_if(lost || gained)
            }
            SetMode::Unique { non_unique } => {
                let (removed, added) = net(self.runs(old), self.runs(new));
                let before = self.unique_state(slot);
                if matches!(before, UniqueState::NonUnique) {
                    self.sets[slot].remove(&removed);
                    self.sets[slot].insert(&added);
                    return Effect::modified_if(self.unique_step(slot, &before, non_unique));
                }
                let mut changed = false;
                if !removed.is_empty() {
                    self.sets[slot].remove(&removed);
                    changed = self.unique_step(slot, &before, non_unique);
                }
                if !added.is_empty() {
                    let between = self.unique_state(slot);
                    self.sets[slot].insert(&added);
                    changed |= self.unique_step(slot, &between, non_unique);
                }
                Effect::modified_if(changed)
            }
        }
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        if matches!(self.mode, SetMode::Distinct { .. }) {
            return self.vectors(slots);
        }
        let values: Vec<Scalar> = slots.iter().map(|&slot| self.value(slot)).collect();
        build(&self.result_type(), values.iter())
    }
}
