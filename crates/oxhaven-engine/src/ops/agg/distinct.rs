//! Aggregations of the sorted multiset of a group's values: count distinct, distinct,
//! unique, median and percentile.
//!
//! Java keeps a segmented sorted multiset per group. Here it is a `BTreeMap` from the value
//! to its count. Each entry also keeps the value it was created with: values that Java's
//! comparisons call equal (`0.0` and `-0.0`) share an entry, and the entry shows the bits of
//! the value that created it, as Java's multiset does.
//!
//! A `BigDecimal` is the exception. Java's multiset of objects orders them by `compareTo`
//! and tells them apart by `equals`, which for `1.0` and `1.00` disagree: they are two
//! entries side by side, in the order they were sorted. Rows given to an empty multiset at
//! once are laid out in that order (an entry's `tie` says where it stands among its equals),
//! which is all a static group of one chunk needs. Once a group holds two such values,
//! where Java's searches of its sorted set land decides what it holds (seen on Deephaven
//! 42.9: removing `1.00` from `1.0, 1.00, 2.5` removes the `1.0`), and that is not ported:
//! the result fails.

use super::operator::{AggOperator, Effect};
use super::values::{build, Run, Scalar};
use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

/// A value as a multiset key: null first, then Java's ordering ([`Scalar::compare`]), in
/// which `0.0` equals `-0.0` and NaN equals NaN, then the place among its equals of a
/// `BigDecimal` entry laid out with others of its value and other scales (0 for every other
/// entry).
#[derive(Clone, Debug)]
struct Key(Scalar, u32);

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
        .then(self.1.cmp(&other.1))
    }
}

/// Whether Java's multiset folds `a` into an entry of `b`: `ObjectComparisons#eq`, which
/// is `equals` for a `BigDecimal` and agrees with the ordering for every other type here.
///
/// Behavior: io.deephaven.util.compare.ObjectComparisons#eq, and the `eq` of the
/// primitive comparisons
fn folds(a: &Scalar, b: &Scalar) -> bool {
    match (a, b) {
        (Scalar::Decimal(_), Scalar::Decimal(_)) => a.same(b),
        _ => Key(a.clone(), 0) == Key(b.clone(), 0),
    }
}

/// The order Java sorts a chunk in before compacting it: `Arrays.sort`, under which `-0.0`
/// comes before `0.0`. Otherwise Java's ordering.
///
/// Behavior: io.deephaven.function.ArraySort#sort(double[], int, int)
fn chunk_order(a: &Scalar, b: &Scalar) -> Ordering {
    Key(a.clone(), 0)
        .cmp(&Key(b.clone(), 0))
        .then_with(|| match (a, b) {
            (Scalar::Double(x), Scalar::Double(y)) if *x == 0.0 && *y == 0.0 => {
                y.is_sign_negative().cmp(&x.is_sign_negative())
            }
            _ => Ordering::Equal,
        })
}

/// The distinct values of a run and how often each occurs, sorted, each shown as the first
/// of its equal values in sorted order. Nulls are dropped unless `nulls`, NaNs unless
/// `nans`. Only neighbours that Java [folds] are one value: `BigDecimal`s of one value and
/// different scales stay apart, in the order of the run, which the sort keeps.
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
            Some((last, count)) if folds(&value, last) => *count += 1,
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
    /// a value was inserted that the group did not have. Runs given to an empty multiset
    /// are laid out as they come: neighbours that compare equal are entries side by side
    /// (`makeLeavesInitial` of ObjectSegmentedSortedMultiset).
    fn insert(&mut self, runs: &[(Scalar, u64)]) -> bool {
        let distinct = self.entries.len();
        let empty = self.entries.is_empty();
        let mut previous: Option<&Scalar> = None;
        let mut tie = 0;
        for (value, count) in runs {
            tie = match previous {
                Some(previous) if empty && Key(previous.clone(), 0) == Key(value.clone(), 0) => {
                    tie + 1
                }
                _ => 0,
            };
            previous = Some(value);
            self.entries
                .entry(Key(value.clone(), tie))
                .or_insert_with(|| (value.clone(), 0))
                .1 += count;
            self.total += count;
        }
        self.entries.len() != distinct
    }

    /// Two values of the multiset that compare equal but that Java does not fold: entries
    /// of a `BigDecimal` laid out side by side.
    fn split(&self) -> Option<(Scalar, Scalar)> {
        let (later, (value, _)) = self.entries.iter().find(|(key, _)| key.1 > 0)?;
        let (_, (first, _)) = self.entries.get_key_value(&Key(later.0.clone(), 0))?;
        Some((first.clone(), value.clone()))
    }

    /// Two values that make Java's insertion of `runs` depend on its sorted set's layout:
    /// one of a [split](Multiset::split) multiset, two of the runs or one of the runs and
    /// an entry that compare equal and are not folded. None when the multiset is empty and
    /// `fresh`, where the runs are laid out as they come.
    fn conflict(&self, runs: &[(Scalar, u64)], fresh: bool) -> Option<(Scalar, Scalar)> {
        if self.entries.is_empty() && fresh {
            return None;
        }
        if let Some(pair) = self.split() {
            return Some(pair);
        }
        let mut previous: Option<&Scalar> = None;
        for (value, _) in runs {
            if let Some(previous) = previous {
                if Key(previous.clone(), 0) == Key(value.clone(), 0) {
                    return Some((previous.clone(), value.clone()));
                }
            }
            previous = Some(value);
            if let Some((held, _)) = self.entries.get(&Key(value.clone(), 0)) {
                if !folds(value, held) {
                    return Some((held.clone(), value.clone()));
                }
            }
        }
        None
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
            let key = Key(value.clone(), 0);
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

/// A value as Java's compaction of modifications tells values apart: by the ordering, and a
/// `BigDecimal` also by its scale (`ObjectComparisons#eq`).
fn net_key(value: &Scalar) -> (Key, i64) {
    let scale = match value {
        Scalar::Decimal(decimal) => i64::from(decimal.scale),
        _ => 0,
    };
    (Key(value.clone(), 0), scale)
}

/// The net change of a modification: what the old values have that the new ones do not,
/// and the reverse, with the values they share cancelled.
///
/// Behavior: io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications.DoubleCompactModifications#compactAndCountModifications
fn net(old: Counted, new: Counted) -> (Counted, Counted) {
    let mut added: BTreeMap<(Key, i64), (Scalar, u64)> = new
        .into_iter()
        .map(|(value, count)| (net_key(&value), (value, count)))
        .collect();
    let mut removed = Vec::new();
    for (value, count) in old {
        match added.get_mut(&net_key(&value)) {
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
    /// an empty vector, except in an aggregation without keys (`singleton`) and for an
    /// initial group, where Java makes the group's set when it is first given rows,
    /// whatever they hold, and drops it only when a removal empties it.
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
    /// Why the result fails: a group of `BigDecimal`s whose answer Java leaves to the
    /// layout of its sorted set. Once set, the sets are no longer changed.
    failure: Option<TableFailure>,
    failed: bool,
}

/// The refusal of a group of `BigDecimal`s that holds, or is given, `a` and `b`.
fn split_failure(a: &Scalar, b: &Scalar) -> TableFailure {
    let text = |value: &Scalar| match value {
        Scalar::Decimal(decimal) => decimal.text(),
        _ => format!("{value:?}"),
    };
    TableFailure {
        message: format!(
            "unported: io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset: a \
             group that holds BigDecimals of one value and different scales ({} and {}) \
             once more rows come or go, where Java's sorted set answers by its layout",
            text(a),
            text(b)
        ),
    }
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
            failure: None,
            failed: false,
        }
    }

    /// Whether the change of `slot` may go ahead: always, but for a group of `BigDecimal`s
    /// where Java's answer depends on its sorted set's layout (see the module's
    /// documentation), which fails the result. `added` are the runs to insert, after the
    /// removal of `removed`; `fresh` says whether runs given to an empty set are laid out
    /// as they come.
    fn allowed(&mut self, slot: usize, removed: &[(Scalar, u64)], added: &[(Scalar, u64)], fresh: bool) -> bool {
        if self.failed {
            return false;
        }
        if self.column_type != ColumnType::BigDecimal {
            return true;
        }
        let set = &self.sets[slot];
        let pair = if removed.is_empty() {
            None
        } else {
            set.split()
        };
        let pair = pair.or_else(|| {
            if added.is_empty() {
                return None;
            }
            if removed.is_empty() {
                return set.conflict(added, fresh);
            }
            let mut after = set.clone();
            after.remove(removed);
            after.conflict(added, fresh)
        });
        match pair {
            None => true,
            Some((a, b)) => {
                self.failure = Some(split_failure(&a, &b));
                self.failed = true;
                false
            }
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
        let equal = |a: &Scalar, b: &Scalar| folds(a, b);
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

    /// Behavior: the `getResultColumns` of the distinct operators, such as
    /// io.deephaven.engine.table.impl.by.ssmcountdistinct.distinct.DoubleChunkedDistinctOperator,
    /// each an io.deephaven.engine.table.impl.by.ssmcountdistinct.SsmBackedColumnSource; the
    /// count and unique operators have array sources.
    fn holds_results(&self) -> bool {
        !matches!(self.mode, SetMode::Distinct { .. })
    }

    fn ensure_slots(&mut self, slots: usize) {
        if self.sets.len() < slots {
            self.sets.resize(slots, Multiset::default());
            // Behavior: without key columns Java gives the operator the table's rows once
            // when the aggregation is made, also when there are none, and its singleton
            // `addChunk` makes the set before it looks at them: the group of a table
            // without rows, which `preserve_empty` keeps, is an empty vector until a
            // removal empties a set that held values (observed on Java 42.9,
            // tasks/4.20-fuzz-findings-2.md, D3).
            let singleton = matches!(self.mode, SetMode::Distinct { singleton: true });
            self.present.resize(slots, singleton);
        }
    }

    /// Behavior: the singleton `addChunk` of the distinct operators, which makes the
    /// group's set before it looks at the chunk: an initial group is an empty vector, not
    /// a null, until a removal empties it.
    fn seeded(&mut self, slot: usize) {
        if matches!(self.mode, SetMode::Distinct { .. }) {
            self.present[slot] = true;
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let runs = self.runs(run);
        if !self.allowed(slot, &[], &runs, true) {
            return Effect::UNCHANGED;
        }
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
        if runs.is_empty() || !self.allowed(slot, &runs, &[], true) {
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
        let (removed, added) = match &self.mode {
            SetMode::Percentile { .. } => (self.runs(old), self.runs(new)),
            _ => net(self.runs(old), self.runs(new)),
        };
        if !self.allowed(slot, &removed, &added, false) {
            return Effect::UNCHANGED;
        }
        match &self.mode {
            SetMode::Percentile { .. } => {
                let before = self.value(slot);
                self.sets[slot].remove(&removed);
                self.sets[slot].insert(&added);
                Effect::modified_if(!before.same(&self.value(slot)))
            }
            SetMode::CountDistinct => {
                let before = self.value(slot);
                self.sets[slot].remove(&removed);
                self.sets[slot].insert(&added);
                Effect::modified_if(!before.same(&self.value(slot)))
            }
            SetMode::Distinct { .. } => {
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

    fn take_failure(&mut self) -> Option<TableFailure> {
        self.failure.take()
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        if matches!(self.mode, SetMode::Distinct { .. }) {
            return self.vectors(slots);
        }
        let values: Vec<Scalar> = slots.iter().map(|&slot| self.value(slot)).collect();
        build(&self.result_type(), values.iter())
    }
}
