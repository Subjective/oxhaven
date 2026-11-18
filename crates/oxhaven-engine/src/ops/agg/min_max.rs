//! Minimum and maximum.

use super::operator::{AggOperator, Effect, Marks, Rows};
use super::values::{build, Run, Scalar, Shape, Values};
use arrow::array::{Array, ArrayRef};
use oxhaven_contracts::schema::ColumnType;
use std::cmp::Ordering;
use std::collections::BTreeMap;

/// The least or greatest non-null value of a column of any type, by Java's ordering (see
/// [`Scalar::compare`]). Null when the group has no non-null value. A NaN in the group
/// makes both the minimum and the maximum NaN.
///
/// The two zeros compare equal, so which one a group shows is decided by how the result
/// is kept, and Java keeps it in two ways.
///
/// On a parent that never removes rows (a static table is one:
/// io.deephaven.engine.table.impl.BaseTable#isAddOnly) the state is the best value so far,
/// replaced only by a strictly better one, so the first zero given is the one kept.
///
/// On a parent that removes rows Java keeps a sorted multiset of each group's values, and
/// [`Multiset`] is that. A zero in it has the sign of the rows that made its entry, for as
/// long as any zero is in the group: rows arrive a chunk at a time, sorted by
/// `Arrays.sort`, which puts `-0.0` before `0.0`, and equal neighbours are folded into the
/// first, so rows that bring both zeros make a negative entry. The result cell is then
/// written only when the multiset's answer does not compare equal to what the cell holds,
/// so a cell that shows one zero keeps it while the answer is a zero, whatever the entry.
///
/// Behavior: io.deephaven.engine.table.impl.by.DoubleChunkedAddOnlyMinMaxOperator (and its
/// per-type copies) for the first case;
/// io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator,
/// io.deephaven.engine.table.impl.by.ssmminmax.DoubleSetResult#setResult and
/// io.deephaven.engine.table.impl.sources.DoubleArraySource#getAndSetUnsafe for the second.
pub struct MinMax {
    column_type: ColumnType,
    minimum: bool,
    state: State,
}

enum State {
    /// Rows only join a group.
    Growing {
        /// The best value that is not NaN, or null.
        best: Vec<Scalar>,
        /// Whether the group has a NaN.
        nan: Vec<bool>,
    },
    /// [`State::Growing`] for an integer-shaped column ([`Shape::Long`]): the best value
    /// of each group, typed.
    GrowingLongs(Vec<Option<i64>>),
    /// [`State::Growing`] for a floating point column ([`Shape::Double`]): the best
    /// value that is not NaN, typed, and whether the group has a NaN.
    GrowingDoubles {
        best: Vec<Option<f64>>,
        nan: Vec<bool>,
    },
    /// Rows can leave a group.
    Retracting {
        values: Vec<Multiset>,
        /// The result cell of each group.
        cells: Vec<Scalar>,
    },
}

/// A non-null value in Java's order, in which the two zeros are one value and so are all
/// NaNs.
struct Ordered(Scalar);

impl PartialEq for Ordered {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Ordered {}

impl PartialOrd for Ordered {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ordered {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.compare(&other.0)
    }
}

/// The non-null values of a group, each distinct value once with the number of rows that
/// have it. The value kept for an entry is the one that made it.
///
/// Behavior: io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset#insert,
/// #remove (and the per-type copies)
#[derive(Default)]
struct Multiset {
    entries: BTreeMap<Ordered, u64>,
}

fn is_negative_zero(value: &Scalar) -> bool {
    matches!(value, Scalar::Double(v) if *v == 0.0 && v.is_sign_negative())
}

impl Multiset {
    /// Takes the values of rows that joined the group in one call. Java sorts them first,
    /// and its sort puts `-0.0` before `0.0`, so a negative zero among them is the one
    /// that makes a new zero entry.
    ///
    /// Behavior: io.deephaven.engine.table.impl.util.compact.DoubleCompactKernel#compactAndCount
    fn insert(&mut self, run: &Run<'_>) {
        let mut values: Vec<Scalar> = run
            .scalars()
            .map(|(_, value)| value)
            .filter(|value| !value.is_null())
            .collect();
        if let Some(at) = values.iter().position(is_negative_zero) {
            values.swap(0, at);
        }
        for value in values {
            *self.entries.entry(Ordered(value)).or_insert(0) += 1;
        }
    }

    fn remove(&mut self, run: &Run<'_>) {
        for (_, value) in run.scalars() {
            if value.is_null() {
                continue;
            }
            let value = Ordered(value);
            if let Some(count) = self.entries.get_mut(&value) {
                *count -= 1;
                if *count == 0 {
                    self.entries.remove(&value);
                }
            }
        }
    }

    /// The least or greatest value, NaN when there is one, null when there is none.
    ///
    /// Behavior: io.deephaven.engine.table.impl.by.ssmminmax.DoubleSetResult#setResult
    fn extreme(&self, minimum: bool) -> Scalar {
        let last = self.entries.keys().next_back();
        let found = match last {
            Some(last) if minimum && !last.0.is_nan() => self.entries.keys().next(),
            last => last,
        };
        found.map_or(Scalar::Null, |value| value.0.clone())
    }
}

/// Whether `value` takes the place of `best`: when there is none yet, or when `value` is
/// strictly less (`minimum`) or strictly greater. For values that are not NaN this is the
/// order of [`Scalar::compare`], in which the two zeros are equal, so the first zero
/// given stays.
#[inline]
fn takes<T: PartialOrd + Copy>(minimum: bool, value: T, best: Option<T>) -> bool {
    match best {
        None => true,
        Some(best) if minimum => value < best,
        Some(best) => value > best,
    }
}

/// One value of a floating point column joins a group that only grows. Whether the
/// group's result changed.
#[inline]
fn grow_double(minimum: bool, value: f64, best: &mut Option<f64>, nan: &mut bool) -> bool {
    // A group with a NaN can never change.
    if *nan {
        return false;
    }
    if value.is_nan() {
        *nan = true;
        return true;
    }
    let better = takes(minimum, value, *best);
    if better {
        *best = Some(value);
    }
    better
}

impl MinMax {
    /// `retracts` says whether rows can leave a group.
    pub fn new(column_type: ColumnType, minimum: bool, retracts: bool) -> MinMax {
        let state = if retracts {
            State::Retracting {
                values: Vec::new(),
                cells: Vec::new(),
            }
        } else {
            match Shape::of(&column_type) {
                Shape::Long => State::GrowingLongs(Vec::new()),
                Shape::Double => State::GrowingDoubles {
                    best: Vec::new(),
                    nan: Vec::new(),
                },
                _ => State::Growing {
                    best: Vec::new(),
                    nan: Vec::new(),
                },
            }
        };
        MinMax {
            column_type,
            minimum,
            state,
        }
    }

    /// Writes the multiset's answer for `slot` into its cell, unless the cell holds a
    /// value that compares equal to it. Whether the cell changed.
    fn write(&mut self, slot: usize) -> Effect {
        let State::Retracting { values, cells } = &mut self.state else {
            return Effect::UNCHANGED;
        };
        let answer = values[slot].extreme(self.minimum);
        let cell = &mut cells[slot];
        let equal = match (answer.is_null(), cell.is_null()) {
            (true, true) => true,
            (false, false) => answer.compare(cell) == Ordering::Equal,
            _ => false,
        };
        if !equal {
            *cell = answer;
        }
        Effect::modified_if(!equal)
    }
}

impl AggOperator for MinMax {
    fn result_type(&self) -> ColumnType {
        self.column_type.clone()
    }

    fn ensure_slots(&mut self, slots: usize) {
        match &mut self.state {
            State::Growing { best, nan } => {
                if best.len() < slots {
                    best.resize(slots, Scalar::Null);
                    nan.resize(slots, false);
                }
            }
            State::GrowingLongs(best) => {
                if best.len() < slots {
                    best.resize(slots, None);
                }
            }
            State::GrowingDoubles { best, nan } => {
                if best.len() < slots {
                    best.resize(slots, None);
                    nan.resize(slots, false);
                }
            }
            State::Retracting { values, cells } => {
                if values.len() < slots {
                    values.resize_with(slots, Multiset::default);
                    cells.resize(slots, Scalar::Null);
                }
            }
        }
    }

    /// The typed states take rows in row order: a group's best value so far is replaced
    /// by a strictly better one whichever rows came with it in a chunk.
    fn adds_rows(&self) -> bool {
        matches!(
            self.state,
            State::GrowingLongs(_) | State::GrowingDoubles { .. }
        )
    }

    fn add_rows(&mut self, rows: &Rows<'_>, marks: &mut Marks) {
        let minimum = self.minimum;
        match (&mut self.state, rows.values) {
            (State::GrowingLongs(best), Some(Values::Longs(array))) => {
                let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
                for (at, (&slot, &value)) in rows.slots.iter().zip(array.values()).enumerate() {
                    if nulls.is_none_or(|nulls| nulls.is_valid(at))
                        && takes(minimum, value, best[slot])
                    {
                        best[slot] = Some(value);
                        marks.note(slot, Effect::MODIFIED);
                    }
                }
            }
            (State::GrowingDoubles { best, nan }, Some(Values::Doubles(array))) => {
                let nulls = array.nulls().filter(|nulls| nulls.null_count() > 0);
                for (at, (&slot, &value)) in rows.slots.iter().zip(array.values()).enumerate() {
                    if nulls.is_none_or(|nulls| nulls.is_valid(at))
                        && grow_double(minimum, value, &mut best[slot], &mut nan[slot])
                    {
                        marks.note(slot, Effect::MODIFIED);
                    }
                }
            }
            _ => unreachable!("add_rows of a minimum or maximum that takes groups"),
        }
    }

    fn add(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        let wanted = if self.minimum {
            Ordering::Less
        } else {
            Ordering::Greater
        };
        match &mut self.state {
            State::Growing { best, nan } => {
                // A group with a NaN can never change.
                if nan[slot] {
                    return Effect::UNCHANGED;
                }
                let mut changed = false;
                for (_, value) in run.scalars() {
                    if value.is_nan() {
                        nan[slot] = true;
                        return Effect::MODIFIED;
                    }
                    if !value.is_null()
                        && (best[slot].is_null() || value.compare(&best[slot]) == wanted)
                    {
                        best[slot] = value;
                        changed = true;
                    }
                }
                Effect::modified_if(changed)
            }
            State::GrowingLongs(best) => {
                let mut changed = false;
                for value in run.longs().flatten() {
                    if takes(self.minimum, value, best[slot]) {
                        best[slot] = Some(value);
                        changed = true;
                    }
                }
                Effect::modified_if(changed)
            }
            State::GrowingDoubles { best, nan } => {
                let mut changed = false;
                for value in run.doubles().flatten() {
                    changed |= grow_double(self.minimum, value, &mut best[slot], &mut nan[slot]);
                }
                Effect::modified_if(changed)
            }
            State::Retracting { values, .. } => {
                values[slot].insert(run);
                self.write(slot)
            }
        }
    }

    fn remove(&mut self, slot: usize, run: &Run<'_>) -> Effect {
        match &mut self.state {
            // Rows never leave such a group.
            State::Growing { .. } | State::GrowingLongs(_) | State::GrowingDoubles { .. } => {
                Effect::UNCHANGED
            }
            State::Retracting { values, .. } => {
                values[slot].remove(run);
                self.write(slot)
            }
        }
    }

    /// The old values leave and the new ones join before the cell is looked at, so a cell
    /// that shows a zero keeps it when the group's zeros are all replaced.
    ///
    /// Behavior: SsmChunkedMinMaxOperator#modifyChunk
    fn modify(&mut self, slot: usize, old: &Run<'_>, new: &Run<'_>) -> Effect {
        match &mut self.state {
            State::Growing { .. } | State::GrowingLongs(_) | State::GrowingDoubles { .. } => {
                Effect::UNCHANGED
            }
            State::Retracting { values, .. } => {
                values[slot].remove(old);
                values[slot].insert(new);
                self.write(slot)
            }
        }
    }

    fn results(&self, slots: &[usize]) -> ArrayRef {
        let results: Vec<Scalar> = slots
            .iter()
            .map(|&slot| match &self.state {
                State::Growing { best, nan } => {
                    if nan[slot] {
                        Scalar::Double(f64::NAN)
                    } else {
                        best[slot].clone()
                    }
                }
                State::GrowingLongs(best) => best[slot].map_or(Scalar::Null, Scalar::Long),
                State::GrowingDoubles { best, nan } => {
                    if nan[slot] {
                        Scalar::Double(f64::NAN)
                    } else {
                        best[slot].map_or(Scalar::Null, Scalar::Double)
                    }
                }
                State::Retracting { cells, .. } => cells[slot].clone(),
            })
            .collect();
        build(&self.column_type, results.iter())
    }
}
