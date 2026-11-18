//! The as-of join: its checks, its initial result and its incremental half.
//!
//! The result has the left table's rows at the left table's row keys. Its added columns
//! read the right table through a row redirection from left row key to right row key.
//!
//! The values of the exact-match columns are numbered by the shared
//! [`KeyTable`](crate::ops::key_table::KeyTable), a chunk of rows at a time, and the join
//! keeps what it knows about each value in vectors indexed by that number (a slot): the
//! right rows that have it, ordered by stamp and then by row key, and, when the right
//! table is refreshing, the left rows that have it, in the same order. The right row of a
//! left row is found in that order. Stamps are kept typed ([`StampValue`]): a column of 64
//! bits or fewer as one number per row, others as [`Stamp`].
//!
//! The right rows of a static right table are one flat array grouped by slot, sorted once,
//! and a left row's right row is found by a search that starts where the previous left
//! row of its slot ended, so that left rows in stamp order are a merge. A refreshing right
//! table keeps an ordered set per slot.
//!
//! Java has six implementations of this, chosen by which side is refreshing and by
//! whether there are exact-match columns, over hash tables of segmented sorted arrays, and
//! each with its own bookkeeping of which left rows a change of the right table reaches.
//! This is one implementation. It keeps Java's order of work within a cycle: the left
//! table's removed rows and shifts, then the right table (removes, shifts, adds,
//! modifies), then the left table's added rows. A change of the right table marks the
//! stamps it touches; the left rows whose right row can have changed are the ones between
//! a marked stamp and the next right stamp, and each of them is looked up again.

use super::stamps::{self, primitive, Stamp, StampValue, Word};
use super::{AsOfJoinError, AsOfMatch, JoinAddition, JoinMatch};
use crate::ops::join::keys::{Cell, Key, KeyColumns};
use crate::ops::key_table::{is_ported_key, same_object_vector_class, KeyTable, NO_SLOT};
use crate::table::{reads_through, MergedOperator, QueryTable, TableSpec};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use arrow::datatypes::DataType;
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex, PoisonError};

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// Left rows read at a time in the initial pass.
const PIECE: u64 = 1 << 16;

/// Rows of one table that have one key, by stamp and then by row key.
type Stamped<S> = BTreeSet<(S, RowKey)>;

/// The right rows of a static right table: the rows of slot `s` are
/// `starts[s]..starts[s + 1]` of `stamps` and `rows`, by stamp and then by row key. A slot
/// past the end has no rows.
struct Sorted<S> {
    starts: Vec<usize>,
    stamps: Vec<S>,
    rows: Vec<RowKey>,
}

impl<S: StampValue> Sorted<S> {
    /// The right rows `rows`, with their slots and stamps, grouped by slot and sorted.
    fn new(slot_count: usize, slots: &[u32], stamps: Vec<S>, rows: &RowSet) -> Self {
        let mut starts = vec![0usize; slot_count + 1];
        for &slot in slots {
            starts[slot as usize + 1] += 1;
        }
        for slot in 0..slot_count {
            starts[slot + 1] += starts[slot];
        }
        let mut next = starts.clone();
        let mut placed: Vec<Option<S>> = vec![None; stamps.len()];
        let mut keys = vec![NULL_ROW_KEY; stamps.len()];
        for ((&slot, stamp), row) in slots.iter().zip(stamps).zip(rows.iter()) {
            let at = &mut next[slot as usize];
            placed[*at] = Some(stamp);
            keys[*at] = row;
            *at += 1;
        }
        let mut sorted: Vec<S> = placed
            .into_iter()
            .map(|stamp| stamp.expect("every place was filled"))
            .collect();
        // Rows came in row key order, so a group already in stamp order is sorted.
        for slot in 0..slot_count {
            let range = starts[slot]..starts[slot + 1];
            if !sorted[range.clone()].is_sorted() {
                let mut pairs: Vec<(S, RowKey)> = sorted[range.clone()]
                    .iter()
                    .cloned()
                    .zip(keys[range.clone()].iter().copied())
                    .collect();
                pairs.sort();
                for (at, (stamp, row)) in range.zip(pairs) {
                    sorted[at] = stamp;
                    keys[at] = row;
                }
            }
        }
        Sorted {
            starts,
            stamps: sorted,
            rows: keys,
        }
    }

    /// The stamps and row keys of the right rows of `slot`.
    fn group(&self, slot: u32) -> (&[S], &[RowKey]) {
        let slot = slot as usize;
        if slot == NO_SLOT as usize || slot + 1 >= self.starts.len() {
            return (&[], &[]);
        }
        let range = self.starts[slot]..self.starts[slot + 1];
        (&self.stamps[range.clone()], &self.rows[range])
    }
}

/// The right rows of each slot.
enum Right<S> {
    /// A static right table's, which never change.
    Fixed(Sorted<S>),
    /// A refreshing right table's.
    Ticking(Vec<Stamped<S>>),
}

/// How the values of the exact-match columns are given slots.
enum Numbering {
    /// The shared hash table, when the columns of both tables have one kind of key.
    Table(KeyTable),
    /// One key per row, compared cell by cell, when a pair of exact-match columns has two
    /// types that Java reads alike but that the hash table keeps apart (two object
    /// classes, or a `Boolean` and a `byte` among several columns).
    Cells(BTreeMap<Key, u32>),
}

impl Numbering {
    fn len(&self) -> usize {
        match self {
            Numbering::Table(table) => table.len(),
            Numbering::Cells(cells) => cells.len(),
        }
    }

    /// The slot of the key of each of `rows` of `table`; with `insert`, a key without one
    /// gets the next, and otherwise [`NO_SLOT`].
    fn slots(
        &mut self,
        table: &dyn Table,
        columns: &KeyColumns,
        rows: &RowSet,
        use_prev: bool,
        insert: bool,
    ) -> Result<Vec<u32>, TableFailure> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let count = rows.size() as usize;
        match self {
            Numbering::Table(keys) => {
                let chunks = columns.chunks(table, rows, use_prev)?;
                Ok(if insert {
                    keys.insert(&chunks, count)
                } else {
                    keys.find(&chunks, count)
                })
            }
            Numbering::Cells(cells) => {
                let keys = keys_of(table, columns, rows, use_prev)?;
                Ok(keys
                    .into_iter()
                    .map(|key| match cells.get(&key) {
                        Some(slot) => *slot,
                        None if insert => {
                            let slot = cells.len() as u32;
                            cells.insert(key, slot);
                            slot
                        }
                        None => NO_SLOT,
                    })
                    .collect())
            }
        }
    }
}

/// What the join keeps between cycles.
struct State<S> {
    numbering: Numbering,
    right: Right<S>,
    /// The left rows of each slot; kept only when the right table is refreshing, the one
    /// case in which a change reaches left rows other than the changed ones.
    left: Vec<Stamped<S>>,
    /// Whether a left row has had the slot's key since the join began.
    left_seen: Vec<bool>,
    /// Whether a right row has had the slot's key since the join began.
    right_seen: Vec<bool>,
}

impl<S: StampValue> State<S> {
    /// Makes the vectors indexed by slot long enough for every slot.
    fn grow(&mut self, track_left: bool) {
        let slots = self.numbering.len();
        if let Right::Ticking(right) = &mut self.right {
            right.resize_with(slots, BTreeSet::new);
        }
        if track_left {
            self.left.resize_with(slots, BTreeSet::new);
        }
        self.left_seen.resize(slots, false);
        self.right_seen.resize(slots, false);
    }

    fn ticking(&mut self) -> &mut Vec<Stamped<S>> {
        match &mut self.right {
            Right::Ticking(right) => right,
            Right::Fixed(_) => unreachable!("a static right table does not change"),
        }
    }
}

/// The partition point of `values` under `before`, which holds for a prefix of them,
/// found from `hint`: by galloping forward from it when the point is not below it, and by
/// a binary search below it otherwise. Left rows that come in stamp order find their
/// points a few places apart.
fn partition_from<S>(values: &[S], hint: usize, before: impl Fn(&S) -> bool) -> usize {
    let hint = hint.min(values.len());
    if hint > 0 && !before(&values[hint - 1]) {
        return values[..hint].partition_point(before);
    }
    let mut low = hint;
    let mut step = 1;
    while low + step <= values.len() && before(&values[low + step - 1]) {
        low += step;
        step *= 2;
    }
    let high = (low + step).min(values.len());
    low + values[low..high].partition_point(before)
}

/// A join's arguments, checked against the two tables.
struct Plan {
    /// Whether the right row is looked for at or before the left stamp (`aj`) or at or
    /// after it (`raj`).
    backwards: bool,
    /// Whether a right row with the left row's stamp is its match.
    allow_equal: bool,
    left_keys: KeyColumns,
    right_keys: KeyColumns,
    /// The first pair of exact-match columns that Java reads as different primitive types.
    key_mismatch: Option<AsOfJoinError>,
    /// Whether the exact-match columns can be numbered by the hash table
    /// ([`Numbering::Table`]).
    hashed: bool,
    /// Whether the stamps are kept as [`Word`]s.
    word_stamps: bool,
    left_stamp: usize,
    right_stamp: usize,
    /// The added columns: the name in the result and the column of the right table.
    additions: Vec<(String, usize)>,
    /// Whether the left rows of each key are kept ([`State::left`]): when the right table
    /// is refreshing.
    track_left: bool,
    /// Whether what [`State`] remembers of a key shows. Java keeps every key in every
    /// join; it is only when both tables are refreshing and there are exact-match columns
    /// that it shows, in [`AsOfJoin::apply_right`].
    keep_keys: bool,
}

/// The columns to add, by name: the right column of every match the left table has no
/// column named like, and then `add` or, without it, the right table's other columns.
/// Refuses names of left columns.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#createColumnsToAddIfMissing, the
/// `revisedAdded` list of io.deephaven.engine.table.impl.QueryTable#ajInternal, and
/// io.deephaven.engine.table.impl.AsOfJoinHelper#checkColumnConflicts
fn revised_additions(
    left: &TableDefinition,
    right: &TableDefinition,
    matches: &[JoinMatch],
    add: &[JoinAddition],
) -> Result<Vec<JoinAddition>, AsOfJoinError> {
    let defaults: Vec<JoinAddition>;
    let add = if add.is_empty() {
        // Java filters by the left name of each match, also where the right name differs.
        let right_columns = right.columns().iter();
        defaults = right_columns
            .filter(|column| !matches.iter().any(|pair| pair.left == column.name))
            .map(|column| JoinAddition::same(column.name.as_str()))
            .collect();
        &defaults
    } else {
        add
    };

    let mut revised: Vec<JoinAddition> = Vec::new();
    let mut from_matches: BTreeSet<&str> = BTreeSet::new();
    for pair in matches {
        if left.index_of(&pair.right).is_none() {
            from_matches.insert(&pair.right);
            revised.push(JoinAddition::same(pair.right.as_str()));
        }
    }
    for addition in add {
        if !from_matches.contains(addition.source.as_str()) {
            revised.push(addition.clone());
        } else if addition.name != addition.source {
            for earlier in &mut revised {
                if earlier.source == addition.source {
                    *earlier = addition.clone();
                }
            }
        }
    }

    let mut conflicts: Vec<String> = Vec::new();
    for addition in &revised {
        if left.index_of(&addition.name).is_some() && !conflicts.contains(&addition.name) {
            conflicts.push(addition.name.clone());
        }
    }
    if conflicts.is_empty() {
        Ok(revised)
    } else {
        Err(AsOfJoinError::ConflictingColumns(conflicts))
    }
}

/// The added columns as the name in the result and the column of the right table. Of
/// several additions with one name the first decides the place and the last the column.
///
/// Behavior: io.deephaven.engine.table.impl.AsOfJoinHelper#makeResult, which puts the
/// columns into a `LinkedHashMap` and throws `NoSuchColumnException` for the first right
/// column that does not exist
fn resolve_additions(
    right: &TableDefinition,
    add: &[JoinAddition],
) -> Result<Vec<(String, usize)>, AsOfJoinError> {
    let mut additions: Vec<(String, usize)> = Vec::with_capacity(add.len());
    for addition in add {
        let Some(source) = right.index_of(&addition.source) else {
            return Err(AsOfJoinError::NoSuchColumn(vec![addition.source.clone()]));
        };
        match additions
            .iter_mut()
            .find(|(name, _)| *name == addition.name)
        {
            Some(earlier) => earlier.1 = source,
            None => additions.push((addition.name.clone(), source)),
        }
    }
    Ok(additions)
}

fn find(definition: &TableDefinition, name: &str) -> Result<usize, AsOfJoinError> {
    definition
        .index_of(name)
        .ok_or_else(|| AsOfJoinError::NoSuchColumn(vec![name.to_string()]))
}

impl Plan {
    /// Behavior: io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin after its shortcut
    /// for a static left table without rows: the lookups of the exact-match columns of the
    /// left table and then of the right table, of the two stamp columns, and the check of
    /// the stamp types; io.deephaven.engine.table.impl.QueryTable#ajInternal for what the
    /// rule means. Java does not compare the types of the exact-match columns: its hash
    /// table has the types of the left table's columns and throws a `ClassCastException`
    /// when it is given a chunk of a right column of another type, which is noted here for
    /// [`AsOfJoin::right_rows`].
    fn new(
        left: &dyn Table,
        right: &dyn Table,
        exact: &[JoinMatch],
        as_of: &AsOfMatch,
        additions: Vec<(String, usize)>,
    ) -> Result<Plan, AsOfJoinError> {
        let (left_definition, right_definition) = (left.definition(), right.definition());
        let mut left_keys = Vec::with_capacity(exact.len());
        for pair in exact {
            left_keys.push(find(left_definition, &pair.left)?);
        }
        let mut right_keys = Vec::with_capacity(exact.len());
        for pair in exact {
            right_keys.push(find(right_definition, &pair.right)?);
        }
        let left_stamp = find(left_definition, &as_of.left)?;
        let right_stamp = find(right_definition, &as_of.right)?;

        let type_of = |definition: &TableDefinition, column: usize| -> ColumnType {
            definition.columns()[column].column_type.clone()
        };
        let left_type = type_of(left_definition, left_stamp);
        let right_type = type_of(right_definition, right_stamp);
        if !stamps::same_stamp_class(&left_type, &right_type) {
            return Err(AsOfJoinError::StampTypes {
                left: left_type,
                right: right_type,
            });
        }
        if let Some(message) = stamps::incomparable(&left_type) {
            return Err(AsOfJoinError::IncomparableStamps(message));
        }
        let mut key_mismatch = None;
        for ((pair, &l), &r) in exact.iter().zip(&left_keys).zip(&right_keys) {
            let left_type = type_of(left_definition, l);
            let right_type = type_of(right_definition, r);
            if left_type != right_type && same_object_vector_class(&left_type, &right_type) {
                unported!(
                    "io.deephaven.engine.table.impl.BucketingContext#BucketingContext",
                    "exact match columns of the vector types {left_type:?} and \
                     {right_type:?}: Java takes the pair (one class, ObjectVector)"
                );
            }
            if !is_ported_key(&left_type) {
                unported!(
                    "io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make",
                    "an exact match column of type {left_type:?}: Java keys vectors of float \
                     or double by the class of the vector objects (-0.0 and 0.0), and arrays \
                     and vectors of arrays by the identity of the arrays"
                );
            }
            if primitive(&left_type) != primitive(&right_type) && key_mismatch.is_none() {
                key_mismatch = Some(AsOfJoinError::KeyTypes {
                    left: pair.left.clone(),
                    left_type,
                    right: pair.right.clone(),
                    right_type,
                });
            }
        }
        let arrow = |column_type: &ColumnType| match column_type.arrow_type() {
            // A time zone is an annotation of the column, not part of its values.
            DataType::Timestamp(unit, Some(_)) => DataType::Timestamp(unit, None),
            other => other,
        };
        let pairs: Vec<(DataType, DataType)> = left_keys
            .iter()
            .zip(&right_keys)
            .map(|(&l, &r)| {
                let left_type = type_of(left_definition, l);
                let right_type = type_of(right_definition, r);
                (arrow(&left_type), arrow(&right_type))
            })
            .collect();
        // One column of 64 bits or fewer is hashed by its value whatever its type, as a
        // `Boolean` of one table and a `byte` of the other need; several columns are
        // hashed as rows of Arrow's row format, which differ between types.
        let hashed = match pairs.as_slice() {
            [(l, r)] if stamps::is_word(l) && stamps::is_word(r) => true,
            pairs => pairs.iter().all(|(l, r)| l == r),
        };
        Ok(Plan {
            backwards: as_of.rule.is_aj(),
            allow_equal: matches!(
                as_of.rule,
                super::AsOfRule::GreaterThanEqual | super::AsOfRule::LessThanEqual
            ),
            left_keys: KeyColumns { columns: left_keys },
            right_keys: KeyColumns {
                columns: right_keys,
            },
            key_mismatch,
            left_stamp,
            right_stamp,
            additions,
            hashed,
            word_stamps: stamps::is_word(&arrow(&left_type)),
            track_left: right.is_refreshing(),
            keep_keys: left.is_refreshing() && right.is_refreshing() && !exact.is_empty(),
        })
    }
    /// Whether a right stamp is before the place of the left stamp in the right rows'
    /// order: the right rows for which this holds are a prefix, and the match is the last
    /// of them looking backwards, or the first after them looking forwards.
    ///
    /// Looking backwards the match is the last row, by stamp and then by place in the
    /// right table, whose stamp is not greater than the left stamp (or is less than it,
    /// when an equal stamp is not allowed). Looking forwards it is the first row whose
    /// stamp is not less (or is greater).
    fn up_to_equal(&self) -> bool {
        self.backwards == self.allow_equal
    }

    /// The right row for a left row with `stamp`, among the right rows `stamps` and `rows`
    /// of its key in their order; the search starts at `hint`, which it moves to where it
    /// ended.
    ///
    /// Behavior: `computeRedirections` of the kernels under
    /// io.deephaven.engine.table.impl.join.stamp over the stamps that
    /// io.deephaven.engine.table.impl.AsOfStampContext#getAndCompactStamps sorts and
    /// compacts, keeping the last of several rows with one stamp
    /// (`compactDuplicates` of the kernels under
    /// io.deephaven.engine.table.impl.join.dupcompact). For `raj` Java reverses the right
    /// table first (io.deephaven.engine.table.impl.QueryTable#rajImpl), so the last row of
    /// a stamp there is the first row of the stamp in the right table.
    fn matched_sorted<S: StampValue>(
        &self,
        stamps: &[S],
        rows: &[RowKey],
        stamp: &S,
        hint: &mut usize,
    ) -> Option<RowKey> {
        let point = if self.up_to_equal() {
            partition_from(stamps, *hint, |right| right <= stamp)
        } else {
            partition_from(stamps, *hint, |right| right < stamp)
        };
        *hint = point;
        if self.backwards {
            point.checked_sub(1).map(|at| rows[at])
        } else {
            rows.get(point).copied()
        }
    }

    /// [`Plan::matched_sorted`] among the right rows of a refreshing right table.
    ///
    /// Behavior: `processEntry` of the classes under io.deephaven.engine.table.impl.ssa,
    /// which find the same row in a sorted array of the right rows
    fn matched<S: StampValue>(&self, right: &Stamped<S>, stamp: &S) -> Option<RowKey> {
        let found = match (self.backwards, self.allow_equal) {
            (true, true) => right.range(..=(stamp.clone(), RowKey::MAX)).next_back(),
            (true, false) => right.range(..(stamp.clone(), RowKey::MIN)).next_back(),
            (false, true) => right.range((stamp.clone(), RowKey::MIN)..).next(),
            (false, false) => {
                let after = (
                    Bound::Excluded((stamp.clone(), RowKey::MAX)),
                    Bound::Unbounded,
                );
                right.range(after).next()
            }
        };
        found.map(|(_, row)| *row)
    }

    /// The left rows of a key whose right row can depend on a right row with `stamp`:
    /// those from `stamp` to the next stamp of a right row, in the direction away from
    /// which the join looks, both ends included.
    fn reached<'a, S: StampValue>(
        &self,
        left: &'a Stamped<S>,
        right: &Stamped<S>,
        stamp: &S,
    ) -> impl Iterator<Item = &'a (S, RowKey)> {
        let (lower, upper) = if self.backwards {
            let after = (
                Bound::Excluded((stamp.clone(), RowKey::MAX)),
                Bound::Unbounded,
            );
            let upper = match right.range(after).next() {
                Some((next, _)) => Bound::Included((next.clone(), RowKey::MAX)),
                None => Bound::Unbounded,
            };
            (Bound::Included((stamp.clone(), RowKey::MIN)), upper)
        } else {
            let before = ..(stamp.clone(), RowKey::MIN);
            let lower = match right.range(before).next_back() {
                Some((previous, _)) => Bound::Included((previous.clone(), RowKey::MIN)),
                None => Bound::Unbounded,
            };
            (lower, Bound::Included((stamp.clone(), RowKey::MAX)))
        };
        left.range((lower, upper))
    }
}

/// The keys of `rows`. A `Boolean` is the byte Java reinterprets it as, so that it equals
/// a `byte` of the other table.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive
/// as called by io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin, and the `eq` of the
/// hashers under io.deephaven.engine.table.impl.asofjoin.typed, which is that of the
/// natural join's
fn keys_of(
    table: &dyn Table,
    columns: &KeyColumns,
    rows: &RowSet,
    use_prev: bool,
) -> Result<Vec<Key>, TableFailure> {
    let mut keys = columns.read(table, rows, use_prev)?;
    for key in &mut keys {
        for cell in key {
            if let Cell::Bool(value) = cell {
                *cell = Cell::Int((*value).into());
            }
        }
    }
    Ok(keys)
}

/// The incremental half of a join, and the state it keeps between cycles.
struct AsOfJoin<S> {
    left: Arc<dyn Table>,
    right: Arc<dyn Table>,
    plan: Plan,
    state: Mutex<State<S>>,
    /// Left row key to the right row key of its match. A left row without a match has no
    /// entry.
    redirection: Arc<WritableRowRedirection>,
    left_columns: ModifiedColumnTransformer,
    right_columns: ModifiedColumnTransformer,
    /// Every added column of the result.
    all_added: ModifiedColumnSet,
}

/// The table an as-of join returns: the left table's columns and, read through
/// `redirection`, the added columns. It has no attributes.
///
/// Behavior: io.deephaven.engine.table.impl.AsOfJoinHelper#makeResult
fn result_spec(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    additions: &[(String, usize)],
    redirection: &Arc<WritableRowRedirection>,
) -> TableSpec {
    let left_count = left.definition().columns().len();
    let mut definition = left.definition().columns().to_vec();
    let mut columns: Vec<Arc<dyn ColumnSource>> =
        (0..left_count).map(|index| left.column(index)).collect();
    let shared: Arc<dyn RowRedirection> = redirection.clone();
    for (name, source) in additions {
        let column_type = right.definition().columns()[*source].column_type.clone();
        definition.push(ColumnDefinition::new(name.as_str(), column_type));
        columns.push(reads_through(RedirectedSource::new(
            shared.clone(),
            right.column(*source),
        )));
    }
    let definition = match TableDefinition::new(definition) {
        Ok(definition) => definition,
        Err(e) => panic!("the added columns passed the name checks: {e}"),
    };
    TableSpec::new(definition, (*left.row_set()).clone(), columns)
}

impl<S: StampValue> AsOfJoin<S> {
    /// The slot and stamp of each of `rows` of the left table; with `insert`, a key the
    /// join has not seen gets a slot, and otherwise [`NO_SLOT`].
    fn left_rows(
        &self,
        state: &mut State<S>,
        rows: &RowSet,
        use_prev: bool,
        insert: bool,
    ) -> Result<(Vec<u32>, Vec<S>), TableFailure> {
        let left = &*self.left;
        let slots = state
            .numbering
            .slots(left, &self.plan.left_keys, rows, use_prev, insert)?;
        state.grow(self.plan.track_left);
        let stamps = stamps::read(left, self.plan.left_stamp, rows, use_prev)?;
        Ok((slots, stamps))
    }

    /// The slot and stamp of each of `rows` of the right table; a key the join has not
    /// seen gets a slot. Fails for exact-match columns that cannot be compared, if there
    /// are rows.
    ///
    /// Behavior: `buildFromRightSide`, `decorateWithRightSide` and `probeRightSide` of the
    /// hashers under io.deephaven.engine.table.impl.asofjoin.typed, which cast the chunks
    /// of the right table's columns to the types of the left table's columns
    fn right_rows(
        &self,
        state: &mut State<S>,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<(Vec<u32>, Vec<S>), AsOfJoinError> {
        if rows.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        if let Some(mismatch) = &self.plan.key_mismatch {
            return Err(mismatch.clone());
        }
        let right = &*self.right;
        let slots = state
            .numbering
            .slots(right, &self.plan.right_keys, rows, use_prev, true)?;
        state.grow(self.plan.track_left);
        let stamps = stamps::read(right, self.plan.right_stamp, rows, use_prev)?;
        Ok((slots, stamps))
    }

    /// Reads both tables and returns the result's initial state with the operator.
    ///
    /// Behavior: the initial pass of `rightStaticAj`, `rightTickingLeftStaticAj`,
    /// `bothIncrementalAj` and the three `zeroKeyAj` methods of
    /// io.deephaven.engine.table.impl.AsOfJoinHelper
    fn start(
        plan: Plan,
        left: &Arc<dyn Table>,
        right: &Arc<dyn Table>,
    ) -> Result<(TableSpec, AsOfJoin<S>), AsOfJoinError> {
        let numbering = if plan.hashed {
            Numbering::Table(KeyTable::new())
        } else {
            Numbering::Cells(BTreeMap::new())
        };
        let mut state = State {
            numbering,
            right: Right::Ticking(Vec::new()),
            left: Vec::new(),
            left_seen: Vec::new(),
            right_seen: Vec::new(),
        };
        let left_count = left.definition().columns().len();
        let added = left_count..left_count + plan.additions.len();
        let right_columns = ModifiedColumnTransformer::new(
            plan.additions
                .iter()
                .zip(added.clone())
                .map(|((_, source), result)| (*source, [result])),
        );
        let mut join = AsOfJoin {
            left: left.clone(),
            right: right.clone(),
            plan,
            state: Mutex::new(State {
                numbering: Numbering::Cells(BTreeMap::new()),
                right: Right::Ticking(Vec::new()),
                left: Vec::new(),
                left_seen: Vec::new(),
                right_seen: Vec::new(),
            }),
            redirection: Arc::new(WritableRowRedirection::new()),
            left_columns: ModifiedColumnTransformer::identity(left_count),
            right_columns,
            all_added: ModifiedColumnSet::of(added),
        };

        let right_rows = right.row_set();
        let (slots, stamps) = join.right_rows(&mut state, &right_rows, false)?;
        for &slot in &slots {
            state.right_seen[slot as usize] = true;
        }
        if right.is_refreshing() {
            let sets = state.ticking();
            for ((slot, stamp), row) in slots.into_iter().zip(stamps).zip(right_rows.iter()) {
                sets[slot as usize].insert((stamp, row));
            }
        } else {
            let sorted = Sorted::new(state.numbering.len(), &slots, stamps, &right_rows);
            state.right = Right::Fixed(sorted);
        }

        let left_rows = left.row_set();
        let mut inner: Vec<RowKey> = Vec::with_capacity(left_rows.size() as usize);
        let mut hints: Vec<usize> = Vec::new();
        let track_left = join.plan.track_left;
        for start in (0..left_rows.size()).step_by(PIECE as usize) {
            let rows = left_rows.sub_set_by_position_range(start, start + PIECE);
            let (slots, stamps) = join.left_rows(&mut state, &rows, false, track_left)?;
            match &state.right {
                Right::Fixed(sorted) => {
                    hints.resize(state.numbering.len(), 0);
                    for (&slot, stamp) in slots.iter().zip(&stamps) {
                        let (stamps, rows) = sorted.group(slot);
                        let found = match hints.get_mut(slot as usize) {
                            Some(hint) => join.plan.matched_sorted(stamps, rows, stamp, hint),
                            None => None,
                        };
                        inner.push(found.unwrap_or(NULL_ROW_KEY));
                    }
                }
                Right::Ticking(sets) => {
                    for (&slot, stamp) in slots.iter().zip(&stamps) {
                        let found = join.plan.matched(&sets[slot as usize], stamp);
                        inner.push(found.unwrap_or(NULL_ROW_KEY));
                    }
                }
            }
            if track_left {
                for ((slot, stamp), row) in slots.into_iter().zip(stamps).zip(rows.iter()) {
                    state.left_seen[slot as usize] = true;
                    state.left[slot as usize].insert((stamp, row));
                }
            }
        }
        join.redirection = Arc::new(WritableRowRedirection::from_rows(&left_rows, inner));
        join.state = Mutex::new(state);
        let spec = result_spec(left, right, &join.plan.additions, &join.redirection);
        let graph = [left, right]
            .into_iter()
            .filter(|table| table.is_refreshing())
            .find_map(|table| table.update_graph());
        if let Some(graph) = graph {
            let clock: Arc<dyn Clock> = Arc::new(graph);
            join.redirection.start_tracking_prev(clock);
        }
        Ok((spec, join))
    }

    /// Takes the rows the left table removed out of their keys' rows and out of the
    /// redirection, with them the modified rows whose key or stamp may have changed, and
    /// moves the rows the left table shifted. Returns whether modified rows were taken out.
    ///
    /// Behavior: the first block of
    /// io.deephaven.engine.table.impl.asofjoin.BucketedChunkedAjMergedListener#process and
    /// io.deephaven.engine.table.impl.asofjoin.ZeroKeyChunkedAjMergedListener#process, and
    /// the listeners of `rightStaticAj` and `zeroKeyAjRightStatic` in
    /// io.deephaven.engine.table.impl.AsOfJoinHelper
    fn retire_left(
        &self,
        state: &mut State<S>,
        update: &TableUpdate,
    ) -> Result<bool, TableFailure> {
        let track = self.plan.track_left;
        let mut watched = self.plan.left_keys.columns.clone();
        watched.push(self.plan.left_stamp);
        let restamp = !update.modified.is_empty() && update.modified_columns.contains_any(&watched);
        let mut leaving = update.removed.clone();
        if restamp {
            leaving.insert(&modified_pre_shift(update).map_err(failure)?);
        }
        if track {
            let (slots, stamps) = self.left_rows(state, &leaving, true, true)?;
            for ((row, slot), stamp) in leaving.iter().zip(slots).zip(stamps) {
                state.left[slot as usize].remove(&(stamp, row));
            }
        }
        for row in leaving.iter() {
            self.redirection.remove(row);
        }

        if !update.shifted.is_empty() {
            let staying = self.left.prev_row_set().minus(&leaving);
            // The slot, stamp, old row key and new row key of each row that moves.
            let mut moves: Vec<(u32, Option<S>, RowKey, RowKey)> = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                if track {
                    let (slots, stamps) = self.left_rows(state, &rows, true, true)?;
                    for ((row, slot), stamp) in rows.iter().zip(slots).zip(stamps) {
                        moves.push((slot, Some(stamp), row, row + range.delta));
                    }
                } else {
                    moves.extend(
                        rows.iter()
                            .map(|row| (NO_SLOT, None, row, row + range.delta)),
                    );
                }
            }
            // Every row leaves its old row key before any row takes a new one: a row may
            // move to where another row was.
            let mut matches = Vec::with_capacity(moves.len());
            for (slot, stamp, old, _) in &moves {
                if let Some(stamp) = stamp {
                    state.left[*slot as usize].remove(&(stamp.clone(), *old));
                }
                matches.push(self.redirection.remove(*old));
            }
            for ((slot, stamp, _, new), inner) in moves.into_iter().zip(matches) {
                if let Some(stamp) = stamp {
                    state.left[slot as usize].insert((stamp, new));
                }
                if inner != NULL_ROW_KEY {
                    self.redirection.put(new, inner);
                }
            }
        }
        Ok(restamp)
    }

    /// Applies the right table's change to the rows of each key and brings the left rows
    /// it reaches up to date. Adds the left rows whose added columns may have changed to
    /// `modified`, and returns the columns of the result that changed for them.
    ///
    /// Behavior: the right half of
    /// io.deephaven.engine.table.impl.asofjoin.BucketedChunkedAjMergedListener#process and
    /// io.deephaven.engine.table.impl.asofjoin.ZeroKeyChunkedAjMergedListener#process, and
    /// the listeners of `rightTickingLeftStaticAj` and `zeroKeyAjRightIncremental` in
    /// io.deephaven.engine.table.impl.AsOfJoinHelper: removed rows and the old place of the
    /// modified rows whose key or stamp may have changed, then shifts, then added rows and
    /// the new place of those modified rows, then the modified rows that stayed in place.
    /// Where Java walks the left rows after each removed, shifted, inserted or modified
    /// right row (`processRemovals`, `applyShift`, `processInsertion` and `findModified` of
    /// the classes under io.deephaven.engine.table.impl.ssa), this looks up again every
    /// left row between a touched stamp and the next right stamp.
    fn apply_right(
        &self,
        state: &mut State<S>,
        update: &TableUpdate,
        modified: &mut Vec<RowKey>,
    ) -> Result<ModifiedColumnSet, TableFailure> {
        let mut watched = self.plan.right_keys.columns.clone();
        watched.push(self.plan.right_stamp);
        let replaced =
            !update.modified.is_empty() && update.modified_columns.contains_any(&watched);
        // The stamps a change touched, by slot.
        let mut touched: BTreeMap<u32, BTreeSet<S>> = BTreeMap::new();
        let mut touch = |slot: u32, stamp: &S| {
            touched.entry(slot).or_default().insert(stamp.clone());
        };

        let mut leaving = update.removed.clone();
        if replaced {
            leaving.insert(&modified_pre_shift(update).map_err(failure)?);
        }
        let (slots, stamps) = self.right_rows(state, &leaving, true).map_err(failure)?;
        for ((row, slot), stamp) in leaving.iter().zip(slots).zip(stamps) {
            touch(slot, &stamp);
            state.ticking()[slot as usize].remove(&(stamp, row));
        }

        // Where the rows that were shifted are now, by where they were.
        let mut moved: BTreeMap<RowKey, RowKey> = BTreeMap::new();
        if !update.shifted.is_empty() {
            let staying = self.right.prev_row_set().minus(&leaving);
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                let (slots, stamps) = self.right_rows(state, &rows, true).map_err(failure)?;
                for ((row, slot), stamp) in rows.iter().zip(slots).zip(stamps) {
                    moves.push((slot, stamp, row, row + range.delta));
                }
            }
            for (slot, stamp, old, new) in &moves {
                touch(*slot, stamp);
                moved.insert(*old, *new);
                state.ticking()[*slot as usize].remove(&(stamp.clone(), *old));
            }
            for (slot, stamp, _, new) in moves {
                state.ticking()[slot as usize].insert((stamp, new));
            }
        }

        let mut arriving = update.added.clone();
        if replaced {
            arriving.insert(&update.modified);
        } else {
            let (slots, stamps) = self
                .right_rows(state, &update.modified, false)
                .map_err(failure)?;
            for (slot, stamp) in slots.into_iter().zip(stamps) {
                touch(slot, &stamp);
            }
        }
        let (slots, stamps) = self.right_rows(state, &arriving, false).map_err(failure)?;
        let before = modified.len();
        for ((row, slot), stamp) in arriving.iter().zip(slots).zip(stamps) {
            touch(slot, &stamp);
            let slot = slot as usize;
            if !state.right_seen[slot] {
                state.right_seen[slot] = true;
                if self.plan.keep_keys && state.left_seen[slot] {
                    // Java reports every left row of a key as modified when the key's first
                    // right rows arrive: it has kept the left rows unsorted until then, and
                    // does not look at which of them the new right rows are joined with
                    // (`processInitial` in `BucketedChunkedAjMergedListener#process`).
                    modified.extend(state.left[slot].iter().map(|(_, row)| *row));
                }
            }
            state.ticking()[slot].insert((stamp, row));
        }

        // The right rows whose values are new: a left row that reads one of them has
        // changed, whichever right row it read before.
        let fresh = update.added.union(&update.modified);
        let Right::Ticking(right) = &state.right else {
            unreachable!("a static right table does not change");
        };
        for (slot, stamps) in &touched {
            let (left, right) = (&state.left[*slot as usize], &right[*slot as usize]);
            let mut reached: BTreeMap<RowKey, &S> = BTreeMap::new();
            for stamp in stamps {
                for (left_stamp, row) in self.plan.reached(left, right, stamp) {
                    reached.insert(*row, left_stamp);
                }
            }
            let rows: Vec<RowKey> = reached.keys().copied().collect();
            let olds = self.redirection.redirect(&rows, false);
            for ((row, stamp), old) in reached.into_iter().zip(olds) {
                let new = self.plan.matched(right, stamp);
                // Where the row that was the match is now, if it is still a row.
                let old_now = if old == NULL_ROW_KEY || leaving.contains(old) {
                    None
                } else {
                    Some(moved.get(&old).copied().unwrap_or(old))
                };
                let unchanged = match (old_now, new) {
                    (None, None) => old == NULL_ROW_KEY,
                    (Some(old_now), Some(new)) => old_now == new && !fresh.contains(new),
                    _ => false,
                };
                if !unchanged {
                    modified.push(row);
                }
                match new {
                    Some(new) if new != old => {
                        self.redirection.put(row, new);
                    }
                    None if old != NULL_ROW_KEY => {
                        self.redirection.remove(row);
                    }
                    _ => {}
                }
            }
        }

        if modified.len() == before {
            Ok(ModifiedColumnSet::empty())
        } else if replaced || !update.added.is_empty() || !update.removed.is_empty() {
            // Java does not look at which rows got another right row.
            Ok(self.all_added.clone())
        } else {
            Ok(self.right_columns.transform(&update.modified_columns))
        }
    }

    /// Puts the rows the left table added, and with `restamp` the rows it modified, into
    /// their keys' rows and finds their right rows.
    ///
    /// Behavior: the last block of
    /// io.deephaven.engine.table.impl.asofjoin.BucketedChunkedAjMergedListener#process and
    /// io.deephaven.engine.table.impl.asofjoin.ZeroKeyChunkedAjMergedListener#process, and
    /// the listeners of `rightStaticAj` and `zeroKeyAjRightStatic` in
    /// io.deephaven.engine.table.impl.AsOfJoinHelper
    fn admit_left(
        &self,
        state: &mut State<S>,
        update: &TableUpdate,
        restamp: bool,
    ) -> Result<(), TableFailure> {
        let track = self.plan.track_left;
        let mut arriving = update.added.clone();
        if restamp {
            arriving.insert(&update.modified);
        }
        let (slots, stamps) = self.left_rows(state, &arriving, false, track)?;
        let mut hints: Vec<usize> = Vec::new();
        for ((row, slot), stamp) in arriving.iter().zip(slots).zip(stamps) {
            let found = match &state.right {
                Right::Fixed(sorted) => {
                    let (stamps, rows) = sorted.group(slot);
                    if stamps.is_empty() {
                        None
                    } else {
                        hints.resize(state.numbering.len(), 0);
                        let hint = &mut hints[slot as usize];
                        self.plan.matched_sorted(stamps, rows, &stamp, hint)
                    }
                }
                Right::Ticking(sets) => self.plan.matched(&sets[slot as usize], &stamp),
            };
            match found {
                Some(inner) => {
                    self.redirection.put(row, inner);
                }
                None => {
                    self.redirection.remove(row);
                }
            }
            if track {
                state.left_seen[slot as usize] = true;
                state.left[slot as usize].insert((stamp, row));
            }
        }
        Ok(())
    }
}

impl<S: StampValue> MergedOperator for AsOfJoin<S> {
    /// `upstream[0]` is the left table's change and `upstream[1]` the right table's.
    ///
    /// Behavior: io.deephaven.engine.table.impl.asofjoin.BucketedChunkedAjMergedListener#process
    /// and io.deephaven.engine.table.impl.asofjoin.ZeroKeyChunkedAjMergedListener#process;
    /// with one refreshing table, the listener of the method of
    /// io.deephaven.engine.table.impl.AsOfJoinHelper that built the join
    fn on_update(
        &self,
        _result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut columns = ModifiedColumnSet::empty();
        let mut modified: Vec<RowKey> = Vec::new();
        let mut downstream = TableUpdate::empty();

        let mut restamp = false;
        if let Some(update) = &upstream[0] {
            restamp = self.retire_left(&mut state, update)?;
        }
        if let Some(update) = &upstream[1] {
            columns = self.apply_right(&mut state, update, &mut modified)?;
        }
        if let Some(update) = &upstream[0] {
            self.admit_left(&mut state, update, restamp)?;
            if !update.modified.is_empty() {
                columns = columns.union(&self.left_columns.transform(&update.modified_columns));
                if restamp {
                    columns = columns.union(&self.all_added);
                }
                modified.extend(update.modified.iter());
            }
            downstream.added = update.added.clone();
            downstream.removed = update.removed.clone();
            downstream.shifted = update.shifted.clone();
        }

        // The rows the left table added are not among these: they were put into their
        // keys' rows after the right table's change was applied.
        downstream.modified = RowSet::from_keys(modified);
        downstream.modified_columns = columns;
        Ok(downstream)
    }
}

/// A join of either form of stamp.
enum Started {
    Word(AsOfJoin<Word>),
    Other(AsOfJoin<Stamp>),
}

impl MergedOperator for Started {
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        match self {
            Started::Word(join) => join.on_update(result, upstream),
            Started::Other(join) => join.on_update(result, upstream),
        }
    }
}

/// Behavior: io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin
pub(super) fn join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    as_of: &AsOfMatch,
    add: &[JoinAddition],
) -> Result<Arc<dyn Table>, AsOfJoinError> {
    if left.is_refreshing() && right.is_refreshing() {
        match (left.update_graph(), right.update_graph()) {
            (Some(a), Some(b)) if Arc::ptr_eq(&a, &b) => {}
            _ => return Err(AsOfJoinError::UpdateGraphConflict),
        }
    }
    let mut matches = exact.to_vec();
    matches.push(JoinMatch::new(as_of.left.as_str(), as_of.right.as_str()));
    let add = revised_additions(left.definition(), right.definition(), &matches, add)?;

    if !left.is_refreshing() && left.row_set().is_empty() {
        // Java returns here, before it looks at the match columns: the result has no rows
        // to match. It is static even when the right table is refreshing.
        let additions = resolve_additions(right.definition(), &add)?;
        let redirection = Arc::new(WritableRowRedirection::new());
        let spec = result_spec(left, right, &additions, &redirection);
        return Ok(QueryTable::new_static(spec));
    }

    let additions = resolve_additions(right.definition(), &add);
    let plan = Plan::new(
        &**left,
        &**right,
        exact,
        as_of,
        additions.clone().unwrap_or_default(),
    )?;
    additions?;

    let parents = [left.clone(), right.clone()];
    let mut refused = None;
    let result = QueryTable::derive_merged(&parents, Vec::new(), || {
        let started = if plan.word_stamps {
            AsOfJoin::<Word>::start(plan, left, right)
                .map(|(spec, join)| (spec, Started::Word(join)))
        } else {
            AsOfJoin::<Stamp>::start(plan, left, right)
                .map(|(spec, join)| (spec, Started::Other(join)))
        };
        started.map_err(|error| {
            let failure = failure(&error);
            refused = Some(error);
            failure
        })
    });
    match result {
        Ok(table) => Ok(table),
        Err(failure) => Err(refused.unwrap_or(AsOfJoinError::Table(failure))),
    }
}

#[cfg(test)]
mod tests {
    use super::partition_from;

    /// From every hint, the point is the one a binary search of the whole slice finds.
    #[test]
    fn a_partition_found_from_any_hint_is_the_partition() {
        let values = [1, 2, 2, 2, 3, 5, 5, 8, 9, 9, 9, 9, 12];
        for target in 0..14 {
            for hint in 0..=values.len() + 2 {
                for strict in [false, true] {
                    let before = |v: &i32| if strict { *v < target } else { *v <= target };
                    assert_eq!(
                        partition_from(&values, hint, before),
                        values.partition_point(before),
                        "target {target} hint {hint} strict {strict}"
                    );
                }
            }
        }
        assert_eq!(partition_from::<i32>(&[], 3, |_| true), 0);
    }
}
