//! The as-of join: its checks, its initial result and its incremental half.
//!
//! The result has the left table's rows at the left table's row keys. Its added columns
//! read the right table through a row redirection from left row key to right row key. The
//! join keeps, for every value of the exact-match columns, the left rows and the right
//! rows that have it, each ordered by stamp and then by row key; the right row of a left
//! row is found in that order.
//!
//! Java has six implementations of this, chosen by which side is refreshing and by
//! whether there are exact-match columns, over hash tables of segmented sorted arrays, and
//! each with its own bookkeeping of which left rows a change of the right table reaches.
//! This is one implementation over ordered sets. It keeps Java's order of work within a
//! cycle: the left table's removed rows and shifts, then the right table (removes, shifts,
//! adds, modifies), then the left table's added rows. A change of the right table marks
//! the stamps it touches; the left rows whose right row can have changed are the ones
//! between a marked stamp and the next right stamp, and each of them is looked up again.

use super::stamps::{self, primitive, Stamp};
use super::{AsOfJoinError, AsOfMatch, JoinAddition, JoinMatch};
use crate::ops::join::keys::{Cell, Key, KeyColumns};
use crate::table::{reads_through, MergedOperator, QueryTable, TableSpec};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex, PoisonError};

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// Rows of one table that have one key, by stamp and then by row key.
type Stamped = BTreeSet<(Stamp, RowKey)>;

/// The rows of both tables that have one value of the exact-match columns.
#[derive(Default)]
struct Bucket {
    left: Stamped,
    right: Stamped,
    /// Whether a left row has had this key since the join began.
    left_seen: bool,
    /// Whether a right row has had this key since the join began.
    right_seen: bool,
}

/// The keys in use, in key order, so that a cycle visits them in the same order on every
/// run. A join without exact-match columns has the one key `[]`.
type Buckets = BTreeMap<Key, Bucket>;

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
    left_stamp: usize,
    right_stamp: usize,
    /// The added columns: the name in the result and the column of the right table.
    additions: Vec<(String, usize)>,
    /// Whether a key is kept after its last row has gone, with what [`Bucket`] remembers
    /// of it. Java keeps every key in every join; it is only when both tables are
    /// refreshing and there are exact-match columns that what it remembers shows, in
    /// [`AsOfJoin::apply_right`].
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
            if primitive(&left_type) != primitive(&right_type) && key_mismatch.is_none() {
                key_mismatch = Some(AsOfJoinError::KeyTypes {
                    left: pair.left.clone(),
                    left_type,
                    right: pair.right.clone(),
                    right_type,
                });
            }
        }
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
            keep_keys: left.is_refreshing() && right.is_refreshing() && !exact.is_empty(),
        })
    }

    /// The right row for a left row with `stamp`, among the right rows of its key.
    ///
    /// Looking backwards it is the last row, by stamp and then by place in the right
    /// table, whose stamp is not greater than `stamp` (or is less than it, when an equal
    /// stamp is not allowed). Looking forwards it is the first row whose stamp is not
    /// less (or is greater).
    ///
    /// Behavior: `computeRedirections` of the kernels under
    /// io.deephaven.engine.table.impl.join.stamp over the stamps that
    /// io.deephaven.engine.table.impl.AsOfStampContext#getAndCompactStamps sorts and
    /// compacts, keeping the last of several rows with one stamp
    /// (`compactDuplicates` of the kernels under
    /// io.deephaven.engine.table.impl.join.dupcompact). For `raj` Java reverses the right
    /// table first (io.deephaven.engine.table.impl.QueryTable#rajImpl), so the last row of
    /// a stamp there is the first row of the stamp in the right table. The ticking
    /// implementations find the same row in a sorted array of the right rows
    /// (`processEntry` of the classes under io.deephaven.engine.table.impl.ssa).
    fn matched(&self, right: &Stamped, stamp: &Stamp) -> Option<RowKey> {
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

    /// The left rows of `bucket` whose right row can depend on a right row with `stamp`:
    /// those from `stamp` to the next stamp of a right row, in the direction away from
    /// which the join looks, both ends included.
    fn reached<'a>(
        &self,
        bucket: &'a Bucket,
        stamp: &Stamp,
    ) -> impl Iterator<Item = &'a (Stamp, RowKey)> {
        let (lower, upper) = if self.backwards {
            let after = (
                Bound::Excluded((stamp.clone(), RowKey::MAX)),
                Bound::Unbounded,
            );
            let upper = match bucket.right.range(after).next() {
                Some((next, _)) => Bound::Included((next.clone(), RowKey::MAX)),
                None => Bound::Unbounded,
            };
            (Bound::Included((stamp.clone(), RowKey::MIN)), upper)
        } else {
            let before = ..(stamp.clone(), RowKey::MIN);
            let lower = match bucket.right.range(before).next_back() {
                Some((previous, _)) => Bound::Included((previous.clone(), RowKey::MIN)),
                None => Bound::Unbounded,
            };
            (lower, Bound::Included((stamp.clone(), RowKey::MAX)))
        };
        bucket.left.range((lower, upper))
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

impl Plan {
    fn drop_if_empty(&self, buckets: &mut Buckets, key: &Key) {
        if !self.keep_keys
            && buckets
                .get(key)
                .is_some_and(|bucket| bucket.left.is_empty() && bucket.right.is_empty())
        {
            buckets.remove(key);
        }
    }
}

/// The incremental half of a join, and the state it keeps between cycles.
struct AsOfJoin {
    left: Arc<dyn Table>,
    right: Arc<dyn Table>,
    plan: Plan,
    buckets: Mutex<Buckets>,
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

impl AsOfJoin {
    /// The key and stamp of each of `rows` of the left table.
    fn left_rows(&self, rows: &RowSet, use_prev: bool) -> Result<Vec<(Key, Stamp)>, TableFailure> {
        let left = &*self.left;
        let keys = keys_of(left, &self.plan.left_keys, rows, use_prev)?;
        let stamps = stamps::read(left, self.plan.left_stamp, rows, use_prev)?;
        Ok(keys.into_iter().zip(stamps).collect())
    }

    /// The key and stamp of each of `rows` of the right table. Fails for exact-match
    /// columns that cannot be compared, if there are rows.
    ///
    /// Behavior: `buildFromRightSide`, `decorateWithRightSide` and `probeRightSide` of the
    /// hashers under io.deephaven.engine.table.impl.asofjoin.typed, which cast the chunks
    /// of the right table's columns to the types of the left table's columns
    fn right_rows(
        &self,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<Vec<(Key, Stamp)>, AsOfJoinError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(mismatch) = &self.plan.key_mismatch {
            return Err(mismatch.clone());
        }
        let right = &*self.right;
        let keys = keys_of(right, &self.plan.right_keys, rows, use_prev)?;
        let stamps = stamps::read(right, self.plan.right_stamp, rows, use_prev)?;
        Ok(keys.into_iter().zip(stamps).collect())
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
    ) -> Result<(TableSpec, AsOfJoin), AsOfJoinError> {
        let redirection = Arc::new(WritableRowRedirection::new());
        let spec = result_spec(left, right, &plan.additions, &redirection);
        let left_count = left.definition().columns().len();
        let added = left_count..left_count + plan.additions.len();
        let right_columns = ModifiedColumnTransformer::new(
            plan.additions
                .iter()
                .zip(added.clone())
                .map(|((_, source), result)| (*source, [result])),
        );
        let join = AsOfJoin {
            left: left.clone(),
            right: right.clone(),
            plan,
            buckets: Mutex::new(Buckets::new()),
            redirection,
            left_columns: ModifiedColumnTransformer::identity(left_count),
            right_columns,
            all_added: ModifiedColumnSet::of(added),
        };

        {
            let mut buckets = join.buckets.lock().unwrap_or_else(PoisonError::into_inner);
            let right_rows = right.row_set();
            let rows = join.right_rows(&right_rows, false)?;
            for (row, (key, stamp)) in right_rows.iter().zip(rows) {
                let bucket = buckets.entry(key).or_default();
                bucket.right_seen = true;
                bucket.right.insert((stamp, row));
            }
            let left_rows = left.row_set();
            let rows = join.left_rows(&left_rows, false)?;
            for (row, (key, stamp)) in left_rows.iter().zip(rows) {
                let bucket = buckets.entry(key).or_default();
                if let Some(inner) = join.plan.matched(&bucket.right, &stamp) {
                    join.redirection.put(row, inner);
                }
                bucket.left_seen = true;
                bucket.left.insert((stamp, row));
            }
        }
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

    /// Takes the rows the left table removed out of their buckets and out of the
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
        buckets: &mut Buckets,
        update: &TableUpdate,
    ) -> Result<bool, TableFailure> {
        let mut watched = self.plan.left_keys.columns.clone();
        watched.push(self.plan.left_stamp);
        let restamp = !update.modified.is_empty() && update.modified_columns.contains_any(&watched);
        let mut leaving = update.removed.clone();
        if restamp {
            leaving.insert(&modified_pre_shift(update).map_err(failure)?);
        }
        for (row, (key, stamp)) in leaving.iter().zip(self.left_rows(&leaving, true)?) {
            if let Some(bucket) = buckets.get_mut(&key) {
                bucket.left.remove(&(stamp, row));
            }
            self.plan.drop_if_empty(buckets, &key);
            self.redirection.remove(row);
        }

        if !update.shifted.is_empty() {
            let staying = self.left.prev_row_set().minus(&leaving);
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                for (row, (key, stamp)) in rows.iter().zip(self.left_rows(&rows, true)?) {
                    moves.push((key, stamp, row, row + range.delta));
                }
            }
            // Every row leaves its old row key before any row takes a new one: a row may
            // move to where another row was.
            let mut matches = Vec::with_capacity(moves.len());
            for (key, stamp, old, _) in &moves {
                if let Some(bucket) = buckets.get_mut(key) {
                    bucket.left.remove(&(stamp.clone(), *old));
                }
                matches.push(self.redirection.remove(*old));
            }
            for ((key, stamp, _, new), inner) in moves.into_iter().zip(matches) {
                buckets.entry(key).or_default().left.insert((stamp, new));
                if inner != NULL_ROW_KEY {
                    self.redirection.put(new, inner);
                }
            }
        }
        Ok(restamp)
    }

    /// Applies the right table's change to the buckets and brings the left rows it reaches
    /// up to date. Adds the left rows whose added columns may have changed to `modified`,
    /// and returns the columns of the result that changed for them.
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
        buckets: &mut Buckets,
        update: &TableUpdate,
        modified: &mut Vec<RowKey>,
    ) -> Result<ModifiedColumnSet, TableFailure> {
        let mut watched = self.plan.right_keys.columns.clone();
        watched.push(self.plan.right_stamp);
        let replaced =
            !update.modified.is_empty() && update.modified_columns.contains_any(&watched);
        // The stamps a change touched, by key.
        let mut touched: BTreeMap<Key, BTreeSet<Stamp>> = BTreeMap::new();
        let mut touch = |key: &Key, stamp: &Stamp| {
            if !touched.contains_key(key) {
                touched.insert(key.clone(), BTreeSet::new());
            }
            if let Some(stamps) = touched.get_mut(key) {
                stamps.insert(stamp.clone());
            }
        };

        let mut leaving = update.removed.clone();
        if replaced {
            leaving.insert(&modified_pre_shift(update).map_err(failure)?);
        }
        let rows = self.right_rows(&leaving, true).map_err(failure)?;
        for (row, (key, stamp)) in leaving.iter().zip(rows) {
            touch(&key, &stamp);
            if let Some(bucket) = buckets.get_mut(&key) {
                bucket.right.remove(&(stamp, row));
            }
        }

        // Where the rows that were shifted are now, by where they were.
        let mut moved: BTreeMap<RowKey, RowKey> = BTreeMap::new();
        if !update.shifted.is_empty() {
            let staying = self.right.prev_row_set().minus(&leaving);
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                let read = self.right_rows(&rows, true).map_err(failure)?;
                for (row, (key, stamp)) in rows.iter().zip(read) {
                    moves.push((key, stamp, row, row + range.delta));
                }
            }
            for (key, stamp, old, new) in &moves {
                touch(key, stamp);
                moved.insert(*old, *new);
                if let Some(bucket) = buckets.get_mut(key) {
                    bucket.right.remove(&(stamp.clone(), *old));
                }
            }
            for (key, stamp, _, new) in moves {
                buckets.entry(key).or_default().right.insert((stamp, new));
            }
        }

        let mut arriving = update.added.clone();
        if replaced {
            arriving.insert(&update.modified);
        } else {
            let rows = self.right_rows(&update.modified, false).map_err(failure)?;
            for (key, stamp) in rows {
                touch(&key, &stamp);
            }
        }
        let rows = self.right_rows(&arriving, false).map_err(failure)?;
        let before = modified.len();
        for (row, (key, stamp)) in arriving.iter().zip(rows) {
            touch(&key, &stamp);
            let bucket = buckets.entry(key).or_default();
            if !bucket.right_seen {
                bucket.right_seen = true;
                if self.plan.keep_keys && bucket.left_seen {
                    // Java reports every left row of a key as modified when the key's first
                    // right rows arrive: it has kept the left rows unsorted until then, and
                    // does not look at which of them the new right rows are joined with
                    // (`processInitial` in `BucketedChunkedAjMergedListener#process`).
                    modified.extend(bucket.left.iter().map(|(_, row)| *row));
                }
            }
            bucket.right.insert((stamp, row));
        }

        // The right rows whose values are new: a left row that reads one of them has
        // changed, whichever right row it read before.
        let fresh = update.added.union(&update.modified);
        for (key, stamps) in &touched {
            if let Some(bucket) = buckets.get(key) {
                let mut reached: BTreeMap<RowKey, &Stamp> = BTreeMap::new();
                for stamp in stamps {
                    for (left_stamp, row) in self.plan.reached(bucket, stamp) {
                        reached.insert(*row, left_stamp);
                    }
                }
                let rows: Vec<RowKey> = reached.keys().copied().collect();
                let olds = self.redirection.redirect(&rows, false);
                for ((row, stamp), old) in reached.into_iter().zip(olds) {
                    let new = self.plan.matched(&bucket.right, stamp);
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
            self.plan.drop_if_empty(buckets, key);
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
    /// their buckets and finds their right rows.
    ///
    /// Behavior: the last block of
    /// io.deephaven.engine.table.impl.asofjoin.BucketedChunkedAjMergedListener#process and
    /// io.deephaven.engine.table.impl.asofjoin.ZeroKeyChunkedAjMergedListener#process, and
    /// the listeners of `rightStaticAj` and `zeroKeyAjRightStatic` in
    /// io.deephaven.engine.table.impl.AsOfJoinHelper
    fn admit_left(
        &self,
        buckets: &mut Buckets,
        update: &TableUpdate,
        restamp: bool,
    ) -> Result<(), TableFailure> {
        let mut arriving = update.added.clone();
        if restamp {
            arriving.insert(&update.modified);
        }
        for (row, (key, stamp)) in arriving.iter().zip(self.left_rows(&arriving, false)?) {
            let bucket = buckets.entry(key).or_default();
            match self.plan.matched(&bucket.right, &stamp) {
                Some(inner) => {
                    self.redirection.put(row, inner);
                }
                None => {
                    self.redirection.remove(row);
                }
            }
            bucket.left_seen = true;
            bucket.left.insert((stamp, row));
        }
        Ok(())
    }
}

impl MergedOperator for AsOfJoin {
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
        let mut buckets = self.buckets.lock().unwrap_or_else(PoisonError::into_inner);
        let mut columns = ModifiedColumnSet::empty();
        let mut modified: Vec<RowKey> = Vec::new();
        let mut downstream = TableUpdate::empty();

        let mut restamp = false;
        if let Some(update) = &upstream[0] {
            restamp = self.retire_left(&mut buckets, update)?;
        }
        if let Some(update) = &upstream[1] {
            columns = self.apply_right(&mut buckets, update, &mut modified)?;
        }
        if let Some(update) = &upstream[0] {
            self.admit_left(&mut buckets, update, restamp)?;
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
        // buckets after the right table's change was applied.
        downstream.modified = RowSet::from_keys(modified);
        downstream.modified_columns = columns;
        Ok(downstream)
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
        AsOfJoin::start(plan, left, right).map_err(|error| {
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
