//! The natural join: its checks, its initial result and its incremental half.
//!
//! The result has the left table's rows at the left table's row keys. Its added columns
//! read the right table through a row redirection from left row key to right row key. The
//! join keeps, for every key, the left rows and the right rows that have it; an update
//! changes those sets and the redirection, and reports the left rows whose right row
//! changed as modified.
//!
//! Java has four implementations of this, chosen by which side is refreshing and by the
//! key type, each with its own hash table. This is one implementation over an ordered map.
//! It keeps Java's order of work within a cycle (right table first: removes, shifts,
//! modifies, adds; then the left table; then the keys the right table touched), because
//! that order decides which combinations of changes fail.

use super::keys::{key_text, Key, KeyColumns};
use super::{JoinAddition, JoinError, JoinMatch, NaturalJoinType};
use crate::table::{reads_through, MergedOperator, QueryTable, TableSpec};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// The rows of both tables that have one key.
#[derive(Default)]
struct Slot {
    left: BTreeSet<RowKey>,
    right: BTreeSet<RowKey>,
}

/// The keys in use, in key order, so that a cycle visits them in the same order on every
/// run. A key stays only while a row of either table has it.
type Slots = BTreeMap<Key, Slot>;

/// What the right table offers for one key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Match {
    /// No right row has the key.
    None,
    /// This right row is the match.
    Row(RowKey),
    /// Several right rows have the key, and the join type does not choose among them.
    Duplicate,
}

/// Behavior: the `joinType` branches of `buildFromRightSide` in the hashers under
/// io.deephaven.engine.table.impl.naturaljoin.typed.staticopen, and
/// io.deephaven.engine.table.impl.NaturalJoinHelper.ModifiedSlotUpdater#accept for the
/// first and last row of a set of duplicates
fn pick(join_type: NaturalJoinType, right: &BTreeSet<RowKey>) -> Match {
    match (right.first(), right.last()) {
        (Some(first), Some(last)) if first == last => Match::Row(*first),
        (Some(first), Some(last)) => match join_type {
            NaturalJoinType::FirstMatch => Match::Row(*first),
            NaturalJoinType::LastMatch => Match::Row(*last),
            NaturalJoinType::ErrorOnDuplicate | NaturalJoinType::ExactlyOneMatch => {
                Match::Duplicate
            }
        },
        _ => Match::None,
    }
}

/// A join's arguments, checked against the two tables.
pub(crate) struct Plan {
    join_type: NaturalJoinType,
    pub(crate) left_keys: KeyColumns,
    pub(crate) right_keys: KeyColumns,
    pub(crate) key_types: Vec<ColumnType>,
    /// The added columns: the name in the result and the column of the right table.
    pub(crate) additions: Vec<(String, usize)>,
}

impl Plan {
    /// Behavior: io.deephaven.engine.table.impl.QueryTable#createColumnsToAddIfMissing for
    /// the default additions, the constructor of
    /// io.deephaven.engine.table.impl.BucketingContext for the name and type checks, and
    /// io.deephaven.engine.table.impl.NaturalJoinHelper#makeResult, which keeps the last of
    /// several additions with one name.
    pub(crate) fn new(
        left: &dyn Table,
        right: &dyn Table,
        on: &[JoinMatch],
        add: &[JoinAddition],
        join_type: NaturalJoinType,
    ) -> Result<Plan, JoinError> {
        let (left_definition, right_definition) = (left.definition(), right.definition());
        let defaults: Vec<JoinAddition>;
        let add = if add.is_empty() {
            // Java filters by the left name of each match, also where the right name differs.
            let right_columns = right_definition.columns().iter();
            defaults = right_columns
                .filter(|column| !on.iter().any(|pair| pair.left == column.name))
                .map(|column| JoinAddition::same(column.name.as_str()))
                .collect();
            &defaults
        } else {
            add
        };

        let conflicts: Vec<String> = add
            .iter()
            .filter(|addition| left_definition.index_of(&addition.name).is_some())
            .map(|addition| addition.name.clone())
            .collect();
        if !conflicts.is_empty() {
            return Err(JoinError::ConflictingColumns(conflicts));
        }

        let find = |definition: &TableDefinition, names: Vec<&String>| {
            let mut found = Vec::with_capacity(names.len());
            let mut missing = Vec::new();
            for name in names {
                match definition.index_of(name) {
                    Some(index) => found.push(index),
                    None => missing.push(name.clone()),
                }
            }
            if missing.is_empty() {
                Ok(found)
            } else {
                Err(JoinError::NoSuchColumn(missing))
            }
        };
        let left_keys = find(left_definition, on.iter().map(|pair| &pair.left).collect())?;
        let right_keys = find(
            right_definition,
            on.iter().map(|pair| &pair.right).collect(),
        )?;
        let mut key_types = Vec::with_capacity(on.len());
        for ((pair, &l), &r) in on.iter().zip(&left_keys).zip(&right_keys) {
            let left_type = &left_definition.columns()[l].column_type;
            let right_type = &right_definition.columns()[r].column_type;
            if left_type != right_type {
                return Err(JoinError::MismatchedTypes {
                    left: pair.left.clone(),
                    left_type: left_type.clone(),
                    right: pair.right.clone(),
                    right_type: right_type.clone(),
                });
            }
            key_types.push(left_type.clone());
        }

        let sources = find(
            right_definition,
            add.iter().map(|addition| &addition.source).collect(),
        )?;
        let mut additions: Vec<(String, usize)> = Vec::with_capacity(add.len());
        for (addition, source) in add.iter().zip(sources) {
            match additions
                .iter_mut()
                .find(|(name, _)| *name == addition.name)
            {
                Some(earlier) => earlier.1 = source,
                None => additions.push((addition.name.clone(), source)),
            }
        }
        Ok(Plan {
            join_type,
            left_keys: KeyColumns { columns: left_keys },
            right_keys: KeyColumns {
                columns: right_keys,
            },
            key_types,
            additions,
        })
    }

    fn zero_keys(&self) -> bool {
        self.key_types.is_empty()
    }

    /// The right row for a left row with `key`, given the right rows that have the key.
    /// `require` says whether an exact join insists on a right row here.
    ///
    /// Behavior: the duplicate checks of `decorateLeftSide` and `addLeftSide` in the hashers
    /// under io.deephaven.engine.table.impl.naturaljoin.typed,
    /// io.deephaven.engine.table.impl.StaticNaturalJoinStateManager#checkExactMatch, and for
    /// a join without match columns
    /// io.deephaven.engine.table.impl.NaturalJoinHelper#checkRightTableSizeZeroKeys
    fn matched(
        &self,
        key: &Key,
        right: &BTreeSet<RowKey>,
        require: bool,
    ) -> Result<Option<RowKey>, JoinError> {
        match pick(self.join_type, right) {
            Match::Row(row) => Ok(Some(row)),
            Match::Duplicate => Err(JoinError::DuplicateRightKey(if self.zero_keys() {
                "naturalJoin with zero key columns may not have more than one row in the right \
                 hand side table!"
                    .to_string()
            } else {
                format!(
                    "Natural Join found duplicate right key for {}",
                    key_text(key, &self.key_types)
                )
            })),
            Match::None if require && self.join_type == NaturalJoinType::ExactlyOneMatch => {
                Err(JoinError::NoRightRow(if self.zero_keys() {
                    "exactJoin with zero key columns must have exactly one row in the right hand \
                     side table!"
                        .to_string()
                } else {
                    format!(
                        "Tables don't have one-to-one mapping - no mappings for key {}.",
                        key_text(key, &self.key_types)
                    )
                }))
            }
            Match::None => Ok(None),
        }
    }
}

/// Behavior: io.deephaven.engine.table.impl.BaseTable#isAddOnly
fn is_add_only(table: &dyn Table) -> bool {
    let attributes = table.attributes();
    !table.is_refreshing() || attributes.add_only || attributes.append_only
}

/// Behavior: io.deephaven.engine.table.impl.BaseTable#isAppendOnly
fn is_append_only(table: &dyn Table) -> bool {
    let attributes = table.attributes();
    !table.is_refreshing() || attributes.append_only || (attributes.add_only && table.is_flat())
}

/// The left table's blink attribute; add-only and append-only when the left table is and no
/// change of the right table can modify a row of the result. A static table counts as both.
///
/// Java copies the left table's attributes with `CopyAttributeOperation.Join`, and nothing
/// from the right table. Of the attributes a table has here, `Join` is listed for
/// `BLINK_TABLE_ATTRIBUTE` and `INPUT_TABLE_ATTRIBUTE`, and not for
/// `ADD_ONLY_TABLE_ATTRIBUTE` or `APPEND_ONLY_TABLE_ATTRIBUTE`, which the join sets itself.
/// The input table is given with `TableSpec::with_input_table_of`. The other attributes
/// copied for `Join` (sortable and filterable columns, layout hints, the snapshot viewport
/// type, key columns, unique keys, the initially empty coalesced source table marker, and
/// every attribute without an entry) are ones no table has here.
///
/// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper#naturalJoin,
/// io.deephaven.engine.table.impl.BaseTable#copyAttributes, and the `Join` entries of
/// `attributeToCopySet` with io.deephaven.engine.table.impl.BaseTable#shouldCopyAttribute
fn join_attributes(left: &dyn Table, right: &dyn Table, plan: &Plan) -> TableAttributes {
    // An exact join already has the one right row of every left row, so an add-only right
    // table has nothing to add to it.
    let right_never_modifies = !right.is_refreshing()
        || (plan.join_type == NaturalJoinType::ExactlyOneMatch && is_add_only(right));
    let mut attributes = TableAttributes::default();
    attributes.blink = left.attributes().blink;
    attributes.add_only = is_add_only(left) && right_never_modifies;
    attributes.append_only = is_append_only(left) && right_never_modifies;
    attributes
}

/// What a cycle did to one key of the right table, kept from the first change to the key
/// until the left rows of the key are brought up to date.
///
/// Behavior: the entries of io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker
struct Touch {
    /// The key's match before the cycle.
    before: Match,
    /// Where the row that was the match before the cycle is now, if it is still a row of
    /// this key: its row key, in the key space the cycle has reached.
    now: Option<RowKey>,
    /// Whether `now` has been shifted in this cycle. A row shifts at most once.
    moved: bool,
    /// Whether a row of the key was added, removed or modified, as opposed to shifted.
    changed: bool,
}

fn touch<'a>(
    touched: &'a mut BTreeMap<Key, Touch>,
    slots: &Slots,
    join_type: NaturalJoinType,
    key: &Key,
) -> &'a mut Touch {
    touched.entry(key.clone()).or_insert_with(|| {
        let before = slots
            .get(key)
            .map_or(Match::None, |slot| pick(join_type, &slot.right));
        Touch {
            before,
            now: match before {
                Match::Row(row) => Some(row),
                Match::None | Match::Duplicate => None,
            },
            moved: false,
            changed: false,
        }
    })
}

fn drop_if_empty(slots: &mut Slots, key: &Key) {
    if slots
        .get(key)
        .is_some_and(|slot| slot.left.is_empty() && slot.right.is_empty())
    {
        slots.remove(key);
    }
}

/// The incremental half of a join, and the state it keeps between cycles.
struct NaturalJoin {
    left: Arc<dyn Table>,
    right: Arc<dyn Table>,
    plan: Plan,
    slots: Mutex<Slots>,
    /// Left row key to the right row key of its match. A left row without a match has no
    /// entry.
    redirection: Arc<WritableRowRedirection>,
    /// Whether an exact join fails when a left row arrives or changes key and has no right
    /// row. Java does not check this when the right table is static, except in a join
    /// without match columns.
    ///
    /// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper.LeftTickingListener#onUpdate,
    /// which never calls `checkExactMatch`, against `copyRedirections` of
    /// `ChunkedMergedJoinListener`, which does, and `checkRightTableSizeZeroKeys`
    require_on_left_changes: bool,
    left_columns: ModifiedColumnTransformer,
    right_columns: ModifiedColumnTransformer,
    /// Every added column of the result.
    all_added: ModifiedColumnSet,
}

impl NaturalJoin {
    /// Reads both tables and returns the result's initial state with the operator.
    ///
    /// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper#naturalJoinInternal and
    /// #makeResult. Java fails only for a key that a left row has, and so does this: right
    /// rows are gathered first without judging them.
    fn start(
        plan: Plan,
        left: &Arc<dyn Table>,
        right: &Arc<dyn Table>,
    ) -> Result<(TableSpec, NaturalJoin), JoinError> {
        let mut slots = Slots::new();
        let right_rows = right.row_set();
        let right_keys = plan.right_keys.read(&**right, &right_rows, false)?;
        for (row, key) in right_rows.iter().zip(right_keys) {
            slots.entry(key).or_default().right.insert(row);
        }
        let redirection = Arc::new(WritableRowRedirection::new());
        let left_rows = left.row_set();
        let left_keys = plan.left_keys.read(&**left, &left_rows, false)?;
        for (row, key) in left_rows.iter().zip(left_keys) {
            let slot = slots.entry(key.clone()).or_default();
            slot.left.insert(row);
            if let Some(inner) = plan.matched(&key, &slot.right, true)? {
                redirection.put(row, inner);
            }
        }
        let graph = [left, right]
            .into_iter()
            .filter(|table| table.is_refreshing())
            .find_map(|table| table.update_graph());
        if let Some(graph) = graph {
            let clock: Arc<dyn Clock> = Arc::new(graph);
            redirection.start_tracking_prev(clock);
        }

        let left_count = left.definition().columns().len();
        let mut definition = left.definition().columns().to_vec();
        let mut columns: Vec<Arc<dyn ColumnSource>> =
            (0..left_count).map(|index| left.column(index)).collect();
        let shared: Arc<dyn RowRedirection> = redirection.clone();
        for (name, source) in &plan.additions {
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
        let spec = TableSpec::new(definition, (*left_rows).clone(), columns)
            .with_attributes(join_attributes(&**left, &**right, &plan))
            .with_input_table_of(&**left);

        let added = left_count..left_count + plan.additions.len();
        let right_columns = ModifiedColumnTransformer::new(
            plan.additions
                .iter()
                .zip(added.clone())
                .map(|((_, source), result)| (*source, [result])),
        );
        let join = NaturalJoin {
            left: left.clone(),
            right: right.clone(),
            require_on_left_changes: right.is_refreshing() || plan.zero_keys(),
            plan,
            slots: Mutex::new(slots),
            redirection,
            left_columns: ModifiedColumnTransformer::identity(left_count),
            right_columns,
            all_added: ModifiedColumnSet::of(added),
        };
        Ok((spec, join))
    }

    /// Applies the right table's change to the keys. Every key a change reaches is noted in
    /// `touched` before it is changed. `probe_modifies` says whether a modify that leaves
    /// the match columns alone changed an added column.
    ///
    /// Behavior: the right half of
    /// io.deephaven.engine.table.impl.NaturalJoinHelper.ChunkedMergedJoinListener#process
    /// and io.deephaven.engine.table.impl.NaturalJoinHelper.RightTickingListener#onUpdate:
    /// removes and the old keys of modified rows, then shifts, then the new keys of
    /// modified rows and adds.
    fn apply_right(
        &self,
        slots: &mut Slots,
        update: &TableUpdate,
        probe_modifies: bool,
        touched: &mut BTreeMap<Key, Touch>,
    ) -> Result<(), TableFailure> {
        let right = &*self.right;
        let keys = &self.plan.right_keys;
        let join_type = self.plan.join_type;
        let keys_modified =
            !update.modified.is_empty() && update.modified_columns.contains_any(&keys.columns);

        let mut leaving = update.removed.clone();
        if keys_modified {
            leaving.insert(&modified_pre_shift(update).map_err(failure)?);
        }
        for (row, key) in leaving.iter().zip(keys.read(right, &leaving, true)?) {
            let touch = touch(touched, slots, join_type, &key);
            touch.changed = true;
            if touch.now == Some(row) {
                touch.now = None;
            }
            if let Some(slot) = slots.get_mut(&key) {
                slot.right.remove(&row);
            }
        }

        if !update.shifted.is_empty() {
            let staying = right.prev_row_set().minus(&leaving);
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                for (row, key) in rows.iter().zip(keys.read(right, &rows, true)?) {
                    moves.push((key, row, row + range.delta));
                }
            }
            // Every row leaves its old key before any row takes a new one: a row may move to
            // where another row of the same key was.
            for (key, old, new) in &moves {
                let touch = touch(touched, slots, join_type, key);
                if !touch.moved && touch.now == Some(*old) {
                    touch.now = Some(*new);
                    touch.moved = true;
                }
                if let Some(slot) = slots.get_mut(key) {
                    slot.right.remove(old);
                }
            }
            for (key, _, new) in moves {
                slots.entry(key).or_default().right.insert(new);
            }
        }

        let mut arriving = update.added.clone();
        if keys_modified {
            arriving.insert(&update.modified);
        } else if probe_modifies {
            for key in keys.read(right, &update.modified, false)? {
                touch(touched, slots, join_type, &key).changed = true;
            }
        }
        for (row, key) in arriving.iter().zip(keys.read(right, &arriving, false)?) {
            touch(touched, slots, join_type, &key).changed = true;
            slots.entry(key).or_default().right.insert(row);
        }
        Ok(())
    }

    fn remove_left(&self, slots: &mut Slots, key: &Key, row: RowKey) {
        if let Some(slot) = slots.get_mut(key) {
            slot.left.remove(&row);
        }
        drop_if_empty(slots, key);
        self.redirection.remove(row);
    }

    /// Applies the left table's change to the keys and the redirection, and says whether a
    /// modified row changed its key.
    ///
    /// Behavior: the left half of
    /// io.deephaven.engine.table.impl.NaturalJoinHelper.ChunkedMergedJoinListener#process,
    /// and io.deephaven.engine.table.impl.NaturalJoinHelper.LeftTickingListener#onUpdate:
    /// removes, then the modified rows whose key really changed leave their old key, then
    /// shifts, then those rows and the added rows are matched.
    fn apply_left(&self, slots: &mut Slots, update: &TableUpdate) -> Result<bool, TableFailure> {
        let left = &*self.left;
        let keys = &self.plan.left_keys;
        let removed = keys.read(left, &update.removed, true)?;
        for (row, key) in update.removed.iter().zip(removed) {
            self.remove_left(slots, &key, row);
        }

        let mut rekeyed_before = Vec::new();
        let mut arriving: Vec<(RowKey, Key)> = Vec::new();
        if !update.modified.is_empty() && update.modified_columns.contains_any(&keys.columns) {
            // Shifts keep the order of rows, so the two sets pair up row by row.
            let before_rows = modified_pre_shift(update).map_err(failure)?;
            let before = keys.read(left, &before_rows, true)?;
            let after = keys.read(left, &update.modified, false)?;
            let rows = before_rows.iter().zip(update.modified.iter());
            for ((old_row, new_row), (old_key, new_key)) in rows.zip(before.into_iter().zip(after))
            {
                if old_key != new_key {
                    self.remove_left(slots, &old_key, old_row);
                    rekeyed_before.push(old_row);
                    arriving.push((new_row, new_key));
                }
            }
        }
        let rekeyed = !arriving.is_empty();

        if !update.shifted.is_empty() {
            let mut staying = left.prev_row_set().minus(&update.removed);
            staying.remove(&RowSet::from_keys(rekeyed_before));
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                for (row, key) in rows.iter().zip(keys.read(left, &rows, true)?) {
                    moves.push((key, row, row + range.delta));
                }
            }
            let mut matches = Vec::with_capacity(moves.len());
            for (key, old, _) in &moves {
                if let Some(slot) = slots.get_mut(key) {
                    slot.left.remove(old);
                }
                matches.push(self.redirection.remove(*old));
            }
            for ((key, _, new), inner) in moves.into_iter().zip(matches) {
                slots.entry(key).or_default().left.insert(new);
                if inner != NULL_ROW_KEY {
                    self.redirection.put(new, inner);
                }
            }
        }

        let added = keys.read(left, &update.added, false)?;
        arriving.extend(update.added.iter().zip(added));
        for (row, key) in arriving {
            let slot = slots.entry(key.clone()).or_default();
            slot.left.insert(row);
            let matched = self
                .plan
                .matched(&key, &slot.right, self.require_on_left_changes);
            if let Some(inner) = matched.map_err(failure)? {
                self.redirection.put(row, inner);
            }
        }
        Ok(rekeyed)
    }

    /// Brings the left rows of one key that the right table touched up to date. Adds the
    /// left rows whose added columns may have changed to `modified`, and returns whether
    /// they now read another right row.
    ///
    /// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper.ModifiedSlotUpdater#accept.
    /// Java compares the new match with the row key of the old one, and so misses a right
    /// row that was removed while another row of the same key was added at its row key; here
    /// the old match is followed through the cycle. Java also treats every change to a set
    /// of duplicates as a new match; here it is one only if the first or last row changed.
    fn settle(
        &self,
        slot: &Slot,
        key: &Key,
        touch: &Touch,
        right_columns_changed: bool,
        modified: &mut Vec<RowKey>,
    ) -> Result<bool, TableFailure> {
        let after = self
            .plan
            .matched(key, &slot.right, false)
            .map_err(failure)?;
        let same_row = match (touch.before, after) {
            (Match::None, None) => true,
            (Match::Row(_), Some(row)) => touch.now == Some(row),
            _ => false,
        };
        if same_row {
            if touch.changed && right_columns_changed {
                modified.extend(&slot.left);
            }
            if let (Match::Row(old), Some(new)) = (touch.before, after) {
                if old != new {
                    // The match was shifted. Its values are the same.
                    for &row in &slot.left {
                        self.redirection.put(row, new);
                    }
                }
            }
            return Ok(false);
        }
        modified.extend(&slot.left);
        match after {
            Some(inner) => {
                for &row in &slot.left {
                    self.redirection.put(row, inner);
                }
            }
            None => {
                // The key lost its right row: an exact join fails here.
                self.plan.matched(key, &slot.right, true).map_err(failure)?;
                for &row in &slot.left {
                    self.redirection.remove(row);
                }
            }
        }
        Ok(true)
    }
}

impl MergedOperator for NaturalJoin {
    /// `upstream[0]` is the left table's change and `upstream[1]` the right table's.
    ///
    /// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper.ChunkedMergedJoinListener#process
    fn on_update(
        &self,
        _result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
        let mut columns = ModifiedColumnSet::empty();
        let mut modified: Vec<RowKey> = Vec::new();
        let mut touched = BTreeMap::new();
        let mut right_columns_changed = false;
        if let Some(update) = &upstream[1] {
            if !update.modified.is_empty() {
                columns = self.right_columns.transform(&update.modified_columns);
            }
            right_columns_changed = !columns.is_empty();
            self.apply_right(&mut slots, update, right_columns_changed, &mut touched)?;
        }

        let mut downstream = TableUpdate::empty();
        if let Some(update) = &upstream[0] {
            if self.apply_left(&mut slots, update)? {
                // Java does not look at which rows got another right row.
                columns = columns.union(&self.all_added);
            }
            let left_columns = self.left_columns.transform(&update.modified_columns);
            columns = columns.union(&left_columns);
            modified.extend(update.modified.iter());
            downstream.added = update.added.clone();
            downstream.removed = update.removed.clone();
            downstream.shifted = update.shifted.clone();
        }

        let mut redirected = false;
        for (key, touch) in &touched {
            if let Some(slot) = slots.get(key).filter(|slot| !slot.left.is_empty()) {
                redirected |=
                    self.settle(slot, key, touch, right_columns_changed, &mut modified)?;
            }
            drop_if_empty(&mut slots, key);
        }
        if redirected {
            columns = columns.union(&self.all_added);
        }

        // A row the left table added in this cycle is added, whatever the right table did.
        let mut modified = RowSet::from_keys(modified);
        modified.remove(&downstream.added);
        downstream.modified = modified;
        downstream.modified_columns = columns;
        Ok(downstream)
    }
}

/// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper#naturalJoin
pub(super) fn join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    on: &[JoinMatch],
    add: &[JoinAddition],
    join_type: NaturalJoinType,
) -> Result<Arc<dyn Table>, JoinError> {
    let plan = Plan::new(&**left, &**right, on, add, join_type)?;
    if left.is_refreshing() && right.is_refreshing() {
        match (left.update_graph(), right.update_graph()) {
            (Some(a), Some(b)) if Arc::ptr_eq(&a, &b) => {}
            _ => return Err(JoinError::UpdateGraphConflict),
        }
    }

    let parents = [left.clone(), right.clone()];
    let mut refused = None;
    let result = QueryTable::derive_merged(&parents, Vec::new(), || {
        // Java's zeroKeyColumnsJoin gives a static right table a redirection that cannot be
        // written, and then writes the first or last right row to it when there are several.
        let picks = matches!(
            join_type,
            NaturalJoinType::FirstMatch | NaturalJoinType::LastMatch
        );
        let started =
            if plan.zero_keys() && picks && !right.is_refreshing() && right.row_set().size() > 1 {
                Err(JoinError::ZeroKeyStaticRight(
                    "SingleValueRowRedirection cannot be cast to WritableSingleValueRowRedirection"
                        .to_string(),
                ))
            } else {
                NaturalJoin::start(plan, left, right)
            };
        started.map_err(|error| {
            let failure = failure(&error);
            refused = Some(error);
            failure
        })
    });
    match result {
        Ok(table) => Ok(table),
        Err(failure) => Err(refused.unwrap_or(JoinError::Table(failure))),
    }
}
