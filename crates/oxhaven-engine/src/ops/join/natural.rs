//! The natural join: its checks, its initial result and its incremental half.
//!
//! The result has the left table's rows at the left table's row keys. Its added columns
//! read the right table through a row redirection from left row key to right row key.
//!
//! The keys of both tables are numbered by a [`KeyTable`], a chunk of key columns at a
//! time, and what the join knows about a key is kept by that number, its slot. How much it
//! keeps depends on which table can change:
//!
//! | Left | Right | Kept per key | Kept per left row |
//! | --- | --- | --- | --- |
//! | static | static | nothing: one pass over each table | the redirection |
//! | refreshing | static | the first and last right row | the redirection |
//! | either | refreshing | the right rows and the left rows | the redirection |
//!
//! An update changes those sets and the redirection, and reports the left rows whose right
//! row changed as modified.
//!
//! Java has four implementations of this, chosen by which side is refreshing, each with a
//! hash table generated per key type. This is one implementation. It keeps Java's order of
//! work within a cycle (right table first: removes, shifts, modifies, adds; then the left
//! table; then the keys the right table touched), because that order decides which
//! combinations of changes fail.

use super::key_table::{KeyTable, NO_SLOT};
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

/// The rows of one table that have one key. Most keys of a right table have one row, and
/// that one is kept without a set.
#[derive(Default, Debug)]
enum Rows {
    #[default]
    Empty,
    One(RowKey),
    /// Two rows or more.
    Many(BTreeSet<RowKey>),
}

impl Rows {
    /// `rows` in increasing order.
    fn from_sorted(rows: Vec<RowKey>) -> Self {
        match rows.as_slice() {
            [] => Rows::Empty,
            [only] => Rows::One(*only),
            _ => Rows::Many(rows.into_iter().collect()),
        }
    }

    fn insert(&mut self, row: RowKey) {
        match self {
            Rows::Empty => *self = Rows::One(row),
            Rows::One(only) if *only == row => {}
            Rows::One(only) => *self = Rows::Many(BTreeSet::from([*only, row])),
            Rows::Many(rows) => {
                rows.insert(row);
            }
        }
    }

    fn remove(&mut self, row: RowKey) {
        match self {
            Rows::One(only) if *only == row => *self = Rows::Empty,
            Rows::Many(rows) => {
                rows.remove(&row);
                if let (1, Some(only)) = (rows.len(), rows.first()) {
                    *self = Rows::One(*only);
                }
            }
            Rows::Empty | Rows::One(_) => {}
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self, Rows::Empty)
    }

    /// The first and the last row.
    fn ends(&self) -> Option<(RowKey, RowKey)> {
        match self {
            Rows::Empty => None,
            Rows::One(only) => Some((*only, *only)),
            Rows::Many(rows) => rows.first().copied().zip(rows.last().copied()),
        }
    }

    fn iter(&self) -> impl Iterator<Item = RowKey> + '_ {
        let (one, many) = match self {
            Rows::Empty => (None, None),
            Rows::One(only) => (Some(*only), None),
            Rows::Many(rows) => (None, Some(rows.iter().copied())),
        };
        one.into_iter().chain(many.into_iter().flatten())
    }
}

/// The rows of each slot below `slots`, for `rows` whose slots are `row_slots` in order.
fn grouped(rows: &RowSet, row_slots: &[u32], slots: usize) -> Vec<Rows> {
    let mut groups = vec![Vec::new(); slots];
    for (row, &slot) in rows.iter().zip(row_slots) {
        groups[slot as usize].push(row);
    }
    groups.into_iter().map(Rows::from_sorted).collect()
}

/// The rows of `slot`, which may be newer than `rows` is long.
fn rows_of(rows: &mut Vec<Rows>, slot: u32) -> &mut Rows {
    let slot = slot as usize;
    if rows.len() <= slot {
        rows.resize_with(slot + 1, Rows::default);
    }
    &mut rows[slot]
}

/// The right rows of every key.
enum RightRows {
    /// Of a static right table: the first and the last row of each slot, which is all a
    /// join ever asks of a key that cannot change.
    Fixed(Vec<Option<(RowKey, RowKey)>>),
    /// Of a refreshing right table.
    Changing(Vec<Rows>),
}

impl RightRows {
    /// The first and last right row of `slot`, which may be `NO_SLOT`.
    fn ends(&self, slot: u32) -> Option<(RowKey, RowKey)> {
        let slot = slot as usize;
        match self {
            RightRows::Fixed(ends) => *ends.get(slot)?,
            RightRows::Changing(rows) => rows.get(slot)?.ends(),
        }
    }
}

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

/// The match of a key whose first and last right rows are `ends`.
///
/// Behavior: the `joinType` branches of `buildFromRightSide` in the hashers under
/// io.deephaven.engine.table.impl.naturaljoin.typed.staticopen, and
/// io.deephaven.engine.table.impl.NaturalJoinHelper.ModifiedSlotUpdater#accept for the
/// first and last row of a set of duplicates
fn pick(join_type: NaturalJoinType, ends: Option<(RowKey, RowKey)>) -> Match {
    match ends {
        Some((first, last)) if first == last => Match::Row(first),
        Some((first, last)) => match join_type {
            NaturalJoinType::FirstMatch => Match::Row(first),
            NaturalJoinType::LastMatch => Match::Row(last),
            NaturalJoinType::ErrorOnDuplicate | NaturalJoinType::ExactlyOneMatch => {
                Match::Duplicate
            }
        },
        None => Match::None,
    }
}

/// Why a join refuses a key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Refusal {
    /// Several right rows have it.
    Duplicate,
    /// No right row has it, in an exact join.
    NoRightRow,
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

    /// The right row for a left row whose key has the first and last right rows `ends`.
    /// `require` says whether an exact join insists on a right row here.
    ///
    /// Behavior: the duplicate checks of `decorateLeftSide` and `addLeftSide` in the hashers
    /// under io.deephaven.engine.table.impl.naturaljoin.typed,
    /// io.deephaven.engine.table.impl.StaticNaturalJoinStateManager#checkExactMatch, and for
    /// a join without match columns
    /// io.deephaven.engine.table.impl.NaturalJoinHelper#checkRightTableSizeZeroKeys
    fn matched(
        &self,
        ends: Option<(RowKey, RowKey)>,
        require: bool,
    ) -> Result<Option<RowKey>, Refusal> {
        match pick(self.join_type, ends) {
            Match::Row(row) => Ok(Some(row)),
            Match::Duplicate => Err(Refusal::Duplicate),
            Match::None if require && self.join_type == NaturalJoinType::ExactlyOneMatch => {
                Err(Refusal::NoRightRow)
            }
            Match::None => Ok(None),
        }
    }

    /// The error of a join that refuses `key`.
    ///
    /// Behavior: the messages of the methods [`Plan::matched`] names
    fn refused(&self, refusal: Refusal, key: &Key) -> JoinError {
        match refusal {
            Refusal::Duplicate => JoinError::DuplicateRightKey(if self.zero_keys() {
                "naturalJoin with zero key columns may not have more than one row in the right \
                 hand side table!"
                    .to_string()
            } else {
                format!(
                    "Natural Join found duplicate right key for {}",
                    key_text(key, &self.key_types)
                )
            }),
            Refusal::NoRightRow => JoinError::NoRightRow(if self.zero_keys() {
                "exactJoin with zero key columns must have exactly one row in the right hand \
                 side table!"
                    .to_string()
            } else {
                format!(
                    "Tables don't have one-to-one mapping - no mappings for key {}.",
                    key_text(key, &self.key_types)
                )
            }),
        }
    }

    /// The error of a join that refuses the key of one of the `rows` of the left table,
    /// each with its reason. When it refuses several keys in one cycle, the error names the
    /// least of them, so that it is the same on every run.
    fn refused_rows(&self, left: &dyn Table, mut rows: Vec<(RowKey, Refusal)>) -> JoinError {
        rows.sort_unstable_by_key(|(row, _)| *row);
        let set = RowSet::from_keys(rows.iter().map(|(row, _)| *row));
        let keys = match self.left_keys.read(left, &set, false) {
            Ok(keys) => keys,
            Err(failure) => return JoinError::Table(failure),
        };
        let least = keys.iter().zip(&rows).min_by(|a, b| a.0.cmp(b.0));
        match least {
            Some((key, (_, refusal))) => self.refused(*refusal, key),
            None => panic!("a refusal names a row"),
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

/// The keys the right table touched in a cycle, by slot.
type Touched = BTreeMap<u32, Touch>;

fn touch<'a>(
    touched: &'a mut Touched,
    right: &[Rows],
    join_type: NaturalJoinType,
    slot: u32,
) -> &'a mut Touch {
    touched.entry(slot).or_insert_with(|| {
        let ends = right.get(slot as usize).and_then(Rows::ends);
        let before = pick(join_type, ends);
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

/// What a join keeps between cycles, besides the redirection.
///
/// A key keeps its slot once it has one, also when no row has the key any more, as a key
/// keeps its place in the hash tables of
/// io.deephaven.engine.table.impl.naturaljoin.IncrementalNaturalJoinStateManagerTypedBase.
struct State {
    /// The slot of every key of the right table, and with a refreshing right table also of
    /// every key of the left table.
    keys: KeyTable,
    right: RightRows,
    /// The left rows of each slot, when the right table is refreshing: a change of a right
    /// row changes the result at the left rows of its key. A static right table never asks
    /// which left rows have a key.
    left: Option<Vec<Rows>>,
}

/// The incremental half of a join, and the state it keeps between cycles.
struct NaturalJoin {
    left: Arc<dyn Table>,
    right: Arc<dyn Table>,
    plan: Plan,
    state: Mutex<State>,
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
        let mut keys = KeyTable::new();
        let right_rows = right.row_set();
        let right_slots = keys.insert(
            &plan.right_keys.chunks(&**right, &right_rows, false)?,
            right_rows.size() as usize,
        );
        let right_state = if right.is_refreshing() {
            RightRows::Changing(grouped(&right_rows, &right_slots, keys.len()))
        } else {
            let mut ends: Vec<Option<(RowKey, RowKey)>> = vec![None; keys.len()];
            for (row, &slot) in right_rows.iter().zip(&right_slots) {
                // Rows come in increasing order.
                match &mut ends[slot as usize] {
                    Some((_, last)) => *last = row,
                    none => *none = Some((row, row)),
                }
            }
            RightRows::Fixed(ends)
        };
        drop(right_slots);

        let left_rows = left.row_set();
        let left_size = left_rows.size() as usize;
        let left_chunks = plan.left_keys.chunks(&**left, &left_rows, false)?;
        // Only a refreshing right table can give a row to a key that has none now.
        let left_slots = if right.is_refreshing() {
            keys.insert(&left_chunks, left_size)
        } else {
            keys.find(&left_chunks, left_size)
        };
        drop(left_chunks);
        let mut inner = Vec::with_capacity(left_size);
        for (position, &slot) in left_slots.iter().enumerate() {
            match plan.matched(right_state.ends(slot), true) {
                Ok(row) => inner.push(row.unwrap_or(NULL_ROW_KEY)),
                Err(refusal) => {
                    let row = left_rows.get(position as u64).unwrap_or(NULL_ROW_KEY);
                    return Err(plan.refused_rows(&**left, vec![(row, refusal)]));
                }
            }
        }
        let redirection = Arc::new(WritableRowRedirection::from_rows(&left_rows, inner));
        let state = State {
            left: right
                .is_refreshing()
                .then(|| grouped(&left_rows, &left_slots, keys.len())),
            keys,
            right: right_state,
        };
        drop(left_slots);
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
            state: Mutex::new(state),
            redirection,
            left_columns: ModifiedColumnTransformer::identity(left_count),
            right_columns,
            all_added: ModifiedColumnSet::of(added),
        };
        Ok((spec, join))
    }

    /// The slot of each of `rows` of the right table, by its key now or, with `use_prev`,
    /// before this cycle.
    fn right_slots(
        &self,
        keys: &mut KeyTable,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<Vec<u32>, TableFailure> {
        let chunks = self.plan.right_keys.chunks(&*self.right, rows, use_prev)?;
        Ok(keys.insert(&chunks, rows.size() as usize))
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
        state: &mut State,
        update: &TableUpdate,
        probe_modifies: bool,
        touched: &mut Touched,
    ) -> Result<(), TableFailure> {
        let State { keys, right, .. } = state;
        let RightRows::Changing(right) = right else {
            panic!("a static right table does not change");
        };
        let join_type = self.plan.join_type;
        let keys_modified = !update.modified.is_empty()
            && update
                .modified_columns
                .contains_any(&self.plan.right_keys.columns);

        let mut leaving = update.removed.clone();
        if keys_modified {
            leaving.insert(&modified_pre_shift(update).map_err(failure)?);
        }
        let slots = self.right_slots(keys, &leaving, true)?;
        for (row, slot) in leaving.iter().zip(slots) {
            let touch = touch(touched, right, join_type, slot);
            touch.changed = true;
            if touch.now == Some(row) {
                touch.now = None;
            }
            rows_of(right, slot).remove(row);
        }

        if !update.shifted.is_empty() {
            let staying = self.right.prev_row_set().minus(&leaving);
            let mut moves = Vec::new();
            for range in update.shifted.ranges() {
                let rows = staying.sub_set_by_key_range(range.first, range.last);
                let slots = self.right_slots(keys, &rows, true)?;
                for (row, slot) in rows.iter().zip(slots) {
                    moves.push((slot, row, row + range.delta));
                }
            }
            // Every row leaves its old key before any row takes a new one: a row may move to
            // where another row of the same key was.
            for &(slot, old, new) in &moves {
                let touch = touch(touched, right, join_type, slot);
                if !touch.moved && touch.now == Some(old) {
                    touch.now = Some(new);
                    touch.moved = true;
                }
                rows_of(right, slot).remove(old);
            }
            for (slot, _, new) in moves {
                rows_of(right, slot).insert(new);
            }
        }

        let mut arriving = update.added.clone();
        if keys_modified {
            arriving.insert(&update.modified);
        } else if probe_modifies {
            for slot in self.right_slots(keys, &update.modified, false)? {
                touch(touched, right, join_type, slot).changed = true;
            }
        }
        let slots = self.right_slots(keys, &arriving, false)?;
        for (row, slot) in arriving.iter().zip(slots) {
            touch(touched, right, join_type, slot).changed = true;
            rows_of(right, slot).insert(row);
        }
        Ok(())
    }

    /// Applies the left table's change to the keys and the redirection, and says whether a
    /// modified row changed its key.
    ///
    /// Behavior: the left half of
    /// io.deephaven.engine.table.impl.NaturalJoinHelper.ChunkedMergedJoinListener#process,
    /// and io.deephaven.engine.table.impl.NaturalJoinHelper.LeftTickingListener#onUpdate:
    /// removes, then the modified rows whose key really changed leave their old key, then
    /// shifts, then those rows and the added rows are matched.
    fn apply_left(&self, state: &mut State, update: &TableUpdate) -> Result<bool, TableFailure> {
        let left = &*self.left;
        let columns = &self.plan.left_keys;
        let State {
            keys,
            right,
            left: left_rows,
        } = state;
        let tracked = left_rows.is_some();
        // The slot of each of `rows` in the join's table. Without a set of left rows per
        // key there is nothing to keep for a key the right table does not have.
        let slots_of = |keys: &mut KeyTable, rows: &RowSet, use_prev: bool| {
            let chunks = columns.chunks(left, rows, use_prev)?;
            let size = rows.size() as usize;
            Ok::<_, TableFailure>(if tracked {
                keys.insert(&chunks, size)
            } else {
                keys.find(&chunks, size)
            })
        };

        if let Some(left_rows) = left_rows.as_mut() {
            let slots = slots_of(keys, &update.removed, true)?;
            for (row, slot) in update.removed.iter().zip(slots) {
                rows_of(left_rows, slot).remove(row);
            }
        }
        for row in update.removed.iter() {
            self.redirection.remove(row);
        }

        let mut rekeyed_before = Vec::new();
        let mut arriving: Vec<(RowKey, u32)> = Vec::new();
        if !update.modified.is_empty() && update.modified_columns.contains_any(&columns.columns) {
            // Shifts keep the order of rows, so the two sets pair up row by row.
            let before_rows = modified_pre_shift(update).map_err(failure)?;
            let size = before_rows.size() as usize;
            let before = columns.chunks(left, &before_rows, true)?;
            let after = columns.chunks(left, &update.modified, false)?;
            // Whether a row changed its key is asked of the two keys. The join's table
            // answers it only when it has a slot for every key of the left table; a table
            // of these rows alone answers it otherwise.
            let (old, new, joined) = if tracked {
                let old = keys.insert(&before, size);
                let new = keys.insert(&after, size);
                (old, new.clone(), new)
            } else {
                let mut these = KeyTable::new();
                let old = these.insert(&before, size);
                (old, these.insert(&after, size), keys.find(&after, size))
            };
            let rows = before_rows.iter().zip(update.modified.iter());
            for (index, (old_row, new_row)) in rows.enumerate() {
                if old[index] != new[index] {
                    if let Some(left_rows) = left_rows.as_mut() {
                        rows_of(left_rows, old[index]).remove(old_row);
                    }
                    self.redirection.remove(old_row);
                    rekeyed_before.push(old_row);
                    arriving.push((new_row, joined[index]));
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
                let slots = if tracked {
                    slots_of(keys, &rows, true)?
                } else {
                    vec![NO_SLOT; rows.size() as usize]
                };
                for (row, slot) in rows.iter().zip(slots) {
                    moves.push((slot, row, row + range.delta));
                }
            }
            let mut matches = Vec::with_capacity(moves.len());
            for &(slot, old, _) in &moves {
                if let Some(left_rows) = left_rows.as_mut() {
                    rows_of(left_rows, slot).remove(old);
                }
                matches.push(self.redirection.remove(old));
            }
            for ((slot, _, new), inner) in moves.into_iter().zip(matches) {
                if let Some(left_rows) = left_rows.as_mut() {
                    rows_of(left_rows, slot).insert(new);
                }
                if inner != NULL_ROW_KEY {
                    self.redirection.put(new, inner);
                }
            }
        }

        let added = slots_of(keys, &update.added, false)?;
        arriving.extend(update.added.iter().zip(added));
        for (row, slot) in arriving {
            if let Some(left_rows) = left_rows.as_mut() {
                rows_of(left_rows, slot).insert(row);
            }
            match self
                .plan
                .matched(right.ends(slot), self.require_on_left_changes)
            {
                Ok(Some(inner)) => {
                    self.redirection.put(row, inner);
                }
                Ok(None) => {}
                Err(refusal) => {
                    return Err(failure(self.plan.refused_rows(left, vec![(row, refusal)])))
                }
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
        left: &Rows,
        right: Option<(RowKey, RowKey)>,
        touch: &Touch,
        right_columns_changed: bool,
        modified: &mut Vec<RowKey>,
    ) -> Result<bool, Refusal> {
        let after = self.plan.matched(right, false)?;
        let same_row = match (touch.before, after) {
            (Match::None, None) => true,
            (Match::Row(_), Some(row)) => touch.now == Some(row),
            _ => false,
        };
        if same_row {
            if touch.changed && right_columns_changed {
                modified.extend(left.iter());
            }
            if let (Match::Row(old), Some(new)) = (touch.before, after) {
                if old != new {
                    // The match was shifted. Its values are the same.
                    for row in left.iter() {
                        self.redirection.put(row, new);
                    }
                }
            }
            return Ok(false);
        }
        modified.extend(left.iter());
        match after {
            Some(inner) => {
                for row in left.iter() {
                    self.redirection.put(row, inner);
                }
            }
            None => {
                // The key lost its right row: an exact join fails here.
                self.plan.matched(right, true)?;
                for row in left.iter() {
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
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut columns = ModifiedColumnSet::empty();
        let mut modified: Vec<RowKey> = Vec::new();
        let mut touched = Touched::new();
        let mut right_columns_changed = false;
        if let Some(update) = &upstream[1] {
            if !update.modified.is_empty() {
                columns = self.right_columns.transform(&update.modified_columns);
            }
            right_columns_changed = !columns.is_empty();
            self.apply_right(&mut state, update, right_columns_changed, &mut touched)?;
        }

        let mut downstream = TableUpdate::empty();
        if let Some(update) = &upstream[0] {
            if self.apply_left(&mut state, update)? {
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
        let mut refused = Vec::new();
        let left_rows = state.left.as_deref().unwrap_or_default();
        for (&slot, touch) in &touched {
            let Some(left) = left_rows.get(slot as usize).filter(|left| !left.is_empty()) else {
                continue;
            };
            let right = state.right.ends(slot);
            match self.settle(left, right, touch, right_columns_changed, &mut modified) {
                Ok(changed) => redirected |= changed,
                Err(refusal) => refused.extend(left.iter().next().map(|row| (row, refusal))),
            }
        }
        if !refused.is_empty() {
            return Err(failure(self.plan.refused_rows(&*self.left, refused)));
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
