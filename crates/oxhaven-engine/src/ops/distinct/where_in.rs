//! `where_in` and `where_not_in`: the rows of a table whose key is, or is not, among the
//! keys of a set table. Task 4.8.
//!
//! In Java the two are a `where` with one filter, `DynamicWhereFilter`, that holds the set
//! of keys of the set table. The result is therefore a filtered table like any other: the
//! left table's columns, a subset of its rows at their row keys, and the updates of a
//! `where` while only the left table changes. What is particular to it:
//!
//! - A refreshing set table is replaced by its `select_distinct` on the set columns, and
//!   the filter listens to that. So the set of keys changes only when a key appears in the
//!   set table for the first time or loses its last row there.
//! - When the set of keys changes, rows of the left table are filtered again, in the same
//!   cycle: the rows not in the result when keys were added, the rows in the result when
//!   keys were removed, all of them when both happened (for `where_not_in` the other way
//!   round). A change of the left table in that cycle is folded into the same update.
//! - Keys are compared after `Boolean` has been turned into `byte` and `Instant` into
//!   `long`, so a `Boolean` column can be matched with a `byte` column and an `Instant`
//!   column with a `long` column. Any other pair of different types is refused.

use super::{select_distinct, DistinctError};
use crate::ops::filter::{filtered_update, shift_failure, where_attributes};
use crate::ops::join::keys::{Cell, Key, KeyColumns};
use crate::ops::join::JoinMatch;
use crate::ops::key_table::{is_ported_key, same_object_vector_class};
use crate::table::{QueryTable, TableSpec};
use crate::update::modified_pre_shift;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableClass};
use oxhaven_contracts::shift::RowSetShiftData;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

/// Why a `where_in` or `where_not_in` was refused. Each variant names the Java exception
/// the reference throws; the recorded Java results have `INVALID_ARGUMENT` for the first
/// two (`fixtures/where_in_static.json`, `fixtures/where_in_ticking.json`).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum WhereInError {
    /// Java's `NoSuchColumnException`: match columns a table does not have. The set table's
    /// are looked up first, and if it lacks any, only those are named.
    NoSuchColumn(Vec<String>),
    /// Java's `IllegalArgumentException` "Reinterpreted key type mismatch": a pair of match
    /// columns of different types, other than `Boolean` with `byte` and `Instant` with
    /// `long`.
    MismatchedTypes {
        left: String,
        left_type: ColumnType,
        right: String,
        right_type: ColumnType,
    },
    /// Java's `UpdateGraphConflictException`: two refreshing tables of different update
    /// graphs.
    UpdateGraphConflict,
    /// A table has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for WhereInError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WhereInError::NoSuchColumn(columns) => {
                write!(f, "Unknown column names [{}]", columns.join(", "))
            }
            WhereInError::MismatchedTypes {
                left,
                left_type,
                right,
                right_type,
            } => write!(
                f,
                "Reinterpreted key type mismatch, {left}={right}: (set key type) \
                 {right_type:?} != {left_type:?} (source key type)"
            ),
            WhereInError::UpdateGraphConflict => {
                f.write_str("Multiple update graphs found in dependencies")
            }
            WhereInError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for WhereInError {}

impl From<TableFailure> for WhereInError {
    fn from(failure: TableFailure) -> Self {
        WhereInError::Table(failure)
    }
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The primitive type a key column is compared as.
#[derive(Clone, PartialEq, Eq, Debug)]
enum KeyClass {
    Byte,
    Char,
    Short,
    Int,
    Long,
    Float,
    Double,
    Text,
    /// A table, compared by identity (`Object#equals` of a table is that), so by its
    /// handle. The class the column's definition names must be the same on both sides.
    Table(TableClass),
    /// A `java.time` class, compared by `equals`: a date, a time, a date and time, a
    /// duration and a day by their numbers, a zone by its id (`Z` is not `UTC`), a zoned
    /// date and time by its instant and zone, a period by its three fields.
    LocalDate,
    LocalTime,
    LocalDateTime,
    Duration,
    DayOfWeek,
    ZoneId,
    ZonedDateTime,
    Period,
    /// A `BigDecimal`, a `BigInteger` or a vector, compared by `equals`: a `BigDecimal` by
    /// value and scale, a vector by its elements. The column type is the class.
    Object(ColumnType),
}

/// Behavior: io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive,
/// which gives a `Boolean` column as `byte` and an `Instant` column as `long`, and the
/// `getType` of the result, which
/// io.deephaven.engine.table.impl.select.DynamicWhereFilter#beginOperation compares
fn key_class(column_type: &ColumnType) -> KeyClass {
    match column_type {
        ColumnType::Boolean | ColumnType::Byte => KeyClass::Byte,
        ColumnType::Char => KeyClass::Char,
        ColumnType::Short => KeyClass::Short,
        ColumnType::Int => KeyClass::Int,
        ColumnType::Long | ColumnType::Instant => KeyClass::Long,
        ColumnType::Float => KeyClass::Float,
        ColumnType::Double => KeyClass::Double,
        ColumnType::String => KeyClass::Text,
        ColumnType::Table(class) => KeyClass::Table(*class),
        // seen on Deephaven 42.9: `where_in` and `where_not_in` match these by `equals`
        ColumnType::LocalDate => KeyClass::LocalDate,
        ColumnType::LocalTime => KeyClass::LocalTime,
        ColumnType::LocalDateTime => KeyClass::LocalDateTime,
        ColumnType::Duration => KeyClass::Duration,
        ColumnType::DayOfWeek => KeyClass::DayOfWeek,
        ColumnType::ZoneId => KeyClass::ZoneId,
        ColumnType::ZonedDateTime => KeyClass::ZonedDateTime,
        ColumnType::Period => KeyClass::Period,
        // seen on Deephaven 42.9: `BigDecimal#equals` tells `3` from `3.000`, a vector is
        // equal by its elements, a null vector to a null one, an empty one to an empty one
        ColumnType::BigDecimal | ColumnType::BigInteger => KeyClass::Object(column_type.clone()),
        vector if is_ported_key(vector) => KeyClass::Object(vector.clone()),
        // a vector of float or double (owner item 7.6) or of arrays, an array (7.7)
        other => unported!(
            "io.deephaven.engine.table.impl.select.DynamicWhereFilter#beginOperation",
            "a key column of type {other:?}"
        ),
    }
}

/// The keys of `rows` of `table`, in row order; with `use_prev`, as of the end of the
/// previous cycle for `rows` of the previous key space.
///
/// Two keys are equal when Java's set kernels and tuples call them equal: a null equals
/// only a null, every NaN equals every NaN, and `-0.0` equals `0.0`. A `Boolean` is its
/// byte: 1 for true, 0 for false, and null for null, which is the null of `byte` too.
///
/// Behavior: io.deephaven.engine.table.impl.select.setinclusion.DoubleSetInclusionKernel
/// and FloatSetInclusionKernel with
/// io.deephaven.engine.table.impl.chunkfilter.DoubleChunkMatchFilterFactory#getBits for one
/// match column, the `equals` of the tuples of io.deephaven.tuple.generated (which is the
/// `eq` of io.deephaven.util.compare.DoubleComparisons and the others) for several, and
/// io.deephaven.util.BooleanUtils#booleanAsByte for `Boolean`
fn read_keys(
    columns: &KeyColumns,
    table: &dyn Table,
    rows: &RowSet,
    use_prev: bool,
) -> Result<Vec<Key>, TableFailure> {
    let mut keys = columns.read(table, rows, use_prev)?;
    for key in &mut keys {
        for cell in key {
            if let Cell::Bool(value) = cell {
                *cell = Cell::Int(i64::from(*value));
            }
        }
    }
    Ok(keys)
}

/// What the result of a `where_in` keeps to follow its two parents.
struct WhereInOperator {
    left: Arc<dyn Table>,
    /// The table whose rows are the keys: the set table itself when it is static,
    /// otherwise what stands in for it (see [`where_in`]).
    set: Arc<dyn Table>,
    left_keys: KeyColumns,
    set_keys: KeyColumns,
    /// Whether the result has the rows whose key is in the set, or the others.
    inclusion: bool,
    /// The keys of the set table's rows.
    live: Mutex<BTreeSet<Key>>,
}

/// Which rows the result must filter again because the set of keys changed.
#[derive(Clone, Copy, Default)]
struct Refilter {
    /// The rows that are in the result.
    matched: bool,
    /// The rows of the left table that are not in the result.
    unmatched: bool,
}

impl WhereInOperator {
    /// The rows of `rows`, which are current rows of the left table, that belong in the
    /// result.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.DynamicWhereFilter#filterLinear
    fn accept(&self, live: &BTreeSet<Key>, rows: &RowSet) -> Result<RowSet, TableFailure> {
        let keys = read_keys(&self.left_keys, &*self.left, rows, false)?;
        let kept = rows
            .iter()
            .zip(&keys)
            .filter(|(_, key)| live.contains(*key) == self.inclusion)
            .map(|(row, _)| row);
        Ok(RowSet::from_keys(kept))
    }

    /// Brings the set of keys up to date with a change of the set table and says what has
    /// to be filtered again.
    ///
    /// Removed rows take their keys out first, then each modified row whose key changed
    /// replaces its old key by its new one, then added rows put their keys in. A key that
    /// is taken out without being there, or put in while there, fails the result: the set
    /// table is expected to have each key once.
    ///
    /// Added keys (and changed keys) make rows that were out eligible to come in, so for
    /// `where_in` the unmatched rows are filtered again; removed keys (and changed keys)
    /// can put rows out, so the matched rows are. For `where_not_in` it is the other way
    /// round.
    ///
    /// Behavior: the `onUpdate` of the listener that
    /// io.deephaven.engine.table.impl.select.DynamicWhereFilter#DynamicWhereFilter adds to
    /// the set table, and DynamicWhereFilter#addKey and #removeKey
    fn set_changed(
        &self,
        live: &mut BTreeSet<Key>,
        upstream: &TableUpdate,
    ) -> Result<Refilter, TableFailure> {
        let has_adds = !upstream.added.is_empty();
        let has_removes = !upstream.removed.is_empty();
        let has_modifies = !upstream.modified.is_empty()
            && upstream
                .modified_columns
                .contains_any(&self.set_keys.columns);
        let mut refilter = Refilter::default();
        if !has_adds && !has_removes && !has_modifies {
            return Ok(refilter);
        }

        let remove = |live: &mut BTreeSet<Key>, key: &Key| {
            if live.remove(key) {
                Ok(())
            } else {
                Err(failure(format!(
                    "Inconsistent state, key not found in set: {key:?}"
                )))
            }
        };
        let add = |live: &mut BTreeSet<Key>, key: Key| {
            if live.contains(&key) {
                return Err(failure(format!(
                    "Inconsistent state, key already in set: {key:?}"
                )));
            }
            live.insert(key);
            Ok(())
        };

        if has_removes {
            for key in read_keys(&self.set_keys, &*self.set, &upstream.removed, true)? {
                remove(live, &key)?;
            }
        }
        let mut true_modification = false;
        if has_modifies {
            let before = modified_pre_shift(upstream).map_err(shift_failure)?;
            let old = read_keys(&self.set_keys, &*self.set, &before, true)?;
            let new = read_keys(&self.set_keys, &*self.set, &upstream.modified, false)?;
            for (old, new) in old.into_iter().zip(new) {
                if old != new {
                    true_modification = true;
                    remove(live, &old)?;
                    add(live, new)?;
                }
            }
        }
        if has_adds {
            for key in read_keys(&self.set_keys, &*self.set, &upstream.added, false)? {
                add(live, key)?;
            }
        }

        let grew = has_adds || true_modification;
        let shrank = has_removes || true_modification;
        if self.inclusion {
            refilter.unmatched = grew;
            refilter.matched = shrank;
        } else {
            refilter.matched = grew;
            refilter.unmatched = shrank;
        }
        Ok(refilter)
    }

    /// The result's change when the set of keys changed, with the left table's change of
    /// the same cycle, if it has one.
    ///
    /// The result first gives up the rows the left table removed and follows its shifts.
    /// Then the rows named by `refilter` are filtered: every row of the left table when
    /// both kinds are asked for; the left table's rows that are not in the result, and
    /// its modified rows, when only the unmatched are, the rows of the result that the
    /// left table neither added nor modified staying; the result's rows and the left
    /// table's added and modified rows when only the matched are. What the new rows have
    /// and the old ones had not is added, the reverse is removed, by the keys from
    /// before the shifts; the left table's modified rows that stay are modified.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable.FilteredTable#doRefilter and
    /// FilteredTable#completeRefilterUpdate
    fn refilter(
        &self,
        live: &BTreeSet<Key>,
        result: &QueryTable,
        upstream: Option<&TableUpdate>,
        refilter: Refilter,
    ) -> Result<TableUpdate, TableFailure> {
        let before = result.row_set();
        let empty = RowSet::empty();
        let (upstream_added, upstream_modified) = match upstream {
            Some(upstream) => (&upstream.added, &upstream.modified),
            None => (&empty, &empty),
        };
        let shifted = upstream.map_or_else(RowSetShiftData::empty, |u| u.shifted.clone());
        let mut removed = match upstream {
            Some(upstream) => before.intersect(&upstream.removed),
            None => RowSet::empty(),
        };
        // The result's rows that the left table still has, in its new key space.
        let mapping = shifted
            .apply(&before.minus(&removed))
            .map_err(shift_failure)?;

        let new_mapping = if refilter.matched && refilter.unmatched {
            self.accept(live, &self.left.row_set())?
        } else if refilter.unmatched {
            let mut rows = self.left.row_set().minus(&mapping);
            // Modified rows are checked, not kept as they were.
            rows.insert(upstream_modified);
            let mut new_mapping = self.accept(live, &rows)?;
            let mut kept = mapping.clone();
            kept.remove(upstream_added);
            kept.remove(upstream_modified);
            new_mapping.insert(&kept);
            new_mapping
        } else {
            let mut rows = mapping.clone();
            rows.insert(upstream_added);
            rows.insert(upstream_modified);
            self.accept(live, &rows)?
        };

        let added = new_mapping.minus(&mapping);
        let dropped = mapping.minus(&new_mapping);
        removed.insert(&shifted.inverse().apply(&dropped).map_err(shift_failure)?);
        let mut modified = upstream_modified.intersect(&new_mapping);
        modified.remove(&added);
        Ok(TableUpdate {
            added,
            removed,
            modified,
            shifted,
            modified_columns: upstream
                .map_or_else(ModifiedColumnSet::empty, |u| u.modified_columns.clone()),
        })
    }

    /// Behavior: io.deephaven.engine.table.impl.WhereListener#process, after the set
    /// table's listener has run (the filter is a dependency of the `where` listener, see
    /// io.deephaven.engine.table.impl.select.DynamicWhereFilter#satisfied)
    fn on_update(
        &self,
        result: &QueryTable,
        left: Option<&TableUpdate>,
        set: Option<&TableUpdate>,
    ) -> Result<TableUpdate, TableFailure> {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        let refilter = match set {
            Some(set) => self.set_changed(&mut live, set)?,
            None => Refilter::default(),
        };
        if refilter.matched || refilter.unmatched {
            return self.refilter(&live, result, left, refilter);
        }
        let Some(left) = left else {
            // The set table changed, the set of keys did not.
            return Ok(TableUpdate::empty());
        };
        // Modified rows are filtered again only if a match column was modified.
        let filter_modified = left.modified_columns.contains_any(&self.left_keys.columns);
        filtered_update(result, left, filter_modified, |rows| {
            self.accept(&live, rows)
        })
    }
}

/// The indices of `names` in `table`, or the names it does not have.
fn indices<'a>(
    table: &dyn Table,
    names: impl Iterator<Item = &'a String>,
) -> Result<Vec<usize>, WhereInError> {
    let declared = table.definition();
    let mut found = Vec::new();
    let mut missing = Vec::new();
    for name in names {
        match declared.index_of(name) {
            Some(index) => found.push(index),
            None => missing.push(name.clone()),
        }
    }
    if missing.is_empty() {
        Ok(found)
    } else {
        Err(WhereInError::NoSuchColumn(missing))
    }
}

/// A `where` of `left` with the filter that holds the keys of `set`'s rows: the rows whose
/// `on` columns are (`inclusion`) or are not the key of a row of `set`. `set` must have
/// each key in one row at most once it changes; a second row with a key fails the result.
///
/// The result has the left table's columns and input table and its blink attribute. It is
/// add-only and append-only when the left table is, static tables included, but only
/// while `set` is static: keys that come and go move rows in and out of the result. It is
/// refreshing when either table is, and never flat.
///
/// Behavior: io.deephaven.engine.table.impl.select.DynamicWhereFilter#DynamicWhereFilter
/// and #beginOperation, and io.deephaven.engine.table.impl.QueryTable#whereInternal with
/// that filter. Not ported: data indexes on the left table (`#filterFullIndex`,
/// `#filterPartialIndex`); no table here has one, and they select the same rows.
pub(super) fn dynamic_where(
    left: &Arc<dyn Table>,
    set: &Arc<dyn Table>,
    on: &[JoinMatch],
    inclusion: bool,
) -> Result<Arc<dyn Table>, WhereInError> {
    let set_keys = indices(&**set, on.iter().map(|pair| &pair.right))?;
    let left_keys = indices(&**left, on.iter().map(|pair| &pair.left))?;
    for (pair, (&left_index, &set_index)) in on.iter().zip(left_keys.iter().zip(&set_keys)) {
        let left_type = &left.definition().columns()[left_index].column_type;
        let right_type = &set.definition().columns()[set_index].column_type;
        if left_type != right_type && same_object_vector_class(left_type, right_type) {
            unported!(
                "io.deephaven.engine.table.impl.select.DynamicWhereFilter#beginOperation",
                "key columns {} and {} of the vector types {left_type:?} and {right_type:?}: \
                 Java takes the pair (one class, ObjectVector)",
                pair.left,
                pair.right
            );
        }
        if key_class(left_type) != key_class(right_type) {
            return Err(WhereInError::MismatchedTypes {
                left: pair.left.clone(),
                left_type: left_type.clone(),
                right: pair.right.clone(),
                right_type: right_type.clone(),
            });
        }
    }
    if left.is_refreshing() && set.is_refreshing() {
        match (left.update_graph(), set.update_graph()) {
            (Some(a), Some(b)) if Arc::ptr_eq(&a, &b) => {}
            _ => return Err(WhereInError::UpdateGraphConflict),
        }
    }

    let parents = [left.clone(), set.clone()];
    let result = QueryTable::derive_merged(&parents, Vec::new(), || {
        let set_keys = KeyColumns { columns: set_keys };
        // Rows of a static set table may share keys; each key counts once.
        let live: BTreeSet<Key> = read_keys(&set_keys, &**set, &set.row_set(), false)?
            .into_iter()
            .collect();
        let operator = WhereInOperator {
            left: left.clone(),
            set: set.clone(),
            left_keys: KeyColumns { columns: left_keys },
            set_keys,
            inclusion,
            live: Mutex::new(BTreeSet::new()),
        };
        let rows = operator.accept(&live, &left.row_set())?;
        *operator.live.lock().unwrap_or_else(PoisonError::into_inner) = live;

        let definition = left.definition().clone();
        let columns = (0..definition.columns().len())
            .map(|index| left.column(index))
            .collect();
        let mut attributes = where_attributes(&**left);
        if set.is_refreshing() {
            attributes.add_only = false;
            attributes.append_only = false;
        }
        let spec = TableSpec::new(definition, rows, columns)
            .keeping_partitioning()
            .with_attributes(attributes)
            .with_input_table_of(&**left);
        Ok((
            spec,
            move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
                operator.on_update(result, upstream[0].as_deref(), upstream[1].as_deref())
            },
        ))
    })?;
    Ok(result)
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#whereInInternal. A refreshing set
/// table is replaced by its `select_distinct` on the set columns; a static one is used as
/// it is. Not ported: a data index of the set table on those columns, and partitioning
/// columns; no table here has either.
fn where_in_internal(
    left: &Arc<dyn Table>,
    set: &Arc<dyn Table>,
    on: &[JoinMatch],
    inclusion: bool,
) -> Result<Arc<dyn Table>, WhereInError> {
    if !set.is_refreshing() {
        return dynamic_where(left, set, on, inclusion);
    }
    // Refused here as the set table's missing columns are refused in `dynamic_where`,
    // before anything is said about the left table.
    indices(&**set, on.iter().map(|pair| &pair.right))?;
    let names: Vec<String> = on.iter().map(|pair| pair.right.clone()).collect();
    let distinct = match select_distinct(set, &names) {
        Ok(distinct) => distinct,
        Err(DistinctError::NoSuchColumn(columns)) => {
            return Err(WhereInError::NoSuchColumn(columns))
        }
        Err(DistinctError::Table(failure)) => return Err(WhereInError::Table(failure)),
    };
    dynamic_where(left, &distinct, on, inclusion)
}

/// The rows of `left` whose values of the `on` columns are those of a row of `set`, in
/// the left table's order and at its row keys, with all of its columns.
///
/// Values are equal as keys are everywhere in the engine: null equals null, every NaN
/// equals every NaN, and `-0.0` equals `0.0`. A pair of match columns must have one type,
/// except that a `Boolean` column matches a `byte` column (true is 1, false is 0, null is
/// null) and an `Instant` column a `long` column (its nanoseconds). With no `on`, every
/// row of `left` is in the result while `set` has a row, and none while it has none.
///
/// The result follows both tables. It is refreshing when either is; of two static tables
/// it is static. It is never flat. It has the left table's blink attribute and input
/// table. It is add-only and append-only when the left table is (every static table is)
/// and `set` is static.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#whereIn
pub fn where_in(
    left: &Arc<dyn Table>,
    set: &Arc<dyn Table>,
    on: &[JoinMatch],
) -> Result<Arc<dyn Table>, WhereInError> {
    where_in_internal(left, set, on, true)
}

/// The rows of `left` whose values of the `on` columns are those of no row of `set`.
/// Otherwise as [`where_in`]; with no `on`, the result has every row of `left` while
/// `set` is empty, and none while it has a row.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#whereNotIn
pub fn where_not_in(
    left: &Arc<dyn Table>,
    set: &Arc<dyn Table>,
    on: &[JoinMatch],
) -> Result<Arc<dyn Table>, WhereInError> {
    where_in_internal(left, set, on, false)
}
