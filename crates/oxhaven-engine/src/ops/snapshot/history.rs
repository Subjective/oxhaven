//! `snapshot_when` with `history`: every row the trigger gets appends a copy of the base.

use super::{definition, failure, readable, reading, sources, Parts, SnapshotError};
use crate::table::{QueryTable, TableSpec};
use arrow::array::{Array, ArrayRef, UInt64Array};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::Dependency;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

/// Where a column of the result comes from.
#[derive(Clone, Copy)]
enum Origin {
    /// A stamp column, by its index among the stamp columns.
    Stamp(usize),
    /// A column of the base.
    Base(usize),
}

/// A stamp column that has the name of a base column. The base column takes its place in
/// the result, and its values.
struct Displaced {
    stamp: usize,
    /// The base column's type.
    by: ColumnType,
}

/// The copies appended so far, at the row keys `0..size`.
struct History {
    parts: Parts,
    origins: Vec<Origin>,
    displaced: Vec<Displaced>,
    columns: Vec<Arc<WritableSource>>,
}

/// Whether Java can write a stamp of type `stamp` into the storage of a column of another
/// type `column`, which it does before the base's value overwrites it.
///
/// The writer is chosen by the stamp's type and calls the `set` overload for it. Boolean,
/// String and Instant stamps are written as objects: null fits every column, any other
/// value only a column stored as objects, which of the column types here is String. A
/// `byte` fits the byte storage of a Boolean column and a `long` the nanosecond storage of
/// an Instant column; every other pair of types has no overload and throws.
///
/// Behavior: io.deephaven.engine.table.impl.chunkfillers.ChunkFiller#fillFromSingleValue
/// against the `set` overloads of io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource
/// and its subclasses, as recorded in `fixtures/snapshot_static.json` (`conflict_history_*`)
fn fits(stamp: &ColumnType, column: &ColumnType, null: bool) -> bool {
    // A vector or an array is stored as an object, like a String, but which `set`
    // overloads take one was not recorded.
    if stamp.list_element().is_some() || column.list_element().is_some() {
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.chunkfillers.ChunkFiller#fillFromSingleValue",
            "a stamp column of type {stamp:?} with the name of a base column of type {column:?}"
        );
    }
    match stamp {
        ColumnType::Boolean | ColumnType::String | ColumnType::Instant => {
            null || *column == ColumnType::String
        }
        ColumnType::Byte => *column == ColumnType::Boolean,
        ColumnType::Long => *column == ColumnType::Instant,
        _ => false,
    }
}

fn take(values: &ArrayRef, positions: impl Iterator<Item = u64>) -> Result<ArrayRef, TableFailure> {
    let positions: UInt64Array = positions.map(Some).collect();
    arrow::compute::take(values.as_ref(), &positions, None).map_err(|e| failure(e.to_string()))
}

impl History {
    /// The result's columns: the stamp columns, then the base's. A base column that has
    /// the name of a stamp column stands where the stamp column would.
    ///
    /// Behavior: the `putAll` of the base's columns into the map of the trigger's in
    /// io.deephaven.engine.table.impl.QueryTable#snapshotHistoryInternal
    fn layout(parts: &Parts) -> (Vec<ColumnDefinition>, Vec<Origin>, Vec<Displaced>) {
        let base = parts.base.definition();
        let mut columns = Vec::new();
        let mut origins = Vec::new();
        let mut displaced = Vec::new();
        for (index, stamp) in parts.stamps.columns.iter().enumerate() {
            match base.index_of(&stamp.name) {
                Some(by) => {
                    let column = &base.columns()[by];
                    if column.column_type != stamp.column_type {
                        displaced.push(Displaced {
                            stamp: index,
                            by: column.column_type.clone(),
                        });
                    }
                    columns.push(column.clone());
                    origins.push(Origin::Base(by));
                }
                None => {
                    columns.push(stamp.clone());
                    origins.push(Origin::Stamp(index));
                }
            }
        }
        let stamps = &parts.stamps.columns;
        for (index, column) in base.columns().iter().enumerate() {
            if !stamps.iter().any(|stamp| stamp.name == column.name) {
                columns.push(column.clone());
                origins.push(Origin::Base(index));
            }
        }
        (columns, origins, displaced)
    }

    /// Appends, for each of `triggers` in order, the rows of the base as they are now,
    /// stamped with that trigger row, after the `size` rows held. Returns the new size.
    ///
    /// Behavior: the static io.deephaven.engine.table.impl.QueryTable#snapshotHistoryInternal
    fn append(&self, triggers: &RowSet, size: u64) -> Result<u64, TableFailure> {
        let rows = self.parts.base.row_set();
        let (copies, each) = (triggers.size(), rows.size());
        if copies == 0 || each == 0 {
            return Ok(size);
        }
        let trigger = &self.parts.trigger;
        let sources = &self.parts.stamps.sources;
        for displaced in &self.displaced {
            let stamps = trigger.column(sources[displaced.stamp]).chunk(triggers)?;
            let column = &self.parts.stamps.columns[displaced.stamp];
            if (0..stamps.len())
                .any(|row| !fits(&column.column_type, &displaced.by, stamps.is_null(row)))
            {
                return Err(failure(format!(
                    "Column name conflicts: the stamp column {} is {:?} and the base's is {:?}",
                    column.name, column.column_type, displaced.by
                )));
            }
        }
        let added = RowSet::from_range(size as i64, (size + copies * each - 1) as i64);
        for (origin, column) in self.origins.iter().zip(&self.columns) {
            let values = match origin {
                Origin::Stamp(stamp) => take(
                    &trigger.column(sources[*stamp]).chunk(triggers)?,
                    (0..copies).flat_map(|copy| std::iter::repeat_n(copy, each as usize)),
                )?,
                Origin::Base(index) => take(
                    &self.parts.base.column(*index).chunk(&rows)?,
                    (0..copies).flat_map(|_| 0..each),
                )?,
            };
            column.fill(&added, &values)?;
        }
        Ok(size + copies * each)
    }
}

/// The result starts with a copy of the base for every row the trigger has. It is flat and
/// carries no attribute.
///
/// Java first takes the trigger as append-only, so an update of the trigger that removes,
/// modifies or shifts rows, or adds a row before its last row, fails the result.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#snapshotHistory and
/// #snapshotHistoryInternal, with the append-only assertions of
/// io.deephaven.engine.table.impl.BaseTable#notifyListeners
pub(super) fn snapshot_history(parts: Parts) -> Result<Arc<dyn Table>, SnapshotError> {
    let base = parts.base.clone();
    let trigger = parts.trigger.clone();
    let graph = super::graph_of(&trigger);
    let (columns, origins, displaced) = History::layout(&parts);
    let history = History {
        columns: sources(graph.as_ref(), &columns),
        parts,
        origins,
        displaced,
    };
    let definition = definition(columns);
    let columns: Vec<Arc<dyn ColumnSource>> = readable(&history.columns).collect();
    let conflict = |failure: TableFailure| SnapshotError::ColumnConflict(failure.message);

    if graph.is_none() {
        let size = reading(&[&base], || {
            history.append(&trigger.row_set(), 0).map_err(conflict)
        })?;
        let spec = TableSpec::new(definition, RowSet::flat(size), columns).flat();
        return Ok(QueryTable::new_static(spec));
    }

    let waits_for: Vec<Arc<dyn Dependency>> = if base.is_refreshing() {
        vec![base.clone()]
    } else {
        Vec::new()
    };
    let mut refused = None;
    let parents = [trigger.clone()];
    let result = QueryTable::derive_merged(&parents, waits_for, || {
        if base.is_failed() {
            return Err(failure("the parent table has failed"));
        }
        let size = history.append(&trigger.row_set(), 0).inspect_err(|e| {
            refused = Some(conflict(e.clone()));
        })?;
        let spec = TableSpec::new(definition, RowSet::flat(size), columns).flat();
        let operator = move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
            let Some(update) = &upstream[0] else {
                return Ok(TableUpdate::empty());
            };
            if !(update.removed.is_empty()
                && update.modified.is_empty()
                && update.shifted.is_empty())
            {
                return Err(failure(
                    "the trigger of a snapshot history removed, modified or shifted rows",
                ));
            }
            let before = trigger.prev_row_set();
            if let (Some(last), Some(first)) = (before.last_row_key(), update.added.first_row_key())
            {
                if last >= first {
                    return Err(failure(
                        "the trigger of a snapshot history added rows before its last row",
                    ));
                }
            }
            let old_size = result.row_set().size();
            let size = history.append(&update.added, old_size)?;
            Ok(TableUpdate {
                added: RowSet::flat(size).minus(&RowSet::flat(old_size)),
                ..TableUpdate::empty()
            })
        };
        Ok((spec, operator))
    });
    match (result, refused) {
        (Ok(result), _) => Ok(result),
        (Err(_), Some(refused)) => Err(refused),
        (Err(failure), None) => Err(failure.into()),
    }
}
