//! `snapshot_when` with `incremental`: the result has the base's row keys, and a trigger
//! copies and stamps only the rows of the base that changed since the trigger before.

use super::{failure, readable, reading, sources, spread, Parts, SnapshotError};
use crate::table::{QueryTable, TableSpec};
use crate::update::compose;
use oxhaven_column::WritableSource;
use oxhaven_contracts::rowset::{RowRange, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::{Arc, Mutex, PoisonError};

/// The copied rows, held at the base's row keys.
struct Copy {
    parts: Parts,
    data: Vec<Arc<WritableSource>>,
    stamps: Vec<Arc<WritableSource>>,
}

impl Copy {
    /// Copies the base's `rows` and stamps them with the trigger's last row. Without a
    /// trigger row the stamp columns are not written: a row keeps the stamp it had, and a
    /// new row has the stamp last written at its row key, or null if there was none. (Java
    /// never clears the stamp of a removed row; `fixtures/snapshot_when_incremental.json`
    /// records a row that comes back with the stamp of its predecessor.)
    ///
    /// Behavior: io.deephaven.engine.table.impl.snapshot.SnapshotIncrementalListener#copyRowsToResult
    fn copy(&self, rows: &RowSet) -> Result<(), TableFailure> {
        if let Some(stamp) = self.parts.stamps.last(&*self.parts.trigger)? {
            for (source, value) in self.stamps.iter().zip(&stamp) {
                source.fill(rows, &spread(value, rows.size())?)?;
            }
        }
        self.parts.copy_data(rows, &self.data, rows)
    }
}

/// What the listener of two refreshing tables remembers between cycles.
#[derive(Default)]
struct Pending {
    /// Whether the first snapshot, which copies the whole base, has been taken.
    started: bool,
    /// The changes of the base since the last trigger as one update, with the base's row
    /// set from before them.
    changes: Option<(RowSet, TableUpdate)>,
    /// The base's row set at the last snapshot after the first, or empty before there was
    /// one. See [`changed_rows`].
    last_rows: RowSet,
}

/// The rows of the base to add, remove and copy again, in this order, given its changes
/// `update` since the last snapshot, its row set `current`, and `last`, the row set
/// remembered from the snapshot before.
///
/// An update without shifts that does not remove and add one row key is taken as it is.
/// Otherwise the row sets are compared: rows only in `current` are added, rows only in
/// `last` are removed, and the rows of both that the update modified, shifted from or to,
/// or removed and added back are copied again.
///
/// `last` is empty until the second snapshot, not the base at the first one, because Java
/// compares with a row set that only this path ever writes. The first comparison therefore
/// finds every row of the base added and none removed: the caller copies and stamps all of
/// them again and keeps the rows the base has deleted
/// (`fixtures/snapshot_when_incremental_readded.json`).
///
/// Behavior: io.deephaven.engine.table.impl.util.RowSetShiftDataExpander#RowSetShiftDataExpander
/// as io.deephaven.engine.table.impl.snapshot.SnapshotIncrementalListener#doSnapshot calls it
fn changed_rows(update: &TableUpdate, current: &RowSet, last: &RowSet) -> (RowSet, RowSet, RowSet) {
    if update.shifted.is_empty() && !update.added.overlaps(&update.removed) {
        return (
            update.added.clone(),
            update.removed.clone(),
            update.modified.clone(),
        );
    }
    let added = current.minus(last);
    let removed = last.minus(current);
    let moved = update.shifted.ranges().iter().flat_map(|range| {
        [
            RowRange::new(range.first, range.last),
            RowRange::new(range.first + range.delta, range.last + range.delta),
        ]
    });
    let modified = update
        .modified
        .union(&RowSet::from_ranges(moved).intersect(current))
        .union(&update.removed.intersect(&update.added))
        .minus(&added);
    (added, removed, modified)
}

impl Copy {
    /// Brings the result up to the base, whose changes since the last snapshot are
    /// `update`, and returns the result's change. `held` is the result's row set.
    ///
    /// Java inserts the added rows into the result's row set and reports them as added
    /// even when the result has them already, which happens after the first comparison
    /// described at [`changed_rows`]. Here such a row is reported as modified: the state is
    /// the same, and an update may not add a row that exists.
    ///
    /// Behavior: io.deephaven.engine.table.impl.snapshot.SnapshotIncrementalListener#doSnapshot
    fn refresh(
        &self,
        update: &TableUpdate,
        last: &RowSet,
        held: &RowSet,
    ) -> Result<TableUpdate, TableFailure> {
        let current = self.parts.base.row_set();
        let (added, removed, modified) = changed_rows(update, &current, last);
        self.copy(&added.union(&modified))?;
        let modified = modified.union(&added.intersect(held));
        Ok(TableUpdate {
            added: added.minus(held),
            removed,
            modified_columns: if modified.is_empty() {
                ModifiedColumnSet::empty()
            } else {
                ModifiedColumnSet::All
            },
            modified,
            ..TableUpdate::empty()
        })
    }
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#snapshotIncrementalInternal
pub(super) fn snapshot_incremental(
    parts: Parts,
    initial: bool,
) -> Result<Arc<dyn Table>, SnapshotError> {
    let definition = parts.base_then_stamps()?;
    let base = parts.base.clone();
    let trigger = parts.trigger.clone();
    let both_refresh = base.is_refreshing() && trigger.is_refreshing();
    // An initial snapshot of a static table with any other is all there will ever be, and
    // so is the empty result of a static trigger.
    let graph = super::graph_of(&trigger).filter(|_| both_refresh || !initial);
    let width = base.definition().columns().len();
    let copy = Copy {
        data: sources(graph.as_ref(), &definition.columns()[..width]),
        stamps: sources(graph.as_ref(), &definition.columns()[width..]),
        parts,
    };
    let columns: Vec<Arc<dyn ColumnSource>> =
        readable(&copy.data).chain(readable(&copy.stamps)).collect();
    let first = |copy: &Copy| -> Result<RowSet, TableFailure> {
        let rows = (*copy.parts.base.row_set()).clone();
        copy.copy(&rows)?;
        Ok(rows)
    };

    if graph.is_none() {
        let rows = reading(&[&base, &trigger], || {
            Ok(if initial {
                first(&copy)?
            } else {
                RowSet::empty()
            })
        })?;
        return Ok(QueryTable::new_static(TableSpec::new(
            definition, rows, columns,
        )));
    }

    if !both_refresh {
        // A static base is copied once, at the first trigger.
        let result = QueryTable::derive(&trigger, || {
            let taken = Mutex::new(false);
            let operator = move |_result: &QueryTable, upstream: &TableUpdate| {
                let mut taken = taken.lock().unwrap_or_else(PoisonError::into_inner);
                if *taken || !copy.parts.stamps.delivers(upstream) {
                    return Ok(TableUpdate::empty());
                }
                *taken = true;
                Ok(TableUpdate {
                    added: first(&copy)?,
                    ..TableUpdate::empty()
                })
            };
            Ok((
                TableSpec::new(definition, RowSet::empty(), columns),
                operator,
            ))
        })?;
        return Ok(result);
    }

    let result = QueryTable::derive_merged(&[base.clone(), trigger], Vec::new(), || {
        let mut pending = Pending::default();
        let rows = if initial {
            pending.started = true;
            first(&copy)?
        } else {
            RowSet::empty()
        };
        let pending = Mutex::new(pending);
        // Behavior: io.deephaven.engine.table.impl.snapshot.SnapshotIncrementalListener#process
        let operator = move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
            let mut pending = pending.lock().unwrap_or_else(PoisonError::into_inner);
            if let (true, Some(update)) = (pending.started, &upstream[0]) {
                // Changes of the base before the first snapshot are part of that snapshot.
                pending.changes = Some(match pending.changes.take() {
                    None => ((*base.prev_row_set()).clone(), (**update).clone()),
                    Some((before, changes)) => {
                        let both = compose(&before, &changes, update)
                            .map_err(|e| failure(format!("the base's updates do not fit: {e}")))?;
                        (before, both)
                    }
                });
            }
            let triggered = upstream[1]
                .as_ref()
                .is_some_and(|update| copy.parts.stamps.delivers(update));
            if !triggered {
                return Ok(TableUpdate::empty());
            }
            if !pending.started {
                pending.started = true;
                return Ok(TableUpdate {
                    added: first(&copy)?,
                    ..TableUpdate::empty()
                });
            }
            let Some((_, changes)) = pending.changes.take() else {
                return Ok(TableUpdate::empty());
            };
            let downstream = copy.refresh(&changes, &pending.last_rows, &result.row_set())?;
            pending.last_rows = (*base.row_set()).clone();
            Ok(downstream)
        };
        Ok((TableSpec::new(definition, rows, columns), operator))
    })?;
    Ok(result)
}
