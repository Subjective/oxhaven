//! The ring table: the rows added to a table last.

use super::{failure, tracked_source, QueryTable, TableSpec, UnaryOperator};
use oxhaven_column::WritableSource;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::{Arc, Mutex, PoisonError};

#[cfg(test)]
mod tests;

/// The largest capacity Java accepts.
///
/// Behavior: io.deephaven.util.datastructures.ArrayUtil#MAX_ARRAY_SIZE
const MAX_CAPACITY: i32 = i32::MAX - 8;

/// A table of the `capacity` rows that were added to `parent` last, in the order they were
/// added. Rows added in one update cycle count in the order of their row keys. What
/// `parent` removes stays in the ring until newer rows push it out, which is what makes a
/// ring of a blink table keep rows. With `initialize` the ring starts with the last rows
/// `parent` has now, and without it empty.
///
/// The row keys count the rows added since the ring was made: the ring holds the keys
/// `n - capacity .. n` after `n` rows. The result has none of the parent's attributes and
/// is not flat. A static parent gives a static result.
///
/// The result fails when `parent` modifies or shifts a row.
///
/// The errors are Java's, with its texts: a capacity that is not positive or is above
/// `i32::MAX - 8`, a static parent without `initialize`, and a parent without columns
/// that is refreshing or has rows to start with. As in Java, the capacity of a ring
/// without columns is not looked at.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ring.RingTableTools#of,
/// io.deephaven.engine.table.impl.sources.ring.AddsToRingsListener#of and #init,
/// io.deephaven.engine.table.impl.sources.ring.AbstractRingChunkSource (the constructor)
pub fn ring_table(
    parent: &Arc<dyn Table>,
    capacity: i32,
    initialize: bool,
) -> Result<Arc<QueryTable>, TableFailure> {
    if capacity > MAX_CAPACITY {
        return Err(failure(format!(
            "io.deephaven.base.verify.RequirementFailure: Requirement failed: required capacity \
             <= {MAX_CAPACITY}, instead capacity == {capacity}."
        )));
    }
    let graph = parent.update_graph().filter(|_| parent.is_refreshing());
    if graph.is_none() && !initialize {
        return Err(failure(
            "java.lang.IllegalArgumentException: Trying to initialize \
             io.deephaven.engine.table.impl.sources.ring.AddsToRingsListener against a static \
             table, but init=NONE; no data will be filled in this case.",
        ));
    }
    let definition = parent.definition().clone();
    let no_columns = definition.columns().is_empty();
    if !no_columns && capacity < 0 {
        return Err(failure(format!(
            "java.lang.NegativeArraySizeException: {capacity}"
        )));
    }
    if !no_columns && capacity == 0 {
        return Err(failure(
            "java.lang.IllegalArgumentException: Capacity must be positive",
        ));
    }
    QueryTable::derive(parent, || {
        let columns: Vec<Arc<WritableSource>> = definition
            .columns()
            .iter()
            .map(|column| match &graph {
                Some(graph) => tracked_source(graph, column.column_type.clone()),
                None => Arc::new(WritableSource::new(column.column_type.clone())),
            })
            .collect();
        let ring = Ring {
            parent: parent.clone(),
            columns,
            capacity: RowKey::from(capacity),
            appended: Mutex::new(0),
        };
        let rows = parent.row_set();
        if initialize && !rows.is_empty() {
            if no_columns {
                // Java reads the row set of the first column's ring.
                return Err(failure(
                    "java.lang.ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0",
                ));
            }
            ring.append(&rows)?;
        }
        if no_columns && graph.is_some() {
            // The text is all of Java's: its listener refuses to have no columns.
            return Err(failure("java.lang.IllegalArgumentException"));
        }
        let sources = ring.columns.iter();
        let sources = sources.map(|column| column.clone() as Arc<dyn ColumnSource>);
        let spec = TableSpec::new(definition.clone(), ring.rows(), sources.collect());
        Ok((spec, ring))
    })
}

/// The columns of a ring table and how many rows it was given.
struct Ring {
    parent: Arc<dyn Table>,
    columns: Vec<Arc<WritableSource>>,
    capacity: RowKey,
    /// The number of rows appended so far, which is the row key of the next one.
    appended: Mutex<RowKey>,
}

/// The row keys `first..=last`, or none if `last < first`.
fn range(first: RowKey, last: RowKey) -> RowSet {
    if last < first {
        RowSet::empty()
    } else {
        RowSet::from_range(first, last)
    }
}

impl Ring {
    /// The first row key of a ring that was given `appended` rows.
    ///
    /// Behavior: AbstractRingChunkSource#firstKey
    fn first_key(&self, appended: RowKey) -> RowKey {
        (appended - self.capacity).max(0)
    }

    /// Behavior: io.deephaven.engine.table.impl.sources.ring.RingColumnSource#rowSet
    fn rows(&self) -> RowSet {
        let appended = *self.appended.lock().unwrap_or_else(PoisonError::into_inner);
        range(self.first_key(appended), appended - 1)
    }

    /// Appends the rows `added` of the parent, in the order of their keys, and returns what
    /// that changes: the rows that fell out of the ring and the rows that are new in it.
    /// Of more rows than the ring holds, only the last are read.
    ///
    /// Behavior: AbstractRingChunkSource#append, RingColumnSource#tableUpdate
    fn append(&self, added: &RowSet) -> Result<TableUpdate, TableFailure> {
        let mut appended = self.appended.lock().unwrap_or_else(PoisonError::into_inner);
        let before = *appended;
        let size = added.size();
        let after = before + size as RowKey;
        let kept = added.sub_set_by_position_range(size.saturating_sub(self.capacity as u64), size);
        let (old_first, new_first) = (self.first_key(before), self.first_key(after));
        let removed = range(old_first, new_first.min(before) - 1);
        let added = range(new_first.max(before), after - 1);
        for (index, column) in self.columns.iter().enumerate() {
            let values = self.parent.column(index).chunk(&kept)?;
            column.remove(&removed);
            column.fill(&added, &values)?;
        }
        *appended = after;
        Ok(TableUpdate {
            added,
            removed,
            ..TableUpdate::empty()
        })
    }
}

impl UnaryOperator for Ring {
    /// Behavior: AddsToRingsListener#onUpdate
    fn on_update(
        &self,
        _result: &QueryTable,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure> {
        if !upstream.modified.is_empty() || !upstream.shifted.is_empty() {
            return Err(failure(
                "java.lang.IllegalStateException: Not expecting modifies or shifts",
            ));
        }
        // What the parent removes stays in the ring.
        if upstream.added.is_empty() {
            return Ok(TableUpdate::empty());
        }
        self.append(&upstream.added)
    }
}
