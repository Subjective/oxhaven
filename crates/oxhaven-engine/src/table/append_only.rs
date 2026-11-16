//! Append-only tables of the rows a blink table showed.

use super::{failure, memoized, tracked_source, CopyType, QueryTable, TableSpec};
use oxhaven_column::WritableSource;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::{Arc, Mutex, PoisonError, Weak};

/// Behavior: io.deephaven.engine.table.impl.BlinkTableTools#isBlink
fn is_blink(table: &dyn Table) -> bool {
    table.is_refreshing() && table.attributes().blink
}

/// A result remembered for a blink table and a size limit.
struct Remembered {
    parent: Weak<dyn Table>,
    limit: u64,
    result: Weak<dyn Table>,
}

/// The results that are still alive. Java keeps them in the blink table, weakly; the
/// `Table` contract has no place for them, so they are kept here and found by the table.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#getResult with the `BlinkToAppendOnly`
/// key of io.deephaven.engine.table.impl.MemoizedOperationKey
static REMEMBERED: Mutex<Vec<Remembered>> = Mutex::new(Vec::new());

/// An append-only table of every row `parent`, a blink table, has shown since this was
/// called, starting with the rows it shows now, up to `limit` rows: rows beyond the limit
/// are not appended. The result is flat, refreshing, add-only and append-only, and fails
/// when `parent` fails or modifies or shifts a row.
///
/// Asked again of the same table with the same limit while the first result is alive, the
/// answer is that result, with the rows it has collected since. `head` of a blink table is
/// this with the limit of the `head`.
///
/// An error with Java's text when `parent` is not a refreshing blink table.
///
/// Behavior: io.deephaven.engine.table.impl.BlinkTableTools#blinkToAppendOnly(Table, long)
/// and its `BlinkToAppendOnlyOperation`
pub fn blink_to_append_only(
    parent: &Arc<dyn Table>,
    limit: u64,
) -> Result<Arc<dyn Table>, TableFailure> {
    if !is_blink(&**parent) {
        return Err(failure(
            "java.lang.IllegalArgumentException: Input is not a blink table!",
        ));
    }
    // The lock is not held while the result is made, which takes the graph's lock.
    let find = || {
        let mut remembered = REMEMBERED.lock().unwrap_or_else(PoisonError::into_inner);
        remembered.retain(|entry| entry.result.strong_count() > 0);
        remembered.iter().find_map(|entry| {
            let same = entry.limit == limit
                && entry
                    .parent
                    .upgrade()
                    .is_some_and(|other| Arc::ptr_eq(&other, parent));
            same.then(|| entry.result.upgrade()).flatten()
        })
    };
    if let Some(result) = find() {
        return Ok(result);
    }
    let result: Arc<dyn Table> = collect(parent, limit)?;
    // Behavior: the `BlinkToAppendOnly` key, which a copy shares with the table it is a
    // copy of and whose copy type is `CopyAttributeOperation.None`: the answer for a copy,
    // such as a blink input table, is not add-only.
    let result = memoized(parent, CopyType::None, false, result)?;
    // Another thread may have made one meanwhile; the first one made is the answer.
    if let Some(first) = find() {
        return Ok(first);
    }
    let mut remembered = REMEMBERED.lock().unwrap_or_else(PoisonError::into_inner);
    remembered.push(Remembered {
        parent: Arc::downgrade(parent),
        limit,
        result: Arc::downgrade(&result),
    });
    Ok(result)
}

/// Copies `rows` of `parent` into `columns` at `keys`, row for row in key order.
fn copy_rows(
    parent: &dyn Table,
    rows: &RowSet,
    columns: &[Arc<WritableSource>],
    keys: &RowSet,
) -> Result<(), TableFailure> {
    for (index, column) in columns.iter().enumerate() {
        column.fill(keys, &parent.column(index).chunk(rows)?)?;
    }
    Ok(())
}

/// Behavior: BlinkTableTools.BlinkToAppendOnlyOperation#initialize and #onUpdate
fn collect(parent: &Arc<dyn Table>, limit: u64) -> Result<Arc<QueryTable>, TableFailure> {
    QueryTable::derive(parent, || {
        let graph = parent
            .update_graph()
            .ok_or_else(|| failure("a refreshing table has an update graph"))?;
        let columns: Vec<Arc<WritableSource>> = parent
            .definition()
            .columns()
            .iter()
            .map(|column| tracked_source(&graph, column.column_type.clone()))
            .collect();
        let initial = parent.row_set().sub_set_by_position_range(0, limit);
        let rows = RowSet::flat(initial.size());
        copy_rows(&**parent, &initial, &columns, &rows)?;

        let mut attributes = TableAttributes::default();
        attributes.add_only = true;
        attributes.append_only = true;
        let sources = columns
            .iter()
            .map(|column| column.clone() as Arc<dyn ColumnSource>)
            .collect();
        let spec = TableSpec::new(parent.definition().clone(), rows, sources)
            .keeping_partitioning()
            .with_attributes(attributes)
            .flat();

        let parent = parent.clone();
        let operator = move |result: &QueryTable, upstream: &TableUpdate| {
            if !upstream.modified.is_empty() || !upstream.shifted.is_empty() {
                return Err(failure(
                    "java.lang.IllegalStateException: Blink tables should not modify or shift!",
                ));
            }
            let size = result.row_set().size();
            // Once full, nothing more is appended.
            let room = limit.saturating_sub(size);
            let taken = upstream.added.sub_set_by_position_range(0, room);
            let mut downstream = TableUpdate::empty();
            if taken.is_empty() {
                return Ok(downstream);
            }
            let keys = RowSet::from_range(size as RowKey, (size + taken.size() - 1) as RowKey);
            copy_rows(&*parent, &taken, &columns, &keys)?;
            downstream.added = keys;
            Ok(downstream)
        };
        Ok((spec, operator))
    })
}
