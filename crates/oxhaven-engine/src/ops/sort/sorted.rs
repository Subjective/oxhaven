//! `sort`: the parent's rows ordered by their sort keys, read through a row redirection.
//!
//! The result is positional: row key `p` is the row at position `p` of the sorted order.
//! Java keeps gaps in the result's key space so that an insertion moves few rows; here an
//! insertion shifts every row after it. The rows, their order and their values are the same,
//! and row keys are not compared (`PARITY.md` A4).

use super::keys::{KeyEncoder, SortKey};
use super::{SortColumn, SortError};
use crate::ops::merge::flattened_shift_data;
use crate::table::{QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// One row of the result: the parent row it shows and what it sorts by.
struct Entry {
    parent_key: RowKey,
    key: SortKey,
}

/// `rows` of the parent with their sort keys, in sorted order. Rows with equal keys keep
/// the parent's order.
///
/// Behavior: io.deephaven.engine.table.impl.SortHelpers#getSortedKeys (a stable sort)
fn sorted_entries(
    encoder: &KeyEncoder,
    parent: &dyn Table,
    rows: &RowSet,
) -> Result<Vec<Entry>, TableFailure> {
    let keys = encoder.encode(parent, rows)?;
    let mut entries: Vec<Entry> = rows
        .iter()
        .zip(keys)
        .map(|(parent_key, key)| Entry { parent_key, key })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(entries)
}

/// The parent's columns read through `redirection`.
fn redirected_columns(
    parent: &Arc<dyn Table>,
    redirection: &Arc<WritableRowRedirection>,
) -> Vec<Arc<dyn ColumnSource>> {
    let redirection: Arc<dyn RowRedirection> = redirection.clone();
    (0..parent.definition().columns().len())
        .map(|index| {
            Arc::new(RedirectedSource::new(
                redirection.clone(),
                parent.column(index),
            )) as Arc<dyn ColumnSource>
        })
        .collect()
}

/// Makes `redirection`, which maps the positions `0..old.len()` to `old`, map the positions
/// `0..new.len()` to `new`.
fn remap(
    redirection: &WritableRowRedirection,
    old: &[RowKey],
    new: impl ExactSizeIterator<Item = RowKey>,
) {
    let size = new.len();
    for (position, parent_key) in new.enumerate() {
        if old.get(position) != Some(&parent_key) {
            redirection.put(position as RowKey, parent_key);
        }
    }
    for position in size..old.len() {
        redirection.remove(position as RowKey);
    }
}

/// Only the blink attribute passes through a sort.
///
/// Behavior: the `Sort` entries of the attribute table in
/// io.deephaven.engine.table.impl.BaseTable
fn sort_attributes(parent: &dyn Table) -> TableAttributes {
    let mut attributes = TableAttributes::default();
    attributes.blink = parent.attributes().blink;
    attributes
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#sort and
/// io.deephaven.engine.table.impl.SortOperation#initialize
pub(super) fn sort(
    parent: &Arc<dyn Table>,
    columns: &[SortColumn],
) -> Result<Arc<dyn Table>, SortError> {
    if columns.is_empty() {
        return Ok(parent.clone());
    }
    let encoder = KeyEncoder::new(&**parent, columns)?;
    if !parent.is_refreshing() {
        return Ok(static_sort(parent, &encoder)?);
    }
    if parent.attributes().blink {
        return Ok(blink_sort(parent, encoder)?);
    }
    Ok(incremental_sort(parent, encoder)?)
}

/// Behavior: io.deephaven.engine.table.impl.SortOperation#historicalSort
fn static_sort(
    parent: &Arc<dyn Table>,
    encoder: &KeyEncoder,
) -> Result<Arc<dyn Table>, TableFailure> {
    if parent.is_failed() {
        return Err(failure("the parent table has failed"));
    }
    let rows = parent.row_set();
    let entries = sorted_entries(encoder, &**parent, &rows)?;
    // A table that is already in order is returned as it is, with its own row keys.
    if entries.iter().map(|entry| entry.parent_key).eq(rows.iter()) {
        return Ok(parent.clone());
    }
    let redirection = Arc::new(WritableRowRedirection::new());
    remap(&redirection, &[], entries.iter().map(|e| e.parent_key));
    let spec = TableSpec::new(
        parent.definition().clone(),
        RowSet::flat(entries.len() as u64),
        redirected_columns(parent, &redirection),
    )
    .with_attributes(sort_attributes(&**parent))
    .flat();
    Ok(QueryTable::new_static(spec))
}

/// The sort of a blink table: every cycle the result is the sorted rows of that cycle.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#blinkTableSort
fn blink_sort(
    parent: &Arc<dyn Table>,
    encoder: KeyEncoder,
) -> Result<Arc<dyn Table>, TableFailure> {
    let result = QueryTable::derive(parent, || {
        let entries = sorted_entries(&encoder, &**parent, &parent.row_set())?;
        let redirection = Arc::new(WritableRowRedirection::new());
        remap(&redirection, &[], entries.iter().map(|e| e.parent_key));
        if let Some(graph) = parent.update_graph() {
            let clock: Arc<dyn Clock> = Arc::new(graph);
            redirection.start_tracking_prev(clock);
        }
        let spec = TableSpec::new(
            parent.definition().clone(),
            RowSet::flat(entries.len() as u64),
            redirected_columns(parent, &redirection),
        )
        .with_attributes(sort_attributes(&**parent))
        .flat();

        let parent = parent.clone();
        let shown = Mutex::new(
            entries
                .into_iter()
                .map(|e| e.parent_key)
                .collect::<Vec<_>>(),
        );
        let operator = move |_result: &QueryTable, upstream: &TableUpdate| {
            if !upstream.modified.is_empty() || !upstream.shifted.is_empty() {
                return Err(failure(
                    "a blink table delivered modified or shifted rows to its sort",
                ));
            }
            let mut shown = shown.lock().unwrap_or_else(PoisonError::into_inner);
            if upstream.removed.size() != shown.len() as u64 {
                return Err(failure(
                    "a blink table did not remove all of its rows from its sort",
                ));
            }
            let entries = sorted_entries(&encoder, &*parent, &upstream.added)?;
            remap(&redirection, &shown, entries.iter().map(|e| e.parent_key));
            let removed = RowSet::flat(shown.len() as u64);
            *shown = entries.into_iter().map(|e| e.parent_key).collect();
            let mut downstream = TableUpdate::empty();
            downstream.added = RowSet::flat(shown.len() as u64);
            downstream.removed = removed;
            Ok(downstream)
        };
        Ok((spec, operator))
    })?;
    Ok(result)
}

/// The incremental half of a sort.
struct SortOperator {
    parent: Arc<dyn Table>,
    encoder: KeyEncoder,
    /// Indices of the sort columns in the parent's definition.
    sort_columns: Vec<usize>,
    transformer: ModifiedColumnTransformer,
    redirection: Arc<WritableRowRedirection>,
    /// The result's rows by position.
    entries: Mutex<Vec<Entry>>,
}

impl SortOperator {
    /// Brings the sorted order up to date with one change of the parent.
    ///
    /// The cases are Java's. Removed rows leave. Shifted rows keep their place and only
    /// change the parent key they show. Added rows are placed by sort key, after the rows
    /// with an equal key that precede them in the parent. A modified row is placed again
    /// only when a sort column is among the modified columns; if it then still lies between
    /// the same neighbours it is reported as modified, otherwise as removed and added.
    /// Where Java finds each place by a binary search of the result and moves the rows in
    /// the way, this merges the rows that stay with the rows to place in one pass.
    ///
    /// Behavior: io.deephaven.engine.table.impl.SortListener#onUpdate
    fn on_update(&self, upstream: &TableUpdate) -> Result<TableUpdate, TableFailure> {
        let resort = !upstream.modified.is_empty()
            && upstream.modified_columns.contains_any(&self.sort_columns);
        let to_place = if resort {
            upstream.added.union(&upstream.modified)
        } else {
            upstream.added.clone()
        };
        let incoming = sorted_entries(&self.encoder, &*self.parent, &to_place)?;

        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        let old = std::mem::take(&mut *entries);
        let old_keys: Vec<RowKey> = old.iter().map(|entry| entry.parent_key).collect();

        // Positions that leave the result, and positions that are new in it.
        let mut removed = Vec::new();
        let mut added = Vec::new();
        let mut modified = Vec::new();

        // Rows that stay where they are in the order, with their previous positions, and
        // the previous positions of the rows to place again, by current parent key.
        let mut staying = Vec::with_capacity(old.len());
        let mut placed_before: BTreeMap<RowKey, usize> = BTreeMap::new();
        for (position, entry) in old.into_iter().enumerate() {
            if upstream.removed.contains(entry.parent_key) {
                removed.push(position as RowKey);
                continue;
            }
            let parent_key = upstream.shifted.apply_to_key(entry.parent_key);
            if resort && upstream.modified.contains(parent_key) {
                placed_before.insert(parent_key, position);
            } else {
                staying.push((
                    position,
                    Entry {
                        parent_key,
                        key: entry.key,
                    },
                ));
            }
        }

        let report_modified = !resort && !upstream.modified.is_empty();
        let mut merged: Vec<Entry> = Vec::with_capacity(staying.len() + incoming.len());
        // The previous position of the last row that kept its place.
        let mut last_kept: Option<usize> = None;
        let mut staying = staying.into_iter().peekable();
        let keep = |merged: &mut Vec<Entry>,
                    modified: &mut Vec<RowKey>,
                    last_kept: &mut Option<usize>,
                    (position, entry): (usize, Entry)| {
            if report_modified && upstream.modified.contains(entry.parent_key) {
                modified.push(merged.len() as RowKey);
            }
            *last_kept = Some(position);
            merged.push(entry);
        };
        for entry in incoming {
            // Rows with equal sort keys are in parent order: ties go to the lower key.
            while let Some((position, before)) = staying.next_if(|(_, staying)| {
                (&staying.key, staying.parent_key) < (&entry.key, entry.parent_key)
            }) {
                keep(
                    &mut merged,
                    &mut modified,
                    &mut last_kept,
                    (position, before),
                );
            }
            let position = merged.len() as RowKey;
            match placed_before.get(&entry.parent_key) {
                // Still after the row that precedes it and before the row that follows.
                Some(&before)
                    if last_kept.is_none_or(|kept| kept < before)
                        && staying.peek().is_none_or(|(next, _)| before < *next) =>
                {
                    modified.push(position);
                    last_kept = Some(before);
                }
                Some(&before) => {
                    removed.push(before as RowKey);
                    added.push(position);
                }
                None => added.push(position),
            }
            merged.push(entry);
        }
        for kept in staying {
            keep(&mut merged, &mut modified, &mut last_kept, kept);
        }

        remap(
            &self.redirection,
            &old_keys,
            merged.iter().map(|entry| entry.parent_key),
        );
        *entries = merged;

        let removed = RowSet::from_keys(removed);
        let added = RowSet::from_keys(added);
        let shifted = flattened_shift_data(&removed, &added, old_keys.len() as u64)?;
        Ok(TableUpdate {
            added,
            removed,
            modified: RowSet::from_keys(modified),
            shifted,
            modified_columns: self.transformer.transform(&upstream.modified_columns),
        })
    }
}

/// Behavior: the refreshing branch of io.deephaven.engine.table.impl.SortOperation#initialize
fn incremental_sort(
    parent: &Arc<dyn Table>,
    encoder: KeyEncoder,
) -> Result<Arc<dyn Table>, TableFailure> {
    let result = QueryTable::derive(parent, || {
        let entries = sorted_entries(&encoder, &**parent, &parent.row_set())?;
        let redirection = Arc::new(WritableRowRedirection::new());
        remap(&redirection, &[], entries.iter().map(|e| e.parent_key));
        if let Some(graph) = parent.update_graph() {
            let clock: Arc<dyn Clock> = Arc::new(graph);
            redirection.start_tracking_prev(clock);
        }
        let definition = parent.definition().clone();
        let transformer = ModifiedColumnTransformer::identity(definition.columns().len());
        // Not flat, as in Java, although the row keys happen to be `0..size`.
        let spec = TableSpec::new(
            definition,
            RowSet::flat(entries.len() as u64),
            redirected_columns(parent, &redirection),
        )
        .with_attributes(sort_attributes(&**parent));
        let operator = SortOperator {
            parent: parent.clone(),
            sort_columns: encoder.column_indices(),
            encoder,
            transformer,
            redirection,
            entries: Mutex::new(entries),
        };
        Ok((spec, move |_result: &QueryTable, upstream: &TableUpdate| {
            operator.on_update(upstream)
        }))
    })?;
    Ok(result)
}
