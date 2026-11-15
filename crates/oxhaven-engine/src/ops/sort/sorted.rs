//! `sort`: the parent's rows ordered by their sort keys, read through a row redirection.
//!
//! The result is positional: row key `p` is the row at position `p` of the sorted order.
//! Java keeps gaps in the result's key space so that an insertion moves few rows; here an
//! insertion shifts every row after it. The rows, their order and their values are the same,
//! and row keys are not compared (`PARITY.md` A4).

use super::keys::{KeyEncoder, SortKey};
use super::{SortColumn, SortError};
use crate::ops::merge::flattened_shift_data;
use crate::table::{is_copied, reads_through, QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, InputTable, Table, TableAttributes, TableFailure};
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
            reads_through(RedirectedSource::new(
                redirection.clone(),
                parent.column(index),
            ))
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

/// Of the add-only, append-only and blink attributes, only blink passes through a sort. The
/// input table of the parent does too, unless the sort is by an absolute value: see
/// [`carried_input_table`].
///
/// Behavior: the `Sort` entries of the attribute table in
/// io.deephaven.engine.table.impl.BaseTable
fn sort_attributes(parent: &dyn Table) -> TableAttributes {
    let mut attributes = TableAttributes::default();
    attributes.blink = parent.attributes().blink;
    attributes
}

/// The input table the result of sorting `parent` by `columns` is written through.
///
/// A plain sort keeps its parent's. Java has no sort by an absolute value: its server adds
/// a column of absolute values with `updateView`, sorts by it, and removes it with
/// `dropColumns`, and `dropColumns` does not keep the `InputTable` attribute.
///
/// Behavior: io.deephaven.server.table.ops.SortTableGrpcImpl#create and the
/// `INPUT_TABLE_ATTRIBUTE` entry of
/// io.deephaven.engine.table.impl.BaseTable#attributeToCopySet, which lists `Sort` and
/// `UpdateView` and not `DropColumns`
fn carried_input_table(parent: &dyn Table, columns: &[SortColumn]) -> Option<Arc<dyn InputTable>> {
    if columns.iter().any(|column| column.absolute) {
        None
    } else {
        parent.input_table()
    }
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
    let input = carried_input_table(&**parent, columns);
    if !parent.is_refreshing() {
        return Ok(static_sort(parent, &encoder, input)?);
    }
    if parent.attributes().blink {
        // Java's server sorts by an absolute value through an `updateView` of a formula,
        // which is a table of its own and not a copy.
        let copied = is_copied(&**parent) && !columns.iter().any(|column| column.absolute);
        return Ok(blink_sort(parent, encoder, input, copied)?);
    }
    Ok(incremental_sort(parent, encoder, input)?)
}

/// Behavior: io.deephaven.engine.table.impl.SortOperation#historicalSort
fn static_sort(
    parent: &Arc<dyn Table>,
    encoder: &KeyEncoder,
    input: Option<Arc<dyn InputTable>>,
) -> Result<Arc<dyn Table>, TableFailure> {
    if parent.is_failed() {
        return Err(failure("the parent table has failed"));
    }
    let rows = parent.row_set();
    let entries = sorted_entries(encoder, &**parent, &rows)?;
    // A table that is already in order is returned as it is, with its own row keys, unless
    // the result is to lose the parent's input table.
    let keeps_input = input.is_some() == parent.input_table().is_some();
    if keeps_input && entries.iter().map(|entry| entry.parent_key).eq(rows.iter()) {
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
    Ok(QueryTable::new_static(TableSpec { input, ..spec }))
}

/// The sort of a blink table: every cycle the result is the sorted rows of that cycle.
/// `copied` says whether Java answers it with a copy, see [`is_copied`].
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#blinkTableSort and the `Sort` key
/// of io.deephaven.engine.table.impl.MemoizedOperationKey
fn blink_sort(
    parent: &Arc<dyn Table>,
    encoder: KeyEncoder,
    input: Option<Arc<dyn InputTable>>,
    copied: bool,
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
        .flat()
        .copied(copied);
        let spec = TableSpec { input, ..spec };

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

/// What an update does with a row that the parent did not remove.
enum Fate {
    /// The row is not placed again.
    Untouched,
    /// The row was placed again and stays where it is, with this sort key.
    Kept(SortKey),
    /// The row was placed again and leaves its position for another.
    Moved,
}

/// A row of the previous result that the parent did not remove.
struct Survivor {
    /// The row's previous position.
    position: usize,
    /// The row's current parent key with its previous sort key.
    entry: Entry,
    fate: Fate,
}

/// The index of the last of `survivors` that does not sort after `target`, comparing the
/// target's current sort key with the previous sort keys of the survivors, and current
/// parent keys where those are equal. `None` when every survivor sorts after the target.
///
/// A modified row is among the survivors with the sort key it had: if its new key sorts
/// before its old one, the place found is before the row itself.
///
/// Behavior: io.deephaven.engine.table.impl.SortListener.TargetComparator#compareTargetTo,
/// as io.deephaven.engine.rowset.RowSet.SearchIterator#binarySearchValue applies it in
/// io.deephaven.engine.table.impl.SortListener#onUpdate
fn place_among(survivors: &[Survivor], target: &Entry) -> Option<usize> {
    survivors
        .partition_point(|survivor| {
            (&survivor.entry.key, survivor.entry.parent_key) <= (&target.key, target.parent_key)
        })
        .checked_sub(1)
}

/// Whether a modified row whose sort columns changed is reported as modified, not as
/// removed and added: the place found for it (`after`) is the row itself (`current`), and
/// the row queued last for insertion does not go to that same place. That row sorts
/// before this one, so this one has to follow it.
///
/// Behavior: the test "check if new location differs from current location or if the
/// previous row needs to slot here" in io.deephaven.engine.table.impl.SortListener#onUpdate
fn keeps_its_place(
    current: usize,
    after: Option<usize>,
    last_queued: Option<Option<usize>>,
) -> bool {
    after == Some(current) && last_queued != Some(Some(current))
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
    /// only when a sort column is among the modified columns, and is then reported as
    /// modified or as removed and added by the rule of [`keeps_its_place`].
    /// Where Java moves the rows in the way of each row it places, this merges the rows
    /// that stay with the rows to place in one pass.
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

        // The rows the parent did not remove, in their previous order and with their
        // previous sort keys, under the parent keys they have now.
        let mut survivors = Vec::with_capacity(old.len());
        for (position, entry) in old.into_iter().enumerate() {
            if upstream.removed.contains(entry.parent_key) {
                removed.push(position as RowKey);
                continue;
            }
            survivors.push(Survivor {
                position,
                entry: Entry {
                    parent_key: upstream.shifted.apply_to_key(entry.parent_key),
                    key: entry.key,
                },
                fate: Fate::Untouched,
            });
        }

        // The rows to put into the result: the added rows and the modified rows that do
        // not keep their place.
        let mut queued = Vec::with_capacity(incoming.len());
        if resort {
            let index_of: BTreeMap<RowKey, usize> = survivors
                .iter()
                .enumerate()
                .filter(|(_, survivor)| upstream.modified.contains(survivor.entry.parent_key))
                .map(|(index, survivor)| (survivor.entry.parent_key, index))
                .collect();
            // Where the row queued last goes: after this survivor, or before all of them.
            let mut last_queued: Option<Option<usize>> = None;
            for entry in incoming {
                let after = place_among(&survivors, &entry);
                let current = index_of.get(&entry.parent_key).copied();
                match current {
                    Some(index) if keeps_its_place(index, after, last_queued) => {
                        survivors[index].fate = Fate::Kept(entry.key);
                    }
                    _ => {
                        if let Some(index) = current {
                            survivors[index].fate = Fate::Moved;
                        }
                        last_queued = Some(after);
                        queued.push(entry);
                    }
                }
            }
        } else {
            queued = incoming;
        }

        // The rows that stay in the result, in their previous order: the kept rows have
        // their new sort keys, and the order is still the sorted one.
        let report_modified = !resort && !upstream.modified.is_empty();
        let mut staying = Vec::with_capacity(survivors.len());
        for survivor in survivors {
            let Survivor {
                position,
                mut entry,
                fate,
            } = survivor;
            let changed = match fate {
                Fate::Moved => {
                    removed.push(position as RowKey);
                    continue;
                }
                Fate::Kept(key) => {
                    entry.key = key;
                    true
                }
                Fate::Untouched => report_modified && upstream.modified.contains(entry.parent_key),
            };
            staying.push((entry, changed));
        }
        removed.sort_unstable();

        let mut merged: Vec<Entry> = Vec::with_capacity(staying.len() + queued.len());
        let mut staying = staying.into_iter().peekable();
        let keep = |merged: &mut Vec<Entry>,
                    modified: &mut Vec<RowKey>,
                    (entry, changed): (Entry, bool)| {
            if changed {
                modified.push(merged.len() as RowKey);
            }
            merged.push(entry);
        };
        for entry in queued {
            // Rows with equal sort keys are in parent order: ties go to the lower key.
            while let Some(before) = staying.next_if(|(staying, _)| {
                (&staying.key, staying.parent_key) < (&entry.key, entry.parent_key)
            }) {
                keep(&mut merged, &mut modified, before);
            }
            added.push(merged.len() as RowKey);
            merged.push(entry);
        }
        for kept in staying {
            keep(&mut merged, &mut modified, kept);
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
    input: Option<Arc<dyn InputTable>>,
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
        let spec = TableSpec { input, ..spec };
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
