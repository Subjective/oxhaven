//! `sort`: the parent's rows ordered by their sort keys, read through a row redirection.
//!
//! The result is positional: row key `p` is the row at position `p` of the sorted order.
//! Java keeps gaps in the result's key space so that an insertion moves few rows; here an
//! insertion shifts every row after it. The rows, their order and their values are the same,
//! and row keys are not compared (`PARITY.md` A4).

use super::keys::{KeyEncoder, SortKey};
use super::{SortColumn, SortDirection, SortError};
use crate::ops::merge::flattened_shift_data;
use crate::table::{
    copy_of, java_hash_order, mark, marks, memoized, reads_through, CopyType, Marks, QueryTable,
    TableSpec, IDENTITY_REDIRECTION,
};
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

/// `rows` of `parent` in the order a sort by `columns` gives them: rows that compare
/// equal keep their order. For an operation that shows the rows of a table in a sorted
/// order without making the sorted table, as a hierarchical table shows a node's rows.
/// The restriction of a table's sorting is not asked: such an operation has its own.
///
/// Behavior: io.deephaven.engine.table.impl.SortHelpers#getSortedKeys (a stable sort), with
/// the checks of io.deephaven.engine.table.impl.SortOperation#SortOperation
pub(crate) fn sorted_rows(
    parent: &dyn Table,
    rows: &RowSet,
    columns: &[SortColumn],
) -> Result<Vec<RowKey>, SortError> {
    let encoder = KeyEncoder::new(parent, columns)?;
    Ok(sorted_entries(&encoder, parent, rows)?
        .into_iter()
        .map(|entry| entry.parent_key)
        .collect())
}

/// Whether `parent` can be sorted by `columns`: it has them, of types that sort, and the
/// absolute ones of types that have an absolute value.
///
/// Behavior: the checks of io.deephaven.engine.table.impl.SortOperation#SortOperation
pub(crate) fn check_sort_columns(
    parent: &dyn Table,
    columns: &[SortColumn],
) -> Result<(), SortError> {
    KeyEncoder::new(parent, columns).map(|_| ())
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

/// The column a sort result is marked as sorted by, and whether it is descending: the
/// first sort column. `None` for a sort with a column sorted by its absolute value, which
/// Java's server sorts by a column it adds and then drops, and dropping columns drops the
/// marks of a sort.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#setSorted,
/// io.deephaven.server.table.ops.SortTableGrpcImpl#create and the
/// `SORTED_COLUMNS_ATTRIBUTE` entry of
/// io.deephaven.engine.table.impl.BaseTable#attributeToCopySet
fn first_column(columns: &[SortColumn]) -> Option<(&str, bool)> {
    if columns.iter().any(|column| column.absolute) {
        return None;
    }
    let first = columns.first()?;
    Some((
        first.column.as_str(),
        first.direction == SortDirection::Descending,
    ))
}

/// What Java notes of the row redirection of a sort result.
enum Redirection {
    /// A sort of a blink table, which notes nothing.
    Unnoted,
    /// A sort of a static table.
    Static,
    /// A sort of a refreshing table, which also keeps its reverse lookup.
    Refreshing,
}

/// The marks of a new sort result: those of `parent` that a sort copies, the first sort
/// column, and what Java notes of the row redirection. A sort by an absolute value has no
/// first column and keeps only what Java's three operations for it keep: the restriction
/// of the sorting.
///
/// The redirected column Java names is the last column that is read through the
/// redirection, which here is the last column: Java leaves a column whose source has one
/// value for every row (`RowKeyAgnosticChunkSource`) as it is, and no source says so here.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#populateRedirectedColumns,
/// #setSorted and #setReverseLookup
fn sorted_marks(
    parent: &dyn Table,
    first: Option<(&str, bool)>,
    redirection: Redirection,
) -> Marks {
    let copied = CopyType::Sort.sort_marks(&marks(parent));
    let Some((column, descending)) = first else {
        return Marks {
            sortable: copied.sortable,
            ..Marks::default()
        };
    };
    let mut marks = copied;
    if !matches!(redirection, Redirection::Unnoted) {
        let last = parent.definition().columns().last();
        marks.sort_redirection =
            Some(last.map_or_else(|| IDENTITY_REDIRECTION.to_string(), |c| c.name.clone()));
        marks.reverse_lookup = matches!(redirection, Redirection::Refreshing);
    }
    marks.set_sorted(column, descending);
    marks
}

/// Refuses a sort by a column that the restriction of `parent` does not name.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#assertSortable and the message of
/// io.deephaven.engine.exceptions.NotSortableException, which names the refused columns
/// in the order of a `HashSet` of them
fn assert_sortable(parent: &dyn Table, columns: &[SortColumn]) -> Result<(), SortError> {
    let of_parent = marks(parent);
    let Some(sortable) = of_parent.sortable_columns() else {
        return Ok(());
    };
    let names: Vec<&str> = columns
        .iter()
        .map(|column| column.column.as_str())
        .collect();
    let refused: Vec<String> = java_hash_order(&names, false)
        .into_iter()
        .filter(|name| !sortable.contains(&name.as_str()))
        .collect();
    if refused.is_empty() {
        return Ok(());
    }
    let refused = refused.join(", ");
    Err(SortError::NotSortable(format!(
        "Sorting is not allowed on this table, but was attempted on: {refused} but was \
         attempted on: {refused}"
    )))
}

/// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#restrictSortTo and
/// io.deephaven.engine.table.impl.LiveAttributeMap#withAttributes, which returns the table
/// itself when it has the attribute with this value already
pub(super) fn restrict_sort_to<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
) -> Result<Arc<dyn Table>, SortError> {
    let definition = parent.definition();
    let missing: Vec<String> = columns
        .iter()
        .map(|column| column.as_ref())
        .filter(|column| definition.index_of(column).is_none())
        .map(str::to_string)
        .collect();
    if !missing.is_empty() {
        return Err(SortError::NoSuchColumn(missing));
    }
    let names: Vec<&str> = columns.iter().map(|column| column.as_ref()).collect();
    let sortable = names.join(",");
    if marks(&**parent).sortable.as_deref() == Some(sortable.as_str()) {
        return Ok(parent.clone());
    }
    Ok(copy_of(parent, false, |marks| {
        marks.sortable = Some(sortable)
    })?)
}

/// A sort by one column that Java holds `parent` sorted by is `parent` itself. A sort of a
/// copy is answered with a copy: see [`memoized`]. A sort with a column sorted by its
/// absolute value is neither: Java's engine has no such sort. Its server makes one of
/// three operations, and so does the server here, which never asks for this one.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#sort and
/// io.deephaven.engine.table.impl.SortOperation#initialize
pub(super) fn sort(
    parent: &Arc<dyn Table>,
    columns: &[SortColumn],
) -> Result<Arc<dyn Table>, SortError> {
    if columns.is_empty() {
        return Ok(parent.clone());
    }
    let first = first_column(columns);
    if let (Some((column, descending)), 1) = (first, columns.len()) {
        if marks(&**parent).is_sorted_by(column, descending) {
            return Ok(parent.clone());
        }
    }
    let encoder = KeyEncoder::new(&**parent, columns)?;
    assert_sortable(&**parent, columns)?;
    let input = carried_input_table(&**parent, columns);
    let result = if !parent.is_refreshing() {
        static_sort(parent, &encoder, input, first)?
    } else if parent.attributes().blink {
        blink_sort(parent, encoder, input, first)?
    } else {
        incremental_sort(parent, encoder, input, first)?
    };
    if first.is_none() {
        return Ok(result);
    }
    // Behavior: the `attributesCompatible` of the `Sort` key of
    // io.deephaven.engine.table.impl.MemoizedOperationKey: a copy whose restriction of
    // its sorting is not that of the table it is a copy of is sorted as any other table
    let of_parent = marks(&**parent);
    if of_parent.copy_of_blink.is_some() && of_parent.copy_of_sortable != of_parent.sortable {
        return Ok(result);
    }
    Ok(memoized(parent, CopyType::Sort, false, result)?)
}

/// A table that is already in order is answered with a copy of it that is marked as sorted
/// by the first sort column, or with the table itself if it has those marks already.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#historicalSort and #withSorted,
/// and io.deephaven.engine.table.impl.LiveAttributeMap#withAttributes, which returns the
/// table itself when it has the attributes already
fn static_sort(
    parent: &Arc<dyn Table>,
    encoder: &KeyEncoder,
    input: Option<Arc<dyn InputTable>>,
    first: Option<(&str, bool)>,
) -> Result<Arc<dyn Table>, TableFailure> {
    if parent.is_failed() {
        return Err(failure("the parent table has failed"));
    }
    let rows = parent.row_set();
    let entries = sorted_entries(encoder, &**parent, &rows)?;
    // A table that is already in order keeps its own row keys, unless the result is to
    // lose the parent's input table.
    let keeps_input = input.is_some() == parent.input_table().is_some();
    if keeps_input && entries.iter().map(|entry| entry.parent_key).eq(rows.iter()) {
        let Some((column, descending)) = first else {
            return Ok(parent.clone());
        };
        let of_parent = marks(&**parent);
        if of_parent.is_sorted_by(column, descending)
            && of_parent.sort_redirection.as_deref() == Some(IDENTITY_REDIRECTION)
        {
            return Ok(parent.clone());
        }
        return copy_of(parent, false, |marks| {
            marks.set_sorted(column, descending);
            marks.sort_redirection = Some(IDENTITY_REDIRECTION.to_string());
        });
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
    let result = QueryTable::new_static(TableSpec { input, ..spec });
    mark(&result, sorted_marks(&**parent, first, Redirection::Static));
    Ok(result)
}

/// The sort of a blink table: every cycle the result is the sorted rows of that cycle.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#blinkTableSort
fn blink_sort(
    parent: &Arc<dyn Table>,
    encoder: KeyEncoder,
    input: Option<Arc<dyn InputTable>>,
    first: Option<(&str, bool)>,
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
    mark(
        &result,
        sorted_marks(&**parent, first, Redirection::Unnoted),
    );
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
    first: Option<(&str, bool)>,
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
    mark(
        &result,
        sorted_marks(&**parent, first, Redirection::Refreshing),
    );
    Ok(result)
}
