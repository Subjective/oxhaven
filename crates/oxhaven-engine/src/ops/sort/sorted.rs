//! `sort`: the parent's rows ordered by their sort keys, read through a row redirection.
//!
//! The result of a static or blink sort is positional: row key `p` is the row at position
//! `p` of the sorted order. The result of an incremental sort keeps gaps in its key space,
//! as Java's does, so that an insertion moves few rows: see [`super::gapped`].

use super::gapped::SortOperator;
use super::keys::{KeyEncoder, SortKeys};
use super::{SortColumn, SortDirection, SortError};
use crate::table::{
    copy_of, java_hash_order, mark, marks, memoized, reads_through, CopyType, Marks, QueryTable,
    TableSpec, IDENTITY_REDIRECTION,
};
use oxhaven_column::{Clock, RedirectedSource, RowRedirection, WritableRowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, InputTable, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::{Arc, Mutex, PoisonError};

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The parent keys of `rows` at the positions `order`.
fn parent_keys(rows: &RowSet, order: &[u32]) -> Vec<RowKey> {
    if let Some(range) = rows.as_single_range() {
        let first = range.first();
        return order.iter().map(|&p| first + RowKey::from(p)).collect();
    }
    let keys: Vec<RowKey> = rows.iter().collect();
    order.iter().map(|&p| keys[p as usize]).collect()
}

/// `rows` of the parent in sorted order: their parent keys, and with them their sort keys
/// in row-key order and the position of each sorted row among those. Rows with equal keys
/// keep the parent's order.
///
/// Behavior: io.deephaven.engine.table.impl.SortHelpers#getSortedKeys (a stable sort)
pub(super) fn sorted_keys(
    encoder: &KeyEncoder,
    parent: &dyn Table,
    rows: &RowSet,
) -> Result<(Vec<RowKey>, SortKeys, Vec<u32>), TableFailure> {
    let keys = encoder.encode(parent, rows, false)?;
    let order = keys.order();
    Ok((parent_keys(rows, &order), keys, order))
}

/// The rows of a sorted result: the parent key at each position, with its sort key.
struct Sorted {
    parents: Vec<RowKey>,
    keys: SortKeys,
}

impl Sorted {
    /// `rows` of the parent in sorted order, with their sort keys.
    fn of(encoder: &KeyEncoder, parent: &dyn Table, rows: &RowSet) -> Result<Self, TableFailure> {
        let (parents, unordered, order) = sorted_keys(encoder, parent, rows)?;
        let mut keys = encoder.empty(order.len(), &unordered);
        for &position in &order {
            keys.push_from(&unordered, position as usize);
        }
        Ok(Self { parents, keys })
    }
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
    Ok(sorted_keys(&encoder, parent, rows)?.0)
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
    let (parents, _, order) = sorted_keys(encoder, &**parent, &rows)?;
    // A table that is already in order keeps its own row keys, unless the result is to
    // lose the parent's input table.
    let keeps_input = input.is_some() == parent.input_table().is_some();
    if keeps_input && (0..).zip(&order).all(|(position, &p)| position == p) {
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
    let flat = RowSet::flat(parents.len() as u64);
    let redirection = Arc::new(WritableRowRedirection::from_rows(&flat, parents));
    let spec = TableSpec::new(
        parent.definition().clone(),
        flat,
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
        let (shown, _, _) = sorted_keys(&encoder, &**parent, &parent.row_set())?;
        let flat = RowSet::flat(shown.len() as u64);
        let redirection = Arc::new(WritableRowRedirection::from_rows(&flat, shown.clone()));
        if let Some(graph) = parent.update_graph() {
            let clock: Arc<dyn Clock> = Arc::new(graph);
            redirection.start_tracking_prev(clock);
        }
        let spec = TableSpec::new(
            parent.definition().clone(),
            flat,
            redirected_columns(parent, &redirection),
        )
        .with_attributes(sort_attributes(&**parent))
        .flat();
        let spec = TableSpec { input, ..spec };

        let parent = parent.clone();
        let shown = Mutex::new(shown);
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
            let (added, _, _) = sorted_keys(&encoder, &*parent, &upstream.added)?;
            remap(&redirection, &shown, added.iter().copied());
            let removed = RowSet::flat(shown.len() as u64);
            *shown = added;
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

/// Behavior: the refreshing branch of io.deephaven.engine.table.impl.SortOperation#initialize
fn incremental_sort(
    parent: &Arc<dyn Table>,
    encoder: KeyEncoder,
    input: Option<Arc<dyn InputTable>>,
    first: Option<(&str, bool)>,
) -> Result<Arc<dyn Table>, TableFailure> {
    let result = QueryTable::derive(parent, || {
        let sorted = Sorted::of(&encoder, &**parent, &parent.row_set())?;
        let (operator, rows, redirection) =
            SortOperator::new(parent, encoder, sorted.parents, sorted.keys);
        if let Some(graph) = parent.update_graph() {
            let clock: Arc<dyn Clock> = Arc::new(graph);
            redirection.start_tracking_prev(clock);
        }
        let definition = parent.definition().clone();
        // Not flat, as in Java: the row keys have gaps (see `gapped`).
        let spec = TableSpec::new(definition, rows, redirected_columns(parent, &redirection))
            .with_attributes(sort_attributes(&**parent));
        let spec = TableSpec { input, ..spec };
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
