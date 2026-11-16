//! `head`, `tail` and `slice`, and their forms that take a share of the table: the
//! parent's rows at a range of positions.
//!
//! The result is a sub-table: it has the parent's columns and a subset of its row keys.
//! On a blink table, whose rows last one cycle, `head` and `tail` instead collect rows as
//! they pass: the first rows ever seen, or the latest.

use super::SortError;
use crate::table::{memoized, tracked_source, CopyType, QueryTable, TableSpec};
use oxhaven_column::WritableSource;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_rowset::shift::intersect;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// Behavior: io.deephaven.engine.table.impl.BlinkTableTools#isBlink
fn is_blink(table: &dyn Table) -> bool {
    table.is_refreshing() && table.attributes().blink
}

fn parent_columns(parent: &Arc<dyn Table>) -> Vec<Arc<dyn ColumnSource>> {
    (0..parent.definition().columns().len())
        .map(|index| parent.column(index))
        .collect()
}

/// A static table with the parent's columns and no rows.
///
/// Behavior: `getSubTable(RowSetFactory.empty().toTracking())` in
/// io.deephaven.engine.table.impl.QueryTable#head, #tail and #slice
fn no_rows(parent: &Arc<dyn Table>) -> Arc<dyn Table> {
    QueryTable::new_static(
        TableSpec::new(
            parent.definition().clone(),
            RowSet::empty(),
            parent_columns(parent),
        )
        .keeping_partitioning(),
    )
}

/// The first `size` rows.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#head
pub(super) fn head(parent: &Arc<dyn Table>, size: i64) -> Result<Arc<dyn Table>, SortError> {
    if size == 0 {
        return Ok(no_rows(parent));
    }
    if size < 0 {
        return Err(SortError::IllegalArgument(format!(
            "size must be >= 0 (found: {size})"
        )));
    }
    if is_blink(&**parent) {
        // Behavior: the `BlinkToAppendOnly` key of
        // io.deephaven.engine.table.impl.MemoizedOperationKey, which a copy shares with
        // the table it is a copy of and whose copy type is `CopyAttributeOperation.None`:
        // `head` of a copy, such as a blink input table, is not add-only.
        let result = blink_to_append_only(parent, size as u64)?;
        return Ok(memoized(parent, CopyType::None, false, result)?);
    }
    Ok(slice_like(parent, Bounds::Positions(0, size))?)
}

/// The last `size` rows.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#tail
pub(super) fn tail(parent: &Arc<dyn Table>, size: i64) -> Result<Arc<dyn Table>, SortError> {
    if size == 0 {
        return Ok(no_rows(parent));
    }
    if size < 0 {
        return Err(SortError::IllegalArgument(format!(
            "size must be >= 0 (found: {size})"
        )));
    }
    if is_blink(&**parent) {
        // Behavior: `Math.toIntExact` in QueryTable#tail and the capacity limit of
        // io.deephaven.engine.table.impl.sources.ring.RingTableTools#of
        // (`ArrayUtil.MAX_ARRAY_SIZE`).
        const MAX_ARRAY_SIZE: i64 = i32::MAX as i64 - 8;
        if size > MAX_ARRAY_SIZE {
            return Err(SortError::IllegalArgument(format!(
                "a ring table cannot hold {size} rows"
            )));
        }
        return Ok(ring(parent, size as u64)?);
    }
    Ok(slice_like(parent, Bounds::Positions(-size, 0))?)
}

/// The rows at positions `first..last`. A negative position counts from the end, and a
/// `last` of zero with a negative `first` means the end.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#slice and
/// io.deephaven.engine.table.impl.SliceLikeOperation#slice
pub(super) fn slice(
    parent: &Arc<dyn Table>,
    first: i64,
    last: i64,
) -> Result<Arc<dyn Table>, SortError> {
    if is_blink(&**parent) {
        return Err(SortError::Unsupported(
            "Blink tables do not support slice; use BlinkTableTools.blinkToAppendOnly to \
             accumulate full history"
                .into(),
        ));
    }
    if first == last {
        return Ok(no_rows(parent));
    }
    if (first < 0 || last >= 0) && last < first {
        return Err(SortError::IllegalArgument(format!(
            "Can not slice with a first position ({first}) after last position ({last})"
        )));
    }
    Ok(slice_like(parent, Bounds::Positions(first, last))?)
}

/// Behavior: `QueryTable#unsupportedForBlinkTables`
fn refuse_blink(parent: &dyn Table, operation: &str) -> Result<(), SortError> {
    if is_blink(parent) {
        return Err(SortError::Unsupported(format!(
            "Blink tables do not support {operation}; use BlinkTableTools.blinkToAppendOnly \
             to accumulate full history"
        )));
    }
    Ok(())
}

/// Behavior: the range check of io.deephaven.engine.table.impl.QueryTable#headPct and
/// #tailPct, which a NaN passes
fn check_share(percent: f64) -> Result<(), SortError> {
    if !(0.0..=1.0).contains(&percent) && !percent.is_nan() {
        return Err(SortError::IllegalArgument(format!(
            "percentage of rows must be between [0,1]: percent={}",
            java_double(percent)
        )));
    }
    Ok(())
}

/// A double as Java prints it, for the values a refused share can have.
///
/// Behavior: `java.lang.Double#toString`
fn java_double(value: f64) -> String {
    if value.is_infinite() {
        let sign = if value < 0.0 { "-" } else { "" };
        return format!("{sign}Infinity");
    }
    if value.is_nan() {
        return "NaN".to_string();
    }
    let magnitude = value.abs();
    if magnitude != 0.0 && !(1e-3..1e7).contains(&magnitude) {
        let text = format!("{value:E}");
        return match text.split_once('E') {
            Some((digits, exponent)) if !digits.contains('.') => format!("{digits}.0E{exponent}"),
            _ => text,
        };
    }
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        format!("{value}")
    }
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#headPct
pub(super) fn head_pct(parent: &Arc<dyn Table>, percent: f64) -> Result<Arc<dyn Table>, SortError> {
    refuse_blink(&**parent, "headPct")?;
    check_share(percent)?;
    Ok(slice_like(parent, Bounds::HeadShare(percent))?)
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#tailPct
pub(super) fn tail_pct(parent: &Arc<dyn Table>, percent: f64) -> Result<Arc<dyn Table>, SortError> {
    refuse_blink(&**parent, "tailPct")?;
    check_share(percent)?;
    Ok(slice_like(parent, Bounds::TailShare(percent))?)
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#slicePct and
/// io.deephaven.engine.table.impl.SliceLikeOperation#slicePct
pub(super) fn slice_pct(
    parent: &Arc<dyn Table>,
    start: f64,
    end: f64,
) -> Result<Arc<dyn Table>, SortError> {
    refuse_blink(&**parent, "slicePct")?;
    // a NaN passes Java's check
    let outside = |share: f64| !(0.0..=1.0).contains(&share) && !share.is_nan();
    if outside(start) || outside(end) {
        return Err(SortError::IllegalArgument(format!(
            "Cannot slice with start percentage ({}) and end percentage ({}), percentages \
             must be between [0, 1]",
            java_double(start),
            java_double(end)
        )));
    }
    Ok(slice_like(parent, Bounds::Share(start, end))?)
}

/// Which rows a slice selects: a range of positions, or a share of the table, which moves
/// as the table grows and shrinks.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Bounds {
    /// `first..last`, a negative position counted from the end.
    Positions(i64, i64),
    /// The first rows: the share of the size, rounded up.
    HeadShare(f64),
    /// The last rows: the share of the size, rounded up.
    TailShare(f64),
    /// From one share of the size to another, each rounded down.
    Share(f64, f64),
}

impl Bounds {
    /// The first and last position for a table of `size` rows, as `slice` takes them.
    /// A product that is NaN is position 0, as Java's cast to `long` makes it.
    ///
    /// Behavior: `getFirstPositionInclusive` and `getLastPositionExclusive` of the
    /// operations io.deephaven.engine.table.impl.SliceLikeOperation#slice, #headPct,
    /// #tailPct and #slicePct make
    fn positions(self, size: u64) -> (i64, i64) {
        let size = size as f64;
        match self {
            Bounds::Positions(first, last) => (first, last),
            Bounds::HeadShare(share) => (0, (share * size).ceil() as i64),
            Bounds::TailShare(share) => (-((share * size).ceil() as i64), 0),
            Bounds::Share(start, end) => {
                ((start * size).floor() as i64, (end * size).floor() as i64)
            }
        }
    }

    /// Whether the result of slicing a flat table is flat.
    ///
    /// Behavior: the `mayBeFlat` each operation is made with
    fn may_be_flat(self) -> bool {
        match self {
            Bounds::Positions(first, _) => first == 0,
            Bounds::HeadShare(_) => true,
            Bounds::TailShare(_) => false,
            Bounds::Share(start, _) => start == 0.0,
        }
    }
}

/// The rows of `rows` that a slice selects.
///
/// Behavior: io.deephaven.engine.table.impl.SliceLikeOperation#computeSliceRowSet
fn slice_of(rows: &RowSet, first: i64, last: i64) -> RowSet {
    let size = rows.size() as i64;
    let (mut start, mut end) = (first, last);
    if start < 0 {
        if end == 0 {
            // A tail.
            end = size;
        }
        start = (start + size).max(0);
    }
    if end < 0 {
        end = (end + size).max(0);
    }
    // A first position after the last position selects nothing.
    end = end.max(start);
    rows.sub_set_by_position_range(start as u64, end as u64)
}

/// Behavior: the attributes set by io.deephaven.engine.table.impl.SliceLikeOperation#initialize
/// with io.deephaven.engine.table.impl.BaseTable#isAddOnly and #isAppendOnly
///
/// The first rows of a share keep both attributes of an append-only parent, and the last
/// rows of a share keep neither. A share from one point to another is treated as a slice
/// at the positions it has when the operation is made: so it is append-only when its
/// parent is, though rows leave it as the parent grows, and add-only when its parent is
/// and its end is at position 0 then.
fn slice_attributes(parent: &dyn Table, bounds: Bounds) -> TableAttributes {
    let inherited = parent.attributes();
    let parent_add_only = !parent.is_refreshing() || inherited.add_only || inherited.append_only;
    let parent_append_only = !parent.is_refreshing()
        || inherited.append_only
        || (inherited.add_only && parent.is_flat());
    let mut attributes = TableAttributes::default();
    let (first, last) = bounds.positions(parent.row_set().size());
    match bounds {
        Bounds::HeadShare(_) => {
            attributes.add_only = parent_append_only;
            attributes.append_only = parent_append_only;
        }
        Bounds::TailShare(_) => {}
        // With a first position counted from the end, rows leave as the parent grows.
        Bounds::Positions(..) | Bounds::Share(..) if first >= 0 => {
            // With a fixed last position, rows may have to leave when rows are added
            // before it.
            attributes.add_only = parent_add_only && last <= 0;
            attributes.append_only = parent_append_only;
        }
        Bounds::Positions(..) | Bounds::Share(..) => {}
    }
    attributes
}

/// Behavior: io.deephaven.engine.table.impl.SliceLikeOperation#initialize and #onUpdate
fn slice_like(parent: &Arc<dyn Table>, bounds: Bounds) -> Result<Arc<QueryTable>, TableFailure> {
    let selected = move |rows: &RowSet| {
        let (first, last) = bounds.positions(rows.size());
        slice_of(rows, first, last)
    };
    QueryTable::derive(parent, || {
        let mut spec = TableSpec::new(
            parent.definition().clone(),
            selected(&parent.row_set()),
            parent_columns(parent),
        )
        .keeping_partitioning()
        .with_attributes(slice_attributes(&**parent, bounds));
        spec.flat = parent.is_flat() && bounds.may_be_flat();

        let parent = parent.clone();
        let operator = move |result: &QueryTable, upstream: &TableUpdate| {
            let wanted = selected(&parent.row_set());
            let mut rows = (*result.row_set()).clone();

            let mut removed = upstream.removed.intersect(&rows);
            rows.remove(&removed);

            let shifted = intersect(&upstream.shifted, &rows);
            rows = shifted.apply(&rows).map_err(failure)?;

            // Rows the parent kept that are no longer in the slice, stated like every
            // removed row in the key space before the shifts.
            let dropped = rows.minus(&wanted);
            rows.remove(&dropped);
            removed.insert(&shifted.inverse().apply(&dropped).map_err(failure)?);

            // Before the new rows join: an added row is never also modified.
            let modified = upstream.modified.intersect(&rows);
            let added = wanted.minus(&rows);
            let modified_columns = if modified.is_empty() {
                ModifiedColumnSet::empty()
            } else {
                upstream.modified_columns.clone()
            };
            Ok(TableUpdate {
                added,
                removed,
                modified,
                shifted,
                modified_columns,
            })
        };
        Ok((spec, operator))
    })
}

/// Copies `rows` of the parent into `columns` at `keys`, row for row in key order.
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

fn writable_columns(parent: &Arc<dyn Table>) -> Vec<Arc<WritableSource>> {
    let columns = parent.definition().columns().iter();
    match parent.update_graph() {
        Some(graph) => columns
            .map(|column| tracked_source(&graph, column.column_type.clone()))
            .collect(),
        None => columns
            .map(|column| Arc::new(WritableSource::new(column.column_type.clone())))
            .collect(),
    }
}

fn as_sources(columns: &[Arc<WritableSource>]) -> Vec<Arc<dyn ColumnSource>> {
    columns
        .iter()
        .map(|column| column.clone() as Arc<dyn ColumnSource>)
        .collect()
}

fn check_blink_update(upstream: &TableUpdate, message: &str) -> Result<(), TableFailure> {
    if upstream.modified.is_empty() && upstream.shifted.is_empty() {
        Ok(())
    } else {
        Err(failure(message))
    }
}

/// `head` of a blink table: an append-only table of the first `limit` rows the blink table
/// ever showed, starting with the rows it shows now.
///
/// Behavior: io.deephaven.engine.table.impl.BlinkTableTools#blinkToAppendOnly(Table, long)
fn blink_to_append_only(
    parent: &Arc<dyn Table>,
    limit: u64,
) -> Result<Arc<QueryTable>, TableFailure> {
    QueryTable::derive(parent, || {
        let columns = writable_columns(parent);
        let initial = parent.row_set().sub_set_by_position_range(0, limit);
        let rows = RowSet::flat(initial.size());
        copy_rows(&**parent, &initial, &columns, &rows)?;

        let mut attributes = TableAttributes::default();
        attributes.add_only = true;
        attributes.append_only = true;
        let spec = TableSpec::new(parent.definition().clone(), rows, as_sources(&columns))
            .keeping_partitioning()
            .with_attributes(attributes)
            .flat();

        let parent = parent.clone();
        let operator = move |result: &QueryTable, upstream: &TableUpdate| {
            check_blink_update(upstream, "Blink tables should not modify or shift!")?;
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

/// `tail` of a blink table: the latest `capacity` rows the blink table added, whether or
/// not it has removed them since. Row keys count the rows seen, so the oldest retained row
/// has the lowest key.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ring.RingTableTools#of(Table, int),
/// io.deephaven.engine.table.impl.sources.ring.AddsToRingsListener#onUpdate and the key
/// range of io.deephaven.engine.table.impl.sources.ring.AbstractRingChunkSource
fn ring(parent: &Arc<dyn Table>, capacity: u64) -> Result<Arc<QueryTable>, TableFailure> {
    /// The rows of `incoming` that a ring that has seen `seen` rows retains, and their keys.
    fn retained(incoming: &RowSet, seen: u64, capacity: u64) -> (RowSet, RowSet) {
        let size = incoming.size();
        let skipped = size.saturating_sub(capacity);
        let kept = incoming.sub_set_by_position_range(skipped, size);
        let keys = if kept.is_empty() {
            RowSet::empty()
        } else {
            RowSet::from_range((seen + skipped) as RowKey, (seen + size - 1) as RowKey)
        };
        (kept, keys)
    }

    QueryTable::derive(parent, || {
        let columns = writable_columns(parent);
        let initial = parent.row_set();
        let (kept, rows) = retained(&initial, 0, capacity);
        copy_rows(&**parent, &kept, &columns, &rows)?;
        let seen = AtomicU64::new(initial.size());
        let spec = TableSpec::new(parent.definition().clone(), rows, as_sources(&columns))
            .keeping_partitioning();

        let parent = parent.clone();
        let operator = move |result: &QueryTable, upstream: &TableUpdate| {
            check_blink_update(upstream, "Not expecting modifies or shifts")?;
            // Removes are ignored: the ring keeps rows the blink table has dropped.
            let mut downstream = TableUpdate::empty();
            if upstream.added.is_empty() {
                return Ok(downstream);
            }
            let before = seen.load(Ordering::Relaxed);
            let (kept, keys) = retained(&upstream.added, before, capacity);
            let after = before + upstream.added.size();
            seen.store(after, Ordering::Relaxed);

            let evicted = match after.checked_sub(capacity + 1) {
                Some(last) => result.row_set().sub_set_by_key_range(0, last as RowKey),
                None => RowSet::empty(),
            };
            for column in &columns {
                column.remove(&evicted);
            }
            copy_rows(&*parent, &kept, &columns, &keys)?;
            downstream.added = keys;
            downstream.removed = evicted;
            Ok(downstream)
        };
        Ok((spec, operator))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_of_selects_positions_from_either_end() {
        let rows = RowSet::from_keys([10, 11, 12, 20, 30, 31]);
        let keys = |first, last| slice_of(&rows, first, last).iter().collect::<Vec<_>>();
        // head and tail
        assert_eq!(keys(0, 2), [10, 11]);
        assert_eq!(keys(0, 99), [10, 11, 12, 20, 30, 31]);
        assert_eq!(keys(-2, 0), [30, 31]);
        assert_eq!(keys(-99, 0), [10, 11, 12, 20, 30, 31]);
        // slices
        assert_eq!(keys(1, 3), [11, 12]);
        assert_eq!(keys(2, -1), [12, 20, 30]);
        assert_eq!(keys(-3, -1), [20, 30]);
        assert_eq!(keys(-4, 4), [12, 20]);
        // a first position at or after the last selects nothing
        assert_eq!(keys(4, -4), [] as [RowKey; 0]);
        assert_eq!(keys(-1, 2), [] as [RowKey; 0]);
        assert_eq!(keys(7, 9), [] as [RowKey; 0]);
    }
}
