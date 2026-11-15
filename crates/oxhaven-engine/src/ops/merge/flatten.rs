//! `flatten`, and the positional key space it shares with `reverse` and `sort`.
//!
//! A positional result addresses its rows by position: row key `p` is the row at position
//! `p` of some sequence of parent rows. Rows that keep their place in the sequence across an
//! update are shifted; the others are removed and added.

use crate::table::{
    copy_of, mark, marks, memoized, reads_through, CopyType, QueryTable, TableSpec,
};
use crate::update::ModifiedColumnTransformer;
use oxhaven_column::{RedirectedSource, RowRedirection};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::{Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// The shifts of a positional table of `prev_size` rows from which the rows at `removed`
/// (previous positions) leave and into which rows at `added` (new positions) arrive: every
/// other row moves from its previous position to its new one.
///
/// Java walks the removed and added ranges and also states shifts across removed
/// positions; this pairs the surviving positions instead, which moves every surviving row
/// identically (shift ranges are not compared, `PARITY.md` A4).
///
/// Behavior: io.deephaven.engine.table.impl.FlattenOperation#computeFlattenedRowSetShiftData
pub(crate) fn flattened_shift_data(
    removed: &RowSet,
    added: &RowSet,
    prev_size: u64,
) -> Result<RowSetShiftData, TableFailure> {
    if removed.is_empty() && added.is_empty() {
        return Ok(RowSetShiftData::empty());
    }
    let new_size = prev_size - removed.size() + added.size();
    let before = RowSet::flat(prev_size).minus(removed);
    let after = RowSet::flat(new_size).minus(added);
    let (from, to) = (before.ranges(), after.ranges());
    let mut shifts = Vec::new();
    let (mut i, mut j) = (0, 0);
    // How much of `from[i]` and `to[j]` earlier pairs have used up.
    let (mut used_from, mut used_to) = (0, 0);
    while i < from.len() && j < to.len() {
        let first = from[i].first() + used_from;
        let target = to[j].first() + used_to;
        let length = (from[i].last() - first).min(to[j].last() - target) + 1;
        shifts.push(ShiftRange {
            first,
            last: first + length - 1,
            delta: target - first,
        });
        used_from += length;
        used_to += length;
        if first + length > from[i].last() {
            i += 1;
            used_from = 0;
        }
        if target + length > to[j].last() {
            j += 1;
            used_to = 0;
        }
    }
    RowSetShiftData::new(shifts).map_err(failure)
}

/// The keys of `rows` at `positions`, in the order given; `NULL_ROW_KEY` for a position
/// outside `0..rows.size()`. With `reversed`, position 0 is the last key.
fn keys_at_positions(rows: &RowSet, positions: &[RowKey], reversed: bool) -> Vec<RowKey> {
    let ranges = rows.ranges();
    // `ends[i]` is the number of keys in `ranges[..=i]`.
    let ends: Vec<u64> = ranges
        .iter()
        .scan(0u64, |total, range| {
            *total += range.size();
            Some(*total)
        })
        .collect();
    let size = ends.last().copied().unwrap_or(0);
    positions
        .iter()
        .map(|&position| {
            if position < 0 || position as u64 >= size {
                return NULL_ROW_KEY;
            }
            let position = if reversed {
                size - 1 - position as u64
            } else {
                position as u64
            };
            let i = ends.partition_point(|&end| end <= position);
            let before = if i == 0 { 0 } else { ends[i - 1] };
            ranges[i].first() + (position - before) as RowKey
        })
        .collect()
}

/// Maps position `p` to the parent's row key at position `p`, or with `reversed` at
/// position `size - 1 - p`. A previous-value read uses the parent's previous row set.
///
/// Behavior: io.deephaven.engine.table.impl.util.WrappedRowSetRowRedirection
struct PositionRedirection {
    parent: Arc<dyn Table>,
    reversed: bool,
}

impl RowRedirection for PositionRedirection {
    fn redirect(&self, keys: &[RowKey], use_prev: bool) -> Vec<RowKey> {
        let rows = if use_prev {
            self.parent.prev_row_set()
        } else {
            self.parent.row_set()
        };
        keys_at_positions(&rows, keys, self.reversed)
    }
}

/// The positions `size - 1 - p` for each position `p` of `positions`.
fn mirror(positions: &RowSet, size: u64) -> RowSet {
    let last = size as RowKey - 1;
    RowSet::from_ranges(
        positions
            .ranges()
            .iter()
            .map(|range| RowRange::new(last - range.last(), last - range.first())),
    )
}

/// A table of the parent's rows addressed by position, in the parent's order or reversed,
/// kept up to date with shifts for the rows that stay.
///
/// Behavior: io.deephaven.engine.table.impl.FlattenOperation#initialize and #onUpdate. The
/// reversed form gives the row order of
/// io.deephaven.engine.table.impl.ReverseOperation with this key space instead of Java's
/// pivot; it is for a refreshing table, and [`pivoted`] is for a static one.
pub(crate) fn positional(
    parent: &Arc<dyn Table>,
    reversed: bool,
    attributes: TableAttributes,
    flat: bool,
) -> Result<Arc<QueryTable>, TableFailure> {
    // Behavior: BaseTable#attributeToCopySet lists `INPUT_TABLE_ATTRIBUTE` for
    // `CopyAttributeOperation.Flatten` and not for `.Reverse`.
    let keeps_input_table = !reversed;
    QueryTable::derive(parent, || {
        let definition = parent.definition().clone();
        let redirection: Arc<dyn RowRedirection> = Arc::new(PositionRedirection {
            parent: parent.clone(),
            reversed,
        });
        let columns = (0..definition.columns().len())
            .map(|index| {
                reads_through(RedirectedSource::new(
                    redirection.clone(),
                    parent.column(index),
                ))
            })
            .collect();
        let transformer = ModifiedColumnTransformer::identity(definition.columns().len());
        let mut spec = TableSpec::new(definition, RowSet::flat(parent.row_set().size()), columns)
            .with_attributes(attributes);
        spec.flat = flat;
        if keeps_input_table {
            spec = spec.with_input_table_of(&**parent);
        }

        let parent = parent.clone();
        let operator = move |_result: &QueryTable, upstream: &TableUpdate| {
            let current = parent.row_set();
            let previous = parent.prev_row_set();
            let (prev_size, size) = (previous.size(), current.size());
            let place = |positions: RowSet, size: u64| {
                if reversed {
                    mirror(&positions, size)
                } else {
                    positions
                }
            };
            let modified = place(current.invert(&upstream.modified).map_err(failure)?, size);
            let added = place(current.invert(&upstream.added).map_err(failure)?, size);
            let removed = place(
                previous.invert(&upstream.removed).map_err(failure)?,
                prev_size,
            );
            // Upstream shifts move no row to another position.
            let shifted = flattened_shift_data(&removed, &added, prev_size)?;
            Ok(TableUpdate {
                added,
                removed,
                modified,
                shifted,
                modified_columns: transformer.transform(&upstream.modified_columns),
            })
        };
        Ok((spec, operator))
    })
}

/// The row key that the reverse of a table whose last row key is `last` counts back from:
/// the row at key `k` is at key `pivot - k` of the reverse.
///
/// Behavior: io.deephaven.engine.table.impl.ReverseOperation#computePivot
fn pivot(last: RowKey) -> RowKey {
    // Behavior: `MINIMUM_PIVOT`, the block size of a row set, and `PIVOT_GROWTH_FACTOR`
    const MINIMUM: RowKey = 1 << 16;
    const GROWTH: RowKey = 4;
    // Behavior: java.lang.Long#highestOneBit, 0 for 0 and for the -1 of an empty table
    let highest = match last {
        RowKey::MIN..=0 => 0,
        _ => 1 << (63 - last.leading_zeros()),
    };
    if highest > RowKey::MAX / GROWTH {
        RowKey::MAX
    } else {
        (highest * GROWTH - 1).max(MINIMUM)
    }
}

/// Maps a row key of the reverse of a table to the table's: both count from the pivot.
///
/// Behavior: io.deephaven.engine.table.impl.ReverseOperation#transform(long)
struct PivotRedirection(RowKey);

impl RowRedirection for PivotRedirection {
    fn redirect(&self, keys: &[RowKey], _use_prev: bool) -> Vec<RowKey> {
        let mirrored = |&key: &RowKey| if key < 0 { key } else { self.0 - key };
        keys.iter().map(mirrored).collect()
    }
}

/// The rows of a static `parent`, last row first, at the row keys Java gives them. The
/// keys are not otherwise compared (`PARITY.md` A4), but whether they are `0..size`
/// decides what a later `flatten`, `select` or `update` answers, and they never are.
///
/// Behavior: io.deephaven.engine.table.impl.ReverseOperation#initialize
pub(crate) fn pivoted(parent: &Arc<dyn Table>, attributes: TableAttributes) -> Arc<QueryTable> {
    let definition = parent.definition().clone();
    let rows = parent.row_set();
    let pivot = pivot(rows.last_row_key().unwrap_or(-1));
    let mirrored = RowSet::from_ranges(
        rows.ranges()
            .iter()
            .rev()
            .map(|range| RowRange::new(pivot - range.last(), pivot - range.first())),
    );
    let redirection: Arc<dyn RowRedirection> = Arc::new(PivotRedirection(pivot));
    let columns = (0..definition.columns().len())
        .map(|index| {
            reads_through(RedirectedSource::new(
                redirection.clone(),
                parent.column(index),
            ))
        })
        .collect();
    QueryTable::new_static(
        TableSpec::new(definition, mirrored, columns).with_attributes(attributes),
    )
}

/// The rows of `parent` in the same order at the row keys `0..size`.
///
/// A table that is already flat is returned as it is. A static table whose row set is
/// flat is answered with a copy of it that is flat. Otherwise the result keeps the
/// append-only and blink attributes of its parent, and is sorted by the columns its parent
/// is; asked of a copy, the answer is then a copy: see [`memoized`].
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#flatten,
/// io.deephaven.engine.table.impl.FlattenOperation and the `Flatten` entries of the
/// attribute table in io.deephaven.engine.table.impl.BaseTable
pub fn flatten(parent: &Arc<dyn Table>) -> Result<Arc<dyn Table>, TableFailure> {
    if parent.is_flat() {
        return Ok(parent.clone());
    }
    let rows = parent.row_set();
    if !parent.is_refreshing() && *rows == RowSet::flat(rows.size()) {
        // Already flat and never changing: a copy of the table, marked flat. Java does
        // not memoize this one, so it is the answer for a copy too.
        return copy_of(parent, true, |_| ());
    }
    let inherited = parent.attributes();
    let mut attributes = TableAttributes::default();
    attributes.append_only = inherited.append_only;
    attributes.blink = inherited.blink;
    let result = positional(parent, false, attributes, true)?;
    mark(&result, CopyType::Flatten.sort_marks(&marks(&**parent)));
    memoized(parent, CopyType::Flatten, false, result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shifts(data: &RowSetShiftData) -> Vec<(RowKey, RowKey, i64)> {
        data.ranges()
            .iter()
            .map(|s| (s.first, s.last, s.delta))
            .collect()
    }

    #[test]
    fn shift_data_moves_each_surviving_position_to_its_new_position() {
        // Ten rows; 2 and 3 leave, rows arrive at new positions 0 and 5.
        let removed = RowSet::from_keys([2, 3]);
        let added = RowSet::from_keys([0, 5]);
        let data = flattened_shift_data(&removed, &added, 10).unwrap();
        // 0,1 -> 1,2; 4,5 -> 3,4; 6..=9 stay at 6..=9.
        assert_eq!(shifts(&data), [(0, 1, 1), (4, 5, -1)]);
        let survivors = RowSet::flat(10).minus(&removed);
        assert_eq!(
            data.apply(&survivors).unwrap(),
            RowSet::flat(10).minus(&added)
        );
    }

    #[test]
    fn the_pivot_of_a_reverse_is_javas() {
        // What `ReverseOperation#computePivot` returns for the same last row keys.
        assert_eq!(pivot(-1), 65536);
        assert_eq!(pivot(0), 65536);
        assert_eq!(pivot(16384), 65536);
        assert_eq!(pivot(32767), 65536);
        assert_eq!(pivot(32768), 131071);
        assert_eq!(pivot(65536), 262143);
        assert_eq!(pivot(1 << 60), (1 << 62) - 1);
        assert_eq!(pivot(1 << 61), RowKey::MAX);
        assert_eq!(pivot(RowKey::MAX), RowKey::MAX);
    }

    #[test]
    fn shift_data_is_empty_when_nothing_moves() {
        let none = RowSet::empty();
        assert!(flattened_shift_data(&none, &none, 5).unwrap().is_empty());
        // A row replaced in place, and rows appended or removed at the end.
        let one = RowSet::from_keys([3]);
        assert!(flattened_shift_data(&one, &one, 5).unwrap().is_empty());
        let appended = RowSet::from_keys([5, 6]);
        assert!(flattened_shift_data(&none, &appended, 5)
            .unwrap()
            .is_empty());
        let last = RowSet::from_keys([4]);
        assert!(flattened_shift_data(&last, &none, 5).unwrap().is_empty());
    }

    #[test]
    fn shift_data_handles_everything_removed_or_nothing_there() {
        let all = RowSet::flat(4);
        let added = RowSet::flat(2);
        assert!(flattened_shift_data(&all, &added, 4).unwrap().is_empty());
        assert!(flattened_shift_data(&RowSet::empty(), &added, 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn keys_at_positions_reads_forward_and_reversed() {
        let rows = RowSet::from_keys([3, 4, 9, 20, 21]);
        assert_eq!(
            keys_at_positions(&rows, &[4, 0, 2, 5, NULL_ROW_KEY, 2], false),
            [21, 3, 9, NULL_ROW_KEY, NULL_ROW_KEY, 9]
        );
        assert_eq!(
            keys_at_positions(&rows, &[0, 1, 4, 7], true),
            [21, 20, 3, NULL_ROW_KEY]
        );
        assert_eq!(
            keys_at_positions(&RowSet::empty(), &[0], false),
            [NULL_ROW_KEY]
        );
    }

    #[test]
    fn mirror_reverses_positions() {
        assert_eq!(
            mirror(&RowSet::from_keys([0, 1, 5]), 7),
            RowSet::from_keys([1, 5, 6])
        );
    }
}
