//! The columns of a columns-to-rows result. Each reads the parent at the row a result key
//! belongs to: the key shifted right by the layout's bits. The low bits are the slot, the
//! place of the row among the rows its parent row became.

use arrow::array::{new_empty_array, Array, ArrayRef, StringArray};
use arrow::compute::interleave;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::Arc;

fn failure(error: impl ToString) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// Where the rows of a result are: every parent row `k` becomes the rows
/// `k << bits | slot` for the slots `0..fanout`.
///
/// Behavior: the key arithmetic of io.deephaven.engine.table.impl.CrossJoinShiftState,
/// which io.deephaven.engine.table.impl.util.ColumnsToRowsTransform shares
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct Layout {
    /// The number of rows of a parent row: the number of labels, at least one.
    pub(super) fanout: usize,
    /// The bits of a slot: the fewest that hold `fanout - 1`.
    pub(super) bits: u32,
}

impl Layout {
    /// The layout for `fanout` labels, at least one.
    pub(super) fn new(fanout: usize) -> Self {
        assert!(fanout > 0, "a layout has at least one slot");
        let bits = usize::BITS - (fanout - 1).leading_zeros();
        Self { fanout, bits }
    }

    /// The parent row of a result row.
    pub(super) fn parent(&self, key: RowKey) -> RowKey {
        key >> self.bits
    }

    /// The slot of a result row.
    pub(super) fn slot(&self, key: RowKey) -> usize {
        (key & ((1 << self.bits) - 1)) as usize
    }

    /// The largest parent row key whose rows are all row keys.
    pub(super) fn last_parent(&self) -> RowKey {
        RowKey::MAX >> self.bits
    }

    /// The first result row of the parent row `key`, which is at most
    /// [`last_parent`](Self::last_parent).
    pub(super) fn first_row(&self, key: RowKey) -> RowKey {
        key << self.bits
    }

    /// The last row key of the slots of the parent row `key`, which is at most
    /// [`last_parent`](Self::last_parent).
    pub(super) fn last_row(&self, key: RowKey) -> RowKey {
        self.first_row(key) | ((1 << self.bits) - 1)
    }

    /// The result rows of the parent rows `rows`, which are at most
    /// [`last_parent`](Self::last_parent): every slot of each when `slots` is `None`, and
    /// the slots it marks otherwise.
    ///
    /// Behavior: the two `transformIndex` of
    /// io.deephaven.engine.table.impl.util.ColumnsToRowsTransform
    pub(super) fn expand(&self, rows: &RowSet, slots: Option<&[bool]>) -> RowSet {
        let all = vec![true; self.fanout];
        let slots = slots.unwrap_or(&all);
        // the runs of marked slots within a parent row, as offsets from its first row
        let mut runs: Vec<(RowKey, RowKey)> = Vec::new();
        for (slot, &marked) in slots.iter().enumerate() {
            let slot = slot as RowKey;
            match runs.last_mut() {
                Some((_, last)) if marked && *last + 1 == slot => *last = slot,
                _ if marked => runs.push((slot, slot)),
                _ => {}
            }
        }
        let dense = runs == [(0, (1 << self.bits) - 1)];
        let mut ranges = Vec::new();
        for range in rows.ranges() {
            if dense {
                // every row of every parent row: one range
                let first = self.first_row(range.first());
                let last = self.last_row(range.last());
                ranges.push(oxhaven_contracts::rowset::RowRange::new(first, last));
                continue;
            }
            for key in range.first()..=range.last() {
                let base = self.first_row(key);
                for (first, last) in &runs {
                    let range = oxhaven_contracts::rowset::RowRange::new(base + first, base + last);
                    ranges.push(range);
                }
            }
        }
        RowSet::from_ranges(ranges)
    }
}

/// The parent rows of `keys`, one for each key, in order.
fn parents(layout: Layout, keys: impl Iterator<Item = RowKey>) -> Vec<RowKey> {
    keys.map(|key| layout.parent(key)).collect()
}

/// A column of the parent that is not transposed: each of the rows of a parent row has
/// the parent row's value.
///
/// Behavior: io.deephaven.engine.table.impl.sources.BitShiftingColumnSource
pub(super) struct Expanded {
    pub(super) parent: Arc<dyn ColumnSource>,
    pub(super) layout: Layout,
}

impl ColumnSource for Expanded {
    fn column_type(&self) -> &ColumnType {
        self.parent.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let keys = parents(self.layout, rows.iter());
        self.parent.chunk_by_keys(&keys, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let keys = parents(self.layout, rows.iter());
        self.parent.chunk_by_keys(&keys, true)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let null = oxhaven_contracts::rowset::NULL_ROW_KEY;
        let keys: Vec<RowKey> = keys
            .iter()
            .map(|&key| {
                if key == null {
                    null
                } else {
                    self.layout.parent(key)
                }
            })
            .collect();
        self.parent.chunk_by_keys(&keys, use_prev)
    }
}

/// The label column: the label of a row's slot. It never changes.
///
/// Behavior: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform.LabelColumnSource
pub(super) struct Labels {
    pub(super) labels: Vec<String>,
    pub(super) layout: Layout,
}

impl Labels {
    fn of(&self, keys: impl Iterator<Item = RowKey>) -> ArrayRef {
        let null = oxhaven_contracts::rowset::NULL_ROW_KEY;
        let values =
            keys.map(|key| (key != null).then(|| self.labels[self.layout.slot(key)].as_str()));
        Arc::new(values.collect::<StringArray>())
    }
}

impl ColumnSource for Labels {
    fn column_type(&self) -> &ColumnType {
        &ColumnType::String
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        Ok(self.of(rows.iter()))
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        Ok(self.of(rows.iter()))
    }

    fn chunk_by_keys(&self, keys: &[RowKey], _use_prev: bool) -> Result<ArrayRef, TableFailure> {
        Ok(self.of(keys.iter().copied()))
    }
}

/// A value column: the row of slot `s` has the value of the `s`-th of its parent
/// columns at the parent row.
///
/// Behavior: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform.TransposedColumnSource
pub(super) struct Transposed {
    pub(super) column_type: ColumnType,
    /// One column of the parent for each slot.
    pub(super) parents: Vec<Arc<dyn ColumnSource>>,
    pub(super) layout: Layout,
}

impl Transposed {
    fn read(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let null = oxhaven_contracts::rowset::NULL_ROW_KEY;
        // the parent rows each slot is asked for, and where each key's value is
        let mut asked: Vec<Vec<RowKey>> = vec![Vec::new(); self.parents.len()];
        let mut places = Vec::with_capacity(keys.len());
        let mut nulls = false;
        for &key in keys {
            if key == null {
                nulls = true;
                places.push(None);
                continue;
            }
            let slot = self.layout.slot(key);
            places.push(Some((slot, asked[slot].len())));
            asked[slot].push(self.layout.parent(key));
        }
        let mut arrays = Vec::with_capacity(self.parents.len() + 1);
        for (parent, keys) in self.parents.iter().zip(&asked) {
            let values = if keys.is_empty() {
                new_empty_array(&self.column_type.arrow_type())
            } else {
                parent.chunk_by_keys(keys, use_prev)?
            };
            arrays.push(values);
        }
        if nulls {
            arrays.push(arrow::array::new_null_array(
                &self.column_type.arrow_type(),
                1,
            ));
        }
        let last = arrays.len() - 1;
        let indices: Vec<(usize, usize)> = places
            .into_iter()
            .map(|place| place.unwrap_or((last, 0)))
            .collect();
        let arrays: Vec<&dyn Array> = arrays.iter().map(AsRef::as_ref).collect();
        interleave(&arrays, &indices).map_err(failure)
    }
}

impl ColumnSource for Transposed {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let keys: Vec<RowKey> = rows.iter().collect();
        self.read(&keys, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let keys: Vec<RowKey> = rows.iter().collect();
        self.read(&keys, true)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        self.read(keys, use_prev)
    }
}
