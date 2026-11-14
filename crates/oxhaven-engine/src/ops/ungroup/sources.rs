//! The columns of an ungrouped table. Both kinds read the parent at the row a key's slot
//! belongs to.

use arrow::array::{Array, ArrayRef, ListArray, UInt64Array};
use oxhaven_contracts::graph::{ClockPhase, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet, NULL_ROW_KEY};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::{Arc, Mutex, PoisonError};

fn failure(error: impl ToString) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// The cells of a vector or array column as Arrow lists.
fn lists(values: &ArrayRef) -> Result<&ListArray, TableFailure> {
    let lists = values.as_any().downcast_ref::<ListArray>();
    lists.ok_or_else(|| {
        failure(format!(
            "a vector or array column answered with {} cells",
            values.data_type()
        ))
    })
}

/// The number of elements of cell `at`. A null cell has none.
fn size(lists: &ListArray, at: usize) -> u64 {
    if lists.is_null(at) {
        0
    } else {
        lists.value_length(at) as u64
    }
}

/// The arrays of a vector or array column of a table: its cells are Arrow lists
/// (`ColumnType::list_element`), a null cell being Java's `null` array, which has no
/// elements.
///
/// One implementation serves every vector and array type: the element type decides only
/// the type of the ungrouped column, where Java has a class for each.
///
/// Behavior: the sources that
/// io.deephaven.engine.table.impl.sources.UngroupedColumnSource#getColumnSource chooses
/// among
pub(super) struct Lists {
    column: Arc<dyn ColumnSource>,
    element: ColumnType,
}

impl Lists {
    /// The arrays of `column`, or `None` if its type is neither a vector nor an array.
    pub(super) fn of(column: Arc<dyn ColumnSource>) -> Option<Self> {
        let element = column.column_type().list_element()?.clone();
        Some(Self { column, element })
    }

    /// The type of the elements, which is the type of the ungrouped column.
    pub(super) fn element_type(&self) -> &ColumnType {
        &self.element
    }

    /// The number of elements of the array at each of `rows`, in row order, now or as of
    /// the previous cycle. A null array has none.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupSizeKernel.ArraySizeKernel#maxSize
    /// and VectorSizeKernel#maxSize
    pub(super) fn sizes(&self, rows: &RowSet, previous: bool) -> Result<Vec<u64>, TableFailure> {
        let values = if previous {
            self.column.prev_chunk(rows)?
        } else {
            self.column.chunk(rows)?
        };
        let lists = lists(&values)?;
        Ok((0..lists.len()).map(|at| size(lists, at)).collect())
    }

    /// For each `(row key, offset)` of `cells`, in that order, the element at the offset
    /// of the row's array, now or as of the previous cycle. Null for a null array and for
    /// an offset at or beyond the array's size, as an array of the element type.
    ///
    /// Behavior: the `get` and `getPrev` of the subclasses of
    /// io.deephaven.engine.table.impl.sources.UngroupedColumnSource
    pub(super) fn elements(
        &self,
        cells: &[(RowKey, u64)],
        previous: bool,
    ) -> Result<ArrayRef, TableFailure> {
        // Each run of cells of one row reads the row's array once.
        let mut rows: Vec<RowKey> = Vec::new();
        let mut cell_rows = Vec::with_capacity(cells.len());
        for (row, _) in cells {
            if rows.last() != Some(row) {
                rows.push(*row);
            }
            cell_rows.push(rows.len() - 1);
        }
        let values = self.column.chunk_by_keys(&rows, previous)?;
        let lists = lists(&values)?;
        let starts = lists.value_offsets();
        let places = cells.iter().zip(cell_rows).map(|((_, offset), at)| {
            (*offset < size(lists, at)).then(|| starts[at] as u64 + offset)
        });
        let places: UInt64Array = places.collect();
        arrow::compute::take(lists.values(), &places, None).map_err(failure)
    }
}

struct Base {
    current: u32,
    /// What `current` was before the cycle `changed`.
    previous: u32,
    /// The step of the last change.
    changed: Option<u64>,
}

/// The base of an ungrouped table, now and as of the previous cycle.
///
/// Behavior: io.deephaven.engine.table.impl.CrossJoinShiftState and the `base` and
/// `prevBase` of io.deephaven.engine.table.impl.sources.UngroupedColumnSource. Java
/// answers with the previous base until the next cycle starts; here the previous base is
/// the current one as soon as the graph is idle, as the `ColumnSource` contract has it.
pub(super) struct Bases {
    /// The graph of a refreshing table.
    graph: Option<Arc<dyn UpdateGraph>>,
    base: Mutex<Base>,
}

impl Bases {
    pub(super) fn new(graph: Option<Arc<dyn UpdateGraph>>, base: u32) -> Self {
        let base = Base {
            current: base,
            previous: base,
            changed: None,
        };
        Self {
            graph,
            base: Mutex::new(base),
        }
    }

    /// The base of a new table.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.UngroupedColumnSource#initializeBase
    pub(super) fn initialize(&self, new: u32) {
        let mut base = self.base.lock().unwrap_or_else(PoisonError::into_inner);
        base.current = new;
        base.previous = new;
        base.changed = None;
    }

    pub(super) fn current(&self) -> u32 {
        self.base
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .current
    }

    /// Behavior: io.deephaven.engine.table.impl.CrossJoinShiftState#getPrevNumShiftBits
    fn previous(&self) -> u32 {
        let base = self.base.lock().unwrap_or_else(PoisonError::into_inner);
        let clock = self.graph.as_ref().map(|graph| graph.clock());
        match clock {
            Some(clock)
                if clock.phase == ClockPhase::Updating && base.changed == Some(clock.step) =>
            {
                base.previous
            }
            _ => base.current,
        }
    }

    /// Changes the base in a cycle; the old one stays the previous base for the rest of
    /// the cycle.
    ///
    /// Behavior: io.deephaven.engine.table.impl.CrossJoinShiftState#setNumShiftBitsAndUpdatePrev
    pub(super) fn set(&self, new: u32) {
        let mut base = self.base.lock().unwrap_or_else(PoisonError::into_inner);
        let step = self.graph.as_ref().map(|graph| graph.clock().step);
        if base.changed != step {
            base.previous = base.current;
            base.changed = step;
        }
        base.current = new;
    }

    fn of(&self, previous: bool) -> u32 {
        if previous {
            self.previous()
        } else {
            self.current()
        }
    }
}

/// A column of the parent that is not ungrouped: every element of a row shows the row's
/// value.
///
/// Behavior: io.deephaven.engine.table.impl.sources.BitShiftingColumnSource
pub(super) struct Repeated {
    pub(super) inner: Arc<dyn ColumnSource>,
    pub(super) bases: Arc<Bases>,
}

impl ColumnSource for Repeated {
    fn column_type(&self) -> &ColumnType {
        self.inner.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let keys: Vec<RowKey> = rows.iter().collect();
        self.chunk_by_keys(&keys, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let keys: Vec<RowKey> = rows.iter().collect();
        self.chunk_by_keys(&keys, true)
    }

    fn chunk_by_keys(&self, keys: &[RowKey], use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let base = self.bases.of(use_prev);
        let rows = keys.iter().map(|&key| match key {
            NULL_ROW_KEY => NULL_ROW_KEY,
            key => key >> base,
        });
        let rows: Vec<RowKey> = rows.collect();
        self.inner.chunk_by_keys(&rows, use_prev)
    }
}

/// An ungrouped column: the elements of the parent's arrays.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UngroupedColumnSource
pub(super) struct Elements {
    pub(super) arrays: Arc<Lists>,
    pub(super) bases: Arc<Bases>,
}

impl Elements {
    fn read(&self, rows: &RowSet, previous: bool) -> Result<ArrayRef, TableFailure> {
        let base = self.bases.of(previous);
        let mask: RowKey = (1 << base) - 1;
        let cells = rows.iter().map(|key| (key >> base, (key & mask) as u64));
        let cells: Vec<(RowKey, u64)> = cells.collect();
        self.arrays.elements(&cells, previous)
    }
}

impl ColumnSource for Elements {
    fn column_type(&self) -> &ColumnType {
        self.arrays.element_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, true)
    }
}
