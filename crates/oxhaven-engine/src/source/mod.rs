//! Tables whose data is stored outside the engine and read when it is asked for.
//!
//! A [`TableLocation`] is a fixed number of rows somewhere else, a Parquet file for example,
//! that can read a range of one column as an Arrow array. [`stored_table`] makes a static
//! table from a definition and a list of locations. The engine knows nothing of the format:
//! a format is a crate that implements the trait (`docs/STORAGE.md`).
//!
//! Each location that has rows is a *region* of the table, numbered in the order given, and
//! the row key of row `r` of region `n` is `n << 43 | r`, as in Java. So a row key says
//! where its row is stored, and reading a column at some row keys is reading ranges of
//! locations.

#[cfg(test)]
mod tests;

use crate::table::{reads_through, QueryTable, TableSpec};
use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::compute::concat;
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, TableAttributes, TableFailure};
use std::ops::Range;
use std::sync::Arc;

/// The bits of a row key that say which row of its region it is.
///
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource#SUB_REGION_ROW_INDEX_ADDRESS_BITS
const ROW_BITS: u32 = 43;

/// The most rows a location can have.
///
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource#REGION_CAPACITY_IN_ELEMENTS
pub const MAX_LOCATION_ROWS: u64 = 1 << ROW_BITS;

/// The most locations with rows a table can have.
///
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource#MAXIMUM_REGION_COUNT
pub const MAX_REGIONS: usize = 1 << 20;

/// Rows stored outside the engine: one file, for example. Its size and its values never
/// change.
///
/// No single Java counterpart: it is what io.deephaven.engine.table.impl.locations.TableLocation,
/// `ColumnLocation` and the `ColumnRegion`s of `sources/regioned` offer a table together.
pub trait TableLocation: Send + Sync {
    /// What to call the location in an error: a file's path.
    fn name(&self) -> &str;

    /// The number of rows.
    fn size(&self) -> u64;

    /// The values of the rows `rows`, which lie in `0..size()`, of the column at `column`
    /// in the definition of the table the location was made for: an array of that column
    /// type's Arrow type with `rows.len()` elements, nulls as validity bits.
    ///
    /// An error fails the operation that asked.
    fn read(&self, column: usize, rows: Range<u64>) -> Result<ArrayRef, TableFailure>;
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The first row key of region `region`.
///
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource#getFirstRowKey
fn first_row_key(region: usize) -> RowKey {
    (region as RowKey) << ROW_BITS
}

/// A static table of the rows of `locations`, in that order, with the columns of
/// `definition`. Nothing is read until a column is.
///
/// A location without rows is not part of the table. The table is add-only, as Java says a
/// table of locations that do not change is. Java also marks a table without rows as an
/// empty source table, which is not a hint the engine has.
///
/// Behavior: io.deephaven.engine.table.impl.SourceTable#doCoalesce,
/// io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager#refresh (which
/// locations become regions, and their row keys) and `#getTableAttributes`
pub fn stored_table(
    definition: TableDefinition,
    locations: Vec<Arc<dyn TableLocation>>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let regions: Arc<[Arc<dyn TableLocation>]> = locations
        .into_iter()
        .filter(|location| location.size() > 0)
        .collect();
    if regions.len() > MAX_REGIONS {
        return Err(failure(format!(
            "a table can have {MAX_REGIONS} locations with rows, not {}",
            regions.len()
        )));
    }
    let mut ranges = Vec::with_capacity(regions.len());
    for (region, location) in regions.iter().enumerate() {
        let size = location.size();
        if size > MAX_LOCATION_ROWS {
            return Err(failure(format!(
                "{} has {size} rows; a location can have {MAX_LOCATION_ROWS}",
                location.name()
            )));
        }
        let first = first_row_key(region);
        ranges.push(RowRange::new(first, first + (size - 1) as RowKey));
    }
    let rows = RowSet::from_ranges(ranges);
    let columns = definition.columns().iter().enumerate();
    let sources = columns
        .map(|(column, column_definition)| {
            reads_through(RegionedSource {
                column_type: column_definition.column_type.clone(),
                column,
                regions: regions.clone(),
            })
        })
        .collect();
    let mut attributes = TableAttributes::default();
    attributes.add_only = true;
    let spec = TableSpec::new(definition, rows, sources).with_attributes(attributes);
    Ok(QueryTable::new_static(spec))
}

/// One column of a stored table: the same column of every region.
///
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceBase and the
/// `RegionedColumnSource*` of each type, whose `fillChunk` looks up the region of each row
/// key
struct RegionedSource {
    column_type: ColumnType,
    column: usize,
    regions: Arc<[Arc<dyn TableLocation>]>,
}

impl RegionedSource {
    /// The values of the row keys `first..=last`, which lie in one region.
    fn read(&self, first: RowKey, last: RowKey) -> Result<ArrayRef, TableFailure> {
        let region = (first >> ROW_BITS) as usize;
        let location = self
            .regions
            .get(region)
            .ok_or_else(|| failure(format!("row key {first} is in no location of the table")))?;
        let offset = (first - first_row_key(region)) as u64;
        let rows = offset..offset + (last - first) as u64 + 1;
        if rows.end > location.size() {
            return Err(failure(format!(
                "row key {last} is outside {}, which has {} rows",
                location.name(),
                location.size()
            )));
        }
        let size = (rows.end - rows.start) as usize;
        let values = location.read(self.column, rows)?;
        let expected = self.column_type.arrow_type();
        if values.data_type() != &expected || values.len() != size {
            return Err(failure(format!(
                "{} answered {} values of Arrow type {} for {size} rows of a column of type {:?}",
                location.name(),
                values.len(),
                values.data_type(),
                self.column_type
            )));
        }
        Ok(values)
    }
}

impl ColumnSource for RegionedSource {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let mut pieces = Vec::new();
        for range in rows.ranges() {
            if range.first() < 0 {
                return Err(failure(format!(
                    "row key {} is in no location of the table",
                    range.first()
                )));
            }
            // a range of row keys ends where its region does
            let mut first = range.first();
            loop {
                let region_last = first | (MAX_LOCATION_ROWS as RowKey - 1);
                let last = range.last().min(region_last);
                pieces.push(self.read(first, last)?);
                if last == range.last() {
                    break;
                }
                first = last + 1;
            }
        }
        match pieces.as_slice() {
            [] => Ok(new_empty_array(&self.column_type.arrow_type())),
            [one] => Ok(one.clone()),
            many => {
                let arrays: Vec<&dyn Array> = many.iter().map(|piece| piece.as_ref()).collect();
                concat(&arrays).map_err(|error| failure(error.to_string()))
            }
        }
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }
}
