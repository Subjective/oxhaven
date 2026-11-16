//! Tables whose data is stored outside the engine and read when it is asked for.
//!
//! A [`TableLocation`] is a fixed number of rows somewhere else, a Parquet file for example,
//! that can read a range of one column as an Arrow array. [`stored_table`] makes a static
//! table from a definition and a list of locations, and [`following_table`] a table that
//! grows by the locations a [`LocationProvider`] finds later. The engine knows nothing of
//! the format: a format is a crate that implements the traits (`docs/STORAGE.md`).
//!
//! Each location that has rows is a *region* of the table, numbered in the order given, and
//! the row key of row `r` of region `n` is `n << 43 | r`, as in Java. So a row key says
//! where its row is stored, and reading a column at some row keys is reading ranges of
//! locations.

#[cfg(test)]
mod tests;

use crate::table::{reads_through, QueryTable, RootSource, TableSpec};
use arrow::array::{new_empty_array, Array, ArrayRef};
use arrow::compute::concat;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::ops::Range;
use std::sync::{Arc, PoisonError, RwLock, Weak};

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

/// Where the locations of a table that grows come from.
///
/// Behavior: what io.deephaven.engine.table.impl.locations.impl.PollingTableLocationProvider
/// and the `TableLocationKeyFinder` behind it offer a source table
pub trait LocationProvider: Send + Sync {
    /// The locations found since the provider was made or last asked, in the order of
    /// their rows. Called once on each cycle of the update graph, under its exclusive
    /// lock, so a provider that looks at a file system decides itself how often it looks.
    ///
    /// An error fails the table.
    fn poll(&self) -> Result<Vec<Arc<dyn TableLocation>>, TableFailure>;
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The locations of a table that have rows, in the order of their regions. A table that
/// grows adds to the end.
#[derive(Default)]
struct Regions(RwLock<Vec<Arc<dyn TableLocation>>>);

impl Regions {
    fn get(&self, region: usize) -> Option<Arc<dyn TableLocation>> {
        let regions = self.0.read().unwrap_or_else(PoisonError::into_inner);
        regions.get(region).cloned()
    }

    /// Makes regions of those of `locations` that have rows, and answers their row keys.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager#refresh
    fn add(&self, locations: Vec<Arc<dyn TableLocation>>) -> Result<RowSet, TableFailure> {
        let mut regions = self.0.write().unwrap_or_else(PoisonError::into_inner);
        let locations: Vec<_> = locations.into_iter().filter(|l| l.size() > 0).collect();
        let count = regions.len() + locations.len();
        if count > MAX_REGIONS {
            return Err(failure(format!(
                "a table can have {MAX_REGIONS} locations with rows, not {count}"
            )));
        }
        let mut ranges = Vec::with_capacity(locations.len());
        for (index, location) in locations.iter().enumerate() {
            let size = location.size();
            if size > MAX_LOCATION_ROWS {
                return Err(failure(format!(
                    "{} has {size} rows; a location can have {MAX_LOCATION_ROWS}",
                    location.name()
                )));
            }
            let first = first_row_key(regions.len() + index);
            ranges.push(RowRange::new(first, first + (size - 1) as RowKey));
        }
        regions.extend(locations);
        Ok(RowSet::from_ranges(ranges))
    }
}

/// The table of the rows of `regions`, which are `rows`, before it is static or not.
fn spec(definition: TableDefinition, regions: &Arc<Regions>, rows: RowSet) -> TableSpec {
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
    TableSpec::new(definition, rows, sources).keeping_partitioning()
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
    let regions = Arc::new(Regions::default());
    let rows = regions.add(locations)?;
    let mut attributes = TableAttributes::default();
    attributes.add_only = true;
    let spec = spec(definition, &regions, rows).with_attributes(attributes);
    Ok(QueryTable::new_static(spec))
}

/// A table of the rows of `locations`, in that order, that grows on a cycle of `graph` by
/// the rows of the locations `provider` then answers, in the order it answers them.
///
/// A location never changes and never leaves, so the table only gains rows after the ones
/// it has: it is append-only. Java says so with that attribute alone, without the add-only
/// one a table of locations that do not change has.
///
/// Behavior: io.deephaven.engine.table.impl.SourceTable#doCoalesce and its
/// `LocationChangePoller`, io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager#refresh
/// and `#getTableAttributes`
pub fn following_table(
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    locations: Vec<Arc<dyn TableLocation>>,
    provider: Arc<dyn LocationProvider>,
) -> Result<Arc<QueryTable>, TableFailure> {
    let regions = Arc::new(Regions::default());
    let rows = regions.add(locations)?;
    let mut attributes = TableAttributes::default();
    attributes.append_only = true;
    let spec = spec(definition, &regions, rows).with_attributes(attributes);
    Ok(QueryTable::root(graph, spec, |table| RootSource {
        source: Arc::new(Follower {
            table,
            regions,
            provider,
        }),
        input: None,
    }))
}

/// Adds the locations a provider finds to a table.
struct Follower {
    table: Weak<QueryTable>,
    regions: Arc<Regions>,
    provider: Arc<dyn LocationProvider>,
}

impl UpdateSource for Follower {
    /// Behavior: io.deephaven.engine.table.impl.SourceTable.LocationChangePoller#instrumentedRefresh
    fn refresh(&self) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        if table.is_failed() {
            return;
        }
        let found = self.provider.poll();
        match found.and_then(|locations| self.regions.add(locations)) {
            Ok(added) if added.is_empty() => {}
            Ok(added) => table.notify(TableUpdate {
                added,
                ..TableUpdate::empty()
            }),
            Err(failure) => table.fail(failure),
        }
    }
}

/// One column of a stored table: the same column of every region.
///
/// Behavior: io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceBase and the
/// `RegionedColumnSource*` of each type, whose `fillChunk` looks up the region of each row
/// key
struct RegionedSource {
    column_type: ColumnType,
    column: usize,
    regions: Arc<Regions>,
}

impl RegionedSource {
    /// The values of the row keys `first..=last`, which lie in one region.
    fn read(&self, first: RowKey, last: RowKey) -> Result<ArrayRef, TableFailure> {
        let region = (first >> ROW_BITS) as usize;
        let location = self
            .regions
            .get(region)
            .ok_or_else(|| failure(format!("row key {first} is in no location of the table")))?;
        let location = location.as_ref();
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
