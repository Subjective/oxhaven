//! The result of a range join, short of a table: its columns in order, of which those of
//! the `group` aggregation are array-valued.
//!
//! The contracts have no array-valued column type, so [`GroupColumn`] is not a
//! `ColumnSource` and a result with one cannot be a table yet. It is what this module needs
//! of such a column, not a proposal for the contract: with a list type in `ColumnType`,
//! `GroupColumn` implements `ColumnSource` with [`GroupColumn::chunk`] as it is, and
//! [`Joined::into_table`] makes the table.

use super::ranges::{match_columns, ranges, Ranges};
use super::{RangeAggregation, RangeJoinError, RangeJoinMatch};
use crate::ops::join::JoinMatch;
use crate::table::{QueryTable, TableSpec};
use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::unported;
use std::sync::Arc;

/// A column of arrays: for each left row, the values of one right column over the row's
/// range.
///
/// Behavior: io.deephaven.engine.table.impl.sources.aggregate.RangeAggregateColumnSource
// Read only by tests until a table can hold the column: see the module documentation.
#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct GroupColumn {
    /// The right column.
    values: Arc<dyn ColumnSource>,
    ranges: Arc<Ranges>,
    /// The rows of the left table, which are the rows of the result.
    rows: Arc<RowSet>,
}

impl GroupColumn {
    /// The type of the elements, which is the type of the right column.
    pub(super) fn element_type(&self) -> &ColumnType {
        self.values.column_type()
    }

    /// The arrays at `rows`, in row-key order, as an Arrow list array whose items are of
    /// the element type's Arrow type in a nullable field named `""`, as Java sends them.
    /// The array of a row without a range is null; that of an empty range is empty.
    ///
    /// The tables are static, so these are also the previous values.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.aggregate.RangeAggregateColumnSourceInt#fillChunk
    /// and its siblings
    // Read only by tests until a table can hold the column: see the module documentation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let failure = |message: String| TableFailure { message };
        let mut keys: Vec<RowKey> = Vec::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut valid: Vec<bool> = Vec::new();
        for key in rows.iter() {
            let row = self
                .rows
                .find(key)
                .ok_or_else(|| failure(format!("row key {key} is not in the table")))?;
            let cell = self.ranges.cells[row as usize];
            if let Some((bucket, first, after)) = cell {
                keys.extend_from_slice(&self.ranges.buckets[bucket][first..after]);
            }
            valid.push(cell.is_some());
            let end = i32::try_from(keys.len())
                .map_err(|_| failure("more array elements than a list array holds".into()))?;
            offsets.push(end);
        }
        let elements = self.values.chunk_by_keys(&keys, false)?;
        let field = Field::new("", self.element_type().arrow_type(), true);
        let lists = ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::new(offsets.into()),
            elements,
            Some(NullBuffer::from(valid)),
        )
        .map_err(|error| failure(error.to_string()))?;
        Ok(Arc::new(lists))
    }
}

/// One column of the result.
pub(super) enum Column {
    /// The column of the left table at this index.
    Left(usize),
    Group(GroupColumn),
}

/// A range join that was not refused.
pub(super) struct Joined {
    pub(super) left: Arc<dyn Table>,
    /// The columns of the result, in order, by name.
    pub(super) columns: Vec<(String, Column)>,
}

/// Behavior: the constructor of io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation
/// for the refusals and their order, with
/// io.deephaven.engine.table.impl.rangejoin.SupportedRangeJoinAggregations#validate; its
/// `StaticRangeJoinPhase3#start` for the columns: the left table's, then one per pair of
/// the aggregations, put into a map in that order, so that a later column of a name takes
/// the place of the earlier one.
pub(super) fn join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    range: &RangeJoinMatch,
    aggregations: &[RangeAggregation],
) -> Result<Joined, RangeJoinError> {
    if left.is_refreshing() || right.is_refreshing() {
        return Err(RangeJoinError::Refreshing {
            left: left.is_refreshing(),
            right: right.is_refreshing(),
        });
    }
    let match_columns = match_columns(&**left, &**right, exact, range)?;
    let unsupported: Vec<String> = aggregations
        .iter()
        .filter_map(|aggregation| match aggregation {
            RangeAggregation::Group(_) => None,
            RangeAggregation::Other(description) => Some(description.clone()),
        })
        .collect();
    if !unsupported.is_empty() {
        return Err(RangeJoinError::UnsupportedAggregations(unsupported));
    }

    let ranges = Arc::new(ranges(&**left, &**right, &match_columns, range)?);

    let rows = left.row_set();
    let mut columns: Vec<(String, Column)> = left
        .definition()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.clone(), Column::Left(index)))
        .collect();
    let right_definition = right.definition();
    for aggregation in aggregations {
        let RangeAggregation::Group(pairs) = aggregation else {
            continue;
        };
        for pair in pairs {
            // Behavior: io.deephaven.engine.table.impl.QueryTable#getColumnSource
            let Some(input) = right_definition.index_of(&pair.input) else {
                let present = right_definition.columns().iter();
                return Err(RangeJoinError::NoSuchColumn {
                    missing: pair.input.clone(),
                    present: present.map(|column| column.name.clone()).collect(),
                });
            };
            let column = Column::Group(GroupColumn {
                values: right.column(input),
                ranges: ranges.clone(),
                rows: rows.clone(),
            });
            match columns.iter_mut().find(|(name, _)| *name == pair.output) {
                Some(existing) => existing.1 = column,
                None => columns.push((pair.output.clone(), column)),
            }
        }
    }
    Ok(Joined {
        left: left.clone(),
        columns,
    })
}

impl Joined {
    /// The result as a table: the left table's rows and none of its attributes.
    ///
    /// Behavior: `new QueryTable(leftTable.getRowSet(), resultColumnSources)` in
    /// `StaticRangeJoinPhase3#start` of
    /// io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation
    pub(super) fn into_table(self) -> Result<Arc<dyn Table>, RangeJoinError> {
        let mut definitions = Vec::with_capacity(self.columns.len());
        let mut sources = Vec::with_capacity(self.columns.len());
        for (name, column) in &self.columns {
            match column {
                Column::Left(index) => {
                    let source = self.left.column(*index);
                    definitions.push(ColumnDefinition::new(name, source.column_type().clone()));
                    sources.push(source);
                }
                Column::Group(group) => unported!(
                    "io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource#forRangeJoin",
                    "column {name} holds arrays of {:?}, and no column type is array-valued",
                    group.element_type()
                ),
            }
        }
        let definition = TableDefinition::new(definitions).map_err(|duplicate| TableFailure {
            message: duplicate.to_string(),
        })?;
        let spec = TableSpec::new(definition, (*self.left.row_set()).clone(), sources);
        Ok(QueryTable::new_static(spec))
    }
}
