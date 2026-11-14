//! The result of a range join: its columns in order, of which those of the `group`
//! aggregation are vector columns read through the ranges, and the table made of them.

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
use std::sync::Arc;

/// A column of vectors: for each left row, the values of one right column over the row's
/// range.
///
/// Behavior: io.deephaven.engine.table.impl.sources.aggregate.RangeAggregateColumnSource
struct GroupColumn {
    /// The right column.
    values: Arc<dyn ColumnSource>,
    /// The vector of the right column's type.
    column_type: ColumnType,
    ranges: Arc<Ranges>,
    /// The rows of the left table, which are the rows of the result.
    rows: Arc<RowSet>,
}

impl GroupColumn {
    /// The column type is the vector of the right column's type, whatever that type is:
    /// the vector of a primitive for a primitive, and an `ObjectVector` with the right
    /// column's class as its component type for any other, a vector among them (observed
    /// on Java 42.9 for every element type an uploaded table can have, and for the group
    /// column of another range join).
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource#forRangeJoin
    /// and the constructor of
    /// io.deephaven.engine.table.impl.sources.aggregate.BaseAggregateColumnSource
    fn new(values: Arc<dyn ColumnSource>, ranges: Arc<Ranges>, rows: Arc<RowSet>) -> Self {
        let column_type = ColumnType::vector(values.column_type().clone());
        Self {
            values,
            column_type,
            ranges,
            rows,
        }
    }
}

impl ColumnSource for GroupColumn {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    /// The vectors at `rows`, in row-key order. The vector of a row without a range is
    /// null; that of an empty range is empty.
    ///
    /// Behavior: io.deephaven.engine.table.impl.sources.aggregate.RangeAggregateColumnSourceInt#fillChunk
    /// and its siblings
    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
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
        let field = Field::new("", self.values.column_type().arrow_type(), true);
        let lists = ListArray::try_new(
            Arc::new(field),
            OffsetBuffer::new(offsets.into()),
            elements,
            Some(NullBuffer::from(valid)),
        )
        .map_err(|error| failure(error.to_string()))?;
        Ok(Arc::new(lists))
    }

    /// The tables are static, so the previous values are the current ones.
    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }
}

/// Behavior: the constructor of io.deephaven.engine.table.impl.rangejoin.RangeJoinOperation
/// for the refusals and their order, with
/// io.deephaven.engine.table.impl.rangejoin.SupportedRangeJoinAggregations#validate; its
/// `StaticRangeJoinPhase3#start` for the columns: the left table's, then one per pair of
/// the aggregations, put into a map in that order, so that a later column of a name takes
/// the place of the earlier one; and for the table,
/// `new QueryTable(leftTable.getRowSet(), resultColumnSources)`: the left table's rows and
/// none of its attributes.
pub(super) fn join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    range: &RangeJoinMatch,
    aggregations: &[RangeAggregation],
) -> Result<Arc<dyn Table>, RangeJoinError> {
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
    let mut columns: Vec<(String, Arc<dyn ColumnSource>)> = left
        .definition()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| (column.name.clone(), left.column(index)))
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
            let column: Arc<dyn ColumnSource> = Arc::new(GroupColumn::new(
                right.column(input),
                ranges.clone(),
                rows.clone(),
            ));
            match columns.iter_mut().find(|(name, _)| *name == pair.output) {
                Some(existing) => existing.1 = column,
                None => columns.push((pair.output.clone(), column)),
            }
        }
    }

    let definitions = columns
        .iter()
        .map(|(name, source)| ColumnDefinition::new(name, source.column_type().clone()))
        .collect();
    let definition = TableDefinition::new(definitions).map_err(|duplicate| TableFailure {
        message: duplicate.to_string(),
    })?;
    let sources = columns.into_iter().map(|(_, source)| source).collect();
    let spec = TableSpec::new(definition, (*rows).clone(), sources);
    Ok(QueryTable::new_static(spec))
}
