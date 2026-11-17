//! Static tables built from data in memory.

use super::{arrange_columns, failure, reads_through, QueryTable, TableSpec};
use arrow::array::{new_empty_array, ArrayRef};
use arrow::record_batch::RecordBatch;
use oxhaven_column::FlatSource;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, TableFailure};
use std::sync::Arc;

/// A static table holding the rows of `batches`, in order, at row keys `0..size`.
///
/// Every batch must have exactly the columns of `definition`, by name, with the Arrow type
/// of the column's type. The order of a batch's columns does not matter.
///
/// Behavior: io.deephaven.engine.table.impl.InMemoryTable#from
pub fn in_memory_table(
    definition: TableDefinition,
    batches: &[RecordBatch],
) -> Result<Arc<QueryTable>, TableFailure> {
    static_table(definition, batches, false)
}

/// As [`in_memory_table`], for the rows a client uploaded. Java keeps those behind a row
/// redirection, so its columns are not in-memory columns: `select` and `update` copy such
/// a column where they would share a column of an [`in_memory_table`], and so a static
/// `select` of rows of this table always has the row keys `0..size`.
///
/// Behavior: the table of io.deephaven.server.arrow.ArrowFlightUtil.DoPutObserver, an
/// io.deephaven.extensions.barrage.table.BarrageRedirectedTable whose columns are
/// io.deephaven.engine.table.impl.sources.WritableRedirectedColumnSource
pub fn uploaded_table(
    definition: TableDefinition,
    batches: &[RecordBatch],
) -> Result<Arc<QueryTable>, TableFailure> {
    static_table(definition, batches, true)
}

fn static_table(
    definition: TableDefinition,
    batches: &[RecordBatch],
    redirected: bool,
) -> Result<Arc<QueryTable>, TableFailure> {
    let declared: Vec<&ColumnDefinition> = definition.columns().iter().collect();
    let arranged = batches
        .iter()
        .map(|batch| arrange_columns(&declared, batch))
        .collect::<Result<Vec<_>, _>>()?;
    let size: usize = batches.iter().map(RecordBatch::num_rows).sum();

    let mut columns: Vec<Arc<dyn ColumnSource>> = Vec::with_capacity(declared.len());
    for (index, column) in declared.iter().enumerate() {
        let parts: Vec<&dyn arrow::array::Array> =
            arranged.iter().map(|batch| batch[index].as_ref()).collect();
        let values: ArrayRef = if parts.is_empty() {
            new_empty_array(&column.column_type.arrow_type())
        } else {
            arrow::compute::concat(&parts).map_err(|e| failure(e.to_string()))?
        };
        let source = FlatSource::new(column.column_type.clone(), values)?;
        columns.push(if redirected {
            reads_through(source)
        } else {
            source.shared()
        });
    }
    Ok(QueryTable::new_static(
        TableSpec::new(definition, RowSet::flat(size as u64), columns).flat(),
    ))
}

/// A static table of `size` rows and no columns.
///
/// Behavior: io.deephaven.engine.util.TableTools#emptyTable
pub fn empty_table(size: u64) -> Arc<QueryTable> {
    let definition = match TableDefinition::new(Vec::new()) {
        Ok(definition) => definition,
        Err(e) => panic!("{e}"),
    };
    QueryTable::new_static(TableSpec::new(definition, RowSet::flat(size), Vec::new()).flat())
}
