//! Tables: the concrete [`QueryTable`] that every operation returns, and the root tables.
//!
//! | Item | What it is |
//! | --- | --- |
//! | [`QueryTable`] | The concrete `Table`: row set, columns, attributes, listeners, and what feeds it |
//! | [`in_memory_table`], [`uploaded_table`], [`empty_table`] | Static tables |
//! | [`append_only_input_table`], [`keyed_input_table`] | Tables written through `Table::input_table` |
//! | [`time_table`] | A row per period of an injected [`TimeSource`] |
//!
//! # How to write an operator
//!
//! An operation has two halves: build the result from the parent's current state, and keep
//! it up to date. Give both to [`QueryTable::derive`] (or [`QueryTable::derive_merged`] for
//! several parents) and it does the rest: it takes the graph lock around the initial read
//! and the subscription, owns the listener, applies each update you return to the row set,
//! delivers it, answers `satisfied`, and fails the result if the parent fails or you return
//! an error. A static parent gives a static result.
//!
//! This operator passes its parent through unchanged:
//!
//! ```
//! use oxhaven_contracts::graph::UpdateGraph;
//! use oxhaven_contracts::rowset::RowSet;
//! use oxhaven_contracts::schema::ColumnType;
//! use oxhaven_contracts::table::{Table, TableFailure};
//! use oxhaven_contracts::update::TableUpdate;
//! use oxhaven_engine::graph::SteppedUpdateGraph;
//! use oxhaven_engine::table::{QueryTable, TableSpec};
//! use oxhaven_testkit::{definition, longs, ConsistencyChecker, ScriptedTable, TableChange};
//! use std::sync::Arc;
//!
//! fn pass_through(parent: &Arc<dyn Table>) -> Result<Arc<QueryTable>, TableFailure> {
//!     QueryTable::derive(parent, || {
//!         // The initial state: the parent's rows and, here, the parent's own columns.
//!         let definition = parent.definition().clone();
//!         let columns = (0..definition.columns().len())
//!             .map(|index| parent.column(index))
//!             .collect();
//!         let spec = TableSpec::new(definition, (*parent.row_set()).clone(), columns)
//!             .with_attributes(parent.attributes());
//!         // The operator: called once per step on which the parent changed, it returns
//!         // the result's change. An operator with its own columns writes them here first.
//!         let operator = |_result: &QueryTable, upstream: &TableUpdate| Ok(upstream.clone());
//!         Ok((spec, operator))
//!     })
//! }
//!
//! let graph = SteppedUpdateGraph::new();
//! let source = ScriptedTable::new(
//!     graph.clone(),
//!     definition(&[("Qty", ColumnType::Long)]),
//!     RowSet::flat(2),
//!     vec![longs([10, 20])],
//! );
//! let parent: Arc<dyn Table> = source.table();
//! let result = pass_through(&parent)?;
//! let checker = ConsistencyChecker::attach(result.clone());
//!
//! source.push(TableChange::new().remove([0]).add([5], vec![longs([30])]));
//! graph.run_cycle();
//! checker.assert_consistent();
//! assert_eq!(*result.row_set(), RowSet::from_keys([1, 5]));
//! # Ok::<(), TableFailure>(())
//! ```
//!
//! An operator that computes values keeps them in [`WritableSource`]s made with
//! [`tracked_source`], which remember the previous value of everything written during a
//! cycle. In `on_update`, bring them up to date in the order of the update: remove
//! `removed`, apply `shifted` to the rows that are left, then write `added` and `modified`.
//! Read the parent with `chunk` for the new values and `prev_chunk` for the old ones.
//!
//! Rules an operator must keep:
//!
//! - Return the result's change, in the result's key space. Use
//!   [`ModifiedColumnTransformer`](crate::update::ModifiedColumnTransformer) to translate
//!   the modified columns. Returning a change that touches nothing is fine.
//! - Do not hold the result table strongly inside the operator; it is passed to `on_update`.
//!   Hold the parents strongly if you read them.
//! - Two or more refreshing parents need [`QueryTable::derive_merged`], never two results.

mod copied;
mod in_memory;
mod input;
mod query_table;
mod storage;
mod time;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use copied::is_copy;
pub(crate) use copied::{copy_of, mark, marks, memoized, CopyType, Marks};
pub use in_memory::{empty_table, in_memory_table, uploaded_table};
pub use input::{append_only_input_table, keyed_input_table};
pub use query_table::{MergedOperator, QueryTable, RootSource, TableSpec, UnaryOperator};
pub(crate) use storage::{forget_reads_through, register_reads_through};
pub use storage::{holds_values, reads_through};
pub use time::{time_table, TimeSource, TimeTableOptions, WallClock};

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use oxhaven_column::{Clock, WritableSource};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType};
use oxhaven_contracts::table::{Table, TableFailure};
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// Whether `table` never removes, modifies or shifts a row: it is static, or says so.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#isAddOnly
pub fn is_add_only(table: &dyn Table) -> bool {
    let attributes = table.attributes();
    !table.is_refreshing() || attributes.add_only || attributes.append_only
}

/// Whether `table` only ever adds rows after its last row: it is static, or says so, or is
/// add-only and flat.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable#isAppendOnly
pub fn is_append_only(table: &dyn Table) -> bool {
    let attributes = table.attributes();
    !table.is_refreshing() || attributes.append_only || (attributes.add_only && table.is_flat())
}

/// An empty writable column that keeps previous values for writes made during a cycle of
/// `graph`. The column of a refreshing table that computes or stores its own values.
pub fn tracked_source(
    graph: &Arc<dyn UpdateGraph>,
    column_type: ColumnType,
) -> Arc<WritableSource> {
    let source = Arc::new(WritableSource::new(column_type));
    let clock: Arc<dyn Clock> = Arc::new(graph.clone());
    source.start_tracking_prev(clock);
    source
}

/// The columns of `batch` named by `declared`, in that order. An error unless the batch
/// has exactly those columns, each with the Arrow type of its column type.
///
/// Behavior: io.deephaven.engine.table.TableDefinition#checkMutualCompatibility
fn arrange_columns(
    declared: &[&ColumnDefinition],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>, TableFailure> {
    let mut problems = Vec::new();
    let mut arranged = Vec::with_capacity(declared.len());
    for column in declared {
        match batch.column_by_name(&column.name) {
            None => problems.push(format!("column {} is missing", column.name)),
            Some(values) if *values.data_type() != column.column_type.arrow_type() => problems
                .push(format!(
                    "column {} is {}, expected {}",
                    column.name,
                    values.data_type(),
                    column.column_type.arrow_type()
                )),
            Some(values) => arranged.push(values.clone()),
        }
    }
    let schema = batch.schema();
    for field in schema.fields() {
        if !declared.iter().any(|column| column.name == *field.name()) {
            problems.push(format!("column {} is not expected", field.name()));
        }
    }
    if problems.is_empty() {
        Ok(arranged)
    } else {
        Err(failure(format!(
            "Table definition incompatibilities: {}",
            problems.join("; ")
        )))
    }
}
