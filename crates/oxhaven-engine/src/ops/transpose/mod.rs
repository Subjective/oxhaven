//! Keyed transpose. Task 4.18.
//!
//! [`keyed_transpose`] aggregates a table by row-by and column-by columns and lays the
//! result out with one row per value of the row-by columns and, for each value of the
//! column-by columns, one column per aggregation output: a pivot, flattened into a table.
//!
//! It is a composition, as in Java: the aggregation ([`crate::ops::agg`]) by all the by
//! columns, split by the column-by columns into one table per value
//! ([`crate::ops::partitioned::partition_by`]), and a multi join of those tables on the
//! row-by columns ([`crate::ops::multi_join`]). The columns are fixed when the result is
//! made. What a value of the column-by columns that appears later does is the caller's
//! choice ([`NewColumnBehavior`]).
//!
//! | File | Holds |
//! | --- | --- |
//! | `mod.rs` | The arguments, the refusals, the composition, the listener that fails on a new column |
//! | `names.rs` | The text of a column-by value and the column name Java makes of it |

mod names;

#[cfg(test)]
mod tests;

use crate::ops::agg::{aggregate_seeded, aggregate_with, AggregateError, Aggregation};
use crate::ops::join::{JoinAddition, JoinMatch};
use crate::ops::multi_join::{multi_join, MultiJoinError, MultiJoinInput};
use crate::ops::partitioned::{partition_by, PartitionedError};
use crate::table::{QueryTable, TableSpec};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::Arc;

/// What the result does when a value of the column-by columns appears that it has no
/// columns for.
///
/// Behavior: io.deephaven.engine.table.impl.util.KeyedTranspose.NewColumnBehavior
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum NewColumnBehavior {
    /// The result fails.
    #[default]
    Fail,
    /// The result goes on without the value's columns.
    Ignore,
}

/// Why a keyed transpose could not be made.
#[derive(Clone, PartialEq, Debug)]
pub enum TransposeError {
    /// What Java reports as `IllegalArgumentException`: no aggregation, no row-by or
    /// column-by column, a table without a value of the column-by columns, or a value no
    /// column name can be made of.
    IllegalArgument(String),
    /// The aggregation by the row-by and column-by columns refused.
    Aggregate(AggregateError),
    /// The split by the column-by columns refused.
    Partitioned(PartitionedError),
    /// The multi join of the tables of each column-by value refused.
    MultiJoin(MultiJoinError),
    /// A table has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for TransposeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransposeError::IllegalArgument(message) => f.write_str(message),
            TransposeError::Aggregate(error) => error.fmt(f),
            TransposeError::Partitioned(error) => error.fmt(f),
            TransposeError::MultiJoin(error) => error.fmt(f),
            TransposeError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for TransposeError {}

impl From<AggregateError> for TransposeError {
    fn from(error: AggregateError) -> Self {
        TransposeError::Aggregate(error)
    }
}

impl From<PartitionedError> for TransposeError {
    fn from(error: PartitionedError) -> Self {
        TransposeError::Partitioned(error)
    }
}

impl From<MultiJoinError> for TransposeError {
    fn from(error: MultiJoinError) -> Self {
        TransposeError::MultiJoin(error)
    }
}

impl From<TableFailure> for TransposeError {
    fn from(failure: TableFailure) -> Self {
        TransposeError::Table(failure)
    }
}

fn failure(message: &str) -> TableFailure {
    TableFailure {
        message: message.to_string(),
    }
}

/// Aggregates `source` by the `row_by` and `column_by` columns and turns the values of
/// the `column_by` columns into columns.
///
/// The result has the `row_by` columns and then, for each value of the `column_by`
/// columns in the order the aggregation first has them, the columns of the aggregations.
/// It has one row per value of the `row_by` columns, with nulls where the aggregation has
/// no group for the row's value and the column's.
///
/// A column is named after its value of the `column_by` columns: the texts of the values,
/// joined by `_` when there are several columns, and after the aggregation's output and
/// `_` when there are several aggregations. Java then makes the names legal and distinct:
/// characters a column name cannot have are dropped, a name that starts with a digit or
/// is a reserved word gets the prefix `column_`, and a name an earlier column has gets a
/// number.
///
/// With `initial_groups` that have a row, the aggregation is made with them and keeps
/// its empty groups, so that the result has the columns of the values they name whether
/// `source` has rows for them or not.
///
/// The columns are those of the values `source` and the initial groups have now. When
/// `source` is refreshing and a value appears later, or a value that lost all its rows
/// gets one again, the result fails ([`NewColumnBehavior::Fail`]) or leaves it out
/// ([`NewColumnBehavior::Ignore`]).
///
/// Not ported: a column-by column of floats, of `Instant`s, of vectors or of arrays.
///
/// Behavior: io.deephaven.engine.table.impl.util.KeyedTranspose#keyedTranspose
pub fn keyed_transpose(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    row_by: &[&str],
    column_by: &[&str],
    initial_groups: Option<&Arc<dyn Table>>,
    new_column_behavior: NewColumnBehavior,
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, TransposeError> {
    let refuse = |message: &str| Err(TransposeError::IllegalArgument(message.to_string()));
    if aggregations.is_empty() {
        return refuse("No aggregations defined");
    }
    if row_by.is_empty() {
        return refuse("No rowByColumns defined");
    }
    if column_by.is_empty() {
        return refuse("No columnByColumns defined");
    }
    let make = || {
        transpose(
            source,
            aggregations,
            row_by,
            column_by,
            initial_groups,
            new_column_behavior,
            compiler,
        )
    };
    // One hold of the lock from the aggregation to the subscription of the result, so
    // that the columns are those of the values the result starts with.
    match source.update_graph().filter(|_| source.is_refreshing()) {
        Some(graph) => locked(&*graph, make),
        None => make(),
    }
}

/// Whether `table` has no rows now.
fn is_empty(table: &Arc<dyn Table>) -> bool {
    let read = || table.row_set().is_empty();
    match table.update_graph().filter(|_| table.is_refreshing()) {
        Some(graph) => locked(&*graph, read),
        None => read(),
    }
}

fn transpose(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    row_by: &[&str],
    column_by: &[&str],
    initial_groups: Option<&Arc<dyn Table>>,
    new_column_behavior: NewColumnBehavior,
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, TransposeError> {
    // Behavior: KeyedTranspose#getAllByColumns, a `LinkedHashSet`.
    let mut all_by: Vec<&str> = Vec::with_capacity(row_by.len() + column_by.len());
    for name in row_by.iter().chain(column_by) {
        if !all_by.contains(name) {
            all_by.push(name);
        }
    }
    let aggregated = match initial_groups.filter(|initial| !is_empty(initial)) {
        Some(initial) => aggregate_seeded(source, aggregations, true, initial, &all_by, compiler)?,
        None => aggregate_with(source, aggregations, false, &all_by, compiler)?,
    };
    let partitioned = partition_by(&aggregated, column_by, false)?;
    let table_of_tables = partitioned.table().clone();
    let rows = table_of_tables.row_set();
    if rows.is_empty() {
        return Err(TransposeError::IllegalArgument(
            "The source table has no values for column by columns".to_string(),
        ));
    }

    // The text of each constituent's value of the column-by columns.
    let definition = table_of_tables.definition();
    let mut values: Vec<String> = vec![String::new(); rows.size() as usize];
    for (position, name) in partitioned.key_columns().iter().enumerate() {
        let index = definition
            .index_of(name)
            .expect("a partitioned table has its key columns");
        let column_type = &definition.columns()[index].column_type;
        let chunk = table_of_tables.column(index).chunk(&rows)?;
        for (value, text) in values
            .iter_mut()
            .zip(names::value_texts(column_type, &chunk))
        {
            if position > 0 {
                value.push('_');
            }
            value.push_str(&text);
        }
    }

    // Behavior: KeyedTranspose#getAddJoinColumns and #legalizeJoinColumnNames
    let aggregated_columns: Vec<&str> = partitioned
        .constituent_definition()
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| !all_by.contains(name))
        .collect();
    let prefixed = aggregations.len() > 1;
    let wanted: Vec<String> = values
        .iter()
        .flat_map(|value| {
            aggregated_columns.iter().map(move |column| {
                if prefixed {
                    format!("{column}_{value}")
                } else {
                    value.clone()
                }
            })
        })
        .collect();
    let mut legal = names::legalize(&wanted)
        .map_err(TransposeError::IllegalArgument)?
        .into_iter();
    let on: Vec<JoinMatch> = row_by.iter().map(|name| JoinMatch::same(*name)).collect();
    let inputs: Vec<MultiJoinInput> = partitioned
        .constituents()?
        .into_iter()
        .map(|table| MultiJoinInput {
            table,
            on: on.clone(),
            add: aggregated_columns
                .iter()
                .zip(&mut legal)
                .map(|(column, name)| JoinAddition {
                    name,
                    source: column.to_string(),
                })
                .collect(),
        })
        .collect();
    let joined = multi_join(&inputs)?;
    if new_column_behavior == NewColumnBehavior::Ignore || !source.is_refreshing() {
        return Ok(joined);
    }

    // The columns of the table of tables whose change means another table for a column.
    let mut table_or_keys: Vec<usize> = partitioned
        .key_columns()
        .iter()
        .filter_map(|name| definition.index_of(name))
        .collect();
    table_or_keys.extend(definition.index_of(partitioned.constituent_column()));
    let parents = [joined.clone(), table_of_tables];
    let result = QueryTable::derive_merged(&parents, Vec::new(), || {
        let columns = (0..joined.definition().columns().len())
            .map(|index| joined.column(index))
            .collect();
        let spec = TableSpec::new(
            joined.definition().clone(),
            (*joined.row_set()).clone(),
            columns,
        );
        // Behavior: the `MergedListener` of KeyedTranspose#keyedTranspose. A table that
        // leaves the table of tables, or moves in it, changes nothing.
        let operator = move |_result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
            if let Some(tables) = upstream.get(1).and_then(Option::as_ref) {
                if !tables.added.is_empty() {
                    return Err(failure(
                        "New constituent table detected in keyedTranspose; consider \
                         setting newColumnBehavior to Ignore.",
                    ));
                }
                if !tables.modified.is_empty()
                    && tables.modified_columns.contains_any(&table_or_keys)
                {
                    return Err(failure(
                        "Modified constituent table detected in keyedTranspose; consider \
                         setting newColumnBehavior to Ignore.",
                    ));
                }
            }
            Ok(match upstream.first().and_then(Option::as_ref) {
                Some(update) => (**update).clone(),
                None => TableUpdate::empty(),
            })
        };
        Ok((spec, operator))
    })?;
    Ok(result)
}
