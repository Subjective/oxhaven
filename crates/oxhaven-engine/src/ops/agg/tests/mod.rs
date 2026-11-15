//! Tests of aggregation.

mod corpus;
mod fixtures;
mod group;
mod incremental;
mod random;
mod static_tables;

use super::{aggregate_with, AggSpec, AggregateError, Aggregation, Pair, Sentinel};
use crate::table::in_memory_table;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Schema, TimestampNanosecondType,
};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use std::collections::HashMap;
use std::sync::Arc;

/// `aggregate_with` the compiler of `oxhaven-expr`, for the filters of a count where.
fn aggregate(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    by: &[&str],
) -> Result<Arc<dyn Table>, AggregateError> {
    aggregate_with(
        parent,
        aggregations,
        preserve_empty,
        by,
        &oxhaven_expr::Compiler,
    )
}

/// `spec` with each of `pairs`, written `"Out = In"` or `"Column"`.
fn agg(spec: AggSpec, pairs: &[&str]) -> Aggregation {
    Aggregation::columns(spec, pairs.iter().map(|pair| parse_pair(pair)))
}

fn parse_pair(text: &str) -> Pair {
    match text.split_once('=') {
        Some((output, input)) => Pair::new(output.trim(), input.trim()),
        None => Pair::of(text.trim()),
    }
}

fn count(column: &str) -> Aggregation {
    Aggregation::count(column)
}

fn batch(columns: &[(&str, ColumnType, ArrayRef)]) -> (TableDefinition, RecordBatch) {
    let definition = TableDefinition::new(
        columns
            .iter()
            .map(|(name, column_type, _)| ColumnDefinition::new(*name, column_type.clone()))
            .collect(),
    )
    .expect("distinct names");
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns.iter().map(|(_, _, array)| array.clone()).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch");
    (definition, batch)
}

/// A static table of the given columns.
fn static_table(columns: &[(&str, ColumnType, ArrayRef)]) -> Arc<dyn Table> {
    let (definition, batch) = batch(columns);
    in_memory_table(definition, &[batch]).expect("a table")
}

/// The current columns of `table`, in row set order.
fn read(table: &dyn Table) -> Vec<ArrayRef> {
    let read = || {
        let rows = table.row_set();
        (0..table.definition().columns().len())
            .map(|index| table.column(index).chunk(&rows).expect("a readable column"))
            .collect()
    };
    match table.update_graph() {
        Some(graph) => locked(&*graph, read),
        None => read(),
    }
}

/// A cell as text: `null`, floats as Rust's `{:?}` prints them, an `Instant` as its
/// nanoseconds, a `char` as its code unit.
fn cell(array: &ArrayRef, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    match array.data_type() {
        DataType::Float64 => format!("{:?}", array.as_primitive::<Float64Type>().value(row)),
        DataType::Float32 => format!("{:?}", array.as_primitive::<Float32Type>().value(row)),
        DataType::Timestamp(_, _) => array
            .as_primitive::<TimestampNanosecondType>()
            .value(row)
            .to_string(),
        DataType::List(_) => {
            let elements = array.as_list::<i32>().value(row);
            let cells: Vec<String> = (0..elements.len()).map(|at| cell(&elements, at)).collect();
            format!("[{}]", cells.join(", "))
        }
        _ => array_value_to_string(array, row).expect("a printable value"),
    }
}

/// The rows of `table` as text: cells joined by `|`, null as `null`.
fn rows(table: &dyn Table) -> Vec<String> {
    let columns = read(table);
    let size = columns.first().map_or(0, |column| column.len());
    (0..size)
        .map(|row| {
            columns
                .iter()
                .map(|column| cell(column, row))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// The names and types of the columns of `table`.
fn schema(table: &dyn Table) -> Vec<(String, ColumnType)> {
    table
        .definition()
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.column_type.clone()))
        .collect()
}

/// `aggregate` without `preserve_empty`, which must succeed.
fn agg_by(parent: &Arc<dyn Table>, aggregations: &[Aggregation], by: &[&str]) -> Arc<dyn Table> {
    aggregate(parent, aggregations, false, by).expect("an aggregation")
}

/// Every aggregation of column `input`, with outputs named after the aggregation.
fn all_of(input: &str) -> Vec<Aggregation> {
    vec![
        agg(AggSpec::Sum, &[&format!("Sum = {input}")]),
        agg(AggSpec::Avg, &[&format!("Avg = {input}")]),
        agg(AggSpec::Min, &[&format!("Min = {input}")]),
        agg(AggSpec::Max, &[&format!("Max = {input}")]),
        agg(AggSpec::First, &[&format!("First = {input}")]),
        agg(AggSpec::Last, &[&format!("Last = {input}")]),
        count("N"),
    ]
}

/// A static table with the current rows of `table`.
fn static_copy(table: &dyn Table) -> Arc<dyn Table> {
    let columns: Vec<(&str, ColumnType, ArrayRef)> = table
        .definition()
        .columns()
        .iter()
        .zip(read(table))
        .map(|(column, values)| (column.name.as_str(), column.column_type.clone(), values))
        .collect();
    static_table(&columns)
}

/// `expected` with its rows in the order `like` has their keys (the first `key_columns`
/// columns), followed by the rows whose key `like` does not have.
///
/// An aggregation that was kept up to date has its groups in the order they were first
/// seen over its whole history, and one made now has them in the order of the current
/// rows. Both are right; the order is checked by the scripted tests and the corpus.
fn in_order_of(expected: &dyn Table, like: &dyn Table, key_columns: usize) -> Arc<dyn Table> {
    let key = |columns: &[ArrayRef], row: usize| {
        columns[..key_columns]
            .iter()
            .map(|column| cell(column, row))
            .collect::<Vec<_>>()
            .join("|")
    };
    let expected_columns = read(expected);
    let like_columns = read(like);
    let size = |columns: &[ArrayRef]| columns.first().map_or(0, |column| column.len());
    let mut row_of: HashMap<String, usize> = (0..size(&expected_columns))
        .map(|row| (key(&expected_columns, row), row))
        .collect();
    let mut order: Vec<u64> = (0..size(&like_columns))
        .filter_map(|row| row_of.remove(&key(&like_columns, row)))
        .map(|row| row as u64)
        .collect();
    let mut unmatched: Vec<u64> = row_of.into_values().map(|row| row as u64).collect();
    unmatched.sort_unstable();
    order.extend(unmatched);
    let order = arrow::array::UInt64Array::from(order);
    let columns: Vec<(&str, ColumnType, ArrayRef)> = expected
        .definition()
        .columns()
        .iter()
        .zip(&expected_columns)
        .map(|(column, values)| {
            let values = arrow::compute::take(values, &order, None).expect("a reordered column");
            (column.name.as_str(), column.column_type.clone(), values)
        })
        .collect();
    static_table(&columns)
}
