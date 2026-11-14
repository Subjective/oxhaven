//! Aggregation. Task 3.6, extended by 4.1.
//!
//! [`aggregate`] is `Table#aggBy`: it groups the rows of a table by key columns and
//! computes one result column per aggregation, and keeps the result up to date as the
//! table changes.
//!
//! | Part | What it does |
//! | --- | --- |
//! | [`aggregate`] | Checks the request, picks the operators, builds the result table |
//! | `aggregator` | The framework: keys to slots, rows to operators, operator answers to result rows |
//! | `keys` | The slot of each key, in first-seen order |
//! | [`AggOperator`] | What one aggregation implements |
//! | [`Run`], [`Values`], [`Scalar`] | Column values as an operator sees them, whatever the column type |
//! | `sum`, `min_max`, `first_last` | The operators: count, sum, avg; min, max; first, last |
//!
//! # Adding an aggregation
//!
//! 1. Implement [`AggOperator`]. It is given the rows of one group at a time as a [`Run`]
//!    and answers with an [`Effect`]. Read values through `Run::longs`, `Run::doubles`,
//!    `Run::bools` or `Run::scalars`, so one implementation serves every column type.
//! 2. Decide what the operator needs. If a group's result can always be brought up to date
//!    from the rows that came and went, implement `add` and `remove` and nothing else. If a
//!    removal can leave it unable to answer, return `true` from `needs_group_rows` and
//!    [`Effect::Recompute`] when that happens; `recompute` is then called with the rows of
//!    the group. The framework keeps the rows of every group only if some operator of the
//!    aggregation asks.
//! 3. Add a variant to [`AggSpec`] and an arm to `make_operator`, where the operator for
//!    the input column's type and the kind of parent is chosen and unsupported types are
//!    refused.
//!
//! Not here yet, for task 4.1: aggregations with several inputs or outputs per operator,
//! and `initialGroups`.

mod aggregator;
mod first_last;
mod keys;
mod min_max;
mod operator;
mod sum;
mod values;

#[cfg(test)]
mod tests;

pub use first_last::{FirstLast, FirstLastMode};
pub use min_max::MinMax;
pub use operator::{AggOperator, Effect, GroupReader};
pub use sum::{BooleanSum, Count, FloatAvg, FloatSum, LongAvg, LongSum};
pub use values::{build, Run, Scalar, Shape, Values};

use crate::table::{QueryTable, TableSpec};
use aggregator::Aggregator;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

/// An aggregation of one input column into one output column.
///
/// Behavior: the `io.deephaven.api.agg.spec.AggSpec` implementations of the same names.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum AggSpec {
    /// The sum of the non-null values. See [`LongSum`], [`FloatSum`], [`BooleanSum`].
    Sum,
    /// The mean of the non-null values. See [`LongAvg`], [`FloatAvg`].
    Avg,
    /// The least non-null value. See [`MinMax`].
    Min,
    /// The greatest non-null value. See [`MinMax`].
    Max,
    /// The value in the group's first row. See [`FirstLast`].
    First,
    /// The value in the group's last row. See [`FirstLast`].
    Last,
}

/// An input column and the output column aggregated from it.
///
/// Behavior: io.deephaven.api.Pair
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Pair {
    pub output: String,
    pub input: String,
}

impl Pair {
    pub fn new(output: impl Into<String>, input: impl Into<String>) -> Pair {
        Pair {
            output: output.into(),
            input: input.into(),
        }
    }

    /// The pair whose output has the name of its input.
    pub fn of(column: impl Into<String>) -> Pair {
        let column = column.into();
        Pair::new(column.clone(), column)
    }
}

/// One entry of an `aggBy` request.
///
/// Behavior: io.deephaven.api.agg.Aggregation, of which this is
/// io.deephaven.api.agg.ColumnAggregation / ColumnAggregations and io.deephaven.api.agg.Count.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum Aggregation {
    /// `spec` applied to each pair.
    Columns { spec: AggSpec, pairs: Vec<Pair> },
    /// The number of rows of each group, as a `long` column named `column`.
    Count { column: String },
}

impl Aggregation {
    pub fn columns(spec: AggSpec, pairs: impl IntoIterator<Item = Pair>) -> Aggregation {
        Aggregation::Columns {
            spec,
            pairs: pairs.into_iter().collect(),
        }
    }

    pub fn count(column: impl Into<String>) -> Aggregation {
        Aggregation::Count {
            column: column.into(),
        }
    }
}

/// Why an aggregation could not be made. Java reports all but `Failed` as an
/// `IllegalArgumentException` or a `NoSuchColumnException`, which a client sees as
/// `INVALID_ARGUMENT`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AggregateError {
    /// No aggregation was given.
    NoAggregations,
    /// A group-by column or an input column is not a column of the table.
    UnknownColumn(String),
    /// Two aggregations have the same output column.
    DuplicateOutput(String),
    /// The aggregation is not defined for the type of its input column.
    UnsupportedType {
        spec: AggSpec,
        column: String,
        column_type: ColumnType,
    },
    /// The table has failed, or could not be read.
    Failed(TableFailure),
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateError::NoAggregations => {
                write!(f, "aggBy must have at least one aggregation, none specified")
            }
            AggregateError::UnknownColumn(column) => write!(f, "unknown column {column}"),
            AggregateError::DuplicateOutput(column) => {
                write!(f, "duplicate output column {column}")
            }
            AggregateError::UnsupportedType {
                spec,
                column,
                column_type,
            } => write!(
                f,
                "{spec:?} is not supported for column {column} of type {column_type:?}"
            ),
            AggregateError::Failed(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for AggregateError {}

impl From<TableFailure> for AggregateError {
    fn from(failure: TableFailure) -> Self {
        AggregateError::Failed(failure)
    }
}

/// What the parent can do to its rows, which decides the operator for min, max, first and
/// last.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ParentKind {
    Static,
    /// Refreshing, and rows are only ever added.
    AddOnly,
    Blink,
    /// Rows are added, removed, modified and shifted.
    General,
}

impl ParentKind {
    fn of(parent: &dyn Table) -> ParentKind {
        let attributes = parent.attributes();
        if !parent.is_refreshing() {
            ParentKind::Static
        } else if attributes.blink {
            ParentKind::Blink
        } else if attributes.add_only || attributes.append_only {
            ParentKind::AddOnly
        } else {
            ParentKind::General
        }
    }
}

/// The operator for `spec` on a column of `input` type, or `None` if Java has none.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor's `Converter`:
/// `visit(AggSpecSum)` / `makeSumOperator`, `visit(AggSpecAvg)` / `makeAvgOperator`,
/// `visit(AggSpecMin)`, `visit(AggSpecMax)` / `makeMinOrMaxOperator`, and
/// `visit(AggSpecFirst)`, `visit(AggSpecLast)` / `makeFirstOrLastOperator`.
fn make_operator(
    spec: AggSpec,
    input: &ColumnType,
    parent: ParentKind,
) -> Option<Box<dyn AggOperator>> {
    let integer = matches!(
        input,
        ColumnType::Byte | ColumnType::Char | ColumnType::Short | ColumnType::Int | ColumnType::Long
    );
    Some(match spec {
        AggSpec::Sum => match input {
            ColumnType::Boolean => Box::<BooleanSum>::default(),
            ColumnType::Float => Box::new(FloatSum::new(true)),
            ColumnType::Double => Box::new(FloatSum::new(false)),
            _ if integer => Box::<LongSum>::default(),
            _ => return None,
        },
        AggSpec::Avg => match input {
            ColumnType::Float | ColumnType::Double => Box::<FloatAvg>::default(),
            _ if integer => Box::<LongAvg>::default(),
            _ => return None,
        },
        AggSpec::Min | AggSpec::Max => Box::new(MinMax::new(
            input.clone(),
            spec == AggSpec::Min,
            parent == ParentKind::General,
        )),
        AggSpec::First | AggSpec::Last => {
            let mode = match parent {
                ParentKind::Static | ParentKind::AddOnly => FirstLastMode::ByRowKey,
                ParentKind::Blink => FirstLastMode::Arrival,
                ParentKind::General => FirstLastMode::Tracked,
            };
            Box::new(FirstLast::new(input.clone(), spec == AggSpec::First, mode))
        }
    })
}

/// Groups the rows of `parent` by the `group_by` columns and aggregates each group.
///
/// The result has the group-by columns, then one column per aggregation output in the
/// order given. It has one row per group that has rows, in the order the groups were first
/// seen; a group that empties and fills again returns to its place. With no group-by
/// columns the whole table is one group, and the result has one row, or none if the table
/// is empty. With `preserve_empty` a group keeps its row when it loses its last row, and
/// the aggregation without keys always has its row.
///
/// The result is static if `parent` is, and otherwise changes with it.
///
/// Not ported: `initialGroups`. Panics with `unported!` if an output column has the name
/// of a group-by column, which Java accepts with a result this port has not pinned down.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#aggBy and #aggNoMemo, over
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation
pub fn aggregate(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    group_by: &[&str],
) -> Result<Arc<dyn Table>, AggregateError> {
    if aggregations.is_empty() {
        return Err(AggregateError::NoAggregations);
    }
    let definition = parent.definition();
    let column = |name: &str| {
        definition
            .index_of(name)
            .ok_or_else(|| AggregateError::UnknownColumn(name.to_string()))
    };

    // Group-by columns named twice count once, as in Java's map of key columns.
    let mut key_columns: Vec<usize> = Vec::new();
    for name in group_by {
        let index = column(name)?;
        if !key_columns.contains(&index) {
            key_columns.push(index);
        }
    }
    let mut result_columns: Vec<ColumnDefinition> = key_columns
        .iter()
        .map(|&index| definition.columns()[index].clone())
        .collect();

    let kind = ParentKind::of(&**parent);
    let mut operators: Vec<(Box<dyn AggOperator>, Option<usize>)> = Vec::new();
    let mut outputs: Vec<&str> = Vec::new();
    let mut planned: Vec<(&str, Box<dyn AggOperator>, Option<usize>)> = Vec::new();
    for aggregation in aggregations {
        match aggregation {
            Aggregation::Count { column } => {
                planned.push((column, Box::<Count>::default(), None));
            }
            Aggregation::Columns { spec, pairs } => {
                for pair in pairs {
                    let index = column(&pair.input)?;
                    let input_type = &definition.columns()[index].column_type;
                    let operator = make_operator(*spec, input_type, kind).ok_or_else(|| {
                        AggregateError::UnsupportedType {
                            spec: *spec,
                            column: pair.input.clone(),
                            column_type: input_type.clone(),
                        }
                    })?;
                    planned.push((&pair.output, operator, Some(index)));
                }
            }
        }
    }
    for (name, operator, input) in planned {
        if outputs.contains(&name) {
            return Err(AggregateError::DuplicateOutput(name.to_string()));
        }
        outputs.push(name);
        if group_by.contains(&name) {
            unported!(
                "io.deephaven.engine.table.impl.by.AggregationContext#getResultColumns",
                "aggregation output {name} has the name of a group-by column"
            );
        }
        result_columns.push(ColumnDefinition::new(name, operator.result_type()));
        operators.push((operator, input));
    }
    let result_definition = TableDefinition::new(result_columns)
        .map_err(|duplicate| AggregateError::DuplicateOutput(duplicate.0))?;

    let keyed = !key_columns.is_empty();
    let flat = match kind {
        ParentKind::Static | ParentKind::AddOnly | ParentKind::Blink => true,
        ParentKind::General => !keyed,
    };
    let table = QueryTable::derive(parent, || {
        let mut aggregator =
            Aggregator::new(parent.clone(), key_columns, operators, preserve_empty)?;
        let rows = aggregator.initial()?;
        let mut spec = TableSpec::new(result_definition, rows, aggregator.columns());
        if flat {
            spec = spec.flat();
        }
        let state = Mutex::new(aggregator);
        let operator = move |_result: &QueryTable, upstream: &TableUpdate| {
            state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .on_update(upstream)
        };
        Ok((spec, operator))
    })?;
    Ok(table)
}
