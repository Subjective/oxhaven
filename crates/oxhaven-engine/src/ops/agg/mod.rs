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
//! | `sum`, `min_max`, `first_last` | The operators: count, sum, abs sum, avg; min, max; first, last |
//! | `var` | Variance and standard deviation |
//! | `distinct` | Count distinct, distinct, unique, median and percentile, over a sorted multiset per group |
//! | `count_where` | Count where: the rows of each group that pass filters |
//! | `weighted` | Weighted average and weighted sum, which read a weight column too |
//! | `sorted` | Sorted first and sorted last, which read sort columns and an output column |
//! | `group` | Group, and with it [`group_by`]: the values of each group as a vector |
//!
//! # Adding an aggregation
//!
//! 1. Implement [`AggOperator`]. It is given the rows of one group at a time as a [`Run`]
//!    and answers with an [`Effect`]. Read values through `Run::longs`, `Run::doubles`,
//!    `Run::bools` or `Run::scalars`, so one implementation serves every column type.
//! 2. Decide what the operator needs. If a group's result can always be brought up to date
//!    from the rows that came and went, implement `add` and `remove` and nothing else. If a
//!    removal can leave it unable to answer, return `true` from `needs_group_rows` and
//!    [`Effect::RECOMPUTE`] when that happens; `recompute` is then called with the rows of
//!    the group. The framework keeps the rows of every group only if some operator of the
//!    aggregation asks.
//! 3. Add a variant to [`AggSpec`] and an arm to `make_operator`, where the operator for
//!    the input column's type and the kind of parent is chosen and unsupported types are
//!    refused. An operator that reads a second column gets it as the weight of its
//!    [`Run`]s; one that reads columns itself names them in `Planned::reads`, so that it is
//!    told when they are modified.
//!
//! `Table#groupBy` is [`group_by`]: the `group` aggregation of every column that is not
//! grouped by.
//!
//! Not here: the aggregations whose results are tables or formulas over vectors
//! (`AggSpecFormula`, `Formula`, `Partition`): the formula language of this port has no
//! vector values, and there are no partitioned tables. Nor `FirstRowKey`, `LastRowKey`, the
//! approximate percentile, t-digest and freeze; `initialGroups`; and, of a column of
//! vectors or arrays, every aggregation but `first`, `last`, `group` and the output of a
//! sorted first or last. The gRPC handler refuses each of them by name.

mod aggregator;
mod count_where;
mod distinct;
mod first_last;
mod group;
pub(crate) mod keys;
mod min_max;
mod operator;
mod sorted;
mod sum;
mod values;
mod var;
mod weighted;

#[cfg(test)]
mod tests;

pub use count_where::CountWhere;
pub use distinct::{SetMode, SortedValues};
pub use first_last::{FirstLast, FirstLastCell, FirstLastMode};
pub use group::Group;
pub use min_max::MinMax;
pub use operator::{AggOperator, Effect, GroupReader};
pub use sorted::{SortColumn, SortedFirstLast};
pub use sum::{BooleanSum, Count, FloatAvg, FloatSum, LongAvg, LongSum};
pub use values::{build, Run, Scalar, Shape, Values};
pub use var::Var;
pub use weighted::{WeightedFloat, WeightedLongSum};

use crate::ops::filter::RowFilters;
use crate::table::{QueryTable, TableSpec};
use aggregator::{Aggregator, Layout, Planned};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

/// An aggregation of one input column into one output column.
///
/// Behavior: the `io.deephaven.api.agg.spec.AggSpec` implementations of the same names.
#[derive(Clone, PartialEq, Debug)]
#[non_exhaustive]
pub enum AggSpec {
    /// The sum of the non-null values. See [`LongSum`], [`FloatSum`], [`BooleanSum`].
    Sum,
    /// The sum of the absolute values of the non-null values. See [`LongSum`],
    /// [`FloatSum`], [`BooleanSum`].
    AbsSum,
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
    /// The sample variance of the non-null values. See [`Var`].
    Var,
    /// The sample standard deviation of the non-null values. See [`Var`].
    Std,
    /// The median of the non-null values. See [`SortedValues`].
    Median { average_evenly_divided: bool },
    /// A percentile, between 0 and 1, of the non-null values. See [`SortedValues`].
    Percentile {
        percentile: f64,
        average_evenly_divided: bool,
    },
    /// The number of distinct values. See [`SortedValues`].
    CountDistinct { count_nulls: bool },
    /// The only distinct value; null for a group without values, and `non_unique` for one
    /// with several. See [`SortedValues`] and [`Sentinel`].
    Unique {
        include_nulls: bool,
        non_unique: Sentinel,
    },
    /// The distinct values, sorted, as a vector. See [`SortedValues`].
    Distinct { include_nulls: bool },
    /// The average weighted by another column. See [`WeightedFloat`].
    WeightedAvg { weight: String },
    /// The sum weighted by another column. See [`WeightedFloat`], [`WeightedLongSum`].
    WeightedSum { weight: String },
    /// The value in the row that sorts first by the columns given, ascending. See
    /// [`SortedFirstLast`].
    SortedFirst { columns: Vec<String> },
    /// The value in the row that sorts last by the columns given, ascending. See
    /// [`SortedFirstLast`].
    SortedLast { columns: Vec<String> },
    /// The values of the group's rows, in row order, as a vector. See [`Group`].
    Group,
}

/// The value `unique` shows for a group with more than one distinct value, as a client
/// gives it: a value of one of these types, which need not be the column's.
///
/// Behavior: io.deephaven.api.object.UnionObject, as
/// io.deephaven.server.table.ops.AggSpecAdapter#adapt(AggSpecNonUniqueSentinel) makes one
#[derive(Clone, PartialEq, Debug)]
pub enum Sentinel {
    Null,
    Boolean(bool),
    Byte(i8),
    Char(u16),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
}

impl Sentinel {
    /// The Java class of the value, as an error names it.
    fn java_class(&self) -> &'static str {
        match self {
            Sentinel::Null => "null",
            Sentinel::Boolean(_) => "java.lang.Boolean",
            Sentinel::Byte(_) => "java.lang.Byte",
            Sentinel::Char(_) => "java.lang.Character",
            Sentinel::Short(_) => "java.lang.Short",
            Sentinel::Int(_) => "java.lang.Integer",
            Sentinel::Long(_) => "java.lang.Long",
            Sentinel::Float(_) => "java.lang.Float",
            Sentinel::Double(_) => "java.lang.Double",
            Sentinel::String(_) => "java.lang.String",
        }
    }

    fn text(&self) -> String {
        match self {
            Sentinel::Null => "null".to_string(),
            Sentinel::Boolean(v) => v.to_string(),
            Sentinel::Byte(v) => v.to_string(),
            Sentinel::Char(v) => String::from_utf16_lossy(&[*v]),
            Sentinel::Short(v) => v.to_string(),
            Sentinel::Int(v) => v.to_string(),
            Sentinel::Long(v) => v.to_string(),
            Sentinel::Float(v) => format!("{v:?}"),
            Sentinel::Double(v) => format!("{v:?}"),
            Sentinel::String(v) => v.clone(),
        }
    }

    /// The value as an exact integer, if it is a number without a fraction. `Err` for a
    /// NaN or an infinity, which Java cannot make a `BigDecimal` of.
    fn integer(&self) -> Result<Option<i128>, ()> {
        let float = |v: f64| {
            if !v.is_finite() {
                Err(())
            } else if v.fract() == 0.0 {
                // A finite double without a fraction is an integer of at most 1024 bits;
                // beyond 127 of them it is out of every range asked about.
                Ok(Some(if v.abs() < 1e38 { v as i128 } else if v > 0.0 { i128::MAX } else { i128::MIN }))
            } else {
                Ok(None)
            }
        };
        match self {
            Sentinel::Byte(v) => Ok(Some(i128::from(*v))),
            Sentinel::Short(v) => Ok(Some(i128::from(*v))),
            Sentinel::Int(v) => Ok(Some(i128::from(*v))),
            Sentinel::Long(v) => Ok(Some(i128::from(*v))),
            Sentinel::Float(v) => float(f64::from(*v)),
            Sentinel::Double(v) => float(*v),
            _ => Ok(None),
        }
    }

    /// The value as a `double`, if it is a number: an integer converted, a `float` widened.
    fn double(&self) -> Option<f64> {
        match self {
            Sentinel::Byte(v) => Some(f64::from(*v)),
            Sentinel::Short(v) => Some(f64::from(*v)),
            Sentinel::Int(v) => Some(f64::from(*v)),
            Sentinel::Long(v) => Some(*v as f64),
            Sentinel::Float(v) => Some(f64::from(*v)),
            Sentinel::Double(v) => Some(*v),
            _ => None,
        }
    }
}

/// The Java class Java's errors name for a column type that can have a sentinel.
fn boxed_class(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Boolean => "java.lang.Boolean",
        ColumnType::Byte => "java.lang.Byte",
        ColumnType::Char => "java.lang.Character",
        ColumnType::Short => "java.lang.Short",
        ColumnType::Int => "java.lang.Integer",
        ColumnType::Long => "java.lang.Long",
        ColumnType::Float => "java.lang.Float",
        ColumnType::Double => "java.lang.Double",
        ColumnType::String => "java.lang.String",
        ColumnType::Instant => "java.time.Instant",
        _ => "java.lang.Object",
    }
}

/// The sentinel as a value of a column of `column_type`, for the output column `output`.
///
/// A sentinel of the column's own type is taken as it is, and one that is the type's null
/// value (`Byte.MIN_VALUE` and the like, `Character.MAX_VALUE`, `-Float.MAX_VALUE`) is a
/// null. A number for a column of another numeric type is converted if it is in the range
/// of the column's type without its null value, and for an integer column has no
/// fraction; a NaN or an infinity is refused there, since Java cannot make a `BigDecimal`
/// of it. Anything else is refused: a number for a `char`, `Boolean`, `String` or
/// `Instant` column, and every sentinel a client can send for an `Instant` column.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor#makeUniqueOperator,
/// #checkType and #isNumericallyCompatible, io.deephaven.engine.table.impl.by.UnionObjectUtils
fn sentinel_value(
    column_type: &ColumnType,
    sentinel: &Sentinel,
    output: &str,
) -> Result<Scalar, AggregateError> {
    let out_of_range = || {
        Err(AggregateError::InvalidSentinel(format!(
            "For result column {output} the Non Unique Sentinel {} is out of range for {}",
            sentinel.text(),
            boxed_class(column_type)
        )))
    };
    let integer_in = |min: i64, max: i64| match sentinel.integer() {
        Err(()) => Err(AggregateError::InvalidSentinel(format!(
            "For result column {output} the Non Unique Sentinel {} is infinite or NaN",
            sentinel.text()
        ))),
        Ok(Some(value)) if (i128::from(min)..=i128::from(max)).contains(&value) => {
            Ok(Scalar::Long(value as i64))
        }
        Ok(_) => out_of_range(),
    };
    // The null value of each integer type is its least value.
    let or_null = |value: i64, null: i64| {
        if value == null {
            Scalar::Null
        } else {
            Scalar::Long(value)
        }
    };
    match (column_type, sentinel) {
        (_, Sentinel::Null) => Ok(Scalar::Null),
        (ColumnType::Boolean, Sentinel::Boolean(v)) => Ok(Scalar::Bool(*v)),
        (ColumnType::String, Sentinel::String(v)) => Ok(Scalar::Str(v.as_str().into())),
        (ColumnType::Char, Sentinel::Char(v)) => Ok(or_null(i64::from(*v), i64::from(u16::MAX))),
        (ColumnType::Byte, Sentinel::Byte(v)) => Ok(or_null(i64::from(*v), i64::from(i8::MIN))),
        (ColumnType::Short, Sentinel::Short(v)) => {
            Ok(or_null(i64::from(*v), i64::from(i16::MIN)))
        }
        (ColumnType::Int, Sentinel::Int(v)) => Ok(or_null(i64::from(*v), i64::from(i32::MIN))),
        (ColumnType::Long, Sentinel::Long(v)) => Ok(or_null(*v, i64::MIN)),
        (ColumnType::Float, Sentinel::Float(v)) if *v == -f32::MAX => Ok(Scalar::Null),
        (ColumnType::Float, Sentinel::Float(v)) => Ok(Scalar::Double(f64::from(*v))),
        (ColumnType::Double, Sentinel::Double(v)) if *v == -f64::MAX => Ok(Scalar::Null),
        (ColumnType::Double, Sentinel::Double(v)) => Ok(Scalar::Double(*v)),
        (ColumnType::Byte, _) if sentinel.double().is_some() => {
            integer_in(i64::from(i8::MIN) + 1, i64::from(i8::MAX))
        }
        (ColumnType::Short, _) if sentinel.double().is_some() => {
            integer_in(i64::from(i16::MIN) + 1, i64::from(i16::MAX))
        }
        (ColumnType::Int, _) if sentinel.double().is_some() => {
            integer_in(i64::from(i32::MIN) + 1, i64::from(i32::MAX))
        }
        (ColumnType::Long, _) if sentinel.double().is_some() => {
            integer_in(i64::MIN + 1, i64::MAX)
        }
        (ColumnType::Float | ColumnType::Double, _) if sentinel.double().is_some() => {
            let value = sentinel.double().expect("a number");
            if !value.is_finite() {
                return Err(AggregateError::InvalidSentinel(format!(
                    "For result column {output} the Non Unique Sentinel {} is infinite or NaN",
                    sentinel.text()
                )));
            }
            // An integer always fits. Its conversion may round, as Java's does.
            if *column_type == ColumnType::Double {
                return Ok(Scalar::Double(value));
            }
            // `MIN_FINITE_FLOAT` is the value above `-Float.MAX_VALUE`, which is the null.
            let least = f64::from(f32::from_bits((-f32::MAX).to_bits() - 1));
            if value < least || value > f64::from(f32::MAX) {
                return out_of_range();
            }
            Ok(Scalar::Double(f64::from(value as f32)))
        }
        _ => Err(AggregateError::InvalidSentinel(format!(
            "For result column {output} the Non Unique Sentinel must be of type {} but is {}",
            boxed_class(column_type),
            sentinel.java_class()
        ))),
    }
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
#[derive(Clone, PartialEq, Debug)]
#[non_exhaustive]
pub enum Aggregation {
    /// `spec` applied to each pair.
    Columns { spec: AggSpec, pairs: Vec<Pair> },
    /// The number of rows of each group, as a `long` column named `column`.
    Count { column: String },
    /// The number of rows of each group that pass every filter, as a `long` column named
    /// `column`. The filters are filter strings as `where` takes them. See [`CountWhere`].
    ///
    /// Behavior: io.deephaven.api.agg.CountWhere, as
    /// io.deephaven.api.agg.Aggregation#AggCountWhere(String, String...) makes one
    CountWhere {
        column: String,
        filters: Vec<String>,
    },
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

    pub fn count_where(
        column: impl Into<String>,
        filters: impl IntoIterator<Item = impl Into<String>>,
    ) -> Aggregation {
        Aggregation::CountWhere {
            column: column.into(),
            filters: filters.into_iter().map(Into::into).collect(),
        }
    }
}

/// Why an aggregation could not be made. Java reports all but `Failed` as an
/// `IllegalArgumentException` or a `NoSuchColumnException`, which a client sees as
/// `INVALID_ARGUMENT`.
#[derive(Clone, PartialEq, Debug)]
pub enum AggregateError {
    /// No aggregation was given.
    NoAggregations,
    /// A `group_by` of a table whose columns are all grouped by.
    NothingToAggregate,
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
    /// A percentile outside `[0, 1]`.
    InvalidPercentile(String),
    /// A sorted first or last without sort columns.
    NoSortColumns,
    /// A non-unique sentinel that is not a value of the column's type.
    InvalidSentinel(String),
    /// A filter of a count where that is not a filter of the table, or that reads other
    /// rows (`Column_`) or the row's position or key (`i`, `ii`, `k`).
    InvalidFilter(String),
    /// The aggregation is not defined for a blink table. Java reports this one as an
    /// `UnsupportedOperationException`.
    BlinkUnsupported(AggSpec),
    /// The table has failed, or could not be read.
    Failed(TableFailure),
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateError::NoAggregations => {
                write!(
                    f,
                    "aggBy must have at least one aggregation, none specified"
                )
            }
            AggregateError::NothingToAggregate => {
                write!(f, "aggAllBy has no columns to aggregate")
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
            AggregateError::InvalidPercentile(percentile) => {
                write!(f, "percentile {percentile} is not between 0 and 1")
            }
            AggregateError::NoSortColumns => write!(f, "columns() must be non-empty"),
            AggregateError::InvalidSentinel(message) | AggregateError::InvalidFilter(message) => {
                f.write_str(message)
            }
            AggregateError::BlinkUnsupported(spec) => write!(
                f,
                "Blink tables do not support {spec:?}; use BlinkTableTools.blinkToAppendOnly \
                 to accumulate full history"
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

fn is_integer(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Byte
            | ColumnType::Char
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
    )
}

fn is_floating(column_type: &ColumnType) -> bool {
    matches!(column_type, ColumnType::Float | ColumnType::Double)
}

/// The operator for `spec` on column `input` of `parent`, or `None` if Java has none for
/// the column's type.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor's `NormalConverter`:
/// `visit(AggSpecSum)`, `visit(AggSpecAbsSum)` / `makeSumOperator`, `visit(AggSpecAvg)` /
/// `makeAvgOperator`, `visit(AggSpecMin)`, `visit(AggSpecMax)` / `makeMinOrMaxOperator`,
/// `visit(AggSpecFirst)`, `visit(AggSpecLast)` / `addFirstOrLastOperators`,
/// `visit(AggSpecVar)`, `visit(AggSpecStd)` / `makeVarOrStdOperator`,
/// `visit(AggSpecMedian)`, `visit(AggSpecPercentile)`, `visit(AggSpecCountDistinct)` /
/// `makeCountDistinctOperator`, `visit(AggSpecDistinct)` / `makeDistinctOperator`,
/// `visit(AggSpecUnique)` / `makeUniqueOperator`,
/// `visit(AggSpecWAvg)`, `visit(AggSpecWSum)` / `addWeightedAvgOrSumOperator`,
/// `visit(AggSpecSortedFirst)`, `visit(AggSpecSortedLast)` / `addSortedFirstOrLastOperator`,
/// and `visit(AggSpecGroup)`, which takes a column of any type and refuses a blink table
/// (`unsupportedForBlinkTables`)
fn make_operator(
    spec: &AggSpec,
    parent: &Arc<dyn Table>,
    input: usize,
    kind: ParentKind,
    keyed: bool,
    output: &str,
) -> Result<Option<Planned>, AggregateError> {
    let definition = parent.definition();
    let column = |name: &str| {
        definition
            .index_of(name)
            .ok_or_else(|| AggregateError::UnknownColumn(name.to_string()))
    };
    let input_type = &definition.columns()[input].column_type;
    let integer = is_integer(input_type);
    let floating = is_floating(input_type);
    // The sums, averages, variances and weighted aggregations have no operator for a
    // vector or an array, as for a `String`. Every other aggregation takes one in Java, by
    // its comparison and storage of objects, and the operators here read cells of the
    // scalar types only.
    let no_lists = |java_method: &str, column_type: &ColumnType| {
        if column_type.list_element().is_some() {
            unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor",
                "{java_method} for a column of type {column_type:?}"
            );
        }
    };
    match spec {
        AggSpec::Min | AggSpec::Max => no_lists("makeMinOrMaxOperator", input_type),
        AggSpec::Distinct { .. } => no_lists("makeDistinctOperator", input_type),
        AggSpec::Median { .. } | AggSpec::Percentile { .. } => {
            no_lists("visit(AggSpecPercentile)", input_type);
        }
        AggSpec::CountDistinct { .. } => no_lists("makeCountDistinctOperator", input_type),
        AggSpec::Unique { .. } => no_lists("makeUniqueOperator", input_type),
        AggSpec::SortedFirst { columns } | AggSpec::SortedLast { columns } => {
            no_lists("addSortedFirstOrLastOperator", input_type);
            for name in columns {
                no_lists(
                    "addSortedFirstOrLastOperator",
                    &definition.columns()[column(name)?].column_type,
                );
            }
        }
        _ => {}
    }
    let op: Box<dyn AggOperator> = match spec {
        AggSpec::Group => {
            if kind == ParentKind::Blink {
                return Err(AggregateError::BlinkUnsupported(spec.clone()));
            }
            // The operator reads the column itself, by the rows of each group, so the
            // framework need not decode it: it is told when the column is modified.
            return Ok(Some(Planned {
                reads: vec![input],
                ..Planned::new(Box::new(Group::new(parent.column(input), input_type)), None)
            }));
        }
        AggSpec::Sum | AggSpec::AbsSum => {
            let absolute = *spec == AggSpec::AbsSum;
            match input_type {
                ColumnType::Boolean => Box::<BooleanSum>::default(),
                ColumnType::Float => Box::new(FloatSum::new(true, absolute)),
                ColumnType::Double => Box::new(FloatSum::new(false, absolute)),
                _ if integer => Box::new(LongSum::new(absolute)),
                _ => return Ok(None),
            }
        }
        AggSpec::Avg => match input_type {
            _ if floating => Box::<FloatAvg>::default(),
            _ if integer => Box::<LongAvg>::default(),
            _ => return Ok(None),
        },
        AggSpec::Min | AggSpec::Max => Box::new(MinMax::new(
            input_type.clone(),
            *spec == AggSpec::Min,
            kind == ParentKind::General,
        )),
        AggSpec::First | AggSpec::Last => {
            let mode = match kind {
                ParentKind::Static | ParentKind::AddOnly => FirstLastMode::ByRowKey,
                ParentKind::Blink => FirstLastMode::Arrival,
                ParentKind::General => FirstLastMode::Tracked,
            };
            // Behavior: AggregationProcessor#addFirstOrLastOperators gives the operator
            // of a table that is not add-only no input columns, and
            // ChunkedOperatorAggregationHelper.OperatorDivision then hands it the modified
            // rows whatever column changed. So the operator watches every column.
            let reads: Vec<usize> = match mode {
                FirstLastMode::Tracked => (0..definition.columns().len()).collect(),
                _ => vec![input],
            };
            if input_type.list_element().is_some() {
                // The cells are read by row key, so the framework need not decode the
                // column.
                let op = FirstLastCell::new(
                    parent.column(input),
                    input_type.clone(),
                    *spec == AggSpec::First,
                    mode,
                );
                return Ok(Some(Planned {
                    reads,
                    ..Planned::new(Box::new(op), None)
                }));
            }
            let op = FirstLast::new(input_type.clone(), *spec == AggSpec::First, mode);
            return Ok(Some(Planned {
                reads,
                ..Planned::new(Box::new(op), Some(input))
            }));
        }
        AggSpec::Var | AggSpec::Std if integer || floating => {
            Box::new(Var::new(*spec == AggSpec::Std, floating))
        }
        AggSpec::Var | AggSpec::Std => return Ok(None),
        AggSpec::Median {
            average_evenly_divided,
        } => Box::new(SortedValues::percentile(
            input_type.clone(),
            0.5,
            *average_evenly_divided,
        )),
        AggSpec::Percentile {
            percentile,
            average_evenly_divided,
        } => {
            // Behavior: io.deephaven.api.agg.spec.AggSpecPercentile#checkPercentile
            if !(0.0..=1.0).contains(percentile) {
                return Err(AggregateError::InvalidPercentile(percentile.to_string()));
            }
            Box::new(SortedValues::percentile(
                input_type.clone(),
                *percentile,
                *average_evenly_divided,
            ))
        }
        AggSpec::CountDistinct { count_nulls } => Box::new(SortedValues::count_distinct(
            input_type.clone(),
            *count_nulls,
        )),
        AggSpec::Unique {
            include_nulls,
            non_unique,
        } => Box::new(SortedValues::unique(
            input_type.clone(),
            *include_nulls,
            sentinel_value(input_type, non_unique, output)?,
        )),
        AggSpec::Distinct { include_nulls } => Box::new(SortedValues::distinct(
            input_type.clone(),
            *include_nulls,
            !keyed,
        )),
        AggSpec::WeightedAvg { weight } | AggSpec::WeightedSum { weight } => {
            let weight = column(weight)?;
            let weight_type = &definition.columns()[weight].column_type;
            if !(is_integer(weight_type) || is_floating(weight_type)) {
                return Err(AggregateError::UnsupportedType {
                    spec: spec.clone(),
                    column: definition.columns()[weight].name.clone(),
                    column_type: weight_type.clone(),
                });
            }
            if !(integer || floating) {
                return Ok(None);
            }
            let sum = matches!(spec, AggSpec::WeightedSum { .. });
            let op: Box<dyn AggOperator> = if sum && integer && is_integer(weight_type) {
                Box::new(WeightedLongSum::new(input_type, weight_type))
            } else {
                Box::new(WeightedFloat::new(!sum))
            };
            return Ok(Some(Planned {
                weight: Some(weight),
                ..Planned::new(op, Some(input))
            }));
        }
        AggSpec::SortedFirst { columns } | AggSpec::SortedLast { columns } => {
            // Behavior: io.deephaven.api.agg.spec.AggSpecSortedFirst#nonEmptyColumns
            if columns.is_empty() {
                return Err(AggregateError::NoSortColumns);
            }
            if columns.len() > 3 {
                unported!(
                    "io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#addSortedFirstOrLastOperator",
                    "more than three sort columns, which Java compares as an ArrayTuple"
                );
            }
            if kind == ParentKind::Blink {
                unported!(
                    "io.deephaven.engine.table.impl.by.DoubleBlinkSortedFirstOrLastChunkedOperator",
                    "sorted first or last of a blink table"
                );
            }
            let mut sort = Vec::with_capacity(columns.len());
            let mut reads = Vec::with_capacity(columns.len() + 1);
            for name in columns {
                let index = column(name)?;
                sort.push(SortColumn {
                    source: parent.column(index),
                    column_type: definition.columns()[index].column_type.clone(),
                });
                reads.push(index);
            }
            reads.push(input);
            let op = Box::new(SortedFirstLast::new(
                matches!(spec, AggSpec::SortedFirst { .. }),
                sort,
                parent.column(input),
                input_type.clone(),
            ));
            return Ok(Some(Planned {
                reads,
                ..Planned::new(op, None)
            }));
        }
    };
    Ok(Some(Planned::new(op, Some(input))))
}

/// Groups the rows of `parent` by the `group_by` columns and aggregates each group.
///
/// The result has the group-by columns, then one column per aggregation output in the
/// order given. An output with the name of a group-by column takes that column's place:
/// the result then has the aggregated values where the key was, and not the key, though
/// the rows are still grouped by it. It has one row per group that has rows, in the order the groups were first
/// seen; a group that empties and fills again returns to its place. With no group-by
/// columns the whole table is one group, and the result has one row, or none if the table
/// is empty. With `preserve_empty` a group keeps its row when it loses its last row, and
/// the aggregation without keys always has its row.
///
/// The result is static if `parent` is, and otherwise changes with it.
///
/// A [`Aggregation::CountWhere`] needs a formula compiler for its filters and is refused
/// here; [`aggregate_with`] takes one.
///
/// Not ported: `initialGroups`.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#aggBy and #aggNoMemo, over
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper#aggregation
pub fn aggregate(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    group_by: &[&str],
) -> Result<Arc<dyn Table>, AggregateError> {
    aggregate_inner(parent, aggregations, preserve_empty, group_by, None)
}

/// [`aggregate`], with the compiler of the formulas among the filters of a
/// [`Aggregation::CountWhere`].
pub fn aggregate_with(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    group_by: &[&str],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, AggregateError> {
    aggregate_inner(parent, aggregations, preserve_empty, group_by, Some(compiler))
}

/// The operator of a count where.
///
/// Java refuses a formula that uses `i`, `ii` or `k`, and fails to make its filter of one
/// that reads a column as an array. Without filters Java's count is not defined: with key
/// columns it counts the flags a pooled chunk was left with (observed: 3, 1, 1, 2 for
/// groups of 3, 2, 1 and 2 rows), and without it counts nothing.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#addCountWhereOperator,
/// io.deephaven.engine.table.impl.by.CountWhereOperator#CountWhereOperator
fn count_where(
    parent: &Arc<dyn Table>,
    filters: &[String],
    keyed: bool,
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<Planned, AggregateError> {
    let Some(compiler) = compiler else {
        return Err(AggregateError::InvalidFilter(
            "a count where needs a formula compiler: use aggregate_with".to_string(),
        ));
    };
    if filters.is_empty() {
        unported!(
            "io.deephaven.engine.table.impl.by.CountWhereOperator#applyFilters",
            "a count where without filters, which in Java counts the contents of an unset chunk"
        );
    }
    let definition = parent.definition();
    let filters =
        RowFilters::parse(filters, definition, compiler).map_err(AggregateError::InvalidFilter)?;
    if filters.uses_row_variables() {
        return Err(AggregateError::InvalidFilter(
            "Count-where does not support filters that reference virtual row variables (i, ii, k)"
                .to_string(),
        ));
    }
    if filters.reads_arrays() {
        return Err(AggregateError::InvalidFilter(
            "Error creating condition filter in CountWhereOperator".to_string(),
        ));
    }
    let reads = filters.columns().to_vec();
    if let Some(list) = reads
        .iter()
        .map(|&column| &definition.columns()[column].column_type)
        .find(|column_type| column_type.list_element().is_some())
    {
        unported!(
            "io.deephaven.engine.table.impl.select.WhereFilter#filter",
            "a count where filter of a column of type {list:?}"
        );
    }
    let op = CountWhere::new(parent.clone(), filters, keyed);
    Ok(Planned {
        reads,
        ..Planned::new(Box::new(op), None)
    })
}

fn aggregate_inner(
    parent: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    preserve_empty: bool,
    group_by: &[&str],
    compiler: Option<&dyn FormulaCompiler>,
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
    let keyed = !key_columns.is_empty();
    let mut layout = Layout {
        keys: (0..key_columns.len()).map(Some).collect(),
        ops: Vec::new(),
    };
    let mut operators: Vec<Planned> = Vec::new();
    let mut outputs: Vec<&str> = Vec::new();
    let mut planned: Vec<(&str, Planned)> = Vec::new();
    for aggregation in aggregations {
        match aggregation {
            Aggregation::Count { column } => {
                planned.push((column, Planned::new(Box::<Count>::default(), None)));
            }
            Aggregation::CountWhere { column, filters } => {
                planned.push((column, count_where(parent, filters, keyed, compiler)?));
            }
            Aggregation::Columns { spec, pairs } => {
                for pair in pairs {
                    let index = column(&pair.input)?;
                    let operator =
                        make_operator(spec, parent, index, kind, keyed, &pair.output)?.ok_or_else(
                            || AggregateError::UnsupportedType {
                                spec: spec.clone(),
                                column: pair.input.clone(),
                                column_type: definition.columns()[index].column_type.clone(),
                            },
                        )?;
                    planned.push((&pair.output, operator));
                }
            }
        }
    }
    for (name, operator) in planned {
        if outputs.contains(&name) {
            return Err(AggregateError::DuplicateOutput(name.to_string()));
        }
        outputs.push(name);
        let output = ColumnDefinition::new(name, operator.op.result_type());
        // Behavior: AggregationContext#getResultColumns; see `Layout`.
        let key = key_columns
            .iter()
            .position(|&index| definition.columns()[index].name == name);
        match key.and_then(|key| layout.keys[key].take()) {
            Some(place) => {
                result_columns[place] = output;
                layout.ops.push(place);
            }
            None => {
                layout.ops.push(result_columns.len());
                result_columns.push(output);
            }
        }
        operators.push(operator);
    }
    let result_definition = TableDefinition::new(result_columns)
        .map_err(|duplicate| AggregateError::DuplicateOutput(duplicate.0))?;

    let flat = match kind {
        ParentKind::Static | ParentKind::AddOnly | ParentKind::Blink => true,
        ParentKind::General => !keyed,
    };
    let table = QueryTable::derive(parent, || {
        let mut aggregator =
            Aggregator::new(parent.clone(), key_columns, operators, preserve_empty, layout)?;
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

/// Groups the rows of `parent` by the `group_by` columns. The result has the group-by
/// columns, then every other column of `parent` as a vector of each group's values: the
/// `group` aggregation ([`AggSpec::Group`]) of each. A table whose columns are all grouped
/// by has nothing to aggregate, which Java refuses.
///
/// The gRPC handler has its own form of this, which also leaves out formatting columns.
///
/// Behavior: io.deephaven.api.TableOperationsDefaults#groupBy(Collection), which is
/// io.deephaven.engine.table.impl.QueryTable#aggAllBy with `AggSpec.group()`
pub fn group_by(
    parent: &Arc<dyn Table>,
    group_by: &[&str],
) -> Result<Arc<dyn Table>, AggregateError> {
    let definition = parent.definition();
    if let Some(unknown) = group_by
        .iter()
        .find(|name| definition.index_of(name).is_none())
    {
        return Err(AggregateError::UnknownColumn(unknown.to_string()));
    }
    let pairs: Vec<Pair> = definition
        .columns()
        .iter()
        .filter(|column| !group_by.contains(&column.name.as_str()))
        .map(|column| Pair::of(column.name.as_str()))
        .collect();
    if pairs.is_empty() {
        return Err(AggregateError::NothingToAggregate);
    }
    aggregate(
        parent,
        &[Aggregation::columns(AggSpec::Group, pairs)],
        false,
        group_by,
    )
}
