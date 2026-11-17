//! The filter kinds: how each is checked against a table definition and how it selects rows.
//!
//! A [`FilterSpec`] is a filter as written, before it has seen a table. [`resolve`] checks it
//! against a definition, as Java's `WhereFilter#init` does, and gives a [`Resolved`] filter
//! that can select rows.

use super::literal::{convert_value, java_equals_ignore_case, Key};
use super::pattern::{full_match, Matcher};
use super::search;
use crate::ops::zoned;
use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanArray, PrimitiveArray, StringArray,
};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::{
    DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type, Int16Type, Int32Type,
    Int64Type, Int8Type, Time64NanosecondType, TimeUnit, TimestampNanosecondType, UInt16Type,
};
use oxhaven_column::parallel;
use oxhaven_column::sentinel::sentinels_to_nulls;
use oxhaven_contracts::expr::{Formula, FormulaCompiler};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_rowset::RowSetBuilderSequential;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// Behavior: io.deephaven.engine.table.MatchOptions
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub(super) struct MatchOptions {
    /// Keep the rows that match none of the values.
    pub inverted: bool,
    /// Compare strings as `String#equalsIgnoreCase` does.
    pub ignore_case: bool,
    /// NaN equals NaN. Without it, floating point values compare as IEEE 754 says.
    pub nan_match: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RangeOp {
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl RangeOp {
    /// The operator with its operands exchanged.
    ///
    /// Behavior: io.deephaven.gui.table.filters.Condition#mirror
    pub(super) fn mirror(self) -> Self {
        match self {
            RangeOp::Less => RangeOp::Greater,
            RangeOp::LessOrEqual => RangeOp::GreaterOrEqual,
            RangeOp::Greater => RangeOp::Less,
            RangeOp::GreaterOrEqual => RangeOp::LessOrEqual,
        }
    }
}

/// What a pattern filter looks for, as written.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(super) enum PatternSpec {
    /// Any of the strings occurs.
    Any(Vec<String>),
    /// All of the strings occur on one line; see [`Matcher::All`].
    All(Vec<String>),
    /// The whole value matches a regular expression in Java's syntax.
    Regex(String),
}

/// A filter before it is checked against a table.
#[derive(Clone, PartialEq, Debug)]
pub(super) enum FilterSpec {
    /// Behavior: io.deephaven.engine.table.impl.select.MatchFilter
    Match {
        column: String,
        /// The values as text; they are converted for the column's type by [`resolve`].
        values: Vec<String>,
        options: MatchOptions,
        /// The formula to use instead if the column or a value does not fit.
        failover: Option<String>,
    },
    /// Behavior: io.deephaven.engine.table.impl.select.RangeFilter
    Range {
        column: String,
        op: RangeOp,
        value: String,
        /// The formula to use instead if the column or the value does not fit.
        failover: Option<String>,
    },
    /// Behavior: io.deephaven.engine.table.impl.select.WhereFilterPatternImpl
    Pattern {
        column: String,
        pattern: PatternSpec,
        ignore_case: bool,
        /// Keep the non-null values the pattern does not find.
        invert_pattern: bool,
    },
    /// Behavior: io.deephaven.engine.table.impl.select.ConditionFilter
    Condition(String),
    /// Behavior: io.deephaven.engine.table.impl.select.ConjunctiveFilter
    And(Vec<FilterSpec>),
    /// Behavior: io.deephaven.engine.table.impl.select.DisjunctiveFilter
    Or(Vec<FilterSpec>),
    /// A quick filter: the text searched for in every column, or in those named. Which
    /// columns can hold the text is known only with the table; see [`search::expand`].
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.WhereFilterFactory#expandQuickFilter
    Search { text: String, columns: Vec<String> },
}

/// A filter that has been checked against a table definition.
pub(super) enum Resolved {
    Match {
        column: usize,
        keys: Vec<Key>,
        options: MatchOptions,
    },
    Range {
        column: usize,
        op: RangeOp,
        pivot: Key,
    },
    /// The values from `lower` to `upper`, each end included or not.
    Between {
        column: usize,
        lower: Key,
        upper: Key,
        lower_inclusive: bool,
        upper_inclusive: bool,
    },
    Pattern {
        column: usize,
        matcher: Matcher,
        invert_pattern: bool,
    },
    /// No row.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.WhereNoneFilter
    Nothing,
    Condition {
        formula: Arc<dyn Formula>,
        text: String,
    },
    And(Vec<Resolved>),
    Or(Vec<Resolved>),
}

fn no_column(name: &str, definition: &TableDefinition) -> String {
    let names: Vec<&str> = definition
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    format!(
        "Column \"{name}\" doesn't exist in this table, available columns: [{}]",
        names.join(", ")
    )
}

/// Behavior: io.deephaven.engine.table.impl.select.AbstractConditionFilter#init and
/// its `checkReturnType`
fn resolve_condition(
    text: &str,
    definition: &TableDefinition,
    compiler: &dyn FormulaCompiler,
) -> Result<Resolved, String> {
    let expr = compiler.parse(text).map_err(|e| e.to_string())?;
    let formula = compiler
        .compile(&expr, definition)
        .map_err(|e| e.to_string())?;
    if *formula.result_type() != ColumnType::Boolean {
        return Err(format!(
            "Invalid condition filter expression type: boolean required. Formula: {text}, \
             expression type: {:?}",
            formula.result_type()
        ));
    }
    Ok(Resolved::Condition {
        formula,
        text: text.to_string(),
    })
}

/// A simple filter that does not fit the table becomes its formula, if it has one and the
/// formula compiles; otherwise the original problem is the error.
fn or_failover(
    simple: Result<Resolved, String>,
    failover: Option<&String>,
    definition: &TableDefinition,
    compiler: &dyn FormulaCompiler,
) -> Result<Resolved, String> {
    match (simple, failover) {
        (Ok(resolved), _) => Ok(resolved),
        (Err(problem), None) => Err(problem),
        (Err(problem), Some(text)) => {
            resolve_condition(text, definition, compiler).map_err(|_| problem)
        }
    }
}

/// Behavior: io.deephaven.engine.table.impl.select.MatchFilter#init
fn resolve_match(
    column: &str,
    values: &[String],
    options: MatchOptions,
    definition: &TableDefinition,
    compiler: &dyn FormulaCompiler,
) -> Result<Resolved, String> {
    let mut values = values.to_vec();
    let index = match definition.index_of(column) {
        Some(index) => index,
        None => match values.as_slice() {
            // The column and the value were written the other way round.
            [only] if definition.index_of(only).is_some() => {
                let index = definition.index_of(only).ok_or("unreachable")?;
                values[0] = column.to_string();
                index
            }
            _ => return Err(no_column(column, definition)),
        },
    };
    let column = &definition.columns()[index];
    let keys = values
        .iter()
        .map(|value| convert_value(column, definition, value, compiler))
        .collect::<Result<_, _>>()?;
    Ok(Resolved::Match {
        column: index,
        keys,
        options,
    })
}

/// Behavior: io.deephaven.engine.table.impl.select.RangeFilter#init
fn resolve_range(
    column: &str,
    op: RangeOp,
    value: &str,
    definition: &TableDefinition,
    compiler: &dyn FormulaCompiler,
) -> Result<Resolved, String> {
    let (index, op, value) = match (definition.index_of(column), definition.index_of(value)) {
        (Some(index), _) => (index, op, value),
        (None, Some(index)) => (index, op.mirror(), column),
        (None, None) => return Err(no_column(column, definition)),
    };
    let column = &definition.columns()[index];
    let pivot = convert_value(column, definition, value, compiler)?;
    // Behavior: RangeFilter#init makes a filter for the primitives, `Instant`, the local
    // dates and times, `ZonedDateTime`, the big numbers, `String` and `Boolean`; any other
    // type, a duration and a day of the week among them, is refused (and the caller falls
    // back to the filter's formula).
    let class = match column.column_type {
        ColumnType::Duration => Some("Duration"),
        ColumnType::DayOfWeek => Some("DayOfWeek"),
        ColumnType::Period => Some("Period"),
        ColumnType::ZoneId => Some("ZoneId"),
        ColumnType::Table(TableClass::QueryTable) => Some("QueryTable"),
        ColumnType::Table(_) => Some("Table"),
        _ => None,
    };
    if let Some(class) = class {
        return Err(format!(
            "RangeFilter does not support type {class} for column {}",
            column.name
        ));
    }
    Ok(Resolved::Range {
        column: index,
        op,
        pivot,
    })
}

/// Behavior: io.deephaven.engine.table.impl.select.WhereFilterPatternImpl#init
fn resolve_pattern(
    column: &str,
    pattern: &PatternSpec,
    ignore_case: bool,
    invert_pattern: bool,
    definition: &TableDefinition,
) -> Result<Resolved, String> {
    let index = definition.index_of(column).ok_or_else(|| {
        format!(
            "Column '{column}' doesn't exist in this table: {}",
            no_column(column, definition)
        )
    })?;
    let column_type = &definition.columns()[index].column_type;
    if *column_type != ColumnType::String {
        return Err(format!(
            "Column '{column}', type {column_type:?}, is not a CharSequence"
        ));
    }
    let matcher = match pattern {
        PatternSpec::Any(needles) => Matcher::any(needles.clone(), ignore_case),
        PatternSpec::All(needles) => Matcher::all(needles.clone(), ignore_case),
        PatternSpec::Regex(regex) => full_match(regex, ignore_case)?,
    };
    Ok(Resolved::Pattern {
        column: index,
        matcher,
        invert_pattern,
    })
}

/// Checks `spec` against `definition` and prepares it to select rows.
///
/// A conjunction of one filter is that filter, and a conjunction inside a conjunction is
/// merged into it; likewise for disjunctions.
///
/// Behavior: the `init` of each `WhereFilter`, and
/// io.deephaven.engine.table.impl.select.ConjunctiveFilter#makeConjunctiveFilter,
/// io.deephaven.engine.table.impl.select.DisjunctiveFilter#makeDisjunctiveFilter
pub(super) fn resolve(
    spec: &FilterSpec,
    definition: &TableDefinition,
    compiler: &dyn FormulaCompiler,
) -> Result<Resolved, String> {
    let resolve_all = |specs: &[FilterSpec]| -> Result<Vec<Resolved>, String> {
        specs
            .iter()
            .map(|spec| resolve(spec, definition, compiler))
            .collect()
    };
    match spec {
        FilterSpec::Match {
            column,
            values,
            options,
            failover,
        } => or_failover(
            resolve_match(column, values, *options, definition, compiler),
            failover.as_ref(),
            definition,
            compiler,
        ),
        FilterSpec::Range {
            column,
            op,
            value,
            failover,
        } => or_failover(
            resolve_range(column, *op, value, definition, compiler),
            failover.as_ref(),
            definition,
            compiler,
        ),
        FilterSpec::Pattern {
            column,
            pattern,
            ignore_case,
            invert_pattern,
        } => resolve_pattern(column, pattern, *ignore_case, *invert_pattern, definition),
        FilterSpec::Condition(text) => resolve_condition(text, definition, compiler),
        FilterSpec::Search { text, columns } => search::expand(definition, text, columns),
        FilterSpec::And(specs) => {
            let mut parts = Vec::new();
            for part in resolve_all(specs)? {
                match part {
                    Resolved::And(inner) if specs.len() > 1 => parts.extend(inner),
                    other => parts.push(other),
                }
            }
            Ok(match parts.len() {
                1 if specs.len() == 1 => parts.remove(0),
                _ => Resolved::And(parts),
            })
        }
        FilterSpec::Or(specs) => {
            let mut parts = Vec::new();
            for part in resolve_all(specs)? {
                match part {
                    Resolved::Or(inner) if specs.len() > 1 => parts.extend(inner),
                    other => parts.push(other),
                }
            }
            Ok(match parts.len() {
                1 if specs.len() == 1 => parts.remove(0),
                _ => Resolved::Or(parts),
            })
        }
    }
}

/// The values of one column for some rows, with nulls as `None`. Cells holding the value
/// Java reserves for null are null.
enum Cells {
    Bool(BooleanArray),
    /// `byte`, `short`, `int`, `long`, `char`, and `Instant` as nanoseconds.
    Int(Vec<Option<i64>>),
    /// `float` widened, and `double`.
    Float(Vec<Option<f64>>),
    Str(StringArray),
    /// A vector or an array, which Java's filters see as an object: whether each cell is
    /// not null.
    Object(Vec<bool>),
    /// A `ZonedDateTime` column.
    Zoned(ArrayRef),
}

/// The cells of `chunk`, values of a column of `column_type`.
fn cells(column_type: &ColumnType, chunk: ArrayRef) -> Result<Cells, TableFailure> {
    // Java filters a vector or an array column as a column of objects, of which a match
    // filter can only hold null.
    match column_type {
        ColumnType::ZonedDateTime => return Ok(Cells::Zoned(chunk)),
        // A period has no convertor of its own: a match filter holds only null. Nor has a
        // table, which Java's `ObjectChunkMatchFilterFactory` would compare by identity.
        ColumnType::Period | ColumnType::Table(_) => {
            return Ok(Cells::Object(
                (0..chunk.len()).map(|row| chunk.is_valid(row)).collect(),
            ))
        }
        _ => {}
    }
    if column_type.list_element().is_some() {
        return Ok(Cells::Object(
            (0..chunk.len()).map(|row| chunk.is_valid(row)).collect(),
        ));
    }
    // A date, a time of day or a duration is an object in Java, null only by reference.
    let values = match chunk.data_type() {
        DataType::Date32 | DataType::Time64(_) | DataType::Duration(_) => chunk,
        _ => sentinels_to_nulls(&chunk)?,
    };
    macro_rules! widen {
        ($kind:ident, $arrow:ty, $to:ty) => {
            Cells::$kind(
                values
                    .as_primitive::<$arrow>()
                    .iter()
                    .map(|v| v.map(<$to>::from))
                    .collect(),
            )
        };
    }
    Ok(match values.data_type() {
        DataType::Boolean => Cells::Bool(values.as_boolean().clone()),
        DataType::Int8 => widen!(Int, Int8Type, i64),
        DataType::Int16 => widen!(Int, Int16Type, i64),
        DataType::Int32 => widen!(Int, Int32Type, i64),
        DataType::Int64 => widen!(Int, Int64Type, i64),
        DataType::UInt16 => widen!(Int, UInt16Type, i64),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => widen!(Int, TimestampNanosecondType, i64),
        DataType::Date32 => widen!(Int, Date32Type, i64),
        DataType::Time64(TimeUnit::Nanosecond) => widen!(Int, Time64NanosecondType, i64),
        DataType::Duration(TimeUnit::Nanosecond) => widen!(Int, DurationNanosecondType, i64),
        DataType::Float32 => widen!(Float, Float32Type, f64),
        DataType::Float64 => widen!(Float, Float64Type, f64),
        DataType::Utf8 => Cells::Str(values.as_string::<i32>().clone()),
        other => return Err(failure(format!("cannot filter a column of {other}"))),
    })
}

fn wrong_key(key: &Key) -> TableFailure {
    failure(format!("a filter value {key:?} does not fit its column"))
}

/// Behavior: `getBits` of io.deephaven.engine.table.impl.chunkfilter.DoubleChunkMatchFilterFactory:
/// the two zeros are one value and every NaN is one value.
fn nan_match_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

/// Whether each cell equals one of `keys`, before inversion.
///
/// Behavior: io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory#getChunkFilter
/// and the filters it makes: `==` for integer types and `char`,
/// DoubleChunkMatchFilterFactory (IEEE equality, or bit equality with NaN matching),
/// ObjectChunkMatchFilterFactory (`Objects.equals`), and StringChunkMatchFilterFactory
/// (`String#equalsIgnoreCase`).
fn matches(cells: &Cells, keys: &[Key], options: MatchOptions) -> Result<Vec<bool>, TableFailure> {
    match cells {
        Cells::Int(values) => {
            let keys: Vec<Option<i64>> = keys
                .iter()
                .map(|key| match key {
                    Key::Null => Ok(None),
                    Key::Int(value) => Ok(Some(*value)),
                    other => Err(wrong_key(other)),
                })
                .collect::<Result<_, _>>()?;
            Ok(values.iter().map(|value| keys.contains(value)).collect())
        }
        Cells::Float(values) => {
            let keys: Vec<Option<f64>> = keys
                .iter()
                .map(|key| match key {
                    Key::Null => Ok(None),
                    Key::Float(value) => Ok(Some(*value)),
                    other => Err(wrong_key(other)),
                })
                .collect::<Result<_, _>>()?;
            let equal = |a: &Option<f64>, b: &Option<f64>| match (a, b) {
                (None, None) => true,
                (Some(a), Some(b)) if options.nan_match => nan_match_bits(*a) == nan_match_bits(*b),
                (Some(a), Some(b)) => a == b,
                _ => false,
            };
            Ok(values
                .iter()
                .map(|value| keys.iter().any(|key| equal(value, key)))
                .collect())
        }
        Cells::Bool(values) => {
            let keys: Vec<Option<bool>> = keys
                .iter()
                .map(|key| match key {
                    Key::Null => Ok(None),
                    Key::Bool(value) => Ok(Some(*value)),
                    other => Err(wrong_key(other)),
                })
                .collect::<Result<_, _>>()?;
            Ok(values.iter().map(|value| keys.contains(&value)).collect())
        }
        Cells::Str(values) => {
            let keys: Vec<Option<&str>> = keys
                .iter()
                .map(|key| match key {
                    Key::Null => Ok(None),
                    Key::Str(value) => Ok(Some(value.as_str())),
                    other => Err(wrong_key(other)),
                })
                .collect::<Result<_, _>>()?;
            if !options.ignore_case {
                return Ok(values.iter().map(|value| keys.contains(&value)).collect());
            }
            // With up to three values Java calls `key.equalsIgnoreCase(cell)` on each key in
            // turn until one matches, so a null key throws when it is reached. With more it
            // uses a hash set that accepts a null key.
            let few = keys.len() <= 3;
            values
                .iter()
                .map(|value| {
                    for key in &keys {
                        let equal = match (key, value) {
                            (None, _) if few => {
                                return Err(failure(
                                    "java.lang.NullPointerException: a null value in a \
                                     case-insensitive match of up to three values",
                                ))
                            }
                            (None, None) => true,
                            (Some(key), Some(value)) => java_equals_ignore_case(key, value),
                            _ => false,
                        };
                        if equal {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })
                .collect()
        }
        // Behavior: ObjectChunkMatchFilterFactory: `Objects.equals` with each value, which
        // for a `ZonedDateTime` is java.time.ZonedDateTime#equals: the same instant in the
        // same zone.
        Cells::Zoned(values) => {
            let keys: Vec<Option<(i64, &str)>> = keys
                .iter()
                .map(|key| match key {
                    Key::Null => Ok(None),
                    Key::Zoned { nanos, zone } => Ok(Some((*nanos, zone.as_str()))),
                    other => Err(wrong_key(other)),
                })
                .collect::<Result<_, _>>()?;
            let (cells, instants, zones) = zoned::parts(values)?;
            Ok((0..cells.len())
                .map(|row| {
                    let value = cells
                        .is_valid(row)
                        .then(|| (instants[row], zones.value(row)));
                    keys.contains(&value)
                })
                .collect())
        }
        // Behavior: ObjectChunkMatchFilterFactory: `Objects.equals` with each value, and
        // `MatchFilter.ColumnTypeConvertorFactory#getConvertor` makes no value but null
        // for such a column.
        Cells::Object(valid) => {
            if let Some(key) = keys.iter().find(|key| **key != Key::Null) {
                return Err(wrong_key(key));
            }
            Ok(valid
                .iter()
                .map(|valid| !valid && !keys.is_empty())
                .collect())
        }
    }
}

/// Behavior: io.deephaven.util.compare.DoubleComparisons#compare for two non-null values:
/// NaN is greater than everything and equal to itself, and the two zeros are equal.
fn compare_floats(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
    }
}

/// Behavior: java.lang.String#compareTo, which orders by UTF-16 code units.
fn compare_strings(a: &str, b: &str) -> Ordering {
    if a.is_ascii() || b.is_ascii() {
        return a.cmp(b);
    }
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Whether each cell is on the side of `pivot` that `op` names. Null is the lowest value of
/// every type, so `<` and `<=` keep nulls. A floating point `>` or `>=` never keeps NaN.
///
/// Behavior: io.deephaven.engine.table.impl.select.RangeFilter#init and the filters it
/// makes: IntRangeFilter#lt/#leq/#gt/#geq with IntRangeComparator (and the other integer
/// types), CharRangeFilter with CharComparisons, DoubleRangeFilter and FloatRangeFilter with
/// DoubleRangeComparator and FloatRangeComparator, RangeFilter#makeInstantRangeFilter, and
/// SingleSidedComparableRangeFilter with ObjectComparisons.
fn in_range(cells: &Cells, op: RangeOp, pivot: &Key) -> Result<Vec<bool>, TableFailure> {
    let keep = |order: Ordering| match op {
        RangeOp::Less => order == Ordering::Less,
        RangeOp::LessOrEqual => order != Ordering::Greater,
        RangeOp::Greater => order == Ordering::Greater,
        RangeOp::GreaterOrEqual => order != Ordering::Less,
    };
    Ok(match cells {
        Cells::Int(values) => {
            let pivot = match pivot {
                Key::Null => None,
                Key::Int(value) => Some(*value),
                other => return Err(wrong_key(other)),
            };
            values.iter().map(|value| keep(value.cmp(&pivot))).collect()
        }
        Cells::Float(values) => {
            let pivot = match pivot {
                Key::Null => None,
                Key::Float(value) => Some(*value),
                other => return Err(wrong_key(other)),
            };
            let upward = matches!(op, RangeOp::Greater | RangeOp::GreaterOrEqual);
            values
                .iter()
                .map(|value| {
                    let order = match (value, pivot) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Less,
                        (Some(_), None) => Ordering::Greater,
                        (Some(value), Some(pivot)) => compare_floats(*value, pivot),
                    };
                    keep(order) && !(upward && value.is_some_and(f64::is_nan))
                })
                .collect()
        }
        Cells::Bool(values) => {
            let pivot = match pivot {
                Key::Null => None,
                Key::Bool(value) => Some(*value),
                other => return Err(wrong_key(other)),
            };
            values.iter().map(|value| keep(value.cmp(&pivot))).collect()
        }
        Cells::Str(values) => {
            let pivot = match pivot {
                Key::Null => None,
                Key::Str(value) => Some(value.as_str()),
                other => return Err(wrong_key(other)),
            };
            values
                .iter()
                .map(|value| {
                    keep(match (value, pivot) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Less,
                        (Some(_), None) => Ordering::Greater,
                        (Some(value), Some(pivot)) => compare_strings(value, pivot),
                    })
                })
                .collect()
        }
        // Behavior: SingleSidedComparableRangeFilter with ObjectComparisons and
        // java.time.chrono.ChronoZonedDateTime#compareTo: the instant, then the local date
        // and time (the offset, for one instant), then the zone's id.
        Cells::Zoned(values) => {
            let order_key =
                |nanos: i64, zone: &str| -> Result<(i64, i32, Vec<u16>), TableFailure> {
                    Ok((
                        nanos,
                        zoned::offset_seconds(zone, nanos)?,
                        zone.encode_utf16().collect(),
                    ))
                };
            let pivot = match pivot {
                Key::Null => None,
                Key::Zoned { nanos, zone } => Some(order_key(*nanos, zone)?),
                other => return Err(wrong_key(other)),
            };
            let (cells, instants, zones) = zoned::parts(values)?;
            (0..cells.len())
                .map(|row| {
                    let value = match cells.is_valid(row) {
                        true => Some(order_key(instants[row], zones.value(row))?),
                        false => None,
                    };
                    Ok(keep(value.cmp(&pivot)))
                })
                .collect::<Result<_, TableFailure>>()?
        }
        Cells::Object(_) => oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.select.RangeFilter#init",
            "a range filter of a vector or array column"
        ),
    })
}

/// Whether each cell lies between `lower` and `upper`, in the order of [`in_range`]: null
/// lowest and, for floating point values, NaN highest.
///
/// Behavior: io.deephaven.engine.table.impl.chunkfilter.DoubleRangeComparator#makeDoubleFilter,
/// `FloatRangeComparator#makeFloatFilter` and `LongRangeComparator#makeLongFilter`, with
/// io.deephaven.util.compare.DoubleComparisons and `LongComparisons`
fn between(
    cells: &Cells,
    lower: &Key,
    upper: &Key,
    lower_inclusive: bool,
    upper_inclusive: bool,
) -> Result<Vec<bool>, TableFailure> {
    let keep = |from_lower: Ordering, from_upper: Ordering| {
        (from_lower == Ordering::Greater || (lower_inclusive && from_lower == Ordering::Equal))
            && (from_upper == Ordering::Less || (upper_inclusive && from_upper == Ordering::Equal))
    };
    Ok(match cells {
        Cells::Int(values) => {
            let end = |key: &Key| match key {
                Key::Null => Ok(None),
                Key::Int(value) => Ok(Some(*value)),
                other => Err(wrong_key(other)),
            };
            let (lower, upper) = (end(lower)?, end(upper)?);
            values
                .iter()
                .map(|value| keep(value.cmp(&lower), value.cmp(&upper)))
                .collect()
        }
        Cells::Float(values) => {
            let end = |key: &Key| match key {
                Key::Null => Ok(None),
                Key::Float(value) => Ok(Some(*value)),
                other => Err(wrong_key(other)),
            };
            let (lower, upper) = (end(lower)?, end(upper)?);
            let order = |value: &Option<f64>, end: &Option<f64>| match (value, end) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(value), Some(end)) => compare_floats(*value, *end),
            };
            values
                .iter()
                .map(|value| keep(order(value, &lower), order(value, &upper)))
                .collect()
        }
        Cells::Bool(_) | Cells::Str(_) | Cells::Object(_) | Cells::Zoned(_) => {
            return Err(failure("a range of two ends needs a column of numbers"))
        }
    })
}

/// Whether each cell of a primitive array passes `test`, which gets the cell as the
/// generic path's [`Cells`] hold it: `None` for a null and for the value Java reserves for
/// null, the value widened otherwise.
fn typed_mask<P: ArrowPrimitiveType, V>(
    array: &PrimitiveArray<P>,
    null_value: P::Native,
    widen: impl Fn(P::Native) -> V,
    test: &impl Fn(Option<V>) -> bool,
) -> BooleanBuffer {
    let values = array.values();
    match array.nulls().filter(|nulls| nulls.null_count() > 0) {
        None => BooleanBuffer::collect_bool(values.len(), |row| {
            let value = values[row];
            test((value != null_value).then(|| widen(value)))
        }),
        Some(nulls) => BooleanBuffer::collect_bool(values.len(), |row| {
            let value = values[row];
            test((nulls.is_valid(row) && value != null_value).then(|| widen(value)))
        }),
    }
}

/// [`typed_mask`] for a chunk of any integer type, `char` or `Instant` with `ints`, or of
/// `float` or `double` with `floats`. `None` for a chunk of another type.
fn primitive_mask(
    chunk: &ArrayRef,
    ints: &impl Fn(Option<i64>) -> bool,
    floats: &impl Fn(Option<f64>) -> bool,
) -> Option<BooleanBuffer> {
    use oxhaven_column::sentinel::{
        NULL_BYTE, NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_INT, NULL_LONG, NULL_SHORT,
    };
    macro_rules! int {
        ($arrow:ty, $null:expr) => {
            typed_mask(chunk.as_primitive::<$arrow>(), $null, i64::from, ints)
        };
    }
    Some(match chunk.data_type() {
        DataType::Int8 => int!(Int8Type, NULL_BYTE),
        DataType::Int16 => int!(Int16Type, NULL_SHORT),
        DataType::Int32 => int!(Int32Type, NULL_INT),
        DataType::Int64 => int!(Int64Type, NULL_LONG),
        DataType::UInt16 => int!(UInt16Type, NULL_CHAR),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            int!(TimestampNanosecondType, NULL_LONG)
        }
        DataType::Float32 => typed_mask(
            chunk.as_primitive::<Float32Type>(),
            NULL_FLOAT,
            f64::from,
            floats,
        ),
        DataType::Float64 => typed_mask(
            chunk.as_primitive::<Float64Type>(),
            NULL_DOUBLE,
            |value| value,
            floats,
        ),
        _ => return None,
    })
}

fn int_key(key: &Key) -> Result<Option<i64>, TableFailure> {
    match key {
        Key::Null => Ok(None),
        Key::Int(value) => Ok(Some(*value)),
        other => Err(wrong_key(other)),
    }
}

fn float_key(key: &Key) -> Result<Option<f64>, TableFailure> {
    match key {
        Key::Null => Ok(None),
        Key::Float(value) => Ok(Some(*value)),
        other => Err(wrong_key(other)),
    }
}

/// The order of two floating point cells: null lowest, then [`compare_floats`].
fn float_order(value: Option<f64>, end: Option<f64>) -> Ordering {
    match (value, end) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(value), Some(end)) => compare_floats(value, end),
    }
}

/// The rows of one filter read as a column chunk are judged in pieces of this many: 2^16,
/// as Java's parallel `where` (`QueryTable.parallelWhereRowsPerSegment`).
const PIECE_ROWS: usize = 1 << 16;

impl Resolved {
    /// What [`matches`], [`in_range`] and [`between`] give for a chunk of a primitive type,
    /// without a value per cell in between: one typed loop over the chunk's values. `None`
    /// for a chunk of another type, a filter of another kind, and keys that do not fit, all
    /// of which the generic path judges.
    ///
    /// Behavior: as [`matches`], [`in_range`] and [`between`].
    pub(super) fn primitive_kernel(&self, chunk: &ArrayRef) -> Option<BooleanBuffer> {
        let is_float = matches!(chunk.data_type(), DataType::Float32 | DataType::Float64);
        // The keys the chunk's type does not use are never looked at.
        let ints = |key: &Key| if is_float { Ok(None) } else { int_key(key) };
        let floats = |key: &Key| if is_float { float_key(key) } else { Ok(None) };
        match self {
            Resolved::Range { op, pivot, .. } => {
                let op = *op;
                let keep = move |order: Ordering| match op {
                    RangeOp::Less => order == Ordering::Less,
                    RangeOp::LessOrEqual => order != Ordering::Greater,
                    RangeOp::Greater => order == Ordering::Greater,
                    RangeOp::GreaterOrEqual => order != Ordering::Less,
                };
                let upward = matches!(op, RangeOp::Greater | RangeOp::GreaterOrEqual);
                let (int_pivot, float_pivot) = (ints(pivot).ok()?, floats(pivot).ok()?);
                primitive_mask(
                    chunk,
                    &|value: Option<i64>| keep(value.cmp(&int_pivot)),
                    &|value: Option<f64>| {
                        keep(float_order(value, float_pivot))
                            && !(upward && value.is_some_and(f64::is_nan))
                    },
                )
            }
            Resolved::Between {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
                ..
            } => {
                let (lower_inclusive, upper_inclusive) = (*lower_inclusive, *upper_inclusive);
                let keep = move |from_lower: Ordering, from_upper: Ordering| {
                    (from_lower == Ordering::Greater
                        || (lower_inclusive && from_lower == Ordering::Equal))
                        && (from_upper == Ordering::Less
                            || (upper_inclusive && from_upper == Ordering::Equal))
                };
                let int_ends = (ints(lower).ok()?, ints(upper).ok()?);
                let float_ends = (floats(lower).ok()?, floats(upper).ok()?);
                primitive_mask(
                    chunk,
                    &|value: Option<i64>| keep(value.cmp(&int_ends.0), value.cmp(&int_ends.1)),
                    &|value: Option<f64>| {
                        keep(
                            float_order(value, float_ends.0),
                            float_order(value, float_ends.1),
                        )
                    },
                )
            }
            Resolved::Match { keys, options, .. } => {
                let options = *options;
                let int_keys: Vec<Option<i64>> =
                    keys.iter().map(ints).collect::<Result<_, _>>().ok()?;
                let float_keys: Vec<Option<f64>> =
                    keys.iter().map(floats).collect::<Result<_, _>>().ok()?;
                let equal = |a: Option<f64>, b: Option<f64>| match (a, b) {
                    (None, None) => true,
                    (Some(a), Some(b)) if options.nan_match => {
                        nan_match_bits(a) == nan_match_bits(b)
                    }
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                };
                primitive_mask(
                    chunk,
                    &|value: Option<i64>| int_keys.contains(&value) != options.inverted,
                    &|value: Option<f64>| {
                        float_keys.iter().any(|key| equal(value, *key)) != options.inverted
                    },
                )
            }
            _ => None,
        }
    }

    /// Whether this filter, one that reads one column, keeps each cell of `chunk`.
    fn mask(
        &self,
        column_type: &ColumnType,
        chunk: ArrayRef,
    ) -> Result<BooleanBuffer, TableFailure> {
        // A column of another type may be held as numbers too, and is not compared as one.
        let primitive = matches!(
            column_type,
            ColumnType::Byte
                | ColumnType::Short
                | ColumnType::Char
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::Instant
        );
        if let Some(mask) = primitive.then(|| self.primitive_kernel(&chunk)).flatten() {
            return Ok(mask);
        }
        self.generic_mask(column_type, chunk)
    }

    /// [`Resolved::mask`] through [`Cells`], for every type of column.
    pub(super) fn generic_mask(
        &self,
        column_type: &ColumnType,
        chunk: ArrayRef,
    ) -> Result<BooleanBuffer, TableFailure> {
        let cells = cells(column_type, chunk)?;
        Ok(match self {
            Resolved::Match { keys, options, .. } => matches(&cells, keys, *options)?
                .into_iter()
                .map(|m| m != options.inverted)
                .collect(),
            Resolved::Range { op, pivot, .. } => in_range(&cells, *op, pivot)?.into(),
            Resolved::Between {
                lower,
                upper,
                lower_inclusive,
                upper_inclusive,
                ..
            } => between(&cells, lower, upper, *lower_inclusive, *upper_inclusive)?.into(),
            // Behavior: WhereFilterPatternImpl#find, #findPatternInverted, #matches,
            // #matchesPatternInverted: a null value is never kept.
            Resolved::Pattern {
                matcher,
                invert_pattern,
                ..
            } => match cells {
                Cells::Str(values) => values
                    .iter()
                    .map(|value| value.is_some_and(|v| matcher.test(v) != *invert_pattern))
                    .collect(),
                _ => return Err(failure("a pattern filter needs a String column")),
            },
            _ => unreachable!("only a filter of one column has a mask"),
        })
    }

    /// The rows of `selection` that this filter, one that reads `column` alone, keeps.
    ///
    /// The column is read once, on the calling thread. Its chunk is then judged in pieces
    /// of [`PIECE_ROWS`] rows on the threads `oxhaven_column::parallel` allows; a piece
    /// reads no table and takes no lock. The pieces are joined in row order, and the
    /// failure reported is that of the first piece that fails, which holds the first row
    /// that fails: what one pass over the chunk reports.
    fn filter_column(
        &self,
        table: &dyn Table,
        column: usize,
        selection: &RowSet,
        use_prev: bool,
    ) -> Result<RowSet, TableFailure> {
        let source = table.column(column);
        let chunk = if use_prev {
            source.prev_chunk(selection)?
        } else {
            source.chunk(selection)?
        };
        let column_type = source.column_type();
        let pieces = parallel::pieces_of(chunk.len(), PIECE_ROWS);
        let masks = parallel::map_pieces(pieces.len(), |piece| {
            let (offset, length) = pieces[piece];
            let mask = self.mask(column_type, chunk.slice(offset, length))?;
            Ok(kept_of_piece(selection, offset, mask))
        });
        let masks = masks
            .into_iter()
            .collect::<Result<Vec<_>, TableFailure>>()?;
        Ok(join_pieces(selection, masks))
    }
}

/// What one piece of a selection keeps: the ranges of row keys, when the selection is one
/// range of keys and so a row's key is known from its position, or else the mask.
enum Kept {
    Ranges(Vec<(RowKey, RowKey)>),
    Mask(BooleanBuffer),
}

/// What the piece of `selection` that starts at position `offset` keeps, given whether it
/// keeps each of its rows.
fn kept_of_piece(selection: &RowSet, offset: usize, mask: BooleanBuffer) -> Kept {
    match selection.as_single_range() {
        Some(range) => {
            let first = range.first() + offset as RowKey;
            Kept::Ranges(
                mask.set_slices()
                    .map(|(start, end)| (first + start as RowKey, first + end as RowKey - 1))
                    .collect(),
            )
        }
        None => Kept::Mask(mask),
    }
}

/// The rows of `selection` its pieces keep, the pieces in row order.
fn join_pieces(selection: &RowSet, pieces: Vec<Kept>) -> RowSet {
    let mut builder = RowSetBuilderSequential::new();
    let mut keys = selection.iter();
    for piece in pieces {
        match piece {
            Kept::Ranges(ranges) => {
                for (first, last) in ranges {
                    builder.append_range(first, last);
                }
            }
            Kept::Mask(mask) => {
                // The mask leads: a zip led by the keys would take one key too many.
                for (keep, key) in mask.iter().zip(keys.by_ref()) {
                    if keep {
                        builder.append_key(key);
                    }
                }
            }
        }
    }
    builder.build()
}

/// The rows of `selection` whose entry in `mask` is true.
fn selected(selection: &RowSet, mask: &BooleanBuffer) -> RowSet {
    let pieces = parallel::pieces_of(mask.len(), PIECE_ROWS);
    let kept = parallel::map_pieces(pieces.len(), |piece| {
        let (offset, length) = pieces[piece];
        kept_of_piece(selection, offset, mask.slice(offset, length))
    });
    join_pieces(selection, kept)
}

impl Resolved {
    /// The rows of `selection` this filter accepts, read from the current values of
    /// `table`, or with `use_prev` from its previous values, `selection` then being rows of
    /// its previous row set.
    pub(super) fn filter(
        &self,
        table: &dyn Table,
        selection: &RowSet,
        use_prev: bool,
    ) -> Result<RowSet, TableFailure> {
        if selection.is_empty() {
            return Ok(RowSet::empty());
        }
        match self {
            Resolved::Match {
                column,
                keys,
                options,
            } => {
                // Behavior: ChunkMatchFilterFactory#getChunkFilter for no values.
                if keys.is_empty() {
                    return Ok(if options.inverted {
                        selection.clone()
                    } else {
                        RowSet::empty()
                    });
                }
                self.filter_column(table, *column, selection, use_prev)
            }
            Resolved::Range { column, .. }
            | Resolved::Between { column, .. }
            | Resolved::Pattern { column, .. } => {
                self.filter_column(table, *column, selection, use_prev)
            }
            Resolved::Nothing => Ok(RowSet::empty()),
            // Behavior: ConditionFilter.ChunkFilter#filter. The generated kernel assigns the
            // formula's value to a `boolean`, so a null `Boolean` throws.
            Resolved::Condition { formula, text } => {
                let values = formula.evaluate(table, selection, use_prev)?;
                let values = values.as_boolean_opt().ok_or_else(|| {
                    failure(format!("filter={{ {text} }} did not evaluate to booleans"))
                })?;
                if values.len() as u64 != selection.size() {
                    return Err(failure(format!(
                        "filter={{ {text} }} returned {} values for {} rows",
                        values.len(),
                        selection.size()
                    )));
                }
                if values.null_count() > 0 {
                    return Err(failure(format!(
                        "java.lang.NullPointerException encountered in filter={{ {text} }}: \
                         the condition evaluated to null"
                    )));
                }
                Ok(selected(selection, values.values()))
            }
            // Behavior: ConjunctiveFilter#andImpl
            Resolved::And(filters) => {
                let mut matched = selection.clone();
                for filter in filters {
                    matched = filter.filter(table, &matched, use_prev)?;
                }
                Ok(matched)
            }
            // Behavior: DisjunctiveFilter#orImpl. Each filter sees only the rows no earlier
            // filter accepted, and no filters at all accept everything.
            Resolved::Or(filters) => {
                let Some((first, rest)) = filters.split_first() else {
                    return Ok(selection.clone());
                };
                let mut matched = first.filter(table, selection, use_prev)?;
                for filter in rest {
                    if matched.size() == selection.size() {
                        break;
                    }
                    let remaining = selection.minus(&matched);
                    matched.insert(&filter.filter(table, &remaining, use_prev)?);
                }
                Ok(matched)
            }
        }
    }

    /// Whether the filter is a formula that uses `i`, `ii` or `k`.
    pub(super) fn uses_row_variables(&self) -> bool {
        match self {
            Resolved::Condition { formula, .. } => formula.uses_row_variables(),
            Resolved::And(filters) | Resolved::Or(filters) => {
                filters.iter().any(Resolved::uses_row_variables)
            }
            _ => false,
        }
    }

    /// Adds the columns this filter reads at the current row to `columns`, and returns
    /// whether it also reads columns at other rows.
    ///
    /// Behavior: `WhereFilter#getColumns` and `#getColumnArrays` of each filter kind.
    pub(super) fn inputs(
        &self,
        definition: &TableDefinition,
        columns: &mut BTreeSet<usize>,
    ) -> bool {
        match self {
            Resolved::Match { column, .. }
            | Resolved::Range { column, .. }
            | Resolved::Between { column, .. }
            | Resolved::Pattern { column, .. } => {
                columns.insert(*column);
                false
            }
            Resolved::Nothing => false,
            Resolved::Condition { formula, .. } => {
                columns.extend(
                    formula
                        .columns()
                        .iter()
                        .filter_map(|name| definition.index_of(name)),
                );
                !formula.column_arrays().is_empty()
            }
            Resolved::And(filters) | Resolved::Or(filters) => {
                let mut arrays = false;
                for filter in filters {
                    arrays |= filter.inputs(definition, columns);
                }
                arrays
            }
        }
    }
}
