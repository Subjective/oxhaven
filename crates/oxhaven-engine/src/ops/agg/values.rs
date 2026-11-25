//! Column values as the aggregation operators see them.
//!
//! Operators are written once for all column types. A chunk of an input column is decoded
//! into one of the shapes of [`Values`], and single values are carried as [`Scalar`]s.

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, ListArray, StringArray, StructArray,
    TimestampNanosecondArray, UInt16Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Float64Type, Int32Type, Int64Type};
use num_bigint::BigInt;
use oxhaven_column::decimal::{self, Decimal};
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

pub(super) fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The shape a column type decodes to.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    /// `byte`, `short`, `int`, `long`, `char` (as its code unit) and `Instant` (as nanoseconds).
    Long,
    /// `float` (widened, which is exact) and `double`.
    Double,
    Bool,
    Str,
    /// A vector or an array, of elements of any shape.
    List,
    /// `ZonedDateTime`, `Period`, `BigDecimal` and `BigInteger`, held cell by cell.
    Cells,
}

impl Shape {
    pub fn of(column_type: &ColumnType) -> Shape {
        match column_type {
            _ if column_type.list_element().is_some() => Shape::List,
            ColumnType::Float | ColumnType::Double => Shape::Double,
            ColumnType::Boolean => Shape::Bool,
            // A `ZoneId` is held as its id.
            ColumnType::String | ColumnType::ZoneId => Shape::Str,
            ColumnType::ZonedDateTime
            | ColumnType::Period
            | ColumnType::BigDecimal
            | ColumnType::BigInteger => Shape::Cells,
            _ => Shape::Long,
        }
    }
}

/// A chunk of one input column, decoded.
#[derive(Debug)]
pub enum Values {
    Longs(Int64Array),
    Doubles(Float64Array),
    Bools(BooleanArray),
    Strings(StringArray),
    /// The cells of a vector, array, `ZonedDateTime`, `Period`, `BigDecimal` or `BigInteger`
    /// column: a [`Scalar::List`], [`Scalar::Zoned`], [`Scalar::Period`], [`Scalar::Decimal`] or
    /// [`Scalar::Integer`] each, or a null.
    Cells(Vec<Scalar>),
}

impl Values {
    /// Decodes `array`, a chunk of a column of `column_type`.
    pub fn decode(column_type: &ColumnType, array: &ArrayRef) -> Result<Values, TableFailure> {
        let cast = |to: &DataType| {
            arrow::compute::cast(array, to).map_err(|error| failure(error.to_string()))
        };
        if !decodes(column_type) {
            return Err(failure(format!(
                "unported: io.deephaven.engine.table.impl.by.AggregationProcessor: an \
                 aggregation reading a column of type {column_type:?}"
            )));
        }
        Ok(match Shape::of(column_type) {
            Shape::Long => {
                Values::Longs(cast(&DataType::Int64)?.as_primitive::<Int64Type>().clone())
            }
            Shape::Double => Values::Doubles(
                cast(&DataType::Float64)?
                    .as_primitive::<Float64Type>()
                    .clone(),
            ),
            Shape::Bool => Values::Bools(
                array
                    .as_boolean_opt()
                    .ok_or_else(|| failure("a Boolean column must hold a boolean array"))?
                    .clone(),
            ),
            Shape::Str => Values::Strings(
                array
                    .as_string_opt::<i32>()
                    .ok_or_else(|| failure("a String column must hold a string array"))?
                    .clone(),
            ),
            Shape::List => {
                let (Some(element), Some(list)) =
                    (column_type.list_element(), array.as_list_opt::<i32>())
                else {
                    return Err(failure("a vector or array column must hold a list array"));
                };
                let elements = Values::decode(element, list.values())?;
                let offsets = list.value_offsets();
                Values::Cells(
                    (0..list.len())
                        .map(|row| {
                            if list.is_null(row) {
                                return Scalar::Null;
                            }
                            let cells = offsets[row] as usize..offsets[row + 1] as usize;
                            Scalar::List(cells.map(|at| elements.scalar(at)).collect())
                        })
                        .collect(),
                )
            }
            Shape::Cells if *column_type == ColumnType::ZonedDateTime => {
                Values::Cells(zoned_cells(array)?)
            }
            Shape::Cells if *column_type != ColumnType::Period => {
                Values::Cells(big_cells(array, *column_type == ColumnType::BigDecimal)?)
            }
            Shape::Cells => Values::Cells(period_cells(array)?),
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Values::Longs(a) => a.len(),
            Values::Doubles(a) => a.len(),
            Values::Bools(a) => a.len(),
            Values::Strings(a) => a.len(),
            Values::Cells(cells) => cells.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The value at `position` as a `double`, as Java's cast of a chunk to doubles gives
    /// it: an integer is converted, a null is `None`. Booleans and strings are `None`; no
    /// operator that reads doubles is made for them.
    pub fn double(&self, position: usize) -> Option<f64> {
        match self {
            Values::Longs(a) if a.is_valid(position) => Some(a.value(position) as f64),
            Values::Doubles(a) if a.is_valid(position) => Some(a.value(position)),
            _ => None,
        }
    }

    /// The value at `position` of an integer-shaped column, or `None` if it is null.
    pub fn long(&self, position: usize) -> Option<i64> {
        match self {
            Values::Longs(a) if a.is_valid(position) => Some(a.value(position)),
            _ => None,
        }
    }

    /// The value at `position`.
    pub fn scalar(&self, position: usize) -> Scalar {
        match self {
            Values::Longs(a) if a.is_valid(position) => Scalar::Long(a.value(position)),
            Values::Doubles(a) if a.is_valid(position) => Scalar::Double(a.value(position)),
            Values::Bools(a) if a.is_valid(position) => Scalar::Bool(a.value(position)),
            Values::Strings(a) if a.is_valid(position) => Scalar::Str(a.value(position).into()),
            Values::Cells(cells) => cells[position].clone(),
            _ => Scalar::Null,
        }
    }
}

/// The rows of one group within one chunk: what an operator is given at a time.
///
/// Behavior: the `(values, startPosition, length)` run that
/// io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper passes to
/// io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator#addChunk
/// after it has brought the rows of each group together.
#[derive(Clone, Copy)]
pub struct Run<'a> {
    /// The chunk of the operator's input column; `None` for an operator without one.
    values: Option<&'a Values>,
    /// The chunk of the operator's weight column, for the weighted aggregations.
    weights: Option<&'a Values>,
    /// The row keys of the chunk.
    row_keys: &'a [RowKey],
    /// Positions in the chunk of this group's rows, increasing.
    positions: &'a [usize],
}

impl<'a> Run<'a> {
    pub fn new(
        values: Option<&'a Values>,
        row_keys: &'a [RowKey],
        positions: &'a [usize],
    ) -> Run<'a> {
        Run {
            values,
            weights: None,
            row_keys,
            positions,
        }
    }

    /// The run with the chunk of a weight column at the same positions.
    pub fn with_weights(self, weights: Option<&'a Values>) -> Run<'a> {
        Run { weights, ..self }
    }

    /// Each row's value and weight as `double`s, in row order.
    ///
    /// # Panics
    /// If the operator has no input or no weight column.
    pub fn weighted_doubles(&self) -> impl Iterator<Item = (Option<f64>, Option<f64>)> + 'a {
        let (Some(values), Some(weights)) = (self.values, self.weights) else {
            panic!("the operator has no input or no weight column");
        };
        self.positions
            .iter()
            .map(move |&p| (values.double(p), weights.double(p)))
    }

    /// Each row's value and weight of integer-shaped columns, in row order. Panics as
    /// [`Run::weighted_doubles`].
    pub fn weighted_longs(&self) -> impl Iterator<Item = (Option<i64>, Option<i64>)> + 'a {
        let (Some(values), Some(weights)) = (self.values, self.weights) else {
            panic!("the operator has no input or no weight column");
        };
        self.positions
            .iter()
            .map(move |&p| (values.long(p), weights.long(p)))
    }

    /// The number of rows.
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// The row keys, increasing.
    pub fn row_keys(&self) -> impl Iterator<Item = RowKey> + 'a {
        let row_keys = self.row_keys;
        self.positions.iter().map(move |&p| row_keys[p])
    }

    /// The values of an integer-shaped column, in row order.
    ///
    /// # Panics
    /// If the operator was given a column of another shape, which its constructor rules out.
    pub fn longs(&self) -> impl Iterator<Item = Option<i64>> + 'a {
        let Some(Values::Longs(array)) = self.values else {
            panic!("the operator's input is not an integer-shaped column");
        };
        self.positions
            .iter()
            .map(move |&p| array.is_valid(p).then(|| array.value(p)))
    }

    /// The values of a floating point column, in row order. Panics as [`Run::longs`].
    pub fn doubles(&self) -> impl Iterator<Item = Option<f64>> + 'a {
        let Some(Values::Doubles(array)) = self.values else {
            panic!("the operator's input is not a floating point column");
        };
        self.positions
            .iter()
            .map(move |&p| array.is_valid(p).then(|| array.value(p)))
    }

    /// The values of a Boolean column, in row order. Panics as [`Run::longs`].
    pub fn bools(&self) -> impl Iterator<Item = Option<bool>> + 'a {
        let Some(Values::Bools(array)) = self.values else {
            panic!("the operator's input is not a Boolean column");
        };
        self.positions
            .iter()
            .map(move |&p| array.is_valid(p).then(|| array.value(p)))
    }

    /// The row key and value of the first row (`first`) or the last row, if there is one.
    /// Panics as [`Run::scalars`].
    pub fn end(&self, first: bool) -> Option<(RowKey, Scalar)> {
        let Some(values) = self.values else {
            panic!("the operator has no input column");
        };
        let position = if first {
            self.positions.first()
        } else {
            self.positions.last()
        };
        position.map(|&p| (self.row_keys[p], values.scalar(p)))
    }

    /// The values of a column of any type, in row order, each with its row key.
    ///
    /// # Panics
    /// If the operator has no input column.
    pub fn scalars(&self) -> impl Iterator<Item = (RowKey, Scalar)> + 'a {
        let Some(values) = self.values else {
            panic!("the operator has no input column");
        };
        let row_keys = self.row_keys;
        self.positions
            .iter()
            .map(move |&p| (row_keys[p], values.scalar(p)))
    }
}

/// The rows of one chunk brought together by group: a [`Run`] for each group, which an
/// operator can take all at once instead of a call per group.
#[derive(Clone, Copy)]
pub struct Runs<'a> {
    values: Option<&'a Values>,
    weights: Option<&'a Values>,
    row_keys: &'a [RowKey],
    /// Positions in the chunk, by group and then increasing.
    order: &'a [usize],
    /// Each group's slot and its range of `order`, a group once.
    runs: &'a [(usize, Range<usize>)],
}

impl<'a> Runs<'a> {
    pub fn new(
        values: Option<&'a Values>,
        weights: Option<&'a Values>,
        row_keys: &'a [RowKey],
        order: &'a [usize],
        runs: &'a [(usize, Range<usize>)],
    ) -> Runs<'a> {
        Runs {
            values,
            weights,
            row_keys,
            order,
            runs,
        }
    }

    /// The number of groups.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Each group's slot and its run, in order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, Run<'a>)> + 'a {
        let Runs {
            values,
            weights,
            row_keys,
            order,
            runs,
        } = *self;
        runs.iter().map(move |(slot, range)| {
            let run = Run::new(values, row_keys, &order[range.clone()]).with_weights(weights);
            (*slot, run)
        })
    }

    /// Each group's slot and the position in the chunk of its first row (`first`) or its
    /// last row, in order. A run is never empty.
    pub fn ends(&self, first: bool) -> impl Iterator<Item = (usize, usize)> + 'a {
        let order = self.order;
        self.runs.iter().map(move |(slot, range)| {
            let end = if first { range.start } else { range.end - 1 };
            (*slot, order[end])
        })
    }

    /// The row key at `position` of the chunk.
    pub fn row_key(&self, position: usize) -> RowKey {
        self.row_keys[position]
    }

    /// The value at `position` of the chunk. Panics as [`Run::scalars`].
    pub fn scalar(&self, position: usize) -> Scalar {
        let Some(values) = self.values else {
            panic!("the operator has no input column");
        };
        values.scalar(position)
    }
}

/// One value of any column type.
#[derive(Clone, Debug)]
pub enum Scalar {
    Null,
    Long(i64),
    Double(f64),
    Bool(bool),
    Str(Arc<str>),
    /// The elements of a vector or an array, each a value of the element type or a null.
    List(Arc<[Scalar]>),
    /// A `ZonedDateTime`: its instant in nanoseconds, the offset from UTC of its zone at
    /// that instant in seconds, and the zone's id.
    Zoned {
        nanos: i64,
        offset: i32,
        zone: Arc<str>,
    },
    /// A `Period`. Java gives it no order; [`Scalar::compare`] orders it by its fields, for
    /// the multisets of a group that holds one period.
    Period {
        years: i32,
        months: i32,
        days: i32,
    },
    /// A `BigDecimal`: its unscaled value and its scale. Two of one value and different
    /// scales are different values (`BigDecimal#equals`) that Java's ordering calls equal
    /// (`BigDecimal#compareTo`).
    Decimal(Arc<Decimal>),
    /// A `BigInteger`.
    Integer(Arc<BigInt>),
}

/// Java's ordering of two values of one type of which either may be null: a null is less
/// than every other value.
///
/// Behavior: io.deephaven.util.compare.ObjectComparisons#compare, and the comparisons of
/// the primitive types under io.deephaven.util.compare, whose null is their least value
fn compare_nullable(a: &Scalar, b: &Scalar) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => a.compare(b),
    }
}

impl Scalar {
    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null)
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, Scalar::Double(v) if v.is_nan())
    }

    /// Whether a reader could tell the two apart: the same value, with every NaN the same
    /// and the two zeros different.
    pub fn same(&self, other: &Scalar) -> bool {
        match (self, other) {
            (Scalar::Null, Scalar::Null) => true,
            (Scalar::Long(a), Scalar::Long(b)) => a == b,
            (Scalar::Double(a), Scalar::Double(b)) => {
                a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
            }
            (Scalar::Bool(a), Scalar::Bool(b)) => a == b,
            (Scalar::Str(a), Scalar::Str(b)) => a == b,
            (Scalar::List(a), Scalar::List(b)) => {
                a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| a.same(b))
            }
            // The offset follows from the instant and the zone.
            (
                Scalar::Zoned {
                    nanos: a, zone: x, ..
                },
                Scalar::Zoned {
                    nanos: b, zone: y, ..
                },
            ) => a == b && x == y,
            (Scalar::Period { .. }, Scalar::Period { .. }) => self.period() == other.period(),
            // The scale is part of the value: `1.0` and `1.00` read differently.
            (Scalar::Decimal(a), Scalar::Decimal(b)) => a == b,
            (Scalar::Integer(a), Scalar::Integer(b)) => a == b,
            _ => false,
        }
    }

    /// Java's ordering of two non-null values of one column: numbers by value with `-0.0`
    /// equal to `0.0` and NaN equal to itself and above everything, `false` before `true`,
    /// and strings by UTF-16 code unit as `String#compareTo` does. A `char` is its code unit
    /// and an `Instant` its nanoseconds, which order as Java orders them. Vectors are
    /// ordered element by element, a null element first, and a vector that is the start of
    /// another before it. A `ZonedDateTime` orders by its instant, then by its offset (for
    /// one instant, the order of the local dates and times), then by its zone's id. Values
    /// of different shapes, nulls and periods compare equal; callers never pass them. A
    /// `BigDecimal` orders by its value whatever its scale, and a `BigInteger` by its value.
    ///
    /// Behavior: io.deephaven.util.compare.DoubleComparisons#compare,
    /// io.deephaven.util.compare.CharComparisons#compare,
    /// io.deephaven.util.compare.ObjectComparisons#compare, java.math.BigDecimal#compareTo,
    /// java.math.BigInteger#compareTo, the `compareTo` of the
    /// vectors under io.deephaven.vector (`ShortVector#compareTo`, `ObjectVector#compareTo`
    /// and the others), and java.time.chrono.ChronoZonedDateTime#compareTo
    pub fn compare(&self, other: &Scalar) -> Ordering {
        match (self, other) {
            (Scalar::Long(a), Scalar::Long(b)) => a.cmp(b),
            (Scalar::Double(a), Scalar::Double(b)) => match (a.is_nan(), b.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            },
            (Scalar::Bool(a), Scalar::Bool(b)) => a.cmp(b),
            (Scalar::Str(a), Scalar::Str(b)) => a.encode_utf16().cmp(b.encode_utf16()),
            (Scalar::List(a), Scalar::List(b)) => a
                .iter()
                .zip(b.iter())
                .map(|(a, b)| compare_nullable(a, b))
                .find(|order| order.is_ne())
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (
                Scalar::Zoned {
                    nanos: a,
                    offset: p,
                    zone: x,
                },
                Scalar::Zoned {
                    nanos: b,
                    offset: q,
                    zone: y,
                },
            ) => a
                .cmp(b)
                .then(p.cmp(q))
                .then_with(|| x.encode_utf16().cmp(y.encode_utf16())),
            // Java has no order of periods; this one keeps two that differ apart in a
            // multiset until the operator fails (`periods::PeriodGuard`).
            (Scalar::Period { .. }, Scalar::Period { .. }) => self.period().cmp(&other.period()),
            (Scalar::Decimal(a), Scalar::Decimal(b)) => a.compare(b),
            (Scalar::Integer(a), Scalar::Integer(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    pub fn is_period(&self) -> bool {
        matches!(self, Scalar::Period { .. })
    }

    fn period(&self) -> Option<(i32, i32, i32)> {
        match self {
            Scalar::Period {
                years,
                months,
                days,
            } => Some((*years, *months, *days)),
            _ => None,
        }
    }

    pub(super) fn long(&self) -> Option<i64> {
        match self {
            Scalar::Long(v) => Some(*v),
            _ => None,
        }
    }

    pub(super) fn double(&self) -> Option<f64> {
        match self {
            Scalar::Double(v) => Some(*v),
            _ => None,
        }
    }
}

/// The cells of a chunk of a `ZonedDateTime` column, each with its zone's offset at its
/// instant.
fn zoned_cells(array: &ArrayRef) -> Result<Vec<Scalar>, TableFailure> {
    let (cells, instants, zones) = crate::ops::zoned::parts(array)?;
    (0..cells.len())
        .map(|row| {
            if cells.is_null(row) {
                return Ok(Scalar::Null);
            }
            let zone = zones.value(row);
            Ok(Scalar::Zoned {
                nanos: instants[row],
                offset: crate::ops::zoned::offset_seconds(zone, instants[row])?,
                zone: zone.into(),
            })
        })
        .collect()
}

/// The values of a chunk of a `BigDecimal` or `BigInteger` column (`decimal`), as their
/// cells hold them.
fn big_cells(array: &ArrayRef, decimal: bool) -> Result<Vec<Scalar>, TableFailure> {
    let cells = array
        .as_binary_opt::<i32>()
        .ok_or_else(|| failure("a BigDecimal or BigInteger column must hold a binary array"))?;
    cells
        .iter()
        .map(|cell| match cell {
            None => Ok(Scalar::Null),
            Some(bytes) if decimal => Decimal::of_cell(bytes)
                .map(|value| Scalar::Decimal(Arc::new(value)))
                .ok_or_else(|| failure("a BigDecimal cell without a scale")),
            Some(bytes) => Ok(Scalar::Integer(Arc::new(decimal::integer_of_cell(bytes)))),
        })
        .collect()
}

/// The cells of a chunk of a `Period` column.
fn period_cells(array: &ArrayRef) -> Result<Vec<Scalar>, TableFailure> {
    let cells = array
        .as_struct_opt()
        .ok_or_else(|| failure("a Period column must hold a struct array"))?;
    let field = |at: usize| {
        cells
            .column(at)
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| failure("the fields of a Period column must be ints"))
    };
    let (years, months, days) = (field(0)?, field(1)?, field(2)?);
    Ok((0..cells.len())
        .map(|row| {
            if cells.is_null(row) {
                Scalar::Null
            } else {
                Scalar::Period {
                    years: years.value(row),
                    months: months.value(row),
                    days: days.value(row),
                }
            }
        })
        .collect())
}

/// Whether `Values::decode` holds the values of `column_type`: the integers, `char` and the
/// instants, dates, times of day, dates and times, durations and days of the week as
/// numbers, the floating point numbers, `Boolean`, `String`, `ZoneId`, `ZonedDateTime`,
/// `Period`, `BigDecimal` and `BigInteger`, and lists of these.
fn decodes(column_type: &ColumnType) -> bool {
    match column_type.list_element() {
        Some(element) => decodes(element),
        None => matches!(
            column_type,
            ColumnType::Byte
                | ColumnType::Short
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Char
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::Boolean
                | ColumnType::String
                | ColumnType::Instant
                | ColumnType::LocalDate
                | ColumnType::LocalTime
                | ColumnType::Duration
                | ColumnType::LocalDateTime
                | ColumnType::DayOfWeek
                | ColumnType::ZoneId
                | ColumnType::ZonedDateTime
                | ColumnType::Period
                | ColumnType::BigDecimal
                | ColumnType::BigInteger
        ),
    }
}

/// An array of `column_type` holding `values`. A value of another shape than the column's
/// becomes null. Integers are narrowed as Java's casts narrow them.
pub fn build<'a>(column_type: &ColumnType, values: impl Iterator<Item = &'a Scalar>) -> ArrayRef {
    if let (Some(element), DataType::List(field)) =
        (column_type.list_element(), column_type.arrow_type())
    {
        let mut lengths = Vec::new();
        let mut valid = Vec::new();
        let mut elements: Vec<&Scalar> = Vec::new();
        for value in values {
            let cells: &[Scalar] = match value {
                Scalar::List(cells) => cells,
                _ => &[],
            };
            lengths.push(cells.len());
            valid.push(matches!(value, Scalar::List(_)));
            elements.extend(cells);
        }
        let elements: Box<dyn Iterator<Item = &Scalar>> = Box::new(elements.into_iter());
        return Arc::new(ListArray::new(
            field,
            OffsetBuffer::from_lengths(lengths),
            build(element, elements),
            Some(NullBuffer::from(valid)),
        ));
    }
    match column_type {
        ColumnType::Byte => Arc::new(
            values
                .map(|s| s.long().map(|v| v as i8))
                .collect::<Int8Array>(),
        ),
        ColumnType::Short => Arc::new(
            values
                .map(|s| s.long().map(|v| v as i16))
                .collect::<Int16Array>(),
        ),
        ColumnType::Int => Arc::new(
            values
                .map(|s| s.long().map(|v| v as i32))
                .collect::<Int32Array>(),
        ),
        ColumnType::Long => Arc::new(values.map(Scalar::long).collect::<Int64Array>()),
        ColumnType::Char => Arc::new(
            values
                .map(|s| s.long().map(|v| v as u16))
                .collect::<UInt16Array>(),
        ),
        ColumnType::Instant => Arc::new(
            values
                .map(Scalar::long)
                .collect::<TimestampNanosecondArray>()
                .with_timezone("UTC"),
        ),
        ColumnType::Float => Arc::new(
            values
                .map(|s| s.double().map(|v| v as f32))
                .collect::<Float32Array>(),
        ),
        ColumnType::Double => Arc::new(values.map(Scalar::double).collect::<Float64Array>()),
        ColumnType::Boolean => Arc::new(
            values
                .map(|s| match s {
                    Scalar::Bool(v) => Some(*v),
                    _ => None,
                })
                .collect::<BooleanArray>(),
        ),
        ColumnType::String | ColumnType::ZoneId => Arc::new(
            values
                .map(|s| match s {
                    Scalar::Str(v) => Some(&**v),
                    _ => None,
                })
                .collect::<StringArray>(),
        ),
        // Numbers of days and of nanoseconds, as `Values::decode` read them.
        ColumnType::LocalDate
        | ColumnType::LocalTime
        | ColumnType::Duration
        | ColumnType::LocalDateTime
        | ColumnType::DayOfWeek => {
            let numbers: ArrayRef = Arc::new(values.map(Scalar::long).collect::<Int64Array>());
            arrow::compute::cast(&numbers, &column_type.arrow_type())
                .expect("a number read from a date, a time of day, a duration or a day")
        }
        ColumnType::ZonedDateTime | ColumnType::Period => {
            let DataType::Struct(fields) = column_type.arrow_type() else {
                unreachable!("a ZonedDateTime and a Period are structs");
            };
            let values: Vec<&Scalar> = values.collect();
            let valid: Vec<bool> = values
                .iter()
                .map(|v| matches!(v, Scalar::Zoned { .. } | Scalar::Period { .. }))
                .collect();
            let columns: Vec<ArrayRef> = if *column_type == ColumnType::ZonedDateTime {
                let instants: TimestampNanosecondArray = values
                    .iter()
                    .map(|v| match v {
                        Scalar::Zoned { nanos, .. } => Some(*nanos),
                        _ => Some(0),
                    })
                    .collect();
                let zones: StringArray = values
                    .iter()
                    .map(|v| match v {
                        Scalar::Zoned { zone, .. } => Some(&**zone),
                        _ => Some("UTC"),
                    })
                    .collect();
                vec![Arc::new(instants.with_timezone("UTC")), Arc::new(zones)]
            } else {
                let parts: Vec<(i32, i32, i32)> = values
                    .iter()
                    .map(|v| v.period().unwrap_or_default())
                    .collect();
                vec![
                    Arc::new(Int32Array::from_iter_values(parts.iter().map(|p| p.0))),
                    Arc::new(Int32Array::from_iter_values(parts.iter().map(|p| p.1))),
                    Arc::new(Int32Array::from_iter_values(parts.iter().map(|p| p.2))),
                ]
            };
            Arc::new(StructArray::new(fields, columns, Some(valid.into())))
        }
        ColumnType::BigDecimal | ColumnType::BigInteger => Arc::new(
            values
                .map(|s| match s {
                    Scalar::Decimal(value) => Some(value.cell()),
                    Scalar::Integer(value) => Some(decimal::integer_bytes(value)),
                    _ => None,
                })
                .collect::<BinaryArray>(),
        ),
        // A column type `Values::decode` refuses: no operator reads one.
        _ => arrow::array::new_null_array(&column_type.arrow_type(), values.count()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, DurationNanosecondArray, Time64NanosecondArray};

    /// What an operator reads of a column, rebuilt as the result column.
    fn round_trip(column_type: &ColumnType, array: ArrayRef) -> ArrayRef {
        let values = Values::decode(column_type, &array).expect("decodes");
        let scalars: Vec<Scalar> = (0..array.len()).map(|at| values.scalar(at)).collect();
        build(column_type, scalars.iter())
    }

    /// Behavior: the first, the last, the minimum and the maximum of a `LocalDate`,
    /// `LocalTime`, `Duration` or `ZoneId` column are its values.
    #[test]
    fn a_date_a_time_a_duration_and_a_zone_are_kept() {
        let columns: [(ColumnType, ArrayRef); 4] = [
            (
                ColumnType::LocalDate,
                Arc::new(Date32Array::from(vec![Some(19_782), None, Some(-1)])),
            ),
            (
                ColumnType::LocalTime,
                Arc::new(Time64NanosecondArray::from(vec![Some(1), None, Some(0)])),
            ),
            (
                ColumnType::Duration,
                Arc::new(DurationNanosecondArray::from(vec![Some(-5), None, Some(7)])),
            ),
            (
                ColumnType::ZoneId,
                Arc::new(StringArray::from(vec![
                    Some("UTC"),
                    None,
                    Some("Asia/Tokyo"),
                ])),
            ),
        ];
        for (column_type, array) in columns {
            assert_eq!(
                round_trip(&column_type, Arc::clone(&array)).as_ref(),
                array.as_ref(),
                "{column_type:?}"
            );
        }
    }

    /// Behavior: the first and the last of a `ZonedDateTime` or a `Period` column are its
    /// values, zone and fields included.
    #[test]
    fn a_zoned_date_time_and_a_period_are_kept() {
        let zoned = crate::ops::zoned::tests::zoned(&[
            Some((1_710_028_800_000_000_000, "America/New_York")),
            None,
            Some((-1, "Asia/Tokyo")),
        ]);
        assert_eq!(
            round_trip(&ColumnType::ZonedDateTime, Arc::clone(&zoned)).as_ref(),
            zoned.as_ref()
        );
        let DataType::Struct(fields) = ColumnType::Period.arrow_type() else {
            unreachable!()
        };
        let period: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![1, 0, 0])),
                Arc::new(Int32Array::from(vec![2, 0, -14])),
                Arc::new(Int32Array::from(vec![3, 0, 5])),
            ],
            Some(vec![true, false, true].into()),
        ));
        assert_eq!(
            round_trip(&ColumnType::Period, Arc::clone(&period)).as_ref(),
            period.as_ref()
        );
    }

    /// Seen on Java 42.9: for one instant, `America/New_York` is before `Asia/Tokyo`
    /// (offsets -4 and +9 hours), and `Europe/London` before `UTC` (one offset, ids in
    /// order).
    #[test]
    fn zoned_date_times_order_by_instant_offset_and_zone() {
        let zoned = crate::ops::zoned::tests::zoned(&[
            Some((1_710_154_800_000_000_000, "America/New_York")),
            Some((1_710_154_800_000_000_000, "Asia/Tokyo")),
            Some((1_704_067_200_000_000_000, "UTC")),
            Some((1_704_067_200_000_000_000, "Europe/London")),
            Some((1_704_067_199_000_000_000, "Asia/Tokyo")),
        ]);
        let values = Values::decode(&ColumnType::ZonedDateTime, &zoned).unwrap();
        let at = |row| values.scalar(row);
        assert_eq!(at(0).compare(&at(1)), Ordering::Less);
        assert_eq!(at(3).compare(&at(2)), Ordering::Less);
        assert_eq!(at(4).compare(&at(3)), Ordering::Less);
        assert_eq!(at(1).compare(&at(1)), Ordering::Equal);
        assert!(!at(0).same(&at(1)));
        assert!(at(1).same(&at(1)));
    }

    /// Behavior: the first and the last of a `BigDecimal` or `BigInteger` column are its
    /// values, scale included (`1.0` and `1.00` read differently).
    #[test]
    fn big_decimals_and_big_integers_are_kept_as_they_are() {
        use oxhaven_column::java_values::{big_decimal_cell, big_integer_cell};
        let decimals: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&big_decimal_cell(1, &[10])[..]),
            None,
            Some(&big_decimal_cell(2, &[100])[..]),
        ]));
        let integers: ArrayRef = Arc::new(BinaryArray::from(vec![
            None,
            Some(&big_integer_cell(-129)[..]),
        ]));
        for (column_type, array) in [
            (ColumnType::BigDecimal, decimals),
            (ColumnType::BigInteger, integers),
        ] {
            assert_eq!(
                round_trip(&column_type, Arc::clone(&array)).as_ref(),
                array.as_ref(),
                "{column_type:?}"
            );
        }
        let decimals = Values::decode(
            &ColumnType::BigDecimal,
            &round_trip(
                &ColumnType::BigDecimal,
                Arc::new(BinaryArray::from(vec![
                    &big_decimal_cell(1, &[10])[..],
                    &big_decimal_cell(2, &[100])[..],
                ])),
            ),
        )
        .expect("decodes");
        assert!(!decimals.scalar(0).same(&decimals.scalar(1)));
        assert!(decimals.scalar(1).same(&decimals.scalar(1)));
    }

    /// A type the operators do not hold is a failure, not a column of nulls.
    #[test]
    fn a_column_of_tables_is_refused() {
        let column_type = ColumnType::Table(oxhaven_contracts::schema::TableClass::Table);
        let tables = arrow::array::new_null_array(&column_type.arrow_type(), 2);
        let Err(failure) = Values::decode(&column_type, &tables) else {
            panic!("a column of tables decoded");
        };
        assert!(failure.message.contains("Table"), "{}", failure.message);
    }
}
