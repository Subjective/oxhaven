//! Column values as the aggregation operators see them.
//!
//! Operators are written once for all column types. A chunk of an input column is decoded
//! into one of four shapes ([`Values`]), and single values are carried as [`Scalar`]s.

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
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
}

impl Shape {
    pub fn of(column_type: &ColumnType) -> Shape {
        match column_type {
            ColumnType::Float | ColumnType::Double => Shape::Double,
            ColumnType::Boolean => Shape::Bool,
            ColumnType::String => Shape::Str,
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
}

impl Values {
    /// Decodes `array`, a chunk of a column of `column_type`.
    pub fn decode(column_type: &ColumnType, array: &ArrayRef) -> Result<Values, TableFailure> {
        let cast = |to: &DataType| {
            arrow::compute::cast(array, to).map_err(|error| failure(error.to_string()))
        };
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
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Values::Longs(a) => a.len(),
            Values::Doubles(a) => a.len(),
            Values::Bools(a) => a.len(),
            Values::Strings(a) => a.len(),
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

/// One value of any column type.
#[derive(Clone, Debug)]
pub enum Scalar {
    Null,
    Long(i64),
    Double(f64),
    Bool(bool),
    Str(Arc<str>),
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
            _ => false,
        }
    }

    /// Java's ordering of two non-null values of one column: numbers by value with `-0.0`
    /// equal to `0.0` and NaN equal to itself and above everything, `false` before `true`,
    /// and strings by UTF-16 code unit as `String#compareTo` does. A `char` is its code unit
    /// and an `Instant` its nanoseconds, which order as Java orders them. Values of
    /// different shapes, or nulls, compare equal; callers never pass them.
    ///
    /// Behavior: io.deephaven.util.compare.DoubleComparisons#compare,
    /// io.deephaven.util.compare.CharComparisons#compare,
    /// io.deephaven.util.compare.ObjectComparisons#compare
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
            _ => Ordering::Equal,
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

/// An array of `column_type` holding `values`. A value of another shape than the column's
/// becomes null. Integers are narrowed as Java's casts narrow them.
pub fn build<'a>(column_type: &ColumnType, values: impl Iterator<Item = &'a Scalar>) -> ArrayRef {
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
        ColumnType::String => Arc::new(
            values
                .map(|s| match s {
                    Scalar::Str(v) => Some(&**v),
                    _ => None,
                })
                .collect::<StringArray>(),
        ),
        // A column type added after this was written: no operator is made for one.
        _ => arrow::array::new_null_array(&column_type.arrow_type(), values.count()),
    }
}
