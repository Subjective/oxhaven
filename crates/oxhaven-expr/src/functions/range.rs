//! `io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual`: a range of numbers cut
//! into bins of one width, which a formula makes with `new` and asks for the bin of a
//! value (`index`) and for the bounds of a bin (`binMin`, `binMax`). The histograms of
//! `deephaven.plot.express` are computed with it.
//!
//! In Java the object is a cell of a column whose type is the class. No column type of
//! the contracts holds an object of a class, so a range is held as a `double[]`
//! ([`COLUMN_TYPE`]) of four elements: the minimum, the maximum, the width of a bin, and
//! [`MARK`], a NaN no computation makes, by which the methods know that a `double[]` is a
//! range. A `double[]` without the mark is refused when a method reads it. What follows
//! from holding it so:
//!
//! - a client is told the column is a `double[]`, where Java tells it the class and
//!   sends the object's identity text (`...DiscretizedRangeEqual@349e7182`);
//! - a method of a range called on a column of `double[]` is accepted when the formula
//!   is compiled, where Java has no such method, and fails when a row is read;
//! - two ranges of the same numbers are one key of a group, where Java's are two
//!   objects.
//!
//! The arithmetic is Java's on the values as the generated code holds them: nothing looks
//! for null, a null number is the value Java reserves for it, and a result is null only
//! where it is that value.

use crate::eval::numeric::with_null_values;
use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

/// The class, as a formula names it after `new`: Java imports no package it is in.
pub(crate) const CLASS: &str = "io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual";

/// The fourth element of a cell that is a range: a quiet NaN whose payload spells `DRE`.
const MARK: u64 = 0x7FF8_0000_0044_5245;

/// How many elements a cell that is a range has.
const CELL: usize = 4;

/// The type of a column of ranges.
pub(crate) fn column_type() -> ColumnType {
    ColumnType::array(ColumnType::Double)
}

/// The constructor and the methods of the class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RangeFn {
    /// `new DiscretizedRangeEqual(double min, double max, int nBins)`.
    New,
    /// `long index(double value)`.
    Index,
    /// `double binMin(long index)`.
    BinMin,
    /// `double binMax(long index)`.
    BinMax,
}

impl RangeFn {
    /// The method of a range that a formula calls as `name`.
    pub(crate) fn method(name: &str) -> Option<RangeFn> {
        Some(match name {
            "index" => RangeFn::Index,
            "binMin" => RangeFn::BinMin,
            "binMax" => RangeFn::BinMax,
            _ => return None,
        })
    }

    /// The types of the parameters, without the range a method is called on.
    pub(crate) fn parameters(self) -> &'static [ColumnType] {
        match self {
            RangeFn::New => &[ColumnType::Double, ColumnType::Double, ColumnType::Int],
            RangeFn::Index => &[ColumnType::Double],
            RangeFn::BinMin | RangeFn::BinMax => &[ColumnType::Long],
        }
    }

    pub(crate) fn result_type(self) -> ColumnType {
        match self {
            RangeFn::New => column_type(),
            RangeFn::Index => ColumnType::Long,
            RangeFn::BinMin | RangeFn::BinMax => ColumnType::Double,
        }
    }

    /// The name in messages.
    pub(crate) fn name(self) -> &'static str {
        match self {
            RangeFn::New => "<init>",
            RangeFn::Index => "index",
            RangeFn::BinMin => "binMin",
            RangeFn::BinMax => "binMax",
        }
    }

    /// Why a table operation of a client may not use this, which is so for all of them:
    /// the class is on none of the lists of `dh-defaults.prop`.
    ///
    /// Behavior: `io.deephaven.server.table.validation.ParsingColumnExpressionValidator#validateInvocations`,
    /// whose messages these are, as Java 42.9 logged them.
    pub(crate) fn refusal(self) -> String {
        match self {
            RangeFn::New => format!(
                "User expressions are not permitted to instantiate class \
                 {CLASS}(double, double, int)"
            ),
            RangeFn::Index => {
                format!("User expressions are not permitted to use method index(double) on class {CLASS}")
            }
            RangeFn::BinMin | RangeFn::BinMax => format!(
                "User expressions are not permitted to use method {}(long) on class {CLASS}",
                self.name()
            ),
        }
    }
}

/// A range as Java's object holds it.
#[derive(Clone, Copy, PartialEq, Debug)]
struct Range {
    min: f64,
    max: f64,
    bin_width: f64,
}

impl Range {
    /// Behavior: `DiscretizedRangeEqual#DiscretizedRangeEqual(double, double, int)`
    fn new(min: f64, max: f64, bins: i32) -> Range {
        Range {
            min,
            max,
            bin_width: (max - min) / f64::from(bins),
        }
    }

    /// Behavior: `DiscretizedRangeEqual#binMin`
    fn bin_min(self, index: i64) -> f64 {
        self.min + self.bin_width * index as f64
    }

    /// Behavior: `DiscretizedRangeEqual#binMax`, whose `index + 1` wraps.
    fn bin_max(self, index: i64) -> f64 {
        self.min + self.bin_width * index.wrapping_add(1) as f64
    }

    /// The bin of `value`, or the null `long` for a value that is outside the range or
    /// not finite. `io.deephaven.function.Numeric#isFinite(double)` is false for the null
    /// `double` too. The cast to `long` is Java's: zero for NaN and the ends of the
    /// `long` range for what lies beyond them, which is what `as` does. A value at the
    /// maximum belongs to the last bin.
    ///
    /// Behavior: `DiscretizedRangeEqual#index`
    fn index(self, value: f64) -> i64 {
        if !value.is_finite() || value == -f64::MAX || value < self.min || value > self.max {
            return i64::MIN;
        }
        let index = ((value - self.min) / self.bin_width) as i64;
        if value == self.max {
            index.wrapping_sub(1)
        } else {
            index
        }
    }
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The ranges of a column of them.
///
/// A null cell fails as Java's call on a null reference does. A cell without the mark is
/// a `double[]` that no constructor of the class made.
fn ranges(cells: &ArrayRef) -> Result<Vec<Range>, TableFailure> {
    let cells = cells.as_list::<i32>();
    let elements = cells.values().as_primitive::<Float64Type>();
    let offsets = cells.offsets();
    (0..cells.len())
        .map(|row| {
            if cells.is_null(row) {
                return Err(failure(format!(
                    "java.lang.NullPointerException: method call on a null {CLASS}"
                )));
            }
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            let marked = end - start == CELL
                && elements.is_valid(start + 3)
                && elements.value(start + 3).to_bits() == MARK;
            if !marked {
                return Err(failure(format!(
                    "a double[] that is not a {CLASS} was called as one"
                )));
            }
            // a null element is the null `double`, which is what Java's object would hold
            let element = |at: usize| {
                if elements.is_valid(at) {
                    elements.value(at)
                } else {
                    -f64::MAX
                }
            };
            Ok(Range {
                min: element(start),
                max: element(start + 1),
                bin_width: element(start + 2),
            })
        })
        .collect()
}

/// A column of ranges.
fn cells(ranges: impl ExactSizeIterator<Item = Range>) -> ArrayRef {
    let rows = ranges.len();
    let mut elements = Vec::with_capacity(rows * CELL);
    for range in ranges {
        elements.extend([range.min, range.max, range.bin_width, f64::from_bits(MARK)]);
    }
    Arc::new(ListArray::new(
        Arc::new(Field::new("", DataType::Float64, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(CELL, rows)),
        Arc::new(Float64Array::from(elements)),
        None,
    ))
}

/// Computes `function` for arguments of the types [`RangeFn::parameters`] names, a
/// method's after the range it is called on.
///
/// An argument is read as the generated code holds it, a null as the value Java reserves
/// for null, and a result that is the null value of its type is a null cell.
pub(crate) fn evaluate(
    function: RangeFn,
    arguments: &[ArrayRef],
) -> Result<ArrayRef, TableFailure> {
    if function == RangeFn::New {
        let min = with_null_values(&arguments[0]);
        let max = with_null_values(&arguments[1]);
        let bins = with_null_values(&arguments[2]);
        let min = min.as_primitive::<Float64Type>().values();
        let max = max.as_primitive::<Float64Type>().values();
        let bins = bins.as_primitive::<Int32Type>().values();
        return Ok(cells(
            (0..min.len()).map(|row| Range::new(min[row], max[row], bins[row])),
        ));
    }
    let ranges = ranges(&arguments[0])?;
    let argument = with_null_values(&arguments[1]);
    Ok(match function {
        RangeFn::Index => {
            let values = argument.as_primitive::<Float64Type>().values();
            Arc::new(
                ranges
                    .iter()
                    .zip(values.iter())
                    .map(|(range, value)| {
                        let index = range.index(*value);
                        (index != i64::MIN).then_some(index)
                    })
                    .collect::<Int64Array>(),
            )
        }
        RangeFn::BinMin | RangeFn::BinMax => {
            let indexes = argument.as_primitive::<Int64Type>().values();
            Arc::new(
                ranges
                    .iter()
                    .zip(indexes.iter())
                    .map(|(range, index)| {
                        let bound = if function == RangeFn::BinMin {
                            range.bin_min(*index)
                        } else {
                            range.bin_max(*index)
                        };
                        (bound != -f64::MAX).then_some(bound)
                    })
                    .collect::<Float64Array>(),
            )
        }
        RangeFn::New => unreachable!("handled above"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cell_round_trips_and_a_plain_array_is_refused() {
        let range = Range::new(0.0, 10.0, 5);
        let column = cells([range, Range::new(1.0, 1.0, 0)].into_iter());
        let read = ranges(&column).unwrap();
        assert_eq!(read[0], range);
        assert_eq!(read[1].min, 1.0);
        assert!(read[1].bin_width.is_nan());

        let plain: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("", DataType::Float64, true)),
            OffsetBuffer::from_lengths([4]),
            Arc::new(Float64Array::from(vec![0.0, 10.0, 2.0, f64::NAN])),
            None,
        ));
        let problem = ranges(&plain).unwrap_err();
        assert!(problem.message.contains("not a"), "{}", problem.message);
    }
}
