//! The cumulative operators: each row's result depends on the rows before it in its bucket.
//!
//! A kernel computes a run of consecutive rows of one bucket. For a run that does not start
//! at the bucket's first row it is seeded with what Java reads back for the row before the
//! run: that row's result or, for `delta`, that row's input.

use super::big::MathContext;
use super::values::{
    double_array, double_result, long_array, long_result, narrowed_floats, narrowed_ints, pick,
    Values,
};
use super::{failure, DeltaControl, BOOLEAN_AS_OBJECTS};
use arrow::array::ArrayRef;
use arrow::compute::concat;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Cumulative {
    Sum,
    Product,
    Min,
    Max,
    ForwardFill,
    Delta(DeltaControl),
}

impl Cumulative {
    /// The type of the result column for an input column of type `input`, or `None` if Java
    /// has no operator for that type.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor
    /// #makeCumSumOperator, #makeCumProdOperator, #makeCumMinMaxOperator,
    /// #makeForwardFillOperator and #makeDeltaOperator, with the result sources of the
    /// operator classes they pick (`BaseLongUpdateByOperator`, `BaseDoubleUpdateByOperator`
    /// or the input's own type)
    pub(super) fn output_type(self, input: &ColumnType) -> Option<ColumnType> {
        use ColumnType::*;
        if super::big::is_big(input) {
            // A big number is `Comparable`, so its minimum and maximum have its type.
            return super::big::cumulative_output(self, input).or(Some(input.clone()));
        }
        match self {
            Cumulative::Sum => match input {
                Boolean | Byte | Char | Short | Int | Long => Some(Long),
                Float | Double => Some(Double),
                _ => None,
            },
            Cumulative::Product => match input {
                Byte | Short | Int | Long => Some(Long),
                Float | Double => Some(Double),
                _ => None,
            },
            // A Boolean is a `Comparable`, so Java makes its operator of objects for it,
            // which fails once it is given rows: see `accumulate`.
            Cumulative::Min | Cumulative::Max => match input {
                Byte | Char | Short | Int | Long | Instant | Float | Double | String | Boolean => {
                    Some(input.clone())
                }
                Vector(element) if super::orderable_vector(element) => Some(input.clone()),
                _ => None,
            },
            Cumulative::ForwardFill => Some(input.clone()),
            Cumulative::Delta(_) => match input {
                Byte | Char | Short | Int | Long | Float | Double => Some(input.clone()),
                // LongDeltaOperator takes a time column as its nanoseconds.
                Instant => Some(Long),
                _ => None,
            },
        }
    }

    /// Whether the seed of a run is the previous row's input instead of its result.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.delta.LongDeltaOperator#initializeCumulative
    /// against io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator#initializeCumulative
    pub(super) fn seeded_by_input(self) -> bool {
        matches!(self, Cumulative::Delta(_))
    }

    /// The results for a run of rows with the input `values`, of type `input`.
    pub(super) fn accumulate(
        self,
        input: &ColumnType,
        values: &ArrayRef,
        seed: Option<&ArrayRef>,
        math: &MathContext,
    ) -> Result<ArrayRef, TableFailure> {
        if super::big::is_big(input) && super::big::cumulative_output(self, input).is_some() {
            return super::big::cumulative(self, input, values, seed, math);
        }
        let output = self
            .output_type(input)
            .ok_or_else(|| failure("no cumulative operator for this column type"))?;
        if matches!(self, Cumulative::Min | Cumulative::Max) && *input == ColumnType::Boolean {
            // Behavior: io.deephaven.engine.table.impl.updateby.internal.BaseObjectBinaryOperator
            // .Context#setValueChunks, which casts the chunk of bytes that a Boolean column is
            // read as to a chunk of objects. It is called for every chunk of rows, so a table
            // without rows has the operation, and loses it with its first row.
            return Err(failure(BOOLEAN_AS_OBJECTS));
        }
        match self {
            Cumulative::Sum | Cumulative::Product => {
                let seed = match seed {
                    Some(seed) => Some(Values::read(seed, &output)?),
                    None => None,
                };
                let product = self == Cumulative::Product;
                Ok(match (Values::read(values, input)?, seed) {
                    (Values::Int(values), None) => long_array(running(&values, None, |a, b| {
                        long_result(if product {
                            a.wrapping_mul(b)
                        } else {
                            a.wrapping_add(b)
                        })
                    })),
                    (Values::Int(values), Some(Values::Int(seed))) => {
                        long_array(running(&values, seed[0], |a, b| {
                            long_result(if product {
                                a.wrapping_mul(b)
                            } else {
                                a.wrapping_add(b)
                            })
                        }))
                    }
                    (Values::Float(values), None) => {
                        double_array(running(&values, None, |a, b| {
                            double_result(if product { a * b } else { a + b })
                        }))
                    }
                    (Values::Float(values), Some(Values::Float(seed))) => {
                        double_array(running(&values, seed[0], |a, b| {
                            double_result(if product { a * b } else { a + b })
                        }))
                    }
                    _ => return Err(failure("a cumulative seed of another kind than its input")),
                })
            }
            Cumulative::Min | Cumulative::Max | Cumulative::ForwardFill => {
                let all = match seed {
                    Some(seed) => concat(&[seed.as_ref(), values.as_ref()])
                        .map_err(|e| failure(e.to_string()))?,
                    None => values.clone(),
                };
                // A fill reads only which cells are null.
                let all = if self == Cumulative::ForwardFill {
                    Values::read(&all, input)?
                } else {
                    Values::read_ordered(&all, input)?
                };
                let skip = usize::from(seed.is_some());
                let mut current: Option<usize> = None;
                let mut picked = Vec::with_capacity(values.len());
                for index in 0..all.len() {
                    current = self.keep(&all, current, index);
                    if index >= skip {
                        picked.push(current);
                    }
                }
                pick(seed, values, picked)
            }
            Cumulative::Delta(control) => {
                let seed = match seed {
                    Some(seed) => Some(Values::read(seed, input)?),
                    None => None,
                };
                Ok(match (Values::read(values, input)?, seed) {
                    (Values::Int(values), None) => narrowed_ints(
                        &output,
                        deltas(&values, None, control, 0, i64::wrapping_sub),
                    ),
                    (Values::Int(values), Some(Values::Int(seed))) => narrowed_ints(
                        &output,
                        deltas(&values, seed[0], control, 0, i64::wrapping_sub),
                    ),
                    (Values::Float(values), None) => {
                        narrowed_floats(&output, deltas(&values, None, control, 0.0, |a, b| a - b))
                    }
                    (Values::Float(values), Some(Values::Float(seed))) => narrowed_floats(
                        &output,
                        deltas(&values, seed[0], control, 0.0, |a, b| a - b),
                    ),
                    _ => return Err(failure("a delta seed of another kind than its input")),
                })
            }
        }
    }

    /// The index of the value a min, max or forward fill holds after the row at `index`,
    /// having held the value at `current` before it.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.minmax.LongCumMinMaxOperator.Context#push,
    /// io.deephaven.engine.table.impl.updateby.internal.BaseObjectBinaryOperator.Context#push with
    /// io.deephaven.engine.table.impl.updateby.minmax.ComparableCumMinMaxOperator#doOperation,
    /// and io.deephaven.engine.table.impl.updateby.fill.LongFillByOperator.Context#push
    fn keep(self, values: &Values, current: Option<usize>, index: usize) -> Option<usize> {
        if values.is_null(index) {
            return current;
        }
        let Some(held) = current else {
            return Some(index);
        };
        let replace = match self {
            Cumulative::ForwardFill => true,
            // A NaN compares as neither, so it never replaces and is never replaced.
            Cumulative::Max => values.compare(index, held) == Some(Ordering::Greater),
            _ => values.compare(index, held) == Some(Ordering::Less),
        };
        Some(if replace { index } else { held })
    }
}

/// A running sum or product: nulls are skipped, and a result that Java reads as null
/// (`combine` returns `None`) is replaced by the next value instead of combined with it.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.sum.LongCumSumOperator.Context#push,
/// io.deephaven.engine.table.impl.updateby.sum.DoubleCumSumOperator.Context#push,
/// io.deephaven.engine.table.impl.updateby.prod.LongCumProdOperator.Context#push
fn running<T: Copy>(
    values: &[Option<T>],
    seed: Option<T>,
    combine: impl Fn(T, T) -> Option<T>,
) -> Vec<Option<T>> {
    let mut current = seed;
    values
        .iter()
        .map(|value| {
            current = match (current, *value) {
                (None, value) => value,
                (Some(held), Some(value)) => combine(held, value),
                (held, None) => held,
            };
            current
        })
        .collect()
}

/// Each value less the one in the row before it. A null row gives null, and so, by default,
/// does the row after a null row or the first row of a bucket.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.delta.LongDeltaOperator.Context#push
fn deltas<T: Copy>(
    values: &[Option<T>],
    seed: Option<T>,
    control: DeltaControl,
    zero: T,
    subtract: impl Fn(T, T) -> T,
) -> Vec<Option<T>> {
    let mut last = seed;
    values
        .iter()
        .map(|value| {
            let result = match (*value, last) {
                (None, _) => None,
                (Some(value), None) => match control {
                    DeltaControl::NullDominates => None,
                    DeltaControl::ValueDominates => Some(value),
                    DeltaControl::ZeroDominates => Some(zero),
                },
                (Some(value), Some(last)) => Some(subtract(value, last)),
            };
            last = *value;
            result
        })
        .collect()
}
