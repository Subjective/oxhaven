//! The exponential moving operators: each row's result decays the result of the row before
//! it and folds in the row's value. They run in the cumulative window, seeded for a run of
//! rows that does not start at the bucket's first row with the result of the row before
//! the run and a timestamp Java finds for it (see `Plan::seed_timestamp`).
//!
//! With a decay in ticks every row decays the result by the same factor; with a decay in
//! time the factor depends on the time since the row that last changed the result.

use super::failure;
use super::values::{double_array, double_result, java_extreme, Values};
use super::BadDataBehavior;
use arrow::array::ArrayRef;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

/// How a result folds in the next value.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum EmKind {
    Average,
    Sum,
    Min,
    Max,
}

/// One exponential moving operator.
#[derive(Clone, Copy, PartialEq, Debug)]
pub(super) struct Em {
    pub(super) kind: EmKind,
    /// The decay: in ticks, or in nanoseconds when `timed`.
    pub(super) units: f64,
    pub(super) timed: bool,
    pub(super) on_null: BadDataBehavior,
    pub(super) on_nan: BadDataBehavior,
}

/// Java's null long, which a timestamp the operator has not seen yet holds; the time since
/// it is computed with Java's wrapping subtraction.
const NULL_LONG: i64 = i64::MIN;

impl Em {
    /// The type of the result column for an input column of type `input`, or `None` if Java
    /// has no operator for that type.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor
    /// #makeEmaOperator, #makeEmsOperator and #makeEmMinMaxOperator, whose operators all extend
    /// io.deephaven.engine.table.impl.updateby.internal.BaseDoubleUpdateByOperator
    pub(super) fn output_type(input: &ColumnType) -> Option<ColumnType> {
        use ColumnType::*;
        matches!(input, Byte | Char | Short | Int | Long | Float | Double).then_some(Double)
    }

    /// Whether the value of a row lets Java take that row's timestamp as the seed of a run.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.em.DoubleEMOperator.Context#isValueValid
    /// and the same method of the other primitive EM operators, which only test for null
    pub(super) fn valid(&self, value: Option<f64>) -> bool {
        match value {
            None => false,
            Some(v) => !v.is_nan() || self.on_nan != BadDataBehavior::Skip,
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor
    /// #makeEmaOperator, #makeEmsOperator and #makeEmMinMaxOperator, their `doubleFunction`
    fn fold(self, previous: f64, value: f64, alpha: f64, one_minus_alpha: f64) -> f64 {
        let decayed = previous * alpha;
        match self.kind {
            EmKind::Average => decayed + one_minus_alpha * value,
            EmKind::Sum => decayed + value,
            EmKind::Min => java_extreme(decayed, value, false),
            EmKind::Max => java_extreme(decayed, value, true),
        }
    }

    /// The results for a run of rows with the input `values` of type `input` and, when
    /// `timed`, their `timestamps`. `seed` is the result of the row before the run and the
    /// timestamp Java seeds the run with.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.em.DoubleEMOperator.Context#accumulateCumulative
    /// and the same method of `FloatEMOperator`, `ByteEMOperator`, `CharEMOperator`,
    /// `ShortEMOperator`, `IntEMOperator` and `LongEMOperator`, with
    /// io.deephaven.engine.table.impl.updateby.em.BasePrimitiveEMOperator#handleBadData and
    /// #initializeCumulative
    pub(super) fn accumulate(
        self,
        input: &ColumnType,
        values: &ArrayRef,
        timestamps: &[Option<i64>],
        seed: Option<(Option<f64>, Option<i64>)>,
    ) -> Result<ArrayRef, TableFailure> {
        // The floating point operators skip a row whose time did not move; the integer ones
        // fold it in with a decay of one.
        let floating = matches!(input, ColumnType::Float | ColumnType::Double);
        let values: Vec<Option<f64>> = match Values::read(values, input)? {
            Values::Int(v) => v.into_iter().map(|v| v.map(|v| v as f64)).collect(),
            Values::Float(v) => v,
            Values::Text(_) => return Err(failure("no exponential operator for this column type")),
        };
        let alpha = (-1.0 / self.units).exp();
        let one_minus_alpha = 1.0 - alpha;
        let (mut current, mut last_stamp) = match seed {
            Some((value, stamp)) => (value, stamp.unwrap_or(NULL_LONG)),
            None => (None, NULL_LONG),
        };
        let mut results = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            match *value {
                None => {
                    match self.on_null {
                        BadDataBehavior::Throw => return Err(failure(
                            "Encountered null value during Exponential Moving output processing",
                        )),
                        BadDataBehavior::Reset => {
                            current = None;
                            last_stamp = NULL_LONG;
                        }
                        BadDataBehavior::Skip | BadDataBehavior::Poison => {}
                    }
                }
                Some(v) if v.is_nan() => match self.on_nan {
                    BadDataBehavior::Throw => {
                        return Err(failure(
                            "Encountered NaN value during Exponential Moving output processing",
                        ))
                    }
                    BadDataBehavior::Poison => current = Some(f64::NAN),
                    BadDataBehavior::Reset => {
                        current = None;
                        last_stamp = NULL_LONG;
                    }
                    BadDataBehavior::Skip => {}
                },
                Some(v) if !self.timed => {
                    current = match current {
                        None => Some(v),
                        Some(held) => double_result(self.fold(held, v, alpha, one_minus_alpha)),
                    };
                }
                Some(v) => match (timestamps[index], current) {
                    (None, _) => {}
                    (Some(stamp), None) => {
                        current = Some(v);
                        last_stamp = stamp;
                    }
                    (Some(stamp), Some(held)) => {
                        let dt = stamp.wrapping_sub(last_stamp);
                        if dt < 0 {
                            return Err(failure(
                                "Timestamp values in UpdateBy operators must not decrease",
                            ));
                        }
                        if dt != 0 || !floating {
                            let alpha = (-dt as f64 / self.units).exp();
                            current = double_result(self.fold(held, v, alpha, 1.0 - alpha));
                            last_stamp = stamp;
                        }
                    }
                },
            }
            results.push(current);
        }
        Ok(double_array(results))
    }
}
