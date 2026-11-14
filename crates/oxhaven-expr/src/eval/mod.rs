//! Evaluation of a type-checked formula over Arrow arrays, one batch of rows at a time.

mod compare;
mod numeric;

use crate::types::{Checked, LogicalOp, Node, Numeric, Typed};
use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use numeric::{without_null_value, JavaNumber};
use oxhaven_contracts::expr::{Formula, Literal};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn arrow_failure(error: arrow::error::ArrowError) -> TableFailure {
    failure(error.to_string())
}

/// The input of one evaluation: the values of the formula's columns for the same rows.
struct Batch {
    /// One array per entry of [`Checked::columns`], each `rows` long.
    columns: Vec<ArrayRef>,
    rows: usize,
}

impl Batch {
    /// The rows for which `keep` is true.
    fn filter(&self, keep: &BooleanArray) -> Result<Batch, TableFailure> {
        let columns = self
            .columns
            .iter()
            .map(|column| arrow::compute::filter(column, keep).map_err(arrow_failure))
            .collect::<Result<_, _>>()?;
        Ok(Batch {
            columns,
            rows: keep.true_count(),
        })
    }
}

/// A formula ready to evaluate against tables that have the definition it was compiled for.
pub(crate) struct CompiledFormula {
    checked: Checked,
}

impl CompiledFormula {
    pub(crate) fn new(checked: Checked) -> Self {
        Self { checked }
    }
}

impl Formula for CompiledFormula {
    fn result_type(&self) -> &ColumnType {
        &self.checked.root.column_type
    }

    fn columns(&self) -> &[String] {
        &self.checked.columns
    }

    fn column_arrays(&self) -> &[String] {
        &[]
    }

    fn uses_row_variables(&self) -> bool {
        false
    }

    fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure> {
        let size = usize::try_from(rows.size())
            .map_err(|_| failure("too many rows for one evaluation"))?;
        let definition = table.definition();
        let columns = self
            .checked
            .columns
            .iter()
            .map(|name| {
                let index = definition
                    .index_of(name)
                    .ok_or_else(|| failure(format!("the table has no column {name}")))?;
                let source = table.column(index);
                let values = if use_prev {
                    source.prev_chunk(rows)?
                } else {
                    source.chunk(rows)?
                };
                if values.len() != size {
                    return Err(failure(format!(
                        "column {name} returned {} values for {size} rows",
                        values.len()
                    )));
                }
                Ok(without_null_values(values))
            })
            .collect::<Result<_, _>>()?;
        evaluate(
            &self.checked.root,
            &Batch {
                columns,
                rows: size,
            },
        )
    }
}

/// Makes null every cell that holds the value Java reserves for null in the column's type.
///
/// Behavior: the null values in `io.deephaven.util.QueryConstants`. `Boolean` and `String`
/// have none.
fn without_null_values(array: ArrayRef) -> ArrayRef {
    fn clean<P: arrow::datatypes::ArrowPrimitiveType>(
        array: &ArrayRef,
        null_value: P::Native,
    ) -> Option<ArrayRef> {
        without_null_value(array.as_primitive::<P>(), null_value).map(|a| Arc::new(a) as ArrayRef)
    }
    let cleaned = match array.data_type() {
        DataType::Int8 => clean::<Int8Type>(&array, i8::MIN),
        DataType::UInt16 => clean::<UInt16Type>(&array, u16::MAX),
        DataType::Int16 => clean::<Int16Type>(&array, i16::MIN),
        DataType::Int32 => clean::<Int32Type>(&array, i32::MIN),
        DataType::Int64 => clean::<Int64Type>(&array, i64::MIN),
        DataType::Float32 => clean::<Float32Type>(&array, -f32::MAX),
        DataType::Float64 => clean::<Float64Type>(&array, -f64::MAX),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            clean::<TimestampNanosecondType>(&array, i64::MIN)
        }
        _ => None,
    };
    cleaned.unwrap_or(array)
}

/// Evaluates `$body` with `$t` naming the Rust type of `$numeric`.
macro_rules! in_numeric_type {
    ($numeric:expr, |$t:ident| $body:expr) => {
        match $numeric {
            Some(Numeric::Int) => {
                type $t = i32;
                $body
            }
            Some(Numeric::Long) => {
                type $t = i64;
                $body
            }
            Some(Numeric::Float) => {
                type $t = f32;
                $body
            }
            Some(Numeric::Double) => {
                type $t = f64;
                $body
            }
            None => unreachable!("the type checker gave a numeric operator a non-numeric type"),
        }
    };
}

fn erase<A: Array + 'static>(array: A) -> ArrayRef {
    Arc::new(array)
}

fn evaluate(expr: &Typed, batch: &Batch) -> Result<ArrayRef, TableFailure> {
    let numeric = Numeric::of(&expr.column_type);
    match &expr.node {
        Node::Column(slot) => Ok(batch.columns[*slot].clone()),
        Node::Literal(literal) => Ok(constant(literal, batch.rows)),
        Node::Negate(operand) => {
            let operand = evaluate(operand, batch)?;
            Ok(in_numeric_type!(numeric, |T| erase(numeric::negate::<T>(
                operand.as_ref()
            ))))
        }
        // `QueryLanguageFunctionUtils#not(Boolean)`: null in, null out.
        Node::Not(operand) => {
            let operand = evaluate(operand, batch)?;
            arrow::compute::not(operand.as_boolean())
                .map(erase)
                .map_err(arrow_failure)
        }
        Node::Arithmetic { op, left, right } => {
            let left = evaluate(left, batch)?;
            let right = evaluate(right, batch)?;
            in_numeric_type!(numeric, |T| numeric::arithmetic::<T>(
                *op,
                left.as_ref(),
                right.as_ref()
            )
            .map(erase))
        }
        Node::Compare {
            op,
            domain,
            left,
            right,
        } => {
            let left = evaluate(left, batch)?;
            let right = evaluate(right, batch)?;
            Ok(Arc::new(compare::compare(
                *op,
                *domain,
                left.as_ref(),
                right.as_ref(),
            )))
        }
        Node::Logical { op, left, right } => logical(*op, left, right, batch),
        // `io.deephaven.function.Basic#isNull`.
        Node::IsNull(operand) => {
            let operand = evaluate(operand, batch)?;
            arrow::compute::is_null(operand.as_ref())
                .map(erase)
                .map_err(arrow_failure)
        }
    }
}

/// Evaluates `left && right` or `left || right`.
///
/// Behavior: `QueryLanguageParser#visit(BinaryExpr)` emits these as Java's own operators.
/// The right operand is evaluated only for the rows the left operand does not decide, so a
/// failure in it (a remainder by zero, a null) cannot arise from a row Java would have
/// skipped. A `Boolean` column is a boxed `java.lang.Boolean` in the generated code
/// (`io.deephaven.engine.table.impl.select.ConditionFilter#insertChunkValues`,
/// `DhFormulaColumn#columnSourceGetMethodReturnType`), so a null operand that is evaluated
/// throws `NullPointerException` when Java unboxes it.
fn logical(
    op: LogicalOp,
    left: &Typed,
    right: &Typed,
    batch: &Batch,
) -> Result<ArrayRef, TableFailure> {
    let null_operand =
        || failure("java.lang.NullPointerException: null Boolean operand of && or ||");

    let left = evaluate(left, batch)?;
    let left = left.as_boolean();
    if left.null_count() > 0 {
        return Err(null_operand());
    }
    // `&&` needs its right operand where the left is true, `||` where it is false.
    let undecided = match op {
        LogicalOp::And => left.clone(),
        LogicalOp::Or => arrow::compute::not(left).map_err(arrow_failure)?,
    };
    let undecided_rows = undecided.true_count();
    if undecided_rows == 0 {
        return Ok(Arc::new(left.clone()));
    }
    let right = if undecided_rows == batch.rows {
        evaluate(right, batch)?
    } else {
        evaluate(right, &batch.filter(&undecided)?)?
    };
    let right = right.as_boolean();
    if right.null_count() > 0 {
        return Err(null_operand());
    }
    // An undecided row takes the right operand's value; a decided row keeps the left's.
    let mut right_values = right.values().iter();
    let result: BooleanArray = undecided
        .values()
        .iter()
        .zip(left.values().iter())
        .map(|(undecided, left)| {
            Some(if undecided {
                right_values.next().unwrap_or(left)
            } else {
                left
            })
        })
        .collect();
    Ok(Arc::new(result))
}

/// `rows` copies of a literal, null where the literal is the value Java reserves for null in
/// its type.
fn constant(literal: &Literal, rows: usize) -> ArrayRef {
    fn number<T: JavaNumber>(value: T, array: impl FnOnce(T) -> ArrayRef, rows: usize) -> ArrayRef {
        if value == T::NULL {
            new_null_array(
                &<T::Arrow as arrow::datatypes::ArrowPrimitiveType>::DATA_TYPE,
                rows,
            )
        } else {
            array(value)
        }
    }
    match literal {
        Literal::Null => unreachable!("the type checker accepts null only as isNull"),
        Literal::Boolean(value) => Arc::new(BooleanArray::from(vec![*value; rows])),
        Literal::Char(u16::MAX) => new_null_array(&DataType::UInt16, rows),
        Literal::Char(value) => Arc::new(UInt16Array::from_value(*value, rows)),
        Literal::Int(value) => number(*value, |v| Arc::new(Int32Array::from_value(v, rows)), rows),
        Literal::Long(value) => number(*value, |v| Arc::new(Int64Array::from_value(v, rows)), rows),
        Literal::Float(value) => number(
            *value,
            |v| Arc::new(Float32Array::from_value(v, rows)),
            rows,
        ),
        Literal::Double(value) => number(
            *value,
            |v| Arc::new(Float64Array::from_value(v, rows)),
            rows,
        ),
        Literal::String(value) => Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            value, rows,
        ))),
        Literal::Instant(i64::MIN) => new_null_array(&ColumnType::Instant.arrow_type(), rows),
        Literal::Instant(value) => Arc::new(
            TimestampNanosecondArray::from_value(*value, rows)
                .with_data_type(ColumnType::Instant.arrow_type()),
        ),
    }
}
