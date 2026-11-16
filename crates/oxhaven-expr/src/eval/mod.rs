//! Evaluation of a type-checked formula over Arrow arrays, one batch of rows at a time.

mod arrays;
mod compare;
pub(crate) mod numeric;
mod objects;
mod text;

use crate::functions::Kernel;
use crate::types::{BitwiseOp, Checked, LogicalOp, Node, Numeric, Typed};
use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use numeric::{with_null_values, without_null_value, JavaPrimitive};
pub(crate) use objects::{class_of, is_primitive};
use oxhaven_contracts::expr::{Formula, Literal, RowVariable};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use std::sync::Arc;
pub(crate) use text::java_strings;

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
    /// The position of each row in the table, when the formula uses `i` or `ii`.
    positions: Option<Int64Array>,
    /// The key of each row, when the formula uses `k`.
    keys: Option<Int64Array>,
    /// One per entry of [`Checked::arrays`]: the columns the formula reads as arrays.
    arrays: Vec<Arc<arrays::ColumnArray>>,
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
        let longs = |values: &Option<Int64Array>| {
            values
                .as_ref()
                .map(|values| {
                    arrow::compute::filter(values, keep)
                        .map(|kept| kept.as_primitive::<Int64Type>().clone())
                        .map_err(arrow_failure)
                })
                .transpose()
        };
        Ok(Batch {
            columns,
            positions: longs(&self.positions)?,
            keys: longs(&self.keys)?,
            arrays: self.arrays.clone(),
            rows: keep.true_count(),
        })
    }

    /// The first row alone. The batch has one.
    fn first_row(&self) -> Batch {
        Batch {
            columns: self
                .columns
                .iter()
                .map(|column| column.slice(0, 1))
                .collect(),
            positions: self.positions.as_ref().map(|values| values.slice(0, 1)),
            keys: self.keys.as_ref().map(|values| values.slice(0, 1)),
            arrays: self.arrays.clone(),
            rows: 1,
        }
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
        &self.checked.arrays
    }

    fn uses_row_variables(&self) -> bool {
        self.checked.positions || self.checked.keys
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
                // A boxed primitive in an `Object` column may hold the value Java reserves
                // for null as a value.
                let boxed = holds_objects(&definition.columns()[index].column_type);
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
                Ok(if boxed {
                    values
                } else {
                    without_null_values(values)
                })
            })
            .collect::<Result<_, _>>()?;
        // Behavior: `DhFormulaColumn#generateFillChunkHelper`: the positions are those of
        // the rows in the table's row set, the previous one for previous values.
        let positions = if self.checked.positions {
            let all = if use_prev {
                table.prev_row_set()
            } else {
                table.row_set()
            };
            let positions = all
                .invert(rows)
                .map_err(|_| failure("a formula with i or ii was given rows the table lacks"))?;
            Some(positions.iter().collect::<Int64Array>())
        } else {
            None
        };
        let keys = self
            .checked
            .keys
            .then(|| rows.iter().collect::<Int64Array>());
        let arrays = self
            .checked
            .arrays
            .iter()
            .map(|name| {
                let index = definition
                    .index_of(name)
                    .ok_or_else(|| failure(format!("the table has no column {name}")))?;
                let all = if use_prev {
                    table.prev_row_set()
                } else {
                    table.row_set()
                };
                Ok(Arc::new(arrays::ColumnArray::new(
                    table.column(index),
                    all,
                    use_prev,
                )))
            })
            .collect::<Result<_, TableFailure>>()?;
        evaluate(
            &self.checked.root,
            &Batch {
                columns,
                positions,
                keys,
                arrays,
                rows: size,
            },
        )
    }
}

/// Applies `body` to the elements of `left` and `right`: see [`Node::Elementwise`].
///
/// Behavior: `QueryLanguageFunctionUtils#plusArray` and its relatives. A value beside an
/// array is paired with each element. Two arrays must have the same length ("Attempt to
/// ... two arrays ... of different length"), and an array that is null fails where Java
/// reads its length.
fn elementwise(
    body: &Typed,
    left: &Typed,
    right: &Typed,
    batch: &Batch,
    result: &ColumnType,
) -> Result<ArrayRef, TableFailure> {
    let sides = [
        (left, evaluate(left, batch)?),
        (right, evaluate(right, batch)?),
    ];
    // The number of elements in each row, from the first side that is a list.
    let mut lengths: Option<Vec<usize>> = None;
    for (side, values) in &sides {
        if side.column_type.list_element().is_none() {
            continue;
        }
        let list = values.as_list::<i32>();
        if list.null_count() > 0 {
            return Err(failure(
                "java.lang.NullPointerException: an operator was given a vector or array that \
                 is null",
            ));
        }
        let own: Vec<usize> = list
            .offsets()
            .windows(2)
            .map(|pair| (pair[1] - pair[0]) as usize)
            .collect();
        match &lengths {
            None => lengths = Some(own),
            Some(first) => {
                if let Some((a, b)) = first.iter().zip(&own).find(|(a, b)| a != b) {
                    return Err(failure(format!(
                        "Attempt to apply an operator to two arrays of different length \
                         (a.length={a}, b.length={b})"
                    )));
                }
            }
        }
    }
    let lengths = lengths.expect("the type checker found a list on one side");
    let total: usize = lengths.iter().sum();
    arrays::too_many_elements(total, 1)?;
    let columns = sides
        .iter()
        .map(|(side, values)| {
            if side.column_type.list_element().is_some() {
                let list = values.as_list::<i32>();
                let first = list.value_offsets()[0] as usize;
                Ok(list.values().slice(first, total))
            } else {
                let rows = lengths
                    .iter()
                    .enumerate()
                    .flat_map(|(row, length)| std::iter::repeat_n(row as u32, *length));
                let rows = arrow::array::UInt32Array::from_iter_values(rows);
                arrow::compute::take(values.as_ref(), &rows, None).map_err(arrow_failure)
            }
        })
        .collect::<Result<Vec<_>, TableFailure>>()?;
    let elements = evaluate(
        body,
        &Batch {
            columns,
            positions: None,
            keys: None,
            arrays: Vec::new(),
            rows: total,
        },
    )?;
    let DataType::List(field) = result.arrow_type() else {
        unreachable!("the type checker gave the operator an array type")
    };
    Ok(Arc::new(ListArray::new(
        field,
        arrow::buffer::OffsetBuffer::from_lengths(lengths),
        elements,
        None,
    )))
}

/// Whether the cells of a column of `column_type`, or their elements, are the cells of an
/// `Object` column, where a boxed primitive may hold the value Java reserves for null.
fn holds_objects(column_type: &ColumnType) -> bool {
    match column_type.list_element() {
        Some(element) => holds_objects(element),
        None => matches!(column_type, ColumnType::Object(_)),
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
        // The elements of a vector or array, which the functions of vectors read in place.
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            let elements = without_null_values(list.values().clone());
            (!Arc::ptr_eq(&elements, list.values())).then(|| {
                Arc::new(ListArray::new(
                    field.clone(),
                    list.offsets().clone(),
                    elements,
                    list.nulls().cloned(),
                )) as ArrayRef
            })
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

/// Evaluates `$body` with `$t` naming the Rust type of the primitive `$column_type`.
macro_rules! in_primitive_type {
    ($column_type:expr, |$t:ident| $body:expr) => {
        match $column_type {
            ColumnType::Byte => {
                type $t = i8;
                $body
            }
            ColumnType::Short => {
                type $t = i16;
                $body
            }
            ColumnType::Char => {
                type $t = u16;
                $body
            }
            ColumnType::Int => {
                type $t = i32;
                $body
            }
            ColumnType::Long => {
                type $t = i64;
                $body
            }
            ColumnType::Float => {
                type $t = f32;
                $body
            }
            ColumnType::Double => {
                type $t = f64;
                $body
            }
            other => unreachable!("the type checker gave a cast the target type {other:?}"),
        }
    };
}

fn erase<A: Array + 'static>(array: A) -> ArrayRef {
    Arc::new(array)
}

fn evaluate(expr: &Typed, batch: &Batch) -> Result<ArrayRef, TableFailure> {
    if !batch.arrays.is_empty() {
        if let Some(values) = arrays::evaluate(expr, batch)? {
            return Ok(values);
        }
    }
    let numeric = Numeric::of(&expr.column_type);
    match &expr.node {
        Node::Column(slot) => Ok(batch.columns[*slot].clone()),
        Node::ColumnArray(_) => unreachable!("a batch has the arrays its formula reads"),
        Node::RowVariable(variable) => {
            let positions = || {
                batch
                    .positions
                    .as_ref()
                    .expect("the checker asked for them")
            };
            match variable {
                // Behavior: `DhFormulaColumn#generateIntSize`: `i` is the position as an
                // `int`, and a position beyond the `int` range fails.
                RowVariable::I => positions()
                    .iter()
                    .map(|position| {
                        i32::try_from(position.expect("no nulls"))
                            .map(Some)
                            .map_err(|_| {
                                failure("FormulaColumn i usage: size exceeds Integer.MAX_VALUE")
                            })
                    })
                    .collect::<Result<Int32Array, _>>()
                    .map(erase),
                RowVariable::Ii => Ok(erase(positions().clone())),
                RowVariable::K => Ok(erase(
                    batch.keys.clone().expect("the checker asked for them"),
                )),
            }
        }
        Node::Literal(literal) => Ok(constant(literal, batch.rows)),
        Node::Null => Ok(new_null_array(&expr.column_type.arrow_type(), batch.rows)),
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
            )?))
        }
        Node::Logical { op, left, right } => logical(*op, left, right, batch),
        Node::BooleanBitwise { op, left, right } => {
            let left = evaluate(left, batch)?;
            let right = evaluate(right, batch)?;
            Ok(Arc::new(boolean_bitwise(
                *op,
                left.as_boolean(),
                right.as_boolean(),
            )))
        }
        Node::Cast(operand) => {
            let operand = evaluate(operand, batch)?;
            Ok(in_primitive_type!(&expr.column_type, |T| erase(
                numeric::cast::<T>(operand.as_ref())
            )))
        }
        // Java's implicit widening does not look for null: the operand is converted as
        // the value the generated code holds, which for null is the reserved value.
        Node::Widen(operand) => {
            let operand = with_null_values(&evaluate(operand, batch)?);
            Ok(in_primitive_type!(&expr.column_type, |T| erase(
                numeric::cast::<T>(operand.as_ref())
            )))
        }
        Node::Ternary {
            condition,
            if_true,
            if_false,
        } => ternary(condition, if_true, if_false, batch),
        Node::Concat { left, right } => {
            let left = evaluate(left, batch)?;
            let right = evaluate(right, batch)?;
            text::concat(left.as_ref(), right.as_ref()).map(erase)
        }
        Node::Call {
            kernel: Kernel::Now,
            ..
        } => Ok(crate::functions::volatile::now(batch.rows)),
        Node::Call {
            kernel: Kernel::Random(function),
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate(argument, batch).map(|values| with_null_values(&values)))
                .collect::<Result<Vec<_>, _>>()?;
            crate::functions::volatile::random(*function, &arguments, batch.rows)
        }
        Node::Call {
            kernel: Kernel::Static(method),
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate(argument, batch))
                .collect::<Result<Vec<_>, _>>()?;
            crate::functions::statics::evaluate(method, &arguments, batch.rows)
                .map(without_null_values)
        }
        Node::Call {
            kernel: Kernel::Color(function),
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate(argument, batch))
                .collect::<Result<Vec<_>, _>>()?;
            crate::functions::color::evaluate(*function, &arguments)
        }
        Node::Call { kernel, arguments } => {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate(argument, batch))
                .collect::<Result<Vec<_>, _>>()?;
            crate::functions::eval::evaluate(*kernel, &arguments)
        }
        Node::External {
            function,
            arguments,
        } => {
            let arguments = arguments
                .iter()
                .map(|argument| match argument {
                    Some(argument) => evaluate(argument, batch),
                    None => Ok(new_null_array(&DataType::Null, batch.rows)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let values = function.0.call(&arguments, batch.rows)?;
            let expected = expr.column_type.arrow_type();
            if values.len() != batch.rows || *values.data_type() != expected {
                return Err(failure(format!(
                    "an external function returned {} values of {:?} for {} rows of {expected:?}",
                    values.len(),
                    values.data_type(),
                    batch.rows
                )));
            }
            Ok(values)
        }
        Node::StringMethod {
            method,
            receiver,
            arguments,
        } => {
            let receiver = evaluate(receiver, batch)?;
            let arguments = arguments
                .iter()
                .map(|argument| evaluate(argument, batch))
                .collect::<Result<Vec<_>, _>>()?;
            text::string_method(*method, receiver.as_ref(), &arguments)
        }
        Node::ObjectText(receiver) => {
            let values = evaluate(receiver, batch)?;
            text::object_text(&receiver.column_type, values.as_ref()).map(erase)
        }
        Node::NewString(bytes) => {
            let bytes = evaluate(bytes, batch)?;
            text::new_string(bytes.as_ref()).map(erase)
        }
        Node::Elementwise { body, left, right } => {
            elementwise(body, left, right, batch, &expr.column_type)
        }
        Node::ToObject(operand) => {
            let values = evaluate(operand, batch)?;
            Ok(objects::to_object(&operand.column_type, values))
        }
        Node::FromObject(operand) => {
            let values = evaluate(operand, batch)?;
            let class = objects::class_of(&operand.column_type);
            objects::from_object(class.as_ref(), &expr.column_type, values)
        }
        Node::ObjectEquals { left, right } => {
            let left_values = evaluate(left, batch)?;
            let right_values = evaluate(right, batch)?;
            let (left_class, right_class) = (
                objects::class_of(&left.column_type),
                objects::class_of(&right.column_type),
            );
            objects::equals(
                left_class.as_ref(),
                right_class.as_ref(),
                &left_values,
                &right_values,
            )
            .map(erase)
        }
        Node::ObjectString(operand) => {
            let values = evaluate(operand, batch)?;
            let class = objects::class_of(&operand.column_type);
            objects::texts(class.as_ref(), &values).map(erase)
        }
        Node::DayOfWeek { number, method } => {
            let numbers = evaluate(number, batch)?;
            objects::days_of_week(&numbers, *method)
        }
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

/// Evaluates `condition ? if_true : if_false`.
///
/// Behavior: `QueryLanguageParser#visit(ConditionalExpr)` prints Java's own operator. Each
/// row evaluates only the branch its condition selects, so a failure in the other branch
/// cannot arise, and a null `Boolean` condition throws `NullPointerException` when Java
/// unboxes it.
fn ternary(
    condition: &Typed,
    if_true: &Typed,
    if_false: &Typed,
    batch: &Batch,
) -> Result<ArrayRef, TableFailure> {
    let condition = evaluate(condition, batch)?;
    let condition = condition.as_boolean();
    if condition.null_count() > 0 {
        return Err(failure(
            "java.lang.NullPointerException: null Boolean condition of ?:",
        ));
    }
    let selected = condition.true_count();
    if selected == batch.rows {
        return evaluate(if_true, batch);
    }
    if selected == 0 {
        return evaluate(if_false, batch);
    }
    let otherwise = arrow::compute::not(condition).map_err(arrow_failure)?;
    let branches = [
        evaluate(if_true, &batch.filter(condition)?)?,
        evaluate(if_false, &batch.filter(&otherwise)?)?,
    ];
    // Each row takes the next value of its branch.
    let mut taken = [0_usize; 2];
    let rows: Vec<(usize, usize)> = condition
        .values()
        .iter()
        .map(|selected| {
            let branch = usize::from(!selected);
            taken[branch] += 1;
            (branch, taken[branch] - 1)
        })
        .collect();
    arrow::compute::interleave(&[branches[0].as_ref(), branches[1].as_ref()], &rows)
        .map_err(arrow_failure)
}

/// `left & right`, `left | right` or `left ^ right` for `Boolean` operands. Both operands
/// are always evaluated.
///
/// Behavior: `QueryLanguageFunctionUtils#binaryAnd(Boolean,Boolean)`,
/// `#binaryOr(Boolean,Boolean)` and `#xor(Boolean,Boolean)`: null when either operand is
/// null. For two primitive `boolean` operands `QueryLanguageParser#visit(BinaryExpr)` prints
/// Java's own operator, which gives the same values.
fn boolean_bitwise(op: BitwiseOp, left: &BooleanArray, right: &BooleanArray) -> BooleanArray {
    left.iter()
        .zip(right.iter())
        .map(|operands| match operands {
            (Some(a), Some(b)) => Some(match op {
                BitwiseOp::And => a & b,
                BitwiseOp::Or => a | b,
                BitwiseOp::Xor => a ^ b,
            }),
            _ => None,
        })
        .collect()
}

/// `rows` copies of a literal, null where the literal is the value Java reserves for null in
/// its type.
fn constant(literal: &Literal, rows: usize) -> ArrayRef {
    fn number<T: JavaPrimitive>(
        value: T,
        array: impl FnOnce(T) -> ArrayRef,
        rows: usize,
    ) -> ArrayRef {
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
        Literal::Null => unreachable!("the type checker turns a null literal into a typed null"),
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
