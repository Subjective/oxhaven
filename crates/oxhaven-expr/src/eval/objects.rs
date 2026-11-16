//! Values of the classes Java holds as objects without a column type of their own: the
//! cells of an `Object` column and `java.time.DayOfWeek`.

use super::numeric::{self, with_null_values};
use super::{arrow_failure, failure, text};
use arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, Int8Array, StringArray,
};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type};
use oxhaven_contracts::schema::{ColumnType, TableClass};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

/// Whether `column_type` is a primitive, whose value Java boxes where a formula makes an
/// object of it.
pub(crate) fn is_primitive(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Char
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
    )
}

/// The class of the values of `column_type` as objects: the class an `Object` column
/// holds, `None` for one of nulls alone, and the type itself for any other, a primitive
/// standing for its box.
pub(crate) fn class_of(column_type: &ColumnType) -> Option<ColumnType> {
    match column_type {
        ColumnType::Object(class) => class.as_deref().cloned(),
        // A cell of a column of tables is a table whatever class the column names, and
        // tables are equal when they are the same table: their handles are equal.
        ColumnType::Table(_) => Some(ColumnType::Table(TableClass::Table)),
        other => Some(other.clone()),
    }
}

/// The class's name, as `Class#getName` gives it, for messages.
pub(crate) fn class_name(class: &ColumnType) -> &'static str {
    match class {
        ColumnType::Boolean => "java.lang.Boolean",
        ColumnType::Byte => "java.lang.Byte",
        ColumnType::Short => "java.lang.Short",
        ColumnType::Char => "java.lang.Character",
        ColumnType::Int => "java.lang.Integer",
        ColumnType::Long => "java.lang.Long",
        ColumnType::Float => "java.lang.Float",
        ColumnType::Double => "java.lang.Double",
        ColumnType::String => "java.lang.String",
        ColumnType::Instant => "java.time.Instant",
        ColumnType::LocalDate => "java.time.LocalDate",
        ColumnType::LocalTime => "java.time.LocalTime",
        ColumnType::DayOfWeek => "java.time.DayOfWeek",
        _ => "java.lang.Object",
    }
}

/// `values` of type `from` as objects: a primitive is boxed as the value Java holds, so
/// its null becomes the reserved value, which is no null in a box.
///
/// Behavior: Java's boxing conversion, which `QueryLanguageParser#visit(CastExpr)` leaves
/// to javac for `(Object) x`, and `QueryLanguageFunctionUtils#eq(Object, Object)` for an
/// operand that is a primitive
pub(crate) fn to_object(from: &ColumnType, values: ArrayRef) -> ArrayRef {
    if is_primitive(from) {
        with_null_values(&values)
    } else {
        values
    }
}

/// `(to) value` of the values of an `Object` column whose cells are of `class`.
///
/// Behavior: `QueryLanguageFunctionUtils#intCast(Object)`, `#longCast(Object)`,
/// `#floatCast(Object)`, `#doubleCast(Object)` (null for null, otherwise the `Number`'s
/// value converted as Java converts primitives, which is null where it lands on the
/// reserved value); Java's checked cast to a class, which keeps null and throws
/// `ClassCastException` for a value of another class
pub(crate) fn from_object(
    class: Option<&ColumnType>,
    to: &ColumnType,
    values: ArrayRef,
) -> Result<ArrayRef, TableFailure> {
    let Some(class) = class else {
        return Ok(new_null_array(&to.arrow_type(), values.len()));
    };
    let number = matches!(
        class,
        ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
    );
    if number
        && matches!(
            to,
            ColumnType::Int | ColumnType::Long | ColumnType::Float | ColumnType::Double
        )
    {
        let values = values.as_ref();
        return Ok(match to {
            ColumnType::Int => Arc::new(numeric::cast::<i32>(values)),
            ColumnType::Long => Arc::new(numeric::cast::<i64>(values)),
            ColumnType::Float => Arc::new(numeric::cast::<f32>(values)),
            _ => Arc::new(numeric::cast::<f64>(values)),
        });
    }
    if class == to && !is_primitive(to) {
        return Ok(values);
    }
    if values.null_count() == values.len() {
        return Ok(new_null_array(&to.arrow_type(), values.len()));
    }
    let target = if is_primitive(to) {
        "java.lang.Number"
    } else {
        class_name(to)
    };
    Err(failure(format!(
        "java.lang.ClassCastException: class {} cannot be cast to class {target}",
        class_name(class)
    )))
}

/// `left == right` of two values as objects, of the classes `left_class` and
/// `right_class`: true where both are null, and where both are values of one class that
/// `equals` each other. A `Float` or `Double` equals by its bits, every NaN being one, so
/// NaN equals NaN and `-0.0` does not equal `0.0`.
///
/// Behavior: `QueryLanguageFunctionUtils#eq(Object, Object)`, `java.lang.Double#equals`,
/// `java.lang.Float#equals`, `java.lang.Enum#equals`
pub(crate) fn equals(
    left_class: Option<&ColumnType>,
    right_class: Option<&ColumnType>,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<BooleanArray, TableFailure> {
    let same_class = matches!((left_class, right_class), (Some(l), Some(r)) if l == r);
    if !same_class {
        let rows = left.len();
        return Ok((0..rows)
            .map(|row| Some(left.is_null(row) && right.is_null(row)))
            .collect());
    }
    let canonical = |values: &ArrayRef| -> ArrayRef {
        match values.data_type() {
            DataType::Float32 => Arc::new(
                values
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(|v| if v.is_nan() { f32::NAN } else { v }),
            ),
            DataType::Float64 => Arc::new(
                values
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(|v| if v.is_nan() { f64::NAN } else { v }),
            ),
            _ => values.clone(),
        }
    };
    arrow::compute::kernels::cmp::not_distinct(&canonical(left), &canonical(right))
        .map_err(arrow_failure)
}

/// The text of each value, of class `class`, as Java converts an object to a string;
/// null stays null.
///
/// Behavior: `java.lang.String#valueOf(Object)`, the `toString` of the boxes, of `String`,
/// `java.time.Instant`, `LocalDate`, `LocalTime` and `DayOfWeek`
pub(crate) fn texts(
    class: Option<&ColumnType>,
    values: &ArrayRef,
) -> Result<StringArray, TableFailure> {
    let Some(class) = class else {
        return Ok(StringArray::new_null(values.len()));
    };
    if *class == ColumnType::DayOfWeek {
        return Ok(values
            .as_primitive::<arrow::datatypes::Int8Type>()
            .iter()
            .map(|day| day.map(day_of_week_name))
            .collect());
    }
    let all = text::java_strings(values.as_ref())?;
    Ok(all
        .into_iter()
        .enumerate()
        .map(|(row, text)| values.is_valid(row).then_some(text))
        .collect())
}

/// The name of the day whose number is `day`, Monday 1.
///
/// Behavior: `java.time.DayOfWeek#of`, `java.lang.Enum#toString`
fn day_of_week_name(day: i8) -> &'static str {
    const NAMES: [&str; 7] = [
        "MONDAY",
        "TUESDAY",
        "WEDNESDAY",
        "THURSDAY",
        "FRIDAY",
        "SATURDAY",
        "SUNDAY",
    ];
    usize::try_from(i32::from(day) - 1)
        .ok()
        .and_then(|at| NAMES.get(at))
        .copied()
        .unwrap_or_else(|| unreachable!("a DayOfWeek is 1 to 7, not {day}"))
}

/// The days of the week whose numbers (Monday 1) are `numbers`, an `int` array that is
/// null where the day is. With `method`, a null is the `NullPointerException` of a method
/// called on a null receiver.
///
/// Behavior: `DateTimeUtils#dayOfWeek(Instant, ZoneId)`, `#dayOfWeek(ZonedDateTime)` and
/// `#dayOfWeek(LocalDate)`, null for null; `java.time.LocalDate#getDayOfWeek`
pub(crate) fn days_of_week(numbers: &ArrayRef, method: bool) -> Result<ArrayRef, TableFailure> {
    let numbers = numbers.as_primitive::<Int32Type>();
    if method && numbers.null_count() > 0 {
        return Err(failure(
            "java.lang.NullPointerException: getDayOfWeek() called on a null LocalDate",
        ));
    }
    let days: Int8Array = numbers
        .iter()
        .map(|number| number.map(|number| number as i8))
        .collect();
    Ok(Arc::new(days))
}
