//! Formulas over `BigDecimal` and `BigInteger` values: comparisons of them with each other
//! and with numbers, and the operators, casts, methods and functions of [`Node::Big`].
//!
//! [`Node::Big`]: crate::types::Node::Big

use super::failure;
use super::numeric::convert;
use crate::decimal::{integer, integer_bytes, integer_of_cell, Decimal, Failure, Rounding};
use crate::types::big::{BigFn, Method};
use crate::types::{ArithmeticOp, BigOperand, Typed};
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanArray, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, UInt16Array,
};
use arrow::datatypes::DataType;
use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
use std::sync::Arc;

/// A value a comparison reads: a number, or NaN, which compares with nothing.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BigValue {
    Number(Decimal),
    NaN,
}

/// The values of one side of a comparison: each cell as a number, NaN, or `None` for null.
///
/// A primitive operand is converted as the overloads of `QueryLanguageFunctionUtils#compareTo`
/// with a `BigDecimal` or `BigInteger` convert it: an integer or `char` by
/// `BigDecimal#valueOf(long)` or `BigInteger#valueOf(long)`, a `float` widened to `double`,
/// and a `double` by `BigDecimal#valueOf(double)`, which fails for an infinity. A
/// `BigInteger` is compared through `new BigDecimal(BigInteger)` where the other side is a
/// decimal, which keeps its value.
///
/// Behavior: `QueryLanguageFunctionUtils#compareTo(BigDecimal, double)`,
/// `#compareTo(BigInteger, float)`, `#compareTo(BigInteger, long)`,
/// `#compareTo(BigDecimal, BigInteger)` and the other overloads with a big number
pub(crate) fn values(
    operand: BigOperand,
    array: &dyn Array,
) -> Result<Vec<Option<BigValue>>, TableFailure> {
    match operand {
        BigOperand::Decimal => decimals(array)?
            .into_iter()
            .map(|d| Ok(d.map(BigValue::Number)))
            .collect(),
        BigOperand::Integer => Ok(integers(array)?
            .into_iter()
            .map(|i| i.map(|i| BigValue::Number(Decimal::of_integer(i))))
            .collect()),
        BigOperand::Primitive => match array.data_type() {
            DataType::Float32 | DataType::Float64 => convert::<f64>(array)
                .iter()
                .map(|value| {
                    value
                        .map(|v| match v.is_nan() {
                            true => Ok(BigValue::NaN),
                            false => Decimal::of_double(v).map(BigValue::Number).map_err(failure),
                        })
                        .transpose()
                })
                .collect(),
            _ => Ok(convert::<i64>(array)
                .iter()
                .map(|value| value.map(|v| BigValue::Number(Decimal::of_long(v))))
                .collect()),
        },
    }
}

fn binary_cells(array: &dyn Array) -> Result<&arrow::array::BinaryArray, TableFailure> {
    array
        .as_binary_opt::<i32>()
        .ok_or_else(|| failure("a big number column needs binary cells"))
}

fn decimals(array: &dyn Array) -> Result<Vec<Option<Decimal>>, TableFailure> {
    binary_cells(array)?
        .iter()
        .map(|cell| {
            cell.map(|cell| {
                Decimal::of_cell(cell).ok_or_else(|| failure("a BigDecimal cell without a number"))
            })
            .transpose()
        })
        .collect()
}

fn integers(array: &dyn Array) -> Result<Vec<Option<BigInt>>, TableFailure> {
    Ok(binary_cells(array)?
        .iter()
        .map(|cell| cell.map(integer_of_cell))
        .collect())
}

/// One cell of an argument or a result of [`Node::Big`](crate::types::Node::Big).
#[derive(Clone, Debug)]
enum Value {
    Null,
    Decimal(Decimal),
    Integer(BigInt),
    /// An integer of a primitive type or a `char`, widened.
    Long(i64),
    /// A `float` or a `double`, widened.
    Double(f64),
    Boolean(bool),
    Text(String),
    /// A value of another class, which only `equals` takes.
    Other,
}

/// The cells of `array`, of type `column_type`: Arrow's nulls are [`Value::Null`], but
/// with `null_values` a primitive's null is the value Java reserves for null in its type,
/// as an argument of a Java method has it.
fn read(
    column_type: &ColumnType,
    array: &dyn Array,
    null_values: bool,
) -> Result<Vec<Value>, TableFailure> {
    let rows = array.len();
    let present = |row: usize| !array.is_null(row);
    Ok(match column_type {
        ColumnType::BigDecimal => decimals(array)?
            .into_iter()
            .map(|d| d.map_or(Value::Null, Value::Decimal))
            .collect(),
        ColumnType::BigInteger => integers(array)?
            .into_iter()
            .map(|i| i.map_or(Value::Null, Value::Integer))
            .collect(),
        ColumnType::Byte
        | ColumnType::Short
        | ColumnType::Char
        | ColumnType::Int
        | ColumnType::Long => {
            let null = null_values.then(|| match column_type {
                ColumnType::Byte => i64::from(i8::MIN),
                ColumnType::Short => i64::from(i16::MIN),
                ColumnType::Char => i64::from(u16::MAX),
                ColumnType::Int => i64::from(i32::MIN),
                _ => i64::MIN,
            });
            convert::<i64>(array)
                .iter()
                .map(|v| v.or(null).map_or(Value::Null, Value::Long))
                .collect()
        }
        ColumnType::Float | ColumnType::Double => {
            let null = null_values.then(|| match column_type {
                ColumnType::Float => f64::from(-f32::MAX),
                _ => -f64::MAX,
            });
            convert::<f64>(array)
                .iter()
                .map(|v| v.or(null).map_or(Value::Null, Value::Double))
                .collect()
        }
        ColumnType::Boolean => array
            .as_boolean()
            .iter()
            .map(|v| v.map_or(Value::Null, Value::Boolean))
            .collect(),
        ColumnType::String if *array.data_type() == DataType::Utf8 => array
            .as_string::<i32>()
            .iter()
            .map(|v| v.map_or(Value::Null, |v| Value::Text(v.to_string())))
            .collect(),
        _ => (0..rows)
            .map(|row| match present(row) {
                true => Value::Other,
                false => Value::Null,
            })
            .collect(),
    })
}

/// The array of `values`, of type `column_type`. A primitive equal to its type's null
/// value is null, because that is how Java stores null.
fn build(column_type: &ColumnType, values: Vec<Value>) -> Result<ArrayRef, TableFailure> {
    let wrong = || {
        failure(format!(
            "a big number function gave a value that is no {column_type:?}"
        ))
    };
    macro_rules! primitive {
        ($array:ty, $native:ty, $null:expr, |$v:ident| $value:expr) => {{
            let cells = values
                .into_iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    $v => {
                        let cell: Option<$native> = $value;
                        let cell = cell.ok_or_else(wrong)?;
                        Ok((cell != $null).then_some(cell))
                    }
                })
                .collect::<Result<Vec<Option<$native>>, TableFailure>>()?;
            Arc::new(<$array>::from(cells)) as ArrayRef
        }};
    }
    let long = |value: Value| match value {
        Value::Long(v) => Some(v),
        _ => None,
    };
    let double = |value: Value| match value {
        Value::Double(v) => Some(v),
        _ => None,
    };
    Ok(match column_type {
        ColumnType::BigDecimal | ColumnType::BigInteger => {
            let mut builder = BinaryBuilder::new();
            for value in values {
                match value {
                    Value::Null => builder.append_null(),
                    Value::Decimal(d) if *column_type == ColumnType::BigDecimal => {
                        builder.append_value(d.cell())
                    }
                    Value::Integer(i) if *column_type == ColumnType::BigInteger => {
                        builder.append_value(integer_bytes(&i))
                    }
                    _ => return Err(wrong()),
                }
            }
            Arc::new(builder.finish())
        }
        ColumnType::Byte => primitive!(Int8Array, i8, i8::MIN, |v| long(v).map(|v| v as i8)),
        ColumnType::Short => primitive!(Int16Array, i16, i16::MIN, |v| long(v).map(|v| v as i16)),
        ColumnType::Char => primitive!(UInt16Array, u16, u16::MAX, |v| long(v).map(|v| v as u16)),
        ColumnType::Int => primitive!(Int32Array, i32, i32::MIN, |v| long(v).map(|v| v as i32)),
        ColumnType::Long => primitive!(Int64Array, i64, i64::MIN, |v| long(v)),
        ColumnType::Float => {
            primitive!(Float32Array, f32, -f32::MAX, |v| double(v)
                .map(|v| v as f32))
        }
        ColumnType::Double => primitive!(Float64Array, f64, -f64::MAX, |v| double(v)),
        ColumnType::Boolean => Arc::new(
            values
                .into_iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Boolean(b) => Ok(Some(b)),
                    _ => Err(wrong()),
                })
                .collect::<Result<BooleanArray, TableFailure>>()?,
        ),
        ColumnType::String => Arc::new(
            values
                .into_iter()
                .map(|value| match value {
                    Value::Null => Ok(None),
                    Value::Text(t) => Ok(Some(t)),
                    _ => Err(wrong()),
                })
                .collect::<Result<StringArray, TableFailure>>()?,
        ),
        _ => return Err(wrong()),
    })
}

/// Evaluates a [`Node::Big`](crate::types::Node::Big) of type `column_type` whose arguments
/// have the values `arguments`, `rows` long.
pub(crate) fn evaluate(
    function: &BigFn,
    column_type: &ColumnType,
    typed: &[Typed],
    arguments: &[ArrayRef],
    rows: usize,
) -> Result<ArrayRef, TableFailure> {
    let columns = typed
        .iter()
        .zip(arguments)
        .map(|(typed, array)| {
            // An operator takes a primitive's null value as null; a Java method takes null
            // as that value.
            match function {
                BigFn::Operator(_) => {
                    let array = super::without_null_values(array.clone());
                    read(&typed.column_type, array.as_ref(), false)
                }
                _ => read(&typed.column_type, array.as_ref(), true),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut values = Vec::with_capacity(rows);
    let mut row_values = Vec::with_capacity(columns.len());
    for row in 0..rows {
        row_values.clear();
        row_values.extend(columns.iter().map(|column| column[row].clone()));
        let value = compute(function, column_type, &row_values).map_err(failure)?;
        values.push(value);
    }
    build(column_type, values)
}

fn null_pointer(what: &str) -> Failure {
    format!("java.lang.NullPointerException: {what}")
}

/// A decimal operand of an operator: a big number or a primitive as
/// `QueryLanguageFunctionUtils` converts it.
fn as_decimal(value: &Value) -> Result<Decimal, Failure> {
    Ok(match value {
        Value::Decimal(d) => d.clone(),
        Value::Integer(i) => Decimal::of_integer(i.clone()),
        Value::Long(v) => Decimal::of_long(*v),
        Value::Double(v) => Decimal::of_double(*v)?,
        _ => return Err("a big number operator of a value that is no number".into()),
    })
}

fn as_integer(value: &Value) -> Result<BigInt, Failure> {
    Ok(match value {
        Value::Integer(i) => i.clone(),
        Value::Long(v) => BigInt::from(*v),
        _ => return Err("a BigInteger operator of a value that is no integer".into()),
    })
}

/// An `int` argument, which [`read`] gave Java's null value for null.
fn as_int(value: &Value) -> i32 {
    match value {
        Value::Long(v) => *v as i32,
        _ => i32::MIN,
    }
}

fn as_long(value: &Value) -> i64 {
    match value {
        Value::Long(v) => *v,
        _ => i64::MIN,
    }
}

fn rounding(value: &Value) -> Result<Rounding, Failure> {
    Rounding::of_number(as_int(value).into())
}

/// The primitive `(T) value` gives, `T` being `column_type`.
///
/// Behavior: `java.math.BigDecimal#intValue`, `#longValue`, `#doubleValue`, `#floatValue`,
/// the same of `java.math.BigInteger`, and `Number#shortValue` and `#byteValue`, which
/// narrow `intValue`.
fn primitive_of(column_type: &ColumnType, value: &Value) -> Result<Value, Failure> {
    let floating = matches!(column_type, ColumnType::Float | ColumnType::Double);
    Ok(match value {
        Value::Null => Value::Null,
        Value::Decimal(d) if floating => match column_type {
            ColumnType::Float => Value::Double(d.to_float().into()),
            _ => Value::Double(d.to_double()),
        },
        Value::Integer(i) if floating => match column_type {
            ColumnType::Float => Value::Double(integer::to_float(i).into()),
            _ => Value::Double(integer::to_double(i)),
        },
        Value::Decimal(d) => Value::Long(d.low_bits()),
        Value::Integer(i) => Value::Long(integer::low_bits(i)),
        _ => return Err("a cast of a value that is no big number".into()),
    })
}

fn compute(function: &BigFn, column_type: &ColumnType, args: &[Value]) -> Result<Value, Failure> {
    match function {
        BigFn::Operator(op) => operator(*op, column_type, &args[0], &args[1]),
        BigFn::Negate => Ok(match &args[0] {
            Value::Decimal(d) => Value::Decimal(d.negate()),
            Value::Integer(i) => Value::Integer(-i),
            _ => Value::Null,
        }),
        BigFn::Cast => primitive_of(column_type, &args[0]),
        BigFn::CharCast => match &args[0] {
            Value::Null => Ok(Value::Null),
            value => {
                let class = match value {
                    Value::Decimal(_) => "java.math.BigDecimal",
                    _ => "java.math.BigInteger",
                };
                Err(format!(
                    "java.lang.ClassCastException: class {class} cannot be cast to class \
                     java.lang.Character ({class} and java.lang.Character are in module \
                     java.base of loader 'bootstrap')"
                ))
            }
        },
        BigFn::Text => Ok(match &args[0] {
            Value::Decimal(d) => Value::Text(d.text()),
            Value::Integer(i) => Value::Text(i.to_string()),
            _ => Value::Null,
        }),
        BigFn::Method(method) => self::method(*method, column_type, args),
        BigFn::Constant(made) => match made {
            Ok(cell) => Ok(match column_type {
                ColumnType::BigDecimal => Value::Decimal(
                    Decimal::of_cell(cell).ok_or("a BigDecimal constant without a number")?,
                ),
                _ => Value::Integer(integer_of_cell(cell)),
            }),
            Err(message) => Err(message.clone()),
        },
        // Behavior: `java.math.BigDecimal#valueOf(long)`, `#valueOf(double)`,
        // `java.math.BigInteger#valueOf(long)`
        BigFn::ValueOf => Ok(match (&args[0], column_type) {
            (Value::Double(v), _) => Value::Decimal(Decimal::of_double(*v)?),
            (value, ColumnType::BigInteger) => Value::Integer(BigInt::from(as_long(value))),
            (value, _) => Value::Decimal(Decimal::of_long(as_long(value))),
        }),
        // Behavior: `java.math.BigDecimal#valueOf(long, int)`
        BigFn::ValueOfScaled => Ok(Value::Decimal(Decimal::new(
            BigInt::from(as_long(&args[0])),
            as_int(&args[1]),
        ))),
        // Behavior: `java.math.BigDecimal#BigDecimal(int)`, `(long)`, `(double)`
        BigFn::New => Ok(Value::Decimal(match &args[0] {
            Value::Double(v) => Decimal::of_double_exactly(*v)?,
            value => Decimal::of_long(as_long(value)),
        })),
        // Behavior: `java.math.BigDecimal#BigDecimal(BigInteger)` and
        // `#BigDecimal(BigInteger, int)`
        BigFn::NewOfInteger => {
            let Value::Integer(unscaled) = &args[0] else {
                return Err(null_pointer("new java.math.BigDecimal(null BigInteger)"));
            };
            let scale = args.get(1).map_or(0, as_int);
            Ok(Value::Decimal(Decimal::new(unscaled.clone(), scale)))
        }
        // Behavior: `java.math.BigDecimal#BigDecimal(String)` and
        // `java.math.BigInteger#BigInteger(String)`, which throw for null.
        BigFn::Parse => {
            let text = match &args[0] {
                Value::Text(text) => text,
                _ => return Err(null_pointer("a null String to parse as a number")),
            };
            if !text.is_ascii() {
                oxhaven_contracts::unported!(
                    "java.math.BigDecimal#BigDecimal(String)",
                    "a number in characters that are not ASCII, which Java reads as digits \
                     where Character#digit has them"
                );
            }
            let invalid =
                || format!("java.lang.NumberFormatException: For input string: \"{text}\"");
            Ok(match column_type {
                ColumnType::BigDecimal => Value::Decimal(Decimal::parse(text).ok_or_else(invalid)?),
                _ => Value::Integer(integer::parse(text).ok_or_else(invalid)?),
            })
        }
        // Behavior: `io.deephaven.function.Numeric#minObj` and `#maxObj`: nulls are
        // skipped, and of equal values the first stays.
        BigFn::Extreme(maximum) => {
            let mut best: Option<&Value> = None;
            for value in args {
                if matches!(value, Value::Null) {
                    continue;
                }
                let better = match best {
                    None => true,
                    Some(best) => {
                        let ordering = compare(value, best)?;
                        if *maximum {
                            ordering == Ordering::Greater
                        } else {
                            ordering == Ordering::Less
                        }
                    }
                };
                if better {
                    best = Some(value);
                }
            }
            Ok(best.cloned().unwrap_or(Value::Null))
        }
        // Behavior: `io.deephaven.function.Basic#ifelseObj(Boolean, T, T)`: null for a null
        // condition.
        BigFn::IfElse => Ok(match &args[0] {
            Value::Boolean(true) => args[1].clone(),
            Value::Boolean(false) => args[2].clone(),
            _ => Value::Null,
        }),
    }
}

/// `compareTo` of two values of one class.
fn compare(a: &Value, b: &Value) -> Result<Ordering, Failure> {
    match (a, b) {
        (Value::Decimal(a), Value::Decimal(b)) => Ok(a.compare(b)),
        (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
        _ => Err("a comparison of big numbers of two classes".into()),
    }
}

/// Behavior: `QueryLanguageFunctionUtils#plus`, `#minus`, `#multiply`, `#divide` and
/// `#remainder` of a big number with a number, a `char` or another big number: null when
/// either is null; a `BigInteger` result is computed with integers, a `BigDecimal` one with
/// decimals. `divide` is `a.divide(b, max(a.scale(), b.scale(), 8), HALF_UP)`
/// (`QueryLanguageFunctionUtils.DEFAULT_SCALE`), `remainder` `BigDecimal#remainder`.
fn operator(
    op: ArithmeticOp,
    column_type: &ColumnType,
    left: &Value,
    right: &Value,
) -> Result<Value, Failure> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if *column_type == ColumnType::BigInteger {
        let (a, b) = (as_integer(left)?, as_integer(right)?);
        return Ok(Value::Integer(match op {
            ArithmeticOp::Add => a + b,
            ArithmeticOp::Subtract => a - b,
            ArithmeticOp::Multiply => a * b,
            ArithmeticOp::Remainder => integer::remainder(&a, &b)?,
            ArithmeticOp::Divide | ArithmeticOp::Bitwise(_) => {
                unreachable!("a quotient is a BigDecimal, and no big number has & | ^")
            }
        }));
    }
    let (a, b) = (as_decimal(left)?, as_decimal(right)?);
    Ok(Value::Decimal(match op {
        ArithmeticOp::Add => a.add(&b)?,
        ArithmeticOp::Subtract => a.subtract(&b)?,
        ArithmeticOp::Multiply => a.multiply(&b)?,
        ArithmeticOp::Divide => {
            let scale = a.scale.max(b.scale).max(8);
            a.divide_to_scale(&b, scale, Rounding::HalfUp)?
        }
        ArithmeticOp::Remainder => a.remainder(&b)?,
        ArithmeticOp::Bitwise(_) => unreachable!("no big number has & | ^"),
    }))
}

/// `receiver.method(arguments)`, the receiver being `args[0]`.
fn method(method: Method, column_type: &ColumnType, args: &[Value]) -> Result<Value, Failure> {
    let receiver = &args[0];
    let parameters = &args[1..];
    if matches!(receiver, Value::Null) {
        return Err(null_pointer(&format!(
            "Cannot invoke \"{}\" because the receiver is null",
            name(method)
        )));
    }
    // `equals` takes any object, null included.
    if method == Method::Equals {
        return Ok(Value::Boolean(match (receiver, &parameters[0]) {
            (Value::Decimal(a), Value::Decimal(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            _ => false,
        }));
    }
    // A big number argument may not be null.
    let other = || match parameters.first() {
        Some(Value::Null) | None => Err(null_pointer(&format!(
            "a null argument of {}",
            name(method)
        ))),
        Some(value) => Ok(value),
    };
    match receiver {
        Value::Decimal(d) => decimal_method(method, column_type, d, parameters, other),
        Value::Integer(i) => integer_method(method, column_type, i, parameters, other),
        _ => Err("a method of a value that is no big number".into()),
    }
}

fn name(method: Method) -> String {
    format!("{method:?}")
}

fn decimal_of(value: &Value) -> Result<&Decimal, Failure> {
    match value {
        Value::Decimal(d) => Ok(d),
        _ => Err("a BigDecimal argument that is no BigDecimal".into()),
    }
}

fn integer_of(value: &Value) -> Result<&BigInt, Failure> {
    match value {
        Value::Integer(i) => Ok(i),
        _ => Err("a BigInteger argument that is no BigInteger".into()),
    }
}

/// Behavior: the methods of `java.math.BigDecimal` in [`Method`].
fn decimal_method<'a>(
    method: Method,
    column_type: &ColumnType,
    d: &Decimal,
    parameters: &'a [Value],
    other: impl Fn() -> Result<&'a Value, Failure>,
) -> Result<Value, Failure> {
    let int = |at: usize| as_int(&parameters[at]);
    let decimal = |d: Decimal| Ok(Value::Decimal(d));
    match method {
        Method::Add => decimal(d.add(decimal_of(other()?)?)?),
        Method::Subtract => decimal(d.subtract(decimal_of(other()?)?)?),
        Method::Multiply => decimal(d.multiply(decimal_of(other()?)?)?),
        Method::Divide => decimal(d.divide_exactly(decimal_of(other()?)?)?),
        Method::DivideToScale => {
            let divisor = decimal_of(other()?)?;
            decimal(d.divide_to_scale(divisor, int(1), rounding(&parameters[2])?)?)
        }
        Method::DivideRounded => {
            let divisor = decimal_of(other()?)?;
            decimal(d.divide_to_scale(divisor, d.scale, rounding(&parameters[1])?)?)
        }
        Method::DivideToIntegralValue => decimal(d.divide_to_integral(decimal_of(other()?)?)?),
        Method::Remainder => decimal(d.remainder(decimal_of(other()?)?)?),
        Method::Pow => decimal(d.pow(int(0))?),
        // Behavior: `java.math.BigDecimal#max` and `#min`: this one where they are equal.
        Method::Max | Method::Min => {
            let b = decimal_of(other()?)?;
            let ordering = d.compare(b);
            let keep = match method {
                Method::Max => ordering != Ordering::Less,
                _ => ordering != Ordering::Greater,
            };
            decimal(if keep { d.clone() } else { b.clone() })
        }
        Method::CompareTo => Ok(Value::Long(ordering_number(
            d.compare(decimal_of(other()?)?),
        ))),
        Method::Negate => decimal(d.negate()),
        Method::Abs => decimal(d.abs()),
        Method::Plus => decimal(d.clone()),
        Method::Signum => Ok(Value::Long(d.signum().into())),
        Method::Scale => Ok(Value::Long(d.scale.into())),
        Method::Precision => Ok(Value::Long(d.precision().into())),
        Method::UnscaledValue => Ok(Value::Integer(d.unscaled.clone())),
        Method::SetScale => decimal(d.set_scale(int(0), Rounding::Unnecessary)?),
        Method::SetScaleRounded => decimal(d.set_scale(int(0), rounding(&parameters[1])?)?),
        Method::StripTrailingZeros => decimal(d.strip_trailing_zeros()),
        Method::MovePointLeft => decimal(d.move_point_left(int(0))?),
        Method::MovePointRight => decimal(d.move_point_right(int(0))?),
        Method::ScaleByPowerOfTen => decimal(d.scale_by_power_of_ten(int(0))?),
        Method::Ulp => decimal(d.ulp()),
        Method::ToBigInteger => Ok(Value::Integer(d.to_integer()?)),
        Method::Value => primitive_of(column_type, &Value::Decimal(d.clone())),
        Method::ToText => Ok(Value::Text(d.text())),
        Method::ToPlainText => Ok(Value::Text(d.plain_text())),
        _ => Err(format!(
            "{} is no method of java.math.BigDecimal",
            name(method)
        )),
    }
}

fn ordering_number(ordering: Ordering) -> i64 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// The most bits a shift may give, as [`crate::decimal`] bounds its powers of ten: Java
/// computes any, and runs out of memory for the largest.
const MAX_SHIFT_BITS: u64 = 4_000_000;

fn shifted_left(i: &BigInt, by: i64) -> Result<BigInt, Failure> {
    if by >= 0 {
        if !i.is_zero() && i.bits() + by as u64 > MAX_SHIFT_BITS {
            return Err(format!(
                "a shift to {} bits, which Java does not finish computing",
                i.bits() + by as u64
            ));
        }
        return Ok(i << by as u64);
    }
    // An arithmetic shift right rounds toward negative infinity.
    let by = by.unsigned_abs();
    if by >= i.bits() {
        return Ok(if i.is_negative() {
            BigInt::from(-1)
        } else {
            BigInt::zero()
        });
    }
    Ok(i >> by)
}

/// Behavior: the methods of `java.math.BigInteger` in [`Method`].
fn integer_method<'a>(
    method: Method,
    column_type: &ColumnType,
    i: &BigInt,
    parameters: &'a [Value],
    other: impl Fn() -> Result<&'a Value, Failure>,
) -> Result<Value, Failure> {
    let int = |at: usize| as_int(&parameters[at]);
    let integer = |i: BigInt| Ok(Value::Integer(i));
    match method {
        Method::Add => integer(i + integer_of(other()?)?),
        Method::Subtract => integer(i - integer_of(other()?)?),
        Method::Multiply => integer(i * integer_of(other()?)?),
        Method::Divide => integer(integer::divide(i, integer_of(other()?)?)?),
        Method::Remainder => integer(integer::remainder(i, integer_of(other()?)?)?),
        Method::Mod => integer(integer::modulo(i, integer_of(other()?)?)?),
        Method::Pow => integer(integer::pow(i, int(0))?),
        Method::Gcd => integer(integer::gcd(i, integer_of(other()?)?)),
        Method::Max => integer(i.max(integer_of(other()?)?).clone()),
        Method::Min => integer(i.min(integer_of(other()?)?).clone()),
        Method::CompareTo => Ok(Value::Long(ordering_number(i.cmp(integer_of(other()?)?)))),
        Method::Negate => integer(-i),
        Method::Abs => integer(i.abs()),
        Method::Signum => Ok(Value::Long(integer::signum(i).into())),
        Method::Value => primitive_of(column_type, &Value::Integer(i.clone())),
        Method::ToText => Ok(Value::Text(i.to_string())),
        // Behavior: `java.math.BigInteger#toString(int)`: base 10 for a radix outside 2
        // to 36, lower case letters.
        Method::ToTextRadix => {
            let radix = int(0);
            let radix = if (2..=36).contains(&radix) { radix } else { 10 };
            Ok(Value::Text(i.to_str_radix(radix as u32)))
        }
        Method::BitLength => Ok(Value::Long(integer::bit_length(i).into())),
        // Behavior: `java.math.BigInteger#bitCount`: the bits that differ from the sign
        // bit, which for a negative number are the ones of its complement.
        Method::BitCount => {
            let bits = if i.is_negative() { -i - 1 } else { i.clone() };
            let ones: u64 = bits
                .magnitude()
                .iter_u64_digits()
                .map(|d| u64::from(d.count_ones()))
                .sum();
            Ok(Value::Long(ones as i64))
        }
        // Behavior: `java.math.BigInteger#testBit`: of the two's complement form;
        // "Negative bit address" below zero.
        Method::TestBit => {
            let n = int(0);
            if n < 0 {
                return Err("java.lang.ArithmeticException: Negative bit address".into());
            }
            let shifted = shifted_left(i, -i64::from(n))?;
            Ok(Value::Boolean(shifted.bit(0)))
        }
        // Behavior: `java.math.BigInteger#shiftLeft` and `#shiftRight`: a negative distance
        // shifts the other way; to the right, toward negative infinity.
        Method::ShiftLeft => integer(shifted_left(i, int(0).into())?),
        Method::ShiftRight => integer(shifted_left(i, -i64::from(int(0)))?),
        Method::And => integer(i & integer_of(other()?)?),
        Method::Or => integer(i | integer_of(other()?)?),
        Method::Xor => integer(i ^ integer_of(other()?)?),
        Method::AndNot => integer(i & !integer_of(other()?)?),
        Method::Not => integer(!i),
        _ => Err(format!(
            "{} is no method of java.math.BigInteger",
            name(method)
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    fn d(text: &str) -> Decimal {
        Decimal::parse(text).expect(text)
    }

    #[test]
    fn a_double_is_the_number_of_its_java_text() {
        let of = |v: f64| Decimal::of_double(v).unwrap();
        assert_eq!(of(0.1).compare(&d("0.1")), Ordering::Equal);
        assert_eq!(of(1e20).compare(&d("1E+20")), Ordering::Equal);
        assert_eq!(of(-0.0).compare(&d("0")), Ordering::Equal);
        // Double.toString(4.9E-324), not the exact value of the smallest double
        assert_eq!(
            of(f64::from_bits(1)).compare(&d("4.9E-324")),
            Ordering::Equal
        );
        // a float is widened first: 0.1f is 0.10000000149011612
        let widened = of(f64::from(0.1f32));
        assert_eq!(widened.compare(&d("0.10000000149011612")), Ordering::Equal);
        assert_eq!(
            widened.compare(&d("0.1000000014901161193847656250")),
            Ordering::Greater
        );
    }

    #[test]
    fn shifts_round_toward_negative_infinity() {
        let i = |v: i64| BigInt::from(v);
        assert_eq!(shifted_left(&i(-7), -1).unwrap(), i(-4));
        assert_eq!(shifted_left(&i(-7), -100).unwrap(), i(-1));
        assert_eq!(shifted_left(&i(7), -100).unwrap(), i(0));
        assert_eq!(shifted_left(&i(-7), 2).unwrap(), i(-28));
        assert!(shifted_left(&i(1), 1 << 40).is_err());
    }
}
