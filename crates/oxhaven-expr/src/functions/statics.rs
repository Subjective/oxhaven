//! Static methods of the classes of `java.lang` that a formula calls through the class:
//! `Math.sin(X)`, `Long.toString(K)`, `String.valueOf(I)`.
//!
//! Java prints such a call into the code it compiles, so the method runs on the values as
//! the generated code holds them: a null `int` is the number `NULL_INT`, and nothing looks
//! for it. `Math.abs(I)` of a null is `abs(-2147483648)`, which is -2147483648 and so null
//! again; `Math.max(I, 2)` of a null is 2. An argument of another primitive type is first
//! converted by the cast function of the parameter's type (`intCast`, `doubleCast`), which
//! keeps null, so `Math.sin(I)` of a null `int` is `sin(NULL_DOUBLE)`. A `Boolean` column
//! is a boxed value there, and passing a null one to a `boolean` parameter throws.
//!
//! These are the raw methods, not the functions of the same names that a formula imports
//! (`abs`, `max`, `sin`), which answer null for null.
//!
//! A method that is not listed in [`METHODS`] is refused as unsupported, by name.

use super::Math;
use crate::eval::numeric::with_null_values;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    StringArray,
};
use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
use oxhaven_contracts::expr::FormulaError;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use std::cmp::Ordering;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// The type of a parameter or of a result.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum P {
    /// A primitive `boolean`: takes a `Boolean` value, and fails on a null one.
    Boolean,
    Char,
    Int,
    Long,
    Float,
    Double,
    Str,
    /// Any reference: a `String`, an `Instant` or a `Boolean`.
    Object,
}

impl P {
    fn java_name(self) -> &'static str {
        match self {
            P::Boolean => "boolean",
            P::Char => "char",
            P::Int => "int",
            P::Long => "long",
            P::Float => "float",
            P::Double => "double",
            P::Str => "java.lang.String",
            P::Object => "java.lang.Object",
        }
    }

    /// The column type of a value of this type; `None` for [`P::Object`], which has many.
    fn column_type(self) -> Option<ColumnType> {
        Some(match self {
            P::Boolean => ColumnType::Boolean,
            P::Char => ColumnType::Char,
            P::Int => ColumnType::Int,
            P::Long => ColumnType::Long,
            P::Float => ColumnType::Float,
            P::Double => ColumnType::Double,
            P::Str => ColumnType::String,
            P::Object => return None,
        })
    }

    /// Whether Java passes a value of `argument` for a parameter of this type.
    ///
    /// Behavior: `QueryLanguageParser#dhqlIsAssignableFrom`: a primitive parameter takes a
    /// primitive argument when binary numeric promotion of the two gives the parameter's
    /// type, so an `int` parameter takes a `byte`, `short` or `char`, and a `char`
    /// parameter only a `char`.
    fn takes(self, argument: &ColumnType) -> bool {
        use ColumnType as C;
        match self {
            P::Boolean => *argument == C::Boolean,
            P::Char => *argument == C::Char,
            P::Int => matches!(argument, C::Byte | C::Short | C::Char | C::Int),
            P::Long => matches!(argument, C::Byte | C::Short | C::Char | C::Int | C::Long),
            P::Float => matches!(
                argument,
                C::Byte | C::Short | C::Char | C::Int | C::Long | C::Float
            ),
            P::Double => matches!(
                argument,
                C::Byte | C::Short | C::Char | C::Int | C::Long | C::Float | C::Double
            ),
            P::Str => *argument == C::String,
            P::Object => matches!(argument, C::String | C::Instant),
        }
    }
}

/// What a static method computes, for whatever types its overload has.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum StaticFn {
    /// A function of `double` to `double`.
    Unary(Math),
    Abs,
    Max,
    Min,
    Sum,
    /// `Math.signum`, in the argument's floating point type.
    Signum,
    /// `Integer.signum` and `Long.signum`, an `int`.
    IntSignum,
    Round,
    Pow,
    Atan2,
    FloorMod,
    FloorDiv,
    /// `Math.random()`.
    Random,
    /// The string Java converts the argument to.
    Text,
    TextInRadix,
    Hex,
    Octal,
    Binary,
    Parse,
    /// `valueOf` of a boxed class given its own primitive, which the formula unboxes again.
    Identity,
    /// `new String(String)`, which reads its argument and so throws for null.
    NonNull,
    IsNaN,
    IsInfinite,
    IsFinite,
    Compare,
    LeadingZeros,
    TrailingZeros,
    BitCount,
}

/// One overload of a static method.
#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Method {
    /// The class, without `java.lang.`.
    pub(crate) class: &'static str,
    pub(crate) name: &'static str,
    pub(crate) parameters: &'static [P],
    pub(crate) result: P,
    pub(crate) function: StaticFn,
}

impl Method {
    /// The result's column type.
    pub(crate) fn result_type(&self) -> ColumnType {
        self.result
            .column_type()
            .expect("no method here returns an Object")
    }

    /// Why a table operation of a client may not call the method, when it may not.
    ///
    /// Behavior: `io.deephaven.server.table.validation.ParsingColumnExpressionValidator`
    /// with the lists of `dh-defaults.prop` (`ColumnExpressionValidator.allowedMethods.*`):
    /// every method of the boxed classes and of `String` is allowed, and none of `Math` or
    /// `StrictMath`.
    pub(crate) fn refusal(&self) -> Option<String> {
        if !matches!(self.class, "Math" | "StrictMath") {
            return None;
        }
        let parameters: Vec<&str> = self.parameters.iter().map(|p| p.java_name()).collect();
        Some(format!(
            "User expressions are not permitted to use static method {}({}) on class java.lang.{}",
            self.name,
            parameters.join(", "),
            self.class
        ))
    }
}

const fn method(
    class: &'static str,
    name: &'static str,
    parameters: &'static [P],
    result: P,
    function: StaticFn,
) -> Method {
    Method {
        class,
        name,
        parameters,
        result,
        function,
    }
}

/// The methods of `Math`, which `StrictMath` has too.
macro_rules! math {
    ($class:literal) => {
        [
            method($class, "abs", &[P::Int], P::Int, StaticFn::Abs),
            method($class, "abs", &[P::Long], P::Long, StaticFn::Abs),
            method($class, "abs", &[P::Float], P::Float, StaticFn::Abs),
            method($class, "abs", &[P::Double], P::Double, StaticFn::Abs),
            method($class, "max", &[P::Int, P::Int], P::Int, StaticFn::Max),
            method($class, "max", &[P::Long, P::Long], P::Long, StaticFn::Max),
            method(
                $class,
                "max",
                &[P::Float, P::Float],
                P::Float,
                StaticFn::Max,
            ),
            method(
                $class,
                "max",
                &[P::Double, P::Double],
                P::Double,
                StaticFn::Max,
            ),
            method($class, "min", &[P::Int, P::Int], P::Int, StaticFn::Min),
            method($class, "min", &[P::Long, P::Long], P::Long, StaticFn::Min),
            method(
                $class,
                "min",
                &[P::Float, P::Float],
                P::Float,
                StaticFn::Min,
            ),
            method(
                $class,
                "min",
                &[P::Double, P::Double],
                P::Double,
                StaticFn::Min,
            ),
            method($class, "signum", &[P::Float], P::Float, StaticFn::Signum),
            method($class, "signum", &[P::Double], P::Double, StaticFn::Signum),
            method($class, "round", &[P::Float], P::Int, StaticFn::Round),
            method($class, "round", &[P::Double], P::Long, StaticFn::Round),
            method(
                $class,
                "pow",
                &[P::Double, P::Double],
                P::Double,
                StaticFn::Pow,
            ),
            method(
                $class,
                "atan2",
                &[P::Double, P::Double],
                P::Double,
                StaticFn::Atan2,
            ),
            method(
                $class,
                "floorMod",
                &[P::Int, P::Int],
                P::Int,
                StaticFn::FloorMod,
            ),
            method(
                $class,
                "floorMod",
                &[P::Long, P::Int],
                P::Int,
                StaticFn::FloorMod,
            ),
            method(
                $class,
                "floorMod",
                &[P::Long, P::Long],
                P::Long,
                StaticFn::FloorMod,
            ),
            method(
                $class,
                "floorDiv",
                &[P::Int, P::Int],
                P::Int,
                StaticFn::FloorDiv,
            ),
            method(
                $class,
                "floorDiv",
                &[P::Long, P::Int],
                P::Long,
                StaticFn::FloorDiv,
            ),
            method(
                $class,
                "floorDiv",
                &[P::Long, P::Long],
                P::Long,
                StaticFn::FloorDiv,
            ),
            method($class, "random", &[], P::Double, StaticFn::Random),
            unary($class, "acos", Math::Acos),
            unary($class, "asin", Math::Asin),
            unary($class, "atan", Math::Atan),
            unary($class, "ceil", Math::Ceil),
            unary($class, "cos", Math::Cos),
            unary($class, "cosh", Math::Cosh),
            unary($class, "exp", Math::Exp),
            unary($class, "expm1", Math::Expm1),
            unary($class, "floor", Math::Floor),
            unary($class, "log", Math::Log),
            unary($class, "log10", Math::Log10),
            unary($class, "log1p", Math::Log1p),
            unary($class, "rint", Math::Rint),
            unary($class, "sin", Math::Sin),
            unary($class, "sinh", Math::Sinh),
            unary($class, "sqrt", Math::Sqrt),
            unary($class, "tan", Math::Tan),
            unary($class, "tanh", Math::Tanh),
            unary($class, "toDegrees", Math::ToDegrees),
            unary($class, "toRadians", Math::ToRadians),
        ]
    };
}

const fn unary(class: &'static str, name: &'static str, function: Math) -> Method {
    method(
        class,
        name,
        &[P::Double],
        P::Double,
        StaticFn::Unary(function),
    )
}

static MATH: &[Method] = &math!("Math");
static STRICT_MATH: &[Method] = &math!("StrictMath");

/// The methods of the boxed classes and of `String`. Where a method has several overloads
/// they stand from the narrowest parameter types to the widest, and a call reaches the
/// first that takes its arguments, as it does in Java.
static METHODS: &[Method] = &[
    method("Integer", "toString", &[P::Int], P::Str, StaticFn::Text),
    method(
        "Integer",
        "toString",
        &[P::Int, P::Int],
        P::Str,
        StaticFn::TextInRadix,
    ),
    method("Integer", "toHexString", &[P::Int], P::Str, StaticFn::Hex),
    method(
        "Integer",
        "toOctalString",
        &[P::Int],
        P::Str,
        StaticFn::Octal,
    ),
    method(
        "Integer",
        "toBinaryString",
        &[P::Int],
        P::Str,
        StaticFn::Binary,
    ),
    method("Integer", "parseInt", &[P::Str], P::Int, StaticFn::Parse),
    method("Integer", "valueOf", &[P::Int], P::Int, StaticFn::Identity),
    method("Integer", "valueOf", &[P::Str], P::Int, StaticFn::Parse),
    method(
        "Integer",
        "compare",
        &[P::Int, P::Int],
        P::Int,
        StaticFn::Compare,
    ),
    method("Integer", "max", &[P::Int, P::Int], P::Int, StaticFn::Max),
    method("Integer", "min", &[P::Int, P::Int], P::Int, StaticFn::Min),
    method("Integer", "sum", &[P::Int, P::Int], P::Int, StaticFn::Sum),
    method("Integer", "signum", &[P::Int], P::Int, StaticFn::IntSignum),
    method(
        "Integer",
        "numberOfLeadingZeros",
        &[P::Int],
        P::Int,
        StaticFn::LeadingZeros,
    ),
    method(
        "Integer",
        "numberOfTrailingZeros",
        &[P::Int],
        P::Int,
        StaticFn::TrailingZeros,
    ),
    method("Integer", "bitCount", &[P::Int], P::Int, StaticFn::BitCount),
    method("Long", "toString", &[P::Long], P::Str, StaticFn::Text),
    method(
        "Long",
        "toString",
        &[P::Long, P::Int],
        P::Str,
        StaticFn::TextInRadix,
    ),
    method("Long", "toHexString", &[P::Long], P::Str, StaticFn::Hex),
    method("Long", "toOctalString", &[P::Long], P::Str, StaticFn::Octal),
    method(
        "Long",
        "toBinaryString",
        &[P::Long],
        P::Str,
        StaticFn::Binary,
    ),
    method("Long", "parseLong", &[P::Str], P::Long, StaticFn::Parse),
    method("Long", "valueOf", &[P::Long], P::Long, StaticFn::Identity),
    method("Long", "valueOf", &[P::Str], P::Long, StaticFn::Parse),
    method(
        "Long",
        "compare",
        &[P::Long, P::Long],
        P::Int,
        StaticFn::Compare,
    ),
    method("Long", "max", &[P::Long, P::Long], P::Long, StaticFn::Max),
    method("Long", "min", &[P::Long, P::Long], P::Long, StaticFn::Min),
    method("Long", "sum", &[P::Long, P::Long], P::Long, StaticFn::Sum),
    method("Long", "signum", &[P::Long], P::Int, StaticFn::IntSignum),
    method(
        "Long",
        "numberOfLeadingZeros",
        &[P::Long],
        P::Int,
        StaticFn::LeadingZeros,
    ),
    method(
        "Long",
        "numberOfTrailingZeros",
        &[P::Long],
        P::Int,
        StaticFn::TrailingZeros,
    ),
    method("Long", "bitCount", &[P::Long], P::Int, StaticFn::BitCount),
    method("Float", "toString", &[P::Float], P::Str, StaticFn::Text),
    method("Float", "parseFloat", &[P::Str], P::Float, StaticFn::Parse),
    method(
        "Float",
        "valueOf",
        &[P::Float],
        P::Float,
        StaticFn::Identity,
    ),
    method("Float", "valueOf", &[P::Str], P::Float, StaticFn::Parse),
    method("Float", "isNaN", &[P::Float], P::Boolean, StaticFn::IsNaN),
    method(
        "Float",
        "isInfinite",
        &[P::Float],
        P::Boolean,
        StaticFn::IsInfinite,
    ),
    method(
        "Float",
        "isFinite",
        &[P::Float],
        P::Boolean,
        StaticFn::IsFinite,
    ),
    method(
        "Float",
        "compare",
        &[P::Float, P::Float],
        P::Int,
        StaticFn::Compare,
    ),
    method(
        "Float",
        "max",
        &[P::Float, P::Float],
        P::Float,
        StaticFn::Max,
    ),
    method(
        "Float",
        "min",
        &[P::Float, P::Float],
        P::Float,
        StaticFn::Min,
    ),
    method(
        "Float",
        "sum",
        &[P::Float, P::Float],
        P::Float,
        StaticFn::Sum,
    ),
    method("Double", "toString", &[P::Double], P::Str, StaticFn::Text),
    method(
        "Double",
        "parseDouble",
        &[P::Str],
        P::Double,
        StaticFn::Parse,
    ),
    method(
        "Double",
        "valueOf",
        &[P::Double],
        P::Double,
        StaticFn::Identity,
    ),
    method("Double", "valueOf", &[P::Str], P::Double, StaticFn::Parse),
    method("Double", "isNaN", &[P::Double], P::Boolean, StaticFn::IsNaN),
    method(
        "Double",
        "isInfinite",
        &[P::Double],
        P::Boolean,
        StaticFn::IsInfinite,
    ),
    method(
        "Double",
        "isFinite",
        &[P::Double],
        P::Boolean,
        StaticFn::IsFinite,
    ),
    method(
        "Double",
        "compare",
        &[P::Double, P::Double],
        P::Int,
        StaticFn::Compare,
    ),
    method(
        "Double",
        "max",
        &[P::Double, P::Double],
        P::Double,
        StaticFn::Max,
    ),
    method(
        "Double",
        "min",
        &[P::Double, P::Double],
        P::Double,
        StaticFn::Min,
    ),
    method(
        "Double",
        "sum",
        &[P::Double, P::Double],
        P::Double,
        StaticFn::Sum,
    ),
    method("Boolean", "toString", &[P::Boolean], P::Str, StaticFn::Text),
    method(
        "Boolean",
        "parseBoolean",
        &[P::Str],
        P::Boolean,
        StaticFn::Parse,
    ),
    method("Character", "toString", &[P::Char], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Boolean], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Char], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Int], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Long], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Float], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Double], P::Str, StaticFn::Text),
    method("String", "valueOf", &[P::Object], P::Str, StaticFn::Text),
    // The constructors, which a formula calls with `new`. `new Float(double)` is not
    // here: it narrows the value Java reserves for a null `double` to `-Infinity`.
    method(
        "Integer",
        CONSTRUCTOR,
        &[P::Int],
        P::Int,
        StaticFn::Identity,
    ),
    method("Integer", CONSTRUCTOR, &[P::Str], P::Int, StaticFn::Parse),
    method("Long", CONSTRUCTOR, &[P::Long], P::Long, StaticFn::Identity),
    method("Long", CONSTRUCTOR, &[P::Str], P::Long, StaticFn::Parse),
    method(
        "Float",
        CONSTRUCTOR,
        &[P::Float],
        P::Float,
        StaticFn::Identity,
    ),
    method("Float", CONSTRUCTOR, &[P::Str], P::Float, StaticFn::Parse),
    method(
        "Double",
        CONSTRUCTOR,
        &[P::Double],
        P::Double,
        StaticFn::Identity,
    ),
    method("Double", CONSTRUCTOR, &[P::Str], P::Double, StaticFn::Parse),
    method(
        "Boolean",
        CONSTRUCTOR,
        &[P::Boolean],
        P::Boolean,
        StaticFn::Identity,
    ),
    method(
        "Boolean",
        CONSTRUCTOR,
        &[P::Str],
        P::Boolean,
        StaticFn::Parse,
    ),
    method(
        "Character",
        CONSTRUCTOR,
        &[P::Char],
        P::Char,
        StaticFn::Identity,
    ),
    method("String", CONSTRUCTOR, &[P::Str], P::Str, StaticFn::NonNull),
];

/// The name the constructors have in [`METHODS`], which no formula can write.
pub(crate) const CONSTRUCTOR: &str = "<init>";

/// The class a formula names as `name`, as [`Method::class`] has it: a class of `java.lang`
/// by its simple name, since the package is imported, or by its full name.
fn class_named(name: &str) -> Option<&'static str> {
    const CLASSES: [&str; 10] = [
        "Boolean",
        "Byte",
        "Character",
        "Double",
        "Float",
        "Integer",
        "Long",
        "Math",
        "Short",
        "StrictMath",
    ];
    let simple = name.strip_prefix("java.lang.").unwrap_or(name);
    if simple == "String" {
        return Some("String");
    }
    CLASSES.iter().copied().find(|class| *class == simple)
}

/// Whether `name` is a class whose static methods [`resolve`] knows.
pub(crate) fn is_class(name: &str) -> bool {
    class_named(name).is_some()
}

fn methods_of(class: &str) -> &'static [Method] {
    match class {
        "Math" => MATH,
        "StrictMath" => STRICT_MATH,
        _ => METHODS,
    }
}

/// The overload of `class.name` that a call with these argument types reaches. An argument
/// is its type, or `None` for the `null` literal.
///
/// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` with a class as the scope, and
/// `#getMethod`, which takes the most specific of the methods whose parameters take the
/// arguments ([`P::takes`]).
pub(crate) fn resolve(
    class: &str,
    name: &str,
    arguments: &[Option<ColumnType>],
) -> Result<&'static Method, FormulaError> {
    let unsupported = |what: String| {
        FormulaError::Unsupported(if name == CONSTRUCTOR {
            format!("constructor new {class}(){what}")
        } else {
            format!("static method call {class}.{name}(){what}")
        })
    };
    let Some(class) = class_named(class) else {
        return Err(unsupported(String::new()));
    };
    let mut named = methods_of(class)
        .iter()
        .filter(|method| method.class == class && method.name == name)
        .peekable();
    if named.peek().is_none() {
        return Err(unsupported(String::new()));
    }
    let Some(types) = arguments.iter().cloned().collect::<Option<Vec<_>>>() else {
        return Err(unsupported(" with a null literal as an argument".into()));
    };
    named
        .find(|method| {
            method.parameters.len() == types.len()
                && method
                    .parameters
                    .iter()
                    .zip(&types)
                    .all(|(parameter, argument)| parameter.takes(argument))
        })
        .ok_or_else(|| unsupported(" with these arguments".into()))
}

/// The type each argument of a call to `method` is converted to, given the arguments'
/// types: the parameter's, and for an `Object` parameter the argument's own.
pub(crate) fn parameter_types(method: &Method, arguments: &[ColumnType]) -> Vec<ColumnType> {
    method
        .parameters
        .iter()
        .zip(arguments)
        .map(|(parameter, argument)| parameter.column_type().unwrap_or_else(|| argument.clone()))
        .collect()
}

/// `java.lang.Math#max(double, double)` and its `float` relative: NaN if either is, and
/// `0.0` above `-0.0`.
fn java_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == 0.0 && b == 0.0 {
        if a.is_sign_negative() {
            b
        } else {
            a
        }
    } else if a >= b {
        a
    } else {
        b
    }
}

fn java_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == 0.0 && b == 0.0 {
        if b.is_sign_negative() {
            b
        } else {
            a
        }
    } else if a <= b {
        a
    } else {
        b
    }
}

/// `java.lang.Math#signum(double)`: the argument itself when it is a zero or NaN.
fn java_signum(value: f64) -> f64 {
    if value == 0.0 || value.is_nan() {
        value
    } else {
        1.0f64.copysign(value)
    }
}

/// `java.lang.Double#compare`: numeric order, with `-0.0` below `0.0` and NaN above all
/// and equal to itself.
fn java_compare(a: f64, b: f64) -> i32 {
    let ordering = match a.partial_cmp(&b) {
        Some(Ordering::Equal) => b.is_sign_negative().cmp(&a.is_sign_negative()),
        Some(ordering) => ordering,
        None => a.is_nan().cmp(&b.is_nan()),
    };
    ordering as i32
}

fn divide_by_zero() -> TableFailure {
    failure("java.lang.ArithmeticException: / by zero")
}

/// `java.lang.Math#floorMod(long, long)`.
fn floor_mod(a: i64, b: i64) -> Result<i64, TableFailure> {
    if b == 0 {
        return Err(divide_by_zero());
    }
    let remainder = a.wrapping_rem(b);
    Ok(if remainder != 0 && (remainder ^ b) < 0 {
        remainder + b
    } else {
        remainder
    })
}

/// `java.lang.Math#floorDiv(long, long)`; `wrap` is `Integer.MIN_VALUE` or `Long.MIN_VALUE`,
/// which divided by -1 is itself.
fn floor_div(a: i64, b: i64, wrap: i64) -> Result<i64, TableFailure> {
    if b == 0 {
        return Err(divide_by_zero());
    }
    if a == wrap && b == -1 {
        return Ok(wrap);
    }
    let quotient = a.wrapping_div(b);
    Ok(if (a ^ b) < 0 && quotient.wrapping_mul(b) != a {
        quotient - 1
    } else {
        quotient
    })
}

/// `java.lang.Long#toString(long, int)`: a radix outside 2 to 36 is 10.
fn in_radix(value: i64, radix: i32) -> String {
    let radix = if (2..=36).contains(&radix) {
        radix as u64
    } else {
        10
    };
    let mut magnitude = value.unsigned_abs();
    let mut text = Vec::new();
    loop {
        let unit = (magnitude % radix) as u32;
        text.push(char::from_digit(unit, radix as u32).expect("below the radix"));
        magnitude /= radix;
        if magnitude == 0 {
            break;
        }
    }
    if value < 0 {
        text.push('-');
    }
    text.iter().rev().collect()
}

fn number_format(text: &str) -> TableFailure {
    failure(format!(
        "java.lang.NumberFormatException: For input string: \"{text}\""
    ))
}

/// The decimal text of a floating point number as Java reads it, ready for Rust to parse;
/// `Err` for text Java refuses.
///
/// Behavior: `jdk.internal.math.FloatingDecimal#readJavaFormatString`, which
/// `Double#parseDouble` and `Float#parseFloat` call: leading and trailing characters up to
/// a space are dropped, then a sign, then `NaN`, `Infinity`, or digits with an optional
/// point, exponent and one of the suffixes `f`, `F`, `d` and `D`. Java also reads a
/// hexadecimal form (`0x1.8p1`), which is refused here by name.
fn java_decimal(text: &str) -> Result<String, TableFailure> {
    let trimmed = text.trim_matches(|c: char| c <= ' ');
    if trimmed.is_empty() {
        return Err(failure("java.lang.NumberFormatException: empty String"));
    }
    let (sign, body) = match trimmed.strip_prefix(['+', '-']) {
        Some(body) => (&trimmed[..1], body),
        None => ("", trimmed),
    };
    if body == "NaN" {
        return Ok("NaN".into());
    }
    if body == "Infinity" {
        return Ok(format!("{sign}inf"));
    }
    if body.starts_with("0x") || body.starts_with("0X") {
        return Err(failure(format!(
            "a hexadecimal floating point number is not read \
             (jdk.internal.math.FloatingDecimal#parseHexString): \"{text}\""
        )));
    }
    let body = body.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(body);
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let decimal = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mantissa_ok =
        decimal(whole) && decimal(fraction) && !(whole.is_empty() && fraction.is_empty());
    let exponent_ok = exponent.is_none_or(|exponent| {
        let unsigned = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        !unsigned.is_empty() && decimal(unsigned)
    });
    if !mantissa_ok || !exponent_ok {
        return Err(number_format(text));
    }
    Ok(format!("{sign}{body}"))
}

/// `java.lang.Double#parseDouble` of text that is not null.
pub(crate) fn parse_double(text: &str) -> Result<f64, TableFailure> {
    let decimal = java_decimal(text)?;
    if decimal == "NaN" {
        return Ok(f64::NAN);
    }
    decimal.parse().map_err(|_| number_format(text))
}

/// `java.lang.Float#parseFloat` of text that is not null: the nearest `float` to the
/// decimal, not the nearest `double` narrowed.
pub(crate) fn parse_float(text: &str) -> Result<f32, TableFailure> {
    let decimal = java_decimal(text)?;
    if decimal == "NaN" {
        return Ok(f32::NAN);
    }
    decimal.parse().map_err(|_| number_format(text))
}

/// The arguments of one call, as the generated code holds them.
struct Arguments<'a> {
    method: &'a Method,
    arrays: Vec<ArrayRef>,
    rows: usize,
}

impl Arguments<'_> {
    fn ints(&self, index: usize) -> &Int32Array {
        self.arrays[index].as_primitive::<Int32Type>()
    }

    fn longs(&self, index: usize) -> &Int64Array {
        self.arrays[index].as_primitive::<Int64Type>()
    }

    fn floats(&self, index: usize) -> &Float32Array {
        self.arrays[index].as_primitive::<Float32Type>()
    }

    fn doubles(&self, index: usize) -> &Float64Array {
        self.arrays[index].as_primitive::<Float64Type>()
    }

    /// The argument at `index`, an `int` or a `long`, as a `long`.
    fn integer(&self, index: usize, row: usize) -> i64 {
        match self.method.parameters[index] {
            P::Int => self.ints(index).value(row).into(),
            _ => self.longs(index).value(row),
        }
    }

    /// The argument at `index`, a `float` or a `double`, as a `double`.
    fn floating(&self, index: usize, row: usize) -> f64 {
        match self.method.parameters[index] {
            P::Float => self.floats(index).value(row).into(),
            _ => self.doubles(index).value(row),
        }
    }

    fn text(&self, index: usize, row: usize) -> Option<&str> {
        let texts = self.arrays[index].as_string::<i32>();
        texts.is_valid(row).then(|| texts.value(row))
    }

    /// An integer result of the method's type: one row from the two arguments as `long`s.
    fn integers(
        &self,
        row: impl Fn(i64, i64) -> Result<i64, TableFailure>,
    ) -> Result<ArrayRef, TableFailure> {
        let second = |at: usize| {
            if self.method.parameters.len() > 1 {
                self.integer(1, at)
            } else {
                0
            }
        };
        let values = (0..self.rows).map(|at| row(self.integer(0, at), second(at)));
        Ok(match self.method.result {
            // Java computes in `int`: what does not fit wraps.
            P::Int => Arc::new(
                values
                    .map(|value| value.map(|value| value as i32))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Int32Array::from)?,
            ),
            _ => Arc::new(
                values
                    .collect::<Result<Vec<_>, _>>()
                    .map(Int64Array::from)?,
            ),
        })
    }

    /// A floating point result of the method's type, from the arguments as `double`s.
    fn floatings(&self, row: impl Fn(f64, f64) -> f64) -> ArrayRef {
        let second = |at: usize| {
            if self.method.parameters.len() > 1 {
                self.floating(1, at)
            } else {
                0.0
            }
        };
        let values = (0..self.rows).map(|at| row(self.floating(0, at), second(at)));
        match self.method.result {
            // A `float` argument, or a sum that rounds as the `float` sum does.
            P::Float => Arc::new(Float32Array::from_iter_values(values.map(|v| v as f32))),
            _ => Arc::new(Float64Array::from_iter_values(values)),
        }
    }

    fn is_integral(&self) -> bool {
        matches!(self.method.parameters[0], P::Int | P::Long)
    }

    fn booleans(&self, row: impl Fn(usize) -> bool) -> ArrayRef {
        Arc::new(BooleanArray::from_iter(
            (0..self.rows).map(|at| Some(row(at))),
        ))
    }

    fn ints_of(&self, row: impl Fn(usize) -> i32) -> ArrayRef {
        Arc::new(Int32Array::from_iter_values((0..self.rows).map(row)))
    }

    fn strings(&self, row: impl Fn(usize) -> String) -> ArrayRef {
        Arc::new(StringArray::from_iter_values((0..self.rows).map(row)))
    }
}

/// Evaluates `method` over `arguments`, which have its parameter types, for `rows` rows.
/// A cell of the result that holds the value Java reserves for null in its type is left
/// for the caller to make null.
pub(crate) fn evaluate(
    method: &Method,
    arguments: &[ArrayRef],
    rows: usize,
) -> Result<ArrayRef, TableFailure> {
    let mut arrays = Vec::with_capacity(arguments.len());
    for (argument, parameter) in arguments.iter().zip(method.parameters) {
        match parameter {
            P::Boolean if argument.null_count() > 0 => {
                return Err(failure(
                    "java.lang.NullPointerException: Cannot invoke \
                     \"java.lang.Boolean.booleanValue()\" because the value is null",
                ))
            }
            P::Boolean | P::Str | P::Object => arrays.push(argument.clone()),
            _ => arrays.push(with_null_values(argument)),
        }
    }
    let a = Arguments {
        method,
        arrays,
        rows,
    };
    let wrap = match method.parameters.first() {
        Some(P::Int) => i64::from(i32::MIN),
        _ => i64::MIN,
    };
    let narrow = method.parameters.first() == Some(&P::Int);
    Ok(match method.function {
        StaticFn::Unary(function) => a.floatings(|value, _| function.apply(value)),
        StaticFn::Abs if a.is_integral() => a.integers(|value, _| {
            Ok(if value == wrap {
                wrap
            } else {
                value.wrapping_abs()
            })
        })?,
        StaticFn::Abs => a.floatings(|value, _| value.abs()),
        StaticFn::Max if a.is_integral() => a.integers(|x, y| Ok(x.max(y)))?,
        StaticFn::Max => a.floatings(java_max),
        StaticFn::Min if a.is_integral() => a.integers(|x, y| Ok(x.min(y)))?,
        StaticFn::Min => a.floatings(java_min),
        StaticFn::Sum if a.is_integral() => a.integers(|x, y| Ok(x.wrapping_add(y)))?,
        // The `double` sum of two `float`s, rounded, is their `float` sum.
        StaticFn::Sum => a.floatings(|x, y| x + y),
        StaticFn::Signum => a.floatings(|value, _| java_signum(value)),
        StaticFn::IntSignum => a.ints_of(|at| a.integer(0, at).signum() as i32),
        StaticFn::Round => match method.parameters[0] {
            P::Float => a.ints_of(|at| super::eval::round_float(a.floats(0).value(at))),
            _ => Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|at| super::eval::round_double(a.doubles(0).value(at))),
            )),
        },
        StaticFn::Pow => a.floatings(super::eval::java_pow),
        StaticFn::Atan2 => a.floatings(f64::atan2),
        StaticFn::FloorMod => a.integers(floor_mod)?,
        StaticFn::FloorDiv => a.integers(|x, y| floor_div(x, y, wrap))?,
        StaticFn::Random => super::volatile::random(super::volatile::RandomFn::Uniform, &[], rows)?,
        StaticFn::Text => {
            let texts = crate::eval::java_strings(a.arrays[0].as_ref())?;
            Arc::new(StringArray::from_iter_values(texts))
        }
        StaticFn::TextInRadix => a.strings(|at| in_radix(a.integer(0, at), a.ints(1).value(at))),
        StaticFn::Hex if narrow => a.strings(|at| format!("{:x}", a.ints(0).value(at))),
        StaticFn::Hex => a.strings(|at| format!("{:x}", a.longs(0).value(at))),
        StaticFn::Octal if narrow => a.strings(|at| format!("{:o}", a.ints(0).value(at))),
        StaticFn::Octal => a.strings(|at| format!("{:o}", a.longs(0).value(at))),
        StaticFn::Binary if narrow => a.strings(|at| format!("{:b}", a.ints(0).value(at))),
        StaticFn::Binary => a.strings(|at| format!("{:b}", a.longs(0).value(at))),
        StaticFn::Parse => parse(&a)?,
        StaticFn::Identity => a.arrays[0].clone(),
        StaticFn::NonNull if a.arrays[0].null_count() > 0 => {
            return Err(failure(
                "java.lang.NullPointerException: Cannot read field \"value\" because \
                 \"original\" is null",
            ))
        }
        StaticFn::NonNull => a.arrays[0].clone(),
        StaticFn::IsNaN => a.booleans(|at| a.floating(0, at).is_nan()),
        StaticFn::IsInfinite => a.booleans(|at| a.floating(0, at).is_infinite()),
        StaticFn::IsFinite => a.booleans(|at| a.floating(0, at).is_finite()),
        StaticFn::Compare if a.is_integral() => {
            a.ints_of(|at| a.integer(0, at).cmp(&a.integer(1, at)) as i32)
        }
        StaticFn::Compare => a.ints_of(|at| java_compare(a.floating(0, at), a.floating(1, at))),
        StaticFn::LeadingZeros if narrow => {
            a.ints_of(|at| a.ints(0).value(at).leading_zeros() as i32)
        }
        StaticFn::LeadingZeros => a.ints_of(|at| a.longs(0).value(at).leading_zeros() as i32),
        StaticFn::TrailingZeros if narrow => {
            a.ints_of(|at| a.ints(0).value(at).trailing_zeros() as i32)
        }
        StaticFn::TrailingZeros => a.ints_of(|at| a.longs(0).value(at).trailing_zeros() as i32),
        StaticFn::BitCount => a.ints_of(|at| match narrow {
            true => a.ints(0).value(at).count_ones() as i32,
            false => a.longs(0).value(at).count_ones() as i32,
        }),
    })
}

/// A `parse` method, or the `valueOf(String)` that is the same.
///
/// Behavior: `Integer#parseInt`, `Long#parseLong`, `Double#parseDouble`,
/// `Float#parseFloat` and `Boolean#parseBoolean`. The integer parsers throw
/// `NumberFormatException` for null, the floating point ones `NullPointerException`, and
/// null is false to `parseBoolean`.
fn parse(a: &Arguments) -> Result<ArrayRef, TableFailure> {
    let rows = 0..a.rows;
    let integer = |at: usize, min: i64, max: i64| match a.text(0, at) {
        Some(text) => super::eval::parse_integer(text, min, max),
        None => Err(failure(
            "java.lang.NumberFormatException: Cannot parse null string: null",
        )),
    };
    let floating = |at: usize| {
        a.text(0, at).ok_or_else(|| {
            failure(
                "java.lang.NullPointerException: Cannot invoke \"String.trim()\" because \
                 \"in\" is null",
            )
        })
    };
    Ok(match a.method.result {
        P::Int => Arc::new(
            rows.map(|at| integer(at, i32::MIN.into(), i32::MAX.into()).map(|v| v as i32))
                .collect::<Result<Vec<_>, _>>()
                .map(Int32Array::from)?,
        ),
        P::Long => Arc::new(
            rows.map(|at| integer(at, i64::MIN, i64::MAX))
                .collect::<Result<Vec<_>, _>>()
                .map(Int64Array::from)?,
        ),
        P::Float => Arc::new(
            rows.map(|at| floating(at).and_then(parse_float))
                .collect::<Result<Vec<_>, _>>()
                .map(Float32Array::from)?,
        ),
        P::Double => Arc::new(
            rows.map(|at| floating(at).and_then(parse_double))
                .collect::<Result<Vec<_>, _>>()
                .map(Float64Array::from)?,
        ),
        _ => a.booleans(|at| {
            a.text(0, at)
                .is_some_and(|text| text.eq_ignore_ascii_case("true"))
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reached(class: &str, name: &str, arguments: &[ColumnType]) -> Vec<P> {
        let arguments: Vec<_> = arguments.iter().cloned().map(Some).collect();
        resolve(class, name, &arguments)
            .unwrap_or_else(|error| panic!("{class}.{name}: {error}"))
            .parameters
            .to_vec()
    }

    #[test]
    fn a_call_reaches_the_narrowest_overload_that_takes_its_arguments() {
        use ColumnType as C;
        assert_eq!(reached("Math", "abs", &[C::Short]), [P::Int]);
        assert_eq!(reached("Math", "abs", &[C::Float]), [P::Float]);
        assert_eq!(reached("Math", "max", &[C::Int, C::Long]), [P::Long; 2]);
        assert_eq!(reached("Math", "max", &[C::Double, C::Int]), [P::Double; 2]);
        assert_eq!(reached("Math", "sin", &[C::Char]), [P::Double]);
        assert_eq!(reached("java.lang.Math", "round", &[C::Float]), [P::Float]);
        assert_eq!(reached("String", "valueOf", &[C::Byte]), [P::Int]);
        assert_eq!(reached("String", "valueOf", &[C::Char]), [P::Char]);
        assert_eq!(reached("String", "valueOf", &[C::Instant]), [P::Object]);
        assert_eq!(
            reached("Math", "floorMod", &[C::Long, C::Short]),
            [P::Long, P::Int]
        );
    }

    #[test]
    fn a_call_no_overload_takes_and_an_unknown_method_are_unsupported() {
        use ColumnType as C;
        for (class, name, arguments) in [
            ("Integer", "toString", vec![Some(C::Long)]),
            ("Long", "toString", vec![Some(C::Double)]),
            ("Math", "sin", vec![Some(C::String)]),
            ("Math", "cbrt", vec![Some(C::Double)]),
            ("Math", "sin", vec![None]),
            ("Calendar", "getInstance", vec![]),
        ] {
            assert!(
                matches!(
                    resolve(class, name, &arguments),
                    Err(FormulaError::Unsupported(_))
                ),
                "{class}.{name}"
            );
        }
    }

    #[test]
    fn only_the_methods_of_math_are_refused_to_a_client() {
        let sin = resolve("StrictMath", "sin", &[Some(ColumnType::Int)]).unwrap();
        assert_eq!(
            sin.refusal().as_deref(),
            Some(
                "User expressions are not permitted to use static method sin(double) on class \
                 java.lang.StrictMath"
            )
        );
        let ints = [Some(ColumnType::Int), Some(ColumnType::Int)];
        let max = resolve("Math", "max", &ints).unwrap();
        assert_eq!(
            max.refusal().as_deref(),
            Some(
                "User expressions are not permitted to use static method max(int, int) on \
                 class java.lang.Math"
            )
        );
        let text = resolve("Long", "toString", &[Some(ColumnType::Int)]).unwrap();
        assert_eq!(text.refusal(), None);
    }

    #[test]
    fn floor_division_rounds_toward_negative_infinity() {
        assert_eq!(floor_mod(-7, 3).unwrap(), 2);
        assert_eq!(floor_mod(7, -3).unwrap(), -2);
        assert_eq!(floor_mod(i64::MIN, -1).unwrap(), 0);
        assert!(floor_mod(1, 0).is_err());
        assert_eq!(floor_div(-7, 2, i64::MIN).unwrap(), -4);
        assert_eq!(floor_div(7, 2, i64::MIN).unwrap(), 3);
        assert_eq!(floor_div(i64::MIN, -1, i64::MIN).unwrap(), i64::MIN);
        let int_min = i64::from(i32::MIN);
        assert_eq!(floor_div(int_min, -1, int_min).unwrap(), int_min);
        assert_eq!(floor_div(int_min, 2, int_min).unwrap(), -1_073_741_824);
    }

    #[test]
    fn a_number_prints_in_a_radix_as_long_to_string_prints_it() {
        assert_eq!(in_radix(255, 16), "ff");
        assert_eq!(in_radix(-255, 16), "-ff");
        assert_eq!(in_radix(i64::MIN, 36), "-1y2p0ij32e8e8");
        assert_eq!(in_radix(i64::MAX, 36), "1y2p0ij32e8e7");
        assert_eq!(in_radix(6, 2), "110");
        assert_eq!(in_radix(12, 1), "12");
        assert_eq!(in_radix(12, 37), "12");
        assert_eq!(in_radix(0, 2), "0");
    }

    #[test]
    fn floating_point_text_is_read_as_java_reads_it() {
        assert_eq!(parse_double(" 1.5 ").unwrap(), 1.5);
        assert_eq!(parse_double("1.").unwrap(), 1.0);
        assert_eq!(parse_double(".5").unwrap(), 0.5);
        assert_eq!(parse_double("-1e3").unwrap(), -1000.0);
        assert_eq!(parse_double("1E+2d").unwrap(), 100.0);
        assert_eq!(parse_double("3f").unwrap(), 3.0);
        assert_eq!(parse_double("-Infinity").unwrap(), f64::NEG_INFINITY);
        assert!(parse_double("NaN").unwrap().is_nan());
        assert_eq!(parse_float("0.1").unwrap(), 0.1f32);
        for refused in [
            "", " ", "nan", "inf", "infinity", "1e", ".", "1.5x", "e5", "1_0", "0x1p3",
        ] {
            assert!(parse_double(refused).is_err(), "{refused:?}");
        }
    }

    #[test]
    fn maximum_minimum_signum_and_compare_treat_zeros_and_nan_as_java_does() {
        assert!(java_max(0.0, -0.0).is_sign_positive());
        assert!(java_max(-0.0, 0.0).is_sign_positive());
        assert!(java_min(0.0, -0.0).is_sign_negative());
        assert!(java_max(1.0, f64::NAN).is_nan());
        assert!(java_min(f64::NAN, 1.0).is_nan());
        assert!(java_signum(-0.0).is_sign_negative());
        assert_eq!(java_signum(-3.5), -1.0);
        assert!(java_signum(f64::NAN).is_nan());
        assert_eq!(java_compare(-0.0, 0.0), -1);
        assert_eq!(java_compare(f64::NAN, f64::INFINITY), 1);
        assert_eq!(java_compare(f64::NAN, f64::NAN), 0);
        assert_eq!(java_compare(1.0, 2.0), -1);
    }
}
