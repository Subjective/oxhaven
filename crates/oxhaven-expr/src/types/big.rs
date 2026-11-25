//! Formulas over `java.math.BigDecimal` and `java.math.BigInteger` values: the operators of
//! `QueryLanguageFunctionUtils`, casts, the methods of the two classes, their static
//! methods, fields and constructors, and the functions of the library that take them.
//!
//! Every form here was run on Java 42.9 (`tests/formulas/big_numbers.rs` holds the
//! answers). What Java computes and this module does not is refused by name.

use super::{
    convert, is_null_literal, java_name, symbol, type_error, typed, unsupported, ArithmeticOp,
    Checker, CompareDomain, CompareOp, Family, Node, Typed,
};
use crate::decimal::{integer, Decimal, Rounding};
use oxhaven_contracts::expr::{BinaryOp, Expr, FormulaError, Literal, UnaryOp};
use oxhaven_contracts::schema::ColumnType;

/// Which of the two classes a value is.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BigKind {
    Decimal,
    Integer,
}

impl BigKind {
    pub(crate) fn of(column_type: &ColumnType) -> Option<BigKind> {
        match column_type {
            ColumnType::BigDecimal => Some(BigKind::Decimal),
            ColumnType::BigInteger => Some(BigKind::Integer),
            _ => None,
        }
    }

    fn column_type(self) -> ColumnType {
        match self {
            BigKind::Decimal => ColumnType::BigDecimal,
            BigKind::Integer => ColumnType::BigInteger,
        }
    }

    fn class(self) -> &'static str {
        match self {
            BigKind::Decimal => "java.math.BigDecimal",
            BigKind::Integer => "java.math.BigInteger",
        }
    }
}

pub(crate) fn is_big(column_type: &ColumnType) -> bool {
    BigKind::of(column_type).is_some()
}

/// The primitive types an operator takes beside a big number: the integers, `char` and the
/// two floating point types.
fn is_number(column_type: &ColumnType) -> bool {
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

fn is_integral(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Char
            | ColumnType::Int
            | ColumnType::Long
    )
}

/// What a [`Node::Big`] computes; its arguments come in the order each variant says.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum BigFn {
    /// `a op b` of two values, one of them a big number: null when either is null.
    Operator(ArithmeticOp),
    /// `-a`.
    Negate,
    /// `(T) a` to the primitive type of the node: null in, null out.
    Cast,
    /// `(char) a`, which casts the object to `Character` and fails for a value.
    CharCast,
    /// The text of `a` in a string concatenation: `toString()`, null for null.
    Text,
    /// A method of the receiver, the first argument.
    Method(Method),
    /// A value the formula names, the same in every row, or the failure of making it.
    Constant(Result<Vec<u8>, String>),
    /// `BigDecimal.valueOf(long)`, `BigDecimal.valueOf(double)` and `BigInteger.valueOf(long)`.
    ValueOf,
    /// `BigDecimal.valueOf(long unscaled, int scale)`.
    ValueOfScaled,
    /// `new BigDecimal(int)`, `(long)` and `(double)`, which takes the double's exact value.
    New,
    /// `new BigDecimal(BigInteger)` and `new BigDecimal(BigInteger, int scale)`.
    NewOfInteger,
    /// `new BigDecimal(String)` and `new BigInteger(String)` of a string that is no literal.
    Parse,
    /// `minObj(...)` and `maxObj(...)`, with `true` for the maximum.
    Extreme(bool),
    /// `ifelseObj(condition, a, b)`.
    IfElse,
}

/// A method of `BigDecimal` or `BigInteger`; what it does depends on the receiver's class.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Method {
    Add,
    Subtract,
    Multiply,
    /// `BigDecimal#divide(BigDecimal)` or `BigInteger#divide(BigInteger)`.
    Divide,
    /// `divide(BigDecimal, int scale, rounding)`.
    DivideToScale,
    /// `divide(BigDecimal, rounding)`, at the receiver's scale.
    DivideRounded,
    DivideToIntegralValue,
    Remainder,
    Mod,
    Pow,
    Gcd,
    Max,
    Min,
    CompareTo,
    Equals,
    Negate,
    Abs,
    Plus,
    Signum,
    Scale,
    Precision,
    UnscaledValue,
    /// `setScale(int)`, which must not round.
    SetScale,
    /// `setScale(int, rounding)`.
    SetScaleRounded,
    StripTrailingZeros,
    MovePointLeft,
    MovePointRight,
    ScaleByPowerOfTen,
    Ulp,
    ToBigInteger,
    /// `intValue()` and its relatives: the primitive type is the node's.
    Value,
    ToText,
    /// `BigInteger#toString(int radix)`.
    ToTextRadix,
    ToPlainText,
    BitLength,
    BitCount,
    TestBit,
    ShiftLeft,
    ShiftRight,
    And,
    Or,
    Xor,
    AndNot,
    Not,
}

/// The type a parameter of a method takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Parameter {
    /// The receiver's own class.
    Same,
    /// `int`, which `byte`, `short` and `char` widen to.
    Int,
    /// A `java.math.RoundingMode`, or its number as an `int` in the older overloads.
    Rounding,
    /// `Object` (`equals`).
    Any,
}

/// The signatures of the methods ported: name, parameters, method, and the result type,
/// where `None` is the receiver's class.
const METHODS: &[(&str, &[Parameter], Method, Result_)] = {
    use Method as M;
    use Parameter::{Any, Int, Rounding as R, Same};
    use Result_::*;
    &[
        ("add", &[Same], M::Add, Own),
        ("subtract", &[Same], M::Subtract, Own),
        ("multiply", &[Same], M::Multiply, Own),
        ("divide", &[Same], M::Divide, Own),
        ("divide", &[Same, Int, R], M::DivideToScale, DecimalOnly),
        ("divide", &[Same, R], M::DivideRounded, DecimalOnly),
        (
            "divideToIntegralValue",
            &[Same],
            M::DivideToIntegralValue,
            DecimalOnly,
        ),
        ("remainder", &[Same], M::Remainder, Own),
        ("mod", &[Same], M::Mod, IntegerOnly),
        ("pow", &[Int], M::Pow, Own),
        ("gcd", &[Same], M::Gcd, IntegerOnly),
        ("max", &[Same], M::Max, Own),
        ("min", &[Same], M::Min, Own),
        ("compareTo", &[Same], M::CompareTo, Of(ColumnType::Int)),
        ("equals", &[Any], M::Equals, Of(ColumnType::Boolean)),
        ("negate", &[], M::Negate, Own),
        ("abs", &[], M::Abs, Own),
        ("plus", &[], M::Plus, DecimalOnly),
        ("signum", &[], M::Signum, Of(ColumnType::Int)),
        ("scale", &[], M::Scale, DecimalOf(ColumnType::Int)),
        ("precision", &[], M::Precision, DecimalOf(ColumnType::Int)),
        (
            "unscaledValue",
            &[],
            M::UnscaledValue,
            DecimalOf(ColumnType::BigInteger),
        ),
        ("setScale", &[Int], M::SetScale, DecimalOnly),
        ("setScale", &[Int, R], M::SetScaleRounded, DecimalOnly),
        (
            "stripTrailingZeros",
            &[],
            M::StripTrailingZeros,
            DecimalOnly,
        ),
        ("movePointLeft", &[Int], M::MovePointLeft, DecimalOnly),
        ("movePointRight", &[Int], M::MovePointRight, DecimalOnly),
        (
            "scaleByPowerOfTen",
            &[Int],
            M::ScaleByPowerOfTen,
            DecimalOnly,
        ),
        ("ulp", &[], M::Ulp, DecimalOnly),
        (
            "toBigInteger",
            &[],
            M::ToBigInteger,
            DecimalOf(ColumnType::BigInteger),
        ),
        ("intValue", &[], M::Value, Of(ColumnType::Int)),
        ("longValue", &[], M::Value, Of(ColumnType::Long)),
        ("shortValue", &[], M::Value, Of(ColumnType::Short)),
        ("byteValue", &[], M::Value, Of(ColumnType::Byte)),
        ("doubleValue", &[], M::Value, Of(ColumnType::Double)),
        ("floatValue", &[], M::Value, Of(ColumnType::Float)),
        ("toString", &[], M::ToText, Of(ColumnType::String)),
        (
            "toString",
            &[Int],
            M::ToTextRadix,
            IntegerOf(ColumnType::String),
        ),
        (
            "toPlainString",
            &[],
            M::ToPlainText,
            DecimalOf(ColumnType::String),
        ),
        ("bitLength", &[], M::BitLength, IntegerOf(ColumnType::Int)),
        ("bitCount", &[], M::BitCount, IntegerOf(ColumnType::Int)),
        (
            "testBit",
            &[Int],
            M::TestBit,
            IntegerOf(ColumnType::Boolean),
        ),
        ("shiftLeft", &[Int], M::ShiftLeft, IntegerOnly),
        ("shiftRight", &[Int], M::ShiftRight, IntegerOnly),
        ("and", &[Same], M::And, IntegerOnly),
        ("or", &[Same], M::Or, IntegerOnly),
        ("xor", &[Same], M::Xor, IntegerOnly),
        ("andNot", &[Same], M::AndNot, IntegerOnly),
        ("not", &[], M::Not, IntegerOnly),
    ]
};

/// The result of a method and the classes that have it.
#[derive(Clone, PartialEq, Debug)]
enum Result_ {
    /// The receiver's class, for both classes.
    Own,
    /// A type, for both classes.
    Of(ColumnType),
    /// The receiver's class, for `BigDecimal` only.
    DecimalOnly,
    DecimalOf(ColumnType),
    IntegerOnly,
    IntegerOf(ColumnType),
}

impl Result_ {
    fn for_kind(&self, kind: BigKind) -> Option<ColumnType> {
        match (self, kind) {
            (Result_::Own, _)
            | (Result_::DecimalOnly, BigKind::Decimal)
            | (Result_::IntegerOnly, BigKind::Integer) => Some(kind.column_type()),
            (Result_::Of(t), _)
            | (Result_::DecimalOf(t), BigKind::Decimal)
            | (Result_::IntegerOf(t), BigKind::Integer) => Some(t.clone()),
            _ => None,
        }
    }
}

/// The public methods of `BigDecimal` and `BigInteger` in JDK 21 that [`METHODS`] does not
/// port, by class: calling one is refused, where a name neither has is a type error.
const OTHER_DECIMAL_METHODS: &[&str] = &[
    "byteValueExact",
    "divideAndRemainder",
    "getClass",
    "hashCode",
    "intValueExact",
    "longValueExact",
    "notify",
    "notifyAll",
    "round",
    "shortValueExact",
    "sqrt",
    "toBigIntegerExact",
    "toEngineeringString",
    "wait",
];

const OTHER_INTEGER_METHODS: &[&str] = &[
    "byteValueExact",
    "clearBit",
    "divideAndRemainder",
    "flipBit",
    "getClass",
    "getLowestSetBit",
    "hashCode",
    "intValueExact",
    "isProbablePrime",
    "longValueExact",
    "modInverse",
    "modPow",
    "nextProbablePrime",
    "notify",
    "notifyAll",
    "parallelMultiply",
    "setBit",
    "shortValueExact",
    "sqrt",
    "sqrtAndRemainder",
    "toByteArray",
    "wait",
];

fn big(column_type: ColumnType, function: BigFn, arguments: Vec<Typed>) -> Typed {
    typed(
        column_type,
        Node::Big {
            function,
            arguments,
        },
    )
}

/// The value of a `BigDecimal` or `BigInteger` constant.
fn constant(kind: BigKind, cell: Vec<u8>) -> Typed {
    big(kind.column_type(), BigFn::Constant(Ok(cell)), Vec::new())
}

/// Whether `expr` names a class of `java.math` that is no import: `MathContext` and
/// `RoundingMode` reach a formula through their constants only.
fn mentions_math_context(expr: &Expr) -> bool {
    match expr {
        Expr::Column(name) => name.starts_with("java.math.MathContext"),
        Expr::Call { function, .. } => function.contains("java.math.MathContext"),
        _ => false,
    }
}

/// The number of the `java.math.RoundingMode` constant `expr` names.
fn rounding_constant(expr: &Expr) -> Option<i32> {
    let Expr::Column(name) = expr else {
        return None;
    };
    Rounding::number_of_name(name.strip_prefix("java.math.RoundingMode.")?)
}

impl Checker<'_> {
    /// An operator with a `BigDecimal` or `BigInteger` operand, neither of them the `null`
    /// literal.
    ///
    /// Behavior: `QueryLanguageFunctionUtils#plus`, `#minus`, `#multiply`, `#divide`,
    /// `#remainder` and the comparisons, of a big number with a primitive number, a `char`
    /// or a big number: the result is a `BigInteger` where both operands are integers (a
    /// `BigInteger` and an integer type or `char`, or two `BigInteger`s) and the operator
    /// is not `/`, and a `BigDecimal` otherwise; `+` beside a `String` is concatenation.
    /// Seen on Java 42.9: no operator takes a `Boolean`, and `&`, `|`, `^` take no big
    /// number.
    pub(super) fn big_operator(
        &mut self,
        op: BinaryOp,
        family: Family,
        left: Typed,
        right: Typed,
    ) -> Result<Typed, FormulaError> {
        let refused = |left: &Typed, right: &Typed| {
            type_error(format!(
                "cannot apply {} to {} and {}",
                symbol(op),
                java_name(&left.column_type),
                java_name(&right.column_type)
            ))
        };
        if op == BinaryOp::Add
            && (left.column_type == ColumnType::String || right.column_type == ColumnType::String)
        {
            return Ok(super::concat(text_of(left), text_of(right)));
        }
        let operand = |side: &Typed| is_big(&side.column_type) || is_number(&side.column_type);
        match family {
            Family::Arithmetic(arithmetic) => {
                if !operand(&left) || !operand(&right) {
                    if super::is_list(&left) || super::is_list(&right) {
                        return Err(unsupported(format!(
                            "{} of a big number and a vector or array",
                            symbol(op)
                        )));
                    }
                    return Err(refused(&left, &right));
                }
                let integers = [&left, &right].iter().all(|side| {
                    side.column_type == ColumnType::BigInteger || is_integral(&side.column_type)
                });
                let result = if integers && arithmetic != ArithmeticOp::Divide {
                    ColumnType::BigInteger
                } else {
                    ColumnType::BigDecimal
                };
                Ok(big(result, BigFn::Operator(arithmetic), vec![left, right]))
            }
            Family::Compare(compare) => self.big_comparison(op, compare, left, right),
            Family::Bitwise(_) | Family::Logical(_) => Err(refused(&left, &right)),
        }
    }

    /// A comparison with a `BigDecimal` or `BigInteger` on at least one side.
    ///
    /// Beside a number, a `char` or another big number, the comparison is by value
    /// ([`CompareDomain::Big`]). Beside a `String` or a `Boolean` it is `eq(Object,
    /// Object)`, true only for two nulls, or `compareTo(Comparable, Comparable)`, which
    /// fails where two values that are not null meet.
    ///
    /// Seen on Java 42.9 (`BD`, `BI` big number columns, `D` double, `F` float, `L` long,
    /// `C` char): `BD == 0.1` is true for `0.10`, `BD == F` false for `0.1f`'s exact value
    /// (Java compares the text `0.10000000149011612`), `BD != D` true where `D` is NaN,
    /// `BI == L` true where both are null, `BD < NULL_INT` false everywhere, `BD == S`
    /// false everywhere, `BD < S` fails; `BD + 1 > 2` compares the sum.
    ///
    /// Behavior: `QueryLanguageFunctionUtils#eq`, `#less`, `#lessEquals`, `#greater`,
    /// `#greaterEquals` and `#compareTo` of `BigDecimal` and `BigInteger` with each other
    /// and with the primitive types, as `QueryLanguageParser#getMethod` resolves them
    fn big_comparison(
        &mut self,
        op: BinaryOp,
        compare: CompareOp,
        left: Typed,
        right: Typed,
    ) -> Result<Typed, FormulaError> {
        use super::BigOperand;
        let operand = |side: &Typed| match BigKind::of(&side.column_type) {
            Some(BigKind::Decimal) => Some(BigOperand::Decimal),
            Some(BigKind::Integer) => Some(BigOperand::Integer),
            None if is_number(&side.column_type) => Some(BigOperand::Primitive),
            None => None,
        };
        let domain = match (operand(&left), operand(&right)) {
            (Some(l), Some(r)) => CompareDomain::Big { left: l, right: r },
            _ => {
                let other = if is_big(&left.column_type) {
                    &right
                } else {
                    &left
                };
                if !matches!(other.column_type, ColumnType::String | ColumnType::Boolean) {
                    return Err(unsupported(format!(
                        "{} of a big number with a value of type {}",
                        symbol(op),
                        java_name(&other.column_type)
                    )));
                }
                let primitive = [false, false];
                match compare.is_equality() {
                    true => CompareDomain::Unrelated { primitive },
                    false => CompareDomain::UnrelatedOrder { primitive },
                }
            }
        };
        Ok(typed(
            ColumnType::Boolean,
            Node::Compare {
                op: compare,
                domain,
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }

    /// `-a` of a big number.
    ///
    /// Behavior: `QueryLanguageFunctionUtils#negate(BigDecimal)`, `#negate(BigInteger)`
    pub(super) fn big_negate(
        &mut self,
        op: UnaryOp,
        operand: Typed,
    ) -> Result<Typed, FormulaError> {
        match op {
            UnaryOp::Negate => Ok(big(
                operand.column_type.clone(),
                BigFn::Negate,
                vec![operand],
            )),
            _ => Err(type_error(format!(
                "cannot apply this operator to a value of type {}",
                java_name(&operand.column_type)
            ))),
        }
    }

    /// `(to) operand` where the operand or the target is a big number.
    ///
    /// Behavior: `QueryLanguageParser#visit(CastExpr)`: to a primitive type it is
    /// `QueryLanguageFunctionUtils#intCast(Object)` and its relatives, null in, null out,
    /// otherwise `Number#intValue` and the others; `charCast(Object)` casts to `Character`
    /// and fails for a big number. A cast to `BigDecimal` or `BigInteger` is javac's own,
    /// which takes the class itself and `null` only. Seen on Java 42.9.
    pub(super) fn big_cast(
        &mut self,
        to: &ColumnType,
        operand: Typed,
    ) -> Result<Typed, FormulaError> {
        let from = operand.column_type.clone();
        if from == *to {
            return Ok(operand);
        }
        match to {
            ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
                if is_big(&from) =>
            {
                Ok(big(to.clone(), BigFn::Cast, vec![operand]))
            }
            ColumnType::Char if is_big(&from) => {
                Ok(big(to.clone(), BigFn::CharCast, vec![operand]))
            }
            ColumnType::Object(_) => Err(unsupported(format!(
                "a value of type {} as a java.lang.Object",
                java_name(&from)
            ))),
            _ => Err(type_error(format!(
                "Incompatible types; {} cannot be converted to {}",
                java_name(&from),
                java_name(to)
            ))),
        }
    }

    /// `receiver.method(arguments)` of a big number receiver.
    ///
    /// Behavior: the methods of `java.math.BigDecimal` and `java.math.BigInteger`, which
    /// `QueryLanguageParser#visit(MethodCallExpr)` prints as they are, after
    /// `#convertParameters` has wrapped a `byte`, `short` or `char` argument of an `int`
    /// parameter in `intCast`. A null receiver throws `NullPointerException`.
    pub(super) fn big_method(
        &mut self,
        receiver: Typed,
        method: &str,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        let kind = BigKind::of(&receiver.column_type).expect("a big number receiver");
        if arguments.iter().any(mentions_math_context) {
            return Err(unsupported(format!(
                "{}.{method}() with a java.math.MathContext",
                kind.class()
            )));
        }
        let candidates: Vec<_> = METHODS
            .iter()
            .filter(|(name, parameters, _, result)| {
                *name == method
                    && parameters.len() == arguments.len()
                    && result.for_kind(kind).is_some()
            })
            .collect();
        if candidates.is_empty() {
            let others = match kind {
                BigKind::Decimal => OTHER_DECIMAL_METHODS,
                BigKind::Integer => OTHER_INTEGER_METHODS,
            };
            let known = METHODS
                .iter()
                .any(|(name, _, _, result)| *name == method && result.for_kind(kind).is_some());
            return Err(if others.contains(&method) {
                unsupported(format!("method {}.{method}()", kind.class()))
            } else if known {
                type_error(format!(
                    "{}.{method}() does not take {} arguments",
                    kind.class(),
                    arguments.len()
                ))
            } else {
                type_error(format!("{} has no method {method}", kind.class()))
            });
        }
        // Each argument: the `null` literal, a rounding constant, or a typed value.
        enum Argument {
            Null,
            Rounding(i32),
            Value(Typed),
        }
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            checked.push(if is_null_literal(argument) {
                Argument::Null
            } else if let Some(number) = rounding_constant(argument) {
                Argument::Rounding(number)
            } else {
                Argument::Value(self.check(argument)?)
            });
        }
        let int_like = |t: &ColumnType| {
            matches!(
                t,
                ColumnType::Byte | ColumnType::Short | ColumnType::Char | ColumnType::Int
            )
        };
        let fits = |parameter: &Parameter, argument: &Argument| match (parameter, argument) {
            (Parameter::Same, Argument::Null) | (Parameter::Any, _) => true,
            (Parameter::Same, Argument::Value(value)) => value.column_type == receiver.column_type,
            (Parameter::Int, Argument::Value(value)) => int_like(&value.column_type),
            (Parameter::Rounding, Argument::Rounding(_)) => true,
            (Parameter::Rounding, Argument::Value(value)) => int_like(&value.column_type),
            _ => false,
        };
        let Some((_, parameters, found, result)) = candidates
            .iter()
            .find(|(_, parameters, _, _)| parameters.iter().zip(&checked).all(|(p, a)| fits(p, a)))
        else {
            let given: Vec<String> = checked
                .iter()
                .map(|argument| match argument {
                    Argument::Null => "null".to_string(),
                    Argument::Rounding(_) => "RoundingMode".to_string(),
                    Argument::Value(value) => java_name(&value.column_type),
                })
                .collect();
            return Err(type_error(format!(
                "{}.{method}({}) does not exist",
                kind.class(),
                given.join(", ")
            )));
        };
        let result = result.for_kind(kind).expect("filtered above");
        let mut values = vec![receiver.clone()];
        for (parameter, argument) in parameters.iter().zip(checked) {
            values.push(match (parameter, argument) {
                (_, Argument::Rounding(number)) => {
                    typed(ColumnType::Int, Node::Literal(Literal::Int(number)))
                }
                (Parameter::Same, Argument::Null) => {
                    typed(receiver.column_type.clone(), Node::Null)
                }
                (Parameter::Any, Argument::Null) => typed(ColumnType::object(None), Node::Null),
                (Parameter::Int | Parameter::Rounding, Argument::Value(value)) => {
                    convert(value, &ColumnType::Int)
                }
                (_, Argument::Value(value)) => value,
                (_, Argument::Null) => unreachable!("only an object parameter takes null"),
            });
        }
        Ok(big(result, BigFn::Method(*found), values))
    }

    /// `java.math.BigDecimal.FIELD` and `java.math.BigInteger.FIELD`; `None` for a field of
    /// another class.
    ///
    /// Behavior: the constants `BigDecimal#ZERO`, `#ONE`, `#TWO`, `#TEN`, `BigInteger#ZERO`,
    /// `#ONE`, `#TWO`, `#TEN`, and the `int` constants `BigDecimal#ROUND_UP` to
    /// `#ROUND_UNNECESSARY`.
    pub(super) fn big_static_field(
        &self,
        scope: &str,
        field: &str,
    ) -> Option<Result<Typed, FormulaError>> {
        let kind = match scope {
            "java.math.BigDecimal" => BigKind::Decimal,
            "java.math.BigInteger" => BigKind::Integer,
            "java.math.RoundingMode" => {
                return Some(Err(match Rounding::number_of_name(field) {
                    Some(_) => unsupported(format!(
                        "java.math.RoundingMode.{field} other than as an argument of a method"
                    )),
                    None => type_error(format!("java.math.RoundingMode has no constant {field}")),
                }));
            }
            _ => return None,
        };
        let value = match field {
            "ZERO" => 0,
            "ONE" => 1,
            "TWO" => 2,
            "TEN" => 10,
            _ if kind == BigKind::Decimal => {
                let rounding = field
                    .strip_prefix("ROUND_")
                    .and_then(Rounding::number_of_name);
                return Some(match rounding {
                    Some(number) => Ok(typed(ColumnType::Int, Node::Literal(Literal::Int(number)))),
                    None => Err(unsupported(format!("field {scope}.{field}"))),
                });
            }
            _ => return Some(Err(unsupported(format!("field {scope}.{field}")))),
        };
        let cell = match kind {
            BigKind::Decimal => Decimal::of_long(value).cell(),
            BigKind::Integer => crate::decimal::integer_bytes(&value.into()),
        };
        Some(Ok(constant(kind, cell)))
    }

    /// Whether `name`, a qualified name, is a big number constant, so a value and no class.
    pub(super) fn is_big_static_field(&self, name: &str) -> bool {
        name.rsplit_once('.').is_some_and(|(scope, field)| {
            matches!(scope, "java.math.BigDecimal" | "java.math.BigInteger")
                && matches!(field, "ZERO" | "ONE" | "TWO" | "TEN")
        })
    }

    /// `java.math.BigDecimal.method(...)` and `java.math.BigInteger.method(...)`; `None` for
    /// another class.
    ///
    /// Behavior: `java.math.BigDecimal#valueOf(long)`, `#valueOf(double)`,
    /// `#valueOf(long, int)` and `java.math.BigInteger#valueOf(long)`, with each argument
    /// wrapped by `QueryLanguageParser#convertParameters` in the cast function of its
    /// parameter, which turns null into the parameter type's null value: seen on Java 42.9,
    /// `BigDecimal.valueOf(I)` is -9223372036854775808 where `I` is null.
    pub(super) fn big_static_call(
        &mut self,
        class: &str,
        method: &str,
        arguments: &[Expr],
    ) -> Option<Result<Typed, FormulaError>> {
        let kind = match class {
            "java.math.BigDecimal" => BigKind::Decimal,
            "java.math.BigInteger" => BigKind::Integer,
            "java.math.RoundingMode" | "java.math.MathContext" => {
                return Some(Err(unsupported(format!(
                    "static method {class}.{method}()"
                ))));
            }
            _ => return None,
        };
        Some(self.big_value_of(kind, method, arguments))
    }

    fn big_value_of(
        &mut self,
        kind: BigKind,
        method: &str,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        if method != "valueOf" {
            return Err(if method == "probablePrime" {
                unsupported(format!("static method {}.{method}()", kind.class()))
            } else {
                type_error(format!("{} has no static method {method}", kind.class()))
            });
        }
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            if is_null_literal(argument) {
                return Err(type_error(format!(
                    "{}.valueOf() takes no null",
                    kind.class()
                )));
            }
            checked.push(self.check(argument)?);
        }
        let types: Vec<&ColumnType> = checked.iter().map(|c| &c.column_type).collect();
        let decimal = kind == BigKind::Decimal;
        match types.as_slice() {
            [t] if is_integral(t) => {
                let value = convert(checked.remove(0), &ColumnType::Long);
                Ok(big(kind.column_type(), BigFn::ValueOf, vec![value]))
            }
            [ColumnType::Float | ColumnType::Double] if decimal => {
                let value = convert(checked.remove(0), &ColumnType::Double);
                Ok(big(kind.column_type(), BigFn::ValueOf, vec![value]))
            }
            [t, s] if decimal && is_integral(t) && is_integral(s) && **s != ColumnType::Long => {
                let scale = convert(checked.remove(1), &ColumnType::Int);
                let unscaled = convert(checked.remove(0), &ColumnType::Long);
                Ok(big(
                    kind.column_type(),
                    BigFn::ValueOfScaled,
                    vec![unscaled, scale],
                ))
            }
            _ => Err(type_error(format!(
                "{}.valueOf({}) does not exist",
                kind.class(),
                types
                    .iter()
                    .map(|t| java_name(t))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// `new java.math.BigDecimal(...)` and `new java.math.BigInteger(...)`; `None` for
    /// another class.
    ///
    /// Behavior: the constructors `BigDecimal(String)`, `(int)`, `(long)`, `(double)`,
    /// `(BigInteger)`, `(BigInteger, int)` and `BigInteger(String)`, with the arguments
    /// converted as `QueryLanguageParser#convertParameters` converts them. A string is
    /// taken as a literal only: Java reads digits that are not ASCII too
    /// (`Character#digit`), which this crate does not.
    pub(super) fn big_creation(
        &mut self,
        created: &str,
        arguments: &[Expr],
    ) -> Option<Result<Typed, FormulaError>> {
        let kind = match created {
            "java.math.BigDecimal" => BigKind::Decimal,
            "java.math.BigInteger" => BigKind::Integer,
            "java.math.MathContext" => {
                return Some(Err(unsupported("constructor new java.math.MathContext()")));
            }
            _ => return None,
        };
        Some(self.big_constructor(kind, arguments))
    }

    fn big_constructor(
        &mut self,
        kind: BigKind,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        let refused = || {
            unsupported(format!(
                "constructor new {}() with these arguments",
                kind.class()
            ))
        };
        if let [Expr::Literal(Literal::String(text))] = arguments {
            if !text.is_ascii() {
                return Err(refused());
            }
            let made = match kind {
                BigKind::Decimal => Decimal::parse(text).map(|d| d.cell()),
                BigKind::Integer => integer::parse(text).map(|i| crate::decimal::integer_bytes(&i)),
            };
            let made = made.ok_or_else(|| {
                format!("java.lang.NumberFormatException: For input string: \"{text}\"")
            });
            return Ok(big(kind.column_type(), BigFn::Constant(made), Vec::new()));
        }
        if arguments.iter().any(is_null_literal) {
            return Err(refused());
        }
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            checked.push(self.check(argument)?);
        }
        let types: Vec<ColumnType> = checked.iter().map(|c| c.column_type.clone()).collect();
        if let [ColumnType::String] = types.as_slice() {
            return Ok(big(kind.column_type(), BigFn::Parse, checked));
        }
        if kind == BigKind::Integer {
            return Err(refused());
        }
        let int_like = |t: &ColumnType| {
            matches!(
                t,
                ColumnType::Byte | ColumnType::Short | ColumnType::Char | ColumnType::Int
            )
        };
        match types.as_slice() {
            [t] if int_like(t) => Ok(big(
                ColumnType::BigDecimal,
                BigFn::New,
                vec![convert(checked.remove(0), &ColumnType::Int)],
            )),
            [ColumnType::Long] => Ok(big(ColumnType::BigDecimal, BigFn::New, checked)),
            [ColumnType::Float | ColumnType::Double] => Ok(big(
                ColumnType::BigDecimal,
                BigFn::New,
                vec![convert(checked.remove(0), &ColumnType::Double)],
            )),
            [ColumnType::BigInteger] => {
                Ok(big(ColumnType::BigDecimal, BigFn::NewOfInteger, checked))
            }
            [ColumnType::BigInteger, s] if int_like(s) => {
                let scale = convert(checked.remove(1), &ColumnType::Int);
                Ok(big(
                    ColumnType::BigDecimal,
                    BigFn::NewOfInteger,
                    vec![checked.remove(0), scale],
                ))
            }
            _ => Err(refused()),
        }
    }

    /// A call of a library function with a big number argument.
    ///
    /// Behavior: the functions of the default imports that take an object: `minObj` and
    /// `maxObj` (`io.deephaven.function.Numeric`, nulls skipped, the first of equal
    /// values), `ifelseObj` and the generic `replaceIfNull` (`io.deephaven.function.Basic`),
    /// and the operator functions of `QueryLanguageFunctionUtils` called by name. Seen on
    /// Java 42.9: `abs`, `sign`, `sum`, `min`, `max`, `round`, `floor`, `isNaN`, `in`,
    /// `compare`, `nullToValue` and `array` of a `BigDecimal` do not compile.
    pub(super) fn big_call(
        &mut self,
        function: &str,
        arguments: &[Expr],
        checked: Vec<Option<Typed>>,
    ) -> Result<Typed, FormulaError> {
        let binary = match function {
            "plus" => Some(BinaryOp::Add),
            "minus" => Some(BinaryOp::Subtract),
            "multiply" => Some(BinaryOp::Multiply),
            "divide" => Some(BinaryOp::Divide),
            "remainder" => Some(BinaryOp::Remainder),
            "eq" => Some(BinaryOp::Equal),
            "less" => Some(BinaryOp::Less),
            "lessEquals" => Some(BinaryOp::LessOrEqual),
            "greater" => Some(BinaryOp::Greater),
            "greaterEquals" => Some(BinaryOp::GreaterOrEqual),
            _ => None,
        };
        if let (Some(op), [left, right]) = (binary, arguments) {
            return self.binary(op, left, right);
        }
        let cast = match function {
            "intCast" => Some(ColumnType::Int),
            "longCast" => Some(ColumnType::Long),
            "doubleCast" => Some(ColumnType::Double),
            "floatCast" => Some(ColumnType::Float),
            "shortCast" => Some(ColumnType::Short),
            "byteCast" => Some(ColumnType::Byte),
            "charCast" => Some(ColumnType::Char),
            _ => None,
        };
        if let (Some(to), [operand]) = (cast, arguments) {
            return self.cast(&to, operand);
        }
        if let ("negate", [operand]) = (function, arguments) {
            return self.unary(UnaryOp::Negate, operand);
        }
        let values: Option<Vec<Typed>> = checked.iter().cloned().collect();
        let same_big = |values: &[Typed]| {
            values
                .first()
                .is_some_and(|first| is_big(&first.column_type))
                && values
                    .iter()
                    .all(|v| v.column_type == values[0].column_type)
        };
        match (function, values) {
            ("minObj" | "maxObj", Some(values)) if same_big(&values) => Ok(big(
                values[0].column_type.clone(),
                BigFn::Extreme(function == "maxObj"),
                values,
            )),
            ("ifelseObj", Some(values))
                if values.len() == 3
                    && values[0].column_type == ColumnType::Boolean
                    && same_big(&values[1..]) =>
            {
                Ok(big(values[1].column_type.clone(), BigFn::IfElse, values))
            }
            ("replaceIfNull", Some(values)) if values.len() == 2 && same_big(&values) => {
                match crate::functions::resolve(
                    function,
                    &[
                        Some(values[0].column_type.clone()),
                        Some(values[1].column_type.clone()),
                    ],
                )? {
                    crate::functions::Resolved::Call(call) => Ok(super::library_call(call, values)),
                    _ => Err(unsupported("replaceIfNull of big numbers")),
                }
            }
            _ if function.ends_with("Obj")
                || matches!(function, "concat" | "repeat" | "sort" | "sortDescending") =>
            {
                Err(unsupported(format!(
                    "function {function}() with a BigDecimal or BigInteger argument"
                )))
            }
            // Seen on Java 42.9 not to compile with a `BigDecimal` argument.
            (
                "abs" | "sign" | "sum" | "min" | "max" | "round" | "floor" | "isNaN" | "isInf"
                | "isFinite" | "in" | "compare" | "nullToValue" | "array",
                _,
            ) => Err(type_error(format!(
                "cannot find {function}() for a BigDecimal or BigInteger argument"
            ))),
            _ => Err(unsupported(format!(
                "function {function}() with a BigDecimal or BigInteger argument"
            ))),
        }
    }
}

/// The text of a string concatenation's operand: a big number as `toString()` gives it, and
/// anything else as it is.
fn text_of(side: Typed) -> Typed {
    if is_big(&side.column_type) {
        big(ColumnType::String, BigFn::Text, vec![side])
    } else {
        side
    }
}
