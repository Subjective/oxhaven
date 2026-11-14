//! Type checking: an expression tree plus a table definition become a typed tree in which
//! every operator knows the type it computes in.
//!
//! Java resolves each operator to an overload of a static method in
//! `io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils`, one per pair of operand
//! types. The overloads follow a few rules, and those rules are stated here once.

use crate::functions::{self, Function};
use oxhaven_contracts::expr::{BinaryOp, Expr, FormulaError, Literal, UnaryOp};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};

/// The four types Java computes numbers in, in promotion order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Numeric {
    Int,
    Long,
    Float,
    Double,
}

impl Numeric {
    /// The type a numeric operand is computed in: `byte`, `short` and `char` widen to `int`.
    /// `None` for a type that is not numeric.
    ///
    /// Behavior: `QueryLanguageFunctionUtils#negate(byte)`, `#negate(short)`, `#negate(char)`
    /// (all return `int`), and the casts in `#plus(byte,byte)` and `#plus(char,char)`.
    pub(crate) fn of(column_type: &ColumnType) -> Option<Numeric> {
        match column_type {
            ColumnType::Byte | ColumnType::Char | ColumnType::Short | ColumnType::Int => {
                Some(Numeric::Int)
            }
            ColumnType::Long => Some(Numeric::Long),
            ColumnType::Float => Some(Numeric::Float),
            ColumnType::Double => Some(Numeric::Double),
            _ => None,
        }
    }

    pub(crate) fn column_type(self) -> ColumnType {
        match self {
            Numeric::Int => ColumnType::Int,
            Numeric::Long => ColumnType::Long,
            Numeric::Float => ColumnType::Float,
            Numeric::Double => ColumnType::Double,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CompareOp {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

impl CompareOp {
    pub(crate) fn is_equality(self) -> bool {
        matches!(self, CompareOp::Equal | CompareOp::NotEqual)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum LogicalOp {
    And,
    Or,
}

/// What two operands are converted to before they are compared.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum CompareDomain {
    /// Both sides are integers, chars or instants: compared exactly as 64-bit integers.
    Integral,
    /// At least one side is floating point. With `through_float` both sides are first
    /// converted to `float`, which loses precision for a large `int` or `long`, as in Java.
    Floating {
        through_float: bool,
    },
    /// An ordering comparison of a `long` with a `float` or `double`: exact, with no
    /// conversion of the `long`.
    LongWithFloating,
    Boolean,
    /// Strings, ordered by UTF-16 code unit as `String#compareTo` does.
    String,
}

/// The type an arithmetic operator computes in and returns.
///
/// Behavior: the return types and operand casts of `QueryLanguageFunctionUtils#plus`,
/// `#minus`, `#multiply`, `#remainder` (binary numeric promotion, as in
/// `QueryLanguageParser#binaryNumericPromotionType`) and `#divide`, which is always floating
/// point: `float` when the divisor is a `float` and the dividend is not a `double`, and
/// `double` otherwise, so `int / float` is `float` and `float / int` is `double`.
pub(crate) fn arithmetic_type(op: ArithmeticOp, left: Numeric, right: Numeric) -> Numeric {
    match op {
        ArithmeticOp::Divide if right == Numeric::Float && left != Numeric::Double => {
            Numeric::Float
        }
        ArithmeticOp::Divide => Numeric::Double,
        _ => left.max(right),
    }
}

/// How two numeric operands are compared.
///
/// Behavior: `QueryLanguageFunctionUtils#eq` compares with Java's `==`, so under binary
/// numeric promotion (`#eq(long,float)` compares as `float`). `#compareTo` compares
/// integers exactly, uses `Float.compare` when an `int`-sized operand meets a `float`
/// (`#compareTo(int,float)`), `Double.compare` when a `double` is involved
/// (`#compareTo(int,double)`, `#compareTo(float,double)`), and its own exact comparison for
/// a `long` against a `float` or `double` (`#compareTo(long,double)`, `#compareTo(long,float)`,
/// `#compareTo(double,long)`).
pub(crate) fn numeric_compare_domain(
    op: CompareOp,
    left: Numeric,
    right: Numeric,
) -> CompareDomain {
    let widest = left.max(right);
    if widest <= Numeric::Long {
        CompareDomain::Integral
    } else if !op.is_equality() && (left == Numeric::Long || right == Numeric::Long) {
        CompareDomain::LongWithFloating
    } else {
        CompareDomain::Floating {
            through_float: widest == Numeric::Float,
        }
    }
}

/// An expression with its type.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Typed {
    pub(crate) column_type: ColumnType,
    pub(crate) node: Node,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Node {
    /// The column at this index of [`Checked::columns`].
    Column(usize),
    /// Never `Literal::Null`.
    Literal(Literal),
    /// Computed in the node's own type.
    Negate(Box<Typed>),
    Not(Box<Typed>),
    /// Computed in the node's own type.
    Arithmetic {
        op: ArithmeticOp,
        left: Box<Typed>,
        right: Box<Typed>,
    },
    Compare {
        op: CompareOp,
        domain: CompareDomain,
        left: Box<Typed>,
        right: Box<Typed>,
    },
    Logical {
        op: LogicalOp,
        left: Box<Typed>,
        right: Box<Typed>,
    },
    IsNull(Box<Typed>),
}

/// A type-checked formula.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Checked {
    pub(crate) root: Typed,
    /// The columns the formula reads, in order of first use.
    pub(crate) columns: Vec<String>,
}

/// Type-checks `expr` against `definition`.
pub(crate) fn check(expr: &Expr, definition: &TableDefinition) -> Result<Checked, FormulaError> {
    let mut checker = Checker {
        definition,
        columns: Vec::new(),
    };
    let root = checker.check(expr)?;
    Ok(Checked {
        root,
        columns: checker.columns,
    })
}

fn type_error(message: impl Into<String>) -> FormulaError {
    FormulaError::Type(message.into())
}

fn unsupported(message: impl Into<String>) -> FormulaError {
    FormulaError::Unsupported(message.into())
}

fn symbol(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Subtract => "-",
        BinaryOp::Multiply => "*",
        BinaryOp::Divide => "/",
        BinaryOp::Remainder => "%",
        BinaryOp::Equal => "==",
        BinaryOp::NotEqual => "!=",
        BinaryOp::Less => "<",
        BinaryOp::LessOrEqual => "<=",
        BinaryOp::Greater => ">",
        BinaryOp::GreaterOrEqual => ">=",
        BinaryOp::And => "&&",
        BinaryOp::Or => "||",
        BinaryOp::BitAnd => "&",
        BinaryOp::BitOr => "|",
        BinaryOp::BitXor => "^",
    }
}

fn boxed(column_type: ColumnType, node: Node) -> Typed {
    Typed { column_type, node }
}

struct Checker<'a> {
    definition: &'a TableDefinition,
    columns: Vec<String>,
}

impl Checker<'_> {
    fn check(&mut self, expr: &Expr) -> Result<Typed, FormulaError> {
        match expr {
            Expr::Column(name) => self.column(name),
            Expr::Literal(literal) => literal_type(literal)
                .map(|column_type| boxed(column_type, Node::Literal(literal.clone()))),
            Expr::RowVariable(variable) => Err(unsupported(format!("row variable {variable:?}"))),
            Expr::Unary { op, operand } => self.unary(*op, operand),
            Expr::Binary { op, left, right } => self.binary(*op, left, right),
            Expr::Call {
                function,
                arguments,
            } => self.call(function, arguments),
            Expr::Ternary { .. } => Err(unsupported("ternary operator (?:)")),
            Expr::Cast { .. } => Err(unsupported("cast")),
            Expr::MethodCall { method, .. } => Err(unsupported(format!("method call .{method}()"))),
            Expr::ColumnVector(name) => Err(unsupported(format!("column array {name}_"))),
            Expr::ColumnArrayAccess { column, .. } => {
                Err(unsupported(format!("column array access {column}_[...]")))
            }
        }
    }

    fn column(&mut self, name: &str) -> Result<Typed, FormulaError> {
        let Some(index) = self.definition.index_of(name) else {
            let array_of = name
                .strip_suffix('_')
                .is_some_and(|base| self.definition.index_of(base).is_some());
            return Err(if array_of {
                unsupported(format!("column array {name}"))
            } else {
                type_error(format!("cannot find column {name}"))
            });
        };
        let column_type = self.definition.columns()[index].column_type.clone();
        let known = matches!(
            column_type,
            ColumnType::Boolean
                | ColumnType::Byte
                | ColumnType::Char
                | ColumnType::Short
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::String
                | ColumnType::Instant
        );
        if !known {
            return Err(unsupported(format!(
                "column {name} of type {column_type:?}"
            )));
        }
        let slot = match self.columns.iter().position(|used| used == name) {
            Some(slot) => slot,
            None => {
                self.columns.push(name.to_string());
                self.columns.len() - 1
            }
        };
        Ok(boxed(column_type, Node::Column(slot)))
    }

    /// Behavior: `QueryLanguageParser#visit(UnaryExpr)`: `-` resolves to
    /// `QueryLanguageFunctionUtils#negate`, which exists for the numeric types, and `!` to
    /// `#not(Boolean)` or Java's own `!`.
    fn unary(&mut self, op: UnaryOp, operand: &Expr) -> Result<Typed, FormulaError> {
        let operand = self.check(operand)?;
        match op {
            UnaryOp::Negate => match Numeric::of(&operand.column_type) {
                Some(numeric) => Ok(boxed(
                    numeric.column_type(),
                    Node::Negate(Box::new(operand)),
                )),
                None => Err(type_error(format!(
                    "cannot negate a value of type {:?}",
                    operand.column_type
                ))),
            },
            UnaryOp::Not => match operand.column_type {
                ColumnType::Boolean => Ok(boxed(ColumnType::Boolean, Node::Not(Box::new(operand)))),
                other => Err(type_error(format!(
                    "cannot apply ! to a value of type {other:?}"
                ))),
            },
            UnaryOp::BitNot => Err(type_error(
                "Unary operation (BITWISE_COMPLEMENT) not supported",
            )),
        }
    }

    fn binary(&mut self, op: BinaryOp, left: &Expr, right: &Expr) -> Result<Typed, FormulaError> {
        let (arithmetic, compare) = match op {
            BinaryOp::Add => (Some(ArithmeticOp::Add), None),
            BinaryOp::Subtract => (Some(ArithmeticOp::Subtract), None),
            BinaryOp::Multiply => (Some(ArithmeticOp::Multiply), None),
            BinaryOp::Divide => (Some(ArithmeticOp::Divide), None),
            BinaryOp::Remainder => (Some(ArithmeticOp::Remainder), None),
            BinaryOp::Equal => (None, Some(CompareOp::Equal)),
            BinaryOp::NotEqual => (None, Some(CompareOp::NotEqual)),
            BinaryOp::Less => (None, Some(CompareOp::Less)),
            BinaryOp::LessOrEqual => (None, Some(CompareOp::LessOrEqual)),
            BinaryOp::Greater => (None, Some(CompareOp::Greater)),
            BinaryOp::GreaterOrEqual => (None, Some(CompareOp::GreaterOrEqual)),
            BinaryOp::And => return self.logical(LogicalOp::And, left, right),
            BinaryOp::Or => return self.logical(LogicalOp::Or, left, right),
            BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor => {
                return Err(unsupported(format!("bitwise operator {}", symbol(op))))
            }
        };

        let null_literal = |expr: &Expr| matches!(expr, Expr::Literal(Literal::Null));
        if null_literal(left) || null_literal(right) {
            return self.null_comparison(op, left, right);
        }

        let left = self.check(left)?;
        let right = self.check(right)?;
        let numeric = Numeric::of(&left.column_type).zip(Numeric::of(&right.column_type));
        let either = |wanted: ColumnType| left.column_type == wanted || right.column_type == wanted;

        if let Some(op) = arithmetic {
            let Some((l, r)) = numeric else {
                return Err(if op == ArithmeticOp::Add && either(ColumnType::String) {
                    unsupported("string concatenation with +")
                } else if either(ColumnType::Instant) {
                    unsupported("arithmetic on Instant values")
                } else {
                    type_error(format!(
                        "cannot apply {} to {:?} and {:?}",
                        symbol(self::binary_op(op)),
                        left.column_type,
                        right.column_type
                    ))
                });
            };
            let result = arithmetic_type(op, l, r);
            return Ok(boxed(
                result.column_type(),
                Node::Arithmetic {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                },
            ));
        }

        let Some(op) = compare else {
            unreachable!("every binary operator is arithmetic, a comparison or logical");
        };
        let domain = match (numeric, &left.column_type, &right.column_type) {
            (Some((l, r)), _, _) => numeric_compare_domain(op, l, r),
            // `eq(Object,Object)` is `equals`; `compareTo(Comparable,Comparable)` is
            // `String#compareTo` and `Instant#compareTo`.
            (None, ColumnType::String, ColumnType::String) => CompareDomain::String,
            (None, ColumnType::Instant, ColumnType::Instant) => CompareDomain::Integral,
            (None, ColumnType::Boolean, ColumnType::Boolean) if op.is_equality() => {
                CompareDomain::Boolean
            }
            (None, ColumnType::Boolean, ColumnType::Boolean) => {
                return Err(unsupported("ordering comparison of Boolean values"))
            }
            (None, l, r) => {
                return Err(unsupported(format!("comparison of {l:?} with {r:?}")));
            }
        };
        Ok(boxed(
            ColumnType::Boolean,
            Node::Compare {
                op,
                domain,
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }

    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)` rewrites `x == null` to `isNull(x)`,
    /// and `x != null` to `!(x == null)` first.
    fn null_comparison(
        &mut self,
        op: BinaryOp,
        left: &Expr,
        right: &Expr,
    ) -> Result<Typed, FormulaError> {
        let operand = match (left, right) {
            (Expr::Literal(Literal::Null), Expr::Literal(Literal::Null)) => {
                return Err(unsupported("comparison of null with null"))
            }
            (Expr::Literal(Literal::Null), operand) | (operand, _) => operand,
        };
        let is_null = |checker: &mut Self| -> Result<Typed, FormulaError> {
            let operand = checker.check(operand)?;
            Ok(boxed(ColumnType::Boolean, Node::IsNull(Box::new(operand))))
        };
        match op {
            BinaryOp::Equal => is_null(self),
            BinaryOp::NotEqual => Ok(boxed(
                ColumnType::Boolean,
                Node::Not(Box::new(is_null(self)?)),
            )),
            _ => Err(unsupported(format!(
                "null literal as an operand of {}",
                symbol(op)
            ))),
        }
    }

    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)` emits `&&` and `||` as Java's own
    /// operators, which javac accepts only for `boolean` and `Boolean` operands.
    fn logical(&mut self, op: LogicalOp, left: &Expr, right: &Expr) -> Result<Typed, FormulaError> {
        let left = self.check(left)?;
        let right = self.check(right)?;
        for operand in [&left, &right] {
            if operand.column_type != ColumnType::Boolean {
                let spelled = match op {
                    LogicalOp::And => "&&",
                    LogicalOp::Or => "||",
                };
                return Err(type_error(format!(
                    "cannot apply {spelled} to a value of type {:?}",
                    operand.column_type
                )));
            }
        }
        Ok(boxed(
            ColumnType::Boolean,
            Node::Logical {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }

    fn call(&mut self, function: &str, arguments: &[Expr]) -> Result<Typed, FormulaError> {
        match functions::lookup(function) {
            None => Err(unsupported(format!("function {function}()"))),
            Some(Function::IsNull) => {
                let [argument] = arguments else {
                    return Err(type_error(format!(
                        "isNull takes one argument, not {}",
                        arguments.len()
                    )));
                };
                let argument = self.check(argument)?;
                Ok(boxed(ColumnType::Boolean, Node::IsNull(Box::new(argument))))
            }
        }
    }
}

fn binary_op(op: ArithmeticOp) -> BinaryOp {
    match op {
        ArithmeticOp::Add => BinaryOp::Add,
        ArithmeticOp::Subtract => BinaryOp::Subtract,
        ArithmeticOp::Multiply => BinaryOp::Multiply,
        ArithmeticOp::Divide => BinaryOp::Divide,
        ArithmeticOp::Remainder => BinaryOp::Remainder,
    }
}

/// Behavior: the literal visitors of `QueryLanguageParser`. A `null` literal has no type of
/// its own and is accepted only where `null_comparison` handles it.
fn literal_type(literal: &Literal) -> Result<ColumnType, FormulaError> {
    Ok(match literal {
        Literal::Null => return Err(unsupported("null literal outside == and !=")),
        Literal::Boolean(_) => ColumnType::Boolean,
        Literal::Char(_) => ColumnType::Char,
        Literal::Int(_) => ColumnType::Int,
        Literal::Long(_) => ColumnType::Long,
        Literal::Float(_) => ColumnType::Float,
        Literal::Double(_) => ColumnType::Double,
        Literal::String(_) => ColumnType::String,
        Literal::Instant(_) => ColumnType::Instant,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const TYPES: [(&str, Numeric); 7] = [
        ("byte", Numeric::Int),
        ("short", Numeric::Int),
        ("char", Numeric::Int),
        ("int", Numeric::Int),
        ("long", Numeric::Long),
        ("float", Numeric::Float),
        ("double", Numeric::Double),
    ];

    fn initial(numeric: Numeric) -> char {
        match numeric {
            Numeric::Int => 'i',
            Numeric::Long => 'l',
            Numeric::Float => 'f',
            Numeric::Double => 'd',
        }
    }

    /// One row per left operand type and one letter per right operand type, both in the
    /// order of `TYPES`: the return type of the overload for that pair.
    fn table(op: ArithmeticOp) -> Vec<String> {
        TYPES
            .iter()
            .map(|(_, left)| {
                TYPES
                    .iter()
                    .map(|(_, right)| initial(arithmetic_type(op, *left, *right)))
                    .collect()
            })
            .collect()
    }

    /// The declared return types of `QueryLanguageFunctionUtils#plus(a,b)` for all 49 pairs
    /// of primitive numeric types; `#minus`, `#multiply` and `#remainder` declare the same.
    #[test]
    fn plus_minus_multiply_and_remainder_return_the_promoted_type() {
        let expected = [
            // byte short char int long float double
            "iiiilfd", // byte
            "iiiilfd", // short
            "iiiilfd", // char
            "iiiilfd", // int
            "lllllfd", // long
            "ffffffd", // float
            "ddddddd", // double
        ];
        for op in [
            ArithmeticOp::Add,
            ArithmeticOp::Subtract,
            ArithmeticOp::Multiply,
            ArithmeticOp::Remainder,
        ] {
            assert_eq!(table(op), expected, "{op:?}");
        }
    }

    /// The declared return types of `QueryLanguageFunctionUtils#divide(a,b)` for all 49 pairs
    /// of primitive numeric types. Note `#divide(int,float)` returns `float` while
    /// `#divide(float,int)` returns `double`.
    #[test]
    fn divide_returns_float_only_for_a_float_divisor() {
        let expected = [
            // byte short char int long float double
            "dddddfd", // byte
            "dddddfd", // short
            "dddddfd", // char
            "dddddfd", // int
            "dddddfd", // long
            "dddddfd", // float
            "ddddddd", // double
        ];
        assert_eq!(table(ArithmeticOp::Divide), expected);
    }

    /// How the body of `QueryLanguageFunctionUtils#compareTo(a,b)` compares two non-null
    /// values, for all 49 pairs of primitive numeric types: `i` with Java's integer `<` and
    /// `==`, `f` with `Float.compare`, `d` with `Double.compare`, and `x` with the exact
    /// long-against-floating comparison of `#compareTo(long,double)`, which
    /// `#compareTo(double,long)` and `#compareTo(float,long)` negate.
    #[test]
    fn ordering_comparisons_use_the_comparison_java_uses() {
        let expected = [
            // byte short char int long float double
            "iiiiifd", // byte
            "iiiiifd", // short
            "iiiiifd", // char
            "iiiiifd", // int
            "iiiiixx", // long
            "ffffxdd", // float
            "ddddxdd", // double
        ];
        let actual: Vec<String> = TYPES
            .iter()
            .map(|(left_name, left)| {
                TYPES
                    .iter()
                    .map(|(right_name, right)| {
                        match numeric_compare_domain(CompareOp::Less, *left, *right) {
                            CompareDomain::Integral => 'i',
                            CompareDomain::LongWithFloating => 'x',
                            // Java uses Double.compare for two floats. Widening a float
                            // to double is exact, so that is Float.compare.
                            CompareDomain::Floating {
                                through_float: true,
                            } if (*left_name, *right_name) != ("float", "float") => 'f',
                            CompareDomain::Floating { .. } => 'd',
                            other => panic!("{other:?} for {left_name}, {right_name}"),
                        }
                    })
                    .collect()
            })
            .collect();
        assert_eq!(actual, expected);
    }

    /// `QueryLanguageFunctionUtils#eq(a,b)` ends in Java's `a == b` for all 49 pairs, which
    /// compares under binary numeric promotion: `#eq(long,float)` converts the `long` to
    /// `float`, and `#eq(long,double)` converts it to `double`.
    #[test]
    fn equality_compares_in_the_promoted_type() {
        for (left_name, left) in TYPES {
            for (right_name, right) in TYPES {
                let expected = match left.max(right) {
                    Numeric::Int | Numeric::Long => CompareDomain::Integral,
                    Numeric::Float => CompareDomain::Floating {
                        through_float: true,
                    },
                    Numeric::Double => CompareDomain::Floating {
                        through_float: false,
                    },
                };
                for op in [CompareOp::Equal, CompareOp::NotEqual] {
                    assert_eq!(
                        numeric_compare_domain(op, left, right),
                        expected,
                        "{left_name} {op:?} {right_name}"
                    );
                }
            }
        }
    }
}
