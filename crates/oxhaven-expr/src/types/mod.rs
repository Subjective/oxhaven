//! Type checking: an expression tree plus a table definition become a typed tree in which
//! every operator knows the type it computes in.
//!
//! Java resolves each operator to an overload of a static method in
//! `io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils`, one per pair of operand
//! types. The overloads follow a few rules, and those rules are stated here once.
//!
//! What is not built is refused at one place each: an unknown function in [`Checker::call`]
//! (the function library registers names in [`crate::functions`]), row variables and column
//! arrays in [`Checker::check`] and [`Checker::column`].

mod color;
mod methods;

use crate::external::{ExternalArgument, ExternalFunctions};
use crate::functions::java_time;
use crate::functions::random;
use crate::functions::range::{self, RangeFn};
use crate::functions::{self, vectors, Kernel, Resolved};
use methods::Parameter;
pub(crate) use methods::StringMethod;
use oxhaven_contracts::expr::{BinaryOp, Expr, FormulaError, Literal, RowVariable, UnaryOp};
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
    /// Behavior: `QueryLanguageParser#binaryNumericPromotionType`, and the casts in
    /// `QueryLanguageFunctionUtils#plus(byte,byte)` and `#plus(char,char)`.
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
pub(crate) enum BitwiseOp {
    And,
    Or,
    Xor,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    /// Only on `int` and `long`.
    Bitwise(BitwiseOp),
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
    Floating { through_float: bool },
    /// An ordering comparison of a `long` with a `float` or `double`: exact, with no
    /// conversion of the `long`.
    LongWithFloating,
    /// `Boolean` values, with false before true as `Boolean#compareTo` has it.
    Boolean,
    /// Strings, ordered by UTF-16 code unit as `String#compareTo` does.
    String,
    /// `==` or `!=` of values of unrelated classes, which are equal only when both are null.
    /// `primitive` says of each side whether it is a primitive, which Java boxes, so that
    /// even its null value is an object.
    Unrelated { primitive: [bool; 2] },
    /// `<` and its relatives of `Comparable` values of two classes: null is below every
    /// value, and two values that are not null fail, as `compareTo` of the other class
    /// does. `primitive` as for [`CompareDomain::Unrelated`].
    UnrelatedOrder { primitive: [bool; 2] },
    /// `==` or `!=` of two values of one class held as an Arrow struct (`Period`,
    /// `ZonedDateTime`), equal when every field is.
    Fields,
}

/// The type an arithmetic operator computes in and returns.
///
/// Behavior: the return types and operand casts of `QueryLanguageFunctionUtils#plus`,
/// `#minus`, `#multiply`, `#remainder` (binary numeric promotion, as in
/// `QueryLanguageParser#binaryNumericPromotionType`) and `#divide`, which is always floating
/// point: `float` when the divisor is a `float` and the dividend is not a `double`, and
/// `double` otherwise, so `int / float` is `float` and `float / int` is `double`.
/// `#binaryAnd`, `#binaryOr` and `#xor` exist for `int` and `long` operands and for pairs of
/// one narrower type, and all return `int` or `long`.
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

/// The type of `condition ? a : b` for two numeric branch types, or `None` when Java
/// refuses the pair.
///
/// Behavior: `QueryLanguageParser#visit(ConditionalExpr)` and `#dhqlIsAssignableFrom`: two
/// branches of one type keep it; otherwise one branch must already have the type that
/// binary numeric promotion gives the pair. So `byte : int` is `int`, and `byte : short` is
/// refused, because their promoted type is `int` and neither branch is one.
pub(crate) fn ternary_type(if_true: &ColumnType, if_false: &ColumnType) -> Option<ColumnType> {
    if if_true == if_false {
        return Some(if_true.clone());
    }
    let promoted = Numeric::of(if_true)?
        .max(Numeric::of(if_false)?)
        .column_type();
    (*if_true == promoted || *if_false == promoted).then_some(promoted)
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
    /// `Col_`: the column at this index of [`Checked::arrays`], whole, as a vector.
    ColumnArray(usize),
    /// `i` or `ii`, the row's position in the table, or `k`, its key.
    RowVariable(RowVariable),
    /// Never `Literal::Null`.
    Literal(Literal),
    /// Null of the node's type.
    Null,
    /// Computed in the node's own type.
    Negate(Box<Typed>),
    Not(Box<Typed>),
    /// Computed in the node's own type.
    Arithmetic {
        op: ArithmeticOp,
        left: Box<Typed>,
        right: Box<Typed>,
    },
    /// `&`, `|` or `^` of two `Boolean` values.
    BooleanBitwise {
        op: BitwiseOp,
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
    /// A conversion between two different primitive types that keeps null: the node's type
    /// is the target.
    Cast(Box<Typed>),
    /// Java's implicit widening to the node's type, which converts the value that stands
    /// for null like any other value.
    Widen(Box<Typed>),
    /// Both branches have the node's type.
    Ternary {
        condition: Box<Typed>,
        if_true: Box<Typed>,
        if_false: Box<Typed>,
    },
    /// String concatenation; the operands have any type.
    Concat {
        left: Box<Typed>,
        right: Box<Typed>,
    },
    /// A library function; the arguments have the kernel's parameter types, in its order.
    Call {
        kernel: Kernel,
        arguments: Vec<Typed>,
    },
    /// A function of the embedder; an argument that is the `null` literal is `None`.
    External {
        function: crate::external::Bound,
        arguments: Vec<Option<Typed>>,
    },
    /// `toString()` of a column of `BigDecimal`, `BigInteger`, `LocalDate`, `LocalTime` or
    /// `LocalDateTime`.
    ObjectText(Box<Typed>),
    /// `new String(bytes)` of a `byte[]`.
    NewString(Box<Typed>),
    /// An operator applied to the elements of a vector or array and a value, or to the
    /// elements of two of them, pairwise. `body` is the operator on one pair: its
    /// `Node::Column(0)` is the left element and its `Node::Column(1)` the right one.
    Elementwise {
        body: Box<Typed>,
        left: Box<Typed>,
        right: Box<Typed>,
    },
    /// The operand as an object: the node's type is `Object` of the operand's class. A
    /// primitive is boxed, its null becoming the value Java reserves for it.
    ToObject(Box<Typed>),
    /// The value of an `Object` operand as the node's type: a `Number` converted to a
    /// primitive, or the operand's own class; a value of another class fails.
    FromObject(Box<Typed>),
    /// `==` of two operands as objects: an `Object`, a `DayOfWeek`, or a value that
    /// [`Node::ToObject`] boxed.
    ObjectEquals {
        left: Box<Typed>,
        right: Box<Typed>,
    },
    /// The `String` Java converts an object to, null for null: an operand of string
    /// concatenation.
    ObjectString(Box<Typed>),
    /// The `DayOfWeek` whose number (Monday 1) `number`, an `Int`, is; with `method`, a
    /// null number fails as a method call on a null `LocalDate` does.
    DayOfWeek {
        number: Box<Typed>,
        method: bool,
    },
    /// `int` arguments are of type `Int`.
    StringMethod {
        method: StringMethod,
        receiver: Box<Typed>,
        arguments: Vec<Typed>,
    },
}

/// A type-checked formula.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Checked {
    pub(crate) root: Typed,
    /// The columns the formula reads, in order of first use.
    pub(crate) columns: Vec<String>,
    /// The columns the formula reads as arrays (`Col_`), in order of first use.
    pub(crate) arrays: Vec<String>,
    /// Whether the formula uses `i` or `ii`.
    pub(crate) positions: bool,
    /// Whether the formula uses `k`.
    pub(crate) keys: bool,
    /// Why Java refuses the formula in a table operation of a client, when it does: the
    /// message of its `ColumnExpressionValidator` for the first call it does not allow.
    pub(crate) refused_to_clients: Option<String>,
}

/// Refuses a formula whose value is a column array of a column that is neither primitive
/// nor a string.
///
/// Java makes the vector an `ObjectVector` (`io.deephaven.engine.table.vectors.ColumnVectors#of`)
/// and the column one of `ObjectVector` without an element type, which a client receives
/// as strings: seen on Java 42.9 for `update("R = Z_")` of a `Boolean` column, a list of
/// `"true"` and `"false"`, for one of an `Instant` column, and for one of a column of
/// vectors, a list of `"[1,3,6]"` and the like.
fn refuse_object_column_array(root: &Typed) -> Result<(), FormulaError> {
    fn is_column_array(expr: &Typed) -> bool {
        match &expr.node {
            Node::ColumnArray(_) => true,
            Node::Ternary {
                if_true, if_false, ..
            } => is_column_array(if_true) || is_column_array(if_false),
            _ => false,
        }
    }
    let object = matches!(
        &root.column_type,
        ColumnType::Vector(element)
            if matches!(
                **element,
                ColumnType::Boolean | ColumnType::Instant | ColumnType::Vector(_)
            )
    );
    if object && is_column_array(root) {
        return Err(unsupported(format!(
            "a column array of type {:?} as the value of a formula, which Java sends as \
             strings (io.deephaven.engine.table.vectors.ColumnVectors#of)",
            root.column_type
        )));
    }
    Ok(())
}

/// Type-checks `expr` against `definition`.
pub(crate) fn check(
    expr: &Expr,
    definition: &TableDefinition,
    functions: Option<&dyn ExternalFunctions>,
) -> Result<Checked, FormulaError> {
    let mut checker = Checker {
        definition,
        functions,
        columns: Vec::new(),
        arrays: Vec::new(),
        positions: false,
        keys: false,
        refused_to_clients: None,
        size_only: false,
    };
    let root = checker.check(expr)?;
    refuse_object_column_array(&root)?;
    Ok(Checked {
        root,
        columns: checker.columns,
        arrays: checker.arrays,
        positions: checker.positions,
        keys: checker.keys,
        refused_to_clients: checker.refused_to_clients,
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

/// The Java name of a column type, for messages.
fn java_name(column_type: &ColumnType) -> String {
    match column_type {
        ColumnType::Boolean => "Boolean".into(),
        ColumnType::Byte => "byte".into(),
        ColumnType::Char => "char".into(),
        ColumnType::Short => "short".into(),
        ColumnType::Int => "int".into(),
        ColumnType::Long => "long".into(),
        ColumnType::Float => "float".into(),
        ColumnType::Double => "double".into(),
        ColumnType::String => "String".into(),
        ColumnType::Instant => "Instant".into(),
        ColumnType::LocalDate => "LocalDate".into(),
        ColumnType::LocalTime => "LocalTime".into(),
        ColumnType::Duration => "Duration".into(),
        ColumnType::Period => "Period".into(),
        ColumnType::ZoneId => "ZoneId".into(),
        ColumnType::ZonedDateTime => "ZonedDateTime".into(),
        ColumnType::DayOfWeek => "DayOfWeek".into(),
        ColumnType::PrimitiveBoolean => "boolean".into(),
        ColumnType::Object(_) => "Object".into(),
        other => format!("{other:?}"),
    }
}

fn typed(column_type: ColumnType, node: Node) -> Typed {
    Typed { column_type, node }
}

/// Whether the value is a vector or an array.
fn is_list(value: &Typed) -> bool {
    value.column_type.list_element().is_some()
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(Literal::Null))
}

/// What a binary operator does, once `&&` and `||` are set aside.
enum Family {
    Arithmetic(ArithmeticOp),
    Bitwise(BitwiseOp),
    Compare(CompareOp),
    Logical(LogicalOp),
}

fn family(op: BinaryOp) -> Family {
    match op {
        BinaryOp::Add => Family::Arithmetic(ArithmeticOp::Add),
        BinaryOp::Subtract => Family::Arithmetic(ArithmeticOp::Subtract),
        BinaryOp::Multiply => Family::Arithmetic(ArithmeticOp::Multiply),
        BinaryOp::Divide => Family::Arithmetic(ArithmeticOp::Divide),
        BinaryOp::Remainder => Family::Arithmetic(ArithmeticOp::Remainder),
        BinaryOp::Equal => Family::Compare(CompareOp::Equal),
        BinaryOp::NotEqual => Family::Compare(CompareOp::NotEqual),
        BinaryOp::Less => Family::Compare(CompareOp::Less),
        BinaryOp::LessOrEqual => Family::Compare(CompareOp::LessOrEqual),
        BinaryOp::Greater => Family::Compare(CompareOp::Greater),
        BinaryOp::GreaterOrEqual => Family::Compare(CompareOp::GreaterOrEqual),
        BinaryOp::And => Family::Logical(LogicalOp::And),
        BinaryOp::Or => Family::Logical(LogicalOp::Or),
        BinaryOp::BitAnd => Family::Bitwise(BitwiseOp::And),
        BinaryOp::BitOr => Family::Bitwise(BitwiseOp::Or),
        BinaryOp::BitXor => Family::Bitwise(BitwiseOp::Xor),
    }
}

/// A `java.time.Duration` or `java.time.Period` inside a formula.
struct Amount {
    /// Its length in nanoseconds, a `Long`; null where the duration or period is null.
    nanos: Typed,
    /// Whether it is a period, of days only.
    period: bool,
}

struct Checker<'a> {
    definition: &'a TableDefinition,
    /// The functions a call may be when the library cannot take it.
    functions: Option<&'a dyn ExternalFunctions>,
    columns: Vec<String>,
    arrays: Vec<String>,
    positions: bool,
    keys: bool,
    refused_to_clients: Option<String>,
    /// Set while the receiver of a call that reads only sizes and nulls is checked; the
    /// column array it names may then be one whose elements Java holds without a type
    /// (see [`Checker::column_array`]). Taken by the first column array checked.
    size_only: bool,
}

impl Checker<'_> {
    fn check(&mut self, expr: &Expr) -> Result<Typed, FormulaError> {
        match expr {
            Expr::Column(name) => self.column(name),
            // Behavior: `QueryLanguageParser#visit(NullLiteralExpr)`: the type of `null`
            // alone is `Object`, which a column of it has.
            Expr::Literal(Literal::Null) => Ok(typed(ColumnType::object(None), Node::Null)),
            Expr::Literal(literal) => literal_type(literal)
                .map(|column_type| typed(column_type, Node::Literal(literal.clone()))),
            // Behavior: `QueryLanguageParser`'s variables `i` (`int`), `ii` and `k` (`long`),
            // which `DhFormulaColumn#generateFillChunkHelper` and `ConditionFilter` supply.
            Expr::RowVariable(variable) => {
                let column_type = match variable {
                    RowVariable::I => ColumnType::Int,
                    RowVariable::Ii | RowVariable::K => ColumnType::Long,
                };
                match variable {
                    RowVariable::I | RowVariable::Ii => self.positions = true,
                    RowVariable::K => self.keys = true,
                }
                Ok(typed(column_type, Node::RowVariable(*variable)))
            }
            Expr::Unary { op, operand } => self.unary(*op, operand),
            Expr::Binary { op, left, right } => self.binary(*op, left, right),
            Expr::Call {
                function,
                arguments,
            } if matches!(function.as_str(), "len" | "isNull")
                && matches!(arguments.as_slice(), [argument] if self.untyped_vectors(argument)) =>
            {
                self.with_size_only(|checker| checker.call(function, arguments))
            }
            Expr::Call {
                function,
                arguments,
            } => self.call(function, arguments),
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } if matches!(method.as_str(), "size" | "isEmpty")
                && arguments.is_empty()
                && self.untyped_vectors(receiver) =>
            {
                self.with_size_only(|checker| checker.method_call(receiver, method, arguments))
            }
            Expr::Ternary {
                condition,
                if_true,
                if_false,
            } => self.ternary(condition, if_true, if_false),
            Expr::Cast { to, operand } => self.cast(to, operand),
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } => self.method_call(receiver, method, arguments),
            Expr::ColumnVector(name) => self.column_array(name),
            // Behavior: `QueryLanguageParser#visit(ArrayAccessExpr)`: an index into
            // anything that is not a Java array is a call of `get`, here on the vector
            // that `Col_` names, or on a column that is itself named `Col_`.
            Expr::ColumnArrayAccess { column, position } => {
                let name = format!("{column}_");
                let known = [name.as_str(), column.as_str()]
                    .iter()
                    .any(|name| self.definition.index_of(name).is_some());
                if !known {
                    return Err(type_error(format!("cannot find column {column}")));
                }
                self.method_call(
                    &Expr::Column(name),
                    crate::parse::INDEX_METHOD,
                    std::slice::from_ref(position),
                )
            }
        }
    }

    /// Whether `expr` is the column array of a column of vectors of `Boolean` or `Instant`,
    /// or an element of it: `T_` or `T_[i]`.
    fn untyped_vectors(&self, expr: &Expr) -> bool {
        let column = match expr {
            Expr::ColumnVector(column) | Expr::ColumnArrayAccess { column, .. } => column.as_str(),
            // `T_` as the receiver of a method, unless a column has that name.
            Expr::Column(name) if self.definition.index_of(name).is_none() => {
                match name.strip_suffix('_') {
                    Some(column) => column,
                    None => return false,
                }
            }
            _ => return false,
        };
        self.definition.index_of(column).is_some_and(|index| {
            matches!(
                &self.definition.columns()[index].column_type,
                ColumnType::Vector(inner)
                    if matches!(**inner, ColumnType::Boolean | ColumnType::Instant)
            )
        })
    }

    /// `check` with [`Checker::size_only`] set, and cleared afterwards whether or not a
    /// column array took it.
    fn with_size_only(
        &mut self,
        check: impl FnOnce(&mut Self) -> Result<Typed, FormulaError>,
    ) -> Result<Typed, FormulaError> {
        self.size_only = true;
        let checked = check(self);
        self.size_only = false;
        checked
    }

    fn column(&mut self, name: &str) -> Result<Typed, FormulaError> {
        // The parser writes `Class.FIELD` as one qualified name; a column name has no `.`.
        if let Some((scope, field)) = name.rsplit_once('.') {
            return self.static_field(scope, field);
        }
        let Some(index) = self.definition.index_of(name) else {
            let array_of = name
                .strip_suffix('_')
                .is_some_and(|base| self.definition.index_of(base).is_some());
            return if array_of {
                self.column_array(&name[..name.len() - 1])
            } else if let Some((column_type, literal)) = functions::constant(name) {
                Ok(constant(column_type, literal))
            } else if functions::is_other_constant(name) {
                Err(unsupported(format!("constant {name}")))
            } else {
                self.refuse_color_name(name)?;
                Err(type_error(format!("cannot find column {name}")))
            };
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
        ) || vectors::is_supported_list(&column_type)
            || java_time::is_time_object(&column_type)
            || is_object(&column_type);
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
        Ok(typed(column_type, Node::Column(slot)))
    }

    /// The column `expr` names, if it is a column of one of the `java.math` and
    /// `java.time` types.
    ///
    /// A formula can do three things with such a column: test it with `isNull`, call its
    /// `toString()`, and nothing else. So a column of these types is not a value like any
    /// other ([`Checker::column`] refuses it), and the two places that take one ask here.
    fn object_column(&mut self, expr: &Expr) -> Option<Typed> {
        let Expr::Column(name) = expr else {
            return None;
        };
        let index = self.definition.index_of(name)?;
        let column_type = self.definition.columns()[index].column_type.clone();
        let object = matches!(
            column_type,
            ColumnType::BigDecimal
                | ColumnType::BigInteger
                | ColumnType::LocalDate
                | ColumnType::LocalTime
                | ColumnType::LocalDateTime
        );
        if !object {
            return None;
        }
        let slot = match self.columns.iter().position(|used| used == name) {
            Some(slot) => slot,
            None => {
                self.columns.push(name.to_string());
                self.columns.len() - 1
            }
        };
        Some(typed(column_type, Node::Column(slot)))
    }

    /// `Col_`: the whole of the column `name` as a vector.
    ///
    /// Behavior: the `Col_` variables that `DhFormulaColumn#applyUsedVariables` and
    /// `AbstractConditionFilter#init` give a formula, of the vector type
    /// `io.deephaven.engine.table.vectors.ColumnVectors#of` makes for the column's type: a
    /// primitive vector for a primitive column, an `ObjectVector` for any other.
    fn column_array(&mut self, name: &str) -> Result<Typed, FormulaError> {
        let Some(index) = self.definition.index_of(name) else {
            return Err(type_error(format!("cannot find column {name}")));
        };
        let element = self.definition.columns()[index].column_type.clone();
        let scalar = |column_type: &ColumnType| {
            matches!(
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
            )
        };
        // The column array of a vector column is an `ObjectVector` of its vectors. One of a
        // column of `ObjectVector`s of `Boolean` or `Instant` has elements without an element
        // type in Java, which a client receives as strings (seen on Java 42.9 for
        // `update("X = T_[i]")` after a `group_by`), so it stays refused, but for the
        // calls that read only sizes and nulls: seen on Java 42.9, `T_.size()`,
        // `T_[i].size()`, `T_[i].isEmpty()`, `len(T_[i])`,
        // `isNull(T_[i + 1])` and `T_[i + 1] == null` are computed, and `T_[i][0]` is a
        // `java.lang.Object` sent as text.
        let size_only = std::mem::take(&mut self.size_only);
        let known = match &element {
            ColumnType::Vector(inner) => {
                scalar(inner)
                    && (size_only || !matches!(**inner, ColumnType::Boolean | ColumnType::Instant))
            }
            other => scalar(other),
        };
        if !known {
            return Err(unsupported(format!(
                "column array {name}_ of a column of type {element:?}"
            )));
        }
        let slot = match self.arrays.iter().position(|used| used == name) {
            Some(slot) => slot,
            None => {
                self.arrays.push(name.to_string());
                self.arrays.len() - 1
            }
        };
        Ok(typed(ColumnType::vector(element), Node::ColumnArray(slot)))
    }

    /// Whether the qualified name `scope` starts with the name of a column, and so is a
    /// field of that column's value and no class.
    fn is_column_scope(&self, scope: &str) -> bool {
        let first = scope.split('.').next().unwrap_or(scope);
        self.definition.index_of(first).is_some()
    }

    /// `scope.field`, where `scope` is a name, simple or qualified.
    ///
    /// Behavior: `QueryLanguageParser#visit(FieldAccessExpr)`, which types the scope with
    /// `#resolveName`, where a column comes before a class, and then takes the type of the
    /// field `Class#getField` finds on it. No column type has a field the language reads,
    /// so a column scope stays refused, as does every field [`functions::static_field`]
    /// does not give. A constant of a class of the default static imports is the constant
    /// a formula names without the class.
    fn static_field(&self, scope: &str, field: &str) -> Result<Typed, FormulaError> {
        let refused = || unsupported(format!("field access .{field}"));
        if self.is_column_scope(scope) {
            return Err(refused());
        }
        if let Some((column_type, literal)) = color::color_util_field(scope, field) {
            return Ok(constant(column_type, literal));
        }
        if functions::is_library_constant(scope, field) {
            return match functions::constant(field) {
                Some((column_type, literal)) => Ok(constant(column_type, literal)),
                None => Err(unsupported(format!("constant {field}"))),
            };
        }
        self.refuse_color_name(&format!("{scope}.{field}"))?;
        match functions::static_field(scope, field) {
            Some((column_type, literal)) => Ok(constant(column_type, literal)),
            None => Err(refused()),
        }
    }

    /// `class.method(arguments)`, where `class` is a name, simple or qualified, that is no
    /// column.
    ///
    /// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` with a class as the scope. A
    /// method of a class of the default static imports is the function a formula calls
    /// without the class. A method of a class of `java.lang` is the raw Java method
    /// ([`functions::statics`]): `#convertParameters` wraps an argument of another
    /// primitive type in the cast function of the parameter's type, and nothing else looks
    /// for null.
    fn static_call(
        &mut self,
        class: &str,
        method: &str,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        if class.contains('.') && self.is_column_scope(class) {
            let field = class.split('.').nth(1).unwrap_or(class);
            return Err(unsupported(format!("field access .{field}")));
        }
        let refused = || unsupported(format!("static method call {class}.{method}()"));
        // `io.deephaven.engine.util.ColorUtil`, the class a table format names, has the
        // methods of `ColorUtilImpl`, which it extends.
        let library = match class {
            "io.deephaven.engine.util.ColorUtil" => "io.deephaven.engine.util.ColorUtilImpl",
            other => other,
        };
        match functions::library_function(library, method) {
            functions::LibraryMember::NotLibrary => {}
            functions::LibraryMember::Missing => {
                return Err(type_error(format!(
                    "Cannot find method {method}() in class {class}"
                )))
            }
            functions::LibraryMember::Import => return self.import_call(method, arguments),
            // The call is that class's method only where the function a formula calls
            // without the class is.
            functions::LibraryMember::Shared { time } => {
                let call = self.import_call(method, arguments)?;
                let of_time = matches!(
                    &call.node,
                    Node::Call { kernel, .. } if kernel.is_shared_time()
                );
                return if of_time == time {
                    Ok(call)
                } else {
                    Err(refused())
                };
            }
        }
        if let Some(result) = self.time_static_call(class, method, arguments)? {
            return Ok(result);
        }
        if !functions::statics::is_class(class) {
            return Err(refused());
        }
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            checked.push(if is_null_literal(argument) {
                None
            } else {
                Some(self.check(argument)?)
            });
        }
        let types: Vec<Option<ColumnType>> = checked
            .iter()
            .map(|argument| argument.as_ref().map(|typed| typed.column_type.clone()))
            .collect();
        let resolved = functions::statics::resolve(class, method, &types)?;
        if self.refused_to_clients.is_none() {
            self.refused_to_clients = resolved.refusal();
        }
        let checked: Vec<Typed> = checked.into_iter().flatten().collect();
        let types: Vec<ColumnType> = types.into_iter().flatten().collect();
        Ok(library_call(
            functions::Call {
                kernel: Kernel::Static(resolved),
                parameters: functions::statics::parameter_types(resolved, &types),
                result: resolved.result_type(),
                exchanged: false,
            },
            checked,
        ))
    }

    /// `LocalDate.ofEpochDay(days)` and `LocalTime.of(hour, minute, ...)`, by the simple or
    /// the full name of the class; `None` for any other static call.
    fn time_static_call(
        &mut self,
        class: &str,
        method: &str,
        arguments: &[Expr],
    ) -> Result<Option<Typed>, FormulaError> {
        if java_time::TimeFn::static_method(class, method, &[ColumnType::Int]).is_none() {
            return Ok(None);
        }
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            if is_null_literal(argument) {
                return Err(unsupported(format!(
                    "static method call {class}.{method}() with a null literal as an argument"
                )));
            }
            checked.push(self.check(argument)?);
        }
        let types: Vec<ColumnType> = checked.iter().map(|c| c.column_type.clone()).collect();
        let Some(found) = java_time::TimeFn::static_method(class, method, &types) else {
            return Ok(None);
        };
        let (function, parameters, result) = found?;
        Ok(Some(library_call(
            functions::Call {
                kernel: Kernel::JavaTime(function),
                parameters,
                result,
                exchanged: false,
            },
            checked,
        )))
    }

    /// Behavior: `QueryLanguageParser#visit(UnaryExpr)`: `-` resolves to
    /// `QueryLanguageFunctionUtils#negate`, and `!` to `#not(Boolean)` or Java's own `!`.
    /// The visitor then asserts that the method returns the operand's type. `#negate(byte)`,
    /// `#negate(short)` and `#negate(char)` return `int`, so Java refuses to negate a
    /// `byte`, `short` or `char` with an assertion failure.
    fn unary(&mut self, op: UnaryOp, operand: &Expr) -> Result<Typed, FormulaError> {
        let operand = self.check(operand)?;
        match op {
            UnaryOp::Negate => match operand.column_type {
                ColumnType::Int | ColumnType::Long | ColumnType::Float | ColumnType::Double => Ok(
                    typed(operand.column_type.clone(), Node::Negate(Box::new(operand))),
                ),
                ColumnType::Byte | ColumnType::Short | ColumnType::Char => {
                    Err(type_error(format!(
                        "cannot negate a value of type {}: negate returns int, and Java's \
                         formula parser asserts that it returns the operand's type",
                        java_name(&operand.column_type)
                    )))
                }
                ref other => Err(type_error(format!(
                    "cannot negate a value of type {}",
                    java_name(other)
                ))),
            },
            UnaryOp::Not => match operand.column_type {
                ColumnType::Boolean => Ok(typed(ColumnType::Boolean, Node::Not(Box::new(operand)))),
                ref other => Err(type_error(format!(
                    "cannot apply ! to a value of type {}",
                    java_name(other)
                ))),
            },
            UnaryOp::BitNot => Err(type_error(
                "Unary operation (BITWISE_COMPLEMENT) not supported",
            )),
        }
    }

    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)`, in its order: `+` with a `String`
    /// on either side is concatenation; `&&` and `||` are Java's own operators; `x == null`
    /// is `isNull(x)`; everything else is a method of `QueryLanguageFunctionUtils` named
    /// after the operator.
    fn binary(&mut self, op: BinaryOp, left: &Expr, right: &Expr) -> Result<Typed, FormulaError> {
        let family = family(op);
        if let Family::Logical(op) = family {
            return self.logical(op, left, right);
        }

        if matches!(
            op,
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide
        ) {
            if let Some(result) = self.with_time_amount(op, left, right)? {
                return Ok(result);
            }
        }

        let null_left = is_null_literal(left);
        let null_right = is_null_literal(right);
        if null_left || null_right {
            if matches!(op, BinaryOp::Equal | BinaryOp::NotEqual) {
                return self.null_comparison(op, left, right);
            }
            if op == BinaryOp::Add && !(null_left && null_right) {
                // Java prints `s + null` as it is, and its string conversion of null is
                // "null".
                let other = self.check(if null_left { right } else { left })?;
                if other.column_type == ColumnType::String {
                    let null = typed(ColumnType::String, Node::Null);
                    let (left, right) = if null_left {
                        (null, other)
                    } else {
                        (other, null)
                    };
                    return Ok(concat(left, right));
                }
            }
            // No method of `QueryLanguageFunctionUtils` takes null for a primitive, and
            // `plus(null, null)` matches none either. Java does accept null beside a
            // `Boolean`, `String` or `Instant` operand, as an `Object` or `Comparable`.
            let rejected = match (null_left, null_right) {
                (true, true) => op == BinaryOp::Add,
                (true, false) => Numeric::of(&self.check(right)?.column_type).is_some(),
                _ => Numeric::of(&self.check(left)?.column_type).is_some(),
            };
            if rejected {
                return Err(type_error(format!("cannot apply {} to null", symbol(op))));
            }
            return self.null_operand(op, family, left, right);
        }

        // A range is held as a `double[]` here (`range::column_type`), and Java compares
        // two of them as objects, which is not ported.
        if matches!(family, Family::Compare(_)) && (makes_range(left) || makes_range(right)) {
            return Err(unsupported(format!(
                "operator {} on a {}",
                symbol(op),
                range::CLASS
            )));
        }
        let left = self.check(left)?;
        let right = self.check(right)?;
        if is_object(&left.column_type) || is_object(&right.column_type) {
            return object_operator(op, left, right);
        }
        // Java turns an operator with a vector or array operand into `eqArray`,
        // `lessArray` and their relatives, which compare element by element.
        if is_list(&left) || is_list(&right) {
            return self.elementwise(op, left, right);
        }
        if java_time::is_time_object(&left.column_type)
            || java_time::is_time_object(&right.column_type)
        {
            return time_object_operator(op, family, left, right);
        }
        match family {
            Family::Arithmetic(arithmetic) => self.arithmetic(op, arithmetic, left, right),
            Family::Bitwise(bitwise) => bitwise_operator(op, bitwise, left, right),
            Family::Compare(compare) => comparison(compare, left, right),
            Family::Logical(_) => unreachable!("handled above"),
        }
    }

    /// The duration or period that `expr` is, when it is one.
    ///
    /// A time literal that is a duration or a period reaches the checker as the call Java
    /// makes of it, `parseDuration("PT1m")` or `parsePeriod("P1d")`. Such a literal, and
    /// a product or quotient of one, is held here as a `long` of nanoseconds where a
    /// formula adds it to or subtracts it from an `Instant`, or bins an instant by it;
    /// everywhere else, and for a period of years or months, it is a `Duration` or
    /// `Period` value, which `time_object_operator` computes with.
    ///
    /// Behavior: `DateTimeUtils#multiply(Duration, long)`, `#multiply(long, Duration)`,
    /// `#multiply(Period, int)`, `#multiply(int, Period)` and `#divide(Duration, long)`,
    /// which `QueryLanguageParser#visit(BinaryExpr)` reaches by the operator's name. Java
    /// widens the number to the parameter, so a null `int` beside a duration is the number
    /// -2147483648.
    fn amount(&mut self, expr: &Expr) -> Result<Option<Amount>, FormulaError> {
        let class = |period: bool| if period { "Period" } else { "Duration" };
        match expr {
            Expr::Call {
                function,
                arguments,
            } if function == "parseDuration" || function == "parsePeriod" => {
                let [Expr::Literal(Literal::String(text))] = arguments.as_slice() else {
                    return Ok(None);
                };
                let period = function == "parsePeriod";
                let nanos = if period {
                    crate::parse::period_days(text).map(|days| {
                        days.and_then(|days| i64::from(days).checked_mul(86_400_000_000_000))
                    })
                } else {
                    crate::parse::duration_nanos(text)
                };
                match nanos {
                    Some(Some(nanos)) => Ok(Some(Amount {
                        nanos: typed(ColumnType::Long, Node::Literal(Literal::Long(nanos))),
                        period,
                    })),
                    // A period of years or months is a `Period` value, which an
                    // `Instant` fails to take as Java does.
                    Some(None) if period => Ok(None),
                    // Java holds durations longer than a `long` of nanoseconds.
                    Some(None) => Err(unsupported(format!(
                        "{} '{text}', which is not a whole number of nanoseconds that a long holds",
                        class(period)
                    ))),
                    None => Ok(None),
                }
            }
            Expr::Binary {
                op: op @ (BinaryOp::Multiply | BinaryOp::Divide),
                left,
                right,
            } => {
                let (amount, scalar) = match (self.amount(left)?, self.amount(right)?) {
                    (None, None) => return Ok(None),
                    (Some(amount), None) => (amount, right),
                    (None, Some(amount)) if *op == BinaryOp::Multiply => (amount, left),
                    _ => {
                        return Err(type_error(format!(
                            "cannot apply {} to these durations or periods",
                            symbol(*op)
                        )))
                    }
                };
                let method = if *op == BinaryOp::Multiply {
                    "multiply"
                } else {
                    "divide"
                };
                if is_null_literal(scalar) {
                    return Err(type_error(format!(
                        "Cannot find method {method}(java.time.{}, null)",
                        class(amount.period)
                    )));
                }
                let scalar = self.check(scalar)?;
                let scalar = amount_scalar(amount.period, *op, scalar)?;
                let kernel = if *op == BinaryOp::Multiply {
                    Kernel::DurationMultiply
                } else {
                    Kernel::DurationDivide
                };
                Ok(Some(Amount {
                    nanos: typed(
                        ColumnType::Long,
                        Node::Call {
                            kernel,
                            arguments: vec![amount.nanos, scalar],
                        },
                    ),
                    period: amount.period,
                }))
            }
            _ => Ok(None),
        }
    }

    /// A field of an instant's date and time in a zone: `hourOfDay(Instant, ZoneId,
    /// boolean)`, `dayOfYear(Instant, ZoneId)` and their relatives; `None` for another
    /// number of arguments.
    ///
    /// Where the formula names the zone, as `timeZone` or `parseTimeZone` of a string
    /// literal (`TimeZoneAliases#zoneId` of the name), the zone is fixed; any other
    /// `ZoneId` value is read in each row, and a null zone makes the field null.
    fn zoned(
        &mut self,
        function: &str,
        field: functions::ZonedField,
        arguments: &[Expr],
    ) -> Result<Option<Typed>, FormulaError> {
        if let Some(result) = self.field_of_value(function, field, arguments)? {
            return Ok(Some(result));
        }
        let (instant, zone, flag) = match (arguments, field.of_day()) {
            ([instant, zone], false) => (instant, zone, None),
            ([instant, zone, flag], true) => (instant, zone, Some(flag)),
            _ => return Ok(None),
        };
        let name = match zone {
            Expr::Call {
                function: maker,
                arguments,
            } if maker == "parseTimeZone" || maker == "timeZone" => match arguments.as_slice() {
                [Expr::Literal(Literal::String(name))] => Some(name),
                _ => None,
            },
            _ => None,
        };
        // The zone a formula names is fixed; any other is a `ZoneId` value of each row.
        let (fixed, by_row) = match name {
            Some(name) => (
                Some(
                    crate::parse::instant::zone(name)
                        .ok_or_else(|| unsupported(format!("time zone '{name}'")))?,
                ),
                None,
            ),
            None if !is_null_literal(zone) => {
                let zone = self.check(zone)?;
                if zone.column_type != ColumnType::ZoneId {
                    return Err(type_error(format!(
                        "Cannot find method {function}() of these arguments"
                    )));
                }
                (None, Some(zone))
            }
            None => {
                return Err(unsupported(format!(
                    "null literal as an argument of {function}()"
                )))
            }
        };
        if is_null_literal(instant) || flag.is_some_and(is_null_literal) {
            return Err(unsupported(format!(
                "null literal as an argument of {function}()"
            )));
        }
        let instant = self.check(instant)?;
        if instant.column_type != ColumnType::Instant {
            return Err(unsupported(format!(
                "function {function}() of a value of type {}",
                java_name(&instant.column_type)
            )));
        }
        let mut checked = vec![instant];
        let kernel = match (fixed, by_row) {
            (Some(zone), _) => Kernel::Zoned(field, zone),
            (None, Some(zone)) => {
                checked.push(zone);
                Kernel::ZonedByColumn(field)
            }
            (None, None) => unreachable!("one of the two is set above"),
        };
        if let Some(flag) = flag {
            let flag = self.check(flag)?;
            if flag.column_type != ColumnType::Boolean {
                return Err(type_error(format!(
                    "Cannot find method {function}() of these arguments"
                )));
            }
            checked.push(flag);
        }
        Ok(Some(typed(
            field.result_type(),
            Node::Call {
                kernel,
                arguments: checked,
            },
        )))
    }

    /// The same field of a `ZonedDateTime` (with the flag of a field of the day), a
    /// `LocalDate` or a `LocalTime`; `None` for other arguments.
    ///
    /// Behavior: `DateTimeUtils#hourOfDay(ZonedDateTime, boolean)`, `#year(ZonedDateTime)`
    /// and their relatives, null for null; `#year(LocalDate)`, `#monthOfYear(LocalDate)`,
    /// `#dayOfMonth(LocalDate)`, `#dayOfYear(LocalDate)`, `#dayOfWeekValue(LocalDate)` and
    /// `#yearOfCentury(LocalDate)`; `#hourOfDay(LocalTime)`, `#minuteOfDay(LocalTime)`,
    /// `#secondOfDay(LocalTime)`, `#millisOfDay(LocalTime)` and `#nanosOfDay(LocalTime)`.
    fn field_of_value(
        &mut self,
        function: &str,
        field: functions::ZonedField,
        arguments: &[Expr],
    ) -> Result<Option<Typed>, FormulaError> {
        use functions::ZonedField as F;
        let of_day = field.of_day();
        let (value, flag) = match (arguments, of_day) {
            ([value], _) => (value, None),
            ([value, flag], true) => (value, Some(flag)),
            _ => return Ok(None),
        };
        if is_null_literal(value) || flag.is_some_and(is_null_literal) {
            return Ok(None);
        }
        let value = self.check(value)?;
        let kernel = match (&value.column_type, flag) {
            (ColumnType::ZonedDateTime, Some(_)) => Kernel::ZonedByColumn(field),
            (ColumnType::ZonedDateTime, None) if !of_day => Kernel::ZonedByColumn(field),
            (ColumnType::LocalTime, None) if of_day => {
                Kernel::JavaTime(java_time::TimeFn::TimeField(field))
            }
            (ColumnType::LocalDate, None)
                if matches!(
                    field,
                    F::Year
                        | F::YearOfCentury
                        | F::MonthOfYear
                        | F::DayOfMonth
                        | F::DayOfYear
                        | F::DayOfWeekValue
                ) =>
            {
                Kernel::JavaTime(java_time::TimeFn::DateField(field))
            }
            _ => return Ok(None),
        };
        let mut checked = vec![value];
        if let Some(flag) = flag {
            let flag = self.check(flag)?;
            if flag.column_type != ColumnType::Boolean {
                return Err(type_error(format!(
                    "Cannot find method {function}() of these arguments"
                )));
            }
            checked.push(flag);
        }
        Ok(Some(typed(
            field.result_type(),
            Node::Call {
                kernel,
                arguments: checked,
            },
        )))
    }

    /// `lowerBin` or `upperBin` of an instant with the interval, and the offset if there
    /// is one, as durations; `None` when neither is a duration or a period.
    ///
    /// Behavior: `DateTimeUtils#lowerBin(Instant, Duration)`, `#lowerBin(Instant,
    /// Duration, Duration)` and the same of `#upperBin`, which bin by the durations'
    /// nanoseconds. No overload takes a period, or a duration beside a number.
    fn bin_by_duration(
        &mut self,
        function: &str,
        instant: &Expr,
        amounts: &[Expr],
    ) -> Result<Option<Typed>, FormulaError> {
        if !(1..=2).contains(&amounts.len()) {
            return Ok(None);
        }
        let mut nanos = Vec::with_capacity(amounts.len());
        for amount in amounts {
            nanos.push(self.amount(amount)?);
        }
        if nanos.iter().all(Option::is_none) {
            return Ok(None);
        }
        let no_method = || {
            type_error(format!(
                "Cannot find method {function}() of these arguments"
            ))
        };
        if is_null_literal(instant) {
            return Err(no_method());
        }
        let instant = self.check(instant)?;
        let mut arguments = vec![instant];
        for amount in nanos {
            match amount {
                Some(amount) if !amount.period => arguments.push(amount.nanos),
                _ => return Err(no_method()),
            }
        }
        if arguments[0].column_type != ColumnType::Instant {
            return Err(no_method());
        }
        let kernel = if function == "lowerBin" {
            Kernel::InstantLowerBin
        } else {
            Kernel::InstantUpperBin
        };
        Ok(Some(typed(
            ColumnType::Instant,
            Node::Call { kernel, arguments },
        )))
    }

    /// `left op right` when one side is a duration or a period; `None` when neither is.
    ///
    /// Behavior: `DateTimeUtils#plus(Instant, Duration)`, `#plus(Instant, Period)` and the
    /// two `minus`: null when either is null, otherwise `Instant#plus`, for which a day of
    /// a period is 86,400 seconds. There is no `plus(Duration, Instant)`.
    fn with_time_amount(
        &mut self,
        op: BinaryOp,
        left: &Expr,
        right: &Expr,
    ) -> Result<Option<Typed>, FormulaError> {
        let sum = matches!(op, BinaryOp::Add | BinaryOp::Subtract);
        let (Some(amount), other) = (match (self.amount(left)?, self.amount(right)?) {
            (None, None) => return Ok(None),
            (None, Some(amount)) if sum => (Some(amount), left),
            (Some(amount), None) if sum && !is_null_literal(right) => {
                let other = self.check(right)?;
                if java_time::is_time_object(&other.column_type) {
                    return Ok(None);
                }
                if other.column_type != ColumnType::String || op != BinaryOp::Add {
                    return Err(type_error(format!(
                        "Cannot find method {}(java.time.{}, {})",
                        if op == BinaryOp::Add { "plus" } else { "minus" },
                        if amount.period { "Period" } else { "Duration" },
                        java_name(&other.column_type)
                    )));
                }
                (None, left)
            }
            // A null beside a duration or period.
            _ if is_null_literal(left) || is_null_literal(right) => (None, left),
            // Any other operator, or two durations or periods: `time_object_operator`.
            _ => return Ok(None),
        }) else {
            return Err(unsupported(
                "a duration or period that is not added to or subtracted from an Instant",
            ));
        };
        let class = if amount.period { "Period" } else { "Duration" };
        let (method, kernel) = if op == BinaryOp::Add {
            ("plus", Kernel::InstantPlus)
        } else {
            ("minus", Kernel::InstantMinus)
        };
        let other = if is_null_literal(other) {
            None
        } else {
            Some(self.check(other)?)
        };
        match other {
            Some(instant) if instant.column_type == ColumnType::Instant => Ok(Some(typed(
                ColumnType::Instant,
                Node::Call {
                    kernel,
                    arguments: vec![instant, amount.nanos],
                },
            ))),
            // Java concatenates the text of the duration or period.
            Some(text) if text.column_type == ColumnType::String && op == BinaryOp::Add => Err(
                unsupported(format!("concatenation of a String and a {class}")),
            ),
            // A duration or period column beside it: `time_object_operator`.
            Some(other) if java_time::is_time_object(&other.column_type) => Ok(None),
            Some(other) => Err(type_error(format!(
                "Cannot find method {method}({}, java.time.{class})",
                java_name(&other.column_type)
            ))),
            None => Err(unsupported(format!("{method}(null, java.time.{class})"))),
        }
    }

    /// An operator with a vector or an array on either side.
    ///
    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)` names the method after the operator
    /// with `Array` appended (`plusArray`, `eqArray`, `lessArray`), and
    /// `#convertParameters` passes a vector as the array of its elements. The methods of
    /// `QueryLanguageFunctionUtils` apply the operator of one value to each element, or to
    /// each pair of elements, and return an array of what that operator returns. They
    /// exist for the primitive numeric types in every combination, for `==` on `boolean`
    /// and on objects, and for the orderings on `Comparable`. `!=` becomes `!(eqArray(..))`,
    /// which fails the parser's assertion that `!` has a `boolean` operand. The arithmetic
    /// is computed here; the comparisons are not.
    fn elementwise(
        &mut self,
        op: BinaryOp,
        left: Typed,
        right: Typed,
    ) -> Result<Typed, FormulaError> {
        if op == BinaryOp::NotEqual {
            return Err(type_error(
                "!= on a vector or array: asserted ret.equals(boolean.class), instead ret is \
                 boolean[]",
            ));
        }
        let element = |side: &Typed| {
            side.column_type
                .list_element()
                .unwrap_or(&side.column_type)
                .clone()
        };
        let (left_element, right_element) = (element(&left), element(&right));
        let number = |element: &ColumnType| Numeric::of(element).is_some();
        let vector = |side: &Typed| matches!(side.column_type, ColumnType::Vector(_));
        let arithmetic = match family(op) {
            Family::Arithmetic(arithmetic) if number(&left_element) && number(&right_element) => {
                arithmetic
            }
            // Behavior: `QueryLanguageFunctionUtils#eqArray`, `#lessArray`,
            // `#lessEqualsArray`, `#greaterArray`, `#greaterEqualsArray` of two arrays of
            // numbers or of an array and a number: each pair of elements compared as
            // `eq`, `less` and their relatives compare two numbers, into a `boolean[]`.
            // They take Java arrays only, not vectors.
            Family::Compare(compare)
                if number(&left_element)
                    && number(&right_element)
                    && !vector(&left)
                    && !vector(&right) =>
            {
                let one = |element: ColumnType, slot: usize| typed(element, Node::Column(slot));
                let body = comparison(compare, one(left_element, 0), one(right_element, 1))?;
                return Ok(typed(
                    ColumnType::array(ColumnType::PrimitiveBoolean),
                    Node::Elementwise {
                        body: Box::new(body),
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                ));
            }
            // `eqArray` and its relatives of other elements, of `Boolean[]`, of `Object[]`,
            // and of vectors are not ported. `binaryAndArray` and its relatives exist for
            // some types only, and `+` with a `String` prints the array's identity.
            _ => {
                return Err(unsupported(format!(
                    "operator {} on {} and {}",
                    symbol(op),
                    java_name(&left.column_type),
                    java_name(&right.column_type)
                )))
            }
        };
        let one = |element: ColumnType, slot: usize| typed(element, Node::Column(slot));
        let body = self.arithmetic(op, arithmetic, one(left_element, 0), one(right_element, 1))?;
        Ok(typed(
            ColumnType::Array(Box::new(body.column_type.clone())),
            Node::Elementwise {
                body: Box::new(body),
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }

    fn arithmetic(
        &mut self,
        spelled: BinaryOp,
        op: ArithmeticOp,
        left: Typed,
        right: Typed,
    ) -> Result<Typed, FormulaError> {
        let either = |wanted: ColumnType| left.column_type == wanted || right.column_type == wanted;
        if op == ArithmeticOp::Add && either(ColumnType::String) {
            return Ok(concat(left, right));
        }
        let Some((l, r)) = Numeric::of(&left.column_type).zip(Numeric::of(&right.column_type))
        else {
            // `plus(Instant, long)`, `minus(Instant, long)` and `minus(Instant, Instant)`
            // of `DateTimeUtils` are the only arithmetic on an `Instant`.
            let name = match op {
                ArithmeticOp::Add => "plus",
                ArithmeticOp::Subtract => "minus",
                _ => return Err(operands_error(spelled, &left, &right)),
            };
            let types = [
                Some(left.column_type.clone()),
                Some(right.column_type.clone()),
            ];
            return match functions::resolve(name, &types) {
                Ok(Resolved::Call(call)) => Ok(library_call(call, vec![left, right])),
                _ => Err(operands_error(spelled, &left, &right)),
            };
        };
        let result = arithmetic_type(op, l, r);
        Ok(typed(
            result.column_type(),
            Node::Arithmetic {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
        ))
    }

    /// An operator with the null literal on one side or both, where the other side is no
    /// number and the operator is neither `==` nor `!=`.
    ///
    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)` looks the operator's method up
    /// with the null as an argument that any reference parameter takes. Java 42.9 was run
    /// for each form here:
    ///
    /// - `&`, `|` and `^` reach `QueryLanguageFunctionUtils#binaryAnd(Boolean, Boolean)` and
    ///   its relatives, which answer null when either side is null, whatever the other is:
    ///   `false & null` and `true | null` are null.
    /// - `<`, `<=`, `>` and `>=` beside a `String`, `Boolean` or `Instant` reach
    ///   `QueryLanguageFunctionUtils#less(Comparable, Comparable)` and its relatives, which
    ///   order null before every value.
    /// - `null - Instant` is `DateTimeUtils#minus(Instant, Instant)`, a null `long`.
    ///
    /// Java finds no method, or more than one, for the rest, and fails.
    fn null_operand(
        &mut self,
        op: BinaryOp,
        family: Family,
        left: &Expr,
        right: &Expr,
    ) -> Result<Typed, FormulaError> {
        let refused = || type_error(format!("cannot apply {} to null", symbol(op)));
        let null_left = is_null_literal(left);
        let other = match (null_left, is_null_literal(right)) {
            (true, true) => None,
            (true, false) => Some(self.check(right)?),
            _ => Some(self.check(left)?),
        };
        let beside = other.as_ref().map(|other| &other.column_type);
        let null_as = |column_type: ColumnType, other: Option<Typed>| {
            let null = typed(column_type.clone(), Node::Null);
            match other {
                None => (null, typed(column_type, Node::Null)),
                Some(other) if null_left => (null, other),
                Some(other) => (other, null),
            }
        };
        match family {
            Family::Bitwise(bitwise) if matches!(beside, None | Some(ColumnType::Boolean)) => {
                let (left, right) = null_as(ColumnType::Boolean, other);
                bitwise_operator(op, bitwise, left, right)
            }
            Family::Compare(compare) => match beside {
                Some(ColumnType::String | ColumnType::Boolean | ColumnType::Instant) => {
                    let column_type = beside.cloned().expect("matched above");
                    let (left, right) = null_as(column_type, other);
                    comparison(compare, left, right)
                }
                // `less(Comparable, Comparable)` and its relatives, which order null
                // first; a `Period` or a `ZoneId` is no `Comparable` (seen on Java 42.9:
                // `Dur < null` and `ZD < null` are false, `P < null` and `Z < null` do not
                // compile). A `DayOfWeek` is an enum, a `Comparable` (seen on Java 42.9:
                // `W >= null` is true and `W > null` true where `W` is not null).
                Some(column_type)
                    if java_time::is_time_object(column_type)
                        || *column_type == ColumnType::DayOfWeek =>
                {
                    if !comparable(column_type) && *column_type != ColumnType::DayOfWeek {
                        return Err(refused());
                    }
                    let (left, right) = null_as(column_type.clone(), other);
                    let primitive = [false, false];
                    Ok(typed(
                        ColumnType::Boolean,
                        Node::Compare {
                            op: compare,
                            domain: CompareDomain::UnrelatedOrder { primitive },
                            left: Box::new(left),
                            right: Box::new(right),
                        },
                    ))
                }
                None => Err(refused()),
                Some(_) => Err(unsupported(format!(
                    "null literal as an operand of {}",
                    symbol(op)
                ))),
            },
            Family::Arithmetic(arithmetic)
                if op == BinaryOp::Subtract
                    && null_left
                    && beside == Some(&ColumnType::Instant) =>
            {
                let (left, right) = null_as(ColumnType::Instant, other);
                self.arithmetic(op, arithmetic, left, right)
            }
            Family::Arithmetic(_)
                if matches!(
                    beside,
                    Some(ColumnType::String | ColumnType::Boolean | ColumnType::Instant)
                ) =>
            {
                Err(refused())
            }
            _ => Err(unsupported(format!(
                "null literal as an operand of {}",
                symbol(op)
            ))),
        }
    }

    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)` rewrites `x == null` to `isNull(x)`,
    /// and `x != null` to `!(x == null)` first. Of `null == null` that is `isNull(null)`,
    /// which is true.
    fn null_comparison(
        &mut self,
        op: BinaryOp,
        left: &Expr,
        right: &Expr,
    ) -> Result<Typed, FormulaError> {
        let operand = match (left, right) {
            (Expr::Literal(Literal::Null), Expr::Literal(Literal::Null)) => {
                return Ok(typed(
                    ColumnType::Boolean,
                    Node::Literal(Literal::Boolean(op == BinaryOp::Equal)),
                ))
            }
            (Expr::Literal(Literal::Null), operand) | (operand, _) => operand,
        };
        let operand = if self.untyped_vectors(operand) {
            self.with_size_only(|checker| checker.check(operand))?
        } else {
            self.check(operand)?
        };
        let is_null = typed(ColumnType::Boolean, Node::IsNull(Box::new(operand)));
        Ok(match op {
            BinaryOp::Equal => is_null,
            _ => typed(ColumnType::Boolean, Node::Not(Box::new(is_null))),
        })
    }

    /// Behavior: `QueryLanguageParser#visit(BinaryExpr)` emits `&&` and `||` as Java's own
    /// operators, which javac accepts only for `boolean` and `Boolean` operands.
    fn logical(&mut self, op: LogicalOp, left: &Expr, right: &Expr) -> Result<Typed, FormulaError> {
        let spelled = match op {
            LogicalOp::And => "&&",
            LogicalOp::Or => "||",
        };
        let mut operand = |expr: &Expr| -> Result<Box<Typed>, FormulaError> {
            if is_null_literal(expr) {
                return Err(type_error(format!("cannot apply {spelled} to null")));
            }
            let operand = self.check(expr)?;
            if operand.column_type != ColumnType::Boolean {
                return Err(type_error(format!(
                    "cannot apply {spelled} to a value of type {}",
                    java_name(&operand.column_type)
                )));
            }
            Ok(Box::new(operand))
        };
        let left = operand(left)?;
        let right = operand(right)?;
        Ok(typed(
            ColumnType::Boolean,
            Node::Logical { op, left, right },
        ))
    }

    /// Behavior: `QueryLanguageParser#visit(ConditionalExpr)`. A `null` branch takes the
    /// type of the other branch (`NULL_INT` and its relatives for a primitive). The branch
    /// types must then agree as [`ternary_type`] says, and Java converts the narrower
    /// numeric branch by plain widening, which does not keep null: `c ? IntCol : LongCol` is
    /// -2147483648 where `IntCol` is null.
    fn ternary(
        &mut self,
        condition: &Expr,
        if_true: &Expr,
        if_false: &Expr,
    ) -> Result<Typed, FormulaError> {
        if is_null_literal(condition) {
            return Err(type_error("the condition of ?: cannot be null"));
        }
        let condition = self.check(condition)?;
        if condition.column_type != ColumnType::Boolean {
            return Err(type_error(format!(
                "the condition of ?: has type {}, not boolean",
                java_name(&condition.column_type)
            )));
        }
        let null_of = |branch: &Typed| typed(branch.column_type.clone(), Node::Null);
        let (if_true, if_false) = match (is_null_literal(if_true), is_null_literal(if_false)) {
            (true, true) => {
                let null = || Box::new(typed(ColumnType::object(None), Node::Null));
                return Ok(typed(
                    ColumnType::object(None),
                    Node::Ternary {
                        condition: Box::new(condition),
                        if_true: null(),
                        if_false: null(),
                    },
                ));
            }
            (true, false) => {
                let if_false = self.check(if_false)?;
                (null_of(&if_false), if_false)
            }
            (false, true) => {
                let if_true = self.check(if_true)?;
                let null = null_of(&if_true);
                (if_true, null)
            }
            (false, false) => (self.check(if_true)?, self.check(if_false)?),
        };
        if if_true.column_type != if_false.column_type
            && (matches!(if_true.column_type, ColumnType::Object(_))
                || matches!(if_false.column_type, ColumnType::Object(_)))
        {
            return object_ternary(condition, if_true, if_false);
        }
        let Some(result) = ternary_type(&if_true.column_type, &if_false.column_type) else {
            if is_list(&if_true) || is_list(&if_false) {
                return Err(unsupported(
                    "?: with a vector or array branch and a branch of another type",
                ));
            }
            return Err(type_error(format!(
                "Incompatible types in condition operation not supported : {} {}",
                java_name(&if_true.column_type),
                java_name(&if_false.column_type)
            )));
        };
        let widen = |branch: Typed| {
            if branch.column_type == result {
                branch
            } else {
                typed(result.clone(), Node::Widen(Box::new(branch)))
            }
        };
        Ok(typed(
            result.clone(),
            Node::Ternary {
                condition: Box::new(condition),
                if_true: Box::new(widen(if_true)),
                if_false: Box::new(widen(if_false)),
            },
        ))
    }

    /// Behavior: `QueryLanguageParser#visit(CastExpr)`. A cast between two different
    /// primitive types becomes `intCast` and its relatives in
    /// `QueryLanguageFunctionUtils`, which keep null. A cast to the operand's own type does
    /// nothing. `Boolean` converts to and from nothing else. A cast to `Boolean`, `String`
    /// or `Instant` is Java's own cast, which javac accepts for the type itself and for
    /// `null`. A cast of `null` to a primitive is `intCast(Object)` or one of its relatives,
    /// which return the type's null value for null.
    fn cast(&mut self, to: &ColumnType, operand: &Expr) -> Result<Typed, FormulaError> {
        let primitive = |column_type: &ColumnType| Numeric::of(column_type).is_some();
        if is_null_literal(operand) {
            return Ok(typed(to.clone(), Node::Null));
        }
        let operand = self.check(operand)?;
        let from = operand.column_type.clone();
        if from == *to {
            return Ok(operand);
        }
        if let ColumnType::Object(_) = to {
            if let ColumnType::Object(_) = from {
                return Ok(operand);
            }
            return boxed(operand);
        }
        if let ColumnType::Object(_) = from {
            return from_object(to, operand);
        }
        if primitive(to) && primitive(&from) {
            return Ok(typed(to.clone(), Node::Cast(Box::new(operand))));
        }
        if primitive(to)
            && (matches!(from, ColumnType::String | ColumnType::Instant) || is_list(&operand))
        {
            // `intCast(Object)`: null for null, and otherwise a ClassCastException.
            return Err(unsupported(format!(
                "cast of {} to {}",
                java_name(&from),
                java_name(to)
            )));
        }
        Err(type_error(format!(
            "Incompatible types; {} cannot be converted to {}",
            java_name(&from),
            java_name(to)
        )))
    }

    /// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` prints an instance method call
    /// as it is, after `#convertParameters` has wrapped each `byte`, `short` or `char`
    /// argument of an `int` parameter in `intCast`.
    fn method_call(
        &mut self,
        receiver: &Expr,
        method: &str,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        let length = method == crate::parse::LENGTH_FIELD;
        if let Expr::Call {
            function,
            arguments: seed,
        } = receiver
        {
            let created = function.strip_prefix(crate::parse::NEW);
            if created.is_some_and(random::is_class) {
                return self.random_call(seed, method, arguments);
            }
        }
        if method == "toString" && arguments.is_empty() {
            // Behavior: `java.math.BigDecimal#toString`, `java.math.BigInteger#toString`,
            // `java.time.LocalDate#toString`, `java.time.LocalTime#toString`,
            // `java.time.LocalDateTime#toString`
            if let Some(receiver) = self.object_column(receiver) {
                return Ok(typed(
                    ColumnType::String,
                    Node::ObjectText(Box::new(receiver)),
                ));
            }
        }
        if let Expr::Column(name) = receiver {
            let column = length
                || self.definition.index_of(name).is_some()
                || name
                    .strip_suffix('_')
                    .is_some_and(|base| self.definition.index_of(base).is_some());
            if !column {
                return self.static_call(name, method, arguments);
            }
        }
        let receiver = self.check(receiver)?;
        if receiver.column_type == range::column_type() {
            if let Some(function) = RangeFn::method(method) {
                return self.range_call(function, Some(receiver), arguments);
            }
        }
        if is_list(&receiver) {
            return self.vector_method(receiver, method, arguments);
        }
        if length {
            return Err(unsupported("field access .length"));
        }
        if method == crate::parse::INDEX_METHOD {
            // Java calls `get` on anything that is not an array.
            return Err(unsupported(format!(
                "an index on a value of type {}",
                java_name(&receiver.column_type)
            )));
        }
        // Behavior: `java.time.LocalDate#getDayOfWeek`, a method `dh-defaults.prop` lets
        // a client call.
        if (&receiver.column_type, method, arguments.len())
            == (&ColumnType::LocalDate, "getDayOfWeek", 0)
        {
            let number = typed(
                ColumnType::Int,
                Node::Call {
                    kernel: Kernel::JavaTime(java_time::TimeFn::DateField(
                        functions::ZonedField::DayOfWeekValue,
                    )),
                    arguments: vec![receiver],
                },
            );
            return Ok(typed(
                ColumnType::DayOfWeek,
                Node::DayOfWeek {
                    number: Box::new(number),
                    method: true,
                },
            ));
        }
        if Numeric::of(&receiver.column_type).is_some() {
            return Err(type_error(format!(
                "cannot call .{method}() on a value of the primitive type {}",
                java_name(&receiver.column_type)
            )));
        }
        // Behavior: `QueryLanguageParser#visit(MethodCallExpr)` with a value as the scope:
        // the method of the value's class, here `java.time.Instant`.
        if receiver.column_type == ColumnType::Instant && arguments.is_empty() {
            if let Some(instant_method) = functions::InstantMethod::named(method) {
                return Ok(typed(
                    instant_method.result_type(),
                    Node::Call {
                        kernel: Kernel::InstantMethod(instant_method),
                        arguments: vec![receiver],
                    },
                ));
            }
        }
        // Behavior: `java.time.Instant#atZone(ZoneId)`, which throws for a null instant or
        // zone.
        if let (ColumnType::Instant, "atZone", [zone]) = (&receiver.column_type, method, arguments)
        {
            if is_null_literal(zone) {
                return Err(unsupported(
                    "null literal as an argument of Instant.atZone()",
                ));
            }
            let zone = self.check(zone)?;
            if zone.column_type != ColumnType::ZoneId {
                return Err(type_error(format!(
                    "java.time.Instant has no method atZone({})",
                    java_name(&zone.column_type)
                )));
            }
            return Ok(typed(
                ColumnType::ZonedDateTime,
                Node::Call {
                    kernel: Kernel::JavaTime(java_time::TimeFn::AtZone),
                    arguments: vec![receiver, zone],
                },
            ));
        }
        // Behavior: `java.time.Duration#multipliedBy(long)`, which the default
        // `ColumnExpressionValidator` lets no client call (dh-defaults.prop lists the methods
        // of `Instant`, `LocalDate`, `LocalTime` and `ZonedDateTime`, not `Duration`). Seen
        // on Java 42.9: "User expressions are not permitted to use method
        // multipliedBy(long) on class java.time.Duration".
        if let (ColumnType::Duration, "multipliedBy", [scalar]) =
            (&receiver.column_type, method, arguments)
        {
            if !is_null_literal(scalar) {
                let scalar = self.check(scalar)?;
                if matches!(
                    scalar.column_type,
                    ColumnType::Byte
                        | ColumnType::Short
                        | ColumnType::Char
                        | ColumnType::Int
                        | ColumnType::Long
                ) {
                    if self.refused_to_clients.is_none() {
                        self.refused_to_clients = Some(
                            "User expressions are not permitted to use method \
                             multipliedBy(long) on class java.time.Duration"
                                .to_string(),
                        );
                    }
                    let scalar = amount_scalar(false, BinaryOp::Multiply, scalar)?;
                    return Ok(time_call(
                        java_time::TimeFn::DurationTimes,
                        vec![receiver, scalar],
                        ColumnType::Duration,
                    ));
                }
            }
        }
        if receiver.column_type != ColumnType::String {
            return Err(unsupported(format!(
                "method call .{method}() on a value of type {}",
                java_name(&receiver.column_type)
            )));
        }
        let Some(string_method) = StringMethod::named(method) else {
            return Err(if methods::is_string_method(method) {
                unsupported(format!("method call String.{method}()"))
            } else {
                type_error(format!("java.lang.String has no method {method}"))
            });
        };
        let Some(parameters) = string_method
            .signatures()
            .iter()
            .find(|parameters| parameters.len() == arguments.len())
        else {
            let count = arguments.len();
            return Err(if string_method.other_arities().contains(&count) {
                unsupported(format!(
                    "method call String.{method}() with {count} arguments"
                ))
            } else {
                type_error(format!("String.{method}() does not take {count} arguments"))
            });
        };
        let mut checked = Vec::with_capacity(arguments.len());
        for (argument, parameter) in arguments.iter().zip(parameters.iter()) {
            if is_null_literal(argument) {
                return Err(unsupported(format!(
                    "null literal as an argument of String.{method}()"
                )));
            }
            let argument = self.check(argument)?;
            let accepted = match (parameter, &argument.column_type) {
                // `replace(char, char)` beside `replace(CharSequence, CharSequence)`:
                // both arguments are a `char`, or both are text.
                (_, ColumnType::Char)
                    if string_method == StringMethod::Replace
                        && checked
                            .iter()
                            .all(|c: &Typed| c.column_type == ColumnType::Char) =>
                {
                    argument
                }
                (_, ColumnType::String)
                    if string_method == StringMethod::Replace
                        && checked.iter().any(|c| c.column_type == ColumnType::Char) =>
                {
                    return Err(type_error(
                        "String.replace() does not take a char and a String",
                    ))
                }
                (Parameter::Int, ColumnType::Int) | (Parameter::String, ColumnType::String) => {
                    argument
                }
                (Parameter::Int, ColumnType::Byte | ColumnType::Short | ColumnType::Char) => {
                    typed(ColumnType::Int, Node::Cast(Box::new(argument)))
                }
                (_, other) if string_method.has_other_overloads() => {
                    return Err(unsupported(format!(
                        "method call String.{method}() with an argument of type {}",
                        java_name(other)
                    )))
                }
                (_, other) => {
                    return Err(type_error(format!(
                        "String.{method}() does not take an argument of type {}",
                        java_name(other)
                    )))
                }
            };
            checked.push(accepted);
        }
        Ok(typed(
            string_method.result_type(),
            Node::StringMethod {
                method: string_method,
                receiver: Box::new(receiver),
                arguments: checked,
            },
        ))
    }

    /// A method of a vector or array, or an index into one; [`vectors::resolve_method`]
    /// says which exist.
    fn vector_method(
        &mut self,
        receiver: Typed,
        method: &str,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        let mut checked = vec![receiver];
        for argument in arguments {
            if is_null_literal(argument) {
                return Err(unsupported(format!(
                    "null literal as an argument of .{method}()"
                )));
            }
            checked.push(self.check(argument)?);
        }
        let types: Vec<ColumnType> = checked[1..]
            .iter()
            .map(|argument| argument.column_type.clone())
            .collect();
        let column_array = matches!(checked[0].node, Node::ColumnArray(_));
        match vectors::resolve_method(&checked[0].column_type, method, &types, column_array)? {
            Resolved::Call(call) => Ok(library_call(call, checked)),
            _ => unreachable!("a method of a vector is a call"),
        }
    }

    /// `new` and what follows it, as [`crate::parse`] names it: `Integer` with the
    /// constructor's arguments, `int[]` with the elements, or `int[n]` with the length.
    ///
    /// Behavior: `QueryLanguageParser#visit(ObjectCreationExpr)` chooses the constructor
    /// as `#visit(MethodCallExpr)` chooses a static method, and converts the arguments
    /// the same way; a formula's value of a boxed type is a column of the primitive type.
    /// `#visit(ArrayCreationExpr)` prints the creation with each element as the parser
    /// has it, and javac converts the elements by assignment: plain widening, which does
    /// not know null, and an `int` literal to a narrower type that holds it. Java 42.9
    /// was run for each form.
    fn creation(&mut self, created: &str, arguments: &[Expr]) -> Result<Typed, FormulaError> {
        if let Some(at) = created.find("[n]") {
            let (name, dimensions) = created.split_at(at);
            let open = dimensions.matches("[]").count();
            return self.sized_array(name, open, arguments);
        }
        if let Some(name @ ("Object" | "java.lang.Object")) = created.strip_suffix("[]") {
            return self.object_array(name, arguments);
        }
        if let Some(name) = created.strip_suffix("[]") {
            let element = array_element(name)?;
            if arguments.is_empty() {
                return Err(type_error(format!(
                    "new {name}[] {{}}: Java cannot tell the type of an array without elements"
                )));
            }
            let elements = arguments
                .iter()
                .map(|argument| self.array_element(name, &element, argument))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(array_of(element, elements));
        }
        let class = created.strip_prefix("java.lang.").unwrap_or(created);
        match (class, arguments) {
            ("String", []) => Ok(typed(
                ColumnType::String,
                Node::Literal(Literal::String(String::new())),
            )),
            // `new Byte(byte)` and `new Short(short)` take their own type only, and the
            // formula's value is the argument.
            ("Byte" | "Short", [argument]) if !is_null_literal(argument) => {
                let argument = self.check(argument)?;
                let own = if class == "Byte" {
                    ColumnType::Byte
                } else {
                    ColumnType::Short
                };
                if argument.column_type == own {
                    Ok(argument)
                } else {
                    Err(unsupported(format!(
                        "constructor new {class}() with these arguments"
                    )))
                }
            }
            ("Boolean" | "Character" | "Double" | "Float" | "Integer" | "Long" | "String", _) => {
                self.static_call(class, functions::statics::CONSTRUCTOR, arguments)
            }
            _ if created == range::CLASS => self.range_call(RangeFn::New, None, arguments),
            // no column type holds the object: see `functions::random`
            _ if random::is_class(created) => Err(unsupported(format!(
                "a {class} that is not asked at once for nextInt(bound): \
                 not ported: {class}#Random",
                class = random::CLASS
            ))),
            _ => Err(unsupported(format!("constructor new {created}()"))),
        }
    }

    /// `new name[a][b]...[]...`: an array of `lengths.len()` dimensions with a length each
    /// and `open` more without, of the element `name`.
    ///
    /// Behavior: `QueryLanguageParser#visit(ArrayCreationExpr)`, which prints the creation
    /// for javac: the innermost arrays are filled with zeros, or with nulls where the
    /// elements are objects, and a dimension without a length holds null arrays. Seen on
    /// Java 42.9: `new int[4][2]` is four `int[]` of two zeros, `new long[2][]` two null
    /// `long[]`, `new String[2][3]` two `String[]` of three nulls, `new int[2][][]` two
    /// null `int[][]`. Here the lengths are literals and the arrays hold at most 1024
    /// elements in all.
    fn sized_array(
        &mut self,
        name: &str,
        open: usize,
        lengths: &[Expr],
    ) -> Result<Typed, FormulaError> {
        let element = array_element(name)?;
        let spelled = format!(
            "new {name}{}{}",
            "[n]".repeat(lengths.len()),
            "[]".repeat(open)
        );
        let refused = || {
            unsupported(format!(
                "array creation {spelled} with lengths that are not literals from 1 to 1024 \
                 or more than 1024 elements in all"
            ))
        };
        let mut sizes = Vec::with_capacity(lengths.len());
        for length in lengths {
            match length {
                Expr::Literal(Literal::Int(length)) if (1..=1024).contains(length) => {
                    sizes.push(*length as usize)
                }
                _ => return Err(refused()),
            }
        }
        if sizes.iter().product::<usize>() > 1024 {
            return Err(refused());
        }
        let mut filler = if open > 0 {
            let mut inner = element;
            for _ in 0..open {
                inner = ColumnType::array(inner);
            }
            typed(inner, Node::Null)
        } else {
            match element {
                ColumnType::String | ColumnType::Instant => typed(element.clone(), Node::Null),
                _ => convert(
                    typed(ColumnType::Int, Node::Literal(Literal::Int(0))),
                    &element,
                ),
            }
        };
        for size in sizes.into_iter().rev() {
            filler = array_of(filler.column_type.clone(), vec![filler; size]);
        }
        Ok(filler)
    }

    /// `new String(array)` and `new String(array, offset, count)` of a `byte[]`, `char[]`
    /// or `int[]`; `None` for other arguments, which [`Self::creation`] answers.
    ///
    /// Behavior: `java.lang.String#String(byte[])`, `#String(char[])`,
    /// `#String(byte[], int, int)`, `#String(char[], int, int)` and
    /// `#String(int[], int, int)`, chosen as `QueryLanguageParser#visit(ObjectCreationExpr)`
    /// chooses a constructor: the offset and the count widen to `int`. Seen on Java 42.9:
    /// a `short` and a `byte` are taken, a `long` does not compile.
    fn new_string(&mut self, arguments: &[Expr]) -> Result<Option<Typed>, FormulaError> {
        use functions::StringSource;
        if !matches!(arguments.len(), 1 | 3) || arguments.iter().any(is_null_literal) {
            return Ok(None);
        }
        let array = self.check(&arguments[0])?;
        let source = match &array.column_type {
            ColumnType::Array(element) => match element.as_ref() {
                ColumnType::Byte => StringSource::Bytes,
                ColumnType::Char => StringSource::Chars,
                ColumnType::Int => StringSource::CodePoints,
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let mut checked = vec![array];
        for argument in &arguments[1..] {
            let argument = self.check(argument)?;
            if !matches!(
                argument.column_type,
                ColumnType::Byte | ColumnType::Short | ColumnType::Char | ColumnType::Int
            ) {
                return Err(type_error(format!(
                    "Cannot find constructor for java.lang.String with a {} offset or count",
                    java_name(&argument.column_type)
                )));
            }
            checked.push(convert(argument, &ColumnType::Int));
        }
        let text = match (source, checked.len()) {
            (StringSource::Bytes, 1) => Node::NewString(Box::new(checked.remove(0))),
            (StringSource::CodePoints, 1) => {
                return Err(type_error(
                    "Cannot find constructor for java.lang.String(int[])",
                ))
            }
            _ => Node::Call {
                kernel: Kernel::NewString(source),
                arguments: checked,
            },
        };
        Ok(Some(typed(ColumnType::String, text)))
    }

    /// `new Random(seed).method(arguments)`. Of the constructors Java takes the one of a
    /// `long`, for a seed that widens to it, and of the methods `nextInt(int)` is
    /// computed, for a bound that widens to `int`; each is converted as a cast converts
    /// it, so a null `int` seed is the null `long`.
    ///
    /// Behavior: `QueryLanguageParser#visit(ObjectCreationExpr)`, `#visit(MethodCallExpr)`
    /// and `#convertParameters`. Java 42.9 was run for each form.
    fn random_call(
        &mut self,
        seed: &[Expr],
        method: &str,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        use ColumnType as C;
        let class = random::CLASS;
        let mut one =
            |arguments: &[Expr], long: bool| -> Result<Result<Typed, String>, FormulaError> {
                let mut found = Vec::with_capacity(arguments.len());
                let mut checked = Vec::with_capacity(arguments.len());
                for argument in arguments {
                    if is_null_literal(argument) {
                        found.push("null".to_string());
                    } else {
                        let argument = self.check(argument)?;
                        found.push(java_name(&argument.column_type));
                        checked.push(argument);
                    }
                }
                if let (1, [argument]) = (arguments.len(), checked.as_slice()) {
                    let from = &argument.column_type;
                    if matches!(from, C::Byte | C::Short | C::Char | C::Int)
                        || (long && *from == C::Long)
                    {
                        return Ok(Ok(checked.remove(0)));
                    }
                }
                Ok(Err(found.join(", ")))
            };
        if seed.is_empty() {
            // the seed is the time and a counter
            return Err(unsupported(format!(
                "a {class} without a seed: not ported: {class}#Random()"
            )));
        }
        let seed = one(seed, true)?.map_err(|found| {
            type_error(format!(
                "Cannot find constructor for {class}({found}) in class {class}"
            ))
        })?;
        if method != "nextInt" || arguments.is_empty() {
            return Err(unsupported(format!(
                "method {method} of a {class}: not ported: {class}#{method}"
            )));
        }
        let bound = match one(arguments, false)? {
            Ok(bound) => bound,
            Err(found) if arguments.len() == 2 => {
                return Err(unsupported(format!(
                    "method nextInt({found}) of a {class}: not ported: {class}#nextInt(int, int)"
                )))
            }
            Err(found) => {
                return Err(type_error(format!(
                    "Cannot find method nextInt({found}) in class {class}"
                )))
            }
        };
        if self.refused_to_clients.is_none() {
            self.refused_to_clients = Some(random::REFUSAL.to_string());
        }
        Ok(typed(
            C::Int,
            Node::Call {
                kernel: Kernel::RandomNextInt,
                arguments: vec![convert(seed, &C::Long), convert(bound, &C::Int)],
            },
        ))
    }

    /// The constructor of `DiscretizedRangeEqual`, or a method of it called on `receiver`.
    /// The class has one constructor and one method of each name, which takes the
    /// arguments that widen to its parameters; each is converted as a cast converts it,
    /// so a null stays the null of the parameter's type.
    ///
    /// Behavior: `QueryLanguageParser#visit(ObjectCreationExpr)`, `#visit(MethodCallExpr)`,
    /// `#dhqlIsAssignableFrom` and `#convertParameters`. Java 42.9 was run for each form.
    fn range_call(
        &mut self,
        function: RangeFn,
        receiver: Option<Typed>,
        arguments: &[Expr],
    ) -> Result<Typed, FormulaError> {
        use ColumnType as C;
        let parameters = function.parameters();
        let mut checked = Vec::with_capacity(arguments.len() + 1);
        let mut found = Vec::with_capacity(arguments.len());
        let mut fits = arguments.len() == parameters.len();
        for (at, argument) in arguments.iter().enumerate() {
            if is_null_literal(argument) {
                found.push("null".to_string());
                fits = false;
                continue;
            }
            let argument = self.check(argument)?;
            found.push(java_name(&argument.column_type));
            let from = &argument.column_type;
            fits &= match parameters.get(at) {
                Some(C::Int) => matches!(from, C::Byte | C::Short | C::Char | C::Int),
                Some(C::Long) => matches!(from, C::Byte | C::Short | C::Char | C::Int | C::Long),
                Some(C::Double) => Numeric::of(from).is_some(),
                _ => false,
            };
            checked.push(argument);
        }
        if !fits {
            let found = found.join(", ");
            return Err(type_error(if function == RangeFn::New {
                format!(
                    "Cannot find constructor for {class}({found}) in class {class}",
                    class = range::CLASS
                )
            } else {
                format!(
                    "Cannot find method {}({found}) in class {}",
                    function.name(),
                    range::CLASS
                )
            }));
        }
        if self.refused_to_clients.is_none() {
            self.refused_to_clients = Some(function.refusal());
        }
        let mut converted: Vec<Typed> = receiver.into_iter().collect();
        converted.extend(
            checked
                .into_iter()
                .zip(parameters)
                .map(|(argument, parameter)| convert(argument, parameter)),
        );
        Ok(typed(
            function.result_type(),
            Node::Call {
                kernel: Kernel::Range(function),
                arguments: converted,
            },
        ))
    }

    /// One element of `new name[] { ... }`, converted to the element type as javac
    /// converts it.
    fn array_element(
        &mut self,
        name: &str,
        element: &ColumnType,
        argument: &Expr,
    ) -> Result<Typed, FormulaError> {
        use ColumnType as C;
        let of_objects = matches!(element, C::String | C::Instant);
        if is_null_literal(argument) {
            return if of_objects {
                Ok(typed(element.clone(), Node::Null))
            } else {
                Err(type_error(format!(
                    "incompatible types: null cannot be an element of {name}[]"
                )))
            };
        }
        let checked = self.check(argument)?;
        let from = &checked.column_type;
        if from == element {
            return Ok(checked);
        }
        let widens = match element {
            C::Short => *from == C::Byte,
            C::Int => matches!(from, C::Byte | C::Short | C::Char),
            C::Long => matches!(from, C::Byte | C::Short | C::Char | C::Int),
            C::Float => matches!(from, C::Byte | C::Short | C::Char | C::Int | C::Long),
            C::Double => matches!(
                from,
                C::Byte | C::Short | C::Char | C::Int | C::Long | C::Float
            ),
            _ => false,
        };
        if widens {
            return Ok(typed(element.clone(), Node::Widen(Box::new(checked))));
        }
        let holds = |value: i32| match element {
            C::Byte => i8::try_from(value).is_ok(),
            C::Short => i16::try_from(value).is_ok(),
            C::Char => u16::try_from(value).is_ok(),
            _ => false,
        };
        match argument {
            Expr::Literal(Literal::Int(value)) if holds(*value) => Ok(convert(checked, element)),
            _ => Err(type_error(format!(
                "incompatible types: {} cannot be an element of {name}[]",
                java_name(from)
            ))),
        }
    }

    /// `new Object[]{...}`: an array whose elements are objects of one class, a primitive
    /// boxed as the value it holds. Elements of two classes are refused, as an `Object`
    /// column of two classes is.
    ///
    /// Behavior: `QueryLanguageParser#visit(ArrayCreationExpr)`, which leaves the
    /// initializer to javac
    fn object_array(&mut self, name: &str, arguments: &[Expr]) -> Result<Typed, FormulaError> {
        if arguments.is_empty() {
            return Err(type_error(format!(
                "new {name}[] {{}}: Java cannot tell the type of an array without elements"
            )));
        }
        let checked = arguments
            .iter()
            .map(|argument| self.check(argument))
            .collect::<Result<Vec<_>, _>>()?;
        let mut class: Option<ColumnType> = None;
        for element in &checked {
            let Some(own) = crate::eval::class_of(&element.column_type) else {
                continue;
            };
            match &class {
                Some(first) if *first != own => {
                    return Err(unsupported(format!(
                        "new {name}[] of a {} and a {}, an array of objects of two classes",
                        java_name(first),
                        java_name(&own)
                    )))
                }
                _ => class = Some(own),
            }
        }
        let element = ColumnType::object(class);
        let elements = checked
            .into_iter()
            .map(|value| match (&value.column_type, &value.node) {
                (column_type, _) if *column_type == element => Ok(value),
                (ColumnType::Object(None), Node::Null) => Ok(typed(element.clone(), Node::Null)),
                (ColumnType::Object(_), _) => Err(unsupported(format!(
                    "new {name}[] with an element of nulls that is not the null literal"
                ))),
                _ => boxed(value),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(array_of(element, elements))
    }

    /// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` for a call without a scope:
    /// a static method of the default imports. [`functions::resolve`] chooses the overload.
    fn call(&mut self, function: &str, arguments: &[Expr]) -> Result<Typed, FormulaError> {
        self.call_of(function, arguments, true)
    }

    /// A call of a default import through its class, which no function of the embedder
    /// can stand in for.
    fn import_call(&mut self, function: &str, arguments: &[Expr]) -> Result<Typed, FormulaError> {
        self.call_of(function, arguments, false)
    }

    fn call_of(
        &mut self,
        function: &str,
        arguments: &[Expr],
        externals: bool,
    ) -> Result<Typed, FormulaError> {
        if let Some(created) = function.strip_prefix(crate::parse::NEW) {
            if function == crate::parse::NEW_STRING {
                if let Some(text) = self.new_string(arguments)? {
                    return Ok(text);
                }
            }
            return self.creation(created, arguments);
        }
        // Behavior: `DateTimeUtils#plus(Instant, Duration)`, `#plus(Instant, Period)` and
        // the two `minus`, called by name as the operators call them.
        if let ("plus" | "minus", [left, right]) = (function, arguments) {
            let op = if function == "plus" {
                BinaryOp::Add
            } else {
                BinaryOp::Subtract
            };
            if let Some(result) = self.with_time_amount(op, left, right)? {
                return Ok(result);
            }
        }
        if function == "dayOfWeek" {
            let field = functions::ZonedField::DayOfWeekValue;
            if let Some(number) = self.zoned(function, field, arguments)? {
                return Ok(typed(
                    ColumnType::DayOfWeek,
                    Node::DayOfWeek {
                        number: Box::new(number),
                        method: false,
                    },
                ));
            }
        }
        if let Some(field) = functions::ZonedField::named(function) {
            if let Some(result) = self.zoned(function, field, arguments)? {
                return Ok(result);
            }
        }
        if let ("lowerBin" | "upperBin", [instant, amounts @ ..]) = (function, arguments) {
            if let Some(result) = self.bin_by_duration(function, instant, amounts)? {
                return Ok(result);
            }
        }
        if function == "isNull" {
            let [argument] = arguments else {
                return Err(type_error(format!(
                    "isNull takes one argument, not {}",
                    arguments.len()
                )));
            };
            // `Basic#isNull(Object)`.
            if is_null_literal(argument) {
                return Ok(typed(
                    ColumnType::Boolean,
                    Node::Literal(Literal::Boolean(true)),
                ));
            }
            let argument = match self.object_column(argument) {
                Some(column) => column,
                None => self.check(argument)?,
            };
            return Ok(typed(ColumnType::Boolean, Node::IsNull(Box::new(argument))));
        }
        if let Some(result) = self.style_call(function, arguments)? {
            return Ok(result);
        }
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            checked.push(if is_null_literal(argument) {
                None
            } else {
                Some(self.check(argument)?)
            });
        }
        let types: Vec<Option<ColumnType>> = checked
            .iter()
            .map(|argument| argument.as_ref().map(|typed| typed.column_type.clone()))
            .collect();
        let resolved = match functions::resolve(function, &types) {
            Ok(resolved) => resolved,
            // Behavior: `QueryLanguageParser#getMethod`: when no static import takes the
            // call, a variable of that name that can be called does. A column of the
            // name is a variable that cannot.
            Err(FormulaError::Type(message)) => {
                let external = match self.functions.filter(|_| externals) {
                    Some(functions) if self.definition.index_of(function).is_none() => {
                        let described: Vec<ExternalArgument<'_>> = checked
                            .iter()
                            .zip(arguments)
                            .map(|(checked, argument)| ExternalArgument {
                                column_type: checked.as_ref().map(|typed| &typed.column_type),
                                literal: match argument {
                                    Expr::Literal(literal) => Some(literal),
                                    _ => None,
                                },
                            })
                            .collect();
                        functions.resolve(function, &described)
                    }
                    _ => None,
                };
                let Some(external) = external else {
                    return Err(FormulaError::Type(message));
                };
                let external = external?;
                return Ok(typed(
                    external.result_type().clone(),
                    Node::External {
                        function: crate::external::Bound(external),
                        arguments: checked,
                    },
                ));
            }
            Err(other) => {
                // Behavior: a method declared with a variable number of arguments, such
                // as `Numeric#median(double...)`, is called with an array of them. The
                // arguments here all have the element type; Java converts others.
                const VARIABLE: [&str; 11] = [
                    "absAvg",
                    "avg",
                    "count",
                    "countDistinct",
                    "first",
                    "last",
                    "median",
                    "product",
                    "std",
                    "sum",
                    "var",
                ];
                let element = match types.as_slice() {
                    [Some(first), rest @ ..]
                        if VARIABLE.contains(&function)
                            && Numeric::of(first).is_some()
                            && rest.iter().all(|other| other.as_ref() == Some(first)) =>
                    {
                        first.clone()
                    }
                    _ => return Err(other),
                };
                let array_type = Some(ColumnType::Array(Box::new(element.clone())));
                let Ok(Resolved::Call(call)) = functions::resolve(function, &[array_type]) else {
                    return Err(other);
                };
                let elements = checked.into_iter().flatten().collect();
                return Ok(library_call(call, vec![array_of(element, elements)]));
            }
        };
        match resolved {
            Resolved::Call(call) => {
                let arguments = checked.into_iter().flatten().collect();
                Ok(library_call(call, arguments))
            }
            Resolved::Binary(op) => self.binary(op, &arguments[0], &arguments[1]),
            Resolved::Unary(op) => self.unary(op, &arguments[0]),
            Resolved::NegateAsInt => {
                let operand = checked.into_iter().flatten().next().expect("one argument");
                let operand = convert(operand, &ColumnType::Int);
                Ok(typed(ColumnType::Int, Node::Negate(Box::new(operand))))
            }
            // `intCast(Object)` and its relatives fail on a value that is not null.
            Resolved::Cast(_) if types[0] == Some(ColumnType::Boolean) => {
                Err(unsupported(format!("function {function}() of a Boolean")))
            }
            // `QueryLanguageFunctionUtils` has no `longCast(long)`: the call reaches
            // `longCast(float)`, as recorded (`longCast(16777217L)` is 16777216).
            Resolved::Cast(ColumnType::Long) if types[0] == Some(ColumnType::Long) => {
                let operand = checked.into_iter().flatten().next().expect("one argument");
                Ok(convert(
                    convert(operand, &ColumnType::Float),
                    &ColumnType::Long,
                ))
            }
            Resolved::Cast(to) => self.cast(&to, &arguments[0]),
        }
    }
}

/// The element type of the array `new name[]` creates. `boolean[]` and the arrays of boxed
/// values and of other objects are types no column has.
fn array_element(name: &str) -> Result<ColumnType, FormulaError> {
    Ok(match name {
        "byte" => ColumnType::Byte,
        "char" => ColumnType::Char,
        "short" => ColumnType::Short,
        "int" => ColumnType::Int,
        "long" => ColumnType::Long,
        "float" => ColumnType::Float,
        "double" => ColumnType::Double,
        "String" | "java.lang.String" => ColumnType::String,
        "Instant" | "java.time.Instant" => ColumnType::Instant,
        // Java makes a `java.lang.Integer[]` of nulls (seen on Java 42.9), which no
        // column type is: `Array(Int)` is `int[]`.
        other
            if matches!(
                other.strip_prefix("java.lang.").unwrap_or(other),
                "Boolean"
                    | "Byte"
                    | "Character"
                    | "Short"
                    | "Integer"
                    | "Long"
                    | "Float"
                    | "Double"
            ) =>
        {
            return Err(unsupported(format!(
                "array creation new {other}[]: no column type is an array of a boxed java.lang \
                 class"
            )))
        }
        other => return Err(unsupported(format!("array creation new {other}[]"))),
    })
}

/// The array of `elements`, which have the type `element`.
fn array_of(element: ColumnType, elements: Vec<Typed>) -> Typed {
    typed(
        ColumnType::Array(Box::new(element)),
        Node::Call {
            kernel: Kernel::ArrayOf,
            arguments: elements,
        },
    )
}

/// `operand` converted to `to` as a cast converts it.
fn convert(operand: Typed, to: &ColumnType) -> Typed {
    if operand.column_type == *to {
        operand
    } else {
        typed(to.clone(), Node::Cast(Box::new(operand)))
    }
}

/// The call `call` of `arguments`, each converted to its parameter's type.
fn library_call(call: functions::Call, arguments: Vec<Typed>) -> Typed {
    // Java widens the argument of a random function as it stands, null or not.
    let widened = matches!(call.kernel, Kernel::Random(_));
    let mut arguments: Vec<Typed> = arguments
        .into_iter()
        .zip(&call.parameters)
        .map(|(argument, parameter)| {
            if widened && argument.column_type != *parameter {
                typed(parameter.clone(), Node::Widen(Box::new(argument)))
            } else {
                convert(argument, parameter)
            }
        })
        .collect();
    if call.exchanged {
        arguments.reverse();
    }
    typed(
        call.result,
        Node::Call {
            kernel: call.kernel,
            arguments,
        },
    )
}

/// A default-imported constant or a static field. A `byte` or `short` one is an `int`
/// literal converted.
fn constant(column_type: ColumnType, literal: Literal) -> Typed {
    match (&column_type, &literal) {
        (_, Literal::Null) => typed(column_type, Node::Null),
        (ColumnType::Byte | ColumnType::Short, _) => {
            let int = typed(ColumnType::Int, Node::Literal(literal));
            typed(column_type, Node::Cast(Box::new(int)))
        }
        _ => typed(column_type, Node::Literal(literal)),
    }
}

fn operands_error(op: BinaryOp, left: &Typed, right: &Typed) -> FormulaError {
    type_error(format!(
        "cannot apply {} to {} and {}",
        symbol(op),
        java_name(&left.column_type),
        java_name(&right.column_type)
    ))
}

/// An operator with a `java.time` value that is no number on one side: a `LocalDate`,
/// `LocalTime`, `Duration`, `Period`, `ZoneId` or `ZonedDateTime`.
///
/// Behavior: `QueryLanguageParser#visit(BinaryExpr)`: `+` beside a `String` is string
/// concatenation, which prints the value with `toString`; `==` and `!=` are
/// `QueryLanguageFunctionUtils#eq(Object, Object)`, which is `equals`; `<` and its
/// relatives are `#less(Comparable, Comparable)` and its relatives, null first. Seen on
/// Java 42.9: `'2020-01-02' == '2020-01-02'` is true, `'2020-01-02' < '2020-01-03'` is
/// true, and `` `a` + '2020-01-02' `` is `a2020-01-02`. Here: concatenation of a date,
/// a time of day and a zone, comparison of two dates, two times of day or two durations,
/// `==` and `!=` of two zones, the sums and differences of `time_arithmetic`, and a
/// duration or period multiplied (a duration also divided) by a whole number; every other
/// operator with these values is refused.
fn time_object_operator(
    op: BinaryOp,
    family: Family,
    left: Typed,
    right: Typed,
) -> Result<Typed, FormulaError> {
    let printable = |t: &ColumnType| {
        matches!(
            t,
            ColumnType::LocalDate | ColumnType::LocalTime | ColumnType::ZoneId
        )
    };
    if let Family::Arithmetic(_) = family {
        if let Some(found) = time_arithmetic(op, &left, &right)? {
            let (function, result) = found;
            return Ok(time_call(function, vec![left, right], result));
        }
        if matches!(op, BinaryOp::Multiply | BinaryOp::Divide) {
            let (amount, scalar, swap) = if java_time::is_time_object(&left.column_type) {
                (left, right, false)
            } else {
                (right, left, true)
            };
            let period = amount.column_type == ColumnType::Period;
            // There is no `divide(long, Duration)`.
            if (!swap || op == BinaryOp::Multiply)
                && matches!(
                    amount.column_type,
                    ColumnType::Duration | ColumnType::Period
                )
                && Numeric::of(&scalar.column_type).is_some()
            {
                let scalar = amount_scalar(period, op, scalar)?;
                let function = match (period, op) {
                    (true, _) => java_time::TimeFn::PeriodTimes,
                    (false, BinaryOp::Multiply) => java_time::TimeFn::DurationTimes,
                    _ => java_time::TimeFn::DurationDivided,
                };
                let result = amount.column_type.clone();
                return Ok(time_call(function, vec![amount, scalar], result));
            }
            let (left, right) = if swap {
                (scalar, amount)
            } else {
                (amount, scalar)
            };
            return Err(unsupported(format!(
                "operator {} on values of type {} and {}",
                symbol(op),
                java_name(&left.column_type),
                java_name(&right.column_type)
            )));
        }
    }
    match family {
        Family::Arithmetic(ArithmeticOp::Add)
            if (left.column_type == ColumnType::String && printable(&right.column_type))
                || (right.column_type == ColumnType::String && printable(&left.column_type)) =>
        {
            Ok(concat(left, right))
        }
        Family::Compare(compare) => time_comparison(compare, left, right),
        _ => Err(unsupported(format!(
            "operator {} on values of type {} and {}",
            symbol(op),
            java_name(&left.column_type),
            java_name(&right.column_type)
        ))),
    }
}

/// A comparison with a `java.time` value that is no number on at least one side.
///
/// Behavior: `QueryLanguageFunctionUtils#eq(Object, Object)` and
/// `#compareTo(Comparable, Comparable)`, as [`comparison`] describes them: `==` of one
/// class is `equals`, of two classes true only for two nulls; `<` and its relatives of
/// two `Comparable` classes fail where two values meet. `Period` and `ZoneId` are no
/// `Comparable`, and javac finds no `less` for them. Seen on Java 42.9 (a `LocalDate` `D`,
/// a `Duration` `Dur`, a `Period` `P`): `D == 2024-03-10`, which compares with the `int`
/// 2011, and ``D == `2024-03-10` `` keep no row; `D == S` of a `String` keeps the rows
/// where both are null; `Dur < null` keeps none and `Dur > null` those not null;
/// `P == 'P1Y'` keeps the `P1Y` rows and `P == 'P12M'` none; `D < S` fails once both are
/// not null; `P < null`, `P < P` and `Dur < P` do not compile.
fn time_comparison(op: CompareOp, left: Typed, right: Typed) -> Result<Typed, FormulaError> {
    let (l, r) = (&left.column_type, &right.column_type);
    let domain = match (l == r, l) {
        (true, ColumnType::LocalDate | ColumnType::LocalTime | ColumnType::Duration) => {
            CompareDomain::Integral
        }
        (true, ColumnType::ZoneId) if op.is_equality() => CompareDomain::String,
        (true, ColumnType::Period | ColumnType::ZonedDateTime) if op.is_equality() => {
            CompareDomain::Fields
        }
        _ if !op.is_equality() && !(comparable(l) && comparable(r)) => {
            return Err(type_error(format!(
                "no ordering comparison ({op:?}) of {} and {}: neither is less(Comparable, Comparable)",
                java_name(l),
                java_name(r)
            )));
        }
        (true, _) => {
            return Err(unsupported(format!(
                "ordering comparison of two values of type {}: {}#compareTo",
                java_name(l),
                java_name(l)
            )));
        }
        (false, _) => return comparison(op, left, right),
    };
    Ok(typed(
        ColumnType::Boolean,
        Node::Compare {
            op,
            domain,
            left: Box::new(left),
            right: Box::new(right),
        },
    ))
}

/// `left + right` or `left - right` of an instant, a date, a duration or a period and a
/// duration or period: the function and its result type. `None` for any other pair.
///
/// Behavior: the `plus` and `minus` overloads of `DateTimeUtils` of `(Instant, Duration)`,
/// `(Instant, Period)`, `(LocalDate, Period)`, `(Duration, Duration)` and
/// `(Period, Period)`, which `QueryLanguageParser#visit(BinaryExpr)` reaches by the
/// operator's name. None takes the duration or period first beside an instant or a date,
/// and Java 42.9 failed `parseDuration(`PT1H`) + '2020-01-01T00:00:00Z'` to match.
fn time_arithmetic(
    op: BinaryOp,
    left: &Typed,
    right: &Typed,
) -> Result<Option<(java_time::TimeFn, ColumnType)>, FormulaError> {
    use java_time::TimeFn as F;
    use ColumnType as T;
    let subtract = match op {
        BinaryOp::Add => false,
        BinaryOp::Subtract => true,
        _ => return Ok(None),
    };
    let found = match (&left.column_type, &right.column_type) {
        (T::Instant, T::Duration) => (F::InstantAndDuration { subtract }, T::Instant),
        (T::Instant, T::Period) => (F::InstantAndPeriod { subtract }, T::Instant),
        (T::LocalDate, T::Period) => (F::LocalDateAndPeriod { subtract }, T::LocalDate),
        (T::Duration, T::Duration) => (F::DurationSum { subtract }, T::Duration),
        (T::Period, T::Period) => (F::PeriodSum { subtract }, T::Period),
        (T::Duration | T::Period, T::Instant | T::LocalDate) => {
            return Err(type_error(format!(
                "Cannot find method {}({}, {})",
                if subtract { "minus" } else { "plus" },
                java_name(&left.column_type),
                java_name(&right.column_type)
            )))
        }
        _ => return Ok(None),
    };
    Ok(Some(found))
}

fn time_call(function: java_time::TimeFn, arguments: Vec<Typed>, result: ColumnType) -> Typed {
    typed(
        result,
        Node::Call {
            kernel: Kernel::JavaTime(function),
            arguments,
        },
    )
}

/// The number a duration or period is multiplied or divided by, as the `long` the kernels
/// read.
///
/// Behavior: `DateTimeUtils#multiply(Duration, long)`, `#multiply(Period, int)` and
/// `#divide(Duration, long)`: Java widens a narrower integer to the parameter, the value
/// that stands for null with it, and has no overload of a `float`, a `double`, a `long`
/// beside a period, or a period divided.
fn amount_scalar(period: bool, op: BinaryOp, scalar: Typed) -> Result<Typed, FormulaError> {
    let class = if period { "Period" } else { "Duration" };
    let method = if op == BinaryOp::Multiply {
        "multiply"
    } else {
        "divide"
    };
    let parameter = if period {
        ColumnType::Int
    } else {
        ColumnType::Long
    };
    let narrow = matches!(
        scalar.column_type,
        ColumnType::Byte | ColumnType::Short | ColumnType::Char | ColumnType::Int
    );
    let taken = narrow || (!period && scalar.column_type == ColumnType::Long);
    if !taken || (period && op == BinaryOp::Divide) {
        return Err(type_error(format!(
            "Cannot find method {method}(java.time.{class}, {})",
            java_name(&scalar.column_type)
        )));
    }
    let scalar = if scalar.column_type == parameter {
        scalar
    } else {
        typed(parameter, Node::Widen(Box::new(scalar)))
    };
    Ok(convert(scalar, &ColumnType::Long))
}

fn concat(left: Typed, right: Typed) -> Typed {
    typed(
        ColumnType::String,
        Node::Concat {
            left: Box::new(left),
            right: Box::new(right),
        },
    )
}

/// Behavior: `QueryLanguageFunctionUtils#binaryAnd`, `#binaryOr` and `#xor`. The overloads
/// take `(int, int)`, `(long, long)`, two operands of one narrower integer type, or
/// `(Boolean, Boolean)`; `QueryLanguageParser#getMethod` lets a narrower integer stand in for
/// an `int` or `long` parameter, and nothing stand in for a `float` or `double`, which have no
/// overload.
fn bitwise_operator(
    spelled: BinaryOp,
    op: BitwiseOp,
    left: Typed,
    right: Typed,
) -> Result<Typed, FormulaError> {
    let (node_type, node) = match (&left.column_type, &right.column_type) {
        (ColumnType::Boolean, ColumnType::Boolean) => (
            ColumnType::Boolean,
            Node::BooleanBitwise {
                op,
                left: Box::new(left),
                right: Box::new(right),
            },
        ),
        (l, r) => {
            let integral = Numeric::of(l)
                .zip(Numeric::of(r))
                .map(|(l, r)| l.max(r))
                .filter(|widest| *widest <= Numeric::Long);
            let Some(widest) = integral else {
                return Err(operands_error(spelled, &left, &right));
            };
            (
                widest.column_type(),
                Node::Arithmetic {
                    op: ArithmeticOp::Bitwise(op),
                    left: Box::new(left),
                    right: Box::new(right),
                },
            )
        }
    };
    Ok(typed(node_type, node))
}

/// Behavior: `QueryLanguageFunctionUtils#eq`, `#less` and their relatives, as
/// `QueryLanguageParser#getMethod` resolves them. Numbers and chars compare as
/// [`numeric_compare_domain`] says. Two values of one other type go to `eq(Object, Object)`,
/// which is `equals`, and to `compareTo(Comparable, Comparable)`. Two values of unrelated
/// types also go to `eq(Object, Object)`: a primitive is boxed and never equals a value of
/// another class, so they are equal only when both are null. An ordering comparison of
/// two `Comparable` types is `compareTo(Comparable, Comparable)`, which orders null first
/// and throws `ClassCastException` when two values that are not null meet. Seen on Java
/// 42.9: `S < I` of a `String` and an `int` fails where `S` is not null, and is true
/// where `S` is null; `B < I` of a `Boolean` and `S < T` of a `String` and an `Instant`
/// fail likewise.
fn comparison(op: CompareOp, left: Typed, right: Typed) -> Result<Typed, FormulaError> {
    let numeric = Numeric::of(&left.column_type).zip(Numeric::of(&right.column_type));
    let primitive = [
        crate::eval::is_primitive(&left.column_type),
        crate::eval::is_primitive(&right.column_type),
    ];
    let domain = match (numeric, &left.column_type, &right.column_type) {
        (Some((l, r)), _, _) => numeric_compare_domain(op, l, r),
        (None, ColumnType::String, ColumnType::String) => CompareDomain::String,
        (None, ColumnType::Instant, ColumnType::Instant) => CompareDomain::Integral,
        (None, ColumnType::Boolean, ColumnType::Boolean) => CompareDomain::Boolean,
        (None, _, _) if op.is_equality() => CompareDomain::Unrelated { primitive },
        (None, l, r) if l != r && comparable(l) && comparable(r) => {
            CompareDomain::UnrelatedOrder { primitive }
        }
        (None, l, r) => {
            return Err(unsupported(format!(
                "ordering comparison of {} with {}",
                java_name(l),
                java_name(r)
            )));
        }
    };
    Ok(typed(
        ColumnType::Boolean,
        Node::Compare {
            op,
            domain,
            left: Box::new(left),
            right: Box::new(right),
        },
    ))
}

/// Whether a value of `column_type` is a `java.lang.Comparable` that this checker orders
/// against a value of another class: a primitive (boxed), `Boolean`, `String`, `Instant`
/// and the `java.time` classes that implement it. `Period` and `ZoneId` do not, and Java
/// finds no `less` for them.
fn comparable(column_type: &ColumnType) -> bool {
    crate::eval::is_primitive(column_type)
        || matches!(
            column_type,
            ColumnType::Boolean
                | ColumnType::String
                | ColumnType::Instant
                | ColumnType::LocalDate
                | ColumnType::LocalTime
                | ColumnType::Duration
                | ColumnType::ZonedDateTime
        )
}

/// Whether `expr` makes a `DiscretizedRangeEqual`.
fn makes_range(expr: &Expr) -> bool {
    matches!(expr, Expr::Call { function, .. }
        if function
            .strip_prefix(crate::parse::NEW)
            .is_some_and(|class| class == range::CLASS || range::CLASS.ends_with(&format!(".{class}"))))
}

/// Whether a value of `column_type` is an object this checker computes with only as one:
/// the cells of an `Object` column and a `java.time.DayOfWeek`.
///
/// A cell of a column of tables is one too: a formula can compare it with `==` and `!=`,
/// test it with `isNull` and pass it on; it is refused as text and in a method call.
fn is_object(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Object(_) | ColumnType::DayOfWeek | ColumnType::Table(_)
    )
}

/// Whether a value of `column_type` can be a cell of an `Object` column: a class whose
/// equality and text this crate and the server know.
fn objectable(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Boolean
            | ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Char
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::String
            | ColumnType::Instant
            | ColumnType::LocalDate
            | ColumnType::LocalTime
            | ColumnType::DayOfWeek
    )
}

/// `(Object) operand`, of an operand that is not yet an `Object`.
///
/// Behavior: `QueryLanguageParser#visit(CastExpr)`, which leaves a cast to a class that is
/// no box to javac: a primitive is boxed as the value it holds.
fn boxed(operand: Typed) -> Result<Typed, FormulaError> {
    if !objectable(&operand.column_type) {
        return Err(unsupported(format!(
            "a value of type {} as a java.lang.Object",
            java_name(&operand.column_type)
        )));
    }
    Ok(typed(
        ColumnType::object(Some(operand.column_type.clone())),
        Node::ToObject(Box::new(operand)),
    ))
}

/// `(to) operand` of an `Object` operand.
///
/// Behavior: `QueryLanguageParser#visit(CastExpr)`: a cast to a primitive is
/// `intCast(Object)` and its relatives, a cast to a class is javac's checked cast.
fn from_object(to: &ColumnType, operand: Typed) -> Result<Typed, FormulaError> {
    let known = matches!(
        to,
        ColumnType::Int | ColumnType::Long | ColumnType::Float | ColumnType::Double
    ) || (objectable(to) && !crate::eval::is_primitive(to));
    if !known {
        return Err(unsupported(format!(
            "cast of java.lang.Object to {}",
            java_name(to)
        )));
    }
    Ok(typed(to.clone(), Node::FromObject(Box::new(operand))))
}

/// An operator with an operand that is an `Object` or a `DayOfWeek`.
///
/// Behavior: `QueryLanguageParser#visit(BinaryExpr)`: `==` is
/// `QueryLanguageFunctionUtils#eq(Object, Object)`, which boxes a primitive operand, and
/// `!=` its negation; `+` beside a `String` is Java's string concatenation. Java refuses
/// any other operator on an `Object`; it orders a `DayOfWeek` by `compareTo`, which is not
/// ported.
fn object_operator(op: BinaryOp, left: Typed, right: Typed) -> Result<Typed, FormulaError> {
    let as_object = |side: Typed| -> Result<Typed, FormulaError> {
        if is_object(&side.column_type) {
            Ok(side)
        } else {
            boxed(side)
        }
    };
    let text = |side: Typed| {
        if is_object(&side.column_type) {
            typed(ColumnType::String, Node::ObjectString(Box::new(side)))
        } else {
            side
        }
    };
    match op {
        BinaryOp::Equal | BinaryOp::NotEqual => {
            let equals = typed(
                ColumnType::Boolean,
                Node::ObjectEquals {
                    left: Box::new(as_object(left)?),
                    right: Box::new(as_object(right)?),
                },
            );
            Ok(if op == BinaryOp::Equal {
                equals
            } else {
                typed(ColumnType::Boolean, Node::Not(Box::new(equals)))
            })
        }
        // The text of a table is its description, which this crate does not know.
        BinaryOp::Add
            if matches!(left.column_type, ColumnType::Table(_))
                || matches!(right.column_type, ColumnType::Table(_)) =>
        {
            Err(unsupported(
                "a table as text: io.deephaven.engine.table.impl.BaseTable#toString".to_string(),
            ))
        }
        BinaryOp::Add
            if left.column_type == ColumnType::String
                || right.column_type == ColumnType::String =>
        {
            Ok(concat(text(left), text(right)))
        }
        _ if left.column_type == ColumnType::DayOfWeek
            || right.column_type == ColumnType::DayOfWeek =>
        {
            Err(unsupported(format!(
                "{} of {} and {}: java.time.DayOfWeek#compareTo",
                symbol(op),
                java_name(&left.column_type),
                java_name(&right.column_type)
            )))
        }
        _ => Err(operands_error(op, &left, &right)),
    }
}

/// `condition ? if_true : if_false` where a branch is an `Object` and the branches have
/// two types. The result is an `Object`, whose cells must have one class.
///
/// Behavior: `QueryLanguageParser#visit(ConditionalExpr)`, which gives the branches their
/// common supertype, `Object`, and boxes a primitive branch.
fn object_ternary(
    condition: Typed,
    if_true: Typed,
    if_false: Typed,
) -> Result<Typed, FormulaError> {
    let (true_class, false_class) = (
        crate::eval::class_of(&if_true.column_type),
        crate::eval::class_of(&if_false.column_type),
    );
    let class = match (true_class, false_class) {
        (Some(a), Some(b)) if a != b => {
            return Err(unsupported(format!(
                "?: whose branches are a {} and a {}, which makes a java.lang.Object column \
                 of two classes",
                java_name(&a),
                java_name(&b)
            )))
        }
        (a, b) => a.or(b),
    };
    let result = ColumnType::object(class);
    let branch = |branch: Typed| -> Result<Typed, FormulaError> {
        if branch.column_type == result {
            return Ok(branch);
        }
        match (&branch.column_type, &branch.node) {
            (ColumnType::Object(None), Node::Null) => Ok(typed(result.clone(), Node::Null)),
            (ColumnType::Object(_), _) => Err(unsupported(
                "?: with a branch of nulls that is not the null literal beside an Object",
            )),
            _ => boxed(branch),
        }
    };
    Ok(typed(
        result.clone(),
        Node::Ternary {
            condition: Box::new(condition),
            if_true: Box::new(branch(if_true)?),
            if_false: Box::new(branch(if_false)?),
        },
    ))
}

/// Behavior: the literal visitors of `QueryLanguageParser`. A `null` literal has no type of
/// its own; the operators that give it one handle it before they reach here.
fn literal_type(literal: &Literal) -> Result<ColumnType, FormulaError> {
    Ok(match literal {
        Literal::Null => {
            return Err(unsupported(
                "null literal outside ==, !=, ?:, a cast and string concatenation",
            ))
        }
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

    /// The type `QueryLanguageParser#visit(ConditionalExpr)` returns for `c ? a : b`, for all
    /// 49 pairs of primitive numeric types, by `#dhqlIsAssignableFrom`: the first letter of
    /// the type, or `-` where it throws "Incompatible types in condition operation".
    #[test]
    fn a_ternary_takes_the_promoted_type_only_when_one_branch_has_it() {
        let types = [
            ColumnType::Byte,
            ColumnType::Short,
            ColumnType::Char,
            ColumnType::Int,
            ColumnType::Long,
            ColumnType::Float,
            ColumnType::Double,
        ];
        let expected = [
            // byte short char int long float double
            "b--ilfd", // byte
            "-s-ilfd", // short
            "--cilfd", // char
            "iiiilfd", // int
            "lllllfd", // long
            "ffffffd", // float
            "ddddddd", // double
        ];
        let actual: Vec<String> = types
            .iter()
            .map(|if_true| {
                types
                    .iter()
                    .map(|if_false| match ternary_type(if_true, if_false) {
                        Some(ColumnType::Byte) => 'b',
                        Some(ColumnType::Short) => 's',
                        Some(ColumnType::Char) => 'c',
                        Some(ColumnType::Int) => 'i',
                        Some(ColumnType::Long) => 'l',
                        Some(ColumnType::Float) => 'f',
                        Some(ColumnType::Double) => 'd',
                        _ => '-',
                    })
                    .collect()
            })
            .collect();
        assert_eq!(actual, expected);
        for other in [ColumnType::Boolean, ColumnType::String, ColumnType::Instant] {
            assert_eq!(ternary_type(&other, &other), Some(other.clone()));
            assert_eq!(ternary_type(&other, &ColumnType::Int), None);
            assert_eq!(ternary_type(&ColumnType::Double, &other), None);
        }
        assert_eq!(
            ternary_type(&ColumnType::String, &ColumnType::Instant),
            None
        );
    }
}
