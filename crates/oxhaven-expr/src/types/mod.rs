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

mod methods;

use crate::external::{ExternalArgument, ExternalFunctions};
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
    Unrelated,
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
    /// An operator applied to the elements of a vector or array and a value, or to the
    /// elements of two of them, pairwise. `body` is the operator on one pair: its
    /// `Node::Column(0)` is the left element and its `Node::Column(1)` the right one.
    Elementwise {
        body: Box<Typed>,
        left: Box<Typed>,
        right: Box<Typed>,
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
/// `"true"` and `"false"`, and for one of an `Instant` column.
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
            if matches!(**element, ColumnType::Boolean | ColumnType::Instant)
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
}

impl Checker<'_> {
    fn check(&mut self, expr: &Expr) -> Result<Typed, FormulaError> {
        match expr {
            Expr::Column(name) => self.column(name),
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
            } => self.call(function, arguments),
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
        ) || vectors::is_supported_list(&column_type);
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
        let known = matches!(
            element,
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
        if functions::is_library_constant(scope, field) {
            return match functions::constant(field) {
                Some((column_type, literal)) => Ok(constant(column_type, literal)),
                None => Err(unsupported(format!("constant {field}"))),
            };
        }
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
        match functions::library_function(class, method) {
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

        let left = self.check(left)?;
        let right = self.check(right)?;
        // Java turns an operator with a vector or array operand into `eqArray`,
        // `lessArray` and their relatives, which compare element by element.
        if is_list(&left) || is_list(&right) {
            return self.elementwise(op, left, right);
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
    /// makes of it, `parseDuration("PT1m")` or `parsePeriod("P1d")`. No column holds
    /// either type, so one is known here only where a formula computes with it: multiplied
    /// or divided by a whole number, and added to or subtracted from an `Instant`.
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
                    // Java adds the years and months of a period to an `Instant` with an
                    // exception, and holds durations longer than a `long` of nanoseconds.
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
                let parameter = if amount.period {
                    ColumnType::Int
                } else {
                    ColumnType::Long
                };
                let narrow = matches!(
                    scalar.column_type,
                    ColumnType::Byte | ColumnType::Short | ColumnType::Char | ColumnType::Int
                );
                let taken = narrow || (!amount.period && scalar.column_type == ColumnType::Long);
                if !taken || (amount.period && *op == BinaryOp::Divide) {
                    return Err(type_error(format!(
                        "Cannot find method {method}(java.time.{}, {})",
                        class(amount.period),
                        java_name(&scalar.column_type)
                    )));
                }
                let scalar = if scalar.column_type == parameter {
                    scalar
                } else {
                    typed(parameter, Node::Widen(Box::new(scalar)))
                };
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
                            arguments: vec![amount.nanos, convert(scalar, &ColumnType::Long)],
                        },
                    ),
                    period: amount.period,
                }))
            }
            _ => Ok(None),
        }
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
            _ => (None, left),
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
        let arithmetic = match family(op) {
            Family::Arithmetic(arithmetic) if number(&left_element) && number(&right_element) => {
                arithmetic
            }
            // `eqArray`, `lessArray` and their relatives return a `boolean[]`. An array of
            // `Boolean` elements is Java's `java.lang.Boolean[]` here, a client would be
            // told that class, and no function here takes one, so the comparison has no
            // use that could be computed. `binaryAndArray` and its relatives exist for
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
        let operand = self.check(operand)?;
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
            (true, true) => return Err(unsupported("null in both branches of ?:")),
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
        if Numeric::of(&receiver.column_type).is_some() {
            return Err(type_error(format!(
                "cannot call .{method}() on a value of the primitive type {}",
                java_name(&receiver.column_type)
            )));
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
            let argument = self.check(argument)?;
            return Ok(typed(ColumnType::Boolean, Node::IsNull(Box::new(argument))));
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
            Err(other) => return Err(other),
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
/// unrelated types compiles in Java and throws `ClassCastException` when it runs.
fn comparison(op: CompareOp, left: Typed, right: Typed) -> Result<Typed, FormulaError> {
    let numeric = Numeric::of(&left.column_type).zip(Numeric::of(&right.column_type));
    let domain = match (numeric, &left.column_type, &right.column_type) {
        (Some((l, r)), _, _) => numeric_compare_domain(op, l, r),
        (None, ColumnType::String, ColumnType::String) => CompareDomain::String,
        (None, ColumnType::Instant, ColumnType::Instant) => CompareDomain::Integral,
        (None, ColumnType::Boolean, ColumnType::Boolean) => CompareDomain::Boolean,
        (None, _, _) if op.is_equality() => CompareDomain::Unrelated,
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
