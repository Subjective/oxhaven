//! The expression tree that formulas parse into.
//!
//! No Java counterpart: Java compiles formula strings to classes. This tree is the single
//! representation that the parser produces and the evaluator consumes. Its semantics (null
//! handling, numeric promotion, overflow) are those of the Java formula language as recorded
//! in PARITY.md.

use crate::rowset::RowSet;
use crate::schema::{ColumnType, TableDefinition};
use crate::table::{Table, TableFailure};
use arrow::array::ArrayRef;
use std::sync::Arc;

#[derive(Clone, PartialEq, Debug)]
pub enum Literal {
    Null,
    Boolean(bool),
    Char(u16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    /// A time literal: nanoseconds since the epoch, UTC.
    Instant(i64),
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum UnaryOp {
    Negate,
    Not,
    BitNot,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    And,
    Or,
    BitAnd,
    BitOr,
    BitXor,
}

/// The built-in row variables of the formula language.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum RowVariable {
    /// `i`: row position as an int.
    I,
    /// `ii`: row position as a long.
    Ii,
    /// `k`: row key as a long.
    K,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Expr {
    /// The value of a column in the current row.
    Column(String),
    Literal(Literal),
    RowVariable(RowVariable),
    Unary {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// `condition ? if_true : if_false`.
    Ternary {
        condition: Box<Expr>,
        if_true: Box<Expr>,
        if_false: Box<Expr>,
    },
    /// `(type) operand`.
    Cast {
        to: ColumnType,
        operand: Box<Expr>,
    },
    /// A call to a registered function, by name.
    Call {
        function: String,
        arguments: Vec<Expr>,
    },
    /// `receiver.method(arguments)`, for example `Sym.startsWith(\`A\`)`. Only methods the
    /// function library registers are accepted.
    MethodCall {
        receiver: Box<Expr>,
        method: String,
        arguments: Vec<Expr>,
    },
    /// `Column_`: the whole column as a vector, as an argument to a function.
    ColumnVector(String),
    /// `Column_[position]`: the value of a column at another row position.
    ColumnArrayAccess {
        column: String,
        position: Box<Expr>,
    },
}

/// One output column of `select`, `update`, `view` and the like: `name = expr`.
#[derive(Clone, PartialEq, Debug)]
pub struct ColumnAssignment {
    pub name: String,
    pub expr: Expr,
}

/// A formula compiled against a table definition: what operators evaluate.
///
/// Behavior: the roles of `io.deephaven.engine.table.impl.select.SelectColumn` and
/// `WhereFilter` that operators depend on.
pub trait Formula: Send + Sync {
    fn result_type(&self) -> &ColumnType;

    /// Columns read at the current row. Operators recompute a formula for a modified row only
    /// when one of these is in the modified-column set.
    fn columns(&self) -> &[String];

    /// Columns read at other rows, through `Column_[..]` or `Column_`. A change anywhere in
    /// one of these can change any row's result.
    fn column_arrays(&self) -> &[String];

    /// Whether the formula uses `i`, `ii` or `k`, so its result depends on row position or key.
    fn uses_row_variables(&self) -> bool;

    /// Evaluates the formula for `rows` of `table`, in row-key order, against current values
    /// or, with `use_prev`, against previous values and the previous row set. A boolean
    /// formula used as a filter returns a boolean array.
    fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        use_prev: bool,
    ) -> Result<ArrayRef, TableFailure>;
}

/// Why a formula was not accepted.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FormulaError {
    /// The text is not valid formula syntax.
    Syntax(String),
    /// Valid in Java's formula language but outside the subset Oxhaven accepts; names the
    /// construct.
    Unsupported(String),
    /// Refers to a missing column or function, or the types do not fit.
    Type(String),
}

impl std::fmt::Display for FormulaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormulaError::Syntax(m) => write!(f, "syntax error: {m}"),
            FormulaError::Unsupported(m) => write!(f, "unsupported in formulas: {m}"),
            FormulaError::Type(m) => write!(f, "type error: {m}"),
        }
    }
}

impl std::error::Error for FormulaError {}

/// Turns formula text or an expression tree into something operators can evaluate. The
/// expression crate implements this; operators receive it and never depend on that crate.
pub trait FormulaCompiler: Send + Sync {
    /// Parses a formula expression such as `(Bid + Ask) / 2`.
    fn parse(&self, text: &str) -> Result<Expr, FormulaError>;

    /// Parses a column assignment such as `Mid = (Bid + Ask) / 2`.
    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError>;

    /// Type-checks `expr` against `definition` and prepares it for evaluation.
    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError>;
}
