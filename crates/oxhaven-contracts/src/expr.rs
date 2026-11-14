//! The expression tree that formulas parse into.
//!
//! No Java counterpart: Java compiles formula strings to classes. This tree is the single
//! representation that the parser produces and the evaluator consumes. Its semantics (null
//! handling, numeric promotion, overflow) are those of the Java formula language as recorded
//! in PARITY.md.

use crate::schema::ColumnType;

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
