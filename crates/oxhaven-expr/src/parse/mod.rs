//! Formula text to expression tree.
//!
//! The grammar is Java's expression grammar, which is what
//! `io.deephaven.engine.table.impl.lang.QueryLanguageParser` hands to JavaParser. Constructs
//! of that grammar that are not built yet are recognized and reported by name, so adding one
//! means replacing its error with the tree node.
//!
//! The parser knows no table and no function library: every call, method call and
//! `Column_[...]` access becomes a tree node, and the type checker decides what it means.

pub(crate) mod instant;
mod lexer;

use lexer::{Spanned, Token};
use oxhaven_contracts::expr::{
    BinaryOp, ColumnAssignment, Expr, FormulaError, Literal, RowVariable, UnaryOp,
};
use oxhaven_contracts::schema::ColumnType;

fn syntax(message: impl Into<String>) -> FormulaError {
    FormulaError::Syntax(message.into())
}

fn unsupported(message: impl Into<String>) -> FormulaError {
    FormulaError::Unsupported(message.into())
}

/// Parses a whole formula expression.
pub(crate) fn parse_expression(text: &str) -> Result<Expr, FormulaError> {
    let mut parser = Parser {
        tokens: lexer::tokenize(text)?,
        position: 0,
        end: text.len(),
    };
    let expr = parser.expression()?;
    match parser.peek() {
        None => Ok(expr),
        Some(_) => Err(parser.unexpected()),
    }
}

/// Parses `Name = expression`, or a bare `Name`, which selects that column unchanged.
///
/// Behavior: the two patterns registered in
/// `io.deephaven.engine.table.impl.select.SelectColumnFactory`, with the identifier pattern
/// `io.deephaven.api.expression.SelectFactoryConstants#ID_PTRN`.
pub(crate) fn parse_assignment(text: &str) -> Result<ColumnAssignment, FormulaError> {
    let trimmed = text.trim_start();
    let name_end = trimmed
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '$'))
        .unwrap_or(trimmed.len());
    let name = &trimmed[..name_end];
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(syntax(format!(
            "expected a column name at the start of \"{text}\""
        )));
    }
    let rest = trimmed[name_end..].trim_start();
    let expr = if rest.is_empty() {
        Expr::Column(name.to_string())
    } else if let Some(formula) = rest.strip_prefix('=') {
        if formula.trim().is_empty() {
            return Err(syntax(format!("missing expression after \"{name} =\"")));
        }
        parse_expression(formula)?
    } else {
        return Err(syntax(format!(
            "expected \"=\" after the column name {name}"
        )));
    };
    Ok(ColumnAssignment {
        name: name.to_string(),
        expr,
    })
}

/// What a binary operator token means to the formula language.
enum Infix {
    Operator(BinaryOp),
    /// Java's parser accepts the token and the formula language refuses it.
    Rejected(&'static str),
}

/// Binding strength and meaning of a binary operator, weakest first, as in the Java Language
/// Specification chapter 15.
///
/// Behavior: `QueryLanguageParser#getOperatorName` for the operators the language refuses,
/// and `QueryLanguageParser#convertSingleEquals` for `=`.
fn infix(symbol: &str) -> Option<(u8, Infix)> {
    use BinaryOp::*;
    let (strength, op) = match symbol {
        "||" => (1, Or),
        "&&" => (2, And),
        "|" => (3, BitOr),
        "^" => (4, BitXor),
        "&" => (5, BitAnd),
        "==" | "=" => (6, Equal),
        "!=" => (6, NotEqual),
        "<" => (7, Less),
        "<=" => (7, LessOrEqual),
        ">" => (7, Greater),
        ">=" => (7, GreaterOrEqual),
        "<<" => return Some((8, Infix::Rejected("leftShift (<<) is not supported"))),
        ">>" => return Some((8, Infix::Rejected("signedRightShift (>>) is not supported"))),
        ">>>" => {
            return Some((
                8,
                Infix::Rejected("unsignedRightShift (>>>) is not supported"),
            ))
        }
        "+" => (9, Add),
        "-" => (9, Subtract),
        "*" => (10, Multiply),
        "/" => (10, Divide),
        "%" => (10, Remainder),
        _ => return None,
    };
    Some((strength, Infix::Operator(op)))
}

const PRIMITIVE_TYPES: &[&str] = &[
    "boolean", "byte", "char", "short", "int", "long", "float", "double",
];

struct Parser {
    tokens: Vec<Spanned>,
    position: usize,
    /// Length of the source text, reported as the offset of the end of input.
    end: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.peek_at(0)
    }

    fn peek_at(&self, ahead: usize) -> Option<&Token> {
        self.tokens.get(self.position + ahead).map(|(t, _)| t)
    }

    fn peek_symbol(&self) -> Option<&'static str> {
        match self.peek() {
            Some(Token::Symbol(symbol)) => Some(symbol),
            _ => None,
        }
    }

    fn advance(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).map(|(t, _)| t.clone());
        self.position += 1;
        token
    }

    fn eat(&mut self, symbol: &str) -> bool {
        let found = self.peek_symbol() == Some(symbol);
        if found {
            self.position += 1;
        }
        found
    }

    fn expect(&mut self, symbol: &str) -> Result<(), FormulaError> {
        if self.eat(symbol) {
            Ok(())
        } else {
            Err(syntax(format!("expected \"{symbol}\" {}", self.location())))
        }
    }

    fn location(&self) -> String {
        match self.tokens.get(self.position) {
            Some((_, offset)) => format!("at offset {offset}"),
            None => format!("at the end of the formula (offset {})", self.end),
        }
    }

    fn unexpected(&self) -> FormulaError {
        match self.peek() {
            Some(Token::Symbol(symbol)) => {
                syntax(format!("unexpected \"{symbol}\" {}", self.location()))
            }
            Some(Token::Identifier(name)) => {
                syntax(format!("unexpected \"{name}\" {}", self.location()))
            }
            Some(Token::Literal(_)) => syntax(format!("unexpected literal {}", self.location())),
            None => syntax("unexpected end of the formula"),
        }
    }

    /// An expression, which is a conditional expression: `a ? b : c` groups to the right
    /// and binds more weakly than every binary operator.
    fn expression(&mut self) -> Result<Expr, FormulaError> {
        let condition = self.binary(0)?;
        if !self.eat("?") {
            return Ok(condition);
        }
        let if_true = self.expression()?;
        self.expect(":")?;
        let if_false = self.expression()?;
        Ok(Expr::Ternary {
            condition: Box::new(condition),
            if_true: Box::new(if_true),
            if_false: Box::new(if_false),
        })
    }

    /// Parses binary operators that bind more strongly than `floor`. All of them associate
    /// to the left.
    fn binary(&mut self, floor: u8) -> Result<Expr, FormulaError> {
        let mut left = self.unary()?;
        loop {
            // `QueryLanguageParser#visit(InstanceOfExpr)` throws.
            if matches!(self.peek(), Some(Token::Identifier(name)) if name == "instanceof") {
                return Err(syntax("InstanceOfExpr Operation not supported!"));
            }
            let Some((strength, meaning)) = self.peek_symbol().and_then(infix) else {
                return Ok(left);
            };
            if strength <= floor {
                return Ok(left);
            }
            let op = match meaning {
                Infix::Operator(op) => op,
                Infix::Rejected(why) => return Err(syntax(why)),
            };
            self.position += 1;
            let right = self.binary(strength)?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
    }

    /// Behavior: `QueryLanguageParser#visit(UnaryExpr)`, which accepts only `!` and `-`.
    fn unary(&mut self) -> Result<Expr, FormulaError> {
        let op = match self.peek_symbol() {
            Some("-") => UnaryOp::Negate,
            Some("!") => UnaryOp::Not,
            Some("+") => return Err(syntax("Unary operation (PLUS) not supported")),
            Some("~") => return Err(syntax("Unary operation (BITWISE_COMPLEMENT) not supported")),
            Some("++") | Some("--") => {
                return Err(syntax("increment and decrement are not supported"))
            }
            Some("(") if self.at_cast() => return self.cast(),
            _ => return self.postfix(),
        };
        self.position += 1;
        Ok(Expr::Unary {
            op,
            operand: Box::new(self.unary()?),
        })
    }

    /// Whether the tokens from the current `(` spell a cast: `(primitive)`, or `(Name)`
    /// directly followed by the start of an operand that is not `+` or `-`, as in the Java
    /// Language Specification section 15.16.
    fn at_cast(&self) -> bool {
        let mut ahead = 1;
        let Some(Token::Identifier(first)) = self.peek_at(ahead) else {
            return false;
        };
        ahead += 1;
        // A qualified name such as java.lang.String.
        while self.peek_at(ahead) == Some(&Token::Symbol("."))
            && matches!(self.peek_at(ahead + 1), Some(Token::Identifier(_)))
        {
            ahead += 2;
        }
        // An array type such as int[].
        while self.peek_at(ahead) == Some(&Token::Symbol("["))
            && self.peek_at(ahead + 1) == Some(&Token::Symbol("]"))
        {
            ahead += 2;
        }
        if self.peek_at(ahead) != Some(&Token::Symbol(")")) {
            return false;
        }
        if PRIMITIVE_TYPES.contains(&first.as_str()) {
            return true;
        }
        match self.peek_at(ahead + 1) {
            Some(Token::Identifier(name)) => name != "instanceof",
            Some(Token::Literal(_)) => true,
            Some(Token::Symbol(symbol)) => matches!(*symbol, "(" | "!" | "~"),
            None => false,
        }
    }

    /// Parses `(type) operand` from its opening parenthesis.
    ///
    /// The tree has one node for a cast to a column type, and `boolean` and `Boolean` are
    /// the same column type while the two casts differ: `(Boolean) x` leaves `x` as it is,
    /// and `(boolean) x` unboxes it, which throws for null. The second is exactly
    /// `x ? true : false`, and that is the tree it becomes.
    ///
    /// Behavior: `QueryLanguageParser#visit(CastExpr)`.
    fn cast(&mut self) -> Result<Expr, FormulaError> {
        self.position += 1;
        let mut name = String::new();
        loop {
            match self.advance() {
                Some(Token::Identifier(part)) => name.push_str(&part),
                _ => return Err(syntax("expected a type name in a cast")),
            }
            if !self.eat(".") {
                break;
            }
            name.push('.');
        }
        while self.eat("[") {
            self.expect("]")?;
            name.push_str("[]");
        }
        self.expect(")")?;
        let operand = Box::new(self.unary()?);
        let to = match name.as_str() {
            "boolean" => {
                return Ok(Expr::Ternary {
                    condition: operand,
                    if_true: Box::new(Expr::Literal(Literal::Boolean(true))),
                    if_false: Box::new(Expr::Literal(Literal::Boolean(false))),
                })
            }
            "byte" => ColumnType::Byte,
            "char" => ColumnType::Char,
            "short" => ColumnType::Short,
            "int" => ColumnType::Int,
            "long" => ColumnType::Long,
            "float" => ColumnType::Float,
            "double" => ColumnType::Double,
            "Boolean" | "java.lang.Boolean" => ColumnType::Boolean,
            "String" | "java.lang.String" => ColumnType::String,
            "Instant" | "java.time.Instant" => ColumnType::Instant,
            other => return Err(unsupported(format!("cast to {other}"))),
        };
        Ok(Expr::Cast { to, operand })
    }

    fn postfix(&mut self) -> Result<Expr, FormulaError> {
        let mut expr = self.primary()?;
        loop {
            match self.peek_symbol() {
                Some(".") => match (self.peek_at(1), self.peek_at(2)) {
                    (Some(Token::Identifier(name)), Some(Token::Symbol("("))) => {
                        let method = name.clone();
                        self.position += 2;
                        expr = Expr::MethodCall {
                            receiver: Box::new(expr),
                            method,
                            arguments: self.arguments()?,
                        };
                    }
                    (Some(Token::Identifier(name)), _) => {
                        return Err(unsupported(format!("field access .{name}")))
                    }
                    _ => {
                        self.position += 1;
                        return Err(self.unexpected());
                    }
                },
                Some("[") => {
                    // `Column_` is the column as an array, and indexing it reads another row.
                    let column = match &expr {
                        Expr::Column(name) => name.strip_suffix('_').filter(|c| !c.is_empty()),
                        _ => None,
                    };
                    let Some(column) = column.map(str::to_string) else {
                        return Err(unsupported("array access [...]"));
                    };
                    self.position += 1;
                    let position = self.expression()?;
                    self.expect("]")?;
                    expr = Expr::ColumnArrayAccess {
                        column,
                        position: Box::new(position),
                    };
                }
                Some("++") | Some("--") => {
                    return Err(syntax("increment and decrement are not supported"))
                }
                // `QueryLanguageParser#visit(MethodReferenceExpr)` and `#visit(LambdaExpr)`
                // throw.
                Some("::") => return Err(syntax("MethodReferenceExpr Operation not supported")),
                Some("->") => return Err(syntax("LambdaExpr Operation not supported!")),
                _ => return Ok(expr),
            }
        }
    }

    fn primary(&mut self) -> Result<Expr, FormulaError> {
        match self.peek() {
            Some(Token::Literal(_)) => match self.advance() {
                Some(Token::Literal(literal)) => Ok(Expr::Literal(literal)),
                _ => Err(self.unexpected()),
            },
            Some(Token::Symbol("(")) => {
                self.position += 1;
                let inner = self.expression()?;
                self.expect(")")?;
                Ok(inner)
            }
            Some(Token::Identifier(_)) => {
                let Some(Token::Identifier(name)) = self.advance() else {
                    return Err(self.unexpected());
                };
                if self.peek_symbol() == Some("(") {
                    return Ok(Expr::Call {
                        function: name,
                        arguments: self.arguments()?,
                    });
                }
                Ok(match name.as_str() {
                    "true" => Expr::Literal(Literal::Boolean(true)),
                    "false" => Expr::Literal(Literal::Boolean(false)),
                    "null" => Expr::Literal(Literal::Null),
                    "i" => Expr::RowVariable(RowVariable::I),
                    "ii" => Expr::RowVariable(RowVariable::Ii),
                    "k" => Expr::RowVariable(RowVariable::K),
                    "new" => return Err(unsupported("object creation (new)")),
                    _ => Expr::Column(name),
                })
            }
            _ => Err(self.unexpected()),
        }
    }

    /// Parses a parenthesized argument list.
    fn arguments(&mut self) -> Result<Vec<Expr>, FormulaError> {
        self.expect("(")?;
        let mut arguments = Vec::new();
        if !self.eat(")") {
            loop {
                arguments.push(self.expression()?);
                if self.eat(")") {
                    break;
                }
                self.expect(",")?;
            }
        }
        Ok(arguments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str) -> Expr {
        Expr::Column(name.into())
    }

    fn int(value: i32) -> Expr {
        Expr::Literal(Literal::Int(value))
    }

    fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn unary(op: UnaryOp, operand: Expr) -> Expr {
        Expr::Unary {
            op,
            operand: Box::new(operand),
        }
    }

    #[test]
    fn multiplication_binds_tighter_than_addition_and_both_group_left() {
        use BinaryOp::*;
        assert_eq!(
            parse_expression("A + B * 2 - C"),
            Ok(binary(
                Subtract,
                binary(Add, column("A"), binary(Multiply, column("B"), int(2))),
                column("C")
            ))
        );
        assert_eq!(
            parse_expression("A - B - C"),
            Ok(binary(
                Subtract,
                binary(Subtract, column("A"), column("B")),
                column("C")
            ))
        );
        assert_eq!(
            parse_expression("A / B % C * D"),
            Ok(binary(
                Multiply,
                binary(
                    Remainder,
                    binary(Divide, column("A"), column("B")),
                    column("C")
                ),
                column("D")
            ))
        );
    }

    #[test]
    fn parentheses_override_precedence() {
        use BinaryOp::*;
        assert_eq!(
            parse_expression("(Bid + Ask) / 2"),
            Ok(binary(
                Divide,
                binary(Add, column("Bid"), column("Ask")),
                int(2)
            ))
        );
    }

    /// The order is that of the Java Language Specification chapter 15: `||` weakest, then
    /// `&&`, `|`, `^`, `&`, equality, relational, additive, multiplicative.
    #[test]
    fn logical_equality_and_relational_operators_nest_in_java_order() {
        use BinaryOp::*;
        assert_eq!(
            parse_expression("A < B + 1 == C > 2 && D || E"),
            Ok(binary(
                Or,
                binary(
                    And,
                    binary(
                        Equal,
                        binary(Less, column("A"), binary(Add, column("B"), int(1))),
                        binary(Greater, column("C"), int(2))
                    ),
                    column("D")
                ),
                column("E")
            ))
        );
        assert_eq!(
            parse_expression("A || B && C | D ^ E & F != G"),
            Ok(binary(
                Or,
                column("A"),
                binary(
                    And,
                    column("B"),
                    binary(
                        BitOr,
                        column("C"),
                        binary(
                            BitXor,
                            column("D"),
                            binary(
                                BitAnd,
                                column("E"),
                                binary(NotEqual, column("F"), column("G"))
                            )
                        )
                    )
                )
            ))
        );
    }

    /// Behavior: `QueryLanguageParser#convertSingleEquals`.
    #[test]
    fn single_equals_is_equality() {
        let expected = Ok(binary(BinaryOp::Equal, column("A"), int(1)));
        assert_eq!(parse_expression("A = 1"), expected);
        assert_eq!(parse_expression("A == 1"), expected);
        assert_eq!(
            parse_expression("A <= 1"),
            Ok(binary(BinaryOp::LessOrEqual, column("A"), int(1)))
        );
        assert!(matches!(
            parse_expression("A === 1"),
            Err(FormulaError::Syntax(_))
        ));
        assert!(matches!(
            parse_expression("A += 1"),
            Err(FormulaError::Syntax(_))
        ));
    }

    #[test]
    fn unary_operators_bind_tighter_than_binary_ones() {
        use BinaryOp::*;
        assert_eq!(
            parse_expression("-A * !B"),
            Ok(binary(
                Multiply,
                unary(UnaryOp::Negate, column("A")),
                unary(UnaryOp::Not, column("B"))
            ))
        );
        assert_eq!(
            parse_expression("A - -1"),
            Ok(binary(
                Subtract,
                column("A"),
                unary(UnaryOp::Negate, int(1))
            ))
        );
        assert_eq!(
            parse_expression("!!A"),
            Ok(unary(UnaryOp::Not, unary(UnaryOp::Not, column("A"))))
        );
        assert_eq!(
            parse_expression("(A) - B"),
            Ok(binary(Subtract, column("A"), column("B")))
        );
    }

    #[test]
    fn names_are_literals_row_variables_or_columns() {
        assert_eq!(
            parse_expression("true"),
            Ok(Expr::Literal(Literal::Boolean(true)))
        );
        assert_eq!(
            parse_expression("false"),
            Ok(Expr::Literal(Literal::Boolean(false)))
        );
        assert_eq!(parse_expression("null"), Ok(Expr::Literal(Literal::Null)));
        assert_eq!(parse_expression("i"), Ok(Expr::RowVariable(RowVariable::I)));
        assert_eq!(
            parse_expression("ii"),
            Ok(Expr::RowVariable(RowVariable::Ii))
        );
        assert_eq!(parse_expression("k"), Ok(Expr::RowVariable(RowVariable::K)));
        assert_eq!(parse_expression(" Price_2$ "), Ok(column("Price_2$")));
    }

    #[test]
    fn a_call_keeps_its_name_and_arguments() {
        assert_eq!(
            parse_expression("isNull(A + 1)"),
            Ok(Expr::Call {
                function: "isNull".into(),
                arguments: vec![binary(BinaryOp::Add, column("A"), int(1))],
            })
        );
        assert_eq!(
            parse_expression("isNull()"),
            Ok(Expr::Call {
                function: "isNull".into(),
                arguments: vec![],
            })
        );
        assert_eq!(
            parse_expression("isNull(A, B)"),
            Ok(Expr::Call {
                function: "isNull".into(),
                arguments: vec![column("A"), column("B")],
            })
        );
    }

    #[test]
    fn any_function_name_is_a_call() {
        assert_eq!(
            parse_expression("abs(A) + f()"),
            Ok(binary(
                BinaryOp::Add,
                Expr::Call {
                    function: "abs".into(),
                    arguments: vec![column("A")],
                },
                Expr::Call {
                    function: "f".into(),
                    arguments: vec![],
                }
            ))
        );
    }

    fn ternary(condition: Expr, if_true: Expr, if_false: Expr) -> Expr {
        Expr::Ternary {
            condition: Box::new(condition),
            if_true: Box::new(if_true),
            if_false: Box::new(if_false),
        }
    }

    /// The Java Language Specification section 15.25: the conditional operator groups to
    /// the right and binds more weakly than `||`.
    #[test]
    fn the_ternary_operator_groups_to_the_right_below_every_binary_operator() {
        use BinaryOp::*;
        assert_eq!(
            parse_expression("A > 1 || B ? C + 1 : D"),
            Ok(ternary(
                binary(Or, binary(Greater, column("A"), int(1)), column("B")),
                binary(Add, column("C"), int(1)),
                column("D")
            ))
        );
        assert_eq!(
            parse_expression("A ? B : C ? D : E"),
            Ok(ternary(
                column("A"),
                column("B"),
                ternary(column("C"), column("D"), column("E"))
            ))
        );
        assert_eq!(
            parse_expression("A ? B ? C : D : E"),
            Ok(ternary(
                column("A"),
                ternary(column("B"), column("C"), column("D")),
                column("E")
            ))
        );
        assert_eq!(
            parse_expression("(A ? B : C) + 1"),
            Ok(binary(
                Add,
                ternary(column("A"), column("B"), column("C")),
                int(1)
            ))
        );
        for text in ["A ? B", "A ? : C", "A ? B :", "A ? B : C : D", "? B : C"] {
            assert!(
                matches!(parse_expression(text), Err(FormulaError::Syntax(_))),
                "{text}"
            );
        }
    }

    fn cast(to: ColumnType, operand: Expr) -> Expr {
        Expr::Cast {
            to,
            operand: Box::new(operand),
        }
    }

    /// The Java Language Specification section 15.16: a cast binds like a unary operator.
    #[test]
    fn a_cast_applies_to_the_unary_expression_after_it() {
        use BinaryOp::*;
        assert_eq!(
            parse_expression("(int) A + B"),
            Ok(binary(Add, cast(ColumnType::Int, column("A")), column("B")))
        );
        assert_eq!(
            parse_expression("(long)(A + B)"),
            Ok(cast(
                ColumnType::Long,
                binary(Add, column("A"), column("B"))
            ))
        );
        assert_eq!(
            parse_expression("(double) -A"),
            Ok(cast(
                ColumnType::Double,
                unary(UnaryOp::Negate, column("A"))
            ))
        );
        assert_eq!(
            parse_expression("(byte)(short) A"),
            Ok(cast(ColumnType::Byte, cast(ColumnType::Short, column("A"))))
        );
        assert_eq!(
            parse_expression("-(float) 'a'"),
            Ok(unary(
                UnaryOp::Negate,
                cast(ColumnType::Float, Expr::Literal(Literal::Char(97)))
            ))
        );
        assert_eq!(
            parse_expression("(char) A.length()"),
            Ok(cast(
                ColumnType::Char,
                Expr::MethodCall {
                    receiver: Box::new(column("A")),
                    method: "length".into(),
                    arguments: vec![],
                }
            ))
        );
        for (name, to) in [
            ("String", ColumnType::String),
            ("java.lang.String", ColumnType::String),
            ("Boolean", ColumnType::Boolean),
            ("java.lang.Boolean", ColumnType::Boolean),
            ("Instant", ColumnType::Instant),
            ("java.time.Instant", ColumnType::Instant),
        ] {
            assert_eq!(
                parse_expression(&format!("({name}) A")),
                Ok(cast(to, column("A"))),
                "{name}"
            );
        }
        // A parenthesized name before a binary minus is not a cast.
        assert_eq!(
            parse_expression("(A) - B"),
            Ok(binary(Subtract, column("A"), column("B")))
        );
        // A primitive type before a minus is.
        assert_eq!(
            parse_expression("(int) - B"),
            Ok(cast(ColumnType::Int, unary(UnaryOp::Negate, column("B"))))
        );
    }

    /// Behavior: `QueryLanguageParser#visit(CastExpr)` prints `(boolean) x` as Java's own
    /// cast, which unboxes.
    #[test]
    fn a_cast_to_primitive_boolean_is_an_unboxing_ternary() {
        assert_eq!(
            parse_expression("(boolean) A"),
            Ok(ternary(
                column("A"),
                Expr::Literal(Literal::Boolean(true)),
                Expr::Literal(Literal::Boolean(false))
            ))
        );
    }

    #[test]
    fn method_calls_chain_from_the_left() {
        let call = |receiver: Expr, method: &str, arguments: Vec<Expr>| Expr::MethodCall {
            receiver: Box::new(receiver),
            method: method.into(),
            arguments,
        };
        assert_eq!(
            parse_expression("Sym.startsWith(`A`)"),
            Ok(call(
                column("Sym"),
                "startsWith",
                vec![Expr::Literal(Literal::String("A".into()))]
            ))
        );
        assert_eq!(
            parse_expression("Sym.trim().substring(1, N + 1).length()"),
            Ok(call(
                call(
                    call(column("Sym"), "trim", vec![]),
                    "substring",
                    vec![int(1), binary(BinaryOp::Add, column("N"), int(1))]
                ),
                "length",
                vec![]
            ))
        );
        assert_eq!(
            parse_expression("-`ab`.length()"),
            Ok(unary(
                UnaryOp::Negate,
                call(
                    Expr::Literal(Literal::String("ab".into())),
                    "length",
                    vec![]
                )
            ))
        );
        assert_eq!(
            parse_expression("(A + B).equals(C)"),
            Ok(call(
                binary(BinaryOp::Add, column("A"), column("B")),
                "equals",
                vec![column("C")]
            ))
        );
    }

    #[test]
    fn indexing_a_column_array_reads_another_row() {
        assert_eq!(
            parse_expression("A_[i - 1] + 1"),
            Ok(binary(
                BinaryOp::Add,
                Expr::ColumnArrayAccess {
                    column: "A".into(),
                    position: Box::new(binary(
                        BinaryOp::Subtract,
                        Expr::RowVariable(RowVariable::I),
                        int(1)
                    )),
                },
                int(1)
            ))
        );
        // Without an index the parser cannot tell `A_` from a column of that name.
        assert_eq!(parse_expression("A_"), Ok(column("A_")));
        assert!(matches!(
            parse_expression("A_[1"),
            Err(FormulaError::Syntax(_))
        ));
    }

    #[test]
    fn constructs_outside_the_subset_are_named() {
        let cases = [
            ("(Integer) A", "cast to Integer"),
            ("(java.math.BigDecimal) A", "cast to java.math.BigDecimal"),
            ("(int[]) A", "cast to int[]"),
            ("Math.PI", "field access .PI"),
            ("A[0]", "array access [...]"),
            ("f(A)[0]", "array access [...]"),
            ("_[0]", "array access [...]"),
            ("new String(`a`)", "object creation (new)"),
            ("A == '2024-01-01'", "time literal '2024-01-01'"),
        ];
        for (text, construct) in cases {
            assert_eq!(
                parse_expression(text),
                Err(FormulaError::Unsupported(construct.into())),
                "{text}"
            );
        }
    }

    /// Behavior: `QueryLanguageParser#getOperatorName` and `#visit(UnaryExpr)` throw for
    /// these, so they are errors in Java too.
    #[test]
    fn operators_java_refuses_are_syntax_errors() {
        for text in [
            "A << 1",
            "A >> 1",
            "A >>> 1",
            "+A",
            "~A",
            "A++",
            "--A",
            "A instanceof String",
            "A -> A",
            "A::b",
        ] {
            assert!(
                matches!(parse_expression(text), Err(FormulaError::Syntax(_))),
                "{text}"
            );
        }
    }

    #[test]
    fn malformed_formulas_are_syntax_errors() {
        for text in [
            "",
            "A +",
            "(A + B",
            "A B",
            "A + * B",
            "A)",
            "isNull(A",
            "isNull(A,)",
            ", A",
            "A .",
        ] {
            assert!(
                matches!(parse_expression(text), Err(FormulaError::Syntax(_))),
                "{text:?} gave {:?}",
                parse_expression(text)
            );
        }
    }

    /// Behavior: the patterns in `SelectColumnFactory`.
    #[test]
    fn assignments_name_a_column_and_an_expression() {
        assert_eq!(
            parse_assignment("Mid = (Bid + Ask) / 2"),
            Ok(ColumnAssignment {
                name: "Mid".into(),
                expr: binary(
                    BinaryOp::Divide,
                    binary(BinaryOp::Add, column("Bid"), column("Ask")),
                    int(2)
                ),
            })
        );
        assert_eq!(
            parse_assignment("  X=Y "),
            Ok(ColumnAssignment {
                name: "X".into(),
                expr: column("Y"),
            })
        );
        assert_eq!(
            parse_assignment(" Sym "),
            Ok(ColumnAssignment {
                name: "Sym".into(),
                expr: column("Sym"),
            })
        );
        assert_eq!(
            parse_assignment("Flag = A = B"),
            Ok(ColumnAssignment {
                name: "Flag".into(),
                expr: binary(BinaryOp::Equal, column("A"), column("B")),
            })
        );
        for text in [
            "", "= 1", "1X = 2", "X =", "X == Y", "X + 1", "X Y", "X = Y +",
        ] {
            assert!(
                matches!(parse_assignment(text), Err(FormulaError::Syntax(_))),
                "{text:?} gave {:?}",
                parse_assignment(text)
            );
        }
    }
}
