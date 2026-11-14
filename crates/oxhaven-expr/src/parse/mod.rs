//! Formula text to expression tree.
//!
//! The grammar is Java's expression grammar, which is what
//! `io.deephaven.engine.table.impl.lang.QueryLanguageParser` hands to JavaParser. Constructs
//! of that grammar that are not built yet are recognized and reported by name, so adding one
//! means replacing its error with the tree node.

mod lexer;

use crate::functions;
use lexer::{Spanned, Token};
use oxhaven_contracts::expr::{
    BinaryOp, ColumnAssignment, Expr, FormulaError, Literal, RowVariable, UnaryOp,
};

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

    fn expression(&mut self) -> Result<Expr, FormulaError> {
        let condition = self.binary(0)?;
        if self.peek_symbol() == Some("?") {
            return Err(unsupported("ternary operator (?:)"));
        }
        Ok(condition)
    }

    /// Parses binary operators that bind more strongly than `floor`. All of them associate
    /// to the left.
    fn binary(&mut self, floor: u8) -> Result<Expr, FormulaError> {
        let mut left = self.unary()?;
        loop {
            if matches!(self.peek(), Some(Token::Identifier(name)) if name == "instanceof") {
                return Err(unsupported("instanceof"));
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
            Some("(") if self.at_cast() => return Err(unsupported("cast")),
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

    fn postfix(&mut self) -> Result<Expr, FormulaError> {
        let primary = self.primary()?;
        match self.peek_symbol() {
            Some(".") => match (self.peek_at(1), self.peek_at(2)) {
                (Some(Token::Identifier(name)), Some(Token::Symbol("("))) => {
                    Err(unsupported(format!("method call .{name}()")))
                }
                (Some(Token::Identifier(name)), _) => {
                    Err(unsupported(format!("field access .{name}")))
                }
                _ => {
                    self.position += 1;
                    Err(self.unexpected())
                }
            },
            Some("[") => Err(match &primary {
                Expr::Column(name) if name.ends_with('_') => {
                    unsupported(format!("column array access {name}[...]"))
                }
                _ => unsupported("array access [...]"),
            }),
            Some("++") | Some("--") => Err(syntax("increment and decrement are not supported")),
            Some("::") => Err(unsupported("method reference (::)")),
            Some("->") => Err(unsupported("lambda (->)")),
            _ => Ok(primary),
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
                    return self.call(name);
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

    /// Parses the argument list of a call to `function`, whose name has been consumed.
    fn call(&mut self, function: String) -> Result<Expr, FormulaError> {
        if functions::lookup(&function).is_none() {
            return Err(unsupported(format!("function {function}()")));
        }
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
        Ok(Expr::Call {
            function,
            arguments,
        })
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
    fn is_null_is_a_call() {
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
    fn constructs_outside_the_subset_are_named() {
        let cases = [
            ("A > 1 ? B : C", "ternary operator (?:)"),
            ("(int) A", "cast"),
            ("(long)(A + B)", "cast"),
            ("(String) A", "cast"),
            ("(java.lang.String) A", "cast"),
            ("(int[]) A", "cast"),
            ("Sym.startsWith(`A`)", "method call .startsWith()"),
            ("Math.PI", "field access .PI"),
            ("abs(A)", "function abs()"),
            ("A_[i - 1]", "column array access A_[...]"),
            ("A[0]", "array access [...]"),
            ("new String(`a`)", "object creation (new)"),
            ("A instanceof String", "instanceof"),
            (
                "A == '2024-01-01'",
                "time literal or escaped char literal '2024-01-01'",
            ),
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
        for text in ["A << 1", "A >> 1", "A >>> 1", "+A", "~A", "A++", "--A"] {
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
