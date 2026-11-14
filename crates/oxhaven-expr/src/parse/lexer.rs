//! Splits formula text into tokens, following Java's lexical grammar plus the formula
//! language's own rewrites: backtick strings and single `=` as equality.

use oxhaven_contracts::expr::{FormulaError, Literal};

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Token {
    Identifier(String),
    Literal(Literal),
    /// An operator or separator, by its spelling.
    Symbol(&'static str),
}

/// A token and the byte offset where it starts.
pub(crate) type Spanned = (Token, usize);

/// Operators and separators, longest first so that the first match is the longest match.
const SYMBOLS: &[&str] = &[
    ">>>=", ">>>", "<<=", ">>=", "<<", ">>", "==", "!=", "<=", ">=", "&&", "||", "++", "--", "+=",
    "-=", "*=", "/=", "%=", "&=", "|=", "^=", "->", "::", "+", "-", "*", "/", "%", "=", "<", ">",
    "!", "&", "|", "^", "~", "(", ")", "[", "]", "{", "}", ",", ".", "?", ":", ";",
];

fn syntax(message: impl Into<String>) -> FormulaError {
    FormulaError::Syntax(message.into())
}

fn unsupported(message: impl Into<String>) -> FormulaError {
    FormulaError::Unsupported(message.into())
}

/// Behavior: `io.deephaven.engine.table.impl.lang.QueryLanguageParser#convertBackticks` (a
/// backtick string is a string literal), `#convertSingleEquals` (a lone `=` is `==`, which
/// the parser applies by treating both spellings as equality), and
/// `io.deephaven.time.TimeLiteralReplacedExpression#convertExpression` (single-quoted text
/// longer than one character is a time literal, not a char literal).
pub(crate) fn tokenize(text: &str) -> Result<Vec<Spanned>, FormulaError> {
    let mut tokens = Vec::new();
    let mut rest = text;
    while let Some(c) = rest.chars().next() {
        let offset = text.len() - rest.len();
        if c.is_whitespace() {
            rest = &rest[c.len_utf8()..];
        } else if rest.starts_with("//") || rest.starts_with("/*") {
            return Err(unsupported("comment"));
        } else if c.is_ascii_digit()
            || (c == '.' && rest[1..].starts_with(|d: char| d.is_ascii_digit()))
        {
            let end = number_end(rest);
            tokens.push((Token::Literal(number(&rest[..end])?), offset));
            rest = &rest[end..];
        } else if c == '_' || c == '$' || c.is_alphabetic() {
            let end = rest
                .find(|d: char| !(d == '_' || d == '$' || d.is_alphanumeric()))
                .unwrap_or(rest.len());
            tokens.push((Token::Identifier(rest[..end].to_string()), offset));
            rest = &rest[end..];
        } else if c == '"' || c == '`' {
            let (value, end) = string(rest, c)?;
            tokens.push((Token::Literal(Literal::String(value)), offset));
            rest = &rest[end..];
        } else if c == '\'' {
            let (value, end) = char_literal(rest)?;
            tokens.push((Token::Literal(Literal::Char(value)), offset));
            rest = &rest[end..];
        } else if let Some(symbol) = SYMBOLS.iter().find(|s| rest.starts_with(**s)) {
            tokens.push((Token::Symbol(symbol), offset));
            rest = &rest[symbol.len()..];
        } else {
            return Err(syntax(format!(
                "unexpected character '{c}' at offset {offset}"
            )));
        }
    }
    Ok(tokens)
}

/// Length of the numeric literal at the start of `text`: letters, digits, underscores and
/// dots, plus a sign that directly follows a decimal exponent marker.
fn number_end(text: &str) -> usize {
    let hex = text.starts_with("0x") || text.starts_with("0X");
    let mut previous = ' ';
    for (i, c) in text.char_indices() {
        let exponent_sign = (c == '+' || c == '-') && !hex && matches!(previous, 'e' | 'E');
        if !(c.is_ascii_alphanumeric() || c == '_' || c == '.' || exponent_sign) {
            return i;
        }
        previous = c;
    }
    text.len()
}

/// Behavior: `QueryLanguageParser#visit(IntegerLiteralExpr)`, `#visit(LongLiteralExpr)` and
/// `#visit(DoubleLiteralExpr)`, and the Java Language Specification section 3.10 for what
/// javac then accepts.
fn number(text: &str) -> Result<Literal, FormulaError> {
    let invalid = || syntax(format!("invalid numeric literal {text}"));
    if text.starts_with('_') || text.ends_with('_') {
        return Err(invalid());
    }
    let digits: String = text.chars().filter(|&c| c != '_').collect();
    let lower = digits.to_ascii_lowercase();

    let radix = if lower.starts_with("0x") {
        Some(16)
    } else if lower.starts_with("0b") {
        Some(2)
    } else {
        None
    };
    if let Some(radix) = radix {
        if radix == 16 && lower.contains(['.', 'p']) {
            return Err(unsupported(format!(
                "hexadecimal floating point literal {text}"
            )));
        }
        let (body, long) = match lower[2..].strip_suffix('l') {
            Some(body) => (body, true),
            None => (&lower[2..], false),
        };
        let bits = u64::from_str_radix(body, radix).map_err(|_| invalid())?;
        return if long {
            // javac reads a hexadecimal or binary long literal as a 64-bit pattern.
            Ok(Literal::Long(bits as i64))
        } else {
            integer(bits, text)
        };
    }

    let floating = lower.contains(['.', 'e']) || lower.ends_with(['f', 'd']);
    if floating {
        return float(&digits, text);
    }

    let (body, long) = match lower.strip_suffix('l') {
        Some(body) => (body, true),
        None => (lower.as_str(), false),
    };
    if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    if body.len() > 1 && body.starts_with('0') {
        // The parser reads this as decimal to choose int or long, and javac then compiles
        // it as octal.
        return Err(unsupported(format!("octal integer literal {text}")));
    }
    let value: u64 = body.parse().map_err(|_| invalid())?;
    if long {
        // javac allows 9223372036854775808L only as the operand of unary minus, and the
        // formula language turns that minus into a method call.
        i64::try_from(value)
            .map(Literal::Long)
            .map_err(|_| syntax(format!("long literal {text} is out of range")))
    } else {
        integer(value, text)
    }
}

/// An integer literal without a suffix is an `int` when it fits and a `long` otherwise.
///
/// Behavior: `QueryLanguageParser#visit(IntegerLiteralExpr)`, which reads the digits with
/// `Long.parseLong` and so never yields a negative value.
fn integer(value: u64, text: &str) -> Result<Literal, FormulaError> {
    let value = i64::try_from(value)
        .map_err(|_| syntax(format!("integer literal {text} is out of range")))?;
    Ok(match i32::try_from(value) {
        Ok(int) => Literal::Int(int),
        Err(_) => Literal::Long(value),
    })
}

/// Behavior: `QueryLanguageParser#visit(DoubleLiteralExpr)`: a literal that ends in a
/// lowercase `f` is a `float`, and anything else is a `double`.
fn float(digits: &str, text: &str) -> Result<Literal, FormulaError> {
    let invalid = || syntax(format!("invalid numeric literal {text}"));
    let (body, is_float) = match digits.as_bytes()[digits.len() - 1] {
        b'f' => (&digits[..digits.len() - 1], true),
        b'F' => {
            // The parser types this as double while javac compiles it as float.
            return Err(unsupported(format!(
                "float literal with an uppercase F suffix {text}"
            )));
        }
        b'd' | b'D' => (&digits[..digits.len() - 1], false),
        _ => (digits, false),
    };

    // Digits [. Digits] [e [sign] Digits], with at least one digit before the exponent.
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let mantissa_ok = mantissa.bytes().any(|b| b.is_ascii_digit())
        && mantissa.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && mantissa.bytes().filter(|&b| b == b'.').count() <= 1;
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && e.bytes().all(|b| b.is_ascii_digit())
    });
    if !mantissa_ok || !exponent_ok {
        return Err(invalid());
    }

    // javac rejects a literal that rounds to infinity, or to zero when it is not zero.
    let nonzero = mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b));
    let (is_infinite, is_zero, literal) = if is_float {
        let value: f32 = body.parse().map_err(|_| invalid())?;
        (value.is_infinite(), value == 0.0, Literal::Float(value))
    } else {
        let value: f64 = body.parse().map_err(|_| invalid())?;
        (value.is_infinite(), value == 0.0, Literal::Double(value))
    };
    if is_infinite {
        return Err(syntax(format!(
            "floating point literal {text} is too large"
        )));
    }
    if is_zero && nonzero {
        return Err(syntax(format!(
            "floating point literal {text} is too small"
        )));
    }
    Ok(literal)
}

/// Reads a string literal that starts at the beginning of `text` with `quote`, and returns
/// its value and its length in the source.
///
/// Behavior: `QueryLanguageParser#convertBackticks`: inside backticks a double quote stands
/// for itself, and a backslash escapes as it does in a Java string.
fn string(text: &str, quote: char) -> Result<(String, usize), FormulaError> {
    let mut value = String::new();
    let mut chars = text.char_indices().skip(1).peekable();
    while let Some((at, c)) = chars.next() {
        match c {
            c if c == quote => return Ok((value, at + c.len_utf8())),
            '\n' | '\r' => break,
            '\'' => {
                // Java scans the whole formula for single-quoted time literals before it
                // parses, string literals included.
                return Err(unsupported("single quote inside a string literal"));
            }
            '\\' => {
                let Some((_, escaped)) = chars.next() else {
                    break;
                };
                value.push(match escaped {
                    'b' => '\u{8}',
                    't' => '\t',
                    'n' => '\n',
                    'f' => '\u{c}',
                    'r' => '\r',
                    '"' => '"',
                    '\\' => '\\',
                    '0'..='7' => {
                        // Up to three octal digits, and only two when the first exceeds 3.
                        let mut code = escaped.to_digit(8).unwrap_or(0);
                        let limit = if escaped <= '3' { 2 } else { 1 };
                        for _ in 0..limit {
                            match chars.peek().and_then(|(_, d)| d.to_digit(8)) {
                                Some(d) => {
                                    code = code * 8 + d;
                                    chars.next();
                                }
                                None => break,
                            }
                        }
                        char::from_u32(code).unwrap_or('\0')
                    }
                    '\'' => return Err(unsupported("single quote inside a string literal")),
                    'u' => return Err(unsupported("unicode escape in a string literal")),
                    's' => return Err(unsupported("\\s escape in a string literal")),
                    other => {
                        return Err(syntax(format!(
                            "invalid escape \\{other} in a string literal"
                        )))
                    }
                });
            }
            c => value.push(c),
        }
    }
    Err(syntax("unterminated string literal"))
}

/// Reads a single-quoted literal at the start of `text`, and returns the char's UTF-16 code
/// unit and the literal's length in the source.
///
/// Behavior: `TimeLiteralReplacedExpression#convertExpression` leaves single-quoted text of
/// at most one character to the Java parser as a char literal, and reads everything longer
/// as a time literal, which rules out escape sequences.
fn char_literal(text: &str) -> Result<(u16, usize), FormulaError> {
    let Some(close) = text[1..].find('\'') else {
        return Err(syntax("unterminated char literal"));
    };
    let content = &text[1..1 + close];
    let mut units = content.encode_utf16();
    match (units.next(), units.next()) {
        (None, _) => Err(syntax("empty char literal")),
        (Some(unit), None) if content != "\\" => Ok((unit, close + 2)),
        _ => Err(unsupported(format!(
            "time literal or escaped char literal '{content}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn literal(text: &str) -> Result<Literal, FormulaError> {
        let tokens = tokenize(text)?;
        match tokens.as_slice() {
            [(Token::Literal(literal), 0)] => Ok(literal.clone()),
            other => panic!("{text} did not lex as one literal: {other:?}"),
        }
    }

    fn is_syntax<T: std::fmt::Debug>(result: Result<T, FormulaError>) -> bool {
        matches!(result, Err(FormulaError::Syntax(_)))
    }

    fn is_unsupported<T: std::fmt::Debug>(result: Result<T, FormulaError>) -> bool {
        matches!(result, Err(FormulaError::Unsupported(_)))
    }

    /// Expected values follow `QueryLanguageParser#visit(IntegerLiteralExpr)`: digits are read
    /// with `Long.parseLong`, and a value outside the `int` range becomes a `long` literal.
    #[test]
    fn integer_literals_are_int_when_they_fit_and_long_otherwise() {
        let cases = [
            ("0", Literal::Int(0)),
            ("2147483647", Literal::Int(i32::MAX)),
            ("2147483648", Literal::Long(2_147_483_648)),
            ("1_000_000", Literal::Int(1_000_000)),
            ("9223372036854775807", Literal::Long(i64::MAX)),
            ("0x7fffffff", Literal::Int(i32::MAX)),
            // Not -1: the parser reads hexadecimal digits as a positive long.
            ("0xFFFFFFFF", Literal::Long(4_294_967_295)),
            ("0X10", Literal::Int(16)),
            ("0b101", Literal::Int(5)),
            ("0B1111_0000", Literal::Int(240)),
            ("7L", Literal::Long(7)),
            ("7l", Literal::Long(7)),
            ("0xFFFFFFFFFFFFFFFFL", Literal::Long(-1)),
            ("0x8000000000000000L", Literal::Long(i64::MIN)),
        ];
        for (text, expected) in cases {
            assert_eq!(literal(text), Ok(expected), "{text}");
        }
    }

    #[test]
    fn integer_literals_out_of_range_are_rejected() {
        // Long.parseLong throws for these.
        assert!(is_syntax(literal("9223372036854775808")));
        assert!(is_syntax(literal("0xFFFFFFFFFFFFFFFF")));
        // javac rejects this outside the operand position of unary minus.
        assert!(is_syntax(literal("9223372036854775808L")));
        assert!(is_syntax(literal("1_")));
        assert!(is_syntax(literal("12abc")));
        assert!(is_unsupported(literal("017")));
        assert!(is_unsupported(literal("017L")));
    }

    /// Expected types follow `QueryLanguageParser#visit(DoubleLiteralExpr)`.
    #[test]
    fn floating_literals_are_double_unless_they_end_in_lowercase_f() {
        let cases = [
            ("1.5", Literal::Double(1.5)),
            ("1.", Literal::Double(1.0)),
            (".5", Literal::Double(0.5)),
            ("1e3", Literal::Double(1000.0)),
            ("1E-3", Literal::Double(0.001)),
            ("2.5e+2", Literal::Double(250.0)),
            ("1d", Literal::Double(1.0)),
            ("1D", Literal::Double(1.0)),
            ("0.1", Literal::Double(0.1)),
            ("1.5f", Literal::Float(1.5)),
            ("1f", Literal::Float(1.0)),
            ("0.1f", Literal::Float(0.1)),
            ("1_0.2_5", Literal::Double(10.25)),
            ("0.0", Literal::Double(0.0)),
            ("0e0f", Literal::Float(0.0)),
        ];
        for (text, expected) in cases {
            assert_eq!(literal(text), Ok(expected), "{text}");
        }
    }

    #[test]
    fn floating_literals_javac_rejects_are_rejected() {
        assert!(is_syntax(literal("1e400")));
        assert!(is_syntax(literal("1e-400")));
        assert!(is_syntax(literal("1e40f")));
        assert!(is_syntax(literal("1e-50f")));
        assert!(is_syntax(literal("1e")));
        assert!(is_syntax(literal("1.2.3")));
        assert!(is_unsupported(literal("1.5F")));
        assert!(is_unsupported(literal("0x1.8p1")));
    }

    /// Behavior: `QueryLanguageParser#convertBackticks`.
    #[test]
    fn strings_use_backticks_or_double_quotes() {
        let cases = [
            ("`AAPL`", "AAPL"),
            ("\"AAPL\"", "AAPL"),
            ("`say \"hi\"`", "say \"hi\""),
            ("\"a`b\"", "a`b"),
            ("`a\\tb\\\\c\\n`", "a\tb\\c\n"),
            ("\"q\\\"q\"", "q\"q"),
            ("`\\101\\0\\477`", "A\0\u{27}7"),
            ("``", ""),
            ("`a = b`", "a = b"),
        ];
        for (text, expected) in cases {
            assert_eq!(
                literal(text),
                Ok(Literal::String(expected.into())),
                "{text}"
            );
        }
        assert!(is_syntax(literal("`abc")));
        assert!(is_syntax(literal("`a\\qb`")));
        assert!(is_syntax(literal("`a\nb`")));
        assert!(is_unsupported(literal("`it's`")));
        assert!(is_unsupported(literal("`\\u0041`")));
    }

    /// Behavior: `TimeLiteralReplacedExpression#convertExpression`.
    #[test]
    fn single_quotes_hold_one_char_or_a_time_literal() {
        assert_eq!(literal("'a'"), Ok(Literal::Char(97)));
        assert_eq!(literal("'='"), Ok(Literal::Char(61)));
        assert_eq!(literal("'\u{e9}'"), Ok(Literal::Char(0xe9)));
        assert!(is_unsupported(literal("'2024-01-01T00:00:00 UTC'")));
        assert!(is_unsupported(literal("'\\n'")));
        assert!(is_unsupported(literal("'\u{1F600}'")));
        assert!(is_syntax(literal("''")));
        assert!(is_syntax(literal("'a")));
    }

    #[test]
    fn symbols_take_the_longest_match() {
        let symbols: Vec<Token> = tokenize("a>=b>>c=d==e")
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect();
        assert_eq!(
            symbols,
            vec![
                Token::Identifier("a".into()),
                Token::Symbol(">="),
                Token::Identifier("b".into()),
                Token::Symbol(">>"),
                Token::Identifier("c".into()),
                Token::Symbol("="),
                Token::Identifier("d".into()),
                Token::Symbol("=="),
                Token::Identifier("e".into()),
            ]
        );
        assert!(is_syntax(tokenize("a # b")));
        assert!(is_unsupported(tokenize("a // b")));
    }

    #[test]
    fn a_number_ends_before_an_operator() {
        let tokens: Vec<Token> = tokenize("1e3-2+0x1e-1")
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect();
        assert_eq!(
            tokens,
            vec![
                // The sign belongs to a decimal exponent only when a digit follows the e.
                Token::Literal(Literal::Double(1000.0)),
                Token::Symbol("-"),
                Token::Literal(Literal::Int(2)),
                Token::Symbol("+"),
                Token::Literal(Literal::Int(0x1e)),
                Token::Symbol("-"),
                Token::Literal(Literal::Int(1)),
            ]
        );
    }
}
