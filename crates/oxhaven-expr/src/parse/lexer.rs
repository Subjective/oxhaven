//! Splits formula text into tokens, following Java's lexical grammar plus the formula
//! language's own rewrites: backtick strings and single `=` as equality.

use super::instant::{time_literals, TimeLiteral, TimeValue};
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
/// longer than one character is a time literal, not a char literal), which Java applies to
/// the raw text first.
pub(crate) fn tokenize(text: &str) -> Result<Vec<Spanned>, FormulaError> {
    let time_literals = time_literals(text);
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
            // Java has replaced a time literal before it looks for string literals, so one
            // that overlaps a string changes the string.
            if let Some(literal) = overlapping(&time_literals, offset, offset + end) {
                return Err(match &literal.value {
                    Err(error @ FormulaError::Syntax(_)) => error.clone(),
                    _ => unsupported("time literal inside a string literal"),
                });
            }
            tokens.push((Token::Literal(Literal::String(value)), offset));
            rest = &rest[end..];
        } else if c == '\'' {
            if let Some(literal) = time_literals.iter().find(|l| l.start == offset) {
                // Java replaces the literal by a variable it sets with `parseDuration`
                // or `parsePeriod`; here the literal becomes that call.
                let call = |function: &str| {
                    let content = text[literal.start + 1..literal.end - 1].to_string();
                    [
                        Token::Identifier(function.to_string()),
                        Token::Symbol("("),
                        Token::Literal(Literal::String(content)),
                        Token::Symbol(")"),
                    ]
                };
                match literal.value.clone()? {
                    TimeValue::Instant(nanos) => {
                        tokens.push((Token::Literal(Literal::Instant(nanos)), offset));
                    }
                    TimeValue::Duration => {
                        tokens.extend(call("parseDuration").map(|token| (token, offset)));
                    }
                    TimeValue::Period => {
                        tokens.extend(call("parsePeriod").map(|token| (token, offset)));
                    }
                    TimeValue::Zone => {
                        tokens.extend(call("parseTimeZone").map(|token| (token, offset)));
                    }
                }
                rest = &text[literal.end..];
                continue;
            }
            let (value, end) = char_literal(rest)?;
            if let Some(literal) = overlapping(&time_literals, offset, offset + end) {
                literal.value.clone()?;
                return Err(unsupported(
                    "time literal that starts inside a char literal",
                ));
            }
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

/// The first time literal with a quote in `start..end`.
fn overlapping(literals: &[TimeLiteral], start: usize, end: usize) -> Option<&TimeLiteral> {
    literals
        .iter()
        .find(|l| (start..end).contains(&l.start) || (start..end).contains(&(l.end - 1)))
}

/// Length of the numeric literal at the start of `text`: letters, digits, underscores and
/// dots, plus a sign that directly follows an exponent marker, which is `e` in a decimal
/// literal and `p` in a hexadecimal one.
fn number_end(text: &str) -> usize {
    let hex = text.starts_with("0x") || text.starts_with("0X");
    let mut previous = ' ';
    for (i, c) in text.char_indices() {
        let marker = if hex {
            matches!(previous, 'p' | 'P')
        } else {
            matches!(previous, 'e' | 'E')
        };
        let exponent_sign = (c == '+' || c == '-') && marker;
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
            return hex_float(&digits, text);
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
        return octal(body, long, text);
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

/// A literal with a leading zero. The formula parser reads the digits as decimal, only to
/// choose between `int` and `long`, and javac then compiles the same digits as octal.
///
/// Behavior: `QueryLanguageParser#visit(IntegerLiteralExpr)`, which appends `L` when
/// `Long.parseLong` of the digits is outside the `int` range and otherwise prints the
/// literal as written; and the Java Language Specification section 3.10.1, by which an octal
/// literal is the bit pattern of its digits and may not have more bits than its type.
fn octal(body: &str, long: bool, text: &str) -> Result<Literal, FormulaError> {
    let bits = u64::from_str_radix(body, 8)
        .map_err(|_| syntax(format!("invalid octal integer literal {text}")))?;
    if long {
        return Ok(Literal::Long(bits as i64));
    }
    // `Long.parseLong` throws beyond the long range.
    let decimal: i64 = body
        .parse()
        .map_err(|_| syntax(format!("integer literal {text} is out of range")))?;
    Ok(match i32::try_from(decimal) {
        // At most ten significant octal digits, which is 30 bits.
        Ok(_) => Literal::Int(bits as i32),
        Err(_) => Literal::Long(bits as i64),
    })
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

/// A hexadecimal floating point literal, `0x1.8p1`: the hexadecimal digits times two to the
/// power of the decimal exponent, rounded once to the nearest value of the type.
///
/// Behavior: the Java Language Specification section 3.10.2, and
/// `QueryLanguageParser#visit(DoubleLiteralExpr)` for the type, as for a decimal literal.
/// Two shapes are unsupported because this reading could round them twice: more than 16
/// significant hexadecimal digits, and a value below the smallest normal value of the type.
fn hex_float(digits: &str, text: &str) -> Result<Literal, FormulaError> {
    let invalid = || syntax(format!("invalid numeric literal {text}"));
    let refused = || unsupported(format!("hexadecimal floating point literal {text}"));
    let Some(marker) = digits.find(['p', 'P']) else {
        return Err(invalid());
    };
    let (mantissa, exponent) = (&digits[2..marker], &digits[marker + 1..]);
    let (exponent, is_float) = match exponent.as_bytes().last() {
        Some(b'f') => (&exponent[..exponent.len() - 1], true),
        // The parser types this as double while javac compiles it as float.
        Some(b'F') => {
            return Err(unsupported(format!(
                "float literal with an uppercase F suffix {text}"
            )))
        }
        Some(b'd' | b'D') => (&exponent[..exponent.len() - 1], false),
        _ => (exponent, false),
    };
    let (negative, exponent) = match exponent.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, exponent.strip_prefix('+').unwrap_or(exponent)),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let hex = |part: &str| part.bytes().all(|b| b.is_ascii_hexdigit());
    if whole.len() + fraction.len() == 0
        || !hex(whole)
        || !hex(fraction)
        || exponent.is_empty()
        || !exponent.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }

    let significant = format!("{whole}{fraction}");
    let significant = significant.trim_start_matches('0');
    if significant.is_empty() {
        return Ok(if is_float {
            Literal::Float(0.0)
        } else {
            Literal::Double(0.0)
        });
    }
    if significant.len() > 16 {
        return Err(refused());
    }
    let bits = u64::from_str_radix(significant, 16).map_err(|_| invalid())?;
    // The value is `bits` times two to the power of `scale`.
    let exponent: i64 = exponent.parse().map_err(|_| refused())?;
    let exponent = if negative { -exponent } else { exponent };
    let scale = exponent
        .checked_sub(4 * fraction.len() as i64)
        .ok_or_else(refused)?;

    // The conversion of the integer rounds to nearest, ties to even, as javac does. The
    // power of two then only moves the exponent, which is exact for a normal value.
    let too_large = || syntax(format!("floating point literal {text} is too large"));
    if is_float {
        let rounded = (bits as f32).to_bits();
        let power = i64::from(rounded >> 23) - 127 + scale;
        if power > 127 {
            return Err(too_large());
        }
        if power < -126 {
            return Err(refused());
        }
        let field = u32::try_from(power + 127).map_err(|_| refused())?;
        Ok(Literal::Float(f32::from_bits(
            (rounded & 0x007f_ffff) | (field << 23),
        )))
    } else {
        let rounded = (bits as f64).to_bits();
        let power = (rounded >> 52) as i64 - 1023 + scale;
        if power > 1023 {
            return Err(too_large());
        }
        if power < -1022 {
            return Err(refused());
        }
        let field = u64::try_from(power + 1023).map_err(|_| refused())?;
        Ok(Literal::Double(f64::from_bits(
            (rounded & 0x000f_ffff_ffff_ffff) | (field << 52),
        )))
    }
}

/// Reads a string literal that starts at the beginning of `text` with `quote`, and returns
/// its value and its length in the source.
///
/// Behavior: `QueryLanguageParser#convertBackticks`: inside backticks a double quote stands
/// for itself, and a backslash escapes as it does in a Java string. The escapes are those of
/// the Java Language Specification section 3.10.7, and `\uXXXX` is the Unicode escape of
/// section 3.3, which javac translates before it reads the literal.
fn string(text: &str, quote: char) -> Result<(String, usize), FormulaError> {
    // UTF-16, because an escape can spell one half of a surrogate pair.
    let mut units: Vec<u16> = Vec::new();
    fn push(units: &mut Vec<u16>, c: char) {
        units.extend(c.encode_utf16(&mut [0; 2]).iter());
    }
    let mut chars = text.char_indices().skip(1).peekable();
    while let Some((at, c)) = chars.next() {
        match c {
            c if c == quote => {
                let value = String::from_utf16(&units)
                    .map_err(|_| unsupported("unpaired surrogate in a string literal"))?;
                return Ok((value, at + c.len_utf8()));
            }
            '\n' | '\r' => break,
            '\\' => {
                let Some((_, escaped)) = chars.next() else {
                    break;
                };
                match escaped {
                    'b' => push(&mut units, '\u{8}'),
                    't' => push(&mut units, '\t'),
                    'n' => push(&mut units, '\n'),
                    'f' => push(&mut units, '\u{c}'),
                    'r' => push(&mut units, '\r'),
                    '"' => push(&mut units, '"'),
                    '\'' => push(&mut units, '\''),
                    '\\' => push(&mut units, '\\'),
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
                        push(&mut units, char::from_u32(code).unwrap_or('\0'));
                    }
                    // The formula parser takes one `u` only, although Java takes several.
                    'u' => {
                        let mut unit = 0_u16;
                        for _ in 0..4 {
                            let digit = chars
                                .next()
                                .and_then(|(_, c)| c.to_digit(16))
                                .ok_or_else(|| syntax("invalid unicode escape"))?;
                            unit = unit * 16 + digit as u16;
                        }
                        // javac sees the character itself, where the formula parser saw an
                        // escape: the two disagree on where the literal ends.
                        if matches!(unit, 0x22 | 0x5c | 0x0a | 0x0d) {
                            return Err(unsupported(
                                "unicode escape of a quote, backslash or line break",
                            ));
                        }
                        units.push(unit);
                    }
                    // Its meaning depends on the Java version that compiles the formula.
                    's' => return Err(unsupported("\\s escape in a string literal")),
                    other => {
                        return Err(syntax(format!(
                            "invalid escape \\{other} in a string literal"
                        )))
                    }
                }
            }
            c => push(&mut units, c),
        }
    }
    Err(syntax("unterminated string literal"))
}

/// Reads a char literal at the start of `text`, and returns the char's UTF-16 code unit and
/// the literal's length in the source.
///
/// Behavior: `TimeLiteralReplacedExpression#convertExpression` reads single-quoted text
/// longer than one `char` as a time literal, so the only char literals that reach the Java
/// parser hold one character, or are `'\''`, whose first two quotes pair up around the
/// backslash.
fn char_literal(text: &str) -> Result<(u16, usize), FormulaError> {
    let mut units = [0_u16; 2];
    match text[1..].chars().next() {
        None | Some('\n' | '\r') => Err(syntax("unterminated char literal")),
        Some('\'') => Err(syntax("empty char literal")),
        Some('\\') if text[2..].starts_with("''") => Ok((u16::from(b'\''), 4)),
        Some('\\') => Err(syntax("invalid escape in a char literal")),
        Some(c) => {
            let end = 1 + c.len_utf8();
            match c.encode_utf16(&mut units) {
                [unit] if text[end..].starts_with('\'') => Ok((*unit, end + 1)),
                _ => Err(syntax("unterminated char literal")),
            }
        }
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
        // Long.parseLong throws for these digits read as decimal.
        assert!(is_syntax(literal("01777777777777777777777")));
    }

    /// Expected types follow `QueryLanguageParser#visit(IntegerLiteralExpr)`, which reads the
    /// digits as decimal; expected values are javac's, which reads them as octal.
    #[test]
    fn a_leading_zero_is_typed_as_decimal_and_valued_as_octal() {
        let cases = [
            ("017", Literal::Int(15)),
            ("00", Literal::Int(0)),
            ("0_7", Literal::Int(7)),
            ("000000000017", Literal::Int(15)),
            ("017L", Literal::Long(15)),
            // 1777777777 as decimal is an int, so javac compiles an int.
            ("01777777777", Literal::Int(0o1777777777)),
            // 17777777777 as decimal is beyond int, so the literal becomes a long.
            ("017777777777", Literal::Long(0o17777777777)),
            ("037777777777", Literal::Long(0xFFFF_FFFF)),
            ("01777777777777777777777L", Literal::Long(-1)),
            ("01000000000000000000000L", Literal::Long(i64::MIN)),
        ];
        for (text, expected) in cases {
            assert_eq!(literal(text), Ok(expected), "{text}");
        }
        assert!(is_syntax(literal("018")));
        assert!(is_syntax(literal("02147483647")));
        assert!(is_syntax(literal("09L")));
        assert!(is_syntax(literal("02000000000000000000000L")));
        // A floating point literal may start with zero.
        assert_eq!(literal("09.5"), Ok(Literal::Double(9.5)));
        assert_eq!(literal("017f"), Ok(Literal::Float(17.0)));
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
        assert!(is_unsupported(literal("0x1.8p1F")));
    }

    #[test]
    fn hexadecimal_floating_literals_are_digits_times_a_power_of_two() {
        for (text, expected) in [
            ("0x1.8p1", Literal::Double(3.0)),
            ("0x1.8p1d", Literal::Double(3.0)),
            ("0x1.8p1f", Literal::Float(3.0)),
            ("0X.8P+1", Literal::Double(1.0)),
            ("0x10p-4", Literal::Double(1.0)),
            ("0x1.p0", Literal::Double(1.0)),
            ("0x0p0", Literal::Double(0.0)),
            ("0x0.0p99999f", Literal::Float(0.0)),
            ("0x1.fffffffffffffp1023", Literal::Double(f64::MAX)),
            ("0x1p-1022", Literal::Double(f64::MIN_POSITIVE)),
            ("0x1.fffffep127f", Literal::Float(f32::MAX)),
            ("0x1p-126f", Literal::Float(f32::MIN_POSITIVE)),
            // 2^53 + 1 is a tie, which rounds to the even 2^53.
            (
                "0x20000000000001p0",
                Literal::Double(9_007_199_254_740_992.0),
            ),
            (
                "0x20000000000003p0",
                Literal::Double(9_007_199_254_740_996.0),
            ),
            // 2^24 + 1 as a float, likewise.
            ("0x1000001p0f", Literal::Float(16_777_216.0)),
            ("0x1000003p0f", Literal::Float(16_777_220.0)),
            // Rounding up carries into the exponent.
            (
                "0xffffffffffffffffp0",
                Literal::Double(18_446_744_073_709_551_616.0),
            ),
        ] {
            assert_eq!(literal(text), Ok(expected), "{text}");
        }
        for text in [
            "0x1.8",
            "0x.p1",
            "0xp1",
            "0x1p",
            "0x1p+",
            "0x1.8p1.0",
            "0x1.2.3p1",
        ] {
            assert!(is_syntax(literal(text)), "{text}");
        }
        // Too large for the type.
        assert!(is_syntax(literal("0x1p1024")));
        assert!(is_syntax(literal("0x1p128f")));
        assert!(is_syntax(literal("0xffffffffffffffffp960")));
        // Subnormal, or more digits than this reading rounds exactly.
        assert!(is_unsupported(literal("0x1p-1023")));
        assert!(is_unsupported(literal("0x1p-127f")));
        assert!(is_unsupported(literal("0x1.00000000000000001p0")));
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
        assert!(is_unsupported(literal("`a\\sb`")));
    }

    /// Behavior: `TimeLiteralReplacedExpression#convertExpression` pairs single quotes over
    /// the whole formula, string literals included.
    #[test]
    fn a_single_quote_in_a_string_is_a_character_unless_it_pairs_into_a_time_literal() {
        let string = |value: &str| Ok(Literal::String(value.into()));
        assert_eq!(literal("`it's`"), string("it's"));
        assert_eq!(literal("`'a'`"), string("'a'"));
        assert_eq!(literal("`''`"), string("''"));
        assert_eq!(literal("\"it\\'s\""), string("it's"));
        // Java replaces the quoted text with a variable name, inside the string.
        assert_eq!(
            literal("`'2024-01-01 UTC'`"),
            Err(FormulaError::Unsupported(
                "time literal inside a string literal".into()
            ))
        );
        assert!(is_unsupported(literal("`'PT1H'`")));
        // Java fails to read the quoted text as a time literal.
        assert!(is_syntax(literal("`'caf\u{e9}s'`")));
        assert!(is_syntax(tokenize("`it's` + `it's`")));
    }

    /// Behavior: the Java Language Specification section 3.3.
    #[test]
    fn unicode_escapes_in_strings_are_utf16_code_units() {
        let string = |value: &str| Ok(Literal::String(value.into()));
        assert_eq!(literal("`\\u0041b`"), string("Ab"));
        assert_eq!(literal("`\\u00e9`"), string("\u{e9}"));
        assert!(is_syntax(literal("`\\uu00e9`")));
        assert_eq!(literal("`\\ud83d\\uDE00`"), string("\u{1F600}"));
        // An escaped backslash does not start a unicode escape.
        assert_eq!(literal("`\\\\u0041`"), string("\\u0041"));
        assert!(is_unsupported(literal("`\\ud83d`")));
        assert!(is_unsupported(literal("`\\ude00\\ud83d`")));
        assert!(is_unsupported(literal("`\\u0022`")));
        assert!(is_unsupported(literal("`\\u005c`")));
        assert!(is_unsupported(literal("`\\u000a`")));
        assert!(is_syntax(literal("`\\u12`")));
        assert!(is_syntax(literal("`\\u12g4`")));
    }

    /// Behavior: `TimeLiteralReplacedExpression#convertExpression`.
    #[test]
    fn single_quotes_hold_one_char_or_a_time_literal() {
        assert_eq!(literal("'a'"), Ok(Literal::Char(97)));
        assert_eq!(literal("'='"), Ok(Literal::Char(61)));
        assert_eq!(literal("'\u{e9}'"), Ok(Literal::Char(0xe9)));
        assert_eq!(literal("'`'"), Ok(Literal::Char(96)));
        assert_eq!(literal("'\"'"), Ok(Literal::Char(34)));
        assert_eq!(literal("'\\''"), Ok(Literal::Char(39)));
        assert_eq!(
            literal("'2023-11-14T22:13:20 UTC'"),
            Ok(Literal::Instant(1_700_000_000_000_000_000))
        );
        assert_eq!(
            literal("'2024-01-01T00:00:00 NY'"),
            Err(FormulaError::Unsupported(
                "time literal '2024-01-01T00:00:00 NY'".into()
            ))
        );
        // Longer than one char, so Java tries these as time literals and fails.
        assert!(is_syntax(literal("'\\n'")));
        assert!(is_syntax(literal("'\\\\'")));
        assert!(is_syntax(literal("'\\u0041'")));
        assert!(is_syntax(literal("'\u{1F600}'")));
        assert!(is_syntax(literal("''")));
        assert!(is_syntax(literal("'a")));
        assert!(is_syntax(literal("'\\'")));
        // The third quote of the apostrophe literal opens a time literal.
        assert!(is_unsupported(tokenize("'\\'' + X + 'a'")));
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
