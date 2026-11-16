//! A JSON record as Java reads it: Jackson's tree of the record, JSON pointers into it, and
//! the conversion of the node a pointer finds to a column's type.
//!
//! Java parses a record with Jackson into a tree (`ObjectMapper#readTree`, set to read every
//! number with a fraction or an exponent as an exact `BigDecimal`) and converts a node with
//! `asInt`, `asLong`, `asDouble`, `asText` or `toString`, which coerce: `true` is 1 in an
//! `int` column, `"12.9"` is 12, an object is 0, and in a `String` column a number is its
//! text and an object its compact JSON. The tree here keeps what those conversions look at:
//! the digits and scale of a number as written, the kind of each node, the order of an
//! object's fields.
//!
//! `docs/KAFKA.md`, "How a JSON field becomes a cell", has the table of conversions.
//!
//! Behavior: io.deephaven.kafka.ingest.JsonNodeUtil

use crate::{not_ported, KafkaError};

/// Jackson's `StreamReadConstraints`: the deepest nesting and the longest number it reads.
const MAX_DEPTH: usize = 1000;
const MAX_NUMBER_LENGTH: usize = 1000;

/// A node of Jackson's tree, or the `MissingNode` a pointer that finds nothing gives.
#[derive(Clone, PartialEq, Debug)]
pub enum Node {
    Missing,
    Null,
    Bool(bool),
    /// A number without fraction and exponent: Jackson's `IntNode`, `LongNode` or
    /// `BigIntegerNode`, by size.
    Integer(Integer),
    /// A number with a fraction or an exponent: Jackson's `DecimalNode`.
    Decimal(Decimal),
    Text(Text),
    Array(Vec<Node>),
    /// The fields in the order of their first appearance; a repeated name keeps its place
    /// and takes the later value, as Jackson's `ObjectNode` does.
    Object(Vec<(String, Node)>),
}

/// An integer as written: its sign and its digits, without leading zeros. Zero has no sign.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Integer {
    negative: bool,
    digits: String,
}

/// A `BigDecimal`: `digits * 10^-scale`, with the scale Java gives the text it was read
/// from (`1.50` has scale 2, `1e3` scale -3). Zero has no sign.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Decimal {
    negative: bool,
    digits: String,
    scale: i32,
}

/// A string. Java's strings are UTF-16 and may hold a surrogate without its pair, which
/// Rust's cannot: such a surrogate is U+FFFD in `value`, and `unpaired` says there was one.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Text {
    value: String,
    unpaired: bool,
    /// The first UTF-16 code unit, as Java's `String#charAt(0)` gives it.
    first: Option<u16>,
}

impl Text {
    #[cfg(test)]
    fn of(value: &str) -> Self {
        Self {
            value: value.to_owned(),
            unpaired: false,
            first: value.encode_utf16().next(),
        }
    }
}

fn parse_failure(error: impl std::fmt::Display) -> KafkaError {
    // Behavior: JsonNodeUtil#makeJsonNode
    KafkaError::Record(format!(
        "io.deephaven.UncheckedDeephavenException: Failed to parse JSON string. ({error})"
    ))
}

/// Reads `text` as `JsonNodeUtil#makeJsonNode` does: one value, and what follows it is not
/// looked at; text without a value is a `MissingNode`.
///
/// Behavior: com.fasterxml.jackson.databind.ObjectMapper#readTree(String), with
/// `JsonNodeFactory.withExactBigDecimals(true)` and `USE_BIG_DECIMAL_FOR_FLOATS`
pub fn parse(text: &str) -> Result<Node, KafkaError> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        text,
        at: 0,
        depth: 0,
    };
    parser.skip_whitespace();
    if parser.at == parser.bytes.len() {
        return Ok(Node::Missing);
    }
    parser.value()
}

struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    at: usize,
    depth: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn fail<T>(&self, what: &str) -> Result<T, KafkaError> {
        Err(parse_failure(format!("{what} at character {}", self.at)))
    }

    /// Jackson takes only these four as white space between tokens.
    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn value(&mut self) -> Result<Node, KafkaError> {
        match self.peek() {
            Some(b'{') => self.container(b'}'),
            Some(b'[') => self.container(b']'),
            Some(b'"') => Ok(Node::Text(self.string()?)),
            Some(b't') => self.word("true", Node::Bool(true)),
            Some(b'f') => self.word("false", Node::Bool(false)),
            Some(b'n') => self.word("null", Node::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            _ => self.fail("expected a value"),
        }
    }

    fn word(&mut self, word: &str, node: Node) -> Result<Node, KafkaError> {
        let end = self.at + word.len();
        let rest = self.bytes.get(end).copied();
        if self.bytes.get(self.at..end) != Some(word.as_bytes())
            || rest.is_some_and(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return self.fail("unrecognized token");
        }
        self.at = end;
        Ok(node)
    }

    fn container(&mut self, close: u8) -> Result<Node, KafkaError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            // Behavior: com.fasterxml.jackson.core.StreamReadConstraints#validateNestingDepth
            return self.fail(&format!(
                "Document nesting depth ({}) exceeds the maximum allowed ({MAX_DEPTH})",
                self.depth
            ));
        }
        self.at += 1;
        let mut fields: Vec<(String, Node)> = Vec::new();
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(close) {
            self.at += 1;
        } else {
            loop {
                self.skip_whitespace();
                if close == b'}' {
                    if self.peek() != Some(b'"') {
                        return self.fail("was expecting double-quote to start field name");
                    }
                    let name = self.string()?;
                    self.skip_whitespace();
                    if self.peek() != Some(b':') {
                        return self.fail("was expecting a colon");
                    }
                    self.at += 1;
                    self.skip_whitespace();
                    let value = self.value()?;
                    // Behavior: com.fasterxml.jackson.databind.node.ObjectNode#replace
                    match fields.iter_mut().find(|(field, _)| *field == name.value) {
                        Some((_, old)) => *old = value,
                        None => fields.push((name.value, value)),
                    }
                } else {
                    items.push(self.value()?);
                }
                self.skip_whitespace();
                match self.peek() {
                    Some(b',') => self.at += 1,
                    Some(b) if b == close => {
                        self.at += 1;
                        break;
                    }
                    _ => return self.fail("was expecting comma or the end of a container"),
                }
            }
        }
        self.depth -= 1;
        Ok(if close == b'}' {
            Node::Object(fields)
        } else {
            Node::Array(items)
        })
    }

    fn hex4(&mut self) -> Result<u16, KafkaError> {
        let digits = self.bytes.get(self.at..self.at + 4);
        let parsed = digits
            .and_then(|d| std::str::from_utf8(d).ok())
            .filter(|d| d.bytes().all(|b| b.is_ascii_hexdigit()))
            .and_then(|d| u16::from_str_radix(d, 16).ok());
        match parsed {
            Some(unit) => {
                self.at += 4;
                Ok(unit)
            }
            None => self.fail("expected a hex-digit for character escape sequence"),
        }
    }

    fn string(&mut self) -> Result<Text, KafkaError> {
        self.at += 1;
        let mut text = Text {
            value: String::new(),
            unpaired: false,
            first: None,
        };
        let push_unit = |text: &mut Text, unit: u16, c: char, unpaired: bool| {
            if text.first.is_none() && text.value.is_empty() {
                text.first = Some(unit);
            }
            text.value.push(c);
            text.unpaired |= unpaired;
        };
        loop {
            let start = self.at;
            while let Some(b) = self.peek() {
                if b == b'"' || b == b'\\' || b < 0x20 {
                    break;
                }
                self.at += 1;
            }
            let run = &self.text[start..self.at];
            if let Some(c) = run.chars().next() {
                let mut units = [0u16; 2];
                push_unit(&mut text, c.encode_utf16(&mut units)[0], c, false);
                text.value.push_str(&run[c.len_utf8()..]);
            }
            match self.peek() {
                None => return self.fail("unexpected end of input in a string"),
                Some(b'"') => {
                    self.at += 1;
                    return Ok(text);
                }
                Some(b'\\') => {
                    self.at += 1;
                    let escaped = self.peek();
                    self.at += 1;
                    let simple = match escaped {
                        Some(b'"') => Some('"'),
                        Some(b'\\') => Some('\\'),
                        Some(b'/') => Some('/'),
                        Some(b'b') => Some('\u{8}'),
                        Some(b'f') => Some('\u{c}'),
                        Some(b'n') => Some('\n'),
                        Some(b'r') => Some('\r'),
                        Some(b't') => Some('\t'),
                        Some(b'u') => None,
                        _ => return self.fail("unrecognized character escape"),
                    };
                    if let Some(c) = simple {
                        push_unit(&mut text, c as u16, c, false);
                        continue;
                    }
                    let unit = self.hex4()?;
                    if (0xD800..0xDC00).contains(&unit)
                        && self.bytes.get(self.at..self.at + 2) == Some(b"\\u")
                    {
                        let save = self.at;
                        self.at += 2;
                        let low = self.hex4()?;
                        if (0xDC00..0xE000).contains(&low) {
                            let code = 0x10000
                                + ((u32::from(unit) - 0xD800) << 10)
                                + (u32::from(low) - 0xDC00);
                            let c = char::from_u32(code).expect("a surrogate pair is a char");
                            push_unit(&mut text, unit, c, false);
                            continue;
                        }
                        self.at = save;
                    }
                    match char::from_u32(u32::from(unit)) {
                        Some(c) => push_unit(&mut text, unit, c, false),
                        None => push_unit(&mut text, unit, '\u{FFFD}', true),
                    }
                }
                Some(_) => {
                    return self.fail(
                        "Illegal unquoted character: has to be escaped using backslash to be \
                         included in string value",
                    )
                }
            }
        }
    }

    fn digits(&mut self) -> &str {
        let start = self.at;
        while self.peek().is_some_and(|b| b.is_ascii_digit()) {
            self.at += 1;
        }
        &self.text[start..self.at]
    }

    /// Behavior: com.fasterxml.jackson.core.json.ReaderBasedJsonParser#_parseNumber,
    /// `StreamReadConstraints#validateIntegerLength` and `#validateFPLength`, and
    /// `java.math.BigDecimal#BigDecimal(String)`, which refuses a scale outside an `int`
    fn number(&mut self) -> Result<Node, KafkaError> {
        let start = self.at;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.at += 1;
        }
        let whole = self.digits().to_owned();
        if whole.is_empty() {
            return self.fail("expected digit (0-9) to follow minus sign");
        }
        if whole.len() > 1 && whole.starts_with('0') {
            return self.fail("Invalid numeric value: Leading zeroes not allowed");
        }
        let mut fraction = None;
        if self.peek() == Some(b'.') {
            self.at += 1;
            let digits = self.digits().to_owned();
            if digits.is_empty() {
                return self.fail("Decimal point not followed by a digit");
            }
            fraction = Some(digits);
        }
        let mut exponent = None;
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.at += 1;
            let sign = match self.peek() {
                Some(b'-') => {
                    self.at += 1;
                    "-"
                }
                Some(b'+') => {
                    self.at += 1;
                    ""
                }
                _ => "",
            };
            let digits = self.digits();
            if digits.is_empty() {
                return self.fail("Exponent indicator not followed by a digit");
            }
            exponent = Some(format!("{sign}{digits}"));
        }
        let length = whole.len()
            + fraction.as_ref().map_or(0, String::len)
            + exponent
                .as_ref()
                .map_or(0, |e| e.trim_start_matches('-').len());
        if length > MAX_NUMBER_LENGTH {
            return self.fail(&format!(
                "Number value length ({length}) exceeds the maximum allowed ({MAX_NUMBER_LENGTH})"
            ));
        }
        if fraction.is_none() && exponent.is_none() {
            let negative = negative && whole != "0";
            return Ok(Node::Integer(Integer {
                negative,
                digits: whole,
            }));
        }
        let written = &self.text[start..self.at];
        let fraction = fraction.unwrap_or_default();
        // A number of at most a thousand digits saturates long before i64 does.
        let exponent: i64 = exponent.map_or(0, |e| e.parse().unwrap_or(i64::MAX));
        let scale = i32::try_from(fraction.len() as i64 - exponent).map_err(|_| {
            KafkaError::Record(format!(
                "java.lang.NumberFormatException: Value \"{written}\" can not be deserialized \
                 as `java.math.BigDecimal`, reason:  Exponent overflow."
            ))
        })?;
        let digits = format!("{whole}{fraction}");
        let digits = match digits.trim_start_matches('0') {
            "" => "0".to_owned(),
            rest => rest.to_owned(),
        };
        let negative = negative && digits != "0";
        Ok(Node::Decimal(Decimal {
            negative,
            digits,
            scale,
        }))
    }
}

/// A JSON pointer, compiled as Jackson compiles one.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pointer {
    text: String,
    segments: Vec<Segment>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
struct Segment {
    name: String,
    /// The array index the segment is, if it is one.
    index: Option<usize>,
}

impl Pointer {
    /// The pointer `text`, which starts with `/` (every pointer Java makes of a spec does).
    /// `~0` is `~` and `~1` is `/`; `~` before anything else is kept as it is, with the
    /// character after it.
    ///
    /// Behavior: com.fasterxml.jackson.core.JsonPointer#compile, #_parseTail,
    /// #_appendEscape and #_parseIndex
    pub fn compile(text: &str) -> Self {
        let mut segments = Vec::new();
        let mut chars = text.strip_prefix('/').unwrap_or(text).chars();
        let mut name = String::new();
        loop {
            match chars.next() {
                None => break,
                Some('/') => segments.push(std::mem::take(&mut name)),
                Some('~') => match chars.next() {
                    Some('0') => name.push('~'),
                    Some('1') => name.push('/'),
                    Some(other) => {
                        name.push('~');
                        name.push(other);
                    }
                    None => name.push('~'),
                },
                Some(c) => name.push(c),
            }
        }
        segments.push(name);
        let segments = segments
            .into_iter()
            .map(|name| Segment {
                index: array_index(&name),
                name,
            })
            .collect();
        Self {
            text: text.to_owned(),
            segments,
        }
    }

    /// The pointer to the field `name` of the top-level object.
    ///
    /// Behavior: io.deephaven.kafka.JsonImpl.JsonConsume#mapFieldNameToJsonPointerStr
    pub fn field(name: &str) -> String {
        format!("/{}", name.replace('~', "~0").replace('/', "~1"))
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    /// The node the pointer finds in `root`, or `Missing`.
    ///
    /// Behavior: com.fasterxml.jackson.databind.JsonNode#at(JsonPointer), with the `_at` of
    /// `ObjectNode` (a field by name, also when the name is a number), `ArrayNode` (an
    /// element by index) and the value nodes (nothing)
    pub fn at<'a>(&self, root: &'a Node) -> &'a Node {
        let mut node = root;
        for segment in &self.segments {
            let next = match node {
                Node::Object(fields) => fields
                    .iter()
                    .find(|(name, _)| *name == segment.name)
                    .map(|(_, value)| value),
                Node::Array(items) => segment.index.and_then(|index| items.get(index)),
                _ => None,
            };
            match next {
                Some(next) => node = next,
                None => return &Node::Missing,
            }
        }
        node
    }
}

/// Behavior: com.fasterxml.jackson.core.JsonPointer#_parseIndex
fn array_index(name: &str) -> Option<usize> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 10 || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    if bytes[0] == b'0' && bytes.len() > 1 {
        return None;
    }
    let value: u64 = name.parse().ok()?;
    (value <= i32::MAX as u64).then_some(value as usize)
}

/// The low 64 bits of the integer written as `digits`.
fn low_bits(digits: &str) -> u64 {
    digits.bytes().fold(0u64, |value, digit| {
        value.wrapping_mul(10).wrapping_add(u64::from(digit - b'0'))
    })
}

fn signed(negative: bool, value: u64) -> i64 {
    if negative {
        (value as i64).wrapping_neg()
    } else {
        value as i64
    }
}

impl Integer {
    /// The low 64 bits, as `intValue` and `longValue` of each of Jackson's integer nodes
    /// keep the low bits.
    fn low_bits(&self) -> i64 {
        signed(self.negative, low_bits(&self.digits))
    }

    /// The value if it is an `IntNode` or a `LongNode`, not a `BigIntegerNode`.
    fn as_long(&self) -> Option<i64> {
        self.text().parse().ok()
    }

    fn text(&self) -> String {
        let sign = if self.negative { "-" } else { "" };
        format!("{sign}{}", self.digits)
    }

    fn double(&self) -> f64 {
        self.text().parse().expect("digits are a number")
    }
}

impl Decimal {
    /// The low 64 bits of the integer part.
    ///
    /// Behavior: java.math.BigDecimal#intValue and #longValue, which truncate toward zero
    /// and keep the low bits
    fn low_bits(&self) -> i64 {
        let value = if self.scale <= 0 {
            let power = 10u64.wrapping_pow(self.scale.unsigned_abs());
            low_bits(&self.digits).wrapping_mul(power)
        } else {
            let scale = self.scale as usize;
            if scale >= self.digits.len() {
                0
            } else {
                low_bits(&self.digits[..self.digits.len() - scale])
            }
        };
        signed(self.negative, value)
    }

    /// Behavior: java.math.BigDecimal#doubleValue, correctly rounded
    fn double(&self) -> f64 {
        let sign = if self.negative { "-" } else { "" };
        let exponent = -i64::from(self.scale);
        format!("{sign}{}e{exponent}", self.digits)
            .parse()
            .expect("digits and an exponent are a number")
    }

    /// Behavior: java.math.BigDecimal#toString: plain digits where the exponent is small,
    /// scientific notation otherwise, and every digit that was written.
    fn text(&self) -> String {
        let sign = if self.negative { "-" } else { "" };
        let digits = &self.digits;
        let scale = i64::from(self.scale);
        if scale == 0 {
            return format!("{sign}{digits}");
        }
        let count = digits.len() as i64;
        let adjusted = count - 1 - scale;
        if scale > 0 && adjusted >= -6 {
            if count > scale {
                let (whole, fraction) = digits.split_at((count - scale) as usize);
                format!("{sign}{whole}.{fraction}")
            } else {
                let zeros = "0".repeat((scale - count) as usize);
                format!("{sign}0.{zeros}{digits}")
            }
        } else {
            let (first, rest) = digits.split_at(1);
            let point = if rest.is_empty() { "" } else { "." };
            let exponent = match adjusted {
                0 => String::new(),
                n if n > 0 => format!("E+{n}"),
                n => format!("E{n}"),
            };
            format!("{sign}{first}{point}{rest}{exponent}")
        }
    }
}

/// Java's `String#trim`: every character up to U+0020 at either end.
fn java_trim(text: &str) -> &str {
    text.trim_matches(|c| c <= ' ')
}

/// The value of `text` as `Double#parseDouble` reads it, or `None` where it throws: after
/// trimming, an optional sign, then `NaN`, `Infinity`, a decimal number with at least one
/// digit and an optional exponent, or a hexadecimal one with a binary exponent; a decimal
/// or hexadecimal number may end in `f`, `F`, `d` or `D`.
///
/// Behavior: jdk.internal.math.FloatingDecimal#readJavaFormatString and #parseHexString
fn java_parse_double(text: &str) -> Option<f64> {
    let text = java_trim(text);
    let (negative, body) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let sign = if negative { -1.0 } else { 1.0 };
    match body {
        "NaN" => return Some(f64::NAN),
        "Infinity" => return Some(sign * f64::INFINITY),
        _ => {}
    }
    let body = body.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(body);
    if let Some(hex) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
        return hex_double(hex).map(|value| sign * value);
    }
    let bytes = body.as_bytes();
    let mut at = 0;
    let count = |at: &mut usize| {
        let start = *at;
        while bytes.get(*at).is_some_and(u8::is_ascii_digit) {
            *at += 1;
        }
        *at - start
    };
    let mut digits = count(&mut at);
    if bytes.get(at) == Some(&b'.') {
        at += 1;
        digits += count(&mut at);
    }
    if digits == 0 {
        return None;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        if count(&mut at) == 0 {
            return None;
        }
    }
    if at != bytes.len() {
        return None;
    }
    // Rust's parse rounds correctly, as Java's does, and reads every text that got here.
    body.parse::<f64>().ok().map(|value| sign * value)
}

/// A hexadecimal floating point number after its `0x`: hex digits with an optional point,
/// at least one digit, then `p` and a decimal exponent of two. Rounded to nearest, ties to
/// even.
fn hex_double(text: &str) -> Option<f64> {
    let (mantissa, exponent) = text.split_once(['p', 'P'])?;
    let exponent = exponent.strip_prefix('+').unwrap_or(exponent);
    let exponent_digits = exponent.strip_prefix('-').unwrap_or(exponent);
    if exponent_digits.is_empty() || !exponent_digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Any exponent beyond this gives zero or infinity whatever the digits.
    let mut binary: i64 = exponent.parse().unwrap_or(if exponent.starts_with('-') {
        -1 << 40
    } else {
        1 << 40
    });
    binary = binary.clamp(-1 << 40, 1 << 40);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut bits: u64 = 0;
    let mut sticky = false;
    for (i, c) in whole.chars().chain(fraction.chars()).enumerate() {
        let digit = u64::from(c.to_digit(16)?);
        let in_fraction = i >= whole.len();
        if bits >> 60 == 0 {
            bits = bits << 4 | digit;
            if in_fraction {
                binary -= 4;
            }
        } else {
            sticky |= digit != 0;
            if !in_fraction {
                binary += 4;
            }
        }
    }
    if bits == 0 {
        return Some(0.0);
    }
    let zeros = bits.leading_zeros();
    let bits = bits << zeros;
    binary -= i64::from(zeros);
    // The value is bits * 2^binary, with the top bit of `bits` set.
    let unbiased = binary + 63;
    if unbiased > 1023 {
        return Some(f64::INFINITY);
    }
    let shift = if unbiased >= -1022 {
        11
    } else {
        11 + (-1022 - unbiased)
    } + 64;
    if shift > 128 {
        return Some(0.0);
    }
    let wide = u128::from(bits) << 64;
    let (kept, rest) = if shift == 128 {
        (0u128, wide)
    } else {
        (wide >> shift, wide & ((1u128 << shift) - 1))
    };
    let half = 1u128 << (shift - 1);
    let up = rest > half || (rest == half && (sticky || kept & 1 == 1));
    let mut kept = kept + u128::from(up);
    let mut unbiased = unbiased;
    if unbiased >= -1022 {
        if kept == 1 << 53 {
            kept >>= 1;
            unbiased += 1;
            if unbiased > 1023 {
                return Some(f64::INFINITY);
            }
        }
        let field = (unbiased + 1023) as u64;
        Some(f64::from_bits(
            field << 52 | (kept as u64 & ((1 << 52) - 1)),
        ))
    } else {
        // Subnormal; a carry to 2^52 is the smallest normal number, as the bits say.
        Some(f64::from_bits(kept as u64))
    }
}

/// Behavior: com.fasterxml.jackson.core.io.NumberInput#parseAsInt and #parseAsLong: the
/// trimmed text as an integer if it is only digits after its sign, as a double cast to the
/// type if it has anything else, and 0 where either fails.
fn parse_as_integer<T: std::str::FromStr + Default>(text: &str, cast: fn(f64) -> T) -> T {
    let text = java_trim(text);
    let (text, digits_from) = match text.as_bytes().first() {
        None => return T::default(),
        Some(b'+') => (&text[1..], 0),
        Some(b'-') => (text, 1),
        Some(_) => (text, 0),
    };
    if !text.as_bytes()[digits_from..]
        .iter()
        .all(u8::is_ascii_digit)
    {
        return java_parse_double(text).map_or_else(T::default, cast);
    }
    text.parse().unwrap_or_default()
}

/// Behavior: io.deephaven.time.DateTimeUtils#epochAutoToEpochNanos, whose
/// multiplications wrap as Java's do
fn epoch_auto_nanos(offset: i64) -> i64 {
    // TimeConstants#MICROTIME_THRESHOLD
    const THRESHOLD: u64 = 9_309_341_000_000;
    let size = offset.unsigned_abs();
    if size > 1000 * THRESHOLD {
        offset
    } else if size > THRESHOLD {
        offset.wrapping_mul(1000)
    } else if size > THRESHOLD / 1000 {
        offset.wrapping_mul(1_000_000)
    } else {
        offset.wrapping_mul(1_000_000_000)
    }
}

/// The cell of an instant of `nanos` since the epoch, which Java makes an `Instant` and
/// then nanoseconds again; that fails from second `Long.MAX_VALUE / 10^9` on.
///
/// Behavior: io.deephaven.time.DateTimeUtils#epochNanosToInstant, #epochNanos and
/// #safeComputeNanos
fn instant_cell(nanos: i64) -> Result<Option<i64>, KafkaError> {
    if nanos == i64::MIN {
        return Ok(None);
    }
    let seconds = nanos.div_euclid(1_000_000_000);
    if seconds >= i64::MAX / 1_000_000_000 {
        return Err(KafkaError::Record(format!(
            "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: Numeric overflow \
             detected during conversion of {seconds} to nanoseconds"
        )));
    }
    Ok(Some(nanos))
}

/// Behavior: io.deephaven.time.DateTimeUtils#parseInstant: a whole number of one to
/// nineteen digits by its size, any other text as a date and time
fn parse_instant(text: &str) -> Result<Option<i64>, KafkaError> {
    let cannot = || {
        KafkaError::Record(format!(
            "io.deephaven.time.DateTimeUtils$DateTimeParseException: Cannot parse instant: \
             {text}"
        ))
    };
    let digits = text.strip_prefix('-').unwrap_or(text);
    if (1..=19).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit()) {
        let offset: i64 = text.parse().map_err(|_| cannot())?;
        if offset == i64::MIN {
            return Ok(None);
        }
        return instant_cell(epoch_auto_nanos(offset));
    }
    match oxhaven_expr::parse_instant_text(text) {
        Ok(nanos) => instant_cell(nanos),
        Err(oxhaven_contracts::expr::FormulaError::Unsupported(what)) => Err(not_ported(
            "io.deephaven.time.DateTimeUtils#parseInstant",
            &format!("the text {text:?} in an Instant column ({what})"),
        )),
        Err(_) => Err(cannot()),
    }
}

fn unpaired(_: &Text) -> KafkaError {
    not_ported(
        "java.lang.String",
        "text with a UTF-16 surrogate that has no pair, which a Rust string cannot hold",
    )
}

/// Jackson's compact JSON of a node.
///
/// Behavior: com.fasterxml.jackson.databind.node.BaseJsonNode#toString, with the escapes
/// of com.fasterxml.jackson.core.io.CharTypes#appendQuoted: `"` and `\` escaped, the
/// control characters below U+0020 as `\b`, `\t`, `\n`, `\f`, `\r` or `\u00XX` in upper
/// case, everything else as it is
fn write_json(node: &Node, out: &mut String) -> Result<(), KafkaError> {
    fn quoted(text: &Text, out: &mut String) -> Result<(), KafkaError> {
        if text.unpaired {
            return Err(unpaired(text));
        }
        out.push('"');
        for c in text.value.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\u{8}' => out.push_str("\\b"),
                '\t' => out.push_str("\\t"),
                '\n' => out.push_str("\\n"),
                '\u{c}' => out.push_str("\\f"),
                '\r' => out.push_str("\\r"),
                c if c < ' ' => out.push_str(&format!("\\u{:04X}", c as u32)),
                c => out.push(c),
            }
        }
        out.push('"');
        Ok(())
    }
    match node {
        Node::Missing => {}
        Node::Null => out.push_str("null"),
        Node::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        Node::Integer(value) => out.push_str(&value.text()),
        Node::Decimal(value) => out.push_str(&value.text()),
        Node::Text(text) => quoted(text, out)?,
        Node::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, out)?;
            }
            out.push(']');
        }
        Node::Object(fields) => {
            out.push('{');
            for (i, (name, value)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let name = Text {
                    value: name.clone(),
                    unpaired: false,
                    first: None,
                };
                quoted(&name, out)?;
                out.push(':');
                write_json(value, out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}

impl Node {
    #[cfg(test)]
    fn is_null(&self) -> bool {
        matches!(self, Node::Missing | Node::Null)
    }

    /// `asInt`, which the `int`, `short` and `byte` columns cast to their width.
    ///
    /// Behavior: JsonNodeUtil#getInt, #getShort and #getByte; `asInt` of Jackson's
    /// `IntNode`, `LongNode`, `BigIntegerNode`, `DecimalNode` (`BigDecimal#intValue`),
    /// `TextNode` (`NumberInput#parseAsInt`), `BooleanNode` and `ContainerNode`
    pub fn as_int(&self) -> Option<i32> {
        Some(match self {
            Node::Missing | Node::Null => return None,
            Node::Bool(value) => i32::from(*value),
            Node::Integer(value) => value.low_bits() as i32,
            Node::Decimal(value) => value.low_bits() as i32,
            Node::Text(text) => parse_as_integer(&text.value, |value| value as i32),
            Node::Array(_) | Node::Object(_) => 0,
        })
    }

    /// Behavior: JsonNodeUtil#getLong; `asLong` of the same nodes as [`Node::as_int`]
    pub fn as_long(&self) -> Option<i64> {
        Some(match self {
            Node::Missing | Node::Null => return None,
            Node::Bool(value) => i64::from(*value),
            Node::Integer(value) => value.low_bits(),
            Node::Decimal(value) => value.low_bits(),
            Node::Text(text) => parse_as_integer(&text.value, |value| value as i64),
            Node::Array(_) | Node::Object(_) => 0,
        })
    }

    /// `asDouble`, which the `float` column casts.
    ///
    /// Behavior: JsonNodeUtil#getDouble and #getFloat; `asDouble` of the numeric nodes,
    /// `TextNode` (`NumberInput#parseAsDouble`: `Double#parseDouble` of the trimmed text,
    /// 0 where it throws), `BooleanNode` and `ContainerNode`
    pub fn as_double(&self) -> Option<f64> {
        Some(match self {
            Node::Missing | Node::Null => return None,
            Node::Bool(value) => f64::from(u8::from(*value)),
            Node::Integer(value) => value.double(),
            Node::Decimal(value) => value.double(),
            Node::Text(text) => java_parse_double(&text.value).unwrap_or(0.0),
            Node::Array(_) | Node::Object(_) => 0.0,
        })
    }

    /// The text of a value node, the compact JSON of an object or array.
    ///
    /// Behavior: JsonNodeUtil#getString; `asText` of the value nodes, `toString` of the
    /// containers
    pub fn text(&self) -> Result<Option<String>, KafkaError> {
        Ok(Some(match self {
            Node::Missing | Node::Null => return Ok(None),
            Node::Bool(value) => value.to_string(),
            Node::Integer(value) => value.text(),
            Node::Decimal(value) => value.text(),
            Node::Text(text) if text.unpaired => return Err(unpaired(text)),
            Node::Text(text) => text.value.clone(),
            Node::Array(_) | Node::Object(_) => {
                let mut out = String::new();
                write_json(self, &mut out)?;
                out
            }
        }))
    }

    /// A cell of a `Boolean` column, which Java reads from the node's text and not with
    /// `asBoolean`: a few spellings of true and false, nothing for empty text, and a
    /// failure for anything else, `0` and objects included. `pointer` is named in the
    /// failure.
    ///
    /// Behavior: io.deephaven.kafka.ingest.JsonNodeBooleanFieldCopier#copyField
    pub fn boolean(&self, pointer: &Pointer) -> Result<Option<bool>, KafkaError> {
        let text = match self {
            // The failure names the text Java would print, with U+FFFD for what Java has
            // as a lone surrogate.
            Node::Text(text) => Some(text.value.clone()),
            other => other.text()?,
        };
        let Some(text) = text else {
            return Ok(None);
        };
        Ok(match java_trim(&text) {
            "TRUE" | "True" | "true" | "T" | "t" | "1" => Some(true),
            "FALSE" | "False" | "false" | "F" | "f" => Some(false),
            "" => None,
            _ => {
                return Err(KafkaError::Record(format!(
                    "io.deephaven.UncheckedDeephavenException: value {text} not recognized as \
                     Boolean for field {}",
                    pointer.text()
                )))
            }
        })
    }

    /// A cell of a `char` column: a number's `intValue` cut to sixteen bits, else the first
    /// UTF-16 unit of `asText`; empty text, and `\uFFFF`, Java's null `char`, are null.
    ///
    /// Behavior: JsonNodeUtil#getChar
    pub fn char(&self) -> Option<u16> {
        let unit = match self {
            Node::Missing | Node::Null => None,
            Node::Integer(value) => Some(value.low_bits() as u16),
            Node::Decimal(value) => Some(value.low_bits() as u16),
            Node::Bool(true) => Some(u16::from(b't')),
            Node::Bool(false) => Some(u16::from(b'f')),
            Node::Text(text) => text.first,
            // `ContainerNode#asText` is empty.
            Node::Array(_) | Node::Object(_) => None,
        };
        unit.filter(|unit| *unit != u16::MAX)
    }

    /// A cell of an `Instant` column, in nanoseconds since the epoch: an `int` or `long`
    /// node counts seconds, milliseconds, microseconds or nanoseconds by its size; any
    /// other node is its text, read by `DateTimeUtils#parseInstant`.
    ///
    /// Behavior: JsonNodeUtil#getInstant, io.deephaven.kafka.ingest.JsonNodeInstantFieldCopier
    pub fn instant(&self) -> Result<Option<i64>, KafkaError> {
        let text = match self {
            Node::Missing | Node::Null => return Ok(None),
            Node::Integer(value) => match value.as_long() {
                Some(i64::MIN) => return Ok(None),
                Some(offset) => return instant_cell(epoch_auto_nanos(offset)),
                None => value.text(),
            },
            Node::Decimal(value) => value.text(),
            Node::Bool(value) => value.to_string(),
            Node::Text(text) if text.unpaired => return Err(unpaired(text)),
            Node::Text(text) => text.value.clone(),
            Node::Array(_) | Node::Object(_) => String::new(),
        };
        parse_instant(&text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(record: &str, name: &str) -> Node {
        Pointer::compile(&Pointer::field(name))
            .at(&parse(record).unwrap())
            .clone()
    }

    fn value(json: &str) -> Node {
        field(&format!("{{\"v\": {json}}}"), "v")
    }

    #[test]
    fn decimals_print_as_big_decimal_does() {
        for (written, java) in [
            ("1.50", "1.50"),
            ("-0.0", "0.0"),
            ("0.000001", "0.000001"),
            ("0.0000001", "1E-7"),
            ("1e3", "1E+3"),
            ("1.5E3", "1.5E+3"),
            ("12.5e1", "125"),
            ("123.45e-2", "1.2345"),
            ("-1.25E-10", "-1.25E-10"),
            ("100e-2", "1.00"),
            ("0e5", "0E+5"),
            ("0e-5", "0.00000"),
            ("-0e0", "0"),
            ("-0.000", "0.000"),
            ("1e2147483648", "1E+2147483648"),
        ] {
            assert_eq!(value(written).text().unwrap().unwrap(), java, "{written}");
        }
    }

    #[test]
    fn objects_keep_the_place_of_a_repeated_field_and_take_its_last_value() {
        let record = r#" {"a": 1, "b": -2.50, "c": "x", "d": true, "e": null, "f": {"g": 1}, "a": 7} trailing"#;
        assert_eq!(field(record, "a").as_int(), Some(7));
        assert_eq!(field(record, "b").text().unwrap().unwrap(), "-2.50");
        assert_eq!(field(record, "c").text().unwrap().unwrap(), "x");
        assert_eq!(field(record, "e"), Node::Null);
        assert_eq!(field(record, "missing"), Node::Missing);
        let nested = r#"{"v": {"x": 1, "y": 2, "x": {"z": 1}}}"#;
        assert_eq!(
            field(nested, "v").text().unwrap().unwrap(),
            r#"{"x":{"z":1},"y":2}"#
        );
    }

    #[test]
    fn text_without_a_value_is_missing_and_bad_json_is_an_error() {
        for text in ["", "  ", "5", "\"x\"", "null", "[1]", "{\"v\": 1}}"] {
            assert!(field(text, "a").is_null(), "{text}");
        }
        for bad in [
            "{\"a\": }",
            "nope",
            "{\"v\": 01}",
            "{\"v\": NaN}",
            "{'v': 1}",
            "{\"v\": 1,}",
            "{\"v\": \"a\tb\"}",
            "{\"v\": 1.}",
            "{\"v\": .5}",
            "{\"v\": +1}",
            "{\"v\": -}",
            "{\"v\": 1e}",
            "{\"v\": \"\\x\"}",
            "{\"v\": tru}",
            "{v: 1}",
            "{\"v\": 1 /* c */}",
            "{\"v\": [1,]}",
            "{\u{b}\"v\": 1}",
            "\u{c}{\"v\": 1}",
            "\u{feff}{\"v\": 1}",
        ] {
            let error = parse(bad)
                .err()
                .unwrap_or_else(|| panic!("{bad}"))
                .to_string();
            assert!(error.contains("Failed to parse JSON string."), "{error}");
        }
    }

    #[test]
    fn the_limits_of_jackson_are_kept() {
        let nested = |depth: usize| format!("{}1{}", "[".repeat(depth), "]".repeat(depth));
        assert!(parse(&nested(1000)).is_ok());
        assert!(parse(&nested(1001)).is_err());
        let deep = format!("{{\"v\": {}}}", nested(999));
        assert!(parse(&deep).is_ok());
        let deeper = format!("{{\"v\": {}}}", nested(1000));
        assert!(parse(&deeper).is_err());
        let ones = |n: usize| "1".repeat(n);
        assert!(parse(&ones(1000)).is_ok());
        assert!(parse(&format!("-{}", ones(1000))).is_ok());
        assert!(parse(&ones(1001)).is_err());
        assert!(parse(&format!("1.{}", ones(999))).is_ok());
        assert!(parse(&format!("1.{}", ones(1000))).is_err());
        assert!(parse(&format!("{}e-1", ones(999))).is_ok());
        assert!(parse(&format!("{}e12", ones(999))).is_err());
        // A scale outside an int is BigDecimal's failure, not Jackson's.
        let error = parse("{\"w\": 1e-2147483648}").unwrap_err().to_string();
        assert_eq!(
            error,
            "java.lang.NumberFormatException: Value \"1e-2147483648\" can not be deserialized \
             as `java.math.BigDecimal`, reason:  Exponent overflow."
        );
        assert!(parse("1e2147483648").is_ok());
        assert!(parse("0.1e-2147483647").is_err());
    }

    #[test]
    fn pointers_find_nested_fields_and_array_elements() {
        let record = r#"{"a": {"b": {"c": 5}, "arr": [10, 20, {"d": "x"}]}, "x/y": 1, "m~n": 2,
            "": 3, "0": 4, "e": [1,2], "01": 6, "-": 7, " ": 8, "a~2": 9, "a~": 10}"#;
        let root = parse(record).unwrap();
        let at = |pointer: &str| Pointer::compile(pointer).at(&root).as_int();
        for (pointer, java) in [
            ("/a/b/c", Some(5)),
            ("/a/arr/1", Some(20)),
            ("/a/arr/5", None),
            ("/a/arr/01", None),
            ("/a/arr/-", None),
            ("/x~1y", Some(1)),
            ("/m~0n", Some(2)),
            ("/", Some(3)),
            ("/0", Some(4)),
            ("/e/0", Some(1)),
            ("/e/01", None),
            ("/01", Some(6)),
            ("/-", Some(7)),
            ("/ ", Some(8)),
            ("/a/b/c/d", None),
            ("/nope/x", None),
            ("/a~2", Some(9)),
            ("/a~", Some(10)),
            ("/a/arr/2147483648", None),
            ("/a//b", None),
            ("/a/b/", None),
            ("/e/-1", None),
            ("/e/+1", None),
        ] {
            assert_eq!(at(pointer), java, "{pointer}");
        }
        let text = |pointer: &str| Pointer::compile(pointer).at(&root).text().unwrap();
        assert_eq!(text("/a/arr/2/d").unwrap(), "x");
        assert_eq!(text("/a/arr").unwrap(), r#"[10,20,{"d":"x"}]"#);
        assert_eq!(Pointer::field("x~/y"), "/x~0~1y");
        let array = parse(r#"[1, "two", {"a": 3}]"#).unwrap();
        assert_eq!(Pointer::compile("/2/a").at(&array).as_int(), Some(3));
        assert!(Pointer::compile("/a").at(&array).is_null());
    }

    #[test]
    fn numbers_convert_by_their_low_bits_and_truncate() {
        for (json, int, long) in [
            ("2147483648", None, Some(2147483648)),
            ("-2147483649", Some(2147483647), Some(-2147483649)),
            ("9223372036854775808", Some(0), None),
            ("-9223372036854775809", Some(-1), Some(9223372036854775807)),
            (
                "123456789012345678901234567890123456789012345",
                Some(-236789895),
                Some(-1199228034047877255),
            ),
            ("2.9", Some(2), Some(2)),
            ("-2.9", Some(-2), Some(-2)),
            ("1e19", Some(-1981284352), Some(-8446744073709551616)),
            ("1e400", Some(0), Some(0)),
            ("4294967296.7", Some(0), Some(4294967296)),
            ("3.4028235e38", None, Some(-483344729602260992)),
            ("true", Some(1), Some(1)),
            ("{}", Some(0), Some(0)),
            ("[7]", Some(0), Some(0)),
        ] {
            let node = value(json);
            let cell = |v: Option<i32>| v.filter(|v| *v != i32::MIN);
            assert_eq!(cell(node.as_int()), int, "{json} as int");
            assert_eq!(
                node.as_long().filter(|v| *v != i64::MIN),
                long,
                "{json} as long"
            );
        }
        assert_eq!(value("1e3").as_int().map(|v| v as i8), Some(-24));
        assert_eq!(value("1e2147483648").as_double(), Some(f64::INFINITY));
        assert_eq!(value("1e2147483648").as_int(), Some(0));
        assert_eq!(value("1e-400").as_double(), Some(0.0));
        assert_eq!(value("4.9e-324").as_double(), Some(f64::from_bits(1)));
        assert_eq!(value("null").as_int(), None);
    }

    #[test]
    fn text_converts_as_jackson_number_input_does() {
        for (text, int, long, double) in [
            ("12", 12, 12, 12.0),
            (" 12 ", 12, 12, 12.0),
            ("+12", 12, 12, 12.0),
            ("12.9", 12, 12, 12.9),
            ("1e3", 1000, 1000, 1000.0),
            ("abc", 0, 0, 0.0),
            ("", 0, 0, 0.0),
            ("0x10", 0, 0, 0.0),
            ("2147483648", 0, 2147483648, 2147483648.0),
            ("9223372036854775808", 0, 0, 9.223372036854776e18),
            ("Infinity", i32::MAX, i64::MAX, f64::INFINITY),
            ("-Infinity", i32::MIN, i64::MIN, f64::NEG_INFINITY),
            ("infinity", 0, 0, 0.0),
            ("1.5f", 1, 1, 1.5),
            ("1.5D", 1, 1, 1.5),
            ("1.5ff", 0, 0, 0.0),
            ("NaNd", 0, 0, 0.0),
            ("\t5", 5, 5, 5.0),
            ("5\n", 5, 5, 5.0),
            ("\u{a0}5", 0, 0, 0.0),
            ("1.", 1, 1, 1.0),
            (".5", 0, 0, 0.5),
            ("-.5", 0, 0, -0.5),
            ("+.5e1", 5, 5, 5.0),
            ("0x1p3", 8, 8, 8.0),
            ("0x1.8p1", 3, 3, 3.0),
            ("0X10P-1", 8, 8, 8.0),
            ("0x1p3d", 8, 8, 8.0),
            ("0x", 0, 0, 0.0),
            ("0x.p1", 0, 0, 0.0),
            ("0x1.fffffffffffff8p1023", i32::MAX, i64::MAX, f64::INFINITY),
            ("0x1p-1075", 0, 0, 0.0),
            ("1e", 0, 0, 0.0),
            ("e3", 0, 0, 0.0),
            ("-", 0, 0, 0.0),
            ("+", 0, 0, 0.0),
            ("+-5", -5, -5, 0.0),
            ("--5", 0, 0, 0.0),
            ("2147483647.5", i32::MAX, 2147483647, 2147483647.5),
            ("1e10", i32::MAX, 10000000000, 1e10),
            ("-00012", -12, -12, -12.0),
            ("1 2", 0, 0, 0.0),
            ("\u{661}", 0, 0, 0.0),
            ("1.e3", 1000, 1000, 1000.0),
            ("1e310", i32::MAX, i64::MAX, f64::INFINITY),
            ("4.9e-325", 0, 0, 0.0),
        ] {
            let node = Node::Text(Text::of(text));
            assert_eq!(node.as_int(), Some(int), "{text:?} as int");
            assert_eq!(node.as_long(), Some(long), "{text:?} as long");
            assert_eq!(node.as_double(), Some(double), "{text:?} as double");
        }
        for nan in ["NaN", "-NaN", "+NaN", " NaN "] {
            let node = Node::Text(Text::of(nan));
            assert!(node.as_double().unwrap().is_nan(), "{nan}");
            assert_eq!(node.as_int(), Some(0));
        }
    }

    #[test]
    fn hexadecimal_doubles_round_to_nearest_even() {
        for (text, java) in [
            ("1p0", 1.0),
            ("1.8p1", 3.0),
            ("0.0000001p0", 2f64.powi(-28)),
            ("1p-1074", f64::from_bits(1)),
            ("1p-1075", 0.0),
            ("1.8p-1074", f64::from_bits(2)),
            ("1.fffffffffffffp1023", f64::MAX),
            ("1.fffffffffffff7p1023", f64::MAX),
            ("1.fffffffffffff8p1023", f64::INFINITY),
            ("1.00000000000008p0", 1.0),
            ("1.00000000000018p0", 1.0 + 2.0 * f64::EPSILON),
            ("1.000000000000080000001p0", 1.0 + f64::EPSILON),
            ("1p99999999999", f64::INFINITY),
            ("1p-99999999999", 0.0),
        ] {
            assert_eq!(hex_double(text), Some(java), "{text}");
        }
    }

    #[test]
    fn strings_print_containers_as_jackson_does() {
        let node = value(
            r#"{"k\"\u0001": "\u0000\u0007\b\t\n\u000b\f\r\u001f \u007f\u0080\u2028\ud83d\ude00/<>&'\\",
              "n": [-0, 0.10, 1E-7, 1e+2, 12345678901234567890, -1.5E-10, true, false, null, {}, []], "": "e"}"#,
        );
        assert_eq!(
            node.text().unwrap().unwrap(),
            "{\"k\\\"\\u0001\":\"\\u0000\\u0007\\b\\t\\n\\u000B\\f\\r\\u001F \u{7f}\u{80}\u{2028}\u{1f600}/<>&'\\\\\",\
             \"n\":[0,0.10,1E-7,1E+2,12345678901234567890,-1.5E-10,true,false,null,{},[]],\"\":\"e\"}"
        );
        assert_eq!(value("-0").text().unwrap().unwrap(), "0");
        assert_eq!(value("{}").text().unwrap().unwrap(), "{}");
        let lone = value(r#""\ud83d""#);
        assert!(matches!(lone.text(), Err(KafkaError::NotPorted { .. })));
        assert_eq!(lone.char(), Some(0xD83D));
        assert_eq!(value(r#""\ud83d\ude00x""#).char(), Some(0xD83D));
        assert_eq!(value(r#""x\udc00""#).char(), Some(u16::from(b'x')));
    }

    #[test]
    fn chars_are_a_number_cut_or_the_first_unit_of_the_text() {
        for (json, java) in [
            ("1", Some(1)),
            ("-1", None),
            ("65535", None),
            ("65536", Some(0)),
            ("-2.9", Some(65534)),
            ("1e3", Some(1000)),
            ("1e400", Some(0)),
            ("65535.5", None),
            ("123456789.123456789", Some(52501)),
            (r#""12""#, Some(49)),
            (r#""""#, None),
            (r#"" ""#, Some(32)),
            (r#""\uffffx""#, None),
            ("true", Some(116)),
            ("false", Some(102)),
            ("{}", None),
            ("[1]", None),
            ("null", None),
        ] {
            assert_eq!(value(json).char(), java, "{json}");
        }
    }

    #[test]
    fn booleans_are_read_from_the_text() {
        let pointer = Pointer::compile("/a~1b");
        let boolean = |json: &str| value(json).boolean(&pointer);
        for (json, java) in [
            ("1", Some(true)),
            ("1e0", Some(true)),
            ("true", Some(true)),
            ("false", Some(false)),
            (r#""""#, None),
            (r#"" ""#, None),
            ("null", None),
            (r#""t""#, Some(true)),
            (r#"" TRUE ""#, Some(true)),
            (r#""\u000bt""#, Some(true)),
            (r#""F""#, Some(false)),
        ] {
            assert_eq!(boolean(json).unwrap(), java, "{json}");
        }
        for (json, text) in [
            ("0", "0"),
            ("2", "2"),
            ("1.0", "1.0"),
            (r#""yes""#, "yes"),
            ("{}", "{}"),
            ("[1]", "[1]"),
            (r#"{"a": 1}"#, r#"{"a":1}"#),
        ] {
            assert_eq!(
                boolean(json).unwrap_err().to_string(),
                format!(
                    "io.deephaven.UncheckedDeephavenException: value {text} not recognized as \
                     Boolean for field /a~1b"
                ),
                "{json}"
            );
        }
    }

    #[test]
    fn instants_count_by_size_or_parse_the_text() {
        let second = 1_000_000_000i64;
        for (json, java) in [
            ("0", Some(0)),
            ("-1", Some(-second)),
            ("1700000000", Some(1_700_000_000 * second)),
            ("1700000000000", Some(1_700_000_000 * second)),
            ("1700000000000000", Some(1_700_000_000 * second)),
            ("1700000000000000000", Some(1_700_000_000 * second)),
            ("9309340999", Some(9309340999i64.wrapping_mul(second))),
            ("9309341001", Some(9309341001 * 1_000_000)),
            ("-9223372036854775808", None),
            ("-9223372036854775807", Some(-9223372036854775807)),
            (r#""1700000000""#, Some(1_700_000_000 * second)),
            (r#""2024-01-02T03:04:05Z""#, Some(1_704_164_645 * second)),
            (
                r#""2024-01-02T03:04:05.123456789 UTC""#,
                Some(1_704_164_645 * second + 123_456_789),
            ),
            ("12.5e1", Some(125 * second)),
            ("null", None),
        ] {
            assert_eq!(value(json).instant().unwrap(), java, "{json}");
        }
        for (json, text) in [
            ("9223372036854775807", "DateTimeOverflowException: Numeric overflow detected during conversion of 9223372036 to nanoseconds"),
            (r#""9223372036854775807""#, "conversion of 9223372036 to"),
            ("9223372036854775808", "Cannot parse instant: 9223372036854775808"),
            ("1.5", "Cannot parse instant: 1.5"),
            ("1e3", "Cannot parse instant: 1E+3"),
            (r#""""#, "Cannot parse instant: "),
            ("true", "Cannot parse instant: true"),
            ("{}", "Cannot parse instant: "),
            (r#"" 1700000000""#, "Cannot parse instant:  1700000000"),
            (r#""abc""#, "Cannot parse instant: abc"),
        ] {
            let error = value(json).instant().unwrap_err().to_string();
            assert!(error.contains(text), "{json}: {error}");
        }
    }
}
