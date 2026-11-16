//! A JSON record as Java reads it: the fields of the top-level object, and the conversion
//! of a field to a column's type.
//!
//! Java parses a record with Jackson into a tree and converts each field with the
//! `asInt`, `asLong`, `asDouble` and `asText` of the node it finds, which
//! coerce: `true` is 1 in an `int` column, a number is text in a `String` column, an object
//! is 0. Jackson is set to read every number with a fraction or an exponent as an exact
//! `BigDecimal`, so the text of a number matters (`1.50` stays `1.50` in a `String`
//! column); the fields are therefore kept as text and not as `serde_json` numbers.
//!
//! What is not converted here is refused by the name of the Jackson method that does it,
//! and fails the table: text in a numeric column, a fraction in an integer column, an
//! object or an array in a `String` column.
//!
//! Behavior: io.deephaven.kafka.ingest.JsonNodeUtil

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::{not_ported, KafkaError};

/// One field of a record, or what stands for it.
#[derive(Clone, PartialEq, Debug)]
pub enum Node {
    /// No such field, or `null`. Java allows both and makes both a null cell.
    Null,
    Bool(bool),
    /// A number without fraction and exponent: Jackson's `IntNode`, `LongNode` or
    /// `BigIntegerNode`, by size.
    Integer(i128),
    /// A number with a fraction or an exponent, as written: Jackson's `DecimalNode`.
    Decimal(String),
    Text(String),
    /// An object or an array.
    Container,
}

/// The fields of the top-level object of a record.
pub struct Document {
    fields: HashMap<String, Box<RawValue>>,
}

fn parse_failure(error: impl std::fmt::Display) -> KafkaError {
    // Behavior: JsonNodeUtil#makeJsonNode
    KafkaError::Record(format!(
        "io.deephaven.UncheckedDeephavenException: Failed to parse JSON string. ({error})"
    ))
}

impl Document {
    /// Behavior: JsonNodeUtil#makeJsonNode. Jackson reads one value and does not look at
    /// what follows it, and answers text without a value with a missing node.
    pub fn parse(text: &str) -> Result<Self, KafkaError> {
        let no_fields = || Self {
            fields: HashMap::new(),
        };
        let start = text.trim_start_matches([' ', '\t', '\n', '\r']);
        match start.as_bytes().first() {
            None => Ok(no_fields()),
            Some(b'{') => {
                let mut reader = serde_json::Deserializer::from_str(start);
                let fields = HashMap::deserialize(&mut reader).map_err(parse_failure)?;
                Ok(Self { fields })
            }
            Some(b'[') => Err(not_ported(
                "com.fasterxml.jackson.databind.node.ArrayNode#_at",
                "a JSON record that is an array",
            )),
            Some(_) => {
                // A scalar has no fields, but must still be JSON.
                let mut reader = serde_json::Deserializer::from_str(start);
                serde::de::IgnoredAny::deserialize(&mut reader).map_err(parse_failure)?;
                Ok(no_fields())
            }
        }
    }

    /// The field named `name`.
    pub fn field(&self, name: &str) -> Result<Node, KafkaError> {
        let Some(raw) = self.fields.get(name) else {
            return Ok(Node::Null);
        };
        let text = raw.get();
        Ok(match text.as_bytes().first() {
            Some(b'n') => Node::Null,
            Some(b't') => Node::Bool(true),
            Some(b'f') => Node::Bool(false),
            Some(b'"') => Node::Text(serde_json::from_str(text).map_err(parse_failure)?),
            Some(b'{' | b'[') => Node::Container,
            _ if text.contains(['.', 'e', 'E']) => Node::Decimal(text.to_owned()),
            _ => Node::Integer(text.parse().map_err(|_| {
                not_ported(
                    "com.fasterxml.jackson.databind.node.BigIntegerNode",
                    &format!("the integer {text}, which is outside 128 bits"),
                )
            })?),
        })
    }
}

impl Node {
    /// `asInt` or `asLong`, before the cast to the column's width, which keeps the low
    /// bits. `method` is the one of the two that the column's type calls.
    ///
    /// Behavior: JsonNodeUtil#getInt, #getShort, #getByte and #getLong;
    /// `IntNode`, `LongNode`, `BigIntegerNode`, `BooleanNode` and `ContainerNode`
    /// `#asInt` and `#asLong`
    pub fn integral(&self, method: &str) -> Result<Option<i128>, KafkaError> {
        Ok(Some(match self {
            Node::Null => return Ok(None),
            Node::Bool(value) => i128::from(*value),
            Node::Integer(value) => *value,
            Node::Container => 0,
            Node::Decimal(text) => {
                return Err(not_ported(
                    &format!("com.fasterxml.jackson.databind.node.DecimalNode#{method}"),
                    &format!("the number {text} in an integer column"),
                ))
            }
            Node::Text(_) => {
                return Err(not_ported(
                    &format!("com.fasterxml.jackson.databind.node.TextNode#{method}"),
                    "text in an integer column",
                ))
            }
        }))
    }

    /// Behavior: JsonNodeUtil#getDouble and #getFloat (which casts this); `NumericNode`,
    /// `BooleanNode` and `ContainerNode` `#asDouble`
    pub fn double(&self) -> Result<Option<f64>, KafkaError> {
        Ok(Some(match self {
            Node::Null => return Ok(None),
            Node::Bool(value) => f64::from(u8::from(*value)),
            Node::Integer(value) => *value as f64,
            Node::Decimal(text) => text.parse().map_err(parse_failure)?,
            Node::Container => 0.0,
            Node::Text(_) => {
                return Err(not_ported(
                    "com.fasterxml.jackson.databind.node.TextNode#asDouble",
                    "text in a floating point column",
                ))
            }
        }))
    }

    /// A cell of a `Boolean` column, which Java reads from the field's text and not with
    /// `asBoolean`: a few spellings of true and false, nothing for empty text, and a
    /// failure for anything else, `0` included. `field` is named in the failure.
    ///
    /// Behavior: io.deephaven.kafka.ingest.JsonNodeBooleanFieldCopier#copyField
    pub fn boolean(&self, field: &str) -> Result<Option<bool>, KafkaError> {
        let Some(text) = self.text()? else {
            return Ok(None);
        };
        // Java's `String#trim` removes every character up to U+0020.
        Ok(match text.trim_matches(|c| c <= ' ') {
            "TRUE" | "True" | "true" | "T" | "t" | "1" => Some(true),
            "FALSE" | "False" | "false" | "F" | "f" => Some(false),
            "" => None,
            _ => {
                let pointer = field.replace('~', "~0").replace('/', "~1");
                return Err(KafkaError::Record(format!(
                    "io.deephaven.UncheckedDeephavenException: value {text} not recognized as \
                     Boolean for field /{pointer}"
                )));
            }
        })
    }

    /// Behavior: JsonNodeUtil#getString; `TextNode`, `BooleanNode`, `IntNode`, `LongNode`,
    /// `BigIntegerNode` and `DecimalNode` `#asText`
    pub fn text(&self) -> Result<Option<String>, KafkaError> {
        Ok(Some(match self {
            Node::Null => return Ok(None),
            Node::Bool(value) => value.to_string(),
            Node::Integer(value) => value.to_string(),
            Node::Decimal(text) => big_decimal_text(text),
            Node::Text(text) => text.clone(),
            Node::Container => {
                return Err(not_ported(
                    "com.fasterxml.jackson.databind.node.ContainerNode#toString",
                    "an object or an array in a String column",
                ))
            }
        }))
    }
}

/// The text Java gives the decimal number written as `text`: plain digits where the
/// exponent is small, scientific notation otherwise, and every digit that was written.
///
/// Behavior: java.math.BigDecimal#BigDecimal(String) and #toString
fn big_decimal_text(text: &str) -> String {
    let (negative, text) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match text.find(['e', 'E']) {
        Some(at) => (&text[..at], text[at + 1..].parse::<i64>().unwrap_or(0)),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let scale = fraction.len() as i64 - exponent;
    let digits = format!("{whole}{fraction}");
    let digits = match digits.trim_start_matches('0') {
        "" => "0",
        rest => rest,
    };
    // Zero has no sign.
    let sign = if negative && digits != "0" { "-" } else { "" };
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

#[cfg(test)]
mod tests {
    use super::*;

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
            ("0.00", "0.00"),
        ] {
            assert_eq!(big_decimal_text(written), java, "{written}");
        }
    }

    #[test]
    fn fields_are_read_by_kind_and_the_last_duplicate_wins() {
        let document = Document::parse(
            r#" {"a": 1, "b": -2.50, "c": "x", "d": true, "e": null, "f": {"g": 1}, "a": 7} trailing"#,
        )
        .unwrap();
        assert_eq!(document.field("a").unwrap(), Node::Integer(7));
        assert_eq!(document.field("b").unwrap(), Node::Decimal("-2.50".into()));
        assert_eq!(document.field("c").unwrap(), Node::Text("x".into()));
        assert_eq!(document.field("d").unwrap(), Node::Bool(true));
        assert_eq!(document.field("e").unwrap(), Node::Null);
        assert_eq!(document.field("f").unwrap(), Node::Container);
        assert_eq!(document.field("missing").unwrap(), Node::Null);
    }

    #[test]
    fn text_without_an_object_has_no_fields_and_bad_json_is_an_error() {
        for text in ["", "  ", "5", "\"x\"", "null"] {
            assert_eq!(
                Document::parse(text).unwrap().field("a").unwrap(),
                Node::Null
            );
        }
        let error = Document::parse("{\"a\": }").err().unwrap().to_string();
        assert!(error.contains("Failed to parse JSON string."), "{error}");
        assert!(Document::parse("nope").is_err());
        assert!(Document::parse("[1]")
            .err()
            .unwrap()
            .to_string()
            .contains("ArrayNode#_at"));
    }

    #[test]
    fn conversions_coerce_as_jackson_does() {
        assert_eq!(Node::Bool(true).integral("asInt").unwrap(), Some(1));
        assert_eq!(Node::Container.integral("asInt").unwrap(), Some(0));
        assert_eq!(Node::Null.integral("asInt").unwrap(), None);
        let refused = Node::Decimal("1.5".into())
            .integral("asLong")
            .err()
            .unwrap();
        assert!(
            refused.to_string().contains("DecimalNode#asLong"),
            "{refused}"
        );
        assert!(Node::Text("1".into()).integral("asInt").is_err());

        assert_eq!(Node::Integer(3).double().unwrap(), Some(3.0));
        assert_eq!(
            Node::Decimal("1e400".into()).double().unwrap(),
            Some(f64::INFINITY)
        );
        assert_eq!(Node::Bool(true).double().unwrap(), Some(1.0));
        assert!(Node::Text("1.5".into()).double().is_err());

        assert_eq!(
            Node::Text(" true\n".into()).boolean("B").unwrap(),
            Some(true)
        );
        assert_eq!(Node::Text("F".into()).boolean("B").unwrap(), Some(false));
        assert_eq!(Node::Integer(1).boolean("B").unwrap(), Some(true));
        assert_eq!(Node::Text(" ".into()).boolean("B").unwrap(), None);
        assert_eq!(Node::Null.boolean("B").unwrap(), None);
        assert_eq!(
            Node::Integer(0).boolean("a/b").unwrap_err().to_string(),
            "io.deephaven.UncheckedDeephavenException: value 0 not recognized as Boolean for \
             field /a~1b"
        );
        assert!(Node::Decimal("1.0".into()).boolean("B").is_err());

        assert_eq!(Node::Integer(-7).text().unwrap().as_deref(), Some("-7"));
        assert_eq!(Node::Bool(false).text().unwrap().as_deref(), Some("false"));
        assert!(Node::Container.text().is_err());
    }
}
