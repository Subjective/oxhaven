//! Avro: schemas as Java's `org.apache.avro.Schema.Parser` (Avro 1.12) reads them, and
//! the binary encoding as `GenericDatumReader` reads it through a `ResolvingDecoder`, from
//! the schema a record was written with to the schema of the spec.
//!
//! Only what the Avro specs need is kept of a schema: the types, the names, the fields
//! with their defaults and aliases, and the two logical types Deephaven maps to other
//! columns (`timestamp-millis` and `timestamp-micros` on a `long`, `decimal` on `bytes` or
//! `fixed`). Every other logical type leaves its type as it is, as in Java.

use crate::json::{self, Node};

/// The text of a failure, with the Java exception's class first.
pub(crate) type Failure = String;

/// A timestamp in a `long`: the number of nanoseconds in one unit.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Stamp {
    Millis,
    Micros,
}

impl Stamp {
    pub(crate) fn nanos(self) -> i64 {
        match self {
            Stamp::Millis => 1_000_000,
            Stamp::Micros => 1_000,
        }
    }
}

/// A type of a schema. Named types are in [`Schema::named`], by index, so that a record
/// may contain itself.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Ty {
    Null,
    Boolean,
    /// Any logical type on an `int` (`date`, `time-millis`) leaves it an `int` in Java.
    Int,
    Long(Option<Stamp>),
    Float,
    Double,
    /// The scale of a valid `decimal`.
    Bytes(Option<i32>),
    String,
    Array(Box<Ty>),
    Map(Box<Ty>),
    Union(Vec<Ty>),
    Named(usize),
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) aliases: Vec<String>,
    pub(crate) ty: Ty,
    pub(crate) default: Option<Node>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Named {
    Record {
        name: String,
        fields: Vec<Field>,
    },
    Enum {
        name: String,
        symbols: Vec<String>,
        default: Option<String>,
    },
    Fixed {
        name: String,
        size: usize,
        /// The scale of a valid `decimal`.
        decimal: Option<i32>,
    },
}

impl Named {
    fn full_name(&self) -> &str {
        match self {
            Named::Record { name, .. } | Named::Enum { name, .. } | Named::Fixed { name, .. } => {
                name
            }
        }
    }
}

/// A parsed schema.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Schema {
    pub(crate) named: Vec<Named>,
    pub(crate) root: Ty,
}

const PARSE: &str = "org.apache.avro.SchemaParseException";
const TYPE: &str = "org.apache.avro.AvroTypeException";
const RUNTIME: &str = "org.apache.avro.AvroRuntimeException";

/// Behavior: org.apache.avro.Schema.Type#getName, upper case as `Type#toString` and
/// `Field#toString` write it
fn type_word(schema: &Schema, ty: &Ty) -> &'static str {
    match ty {
        Ty::Null => "null",
        Ty::Boolean => "boolean",
        Ty::Int => "int",
        Ty::Long(_) => "long",
        Ty::Float => "float",
        Ty::Double => "double",
        Ty::Bytes(_) => "bytes",
        Ty::String => "string",
        Ty::Array(_) => "array",
        Ty::Map(_) => "map",
        Ty::Union(_) => "union",
        Ty::Named(i) => match schema.named[*i] {
            Named::Record { .. } => "record",
            Named::Enum { .. } => "enum",
            Named::Fixed { .. } => "fixed",
        },
    }
}

impl Schema {
    /// `Schema#getFullName`: the full name of a named type, the type's name otherwise.
    fn full_name<'a>(&'a self, ty: &Ty) -> &'a str {
        match ty {
            Ty::Named(i) => self.named[*i].full_name(),
            other => type_word(self, other),
        }
    }

    /// Whether `ty` is a union, after following nothing: unions are never named.
    pub(crate) fn record_fields(&self, ty: &Ty) -> Option<&[Field]> {
        match ty {
            Ty::Named(i) => match &self.named[*i] {
                Named::Record { fields, .. } => Some(fields),
                _ => None,
            },
            _ => None,
        }
    }
}

/// Reads a schema as `new Schema.Parser().parse(text)` does.
///
/// Behavior: org.apache.avro.Schema.Parser#parse(String), org.apache.avro.ParseContext,
/// org.apache.avro.Schema#parse(JsonNode, ParseContext, String), org.apache.avro.LogicalTypes#fromSchemaIgnoreInvalid
pub(crate) fn parse(text: &str) -> Result<Schema, Failure> {
    let (node, rest) = json::parse_prefix(text).map_err(|detail| format!("{PARSE}: {detail}"))?;
    if matches!(node, Node::Missing) {
        return Err(format!("{PARSE}: No content to map due to end-of-input"));
    }
    let rest = rest.trim_matches([' ', '\t', '\n', '\r']);
    if !rest.is_empty() {
        return Err(format!(
            "{PARSE}: dangling content after end of schema: {rest}"
        ));
    }
    let mut parser = SchemaParser::default();
    let root = parser.ty(&node, "")?;
    let mut schema = Schema {
        named: parser
            .named
            .into_iter()
            .map(|n| n.expect("defined"))
            .collect(),
        root,
    };
    // Names may be used before their definition: resolve them now.
    let (refs, names) = (parser.refs, parser.names);
    let resolve = |ty: &mut Ty| resolve_refs(ty, &refs, &names);
    resolve(&mut schema.root)?;
    for named in &mut schema.named {
        if let Named::Record { fields, .. } = named {
            for field in fields {
                resolve(&mut field.ty)?;
            }
        }
    }
    // Defaults are checked once every name is known.
    for i in 0..schema.named.len() {
        if let Named::Record { fields, .. } = &schema.named[i] {
            for field in fields {
                if let Some(default) = &field.default {
                    if !valid_default(&schema, &field.ty, default) {
                        return Err(format!(
                            "{TYPE}: Invalid default for field {}: {} not a {}",
                            field.name,
                            json::compact(default),
                            schema_json(&schema, &field.ty)
                        ));
                    }
                }
            }
        }
    }
    Ok(schema)
}

/// A reference to a named type before it is resolved: `Ty::Named(REF_BASE + i)` for the
/// candidates `refs[i]`.
const REF_BASE: usize = usize::MAX / 2;

#[derive(Default)]
struct SchemaParser {
    named: Vec<Option<Named>>,
    /// The full names of `named`, with their indexes.
    names: Vec<(String, usize)>,
    /// References not resolved when met: the full names to try, in order.
    refs: Vec<Vec<String>>,
}

fn resolve_refs(
    ty: &mut Ty,
    refs: &[Vec<String>],
    names: &[(String, usize)],
) -> Result<(), Failure> {
    match ty {
        Ty::Array(inner) | Ty::Map(inner) => resolve_refs(inner, refs, names),
        Ty::Union(branches) => branches
            .iter_mut()
            .try_for_each(|b| resolve_refs(b, refs, names)),
        Ty::Named(i) if *i >= REF_BASE => {
            let candidates = &refs[*i - REF_BASE];
            for candidate in candidates {
                if let Some((_, index)) = names.iter().find(|(n, _)| n == candidate) {
                    *i = *index;
                    return Ok(());
                }
            }
            Err(format!("{TYPE}: Undefined schema: {}", candidates[0]))
        }
        _ => Ok(()),
    }
}

/// Behavior: org.apache.avro.Schema.Name#validate with `NameValidator.UTF_VALIDATOR`
fn validate_name(name: &str) -> Result<(), Failure> {
    let mut chars = name.chars();
    match chars.next() {
        None => Err(format!("{PARSE}: Empty name")),
        Some(first) if !(first.is_alphabetic() || first == '_') => {
            Err(format!("{PARSE}: Illegal initial character: {name}"))
        }
        Some(_) if chars.any(|c| !(c.is_alphanumeric() || c == '_')) => {
            Err(format!("{PARSE}: Illegal character in: {name}"))
        }
        Some(_) => Ok(()),
    }
}

fn primitive(name: &str) -> Option<Ty> {
    Some(match name {
        "null" => Ty::Null,
        "boolean" => Ty::Boolean,
        "int" => Ty::Int,
        "long" => Ty::Long(None),
        "float" => Ty::Float,
        "double" => Ty::Double,
        "bytes" => Ty::Bytes(None),
        "string" => Ty::String,
        _ => return None,
    })
}

/// The decimal of `logicalType` and its properties, when valid; `None` when Java ignores
/// it as invalid.
///
/// Behavior: org.apache.avro.LogicalTypes.Decimal#validate and #maxPrecision
fn decimal(node: &Node, fixed_size: Option<usize>) -> Option<i32> {
    if node.get("logicalType").and_then(Node::as_str) != Some("decimal") {
        return None;
    }
    let int = |name: &str| -> Option<Option<i64>> {
        match node.get(name) {
            None => Some(None),
            Some(value) => value
                .exact_long()
                .filter(|v| i32::try_from(*v).is_ok())
                .map(Some),
        }
    };
    let precision = int("precision")??;
    let scale = int("scale")?.unwrap_or(0);
    if precision <= 0 || scale < 0 || scale > precision {
        return None;
    }
    if let Some(size) = fixed_size {
        // floor(log10(2^(8 * size - 1) - 1))
        let bits = 8 * size as u32;
        let max = if bits >= 128 {
            i64::MAX
        } else {
            let limit = (1u128 << (bits - 1)) - 1;
            (limit as f64).log10().floor() as i64
        };
        if precision > max {
            return None;
        }
    }
    Some(scale as i32)
}

impl SchemaParser {
    fn define(&mut self, full: &str, named: Named) -> usize {
        let index = self.named.len();
        self.named.push(Some(named));
        self.names.push((full.to_owned(), index));
        index
    }

    fn reference(&mut self, name: &str, namespace: &str) -> Ty {
        let mut candidates = Vec::new();
        if !name.contains('.') && !namespace.is_empty() {
            candidates.push(format!("{namespace}.{name}"));
        }
        candidates.push(name.to_owned());
        // Known already: resolve at once.
        for candidate in &candidates {
            if let Some((_, index)) = self.names.iter().find(|(n, _)| n == candidate) {
                return Ty::Named(*index);
            }
        }
        let index = REF_BASE + self.refs.len();
        self.refs.push(candidates);
        Ty::Named(index)
    }

    /// The full name and the namespace of a named type's definition.
    fn full_name(node: &Node, namespace: &str) -> Result<(String, String), Failure> {
        let Some(name) = node.get("name").and_then(Node::as_str) else {
            return Err(format!(
                "{PARSE}: No name in schema: {}",
                json::compact(node)
            ));
        };
        let space = match node.get("namespace").and_then(Node::as_str) {
            Some(space) => space.to_owned(),
            None => namespace.to_owned(),
        };
        let (space, short) = match name.rfind('.') {
            Some(dot) => (name[..dot].to_owned(), &name[dot + 1..]),
            None => (space, name),
        };
        validate_name(short)?;
        if !space.is_empty() {
            for part in space.split('.') {
                validate_name(part)?;
            }
        }
        let full = if space.is_empty() {
            short.to_owned()
        } else {
            format!("{space}.{short}")
        };
        Ok((full, space))
    }

    fn check_new(&self, full: &str) -> Result<(), Failure> {
        if primitive(full).is_some() || self.names.iter().any(|(n, _)| n == full) {
            return Err(format!("{PARSE}: Can't redefine: {full}"));
        }
        Ok(())
    }

    fn ty(&mut self, node: &Node, namespace: &str) -> Result<Ty, Failure> {
        match node {
            Node::Text(_) => {
                let name = node.as_str().unwrap_or_default();
                Ok(primitive(name).unwrap_or_else(|| self.reference(name, namespace)))
            }
            Node::Array(branches) => {
                let mut types = Vec::new();
                let mut seen: Vec<String> = Vec::new();
                for branch in branches {
                    let ty = self.ty(branch, namespace)?;
                    if matches!(ty, Ty::Union(_)) {
                        return Err(format!("{RUNTIME}: Nested union: {}", json::compact(node)));
                    }
                    let key = match (&ty, branch) {
                        (Ty::Named(i), _) if *i < REF_BASE => self.named[*i]
                            .as_ref()
                            .map_or_else(String::new, |n| n.full_name().to_owned()),
                        (Ty::Named(_), Node::Text(_)) => {
                            branch.as_str().unwrap_or_default().to_owned()
                        }
                        (other, _) => type_word(
                            &Schema {
                                named: vec![],
                                root: Ty::Null,
                            },
                            other,
                        )
                        .to_owned(),
                    };
                    if seen.contains(&key) {
                        return Err(format!("{RUNTIME}: Duplicate in union:{key}"));
                    }
                    seen.push(key);
                    types.push(ty);
                }
                Ok(Ty::Union(types))
            }
            Node::Object(_) => self.complex(node, namespace),
            _ => Err(format!(
                "{PARSE}: Schema not yet supported: {}",
                json::compact(node)
            )),
        }
    }

    fn complex(&mut self, node: &Node, namespace: &str) -> Result<Ty, Failure> {
        // `{"type": {...}}` and `{"type": [...]}` are refused too (probes s_nested_type,
        // s_top_nested, s_type_array).
        let Some(kind) = node.get("type").and_then(Node::as_str) else {
            return Err(format!("{PARSE}: No type: {}", json::compact(node)));
        };
        if let Some(ty) = primitive(kind) {
            let logical = node.get("logicalType").and_then(Node::as_str);
            return Ok(match ty {
                Ty::Long(_) => Ty::Long(match logical {
                    Some("timestamp-millis") => Some(Stamp::Millis),
                    Some("timestamp-micros") => Some(Stamp::Micros),
                    _ => None,
                }),
                Ty::Bytes(_) => Ty::Bytes(decimal(node, None)),
                other => other,
            });
        }
        match kind {
            "record" | "error" => self.record(node, namespace),
            "enum" => self.enumeration(node, namespace),
            "fixed" => {
                let (full, _) = Self::full_name(node, namespace)?;
                let size = node
                    .get("size")
                    .and_then(Node::exact_long)
                    .filter(|size| (0..=i32::MAX as i64).contains(size));
                let Some(size) = size else {
                    return Err(format!(
                        "{PARSE}: Invalid or no size: {}",
                        json::compact(node)
                    ));
                };
                self.check_new(&full)?;
                let size = size as usize;
                let decimal = decimal(node, Some(size));
                Ok(Ty::Named(self.define(
                    &full,
                    Named::Fixed {
                        name: full.clone(),
                        size,
                        decimal,
                    },
                )))
            }
            "array" => match node.get("items") {
                None => Err(format!(
                    "{PARSE}: Array has no items type: {}",
                    json::compact(node)
                )),
                Some(items) => Ok(Ty::Array(Box::new(self.ty(items, namespace)?))),
            },
            "map" => match node.get("values") {
                None => Err(format!(
                    "{PARSE}: Map has no values type: {}",
                    json::compact(node)
                )),
                Some(values) => Ok(Ty::Map(Box::new(self.ty(values, namespace)?))),
            },
            other => Ok(self.reference(other, namespace)),
        }
    }

    fn enumeration(&mut self, node: &Node, namespace: &str) -> Result<Ty, Failure> {
        let (full, _) = Self::full_name(node, namespace)?;
        let Some(Node::Array(items)) = node.get("symbols") else {
            return Err(format!(
                "{PARSE}: Enum has no symbols: {}",
                json::compact(node)
            ));
        };
        let mut symbols: Vec<String> = Vec::new();
        for item in items {
            let symbol = item.as_str().unwrap_or_default().to_owned();
            validate_name(&symbol)?;
            if symbols.contains(&symbol) {
                return Err(format!("{PARSE}: Duplicate enum symbol: {symbol}"));
            }
            symbols.push(symbol);
        }
        let default = node
            .get("default")
            .and_then(Node::as_str)
            .map(str::to_owned);
        if let Some(default) = &default {
            if !symbols.contains(default) {
                return Err(format!(
                    "{PARSE}: The Enum Default: {default} is not in the enum symbol set: [{}]",
                    symbols.join(", ")
                ));
            }
        }
        self.check_new(&full)?;
        Ok(Ty::Named(self.define(
            &full,
            Named::Enum {
                name: full.clone(),
                symbols,
                default,
            },
        )))
    }

    fn record(&mut self, node: &Node, namespace: &str) -> Result<Ty, Failure> {
        let (full, space) = Self::full_name(node, namespace)?;
        self.check_new(&full)?;
        let Some(Node::Array(items)) = node.get("fields") else {
            return Err(format!(
                "{PARSE}: Record has no fields: {}",
                json::compact(node)
            ));
        };
        // Defined before its fields, which may name it.
        let index = self.define(
            &full,
            Named::Record {
                name: full.clone(),
                fields: Vec::new(),
            },
        );
        let mut fields: Vec<Field> = Vec::new();
        for item in items {
            let Some(name) = item.get("name").and_then(Node::as_str) else {
                return Err(format!("{PARSE}: No field name: {}", json::compact(item)));
            };
            validate_name(name)?;
            let Some(type_node) = item.get("type") else {
                return Err(format!("{PARSE}: No field type: {}", json::compact(item)));
            };
            let ty = self.ty(type_node, &space)?;
            if let Some(old) = fields.iter().position(|f| f.name == name) {
                let word = |ty: &Ty| -> String {
                    match ty {
                        Ty::Named(i) if *i < REF_BASE => match self.named[*i] {
                            Some(Named::Record { .. }) | None => "RECORD".into(),
                            Some(Named::Enum { .. }) => "ENUM".into(),
                            Some(Named::Fixed { .. }) => "FIXED".into(),
                        },
                        Ty::Named(_) => "RECORD".into(),
                        other => type_word(
                            &Schema {
                                named: vec![],
                                root: Ty::Null,
                            },
                            other,
                        )
                        .to_uppercase(),
                    }
                };
                return Err(format!(
                    "{RUNTIME}: Duplicate field {name} in record {full}: {name} type:{} pos:{} \
                     and {name} type:{} pos:{old}.",
                    word(&ty),
                    fields.len(),
                    word(&fields[old].ty)
                ));
            }
            let aliases = match item.get("aliases") {
                Some(Node::Array(names)) => names
                    .iter()
                    .filter_map(Node::as_str)
                    .map(str::to_owned)
                    .collect(),
                _ => Vec::new(),
            };
            let default = item.get("default").cloned();
            fields.push(Field {
                name: name.to_owned(),
                aliases,
                ty,
                default,
            });
        }
        self.named[index] = Some(Named::Record { name: full, fields });
        Ok(Ty::Named(index))
    }
}

/// The schema of `ty` as JSON, as the message of an invalid default shows it.
fn schema_json(schema: &Schema, ty: &Ty) -> String {
    match ty {
        Ty::Named(i) => format!("\"{}\"", schema.named[*i].full_name()),
        Ty::Array(inner) => format!(
            "{{\"type\":\"array\",\"items\":{}}}",
            schema_json(schema, inner)
        ),
        Ty::Map(inner) => format!(
            "{{\"type\":\"map\",\"values\":{}}}",
            schema_json(schema, inner)
        ),
        Ty::Union(branches) => {
            let parts: Vec<String> = branches.iter().map(|b| schema_json(schema, b)).collect();
            format!("[{}]", parts.join(","))
        }
        other => format!("\"{}\"", type_word(schema, other)),
    }
}

/// Behavior: org.apache.avro.Schema#isValidDefault
fn valid_default(schema: &Schema, ty: &Ty, value: &Node) -> bool {
    match ty {
        Ty::Null => matches!(value, Node::Null),
        Ty::Boolean => matches!(value, Node::Bool(_)),
        Ty::Int => value.exact_long().is_some_and(|v| i32::try_from(v).is_ok()),
        Ty::Long(_) => value.exact_long().is_some(),
        Ty::Float | Ty::Double => value.is_number(),
        Ty::Bytes(_) | Ty::String => value.as_str().is_some(),
        Ty::Array(inner) => match value {
            Node::Array(items) => items.iter().all(|item| valid_default(schema, inner, item)),
            _ => false,
        },
        Ty::Map(inner) => match value {
            Node::Object(fields) => fields.iter().all(|(_, v)| valid_default(schema, inner, v)),
            _ => false,
        },
        Ty::Union(branches) => branches.iter().any(|b| valid_default(schema, b, value)),
        Ty::Named(i) => match &schema.named[*i] {
            Named::Enum { .. } | Named::Fixed { .. } => value.as_str().is_some(),
            Named::Record { fields, .. } => {
                matches!(value, Node::Object(_))
                    && fields.iter().all(|field| match value.get(&field.name) {
                        Some(v) => valid_default(schema, &field.ty, v),
                        None => field.default.is_some(),
                    })
            }
        },
    }
}

/// A value read with the reader's schema. A union's value is its branch's.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Value {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bytes(Vec<u8>),
    /// Java's `Utf8#toString`: the bytes as UTF-8, with U+FFFD for what is not.
    String(String),
    /// The symbol.
    Enum(String),
    Fixed(Vec<u8>),
    Array(Vec<Value>),
    Map(Vec<(String, Value)>),
    /// The fields in the reader's order.
    Record(Vec<Value>),
}

/// UTF-8 decoded with a U+FFFD for each malformed sequence. This is Rust's lossy decoding
/// except for an encoded surrogate (`ED` then `A0`..`BF`), which Java takes as one
/// sequence: one U+FFFD for its three bytes, or for its first two when the third is not
/// a continuation byte (probes utf_surr, utf_surr_lo, utf_surr_pair, utf_ed_x, utf_ed_end).
///
/// Behavior: org.apache.avro.util.Utf8#toString, java.lang.String(byte[], UTF_8)
pub(crate) fn java_utf8(bytes: &[u8]) -> String {
    let surrogate = |at: usize| bytes[at] == 0xED && matches!(bytes.get(at + 1), Some(0xA0..=0xBF));
    let mut out = String::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if surrogate(at) {
            out.push('\u{FFFD}');
            at += if matches!(bytes.get(at + 2), Some(0x80..=0xBF)) {
                3
            } else {
                2
            };
            continue;
        }
        // `ED` is never a continuation byte, so cutting before it ends no sequence early.
        let end = (at + 1..bytes.len())
            .find(|&i| surrogate(i))
            .unwrap_or(bytes.len());
        out.push_str(&String::from_utf8_lossy(&bytes[at..end]));
        at = end;
    }
    out
}

/// Reads the binary encoding.
///
/// Behavior: org.apache.avro.io.BinaryDecoder
pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    at: usize,
}

const EOF: &str = "java.io.EOFException";

impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn byte(&mut self) -> Result<u8, Failure> {
        let b = *self.bytes.get(self.at).ok_or_else(|| EOF.to_owned())?;
        self.at += 1;
        Ok(b)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Failure> {
        let end = self
            .at
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len());
        let end = end.ok_or_else(|| EOF.to_owned())?;
        let taken = &self.bytes[self.at..end];
        self.at = end;
        Ok(taken)
    }

    /// Behavior: BinaryDecoder#readLong: a zig-zag varint of at most ten bytes
    fn long(&mut self) -> Result<i64, Failure> {
        let mut raw: u64 = 0;
        for i in 0..10 {
            let b = self.byte()?;
            raw |= u64::from(b & 0x7f) << (7 * i);
            if b & 0x80 == 0 {
                return Ok((raw >> 1) as i64 ^ -((raw & 1) as i64));
            }
        }
        Err(format!("{RUNTIME}: Invalid long encoding"))
    }

    /// Behavior: BinaryDecoder#readInt: a zig-zag varint of at most five bytes
    fn int(&mut self) -> Result<i32, Failure> {
        let mut raw: u32 = 0;
        for i in 0..5 {
            let b = self.byte()?;
            raw |= u32::from(b & 0x7f).wrapping_shl(7 * i);
            if b & 0x80 == 0 {
                return Ok((raw >> 1) as i32 ^ -((raw & 1) as i32));
            }
        }
        Err(format!("{RUNTIME}: Invalid int encoding"))
    }

    /// Behavior: BinaryDecoder#readBoolean: one byte, true only when it is 1
    fn boolean(&mut self) -> Result<bool, Failure> {
        Ok(self.byte()? == 1)
    }

    fn float(&mut self) -> Result<f32, Failure> {
        let b = self.take(4)?;
        Ok(f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn double(&mut self) -> Result<f64, Failure> {
        let b = self.take(8)?;
        Ok(f64::from_le_bytes(b.try_into().expect("eight bytes")))
    }

    /// Behavior: BinaryDecoder#readBytes and #readString, which refuse a negative length
    fn bytes(&mut self) -> Result<&'a [u8], Failure> {
        let length = self.long()?;
        if length < 0 {
            return Err(format!(
                "{RUNTIME}: Malformed data. Length is negative: {length}"
            ));
        }
        let length = usize::try_from(length).map_err(|_| EOF.to_owned())?;
        self.take(length)
    }

    /// The item count of the next block of an array or a map; a negative count is
    /// followed by the block's size in bytes.
    fn block(&mut self) -> Result<u64, Failure> {
        let count = self.long()?;
        if count < 0 {
            self.long()?;
        }
        Ok(count.unsigned_abs())
    }
}

fn mismatch(writer: &Schema, w: &Ty, reader: &Schema, r: &Ty) -> Failure {
    format!(
        "{TYPE}: Found {}, expecting {}",
        writer.full_name(w),
        reader.full_name(r)
    )
}

fn enum_index(d: &mut Decoder<'_>, count: usize) -> Result<usize, Failure> {
    let index = d.int()?;
    usize::try_from(index)
        .ok()
        .filter(|i| *i < count)
        .ok_or_else(|| format!("{RUNTIME}: Malformed data. Index {index} is out of range"))
}

/// Reads a value written with `writer`'s `w`, as the reader's `r`.
///
/// Behavior: org.apache.avro.io.parsing.ResolvingGrammarGenerator#generate, #resolveRecords,
/// #resolveUnion and #bestBranch, as `org.apache.avro.generic.GenericDatumReader#read`
/// follows them: the same type, the promotions `int` to `long`, `float` or `double`,
/// `long` to `float` or `double`, `float` to `double`, `string` to `bytes` and back; a
/// writer's union by its branch; a reader's union by its best branch
pub(crate) fn read(
    d: &mut Decoder<'_>,
    writer: &Schema,
    w: &Ty,
    reader: &Schema,
    r: &Ty,
) -> Result<Value, Failure> {
    if let Ty::Union(branches) = w {
        let index = enum_index(d, branches.len())?;
        return read(d, writer, &branches[index], reader, r);
    }
    match (w, r) {
        (Ty::Null, Ty::Null) => Ok(Value::Null),
        (Ty::Boolean, Ty::Boolean) => Ok(Value::Boolean(d.boolean()?)),
        (Ty::Int, Ty::Int) => Ok(Value::Int(d.int()?)),
        (Ty::Int, Ty::Long(_)) => Ok(Value::Long(i64::from(d.int()?))),
        (Ty::Int, Ty::Float) => Ok(Value::Float(d.int()? as f32)),
        (Ty::Int, Ty::Double) => Ok(Value::Double(f64::from(d.int()?))),
        (Ty::Long(_), Ty::Long(_)) => Ok(Value::Long(d.long()?)),
        (Ty::Long(_), Ty::Float) => Ok(Value::Float(d.long()? as f32)),
        (Ty::Long(_), Ty::Double) => Ok(Value::Double(d.long()? as f64)),
        (Ty::Float, Ty::Float) => Ok(Value::Float(d.float()?)),
        (Ty::Float, Ty::Double) => Ok(Value::Double(f64::from(d.float()?))),
        (Ty::Double, Ty::Double) => Ok(Value::Double(d.double()?)),
        (Ty::Bytes(_) | Ty::String, Ty::Bytes(_)) => Ok(Value::Bytes(d.bytes()?.to_vec())),
        (Ty::Bytes(_) | Ty::String, Ty::String) => Ok(Value::String(java_utf8(d.bytes()?))),
        (Ty::Array(wi), Ty::Array(ri)) => {
            let mut items = Vec::new();
            loop {
                let count = d.block()?;
                if count == 0 {
                    break;
                }
                for _ in 0..count {
                    items.push(read(d, writer, wi, reader, ri)?);
                }
            }
            Ok(Value::Array(items))
        }
        (Ty::Map(wi), Ty::Map(ri)) => {
            let mut entries = Vec::new();
            loop {
                let count = d.block()?;
                if count == 0 {
                    break;
                }
                for _ in 0..count {
                    let key = java_utf8(d.bytes()?);
                    entries.push((key, read(d, writer, wi, reader, ri)?));
                }
            }
            Ok(Value::Map(entries))
        }
        (_, Ty::Union(branches)) => match best_branch(writer, w, reader, branches) {
            Some(j) => read(d, writer, w, reader, &branches[j]),
            None => Err(mismatch(writer, w, reader, r)),
        },
        (Ty::Named(wi), Ty::Named(ri)) => read_named(d, writer, *wi, reader, *ri, w, r),
        _ => Err(mismatch(writer, w, reader, r)),
    }
}

fn read_named(
    d: &mut Decoder<'_>,
    writer: &Schema,
    wi: usize,
    reader: &Schema,
    ri: usize,
    w: &Ty,
    r: &Ty,
) -> Result<Value, Failure> {
    match (&writer.named[wi], &reader.named[ri]) {
        (
            Named::Fixed {
                name: wn, size: ws, ..
            },
            Named::Fixed {
                name: rn, size: rs, ..
            },
        ) => {
            if wn != rn || ws != rs {
                return Err(mismatch(writer, w, reader, r));
            }
            Ok(Value::Fixed(d.take(*ws)?.to_vec()))
        }
        (
            Named::Enum {
                name: wn,
                symbols: wsym,
                ..
            },
            Named::Enum {
                name: rn,
                symbols: rsym,
                default,
                ..
            },
        ) => {
            if wn != rn {
                return Err(mismatch(writer, w, reader, r));
            }
            let symbol = &wsym[enum_index(d, wsym.len())?];
            if rsym.contains(symbol) {
                Ok(Value::Enum(symbol.clone()))
            } else if let Some(default) = default {
                Ok(Value::Enum(default.clone()))
            } else {
                Err(format!("{TYPE}: No match for {symbol}"))
            }
        }
        (
            Named::Record {
                name: wn,
                fields: wf,
            },
            Named::Record {
                name: rn,
                fields: rf,
            },
        ) => {
            // A reader's field without a writer's field needs a default.
            let source = |field: &Field| {
                wf.iter()
                    .position(|w| w.name == field.name || field.aliases.contains(&w.name))
            };
            if let Some(missing) = rf
                .iter()
                .find(|f| source(f).is_none() && f.default.is_none())
            {
                return Err(format!(
                    "{TYPE}: Found {wn}, expecting {rn}, missing required field {}",
                    missing.name
                ));
            }
            let mut values: Vec<Option<Value>> = vec![None; rf.len()];
            for (position, field) in wf.iter().enumerate() {
                let target = rf.iter().position(|f| source(f) == Some(position));
                match target {
                    Some(t) => {
                        let value = read(d, writer, &field.ty, reader, &rf[t].ty).map_err(|e| {
                            match e.strip_prefix(&format!("{TYPE}: Found ")) {
                                Some(rest) if !rest.contains(", missing required field") => {
                                    format!(
                                        "{TYPE}: Field \"{}\" content mismatch: Found {rest}",
                                        rf[t].name
                                    )
                                }
                                _ => e,
                            }
                        })?;
                        values[t] = Some(value);
                    }
                    None => skip(d, writer, &field.ty)?,
                }
            }
            let mut out = Vec::with_capacity(rf.len());
            for (field, value) in rf.iter().zip(values) {
                out.push(match value {
                    Some(value) => value,
                    None => {
                        let default = field.default.as_ref().expect("checked above");
                        default_value(reader, &field.ty, default)?
                    }
                });
            }
            Ok(Value::Record(out))
        }
        _ => Err(mismatch(writer, w, reader, r)),
    }
}

/// Skips a value written with `w`.
fn skip(d: &mut Decoder<'_>, writer: &Schema, w: &Ty) -> Result<(), Failure> {
    match w {
        Ty::Null => {}
        Ty::Boolean => {
            d.byte()?;
        }
        Ty::Int => {
            d.int()?;
        }
        Ty::Long(_) => {
            d.long()?;
        }
        Ty::Float => {
            d.take(4)?;
        }
        Ty::Double => {
            d.take(8)?;
        }
        Ty::Bytes(_) | Ty::String => {
            d.bytes()?;
        }
        Ty::Array(inner) => loop {
            let count = d.block()?;
            if count == 0 {
                break;
            }
            for _ in 0..count {
                skip(d, writer, inner)?;
            }
        },
        Ty::Map(inner) => loop {
            let count = d.block()?;
            if count == 0 {
                break;
            }
            for _ in 0..count {
                d.bytes()?;
                skip(d, writer, inner)?;
            }
        },
        Ty::Union(branches) => {
            let index = enum_index(d, branches.len())?;
            skip(d, writer, &branches[index])?;
        }
        Ty::Named(i) => match &writer.named[*i] {
            Named::Fixed { size, .. } => {
                d.take(*size)?;
            }
            Named::Enum { symbols, .. } => {
                enum_index(d, symbols.len())?;
            }
            Named::Record { fields, .. } => {
                for field in fields {
                    skip(d, writer, &field.ty)?;
                }
            }
        },
    }
    Ok(())
}

/// The kind of a type, as `Schema#getType` gives it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array,
    Map,
    Union,
    Record,
    Enum,
    Fixed,
}

fn kind(schema: &Schema, ty: &Ty) -> Kind {
    match ty {
        Ty::Null => Kind::Null,
        Ty::Boolean => Kind::Boolean,
        Ty::Int => Kind::Int,
        Ty::Long(_) => Kind::Long,
        Ty::Float => Kind::Float,
        Ty::Double => Kind::Double,
        Ty::Bytes(_) => Kind::Bytes,
        Ty::String => Kind::String,
        Ty::Array(_) => Kind::Array,
        Ty::Map(_) => Kind::Map,
        Ty::Union(_) => Kind::Union,
        Ty::Named(i) => match schema.named[*i] {
            Named::Record { .. } => Kind::Record,
            Named::Enum { .. } => Kind::Enum,
            Named::Fixed { .. } => Kind::Fixed,
        },
    }
}

fn short_name(full: &str) -> &str {
    full.rsplit('.').next().unwrap_or(full)
}

/// The branch of the reader's union that a writer's type is read as.
///
/// Behavior: org.apache.avro.io.parsing.ResolvingGrammarGenerator#bestBranch
fn best_branch(writer: &Schema, w: &Ty, reader: &Schema, branches: &[Ty]) -> Option<usize> {
    let wk = kind(writer, w);
    let mut structure = None;
    for (j, b) in branches.iter().enumerate() {
        if kind(reader, b) != wk {
            continue;
        }
        if !matches!(wk, Kind::Record | Kind::Enum | Kind::Fixed) {
            return Some(j);
        }
        let (wn, bn) = (writer.full_name(w), reader.full_name(b));
        if wn == bn {
            return Some(j);
        }
        if wk == Kind::Record
            && resolvable(writer, w, reader, b, &mut Vec::new())
            && (structure.is_none() || short_name(wn) == short_name(bn))
        {
            structure = Some(j);
        }
    }
    if structure.is_some() {
        return structure;
    }
    branches.iter().position(|b| {
        matches!(
            (wk, kind(reader, b)),
            (Kind::Int, Kind::Long | Kind::Double | Kind::Float)
                | (Kind::Long, Kind::Double | Kind::Float)
                | (Kind::Float, Kind::Double)
                | (Kind::String, Kind::Bytes)
                | (Kind::Bytes, Kind::String)
        )
    })
}

/// Whether reading `w` as `r` can succeed: Java's `hasMatchError` is false.
fn resolvable(
    writer: &Schema,
    w: &Ty,
    reader: &Schema,
    r: &Ty,
    seen: &mut Vec<(usize, usize)>,
) -> bool {
    match (w, r) {
        (Ty::Union(branches), _) => branches
            .iter()
            .any(|b| resolvable(writer, b, reader, r, seen)),
        (_, Ty::Union(branches)) => match best_branch(writer, w, reader, branches) {
            Some(j) => resolvable(writer, w, reader, &branches[j], seen),
            None => false,
        },
        (Ty::Array(wi), Ty::Array(ri)) | (Ty::Map(wi), Ty::Map(ri)) => {
            resolvable(writer, wi, reader, ri, seen)
        }
        (Ty::Named(wi), Ty::Named(ri)) => {
            if seen.contains(&(*wi, *ri)) {
                return true;
            }
            seen.push((*wi, *ri));
            match (&writer.named[*wi], &reader.named[*ri]) {
                (Named::Record { fields: wf, .. }, Named::Record { fields: rf, .. }) => {
                    rf.iter().all(|f| {
                        match wf
                            .iter()
                            .find(|w| w.name == f.name || f.aliases.contains(&w.name))
                        {
                            Some(wfield) => resolvable(writer, &wfield.ty, reader, &f.ty, seen),
                            None => f.default.is_some(),
                        }
                    })
                }
                (Named::Enum { name: a, .. }, Named::Enum { name: b, .. }) => a == b,
                (
                    Named::Fixed {
                        name: a, size: x, ..
                    },
                    Named::Fixed {
                        name: b, size: y, ..
                    },
                ) => a == b && x == y,
                _ => false,
            }
        }
        _ => {
            matches!(
                (kind(writer, w), kind(reader, r)),
                (a, b) if a == b
            ) || matches!(
                (kind(writer, w), kind(reader, r)),
                (Kind::Int, Kind::Long | Kind::Float | Kind::Double)
                    | (Kind::Long, Kind::Float | Kind::Double)
                    | (Kind::Float, Kind::Double)
                    | (Kind::String, Kind::Bytes)
                    | (Kind::Bytes, Kind::String)
            )
        }
    }
}

/// The value of a field's default, read as the field's type; a union's default is of its
/// first branch.
///
/// Behavior: org.apache.avro.io.parsing.ResolvingGrammarGenerator#encode
fn default_value(schema: &Schema, ty: &Ty, node: &Node) -> Result<Value, Failure> {
    let bad = || {
        format!(
            "{TYPE}: Invalid default {} for {}",
            json::compact(node),
            schema_json(schema, ty)
        )
    };
    Ok(match ty {
        Ty::Null => match node {
            Node::Null => Value::Null,
            _ => return Err(bad()),
        },
        Ty::Boolean => match node {
            Node::Bool(b) => Value::Boolean(*b),
            _ => return Err(bad()),
        },
        Ty::Int => Value::Int(node.exact_long().ok_or_else(bad)? as i32),
        Ty::Long(_) => Value::Long(node.exact_long().ok_or_else(bad)?),
        Ty::Float => Value::Float(
            node.as_double()
                .filter(|_| node.is_number())
                .ok_or_else(bad)? as f32,
        ),
        Ty::Double => Value::Double(
            node.as_double()
                .filter(|_| node.is_number())
                .ok_or_else(bad)?,
        ),
        // Bytes in a default are the characters' low eight bits.
        Ty::Bytes(_) => Value::Bytes(
            node.as_str()
                .ok_or_else(bad)?
                .chars()
                .map(|c| c as u32 as u8)
                .collect(),
        ),
        Ty::String => Value::String(node.as_str().ok_or_else(bad)?.to_owned()),
        Ty::Array(inner) => match node {
            Node::Array(items) => Value::Array(
                items
                    .iter()
                    .map(|i| default_value(schema, inner, i))
                    .collect::<Result<_, _>>()?,
            ),
            _ => return Err(bad()),
        },
        Ty::Map(inner) => match node {
            Node::Object(fields) => Value::Map(
                fields
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), default_value(schema, inner, v)?)))
                    .collect::<Result<_, Failure>>()?,
            ),
            _ => return Err(bad()),
        },
        Ty::Union(branches) => match branches.first() {
            Some(first) => default_value(schema, first, node)?,
            None => return Err(bad()),
        },
        Ty::Named(i) => match &schema.named[*i] {
            Named::Enum { .. } => Value::Enum(node.as_str().ok_or_else(bad)?.to_owned()),
            Named::Fixed { .. } => Value::Fixed(
                node.as_str()
                    .ok_or_else(bad)?
                    .chars()
                    .map(|c| c as u32 as u8)
                    .collect(),
            ),
            Named::Record { fields, .. } => {
                let mut out = Vec::new();
                for field in fields {
                    let value = match (node.get(&field.name), &field.default) {
                        (Some(v), _) | (None, Some(v)) => default_value(schema, &field.ty, v)?,
                        (None, None) => return Err(bad()),
                    };
                    out.push(value);
                }
                Value::Record(out)
            }
        },
    })
}

/// The type a field of a union with null stands for, as Deephaven sees it: the other
/// branch of a union of two of which one is `null`; any other type as it is.
///
/// Behavior: io.deephaven.kafka.KafkaSchemaUtils#getEffectiveSchema
pub(crate) fn effective<'a>(ty: &'a Ty, field: &str) -> Result<&'a Ty, Failure> {
    match ty {
        Ty::Union(branches) if branches.is_empty() => Err(format!(
            "java.lang.IllegalArgumentException: empty union {field}"
        )),
        Ty::Union(branches) if branches.len() == 2 => Ok(match (&branches[0], &branches[1]) {
            (other, Ty::Null) => other,
            (Ty::Null, other) => other,
            _ => ty,
        }),
        other => Ok(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(fields: &str) -> String {
        format!(r#"{{"type": "record", "name": "R", "fields": [{fields}]}}"#)
    }

    fn zigzag(value: i64) -> Vec<u8> {
        let mut raw = ((value << 1) ^ (value >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            let b = (raw & 0x7f) as u8;
            raw >>= 7;
            if raw == 0 {
                out.push(b);
                return out;
            }
            out.push(b | 0x80);
        }
    }

    fn read_as(writer: &str, reader: &str, bytes: &[u8]) -> Result<Value, Failure> {
        let (w, r) = (parse(writer).unwrap(), parse(reader).unwrap());
        read(&mut Decoder::new(bytes), &w, &w.root, &r, &r.root)
    }

    /// The reference's strings for these bytes (probes utf_*).
    #[test]
    fn malformed_utf8_is_replaced_as_java_replaces_it() {
        const R: char = '\u{FFFD}';
        for (bytes, java) in [
            (&b"\xed\xa0\x80"[..], format!("{R}")),
            (b"\xed\xb0\x80", format!("{R}")),
            (b"\xed\xa0\xbd\xed\xb8\x80", format!("{R}{R}")),
            (b"\xed\xa0x", format!("{R}x")),
            (b"\xed\xa0", format!("{R}")),
            (b"a\xed\xa0\x80b", format!("a{R}b")),
            (b"\xc0\xaf", format!("{R}{R}")),
            (b"\xc1\xbf", format!("{R}{R}")),
            (b"\xe0\x80\x80", format!("{R}{R}{R}")),
            (b"\xe0\xa0", format!("{R}")),
            (b"\xe0\x9f", format!("{R}{R}")),
            (b"a\xe2\x82", format!("a{R}")),
            (b"\xe1\x80x", format!("{R}x")),
            (b"\xf0\x80\x80\x80", format!("{R}{R}{R}{R}")),
            (b"\xf4\x90\x80\x80", format!("{R}{R}{R}{R}")),
            (b"\xf5\x80\x80\x80", format!("{R}{R}{R}{R}")),
            (b"\xf0\x9f\x98", format!("{R}")),
            (b"\xf1\x80\x80x", format!("{R}x")),
            (b"\xf8\x88\x80\x80\x80", format!("{R}{R}{R}{R}{R}")),
            (b"\x80a", format!("{R}a")),
            (b"\xc2", format!("{R}")),
            (b"\xf0\x9f\x98\x80", "\u{1F600}".to_owned()),
        ] {
            assert_eq!(java_utf8(bytes), java, "{bytes:x?}");
        }
    }

    /// The texts are those the reference raised for the same schemas (probes s_*).
    #[test]
    fn schemas_java_refuses_are_refused_with_its_words() {
        for (text, error) in [
            (r#"{"type": "record", "fields": []}"#, "org.apache.avro.SchemaParseException: No name in schema: {\"type\":\"record\",\"fields\":[]}"),
            (r#"{"type": "record", "name": "R"}"#, "org.apache.avro.SchemaParseException: Record has no fields: {\"type\":\"record\",\"name\":\"R\"}"),
            (&record(r#"{"name": "a", "type": "Nope"}"#), "org.apache.avro.AvroTypeException: Undefined schema: Nope"),
            (&record(r#"{"name": "a", "type": "int"}, {"name": "a", "type": "long"}"#), "org.apache.avro.AvroRuntimeException: Duplicate field a in record R: a type:LONG pos:1 and a type:INT pos:0."),
            (&record(r#"{"name": "a", "type": "int", "default": "x"}"#), "org.apache.avro.AvroTypeException: Invalid default for field a: \"x\" not a \"int\""),
            (&record(r#"{"name": "a-b", "type": "int"}"#), "org.apache.avro.SchemaParseException: Illegal character in: a-b"),
            (&format!("{} xx", record(r#"{"name": "a", "type": "int"}"#)), "org.apache.avro.SchemaParseException: dangling content after end of schema: xx"),
            (&record(r#"{"name": "a", "type": ["int", "int"]}"#), "org.apache.avro.AvroRuntimeException: Duplicate in union:int"),
            (&record(r#"{"name": "a", "type": ["null", ["int"]]}"#), "org.apache.avro.AvroRuntimeException: Nested union: [\"null\",[\"int\"]]"),
            (&record(r#"{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["1A"]}}"#), "org.apache.avro.SchemaParseException: Illegal initial character: 1A"),
            (&record(r#"{"name": "f", "type": {"type": "fixed", "name": "F"}}"#), "org.apache.avro.SchemaParseException: Invalid or no size: {\"type\":\"fixed\",\"name\":\"F\"}"),
            (&record(r#"{"name": "a", "type": {"type": "record", "name": "R", "fields": []}}"#), "org.apache.avro.SchemaParseException: Can't redefine: R"),
            (&record(r#"{"name": "a"}"#), "org.apache.avro.SchemaParseException: No field type: {\"name\":\"a\"}"),
            (&record(r#"{"name": "a", "type": {"type": {"type": "int"}}}"#), "org.apache.avro.SchemaParseException: No type: {\"type\":{\"type\":\"int\"}}"),
            (r#"{"type": {"type": "record", "name": "R", "fields": []}}"#, "org.apache.avro.SchemaParseException: No type: {\"type\":{\"type\":\"record\",\"name\":\"R\",\"fields\":[]}}"),
        ] {
            assert_eq!(parse(text).unwrap_err(), error, "{text}");
        }
        assert!(parse("{")
            .unwrap_err()
            .starts_with("org.apache.avro.SchemaParseException: "));
    }

    /// Accepted by the reference too (probes s_empty_fields, s_spaces, s_null_default_bad,
    /// s_logical_unknown, s_empty_union).
    #[test]
    fn schemas_java_accepts_are_accepted() {
        for text in [
            record(""),
            format!("   {}", record(r#"{"name": "a", "type": "int"}"#)),
            record(r#"{"name": "a", "type": ["int", "null"], "default": null}"#),
            record(r#"{"name": "a", "type": {"type": "long", "logicalType": "foo"}}"#),
            record(r#"{"name": "a", "type": []}"#),
            record(
                r#"{"name": "a", "type": "S"}, {"name": "b", "type": {"type": "record", "name": "S", "fields": []}}"#,
            ),
        ] {
            assert!(parse(&text).is_ok(), "{text}");
        }
    }

    #[test]
    fn logical_types_are_kept_only_where_java_keeps_them() {
        let schema = parse(&record(
            r#"{"name": "a", "type": {"type": "long", "logicalType": "timestamp-micros"}},
               {"name": "b", "type": {"type": "int", "logicalType": "timestamp-millis"}},
               {"name": "c", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}},
               {"name": "d", "type": {"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 3}},
               {"name": "e", "type": {"type": "fixed", "name": "F", "size": 2, "logicalType": "decimal", "precision": 5}},
               {"name": "f", "type": {"type": "fixed", "name": "G", "size": 2, "logicalType": "decimal", "precision": 4}}"#,
        ))
        .unwrap();
        let fields = schema.record_fields(&schema.root).unwrap();
        assert_eq!(fields[0].ty, Ty::Long(Some(Stamp::Micros)));
        assert_eq!(fields[1].ty, Ty::Int);
        assert_eq!(fields[2].ty, Ty::Bytes(Some(2)));
        assert_eq!(fields[3].ty, Ty::Bytes(None));
        // Two bytes hold four digits at most.
        let Ty::Named(e) = fields[4].ty else { panic!() };
        assert!(matches!(
            schema.named[e],
            Named::Fixed { decimal: None, .. }
        ));
        let Ty::Named(f) = fields[5].ty else { panic!() };
        assert!(matches!(
            schema.named[f],
            Named::Fixed {
                decimal: Some(0),
                ..
            }
        ));
    }

    /// Probes reg_v1 and ev_json_v2: a newer writer, an older reader, and the reverse.
    #[test]
    fn records_resolve_as_java_resolves_them() {
        let v1 = record(r#"{"name": "a", "type": "int"}, {"name": "s", "type": "string"}"#);
        let v2 = record(
            r#"{"name": "a", "type": "long"}, {"name": "s", "type": "string"},
               {"name": "n", "type": ["null", "double"], "default": null},
               {"name": "k", "type": "string", "default": "dflt"}"#,
        );
        let mut bytes = zigzag(3);
        bytes.extend(zigzag(3));
        bytes.extend(b"one");
        assert_eq!(
            read_as(&v1, &v2, &bytes).unwrap(),
            Value::Record(vec![
                Value::Long(3),
                Value::String("one".into()),
                Value::Null,
                Value::String("dflt".into())
            ])
        );
        let mut bytes = zigzag(2);
        bytes.extend(zigzag(3));
        bytes.extend(b"two");
        bytes.extend([2]);
        bytes.extend(2.5f64.to_le_bytes());
        bytes.extend(zigzag(2));
        bytes.extend(b"kk");
        assert_eq!(
            read_as(&v2, &v1, &bytes).unwrap_err(),
            "org.apache.avro.AvroTypeException: Field \"a\" content mismatch: Found long, expecting int"
        );
        let only_a = record(r#"{"name": "a", "type": "string"}"#);
        assert_eq!(
            read_as(&only_a, &v1, &[2, b'x']).unwrap_err(),
            "org.apache.avro.AvroTypeException: Found R, expecting R, missing required field s"
        );
    }

    #[test]
    fn promotions_unions_and_enums_follow_java() {
        let w = record(
            r#"{"name": "i", "type": "int"}, {"name": "l", "type": "long"},
               {"name": "u", "type": ["null", "string"]},
               {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "B"]}}"#,
        );
        let r = record(
            r#"{"name": "i", "type": "double"}, {"name": "l", "type": "float"},
               {"name": "u", "type": "bytes"},
               {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "C"], "default": "C"}}"#,
        );
        let mut bytes = zigzag(3);
        bytes.extend(zigzag(1 << 40));
        bytes.extend([2, 2, b'h']);
        bytes.extend(zigzag(1));
        assert_eq!(
            read_as(&w, &r, &bytes).unwrap(),
            Value::Record(vec![
                Value::Double(3.0),
                Value::Float((1i64 << 40) as f32),
                Value::Bytes(b"h".to_vec()),
                Value::Enum("C".into())
            ])
        );
        // The union's null branch has no match in the reader's bytes.
        let mut null_branch = zigzag(3);
        null_branch.extend(zigzag(1));
        null_branch.extend([0]);
        null_branch.extend(zigzag(0));
        assert_eq!(
            read_as(&w, &r, &null_branch).unwrap_err(),
            "org.apache.avro.AvroTypeException: Field \"u\" content mismatch: Found null, expecting bytes"
        );
    }

    #[test]
    fn malformed_data_fails() {
        let s = record(r#"{"name": "s", "type": "string"}"#);
        assert_eq!(
            read_as(&s, &s, &[1]).unwrap_err(),
            "org.apache.avro.AvroRuntimeException: Malformed data. Length is negative: -1"
        );
        assert_eq!(
            read_as(&s, &s, &[6, b'a']).unwrap_err(),
            "java.io.EOFException"
        );
        assert_eq!(
            read_as(&s, &s, &[0xff; 11]).unwrap_err(),
            "org.apache.avro.AvroRuntimeException: Invalid long encoding"
        );
        // Bytes that are not UTF-8 become U+FFFD, as `Utf8#toString` makes them.
        assert_eq!(
            read_as(&s, &s, &[6, b'a', 0xff, b'b']).unwrap(),
            Value::Record(vec![Value::String("a\u{fffd}b".into())])
        );
    }
}
