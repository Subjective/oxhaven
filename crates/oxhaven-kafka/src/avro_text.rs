//! An Avro schema as Java's `Schema#toString` writes it: the text a producer registers,
//! and the text Avro's messages show of a part of a schema.
//!
//! The schema is kept as the JSON it was read from (already checked by `avro::parse`), so
//! that what `avro::Schema` drops (documentation, aliases, custom properties, the order of
//! fields) is written back as Java writes it.

use std::collections::HashMap;

use crate::json::{self, Node};

/// A type of a schema, with what Java's `toString` writes of it.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Ty {
    /// A primitive type, with its properties (`logicalType` among them).
    Primitive {
        name: String,
        props: Vec<(String, Node)>,
    },
    Array {
        items: Box<Ty>,
        props: Vec<(String, Node)>,
    },
    Map {
        values: Box<Ty>,
        props: Vec<(String, Node)>,
    },
    Union(Vec<Ty>),
    /// A named type, by its full name: its definition is in [`Text::named`].
    Named(String),
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) ty: Ty,
    doc: Option<String>,
    default: Option<Node>,
    order: Option<String>,
    aliases: Vec<String>,
    props: Vec<(String, Node)>,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Kind {
    Record { error: bool, fields: Vec<Field> },
    Enum { symbols: Vec<String>, default: Option<String> },
    Fixed { size: Node },
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Named {
    /// The name without its namespace.
    pub(crate) name: String,
    pub(crate) space: Option<String>,
    doc: Option<String>,
    aliases: Vec<String>,
    props: Vec<(String, Node)>,
    pub(crate) kind: Kind,
}

/// A schema and its named types.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Text {
    pub(crate) root: Ty,
    pub(crate) named: HashMap<String, Named>,
}

const PRIMITIVES: [&str; 8] = [
    "null", "boolean", "int", "long", "float", "double", "bytes", "string",
];
/// Behavior: org.apache.avro.Schema#SCHEMA_RESERVED, and `default` of an enum
const RESERVED: [&str; 10] = [
    "doc",
    "fields",
    "items",
    "name",
    "namespace",
    "size",
    "symbols",
    "values",
    "type",
    "aliases",
];
/// Behavior: org.apache.avro.Schema#FIELD_RESERVED
const FIELD_RESERVED: [&str; 6] = ["default", "doc", "name", "order", "type", "aliases"];

fn text_of(node: Option<&Node>) -> Option<String> {
    node.and_then(Node::as_str).map(str::to_owned)
}

fn texts(node: Option<&Node>) -> Vec<String> {
    match node {
        Some(Node::Array(items)) => items.iter().filter_map(|i| text_of(Some(i))).collect(),
        _ => Vec::new(),
    }
}

fn props(fields: &[(String, Node)], reserved: &[&str]) -> Vec<(String, Node)> {
    fields
        .iter()
        .filter(|(name, _)| !reserved.contains(&name.as_str()))
        .cloned()
        .collect()
}

/// Splits a name as Avro's `Schema.Name` does: a dotted name carries its namespace, an
/// empty namespace is none.
fn split(name: &str, space: Option<&str>) -> (String, Option<String>) {
    match name.rfind('.') {
        Some(dot) => (name[dot + 1..].to_owned(), Some(name[..dot].to_owned())),
        None => (
            name.to_owned(),
            space.filter(|s| !s.is_empty()).map(str::to_owned),
        ),
    }
}

fn full(name: &str, space: Option<&str>) -> String {
    match space {
        Some(space) => format!("{space}.{name}"),
        None => name.to_owned(),
    }
}

impl Text {
    /// Reads the JSON of a schema that `avro::parse` accepted.
    ///
    /// Behavior: org.apache.avro.Schema#parse(JsonNode, ParseContext, String), for what
    /// `toString` writes back
    pub(crate) fn read(text: &str) -> Result<Self, String> {
        let node = json::parse(text).map_err(|error| error.to_string())?;
        let mut named = HashMap::new();
        let root = read_ty(&node, None, &mut named);
        Ok(Self { root, named })
    }

    /// Java's `Schema#toString()` of the whole schema.
    pub(crate) fn to_java(&self) -> String {
        self.ty_to_java(&self.root)
    }

    /// Java's `Schema#toString()` of `ty` as a schema of its own: a named type it uses is
    /// defined at its first use.
    pub(crate) fn ty_to_java(&self, ty: &Ty) -> String {
        let mut out = String::new();
        self.write(ty, &mut Vec::new(), None, &mut out);
        out
    }

    /// The type `ty` stands for, following a name.
    pub(crate) fn resolve<'a>(&'a self, ty: &'a Ty) -> Resolved<'a> {
        match ty {
            Ty::Named(name) => Resolved::Named(&self.named[name]),
            other => Resolved::Plain(other),
        }
    }

    /// Behavior: org.apache.avro.Schema#toJson of each type, through `JsonGenerator`
    fn write(&self, ty: &Ty, done: &mut Vec<String>, space: Option<&str>, out: &mut String) {
        match ty {
            Ty::Primitive { name, props } if props.is_empty() => json::write_quoted(name, out),
            Ty::Primitive { name, props } => {
                out.push_str("{\"type\":");
                json::write_quoted(name, out);
                write_props(props, out);
                out.push('}');
            }
            Ty::Array { items, props } => {
                out.push_str("{\"type\":\"array\",\"items\":");
                self.write(items, done, space, out);
                write_props(props, out);
                out.push('}');
            }
            Ty::Map { values, props } => {
                out.push_str("{\"type\":\"map\",\"values\":");
                self.write(values, done, space, out);
                write_props(props, out);
                out.push('}');
            }
            Ty::Union(branches) => {
                out.push('[');
                for (i, branch) in branches.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    self.write(branch, done, space, out);
                }
                out.push(']');
            }
            Ty::Named(full_name) => {
                let named = &self.named[full_name];
                if done.contains(full_name) {
                    // Behavior: Schema.Name#getQualified: the short name within its space
                    let shown = if named.space.as_deref() == space {
                        &named.name
                    } else {
                        full_name
                    };
                    json::write_quoted(shown, out);
                    return;
                }
                done.push(full_name.clone());
                self.write_named(named, done, space, out);
            }
        }
    }

    fn write_named(&self, named: &Named, done: &mut Vec<String>, space: Option<&str>, out: &mut String) {
        let word = match &named.kind {
            Kind::Record { error: true, .. } => "error",
            Kind::Record { .. } => "record",
            Kind::Enum { .. } => "enum",
            Kind::Fixed { .. } => "fixed",
        };
        out.push_str("{\"type\":\"");
        out.push_str(word);
        out.push_str("\",\"name\":");
        json::write_quoted(&named.name, out);
        // Behavior: org.apache.avro.Schema.Name#writeName
        match (&named.space, space) {
            (Some(own), outer) if Some(own.as_str()) != outer => {
                out.push_str(",\"namespace\":");
                json::write_quoted(own, out);
            }
            (None, Some(_)) => out.push_str(",\"namespace\":\"\""),
            _ => {}
        }
        if let Some(doc) = &named.doc {
            out.push_str(",\"doc\":");
            json::write_quoted(doc, out);
        }
        match &named.kind {
            Kind::Record { fields, .. } => {
                out.push_str(",\"fields\":[");
                let inner = named.space.as_deref();
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    self.write_field(field, done, inner, out);
                }
                out.push(']');
            }
            Kind::Enum { symbols, default } => {
                out.push_str(",\"symbols\":[");
                for (i, symbol) in symbols.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    json::write_quoted(symbol, out);
                }
                out.push(']');
                if let Some(default) = default {
                    out.push_str(",\"default\":");
                    json::write_quoted(default, out);
                }
            }
            Kind::Fixed { size } => {
                out.push_str(",\"size\":");
                out.push_str(&json::compact(size));
            }
        }
        write_props(&named.props, out);
        write_aliases(&named.aliases, out);
        out.push('}');
    }

    fn write_field(&self, field: &Field, done: &mut Vec<String>, space: Option<&str>, out: &mut String) {
        out.push_str("{\"name\":");
        json::write_quoted(&field.name, out);
        out.push_str(",\"type\":");
        self.write(&field.ty, done, space, out);
        if let Some(doc) = &field.doc {
            out.push_str(",\"doc\":");
            json::write_quoted(doc, out);
        }
        if let Some(default) = &field.default {
            out.push_str(",\"default\":");
            out.push_str(&json::compact(default));
        }
        if let Some(order) = &field.order {
            out.push_str(",\"order\":");
            json::write_quoted(order, out);
        }
        write_aliases(&field.aliases, out);
        write_props(&field.props, out);
        out.push('}');
    }

    /// The type as Avro's messages describe it: `int`, `List<int>`, `Map<String, int>`,
    /// or a named type's name.
    ///
    /// Behavior: org.apache.avro.path.TracingClassCastException and SchemaUtil#describe
    pub(crate) fn describe(&self, ty: &Ty) -> String {
        match ty {
            Ty::Primitive { name, .. } => name.clone(),
            Ty::Array { items, .. } => format!("List<{}>", self.describe(items)),
            Ty::Map { values, .. } => format!("Map<String, {}>", self.describe(values)),
            Ty::Union(_) => self.ty_to_java(ty),
            Ty::Named(name) => self.named[name].name.clone(),
        }
    }
}

impl Named {
    /// The `logicalType` property, as its text.
    pub(crate) fn logical_type(&self) -> Option<String> {
        logical_type_of(&self.props)
    }
}

/// The `logicalType` among `props`, as its text.
pub(crate) fn logical_type_of(props: &[(String, Node)]) -> Option<String> {
    props
        .iter()
        .find(|(name, _)| name == "logicalType")
        .map(|(_, value)| value.as_str().map_or_else(|| json::compact(value), str::to_owned))
}

/// What a type stands for.
pub(crate) enum Resolved<'a> {
    Plain(&'a Ty),
    Named(&'a Named),
}

fn write_props(props: &[(String, Node)], out: &mut String) {
    for (name, value) in props {
        out.push(',');
        json::write_quoted(name, out);
        out.push(':');
        out.push_str(&json::compact(value));
    }
}

fn write_aliases(aliases: &[String], out: &mut String) {
    if aliases.is_empty() {
        return;
    }
    out.push_str(",\"aliases\":[");
    for (i, alias) in aliases.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json::write_quoted(alias, out);
    }
    out.push(']');
}

fn read_ty(node: &Node, space: Option<&str>, named: &mut HashMap<String, Named>) -> Ty {
    match node {
        Node::Text(_) => {
            let name = node.as_str().unwrap_or_default();
            if PRIMITIVES.contains(&name) {
                return Ty::Primitive {
                    name: name.to_owned(),
                    props: Vec::new(),
                };
            }
            let (short, own) = split(name, space);
            let qualified = full(&short, own.as_deref());
            // A name that is not in the space given may be one without a namespace.
            if named.contains_key(&qualified) || name.contains('.') {
                Ty::Named(qualified)
            } else {
                Ty::Named(name.to_owned())
            }
        }
        Node::Array(branches) => Ty::Union(
            branches
                .iter()
                .map(|b| read_ty(b, space, named))
                .collect(),
        ),
        Node::Object(fields) => {
            let kind = node.get("type");
            let word = kind.and_then(Node::as_str).unwrap_or_default();
            match word {
                "record" | "error" | "enum" | "fixed" => read_named(node, fields, word, space, named),
                "array" => Ty::Array {
                    items: Box::new(read_ty(node.get("items").unwrap_or(&Node::Null), space, named)),
                    props: props(fields, &RESERVED),
                },
                "map" => Ty::Map {
                    values: Box::new(read_ty(node.get("values").unwrap_or(&Node::Null), space, named)),
                    props: props(fields, &RESERVED),
                },
                _ if PRIMITIVES.contains(&word) => Ty::Primitive {
                    name: word.to_owned(),
                    props: props(fields, &RESERVED),
                },
                // `{"type": <a type>}` is that type.
                _ => read_ty(kind.unwrap_or(&Node::Null), space, named),
            }
        }
        _ => Ty::Primitive {
            name: "null".to_owned(),
            props: Vec::new(),
        },
    }
}

fn read_named(
    node: &Node,
    fields: &[(String, Node)],
    word: &str,
    space: Option<&str>,
    named: &mut HashMap<String, Named>,
) -> Ty {
    let given = node.get("namespace").and_then(Node::as_str);
    let (name, own) = split(
        node.get("name").and_then(Node::as_str).unwrap_or_default(),
        given.or(space),
    );
    let full_name = full(&name, own.as_deref());
    let mut entry = Named {
        name,
        space: own.clone(),
        doc: text_of(node.get("doc")),
        aliases: texts(node.get("aliases")),
        props: Vec::new(),
        kind: Kind::Fixed { size: Node::Null },
    };
    match word {
        "enum" => {
            let mut reserved = RESERVED.to_vec();
            reserved.push("default");
            entry.props = props(fields, &reserved);
            entry.kind = Kind::Enum {
                symbols: texts(node.get("symbols")),
                default: text_of(node.get("default")),
            };
            named.insert(full_name.clone(), entry);
        }
        "fixed" => {
            entry.props = props(fields, &RESERVED);
            entry.kind = Kind::Fixed {
                size: node.get("size").cloned().unwrap_or(Node::Null),
            };
            named.insert(full_name.clone(), entry);
        }
        _ => {
            entry.props = props(fields, &RESERVED);
            entry.kind = Kind::Record {
                error: word == "error",
                fields: Vec::new(),
            };
            // Defined before its fields, which may refer to it.
            named.insert(full_name.clone(), entry);
            let mut out = Vec::new();
            if let Some(Node::Array(items)) = node.get("fields") {
                for item in items {
                    let Node::Object(attributes) = item else {
                        continue;
                    };
                    out.push(Field {
                        name: text_of(item.get("name")).unwrap_or_default(),
                        ty: read_ty(item.get("type").unwrap_or(&Node::Null), own.as_deref(), named),
                        doc: text_of(item.get("doc")),
                        default: item.get("default").cloned(),
                        order: text_of(item.get("order")).filter(|o| o != "ascending"),
                        aliases: texts(item.get("aliases")),
                        props: props(attributes, &FIELD_RESERVED),
                    });
                }
            }
            if let Some(Named {
                kind: Kind::Record { fields, .. },
                ..
            }) = named.get_mut(&full_name)
            {
                *fields = out;
            }
        }
    }
    Ty::Named(full_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each pair was read from the reference: `new Schema.Parser().parse(in).toString()`
    /// on Avro 1.12 (probe tostr, task 7.5.2).
    #[test]
    fn schemas_are_written_as_java_writes_them() {
        let pairs = [
            (
                r#"{"fields":[{"type":"int","name":"a","doc":"d","default":3,"order":"descending","aliases":["b"],"x":1}],"name":"R","type":"record","doc":"rd","aliases":["Q"],"z":"p","namespace":"n.s"}"#,
                r#"{"type":"record","name":"R","namespace":"n.s","doc":"rd","fields":[{"name":"a","type":"int","doc":"d","default":3,"order":"descending","aliases":["b"],"x":1}],"z":"p","aliases":["Q"]}"#,
            ),
            (
                r#"{"type":"record","name":"R","namespace":"n","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"A","doc":"ed","p":true}},{"name":"e2","type":"E"},{"name":"f","type":{"type":"fixed","name":"o.F","size":3,"logicalType":"decimal","precision":4,"scale":1}},{"name":"g","type":"o.F"}]}"#,
                r#"{"type":"record","name":"R","namespace":"n","fields":[{"name":"e","type":{"type":"enum","name":"E","doc":"ed","symbols":["A","B"],"default":"A","p":true}},{"name":"e2","type":"E"},{"name":"f","type":{"type":"fixed","name":"F","namespace":"o","size":3,"logicalType":"decimal","precision":4,"scale":1}},{"name":"g","type":"o.F"}]}"#,
            ),
            (
                r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"long","q":1}},{"name":"m","type":{"type":"map","values":["null","string"]}},{"name":"s","type":{"type":"string","avro.java.string":"String"}},{"name":"d","type":["null","double"],"default":null},{"name":"r","type":{"type":"record","name":"In","namespace":"","fields":[]}}]}"#,
                r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"long","q":1}},{"name":"m","type":{"type":"map","values":["null","string"]}},{"name":"s","type":{"type":"string","avro.java.string":"String"}},{"name":"d","type":["null","double"],"default":null},{"name":"r","type":{"type":"record","name":"In","fields":[]}}]}"#,
            ),
            (
                r#"{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":1},{"name":"s","type":"string","default":"é\n"},{"name":"l","type":"long","default":10000000000},{"name":"x","type":{"type":"int"}},{"name":"y","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}}]}"#,
                r#"{"type":"record","name":"R","fields":[{"name":"f","type":"float","default":1},{"name":"s","type":"string","default":"é\n"},{"name":"l","type":"long","default":10000000000},{"name":"x","type":"int"},{"name":"y","type":{"type":"bytes","logicalType":"decimal","precision":5,"scale":2}}]}"#,
            ),
            (
                r#"{"type":"error","name":"R","fields":[{"name":"z","type":{"type":"record","name":"R2","fields":[{"name":"q","type":["null","R2"]}]}}]}"#,
                r#"{"type":"error","name":"R","fields":[{"name":"z","type":{"type":"record","name":"R2","fields":[{"name":"q","type":["null","R2"]}]}}]}"#,
            ),
            (
                r#"{"type": "record", "name": "R", "fields": [{"name": "o", "type": {"type": "map", "values": "int"}, "default": {"b": 1, "a": 2}}, {"name": "a", "type": {"type": "array", "items": "int"}, "default": [1, 2]}, {"name": "u", "type": ["int", "null"], "default": 1}, {"name": "t", "type": "boolean", "default": true}]}"#,
                r#"{"type":"record","name":"R","fields":[{"name":"o","type":{"type":"map","values":"int"},"default":{"b":1,"a":2}},{"name":"a","type":{"type":"array","items":"int"},"default":[1,2]},{"name":"u","type":["int","null"],"default":1},{"name":"t","type":"boolean","default":true}]}"#,
            ),
        ];
        for (input, java) in pairs {
            assert_eq!(Text::read(input).unwrap().to_java(), java, "{input}");
        }
    }
}
