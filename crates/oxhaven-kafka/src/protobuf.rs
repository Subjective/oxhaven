//! Protobuf for consumed records: a `.proto` schema from the registry read into
//! descriptors, the message a spec names, and the binary encoding read as protobuf-java's
//! `DynamicMessage` reads it, with its texts for what it refuses.

use std::collections::BTreeMap;

use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor, Syntax};
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

/// The name the schema's own text is compiled under. Its imports are the names of the
/// schema's references, and the well-known types under `google/protobuf/`.
const ROOT: &str = "oxhaven-registry-schema.proto";

const INVALID: &str = "com.google.protobuf.InvalidProtocolBufferException";
const TRUNCATED: &str = "While parsing a protocol message, the input ended unexpectedly in the \
                         middle of a field.  This could mean either that the input has been \
                         truncated or that an embedded message misreported its own length.";
const MALFORMED_VARINT: &str = "CodedInputStream encountered a malformed varint.";
const NEGATIVE_SIZE: &str =
    "CodedInputStream encountered an embedded string or message which claimed to have negative \
     size.";
/// protobuf-java's `CodedInputStream#DEFAULT_RECURSION_LIMIT`.
const RECURSION_LIMIT: usize = 100;

/// A `.proto` schema and the files it imports: the schema's text and its references, by the
/// name each is imported as.
struct Sources {
    files: Vec<(String, String)>,
}

impl FileResolver for Sources {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.files.iter().find(|(known, _)| known == name) {
            Some((_, text)) => File::from_source(name, text),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

/// The descriptors of a registry's `.proto` schema `text`, whose imports are `references`
/// (each the name it is imported as, and its text) or the well-known types protobuf-java
/// bundles. The error is the text of what Java's Confluent client throws for a schema it
/// cannot parse; the parser's own words are protox's.
///
/// Behavior: io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider#parseSchemaOrElseThrow
pub(crate) fn compile(
    text: &str,
    references: &[(String, String)],
) -> Result<DescriptorPool, String> {
    let mut files = vec![(ROOT.to_owned(), text.to_owned())];
    files.extend(references.iter().cloned());
    let mut resolver = ChainFileResolver::new();
    resolver.add(Sources { files });
    resolver.add(GoogleFileResolver::new());
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_imports(true);
    compiler.open_file(ROOT).map_err(|error| {
        format!(
            "com.google.common.util.concurrent.UncheckedExecutionException: \
             java.lang.IllegalArgumentException: Invalid schema of type PROTOBUF, details: Could \
             not parse Protobuf - {error}"
        )
    })?;
    Ok(compiler.descriptor_pool())
}

/// The message of the schema that a spec reads: the one named, by its full name or by its
/// name without the package (`Outer.Inner`), among the schema's messages, nested ones and
/// those of the files it imports; without a name, the schema's first top-level message.
/// `None` where Java's lookup finds none, which makes its `NullPointerException`. A name
/// without its package that two files share finds none.
///
/// Behavior: io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema#toDescriptor,
/// io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema#getMessageDescriptor
/// (probes nm_*, nomsgs, ref_shared)
pub(crate) fn message(pool: &DescriptorPool, name: Option<&str>) -> Option<MessageDescriptor> {
    let Some(name) = name else {
        let root = pool.get_file_by_name(ROOT)?;
        return root.messages().next();
    };
    let short = |message: &MessageDescriptor| {
        let package = message.package_name();
        let full = message.full_name();
        if package.is_empty() {
            full.to_owned()
        } else {
            full[package.len() + 1..].to_owned()
        }
    };
    let by_short: Vec<MessageDescriptor> = pool
        .all_messages()
        .filter(|message| !message.is_map_entry() && short(message) == name)
        .collect();
    if by_short.len() == 1 {
        return by_short.into_iter().next();
    }
    if by_short.len() > 1 {
        // Java drops a short name that is not unique, then tries the full names.
        return pool
            .all_messages()
            .find(|message| message.full_name() == name);
    }
    pool.all_messages()
        .find(|message| !message.is_map_entry() && message.full_name() == name)
}

/// A field's value as protobuf-java holds it: the Java `int`, `long`, `float`, `double`,
/// `boolean`, `String` or bytes, an enum's number, or a message.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Value {
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Enum(i32),
    Message(Message),
}

/// A decoded message: the fields that are set, by number. A repeated field holds its
/// elements in the order they came.
#[derive(Clone, PartialEq, Debug, Default)]
pub(crate) struct Message {
    fields: BTreeMap<u32, Slot>,
}

#[derive(Clone, PartialEq, Debug)]
enum Slot {
    One(Value),
    Many(Vec<Value>),
}

impl Message {
    /// The value of a singular field that is set.
    pub(crate) fn get(&self, field: &FieldDescriptor) -> Option<&Value> {
        match self.fields.get(&field.number()) {
            Some(Slot::One(value)) => Some(value),
            _ => None,
        }
    }

    /// The elements of a repeated field.
    pub(crate) fn list(&self, field: &FieldDescriptor) -> &[Value] {
        match self.fields.get(&field.number()) {
            Some(Slot::Many(values)) => values,
            _ => &[],
        }
    }

    /// The message of a singular message field, if set.
    pub(crate) fn message(&self, field: &FieldDescriptor) -> Option<&Message> {
        match self.get(field) {
            Some(Value::Message(message)) => Some(message),
            _ => None,
        }
    }
}

/// What a field holds when it is not set: its default.
///
/// Behavior: com.google.protobuf.DynamicMessage#getField
pub(crate) fn default_value(field: &FieldDescriptor) -> Value {
    match field.default_value() {
        prost_reflect::Value::Bool(b) => Value::Bool(b),
        prost_reflect::Value::I32(i) => Value::Int(i),
        prost_reflect::Value::U32(u) => Value::Int(u as i32),
        prost_reflect::Value::I64(l) => Value::Long(l),
        prost_reflect::Value::U64(u) => Value::Long(u as i64),
        prost_reflect::Value::F32(f) => Value::Float(f),
        prost_reflect::Value::F64(d) => Value::Double(d),
        prost_reflect::Value::String(s) => Value::String(s),
        prost_reflect::Value::Bytes(b) => Value::Bytes(b.to_vec()),
        prost_reflect::Value::EnumNumber(n) => Value::Enum(n),
        _ => Value::Message(Message::default()),
    }
}

fn invalid(text: &str) -> String {
    format!("{INVALID}: {text}")
}

/// The bytes of one message, read as `CodedInputStream` reads them.
struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    /// Where the current length-delimited value ends.
    limit: usize,
    depth: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, String> {
        if self.at >= self.limit {
            return Err(invalid(TRUNCATED));
        }
        self.at += 1;
        Ok(self.bytes[self.at - 1])
    }

    /// Behavior: com.google.protobuf.CodedInputStream#readRawVarint64
    fn varint64(&mut self) -> Result<u64, String> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            value |= u64::from(b & 0x7F) << shift;
            if b & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(invalid(MALFORMED_VARINT))
    }

    /// Behavior: com.google.protobuf.CodedInputStream#readRawVarint32: the low 32 bits of
    /// a varint of up to ten bytes
    fn varint32(&mut self) -> Result<i32, String> {
        self.varint64().map(|v| v as i32)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], String> {
        if self.limit - self.at < N {
            return Err(invalid(TRUNCATED));
        }
        let mut out = [0u8; N];
        out.copy_from_slice(&self.bytes[self.at..self.at + N]);
        self.at += N;
        Ok(out)
    }

    /// The end of a length-delimited value that starts here.
    ///
    /// Behavior: com.google.protobuf.CodedInputStream#pushLimit
    fn length(&mut self) -> Result<usize, String> {
        let size = self.varint32()?;
        if size < 0 {
            return Err(invalid(NEGATIVE_SIZE));
        }
        let end = self.at + size as usize;
        if end > self.limit {
            return Err(invalid(TRUNCATED));
        }
        Ok(end)
    }

    fn bytes(&mut self) -> Result<&[u8], String> {
        let end = self.length()?;
        let bytes = &self.bytes[self.at..end];
        self.at = end;
        Ok(bytes)
    }

    fn deeper(&mut self) -> Result<(), String> {
        if self.depth >= RECURSION_LIMIT {
            return Err(invalid(
                "Protocol message had too many levels of nesting.  May be malicious.  Use \
                 setRecursionLimit() to increase the recursion depth limit.",
            ));
        }
        self.depth += 1;
        Ok(())
    }

    /// Skips a field this message does not have, or has with another wire type. Returns
    /// false at an end-group tag.
    ///
    /// Behavior: com.google.protobuf.UnknownFieldSet.Builder#mergeFieldFrom
    fn skip(&mut self, tag: u32) -> Result<bool, String> {
        match tag & 7 {
            0 => {
                self.varint64()?;
            }
            1 => {
                self.fixed::<8>()?;
            }
            2 => {
                self.bytes()?;
            }
            3 => {
                self.deeper()?;
                loop {
                    let inner = self.tag()?;
                    if inner == 0 || !self.skip(inner)? {
                        if inner != (tag & !7) | 4 {
                            return Err(invalid(END_TAG));
                        }
                        break;
                    }
                }
                self.depth -= 1;
            }
            4 => return Ok(false),
            5 => {
                self.fixed::<4>()?;
            }
            _ => {
                return Err(format!(
                    "{INVALID}$InvalidWireTypeException: Protocol message tag had invalid wire \
                     type."
                ))
            }
        }
        Ok(true)
    }

    /// The next tag, or 0 at the end of the message.
    ///
    /// Behavior: com.google.protobuf.CodedInputStream#readTag
    fn tag(&mut self) -> Result<u32, String> {
        if self.at >= self.limit {
            return Ok(0);
        }
        let tag = self.varint32()? as u32;
        if tag >> 3 == 0 {
            return Err(invalid("Protocol message contained an invalid tag (zero)."));
        }
        Ok(tag)
    }
}

const END_TAG: &str = "Protocol message end-group tag did not match expected tag.";

/// The wire type a field of `kind` is written with when it is not packed.
fn wire_type(field: &FieldDescriptor) -> u32 {
    match field.kind() {
        Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => 1,
        Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => 5,
        Kind::String | Kind::Bytes => 2,
        Kind::Message(_) if field.is_group() => 3,
        Kind::Message(_) => 2,
        _ => 0,
    }
}

/// Whether `field` takes a closed enum's value, which an unknown number does not set.
fn closed_enum(field: &FieldDescriptor) -> bool {
    matches!(field.kind(), Kind::Enum(e) if e.parent_file().syntax() == Syntax::Proto2)
}

/// One scalar of `field`'s kind.
fn scalar(reader: &mut Reader, field: &FieldDescriptor) -> Result<Value, String> {
    Ok(match field.kind() {
        Kind::Int32 | Kind::Uint32 => Value::Int(reader.varint32()?),
        Kind::Sint32 => {
            let v = reader.varint32()? as u32;
            Value::Int(((v >> 1) as i32) ^ -((v & 1) as i32))
        }
        Kind::Int64 | Kind::Uint64 => Value::Long(reader.varint64()? as i64),
        Kind::Sint64 => {
            let v = reader.varint64()?;
            Value::Long(((v >> 1) as i64) ^ -((v & 1) as i64))
        }
        Kind::Fixed32 | Kind::Sfixed32 => Value::Int(i32::from_le_bytes(reader.fixed()?)),
        Kind::Fixed64 | Kind::Sfixed64 => Value::Long(i64::from_le_bytes(reader.fixed()?)),
        Kind::Float => Value::Float(f32::from_le_bytes(reader.fixed()?)),
        Kind::Double => Value::Double(f64::from_le_bytes(reader.fixed()?)),
        Kind::Bool => Value::Bool(reader.varint64()? != 0),
        Kind::Enum(_) => Value::Enum(reader.varint32()?),
        Kind::Bytes => Value::Bytes(reader.bytes()?.to_vec()),
        Kind::String => {
            let strict = field.parent_file().syntax() == Syntax::Proto3;
            let bytes = reader.bytes()?;
            match std::str::from_utf8(bytes) {
                Ok(text) => Value::String(text.to_owned()),
                // Behavior: Utf8Validation.STRICT for proto3, LOOSE (new String) otherwise
                Err(_) if strict => return Err(invalid("Protocol message had invalid UTF-8.")),
                Err(_) => Value::String(crate::avro::java_utf8(bytes)),
            }
        }
        Kind::Message(_) => unreachable!("messages are read by `merge`"),
    })
}

/// A repeated field, a map field among them (`FieldDescriptor#is_list` is not true of a
/// map).
pub(crate) fn repeated(field: &FieldDescriptor) -> bool {
    field.cardinality() == prost_reflect::Cardinality::Repeated
}

/// Sets a singular field, clearing the other fields of its oneof.
fn set(message: &mut Message, field: &FieldDescriptor, value: Value) {
    if let Some(oneof) = field.containing_oneof() {
        for other in oneof.fields() {
            if other.number() != field.number() {
                message.fields.remove(&other.number());
            }
        }
    }
    message.fields.insert(field.number(), Slot::One(value));
}

fn push(message: &mut Message, field: &FieldDescriptor, value: Value) {
    let slot = message
        .fields
        .entry(field.number())
        .or_insert_with(|| Slot::Many(Vec::new()));
    if let Slot::Many(values) = slot {
        values.push(value);
    }
}

/// Reads a message of `descriptor` into `message` until its end, or until the end-group
/// tag of `group`.
///
/// Behavior: com.google.protobuf.DynamicMessage.Builder#mergeFrom,
/// com.google.protobuf.MessageReflection#mergeFieldFrom
fn merge(
    reader: &mut Reader,
    descriptor: &MessageDescriptor,
    message: &mut Message,
    group: Option<u32>,
) -> Result<(), String> {
    // The tag the message ended at: 0 at its end, or an end-group tag.
    let ended = loop {
        let tag = reader.tag()?;
        if tag == 0 {
            break 0;
        }
        let number = tag >> 3;
        let wire = tag & 7;
        let Some(field) = descriptor.get_field(number) else {
            if reader.skip(tag)? {
                continue;
            }
            break tag;
        };
        let packed = repeated(&field) && wire == 2 && wire_type(&field) != 2;
        if wire != wire_type(&field) && !packed {
            if reader.skip(tag)? {
                continue;
            }
            break tag;
        }
        if packed {
            let end = reader.length()?;
            let outer = reader.limit;
            reader.limit = end;
            while reader.at < end {
                let value = scalar(reader, &field)?;
                push(message, &field, value);
            }
            reader.limit = outer;
            continue;
        }
        if let Kind::Message(inner) = field.kind() {
            reader.deeper()?;
            let mut sub = match message.get(&field) {
                Some(Value::Message(existing)) if !repeated(&field) => existing.clone(),
                _ => Message::default(),
            };
            if field.is_group() {
                merge(reader, &inner, &mut sub, Some(number))?;
            } else {
                let end = reader.length()?;
                let outer = reader.limit;
                reader.limit = end;
                merge(reader, &inner, &mut sub, None)?;
                reader.limit = outer;
            }
            reader.depth -= 1;
            if inner.is_map_entry() {
                fill_map_entry(&inner, &mut sub);
            }
            if repeated(&field) {
                push(message, &field, Value::Message(sub));
            } else {
                set(message, &field, Value::Message(sub));
            }
            continue;
        }
        let value = scalar(reader, &field)?;
        if let (Value::Enum(n), Kind::Enum(e)) = (&value, field.kind()) {
            if closed_enum(&field) && e.get_value(*n).is_none() {
                continue;
            }
        }
        if repeated(&field) {
            push(message, &field, value);
        } else {
            set(message, &field, value);
        }
    };
    // Behavior: CodedInputStream#checkLastTagWas
    let expected = group.map_or(0, |number| number << 3 | 4);
    if ended != expected {
        return Err(invalid(END_TAG));
    }
    Ok(())
}

/// A map entry has its key and its value, their defaults where the record left them out.
///
/// Behavior: com.google.protobuf.MapEntry, as `DynamicMessage` builds a map field's
/// entries (probes mapmsg, map2, pm_map)
fn fill_map_entry(entry: &MessageDescriptor, message: &mut Message) {
    for field in [entry.map_entry_key_field(), entry.map_entry_value_field()] {
        if message.get(&field).is_none() {
            message
                .fields
                .insert(field.number(), Slot::One(default_value(&field)));
        }
    }
}

/// Reads one message of `descriptor` from all of `bytes`, and checks that its required
/// fields are set, as `DynamicMessage.Builder#build` does.
///
/// Behavior: com.google.protobuf.DynamicMessage.Builder#mergeFrom(byte[], int, int) and #build
pub(crate) fn decode(descriptor: &MessageDescriptor, bytes: &[u8]) -> Result<Message, String> {
    let mut reader = Reader {
        bytes,
        at: 0,
        limit: bytes.len(),
        depth: 0,
    };
    let mut message = Message::default();
    merge(&mut reader, descriptor, &mut message, None)?;
    let mut missing = Vec::new();
    missing_fields(descriptor, &message, "", &mut missing);
    if !missing.is_empty() {
        return Err(format!(
            "com.google.protobuf.UninitializedMessageException: Message missing required fields: \
             {}",
            missing.join(", ")
        ));
    }
    Ok(message)
}

/// Behavior: com.google.protobuf.MessageReflection#findMissingFields
fn missing_fields(
    descriptor: &MessageDescriptor,
    message: &Message,
    prefix: &str,
    out: &mut Vec<String>,
) {
    for field in descriptor.fields() {
        let required = field.cardinality() == prost_reflect::Cardinality::Required;
        if required && message.get(&field).is_none() {
            out.push(format!("{prefix}{}", field.name()));
        }
    }
    for (number, slot) in &message.fields {
        let Some(field) = descriptor.get_field(*number) else {
            continue;
        };
        let Kind::Message(inner) = field.kind() else {
            continue;
        };
        match slot {
            Slot::Many(values) => {
                for (i, value) in values.iter().enumerate() {
                    if let Value::Message(sub) = value {
                        let prefix = format!("{prefix}{}[{i}].", field.name());
                        missing_fields(&inner, sub, &prefix, out);
                    }
                }
            }
            Slot::One(Value::Message(sub)) => {
                let prefix = format!("{prefix}{}.", field.name());
                missing_fields(&inner, sub, &prefix, out);
            }
            Slot::One(_) => {}
        }
    }
}

/// Where the message starts in a record of Confluent's wire format: after the magic byte,
/// the schema id and the message indexes, which are read and not used. The error is the
/// text of Java's exception.
///
/// Behavior: io.deephaven.kafka.ProtobufDeserializers.Serdes#deserialize and #protobufOffset,
/// org.apache.kafka.common.utils.ByteUtils#readVarint
pub(crate) fn serdes_offset(bytes: &[u8]) -> Result<usize, String> {
    const SERIALIZATION: &str = "org.apache.kafka.common.errors.SerializationException";
    if bytes.len() < 6 {
        return Err(format!(
            "{SERIALIZATION}: Not serdes protocol, expected at least 6 bytes"
        ));
    }
    if bytes[0] != 0 {
        return Err(format!(
            "{SERIALIZATION}: Not serdes protocol, magic byte is incorrect"
        ));
    }
    let mut at = 5;
    let count = kafka_varint(bytes, &mut at)?;
    for _ in 0..count.max(0) {
        kafka_varint(bytes, &mut at)?;
    }
    Ok(at)
}

/// Behavior: org.apache.kafka.common.utils.ByteUtils#readVarint and #readUnsignedVarint:
/// at most five bytes, the fifth shifted into the top of an `int`, then zigzag
fn kafka_varint(bytes: &[u8], at: &mut usize) -> Result<i32, String> {
    let mut next = || -> Result<i8, String> {
        let b = *bytes
            .get(*at)
            .ok_or_else(|| "java.nio.BufferUnderflowException".to_owned())?;
        *at += 1;
        Ok(b as i8)
    };
    let mut result: i32 = 0;
    for shift in [0, 7, 14, 21] {
        let b = next()?;
        if b >= 0 {
            result |= i32::from(b) << shift;
            return Ok(((result as u32) >> 1) as i32 ^ -(result & 1));
        }
        result |= i32::from(b & 0x7F) << shift;
    }
    let b = next()?;
    result |= i32::from(b).wrapping_shl(28);
    if b < 0 {
        return Err(format!(
            "java.lang.IllegalArgumentException: Varint is too long, the most significant bit in \
             the 5th byte is set, converted value: {:x}",
            result as u32
        ));
    }
    Ok(((result as u32) >> 1) as i32 ^ -(result & 1))
}

#[cfg(test)]
mod tests {
    use super::{compile, decode, message, serdes_offset, Message, Value};

    const SCHEMA: &str = r#"syntax = "proto3";
package w;
message Leaf { int32 i = 1; string s = 2; }
message Node { Leaf leaf = 1; repeated int32 xs = 2; Node next = 3; }
"#;

    const PROTO2: &str = r#"syntax = "proto2";
package r;
message Outer { optional In in = 1; repeated In rin = 2; map<string, In> m = 3; optional string s = 4; }
message In { required int32 r = 1; optional In deeper = 2; }
"#;

    fn read(schema: &str, name: &str, bytes: &[u8]) -> Result<Message, String> {
        let pool = compile(schema, &[]).unwrap();
        decode(&message(&pool, Some(name)).unwrap(), bytes)
    }

    fn invalid(text: &str) -> Result<Message, String> {
        Err(format!(
            "com.google.protobuf.InvalidProtocolBufferException: {text}"
        ))
    }

    const TRUNCATED: &str = "While parsing a protocol message, the input ended unexpectedly \
                             in the middle of a field.  This could mean either that the input \
                             has been truncated or that an embedded message misreported its \
                             own length.";

    /// Behavior: com.google.protobuf.CodedInputStream and DynamicMessage.Builder#mergeFrom
    /// (probes w_tag0, w_wt6, w_trunc, w_trunc_ld, w_badvarint, w_negsize, w_endgroup,
    /// w_badgroup, w_utf8)
    #[test]
    fn malformed_messages_fail_with_protobuf_javas_texts() {
        let node = |bytes: &[u8]| read(SCHEMA, "Node", bytes);
        assert_eq!(
            node(&[0]),
            invalid("Protocol message contained an invalid tag (zero).")
        );
        assert_eq!(
            node(&[0x7e]),
            Err(
                "com.google.protobuf.InvalidProtocolBufferException$InvalidWireTypeException: \
                 Protocol message tag had invalid wire type."
                    .to_owned()
            )
        );
        assert_eq!(node(&[0x10]), invalid(TRUNCATED));
        assert_eq!(node(&[0x0a, 5, 8]), invalid(TRUNCATED));
        assert_eq!(
            node(&[0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
            invalid("CodedInputStream encountered a malformed varint.")
        );
        assert_eq!(
            node(&[0x0a, 0xff, 0xff, 0xff, 0xff, 0x0f]),
            invalid(
                "CodedInputStream encountered an embedded string or message which claimed to \
                 have negative size."
            )
        );
        // An end-group tag where none is open, and a group that ends with another's tag.
        let mismatch = invalid("Protocol message end-group tag did not match expected tag.");
        assert_eq!(node(&[0x0c]), mismatch);
        assert_eq!(node(&[(9 << 3) | 3, (10 << 3) | 4]), mismatch);
        // A proto3 string must be UTF-8.
        assert_eq!(
            node(&[0x0a, 4, 0x12, 2, 0xc3, 0x28]),
            invalid("Protocol message had invalid UTF-8.")
        );
    }

    /// Behavior: CodedInputStream#DEFAULT_RECURSION_LIMIT (probes nest100, nest101)
    #[test]
    fn a_hundred_nested_messages_are_read_and_a_hundred_and_one_are_not() {
        let nested = |depth: usize| {
            let mut bytes = vec![0x10, 1];
            for _ in 0..depth {
                let size = bytes.len();
                let mut outer = vec![0x1a];
                if size >= 0x80 {
                    outer.extend([(size as u8) | 0x80, (size >> 7) as u8]);
                } else {
                    outer.push(size as u8);
                }
                outer.extend(bytes);
                bytes = outer;
            }
            read(SCHEMA, "Node", &bytes)
        };
        assert!(nested(100).is_ok());
        assert_eq!(
            nested(101),
            invalid(
                "Protocol message had too many levels of nesting.  May be malicious.  Use \
                 setRecursionLimit() to increase the recursion depth limit."
            )
        );
    }

    /// Unknown fields, groups among them, and fields of the wrong wire type are skipped;
    /// the last value of a field wins and a message merges with the one before it.
    ///
    /// Behavior: MessageReflection#mergeFieldFrom (probes unkgroup, merge)
    #[test]
    fn unknown_fields_are_skipped_and_messages_merge() {
        let pool = compile(SCHEMA, &[]).unwrap();
        let node = message(&pool, Some("Node")).unwrap();
        let leaf = message(&pool, Some("Leaf")).unwrap();
        let bytes = [
            (14 << 3) | 3,
            (15 << 3) | 3,
            (15 << 3) | 4,
            (14 << 3) | 4,
            // leaf { i: 1 }, then leaf { s: "x" }
            0x0a,
            2,
            0x08,
            1,
            0x0a,
            3,
            0x12,
            1,
            b'x',
            // xs as a fixed32, the wrong wire type, then packed, then one more
            0x15,
            0,
            0,
            0,
            0,
            0x12,
            2,
            5,
            6,
            0x10,
            7,
        ];
        let read = decode(&node, &bytes).unwrap();
        let field = |name: &str| node.get_field_by_name(name).unwrap();
        let got = read.message(&field("leaf")).unwrap();
        let leaf_field = |name: &str| leaf.get_field_by_name(name).unwrap();
        assert_eq!(got.get(&leaf_field("i")), Some(&Value::Int(1)));
        assert_eq!(got.get(&leaf_field("s")), Some(&Value::String("x".into())));
        assert_eq!(
            read.list(&field("xs")),
            [Value::Int(5), Value::Int(6), Value::Int(7)]
        );
    }

    /// Behavior: DynamicMessage.Builder#build, MessageReflection#findMissingFields (probes
    /// nr_missing_rep, pm_map); a proto2 string that is not UTF-8 is read with
    /// replacements (probe p2_utf8)
    #[test]
    fn missing_required_fields_are_listed_and_proto2_strings_are_loose() {
        let outer = |bytes: &[u8]| read(PROTO2, "Outer", bytes);
        assert_eq!(
            outer(&[0x12, 2, 0x08, 1, 0x12, 4, 0x12, 2, 0x08, 1, 0x12, 2, 0x12, 0]),
            Err(
                "com.google.protobuf.UninitializedMessageException: Message missing required \
                 fields: rin[1].r, rin[2].r, rin[2].deeper.r"
                    .to_owned()
            )
        );
        assert_eq!(
            outer(&[0x1a, 3, 0x0a, 1, b'k']),
            Err(
                "com.google.protobuf.UninitializedMessageException: Message missing required \
                 fields: m[0].value.r"
                    .to_owned()
            )
        );
        let pool = compile(PROTO2, &[]).unwrap();
        let descriptor = message(&pool, Some("Outer")).unwrap();
        let read = decode(&descriptor, &[0x22, 3, b'a', 0xff, b'b']).unwrap();
        let s = descriptor.get_field_by_name("s").unwrap();
        assert_eq!(read.get(&s), Some(&Value::String("a\u{fffd}b".into())));
    }

    /// Behavior: io.deephaven.kafka.protobuf.ProtobufDeserializers.Serdes and Kafka's
    /// ByteUtils#readVarint (probes w_short, w_badmagic, w_idxbad, w_idxcut, w_idx)
    #[test]
    fn the_serdes_framing_is_read_as_java_reads_it() {
        assert_eq!(serdes_offset(&[0, 0, 0, 0, 1, 0, 8]), Ok(6));
        // Two indexes, and a negative count that reads none.
        assert_eq!(serdes_offset(&[0, 0, 0, 0, 1, 4, 2, 4, 8]), Ok(8));
        assert_eq!(serdes_offset(&[0, 0, 0, 0, 1, 1, 8]), Ok(6));
        assert_eq!(
            serdes_offset(&[0, 0, 0, 0, 1]),
            Err(
                "org.apache.kafka.common.errors.SerializationException: Not serdes protocol, \
                 expected at least 6 bytes"
                    .to_owned()
            )
        );
        assert_eq!(
            serdes_offset(&[1, 0, 0, 0, 1, 0]),
            Err(
                "org.apache.kafka.common.errors.SerializationException: Not serdes protocol, \
                 magic byte is incorrect"
                    .to_owned()
            )
        );
        assert_eq!(
            serdes_offset(&[0, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff, 0xff]),
            Err(
                "java.lang.IllegalArgumentException: Varint is too long, the most significant \
                 bit in the 5th byte is set, converted value: ffffffff"
                    .to_owned()
            )
        );
        assert_eq!(
            serdes_offset(&[0, 0, 0, 0, 1, 4, 2]),
            Err("java.nio.BufferUnderflowException".to_owned())
        );
    }

    /// Behavior: ProtobufSchema#toDescriptor (probes nm_*, nomsgs)
    #[test]
    fn a_message_is_found_by_its_full_or_short_name() {
        let pool = compile(
            "syntax = \"proto3\";\npackage n;\n\
             message Outer { message In2 { int32 q = 1; } In2 i = 1; }\n",
            &[],
        )
        .unwrap();
        let name = |name: Option<&str>| message(&pool, name).map(|m| m.full_name().to_owned());
        assert_eq!(name(None).as_deref(), Some("n.Outer"));
        assert_eq!(name(Some("Outer")).as_deref(), Some("n.Outer"));
        assert_eq!(name(Some("n.Outer.In2")).as_deref(), Some("n.Outer.In2"));
        assert_eq!(name(Some("Outer.In2")).as_deref(), Some("n.Outer.In2"));
        assert_eq!(name(Some("In2")), None);
        assert_eq!(name(Some(".n.Outer")), None);
        let empty = compile("syntax = \"proto3\";\nenum X { A = 0; }\n", &[]).unwrap();
        assert!(message(&empty, None).is_none());
    }
}
