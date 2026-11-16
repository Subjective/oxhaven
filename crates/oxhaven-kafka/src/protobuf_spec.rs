//! The columns of a Protobuf spec, from the message's descriptor, and their cells.
//!
//! Java turns the descriptor into one function per column
//! (`io.deephaven.protobuf.ProtobufDescriptorParserImpl`), composed of the functions of the
//! fields on the way to it. A function here is the same chain written as data: the
//! message fields from the record to the value ([`Leaf`]), or to a repeated field whose
//! elements are each read so ([`Getter::Array`]). Where Java's chain skips a missing
//! message (`BypassOnNull`) the cell is null; where it does not, as for the first message
//! field inside a repeated message, Java's function is given null and may throw.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, BooleanBuilder, DurationNanosecondArray, Float32Array, Float32Builder,
    Float64Array, Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, Int8Builder,
    ListBuilder, StringArray, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType, Field};
use oxhaven_contracts::schema::ColumnType;
use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor};

use crate::protobuf::{self, Message, Value};
use crate::registry::Registry;
use crate::{not_ported, KafkaError};

/// How a record's key or value is framed.
///
/// Behavior: io.deephaven.kafka.protobuf.Protocol
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProtobufProtocol {
    /// Confluent's wire format: a magic byte, a schema id, the message indexes, then the
    /// message.
    Serdes,
    /// The message alone.
    Raw,
}

/// The well-known message types Java reads as one value.
///
/// Behavior: io.deephaven.protobuf.Builtin#parsers
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WellKnown {
    Timestamp,
    Duration,
    Bool,
    Int32,
    Int64,
    Float,
    Double,
    String,
    Bytes,
    /// `Any` and `FieldMask`, which Java makes columns of their Java classes.
    Object(&'static str),
}

impl WellKnown {
    fn of(message: &MessageDescriptor) -> Option<Self> {
        Some(match message.full_name() {
            "google.protobuf.Timestamp" => Self::Timestamp,
            "google.protobuf.Duration" => Self::Duration,
            "google.protobuf.BoolValue" => Self::Bool,
            "google.protobuf.Int32Value" | "google.protobuf.UInt32Value" => Self::Int32,
            "google.protobuf.Int64Value" | "google.protobuf.UInt64Value" => Self::Int64,
            "google.protobuf.FloatValue" => Self::Float,
            "google.protobuf.DoubleValue" => Self::Double,
            "google.protobuf.StringValue" => Self::String,
            "google.protobuf.BytesValue" => Self::Bytes,
            "google.protobuf.Any" => Self::Object("com.google.protobuf.Any"),
            "google.protobuf.FieldMask" => Self::Object("com.google.protobuf.FieldMask"),
            _ => return None,
        })
    }
}

/// What a chain of message fields ends at.
#[derive(Clone, PartialEq, Debug)]
enum End {
    /// A field of the last message.
    Field(FieldDescriptor),
    /// The last message, a well-known type.
    WellKnown(WellKnown, MessageDescriptor),
    /// The element of a repeated scalar field.
    Element(FieldDescriptor),
}

/// From a message, through singular message fields, to a value. Each step says whether
/// Java skips the rest when the message is missing.
#[derive(Clone, PartialEq, Debug)]
struct Leaf {
    steps: Vec<(FieldDescriptor, bool)>,
    end: End,
}

#[derive(Clone, PartialEq, Debug)]
enum Getter {
    One(Leaf),
    /// Through singular message fields, each skipped when missing, to a repeated field,
    /// each element read by the leaf.
    Array {
        steps: Vec<FieldDescriptor>,
        list: FieldDescriptor,
        element: Leaf,
    },
    /// What Java makes a column of a class that has no column type here.
    Refused(String),
}

/// One function of Java's: the field path, which names the column, and the getter.
#[derive(Clone, Debug)]
struct Function {
    path: Vec<FieldDescriptor>,
    getter: Getter,
}

/// A column of a Protobuf spec.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ProtobufColumn {
    getter: Getter,
    cell: Cell,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Cell {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    Instant,
    Duration,
    IntArray,
    LongArray,
    FloatArray,
    DoubleArray,
    BooleanArray,
    StringArray,
    InstantArray,
    /// `byte[][]`
    BytesArray,
}

/// Whether a field path is included, by the `include` paths of the spec: none means all.
///
/// Behavior: io.deephaven.protobuf.FieldPath#matches and #anyMatches, as
/// `deephaven.stream.kafka.consumer.protobuf_spec` passes `include`
struct Include(Option<Vec<(Vec<String>, bool)>>);

impl Include {
    fn new(paths: Option<&[String]>) -> Self {
        Self(paths.map(|paths| paths.iter().map(|path| Self::compile(path)).collect()))
    }

    fn compile(path: &str) -> (Vec<String>, bool) {
        let path = path.strip_prefix('/').unwrap_or(path);
        let mut parts = java_split_slash(path);
        let star = parts.last().is_some_and(|last| last == "*");
        if star {
            parts.pop();
        }
        (parts, star)
    }

    fn includes(&self, path: &[FieldDescriptor]) -> bool {
        let Some(patterns) = &self.0 else {
            return true;
        };
        let names: Vec<&str> = path.iter().map(FieldDescriptor::name).collect();
        patterns.iter().any(|(pattern, star)| {
            // The pattern starts with the path, or with `*` the path starts with it.
            let leading = names.len() <= pattern.len() && pattern[..names.len()] == names[..];
            let under =
                *star && names.len() >= pattern.len() && names[..pattern.len()] == pattern[..];
            leading || under
        })
    }
}

/// Behavior: java.lang.String#split("/"): trailing empty parts are dropped, and text
/// without `/` is one part
fn java_split_slash(text: &str) -> Vec<String> {
    if !text.contains('/') {
        return vec![text.to_owned()];
    }
    let mut parts: Vec<String> = text.split('/').map(str::to_owned).collect();
    while parts.last().is_some_and(String::is_empty) {
        parts.pop();
    }
    parts
}

fn invalid(text: String) -> KafkaError {
    KafkaError::Invalid(text)
}

/// Translates a descriptor into Java's functions.
///
/// Behavior: io.deephaven.protobuf.ProtobufDescriptorParserImpl
struct Translator {
    include: Include,
}

impl Translator {
    /// Behavior: ProtobufDescriptorParserImpl.DescriptorContext, its cycle check and
    /// #functions
    fn message(
        &self,
        path: &[FieldDescriptor],
        message: &MessageDescriptor,
    ) -> Result<Vec<Function>, KafkaError> {
        let name = message.full_name();
        if path
            .iter()
            .any(|field| field.parent_message().full_name() == name)
        {
            let mut text = String::new();
            for field in path {
                text += &format!(
                    "`{}` \"{}\" -> ",
                    field.parent_message().full_name(),
                    field.name()
                );
            }
            text += &format!("`{name}`");
            return Err(invalid(format!(
                "java.lang.IllegalArgumentException: Cyclical protobuf message descriptor \
                 detected at [{text}]. Cyclic protobuf message descriptors are not supported; \
                 use `io.deephaven.protobuf.ProtobufDescriptorParserOptions` fieldOptions to \
                 exclude one of the fields to work around this."
            )));
        }
        if let Some(known) = WellKnown::of(message) {
            return Ok(vec![Function {
                path: Vec::new(),
                getter: Getter::One(Leaf {
                    steps: Vec::new(),
                    end: End::WellKnown(known, message.clone()),
                }),
            }]);
        }
        let mut out = Vec::new();
        for field in message.fields() {
            let mut field_path = path.to_vec();
            field_path.push(field.clone());
            if self.include.includes(&field_path) {
                out.extend(self.field(&field_path, &field)?);
            }
        }
        Ok(out)
    }

    /// Behavior: ProtobufDescriptorParserImpl.FieldContext#functions, FieldObject,
    /// MapFieldObject and RepeatedFieldObject
    fn field(
        &self,
        path: &[FieldDescriptor],
        field: &FieldDescriptor,
    ) -> Result<Vec<Function>, KafkaError> {
        let named = |getter: Getter| {
            vec![Function {
                path: vec![field.clone()],
                getter,
            }]
        };
        if field.is_map() {
            let Kind::Message(entry) = field.kind() else {
                unreachable!("a map field is of its entry message")
            };
            // The key and the value are included whatever `include` says, but what is
            // below the value is not.
            let value_field = entry.map_entry_value_field();
            let mut value_path = path.to_vec();
            value_path.push(value_field.clone());
            let values = self.field(&value_path, &value_field)?;
            if values.len() == 1 {
                return Ok(named(Getter::Refused("java.util.Map".to_owned())));
            }
            return self.repeated_message(path, field, &entry);
        }
        if field.is_list() {
            return Ok(match field.kind() {
                Kind::Message(inner) => self.repeated_message(path, field, &inner)?,
                Kind::Enum(_) => named(Getter::Refused(
                    "com.google.protobuf.Descriptors.EnumValueDescriptor[]".to_owned(),
                )),
                _ => named(Getter::Array {
                    steps: Vec::new(),
                    list: field.clone(),
                    element: Leaf {
                        steps: Vec::new(),
                        end: End::Element(field.clone()),
                    },
                }),
            });
        }
        let Kind::Message(inner) = field.kind() else {
            if let Kind::Enum(_) = field.kind() {
                return Ok(named(Getter::Refused(
                    "com.google.protobuf.Descriptors.EnumValueDescriptor".to_owned(),
                )));
            }
            return Ok(named(Getter::One(Leaf {
                steps: Vec::new(),
                end: End::Field(field.clone()),
            })));
        };
        // Java skips the rest when the message is missing, except right below an element
        // of a repeated field.
        let parent_repeated = path.len() >= 2 && protobuf::repeated(&path[path.len() - 2]);
        let mut out = Vec::new();
        for function in self.message(path, &inner)? {
            let mut function_path = vec![field.clone()];
            function_path.extend(function.path);
            let getter = match function.getter {
                Getter::One(mut leaf) => {
                    leaf.steps.insert(0, (field.clone(), !parent_repeated));
                    Getter::One(leaf)
                }
                Getter::Array {
                    mut steps,
                    list,
                    element,
                } => {
                    steps.insert(0, field.clone());
                    Getter::Array {
                        steps,
                        list,
                        element,
                    }
                }
                refused => refused,
            };
            out.push(Function {
                path: function_path,
                getter,
            });
        }
        Ok(out)
    }

    /// Behavior: ProtobufDescriptorParserImpl.FieldContext.RepeatedFieldObject#functions,
    /// for a message, and its ToRepeatedType
    fn repeated_message(
        &self,
        path: &[FieldDescriptor],
        field: &FieldDescriptor,
        inner: &MessageDescriptor,
    ) -> Result<Vec<Function>, KafkaError> {
        let mut out = Vec::new();
        for function in self.message(path, inner)? {
            let mut function_path = vec![field.clone()];
            function_path.extend(function.path);
            let getter = match function.getter {
                Getter::One(leaf) => Getter::Array {
                    steps: Vec::new(),
                    list: field.clone(),
                    element: leaf,
                },
                Getter::Array { .. } => Getter::Refused(
                    "an array of arrays (a repeated field in a repeated message)".to_owned(),
                ),
                Getter::Refused(class) => Getter::Refused(format!("{class}[]")),
            };
            out.push(Function {
                path: function_path,
                getter,
            });
        }
        Ok(out)
    }
}

/// The type of a value that a leaf gives, as Java types the function after
/// `ProtobufImpl#withMostAppropriateType`: `None` where Java's class has no column type here.
fn leaf_cell(leaf: &Leaf, in_array: bool) -> Result<Cell, String> {
    let primitive = |cell: Cell, presence: bool, boxed: &str| {
        if in_array && presence {
            Err(format!("java.lang.{boxed}[]"))
        } else {
            Ok(cell)
        }
    };
    match &leaf.end {
        End::Field(field) | End::Element(field) => {
            let presence = matches!(leaf.end, End::Field(_)) && field.supports_presence();
            match field.kind() {
                Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Uint32 | Kind::Fixed32 => {
                    primitive(Cell::Int, presence, "Integer")
                }
                Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint64 | Kind::Fixed64 => {
                    primitive(Cell::Long, presence, "Long")
                }
                Kind::Float => primitive(Cell::Float, presence, "Float"),
                Kind::Double => primitive(Cell::Double, presence, "Double"),
                Kind::Bool if in_array && !presence => Ok(Cell::BooleanArray),
                Kind::Bool => primitive(Cell::Boolean, presence, "Boolean"),
                Kind::String => Ok(Cell::String),
                Kind::Bytes if in_array => Ok(Cell::BytesArray),
                Kind::Bytes => Ok(Cell::Bytes),
                Kind::Enum(_) => {
                    Err("com.google.protobuf.Descriptors.EnumValueDescriptor".to_owned())
                }
                Kind::Message(_) => unreachable!("a message field is a step"),
            }
        }
        End::WellKnown(known, _) => Ok(match known {
            WellKnown::Timestamp => Cell::Instant,
            WellKnown::Duration if in_array => return Err("java.time.Duration[]".to_owned()),
            WellKnown::Duration => Cell::Duration,
            WellKnown::Bool if in_array => Cell::BooleanArray,
            WellKnown::Bool => Cell::Boolean,
            WellKnown::Int32 => Cell::Int,
            WellKnown::Int64 => Cell::Long,
            WellKnown::Float => Cell::Float,
            WellKnown::Double => Cell::Double,
            WellKnown::String => Cell::String,
            WellKnown::Bytes if in_array => Cell::BytesArray,
            WellKnown::Bytes => Cell::Bytes,
            WellKnown::Object(class) => return Err((*class).to_owned()),
        }),
    }
}

fn array_cell(element: Cell) -> Cell {
    match element {
        Cell::Int => Cell::IntArray,
        Cell::Long => Cell::LongArray,
        Cell::Float => Cell::FloatArray,
        Cell::Double => Cell::DoubleArray,
        Cell::String => Cell::StringArray,
        Cell::Instant => Cell::InstantArray,
        other => other,
    }
}

impl Cell {
    fn column_type(self) -> ColumnType {
        match self {
            Cell::Boolean => ColumnType::Boolean,
            Cell::Int => ColumnType::Int,
            Cell::Long => ColumnType::Long,
            Cell::Float => ColumnType::Float,
            Cell::Double => ColumnType::Double,
            Cell::String => ColumnType::String,
            Cell::Bytes => ColumnType::array(ColumnType::Byte),
            Cell::Instant => ColumnType::Instant,
            Cell::Duration => ColumnType::Duration,
            Cell::IntArray => ColumnType::array(ColumnType::Int),
            Cell::LongArray => ColumnType::array(ColumnType::Long),
            Cell::FloatArray => ColumnType::array(ColumnType::Float),
            Cell::DoubleArray => ColumnType::array(ColumnType::Double),
            Cell::BooleanArray => ColumnType::array(ColumnType::PrimitiveBoolean),
            Cell::StringArray => ColumnType::array(ColumnType::String),
            Cell::InstantArray => ColumnType::array(ColumnType::Instant),
            Cell::BytesArray => ColumnType::array(ColumnType::array(ColumnType::Byte)),
        }
    }
}

/// The message of a spec: the schema of `subject` at `version` fetched with what it
/// references, read, and the message named in it looked up, `None` for no such message.
///
/// Behavior: io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl#descriptor,
/// ProtobufSchema#toDescriptor(String)
pub(crate) fn fetch(
    registry: &Registry,
    subject: &str,
    version: Option<i32>,
    message: Option<&str>,
) -> Result<Option<MessageDescriptor>, KafkaError> {
    let schema = registry.protobuf_schema(subject, version)?;
    let pool = protobuf::compile(&schema.text, &schema.references).map_err(invalid)?;
    Ok(protobuf::message(&pool, message))
}

/// The columns of `message`: each with its name, its type and how it is read.
///
/// Behavior: io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl#getIngestData,
/// ProtobufConsumeOptions#joinNamePathWithUnderscore
pub(crate) fn columns(
    message: &MessageDescriptor,
    include: Option<&[String]>,
) -> Result<Vec<(String, ColumnType, ProtobufColumn)>, KafkaError> {
    let translator = Translator {
        include: Include::new(include),
    };
    let functions = translator.message(&[], message)?;
    let mut seen: Vec<Vec<String>> = Vec::new();
    let mut out = Vec::new();
    for function in functions {
        let key: Vec<String> = function
            .path
            .iter()
            .map(|f| f.full_name().to_owned())
            .collect();
        let occurrence = seen.iter().filter(|known| **known == key).count();
        seen.push(key);
        let simple: Vec<&str> = function.path.iter().map(FieldDescriptor::name).collect();
        let simple = simple.join("_");
        let name = if occurrence == 0 {
            simple
        } else {
            format!("{simple}_{}", occurrence + 1)
        };
        // Behavior: io.deephaven.api.ColumnName#of (probe ts_root)
        if name.is_empty() {
            return Err(invalid(
                "io.deephaven.api.util.NameValidator$InvalidNameException: Invalid column name: \
                 name can not be null or empty"
                    .to_owned(),
            ));
        }
        let refuse = |class: &str| {
            not_ported(
                "io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl#getIngestData",
                &format!(
                    "the column {name} of the Protobuf field {}, which Java makes of the class {class}",
                    function.path.last().map_or("", |f| f.full_name())
                ),
            )
        };
        let cell = match &function.getter {
            Getter::Refused(class) => return Err(refuse(class)),
            Getter::One(leaf) => leaf_cell(leaf, false).map_err(|class| refuse(&class))?,
            Getter::Array { element, .. } => {
                array_cell(leaf_cell(element, true).map_err(|class| refuse(&class))?)
            }
        };
        out.push((
            name,
            cell.column_type(),
            ProtobufColumn {
                getter: function.getter,
                cell,
            },
        ));
    }
    Ok(out)
}

/// The decoded key or value of a record, or the failure of decoding it.
pub(crate) fn decode(
    message: &MessageDescriptor,
    protocol: ProtobufProtocol,
    bytes: &[u8],
) -> Result<Message, String> {
    let start = match protocol {
        ProtobufProtocol::Raw => 0,
        ProtobufProtocol::Serdes => protobuf::serdes_offset(bytes)?,
    };
    protobuf::decode(message, &bytes[start..]).map_err(|cause| {
        if cause.starts_with("com.google.protobuf.InvalidProtocolBufferException") {
            // Behavior: ProtobufDeserializers, `new SerializationException(e)`
            format!("org.apache.kafka.common.errors.SerializationException: {cause}")
        } else {
            cause
        }
    })
}

/// A value read through a leaf: a Java value, or null.
enum Read<'m> {
    Null,
    Value(Value),
    Message(&'m Message),
}

fn record_failure(text: String) -> KafkaError {
    KafkaError::Record(text)
}

/// The helpful text of the NullPointerException Java's function throws when it is given
/// a missing message. Behavior: java.lang.NullPointerException#getMessage (probes npe_*,
/// rh_absent)
fn null_pointer(end: &End) -> KafkaError {
    let call = match end {
        End::Field(field) | End::Element(field) => match field.kind() {
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 | Kind::Uint32 | Kind::Fixed32 => {
                "java.lang.Integer.intValue()"
            }
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint64 | Kind::Fixed64 => {
                "java.lang.Long.longValue()"
            }
            Kind::Float => "java.lang.Float.floatValue()",
            Kind::Double => "java.lang.Double.doubleValue()",
            Kind::Bool => "java.lang.Boolean.booleanValue()",
            _ => unreachable!("an object is null, not a failure"),
        },
        End::WellKnown(..) => {
            "com.google.protobuf.Message.getField(com.google.protobuf.Descriptors$FieldDescriptor)"
        }
    };
    record_failure(format!(
        "java.lang.NullPointerException: Cannot invoke \"{call}\" because \"value\" is null"
    ))
}

/// Reads a leaf from `message`, which Java may give as null.
///
/// Behavior: ProtobufDescriptorParserImpl.FieldContext.FieldObject#apply, BypassOnNull,
/// and the well-known types' functions of io.deephaven.protobuf.Builtin
fn read<'m>(leaf: &Leaf, message: Option<&'m Message>) -> Result<Read<'m>, KafkaError> {
    let mut current = message;
    for (field, skips) in &leaf.steps {
        let next = current.and_then(|m| m.message(field));
        if next.is_none() && *skips {
            return Ok(Read::Null);
        }
        current = next;
    }
    match &leaf.end {
        End::Element(_) => unreachable!("an element is read by `element`"),
        End::Field(field) => {
            let Some(message) = current else {
                return match field.kind() {
                    Kind::String | Kind::Bytes => Ok(Read::Null),
                    _ if field.supports_presence() => Ok(Read::Null),
                    _ => Err(null_pointer(&leaf.end)),
                };
            };
            if let Kind::Message(_) = field.kind() {
                return Ok(message.message(field).map_or(Read::Null, Read::Message));
            }
            match message.get(field) {
                Some(value) => Ok(Read::Value(value.clone())),
                None if field.supports_presence() => Ok(Read::Null),
                None => Ok(Read::Value(protobuf::default_value(field))),
            }
        }
        End::WellKnown(_, descriptor) => {
            let Some(message) = current else {
                return Err(null_pointer(&leaf.end));
            };
            // The value of a wrapper, or the message of a timestamp or duration.
            if let Some(value_field) = descriptor.get_field_by_name("value") {
                return Ok(Read::Value(
                    message
                        .get(&value_field)
                        .cloned()
                        .unwrap_or_else(|| protobuf::default_value(&value_field)),
                ));
            }
            Ok(Read::Message(message))
        }
    }
}

/// The seconds and nanoseconds of a timestamp or duration message.
fn seconds_nanos(descriptor: &MessageDescriptor, message: &Message) -> (i64, i32) {
    let get = |name: &str| {
        let field = descriptor
            .get_field_by_name(name)
            .expect("well-known field");
        message
            .get(&field)
            .cloned()
            .unwrap_or_else(|| protobuf::default_value(&field))
    };
    let seconds = match get("seconds") {
        Value::Long(l) => l,
        _ => 0,
    };
    let nanos = match get("nanos") {
        Value::Int(i) => i,
        _ => 0,
    };
    (seconds, nanos)
}

/// The nanoseconds since the epoch of `Instant.ofEpochSecond(seconds, nanos)`, as
/// `DateTimeUtils#epochNanos` computes them for a column; `array` for an element of an
/// `Instant[]`, which here must hold nanoseconds too.
///
/// Behavior: java.time.Instant#ofEpochSecond(long, long), DateTimeUtils#epochNanos
fn instant_nanos(seconds: i64, nanos: i32, array: bool) -> Result<Option<i64>, KafkaError> {
    let nanos = i64::from(nanos);
    let seconds = seconds
        .checked_add(nanos.div_euclid(1_000_000_000))
        .ok_or_else(|| record_failure("java.lang.ArithmeticException: long overflow".to_owned()))?;
    let nanos = nanos.rem_euclid(1_000_000_000);
    if !(-31_557_014_167_219_200..=31_556_889_864_403_199).contains(&seconds) {
        return Err(record_failure(
            "java.time.DateTimeException: Instant exceeds minimum or maximum instant".to_owned(),
        ));
    }
    if array {
        // Java keeps the Instant; a cell here is nanoseconds.
        return seconds
            .checked_mul(1_000_000_000)
            .and_then(|n| n.checked_add(nanos))
            .map(Some)
            .ok_or_else(|| {
                not_ported(
                    "io.deephaven.time.DateTimeUtils#epochNanos",
                    "an element of an Instant[] beyond the nanoseconds of a long",
                )
            });
    }
    if seconds >= i64::MAX / 1_000_000_000 {
        return Err(record_failure(format!(
            "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: Numeric overflow \
             detected during conversion of {seconds} to nanoseconds"
        )));
    }
    // Java's product wraps below the range, and its null is a null.
    let value = seconds.wrapping_mul(1_000_000_000).wrapping_add(nanos);
    Ok(Some(value).filter(|v| *v != i64::MIN))
}

/// Behavior: java.time.Duration#ofSeconds(long, long); a duration beyond the nanoseconds of
/// a long, which Java holds and fails to send (`Duration#toNanos`), is refused
fn duration_nanos(seconds: i64, nanos: i32) -> Result<i64, KafkaError> {
    let nanos = i64::from(nanos);
    let seconds = seconds
        .checked_add(nanos.div_euclid(1_000_000_000))
        .ok_or_else(|| record_failure("java.lang.ArithmeticException: long overflow".to_owned()))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(nanos.rem_euclid(1_000_000_000)))
        .ok_or_else(|| {
            not_ported(
                "java.time.Duration#toNanos",
                "a Duration beyond the nanoseconds of a long",
            )
        })
}

/// A scalar cell from what a leaf read.
#[derive(Clone, Debug, PartialEq)]
enum Scalar {
    Null,
    Bool(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Nanos(i64),
}

fn scalar(leaf: &Leaf, read: Read, cell: Cell, array: bool) -> Result<Scalar, KafkaError> {
    Ok(match read {
        Read::Null => Scalar::Null,
        Read::Value(value) => match value {
            Value::Int(i) => Scalar::Int(i),
            Value::Long(l) => Scalar::Long(l),
            Value::Float(f) => Scalar::Float(f),
            Value::Double(d) => Scalar::Double(d),
            Value::Bool(b) => Scalar::Bool(b),
            Value::String(s) => Scalar::String(s),
            Value::Bytes(b) => Scalar::Bytes(b),
            Value::Enum(_) | Value::Message(_) => unreachable!("refused in `columns`"),
        },
        Read::Message(message) => {
            let End::WellKnown(_, descriptor) = &leaf.end else {
                unreachable!("only a well-known type reads as a message")
            };
            let (seconds, nanos) = seconds_nanos(descriptor, message);
            match cell {
                Cell::Duration => Scalar::Nanos(duration_nanos(seconds, nanos)?),
                _ => match instant_nanos(seconds, nanos, array)? {
                    Some(n) => Scalar::Nanos(n),
                    None => Scalar::Null,
                },
            }
        }
    })
}

/// The cells of `column` for the decoded records. A value equal to Java's null of the
/// column's type is a null, as Java stores it.
///
/// Behavior: io.deephaven.kafka.ProtobufImpl.ProtobufConsumeImpl#add (the field copiers
/// over `ToChunkTypeTransform` of each function)
pub(crate) fn cells(column: &ProtobufColumn, values: &[Message]) -> Result<ArrayRef, KafkaError> {
    match &column.getter {
        Getter::One(leaf) => {
            let mut cells = Vec::with_capacity(values.len());
            for message in values {
                let read = read(leaf, Some(message))?;
                cells.push(scalar(leaf, read, column.cell, false)?);
            }
            Ok(scalar_array(column.cell, &cells))
        }
        Getter::Array {
            steps,
            list,
            element,
        } => {
            let mut rows: Vec<Option<Vec<Scalar>>> = Vec::with_capacity(values.len());
            for message in values {
                let mut current = Some(message);
                for field in steps {
                    current = current.and_then(|m| m.message(field));
                }
                let Some(holder) = current else {
                    rows.push(None);
                    continue;
                };
                let mut items = Vec::new();
                for value in holder.list(list) {
                    items.push(match (&element.end, value) {
                        (End::Element(_), value) => {
                            scalar(element, Read::Value(value.clone()), column.cell, true)?
                        }
                        (_, Value::Message(sub)) => {
                            let read = read(element, Some(sub))?;
                            scalar(element, read, column.cell, true)?
                        }
                        _ => unreachable!("a repeated message holds messages"),
                    });
                }
                rows.push(Some(items));
            }
            Ok(list_array(column.cell, &rows))
        }
        Getter::Refused(_) => unreachable!("refused in `columns`"),
    }
}

fn scalar_array(cell: Cell, cells: &[Scalar]) -> ArrayRef {
    macro_rules! collect {
        ($array:ty, $variant:ident, $null:expr) => {
            Arc::new(<$array>::from(
                cells
                    .iter()
                    .map(|c| match c {
                        Scalar::$variant(v) => Some(v.clone()).filter(|v| *v != $null),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match cell {
        Cell::Boolean => Arc::new(BooleanArray::from(
            cells
                .iter()
                .map(|c| match c {
                    Scalar::Bool(b) => Some(*b),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        Cell::Int => collect!(Int32Array, Int, i32::MIN),
        Cell::Long => collect!(Int64Array, Long, i64::MIN),
        Cell::Float => collect!(Float32Array, Float, -f32::MAX),
        Cell::Double => collect!(Float64Array, Double, -f64::MAX),
        Cell::String => Arc::new(StringArray::from(
            cells
                .iter()
                .map(|c| match c {
                    Scalar::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        Cell::Bytes => {
            let field = Field::new("", ColumnType::Byte.arrow_type(), true);
            let mut b = ListBuilder::new(Int8Builder::new()).with_field(Arc::new(field));
            for c in cells {
                match c {
                    Scalar::Bytes(bytes) => {
                        b.values().extend(bytes.iter().map(|x| Some(*x as i8)));
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        Cell::Instant => Arc::new(
            TimestampNanosecondArray::from(
                cells
                    .iter()
                    .map(|c| match c {
                        Scalar::Nanos(n) => Some(*n),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Cell::Duration => Arc::new(DurationNanosecondArray::from(
            cells
                .iter()
                .map(|c| match c {
                    Scalar::Nanos(n) => Some(*n),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        _ => unreachable!("an array cell is built by `list_array`"),
    }
}

fn list_array(cell: Cell, rows: &[Option<Vec<Scalar>>]) -> ArrayRef {
    let item = |element: ColumnType| Arc::new(Field::new("", element.arrow_type(), true));
    macro_rules! lists {
        ($builder:expr, $element:expr, |$b:ident, $s:ident| $push:expr) => {{
            let mut list = ListBuilder::new($builder).with_field(item($element));
            for row in rows {
                match row {
                    Some(items) => {
                        for $s in items {
                            let $b = list.values();
                            $push;
                        }
                        list.append(true);
                    }
                    None => list.append(false),
                }
            }
            Arc::new(list.finish()) as ArrayRef
        }};
    }
    match cell {
        Cell::IntArray => lists!(Int32Builder::new(), ColumnType::Int, |b, s| b
            .append_option(match s {
                Scalar::Int(v) => Some(*v).filter(|v| *v != i32::MIN),
                _ => None,
            })),
        Cell::LongArray => lists!(Int64Builder::new(), ColumnType::Long, |b, s| b
            .append_option(match s {
                Scalar::Long(v) => Some(*v).filter(|v| *v != i64::MIN),
                _ => None,
            })),
        Cell::FloatArray => lists!(Float32Builder::new(), ColumnType::Float, |b, s| b
            .append_option(match s {
                Scalar::Float(v) => Some(*v).filter(|v| *v != -f32::MAX),
                _ => None,
            })),
        Cell::DoubleArray => lists!(Float64Builder::new(), ColumnType::Double, |b, s| b
            .append_option(match s {
                Scalar::Double(v) => Some(*v).filter(|v| *v != -f64::MAX),
                _ => None,
            })),
        Cell::BooleanArray => lists!(
            BooleanBuilder::new(),
            ColumnType::PrimitiveBoolean,
            |b, s| b.append_option(match s {
                Scalar::Bool(v) => Some(*v),
                _ => None,
            })
        ),
        Cell::StringArray => lists!(StringBuilder::new(), ColumnType::String, |b, s| b
            .append_option(match s {
                Scalar::String(v) => Some(v.as_str()),
                _ => None,
            })),
        Cell::InstantArray => lists!(
            TimestampNanosecondBuilder::new().with_timezone("UTC"),
            ColumnType::Instant,
            |b, s| b.append_option(match s {
                Scalar::Nanos(v) => Some(*v).filter(|v| *v != i64::MIN),
                _ => None,
            })
        ),
        Cell::BytesArray => {
            let DataType::List(outer) = cell.column_type().arrow_type() else {
                unreachable!("an array is a list")
            };
            let DataType::List(inner) = outer.data_type().clone() else {
                unreachable!("an array of arrays is a list of lists")
            };
            let bytes = ListBuilder::new(Int8Builder::new()).with_field(inner);
            let mut list = ListBuilder::new(bytes).with_field(outer);
            for row in rows {
                match row {
                    Some(items) => {
                        for item in items {
                            let element = list.values();
                            match item {
                                Scalar::Bytes(bytes) => {
                                    element
                                        .values()
                                        .extend(bytes.iter().map(|b| Some(*b as i8)));
                                    element.append(true);
                                }
                                _ => element.append(false),
                            }
                        }
                        list.append(true);
                    }
                    None => list.append(false),
                }
            }
            Arc::new(list.finish())
        }
        _ => unreachable!("a scalar cell is built by `scalar_array`"),
    }
}
