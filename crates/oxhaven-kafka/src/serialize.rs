//! Rows as the keys and values of records: what Java's chunk serializers and Kafka's
//! serializers make of a table's cells.
//!
//! A simple spec writes one column with Kafka's serializer for its type: big-endian
//! numbers of the type's size, text in the configured encoding, a `byte[]` as it is. A JSON
//! spec writes an object per row with Jackson, as `JsonKeyOrValueSerializer` builds it:
//! a template with the nested objects, copied for each row, then each column put in its
//! field in order. `docs/KAFKA.md`, "How a row becomes a record", has the details.

use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    Time64NanosecondType, TimestampNanosecondType, UInt16Type,
};
use oxhaven_column::java_values::{
    big_decimal_parts, big_decimal_text, big_integer_text, local_date_text, local_time_text,
};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;

use crate::json::write_quoted;
use crate::{not_ported, property, KafkaError};

/// The charset of Kafka's `StringSerializer`, which writes the text of a simple `String`
/// column and of a JSON spec.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Encoding {
    Utf8,
    /// Big-endian with a byte order mark, as Java's `UTF-16` encoder writes it.
    Utf16,
    Utf16Be,
    Utf16Le,
    Latin1,
    Ascii,
}

impl Encoding {
    /// The encoding the properties give a key's or a value's serializer.
    ///
    /// Behavior: org.apache.kafka.common.serialization.StringSerializer#configure:
    /// `key.serializer.encoding` or `value.serializer.encoding`, else
    /// `serializer.encoding`, else UTF-8; the name as `java.nio.charset.Charset#forName`
    /// takes it, in any case
    pub fn of(config: &[(String, String)], is_key: bool) -> Result<Self, KafkaError> {
        let own = if is_key {
            "key.serializer.encoding"
        } else {
            "value.serializer.encoding"
        };
        let Some(name) = property(config, own).or_else(|| property(config, "serializer.encoding"))
        else {
            return Ok(Encoding::Utf8);
        };
        let known = [
            ("UTF-8", Encoding::Utf8),
            ("UTF8", Encoding::Utf8),
            ("UTF-16", Encoding::Utf16),
            ("UTF-16BE", Encoding::Utf16Be),
            ("UTF-16LE", Encoding::Utf16Le),
            ("ISO-8859-1", Encoding::Latin1),
            ("US-ASCII", Encoding::Ascii),
        ];
        known
            .iter()
            .find(|(known, _)| known.eq_ignore_ascii_case(name))
            .map(|(_, encoding)| *encoding)
            .ok_or_else(|| {
                not_ported(
                    "java.nio.charset.Charset#forName",
                    &format!(
                        "the encoding {name:?}: only UTF-8, UTF-16, UTF-16BE, UTF-16LE, \
                         ISO-8859-1 and US-ASCII are ported"
                    ),
                )
            })
    }

    /// Behavior: java.lang.String#getBytes(Charset): a character the charset cannot hold
    /// is `?` in ISO-8859-1 and US-ASCII
    pub fn encode(self, text: &str) -> Vec<u8> {
        let units = |big: bool| -> Vec<u8> {
            let bytes = |unit: u16| if big { unit.to_be_bytes() } else { unit.to_le_bytes() };
            text.encode_utf16().flat_map(bytes).collect()
        };
        let narrow = |limit: u32| -> Vec<u8> {
            // A supplementary character is one character here and two in Java, and Java
            // writes one `?` for the pair.
            let byte = |c: char| if (c as u32) < limit { c as u8 } else { b'?' };
            text.chars().map(byte).collect()
        };
        match self {
            Encoding::Utf8 => text.as_bytes().to_vec(),
            Encoding::Utf16 => {
                let mut out = vec![0xFE, 0xFF];
                out.extend(units(true));
                out
            }
            Encoding::Utf16Be => units(true),
            Encoding::Utf16Le => units(false),
            Encoding::Latin1 => narrow(0x100),
            Encoding::Ascii => narrow(0x80),
        }
    }
}

fn record_failure(failure: impl ToString) -> KafkaError {
    KafkaError::Record(failure.to_string())
}

fn cells(
    table: &dyn Table,
    column: usize,
    rows: &RowSet,
    prev: bool,
) -> Result<ArrayRef, KafkaError> {
    let source = table.column(column);
    let chunk = if prev {
        source.prev_chunk(rows)
    } else {
        source.chunk(rows)
    };
    chunk.map_err(record_failure)
}

/// The text of the `io.deephaven.qst.type.Type` of a column, as Java's refusal of a simple
/// spec prints it; `None` where it was not seen.
///
/// Behavior: io.deephaven.qst.type.Type#find and the `toString` of its types
fn qst_type_name(column_type: &ColumnType) -> Option<String> {
    let primitive = |column_type: &ColumnType| {
        Some(match column_type {
            ColumnType::Byte => ("B", "ByteType", "Byte"),
            ColumnType::Char => ("C", "CharType", "Char"),
            ColumnType::Short => ("S", "ShortType", "Short"),
            ColumnType::Int => ("I", "IntType", "Int"),
            ColumnType::Long => ("J", "LongType", "Long"),
            ColumnType::Float => ("F", "FloatType", "Float"),
            ColumnType::Double => ("D", "DoubleType", "Double"),
            _ => return None,
        })
    };
    let custom = |class: &str| format!("CustomType{{clazz=class {class}}}");
    Some(match column_type {
        ColumnType::Boolean => "BoxedType(io.deephaven.qst.type.BooleanType)".to_owned(),
        ColumnType::Instant => "io.deephaven.qst.type.InstantType".to_owned(),
        ColumnType::LocalDate => "io.deephaven.qst.type.LocalDateType".to_owned(),
        ColumnType::LocalTime => "io.deephaven.qst.type.LocalTimeType".to_owned(),
        ColumnType::Duration => "io.deephaven.qst.type.DurationType".to_owned(),
        ColumnType::BigDecimal => custom("java.math.BigDecimal"),
        ColumnType::BigInteger => custom("java.math.BigInteger"),
        ColumnType::ZoneId => custom("java.time.ZoneId"),
        ColumnType::Period => custom("java.time.Period"),
        ColumnType::LocalDateTime => custom("java.time.LocalDateTime"),
        ColumnType::ZonedDateTime => custom("java.time.ZonedDateTime"),
        ColumnType::Array(element) => {
            let (code, name, _) = primitive(element)?;
            format!(
                "NativeArrayType{{clazz=class [{code}, componentType=io.deephaven.qst.type.{name}}}"
            )
        }
        ColumnType::Vector(element) => {
            let (_, name, vector) = primitive(element)?;
            format!(
                "PrimitiveVectorType{{clazz=interface io.deephaven.vector.{vector}Vector, \
                 componentType=io.deephaven.qst.type.{name}}}"
            )
        }
        other => format!("io.deephaven.qst.type.{}", primitive(other)?.1),
    })
}

/// What Kafka serializer writes a simple column.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Simple {
    Short,
    Int,
    Long,
    Float,
    Double,
    Text(Encoding),
    Bytes,
}

/// How one side of a record, the key or the value, is written.
pub(crate) enum Serializer {
    Simple { column: usize, kind: Simple },
    Json(JsonSerializer),
}

impl Serializer {
    /// A simple spec: Kafka's serializer for the column's type.
    ///
    /// Behavior: io.deephaven.kafka.SimpleImpl.SimpleProduce#getSerializer, its
    /// `SerDeserVisitor`, and `SimpleKeyOrValueSerializer`
    pub fn simple(
        definition: &TableDefinition,
        column: &str,
        encoding: Encoding,
    ) -> Result<Self, KafkaError> {
        let Some(index) = definition.index_of(column) else {
            return Err(KafkaError::Invalid(
                "java.lang.NullPointerException: Cannot invoke \
                 \"io.deephaven.engine.table.ColumnDefinition.getDataType()\" because \"cd\" \
                 is null"
                    .to_owned(),
            ));
        };
        let column_type = &definition.columns()[index].column_type;
        let kind = match column_type {
            ColumnType::Short => Simple::Short,
            ColumnType::Int => Simple::Int,
            ColumnType::Long => Simple::Long,
            ColumnType::Float => Simple::Float,
            ColumnType::Double => Simple::Double,
            ColumnType::String => Simple::Text(encoding),
            ColumnType::Array(element) if **element == ColumnType::Byte => Simple::Bytes,
            other => {
                return Err(match qst_type_name(other) {
                    Some(name) => KafkaError::Invalid(format!(
                        "io.deephaven.UncheckedDeephavenException: Serializer not found for \
                         column {column}, type {name}"
                    )),
                    None => not_ported(
                        "io.deephaven.kafka.SimpleImpl.SerDeserVisitor",
                        &format!("a simple column {column} of type {other:?}"),
                    ),
                })
            }
        };
        Ok(Serializer::Simple {
            column: index,
            kind,
        })
    }

    /// The same serializer over another table that has the same columns, maybe in another
    /// order: the one a `lastBy` gives.
    pub fn over(self, from: &TableDefinition, to: &TableDefinition) -> Self {
        let moved = |index: usize| {
            let name = &from.columns()[index].name;
            to.index_of(name)
                .expect("the table published has every column of the spec")
        };
        match self {
            Serializer::Simple { column, kind } => Serializer::Simple {
                column: moved(column),
                kind,
            },
            Serializer::Json(mut json) => {
                for processor in &mut json.processors {
                    if let Source::Column { index, .. } = &mut processor.source {
                        *index = moved(*index);
                    }
                }
                Serializer::Json(json)
            }
        }
    }

    /// The key or value of each of `rows`, `None` for a record without one. `prev` reads
    /// the values before this cycle's changes, as for removed rows.
    ///
    /// Behavior: io.deephaven.kafka.publish.KeyOrValueSerializer#handleChunk
    pub fn serialize(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        prev: bool,
    ) -> Result<Vec<Option<Vec<u8>>>, KafkaError> {
        match self {
            Serializer::Simple { column, kind } => {
                simple_cells(&*cells(table, *column, rows, prev)?, *kind)
            }
            Serializer::Json(json) => json.serialize(table, rows, prev),
        }
    }
}

/// Behavior: the serializers of org.apache.kafka.common.serialization.Serdes for `Short`,
/// `Integer`, `Long`, `Float` (`floatToRawIntBits`), `Double`, `String` and `byte[]`, of
/// the boxed cell, which is null for a null cell
fn simple_cells(array: &dyn Array, kind: Simple) -> Result<Vec<Option<Vec<u8>>>, KafkaError> {
    fn each<T>(
        values: impl Iterator<Item = Option<T>>,
        bytes: impl Fn(T) -> Vec<u8>,
    ) -> Vec<Option<Vec<u8>>> {
        values.map(|value| value.map(&bytes)).collect()
    }
    Ok(match kind {
        Simple::Short => each(array.as_primitive::<Int16Type>().iter(), |v| {
            v.to_be_bytes().to_vec()
        }),
        Simple::Int => each(array.as_primitive::<Int32Type>().iter(), |v| {
            v.to_be_bytes().to_vec()
        }),
        Simple::Long => each(array.as_primitive::<Int64Type>().iter(), |v| {
            v.to_be_bytes().to_vec()
        }),
        Simple::Float => each(array.as_primitive::<Float32Type>().iter(), |v| {
            v.to_bits().to_be_bytes().to_vec()
        }),
        Simple::Double => each(array.as_primitive::<Float64Type>().iter(), |v| {
            v.to_bits().to_be_bytes().to_vec()
        }),
        Simple::Text(encoding) => each(array.as_string::<i32>().iter(), |v| encoding.encode(v)),
        Simple::Bytes => {
            let lists = array.as_list::<i32>();
            (0..lists.len())
                .map(|row| {
                    lists.is_valid(row).then(|| {
                        let bytes = lists.value(row);
                        let bytes = bytes.as_primitive::<Int8Type>();
                        bytes.values().iter().map(|byte| *byte as u8).collect()
                    })
                })
                .collect()
        }
    })
}

/// How a cell of a JSON field is written, by the column's Java type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Field {
    Boolean,
    Byte,
    Char,
    Short,
    Int,
    Long,
    Float,
    Double,
    BigDecimal,
    BigInteger,
    String,
    Instant,
    LocalDate,
    LocalTime,
    ZoneId,
}

/// Behavior: JsonKeyOrValueSerializer#makeFieldProcessor: a processor per primitive type,
/// `Boolean`, `BigDecimal` and `BigInteger`; every other type is put as its `toString`
fn json_field(column: &str, column_type: &ColumnType) -> Result<Field, KafkaError> {
    Ok(match column_type {
        ColumnType::Boolean => Field::Boolean,
        ColumnType::Byte => Field::Byte,
        ColumnType::Char => Field::Char,
        ColumnType::Short => Field::Short,
        ColumnType::Int => Field::Int,
        ColumnType::Long => Field::Long,
        ColumnType::Float => Field::Float,
        ColumnType::Double => Field::Double,
        ColumnType::BigDecimal => Field::BigDecimal,
        ColumnType::BigInteger => Field::BigInteger,
        ColumnType::String => Field::String,
        ColumnType::Instant => Field::Instant,
        ColumnType::LocalDate => Field::LocalDate,
        ColumnType::LocalTime => Field::LocalTime,
        ColumnType::ZoneId => Field::ZoneId,
        // An array's `toString` is its class and identity hash (`[B@7841b281`, seen in the
        // probes), which no port can reproduce; the others are not ported yet.
        other => {
            return Err(not_ported(
                "java.util.Objects#toString",
                &format!("the text of a cell of type {other:?} in the JSON field of {column}"),
            ))
        }
    })
}

/// A node of the object Jackson writes for a row: an object, or a value already written
/// with the class of its Jackson node, which a cast error names.
#[derive(Clone, Debug)]
enum Out {
    Object(Vec<(String, Out)>),
    Value { json: String, class: &'static str },
}

/// Behavior: com.fasterxml.jackson.databind.node.ObjectNode#set: a field already there
/// keeps its place and takes the new value
fn put(object: &mut Vec<(String, Out)>, name: &str, value: Out) {
    match object.iter_mut().find(|(field, _)| field == name) {
        Some((_, old)) => *old = value,
        None => object.push((name.to_owned(), value)),
    }
}

fn write_object(fields: &[(String, Out)], out: &mut String) {
    out.push('{');
    for (i, (name, value)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_quoted(name, out);
        out.push(':');
        match value {
            Out::Object(fields) => write_object(fields, out),
            Out::Value { json, .. } => out.push_str(json),
        }
    }
    out.push('}');
}

fn class_cast(class: &str) -> KafkaError {
    let node = "com.fasterxml.jackson.databind.node";
    KafkaError::Record(format!(
        "java.lang.ClassCastException: class {node}.{class} cannot be cast to class \
         {node}.ObjectNode ({node}.{class} and {node}.ObjectNode are in unnamed module of \
         loader 'app')"
    ))
}

/// The object a field's parents lead to in `root`.
///
/// Behavior: JsonKeyOrValueSerializer.JSONFieldProcessor#getChildNode, whose cast of each
/// parent to `ObjectNode` fails where an earlier field put a value in its place
fn child<'a>(
    root: &'a mut Vec<(String, Out)>,
    parents: &[String],
) -> Result<&'a mut Vec<(String, Out)>, KafkaError> {
    let mut node = root;
    for name in parents {
        let found = node.iter_mut().find(|(field, _)| field == name);
        node = match found {
            Some((_, Out::Object(fields))) => fields,
            Some((_, Out::Value { class, .. })) => return Err(class_cast(class)),
            // The template made every parent, and a field is only ever replaced.
            None => unreachable!("the template of a JSON spec has every parent object"),
        };
    }
    Ok(node)
}

/// Where a field's cells come from.
#[derive(Clone, Debug)]
enum Source {
    Column { index: usize, field: Field },
    /// The time of the serialization in nanoseconds, as text.
    Timestamp,
}

#[derive(Clone, Debug)]
struct Processor {
    /// The objects the field is nested in, outermost first.
    parents: Vec<String>,
    name: String,
    source: Source,
}

/// Splits a field name at a nested-object delimiter.
///
/// Behavior: java.lang.String#split(String): the delimiter is a regular expression; one
/// that matches only itself is split on as text, and with it Java's rules: an empty
/// string before a delimiter at the start is kept, empty strings at the end are dropped,
/// and a name without the delimiter is itself
fn java_split(text: &str, delimiter: &str) -> Result<Vec<String>, KafkaError> {
    const META: &str = "\\^$.|?*+()[]{}";
    let mut chars = delimiter.chars();
    let literal = match (chars.next(), chars.next(), chars.next()) {
        // `\` and a character that is no letter or digit is that character.
        (Some('\\'), Some(c), None) if !c.is_ascii_alphanumeric() => c.to_string(),
        _ if !delimiter.is_empty() && !delimiter.chars().any(|c| META.contains(c)) => {
            delimiter.to_owned()
        }
        _ => {
            return Err(not_ported(
                "java.lang.String#split",
                &format!("the nested delimiter {delimiter:?}, a regular expression"),
            ))
        }
    };
    let mut parts: Vec<String> = text.split(literal.as_str()).map(str::to_owned).collect();
    while parts.len() > 1 && parts.last().is_some_and(String::is_empty) {
        parts.pop();
    }
    if parts.len() == 1 && parts[0].is_empty() && !text.is_empty() {
        parts.clear();
    }
    Ok(parts)
}

fn index_out_of_bounds() -> String {
    "java.lang.ArrayIndexOutOfBoundsException: Index -1 out of bounds for length 0".to_owned()
}

/// The arguments of a JSON spec that shape each object.
pub(crate) struct JsonShape<'a> {
    /// The table's columns to write, each with its field.
    pub fields: Vec<(&'a str, &'a str)>,
    pub nested_delimiter: Option<&'a str>,
    pub output_nulls: bool,
    pub timestamp_field: Option<&'a str>,
}

/// A JSON spec's serializer.
///
/// Behavior: io.deephaven.kafka.publish.JsonKeyOrValueSerializer
pub(crate) struct JsonSerializer {
    processors: Vec<Processor>,
    template: Vec<(String, Out)>,
    output_nulls: bool,
    encoding: Encoding,
}

impl JsonSerializer {
    /// Behavior: JsonKeyOrValueSerializer#JsonKeyOrValueSerializer
    pub fn new(
        definition: &TableDefinition,
        shape: &JsonShape<'_>,
        encoding: Encoding,
    ) -> Result<Self, KafkaError> {
        let path = |name: &str| -> Result<Option<(Vec<String>, String)>, KafkaError> {
            let mut names = match shape.nested_delimiter {
                Some(delimiter) => java_split(name, delimiter)?,
                None => vec![name.to_owned()],
            };
            Ok(names.pop().map(|last| (names, last)))
        };
        let mut processors = Vec::new();
        for (column, field_name) in &shape.fields {
            let unknown = |cause: String| {
                KafkaError::Invalid(format!(
                    "java.lang.IllegalArgumentException: Unknown column name {column} for \
                     table (caused by {cause})"
                ))
            };
            let Some(index) = definition.index_of(column) else {
                return Err(unknown(format!(
                    "io.deephaven.engine.table.impl.NoSuchColumnException: {column}"
                )));
            };
            let field = json_field(column, &definition.columns()[index].column_type)?;
            let (parents, name) = path(field_name)?.ok_or_else(|| unknown(index_out_of_bounds()))?;
            processors.push(Processor {
                parents,
                name,
                source: Source::Column { index, field },
            });
        }
        if let Some(field_name) = shape.timestamp_field.filter(|name| !name.is_empty()) {
            let (parents, name) = path(field_name)?
                .ok_or_else(|| KafkaError::Invalid(index_out_of_bounds()))?;
            processors.push(Processor {
                parents,
                name,
                source: Source::Timestamp,
            });
        }
        let mut template = Vec::new();
        for processor in &processors {
            let mut node = &mut template;
            for name in &processor.parents {
                if !node.iter().any(|(field, _)| field == name) {
                    node.push((name.clone(), Out::Object(Vec::new())));
                }
                node = match node.iter_mut().find(|(field, _)| field == name) {
                    Some((_, Out::Object(fields))) => fields,
                    _ => unreachable!("the template holds only objects"),
                };
            }
        }
        Ok(Self {
            processors,
            template,
            output_nulls: shape.output_nulls,
            encoding,
        })
    }

    /// Behavior: JsonKeyOrValueSerializer#handleChunk: each field for every row in turn,
    /// then each row written by Jackson's `ObjectMapper#writeValueAsString` and encoded
    fn serialize(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        prev: bool,
    ) -> Result<Vec<Option<Vec<u8>>>, KafkaError> {
        let count = rows.size() as usize;
        let mut objects = vec![self.template.clone(); count];
        for processor in &self.processors {
            let values = match &processor.source {
                Source::Column { index, field } => {
                    let array = cells(table, *index, rows, prev)?;
                    self.json_values(&*array, *field)?
                }
                Source::Timestamp => {
                    // Behavior: JsonKeyOrValueSerializer.TimestampFieldProcessor: one time
                    // for the whole chunk
                    let since = SystemTime::now().duration_since(UNIX_EPOCH);
                    let nanos = since.map_or(0, |since| since.as_nanos());
                    let mut json = String::new();
                    write_quoted(&nanos.to_string(), &mut json);
                    let value = Out::Value {
                        json,
                        class: "TextNode",
                    };
                    vec![Some(value); count]
                }
            };
            for (object, value) in objects.iter_mut().zip(values) {
                let node = child(object, &processor.parents)?;
                if let Some(value) = value {
                    put(node, &processor.name, value);
                }
            }
        }
        Ok(objects
            .iter()
            .map(|object| {
                let mut json = String::new();
                write_object(object, &mut json);
                Some(self.encoding.encode(&json))
            })
            .collect())
    }

    /// Each cell as the value Jackson writes, `None` for a null left out.
    ///
    /// Behavior: the `outputField` of each JSONFieldProcessor: Java's null of the type
    /// is `null` when nulls are output and no field otherwise; a `char` is a string of
    /// one; a `float` or `double` is a number as `Float#toString` and `Double#toString`
    /// write it, and `NaN` and the infinities are strings (Jackson's
    /// `WRITE_NAN_AS_STRINGS`); `BigDecimal` is its `toString`, scale kept
    fn json_values(&self, array: &dyn Array, field: Field) -> Result<Vec<Option<Out>>, KafkaError> {
        fn value(json: String, class: &'static str) -> Out {
            Out::Value { json, class }
        }
        fn text(value: &str) -> Out {
            let mut json = String::new();
            write_quoted(value, &mut json);
            Out::Value {
                json,
                class: "TextNode",
            }
        }
        fn number<T: ToString>(class: &'static str) -> impl Fn(T) -> Out {
            move |number| value(number.to_string(), class)
        }
        fn each<T>(
            values: impl Iterator<Item = Option<T>>,
            write: impl Fn(T) -> Out,
        ) -> Vec<Option<Out>> {
            values.map(|cell| Some(cell.map(&write)?)).collect()
        }
        let encoding = self.encoding;
        let mut values = match field {
            Field::Boolean => each(array.as_boolean().iter(), |b| {
                value(b.to_string(), "BooleanNode")
            }),
            Field::Byte => each(array.as_primitive::<Int8Type>().iter(), number("IntNode")),
            Field::Short => each(
                array.as_primitive::<Int16Type>().iter(),
                number("ShortNode"),
            ),
            Field::Int => each(array.as_primitive::<Int32Type>().iter(), number("IntNode")),
            Field::Long => each(array.as_primitive::<Int64Type>().iter(), number("LongNode")),
            Field::Float => each(array.as_primitive::<Float32Type>().iter(), |f| {
                floating(float_text(f), f.is_finite(), "FloatNode")
            }),
            Field::Double => each(array.as_primitive::<Float64Type>().iter(), |d| {
                floating(double_text(d), d.is_finite(), "DoubleNode")
            }),
            Field::Char => {
                let mut values = Vec::with_capacity(array.len());
                for unit in array.as_primitive::<UInt16Type>().iter() {
                    values.push(match unit {
                        None => None,
                        Some(unit) => Some(text(&char_text(unit, encoding)?)),
                    });
                }
                values
            }
            Field::String | Field::ZoneId => each(array.as_string::<i32>().iter(), text),
            Field::Instant => each(
                array.as_primitive::<TimestampNanosecondType>().iter(),
                |nanos| text(&instant_text(nanos)),
            ),
            Field::LocalDate => each(array.as_primitive::<Date32Type>().iter(), |days| {
                text(&local_date_text(i64::from(days)))
            }),
            Field::LocalTime => each(
                array.as_primitive::<Time64NanosecondType>().iter(),
                |nanos| text(&local_time_text(nanos)),
            ),
            Field::BigDecimal => each(array.as_binary::<i32>().iter(), |cell| {
                let (scale, unscaled) =
                    big_decimal_parts(cell).expect("a BigDecimal cell has a scale");
                value(big_decimal_text(scale, unscaled), "DecimalNode")
            }),
            Field::BigInteger => each(array.as_binary::<i32>().iter(), |cell| {
                value(big_integer_text(cell), "BigIntegerNode")
            }),
        };
        if self.output_nulls {
            for cell in &mut values {
                cell.get_or_insert_with(|| value("null".to_owned(), "NullNode"));
            }
        }
        Ok(values)
    }
}

fn floating(text: String, finite: bool, class: &'static str) -> Out {
    let json = if finite { text } else { format!("\"{text}\"") };
    Out::Value { json, class }
}

/// A `char` as a string of one. Half of a surrogate pair cannot be a Rust string: Java
/// writes it into the JSON text as it is, and its UTF-8 encoder then makes it `?`.
///
/// Behavior: java.lang.String#valueOf(char) and java.lang.String#getBytes(Charset)
fn char_text(unit: u16, encoding: Encoding) -> Result<String, KafkaError> {
    match char::from_u32(u32::from(unit)) {
        Some(c) => Ok(c.to_string()),
        None if encoding == Encoding::Utf8 => Ok("?".to_owned()),
        None => Err(not_ported(
            "java.lang.String#getBytes",
            &format!("half of a surrogate pair in a char field, in the encoding {encoding:?}"),
        )),
    }
}

/// Behavior: java.time.Instant#toString, `DateTimeFormatter.ISO_INSTANT`: UTC, the
/// seconds always, and the fraction in zero, three, six or nine digits
fn instant_text(nanos: i64) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    let date = local_date_text(seconds.div_euclid(86_400));
    let time = seconds.rem_euclid(86_400);
    let fraction = if fraction == 0 {
        String::new()
    } else if fraction % 1_000_000 == 0 {
        format!(".{:03}", fraction / 1_000_000)
    } else if fraction % 1_000 == 0 {
        format!(".{:06}", fraction / 1_000)
    } else {
        format!(".{fraction:09}")
    };
    let (hour, minute, second) = (time / 3_600, time / 60 % 60, time % 60);
    format!("{date}T{hour:02}:{minute:02}:{second:02}{fraction}Z")
}

/// Lays out shortest digits as `Double#toString` does: plain from 10^-3 up to 10^7 with at
/// least one digit after the point, else one digit, a point, the rest or `0`, `E` and the
/// exponent.
fn java_layout(digits: &str, exponent: i32) -> String {
    if (-3..7).contains(&exponent) {
        if exponent < 0 {
            let zeros = "0".repeat((-exponent - 1) as usize);
            return format!("0.{zeros}{digits}");
        }
        let whole = exponent as usize + 1;
        if digits.len() <= whole {
            let zeros = "0".repeat(whole - digits.len());
            format!("{digits}{zeros}.0")
        } else {
            format!("{}.{}", &digits[..whole], &digits[whole..])
        }
    } else {
        let rest = if digits.len() > 1 { &digits[1..] } else { "0" };
        format!("{}.{rest}E{exponent}", &digits[..1])
    }
}

fn digits_and_exponent(scientific: &str) -> (String, i32) {
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((scientific, "0"));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    (digits.to_owned(), exponent.parse().unwrap_or(0))
}

macro_rules! java_to_string {
    ($name:ident, $native:ty) => {
        /// Behavior: `java.lang.Double#toString(double)` and `java.lang.Float#toString(float)`
        /// as specified since JDK 19: the shortest decimal that rounds to the value, and
        /// among those the closest, with no fewer than two digits (`1.4E-45`).
        fn $name(value: $native) -> String {
            if value.is_nan() {
                return "NaN".to_owned();
            }
            let sign = if value.is_sign_negative() { "-" } else { "" };
            let magnitude = value.abs();
            if magnitude.is_infinite() {
                return format!("{sign}Infinity");
            }
            if magnitude == 0.0 {
                return format!("{sign}0.0");
            }
            let (mut digits, mut exponent) = digits_and_exponent(&format!("{magnitude:e}"));
            if digits.len() == 1 {
                (digits, exponent) = digits_and_exponent(&format!("{magnitude:.1e}"));
            }
            format!("{sign}{}", java_layout(&digits, exponent))
        }
    };
}

java_to_string!(double_text, f64);
java_to_string!(float_text, f32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_are_written_as_java_writes_them() {
        let doubles = [
            (1e7, "1.0E7"),
            (1e-4, "1.0E-4"),
            (1e21, "1.0E21"),
            (123456789.0, "1.23456789E8"),
            (4.9e-324, "4.9E-324"),
            (0.001, "0.001"),
            (-0.0, "-0.0"),
            (9999999.0, "9999999.0"),
            (0.00099, "9.9E-4"),
        ];
        for (value, text) in doubles {
            assert_eq!(double_text(value), text);
        }
        let floats = [
            (0.1f32, "0.1"),
            (1e10, "1.0E10"),
            (1.0, "1.0"),
            (3.402_823_5e38, "3.4028235E38"),
            (1e-5, "1.0E-5"),
            (100.0, "100.0"),
            (f32::from_bits(1), "1.4E-45"),
            (1.0e-3, "0.001"),
        ];
        for (value, text) in floats {
            assert_eq!(float_text(value), text);
        }
    }

    #[test]
    fn instants_and_splits_are_java_s() {
        assert_eq!(instant_text(1_704_164_645_123_000_000), "2024-01-02T03:04:05.123Z");
        assert_eq!(instant_text(-999_999_999), "1969-12-31T23:59:59.000000001Z");
        assert_eq!(instant_text(-315_619_199_999_999_000), "1960-01-01T00:00:00.000001Z");
        let split = |text: &str, delimiter: &str| java_split(text, delimiter).unwrap();
        assert_eq!(split("_a__b_", "_"), ["", "a", "", "b"]);
        assert_eq!(split("a__b", "__"), ["a", "b"]);
        assert_eq!(split("ab", "__"), ["ab"]);
        assert_eq!(split("", "_"), [""]);
        assert!(split("__", "__").is_empty());
        assert_eq!(split("a\\.b", "\\."), ["a\\", "b"]);
        for regular in [".", "a|b", "", "\\d"] {
            let refused = java_split("a.b", regular).unwrap_err();
            assert!(matches!(refused, KafkaError::NotPorted { .. }), "{regular}");
        }
    }

    #[test]
    fn text_is_encoded_as_java_encodes_it() {
        let config = |name: &str, value: &str| vec![(name.to_owned(), value.to_owned())];
        let utf16 = Encoding::of(&config("value.serializer.encoding", "utf-16"), false);
        assert_eq!(utf16.unwrap().encode("ü"), [0xFE, 0xFF, 0x00, 0xFC]);
        let latin1 = Encoding::of(&config("serializer.encoding", "ISO-8859-1"), true);
        assert_eq!(latin1.unwrap().encode("é€"), [0xE9, b'?']);
        let other_side = Encoding::of(&config("key.serializer.encoding", "UTF-16LE"), false);
        assert_eq!(other_side.unwrap(), Encoding::Utf8);
        let refused = Encoding::of(&config("serializer.encoding", "windows-1252"), true);
        assert!(matches!(refused, Err(KafkaError::NotPorted { .. })));
        assert_eq!(Encoding::Utf16Le.encode("é"), [0xE9, 0x00]);
    }

    #[test]
    fn simple_columns_of_other_types_are_refused_as_java_refuses_them() {
        let definition = TableDefinition::new(vec![
            oxhaven_contracts::schema::ColumnDefinition::new("B", ColumnType::Boolean),
            oxhaven_contracts::schema::ColumnDefinition::new(
                "IA",
                ColumnType::array(ColumnType::Int),
            ),
        ])
        .unwrap();
        let text = |column: &str| match Serializer::simple(&definition, column, Encoding::Utf8) {
            Ok(_) => panic!("accepted"),
            Err(error) => error.to_string(),
        };
        assert_eq!(
            text("B"),
            "io.deephaven.UncheckedDeephavenException: Serializer not found for column B, \
             type BoxedType(io.deephaven.qst.type.BooleanType)"
        );
        assert!(text("IA").ends_with(
            "type NativeArrayType{clazz=class [I, componentType=io.deephaven.qst.type.IntType}"
        ));
        assert!(text("Nope").contains("because \"cd\" is null"));
    }
}
