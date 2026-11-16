//! Avro when producing: `KafkaTools.Produce#avroSpec`.
//!
//! The spec's schema is fetched from the registry, or made from the table's columns and
//! registered (`publish_schema`). Each row becomes a `GenericRecord` as Java's
//! `GenericRecordKeyOrValueSerializer` fills one, with each column's cells boxed as Java
//! boxes them, and is written as Confluent's `KafkaAvroSerializer` writes it: magic byte
//! 0, the id the registry gives the schema under the subject `<topic>-key` or
//! `<topic>-value`, then the record in Avro's binary encoding, through Avro's
//! `GenericDatumWriter`, whose casts and failures are reproduced.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampNanosecondType,
    UInt16Type,
};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;

use crate::avro_text::{Kind, Resolved, Text, Ty};
use crate::json;
use crate::registry::Registry;
use crate::serialize::{double_text, float_text};
use crate::{avro, not_ported, KafkaError};

/// The arguments of `KafkaTools.Produce#avroSpec(String, ...)`, the one Python reaches.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct AvroProduceSpec {
    /// The subject of the schema in the registry; with `publish_schema`, also the name
    /// of the record made from the table.
    pub subject: String,
    /// `latest` or a version number, as text.
    pub version: Option<String>,
    /// The column of each field named; a field not named is the column of its name.
    pub mapping: Option<Vec<(String, String)>>,
    /// A field of the schema that is given the time the record is made.
    pub timestamp_field: Option<String>,
    pub include_only: Option<Vec<String>>,
    pub exclude: Option<Vec<String>>,
    /// Makes the schema from the table's columns and registers it under `subject`.
    pub publish_schema: bool,
    /// The namespace of that schema.
    pub namespace: Option<String>,
    /// `<column>.precision` and `<column>.scale` of `BigDecimal` columns.
    pub column_properties: Vec<(String, String)>,
}

const UNCHECKED: &str = "io.deephaven.UncheckedDeephavenException";

impl AvroProduceSpec {
    fn excluded(&self, name: &str) -> bool {
        let out = self.exclude.as_ref().is_some_and(|e| e.iter().any(|x| x == name));
        let not_in = self
            .include_only
            .as_ref()
            .is_some_and(|i| !i.iter().any(|x| x == name));
        out || not_in
    }

    /// The schema, fetched or made and registered, and the columns of its fields.
    ///
    /// Behavior: io.deephaven.kafka.AvroImpl.AvroProduce#getColumnNames and #ensureSchema
    pub(crate) fn columns(
        &self,
        definition: &TableDefinition,
        refreshing: bool,
        registry: &Registry,
    ) -> Result<(Vec<String>, Arc<Text>), KafkaError> {
        let text = if self.publish_schema {
            let text = self.schema_of_table(definition, refreshing)?;
            registry
                .register(&self.subject, &text)
                .map_err(|failure| KafkaError::Invalid(format!("{UNCHECKED}: {}", failure.0)))?;
            text
        } else {
            registry.subject_schema_text(&self.subject, self.version.as_deref())?
        };
        avro::parse(&text).map_err(KafkaError::Invalid)?;
        let text = Arc::new(Text::read(&text).map_err(KafkaError::Invalid)?);
        let Some(fields) = record_fields(&text) else {
            // Behavior: org.apache.avro.Schema#getFields of a schema that is not a record
            return Err(KafkaError::Invalid(format!(
                "org.apache.avro.AvroRuntimeException: Not a record: {}",
                text.to_java()
            )));
        };
        if let Some(stamp) = &self.timestamp_field {
            if !fields.iter().any(|f| &f.name == stamp) {
                return Err(KafkaError::Invalid(format!(
                    "java.lang.IllegalArgumentException: timestampFieldName={stamp} is not a \
                     field name in the provided schema."
                )));
            }
        }
        let mut columns = Vec::new();
        for field in fields {
            if Some(&field.name) == self.timestamp_field.as_ref() {
                continue;
            }
            let mapped = self.mapping.as_ref().and_then(|mapping| {
                mapping.iter().rev().find(|(f, _)| *f == field.name).map(|(_, c)| c.clone())
            });
            let column = mapped.unwrap_or_else(|| field.name.clone());
            if !self.excluded(&column) {
                columns.push(column);
            }
        }
        Ok((columns, text))
    }

    /// The schema Java makes of the columns: a record of the subject's name, with a field
    /// per column that is a union of the column's type and null.
    ///
    /// Behavior: io.deephaven.kafka.AvroImpl#columnDefinitionsToAvroSchema and
    /// #addFieldForColDef, as `SchemaBuilder` writes them (probes a7, a7b)
    fn schema_of_table(
        &self,
        definition: &TableDefinition,
        _refreshing: bool,
    ) -> Result<String, KafkaError> {
        let mut out = String::from("{\"type\":\"record\",\"name\":");
        json::write_quoted(&self.subject, &mut out);
        if let Some(space) = &self.namespace {
            out.push_str(",\"namespace\":");
            json::write_quoted(space, &mut out);
        }
        out.push_str(",\"fields\":[");
        let mut first = true;
        for column in definition.columns() {
            if self.excluded(&column.name) {
                continue;
            }
            let ty = match &column.column_type {
                ColumnType::Byte => r#"{"type":"int","dhType":"byte"}"#.to_owned(),
                ColumnType::Char => r#"{"type":"int","dhType":"char"}"#.to_owned(),
                ColumnType::Short => r#"{"type":"int","dhType":"short"}"#.to_owned(),
                ColumnType::Int => "\"int\"".to_owned(),
                ColumnType::Long => "\"long\"".to_owned(),
                ColumnType::Float => "\"float\"".to_owned(),
                ColumnType::Double => "\"double\"".to_owned(),
                ColumnType::String => "\"string\"".to_owned(),
                ColumnType::Instant => {
                    r#"{"type":"long","logicalType":"timestamp-micros"}"#.to_owned()
                }
                ColumnType::Boolean => r#"{"type":"bytes","dhType":"java.lang.Boolean"}"#.to_owned(),
                ColumnType::BigDecimal => {
                    return Err(not_ported(
                        "io.deephaven.kafka.AvroImpl#addFieldForColDef",
                        &format!(
                            "a BigDecimal column {} in a schema made from the table",
                            column.name
                        ),
                    ))
                }
                other => {
                    return Err(not_ported(
                        "io.deephaven.kafka.AvroImpl#addFieldForColDef",
                        &format!("a column {} of type {other:?} in a schema made from the table", column.name),
                    ))
                }
            };
            if !first {
                out.push(',');
            }
            first = false;
            out.push_str("{\"name\":");
            json::write_quoted(&column.name, &mut out);
            out.push_str(",\"type\":[");
            out.push_str(&ty);
            out.push_str(",\"null\"]}");
        }
        out.push_str("]}");
        Ok(out)
    }
}

fn record_fields(text: &Text) -> Option<&[crate::avro_text::Field]> {
    match text.resolve(&text.root) {
        Resolved::Named(named) => match &named.kind {
            Kind::Record { fields, .. } => Some(fields),
            _ => None,
        },
        Resolved::Plain(_) => None,
    }
}

/// A time unit of a `long` with a logical type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Unit {
    Millis,
    Micros,
}

/// How a column's cells become the values of a field.
#[derive(Clone, Debug)]
enum Source {
    /// The cells boxed: `TypeUtils#box`, and the object of other columns.
    Column { index: usize, column_type: ColumnType },
    /// An `Instant` column as a number of units since the epoch.
    Instant { index: usize, unit: Unit },
    /// The time of the chunk, for `timestamp_field`.
    Now(Unit),
}

/// A value as Java holds it in a `GenericRecord`: boxed, or null.
#[derive(Clone, PartialEq, Debug)]
enum Datum {
    Null,
    Boolean(bool),
    Byte(i8),
    Short(i16),
    Char(u16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Str(String),
}

impl Datum {
    fn class(&self) -> &'static str {
        match self {
            Datum::Null => "null",
            Datum::Boolean(_) => "java.lang.Boolean",
            Datum::Byte(_) => "java.lang.Byte",
            Datum::Short(_) => "java.lang.Short",
            Datum::Char(_) => "java.lang.Character",
            Datum::Int(_) => "java.lang.Integer",
            Datum::Long(_) => "java.lang.Long",
            Datum::Float(_) => "java.lang.Float",
            Datum::Double(_) => "java.lang.Double",
            Datum::Str(_) => "java.lang.String",
        }
    }

    /// Behavior: the `toString` of each boxed type
    fn text(&self) -> String {
        match self {
            Datum::Null => "null".to_owned(),
            Datum::Boolean(b) => b.to_string(),
            Datum::Byte(v) => v.to_string(),
            Datum::Short(v) => v.to_string(),
            Datum::Char(unit) => char::from_u32(u32::from(*unit)).map_or("?".to_owned(), String::from),
            Datum::Int(v) => v.to_string(),
            Datum::Long(v) => v.to_string(),
            Datum::Float(v) => float_text(*v),
            Datum::Double(v) => double_text(*v),
            Datum::Str(s) => s.clone(),
        }
    }

    /// `java.lang.Number#doubleValue` and its siblings, by cast as Java casts.
    fn number(&self) -> Option<Number> {
        Some(match self {
            Datum::Byte(v) => Number::Integral(i64::from(*v)),
            Datum::Short(v) => Number::Integral(i64::from(*v)),
            Datum::Int(v) => Number::Integral(i64::from(*v)),
            Datum::Long(v) => Number::Integral(*v),
            Datum::Float(v) => Number::Float(*v),
            Datum::Double(v) => Number::Double(*v),
            _ => return None,
        })
    }

    /// The type name `GenericData#getSchemaName` gives a datum, by which a union's
    /// branch is chosen.
    ///
    /// Behavior: org.apache.avro.generic.GenericData#getSchemaName
    fn schema_name(&self) -> Option<&'static str> {
        Some(match self {
            Datum::Null => "null",
            Datum::Boolean(_) => "boolean",
            Datum::Str(_) => "string",
            Datum::Int(_) => "int",
            Datum::Long(_) => "long",
            Datum::Float(_) => "float",
            Datum::Double(_) => "double",
            Datum::Byte(_) | Datum::Short(_) | Datum::Char(_) => return None,
        })
    }
}

#[derive(Clone, Copy)]
enum Number {
    Integral(i64),
    Float(f32),
    Double(f64),
}

impl Number {
    fn int_value(self) -> i32 {
        match self {
            Number::Integral(v) => v as i32,
            Number::Float(v) => v as i32,
            Number::Double(v) => v as i32,
        }
    }
    fn long_value(self) -> i64 {
        match self {
            Number::Integral(v) => v,
            Number::Float(v) => v as i64,
            Number::Double(v) => v as i64,
        }
    }
    fn float_value(self) -> f32 {
        match self {
            Number::Integral(v) => v as f32,
            Number::Float(v) => v,
            Number::Double(v) => v as f32,
        }
    }
    fn double_value(self) -> f64 {
        match self {
            Number::Integral(v) => v as f64,
            Number::Float(v) => f64::from(v),
            Number::Double(v) => v,
        }
    }
}

/// Why a value could not be written, before the field it is in is known.
enum Fail {
    /// A null where the type has none: the type's description and the JVM's message.
    Null { describe: String, helpful: &'static str },
    /// A value of the wrong class: the description and the class it was cast to.
    Cast { datum: Datum, describe: String, target: &'static str },
    NotEnum { datum: Datum, name: String },
    Unknown(Datum),
    NotInUnion { union: String, datum: Datum },
}

const SERIALIZATION: &str =
    "org.apache.kafka.common.errors.SerializationException: Error serializing Avro message";

impl Fail {
    /// Behavior: org.apache.avro.generic.GenericDatumWriter#writeField, which names the
    /// field, and the JVM's own messages
    fn text(self, record: &str, field: &str) -> String {
        let at = format!("{record}.{field}");
        let cause = match self {
            Fail::Null { describe, helpful } => format!(
                "java.lang.NullPointerException: null value for (non-nullable) {describe} at \
                 {at} (caused by java.lang.NullPointerException: {helpful})"
            ),
            Fail::Cast { datum, describe, target } => {
                let class = datum.class();
                let modules = if target.starts_with("org.apache.avro") {
                    format!(
                        "{class} is in module java.base of loader 'bootstrap'; {target} is in \
                         unnamed module of loader 'app'"
                    )
                } else {
                    format!("{class} and {target} are in module java.base of loader 'bootstrap'")
                };
                format!(
                    "java.lang.ClassCastException: value {} (a {class}) cannot be cast to \
                     expected type {describe} at {at} (caused by java.lang.ClassCastException: \
                     class {class} cannot be cast to class {target} ({modules}))",
                    datum.text()
                )
            }
            Fail::NotEnum { datum, name } => {
                let value = match datum {
                    Datum::Null => "null".to_owned(),
                    other => format!("{} (a {})", other.text(), other.class()),
                };
                format!(
                    "org.apache.avro.AvroTypeException: value {value} is not a {name} at {at} \
                     (caused by org.apache.avro.AvroTypeException: value {value} is not a {name})"
                )
            }
            Fail::Unknown(datum) => format!(
                "org.apache.avro.AvroRuntimeException: Unknown datum type {}: {}",
                datum.class(),
                datum.text()
            ),
            Fail::NotInUnion { union, datum } => format!(
                "org.apache.avro.UnresolvedUnionException: Not in union {union}: {} \
                 (field={field})",
                datum.text()
            ),
        };
        format!("{SERIALIZATION} (caused by {cause})")
    }
}

fn varint(value: i64, out: &mut Vec<u8>) {
    let mut n = ((value << 1) ^ (value >> 63)) as u64;
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

/// Writes `datum` as `ty`.
///
/// Behavior: org.apache.avro.generic.GenericDatumWriter#write and #writeWithoutConversion
/// with Avro's `BinaryEncoder`; the logical types have no conversions in Confluent's
/// serializer, so they write their underlying type (probe matrix m_*)
fn write(text: &Text, ty: &Ty, datum: &Datum, out: &mut Vec<u8>) -> Result<(), Fail> {
    let describe = || text.describe(ty);
    let null = |helpful| Fail::Null {
        describe: describe(),
        helpful,
    };
    let cast = |target| Fail::Cast {
        datum: datum.clone(),
        describe: describe(),
        target,
    };
    match text.resolve(ty) {
        Resolved::Plain(Ty::Union(branches)) => {
            let Some(name) = datum.schema_name() else {
                return Err(Fail::Unknown(datum.clone()));
            };
            let found = branches.iter().position(|branch| match branch {
                Ty::Primitive { name: n, .. } => n == name,
                _ => false,
            });
            let Some(index) = found else {
                return Err(Fail::NotInUnion {
                    union: text.ty_to_java(ty),
                    datum: datum.clone(),
                });
            };
            varint(index as i64, out);
            write(text, &branches[index], datum, out)
        }
        Resolved::Plain(Ty::Primitive { name, .. }) => {
            let number = || match datum {
                Datum::Null => None,
                other => other.number().map(Some).unwrap_or(None),
            };
            match name.as_str() {
                "null" => Ok(()),
                "int" | "long" | "float" | "double" => {
                    if *datum == Datum::Null {
                        return Err(null(match name.as_str() {
                            "int" => "Cannot invoke \"java.lang.Number.intValue()\" because \"datum\" is null",
                            "long" => "Cannot invoke \"java.lang.Number.longValue()\" because \"datum\" is null",
                            "float" => "Cannot invoke \"java.lang.Number.floatValue()\" because \"datum\" is null",
                            _ => "Cannot invoke \"java.lang.Number.doubleValue()\" because \"datum\" is null",
                        }));
                    }
                    let Some(n) = number() else {
                        return Err(cast("java.lang.Number"));
                    };
                    match name.as_str() {
                        "int" => varint(i64::from(n.int_value()), out),
                        "long" => varint(n.long_value(), out),
                        "float" => out.extend(n.float_value().to_bits().to_le_bytes()),
                        _ => out.extend(n.double_value().to_bits().to_le_bytes()),
                    }
                    Ok(())
                }
                "boolean" => match datum {
                    Datum::Boolean(b) => {
                        out.push(u8::from(*b));
                        Ok(())
                    }
                    Datum::Null => Err(null(
                        "Cannot invoke \"java.lang.Boolean.booleanValue()\" because \"datum\" is null",
                    )),
                    _ => Err(cast("java.lang.Boolean")),
                },
                "string" => match datum {
                    Datum::Str(s) => {
                        varint(s.len() as i64, out);
                        out.extend(s.as_bytes());
                        Ok(())
                    }
                    Datum::Null => Err(null(
                        "Cannot invoke \"java.lang.CharSequence.toString()\" because \"charSequence\" is null",
                    )),
                    _ => Err(cast("java.lang.CharSequence")),
                },
                // "bytes": no column becomes a ByteBuffer here.
                _ => match datum {
                    Datum::Null => Err(null(
                        "Cannot invoke \"java.nio.ByteBuffer.limit()\" because \"bytes\" is null",
                    )),
                    _ => Err(cast("java.nio.ByteBuffer")),
                },
            }
        }
        Resolved::Plain(Ty::Array { .. }) => Err(match datum {
            Datum::Null => null("Cannot invoke \"java.util.Collection.size()\" because \"array\" is null"),
            _ => cast("java.util.Collection"),
        }),
        Resolved::Plain(Ty::Map { .. }) => Err(match datum {
            Datum::Null => null("Cannot invoke \"java.util.Map.size()\" because \"map\" is null"),
            _ => cast("java.util.Map"),
        }),
        Resolved::Plain(Ty::Named(_)) => unreachable!("a name is resolved"),
        Resolved::Named(named) => Err(match &named.kind {
            Kind::Enum { .. } => Fail::NotEnum {
                datum: datum.clone(),
                name: named.name.clone(),
            },
            Kind::Fixed { .. } => match datum {
                Datum::Null => null(
                    "Cannot invoke \"org.apache.avro.generic.GenericFixed.bytes()\" because \"datum\" is null",
                ),
                _ => cast("org.apache.avro.generic.GenericFixed"),
            },
            Kind::Record { .. } => match datum {
                Datum::Null => null(
                    "Cannot invoke \"org.apache.avro.generic.IndexedRecord.get(int)\" because \"record\" is null",
                ),
                _ => cast("org.apache.avro.generic.IndexedRecord"),
            },
        }),
    }
}

/// The logical type of a field, through a union of null and one type.
///
/// Behavior: io.deephaven.kafka.KafkaSchemaUtils#getEffectiveSchema and
/// GenericRecordKeyOrValueSerializer#getLogicalType
fn logical_type(text: &Text, ty: &Ty) -> Option<String> {
    let null = |t: &Ty| matches!(t, Ty::Primitive { name, .. } if name == "null");
    let effective = match ty {
        Ty::Union(branches) if branches.len() == 2 && branches.iter().any(null) => {
            branches.iter().find(|b| !null(b)).unwrap_or(ty)
        }
        other => other,
    };
    let props = match effective {
        Ty::Primitive { props, .. } | Ty::Array { props, .. } | Ty::Map { props, .. } => props,
        Ty::Named(name) => return text.named[name].logical_type(),
        Ty::Union(_) => return None,
    };
    crate::avro_text::logical_type_of(props)
}

/// Writes the rows of a table as Avro records.
///
/// Behavior: io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer
pub(crate) struct AvroSerializer {
    text: Arc<Text>,
    record: String,
    /// The value of each field, in the schema's order.
    sources: Vec<Source>,
    /// The schema as Java writes it, which is what is registered.
    pub(crate) registered: String,
}

impl AvroSerializer {
    /// Behavior: GenericRecordKeyOrValueSerializer#GenericRecordKeyOrValueSerializer,
    /// #checkTimestampFieldAndGetUnit, #makeFieldProcessor, #getFieldProcessorForType
    pub(crate) fn new(
        text: Arc<Text>,
        definition: &TableDefinition,
        columns: &[String],
        timestamp_field: Option<&str>,
    ) -> Result<Self, KafkaError> {
        let invalid = |text: String| KafkaError::Invalid(format!("java.lang.IllegalArgumentException: {text}"));
        let (record, fields) = match text.resolve(&text.root) {
            Resolved::Named(named) => match &named.kind {
                Kind::Record { fields, .. } => (named.name.clone(), fields.clone()),
                _ => return Err(invalid("The schema is not a toplevel record definition.".to_owned())),
            },
            Resolved::Plain(Ty::Union(_)) => {
                return Err(KafkaError::Invalid(
                    "java.lang.UnsupportedOperationException: Schemas defined as a union of \
                     records are not supported"
                        .to_owned(),
                ))
            }
            Resolved::Plain(_) => {
                return Err(invalid("The schema is not a toplevel record definition.".to_owned()))
            }
        };
        let timestamp_field = timestamp_field.filter(|name| !name.is_empty());
        let mut stamp = None;
        if let Some(name) = timestamp_field {
            let Some(field) = fields.iter().find(|f| f.name == name) else {
                return Err(invalid(format!("Field of name timestampFieldName='{name}' not found.")));
            };
            let unit = match &field.ty {
                Ty::Primitive { name: word, .. } if word == "long" => {
                    match logical_type(&text, &field.ty).as_deref() {
                        Some("timestamp-micros") => Unit::Micros,
                        Some("timestamp-millis") => Unit::Millis,
                        other => {
                            return Err(KafkaError::Invalid(format!(
                                "java.lang.IllegalStateException: Field of name \
                                 timestampFieldName='{name}' has wrong logical type {}",
                                other.map_or("null".to_owned(), |o| format!("LogicalType{{name={o}}}"))
                            )))
                        }
                    }
                }
                other => {
                    return Err(invalid(format!(
                        "Field of name timestampFieldName='{name}' has wrong type {}",
                        type_word(&text, other)
                    )))
                }
            };
            stamp = Some(unit);
        }
        let mut sources = Vec::with_capacity(fields.len());
        let mut next = 0;
        for field in &fields {
            if Some(field.name.as_str()) == timestamp_field {
                sources.push(Source::Now(stamp.expect("checked above")));
                continue;
            }
            // Java pairs fields with the column names by position, also when the names
            // left some fields out (probes a6, a6i).
            let Some(column) = columns.get(next) else {
                return Err(KafkaError::Invalid(format!(
                    "java.lang.ArrayIndexOutOfBoundsException: Index {next} out of bounds for \
                     length {}",
                    columns.len()
                )));
            };
            next += 1;
            let Some(index) = definition.index_of(column) else {
                let names: Vec<&str> = definition.columns().iter().map(|c| c.name.as_str()).collect();
                return Err(KafkaError::Invalid(format!(
                    "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names \
                     [{column}], available column names are [{}]",
                    names.join(", ")
                )));
            };
            let column_type = definition.columns()[index].column_type.clone();
            sources.push(match column_type {
                ColumnType::Instant => {
                    let unit = match logical_type(&text, &field.ty).as_deref() {
                        Some("timestamp-millis") => Unit::Millis,
                        Some("timestamp-micros") => Unit::Micros,
                        None => {
                            return Err(invalid(format!(
                                "field {} for column {column} has no logical type.",
                                field.name
                            )))
                        }
                        Some(other) => {
                            return Err(invalid(format!(
                                "field {} for column {column} has unrecognized logical type {other}",
                                field.name
                            )))
                        }
                    };
                    Source::Instant { index, unit }
                }
                ColumnType::Byte
                | ColumnType::Short
                | ColumnType::Char
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::Boolean
                | ColumnType::String => Source::Column { index, column_type },
                ColumnType::BigDecimal => {
                    return Err(not_ported(
                        "io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer#makeBigDecimalFieldProcessor",
                        &format!("an Avro field {} of a BigDecimal column {column}", field.name),
                    ))
                }
                other => {
                    return Err(not_ported(
                        "io.deephaven.kafka.publish.GenericRecordKeyOrValueSerializer#makeObjectFieldProcessor",
                        &format!("an Avro field {} of a column {column} of type {other:?}", field.name),
                    ))
                }
            });
        }
        let registered = text.to_java();
        Ok(Self {
            text,
            record,
            sources,
            registered,
        })
    }

    /// The Avro encoding of each row, or why it cannot be written.
    ///
    /// Behavior: GenericRecordKeyOrValueSerializer#handleChunk, then
    /// `KafkaAvroSerializer#serialize` of each record (without its header)
    pub(crate) fn serialize(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        prev: bool,
        now_nanos: i64,
    ) -> Result<Vec<Result<Vec<u8>, KafkaError>>, KafkaError> {
        let count = rows.size() as usize;
        let mut columns: Vec<Vec<Datum>> = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            columns.push(match source {
                Source::Now(unit) => {
                    let value = match unit {
                        Unit::Micros => now_nanos.div_euclid(1_000),
                        Unit::Millis => now_nanos.div_euclid(1_000_000),
                    };
                    vec![Datum::Long(value); count]
                }
                Source::Column { index, column_type } => {
                    datums(&*crate::serialize::cells(table, *index, rows, prev)?, column_type)
                }
                Source::Instant { index, unit } => {
                    let array = crate::serialize::cells(table, *index, rows, prev)?;
                    array
                        .as_primitive::<TimestampNanosecondType>()
                        .iter()
                        .map(|cell| match cell {
                            None | Some(i64::MIN) => Datum::Null,
                            // Behavior: Instant#toEpochMilli (floor) and
                            // DateTimeUtils#epochMicros (truncation)
                            Some(nanos) => Datum::Long(match unit {
                                Unit::Millis => nanos.div_euclid(1_000_000),
                                Unit::Micros => nanos / 1_000,
                            }),
                        })
                        .collect()
                }
            });
        }
        let fields = record_fields(&self.text).expect("a record").to_vec();
        let mut out = Vec::with_capacity(count);
        for row in 0..count {
            let mut bytes = Vec::new();
            let mut outcome = Ok(());
            for (field, values) in fields.iter().zip(&columns) {
                if let Err(fail) = write(&self.text, &field.ty, &values[row], &mut bytes) {
                    outcome = Err(KafkaError::Record(fail.text(&self.record, &field.name)));
                    break;
                }
            }
            out.push(outcome.map(|()| bytes));
        }
        Ok(out)
    }
}

/// Behavior: org.apache.avro.Schema.Type#toString (upper case)
fn type_word(text: &Text, ty: &Ty) -> String {
    match text.resolve(ty) {
        Resolved::Plain(Ty::Primitive { name, .. }) => name.to_uppercase(),
        Resolved::Plain(Ty::Array { .. }) => "ARRAY".to_owned(),
        Resolved::Plain(Ty::Map { .. }) => "MAP".to_owned(),
        Resolved::Plain(_) => "UNION".to_owned(),
        Resolved::Named(named) => match named.kind {
            Kind::Record { .. } => "RECORD",
            Kind::Enum { .. } => "ENUM",
            Kind::Fixed { .. } => "FIXED",
        }
        .to_owned(),
    }
}

/// The cells of a column boxed as `TypeUtils#box` boxes them: Java's null of the type is
/// null.
///
/// Behavior: io.deephaven.util.type.TypeUtils#box
fn datums(array: &dyn Array, column_type: &ColumnType) -> Vec<Datum> {
    fn each<T: Copy + PartialEq>(
        values: impl Iterator<Item = Option<T>>,
        null: T,
        datum: impl Fn(T) -> Datum,
    ) -> Vec<Datum> {
        values
            .map(|cell| match cell {
                Some(value) if value != null => datum(value),
                _ => Datum::Null,
            })
            .collect()
    }
    match column_type {
        ColumnType::Byte => each(array.as_primitive::<Int8Type>().iter(), i8::MIN, Datum::Byte),
        ColumnType::Short => each(array.as_primitive::<Int16Type>().iter(), i16::MIN, Datum::Short),
        ColumnType::Char => each(array.as_primitive::<UInt16Type>().iter(), u16::MAX, Datum::Char),
        ColumnType::Int => each(array.as_primitive::<Int32Type>().iter(), i32::MIN, Datum::Int),
        ColumnType::Long => each(array.as_primitive::<Int64Type>().iter(), i64::MIN, Datum::Long),
        ColumnType::Float => each(array.as_primitive::<Float32Type>().iter(), -f32::MAX, Datum::Float),
        ColumnType::Double => each(array.as_primitive::<Float64Type>().iter(), -f64::MAX, Datum::Double),
        ColumnType::Boolean => array
            .as_boolean()
            .iter()
            .map(|cell| cell.map_or(Datum::Null, Datum::Boolean))
            .collect(),
        ColumnType::String => array
            .as_string::<i32>()
            .iter()
            .map(|cell| cell.map_or(Datum::Null, |s| Datum::Str(s.to_owned())))
            .collect(),
        _ => unreachable!("other columns are refused when the serializer is made"),
    }
}
