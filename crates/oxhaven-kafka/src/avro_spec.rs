//! The Avro spec: the columns of a reader's schema, the Confluent wire format of a record,
//! and the cells of each column.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, ListBuilder,
    StringArray, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder,
};
use arrow::datatypes::Field;
use oxhaven_column::java_values::big_decimal_cell;
use oxhaven_contracts::schema::ColumnType;

use crate::avro::{self, Decoder, Named, Schema, Ty, Value};
use crate::registry::Registry;
use crate::{not_ported, KafkaError};

/// How the fields of a schema are named as columns.
///
/// Behavior: `deephaven.stream.kafka.consumer._dict_to_j_func`,
/// io.deephaven.kafka.KafkaTools#DIRECT_MAPPING, io.deephaven.python.PythonTools
/// `functionFromMapWithIdentityDefaults` and `functionFromMapWithDefault`
#[derive(Clone, PartialEq, Debug)]
pub enum AvroMapping {
    /// Every field, named by its path with `__` for the separator `.`: no mapping, or an
    /// empty one.
    Direct,
    /// The pairs given, and every other field named by its path as it is (`a.b`). A
    /// field mapped to `None` is left out.
    Identity(Vec<(String, Option<String>)>),
    /// Only the fields of the pairs given (`mapped_only`).
    Only(Vec<(String, Option<String>)>),
}

impl AvroMapping {
    fn column(&self, path: &str) -> Option<String> {
        let find = |pairs: &[(String, Option<String>)]| {
            pairs.iter().find(|(field, _)| field == path).map(|(_, column)| column.clone())
        };
        match self {
            AvroMapping::Direct => Some(path.replace('.', "__")),
            AvroMapping::Identity(pairs) => find(pairs).unwrap_or_else(|| Some(path.to_owned())),
            AvroMapping::Only(pairs) => find(pairs).flatten(),
        }
    }
}

/// Where the reader's schema comes from.
#[derive(Clone, PartialEq, Debug)]
pub enum AvroSchema {
    /// The schema as JSON text.
    Text(String),
    /// A subject of the schema registry at a version: `latest` or a number.
    /// `None` is Java's null, which `Integer#parseInt` refuses.
    Registry {
        subject: String,
        version: Option<String>,
    },
}

/// What a column's cells are made of.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Cell {
    Boolean,
    Int,
    Long,
    Instant(i64),
    Float,
    Double,
    String,
    Decimal(i32),
    IntArray,
    LongArray,
    FloatArray,
    DoubleArray,
    StringArray,
    InstantArray(i64),
}

/// One column: the field positions from the record down to the field, through records
/// and unions with null.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct AvroColumn {
    path: Vec<usize>,
    cell: Cell,
}

/// A key's or a value's schema and its columns.
#[derive(Debug)]
pub(crate) struct AvroSide {
    pub(crate) schema: Schema,
    pub(crate) registry: Arc<Registry>,
}

fn invalid(text: &str) -> KafkaError {
    KafkaError::Invalid(text.to_owned())
}

fn named<'a>(schema: &'a Schema, ty: &Ty) -> Option<&'a Named> {
    match ty {
        Ty::Named(i) => Some(&schema.named[*i]),
        _ => None,
    }
}

/// The columns of `schema`, in Java's order, each with its name, type and cells.
///
/// Behavior: io.deephaven.kafka.AvroImpl#avroSchemaToColumnDefinitions and
/// #pushColumnTypesFromAvroField, io.deephaven.kafka.ingest.GenericRecordChunkAdapter#makeFieldCopier
pub(crate) fn columns(
    schema: &Schema,
    mapping: &AvroMapping,
) -> Result<Vec<(String, ColumnType, AvroColumn)>, KafkaError> {
    if matches!(schema.root, Ty::Union(_)) {
        return Err(invalid(
            "java.lang.UnsupportedOperationException: Schemas defined as a union of records \
             are not supported",
        ));
    }
    let Some(fields) = schema.record_fields(&schema.root) else {
        return Err(invalid(
            "java.lang.IllegalArgumentException: The schema is not a toplevel record definition.",
        ));
    };
    let mut out = Vec::new();
    for (position, field) in fields.iter().enumerate() {
        push_field(schema, mapping, "", &mut vec![position], &field.name, &field.ty, &mut out)?;
    }
    Ok(out)
}

fn push_field(
    schema: &Schema,
    mapping: &AvroMapping,
    prefix: &str,
    path: &mut Vec<usize>,
    name: &str,
    ty: &Ty,
    out: &mut Vec<(String, ColumnType, AvroColumn)>,
) -> Result<(), KafkaError> {
    let field_path = format!("{prefix}{name}");
    let Some(column) = mapping.column(&field_path) else {
        return Ok(());
    };
    let ty = avro::effective(ty, name).map_err(KafkaError::Invalid)?;
    let refuse = |java: &str, what: &str| {
        Err(not_ported(java, &format!("the Avro field {field_path}: {what}")))
    };
    let (column_type, cell) = match ty {
        Ty::Boolean => (ColumnType::Boolean, Cell::Boolean),
        Ty::Int => (ColumnType::Int, Cell::Int),
        Ty::Long(Some(stamp)) => (ColumnType::Instant, Cell::Instant(stamp.nanos())),
        Ty::Long(None) => (ColumnType::Long, Cell::Long),
        Ty::Float => (ColumnType::Float, Cell::Float),
        Ty::Double => (ColumnType::Double, Cell::Double),
        Ty::String => (ColumnType::String, Cell::String),
        Ty::Bytes(Some(scale)) => (ColumnType::BigDecimal, Cell::Decimal(*scale)),
        Ty::Union(_) => {
            return refuse(
                "io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier",
                "a union other than with null, which Java holds in a GenericRecord column",
            )
        }
        Ty::Array(element) => {
            let element = avro::effective(element, name).map_err(KafkaError::Invalid)?;
            match element {
                Ty::Int => (ColumnType::array(ColumnType::Int), Cell::IntArray),
                Ty::Long(Some(stamp)) => {
                    (ColumnType::array(ColumnType::Instant), Cell::InstantArray(stamp.nanos()))
                }
                Ty::Long(None) => (ColumnType::array(ColumnType::Long), Cell::LongArray),
                Ty::Float => (ColumnType::array(ColumnType::Float), Cell::FloatArray),
                Ty::Double => (ColumnType::array(ColumnType::Double), Cell::DoubleArray),
                Ty::String => (ColumnType::array(ColumnType::String), Cell::StringArray),
                Ty::Named(i) if matches!(schema.named[*i], Named::Enum { .. }) => {
                    (ColumnType::array(ColumnType::String), Cell::StringArray)
                }
                Ty::Boolean => {
                    return refuse(
                        "io.deephaven.kafka.ingest.GenericRecordArrayFieldCopier",
                        "an array of booleans, which Java declares Boolean[] and fills with \
                         Object[], so that its clients cannot read the column",
                    )
                }
                _ => {
                    return refuse(
                        "io.deephaven.kafka.ingest.GenericRecordArrayFieldCopier",
                        "an array of other than int, long, float, double, string or enum, \
                         which Java holds in an Object[] column",
                    )
                }
            }
        }
        Ty::Map(_) => {
            return refuse(
                "io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier",
                "a map, which Java holds in a java.util.Map column",
            )
        }
        Ty::Null => {
            return refuse(
                "io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier",
                "the null type, which Java holds in a GenericContainer column",
            )
        }
        Ty::Bytes(None) => {
            return refuse(
                "io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier",
                "bytes, which Java declares ByteVector and fills with ByteBuffer, so that its \
                 clients cannot read the column",
            )
        }
        Ty::Named(_) => match named(schema, ty).expect("named") {
            Named::Enum { .. } => (ColumnType::String, Cell::String),
            Named::Fixed { decimal: Some(_), .. } => {
                // Behavior: GenericRecordBigDecimalFieldCopier#copyField, which fails on
                // the GenericFixed it is given.
                return refuse(
                    "io.deephaven.kafka.ingest.GenericRecordBigDecimalFieldCopier#copyField",
                    "a decimal in a fixed, whose first non-null value fails the table in Java \
                     (Object of type org.apache.avro.generic.GenericData$Fixed not recognized \
                     for decimal type backing)",
                );
            }
            Named::Fixed { decimal: None, .. } => {
                return refuse(
                    "io.deephaven.kafka.ingest.GenericRecordObjectFieldCopier",
                    "a fixed, which Java declares ByteVector and fills with GenericFixed, so \
                     that its clients cannot read the column",
                )
            }
            Named::Record { fields, .. } => {
                let prefix = format!("{field_path}.");
                for (position, field) in fields.iter().enumerate() {
                    path.push(position);
                    let pushed =
                        push_field(schema, mapping, &prefix, path, &field.name, &field.ty, out);
                    path.pop();
                    pushed?;
                }
                return Ok(());
            }
        },
    };
    out.push((column, column_type, AvroColumn { path: path.clone(), cell }));
    Ok(())
}

/// Turns the key or value of each record into the reader's value: `None` for a record
/// without one, as Java's deserializer gives null for null.
///
/// Behavior: io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer#deserialize with
/// a reader's schema (`AvroImpl.AvroConsume.KafkaAvroDeserializerWithReaderSchema`): a
/// magic byte 0, a big-endian schema id, then the value in the writer's schema
pub(crate) fn decode(
    side: &AvroSide,
    subject: &str,
    bytes: Option<&[u8]>,
) -> Result<Option<Value>, String> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    const SERIALIZATION: &str = "org.apache.kafka.common.errors.SerializationException";
    if bytes.len() < 5 || bytes[0] != 0 {
        if bytes.first() == Some(&1) && bytes.len() >= 17 {
            return Err(format!(
                "not ported yet: io.confluent.kafka.serializers.schema.id.SchemaId: a record \
                 with a schema GUID (magic byte 1)"
            ));
        }
        return Err(format!("{SERIALIZATION}: Error deserializing schema ID"));
    }
    let id = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let schema_id = format!("SchemaId{{schemaType='AVRO', id={id}, guid='null', messageIndexes=null}}");
    let kind = if subject.ends_with("-key") { "key" } else { "value" };
    let writer = side.registry.writer_schema(id, subject).map_err(|failure| {
        format!(
            "{SERIALIZATION}: Error retrieving Avro {kind} schema for id {schema_id}: {}",
            failure.0
        )
    })?;
    let reader = &side.schema;
    avro::read(&mut Decoder::new(&bytes[5..]), &writer, &writer.root, reader, &reader.root)
        .map(Some)
        .map_err(|cause| {
            format!("{SERIALIZATION}: Error deserializing Avro message for id {schema_id}: {cause}")
        })
}

fn at<'v>(value: Option<&'v Value>, path: &[usize]) -> Option<&'v Value> {
    let mut current = value?;
    for position in path {
        match current {
            Value::Record(fields) => current = fields.get(*position)?,
            _ => return None,
        }
    }
    match current {
        Value::Null => None,
        other => Some(other),
    }
}

fn record_failure(text: &str) -> KafkaError {
    KafkaError::Record(text.to_owned())
}

/// The cells of `column` for the decoded records. A value equal to Java's null of the
/// column's type is a null cell, as Java stores it.
///
/// Behavior: the `copyField` of io.deephaven.kafka.ingest's `GenericRecordBooleanFieldCopier`,
/// `GenericRecordIntFieldCopier`, `GenericRecordLongFieldCopier`,
/// `GenericRecordLongFieldCopierWithMultiplier`, `GenericRecordFloatFieldCopier`,
/// `GenericRecordDoubleFieldCopier`, `GenericRecordStringFieldCopier`,
/// `GenericRecordBigDecimalFieldCopier`, `GenericRecordArrayFieldCopier` and
/// `GenericRecordInstantArrayFieldCopier`
pub(crate) fn cells(column: &AvroColumn, values: &[Option<Value>]) -> Result<ArrayRef, KafkaError> {
    let found = values.iter().map(|v| at(v.as_ref(), &column.path));
    let item = |element: ColumnType| Field::new("", element.arrow_type(), true);
    Ok(match column.cell {
        Cell::Boolean => Arc::new(BooleanArray::from(
            found.map(|v| match v { Some(Value::Boolean(b)) => Some(*b), _ => None }).collect::<Vec<_>>(),
        )),
        Cell::Int => Arc::new(Int32Array::from(
            found.map(|v| int(v).filter(|i| *i != i32::MIN)).collect::<Vec<_>>(),
        )),
        Cell::Long => Arc::new(Int64Array::from(
            found.map(|v| long(v).filter(|l| *l != i64::MIN)).collect::<Vec<_>>(),
        )),
        // Java multiplies unless the value is its null, and the product wraps.
        Cell::Instant(nanos) => Arc::new(
            TimestampNanosecondArray::from(
                found
                    .map(|v| {
                        long(v)
                            .filter(|l| *l != i64::MIN)
                            .map(|l| l.wrapping_mul(nanos))
                            .filter(|l| *l != i64::MIN)
                    })
                    .collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        ),
        Cell::Float => Arc::new(Float32Array::from(
            found.map(|v| match v { Some(Value::Float(f)) => Some(*f), _ => None }.filter(|f| *f != -f32::MAX)).collect::<Vec<_>>(),
        )),
        Cell::Double => Arc::new(Float64Array::from(
            found.map(|v| match v { Some(Value::Double(f)) => Some(*f), _ => None }.filter(|f| *f != -f64::MAX)).collect::<Vec<_>>(),
        )),
        Cell::String => Arc::new(StringArray::from(found.map(text).collect::<Vec<_>>())),
        Cell::Decimal(scale) => {
            // Java's copier returns at the first null, so the rest of its chunk stays null.
            let mut cells: Vec<Option<Vec<u8>>> = Vec::new();
            let mut stopped = false;
            for v in found {
                if stopped {
                    cells.push(None);
                    continue;
                }
                match v {
                    None => {
                        stopped = true;
                        cells.push(None);
                    }
                    Some(Value::Bytes(bytes)) if bytes.is_empty() => {
                        return Err(record_failure(
                            "java.lang.NumberFormatException: Zero length BigInteger",
                        ))
                    }
                    Some(Value::Bytes(bytes)) => cells.push(Some(big_decimal_cell(scale, bytes))),
                    Some(_) => return Err(record_failure("java.lang.IllegalStateException: not a decimal")),
                }
            }
            Arc::new(BinaryArray::from(cells.iter().map(|c| c.as_deref()).collect::<Vec<_>>()))
        }
        Cell::IntArray => {
            let mut b = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(item(ColumnType::Int)));
            for v in found {
                match v {
                    Some(Value::Array(items)) => {
                        for i in items {
                            b.values().append_option(int(Some(i)).filter(|i| *i != i32::MIN));
                        }
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        Cell::LongArray => {
            let mut b = ListBuilder::new(Int64Builder::new()).with_field(Arc::new(item(ColumnType::Long)));
            for v in found {
                match v {
                    Some(Value::Array(items)) => {
                        for i in items {
                            b.values().append_option(long(Some(i)).filter(|l| *l != i64::MIN));
                        }
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        Cell::FloatArray => {
            let mut b = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(item(ColumnType::Float)));
            for v in found {
                match v {
                    Some(Value::Array(items)) => {
                        for i in items {
                            let f = match i { Value::Float(f) => Some(*f), _ => None };
                            b.values().append_option(f.filter(|f| *f != -f32::MAX));
                        }
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        Cell::DoubleArray => {
            let mut b = ListBuilder::new(Float64Builder::new()).with_field(Arc::new(item(ColumnType::Double)));
            for v in found {
                match v {
                    Some(Value::Array(items)) => {
                        for i in items {
                            let f = match i { Value::Double(f) => Some(*f), _ => None };
                            b.values().append_option(f.filter(|f| *f != -f64::MAX));
                        }
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        Cell::StringArray => {
            let mut b = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(item(ColumnType::String)));
            for v in found {
                match v {
                    Some(Value::Array(items)) => {
                        for i in items {
                            match i {
                                Value::String(s) | Value::Enum(s) => b.values().append_value(s),
                                // Behavior: GenericRecordArrayFieldCopier, whose
                                // `CharSequence::toString` is given the null.
                                _ => return Err(record_failure("java.lang.NullPointerException")),
                            }
                        }
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
        Cell::InstantArray(nanos) => {
            let mut b = ListBuilder::new(TimestampNanosecondBuilder::new().with_timezone("UTC"))
                .with_field(Arc::new(item(ColumnType::Instant)));
            for v in found {
                match v {
                    Some(Value::Array(items)) => {
                        for i in items {
                            // Behavior: GenericRecordInstantArrayFieldCopier#convertArray:
                            // the product wraps, and only Java's null becomes a null.
                            let Some(l) = long(Some(i)) else {
                                return Err(record_failure("java.lang.NullPointerException"));
                            };
                            let nanos = l.wrapping_mul(nanos);
                            b.values().append_option(Some(nanos).filter(|n| *n != i64::MIN));
                        }
                        b.append(true);
                    }
                    _ => b.append(false),
                }
            }
            Arc::new(b.finish())
        }
    })
}

fn int(v: Option<&Value>) -> Option<i32> {
    match v {
        Some(Value::Int(i)) => Some(*i),
        _ => None,
    }
}

fn long(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Long(l)) => Some(*l),
        _ => None,
    }
}

fn text(v: Option<&Value>) -> Option<String> {
    match v {
        Some(Value::String(s) | Value::Enum(s)) => Some(s.clone()),
        _ => None,
    }
}
