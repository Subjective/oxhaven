//! The columns of a consumed topic's table, and how records become their cells.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray,
};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};

use crate::json::Document;
use crate::{not_ported, property, KafkaError, Record, Spec};

/// The columns every table has unless a property names them away, in Java's order, each
/// with the property that renames it and its name when the property is not set.
///
/// Behavior: `io.deephaven.kafka.KafkaTools.CommonColumn`
const COMMON: [(Common, &str, Option<&str>); 6] = [
    (
        Common::Partition,
        "deephaven.partition.column.name",
        Some("KafkaPartition"),
    ),
    (
        Common::Offset,
        "deephaven.offset.column.name",
        Some("KafkaOffset"),
    ),
    (
        Common::Timestamp,
        "deephaven.timestamp.column.name",
        Some("KafkaTimestamp"),
    ),
    (
        Common::ReceiveTime,
        "deephaven.receivetime.column.name",
        None,
    ),
    (Common::KeyBytes, "deephaven.keybytes.column.name", None),
    (Common::ValueBytes, "deephaven.valuebytes.column.name", None),
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Common {
    Partition,
    Offset,
    Timestamp,
    ReceiveTime,
    KeyBytes,
    ValueBytes,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Side {
    Key,
    Value,
}

impl Side {
    fn bytes<'a>(self, record: &'a Record) -> Option<&'a [u8]> {
        match self {
            Side::Key => record.key.as_deref(),
            Side::Value => record.value.as_deref(),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Side::Key => "key",
            Side::Value => "value",
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
enum Cells {
    Common(Common),
    /// The whole key or value, read by the Kafka deserializer of the column's type.
    Simple(Side),
    /// One field of the key's or value's JSON object.
    Json(Side, String),
}

/// The definition of a consumed topic's table and the source of each column.
#[derive(Clone, Debug)]
pub struct Plan {
    definition: TableDefinition,
    cells: Vec<Cells>,
}

fn invalid(text: String) -> KafkaError {
    KafkaError::Invalid(text)
}

/// Behavior: io.deephaven.kafka.SimpleImpl.SimpleConsume#getColumnName and #getType
fn simple_column(
    config: &[(String, String)],
    side: Side,
    column: Option<&str>,
    data_type: Option<&ColumnType>,
) -> Result<ColumnDefinition, KafkaError> {
    let side_name = side.name();
    let name = column
        .or_else(|| property(config, &format!("deephaven.{side_name}.column.name")))
        .unwrap_or(match side {
            Side::Key => "KafkaKey",
            Side::Value => "KafkaValue",
        });
    let type_property = format!("deephaven.{side_name}.column.type");
    let deserializer_property = format!("{side_name}.deserializer");
    let unsupported = |what: &str| {
        Err(not_ported(
            "io.deephaven.kafka.SimpleImpl.SerDeserVisitor",
            &format!("a simple {side_name} column of type {what}"),
        ))
    };
    let column_type = if let Some(data_type) = data_type {
        data_type.clone()
    } else if let Some(named) = property(config, &type_property) {
        match named {
            "short" => ColumnType::Short,
            "int" => ColumnType::Int,
            "long" => ColumnType::Long,
            "float" => ColumnType::Float,
            "double" => ColumnType::Double,
            "String" | "string" => ColumnType::String,
            "byte[]" => return unsupported("byte[]"),
            other => {
                return Err(invalid(format!(
                    "java.lang.IllegalArgumentException: Property {type_property} value {other} \
                     not supported"
                )))
            }
        }
    } else if let Some(class) = property(config, &deserializer_property) {
        const PACKAGE: &str = "org.apache.kafka.common.serialization.";
        match class.strip_prefix(PACKAGE) {
            Some("ShortDeserializer") => ColumnType::Short,
            Some("IntegerDeserializer") => ColumnType::Int,
            Some("LongDeserializer") => ColumnType::Long,
            Some("FloatDeserializer") => ColumnType::Float,
            Some("DoubleDeserializer") => ColumnType::Double,
            Some(
                other @ ("ByteArrayDeserializer"
                | "UUIDDeserializer"
                | "ByteBufferDeserializer"
                | "BytesDeserializer"),
            ) => return unsupported(&format!("the one {other} reads")),
            _ => {
                return Err(invalid(format!(
                    "java.lang.IllegalArgumentException: Deserializer type {class} for \
                     {deserializer_property} not supported."
                )))
            }
        }
    } else {
        return Err(invalid(format!(
            "io.deephaven.UncheckedDeephavenException: Unable to find the type for column \
             '{name}' ({side_name}_spec). Please explicitly set the data type in the \
             constructor, or through the kafka configuration '{type_property}' or \
             '{deserializer_property}'."
        )));
    };
    match column_type {
        ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::String => Ok(ColumnDefinition::new(name, column_type)),
        other => unsupported(&format!("{other:?}")),
    }
}

/// The field a JSON pointer of one step names.
///
/// Behavior: com.fasterxml.jackson.core.JsonPointer#compile, for one segment
fn pointer_field(pointer: &str) -> Result<String, KafkaError> {
    let segment = &pointer[1..];
    if segment.contains('/') {
        return Err(not_ported(
            "com.fasterxml.jackson.databind.JsonNode#at(JsonPointer)",
            &format!("the JSON pointer {pointer}, which names a nested field"),
        ));
    }
    Ok(segment.replace("~1", "/").replace("~0", "~"))
}

/// Behavior: io.deephaven.kafka.JsonImpl.JsonConsume#getIngestData,
/// io.deephaven.kafka.ingest.JsonNodeChunkAdapter#makeFieldCopier
fn json_columns(
    side: Side,
    columns: &[(String, ColumnType)],
    mapping: Option<&[(String, String)]>,
    definitions: &mut Vec<ColumnDefinition>,
    cells: &mut Vec<Cells>,
) -> Result<(), KafkaError> {
    for (name, column_type) in columns {
        let refused = |copier: &str| {
            Err(not_ported(
                &format!("io.deephaven.kafka.ingest.{copier}"),
                &format!("the JSON column {name} of type {column_type:?}"),
            ))
        };
        match column_type {
            ColumnType::Boolean
            | ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::String => {}
            ColumnType::Char => return refused("JsonNodeCharFieldCopier"),
            ColumnType::Instant => return refused("JsonNodeInstantFieldCopier"),
            _ => return refused("JsonNodeChunkAdapter#makeFieldCopier"),
        }
        let mut fields = Vec::new();
        for (field, column) in mapping.unwrap_or_default() {
            if column == name {
                // Behavior: JsonImpl.JsonConsume#mapNonPointers
                fields.push(match field.strip_prefix('/') {
                    Some(_) => pointer_field(field)?,
                    None => field.clone(),
                });
            }
        }
        let field = match fields.len() {
            0 => name.clone(),
            1 => fields.remove(0),
            _ => {
                return Err(not_ported(
                    "io.deephaven.kafka.ingest.MultiFieldChunkAdapter",
                    &format!("several fields mapped to the column {name}"),
                ))
            }
        };
        definitions.push(ColumnDefinition::new(name.clone(), column_type.clone()));
        cells.push(Cells::Json(side, field));
    }
    for (field, column) in mapping.unwrap_or_default() {
        if !columns.iter().any(|(name, _)| name == column) {
            return Err(invalid(format!(
                "the JSON field {field} is mapped to the column {column}, which is not defined"
            )));
        }
    }
    Ok(())
}

fn fixed<const N: usize>(bytes: &[u8], deserializer: &str) -> Result<[u8; N], KafkaError> {
    bytes.try_into().map_err(|_| {
        // Java's consumer throws this from `poll`, and the ingester then stops reading
        // without telling the table (docs/KAFKA.md, "For the owner's ruling").
        not_ported(
            "io.deephaven.kafka.ingest.KafkaIngester#pollOnce",
            &format!(
                "a record the deserializer refuses \
                 (org.apache.kafka.common.errors.SerializationException: Size of data received \
                 by {deserializer} is not {N})"
            ),
        )
    })
}

impl Plan {
    /// Behavior: io.deephaven.kafka.KafkaTools#getConsumeStruct
    pub fn new(config: &[(String, String)], key: &Spec, value: &Spec) -> Result<Self, KafkaError> {
        if *key == Spec::Ignore && *value == Spec::Ignore {
            return Err(invalid(
                "java.lang.IllegalArgumentException: can't ignore both key and value: keySpec \
                 and valueSpec can't both be ignore specs"
                    .to_owned(),
            ));
        }
        let mut definitions = Vec::new();
        let mut cells = Vec::new();
        for (common, name_property, default) in COMMON {
            // Behavior: KafkaTools.CommonColumn#getDefinition: an empty name removes the
            // column.
            let name = property(config, name_property).or(default);
            let Some(name) = name.filter(|name| !name.is_empty()) else {
                continue;
            };
            let column_type = match common {
                Common::Partition | Common::KeyBytes | Common::ValueBytes => ColumnType::Int,
                Common::Offset => ColumnType::Long,
                Common::Timestamp | Common::ReceiveTime => ColumnType::Instant,
            };
            definitions.push(ColumnDefinition::new(name, column_type));
            cells.push(Cells::Common(common));
        }
        for (side, spec) in [(Side::Key, key), (Side::Value, value)] {
            match spec {
                Spec::Ignore => {}
                Spec::Simple { column, data_type } => {
                    let column = column.as_deref();
                    definitions.push(simple_column(config, side, column, data_type.as_ref())?);
                    cells.push(Cells::Simple(side));
                }
                Spec::Json { columns, mapping } => {
                    let mapping = mapping.as_deref();
                    json_columns(side, columns, mapping, &mut definitions, &mut cells)?;
                }
            }
        }
        let definition = TableDefinition::new(definitions).map_err(|duplicate| {
            invalid(format!(
                "io.deephaven.engine.table.impl.NoSuchColumnException: duplicate column name \
                 {}",
                duplicate.0
            ))
        })?;
        Ok(Self { definition, cells })
    }

    pub fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    /// The columns of the rows that `records` become, in the definition's order.
    /// `received` is when the records arrived, in nanoseconds since the epoch.
    ///
    /// A value Java stores as the null of its type is a null cell.
    ///
    /// Behavior: io.deephaven.kafka.ingest.KafkaStreamPublisher#consumeRecords
    pub fn decode(&self, records: &[Record], received: i64) -> Result<Vec<ArrayRef>, KafkaError> {
        let documents = |side: Side| -> Result<Vec<Document>, KafkaError> {
            let used = |cells: &Cells| matches!(cells, Cells::Json(s, _) if *s == side);
            if !self.cells.iter().any(used) {
                return Ok(Vec::new());
            }
            let parse = |record: &Record| match side.bytes(record) {
                // Behavior: JsonNodeUtil#makeJsonNode is given null and Jackson refuses it.
                None => Err(KafkaError::Record(
                    "java.lang.IllegalArgumentException: argument \"content\" is null".to_owned(),
                )),
                Some(bytes) => Document::parse(&utf8(bytes)?),
            };
            records.iter().map(parse).collect()
        };
        let key_documents = documents(Side::Key)?;
        let value_documents = documents(Side::Value)?;

        let mut columns = Vec::with_capacity(self.cells.len());
        for (cells, column) in self.cells.iter().zip(self.definition.columns()) {
            let column_type = &column.column_type;
            columns.push(match cells {
                Cells::Common(common) => common_column(*common, records, received)?,
                Cells::Simple(side) => simple_cells(*side, column_type, records)?,
                Cells::Json(side, field) => {
                    let documents = match side {
                        Side::Key => &key_documents,
                        Side::Value => &value_documents,
                    };
                    json_cells(column_type, field, documents)?
                }
            });
        }
        Ok(columns)
    }
}

fn instants(nanos: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(TimestampNanosecondArray::from(nanos).with_timezone("UTC"))
}

fn size(bytes: Option<&[u8]>) -> Option<i32> {
    bytes.and_then(|bytes| i32::try_from(bytes.len()).ok())
}

fn common_column(
    common: Common,
    records: &[Record],
    received: i64,
) -> Result<ArrayRef, KafkaError> {
    Ok(match common {
        Common::Partition => Arc::new(Int32Array::from_iter_values(
            records.iter().map(|r| r.partition),
        )),
        Common::Offset => Arc::new(Int64Array::from_iter_values(
            records.iter().map(|r| r.offset),
        )),
        Common::Timestamp => {
            // Behavior: ConsumerRecord#NO_TIMESTAMP, DateTimeUtils#millisToNanos
            let nanos = |record: &Record| match record.timestamp {
                -1 => Ok(None),
                millis => millis.checked_mul(1_000_000).map(Some).ok_or_else(|| {
                    KafkaError::Record(format!(
                        "io.deephaven.time.DateTimeUtils$DateTimeOverflowException: \
                         the timestamp {millis} ms"
                    ))
                }),
            };
            instants(records.iter().map(nanos).collect::<Result<_, _>>()?)
        }
        Common::ReceiveTime => instants(vec![Some(received); records.len()]),
        Common::KeyBytes => Arc::new(Int32Array::from_iter(
            records.iter().map(|r| size(r.key.as_deref())),
        )),
        Common::ValueBytes => Arc::new(Int32Array::from_iter(
            records.iter().map(|r| size(r.value.as_deref())),
        )),
    })
}

fn utf8(bytes: &[u8]) -> Result<String, KafkaError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        not_ported(
            "org.apache.kafka.common.serialization.StringDeserializer#deserialize",
            "bytes that are not UTF-8",
        )
    })
}

/// Behavior: the `deserialize` of `org.apache.kafka.common.serialization`'s
/// `StringDeserializer`, `ShortDeserializer`, `IntegerDeserializer`, `LongDeserializer`,
/// `FloatDeserializer` and `DoubleDeserializer`: big-endian, of exactly the type's size
fn simple_cells(
    side: Side,
    column_type: &ColumnType,
    records: &[Record],
) -> Result<ArrayRef, KafkaError> {
    let payloads = records.iter().map(|record| side.bytes(record));
    macro_rules! numbers {
        ($array:ty, $native:ty, $deserializer:literal, $null:expr) => {{
            let cell = |bytes: Option<&[u8]>| -> Result<Option<$native>, KafkaError> {
                let Some(bytes) = bytes else { return Ok(None) };
                let value = <$native>::from_be_bytes(fixed(bytes, $deserializer)?);
                Ok(Some(value).filter(|value| *value != $null))
            };
            let cells: Vec<Option<$native>> = payloads.map(cell).collect::<Result<_, _>>()?;
            Arc::new(<$array>::from(cells)) as ArrayRef
        }};
    }
    Ok(match column_type {
        ColumnType::String => {
            let cells: Vec<Option<String>> = payloads
                .map(|bytes| bytes.map(utf8).transpose())
                .collect::<Result<_, _>>()?;
            Arc::new(StringArray::from(cells))
        }
        ColumnType::Short => numbers!(Int16Array, i16, "ShortDeserializer", i16::MIN),
        ColumnType::Int => numbers!(Int32Array, i32, "IntegerDeserializer", i32::MIN),
        ColumnType::Long => numbers!(Int64Array, i64, "LongDeserializer", i64::MIN),
        ColumnType::Float => numbers!(Float32Array, f32, "FloatDeserializer", -f32::MAX),
        ColumnType::Double => numbers!(Float64Array, f64, "DoubleDeserializer", -f64::MAX),
        other => unreachable!("Plan::new admits no simple column of type {other:?}"),
    })
}

/// Behavior: the `copyField` of `io.deephaven.kafka.ingest`'s `JsonNodeBooleanFieldCopier`,
/// `JsonNodeByteFieldCopier`, `JsonNodeShortFieldCopier`, `JsonNodeIntFieldCopier`,
/// `JsonNodeLongFieldCopier`, `JsonNodeFloatFieldCopier`, `JsonNodeDoubleFieldCopier` and
/// `JsonNodeStringFieldCopier`
fn json_cells(
    column_type: &ColumnType,
    field: &str,
    documents: &[Document],
) -> Result<ArrayRef, KafkaError> {
    let nodes = documents.iter().map(|document| document.field(field));
    macro_rules! integers {
        ($array:ty, $native:ty, $method:literal) => {{
            let cells: Vec<Option<$native>> = nodes
                .map(|node| {
                    // The cast keeps the low bits, and the type's null value is a null.
                    let value = node?.integral($method)?.map(|value| value as $native);
                    Ok(value.filter(|value| *value != <$native>::MIN))
                })
                .collect::<Result<_, KafkaError>>()?;
            Arc::new(<$array>::from(cells)) as ArrayRef
        }};
    }
    Ok(match column_type {
        ColumnType::Boolean => {
            let cells: Vec<Option<bool>> = nodes
                .map(|node| Ok(node?.boolean()))
                .collect::<Result<_, KafkaError>>()?;
            Arc::new(BooleanArray::from(cells))
        }
        ColumnType::Byte => integers!(Int8Array, i8, "asInt"),
        ColumnType::Short => integers!(Int16Array, i16, "asInt"),
        ColumnType::Int => integers!(Int32Array, i32, "asInt"),
        ColumnType::Long => integers!(Int64Array, i64, "asLong"),
        ColumnType::Float => {
            let cells: Vec<Option<f32>> = nodes
                .map(|node| {
                    let value = node?.double()?.map(|value| value as f32);
                    Ok(value.filter(|value| *value != -f32::MAX))
                })
                .collect::<Result<_, KafkaError>>()?;
            Arc::new(Float32Array::from(cells))
        }
        ColumnType::Double => {
            let cells: Vec<Option<f64>> = nodes
                .map(|node| Ok(node?.double()?.filter(|value| *value != -f64::MAX)))
                .collect::<Result<_, KafkaError>>()?;
            Arc::new(Float64Array::from(cells))
        }
        ColumnType::String => {
            let cells: Vec<Option<String>> = nodes
                .map(|node| node?.text())
                .collect::<Result<_, KafkaError>>()?;
            Arc::new(StringArray::from(cells))
        }
        other => unreachable!("Plan::new admits no JSON column of type {other:?}"),
    })
}
