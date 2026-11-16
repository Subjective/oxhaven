//! The columns of a consumed topic's table, and how records become their cells.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, Int8Builder, ListBuilder, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::Field;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableClass, TableDefinition};

use crate::json::{self, Node, Pointer};
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
    fn bytes(self, record: &Record) -> Option<&[u8]> {
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
    /// The nodes the pointers find in the key's or value's JSON. Java fills the column
    /// once per pointer, so a column with none or with several fails the table when
    /// records arrive.
    Json(Side, Vec<Pointer>),
}

/// The definition of a consumed topic's table and the source of each column.
#[derive(Clone, Debug)]
pub struct Plan {
    topic: String,
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
            "byte[]" => ColumnType::array(ColumnType::Byte),
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
            Some("ByteArrayDeserializer") => ColumnType::array(ColumnType::Byte),
            Some(other @ ("UUIDDeserializer" | "ByteBufferDeserializer" | "BytesDeserializer")) => {
                return unsupported(&format!("the one {other} reads"))
            }
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
        ColumnType::Array(ref element) if **element == ColumnType::Byte => {
            Ok(ColumnDefinition::new(name, column_type))
        }
        other => unsupported(&format!("{other:?}")),
    }
}

/// Java's simple name of the class of a column of `column_type`.
fn simple_name(column_type: &ColumnType) -> Option<String> {
    Some(match column_type {
        ColumnType::Boolean => "Boolean".into(),
        ColumnType::Byte => "byte".into(),
        ColumnType::Char => "char".into(),
        ColumnType::Short => "short".into(),
        ColumnType::Int => "int".into(),
        ColumnType::Long => "long".into(),
        ColumnType::Float => "float".into(),
        ColumnType::Double => "double".into(),
        ColumnType::String => "String".into(),
        ColumnType::Instant => "Instant".into(),
        ColumnType::Array(element) => format!("{}[]", simple_name(element)?),
        ColumnType::Vector(element) => match **element {
            ColumnType::Byte => "ByteVector".into(),
            ColumnType::Char => "CharVector".into(),
            ColumnType::Short => "ShortVector".into(),
            ColumnType::Int => "IntVector".into(),
            ColumnType::Long => "LongVector".into(),
            ColumnType::Float => "FloatVector".into(),
            ColumnType::Double => "DoubleVector".into(),
            _ => "ObjectVector".into(),
        },
        ColumnType::Table(TableClass::Table) => "Table".into(),
        ColumnType::Table(TableClass::QueryTable) => "QueryTable".into(),
        ColumnType::BigDecimal => "BigDecimal".into(),
        ColumnType::BigInteger => "BigInteger".into(),
        ColumnType::LocalDate => "LocalDate".into(),
        ColumnType::LocalTime => "LocalTime".into(),
        ColumnType::LocalDateTime => "LocalDateTime".into(),
        ColumnType::Duration => "Duration".into(),
        ColumnType::Period => "Period".into(),
        ColumnType::ZoneId => "ZoneId".into(),
        ColumnType::ZonedDateTime => "ZonedDateTime".into(),
        _ => return None,
    })
}

/// The pointers each column is filled from, in the order of `columns`.
///
/// Java makes a map from pointer to column: the mapping's fields, each made a pointer
/// unless it is one, then a pointer to the field of its own name for each column the
/// mapping does not name, which replaces a mapped pointer that is the same. Each entry of
/// the map fills its column once, so a column ends with no pointer, one, or several.
///
/// Behavior: io.deephaven.kafka.JsonImpl.JsonConsume#mapNonPointers and #getIngestData,
/// io.deephaven.kafka.ingest.MultiFieldChunkAdapter#MultiFieldChunkAdapter
fn json_pointers(
    columns: &[(String, ColumnType)],
    mapping: Option<&[(String, String)]>,
) -> Result<Vec<Vec<Pointer>>, KafkaError> {
    let mapping = mapping.unwrap_or_default();
    let pointer_of = |field: &String| {
        if field.starts_with('/') {
            field.clone()
        } else {
            Pointer::field(field)
        }
    };
    let pointers: Vec<String> = mapping.iter().map(|(field, _)| pointer_of(field)).collect();
    let collide = (0..pointers.len()).any(|i| pointers[..i].contains(&pointers[i]));
    // Java puts the fields, made pointers, into a new map in the order it iterates the
    // `HashMap` that the Python `dict` became, so the later of two equal pointers wins.
    let order: Vec<usize> = if collide {
        let fields: Vec<&str> = mapping.iter().map(|(field, _)| field.as_str()).collect();
        java_sized_hash_map_order(&fields).ok_or_else(|| {
            not_ported(
                "java.util.HashMap#treeifyBin",
                "a JSON mapping whose fields collide as pointers and fill one bucket of \
                 Java's hash map with eight or more",
            )
        })?
    } else {
        (0..mapping.len()).collect()
    };
    let mut map: Vec<(String, String)> = Vec::new();
    for index in order {
        let (pointer, column) = (&pointers[index], &mapping[index].1);
        match map.iter_mut().find(|(known, _)| known == pointer) {
            None => map.push((pointer.clone(), column.clone())),
            Some(entry) => entry.1 = column.clone(),
        }
    }
    if map
        .iter()
        .any(|(_, column)| !columns.iter().any(|(name, _)| name == column))
    {
        return Err(invalid(
            "java.lang.IllegalArgumentException: Column not found in Deephaven table: -1"
                .to_owned(),
        ));
    }
    let covered: Vec<String> = map.iter().map(|(_, column)| column.clone()).collect();
    for (name, _) in columns {
        if !covered.contains(name) {
            let pointer = Pointer::field(name);
            match map.iter_mut().find(|(known, _)| *known == pointer) {
                Some(entry) => entry.1 = name.clone(),
                None => map.push((pointer, name.clone())),
            }
        }
    }
    Ok(columns
        .iter()
        .map(|(name, _)| {
            let pointers = map.iter().filter(|(_, column)| column == name);
            pointers
                .map(|(pointer, _)| Pointer::compile(pointer))
                .collect()
        })
        .collect())
}

/// The indexes of `keys` (distinct, in the order of arrival) in the order Java iterates
/// a `HashMap` made with `new HashMap(keys.length)` and given them in that order: by
/// bucket of the final table, and within a bucket in the order of arrival, which a
/// resize keeps. `None` where a bucket reaches eight keys, as Java then turns it into a
/// tree or resizes early.
///
/// Behavior: org.jpy.PyLib via deephaven.jcompat.j_hashmap, java.util.HashMap#HashMap(int),
/// #tableSizeFor, #putVal, #resize, #treeifyBin and HashMap.HashIterator
fn java_sized_hash_map_order(keys: &[&str]) -> Option<Vec<usize>> {
    let hashes: Vec<usize> = keys
        .iter()
        .map(|key| crate::client::java_hash(key) as usize)
        .collect();
    let mut capacity = keys.len().max(1).next_power_of_two();
    for count in 1..=keys.len() {
        let bucket = hashes[count - 1] & (capacity - 1);
        let before = hashes[..count - 1]
            .iter()
            .filter(|hash| *hash & (capacity - 1) == bucket)
            .count();
        if before >= 7 {
            return None;
        }
        if count > capacity * 3 / 4 {
            capacity *= 2;
        }
    }
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by_key(|index| hashes[*index] & (capacity - 1));
    Some(order)
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
    let pointers = json_pointers(columns, mapping)?;
    for ((name, column_type), pointers) in columns.iter().zip(pointers) {
        let supported = matches!(
            column_type,
            ColumnType::Boolean
                | ColumnType::Byte
                | ColumnType::Char
                | ColumnType::Short
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::String
                | ColumnType::Instant
        );
        // Java checks the type when it makes a column's copier, so not for a column
        // without a pointer.
        if !supported && !pointers.is_empty() {
            return Err(match simple_name(column_type) {
                Some(simple) => invalid(format!(
                    "io.deephaven.UncheckedDeephavenException: Type {simple} not supported \
                     for JSON"
                )),
                None => not_ported(
                    "io.deephaven.kafka.ingest.JsonNodeChunkAdapter#makeFieldCopier",
                    &format!("the JSON column {name} of type {column_type:?}"),
                ),
            });
        }
        definitions.push(ColumnDefinition::new(name.clone(), column_type.clone()));
        cells.push(Cells::Json(side, pointers));
    }
    Ok(())
}

/// The bytes of a number, which must be exactly its size.
///
/// Behavior: `org.apache.kafka.clients.consumer.internals.CompletedFetch#parseRecord`, which
/// wraps what the deserializer throws; the exception fails the table through
/// `io.deephaven.kafka.ingest.KafkaIngester#pollOnce`.
fn fixed<const N: usize>(
    bytes: &[u8],
    topic: &str,
    side: Side,
    record: &Record,
) -> Result<[u8; N], KafkaError> {
    bytes.try_into().map_err(|_| {
        let side = side.name().to_uppercase();
        let (partition, offset) = (record.partition, record.offset);
        KafkaError::Record(format!(
            "org.apache.kafka.common.errors.RecordDeserializationException: Error \
             deserializing {side} for partition {topic}-{partition} at offset {offset}. If \
             needed, please seek past the record to continue consumption."
        ))
    })
}

impl Plan {
    /// The table of `topic`, whose name appears in the failure a bad record causes.
    ///
    /// Behavior: io.deephaven.kafka.KafkaTools#getConsumeStruct
    pub fn new(
        topic: &str,
        config: &[(String, String)],
        key: &Spec,
        value: &Spec,
    ) -> Result<Self, KafkaError> {
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
        // Behavior: io.deephaven.engine.table.TableDefinition#checkDuplicateNames
        let definition = TableDefinition::new(definitions).map_err(|duplicate| {
            invalid(format!(
                "java.lang.IllegalArgumentException: Supplied ColumnDefinitions include \
                 duplicate names [{}]",
                duplicate.0
            ))
        })?;
        Ok(Self {
            topic: topic.to_owned(),
            definition,
            cells,
        })
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
        let documents = |side: Side| -> Result<Vec<Node>, KafkaError> {
            let used = |cells: &Cells| matches!(cells, Cells::Json(s, _) if *s == side);
            if !self.cells.iter().any(used) {
                return Ok(Vec::new());
            }
            let parse = |record: &Record| match side.bytes(record) {
                // Behavior: JsonNodeUtil#makeJsonNode is given null and Jackson refuses it.
                None => Err(KafkaError::Record(
                    "java.lang.IllegalArgumentException: argument \"content\" is null".to_owned(),
                )),
                Some(bytes) => json::parse(&utf8(bytes)?),
            };
            records.iter().map(parse).collect()
        };
        let key_documents = documents(Side::Key)?;
        let value_documents = documents(Side::Value)?;

        let mut columns = Vec::with_capacity(self.cells.len());
        // Java's chunk of each column has a row per record and pointer; they must agree.
        let mut sizes = Vec::with_capacity(self.cells.len());
        for (cells, column) in self.cells.iter().zip(self.definition.columns()) {
            let column_type = &column.column_type;
            match cells {
                Cells::Common(common) => columns.push(common_column(*common, records, received)?),
                Cells::Simple(side) => {
                    columns.push(simple_cells(&self.topic, *side, column_type, records)?)
                }
                Cells::Json(side, pointers) => {
                    let documents = match side {
                        Side::Key => &key_documents,
                        Side::Value => &value_documents,
                    };
                    let mut filled = pointers
                        .iter()
                        .map(|pointer| json_cells(column_type, pointer, documents))
                        .collect::<Result<Vec<_>, _>>()?;
                    columns.push(match filled.len() {
                        1 => filled.remove(0),
                        // Fails below when there are records.
                        _ => arrow::array::new_empty_array(&column_type.arrow_type()),
                    });
                    sizes.push(records.len() * pointers.len());
                    continue;
                }
            }
            sizes.push(records.len());
        }
        if !records.is_empty() && sizes.iter().any(|size| *size != records.len()) {
            // Behavior: io.deephaven.kafka.ingest.KafkaStreamPublisher#checkChunkSizes
            let sizes: Vec<String> = sizes.iter().map(usize::to_string).collect();
            return Err(KafkaError::Record(format!(
                "java.lang.IllegalStateException: Publisher chunks have size mismatch: {}",
                sizes.join(", ")
            )));
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
    topic: &str,
    side: Side,
    column_type: &ColumnType,
    records: &[Record],
) -> Result<ArrayRef, KafkaError> {
    macro_rules! numbers {
        ($array:ty, $native:ty, $null:expr) => {{
            let cell = |record: &Record| -> Result<Option<$native>, KafkaError> {
                let Some(bytes) = side.bytes(record) else {
                    return Ok(None);
                };
                let value = <$native>::from_be_bytes(fixed(bytes, topic, side, record)?);
                Ok(Some(value).filter(|value| *value != $null))
            };
            let cells: Vec<Option<$native>> = records.iter().map(cell).collect::<Result<_, _>>()?;
            Arc::new(<$array>::from(cells)) as ArrayRef
        }};
    }
    Ok(match column_type {
        ColumnType::String => {
            let cells: Vec<Option<String>> = records
                .iter()
                .map(|record| side.bytes(record).map(utf8).transpose())
                .collect::<Result<_, _>>()?;
            Arc::new(StringArray::from(cells))
        }
        ColumnType::Short => numbers!(Int16Array, i16, i16::MIN),
        ColumnType::Int => numbers!(Int32Array, i32, i32::MIN),
        ColumnType::Long => numbers!(Int64Array, i64, i64::MIN),
        ColumnType::Float => numbers!(Float32Array, f32, -f32::MAX),
        ColumnType::Double => numbers!(Float64Array, f64, -f64::MAX),
        // Behavior: org.apache.kafka.common.serialization.ByteArrayDeserializer#deserialize,
        // which gives the bytes as they are, an empty array for none
        ColumnType::Array(_) => {
            let field = Field::new("", ColumnType::Byte.arrow_type(), true);
            let mut cells = ListBuilder::new(Int8Builder::new()).with_field(Arc::new(field));
            for record in records {
                match side.bytes(record) {
                    Some(bytes) => {
                        cells.values().extend(bytes.iter().map(|b| Some(*b as i8)));
                        cells.append(true);
                    }
                    None => cells.append(false),
                }
            }
            Arc::new(cells.finish())
        }
        other => unreachable!("Plan::new admits no simple column of type {other:?}"),
    })
}

/// The cells of a JSON column of `column_type`: the node `pointer` finds in each record,
/// converted. A value equal to Java's null of the type is a null cell.
///
/// Behavior: the `copyField` of `io.deephaven.kafka.ingest`'s `JsonNodeBooleanFieldCopier`,
/// `JsonNodeByteFieldCopier`, `JsonNodeCharFieldCopier`, `JsonNodeShortFieldCopier`,
/// `JsonNodeIntFieldCopier`, `JsonNodeLongFieldCopier`, `JsonNodeFloatFieldCopier`,
/// `JsonNodeDoubleFieldCopier`, `JsonNodeStringFieldCopier` and
/// `JsonNodeInstantFieldCopier`
fn json_cells(
    column_type: &ColumnType,
    pointer: &Pointer,
    documents: &[Node],
) -> Result<ArrayRef, KafkaError> {
    let nodes = documents.iter().map(|document| pointer.at(document));
    macro_rules! cells {
        ($array:ty, $convert:expr) => {{
            let cells = nodes
                .map($convert)
                .collect::<Result<Vec<_>, KafkaError>>()?;
            Arc::new(<$array>::from(cells)) as ArrayRef
        }};
    }
    // The cast keeps the low bits, and the type's null value is a null.
    Ok(match column_type {
        ColumnType::Boolean => cells!(BooleanArray, |node: &Node| node.boolean(pointer)),
        ColumnType::Byte => cells!(Int8Array, |node: &Node| {
            Ok(node.as_int().map(|v| v as i8).filter(|v| *v != i8::MIN))
        }),
        ColumnType::Short => cells!(Int16Array, |node: &Node| {
            Ok(node.as_int().map(|v| v as i16).filter(|v| *v != i16::MIN))
        }),
        ColumnType::Int => cells!(Int32Array, |node: &Node| {
            Ok(node.as_int().filter(|v| *v != i32::MIN))
        }),
        ColumnType::Long => cells!(Int64Array, |node: &Node| {
            Ok(node.as_long().filter(|v| *v != i64::MIN))
        }),
        ColumnType::Float => cells!(Float32Array, |node: &Node| {
            Ok(node
                .as_double()
                .map(|v| v as f32)
                .filter(|v| *v != -f32::MAX))
        }),
        ColumnType::Double => cells!(Float64Array, |node: &Node| {
            Ok(node.as_double().filter(|v| *v != -f64::MAX))
        }),
        ColumnType::Char => cells!(UInt16Array, |node: &Node| Ok(node.char())),
        ColumnType::String => cells!(StringArray, |node: &Node| node.text()),
        ColumnType::Instant => {
            let cells = nodes.map(Node::instant).collect::<Result<Vec<_>, _>>()?;
            instants(cells)
        }
        other => unreachable!("Plan::new admits no JSON column of type {other:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::java_sized_hash_map_order;

    /// Behavior: java.util.HashMap#HashMap(int), #putVal, #resize and HashMap.HashIterator
    #[test]
    fn keys_come_in_the_order_of_a_java_hash_map_of_their_number() {
        // Two keys end in a table of four buckets: "Y" in 1, "/Y" in 2.
        assert_eq!(java_sized_hash_map_order(&["/Y", "Y"]), Some(vec![1, 0]));
        assert_eq!(java_sized_hash_map_order(&["Y", "/Y"]), Some(vec![0, 1]));
        // "Aa" and "BB" have one hash: one bucket, in the order of arrival.
        assert_eq!(java_sized_hash_map_order(&["BB", "Aa"]), Some(vec![0, 1]));
        assert_eq!(java_sized_hash_map_order(&["Aa", "BB"]), Some(vec![0, 1]));
    }

    /// Behavior: java.util.HashMap#treeifyBin
    #[test]
    fn a_bucket_of_eight_keys_is_not_ordered() {
        let keys: Vec<String> = (0..8u32)
            .map(|bits| {
                (0..3)
                    .map(|bit| if bits >> bit & 1 == 1 { "Aa" } else { "BB" })
                    .collect()
            })
            .collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        assert_eq!(java_sized_hash_map_order(&keys), None);
        assert!(java_sized_hash_map_order(&keys[..7]).is_some());
    }
}
