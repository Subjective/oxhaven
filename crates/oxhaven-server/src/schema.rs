//! Table schemas as clients receive them: an Arrow schema carrying Deephaven metadata, and
//! its encoding as an Arrow IPC schema message.
//!
//! Shared by the table service (`ExportedTableCreationResponse.schema_header`) and by Flight
//! (`SchemaResult`, `FlightInfo`, the first message of `DoGet`). Port of the schema half of
//! `io.deephaven.extensions.barrage.util.BarrageUtil`.
//!
//! Java builds its metadata in hash maps, so the order of metadata entries on the wire is
//! unspecified there and here. The bytes are therefore compared after decoding, never as
//! bytes.

use crate::wire_columns::wire_type;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableAttributes};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

/// Prefix of every Deephaven metadata key. `BarrageUtil.ATTR_DH_PREFIX`.
pub const METADATA_PREFIX: &str = "deephaven:";

/// Field metadata key holding the Java class name of a column's type.
/// `BarrageUtil.ATTR_DH_PREFIX + ATTR_TYPE_TAG`.
pub const TYPE_KEY: &str = "deephaven:type";

const TRUE: &str = "true";
const JAVA_BOOLEAN: &str = "java.lang.Boolean";

/// The start of an encapsulated Arrow IPC message. `MessageHelper.IPC_CONTINUATION_TOKEN`.
const IPC_CONTINUATION_TOKEN: [u8; 4] = [0xff; 4];

/// What a table contributes to its schema besides its columns.
///
/// Build one with `SchemaContext::default()` and set fields, or take it from a table with
/// [`SchemaContext::of`].
#[derive(Clone, PartialEq, Eq, Default, Debug)]
#[non_exhaustive]
pub struct SchemaContext {
    pub attributes: TableAttributes,
    /// `Table::is_flat`.
    pub is_flat: bool,
    /// The key column names if the table is an input table; `Some(vec![])` for an
    /// append-only input table, `None` for any other table.
    pub input_table_keys: Option<Vec<String>>,
    /// The columns that hold a preview of the values of another table's column, by name.
    pub previews: HashMap<String, PreviewColumn>,
}

impl SchemaContext {
    pub fn of(table: &dyn Table) -> Self {
        Self {
            attributes: table.attributes(),
            is_flat: table.is_flat(),
            input_table_keys: table.input_table().map(|input| input.key_columns()),
            previews: previews_of(table),
        }
    }
}

/// A column of text that stands for the values of a column a user interface could not
/// show: what `ApplyPreviewColumns` puts in the place of that column.
///
/// In Java such a column has a type of its own, a class that holds the text. A column type
/// here cannot say so; the column is a `String` column to the engine, and this says what
/// a client is told about it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PreviewColumn {
    /// The Java class of the column: `deephaven:type`.
    pub java_type: &'static str,
    /// `deephaven:description`: Java's entry for the column in the table attribute
    /// `ColumnDescriptions`.
    pub description: String,
}

/// The preview columns of tables, by the table that has them.
///
/// Java keeps the class in the column definition and the description in a table
/// attribute. A table here has no place for either, so they are kept beside the table,
/// found by the table's address, and forgotten when the table is dropped.
static PREVIEWS: Mutex<Vec<TablePreviews>> = Mutex::new(Vec::new());

/// A table and its preview columns.
type TablePreviews = (Weak<dyn Table>, HashMap<String, PreviewColumn>);

/// Remembers that these columns of `table` are previews.
///
/// Behavior: `io.deephaven.engine.table.impl.preview.ColumnPreviewManager#makePreviewTable`
/// (the column types of its `updateView`, and the `COLUMN_DESCRIPTIONS_ATTRIBUTE` it sets)
pub fn remember_previews(table: &Arc<dyn Table>, previews: HashMap<String, PreviewColumn>) {
    let mut all = PREVIEWS.lock().unwrap_or_else(PoisonError::into_inner);
    all.retain(|(table, _)| table.strong_count() > 0);
    all.push((Arc::downgrade(table), previews));
}

/// The preview columns of `table`; none for a table that is not the result of
/// `ApplyPreviewColumns`. A table derived from such a result has none either: in Java its
/// columns keep their class and lose their description.
pub fn previews_of(table: &dyn Table) -> HashMap<String, PreviewColumn> {
    let wanted: *const () = (table as *const dyn Table).cast();
    let all = PREVIEWS.lock().unwrap_or_else(PoisonError::into_inner);
    all.iter()
        .find_map(|(candidate, previews)| {
            // a dropped table's address may belong to another table by now
            let live = candidate.upgrade()?;
            (Arc::as_ptr(&live).cast::<()>() == wanted).then(|| previews.clone())
        })
        .unwrap_or_default()
}

/// Field metadata key holding the Java class name of the elements of an array or vector
/// column. `BarrageUtil.ATTR_DH_PREFIX + ATTR_COMPONENT_TYPE_TAG`.
pub const COMPONENT_TYPE_KEY: &str = "deephaven:componentType";

pub(crate) const OBJECT_VECTOR: &str = "io.deephaven.vector.ObjectVector";

/// The Java vector class of a primitive element type, or `None` for an element that Java
/// holds in an `ObjectVector`.
///
/// Behavior: the classes `VectorExpansionKernel#getComponentType` tells apart
fn primitive_vector_name(element: &ColumnType) -> Option<&'static str> {
    Some(match element {
        ColumnType::Byte => "io.deephaven.vector.ByteVector",
        ColumnType::Char => "io.deephaven.vector.CharVector",
        ColumnType::Short => "io.deephaven.vector.ShortVector",
        ColumnType::Int => "io.deephaven.vector.IntVector",
        ColumnType::Long => "io.deephaven.vector.LongVector",
        ColumnType::Float => "io.deephaven.vector.FloatVector",
        ColumnType::Double => "io.deephaven.vector.DoubleVector",
        _ => return None,
    })
}

/// The Java class name clients see for a column type, as `Class#getCanonicalName` prints it:
/// `int`, `java.lang.String`, `io.deephaven.vector.IntVector`, `java.lang.String[]`.
///
/// Behavior: `BarrageUtil#columnDefinitionsToFields` (the `deephaven:type` entry)
pub fn java_type_name(column_type: &ColumnType) -> Cow<'static, str> {
    Cow::Borrowed(match column_type {
        ColumnType::Boolean => JAVA_BOOLEAN,
        ColumnType::Byte => "byte",
        ColumnType::Char => "char",
        ColumnType::Short => "short",
        ColumnType::Int => "int",
        ColumnType::Long => "long",
        ColumnType::Float => "float",
        ColumnType::Double => "double",
        ColumnType::String => "java.lang.String",
        ColumnType::Instant => "java.time.Instant",
        ColumnType::Vector(element) => primitive_vector_name(element).unwrap_or(OBJECT_VECTOR),
        ColumnType::Array(element) => {
            return Cow::Owned(format!("{}[]", java_type_name(element)));
        }
        other => oxhaven_contracts::unported!(
            "io.deephaven.extensions.barrage.util.BarrageUtil#columnDefinitionsToFields",
            "Java class name of column type {other:?}"
        ),
    })
}

/// The Java class name of the elements of an array or vector column, which clients see as
/// `deephaven:componentType`; `None` for a column of any other type. For a vector of a
/// primitive it is the primitive, as `ColumnDefinition#fromGenericType` infers it.
///
/// Behavior: `BarrageUtil#columnDefinitionsToFields` (the `deephaven:componentType` entry);
/// every element type here is one `BarrageUtil#isTypeNativelySupported` accepts
pub fn java_component_type_name(column_type: &ColumnType) -> Option<Cow<'static, str>> {
    column_type.list_element().map(java_type_name)
}

/// The column type of a field whose `deephaven:type` is `name` and whose
/// `deephaven:componentType` is `component`, if it has one: the inverse of
/// [`java_type_name`] and [`java_component_type_name`], also for arrays and vectors.
///
/// `None` for a class no column type stands for, and for a vector whose elements this
/// cannot name: an `ObjectVector` without a component type, and one whose component is
/// again an `ObjectVector`, where Java itself does not know the inner elements.
///
/// Behavior: `BarrageUtil#convertArrowSchema`, `BarrageUtil#getDefaultType`
pub fn column_type_for_java_names(name: &str, component: Option<&str>) -> Option<ColumnType> {
    if let Some(element) = name.strip_suffix("[]") {
        // An array's component is its element class; Java refuses any other.
        if component.is_some_and(|component| component != element) {
            return None;
        }
        return column_type_for_java_names(element, None).map(ColumnType::array);
    }
    if name == OBJECT_VECTOR {
        let element = column_type_for_java_names(component?, None)?;
        // A primitive in an ObjectVector is the boxed class, which no column type is.
        return primitive_vector_name(&element)
            .is_none()
            .then(|| ColumnType::vector(element));
    }
    let primitive = [
        ColumnType::Byte,
        ColumnType::Char,
        ColumnType::Short,
        ColumnType::Int,
        ColumnType::Long,
        ColumnType::Float,
        ColumnType::Double,
    ]
    .into_iter()
    .find(|element| primitive_vector_name(element) == Some(name));
    if let Some(element) = primitive {
        let agrees = component.is_none_or(|component| component == java_type_name(&element));
        return agrees.then(|| ColumnType::vector(element));
    }
    column_type_for_java_name(name)
}

/// Whether Java can sort a column of this type, which clients see as
/// `deephaven:isSortable`: a primitive, a `Comparable`, which every vector is, or a class
/// with a registered comparator, which the arrays of primitives, `String` and `Instant`
/// are and an array of `Boolean`, of arrays or of vectors is not.
///
/// Behavior: `BarrageUtil#isDataTypeSortable`, `io.deephaven.vector.Vector` (which
/// extends `Comparable`), `io.deephaven.engine.table.impl.ComparatorRegistry#reset`
fn is_sortable(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Array(element) => matches!(
            **element,
            ColumnType::Byte
                | ColumnType::Char
                | ColumnType::Short
                | ColumnType::Int
                | ColumnType::Long
                | ColumnType::Float
                | ColumnType::Double
                | ColumnType::String
                | ColumnType::Instant
        ),
        _ => true,
    }
}

/// The inverse of [`java_type_name`] for the types that are not arrays or vectors, for
/// schemas a client sends; see [`column_type_for_java_names`] for all of them.
pub fn column_type_for_java_name(name: &str) -> Option<ColumnType> {
    Some(match name {
        JAVA_BOOLEAN => ColumnType::Boolean,
        "byte" => ColumnType::Byte,
        "char" => ColumnType::Char,
        "short" => ColumnType::Short,
        "int" => ColumnType::Int,
        "long" => ColumnType::Long,
        "float" => ColumnType::Float,
        "double" => ColumnType::Double,
        "java.lang.String" => ColumnType::String,
        "java.time.Instant" => ColumnType::Instant,
        _ => return None,
    })
}

fn put(metadata: &mut HashMap<String, String>, key: &str, value: &str) {
    metadata.insert(format!("{METADATA_PREFIX}{key}"), value.to_owned());
}

const STYLE_SUFFIX: &str = "__TABLE_STYLE_FORMAT";
const NUMBER_SUFFIX: &str = "__TABLE_NUMBER_FORMAT";
const DATE_SUFFIX: &str = "__TABLE_DATE_FORMAT";
const ROW_STYLE_COLUMN: &str = "__ROW__TABLE_STYLE_FORMAT";

/// Schema-level metadata.
///
/// Behavior: `BarrageUtil#attributesToMetadata`
fn schema_metadata(context: &SchemaContext) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    let mut flag = |name: &str, set: bool| {
        if set {
            put(&mut metadata, &format!("attribute.{name}"), TRUE);
            put(
                &mut metadata,
                &format!("attribute_type.{name}"),
                JAVA_BOOLEAN,
            );
        }
    };
    flag("IsFlat", context.is_flat);
    flag("BlinkTable", context.attributes.blink);
    flag("AddOnly", context.attributes.add_only);
    flag("AppendOnly", context.attributes.append_only);
    if context.input_table_keys.is_some() {
        // The value is the updater object, which Java cannot send and marks as such.
        put(&mut metadata, "unsent.attribute.InputTable", "");
    }
    metadata
}

/// The value of `deephaven:tableMetadata` for an input table with these key columns: a
/// `DeephavenTableMetadata` message, base64 encoded, that says of every column whether it
/// is a key or a value.
///
/// The message is written by hand because its map must list the columns in the table's
/// order, as Java's does, and a generated map does not keep an order.
///
/// Behavior: `BarrageUtil#maybeAddInputTableMetadata`
fn input_table_metadata(definition: &TableDefinition, keys: &[String]) -> String {
    use base64::Engine;
    use oxhaven_proto::grpc::input_table_column_info::Kind;
    use prost::encoding::encode_varint;

    const LENGTH_DELIMITED: u8 = 2;
    fn field(number: u8, payload: &[u8], out: &mut Vec<u8>) {
        out.push(number << 3 | LENGTH_DELIMITED);
        encode_varint(payload.len() as u64, out);
        out.extend_from_slice(payload);
    }

    // InputTableMetadata: `map<string, InputTableColumnInfo> column_info = 1`
    let mut column_info = Vec::new();
    for column in definition.columns() {
        let kind = if keys.contains(&column.name) {
            Kind::Key
        } else {
            Kind::Value
        };
        // InputTableColumnInfo: `Kind kind = 1`, a varint
        let info = [1 << 3, kind as u8];
        let mut entry = Vec::new();
        field(1, column.name.as_bytes(), &mut entry);
        field(2, &info, &mut entry);
        field(1, &entry, &mut column_info);
    }
    // DeephavenTableMetadata: `InputTableMetadata input_table_metadata = 1`
    let mut message = Vec::new();
    field(1, &column_info, &mut message);
    base64::engine::general_purpose::STANDARD.encode(message)
}

/// The Arrow schema Java sends for a table with this definition.
///
/// Every field is nullable and carries `deephaven:type` and `deephaven:isSortable`; format
/// columns and input table key and value columns are marked as Java marks them. An array or
/// vector column also carries `deephaven:componentType`, and its field is a list whose
/// element field has the empty name, is nullable and has no metadata. The Arrow type of
/// a field is [`crate::wire_columns::wire_type`] of its column type, which is not the type
/// the engine stores for a `byte[]` and for a vector of vectors of objects.
///
/// An input table also carries `deephaven:tableMetadata`.
///
/// Not emitted: column descriptions, and restrictions on the values of an input table's
/// columns, neither of which a table in this engine can carry yet.
///
/// Behavior: `BarrageUtil#makeSchema`, `BarrageUtil#columnDefinitionsToFields`,
/// `BarrageUtil#arrowFieldFor`
pub fn arrow_schema(definition: &TableDefinition, context: &SchemaContext) -> Schema {
    let columns = definition.columns();
    let has_column = |name: &str| columns.iter().any(|c| c.name == name);
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let name = column.name.as_str();
            let mut metadata = HashMap::new();
            let preview = context.previews.get(name);
            // Java writes the entry only when it is true. A preview class is not
            // `Comparable` and has no comparator.
            if preview.is_none() && is_sortable(&column.column_type) {
                put(&mut metadata, "isSortable", TRUE);
            }
            for (key, suffix) in [
                ("styleColumn", STYLE_SUFFIX),
                ("numberFormatColumn", NUMBER_SUFFIX),
                ("dateFormatColumn", DATE_SUFFIX),
            ] {
                let format_column = format!("{name}{suffix}");
                if has_column(&format_column) {
                    put(&mut metadata, key, &format_column);
                }
            }
            match preview {
                // Java sends a class it has no Arrow type for as the text of its values
                Some(preview) => {
                    put(&mut metadata, "type", preview.java_type);
                    put(&mut metadata, "description", &preview.description);
                }
                None => {
                    put(&mut metadata, "type", &java_type_name(&column.column_type));
                    if let Some(component) = java_component_type_name(&column.column_type) {
                        put(&mut metadata, "componentType", &component);
                    }
                }
            }
            if name == ROW_STYLE_COLUMN {
                put(&mut metadata, "isRowStyle", TRUE);
            }
            for (key, suffix) in [
                ("isStyle", STYLE_SUFFIX),
                ("isNumberFormat", NUMBER_SUFFIX),
                ("isDateFormat", DATE_SUFFIX),
            ] {
                if name.ends_with(suffix) {
                    put(&mut metadata, key, TRUE);
                }
            }
            if let Some(keys) = &context.input_table_keys {
                let key = if keys.iter().any(|k| k == name) {
                    "inputtable.isKey"
                } else {
                    "inputtable.isValue"
                };
                put(&mut metadata, key, TRUE);
            }
            Field::new(name, wire_type(&column.column_type), true).with_metadata(metadata)
        })
        .collect();
    let mut metadata = schema_metadata(context);
    if let Some(keys) = &context.input_table_keys {
        let value = input_table_metadata(definition, keys);
        put(&mut metadata, "tableMetadata", &value);
    }
    Schema::new_with_metadata(fields, metadata)
}

/// [`arrow_schema`] for a table.
pub fn table_schema(table: &dyn Table) -> Schema {
    arrow_schema(table.definition(), &SchemaContext::of(table))
}

/// The flatbuffer `Message` (version V5, header `Schema`, no body) for `schema`, without
/// the IPC framing. This is the `data_header` of a Flight schema message.
///
/// Behavior: `BarrageUtil#schemaBytes` (the `MessageHelper#wrapInMessage` step)
pub fn schema_message(schema: &Schema) -> Vec<u8> {
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options)
        .ipc_message
}

/// `schema` as an encapsulated Arrow IPC schema message: the continuation token, the
/// little-endian length of the message padded to a multiple of 8, the message, the padding.
/// These are the bytes of `ExportedTableCreationResponse.schema_header` and of
/// `SchemaResult.schema`.
///
/// Behavior: `MessageHelper#toIpcBytes`
pub fn schema_ipc_bytes(schema: &Schema) -> Vec<u8> {
    frame_ipc_message(&schema_message(schema))
}

/// Frames a flatbuffer `Message` as an encapsulated IPC message.
fn frame_ipc_message(message: &[u8]) -> Vec<u8> {
    let padded = message.len().div_ceil(8) * 8;
    let mut bytes = Vec::with_capacity(8 + padded);
    bytes.extend_from_slice(&IPC_CONTINUATION_TOKEN);
    bytes.extend_from_slice(&(padded as u32).to_le_bytes());
    bytes.extend_from_slice(message);
    bytes.resize(8 + padded, 0);
    bytes
}

/// [`schema_ipc_bytes`] of [`table_schema`].
pub fn table_schema_ipc_bytes(table: &dyn Table) -> Vec<u8> {
    schema_ipc_bytes(&table_schema(table))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use arrow::ipc::reader::StreamReader;
    use oxhaven_contracts::schema::ColumnDefinition;

    fn definition(columns: &[(&str, ColumnType)]) -> TableDefinition {
        TableDefinition::new(
            columns
                .iter()
                .map(|(name, column_type)| ColumnDefinition::new(*name, column_type.clone()))
                .collect(),
        )
        .unwrap()
    }

    fn metadata(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn fields_are_nullable_and_carry_the_java_type() {
        let schema = arrow_schema(
            &definition(&[
                ("B", ColumnType::Boolean),
                ("C", ColumnType::Char),
                ("I", ColumnType::Int),
                ("S", ColumnType::String),
                ("T", ColumnType::Instant),
            ]),
            &SchemaContext::default(),
        );
        let seen: Vec<_> = schema
            .fields()
            .iter()
            .map(|f| {
                (
                    f.name().as_str(),
                    f.data_type().clone(),
                    f.is_nullable(),
                    f.metadata()[TYPE_KEY].as_str(),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                ("B", DataType::Boolean, true, "java.lang.Boolean"),
                ("C", DataType::UInt16, true, "char"),
                ("I", DataType::Int32, true, "int"),
                ("S", DataType::Utf8, true, "java.lang.String"),
                (
                    "T",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    true,
                    "java.time.Instant"
                ),
            ]
        );
        assert_eq!(
            schema.field(2).metadata(),
            &metadata(&[("deephaven:isSortable", "true"), ("deephaven:type", "int")])
        );
        assert!(schema.metadata().is_empty());
    }

    #[test]
    fn java_type_names_round_trip() {
        for column_type in [
            ColumnType::Boolean,
            ColumnType::Byte,
            ColumnType::Char,
            ColumnType::Short,
            ColumnType::Int,
            ColumnType::Long,
            ColumnType::Float,
            ColumnType::Double,
            ColumnType::String,
            ColumnType::Instant,
        ] {
            assert_eq!(
                column_type_for_java_name(&java_type_name(&column_type)),
                Some(column_type)
            );
        }
        assert_eq!(column_type_for_java_name("java.math.BigDecimal"), None);
    }

    #[test]
    fn attributes_become_schema_metadata() {
        let mut attributes = TableAttributes::default();
        attributes.blink = true;
        let context = SchemaContext {
            attributes,
            is_flat: true,
            ..Default::default()
        };
        let schema = arrow_schema(&definition(&[("X", ColumnType::Long)]), &context);
        assert_eq!(
            schema.metadata(),
            &metadata(&[
                ("deephaven:attribute.IsFlat", "true"),
                ("deephaven:attribute_type.IsFlat", "java.lang.Boolean"),
                ("deephaven:attribute.BlinkTable", "true"),
                ("deephaven:attribute_type.BlinkTable", "java.lang.Boolean"),
            ])
        );

        let mut attributes = TableAttributes::default();
        attributes.add_only = true;
        attributes.append_only = true;
        let context = SchemaContext {
            attributes,
            ..Default::default()
        };
        let schema = arrow_schema(&definition(&[("X", ColumnType::Long)]), &context);
        assert_eq!(
            schema.metadata(),
            &metadata(&[
                ("deephaven:attribute.AddOnly", "true"),
                ("deephaven:attribute_type.AddOnly", "java.lang.Boolean"),
                ("deephaven:attribute.AppendOnly", "true"),
                ("deephaven:attribute_type.AppendOnly", "java.lang.Boolean"),
            ])
        );
    }

    #[test]
    fn input_table_columns_are_marked_key_or_value() {
        let context = SchemaContext {
            input_table_keys: Some(vec!["K".to_owned()]),
            ..Default::default()
        };
        let schema = arrow_schema(
            &definition(&[("K", ColumnType::String), ("V", ColumnType::Int)]),
            &context,
        );
        assert_eq!(
            schema.field(0).metadata(),
            &metadata(&[
                ("deephaven:isSortable", "true"),
                ("deephaven:type", "java.lang.String"),
                ("deephaven:inputtable.isKey", "true"),
            ])
        );
        assert_eq!(
            schema.field(1).metadata(),
            &metadata(&[
                ("deephaven:isSortable", "true"),
                ("deephaven:type", "int"),
                ("deephaven:inputtable.isValue", "true"),
            ])
        );
        assert_eq!(
            schema.metadata(),
            &metadata(&[
                ("deephaven:unsent.attribute.InputTable", ""),
                // DeephavenTableMetadata { input_table_metadata { K: key, V: value } }
                ("deephaven:tableMetadata", "ChIKBwoBSxICCAEKBwoBVhICCAI="),
            ])
        );
    }

    #[test]
    fn format_columns_are_linked_to_their_base_column() {
        let schema = arrow_schema(
            &definition(&[
                ("Price", ColumnType::Double),
                ("Price__TABLE_STYLE_FORMAT", ColumnType::Long),
                ("Price__TABLE_NUMBER_FORMAT", ColumnType::String),
                ("__ROW__TABLE_STYLE_FORMAT", ColumnType::Long),
            ]),
            &SchemaContext::default(),
        );
        assert_eq!(
            schema.field(0).metadata(),
            &metadata(&[
                ("deephaven:isSortable", "true"),
                ("deephaven:type", "double"),
                ("deephaven:styleColumn", "Price__TABLE_STYLE_FORMAT"),
                ("deephaven:numberFormatColumn", "Price__TABLE_NUMBER_FORMAT"),
            ])
        );
        assert_eq!(
            schema.field(1).metadata(),
            &metadata(&[
                ("deephaven:isSortable", "true"),
                ("deephaven:type", "long"),
                ("deephaven:isStyle", "true"),
            ])
        );
        assert_eq!(
            schema.field(3).metadata(),
            &metadata(&[
                ("deephaven:isSortable", "true"),
                ("deephaven:type", "long"),
                ("deephaven:isRowStyle", "true"),
                ("deephaven:isStyle", "true"),
            ])
        );
    }

    #[test]
    fn a_framed_message_is_padded_to_eight_bytes_and_its_length_counts_the_padding() {
        assert_eq!(
            frame_ipc_message(&[1, 2, 3, 4, 5]),
            vec![0xff, 0xff, 0xff, 0xff, 8, 0, 0, 0, 1, 2, 3, 4, 5, 0, 0, 0]
        );
        assert_eq!(
            frame_ipc_message(&[9; 8]),
            [&[0xff, 0xff, 0xff, 0xff, 8, 0, 0, 0][..], &[9; 8][..]].concat()
        );
        assert_eq!(
            frame_ipc_message(&[]),
            vec![0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0]
        );
    }

    #[test]
    fn ipc_bytes_are_one_framed_schema_message_a_stream_reader_decodes() {
        let context = SchemaContext {
            is_flat: true,
            ..Default::default()
        };
        let schema = arrow_schema(
            &definition(&[("A", ColumnType::Int), ("T", ColumnType::Instant)]),
            &context,
        );
        let bytes = schema_ipc_bytes(&schema);

        assert_eq!(bytes[..4], [0xff; 4]);
        let length = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        assert_eq!(length % 8, 0);
        assert_eq!(bytes.len(), 8 + length);
        let message = schema_message(&schema);
        assert_eq!(bytes[8..8 + message.len()], message[..]);

        let message = arrow::ipc::root_as_message(&bytes[8..]).unwrap();
        assert_eq!(message.version(), arrow::ipc::MetadataVersion::V5);
        assert_eq!(message.header_type(), arrow::ipc::MessageHeader::Schema);
        assert_eq!(message.bodyLength(), 0);

        let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        assert_eq!(reader.schema().as_ref(), &schema);
    }
}
