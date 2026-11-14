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

use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableAttributes};
use std::collections::HashMap;

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
}

impl SchemaContext {
    pub fn of(table: &dyn Table) -> Self {
        Self {
            attributes: table.attributes(),
            is_flat: table.is_flat(),
            input_table_keys: table.input_table().map(|input| input.key_columns()),
        }
    }
}

/// The Java class name clients see for a column type, as `Class#getCanonicalName` prints it.
///
/// Behavior: `BarrageUtil#columnDefinitionsToFields` (the `deephaven:type` entry)
pub fn java_type_name(column_type: &ColumnType) -> &'static str {
    match column_type {
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
        other => oxhaven_contracts::unported!(
            "io.deephaven.extensions.barrage.util.BarrageUtil#columnDefinitionsToFields",
            "Java class name of column type {other:?}"
        ),
    }
}

/// The inverse of [`java_type_name`], for schemas a client sends.
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
            put(&mut metadata, &format!("attribute_type.{name}"), JAVA_BOOLEAN);
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

/// The Arrow schema Java sends for a table with this definition.
///
/// Every field is nullable and carries `deephaven:type` and `deephaven:isSortable`; format
/// columns and input table key and value columns are marked as Java marks them.
///
/// Not emitted: `deephaven:tableMetadata` (the protobuf `DeephavenTableMetadata` Java adds
/// for input tables), column descriptions, and component types, none of which a table in
/// this engine can carry yet.
///
/// Behavior: `BarrageUtil#makeSchema`, `BarrageUtil#columnDefinitionsToFields`
pub fn arrow_schema(definition: &TableDefinition, context: &SchemaContext) -> Schema {
    let columns = definition.columns();
    let has_column = |name: &str| columns.iter().any(|c| c.name == name);
    let fields: Vec<Field> = columns
        .iter()
        .map(|column| {
            let name = column.name.as_str();
            let mut metadata = HashMap::new();
            // Every supported column type is a Java primitive or Comparable.
            put(&mut metadata, "isSortable", TRUE);
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
            put(&mut metadata, "type", java_type_name(&column.column_type));
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
            Field::new(name, column.column_type.arrow_type(), true).with_metadata(metadata)
        })
        .collect();
    Schema::new_with_metadata(fields, schema_metadata(context))
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
    let message = schema_message(schema);
    let padded = message.len().div_ceil(8) * 8;
    let mut bytes = Vec::with_capacity(8 + padded);
    bytes.extend_from_slice(&IPC_CONTINUATION_TOKEN);
    bytes.extend_from_slice(&(padded as u32).to_le_bytes());
    bytes.extend_from_slice(&message);
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
                column_type_for_java_name(java_type_name(&column_type)),
                Some(column_type)
            );
        }
        assert_eq!(column_type_for_java_name("java.math.BigDecimal"), None);
    }

    #[test]
    fn attributes_become_schema_metadata() {
        let mut attributes = TableAttributes::default();
        attributes.blink = true;
        let mut context = SchemaContext::default();
        context.attributes = attributes;
        context.is_flat = true;
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
        let mut context = SchemaContext::default();
        context.attributes = attributes;
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
        let mut context = SchemaContext::default();
        context.input_table_keys = Some(vec!["K".to_owned()]);
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
            &metadata(&[("deephaven:unsent.attribute.InputTable", "")])
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
    fn ipc_bytes_are_one_framed_schema_message_a_stream_reader_decodes() {
        let mut context = SchemaContext::default();
        context.is_flat = true;
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
