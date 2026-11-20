//! The schema a server describes a table with, read as a client reads it: the table's
//! definition, its attributes, and which of its columns this client can read.

use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::TableAttributes;

use super::error::{not_ported, protocol, ClientError};
use crate::schema::{COMPONENT_TYPE_KEY, METADATA_PREFIX, OBJECT_VECTOR, TYPE_KEY};

const NEW_READER: &str =
    "io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory#newReader";
const CONVERT_ARROW_SCHEMA: &str =
    "io.deephaven.extensions.barrage.util.BarrageUtil#convertArrowSchema";

/// An attribute of a remote table as its schema carries it: the text of the value and the
/// Java class the server says it has.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RemoteAttribute {
    pub value: String,
    /// `None` when the schema has the value and no `deephaven:attribute_type` entry.
    pub java_type: Option<String>,
}

/// What a server said about a table when it was fetched.
///
/// Port of `io.deephaven.extensions.barrage.util.BarrageUtil.ConvertedArrowSchema`.
#[derive(Clone, Debug)]
pub struct RemoteSchema {
    pub definition: TableDefinition,
    /// The attributes the schema carries, by name (`BlinkTable`, `AddOnly`, `SortedColumns`,
    /// ...). Attributes the server could not send (`deephaven:unsent.attribute.*`) are not
    /// here.
    pub attributes: BTreeMap<String, RemoteAttribute>,
    /// The schema as the server sent it.
    pub arrow: Schema,
}

impl RemoteSchema {
    /// Whether the attribute `name` is the boolean `true`.
    ///
    /// Behavior: the `getAttribute` predicate of
    /// `io.deephaven.extensions.barrage.table.BarrageTable#make`, over the attributes
    /// `BarrageUtil#convertArrowSchema` parsed: only a value declared as
    /// `java.lang.Boolean` is one, and `Boolean.valueOf` reads `true` in any case
    pub fn flag(&self, name: &str) -> bool {
        self.attributes.get(name).is_some_and(|attribute| {
            attribute.java_type.as_deref() == Some("java.lang.Boolean")
                && attribute.value.eq_ignore_ascii_case("true")
        })
    }

    /// Whether the remote table is a blink table.
    pub fn is_blink(&self) -> bool {
        self.flag("BlinkTable")
    }

    /// Whether the remote table says its row set is always `0..size`.
    ///
    /// Behavior: `BarrageUtil#TABLE_ATTRIBUTE_IS_FLAT`
    pub fn is_flat(&self) -> bool {
        self.flag("IsFlat")
    }

    /// The attributes a local table of this schema carries.
    ///
    /// Java sets every attribute of the schema on the `BarrageTable`. A table here has a
    /// place for these three and for `IsFlat`; the others are in
    /// [`RemoteSchema::attributes`] and are not attributes of the local table.
    ///
    /// Behavior: the constructor of `io.deephaven.extensions.barrage.table.BarrageTable`
    pub fn table_attributes(&self) -> TableAttributes {
        let mut attributes = TableAttributes::default();
        attributes.blink = self.is_blink();
        attributes.add_only = self.flag("AddOnly");
        attributes.append_only = self.flag("AppendOnly");
        attributes
    }

    /// Reads the schema of a table a server exported.
    ///
    /// An error names the first column this client cannot read; see [`column_type_of`].
    ///
    /// Behavior: `BarrageUtil#convertArrowSchema`
    pub fn of(arrow: Schema) -> Result<Self, ClientError> {
        let mut columns = Vec::with_capacity(arrow.fields().len());
        for field in arrow.fields() {
            if !is_plain_identifier(field.name()) {
                return Err(not_ported(
                    "io.deephaven.api.util.NameValidator#legalizeColumnName",
                    format_args!("column name '{}'", field.name()),
                ));
            }
            let column_type = column_type_of(field)?;
            columns.push(ColumnDefinition::new(field.name().clone(), column_type));
        }
        let definition = TableDefinition::new(columns).map_err(protocol)?;
        let prefix = |tag: &str| format!("{METADATA_PREFIX}{tag}.");
        let (value_prefix, type_prefix) = (prefix("attribute"), prefix("attribute_type"));
        let mut attributes = BTreeMap::new();
        for (key, value) in arrow.metadata() {
            if let Some(name) = key.strip_prefix(&value_prefix) {
                attributes.insert(
                    name.to_owned(),
                    RemoteAttribute {
                        value: value.clone(),
                        java_type: arrow
                            .metadata()
                            .get(&format!("{type_prefix}{name}"))
                            .cloned(),
                    },
                );
            }
        }
        Ok(RemoteSchema {
            definition,
            attributes,
            arrow,
        })
    }
}

/// Whether a column name needs no change to be a legal one. Java renames the others.
fn is_plain_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && characters.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// The column type of a Java class this client reads when it is not an array or a vector.
/// `element` says the class is that of the elements of an array, where `boolean` is the
/// primitive; as the class of a column Java boxes it.
fn scalar(name: &str, element: bool) -> Option<ColumnType> {
    Some(match name {
        "java.lang.Boolean" => ColumnType::Boolean,
        "boolean" if element => ColumnType::PrimitiveBoolean,
        "boolean" => ColumnType::Boolean,
        "byte" => ColumnType::Byte,
        "char" => ColumnType::Char,
        "short" => ColumnType::Short,
        "int" => ColumnType::Int,
        "long" => ColumnType::Long,
        "float" => ColumnType::Float,
        "double" => ColumnType::Double,
        "java.lang.String" => ColumnType::String,
        "java.time.Instant" => ColumnType::Instant,
        "java.time.ZonedDateTime" => ColumnType::ZonedDateTime,
        "java.time.LocalDate" => ColumnType::LocalDate,
        "java.time.LocalTime" => ColumnType::LocalTime,
        "java.time.Duration" => ColumnType::Duration,
        "java.time.Period" => ColumnType::Period,
        "java.math.BigDecimal" => ColumnType::BigDecimal,
        "java.math.BigInteger" => ColumnType::BigInteger,
        _ => return None,
    })
}

fn primitive_vector(name: &str) -> Option<ColumnType> {
    Some(match name {
        "io.deephaven.vector.ByteVector" => ColumnType::Byte,
        "io.deephaven.vector.CharVector" => ColumnType::Char,
        "io.deephaven.vector.ShortVector" => ColumnType::Short,
        "io.deephaven.vector.IntVector" => ColumnType::Int,
        "io.deephaven.vector.LongVector" => ColumnType::Long,
        "io.deephaven.vector.FloatVector" => ColumnType::Float,
        "io.deephaven.vector.DoubleVector" => ColumnType::Double,
        _ => return None,
    })
}

/// The column type the Java class `name` stands for, with `component` as the class of its
/// elements where the class does not say.
fn type_named(
    field: &Field,
    name: &str,
    component: Option<&str>,
    element: bool,
) -> Result<ColumnType, ClientError> {
    if let Some(inner) = name.strip_suffix("[]") {
        return Ok(ColumnType::array(type_named(field, inner, None, true)?));
    }
    if let Some(primitive) = primitive_vector(name) {
        return Ok(ColumnType::vector(primitive));
    }
    if name == OBJECT_VECTOR {
        // Java knows the elements of an ObjectVector only from the component type of the
        // column, so one that is itself an element has elements of `Object`, which it
        // has no reader for.
        let Some(component) = component.filter(|_| !element) else {
            return Err(ClientError::NotReadable(format!(
                "Column '{}' has an io.deephaven.vector.ObjectVector whose elements are not \
                 named; Java reads them as java.lang.Object, which cannot be serialized \
                 directly ({NEW_READER})",
                field.name()
            )));
        };
        return Ok(ColumnType::vector(type_named(
            field, component, None, true,
        )?));
    }
    scalar(name, element).ok_or_else(|| {
        not_ported(
            CONVERT_ARROW_SCHEMA,
            format_args!("field '{}' of type {name}", field.name()),
        )
    })
}

/// The column type Java gives a field that names none.
///
/// Behavior: `BarrageUtil#getDefaultType(Field, Class)`
fn default_type(field: &Field) -> Result<ColumnType, ClientError> {
    Ok(match field.data_type() {
        DataType::Boolean => ColumnType::Boolean,
        DataType::Int8 => ColumnType::Byte,
        DataType::Int16 => ColumnType::Short,
        DataType::Int32 => ColumnType::Int,
        DataType::Int64 => ColumnType::Long,
        DataType::UInt16 => ColumnType::Char,
        DataType::Float32 => ColumnType::Float,
        DataType::Float64 => ColumnType::Double,
        DataType::Utf8 => ColumnType::String,
        DataType::Timestamp(_, zone) if zone.as_deref().is_none_or(|zone| zone == "UTC") => {
            ColumnType::Instant
        }
        DataType::Duration(_) => ColumnType::Duration,
        DataType::Time32(_) | DataType::Time64(_) => ColumnType::LocalTime,
        DataType::Date32 | DataType::Date64 => ColumnType::LocalDate,
        DataType::Interval(IntervalUnit::YearMonth) => ColumnType::Period,
        DataType::Binary => ColumnType::array(ColumnType::Byte),
        DataType::List(element) => ColumnType::array(default_type(element)?),
        other => {
            return Err(not_ported(
                "io.deephaven.extensions.barrage.util.BarrageUtil#getDefaultType",
                format_args!("field '{}' of Arrow type {other}", field.name()),
            ))
        }
    })
}

/// The column type of a field of a remote table: the class its `deephaven:type` names,
/// with its `deephaven:componentType` for the elements of an `ObjectVector`, or what the
/// Arrow type says by itself. Then the Arrow type must be one this client reads that
/// column type from.
///
/// Readable are the types the serving side of this crate writes and Java's own client
/// reads back: `Boolean`, the primitives, `String`, `Instant`, `ZonedDateTime`,
/// `LocalDate`, `LocalTime`, `Duration`, `Period`, `BigDecimal`, `BigInteger`, and arrays
/// and vectors of them at any depth (`byte[]` from `Binary`).
///
/// A column a server sends as text because Java has no Arrow type for its class
/// (`LocalDateTime`, `ZoneId`, `DayOfWeek`, `Object`, a table, a vector that is itself
/// the element of a vector) is refused: Java's client refuses it with the same words.
///
/// Behavior: `BarrageUtil#convertArrowSchema`, `DefaultChunkReaderFactory#newReaderPojo`
pub fn column_type_of(field: &Field) -> Result<ColumnType, ClientError> {
    let metadata = |key| field.metadata().get(key).map(String::as_str);
    let column_type = match metadata(TYPE_KEY) {
        None => default_type(field)?,
        Some(name) => {
            match type_named(field, name, metadata(COMPONENT_TYPE_KEY), false) {
                Ok(column_type) => column_type,
                // Java knows the class and has no reader from text to it
                Err(ClientError::NotReadable(_)) if *field.data_type() == DataType::Utf8 => {
                    return Err(ClientError::NotReadable(format!(
                        "Column '{}' with type {name} cannot be serialized directly. Consider \
                         an updateView() expression to convert to String or registering \
                         ChunkReaderFactory/ChunkWriterFactory types for custom \
                         serialization. ({NEW_READER})",
                        field.name()
                    )));
                }
                Err(other) => return Err(other),
            }
        }
    };
    check_readable(field, &column_type, field.data_type())?;
    Ok(column_type)
}

/// Whether this client reads cells of `column_type` from `wire`; the conversion itself is
/// [`super::decode::to_stored`].
///
/// Behavior: `DefaultChunkReaderFactory#newReaderPojo`, which has readers for many more
/// pairs (wider and narrower numbers, other time units, decimals, dictionaries, unions,
/// maps); those are refused here by name
pub(super) fn check_readable(
    field: &Field,
    column_type: &ColumnType,
    wire: &DataType,
) -> Result<(), ClientError> {
    let nanos = TimeUnit::Nanosecond;
    let readable = match (column_type, wire) {
        (ColumnType::Boolean | ColumnType::PrimitiveBoolean, DataType::Boolean)
        | (ColumnType::Byte, DataType::Int8)
        | (ColumnType::Char, DataType::UInt16)
        | (ColumnType::Short, DataType::Int16)
        | (ColumnType::Int, DataType::Int32)
        | (ColumnType::Long, DataType::Int64)
        | (ColumnType::Float, DataType::Float32)
        | (ColumnType::Double, DataType::Float64)
        | (ColumnType::String, DataType::Utf8)
        | (ColumnType::LocalDate, DataType::Date32 | DataType::Date64)
        | (ColumnType::Period, DataType::Interval(IntervalUnit::YearMonth))
        | (ColumnType::BigDecimal | ColumnType::BigInteger, DataType::Binary) => true,
        (ColumnType::Instant, DataType::Timestamp(unit, _)) => *unit == nanos,
        // Java takes the zone of the cells from the field; without one it uses the zone
        // of the machine it runs on, which is not guessed here.
        (ColumnType::ZonedDateTime, DataType::Timestamp(unit, Some(zone))) => {
            *unit == nanos && zone.as_ref() == "UTC"
        }
        (ColumnType::LocalTime, DataType::Time64(unit)) => *unit == nanos,
        (ColumnType::Duration, DataType::Duration(unit)) => *unit == nanos,
        (ColumnType::Array(element), DataType::Binary) => **element == ColumnType::Byte,
        (ColumnType::Array(element) | ColumnType::Vector(element), DataType::List(sent)) => {
            return check_readable(field, element, sent.data_type());
        }
        _ => false,
    };
    if readable {
        Ok(())
    } else {
        Err(not_ported(
            NEW_READER,
            format_args!(
                "field '{}': reading Arrow type {wire} as {column_type:?}",
                field.name()
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{arrow_schema, SchemaContext};
    use std::collections::HashMap;

    fn field(name: &str, data_type: DataType, class: &str, component: Option<&str>) -> Field {
        let mut metadata = HashMap::from([(TYPE_KEY.to_owned(), class.to_owned())]);
        if let Some(component) = component {
            metadata.insert(COMPONENT_TYPE_KEY.to_owned(), component.to_owned());
        }
        Field::new(name, data_type, true).with_metadata(metadata)
    }

    /// Every type the client reads, described as the serving side of this crate describes
    /// it, comes back as the same column type.
    #[test]
    fn the_schema_this_server_sends_is_read_back_as_the_same_definition() {
        let readable = [
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
            ColumnType::ZonedDateTime,
            ColumnType::LocalDate,
            ColumnType::LocalTime,
            ColumnType::Duration,
            ColumnType::Period,
            ColumnType::BigDecimal,
            ColumnType::BigInteger,
            ColumnType::array(ColumnType::Byte),
            ColumnType::array(ColumnType::Int),
            ColumnType::array(ColumnType::PrimitiveBoolean),
            ColumnType::array(ColumnType::array(ColumnType::String)),
            ColumnType::vector(ColumnType::Double),
            ColumnType::vector(ColumnType::String),
            ColumnType::vector(ColumnType::Instant),
            ColumnType::array(ColumnType::vector(ColumnType::Long)),
            ColumnType::vector(ColumnType::array(ColumnType::Int)),
        ];
        let columns = readable
            .iter()
            .enumerate()
            .map(|(index, column_type)| {
                ColumnDefinition::new(format!("C{index}"), column_type.clone())
            })
            .collect();
        let definition = TableDefinition::new(columns).unwrap();
        let mut attributes = TableAttributes::default();
        attributes.blink = true;
        let sent = arrow_schema(
            &definition,
            &SchemaContext {
                attributes,
                is_flat: true,
                ..Default::default()
            },
        );
        let read = RemoteSchema::of(sent).unwrap();
        assert_eq!(read.definition, definition);
        assert!(read.is_blink() && read.is_flat());
        assert!(read.table_attributes().blink && !read.table_attributes().add_only);
    }

    /// The classes Java sends as text and has no reader for are refused in Java's words,
    /// and a class nothing here stands for is refused by name.
    #[test]
    fn columns_java_cannot_read_back_are_refused_by_name() {
        for class in [
            "java.time.LocalDateTime",
            "java.time.ZoneId",
            "java.time.DayOfWeek",
            "java.lang.Object",
            "io.deephaven.engine.table.Table",
        ] {
            let refused = column_type_of(&field("X", DataType::Utf8, class, None)).unwrap_err();
            let text = refused.to_string();
            assert!(
                text.contains(&format!(
                    "Column 'X' with type {class} cannot be serialized directly"
                )),
                "{text}"
            );
        }
        let nested = DataType::List(std::sync::Arc::new(Field::new(
            "",
            DataType::List(std::sync::Arc::new(Field::new("", DataType::Utf8, true))),
            true,
        )));
        let refused =
            column_type_of(&field("V", nested, OBJECT_VECTOR, Some(OBJECT_VECTOR))).unwrap_err();
        assert!(refused.to_string().contains("ObjectVector"), "{refused}");
        let refused = column_type_of(&field("W", DataType::Int64, "int", None)).unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("reading Arrow type Int64 as Int"),
            "{refused}"
        );
        let refused =
            column_type_of(&field("U", DataType::Int32, "com.example.Thing", None)).unwrap_err();
        assert!(
            refused.to_string().contains("com.example.Thing"),
            "{refused}"
        );
    }

    /// Only an attribute the schema declares as a `java.lang.Boolean` of `true` is set.
    #[test]
    fn an_attribute_is_a_flag_only_when_it_is_a_true_boolean() {
        let metadata = HashMap::from([
            ("deephaven:attribute.AddOnly".to_owned(), "TRUE".to_owned()),
            (
                "deephaven:attribute_type.AddOnly".to_owned(),
                "java.lang.Boolean".to_owned(),
            ),
            (
                "deephaven:attribute.BlinkTable".to_owned(),
                "true".to_owned(),
            ),
            (
                "deephaven:attribute_type.BlinkTable".to_owned(),
                "java.lang.String".to_owned(),
            ),
            (
                "deephaven:attribute.AppendOnly".to_owned(),
                "false".to_owned(),
            ),
            (
                "deephaven:attribute_type.AppendOnly".to_owned(),
                "java.lang.Boolean".to_owned(),
            ),
            (
                "deephaven:attribute.SortedColumns".to_owned(),
                "A=Ascending".to_owned(),
            ),
            (
                "deephaven:unsent.attribute.InputTable".to_owned(),
                String::new(),
            ),
        ]);
        let schema =
            Schema::new_with_metadata(vec![field("A", DataType::Int32, "int", None)], metadata);
        let read = RemoteSchema::of(schema).unwrap();
        let attributes = read.table_attributes();
        assert!(attributes.add_only && !attributes.blink && !attributes.append_only);
        assert_eq!(read.attributes["SortedColumns"].value, "A=Ascending");
        assert_eq!(read.attributes["SortedColumns"].java_type, None);
        assert!(!read.attributes.contains_key("InputTable"));
    }
}
