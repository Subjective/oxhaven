//! From the schema of a Parquet file to the columns of a table.

use crate::{not_ported, ReadError, ReadOptions, UnsignedLongTarget};
use oxhaven_contracts::schema::ColumnType;
use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
use parquet::file::metadata::KeyValue;
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};
use std::collections::HashMap;

/// One column of a file, as a table sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileColumn {
    /// The column's name in the table.
    pub name: String,
    /// The index of the column among the fields of the file's schema.
    pub root: usize,
    pub column_type: ColumnType,
    /// How an unsigned 64-bit column becomes a `long`.
    pub unsigned_long: Option<UnsignedLongTarget>,
}

const READ_SCHEMA: &str = "io.deephaven.parquet.table.ParquetSchemaReader#readParquetSchema";
const LEGALIZE: &str = "io.deephaven.api.util.NameValidator#legalizeColumnName";

/// The key of the footer entry Deephaven writes.
///
/// Behavior: `io.deephaven.parquet.table.ParquetTableWriter#METADATA_KEY`
const METADATA_KEY: &str = "deephaven";

/// What the `deephaven` footer entry says of one column.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ColumnInfo {
    special: Option<String>,
    /// The Java type an object codec decodes to.
    codec_type: Option<String>,
}

/// The columns the `deephaven` footer entry describes, by name. Deephaven lists a column
/// there only when the Parquet schema alone does not say its type.
///
/// Behavior: `ParquetSchemaReader#parseMetadata`,
/// `io.deephaven.parquet.table.metadata.TableInfo#columnTypeMap`
fn column_infos(
    key_values: Option<&Vec<KeyValue>>,
) -> Result<HashMap<String, ColumnInfo>, ReadError> {
    let entry = key_values
        .into_iter()
        .flatten()
        .find(|entry| entry.key == METADATA_KEY);
    let Some(text) = entry.and_then(|entry| entry.value.as_deref()) else {
        return Ok(HashMap::new());
    };
    let malformed = || ReadError::Invalid(format!("Failed to parse {METADATA_KEY} metadata"));
    let info: serde_json::Value = serde_json::from_str(text).map_err(|_| malformed())?;
    let mut columns = HashMap::new();
    let listed = info.get("columnTypes").and_then(|list| list.as_array());
    for column in listed.into_iter().flatten() {
        let text = |value: Option<&serde_json::Value>| {
            let text = value.and_then(|value| value.as_str());
            text.filter(|text| !text.is_empty()).map(str::to_string)
        };
        let name = text(column.get("columnName")).ok_or_else(malformed)?;
        let codec = column.get("codec");
        let info = ColumnInfo {
            special: text(column.get("specialType")),
            codec_type: text(codec.and_then(|codec| codec.get("dataType"))),
        };
        columns.insert(name, info);
    }
    Ok(columns)
}

/// Whether a column name needs no change to be a legal one. Java renames the others; a
/// name that is a reserved word passes here and is left to the caller, which knows the
/// reserved words.
fn is_plain_identifier(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && characters.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// The logical type of a column, from its logical type or, in a file that has none, from
/// its converted type.
///
/// Behavior: `org.apache.parquet.schema.LogicalTypeAnnotation#fromOriginalType`, through
/// which parquet-mr gives Java one annotation for both
fn annotation(column: &ColumnDescriptor) -> Option<LogicalType> {
    if let Some(logical) = column.logical_type_ref() {
        return Some(logical.clone());
    }
    use parquet::basic::TimeUnit;
    Some(match column.converted_type() {
        ConvertedType::NONE => return None,
        ConvertedType::UTF8 => LogicalType::String,
        ConvertedType::MAP | ConvertedType::MAP_KEY_VALUE => LogicalType::Map,
        ConvertedType::LIST => LogicalType::List,
        ConvertedType::ENUM => LogicalType::Enum,
        ConvertedType::DECIMAL => {
            LogicalType::decimal(column.type_scale(), column.type_precision())
        }
        ConvertedType::DATE => LogicalType::Date,
        ConvertedType::TIME_MILLIS => LogicalType::time(true, TimeUnit::MILLIS),
        ConvertedType::TIME_MICROS => LogicalType::time(true, TimeUnit::MICROS),
        ConvertedType::TIMESTAMP_MILLIS => LogicalType::timestamp(true, TimeUnit::MILLIS),
        ConvertedType::TIMESTAMP_MICROS => LogicalType::timestamp(true, TimeUnit::MICROS),
        ConvertedType::UINT_8 => LogicalType::integer(8, false),
        ConvertedType::UINT_16 => LogicalType::integer(16, false),
        ConvertedType::UINT_32 => LogicalType::integer(32, false),
        ConvertedType::UINT_64 => LogicalType::integer(64, false),
        ConvertedType::INT_8 => LogicalType::integer(8, true),
        ConvertedType::INT_16 => LogicalType::integer(16, true),
        ConvertedType::INT_32 => LogicalType::integer(32, true),
        ConvertedType::INT_64 => LogicalType::integer(64, true),
        ConvertedType::JSON => LogicalType::Json,
        ConvertedType::BSON => LogicalType::Bson,
        // an interval has no logical type in this crate; Java refuses it by name
        ConvertedType::INTERVAL => LogicalType::Unknown,
    })
}

/// A type Java reads that has no column type here yet.
fn no_column_type(java_type: &str, column: &str) -> ReadError {
    not_ported(
        READ_SCHEMA,
        format_args!("column {column} of type {java_type}"),
    )
}

/// The type of the values of a column, whether it holds one per row or a list of them.
///
/// Behavior: the body of `ParquetSchemaReader#readParquetSchema` for one column, and the
/// visitor of `ParquetSchemaReader#getVisitor`
fn value_type(
    column: &ColumnDescriptor,
    name: &str,
    info: &ColumnInfo,
    unsigned_long: Option<UnsignedLongTarget>,
    legacy: bool,
) -> Result<ColumnType, ReadError> {
    let path = column.path().parts().join(", ");
    let refused = |what: &str| {
        // Java's text
        ReadError::Invalid(format!(
            "Unable to read column [{path}]: {what} not supported"
        ))
    };
    let special = info.special.as_deref();
    let Some(logical) = annotation(column).filter(|logical| *logical != LogicalType::Unknown)
    else {
        return match column.physical_type() {
            PhysicalType::BOOLEAN => Ok(ColumnType::Boolean),
            PhysicalType::INT32 => Ok(ColumnType::Int),
            PhysicalType::INT64 => Ok(ColumnType::Long),
            PhysicalType::INT96 => Ok(ColumnType::Instant),
            PhysicalType::FLOAT => Ok(ColumnType::Float),
            PhysicalType::DOUBLE => Ok(ColumnType::Double),
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => match special {
                Some("StringSet") => Err(no_column_type("io.deephaven.stringset.StringSet", name)),
                Some(other) => Err(ReadError::Invalid(format!(
                    "BINARY or FIXED_LEN_BYTE_ARRAY type for column [{path}] with unknown \
                     special type {other}"
                ))),
                None if legacy => Err(no_column_type(
                    "java.lang.String, decoded from bytes by UTF8StringAsByteArrayCodec",
                    name,
                )),
                None => Err(no_column_type("byte[]", name)),
            },
        };
    };
    match logical {
        LogicalType::String | LogicalType::Enum => match special {
            Some("StringSet") => Err(no_column_type("io.deephaven.stringset.StringSet", name)),
            Some(other) if other != "Vector" => Err(ReadError::Invalid(format!(
                "Type for column [{path}] with unknown or incompatible special type {other}"
            ))),
            _ => Ok(ColumnType::String),
        },
        LogicalType::Map => Err(refused("MapLogicalType")),
        LogicalType::List => Err(refused("ListLogicalType")),
        LogicalType::Json => Err(refused("JsonLogicalType")),
        LogicalType::Bson => Err(refused("BsonLogicalType")),
        LogicalType::Uuid => Err(refused("UUIDLogicalType")),
        LogicalType::Decimal(decimal) => {
            // Java writes a `BigInteger` as a decimal of precision 1 and scale 0
            let big_integer = decimal.precision == 1 && decimal.scale == 0;
            Err(no_column_type(
                if big_integer {
                    "java.math.BigInteger"
                } else {
                    "java.math.BigDecimal"
                },
                name,
            ))
        }
        LogicalType::Date => Err(no_column_type("java.time.LocalDate", name)),
        LogicalType::Time(_) => Err(no_column_type("java.time.LocalTime", name)),
        LogicalType::Timestamp(timestamp) => {
            if timestamp.is_adjusted_to_u_t_c {
                Ok(ColumnType::Instant)
            } else {
                Err(no_column_type("java.time.LocalDateTime", name))
            }
        }
        LogicalType::Integer(integer) => match (integer.is_signed, integer.bit_width) {
            (true, 64) => Ok(ColumnType::Long),
            (true, 32) => Ok(ColumnType::Int),
            (true, 16) => Ok(ColumnType::Short),
            (true, 8) => Ok(ColumnType::Byte),
            // an unsigned 16-bit integer is Java's char; the other unsigned types are
            // read as the next wider type
            (false, 8 | 16) => Ok(ColumnType::Char),
            (false, 32) => Ok(ColumnType::Long),
            (false, 64) => match unsigned_long {
                Some(_) => Ok(ColumnType::Long),
                None => Err(no_column_type("java.math.BigInteger", name)),
            },
            (signed, width) => Err(refused(&format!(
                "IntLogicalType, isSigned={signed}, bitWidth={width}"
            ))),
        },
        _ => Err(ReadError::Invalid(format!(
            "Unable to read column [{path}]: no mappable logical type annotation found"
        ))),
    }
}

/// The columns of a file with the Parquet schema `schema` and the footer entries
/// `key_values`, named as `options` say.
///
/// A column Java reads as a type that has no [`ColumnType`] yet, or under a name it would
/// first make legal, is refused as not ported; a column Java refuses is refused with
/// Java's reason.
///
/// Behavior: `io.deephaven.parquet.table.ParquetSchemaReader#readParquetSchema` and the
/// consumer of `#convertSchema` that turns each column into a column definition
pub(crate) fn columns(
    schema: &SchemaDescriptor,
    key_values: Option<&Vec<KeyValue>>,
    options: &ReadOptions,
) -> Result<Vec<FileColumn>, ReadError> {
    let infos = column_infos(key_values)?;
    let no_info = ColumnInfo::default();
    let mut columns: Vec<FileColumn> = Vec::new();
    let mut previous: Option<(usize, String)> = None;
    for (leaf, column) in schema.columns().iter().enumerate() {
        let path = column.path().parts();
        if column.max_rep_level() > 1 {
            return Err(ReadError::Invalid(format!(
                "Unsupported maximum repetition level {} in column {}",
                column.max_rep_level(),
                path.join("/")
            )));
        }
        let root = schema.get_column_root_idx(leaf);
        let parquet_name = &path[0];
        if let Some((_, first)) = previous.as_ref().filter(|(index, _)| *index == root) {
            return Err(ReadError::Invalid(format!(
                "Encountered unsupported multi-column field {parquet_name}: found columns \
                 {first} and {}",
                path.join("/")
            )));
        }
        previous = Some((root, path.join("/")));

        let name = match options.name_of(parquet_name) {
            Some(name) => name.to_string(),
            None if is_plain_identifier(parquet_name) => parquet_name.clone(),
            None => {
                return Err(not_ported(
                    LEGALIZE,
                    format_args!("column name '{parquet_name}'"),
                ))
            }
        };
        let info = infos.get(&name).unwrap_or(&no_info);
        if let Some(java_type) = &info.codec_type {
            return Err(not_ported(
                "io.deephaven.parquet.table.pagestore.topage.ToObjectPage#create",
                format_args!("column {name}, which a codec decodes to {java_type}"),
            ));
        }
        let unsigned_long = options.unsigned_long_of(&name);
        let values = value_type(column, &name, info, unsigned_long, options.legacy)?;
        let is_list = column.max_rep_level() > 0;
        let column_type = match (info.special.as_deref(), is_list) {
            (Some("Vector"), true) => ColumnType::vector(values),
            (Some("Vector"), false) => {
                return Err(not_ported(
                    READ_SCHEMA,
                    format_args!("column {name}, a vector that is not a list in the file"),
                ))
            }
            (_, true) => ColumnType::array(values),
            (_, false) if path.len() > 1 => {
                // Java reads the one leaf of the struct as the column
                return Err(not_ported(
                    READ_SCHEMA,
                    format_args!("column {name}, a struct with the one field {}", path[1..].join("/")),
                ));
            }
            (_, false) => values,
        };
        columns.push(FileColumn {
            name,
            root,
            column_type,
            unsigned_long,
        });
    }
    Ok(columns)
}
