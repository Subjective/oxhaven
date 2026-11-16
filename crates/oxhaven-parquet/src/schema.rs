//! From the schema of a Parquet file to the columns of a table.

use crate::{not_ported, ReadError, ReadOptions, UnsignedLongTarget};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_csv::is_reserved;
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
    /// The index of the column among the leaves of the file's schema.
    pub leaf: usize,
    /// Whether the column is the one leaf of a group that is not a list, a struct with one
    /// field.
    pub nested: bool,
    pub column_type: ColumnType,
    /// How an unsigned 64-bit column becomes a `long`.
    pub unsigned_long: Option<UnsignedLongTarget>,
}

const READ_SCHEMA: &str = "io.deephaven.parquet.table.ParquetSchemaReader#readParquetSchema";

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

/// The column name Java makes of the name `parquet_name` of a file column: a space
/// becomes an underscore; a reserved word gets the prefix `column_`; every character that
/// is not an ASCII letter, a digit, `_` or `$` is dropped; a name that then starts with a
/// digit gets the prefix too; and a name in `taken` gets the first number from 2 on that
/// makes it new. A name with nothing left, or left with a reserved word, is an error.
///
/// Behavior: `io.deephaven.api.util.NameValidator#legalizeColumnName(String, Function, Set)`
/// with the replacement of `ParquetSchemaReader#convertSchema`; the key of a partition
/// directory is made a name without that replacement (`space_to_underscore`), so a space
/// in it is dropped
pub(crate) fn legalize(
    parquet_name: &str,
    space_to_underscore: bool,
    taken: &[String],
) -> Result<String, ReadError> {
    let error = || {
        // Java: a `NameValidator.LegalizeNameException` with this text
        ReadError::Invalid(format!("Can not legalize column name {parquet_name}"))
    };
    let mut name = if space_to_underscore {
        parquet_name.replace(' ', "_")
    } else {
        parquet_name.to_string()
    };
    if is_reserved(&name) {
        name = format!("column_{name}");
    }
    name.retain(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if name.is_empty() {
        return Err(error());
    }
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        name = format!("column_{name}");
    }
    let mut unique = name.clone();
    let mut next = 2;
    while taken.contains(&unique) {
        unique = format!("{name}{next}");
        next += 1;
    }
    // what is left is ASCII, so it is a column name unless it is a reserved word
    if is_reserved(&name) {
        return Err(error());
    }
    Ok(unique)
}

/// The logical type of a column, from its logical type or, in a file that has none, from
/// its converted type.
///
/// Behavior: `org.apache.parquet.schema.LogicalTypeAnnotation#fromOriginalType`, through
/// which parquet-mr gives Java one annotation for both
pub(crate) fn annotation(column: &ColumnDescriptor) -> Option<LogicalType> {
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
                // a `byte[]`, which `columns` knows by the array of bytes that is no list
                None => Ok(ColumnType::array(ColumnType::Byte)),
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
            if decimal.precision == 1 && decimal.scale == 0 {
                Ok(ColumnType::BigInteger)
            } else {
                Ok(ColumnType::BigDecimal)
            }
        }
        LogicalType::Date => Ok(ColumnType::LocalDate),
        LogicalType::Time(_) => Ok(ColumnType::LocalTime),
        LogicalType::Timestamp(timestamp) => {
            if timestamp.is_adjusted_to_u_t_c {
                Ok(ColumnType::Instant)
            } else {
                Ok(ColumnType::LocalDateTime)
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
                None => Ok(ColumnType::BigInteger),
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
/// A column no instruction names gets the name Java makes of its name in the file
/// ([`legalize`]). A column Java reads as a type that has no [`ColumnType`] yet is refused
/// as not ported; a column Java refuses is refused with Java's reason.
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
    // The names a changed name must differ from: none until the first name is changed,
    // then the names the instructions give and the changed names. A name read as it is in
    // the file is not among them, so a changed name can repeat one, and the table
    // definition then refuses the two columns of one name. The first changed name can be
    // the name of an instruction, which the mapping then refuses.
    //
    // Behavior: `ParquetInstructions.Builder#getTakenNames` of the builder that
    // `ParquetSchemaReader#readParquetSchema` makes when it first adds a name mapping,
    // `ParquetInstructions.Builder#addColumnNameMapping`
    let mut taken: Option<Vec<String>> = None;
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
            None => {
                let legal = legalize(parquet_name, true, taken.as_deref().unwrap_or(&[]))?;
                if legal != *parquet_name {
                    let named = |c: &&crate::ColumnInstruction| c.name.as_ref() == Some(&legal);
                    let instructed = options.columns.iter().find(named);
                    if let Some(instructed) = instructed {
                        return Err(ReadError::Invalid(format!(
                            "Cannot add a mapping from parquetColumnName={parquet_name}: \
                             columnName={legal} already mapped to parquetColumnName={}",
                            instructed.parquet_name
                        )));
                    }
                    let instructed = options.columns.iter().filter_map(|c| c.name.clone());
                    let taken = taken.get_or_insert_with(|| instructed.collect());
                    taken.push(legal.clone());
                }
                legal
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
        // Java has these as objects, and a list of them as an array of objects
        let object = match &values {
            ColumnType::BigDecimal => Some("java.math.BigDecimal"),
            ColumnType::BigInteger => Some("java.math.BigInteger"),
            ColumnType::LocalDate => Some("java.time.LocalDate"),
            ColumnType::LocalTime => Some("java.time.LocalTime"),
            ColumnType::LocalDateTime => Some("java.time.LocalDateTime"),
            ColumnType::Array(_) => Some("byte[]"),
            _ => None,
        };
        if let Some(java_type) = object.filter(|_| is_list) {
            return Err(not_ported(
                "io.deephaven.parquet.table.pagestore.topage.ToArrayPage#create",
                format_args!("column {name}, a list of {java_type}"),
            ));
        }
        let nested = !is_list && path.len() > 1;
        let column_type = match (info.special.as_deref(), is_list) {
            (Some("Vector"), true) => ColumnType::vector(values),
            (Some("Vector"), false) => {
                return Err(not_ported(
                    READ_SCHEMA,
                    format_args!("column {name}, a vector that is not a list in the file"),
                ))
            }
            (_, true) => ColumnType::array(values),
            // of a struct with one field, Java reads the field
            (_, false) => values,
        };
        columns.push(FileColumn {
            name,
            root,
            leaf,
            nested,
            column_type,
            unsigned_long,
        });
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnInstruction;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    fn schema(message: &str) -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(message).unwrap()))
    }

    fn types(message: &str) -> Result<Vec<(String, ColumnType)>, ReadError> {
        let columns = columns(&schema(message), None, &ReadOptions::default())?;
        let columns = columns.into_iter();
        Ok(columns.map(|c| (c.name, c.column_type)).collect())
    }

    fn deephaven(json: &str) -> Vec<KeyValue> {
        vec![
            KeyValue::new("other".to_string(), "ignored".to_string()),
            KeyValue::new("deephaven".to_string(), json.to_string()),
        ]
    }

    #[test]
    fn a_file_with_converted_types_only_reads_like_one_with_logical_types() {
        use ColumnType::*;
        // what writers before the logical types left in a schema
        let old = "message m {
            optional binary Text (UTF8);
            optional int32 Small (INT_8);
            optional int32 Mid (INT_16);
            optional int32 Letter (UINT_16);
            optional int32 Wide (UINT_32);
            optional int64 Stamp (TIMESTAMP_MILLIS);
            optional int64 Micros (TIMESTAMP_MICROS);
            optional int64 Count (INT_64);
            optional binary Choice (ENUM);
            required double Plain;
        }";
        let expected = [
            ("Text", String),
            ("Small", Byte),
            ("Mid", Short),
            ("Letter", Char),
            ("Wide", Long),
            ("Stamp", Instant),
            ("Micros", Instant),
            ("Count", Long),
            ("Choice", String),
            ("Plain", Double),
        ];
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect();
        assert_eq!(types(old).unwrap(), expected);
        // the descriptors really have no logical type, so the converted type was used
        assert!(schema(old).column(0).logical_type_ref().is_none());

        for (declared, column_type) in [
            ("int32 V (DATE)", LocalDate),
            ("int32 V (TIME_MILLIS)", LocalTime),
            ("int64 V (TIME_MICROS)", LocalTime),
            ("int64 V (UINT_64)", BigInteger),
            ("int32 V (DECIMAL(9,2))", BigDecimal),
            ("int32 V (DECIMAL(1,0))", BigInteger),
            ("fixed_len_byte_array(1) V (DECIMAL(1,0))", BigInteger),
            ("fixed_len_byte_array(1) V (DECIMAL(1,1))", BigDecimal),
            ("fixed_len_byte_array(1) V (DECIMAL(2,0))", BigDecimal),
        ] {
            let read = types(&format!("message m {{ optional {declared}; }}"));
            assert_eq!(read, Ok(vec![("V".to_string(), column_type)]), "{declared}");
        }
    }

    #[test]
    fn the_types_java_holds_as_objects_are_read_one_to_a_row_only() {
        use ColumnType::*;
        let plain = "message m {
            optional binary Bytes;
            optional fixed_len_byte_array(3) Fixed;
            optional int64 Local (TIMESTAMP(MICROS,false));
            optional int64 Stamp (TIMESTAMP(MICROS,true));
            optional int64 Nanos (TIME(NANOS,true));
            optional group Pair { optional int32 only; }
        }";
        let expected = [
            ("Bytes", ColumnType::array(Byte)),
            ("Fixed", ColumnType::array(Byte)),
            ("Local", LocalDateTime),
            ("Stamp", Instant),
            ("Nanos", LocalTime),
            ("Pair", Int),
        ];
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect();
        assert_eq!(types(plain).unwrap(), expected);
        let read = columns(&schema(plain), None, &ReadOptions::default()).unwrap();
        let nested: Vec<bool> = read.iter().map(|column| column.nested).collect();
        assert_eq!(nested, [false, false, false, false, false, true]);
        assert_eq!(read[5].leaf, 5);

        for (element, java_type) in [
            ("binary element", "byte[]"),
            ("int32 element (DATE)", "java.time.LocalDate"),
            ("int64 element (TIME(NANOS,true))", "java.time.LocalTime"),
            (
                "int64 element (TIMESTAMP(NANOS,false))",
                "java.time.LocalDateTime",
            ),
            ("int64 element (INTEGER(64,false))", "java.math.BigInteger"),
            ("int64 element (DECIMAL(18,2))", "java.math.BigDecimal"),
        ] {
            let list = format!(
                "message m {{ optional group V (LIST) {{ repeated group list {{ optional {element}; }} }} }}"
            );
            let Err(ReadError::NotPorted { java, what }) = types(&list) else {
                panic!("a list of {java_type} was read");
            };
            assert!(java.ends_with("ToArrayPage#create"), "{java}");
            assert_eq!(what, format!("column V, a list of {java_type}"));
        }
    }

    /// The names are the ones Java 42.9 gave the columns of `tests/data/names.parquet` and
    /// `reserved.parquet`, and of files written for the probe of task 7.1b.
    #[test]
    fn a_name_that_is_not_legal_is_changed_as_java_changes_it() {
        let names = |fields: &[&str], options: &ReadOptions| {
            let fields: Vec<_> = fields
                .iter()
                .map(|name| {
                    parquet::schema::types::Type::primitive_type_builder(name, PhysicalType::INT32)
                        .build()
                })
                .map(|field| Arc::new(field.unwrap()))
                .collect();
            let message = parquet::schema::types::Type::group_type_builder("m").with_fields(fields);
            let schema = SchemaDescriptor::new(Arc::new(message.build().unwrap()));
            let columns = columns(&schema, None, options)?;
            Ok::<_, ReadError>(columns.into_iter().map(|c| c.name).collect::<Vec<_>>())
        };
        let plain = ReadOptions::default();
        assert_eq!(
            names(
                &["a b", "2x", "ok", "int", "a-b", "ünï", "i", "$x", "_"],
                &plain
            )
            .unwrap(),
            [
                "a_b",
                "column_2x",
                "ok",
                "column_int",
                "ab",
                "n",
                "column_i",
                "$x",
                "column__"
            ]
        );
        // a changed name differs from the changed names before it, by a number
        assert_eq!(
            names(&["a b", "a.b", "a_b!", "a,b"], &plain).unwrap(),
            ["a_b", "ab", "a_b2", "ab2"]
        );
        assert_eq!(
            names(&["a b", "x", "a  b", "a_b2", "a!b", "ab"], &plain).unwrap(),
            ["a_b", "x", "a__b", "a_b2", "ab", "ab2"]
        );
        // and from the names of the instructions, but for the first name that is changed
        let instructed = |parquet_name: &str, name: &str| ReadOptions {
            columns: vec![ColumnInstruction {
                parquet_name: parquet_name.to_string(),
                name: Some(name.to_string()),
                ..ColumnInstruction::default()
            }],
            ..ReadOptions::default()
        };
        assert_eq!(
            names(&["c d", "a b"], &instructed("other", "a_b")).unwrap(),
            ["c_d", "a_b2"]
        );
        let refused = "Cannot add a mapping from parquetColumnName=a b: columnName=a_b already \
                       mapped to parquetColumnName=";
        assert_eq!(
            names(&["a b"], &instructed("other", "a_b")),
            Err(ReadError::Invalid(format!("{refused}other")))
        );
        assert_eq!(
            names(&["a b", "a_b"], &instructed("a_b", "a_b")),
            Err(ReadError::Invalid(format!("{refused}a_b")))
        );
        // an instruction's name is taken as it is
        let mapped = ReadOptions {
            columns: vec![ColumnInstruction {
                parquet_name: "a b".to_string(),
                name: Some("Q".to_string()),
                ..ColumnInstruction::default()
            }],
            ..ReadOptions::default()
        };
        assert_eq!(names(&["a b", "c d"], &mapped).unwrap(), ["Q", "c_d"]);
        // a name that needed no change is not among the taken ones, so a later column
        // can get it again; a changed name is, and changes a legal name after it
        assert_eq!(names(&["a_b", "a b"], &plain).unwrap(), ["a_b", "a_b"]);
        assert_eq!(names(&["a b", "a_b"], &plain).unwrap(), ["a_b", "a_b2"]);
        for name in ["", "!!", "ü"] {
            let expected = format!("Can not legalize column name {name}");
            assert_eq!(names(&[name], &plain), Err(ReadError::Invalid(expected)));
        }
    }

    #[test]
    fn logical_types_java_has_no_type_for_are_refused_as_java_does() {
        for (declared, reason) in [
            ("binary V (JSON)", "JsonLogicalType not supported"),
            ("binary V (BSON)", "BsonLogicalType not supported"),
            (
                "fixed_len_byte_array(16) V (UUID)",
                "UUIDLogicalType not supported",
            ),
            (
                "fixed_len_byte_array(2) V (FLOAT16)",
                "no mappable logical type annotation found",
            ),
        ] {
            let refused = types(&format!("message m {{ optional {declared}; }}"));
            let expected = format!("Unable to read column [V]: {reason}");
            assert_eq!(refused, Err(ReadError::Invalid(expected)), "{declared}");
        }
    }

    #[test]
    fn lists_in_every_shape_are_arrays_of_their_values() {
        use ColumnType::*;
        let lists = "message m {
            optional group Three (LIST) { repeated group list { optional int32 element; } }
            optional group Two (LIST) { repeated binary array (UTF8); }
            repeated double One;
        }";
        let expected = [
            ("Three", ColumnType::array(Int)),
            ("Two", ColumnType::array(String)),
            ("One", ColumnType::array(Double)),
        ];
        let expected: Vec<_> = expected
            .into_iter()
            .map(|(n, t)| (n.to_string(), t))
            .collect();
        assert_eq!(types(lists).unwrap(), expected);
    }

    #[test]
    fn the_deephaven_entry_names_vectors_and_what_is_not_ported() {
        let message = "message m {
            optional group V (LIST) { repeated group list { optional int32 element; } }
            optional binary S (UTF8);
        }";
        let read = |json: &str| {
            let entry = deephaven(json);
            columns(&schema(message), Some(&entry), &ReadOptions::default())
        };
        let vector =
            r#"{"version":"42.9","columnTypes":[{"columnName":"V","specialType":"Vector"}]}"#;
        let columns = read(vector).unwrap();
        assert_eq!(columns[0].column_type, ColumnType::vector(ColumnType::Int));
        assert_eq!(columns[1].column_type, ColumnType::String);
        // an entry without column types changes nothing
        let plain = read(r#"{"version":"42.9"}"#).unwrap();
        assert_eq!(plain[0].column_type, ColumnType::array(ColumnType::Int));

        let string_set = r#"{"columnTypes":[{"columnName":"S","specialType":"StringSet"}]}"#;
        let Err(ReadError::NotPorted { what, .. }) = read(string_set) else {
            panic!("a string set was read");
        };
        assert_eq!(what, "column S of type io.deephaven.stringset.StringSet");

        let codec = r#"{"columnTypes":[{"columnName":"S","codec":
            {"codecName":"io.deephaven.util.codec.BigDecimalCodec","dataType":"java.math.BigDecimal"}}]}"#;
        let Err(ReadError::NotPorted { java, what }) = read(codec) else {
            panic!("a codec column was read");
        };
        assert!(java.ends_with("ToObjectPage#create"));
        assert!(what.contains("java.math.BigDecimal"), "{what}");

        let malformed = Err(ReadError::Invalid(
            "Failed to parse deephaven metadata".to_string(),
        ));
        assert_eq!(read("{not json"), malformed);
    }

    #[test]
    fn the_metadata_of_a_renamed_column_is_found_under_its_new_name() {
        let message = "message m {
            optional group v (LIST) { repeated group list { optional int64 element (INTEGER(64,false)); } }
        }";
        let entry = deephaven(r#"{"columnTypes":[{"columnName":"V","specialType":"Vector"}]}"#);
        let options = ReadOptions {
            columns: vec![ColumnInstruction {
                parquet_name: "v".to_string(),
                name: Some("V".to_string()),
                unsigned_long: Some(UnsignedLongTarget::SignedLong),
                ..ColumnInstruction::default()
            }],
            ..ReadOptions::default()
        };
        let columns = columns(&schema(message), Some(&entry), &options).unwrap();
        assert_eq!(columns[0].name, "V");
        assert_eq!(columns[0].column_type, ColumnType::vector(ColumnType::Long));
        assert_eq!(
            columns[0].unsigned_long,
            Some(UnsignedLongTarget::SignedLong)
        );
    }
}
