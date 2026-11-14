//! Table schemas.
//!
//! Port of `io.deephaven.engine.table.TableDefinition` and
//! `io.deephaven.engine.table.ColumnDefinition`, reduced to what crosses crate boundaries.

use arrow::datatypes::{DataType, TimeUnit};

/// The Deephaven type of a column, named after the Java type clients see in the schema.
///
/// Adding a variant is a contract change. The Arrow mapping below must agree with the schemas
/// the Java server sends; task 2.3 checks it against schemas captured from the oracle.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum ColumnType {
    Boolean,
    Byte,
    Char,
    Short,
    Int,
    Long,
    Float,
    Double,
    String,
    /// `java.time.Instant`, nanoseconds since the epoch, UTC.
    Instant,
}

impl ColumnType {
    /// The Arrow type used in memory for this column type. Nulls are validity bits, never
    /// Java's sentinel values; see PARITY.md for how results must still match Java.
    pub fn arrow_type(&self) -> DataType {
        match self {
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Byte => DataType::Int8,
            ColumnType::Char => DataType::UInt16,
            ColumnType::Short => DataType::Int16,
            ColumnType::Int => DataType::Int32,
            ColumnType::Long => DataType::Int64,
            ColumnType::Float => DataType::Float32,
            ColumnType::Double => DataType::Float64,
            ColumnType::String => DataType::Utf8,
            ColumnType::Instant => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        }
    }
}

/// One column of a table: its name and type.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ColumnDefinition {
    pub name: String,
    pub column_type: ColumnType,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
        }
    }
}

/// The ordered columns of a table. Column names are unique.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TableDefinition {
    columns: Vec<ColumnDefinition>,
}

/// A table definition was given two columns with the same name.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DuplicateColumn(pub String);

impl std::fmt::Display for DuplicateColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "duplicate column name: {}", self.0)
    }
}

impl std::error::Error for DuplicateColumn {}

impl TableDefinition {
    pub fn new(columns: Vec<ColumnDefinition>) -> Result<Self, DuplicateColumn> {
        for (i, c) in columns.iter().enumerate() {
            if columns[..i].iter().any(|p| p.name == c.name) {
                return Err(DuplicateColumn(c.name.clone()));
            }
        }
        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    /// Index of the column named `name`.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_rejects_duplicate_names_and_finds_columns() {
        let def = TableDefinition::new(vec![
            ColumnDefinition::new("Sym", ColumnType::String),
            ColumnDefinition::new("Price", ColumnType::Double),
        ])
        .unwrap();
        assert_eq!(def.index_of("Price"), Some(1));
        assert_eq!(def.index_of("Size"), None);
        assert_eq!(
            TableDefinition::new(vec![
                ColumnDefinition::new("A", ColumnType::Int),
                ColumnDefinition::new("A", ColumnType::Long),
            ]),
            Err(DuplicateColumn("A".into()))
        );
    }

    #[test]
    fn char_is_unsigned_16_bit_and_instant_is_utc_nanos() {
        assert_eq!(ColumnType::Char.arrow_type(), DataType::UInt16);
        assert_eq!(
            ColumnType::Instant.arrow_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
    }
}
