//! Table schemas.
//!
//! Port of `io.deephaven.engine.table.TableDefinition` and
//! `io.deephaven.engine.table.ColumnDefinition`, reduced to what crosses crate boundaries.

use arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

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
    /// A Deephaven vector of the element type: the type of the columns that `group_by`, the
    /// `group` and `distinct` aggregations and a rolling group produce, and of a column
    /// array in a formula.
    ///
    /// The element fixes the Java class. A primitive element (`Byte`, `Char`, `Short`,
    /// `Int`, `Long`, `Float`, `Double`) gives the vector of that primitive:
    /// `io.deephaven.vector.ByteVector`, `CharVector`, `ShortVector`, `IntVector`,
    /// `LongVector`, `FloatVector`, `DoubleVector`. Any other element gives
    /// `io.deephaven.vector.ObjectVector`, with the element's class as the column's
    /// component type (`io.deephaven.engine.table.ColumnDefinition#getComponentType`):
    /// `java.lang.Boolean`, `java.lang.String`, `java.time.Instant`, or the class of the
    /// inner vector or array where the element is itself one. Java has no vector of
    /// `boolean`. An `ObjectVector` of a boxed primitive (`ObjectVector<Integer>`) cannot
    /// be written here, as a column of a boxed primitive cannot.
    ///
    /// Java's column definition holds a data type and one component type, so Java does not
    /// know the element type of an `ObjectVector` that is itself an element
    /// (`Vector(Vector(String))`, `Array(Vector(Instant))`). This type does, and the engine
    /// may compute with such a column, but the server refuses to describe it to a client:
    /// `io.deephaven.extensions.barrage.util.BarrageUtil#arrowFieldFor` declares the inner
    /// elements as strings, which is not ported.
    ///
    /// [`ColumnType::list_element`] says what the cells of such a column are.
    Vector(Box<ColumnType>),
    /// A Java array of the element type: `int[]`, `double[]`, `java.lang.String[]`,
    /// `java.time.Instant[]`, `int[][]`, an array of vectors, and so on. Java makes these
    /// where an operation or a formula builds an array and not a vector, for example the
    /// `UNIQUE_KEYS` (`String[]`) and `UNIQUE_COUNTS` (`long[]`) columns of
    /// `io.deephaven.server.table.stats.ObjectChunkedStats`.
    ///
    /// A vector and an array of one element type hold the same cells and have the same
    /// Arrow type. They are two column types because they are two types in Java: a client
    /// is told `deephaven:type` `io.deephaven.vector.IntVector` for one and `int[]` for the
    /// other, and an operation that takes one need not take the other.
    ///
    /// `Array(Byte)` is Java's `byte[]`, which a client is sent as Arrow `Binary` and not as
    /// a list (`BarrageUtil#arrowTypeFor`). In memory it is a list like every other array;
    /// the server refuses to send one until that conversion is ported.
    Array(Box<ColumnType>),
    /// A table: the type of the constituent column of a partitioned table
    /// (`io.deephaven.engine.table.PartitionedTable#constituentColumnName`). The class is
    /// the Java class the column's definition names.
    ///
    /// A cell is a handle, and a chunk of such a column an Arrow `Int64` array of handles.
    /// The crate that made a handle turns it back into the table; for the engine that is
    /// `oxhaven_engine::ops::partitioned::resolve`. A handle is good while the column
    /// source it was first read from is alive, and identifies nothing afterwards. A null
    /// cell is Java's `null` reference. Handles are not sent to clients: Java sends the
    /// text of each table (`BarrageUtil#arrowTypeFor` gives such a column Arrow `Utf8`).
    ///
    /// Two cells are the same table when their handles are equal. Nothing else may be
    /// computed from a handle: it has no order and no meaning as a number.
    Table(TableClass),
}

/// The Java class of the cells of a [`ColumnType::Table`] column.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
#[non_exhaustive]
pub enum TableClass {
    /// `io.deephaven.engine.table.Table`: what a transform of a partitioned table declares
    /// (`io.deephaven.engine.table.impl.partitioned.TableTransformationColumn`).
    Table,
    /// `io.deephaven.engine.table.impl.QueryTable`: what the partition aggregation declares
    /// (`io.deephaven.engine.table.impl.by.PartitionByChunkedOperator#getResultColumns`).
    QueryTable,
}

impl ColumnType {
    /// [`ColumnType::Vector`] of `element`.
    pub fn vector(element: ColumnType) -> Self {
        ColumnType::Vector(Box::new(element))
    }

    /// [`ColumnType::Array`] of `element`.
    pub fn array(element: ColumnType) -> Self {
        ColumnType::Array(Box::new(element))
    }

    /// The element type of a list-valued column, which is a [`ColumnType::Vector`] or a
    /// [`ColumnType::Array`]; `None` for every other type.
    ///
    /// A chunk of a list-valued column, from `ColumnSource::chunk`, `prev_chunk` or
    /// `chunk_by_keys`, is an Arrow `ListArray` (32-bit offsets) of exactly
    /// [`arrow_type`](Self::arrow_type), so no trait beyond `ColumnSource` is needed to
    /// read one:
    ///
    /// - a cell is one list entry, with its elements in the order Java iterates the vector
    ///   or array;
    /// - a null cell, Java's `null` reference, is a null list entry. It is not an empty
    ///   list: an empty vector is a valid entry of length 0, and the two are different
    ///   values. A `NULL_ROW_KEY` in `chunk_by_keys` reads as a null entry;
    /// - a null element is a null of the child array: for a primitive element the value
    ///   Java reserves for null (PARITY.md, "Null representation in the Java engine"), for
    ///   any other its `null` reference;
    /// - where the element is itself list-valued, the child is again such a `ListArray`.
    ///
    /// The offsets a null entry spans and the elements under it mean nothing, and neither
    /// does where an entry's elements lie in the child. Two chunks hold the same cells when
    /// they agree entry by entry.
    pub fn list_element(&self) -> Option<&ColumnType> {
        match self {
            ColumnType::Vector(element) | ColumnType::Array(element) => Some(element),
            _ => None,
        }
    }

    /// The Arrow type used in memory for this column type. Nulls are validity bits, never
    /// Java's sentinel values; see PARITY.md for how results must still match Java.
    ///
    /// A vector or an array is a `List` whose child field is named `""` and is nullable.
    /// That is the field Java declares for the elements
    /// (`io.deephaven.extensions.barrage.util.BarrageUtil#arrowFieldForVectorType`,
    /// `#arrowFieldFor`), so the type equals the one in a schema from Java.
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
            ColumnType::Vector(element) | ColumnType::Array(element) => {
                DataType::List(Arc::new(Field::new("", element.arrow_type(), true)))
            }
            ColumnType::Table(_) => DataType::Int64,
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

    #[test]
    fn a_vector_and_an_array_are_lists_of_a_nullable_unnamed_element() {
        let ints = DataType::List(Arc::new(Field::new("", DataType::Int32, true)));
        assert_eq!(ColumnType::vector(ColumnType::Int).arrow_type(), ints);
        assert_eq!(ColumnType::array(ColumnType::Int).arrow_type(), ints);
        assert_eq!(
            ColumnType::vector(ColumnType::array(ColumnType::Int)).arrow_type(),
            DataType::List(Arc::new(Field::new("", ints, true)))
        );
    }

    #[test]
    fn a_table_cell_is_a_handle_and_the_two_classes_are_two_types() {
        let made = ColumnType::Table(TableClass::QueryTable);
        let declared = ColumnType::Table(TableClass::Table);
        assert_eq!(made.arrow_type(), DataType::Int64);
        assert_eq!(declared.arrow_type(), DataType::Int64);
        assert_ne!(made, declared);
        assert_eq!(made.list_element(), None);
    }

    #[test]
    fn a_vector_is_not_an_array_and_only_they_have_an_element() {
        let vector = ColumnType::vector(ColumnType::Double);
        let array = ColumnType::array(ColumnType::Double);
        assert_ne!(vector, array);
        assert_ne!(vector, ColumnType::vector(ColumnType::Float));
        assert_eq!(vector, ColumnType::Vector(Box::new(ColumnType::Double)));
        assert_eq!(vector.list_element(), Some(&ColumnType::Double));
        assert_eq!(array.list_element(), Some(&ColumnType::Double));
        assert_eq!(ColumnType::Double.list_element(), None);
        assert_eq!(
            ColumnType::array(vector.clone()).list_element(),
            Some(&vector)
        );
    }
}
