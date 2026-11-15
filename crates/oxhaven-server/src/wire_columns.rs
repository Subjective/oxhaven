//! Array and vector columns whose cells a client is not sent as the engine stores them.
//!
//! The engine stores every array and vector as an Arrow list of its elements. Java's
//! schema is derived from the column's class and one component class, and differs in two
//! places:
//!
//! - A `byte[]`, at any depth, is Arrow `Binary`: the bytes of the array, a null element
//!   as the byte -128.
//! - A vector that is itself an element and is not a vector of a primitive is an
//!   `ObjectVector` whose own elements Java knows nothing about. It declares them as
//!   strings and sends the `toString` of each. So a `Vector(Vector(String))` is a
//!   `list<list<utf8>>` of the strings themselves, a `Vector(Vector(Instant))` one of
//!   `2023-11-14T22:13:20Z` and a `Vector(Vector(Vector(Int)))` one of `[1,2]`.
//!
//! A column of tables, the constituent column of a partitioned table, is stored as handles
//! and sent as strings: Java has no Arrow type for a table and sends the text of each.
//!
//! A table a client uploaded is sent back in the layout it was sent in, which for these
//! types is the stored one: a client cannot upload a list whose elements are an
//! `ObjectVector`, and a `list<int8>` it uploaded stays one.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, ListArray, StringArray};
use arrow::datatypes::{DataType, Field, Int8Type};
use oxhaven_column::sentinel::nulls_to_sentinels;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

use crate::java_text::object_texts;

/// Whether a vector of `element` is one of Java's primitive vectors (`IntVector`), and
/// not an `ObjectVector`.
pub fn is_primitive(element: &ColumnType) -> bool {
    matches!(
        element,
        ColumnType::Byte
            | ColumnType::Char
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
    )
}

fn list_of(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("", element, true)))
}

fn wire_type_at(column_type: &ColumnType, outermost: bool) -> DataType {
    match column_type {
        ColumnType::Array(element) if **element == ColumnType::Byte => DataType::Binary,
        ColumnType::Vector(element) if !outermost && !is_primitive(element) => {
            list_of(DataType::Utf8)
        }
        ColumnType::Array(element) | ColumnType::Vector(element) => {
            list_of(wire_type_at(element, false))
        }
        ColumnType::Table(_) => DataType::Utf8,
        other => other.arrow_type(),
    }
}

/// The Arrow type Java declares for a column of `column_type`. An element field has the
/// empty name and is nullable.
///
/// Behavior: `io.deephaven.extensions.barrage.util.BarrageUtil#arrowFieldFor`,
/// `BarrageUtil#arrowFieldForVectorType`, `BarrageUtil#arrowTypeFor` (a `byte[]` is
/// `VARBINARY`), `io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel#getComponentType`
/// (the elements of an `ObjectVector` without a component type are `Object`, which
/// `arrowTypeFor` makes `VARCHAR`)
pub fn wire_type(column_type: &ColumnType) -> DataType {
    wire_type_at(column_type, true)
}

fn failure(problem: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: problem.to_string(),
    }
}

fn to_wire_at(
    column_type: &ColumnType,
    stored: &ArrayRef,
    outermost: bool,
) -> Result<ArrayRef, TableFailure> {
    if matches!(column_type, ColumnType::Table(_)) {
        return table_texts(stored);
    }
    let Some(element) = column_type.list_element() else {
        return Ok(stored.clone());
    };
    let cells = stored.as_list_opt::<i32>().ok_or_else(|| {
        failure(format!(
            "a column of {column_type:?} holds {}",
            stored.data_type()
        ))
    })?;
    let is_array = matches!(column_type, ColumnType::Array(_));
    if is_array && *element == ColumnType::Byte {
        // Behavior: io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory#binaryFromByteArray,
        // which writes the array as it is: a null element is the byte NULL_BYTE
        let bytes = nulls_to_sentinels(cells.values())?;
        let bytes = bytes.as_primitive::<Int8Type>().values().inner().clone();
        let binary = BinaryArray::try_new(cells.offsets().clone(), bytes, cells.nulls().cloned())
            .map_err(failure)?;
        return Ok(Arc::new(binary));
    }
    let elements: ArrayRef = if !is_array && !outermost && !is_primitive(element) {
        // Behavior: io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory#utf8FromObject,
        // which writes `toString` of each element, and a null for a null
        Arc::new(StringArray::from(object_texts(element, cells.values())))
    } else {
        to_wire_at(element, cells.values(), false)?
    };
    let field = Arc::new(Field::new("", elements.data_type().clone(), true));
    let converted = ListArray::try_new(
        field,
        cells.offsets().clone(),
        elements,
        cells.nulls().cloned(),
    )
    .map_err(failure)?;
    Ok(Arc::new(converted))
}

/// The text Java sends for a table.
///
/// Behavior: `io.deephaven.engine.table.impl.QueryTable#toString`, by way of
/// `io.deephaven.engine.table.impl.BaseTable#getDescription`. Observed for the
/// constituents of `partitionBy` and of a transform; a table of another class (an
/// uncoalesced source table, say) has another text, and none of them can be a constituent
/// here yet.
const TABLE_TEXT: &str = "QueryTable";

/// The cells of a column of tables as Java sends them: the text of each table, and a null
/// for a null. The handles the engine stores stay in the server.
///
/// Behavior: `io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory#utf8FromObject`
fn table_texts(stored: &ArrayRef) -> Result<ArrayRef, TableFailure> {
    let handles = stored
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| failure(format!("a column of tables holds {}", stored.data_type())))?;
    let texts: StringArray = handles
        .iter()
        .map(|handle| handle.map(|_| TABLE_TEXT))
        .collect();
    Ok(Arc::new(texts))
}

/// How one column of a table reaches a client.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WireColumn {
    column_type: ColumnType,
    /// Whether the cells are sent in the type [`wire_type`] gives and that is not the
    /// stored one.
    converted: bool,
}

impl WireColumn {
    /// A column of `column_type` of a table that is described from its definition.
    pub fn described(column_type: &ColumnType) -> Self {
        WireColumn {
            converted: wire_type(column_type) != column_type.arrow_type(),
            column_type: column_type.clone(),
        }
    }

    /// A column of `column_type` that is sent in the layout the engine stores.
    pub fn stored(column_type: &ColumnType) -> Self {
        WireColumn {
            converted: false,
            column_type: column_type.clone(),
        }
    }

    /// The Arrow type of the arrays [`WireColumn::cells`] gives.
    pub fn arrow_type(&self) -> DataType {
        if self.converted {
            wire_type(&self.column_type)
        } else {
            self.column_type.arrow_type()
        }
    }

    /// Cells read from the column, as the client is sent them.
    ///
    /// # Panics
    /// By `unported!`, where Java sends the text of an array: see
    /// [`crate::java_text::object_texts`].
    pub fn cells(&self, stored: ArrayRef) -> Result<ArrayRef, TableFailure> {
        if self.converted {
            to_wire_at(&self.column_type, &stored, true)
        } else {
            Ok(stored)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Int32Array, Int8Array, TimestampNanosecondArray};
    use oxhaven_testkit::list_of as cells_of;

    fn utf8_lists() -> DataType {
        list_of(list_of(DataType::Utf8))
    }

    #[test]
    fn only_a_byte_array_and_an_inner_object_vector_are_declared_differently() {
        use ColumnType as T;
        for column_type in [
            T::Int,
            T::vector(T::String),
            T::array(T::Boolean),
            T::vector(T::Byte),
            T::vector(T::vector(T::Int)),
            T::vector(T::array(T::String)),
            T::array(T::vector(T::Double)),
            T::array(T::array(T::Instant)),
        ] {
            assert_eq!(wire_type(&column_type), column_type.arrow_type());
            assert!(!WireColumn::described(&column_type).converted);
        }
        assert_eq!(wire_type(&T::array(T::Byte)), DataType::Binary);
        assert_eq!(
            wire_type(&T::vector(T::array(T::Byte))),
            list_of(DataType::Binary)
        );
        for element in [T::String, T::Boolean, T::Instant, T::vector(T::Int)] {
            let inner = T::vector(element);
            assert_eq!(wire_type(&T::vector(inner.clone())), utf8_lists());
            assert_eq!(wire_type(&T::array(inner.clone())), utf8_lists());
            assert_eq!(
                wire_type(&T::vector(T::vector(inner))),
                utf8_lists(),
                "the elements of the first inner ObjectVector are text already"
            );
        }
        let column = WireColumn::described(&T::vector(T::vector(T::String)));
        assert_eq!(column.arrow_type(), utf8_lists());
        let column = WireColumn::stored(&T::array(T::Byte));
        assert_eq!(column.arrow_type(), T::array(T::Byte).arrow_type());
    }

    /// An array is never sent as the text of its elements, however deep it is.
    #[test]
    fn byte_arrays_in_arrays_of_arrays_are_binary() {
        let bytes = ColumnType::array(ColumnType::Byte);
        let middle = ColumnType::array(bytes.clone());
        let outer = ColumnType::array(middle.clone());
        let stored = cells_of(
            &outer,
            [Some(1), None],
            cells_of(
                &middle,
                [Some(2)],
                cells_of(
                    &bytes,
                    [Some(2), Some(0)],
                    Arc::new(Int8Array::from(vec![Some(1), None])),
                ),
            ),
        );
        let column = WireColumn::described(&outer);
        assert_eq!(column.arrow_type(), list_of(list_of(DataType::Binary)));
        let sent = column.cells(stored).unwrap();
        assert_eq!(sent.data_type(), &list_of(list_of(DataType::Binary)));
        let rows = sent.as_list::<i32>();
        assert!(rows.is_null(1));
        let middle = rows.value(0);
        let binary = middle.as_list::<i32>().value(0);
        let binary = binary.as_binary::<i32>();
        assert_eq!(binary.value(0), [1u8, 0x80].as_slice());
        assert_eq!(binary.value(1), [0u8; 0].as_slice());
    }

    /// A cell of lists of texts.
    type Texts = Option<Vec<Option<Vec<Option<String>>>>>;

    fn strings_of(cells: &ArrayRef) -> Vec<Texts> {
        cells
            .as_list::<i32>()
            .iter()
            .map(|cell| {
                cell.map(|cell| {
                    cell.as_list::<i32>()
                        .iter()
                        .map(|inner| {
                            inner.map(|inner| {
                                inner
                                    .as_string::<i32>()
                                    .iter()
                                    .map(|text| text.map(str::to_string))
                                    .collect()
                            })
                        })
                        .collect()
                })
            })
            .collect()
    }

    fn text(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    /// As Java sent `group_by` of `group_by` of a `Boolean` and of an `Instant` column:
    /// `[['true'], [None]]` and `[['1970-01-01T00:00:00Z', ...], [None]]`.
    #[test]
    fn the_elements_of_an_inner_object_vector_are_sent_as_their_text() {
        let inner = ColumnType::vector(ColumnType::Boolean);
        let outer = ColumnType::vector(inner.clone());
        let stored = cells_of(
            &outer,
            [Some(2), None, Some(0), Some(1)],
            cells_of(
                &inner,
                [Some(2), Some(1), None],
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            ),
        );
        let sent = WireColumn::described(&outer).cells(stored).unwrap();
        assert_eq!(*sent.data_type(), utf8_lists());
        assert_eq!(
            strings_of(&sent),
            vec![
                Some(vec![
                    Some(vec![text("true"), text("false")]),
                    Some(vec![None])
                ]),
                None,
                Some(vec![]),
                Some(vec![None]),
            ]
        );

        let inner = ColumnType::vector(ColumnType::Instant);
        let outer = ColumnType::array(inner.clone());
        let instants =
            TimestampNanosecondArray::from(vec![Some(0), None, Some(1)]).with_timezone("UTC");
        let stored = cells_of(
            &outer,
            [Some(1)],
            cells_of(&inner, [Some(3)], Arc::new(instants)),
        );
        let sent = WireColumn::described(&outer).cells(stored).unwrap();
        assert_eq!(
            strings_of(&sent),
            vec![Some(vec![Some(vec![
                text("1970-01-01T00:00:00Z"),
                None,
                text("1970-01-01T00:00:00.000000001Z")
            ])])]
        );
    }

    /// As Java sent three `group_by`s of an `int` column: `[['[1]', '[ ]'], ['[-3,1]']]`.
    #[test]
    fn a_vector_inside_an_inner_object_vector_is_sent_as_its_text() {
        let ints = ColumnType::vector(ColumnType::Int);
        let inner = ColumnType::vector(ints.clone());
        let outer = ColumnType::vector(inner.clone());
        let stored = cells_of(
            &outer,
            [Some(2)],
            cells_of(
                &inner,
                [Some(2), Some(1)],
                cells_of(
                    &ints,
                    [Some(1), Some(1), Some(2)],
                    Arc::new(Int32Array::from(vec![Some(1), None, Some(-3), Some(1)])),
                ),
            ),
        );
        let sent = WireColumn::described(&outer).cells(stored).unwrap();
        assert_eq!(
            strings_of(&sent),
            vec![Some(vec![
                Some(vec![text("[1]"), text("[ ]")]),
                Some(vec![text("[-3,1]")])
            ])]
        );
    }

    /// As Java sent `head` of an uploaded `list<int8>`: `b'\x01\x80\x80'`, null, empty.
    #[test]
    fn a_byte_array_is_sent_as_binary_with_the_null_byte_for_a_null_element() {
        let column_type = ColumnType::array(ColumnType::Byte);
        let stored = cells_of(
            &column_type,
            [Some(3), None, Some(0), Some(1)],
            Arc::new(Int8Array::from(vec![Some(1), None, Some(-128), Some(7)])),
        );
        let sent = WireColumn::described(&column_type)
            .cells(stored.clone())
            .unwrap();
        let binary = sent.as_binary::<i32>();
        let cells: Vec<Option<&[u8]>> = binary.iter().collect();
        assert_eq!(
            cells,
            vec![
                Some(&[1, 0x80, 0x80][..]),
                None,
                Some(&[][..]),
                Some(&[7][..])
            ]
        );
        // an uploaded one stays the list it was
        let echoed = WireColumn::stored(&column_type)
            .cells(stored.clone())
            .unwrap();
        assert_eq!(&echoed, &stored);
    }

    #[test]
    fn a_column_that_is_not_converted_is_sent_as_it_is_stored() {
        let column_type = ColumnType::vector(ColumnType::vector(ColumnType::Int));
        let stored = cells_of(
            &column_type,
            [Some(1)],
            cells_of(
                &ColumnType::vector(ColumnType::Int),
                [Some(1)],
                Arc::new(Int32Array::from(vec![1])),
            ),
        );
        let sent = WireColumn::described(&column_type)
            .cells(stored.clone())
            .unwrap();
        assert_eq!(&sent, &stored);
    }
}
