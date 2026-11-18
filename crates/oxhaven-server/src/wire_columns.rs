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
//! Five of the `java.math` and `java.time` types are sent otherwise than stored: a
//! `BigDecimal` without the zeros at its end (`1.5` for `1.50`), a `LocalDate` as
//! milliseconds where it is stored as days, a `LocalDateTime` as its text, because
//! Java has no Arrow type for one, a `Period` as its months alone and a `ZonedDateTime`
//! as its instant alone.
//!
//! A table a client uploaded is sent back in the layout it was sent in, which for these
//! types is the stored one: a client cannot upload a list whose elements are an
//! `ObjectVector`, and a `list<int8>` it uploaded stays one.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, Date64Array, IntervalYearMonthArray, ListArray,
    StringArray, TimestampNanosecondArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Field, Int32Type, Int8Type, IntervalUnit, TimestampNanosecondType,
};
use oxhaven_column::java_values::{big_decimal_parts, local_date_time_text, strip_trailing_zeros};
use oxhaven_column::sentinel::nulls_to_sentinels;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;

use crate::java_text::{self, object_texts};

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
        // Behavior: `BarrageUtil#arrowTypeFor`: `DATEMILLI` for a `LocalDate`, and the
        // text of a class it has no type for
        ColumnType::LocalDate => DataType::Date64,
        ColumnType::LocalDateTime => DataType::Utf8,
        // Behavior: `BarrageUtil#arrowTypeFor`: the months of a `Period`
        // (`IntervalUnit.YEAR_MONTH`), and the instant of a `ZonedDateTime` without its zone
        ColumnType::Period => DataType::Interval(IntervalUnit::YearMonth),
        ColumnType::ZonedDateTime => ColumnType::Instant.arrow_type(),
        // Behavior: `BarrageUtil#arrowTypeFor`: the text of a class it has no type for
        ColumnType::DayOfWeek | ColumnType::Object(_) => DataType::Utf8,
        other => other.arrow_type(),
    }
}

/// Whether a column of `column_type` holds a `BigDecimal` at any depth: the one type
/// whose cells are sent in the Arrow type they are stored in and are not the same bytes.
fn holds_decimal(column_type: &ColumnType) -> bool {
    match column_type.list_element() {
        Some(element) => holds_decimal(element),
        None => *column_type == ColumnType::BigDecimal,
    }
}

/// The cells of a `BigDecimal`, `LocalDate`, `LocalDateTime`, `Period`, `ZonedDateTime`,
/// `DayOfWeek` or `Object` column as Java sends them; `None` for a column of any other
/// type.
///
/// Behavior: `io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory#binaryFromBigDecimal`
/// (the scale and the unscaled integer of the number without the zeros at its end),
/// `#dateFromLocalDate` (the milliseconds of the day's start), `#utf8FromObject`,
/// `#intervalFromPeriod` (`getMonths() + getYears() * 12` in `int` arithmetic, which
/// drops the days), `#timestampFromZonedDateTime` (the instant, without the zone),
/// `#utf8FromObject` (the `toString` of a `DayOfWeek` and of the cells of an `Object`
/// column, a boxed primitive printing the value Java reserves for null as a number)
fn object_cells(
    column_type: &ColumnType,
    stored: &ArrayRef,
) -> Option<Result<ArrayRef, TableFailure>> {
    let unexpected = || {
        failure(format!(
            "a column of {column_type:?} holds {}",
            stored.data_type()
        ))
    };
    Some(match column_type {
        ColumnType::BigDecimal => (|| {
            let cells = stored.as_binary_opt::<i32>().ok_or_else(unexpected)?;
            let mut sent = Vec::with_capacity(cells.len());
            for cell in cells {
                let Some(cell) = cell else {
                    sent.push(None);
                    continue;
                };
                let (scale, unscaled) = big_decimal_parts(cell)
                    .ok_or_else(|| failure("a BigDecimal cell without a number"))?;
                sent.push(Some(strip_trailing_zeros(scale, unscaled)));
            }
            Ok(Arc::new(BinaryArray::from_iter(sent)) as ArrayRef)
        })(),
        ColumnType::LocalDate => (|| {
            let days = stored
                .as_primitive_opt::<Date32Type>()
                .ok_or_else(unexpected)?;
            let millis: Date64Array = days.unary(|days| i64::from(days) * 86_400_000);
            Ok(Arc::new(millis) as ArrayRef)
        })(),
        ColumnType::LocalDateTime => (|| {
            let nanos = stored
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(unexpected)?;
            let texts: StringArray = nanos
                .iter()
                .map(|nanos| nanos.map(local_date_time_text))
                .collect();
            Ok(Arc::new(texts) as ArrayRef)
        })(),
        ColumnType::Period => (|| {
            let parts = stored.as_struct_opt().ok_or_else(unexpected)?;
            let field = |at: usize| {
                parts
                    .column(at)
                    .as_primitive_opt::<Int32Type>()
                    .ok_or_else(unexpected)
            };
            let (years, months) = (field(0)?, field(1)?);
            let sent: IntervalYearMonthArray = (0..parts.len())
                .map(|row| {
                    parts.is_valid(row).then(|| {
                        months
                            .value(row)
                            .wrapping_add(years.value(row).wrapping_mul(12))
                    })
                })
                .collect();
            Ok(Arc::new(sent) as ArrayRef)
        })(),
        ColumnType::ZonedDateTime => (|| {
            let parts = stored.as_struct_opt().ok_or_else(unexpected)?;
            let instants = parts
                .column(0)
                .as_primitive_opt::<TimestampNanosecondType>()
                .ok_or_else(unexpected)?;
            let sent =
                TimestampNanosecondArray::new(instants.values().clone(), parts.nulls().cloned())
                    .with_data_type(ColumnType::Instant.arrow_type());
            Ok(Arc::new(sent) as ArrayRef)
        })(),
        ColumnType::DayOfWeek | ColumnType::Object(Some(_)) => {
            let class = match column_type {
                ColumnType::Object(Some(class)) => class.as_ref(),
                _ => column_type,
            };
            if !java_text::has_object_text(class) || *stored.data_type() != class.arrow_type() {
                return Some(Err(unexpected()));
            }
            Ok(Arc::new(StringArray::from(object_texts(class, stored))) as ArrayRef)
        }
        ColumnType::Object(None) => Ok(arrow::array::new_null_array(&DataType::Utf8, stored.len())),
        _ => return None,
    })
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
    if let Some(cells) = object_cells(column_type, stored) {
        return cells;
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
        .map(|handle| handle.map(|_| java_text::TABLE_TEXT))
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
            converted: wire_type(column_type) != column_type.arrow_type()
                || holds_decimal(column_type),
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

    /// What Java 42.9 sent for the same values, read from the Parquet files of
    /// `crates/oxhaven-parquet/tests/data` (`no_*.parquet`, `more_types.parquet`).
    /// A period is sent as `getMonths() + getYears() * 12` (`P1Y2M3D` as 14) and a zoned
    /// date and time as its instant, as Java 42.9 declared them for formula columns.
    #[test]
    fn a_period_is_sent_as_months_and_a_zoned_date_time_as_its_instant() {
        use arrow::array::{IntervalYearMonthArray, StringArray, StructArray};
        use arrow::datatypes::Fields;
        let DataType::Struct(fields) = ColumnType::Period.arrow_type() else {
            unreachable!()
        };
        let ints = |values: [i32; 3]| Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
        let periods = StructArray::new(
            fields,
            vec![ints([1, 0, 0]), ints([2, 0, i32::MAX]), ints([3, 7, 0])],
            Some(vec![true, false, true].into()),
        );
        let column = WireColumn::described(&ColumnType::Period);
        let cells = column.cells(Arc::new(periods)).unwrap();
        assert_eq!(cells.data_type(), &column.arrow_type());
        let months: Vec<Option<i32>> = cells
            .as_any()
            .downcast_ref::<IntervalYearMonthArray>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(months, [Some(14), None, Some(i32::MAX)]);

        let DataType::Struct(fields) = ColumnType::ZonedDateTime.arrow_type() else {
            unreachable!()
        };
        let fields: Fields = fields;
        let zoned = StructArray::new(
            fields,
            vec![
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_635_901_323_000_000_000, 0])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec!["GMT", "GMT"])),
            ],
            Some(vec![true, false].into()),
        );
        let column = WireColumn::described(&ColumnType::ZonedDateTime);
        let cells = column.cells(Arc::new(zoned)).unwrap();
        assert_eq!(cells.data_type(), &ColumnType::Instant.arrow_type());
        let instants: Vec<Option<i64>> = cells
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect();
        assert_eq!(instants, [Some(1_635_901_323_000_000_000), None]);
    }

    /// Seen on Java 42.9: `(Object) I` of a null `int` is sent as `"-2147483648"`, a
    /// `null` column as nulls of `string`, `dayOfWeek(...)` as the names of the days.
    #[test]
    fn an_object_column_and_a_day_of_week_are_sent_as_their_text() {
        let texts = |column_type: &ColumnType, stored: ArrayRef| -> Vec<Option<String>> {
            let column = WireColumn::described(column_type);
            assert_eq!(column.arrow_type(), DataType::Utf8);
            let cells = column.cells(stored).unwrap();
            cells
                .as_string::<i32>()
                .iter()
                .map(|cell| cell.map(str::to_string))
                .collect()
        };
        let boxed = Int32Array::from(vec![Some(1), Some(i32::MIN), None]);
        assert_eq!(
            texts(&ColumnType::object(Some(ColumnType::Int)), Arc::new(boxed)),
            [Some("1".into()), Some("-2147483648".into()), None]
        );
        assert_eq!(
            texts(
                &ColumnType::object(None),
                arrow::array::new_null_array(&DataType::Utf8, 2)
            ),
            [None, None]
        );
        let days = Int8Array::from(vec![Some(1), None, Some(7)]);
        assert_eq!(
            texts(&ColumnType::DayOfWeek, Arc::new(days)),
            [Some("MONDAY".into()), None, Some("SUNDAY".into())]
        );
        assert!(
            WireColumn::described(&ColumnType::object(Some(ColumnType::Int)))
                .cells(Arc::new(StringArray::from(vec!["1"])))
                .is_err()
        );
    }

    #[test]
    fn the_java_math_and_time_types_are_sent_as_java_sends_them() {
        use arrow::array::{Date32Array, Time64NanosecondArray};
        use arrow::datatypes::TimeUnit;
        use oxhaven_column::java_values::{big_decimal_cell, big_integer_cell};
        use ColumnType as T;
        let sent = |column_type: &ColumnType, stored: ArrayRef| {
            let column = WireColumn::described(column_type);
            let cells = column.cells(stored).unwrap();
            assert_eq!(cells.data_type(), &column.arrow_type(), "{column_type:?}");
            cells
        };

        // 1.50, 0.00 and -1.50: without the zeros at the end, and zero with scale 0
        let decimal =
            |unscaled: i128, scale: i32| Some(big_decimal_cell(scale, &big_integer_cell(unscaled)));
        let stored =
            BinaryArray::from_iter([decimal(150, 2), None, decimal(0, 2), decimal(-150, 2)]);
        let cells = sent(&T::BigDecimal, Arc::new(stored));
        let cells: Vec<Option<&[u8]>> = cells.as_binary::<i32>().iter().collect();
        let expected: [Option<&[u8]>; 4] = [
            Some(&[1, 0, 0, 0, 0x0f]),
            None,
            Some(&[0, 0, 0, 0, 0x00]),
            Some(&[1, 0, 0, 0, 0xf1]),
        ];
        assert_eq!(cells, expected);
        assert!(WireColumn::described(&T::BigDecimal).converted);
        assert!(WireColumn::described(&T::vector(T::BigDecimal)).converted);

        // an integer goes as it is stored
        let stored: ArrayRef = Arc::new(BinaryArray::from_iter([Some(big_integer_cell(1)), None]));
        assert!(!WireColumn::described(&T::BigInteger).converted);
        assert_eq!(wire_type(&T::BigInteger), DataType::Binary);
        assert_eq!(
            sent(&T::BigInteger, stored.clone()).to_data(),
            stored.to_data()
        );

        // 2024-01-02 and the day before 1970: milliseconds
        let days = Date32Array::from(vec![Some(19_724), None, Some(-1)]);
        let cells = sent(&T::LocalDate, Arc::new(days));
        assert_eq!(cells.data_type(), &DataType::Date64);
        let millis: Vec<Option<i64>> = cells
            .as_primitive::<arrow::datatypes::Date64Type>()
            .iter()
            .collect();
        assert_eq!(millis, [Some(1_704_153_600_000), None, Some(-86_400_000)]);

        let times: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![
            Some(3_723_000_000_000),
            None,
        ]));
        assert_eq!(
            wire_type(&T::LocalTime),
            DataType::Time64(TimeUnit::Nanosecond)
        );
        assert_eq!(
            sent(&T::LocalTime, times.clone()).to_data(),
            times.to_data()
        );

        // Java has no Arrow type for a LocalDateTime and sends its text
        let local = TimestampNanosecondArray::from(vec![Some(0), None, Some(-1)]);
        let cells = sent(&T::LocalDateTime, Arc::new(local));
        let texts: Vec<Option<&str>> = cells.as_string::<i32>().iter().collect();
        assert_eq!(
            texts,
            [
                Some("1970-01-01T00:00"),
                None,
                Some("1969-12-31T23:59:59.999999999")
            ]
        );
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

    #[test]
    fn a_table_is_sent_as_the_text_java_has_for_it_and_never_as_its_handle() {
        use oxhaven_contracts::schema::TableClass;
        for class in [TableClass::Table, TableClass::QueryTable] {
            let column = WireColumn::described(&ColumnType::Table(class));
            assert_eq!(column.arrow_type(), DataType::Utf8);
            let stored: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![Some(41), None]));
            let sent = column.cells(stored).unwrap();
            let sent = sent.as_string::<i32>();
            assert_eq!(sent.iter().collect::<Vec<_>>(), [Some("QueryTable"), None]);
        }
    }
}
