//! Sort keys: the values of the sort columns of a set of rows, in a form that compares the
//! way Java compares the rows, kept in a few flat buffers for the whole set and never in an
//! allocation per row.
//!
//! The values are first brought to Java's notion of equality and order: every NaN is one
//! value and the greatest, `-0.0` is `0.0`, a string orders by its UTF-16 code units, and
//! null is the least value of every type. A descending column negates the comparison:
//! nulls last, NaN first.
//!
//! There are three forms. A sort by one column of a fixed-width type keeps each row's value
//! as an unsigned integer whose order is the column's order; a sort by one string column
//! keeps the strings' bytes; any other sort keeps Arrow's row format over its columns.

use super::{SortColumn, SortDirection, SortError};
use crate::ops::zoned;
use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, StringArray};
use arrow::buffer::Buffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, DurationNanosecondType, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Time64NanosecondType, TimeUnit,
    TimestampNanosecondType, UInt16Type,
};
use arrow::row::{RowConverter, Rows, SortField};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use std::cmp::Ordering;
use std::sync::Arc;

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

struct KeyColumn {
    /// Index of the column in the parent's definition.
    index: usize,
    column_type: ColumnType,
    absolute: bool,
}

/// How an encoder keeps its keys.
enum Form {
    /// One column of a fixed-width type, in this direction.
    Fixed { descending: bool },
    /// One string column, in this direction.
    Text { descending: bool },
    /// Arrow's row format over every sort column.
    Rows(RowConverter),
}

/// Turns rows of a table into [`SortKeys`] for one sort specification.
pub(super) struct KeyEncoder {
    columns: Vec<KeyColumn>,
    form: Form,
}

impl KeyEncoder {
    /// An encoder for sorting `parent` by `columns`.
    ///
    /// Behavior: the checks of io.deephaven.engine.table.impl.SortOperation#SortOperation and
    /// io.deephaven.engine.table.impl.BaseTable#assertSortable
    pub(super) fn new(parent: &dyn Table, columns: &[SortColumn]) -> Result<Self, SortError> {
        Self::build(parent, columns, true)
    }

    /// An encoder that keeps the keys of one fixed-width or string column in their own
    /// form when `typed`, and in Arrow's row format otherwise.
    fn build(parent: &dyn Table, columns: &[SortColumn], typed: bool) -> Result<Self, SortError> {
        let definition = parent.definition();
        let mut key_columns = Vec::with_capacity(columns.len());
        let mut missing = Vec::new();
        for column in columns {
            match definition.index_of(&column.column) {
                Some(index) => key_columns.push(KeyColumn {
                    index,
                    column_type: definition.columns()[index].column_type.clone(),
                    absolute: column.absolute,
                }),
                None => missing.push(column.column.clone()),
            }
        }
        if !missing.is_empty() {
            return Err(SortError::NoSuchColumn(missing));
        }
        let mut fields = Vec::with_capacity(columns.len());
        for (key, column) in key_columns.iter().zip(columns) {
            if key.absolute {
                check_absolute(&column.column, &key.column_type)?;
            }
            let descending = column.direction == SortDirection::Descending;
            let options = SortOptions {
                descending,
                nulls_first: !descending,
            };
            fields.push(SortField::new_with_options(
                encoded_type(&column.column, &key.column_type)?,
                options,
            ));
        }
        let form = match (&key_columns[..], columns) {
            ([key], [column]) if typed && is_fixed_width(&key.column_type) => Form::Fixed {
                descending: column.direction == SortDirection::Descending,
            },
            ([key], [column]) if typed && key.column_type == ColumnType::String => Form::Text {
                descending: column.direction == SortDirection::Descending,
            },
            _ => Form::Rows(RowConverter::new(fields).map_err(failure)?),
        };
        Ok(Self {
            columns: key_columns,
            form,
        })
    }

    /// Indices, in the parent's definition, of the columns sorted by.
    pub(super) fn column_indices(&self) -> Vec<usize> {
        self.columns.iter().map(|column| column.index).collect()
    }

    /// The sort keys of the rows `rows`, in row-key order, from the current values.
    pub(super) fn encode(
        &self,
        parent: &dyn Table,
        rows: &RowSet,
    ) -> Result<SortKeys, TableFailure> {
        let mut values = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            values.push(parent.column(column.index).chunk(rows)?);
        }
        let keys = match &self.form {
            Form::Fixed { descending } => {
                let column = &self.columns[0];
                let (values, nulls) = fixed_keys(&values[0], column.absolute)?;
                SortKeys::fixed(values, nulls, *descending)
            }
            Form::Text { descending } => {
                let text = downcast::<StringArray>(&values[0])?;
                let mut offsets = Vec::with_capacity(text.len() + 1);
                let first = text.value_offsets()[0];
                offsets.extend(text.value_offsets().iter().map(|o| (o - first) as usize));
                let bytes = &text.value_data()[first as usize..];
                let bytes = bytes[..*offsets.last().unwrap_or(&0)]
                    .iter()
                    .map(|&byte| utf16_ordered(byte))
                    .collect();
                let nulls = text
                    .nulls()
                    .filter(|nulls| nulls.null_count() > 0)
                    .map(|nulls| nulls.iter().map(|valid| !valid).collect());
                SortKeys {
                    descending: *descending,
                    nulls,
                    values: Values::Text { offsets, bytes },
                }
            }
            Form::Rows(converter) => {
                let comparable = self
                    .columns
                    .iter()
                    .zip(&values)
                    .map(|(column, values)| {
                        comparable(values, &column.column_type, column.absolute)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                SortKeys {
                    descending: false,
                    nulls: None,
                    values: Values::Rows(converter.convert_columns(&comparable).map_err(failure)?),
                }
            }
        };
        Ok(keys)
    }

    /// Keys of no rows, of this encoder's form, with room for `capacity` rows like those of
    /// `like`.
    pub(super) fn empty(&self, capacity: usize, like: &SortKeys) -> SortKeys {
        let values = match (&self.form, &like.values) {
            (Form::Fixed { .. }, _) => Values::Fixed(Vec::with_capacity(capacity)),
            (Form::Text { .. }, Values::Text { bytes, offsets }) => {
                let average = bytes.len() / offsets.len().max(1);
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                Values::Text {
                    offsets,
                    bytes: Vec::with_capacity(average * capacity),
                }
            }
            (Form::Text { .. }, _) => Values::Text {
                offsets: vec![0],
                bytes: Vec::new(),
            },
            (Form::Rows(converter), _) => Values::Rows(converter.empty_rows(capacity, 0)),
        };
        let descending = match self.form {
            Form::Fixed { descending } | Form::Text { descending } => descending,
            Form::Rows(_) => false,
        };
        SortKeys {
            descending,
            nulls: None,
            values,
        }
    }
}

/// The sort keys of a sequence of rows, compared by position in the sequence.
pub(super) struct SortKeys {
    /// For the `Fixed` and `Text` forms: whether the comparison is negated, which puts
    /// nulls last. The values of a descending `Fixed` key are already negated.
    descending: bool,
    /// Which rows are null, when any is. Not used by the row format, which has its own.
    nulls: Option<Vec<bool>>,
    values: Values,
}

enum Values {
    /// A value per row whose unsigned order is the column's order; 0 for a null.
    Fixed(Vec<u64>),
    /// The strings of the rows, row `i` at `bytes[offsets[i]..offsets[i + 1]]`, with the
    /// bytes changed by [`utf16_ordered`]; empty for a null.
    Text { offsets: Vec<usize>, bytes: Vec<u8> },
    /// Arrow's row format.
    Rows(Rows),
}

impl SortKeys {
    fn fixed(mut values: Vec<u64>, nulls: Option<Vec<bool>>, descending: bool) -> Self {
        if descending {
            for value in &mut values {
                *value = !*value;
            }
            if let Some(nulls) = &nulls {
                for (value, _) in values.iter_mut().zip(nulls).filter(|(_, null)| **null) {
                    *value = 0;
                }
            }
        }
        Self {
            descending,
            nulls,
            values: Values::Fixed(values),
        }
    }

    pub(super) fn len(&self) -> usize {
        match &self.values {
            Values::Fixed(values) => values.len(),
            Values::Text { offsets, .. } => offsets.len() - 1,
            Values::Rows(rows) => rows.num_rows(),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        self.nulls.as_ref().is_some_and(|nulls| nulls[row])
    }

    /// Null rows come first in an ascending sort and last in a descending one.
    fn rank(&self, row: usize) -> bool {
        self.is_null(row) == self.descending
    }

    /// The order of row `row` of these keys against row `other_row` of `other`, which come
    /// from the same encoder.
    pub(super) fn compare(&self, row: usize, other: &SortKeys, other_row: usize) -> Ordering {
        let rank = self.rank(row).cmp(&other.rank(other_row));
        if rank != Ordering::Equal {
            return rank;
        }
        match (&self.values, &other.values) {
            (Values::Fixed(values), Values::Fixed(others)) => values[row].cmp(&others[other_row]),
            (Values::Text { .. }, Values::Text { .. }) => {
                let order = self.text(row).cmp(other.text(other_row));
                if self.descending {
                    order.reverse()
                } else {
                    order
                }
            }
            (Values::Rows(rows), Values::Rows(others)) => rows.row(row).cmp(&others.row(other_row)),
            _ => unreachable!("sort keys of one encoder have one form"),
        }
    }

    fn text(&self, row: usize) -> &[u8] {
        let Values::Text { offsets, bytes } = &self.values else {
            unreachable!("text of a text key")
        };
        &bytes[offsets[row]..offsets[row + 1]]
    }

    /// Appends row `row` of `other`, which come from the same encoder.
    pub(super) fn push_from(&mut self, other: &SortKeys, row: usize) {
        let null = other.is_null(row);
        if null && self.nulls.is_none() {
            self.nulls = Some(vec![false; self.len()]);
        }
        if let Some(nulls) = &mut self.nulls {
            nulls.push(null);
        }
        match (&mut self.values, &other.values) {
            (Values::Fixed(values), Values::Fixed(others)) => values.push(others[row]),
            (Values::Text { offsets, bytes }, Values::Text { .. }) => {
                bytes.extend_from_slice(other.text(row));
                offsets.push(bytes.len());
            }
            (Values::Rows(rows), Values::Rows(others)) => rows.push(others.row(row)),
            _ => unreachable!("sort keys of one encoder have one form"),
        }
    }

    /// The bytes of row `row` of a `Text` or `Rows` key.
    fn bytes(&self, row: usize) -> &[u8] {
        match &self.values {
            Values::Text { .. } => self.text(row),
            Values::Rows(rows) => rows.row(row).data(),
            Values::Fixed(_) => unreachable!("bytes of a key of bytes"),
        }
    }

    /// The positions of the rows in sorted order. Rows with equal keys keep their order.
    ///
    /// Each row is sorted as a small value that carries its key, or the start of it, with
    /// its position, so that most comparisons read nothing else: one unsigned integer for
    /// a `Fixed` key, [`prefix`] of the bytes otherwise. Ties of position-carrying values
    /// are broken by position, which makes the sort stable.
    pub(super) fn order(&self) -> Vec<u32> {
        let size = u32::try_from(self.len()).expect("a sort of fewer than 2^32 rows");
        // Nulls are sorted apart, in their order.
        let mut nulls = Vec::new();
        let sorted: Vec<u32> = match &self.values {
            Values::Fixed(values) => {
                let mut pairs: Vec<(u64, u32)> = Vec::with_capacity(values.len());
                for (position, &value) in (0..size).zip(values) {
                    if self.is_null(position as usize) {
                        nulls.push(position);
                    } else {
                        pairs.push((value, position));
                    }
                }
                pairs.sort_unstable();
                pairs.into_iter().map(|(_, position)| position).collect()
            }
            Values::Text { .. } | Values::Rows(_) => {
                let mut keyed: Vec<(u64, u64, u32)> = Vec::with_capacity(size as usize);
                for position in 0..size {
                    if self.is_null(position as usize) {
                        nulls.push(position);
                    } else {
                        let (high, low) = prefix(self.bytes(position as usize));
                        keyed.push((high, low, position));
                    }
                }
                let descending = self.descending;
                keyed.sort_unstable_by(|a, b| {
                    let order = (a.0, a.1).cmp(&(b.0, b.1)).then_with(|| {
                        if a.1 & 0xff == PREFIX_LONG {
                            self.bytes(a.2 as usize).cmp(self.bytes(b.2 as usize))
                        } else {
                            Ordering::Equal
                        }
                    });
                    let order = if descending { order.reverse() } else { order };
                    order.then(a.2.cmp(&b.2))
                });
                keyed.into_iter().map(|(_, _, position)| position).collect()
            }
        };
        if self.descending {
            let mut sorted = sorted;
            sorted.extend(nulls);
            sorted
        } else {
            nulls.extend(sorted);
            nulls
        }
    }
}

/// The length byte of a [`prefix`] of more than 15 bytes.
const PREFIX_LONG: u64 = 16;

/// The first 15 bytes of `bytes`, padded with zeros, then its length, or [`PREFIX_LONG`] if
/// it is longer, as a big-endian 128-bit integer in two halves. Two byte strings of at most
/// 15 bytes order as their prefixes: a string that is a prefix of another pads to the
/// other's first bytes or below them, and is shorter. Longer strings order by the prefix
/// first and by all their bytes when the prefixes are equal.
fn prefix(bytes: &[u8]) -> (u64, u64) {
    let mut buffer = [0u8; 16];
    let size = bytes.len().min(15);
    buffer[..size].copy_from_slice(&bytes[..size]);
    buffer[15] = bytes.len().min(PREFIX_LONG as usize) as u8;
    let whole = u128::from_be_bytes(buffer);
    ((whole >> 64) as u64, whole as u64)
}

fn downcast<T: 'static>(values: &ArrayRef) -> Result<&T, TableFailure> {
    values
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| failure("a sort column's values do not have the column's type"))
}

/// Whether a column of this type is sorted by one unsigned integer per row.
fn is_fixed_width(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Boolean
            | ColumnType::Byte
            | ColumnType::Char
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long
            | ColumnType::Float
            | ColumnType::Double
            | ColumnType::Instant
            | ColumnType::LocalDate
            | ColumnType::LocalTime
            | ColumnType::LocalDateTime
            | ColumnType::Duration
            | ColumnType::DayOfWeek
    )
}

/// A signed integer as an unsigned one of the same order.
fn signed_key(value: i64) -> u64 {
    (value as u64) ^ (1 << 63)
}

/// A floating point value as an unsigned integer whose order is Java's: every NaN is one
/// value and the greatest, and `-0.0` is `0.0`.
///
/// Behavior: io.deephaven.util.compare.DoubleComparisons#compare
fn float_key(value: f64) -> u64 {
    let value = if value.is_nan() {
        f64::NAN
    } else if value == 0.0 {
        0.0
    } else {
        value
    };
    let bits = value.to_bits();
    if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    }
}

/// The keys of the cells of a primitive array: `key` of each value, and a null where the
/// cell is null or `key` gives none.
fn primitive_keys<T: ArrowPrimitiveType>(
    values: &ArrayRef,
    key: impl Fn(T::Native) -> Option<u64>,
) -> Result<(Vec<u64>, Option<Vec<bool>>), TableFailure> {
    let array = values
        .as_primitive_opt::<T>()
        .ok_or_else(|| failure("a sort column's values do not have the column's type"))?;
    let size = array.len();
    let mut keys = Vec::with_capacity(size);
    let mut nulls: Option<Vec<bool>> = None;
    let mut push_null = |keys: &mut Vec<u64>, row: usize| {
        nulls.get_or_insert_with(|| vec![false; size])[row] = true;
        keys.push(0);
    };
    match array.nulls().filter(|nulls| nulls.null_count() > 0) {
        None => {
            for (row, &value) in array.values().iter().enumerate() {
                match key(value) {
                    Some(key) => keys.push(key),
                    None => push_null(&mut keys, row),
                }
            }
        }
        Some(valid) => {
            for (row, &value) in array.values().iter().enumerate() {
                match key(value).filter(|_| valid.is_valid(row)) {
                    Some(key) => keys.push(key),
                    None => push_null(&mut keys, row),
                }
            }
        }
    }
    Ok((keys, nulls))
}

/// The key of an integer, or of its absolute value. The minimum value has no absolute
/// value; in Java it is the null sentinel, and `abs(null)` is null.
///
/// Behavior: io.deephaven.function.Numeric#abs
fn integer_keys<T: ArrowPrimitiveType>(
    values: &ArrayRef,
    absolute: bool,
    min: i64,
) -> Result<(Vec<u64>, Option<Vec<bool>>), TableFailure>
where
    T::Native: Into<i64>,
{
    if absolute {
        primitive_keys::<T>(values, |value| {
            let value: i64 = value.into();
            (value != min).then(|| signed_key(value.abs()))
        })
    } else {
        primitive_keys::<T>(values, |value| Some(signed_key(value.into())))
    }
}

fn float_keys<T: ArrowPrimitiveType>(
    values: &ArrayRef,
    absolute: bool,
) -> Result<(Vec<u64>, Option<Vec<bool>>), TableFailure>
where
    T::Native: Into<f64>,
{
    if absolute {
        primitive_keys::<T>(values, |value| Some(float_key(value.into().abs())))
    } else {
        primitive_keys::<T>(values, |value| Some(float_key(value.into())))
    }
}

/// The keys of a column of a fixed-width type, ascending, and which cells are null.
///
/// Behavior: io.deephaven.util.compare.DoubleComparisons#compare,
/// io.deephaven.util.compare.CharComparisons#compare (unsigned),
/// java.lang.Boolean#compareTo, java.time.LocalDate#compareTo,
/// java.time.LocalTime#compareTo, java.time.LocalDateTime#compareTo,
/// java.time.Duration#compareTo, java.lang.Enum#compareTo; null is the least value of each,
/// as io.deephaven.util.compare.ObjectComparisons#compare makes it
fn fixed_keys(
    values: &ArrayRef,
    absolute: bool,
) -> Result<(Vec<u64>, Option<Vec<bool>>), TableFailure> {
    match values.data_type() {
        DataType::Boolean => {
            let array = values.as_boolean();
            let nulls = array
                .nulls()
                .filter(|nulls| nulls.null_count() > 0)
                .map(|nulls| nulls.iter().map(|valid| !valid).collect::<Vec<bool>>());
            let keys = array.values().iter().map(u64::from).collect::<Vec<u64>>();
            let keys = match &nulls {
                Some(nulls) => keys
                    .iter()
                    .zip(nulls)
                    .map(|(k, n)| if *n { 0 } else { *k })
                    .collect(),
                None => keys,
            };
            Ok((keys, nulls))
        }
        DataType::Int8 => integer_keys::<Int8Type>(values, absolute, i8::MIN.into()),
        DataType::Int16 => integer_keys::<Int16Type>(values, absolute, i16::MIN.into()),
        DataType::Int32 => integer_keys::<Int32Type>(values, absolute, i32::MIN.into()),
        DataType::Int64 => integer_keys::<Int64Type>(values, absolute, i64::MIN),
        DataType::UInt16 => integer_keys::<UInt16Type>(values, false, 0),
        DataType::Date32 => integer_keys::<Date32Type>(values, false, 0),
        DataType::Time64(TimeUnit::Nanosecond) => {
            integer_keys::<Time64NanosecondType>(values, false, 0)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            integer_keys::<TimestampNanosecondType>(values, false, 0)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            integer_keys::<DurationNanosecondType>(values, false, 0)
        }
        DataType::Float32 => float_keys::<Float32Type>(values, absolute),
        DataType::Float64 => float_keys::<Float64Type>(values, absolute),
        other => Err(failure(format!(
            "a sort column has values of type {other}, which is not of a fixed width"
        ))),
    }
}

/// A byte of UTF-8 changed so that byte order is the order of UTF-16 code units: the lead
/// bytes of the characters U+E000 to U+FFFF (0xEE and 0xEF) become 0xF5 and 0xF6, which UTF-8
/// never uses, so that those characters order after the characters above U+FFFF (lead bytes
/// 0xF0 to 0xF4), as their UTF-16 code units order after surrogates. Neither byte is a
/// continuation byte, the order of every other pair of characters is unchanged, and the
/// first byte at which two strings differ is still in the first character they differ in.
///
/// Behavior: java.lang.String#compareTo, which compares UTF-16 code units
fn utf16_ordered(byte: u8) -> u8 {
    match byte {
        0xEE => 0xF5,
        0xEF => 0xF6,
        other => other,
    }
}

/// Behavior: `abs` exists for the numeric primitives only, in
/// `engine/function/src/templates/Numeric.ftl` (io.deephaven.function.Numeric#abs); the
/// server sorts by `abs(Column)` for an absolute sort
/// (io.deephaven.engine.table.impl.AbsoluteSortColumnConventions#makeSelectable).
fn check_absolute(name: &str, column_type: &ColumnType) -> Result<(), SortError> {
    match column_type {
        ColumnType::Byte
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double => Ok(()),
        ColumnType::Boolean | ColumnType::String | ColumnType::Instant => {
            Err(SortError::AbsoluteNotSupported {
                column: name.to_string(),
                column_type: column_type.clone(),
            })
        }
        // Whether the formula `abs(charColumn)` compiles, by widening to `int`, is decided
        // by the query language parser's overload resolution.
        ColumnType::Char => unported!(
            "io.deephaven.engine.table.impl.lang.QueryLanguageParser#visit(MethodCallExpr)",
            "abs of a char column for an absolute sort"
        ),
        other => unported!(
            "io.deephaven.engine.table.impl.SortOperation#SortOperation",
            "sort by a column of type {other:?}"
        ),
    }
}

/// The Arrow type of the values [`comparable`] returns for a column type.
///
/// A date, a time of day, a date and time, a duration and a day of the week are their
/// numbers (days, nanoseconds, the day's number), whose order is Java's `compareTo` of the
/// class; a null is the least value of each, as `ObjectComparisons#compare` makes it.
/// `ZoneId`, `Period` and a column Java declares as `Object` are no `Comparable` and are
/// refused.
///
/// Behavior: io.deephaven.engine.table.impl.SortOperation#SortOperation (the check that
/// a column's class is `Comparable` or primitive), java.time.LocalDate#compareTo,
/// java.time.LocalTime#compareTo, java.time.LocalDateTime#compareTo,
/// java.time.Duration#compareTo, java.lang.Enum#compareTo
fn encoded_type(name: &str, column_type: &ColumnType) -> Result<DataType, SortError> {
    Ok(match column_type {
        ColumnType::String => DataType::Binary,
        ColumnType::ZonedDateTime => zoned::sort_key_type(),
        ColumnType::Boolean
        | ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::Instant
        | ColumnType::LocalDate
        | ColumnType::LocalTime
        | ColumnType::LocalDateTime
        | ColumnType::Duration
        | ColumnType::DayOfWeek => column_type.arrow_type(),
        ColumnType::Period | ColumnType::ZoneId | ColumnType::Object(_) => {
            let class = match column_type {
                ColumnType::Period => "java.time.Period",
                ColumnType::ZoneId => "java.time.ZoneId",
                _ => "java.lang.Object",
            };
            return Err(SortError::NotSortableColumn(format!(
                "{name} is not a sortable type: class {class}"
            )));
        }
        other => unported!(
            "io.deephaven.engine.table.impl.SortOperation#SortOperation",
            "sort by a column of type {other:?}"
        ),
    })
}

/// Floating point values with one NaN and one zero, and optionally without their sign.
macro_rules! canonical_floats {
    ($array:ty, $arrow_type:ty, $native:ty, $values:expr, $absolute:expr) => {{
        let values = downcast::<$array>($values)?;
        let absolute = $absolute;
        Arc::new(values.unary::<_, $arrow_type>(move |value| {
            if value.is_nan() {
                <$native>::NAN
            } else if absolute || value == 0.0 {
                value.abs()
            } else {
                value
            }
        })) as ArrayRef
    }};
}

/// The absolute values of an integer column. The minimum value has no absolute value; in
/// Java it is the null sentinel, and `abs(null)` is null.
macro_rules! absolute_integers {
    ($array:ty, $values:expr) => {{
        let values = downcast::<$array>($values)?;
        Arc::new(
            values
                .iter()
                .map(|value| value.and_then(|value| value.checked_abs()))
                .collect::<$array>(),
        ) as ArrayRef
    }};
}

/// `values` as an array whose Arrow row format orders like Java's comparison for the type.
///
/// Behavior: io.deephaven.util.compare.DoubleComparisons#compare (NaN equal to NaN and
/// greatest, `-0.0` equal to `0.0`), io.deephaven.util.compare.ObjectComparisons#compare
/// (`String#compareTo`, UTF-16 code unit order), io.deephaven.util.compare.CharComparisons#compare
/// io.deephaven.function.Numeric#abs and java.time.chrono.ChronoZonedDateTime#compareTo.
/// Null first in every type is the row format's
/// `nulls_first`.
fn comparable(
    values: &ArrayRef,
    column_type: &ColumnType,
    absolute: bool,
) -> Result<ArrayRef, TableFailure> {
    use arrow::array::{Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array};
    Ok(match column_type {
        ColumnType::Float => canonical_floats!(Float32Array, Float32Type, f32, values, absolute),
        ColumnType::Double => canonical_floats!(Float64Array, Float64Type, f64, values, absolute),
        ColumnType::Byte if absolute => absolute_integers!(Int8Array, values),
        ColumnType::Short if absolute => absolute_integers!(Int16Array, values),
        ColumnType::Int if absolute => absolute_integers!(Int32Array, values),
        ColumnType::Long if absolute => absolute_integers!(Int64Array, values),
        ColumnType::ZonedDateTime => zoned::sort_key(values)?,
        ColumnType::String => {
            // The bytes of the strings, in one buffer, changed to order as UTF-16 does.
            let values = downcast::<StringArray>(values)?;
            let data = values.values();
            let data = if data.iter().any(|&byte| byte != utf16_ordered(byte)) {
                Buffer::from_iter(data.iter().map(|&byte| utf16_ordered(byte)))
            } else {
                data.clone()
            };
            Arc::new(BinaryArray::new(
                values.offsets().clone(),
                data,
                values.nulls().cloned(),
            ))
        }
        _ => values.clone(),
    })
}

#[cfg(test)]
#[path = "keys_tests.rs"]
mod tests;
