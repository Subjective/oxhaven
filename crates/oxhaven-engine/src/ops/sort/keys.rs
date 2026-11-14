//! Sort keys: the values of the sort columns of a row as bytes that compare, byte by byte,
//! the way Java compares the row.
//!
//! The bytes are Arrow's row format over the sort columns after each column is brought to
//! Java's notion of equality and order: every NaN is one value and the greatest, `-0.0` is
//! `0.0`, a string orders by its UTF-16 code units, and null is the least value of every
//! type. A descending column inverts its bytes, which is Java's negated comparison: nulls
//! last, NaN first.

use super::{SortColumn, SortDirection, SortError};
use arrow::array::{
    Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use arrow::row::{RowConverter, SortField};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use std::sync::Arc;

/// The bytes one row sorts by.
pub(super) type SortKey = Box<[u8]>;

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

/// Turns rows of a table into [`SortKey`]s for one sort specification.
pub(super) struct KeyEncoder {
    columns: Vec<KeyColumn>,
    converter: RowConverter,
}

fn downcast<T: 'static>(values: &ArrayRef) -> Result<&T, TableFailure> {
    values
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| failure("a sort column's values do not have the column's type"))
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

impl KeyEncoder {
    /// An encoder for sorting `parent` by `columns`.
    ///
    /// Behavior: the checks of io.deephaven.engine.table.impl.SortOperation#SortOperation and
    /// io.deephaven.engine.table.impl.BaseTable#assertSortable
    pub(super) fn new(parent: &dyn Table, columns: &[SortColumn]) -> Result<Self, SortError> {
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
                encoded_type(&key.column_type)?,
                options,
            ));
        }
        let converter = RowConverter::new(fields).map_err(failure)?;
        Ok(Self {
            columns: key_columns,
            converter,
        })
    }

    /// Indices, in the parent's definition, of the columns sorted by.
    pub(super) fn column_indices(&self) -> Vec<usize> {
        self.columns.iter().map(|column| column.index).collect()
    }

    /// The sort key of each row of `rows`, in row-key order, from the current values.
    pub(super) fn encode(
        &self,
        parent: &dyn Table,
        rows: &RowSet,
    ) -> Result<Vec<SortKey>, TableFailure> {
        let values = self
            .columns
            .iter()
            .map(|column| {
                let values = parent.column(column.index).chunk(rows)?;
                comparable(&values, &column.column_type, column.absolute)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let encoded = self.converter.convert_columns(&values).map_err(failure)?;
        Ok((0..encoded.num_rows())
            .map(|row| encoded.row(row).as_ref().into())
            .collect())
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
fn encoded_type(column_type: &ColumnType) -> Result<DataType, SortError> {
    Ok(match column_type {
        ColumnType::String => DataType::Binary,
        ColumnType::Boolean
        | ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::Instant => column_type.arrow_type(),
        other => unported!(
            "io.deephaven.engine.table.impl.SortOperation#SortOperation",
            "sort by a column of type {other:?}"
        ),
    })
}

/// `values` as an array whose Arrow row format orders like Java's comparison for the type.
///
/// Behavior: io.deephaven.util.compare.DoubleComparisons#compare (NaN equal to NaN and
/// greatest, `-0.0` equal to `0.0`), io.deephaven.util.compare.ObjectComparisons#compare
/// (`String#compareTo`, UTF-16 code unit order), io.deephaven.util.compare.CharComparisons#compare
/// and io.deephaven.function.Numeric#abs. Null first in every type is the row format's
/// `nulls_first`.
fn comparable(
    values: &ArrayRef,
    column_type: &ColumnType,
    absolute: bool,
) -> Result<ArrayRef, TableFailure> {
    Ok(match column_type {
        ColumnType::Float => canonical_floats!(Float32Array, Float32Type, f32, values, absolute),
        ColumnType::Double => canonical_floats!(Float64Array, Float64Type, f64, values, absolute),
        ColumnType::Byte if absolute => absolute_integers!(Int8Array, values),
        ColumnType::Short if absolute => absolute_integers!(Int16Array, values),
        ColumnType::Int if absolute => absolute_integers!(Int32Array, values),
        ColumnType::Long if absolute => absolute_integers!(Int64Array, values),
        ColumnType::String => {
            // Big-endian UTF-16 code units compare, as bytes, like the code units.
            let values = downcast::<StringArray>(values)?;
            Arc::new(
                values
                    .iter()
                    .map(|value| {
                        value.map(|value| {
                            value
                                .encode_utf16()
                                .flat_map(u16::to_be_bytes)
                                .collect::<Vec<u8>>()
                        })
                    })
                    .collect::<BinaryArray>(),
            )
        }
        _ => values.clone(),
    })
}
