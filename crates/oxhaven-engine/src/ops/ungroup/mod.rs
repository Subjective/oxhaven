//! `ungroup`. Task 4.7.
//!
//! `ungroup` replaces array columns by their elements: the result has a row for every
//! element, in the order of the source's rows and within a row of the array (PARITY.md,
//! "Order guarantees per operation"). The columns that are not ungrouped repeat the row's
//! value. The arrays of one row must have one size, unless `null_fill` is set, which
//! gives the row the size of its largest array and fills the others with nulls.
//!
//! # What is here and what is not
//!
//! This module was written before the contracts had array-valued column types
//! (`ColumnType::Vector`, `ColumnType::Array`), and does not read their list cells yet.
//! [`ungroup`] answers the two cases that need no arrays, as Java does: without named
//! columns it returns a source without array columns as it is, and a named column that
//! does not exist or is not an array is refused. A vector or array column is recognized
//! and then stops at [`arrays_of`], which is `unported!`.
//!
//! Everything behind that is built against a private stand-in for an array column
//! ([`sources::ArrayColumn`]: the sizes of the arrays of some rows, and elements by row and
//! offset) and tested with stand-ins made of ordinary columns: the row keys of the result
//! ([`keys`]), its columns ([`sources`]), the initial result and the change of the result
//! for every change of the source ([`operator`]). What is left is the one function
//! [`arrays_of`], an [`sources::ArrayColumn`] over the list arrays that
//! `ColumnSource::chunk` returns, and the comparison with the recorded Java results, which
//! need `group_by`.

mod keys;
mod operator;
mod sources;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod tests;

use operator::Ungrouped;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use sources::ArrayColumn;
use std::fmt;
use std::sync::Arc;

/// Why an `ungroup` was refused, or why its result failed. The text is Java's message.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum UngroupError {
    /// A named column the table does not have. The Java server answers `INVALID_ARGUMENT`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable#getColumnSource, which
    /// io.deephaven.engine.table.impl.UngroupOperation#UngroupOperation calls for each
    /// name in turn
    NoSuchColumn {
        name: String,
        /// The names of the table's columns.
        available: Vec<String>,
    },
    /// A named column that is not an array. The Java server answers `INVALID_ARGUMENT`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation#UngroupOperation
    NotAnArray(String),
    /// Without `null_fill`, the arrays of one row differ in size. The Java server answers
    /// `INVALID_ARGUMENT`.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation#computeMaxSize
    SizesDiffer {
        row_key: RowKey,
        /// Whether the row was read as of the previous cycle.
        previous: bool,
        /// The row's position in the source, as Java's `RowSet#find` gives it.
        position: i64,
        /// The column the others are compared with, and the size of its array.
        reference: String,
        reference_size: u64,
        /// The column that differs, and the size of its array.
        column: String,
        size: u64,
    },
    /// The row keys of the result do not fit in 63 bits.
    ///
    /// Behavior: io.deephaven.engine.table.impl.UngroupOperation#getUngroupRowset
    KeyOverflow { last_row_key: RowKey, base: u32 },
    /// The source has failed, or reading it failed.
    Table(TableFailure),
}

impl fmt::Display for UngroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UngroupError::NoSuchColumn { name, available } => write!(
                f,
                "Unknown column names [{name}], available column names are [{}]",
                available.join(", ")
            ),
            UngroupError::NotAnArray(name) => {
                write!(f, "Column {name} is not an array or Vector")
            }
            UngroupError::SizesDiffer {
                row_key,
                previous,
                position,
                reference,
                reference_size,
                column,
                size,
            } => write!(
                f,
                "Array sizes differ at row key {row_key} ({}position {position}), \
                 {reference} has size {reference_size}, {column} has size {size}",
                if *previous { "previous " } else { "" }
            ),
            UngroupError::KeyOverflow { last_row_key, base } => write!(
                f,
                "Key overflow detected, perhaps you should flatten your table before \
                 calling ungroup: lastRowKey={last_row_key}, base={base}"
            ),
            UngroupError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for UngroupError {}

impl From<TableFailure> for UngroupError {
    fn from(failure: TableFailure) -> Self {
        UngroupError::Table(failure)
    }
}

impl From<UngroupError> for TableFailure {
    fn from(error: UngroupError) -> Self {
        match error {
            UngroupError::Table(failure) => failure,
            other => TableFailure {
                message: other.to_string(),
            },
        }
    }
}

/// Whether a column of `column_type` holds arrays: a vector or an array column does.
///
/// With `unported!` for a type this module does not know: a type added to the contracts
/// must be decided here, not passed over.
///
/// Behavior: the test `c.getDataType().isArray() || QueryLanguageParser.isTypedVector(c.getDataType())`
/// of io.deephaven.engine.table.impl.QueryTable#ungroup and the test of
/// io.deephaven.engine.table.impl.UngroupOperation#UngroupOperation
fn is_array(column_type: &ColumnType) -> bool {
    match column_type {
        ColumnType::Boolean
        | ColumnType::Byte
        | ColumnType::Char
        | ColumnType::Short
        | ColumnType::Int
        | ColumnType::Long
        | ColumnType::Float
        | ColumnType::Double
        | ColumnType::String
        | ColumnType::Instant => false,
        ColumnType::Vector(_) | ColumnType::Array(_) => true,
        other => unported!(
            "io.deephaven.engine.table.impl.UngroupOperation#UngroupOperation",
            "a column of type {other:?}"
        ),
    }
}

/// The arrays of column `index` of `source`, which [`is_array`].
///
/// `unported!`: reading the list cells of a vector or array column as an [`ArrayColumn`]
/// is not written yet.
///
/// Behavior: io.deephaven.engine.table.impl.sources.UngroupedColumnSource#getColumnSource
fn arrays_of(source: &dyn Table, index: usize) -> Arc<dyn ArrayColumn> {
    let column = &source.definition().columns()[index];
    unported!(
        "io.deephaven.engine.table.impl.sources.UngroupedColumnSource#getColumnSource",
        "column {} of type {:?}",
        column.name,
        column.column_type
    )
}

/// `source` with each of `columns` replaced by the elements of its arrays; without
/// `columns`, with every array column replaced. The result has a row for every element, in
/// the order of the source's rows and within a row of the array; the other columns repeat
/// the row's value.
///
/// The arrays of one row must have one size. With `null_fill` they need not: the row gets
/// the size of its largest array and the shorter ones are filled with nulls.
///
/// A source without array columns, ungrouped without `columns`, is returned as it is. A
/// named column must exist and be an array; the first that is not is the error.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#ungroup(boolean, Collection) and
/// io.deephaven.engine.table.impl.UngroupOperation
pub fn ungroup(
    source: &Arc<dyn Table>,
    null_fill: bool,
    columns: &[String],
) -> Result<Arc<dyn Table>, UngroupError> {
    let declared = source.definition().columns();
    let indices: Vec<usize> = if columns.is_empty() {
        let arrays = declared.iter().enumerate();
        let arrays = arrays.filter(|(_, column)| is_array(&column.column_type));
        arrays.map(|(index, _)| index).collect()
    } else {
        let mut indices = Vec::with_capacity(columns.len());
        for name in columns {
            let Some(index) = source.definition().index_of(name) else {
                return Err(UngroupError::NoSuchColumn {
                    name: name.clone(),
                    available: declared.iter().map(|column| column.name.clone()).collect(),
                });
            };
            if !is_array(&declared[index].column_type) {
                return Err(UngroupError::NotAnArray(name.clone()));
            }
            indices.push(index);
        }
        indices
    };
    if indices.is_empty() {
        return Ok(source.clone());
    }
    let ungrouped = indices.into_iter().map(|index| Ungrouped {
        index,
        arrays: arrays_of(&**source, index),
    });
    operator::ungroup_arrays(source, null_fill, ungrouped.collect(), keys::MINIMUM_BASE)
}
