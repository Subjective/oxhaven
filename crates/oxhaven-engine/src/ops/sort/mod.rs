//! Sort, head, tail and slice. Task 3.3.
//!
//! | Function | Result |
//! | --- | --- |
//! | [`sort`] | The rows ordered by one or more columns, ties in the parent's order |
//! | [`reverse`] | The rows in the opposite order |
//! | [`head`], [`tail`] | The first or last rows |
//! | [`slice`] | The rows at a range of positions, counted from either end |
//! | [`head_pct`], [`tail_pct`], [`slice_pct`] | The rows of a share of the table |
//! | [`restrict_sort_to`] | The table, sortable by the named columns only |

mod keys;
mod slice;
mod sorted;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod shape_tests;
#[cfg(test)]
pub(crate) mod testing;
#[cfg(test)]
mod tests;

use crate::ops::merge::{pivoted, positional};
use crate::table::{mark, marks, memoized, CopyType};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableAttributes, TableFailure};
use std::fmt;
use std::sync::Arc;

/// Behavior: io.deephaven.engine.table.impl.SortingOrder
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SortDirection {
    /// Least first. Null is the least value of every type and NaN the greatest.
    Ascending,
    /// The negated comparison: greatest first, so NaN first and nulls last. Rows that
    /// compare equal still keep the parent's order.
    Descending,
}

/// One column to sort by.
///
/// Behavior: io.deephaven.api.SortColumn, and for `absolute` the `is_absolute` flag of the
/// gRPC `SortDescriptor`, which the Java server implements as a sort by `abs(Column)`
/// (io.deephaven.server.table.ops.SortTableGrpcImpl#create).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SortColumn {
    pub column: String,
    pub direction: SortDirection,
    /// Sort by the absolute value. For numeric columns only.
    pub absolute: bool,
}

impl SortColumn {
    pub fn ascending(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: SortDirection::Ascending,
            absolute: false,
        }
    }

    pub fn descending(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            direction: SortDirection::Descending,
            absolute: false,
        }
    }

    /// The same column and direction, by absolute value.
    pub fn absolute(mut self) -> Self {
        self.absolute = true;
        self
    }
}

/// Why an operation of this module was refused. The variants name the kind of Java
/// exception the reference throws, so that a gRPC handler can map each to the status Java
/// reports.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SortError {
    /// Java's `NoSuchColumnException`: sort columns the table does not have.
    NoSuchColumn(Vec<String>),
    /// Java's `NotSortableException`: sort columns that the table's restriction of its
    /// sorting does not name. The text is the exception's message.
    NotSortable(String),
    /// An absolute sort of a column whose type has no `abs`. In Java the formula
    /// `abs(Column)` fails to compile.
    AbsoluteNotSupported {
        column: String,
        column_type: ColumnType,
    },
    /// Java's `IllegalArgumentException`, and the failed `Require` of a negative size.
    IllegalArgument(String),
    /// Java's `UnsupportedOperationException`: the operation does not apply to this kind
    /// of table.
    Unsupported(String),
    /// The parent has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for SortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortError::NoSuchColumn(columns) => {
                write!(f, "Unknown column names [{}]", columns.join(", "))
            }
            SortError::AbsoluteNotSupported {
                column,
                column_type,
            } => write!(
                f,
                "cannot sort by the absolute value of {column}, a {column_type:?} column"
            ),
            SortError::IllegalArgument(message)
            | SortError::Unsupported(message)
            | SortError::NotSortable(message) => f.write_str(message),
            SortError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for SortError {}

impl From<TableFailure> for SortError {
    fn from(failure: TableFailure) -> Self {
        SortError::Table(failure)
    }
}

/// The rows of `parent` ordered by `columns`: the first column decides, later columns break
/// ties, and rows equal in every column keep the parent's order, initially and after every
/// update. With no columns the result is `parent` itself, and so it is for one column that
/// `parent` is the result of a sort by, in the same direction.
///
/// The result of sorting a static table is flat, or, when its rows are already in order,
/// a copy of `parent` with its row keys and attributes. The result of sorting a refreshing
/// table is not flat, except for a blink table, whose sort is the sorted rows of each
/// cycle. The sort of a copy is a copy, with the attributes Java copies for a sort.
///
/// # Panics
/// With `unported!` for an absolute sort of a `Char` column.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#sort,
/// io.deephaven.engine.table.impl.SortOperation and io.deephaven.engine.table.impl.SortListener
pub fn sort(parent: &Arc<dyn Table>, columns: &[SortColumn]) -> Result<Arc<dyn Table>, SortError> {
    sorted::sort(parent, columns)
}

/// The rows of `parent`, last row first. The result is not flat and keeps the blink
/// attribute of its parent. The reverse of a static table has the row keys Java gives it,
/// which are never `0..size`; the reverse of a refreshing table has the keys `0..size`.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#reverse,
/// io.deephaven.engine.table.impl.ReverseOperation and the `Reverse` entries of the
/// attribute table in io.deephaven.engine.table.impl.BaseTable
pub fn reverse(parent: &Arc<dyn Table>) -> Result<Arc<dyn Table>, SortError> {
    let mut attributes = TableAttributes::default();
    attributes.blink = parent.attributes().blink;
    let result = if parent.is_refreshing() {
        positional(parent, true, attributes, false)?
    } else {
        pivoted(parent, attributes)
    };
    mark(&result, CopyType::Reverse.sort_marks(&marks(&**parent)));
    Ok(memoized(parent, CopyType::Reverse, false, result)?)
}

/// The first `size` rows of `parent`, or all of them if it has fewer.
///
/// On a blink table: an append-only table of the first `size` rows the blink table ever
/// shows. A `size` of zero gives a static table without rows.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#head
pub fn head(parent: &Arc<dyn Table>, size: i64) -> Result<Arc<dyn Table>, SortError> {
    slice::head(parent, size)
}

/// The last `size` rows of `parent`, or all of them if it has fewer.
///
/// On a blink table: the latest `size` rows the blink table added. A `size` of zero gives
/// a static table without rows.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#tail
pub fn tail(parent: &Arc<dyn Table>, size: i64) -> Result<Arc<dyn Table>, SortError> {
    slice::tail(parent, size)
}

/// The rows of `parent` at positions `first..last`. A negative position counts back from
/// the end, and `last == 0` with a negative `first` means the end: `slice(t, -3, 0)` is
/// `tail(t, 3)`. Not supported on a blink table.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#slice
pub fn slice(parent: &Arc<dyn Table>, first: i64, last: i64) -> Result<Arc<dyn Table>, SortError> {
    slice::slice(parent, first, last)
}

/// The first rows of `parent`: `percent` of its size, a share from 0 to 1, rounded up. The
/// result follows the size of a refreshing table: rows join it as the table grows and
/// leave it as the table shrinks. Not supported on a blink table.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#headPct
pub fn head_pct(parent: &Arc<dyn Table>, percent: f64) -> Result<Arc<dyn Table>, SortError> {
    slice::head_pct(parent, percent)
}

/// The last rows of `parent`: `percent` of its size, a share from 0 to 1, rounded up.
/// Otherwise as [`head_pct`].
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#tailPct
pub fn tail_pct(parent: &Arc<dyn Table>, percent: f64) -> Result<Arc<dyn Table>, SortError> {
    slice::tail_pct(parent, percent)
}

/// The rows of `parent` from the position at the share `start` of its size to the position
/// at the share `end`, each from 0 to 1 and rounded down. An `end` before `start` selects
/// nothing. Otherwise as [`head_pct`].
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#slicePct
pub fn slice_pct(
    parent: &Arc<dyn Table>,
    start: f64,
    end: f64,
) -> Result<Arc<dyn Table>, SortError> {
    slice::slice_pct(parent, start, end)
}

/// A copy of `parent` that may be sorted by `columns` only: [`sort`] refuses any other
/// column of it, and of the tables made from it that keep the restriction. The same
/// restriction again is `parent` itself.
///
/// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#restrictSortTo
pub fn restrict_sort_to<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
) -> Result<Arc<dyn Table>, SortError> {
    sorted::restrict_sort_to(parent, columns)
}
