//! Natural join and exact join. Task 4.2.
//!
//! | Function | Result |
//! | --- | --- |
//! | [`natural_join`] | The left table's rows, each with columns of the right row that has the same key |
//! | [`exact_join`] | The same, and every left row must have exactly one right row |
//!
//! The pinned reference has no left join: `LeftJoinTables` is in its `table.proto`, and
//! `io.deephaven.server.table.ops.JoinTablesGrpcImpl.LeftJoinTablesGrpcImpl#doJoin` answers
//! it with `UNIMPLEMENTED`. There is nothing in the engine to port for it.

pub(crate) mod keys;
pub(crate) mod natural;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod random;
#[cfg(test)]
mod tests;

use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use std::fmt;
use std::sync::Arc;

/// Two columns whose values must be equal for a left row and a right row to match.
///
/// Behavior: io.deephaven.api.JoinMatch
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct JoinMatch {
    /// The column of the left table.
    pub left: String,
    /// The column of the right table.
    pub right: String,
}

impl JoinMatch {
    /// A column that has the same name in both tables.
    pub fn same(column: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            left: column.clone(),
            right: column,
        }
    }

    pub fn new(left: impl Into<String>, right: impl Into<String>) -> Self {
        Self {
            left: left.into(),
            right: right.into(),
        }
    }
}

/// A column of the right table to add to the result.
///
/// Behavior: io.deephaven.api.JoinAddition
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct JoinAddition {
    /// The name of the column in the result.
    pub name: String,
    /// The column of the right table.
    pub source: String,
}

impl JoinAddition {
    /// A right column added under its own name.
    pub fn same(column: impl Into<String>) -> Self {
        let column = column.into();
        Self {
            name: column.clone(),
            source: column,
        }
    }

    pub fn renamed(name: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            source: source.into(),
        }
    }
}

/// What a natural join does with a left row whose key several right rows have.
///
/// Behavior: io.deephaven.api.NaturalJoinType, and the `JoinType` of the gRPC
/// `NaturalJoinTablesRequest`, where `JOIN_TYPE_NOT_SPECIFIED` means `ErrorOnDuplicate`
/// (io.deephaven.server.table.ops.JoinTablesGrpcImpl.NaturalJoinTablesGrpcImpl#adapt).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NaturalJoinType {
    /// The join fails.
    ErrorOnDuplicate,
    /// The first of them in the right table's order is the match.
    FirstMatch,
    /// The last of them in the right table's order is the match.
    LastMatch,
    /// The join fails, and it also fails for a left row that no right row matches.
    ExactlyOneMatch,
}

/// Why a join was refused. Each variant names the Java exception the reference throws, so
/// that a gRPC handler can map it to the status Java reports. The recorded Java results
/// have `INVALID_ARGUMENT` for every one of them.
///
/// When the same condition arises in an update cycle, the result table fails with the same
/// text as its [`TableFailure`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum JoinError {
    /// Java's `NoSuchColumnException`: match or added columns a table does not have.
    NoSuchColumn(Vec<String>),
    /// Java's `RuntimeException` "Conflicting column names": added columns named like
    /// columns of the left table.
    ConflictingColumns(Vec<String>),
    /// Java's `IllegalArgumentException` "Mismatched join types": a pair of match columns
    /// of different types.
    MismatchedTypes {
        left: String,
        left_type: ColumnType,
        right: String,
        right_type: ColumnType,
    },
    /// Several right rows have the key of a left row, in a join that does not allow it.
    /// Java's `IllegalStateException` "Natural Join found duplicate right key for", or with
    /// no match columns its `RuntimeException` "naturalJoin with zero key columns may not
    /// have more than one row in the right hand side table!".
    DuplicateRightKey(String),
    /// No right row has the key of a left row, in an exact join. Java's `RuntimeException`
    /// "Tables don't have one-to-one mapping - no mappings for key", or with no match
    /// columns "exactJoin with zero key columns must have exactly one row in the right hand
    /// side table!".
    NoRightRow(String),
    /// Java's `ClassCastException`: a first-match or last-match join with no match columns
    /// against a static right table of more than one row.
    ZeroKeyStaticRight(String),
    /// Java's `UpdateGraphConflictException`: two refreshing tables of different update
    /// graphs.
    UpdateGraphConflict,
    /// A table has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::NoSuchColumn(columns) => {
                write!(f, "Unknown column names [{}]", columns.join(", "))
            }
            JoinError::ConflictingColumns(columns) => {
                write!(f, "Conflicting column names [{}]", columns.join(", "))
            }
            JoinError::MismatchedTypes {
                left,
                left_type,
                right,
                right_type,
            } => write!(
                f,
                "Mismatched join types, {left}={right}: {left_type:?} != {right_type:?}"
            ),
            JoinError::DuplicateRightKey(message)
            | JoinError::NoRightRow(message)
            | JoinError::ZeroKeyStaticRight(message) => f.write_str(message),
            JoinError::UpdateGraphConflict => {
                f.write_str("Multiple update graphs found in dependencies")
            }
            JoinError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for JoinError {}

impl From<TableFailure> for JoinError {
    fn from(failure: TableFailure) -> Self {
        JoinError::Table(failure)
    }
}

/// The rows of `left`, in its order and at its row keys, each with the columns `add` of the
/// right row whose `on` columns equal its own, or with nulls when there is none.
///
/// With no `add`, every right column is added whose name is not the left name of a match.
/// With no `on`, every left row matches every right row. Keys are equal as Java's join
/// hashers compare them: null equals null, every NaN equals every NaN, and `-0.0` equals
/// `0.0`.
///
/// What happens when several right rows match a left row is `join_type`'s to say. Right
/// rows that no left row matches never fail a join, however many share a key. The same
/// rules hold after every update of either table; a join that would then be refused fails
/// its result instead.
///
/// The result is refreshing when either table is. It is not flat. It keeps the left
/// table's blink attribute, and is add-only or append-only when the left table is and the
/// right table cannot modify it.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#naturalJoin and
/// io.deephaven.engine.table.impl.NaturalJoinHelper#naturalJoin
pub fn natural_join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    on: &[JoinMatch],
    add: &[JoinAddition],
    join_type: NaturalJoinType,
) -> Result<Arc<dyn Table>, JoinError> {
    natural::join(left, right, on, add, join_type)
}

/// [`natural_join`] of type [`NaturalJoinType::ExactlyOneMatch`].
///
/// One case is not checked, as in Java: when `right` is static and there are match
/// columns, a row that `left` adds or changes later is not required to have a right row.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#exactJoin
pub fn exact_join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    on: &[JoinMatch],
    add: &[JoinAddition],
) -> Result<Arc<dyn Table>, JoinError> {
    natural::join(left, right, on, add, NaturalJoinType::ExactlyOneMatch)
}
