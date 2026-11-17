//! As-of joins. Task 4.4.
//!
//! | Function | Result |
//! | --- | --- |
//! | [`as_of_join`] | The left table's rows, each with columns of the right row whose stamp is closest to its own on the side the rule names |
//! | [`aj`] | [`as_of_join`] for the rules `>=` and `>`: the closest right stamp that is not after the left stamp |
//! | [`raj`] | [`as_of_join`] for the rules `<=` and `<`: the closest right stamp that is not before the left stamp |
//!
//! The legacy `AsOfJoinTablesRequest` of the gRPC API is the same operation: its handler
//! (`io.deephaven.server.table.ops.JoinTablesGrpcImpl.AsOfJoinTablesGrpcImpl#doJoin`)
//! turns the request's match rule into an [`AsOfRule`] and calls `Table#asOfJoin`, which is
//! [`as_of_join`] here.

mod join;
mod stamps;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod random;
#[cfg(test)]
mod tests;

pub use super::join::{JoinAddition, JoinMatch};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use std::fmt;
use std::sync::Arc;

/// How the stamp of a left row relates to the stamp of the right row it is joined with.
///
/// Behavior: io.deephaven.api.AsOfJoinRule
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AsOfRule {
    /// `left <= right`: the least right stamp that is not less than the left stamp.
    LessThanEqual,
    /// `left < right`: the least right stamp that is greater than the left stamp.
    LessThan,
    /// `left >= right`: the greatest right stamp that is not greater than the left stamp.
    GreaterThanEqual,
    /// `left > right`: the greatest right stamp that is less than the left stamp.
    GreaterThan,
}

impl AsOfRule {
    /// Whether this is a rule of `aj`, which looks backwards from the left stamp. The
    /// others are the rules of `raj`.
    ///
    /// Behavior: io.deephaven.api.AsOfJoinMatch#isAj
    pub fn is_aj(self) -> bool {
        matches!(self, AsOfRule::GreaterThanEqual | AsOfRule::GreaterThan)
    }

    /// Behavior: io.deephaven.api.AsOfJoinRule#operatorString
    pub fn operator(self) -> &'static str {
        match self {
            AsOfRule::LessThanEqual => "<=",
            AsOfRule::LessThan => "<",
            AsOfRule::GreaterThanEqual => ">=",
            AsOfRule::GreaterThan => ">",
        }
    }
}

/// The stamp columns of an as-of join and the rule between them.
///
/// Behavior: io.deephaven.api.AsOfJoinMatch
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AsOfMatch {
    /// The stamp column of the left table.
    pub left: String,
    pub rule: AsOfRule,
    /// The stamp column of the right table.
    pub right: String,
}

impl AsOfMatch {
    pub fn new(left: impl Into<String>, rule: AsOfRule, right: impl Into<String>) -> Self {
        Self {
            left: left.into(),
            rule,
            right: right.into(),
        }
    }
}

/// Why an as-of join was refused. Each variant names the Java exception the reference
/// throws, so that a gRPC handler can map it to the status Java reports. The recorded Java
/// results (`fixtures/asof_refusals.json`) have `INVALID_ARGUMENT` for every one of them.
///
/// When [`AsOfJoinError::KeyTypes`] arises in an update cycle, the result table fails with
/// the same text as its [`TableFailure`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum AsOfJoinError {
    /// Java's `IllegalArgumentException` "Unsupported operation for aj" or "for raj": a
    /// rule of the other operation.
    UnsupportedRule {
        operation: &'static str,
        rule: AsOfRule,
    },
    /// Java's `NoSuchColumnException`: a match or added column a table does not have.
    NoSuchColumn(Vec<String>),
    /// Java's `RuntimeException` "Conflicting column names": added columns named like
    /// columns of the left table.
    ConflictingColumns(Vec<String>),
    /// Java's `IllegalArgumentException` "Can not aj() with different stamp types".
    StampTypes { left: ColumnType, right: ColumnType },
    /// Java's `ClassCastException` from comparing two stamps of a class that is not a
    /// `Comparable` (`Period`, `ZoneId`). Java throws it only once it compares two such
    /// stamps that are not one object; this server refuses the join before it starts.
    IncomparableStamps(String),
    /// Java's `ClassCastException`: a pair of exact-match columns that Java reads as
    /// different primitive types, in a join that reads a row of the right table.
    KeyTypes {
        left: String,
        left_type: ColumnType,
        right: String,
        right_type: ColumnType,
    },
    /// Java's `UpdateGraphConflictException`: two refreshing tables of different update
    /// graphs.
    UpdateGraphConflict,
    /// A table has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for AsOfJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AsOfJoinError::UnsupportedRule { operation, rule } => {
                write!(f, "Unsupported operation for {operation}: {rule:?}")
            }
            AsOfJoinError::NoSuchColumn(columns) => {
                write!(f, "Unknown column names [{}]", columns.join(", "))
            }
            AsOfJoinError::ConflictingColumns(columns) => {
                write!(f, "Conflicting column names [{}]", columns.join(", "))
            }
            AsOfJoinError::StampTypes { left, right } => write!(
                f,
                "Can not aj() with different stamp types: left={left:?}, right={right:?}"
            ),
            AsOfJoinError::IncomparableStamps(message) => f.write_str(message),
            AsOfJoinError::KeyTypes {
                left,
                left_type,
                right,
                right_type,
            } => write!(
                f,
                "Mismatched as-of join key types, {left}={right}: {left_type:?} != {right_type:?}"
            ),
            AsOfJoinError::UpdateGraphConflict => {
                f.write_str("Multiple update graphs found in dependencies")
            }
            AsOfJoinError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for AsOfJoinError {}

impl From<TableFailure> for AsOfJoinError {
    fn from(failure: TableFailure) -> Self {
        AsOfJoinError::Table(failure)
    }
}

/// The rows of `left`, in its order and at its row keys, each with the columns `add` of
/// one row of `right`, or with nulls when there is none.
///
/// The right row of a left row is found among the right rows whose `exact` columns equal
/// the left row's; with no `exact`, among all of them. Of these it is the row whose stamp
/// (`as_of.right`) is closest to the left row's stamp (`as_of.left`) on the side
/// `as_of.rule` names; of several right rows with that stamp it is the last in the right
/// table's order for the rules of `aj`, and the first for the rules of `raj`.
///
/// Stamps are ordered as Java orders the values of a column: null is the least value and
/// NaN the greatest, and `-0.0` equals `0.0`. A null stamp is a value like any other: a
/// left row with a null stamp is joined with a right row with a null stamp under `>=` and
/// `<=`. The right table need not be sorted. Exact-match columns are equal as the keys of
/// a natural join are: null equals null and NaN equals NaN.
///
/// The added columns are, first, the right column of every match that is not the name of a
/// left column, and then `add`; with no `add`, every right column whose name is not the
/// left name of a match. An entry of `add` that renames the right column of a match takes
/// that column's place.
///
/// The two stamp columns must have one type, where `Boolean` counts as `byte` and
/// `Instant` as `long`. The `java.time` types order as their `compareTo` does
/// ([`stamps::Stamp`]); a `Period` or `ZoneId` stamp is refused
/// ([`AsOfJoinError::IncomparableStamps`]). Exact-match columns of different types are refused only when the
/// right table has a row; if it gets its first row later, the result fails then. Nothing
/// but the names of the added columns is checked when `left` is static and has no rows:
/// the result is then a static table without rows.
///
/// The result is refreshing when either table is, except in that last case. It is not
/// flat and has no attributes.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#asOfJoin, #ajInternal and
/// io.deephaven.engine.table.impl.AsOfJoinHelper#asOfJoin
pub fn as_of_join(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    as_of: &AsOfMatch,
    add: &[JoinAddition],
) -> Result<Arc<dyn Table>, AsOfJoinError> {
    join::join(left, right, exact, as_of, add)
}

/// [`as_of_join`] with a rule of `aj`, `>=` or `>`; any other rule is refused.
///
/// Behavior: io.deephaven.api.AsOfJoinMatch#parseForAj, which the handler of `AjTables`
/// calls (io.deephaven.server.table.ops.AjRajGrpcImpl.AjGrpcImpl)
pub fn aj(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    as_of: &AsOfMatch,
    add: &[JoinAddition],
) -> Result<Arc<dyn Table>, AsOfJoinError> {
    if !as_of.rule.is_aj() {
        return Err(AsOfJoinError::UnsupportedRule {
            operation: "aj",
            rule: as_of.rule,
        });
    }
    join::join(left, right, exact, as_of, add)
}

/// [`as_of_join`] with a rule of `raj`, `<=` or `<`; any other rule is refused.
///
/// Behavior: io.deephaven.api.AsOfJoinMatch#parseForRaj, which the handler of `RajTables`
/// calls (io.deephaven.server.table.ops.AjRajGrpcImpl.RajGrpcImpl)
pub fn raj(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    as_of: &AsOfMatch,
    add: &[JoinAddition],
) -> Result<Arc<dyn Table>, AsOfJoinError> {
    if as_of.rule.is_aj() {
        return Err(AsOfJoinError::UnsupportedRule {
            operation: "raj",
            rule: as_of.rule,
        });
    }
    join::join(left, right, exact, as_of, add)
}
