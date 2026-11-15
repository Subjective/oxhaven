//! Multi join: one row per key that any of several tables has, with the columns of the row
//! of each table that has the key. Task 4.12.
//!
//! | Item | What it is |
//! | --- | --- |
//! | [`multi_join`] | The join |
//! | [`MultiJoinInput`] | One table of a join: its key columns and the columns it adds |
//! | [`MultiJoinError`] | Why a join was refused |
//!
//! The work is in three files: `plan.rs` checks the arguments, `keyed.rs` is the join on
//! key columns and `zero.rs` the join without any, which has at most one row.

mod keyed;
mod plan;
mod zero;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod random;
#[cfg(test)]
mod tests;

pub use crate::ops::join::{JoinAddition, JoinMatch};

use crate::table::QueryTable;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use std::fmt;
use std::sync::Arc;

/// One table of a multi join.
///
/// Behavior: io.deephaven.engine.table.MultiJoinInput
#[derive(Clone)]
pub struct MultiJoinInput {
    pub table: Arc<dyn Table>,
    /// The key columns: `left` is the name in the result, `right` the column of `table`.
    /// Every table of a join must give the same result names, in any order.
    pub on: Vec<JoinMatch>,
    /// The columns of `table` to add to the result. Empty for every column that is not the
    /// `right` of a key column.
    pub add: Vec<JoinAddition>,
}

impl MultiJoinInput {
    /// `table` joined on columns it has under the result's names, adding all its others.
    pub fn new(table: Arc<dyn Table>, on: &[&str]) -> Self {
        Self {
            table,
            on: on.iter().map(|column| JoinMatch::same(*column)).collect(),
            add: Vec::new(),
        }
    }
}

/// Why a multi join was refused. Each variant names the Java exception the reference
/// throws; its gRPC handler answers every one of them with `INVALID_ARGUMENT`.
///
/// When a duplicate arises in an update cycle, the result table fails with the same text
/// as its [`TableFailure`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MultiJoinError {
    /// Java's `IllegalArgumentException` "At least one table must be included in
    /// MultiJoinTable."
    NoInputs,
    /// Java's `NoSuchColumnException`: a key column or an added column that table `table`
    /// does not have.
    NoSuchColumn { table: usize, column: String },
    /// Java's `IllegalArgumentException` "Key column mismatch": table `table` does not
    /// name the key columns the first table names.
    KeyColumnMismatch {
        table: usize,
        first: Vec<String>,
        this: Vec<String>,
    },
    /// Java's `IllegalArgumentException` "Key column type mismatch": the key columns of
    /// table `table` do not have the types of the first table's, taken as sets. An
    /// `Instant` counts as a `long` and a `Boolean` as a `byte`.
    KeyTypeMismatch {
        table: usize,
        first: Vec<ColumnType>,
        this: Vec<ColumnType>,
    },
    /// Java's `ClassCastException`: the key columns of table `table` have the first
    /// table's types, but not column by column.
    KeyTypeOrder {
        table: usize,
        column: String,
        first: ColumnType,
        this: ColumnType,
    },
    /// Java's `IllegalArgumentException` "Column X defined in table A and table B": a
    /// result column that is added twice, or added under the name of a key column
    /// (`earlier` is then `None`).
    ColumnConflict {
        column: String,
        earlier: Option<usize>,
        table: usize,
    },
    /// Java's `IllegalStateException` "Duplicate key found for K in table N.": two rows of
    /// one table have one key.
    DuplicateKey { key: String, table: usize },
    /// Java's `IllegalStateException` "Duplicate rows for table N on zero-key multiJoin.",
    /// or in an update cycle "Multiple rows in N for zero-key multiJoin.": a table of a
    /// join without key columns has more than one row.
    ZeroKeyRows { table: usize, in_cycle: bool },
    /// Java's `UpdateGraphConflictException`: refreshing tables of different update graphs.
    UpdateGraphConflict,
    /// A table has failed or could not be read.
    Table(TableFailure),
}

impl fmt::Display for MultiJoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MultiJoinError::NoInputs => {
                f.write_str("At least one table must be included in MultiJoinTable.")
            }
            MultiJoinError::NoSuchColumn { table, column } => {
                write!(f, "Unknown column names [{column}] in table {table}")
            }
            MultiJoinError::KeyColumnMismatch { table, first, this } => write!(
                f,
                "Key column mismatch for table {table}, first table has key columns=[{}], \
                 this table has [{}]",
                first.join(", "),
                this.join(", ")
            ),
            MultiJoinError::KeyTypeMismatch { table, first, this } => write!(
                f,
                "Key column type mismatch for table {table}, first table has key column \
                 types={first:?}, this table has {this:?}"
            ),
            MultiJoinError::KeyTypeOrder {
                table,
                column,
                first,
                this,
            } => write!(
                f,
                "Key column {column} of table {table} is {this:?}, the first table's is \
                 {first:?}"
            ),
            MultiJoinError::ColumnConflict {
                column,
                earlier,
                table,
            } => match earlier {
                Some(earlier) => {
                    write!(
                        f,
                        "Column {column} defined in table {earlier} and table {table}"
                    )
                }
                None => write!(
                    f,
                    "Column {column} defined in table key columns and table {table}"
                ),
            },
            MultiJoinError::DuplicateKey { key, table } => {
                write!(f, "Duplicate key found for {key} in table {table}.")
            }
            MultiJoinError::ZeroKeyRows {
                table,
                in_cycle: false,
            } => write!(f, "Duplicate rows for table {table} on zero-key multiJoin."),
            MultiJoinError::ZeroKeyRows {
                table,
                in_cycle: true,
            } => write!(f, "Multiple rows in {table} for zero-key multiJoin."),
            MultiJoinError::UpdateGraphConflict => {
                f.write_str("Multiple update graphs found in dependencies")
            }
            MultiJoinError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for MultiJoinError {}

impl From<TableFailure> for MultiJoinError {
    fn from(failure: TableFailure) -> Self {
        MultiJoinError::Table(failure)
    }
}

fn failure(error: impl fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// One row for every key that a row of any of `inputs` has, with the key columns first,
/// under the names and with the types of the first input, and then the added columns of
/// each input in turn. A row has nulls in the columns of an input that has no row of its
/// key. Keys are equal as in the other joins: null equals null, every NaN equals every
/// NaN, and `-0.0` equals `0.0`; a key column shows the value of the row that brought the
/// key.
///
/// No input may have two rows of one key. That refuses the join, and fails the result when
/// it comes about in an update cycle.
///
/// Rows are in the order in which their keys first appeared: through the inputs in turn,
/// each in its row order. In a cycle the inputs are again taken in turn, and of one input
/// the modified rows whose key columns changed come before its added rows. A row leaves
/// the result when no input has its key any more, and comes back at its old place, not at
/// the end, when one has it again.
///
/// Without key columns the result has one row while any input has a row, and no input may
/// have more than one.
///
/// The result is refreshing when any input is. It has no attributes and is not flat.
///
/// Behavior: io.deephaven.engine.table.MultiJoinFactory#of,
/// io.deephaven.engine.table.impl.MultiJoinTableCreatorImpl#of and the constructor of
/// io.deephaven.engine.table.impl.MultiJoinTableImpl
pub fn multi_join(inputs: &[MultiJoinInput]) -> Result<Arc<dyn Table>, MultiJoinError> {
    if inputs.is_empty() {
        return Err(MultiJoinError::NoInputs);
    }
    // Java asks for the update graph of the tables before it reads its arguments.
    let mut graphs = inputs
        .iter()
        .filter(|input| input.table.is_refreshing())
        .map(|input| input.table.update_graph());
    if let Some(first) = graphs.next() {
        let same = |other: Option<_>| match (&first, &other) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        if !graphs.all(same) {
            return Err(MultiJoinError::UpdateGraphConflict);
        }
    }
    let plan = plan::Plan::new(inputs)?;

    let parents: Vec<Arc<dyn Table>> = inputs.iter().map(|input| input.table.clone()).collect();
    let mut refused = None;
    let mut refuse = |error: MultiJoinError| {
        let failure = failure(&error);
        refused = Some(error);
        failure
    };
    let result = if plan.keys.is_empty() {
        QueryTable::derive_merged(&parents, Vec::new(), || {
            zero::start(plan, &parents).map_err(&mut refuse)
        })
    } else {
        QueryTable::derive_merged(&parents, Vec::new(), || {
            keyed::start(plan, &parents).map_err(&mut refuse)
        })
    };
    match result {
        Ok(table) => Ok(table),
        Err(failure) => Err(refused.unwrap_or(MultiJoinError::Table(failure))),
    }
}
