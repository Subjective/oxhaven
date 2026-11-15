//! Merge and flatten. Task 3.5.
//!
//! | Function | Result |
//! | --- | --- |
//! | [`merge`] | The rows of several tables, one table after the other |
//! | [`merge_sorted`] | The rows of several static tables, merged by a key column |
//! | [`flatten`] | The rows of one table at the row keys `0..size` |

mod flatten;
mod sorted;
mod union;

#[cfg(test)]
mod tests;

pub use flatten::flatten;
pub(crate) use flatten::{flattened_shift_data, pivoted, positional};
pub use sorted::merge_sorted;
pub use union::merge;
pub(crate) use union::{append_shift_data, key_space_for, next_slot};

use oxhaven_contracts::table::TableFailure;
use std::fmt;

/// Why a merge was refused. The variants name the kind of Java exception the reference
/// throws, so that a gRPC handler can map each to the status Java reports.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MergeError {
    /// Java's `IllegalArgumentException`: no tables, or tables of different update graphs.
    IllegalArgument(String),
    /// Java's `IncompatibleTableDefinitionException`: a table whose columns differ from
    /// the first table's.
    IncompatibleDefinitions(String),
    /// Java's `UnsupportedOperationException`: a sorted merge of a refreshing table.
    Unsupported(String),
    /// Java's `NoSuchColumnException`: a table without the key column of a sorted merge.
    NoSuchColumn(String),
    /// Java's `RuntimeException` and `RequirementFailure` for the tables of a sorted merge
    /// whose column names or column types differ.
    Runtime(String),
    /// Java's `NullPointerException`: a sorted merge compared a null key.
    NullKey(String),
    /// A table to merge has failed, or the key space is exhausted.
    Table(TableFailure),
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MergeError::IllegalArgument(message)
            | MergeError::IncompatibleDefinitions(message)
            | MergeError::Unsupported(message)
            | MergeError::NoSuchColumn(message)
            | MergeError::Runtime(message)
            | MergeError::NullKey(message) => f.write_str(message),
            MergeError::Table(failure) => failure.fmt(f),
        }
    }
}

impl std::error::Error for MergeError {}

impl From<TableFailure> for MergeError {
    fn from(failure: TableFailure) -> Self {
        MergeError::Table(failure)
    }
}
