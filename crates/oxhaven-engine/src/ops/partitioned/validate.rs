//! The checks a `PartitionedTableProxy` makes of the partitioned tables of an operation
//! that takes two: that both have the same keys, and that no join key is in more than one
//! constituent.
//!
//! A check is made once, when the operation is made, and fails it. For a refreshing
//! partitioned table it is made again in every update cycle that changes what it reads:
//! the partitioned table the operation then uses has a copy of the underlying table that
//! follows it and fails when the check does, and with it everything made of it.

use super::transform::match_key_columns;
use super::{failure, PartitionedError, PartitionedTable, Transformer};
use crate::ops::distinct::{select_distinct, where_not_in};
use crate::ops::join::JoinMatch;
use crate::ops::sort::head;
use crate::table::{QueryTable, TableSpec};
use arrow::array::Array;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

const NON_MATCHING_KEYS: &str = "Partitioned table arguments have non-matching keys; re-assess \
     your input data or create a proxy with requireMatchingKeys=false";

const OVERLAPPING_JOIN_KEYS: &str = "Partitioned table has join keys found in multiple \
     constituents; re-assess your input data or create a proxy with \
     sanityCheckJoinOperations=false";

const NULL_JOIN_KEY: &str = "java.lang.NullPointerException: a null join key in the check of \
     the join keys of a partitioned table (PartitionedTableProxyImpl#claimKey)";

/// A check, and the tables it reads: it is made again when one of them changes.
struct Validation {
    reads: Vec<Arc<dyn Table>>,
    check: Box<dyn Fn() -> Result<(), String> + Send + Sync>,
}

/// `parent`, or for a refreshing `parent` a table with its rows, columns and changes that
/// fails in the update cycle in which the check of `validation` does. The check has
/// passed once.
///
/// Behavior: io.deephaven.engine.table.impl.partitioned.PartitionedTableProxyImpl#validated
fn validated(
    parent: &Arc<dyn Table>,
    validation: Validation,
) -> Result<Arc<dyn Table>, TableFailure> {
    if !parent.is_refreshing() {
        return Ok(parent.clone());
    }
    let mut parents = vec![parent.clone()];
    parents.extend(
        validation
            .reads
            .iter()
            .filter(|table| table.is_refreshing())
            .cloned(),
    );
    let result = QueryTable::derive_merged(&parents, Vec::new(), || {
        let definition = parent.definition().clone();
        let columns = (0..definition.columns().len())
            .map(|index| parent.column(index))
            .collect();
        let mut spec = TableSpec::new(definition, (*parent.row_set()).clone(), columns);
        if parent.is_flat() {
            spec = spec.flat();
        }
        let operator = move |_: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
            (validation.check)().map_err(failure)?;
            Ok(match &upstream[0] {
                Some(update) => (**update).clone(),
                None => TableUpdate::empty(),
            })
        };
        Ok((spec, operator))
    })?;
    Ok(result)
}

fn illegal(error: impl ToString) -> PartitionedError {
    PartitionedError::IllegalArgument(error.to_string())
}

/// `lhs` and `rhs` as an operation of both uses them when it requires that they have
/// the same keys: it fails now if one has a key the other has not, and for a refreshing
/// one the partitioned table answered fails in the update cycle in which that becomes so.
/// The key columns are matched as `partitioned_transform` matches them.
///
/// Behavior: PartitionedTableProxyImpl#matchingKeysValidation and #checkNonMatchingKeys,
/// as #complexTransform uses them with `requireMatchingKeys`
pub fn require_matching_keys(
    lhs: &PartitionedTable,
    rhs: &PartitionedTable,
) -> Result<(PartitionedTable, PartitionedTable), PartitionedError> {
    let on = match_key_columns(lhs, rhs)?;
    let reversed: Vec<JoinMatch> = on
        .iter()
        .map(|pair| JoinMatch::new(pair.right.as_str(), pair.left.as_str()))
        .collect();
    // The rows of each underlying table whose key the other has not.
    let only_lhs = where_not_in(lhs.table(), rhs.table(), &on).map_err(illegal)?;
    let only_rhs = where_not_in(rhs.table(), lhs.table(), &reversed).map_err(illegal)?;
    let validation = || {
        let (only_lhs, only_rhs) = (only_lhs.clone(), only_rhs.clone());
        Validation {
            reads: vec![only_lhs.clone(), only_rhs.clone()],
            check: Box::new(move || {
                match only_lhs.row_set().is_empty() && only_rhs.row_set().is_empty() {
                    true => Ok(()),
                    false => Err(NON_MATCHING_KEYS.to_string()),
                }
            }),
        }
    };
    (validation().check)().map_err(PartitionedError::IllegalArgument)?;
    Ok((
        lhs.with_table(validated(lhs.table(), validation())?)?,
        rhs.with_table(validated(rhs.table(), validation())?)?,
    ))
}

impl PartitionedTable {
    /// This partitioned table as a join uses it when it checks its keys: it fails now if
    /// two constituents have a row with the same values of `columns`, the columns the
    /// join matches exactly, and for a refreshing one the partitioned table answered fails
    /// in the update cycle after which that is so. With no column, every row has the one
    /// join key there is, and only one constituent may have rows. With one column, a
    /// null in it fails the check, as it does in Java.
    ///
    /// Behavior: PartitionedTableProxyImpl#nonOverlappingJoinKeysValidation. Java keeps
    /// the owner of each key and settles the claims of a cycle when every constituent
    /// has reported; here the distinct keys of each constituent are merged and counted,
    /// which is the same state at the end of the cycle.
    pub fn require_distinct_join_keys(
        &self,
        columns: &[String],
    ) -> Result<PartitionedTable, PartitionedError> {
        let names = columns.to_vec();
        let keys_of: Transformer = Arc::new(move |tables: &[Arc<dyn Table>]| {
            let keys = match names.is_empty() {
                true => head(&tables[0], 1).map_err(|error| failure(error.to_string()))?,
                false => select_distinct(&tables[0], &names)
                    .map_err(|error| failure(error.to_string()))?,
            };
            Ok(Some(keys))
        });
        // One row for each join key of each constituent, and for each join key there is.
        let merged = self.transform(keys_of, &[])?.merge()?;
        let distinct = match columns.is_empty() {
            true => head(&merged, 1).map_err(illegal)?,
            false => select_distinct(&merged, columns).map_err(illegal)?,
        };
        // Behavior: #claimKey, whose map of owners cannot hold null: the key of a join on
        // one column is the value itself, so a null value there fails the check with a
        // NullPointerException. A key of several columns is a tuple and may hold nulls.
        let single = columns.len() == 1;
        let validation = Validation {
            reads: vec![merged.clone(), distinct.clone()],
            check: Box::new(move || {
                let keys = distinct.row_set();
                if single {
                    let values = distinct
                        .column(0)
                        .chunk(&keys)
                        .map_err(|error| error.to_string())?;
                    if values.null_count() > 0 {
                        return Err(NULL_JOIN_KEY.to_string());
                    }
                }
                match merged.row_set().size() == keys.size() {
                    true => Ok(()),
                    false => Err(OVERLAPPING_JOIN_KEYS.to_string()),
                }
            }),
        };
        (validation.check)().map_err(PartitionedError::IllegalArgument)?;
        self.with_table(validated(&self.table, validation)?)
    }
}
