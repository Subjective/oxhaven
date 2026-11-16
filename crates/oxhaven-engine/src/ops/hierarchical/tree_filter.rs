//! The filter of a tree: the rows that pass, and their ancestors.

use super::keys::parts;
use super::tree::RowLookup;
use crate::ops::filter::{CheckedFilters, FilterError};
use crate::table::{QueryTable, TableSpec};
use arrow::array::Array;
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::Arc;

fn failure(error: impl std::fmt::Display) -> TableFailure {
    TableFailure {
        message: error.to_string(),
    }
}

/// The rows of `matched` and the rows of `source` that are their ancestors: the row of
/// the identifier that a row's parent column names, and so on up.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.TreeTableFilter#computeParents
fn with_ancestors(
    source: &dyn Table,
    matched: &RowSet,
    lookup: &RowLookup,
    parent_column: usize,
) -> Result<RowSet, TableFailure> {
    let source_rows = source.row_set();
    let mut included = matched.clone();
    let mut level = matched.clone();
    while !level.is_empty() {
        let parents = source.column(parent_column).chunk(&level)?;
        let mut rows: Vec<_> = parts(&parents)?
            .iter()
            .enumerate()
            // a row without a parent is a child of the root
            .filter(|(at, _)| !parents.is_null(*at))
            .filter_map(|(_, parent)| lookup.get(parent))
            .filter(|&row| source_rows.contains(row))
            .collect();
        rows.sort_unstable();
        rows.dedup();
        level = RowSet::from_keys(rows).minus(&included);
        included.insert(&level);
    }
    Ok(included)
}

/// The rows of `source`, the source of a tree, that every filter accepts, and their
/// ancestors: a table with the columns and the row keys of `source`. `lookup` is the
/// tree's.
///
/// Java keeps the matched rows, the ancestors and the children of every parent between
/// cycles and changes them by what the source's update names. Here a cycle in which the
/// source changes finds the ancestors of every matched row again, and the result's
/// change is the difference from the rows it had: the same rows, for the cost of a walk
/// up from every matched row in every such cycle. The matched rows themselves are kept
/// by the engine's `where`.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.TreeTableFilter
pub(super) fn filtered(
    source: &Arc<dyn Table>,
    lookup: &Arc<RowLookup>,
    parent_column: usize,
    filters: &CheckedFilters,
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, FilterError> {
    let matched = filters.apply(source, compiler)?;
    let included = {
        let (source, matched, lookup) = (source.clone(), matched.clone(), lookup.clone());
        move || with_ancestors(source.as_ref(), &matched.row_set(), &lookup, parent_column)
    };
    let result = QueryTable::derive_merged(
        &[source.clone(), matched.clone()],
        vec![lookup.dependency()],
        || {
            let definition = source.definition().clone();
            let columns = (0..definition.columns().len())
                .map(|index| source.column(index))
                .collect();
            let spec = TableSpec::new(definition, included()?, columns);
            // Behavior: TreeTableFilter.Listener#onUpdate
            let operator = move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
                let Some(upstream) = &upstream[0] else {
                    return Ok(TableUpdate::empty());
                };
                let now = included()?;
                let before = result.row_set();
                let survived = upstream
                    .shifted
                    .apply(&before.minus(&upstream.removed))
                    .map_err(failure)?;
                let kept = survived.intersect(&now);
                let kept_before = upstream.shifted.inverse().apply(&kept).map_err(failure)?;
                let modified = upstream.modified.intersect(&kept);
                Ok(TableUpdate {
                    added: now.minus(&kept),
                    removed: before.minus(&kept_before),
                    modified_columns: if modified.is_empty() {
                        ModifiedColumnSet::empty()
                    } else {
                        upstream.modified_columns.clone()
                    },
                    modified,
                    shifted: upstream.shifted.clone(),
                })
            };
            Ok((spec, operator))
        },
    )
    .map_err(FilterError::Failed)?;
    Ok(result)
}
