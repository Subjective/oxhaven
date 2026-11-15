//! Structured and formula filters. Task 3.2.
//!
//! [`unstructured_filter`] is Java's `where` on filter strings and [`filter`] is the gRPC
//! `filter` operation on structured [`Condition`]s. Both build the same kinds of filter and
//! one result: a table with the parent's columns and the subset of its rows that every
//! filter accepts, kept up to date as the parent changes.
//!
//! How a filter compares depends on its kind, not only on its text; `PARITY.md` has the
//! table. `A == 5`, `A in 5`, `A > 5` and `A + 0 > 5` are a match filter that follows
//! IEEE 754, a match filter in which NaN equals NaN, a range filter with one total order
//! (null lowest, NaN excluded from `>`), and a formula.

mod condition;
mod factory;
mod filters;
mod literal;
mod pattern;

#[cfg(test)]
mod corpus_tests;
#[cfg(test)]
mod tests;

pub use condition::{
    normalize, print, CaseSensitivity, CompareOp, Condition, Literal, MatchType, NanComparison,
    Value,
};

use crate::table::{
    is_add_only, is_append_only, mark, marks, memoized, CopyType, QueryTable, TableSpec,
};
use filters::{FilterSpec, Resolved};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::{Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

/// Why a filter operation did not produce a table.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum FilterError {
    /// A filter is not valid, or does not fit the table: text that is no filter, a missing
    /// column, a value of the wrong type, a formula that does not compile or is not boolean.
    Invalid(String),
    /// The parent table has failed, or a filter failed on the table's rows, for example a
    /// formula that evaluated to null.
    Failed(TableFailure),
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterError::Invalid(message) => f.write_str(message),
            FilterError::Failed(failure) => write!(f, "{failure}"),
        }
    }
}

impl std::error::Error for FilterError {}

impl From<TableFailure> for FilterError {
    fn from(failure: TableFailure) -> Self {
        FilterError::Failed(failure)
    }
}

/// The rows of `selection` that every filter accepts. Each filter sees only the rows the
/// filters before it accepted, and is not run once no rows are left.
///
/// Behavior: io.deephaven.engine.table.impl.AbstractFilterExecution#scheduleCompletion and
/// its `doFilter`. The filters run in the order given; in-memory tables have no cheaper
/// order to offer.
fn accepted(
    filters: &[Resolved],
    table: &dyn Table,
    selection: &RowSet,
) -> Result<RowSet, TableFailure> {
    let mut matched = selection.clone();
    for filter in filters {
        if matched.is_empty() {
            break;
        }
        matched = filter.filter(table, &matched)?;
    }
    Ok(matched)
}

pub(crate) fn shift_failure(error: impl fmt::Display) -> TableFailure {
    TableFailure {
        message: format!("where could not apply the parent's shifts: {error}"),
    }
}

/// What the result of a `where` keeps to follow its parent.
struct WhereOperator {
    parent: Arc<dyn Table>,
    filters: Vec<Resolved>,
    /// The columns the filters read at the current row.
    columns: Vec<usize>,
    /// Whether a filter reads a column at other rows, so that any modification matters.
    reads_arrays: bool,
}

impl WhereOperator {
    /// Behavior: io.deephaven.engine.table.impl.WhereListener#process and
    /// WhereListener#makeUpdateFilterExecution
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure> {
        // Modified rows are filtered again only if a column a filter reads was modified.
        let filter_modified =
            self.reads_arrays || upstream.modified_columns.contains_any(&self.columns);
        filtered_update(result, upstream, filter_modified, |rows| {
            accepted(&self.filters, &*self.parent, rows)
        })
    }
}

/// The change of a filtered table `result` for the change `upstream` of its parent, when
/// what the filters accept has not changed. `accept` gives the rows of a selection of the
/// parent's current rows that pass; it is asked about the added rows and, with
/// `filter_modified`, about the modified rows.
///
/// Behavior: io.deephaven.engine.table.impl.WhereListener#completeUpdate
pub(crate) fn filtered_update(
    result: &QueryTable,
    upstream: &TableUpdate,
    filter_modified: bool,
    accept: impl FnOnce(&RowSet) -> Result<RowSet, TableFailure>,
) -> Result<TableUpdate, TableFailure> {
    let to_filter = if filter_modified {
        upstream.added.union(&upstream.modified)
    } else {
        upstream.added.clone()
    };
    let passed = accept(&to_filter)?;
    let mut added = passed.intersect(&upstream.added);

    // The result's rows before this step, then in the parent's new key space.
    let before = result.row_set();
    let mut removed = before.intersect(&upstream.removed);
    let mapping = upstream
        .shifted
        .apply(&before.minus(&removed))
        .map_err(shift_failure)?;

    let modified = if filter_modified {
        let passed_modified = passed.minus(&upstream.added);
        // A modified row that passes now and did not before is added.
        added.insert(&passed_modified.minus(&mapping));
        // A modified row that passed before and does not now is removed, by the key
        // it had before the shifts.
        let dropped = upstream
            .modified
            .minus(&passed_modified)
            .intersect(&mapping);
        removed.insert(
            &upstream
                .shifted
                .inverse()
                .apply(&dropped)
                .map_err(shift_failure)?,
        );
        passed_modified.intersect(&mapping)
    } else {
        upstream.modified.intersect(&mapping)
    };
    let modified_columns = if modified.is_empty() {
        ModifiedColumnSet::empty()
    } else {
        upstream.modified_columns.clone()
    };
    // A filter never moves a row, so the parent's shifts are the result's.
    Ok(TableUpdate {
        added,
        removed,
        modified,
        shifted: upstream.shifted.clone(),
        modified_columns,
    })
}

/// The attributes of a `where` result. The blink attribute is copied. The result is add-only
/// when the parent is add-only or static, and append-only when the parent is append-only or
/// static: the filters here never change, so a row never leaves or enters on its own.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#whereInternal, which copies the
/// attributes that io.deephaven.engine.table.impl.BaseTable#attributeToCopySet lists for
/// `CopyAttributeOperation.Filter` (of the three here, `BLINK_TABLE_ATTRIBUTE` only) and
/// then sets `ADD_ONLY_TABLE_ATTRIBUTE` and `APPEND_ONLY_TABLE_ATTRIBUTE` from
/// `BaseTable#isAddOnly` and `#isAppendOnly`, which are true of every static table.
pub(crate) fn where_attributes(parent: &dyn Table) -> TableAttributes {
    let mut attributes = TableAttributes::default();
    attributes.blink = parent.attributes().blink;
    attributes.add_only = is_add_only(parent);
    attributes.append_only = is_append_only(parent);
    attributes
}

/// Whether Java memoizes the result of a `where` with this filter: a match filter and a
/// pattern filter, and a conjunction or disjunction of those. A match that Java hands on
/// to a formula is a formula here, and neither that nor a range filter is memoized.
///
/// Behavior: the `canMemoize` of io.deephaven.engine.table.impl.select.MatchFilter,
/// `WhereFilterPatternImpl` and `ComposedFilter`, and the default in `WhereFilter`
fn is_memoized(filter: &Resolved) -> bool {
    match filter {
        Resolved::Match { .. } | Resolved::Pattern { .. } => true,
        Resolved::Range { .. } | Resolved::Condition { .. } => false,
        Resolved::And(parts) | Resolved::Or(parts) => parts.iter().all(is_memoized),
    }
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#whereInternal. The result has the
/// parent's column sources, the attributes of [`where_attributes`], and the parent's input
/// table (`INPUT_TABLE_ATTRIBUTE` is copied for `CopyAttributeOperation.Filter`), and it
/// is sorted by the columns its parent is. Asked of a copy, with filters that Java
/// memoizes, the answer is a copy: see [`memoized`].
fn where_resolved(
    parent: &Arc<dyn Table>,
    filters: Vec<Resolved>,
) -> Result<Arc<dyn Table>, FilterError> {
    // A conjunction at the top is its parts, one filter each.
    let mut flat = Vec::new();
    for filter in filters {
        match filter {
            Resolved::And(parts) => flat.extend(parts),
            other => flat.push(other),
        }
    }
    if flat.is_empty() {
        return Ok(parent.clone());
    }
    let is_memoized = flat.iter().all(is_memoized);
    let mut columns = BTreeSet::new();
    let mut reads_arrays = false;
    for filter in &flat {
        reads_arrays |= filter.inputs(parent.definition(), &mut columns);
    }
    let operator = WhereOperator {
        parent: parent.clone(),
        filters: flat,
        columns: columns.into_iter().collect(),
        reads_arrays,
    };
    let result = QueryTable::derive(parent, || {
        let rows = accepted(&operator.filters, &**parent, &parent.row_set())?;
        let definition = parent.definition().clone();
        let columns = (0..definition.columns().len())
            .map(|index| parent.column(index))
            .collect();
        let spec = TableSpec::new(definition, rows, columns)
            .with_attributes(where_attributes(&**parent))
            .with_input_table_of(&**parent);
        Ok((spec, move |result: &QueryTable, upstream: &TableUpdate| {
            operator.on_update(result, upstream)
        }))
    })?;
    mark(&result, CopyType::Filter.sort_marks(&marks(&**parent)));
    if !is_memoized {
        return Ok(result);
    }
    Ok(memoized(parent, CopyType::Filter, false, result)?)
}

fn where_specs(
    parent: &Arc<dyn Table>,
    specs: &[FilterSpec],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, FilterError> {
    let filters = specs
        .iter()
        .map(|spec| filters::resolve(spec, parent.definition(), compiler))
        .collect::<Result<Vec<_>, _>>()
        .map_err(FilterError::Invalid)?;
    where_resolved(parent, filters)
}

/// The rows of `parent` that satisfy every filter string, as Java's `Table#where` and the
/// gRPC `unstructured_filter` operation select them.
///
/// A string that compares a column with a literal or another column (`A > 5`, `A == B`),
/// or lists values (`A in 1, 2, 3`, `A icase not in "x", "y"`), or searches strings
/// (`A includes any "x", "y"`), is a match, range or pattern filter; anything else, and a
/// comparison whose column or value does not fit the table, is a formula compiled by
/// `compiler`. A formula must be boolean, and one that evaluates to null fails: at once for
/// rows the parent has now, and by failing the result table for rows that arrive later.
///
/// With no filters the result is `parent` itself. If `parent` is static so is the result.
///
/// Behavior: io.deephaven.server.table.ops.UnstructuredFilterTableGrpcImpl#create (without
/// its expression validation, which belongs to the handler),
/// io.deephaven.engine.table.impl.select.WhereFilterFactory#getExpressions,
/// io.deephaven.engine.table.impl.QueryTable#whereInternal,
/// io.deephaven.engine.table.impl.WhereListener
pub fn unstructured_filter(
    parent: &Arc<dyn Table>,
    filters: &[impl AsRef<str>],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, FilterError> {
    let specs = filters
        .iter()
        .map(|text| factory::parse_filter(text.as_ref()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(FilterError::Invalid)?;
    where_specs(parent, &specs, compiler)
}

/// The rows of `parent` that satisfy every condition, as the gRPC `filter` operation
/// selects them.
///
/// The conditions are joined by AND, normalized (see [`normalize`]) and turned into
/// filters: an ordering of a column and a literal is a range filter, `in` a match filter,
/// `contains` and `matches` pattern filters, and the rest goes through its printed text
/// (see [`print`]) as a filter string does in [`unstructured_filter`].
///
/// With no conditions the result is `parent` itself.
///
/// Behavior: io.deephaven.server.table.ops.FilterTableGrpcImpl#create (without its
/// expression validation), io.deephaven.server.table.ops.filter.FilterFactory#makeFilter
pub fn filter(
    parent: &Arc<dyn Table>,
    conditions: &[Condition],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, FilterError> {
    let Some(condition) = normalize(conditions).map_err(FilterError::Invalid)? else {
        return Ok(parent.clone());
    };
    let spec = condition::to_spec(&condition).map_err(FilterError::Invalid)?;
    where_specs(parent, &[spec], compiler)
}
