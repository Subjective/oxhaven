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
mod kernels;
mod literal;
mod pattern;
mod search;

#[cfg(test)]
mod corpus_tests;
#[cfg(test)]
mod kernel_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod time_tests;

pub(crate) use condition::java_double_to_string;
pub use condition::{
    normalize, print, CaseSensitivity, CompareOp, Condition, Literal, MatchType, NanComparison,
    Value,
};

use crate::ops::select::arrays::Shift;
use crate::ops::select::arrays::{add_shifted_columns, constant_array_accesses, Replacing};
use crate::ops::select::{drop_columns, SelectError};
use crate::table::{
    is_add_only, is_append_only, mark, marks, memoized, CopyType, QueryTable, TableSpec,
};
use filters::{FilterSpec, Resolved};
use oxhaven_contracts::expr::{Expr, FormulaCompiler};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::TableDefinition;
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
        matched = filter.filter(table, &matched, false)?;
    }
    Ok(matched)
}

/// Filter strings checked against a table, for an operation that asks row by row which
/// rows pass, before and after a change: the `count_where` aggregation and the count-where
/// operators of `update_by`.
pub(crate) struct RowFilters {
    filters: Vec<Resolved>,
    columns: Vec<usize>,
    reads_arrays: bool,
    shapes: Vec<FilterShape>,
}

/// What an operator that treats the kinds of filter differently needs to know of one.
pub(crate) struct FilterShape {
    /// The filter is a formula.
    pub(crate) formula: bool,
    /// It was written as one, and is not a comparison that became one because its column
    /// or value did not fit a match or a range filter. Java keeps such a formula inside
    /// the filter it tried first, where a test for a `ConditionFilter` does not see it.
    pub(crate) written_as_formula: bool,
    /// It is a formula that was written as a match (`A == B`, `A in x`).
    pub(crate) written_as_match: bool,
    /// It uses `i`, `ii` or `k`.
    pub(crate) row_variables: bool,
    /// It reads a column at other rows.
    pub(crate) arrays: bool,
    /// The columns it reads at the current row, as indexes.
    pub(crate) columns: Vec<usize>,
}

impl RowFilters {
    /// The filters of `texts`, each made as [`unstructured_filter`] makes one.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.WhereFilterFactory#getExpression and
    /// the `init` of each `WhereFilter`
    pub(crate) fn parse(
        texts: &[String],
        definition: &oxhaven_contracts::schema::TableDefinition,
        compiler: &dyn FormulaCompiler,
    ) -> Result<RowFilters, String> {
        let mut filters = Vec::with_capacity(texts.len());
        let mut columns = BTreeSet::new();
        let mut reads_arrays = false;
        let mut shapes = Vec::with_capacity(texts.len());
        for text in texts {
            let spec = factory::parse_filter(text)?;
            let filter = filters::resolve(&spec, definition, compiler)?;
            let mut own = BTreeSet::new();
            let arrays = filter.inputs(definition, &mut own);
            reads_arrays |= arrays;
            shapes.push(FilterShape {
                formula: matches!(filter, Resolved::Condition { .. }),
                written_as_formula: matches!(spec, FilterSpec::Condition(_)),
                written_as_match: matches!(filter, Resolved::Condition { .. })
                    && matches!(spec, FilterSpec::Match { .. }),
                row_variables: filter.uses_row_variables(),
                arrays,
                columns: own.iter().copied().collect(),
            });
            columns.extend(own);
            filters.push(filter);
        }
        Ok(RowFilters {
            filters,
            columns: columns.into_iter().collect(),
            reads_arrays,
            shapes,
        })
    }

    /// The shape of each filter, in order.
    pub(crate) fn shapes(&self) -> &[FilterShape] {
        &self.shapes
    }

    /// The columns the filters read at the current row, as indexes, increasing.
    pub(crate) fn columns(&self) -> &[usize] {
        &self.columns
    }

    /// Whether a filter reads a column at other rows (`Column_`).
    pub(crate) fn reads_arrays(&self) -> bool {
        self.reads_arrays
    }

    /// Whether a filter uses `i`, `ii` or `k`.
    pub(crate) fn uses_row_variables(&self) -> bool {
        self.filters.iter().any(Resolved::uses_row_variables)
    }

    /// The rows of `selection` that every filter accepts, by the current values of
    /// `table`, or with `use_prev` by its previous values. The filters run in order, each
    /// on the rows the ones before it accepted.
    pub(crate) fn accepted(
        &self,
        table: &dyn Table,
        selection: &RowSet,
        use_prev: bool,
    ) -> Result<RowSet, TableFailure> {
        let mut matched = selection.clone();
        for filter in &self.filters {
            if matched.is_empty() {
                break;
            }
            matched = filter.filter(table, &matched, use_prev)?;
        }
        Ok(matched)
    }
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

/// Whether Java memoizes the result of a `where` with this filter: a match filter, a
/// pattern filter and the filter that accepts no row, and a conjunction or disjunction of
/// those. A match that Java hands on to a formula is a formula here, and neither that nor
/// a range filter is memoized.
///
/// Behavior: the `canMemoize` of io.deephaven.engine.table.impl.select.MatchFilter,
/// `WhereFilterPatternImpl`, `WhereNoneFilter` and `ComposedFilter`, and the default in
/// `WhereFilter`
fn is_memoized(filter: &Resolved) -> bool {
    match filter {
        Resolved::Match { .. } | Resolved::Pattern { .. } | Resolved::Nothing => true,
        // Behavior: VectorComponentFilterWrapper takes the default of WhereFilter.
        Resolved::Range { .. }
        | Resolved::Between { .. }
        | Resolved::Condition { .. }
        | Resolved::AnyElement { .. } => false,
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
            .keeping_partitioning()
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

/// The formulas of `spec` that read a column at a fixed distance from the current row
/// (`Col_[i - 1]`), each with the expression that reads a shifted column instead, and the
/// shifted columns those read. Fails, as Java's parser does, for an offset that
/// `Long#parseLong` cannot read (`Col_[i + 0x1]`).
///
/// Behavior: io.deephaven.engine.table.impl.select.ExtractShiftedColumnDefinitions and
/// io.deephaven.engine.table.impl.select.TransformToFinalFormula
fn shifted_reads(
    spec: &FilterSpec,
    compiler: &dyn FormulaCompiler,
    rewritten: &mut Vec<(String, Expr)>,
    shifts: &mut Vec<Shift>,
) -> Result<(), FilterError> {
    let text = match spec {
        FilterSpec::And(parts) | FilterSpec::Or(parts) => {
            for part in parts {
                shifted_reads(part, compiler, rewritten, shifts)?;
            }
            return Ok(());
        }
        FilterSpec::Condition(text) => text,
        FilterSpec::Match {
            failover: Some(text),
            ..
        }
        | FilterSpec::Range {
            failover: Some(text),
            ..
        } => text,
        _ => return Ok(()),
    };
    let Ok(expr) = compiler.parse(text) else {
        return Ok(());
    };
    let found = constant_array_accesses(text, &expr)
        .map_err(|error| FilterError::Invalid(format!("{text}: {error}")))?;
    if let Some(found) = found {
        for shift in found.shifts {
            if !shifts.contains(&shift) {
                shifts.push(shift);
            }
        }
        rewritten.push((text.clone(), found.expr));
    }
    Ok(())
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#whereInternal, and
/// io.deephaven.engine.table.impl.ShiftedColumnsFactory#where for filters that read a
/// column at a fixed distance from the current row: the shifted columns of all the filters
/// are added to the table, the filters are applied reading those, and the columns are
/// dropped. The table in between has none of the parent's attributes.
fn where_specs(
    parent: &Arc<dyn Table>,
    specs: &[FilterSpec],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, FilterError> {
    let resolve = |definition: &TableDefinition, compiler: &dyn FormulaCompiler| {
        specs
            .iter()
            .map(|spec| filters::resolve(spec, definition, compiler))
            .collect::<Result<Vec<_>, _>>()
            .map_err(FilterError::Invalid)
    };
    let filters = resolve(parent.definition(), compiler);
    let (mut rewritten, mut shifts) = (Vec::new(), Vec::new());
    for spec in specs {
        shifted_reads(spec, compiler, &mut rewritten, &mut shifts)?;
    }
    // A shifted column needs no more of its column than that it exists, so filters that
    // do not fit the table are judged again once the shifted columns stand in.
    let shifted = !shifts.is_empty()
        && (filters.is_ok()
            || shifts
                .iter()
                .all(|shift| parent.definition().index_of(&shift.column).is_some()));
    if !shifted {
        return where_resolved(parent, filters?);
    }
    let with_shifts = add_shifted_columns(parent, &shifts)?;
    let replacing = Replacing::expressions(compiler, rewritten);
    let filters = resolve(with_shifts.definition(), &replacing)?;
    let kept = where_resolved(&with_shifts, filters)?;
    let dropped: Vec<String> = shifts.iter().map(Shift::result_name).collect();
    drop_columns(&kept, &dropped).map_err(|error| match error {
        SelectError::Failed(failure) => FilterError::Failed(failure),
        other => FilterError::Invalid(other.to_string()),
    })
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

/// The rows of `parent` that satisfy at least one of the filter strings, each read as
/// [`unstructured_filter`] reads a filter string. One filter string is that filter. None
/// is refused: Java makes the filter that accepts no row of them, and its `where` does
/// not take that filter.
///
/// A row is kept by the first of the filters that accepts it, so a formula is not asked
/// about a row that an earlier filter accepted.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#where of
/// io.deephaven.api.filter.Filter#or of io.deephaven.api.RawString filters, through
/// io.deephaven.engine.table.impl.select.WhereFilterAdapter and
/// io.deephaven.engine.table.impl.select.DisjunctiveFilter
pub fn where_one_of(
    parent: &Arc<dyn Table>,
    filters: &[impl AsRef<str>],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, FilterError> {
    let mut specs = filters
        .iter()
        .map(|text| factory::parse_filter(text.as_ref()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(FilterError::Invalid)?;
    let spec = match specs.len() {
        // Behavior: `WhereFilterAdapter#of(boolean)` for `Filter#ofFalse`
        0 => {
            return Err(FilterError::Invalid(
                "Should not be constructing literal boolean WhereFilter".into(),
            ))
        }
        1 => specs.remove(0),
        _ => FilterSpec::Or(specs),
    };
    where_specs(parent, &[spec], compiler)
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

/// What one filter of a [`CheckedFilters`] reads.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FilterReads {
    /// The filter as it was written, or as its condition prints.
    pub text: String,
    /// The columns it reads at the current row.
    pub columns: Vec<String>,
    /// Whether it reads a column at other rows (`Column_`).
    pub arrays: bool,
    /// Whether it uses `i`, `ii` or `k`.
    pub row_variables: bool,
}

/// Filters made for the columns of a table before they are applied to it, for an
/// operation that restricts what its filters may read, as a rollup does.
///
/// Behavior: the `WhereFilter#init`, `#getColumns` and `#getColumnArrays` that
/// io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#initializeAndValidateFilters
/// calls before its filters are applied
pub struct CheckedFilters {
    specs: Vec<FilterSpec>,
    reads: Vec<FilterReads>,
}

impl CheckedFilters {
    fn of_specs(
        specs: Vec<(String, FilterSpec)>,
        definition: &TableDefinition,
        compiler: &dyn FormulaCompiler,
    ) -> Result<CheckedFilters, FilterError> {
        let mut reads = Vec::with_capacity(specs.len());
        for (text, spec) in &specs {
            let (mut rewritten, mut shifts) = (Vec::new(), Vec::new());
            shifted_reads(spec, compiler, &mut rewritten, &mut shifts)?;
            let mut columns = BTreeSet::new();
            let (mut arrays, mut row_variables) = (!shifts.is_empty(), false);
            match filters::resolve(spec, definition, compiler) {
                Ok(filter) => {
                    arrays |= filter.inputs(definition, &mut columns);
                    row_variables = filter.uses_row_variables();
                }
                // a filter that reads a shifted column is made again by `where` once that
                // column stands in
                Err(_) if arrays => {}
                Err(error) => return Err(FilterError::Invalid(error)),
            }
            reads.push(FilterReads {
                text: text.clone(),
                columns: columns
                    .into_iter()
                    .map(|index| definition.columns()[index].name.clone())
                    .collect(),
                arrays,
                row_variables,
            });
        }
        Ok(CheckedFilters {
            specs: specs.into_iter().map(|(_, spec)| spec).collect(),
            reads,
        })
    }

    /// The filters of `texts`, each read as [`unstructured_filter`] reads a filter string,
    /// for a table of `definition`.
    pub fn parse(
        texts: &[impl AsRef<str>],
        definition: &TableDefinition,
        compiler: &dyn FormulaCompiler,
    ) -> Result<CheckedFilters, FilterError> {
        let specs = texts
            .iter()
            .map(|text| {
                Ok((
                    text.as_ref().to_string(),
                    factory::parse_filter(text.as_ref())?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(FilterError::Invalid)?;
        CheckedFilters::of_specs(specs, definition, compiler)
    }

    /// The one filter that `conditions` are, as [`filter`] makes it, for a table of
    /// `definition`; no filter for no conditions.
    pub fn of_conditions(
        conditions: &[Condition],
        definition: &TableDefinition,
        compiler: &dyn FormulaCompiler,
    ) -> Result<CheckedFilters, FilterError> {
        let specs = match normalize(conditions).map_err(FilterError::Invalid)? {
            Some(condition) => vec![(
                print(&condition),
                condition::to_spec(&condition).map_err(FilterError::Invalid)?,
            )],
            None => Vec::new(),
        };
        CheckedFilters::of_specs(specs, definition, compiler)
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// What each filter reads, in order.
    pub fn reads(&self) -> &[FilterReads] {
        &self.reads
    }

    /// The rows of `parent` that every filter accepts, as [`unstructured_filter`] and
    /// [`filter`] give them. `parent` has the columns the filters were made for.
    pub fn apply(
        &self,
        parent: &Arc<dyn Table>,
        compiler: &dyn FormulaCompiler,
    ) -> Result<Arc<dyn Table>, FilterError> {
        if self.specs.is_empty() {
            return Ok(parent.clone());
        }
        where_specs(parent, &self.specs, compiler)
    }
}
