//! `view`, `update_view`, `lazy_update`, `select`, `update` and `drop_columns`.
//!
//! Each takes the parent table and a list of column strings, `Name`, `Name = Other` or
//! `Name = formula`, and the formula compiler to read them with.
//!
//! | Operation | Result columns | Formula values |
//! | --- | --- | --- |
//! | [`view`] | the listed ones | computed on every read |
//! | [`update_view`] | the parent's, then the listed ones | computed on every read |
//! | [`lazy_update`] | the parent's, then the listed ones | computed on read, once per distinct input values |
//! | [`select`] | the listed ones | stored |
//! | [`update`] | the parent's, then the listed ones | stored |
//! | [`drop_columns`] | the parent's without the listed ones | |
//!
//! A column of the list can read the parent's columns and the columns listed before it. A
//! listed name that exists already, in the parent or earlier in the list, replaces that
//! column where it stands, and columns listed after it read the replacement.
//!
//! A formula that fails while a stored column is computed fails the operation, or, in a
//! later cycle, the result table. A formula computed on read fails that read only.
//!
//! Not ported: formulas that read a column at another row (`Col_[i - 1]`), which Java turns
//! into a join of the table with itself in `ShiftedColumnsFactory`; the redirected storage of
//! `QueryTable.redirectSelect` and `QueryTable.redirectUpdate`, which are off by default; and
//! `DeferredViewTable`, which serves source tables that are not in memory.

mod materialize;
mod plan;
mod sources;

#[cfg(test)]
mod tests;

use crate::table::{QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
use oxhaven_contracts::expr::{FormulaCompiler, FormulaError};
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use plan::{Kind, Plan, Sources};
use sources::{ConstantSource, FormulaInput, FormulaSource, LazySource};
use std::fmt;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// Why one of the operations of this module was refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SelectError {
    /// A column string is not a valid assignment, or its formula does not compile against
    /// the columns it can read.
    Formula {
        /// The column string.
        column: String,
        error: FormulaError,
    },
    /// A column named on its own, or a column to drop, does not exist.
    NoSuchColumn(String),
    /// A result column would have a reserved name.
    InvalidName(String),
    /// A formula depends on row positions, row keys or other rows of a refreshing table
    /// whose rows can move.
    UnsafeForRefresh(String),
    /// A formula failed while the result was computed, or the parent table has failed.
    Failed(TableFailure),
}

impl fmt::Display for SelectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelectError::Formula { column, error } => write!(f, "{column}: {error}"),
            SelectError::NoSuchColumn(message)
            | SelectError::InvalidName(message)
            | SelectError::UnsafeForRefresh(message) => f.write_str(message),
            SelectError::Failed(failure) => write!(f, "{failure}"),
        }
    }
}

impl std::error::Error for SelectError {}

impl From<TableFailure> for SelectError {
    fn from(failure: TableFailure) -> Self {
        SelectError::Failed(failure)
    }
}

/// Which of the five formula operations a call is.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable.Flavor and
/// io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.Mode
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flavor {
    View,
    UpdateView,
    LazyUpdate,
    Select,
    Update,
}

impl Flavor {
    /// Whether the result starts with the parent's columns.
    fn keeps_parent_columns(self) -> bool {
        matches!(
            self,
            Flavor::UpdateView | Flavor::LazyUpdate | Flavor::Update
        )
    }
}

/// The attributes of the result of a formula operation.
///
/// Behavior: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#applyShiftsAndRemainingColumns,
/// which sets them for a refreshing parent only, from
/// io.deephaven.engine.table.impl.BaseTable#isAddOnly, #isAppendOnly and #isBlink. The
/// `copyAttributes` calls of `view`, `updateView` and `lazyUpdate` copy none of the three.
fn result_attributes(parent: &dyn Table) -> TableAttributes {
    let mut result = TableAttributes::default();
    if parent.is_refreshing() {
        let attributes = parent.attributes();
        result.add_only = attributes.add_only || attributes.append_only;
        result.append_only = attributes.append_only || (attributes.add_only && parent.is_flat());
        result.blink = attributes.blink;
    }
    result
}

/// The listed columns, each computed from the parent's current values whenever it is read.
/// With no columns, the parent itself.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#view
pub fn view<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, SelectError> {
    if columns.is_empty() {
        return Ok(parent.clone());
    }
    computed_on_read(parent, columns, compiler, Flavor::View)
}

/// The parent's columns and the listed ones, each computed from the parent's current values
/// whenever it is read. With no columns, the parent itself.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#updateView
pub fn update_view<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, SelectError> {
    if columns.is_empty() {
        return Ok(parent.clone());
    }
    computed_on_read(parent, columns, compiler, Flavor::UpdateView)
}

/// As [`update_view`], but each formula remembers its result for every distinct tuple of
/// the values of the columns it reads, and computes a tuple it has seen before never again:
/// not for another row, not in a later cycle, not for a previous-value read. Nothing is
/// stored per row and nothing is ever forgotten, so it suits a costly formula over few
/// distinct inputs. A formula that fails is not remembered.
///
/// When the parent modifies rows the result reports every column as modified, because
/// Java's listener for this operation does not translate modified columns.
///
/// # Panics
/// With `unported!` for a formula that uses `i`, `ii`, `k` or a column array: Java's cache
/// key for those has not been ported.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#lazyUpdate, with the listener
/// io.deephaven.engine.table.impl.BaseTable.ListenerImpl#onUpdate
pub fn lazy_update<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, SelectError> {
    computed_on_read(parent, columns, compiler, Flavor::LazyUpdate)
}

/// The listed columns with their values stored, so that a formula is computed once per row
/// and again only when a column it reads changes in that row. With no columns, all of the
/// parent's.
///
/// A column that only names a parent column whose values are already held in memory shares
/// that column instead of copying it. The result of a static table has row keys `0..size`
/// unless such a shared column ties it to the parent's row keys.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#select
pub fn select<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, SelectError> {
    if columns.is_empty() {
        let all: Vec<&str> = parent
            .definition()
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        return materialize::stored(parent, &all, compiler, Flavor::Select);
    }
    materialize::stored(parent, columns, compiler, Flavor::Select)
}

/// The parent's columns and the listed ones, the listed ones with their values stored as
/// in [`select`].
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#update
pub fn update<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, SelectError> {
    materialize::stored(parent, columns, compiler, Flavor::Update)
}

/// The parent without the named columns. With no names, the parent itself.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#dropColumns
pub fn drop_columns<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
) -> Result<Arc<dyn Table>, SelectError> {
    if columns.is_empty() {
        return Ok(parent.clone());
    }
    let declared = parent.definition().columns();
    let missing: Vec<&str> = columns
        .iter()
        .map(AsRef::as_ref)
        .filter(|name| parent.definition().index_of(name).is_none())
        .collect();
    if !missing.is_empty() {
        let available: Vec<&str> = declared.iter().map(|c| c.name.as_str()).collect();
        return Err(SelectError::NoSuchColumn(format!(
            "Unknown column names [{}], available column names are [{}]",
            missing.join(", "),
            available.join(", ")
        )));
    }
    let kept: Vec<usize> = (0..declared.len())
        .filter(|&index| !columns.iter().any(|c| c.as_ref() == declared[index].name))
        .collect();
    let definition = kept
        .iter()
        .map(|&index| declared[index].clone())
        .collect::<Vec<ColumnDefinition>>();
    let definition = match TableDefinition::new(definition) {
        Ok(definition) => definition,
        Err(e) => panic!("a subset of distinct names is distinct: {e}"),
    };
    // Behavior: the entries of io.deephaven.engine.table.impl.BaseTable#attributeToCopySet
    // that list `CopyAttributeOperation.DropColumns`.
    let mut attributes = TableAttributes::default();
    let inherited = parent.attributes();
    attributes.add_only = inherited.add_only;
    attributes.append_only = inherited.append_only;
    attributes.blink = inherited.blink;

    let transformer =
        ModifiedColumnTransformer::new(kept.iter().enumerate().map(|(to, &from)| (from, [to])));
    let table = QueryTable::derive(parent, || {
        let sources = kept.iter().map(|&index| parent.column(index)).collect();
        Ok((
            spec(parent, definition, sources, attributes),
            // A change to dropped columns only leaves modified rows without modified
            // columns, which the table drops before it delivers the update.
            move |_result: &QueryTable, upstream: &TableUpdate| {
                Ok(TableUpdate {
                    modified_columns: transformer.transform(&upstream.modified_columns),
                    ..upstream.clone()
                })
            },
        ))
    })?;
    Ok(table)
}

/// A result that has the parent's rows.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#propagateFlatness
fn spec(
    parent: &Arc<dyn Table>,
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
    attributes: TableAttributes,
) -> TableSpec {
    let spec = TableSpec::new(definition, (*parent.row_set()).clone(), columns)
        .with_attributes(attributes);
    if parent.is_flat() {
        spec.flat()
    } else {
        spec
    }
}

/// Behavior: io.deephaven.engine.table.impl.QueryTable#viewOrUpdateView and #lazyUpdate,
/// with the layers io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext
/// makes in its `VIEW_EAGER` and `VIEW_LAZY` modes
fn computed_on_read<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
    flavor: Flavor,
) -> Result<Arc<dyn Table>, SelectError> {
    let plan = plan::analyze(parent, columns, compiler, flavor)?;
    let mut sources = Sources::new(parent);
    for step in &plan.steps {
        let source = match &step.kind {
            Kind::Source(named) => sources.of(named.origin),
            Kind::Constant(formula) => ConstantSource::evaluate(
                formula,
                step.scope_definition(),
                sources.all(&step.scope),
            )?,
            Kind::Formula(formula) => {
                let input =
                    FormulaInput::over(parent, step.scope_definition(), sources.all(&step.scope));
                if flavor == Flavor::LazyUpdate {
                    if formula.uses_row_variables() || !formula.column_arrays().is_empty() {
                        unported!(
                            "io.deephaven.engine.table.impl.select.DhFormulaColumn#generateAppropriateGetMethod",
                            "the cache key of a lazy formula that uses i, ii, k or a column array: {}",
                            step.name
                        );
                    }
                    LazySource::shared(formula.clone(), input)?
                } else {
                    FormulaSource::shared(formula.clone(), input)
                }
            }
        };
        sources.push(source);
    }
    let columns = sources.all(&plan.published);
    let table = QueryTable::derive(parent, || {
        let mut result = spec(
            parent,
            plan.definition(),
            columns,
            result_attributes(&**parent),
        );
        // Behavior: QueryTable#viewOrUpdateView copies attributes with
        // `CopyAttributeOperation.UpdateView` or `.View`, and QueryTable#lazyUpdate with
        // `.UpdateView`. BaseTable#attributeToCopySet lists `INPUT_TABLE_ATTRIBUTE` for
        // `UpdateView` and not for `View`.
        if flavor != Flavor::View {
            result = result.with_input_table_of(&**parent);
        }
        Ok((result, passes_changes_through(&plan, flavor)))
    })?;
    Ok(table)
}

/// The operator of a result that stores nothing: the parent's change is the result's, with
/// the modified columns translated.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable.ViewOrUpdateViewListener#onUpdate
/// and, for `lazy_update`, io.deephaven.engine.table.impl.BaseTable.ListenerImpl#onUpdate,
/// which cannot reuse the parent's modified columns and reports all.
fn passes_changes_through(
    plan: &Plan,
    flavor: Flavor,
) -> impl Fn(&QueryTable, &TableUpdate) -> Result<TableUpdate, TableFailure> + Send + Sync + 'static
{
    let transformer = plan.transformer();
    move |_result, upstream| {
        Ok(TableUpdate {
            modified_columns: if flavor == Flavor::LazyUpdate {
                ModifiedColumnSet::All
            } else {
                transformer.transform(&upstream.modified_columns)
            },
            ..upstream.clone()
        })
    }
}
