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
//! | [`rename_columns`] | the parent's, some under another name | |
//! | [`move_columns`], [`move_columns_up`], [`move_columns_down`] | the parent's in another order, some under another name | |
//!
//! A column of the list can read the parent's columns and the columns listed before it. A
//! listed name that exists already, in the parent or earlier in the list, replaces that
//! column where it stands, and columns listed after it read the replacement.
//!
//! A formula that fails while a stored column is computed fails the operation, or, in a
//! later cycle, the result table. A formula computed on read fails that read only.
//!
//! A formula can read a column at another row: `Col_[i - 1]`, `Col_[3]`, `sum(Col_)`. The
//! module `arrays` has the form Java rewrites into a shifted column and what is refused.
//!
//! Not ported: the redirected storage of `QueryTable.redirectSelect` and `QueryTable.redirectUpdate`, which are off by default; and
//! `DeferredViewTable`, which serves source tables that are not in memory.

mod array_formula;
pub(crate) mod arrays;
mod materialize;
mod plan;
mod rename;
mod sources;

#[cfg(test)]
mod tests;

use crate::table::{mark, marks, memoized, CopyType, Marks, QueryTable, TableSpec};
use crate::update::ModifiedColumnTransformer;
pub use array_formula::ArrayFormula;
use oxhaven_contracts::expr::{ColumnAssignment, FormulaCompiler, FormulaError};
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use plan::{Kind, Plan, Sources};
pub use rename::{move_columns, move_columns_down, move_columns_up, rename_columns, ColumnPair};
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
    /// A result column would have a reserved name, or a name is not a column name.
    InvalidName(String),
    /// Java's `IllegalArgumentException` for the pairs of a rename or a move: a column to
    /// rename does not exist, or a column is named twice.
    IllegalArgument(String),
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
            | SelectError::IllegalArgument(message)
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
    computed_on_read(parent, columns, compiler, Flavor::View, None)
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
    computed_on_read(parent, columns, compiler, Flavor::UpdateView, None)
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
    computed_on_read(parent, columns, compiler, Flavor::LazyUpdate, None)
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
        return materialize::stored(parent, &all, compiler, Flavor::Select, None);
    }
    materialize::stored(parent, columns, compiler, Flavor::Select, None)
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
    materialize::stored(parent, columns, compiler, Flavor::Update, None)
}

/// The parent without the named columns. With no names, the parent itself.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#dropColumns
pub fn drop_columns<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
) -> Result<Arc<dyn Table>, SelectError> {
    drop_columns_as(parent, columns, None)
}

/// [`drop_columns`], with `replaced` in place of the attributes it copies from the parent
/// when given; such a result is not memoized.
fn drop_columns_as<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    replaced: Option<TableAttributes>,
) -> Result<Arc<dyn Table>, SelectError> {
    if columns.is_empty() && replaced.is_none() {
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
    let memoize = replaced.is_none();
    let attributes = replaced.unwrap_or(attributes);

    let of_parent = marks(&**parent);
    let mut result_marks = CopyType::DropColumns.sort_marks(&of_parent);
    result_marks.sortable = rename::sortable_after_drop(&of_parent, &definition);

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
    mark(&table, result_marks);
    if !memoize {
        return Ok(table);
    }
    Ok(memoized(parent, CopyType::DropColumns, false, table)?)
}

/// The definition of a result with the columns `definition` and their sources `columns`:
/// a column is a partitioning column when its source is the source of a partitioning
/// column of the parent, under whatever name.
///
/// Behavior: io.deephaven.engine.table.TableDefinition#inferFrom(Table, Map)
fn inferred(
    parent: &Arc<dyn Table>,
    definition: TableDefinition,
    columns: &[Arc<dyn ColumnSource>],
) -> TableDefinition {
    let partitioning: Vec<Arc<dyn ColumnSource>> = parent
        .definition()
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, column)| column.partitioning)
        .map(|(index, _)| parent.column(index))
        .collect();
    let same = |a: &Arc<dyn ColumnSource>, b: &Arc<dyn ColumnSource>| {
        std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
    };
    if partitioning.is_empty() {
        return definition;
    }
    let declared = definition.columns().iter().zip(columns);
    let declared = declared.map(|(column, source)| ColumnDefinition {
        partitioning: partitioning.iter().any(|kept| same(kept, source)),
        ..column.clone()
    });
    match TableDefinition::new(declared.collect()) {
        Ok(definition) => definition,
        Err(duplicate) => unreachable!("{duplicate}: the names were a definition's"),
    }
}

/// A result that has the parent's rows, and the partitioning columns of [`inferred`].
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#propagateFlatness
fn spec(
    parent: &Arc<dyn Table>,
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
    attributes: TableAttributes,
) -> TableSpec {
    let definition = inferred(parent, definition, &columns);
    let spec = TableSpec::new(definition, (*parent.row_set()).clone(), columns)
        .keeping_partitioning()
        .with_attributes(attributes);
    if parent.is_flat() {
        spec.flat()
    } else {
        spec
    }
}

/// `first` is as for [`plan::analyze`].
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#viewOrUpdateView and #lazyUpdate,
/// with the layers io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext
/// makes in its `VIEW_EAGER` and `VIEW_LAZY` modes
fn computed_on_read<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
    flavor: Flavor,
    first: Option<ColumnAssignment>,
) -> Result<Arc<dyn Table>, SelectError> {
    let mut plan = plan::analyze(parent, columns, compiler, flavor, first)?;
    if let Some(shifted) = plan.shifted.take() {
        return arrays::with_shifted_column(parent, columns, compiler, flavor, shifted);
    }
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
    let copy_type = match flavor {
        Flavor::View => CopyType::View,
        _ => CopyType::UpdateView,
    };
    let of_parent = marks(&**parent);
    mark(
        &table,
        Marks {
            sortable: rename::sortable_after_select(&of_parent, &plan),
            ..copy_type.sort_marks(&of_parent)
        },
    );
    if flavor == Flavor::LazyUpdate {
        // Behavior: QueryTable#lazyUpdate does not memoize its result
        return Ok(table);
    }
    if !plan.is_memoized() {
        return Ok(table);
    }
    Ok(memoized(parent, copy_type, false, table)?)
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
