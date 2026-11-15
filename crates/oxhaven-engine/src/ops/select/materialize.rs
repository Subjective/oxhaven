//! `select` and `update`: columns whose values are computed once and stored.

use super::plan::{self, Kind, Origin, Plan, Sources, Step};
use super::sources::{ByPosition, ConstantSource, FormulaInput};
use super::{result_attributes, spec, Flavor, SelectError};
use crate::table::{holds_values, memoized, tracked_source, CopyType, QueryTable, TableSpec};
use arrow::array::ArrayRef;
use oxhaven_column::{FlatSource, RedirectedSource, RowRedirection, WritableSource};
use oxhaven_contracts::expr::{Formula, FormulaCompiler};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

/// Behavior: io.deephaven.engine.table.impl.QueryTable#selectOrUpdate
pub(super) fn stored<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
    flavor: Flavor,
) -> Result<Arc<dyn Table>, SelectError> {
    let plan = plan::analyze(parent, columns, compiler, flavor)?;
    let table = match parent.update_graph() {
        Some(graph) if parent.is_refreshing() => QueryTable::derive(parent, || {
            let (columns, stored) = refreshing_columns(parent, &plan, |column_type| {
                tracked_source(&graph, column_type)
            })?;
            let result = spec(
                parent,
                plan.definition(),
                columns,
                result_attributes(&**parent),
            );
            Ok((result, keeps_up_to_date(&plan, stored)))
        })?,
        _ => QueryTable::new_static(static_result(parent, &plan, flavor)?),
    };
    if !plan.is_memoized() {
        return Ok(table);
    }
    // Behavior: the `Select` and `Update` flavors of
    // MemoizedOperationKey.SelectUpdateViewOrUpdateView#copyType, which copy nothing
    Ok(memoized(parent, CopyType::None, false, table)?)
}

/// Where a stored column's values come from.
enum Values {
    /// Another column that does not hold its values, read once per row and kept.
    Copy(Arc<dyn ColumnSource>),
    Formula(Arc<dyn Formula>, FormulaInput),
}

impl Values {
    fn read(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        match self {
            Values::Copy(source) => source.chunk(rows),
            Values::Formula(formula, input) => formula.evaluate(input, rows, false),
        }
    }
}

/// A column of a refreshing result that this operation writes.
struct Stored {
    column: Arc<WritableSource>,
    values: Values,
    /// The parent columns whose change in a row changes this column in that row.
    parent_columns: Vec<usize>,
}

/// The source column a step can share in place of making its own: the one it names if that
/// holds its values, or else the copy an earlier step of this call made of the same name.
///
/// Behavior: the `PreserveColumnLayer` branches and `resultAlias` of
/// io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext
struct Shared {
    /// Copies made so far, by the name of the column copied.
    copies: Vec<(String, Arc<dyn ColumnSource>)>,
}

impl Shared {
    /// Call at every step, before anything else: a new column of a name hides the copy that
    /// was made of the old column of that name.
    fn begin(&mut self, step: &Step) {
        self.copies.retain(|(name, _)| *name != step.name);
    }

    fn existing(
        &self,
        name: &str,
        named: &Arc<dyn ColumnSource>,
        holds: bool,
    ) -> Option<Arc<dyn ColumnSource>> {
        if holds {
            return Some(named.clone());
        }
        self.copies
            .iter()
            .find(|(copied, _)| copied == name)
            .map(|(_, copy)| copy.clone())
    }

    fn made(&mut self, name: &str, copy: &Arc<dyn ColumnSource>) {
        self.copies.push((name.to_string(), copy.clone()));
    }
}

/// Every column of a result, and those of them the operation has to keep up to date.
type Columns = (Vec<Arc<dyn ColumnSource>>, Vec<Stored>);

/// The columns of a refreshing result, filled for the parent's current rows, and those of
/// them the operation has to keep up to date.
///
/// Behavior: the `SELECT_REFRESHING` mode of
/// io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext and
/// the initial update of io.deephaven.engine.table.impl.QueryTable#selectOrUpdate
fn refreshing_columns(
    parent: &Arc<dyn Table>,
    plan: &Plan,
    new_column: impl Fn(ColumnType) -> Arc<WritableSource>,
) -> Result<Columns, TableFailure> {
    let rows = parent.row_set();
    let mut sources = Sources::new(parent);
    let mut shared = Shared { copies: Vec::new() };
    let mut stored = Vec::new();
    for step in &plan.steps {
        shared.begin(step);
        let values = match &step.kind {
            Kind::Constant(formula) => {
                let constant = ConstantSource::evaluate(
                    formula,
                    step.scope_definition(),
                    sources.all(&step.scope),
                )?;
                sources.push(constant);
                continue;
            }
            Kind::Source(named) => {
                let source = sources.of(named.origin);
                let holds = holds_values(&source);
                if let Some(existing) = shared.existing(&named.name, &source, holds) {
                    sources.push(existing);
                    continue;
                }
                Values::Copy(source)
            }
            Kind::Formula(formula) => Values::Formula(
                formula.clone(),
                FormulaInput::over(parent, step.scope_definition(), sources.all(&step.scope)),
            ),
        };
        let column = new_column(step.column_type.clone());
        column.fill(&rows, &values.read(&rows)?)?;
        let source: Arc<dyn ColumnSource> = column.clone();
        if let Kind::Source(named) = &step.kind {
            shared.made(&named.name, &source);
        }
        sources.push(source);
        stored.push(Stored {
            column,
            values,
            parent_columns: step.parent_columns.iter().copied().collect(),
        });
    }
    Ok((sources.all(&plan.published), stored))
}

/// The operator of a refreshing `select` or `update`. For every stored column, in list
/// order so that a column reads the new values of the columns before it: forget the removed
/// rows, move the shifted ones, recompute the modified rows if a column this one depends on
/// was modified, and compute the added rows.
///
/// Behavior: io.deephaven.engine.table.impl.SelectOrUpdateListener#onUpdate and
/// io.deephaven.engine.table.impl.select.analyzers.SelectColumnLayer#doApplyUpdate. Java
/// leaves the old values of removed rows of primitive columns in place; here they are
/// released, which no reader can see.
fn keeps_up_to_date(
    plan: &Plan,
    stored: Vec<Stored>,
) -> impl Fn(&QueryTable, &TableUpdate) -> Result<TableUpdate, TableFailure> + Send + Sync + 'static
{
    let transformer = plan.transformer();
    move |result, upstream| {
        let survivors = result.row_set().minus(&upstream.removed);
        for stored in &stored {
            stored.column.remove(&upstream.removed);
            stored.column.apply_shifts(&upstream.shifted, &survivors)?;
            let recompute = !upstream.modified.is_empty()
                && upstream
                    .modified_columns
                    .contains_any(&stored.parent_columns);
            let changed = if recompute {
                upstream.added.union(&upstream.modified)
            } else {
                upstream.added.clone()
            };
            if !changed.is_empty() {
                stored
                    .column
                    .fill(&changed, &stored.values.read(&changed)?)?;
            }
        }
        Ok(TableUpdate {
            modified_columns: transformer.transform(&upstream.modified_columns),
            ..upstream.clone()
        })
    }
}

/// Whether a sparse column keyed by `rows` would take more than 1.1 times the blocks of
/// 1024 values that a dense column of as many rows takes.
///
/// Behavior: io.deephaven.engine.table.impl.sources.sparse.SparseConstants#sparseStructureExceedsOverhead
/// with the default of `QueryTable.maximumStaticSelectMemoryOverhead`
pub(super) fn sparse_storage_wasteful(rows: &RowSet) -> bool {
    const LOG_BLOCK_SIZE: u32 = 10;
    const MAXIMUM_OVERHEAD: f64 = 1.1;
    let required = rows.size().div_ceil(1 << LOG_BLOCK_SIZE);
    let acceptable = (MAXIMUM_OVERHEAD * required as f64) as i64;
    let mut last_block = -1;
    let mut used = 0;
    for range in rows.ranges() {
        let mut first_block = range.first() >> LOG_BLOCK_SIZE;
        let end_block = range.last() >> LOG_BLOCK_SIZE;
        if last_block >= 0 && first_block == last_block {
            if first_block == end_block {
                continue;
            }
            first_block += 1;
        }
        last_block = end_block;
        used += end_block - first_block + 1;
        if used > acceptable {
            return true;
        }
    }
    false
}

/// How a static result addresses its rows.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Keys {
    /// By the parent's row keys.
    Parents,
    /// By position, because the parent was flattened first: every step sees a flat table.
    ///
    /// Behavior: io.deephaven.engine.table.impl.QueryTable#selectInternal
    Flattened,
    /// By position, because no column of the result is shared with the parent: a step that
    /// reads only parent columns sees the parent's keys, a step that reads an earlier
    /// step's column sees positions.
    ///
    /// Behavior: `flatResult` and `useResultKeySpace` of
    /// io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext
    Renumbered,
}

/// The result of `select` or `update` on a static table.
///
/// Behavior: the `SELECT_STATIC` and `SELECT_REDIRECTED_STATIC` modes of
/// io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext, and
/// the row set and flatness io.deephaven.engine.table.impl.QueryTable#selectOrUpdate gives
/// a static result. The redirected mode differs in storage only.
fn static_result(
    parent: &Arc<dyn Table>,
    plan: &Plan,
    flavor: Flavor,
) -> Result<TableSpec, TableFailure> {
    let parent_rows = parent.row_set();
    let size = parent_rows.size();
    let flat_rows = RowSet::flat(size);
    let parent_flat = parent.is_flat();
    let wasteful = sparse_storage_wasteful(&parent_rows);
    let keeps_no_parent_column =
        parent.definition().columns().is_empty() || !flavor.keeps_parent_columns();
    let keys = if flavor == Flavor::Select && !parent_flat && wasteful {
        Keys::Flattened
    } else if !parent_flat
        && keeps_no_parent_column
        && !(flavor == Flavor::Update && wasteful)
        && plan.may_flatten
    {
        Keys::Renumbered
    } else {
        Keys::Parents
    };
    let result_rows = if keys == Keys::Parents {
        (*parent_rows).clone()
    } else {
        flat_rows.clone()
    };
    // A result column is a plain array when its keys are positions.
    let dense = parent_flat || keys != Keys::Parents;
    let store = |step: &Step, values: ArrayRef| -> Result<Arc<dyn ColumnSource>, TableFailure> {
        if dense {
            Ok(Arc::new(FlatSource::new(step.column_type.clone(), values)?))
        } else {
            let column = WritableSource::new(step.column_type.clone());
            column.fill(&parent_rows, &values)?;
            Ok(Arc::new(column))
        }
    };
    let by_position: Arc<dyn RowRedirection> = Arc::new(ByPosition(parent_rows.clone()));
    let at_position = |column: Arc<dyn ColumnSource>| -> Arc<dyn ColumnSource> {
        if *parent_rows == flat_rows {
            column
        } else {
            Arc::new(RedirectedSource::new(by_position.clone(), column))
        }
    };

    let mut sources = Sources::new(parent);
    let mut shared = Shared { copies: Vec::new() };
    for step in &plan.steps {
        shared.begin(step);
        // Whether the step reads its inputs by the result's keys or by the parent's.
        let by_result_keys = match keys {
            Keys::Parents => false,
            Keys::Flattened => true,
            Keys::Renumbered => step.reads_steps,
        };
        let scope = if by_result_keys {
            sources.all_with(&step.scope, at_position)
        } else {
            sources.all(&step.scope)
        };
        let rows = if by_result_keys {
            &result_rows
        } else {
            &*parent_rows
        };
        let source = match &step.kind {
            Kind::Constant(formula) => {
                ConstantSource::evaluate(formula, step.scope_definition(), scope)?
            }
            Kind::Source(named) => {
                let source = sources.of(named.origin);
                // The columns of a flattened parent are read through its positions and so
                // do not hold their values.
                let holds = holds_values(&source)
                    && !(keys == Keys::Flattened && matches!(named.origin, Origin::Parent(_)));
                match shared.existing(&named.name, &source, holds) {
                    Some(existing) => existing,
                    None => {
                        let read = match named.origin {
                            Origin::Parent(_) if by_result_keys => at_position(source),
                            _ => source,
                        };
                        let copy = store(step, read.chunk(rows)?)?;
                        shared.made(&named.name, &copy);
                        copy
                    }
                }
            }
            Kind::Formula(formula) => {
                let input = if by_result_keys {
                    if keys == Keys::Renumbered && formula.uses_row_variables() {
                        unported!(
                            "io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext",
                            "i, ii or k in a formula of a static select that reads an earlier column of the same call while the result gets new row keys: {}",
                            step.name
                        );
                    }
                    FormulaInput::fixed(result_rows.clone(), step.scope_definition(), scope)
                } else {
                    FormulaInput::over(parent, step.scope_definition(), scope)
                };
                store(step, formula.evaluate(&input, rows, false)?)?
            }
        };
        sources.push(source);
    }

    // Behavior: QueryTable#selectOrUpdate marks a static result flat when its row set is.
    let result = TableSpec::new(
        plan.definition(),
        result_rows.clone(),
        sources.all(&plan.published),
    );
    Ok(if result_rows == flat_rows {
        result.flat()
    } else {
        result
    })
}
