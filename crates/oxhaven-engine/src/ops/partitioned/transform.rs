//! `transform` and `partitioned_transform`: a partitioned table whose constituents are what
//! a function makes of the constituents of another, or of the pairs of constituents of two
//! that have the same key.
//!
//! The result's underlying table has the rows and the other columns of the table it was
//! made from; only the constituent column is its own. The function is called once for
//! each row when the result is made and then, inside the update cycle, for each row that
//! is added and for each row whose constituent changed. A row that is removed gives up
//! its table. What the function answers is checked as Java checks it: it is a table, and
//! it has the columns the function gave a constituent without rows.

use super::cells::{TableCells, TableColumn};
use super::{failure, tables_of, PartitionedError, PartitionedTable};
use crate::ops::filter::filtered_update;
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use crate::table::{QueryTable, TableSpec};
use arrow::array::{Array, ArrayRef, Int64Array};
use oxhaven_column::{Clock, WritableSource};
use oxhaven_contracts::graph::{locked, Dependency, UpdateGraph};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::{Arc, Mutex, PoisonError};

/// What a transform applies: given one constituent, or one of each of two partitioned
/// tables, it answers a table, or `None` where Java's function answers `null`. It is
/// called under the update graph's lock or inside an update cycle, and may make tables
/// of the tables it is given.
pub type Transformer =
    Arc<dyn Fn(&[Arc<dyn Table>]) -> Result<Option<Arc<dyn Table>>, TableFailure> + Send + Sync>;

/// The column that holds the other partitioned table's constituents while a
/// `partitioned_transform` is made.
///
/// Behavior: PartitionedTableImpl's `RHS_CONSTITUENT`
const RHS_CONSTITUENT: &str = "__RHS_CONSTITUENT__";

const REFRESHING_IN_STATIC: &str =
    "Static partitioned tables cannot contain refreshing constituents. Did you mean to specify \
     expectRefreshingResults=true for this transform?";

/// The cells of the result's constituent column, and what fills them.
struct Cells {
    /// The columns of the source table that hold the function's arguments.
    inputs: Vec<Arc<dyn ColumnSource>>,
    transformer: Transformer,
    /// Whether the result is refreshing, and so may hold refreshing tables.
    refreshing: bool,
    handles: Arc<WritableSource>,
    tables: Arc<TableCells>,
    /// The handles of cells that were removed or replaced on the last update. A listener
    /// of the result may still read them as previous values during that cycle, so they
    /// are given up on the next one.
    retired: Mutex<Vec<i64>>,
}

impl Cells {
    /// What the function makes of the constituents at `rows` of the source table, in
    /// their order.
    ///
    /// Behavior: io.deephaven.engine.table.impl.partitioned.TableTransformationColumn and
    /// BiTableTransformationColumn, `transformAndFill`; and
    /// PartitionedTableImpl#assertResultsStatic for a result that is not refreshing
    fn compute(&self, rows: &RowSet) -> Result<Vec<Option<Arc<dyn Table>>>, PartitionedError> {
        let arguments = self
            .inputs
            .iter()
            .map(|input| tables_of(&input.chunk(rows)?))
            .collect::<Result<Vec<_>, TableFailure>>()?;
        (0..rows.size() as usize)
            .map(|row| {
                let tables = arguments
                    .iter()
                    .map(|column| column[row].clone())
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| failure("a partitioned table has a null constituent"))?;
                let made = (self.transformer)(&tables)?;
                if !self.refreshing && made.as_ref().is_some_and(|table| table.is_refreshing()) {
                    return Err(PartitionedError::IllegalState(
                        REFRESHING_IN_STATIC.to_string(),
                    ));
                }
                Ok(made)
            })
            .collect()
    }

    /// Checks `made` against `expected`, the definition of the constituents, and writes
    /// the tables at `rows`.
    ///
    /// Behavior: PartitionedTableImpl#validateConstituents, as `ValidateConstituents`
    /// calls it for the rows a partitioned table starts with and for the rows that are
    /// added or modified in a cycle
    fn store(
        &self,
        rows: &RowSet,
        made: Vec<Option<Arc<dyn Table>>>,
        expected: &TableDefinition,
    ) -> Result<(), PartitionedError> {
        let mut tables = Vec::with_capacity(made.len());
        for table in made {
            let Some(table) = table else {
                return Err(PartitionedError::IllegalState(
                    "Encountered null constituent".to_string(),
                ));
            };
            if !self.refreshing && table.is_refreshing() {
                return Err(PartitionedError::IllegalState(
                    "Constituents may not be refreshing if the underlying table is static"
                        .to_string(),
                ));
            }
            check_mutual_compatibility(expected, table.definition())?;
            tables.push(table);
        }
        let handles: Int64Array = tables
            .into_iter()
            .map(|table| Some(self.tables.register(table)))
            .collect();
        let handles: ArrayRef = Arc::new(handles);
        Ok(self.handles.fill(rows, &handles)?)
    }

    /// Notes that the cells at `rows` are about to be removed or replaced.
    fn retire(&self, rows: &RowSet) -> Result<(), TableFailure> {
        if rows.is_empty() {
            return Ok(());
        }
        let handles = self.handles.chunk(rows)?;
        let handles = arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(&handles);
        self.retired
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(handles.iter().flatten());
        Ok(())
    }

    fn release_retired(&self) {
        let retired =
            std::mem::take(&mut *self.retired.lock().unwrap_or_else(PoisonError::into_inner));
        for handle in retired {
            self.tables.release(handle);
        }
    }
}

/// Behavior: io.deephaven.engine.table.TableDefinition#checkMutualCompatibility with the
/// names "expected" and "constituent", and #describeCompatibilityDifferences for the text
fn check_mutual_compatibility(
    expected: &TableDefinition,
    constituent: &TableDefinition,
) -> Result<(), PartitionedError> {
    let mut differences = Vec::new();
    for column in expected.columns() {
        match constituent.index_of(&column.name) {
            None => differences.push(format!(
                "expected column '{}' is missing in constituent",
                column.name
            )),
            Some(index) => {
                let other = &constituent.columns()[index];
                if other.column_type != column.column_type {
                    differences.push(format!("column '{}' is different ...", column.name));
                    differences.push(format!(
                        "    {}: expected dataType '{:?}' does not match constituent dataType \
                         '{:?}'",
                        column.name, column.column_type, other.column_type
                    ));
                }
            }
        }
    }
    for column in constituent.columns() {
        if expected.index_of(&column.name).is_none() {
            differences.push(format!(
                "constituent column '{}' is missing in expected",
                column.name
            ));
        }
    }
    if differences.is_empty() {
        return Ok(());
    }
    Err(PartitionedError::Table(failure(format!(
        "Table definition incompatibilities: \n\t{}",
        differences.join("\n\t")
    ))))
}

/// Keeps the cells of a transform up to date with its source table.
struct Operator {
    cells: Arc<Cells>,
    expected: TableDefinition,
    /// The columns of the source table that hold the function's arguments.
    inputs: Vec<usize>,
    /// The result's column of each column of the source table.
    columns: Vec<usize>,
}

impl Operator {
    /// Behavior: the `update` of PartitionedTableImpl#transform and #partitionedTransform
    /// (io.deephaven.engine.table.impl.SelectOrUpdateListener#onUpdate for a column that
    /// reads the constituent columns), then `ValidateConstituents`' listener
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure> {
        let cells = &self.cells;
        cells.release_retired();
        let before = result.row_set();
        cells.retire(&upstream.removed)?;
        cells.handles.remove(&upstream.removed);
        cells
            .handles
            .apply_shifts(&upstream.shifted, &before.minus(&upstream.removed))?;
        // A modified row is made again only when a constituent it is made of changed.
        let again =
            !upstream.modified.is_empty() && upstream.modified_columns.contains_any(&self.inputs);
        let rows = if again {
            cells.retire(&upstream.modified)?;
            upstream.added.union(&upstream.modified)
        } else {
            upstream.added.clone()
        };
        if !rows.is_empty() {
            let made = cells.compute(&rows).map_err(in_cycle)?;
            cells.store(&rows, made, &self.expected).map_err(in_cycle)?;
        }
        let modified_columns = match &upstream.modified_columns {
            ModifiedColumnSet::All => ModifiedColumnSet::All,
            ModifiedColumnSet::Columns(columns) => {
                ModifiedColumnSet::of(columns.iter().map(|&column| self.columns[column]))
            }
        };
        Ok(TableUpdate {
            added: upstream.added.clone(),
            removed: upstream.removed.clone(),
            modified: upstream.modified.clone(),
            shifted: upstream.shifted.clone(),
            modified_columns,
        })
    }
}

fn in_cycle(error: PartitionedError) -> TableFailure {
    match error {
        PartitionedError::Table(failure) => failure,
        other => failure(other.to_string()),
    }
}

/// When a transform of a table that is not refreshing has finished for a step: when the
/// tables its function reads have. Nothing notifies its result.
struct After(Vec<Arc<dyn Dependency>>);

impl Dependency for After {
    fn satisfied(&self, step: u64) -> bool {
        self.0.iter().all(|dependency| dependency.satisfied(step))
    }
}

/// The table of a transform of `source`: its rows and columns, without the column
/// `dropped`, and with what `transformer` makes of the tables in the columns `inputs` in
/// the place of the first of them. `empties` are tables without rows that have the
/// columns of the tables in `inputs`. Also answers the definition of the constituents.
///
/// The result is refreshing when `source` is or there is a dependency, which is a
/// refreshing table that `transformer` reads: it is not called in a cycle before each of
/// them has finished.
///
/// Behavior: PartitionedTableImpl#transform from its `prepareForTransform` to the
/// `ValidateConstituents` of the constructor it calls; and #prepareForTransform, where a
/// static table is made refreshing when there are dependencies
fn transform_table(
    source: &Arc<dyn Table>,
    inputs: &[usize],
    dropped: Option<usize>,
    empties: &[Arc<dyn Table>],
    transformer: Transformer,
    dependencies: &[Arc<dyn Table>],
) -> Result<(Arc<dyn Table>, TableDefinition), PartitionedError> {
    let dependencies: Vec<Arc<dyn Table>> = dependencies
        .iter()
        .filter(|table| table.is_refreshing())
        .cloned()
        .collect();
    let graph: Option<Arc<dyn UpdateGraph>> = std::iter::once(source)
        .chain(&dependencies)
        .filter(|table| table.is_refreshing())
        .find_map(|table| table.update_graph());
    let work = || {
        transform_locked(
            source,
            inputs,
            dropped,
            empties,
            transformer,
            dependencies.clone(),
            graph.clone(),
        )
    };
    match &graph {
        Some(graph) => locked(&**graph, work),
        None => work(),
    }
}

fn transform_locked(
    source: &Arc<dyn Table>,
    inputs: &[usize],
    dropped: Option<usize>,
    empties: &[Arc<dyn Table>],
    transformer: Transformer,
    dependencies: Vec<Arc<dyn Table>>,
    graph: Option<Arc<dyn UpdateGraph>>,
) -> Result<(Arc<dyn Table>, TableDefinition), PartitionedError> {
    if source.is_failed() {
        return Err(failure("the parent table has failed").into());
    }
    let column_type = ColumnType::Table(TableClass::Table);
    let handles = Arc::new(WritableSource::new(column_type.clone()));
    if let Some(graph) = &graph {
        let clock: Arc<dyn Clock> = Arc::new(graph.clone());
        handles.start_tracking_prev(clock);
    }
    let cells = Arc::new(Cells {
        inputs: inputs.iter().map(|&index| source.column(index)).collect(),
        transformer,
        refreshing: graph.is_some(),
        handles,
        tables: Arc::default(),
        retired: Mutex::default(),
    });

    let rows = source.row_set();
    let made = cells.compute(&rows)?;
    let empty = (cells.transformer)(empties)?.ok_or_else(|| {
        PartitionedError::NullPointer(
            "Cannot invoke \"io.deephaven.engine.table.Table.getDefinition()\" because \
             \"resultEmptyConstituent\" is null"
                .to_string(),
        )
    })?;
    let expected = empty.definition().clone();
    cells.store(&rows, made, &expected)?;

    let output = inputs[0];
    let constituents: Arc<dyn ColumnSource> = Arc::new(TableColumn::new(
        cells.handles.clone(),
        cells.tables.clone(),
    ));
    let mut definitions = Vec::new();
    let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::new();
    // The result's column of each column of the source: the dropped one is an argument
    // of the function, so a change of it is a change of the constituent column.
    let mut columns = vec![0; source.definition().columns().len()];
    for (index, column) in source.definition().columns().iter().enumerate() {
        if Some(index) == dropped {
            continue;
        }
        columns[index] = definitions.len();
        if index == output {
            definitions.push(ColumnDefinition::new(
                column.name.as_str(),
                column_type.clone(),
            ));
            sources.push(constituents.clone());
        } else {
            definitions.push(column.clone());
            sources.push(source.column(index));
        }
    }
    if let Some(dropped) = dropped {
        columns[dropped] = columns[output];
    }
    let definition = TableDefinition::new(definitions)
        .map_err(|duplicate| PartitionedError::IllegalArgument(duplicate.to_string()))?;
    let mut spec = TableSpec::new(definition, (*rows).clone(), sources);
    // Behavior: io.deephaven.engine.table.impl.QueryTable#selectOrUpdate, where a result
    // that is not refreshing is flat when its row set is, and a refreshing one when its
    // parent is (#propagateFlatness)
    let flat = match graph {
        None => *rows == RowSet::flat(rows.size()),
        Some(_) => source.is_flat(),
    };
    if flat {
        spec = spec.flat();
    }

    let table: Arc<dyn Table> = match graph {
        None => QueryTable::new_static(spec),
        Some(graph) if !source.is_refreshing() => {
            let waits = dependencies
                .into_iter()
                .map(|table| table as Arc<dyn Dependency>)
                .collect();
            QueryTable::dependent(graph, spec, Arc::new(After(waits)))
        }
        Some(_) => {
            let operator = Operator {
                cells,
                expected: expected.clone(),
                inputs: inputs.to_vec(),
                columns,
            };
            let waits = dependencies
                .into_iter()
                .map(|table| table as Arc<dyn Dependency>)
                .collect();
            QueryTable::derive_merged(std::slice::from_ref(source), waits, || {
                Ok((
                    spec,
                    move |result: &QueryTable, upstream: &[Option<Arc<TableUpdate>>]| {
                        match &upstream[0] {
                            Some(update) => operator.on_update(result, update),
                            None => Ok(TableUpdate::empty()),
                        }
                    },
                ))
            })?
        }
    };
    Ok((table, expected))
}

/// The rows of `table` whose cell of the column `column`, a column of tables, is not null.
///
/// Behavior: the `where` with an inverted match of `null` of
/// PartitionedTableImpl#partitionedTransform
fn with_table_in(table: &Arc<dyn Table>, column: usize) -> Result<Arc<dyn Table>, TableFailure> {
    let cells = table.column(column);
    let accept = move |rows: &RowSet| -> Result<RowSet, TableFailure> {
        let values = cells.chunk(rows)?;
        Ok(RowSet::from_keys(
            rows.iter()
                .enumerate()
                .filter(|(position, _)| values.is_valid(*position))
                .map(|(_, key)| key),
        ))
    };
    let result = QueryTable::derive(table, || {
        let definition = table.definition().clone();
        let columns = (0..definition.columns().len())
            .map(|index| table.column(index))
            .collect();
        let spec = TableSpec::new(definition, accept(&table.row_set())?, columns);
        let operator = move |result: &QueryTable, upstream: &TableUpdate| {
            let again = upstream.modified_columns.contains(column);
            filtered_update(result, upstream, again, &accept)
        };
        Ok((spec, operator))
    })?;
    Ok(result)
}

/// Java's text for the type of a key column.
///
/// Behavior: io.deephaven.engine.table.ColumnDefinition#describeForCompatibility
fn describe(column: &ColumnDefinition) -> String {
    let type_name = match &column.column_type {
        ColumnType::Boolean => "class java.lang.Boolean".to_string(),
        ColumnType::Byte => "byte".to_string(),
        ColumnType::Char => "char".to_string(),
        ColumnType::Short => "short".to_string(),
        ColumnType::Int => "int".to_string(),
        ColumnType::Long => "long".to_string(),
        ColumnType::Float => "float".to_string(),
        ColumnType::Double => "double".to_string(),
        ColumnType::String => "class java.lang.String".to_string(),
        ColumnType::Instant => "class java.time.Instant".to_string(),
        other => format!("{other:?}"),
    };
    format!("[{}, {type_name}]", column.name)
}

/// The key columns of `left` and `right` in pairs, or Java's refusal.
///
/// Behavior: PartitionedTableImpl#matchKeyColumns
pub(super) fn match_key_columns(
    left: &PartitionedTable,
    right: &PartitionedTable,
) -> Result<Vec<JoinMatch>, PartitionedError> {
    let (left_keys, right_keys) = (left.key_columns(), right.key_columns());
    if left_keys.len() != right_keys.len() {
        return Err(PartitionedError::IllegalArgument(format!(
            "Incompatible partitioned table input for partitioned transform; key column sets \
             don't contain the same names or the same number of columns: first has [{}], \
             second has [{}]",
            left_keys.join(", "),
            right_keys.join(", ")
        )));
    }
    // The same names in any order are matched by name, others by position.
    let same_names = left_keys.iter().all(|name| right_keys.contains(name));
    let pairs: Vec<JoinMatch> = left_keys
        .iter()
        .zip(right_keys)
        .map(|(left, right)| match same_names {
            true => JoinMatch::same(left.as_str()),
            false => JoinMatch::new(left.as_str(), right.as_str()),
        })
        .collect();
    let column = |partitioned: &PartitionedTable, name: &str| {
        let definition = partitioned.table().definition();
        let index = definition.index_of(name).expect("a key column");
        definition.columns()[index].clone()
    };
    let mismatches: Vec<String> = pairs
        .iter()
        .map(|pair| (column(left, &pair.left), column(right, &pair.right)))
        .filter(|(left, right)| left.column_type != right.column_type)
        .map(|(left, right)| format!("{} doesn't match {}", describe(&left), describe(&right)))
        .collect();
    if !mismatches.is_empty() {
        return Err(PartitionedError::IllegalArgument(format!(
            "Incompatible partitioned table input for partitioned transform; key column \
             definitions don't match: {}",
            mismatches.join(", ")
        )));
    }
    Ok(pairs)
}

impl PartitionedTable {
    fn constituent_index(&self) -> usize {
        self.table
            .definition()
            .index_of(&self.constituent_column)
            .expect("checked when the partitioned table was made")
    }

    /// A partitioned table whose constituents are what `transformer` makes of the
    /// constituents of this one: it is called with one table, for every constituent now
    /// and, when this partitioned table is refreshing, in the update cycle for every
    /// constituent that appears. The other columns, the rows and their keys are those of
    /// this partitioned table.
    ///
    /// `dependencies` are the refreshing tables that `transformer` reads besides the
    /// constituent; it is called in a cycle after they have finished. With a dependency
    /// the result of a partitioned table that is not refreshing is refreshing, and so
    /// may its constituents be; without one they may not.
    ///
    /// Behavior: PartitionedTableImpl#transform, as `deephaven.table.PartitionedTable.
    /// transform` calls it: `expectRefreshingResults` is whether the underlying table is
    /// refreshing, and only refreshing tables are dependencies
    pub fn transform(
        &self,
        transformer: Transformer,
        dependencies: &[Arc<dyn Table>],
    ) -> Result<PartitionedTable, PartitionedError> {
        let (table, definition) = transform_table(
            &self.table,
            &[self.constituent_index()],
            None,
            &[self.empty_constituent()?],
            transformer,
            dependencies,
        )?;
        PartitionedTable::new(
            table,
            self.key_columns.clone(),
            self.unique_keys,
            self.constituent_column.clone(),
            definition,
            self.constituent_changes_permitted,
        )
    }

    /// A partitioned table with a row for each row of this one whose key `other` has
    /// too, and whose constituents are what `transformer` makes of the two constituents
    /// of that key: it is called with this one's and then `other`'s, for every such pair
    /// now and, when either partitioned table is refreshing, in the update cycle for
    /// every pair that appears. The other columns are those of this partitioned table.
    /// `dependencies` are as in [`PartitionedTable::transform`].
    ///
    /// The key columns are matched by name when both have the same names, by position
    /// otherwise, and must have the same types.
    ///
    /// Behavior: PartitionedTableImpl#partitionedTransform for a partitioned table with
    /// unique keys: a natural join of the two underlying tables, less the rows that found
    /// no constituent
    pub fn partitioned_transform(
        &self,
        other: &PartitionedTable,
        transformer: Transformer,
        dependencies: &[Arc<dyn Table>],
    ) -> Result<PartitionedTable, PartitionedError> {
        let on = match_key_columns(self, other)?;
        if !self.unique_keys {
            return Err(PartitionedError::Unsupported(
                "unported: io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl#\
                 partitionedTransform: a partitioned table whose keys are not unique"
                    .to_string(),
            ));
        }
        let add = [JoinAddition::renamed(
            RHS_CONSTITUENT,
            other.constituent_column(),
        )];
        let joined = natural_join(
            &self.table,
            other.table(),
            &on,
            &add,
            NaturalJoinType::ErrorOnDuplicate,
        )
        .map_err(|error| PartitionedError::IllegalArgument(error.to_string()))?;
        let rhs = joined
            .definition()
            .index_of(RHS_CONSTITUENT)
            .expect("the join added it");
        let matched = with_table_in(&joined, rhs)?;
        let (table, definition) = transform_table(
            &matched,
            &[self.constituent_index(), rhs],
            Some(rhs),
            &[self.empty_constituent()?, other.empty_constituent()?],
            transformer,
            dependencies,
        )?;
        PartitionedTable::new(
            table,
            self.key_columns.clone(),
            self.unique_keys,
            self.constituent_column.clone(),
            definition,
            self.constituent_changes_permitted || other.constituent_changes_permitted,
        )
    }
}
