//! Partitioned tables made from a table that has a column of tables, or from tables.
//!
//! Behavior: io.deephaven.engine.table.impl.partitioned.PartitionedTableCreatorImpl, the
//! engine's `PartitionedTableFactory`

use super::cells::{TableCells, TableColumn};
use super::transform::{check_mutual_compatibility, After};
use super::{failure, tables_of, PartitionedError, PartitionedTable, CONSTITUENT};
use crate::table::{QueryTable, TableSpec};
use arrow::array::{ArrayRef, Int64Array};
use oxhaven_column::FlatSource;
use oxhaven_contracts::graph::{locked, Dependency};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableClass, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::Arc;

/// How Java's messages name a table: its description, which is `QueryTable` for most
/// tables. Java's descriptions of other tables (`FilteredTable(null)` for a `where`) are
/// not kept here.
const DESCRIPTION: &str = "QueryTable";

/// The rows Java checks of the rows it is given: one chunk.
///
/// Behavior: PartitionedTableImpl#validateConstituents, which reads one chunk of
/// `ChunkedColumnIterator#DEFAULT_CHUNK_SIZE` rows of the rows to validate
const CHECKED_ROWS: u64 = 1 << 11;

/// The names of the columns of `definition`, as Java's `getColumnNamesAsString`.
fn names(definition: &TableDefinition) -> Vec<&str> {
    definition
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

/// Checks the constituents in the cells of `column` at `rows`, at most the first
/// [`CHECKED_ROWS`] of them, as Java does: none is null, none is refreshing unless the
/// underlying table is, and each has the columns of `expected`.
///
/// Behavior: PartitionedTableImpl#validateConstituents
fn check_constituents(
    column: &Arc<dyn ColumnSource>,
    rows: &RowSet,
    expected: &TableDefinition,
    refreshing: bool,
) -> Result<(), PartitionedError> {
    if rows.is_empty() {
        return Ok(());
    }
    let first = rows.sub_set_by_position_range(0, CHECKED_ROWS);
    for table in tables_of(&column.chunk(&first)?)? {
        let Some(table) = table else {
            return Err(PartitionedError::IllegalState(
                "Encountered null constituent".to_string(),
            ));
        };
        if !refreshing && table.is_refreshing() {
            return Err(PartitionedError::IllegalState(
                "Constituents may not be refreshing if the underlying table is static".to_string(),
            ));
        }
        check_mutual_compatibility(expected, table.definition())?;
    }
    Ok(())
}

/// `table` with its constituents checked now and, for a refreshing table, in every cycle
/// for the rows added and modified; the table answered fails in the cycle in which a
/// check does.
///
/// Behavior: PartitionedTableImpl's `ValidateConstituents`
fn validated(
    table: &Arc<dyn Table>,
    constituent_column: usize,
    expected: &TableDefinition,
) -> Result<Arc<dyn Table>, PartitionedError> {
    let column = table.column(constituent_column);
    let refreshing = table.is_refreshing();
    let check = |rows: &RowSet| check_constituents(&column, rows, expected, refreshing);
    if !refreshing {
        check(&table.row_set())?;
        return Ok(table.clone());
    }
    let graph = table
        .update_graph()
        .expect("a refreshing table has a graph");
    locked(&*graph, || {
        check(&table.row_set())?;
        let column = column.clone();
        let expected = expected.clone();
        let result = QueryTable::derive(table, || {
            let definition = table.definition().clone();
            let columns = (0..definition.columns().len())
                .map(|index| table.column(index))
                .collect();
            let mut spec = TableSpec::new(definition, (*table.row_set()).clone(), columns);
            if table.is_flat() {
                spec = spec.flat();
            }
            let operator = move |_: &QueryTable, upstream: &TableUpdate| {
                for rows in [&upstream.modified, &upstream.added] {
                    check_constituents(&column, rows, &expected, true).map_err(in_cycle)?;
                }
                Ok(upstream.clone())
            };
            Ok((spec, operator))
        })?;
        Ok(result as Arc<dyn Table>)
    })
}

fn in_cycle(error: PartitionedError) -> TableFailure {
    match error {
        PartitionedError::Table(failure) => failure,
        other => failure(other.to_string()),
    }
}

impl PartitionedTable {
    /// The partitioned table of `table`, whose column `constituent_column` holds tables
    /// with the columns of `constituent_definition`, and whose columns `key_columns` say
    /// which. The constituents are checked now and, for a refreshing table, as rows are
    /// added and modified. Constituents can change only when `table` refreshes.
    ///
    /// Behavior: io.deephaven.engine.table.PartitionedTableFactory#of(Table, Collection,
    /// boolean, String, TableDefinition, boolean), PartitionedTableCreatorImpl#of
    pub fn of(
        table: &Arc<dyn Table>,
        key_columns: Vec<String>,
        unique_keys: bool,
        constituent_column: &str,
        constituent_definition: TableDefinition,
        constituent_changes_permitted: bool,
    ) -> Result<PartitionedTable, PartitionedError> {
        let definition = table.definition();
        if key_columns
            .iter()
            .any(|name| definition.index_of(name).is_none())
        {
            return Err(PartitionedError::IllegalArgument(format!(
                "Partitioned table {DESCRIPTION} does not have all key columns in [{}]; \
                 instead has {}",
                key_columns.join(", "),
                names(definition).join(",")
            )));
        }
        let Some(index) = definition.index_of(constituent_column) else {
            return Err(PartitionedError::IllegalArgument(format!(
                "Partitioned table {DESCRIPTION} has no column named {constituent_column}"
            )));
        };
        let column_type = &definition.columns()[index].column_type;
        if !matches!(column_type, ColumnType::Table(_)) {
            return Err(PartitionedError::IllegalArgument(format!(
                "Constituent column {constituent_column} has unsupported data type {}",
                java_class(column_type)
            )));
        }
        let checked = validated(table, index, &constituent_definition)?;
        // Key columns named twice count once, as in Java's `LinkedHashSet` of them.
        let mut keys: Vec<String> = Vec::new();
        for key in key_columns {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        PartitionedTable::new(
            checked,
            keys,
            unique_keys,
            constituent_column.to_string(),
            constituent_definition,
            constituent_changes_permitted && table.is_refreshing(),
        )
    }

    /// The partitioned table of `table`, which has one column of tables: every other
    /// column is a key column, the keys are not known to be unique, and the constituents
    /// have the columns of the first one.
    ///
    /// Behavior: io.deephaven.engine.table.PartitionedTableFactory#of(Table),
    /// PartitionedTableCreatorImpl#internalOf
    pub fn of_table(table: &Arc<dyn Table>) -> Result<PartitionedTable, PartitionedError> {
        let definition = table.definition();
        let (tables, keys): (Vec<&ColumnDefinition>, Vec<&ColumnDefinition>) = definition
            .columns()
            .iter()
            .partition(|column| matches!(column.column_type, ColumnType::Table(_)));
        let [constituents] = tables[..] else {
            return Err(PartitionedError::IllegalArgument(format!(
                "Underlying table {DESCRIPTION} has multiple possible constituent columns: {}",
                tables
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        };
        let name = constituents.name.as_str();
        let index = definition.index_of(name).expect("a column of the table");
        let read_first = || -> Result<Option<Arc<dyn Table>>, PartitionedError> {
            let rows = table.row_set();
            if rows.is_empty() {
                return Ok(None);
            }
            let first = rows.sub_set_by_position_range(0, 1);
            Ok(tables_of(&table.column(index).chunk(&first)?)?
                .pop()
                .flatten())
        };
        let first = match table.update_graph() {
            Some(graph) if table.is_refreshing() => locked(&*graph, read_first),
            _ => read_first(),
        }?;
        let Some(first) = first else {
            return Err(PartitionedError::IllegalArgument(format!(
                "Underlying table {DESCRIPTION} has no constituent tables in column: {name}"
            )));
        };
        let key_columns = keys.iter().map(|column| column.name.clone()).collect();
        let checked = validated(table, index, first.definition())?;
        PartitionedTable::new(
            checked,
            key_columns,
            false,
            name.to_string(),
            first.definition().clone(),
            table.is_refreshing(),
        )
    }

    /// The partitioned table of `tables`, without the `None`s: no key columns, one row
    /// for each table in the column `__CONSTITUENT__`, constituents that cannot change,
    /// and the columns of `definition` or of the first table. When a table refreshes,
    /// the underlying table is refreshing, though its rows never change.
    ///
    /// Behavior: io.deephaven.engine.table.PartitionedTableFactory#ofTables,
    /// PartitionedTableCreatorImpl#constituentsToPartitionedTable
    pub fn of_tables(
        tables: &[Option<Arc<dyn Table>>],
        definition: Option<TableDefinition>,
    ) -> Result<PartitionedTable, PartitionedError> {
        let given: Vec<Arc<dyn Table>> = tables.iter().flatten().cloned().collect();
        let Some(first) = given.first() else {
            return Err(PartitionedError::IllegalArgument(
                "No non-null constituents provided".to_string(),
            ));
        };
        // Java asks the first table given, null or not, for the update graph.
        if tables[0].is_none() {
            return Err(PartitionedError::NullPointer(
                "Cannot invoke \"io.deephaven.engine.table.Table.getUpdateGraph(\
                 io.deephaven.engine.updategraph.NotificationQueue$Dependency[])\" because \
                 \"constituents[0]\" is null"
                    .to_string(),
            ));
        }
        let definition = definition.unwrap_or_else(|| first.definition().clone());
        let column_type = ColumnType::Table(TableClass::Table);
        let cells = Arc::new(TableCells::default());
        let handles: Int64Array = given
            .iter()
            .map(|table| Some(cells.register(table.clone())))
            .collect();
        let handles: ArrayRef = Arc::new(handles);
        let source = Arc::new(FlatSource::new(column_type.clone(), handles)?);
        let column: Arc<dyn ColumnSource> = Arc::new(TableColumn::new(source, cells));
        let spec = TableSpec::new(
            TableDefinition::new(vec![ColumnDefinition::new(CONSTITUENT, column_type)])
                .expect("one column"),
            RowSet::flat(given.len() as u64),
            vec![column],
        )
        .flat();
        // Behavior: BaseTable#addParentReference, which makes the table refreshing when
        // a parent is
        let refreshing: Vec<Arc<dyn Dependency>> = given
            .iter()
            .filter(|table| table.is_refreshing())
            .map(|table| table.clone() as Arc<dyn Dependency>)
            .collect();
        let graph = given
            .iter()
            .filter(|table| table.is_refreshing())
            .find_map(|table| table.update_graph());
        let table: Arc<dyn Table> = match graph {
            Some(graph) => QueryTable::dependent(graph, spec, Arc::new(After(refreshing))),
            None => QueryTable::new_static(spec),
        };
        let checked = validated(&table, 0, &definition)?;
        PartitionedTable::new(
            checked,
            Vec::new(),
            false,
            CONSTITUENT.to_string(),
            definition,
            false,
        )
    }
}

/// Java's text for the class of a column's values, as `Class#toString` writes it.
fn java_class(column_type: &ColumnType) -> String {
    super::transform::java_type_name(column_type)
}
