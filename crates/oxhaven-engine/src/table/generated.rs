//! The function generated table: a table whose rows a function makes anew, when one of
//! some tables changes or when an interval has passed.

use super::TimeSource;
use super::{failure, tracked_source, MergedOperator, QueryTable, RootSource, TableSpec};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{locked, UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::{Arc, Mutex, PoisonError, Weak};

#[cfg(test)]
mod tests;

/// The function of a function generated table: it returns the table whose columns and
/// rows the generated table then has. It is called once when the generated table is made,
/// by the thread that makes it, and then by the update cycle.
pub type TableGenerator = Arc<dyn Fn() -> Result<Arc<dyn Table>, TableFailure> + Send + Sync>;

const NOT_SATISFIED: &str =
    "java.lang.IllegalStateException: The function-generated table is not satisfied; did you \
     miss a dependency when specifying source tables?";
const ANOTHER_GRAPH: &str =
    "java.lang.IllegalStateException: Function-generated tables must belong to the same \
     UpdateGraph as the creating FunctionGeneratedTableFactory.";

/// A table with the columns and rows of the table `generator` returns, generated again in
/// every update cycle in which one of `sources` changed, after all of them have finished.
///
/// The result has the row keys `0..size`, no attributes, and is not flat. Each time the
/// function has run, every row that was there before and still is counts as modified in
/// every column, and rows are added or removed at the end.
///
/// Without sources the function is called once and the result is static. A source that
/// is not refreshing is an error. The result fails when the function fails in a cycle,
/// or returns a table that cannot be copied ([`Generated::copy`]).
///
/// Call it without the lock of whatever `generator` needs: the function is called under
/// the update graph's shared lock here, and by the cycle later.
///
/// Behavior: io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory#create(Supplier, Table...)
/// and its `FunctionBackedTable#doRefresh`
pub fn generated_from_sources(
    generator: TableGenerator,
    sources: &[Arc<dyn Table>],
) -> Result<Arc<QueryTable>, TableFailure> {
    if sources.is_empty() {
        return generated_once(&generator);
    }
    let still = sources.iter().filter(|source| !source.is_refreshing());
    if still.count() > 0 {
        return Err(failure(
            "java.lang.IllegalArgumentException: All source tables must be refreshing",
        ));
    }
    let graph = match sources[0].update_graph() {
        Some(graph) => graph,
        None => panic!("a refreshing table has an update graph"),
    };
    QueryTable::derive_merged(sources, Vec::new(), || {
        let (generated, rows) = Generated::start(generator, Some(&graph))?;
        let spec = generated.spec(rows);
        Ok((spec, generated))
    })
}

/// A table with the columns and rows of the table `generator` returns, generated again by
/// the first update cycle of `graph` that starts `interval_millis` or more after the last
/// time, by `clock`.
///
/// With an interval of zero the result is refreshing and never generated again. With a
/// negative one the function is called once and the result is static. Otherwise as
/// [`generated_from_sources`].
///
/// A function that returns a refreshing table fails the result at the first refresh:
/// the function is called while the graph's sources refresh, when Java does not take a
/// refreshing table for satisfied.
///
/// Behavior: io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory#create(Supplier, int),
/// its `FunctionBackedTable#run` and `#updateTable`
pub fn generated_on_interval(
    generator: TableGenerator,
    graph: Arc<dyn UpdateGraph>,
    clock: Arc<dyn TimeSource>,
    interval_millis: i32,
) -> Result<Arc<QueryTable>, TableFailure> {
    if interval_millis < 0 {
        return generated_once(&generator);
    }
    let interval = i64::from(interval_millis);
    let next_refresh = millis(&*clock) + interval;
    let (generated, rows) = Generated::start(generator, Some(&graph))?;
    let spec = generated.spec(rows);
    Ok(QueryTable::root(graph, spec, |table| RootSource {
        source: Arc::new(Periodic {
            table,
            generated,
            clock,
            interval,
            next_refresh: Mutex::new(next_refresh),
        }),
        input: None,
    }))
}

/// The static table of a function that is called once.
fn generated_once(generator: &TableGenerator) -> Result<Arc<QueryTable>, TableFailure> {
    let (generated, rows) = Generated::start(generator.clone(), None)?;
    Ok(QueryTable::new_static(generated.spec(rows)))
}

/// Milliseconds since the epoch on `clock`.
fn millis(clock: &dyn TimeSource) -> i64 {
    clock.now_nanos().div_euclid(1_000_000)
}

/// The function of a generated table and the columns its rows are copied into.
struct Generated {
    generator: TableGenerator,
    definition: TableDefinition,
    columns: Vec<Arc<WritableSource>>,
    /// The graph of a refreshing result.
    graph: Option<Arc<dyn UpdateGraph>>,
}

impl Generated {
    /// Calls the function for the first time. The table it returns gives the result its
    /// columns and its first rows, whose number is returned. `graph` is the graph of a
    /// refreshing result.
    ///
    /// Behavior: the constructor of FunctionGeneratedTableFactory
    fn start(
        generator: TableGenerator,
        graph: Option<&Arc<dyn UpdateGraph>>,
    ) -> Result<(Generated, u64), TableFailure> {
        let first = generator()?;
        let other_graph = |own: &Arc<dyn UpdateGraph>| {
            let of_first = first.update_graph();
            !of_first.is_some_and(|of_first| Arc::ptr_eq(&of_first, own))
        };
        if first.is_refreshing() && graph.is_some_and(other_graph) {
            return Err(failure(ANOTHER_GRAPH));
        }
        let definition = first.definition().clone();
        let columns = definition.columns().iter();
        let columns = columns.map(|column| match graph {
            Some(graph) => tracked_source(graph, column.column_type.clone()),
            None => Arc::new(WritableSource::new(column.column_type.clone())),
        });
        let columns = columns.collect();
        let generated = Generated {
            generator,
            definition,
            columns,
            graph: graph.cloned(),
        };
        // A refreshing table is read under the lock of its graph, which the caller may
        // hold already.
        let rows = match first.update_graph().filter(|_| first.is_refreshing()) {
            Some(graph) => locked(&*graph, || generated.copy(&first, 0))?,
            None => generated.copy(&first, 0)?,
        };
        Ok((generated, rows))
    }

    fn spec(&self, rows: u64) -> TableSpec {
        let sources = self.columns.iter();
        let sources = sources.map(|column| column.clone() as Arc<dyn ColumnSource>);
        TableSpec::new(
            self.definition.clone(),
            RowSet::flat(rows),
            sources.collect(),
        )
    }

    /// Copies the rows of `table` to the row keys `0..size`, each column to the column of
    /// its name, forgets the rest of `old_rows` rows, and returns the size.
    ///
    /// An error, as in Java, for a column the result does not have and for a column of a
    /// type that Java stores differently. Two cases that Java copies are refused here by
    /// the name of its method: a table that lacks a column of the result, where Java keeps
    /// the values the column had and shows nulls in new rows, and a column of another
    /// object type (a `BigDecimal` for a `String`), where Java stores the objects in a
    /// column whose type says otherwise.
    ///
    /// Behavior: FunctionGeneratedTableFactory#copyTable
    fn copy(&self, table: &Arc<dyn Table>, old_rows: u64) -> Result<u64, TableFailure> {
        const COPY: &str =
            "io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory#copyTable";
        let columns = table.definition().columns();
        let mut targets = Vec::with_capacity(columns.len());
        for column in columns {
            let Some(target) = self.definition.index_of(&column.name) else {
                // Java looks the column up in a map and calls a method of what it finds.
                return Err(failure(format!(
                    "java.lang.NullPointerException: the function-generated table has no \
                     column {}",
                    column.name
                )));
            };
            let declared = &self.definition.columns()[target].column_type;
            if *declared != column.column_type {
                return Err(failure(format!(
                    "the type of column {} changed from {declared:?} to {:?}: a \
                     java.lang.ClassCastException in Java, except between two object types, \
                     which {COPY} copies (not ported)",
                    column.name, column.column_type
                )));
            }
            targets.push(target);
        }
        if let Some(missing) = (0..self.columns.len()).find(|index| !targets.contains(index)) {
            return Err(failure(format!(
                "not ported yet: {COPY}: the generated table no longer has the column {}",
                self.definition.columns()[missing].name
            )));
        }
        let rows = table.row_set();
        let size = rows.size();
        let (kept, dropped) = (RowSet::flat(size), tail(size, old_rows));
        for (index, target) in targets.into_iter().enumerate() {
            let values = table.column(index).chunk(&rows)?;
            self.columns[target].remove(&dropped);
            self.columns[target].fill(&kept, &values)?;
        }
        Ok(size)
    }

    /// Calls the function in an update cycle and returns the change of a result that had
    /// `old_rows` rows.
    ///
    /// Behavior: FunctionGeneratedTableFactory#updateTable, FunctionBackedTable#doRefresh
    fn refresh(&self, old_rows: u64) -> Result<TableUpdate, TableFailure> {
        let table = (self.generator)()?;
        if table.is_refreshing() {
            let Some(graph) = &self.graph else {
                panic!("a static generated table is never refreshed");
            };
            let same = |other: Arc<dyn UpdateGraph>| Arc::ptr_eq(&other, graph);
            if !table.update_graph().is_some_and(same) {
                return Err(failure(ANOTHER_GRAPH));
            }
            // Java asks the graph, not the table: every source has refreshed.
            if !graph.sources_satisfied(graph.clock().step) {
                return Err(failure(NOT_SATISFIED));
            }
        }
        let new_rows = self.copy(&table, old_rows)?;
        let modified = RowSet::flat(old_rows.min(new_rows));
        let modified_columns = if modified.is_empty() {
            ModifiedColumnSet::empty()
        } else {
            ModifiedColumnSet::All
        };
        Ok(TableUpdate {
            added: tail(old_rows, new_rows),
            removed: tail(new_rows, old_rows),
            modified,
            modified_columns,
            ..TableUpdate::empty()
        })
    }
}

/// The row keys `from..to`, or none if `to <= from`.
fn tail(from: u64, to: u64) -> RowSet {
    if to <= from {
        RowSet::empty()
    } else {
        RowSet::from_range(from as i64, to as i64 - 1)
    }
}

impl MergedOperator for Generated {
    fn on_update(
        &self,
        result: &QueryTable,
        _upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        self.refresh(result.row_set().size())
    }
}

/// The update source of a table generated on an interval.
struct Periodic {
    table: Weak<QueryTable>,
    generated: Generated,
    clock: Arc<dyn TimeSource>,
    interval: i64,
    /// The time, in milliseconds on `clock`, from which the next cycle generates again.
    next_refresh: Mutex<i64>,
}

impl UpdateSource for Periodic {
    /// Behavior: FunctionBackedTable#run
    fn refresh(&self) {
        // Java does not register a table with an interval of zero as a source.
        if self.interval == 0 {
            return;
        }
        let Some(table) = self.table.upgrade().filter(|table| !table.is_failed()) else {
            return;
        };
        let now = millis(&*self.clock);
        {
            let mut next_refresh = self
                .next_refresh
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if now < *next_refresh {
                return;
            }
            *next_refresh = now + self.interval;
        }
        match self.generated.refresh(table.row_set().size()) {
            Ok(update) => table.notify(update),
            Err(failure) => table.fail(failure),
        }
    }
}
