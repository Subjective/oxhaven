//! Test support shared by the tests of the sort and merge families.

use crate::graph::SteppedUpdateGraph;
use crate::table::{tracked_source, QueryTable, RootSource, TableSpec};
use arrow::array::{Array, ArrayRef};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{
    definition, test_seeds, ConsistencyChecker, RandomUpdates, Recorder, ReferenceTable,
    ScriptedTable, TableChange, TableSnapshot,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

pub(crate) fn graph() -> Arc<dyn UpdateGraph> {
    SteppedUpdateGraph::new()
}

pub(crate) type Columns<'a> = &'a [(&'a str, ColumnType, ArrayRef)];

fn split(columns: Columns) -> (TableDefinition, Vec<ArrayRef>) {
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let values = columns
        .iter()
        .map(|(_, _, values)| values.clone())
        .collect();
    (definition(&names), values)
}

/// A static table with the given columns at the row keys `0..size`.
pub(crate) fn static_table(columns: Columns) -> Arc<dyn Table> {
    let (definition, values) = split(columns);
    let size = values.first().map_or(0, |v: &ArrayRef| v.len());
    ReferenceTable::new_static(definition, RowSet::flat(size as u64), values)
}

/// A scripted table with the given columns at `keys`.
pub(crate) fn scripted(
    graph: &Arc<dyn UpdateGraph>,
    keys: impl IntoIterator<Item = RowKey>,
    columns: Columns,
) -> Arc<ScriptedTable> {
    let (definition, values) = split(columns);
    ScriptedTable::new(graph.clone(), definition, RowSet::from_keys(keys), values)
}

/// A static copy of what `table` holds now, at the row keys `0..size`.
pub(crate) fn frozen(table: &dyn Table) -> Arc<dyn Table> {
    let snapshot = TableSnapshot::current(table).unwrap();
    ReferenceTable::new_static(
        table.definition().clone(),
        RowSet::flat(snapshot.rows.size()),
        snapshot.columns,
    )
}

/// The current values of column `column`, in row order, as text; `null` for a null.
pub(crate) fn cells(table: &dyn Table, column: usize) -> Vec<String> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let values = &snapshot.columns[column];
    let options = FormatOptions::default().with_null("null");
    let formatter = ArrayFormatter::try_new(values.as_ref(), &options).unwrap();
    (0..values.len())
        .map(|row| formatter.value(row).to_string())
        .collect()
}

/// The values of the column named `Tag`.
pub(crate) fn tags(table: &dyn Table) -> Vec<String> {
    cells(table, table.definition().index_of("Tag").unwrap())
}

pub(crate) fn keys_of(table: &dyn Table) -> Vec<RowKey> {
    table.row_set().iter().collect()
}

/// A blink table for tests: every cycle it drops the rows it had and shows the rows
/// pushed for that cycle, at row keys that keep counting up.
pub(crate) struct Blink {
    table: Weak<QueryTable>,
    columns: Vec<Arc<WritableSource>>,
    queue: Mutex<VecDeque<Vec<ArrayRef>>>,
    next_key: Mutex<RowKey>,
}

impl Blink {
    /// Queues the rows of one cycle: one array per column.
    pub(crate) fn push(&self, rows: Vec<ArrayRef>) {
        self.queue.lock().unwrap().push_back(rows);
    }
}

impl UpdateSource for Blink {
    fn refresh(&self) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        let removed = (*table.row_set()).clone();
        let rows = self.queue.lock().unwrap().pop_front();
        let size = rows.as_ref().map_or(0, |rows| rows[0].len()) as RowKey;
        if removed.is_empty() && size == 0 {
            return;
        }
        let mut update = TableUpdate::empty();
        for column in &self.columns {
            column.remove(&removed);
        }
        update.removed = removed;
        if let Some(rows) = rows.filter(|_| size > 0) {
            let mut next = self.next_key.lock().unwrap();
            update.added = RowSet::from_range(*next, *next + size - 1);
            *next += size;
            for (column, values) in self.columns.iter().zip(&rows) {
                column.fill(&update.added, values).unwrap();
            }
        }
        table.notify(update);
    }
}

pub(crate) fn blink_table(
    graph: &Arc<dyn UpdateGraph>,
    columns: &[(&str, ColumnType)],
) -> (Arc<dyn Table>, Arc<Blink>) {
    let definition = definition(columns);
    let sources: Vec<Arc<WritableSource>> = columns
        .iter()
        .map(|(_, column_type)| tracked_source(graph, column_type.clone()))
        .collect();
    let mut attributes = TableAttributes::default();
    attributes.blink = true;
    let spec = TableSpec::new(
        definition,
        RowSet::empty(),
        sources
            .iter()
            .map(|source| source.clone() as Arc<dyn ColumnSource>)
            .collect(),
    )
    .with_attributes(attributes);
    let mut handle = None;
    let table = QueryTable::root(graph.clone(), spec, |table| {
        let source = Arc::new(Blink {
            table,
            columns: sources,
            queue: Mutex::default(),
            next_key: Mutex::new(0),
        });
        handle = Some(source.clone());
        RootSource {
            source,
            input: None,
        }
    });
    (table, handle.unwrap())
}

/// One scripted table and one operation on it, with every update recorded and checked.
pub(crate) struct Ticking {
    pub(crate) graph: Arc<dyn UpdateGraph>,
    pub(crate) script: Arc<ScriptedTable>,
    pub(crate) result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Ticking {
    /// `build` on a scripted table with `columns` at `keys`.
    pub(crate) fn new(
        keys: impl IntoIterator<Item = RowKey>,
        columns: Columns,
        build: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
    ) -> Self {
        let graph = graph();
        let script = scripted(&graph, keys, columns);
        let source: Arc<dyn Table> = script.table();
        let result = build(&source);
        let recorder = Recorder::attach(result.clone());
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), move || {
            build(&frozen(&*source))
        });
        checker.assert_consistent();
        Self {
            graph,
            script,
            result,
            recorder,
            checker,
        }
    }

    /// Applies `change` to the source in one cycle and returns the update the result
    /// delivered in it, if it delivered one.
    ///
    /// # Panics
    /// If the result is then inconsistent or differs from the operation on a static copy
    /// of the source, or if it failed.
    pub(crate) fn step(&self, change: TableChange) -> Option<TableUpdate> {
        let before = self.recorder.events().len();
        self.script.push(change);
        self.graph.run_cycle();
        self.checker.assert_consistent();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one update in a cycle");
        events.pop().map(|event| event.unwrap())
    }

    /// Fails the source in one cycle and returns the failure the result delivered.
    pub(crate) fn fail(&self, message: &str) -> TableFailure {
        self.script.push_failure(message);
        self.graph.run_cycle();
        let event = self.recorder.events().pop();
        event.unwrap().unwrap_err()
    }
}

/// The shifts of `update` as `(first, last, delta)`.
pub(crate) fn shifts(update: &TableUpdate) -> Vec<(RowKey, RowKey, i64)> {
    let ranges = update.shifted.ranges().iter();
    ranges.map(|r| (r.first, r.last, r.delta)).collect()
}

pub(crate) fn keys(rows: &RowSet) -> Vec<RowKey> {
    rows.iter().collect()
}

/// An operation under test: builds its result from the source tables.
pub(crate) type Operation<'a> = (
    &'a str,
    Arc<dyn Fn(&[Arc<dyn Table>]) -> Arc<dyn Table> + Send + Sync>,
);

pub(crate) fn operation<'a>(
    name: &'a str,
    build: impl Fn(&[Arc<dyn Table>]) -> Arc<dyn Table> + Send + Sync + 'static,
) -> Operation<'a> {
    (name, Arc::new(build))
}

/// Drives `sources` scripted tables of `columns` through `steps` random cycles for every
/// seed of `seeds` that the seed budget runs (`oxhaven_testkit::test_seeds`), with every operation built on them before the first cycle. After each cycle each
/// result must be consistent and equal to the same operation on static copies of the
/// sources. A source changes in about two cycles out of three.
pub(crate) fn check_random(
    seeds: std::ops::Range<u64>,
    steps: usize,
    sources: usize,
    columns: &[(&str, ColumnType)],
    operations: &[Operation],
) {
    for seed in test_seeds(seeds) {
        let graph = graph();
        let mut random = RandomUpdates::new(seed);
        let scripts: Vec<Arc<ScriptedTable>> = (0..sources)
            .map(|index| {
                // Some sources start empty.
                let empty = (seed + index as u64).is_multiple_of(4);
                let size = if empty { 0 } else { 6 };
                let values = columns
                    .iter()
                    .map(|(_, column_type)| random.values(column_type, size))
                    .collect();
                ScriptedTable::new(
                    graph.clone(),
                    definition(columns),
                    RowSet::from_keys((0..size as RowKey).map(|k| 3 * k + 1)),
                    values,
                )
            })
            .collect();
        let tables: Vec<Arc<dyn Table>> = scripts
            .iter()
            .map(|script| script.table() as Arc<dyn Table>)
            .collect();
        let checkers: Vec<(&str, Arc<ConsistencyChecker>)> = operations
            .iter()
            .map(|(name, build)| {
                let result = build(&tables);
                let (build, tables) = (build.clone(), tables.clone());
                let checker = ConsistencyChecker::attach_with_recompute(result, move || {
                    let copies: Vec<Arc<dyn Table>> =
                        tables.iter().map(|table| frozen(&**table)).collect();
                    build(&copies)
                });
                (*name, checker)
            })
            .collect();
        for (name, checker) in &checkers {
            if let Err(e) = checker.verify() {
                panic!("{name}, seed {seed}, before the first step: {e}");
            }
        }
        // A separate stream decides which sources change, so that the changes themselves
        // do not depend on the number of sources.
        let mut chooser = RandomUpdates::new(seed ^ 0x5eed);
        for step in 0..steps {
            for (index, script) in scripts.iter().enumerate() {
                let pick = chooser.values(&ColumnType::Byte, 1);
                let pick = pick
                    .as_any()
                    .downcast_ref::<arrow::array::Int8Array>()
                    .unwrap();
                let changes =
                    sources == 1 || pick.is_null(0) || (pick.value(0) + index as i8) % 3 != 0;
                if changes {
                    script.push(random.next_change(&*script.table(), 4));
                }
            }
            graph.run_cycle();
            for (name, checker) in &checkers {
                if let Err(e) = checker.verify() {
                    panic!("{name}, seed {seed}, step {step}: {e}");
                }
            }
        }
    }
}
