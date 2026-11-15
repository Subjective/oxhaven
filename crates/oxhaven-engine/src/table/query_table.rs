//! The concrete table: what every operation returns and every root table is.

use super::failure;
use crate::graph::{MergedHandler, MergedListener, ParentListener, Subscribers, UpdateHandler};
use oxhaven_contracts::graph::{locked, ClockPhase, Dependency, UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{
    ColumnSource, InputTable, Table, TableAttributes, TableFailure, UpdateListener,
};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_rowset::TrackingRowSet;
use std::sync::{Arc, PoisonError, RwLock, Weak};

/// What a new table starts as: its schema, initial rows, columns and hints.
pub struct TableSpec {
    pub definition: TableDefinition,
    /// The initial row set.
    pub rows: RowSet,
    /// One source per column of `definition`, in order, of that column's type.
    pub columns: Vec<Arc<dyn ColumnSource>>,
    pub attributes: TableAttributes,
    /// Whether the row set is `0..size` now and after every update. Port of `Table#isFlat`.
    pub flat: bool,
    /// The write side of the input table this table has the `InputTable` attribute of.
    pub input: Option<Arc<dyn InputTable>>,
}

impl TableSpec {
    /// A spec with no attributes that is not flat.
    pub fn new(
        definition: TableDefinition,
        rows: RowSet,
        columns: Vec<Arc<dyn ColumnSource>>,
    ) -> Self {
        Self {
            definition,
            rows,
            columns,
            attributes: TableAttributes::default(),
            flat: false,
            input: None,
        }
    }

    /// Gives the table the `InputTable` attribute of `parent`, if it has one. For the
    /// operations whose result is still written through its parent's input table.
    ///
    /// Behavior: the `INPUT_TABLE_ATTRIBUTE` entry of
    /// io.deephaven.engine.table.impl.BaseTable#attributeToCopySet, which lists the
    /// operations `Flatten`, `Sort`, `UpdateView`, `Join` and `Filter`. The attribute's
    /// value is the parent's `InputTableUpdater` itself.
    pub fn with_input_table_of(mut self, parent: &dyn Table) -> Self {
        self.input = parent.input_table();
        self
    }

    pub fn with_attributes(mut self, attributes: TableAttributes) -> Self {
        self.attributes = attributes;
        self
    }

    pub fn flat(mut self) -> Self {
        self.flat = true;
        self
    }
}

/// The incremental half of an operation with one parent.
pub trait UnaryOperator: Send + Sync + 'static {
    /// Called once for each step on which the parent changed. Bring the result's columns
    /// and the operator's own state up to date and return the result's change; the table
    /// applies it to its row set and delivers it. `result` still has the row set from before
    /// this step. The parent's previous row set and values are readable.
    ///
    /// Return an update that changes nothing if the parent's change does not show in the
    /// result. An `Err` fails the result table.
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure>;
}

impl<F> UnaryOperator for F
where
    F: Fn(&QueryTable, &TableUpdate) -> Result<TableUpdate, TableFailure> + Send + Sync + 'static,
{
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure> {
        self(result, upstream)
    }
}

/// The incremental half of an operation with several parents.
pub trait MergedOperator: Send + Sync + 'static {
    /// Called once for each step on which any parent changed, after all of them have
    /// finished. `upstream[i]` is the change of parent `i`, in the order the parents were
    /// given, or `None` if it did not change. Otherwise as [`UnaryOperator::on_update`].
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure>;
}

impl<F> MergedOperator for F
where
    F: Fn(&QueryTable, &[Option<Arc<TableUpdate>>]) -> Result<TableUpdate, TableFailure>
        + Send
        + Sync
        + 'static,
{
    fn on_update(
        &self,
        result: &QueryTable,
        upstream: &[Option<Arc<TableUpdate>>],
    ) -> Result<TableUpdate, TableFailure> {
        self(result, upstream)
    }
}

/// What a root table hands to [`QueryTable::root`]: the update source that drives it and,
/// for an input table, its write side.
pub struct RootSource {
    pub source: Arc<dyn UpdateSource>,
    pub input: Option<Arc<dyn InputTable>>,
}

/// What can make a table change. The table owns it, and so keeps its upstream alive.
enum Feed {
    Static,
    Root {
        _source: Arc<dyn UpdateSource>,
    },
    Unary {
        listener: Arc<dyn UpdateListener>,
        operator: Box<dyn UnaryOperator>,
    },
    Merged {
        listener: Arc<MergedListener>,
        operator: Box<dyn MergedOperator>,
    },
}

struct Live {
    graph: Arc<dyn UpdateGraph>,
    subscribers: Subscribers,
}

/// The concrete [`Table`]: a row set, one column source per column, attributes, and, when
/// refreshing, its listeners and whatever feeds it.
///
/// There are four ways to make one, and each wires `Dependency::satisfied`, listener
/// ownership and failure handling for its kind:
///
/// - [`QueryTable::new_static`]: a table that never changes.
/// - [`QueryTable::derive`]: the result of an operation on one table.
/// - [`QueryTable::derive_merged`]: the result of an operation on several tables.
/// - [`QueryTable::root`]: a table driven by an update source, such as an input table.
///
/// See the [module documentation](super) for how to write an operator.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable, io.deephaven.engine.table.impl.BaseTable
pub struct QueryTable {
    definition: TableDefinition,
    columns: Vec<Arc<dyn ColumnSource>>,
    attributes: TableAttributes,
    flat: bool,
    rows: RwLock<TrackingRowSet>,
    /// `None` for a static table.
    live: Option<Live>,
    feed: Feed,
    input: Option<Arc<dyn InputTable>>,
}

impl Drop for QueryTable {
    fn drop(&mut self) {
        super::copied::forget(self);
    }
}

impl QueryTable {
    fn build(
        mut spec: TableSpec,
        graph: Option<Arc<dyn UpdateGraph>>,
        feed: Feed,
        input: Option<Arc<dyn InputTable>>,
    ) -> QueryTable {
        let input = input.or(spec.input.take());
        let declared = spec.definition.columns();
        assert_eq!(
            declared.len(),
            spec.columns.len(),
            "a table needs one column source per column of its definition"
        );
        for (column, source) in declared.iter().zip(&spec.columns) {
            assert_eq!(
                &column.column_type,
                source.column_type(),
                "column {} has a source of another type",
                column.name
            );
        }
        if spec.flat {
            assert_eq!(
                spec.rows,
                RowSet::flat(spec.rows.size()),
                "a flat table must start with a flat row set"
            );
        }
        QueryTable {
            definition: spec.definition,
            columns: spec.columns,
            attributes: spec.attributes,
            flat: spec.flat,
            rows: RwLock::new(TrackingRowSet::new(spec.rows)),
            live: graph.map(|graph| Live {
                subscribers: Subscribers::new(graph.clone()),
                graph,
            }),
            feed,
            input,
        }
    }

    /// A table that never changes.
    ///
    /// # Panics
    /// As every constructor: if the spec's columns do not match its definition, or it is
    /// flat without a flat row set.
    pub fn new_static(spec: TableSpec) -> Arc<QueryTable> {
        Arc::new(Self::build(spec, None, Feed::Static, None))
    }

    /// The result of an operation on `parent`.
    ///
    /// `init` computes the result's initial state from the parent's current state and
    /// returns it with the operator that will keep it up to date. If the parent is
    /// refreshing, `init` runs under the graph's shared lock and the result is subscribed
    /// to the parent before the lock is released, so no update can fall between the two.
    /// If the parent is static, so is the result, and the operator is dropped.
    ///
    /// An error from `init`, or a parent that has failed, is the operation's error.
    ///
    /// Not supported yet: calling this from inside an update cycle for a parent that has
    /// not finished updating. The initial state would be read half-way through the step.
    pub fn derive<O: UnaryOperator>(
        parent: &Arc<dyn Table>,
        init: impl FnOnce() -> Result<(TableSpec, O), TableFailure>,
    ) -> Result<Arc<QueryTable>, TableFailure> {
        let graph = match parent.update_graph() {
            Some(graph) if parent.is_refreshing() => graph,
            _ => return Ok(Self::new_static(init()?.0)),
        };
        locked(&*graph, || {
            check_not_failed(&**parent)?;
            let (spec, operator) = init()?;
            Ok(Arc::new_cyclic(|result: &Weak<QueryTable>| {
                let handler: Weak<dyn UpdateHandler> = result.clone();
                let listener = ParentListener::subscribe(parent.clone(), handler);
                let feed = Feed::Unary {
                    listener,
                    operator: Box::new(operator),
                };
                Self::build(spec, Some(graph.clone()), feed, None)
            }))
        })
    }

    /// The result of an operation on several tables. As [`QueryTable::derive`], with one
    /// call of the operator per step however many parents changed.
    ///
    /// `dependencies` are tables or listeners the operator reads without reacting to their
    /// changes; it runs only after they have finished updating too.
    ///
    /// # Panics
    /// If the refreshing parents do not all use one update graph.
    pub fn derive_merged<O: MergedOperator>(
        parents: &[Arc<dyn Table>],
        dependencies: Vec<Arc<dyn Dependency>>,
        init: impl FnOnce() -> Result<(TableSpec, O), TableFailure>,
    ) -> Result<Arc<QueryTable>, TableFailure> {
        let mut graphs = parents
            .iter()
            .filter(|parent| parent.is_refreshing())
            .filter_map(|parent| parent.update_graph());
        let Some(graph) = graphs.next() else {
            return Ok(Self::new_static(init()?.0));
        };
        assert!(
            graphs.all(|other| Arc::ptr_eq(&other, &graph)),
            "the parents of one operation must share an update graph"
        );
        locked(&*graph, || {
            for parent in parents {
                check_not_failed(&**parent)?;
            }
            let (spec, operator) = init()?;
            Ok(Arc::new_cyclic(|result: &Weak<QueryTable>| {
                let handler: Weak<dyn MergedHandler> = result.clone();
                let listener = MergedListener::subscribe(
                    graph.clone(),
                    parents.to_vec(),
                    dependencies,
                    handler,
                );
                let feed = Feed::Merged {
                    listener,
                    operator: Box::new(operator),
                };
                Self::build(spec, Some(graph.clone()), feed, None)
            }))
        })
    }

    /// A refreshing table with no parent, driven by an update source.
    ///
    /// `source` receives a weak reference to the new table and returns the update source,
    /// which the table owns and registers with `graph`. In `refresh`, the source writes the
    /// step's changes into the table's columns and calls [`QueryTable::notify`] once, or
    /// not at all if nothing changed.
    pub fn root(
        graph: Arc<dyn UpdateGraph>,
        spec: TableSpec,
        source: impl FnOnce(Weak<QueryTable>) -> RootSource,
    ) -> Arc<QueryTable> {
        let table = Arc::new_cyclic(|table: &Weak<QueryTable>| {
            let RootSource { source, input } = source(table.clone());
            let feed = Feed::Root { _source: source };
            Self::build(spec, Some(graph.clone()), feed, input)
        });
        if let Feed::Root { _source } = &table.feed {
            graph.add_source(_source);
        }
        table
    }

    /// Applies `update` to the row set and delivers it to the listeners. For the update
    /// source of a root table; an operator returns its update instead.
    ///
    /// The columns must already hold the new values. An update that is not a valid change
    /// of this table, or that breaks one of its attributes, fails the table.
    ///
    /// # Panics
    /// If the table is static or has failed, the graph is idle, or the table already
    /// notified on this step.
    pub fn notify(&self, update: TableUpdate) {
        if let Err(failure) = self.publish(update) {
            self.fail(failure);
        }
    }

    /// Fails the table and tells its listeners. For the update source of a root table; an
    /// operator returns an error instead.
    ///
    /// # Panics
    /// If the table is static or the graph is idle.
    pub fn fail(&self, failure: TableFailure) {
        self.live().subscribers.notify_failure(failure);
    }

    fn live(&self) -> &Live {
        match &self.live {
            Some(live) => live,
            None => panic!("a static table has no listeners to notify"),
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.BaseTable#notifyListeners
    fn publish(&self, update: TableUpdate) -> Result<(), TableFailure> {
        let live = self.live();
        let Some(update) = update.normalized() else {
            return Ok(());
        };
        {
            let mut rows = self.rows.write().unwrap_or_else(PoisonError::into_inner);
            let next = update
                .apply_to(rows.current())
                .map_err(|e| failure(format!("invalid update: {e}")))?;
            self.check_attributes(&update, rows.current(), &next)?;
            *rows.write(live.graph.clock().step) = next;
        }
        live.subscribers.notify(update);
        Ok(())
    }

    /// Behavior: the attribute assertions of io.deephaven.engine.table.impl.BaseTable#notifyListeners
    fn check_attributes(
        &self,
        update: &TableUpdate,
        before: &RowSet,
        after: &RowSet,
    ) -> Result<(), TableFailure> {
        // The update is known to be a valid change of `before`, so its removed rows are
        // rows of `before`.
        let only_adds_and_removes = update.modified.is_empty() && update.shifted.is_empty();
        let only_adds = only_adds_and_removes && update.removed.is_empty();
        if self.flat && *after != RowSet::flat(after.size()) {
            return Err(failure("a flat table was given a row set that is not flat"));
        }
        if (self.attributes.add_only || self.attributes.append_only) && !only_adds {
            return Err(failure(
                "an add-only table was given removes, modifies or shifts",
            ));
        }
        if self.attributes.append_only {
            if let (Some(last), Some(first)) = (before.last_row_key(), update.added.first_row_key())
            {
                if last >= first {
                    return Err(failure(
                        "an append-only table was given rows before its last row",
                    ));
                }
            }
        }
        if self.attributes.blink
            && !(only_adds_and_removes && update.removed.size() == before.size())
        {
            return Err(failure(
                "a blink table must replace all of its rows on every update",
            ));
        }
        Ok(())
    }
}

fn check_not_failed(parent: &dyn Table) -> Result<(), TableFailure> {
    if parent.is_failed() {
        return Err(failure("the parent table has failed"));
    }
    Ok(())
}

impl UpdateHandler for QueryTable {
    fn on_update(&self, upstream: &TableUpdate) -> Result<(), TableFailure> {
        match &self.feed {
            Feed::Unary { operator, .. } => self.publish(operator.on_update(self, upstream)?),
            _ => panic!("only a table with one parent has a parent listener"),
        }
    }

    fn on_failure(&self, failure: &TableFailure) {
        self.fail(failure.clone());
    }
}

impl MergedHandler for QueryTable {
    fn process(&self, upstream: &[Option<Arc<TableUpdate>>]) -> Result<(), TableFailure> {
        match &self.feed {
            Feed::Merged { operator, .. } => self.publish(operator.on_update(self, upstream)?),
            _ => panic!("only a table with several parents has a merged listener"),
        }
    }

    fn on_failure(&self, failure: &TableFailure) {
        self.fail(failure.clone());
    }
}

impl Dependency for QueryTable {
    /// Behavior: io.deephaven.engine.table.impl.BaseTable#satisfied
    fn satisfied(&self, step: u64) -> bool {
        let Some(live) = &self.live else {
            return true;
        };
        live.subscribers.satisfied(step, || match &self.feed {
            Feed::Static => true,
            Feed::Root { .. } => live.graph.sources_satisfied(step),
            Feed::Unary { listener, .. } => listener.satisfied(step),
            Feed::Merged { listener, .. } => listener.satisfied(step),
        })
    }
}

impl Table for QueryTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn row_set(&self) -> Arc<RowSet> {
        let rows = self.rows.read().unwrap_or_else(PoisonError::into_inner);
        Arc::new(rows.current().clone())
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        let rows = self.rows.read().unwrap_or_else(PoisonError::into_inner);
        let prev = match self.live.as_ref().map(|live| live.graph.clock()) {
            Some(clock) if clock.phase == ClockPhase::Updating => rows.prev(clock.step),
            // While idle, previous is current, whatever step the last write was made on.
            _ => rows.current(),
        };
        Arc::new(prev.clone())
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        self.columns[index].clone()
    }

    fn is_refreshing(&self) -> bool {
        self.live.is_some()
    }

    fn attributes(&self) -> TableAttributes {
        self.attributes
    }

    fn is_flat(&self) -> bool {
        self.flat
    }

    fn is_failed(&self) -> bool {
        self.live
            .as_ref()
            .is_some_and(|live| live.subscribers.is_failed())
    }

    fn input_table(&self) -> Option<Arc<dyn InputTable>> {
        self.input.clone()
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        self.live.as_ref().map(|live| live.graph.clone())
    }

    fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
        if let Some(live) = &self.live {
            live.subscribers.add(listener);
        }
    }
}
