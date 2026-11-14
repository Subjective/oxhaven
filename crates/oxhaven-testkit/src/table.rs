//! The reference table: a row set and one Arrow array per column, rebuilt on every change.

use crate::graph::lock;
use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::compute::{concat, take};
use oxhaven_contracts::graph::{ClockPhase, Dependency, Notification, UpdateGraph};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::{
    ColumnSource, Table, TableAttributes, TableFailure, UpdateListener,
};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate, UpdateError};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

/// One cycle's change to a [`ReferenceTable`]: the update and the values it brings.
///
/// Build one with the chained methods; each may be used at most once per change. Values are
/// always in increasing row-key order, whatever order the keys were given in.
///
/// ```
/// use oxhaven_testkit::{longs, strings, TableChange};
/// // Remove row 1, move rows 5..=9 down by 3, add rows 20 and 21, and change column 1 of row 0.
/// let change = TableChange::new()
///     .remove([1])
///     .shift(5, 9, -3)
///     .add([20, 21], vec![longs([7, 8]), strings(["x", "y"])])
///     .modify([0], [(1, strings(["z"]))]);
/// assert_eq!(change.update.added.size(), 2);
/// ```
#[derive(Clone, Debug)]
pub struct TableChange {
    pub update: TableUpdate,
    /// One array per column of the table, each with one value per added row. May be empty
    /// when no rows are added.
    pub added_values: Vec<ArrayRef>,
    /// For each modified column, by index in the definition, one value per modified row.
    pub modified_values: BTreeMap<usize, ArrayRef>,
}

impl Default for TableChange {
    fn default() -> Self {
        Self::new()
    }
}

impl TableChange {
    /// A change that changes nothing. Applying it delivers no update.
    pub fn new() -> Self {
        Self {
            update: TableUpdate::empty(),
            added_values: Vec::new(),
            modified_values: BTreeMap::new(),
        }
    }

    /// Removes the rows at `keys`, in the key space before this change.
    pub fn remove(mut self, keys: impl IntoIterator<Item = RowKey>) -> Self {
        self.update.removed = RowSet::from_keys(keys);
        self
    }

    /// Moves the rows in `first..=last`, in the key space before this change, by `delta`.
    /// Call once per range, in increasing key order.
    ///
    /// # Panics
    /// If the range is not valid after the ranges already given.
    pub fn shift(mut self, first: RowKey, last: RowKey, delta: i64) -> Self {
        let mut ranges = self.update.shifted.ranges().to_vec();
        ranges.push(ShiftRange { first, last, delta });
        self.update.shifted = match RowSetShiftData::new(ranges) {
            Ok(shifted) => shifted,
            Err(e) => panic!("invalid shift: {e}"),
        };
        self
    }

    /// Adds rows at `keys`, in the key space after this change, with one array per column.
    pub fn add(mut self, keys: impl IntoIterator<Item = RowKey>, values: Vec<ArrayRef>) -> Self {
        self.update.added = RowSet::from_keys(keys);
        self.added_values = values;
        self
    }

    /// Replaces the values of the given columns for the rows at `keys`, in the key space
    /// after this change. The modified column set is exactly the columns given.
    pub fn modify(
        mut self,
        keys: impl IntoIterator<Item = RowKey>,
        values: impl IntoIterator<Item = (usize, ArrayRef)>,
    ) -> Self {
        self.update.modified = RowSet::from_keys(keys);
        self.modified_values = values.into_iter().collect();
        self.update.modified_columns = ModifiedColumnSet::of(self.modified_values.keys().copied());
        self
    }
}

/// Why a [`ReferenceTable`] refused a change. The table is unchanged.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ChangeError {
    /// The update is not a valid change from the table's current row set.
    Update(UpdateError),
    /// The values do not match the update or the table's definition.
    Values(String),
    /// The table is static, or its graph is not inside a cycle.
    NotUpdating,
    /// The table has already notified its listeners in this step.
    AlreadyNotified,
    /// The table has failed.
    Failed,
}

impl fmt::Display for ChangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChangeError::Update(e) => write!(f, "invalid update: {e}"),
            ChangeError::Values(message) => write!(f, "invalid values: {message}"),
            ChangeError::NotUpdating => write!(f, "tables change only inside a cycle"),
            ChangeError::AlreadyNotified => write!(f, "table already notified in this step"),
            ChangeError::Failed => write!(f, "table has failed"),
        }
    }
}

impl std::error::Error for ChangeError {}

/// Everything a table holds at one instant. Column `c`'s value for the row set's `i`th key is
/// element `i` of `columns[c]`.
#[derive(Clone)]
struct State {
    rows: Arc<RowSet>,
    columns: Vec<ArrayRef>,
}

struct Inner {
    current: State,
    /// The whole state as it was before the first change of `changed_step`.
    prev: State,
    changed_step: Option<u64>,
    notified_step: Option<u64>,
    listeners: Vec<Weak<dyn UpdateListener>>,
    failed: bool,
}

struct Shared {
    graph: Option<Arc<dyn UpdateGraph>>,
    inner: Mutex<Inner>,
}

impl Shared {
    /// The current state, or with `prev` the state at the end of the previous cycle. The two
    /// differ only inside a cycle in which the table has changed.
    fn state(&self, prev: bool) -> State {
        let clock = self.graph.as_ref().map(|graph| graph.clock());
        let inner = lock(&self.inner);
        match clock {
            Some(clock)
                if prev
                    && clock.phase == ClockPhase::Updating
                    && inner.changed_step == Some(clock.step) =>
            {
                inner.prev.clone()
            }
            _ => inner.current.clone(),
        }
    }
}

/// The simplest correct [`Table`], for tests and as the model real tables are compared with.
///
/// Its state is a row set plus one Arrow array per column. A change builds a complete new
/// state, and the first change of a step keeps the complete old state as the previous values
/// for the rest of that cycle.
///
/// A refreshing reference table is a root of the graph: it changes only while sources
/// refresh (see [`crate::ScriptedTable`]), so it is satisfied for a step once it has notified
/// or once every source has refreshed.
pub struct ReferenceTable {
    definition: TableDefinition,
    shared: Arc<Shared>,
}

struct ReferenceColumn {
    column_type: ColumnType,
    index: usize,
    shared: Arc<Shared>,
}

impl ReferenceColumn {
    fn read(&self, rows: &RowSet, prev: bool) -> Result<ArrayRef, TableFailure> {
        let state = self.shared.state(prev);
        let positions = state.rows.invert(rows).map_err(|_| TableFailure {
            message: format!(
                "rows {:?} are not in the {} row set {:?}",
                rows.minus(&state.rows),
                if prev { "previous" } else { "current" },
                state.rows
            ),
        })?;
        let positions = UInt64Array::from_iter_values(positions.iter().map(|p| p as u64));
        take(state.columns[self.index].as_ref(), &positions, None).map_err(|e| TableFailure {
            message: e.to_string(),
        })
    }
}

impl ColumnSource for ReferenceColumn {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, true)
    }
}

/// Delivers one update or failure to one listener.
struct Delivery {
    listener: Arc<dyn UpdateListener>,
    event: Result<Arc<TableUpdate>, TableFailure>,
}

impl Notification for Delivery {
    fn can_execute(&self, _step: u64) -> bool {
        // The only thing a delivery waits for is its table, which has already changed.
        true
    }

    fn run(self: Box<Self>) {
        match &self.event {
            Ok(update) => {
                if let Err(failure) = self.listener.on_update(update) {
                    self.listener.on_failure(&failure);
                }
            }
            Err(failure) => self.listener.on_failure(failure),
        }
    }
}

impl ReferenceTable {
    /// A table that never changes.
    ///
    /// # Panics
    /// If `columns` does not match `definition` and `rows`.
    pub fn new_static(
        definition: TableDefinition,
        rows: RowSet,
        columns: Vec<ArrayRef>,
    ) -> Arc<Self> {
        Self::build(None, definition, rows, columns)
    }

    /// A table driven by `graph`. Change it with [`ReferenceTable::apply`] from an update
    /// source, or use [`crate::ScriptedTable`], which does that for you.
    ///
    /// # Panics
    /// If `columns` does not match `definition` and `rows`.
    pub fn new_refreshing(
        graph: Arc<dyn UpdateGraph>,
        definition: TableDefinition,
        rows: RowSet,
        columns: Vec<ArrayRef>,
    ) -> Arc<Self> {
        Self::build(Some(graph), definition, rows, columns)
    }

    fn build(
        graph: Option<Arc<dyn UpdateGraph>>,
        definition: TableDefinition,
        rows: RowSet,
        columns: Vec<ArrayRef>,
    ) -> Arc<Self> {
        if let Err(message) = check_values(&definition, &columns, rows.size(), "row") {
            panic!("invalid reference table: {message}");
        }
        let state = State {
            rows: Arc::new(rows),
            columns,
        };
        Arc::new(Self {
            definition,
            shared: Arc::new(Shared {
                graph,
                inner: Mutex::new(Inner {
                    current: state.clone(),
                    prev: state,
                    changed_step: None,
                    notified_step: None,
                    listeners: Vec::new(),
                    failed: false,
                }),
            }),
        })
    }

    /// Applies `change` and schedules its update for delivery to the listeners. Must be
    /// called inside a cycle, at most once per step. A change whose update is empty after
    /// [`TableUpdate::normalized`] does nothing.
    pub fn apply(&self, change: &TableChange) -> Result<(), ChangeError> {
        self.apply_reporting(change, None)
    }

    /// Applies `change` but delivers `reported` in place of its update, when given. Tests of
    /// the checker use this to make a table that lies about what changed.
    pub(crate) fn apply_reporting(
        &self,
        change: &TableChange,
        reported: Option<TableUpdate>,
    ) -> Result<(), ChangeError> {
        let (graph, step) = self.updating_step()?;
        let listeners = {
            let mut inner = lock(&self.shared.inner);
            if inner.failed {
                return Err(ChangeError::Failed);
            }
            if inner.notified_step == Some(step) {
                return Err(ChangeError::AlreadyNotified);
            }
            let Some(update) = change.update.clone().normalized() else {
                return Ok(());
            };
            let next = self.next_state(&inner.current, &update, change)?;
            inner.prev = std::mem::replace(&mut inner.current, next);
            inner.changed_step = Some(step);
            inner.notified_step = Some(step);
            let update = Arc::new(reported.unwrap_or(update));
            live(&mut inner.listeners)
                .into_iter()
                .map(|listener| Delivery {
                    listener,
                    event: Ok(update.clone()),
                })
                .collect::<Vec<_>>()
        };
        for delivery in listeners {
            graph.add_notification(Box::new(delivery));
        }
        Ok(())
    }

    /// Fails the table: listeners receive `message` through `on_failure`, and the table
    /// accepts no further changes. Must be called inside a cycle.
    pub fn fail(&self, message: &str) -> Result<(), ChangeError> {
        let (graph, step) = self.updating_step()?;
        let failure = TableFailure {
            message: message.to_string(),
        };
        let listeners = {
            let mut inner = lock(&self.shared.inner);
            if inner.failed {
                return Err(ChangeError::Failed);
            }
            if inner.notified_step == Some(step) {
                return Err(ChangeError::AlreadyNotified);
            }
            inner.failed = true;
            inner.notified_step = Some(step);
            live(&mut inner.listeners)
        };
        for listener in listeners {
            graph.add_notification(Box::new(Delivery {
                listener,
                event: Err(failure.clone()),
            }));
        }
        Ok(())
    }

    fn updating_step(&self) -> Result<(&Arc<dyn UpdateGraph>, u64), ChangeError> {
        let graph = self.shared.graph.as_ref().ok_or(ChangeError::NotUpdating)?;
        let clock = graph.clock();
        if clock.phase != ClockPhase::Updating {
            return Err(ChangeError::NotUpdating);
        }
        Ok((graph, clock.step))
    }

    /// The state after `update`, whose values come from `change`.
    fn next_state(
        &self,
        old: &State,
        update: &TableUpdate,
        change: &TableChange,
    ) -> Result<State, ChangeError> {
        let rows = update.apply_to(&old.rows).map_err(ChangeError::Update)?;

        if !(update.added.is_empty() && change.added_values.is_empty()) {
            let size = update.added.size();
            check_values(&self.definition, &change.added_values, size, "added row")
                .map_err(ChangeError::Values)?;
        }
        let column_count = self.definition.columns().len();
        let modified_values = if update.modified.is_empty() {
            BTreeMap::new()
        } else {
            let wanted: Vec<usize> = (0..column_count)
                .filter(|c| update.modified_columns.contains(*c))
                .collect();
            let given: Vec<usize> = change.modified_values.keys().copied().collect();
            if wanted != given {
                return Err(ChangeError::Values(format!(
                    "modified columns are {wanted:?} but values were given for {given:?}"
                )));
            }
            for (&c, values) in &change.modified_values {
                let column = &self.definition.columns()[c];
                check_array(&column.name, &column.column_type, values)
                    .and_then(|()| {
                        check_len(&column.name, values, update.modified.size(), "modified row")
                    })
                    .map_err(ChangeError::Values)?;
            }
            change.modified_values.clone()
        };

        // Each new row takes its value from the added values, the modified values, or the
        // old value at the key it had before the shifts.
        let unshift = update.shifted.inverse();
        let old_size = old.rows.size();
        let added_size = update.added.size();
        let columns = (0..column_count)
            .map(|c| {
                let mut pieces: Vec<&dyn Array> = vec![old.columns[c].as_ref()];
                if !update.added.is_empty() {
                    pieces.push(change.added_values[c].as_ref());
                }
                let modified = modified_values.get(&c);
                if let Some(values) = modified {
                    pieces.push(values.as_ref());
                }
                let from: UInt64Array = rows
                    .iter()
                    .map(|key| {
                        if let Some(i) = update.added.find(key) {
                            Some(old_size + i)
                        } else if let Some(i) = modified.and(update.modified.find(key)) {
                            Some(old_size + added_size + i)
                        } else {
                            old.rows.find(unshift.apply_to_key(key))
                        }
                    })
                    .collect();
                concat(&pieces)
                    .and_then(|all| take(all.as_ref(), &from, None))
                    .map_err(|e| ChangeError::Values(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(State {
            rows: Arc::new(rows),
            columns,
        })
    }
}

/// The listeners that are still alive; forgets the rest.
fn live(listeners: &mut Vec<Weak<dyn UpdateListener>>) -> Vec<Arc<dyn UpdateListener>> {
    listeners.retain(|listener| listener.strong_count() > 0);
    listeners.iter().filter_map(Weak::upgrade).collect()
}

fn check_array(name: &str, column_type: &ColumnType, values: &ArrayRef) -> Result<(), String> {
    let expected = column_type.arrow_type();
    if values.data_type() != &expected {
        return Err(format!(
            "column {name} needs {expected:?} values but was given {:?}",
            values.data_type()
        ));
    }
    Ok(())
}

fn check_len(name: &str, values: &ArrayRef, size: u64, what: &str) -> Result<(), String> {
    if values.len() as u64 != size {
        return Err(format!(
            "column {name} has {} values for {size} {what}s",
            values.len()
        ));
    }
    Ok(())
}

/// Checks that `values` holds one array per column of `definition`, of the right type, with
/// `size` elements each.
fn check_values(
    definition: &TableDefinition,
    values: &[ArrayRef],
    size: u64,
    what: &str,
) -> Result<(), String> {
    let columns = definition.columns();
    if values.len() != columns.len() {
        return Err(format!(
            "{} arrays for {} columns",
            values.len(),
            columns.len()
        ));
    }
    for (column, values) in columns.iter().zip(values) {
        check_array(&column.name, &column.column_type, values)?;
        check_len(&column.name, values, size, what)?;
    }
    Ok(())
}

impl Dependency for ReferenceTable {
    fn satisfied(&self, step: u64) -> bool {
        match &self.shared.graph {
            None => true,
            Some(graph) => {
                let notified = lock(&self.shared.inner).notified_step;
                notified == Some(step) || graph.sources_satisfied(step)
            }
        }
    }
}

impl Table for ReferenceTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn row_set(&self) -> Arc<RowSet> {
        self.shared.state(false).rows
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        self.shared.state(true).rows
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        Arc::new(ReferenceColumn {
            column_type: self.definition.columns()[index].column_type.clone(),
            index,
            shared: self.shared.clone(),
        })
    }

    fn is_refreshing(&self) -> bool {
        self.shared.graph.is_some()
    }

    fn attributes(&self) -> TableAttributes {
        TableAttributes::default()
    }

    fn is_failed(&self) -> bool {
        lock(&self.shared.inner).failed
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        self.shared.graph.clone()
    }

    fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
        if self.is_refreshing() {
            lock(&self.shared.inner)
                .listeners
                .push(Arc::downgrade(listener));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{long_values, string_values};
    use crate::{definition, longs, strings, Recorder, ReferenceGraph, ScriptedTable};
    use oxhaven_contracts::graph::UpdateSource;

    fn sym_qty() -> TableDefinition {
        definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)])
    }

    /// Rows 0, 1, 5, 6 holding a..d and 10..40.
    fn scripted(graph: &Arc<ReferenceGraph>) -> Arc<ScriptedTable> {
        ScriptedTable::new(
            graph.clone(),
            sym_qty(),
            RowSet::from_keys([0, 1, 5, 6]),
            vec![strings(["a", "b", "c", "d"]), longs([10, 20, 30, 40])],
        )
    }

    #[test]
    fn change_removes_shifts_adds_and_modifies_values() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        script.push(
            TableChange::new()
                .remove([1])
                .shift(5, 6, -3)
                .add([1, 9], vec![strings(["x", "y"]), longs([7, 8])])
                .modify([0, 3], [(1, longs([11, 41]))]),
        );
        graph.run_cycle();

        let rows = table.row_set();
        assert_eq!(*rows, RowSet::from_keys([0, 1, 2, 3, 9]));
        assert_eq!(
            string_values(&table.column(0).chunk(&rows).unwrap()),
            ["a", "x", "c", "d", "y"].map(Some)
        );
        assert_eq!(
            long_values(&table.column(1).chunk(&rows).unwrap()),
            [11, 7, 30, 41, 8].map(Some)
        );
        // Reads are by key, in key order, for any subset.
        assert_eq!(
            long_values(&table.column(1).chunk(&RowSet::from_keys([9, 2])).unwrap()),
            [30, 8].map(Some)
        );
    }

    /// Previous rows, previous Qty values, current rows, current Qty values.
    type Seen = (RowSet, Vec<Option<i64>>, RowSet, Vec<Option<i64>>);

    /// Reads the table's previous and current state when its update arrives.
    struct Observed {
        table: Arc<ReferenceTable>,
        seen: Mutex<Vec<Seen>>,
    }

    impl Dependency for Observed {
        fn satisfied(&self, step: u64) -> bool {
            self.table.satisfied(step)
        }
    }

    impl UpdateListener for Observed {
        fn on_update(&self, _update: &TableUpdate) -> Result<(), TableFailure> {
            let (prev, rows) = (self.table.prev_row_set(), self.table.row_set());
            let qty = self.table.column(1);
            lock(&self.seen).push((
                (*prev).clone(),
                long_values(&qty.prev_chunk(&prev)?),
                (*rows).clone(),
                long_values(&qty.chunk(&rows)?),
            ));
            Ok(())
        }
        fn on_failure(&self, _failure: &TableFailure) {}
    }

    #[test]
    fn previous_state_is_readable_during_the_cycle_and_gone_after() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        let observed = Arc::new(Observed {
            table: table.clone(),
            seen: Mutex::default(),
        });
        let listener: Arc<dyn UpdateListener> = observed.clone();
        table.add_listener(&listener);

        script.push(
            TableChange::new()
                .remove([0])
                .modify([5], [(1, longs([31]))]),
        );
        graph.run_cycle();
        assert_eq!(
            *lock(&observed.seen),
            [(
                RowSet::from_keys([0, 1, 5, 6]),
                [10, 20, 30, 40].map(Some).to_vec(),
                RowSet::from_keys([1, 5, 6]),
                [20, 31, 40].map(Some).to_vec(),
            )]
        );

        // Idle: previous equals current.
        assert_eq!(table.prev_row_set(), table.row_set());
        let prev = table.column(1).prev_chunk(&table.row_set()).unwrap();
        assert_eq!(long_values(&prev), [20, 31, 40].map(Some));

        // A cycle in which the table does not change has no distinct previous state either.
        struct Probe(Arc<ReferenceTable>);
        impl UpdateSource for Probe {
            fn refresh(&self) {
                assert_eq!(self.0.prev_row_set(), self.0.row_set());
                let prev = self.0.column(1).prev_chunk(&self.0.row_set()).unwrap();
                assert_eq!(long_values(&prev), [20, 31, 40].map(Some));
            }
        }
        let probe: Arc<dyn UpdateSource> = Arc::new(Probe(table.clone()));
        graph.add_source(&probe);
        graph.run_cycle();
        assert_eq!(lock(&observed.seen).len(), 1);
    }

    #[test]
    fn reading_rows_outside_the_row_set_is_an_error() {
        let graph = ReferenceGraph::new();
        let table = scripted(&graph).table();
        let failure = table.column(0).chunk(&RowSet::from_keys([1, 2])).err();
        assert!(failure.is_some_and(|f| f.message.contains("RowSet{2}")));
    }

    #[test]
    fn satisfied_once_notified_or_once_sources_have_refreshed() {
        /// Applies its change, if it has one, and records `satisfied` before and after.
        struct Check {
            table: Arc<ReferenceTable>,
            change: Mutex<Option<TableChange>>,
            seen: Mutex<Vec<bool>>,
        }
        impl UpdateSource for Check {
            fn refresh(&self) {
                let step = self.table.update_graph().unwrap().clock().step;
                lock(&self.seen).push(self.table.satisfied(step));
                if let Some(change) = lock(&self.change).take() {
                    self.table.apply(&change).unwrap();
                }
                lock(&self.seen).push(self.table.satisfied(step));
            }
        }
        let graph = ReferenceGraph::new();
        let empty = || vec![strings([]), longs([])];
        let table =
            ReferenceTable::new_refreshing(graph.clone(), sym_qty(), RowSet::empty(), empty());
        let check = Arc::new(Check {
            table: table.clone(),
            change: Mutex::new(Some(
                TableChange::new().add([3], vec![strings(["a"]), longs([1])]),
            )),
            seen: Mutex::default(),
        });
        let source: Arc<dyn UpdateSource> = check.clone();
        graph.add_source(&source);

        graph.run_cycle();
        assert_eq!(*lock(&check.seen), [false, true]);
        // No change in step 2: unsatisfied while sources refresh, satisfied afterwards.
        graph.run_cycle();
        assert_eq!(*lock(&check.seen), [false, true, false, false]);
        assert!(table.satisfied(2) && !table.satisfied(3));
        assert!(ReferenceTable::new_static(sym_qty(), RowSet::empty(), empty()).satisfied(9));
    }

    /// Applies each change inside a cycle and returns the results.
    fn apply_in_cycle(
        graph: &Arc<ReferenceGraph>,
        table: &Arc<ReferenceTable>,
        changes: Vec<TableChange>,
    ) -> Vec<Result<(), ChangeError>> {
        struct Apply {
            table: Arc<ReferenceTable>,
            changes: Vec<TableChange>,
            results: Mutex<Vec<Result<(), ChangeError>>>,
        }
        impl UpdateSource for Apply {
            fn refresh(&self) {
                let results = self.changes.iter().map(|c| self.table.apply(c));
                lock(&self.results).extend(results);
            }
        }
        let apply = Arc::new(Apply {
            table: table.clone(),
            changes,
            results: Mutex::default(),
        });
        let source: Arc<dyn UpdateSource> = apply.clone();
        graph.add_source(&source);
        graph.run_cycle();
        let results = lock(&apply.results).clone();
        results
    }

    #[test]
    fn invalid_changes_are_refused_and_leave_the_table_alone() {
        let graph = ReferenceGraph::new();
        let table = scripted(&graph).table();
        let before = table.row_set();
        let add = |values| TableChange::new().add([9], values);
        let results = apply_in_cycle(
            &graph,
            &table,
            vec![
                TableChange::new().remove([4]),
                add(vec![strings(["x"])]),
                add(vec![longs([1]), longs([1])]),
                add(vec![strings(["x", "y"]), longs([1, 2])]),
                TableChange::new().modify([0], [(1, strings(["x"]))]),
                TableChange::new().modify([0], [(1, longs([1, 2]))]),
                TableChange {
                    modified_values: BTreeMap::new(),
                    ..TableChange::new().modify([0], [(1, longs([1]))])
                },
                TableChange::new(),
            ],
        );
        assert!(matches!(
            results[0],
            Err(ChangeError::Update(UpdateError::RemovedNotPresent(_)))
        ));
        let messages = [
            "1 arrays for 2 columns",
            "column Sym needs Utf8 values but was given Int64",
            "column Sym has 2 values for 1 added rows",
            "column Qty needs Int64 values but was given Utf8",
            "column Qty has 2 values for 1 modified rows",
            "modified columns are [1] but values were given for []",
        ];
        for (result, message) in results[1..7].iter().zip(messages) {
            assert_eq!(result, &Err(ChangeError::Values(message.to_string())));
        }
        assert_eq!(results[7], Ok(()));
        assert_eq!(table.row_set(), before);
        assert_eq!(
            long_values(&table.column(1).chunk(&before).unwrap()),
            [10, 20, 30, 40].map(Some)
        );
        assert_eq!(
            table.apply(&TableChange::new().remove([0])),
            Err(ChangeError::NotUpdating)
        );
    }

    #[test]
    fn table_changes_at_most_once_per_step() {
        let graph = ReferenceGraph::new();
        let table = scripted(&graph).table();
        let results = apply_in_cycle(
            &graph,
            &table,
            vec![
                TableChange::new().remove([0]),
                TableChange::new().remove([1]),
            ],
        );
        assert_eq!(results, [Ok(()), Err(ChangeError::AlreadyNotified)]);
        assert_eq!(*table.row_set(), RowSet::from_keys([1, 5, 6]));
    }

    #[test]
    fn listeners_get_each_update_once_until_dropped() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        let (kept, dropped) = (
            Recorder::attach(table.clone()),
            Recorder::attach(table.clone()),
        );

        script.push(TableChange::new().remove([0]));
        graph.run_cycle();
        drop(dropped);
        script.push(TableChange::new());
        graph.run_cycle();
        script.push(TableChange::new().remove([1]));
        graph.run_cycle();

        let removed = |key| {
            Ok(TableUpdate {
                removed: RowSet::from_keys([key]),
                ..TableUpdate::empty()
            })
        };
        assert_eq!(kept.events(), [removed(0), removed(1)]);
        // The dropped listener is forgotten, not just skipped.
        assert_eq!(lock(&table.shared.inner).listeners.len(), 1);
    }

    #[test]
    fn failure_reaches_listeners_and_stops_the_table() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        let recorder = Recorder::attach(table.clone());
        script.push_failure("boom");
        graph.run_cycle();
        assert!(table.is_failed());
        assert_eq!(
            recorder.events(),
            [Err(TableFailure {
                message: "boom".into()
            })]
        );
        let results = apply_in_cycle(&graph, &table, vec![TableChange::new().remove([0])]);
        assert_eq!(results, [Err(ChangeError::Failed)]);
    }

    #[test]
    fn static_table_reads_values_and_ignores_listeners() {
        let table = ReferenceTable::new_static(
            sym_qty(),
            RowSet::from_keys([2, 4]),
            vec![strings(["a", "b"]), longs([1, 2])],
        );
        assert!(!table.is_refreshing() && table.update_graph().is_none());
        let _recorder = Recorder::attach(table.clone());
        assert!(lock(&table.shared.inner).listeners.is_empty());
        assert_eq!(table.prev_row_set(), table.row_set());
        let values = table.column(0).chunk(&RowSet::from_keys([4])).unwrap();
        assert_eq!(string_values(&values), [Some("b")]);
    }

    #[test]
    #[should_panic(expected = "column Qty has 1 values for 2 rows")]
    fn construction_rejects_columns_of_the_wrong_length() {
        ReferenceTable::new_static(
            sym_qty(),
            RowSet::from_keys([2, 4]),
            vec![strings(["a", "b"]), longs([1])],
        );
    }
}
