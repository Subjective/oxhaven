//! A listener that records what it receives.

use crate::graph::lock;
use oxhaven_contracts::graph::Dependency;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use std::sync::{Arc, Mutex};

/// Records every update and failure a table delivers, for tests to assert on.
///
/// Nothing should wait on a recorder as a [`Dependency`]: see [`listener_satisfied`].
pub struct Recorder {
    table: Arc<dyn Table>,
    events: Mutex<Vec<Result<TableUpdate, TableFailure>>>,
    ran_step: Mutex<Option<u64>>,
}

impl Recorder {
    /// Starts recording `table`. Recording stops when the returned value is dropped.
    pub fn attach(table: Arc<dyn Table>) -> Arc<Self> {
        let recorder = Arc::new(Self {
            table,
            events: Mutex::default(),
            ran_step: Mutex::default(),
        });
        let listener: Arc<dyn UpdateListener> = recorder.clone();
        recorder.table.add_listener(&listener);
        recorder
    }

    /// Everything received so far, oldest first.
    pub fn events(&self) -> Vec<Result<TableUpdate, TableFailure>> {
        lock(&self.events).clone()
    }

    fn record(&self, event: Result<TableUpdate, TableFailure>) {
        lock(&self.events).push(event);
        *lock(&self.ran_step) = current_step(&*self.table);
    }
}

/// The step of `table`'s graph, or `None` for a static table.
pub(crate) fn current_step(table: &dyn Table) -> Option<u64> {
    table.update_graph().map(|graph| graph.clock().step)
}

/// Whether a listener of `table` that last ran in `ran_step` is satisfied for `step`.
///
/// This is true once the listener has run, which is exact, or once its table is satisfied,
/// which is too early when the table has notified and the delivery is still queued: the
/// contracts give a listener no way to see that it is queued. The listeners in this crate are
/// leaves that nothing waits on, so the early answer is harmless here.
pub(crate) fn listener_satisfied(table: &dyn Table, ran_step: Option<u64>, step: u64) -> bool {
    ran_step == Some(step) || table.satisfied(step)
}

impl Dependency for Recorder {
    fn satisfied(&self, step: u64) -> bool {
        listener_satisfied(&*self.table, *lock(&self.ran_step), step)
    }
}

impl UpdateListener for Recorder {
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        self.record(Ok(update.clone()));
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        self.record(Err(failure.clone()));
    }
}
