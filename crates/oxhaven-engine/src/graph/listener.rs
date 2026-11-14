//! The listener of an operation with one parent table.

use super::stepped::NEVER;
use oxhaven_contracts::graph::{Dependency, UpdateGraph};
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

/// What an operation with one parent does with its parent's changes. Implemented by the
/// operation's result (the table, or the state behind it).
pub trait UpdateHandler: Send + Sync {
    /// Applies the parent's update to the result and notifies the result's listeners. The
    /// parent's previous row set and previous values are readable. On `Err` the result must
    /// be left for `on_failure` to fail; it is called next with the same failure.
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure>;

    /// The parent failed, or `on_update` returned an error: fail the result, normally with
    /// [`Subscribers::notify_failure`](super::Subscribers::notify_failure). Called at most
    /// once.
    fn on_failure(&self, failure: &TableFailure);
}

/// The listener between one parent table and one result.
///
/// It applies the ownership rule of [`UpdateListener`]: it owns the parent, refers to the
/// result weakly, and is owned by the result. The result keeps the `Arc` returned by
/// [`ParentListener::subscribe`] and answers `Dependency::satisfied` for a step on which it
/// did not notify with this listener's `satisfied`:
///
/// ```text
/// let result = Arc::new_cyclic(|result: &Weak<MyTable>| MyTable {
///     listener: ParentListener::subscribe(parent, result.clone()),
///     subscribers: Subscribers::new(graph),
/// });
/// // impl Dependency for MyTable:
/// self.subscribers.satisfied(step, || self.listener.satisfied(step))
/// ```
///
/// Once the result is dropped the listener does nothing, and the parent forgets it.
///
/// Behavior: io.deephaven.engine.table.impl.BaseTable.ListenerImpl,
/// io.deephaven.engine.table.impl.InstrumentedTableListenerBase
pub struct ParentListener {
    parent: Arc<dyn Table>,
    /// `None` when the parent is static and so never notifies.
    graph: Option<Arc<dyn UpdateGraph>>,
    result: Weak<dyn UpdateHandler>,
    completed_step: AtomicU64,
    failed: AtomicBool,
}

impl ParentListener {
    /// Creates the listener and registers it with `parent`. Call it while holding the
    /// graph's shared lock, together with the read of the parent's state that the result
    /// starts from.
    pub fn subscribe(
        parent: Arc<dyn Table>,
        result: Weak<dyn UpdateHandler>,
    ) -> Arc<dyn UpdateListener> {
        let listener = Arc::new(ParentListener {
            graph: parent.update_graph(),
            parent,
            result,
            completed_step: AtomicU64::new(NEVER),
            failed: AtomicBool::new(false),
        });
        let registered: Arc<dyn UpdateListener> = listener.clone();
        listener.parent.add_listener(&registered);
        registered
    }

    fn complete(&self) {
        if let Some(graph) = &self.graph {
            self.completed_step
                .store(graph.clock().step, Ordering::SeqCst);
        }
    }
}

impl Dependency for ParentListener {
    /// True once the listener has been called on `step`, or once the parent is satisfied
    /// without having called it. The second case relies on the parent not being satisfied
    /// while a delivery is queued, see [`Subscribers`](super::Subscribers).
    ///
    /// Behavior: io.deephaven.engine.table.impl.InstrumentedTableListenerBase#satisfied
    fn satisfied(&self, step: u64) -> bool {
        self.completed_step.load(Ordering::SeqCst) == step || self.parent.satisfied(step)
    }
}

impl UpdateListener for ParentListener {
    /// Behavior: io.deephaven.engine.table.impl.InstrumentedTableListenerBase.NotificationBase#doRunInternal
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        let outcome = match self.result.upgrade() {
            Some(result) if !self.failed.load(Ordering::SeqCst) => result.on_update(update),
            _ => Ok(()),
        };
        self.complete();
        outcome
    }

    /// Behavior: io.deephaven.engine.table.impl.InstrumentedTableListenerBase.ErrorNotification#run
    fn on_failure(&self, failure: &TableFailure) {
        if !self.failed.swap(true, Ordering::SeqCst) {
            if let Some(result) = self.result.upgrade() {
                result.on_failure(failure);
            }
        }
        self.complete();
    }
}
