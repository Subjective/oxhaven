//! The listener of an operation with several parent tables.

use super::stepped::NEVER;
use oxhaven_contracts::graph::{Dependency, Notification, UpdateGraph};
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What an operation with several parents does once per step in which any parent changed.
/// Implemented by the operation's result (the table, or the state behind it).
pub trait MergedHandler: Send + Sync {
    /// Applies the parents' updates to the result and notifies the result's listeners.
    /// `updates[i]` is the update of parent `i`, in the order the parents were given to
    /// [`MergedListener::subscribe`], or `None` if that parent did not change on this step.
    /// Every parent has finished updating. On `Err`, `on_failure` is called next with the
    /// same failure.
    fn process(&self, updates: &[Option<Arc<TableUpdate>>]) -> Result<(), TableFailure>;

    /// A parent failed, or `process` returned an error: fail the result, normally with
    /// [`Subscribers::notify_failure`](super::Subscribers::notify_failure). Called at most
    /// once.
    fn on_failure(&self, failure: &TableFailure);
}

/// The listener between several parent tables and one result.
///
/// Each parent gets a recorder, which keeps that parent's update for the step and asks for
/// the merged notification. The graph runs the notification once, after every parent and
/// every extra dependency is satisfied, however many parents changed. A result with more
/// than one refreshing parent must use this; separate listeners would make it notify more
/// than once per step.
///
/// Ownership follows the rule of [`UpdateListener`]: the result keeps the `Arc` returned by
/// [`MergedListener::subscribe`]; the merged listener owns its recorders, which own the
/// parents; the result is referred to weakly. The result answers `Dependency::satisfied` for
/// a step on which it did not notify with this listener's `satisfied`.
///
/// Behavior: io.deephaven.engine.table.impl.MergedListener,
/// io.deephaven.engine.table.impl.ListenerRecorder
pub struct MergedListener {
    graph: Arc<dyn UpdateGraph>,
    recorders: Vec<Arc<Recorder>>,
    dependencies: Vec<Arc<dyn Dependency>>,
    result: Weak<dyn MergedHandler>,
    state: Mutex<State>,
}

struct State {
    enqueued_step: u64,
    completed_step: u64,
    failed: bool,
    /// The first failure a parent reported on the enqueued step.
    upstream_failure: Option<TableFailure>,
}

impl MergedListener {
    /// Creates the listener and registers a recorder with each parent. Call it while holding
    /// the graph's shared lock, together with the read of the parents' state that the result
    /// starts from.
    ///
    /// `dependencies` are further things the notification waits for without listening to
    /// them, for example a table the operation reads but does not react to.
    pub fn subscribe(
        graph: Arc<dyn UpdateGraph>,
        parents: Vec<Arc<dyn Table>>,
        dependencies: Vec<Arc<dyn Dependency>>,
        result: Weak<dyn MergedHandler>,
    ) -> Arc<MergedListener> {
        let merged = Arc::new_cyclic(|merged: &Weak<MergedListener>| MergedListener {
            graph: graph.clone(),
            recorders: parents
                .into_iter()
                .map(|parent| {
                    Arc::new(Recorder {
                        graph: graph.clone(),
                        parent,
                        merged: merged.clone(),
                        recorded: Mutex::new(Recorded {
                            step: NEVER,
                            update: None,
                        }),
                    })
                })
                .collect(),
            dependencies,
            result,
            state: Mutex::new(State {
                enqueued_step: NEVER,
                completed_step: NEVER,
                failed: false,
                upstream_failure: None,
            }),
        });
        for recorder in &merged.recorders {
            let listener: Arc<dyn UpdateListener> = recorder.clone();
            recorder.parent.add_listener(&listener);
        }
        merged
    }

    /// Called by a recorder when its parent changed or failed: queues the merged
    /// notification unless it is already queued for this step.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MergedListener#notifyInternal
    fn parent_notified(self: &Arc<Self>, failure: Option<&TableFailure>) {
        let step = self.graph.clock().step;
        {
            let mut state = lock(&self.state);
            if state.failed {
                return;
            }
            assert!(
                state.completed_step != step,
                "a merged listener ran before all of its parents were satisfied"
            );
            if state.upstream_failure.is_none() {
                state.upstream_failure = failure.cloned();
            }
            if state.enqueued_step == step {
                return;
            }
            state.enqueued_step = step;
        }
        self.graph
            .add_notification(Box::new(MergedNotification(self.clone())));
    }

    /// Behavior: io.deephaven.engine.table.impl.MergedListener#canExecute
    fn can_execute(&self, step: u64) -> bool {
        self.recorders.iter().all(|r| r.satisfied(step))
            && self.dependencies.iter().all(|d| d.satisfied(step))
    }

    /// Behavior: io.deephaven.engine.table.impl.MergedListener.MergedNotification#run
    fn run(&self) {
        let step = self.graph.clock().step;
        let upstream_failure = {
            let mut state = lock(&self.state);
            assert!(
                state.enqueued_step == step && state.completed_step != step,
                "merged notification ran on a step it was not queued for"
            );
            state.upstream_failure.take()
        };
        let updates: Vec<Option<Arc<TableUpdate>>> =
            self.recorders.iter().map(|r| r.take(step)).collect();
        if let Some(result) = self.result.upgrade() {
            let outcome = match upstream_failure {
                Some(failure) => Err(failure),
                None => result.process(&updates),
            };
            if let Err(failure) = outcome {
                lock(&self.state).failed = true;
                result.on_failure(&failure);
            }
        }
        lock(&self.state).completed_step = step;
    }
}

impl Dependency for MergedListener {
    /// True once the merged notification has run on `step`, or once every parent and
    /// dependency is satisfied without having queued it.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MergedListener#satisfied
    fn satisfied(&self, step: u64) -> bool {
        let (completed_step, enqueued_step) = {
            let state = lock(&self.state);
            (state.completed_step, state.enqueued_step)
        };
        if completed_step == step {
            true
        } else if enqueued_step == step {
            false
        } else {
            self.can_execute(step)
        }
    }
}

struct MergedNotification(Arc<MergedListener>);

impl Notification for MergedNotification {
    fn can_execute(&self, step: u64) -> bool {
        self.0.can_execute(step)
    }

    fn run(self: Box<Self>) {
        self.0.run();
    }
}

/// Keeps one parent's update for the current step.
struct Recorder {
    graph: Arc<dyn UpdateGraph>,
    parent: Arc<dyn Table>,
    merged: Weak<MergedListener>,
    recorded: Mutex<Recorded>,
}

struct Recorded {
    /// The step on which `update` was recorded. An update left over from a cycle that was
    /// cut short by a panic must not be taken for a later step's.
    step: u64,
    update: Option<Arc<TableUpdate>>,
}

impl Recorder {
    fn record(&self, update: Option<Arc<TableUpdate>>, failure: Option<&TableFailure>) {
        *lock(&self.recorded) = Recorded {
            step: self.graph.clock().step,
            update,
        };
        if let Some(merged) = self.merged.upgrade() {
            merged.parent_notified(failure);
        }
    }

    /// The update recorded on `step`, if any. The recorder does not keep it.
    fn take(&self, step: u64) -> Option<Arc<TableUpdate>> {
        let mut recorded = lock(&self.recorded);
        let update = recorded.update.take();
        update.filter(|_| recorded.step == step)
    }
}

impl Dependency for Recorder {
    /// A parent is not satisfied while a delivery to this recorder is queued (see
    /// [`Subscribers`](super::Subscribers)), so once it is, the recorder has recorded the
    /// parent's notification for `step` or there is none.
    ///
    /// Behavior: io.deephaven.engine.table.impl.ListenerRecorder#canExecute
    fn satisfied(&self, step: u64) -> bool {
        self.parent.satisfied(step)
    }
}

impl UpdateListener for Recorder {
    /// Behavior: io.deephaven.engine.table.impl.ListenerRecorder#onUpdate
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        self.record(Some(Arc::new(update.clone())), None);
        Ok(())
    }

    /// Behavior: io.deephaven.engine.table.impl.ListenerRecorder#onFailureInternal
    fn on_failure(&self, failure: &TableFailure) {
        self.record(None, Some(failure));
    }
}
