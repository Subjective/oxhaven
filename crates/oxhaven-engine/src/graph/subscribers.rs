//! The listener side of a table: who listens, and delivery of each step's update to them.

use super::stepped::NEVER;
use oxhaven_contracts::graph::{ClockPhase, Notification, UpdateGraph, UpdateSource};
use oxhaven_contracts::table::{TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};

/// A table's listeners and its delivery state. Every refreshing table owns one and forwards
/// `Table::add_listener`, its notifications and `Dependency::satisfied` to it.
///
/// # Why a table's `satisfied` must come from here
///
/// A listener learns that its parent ticked only when the graph runs the delivery; nothing
/// tells it that a delivery is queued. So a listener that has not run can call itself
/// satisfied only if "the parent is satisfied" already implies "no delivery to me is queued".
/// [`Subscribers::satisfied`] gives exactly that: after a table notifies, it is satisfied
/// once every delivery of that notification has run. Java tracks the queued delivery on the
/// listener instead (`InstrumentedTableListenerBase#satisfied`, `lastEnqueuedStep`), which
/// the listener contract here cannot express. A table that reports itself satisfied as soon
/// as it has queued its deliveries lets a merged listener run before its recorders.
pub struct Subscribers {
    inner: Arc<Inner>,
}

struct Inner {
    graph: Arc<dyn UpdateGraph>,
    listeners: Mutex<Vec<Weak<dyn UpdateListener>>>,
    delivery: Arc<DeliveryState>,
    failed: AtomicBool,
    /// A failure waiting for the next cycle, see [`Subscribers::notify_failure`]. The graph
    /// holds sources weakly; this is the owner.
    deferred_failure: Mutex<Option<Arc<dyn UpdateSource>>>,
}

struct DeliveryState {
    notified_step: AtomicU64,
    /// Deliveries queued and not yet finished.
    outstanding: AtomicUsize,
}

enum Event {
    Update(Arc<TableUpdate>),
    Failure(Arc<TableFailure>),
}

impl Subscribers {
    pub fn new(graph: Arc<dyn UpdateGraph>) -> Self {
        Self {
            inner: Arc::new(Inner {
                graph,
                listeners: Mutex::new(Vec::new()),
                delivery: Arc::new(DeliveryState {
                    notified_step: AtomicU64::new(NEVER),
                    outstanding: AtomicUsize::new(0),
                }),
                failed: AtomicBool::new(false),
                deferred_failure: Mutex::new(None),
            }),
        }
    }

    pub fn graph(&self) -> &Arc<dyn UpdateGraph> {
        &self.inner.graph
    }

    /// Registers a listener, held weakly. The implementation of `Table::add_listener`.
    ///
    /// A listener added during a cycle, after this table notified, misses that notification.
    pub fn add(&self, listener: &Arc<dyn UpdateListener>) {
        self.inner
            .listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::downgrade(listener));
    }

    /// Delivers `update` to every listener through the graph, after normalizing it. An update
    /// that normalizes to nothing is not delivered and does not count as a notification.
    /// Returns whether listeners were notified.
    ///
    /// The table must have applied the update to its own state before calling this.
    ///
    /// # Panics
    /// If the graph is idle, the table has failed, or it already notified on this step.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseTable#notifyListeners
    pub fn notify(&self, update: TableUpdate) -> bool {
        assert!(!self.is_failed(), "a failed table cannot notify listeners");
        let Some(update) = update.normalized() else {
            return false;
        };
        self.inner.deliver(Event::Update(Arc::new(update)));
        true
    }

    /// Fails the table: [`Subscribers::is_failed`] is true from now on, and every listener's
    /// `on_failure` is called through the graph.
    ///
    /// If the table already notified on this step, the failure is delivered at the start of
    /// the next cycle, because a table notifies at most once per step. This happens when an
    /// operator notifies its result and then fails in the same `on_update`. Has no effect on
    /// a table that has already failed.
    ///
    /// # Panics
    /// If the graph is idle.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseTable#notifyListenersOnError,
    /// io.deephaven.engine.table.impl.MergedListener#propagateErrorDownstream
    pub fn notify_failure(&self, failure: TableFailure) {
        if self.inner.failed.swap(true, Ordering::SeqCst) {
            return;
        }
        let failure = Arc::new(failure);
        let step = self.inner.graph.clock().step;
        if self.inner.delivery.notified_step.load(Ordering::SeqCst) != step {
            self.inner.deliver(Event::Failure(failure));
            return;
        }
        let source: Arc<dyn UpdateSource> = Arc::new(DeferredFailure {
            subscribers: Arc::downgrade(&self.inner),
            failure,
        });
        self.inner.graph.add_source(&source);
        *self
            .inner
            .deferred_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(source);
    }

    /// Whether [`Subscribers::notify_failure`] has been called. The implementation of
    /// `Table::is_failed`.
    pub fn is_failed(&self) -> bool {
        self.inner.failed.load(Ordering::SeqCst)
    }

    /// Whether the table notified its listeners on `step`.
    pub fn notified(&self, step: u64) -> bool {
        self.inner.delivery.notified_step.load(Ordering::SeqCst) == step
    }

    /// The implementation of `Dependency::satisfied` for the owning table.
    ///
    /// If the table notified on `step`, it is satisfied once all its listeners have been
    /// called. Otherwise it is satisfied when `upstream` says nothing can make it notify any
    /// more: for a table fed by an update source, `graph.sources_satisfied(step)`; for the
    /// result of an operation, the `satisfied` of the listener that feeds it.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseTable#satisfied
    pub fn satisfied(&self, step: u64, upstream: impl FnOnce() -> bool) -> bool {
        if self.notified(step) {
            self.inner.delivery.outstanding.load(Ordering::SeqCst) == 0
        } else {
            upstream()
        }
    }
}

impl Inner {
    fn deliver(&self, event: Event) {
        let clock = self.graph.clock();
        assert!(
            clock.phase == ClockPhase::Updating,
            "a table can only notify listeners while the update graph is updating"
        );
        let last = self
            .delivery
            .notified_step
            .swap(clock.step, Ordering::SeqCst);
        assert!(
            last != clock.step,
            "a table may notify its listeners only once per step"
        );
        let listeners: Vec<Arc<dyn UpdateListener>> = {
            let mut listeners = self
                .listeners
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            listeners.retain(|listener| listener.strong_count() > 0);
            listeners.iter().filter_map(Weak::upgrade).collect()
        };
        for listener in listeners {
            self.delivery.outstanding.fetch_add(1, Ordering::SeqCst);
            self.graph.add_notification(Box::new(Delivery {
                listener,
                event: match &event {
                    Event::Update(update) => Event::Update(update.clone()),
                    Event::Failure(failure) => Event::Failure(failure.clone()),
                },
                state: self.delivery.clone(),
            }));
        }
    }
}

/// One listener's call for one notification.
struct Delivery {
    listener: Arc<dyn UpdateListener>,
    event: Event,
    state: Arc<DeliveryState>,
}

impl Notification for Delivery {
    /// The table has already updated, which is all a delivery waits for.
    fn can_execute(&self, _step: u64) -> bool {
        true
    }

    /// Behavior: io.deephaven.engine.table.impl.InstrumentedTableListenerBase.NotificationBase#doRunInternal
    fn run(self: Box<Self>) {
        match &self.event {
            Event::Update(update) => {
                if let Err(failure) = self.listener.on_update(update) {
                    self.listener.on_failure(&failure);
                }
            }
            Event::Failure(failure) => self.listener.on_failure(failure),
        }
    }
}

impl Drop for Delivery {
    /// Runs when the delivery has finished, and also if it is discarded unrun, so that the
    /// count cannot stay raised.
    fn drop(&mut self) {
        self.state.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Delivers a failure on the cycle after the one that produced it.
///
/// Behavior: io.deephaven.engine.table.impl.MergedListener.DelayedErrorNotifier
struct DeferredFailure {
    subscribers: Weak<Inner>,
    failure: Arc<TableFailure>,
}

impl UpdateSource for DeferredFailure {
    fn refresh(&self) {
        let Some(subscribers) = self.subscribers.upgrade() else {
            return;
        };
        // Dropping the owner's reference removes this source from the graph. The graph keeps
        // it alive until this call returns.
        let pending = subscribers
            .deferred_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if pending.is_some() {
            subscribers.deliver(Event::Failure(self.failure.clone()));
        }
    }
}
