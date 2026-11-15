//! The update graph: clock, sources, notification queue and lock.

use super::lock::GraphLock;
use oxhaven_contracts::graph::{ClockPhase, ClockValue, Notification, UpdateGraph, UpdateSource};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::{self, ThreadId};

/// The step of a clock that has not run a cycle yet. No cycle ever has this step, so helpers
/// use a smaller value to mean "never".
///
/// Behavior: io.deephaven.engine.updategraph.LogicalClockImpl#INITIAL_CLOCK_VALUE
const INITIAL_STEP: u64 = 2;

/// A step value that no cycle has: the initial value of "the last step on which this happened".
pub(super) const NEVER: u64 = 0;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An update graph that advances only when [`UpdateGraph::run_cycle`] is called, and runs each
/// cycle on the calling thread in a fixed order.
///
/// A cycle refreshes every source in the order the sources were added, then runs
/// notifications. Notifications are examined in the order they were added; the ones whose
/// dependencies are satisfied run in that order, and the rest wait for the next pass together
/// with the notifications added meanwhile.
///
/// Behavior: io.deephaven.engine.updategraph.impl.BaseUpdateGraph#doRefresh,
/// io.deephaven.engine.updategraph.impl.BaseUpdateGraph#flushNormalNotificationsAndCompleteCycle
pub struct SteppedUpdateGraph {
    lock: GraphLock,
    clock: Mutex<ClockValue>,
    sources: Mutex<Vec<Weak<dyn UpdateSource>>>,
    pending: Mutex<Vec<Box<dyn Notification>>>,
    sources_satisfied_step: AtomicU64,
    cycle_thread: Mutex<Option<ThreadId>>,
}

impl SteppedUpdateGraph {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            lock: GraphLock::new(),
            clock: Mutex::new(ClockValue {
                step: INITIAL_STEP,
                phase: ClockPhase::Idle,
            }),
            sources: Mutex::new(Vec::new()),
            pending: Mutex::new(Vec::new()),
            sources_satisfied_step: AtomicU64::new(NEVER),
            cycle_thread: Mutex::new(None),
        })
    }

    /// The sources that are still alive, in the order they were added. Forgets the others.
    fn live_sources(&self) -> Vec<Arc<dyn UpdateSource>> {
        let mut sources = lock(&self.sources);
        sources.retain(|source| source.strong_count() > 0);
        sources.iter().filter_map(Weak::upgrade).collect()
    }

    /// Runs notifications until none is left.
    fn flush_notifications(&self, step: u64) {
        loop {
            let batch = std::mem::take(&mut *lock(&self.pending));
            if batch.is_empty() {
                return;
            }
            let (ready, waiting): (Vec<_>, Vec<_>) = batch
                .into_iter()
                .partition(|notification| notification.can_execute(step));
            assert!(
                !ready.is_empty(),
                "update graph step {step}: {} notifications are waiting on dependencies that \
                 nothing left in the cycle can satisfy",
                waiting.len()
            );
            // The waiting ones keep their place ahead of anything the ready ones add.
            lock(&self.pending).splice(0..0, waiting);
            for notification in ready {
                notification.run();
            }
        }
    }
}

impl SteppedUpdateGraph {
    /// Runs `wait`, which waits for a cycle of this graph that another thread runs, as
    /// [`UpdateGraph::run_cycle`] waits for its own: without this thread's holds of the
    /// shared lock when the thread is inside
    /// [`yielding_to_cycles`](super::yielding_to_cycles).
    ///
    /// # Panics
    /// If the thread holds the shared lock and is not inside `yielding_to_cycles`.
    pub fn waiting_for_cycle<T>(&self, wait: impl FnOnce() -> T) -> T {
        self.lock.released(wait)
    }

    /// One cycle, on this thread.
    fn cycle(&self) {
        let _exclusive = self.lock.exclusive();
        let step = {
            let mut clock = lock(&self.clock);
            clock.step += 1;
            clock.phase = ClockPhase::Updating;
            clock.step
        };
        *lock(&self.cycle_thread) = Some(thread::current().id());
        let _end = CycleEnd(self);

        // No notification runs until every source has refreshed.
        for source in self.live_sources() {
            source.refresh();
        }
        self.sources_satisfied_step.store(step, Ordering::SeqCst);
        self.flush_notifications(step);
    }
}

/// Returns the graph to idle when a cycle ends, normally or by a panic.
///
/// Behavior: io.deephaven.engine.updategraph.LogicalClockImpl#ensureUpdateCycleCompleted
struct CycleEnd<'a>(&'a SteppedUpdateGraph);

impl Drop for CycleEnd<'_> {
    fn drop(&mut self) {
        // Empty only after a panic; a notification must not survive into the next step.
        lock(&self.0.pending).clear();
        lock(&self.0.clock).phase = ClockPhase::Idle;
        *lock(&self.0.cycle_thread) = None;
    }
}

impl UpdateGraph for SteppedUpdateGraph {
    fn clock(&self) -> ClockValue {
        *lock(&self.clock)
    }

    /// Adding a source that is already registered has no effect. A source added during a
    /// cycle first refreshes on the next cycle.
    fn add_source(&self, source: &Arc<dyn UpdateSource>) {
        let source = Arc::downgrade(source);
        let mut sources = lock(&self.sources);
        if !sources.iter().any(|known| known.ptr_eq(&source)) {
            sources.push(source);
        }
    }

    /// # Panics
    /// If the graph is idle.
    fn add_notification(&self, notification: Box<dyn Notification>) {
        let mut pending = lock(&self.pending);
        assert!(
            self.clock().phase == ClockPhase::Updating,
            "a notification can only be added while the update graph is updating"
        );
        pending.push(notification);
    }

    fn sources_satisfied(&self, step: u64) -> bool {
        self.sources_satisfied_step.load(Ordering::SeqCst) == step
    }

    /// A thread that holds the shared lock inside
    /// [`yielding_to_cycles`](super::yielding_to_cycles) gives its holds up for the cycle and
    /// has them again when this returns.
    ///
    /// # Panics
    /// If called from inside a cycle of this graph, or by a thread that holds the shared
    /// lock and is not inside `yielding_to_cycles`. If notifications remain that can never
    /// run. A panic in a source or a notification propagates to the caller after the graph
    /// has returned to idle.
    fn run_cycle(&self) {
        assert!(
            !self.current_thread_processes_updates(),
            "run_cycle called from inside an update cycle"
        );
        self.lock.released(|| self.cycle());
    }

    fn with_shared_lock(&self, f: &mut dyn FnMut()) {
        if self.current_thread_processes_updates() {
            f();
        } else {
            let _shared = self.lock.shared();
            f();
        }
    }

    fn current_thread_processes_updates(&self) -> bool {
        *lock(&self.cycle_thread) == Some(thread::current().id())
    }
}
