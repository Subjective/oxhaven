//! The update graph: the clock and scheduler for refreshing tables.
//!
//! Port of the roles of `io.deephaven.engine.updategraph.UpdateGraph`,
//! `io.deephaven.engine.updategraph.LogicalClock` and
//! `io.deephaven.engine.updategraph.NotificationQueue`.

use std::sync::Arc;

/// Port of `LogicalClock.State`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClockPhase {
    /// Between cycles. Previous values equal current values.
    Idle,
    /// Inside a cycle. Tables are changing and previous values are distinct.
    Updating,
}

/// A point in logical time. Port of the value returned by `LogicalClock#currentValue`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ClockValue {
    /// Increases by one at the start of each cycle.
    pub step: u64,
    pub phase: ClockPhase,
}

/// Something a notification can wait for within a cycle: a table, a listener, or the graph's
/// sources. Port of `NotificationQueue.Dependency`.
pub trait Dependency: Send + Sync {
    /// Whether this dependency has finished for `step`: it has produced its update, or it is
    /// known that it will not produce one. Once true for a step it stays true for that step.
    ///
    /// Port of `NotificationQueue.Dependency#satisfied`.
    fn satisfied(&self, step: u64) -> bool;
}

/// A root of the graph: something that produces changes from outside, such as an input table
/// or a Kafka consumer. Port of the `Runnable` sources registered with `UpdateGraph#addSource`.
pub trait UpdateSource: Send + Sync {
    /// Called once per cycle during the updating phase, before any listener runs. The source
    /// applies its pending changes and notifies its listeners through the graph.
    fn refresh(&self);
}

/// A unit of work scheduled within a cycle, normally the delivery of one update to one
/// listener. Port of `NotificationQueue.Notification`.
pub trait Notification: Send {
    /// Whether everything this notification depends on has finished for `step`. The graph
    /// does not run a notification until this is true.
    ///
    /// Port of `Notification#canExecute`.
    fn can_execute(&self, step: u64) -> bool;

    fn run(self: Box<Self>);
}

/// Drives cycles. Within one cycle every listener observes one consistent snapshot: all
/// sources refresh, then notifications run in dependency order, then the phase returns to idle.
pub trait UpdateGraph: Send + Sync {
    fn clock(&self) -> ClockValue;

    /// Port of `UpdateGraph#addSource`. The graph holds the source weakly.
    fn add_source(&self, source: &Arc<dyn UpdateSource>);

    /// Schedules work for the current cycle. Must be called during the updating phase.
    ///
    /// Port of `NotificationQueue#addNotification`.
    fn add_notification(&self, notification: Box<dyn Notification>);

    /// Whether every source has refreshed for `step`. A table fed directly by a source uses
    /// this to decide that it will not notify this step.
    ///
    /// Port of `UpdateGraph#satisfied`.
    fn sources_satisfied(&self, step: u64) -> bool;

    /// Runs one full cycle and returns when the graph is idle again. Takes the exclusive
    /// side of the graph lock for the whole cycle, so it never overlaps a
    /// `with_shared_lock` call.
    ///
    /// Port of the effect of `PeriodicUpdateGraph#requestRefresh` followed by one completed
    /// cycle. Tests and the differential harness use this to step the engine deterministically.
    fn run_cycle(&self);

    /// Runs `f` while holding the shared side of the graph lock: no cycle runs during `f`,
    /// and other shared holders may. Re-entrant, and a no-op wrapper on a thread that is
    /// already processing updates for this graph. Use [`locked`] to return a value.
    ///
    /// Port of `UpdateGraph#sharedLock` with `doLocked`.
    fn with_shared_lock(&self, f: &mut dyn FnMut());

    /// Whether the calling thread is running a cycle of this graph (a source refresh or a
    /// notification). Such a thread already sees a consistent state and must not wait for
    /// the lock.
    ///
    /// Port of `UpdateGraph#currentThreadProcessesUpdates`.
    fn current_thread_processes_updates(&self) -> bool;
}

/// Runs `f` under `graph`'s shared lock and returns its result.
pub fn locked<T>(graph: &dyn UpdateGraph, f: impl FnOnce() -> T) -> T {
    let mut f = Some(f);
    let mut out = None;
    graph.with_shared_lock(&mut || {
        if let Some(f) = f.take() {
            out = Some(f());
        }
    });
    match out {
        Some(value) => value,
        None => panic!("UpdateGraph::with_shared_lock did not run its closure"),
    }
}
