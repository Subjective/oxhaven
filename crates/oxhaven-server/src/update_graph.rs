//! The server's update graph: a stepped graph whose cycles one driver thread runs, on a
//! timer and on request.
//!
//! Port of the scheduling half of
//! `io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph`; the cycle itself is the
//! engine's [`SteppedUpdateGraph`].

use oxhaven_contracts::graph::{ClockValue, Notification, UpdateGraph, UpdateSource};
use oxhaven_engine::graph::SteppedUpdateGraph;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// The time from the start of one cycle to the start of the next, unless a cycle is
/// requested sooner.
///
/// Behavior: `PeriodicUpdateGraph#getDefaultTargetCycleDurationMillis` (the default of the
/// `PeriodicUpdateGraph.targetCycleDurationMillis` property)
pub const DEFAULT_TARGET_CYCLE_DURATION: Duration = Duration::from_millis(1000);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Default)]
struct Schedule {
    /// A cycle has been asked for and no cycle has started since.
    requested: bool,
    /// Cycles started and cycles finished. They differ while a cycle runs.
    started: u64,
    completed: u64,
    stopped: bool,
}

struct Driver {
    schedule: Mutex<Schedule>,
    /// Wakes the driver thread: a request, or the stop.
    wake: Condvar,
    /// Wakes the threads waiting for a cycle to finish.
    done: Condvar,
}

/// An update graph that runs its own cycles.
///
/// One thread, started by [`PeriodicUpdateGraph::start`], runs a cycle every target cycle
/// duration, measured from the start of the previous cycle, and sooner when one is
/// requested. Every other thread asks and waits: [`UpdateGraph::run_cycle`] here requests a
/// cycle and returns once a cycle that started after the request has finished, which is
/// what a writer to an input table needs.
///
/// The thread stops when the graph is dropped; tables keep their graph alive.
///
/// Behavior: `PeriodicUpdateGraph#requestRefresh`, `PeriodicUpdateGraph#waitForNextCycle`
pub struct PeriodicUpdateGraph {
    graph: Arc<SteppedUpdateGraph>,
    driver: Arc<Driver>,
}

impl PeriodicUpdateGraph {
    /// Starts the graph and its driver thread. With `target_cycle_duration` of `None`
    /// cycles run only on request, which is what tests want.
    pub fn start(target_cycle_duration: Option<Duration>) -> Arc<Self> {
        let graph = SteppedUpdateGraph::new();
        let driver = Arc::new(Driver {
            schedule: Mutex::new(Schedule::default()),
            wake: Condvar::new(),
            done: Condvar::new(),
        });
        let spawned = std::thread::Builder::new()
            .name("update-graph".to_string())
            .spawn({
                let graph = graph.clone();
                let driver = driver.clone();
                move || drive(&graph, &driver, target_cycle_duration)
            });
        if let Err(e) = spawned {
            panic!("cannot start the update graph thread: {e}");
        }
        Arc::new(Self { graph, driver })
    }

    /// Asks for a cycle to start now instead of at the end of the current period, without
    /// waiting for it. Requests made while no cycle has started since the last request
    /// are one request.
    ///
    /// Behavior: `PeriodicUpdateGraph#requestRefresh`
    pub fn request_cycle(&self) {
        lock(&self.driver.schedule).requested = true;
        self.driver.wake.notify_all();
    }

    /// How many cycles have finished.
    pub fn completed_cycles(&self) -> u64 {
        lock(&self.driver.schedule).completed
    }
}

impl Drop for PeriodicUpdateGraph {
    fn drop(&mut self) {
        lock(&self.driver.schedule).stopped = true;
        self.driver.wake.notify_all();
        self.driver.done.notify_all();
    }
}

/// The driver thread: waits for the next cycle's time or a request, runs the cycle, tells
/// the waiters.
fn drive(graph: &SteppedUpdateGraph, driver: &Driver, target: Option<Duration>) {
    let mut next = target.map(|target| Instant::now() + target);
    loop {
        {
            let mut schedule = lock(&driver.schedule);
            loop {
                if schedule.stopped {
                    return;
                }
                if schedule.requested {
                    break;
                }
                schedule = match next {
                    None => driver
                        .wake
                        .wait(schedule)
                        .unwrap_or_else(PoisonError::into_inner),
                    Some(next) => match next.checked_duration_since(Instant::now()) {
                        None | Some(Duration::ZERO) => break,
                        Some(remaining) => {
                            driver
                                .wake
                                .wait_timeout(schedule, remaining)
                                .unwrap_or_else(PoisonError::into_inner)
                                .0
                        }
                    },
                };
            }
            schedule.requested = false;
            schedule.started += 1;
        }
        let start = Instant::now();
        // A source or a notification that panics has already failed what it could; the
        // graph is idle again and the next cycle must still run.
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| graph.run_cycle())) {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("no message");
            tracing::error!("update graph cycle failed: {message}");
        }
        {
            let mut schedule = lock(&driver.schedule);
            schedule.completed = schedule.started;
        }
        driver.done.notify_all();
        next = target.map(|target| start + target);
    }
}

impl UpdateGraph for PeriodicUpdateGraph {
    fn clock(&self) -> ClockValue {
        self.graph.clock()
    }

    fn add_source(&self, source: &Arc<dyn UpdateSource>) {
        self.graph.add_source(source);
    }

    fn add_notification(&self, notification: Box<dyn Notification>) {
        self.graph.add_notification(notification);
    }

    fn sources_satisfied(&self, step: u64) -> bool {
        self.graph.sources_satisfied(step)
    }

    /// Requests a cycle and waits until a cycle that started after the request has
    /// finished. The cycle runs on the driver thread. A thread that holds the shared lock
    /// inside `oxhaven_engine::graph::yielding_to_cycles` gives its holds up while it waits
    /// and has them again when this returns.
    ///
    /// # Panics
    /// If called from inside a cycle, or by a thread inside `with_shared_lock` that is not
    /// inside `yielding_to_cycles`: the cycle it waits for could never run. If the graph
    /// has stopped.
    fn run_cycle(&self) {
        assert!(
            !self.graph.current_thread_processes_updates(),
            "run_cycle called from inside an update cycle"
        );
        self.graph.waiting_for_cycle(|| {
            let mut schedule = lock(&self.driver.schedule);
            // A cycle that is running now started before this request and may have missed
            // what the caller did, so only the next one counts.
            let wanted = schedule.started + 1;
            schedule.requested = true;
            self.driver.wake.notify_all();
            while schedule.completed < wanted {
                assert!(
                    !schedule.stopped,
                    "Cannot request refresh when UpdateGraph is no longer running."
                );
                schedule = self
                    .driver
                    .done
                    .wait(schedule)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        });
    }

    fn with_shared_lock(&self, f: &mut dyn FnMut()) {
        self.graph.with_shared_lock(f);
    }

    fn current_thread_processes_updates(&self) -> bool {
        self.graph.current_thread_processes_updates()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::record_batch::RecordBatch;
    use oxhaven_contracts::graph::locked;
    use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
    use oxhaven_contracts::table::Table;
    use oxhaven_engine::graph::yielding_to_cycles;
    use oxhaven_engine::table::append_only_input_table;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct Counter(AtomicU64);

    impl UpdateSource for Counter {
        fn refresh(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counted(graph: &PeriodicUpdateGraph) -> Arc<Counter> {
        let counter = Arc::new(Counter::default());
        let source: Arc<dyn UpdateSource> = counter.clone();
        graph.add_source(&source);
        counter
    }

    fn wait_for(what: &str, condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn without_a_period_a_cycle_runs_only_when_asked() {
        let graph = PeriodicUpdateGraph::start(None);
        let counter = counted(&graph);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
        assert_eq!(graph.completed_cycles(), 0);

        graph.run_cycle();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert_eq!(graph.completed_cycles(), 1);
        assert_eq!(graph.clock().step, 3);

        graph.run_cycle();
        assert_eq!(counter.0.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn cycles_run_on_the_period_without_being_asked() {
        let graph = PeriodicUpdateGraph::start(Some(Duration::from_millis(10)));
        let counter = counted(&graph);
        wait_for("three periodic cycles", || {
            counter.0.load(Ordering::SeqCst) >= 3
        });
    }

    #[test]
    fn a_request_does_not_wait_for_the_period() {
        let graph = PeriodicUpdateGraph::start(Some(Duration::from_secs(3600)));
        let counter = counted(&graph);
        graph.request_cycle();
        wait_for("the requested cycle", || graph.completed_cycles() == 1);
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);

        let started = Instant::now();
        graph.run_cycle();
        assert!(started.elapsed() < Duration::from_secs(60));
        assert_eq!(counter.0.load(Ordering::SeqCst), 2);
    }

    /// A source that, while refreshing, lets a test thread make its request mid-cycle.
    struct Slow {
        refreshes: AtomicU64,
        entered: std::sync::mpsc::Sender<()>,
        proceed: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl UpdateSource for Slow {
        fn refresh(&self) {
            if self.refreshes.fetch_add(1, Ordering::SeqCst) == 0 {
                let _ = self.entered.send(());
                let _ = lock(&self.proceed).recv();
            }
        }
    }

    #[test]
    fn a_request_made_during_a_cycle_waits_for_the_next_one() {
        let graph = PeriodicUpdateGraph::start(None);
        let (entered, has_entered) = std::sync::mpsc::channel();
        let (proceed, may_proceed) = std::sync::mpsc::channel();
        let slow = Arc::new(Slow {
            refreshes: AtomicU64::new(0),
            entered,
            proceed: Mutex::new(may_proceed),
        });
        let source: Arc<dyn UpdateSource> = slow.clone();
        graph.add_source(&source);

        graph.request_cycle();
        has_entered.recv().unwrap();
        // The first cycle is running. A waiter that asks now must see a second cycle.
        let waiter = std::thread::spawn({
            let graph = graph.clone();
            let slow = slow.clone();
            move || {
                graph.run_cycle();
                slow.refreshes.load(Ordering::SeqCst)
            }
        });
        std::thread::sleep(Duration::from_millis(50));
        proceed.send(()).unwrap();
        assert_eq!(waiter.join().unwrap(), 2);
    }

    #[test]
    fn no_cycle_overlaps_a_shared_lock_holder() {
        let graph = PeriodicUpdateGraph::start(Some(Duration::from_millis(1)));
        let counter = counted(&graph);
        wait_for("a first cycle", || counter.0.load(Ordering::SeqCst) >= 1);
        locked(graph.as_ref(), || {
            let before = counter.0.load(Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(40));
            assert_eq!(counter.0.load(Ordering::SeqCst), before);
        });
        let after = counter.0.load(Ordering::SeqCst);
        wait_for("cycles to resume", || {
            counter.0.load(Ordering::SeqCst) > after
        });
    }

    #[test]
    #[should_panic(expected = "while holding the update graph's shared lock")]
    fn waiting_for_a_cycle_under_the_shared_lock_is_refused() {
        let graph = PeriodicUpdateGraph::start(None);
        locked(graph.as_ref(), || graph.run_cycle());
    }

    #[test]
    fn a_yielding_holder_waits_for_a_cycle_and_holds_the_lock_again() {
        let graph = PeriodicUpdateGraph::start(None);
        let counter = counted(&graph);
        locked(graph.as_ref(), || {
            locked(graph.as_ref(), || {
                yielding_to_cycles(|| graph.run_cycle());
                assert_eq!(counter.0.load(Ordering::SeqCst), 1);
            });
            // The holds are back: a requested cycle waits for this thread.
            graph.request_cycle();
            std::thread::sleep(Duration::from_millis(60));
            assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        });
        wait_for("the cycle that waited for the holder", || {
            counter.0.load(Ordering::SeqCst) == 2
        });
    }

    #[test]
    fn a_yielding_holder_writes_to_an_input_table_and_reads_the_rows() {
        let graph = PeriodicUpdateGraph::start(None);
        let definition =
            TableDefinition::new(vec![ColumnDefinition::new("X", ColumnType::Int)]).unwrap();
        let table = append_only_input_table(graph.clone(), definition, None).unwrap();
        let input = table.input_table().unwrap();
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let rows = RecordBatch::try_from_iter([("X", values)]).unwrap();

        // Another holder of the shared lock does not stop the write for longer than it
        // holds the lock.
        let (held, is_held) = std::sync::mpsc::channel();
        let other = std::thread::spawn({
            let graph = graph.clone();
            move || {
                locked(graph.as_ref(), || {
                    held.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(60));
                });
            }
        });
        is_held.recv().unwrap();
        locked(graph.as_ref(), || {
            yielding_to_cycles(|| input.add(rows).unwrap());
            assert_eq!(table.row_set().size(), 3);
        });
        other.join().unwrap();
    }

    struct Panics(AtomicU64);

    impl UpdateSource for Panics {
        fn refresh(&self) {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("first refresh fails");
            }
        }
    }

    #[test]
    fn a_cycle_that_panics_does_not_stop_the_driver() {
        let graph = PeriodicUpdateGraph::start(None);
        let panics = Arc::new(Panics(AtomicU64::new(0)));
        let source: Arc<dyn UpdateSource> = panics.clone();
        graph.add_source(&source);
        graph.run_cycle();
        graph.run_cycle();
        assert_eq!(panics.0.load(Ordering::SeqCst), 2);
        assert_eq!(graph.completed_cycles(), 2);
    }

    #[test]
    fn an_input_table_write_waits_for_the_driver_to_apply_it() {
        let graph = PeriodicUpdateGraph::start(None);
        let definition =
            TableDefinition::new(vec![ColumnDefinition::new("X", ColumnType::Int)]).unwrap();
        let table = append_only_input_table(graph.clone(), definition, None).unwrap();
        let input = table.input_table().unwrap();
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let rows = RecordBatch::try_from_iter([("X", values)]).unwrap();

        // The writer does not run the cycle: the driver does, and the write returns once
        // the rows are in the table.
        input.add(rows).unwrap();
        assert_eq!(graph.completed_cycles(), 1);
        assert_eq!(locked(graph.as_ref(), || table.row_set().size()), 3);
    }

    #[test]
    fn dropping_the_graph_stops_its_thread() {
        let graph = PeriodicUpdateGraph::start(Some(Duration::from_millis(1)));
        let driver = Arc::downgrade(&graph.driver);
        drop(graph);
        // The thread holds the only other reference to the driver state.
        wait_for("the driver thread to exit", || driver.strong_count() == 0);
    }
}
