//! A single-threaded update graph: sources refresh, then notifications run until none are left.

use oxhaven_contracts::graph::{ClockPhase, ClockValue, Notification, UpdateGraph, UpdateSource};
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::{self, ThreadId};

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The reference [`UpdateGraph`]. A cycle runs entirely on the thread that calls
/// [`UpdateGraph::run_cycle`]: every live source refreshes in registration order, then queued
/// notifications run, oldest executable first, until the queue is empty.
///
/// The graph lock is one re-entrant exclusive lock, so shared holders on different threads
/// take turns. The contract allows that, and it keeps the model obvious.
pub struct ReferenceGraph {
    state: Mutex<State>,
    lock: Mutex<LockState>,
    released: Condvar,
}

struct State {
    step: u64,
    phase: ClockPhase,
    sources: Vec<Weak<dyn UpdateSource>>,
    pending: VecDeque<Box<dyn Notification>>,
    /// The last step for which every source has refreshed.
    refreshed_step: u64,
    cycle_thread: Option<ThreadId>,
}

struct LockState {
    holder: Option<ThreadId>,
    depth: usize,
}

struct LockGuard<'a>(&'a ReferenceGraph);

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.0.lock);
        state.depth -= 1;
        if state.depth == 0 {
            state.holder = None;
            self.0.released.notify_all();
        }
    }
}

/// Returns the graph to idle when a cycle ends, including by a panic in a source or listener.
struct CycleGuard<'a>(&'a ReferenceGraph);

impl Drop for CycleGuard<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.0.state);
        state.phase = ClockPhase::Idle;
        state.cycle_thread = None;
        state.pending.clear();
    }
}

impl ReferenceGraph {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                step: 0,
                phase: ClockPhase::Idle,
                sources: Vec::new(),
                pending: VecDeque::new(),
                refreshed_step: 0,
                cycle_thread: None,
            }),
            lock: Mutex::new(LockState {
                holder: None,
                depth: 0,
            }),
            released: Condvar::new(),
        })
    }

    fn acquire(&self, for_cycle: bool) -> LockGuard<'_> {
        let me = thread::current().id();
        let mut state = lock(&self.lock);
        if for_cycle && state.holder == Some(me) {
            drop(state);
            panic!("run_cycle called while this thread holds the graph lock");
        }
        while state.holder.is_some_and(|holder| holder != me) {
            state = self
                .released
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.holder = Some(me);
        state.depth += 1;
        LockGuard(self)
    }
}

impl UpdateGraph for ReferenceGraph {
    fn clock(&self) -> ClockValue {
        let state = lock(&self.state);
        ClockValue {
            step: state.step,
            phase: state.phase,
        }
    }

    fn add_source(&self, source: &Arc<dyn UpdateSource>) {
        lock(&self.state).sources.push(Arc::downgrade(source));
    }

    fn add_notification(&self, notification: Box<dyn Notification>) {
        let mut state = lock(&self.state);
        if state.phase != ClockPhase::Updating {
            drop(state);
            panic!("add_notification called outside a cycle");
        }
        state.pending.push_back(notification);
    }

    fn sources_satisfied(&self, step: u64) -> bool {
        step <= lock(&self.state).refreshed_step
    }

    fn run_cycle(&self) {
        let _lock = self.acquire(true);
        let (step, sources) = {
            let mut state = lock(&self.state);
            state.step += 1;
            state.phase = ClockPhase::Updating;
            state.cycle_thread = Some(thread::current().id());
            state.sources.retain(|source| source.strong_count() > 0);
            let sources: Vec<_> = state.sources.iter().filter_map(Weak::upgrade).collect();
            (state.step, sources)
        };
        let _cycle = CycleGuard(self);

        for source in sources {
            source.refresh();
        }
        lock(&self.state).refreshed_step = step;

        let mut queue: VecDeque<Box<dyn Notification>> = VecDeque::new();
        loop {
            queue.append(&mut lock(&self.state).pending);
            if queue.is_empty() {
                break;
            }
            // `can_execute` may ask this graph questions, so it runs without the state lock.
            let ready = queue.iter().position(|n| n.can_execute(step));
            match ready.and_then(|i| queue.remove(i)) {
                Some(notification) => notification.run(),
                None => panic!(
                    "step {step}: {} notifications are waiting on dependencies that will never \
                     be satisfied",
                    queue.len()
                ),
            }
        }
    }

    fn with_shared_lock(&self, f: &mut dyn FnMut()) {
        let _lock = self.acquire(false);
        f();
    }

    fn current_thread_processes_updates(&self) -> bool {
        lock(&self.state).cycle_thread == Some(thread::current().id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxhaven_contracts::graph::locked;
    use std::sync::atomic::{AtomicBool, Ordering};

    type Log = Arc<Mutex<Vec<String>>>;

    /// Logs when it runs, and can be made to wait for a flag.
    struct Note {
        log: Log,
        name: &'static str,
        gate: Option<Arc<AtomicBool>>,
        opens: Option<Arc<AtomicBool>>,
    }

    impl Notification for Note {
        fn can_execute(&self, _step: u64) -> bool {
            self.gate.as_ref().is_none_or(|g| g.load(Ordering::SeqCst))
        }
        fn run(self: Box<Self>) {
            lock(&self.log).push(self.name.to_string());
            if let Some(flag) = &self.opens {
                flag.store(true, Ordering::SeqCst);
            }
        }
    }

    struct Source {
        graph: Arc<ReferenceGraph>,
        log: Log,
        name: &'static str,
        notes: Mutex<Vec<Note>>,
    }

    impl UpdateSource for Source {
        fn refresh(&self) {
            let clock = self.graph.clock();
            assert_eq!(clock.phase, ClockPhase::Updating);
            assert!(self.graph.current_thread_processes_updates());
            assert!(!self.graph.sources_satisfied(clock.step));
            lock(&self.log).push(format!("{} refresh {}", self.name, clock.step));
            for note in lock(&self.notes).drain(..) {
                self.graph.add_notification(Box::new(note));
            }
        }
    }

    fn source(
        graph: &Arc<ReferenceGraph>,
        log: &Log,
        name: &'static str,
        notes: Vec<Note>,
    ) -> Arc<dyn UpdateSource> {
        let source: Arc<dyn UpdateSource> = Arc::new(Source {
            graph: graph.clone(),
            log: log.clone(),
            name,
            notes: Mutex::new(notes),
        });
        graph.add_source(&source);
        source
    }

    fn note(log: &Log, name: &'static str) -> Note {
        Note {
            log: log.clone(),
            name,
            gate: None,
            opens: None,
        }
    }

    fn idle(step: u64) -> ClockValue {
        ClockValue {
            step,
            phase: ClockPhase::Idle,
        }
    }

    #[test]
    fn cycle_refreshes_every_source_before_any_notification() {
        let graph = ReferenceGraph::new();
        let log = Log::default();
        let _a = source(&graph, &log, "a", vec![note(&log, "n1")]);
        let _b = source(&graph, &log, "b", vec![note(&log, "n2")]);
        assert_eq!(graph.clock(), idle(0));

        graph.run_cycle();
        assert_eq!(
            *lock(&log),
            ["a refresh 1", "b refresh 1", "n1", "n2"].map(String::from)
        );
        assert_eq!(graph.clock(), idle(1));
        assert!(graph.sources_satisfied(1) && !graph.sources_satisfied(2));
        assert!(!graph.current_thread_processes_updates());
    }

    #[test]
    fn notification_waits_until_it_can_execute() {
        let graph = ReferenceGraph::new();
        let log = Log::default();
        let flag = Arc::new(AtomicBool::new(false));
        let waiting = Note {
            gate: Some(flag.clone()),
            ..note(&log, "waits")
        };
        let opener = Note {
            opens: Some(flag),
            ..note(&log, "opens")
        };
        let _s = source(&graph, &log, "s", vec![waiting, opener]);
        graph.run_cycle();
        assert_eq!(
            *lock(&log),
            ["s refresh 1", "opens", "waits"].map(String::from)
        );
    }

    #[test]
    fn dropped_source_no_longer_refreshes() {
        let graph = ReferenceGraph::new();
        let log = Log::default();
        let _kept = source(&graph, &log, "kept", vec![]);
        let dropped = source(&graph, &log, "dropped", vec![]);
        drop(dropped);
        graph.run_cycle();
        assert_eq!(*lock(&log), ["kept refresh 1"].map(String::from));
    }

    fn graph_with_stuck_notification() -> (Arc<ReferenceGraph>, Arc<dyn UpdateSource>) {
        let graph = ReferenceGraph::new();
        let log = Log::default();
        let stuck = Note {
            gate: Some(Arc::new(AtomicBool::new(false))),
            ..note(&log, "stuck")
        };
        let source = source(&graph, &log, "s", vec![stuck]);
        (graph, source)
    }

    #[test]
    #[should_panic(expected = "1 notifications are waiting")]
    fn notification_that_can_never_execute_is_reported() {
        let (graph, _source) = graph_with_stuck_notification();
        graph.run_cycle();
    }

    #[test]
    #[should_panic(expected = "outside a cycle")]
    fn notification_outside_a_cycle_is_rejected() {
        let graph = ReferenceGraph::new();
        graph.add_notification(Box::new(note(&Log::default(), "idle")));
    }

    #[test]
    fn graph_is_idle_and_usable_after_a_panicking_cycle() {
        let (graph, _source) = graph_with_stuck_notification();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| graph.run_cycle()));
        assert!(result.is_err());
        assert_eq!(graph.clock(), idle(1));
        graph.run_cycle();
        assert_eq!(graph.clock(), idle(2));
    }

    #[test]
    fn shared_lock_is_reentrant_and_excludes_cycles_on_other_threads() {
        let graph = ReferenceGraph::new();
        assert_eq!(locked(&*graph, || locked(&*graph, || 7)), 7);

        let cycled = Arc::new(AtomicBool::new(false));
        let worker = locked(&*graph, || {
            let (g, c) = (graph.clone(), cycled.clone());
            let worker = thread::spawn(move || {
                g.run_cycle();
                c.store(true, Ordering::SeqCst);
            });
            thread::sleep(std::time::Duration::from_millis(50));
            assert!(!cycled.load(Ordering::SeqCst), "cycle ran under the lock");
            worker
        });
        worker.join().unwrap();
        assert!(cycled.load(Ordering::SeqCst));
    }
}
