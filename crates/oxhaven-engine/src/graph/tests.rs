use super::*;
use oxhaven_contracts::graph::{
    locked, ClockPhase, ClockValue, Dependency, Notification, UpdateGraph, UpdateSource,
};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::table::{
    ColumnSource, Table, TableAttributes, TableFailure, UpdateListener,
};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

// ---------------------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------------------

/// What happened, in order, across every fake in a test.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<String>>>);

impl Log {
    fn push(&self, line: impl Into<String>) {
        self.0.lock().unwrap().push(line.into());
    }

    /// The lines logged since the last call.
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

fn adds(keys: impl IntoIterator<Item = RowKey>) -> TableUpdate {
    TableUpdate {
        added: RowSet::from_keys(keys),
        ..TableUpdate::empty()
    }
}

fn show(update: &TableUpdate) -> String {
    format!("+{:?}", update.added.iter().collect::<Vec<_>>())
}

struct FnSource<F>(F);

impl<F: Fn() + Send + Sync> UpdateSource for FnSource<F> {
    fn refresh(&self) {
        (self.0)()
    }
}

fn source(
    graph: &SteppedUpdateGraph,
    f: impl Fn() + Send + Sync + 'static,
) -> Arc<dyn UpdateSource> {
    let source: Arc<dyn UpdateSource> = Arc::new(FnSource(f));
    graph.add_source(&source);
    source
}

struct FnNotification<C, R> {
    can_execute: C,
    run: R,
}

impl<C: Fn(u64) -> bool + Send, R: FnOnce() + Send> Notification for FnNotification<C, R> {
    fn can_execute(&self, step: u64) -> bool {
        (self.can_execute)(step)
    }

    fn run(self: Box<Self>) {
        (self.run)()
    }
}

fn notification(
    can_execute: impl Fn(u64) -> bool + Send + 'static,
    run: impl FnOnce() + Send + 'static,
) -> Box<dyn Notification> {
    Box::new(FnNotification { can_execute, run })
}

/// What makes a fake table notify.
enum Feed {
    /// The table is an update source: it delivers the update queued with `FakeTable::queue`.
    Source,
    /// The table passes its parent's updates on.
    Parent(Arc<dyn UpdateListener>),
    /// The table notifies once per step with the rows any parent added.
    Merged(Arc<MergedListener>),
    /// The table never changes.
    Static,
}

/// A table with no data: only the listener plumbing that a real table must have.
struct FakeTable {
    name: &'static str,
    graph: Arc<dyn UpdateGraph>,
    definition: TableDefinition,
    subscribers: Subscribers,
    feed: Feed,
    log: Log,
    queued: Mutex<Option<TableUpdate>>,
    /// Do not pass updates on.
    swallow: AtomicBool,
    /// Return an error from the next update.
    fail: AtomicBool,
    /// Pass the update on even when failing.
    notify_then_fail: AtomicBool,
    /// Panic on the next update.
    panic: AtomicBool,
}

impl FakeTable {
    fn with_feed(
        graph: &Arc<SteppedUpdateGraph>,
        name: &'static str,
        log: &Log,
        feed: Feed,
    ) -> FakeTable {
        let graph: Arc<dyn UpdateGraph> = graph.clone();
        FakeTable {
            name,
            definition: TableDefinition::new(Vec::new()).unwrap(),
            subscribers: Subscribers::new(graph.clone()),
            graph,
            feed,
            log: log.clone(),
            queued: Mutex::new(None),
            swallow: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            notify_then_fail: AtomicBool::new(false),
            panic: AtomicBool::new(false),
        }
    }

    fn source(graph: &Arc<SteppedUpdateGraph>, name: &'static str, log: &Log) -> Arc<FakeTable> {
        let table = Arc::new(Self::with_feed(graph, name, log, Feed::Source));
        let as_source: Arc<dyn UpdateSource> = table.clone();
        graph.add_source(&as_source);
        table
    }

    fn fixed(graph: &Arc<SteppedUpdateGraph>, name: &'static str, log: &Log) -> Arc<FakeTable> {
        Arc::new(Self::with_feed(graph, name, log, Feed::Static))
    }

    fn derived(
        graph: &Arc<SteppedUpdateGraph>,
        name: &'static str,
        log: &Log,
        parent: &Arc<FakeTable>,
    ) -> Arc<FakeTable> {
        locked(&**graph, || {
            Arc::new_cyclic(|result: &Weak<FakeTable>| {
                let listener = ParentListener::subscribe(parent.clone(), result.clone());
                Self::with_feed(graph, name, log, Feed::Parent(listener))
            })
        })
    }

    fn merged(
        graph: &Arc<SteppedUpdateGraph>,
        name: &'static str,
        log: &Log,
        parents: &[&Arc<FakeTable>],
    ) -> Arc<FakeTable> {
        locked(&**graph, || {
            Arc::new_cyclic(|result: &Weak<FakeTable>| {
                let listener = MergedListener::subscribe(
                    graph.clone(),
                    parents
                        .iter()
                        .map(|p| -> Arc<dyn Table> { (*p).clone() })
                        .collect(),
                    Vec::new(),
                    result.clone(),
                );
                Self::with_feed(graph, name, log, Feed::Merged(listener))
            })
        })
    }

    /// Makes a source table deliver `update` on the next cycle.
    fn queue(&self, update: TableUpdate) {
        *self.queued.lock().unwrap() = Some(update);
    }

    fn failure(&self) -> TableFailure {
        TableFailure {
            message: format!("{} broke", self.name),
        }
    }
}

impl UpdateSource for FakeTable {
    fn refresh(&self) {
        if let Some(update) = self.queued.lock().unwrap().take() {
            self.subscribers.notify(update);
        }
    }
}

impl UpdateHandler for FakeTable {
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        self.log
            .push(format!("{} update {}", self.name, show(update)));
        if self.panic.swap(false, Ordering::SeqCst) {
            panic!("{} panicked", self.name);
        }
        let fail = self.fail.load(Ordering::SeqCst);
        if !self.swallow.load(Ordering::SeqCst)
            && (!fail || self.notify_then_fail.load(Ordering::SeqCst))
        {
            self.subscribers.notify(update.clone());
        }
        if fail {
            return Err(self.failure());
        }
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        self.log.push(format!("{} failed: {failure}", self.name));
        self.subscribers.notify_failure(failure.clone());
    }
}

impl MergedHandler for FakeTable {
    fn process(&self, updates: &[Option<Arc<TableUpdate>>]) -> Result<(), TableFailure> {
        let seen: Vec<String> = updates
            .iter()
            .map(|u| u.as_ref().map_or("-".to_string(), |u| show(u)))
            .collect();
        self.log
            .push(format!("{} process [{}]", self.name, seen.join(", ")));
        if self.fail.load(Ordering::SeqCst) {
            return Err(self.failure());
        }
        let mut added = RowSet::empty();
        for update in updates.iter().flatten() {
            added.insert(&update.added);
        }
        self.subscribers.notify(TableUpdate {
            added,
            ..TableUpdate::empty()
        });
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        UpdateHandler::on_failure(self, failure)
    }
}

impl Dependency for FakeTable {
    fn satisfied(&self, step: u64) -> bool {
        match &self.feed {
            Feed::Source => self
                .subscribers
                .satisfied(step, || self.graph.sources_satisfied(step)),
            Feed::Parent(listener) => self
                .subscribers
                .satisfied(step, || listener.satisfied(step)),
            Feed::Merged(listener) => self
                .subscribers
                .satisfied(step, || listener.satisfied(step)),
            Feed::Static => true,
        }
    }
}

impl Table for FakeTable {
    fn definition(&self) -> &TableDefinition {
        &self.definition
    }

    fn row_set(&self) -> Arc<RowSet> {
        Arc::new(RowSet::empty())
    }

    fn prev_row_set(&self) -> Arc<RowSet> {
        Arc::new(RowSet::empty())
    }

    fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
        panic!("a fake table has no column {index}")
    }

    fn is_refreshing(&self) -> bool {
        !matches!(self.feed, Feed::Static)
    }

    fn attributes(&self) -> TableAttributes {
        TableAttributes::default()
    }

    fn is_failed(&self) -> bool {
        self.subscribers.is_failed()
    }

    fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
        self.is_refreshing().then(|| self.graph.clone())
    }

    fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
        if self.is_refreshing() {
            self.subscribers.add(listener);
        }
    }
}

/// A listener at the end of a graph that logs what it is told.
struct Probe {
    name: &'static str,
    log: Log,
}

impl Dependency for Probe {
    fn satisfied(&self, _step: u64) -> bool {
        true
    }
}

impl UpdateListener for Probe {
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        self.log.push(format!("{} saw {}", self.name, show(update)));
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        self.log
            .push(format!("{} saw failure: {failure}", self.name));
    }
}

fn probe(table: &Arc<FakeTable>, name: &'static str, log: &Log) -> Arc<dyn UpdateListener> {
    let probe: Arc<dyn UpdateListener> = Arc::new(Probe {
        name,
        log: log.clone(),
    });
    table.add_listener(&probe);
    probe
}

// ---------------------------------------------------------------------------------------
// Clock, sources and the notification queue
// ---------------------------------------------------------------------------------------

#[test]
fn clock_steps_once_per_cycle_and_is_updating_only_inside_it() {
    let graph = SteppedUpdateGraph::new();
    let start = graph.clock();
    assert_eq!(start.phase, ClockPhase::Idle);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let (g, s) = (graph.clone(), seen.clone());
    let _source = source(&graph, move || {
        s.lock().unwrap().push(g.clock());
        let (g, s) = (g.clone(), s.clone());
        g.clone().add_notification(notification(
            |_| true,
            move || s.lock().unwrap().push(g.clock()),
        ));
    });

    graph.run_cycle();
    let updating = ClockValue {
        step: start.step + 1,
        phase: ClockPhase::Updating,
    };
    assert_eq!(*seen.lock().unwrap(), vec![updating, updating]);
    assert_eq!(
        graph.clock(),
        ClockValue {
            step: start.step + 1,
            phase: ClockPhase::Idle
        }
    );

    graph.run_cycle();
    assert_eq!(graph.clock().step, start.step + 2);
    assert_eq!(seen.lock().unwrap()[2].step, start.step + 2);
}

#[test]
fn sources_refresh_once_in_the_order_added_and_dropped_sources_are_forgotten() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let l = log.clone();
    let a = source(&graph, move || l.push("a"));
    let l = log.clone();
    let b = source(&graph, move || l.push("b"));
    let l = log.clone();
    let _c = source(&graph, move || l.push("c"));
    graph.add_source(&a);

    graph.run_cycle();
    assert_eq!(log.take(), ["a", "b", "c"]);

    drop(b);
    graph.run_cycle();
    assert_eq!(log.take(), ["a", "c"]);
}

#[test]
fn notifications_wait_until_every_source_has_refreshed() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let (g, l) = (graph.clone(), log.clone());
    let _first = source(&graph, move || {
        let step = g.clock().step;
        l.push(format!(
            "first refresh, sources satisfied: {}",
            g.sources_satisfied(step)
        ));
        let (g2, l2) = (g.clone(), l.clone());
        g.add_notification(notification(
            |_| true,
            move || {
                l2.push(format!(
                    "notification, sources satisfied: {}",
                    g2.sources_satisfied(step)
                ))
            },
        ));
    });
    let l = log.clone();
    let _second = source(&graph, move || l.push("second refresh"));

    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "first refresh, sources satisfied: false",
            "second refresh",
            "notification, sources satisfied: true"
        ]
    );
    assert!(!graph.sources_satisfied(graph.clock().step + 1));
}

#[test]
fn a_notification_runs_only_once_it_can_execute() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let (g, l) = (graph.clone(), log.clone());
    let _source = source(&graph, move || {
        let ready = Arc::new(AtomicBool::new(false));
        let (r, l1) = (ready.clone(), l.clone());
        g.add_notification(notification(
            move |_| r.load(Ordering::SeqCst),
            move || l1.push("waited"),
        ));
        // Two passes later, a notification makes the first one executable.
        let (g1, l2) = (g.clone(), l.clone());
        g.add_notification(notification(
            |_| true,
            move || {
                l2.push("first pass");
                let l3 = l2.clone();
                g1.add_notification(notification(
                    |_| true,
                    move || {
                        l3.push("second pass");
                        ready.store(true, Ordering::SeqCst);
                    },
                ));
            },
        ));
    });

    graph.run_cycle();
    assert_eq!(log.take(), ["first pass", "second pass", "waited"]);
}

#[test]
fn a_notification_that_can_never_run_fails_the_cycle_and_leaves_the_graph_usable() {
    let graph = SteppedUpdateGraph::new();
    let stuck = Arc::new(AtomicBool::new(true));
    let (g, s) = (graph.clone(), stuck.clone());
    let _source = source(&graph, move || {
        if s.load(Ordering::SeqCst) {
            g.add_notification(notification(|_| false, || ()));
        }
    });

    let outcome = catch_unwind(AssertUnwindSafe(|| graph.run_cycle()));
    let message = *outcome.unwrap_err().downcast::<String>().unwrap();
    assert!(message.contains("1 notifications are waiting"), "{message}");
    assert_eq!(graph.clock().phase, ClockPhase::Idle);
    assert!(!graph.current_thread_processes_updates());

    stuck.store(false, Ordering::SeqCst);
    let step = graph.clock().step;
    graph.run_cycle();
    assert_eq!(graph.clock().step, step + 1);
}

#[test]
#[should_panic(expected = "only be added while the update graph is updating")]
fn adding_a_notification_while_idle_panics() {
    let graph = SteppedUpdateGraph::new();
    graph.add_notification(notification(|_| true, || ()));
}

// ---------------------------------------------------------------------------------------
// Tables and listeners
// ---------------------------------------------------------------------------------------

#[test]
fn an_update_travels_down_a_chain_in_one_cycle() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    let b = FakeTable::derived(&graph, "b", &log, &a);
    let _probe = probe(&b, "probe", &log);

    s.queue(adds([1, 2]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        ["a update +[1, 2]", "b update +[1, 2]", "probe saw +[1, 2]"]
    );
    let step = graph.clock().step;
    assert!(s.satisfied(step) && a.satisfied(step) && b.satisfied(step));

    // Nothing queued: nobody is called, and everything is still satisfied for the new step.
    graph.run_cycle();
    assert!(log.take().is_empty());
    let step = graph.clock().step;
    assert!(s.satisfied(step) && a.satisfied(step) && b.satisfied(step));
    assert!(!b.satisfied(step + 1));
}

#[test]
fn a_table_is_not_satisfied_until_its_listeners_have_been_called() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);

    // Refreshes after `s`, so `s` has notified but the delivery to `a` has not run.
    let (g, l, s2, a2) = (graph.clone(), log.clone(), s.clone(), a.clone());
    let _observer = source(&graph, move || {
        let step = g.clock().step;
        l.push(format!("s {} a {}", s2.satisfied(step), a2.satisfied(step)));
    });

    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(log.take(), ["s false a false", "a update +[1]"]);

    // Without an update, both are unsatisfied only until the sources are done.
    graph.run_cycle();
    assert_eq!(log.take(), ["s false a false"]);
    assert!(a.satisfied(graph.clock().step));
}

#[test]
fn a_listener_is_satisfied_once_it_has_run_even_if_its_parent_is_not() {
    /// Logs whether a table is satisfied at the moment this listener is called.
    struct Asks {
        about: Arc<FakeTable>,
        log: Log,
    }
    impl Dependency for Asks {
        fn satisfied(&self, _step: u64) -> bool {
            true
        }
    }
    impl UpdateListener for Asks {
        fn on_update(&self, _update: &TableUpdate) -> Result<(), TableFailure> {
            let step = self.about.graph.clock().step;
            self.log.push(format!(
                "{} satisfied: {}",
                self.about.name,
                self.about.satisfied(step)
            ));
            Ok(())
        }
        fn on_failure(&self, _failure: &TableFailure) {}
    }

    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    a.swallow.store(true, Ordering::SeqCst);
    // Called after `a`'s listener, while `s` still has this delivery outstanding.
    let asks: Arc<dyn UpdateListener> = Arc::new(Asks {
        about: a.clone(),
        log: log.clone(),
    });
    s.add_listener(&asks);

    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(log.take(), ["a update +[1]", "a satisfied: true"]);
}

#[test]
fn an_update_recorded_in_a_cycle_cut_short_by_a_panic_is_not_seen_later() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let other = FakeTable::source(&graph, "other", &log);
    let _j = FakeTable::merged(&graph, "j", &log, &[&s, &other]);
    // Called after `j`'s recorder for `s`, in the same pass, before `j` runs.
    let bomb = FakeTable::derived(&graph, "bomb", &log, &s);
    bomb.panic.store(true, Ordering::SeqCst);

    s.queue(adds([1]));
    assert!(catch_unwind(AssertUnwindSafe(|| graph.run_cycle())).is_err());
    assert_eq!(log.take(), ["bomb update +[1]"]);

    other.queue(adds([2]));
    graph.run_cycle();
    assert_eq!(log.take(), ["j process [-, +[2]]"]);
}

#[test]
fn a_diamond_reaches_the_merged_listener_once_with_both_updates() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    let b = FakeTable::derived(&graph, "b", &log, &s);
    let j = FakeTable::merged(&graph, "j", &log, &[&a, &b]);
    let _probe = probe(&j, "probe", &log);

    s.queue(adds([7]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "a update +[7]",
            "b update +[7]",
            "j process [+[7], +[7]]",
            "probe saw +[7]"
        ]
    );
    assert!(j.satisfied(graph.clock().step));
}

#[test]
fn a_diamond_with_one_long_side_waits_for_the_long_side() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let short = FakeTable::derived(&graph, "short", &log, &s);
    let long1 = FakeTable::derived(&graph, "long1", &log, &s);
    let long2 = FakeTable::derived(&graph, "long2", &log, &long1);
    let long3 = FakeTable::derived(&graph, "long3", &log, &long2);
    let j = FakeTable::merged(&graph, "j", &log, &[&short, &long3]);
    let _probe = probe(&j, "probe", &log);

    s.queue(adds([7]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "short update +[7]",
            "long1 update +[7]",
            "long2 update +[7]",
            "long3 update +[7]",
            "j process [+[7], +[7]]",
            "probe saw +[7]"
        ]
    );
}

#[test]
fn a_parent_that_does_not_tick_is_reported_as_absent() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s1 = FakeTable::source(&graph, "s1", &log);
    let s2 = FakeTable::source(&graph, "s2", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s1);
    let b = FakeTable::derived(&graph, "b", &log, &s2);
    let j = FakeTable::merged(&graph, "j", &log, &[&a, &b]);
    let _probe = probe(&j, "probe", &log);

    s1.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        ["a update +[1]", "j process [+[1], -]", "probe saw +[1]"]
    );

    s2.queue(adds([2]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        ["b update +[2]", "j process [-, +[2]]", "probe saw +[2]"]
    );

    // Neither parent ticks: the merged listener does not run, and is satisfied.
    graph.run_cycle();
    assert!(log.take().is_empty());
    assert!(j.satisfied(graph.clock().step));

    s1.queue(adds([3]));
    s2.queue(adds([4]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "a update +[3]",
            "b update +[4]",
            "j process [+[3], +[4]]",
            "probe saw +[3, 4]"
        ]
    );
}

#[test]
fn a_merged_listener_waits_to_learn_that_a_slow_parent_will_not_tick() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let quick = FakeTable::derived(&graph, "quick", &log, &s);
    let slow1 = FakeTable::derived(&graph, "slow1", &log, &s);
    let slow2 = FakeTable::derived(&graph, "slow2", &log, &slow1);
    let filter = FakeTable::derived(&graph, "filter", &log, &slow2);
    let quiet = FakeTable::derived(&graph, "quiet", &log, &filter);
    let _j = FakeTable::merged(&graph, "j", &log, &[&quick, &quiet]);
    filter.swallow.store(true, Ordering::SeqCst);

    s.queue(adds([5]));
    graph.run_cycle();
    // `j` is queued as soon as `quick` ticks, but runs only after `filter` has been called
    // and so is known not to pass anything on to `quiet`.
    assert_eq!(
        log.take(),
        [
            "quick update +[5]",
            "slow1 update +[5]",
            "slow2 update +[5]",
            "filter update +[5]",
            "j process [+[5], -]"
        ]
    );
}

#[test]
fn a_failing_listener_fails_its_result_and_everything_downstream() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    let b = FakeTable::derived(&graph, "b", &log, &a);
    let healthy = FakeTable::derived(&graph, "healthy", &log, &s);
    let _probe = probe(&b, "probe", &log);
    a.fail.store(true, Ordering::SeqCst);

    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "a update +[1]",
            "a failed: a broke",
            "healthy update +[1]",
            "b failed: a broke",
            "probe saw failure: a broke"
        ]
    );
    assert!(a.is_failed() && b.is_failed());
    assert!(!s.is_failed() && !healthy.is_failed());
    let step = graph.clock().step;
    assert!(a.satisfied(step) && b.satisfied(step));

    // A failed table delivers nothing further; the rest of the graph carries on.
    s.queue(adds([2]));
    graph.run_cycle();
    assert_eq!(log.take(), ["healthy update +[2]"]);
    let step = graph.clock().step;
    assert!(a.satisfied(step) && b.satisfied(step));
}

#[test]
fn a_failed_parent_fails_a_merged_listener_without_processing() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    let b = FakeTable::derived(&graph, "b", &log, &s);
    let j = FakeTable::merged(&graph, "j", &log, &[&a, &b]);
    let _probe = probe(&j, "probe", &log);
    a.fail.store(true, Ordering::SeqCst);

    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "a update +[1]",
            "a failed: a broke",
            "b update +[1]",
            "j failed: a broke",
            "probe saw failure: a broke"
        ]
    );
    assert!(j.is_failed());

    s.queue(adds([2]));
    graph.run_cycle();
    assert_eq!(log.take(), ["b update +[2]"]);
    assert!(j.satisfied(graph.clock().step));
}

#[test]
fn a_failing_merged_process_fails_the_result() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let other = FakeTable::source(&graph, "other", &log);
    let j = FakeTable::merged(&graph, "j", &log, &[&s, &other]);
    let _probe = probe(&j, "probe", &log);
    j.fail.store(true, Ordering::SeqCst);

    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "j process [+[1], -]",
            "j failed: j broke",
            "probe saw failure: j broke"
        ]
    );

    other.queue(adds([2]));
    graph.run_cycle();
    assert!(log.take().is_empty());
}

#[test]
fn a_failure_after_notifying_is_delivered_on_the_next_cycle() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    let _probe = probe(&a, "probe", &log);
    a.fail.store(true, Ordering::SeqCst);
    a.notify_then_fail.store(true, Ordering::SeqCst);

    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(
        log.take(),
        ["a update +[1]", "a failed: a broke", "probe saw +[1]"]
    );
    assert!(a.is_failed());

    graph.run_cycle();
    assert_eq!(log.take(), ["probe saw failure: a broke"]);

    graph.run_cycle();
    assert!(log.take().is_empty());
}

#[test]
fn a_result_keeps_its_parents_alive_and_dropping_it_unsubscribes() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &s);
    let other = FakeTable::source(&graph, "other", &log);
    let j = FakeTable::merged(&graph, "j", &log, &[&a, &other]);
    let (weak_a, weak_other) = (Arc::downgrade(&a), Arc::downgrade(&other));
    drop((a, other));

    // Only `j` holds `a` and `other` now, and they still work, including as sources.
    weak_other.upgrade().unwrap().queue(adds([9]));
    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(log.take(), ["a update +[1]", "j process [+[1], +[9]]"]);

    drop(j);
    assert!(weak_a.upgrade().is_none() && weak_other.upgrade().is_none());
    s.queue(adds([2]));
    graph.run_cycle();
    assert!(log.take().is_empty());
}

#[test]
fn a_static_parent_never_ticks_and_is_always_satisfied() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let fixed = FakeTable::fixed(&graph, "fixed", &log);
    let s = FakeTable::source(&graph, "s", &log);
    let a = FakeTable::derived(&graph, "a", &log, &fixed);
    let j = FakeTable::merged(&graph, "j", &log, &[&fixed, &s]);

    let step = graph.clock().step;
    assert!(a.satisfied(step) && a.satisfied(step + 1));
    s.queue(adds([1]));
    graph.run_cycle();
    assert_eq!(log.take(), ["j process [-, +[1]]"]);
    assert!(a.satisfied(graph.clock().step) && j.satisfied(graph.clock().step));
}

#[test]
fn an_update_that_normalizes_to_nothing_is_not_a_notification() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let _probe = probe(&s, "probe", &log);

    // Modified rows without modified columns: nothing was modified.
    s.queue(TableUpdate {
        modified: RowSet::from_keys([1]),
        ..TableUpdate::empty()
    });
    graph.run_cycle();
    assert!(log.take().is_empty());
    assert!(!s.subscribers.notified(graph.clock().step));

    s.queue(TableUpdate {
        added: RowSet::from_keys([2]),
        modified_columns: ModifiedColumnSet::of([0]),
        ..TableUpdate::empty()
    });
    graph.run_cycle();
    assert_eq!(log.take(), ["probe saw +[2]"]);
    assert!(s.subscribers.notified(graph.clock().step));
}

#[test]
fn a_table_that_notifies_twice_in_one_step_panics() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    let s = FakeTable::source(&graph, "s", &log);
    let s2 = s.clone();
    let _again = source(&graph, move || {
        s2.subscribers.notify(adds([2]));
    });

    s.queue(adds([1]));
    let outcome = catch_unwind(AssertUnwindSafe(|| graph.run_cycle()));
    let message = *outcome.unwrap_err().downcast::<&str>().unwrap();
    assert_eq!(
        message,
        "a table may notify its listeners only once per step"
    );
}

#[test]
#[should_panic(expected = "only notify listeners while the update graph is updating")]
fn a_table_that_notifies_while_idle_panics() {
    let graph = SteppedUpdateGraph::new();
    let s = FakeTable::source(&graph, "s", &Log::default());
    s.subscribers.notify(adds([1]));
}

// ---------------------------------------------------------------------------------------
// The lock
// ---------------------------------------------------------------------------------------

/// Long enough for a thread that is not blocked to make progress.
const SETTLE: Duration = Duration::from_millis(100);

#[test]
fn the_shared_lock_is_reentrant_and_returns_the_closure_value() {
    let graph = SteppedUpdateGraph::new();
    let value = locked(&*graph, || locked(&*graph, || locked(&*graph, || 42)));
    assert_eq!(value, 42);
    // Every hold was released: a cycle can run.
    graph.run_cycle();
}

#[test]
fn a_cycle_waits_for_shared_holders_and_blocks_new_ones() {
    let graph = SteppedUpdateGraph::new();
    let start = graph.clock().step;
    let (held_tx, held_rx) = channel();
    let (release_tx, release_rx) = channel::<()>();

    let g = graph.clone();
    let reader = thread::spawn(move || {
        locked(&*g, || {
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            // Re-entry must not wait behind the cycle that is now waiting for this thread.
            locked(&*g, || g.clock())
        })
    });
    held_rx.recv().unwrap();

    let g = graph.clone();
    let cycle = thread::spawn(move || g.run_cycle());
    thread::sleep(SETTLE);
    assert_eq!(
        graph.clock().step,
        start,
        "the cycle ran under a shared hold"
    );

    let (late_tx, late_rx) = channel();
    let g = graph.clone();
    let late_reader = thread::spawn(move || late_tx.send(locked(&*g, || g.clock())).unwrap());
    thread::sleep(SETTLE);
    assert!(
        late_rx.try_recv().is_err(),
        "a new reader overtook the cycle"
    );

    release_tx.send(()).unwrap();
    let seen_by_reader = reader.join().unwrap();
    cycle.join().unwrap();
    late_reader.join().unwrap();
    assert_eq!(seen_by_reader.step, start);
    assert_eq!(
        late_rx.recv().unwrap(),
        ClockValue {
            step: start + 1,
            phase: ClockPhase::Idle
        }
    );
}

#[test]
fn the_shared_lock_waits_for_a_running_cycle() {
    let graph = SteppedUpdateGraph::new();
    let (inside_tx, inside_rx) = channel();
    let (release_tx, release_rx) = channel::<()>();
    let release_rx = Mutex::new(release_rx);
    let inside_tx = Mutex::new(inside_tx);
    let _source = source(&graph, move || {
        inside_tx.lock().unwrap().send(()).unwrap();
        release_rx.lock().unwrap().recv().unwrap();
    });

    let g = graph.clone();
    let cycle = thread::spawn(move || g.run_cycle());
    inside_rx.recv().unwrap();
    assert_eq!(graph.clock().phase, ClockPhase::Updating);
    assert!(!graph.current_thread_processes_updates());

    let (seen_tx, seen_rx) = channel();
    let g = graph.clone();
    let reader = thread::spawn(move || seen_tx.send(locked(&*g, || g.clock().phase)).unwrap());
    thread::sleep(SETTLE);
    assert!(seen_rx.try_recv().is_err(), "a reader ran during a cycle");

    release_tx.send(()).unwrap();
    cycle.join().unwrap();
    reader.join().unwrap();
    assert_eq!(seen_rx.recv().unwrap(), ClockPhase::Idle);
}

#[test]
fn the_thread_running_a_cycle_takes_the_shared_lock_without_waiting() {
    let graph = SteppedUpdateGraph::new();
    let log = Log::default();
    assert!(!graph.current_thread_processes_updates());
    let (g, l) = (graph.clone(), log.clone());
    let _source = source(&graph, move || {
        l.push(format!("refresh {}", g.current_thread_processes_updates()));
        let phase = locked(&*g, || g.clock().phase);
        l.push(format!("locked in refresh {phase:?}"));
        let (g2, l2) = (g.clone(), l.clone());
        g.add_notification(notification(
            |_| true,
            move || {
                let processes = locked(&*g2, || g2.current_thread_processes_updates());
                l2.push(format!("locked in notification {processes}"));
            },
        ));
    });

    graph.run_cycle();
    assert_eq!(
        log.take(),
        [
            "refresh true",
            "locked in refresh Updating",
            "locked in notification true"
        ]
    );
    assert!(!graph.current_thread_processes_updates());
}

#[test]
#[should_panic(expected = "while holding the shared lock")]
fn running_a_cycle_while_holding_the_shared_lock_panics() {
    let graph = SteppedUpdateGraph::new();
    locked(&*graph, || graph.run_cycle());
}

#[test]
#[should_panic(expected = "run_cycle called from inside an update cycle")]
fn running_a_cycle_from_inside_a_cycle_panics() {
    let graph = SteppedUpdateGraph::new();
    let g = graph.clone();
    let _source = source(&graph, move || g.run_cycle());
    graph.run_cycle();
}

#[test]
fn two_graphs_do_not_share_a_lock() {
    let one = SteppedUpdateGraph::new();
    let two = SteppedUpdateGraph::new();
    // Holding one graph's shared lock does not count as holding the other's.
    locked(&*one, || two.run_cycle());
    assert_eq!(two.clock().step, one.clock().step + 1);
}
