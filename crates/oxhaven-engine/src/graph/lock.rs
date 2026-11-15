//! The update graph's reader-writer lock.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

static NEXT_LOCK_ID: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// How many times this thread holds the shared side of each lock, by lock id.
    static SHARED_HOLDS: RefCell<BTreeMap<u64, usize>> = const { RefCell::new(BTreeMap::new()) };
    /// How many times this thread holds the exclusive side of each lock through
    /// [`GraphLock::acquire_exclusive`], by lock id. A cycle's own hold is not counted.
    static EXCLUSIVE_HOLDS: RefCell<BTreeMap<u64, usize>> = const { RefCell::new(BTreeMap::new()) };
    /// How many [`yielding_to_cycles`] calls this thread is inside.
    static YIELDING: Cell<usize> = const { Cell::new(0) };
}

/// Runs `f` as a holder of the shared lock that gives the lock up while it waits for a
/// cycle: inside `f`, `run_cycle` called by a thread that holds the shared side of a graph's
/// lock releases every hold the thread has, waits for the cycle, and takes the holds again
/// before it returns. Outside such a call, `run_cycle` under the shared lock panics, because
/// the cycle could never run.
///
/// A blocking write to an input table is such a wait. What the thread read from refreshing
/// tables before the wait may have changed after it, so only a caller that does not rely on
/// one snapshot across the write may use this: a console command, which in Java runs under
/// the exclusive lock and gives that up while it waits on the lock's condition.
///
/// Behavior: io.deephaven.engine.table.impl.util.BaseArrayBackedInputTable.ArrayBackedInputTableUpdater#waitForSequence
/// and io.deephaven.engine.table.impl.BaseTable#awaitUpdate
pub fn yielding_to_cycles<T>(f: impl FnOnce() -> T) -> T {
    struct Leave;
    impl Drop for Leave {
        fn drop(&mut self) {
            YIELDING.with(|depth| depth.set(depth.get() - 1));
        }
    }
    YIELDING.with(|depth| depth.set(depth.get() + 1));
    let _leave = Leave;
    f()
}

#[derive(Default)]
struct State {
    /// Threads holding the shared side.
    readers: usize,
    writer: bool,
    writers_waiting: usize,
}

/// A reader-writer lock whose shared side is re-entrant.
///
/// A thread that waits for the exclusive side stops new threads from taking the shared side,
/// so readers cannot starve the update cycle. A thread that already holds the shared side
/// may always take it again.
///
/// Behavior: io.deephaven.engine.updategraph.UpdateGraphLock
pub(super) struct GraphLock {
    id: u64,
    state: Mutex<State>,
    changed: Condvar,
}

impl GraphLock {
    pub(super) fn new() -> Self {
        Self {
            id: NEXT_LOCK_ID.fetch_add(1, Ordering::Relaxed),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // The state is three counters that every path leaves consistent, so a panic elsewhere
        // while it was held does not invalidate it.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn shared_holds(&self) -> usize {
        SHARED_HOLDS.with(|holds| holds.borrow().get(&self.id).copied().unwrap_or(0))
    }

    fn exclusive_holds(&self) -> usize {
        EXCLUSIVE_HOLDS.with(|holds| holds.borrow().get(&self.id).copied().unwrap_or(0))
    }

    /// Whether this thread holds the shared side.
    pub(super) fn holds_shared(&self) -> bool {
        self.shared_holds() > 0
    }

    /// Whether this thread holds the exclusive side through
    /// [`GraphLock::acquire_exclusive`].
    pub(super) fn holds_exclusive(&self) -> bool {
        self.exclusive_holds() > 0
    }

    /// Takes the shared side until [`GraphLock::release_shared`], for a holder that cannot
    /// be a scope of Rust code. Waits as [`GraphLock::shared`] does.
    pub(super) fn acquire_shared(&self) {
        std::mem::forget(self.shared());
    }

    /// Gives up one hold taken with [`GraphLock::acquire_shared`].
    ///
    /// # Panics
    /// If this thread holds neither side.
    pub(super) fn release_shared(&self) {
        if self.holds_exclusive() {
            // taken under the exclusive side, where the shared side takes nothing
            return;
        }
        assert!(
            self.holds_shared(),
            "the update graph's shared lock is released by a thread that does not hold it"
        );
        drop(SharedGuard(Some(self)));
    }

    /// Takes the exclusive side until [`GraphLock::release_exclusive`]: no cycle runs and
    /// no other thread holds either side. The thread may take both sides again while it
    /// holds it, and gives it up for the wait of [`GraphLock::released`].
    ///
    /// # Panics
    /// If this thread holds the shared side and not the exclusive one.
    ///
    /// Behavior: io.deephaven.engine.updategraph.UpdateGraphLock#exclusiveLock, a
    /// reentrant write lock under which the read lock can be taken
    pub(super) fn acquire_exclusive(&self) {
        if !self.holds_exclusive() {
            std::mem::forget(self.exclusive());
        }
        EXCLUSIVE_HOLDS.with(|holds| *holds.borrow_mut().entry(self.id).or_insert(0) += 1);
    }

    /// Gives up one hold taken with [`GraphLock::acquire_exclusive`].
    ///
    /// # Panics
    /// If this thread does not hold the exclusive side.
    pub(super) fn release_exclusive(&self) {
        let last = EXCLUSIVE_HOLDS.with(|holds| {
            let mut holds = holds.borrow_mut();
            match holds.get_mut(&self.id) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                Some(_) => {
                    holds.remove(&self.id);
                    true
                }
                None => panic!(
                    "the update graph's exclusive lock is released by a thread that does not \
                     hold it"
                ),
            }
        });
        if last {
            drop(ExclusiveGuard(self));
        }
    }

    /// Takes the shared side until the guard is dropped. Waits while a cycle runs or is
    /// waiting to run, unless this thread already holds the shared side. A thread that
    /// holds the exclusive side has what the shared side gives, and takes nothing.
    pub(super) fn shared(&self) -> SharedGuard<'_> {
        if self.holds_exclusive() {
            return SharedGuard(None);
        }
        if self.shared_holds() == 0 {
            let mut state = self.state();
            while state.writer || state.writers_waiting > 0 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            state.readers += 1;
        }
        SHARED_HOLDS.with(|holds| *holds.borrow_mut().entry(self.id).or_insert(0) += 1);
        SharedGuard(Some(self))
    }

    /// Runs `f`, which waits for a cycle, without this thread's holds of the shared side:
    /// they are released before `f` and taken again after it, also when `f` panics. Taking
    /// them again waits for a running or waiting cycle, as [`GraphLock::shared`] does. The
    /// holds of [`GraphLock::acquire_exclusive`] are given up and taken again the same way,
    /// as a wait on a condition of Java's exclusive lock gives that lock up.
    ///
    /// # Panics
    /// If this thread holds the shared side and is not inside [`yielding_to_cycles`].
    pub(super) fn released<T>(&self, f: impl FnOnce() -> T) -> T {
        struct RetakeExclusive<'a> {
            lock: &'a GraphLock,
            holds: usize,
        }
        impl Drop for RetakeExclusive<'_> {
            fn drop(&mut self) {
                std::mem::forget(self.lock.exclusive());
                EXCLUSIVE_HOLDS.with(|holds| holds.borrow_mut().insert(self.lock.id, self.holds));
            }
        }
        let exclusive = self.exclusive_holds();
        if exclusive > 0 {
            EXCLUSIVE_HOLDS.with(|holds| holds.borrow_mut().remove(&self.id));
            drop(ExclusiveGuard(self));
            let _retake = RetakeExclusive {
                lock: self,
                holds: exclusive,
            };
            return f();
        }

        struct Retake<'a> {
            lock: &'a GraphLock,
            holds: usize,
        }
        impl Drop for Retake<'_> {
            fn drop(&mut self) {
                let lock = self.lock;
                let mut state = lock.state();
                while state.writer || state.writers_waiting > 0 {
                    state = lock
                        .changed
                        .wait(state)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                state.readers += 1;
                drop(state);
                SHARED_HOLDS.with(|holds| holds.borrow_mut().insert(lock.id, self.holds));
            }
        }

        let holds = self.shared_holds();
        if holds == 0 {
            return f();
        }
        assert!(
            YIELDING.with(Cell::get) > 0,
            "run_cycle called while holding the update graph's shared lock"
        );
        SHARED_HOLDS.with(|holds| holds.borrow_mut().remove(&self.id));
        self.state().readers -= 1;
        self.changed.notify_all();
        let _retake = Retake { lock: self, holds };
        f()
    }

    /// Takes the exclusive side until the guard is dropped.
    ///
    /// # Panics
    /// If this thread holds the shared side: the lock cannot be upgraded, as in Java.
    pub(super) fn exclusive(&self) -> ExclusiveGuard<'_> {
        assert!(
            self.shared_holds() == 0,
            "cannot take the exclusive update graph lock while holding the shared lock"
        );
        let mut state = self.state();
        state.writers_waiting += 1;
        while state.writer || state.readers > 0 {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        state.writers_waiting -= 1;
        state.writer = true;
        ExclusiveGuard(self)
    }
}

/// A hold of the shared side; of nothing, for a thread that holds the exclusive side.
pub(super) struct SharedGuard<'a>(Option<&'a GraphLock>);

impl Drop for SharedGuard<'_> {
    fn drop(&mut self) {
        let Some(lock) = self.0 else {
            return;
        };
        let released = SHARED_HOLDS.with(|holds| {
            let mut holds = holds.borrow_mut();
            match holds.get_mut(&lock.id) {
                Some(count) if *count > 1 => {
                    *count -= 1;
                    false
                }
                _ => {
                    holds.remove(&lock.id);
                    true
                }
            }
        });
        if released {
            lock.state().readers -= 1;
            lock.changed.notify_all();
        }
    }
}

pub(super) struct ExclusiveGuard<'a>(&'a GraphLock);

impl Drop for ExclusiveGuard<'_> {
    fn drop(&mut self) {
        self.0.state().writer = false;
        self.0.changed.notify_all();
    }
}
