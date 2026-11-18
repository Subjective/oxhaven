//! Listeners, the graph lock, liveness scopes and `await_update`, through the console of a
//! running server. The Python here imports the standard library only, so the values of an
//! update are lists or numpy arrays, whichever the interpreter of the test has; `vals`
//! makes them lists. What Java answers was observed on Java 42.9
//! (`tasks/6.12-python-listeners-contexts.md`); the scenario
//! `oracle/scenarios/python/listen_counts.py` is replayed against the recording.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use oxhaven_contracts::graph::UpdateGraph;

use super::super::tests::Python;

const SETUP: &str = r#"
import threading, time
from deephaven import DHError, dtypes, empty_table, input_table, new_table
from deephaven import update_graph as ug
from deephaven.column import int_col, string_col
from deephaven.execution_context import ExecutionContext, get_exec_ctx, make_user_exec_ctx
from deephaven.liveness_scope import LivenessScope, is_liveness_referent, liveness_scope
from deephaven.table_listener import (
    MergedListener, TableListener, TableUpdate, listen, merged_listen,
)
def kv(keys, values):
    return new_table([int_col('K', keys), int_col('V', values)])
def vals(rows):
    return {name: [int(v) for v in values] for name, values in rows.items()}
def raised(call):
    try:
        call()
    except BaseException as e:
        return type(e).__name__, str(e)
    return None
def on_thread(call):
    out = []
    def body():
        try:
            out.append(call())
        except BaseException as e:
            out.append((type(e).__name__, str(e)))
    thread = threading.Thread(target=body)
    thread.start()
    thread.join(60)
    assert not thread.is_alive()
    return out[0]
"#;

/// The text of the error the code fails with.
fn error(python: &Python, code: &str) -> String {
    let error = python.console.evaluate(code).error;
    error.unwrap_or_else(|| panic!("no error: {code}"))
}

/// Waits, between commands, until the code stops failing.
fn until(python: &Python, code: &str) {
    for _ in 0..600 {
        if python.console.evaluate(code).error.is_none() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("never: {code}");
}

#[tokio::test]
async fn a_listener_is_called_in_the_cycle_with_the_rows_of_the_change() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
der = src.update('W = V + 1')
seen = []
command_thread = threading.get_ident()
def on(update, is_replay):
    seen.append((
        is_replay, vals(update.added()), vals(update.removed()), vals(update.modified()),
        vals(update.modified_prev()), sorted(update.modified_columns),
        threading.get_ident() == command_thread,
    ))
handle = listen(der, on)
assert handle.started and seen == []
empty = {'K': [], 'V': [], 'W': []}

src.add(kv([1, 2, 3], [10, 20, 30]))
# the callback has run before the write returns, on the cycle's thread
assert seen == [(False, {'K': [1, 2, 3], 'V': [10, 20, 30], 'W': [11, 21, 31]}, empty, empty, empty, [], False)], seen

src.add(kv([2, 4], [21, 40]))
assert seen[1] == (
    False, {'K': [4], 'V': [40], 'W': [41]}, empty,
    {'K': [2], 'V': [21], 'W': [22]}, {'K': [2], 'V': [20], 'W': [21]}, ['K', 'V', 'W'], False,
), seen[1]

src.delete(new_table([int_col('K', [1])]))
assert seen[2] == (False, empty, {'K': [1], 'V': [10], 'W': [11]}, empty, empty, [], False), seen[2]

handle.stop()
handle.stop()
assert not handle.started
src.add(kv([9], [90]))
assert len(seen) == 3

handle.start(do_replay=True)
assert seen[3] == (
    True, {'K': [2, 3, 4, 9], 'V': [21, 30, 40, 90], 'W': [22, 31, 41, 91]}, empty, empty, empty, [], True,
), seen[3]
assert raised(handle.start) == ('RuntimeError', 'Attempting to start an already started listener..')
src.add(kv([5], [50]))
assert len(seen) == 5 and seen[4][1] == {'K': [5], 'V': [50], 'W': [51]}
"#,
    );
    // an object as the listener, the columns asked for, chunks, and an update that is kept
    python.run(
        r#"
kept = []
class Rows(TableListener):
    def __init__(self):
        self.calls = []
    def on_update(self, update, is_replay):
        kept.append(update)
        self.calls.append((
            vals(update.added('V')), list(update.added(['W', 'K'])),
            [vals(chunk) for chunk in update.added_chunks(2, 'K')],
            [vals(chunk) for chunk in update.removed_chunks(2)],
            raised(lambda: update.added('Nope')), raised(lambda: update.shifted),
            raised(lambda: update.added_chunks(-1)), list(update.added_chunks(0)),
        ))
rows = Rows()
by_object = listen(der, rows, do_replay=True)
assert rows.calls == [(
    {'V': [21, 30, 40, 90, 50]}, ['K', 'W'], [{'K': [2, 3]}, {'K': [4, 9]}, {'K': [5]}], [],
    ('ValueError', "Invalid column names: {'Nope'}"),
    ('NotImplementedError', 'shifts are not supported yet.'),
    ('ValueError', 'chunk_size must not be negative.'), [],
)], rows.calls
# after the call the update holds nothing, as a Java update that was released
update = kept[0]
assert isinstance(update, TableUpdate) and update.table is der
assert (update.added(), update.removed(), update.modified(), update.modified_prev()) == ({}, {}, {}, {})
assert list(update.added_chunks(2)) == []
assert raised(lambda: update.modified_columns)[0] == 'AttributeError'
"#,
    );
    // what `listen` refuses
    python.run(
        r#"
def message(call):
    kind, text = raised(call)
    return kind, text.split(' : ')[0]
assert message(lambda: listen(empty_table(3), on)) == ('DHError', 'table must be a refreshing table.')
assert raised(lambda: listen(src, lambda update: None)) == (
    'ValueError', 'listener function must have 2 (update, is_replay) parameters.')
assert message(lambda: listen(src, 5)) == ('DHError', 'listener is neither callable nor TableListener object')
assert message(lambda: listen(src, Rows(), on_error=print)) == (
    'DHError',
    'Invalid on_error argument for listeners of TableListener type which already have an on_error method.')
assert message(lambda: listen(src, on, dependencies=[5])) == ('DHError', 'failed to create a table listener.')
"#,
    );
}

#[tokio::test]
async fn an_exception_ends_the_listener_and_not_the_table() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
below = src.where('K > 0')
calls, errors = [], []
def bad(update, is_replay):
    calls.append(vals(update.added())['K'])
    raise ValueError('boom')
handle = listen(src, bad, on_error=errors.append)
src.add(kv([1], [10]))
src.add(kv([2], [20]))
# called once, told once, never called again; the table and what follows it go on
assert calls == [[1]], calls
assert len(errors) == 1 and type(errors[0]) is RuntimeError
text = str(errors[0])
assert text.startswith('Error in Python interpreter:\nTraceback') and 'ValueError: boom' in text, text
assert handle.started and (src.size, below.size) == (2, 2)

# an on_error that raises is logged; a listener object's own on_error is used
class Bad(TableListener):
    told = []
    def on_update(self, update, is_replay):
        raise KeyError('kk')
    def on_error(self, e):
        Bad.told.append(str(e))
        raise RuntimeError('in on_error')
by_object = listen(src, Bad())
src.add(kv([3], [30]))
assert len(Bad.told) == 1 and "KeyError: 'kk'" in Bad.told[0]
assert src.size == 3

# in a replay the exception goes to the caller
kind, text = raised(lambda: listen(src, bad, do_replay=True))
assert (kind, text.split(' : ')[0]) == ('DHError', 'failed to listen to the table changes.')
assert 'boom' in text and calls[-1] == [1, 2, 3]
"#,
    );
}

#[tokio::test]
async fn a_merged_listener_is_called_once_per_cycle_after_its_dependencies() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
a = input_table({'K': dtypes.int32})
b = input_table({'S': dtypes.string})
c = a.update('K2 = K * 2')
def ks(keys):
    return new_table([int_col('K', keys)])
def shown(update):
    return None if update is None else {name: list(values) for name, values in update.added().items()}
seen = []
def on(updates, is_replay):
    assert list(updates) == [a, b, c]
    seen.append((is_replay, [shown(update) for update in updates.values()]))
handle = merged_listen([a, b, c], on)
a.add(ks([1, 2]))
b.add(new_table([string_col('S', ['x'])]))
assert seen == [
    (False, [{'K': [1, 2]}, None, {'K': [1, 2], 'K2': [2, 4]}]),
    (False, [None, {'S': ['x']}, None]),
], seen
handle.stop()
a.add(ks([3]))
handle.start(do_replay=True)
assert seen[2:] == [(True, [{'K': [1, 2, 3]}, {'S': ['x']}, {'K': [1, 2, 3], 'K2': [2, 4, 6]}])], seen[2:]
handle.stop()

# a listener of `a` that reads `c` is called after `c` has changed
sizes = []
dependent = listen(a, lambda update, is_replay: sizes.append((a.size, c.size)), dependencies=c)
a.add(ks([4]))
assert sizes == [(4, 4)], sizes
dependent.stop()

class Both(MergedListener):
    told = []
    def on_update(self, updates, is_replay):
        raise ValueError('ml boom')
    def on_error(self, e):
        Both.told.append(str(e))
by_object = merged_listen([a, b], Both())
a.add(ks([5]))
a.add(ks([6]))
assert len(Both.told) == 1 and 'ValueError: ml boom' in Both.told[0], Both.told

def message(call):
    kind, text = raised(call)
    return kind, text.split(' : ')[0]
assert message(lambda: merged_listen([a], on)) == (
    'DHError', 'A merged listener must listen to at least 2 refreshing tables.')
assert message(lambda: merged_listen([a, empty_table(1)], on)) == ('DHError', 'table must be a refreshing table.')
assert raised(lambda: merged_listen([a, b], lambda updates: None)) == (
    'ValueError', 'merged listener function must have 2 parameters (updates, is_replay).')
assert message(lambda: merged_listen([a, b], 3)) == (
    'DHError', 'listener is neither callable nor MergedListener object')
assert message(lambda: merged_listen([a, b], Both(), on_error=print)) == (
    'DHError',
    'Invalid on_error argument for listeners of MergedListener type which already have an on_error method.')
assert message(lambda: merged_listen([a, b], Both(), do_replay=True)) == (
    'DHError', 'failed to listen to the table changes.')
"#,
    );
}

#[tokio::test]
async fn a_callback_runs_operations_and_is_refused_what_waits_for_its_cycle() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
seen = []
def on(update, is_replay):
    inner = listen(src, lambda update, is_replay: None, do_replay=True)
    inner.stop()
    with get_exec_ctx():
        seen.append((
            ug.has_shared_lock(src), ug.has_exclusive_lock(src),
            src.where('K > 1').size,
            raised(lambda: ug.shared_lock(src).__enter__()),
            raised(lambda: ug.exclusive_lock(src).__enter__()),
            raised(lambda: src.add(kv([100], [1]))),
            raised(lambda: src.await_update(100)),
        ))
handle = listen(src, on)
src.add(kv([1, 2], [10, 20]))
locks = 'java.lang.UnsupportedOperationException: Non-terminal notifications must not lock the update graph'
(shared, exclusive, size, shared_lock, exclusive_lock, add, await_update), = seen
assert (shared, exclusive, size) == (False, False, 1), seen
assert shared_lock == ('RuntimeError', locks) and exclusive_lock == ('RuntimeError', locks), seen
assert add[0] == 'DHError' and 'blocking input table edit from a listener' in add[1], add
assert await_update == ('DHError', 'await_update was interrupted. : ' + locks), await_update
assert src.size == 2
"#,
    );
}

#[tokio::test]
async fn the_graph_lock_is_what_java_says_it_is_in_a_command_and_on_a_thread() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
graph = src.update_graph
assert type(graph) is ug.UpdateGraph and graph == empty_table(1).update_graph == get_exec_ctx().update_graph
assert ug.auto_locking is True

# a command holds the exclusive lock, and both locks nest in it
assert (ug.has_shared_lock(src), ug.has_exclusive_lock(graph)) == (False, True)
with ug.shared_lock(src):
    assert (ug.has_shared_lock(src), ug.has_exclusive_lock(src)) == (True, True)
    with ug.exclusive_lock(graph):
        src.add(kv([1], [10]))
assert (ug.has_shared_lock(src), ug.has_exclusive_lock(src), src.size) == (False, True, 1)
assert raised(lambda: ug.shared_lock(5).__enter__()) == (
    'AttributeError', "'int' object has no attribute 'update_graph'")
assert ug.has_shared_lock(empty_table(1)) is False
"#,
    );
    // a thread holds nothing until it asks
    python.run(
        r#"
def shared():
    seen = [(ug.has_shared_lock(src), ug.has_exclusive_lock(src))]
    with ug.shared_lock(src):
        seen.append((ug.has_shared_lock(src), ug.has_exclusive_lock(src)))
        with ug.shared_lock(src):
            seen.append(src.where('K > 0').size)
        seen.append(raised(lambda: ug.exclusive_lock(src).__enter__()))
        seen.append(raised(lambda: src.await_update(10)))
        seen.append(raised(lambda: src.add(kv([7], [70]))))
        seen.append(raised(lambda: src.delete(kv([1], [10]).view('K'))))
    seen.append((ug.has_shared_lock(src), ug.has_exclusive_lock(src)))
    return seen
blocking = ("java.lang.UnsupportedOperationException: Attempted to make a blocking input table edit "
            "while holding the update graph's shared lock. This is unsupported, because it will "
            "block the update graph from making progress and hang indefinitely.")
upgrade = 'java.lang.UnsupportedOperationException: Cannot upgrade a shared lock to an exclusive lock'
assert on_thread(shared) == [
    (False, False), (True, False), 1, ('RuntimeError', upgrade),
    ('DHError', 'await_update was interrupted. : ' + upgrade),
    ('DHError', 'add to InputTable failed. : ' + blocking),
    ('DHError', 'delete data in the InputTable failed. : ' + blocking), (False, False),
], on_thread(shared)
assert src.size == 1

@ug.shared_locked(src)
def read(value):
    return value, ug.has_shared_lock(src), ug.has_exclusive_lock(src)
@ug.exclusive_locked(src)
def write(value):
    return value, ug.has_shared_lock(src), ug.has_exclusive_lock(src)
assert on_thread(lambda: read(1)) == (1, True, False)
def inside(error):
    with ug.shared_lock(src):
        raise error
assert on_thread(lambda: inside(ValueError('inside'))) == (
    'DHError', 'exception raised in the enclosed code block. : inside')
assert on_thread(lambda: inside(KeyboardInterrupt('inside'))) == ('KeyboardInterrupt', 'inside')
assert on_thread(lambda: (ug.has_shared_lock(src), ug.has_exclusive_lock(src))) == (False, False)
unlocked = on_thread(lambda: ug.auto_locking_op(lambda table: ug.has_shared_lock(table))(src))
assert unlocked is True
"#,
    );
    // the exclusive lock waits for the command, so the thread runs after it
    python.run(
        r#"
results = []
def exclusive():
    with ug.exclusive_lock(src):
        seen = [(ug.has_shared_lock(src), ug.has_exclusive_lock(src))]
        with ug.shared_lock(src):
            seen.append((ug.has_shared_lock(src), src.where('K > 0').size))
        with ug.exclusive_lock(src):
            pass
        # a blocking write gives the lock up for its cycle and has it again
        src.add(kv([2], [20]))
        seen.append((src.size, ug.has_exclusive_lock(src)))
    seen.append(write(3))
    seen.append((ug.has_shared_lock(src), ug.has_exclusive_lock(src)))
    results.append(seen)
worker = threading.Thread(target=exclusive)
worker.start()
"#,
    );
    // the command is over, so the thread has the lock; a command that joined it would
    // keep it waiting
    until(&python, "assert results");
    python.run(
        r#"
worker.join(60)
assert results == [[(False, True), (True, 1), (2, True), (3, False, True), (False, False)]], results
"#,
    );
}

/// A thread that holds the exclusive lock keeps cycles out until it gives it up.
#[tokio::test]
async fn a_thread_that_holds_the_exclusive_lock_keeps_cycles_out() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
held = []
def hold():
    with ug.exclusive_lock(src):
        held.append(time.monotonic())
        time.sleep(1.5)
        held.append(time.monotonic())
holder = threading.Thread(target=hold)
holder.start()
"#,
    );
    // the thread has the lock as soon as the command that started it is over; a command
    // would wait for it, so nothing is asked of Python while it is held
    std::thread::sleep(Duration::from_millis(300));
    let graph = python.server.state.graph.clone();
    let before = graph.completed_cycles();
    graph.request_cycle();
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(
        graph.completed_cycles(),
        before,
        "a cycle ran under the lock"
    );
    graph.run_cycle();
    assert!(graph.completed_cycles() > before);
    python.run("holder.join(60)\nassert not holder.is_alive()");
    // the lock was held from before the request until after the wait for its cycle
    python.run("assert len(held) == 2 and held[1] - held[0] >= 1.5, held");
}

#[tokio::test]
async fn await_update_waits_for_a_change_and_lets_the_cycle_run() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
kind, text = raised(lambda: empty_table(1).await_update())
assert (kind, text) == ('DHError', 'await_update can only be called on refreshing tables.')
started = time.monotonic()
assert src.await_update(200) is False
assert 0.15 < time.monotonic() - started < 5
assert src.await_update(0) is False and src.await_update(-1) is False

# a change made by another thread while the command waits
def write():
    time.sleep(0.1)
    src.add(kv([1], [10]))
writer = threading.Thread(target=write)
writer.start()
assert src.await_update(30000) is True
assert src.size == 1
writer.join(60)
writer = threading.Thread(target=lambda: (time.sleep(0.1), src.add(kv([2], [20]))))
writer.start()
assert src.await_update() is True
writer.join(60)

# on a thread, and on one that holds the exclusive lock
def waits():
    return src.await_update(30000), src.size
def locked_waits():
    with ug.exclusive_lock(src):
        return src.await_update(30000), src.size, ug.has_exclusive_lock(src)
waiters = [[], []]
threads = [
    threading.Thread(target=lambda: waiters[0].append(waits())),
    threading.Thread(target=lambda: waiters[1].append(locked_waits())),
]
for thread in threads:
    thread.start()
"#,
    );
    // the threads wait: one inside the exclusive lock, which it gave up for the wait
    std::thread::sleep(Duration::from_millis(300));
    python.run(
        r#"
assert waiters == [[], []], waiters
src.add(kv([3], [30]))
"#,
    );
    // the thread that waited inside the exclusive lock takes it again when no command runs
    until(&python, "assert all(waiters)");
    python.run("assert waiters == [[(True, 3)], [(True, 3, True)]], waiters");
}

#[tokio::test]
async fn a_liveness_scope_releases_what_nothing_else_holds() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
src.add(kv([1, 2, 3], [10, 20, 30]))
dead = 'io.deephaven.engine.liveness.LivenessStateException'
def use(table):
    return raised(lambda: table.where('K > 1').size) or table.where('K > 1').size
calls = []
with liveness_scope() as scope:
    inner = src.where('K > 0')
    child = inner.update('X = K')
    kept = src.update('X = K')
    fixed = empty_table(3).update('K = i')
    scoped = listen(src, lambda update, is_replay: calls.append('scoped'))
    scope.preserve(kept)
    assert use(inner) == 2
    assert [is_liveness_referent(x) for x in (inner, fixed, scoped, 5, scope)] == [True, True, True, False, False]
# a table made in the scope is gone, with the table made from it; one that was preserved,
# a static one, and the table the scope did not make are not
for table in (inner, child):
    kind, text = use(table)
    assert kind == 'DHError' and dead in text, text
    assert raised(lambda: table.size)[0] == 'DHError'
seen = (use(kept), use(fixed), use(src))
assert seen == (2, 1, 2), seen
src.add(kv([4], [40]))
assert (calls, kept.size) == ([], 4), (calls, kept.size)
scoped.stop()
kind, text = raised(lambda: scoped.start())
assert kind == 'DHError' and dead in text, text
"#,
    );
    // a table that something else holds lives on: a client's export, a table made from it
    python.run(
        r#"
scope = LivenessScope()
with scope.open():
    exported = src.where('K > 0')
    lost = src.where('K > 1')
    parent = src.where('K > 2')
with scope.open():
    later = src.where('K > 3')
derived = parent.update('X = K')
assert [use(table) for table in (exported, lost, parent, later)] == [3, 3, 2, 1]
"#,
    );
    let exported = python.table("exported");
    python.run(
        r#"
kind, text = raised(lambda: scope.manage(5))
assert (kind, text) == ('DHError', "Provided referent isn't a LivenessReferent or a JObjectWrapper around one")
outside = src.where('K > 0')
scope.manage(outside)
scope.manage(fixed)
scope.unmanage(fixed)
scope.release()
assert use(exported) == 3 and use(parent) == 2 and use(outside) == 3 and use(derived) == 2
for table in (lost, later):
    assert use(table)[0] == 'DHError', use(table)
assert raised(scope.release)[0] == 'DHError'
assert raised(lambda: scope.open().__enter__()) == ('RuntimeError', 'java.lang.NullPointerException')
"#,
    );
    assert_eq!(exported.row_set().size(), 4);
    python.run(
        r#"
# scopes nest; preserve hands a table to the scope around, and only the innermost can
with liveness_scope() as outer:
    with liveness_scope() as inner_scope:
        handed = src.where('K > 1')
        kind, text = raised(lambda: outer.preserve(handed))
        assert kind == 'DHError' and text.startswith('failed to pop the current scope'), text
        inner_scope.preserve(handed)
    assert use(handed) == 3
assert use(handed)[0] == 'DHError'

@liveness_scope()
def made():
    return src.where('K > 0')
assert use(made())[0] == 'DHError'

# a scope is its thread's: a table made on another thread is not in it
with liveness_scope():
    elsewhere = on_thread(lambda: src.where('K > 0'))
assert use(elsewhere) == 3
"#,
    );
    assert!(error(&python, "lost.size").contains("LivenessStateException"));
}

#[tokio::test]
async fn an_execution_context_is_accepted_and_frozen_variables_are_refused() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
ctx = get_exec_ctx()
assert type(ctx) is ExecutionContext and type(make_user_exec_ctx()) is ExecutionContext
x = 5
def in_context():
    with ctx:
        return src.update('Y = K + x').size, empty_table(2).update('Z = x').size
@ctx
def decorated():
    return src.update('Y = K + x').size
assert on_thread(in_context) == (0, 2) and on_thread(decorated) == 0
kind, text = raised(lambda: make_user_exec_ctx(freeze_vars=['x']))
assert kind == 'DHError', kind
assert text == ('failed to make a new ExecutionContext : not ported: '
                'io.deephaven.engine.context.ExecutionContext.Builder#captureQueryScopeVars'), text
from deephaven._gc import garbage_collect
garbage_collect()
"#,
    );
}

/// The lock rule of the module: a cycle that calls a Python listener, commands that write
/// and so wait for that cycle, and a Python thread that takes both sides of the graph
/// lock around table operations all make progress.
#[tokio::test]
async fn callbacks_commands_and_locking_threads_all_make_progress() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
made = src.update('A = K + 1')
added = []
def on(update, is_replay):
    added.extend(int(k) for k in update.added()['K'])
    made.where('A > 0').size
    sum(n for n in range(2000))
handle = listen(made, on)
running = True
rounds = 0
def work():
    global rounds
    while running:
        with ug.shared_lock(made):
            made.where('A > 1').size
            sum(n for n in range(500))
        with ug.exclusive_lock(made):
            made.size
        rounds += 1
worker = threading.Thread(target=work)
worker.start()
"#,
    );
    let stop = Arc::new(AtomicBool::new(false));
    let (done, finished) = mpsc::channel();
    let cycling = std::thread::spawn({
        let graph = python.server.state.graph.clone();
        let stop = stop.clone();
        let done = done.clone();
        move || {
            let mut cycles = 0;
            while !stop.load(Ordering::Relaxed) {
                graph.run_cycle();
                cycles += 1;
            }
            done.send(("cycles", cycles)).unwrap();
        }
    });
    let commands = std::thread::spawn({
        let console = python.console.clone();
        move || {
            for round in 0..30 {
                let changes = console.evaluate(&format!(
                    "src.add(new_table([int_col('K', [{round}])]))\n\
                     assert added[-1] == {round}, added\n\
                     u = made.where('A > 0')\n\
                     n = sum(n for n in range(2000))"
                ));
                assert_eq!(changes.error, None, "round {round}");
                std::thread::sleep(Duration::from_millis(2));
            }
            done.send(("commands", 30)).unwrap();
        }
    });

    // a deadlock is a party that never reports
    let wait = Duration::from_secs(120);
    let first = finished.recv_timeout(wait).expect("the commands finish");
    assert_eq!(first, ("commands", 30));
    stop.store(true, Ordering::Relaxed);
    let (who, cycles) = finished.recv_timeout(wait).expect("the cycles finish");
    assert_eq!(who, "cycles");
    assert!(cycles >= 5, "{cycles} cycles");
    commands.join().unwrap();
    cycling.join().unwrap();

    // a command that joined the thread would keep it waiting for the exclusive lock
    python.run("running = False");
    until(&python, "assert not worker.is_alive()");
    python.run("assert rounds >= 3, rounds\nassert added == list(range(30)), added");
    assert!(!python.table("made").is_failed());
}

/// What Java 42.9 answered for a partitioned table and liveness scopes: a scope takes one,
/// and none releases it (`tasks/7.10.1-js-plugins.md`).
#[tokio::test]
async fn a_liveness_scope_takes_a_partitioned_table_and_never_releases_it() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
src.add(kv([1, 2, 3], [10, 20, 30]))
fixed = empty_table(6).update(['K = i % 3', 'V = i'])
def usable(pt):
    return (pt.table.size, len(pt.constituent_tables), pt.merge().size, pt.keys().size)
for table, rows in ((fixed, 6), (src, 3)):
    pt = table.partition_by('K')
    # managed and released, twice: Java manages it again without complaint
    for _ in range(2):
        scope = LivenessScope()
        scope.manage(pt)
        scope.release()
    scope = LivenessScope()
    scope.manage(pt)
    scope.unmanage(pt)
    LivenessScope().unmanage(pt)
    with liveness_scope() as inner:
        preserved = table.partition_by('K')
        inner.preserve(preserved)
    with liveness_scope():
        scoped = table.partition_by('K')
    outer = LivenessScope()
    with liveness_scope():
        handed = table.partition_by('K')
        outer.manage(handed)
    outer.release()
    for each in (pt, preserved, scoped, handed):
        assert usable(each) == (3, 3, rows, 3), usable(each)
assert raised(lambda: LivenessScope().manage(5)) == (
    'DHError', "Provided referent isn't a LivenessReferent or a JObjectWrapper around one")
"#,
    );
}

/// `deephaven.server.executors` as Java 42.9 answered: two names, one thread behind both,
/// the tasks in the order they were submitted, and a failure of a task seen by no one.
#[tokio::test]
async fn a_task_is_run_by_the_one_thread_of_both_executors() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
from deephaven.server import executors
assert executors.executor_names() == ['serial', 'concurrent']
has = executors.has_executor
assert (has('serial'), has('concurrent'), has('fast'), has(5)) == (True, True, False, False)
assert raised(lambda: executors.submit_task('fast', lambda: None)) == ('KeyError', "'fast'")
assert raised(lambda: executors.submit_task(None, lambda: None)) == ('KeyError', 'None')
main = threading.current_thread()
seen, running, done = [], [], threading.Event()
def task(i):
    def run():
        running.append(i)
        if i == 0:
            time.sleep(0.2)
        seen.append((i, len(running), threading.current_thread() is main))
        running.pop()
        if i == 2:
            raise ValueError('seen by no one')
        if i == 5:
            done.set()
    return run
for i in range(6):
    if i == 3:
        # what is no function is taken, and fails unseen
        assert executors.submit_task('serial', 5) is None
        assert executors.submit_task('concurrent', lambda x: None) is None
    assert executors.submit_task('serial' if i % 2 else 'concurrent', task(i)) is None
assert done.wait(20)
# one at a time, in the order of submission, on a thread of its own
assert seen == [(i, 1, False) for i in range(6)], seen
assert raised(lambda: executors._register_named_java_executor('serial', None)) == (
    'DHError', 'Executor with name serial already registered')
kind, text = raised(lambda: executors._register_named_java_executor('other', None))
assert kind == 'NotPorted' and 'PythonDeephavenSession#registerJavaExecutor' in text, text
assert executors.executor_names() == ['serial', 'concurrent']

# a listener hands its work to a task, as a plugin's does, and the task reads the table
src = input_table({'K': dtypes.int32, 'V': dtypes.int32}, key_cols='K')
sizes, told = [], threading.Event()
def later():
    sizes.append(src.where('K > 0').size)
    told.set()
handle = listen(src, lambda update, is_replay: executors.submit_task('concurrent', later))
src.add(kv([1, 2], [10, 20]))
assert told.wait(20) and sizes == [2], sizes
handle.stop()
"#,
    );
}

/// The business calendars of the names Deephaven ships are handed out, in any case; the
/// rest of `deephaven.calendar` is in `calendar_tests.rs`.
#[tokio::test]
async fn the_shipped_business_calendars_are_handed_out() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
from deephaven import calendar as cal
for name, zone in (('UTC', 'UTC'), ('usnyse_example', 'America/New_York'), ('USBANK_EXAMPLE', 'America/New_York')):
    found = cal.calendar(name)
    assert found.name() == name.upper() and str(found.timeZone()) == zone, (name, str(found.timeZone()))
assert raised(lambda: cal.add_calendar(object())) == ('DHError', 'failed to add calendar : no matching Java method overloads found')
assert raised(lambda: cal.add_calendar(None)) == ('DHError', 'cal must be specified')
"#,
    );
}
