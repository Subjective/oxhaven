//! Ring tables and function generated tables, through the console of a running server.
//! The Python here imports the standard library only. What Java answers was observed on
//! Java 42.9 (`tasks/4.19-table-sources.md`); the scenarios
//! `oracle/scenarios/python/input_ring_and_generated.py`, `input_ring_ticking.py` and
//! `input_generated_ticking.py` are replayed against the recordings.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use oxhaven_contracts::graph::UpdateGraph;

use super::super::tests::{described, Python};

const SETUP: &str = r#"
import threading
from deephaven import DHError, dtypes, empty_table, function_generated_table, input_table
from deephaven import new_table, ring_table
from deephaven import update_graph as ug
from deephaven.column import int_col, string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table_listener import listen
def ks(keys):
    return new_table([int_col('K', keys)])
def raised(call):
    try:
        call()
    except BaseException as e:
        return type(e).__name__, str(e)
    return None
"#;

/// Waits, between commands, until the code stops failing.
fn until(python: &Python, code: &str) {
    for _ in 0..600 {
        if python.console.evaluate(code).error.is_none() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("never: {code}");
}

#[tokio::test]
async fn a_ring_table_keeps_the_last_rows_of_an_input_table() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "src = input_table(init_table=ks([1, 2, 3]))\n\
         ring = ring_table(src, 2)\n\
         later = ring_table(src, 2, initialize=False)\n\
         fixed = ring_table(empty_table(5).update('X = i'), True)\n\
         assert ring.is_refreshing and not ring.is_flat and not fixed.is_refreshing\n\
         assert ring.attributes() == {}, ring.attributes()\n\
         src.add(ks([4]))",
    );
    assert_eq!(described(&python, "ring"), ["K: Int = 3, 4"]);
    assert_eq!(described(&python, "later"), ["K: Int = 4"]);
    // `True` is the capacity 1, as jpy takes it
    assert_eq!(described(&python, "fixed"), ["X: Int = 4"]);
    python.run("src.add(ks([5, 6, 7]))");
    assert_eq!(described(&python, "ring"), ["K: Int = 6, 7"]);
    assert_eq!(described(&python, "later"), ["K: Int = 6, 7"]);
}

#[tokio::test]
async fn a_ring_table_is_refused_with_a_dherror_as_in_java() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
head = 'failed to create a ring table. : '
for call, text in [
    (lambda: ring_table(src, 0), 'java.lang.IllegalArgumentException: Capacity must be positive'),
    (lambda: ring_table(src, -1), 'java.lang.NegativeArraySizeException: -1'),
    # the low 32 bits, as jpy hands an integer over
    (lambda: ring_table(src, 2**31), 'java.lang.NegativeArraySizeException: -2147483648'),
    (lambda: ring_table(src, 2**31 - 1), 'required capacity <= 2147483639'),
    (lambda: ring_table(empty_table(1).update('X = 1'), 2, initialize=False), 'but init=NONE'),
    (lambda: ring_table(src, 2.5), 'no matching Java method overloads found'),
    (lambda: ring_table(src, '2'), 'no matching Java method overloads found'),
    (lambda: ring_table(src, 2, None), 'no matching Java method overloads found'),
    (lambda: ring_table(None, 2), "'NoneType' object has no attribute 'j_table'"),
    (lambda: ring_table(input_table({}), 2), 'java.lang.IllegalArgumentException'),
]:
    kind, message = raised(call)
    assert kind == 'DHError' and message.startswith(head) and text in message, (kind, message)
assert ring_table(src, 2, 1).size == 0
"#,
    );
}

#[tokio::test]
async fn a_ring_table_fails_when_its_parent_modifies_a_row() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        "keyed = input_table(init_table=ks([1, 2, 3]), key_cols='K')\n\
         ring = ring_table(keyed, 2)\n\
         keyed.delete(ks([3]))\n\
         keyed.add(ks([7]))",
    );
    // what the parent deleted stays
    assert_eq!(described(&python, "ring"), ["K: Int = 3, 7"]);
    assert!(!python.table("ring").is_failed());
    python.run("keyed.add(ks([1]))");
    assert!(python.table("ring").is_failed());
}

#[tokio::test]
async fn a_generated_table_follows_its_source_tables() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
other = input_table({'K': dtypes.int32})
calls = []
def rows(prefix, bonus=0):
    calls.append(threading.get_ident())
    size = src.size + other.size + bonus
    return empty_table(size).update(['N = i', 'S = `' + prefix + '-` + N'])
both = function_generated_table(rows, source_tables=[src, other], args=('p',), kwargs={'bonus': 1})
one = function_generated_table(lambda: src.update('L = K * 2'), source_tables=src)
seen = []
def on(update, is_replay):
    cells = lambda rows: [int(n) for n in rows['N']]
    seen.append((cells(update.added()), cells(update.removed()), cells(update.modified())))
handle = listen(both, on)
assert both.is_refreshing and not both.is_flat and both.attributes() == {}
assert len(calls) == 1 and calls[0] == threading.get_ident()
src.add(ks([5]))
# generated by the cycle, before the command has its `add` back, and once per cycle
assert len(calls) == 2 and calls[1] != threading.get_ident(), calls
assert seen == [([1], [], [0])], seen
other.add(ks([6, 7]))
assert seen[1:] == [([2, 3], [], [0, 1])], seen
"#,
    );
    let rows = ["N: Int = 0, 1, 2, 3", "S: String = p-0, p-1, p-2, p-3"];
    assert_eq!(described(&python, "both"), rows);
    assert_eq!(described(&python, "one"), ["K: Int = 5", "L: Int = 10"]);
}

#[tokio::test]
async fn a_generated_table_takes_its_arguments_as_the_reference_package_does() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
fixed = empty_table(1)
make = lambda: empty_table(2).update('X = i')
# without a source table that could change, or with a negative interval: generated once
for table in [
    function_generated_table(make, source_tables=[]),
    function_generated_table(make, refresh_interval_ms=-5),
    # the low 32 bits of the interval, as jpy hands an integer over
    function_generated_table(make, refresh_interval_ms=2**31),
]:
    assert not table.is_refreshing and table.size == 2
never = function_generated_table(make, refresh_interval_ms=0)
assert never.is_refreshing and never.size == 2
assert function_generated_table(make, source_tables=(src,), exec_ctx=get_exec_ctx()).size == 2
assert function_generated_table(make, refresh_interval_ms=True).is_refreshing

for call, kind, text in [
    (lambda: function_generated_table(make), 'DHError',
     'Either refresh_interval_ms or source_tables must be provided!'),
    (lambda: function_generated_table(make, source_tables=src, refresh_interval_ms=5), 'DHError',
     'Only one of refresh_interval_ms and source_tables must be provided!'),
    # what the engine refuses is not a DHError in the reference package
    (lambda: function_generated_table(make, source_tables=fixed), 'RuntimeError',
     'java.lang.IllegalArgumentException: All source tables must be refreshing'),
    (lambda: function_generated_table(make, source_tables=[src, fixed]), 'RuntimeError',
     'All source tables must be refreshing'),
    (lambda: function_generated_table(lambda: None, source_tables=src), 'RuntimeError',
     'table_generator did not return a result'),
    (lambda: function_generated_table(lambda: 5, refresh_interval_ms=100), 'RuntimeError',
     'table_generator did not return a Table'),
    (lambda: function_generated_table(lambda: 1 / 0, source_tables=src), 'RuntimeError',
     'Error in Python interpreter:\nTraceback'),
    (lambda: function_generated_table(lambda: 1 / 0, source_tables=src), 'RuntimeError',
     'ZeroDivisionError: division by zero'),
    (lambda: function_generated_table(lambda x: fixed, source_tables=src), 'RuntimeError',
     'missing 1 required positional argument'),
    (lambda: function_generated_table(make, source_tables=src, exec_ctx=5), 'RuntimeError',
     'Error in Python interpreter:'),
    (lambda: function_generated_table(make, source_tables=5), 'TypeError',
     "'int' object is not iterable"),
    (lambda: function_generated_table(make, source_tables=[src, 5]), 'AttributeError',
     "'int' object has no attribute 'j_table'"),
    (lambda: function_generated_table(make, refresh_interval_ms='x'), 'ValueError',
     "cannot convert a Python 'str' to a Java 'io.deephaven.engine.table.Table'"),
    (lambda: function_generated_table(make, refresh_interval_ms=2.5), 'RuntimeError',
     'no matching Java method overloads found'),
    (lambda: function_generated_table(make, refresh_interval_ms=2**63), 'RuntimeError',
     'OverflowError: Python int too large to convert to C long'),
]:
    got = raised(call)
    assert got is not None and got[0] == kind and text in got[1], (kind, text, got)
"#,
    );
}

#[tokio::test]
async fn a_generated_table_fails_when_its_function_fails_in_a_cycle() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
def raising():
    if src.size >= 2:
        raise ValueError('boom')
    return empty_table(src.size + 1).update('X = i')
def nothing():
    return empty_table(1) if src.size < 2 else None
def another_column():
    return empty_table(1).update(['A = 1'] + (['B = 2'] if src.size >= 2 else []))
def a_column_less():
    return empty_table(1).update(['A = 1'] + (['B = 2'] if src.size < 2 else []))
def another_type():
    return empty_table(1).update('A = 1' if src.size < 2 else 'A = `x`')
def locking():
    if src.size >= 2:
        with ug.shared_lock(src):
            pass
    return empty_table(1)
def swapped():
    return empty_table(1).update(['A = 1', 'B = 2'] if src.size < 2 else ['B = 3', 'A = 4'])
names = ['raising', 'nothing', 'another_column', 'a_column_less', 'another_type', 'locking']
tables = {name: function_generated_table(globals()[name], source_tables=src) for name in names}
globals().update({'g_' + name: table for name, table in tables.items()})
g_swapped = function_generated_table(swapped, source_tables=src)
errors = {}
handles = [
    listen(table, lambda update, is_replay: None, on_error=lambda e, name=name: errors.update({name: str(e)}))
    for name, table in tables.items()
]
src.add(ks([1]))
assert errors == {}, errors
src.add(ks([2]))
assert sorted(errors) == sorted(names), errors
assert errors['raising'].startswith('Error in Python interpreter:\nTraceback'), errors['raising']
assert 'ValueError: boom' in errors['raising']
assert 'table_generator did not return a result' in errors['nothing']
assert 'java.lang.NullPointerException' in errors['another_column']
assert 'FunctionGeneratedTableFactory#copyTable' in errors['a_column_less']
assert 'the type of column A changed' in errors['another_type']
assert 'Non-terminal notifications must not lock the update graph' in errors['locking']
# and the source goes on
src.add(ks([3]))
"#,
    );
    for name in [
        "raising",
        "nothing",
        "another_column",
        "a_column_less",
        "another_type",
        "locking",
    ] {
        assert!(python.table(&format!("g_{name}")).is_failed(), "{name}");
    }
    // the rows it had when it failed
    assert_eq!(described(&python, "g_raising"), ["X: Int = 0, 1"]);
    // columns in another order are the same columns
    assert!(!python.table("g_swapped").is_failed());
    assert_eq!(
        described(&python, "g_swapped"),
        ["A: Int = 4", "B: Int = 3"]
    );
}

#[tokio::test]
async fn a_generated_table_is_generated_again_when_its_interval_has_passed() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
count = 0
def counted():
    global count
    count += 1
    return new_table([int_col('N', [count])])
timed = function_generated_table(counted, refresh_interval_ms=1)
never = function_generated_table(counted, refresh_interval_ms=0)
slow = function_generated_table(counted, refresh_interval_ms=3_600_000)
ticking = function_generated_table(lambda: src, refresh_interval_ms=1)
assert count == 3
"#,
    );
    std::thread::sleep(Duration::from_millis(5));
    python.server.state.graph.run_cycle();
    until(&python, "assert count == 4, count");
    assert_eq!(described(&python, "timed"), ["N: Int = 4"]);
    assert_eq!(described(&python, "never"), ["N: Int = 2"]);
    assert_eq!(described(&python, "slow"), ["N: Int = 3"]);
    // Java does not take a refreshing table for satisfied while the sources refresh
    assert!(python.table("ticking").is_failed());
    std::thread::sleep(Duration::from_millis(5));
    python.server.state.graph.run_cycle();
    until(&python, "assert count == 5, count");
    assert_eq!(described(&python, "timed"), ["N: Int = 5"]);
}

/// A deadlock between the update graph's lock and the interpreter lock is a party that
/// never reports. With the `py.detach` of `function_generated_table` taken out, the
/// thread that makes a table waits for the graph lock with the interpreter lock held
/// while the cycle that holds the graph lock waits for the interpreter, and this test
/// does not finish.
#[tokio::test]
async fn a_generated_table_and_a_command_make_progress_together() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
src = input_table({'K': dtypes.int32})
def rows():
    made = src.update('A = K + 1').where('A > 0')
    sum(n for n in range(2000))
    return empty_table(made.size).update('N = i')
first = function_generated_table(rows, source_tables=src)
timed = function_generated_table(lambda: empty_table(1).update('X = 1'), refresh_interval_ms=1)
running = True
rounds = 0
def work():
    global rounds
    while running:
        function_generated_table(rows, source_tables=src).size
        function_generated_table(rows, refresh_interval_ms=1).size
        with ug.shared_lock(src):
            ring_table(src, 3).size
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
                    "src.add(ks([{round}]))\n\
                     assert first.size == {round} + 1, first.size\n\
                     again = function_generated_table(rows, source_tables=src)\n\
                     assert again.size == {round} + 1, again.size"
                ));
                assert_eq!(changes.error, None, "round {round}");
                std::thread::sleep(Duration::from_millis(2));
            }
            done.send(("commands", 30)).unwrap();
        }
    });

    let wait = Duration::from_secs(120);
    let first = finished.recv_timeout(wait).expect("the commands finish");
    assert_eq!(first, ("commands", 30));
    stop.store(true, Ordering::Relaxed);
    let (who, cycles) = finished.recv_timeout(wait).expect("the cycles finish");
    assert_eq!(who, "cycles");
    assert!(cycles >= 5, "{cycles} cycles");
    commands.join().unwrap();
    cycling.join().unwrap();
    python.run(
        "running = False\n\
         worker.join(60)\n\
         assert not worker.is_alive() and rounds > 0, rounds",
    );
}
