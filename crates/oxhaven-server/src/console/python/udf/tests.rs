//! Python functions in query strings, through the console of a running server. The Python
//! here imports the standard library only; the scenarios `oracle/scenarios/python/udf_*`
//! compare the same calls, and those of numpy types, with Java.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::table::Table;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{SelectOrUpdateRequest, TableReference, Ticket};
use oxhaven_testkit::TableSnapshot;

use super::super::tests::{described, Python};
use crate::session::export_id_to_ticket;
use crate::test_support::with_auth;

const FUNCTIONS: &str = "\
from typing import Optional
from deephaven import empty_table, time_table
def twice(x) -> int:
    return 2 * x
def half(x) -> float:
    return x / 2
def small(x) -> bool:
    return x < 2
def named(x) -> str:
    return 's' + str(x)
def maybe(x) -> Optional[int]:
    return None if x == 1 else x
def text(x) -> 'int':
    return x + 1
def show(x) -> str:
    return type(x).__name__ + ':' + repr(x)
src = empty_table(3).update('X = i')
";

/// The text of the error the code fails with.
fn error(python: &Python, code: &str) -> String {
    let error = python.console.evaluate(code).error;
    error.unwrap_or_else(|| panic!("no error: {code}"))
}

fn longs(table: &dyn Table, column: usize) -> Vec<Option<i64>> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let values = snapshot.columns[column]
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("a long column");
    values.iter().collect()
}

#[tokio::test]
async fn a_call_gives_a_column_of_the_type_the_function_is_annotated_to_return() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "made = src.update(['A = twice(X)', 'B = half(X)', 'C = small(X)', 'D = named(X)', \
         'E = maybe(X)', 'F = text(X) + 1', 'G = twice(twice(X)) > 4 ? named(X) : `no`'])\n\
         kept = src.where('small(X)').select('A = twice(X) + 1')\n\
         viewed = src.update_view('A = twice(X)').view('B = half(A)')",
    );
    assert_eq!(
        described(&python, "made")[1..],
        [
            "A: Long = 0, 2, 4",
            "B: Double = 0.0, 0.5, 1.0",
            "C: Boolean = true, true, false",
            "D: String = s0, s1, s2",
            "E: Long = 0, , 2",
            "F: Long = 2, 3, 4",
            "G: String = no, no, s2",
        ]
    );
    assert_eq!(described(&python, "kept"), ["A: Long = 1, 3"]);
    assert_eq!(described(&python, "viewed"), ["B: Double = 0.0, 1.0, 2.0"]);
}

#[tokio::test]
async fn a_function_is_given_python_values_and_the_values_java_reserves_for_null() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "def perhaps(x: Optional[int]) -> str:\n    return repr(x)\n\
         def whole(x: int) -> str:\n    return repr(x)\n\
         number = 7\n\
         nulls = empty_table(2).update(['I = i == 1 ? null : i', 'D = i == 1 ? null : i * 1.5', \
         'T = i == 1 ? null : `s`', 'Z = i == 1 ? null : (Boolean) true', \
         'C = i == 1 ? null : (char) 65', 'B = i == 1 ? null : (byte) 3'])\n\
         given = nulls.select(['A = show(I)', 'E = show(D)', 'F = show(T)', 'G = show(Z)', \
         'H = show(C)', 'J = show(B)', 'K = perhaps(I)', 'L = show(1.5f)', 'M = show(null)', \
         'N = show(number)', 'O = whole(B)', 'P = show(I + 1L)'])",
    );
    assert_eq!(
        described(&python, "given"),
        [
            "A: String = int:0, int:-2147483648",
            "E: String = float:0.0, float:-1.7976931348623157e+308",
            "F: String = str:'s', NoneType:None",
            "G: String = bool:True, NoneType:None",
            "H: String = int:65, int:65535",
            "J: String = int:3, int:-128",
            "K: String = 0, None",
            "L: String = float:1.5, float:1.5",
            "M: String = NoneType:None, NoneType:None",
            "N: String = int:7, int:7",
            "O: String = 3, -128",
            "P: String = int:1, int:-9223372036854775808",
        ]
    );
}

#[tokio::test]
async fn the_library_and_a_column_come_before_a_function_and_the_callers_scope_before_globals() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "import functools\n\
         def abs(x) -> str:\n    return 'python'\n\
         def sqrt(t: str) -> str:\n    return 'python ' + t\n\
         def add(amount, x) -> int:\n    return x + amount\n\
         class Adder:\n    def __call__(self, x) -> int:\n        return x + 5\n\
         add_five = Adder()\n\
         add_seven = functools.partial(add, 7)\n\
         def in_function():\n    def twice(x) -> int:\n        return 1000 + x\n    \
         return src.update('A = twice(X)')\n\
         made = src.update(['A = abs(X - 2)', 'B = sqrt(`t`)', 'C = add_five(X)', \
         'D = add_seven(X)'])\n\
         local = in_function()",
    );
    assert_eq!(
        described(&python, "made")[1..],
        [
            "A: Int = 2, 1, 0",
            "B: String = python t, python t, python t",
            "C: Long = 5, 6, 7",
            "D: Long = 7, 8, 9",
        ]
    );
    assert_eq!(described(&python, "local")[1..], ["A: Long = 1000, 1001, 1002"]);

    // Java: "Cannot find method twice(int)" in a longer text; the column hides the function
    let hidden = error(&python, "bad = src.update('twice = X').update('A = twice(X)')");
    assert!(hidden.contains("twice"), "{hidden}");
    let missing = error(&python, "bad = src.update('A = no_such(X)')");
    assert!(missing.contains("no_such"), "{missing}");
    // a variable that is no callable is no function
    let variable = error(&python, "number = 7\nbad = src.update('A = number(X)')");
    assert!(variable.contains("number"), "{variable}");
}

#[tokio::test]
async fn a_function_is_called_once_for_each_row_in_row_order_across_chunks() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    let rows = 2 * super::CHUNK + 5;
    python.run(&format!(
        "seen = []\n\
         def record(x) -> int:\n    seen.append(x)\n    return len(seen)\n\
         big = empty_table({rows}).update('N = record(ii)')\n\
         assert seen == list(range({rows})), (len(seen), seen[:5])\n\
         def never() -> int:\n    raise AssertionError('called')\n\
         none = empty_table(0).update('N = never()')\n\
         lazy = big.update_view('M = record(N)')\n\
         assert len(seen) == {rows}"
    ));
    let numbers = longs(python.table("big").as_ref(), 0);
    assert_eq!(numbers.len(), rows);
    assert!(
        numbers
            .iter()
            .enumerate()
            .all(|(row, n)| *n == Some(row as i64 + 1)),
        "{:?}",
        &numbers[..8]
    );
    assert_eq!(described(&python, "none"), ["N: Long = "]);
}

#[tokio::test]
async fn an_argument_the_annotation_does_not_take_is_refused_as_java_refuses_it() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "def whole(x: int) -> int:\n    return x\n\
         def real(x: float) -> float:\n    return x\n\
         def nothing() -> int:\n    return 1\n\
         src = src.update(['D = X * 1.5', 'T = `s`'])",
    );
    for (formula, text) in [
        (
            "whole(D)",
            "whole: Expected argument (x) to be either one of [long] or their compatible ones, \
             got double",
        ),
        (
            "whole(T)",
            "whole: Expected argument (x) to be either one of [long] or their compatible ones, \
             got class java.lang.String",
        ),
        (
            "real(X)",
            "real: Expected argument (x) to be either one of [double] or their compatible ones, \
             got int",
        ),
        ("whole(null)", "compatible ones, got null"),
        ("nothing(X)", "nothing: Expected no arguments, got 1"),
    ] {
        let error = error(&python, &format!("bad = src.update('A = {formula}')"));
        assert!(error.contains(text), "{formula}: {error}");
        assert!(!error.contains("not ported"), "{formula}: {error}");
    }
    // the number of arguments is Python's to check, for each row
    let count = error(&python, "bad = src.update('A = whole(X, X)')");
    assert!(count.contains("TypeError"), "{count}");
    python.run("ok = src.update(['A = whole(X)', 'B = real(D)', 'C = real(1.5f)'])");
}

#[tokio::test]
async fn an_exception_in_the_function_fails_the_operation_with_its_type_and_text() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run("def raises(x) -> int:\n    raise ValueError('boom ' + str(x))");
    for code in [
        "bad = src.update('A = raises(X)')",
        "bad = src.select('A = raises(X)')",
        "bad = src.where('raises(X) > 0')",
    ] {
        let error = error(&python, code);
        assert!(error.contains("ValueError: boom 0"), "{code}: {error}");
    }
    // a call that failed leaves nothing behind
    python.run("ok = src.update('A = twice(X)')");
    assert_eq!(described(&python, "ok")[1..], ["A: Long = 0, 2, 4"]);
}

#[tokio::test]
async fn what_java_does_and_this_server_does_not_is_refused_with_the_java_method() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "import datetime\n\
         from typing import Any, Sequence, Union\n\
         def bare(x):\n    return x\n\
         def any_of(x) -> Any:\n    return x\n\
         def either(x) -> Union[int, str]:\n    return x\n\
         def none(x) -> None:\n    return None\n\
         def when(x) -> datetime.datetime:\n    return datetime.datetime.now()\n\
         def several(x) -> Sequence[int]:\n    return [x]\n\
         def raw(x) -> bytes:\n    return b''\n\
         def total(*xs) -> int:\n    return len(xs)\n\
         def perhaps(x: Optional[int]) -> int:\n    return 0 if x is None else 1\n\
         def wrong(x) -> int:\n    return 1.5\n\
         def flag(x) -> int:\n    return True\n\
         def huge(x) -> int:\n    return 2**70\n\
         def missing(x) -> float:\n    return None\n\
         def number(x) -> str:\n    return 1\n\
         def truth(x) -> bool:\n    return 1\n\
         src = src.update('L = (long) X')\n\
         grouped = src.group_by()\n\
         times = time_table(10**9)",
    );
    const PARSE: &str = "PyCallableWrapperJpyImpl#parseSignature";
    const CALL: &str = "PyCallableWrapperJpyImpl#call";
    for (code, what, java) in [
        ("src.update('A = bare(X)')", "bare has no return annotation", PARSE),
        ("src.update('A = (int) bare(X)')", "bare has no return annotation", PARSE),
        ("src.update('A = any_of(X)')", "any_of has no return annotation", PARSE),
        ("src.update('A = either(X)')", "either has no return annotation", PARSE),
        ("src.update('A = none(X)')", "none has no return annotation", PARSE),
        ("src.where('bare(X)')", "bare has no return annotation", PARSE),
        ("src.update('A = (lambda x: x)(X)')", "", ""),
        (
            "src.update('A = when(X)')",
            "when returns a time",
            "_ParsedReturnAnnotation#setup_return_converter",
        ),
        (
            "times.update('A = twice(Timestamp)')",
            "an Instant as an argument of twice",
            "_ParsedParam#_setup_datetime_arg_converter",
        ),
        ("src.update('A = several(X)')", "several returns an array", "deephaven._udf#_udf_parser"),
        ("src.update('A = raw(X)')", "raw returns an array", "deephaven._udf#_udf_parser"),
        ("grouped.update('A = twice(X)')", "as an argument of twice", "deephaven._udf#_udf_parser"),
        ("src.update('A = twice(\\'a\\')')", "a character literal as an argument of twice", CALL),
        (
            "src.update('A = total()')",
            "a call of total, which takes *args, without arguments",
            "QueryLanguageParser#visit(MethodCallExpr)",
        ),
        (
            "src.update(['A = perhaps(X)', 'B = perhaps(L)'])",
            "calls of perhaps in one operation that give its parameter x, which may be None, \
             a int and a long",
            "_ParsedParam#setup_arg_converter",
        ),
        ("src.update('A = wrong(X)')", "wrong returned a Python float for a long column", CALL),
        ("src.update('A = flag(X)')", "flag returned a Python bool for a long column", CALL),
        ("src.update('A = huge(X)')", "huge returned a Python int for a long column", CALL),
        ("src.update('A = missing(X)')", "missing returned a Python NoneType for a double", CALL),
        ("src.update('A = number(X)')", "number returned a Python int for a java.lang.String", CALL),
        ("src.update('A = truth(X)')", "truth returned a Python int for a java.lang.Boolean", CALL),
    ] {
        if java.is_empty() {
            // a lambda in a formula is no formula at all, in Java as here
            assert!(python.console.evaluate(&format!("bad = {code}")).error.is_some());
            continue;
        }
        let error = error(&python, &format!("bad = {code}"));
        assert!(error.contains(what), "{code}: {error}");
        assert!(error.contains("not ported"), "{code}: {error}");
        assert!(error.contains(java), "{code}: {error}");
    }
    // each operation has its own converters, so the two calls may be two operations
    python.run("ok = src.update('A = perhaps(X)').update('B = perhaps(L)')");
    assert_eq!(
        described(&python, "ok")[2..],
        ["A: Long = 1, 1, 1", "B: Long = 1, 1, 1"]
    );
}

#[tokio::test]
async fn a_filter_fails_on_none_where_java_fails_and_where_java_drops_the_row() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run("def unsure(x) -> Optional[bool]:\n    return None if x == 1 else x > 1");
    // Java fails these two as well
    for filter in ["unsure(X) && X >= 0", "!unsure(X)"] {
        let error = error(&python, &format!("bad = src.where('{filter}')"));
        assert!(error.contains("NullPointerException"), "{filter}: {error}");
    }
    // Java: FilterKernelPythonChunkedFunction#filter drops the row; docs/PYTHON_UDF.md
    let bare = error(&python, "bad = src.where('unsure(X)')");
    assert!(bare.contains("NullPointerException"), "{bare}");
    python.run("ok = src.where('X != 1 && unsure(X)')");
    assert_eq!(described(&python, "ok"), ["X: Int = 2"]);
}

#[tokio::test]
async fn a_formula_of_a_grpc_client_cannot_call_python() {
    let python = Python::start().await;
    let bearer = python.server.login().await;
    python.run(FUNCTIONS);
    python.run("b = src");
    let mut tables = TableServiceClient::new(python.server.channel.clone());
    let mut update = async |result: i32, formula: &str| {
        let request = SelectOrUpdateRequest {
            result_id: Some(export_id_to_ticket(result)),
            source_id: Some(TableReference {
                r#ref: Some(Ref::Ticket(Ticket {
                    ticket: b"s/b".to_vec(),
                })),
            }),
            column_specs: vec![formula.to_string()],
        };
        tables.update(with_auth(request, &bearer)).await
    };
    // Java: INVALID_ARGUMENT, "Cannot validate user expression"
    let refused = update(1, "Z = twice(X)").await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert!(update(2, "Z = X + 1").await.unwrap().into_inner().success);
    // the same formula in the session
    python.run("ok = b.update('Z = twice(X)')");
}

/// Runs a cycle, in which a new time table gets its first row, waits until one with a
/// period of a microsecond has `rows` more rows to add, and runs the cycle that adds them.
fn cycle_of(python: &Python, rows: usize) {
    python.server.state.graph.run_cycle();
    std::thread::sleep(Duration::from_micros(rows as u64 + 1000));
    python.server.state.graph.run_cycle();
}

#[tokio::test]
async fn a_cycle_calls_the_function_the_formula_was_compiled_with() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "def stamp(x) -> int:\n    return x + 1\n\
         source = time_table(1000)\n\
         made = source.update('A = stamp(ii)')\n\
         kept = source.where('stamp(ii) % 2 == 0')\n\
         lazy = source.update_view('A = stamp(ii)')\n\
         def stamp(x) -> int:\n    return x + 100",
    );
    let before = python.table("made").row_set().size();
    // more than a chunk in one cycle
    cycle_of(&python, 2 * super::CHUNK);
    let made = python.table("made");
    assert!(!made.is_failed());
    let stamps = longs(made.as_ref(), 1);
    assert!(
        stamps.len() as u64 > before + super::CHUNK as u64,
        "{before} rows, then {}",
        stamps.len()
    );
    assert!(
        stamps
            .iter()
            .enumerate()
            .all(|(row, stamp)| *stamp == Some(row as i64 + 1)),
        "{:?}",
        &stamps[..4]
    );
    assert_eq!(longs(python.table("lazy").as_ref(), 1), stamps);
    assert_eq!(
        python.table("kept").row_set().size(),
        stamps.len() as u64 / 2
    );
    // a formula compiled now calls the function the name holds now
    python.run("after = source.update('A = stamp(ii)')");
    assert_eq!(longs(python.table("after").as_ref(), 1)[0], Some(100));
}

#[tokio::test]
async fn an_exception_in_a_cycle_fails_the_table_and_no_other() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "def fragile(x) -> int:\n    if x > 500:\n        raise ValueError('too far')\n    \
         return x\n\
         source = time_table(1000)\n\
         failing = source.update('A = fragile(ii)')\n\
         fine = source.update('A = twice(ii)')",
    );
    assert!(!python.table("failing").is_failed());
    cycle_of(&python, 2000);
    assert!(python.table("failing").is_failed());
    assert!(!python.table("fine").is_failed());
    assert!(python.table("fine").row_set().size() > 500);
    // the session and the next cycle go on
    python.run("again = source.update('A = twice(ii)')");
    cycle_of(&python, 10);
    assert!(!python.table("again").is_failed());
    assert!(!python.table("fine").is_failed());
}

/// The lock rule of `docs/PYTHON_UDF.md`: a cycle whose formula calls Python, commands
/// that call table operations, and a Python thread that does both, all make progress.
#[tokio::test]
async fn a_cycle_calls_python_while_commands_run() {
    let python = Python::start().await;
    python.run(FUNCTIONS);
    python.run(
        "import threading, time\n\
         def stamp(x) -> int:\n    return x + 1\n\
         source = time_table(1_000_000)\n\
         made = source.update('A = stamp(ii)')\n\
         kept = made.where('stamp(A) % 2 == 0')\n\
         running = True\n\
         rounds = 0\n\
         def _work():\n    global rounds\n    while running:\n        \
         t = made.update('B = stamp(A)').where('stamp(B) > 0')\n        \
         sum(stamp(n) for n in range(500))\n        rounds += 1\n\
         _thread = threading.Thread(target=_work)\n\
         _thread.start()",
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
                let changes = console.evaluate(
                    "u = made.update('B = stamp(A)').where('stamp(B) > 0')\n\
                     assert u.size <= made.size\n\
                     n = sum(stamp(n) for n in range(2000))",
                );
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

    python.run("running = False\n_thread.join(60)\nassert not _thread.is_alive()");
    python.run("assert rounds >= 3, rounds");
    let made = python.table("made");
    assert!(!made.is_failed());
    assert!(!python.table("kept").is_failed());
    let stamps = longs(made.as_ref(), 1);
    assert!(stamps.len() > 10, "{}", stamps.len());
    assert!(stamps
        .iter()
        .enumerate()
        .all(|(row, stamp)| *stamp == Some(row as i64 + 1)));
}

/// What taking the interpreter lock costs a thread that evaluates a formula. Not a test:
/// `cargo test -p oxhaven-server --features python --lib -- --ignored lock_cost --nocapture`
#[tokio::test]
#[ignore = "a measurement, printed"]
async fn lock_cost() {
    use pyo3::Python as Interpreter;

    let python = Python::start().await;
    python.run(FUNCTIONS);
    let mean = |rounds: u32, f: &mut dyn FnMut()| {
        let start = Instant::now();
        for _ in 0..rounds {
            f();
        }
        start.elapsed() / rounds
    };
    let attach = &mut || Interpreter::attach(|_| ());

    println!("lock taken and released, no Python thread: {:?}", mean(200_000, attach));

    let rows = 100 * super::CHUNK;
    let update = &mut || {
        python.run(&format!("t = empty_table({rows}).update('A = twice(ii)')"));
    };
    let call = mean(3, update) / rows as u32;
    println!("one row of update('A = twice(ii)'), {rows} rows: {call:?}");
    let plain = &mut || {
        python.run(&format!("t = empty_table({rows}).update('A = 2 * ii')"));
    };
    println!("one row of update('A = 2 * ii'): {:?}", mean(3, plain) / rows as u32);

    python.run(
        "import threading, time\n\
         busy = True\n\
         def _spin():\n    while busy:\n        pass\n\
         _spinner = threading.Thread(target=_spin)\n\
         _spinner.start()",
    );
    println!("lock taken and released, a Python thread computing: {:?}", mean(200, attach));
    let cycle = |formula: &str, title: &str| {
        python.run(&format!(
            "source = time_table(1000)\nmade = source.update('{formula}')"
        ));
        python.server.state.graph.run_cycle();
        std::thread::sleep(Duration::from_micros(10 * super::CHUNK as u64));
        let before = python.table("made").row_set().size();
        let start = Instant::now();
        python.server.state.graph.run_cycle();
        let took = start.elapsed();
        let rows = python.table("made").row_set().size() - before;
        println!("a cycle that adds {rows} rows of {formula}, {title}: {took:?}");
    };
    cycle("A = twice(ii)", "a Python thread computing");
    cycle("A = 2 * ii", "a Python thread computing");
    python.run("busy = False\n_spinner.join()");
    cycle("A = twice(ii)", "no Python thread");
    cycle("A = 2 * ii", "no Python thread");
}
