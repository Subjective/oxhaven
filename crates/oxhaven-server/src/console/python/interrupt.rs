//! Stopping a running Python command.
//!
//! No Java counterpart: the Java server never stops a command. On Java 42.9 a command
//! whose `ExecuteCommand` call the client cancelled, or whose session was closed, ran to
//! its end (`time.sleep(25)` and a loop of 25 seconds were tried); `CancelCommand` is not
//! implemented there, and the web client's cancel is not either. A command that never ends
//! holds the update graph's lock, so no table ticks again until the server is restarted.
//! This server stops the command.
//!
//! # The mechanism
//!
//! CPython interrupts running code in one way that also wakes it from a blocking call: a
//! signal, whose Python handler runs on the *main thread* of the interpreter, between two
//! bytecode instructions or when a blocking call returns with `EINTR`. That is how
//! `Ctrl-C` works in a Python program. So:
//!
//! - The interpreter is started on a thread of its own ([`start_main_thread`]), which is
//!   therefore Python's main thread, and every command runs on it ([`on_main_thread`]).
//!   The thread does nothing else: it waits for the next command.
//! - When the interpreter starts, a Python handler that raises `KeyboardInterrupt` is
//!   installed for [`STOP_SIGNAL`] with `signal.signal`.
//! - A request to stop the running command ([`Stop::request`], from the call that was
//!   cancelled or the session that ended) sends that signal to the main thread with
//!   `pthread_kill`. Nothing else is touched from the requesting thread: not the
//!   interpreter, not its lock.
//! - The command sees `KeyboardInterrupt` raised where it is. Unless it catches it, it
//!   ends as with any other exception: what it assigned before is kept and published, and
//!   its error is the traceback.
//! - When the command has ended, no request can send a signal any more
//!   ([`Stop::disarm`]), and a signal that arrived too late to interrupt it is consumed
//!   before the next command starts ([`discard_pending`]), so one command's stop never
//!   reaches another.
//!
//! `PyThreadState_SetAsyncExc` was the alternative: it needs no thread of its own, but
//! the exception it sets is only seen at the next bytecode instruction, so
//! `time.sleep(600)` would sleep to its end.
//!
//! # What is and is not stopped
//!
//! - **Python code** is interrupted at the next bytecode instruction: `while True: pass`.
//! - **Blocking calls the interpreter makes interruptible** return at once:
//!   `time.sleep`, waiting for a lock, an event, a queue, a subprocess or a socket.
//! - **Native code that does not return** is not stopped: a C extension in a loop, or
//!   blocked in a call that the signal does not wake or that it retries. The exception is
//!   raised when the native code returns to the interpreter, if ever. Nothing can be done
//!   about this from outside the thread; the server then stays as the Java server does.
//! - **An engine operation** the command called (`t.update(...)`, a sort, a join) is
//!   native code too, and is *finished, not abandoned*: it runs to its end with the
//!   interpreter lock released, its result is dropped, and the exception is raised when
//!   it returns. No engine state is left half built. A system call the operation is in
//!   when the signal arrives may return `EINTR`; the standard library retries those.
//! - **Code that catches `KeyboardInterrupt`**, or `BaseException`, and goes on is not
//!   stopped; a second request (the session ending after the call was cancelled) sends a
//!   second signal.
//! - **Other threads** a script started, and code that does not run as a command, are
//!   never interrupted: the signal is for the command.
//! - **A command that has not started** when the stop is requested does not run.
//!
//! # What this costs
//!
//! - The process uses [`STOP_SIGNAL`] (`SIGUSR2`). A script that installs its own handler
//!   for it disables stopping; a `SIGUSR2` sent to the process from outside interrupts
//!   the command that is running, and does nothing when none is.
//! - Commands run on Python's main thread, so a script can call `signal.signal`, which a
//!   script in the Java server cannot (it runs on a thread of the JVM). A handler for
//!   `SIGINT` installed that way would take `Ctrl-C` from the server.
//! - Unix only: signals directed at a thread.
//!
//! If the interpreter was started by something else than [`start_main_thread`] (PyO3's
//! own initialization, when Python is used before the session exists), there is no main
//! thread to run on: commands run where they are called and cannot be stopped.

use std::os::unix::thread::JoinHandleExt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::mpsc;
use std::sync::OnceLock;
use std::thread::JoinHandle;

use pyo3::exceptions::PyKeyboardInterrupt;
use pyo3::prelude::*;
use pyo3::types::{PyCFunction, PyDict, PyTuple};

use super::super::session::Stop;

/// The signal that stops a command.
pub const STOP_SIGNAL: i32 = libc::SIGUSR2;

type Job = Box<dyn FnOnce() + Send>;

/// The thread the interpreter was started on.
struct MainThread {
    handle: JoinHandle<()>,
    jobs: mpsc::Sender<Job>,
}

static MAIN_THREAD: OnceLock<MainThread> = OnceLock::new();

/// Installs the handler of [`STOP_SIGNAL`]. Must run on Python's main thread, which is the
/// only one `signal.signal` accepts.
fn install_handler(py: Python<'_>) -> PyResult<()> {
    let raise = |_: &Bound<'_, PyTuple>, _: Option<&Bound<'_, PyDict>>| -> PyResult<()> {
        Err(PyKeyboardInterrupt::new_err(()))
    };
    let handler = PyCFunction::new_closure(py, Some(c"stop_command"), None, raise)?;
    py.import("signal")?
        .call_method1("signal", (STOP_SIGNAL, handler))?;
    Ok(())
}

/// Starts the thread that is Python's main thread from now on: runs `start` on it, which
/// must initialize the interpreter and release its lock, and then every job that
/// [`on_main_thread`] is given, for as long as the process lives. Fails with the error of
/// `start`, and then there is no such thread.
///
/// Call it once per process, before anything else starts the interpreter.
pub fn start_main_thread(
    start: impl FnOnce() -> Result<(), String> + Send + 'static,
) -> Result<(), String> {
    let (jobs, queue) = mpsc::channel::<Job>();
    let (started, outcome) = mpsc::channel();
    let thread = std::thread::Builder::new().name("python-main".to_string());
    let handle = thread
        .spawn(move || {
            let result = start().and_then(|()| {
                Python::attach(install_handler).map_err(|error| {
                    format!("the handler that stops a Python command was not installed: {error}")
                })
            });
            let failed = result.is_err();
            let _ = started.send(result);
            if failed {
                return;
            }
            // ends only with the process: the sender lives in a static
            for job in queue {
                job();
            }
        })
        .map_err(|error| format!("the thread of the Python interpreter did not start: {error}"))?;
    outcome
        .recv()
        .map_err(|_| "the thread of the Python interpreter ended while starting".to_string())??;
    let main = MainThread { handle, jobs };
    MAIN_THREAD
        .set(main)
        .map_err(|_| "the Python interpreter was started twice".to_string())
}

/// Runs `job` on Python's main thread and returns its result; a panic of the job is this
/// call's. Without such a thread (see the [module documentation](self)), and when called
/// on it, the job runs here.
pub fn on_main_thread<R: Send + 'static>(job: impl FnOnce() -> R + Send + 'static) -> R {
    let Some(main) = MAIN_THREAD.get() else {
        return job();
    };
    if std::thread::current().id() == main.handle.thread().id() {
        return job();
    }
    let (done, result) = mpsc::sync_channel(1);
    let job: Job = Box::new(move || {
        let _ = done.send(catch_unwind(AssertUnwindSafe(job)));
    });
    match main.jobs.send(job).ok().and_then(|()| result.recv().ok()) {
        Some(Ok(value)) => value,
        Some(Err(panic)) => resume_unwind(panic),
        None => panic!("the thread of the Python interpreter is gone"),
    }
}

/// Sends [`STOP_SIGNAL`] to Python's main thread, if there is one.
fn signal_main_thread() {
    let Some(main) = MAIN_THREAD.get() else {
        return;
    };
    // SAFETY: `pthread_kill` needs the identifier of a thread that has not been joined or
    // detached and ended. `handle` is the join handle of the main thread, kept in a static
    // and never joined, and that thread's loop ends only when the sender of its jobs is
    // dropped, which the same static never does. The signal number is a valid one. The
    // call only marks the signal as pending for that thread.
    let result = unsafe { libc::pthread_kill(main.handle.as_pthread_t(), STOP_SIGNAL) };
    if result != 0 {
        tracing::error!("the Python command could not be signalled to stop: error {result}");
    }
}

/// Runs the Python handlers of the signals that have arrived for this thread, and drops
/// what they raise. Called on the main thread between commands, where an interrupt has
/// nothing to interrupt.
fn discard_pending(py: Python<'_>) {
    // A signal sent to this thread is delivered at the latest when the thread returns
    // from the kernel, so after one system call the interpreter has been told of it.
    std::thread::yield_now();
    for _ in 0..64 {
        if py.check_signals().is_ok() {
            return;
        }
    }
}

/// Runs `command` so that a request to `stop` interrupts it with `KeyboardInterrupt`;
/// `None`, without running it, if the stop was requested already. Call it on Python's
/// main thread; anywhere else the command runs and nothing interrupts it.
pub fn stoppable<R>(py: Python<'_>, stop: &Stop, command: impl FnOnce() -> R) -> Option<R> {
    discard_pending(py);
    if !stop.arm(signal_main_thread) {
        return None;
    }
    let result = command();
    stop.disarm();
    discard_pending(py);
    Some(result)
}
