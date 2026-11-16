"""Running a function on a thread of the server, away from the thread that asks for it: a
listener's function hands work to one so that the update cycle does not wait for it.

There are two names, "serial" and "concurrent", and behind both the same single thread, as
in Java's Python server, which gives both names the one thread of a pool of one
(`PythonDeephavenSession#registerJavaExecutor`). So tasks run one at a time, in the order
they were submitted, whichever name they were given. A task that raises ends, nothing is
told or logged, and the next one runs; what is no function is taken and fails the same
silent way. This is what Java 42.9 was seen to do.

Differences: the thread is a Python thread here (`threading.Thread`, where Java's shows as
a `_DummyThread`), and a table operation in a task runs without an execution context, where
Java refuses a formula with "No ExecutionContext registered" (`docs/CONSOLE.md`).

Behavior: `deephaven.server.executors`,
`io.deephaven.integrations.python.PythonDeephavenSession#registerJavaExecutor`
"""

import queue
import threading
from typing import Callable

from deephaven import DHError
from deephaven.dtypes import _java_time

_tasks: "queue.SimpleQueue[Callable[[], None]]" = queue.SimpleQueue()
_thread = None
_start = threading.Lock()


def _work() -> None:
    while True:
        task = _tasks.get()
        try:
            task()
        except BaseException:
            # Java hands the task to `ExecutorService#submit` and never asks for its result,
            # so what it raised is seen by no one
            pass
        del task


def _submit(task: Callable[[], None]) -> None:
    """Queues a task for the one thread, which is started for the first."""
    global _thread
    with _start:
        if _thread is None:
            _thread = threading.Thread(target=_work, name="oxhaven-python-executor", daemon=True)
            _thread.start()
    _tasks.put(task)


_executors: dict[str, Callable[[Callable[[], None]], None]] = {
    "serial": _submit,
    "concurrent": _submit,
}


def has_executor(executor_name: str) -> bool:
    """Whether there is an executor of that name."""
    return executor_name in executor_names()


def executor_names() -> list[str]:
    """The names of the executors."""
    return list(_executors.keys())


def submit_task(executor_name: str, task: Callable[[], None]) -> None:
    """Queues `task` to run on the named executor, and returns without waiting for it.

    The task sets up what it needs itself: an execution context, a liveness scope. It
    must not wait for another task, which runs only after it.

    Raises:
        KeyError: there is no executor of that name
    """
    _executors[executor_name](task)


def _register_named_java_executor(executor_name: str, java_executor) -> None:
    """In the reference, called by the Java server when it starts, with a Java executor.
    This server has none to give, and its two names are there from the start."""
    if executor_name in executor_names():
        raise DHError(f"Executor with name {executor_name} already registered")
    raise _java_time.NotPorted(
        "io.deephaven.integrations.python.PythonDeephavenSession#registerJavaExecutor"
    )
