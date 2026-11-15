"""The update graph's lock for Python code.

Behavior: `deephaven.update_graph` of the reference package. The server has one update
graph, and its lock has two sides: an update cycle holds the exclusive one, and a table
operation takes the shared one for as long as it runs. `shared_lock` and `exclusive_lock`
hold a side around a block of Python code, on the thread that entered the block.

A console command of this server runs under the lock already, and to a script that hold
is the exclusive lock, as it is in the reference: `has_exclusive_lock` is true in a
command, and both locks nest in it. Code that an update cycle calls, a table listener or
a function in a formula, cannot take either lock, as in the reference.

The engine takes the shared lock for every operation that needs it, whatever
`auto_locking` says; the reference fails an operation on a refreshing table from an
unlocked thread when it is off.
"""

import contextlib
import threading
from functools import wraps
from typing import Callable

import _oxhaven

from .dherror import DHError

auto_locking = True
"""Whether table operations take the shared lock by themselves. The engine always does."""


class UpdateGraph:
    """The update graph of the server. Every table has the same one.

    Behavior: `deephaven.update_graph.UpdateGraph`, the wrapper of a Java `UpdateGraph`
    """

    def __init__(self, j_update_graph=None):
        self.j_update_graph = j_update_graph

    @property
    def j_object(self):
        return self.j_update_graph

    def __eq__(self, other):
        return isinstance(other, UpdateGraph)

    def __hash__(self):
        return hash(UpdateGraph)


def _graph(ug) -> UpdateGraph:
    """The graph of an update graph, a table, or anything else with an `update_graph`."""
    return ug if isinstance(ug, UpdateGraph) else ug.update_graph


def _main() -> bool:
    """Whether this is the thread commands run on."""
    return threading.current_thread() is threading.main_thread()


def has_exclusive_lock(ug) -> bool:
    """Whether this thread holds the exclusive lock of the update graph `ug`, or of the
    graph of the table `ug`."""
    _graph(ug)
    return _oxhaven.graph_holds(_main())[1]


def has_shared_lock(ug) -> bool:
    """Whether this thread holds the shared lock of the update graph `ug`, or of the graph
    of the table `ug`."""
    _graph(ug)
    return _oxhaven.graph_holds(_main())[0]


@contextlib.contextmanager
def _locked(ug, exclusive: bool):
    _graph(ug)
    _oxhaven.graph_lock(exclusive, _main())
    try:
        yield
    except Exception as e:
        raise DHError(e, "exception raised in the enclosed code block.") from e
    finally:
        _oxhaven.graph_unlock(exclusive)


def exclusive_lock(ug):
    """A context manager that holds the exclusive lock of the update graph around a block:
    no update cycle runs and no other thread holds the lock while the block runs."""
    return _locked(ug, True)


def shared_lock(ug):
    """A context manager that holds the shared lock of the update graph around a block: no
    update cycle runs while the block runs."""
    return _locked(ug, False)


def exclusive_locked(ug) -> Callable:
    """A decorator that runs the function under the exclusive lock of the update graph."""
    ug = _graph(ug)

    def inner_wrapper(f: Callable) -> Callable:
        @wraps(f)
        def do_locked(*arg, **kwargs):
            with exclusive_lock(ug):
                return f(*arg, **kwargs)

        return do_locked

    return inner_wrapper


def shared_locked(ug) -> Callable:
    """A decorator that runs the function under the shared lock of the update graph."""
    ug = _graph(ug)

    def inner_wrapper(f: Callable) -> Callable:
        @wraps(f)
        def do_locked(*arg, **kwargs):
            with shared_lock(ug):
                return f(*arg, **kwargs)

        return do_locked

    return inner_wrapper


def _is_arg_refreshing(arg) -> bool:
    if isinstance(arg, (list, tuple)):
        return any(_is_arg_refreshing(item) for item in arg)
    return bool(getattr(arg, "is_refreshing", False))


def _first_refreshing_table(*args, **kwargs):
    for arg in list(args) + list(kwargs.values()):
        if _is_arg_refreshing(arg):
            return arg
    return None


def _current_thread_processes_updates() -> bool:
    return _oxhaven.graph_in_cycle()


def _needs_lock(arg) -> bool:
    """Whether a block that reads `arg` has to take the shared lock: `arg` is refreshing,
    and this thread neither holds the lock nor runs an update cycle."""
    if not arg or not auto_locking or _current_thread_processes_updates():
        return False
    return not (has_shared_lock(arg) or has_exclusive_lock(arg))


def auto_locking_op(f: Callable) -> Callable:
    """A decorator that runs the function under the shared lock when one of its arguments
    is a refreshing table and the thread does not hold the lock."""

    @wraps(f)
    def do_locked(*args, **kwargs):
        arg = _first_refreshing_table(*args, **kwargs)
        if not _needs_lock(arg):
            return f(*args, **kwargs)
        with shared_lock(arg.update_graph):
            return f(*args, **kwargs)

    return do_locked


@contextlib.contextmanager
def auto_locking_ctx(*args, **kwargs):
    """A context manager that holds the shared lock when one of the arguments is a
    refreshing table and the thread does not hold the lock."""
    arg = _first_refreshing_table(*args, **kwargs)
    if not _needs_lock(arg):
        yield
    else:
        with shared_lock(arg.update_graph):
            yield
