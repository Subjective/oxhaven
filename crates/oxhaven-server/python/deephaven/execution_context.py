"""Execution contexts.

Behavior: `deephaven.execution_context` of the reference package, as far as this server
has the thing it carries. In Java an `ExecutionContext` holds what a table operation
needs beyond its arguments: the query scope, the query library, the formula compiler and
the update graph. A thread that a script started, and the thread of a table listener,
have none, and an operation there that compiles a formula or makes a refreshing table
fails until it is run inside `with ctx:`.

This server has one update graph, one formula compiler and one function library, and
every thread reaches them. So a context here carries nothing: `get_exec_ctx()` and
`make_user_exec_ctx()` return one, and entering it, or decorating a function with it,
changes nothing. Code written for the reference package, which opens a context in every
thread and listener, runs as it does there. Code that leaves the context out fails in the
reference and runs here (`docs/CONSOLE.md`, "Listeners, locks, liveness scopes and
execution contexts").

A context with frozen variables (`make_user_exec_ctx(freeze_vars=...)`) would have to
replace the query scope of the formulas compiled inside it, which this server takes from
the code that calls the operation. It is refused by the name of the Java method.
"""

from collections.abc import Sequence
from contextlib import ContextDecorator
from typing import Optional, Union

from . import _engine
from .dherror import DHError
from .update_graph import UpdateGraph


class ExecutionContext(ContextDecorator):
    """A context manager and decorator that makes the code inside it run in this
    execution context.

    Behavior: `deephaven.execution_context.ExecutionContext`
    """

    def __init__(self, j_exec_ctx=None):
        self.j_exec_ctx = j_exec_ctx

    @property
    def j_object(self):
        return self.j_exec_ctx

    @property
    def update_graph(self) -> UpdateGraph:
        """The update graph of the context: the server's."""
        return UpdateGraph()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None


def make_user_exec_ctx(freeze_vars: Optional[Union[str, Sequence[str]]] = None) -> ExecutionContext:
    """A new execution context with the query scope, the formula compiler, the function
    library and the update graph of the server.

    With `freeze_vars` the reference makes a context whose query scope holds those
    variables only, with the values they have now. That is not ported.
    """
    if _engine.to_sequence(freeze_vars):
        error = NotImplementedError(
            "not ported: io.deephaven.engine.context.ExecutionContext.Builder#captureQueryScopeVars"
        )
        raise DHError(error, message="failed to make a new ExecutionContext") from error
    return ExecutionContext()


def get_exec_ctx() -> ExecutionContext:
    """The execution context of the calling thread: the server's."""
    return ExecutionContext()
