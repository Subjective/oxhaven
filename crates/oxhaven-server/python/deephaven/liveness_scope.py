"""Liveness scopes: releasing the refreshing tables and listeners that a block of code made.

Behavior: `deephaven.liveness_scope` of the reference package, as far as this engine has
the thing it manages. Java counts the references to every refreshing table and listener
(`io.deephaven.engine.liveness`): a scope holds one to each object made while it is open,
and an object that loses its last one stops updating and can no longer be operated on.
The engine here has no such counts. A table lives as long as something holds it: a Python
`Table`, a client's export, or a table derived from it.

So a scope here remembers the Python objects made on its thread while it is open: every
`Table` of a refreshing table and every listener handle. When the scope is released, an
object that no other scope manages is released too:

- a `Table` whose engine table nothing else holds (no export of a client, no table
  derived from it, no listener) gives its hold up, and every later use of it raises
  `DHError`. A table that something else holds stays usable, as in Java, where that
  holder's reference keeps it live;
- a listener stops and cannot be started again.

What differs from Java is in `docs/CONSOLE.md`, "Listeners, locks, liveness scopes and
execution contexts": mainly that a released table cannot be read either, where Java still
reads the rows it had.
"""

import contextlib
import threading
from collections.abc import Iterator

from .dherror import DHError

_RELEASED = (
    "io.deephaven.engine.liveness.LivenessStateException: the table was released with its "
    "liveness scope and is no longer live"
)
"""What the use of a released table raises. Java's text names the operation's listener."""

_open = threading.local()


def _stack() -> list:
    """The scopes that are open on this thread, the innermost last.

    Behavior: `io.deephaven.engine.liveness.LivenessScopeStack`, which is per thread
    """
    try:
        return _open.scopes
    except AttributeError:
        _open.scopes = []
        return _open.scopes


def _is_referent(referent) -> bool:
    return callable(getattr(referent, "_liveness_release", None))


def _is_live(referent) -> bool:
    """Whether the engine would count references to `referent`: a refreshing table, or a
    listener. A static table is never released."""
    try:
        return bool(getattr(referent, "is_refreshing", True))
    except DHError:  # a table that was released
        return False


def _created(referent) -> None:
    """Called for every `Table` and listener handle when it is made: the innermost scope
    that is open on this thread manages it.

    Behavior: `io.deephaven.engine.liveness.LivenessScopeStack#peek` and `#manage` in the
    constructors of Java's refreshing tables and listeners. An object made while no scope
    is open is the session's and is never released.
    """
    scopes = _stack()
    if scopes and _is_live(referent):
        scopes[-1]._add(referent)


def _unwrap_to_liveness_referent(referent):
    if _is_referent(referent):
        return referent
    raise DHError("Provided referent isn't a LivenessReferent or a JObjectWrapper around one")


class _BaseLivenessScope:
    """What `SimpleLivenessScope` and `LivenessScope` share."""

    def __init__(self):
        self._referents = []
        self._released = False

    @property
    def j_object(self):
        return None if self._released else self

    @property
    def j_scope(self):
        return self.j_object

    def _add(self, referent) -> None:
        # the number of scopes that manage it; an object without the count is the
        # session's
        referent._liveness_managers = getattr(referent, "_liveness_managers", 0) + 1
        self._referents.append(referent)

    def _drop(self, referents: list) -> None:
        """Takes this scope's references to `referents` back, and releases those that no
        scope manages any more: the newest first, so that a table derived from another is
        gone before the table it was derived from is asked whether anything holds it."""
        unmanaged = []
        for referent in reversed(referents):
            managers = getattr(referent, "_liveness_managers", None)
            if managers is None:
                continue
            referent._liveness_managers = managers - 1
            if managers == 1 and referent not in unmanaged:
                unmanaged.append(referent)
        waiting = unmanaged
        while waiting:
            held = [referent for referent in waiting if not referent._liveness_release()]
            if len(held) == len(waiting):
                break
            waiting = held
        for referent in waiting:
            # something else holds it: it lives on as the session's
            del referent._liveness_managers

    def _release(self) -> None:
        """Releases everything this scope manages."""
        try:
            if self._released:
                raise AttributeError("'NoneType' object has no attribute 'release'")
            self._released = True
            referents, self._referents = self._referents, []
            self._drop(referents)
        except Exception as e:
            raise DHError(e, message="failed to close the LivenessScope.")

    def preserve(self, referent) -> None:
        """Keeps `referent` alive past this scope: the scope around this one manages it
        too, or, when there is none, it becomes the session's. This scope must be the
        innermost one that is open."""
        referent = _unwrap_to_liveness_referent(referent)
        scopes = _stack()
        if not scopes or scopes[-1] is not self:
            error = RuntimeError(
                "java.lang.IllegalStateException: Caller requested to pop a LivenessScope "
                "that is not the top of the stack"
            )
            raise DHError(
                error,
                message="failed to pop the current scope - is preserve() being called on the right scope?",
            )
        if not _is_live(referent) or not hasattr(referent, "_liveness_managers"):
            # nothing is ever released: a static table, or an object of the session
            return
        if len(scopes) > 1:
            scopes[-2]._add(referent)
        else:
            del referent._liveness_managers

    def manage(self, referent) -> None:
        """Makes this scope manage `referent`, which is released with the scope unless
        something else manages it."""
        referent = _unwrap_to_liveness_referent(referent)
        if _is_live(referent) and hasattr(referent, "_liveness_managers"):
            self._add(referent)

    def unmanage(self, referent) -> None:
        """Ends this scope's management of `referent`, which is released now if nothing
        else manages it."""
        referent = _unwrap_to_liveness_referent(referent)
        for index, managed in enumerate(self._referents):
            if managed is referent:
                del self._referents[index]
                self._drop([referent])
                return


class SimpleLivenessScope(_BaseLivenessScope):
    """The scope that `liveness_scope` opens."""


class LivenessScope(_BaseLivenessScope):
    """A scope that can be opened more than once, and is released by a call."""

    def release(self) -> None:
        """Releases everything the scope manages. The scope cannot be used afterwards."""
        self._release()

    @contextlib.contextmanager
    def open(self) -> Iterator[None]:
        """A context manager in which this scope manages what is made."""
        if self._released:
            # Java pushes the scope that the release set to null
            raise RuntimeError("java.lang.NullPointerException")
        _push(self)
        try:
            yield None
        finally:
            _pop(self)


def _push(scope: _BaseLivenessScope) -> None:
    _stack().append(scope)


def _pop(scope: _BaseLivenessScope) -> None:
    scopes = _stack()
    if not scopes or scopes[-1] is not scope:
        error = RuntimeError(
            "java.lang.IllegalStateException: Caller requested to pop a LivenessScope that "
            "is not the top of the stack"
        )
        raise DHError(error, message="failed to pop the LivenessScope from the stack.")
    scopes.pop()


def is_liveness_referent(referent) -> bool:
    """Whether a liveness scope can manage `referent`: a table or a listener handle."""
    return _is_referent(referent)


@contextlib.contextmanager
def liveness_scope() -> Iterator[SimpleLivenessScope]:
    """A context manager, and a decorator, that releases the refreshing tables and the
    listeners made in its block when the block ends, except those that `preserve` kept.
    """
    scope = SimpleLivenessScope()
    _push(scope)
    try:
        yield scope
    finally:
        _pop(scope)
        scope._release()
