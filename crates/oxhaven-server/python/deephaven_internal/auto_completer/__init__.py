"""Completion for the console, answered by the library `jedi` (MIT licence).

The server imports `jedi_settings` into the session's globals when a client opens a
completion stream, and asks it for everything. A script may change how it answers:

    from deephaven_internal.auto_completer import jedi_settings
    jedi_settings.mode = "OFF"      # no completion
    jedi_settings.mode = "SAFE"     # from the text of the document alone
    jedi_settings.mode = "STRONG"   # from the session's variables too (the default)

(`Mode` itself is in `deephaven_internal.auto_completer._completer`, as in the reference.)

Importing this package fails when `jedi` or `docstring_parser` is not installed, and the
server then answers every request "nothing found", as the Java server does.

Behavior: `py/server/deephaven_internal/auto_completer/__init__.py`
"""

from jedi import Interpreter, preload_module

from . import _completer
from ._completer import Completer

jedi_settings = Completer()

# the first use of jedi is slow; do it here and not at the first key the user presses
preload_module("deephaven")
Interpreter("", []).complete(1, 0)


def set_max_recursion_limit(limit: int) -> None:
    """Sets the recursion limit that completion keeps Python 3.9 and 3.10 under.

    Behavior: `deephaven_internal.auto_completer.set_max_recursion_limit`
    """
    _completer.MAX_RECURSION_LIMIT = limit
