"""Garbage collection.

Behavior: `deephaven._gc` of the reference package, which collects in Python and then asks
the JVM to. The engine here frees a table when its last holder lets go of it, so the
Python collection is all there is to do: it ends the tables and listeners that only
unreachable Python objects held.
"""

import gc

from .dherror import DHError


def garbage_collect() -> None:
    """Collects the Python objects that nothing refers to."""
    try:
        gc.collect()
    except Exception as e:
        raise DHError(e, "failed to initiate system-wide garbage collection.") from e
