"""Tables fed by streams of data.

Behavior: `deephaven.stream`. `blink_to_append_only` is the engine's; the Kafka consumer is
in `deephaven.stream.kafka` and the table publisher in `deephaven.stream.table_publisher`
(`docs/KAFKA.md`). `add_only_to_blink` names the Java method it would call.
"""

from warnings import warn

from .. import _engine
from .._java_time import NotPorted
from ..dherror import DHError
from ..table import Table


def add_only_to_blink(table: Table) -> Table:
    """Creates a blink table from the given add-only table. Not ported yet."""
    try:
        raise NotPorted("io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter#toBlink")
    except Exception as e:
        raise DHError(e, "failed to create a blink table.") from e


def blink_to_append_only(table: Table) -> Table:
    """Creates an append-only table of every row the blink table `table` shows from now on,
    starting with the rows it shows now. Asked again of the same table while the first
    result is in use, the answer is that result.

    Raises:
        DHError
    """
    try:
        return Table(_engine.native.blink_to_append_only(table.j_table))
    except Exception as e:
        raise DHError(e, "failed to create an append-only table.") from e


def stream_to_append_only(table: Table) -> Table:
    """Deprecated synonym for blink_to_append_only"""
    warn(
        "This function is deprecated, prefer blink_to_append_only",
        DeprecationWarning,
        stacklevel=2,
    )
    return blink_to_append_only(table)
