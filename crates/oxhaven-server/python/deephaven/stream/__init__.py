"""Tables fed by streams of data.

Behavior: `deephaven.stream`. Only `deephaven.stream.kafka.consumer` is built so far
(`docs/KAFKA.md`); the functions of this module name the Java method they would call.
"""

from .._java_time import NotPorted
from ..table import Table


def blink_to_append_only(table: Table) -> Table:
    raise NotPorted("io.deephaven.engine.table.impl.BlinkTableTools#blinkToAppendOnly")


def add_only_to_blink(table: Table) -> Table:
    raise NotPorted("io.deephaven.engine.table.impl.AddOnlyToBlinkTableAdapter#toBlink")
