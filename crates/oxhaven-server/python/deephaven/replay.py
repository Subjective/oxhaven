"""Replaying historical data as live data. Not ported: the engine has no replayer."""

from . import dtypes, time
from .dherror import DHError
from .table import Table


class TableReplayer:
    """Not ported. The reference replays tables that have a column of instants as ticking
    tables, between a start time and an end time.

    Behavior: `deephaven.replay.TableReplayer`
    """

    def __init__(self, start_time, end_time):
        # the reference converts the times first, and so refuses what is not a time
        self.start_time = time.to_j_instant(start_time)
        self.end_time = time.to_j_instant(end_time)
        try:
            raise dtypes._java_time.NotPorted(
                "io.deephaven.engine.table.impl.replay.Replayer#Replayer(Instant, Instant)"
            )
        except Exception as e:
            raise DHError(e, "failed to create a replayer.") from e

    def add_table(self, table: Table, col: str) -> Table:
        try:
            raise dtypes._java_time.NotPorted(
                "io.deephaven.engine.table.impl.replay.Replayer#replay(Table, String)"
            )
        except Exception as e:
            raise DHError(e, "failed to add a historical table.") from e

    def start(self) -> None:
        try:
            raise dtypes._java_time.NotPorted("io.deephaven.engine.table.impl.replay.Replayer#start")
        except Exception as e:
            raise DHError(e, "failed to start the replayer.") from e

    def shutdown(self) -> None:
        try:
            raise dtypes._java_time.NotPorted(
                "io.deephaven.engine.table.impl.replay.Replayer#shutdown"
            )
        except Exception as e:
            raise DHError(e, "failed to shutdown the replayer.") from e
