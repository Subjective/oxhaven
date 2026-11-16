"""The table_publisher module supports publishing Deephaven Tables into blink Tables.

Behavior: `deephaven.stream.table_publisher` (names, signatures and the checks made in
Python), `io.deephaven.stream.TablePublisher` for the rest, which is the engine's stream
table here.
"""

from typing import Callable, Optional

from .. import _engine, dtypes
from ..table import Table, TableDefinition
from ..update_graph import UpdateGraph


class TablePublisher:
    """The interface for publishing table data into a blink table."""

    def __init__(self, j_table_publisher):
        # what the native functions take; the reference holds a Java TablePublisher here
        self.j_table_publisher = j_table_publisher

    @property
    def j_object(self):
        return self.j_table_publisher

    @property
    def is_alive(self) -> bool:
        """Checks whether this is alive; if False, the caller should stop adding new data and
        release any related resources as soon as practicable since adding new data won't have
        any downstream effects.

        Once this is False, it will always remain False. For more prompt notifications,
        callers may prefer to use on_shutdown_callback during construction.
        """
        return self.j_table_publisher.is_alive()

    def add(self, table: Table) -> None:
        """Adds a snapshot of the data from table into the blink table. The table must
        contain a superset of the columns from the blink table's definition; the columns may
        be in any order. Columns from table that are not in the blink table's definition are
        ignored.

        All of the data from table will be consistent with a point in time, fully contained
        in a single blink table's update cycle, and non-interleaved with any other calls to
        add.
        """
        self.j_table_publisher.add(table.j_table)

    def publish_failure(self, failure: Exception) -> None:
        """Indicate that data publication has failed. Blink table listeners will be notified
        of the failure, the on-shutdown callback will be invoked if it hasn't already been,
        this publisher will no longer be alive, and future calls to add will silently return
        without publishing. These effects may resolve asynchronously.
        """
        self.j_table_publisher.publish_failure(str(failure))


def table_publisher(
    name: str,
    col_defs,
    on_flush_callback: Optional[Callable[[TablePublisher], None]] = None,
    on_shutdown_callback: Optional[Callable[[], None]] = None,
    update_graph: Optional[UpdateGraph] = None,
    chunk_size: int = 2048,
) -> tuple[Table, TablePublisher]:
    """Constructs a blink Table and TablePublisher to populate it.

    `name` is used for logging. The on-flush callback, if present, is called once at the
    beginning of each update graph cycle, with a TablePublisher of the table; the
    on-shutdown callback, if present, is called one time, when the caller should stop adding
    new data. The server has one update graph, which `update_graph` can only name. The chunk
    size must be positive.

    Returns:
        a two-tuple, where the first item is resulting blink Table and the second item is the
        TablePublisher
    """
    columns = [
        (column.name, dtypes._engine_type(column.data_type))
        for column in TableDefinition(col_defs).values()
    ]

    handle = None

    def flush():
        on_flush_callback(TablePublisher(j_table_publisher=handle))  # type: ignore[misc]

    j_table, handle = _engine.native.table_publisher(
        columns,
        flush if on_flush_callback else None,
        on_shutdown_callback,
        chunk_size,
    )
    return Table(j_table=j_table), TablePublisher(j_table_publisher=handle)
