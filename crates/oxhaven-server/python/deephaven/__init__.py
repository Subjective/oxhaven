"""The Deephaven Python package of an Oxhaven server.

It offers the names and signatures of the `deephaven` package of the Java server
(`py/server/deephaven` in the reference), written anew over the Rust engine: a `Table` holds
a handle to an engine table, and every table operation is the operation of the same name in
the server's table service. `docs/CONSOLE.md` lists what exists.
"""

from .csv import read as read_csv
from .csv import write as write_csv
from .dherror import DHError
from .replay import TableReplayer
from .table import SortDirection
from .table_factory import (
    DynamicTableWriter,
    empty_table,
    function_generated_table,
    input_table,
    merge,
    merge_sorted,
    new_table,
    ring_table,
    time_table,
)

__all__ = [
    "DHError",
    "DynamicTableWriter",
    "SortDirection",
    "TableReplayer",
    "empty_table",
    "function_generated_table",
    "input_table",
    "merge",
    "merge_sorted",
    "new_table",
    "read_csv",
    "ring_table",
    "time_table",
    "write_csv",
]
