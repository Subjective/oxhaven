"""The Deephaven Python package of an Oxhaven server.

It offers the names and signatures of the `deephaven` package of the Java server
(`py/server/deephaven` in the reference), written anew over the Rust engine: a `Table` holds
a handle to an engine table, and every table operation is the operation of the same name in
the server's table service. `docs/CONSOLE.md` lists what exists.
"""

from .dherror import DHError
from .table_factory import empty_table, new_table, time_table

__all__ = ["DHError", "empty_table", "new_table", "time_table"]
