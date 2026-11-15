"""Reading and writing CSV files.

Behavior: `deephaven.csv` (the names, signatures and error texts; `docs/CSV.md` has the
rules by which a column gets its type and the list of what is refused for now).
"""

from typing import Optional

from . import _engine, _java_time, dtypes
from .constants import MAX_LONG
from .dherror import DHError
from .table import Table

# The parser of `io.deephaven.csv.parsers.Parsers` for a type named in `header`; a type
# without one is a `KeyError`.
_PARSERS = {
    dtypes.bool_: "BOOLEAN",
    dtypes.byte: "BYTE",
    dtypes.char: "CHAR",
    dtypes.short: "SHORT",
    dtypes.int32: "INT",
    dtypes.long: "LONG",
    dtypes.float32: "FLOAT_FAST",
    dtypes.double: "DOUBLE",
    dtypes.string: "STRING",
    dtypes.Instant: "DATETIME",
}


def read(
    path: str,
    header: Optional[dict[str, dtypes.DType]] = None,
    headless: bool = False,
    header_row: int = 0,
    skip_rows: int = 0,
    num_rows: int = MAX_LONG,
    ignore_empty_lines: bool = False,
    allow_missing_columns: bool = False,
    ignore_excess_columns: bool = False,
    delimiter: str = ",",
    quote: str = '"',
    ignore_surrounding_spaces: bool = True,
    trim: bool = False,
) -> Table:
    """Reads the CSV file at `path` into a static in-memory table.

    The column names are those of the file's header row and each column gets the
    narrowest type that takes all of its cells, unless `header` names the columns and
    their types, in the order of the file. `headless` says that the file has no header
    row; `header_row` is the number of rows before it, `skip_rows` the number of data
    rows to pass over and `num_rows` the largest number of them to read.

    A row may have fewer cells than there are columns with `allow_missing_columns`, and
    more with `ignore_excess_columns`; `ignore_empty_lines` passes over empty lines.
    `delimiter` and `quote` are one character each. `ignore_surrounding_spaces` drops the
    blanks around a cell that is not quoted, and `trim` those inside the quotes of one
    that is.

    A URL, a compressed file and a date-time in a time zone that changes its offset are
    not supported yet.
    """
    try:
        columns = []
        if header:
            columns = [(name, _PARSERS[dtype]) for name, dtype in header.items()]
        return Table(_engine.native.read_csv(
            path,
            columns,
            not headless,
            header_row,
            skip_rows,
            num_rows,
            ignore_empty_lines,
            allow_missing_columns,
            ignore_excess_columns,
            ord(delimiter),
            ord(quote),
            ignore_surrounding_spaces,
            trim,
        ))
    except Exception as e:
        raise DHError(e, "read csv failed") from e


def write(table: Table, path: str, cols: list[str] = []) -> None:
    """Writes `table` to the CSV file at `path`, which is made or replaced.

    `cols` names the columns to write, in order; without it every column is written. A
    column of instants is not supported yet on a server whose time zone is not UTC, nor
    is a column of arrays or vectors.
    """
    try:
        zone = _java_time.system_zone().id
        _engine.native.write_csv(table.j_table, path, list(cols), zone)
    except Exception as e:
        raise DHError(e, "write csv failed.") from e
