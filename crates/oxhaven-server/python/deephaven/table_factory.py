"""Functions that create tables."""

from typing import Mapping, Optional, Sequence, Union

from . import _engine
from .column import InputColumn
from .dherror import DHError
from .table import Table


def empty_table(size: int) -> Table:
    """Creates a table with rows but no columns."""
    try:
        return Table(_engine.table_op("empty_table", {"size": int(size)}))
    except Exception as e:
        raise DHError(e, "failed to create an empty table.") from e


def time_table(
    period: Union[int, str],
    start_time: Optional[Union[int, str]] = None,
    blink_table: bool = False,
) -> Table:
    """Creates a table that adds a new row on a regular interval.

    `period` and `start_time` are an integer in nanoseconds or a string such as `"PT1s"`
    and `"2024-01-01T00:00:00 ET"`; the other duration and instant types of the reference
    package are not accepted yet.
    """
    try:
        request = {"blink_table": bool(blink_table)}
        for field, value in (("period", period), ("start_time", start_time)):
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                raise TypeError(f"{field} must be an int or a str, not {type(value).__name__}")
            request[f"{field}_nanos" if isinstance(value, int) else f"{field}_string"] = value
        return Table(_engine.table_op("time_table", request))
    except Exception as e:
        raise DHError(e, "failed to create a time table.") from e


def new_table(cols: Union[Sequence[InputColumn], Mapping[str, Sequence]]) -> Table:
    """Creates an in-memory table from a list of input columns.

    The mapping form of the reference package, which infers the column types, is not
    accepted yet.
    """
    try:
        if isinstance(cols, Mapping):
            raise TypeError("new_table from a mapping of column names to data is not supported yet")
        columns = []
        for col in cols:
            if not isinstance(col, InputColumn):
                raise TypeError(f"expected an InputColumn, not {type(col).__name__}")
            columns.append((col.name, col.data_type, list(col.input_data)))
        return Table(_engine.new_table(columns))
    except Exception as e:
        # the text is the reference's, "time" included
        raise DHError(e, "failed to create a new time table.") from e
