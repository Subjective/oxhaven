"""Functions that create tables."""

from typing import Mapping, Sequence, Union

from . import _engine, dtypes
from .column import InputColumn
from .dherror import DHError
from .table import Table


def empty_table(size: int) -> Table:
    """Creates a table with rows but no columns."""
    try:
        return Table(_engine.table_op("empty_table", {"size": int(size)}))
    except Exception as e:
        raise DHError(e, "failed to create an empty table.") from e


def _nanos_of_duration(period) -> int:
    """The nanoseconds of a duration given as an integer, a `datetime.timedelta`, a
    `numpy.timedelta64` or a `pandas.Timedelta`.

    Behavior: `deephaven.time.to_j_duration`, for everything but a string
    """
    import datetime
    import sys

    if isinstance(period, int) and not isinstance(period, bool):
        return period
    pandas, numpy = sys.modules.get("pandas"), sys.modules.get("numpy")
    if pandas is not None and isinstance(period, pandas.Timedelta):
        return period.value
    if isinstance(period, datetime.timedelta):
        return int(period / datetime.timedelta(microseconds=1) * 1000)
    if numpy is not None and isinstance(period, numpy.timedelta64):
        return int(period.astype("timedelta64[ns]").astype(numpy.int64))
    raise TypeError("Unsupported conversion: " + str(type(period)) + " -> Duration")


def _nanos_of_instant(instant) -> int:
    """The nanoseconds since the epoch of an instant given as an integer, a
    `datetime.datetime` (a `pandas.Timestamp` is one) or a `numpy.datetime64`.

    Behavior: `deephaven.time.to_j_instant`, for everything but a string
    """
    import datetime
    import sys

    if isinstance(instant, int) and not isinstance(instant, bool):
        return instant
    numpy = sys.modules.get("numpy")
    if isinstance(instant, datetime.datetime):
        # the reference's arithmetic, in floating point as there
        epoch_time = instant.timestamp()
        seconds = int(epoch_time)
        return seconds * 1000000000 + int((epoch_time - seconds) * 1000000000)
    if numpy is not None and isinstance(instant, numpy.datetime64):
        return int(instant.astype("datetime64[ns]").astype(numpy.int64))
    raise TypeError("Unsupported conversion: " + str(type(instant)) + " -> Instant")


def _is_null(value) -> bool:
    """Whether `value` is what `pandas.isnull` calls null: `None`, a NaN, a not-a-time."""
    import sys

    if value is None:
        return True
    pandas = sys.modules.get("pandas")
    if pandas is not None:
        # a boolean for one value, an array for a collection
        null = pandas.isnull(value)
        return bool(null) if getattr(null, "ndim", 0) == 0 else False
    return isinstance(value, float) and value != value


def time_table(period, start_time=None, blink_table: bool = False) -> Table:
    """Creates a table that adds a new row on a regular interval.

    `period` is an integer in nanoseconds, a string such as `"PT1s"`, a
    `datetime.timedelta`, a `numpy.timedelta64` or a `pandas.Timedelta`. `start_time` is an
    integer in nanoseconds since the epoch, a string such as `"2024-01-01T00:00:00 ET"`, a
    `datetime.datetime`, a `numpy.datetime64` or a `pandas.Timestamp`; without it the table
    starts now.
    """
    try:
        if period is None:
            raise ValueError("period must be specified")
        if _is_null(period):
            # the reference hands Java a null duration, which it refuses
            raise ValueError("period must not be null")
        request = {"blink_table": bool(blink_table)}
        if isinstance(period, str):
            request["period_string"] = period
        else:
            request["period_nanos"] = _nanos_of_duration(period)
        if start_time:
            if isinstance(start_time, str):
                request["start_time_string"] = start_time
            elif not _is_null(start_time):
                request["start_time_nanos"] = _nanos_of_instant(start_time)
        return Table(_engine.table_op("time_table", request))
    except Exception as e:
        raise DHError(e, "failed to create a time table.") from e


def _engine_values(data_type, values) -> list:
    """The values of an input column as the engine takes them: an instant as its
    nanoseconds since the epoch, in an array cell too."""
    name = data_type.j_name
    if name == "java.time.Instant":
        return [None if value is None else value.epoch_nanos() for value in values]
    component = dtypes._component_name(name)
    if component is not None:
        component = dtypes._from_java_name(component)
        return [None if value is None else _engine_values(component, value) for value in values]
    return list(values)


def new_table(cols: Union[Sequence[InputColumn], Mapping[str, Sequence]]) -> Table:
    """Creates an in-memory table from a list of input columns, or from a mapping of
    column names to values, whose column types are those pandas infers.

    Each column has the same number of values. Of two input columns with one name the
    later one replaces the earlier in its place, as in the reference.
    """
    try:
        if isinstance(cols, Mapping):
            import pandas as pd

            from .pandas import to_table

            return to_table(pd.DataFrame(cols).convert_dtypes())
        columns = {}
        for col in cols:
            # the reference reads `j_column` of whatever it is given
            values = col.j_column
            definition = col._column_definition
            columns[definition.name] = (
                definition.name,
                col._engine_type,
                _engine_values(definition.data_type, values),
            )
        return Table(_engine.new_table(list(columns.values())))
    except Exception as e:
        # the text is the reference's, "time" included
        raise DHError(e, "failed to create a new time table.") from e


def merge(tables: Sequence[Table]) -> Table:
    """Creates a table of the rows of the tables, which have the same columns, one table
    after another."""
    try:
        handles = [table.j_table for table in tables]
        request = {"source_ids": [_engine.source(index) for index in range(len(handles))]}
        return Table(_engine.table_op("merge", request, handles))
    except Exception as e:
        raise DHError(e, "merge tables operation failed.") from e


def merge_sorted(tables: Sequence[Table], order_by: str) -> Table:
    """Creates a table of the rows of the tables, each of which is sorted by the column
    `order_by`, in the order of that column."""
    try:
        handles = [table.j_table for table in tables]
        if any(handle.is_refreshing() for handle in handles):
            # the table service sorts a merge of refreshing tables, which Java's
            # `TableTools.mergeSorted` does not
            return Table(_engine.local_op("merge_sorted", {"order_by": order_by}, handles))
        request = {
            "source_ids": [_engine.source(index) for index in range(len(handles))],
            "key_column": order_by,
        }
        return Table(_engine.table_op("merge", request, handles))
    except Exception as e:
        raise DHError(e, "merge sorted operation failed.") from e
