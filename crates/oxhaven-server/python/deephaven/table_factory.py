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


def _mapping_values(array):
    """The values of a pandas column with the engine's null values in place of
    `pandas.NA`. In a nullable float column a NaN stays a NaN: pandas marks both as missing,
    and only an entry that is missing without being a NaN is a null.

    Behavior: `deephaven.pandas._map_na`, for the nullable types `convert_dtypes` gives
    """
    import numpy as np
    import pandas as pd

    from . import constants

    if isinstance(array, np.ndarray):
        return array
    pd_dtype = array.dtype
    nulls = {
        pd.Int8Dtype: constants.NULL_BYTE,
        pd.Int16Dtype: constants.NULL_SHORT,
        pd.UInt16Dtype: constants.NULL_CHAR,
        pd.Int32Dtype: constants.NULL_INT,
        pd.Int64Dtype: constants.NULL_LONG,
        pd.Float32Dtype: constants.NULL_FLOAT,
        pd.Float64Dtype: constants.NULL_DOUBLE,
    }
    if isinstance(pd_dtype, (pd.Float32Dtype, pd.Float64Dtype)):
        values = np.copy(array._data)
        values[np.logical_and(array._mask, np.logical_not(np.isnan(values)))] = nulls[type(pd_dtype)]
        return values
    if isinstance(pd_dtype, pd.BooleanDtype):
        # the null of a Boolean held as a byte, `BooleanUtils#NULL_BOOLEAN_AS_BYTE`
        return np.array([constants.NULL_BYTE if v is pd.NA else v for v in array])
    if isinstance(pd_dtype, pd.StringDtype):
        return np.array([None if (v is pd.NA or v is np.nan) else v for v in array])
    if type(pd_dtype) in nulls:
        return array.fillna(nulls[type(pd_dtype)])
    return array


def _mapping_columns(cols: Mapping) -> list:
    """Input columns from a mapping of column names to values, with the types pandas
    infers for them: integers are `long`, and in a column of floating point numbers `None`
    is a NaN, not a null.

    Behavior: `deephaven.table_factory.new_table` for a mapping, which makes a
    `pandas.DataFrame`, calls `convert_dtypes` and hands it to `deephaven.pandas.to_table`.
    This is that function for the frame made here; `deephaven.pandas` itself is task 6.9.
    """
    import re

    import pandas as pd

    frame = pd.DataFrame(cols).convert_dtypes()
    columns = []
    for label in frame.columns.tolist():
        values = frame[label].values
        pd_dtype = frame.dtypes[label]
        if isinstance(pd_dtype, pd.ArrowDtype):
            raise NotImplementedError(
                "not ported: deephaven.arrow.to_table, for a pandas column of an Arrow type"
            )
        if isinstance(pd_dtype, pd.CategoricalDtype):
            pd_dtype = pd_dtype.categories.dtype
        else:
            pd_dtype = values.dtype
        # Behavior: `deephaven.numpy._to_column_name`
        name = re.sub(r"\s+", "_", re.sub(r"\W+", " ", str(label)).strip())
        columns.append(
            InputColumn(
                name=name,
                data_type=dtypes.from_np_dtype(pd_dtype),
                input_data=_mapping_values(values),
            )
        )
    return columns


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
            cols = _mapping_columns(cols)
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
        # the table service's `merge` with a key column sorts a merge of refreshing tables
        # and takes at least one table; Java's `TableTools.mergeSorted` does neither
        return Table(_engine.local_op("merge_sorted", {"order_by": order_by}, handles))
    except Exception as e:
        raise DHError(e, "merge sorted operation failed.") from e
