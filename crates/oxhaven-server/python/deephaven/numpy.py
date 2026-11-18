"""Conversion between Deephaven tables and numpy arrays.

Behavior: `deephaven.numpy` of the reference package. A numpy array holds no nulls, so a
null of a primitive column is the value Java reserves for null in its type (the
constants of `deephaven.constants`), a null `Boolean` is `True`, a null string the text
`None` and a null instant `NaT`, as in the reference.
"""

import re
from typing import Optional, Sequence

import numpy as np

from . import _engine, _java_time, dtypes
from .column import ColumnDefinition, InputColumn
from .dherror import DHError
from .dtypes import BusinessCalendar, DType
from .table import Table
from .table_factory import new_table


def _to_column_name(name: str) -> str:
    """A column name from any text: its runs of letters, digits and underscores, joined
    by underscores."""
    return "_".join(re.findall(r"\w+", str(name)))


def _values_to_numpy_array(dtype: DType, values) -> np.ndarray:
    """The numpy array of one column's values as the engine hands them out
    (`_oxhaven.column_values`): a `bytearray` of the values of a type of fixed width, a
    list of Python values for any other. Nulls are not converted.

    Behavior: `deephaven.jcompat._j_array_to_numpy_array` with `conv_null=False`
    """
    if isinstance(values, bytearray):
        if dtype == dtypes.Instant:
            return np.frombuffer(values, np.int64).view(np.dtype(dtype.np_type))
        return np.frombuffer(values, dtype.np_type)
    if dtype == dtypes.string:
        return np.array(values, dtypes.string.np_type)
    return np.array(values, np.object_)


def _column_to_numpy_array(col_def: ColumnDefinition, values) -> np.ndarray:
    """The numpy array of the values of the column `col_def`."""
    try:
        return _values_to_numpy_array(col_def.data_type, values)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, f"failed to create a numpy array for the column {col_def.name}") from e


def _columns_to_2d_numpy_array(col_def: ColumnDefinition, columns: list) -> np.ndarray:
    """One array of the values of several columns of one type, a column of the table
    along the second axis."""
    try:
        if col_def.data_type.is_primitive:
            np_type = col_def.data_type.np_type
            stacked = np.empty(shape=(len(columns[0]) // np.dtype(np_type).itemsize, len(columns)), dtype=np_type)
            for index, values in enumerate(columns):
                stacked[:, index] = np.frombuffer(values, np_type)
            return stacked
        return np.stack([_column_to_numpy_array(col_def, values) for values in columns], axis=1)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, f"failed to create a numpy array for the column {col_def.name}") from e


def _make_input_column(col: str, np_array: np.ndarray, dtype: DType) -> InputColumn:
    """An input column of a numpy array, named by `col` made a legal column name."""
    return InputColumn(name=_to_column_name(col), data_type=dtype, input_data=np_array)


def to_numpy(table: Table, cols: Optional[Sequence[str]] = None) -> np.ndarray:
    """Creates a numpy array of a consistent snapshot of the columns `cols` of a table,
    or of all its columns. The columns must have one data type; the array has a row per
    row of the table and a column per column.
    """
    try:
        # as the reference, which snapshots a refreshing table first
        table.is_refreshing
        table_def = table.definition
        if not cols:
            cols = list(table_def.keys())
        else:
            missing = set(cols) - set(table_def.keys())
            if missing:
                raise DHError(message=f"columns - {list(missing)} not found")
        col_defs = [table_def[col] for col in cols]
        if len({col_def.data_type for col_def in col_defs}) != 1:
            raise DHError(message="columns must be of the same data type.")
        columns = _engine.native.column_values(table.j_table, [col_def.name for col_def in col_defs])
        return _columns_to_2d_numpy_array(col_defs[0], columns)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a numpy array from the table column.") from e


def to_table(np_array: np.ndarray, cols: Sequence[str]) -> Table:
    """Creates a table from a numpy array of two dimensions, each column of the array a
    column of the table named by `cols`.
    """
    try:
        _, *dims = np_array.shape
        if dims and (not cols or len(cols) != dims[0]):
            raise DHError(
                message=f"the number of array columns {dims[0]} doesn't match "
                f"the number of column names {len(cols)}"
            )
        dtype = dtypes.from_np_dtype(np_array.dtype)
        if len(cols) == 1:
            input_cols = [_make_input_column(cols[0], np_array.T[0], dtype)]
        else:
            input_cols = [
                _make_input_column(col, np_array[:, [index]].T[0], dtype) for index, col in enumerate(cols)
            ]
        return new_table(cols=input_cols)
    except DHError:
        raise
    except Exception as e:
        # the text is the reference's
        raise DHError(e, "failed to create a Deephaven Table from a Pandas DataFrame.") from e


def to_np_busdaycalendar(cal: BusinessCalendar, include_partial: bool = True) -> np.busdaycalendar:
    """Creates a numpy business day calendar from a Java BusinessCalendar.

    Partial holidays in the business calendar are interepreted as full holidays in the numpy business day calendar.

    Args:
        cal (BusinessCalendar): the Java BusinessCalendar
        include_partial (bool): whether to include partial holidays in the numpy business day calendar, default is True

    Returns:
        a numpy busdaycalendar

    Raise:
        DHError
    """
    if not cal:
        raise DHError(message="cal must not be None")
    elif not isinstance(cal, BusinessCalendar.j_type):
        raise DHError(message="cal must be a Java BusinessCalendar")

    try:
        weekend = cal.weekendDays()
        weekmask = "".join("0" if weekend.contains(day) else "1" for day in _java_time.DayOfWeek.values())
        holidays = cal.holidays()
        dates = [
            np.datetime64(str(date), "D")
            for date in holidays.keySet().toArray()
            if holidays.get(date).businessNanos() == 0 or include_partial
        ]
        return np.busdaycalendar(weekmask=weekmask, holidays=dates)
    except Exception as e:
        raise DHError(e, "failed to create a numpy busdaycalendar from a Java BusinessCalendar.") from e
