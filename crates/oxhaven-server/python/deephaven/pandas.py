"""Conversion between Deephaven tables and pandas DataFrames.

Behavior: `deephaven.pandas` of the reference package. With a `dtype_backend` a table
becomes a DataFrame by way of pyarrow (`deephaven.arrow`), and a null is `pandas.NA`.
Without one the columns are numpy arrays (`deephaven.numpy`), whose nulls are converted
to `pandas.NA` in nullable pandas types, or with `conv_null=False` left as the values
Java reserves for null.
"""

from typing import List, Literal, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.api.types import is_object_dtype

from . import _engine, arrow, dtypes
from .column import ColumnDefinition
from .constants import (
    NULL_BYTE,
    NULL_CHAR,
    NULL_DOUBLE,
    NULL_FLOAT,
    NULL_INT,
    NULL_LONG,
    NULL_SHORT,
)
from .dherror import DHError
from .numpy import _make_input_column, _values_to_numpy_array
from .table import Table
from .table_factory import new_table

# Behavior: `io.deephaven.util.BooleanUtils#NULL_BOOLEAN_AS_BYTE`
_NULL_BOOLEAN_AS_BYTE = NULL_BYTE
# as the reference decides it: a comparison of the version texts
_is_dtype_backend_supported = pd.__version__ >= "2.0.0"

# The nullable pandas type that holds a column of each type with nulls, and the value
# Java reserves for null in that type.
_NULLABLE = {
    dtypes.bool_: (pd.BooleanDtype, _NULL_BOOLEAN_AS_BYTE),
    dtypes.byte: (pd.Int8Dtype, NULL_BYTE),
    dtypes.short: (pd.Int16Dtype, NULL_SHORT),
    dtypes.char: (pd.UInt16Dtype, NULL_CHAR),
    dtypes.int32: (pd.Int32Dtype, NULL_INT),
    dtypes.int64: (pd.Int64Dtype, NULL_LONG),
    dtypes.float32: (pd.Float32Dtype, NULL_FLOAT),
    dtypes.float64: (pd.Float64Dtype, NULL_DOUBLE),
}


def _values_to_series(dtype: dtypes.DType, values, conv_null: bool) -> pd.Series:
    """A Series of one column's values as the engine hands them out.

    Behavior: `deephaven.jcompat._j_array_to_series`
    """
    if conv_null and dtype == dtypes.bool_:
        as_bytes = pd.Series(data=np.frombuffer(values, dtype=np.byte), dtype=pd.Int8Dtype(), copy=False)
        as_bytes.mask(as_bytes == _NULL_BOOLEAN_AS_BYTE, inplace=True)
        return as_bytes.astype(pd.BooleanDtype(), copy=False)
    np_array = _values_to_numpy_array(dtype, values)
    if conv_null and dtype in _NULLABLE:
        nullable, null_value = _NULLABLE[dtype]
        series = pd.Series(data=np_array, dtype=nullable(), copy=False)
        series.mask(series == null_value, inplace=True)
        return series
    return pd.Series(data=np_array, copy=False)


def _column_to_series(col_def: ColumnDefinition, values, conv_null: bool) -> pd.Series:
    try:
        return _values_to_series(col_def.data_type, values, conv_null)
    except DHError:
        raise
    except Exception as e:
        # the text is the reference's, which does not fill in the column
        raise DHError(e, message="failed to create a pandas Series for {col}") from e


def _arrow_dtypes() -> dict:
    types = [pa.int8(), pa.int16(), pa.int32(), pa.int64(), pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()]
    types += [pa.bool_(), pa.float32(), pa.float64(), pa.string(), pa.timestamp("ns"), pa.timestamp("ns", tz="UTC")]
    return {arrow_type: pd.ArrowDtype(arrow_type) for arrow_type in types}


# The pandas type of a column of each pyarrow type, per `dtype_backend`; a column of any
# other pyarrow type has the type pyarrow chooses.
_TYPE_MAPPERS = {
    "pyarrow": _arrow_dtypes().get,
    "numpy_nullable": {
        pa.int8(): pd.Int8Dtype(),
        pa.int16(): pd.Int16Dtype(),
        pa.uint16(): pd.UInt16Dtype(),
        pa.int32(): pd.Int32Dtype(),
        pa.int64(): pd.Int64Dtype(),
        pa.bool_(): pd.BooleanDtype(),
        pa.float32(): pd.Float32Dtype(),
        pa.float64(): pd.Float64Dtype(),
        pa.string(): pd.StringDtype(),
    }.get,
}


def to_pandas(
    table: Table,
    cols: Optional[List[str]] = None,
    dtype_backend: Literal[None, "pyarrow", "numpy_nullable"] = "numpy_nullable",
    conv_null: bool = True,
) -> pd.DataFrame:
    """Creates a DataFrame from a consistent snapshot of a table, or of the columns
    `cols` of it.

    `dtype_backend` chooses the types of the DataFrame's columns: nullable pandas types
    (`"numpy_nullable"`, the default), pyarrow types (`"pyarrow"`), or numpy types
    (`None`). `conv_null` says whether nulls become `pandas.NA`; it can be turned off
    only without a `dtype_backend`.
    """
    try:
        if dtype_backend == "pyarrow" and not _is_dtype_backend_supported:
            raise DHError(
                message=f"the dtype_backend ({dtype_backend}) option is only available for pandas 2.0.0 and "
                f"above. {pd.__version__} is being used."
            )
        if dtype_backend is not None and not conv_null:
            raise DHError(
                message="conv_null can't be turned off when dtype_backend is either numpy_nullable or pyarrow"
            )
        types_mapper = _TYPE_MAPPERS.get(str(dtype_backend))
        if types_mapper:
            return arrow.to_arrow(table=table, cols=cols).to_pandas(types_mapper=types_mapper)

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
        columns = _engine.native.column_values(table.j_table, [col_def.name for col_def in col_defs])
        data = {
            col: _column_to_series(col_def, values, conv_null)
            for col, col_def, values in zip(cols, col_defs, columns)
        }
        return pd.DataFrame(data=data, columns=cols, copy=False)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a pandas DataFrame from table.") from e


def _null_value(pd_dtype):
    """The value Java reserves for null in the column type of a nullable pandas type or
    of a pyarrow type; `None` for a type that holds its nulls as they are."""
    for nullable, null_value in _NULLABLE.values():
        if isinstance(pd_dtype, nullable):
            return null_value
    if isinstance(pd_dtype, pd.ArrowDtype):
        arrow_type = pd_dtype.pyarrow_dtype
        if arrow_type == pa.bool_() or arrow_type == pa.int8():
            return NULL_BYTE
        by_type = {pa.int16(): NULL_SHORT, pa.int32(): NULL_INT, pa.int64(): NULL_LONG}
        by_type.update({pa.float32(): NULL_FLOAT, pa.float64(): NULL_DOUBLE})
        return by_type.get(arrow_type)
    return None


def _map_na(array):
    """The values of a pandas column with the engine's null values in place of
    `pandas.NA`. In a nullable float column a NaN stays a NaN: pandas marks both as
    missing, and only an entry that is missing without being a NaN is a null.
    """
    if isinstance(array, np.ndarray):
        return array
    pd_dtype = array.dtype
    null_value = _null_value(pd_dtype)
    if isinstance(pd_dtype, (pd.Float32Dtype, pd.Float64Dtype)) and isinstance(getattr(array, "_data"), np.ndarray):
        values = array._data
        nulls = np.logical_and(array._mask, np.logical_not(np.isnan(values)))
        if any(nulls):
            values = np.copy(values)
            values[nulls] = null_value
        return values
    if isinstance(pd_dtype, pd.BooleanDtype) or pd_dtype == pd.ArrowDtype(pa.bool_()):
        return np.array([null_value if v is pd.NA else v for v in array])
    if isinstance(pd_dtype, pd.StringDtype):
        return np.array([None if (v is pd.NA or v is np.nan) else v for v in array])
    if null_value is not None:
        return array.fillna(null_value)
    return array


def to_table(df: pd.DataFrame, cols: Optional[List[str]] = None, infer_objects: bool = True) -> Table:
    """Creates a static table from a DataFrame, or from the columns `cols` of it.

    With `infer_objects`, a column of Python objects gets the type pandas infers for its
    values. A column of a pyarrow type goes through `deephaven.arrow.to_table`, with every
    other column of the frame.
    """
    if not cols:
        cols = df.columns.tolist()
    else:
        missing = set(cols) - set(df.columns.tolist())
        if missing:
            raise DHError(message=f"columns - {list(missing)} not found")

    frame = df
    if infer_objects:
        frame = df[cols]
        for col in cols:
            if is_object_dtype(df.dtypes[col]):
                frame[col] = df[col].convert_dtypes()

    if _is_dtype_backend_supported and any(isinstance(frame[col].dtype, pd.ArrowDtype) for col in cols):
        try:
            return arrow.to_table(pa.Table.from_pandas(df=frame, columns=cols))
        except Exception as e:
            # The reference goes on to the numpy types whatever failed. A failure for
            # something Java reads and this server does not yet is not one Java has:
            # going on would give a table where Java gives another.
            if isinstance(e.__cause__, NotImplementedError):
                raise

    try:
        input_cols = []
        for col in cols:
            series = frame.get(col)
            assert series is not None
            values = series.values
            if isinstance(frame.dtypes[col], pd.CategoricalDtype):
                np_dtype = frame.dtypes[col].categories.dtype
            else:
                np_dtype = values.dtype
            dh_dtype = dtypes.from_np_dtype(np_dtype)
            input_cols.append(_make_input_column(col, _map_na(values), dh_dtype))
        return new_table(cols=input_cols)
    except DHError:
        raise
    except Exception as e:
        raise DHError(e, "failed to create a Deephaven Table from a pandas DataFrame.") from e
