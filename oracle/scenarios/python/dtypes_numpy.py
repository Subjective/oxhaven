# Input columns from numpy arrays, the DType of a numpy dtype, and dtypes.array.
# oracle: results from_arrays, np_types, built
import numpy as np

from deephaven import dtypes, new_table
from deephaven.column import (
    bool_col, byte_col, datetime_col, double_col, float_col, int_col, long_col, short_col,
    string_col,
)

from_arrays = new_table([
    bool_col("Bool", np.array([True, False, True])),
    byte_col("Byte", np.array([1, -1, 127], dtype=np.int8)),
    short_col("Short", np.array([300, -300, 0], dtype=np.int16)),
    int_col("Int", np.array([70000, -70000, 0], dtype=np.int32)),
    long_col("Long", np.array([5000000000, -5000000000, 0], dtype=np.int64)),
    float_col("Float", np.array([1.5, np.nan, np.inf], dtype=np.float32)),
    double_col("Double", np.array([2.25, np.nan, -np.inf])),
    string_col("String", np.array(["x", "y", "z"])),
    int_col("Narrowed", np.array([2**32 + 1, 2, 3], dtype=np.int64)),
    double_col("FromInts", np.array([1, 2, 3], dtype=np.int32)),
    datetime_col("Instant", np.array(
        ["2024-01-02T03:04:05.123456789", "1970-01-01", "NaT"], dtype="datetime64[ns]")),
])
kinds = [np.bool_, np.int8, np.int16, np.int32, np.int64, np.float32, np.float64, np.str_,
         np.object_, np.dtype("datetime64[ns]"), np.dtype("U3"), np.uint16, np.uint8]
np_types = new_table([
    string_col("Numpy", [str(np.dtype(kind)) for kind in kinds]),
    string_col("DType", [repr(dtypes.from_np_dtype(np.dtype(kind))) for kind in kinds]),
])
built = new_table([
    string_col("Call", ["int32", "int32 remap", "double", "string", "bool_", "long from numpy"]),
    string_col("Values", [str(list(a)) for a in (
        dtypes.array(dtypes.int32, [1, None, 2**32 + 3]),
        dtypes.array(dtypes.int32, [1, None], remap=lambda v: 9 if v is None else v),
        dtypes.array(dtypes.double, [1, 2.5, None]),
        dtypes.array(dtypes.string, ["a", None]),
        dtypes.array(dtypes.bool_, [True, None, 0]),
        dtypes.array(dtypes.long, np.array([1, 2], dtype=np.int64)),
    )]),
])
