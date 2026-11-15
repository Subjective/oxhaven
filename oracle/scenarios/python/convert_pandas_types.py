# A table as a DataFrame with each dtype_backend, with the types and the nulls pandas then
# holds, and back; DataFrames of the pandas types as tables.
# oracle: results nullable, arrow_backed, numpy_backed, numpy_raw, some, typed, inferred, kept_objects, times, categories, arrow_typed, renamed, chosen, ticked
import numpy as np
import pandas as pd
import pyarrow as pa

from deephaven import constants, empty_table, time_table
from deephaven.pandas import to_pandas, to_table

source = empty_table(3).update([
    "B = (byte)(i == 1 ? null : i)", "H = (short)(i == 1 ? null : i)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long)i", "F = (float)(i == 1 ? null : i * 0.5)",
    "D = i == 1 ? null : i * 0.5", "C = i == 1 ? null : (char)(65 + i)",
    "S = i == 1 ? null : `s` + i", "Z = i == 1 ? null : i == 0",
    "T = i == 1 ? null : '2024-01-01T00:00:00Z' + i * SECOND",
])
names = ["B", "H", "I", "L", "F", "D", "C", "S", "Z", "T"]


def kinds(frame):
    return [str(kind) for kind in frame.dtypes]


frame = to_pandas(source)
assert isinstance(frame, pd.DataFrame) and frame.columns.tolist() == names and len(frame) == 3
assert kinds(frame) == [
    "Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "UInt16", "string", "boolean",
    "datetime64[ns, UTC]"]
assert frame.isna().all(axis=1).tolist() == [False, True, False]
assert frame["I"].tolist()[::2] == [0, 2] and frame["I"][1] is pd.NA and frame["S"][1] is pd.NA
assert frame["S"].tolist()[::2] == ["s0", "s2"] and frame["Z"].tolist()[::2] == [True, False]
assert frame["T"][2] == pd.Timestamp("2024-01-01T00:00:02Z") and frame["T"][1] is pd.NaT
nullable = to_table(frame)

frame = to_pandas(source, dtype_backend="pyarrow")
assert kinds(frame) == [
    "int8[pyarrow]", "int16[pyarrow]", "int32[pyarrow]", "int64[pyarrow]", "float[pyarrow]",
    "double[pyarrow]", "uint16[pyarrow]", "string[pyarrow]", "bool[pyarrow]",
    "timestamp[ns, tz=UTC][pyarrow]"]
assert frame.isna().all(axis=1).tolist() == [False, True, False]
arrow_backed = to_table(frame)

frame = to_pandas(source, dtype_backend=None)
assert kinds(frame)[:7] == ["Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "UInt16"]
assert kinds(frame)[8:] == ["boolean", "datetime64[ns]"]
assert frame["I"][1] is pd.NA and frame["Z"][1] is pd.NA and frame["T"][1] is pd.NaT
# a null string is the text None
assert frame["S"].tolist() == ["s0", "None", "s2"]
numpy_backed = to_table(frame)

frame = to_pandas(source, dtype_backend=None, conv_null=False)
assert kinds(frame)[:7] == ["int8", "int16", "int32", "int64", "float32", "float64", "uint16"]
assert kinds(frame)[8:] == ["bool", "datetime64[ns]"]
assert frame["I"].tolist() == [0, constants.NULL_INT, 2] and frame["L"].tolist() == [0, constants.NULL_LONG, 2]
assert frame["Z"].tolist() == [True, True, False]
numpy_raw = to_table(frame)

for backend in ("numpy_nullable", "pyarrow", None):
    frame = to_pandas(source, cols=["S", "I"], dtype_backend=backend)
    assert frame.columns.tolist() == ["S", "I"] and len(frame) == 3, backend
some = to_table(to_pandas(source, cols=["S", "I"]))
assert to_pandas(source.head(0)).shape == (0, 10)
assert to_pandas(source.head(0), dtype_backend=None).shape == (0, 10)

NA = pd.NA
typed = to_table(pd.DataFrame({
    "Int8": pd.Series([1, NA, 3], dtype="Int8"),
    "Int16": pd.Series([1, NA, 3], dtype="Int16"),
    "Int32": pd.Series([1, NA, 3], dtype="Int32"),
    "Int64": pd.Series([1, NA, 3], dtype="Int64"),
    "UInt16": pd.Series([65, NA, 67], dtype="UInt16"),
    "Float32": pd.Series([1.5, NA, np.nan], dtype="Float32"),
    "Float64": pd.Series([1.5, NA, np.nan], dtype="Float64"),
    "Boolean": pd.Series([True, NA, False], dtype="boolean"),
    "String": pd.Series(["a", NA, "c"], dtype="string"),
    "int8": pd.Series([1, -128, 3], dtype="int8"),
    "int64": pd.Series([1, 2, 3]),
    "uint16": pd.Series([65, 65535, 67], dtype="uint16"),
    "float32": pd.Series([1.5, np.nan, 3.5], dtype="float32"),
    "float64": pd.Series([1.5, np.nan, None]),
    "bool": pd.Series([True, False, True]),
    "str": pd.Series(["a", None, "c"]),
}))
objects = pd.DataFrame({
    "Strings": pd.Series(["a", None, "c"], dtype=object),
    "Ints": pd.Series([1, None, 3], dtype=object),
    "Bools": pd.Series([True, None, False], dtype=object),
    "Floats": pd.Series([1.5, None, 2.5], dtype=object),
    "Plain": [1, 2, 3],
})
inferred = to_table(objects)
# the frame given is not changed
assert kinds(objects) == ["object", "object", "object", "object", "int64"]
kept_objects = to_table(objects, cols=["Plain"], infer_objects=False)
stamps = pd.to_datetime(["2024-01-01T00:00:00.000000", None, "2024-06-01T12:30:00.123456"])
times = to_table(pd.DataFrame({
    "Nanos": stamps.as_unit("ns"),
    "Micros": stamps.as_unit("us"),
    "Seconds": pd.to_datetime(["2024-01-01", None, "2024-06-01"]).as_unit("s"),
    "Utc": stamps.tz_localize("UTC"),
    "NewYork": stamps.tz_localize("America/New_York"),
}))
categories = to_table(pd.DataFrame({
    "Letters": pd.Series(["a", "b", "a"], dtype="category"),
    "Numbers": pd.Series([1, 2, 1], dtype="category"),
}))
arrow_typed = to_table(pd.DataFrame({
    "Int": pd.Series([1, NA, 3], dtype=pd.ArrowDtype(pa.int32())),
    "Long": pd.Series([1, NA, 3], dtype=pd.ArrowDtype(pa.int64())),
    "Bool": pd.Series([True, NA, False], dtype=pd.ArrowDtype(pa.bool_())),
    "Double": pd.Series([1.5, NA, 2.5], dtype=pd.ArrowDtype(pa.float64())),
    "String": pd.Series(["a", NA, "c"], dtype=pd.ArrowDtype(pa.string())),
    "Naive": pd.Series([pd.Timestamp("2024-01-01"), NA, pd.Timestamp("2024-01-02")],
                       dtype=pd.ArrowDtype(pa.timestamp("ns"))),
    "List": pd.Series([[1, 2], None, []], dtype=pd.ArrowDtype(pa.list_(pa.int32()))),
    "Plain": [1.5, 2.5, 3.5],
}))
renamed = to_table(pd.DataFrame({"a b": [1], "A-B.c": [2.5], "x": ["y"]}))
indexed = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"], "C": [1.5, 2.5, 3.5]}, index=[30, 10, 20])
chosen = to_table(indexed, cols=["C", "A"])

# a refreshing table is read at one moment
ticking = time_table("PT0.05S").update("X = ii")
frame = to_pandas(ticking)
assert frame.columns.tolist() == ["Timestamp", "X"] and frame["X"].tolist() == list(range(len(frame)))
frame = to_pandas(ticking, dtype_backend=None)
assert frame["X"].tolist() == list(range(len(frame))) and len(frame["Timestamp"]) == len(frame)
ticked = to_table(frame.head(0))
del ticking
