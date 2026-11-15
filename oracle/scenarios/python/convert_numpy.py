# Columns of a table as numpy arrays, with the values numpy holds in place of nulls, and
# numpy arrays of two dimensions as tables.
# oracle: results longs, doubles, floats, strings, bools, instants, chars, single, renamed, no_rows
import numpy as np

from deephaven import constants, empty_table
from deephaven.numpy import to_numpy, to_table

source = empty_table(3).update([
    "B = (byte)(i == 1 ? null : i)", "H = (short)(i == 1 ? null : i)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long)i", "F = (float)(i == 1 ? null : i * 0.5)",
    "D = i == 1 ? null : i * 0.5", "C = i == 1 ? null : (char)(65 + i)",
    "S = i == 1 ? null : `s` + i", "Z = i == 1 ? null : i == 0",
    "T = i == 1 ? null : '2024-01-01T00:00:00Z' + i * SECOND", "I2 = i * 10",
])
expected = {
    "B": ("int8", [0, constants.NULL_BYTE, 2]), "H": ("int16", [0, constants.NULL_SHORT, 2]),
    "I": ("int32", [0, constants.NULL_INT, 2]), "L": ("int64", [0, constants.NULL_LONG, 2]),
    "F": ("float32", [0.0, constants.NULL_FLOAT, 1.0]), "D": ("float64", [0.0, constants.NULL_DOUBLE, 1.0]),
    "C": ("uint16", [65, constants.NULL_CHAR, 67]), "S": ("<U4", ["s0", "None", "s2"]),
    "Z": ("bool", [True, True, False]),
}
for name, (kind, column) in expected.items():
    array = to_numpy(source, [name])
    assert isinstance(array, np.ndarray) and array.shape == (3, 1), name
    assert str(array.dtype) == kind and array[:, 0].tolist() == column, (name, array)
times = to_numpy(source, ["T"])
assert str(times.dtype) == "datetime64[ns]" and times.shape == (3, 1)
assert [str(time) for time in times[:, 0]] == [
    "2024-01-01T00:00:00.000000000", "NaT", "2024-01-01T00:00:02.000000000"]
both = to_numpy(source, ["I2", "I"])
assert both.shape == (3, 2) and both.tolist() == [[0, 0], [10, constants.NULL_INT], [20, 2]]
# the array is the caller's own
both[0, 0] = 7
assert to_numpy(source, ["I2"])[0, 0] == 0
assert to_numpy(source.view("I"))[2, 0] == 2
assert to_numpy(source.head(0), ["I"]).shape == (0, 1)
assert to_numpy(source.update("S2 = S + `x`"), ["S", "S2"]).tolist() == [
    ["s0", "s0x"], ["None", "nullx"], ["s2", "s2x"]]

longs = to_table(np.array([[1, 2], [3, 4], [5, 6]]), ["X", "Y"])
doubles = to_table(np.array([[1.5, np.nan], [constants.NULL_DOUBLE, np.inf]]), ["X", "Y"])
floats = to_table(np.array([[1.5, 2.5], [3.5, 4.5]], dtype=np.float32), ["X", "Y"])
strings = to_table(np.array([["a", "bc"], ["def", ""]]), ["X", "Y"])
bools = to_table(np.array([[True, False], [False, True]]), ["X", "Y"])
instants = to_table(
    np.array([["2024-01-01T00:00:00", "NaT"], ["1970-01-01", "2262-01-01"]], dtype="datetime64[ns]"),
    ["X", "Y"])
chars = to_table(np.array([[65, 66], [67, constants.NULL_CHAR]], dtype=np.uint16), ["X", "Y"])
single = to_table(np.array([[1], [3]], dtype=np.int32), ["Only"])
renamed = to_table(np.array([[1, 2]], dtype=np.int8), ["a b", "c-d.e"])
no_rows = to_table(np.zeros((0, 2)), ["X", "Y"])
# a table to numpy and back
again = to_numpy(to_table(to_numpy(source, ["I2", "I"]), ["P", "Q"]))
assert again.tolist() == [[0, 0], [10, constants.NULL_INT], [20, 2]]
