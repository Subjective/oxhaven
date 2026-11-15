# The rows of a table as Python values, one at a time and a chunk at a time: instants and
# arrays among them. The result tables are built from what was read.
# oracle: results from_rows, from_chunks, lengths
import numpy as np

from deephaven import constants, dtypes, empty_table, new_table
from deephaven.column import InputColumn, datetime_col, int_col, long_col, string_col

source = empty_table(3).update([
    "I = i == 1 ? null : i", "S = i == 1 ? null : `s` + i",
    "T = i == 1 ? null : '2024-01-01T00:00:00Z' + i * SECOND",
])
rows = list(source.iter_dict())
assert [row["I"] for row in rows] == [0, constants.NULL_INT, 2] and [row["S"] for row in rows] == ["s0", None, "s2"]
assert rows[1]["T"] is None and str(rows[0]["T"]) == "2024-01-01T00:00:00Z"
assert str(rows[2]["T"]) == "2024-01-01T00:00:02Z"
tuples = list(source.iter_tuple(["I", "T"], tuple_name="Row", chunk_size=2))
assert [type(row).__name__ for row in tuples] == ["Row"] * 3 and tuples[1].T is None
assert [str(row.T) for row in tuples[::2]] == ["2024-01-01T00:00:00Z", "2024-01-01T00:00:02Z"]
# an instant read from a table is one a column is made of
from_rows = new_table([
    datetime_col("T", [row["T"] for row in rows]),
    int_col("I", [row["I"] for row in rows]),
    string_col("S", [row["S"] for row in rows]),
])

chunks = list(source.iter_chunk_dict(chunk_size=2))
assert [len(chunk["T"]) for chunk in chunks] == [2, 1]
assert [str(chunk["T"].dtype) for chunk in chunks] == ["datetime64[ns]"] * 2
stamps = np.concatenate([chunk["T"] for chunk in chunks])
assert [str(stamp) for stamp in stamps] == [
    "2024-01-01T00:00:00.000000000", "NaT", "2024-01-01T00:00:02.000000000"]
whole = next(source.iter_chunk_tuple(["I", "T"]))
assert str(whole.T.dtype) == "datetime64[ns]" and whole.I.tolist() == [0, constants.NULL_INT, 2]
from_chunks = new_table([datetime_col("T", stamps), int_col("I", whole.I)])


def cells(dtype, *values):
    return [None if value is None else dtypes.array(dtype, value) for value in values]


arrays = new_table([
    InputColumn("Ints", dtypes.int32_array, input_data=cells(dtypes.int32, [1, constants.NULL_INT, 3], None, [])),
    InputColumn("Strings", dtypes.string_array, input_data=cells(dtypes.string, ["a", None], None, ["b"])),
    InputColumn("Doubles", dtypes.double_array, input_data=cells(dtypes.double, [1.5], [2.5, 3.5], [])),
    InputColumn("Times", dtypes.instant_array,
                input_data=cells(dtypes.Instant, ["2024-01-01T00:00:00Z", None], None, [])),
    InputColumn("Bools", dtypes.boolean_array, input_data=cells(dtypes.bool_, [True, None], [False], None)),
])
rows = list(arrays.iter_dict())
assert [None if row["Ints"] is None else list(row["Ints"]) for row in rows] == [[1, constants.NULL_INT, 3], None, []]
assert [None if row["Strings"] is None else list(row["Strings"]) for row in rows] == [["a", None], None, ["b"]]
assert [list(row["Doubles"]) for row in rows] == [[1.5], [2.5, 3.5], []]
assert [None if row["Bools"] is None else list(row["Bools"]) for row in rows] == [[True, None], [False], None]
first = rows[0]["Times"]
assert len(first) == 2 and str(first[0]) == "2024-01-01T00:00:00Z" and first[1] is None
assert rows[1]["Times"] is None and len(rows[2]["Times"]) == 0
assert len(rows[0]["Ints"]) == 3 and rows[0]["Ints"][2] == 3
row = next(arrays.iter_tuple(["Doubles"]))
assert list(row.Doubles) == [1.5]
# an array column of arrays of different lengths is a numpy array of them
chunk = next(arrays.iter_chunk_dict(["Doubles", "Ints"]))
assert chunk["Doubles"].dtype == object and chunk["Doubles"].shape == (3,)
assert [len(cell) for cell in chunk["Doubles"]] == [1, 2, 0] and chunk["Ints"][1] is None
lengths = new_table([
    long_col("Ints", [-1 if row["Ints"] is None else len(row["Ints"]) for row in rows]),
    long_col("Doubles", [len(cell) for cell in chunk["Doubles"]]),
])
