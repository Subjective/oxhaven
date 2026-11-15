# A table to a pandas DataFrame and back, and a DataFrame built in Python to a table.
# oracle: results round_trip, from_frame
from deephaven import empty_table
from deephaven.pandas import to_pandas, to_table

import pandas

source = empty_table(4).update(["I = i", "D = i * 0.5", "S = `s` + i", "B = i % 2 == 0"])
frame = to_pandas(source)
round_trip = to_table(frame)
from_frame = to_table(pandas.DataFrame({
    "X": [1, 2, 3],
    "Y": [1.5, 2.5, 3.5],
    "Z": ["a", "b", "c"],
}))
