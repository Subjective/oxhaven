# What the conversions to and from pyarrow refuse. The table made first is there after
# each refusal.
# oracle: results kept
# oracle: fails
import pyarrow as pa

from deephaven import empty_table
from deephaven.arrow import to_arrow, to_table

kept = empty_table(2).update("X = i")
to_arrow(kept, cols=["Nope"])
# oracle: step
# oracle: fails
to_arrow(5)
# oracle: step
# oracle: fails
to_table(pa.table({"X": pa.array(["a"], pa.large_string())}))
# oracle: step
# oracle: fails
to_table(pa.table({"X": pa.array(["a", "b"]).dictionary_encode()}))
# oracle: step
# oracle: fails
to_table(pa.table({"X": pa.array([{"a": 1}])}))
# oracle: step
# oracle: fails
to_table(pa.table({"X": [1]}), cols=["Nope"])
# oracle: step
# oracle: fails
to_table([1])
# oracle: step
# oracle: fails
to_table(pa.table({"X": [1]}).to_batches()[0])
# oracle: step
# oracle: fails
to_table(pa.Table.from_arrays([pa.array([1]), pa.array([2])], names=["A", "A"]))
