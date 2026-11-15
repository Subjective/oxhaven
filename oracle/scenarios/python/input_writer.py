# A DynamicTableWriter: rows are in its table after the script that wrote them, a number
# is converted to the column's type with loss, a column of strings takes an integer or a
# Boolean as its text, and closing the writer changes nothing that shows.
# oracle: results written, in_script
from deephaven import DynamicTableWriter, dtypes, new_table
from deephaven.column import int_col
from deephaven.time import to_j_instant

writer = DynamicTableWriter(
    {
        "I": dtypes.int32,
        "B": dtypes.byte,
        "H": dtypes.short,
        "L": dtypes.long,
        "D": dtypes.double,
        "F": dtypes.float32,
        "S": dtypes.string,
        "T": dtypes.bool_,
        "W": dtypes.Instant,
    }
)
written = writer.table
writer.write_row(1, 2, 3, 4, 5.5, 6.5, "a", True, to_j_instant("2024-01-01T00:00:00Z"))
writer.write_row(3.7, 300, 70000, 2**40, 1, 2, None, None, None)
writer.write_row(None, None, None, None, None, None, "c", False, None)
in_script = new_table([int_col("Size", [written.size])])
# oracle: step
writer.write_row(-3.7, -1.5, 1e10, 1e30, float("nan"), 1e40, "d", True, None)
writer.write_row(2**31, 2**31 + 5, -(2**15) - 1, float("nan"), -0.0, 0.1, "", False, None)
writer.close()
# oracle: step
with writer:
    writer.write_row(9, 9, 9, 9, 9, 9, "after close", False, None)
writer.write_row(9, 9, 9, 9, 9, 9, 3, False, None)
writer.write_row(9, 9, 9, 9, 9, 9, True, False, None)
writer.write_row(9, 9, 9, 9, 9, 9, 2**40, False, None)
in_script = new_table([int_col("Size", [written.size])])
