# What an input table and a writer refuse, one refusal a step. Nothing here changes a
# table, and a refused row of a writer is not written in part.
# oracle: results appended, keyed, written
from deephaven import DynamicTableWriter, dtypes, empty_table, input_table, new_table
from deephaven.column import double_col, int_col, string_col
from deephaven.table_factory import InputTable
from deephaven.time import to_j_instant


def rows(keys, values):
    return new_table([int_col("K", keys), double_col("V", values)])


def texts(keys, values):
    return new_table([int_col("K", keys), string_col("S", values)])


appended = input_table(init_table=texts([1], ["a"]))
keyed = input_table(init_table=rows([1], [1.0]), key_cols="K")
writer = DynamicTableWriter(
    {
        "I": dtypes.int32,
        "D": dtypes.double,
        "S": dtypes.string,
        "T": dtypes.bool_,
        "C": dtypes.char,
        "W": dtypes.Instant,
    }
)
written = writer.table
now = to_j_instant("2024-01-01T00:00:00Z")
writer.write_row(1, 1.5, "a", True, None, now)
# oracle: step
# oracle: fails
appended.delete(texts([1], ["a"]).view("K"))
# oracle: step
# oracle: fails
appended.add(rows([2], [2.0]))
# oracle: step
# oracle: fails
appended.add_async(rows([2], [2.0]))
# oracle: step
# oracle: fails
keyed.delete(rows([1], [1.0]))
# oracle: step
# oracle: fails
keyed.delete_async(rows([1], [1.0]))
# oracle: step
# oracle: fails
InputTable(empty_table(1).j_table)
# oracle: step
# oracle: fails
input_table()
# oracle: step
# oracle: fails
input_table({"K": dtypes.int32}, rows([1], [1.0]))
# oracle: step
# oracle: fails
input_table({"K": dtypes.int32}, key_cols="Nope")
# oracle: step
# oracle: fails
writer.write_row(1, 2.5)
# oracle: step
# oracle: fails
writer.write_row(True, 2.5, "b", True, None, now)
# oracle: step
# oracle: fails
writer.write_row("2", 2.5, "b", True, None, now)
# oracle: step
# oracle: fails
writer.write_row(2, "2.5", "b", True, None, now)
# oracle: step
# oracle: fails
writer.write_row(2, 2.5, "b", 1, None, now)
# oracle: step
# oracle: fails
writer.write_row(2, 2.5, "b", True, "c", now)
# oracle: step
# oracle: fails
writer.write_row(2, 2.5, "b", True, 99, now)
# oracle: step
# oracle: fails
writer.write_row(2, 2.5, "b", True, None, "2024-01-01T00:00:00Z")
# oracle: step
# oracle: fails
writer.write_row(2, 2.5, "b", True, None, 5)
