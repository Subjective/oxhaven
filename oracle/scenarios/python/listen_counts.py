# A table listener written in Python: it counts the rows each update adds to an input
# table, and a later script puts what it saw into a table.
# oracle: results seen
from deephaven import dtypes, input_table, new_table
from deephaven.column import int_col
from deephaven.table_listener import listen

source = input_table({"K": dtypes.int32})
added = []


def on_update(update, is_replay):
    added.append(len(update.added()["K"]))


handle = listen(source, on_update)
seen = new_table([int_col("Added", added)])
# oracle: step
source.add(new_table([int_col("K", [1, 2, 3])]))
# oracle: step
source.add(new_table([int_col("K", [4])]))
# oracle: step
handle.stop()
seen = new_table([int_col("Added", added)])
