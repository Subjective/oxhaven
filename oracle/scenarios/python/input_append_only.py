# An append-only input table from column definitions, and a table derived from it: rows
# are added from the script, one step at a time.
# oracle: results rows, total
from deephaven import dtypes, input_table, new_table
from deephaven.column import int_col, string_col

rows = input_table({"K": dtypes.int32, "S": dtypes.string})
total = rows.count_by("N")
# oracle: step
rows.add(new_table([int_col("K", [1, 2]), string_col("S", ["a", "b"])]))
# oracle: step
rows.add(new_table([int_col("K", [1]), string_col("S", ["again"])]))
