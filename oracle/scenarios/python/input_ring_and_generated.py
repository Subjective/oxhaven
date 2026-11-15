# Tables made from other tables or from a function, which Oxhaven's engine does not have
# yet: a ring table of an input table, and a function generated table.
# oracle: results ring, generated
from deephaven import dtypes, empty_table, function_generated_table, input_table, new_table, ring_table
from deephaven.column import int_col

source = input_table({"K": dtypes.int32})
source.add(new_table([int_col("K", [1, 2, 3])]))
ring = ring_table(source, 2)
# oracle: step
source.add(new_table([int_col("K", [4])]))
# oracle: step
generated = function_generated_table(lambda: empty_table(2).update("X = i"), source_tables=source)
