# What new_table, an input column, a DType and TableDefinition refuse. The table made first
# is there after each refusal.
# oracle: results kept
# oracle: fails
from deephaven import dtypes, new_table
from deephaven.column import InputColumn, col_def, int_col, long_col, string_col
from deephaven.table import TableDefinition

kept = new_table([int_col("Id", [1])])
new_table([int_col("Id", ["x"])])
# oracle: step
# oracle: fails
new_table([string_col("S", [1])])
# oracle: step
# oracle: fails
new_table([long_col("L", [2**63])])
# oracle: step
# oracle: fails
new_table([int_col("1d", [1])])
# oracle: step
# oracle: fails
new_table([int_col("A", [1]), int_col("B", [1, 2])])
# oracle: step
# oracle: fails
new_table({"A": [1], "B": [1, 2]})
# oracle: step
# oracle: fails
dtypes.int32(3)
# oracle: step
# oracle: fails
dtypes.null_remap(dtypes.string)
# oracle: step
# oracle: fails
TableDefinition(5)
# oracle: step
# oracle: fails
TableDefinition([col_def("A", dtypes.int32), col_def("A", dtypes.string)])
# oracle: step
# oracle: fails
TableDefinition({"A": int})
