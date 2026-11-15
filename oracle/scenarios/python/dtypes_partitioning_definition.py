# The meta table of a TableDefinition that has a partitioning column.
# oracle: results meta
from deephaven import dtypes
from deephaven.column import ColumnType, col_def
from deephaven.table import TableDefinition

meta = TableDefinition([
    col_def("P", dtypes.string, column_type=ColumnType.PARTITIONING),
    col_def("V", dtypes.long),
]).table
