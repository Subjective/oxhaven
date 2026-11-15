# TableDefinition from a mapping, from column definitions and of a table: its meta table,
# its text and what it answers as a mapping.
# oracle: results from_mapping, from_columns, of_table, texts, answers
from deephaven import dtypes, empty_table, new_table
from deephaven.column import ColumnType, col_def, int_col, string_col
from deephaven.table import TableDefinition

mapping = TableDefinition({
    "Bool": dtypes.bool_, "Byte": dtypes.byte, "Char": dtypes.char, "Short": dtypes.short,
    "Int": dtypes.int32, "Long": dtypes.long, "Float": dtypes.float32, "Double": dtypes.double,
    "String": dtypes.string, "Instant": dtypes.Instant, "Ints": dtypes.int32_array,
    "Strings": dtypes.string_array,
})
columns = TableDefinition([
    col_def("A", dtypes.int32),
    col_def("B", dtypes.string),
    col_def("C", dtypes.double_array),
])
partitioned = TableDefinition([
    col_def("P", dtypes.string, column_type=ColumnType.PARTITIONING),
    col_def("V", dtypes.long_array),
])
source = empty_table(2).update(["X = ii", "Y = `a`", "Z = i % 2 == 0", "W = (float) i"])
from_mapping = mapping.table
from_columns = columns.table
of_table = source.definition.table
texts = new_table([string_col("Text", (
    [str(mapping), str(columns), str(partitioned), str(source.definition)]
    + [str(column) for column in mapping.values()]
    + [repr(column.data_type) + " " + repr(column.component_type) + " " + column.column_type.name
       for column in list(columns.values()) + list(partitioned.values())]
))])
same = TableDefinition({"A": dtypes.int32, "B": dtypes.string})
table = new_table([int_col("A", [1]), string_col("B", ["x"])])
answers = new_table([
    string_col("Question", [
        "keys", "len", "contains", "of table equals mapping", "equals itself copied",
        "other order", "other type", "item", "items",
    ]),
    string_col("Answer", [str(answer) for answer in (
        list(mapping.keys()), len(mapping), ("Int" in mapping, "None" in mapping),
        table.definition == same, same == TableDefinition(same),
        same == TableDefinition({"B": dtypes.string, "A": dtypes.int32}),
        same == TableDefinition({"A": dtypes.long, "B": dtypes.string}),
        mapping["Long"] == col_def("Long", dtypes.long),
        [(name, repr(column.data_type)) for name, column in same.items()],
    )]),
])
