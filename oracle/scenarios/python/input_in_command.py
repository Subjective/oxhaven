# A script reads its own changes: a synchronous add or delete is in the input table when
# the call returns, so the sizes the script saw are compared too. An initial table with a
# repeated key, the names of the key and value columns, and an input table reached through
# a table derived from it.
# oracle: results appended, keyed, sizes, names, large
from deephaven import dtypes, input_table, new_table
from deephaven.column import double_col, int_col, string_col
from deephaven.table_factory import InputTable


def rows(keys, values):
    return new_table([int_col("K", keys), double_col("V", values)])


appended = input_table({"K": dtypes.int32, "V": dtypes.double})
seen = [appended.size]
appended.add(rows([1, 2], [1.0, 2.0]))
seen.append(appended.size)
appended.add(rows([3], [3.0]))
seen.append(appended.size)

keyed = input_table(init_table=rows([1, 2, 1], [1.0, 2.0, 3.0]), key_cols="K")
seen.append(keyed.size)
keyed.add(rows([2, 4], [0.5, 4.0]))
seen.append(keyed.size)
keyed.delete(rows([1], [0.0]).view("K"))
seen.append(keyed.size)

large = keyed.where("V > 1")
through = InputTable(large.j_table)
through.add(rows([7, 8], [7.0, 0.25]))
seen.append(keyed.size)
seen.append(large.size)

sizes = new_table([int_col("Size", seen)])
names = new_table(
    [
        string_col(
            "Names",
            appended.key_names
            + ["/"]
            + appended.value_names
            + ["/"]
            + keyed.key_names
            + ["/"]
            + keyed.value_names
            + ["/"]
            + through.key_names,
        )
    ]
)
# oracle: step
through.delete(rows([4], [0.0]).view("K"))
sizes = new_table([int_col("Size", [keyed.size, large.size])])
