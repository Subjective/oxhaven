# A keyed input table from an initial table: a row with an existing key replaces that
# row, and rows are deleted by key.
# oracle: results keyed, large
from deephaven import input_table, new_table
from deephaven.column import double_col, int_col


def rows(keys, values):
    return new_table([int_col("K", keys), double_col("V", values)])


keyed = input_table(init_table=rows([1, 2, 3], [1.0, 2.0, 3.0]), key_cols="K")
large = keyed.where("V > 1.5")
# oracle: step
keyed.add(rows([2, 4], [0.5, 4.0]))
# oracle: step
keyed.delete(rows([3, 9], [0.0, 0.0]).view("K"))
