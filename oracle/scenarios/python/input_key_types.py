# Input tables of two unusual shapes: one without columns, and a keyed one whose key
# column is a double, which Oxhaven's engine does not have yet.
# oracle: results no_columns, by_double
from deephaven import input_table, new_table
from deephaven.column import double_col, int_col


def rows(keys, values):
    return new_table([int_col("K", keys), double_col("V", values)])


no_columns = input_table({})
# oracle: step
by_double = input_table(init_table=rows([1, 2, 3], [1.5, 2.5, 1.5]), key_cols="V")
by_double.add(rows([4], [2.5]))
by_double.delete(rows([0], [1.5]).view("V"))
