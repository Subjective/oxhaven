# partition_by of a ticking table, a keyed input table: a new key gets a row and a
# constituent, a constituent follows the rows of its key, a key that loses every row loses
# its row and gets it back with the rows, and a row moves from one constituent to another.
# The merge and a constituent held from the start follow all of it.
# oracle: results rows, underlying, keys, merged, ones, twos, fives
from deephaven import input_table, new_table
from deephaven.column import int_col


def change(ids, keys, values):
    return new_table([int_col("Id", ids), int_col("K", keys), int_col("V", values)])


rows = input_table(init_table=change([1, 2, 3, 4], [1, 2, 1, 3], [10, 20, 30, 40]), key_cols="Id")
pt = rows.partition_by("K")
underlying = pt.table
keys = pt.keys()
merged = pt.merge()
ones = pt.get_constituent(1)
twos = pt.get_constituent(2)
assert pt.is_refreshing
assert pt.unique_keys
assert pt.constituent_changes_permitted
assert ones.is_refreshing
assert pt.get_constituent(5) is None
# oracle: step
rows.add(change([5, 6], [5, 1], [50, 60]))
# oracle: step
fives = pt.get_constituent(5)
rows.add(change([1], [1], [11]))
# oracle: step
rows.delete(change([2], [0], [0]).view("Id"))
# oracle: step
assert pt.get_constituent(2) is None
rows.add(change([7, 8], [2, 2], [70, 80]))
# oracle: step
rows.add(change([3], [3], [31]))
# oracle: step
rows.delete(change([1, 3, 4, 5, 6, 7, 8], [0] * 7, [0] * 7).view("Id"))
