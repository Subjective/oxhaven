# tree of a ticking table, a keyed input table, read after each change, with the rows whose
# parent is no row left out and promoted: the parent of such a row arrives, a row moves to
# another parent, a parent goes away, a row becomes its own ancestor and stops being it, a
# row gets its first child and loses it, and every row goes and one comes back.
# oracle: views closed = t, everything = t all_keys A, chosen = t chosen_keys
# oracle: views promoted_closed = tp, promoted_everything = tp all_keys A, promoted_chosen = tp chosen_keys
from deephaven import input_table, new_table
from deephaven.column import byte_col, int_col, string_col


def rows(ids, parents, names):
    return new_table([int_col("Id", ids), int_col("Parent", parents), string_col("Name", names)])


def ids(values):
    return new_table([int_col("Id", values)])


source = input_table(
    init_table=rows([1, 2, 3, 4, 5], [None, None, 1, 3, 9], list("abcde")),
    key_cols="Id",
)
t = source.tree("Id", "Parent")
tp = source.tree("Id", "Parent", promote_orphans=True)
all_keys = new_table([int_col("Id", [None]), byte_col("A", [3])])
chosen_keys = ids([1, 3, 2])
# oracle: step
# 9 arrives, under 2: 5 has a parent now; 4 moves from 3 to 2; 6 is the first child of 5
source.add(rows([9, 4, 6], [2, 2, 5], ["nine", "d2", "f"]))
# oracle: step
# 1 goes away: its child 3 has no parent that is a row
source.delete(ids([1]))
# oracle: step
# 1 comes back as a child of its child
source.add(rows([1], [3], ["loop"]))
# oracle: step
source.add(rows([1], [None], ["back"]))
# oracle: step
# 5 loses its only child
source.delete(ids([6]))
# oracle: step
source.add(rows([3, 7], [None, 3], ["c2", "g"]))
# oracle: step
source.delete(ids([1, 2, 3, 4, 5, 7, 9]))
# oracle: step
source.add(rows([4, 8], [None, 4], ["again", "h"]))
