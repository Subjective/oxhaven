# A tree whose nodes are sorted, of a ticking table: descending, by a later sort that
# decides first, by an absolute value, by two columns, and of a tree with its orphans
# promoted; read after rows are added, moved and removed.
# oracle: views plain = t all_keys A, descending = down all_keys A, then_names = named all_keys A
# oracle: views absolute = magnitude all_keys A, two = both all_keys A, chosen = down chosen_keys
# oracle: views promoted = promoted_down all_keys A
from deephaven import input_table, new_table
from deephaven.column import byte_col, int_col, string_col


def rows(ids, parents, names, values):
    return new_table([
        int_col("Id", ids),
        int_col("Parent", parents),
        string_col("Name", names),
        int_col("V", values),
    ])


source = input_table(
    init_table=rows(
        [1, 2, 3, 4, 5, 6, 7, 8],
        [None, None, 1, 1, 3, 3, 2, 77],
        ["a", "b", "c", "c", "e", "f", "g", "h"],
        [5, 9, 3, 8, -7, 2, 1, 6],
    ),
    key_cols="Id",
)
t = source.tree("Id", "Parent")
recorder = t.node_operation_recorder()
down = t.with_node_operations(recorder.sort_descending("V"))
named = down.with_node_operations(t.node_operation_recorder().sort(["Name"]))
magnitude = t.with_node_operations(recorder.sort("__ABS__V"))
both = t.with_node_operations(recorder.sort_descending("Name").sort("V"))
promoted_down = source.tree("Id", "Parent", promote_orphans=True).with_node_operations(
    recorder.sort_descending("V")
)
all_keys = new_table([int_col("Id", [None]), byte_col("A", [3])])
chosen_keys = new_table([int_col("Id", [1, 3])])
# oracle: step
source.add(rows([9, 5], [4, 4], ["i", "e2"], [1, 50]))
# oracle: step
# the expanded node 3 goes away
source.delete(rows([6, 3], [None, None], ["", ""], [0, 0]).view("Id"))
# oracle: step
source.add(rows([3, 2], [1, None], ["c2", "b2"], [0, -9]))
