# A tree with filters, of a ticking table: the rows that pass and their ancestors; a
# filter of a filtered tree, which the ancestors the first one kept may pass; a filter of
# a sorted tree; a filter that nothing passes; and a filter of a tree with its orphans
# promoted; read after rows are added, moved and removed.
# oracle: views plain = t all_keys A, small = below all_keys A, small_closed = below
# oracle: views named = some all_keys A, twice = again all_keys A, sorted = ordered all_keys A
# oracle: views none = nothing all_keys A, promoted = promoted_below all_keys A, chosen = below chosen_keys
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
        list("abcdefgh"),
        [5, 9, 3, 8, -7, 2, 1, 0],
    ),
    key_cols="Id",
)
t = source.tree("Id", "Parent")
below = t.with_filters("V < 3")
some = t.with_filters(["Name in `e`, `g`"])
again = below.with_filters("V > 0")
ordered = t.with_node_operations(t.node_operation_recorder().sort_descending("V")).with_filters("V < 3")
nothing = t.with_filters(["V > 100", "V < 3"])
promoted_below = source.tree("Id", "Parent", promote_orphans=True).with_filters("V < 3")
all_keys = new_table([int_col("Id", [None]), byte_col("A", [3])])
chosen_keys = new_table([int_col("Id", [1, 3])])
# oracle: step
source.add(rows([9, 5], [4, 4], ["i", "e2"], [1, 50]))
# oracle: step
source.delete(rows([6, 3], [None, None], ["", ""], [0, 0]).view("Id"))
# oracle: step
source.add(rows([3, 10], [1, 3], ["c2", "j"], [0, 200]))
# oracle: step
source.add(rows([10, 7], [3, 2], ["j2", "g2"], [-1, 30]))
