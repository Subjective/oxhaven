# What Java refuses of a tree: a column the table does not have, an identifier and a parent
# of two types, two rows with one identifier, and a sort or a filter by a column the tree
# does not have. And what it does not refuse: a second row with an identifier that arrives
# later, after which the tree no longer finds rows by their identifier.
# oracle: views whole = t everything A, late = ticking everything A, late_promoted = ticking_promoted everything A
# oracle: views late_filtered = ticking_filtered everything A
from deephaven import input_table, new_table
from deephaven.column import byte_col, int_col, string_col


def rows(ids, parents, names):
    return new_table([int_col("Id", ids), int_col("Parent", parents), string_col("Name", names)])


source = rows([1, 2, 3], [None, 1, 1], ["a", "b", "c"])
t = source.tree("Id", "Parent")
everything = new_table([int_col("Id", [None]), byte_col("A", [3])])
appended = input_table(init_table=source)
ticking = appended.tree("Id", "Parent")
ticking_promoted = appended.tree("Id", "Parent", promote_orphans=True)
ticking_filtered = ticking.with_filters("Id > 1")
# oracle: step
# oracle: fails
source.tree("Nope", "Parent")
# oracle: step
# oracle: fails
source.tree("Id", "Nope")
# oracle: step
# oracle: fails
source.tree("Id", "Name")
# oracle: step
# oracle: fails
source.tree("Nope", "Parent", promote_orphans=True)
# oracle: step
# oracle: fails
source.tree("Id", "Name", promote_orphans=True)
# oracle: step
# oracle: fails
rows([1, 2, 2], [None, 1, 1], ["a", "b", "b2"]).tree("Id", "Parent")
# oracle: step
# oracle: fails
t.with_node_operations(t.node_operation_recorder().sort("Nope"))
# oracle: step
# oracle: fails
t.with_filters("Nope > 1")
# oracle: step
appended.add(rows([2], [None], ["b2"]))
# oracle: step
appended.add(rows([7], [None], ["g"]))
