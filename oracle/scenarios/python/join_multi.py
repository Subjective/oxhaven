# multi_join: one row per key that any of several tables has, from tables that share their
# key columns and from MultiJoinInput objects, each with its own columns; what the two
# forms refuse; and last keyed_transpose, which waits for an engine operator.
# oracle: results shared, shared_list, single, chosen, renamed, two_keys, transposed
from deephaven import agg, empty_table
from deephaven.table import MultiJoinInput, MultiJoinTable, keyed_transpose, multi_join

a = empty_table(3).update(["K = i", "A = i * 2"])
b = empty_table(4).update(["K = 3 - i", "B = i * 3"])
c = empty_table(2).update(["Key = i + 1", "C = `c` + i", "D = i"])
shared = multi_join([a, b], on="K").table
shared_list = MultiJoinTable([a, b], on=["K"]).table
single = multi_join(a, on=" K ,").table
chosen = multi_join([
    MultiJoinInput(a, "K"),
    MultiJoinInput(b, on=["K"], joins="B"),
    MultiJoinInput(c, on="K = Key", joins=["C"]),
]).table
renamed = multi_join(MultiJoinInput(c, on="K = Key", joins=["Cee = C"])).table
two_keys = multi_join([MultiJoinInput(a, ["K", "A"]), MultiJoinInput(b, ["K", "A = B"])]).table
# oracle: step
# oracle: fails
both = multi_join([MultiJoinInput(a, "K"), MultiJoinInput(b, "K")], on="K")
# oracle: step
# oracle: fails
mixed = multi_join([a, MultiJoinInput(b, "K")])
# oracle: step
# oracle: fails
no_keys = multi_join([a, b])
# oracle: step
# oracle: fails
twice = multi_join([a, a], on="K")
# oracle: step
# oracle: fails
not_a_name = MultiJoinInput(a, "K,A")
# oracle: step
rows = empty_table(6).update(["R = i % 2", "C = `c` + (i % 3)", "V = i"])
transposed = keyed_transpose(rows, agg.sum_("V"), "R", "C")
