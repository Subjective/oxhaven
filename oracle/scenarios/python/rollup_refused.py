# What Java refuses of a rollup: the aggregations it has no rollup of, a result named like
# a group-by column, a group-by column the table does not have, and a view whose key table
# cannot be read as keys. And what it does not refuse: a rollup without aggregations.
# oracle: views whole = r, mistyped = r mistyped_keys A, no_depth = r no_depth_keys A, bad_action = r bad_action_keys A, bare = bare_rollup
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, long_col, string_col

source = empty_table(8).update(["K = `k` + (i % 2)", "J = i % 4", "V = i"])
r = source.rollup(aggs=[agg.sum_("V")], by=["K", "J"])
# J is an int column of the rollup
mistyped_keys = new_table([
    int_col("__DEPTH__", [2]), string_col("K", ["k0"]), long_col("J", [None]), byte_col("A", [1]),
])
no_depth_keys = new_table([string_col("K", ["k0"]), int_col("J", [None]), byte_col("A", [1])])
bad_action_keys = new_table([
    int_col("__DEPTH__", [2]), string_col("K", ["k0"]), int_col("J", [None]), byte_col("A", [2]),
])
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.median("V")], by="K")
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.pct(0.5, "V")], by="K")
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.partition("Part")], by="K")
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.formula("sum(each)", "each", ["V"])], by="K")
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.max_("J = V")], by=["K", "J"])
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.sum_("V")], by=["K", "Nope"])
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.sum_("Nope")], by=["K"])
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.sum_("V")], by=["K", "K"])
# oracle: step
# a rollup without aggregations has the group-by columns alone
bare_rollup = source.rollup(aggs=[], by=["K"])
