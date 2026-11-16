# rollup with constituents whose nodes are sorted and whose source is filtered: a recorder
# for the constituent nodes sorts the rows of the source under a finest group, a recorder
# for the aggregated nodes sorts the levels alone, both together, recorders applied one
# after another; filters of group-by columns and of other columns, which the rows of the
# source follow; and what a constituent recorder refuses.
# oracle: views plain_all = plain everything A, by_p_all = by_p everything A, by_other_desc_all = by_other_desc everything A
# oracle: views levels_all = levels everything A, both_all = both everything A, by_abs_all = by_abs everything A
# oracle: views chained_all = chained everything A, twice_all = twice everything A, same_all = same everything A
# oracle: views by_keys_all = by_keys everything A, by_float_all = by_float everything A, by_time_some = by_time some A
# oracle: views flat_all = flat, filtered_key_all = filtered_key everything A, filtered_other_all = filtered_other everything A
# oracle: views filtered_sorted_all = filtered_sorted everything A, sorted_filtered_all = sorted_filtered everything A
# oracle: views nothing_all = nothing everything A
# oracle: computed plain_all.Avg, by_p_all.Avg, by_other_desc_all.Avg, levels_all.Avg, both_all.Avg, by_abs_all.Avg
# oracle: computed chained_all.Avg, twice_all.Avg, same_all.Avg, by_keys_all.Avg, by_float_all.Avg, by_time_some.Avg
# oracle: computed filtered_key_all.Avg, filtered_other_all.Avg, filtered_sorted_all.Avg, sorted_filtered_all.Avg
# oracle: computed nothing_all.Avg
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col
from deephaven.table import NodeType, RollupNodeOperationsRecorder

source = empty_table(36).update([
    "Sym = (i % 4 == 3) ? null : `s` + (i % 4)",
    "Ex = (i % 6 == 5) ? null : `x` + (i % 3)",
    "Qty = (i % 7 == 6) ? NULL_INT : (i * 37 % 11) - 5",
    "P = (i * 7 % 5) * 1.25",
    "F = (i % 5 == 4) ? Double.NaN : (i % 9 == 8) ? NULL_DOUBLE : ((i % 9) - 4) * 0.5",
    "T = '2024-01-01T00:00:00Z' + ((i * 7) % 5) * 1000000000L",
    "Other = 35 - i",
])
plain = source.rollup(
    aggs=[agg.sum_("Qty"), agg.count_("Rows"), agg.avg("Avg = P"), agg.max_("MaxF = F")],
    by=["Sym"],
    include_constituents=True,
)


def rows():
    return plain.node_operation_recorder(NodeType.CONSTITUENT)


def levels_recorder():
    return plain.node_operation_recorder(NodeType.AGGREGATED)


assert isinstance(rows(), RollupNodeOperationsRecorder)
by_p = plain.with_node_operations([rows().sort("P")])
assert by_p.include_constituents and by_p.aggs is plain.aggs
by_other_desc = plain.with_node_operations([rows().sort_descending("Other")])
# the sorts of the aggregated nodes do not sort the rows of the source
levels = plain.with_node_operations([levels_recorder().sort_descending("Qty")])
both = plain.with_node_operations([levels_recorder().sort_descending("Qty"), rows().sort_descending("Qty")])
by_abs = plain.with_node_operations([rows().sort("__ABS__Qty")])
chained = plain.with_node_operations([rows().sort("Other").sort_descending("Ex").sort("P")])
# a later sort decides first: by P, then by what the rows were sorted by before
twice = by_other_desc.with_node_operations([rows().sort("P")])
# a column named again sorts as it was named first
same = by_p.with_node_operations([rows().sort_descending("P").sort("P")])
# the rows under one group are equal by the group-by column; the other key sorts them
by_keys = plain.with_node_operations([rows().sort_descending(["Sym", "Ex"])])
# NaN is the greatest value and null the least
by_float = plain.with_node_operations([rows().sort("F")])
by_time = plain.with_node_operations([rows().sort_descending("T").sort("Ex")])
flat_plain = source.rollup(aggs=[agg.sum_("Qty")], include_constituents=True)
flat = flat_plain.with_node_operations(
    [flat_plain.node_operation_recorder(NodeType.CONSTITUENT).sort_descending(["Qty", "Other"])]
)

filtered_key = plain.with_filters("Sym != `s1`")
filtered_other = plain.with_filters(["Ex != `x1`", "Other > 6"])
filtered_sorted = by_p.with_filters("Other % 3 != 0")
sorted_filtered = filtered_other.with_node_operations(
    [filtered_other.node_operation_recorder(NodeType.CONSTITUENT).sort_descending("P")]
)
nothing = plain.with_filters("Other > 100")

everything = new_table([int_col("__DEPTH__", [0]), string_col("Sym", [None]), byte_col("A", [3])])
some = new_table([
    int_col("__DEPTH__", [2, 2, 2]), string_col("Sym", ["s2", None, "nope"]), byte_col("A", [1, 1, 1]),
])
# oracle: step
# oracle: fails
rows().sort("Avg")
# oracle: step
# oracle: fails
rows().sort("Rows")
# oracle: step
# oracle: fails
rows().sort_descending(["Qty", "__DEPTH__"])
# oracle: step
# oracle: fails
rows().sort("__ABS__Sym")
# oracle: step
# oracle: fails
levels_recorder().sort("Other")
# oracle: step
# oracle: fails
plain.with_filters("Qty > 0")
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.sum_("Qty")], by="Sym").node_operation_recorder(NodeType.CONSTITUENT)
# oracle: step
# a recorder for constituents applied to a rollup that has none
# oracle: fails
source.rollup(aggs=[agg.sum_("Qty")], by="Sym").with_node_operations([rows().sort("P")])
