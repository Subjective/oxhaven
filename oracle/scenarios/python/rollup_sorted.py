# rollup whose nodes are sorted with a node operations recorder: by a result, descending,
# by an absolute value, by group-by columns (which sort only the levels that have them), by
# several columns, with nulls and equal values; recorders applied one after another and
# together; a sorted rollup read with some nodes expanded; and what a recorder refuses.
# oracle: views plain_all = plain everything A, by_qty_all = by_qty everything A, by_qty_some = by_qty some A
# oracle: views by_qty_desc_all = by_qty_desc everything A, by_abs_all = by_abs everything A, by_abs_closed = by_abs
# oracle: views by_sym_desc_all = by_sym_desc everything A, by_ex_all = by_ex everything A, by_keys_all = by_keys everything A
# oracle: views by_rows_all = by_rows everything A, twice_all = twice everything A, together_all = together everything A
# oracle: views chained_all = chained everything A, same_all = same everything A, unchanged_all = unchanged everything A
# oracle: views by_time_all = by_time everything A, by_float_all = by_float everything A, flat_all = flat everything A
# oracle: views three_all = three everything3 A, three_some = three some3 A
# oracle: computed plain_all.Avg, by_qty_all.Avg, by_qty_some.Avg, by_qty_desc_all.Avg, by_abs_all.Avg, by_abs_closed.Avg
# oracle: computed by_sym_desc_all.Avg, by_ex_all.Avg, by_keys_all.Avg, by_rows_all.Avg, twice_all.Avg, together_all.Avg
# oracle: computed chained_all.Avg, same_all.Avg, unchanged_all.Avg, by_time_all.Avg, by_float_all.Avg, flat_all.Avg
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col
from deephaven.table import NodeType, RollupNodeOperationsRecorder

source = empty_table(36).update([
    "Sym = (i % 4 == 3) ? null : `s` + (i % 4)",
    "Ex = (i % 6 == 5) ? null : `x` + (i % 3)",
    "Side = i % 2",
    "Qty = (i % 7 == 6) ? NULL_INT : (i * 37 % 11) - 5",
    "P = i * 1.25",
    "F = (i % 5 == 4) ? Double.NaN : ((i % 9) - 4) * 0.5",
    "T = '2024-01-01T00:00:00Z' + ((i * 7) % 5) * 1000000000L",
])
plain = source.rollup(
    aggs=[
        agg.sum_("Qty"),
        agg.count_("Rows"),
        agg.avg("Avg = P"),
        agg.max_("MaxF = F"),
        agg.first("FirstT = T"),
    ],
    by=["Sym", "Ex"],
)


def recorder():
    return plain.node_operation_recorder(NodeType.AGGREGATED)


assert isinstance(recorder(), RollupNodeOperationsRecorder)
assert isinstance(recorder().sort("Qty"), RollupNodeOperationsRecorder)
by_qty = plain.with_node_operations([recorder().sort("Qty")])
assert by_qty.by == plain.by and by_qty.aggs is plain.aggs and not by_qty.include_constituents
by_qty_desc = plain.with_node_operations([recorder().sort_descending("Qty")])
by_abs = plain.with_node_operations([recorder().sort("__ABS__Qty")])
by_sym_desc = plain.with_node_operations([recorder().sort_descending("Sym")])
by_ex = plain.with_node_operations([recorder().sort("Ex")])
by_keys = plain.with_node_operations([recorder().sort_descending(["Ex", "Sym"])])
# equal counts keep the order the rows had
by_rows = plain.with_node_operations([recorder().sort_descending("Rows")])
# a later sort decides first: by the count, then by what it was sorted by before
twice = by_qty_desc.with_node_operations([recorder().sort("Rows")])
together = plain.with_node_operations([recorder().sort("Avg"), recorder().sort_descending("Rows")])
chained = plain.with_node_operations([recorder().sort("Rows").sort_descending("Sym").sort("Rows")])
# a column named again sorts as it was named first
same = by_qty.with_node_operations([recorder().sort_descending("Qty").sort("Qty")])
# recorders that recorded nothing change nothing
unchanged = by_qty.with_node_operations([recorder(), recorder().sort([])])
by_time = plain.with_node_operations([recorder().sort_descending("FirstT").sort("Ex")])
# NaN is the greatest value and null the least
by_float = plain.with_node_operations([recorder().sort("MaxF")])
flat_plain = source.rollup(aggs=[agg.sum_("Qty"), agg.avg("Avg = P")])
flat = flat_plain.with_node_operations(
    [flat_plain.node_operation_recorder(NodeType.AGGREGATED).sort_descending("Qty")]
)
three_plain = source.rollup(aggs=[agg.count_("Rows"), agg.sum_("Qty")], by=["Sym", "Ex", "Side"])
three = three_plain.with_node_operations([
    three_plain.node_operation_recorder(NodeType.AGGREGATED).sort_descending(["Qty", "Side"])
])

everything = new_table([
    int_col("__DEPTH__", [0]), string_col("Sym", [None]), string_col("Ex", [None]), byte_col("A", [3]),
])
# two nodes of the first level, one of them the null key, in the order of the key table
some = new_table([
    int_col("__DEPTH__", [2, 2, 2]),
    string_col("Sym", ["s2", None, "nope"]),
    string_col("Ex", [None, None, None]),
    byte_col("A", [1, 1, 1]),
])
everything3 = new_table([
    int_col("__DEPTH__", [0]), string_col("Sym", [None]), string_col("Ex", [None]),
    int_col("Side", [None]), byte_col("A", [3]),
])
some3 = new_table([
    int_col("__DEPTH__", [2, 3, 3, 2]),
    string_col("Sym", ["s1", "s1", "s0", "s0"]),
    string_col("Ex", [None, "x1", "x2", None]),
    int_col("Side", [None, None, None, None]),
    byte_col("A", [1, 1, 1, 4]),
])
# oracle: step
# oracle: fails
recorder().sort("Nope")
# oracle: step
# oracle: fails
recorder().sort_descending(["Qty", "__DEPTH__"])
# oracle: step
# oracle: fails
recorder().sort("__ABS__Sym")
# oracle: step
# oracle: fails
plain.node_operation_recorder(NodeType.CONSTITUENT)
# oracle: step
# oracle: fails
plain.node_operation_recorder(None)
# oracle: step
# oracle: fails
plain.with_node_operations(recorder().sort("Qty"))
# oracle: step
# oracle: fails
plain.with_node_operations(["Qty"])
