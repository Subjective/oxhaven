# Update views of the nodes of a rollup: columns added to the aggregated nodes or to the
# rows of the source, computed when a client reads them, that a sort can sort by. And
# what Java refuses: a column the nodes have, the position of a row, and operations that
# add columns for nodes that have operations already.
# oracle: views v_uv = uv everything A, v_cons = cons everything A, v_wuv = wuv everything A
# oracle: views v_sorted = sort_by_view everything A, v_later = sorted_later everything A, v_chain = chain everything A
# oracle: views v_filtered = filtered everything A, v_default = uv
# oracle: computed v_cons.Half, v_cons.Avg, v_uv.Avg, v_wuv.Avg, v_sorted.Avg, v_later.Avg, v_chain.Avg, v_filtered.Avg, v_default.Avg
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col
from deephaven.table import NodeType

source = empty_table(12).update([
    "Sym = (i % 4 == 3) ? null : `s` + (i % 4)",
    "Qty = (i % 7 == 6) ? NULL_INT : (i * 37 % 11) - 5",
    "P = (i * 7 % 5) * 1.25",
])
plain = source.rollup(aggs=[agg.sum_("Qty"), agg.avg("Avg = P")], by=["Sym"], include_constituents=True)


def levels():
    return plain.node_operation_recorder(NodeType.AGGREGATED)


def rows():
    return plain.node_operation_recorder(NodeType.CONSTITUENT)


uv = plain.with_node_operations([levels().update_view(["Twice = Qty * 2", "Label = Sym + `!`"])])
cons = plain.with_node_operations([rows().update_view("Half = P / 2"), levels().update_view("Neg = -Qty")])
wuv = plain.with_update_view(["Neg = -Qty"])
sort_by_view = plain.with_node_operations([levels().update_view("Neg = -Qty").sort("Neg")])
sorted_later = uv.with_node_operations([uv.node_operation_recorder(NodeType.AGGREGATED).sort_descending("Twice")])
chain = plain.with_node_operations([levels().update_view("A1 = Qty + 1").update_view(["A2 = A1 + 1", "B = Qty", "B = Qty + 1"])])
filtered = uv.with_filters("Sym != `s1`")
everything = new_table([int_col("__DEPTH__", [0]), string_col("Sym", [None]), byte_col("A", [3])])
# oracle: step
# oracle: fails
# two recorders that add columns to the nodes of one type
plain.with_node_operations([levels().update_view("A = Qty"), levels().update_view("B = Qty")])
# oracle: step
# oracle: fails
# nodes that have an update view take no other
uv.with_node_operations([uv.node_operation_recorder(NodeType.AGGREGATED).update_view("Thrice = Qty * 3")])
# oracle: step
# oracle: fails
plain.with_update_view("Twice = Qty * 2").with_update_view("Thrice = Qty * 3")
# oracle: step
# oracle: fails
# nor nodes that are sorted
plain.with_node_operations([levels().sort("Qty")]).with_node_operations([levels().update_view("A = Qty")])
# oracle: step
# oracle: fails
levels().update_view("Qty = 1")
# oracle: step
# oracle: fails
levels().update_view("R = i")
# oracle: step
# oracle: fails
levels().update_view("A = Nope + 1")
# oracle: step
# oracle: fails
levels().update_view("A = Qty").update_view("A = Qty + 1")
# oracle: step
# oracle: fails
# a column of the source is no column of the aggregated nodes
source.rollup(aggs=[agg.sum_("Qty")], by=["Sym"]).with_update_view("P2 = P")
# oracle: step
# oracle: fails
source.rollup(aggs=[agg.sum_("Qty")], by=["Sym"]).node_operation_recorder(NodeType.CONSTITUENT)
