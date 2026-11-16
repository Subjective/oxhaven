# Formats of the nodes of a rollup: hidden columns after the columns of the nodes of
# their type, for the aggregated nodes and for the rows of the source, of a column, of a
# column an update view adds, and of the rows where a condition holds. And what Java
# refuses: a format of no column, a sort by a hidden column, and formats for nodes that
# have operations already.
# oracle: views v_fmt = fmt everything A, v_both = both everything A, v_cons = cons everything A
# oracle: views v_rows = row_where everything A, v_where = column_where everything A, v_refmt = refmt everything A
# oracle: views v_sorted = fmt_then_sort everything A, v_default = fmt
# oracle: computed v_fmt.Avg, v_both.Avg, v_cons.Avg, v_cons.Half, v_rows.Avg, v_where.Avg, v_refmt.Avg, v_sorted.Avg, v_default.Avg
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


fmt = plain.with_node_operations([levels().format_column(["Qty = Qty > 0 ? RED : NO_FORMATTING", "Avg = Decimal(`0.0`)"])])
both = plain.with_node_operations([levels().update_view("Twice = Qty * 2").format_column("Twice = BLUE")])
cons = plain.with_node_operations([
    rows().update_view("Half = P / 2").format_column("P = heatmap(P, 0, 5, RED, GREEN)"),
    levels().format_column("Sym = `GREEN`"),
])
row_where = plain.with_node_operations([levels().format_row_where("Qty < 0", "PINK")])
column_where = plain.with_node_operations([levels().format_column_where("Avg", "Avg > 2.5", "bgfg(BLUE, WHITE)")])
# a recorder of the formatted rollup formats the column again
refmt = fmt.with_node_operations([fmt.node_operation_recorder(NodeType.AGGREGATED).format_column("Qty = BLUE")])
fmt_then_sort = fmt.with_node_operations([fmt.node_operation_recorder(NodeType.AGGREGATED).sort_descending("Qty")])
everything = new_table([int_col("__DEPTH__", [0]), string_col("Sym", [None]), byte_col("A", [3])])
# oracle: step
# oracle: fails
# formats for nodes that are sorted
plain.with_node_operations([levels().sort("Qty")]).with_node_operations([levels().format_column("Qty = RED")])
# oracle: step
# oracle: fails
# formats for nodes that have an update view
both.with_node_operations([both.node_operation_recorder(NodeType.AGGREGATED).format_column("Avg = RED")])
# oracle: step
# oracle: fails
levels().format_column("Nope = RED")
# oracle: step
# oracle: fails
levels().format_column("Qty RED")
# oracle: step
# oracle: fails
levels().format_column("Qty = 5").format_column("Qty__TABLE_STYLE_FORMAT = RED")
# oracle: step
# oracle: fails
levels().sort("Qty__TABLE_STYLE_FORMAT")
# oracle: step
# oracle: fails
fmt.node_operation_recorder(NodeType.AGGREGATED).sort("Qty__TABLE_STYLE_FORMAT")
# oracle: step
# oracle: fails
# the column of the source is no column of the aggregated nodes
levels().format_column_where("P", "Qty > 0", "RED")
