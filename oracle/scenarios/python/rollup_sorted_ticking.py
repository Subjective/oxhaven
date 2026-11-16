# sorted rollup of a ticking table, a keyed input table, read after each change: a row that
# changes the order of the first level and of a node's children, a new group that sorts in
# between, values that become equal, a group that goes away, and an expanded node that
# moves; sorted by a result, by a group-by column, and by two columns.
# Java fails a view of a sorted rollup whose key table expands a node that was there and
# has no rows now (HierarchicalTableImpl.LinkedDirective#setRowKeyInParentSorted): the
# views followed and by_qty_some after the deletes. Oxhaven does the same.
# oracle: views plain = r everything A, by_qty = sorted_qty everything A, by_qty_some = sorted_qty some A
# oracle: views by_qty_closed = sorted_qty, by_ex_desc = sorted_ex everything A, by_two = sorted_two everything A
# oracle: views followed = sorted_two live_keys A
from deephaven import agg, input_table, new_table
from deephaven.column import byte_col, int_col, string_col
from deephaven.table import NodeType


def rows(ids, syms, exs, qtys):
    return new_table([
        int_col("Id", ids),
        string_col("Sym", syms),
        string_col("Ex", exs),
        int_col("Qty", qtys),
    ])


def expansions(depths, syms, exs, actions):
    return new_table([
        int_col("__DEPTH__", depths),
        string_col("Sym", syms),
        string_col("Ex", exs),
        byte_col("A", actions),
    ])


source = input_table(
    init_table=rows(
        [1, 2, 3, 4, 5, 6],
        ["a", "a", "b", "c", "b", None],
        ["x", "y", "x", "x", "z", "y"],
        [10, 20, 30, 5, -40, None],
    ),
    key_cols="Id",
)
r = source.rollup(
    aggs=[agg.sum_("Qty"), agg.count_("N"), agg.min_("MinQty = Qty"), agg.last("LastId = Id")],
    by=["Sym", "Ex"],
)


def recorder():
    return r.node_operation_recorder(NodeType.AGGREGATED)


sorted_qty = r.with_node_operations([recorder().sort_descending("Qty")])
sorted_ex = r.with_node_operations([recorder().sort_descending("Ex")])
sorted_two = r.with_node_operations([recorder().sort(["N", "__ABS__MinQty"])])
everything = expansions([0], [None], [None], [3])
some = expansions([2, 2], ["b", "d"], [None, None], [1, 1])
live_keys = input_table(init_table=expansions([2], ["a"], [None], [1]), key_cols=["__DEPTH__", "Sym", "Ex"])
# oracle: step
# c becomes the greatest sum, and (a, y) the least of a
source.add(rows([4, 2], ["c", "a"], ["x", "y"], [100, -20]))
# oracle: step
# a new group between the others, and a second row for (b, x)
source.add(rows([7, 8], ["d", "b"], ["w", "x"], [7, 1]))
# oracle: step
# sums that are equal keep the order of the rows: a and d have 7 now
source.add(rows([1, 2], ["a", "a"], ["x", "y"], [27, -20]))
# oracle: step
live_keys.add(expansions([2, 2], ["d", None], [None, None], [1, 1]))
# oracle: step
# row 3 moves from (b, x) to (d, x)
source.add(rows([3], ["d"], ["x"], [30]))
# oracle: step
source.delete(rows([4, 6], ["c", None], ["x", "y"], [0, 0]).view("Id"))
# oracle: step
live_keys.delete(expansions([2], ["a"], [None], [1]).view(["__DEPTH__", "Sym", "Ex"]))
source.add(rows([4, 9], ["c", None], ["y", "x"], [None, 3]))
# oracle: step
source.delete(rows([1, 2, 3, 4, 5, 7, 8, 9], ["a"] * 8, ["x"] * 8, [0] * 8).view("Id"))
# oracle: step
source.add(rows([10], ["a"], ["x"], [90]))
