# rollup with constituents of a ticking table, a keyed input table, read after each
# change: rows added to a group, a new group, a row that changes in place, a row that moves
# between groups, a group that loses every row and comes back, an expanded finest group
# that goes away, a key table that ticks; with the rows of the source unsorted and sorted,
# a rollup without group-by columns, and a filtered one.
# oracle: views closed = r, everything = r all_keys A, chosen = r chosen_keys, followed = r live_keys A
# oracle: views sorted_everything = by_qty all_keys A, flat_open = flat, filtered_everything = filtered all_keys A
# oracle: computed closed.Avg, everything.Avg, chosen.Avg, followed.Avg, sorted_everything.Avg, filtered_everything.Avg
from deephaven import agg, input_table, new_table
from deephaven.column import byte_col, double_col, int_col, string_col
from deephaven.table import NodeType


def rows(ids, syms, exs, qtys):
    return new_table([
        int_col("Id", ids),
        string_col("Sym", syms),
        string_col("Ex", exs),
        int_col("Qty", qtys),
        double_col("P", [qty * 0.5 for qty in qtys]),
        int_col("Lot", [id_ % 3 for id_ in ids]),
    ])


def expansions(depths, syms, exs, actions):
    return new_table([
        int_col("__DEPTH__", depths),
        string_col("Sym", syms),
        string_col("Ex", exs),
        byte_col("A", actions),
    ])


source = input_table(
    init_table=rows([1, 2, 3, 4], ["a", "a", "b", "c"], ["x", "y", "x", "x"], [10, 20, 30, 40]),
    key_cols="Id",
)
r = source.rollup(
    aggs=[agg.sum_("Qty"), agg.count_("N"), agg.avg("Avg = P"), agg.last("LastId = Id")],
    by=["Sym", "Ex"],
    include_constituents=True,
)
by_qty = r.with_node_operations([r.node_operation_recorder(NodeType.CONSTITUENT).sort_descending("Qty")])
flat = source.rollup(aggs=[agg.sum_("Qty"), agg.count_("N")], include_constituents=True)
filtered = r.with_filters("Lot != 0")
all_keys = expansions([0], [None], [None], [3])
# finest groups by key: one that is there, one whose parent is not expanded, one that comes
chosen_keys = expansions([2, 3, 3, 3], ["a", "a", "b", "d"], [None, "x", "x", "z"], [1, 1, 1, 1]).drop_columns("A")
live_keys = input_table(
    init_table=expansions([2, 3], ["a", "a"], [None, "y"], [1, 1]), key_cols=["__DEPTH__", "Sym", "Ex"],
)
# oracle: step
source.add(rows([5, 6], ["a", "b"], ["x", "y"], [50, 60]))
# oracle: step
source.add(rows([7], ["d"], ["z"], [70]))
# oracle: step
# row 1 changes in place
source.add(rows([1], ["a"], ["x"], [11]))
# oracle: step
# row 2 moves from (a, y) to (d, z): the group (a, y), which a view has expanded, is gone
source.add(rows([2], ["d"], ["z"], [21]))
# oracle: step
live_keys.add(expansions([2, 3, 0], ["d", "d", None], [None, "z", None], [1, 1, 3]))
# oracle: step
source.delete(rows([4], ["c"], ["x"], [0]).view("Id"))
# oracle: step
live_keys.delete(expansions([0], [None], [None], [3]).view(["__DEPTH__", "Sym", "Ex"]))
source.add(rows([4, 8, 2], ["c", "c", "a"], ["y", "y", "y"], [41, 80, 22]))
# oracle: step
source.delete(rows([1, 2, 3, 4, 5, 6, 7, 8], ["a"] * 8, ["x"] * 8, [0] * 8).view("Id"))
# oracle: step
source.add(rows([9, 10], ["a", "a"], ["x", "x"], [90, 15]))
