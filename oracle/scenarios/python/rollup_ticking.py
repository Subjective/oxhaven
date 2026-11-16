# rollup of a ticking table, a keyed input table, read after each change: rows added to a
# group, a new group, a row that moves between groups, a group that loses every row and comes
# back, an expanded node that goes away, and a key table that ticks.
# oracle: views closed = r, everything = r all_keys A, chosen = r chosen_keys, followed = r live_keys A
# oracle: computed closed.Avg, everything.Avg, chosen.Avg, followed.Avg
from deephaven import agg, input_table, new_table
from deephaven.column import byte_col, double_col, int_col, string_col


def rows(ids, syms, exs, qtys):
    return new_table([
        int_col("Id", ids),
        string_col("Sym", syms),
        string_col("Ex", exs),
        int_col("Qty", qtys),
        double_col("P", [qty * 0.5 for qty in qtys]),
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
    aggs=[
        agg.sum_("Qty"),
        agg.count_("N"),
        agg.avg("Avg = P"),
        agg.min_("MinQty = Qty"),
        agg.max_("MaxQty = Qty"),
        agg.first("FirstId = Id"),
        agg.last("LastId = Id"),
        agg.count_distinct("Exs = Ex"),
    ],
    by=["Sym", "Ex"],
)
all_keys = expansions([0], [None], [None], [3])
chosen_keys = expansions([2, 2], ["a", "d"], [None, None], [1, 1]).drop_columns("A")
live_keys = input_table(init_table=expansions([2], ["b"], [None], [1]), key_cols=["__DEPTH__", "Sym", "Ex"])
# oracle: step
source.add(rows([5, 6], ["a", "b"], ["x", "y"], [50, 60]))
# oracle: step
source.add(rows([7], ["d"], ["z"], [70]))
# oracle: step
# row 2 moves from (a, y) to (d, z): the node (a, y) is gone
source.add(rows([2], ["d"], ["z"], [21]))
# oracle: step
live_keys.add(expansions([2, 0], ["d", None], [None, None], [1, 3]))
# oracle: step
source.delete(rows([4], ["c"], ["x"], [0]).view("Id"))
# oracle: step
live_keys.delete(expansions([0], [None], [None], [3]).view(["__DEPTH__", "Sym", "Ex"]))
source.add(rows([4, 8], ["c", "c"], ["y", "y"], [41, 80]))
# oracle: step
source.delete(rows([1, 2, 3, 4, 5, 6, 7, 8], ["a"] * 8, ["x"] * 8, [0] * 8).view("Id"))
# oracle: step
source.add(rows([9], ["a"], ["x"], [90]))
