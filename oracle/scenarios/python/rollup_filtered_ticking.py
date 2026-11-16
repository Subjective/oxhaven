# filtered rollup of a ticking table, a keyed input table, read after each change: rows
# that enter and leave a kept group, a group the filter leaves out, a row that moves into
# and out of what the filter keeps, a group that goes away and comes back; filtered by a
# group-by column, by a column no aggregation reads, and filtered and sorted.
# oracle: views plain = r everything A, by_sym = kept_sym everything A, by_sym_closed = kept_sym
# oracle: views by_lot = kept_lot everything A, by_both = kept_both everything A, by_both_some = kept_both some A
# oracle: views followed = kept_sym live_keys A
from deephaven import agg, input_table, new_table
from deephaven.column import byte_col, int_col, string_col
from deephaven.table import NodeType


def rows(ids, syms, exs, qtys, lots):
    return new_table([
        int_col("Id", ids),
        string_col("Sym", syms),
        string_col("Ex", exs),
        int_col("Qty", qtys),
        int_col("Lot", lots),
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
        [1, 2, 3, 4, None, 6],
    ),
    key_cols="Id",
)
r = source.rollup(
    aggs=[agg.sum_("Qty"), agg.count_("N"), agg.max_("MaxQty = Qty")],
    by=["Sym", "Ex"],
)
kept_sym = r.with_filters("Sym != `b`")
kept_lot = r.with_filters("Lot >= 3")
kept_both = (
    r.with_filters(["Ex in `x`, `y`", "Lot % 2 == 0"])
    .with_node_operations([r.node_operation_recorder(NodeType.AGGREGATED).sort_descending("Qty")])
)
everything = expansions([0], [None], [None], [3])
some = expansions([2, 2], ["a", "b"], [None, None], [1, 1])
live_keys = input_table(init_table=expansions([2], ["b"], [None], [1]), key_cols=["__DEPTH__", "Sym", "Ex"])
# oracle: step
# a row for a kept group, one for the group that is left out, one with a lot that fails
source.add(rows([7, 8, 9], ["a", "b", "c"], ["x", "x", "y"], [70, 80, 90], [8, 8, 1]))
# oracle: step
# row 3 moves from b, which the filter leaves out, to a; row 1 gets a lot that passes
source.add(rows([3, 1], ["a", "a"], ["x", "x"], [30, 10], [3, 4]))
# oracle: step
live_keys.add(expansions([2, 2], ["a", None], [None, None], [1, 1]))
# oracle: step
# row 7 moves to b: out of kept_sym
source.add(rows([7], ["b"], ["y"], [70], [8]))
# oracle: step
source.delete(rows([4, 9], ["c", "c"], ["x", "y"], [0, 0], [0, 0]).view("Id"))
# oracle: step
source.add(rows([4, 10], ["c", "e"], ["y", "x"], [41, 11], [2, 12]))
# oracle: step
source.delete(rows(list(range(1, 11)), ["a"] * 10, ["x"] * 10, [0] * 10, [0] * 10).view("Id"))
# oracle: step
source.add(rows([11, 12], ["a", "b"], ["x", "x"], [90, 91], [6, 6]))
