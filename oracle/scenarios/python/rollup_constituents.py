# rollup with constituents: the rows of the source under the finest groups. The default
# view, every node expanded, some finest groups expanded by key, a node contracted under
# an expand-all, an expansion to a depth; a rollup without group-by columns, whose grand
# total shows the rows of the source; one without aggregations; one whose first 2000 rows
# end inside the rows of a group; and what a key table cannot name.
# oracle: views closed = r, everything = r all_keys A, some = r some_keys A, but_one = r but_one_keys A
# oracle: views to_depth_1 = r depth_1_keys A, to_depth_2 = r depth_2_keys A, to_depth_3 = r depth_3_keys A
# oracle: views flat_open = flat, flat_closed = flat flat_closed_keys A, flat_all = flat flat_all_keys A
# oracle: views bare_all = bare bare_keys A, one_key = by_sym sym_keys A, typed_all = typed typed_keys A
# oracle: views big_window = big big_keys A, too_deep = r too_deep_keys A
# oracle: computed closed.Avg, everything.Avg, some.Avg, but_one.Avg, to_depth_1.Avg, to_depth_2.Avg, to_depth_3.Avg
# oracle: computed too_deep.Avg, flat_open.Avg, flat_closed.Avg, flat_all.Avg
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col

source = empty_table(30).update([
    "Sym = (i % 4 == 3) ? null : `s` + (i % 4)",
    "Ex = (i % 6 == 5) ? null : `x` + (i % 3)",
    "Qty = (i % 7 == 6) ? NULL_INT : (i * 37 % 11) - 5",
    "P = (i % 5 == 4) ? Double.NaN : i * 1.25",
    "T = '2024-01-01T00:00:00Z' + ((i * 7) % 5) * 1000000000L",
])
aggs = [agg.sum_("Qty"), agg.count_("Rows"), agg.avg("Avg = P"), agg.last("LastT = T")]
r = source.rollup(aggs=aggs, by=["Sym", "Ex"], include_constituents=True)
assert r.include_constituents


def keys(depths, syms, exs, actions):
    return new_table([
        int_col("__DEPTH__", depths),
        string_col("Sym", syms),
        string_col("Ex", exs),
        byte_col("A", actions),
    ])


all_keys = keys([0], [None], [None], [3])
# a first-level node, two finest groups under it, a finest group under a node that is not
# expanded, the finest group of the null keys, and one that is not there
some_keys = keys(
    [2, 3, 3, 3, 2, 3, 3],
    ["s0", "s0", "s0", "s1", None, None, "s0"],
    [None, "x0", "x2", "x1", None, None, "nope"],
    [1, 1, 1, 1, 1, 1, 1],
)
# everything but the rows of one finest group and the groups of one node
but_one_keys = keys([0, 3, 2], [None, "s0", "s2"], [None, "x1", None], [3, 4, 4])
# from the root: 0x70 is one level, the root's own
depth_1_keys = keys([0], [None], [None], [0x70])
depth_2_keys = keys([0], [None], [None], [0x71])
depth_3_keys = keys([0], [None], [None], [0x73])
# a key with every group-by column and a depth below the finest groups
too_deep_keys = keys([2, 4], ["s0", "s0"], [None, "x0"], [1, 1])

flat = source.rollup(aggs=[agg.sum_("Qty"), agg.avg("Avg = P")], include_constituents=True)
flat_closed_keys = new_table([int_col("__DEPTH__", [1]), byte_col("A", [4])])
flat_all_keys = new_table([int_col("__DEPTH__", [0]), byte_col("A", [3])])

bare = source.rollup(aggs=[], by=["Sym"], include_constituents=True)
bare_keys = new_table([int_col("__DEPTH__", [0]), string_col("Sym", [None]), byte_col("A", [3])])
by_sym = source.rollup(aggs=[agg.count_("Rows"), agg.max_("MaxQty = Qty")], by="Sym", include_constituents=True)
sym_keys = new_table([
    int_col("__DEPTH__", [2, 2]), string_col("Sym", ["s2", None]), byte_col("A", [1, 1]),
])

# group-by columns of other types, with a null key
typed_source = empty_table(12).update([
    "I = (i % 5 == 4) ? NULL_INT : i % 2",
    "D = (i % 3) * 0.5",
    "B = i % 4 < 2",
    "V = (long) i",
])
typed = typed_source.rollup(aggs=[agg.sum_("V"), agg.first("FirstB = B")], by=["I", "D"], include_constituents=True)
typed_keys = empty_table(1).update(["__DEPTH__ = 0", "I = NULL_INT", "D = NULL_DOUBLE", "A = (byte) 3"])

# the first rows of the view end inside the 2500 rows of the first group
big_source = empty_table(5000).update(["K = i < 2500 ? `a` : `b`", "V = i"])
big = big_source.rollup(aggs=[agg.sum_("V")], by="K", include_constituents=True)
big_keys = new_table([int_col("__DEPTH__", [0]), string_col("K", [None]), byte_col("A", [3])])
