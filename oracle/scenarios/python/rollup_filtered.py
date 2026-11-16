# rollup whose source is filtered with with_filters: by a group-by column, by both, by a
# column that no aggregation reads (which filters the source), by several filters, with
# nulls, a filter of a filtered rollup, a filter that leaves nothing, a filter that reads a
# variable, a filtered rollup that is sorted and a sorted one that is filtered, a key table
# with keys the filter left out; and the filters a rollup refuses.
# oracle: views plain_all = plain everything A, one_sym_all = one_sym everything A, not_x0_all = not_x0 everything A
# oracle: views not_x0_closed = not_x0, not_x0_some = not_x0 some A, both_all = both everything A
# oracle: views by_other_all = by_other everything A, mixed_all = mixed everything A, nulls_all = nulls everything A
# oracle: views again_all = again everything A, nothing_all = nothing everything A, nothing_closed = nothing
# oracle: views scoped_all = scoped everything A, unfiltered_all = unfiltered everything A, in_list_all = in_list everything A
# oracle: views filtered_sorted_all = filtered_sorted everything A, sorted_filtered_all = sorted_filtered everything A
# oracle: views flat_all = flat everything A, counted_all = counted everything A
# oracle: computed plain_all.Avg, one_sym_all.Avg, not_x0_all.Avg, not_x0_closed.Avg, not_x0_some.Avg, both_all.Avg
# oracle: computed by_other_all.Avg, mixed_all.Avg, nulls_all.Avg, again_all.Avg, nothing_all.Avg, nothing_closed.Avg
# oracle: computed scoped_all.Avg, unfiltered_all.Avg, in_list_all.Avg, filtered_sorted_all.Avg, sorted_filtered_all.Avg
# oracle: computed flat_all.Avg
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col
from deephaven.table import NodeType

source = empty_table(36).update([
    "Sym = (i % 4 == 3) ? null : `s` + (i % 4)",
    "Ex = (i % 6 == 5) ? null : `x` + (i % 3)",
    "Qty = (i % 7 == 6) ? NULL_INT : (i * 37 % 11) - 5",
    "P = i * 1.25",
    "N = i",
    "Tag = (i % 5 == 0) ? null : `t` + (i % 2)",
])
plain = source.rollup(
    aggs=[agg.sum_("Qty"), agg.count_("Rows"), agg.avg("Avg = P")],
    by=["Sym", "Ex"],
)
one_sym = plain.with_filters("Sym == `s1`")
assert one_sym.by == plain.by and one_sym.aggs is plain.aggs and not one_sym.include_constituents
not_x0 = plain.with_filters("Ex != `x0`")
both = plain.with_filters(["Sym in `s0`, `s2`", "Ex != `x1`"])
# a column that is neither grouped by nor aggregated: the rows of the source are filtered
by_other = plain.with_filters("N > 9")
mixed = plain.with_filters(["Tag == `t1`", "Sym != `s0`"])
nulls = plain.with_filters("isNull(Sym) || isNull(Ex)")
again = not_x0.with_filters("Sym != `s2`").with_filters("N < 30")
nothing = plain.with_filters("Sym == `nope`")
limit = 20
scoped = plain.with_filters("N >= limit && Sym != null")
unfiltered = plain.with_filters([])
in_list = plain.with_filters("Ex not in `x0`, `x2`")
filtered_sorted = not_x0.with_node_operations(
    [not_x0.node_operation_recorder(NodeType.AGGREGATED).sort_descending("Qty")]
)
sorted_filtered = plain.with_node_operations(
    [plain.node_operation_recorder(NodeType.AGGREGATED).sort("Avg").sort_descending("Rows")]
).with_filters("N % 3 != 0")
flat = source.rollup(aggs=[agg.sum_("Qty"), agg.avg("Avg = P")]).with_filters("Sym == `s2`")
counted = source.rollup(aggs=[agg.count_("Rows")], by=["Sym", "Ex"]).with_filters("Qty > 0")
everything = new_table([
    int_col("__DEPTH__", [0]), string_col("Sym", [None]), string_col("Ex", [None]), byte_col("A", [3]),
])
# s0 has no x0 child left in not_x0; s9 is no key at all
some = new_table([
    int_col("__DEPTH__", [2, 3, 2, 2]),
    string_col("Sym", ["s0", "s0", "s9", None]),
    string_col("Ex", [None, "x0", None, None]),
    byte_col("A", [1, 1, 1, 1]),
])
# oracle: step
# oracle: fails
plain.with_filters("Qty > 0")
# oracle: step
# oracle: fails
plain.with_filters(["Sym == `s1`", "P < 10.0"])
# oracle: step
# oracle: fails
plain.with_filters("Rows > 3")
# oracle: step
# oracle: fails
plain.with_filters("Nope == 1")
# oracle: step
# oracle: fails
plain.with_filters("Sym_[i - 1] == `s1`")
# oracle: step
# oracle: fails
plain.with_filters("N +")
# oracle: step
# oracle: fails
plain.with_filters("N + 1")
