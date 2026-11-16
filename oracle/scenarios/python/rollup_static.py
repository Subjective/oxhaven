# rollup of a static table, read as a client reads it: the default view (the grand total
# and the first level), views with nodes expanded, and everything expanded; by two columns,
# by one, by none, by three with null keys; and each aggregation a rollup takes.
# oracle: views two_default = two, two_some = two some A, two_all = two everything A
# oracle: views one_default = one, one_all = one everything A, none_default = none, none_all = none everything A
# oracle: views three_default = three, three_all = three everything A, three_nulls = three nulls
# oracle: views specs_all = specs everything A, sorted_all = ordered everything A, weighted_all = weighted everything A
# oracle: views moments_all = moments everything A, counted_all = counted everything A, flat_sorted_all = flat_sorted
# oracle: computed two_default.Avg, two_some.Avg, two_all.Avg, weighted_all.WAvg, weighted_all.WSum
# oracle: computed moments_all.Std, moments_all.Var, moments_all.Avg, moments_all.Sum
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col

source = empty_table(24).update([
    "Sym = (i % 4 == 3) ? null : `s` + (i % 4)",
    "Ex = (i % 6 == 5) ? null : `x` + (i % 3)",
    "Side = (i % 5 == 0) ? NULL_INT : i % 2",
    "Qty = (i % 7 == 6) ? NULL_INT : i * 10",
    "P = i * 1.25",
    "W = (i % 5) + 1",
    "F = (float) (i * 0.5)",
    "L = (long) i * 3",
    "B = i % 3 == 0",
    "T = '2024-01-01T00:00:00Z' + i * 1000000000L",
    "C = (char) ('a' + i % 5)",
])
two = source.rollup(
    aggs=[agg.sum_("Qty"), agg.count_("Rows"), agg.avg("Avg = P"), agg.first("FirstEx = Ex")],
    by=["Sym", "Ex"],
)
assert two.by == ("Sym", "Ex") or list(two.by) == ["Sym", "Ex"]
assert not two.include_constituents
assert len(two.aggs) == 4
one = source.rollup(aggs=[agg.sum_("Qty"), agg.max_("MaxP = P")], by="Sym")
none = source.rollup(aggs=[agg.sum_("Qty"), agg.count_("Rows")])
three = source.rollup(aggs=agg.count_("Rows"), by=["Sym", "Ex", "Side"])
specs = source.rollup(
    aggs=[
        agg.min_("MinQty = Qty"),
        agg.max_("MaxQty = Qty"),
        agg.first("FirstQty = Qty"),
        agg.last(["LastQty = Qty", "LastEx = Ex"]),
        agg.abs_sum("AbsQty = Qty"),
        agg.min_(["MinT = T", "MinC = C", "MinEx2 = Ex"]),
        agg.max_(["MaxT = T", "MaxC = C", "MaxF = F"]),
        agg.first(["B", "P"]),
        agg.sum_(["SumF = F", "SumL = L"]),
    ],
    by=["Sym", "Ex"],
)
# A sorted first or last of a rollup with group-by columns sorts by its own results.
ordered = source.rollup(
    aggs=[agg.sorted_first("W", ["W", "LightQty = Qty"]), agg.sorted_last("Qty", ["Qty", "Most = P"])],
    by=["Sym", "Ex"],
)
flat_sorted = source.rollup(
    aggs=[agg.sorted_first("W", "LightQty = Qty"), agg.sorted_last("W", "HeavyQty = Qty")]
)
weighted = source.rollup(
    aggs=[agg.weighted_avg("W", "WAvg = P"), agg.weighted_sum("W", "WSum = P")],
    by=["Sym"],
)
moments = source.rollup(
    aggs=[agg.std("Std = P"), agg.var("Var = P"), agg.avg("Avg = P"), agg.sum_("Sum = P")],
    by=["Sym"],
)
counted = source.rollup(
    aggs=[
        agg.count_distinct("ExCount = Ex"),
        agg.count_distinct("ExCountNulls = Ex", count_nulls=True),
        agg.unique("OneEx = Ex"),
        agg.unique("OneW = W", non_unique_sentinel=-1),
        agg.distinct("Exs = Ex"),
        agg.distinct("ExsNulls = Ex", include_nulls=True),
    ],
    by=["Sym"],
)

# The depth of a key is one more than the number of its key columns: 0 is the root, 1 the
# grand total, 2 a row of the first level.
some = new_table([
    int_col("__DEPTH__", [2, 2, 3]),
    string_col("Sym", ["s0", None, "s1"]),
    string_col("Ex", [None, None, "x1"]),
    byte_col("A", [1, 1, 1]),
])
# expand all, from the root
everything = new_table([
    int_col("__DEPTH__", [0]),
    string_col("Sym", [None]),
    string_col("Ex", [None]),
    int_col("Side", [None]),
    byte_col("A", [3]),
])
# without an action column every row expands its node
nulls = new_table([
    int_col("__DEPTH__", [2, 3, 3]),
    string_col("Sym", [None, None, "s0"]),
    string_col("Ex", [None, None, "x0"]),
    int_col("Side", [None, None, None]),
])
