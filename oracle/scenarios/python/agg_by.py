# One agg_by with the common aggregations, by a key column and over the whole table; then
# the other functions of deephaven.agg with their options, preserve_empty, agg_all_by, what
# the two refuse, and last the calls that wait for an engine operator, one a step.
# oracle: results by_key, overall, spread, picked, sets, counted, weighted, formulas, uniques, emptied, kept, every_sum, every_formula, every_weighted, partitioned, seeded
# oracle: computed by_key.Sum, by_key.Avg, overall.Sum, overall.Avg, spread.Std, spread.Var, spread.Median, spread.Pct, weighted.WAvg, weighted.WSum, formulas.Out, formulas.FV, every_sum.V, every_formula.V, every_weighted.K, every_weighted.V, every_weighted.N
from deephaven import agg, empty_table

source = empty_table(9).update(["K = i % 3", "V = i * 1.5", "N = i"])
aggs = [
    agg.sum_("Sum = V"),
    agg.avg("Avg = V"),
    agg.min_("Min = N"),
    agg.max_("Max = N"),
    agg.first("First = N"),
    agg.last("Last = N"),
    agg.count_("Count"),
]
by_key = source.agg_by(aggs, by=["K"])
overall = source.agg_by(aggs)

wide = source.update(["S = `s` + (i % 2)", "W = i % 4"])
spread = wide.agg_by([
    agg.std("Std = V"),
    agg.var("Var = V"),
    agg.median("Median = V"),
    agg.median("LowMedian = N", average_evenly_divided=False),
    agg.pct(0.3, "Pct = V"),
    agg.pct(0.5, "MidPct = N", average_evenly_divided=True),
    agg.abs_sum("Abs = N"),
], by="S")
picked = wide.agg_by([
    agg.sorted_first("V", "ByV = N"),
    agg.sorted_last(["W", "V"], ["ByWV = N", "S"]),
], by="K")
sets = wide.agg_by([
    agg.group(["Vs = V"]),
    agg.distinct("Ss = S"),
    agg.distinct("Ws = W", include_nulls=True),
    agg.count_distinct("Distinct = S"),
    agg.count_distinct("WithNulls = W", count_nulls=True),
], by="K")
counted = wide.agg_by([
    agg.count_where("Big", "N > 3"),
    agg.count_where("Both", ["N > 3", "V < 10"]),
], by="K")
weighted = wide.agg_by([agg.weighted_avg("W", "WAvg = V"), agg.weighted_sum("W", "WSum = V")], by="K")
formulas = wide.agg_by([
    agg.formula("Out = sum(V) + max(N)"),
    agg.formula("sum(x)", "x", ["FV = V", "FN = N"]),
], by="K")
uniques = wide.agg_by([
    agg.unique("OneK = K"),
    agg.unique("S"),
    agg.unique("Many = S", non_unique_sentinel="many"),
    agg.unique("Small = N", non_unique_sentinel=-1),
    agg.unique("Wide = N", non_unique_sentinel=100000),
    agg.unique("Real = V", non_unique_sentinel=1.5),
    agg.unique("Nulls = S", include_nulls=True, non_unique_sentinel="x"),
], by="K")
emptied = wide.where("N < 0").agg_by([agg.sum_("V"), agg.count_("Count")])
kept = wide.where("N < 0").agg_by([agg.sum_("V"), agg.count_("Count")], preserve_empty=True)
every_sum = wide.agg_all_by(agg.sum_(), by="S")
every_formula = wide.agg_all_by(agg.formula("sum(x)", "x"), by=["S"])
every_weighted = wide.agg_all_by(agg.weighted_avg("W"), "S")
# oracle: step
# oracle: fails
no_columns = wide.agg_by([agg.sum_()], by="K")
# oracle: step
# oracle: fails
no_spec = wide.agg_all_by(agg.count_("Count"), by="K")
# oracle: step
# oracle: fails
no_keys = wide.agg_by(agg.sum_("V"), initial_groups=wide)
# oracle: step
# oracle: fails
reaggregating = wide.agg_by(agg.formula("Out = sum(V)", reaggregating=True), by="K")
# oracle: step
# oracle: fails
no_name = agg.count_("Not a name")
# oracle: step
partitioned = wide.agg_by([agg.partition("Rows")], by="K").view("K")
# oracle: step
seeded = wide.agg_by(agg.sum_("V"), by="K", preserve_empty=True, initial_groups=empty_table(5).update("K = 4 - i"))
