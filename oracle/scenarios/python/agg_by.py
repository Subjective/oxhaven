# One agg_by with the common aggregations, by a key column and over the whole table.
# oracle: results by_key, overall
# oracle: computed by_key.Sum, by_key.Avg, overall.Sum, overall.Avg
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
