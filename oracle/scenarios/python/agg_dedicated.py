# The table methods that apply one aggregation to every column that is not a key.
# oracle: results sums, averages, counts, firsts, lasts, minima, maxima, grouped
# oracle: computed sums.V, averages.V, averages.N
from deephaven import empty_table

source = empty_table(9).update(["K = i % 3", "V = i * 1.5", "N = i"])
sums = source.sum_by("K")
averages = source.avg_by(["K"])
counts = source.count_by("Count", "K")
firsts = source.first_by("K")
lasts = source.last_by("K")
minima = source.min_by("K")
maxima = source.max_by("K")
grouped = source.group_by("K")
