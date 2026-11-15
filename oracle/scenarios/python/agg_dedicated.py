# The table methods that apply one aggregation to every column that is not a key, with and
# without key columns, and head_by and tail_by.
# oracle: results sums, averages, counts, firsts, lasts, minima, maxima, grouped, absolute, deviations, variances, medians, weighted_sums, weighted_averages, total, count, first, everything, heads, tails, head_of_all
# oracle: computed sums.V, averages.V, averages.N, absolute.V, deviations.V, deviations.N, variances.V, variances.N, medians.V, weighted_sums.V, weighted_averages.V, total.V
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
absolute = source.update("V = -V").abs_sum_by("K")
deviations = source.std_by("K")
variances = source.var_by(["K"])
medians = source.where("N < 8").median_by("K")
weighted_sums = source.weighted_sum_by("N", "K")
weighted_averages = source.weighted_avg_by("N", ["K"])
total = source.sum_by()
count = source.count_by("Rows")
first = source.first_by()
everything = source.group_by()
heads = source.head_by(2, "K")
tails = source.tail_by(1, ["K"])
head_of_all = source.head_by(2)
# oracle: step
# oracle: fails
no_rows = source.head_by(1.5, "K")
# oracle: step
# oracle: fails
no_weight = source.weighted_avg_by("Nope", "K")
