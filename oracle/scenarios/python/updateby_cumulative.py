# One update_by with the cumulative operators, by a key column and over the whole table.
# oracle: results by_key, overall
# oracle: computed by_key.Sum, by_key.Prod, overall.Sum, overall.Prod
from deephaven import empty_table
from deephaven.updateby import cum_max, cum_min, cum_prod, cum_sum, delta, forward_fill

source = empty_table(10).update([
    "K = i % 2",
    "V = (i % 4 == 3) ? NULL_DOUBLE : i * 0.5 + 1",
    "N = 10 - i",
])
ops = [
    cum_sum("Sum = V"),
    cum_prod("Prod = V"),
    cum_min("Min = N"),
    cum_max("Max = N"),
    forward_fill("Filled = V"),
    delta("Delta = N"),
]
by_key = source.update_by(ops, by=["K"])
overall = source.update_by(ops)
