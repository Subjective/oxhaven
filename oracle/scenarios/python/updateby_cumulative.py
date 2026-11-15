# One update_by with the cumulative operators, by a key column and over the whole table;
# then several column pairs in one operator, the controls of delta, what the operators
# refuse when they are made, and last the count where in a step of its own.
# oracle: results by_key, overall, pairs, deltas, nothing_added, counted
# oracle: computed by_key.Sum, by_key.Prod, overall.Sum, overall.Prod, pairs.SumV, deltas.Value, deltas.Zero
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

from deephaven.updateby import DeltaControl, UpdateByOperation, cum_count_where

pairs = source.update_by(cum_sum(["SumN = N", " SumV = V "]), by="K")
deltas = source.update_by([
    delta("Null = V", DeltaControl.NULL_DOMINATES),
    delta("Value = V", DeltaControl.VALUE_DOMINATES),
    delta("Zero = V", delta_control=DeltaControl.ZERO_DOMINATES),
])
assert isinstance(cum_sum("V"), UpdateByOperation)
nothing_added = source.update_by(cum_sum([]))
# oracle: step
# oracle: fails
not_a_pair = cum_sum("Sum of V")
# oracle: step
# oracle: fails
not_a_control = delta("Delta = N", "NULL_DOMINATES")
# oracle: step
# oracle: fails
not_a_column = cum_count_where(1, "N > 5")
# oracle: step
# oracle: fails
no_operation = source.update_by([])
# oracle: step
# oracle: fails
no_such_key = source.update_by(cum_sum("Sum = V"), by="K,N")
# oracle: step
counted = source.update_by([cum_count_where("Big", "N > 5"), cum_count_where("Both", ["N > 5", "K == 1"])], by="K")
