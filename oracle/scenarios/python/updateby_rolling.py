# Windowed update_by operators over rows: a rolling sum and average, and an exponential
# moving average; then every other rolling operator, the windows they refuse when they are
# made, and last the rolling formula and count where, one a step.
# oracle: results rolled, smoothed, windows, groups, by_formula, by_columns, counted
# oracle: computed rolled.Sum, rolled.Avg, smoothed.Ema, windows.Prod, windows.Std, windows.WAvg
from deephaven import empty_table
from deephaven.updateby import ema_tick, rolling_avg_tick, rolling_sum_tick

source = empty_table(10).update(["K = i % 2", "V = i * 0.25"])
rolled = source.update_by([
    rolling_sum_tick("Sum = V", rev_ticks=3),
    rolling_avg_tick("Avg = V", rev_ticks=2, fwd_ticks=1),
], by="K")
smoothed = source.update_by(ema_tick(decay_ticks=2, cols="Ema = V"))

import deephaven.updateby as uby

wide = source.update(["V = (i % 4 == 3) ? NULL_DOUBLE : V", "N = 10 - i", "W = i % 3 + 1"])
windows = wide.update_by([
    uby.rolling_min_tick("Min = N", 2),
    uby.rolling_max_tick("Max = N", 2, 1),
    uby.rolling_prod_tick("Prod = N", rev_ticks=2),
    uby.rolling_count_tick(["Count = V", "CountN = N"], 3),
    uby.rolling_std_tick("Std = V", 3),
    uby.rolling_wavg_tick("W", "WAvg = V", rev_ticks=3),
    uby.rolling_sum_tick("Ahead = N", rev_ticks=-1, fwd_ticks=3),
], by="K")
groups = wide.update_by(uby.rolling_group_tick(["Ns = N", "Vs = V"], rev_ticks=2, fwd_ticks=1), by=["K"])
# oracle: step
# oracle: fails
negative = uby.rolling_sum_tick("Sum = V", rev_ticks=-5)
# oracle: step
# oracle: fails
too_wide = uby.rolling_avg_tick("Avg = V", rev_ticks=2**40)
# oracle: step
# oracle: fails
not_whole = uby.rolling_min_tick("Min = V", rev_ticks=1.5)
# oracle: step
# oracle: fails
no_formula = uby.rolling_formula_tick("", rev_ticks=2)
# oracle: step
# oracle: fails
no_result = uby.rolling_formula_tick("sum(N)", rev_ticks=2)
# oracle: step
# oracle: fails
not_a_column = uby.rolling_count_where_tick(None, "N > 5", 2)
# oracle: step
by_formula = wide.update_by(uby.rolling_formula_tick("sum(x)", "x", "Total = N", rev_ticks=2), by="K")
# oracle: step
by_columns = wide.update_by(uby.rolling_formula_tick("Out = sum(N) + max(W)", rev_ticks=2, fwd_ticks=1))
# oracle: step
counted = wide.update_by(uby.rolling_count_where_tick("Big", ["N > 5"], rev_ticks=3, fwd_ticks=1), by="K")
