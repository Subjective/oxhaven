# The rolling operators of update_by over a window of time, given in nanoseconds and as
# duration strings; the windows they refuse; and last the rolling formula and count where,
# which wait for engine operators.
# oracle: results windows, groups, by_formula, by_columns, counted
# oracle: computed windows.Sum, windows.Avg, windows.Prod, windows.Std, windows.WAvg
from deephaven import empty_table
import deephaven.updateby as uby

source = empty_table(10).update([
    "K = i % 2",
    "T = '2024-01-01T00:00:00Z' + i * SECOND",
    "V = (i % 4 == 3) ? NULL_DOUBLE : i * 0.5 + 1",
    "N = 10 - i",
    "W = i % 3 + 1",
])
windows = source.update_by([
    uby.rolling_sum_time("T", "Sum = V", rev_time="PT3s"),
    uby.rolling_avg_time("T", "Avg = V", "PT2s", "PT1s"),
    uby.rolling_min_time("T", "Min = N", 2_000_000_000),
    uby.rolling_max_time("T", "Max = N", "PT00:00:02", 1_000_000_000),
    uby.rolling_prod_time("T", "Prod = N", rev_time="PT2s"),
    uby.rolling_count_time("T", ["Count = V", "CountN = N"], "PT3s"),
    uby.rolling_std_time("T", "Std = V", "PT3s"),
    uby.rolling_wavg_time("T", "W", "WAvg = V", rev_time="PT3s"),
], by="K")
groups = source.update_by(uby.rolling_group_time("T", ["Ns = N", "Vs = V"], rev_time="PT2s", fwd_time="PT1s"))
# oracle: step
# oracle: fails
negative = uby.rolling_sum_time("T", "Sum = V", rev_time=-5)
# oracle: step
# oracle: fails
not_a_duration = uby.rolling_avg_time("T", "Avg = V", rev_time="three seconds")
# oracle: step
# oracle: fails
no_time_column = uby.rolling_min_time("", "Min = V", rev_time=5)
# oracle: step
# oracle: fails
not_a_time = source.update_by(uby.rolling_sum_time("K", "Sum = V", rev_time=5))
# oracle: step
by_formula = source.update_by(uby.rolling_formula_time("T", "sum(x)", "x", ["Total = N"], rev_time="PT2s"), by="K")
# oracle: step
by_columns = source.update_by(uby.rolling_formula_time("T", "Out = sum(N)", rev_time="PT2s", fwd_time="PT1s"))
# oracle: step
counted = source.update_by(uby.rolling_count_where_time("T", "Big", "N > 5", rev_time="PT3s", fwd_time="PT1s"), by="K")
