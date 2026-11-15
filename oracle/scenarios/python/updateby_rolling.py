# Windowed update_by operators over rows: a rolling sum and average, and an exponential
# moving average.
# oracle: results rolled, smoothed
# oracle: computed rolled.Sum, rolled.Avg, smoothed.Ema
from deephaven import empty_table
from deephaven.updateby import ema_tick, rolling_avg_tick, rolling_sum_tick

source = empty_table(10).update(["K = i % 2", "V = i * 0.25"])
rolled = source.update_by([
    rolling_sum_tick("Sum = V", rev_ticks=3),
    rolling_avg_tick("Avg = V", rev_ticks=2, fwd_ticks=1),
], by="K")
smoothed = source.update_by(ema_tick(decay_ticks=2, cols="Ema = V"))
