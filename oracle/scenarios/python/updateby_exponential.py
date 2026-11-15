# The exponential moving operators of update_by with a decay in rows and in time, and the
# OperationControl that says what a null or a NaN does to them.
# oracle: results by_rows, by_time, skipped, poisoned, restarted, with_context
# oracle: computed by_rows.Ema, by_rows.Ems, by_rows.Min, by_rows.Max, by_rows.Std, by_time.Ema, by_time.Ems, by_time.Min, by_time.Max, by_time.Std, skipped.Ema, poisoned.Ema, restarted.Ema, with_context.Ema
from deephaven import empty_table
from deephaven.updateby import (
    BadDataBehavior,
    MathContext,
    OperationControl,
    ema_tick,
    ema_time,
    emmax_tick,
    emmax_time,
    emmin_tick,
    emmin_time,
    ems_tick,
    ems_time,
    emstd_tick,
    emstd_time,
)

source = empty_table(10).update([
    "K = i % 2",
    "T = '2024-01-01T00:00:00Z' + i * SECOND",
    "V = (i % 4 == 3) ? NULL_DOUBLE : i * 0.5 + 1",
    "B = (i % 4 == 1) ? Double.NaN : i * 1.0",
])
by_rows = source.update_by([
    ema_tick(2, "Ema = V"),
    ems_tick(2.5, "Ems = V"),
    emmin_tick(2, "Min = V"),
    emmax_tick(decay_ticks=2, cols="Max = V"),
    emstd_tick(2, ["Std = V"]),
], by="K")
by_time = source.update_by([
    ema_time("T", "PT2s", "Ema = V"),
    ems_time("T", 2_000_000_000, "Ems = V"),
    emmin_time("T", "PT00:00:02", "Min = V"),
    emmax_time(ts_col="T", decay_time="PT2S", cols="Max = V"),
    emstd_time("T", "PT2s", ["Std = V"]),
], by=["K"])
skipped = source.update_by(ema_tick(2, "Ema = B"))
poisoned = source.update_by(ema_tick(2, "Ema = B", OperationControl(on_nan=BadDataBehavior.POISON)))
restarted = source.update_by(ema_tick(2, "Ema = B", op_control=OperationControl(
    on_null=BadDataBehavior.RESET, on_nan=BadDataBehavior.RESET)))
with_context = source.update_by(ema_time("T", "PT2s", "Ema = V", OperationControl(
    on_null=BadDataBehavior.POISON, big_value_context=MathContext.UNLIMITED)))
# oracle: step
# oracle: fails
thrown = source.update_by(ema_tick(2, "Ema = B", OperationControl(on_nan=BadDataBehavior.THROW)))
# oracle: step
# oracle: fails
not_a_behavior = OperationControl(on_null="SKIP")
# oracle: step
# oracle: fails
not_a_duration = ema_time("T", "two seconds", "Ema = V")
# oracle: step
# oracle: fails
no_time_column = ems_time("", 5, "Ems = V")
# oracle: step
# oracle: fails
not_a_number = emmin_tick("2", "Min = V")
