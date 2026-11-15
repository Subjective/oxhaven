# Table creation: empty_table, time_table from a start time in the past with the period and
# the start given as strings, as integers and as datetime objects, and merge of static and
# of ticking tables. The time tables have their rows after one update cycle; step 1 takes
# snapshots of them. merge_sorted is the last step.
# oracle: results empty, clock, clock_nanos, clock_delta, clock_micros, merged, merged_clocks, frozen, stamped, stamped_as, sorted_merge
from datetime import datetime, timedelta, timezone

from deephaven import empty_table, merge, merge_sorted, time_table

empty = empty_table(0)
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC").head(4)
clock_nanos = time_table(3_600_000_000_000, start_time=1_704_067_200_000_000_000).head(4)
clock_delta = time_table(
    timedelta(hours=1), start_time=datetime(2024, 1, 1, tzinfo=timezone.utc)
).head(4)
clock_micros = time_table(
    timedelta(minutes=30, microseconds=7),
    start_time=datetime(2024, 1, 1, 0, 0, 0, 123457, tzinfo=timezone.utc),
).head(4)
a = empty_table(3).update(["K = i", "V = i * 10"])
b = empty_table(3).update(["K = i + 1", "V = i * 100"])
merged = merge([a, b])
merged_clocks = merge([clock, clock_nanos])
# oracle: step
frozen = clock.snapshot()
stamped = a.snapshot_when(clock, initial=True)
stamped_as = a.snapshot_when(clock, stamp_cols="At = Timestamp", initial=True)
# oracle: step
sorted_merge = merge_sorted([a, b], "K")
