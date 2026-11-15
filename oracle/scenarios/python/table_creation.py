# Table creation: empty_table, time_table from a start time in the past, merge and
# merge_sorted. The time table has its rows after one update cycle; step 1 only waits.
# oracle: results empty, clock, merged, sorted_merge
from deephaven import empty_table, merge, merge_sorted, time_table

empty = empty_table(0)
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC").head(4)
a = empty_table(3).update(["K = i", "V = i * 10"])
b = empty_table(3).update(["K = i + 1", "V = i * 100"])
merged = merge([a, b])
sorted_merge = merge_sorted([a, b], "K")
# oracle: step
