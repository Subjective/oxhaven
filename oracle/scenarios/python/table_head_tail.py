# Rows by position: head and tail beyond the size, slice with positive and negative
# positions, of a static table and of a ticking one; then the percentage forms, one a step.
# oracle: results none, everything, middle, from_end, to_end, from_start, clock_tail, clock_slice, half, last_quarter, middle_half
from deephaven import empty_table, time_table

source = empty_table(8).update("I = i")
none = source.head(0)
everything = source.tail(100)
middle = source.slice(2, 5)
from_end = source.slice(-3, -1)
to_end = source.slice(5, 100)
from_start = source.slice(0, -6)
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC").head(6)
clock_tail = clock.tail(2)
clock_slice = clock.slice(1, -2)
# oracle: step
half = source.head_pct(0.5)
# oracle: step
last_quarter = source.tail_pct(0.25)
# oracle: step
middle_half = source.slice_pct(0.25, 0.75)
