# Ordering: sort on one and on several columns, with directions, and reverse, of a static
# table and of a ticking one.
# oracle: results ascending, descending, mixed, one_direction, unsorted, reversed, twice_reversed, clock_descending, clock_reversed
from deephaven import SortDirection, empty_table, time_table

source = empty_table(8).update(["K = (i * 3) % 4", "V = 8 - i", "S = `s` + (i % 2)"])
ascending = source.sort("K")
descending = source.sort_descending(["K", "V"])
mixed = source.sort(["S", "V"], [SortDirection.DESCENDING, SortDirection.ASCENDING])
one_direction = source.sort("V", SortDirection.DESCENDING)
unsorted = source.sort([])
reversed = source.reverse()
twice_reversed = reversed.reverse()
clock = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC").head(5)
clock_descending = clock.update("Hour = ii").sort_descending("Hour")
clock_reversed = clock.reverse()
