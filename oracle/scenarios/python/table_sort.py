# Ordering: sort on one and on several columns, with directions, and reverse.
# oracle: results ascending, descending, mixed, reversed
from deephaven import SortDirection, empty_table

source = empty_table(8).update(["K = (i * 3) % 4", "V = 8 - i", "S = `s` + (i % 2)"])
ascending = source.sort("K")
descending = source.sort_descending(["K", "V"])
mixed = source.sort(["S", "V"], [SortDirection.DESCENDING, SortDirection.ASCENDING])
reversed = source.reverse()
