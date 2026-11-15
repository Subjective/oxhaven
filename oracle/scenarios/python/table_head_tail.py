# Rows by position: head and tail beyond the size, slice with positive and negative
# positions, and the percentage forms.
# oracle: results none, everything, middle, from_end, half, last_quarter
from deephaven import empty_table

source = empty_table(8).update("I = i")
none = source.head(0)
everything = source.tail(100)
middle = source.slice(2, 5)
from_end = source.slice(-3, -1)
half = source.head_pct(0.5)
last_quarter = source.tail_pct(0.25)
