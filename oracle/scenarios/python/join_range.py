# range_join: for each left row the right rows of its key whose value lies in a range the
# left row names, as grouped columns; the forms of the range match, and what it refuses.
# oracle: results inclusive, exclusive, neighbours
from deephaven import agg, empty_table

left = empty_table(4).update(["K = i % 2", "Lo = i * 2", "Hi = i * 2 + 5"])
right = empty_table(10).update(["K = i % 2", "V = i", "X = i * 1.5"])
inclusive = left.range_join(right, on=["K", "Lo <= V <= Hi"], aggs=agg.group("X"))
exclusive = left.range_join(right, on=["K", "Lo < V < Hi"], aggs=[agg.group(["X", "Values = V"])])
neighbours = left.range_join(right, on="<- Lo <= V <= Hi ->", aggs=[agg.group("X")])
# oracle: step
# oracle: fails
summed = left.range_join(right, on=["K", "Lo <= V <= Hi"], aggs=agg.sum_("X"))
# oracle: step
# oracle: fails
no_range = left.range_join(right, on=[], aggs=agg.group("X"))
# oracle: step
# oracle: fails
one_text = left.range_join(right, on="K, Lo <= V <= Hi", aggs=agg.group("X"))
