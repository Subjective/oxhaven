# The joins that match several rows or the nearest row: join, aj and raj.
# oracle: results crossed, keyed, as_of, reverse_as_of
from deephaven import empty_table

left = empty_table(4).update(["K = i % 2", "T = i * 10 + 5"])
right = empty_table(6).update(["K = i % 2", "T = i * 7", "R = i"])
crossed = left.view("L = K").join(right.head(2).view("R"))
keyed = left.join(right, on="K", joins=["RightT = T", "R"])
as_of = left.aj(right, on=["K", "T"], joins=["RightT = T", "R"])
reverse_as_of = left.raj(right, on=["K", "T"], joins=["RightT = T", "R"])
