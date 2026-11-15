# The joins that match several rows or the nearest row: join, aj and raj; then the forms
# of their arguments, the comparisons of an as-of match, and what they refuse.
# oracle: results crossed, keyed, as_of, reverse_as_of, keyed_all, strictly_before, unkeyed, by_default, strictly_after, unkeyed_reverse
from deephaven import empty_table

left = empty_table(4).update(["K = i % 2", "T = i * 10 + 5"])
right = empty_table(6).update(["K = i % 2", "T = i * 7", "R = i"])
crossed = left.view("L = K").join(right.head(2).view("R"))
keyed = left.join(right, on="K", joins=["RightT = T", "R"])
as_of = left.aj(right, on=["K", "T"], joins=["RightT = T", "R"])
reverse_as_of = left.raj(right, on=["K", "T"], joins=["RightT = T", "R"])

stamps = left.update("T = i * 7")
keyed_all = left.view("K").join(right.view(["Key = K", "R"]), "K = Key")
strictly_before = stamps.aj(right, on="K, T > T", joins="RightT = T, R")
unkeyed = left.aj(right, on="T >= T", joins="RightT = T")
by_default = left.aj(right.view(["T", "R"]), on="T")
strictly_after = stamps.raj(right, on=["K", "T < T"], joins=["RightT = T", "R"])
unkeyed_reverse = left.raj(right, on="T <= T", joins="RightT = T")
# oracle: step
# oracle: fails
same_names = left.join(right, "K")
# oracle: step
# oracle: fails
wrong_side = left.aj(right, on="T < T", joins="RightT = T")
# oracle: step
# oracle: fails
no_stamp = left.aj(right, on=[], joins="RightT = T")
# oracle: step
# oracle: fails
wrong_side_reverse = left.raj(right, on="T >= T", joins="RightT = T")
