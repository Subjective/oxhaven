# The joins that add the columns of at most one right row: natural_join with and without
# a column list, and exact_join.
# oracle: results all_columns, some_columns, exact
from deephaven import empty_table

left = empty_table(6).update(["K = i % 4", "L = i"])
right = empty_table(3).update(["K = i", "R = i * 10", "S = `r` + i"])
all_columns = left.natural_join(right, on="K")
some_columns = left.natural_join(right, on=["K"], joins=["Right = R"])
exact = left.where("K < 3").exact_join(right, on="K", joins="S")
