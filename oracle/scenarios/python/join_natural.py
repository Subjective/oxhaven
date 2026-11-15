# The joins that add the columns of at most one right row: natural_join with and without
# a column list, and exact_join; then the forms of their arguments, the join types, and
# what they refuse.
# oracle: results all_columns, some_columns, exact, from_text, renamed_key, first_match, last_match, one_match, no_key, exact_all
from deephaven import empty_table

left = empty_table(6).update(["K = i % 4", "L = i"])
right = empty_table(3).update(["K = i", "R = i * 10", "S = `r` + i"])
all_columns = left.natural_join(right, on="K")
some_columns = left.natural_join(right, on=["K"], joins=["Right = R"])
exact = left.where("K < 3").exact_join(right, on="K", joins="S")

from deephaven.table import NaturalJoinType

repeated = empty_table(6).update(["K = i % 3", "R = i * 10"])
from_text = left.natural_join(right, on=" K ", joins="Right = R, S")
renamed_key = left.natural_join(right, on="L == K", joins=["Right=R", "S"])
first_match = left.natural_join(repeated, on="K", joins="R", type=NaturalJoinType.FIRST_MATCH)
last_match = left.natural_join(repeated, on="K", type=NaturalJoinType.LAST_MATCH)
one_match = left.where("K < 3").natural_join(right, on="K", joins="R", type=NaturalJoinType.EXACTLY_ONE_MATCH)
no_key = left.natural_join(right.head(1), on=[], joins="R")
exact_all = left.where("K < 3").exact_join(right, on=["K"])
# oracle: step
# oracle: fails
duplicates = left.natural_join(repeated, on="K", joins="R")
# oracle: step
# oracle: fails
not_a_type = left.natural_join(right, on="K", joins="R", type="FIRST_MATCH")
# oracle: step
# oracle: fails
no_match = left.exact_join(right, on="K", joins="S")
# oracle: step
# oracle: fails
no_column = left.natural_join(right, on="K = Nope")
