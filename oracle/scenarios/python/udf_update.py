# Python functions called from query strings: with a return annotation in update, and as a
# filter in where.
# oracle: results doubled, kept
from deephaven import empty_table


def double_it(x) -> int:
    return 2 * x


def is_small(x) -> bool:
    return x < 3


source = empty_table(5).update("X = i")
doubled = source.update("Y = double_it(X)")
kept = source.where("is_small(X)")
