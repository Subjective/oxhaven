# What a key table asks of a view of a rollup: expand, expand all, contract under an expand
# all, expand to a depth; keys of nodes that are not there, keys below a node that is not
# expanded, a key given twice, the order of the rows, a key table without rows, one without
# an action column, one with more columns than the rollup groups by.
# oracle: views expand_one = r one A, expand_leaf_parent = r leaf_parent A, below_closed = r below_closed A
# oracle: views all_root = r all_root A, all_total = r all_total A, all_from_s1 = r all_from_s1 A
# oracle: views contracted = r contracted A, contracted_first = r contracted_first A, expanded_under_all = r expanded_under_all A
# oracle: views to_depth_0 = r to_depth_0 A, to_depth_1 = r to_depth_1 A, to_depth_2 = r to_depth_2 A, to_depth_9 = r to_depth_9 A
# oracle: views absent = r absent A, twice = r twice A, child_then_parent = r child_then_parent A, parent_then_child = r parent_then_child A
# oracle: views no_rows = r no_rows A, no_action = r no_action, wide = r wide A, total_closed = r total_closed A
from deephaven import agg, empty_table, new_table
from deephaven.column import byte_col, int_col, string_col

source = empty_table(36).update([
    "A1 = `a` + (i % 3)",
    "B1 = `b` + (i % 2)",
    "C1 = i % 4",
    "V = i",
])
r = source.rollup(aggs=[agg.sum_("V"), agg.count_("N")], by=["A1", "B1", "C1"])

EXPAND, EXPAND_ALL, CONTRACT = 1, 3, 4


def to_depth(levels):
    return 0x70 + levels


def keys(*rows, action=True):
    """A key table of `(depth, A1, B1, C1, action)` rows."""
    columns = [
        int_col("__DEPTH__", [row[0] for row in rows]),
        string_col("A1", [row[1] for row in rows]),
        string_col("B1", [row[2] for row in rows]),
        int_col("C1", [row[3] for row in rows]),
    ]
    if action:
        columns.append(byte_col("A", [row[4] for row in rows]))
    return new_table(columns)


ROOT = (0, None, None, None)
TOTAL = (1, None, None, None)
one = keys((2, "a1", None, None, EXPAND))
leaf_parent = keys((2, "a0", None, None, EXPAND), (3, "a0", "b1", None, EXPAND))
# the parent is not expanded, so the node is not shown
below_closed = keys((3, "a0", "b1", None, EXPAND))
all_root = keys((*ROOT, EXPAND_ALL))
all_total = keys((*TOTAL, EXPAND_ALL))
all_from_s1 = keys((2, "a1", None, None, EXPAND_ALL))
contracted = keys((*ROOT, EXPAND_ALL), (2, "a1", None, None, CONTRACT), (3, "a2", "b0", None, CONTRACT))
contracted_first = keys((2, "a1", None, None, CONTRACT), (*ROOT, EXPAND_ALL))
# an expand of its own ends the expand all at a node: its children are not expanded
expanded_under_all = keys((*ROOT, EXPAND_ALL), (2, "a1", None, None, EXPAND))
to_depth_0 = keys((*ROOT, to_depth(0)))
to_depth_1 = keys((*ROOT, to_depth(1)))
to_depth_2 = keys((*ROOT, to_depth(2)))
to_depth_9 = keys((2, "a2", None, None, to_depth(9)))
absent = keys((2, "zz", None, None, EXPAND), (3, "a0", "zz", None, EXPAND), (2, "a0", None, None, EXPAND))
twice = keys((2, "a0", None, None, EXPAND_ALL), (2, "a1", None, None, EXPAND), (2, "a0", None, None, EXPAND))
child_then_parent = keys((3, "a0", "b0", None, EXPAND), (2, "a0", None, None, EXPAND))
parent_then_child = keys((2, "a0", None, None, EXPAND), (3, "a0", "b0", None, EXPAND))
no_rows = keys((2, "a0", None, None, EXPAND)).head(0)
no_action = keys((2, "a0", None, None), (3, "a0", "b1", None), action=False)
wide = keys((2, "a2", None, None, EXPAND)).update("Extra = 7")
total_closed = keys((*TOTAL, CONTRACT))
