# The key tables of a view of a tree: nodes named by their identifier, with and without
# an action column; a row that has no children and an identifier that is no row; expansion
# to a depth and a contraction under an expansion of everything; a key table that ticks;
# and the keys Java fails a view for: a row whose parent is no row but that has children,
# two rows that are each other's ancestors, a column of another type and no column.
# oracle: views chosen = family chosen_keys, shallow = family shallow_keys A, contracted = family contracted_keys A
# oracle: views promoted_chosen = promoted chosen_keys, followed = family live_keys A
# oracle: views orphan = family orphan_keys, childless_orphan = family childless_keys
# oracle: views one_of_a_loop = loops one_keys, two_of_a_loop = loops two_keys
# oracle: views words_chosen = words word_keys, mistyped = words chosen_keys, unnamed = family unnamed_keys
from deephaven import input_table, new_table
from deephaven.column import byte_col, int_col, string_col


def rows(ids, parents, names):
    return new_table([int_col("Id", ids), int_col("Parent", parents), string_col("Name", names)])


def expansions(ids, actions):
    return new_table([int_col("Id", ids), byte_col("A", actions)])


source = rows(
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    [None, None, 1, 1, 3, 2, 99, 8, 7, 98],
    list("abcdefghij"),
)
family = source.tree("Id", "Parent")
promoted = source.tree("Id", "Parent", promote_orphans=True)
# 6 has no children and 42 is no row; 7 has a child and, unless promoted, no parent that
# is a row, which fails the view
chosen_keys = new_table([int_col("Id", [1, 3, 7, 6, 42])])
# the root and one level under it
shallow_keys = expansions([None], [0x71])
contracted_keys = expansions([None, 3], [3, 4])
live_keys = input_table(init_table=expansions([1], [1]), key_cols="Id")
# 7 has the child 9, and its parent 99 is no row; 10 has no children
orphan_keys = new_table([int_col("Id", [7])])
childless_keys = new_table([int_col("Id", [1, 10])])
loops = rows([1, 2, 3, 4, 5], [None, 2, 2, 5, 4], ["a", "self", "c", "x", "y"]).tree("Id", "Parent")
one_keys = new_table([int_col("Id", [1, 4])])
two_keys = new_table([int_col("Id", [4, 5])])
words = new_table([
    string_col("Id", ["a", "b", "c"]),
    string_col("Parent", [None, "a", "b"]),
]).tree("Id", "Parent")
word_keys = new_table([string_col("Id", ["a", "b"])])
unnamed_keys = new_table([int_col("X", [1])])
# oracle: step
live_keys.add(expansions([3, 2], [1, 1]))
# oracle: step
live_keys.delete(expansions([1], [1]).view("Id"))
# oracle: step
live_keys.add(expansions([None], [3]))
