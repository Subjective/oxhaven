# tree of a static table, read as a client reads it: the default view (the rows without a
# parent), and everything expanded; rows whose parent is no row, left out and promoted;
# rows that are their own ancestors; a row without an identifier, not expanded; identifiers
# that are strings; one column as both identifier and parent; and a tree of no rows.
# oracle: views family_default = family, family_all = family everything A
# oracle: views promoted_default = promoted, promoted_all = promoted everything A
# oracle: views loops_all = loops everything A, loops_promoted_all = loops_promoted everything A
# oracle: views nameless_default = nameless, words_default = words, words_all = words word_everything A
# oracle: views same_all = same everything A, nothing_all = nothing everything A
from deephaven import new_table
from deephaven.column import byte_col, int_col, string_col


def rows(ids, parents, names):
    return new_table([int_col("Id", ids), int_col("Parent", parents), string_col("Name", names)])


source = rows(
    [1, 2, 3, 4, 5, 6, 7, 8, 9],
    [None, None, 1, 1, 3, 2, 99, 8, 7],
    list("abcdefghi"),
)
family = source.tree("Id", "Parent")
promoted = source.tree(id_col="Id", parent_col="Parent", promote_orphans=True)
everything = new_table([int_col("Id", [None]), byte_col("A", [3])])
# 2 is its own parent, 4 and 5 are each other's
looping = rows([1, 2, 3, 4, 5], [None, 2, 2, 5, 4], ["a", "self", "c", "x", "y"])
loops = looping.tree("Id", "Parent")
loops_promoted = looping.tree("Id", "Parent", promote_orphans=True)
nameless = rows([None, 1, 2], [None, None, 1], ["n", "a", "b"]).tree("Id", "Parent")
words = new_table([
    string_col("Id", ["a", "b", "c", "d"]),
    string_col("Parent", [None, "a", "b", "a"]),
    int_col("V", [1, 2, 3, 4]),
]).tree("Id", "Parent")
word_everything = new_table([string_col("Id", [None]), byte_col("A", [3])])
same = rows([1, 2], [None, 1], ["a", "b"]).tree("Id", "Id")
nothing = rows([], [], []).tree("Id", "Parent")
