# Column arrays in formulas and filters: `Col_[i - 1]` reads a column at another row and
# `Col_` is the whole column, on a static table whose row keys are not its positions and
# on input tables whose rows are added, replaced and deleted.
# oracle: results before, around, whole, stored, viewed, lazy, kept, kept_two, kept_whole
# oracle: results keyed_before, keyed_after, keyed_both, keyed_kept, keyed_chain
# oracle: results keyed_fixed, keyed_size
# oracle: results appended_before, appended_ahead, appended_kept
from deephaven import empty_table, input_table, new_table
from deephaven.column import int_col


def rows(keys, values):
    return new_table([int_col("K", keys), int_col("X", values)])


source = empty_table(8).update(
    ["X = i * 10", "S = `s` + i", "Z = i % 3 == 0 ? (Boolean)null : (i % 2 == 0)"]
).where("X != 20")
before = source.update(
    ["P = X_[i - 1]", "N = X_[ii + 2]", "T = S_[i - 1]", "B = Z_[i + 1]", "K = k"]
)
around = source.update(
    ["D = X - X_[i - 1]", "M = (X_[i - 1] + X_[i + 1]) / 2", "Q = D_[i - 1] + 1"]
)
whole = source.update(
    [
        "F = X_[1]",
        "L = X_[-1]",
        "C = X_.size()",
        "U = sum(X_)",
        "G = X_[6 - i]",
        "W = isNull(X_[i - 1])",
        "V = X_.subVector(i, i + 2)",
        "E = Z_[1]",
    ]
)
stored = source.select(["X", "P = X_[i - 1]", "R = P + 1"])
viewed = source.view(["P = X_[i + 1]", "X"])
lazy = source.lazy_update(["P = X_[i - 1] * 2"])
kept = source.where("X_[i - 1] > 10")
kept_two = source.where(["X > 10", "X_[i - 1] < X_[i + 1] - 20 || isNull(X_[i + 1])"])
kept_whole = source.where("X_[1] < X && X_.size() > i + 2")

keyed = input_table(
    init_table=rows([1, 2, 3, 4, 5, 6], [10, 20, 30, 40, 50, 60]), key_cols="K"
)
keyed_before = keyed.update(["P = X_[i - 1]", "D = X - P"])
keyed_after = keyed.update_view(["N = X_[i + 2]"])
keyed_both = keyed.view(["K", "S = X_[i - 1] + X_[i]"])
keyed_kept = keyed.where("X_[i - 1] > 15")
keyed_chain = keyed.update("P = X_[i - 1]").where("P_[i + 1] > 30")
# A filter may use a column array in any way. Java asks it again about a row only when
# that row is added or modified.
keyed_fixed = keyed.where("X_[1] > 15")
keyed_size = keyed.where("X_.size() > i + 3")

appended = input_table(init_table=rows([1, 2, 3], [10, 20, 30]))
appended_before = appended.update(["P = X_[i - 1]", "I = i"])
appended_ahead = appended.update_view(["N = X_[i + 1]"])
appended_kept = appended.where("X_[i - 1] > 5 && i > 0")
# oracle: step
keyed.add(rows([3, 7], [31, 70]))
appended.add(rows([4], [40]))
# oracle: step
keyed.delete(rows([2], [0]).view("K"))
appended.add(rows([5, 6], [50, 60]))
# oracle: step
keyed.add(rows([2, 6, 0], [21, 61, 5]))
# oracle: step
keyed.delete(rows([1, 7], [0, 0]).view("K"))
# oracle: step
# oracle: fails
refused = keyed.update("P = X_[i - 1] + i")
# oracle: step
# oracle: fails
refused = keyed.update("P = X_[1]")
# oracle: step
# oracle: fails
refused = appended.update("P = isNull(X_[i - 1])")
# oracle: step
# oracle: fails
refused = source.update("P = Nope_[i - 1]")
# oracle: step
# oracle: fails
refused = source.where("Z_[i - 1]")
