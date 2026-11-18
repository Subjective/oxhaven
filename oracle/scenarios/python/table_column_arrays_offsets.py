# Column arrays that Java does not read as a column at a fixed distance, and offsets it cannot
# read: `(X_)[i - 1]` and `X_[(i + 1)]` index the whole column, an offset is read with
# Long#parseLong (so `0x10`, `1_0` and `0b1` fail the formula), a column named outside ASCII
# is shifted, and two distances in one formula on a table made again after each change.
# oracle: results paren, viewed, kept, unicode, fresh, filtered
from deephaven import empty_table, input_table, new_table
from deephaven.column import int_col


def rows(keys, values):
    return new_table([int_col("K", keys), int_col("X", values)])


def again(table):
    return table.snapshot().update(
        ["M = X_[i - 1] + X_[i + 1]", "P = X_[i - 1] + X_[i - 2]", "Q = X_[i - 1] * 1000 + X_[i + 2]"]
    )


source = empty_table(6).update(["X = i * 10"]).where("X != 20")
paren = source.update(["P = (X_)[i - 1]", "Q = X_[(i + 1)]", "R = isNull(X_[i + 0x1])"])
viewed = source.view(["X", "P = ((X_))[i]", "N = X_[i + 010]", "L = X_[i - 2147483648]"])
kept = source.where("(X_)[i - 1] > 0")
unicode = new_table([int_col("Ä", [1, 2, 3])]).update("Y = Ä_[i - 1]")
keyed = input_table(
    init_table=rows([1, 2, 3, 4, 5, 6, 7, 8], [10, 20, 30, 40, 50, 60, 70, 80]), key_cols="K"
)
fresh = again(keyed)
filtered = source.where("X_[i - 1] + X_[i + 1] > 40")
# oracle: step
keyed.add(rows([4], [41]))
fresh = again(keyed)
# oracle: step
keyed.add(rows([9], [90]))
fresh = again(keyed)
# oracle: step
keyed.delete(rows([6], [0]).view("K"))
fresh = again(keyed)
# oracle: step
# oracle: fails
refused = source.update("P = X_[i + 0x10]")
# oracle: step
# oracle: fails
refused = source.update_view("P = Nope + X_[i - 1_0]")
# oracle: step
# oracle: fails
refused = source.where("X_[i + 0b1] > 0")
# oracle: step
# oracle: fails
refused = keyed.update("P = (X_)[i - 1]")
# oracle: step
# oracle: fails
refused = source.update("X_ = X + 1").update("P = X_[i - 1]")
