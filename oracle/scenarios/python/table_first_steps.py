# What the console could do before the Python API tasks: tables from empty_table and
# new_table, the select family, where, head and tail.
# oracle: results source, updated, viewed, selected, kept, first, last, typed
from deephaven import empty_table, new_table
from deephaven.column import bool_col, double_col, int_col, long_col, string_col

source = empty_table(10).update(["I = i", "D = i * 0.5", "S = `s` + (i % 3)"])
updated = source.update("Twice = I * 2")
viewed = source.view(["I", "Half = D / 2"])
selected = source.update_view("Odd = I % 2 == 1").select(["S", "Odd"])
kept = source.where(["I > 2", "D < 4.0"])
first = source.head(3)
last = source.lazy_update("Neg = -I").tail(2)
typed = new_table([
    int_col("I", [1, 2, 3]),
    long_col("L", [10, 20, 30]),
    double_col("D", [0.5, 1.5, 2.5]),
    string_col("S", ["a", "b", "c"]),
    bool_col("B", [True, False, True]),
])
