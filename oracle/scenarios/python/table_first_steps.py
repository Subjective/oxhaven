# What a user types first: tables from empty_table and new_table, the select family, where,
# head and tail; then what a table says about itself (its size, its columns, its text, its
# rows as Python values), which is compared as the strings of the table `facts`; then the
# operations that keep the rows and change the shape.
# oracle: results source, updated, viewed, selected, kept, first, last, typed, facts, described, flat, same, frozen, ungrouped
from deephaven import empty_table, new_table, time_table
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
# oracle: step
nulls = empty_table(3).update([
    "B = (byte)(i == 1 ? null : i)", "H = (short)(i == 1 ? null : i)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long)i", "F = (float)(i == 1 ? null : i * 0.5)",
    "D = i == 1 ? null : i * 1e10 / 3", "C = i == 1 ? null : (char)(65 + i)",
    "S = i == 1 ? null : `a string of more than ten characters ` + i",
    "Z = i == 1 ? null : i == 0",
])
ticking = time_table("PT1h", start_time="2024-01-01T00:00:00 UTC")
said = {
    "size": source.size,
    "size of kept": kept.size,
    "is_refreshing": (source.is_refreshing, ticking.is_refreshing, ticking.head(2).is_refreshing),
    "is_blink": (source.is_blink, ticking.is_blink, time_table("PT1h", blink_table=True).is_blink),
    "is_flat": (source.is_flat, kept.is_flat, kept.flatten().is_flat, ticking.is_flat),
    "column_names": typed.column_names,
    "columns": [(col.name, str(col.data_type)) for col in nulls.columns],
    "definition": (list(typed.definition), len(typed.definition), "L" in typed.definition),
    "same definition": (source.definition == first.definition, source.definition == typed.definition),
    "has_columns": (typed.has_columns("I"), typed.has_columns(["I", "S"]), typed.has_columns(["I", "X"])),
    "to_string": nulls.to_string(),
    "to_string of two": source.to_string(2, ["S", "I"]),
    "to_string of none": typed.to_string(0),
    "iter_dict": list(nulls.iter_dict()),
    "iter_dict of some": list(source.iter_dict(["S", "I"], chunk_size=4)),
    "iter_tuple": list(typed.iter_tuple()),
    "iter_tuple of some": list(typed.iter_tuple(["B", "S"], tuple_name="Row", chunk_size=2)),
    "iter of no rows": list(source.head(0).iter_dict()),
}
facts = new_table([
    string_col("Fact", list(said)),
    string_col("Value", [repr(value) for value in said.values()]),
])
described = typed.definition.table
flat = kept.flatten()
same = source.coalesce()
frozen = source.snapshot()
ungrouped = source.ungroup()
