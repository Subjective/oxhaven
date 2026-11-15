# The select family beyond update and view: dropping, renaming, moving and deduplicating
# columns, and the table that describes the columns.
# oracle: results dropped, renamed, moved, moved_up, moved_down, distinct, described
from deephaven import empty_table

source = empty_table(6).update(["A = i", "B = i % 2", "C = `c` + (i % 3)", "D = i * 1.5"])
dropped = source.drop_columns(["B", "D"])
renamed = source.rename_columns(["First = A", "Last = D"])
moved = source.move_columns(2, ["A"])
moved_up = source.move_columns_up(["D", "C"])
moved_down = source.move_columns_down("A")
distinct = source.select_distinct("B")
described = source.meta_table
