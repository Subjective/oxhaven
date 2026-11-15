# The select family beyond update and view: dropping and deduplicating columns, and the
# table that describes the columns; then renaming and moving columns and naming the columns
# a table may be sorted by, one a step.
# oracle: results dropped, dropped_one, distinct, distinct_pair, distinct_all, distinct_formula, described, nothing_renamed, nothing_moved, renamed, moved, moved_up, moved_down, restricted
from deephaven import empty_table

source = empty_table(6).update(["A = i", "B = i % 2", "C = `c` + (i % 3)", "D = i * 1.5"])
dropped = source.drop_columns(["B", "D"])
dropped_one = source.drop_columns("C")
distinct = source.select_distinct("B")
distinct_pair = source.select_distinct(["C", "B"])
distinct_all = source.view(["B", "Half = A / 3"]).select_distinct()
distinct_formula = source.select_distinct(["B", "Third = A % 3 == 0"])
described = source.meta_table
nothing_renamed = source.rename_columns([])
nothing_moved = source.move_columns_up([])
# oracle: step
renamed = source.rename_columns(["First = A", "Last = D"])
# oracle: step
moved = source.move_columns(2, ["A"])
# oracle: step
moved_up = source.move_columns_up(["D", "C"])
# oracle: step
moved_down = source.move_columns_down("A")
# oracle: step
restricted = source.restrict_sort_to("B").sort("B")
