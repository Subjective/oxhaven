# Scripts that fail: in a table operation the exception is DHError and its message says
# which operation failed; anything else is the Python exception it is. The variable
# assigned before the failure is still there, and the one that was not assigned is not.
# oracle: results before, after
from deephaven import empty_table

before = empty_table(3).update("I = i")
# oracle: fails
after = before.update("J = NoSuchColumn + 1")
# oracle: step
# oracle: fails
after = before.where("I >")
# oracle: step
# oracle: fails
after = before.view(["I"]).select("NoSuchColumn")
# oracle: step
# oracle: fails
after = before.head(1) / 0
# oracle: step
# oracle: fails
after = before.update([])
# oracle: step
# oracle: fails
after = before.sort("I", "ASCENDING")
# oracle: step
# oracle: fails
after = before.sort_descending("NoSuchColumn")
# oracle: step
# oracle: fails
after = before.drop_columns("NoSuchColumn")
# oracle: step
# oracle: fails
after = before.head(1.5)
# oracle: step
# oracle: fails
after = before.slice(2, 1)
# oracle: step
# oracle: fails
after = before.select_distinct("NoSuchColumn")
# oracle: step
# oracle: fails
after = before.where_in(before.view("J = I"), "I")
# oracle: step
# oracle: fails
after = before.snapshot_when(before, history=True, incremental=True)
# oracle: step
# oracle: fails
after = before.where_one_of()
# oracle: step
# oracle: fails
after = before.head_pct(2.0)
# oracle: step
# oracle: fails
after = before.to_string(cols="NoSuchColumn")
# oracle: step
# oracle: fails
after = list(before.iter_dict("NoSuchColumn"))
# oracle: step
# oracle: fails
after = list(before.iter_tuple(chunk_size=-1))
