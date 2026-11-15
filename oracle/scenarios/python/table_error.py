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
