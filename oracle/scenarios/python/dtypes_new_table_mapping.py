# new_table from a mapping of column names to values, which infers the column types.
# oracle: results inferred
from deephaven import new_table

inferred = new_table({
    "Int": [1, 2, 3],
    "Float": [0.5, 1.5, 2.5],
    "String": ["a", "b", "c"],
    "Bool": [True, False, True],
})
