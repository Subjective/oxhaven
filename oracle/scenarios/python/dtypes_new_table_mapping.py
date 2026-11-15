# new_table from a mapping of column names to values, which infers the column types, also
# where a value is None.
# oracle: results inferred, with_none
from deephaven import new_table

inferred = new_table({
    "Int": [1, 2, 3],
    "Float": [0.5, 1.5, 2.5],
    "String": ["a", "b", "c"],
    "Bool": [True, False, True],
})
with_none = new_table({
    "Int": [1, None, 3],
    "Float": [0.5, None, 2.5],
    "String": ["a", None, "c"],
    "Bool": [True, None, False],
})
