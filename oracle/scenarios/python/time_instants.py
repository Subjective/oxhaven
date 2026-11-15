# Instants: a column built from strings through deephaven.time, and time arithmetic on it.
# oracle: results instants, shifted
from deephaven import new_table
from deephaven.column import datetime_col, int_col
from deephaven.time import to_j_instant

instants = new_table([
    int_col("I", [0, 1, 2]),
    datetime_col("T", [
        to_j_instant("2024-01-01T00:00:00 UTC"),
        to_j_instant("2024-03-10T01:59:59.5 America/New_York"),
        None,
    ]),
])
shifted = instants.update(["Later = T + I * HOUR", "Nanos = epochNanos(T)"])
