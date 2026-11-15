# Instants: a column built through deephaven.time from text, numbers and Python, numpy and
# pandas values, and time arithmetic on it.
# oracle: results instants, shifted, mixed
import datetime

import numpy as np
import pandas as pd

from deephaven import new_table
from deephaven.column import datetime_col, int_col, string_col
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
values = [
    ("text", "2024-01-02T03:04:05.123456789 ET"),
    ("text with an offset", "2024-01-02T03:04:05+01:00"),
    ("text of a date and time", "2024-01-02T03:04 UTC"),
    ("nanoseconds", 1704164645123456789),
    ("microseconds", 1704164645123456),
    ("milliseconds", 1704164645123),
    ("seconds", 1704164645),
    ("datetime in UTC", datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)),
    ("datetime with an offset", datetime.datetime(
        2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))),
    ("datetime64 of seconds", np.datetime64("2024-01-02T03:04:05")),
    ("datetime64 of nanoseconds", np.datetime64("2024-01-02T03:04:05.123456789")),
    ("Timestamp in UTC", pd.Timestamp("2024-01-02T03:04:05.123456789", tz="UTC")),
    ("Timestamp in a zone", pd.Timestamp("2024-07-02T03:04:05", tz="Europe/Paris")),
    ("Timestamp without a zone", pd.Timestamp("2024-01-02T03:04:05")),
    ("an instant", to_j_instant("1969-12-31T23:59:59.999999999 UTC")),
    ("None", None),
]
mixed = new_table([
    string_col("From", [name for name, _ in values]),
    datetime_col("T", [value for _, value in values]),
    string_col("Text", [str(to_j_instant(value)) for _, value in values]),
])
