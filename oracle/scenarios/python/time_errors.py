# What deephaven.time refuses. The table made first is there after each refusal.
# oracle: results kept
# oracle: fails
import numpy as np

from deephaven import new_table
from deephaven.column import datetime_col
from deephaven.time import (
    time_zone_alias_add, to_j_duration, to_j_instant, to_j_local_date, to_j_local_time,
    to_j_period, to_j_time_zone, to_j_zdt,
)

kept = new_table([datetime_col("T", ["2024-12-01T00:00:00 UTC"])])
to_j_instant("2024-13-01T00:00:00 UTC")
# oracle: step
# oracle: fails
to_j_instant("2024-01-01T00:00:00 Nowhere")
# oracle: step
# oracle: fails
to_j_instant(1.5)
# oracle: step
# oracle: fails
to_j_time_zone("Nowhere/None")
# oracle: step
# oracle: fails
to_j_instant("2024-07-01T12:00:00 Z")
# oracle: step
# oracle: fails
to_j_time_zone(5)
# oracle: step
# oracle: fails
to_j_local_date("2024-13-01")
# oracle: step
# oracle: fails
to_j_local_time("25:00")
# oracle: step
# oracle: fails
to_j_local_time(np.datetime64("2024-01-02T03:04:05.000000007"))
# oracle: step
# oracle: fails
to_j_zdt("2024-01-02")
# oracle: step
# oracle: fails
to_j_duration("1 hour")
# oracle: step
# oracle: fails
to_j_period("PT1H")
# oracle: step
# oracle: fails
time_zone_alias_add("TOK", "Asia/Tokyo")
# oracle: step
# oracle: fails
time_zone_alias_add("ET", "Europe/Paris")
