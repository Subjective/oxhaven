# The application `remote_app` of the remote: fields that `dh+plain://<remote>/app/remote_app/
# field/<name>` and the ticket `a/remote_app/f/<name>` name.
from deephaven import empty_table
from deephaven.appmode import get_app_state

app = get_app_state()
app["app_t"] = empty_table(12).update([
    "I = i", "S = i % 3 == 0 ? null : `app` + i", "D = i % 4 == 0 ? NULL_DOUBLE : i / 8.0",
])
app["app_sorted"] = empty_table(12).update(["K = (i * 7) % 12", "T = '2024-01-01T00:00:00Z' + K * MINUTE"]).sort("K")
