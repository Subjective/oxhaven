# The application whose id is the remote's host, 127.0.0.1: `dh+plain://127.0.0.1:<port>/field/
# <name>` names its fields (FieldUri: the host is the application id).
from deephaven import empty_table
from deephaven.appmode import get_app_state

app = get_app_state()
app["host_t"] = empty_table(5).update(["H = i * 11", "S = `host` + i"])
