# Application fields of the remote: resolved as `/app/<id>/field/<name>` and as
# `/field/<name>` (the host is the application id), subscribed and snapshotted by the ticket
# `a/<id>/f/<name>`; and the calls that answer from the local server: a local variable, a
# value, a missing variable or application (None), and an authority Java cannot read as a
# host, which makes the URI a local one (owner item 7.23).
# oracle: results r_app, r_sorted, r_host, b_app, s_app, local_t, r_local, outcomes
import os

from deephaven import empty_table, new_table
from deephaven.barrage import barrage_session
from deephaven.column import string_col
from deephaven.table import Table
from deephaven.uri import resolve

target = os.environ["ORACLE_REMOTE_TARGET"]
host, port = target.rsplit(":", 1)
r_app = resolve(f"dh+plain://{target}/app/remote_app/field/app_t")
r_sorted = resolve(f"dh+plain://{target}/app/remote_app/field/app_sorted")
r_host = resolve(f"dh+plain://{target}/field/host_t")
bs = barrage_session(host, int(port))
b_app = bs.subscribe(b"a/remote_app/f/app_t")
s_app = bs.snapshot("a/remote_app/f/app_sorted")

local_t = empty_table(3).update("X = i")
local_value = 42
r_local = resolve("dh:///scope/local_t")


def shown(value):
    if isinstance(value, Table):
        return f"Table rows={value.size} refreshing={value.is_refreshing}"
    return f"value {value!r}"


cases = [
    ("local value", "dh:///scope/local_value"),
    ("local missing", "dh:///scope/missing"),
    ("local app missing", "dh:///app/none/field/f"),
    ("7.23 host_name is local", "dh://host_name/scope/local_t"),
    ("7.23 huge port is local", "dh://h:99999999999/scope/local_value"),
]
outcomes = new_table([
    string_col("Case", [name for name, _ in cases]),
    string_col("Outcome", [shown(resolve(uri)) for _, uri in cases]),
])
# oracle: step
bs.close()
bs.close()
