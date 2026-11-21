# URIs whose authority Java's URI parser cannot read as a host (owner item 7.23): Java
# takes them as local and resolves them on this server, its authority dropped; Oxhaven
# refuses them by the owner's ruling (PARITY.md A26), so the two steps that resolve them
# succeed in Java and fail here, and the baseline (remote-baseline.json) lists this scenario
# as that difference. The URIs that are local in both are in remote_app_fields.
# oracle: results local_t
import os

from deephaven import empty_table
from deephaven.uri import resolve

target = os.environ["ORACLE_REMOTE_TARGET"]
local_t = empty_table(3).update("X = i")
local_value = 42
# oracle: step
# an underscore in the host
host_name = resolve("dh://host_name/scope/local_t")
# oracle: step
# a port too large
huge_port = resolve("dh://h:99999999999/scope/local_value")
