# The error outcomes of deephaven.uri.resolve and deephaven.barrage that task 13.2b compared
# with Deephaven by hand, one step each: every step's call must fail, with Java's exception
# type and, for a DHError, its message (the cause is not compared: PARITY.md A5). Steps 0
# and the last make and read sessions that do work.
# oracle: results ok_t
import os

from deephaven import empty_table
from deephaven.barrage import barrage_session
from deephaven.uri import resolve

target = os.environ["ORACLE_REMOTE_TARGET"]
host, port = target.rsplit(":", 1)
bs = barrage_session(host, int(port))
closed = barrage_session(host, int(port))
closed.close()
closed.close()
with barrage_session(host, int(port)) as scoped:
    ok_t = scoped.snapshot(b"s/types_t").view(["N", "St"])
# oracle: step
# oracle: fails
# no resolver for dh+plain without a host
resolve("dh+plain:///scope/x")
# oracle: step
# oracle: fails
# no resolver for a local field
resolve("dh:///field/f")
# oracle: step
# oracle: fails
# schemes are exact
resolve("DH:///scope/x")
# oracle: step
# oracle: fails
# a relative URI has no scheme
resolve("scope/x")
# oracle: step
# oracle: fails
# an empty URI
resolve("")
# oracle: step
# oracle: fails
# parquet with a host
resolve("parquet://host/x.parquet")
# oracle: step
# oracle: fails
# a syntax error
resolve("dh:///scope/a b")
# oracle: step
# oracle: fails
# a scheme alone
resolve("dh:")
# oracle: step
# oracle: fails
# a variable name that is not one
resolve("dh:///scope/a%2Fb")
# oracle: step
# oracle: fails
# a field name that is not one
resolve("dh:///app/a/field/b%20c")
# oracle: step
# oracle: fails
# a URI that is not a string
resolve(5)
# oracle: step
# oracle: fails
# a proxied URI
resolve(f"dh+plain://{target}?uri=dh%2Bplain%3A%2F%2Fo%2Fscope%2Fx")
# oracle: step
# oracle: fails
# a remote custom URI
resolve(f"dh+plain://{target}?uri=https%3A%2F%2Fexample.com%2Fx")
# oracle: step
# oracle: fails
# a remote variable that is missing
resolve(f"dh+plain://{target}/scope/missing")
# oracle: step
# oracle: fails
# a remote application that is missing
resolve(f"dh+plain://{target}/app/none/field/f")
# oracle: step
# oracle: fails
# TLS to the remote (owner item 7.24)
resolve(f"dh://{target}/scope/types_t")
# oracle: step
# oracle: fails
# a refused connection
resolve("dh+plain://127.0.0.1:1/scope/x")
# oracle: step
# oracle: fails
# an opaque CSV URI
resolve("csv:x.csv")
# oracle: step
# oracle: fails
# a CSV file that is missing
resolve("csv:///nonexistent-oracle-directory/missing.csv")
# oracle: step
# oracle: fails
# CSV over HTTPS
resolve("csv+https://example.invalid/x.csv")
# oracle: step
# oracle: fails
# a Parquet file that is missing
resolve("parquet:///nonexistent-oracle-directory/missing.parquet")
# oracle: step
# oracle: fails
# a session with TLS (owner item 7.24)
barrage_session(host, int(port), use_tls=True)
# oracle: step
# oracle: fails
# root certificates without TLS
barrage_session(host, int(port), tls_root_certs=b"pem")
# oracle: step
# oracle: fails
# a host Java cannot read
barrage_session("my_host", int(port))
# oracle: step
# oracle: fails
# a host with a space
barrage_session("a b")
# oracle: step
# oracle: fails
# a session whose connection is refused
barrage_session("127.0.0.1", 1)
# oracle: step
# oracle: fails
# a subscription to a missing variable
bs.subscribe(b"s/missing")
# oracle: step
# oracle: fails
# a snapshot of a missing variable
bs.snapshot(b"s/missing")
# oracle: step
# oracle: fails
# a snapshot by a closed session
closed.snapshot(b"s/types_t")
# oracle: step
# oracle: fails
# a snapshot by a session a with closed
scoped.snapshot(b"s/types_t")
# oracle: step
bs.close()
