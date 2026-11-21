# Barrage sessions over TLS (`use_tls=True`; task 13.4) to the remote, through the TLS relay
# in front of it (bin/tls_front.py), whose certificate names the host of
# ORACLE_REMOTE_TLS_TARGET alone and is signed by an authority made for the run
# (ORACLE_REMOTE_TLS_ROOTS): a static table subscribed and snapshotted, and a view of a keyed
# input table followed through adds, a modify and a delete (a view, which does not carry the
# column metadata of an input table that a copy loses, as the baseline's line for the
# ticking scenario says). Then the refusals, each a step: the system's roots (Java's JDK
# trust store, OpenSSL's default paths), which do not hold the authority, for `dh://` and
# for a session without `tls_root_certs`; the roots of another authority, which replace the
# system's; the address of the remote, which the certificate does not name; and the
# plaintext port of the remote.
# oracle: results t_types, s_types, t_src
# remote: from deephaven import input_table, new_table, dtypes
# remote: from deephaven.column import int_col, string_col
# remote: tls_src = input_table(col_defs={"K": dtypes.int32, "S": dtypes.string}, key_cols="K")
# remote: tls_src.add(new_table([int_col("K", [1, 2, 3]), string_col("S", ["a", None, "c"])]))
# remote: tls_view = tls_src.view(["K", "S"])
# remote-sync: t_src = tls_view
import os

from deephaven.barrage import barrage_session
from deephaven.uri import resolve

plain_host, plain_port = os.environ["ORACLE_REMOTE_TARGET"].rsplit(":", 1)
tls_target = os.environ["ORACLE_REMOTE_TLS_TARGET"]
tls_host, tls_port = tls_target.rsplit(":", 1)
with open(os.environ["ORACLE_REMOTE_TLS_ROOTS"], "rb") as handle:
    roots = handle.read()
with open(os.environ["ORACLE_REMOTE_TLS_OTHER_ROOTS"], "rb") as handle:
    other_roots = handle.read()
readable = [
    "PerText = Per == null ? null : Per.toString()",
    "BytesLength = Bytes == null ? NULL_INT : Bytes.length",
    "Byte0 = Bytes == null ? NULL_BYTE : Bytes[0]",
    "Byte1 = Bytes == null ? NULL_BYTE : Bytes[1]",
    "ZdtText = Zdt == null ? null : Zdt.toString()",
]


def comparable(t):
    return t.update(readable).drop_columns(["Per", "Bytes"])


bs = barrage_session(tls_host, int(tls_port), use_tls=True, tls_root_certs=roots)
t_types = comparable(bs.subscribe(b"s/types_t"))
s_types = comparable(bs.snapshot(b"s/types_t"))
t_src = bs.subscribe(b"s/tls_view")
# oracle: step
# remote: tls_src.add(new_table([int_col("K", [2, 4]), string_col("S", ["b", "d"])]))
# remote-sync: t_src = tls_view
# oracle: step
# remote: tls_src.delete(new_table([int_col("K", [1])]))
# remote-sync: t_src = tls_view
# oracle: step
# oracle: fails
# dh:// trusts the system's roots, which do not hold the run's authority
resolve(f"dh://{tls_target}/scope/types_t")
# oracle: step
# oracle: fails
# so does a session without tls_root_certs
barrage_session(tls_host, int(tls_port), use_tls=True)
# oracle: step
# oracle: fails
# the roots of another authority replace the system's
barrage_session(tls_host, int(tls_port), use_tls=True, tls_root_certs=other_roots)
# oracle: step
# oracle: fails
# an address the certificate does not name
barrage_session(plain_host, int(tls_port), use_tls=True, tls_root_certs=roots)
# oracle: step
# oracle: fails
# TLS to the plaintext port
barrage_session(plain_host, int(plain_port), use_tls=True, tls_root_certs=roots)
# oracle: step
bs.close()
