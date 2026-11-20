# The remote's static scope tables of every column type it can make (types_t), of vectors
# (vectors_t, a group_by) and without rows (empty_t), read as a URI (the resolver's
# subscription), subscribed and snapshotted through a Barrage session, and snapshotted
# through a session that a `with` closes; then the session of the subscription closes.
#
# Two types are compared through formulas, because the rules cannot read them: a Period
# (an Arrow month interval) as its text, and a byte[] (Arrow binary) as its length and first
# two bytes. A ZonedDateTime is compared as it arrives and as its text, which names its zone.
# oracle: results r_types, b_types, s_types, w_types, r_vectors, s_vectors, r_empty, s_empty
import os

from deephaven.barrage import barrage_session
from deephaven.uri import resolve

target = os.environ["ORACLE_REMOTE_TARGET"]
host, port = target.rsplit(":", 1)
readable = [
    "PerText = Per == null ? null : Per.toString()",
    "BytesLength = Bytes == null ? NULL_INT : Bytes.length",
    "Byte0 = Bytes == null ? NULL_BYTE : Bytes[0]",
    "Byte1 = Bytes == null ? NULL_BYTE : Bytes[1]",
    "ZdtText = Zdt == null ? null : Zdt.toString()",
]


def comparable(t):
    return t.update(readable).drop_columns(["Per", "Bytes"])


r_types = comparable(resolve(f"dh+plain://{target}/scope/types_t"))
r_vectors = resolve(f"dh+plain://{target}/scope/vectors_t")
bs = barrage_session(host, int(port))
b_types = comparable(bs.subscribe(b"s/types_t"))
s_types = comparable(bs.snapshot(b"s/types_t"))
s_vectors = bs.snapshot(b"s/vectors_t")
r_empty = comparable(resolve(f"dh+plain://{target}/scope/empty_t"))
s_empty = comparable(bs.snapshot(b"s/empty_t"))
with barrage_session(host, int(port)) as scoped:
    w_types = comparable(scoped.snapshot(b"s/types_t"))
# oracle: step
bs.close()
