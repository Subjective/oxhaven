# A blink table of the remote (a table publisher's), subscribed and kept by
# blink_to_append_only, through two batches of rows published there; and its snapshot at
# rest, which has no rows. Oxhaven's server refuses a subscription to a blink table
# (BarrageMessageProducer, blink tables, not ported), so against an Oxhaven remote the
# subscription fails, for Java's client as for Oxhaven's; and Java's client cannot read
# what Oxhaven's server answers to a snapshot of it ("Missing body tag", tasks/13.3). The
# outcomes are kept in a table, so that the same script records against either remote.
# oracle: results s_blink, kept, outcome
# remote: from deephaven import dtypes, new_table
# remote: from deephaven.column import int_col, string_col
# remote: from deephaven.stream import blink_to_append_only
# remote: from deephaven.stream.table_publisher import table_publisher
# remote: rb_blink, rb_publisher = table_publisher("rb_blink", {"K": dtypes.int32, "S": dtypes.string})
# remote: rb_kept = blink_to_append_only(rb_blink)
import os

from deephaven import new_table
from deephaven.barrage import barrage_session
from deephaven.column import string_col
from deephaven.dherror import DHError
from deephaven.stream import blink_to_append_only

target = os.environ["ORACLE_REMOTE_TARGET"]
host, port = target.rsplit(":", 1)
bs = barrage_session(host, int(port))
try:
    s_blink = bs.snapshot(b"s/rb_blink")
    snapshot = "snapshot"
except DHError as error:
    snapshot = f"DHError {error._message}"
try:
    kept = blink_to_append_only(bs.subscribe(b"s/rb_blink"))
    subscription = "subscribed"
except DHError as error:
    subscription = f"DHError {error._message}"
outcome = new_table([string_col("Snapshot", [snapshot]), string_col("Subscribe", [subscription])])
# oracle: step
# remote: rb_publisher.add(new_table([int_col("K", [1, 2, 3]), string_col("S", ["a", None, "c"])]))
# remote-sync: kept = rb_kept
# oracle: step
# remote: rb_publisher.add(new_table([int_col("K", [4]), string_col("S", ["d"])]))
# remote-sync: kept = rb_kept
# oracle: step
bs.close()
