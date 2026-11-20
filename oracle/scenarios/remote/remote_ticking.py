# Ticking tables of the remote, followed through steps of changes made there: a keyed input
# table (adds, modifies by key, removes), its sort by value (whose rows shift as values
# change), its columns of every type, and an append-only input table. Each is read as a URI
# and some through a Barrage session; on the copies, operations of the local server: where,
# update, agg_by, sort, and natural_join with a local table. A snapshot keeps the rows of
# step 0. Then the session closes: its tables fail, and the resolver's go on ticking.
# oracle: results r_src, r_sorted, r_log, r_types, b_src, b_sorted, b_log, snap, w, u, a, srt, nj, b_agg
# remote: from deephaven import input_table, new_table, dtypes
# remote: from deephaven.column import int_col, string_col, double_col
# remote: rt_src = input_table(col_defs={"K": dtypes.int32, "S": dtypes.string, "V": dtypes.double}, key_cols="K")
# remote: rt_sorted = rt_src.sort(["V", "K"])
# remote: rt_types = rt_src.update(["N = K"]).update(oracle_types)
# remote: rt_log = input_table(col_defs={"K": dtypes.int32, "S": dtypes.string})
# remote: rt_src.add(new_table([int_col("K", [1, 2, 3, 4, 5, 6]), string_col("S", ["a", None, "c", "d", "e", "f"]), double_col("V", [6.5, 2.0, 4.25, 1.0, 3.5, 5.0])]))
# remote: rt_log.add(new_table([int_col("K", [1, 2]), string_col("S", ["one", None])]))
# remote-sync: r_src = rt_src, r_sorted = rt_sorted, r_log = rt_log, r_types_raw = rt_types, b_src = rt_src, b_sorted = rt_sorted, b_log = rt_log
import os

from deephaven import agg, new_table
from deephaven.barrage import barrage_session
from deephaven.column import int_col, string_col
from deephaven.uri import resolve

target = os.environ["ORACLE_REMOTE_TARGET"]
host, port = target.rsplit(":", 1)
base = f"dh+plain://{target}/scope/"
r_src = resolve(base + "rt_src")
r_sorted = resolve(base + "rt_sorted")
r_log = resolve(base + "rt_log")
r_types_raw = resolve(base + "rt_types")
r_types = r_types_raw.update([
    "PerText = Per == null ? null : Per.toString()",
    "BytesLength = Bytes == null ? NULL_INT : Bytes.length",
    "Byte0 = Bytes == null ? NULL_BYTE : Bytes[0]",
    "ZdtText = Zdt == null ? null : Zdt.toString()",
]).drop_columns(["Per", "Bytes"])
bs = barrage_session(host, int(port))
b_src = bs.subscribe(b"s/rt_src")
b_sorted = bs.subscribe("s/rt_sorted")
b_log = bs.subscribe(b"s/rt_log")
snap = bs.snapshot(b"s/rt_src")
names = new_table([int_col("G", [0, 1, 2]), string_col("Name", ["zero", "one", "two"])])
w = r_src.where("V > 3.0")
u = r_src.update(["W = V * 2", "T = S == null ? `none` : S + `!`"])
a = r_src.update("G = K % 3").agg_by([agg.sum_("Sum = V"), agg.count_("N"), agg.last("LastS = S")], "G")
srt = r_src.sort_descending(["V", "K"])
nj = r_src.update("G = K % 3").natural_join(names, on="G", joins="Name")
b_agg = b_sorted.agg_by([agg.max_("Max = V"), agg.min_("Min = K"), agg.count_("N")])
# oracle: step
# remote: rt_src.add(new_table([int_col("K", [7, 8]), string_col("S", ["g", "h"]), double_col("V", [0.5, 9.0])]))
# remote: rt_log.add(new_table([int_col("K", [3]), string_col("S", ["three"])]))
# remote-sync: r_src = rt_src, r_sorted = rt_sorted, r_log = rt_log, r_types_raw = rt_types, b_src = rt_src, b_sorted = rt_sorted, b_log = rt_log
# oracle: step
# remote: rt_src.add(new_table([int_col("K", [2, 5, 7]), string_col("S", ["B", None, "G"]), double_col("V", [8.0, 0.25, 3.75])]))
# remote-sync: r_src = rt_src, r_sorted = rt_sorted, r_types_raw = rt_types, b_src = rt_src, b_sorted = rt_sorted
# oracle: step
# remote: rt_src.delete(new_table([int_col("K", [1, 4])]))
# remote: rt_log.add(new_table([int_col("K", [4, 5]), string_col("S", [None, "five"])]))
# remote-sync: r_src = rt_src, r_sorted = rt_sorted, r_log = rt_log, r_types_raw = rt_types, b_src = rt_src, b_sorted = rt_sorted, b_log = rt_log
# oracle: step
# remote: rt_src.add(new_table([int_col("K", [9, 10, 3]), string_col("S", ["i", "j", "C"]), double_col("V", [4.5, 1.5, 7.0])]))
# remote: rt_src.delete(new_table([int_col("K", [6])]))
# remote-sync: r_src = rt_src, r_sorted = rt_sorted, r_types_raw = rt_types, b_src = rt_src, b_sorted = rt_sorted
# oracle: step
bs.close()
# oracle: await b_src fails, b_sorted fails, b_log fails
# oracle: step
# remote: rt_src.add(new_table([int_col("K", [11]), string_col("S", ["k"]), double_col("V", [2.5])]))
# remote-sync: r_src = rt_src, r_sorted = rt_sorted, r_types_raw = rt_types
