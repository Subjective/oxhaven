"""Writes the test files that come from the Java reference, with its own
`deephaven.parquet.write`, so that they carry what Deephaven puts in a file: the
`deephaven` footer entry, its choice of Parquet types, its encodings.

    oracle/bin/start-java.sh 10000
    oracle/.venv/bin/python crates/oxhaven-parquet/tests/data/make_java.py 10000

`README.md` lists the files.
"""
import os
import sys

from pydeephaven import Session

D = os.path.dirname(os.path.abspath(__file__))

SCRIPT = f"""
import os
from deephaven import empty_table
from deephaven.parquet import write

D = {D!r}
os.makedirs(D + "/flat", exist_ok=True)

# Every column type Oxhaven has, with a null in each column, a NaN and an empty string.
types = empty_table(7).update([
    "B = i % 3 == 0 ? null : (Boolean)(i % 2 == 0)",
    "Y = i == 1 ? null : (byte)(i - 3)",
    "H = i == 2 ? null : (short)(i * 1000 - 3000)",
    "I = i == 3 ? null : i * 100000 - 300000",
    "L = i == 4 ? null : (long)i * 10000000000L - 30000000000L",
    "C = i == 5 ? null : (char)('a' + i)",
    "F = i == 6 ? null : (float)(i * 0.5f - 1)",
    "D = i == 0 ? null : (i == 1 ? Double.NaN : i * 0.25 - 1)",
    "S = i == 2 ? null : (i == 3 ? `` : `s` + i)",
    "T = i == 4 ? null : '2024-01-01T00:00:00Z' + i * 1000000001L",
])
write(types, D + "/types.parquet", compression_codec_name="UNCOMPRESSED")
write(types, D + "/types_lz4.parquet", compression_codec_name="LZ4_RAW")
write(types, D + "/types_gzip.parquet", compression_codec_name="GZIP")
# Java's default codec
write(types, D + "/types_snappy.parquet")

# Vectors (from group_by) and arrays.
lists = (
    empty_table(5)
    .update(["K = i % 2", "V = i", "W = i * 0.5", "S = `s` + i"])
    .group_by("K")
    .update(["A = new int[] {{(int)K, 2}}", "E = new String[] {{`x`, null}}"])
)
write(lists, D + "/lists.parquet", compression_codec_name="UNCOMPRESSED")

# A directory of files, one of them without rows.
for n in range(3):
    part = empty_table(3 + n).update([f"P = {{n}}", "X = i", f"S = `f{{n}}_` + i"])
    write(part, D + f"/flat/part{{n}}.parquet", compression_codec_name="UNCOMPRESSED")
empty = empty_table(0).update(["P = 9", "X = i", "S = `e`"])
write(empty, D + "/flat/empty.parquet", compression_codec_name="UNCOMPRESSED")
"""

session = Session(
    host="127.0.0.1",
    port=int(sys.argv[1]),
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token=os.environ.get("ORACLE_PSK", "oxhaven"),
)
session.run_script(SCRIPT)
session.close()
