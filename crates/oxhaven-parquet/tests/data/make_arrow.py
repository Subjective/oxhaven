"""Writes the test files that come from pyarrow, not from Deephaven.

    oracle/.venv/bin/python crates/oxhaven-parquet/tests/data/make_arrow.py

They are the files of another writer: types Deephaven does not write itself, values that
are Java's nulls, several row groups, an Arrow schema in the footer, other codecs, and
directory layouts. `README.md` lists them.
"""
import datetime
import decimal
import os

import pyarrow as pa
import pyarrow.parquet as pq

D = os.path.dirname(os.path.abspath(__file__))


def write(columns, name, **options):
    path = os.path.join(D, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    options.setdefault("compression", "NONE")
    options.setdefault("write_statistics", False)
    pq.write_table(pa.table(columns), path, **options)


# Three row groups (3, 3 and 1 rows), with values equal to Java's null of the type in
# slots that are not null: 65535, -2147483648, the least long, -128.
write({
    "U8": pa.array([0, 255, None, 7, 8, 9, 10], pa.uint8()),
    "U16": pa.array([0, 65535, None, 7, 65534, 9, 10], pa.uint16()),
    "U32": pa.array([0, 4294967295, None, 7, 8, 9, 10], pa.uint32()),
    "I32": pa.array([-2147483648, 1, None, 2147483647, 8, 9, 10], pa.int32()),
    "I64": pa.array([-9223372036854775808, 1, None, 9223372036854775807, 8, 9, 10], pa.int64()),
    "I8": pa.array([-128, 127, None, 7, 8, 9, 10], pa.int8()),
    "Ms": pa.array([0, 1700000000123, None, -1, 8, 9, 10], pa.timestamp("ms", tz="UTC")),
    "Us": pa.array([0, 1700000000123456, None, -1, 8, 9, 10], pa.timestamp("us", tz="UTC")),
    "Ns": pa.array([0, 1700000000123456789, None, -1, 8, 9, 10], pa.timestamp("ns", tz="UTC")),
    "Str": pa.array(["a", "", None, "δ", "e", "f", "g"], pa.large_string()),
    "Dict": pa.array(["x", "y", None, "x", "y", "x", "z"], pa.string()).dictionary_encode(),
    "Flag": pa.array([True, False, None, True, False, True, False], pa.bool_()),
    "Ints": pa.array([[1, 2], [], None, [None, 3], [4], [5], [6]], pa.list_(pa.int32())),
}, "foreign.parquet", row_group_size=3)

# INT96 timestamps, as Spark and Impala write them.
write({"When": pa.array([0, 1700000000123456789, None], pa.timestamp("ns"))},
      "int96.parquet", use_deprecated_int96_timestamps=True)

# Unsigned 64-bit values on both sides of the largest long.
write({"V": pa.array([1, 2**63, 2**64 - 1, None, 2**63 - 1], pa.uint64())}, "uint64_big.parquet")

# One file per type that Java reads and that has no column type in Oxhaven yet, and two
# that Java refuses as well (a map and a list of lists).
for name, values in {
    "decimal": pa.array([decimal.Decimal("1.50"), None], pa.decimal128(10, 2)),
    "date": pa.array([datetime.date(2024, 1, 2), None], pa.date32()),
    "time": pa.array([datetime.time(1, 2, 3), None], pa.time64("us")),
    "local_timestamp": pa.array([0, None], pa.timestamp("us")),
    "uint64": pa.array([1, None], pa.uint64()),
    "binary": pa.array([b"ab", None], pa.binary()),
    "struct": pa.array([{"a": 1}, None], pa.struct([("a", pa.int32())])),
    "map": pa.array([[("k", 1)], None], pa.map_(pa.string(), pa.int32())),
    "nested_list": pa.array([[[1]], None], pa.list_(pa.list_(pa.int32()))),
}.items():
    write({"V": values}, f"no_{name}.parquet")

# Column names Deephaven changes to make them legal.
write({"a b": pa.array([1], pa.int32()), "2x": pa.array([2], pa.int32()), "ok": pa.array([3], pa.int32())},
      "names.parquet")

# ... and one that is a reserved word, which Deephaven reads as column_int.
write({"int": pa.array([1], pa.int32()), "ok": pa.array([2], pa.int32())}, "reserved.parquet")

# The codecs.
small = {"X": pa.array(list(range(20)), pa.int32()), "S": pa.array([f"s{i % 3}" for i in range(20)])}
for codec in ("SNAPPY", "ZSTD", "BROTLI", "GZIP", "LZ4"):
    write(small, f"codec_{codec.lower()}.parquet", compression=codec)

# A directory whose files do not have the same columns: the last one decides.
write({"X": pa.array([1, 2], pa.int32())}, "evolve/a.parquet")
write({"Y": pa.array(["p", "q"]), "X": pa.array([3, 4], pa.int32()), "Z": pa.array([0.5, 1.5])},
      "evolve/b.parquet")
# ... and one whose files disagree on the type of a column.
write({"X": pa.array([1.5, 2.5])}, "mismatch/a.parquet")
write({"X": pa.array([3, 4], pa.int32())}, "mismatch/b.parquet")

# Hidden files and directories, and a file that is not Parquet.
one = {"X": pa.array([1], pa.int32())}
write(one, "hidden/b.parquet")
write({"X": pa.array([2], pa.int32())}, "hidden/a.parquet")
write({"X": pa.array([3], pa.int32())}, "hidden/.c.parquet")
write({"X": pa.array([4], pa.int32())}, "hidden/.dir/d.parquet")
with open(os.path.join(D, "hidden", "notes.txt"), "w") as f:
    f.write("not parquet\n")

# Partition directories.
write(one, "kv/K=1/a.parquet")
write({"X": pa.array([2], pa.int32())}, "kv/K=2/a.parquet")

# A metadata file next to the data it describes.
os.makedirs(os.path.join(D, "meta"), exist_ok=True)
collected = []
pq.write_table(pa.table(one), os.path.join(D, "meta", "a.parquet"), compression="NONE",
               metadata_collector=collected)
collected[0].set_file_path("a.parquet")
pq.write_metadata(pa.table(one).schema, os.path.join(D, "meta", "_metadata"),
                  metadata_collector=collected)
