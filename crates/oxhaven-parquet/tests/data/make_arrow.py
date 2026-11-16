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

# The types Deephaven reads as objects of java.math and java.time, with the values that
# are at an edge: before 1970, fractions of a second, the largest unsigned long, a
# decimal whose bytes are all ones (-0.01 here), an empty byte string, the byte -128.
D2 = decimal.Decimal
write({
    "Dec": pa.array([D2("1.50"), D2("-1.50"), D2("0.00"), None, D2("-0.01"), D2("12345.67")],
                    pa.decimal128(9, 2)),
    "Wide": pa.array([D2("1.0000000000"), D2("-12345678901234567890.0123456789"), D2("0E-10"), None,
                      D2("0.0000000001"), D2("9999999999999999999999999999.9999999999")],
                     pa.decimal128(38, 10)),
    "Whole": pa.array([D2("15"), D2("-15"), D2("0"), None, D2("-1"), D2("255")], pa.decimal128(5, 0)),
    "Date": pa.array([19724, -1, None, 0, -719529, 2932897], pa.date32()),
    "TimeMs": pa.array([3723000, 0, None, 86399999, 3720500, 3720000], pa.time32("ms")),
    "TimeUs": pa.array([3723000000, 0, None, 86399999999, 1, 3720000000], pa.time64("us")),
    "TimeNs": pa.array([3723000000000, 0, None, 86399999999999, 1, 3720000000000], pa.time64("ns")),
    "LocalMs": pa.array([0, -1, None, 1704157323000, 1704157323500, -86400000], pa.timestamp("ms")),
    "LocalUs": pa.array([0, -1, None, 1704157323000000, 1704157323000001, -86400000000],
                        pa.timestamp("us")),
    "LocalNs": pa.array([0, -1, None, 1704157323000000000, 1704157323000000001, -86400000000000],
                        pa.timestamp("ns")),
    "U64": pa.array([0, 1, None, 2**63, 2**64 - 1, 2**63 - 1], pa.uint64()),
    "Bin": pa.array([b"ab", b"", None, bytes([0x80, 0x00, 0xff]), "é".encode(), b"x"], pa.binary()),
    "Fixed": pa.array([b"ab", bytes([0x80, 0x7f]), None, b"cd", b"\x00\x00", b"zz"], pa.binary(2)),
}, "more_types.parquet", row_group_size=4)

# Decimals stored as integers, of 32 and of 64 bits.
write({
    "D32": pa.array([D2("1.50"), D2("-1.50"), D2("0.00"), None, D2("-0.01"), D2("-9999999.99")],
                    pa.decimal128(9, 2)),
    "D64": pa.array([D2("1.500"), D2("-1.500"), D2("0.000"), None, D2("-0.001"),
                     D2("-999999999999999.999")], pa.decimal128(18, 3)),
}, "more_decimals_int.parquet", store_decimal_as_integer=True)

# Structs with one field: where only one of the two levels can be left out, Deephaven
# reads the field; where both can, it cannot read the column.
inner = pa.field("a", pa.int32(), nullable=True)
required_struct = pa.field("Outer", pa.struct([inner]), nullable=False)
required_leaf = pa.field("Leaf", pa.struct([pa.field("a", pa.int32(), nullable=False)]), nullable=True)
both = pa.field("Both", pa.struct([pa.field("a", pa.int32(), nullable=False)]), nullable=False)
structs = pa.table(
    [
        pa.array([{"a": 1}, {"a": None}, {"a": 3}], required_struct.type),
        pa.array([{"a": 1}, None, {"a": 3}], required_leaf.type),
        pa.array([{"a": 1}, {"a": 2}, {"a": 3}], both.type),
    ],
    schema=pa.schema([required_struct, required_leaf, both]),
)
pq.write_table(structs, os.path.join(D, "more_structs.parquet"), compression="NONE",
               write_statistics=False)
