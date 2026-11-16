# Partitioned directories written with metadata files, read back through them: the
# partitioning columns are known only from `_common_metadata`, and their values from the
# paths `_metadata` lists. Read by inference from the directory and from the path of
# either metadata file, and with the explicit METADATA_PARTITIONED layout; then with only
# one of the two metadata files, with files the metadata does not list, with a listed
# file that is gone, and from directories pyarrow wrote whose partitions Java reads by
# position, puts after the files' columns, or refuses. The directories are written in
# the first step, the reads that fail come next, and the tables are read in the last.
# oracle: results inferred, by_layout, by_metadata_file, by_common_file, renamed, typed, metadata_only, common_only, common_only_layout, extra, missing, refreshing, positional, last, levels, twice, unknown, unparsed
import datetime
import decimal
import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from deephaven import dtypes, empty_table
from deephaven.column import ColumnType, col_def
from deephaven.parquet import ColumnInstruction, ParquetFileLayout, read, write_partitioned

root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_read_common_metadata")
shutil.rmtree(root, ignore_errors=True)


def two_keys(directory):
    """Twelve rows under A=<int>/B=<string or null>, with metadata files."""
    source = empty_table(12).update([
        "A = (int)(i % 3)", "B = i % 4 == 3 ? null : (i % 2 == 0 ? `x` : `y`)", "X = i", "S = `s` + i"])
    write_partitioned(source, os.path.join(root, directory), table_definition=[
        col_def("A", dtypes.int32, column_type=ColumnType.PARTITIONING),
        col_def("B", dtypes.string, column_type=ColumnType.PARTITIONING),
        col_def("X", dtypes.int32),
        col_def("S", dtypes.string),
    ], base_name="{partitions}-{i}", generate_metadata_files=True)  # no random UUID
    return os.path.join(root, directory)


def by_pyarrow(directory, files, common):
    """Files of the column X, each at a path relative to `directory`, with metadata files."""
    collected = []
    for relative, values in files:
        path = os.path.join(root, directory, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pq.write_table(pa.table({"X": pa.array(values, pa.int32())}), path, metadata_collector=collected)
        collected[-1].set_file_path(relative)
    schema = pa.schema([("X", pa.int32())])
    pq.write_metadata(schema, os.path.join(root, directory, "_metadata"), metadata_collector=collected)
    pq.write_metadata(pa.schema(common), os.path.join(root, directory, "_common_metadata"))
    return os.path.join(root, directory)


two = two_keys("two")
# a key of each type the writer takes, from a file pyarrow writes: a formula cannot make
# the object types on every server
pq.write_table(pa.table({
    "Bo": pa.array([True, False, True, False]),
    "C": pa.array([97, 98, 97, 98], pa.uint16()),
    "By": pa.array([-1, 0, -1, 0], pa.int8()),
    "Sh": pa.array([300, 301, 300, 301], pa.int16()),
    "L": pa.array([0, 10000000000, 0, 10000000000], pa.int64()),
    "BI": pa.array([decimal.Decimal(-5), decimal.Decimal(4)] * 2, pa.decimal128(1, 0)),
    "BD": pa.array([decimal.Decimal("0.0"), decimal.Decimal("2.5")] * 2, pa.decimal128(2, 1)),
    "D": pa.array([datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)] * 2),
    "T": pa.array([datetime.time(10, 0, 30), datetime.time(10, 1, 30)] * 2, pa.time64("ns")),
    "X": pa.array([0, 1, 2, 3], pa.int32()),
}), root + "/types.parquet")
source = read(root + "/types.parquet").partition_by(["Bo", "C", "By", "Sh", "L", "BI", "BD", "D", "T"])
write_partitioned(source, root + "/types", base_name="data", generate_metadata_files=True)
# without `_common_metadata` there are no partitioning columns
os.remove(two_keys("metadata_only") + "/_common_metadata")
# without `_metadata` the directories are read by their layout
os.remove(two_keys("common_only") + "/_metadata")
# files `_metadata` does not list are not read
extra_files = two_keys("extra")
shutil.copy(extra_files + "/A=0/B=x/A=0_B=x-0.parquet", extra_files + "/A=0/B=x/other.parquet")
shutil.copytree(extra_files + "/A=1/B=y", extra_files + "/A=7/B=z")
# a listed file that is gone has no rows
os.remove(two_keys("missing") + "/A=1/B=y/A=1_B=y-0.parquet")
# directories without `=` hold the values of the partitioning columns in order
by_pyarrow("positional", [("a/1/f.parquet", [1]), ("b/2/f.parquet", [2, 3])],
           [("S", pa.string()), ("N", pa.int32()), ("X", pa.int32())])
# the columns come in the order of `_common_metadata`
by_pyarrow("last", [("K=2/f.parquet", [1]), ("K=1/f.parquet", [2])], [("X", pa.int32()), ("K", pa.int32())])
# what Java refuses: too few directories, a key twice, a key that is no column, a value
# that is not of its type
by_pyarrow("levels", [("K=1/f.parquet", [1])], [("K", pa.int32()), ("L", pa.int32()), ("X", pa.int32())])
by_pyarrow("twice", [("K=1/K=2/f.parquet", [1])], [("K", pa.int32()), ("L", pa.int32()), ("X", pa.int32())])
by_pyarrow("unknown", [("Z=1/f.parquet", [1])], [("K", pa.int32()), ("X", pa.int32())])
by_pyarrow("unparsed", [("K=abc/f.parquet", [1])], [("K", pa.int32()), ("X", pa.int32())])
# oracle: step
# oracle: fails
# the metadata layout needs `_metadata`
common_only_layout = read(root + "/common_only", file_layout=ParquetFileLayout.METADATA_PARTITIONED)
# oracle: step
# oracle: fails
refreshing = read(two, is_refreshing=True)
# oracle: step
# oracle: fails
levels = read(root + "/levels")
# oracle: step
# oracle: fails
twice = read(root + "/twice")
# oracle: step
# oracle: fails
unknown = read(root + "/unknown")
# oracle: step
# oracle: fails
unparsed = read(root + "/unparsed")
# oracle: step
inferred = read(two)
by_layout = read(two, file_layout=ParquetFileLayout.METADATA_PARTITIONED)
by_metadata_file = read(two + "/_metadata")
by_common_file = read(two + "/_common_metadata", file_layout=ParquetFileLayout.METADATA_PARTITIONED)
# an instruction names a key as it names a column
renamed = read(two, col_instructions=[ColumnInstruction(column_name="Q", parquet_column_name="A")])
# the object types are compared as text
typed = read(root + "/types").update_view([
    "CI = (int)C", "BIT = BI.toString()", "BDT = BD.toString()", "DT = D.toString()", "TT = T.toString()",
]).drop_columns(["C", "BI", "BD", "D", "T"])
metadata_only = read(root + "/metadata_only")
common_only = read(root + "/common_only")
extra = read(extra_files)
missing = read(root + "/missing")
positional = read(root + "/positional")
last = read(root + "/last")
