# The arguments of a write: compression codecs, row groups, column names in the file,
# dictionary limits, a table definition, several tables at once, a sorted table, a file
# that is replaced, delete, and in the second step a table that changes. Each file is read back, and `files`
# describes the files themselves, as pyarrow reads their footers.
# oracle: results codecs, groups, renamed, defined, first, second, replaced, ticking, files, left
import os
import shutil
import tempfile

import pyarrow.parquet as pq

from deephaven import dtypes, empty_table, merge, new_table, time_table
from deephaven.column import int_col, string_col
from deephaven.parquet import (
    ColumnInstruction,
    RowGroupInfo,
    batch_write,
    delete,
    read,
    write,
)

root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_write_options")
shutil.rmtree(root, ignore_errors=True)


def describe(names):
    """One row for each column of each of the files `names`."""
    rows = []
    for name in names:
        file = pq.ParquetFile(os.path.join(root, name))
        footer = file.metadata
        entries = footer.metadata or {}
        groups = ",".join(str(footer.row_group(g).num_rows) for g in range(footer.num_row_groups))
        for index in range(len(file.schema)):
            column = file.schema.column(index)
            chunk = footer.row_group(0).column(index) if footer.num_row_groups else None
            rows.append((
                name,
                column.path,
                column.physical_type,
                str(column.logical_type),
                chunk.compression if chunk else None,
                ("yes" if "RLE_DICTIONARY" in chunk.encodings else "no") if chunk else None,
                entries.get(b"deephaven", b"").decode(),
                footer.num_rows,
                groups,
            ))
    return new_table([
        string_col("File", [row[0] for row in rows]),
        string_col("Path", [row[1] for row in rows]),
        string_col("Physical", [row[2] for row in rows]),
        string_col("Logical", [row[3] for row in rows]),
        string_col("Codec", [row[4] for row in rows]),
        string_col("Dictionary", [row[5] for row in rows]),
        string_col("Deephaven", [row[6] for row in rows]),
        int_col("Rows", [row[7] for row in rows]),
        string_col("RowGroups", [row[8] for row in rows]),
    ])


written = []


def put(table, name, **arguments):
    """Writes `table` to the file `name` and notes the file for `files`."""
    write(table, os.path.join(root, name), **arguments)
    written.append(name)
    return os.path.join(root, name)


source = empty_table(10).update([
    "X = i", "S = `s` + (i % 3)", "G = i / 4", "K = (int)(i / 4)", "P = (int)(i / 2)",
    "T = '2024-01-01T00:00:00Z' + i * HOUR",
])

# every codec name the reference takes and this server writes: a name is the simple name
# of a Hadoop codec class in any case, with or without "Codec"; "" is the default
CODECS = [
    "UNCOMPRESSED", "SNAPPY", "GZIP", "LZ4", "LZ4_RAW", "ZSTD", "", "snappy", "Gzip",
    "jdkgzip", "zstdcodec", "SnappyCodec", "lz4raw", "lz4_raw", "uncompressed",
]
codecs = merge([
    read(put(source, f"codec{index}.parquet", compression_codec_name=codec)).update_view(f"Codec = {index}")
    for index, codec in enumerate(CODECS)
])

GROUPS = [
    RowGroupInfo.single_group(),
    RowGroupInfo.max_groups(1),
    RowGroupInfo.max_groups(3),
    RowGroupInfo.max_groups(20),
    RowGroupInfo.max_rows(4),
    RowGroupInfo.max_rows(10),
    RowGroupInfo.max_rows(100),
    RowGroupInfo.by_groups(["K"]),
    RowGroupInfo.by_groups(["K"], 3),
    RowGroupInfo.by_groups(["K", "P"]),
    RowGroupInfo.by_groups(["G"], 0),
    RowGroupInfo.by_groups(["T"], 1),
]
groups = merge([
    read(put(source, f"groups{index}.parquet", row_group_info=info)).update_view(f"Groups = {index}")
    for index, info in enumerate(GROUPS)
])
# a table without rows has no row group, whatever the instructions
put(source.head(0), "groups_none.parquet", row_group_info=RowGroupInfo.max_groups(4))
put(source.head(0), "groups_by_none.parquet", row_group_info=RowGroupInfo.by_groups(["K"]))
# the values of a group need only be next to each other: nulls, NaNs and both zeros
keys = empty_table(8).update(
    "D = i < 2 ? NULL_DOUBLE : i < 4 ? Double.NaN : i == 4 ? 0.0 : i == 5 ? -0.0 : (double)i"
)
put(keys, "groups_keys.parquet", row_group_info=RowGroupInfo.by_groups(["D"]))

# names in the file; an instruction for a column the table does not have is ignored
put(source, "renamed.parquet", col_instructions=[
    ColumnInstruction(column_name="X", parquet_column_name="ex"),
    ColumnInstruction(column_name="S", parquet_column_name="Es"),
    ColumnInstruction(column_name="Nope", parquet_column_name="nope"),
])
renamed = read(root + "/renamed.parquet")
# a vector under another name: the `deephaven` entry names the column of the file, and
# the first column the table is sorted by under its own name
sorted_vectors = source.view(["S", "V = X"]).group_by("S").sort_descending("S")
put(sorted_vectors, "sorted_vectors.parquet", col_instructions=[
    ColumnInstruction(column_name="V", parquet_column_name="vee"),
    ColumnInstruction(column_name="S", parquet_column_name="ess"),
])
put(source.sort("X"), "sorted.parquet")
put(source.sort(["S", "X"]), "sorted_twice.parquet")
put(source.sort("X").where("X > 2"), "sorted_filtered.parquet")
put(source.sort("X").update("Y = 1"), "sorted_updated.parquet")

# a string column has a dictionary unless it has more distinct values, or more bytes of
# them, than the limits; `use_dictionary` lifts the limits for a column
put(source, "keys2.parquet", max_dictionary_keys=2)
put(source, "keys3.parquet", max_dictionary_keys=3)
put(source, "size5.parquet", max_dictionary_size=5)
put(source, "size6.parquet", max_dictionary_size=6)
put(source, "keys0.parquet", max_dictionary_keys=0)
put(source, "hint.parquet", max_dictionary_keys=0, col_instructions=[
    ColumnInstruction(column_name="S", parquet_column_name="S", use_dictionary=True),
    ColumnInstruction(column_name="X", parquet_column_name="X", use_dictionary=True),
])
put(source.update("N = (String)null"), "null_strings.parquet")
put(source.view(["S", "V = S"]).group_by("S"), "string_vectors.parquet", max_dictionary_keys=1)
put(source, "pages.parquet", target_page_size=2048)

# a definition leaves columns out, orders them, and adds columns of nulls
put(source, "defined.parquet", table_definition={
    "S": dtypes.string, "N": dtypes.int64, "X": dtypes.int32, "A": dtypes.double_array,
})
defined = read(root + "/defined.parquet")
put(source, "defined_by_table.parquet", table_definition=source.view(["G", "X"]).definition)

# several tables, with the same columns or with a definition; directories are made
batch_write([source, source.head(3)], [root + "/first.parquet", root + "/a/b/second.parquet"])
batch_write(
    (source.view("X"), source.view("S")),
    (root + "/third.parquet", root + "/a/fourth.parquet"),
    table_definition={"X": dtypes.int32, "S": dtypes.string},
    compression_codec_name="GZIP",
)
written += ["first.parquet", "a/b/second.parquet", "third.parquet", "a/fourth.parquet"]
first = read(root + "/first.parquet")
second = read(root + "/a/b/second.parquet")

# a file that is there is replaced
put(source, "replaced.parquet")
put(source.tail(2).view(["S", "X"]), "replaced.parquet")
written.pop()
replaced = read(root + "/replaced.parquet")

files = describe(written)

# delete removes a file, a directory with everything in it, and nothing at all
write(source, root + "/gone/deep/down.parquet")
delete(root + "/third.parquet")
delete(root + "/gone")
delete(root + "/nothing.parquet")
left = new_table([string_col("Name", sorted(
    os.path.relpath(os.path.join(directory, name), root)
    for directory, _, names in os.walk(root)
    for name in names
    if not name.startswith(("codec", "groups", "keys", "size", "sorted"))
))])

# a table that changes is written as it is when it is written: five rows after a cycle
ticking = time_table("PT1H", start_time="2024-01-01T00:00:00Z").head(5).update_view("X = ii")
# oracle: step
put(ticking, "ticking.parquet")
ticking = read(root + "/ticking.parquet")
