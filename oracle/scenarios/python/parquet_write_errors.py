# Writes that fail: the exception and its message, and what is left on disk. A write that
# fails leaves the destination as it was: the file that was there, no new file, and none
# of the directories it made. The exception is a destination given twice, which fails and
# is left as the last table.
# oracle: results kept, twice, left
import os
import shutil
import tempfile

from deephaven import dtypes, empty_table, new_table
from deephaven.column import string_col
from deephaven.parquet import (
    ColumnInstruction,
    RowGroupInfo,
    batch_write,
    delete,
    read,
    write,
    write_partitioned,
)

root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_write_errors")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root + "/directory.parquet")
with open(root + "/file", "w") as f:
    f.write("not a directory")

source = empty_table(10).update(["X = i", "S = `s` + (i % 3)", "K = (int)(i / 4)"])
write(source.head(2), root + "/kept.parquet")


def listing():
    """The files and directories under the root."""
    names = []
    for directory, directories, files in os.walk(root):
        names += [os.path.relpath(os.path.join(directory, name), root) for name in directories + files]
    return new_table([string_col("Name", sorted(names))])


# oracle: fails
write(source, root + "/no_extension")
# oracle: step
# oracle: fails
write(source, root + "/made/deep/no_extension.txt")
# oracle: step
# oracle: fails
write(empty_table(3), root + "/no_columns.parquet")
# oracle: step
# oracle: fails
write(source, root + "/codec.parquet", compression_codec_name="BROTLI")
# oracle: step
# oracle: fails
write(source, root + "/codec.parquet", compression_codec_name="bogus")
# oracle: step
# oracle: fails
write(source, root + "/codec.parquet", compression_codec_name="zstandard")
# oracle: step
# oracle: fails
write(source, root + "/codec.parquet", compression_codec_name="LZ4 RAW")
# oracle: step
# oracle: fails
write(source, root + "/instruction.parquet", col_instructions=[ColumnInstruction(column_name="X")])
# oracle: step
# oracle: fails
write(source, root + "/instruction.parquet", col_instructions=[ColumnInstruction(parquet_column_name="x")])
# oracle: step
# oracle: fails
write(source, root + "/instruction.parquet", col_instructions=[
    ColumnInstruction(column_name="X", parquet_column_name="a"),
    ColumnInstruction(column_name="X", parquet_column_name="b"),
])
# oracle: step
# oracle: fails
write(source, root + "/instruction.parquet", col_instructions=[
    ColumnInstruction(column_name="X", parquet_column_name="a"),
    ColumnInstruction(column_name="S", parquet_column_name="a"),
])
# oracle: step
# oracle: fails
write(source, root + "/instruction.parquet", col_instructions=[
    ColumnInstruction(column_name="X", parquet_column_name="S"),
])
# oracle: step
# a codec for objects, on a column of ints
# oracle: fails
write(source, root + "/instruction.parquet", col_instructions=[
    ColumnInstruction(column_name="X", parquet_column_name="X", codec_name="io.deephaven.util.codec.SerializableCodec"),
])
# oracle: step
# oracle: fails
write(source, root + "/limits.parquet", max_dictionary_keys=-2)
# oracle: step
# oracle: fails
write(source, root + "/limits.parquet", max_dictionary_size=-2)
# oracle: step
# oracle: fails
write(source, root + "/limits.parquet", target_page_size=100)
# oracle: step
# oracle: fails
RowGroupInfo.max_groups(0)
# oracle: step
# oracle: fails
RowGroupInfo.max_rows(0)
# oracle: step
# oracle: fails
RowGroupInfo.by_groups([])
# oracle: step
# oracle: fails
RowGroupInfo.by_groups(["K"], -1)
# oracle: step
# oracle: fails
write(source.head(0), root + "/groups.parquet", row_group_info=RowGroupInfo.max_rows(4))
# oracle: step
# oracle: fails
write(source, root + "/made/groups.parquet", row_group_info=RowGroupInfo.by_groups(["S"]))
# oracle: step
# oracle: fails
write(source, root + "/groups.parquet", row_group_info=RowGroupInfo.by_groups(["Nope"]))
# oracle: step
# oracle: fails
batch_write([source], [root + "/one.parquet", root + "/two.parquet"])
# oracle: step
# oracle: fails
batch_write([], [])
# oracle: step
# oracle: fails
batch_write([source, source.view("X")], [root + "/one.parquet", root + "/two.parquet"])
# oracle: step
# oracle: fails
batch_write([source, source.head(3)], [root + "/twice.parquet", root + "/twice.parquet"])
# oracle: step
# the second table fails, and the first file and its directories are taken away again
# oracle: fails
batch_write(
    [source, source],
    [root + "/made/one.parquet", root + "/made/two.parquet"],
    row_group_info=RowGroupInfo.by_groups(["S"]),
)
# oracle: step
# oracle: fails
batch_write([source, source], [root + "/made/one.parquet", root + "/directory.parquet"])
# oracle: step
# oracle: fails
batch_write([source, source], [root + "/kept.parquet", root + "/file/two.parquet"])
# oracle: step
# oracle: fails
write(source, root + "/directory.parquet")
# oracle: step
# oracle: fails
write(source, root + "/file/under.parquet")
# oracle: step
# oracle: fails
write_partitioned(source, root + "/partitioned")
# oracle: step
# oracle: fails
write(source, root + "/defined.parquet", table_definition={"X": dtypes.string})
# oracle: step
# oracle: fails
write(source, root + "/defined.parquet", table_definition={"S": dtypes.int32})
# oracle: step
# oracle: fails
write("source", root + "/argument.parquet")
# oracle: step
# oracle: fails
write(source, None)
# oracle: step
# oracle: fails
delete(None)
# oracle: step
kept = read(root + "/kept.parquet")
twice = read(root + "/twice.parquet")
left = listing()
