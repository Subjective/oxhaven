# Writes of the reference that are not ported: each step is one of them, and is refused
# with an error that names the Java method (docs/STORAGE.md, "Writing Parquet").
# oracle: results lzo, by_class, metadata, indexed, by_uri, nested, narrowed, partitioned
import os
import shutil
import tempfile

from deephaven import dtypes, empty_table, new_table
from deephaven.column import string_col
from deephaven.parquet import ColumnInstruction, read, write, write_partitioned

root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_write_unported")
shutil.rmtree(root, ignore_errors=True)


def source():
    """A new table for each write: an index written with a table stays with it."""
    return empty_table(10).update(["X = i", "S = `s` + (i % 3)", "K = (int)(i / 4)"])


def listing(directory):
    """The files under `directory`."""
    names = []
    for parent, _, files in os.walk(os.path.join(root, directory)):
        names += [os.path.relpath(os.path.join(parent, name), root) for name in files]
    return new_table([string_col("Name", sorted(names))])


# a codec this server reads and writes nothing with
write(source(), root + "/lzo.parquet", compression_codec_name="LZO")
lzo = read(root + "/lzo.parquet")
# oracle: step
# a codec by the name of its class
write(source(), root + "/by_class.parquet", compression_codec_name="org.apache.hadoop.io.compress.GzipCodec")
by_class = read(root + "/by_class.parquet")
# oracle: step
write(source(), root + "/metadata/table.parquet", generate_metadata_files=True)
metadata = listing("metadata")
# oracle: step
write(source(), root + "/indexed/table.parquet", index_columns=[["K"], ["S", "K"]])
indexed = listing("indexed")
# oracle: step
write(source(), "file://" + root + "/by_uri.parquet")
by_uri = read(root + "/by_uri.parquet")
# oracle: step
# a vector of vectors is written with a codec
write(source().group_by(["K", "S"]).group_by("K"), root + "/nested.parquet")
nested = listing("nested.parquet")
# oracle: step
# an int column written as shorts, which is an accident of the reference's casts
write(source(), root + "/narrowed.parquet", table_definition={"X": dtypes.short, "S": dtypes.string})
narrowed = read(root + "/narrowed.parquet")
# oracle: step
from deephaven.column import ColumnType, col_def

write_partitioned(source(), root + "/partitioned", table_definition=[
    col_def("K", dtypes.int32, column_type=ColumnType.PARTITIONING),
    col_def("X", dtypes.int32),
    col_def("S", dtypes.string),
], base_name="{partitions}-{i}")  # the default name is a random UUID, which no run repeats
partitioned = listing("partitioned")
