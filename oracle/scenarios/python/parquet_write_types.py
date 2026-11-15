# Tables written as Parquet files and read back: every column type with a null in each
# column, a NaN and an empty string; vectors, from group_by; arrays; a table without rows.
# `columns` describes the files themselves, as pyarrow reads their footers: the Parquet
# schema, the `deephaven` entry, the row groups, the codec and whether a column chunk
# has a dictionary. It is what makes a file one that Java reads back as the same table.
# oracle: results types, vectors, arrays, no_rows, filtered, columns
import os
import shutil
import tempfile

import pyarrow.parquet as pq

from deephaven import dtypes, empty_table, new_table
from deephaven.column import InputColumn, int_col, string_col
from deephaven.parquet import read, write
from deephaven.time import to_j_instant

root = os.path.join(tempfile.gettempdir(), "oxhaven_parquet_write_types")
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
                column.max_definition_level,
                column.max_repetition_level,
                chunk.compression if chunk else None,
                ("yes" if "RLE_DICTIONARY" in chunk.encodings else "no") if chunk else None,
                ",".join(sorted(key.decode() for key in entries)),
                entries.get(b"deephaven", b"").decode(),
                footer.num_rows,
                groups,
            ))
    return new_table([
        string_col("File", [row[0] for row in rows]),
        string_col("Path", [row[1] for row in rows]),
        string_col("Physical", [row[2] for row in rows]),
        string_col("Logical", [row[3] for row in rows]),
        int_col("MaxDefinition", [row[4] for row in rows]),
        int_col("MaxRepetition", [row[5] for row in rows]),
        string_col("Codec", [row[6] for row in rows]),
        string_col("Dictionary", [row[7] for row in rows]),
        string_col("Keys", [row[8] for row in rows]),
        string_col("Deephaven", [row[9] for row in rows]),
        int_col("Rows", [row[10] for row in rows]),
        string_col("RowGroups", [row[11] for row in rows]),
    ])


source = empty_table(7).update([
    "B = i % 3 == 0 ? null : (Boolean)(i % 2 == 0)",
    "Y = i == 1 ? null : (byte)(i - 3)",
    "H = i == 2 ? null : (short)(i * 1000 - 3000)",
    "I = i == 3 ? null : i * 100000 - 300000",
    "L = i == 4 ? null : (long)i * 5000000000L - 15000000000L",
    "C = i == 5 ? null : (char)('a' + i)",
    "F = i == 6 ? null : (float)(i * 0.5 - 1)",
    "D = i == 0 ? null : i == 3 ? Double.NaN : i * 0.25 - 0.5",
    "S = i == 1 ? null : i == 2 ? `` : `sé` + i",
    "T = i == 2 ? null : '2024-01-01T00:00:00Z' + i * 1000000007L",
])
write(source, root + "/types.parquet")
# The harness has no comparison for a char column, so C is compared as its code.
COLUMNS = ["B", "Y", "H", "I", "L", "Code = (int)C", "F", "D", "S", "T"]
types = read(root + "/types.parquet").view(COLUMNS)
filtered = read(root + "/types.parquet").where("I > 0").view(COLUMNS)

# a vector of each type; the vector of chars is written and described, and not compared
grouped = source.update_view("K = i % 2").group_by("K")
write(grouped, root + "/vectors.parquet")
vectors = read(root + "/vectors.parquet").drop_columns("C")

instant = to_j_instant("2024-01-01T00:00:00Z")
lists = new_table([
    InputColumn("AI", dtypes.int32_array, input_data=[[0, None], [1, None], None, [], [4]]),
    InputColumn("AS", dtypes.string_array, input_data=[["x", None], None, [], [""], ["y"]]),
    InputColumn("AY", dtypes.byte_array, input_data=[[1, None], None, [], [-3], [4]]),
    InputColumn("AD", dtypes.double_array, input_data=[[1.5, None, float("nan")], None, [], [0.0], [-0.0]]),
    InputColumn("AL", dtypes.long_array, input_data=[[2**40, None], None, [], [1], [2]]),
    InputColumn("AH", dtypes.short_array, input_data=[[300, None], None, [], [1], [2]]),
    InputColumn("AF", dtypes.float32_array, input_data=[[0.5, None], None, [], [1], [2]]),
    InputColumn("AB", dtypes.boolean_array, input_data=[[True, None, False], None, [], [True], [False]]),
    InputColumn("AT", dtypes.instant_array, input_data=[[instant, None], None, [], [instant], []]),
    InputColumn("AC", dtypes.char_array, input_data=[[97, None], None, [], [98], [99]]),
])
write(lists, root + "/arrays.parquet")
# a byte[] column is written and described too; the server does not send one yet
arrays = read(root + "/arrays.parquet").drop_columns(["AC", "AY"])

write(source.head(0), root + "/no_rows.parquet")
no_rows = read(root + "/no_rows.parquet").view(COLUMNS)

columns = describe(["types.parquet", "vectors.parquet", "arrays.parquet", "no_rows.parquet"])
