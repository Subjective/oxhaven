# Reads that fail: the exception is DHError and its message says that reading failed.
# oracle: results good
import os
import shutil
import tempfile

root = os.path.join(tempfile.gettempdir(), "oxhaven_csv_read_errors")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root)


def file(name, text):
    path = os.path.join(root, name)
    with open(path, "w", newline="") as f:
        f.write(text)
    return path


from deephaven import dtypes
from deephaven.csv import read

rows = file("rows.csv", "A,B\n1,x\n2,y\n")
good = read(rows)
# oracle: fails
bad = read(root + "/nope.csv")
# oracle: step
# oracle: fails
bad = read(file("empty.csv", ""))
# oracle: step
# oracle: fails
bad = read(file("short.csv", "A,B,C\n1,2\n3,4,5\n"))
# oracle: step
# oracle: fails
bad = read(file("long.csv", "A,B\n1,2,3\n"))
# oracle: step
# oracle: fails
bad = read(file("open_quote.csv", "A,B\n\"x,2\n"))
# oracle: step
# oracle: fails
bad = read(file("after_quote.csv", "A,B\n\"x\"y,2\n"))
# oracle: step
# oracle: fails
bad = read(file("no_name.csv", "A,!!\n1,2\n"))
# oracle: step
# a named type takes no other cell, and the null value of a type is no cell of it
# oracle: fails
bad = read(rows, header={"N": dtypes.int32, "S": dtypes.int32})
# oracle: step
# oracle: fails
bad = read(file("byte.csv", "A\n127\n128\n"), header={"N": dtypes.byte})
# oracle: step
# oracle: fails
bad = read(file("null_int.csv", "A\n-2147483648\n"), header={"N": dtypes.int32})
# oracle: step
# oracle: fails
bad = read(rows, header={"N": dtypes.int32})
# oracle: step
# oracle: fails
bad = read(rows, header={"N": dtypes.int32, "S": dtypes.string, "More": dtypes.string})
# oracle: step
# oracle: fails
bad = read(rows, header={"N": dtypes.int32, "S": dtypes.int32_array})
# oracle: step
# a date-time with a month, a day or an hour that does not exist fails the read
# oracle: fails
bad = read(file("month.csv", "A\n2024-13-02T03:04:05Z\n"))
# oracle: step
# oracle: fails
bad = read(file("day.csv", "A\n2024-01-02T03:04:05Z\n2023-02-29T03:04:05Z\n"))
# oracle: step
# oracle: fails
bad = read(file("no_header.csv", "A,,B\n1,2,3\n"))
# oracle: step
# oracle: fails
bad = read(rows, skip_rows=-1)
# oracle: step
# oracle: fails
bad = read(rows, num_rows=-1)
# oracle: step
# oracle: fails
bad = read(rows, header_row=5)
# oracle: step
# oracle: fails
bad = read(rows, header_row=1, headless=True)
# oracle: step
# oracle: fails
bad = read(rows, delimiter="é")
# oracle: step
# oracle: fails
bad = read(rows, delimiter="ab")
# oracle: step
# oracle: fails
bad = read(rows, quote="")
# oracle: step
# a row that is passed over is still checked
# oracle: fails
bad = read(file("skipped.csv", "A,B\n1,2,3,4\n5,6\n"), skip_rows=1)
