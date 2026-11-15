# What Java reads and writes and Oxhaven refuses for now (docs/CSV.md): a date-time in a
# time zone whose offset changes, and a column of vectors.
# oracle: results regions, mixed, grouped_lines
import os
import shutil
import tempfile

root = os.path.join(tempfile.gettempdir(), "oxhaven_csv_not_ported")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root)


def file(name, text):
    path = os.path.join(root, name)
    with open(path, "w", newline="") as f:
        f.write(text)
    return path


from deephaven import new_table
from deephaven.column import string_col
from deephaven.csv import read, write

regions = read(file("regions.csv", (
    "Alias,Name,Lower\n"
    "2024-01-02T03:04:05 ET,2024-01-02T03:04:05 Asia/Tokyo,2024-07-02T03:04:05 america/new_york\n"
    "2024-07-02T03:04:05 JP,2024-07-02T03:04:05 Europe/London,2024-01-02T03:04:05 europe/paris\n"
)))
# oracle: step
# a zone that is not one makes the column a String column, whatever zones came before
mixed = read(file("mixed.csv", "A\n2024-01-02T03:04:05 ET\n2024-01-02T03:04:05 NY\n"))
# oracle: step
grouped = read(file("grouped.csv", "K,V\n1,10\n2,20\n1,30\n")).group_by("K")
write(grouped, os.path.join(root, "grouped_out.csv"))
with open(os.path.join(root, "grouped_out.csv"), newline="") as f:
    grouped_lines = new_table([string_col("Line", f.read().split("\n"))])
