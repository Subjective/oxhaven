# The arguments of a CSV read: which rows are headers and which are data, what separates
# and quotes the cells, what happens to blanks, to short and long rows and to empty lines,
# and the column names Java makes of headers that are not legal names.
# oracle: results headless, header_row, renamed, skipped, counted, beyond, none, pipes, tabs, single_quotes, blanks, kept_blanks, trimmed, quoted, line_ends, short_rows, long_rows, empty_lines, no_empty_lines, names, repeated, bom, no_columns, only_headers
import os
import shutil
import tempfile

root = os.path.join(tempfile.gettempdir(), "oxhaven_csv_read_options")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root)


def file(name, text):
    path = os.path.join(root, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        f.write(text)
    return path


from deephaven import dtypes
from deephaven.csv import read

rows = file("rows.csv", "A,B\n1,x\n2,y\n3,z\n4,w\n")
headless = read(rows, headless=True)
header_row = read(file("header_row.csv", "junk\nmore junk,x,y\nA,B\n1,2\n"), header_row=2)
renamed = read(rows, header={"N": dtypes.double, "S": dtypes.string})
# oracle: step
skipped = read(rows, skip_rows=2)
counted = read(rows, skip_rows=1, num_rows=2)
beyond = read(rows, skip_rows=10)
none = read(rows, num_rows=0)
# oracle: step
pipes = read(file("pipes.csv", "A|B\n1|2,3\n"), delimiter="|")
tabs = read(file("tabs.csv", "A\tB\n1\t x \n"), delimiter="\t")
single_quotes = read(file("single_quotes.csv", "A,B\n'x,y',\"z\"\n"), quote="'")
# oracle: step
spaces = file("spaces.csv", "A,B,C\n 1 , hello ,\" quoted \"\n 2 ,  x,\"y\"\n")
blanks = read(spaces)
kept_blanks = read(spaces, ignore_surrounding_spaces=False)
trimmed = read(spaces, trim=True)
# oracle: step
quoted = read(file("quoted.csv", "A,B,C\n\"x\ny\",\"x\"\"y\",\"\"\n\"a\r\nb\",\"\"\"\",2\n"))
line_ends = read(file("line_ends.csv", "A,B\r\n1,2\r\n3,4\r5,6"))
# oracle: step
short_rows = read(file("short_rows.csv", "A,B,C\n1,2\n3,4,5\n"), allow_missing_columns=True)
long_rows = read(file("long_rows.csv", "A,B\n1,2,3\n4,5\n"), ignore_excess_columns=True)
with_empty_lines = file("empty_lines.csv", "A\n1\n\n3\n")
empty_lines = read(with_empty_lines)
no_empty_lines = read(with_empty_lines, ignore_empty_lines=True)
# oracle: step
names = read(file("names.csv", (
    "First Name,last-name,2nd,int,i,a.b,A B,A_B,$x,_,true,null,var,k,in,not,x y-z,1,_1,a$\n"
    + ",".join(["1"] * 20) + "\n"
)))
repeated = read(file("repeated.csv", "A,A,B,B2,B,\"C D\"\n1,2,3,4,5,6\n"))
bom = read(file("bom.csv", "﻿A,B\n1,2\n"))
# oracle: step
# a file whose header line is empty has rows and no column
no_columns = read(file("no_columns.csv", "\n\n\n"))
only_headers = read(file("only_headers.csv", "A,B"))
