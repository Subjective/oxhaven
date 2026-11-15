# A table written as a CSV file: a line of column names and a line per row, a null as
# nothing, a cell with a comma, a quote or a line feed in quotes, numbers as Java prints
# them and an instant in the server's time zone. Each file is compared as its lines, and
# read back.
# oracle: results all_lines, some_lines, numbers_lines, empty_lines, back, numbers_back, source
import os
import shutil
import tempfile

root = os.path.join(tempfile.gettempdir(), "oxhaven_csv_write")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root)


def path(name):
    return os.path.join(root, name)


def lines(name):
    with open(path(name), newline="") as f:
        text = f.read()
    return new_table([string_col("Line", text.split("\n"))])


from deephaven import dtypes, new_table, read_csv, write_csv
from deephaven.column import string_col
from deephaven.csv import read, write

with open(path("source.csv"), "w", newline="") as f:
    f.write(
        "a,b,c,d,e,f,g,h,i,j\n"
        "true,1,x,2,3,4,1.5,2.5,plain,2024-01-02T03:04:05Z\n"
        ",,,,,,,,,\n"
        "false,-127,\",\",-32767,-7,3000000000,0.1,0.1,\"q\"\"uo,te\",2024-01-02T03:04:05.5Z\n"
        "true,0,\"\"\"\",0,0,0,-0,1e21,\"two\nlines\",1970-01-01T00:00Z\n"
        "true,5,y,6,7,8,NaN,Infinity, kept ,2262-04-11T23:47:16.854775807Z\n"
    )
source = read(path("source.csv"), header={
    "Bo": dtypes.bool_, "By": dtypes.byte, "Ch": dtypes.char, "Sh": dtypes.short,
    "In": dtypes.int32, "Lo": dtypes.long, "Fl": dtypes.float32, "Do": dtypes.double,
    "St": dtypes.string, "Ts": dtypes.Instant,
}, ignore_surrounding_spaces=False).update("Code = (int)Ch")
write(source.drop_columns("Code"), path("all.csv"))
all_lines = lines("all.csv")
# oracle: step
write_csv(source, path("some.csv"), cols=["St", "In", "Code"])
some_lines = lines("some.csv")
# oracle: step
numbers = read_csv(path("source.csv"), headless=True, skip_rows=1, num_rows=1).view([
    "Whole = 100.0 * ii", "Small = 1.0E-5", "Large = 1.0E7", "Under = 9999999.0",
    "Third = 1.0 / 3", "Tiny = 4.9E-324", "Single = (float)1.1", "SingleLarge = (float)1.0E10",
    "Micros = '2024-01-02T03:04:05.000001Z'", "Millis = '2024-01-02T03:04:05.120Z'",
    "Early = '1969-12-31T23:59:59.999999999Z'",
])
write(numbers, path("numbers.csv"))
numbers_lines = lines("numbers.csv")
# oracle: step
write(source.head(0), path("empty.csv"), cols=["Bo", "St"])
empty_lines = lines("empty.csv")
# oracle: step
# what was written reads back as the same values, the types as the cells give them
back = read(path("all.csv")).view(["Bo", "By", "Code = (int)Ch", "Sh", "In", "Lo", "Fl", "Do", "St", "Ts"])
numbers_back = read(path("numbers.csv"))
# oracle: step
# oracle: fails
write(source, path("unknown.csv"), cols=["Nope"])
# oracle: step
# oracle: fails
write(source, os.path.join(root, "no", "such", "directory.csv"))
