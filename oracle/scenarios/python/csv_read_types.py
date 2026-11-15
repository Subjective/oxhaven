# The type a column of a CSV file gets from its cells: the narrowest of boolean, int, long,
# double, date-time, char and String that takes every cell. An empty cell is a null, the
# value that stands for null in a type moves the column to the next type, and a type named
# in `header` is taken as it is.
# oracle: results types, widened, nulls, numbers, instants, named, static
import os
import shutil
import tempfile

root = os.path.join(tempfile.gettempdir(), "oxhaven_csv_read_types")
shutil.rmtree(root, ignore_errors=True)
os.makedirs(root)


def file(name, text):
    path = os.path.join(root, name)
    with open(path, "w", newline="") as f:
        f.write(text)
    return path


from deephaven import dtypes, read_csv
from deephaven.csv import read

# the harness does not compare a char column, so Letter is compared as its code
types = read(file("types.csv", (
    "Flag,Small,Big,Real,Text,When,Letter\n"
    "true,1,3000000000,1.5,hello,2024-01-02T03:04:05Z,x\n"
    "FALSE,-2,-4,2,\"wor,ld\",2024-01-02T03:04:05.123456789Z,y\n"
    "True,3,5,1e3,,2024-01-02T03:04Z,z\n"
))).view(["Flag", "Small", "Big", "Real", "Text", "When", "Code = (int)Letter"])
# oracle: step
# each column starts as one type and meets a cell that the type does not take
widened = read_csv(file("widened.csv", (
    "IntToLong,IntToDouble,IntToString,LongToDouble,DoubleToString,BoolToString,WhenToString,CharToString,CharOfDigits\n"
    "1,1,1,3000000000,1.5,true,2024-01-02T03:04:05Z,a,1\n"
    "3000000000,2.5,x,1e3,x,1,nope,bc,x\n"
    "-7,-0,7,0x1p4,NaN,false,2024-01-02,d,5\n"
))).view([
    "IntToLong", "IntToDouble", "IntToString", "LongToDouble", "DoubleToString", "BoolToString",
    "WhenToString", "CharToString", "Code = (int)CharOfDigits",
])
# oracle: step
# empty cells are nulls whatever the type, a column of nothing else is a String column,
# and the null value of a type is not a value of the type
nulls = read(file("nulls.csv", (
    "I,S,B,T,Nothing,NotInt,NotLong,NotDouble\n"
    ",,,,,-2147483648,-9223372036854775808,-1.7976931348623157e308\n"
    "5,x,true,2024-01-02T03:04:05Z,,1,1,1\n"
    ",,,,,,,\n"
)))
# oracle: step
numbers = read(file("numbers.csv", (
    "Plain,Signed,Exponent,Suffix,Hex,Special,Long,TooLong,Quoted,Spaced\n"
    "1.,+5,1e+05,1d,0x1p3,NaN,9223372036854775807,9223372036854775808,\"1\", 7 \n"
    ".5,-5,1E-5,2.5F,0X1.8P1,Infinity,-9223372036854775807,123456789012345678901234567890,\"2\",\" 8\"\n"
    "0.1,0,1.7976931348623157E308,3f,0x1p-1074,-Infinity,0,1e400,\"-3\",\"9 \"\n"
)))
# oracle: step
instants = read(file("instants.csv", (
    "Zulu,Offset,Space,Zone,Compact,NoZone,Far\n"
    "2024-01-02T03:04:05Z,2024-01-02T03:04:05+01:00,2024-01-02 03:04:05Z,2024-01-02T03:04:05 UTC,20240102T030405Z,2024-01-02T03:04:05,9999-12-31T23:59:59Z\n"
    "2024-02-29T03:04Z,2024-01-02T03:04:05.5-05:00,2024-01-02 03:04:05.123Z,2024-01-02T03:04:05 Etc/GMT+5,20240102T03Z,2024-01-02T03:04:05.123456789,0000-01-01T00:00:00Z\n"
)))
# oracle: step
named = read(file("named.csv", (
    "a,b,c,d,e,f,g,h,i,j\n"
    "true,1,x,2,3,4,1.5,2.5,s,2024-01-02T03:04:05Z\n"
    ",,,,,,,,,\n"
    "false,-127,y,-32767,7,8,0.1,0.1,7,2024-01-02T03:04:05.5Z\n"
)), header={
    "Bo": dtypes.bool_, "By": dtypes.byte, "Ch": dtypes.char, "Sh": dtypes.short,
    "In": dtypes.int32, "Lo": dtypes.long, "Fl": dtypes.float32, "Do": dtypes.double,
    "St": dtypes.string, "Ts": dtypes.Instant,
}).view(["Bo", "By", "Code = (int)Ch", "Sh", "In", "Lo", "Fl", "Do", "St", "Ts"])
# oracle: step
# the table is static and in memory: operations on it are those of any other table
big = read(file("big.csv", "K,V\n" + "".join(f"{i % 7},{i * 0.5}\n" for i in range(5000))))
static = big.where("V > 100").update("W = V * K").sum_by("K").sort("K")
