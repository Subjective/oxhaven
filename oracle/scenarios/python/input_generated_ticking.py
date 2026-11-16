# Function generated tables that tick: generated again when a source table changes, one
# of two too; with rows that come and go; from a refreshing table; with arguments; once
# only for no source table and for a negative interval; and on an interval, which the
# script lets pass before it makes the update cycle run that generates the table again.
# oracle: results counted, both, derived, shrinking, once, never, timed
import time

from deephaven import dtypes, empty_table, function_generated_table, input_table, new_table
from deephaven.column import int_col, string_col

source = input_table({"K": dtypes.int32})
other = input_table({"K": dtypes.int32})
calls = []
marks = []


def count(label, scale=1):
    calls.append(label)
    return new_table([int_col("Calls", [len(calls) * scale]), string_col("Label", [label])])


def of_both():
    return empty_table(source.size + other.size).update("X = i")


def fewer():
    return empty_table(max(0, 3 - source.size)).update("X = ii + %d" % source.size)


def marked():
    return new_table([int_col("Marks", [len(marks)])])


def key(value):
    return new_table([int_col("K", [value])])


counted = function_generated_table(count, source_tables=source, args=("c",), kwargs={"scale": 10})
both = function_generated_table(of_both, source_tables=[source, other])
derived = function_generated_table(lambda: source.update("L = K * 2"), source_tables=source)
shrinking = function_generated_table(fewer, source_tables=source)
once = function_generated_table(of_both, source_tables=[])
never = function_generated_table(of_both, refresh_interval_ms=-1)
timed = function_generated_table(marked, refresh_interval_ms=50)
# oracle: step
source.add(key(1))
# oracle: step
other.add(key(1))
other.add(key(2))
# oracle: step
marks.append(1)
time.sleep(0.3)
source.add(key(2))
# oracle: step
source.add(key(3))
source.add(key(4))
# oracle: step
# oracle: fails
function_generated_table(of_both, source_tables=empty_table(1))
# oracle: step
# oracle: fails
function_generated_table(of_both)
