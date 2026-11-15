# The asynchronous forms: the changes are made after the script that asked for them, in
# the order it asked, and then the callbacks are called. The order of the callbacks is not
# the order of the calls on the reference, so only their number is compared. A synchronous
# add made after asynchronous ones is applied after them; whether their callbacks have
# been called when it returns is not defined, so that is not looked at (the -1).
# oracle: results keyed, in_script, ended_count
import time

from deephaven import input_table, new_table
from deephaven.column import double_col, int_col


def rows(keys, values):
    return new_table([int_col("K", keys), double_col("V", values)])


def count_of(values):
    return new_table([int_col("N", [len(values)])])


def wait_for(count):
    deadline = time.time() + 30
    while len(ended) < count and time.time() < deadline:
        time.sleep(0.01)


keyed = input_table(init_table=rows([1], [1.0]), key_cols="K")
ended = []
keyed.add_async(rows([1, 2], [10.0, 20.0]), on_success=lambda: ended.append("added"))
keyed.delete_async(rows([2], [0.0]).view("K"), on_success=lambda: ended.append("deleted"))
keyed.add_async(rows([1, 3], [11.0, 30.0]), lambda: ended.append("added again"))
in_script = new_table([int_col("Size", [keyed.size]), int_col("Ended", [len(ended)])])
ended_count = count_of([])
# oracle: step
wait_for(3)
ended_count = count_of(ended)
# oracle: step
keyed.add_async(rows([3], [31.0]))
keyed.add_async(rows([3, 4], [32.0, 40.0]), lambda: ended.append("last"))
keyed.add(rows([3], [33.0]))
in_script = new_table([int_col("Size", [keyed.size]), int_col("Ended", [-1])])
# oracle: step
wait_for(4)
ended_count = count_of(ended)
