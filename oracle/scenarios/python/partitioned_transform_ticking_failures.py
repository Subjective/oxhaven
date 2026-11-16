# What a transform function may not answer. When the result is made, the transform fails.
# In an update cycle, for a constituent that appears, the result fails: its underlying
# table and its merge are failed tables from then on, and the partitioned table it was
# made of goes on. The function fails there by raising, by answering None or something
# that is not a table, by answering a table with other columns than it gave a table
# without rows, and by using a formula, for which it has no query scope in a cycle either.
# oracle: results rows, source_merged, good_merged
from deephaven import input_table, new_table
from deephaven.column import int_col


def change(ids, keys, values):
    return new_table([int_col("Id", ids), int_col("K", keys), int_col("V", values)])


rows = input_table(init_table=change([1, 2, 3, 4], [1, 2, 1, 3], [10, 20, 30, 40]), key_cols="Id")
pt = rows.partition_by("K")
source_merged = pt.merge()


def fifth(table):
    """Whether `table` is the constituent of the key 5, which appears in a cycle."""
    return table.size == 1 and table.is_refreshing and not seen_all


def raises(table):
    if fifth(table):
        raise ValueError("no fifth")
    return table.tail(1)


def nothing(table):
    return None if fifth(table) else table.tail(1)


def number(table):
    return 5 if fifth(table) else table.tail(1)


def other_columns(table):
    return table.drop_columns("V") if fifth(table) else table.tail(1)


def formula(table):
    return table.update("W = V + 1").drop_columns("W") if fifth(table) else table.tail(1)


# The constituents at the start have one or two rows; none of them is to be taken for
# the one that appears.
seen_all = True
made = {
    function.__name__: pt.transform(function)
    for function in (raises, nothing, number, other_columns, formula)
}
good = pt.transform(lambda table: table.tail(1))
good_merged = good.merge()
held = {name: (transformed.table, transformed.merge()) for name, transformed in made.items()}
assert not any(table.is_failed for pair in held.values() for table in pair)
seen_all = False
# oracle: step
rows.add(change([5], [5], [50]))
# oracle: step
for name, (underlying, merged) in held.items():
    assert underlying.is_failed, name
    assert merged.is_failed, name
assert not pt.table.is_failed and not source_merged.is_failed
assert not good.table.is_failed and not good_merged.is_failed
assert pt.get_constituent(5).size == 1
seen_all = True
rows.add(change([6], [5], [60]))
# oracle: step
# oracle: fails
at_once = pt.transform(lambda table: None if table.size else table)
# oracle: step
# oracle: fails
at_once = pt.transform(lambda table: None)
# oracle: step
# oracle: fails
at_once = pt.transform(lambda table: table.drop_columns("V") if table.size else table)
# oracle: step
# oracle: fails
at_once = pt.transform(lambda table: 1)
# oracle: step
# oracle: fails
at_once = pt.transform(lambda table: table.tail(1 // table.size))
# oracle: step
# oracle: fails
at_once = pt.transform(lambda table: table.where("V > 2"))
# oracle: step
assert pt.transform(lambda table: table).get_constituent(5).size == 2
