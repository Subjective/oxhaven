# transform of a refreshing partitioned table: the function is called for every constituent
# when the result is made and for a table without rows, and then in the update cycle for
# every constituent that appears, also for one that comes back after its key had lost
# every row. A change inside a constituent is no call: it reaches what the function made.
# oracle: results rows, underlying, keys, merged, ones, counted, counted_underlying
from deephaven import SortDirection, input_table, new_table
from deephaven.column import int_col


def change(ids, keys, values):
    return new_table([int_col("Id", ids), int_col("K", keys), int_col("V", values)])


calls = []


def last(table):
    calls.append((table.size, table.is_refreshing))
    return table.tail(1)


def called():
    seen = sorted(calls)
    calls.clear()
    return seen


rows = input_table(init_table=change([1, 2, 3, 4], [1, 2, 1, 3], [10, 20, 30, 40]), key_cols="Id")
pt = rows.partition_by("K")
lasts = pt.transform(last)
assert called() == [(0, False), (1, True), (1, True), (2, True)]
assert lasts.is_refreshing and lasts.unique_keys and lasts.constituent_changes_permitted
assert list(lasts.key_columns) == ["K"]
assert lasts.constituent_column == "__CONSTITUENT__"
assert [column.name for column in lasts.constituent_table_columns] == ["Id", "K", "V"]
underlying = lasts.table
keys = lasts.keys()
merged = lasts.merge()
ones = lasts.get_constituent(1)
assert ones.is_refreshing
count = pt.sort("K", SortDirection.DESCENDING).transform(lambda table: table.count_by("N", "K"))
assert [column.name for column in count.constituent_table_columns] == ["K", "N"]
counted = count.merge()
counted_underlying = count.table
# oracle: step
rows.add(change([5, 6], [5, 1], [50, 60]))
# oracle: step
assert called() == [(1, True)]
rows.delete(change([2], [0], [0]).view("Id"))
# oracle: step
assert called() == []
assert lasts.get_constituent(2) is None
rows.add(change([7, 8], [2, 2], [70, 80]))
# oracle: step
assert called() == [(2, True)]
rows.add(change([3, 4], [3, 3], [31, 41]))
# oracle: step
assert called() == []
assert [table.size for table in lasts.constituent_tables] == [1, 1, 1, 1]
rows.delete(change([1, 3, 4, 5, 6, 7, 8], [0] * 7, [0] * 7).view("Id"))
# oracle: step
assert called() == []
assert lasts.constituent_tables == []
