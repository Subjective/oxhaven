# partitioned_transform of refreshing partitioned tables, and of one with one that is not:
# a pair appears when the second table gets the key or the first one does, and goes when
# either loses it; the function is called in the cycle for each pair that appears, also
# for one that comes back, and not for a change inside a constituent.
# oracle: results left, right, both, both_underlying, static_left, static_left_underlying, static_right, static_right_underlying
from deephaven import empty_table, input_table, new_table
from deephaven.column import int_col


def change(ids, keys, values):
    return new_table([int_col("Id", ids), int_col("K", keys), int_col("V", values)])


left = input_table(init_table=change([1, 2, 3], [1, 2, 1], [10, 20, 30]), key_cols="Id")
right = input_table(init_table=change([1, 2], [1, 3], [100, 300]), key_cols="Id")
lefts = left.partition_by("K")
rights = right.partition_by("K")
static = empty_table(6).update(["K = i % 3", "V = i"]).partition_by("K")
calls = []


def pair(first, second):
    calls.append((first.size, second.size, first.is_refreshing, second.is_refreshing))
    return first.natural_join(second.view(["K", "W = V"]).last_by("K"), "K", "W")


def called():
    seen = sorted(calls)
    calls.clear()
    return seen


pairs = lefts.partitioned_transform(rights, pair)
assert called() == [(0, 0, False, False), (2, 1, True, True)]
assert pairs.is_refreshing and pairs.unique_keys and pairs.constituent_changes_permitted
assert [column.name for column in pairs.constituent_table_columns] == ["Id", "K", "V", "W"]
both = pairs.merge()
both_underlying = pairs.table

from_static = static.partitioned_transform(rights, pair)
assert called() == [(0, 0, False, False), (2, 1, False, True)]
assert from_static.is_refreshing and from_static.constituent_changes_permitted
static_left = from_static.merge()
static_left_underlying = from_static.table

to_static = lefts.partitioned_transform(static, pair)
assert called() == [(0, 0, False, False), (1, 2, True, False), (2, 2, True, False)]
assert to_static.is_refreshing and to_static.constituent_changes_permitted
static_right = to_static.merge()
static_right_underlying = to_static.table
# oracle: step
right.add(change([3], [2], [200]))
# oracle: step
assert called() == [(1, 1, True, True), (2, 1, False, True)]
left.add(change([4], [3], [40]))
# oracle: step
assert called() == [(1, 1, True, True)]
right.delete(change([1], [0], [0]).view("Id"))
# oracle: step
assert called() == []
right.add(change([1], [1], [101]))
# oracle: step
assert called() == [(2, 1, False, True), (2, 1, True, True)]
left.delete(change([1, 3], [0, 0], [0, 0]).view("Id"))
# oracle: step
assert called() == []
left.add(change([1], [1], [11]))
# oracle: step
assert called() == [(1, 1, True, True), (1, 2, True, False)]
right.add(change([9], [1], [109]))
# oracle: step
assert called() == []
