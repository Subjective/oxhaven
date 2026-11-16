# partitioned_transform of two partitioned tables that are not refreshing: a row for each
# row of the first whose key the second has, with what the function makes of the two
# constituents. The key columns are matched by name, or by position when the names
# differ; the second may be without its key columns. Then what is refused.
# oracle: results paired, paired_underlying, paired_keys, by_position, itself, without_keys, again
from deephaven import SortDirection, empty_table

first = empty_table(6).update(["K = i % 3", "V = i"]).partition_by("K")
second = empty_table(8).update(["K = 3 - i % 4", "W = i * 10"]).partition_by("K")
calls = []


def pair(left, right):
    calls.append((left.size, right.size))
    return left.natural_join(right.last_by("K"), "K", "W")


pairs = first.partitioned_transform(second, pair)
assert calls == [(2, 2), (2, 2), (2, 2), (0, 0)]
assert not pairs.is_refreshing and pairs.unique_keys and not pairs.constituent_changes_permitted
assert list(pairs.key_columns) == ["K"]
assert pairs.constituent_column == "__CONSTITUENT__"
assert [column.name for column in pairs.constituent_table_columns] == ["K", "V", "W"]
paired = pairs.merge()
paired_underlying = pairs.table
paired_keys = pairs.keys()

other_name = empty_table(8).update(["J = i % 4", "W = i * 10"]).partition_by("J")
by_position = first.partitioned_transform(other_name, lambda left, right: left.head(1))
assert list(by_position.key_columns) == ["K"]
by_position = by_position.merge()
itself = first.partitioned_transform(first, lambda left, right: left.tail(1)).merge()
dropped = empty_table(8).update(["K = i % 4", "W = i * 10"]).partition_by("K", drop_keys=True)
without = first.partitioned_transform(
    dropped, lambda left, right: left.head(1).natural_join(right.tail(1), [], "W")
)
assert [column.name for column in without.constituent_table_columns] == ["K", "V", "W"]
without_keys = without.merge()
# a pair of a transform, and of a sorted and filtered partitioned table
again = (
    pairs.sort("K", SortDirection.DESCENDING)
    .partitioned_transform(second.filter("K > 0"), lambda left, right: left.tail(1))
    .merge()
)
# oracle: step
# oracle: fails
two_keys = empty_table(8).update(["K = i % 4", "L = i % 2"]).partition_by(["K", "L"])
refused = first.partitioned_transform(two_keys, lambda left, right: left)
# oracle: step
# oracle: fails
long_keys = empty_table(8).update(["K = (long)(i % 4)", "W = i"]).partition_by("K")
refused = first.partitioned_transform(long_keys, lambda left, right: left)
# oracle: step
# oracle: fails
refused = first.partitioned_transform(second, lambda left, right: None)
# oracle: step
# oracle: fails
refused = first.partitioned_transform(second, lambda left, right: None if left.size else left)
# oracle: step
# oracle: fails
refused = first.partitioned_transform(second, lambda left, right: left.update("X = 1"))
# oracle: step
# oracle: fails
refused = first.partitioned_transform(second, lambda left, right: right if left.size else left)
# oracle: step
# oracle: fails
refused = first.partitioned_transform(first.table, lambda left, right: left)
# oracle: step
# oracle: fails
refused = first.partitioned_transform(second, lambda table: table)
